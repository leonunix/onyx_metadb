//! Refcount shard: a [`PagedRefcountArray`] fronted by a 4-slot TXG ring
//! of [`DeltaMap`]s.
//!
//! ## Why a TXG-slot ring (mirrors L2P)
//!
//! Under `rc_authoritative_reclaim` the refcount is *derived* from L2P
//! remaps (`incref(new) + decref(old)`). L2P buffers its updates in a
//! 4-slot TXG ring ([`crate::db::l2p_buffer::L2pBuffer`]) and the sync
//! folds only the frozen Syncing slot, so L2P durability is keyed to a
//! clean per-TXG `checkpoint_lsn` prefix. refcount must fold on the SAME
//! boundary: metadb has no data-plane WAL, so on crash onyx re-derives
//! the data plane by replaying its LV2 buffer and re-issuing the L2P
//! remaps; rc net-zero idempotency on replay holds ONLY when rc-durable
//! reflects exactly the same commit set as L2P-durable. A free-running rc
//! fold (the old drainer) could make rc durable ahead of L2P for
//! open-TXG commits → replay double-count. Keying rc deltas by TXG and
//! folding only the Syncing slot closes that, and bounds each fold to one
//! TXG (killing the unbounded force-drain commit spike).
//!
//! ## Read path (`get` / `lookup_entry`)
//!
//! refcount is *cumulative*, so the effective rc = on-disk array base +
//! the SUM of pending deltas across all four slots (unlike L2P's
//! newest-slot-wins). Underflow is floored to 0 via
//! [`super::merge_read_or_floor`] (the same transient-checkpoint-race
//! floor as before).
//!
//! ## Apply path (`stage`)
//!
//! `stage(txg, …)` merges its delta into `delta_slots[txg & 3]`, after
//! reading the cumulative prev. It holds only the open slot's lock across
//! its own read+merge; the other slots are read under brief individual
//! locks. Concurrent commits on a shard all target the same open slot and
//! serialise on its lock.
//!
//! ## Fold path (`begin_checkpoint`)
//!
//! `begin_checkpoint(txg)` folds ONLY `delta_slots[txg & 3]` (the frozen
//! Syncing slot — `promote_to_syncing` required `inflight == 0`, so it
//! receives no concurrent inserts). It is **publish-before-clear**: it
//! folds the slot's entries into the array (via `stage_deltas_in_memory`,
//! which installs into the page cache + page table) and only THEN clears
//! the slot. So a concurrent cumulative read can transiently *over*-count
//! (slot still full + array already folded) but never *under*-count — the
//! safe direction (no spurious `freed_pba`).
//!
//! Because the Syncing slot holds exactly TXG `txg`'s rc deltas, and every
//! commit with `lsn <= wal_checkpoint(txg)` is stamped to `txg` or an
//! earlier (already-folded) TXG, folding the slot makes rc durable to
//! `wal_checkpoint` — the SAME boundary L2P's `drain_syncing_slot_into_trees`
//! reaches. So the flush's existing per-shard `durable_seq =
//! wal_checkpoint.max(prev)` (selected) / `prev` (unselected) is already
//! correct for rc, exactly as for a non-buffered L2P shard; no separate
//! watermark term is needed (the slot-keying — not a cap — is what keeps rc
//! durability aligned with L2P's `checkpoint_lsn` prefix).
//!
//! `begin_checkpoint_all_slots` folds every slot — used by the threads-OFF
//! inline flush and by recovery (rc analogue of `L2pBuffer::drain_all_slots`).
//!
//! ## Lock order
//!
//! Within a slot: `delta_slots[i]` → `array.inner`. Multi-slot holders
//! (only `stage`, holding the open slot while briefly reading others)
//! never hold two slot locks while acquiring a lower-indexed one in a way
//! that can cycle, because all concurrent stages share the same open slot
//! and serialise on it.

use std::sync::Arc;

use parking_lot::Mutex;

use super::RcEntry;
use super::array::{PagedRefcountArray, StagedDeltas};
use super::delta::{DeltaMap, Pending};
use crate::cache::PageCache;
use crate::error::Result;
use crate::page_store::PageStore;
use crate::types::{Lsn, PageId, Pba, Txg};

/// Number of TXG ring slots. Matches `crate::db::l2p_buffer::TXG_SIZE`.
const TXG_SLOTS: usize = 4;

#[inline]
fn slot_index(txg: Txg) -> usize {
    (txg as usize) & (TXG_SLOTS - 1)
}

pub struct RcShard {
    /// Pending deltas keyed by TXG ring slot (`txg & 3`). A commit
    /// stamped to TXG `t` merges into `delta_slots[t & 3]`; the sync
    /// folds only the Syncing slot. Reads sum across all four (refcount
    /// is cumulative).
    delta_slots: [Mutex<DeltaMap>; TXG_SLOTS],
    pub(super) array: PagedRefcountArray,
    /// Serialises the fold's [publish, clear] inconsistency window
    /// against a *consistent* read ([`Self::get_consistent`]). The fold
    /// (`checkpoint_slots`) installs the folded delta into the array and
    /// only THEN clears the slot, so between the two a `sum_pending` +
    /// `array.get` pair can straddle the fold and double-count the folded
    /// delta — for a net-decref slot that under-counts and `merge_read_or_floor`
    /// floors it to a SPURIOUS 0. The cheap hot read (`get`) tolerates that
    /// (a dedup-hit guard just demotes to a fresh miss, fully reversible),
    /// but the GC reclaim Gate-1 treats rc==0 as proof to IRREVERSIBLY free
    /// the PBA (Gate-2 blockmap reverify is skipped under
    /// `rc_authoritative_reclaim`) → premature free → reuse → read CRC.
    /// The fold takes this in write mode across the in-memory publish+clear
    /// (microseconds — the page IO is staged and written later); the
    /// consistent read takes it in read mode across its sample. The hot
    /// `get`/`stage` paths never touch it.
    fold_lock: parking_lot::RwLock<()>,
}

/// Checkpoint produced by [`RcShard::begin_checkpoint`]. Carries the
/// sealed pages, the snapshots needed to drive `paged_meta::write_chain`
/// outside the apply gate, and the drained deltas for
/// [`RcShard::abort_checkpoint`] to restore on a failed flush.
pub struct RcCheckpoint {
    pub(super) staged: StagedDeltas,
    /// The slot's drained entries + which slot they came from — restored
    /// by `abort_checkpoint` so a retry redoes the fold.
    drained_deltas: Vec<(Pba, Pending)>,
    /// Slot indices folded by this checkpoint (one for the per-TXG path,
    /// up to four for `begin_checkpoint_all_slots`). Abort restores
    /// `drained_deltas` into the first of these (they were merged on the
    /// way in, so a single restore slot is sufficient and correct — the
    /// next fold re-spreads them is unnecessary because abort only needs
    /// the deltas back in *some* live slot to be re-folded).
    restore_slot: usize,
    snapshot_page_table: Vec<PageId>,
    snapshot_meta_chain: Vec<PageId>,
}

impl RcCheckpoint {
    /// Empty checkpoint — fast path when nothing was drained / staged.
    pub fn is_empty(&self) -> bool {
        self.staged.is_empty()
    }

    /// Append sealed pages to a shared write-out vec; lifecycle.rs uses
    /// this to fold refcount writes into the same
    /// `page_store.write_sealed_page_runs` + `sync` as L2P.
    pub fn append_sealed_pages(&self, out: &mut Vec<(PageId, Arc<crate::page::Page>)>) {
        self.staged.append_sealed_pages(out);
    }

    pub(super) fn snapshot_page_table(&self) -> &[PageId] {
        &self.snapshot_page_table
    }

    pub(super) fn snapshot_meta_chain(&self) -> &[PageId] {
        &self.snapshot_meta_chain
    }

    /// Number of delta-map entries the fold processed.
    pub fn drained_deltas_count(&self) -> usize {
        self.drained_deltas.len()
    }

    /// Number of freshly-allocated data pages this checkpoint produced.
    pub fn fresh_pages_count(&self) -> usize {
        self.staged.pages.iter().filter(|p| p.is_fresh).count()
    }

    #[cfg(test)]
    pub(crate) fn fresh_page_ids(&self) -> Vec<(usize, PageId)> {
        self.staged
            .pages
            .iter()
            .filter(|p| p.is_fresh)
            .map(|p| (p.page_idx, p.page_id))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn touched_existing_page_ids(&self) -> Vec<PageId> {
        self.staged
            .pages
            .iter()
            .filter(|p| !p.is_fresh)
            .map(|p| p.page_id)
            .collect()
    }
}

impl RcShard {
    pub fn create(page_store: Arc<PageStore>, page_cache: Arc<PageCache>) -> Result<Self> {
        let array = PagedRefcountArray::create(page_store, page_cache)?;
        Ok(Self::new_with_array(array))
    }

    /// Open an existing shard at `meta_page_id` (read from the manifest).
    pub fn open(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        meta_page_id: PageId,
    ) -> Result<Self> {
        let array = PagedRefcountArray::open(page_store, page_cache, meta_page_id)?;
        Ok(Self::new_with_array(array))
    }

    fn new_with_array(array: PagedRefcountArray) -> Self {
        Self {
            delta_slots: std::array::from_fn(|_| Mutex::new(DeltaMap::new())),
            array,
            fold_lock: parking_lot::RwLock::new(()),
        }
    }

    pub fn meta_page_id(&self) -> PageId {
        self.array.meta_page_id()
    }

    /// Logical refcount. Sums pending across all four slots, falls back
    /// to the on-disk array, floors a transient underflow to 0.
    pub fn get(&self, pba: Pba) -> Result<u32> {
        Ok(self.lookup_entry(pba)?.rc)
    }

    /// Full entry (rc + birth_lsn). Internal use only — public callers
    /// usually want [`get`].
    pub fn get_entry(&self, pba: Pba) -> Result<RcEntry> {
        self.lookup_entry(pba)
    }

    /// Fold-consistent refcount read. Unlike [`get`] (which can straddle a
    /// concurrent fold's [publish, clear] window and transiently double-count
    /// the folded delta — under-counting a net-decref slot to a spurious 0),
    /// this takes `fold_lock` in read mode so it never overlaps the fold's
    /// inconsistency window. The returned rc reflects a coherent
    /// `array.base ⊕ Σ pending` sample. Used by the GC reclaim path, where an
    /// rc==0 misread is irreversible (frees a still-referenced PBA). Costlier
    /// than `get` (a shared lock); keep it on the cold reclaim path, not the
    /// dedup hot path.
    pub fn get_consistent(&self, pba: Pba) -> Result<u32> {
        let _read = self.fold_lock.read();
        Ok(self.lookup_entry(pba)?.rc)
    }

    /// Sum the pending deltas for `pba` across all four slots. Returns
    /// `(net_delta, max_lsn, any)`. Each slot is read under a brief
    /// individual lock — never holding two at once.
    fn sum_pending(&self, pba: Pba, skip: Option<usize>) -> (i64, Lsn, bool) {
        let mut net: i64 = 0;
        let mut max_lsn: Lsn = 0;
        let mut any = false;
        for (i, slot) in self.delta_slots.iter().enumerate() {
            if Some(i) == skip {
                continue;
            }
            if let Some(p) = slot.lock().get(pba) {
                net += p.delta;
                if p.last_lsn > max_lsn {
                    max_lsn = p.last_lsn;
                }
                any = true;
            }
        }
        (net, max_lsn, any)
    }

    fn lookup_entry(&self, pba: Pba) -> Result<RcEntry> {
        let (net, max_lsn, any) = self.sum_pending(pba, None);
        let base = self.array.get(pba)?;
        if !any {
            return Ok(base);
        }
        super::merge_read_or_floor(base, net, max_lsn)
    }

    /// Stage one op into the pending delta for `txg`'s slot. Returns the
    /// cumulative `(prev_rc, new_rc)` (callers surface `freed_pba` on
    /// `new == 0 && prev > 0`).
    ///
    /// A decref past zero is a benign double-decref — skipped (count
    /// left at its floor) rather than poisoning the commit pipeline.
    /// Overflow (delta >= 0) is fatal.
    pub fn stage(&self, txg: Txg, pba: Pba, delta: i64, lsn: Lsn) -> Result<(u32, u32)> {
        let idx = slot_index(txg);
        // Read the other three slots first (brief individual locks), then
        // hold the open slot across read-own + array read + merge so a
        // concurrent stage targeting the same open slot serialises here.
        let (mut net, mut max_lsn, mut any) = self.sum_pending(pba, Some(idx));
        let mut open = self.delta_slots[idx].lock();
        let pa = open.get(pba);
        if let Some(p) = pa {
            net += p.delta;
            if p.last_lsn > max_lsn {
                max_lsn = p.last_lsn;
            }
            any = true;
        }
        let base = self.array.get(pba)?;
        // Replay-skip: with no pending anywhere AND the on-disk page
        // generation already at/after this LSN, the op was already
        // applied (recovery same-LSN re-application). `>=` is correct
        // here — there is no drainer cycle-split hazard in the slot model.
        if !any && self.array.page_lsn(pba)? >= lsn {
            return Ok((base.rc, base.rc));
        }
        let merged_prev = super::merge_read_or_floor(base, net, max_lsn)?;
        let (post, skipped) = super::apply_delta_or_skip(merged_prev, delta, lsn)?;
        if skipped {
            super::note_decref_underflow_skip(delta, lsn, merged_prev.rc, "stage");
            return Ok((merged_prev.rc, merged_prev.rc));
        }
        open.merge(pba, delta, lsn);
        Ok((merged_prev.rc, post.rc))
    }

    /// Fold ONLY the Syncing slot (`txg & 3`). Caller has frozen the slot
    /// by promoting `txg` to Syncing (`inflight == 0`). After the fold rc
    /// is durable up to that TXG's `wal_checkpoint` (the slot held exactly
    /// this TXG's deltas), so the flush's `wal_checkpoint.max(prev)`
    /// durable_seq is correct — no separate watermark to record.
    pub fn begin_checkpoint(&self, txg: Txg) -> Result<RcCheckpoint> {
        self.checkpoint_slots(&[slot_index(txg)])
    }

    /// Fold every slot. Used by the threads-OFF inline flush and by
    /// recovery's post-replay / cold `flush()` path (rc analogue of
    /// `force_compact_l2p_buffers`).
    pub fn begin_checkpoint_all_slots(&self) -> Result<RcCheckpoint> {
        self.checkpoint_slots(&[0, 1, 2, 3])
    }

    /// Shared fold body (publish-before-clear).
    fn checkpoint_slots(&self, slots: &[usize]) -> Result<RcCheckpoint> {
        // Snapshot (clone) the slot contents — publish-before-clear: we
        // fold into the array first and clear the slot only after, so a
        // concurrent cumulative read can transiently over-count but never
        // under-count.
        let restore_slot = slots[0];
        let mut drained: Vec<(Pba, Pending)> = Vec::new();
        for &i in slots {
            let slot = self.delta_slots[i].lock();
            for (pba, pending) in slot.iter() {
                drained.push((*pba, *pending));
            }
        }

        if drained.is_empty() {
            return Ok(RcCheckpoint {
                staged: StagedDeltas {
                    pages: Vec::new(),
                    max_lsn: 0,
                },
                drained_deltas: Vec::new(),
                restore_slot,
                snapshot_page_table: self.array.page_table_snapshot(),
                snapshot_meta_chain: self.array.meta_chain_snapshot(),
            });
        }

        // The [publish, clear] span below is the only window where the
        // array and the slots disagree (array folded, slot not yet cleared).
        // Hold `fold_lock` in write mode across it so a concurrent
        // `get_consistent` can never observe that torn state (the cheap `get`
        // still can, by design — see the field doc). This guards only the
        // in-memory publish+clear; the staged pages' IO happens later in the
        // flush, outside this lock, so the critical section is microseconds.
        let _fold = self.fold_lock.write();

        // Fold + publish into the array (cache + page_table) WITHOUT
        // clearing the slots yet.
        let staged = self.array.stage_deltas_in_memory(drained.clone())?;

        // Publish is visible now; clear the folded slots. They are frozen
        // (Syncing / threads-off quiesce) so a fresh `DeltaMap` discards
        // exactly what we folded with no risk of dropping a concurrent
        // insert.
        for &i in slots {
            *self.delta_slots[i].lock() = DeltaMap::new();
        }
        drop(_fold);

        let snapshot_page_table = self.array.page_table_snapshot();
        let snapshot_meta_chain = self.array.meta_chain_snapshot();
        Ok(RcCheckpoint {
            staged,
            drained_deltas: drained,
            restore_slot,
            snapshot_page_table,
            snapshot_meta_chain,
        })
    }

    /// Outside-gate IO: write a fresh meta chain. Cold-path shim
    /// (`RcShard::flush`, snapshot / drop_volume). The flush hot path
    /// uses [`Self::build_meta_chain`] + folds the sealed pages into the
    /// global checkpoint batch.
    pub fn write_meta_chain(&self, ckpt: &RcCheckpoint, free_lsn: Lsn) -> Result<Vec<PageId>> {
        if ckpt.is_empty() {
            return Ok(ckpt.snapshot_meta_chain.to_vec());
        }
        self.array.write_meta_chain_external(
            ckpt.snapshot_page_table(),
            ckpt.snapshot_meta_chain(),
            free_lsn,
        )
    }

    /// Outside-gate, **no-IO** companion of [`Self::write_meta_chain`]:
    /// builds + seals every page in the new chain in memory and returns
    /// the chain layout. Callers (`flush_with_gate`) drive one batched
    /// [`PageStore::write_sealed_page_runs`] across every shard's sealed
    /// pages, then walk the per-shard `to_free` lists + `install_meta_chain`
    /// after the manifest commit is durable.
    pub fn build_meta_chain(
        &self,
        ckpt: &RcCheckpoint,
    ) -> Result<(
        Vec<PageId>,
        Vec<(PageId, Arc<crate::page::Page>)>,
        Vec<PageId>,
    )> {
        if ckpt.is_empty() {
            return Ok((ckpt.snapshot_meta_chain.to_vec(), Vec::new(), Vec::new()));
        }
        self.array
            .build_meta_chain_external(ckpt.snapshot_page_table(), ckpt.snapshot_meta_chain())
    }

    /// Install the new meta chain. Briefly takes the array's inner lock.
    pub fn install_meta_chain(&self, new_chain: Vec<PageId>) {
        self.array.install_meta_chain(new_chain);
    }

    /// Drop the dirty-staged overlay entries for `ckpt`'s pages. The
    /// flush hot path MUST call this once `write_sealed_page_runs` +
    /// `page_store.sync()` have made the staged bytes durable; until
    /// then the overlay is what protects concurrent rc reads from the
    /// evictable-LRU / unwritten-disk window (see
    /// `PagedRefcountArray`'s `staged_overlay` doc). The cold path
    /// (`RcShard::flush` → `write_staged_pages`) clears internally.
    pub fn mark_staged_durable(&self, ckpt: &RcCheckpoint) {
        self.array.clear_staged(&ckpt.staged);
    }

    /// Roll back a checkpoint that failed before install. Frees fresh
    /// page ids + invalidates touched cache entries, then restores the
    /// drained deltas into a live slot so a retry redoes the fold.
    pub fn abort_checkpoint(&self, ckpt: RcCheckpoint, free_lsn: Lsn) {
        if ckpt.is_empty() {
            return;
        }
        self.array.abort_staged_deltas(&ckpt.staged, free_lsn);
        let mut slot = self.delta_slots[ckpt.restore_slot].lock();
        for (pba, pending) in ckpt.drained_deltas {
            slot.merge_pending(pba, pending);
        }
    }

    /// Synchronous flush for non-checkpoint callers (cold path). Folds
    /// every slot to disk and rotates the meta chain.
    pub fn flush(&self) -> Result<()> {
        let ckpt = self.begin_checkpoint_all_slots()?;
        if ckpt.is_empty() {
            return Ok(());
        }
        if let Err(err) = self.array.write_staged_pages(&ckpt.staged) {
            self.abort_checkpoint(ckpt, 0);
            return Err(err);
        }
        let new_chain = match self.write_meta_chain(&ckpt, 0) {
            Ok(c) => c,
            Err(err) => {
                self.abort_checkpoint(ckpt, 0);
                return Err(err);
            }
        };
        self.install_meta_chain(new_chain);
        Ok(())
    }

    /// Iterate every live entry. Forces a flush first.
    pub fn iter_live_flushed(&self) -> Result<Vec<(Pba, RcEntry)>> {
        self.flush()?;
        self.array.iter_live()
    }

    /// Iterate every live entry already present in the backing array.
    /// The caller is responsible for checkpointing / draining first.
    pub fn iter_live(&self) -> Result<Vec<(Pba, RcEntry)>> {
        self.array.iter_live()
    }

    /// Number of data pages currently on disk for this shard.
    pub fn allocated_data_pages(&self) -> usize {
        self.array.allocated_data_pages()
    }

    /// Best-effort count of in-memory rc deltas awaiting a fold, summed
    /// across all slots. `try_lock` so a slow shard doesn't stall the
    /// diag/watermark path.
    pub fn pending_delta_count(&self) -> usize {
        self.delta_slots
            .iter()
            .map(|s| s.try_lock().map(|d| d.len()).unwrap_or(0))
            .sum()
    }
}

#[cfg(test)]
mod tests;
