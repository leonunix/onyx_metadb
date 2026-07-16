//! Refcount shard: a [`PagedRefcountArray`] fronted by a 4-slot BFG ring
//! of [`DeltaMap`]s.
//!
//! ## Why a BFG-slot ring (mirrors L2P)
//!
//! Under `rc_authoritative_reclaim` the refcount is *derived* from L2P
//! remaps (`incref(new) + decref(old)`). L2P buffers its updates in a
//! 4-slot BFG ring ([`crate::db::l2p_buffer::L2pBuffer`]) and the sync
//! folds only the frozen Syncing slot, so L2P durability is keyed to a
//! clean per-BFG `checkpoint_lsn` prefix. refcount must fold on the SAME
//! boundary: metadb has no data-plane WAL, so on crash onyx re-derives
//! the data plane by replaying its LV2 buffer and re-issuing the L2P
//! remaps; rc net-zero idempotency on replay holds ONLY when rc-durable
//! reflects exactly the same commit set as L2P-durable. A free-running rc
//! fold (the old drainer) could make rc durable ahead of L2P for
//! open-BFG commits → replay double-count. Keying rc deltas by BFG and
//! folding only the Syncing slot closes that, and bounds each fold to one
//! BFG (killing the unbounded force-drain commit spike).
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
//! `stage(bfg, …)` merges its delta into `delta_slots[bfg & 3]` after reading
//! the cumulative prev. Like `stage_batch`, it samples the array before taking
//! `fold_lock.read()`, validates `fold_epoch`, then locks all four slots in
//! order. This makes the base + pending view atomic with respect to a
//! checkpoint's publish-before-clear move, which is required for an exact
//! `prev > 0 && new == 0` freed transition.
//!
//! ## Fold path (`begin_checkpoint`)
//!
//! `begin_checkpoint(bfg)` folds ONLY `delta_slots[bfg & 3]` (the frozen
//! Syncing slot — `promote_to_syncing` required `inflight == 0`, so it
//! receives no concurrent inserts). It is **publish-before-clear**: it
//! folds the slot's entries into the array (via `stage_deltas_in_memory`,
//! which installs into the page cache + page table) and only THEN clears
//! the slot. So a concurrent cumulative read can transiently *over*-count
//! (slot still full + array already folded) but never *under*-count — the
//! safe direction (no spurious `freed_pba`).
//!
//! Because the Syncing slot holds exactly BFG `bfg`'s rc deltas, and every
//! commit with `lsn <= wal_checkpoint(bfg)` is stamped to `bfg` or an
//! earlier (already-folded) BFG, folding the slot makes rc durable to
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
//! Commit staging takes `fold_lock.read()` then all `delta_slots` in ascending
//! order; after every slot guard is acquired it releases the fold guard before
//! merging. Checkpoint publish takes `fold_lock.write()` before touching its
//! slot. Read-only cumulative lookups take at most one slot at a time.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use super::RcEntry;
use super::array::{
    ENTRIES_PER_PAGE, PagedRefcountArray, STAGE_BASE_READ_BATCH_PAGES, StagedDeltas, StagedPageMeta,
};
use super::delta::{DeltaMap, Pending};
use crate::cache::PageCache;
use crate::error::{MetaDbError, Result};
use crate::page_store::PageStore;
use crate::types::{Bfg, Lsn, PageId, Pba};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RefcountApplyStageTimings {
    pub base_page_lookup: Duration,
    pub fold_lock_wait: Duration,
    pub slot_lock_wait: Duration,
    pub pending_slot_scan: Duration,
    pub delta_merge: Duration,
    pub base_lookup_attempts: u64,
    pub epoch_retries: u64,
    pub sampled_pbas: u64,
}

#[inline]
fn sample_refcount_breakdown(lsn: Lsn) -> bool {
    // Mix the commit LSN before sampling so BFG and checkpoint periods do not
    // alias with the sample selector. Every bucket in one commit still makes
    // the same decision, preserving the commit's complete PBA shape.
    let mut mixed = lsn.wrapping_add(0x9e37_79b9_7f4a_7c15);
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    (mixed ^ (mixed >> 31)) & 0x0f == 0
}

#[inline]
fn active_slot_mask(slots: &[parking_lot::MutexGuard<'_, DeltaMap>], open_idx: usize) -> u8 {
    debug_assert_eq!(slots.len(), BFG_SLOTS);
    debug_assert!(open_idx < slots.len());
    slots
        .iter()
        .enumerate()
        .fold(1 << open_idx, |mask, (idx, slot)| {
            if slot.is_empty() {
                mask
            } else {
                mask | (1 << idx)
            }
        })
}

#[inline]
fn scan_active_locked_pending(
    slots: &[parking_lot::MutexGuard<'_, DeltaMap>],
    mut active_slots: u8,
    pba: Pba,
) -> (i64, Lsn, bool) {
    let mut net = 0i64;
    let mut max_lsn = 0u64;
    let mut any = false;
    while active_slots != 0 {
        let slot_idx = active_slots.trailing_zeros() as usize;
        active_slots &= active_slots - 1;
        let slot = &slots[slot_idx];
        if let Some(pending) = slot.get(pba) {
            net += pending.delta;
            max_lsn = max_lsn.max(pending.last_lsn);
            any = true;
        }
    }
    (net, max_lsn, any)
}

#[inline]
#[allow(clippy::too_many_arguments)]
fn merge_staged_action(
    open: &mut DeltaMap,
    pba: Pba,
    delta: i64,
    lsn: Lsn,
    replay_skip: bool,
    base: RcEntry,
    page_lsn: Lsn,
    net: i64,
    max_lsn: Lsn,
    any: bool,
    context: &'static str,
    zero_decref_is_noop: bool,
) -> Result<(u32, u32)> {
    if replay_skip && !any && page_lsn >= lsn {
        return Ok((base.rc, base.rc));
    }
    // The caller holds a fold-epoch-validated base plus every slot guard, so
    // this is a coherent mutation sample. Unlike a plain get, an underflow here
    // is not a publish-before-clear tear and must not be hidden by a read floor.
    let merged_prev = super::apply_delta_pure(base, net, max_lsn)?;
    if zero_decref_is_noop && delta < 0 && merged_prev.rc == 0 {
        return Ok((0, 0));
    }
    let (post, skipped) = super::apply_delta_or_skip(merged_prev, delta, lsn)?;
    if skipped {
        super::note_decref_underflow_skip(delta, lsn, merged_prev.rc, context);
        return Ok((merged_prev.rc, merged_prev.rc));
    }
    open.merge(pba, delta, lsn);
    Ok((merged_prev.rc, post.rc))
}

/// Number of BFG ring slots. Matches `crate::db::l2p_buffer::BFG_SIZE`.
const BFG_SLOTS: usize = 4;
const ALL_SLOT_MASK: u8 = (1 << BFG_SLOTS) - 1;

#[inline]
fn slot_index(bfg: Bfg) -> usize {
    (bfg as usize) & (BFG_SLOTS - 1)
}

struct FoldEpochGuard<'a> {
    epoch: &'a AtomicU64,
}

impl Drop for FoldEpochGuard<'_> {
    fn drop(&mut self) {
        self.epoch.fetch_add(1, Ordering::Release);
    }
}

#[cfg(test)]
struct StageBatchTestHook {
    after_lookup: std::sync::Barrier,
    resume: std::sync::Barrier,
    lookup_count: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
struct StreamingCheckpointTestHook {
    pause_after_first_chunk: bool,
    after_first_chunk: std::sync::Barrier,
    resume: std::sync::Barrier,
    chunks: std::sync::atomic::AtomicUsize,
    max_chunk_pages: std::sync::atomic::AtomicUsize,
    max_overlay_pages: std::sync::atomic::AtomicUsize,
    page_weaks: Mutex<Vec<std::sync::Weak<crate::page::Page>>>,
    fail_before_write_chunk: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl StreamingCheckpointTestHook {
    fn new(pause_after_first_chunk: bool) -> Self {
        Self {
            pause_after_first_chunk,
            after_first_chunk: std::sync::Barrier::new(2),
            resume: std::sync::Barrier::new(2),
            chunks: std::sync::atomic::AtomicUsize::new(0),
            max_chunk_pages: std::sync::atomic::AtomicUsize::new(0),
            max_overlay_pages: std::sync::atomic::AtomicUsize::new(0),
            page_weaks: Mutex::new(Vec::new()),
            fail_before_write_chunk: std::sync::atomic::AtomicUsize::new(usize::MAX),
        }
    }

    fn fail_before_write_chunk(&self, chunk: usize) {
        self.fail_before_write_chunk.store(chunk, Ordering::SeqCst);
    }

    fn before_write(&self) -> Result<()> {
        let chunk = self.chunks.load(Ordering::SeqCst);
        if self.fail_before_write_chunk.load(Ordering::SeqCst) == chunk {
            return Err(MetaDbError::InjectedFault(
                "refcount.streaming_chunk_write.before",
            ));
        }
        Ok(())
    }

    fn capture_staged(&self, staged: &StagedDeltas, overlay_pages: usize) {
        self.max_chunk_pages
            .fetch_max(staged.pages.len(), Ordering::Relaxed);
        self.max_overlay_pages
            .fetch_max(overlay_pages, Ordering::Relaxed);
        self.page_weaks
            .lock()
            .extend(staged.pages.iter().map(|page| Arc::downgrade(&page.sealed)));
    }

    fn after_chunk(&self) {
        if self.chunks.fetch_add(1, Ordering::SeqCst) == 0 && self.pause_after_first_chunk {
            self.after_first_chunk.wait();
            self.resume.wait();
        }
    }
}

#[cfg(test)]
impl StageBatchTestHook {
    fn new() -> Self {
        Self {
            after_lookup: std::sync::Barrier::new(2),
            resume: std::sync::Barrier::new(2),
            lookup_count: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn after_lookup(&self) {
        if self.lookup_count.fetch_add(1, Ordering::SeqCst) == 0 {
            self.after_lookup.wait();
            self.resume.wait();
        }
    }
}

pub struct RcShard {
    /// Pending deltas keyed by BFG ring slot (`bfg & 3`). A commit
    /// stamped to BFG `t` merges into `delta_slots[t & 3]`; the sync
    /// folds only the Syncing slot. Reads sum across all four (refcount
    /// is cumulative).
    delta_slots: [Mutex<DeltaMap>; BFG_SLOTS],
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
    /// The fold takes this in write mode across the in-memory stage + publish +
    /// clear (durable page writes still happen later); the consistent read and
    /// `stage_batch` and single-item `stage` validation take it in read mode.
    /// The cheap single-item `get` path does not touch it.
    fold_lock: parking_lot::RwLock<()>,
    /// Sequence for representation moves between `delta_slots` and `array`.
    /// `stage_batch` reads base pages without slot locks, then validates this
    /// under `fold_lock.read()` before accepting the matching slot snapshot.
    fold_epoch: AtomicU64,
    #[cfg(test)]
    stage_batch_test_hook: Mutex<Option<Arc<StageBatchTestHook>>>,
    #[cfg(test)]
    streaming_checkpoint_test_hook: Mutex<Option<Arc<StreamingCheckpointTestHook>>>,
}

/// Refcount checkpoint carried into the manifest phase. Cold checkpoints keep
/// sealed pages for the global write batch; production streaming checkpoints
/// keep only compact page metadata after writing each bounded chunk. Both carry
/// the page-table snapshots needed by `paged_meta::write_chain`.
pub struct RcCheckpoint {
    pub(super) staged: StagedDeltas,
    /// Streaming hot-path pages after their write CQEs completed. Only compact
    /// allocation metadata remains; page payload Arcs were dropped per chunk.
    streamed_pages: Vec<StagedPageMeta>,
    /// Time this shard checkpoint spent waiting to acquire `fold_lock.write()`.
    /// The lifecycle sums this value across selected shards for one checkpoint.
    fold_lock_wait_us: u64,
    /// Time spent folding array/slot state, excluding streaming page writes.
    /// Starts only after `fold_lock.write()` has been acquired.
    fold_service_us: u64,
    streaming_write_stats: RcStreamingWriteStats,
    streaming: bool,
    /// The slot's drained entries + which slot they came from — restored
    /// by `abort_checkpoint` so a retry redoes the fold.
    drained_deltas: Vec<(Pba, Pending)>,
    /// Slot indices folded by this checkpoint (one for the per-BFG path,
    /// up to four for `begin_checkpoint_all_slots`). Abort restores
    /// `drained_deltas` into the first of these (they were merged on the
    /// way in, so a single restore slot is sufficient and correct — the
    /// next fold re-spreads them is unnecessary because abort only needs
    /// the deltas back in *some* live slot to be re-folded).
    restore_slot: usize,
    snapshot_page_table: Vec<PageId>,
    snapshot_meta_chain: Vec<PageId>,
}

/// Service work completed by the bounded data-page writes of a streaming
/// refcount checkpoint. The lifecycle aggregates these per-shard values into
/// the public `flush_rc_stream_*` metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RcStreamingWriteStats {
    pub calls: u64,
    pub pages: u64,
    pub service_us: u64,
    pub max_chunk_us: u64,
    pub max_chunk_pages: u64,
}

impl RcStreamingWriteStats {
    fn record_chunk(&mut self, pages: usize, elapsed: Duration) {
        let elapsed_us = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
        self.calls = self.calls.saturating_add(1);
        self.pages = self.pages.saturating_add(pages as u64);
        self.service_us = self.service_us.saturating_add(elapsed_us);
        self.max_chunk_us = self.max_chunk_us.max(elapsed_us);
        self.max_chunk_pages = self.max_chunk_pages.max(pages as u64);
    }

    pub(crate) fn merge(&mut self, other: Self) {
        self.calls = self.calls.saturating_add(other.calls);
        self.pages = self.pages.saturating_add(other.pages);
        self.service_us = self.service_us.saturating_add(other.service_us);
        self.max_chunk_us = self.max_chunk_us.max(other.max_chunk_us);
        self.max_chunk_pages = self.max_chunk_pages.max(other.max_chunk_pages);
    }
}

impl RcCheckpoint {
    /// Empty checkpoint — fast path when nothing was drained / staged.
    pub fn is_empty(&self) -> bool {
        self.staged.is_empty() && self.streamed_pages.is_empty()
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
            + self.streamed_pages.iter().filter(|p| p.is_fresh).count()
    }

    pub fn data_pages_count(&self) -> usize {
        self.staged.pages.len() + self.streamed_pages.len()
    }

    pub(crate) fn streaming_write_stats(&self) -> RcStreamingWriteStats {
        self.streaming_write_stats
    }

    pub(crate) fn fold_lock_wait_us(&self) -> u64 {
        self.fold_lock_wait_us
    }

    pub(crate) fn fold_service_us(&self) -> u64 {
        self.fold_service_us
    }

    #[cfg(test)]
    pub(crate) fn fresh_page_ids(&self) -> Vec<(usize, PageId)> {
        self.staged
            .pages
            .iter()
            .filter(|p| p.is_fresh)
            .map(|p| (p.page_idx, p.page_id))
            .chain(
                self.streamed_pages
                    .iter()
                    .filter(|p| p.is_fresh)
                    .map(|p| (p.page_idx, p.page_id)),
            )
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn touched_existing_page_ids(&self) -> Vec<PageId> {
        self.staged
            .pages
            .iter()
            .filter(|p| !p.is_fresh)
            .map(|p| p.page_id)
            .chain(
                self.streamed_pages
                    .iter()
                    .filter(|p| !p.is_fresh)
                    .map(|p| p.page_id),
            )
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
            fold_epoch: AtomicU64::new(0),
            #[cfg(test)]
            stage_batch_test_hook: Mutex::new(None),
            #[cfg(test)]
            streaming_checkpoint_test_hook: Mutex::new(None),
        }
    }

    pub fn meta_page_id(&self) -> PageId {
        self.array.meta_page_id()
    }

    pub(crate) fn warmup_data_pages(&self) -> Result<u64> {
        self.array.warmup_data_pages()
    }

    /// Logical refcount. Sums pending across all four slots, falls back
    /// to the on-disk array, floors a transient underflow to 0.
    pub fn get(&self, pba: Pba) -> Result<u32> {
        Ok(self.lookup_entry(pba)?.rc)
    }

    /// Batched hot-path lookup for PBAs routed to this shard. Each pending
    /// BFG slot is locked once and backing refcount pages are fetched through
    /// one cache multi-get.
    pub(crate) fn get_many(&self, pbas: &[Pba]) -> Result<Vec<u32>> {
        let bases = self.array.get_many_with_page_lsn(pbas)?;
        let mut net = vec![0i64; pbas.len()];
        let mut max_lsn = vec![0u64; pbas.len()];
        let mut any = vec![false; pbas.len()];
        for slot in &self.delta_slots {
            let slot = slot.lock();
            for (idx, &pba) in pbas.iter().enumerate() {
                if let Some(pending) = slot.get(pba) {
                    net[idx] += pending.delta;
                    max_lsn[idx] = max_lsn[idx].max(pending.last_lsn);
                    any[idx] = true;
                }
            }
        }
        bases
            .into_iter()
            .enumerate()
            .map(|(idx, (base, _))| {
                if any[idx] {
                    super::merge_read_or_floor(base, net[idx], max_lsn[idx]).map(|entry| entry.rc)
                } else {
                    Ok(base.rc)
                }
            })
            .collect()
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

    /// Batched fold-consistent read for PBAs that all route to THIS shard.
    /// Takes `fold_lock` in read mode ONCE for the whole batch and fills
    /// `out[i] = rc(pbas[idxs[i]])` for each `idxs` entry. Equivalent to
    /// calling [`get_consistent`] per PBA, but the lock is amortized: the GC
    /// reclaim gate reads up to a full per-cycle block budget, and a per-PBA
    /// `fold_lock` acquisition (O(pbas)) contended the BFG fold under sustained
    /// reclaim, making reclaim cost super-linear in retired depth. One read
    /// guard per shard bounds the fold-blocking window to this shard's slice.
    /// Consistency is unchanged — even stronger: every PBA in the batch
    /// observes one coherent `array.base ⊕ Σ pending` fold state.
    pub fn get_consistent_into(&self, pbas: &[Pba], idxs: &[usize], out: &mut [u32]) -> Result<()> {
        let _read = self.fold_lock.read();
        for &idx in idxs {
            out[idx] = self.lookup_entry(pbas[idx])?.rc;
        }
        Ok(())
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

    /// Stage one op into the pending delta for `bfg`'s slot. Returns the
    /// cumulative `(prev_rc, new_rc)` (callers surface `freed_pba` on
    /// `new == 0 && prev > 0`).
    ///
    /// A decref past zero is a benign double-decref — skipped (count
    /// left at its floor) rather than poisoning the commit pipeline.
    /// Overflow (delta >= 0) is fatal.
    pub fn stage(&self, bfg: Bfg, pba: Pba, delta: i64, lsn: Lsn) -> Result<(u32, u32)> {
        self.stage_inner(bfg, pba, delta, lsn, true, false)
    }

    /// Coherently decrement one live reference, or return `(0, 0)` without
    /// recording an underflow when the PBA is already at zero.
    ///
    /// This is the mutation-safe replacement for `get(pba) > 0` followed by
    /// `stage(-1)`: a plain get can tear across checkpoint publish-before-clear
    /// and spuriously return zero, while this decision is made from the same
    /// fold-epoch-validated base+slots snapshot used for the mutation.
    pub(crate) fn stage_decref_if_positive(
        &self,
        bfg: Bfg,
        pba: Pba,
        lsn: Lsn,
    ) -> Result<(u32, u32)> {
        self.stage_inner(bfg, pba, -1, lsn, true, true)
    }

    /// Batch [`Self::stage`] for strictly increasing PBAs routed to this shard.
    /// All four BFG slots and the refcount page table are sampled once, then
    /// the Open-slot deltas are merged under the same lock set. This preserves
    /// per-PBA replay-skip and underflow semantics while eliminating the six
    /// mutex/cache lookups previously paid for every remap action.
    pub(crate) fn stage_batch(
        &self,
        bfg: Bfg,
        actions: &[(Pba, i64)],
        lsn: Lsn,
    ) -> Result<(Vec<(u32, u32)>, RefcountApplyStageTimings)> {
        if actions.is_empty() {
            return Ok((Vec::new(), RefcountApplyStageTimings::default()));
        }
        debug_assert!(
            actions.windows(2).all(|pair| pair[0].0 < pair[1].0),
            "RcShard::stage_batch requires strictly increasing PBAs"
        );

        // Sample by commit LSN, not by PBA. A sampled commit therefore keeps
        // its complete per-shard PBA shape while only about 1/16 commits pay
        // the per-action timestamp cost.
        let sample_breakdown = sample_refcount_breakdown(lsn);
        let pbas: Vec<Pba> = actions.iter().map(|(pba, _)| *pba).collect();
        let open_idx = slot_index(bfg);
        let mut base_page_lookup = Duration::ZERO;
        let mut fold_lock_wait = Duration::ZERO;
        let mut slot_lock_wait = Duration::ZERO;
        let mut base_lookup_attempts = 0u64;
        let mut epoch_retries = 0u64;
        let (bases, mut slots, active_slots) = loop {
            let epoch_before = self.fold_epoch.load(Ordering::Acquire);
            base_lookup_attempts = base_lookup_attempts.saturating_add(1);
            let lookup_started = sample_breakdown.then(Instant::now);
            let bases = self.array.get_many_sorted_with_page_lsn(&pbas)?;
            base_page_lookup += lookup_started.map_or(Duration::ZERO, |start| start.elapsed());

            #[cfg(test)]
            let test_hook = self.stage_batch_test_hook.lock().clone();
            #[cfg(test)]
            if let Some(hook) = test_hook {
                hook.after_lookup();
            }

            let fold_lock_started = sample_breakdown.then(Instant::now);
            let fold = self.fold_lock.read();
            fold_lock_wait += fold_lock_started.map_or(Duration::ZERO, |start| start.elapsed());
            if self.fold_epoch.load(Ordering::Acquire) != epoch_before {
                epoch_retries = epoch_retries.saturating_add(1);
                drop(fold);
                continue;
            }

            // Acquiring every slot while the fold read guard is held closes
            // the validation-to-snapshot gap. Once the guards are ours, a new
            // checkpoint may publish its array page but cannot clear the
            // corresponding slot until this batch finishes its merge.
            let mut slots = Vec::with_capacity(BFG_SLOTS);
            let slot_lock_started = sample_breakdown.then(Instant::now);
            slots.extend(self.delta_slots.iter().map(|slot| slot.lock()));
            slot_lock_wait += slot_lock_started.map_or(Duration::ZERO, |start| start.elapsed());
            // Every non-empty slot contributes to cumulative RC, including a
            // zero-net entry whose last_lsn suppresses replay skipping. Empty
            // slots are stable while their guards are held. Keep the open slot
            // active even when initially empty because this batch mutates it;
            // the other empty maps cannot gain entries while locked.
            let active_slots = active_slot_mask(&slots, open_idx);
            drop(fold);
            break (bases, slots, active_slots);
        };

        let mut out = Vec::with_capacity(actions.len());
        let mut pending_slot_scan = Duration::ZERO;
        let mut delta_merge = Duration::ZERO;

        if sample_breakdown {
            for (action_idx, &(pba, delta)) in actions.iter().enumerate() {
                let scan_started = Instant::now();
                let (net, max_lsn, any) = scan_active_locked_pending(&slots, active_slots, pba);
                pending_slot_scan += scan_started.elapsed();

                let (base, page_lsn) = bases[action_idx];
                let merge_started = Instant::now();
                let merged = merge_staged_action(
                    &mut slots[open_idx],
                    pba,
                    delta,
                    lsn,
                    true,
                    base,
                    page_lsn,
                    net,
                    max_lsn,
                    any,
                    "stage_batch",
                    false,
                );
                delta_merge += merge_started.elapsed();
                out.push(merged?);
            }
        } else {
            for (action_idx, &(pba, delta)) in actions.iter().enumerate() {
                let (net, max_lsn, any) = scan_active_locked_pending(&slots, active_slots, pba);
                let (base, page_lsn) = bases[action_idx];
                out.push(merge_staged_action(
                    &mut slots[open_idx],
                    pba,
                    delta,
                    lsn,
                    true,
                    base,
                    page_lsn,
                    net,
                    max_lsn,
                    any,
                    "stage_batch",
                    false,
                )?);
            }
        }
        Ok((
            out,
            RefcountApplyStageTimings {
                base_page_lookup,
                fold_lock_wait,
                slot_lock_wait,
                pending_slot_scan,
                delta_merge,
                base_lookup_attempts,
                epoch_retries,
                sampled_pbas: sample_breakdown
                    .then_some(actions.len() as u64)
                    .unwrap_or(0),
            },
        ))
    }

    /// Stage one op into `bfg`'s slot WITHOUT the per-op `page_lsn >= lsn`
    /// replay-skip early-return that [`stage`](Self::stage) applies, for
    /// NON-WAL callers (the snapshot-take root incref / drop decref /
    /// tree-create root +1 relocated from the page header in
    /// `l2p_page_rc`). These ops are not WAL-replayed, so the early-return
    /// (which would skip a delta whose array page is already folded at
    /// `lsn`) is wrong for them — they must always merge.
    ///
    /// `lsn` is still recorded as the delta's `last_lsn` so the FOLD's own
    /// replay-skip (`stage_one_page`: `page_generation >= last_lsn`) keeps
    /// the delta from being double-applied on a checkpoint retry, AND so
    /// the fold actually applies it (a `last_lsn = 0` delta would be
    /// permanently skipped by `page_generation >= 0`). Callers pass a real,
    /// monotone lsn: the snapshot's `created_lsn`, the drop's lsn, or the
    /// volume's `created_lsn` for the create root. Because these ops sit at
    /// or below the next durable `checkpoint_lsn`, raising the array page
    /// generation to `lsn` never poisons a future op (which has a strictly
    /// higher lsn) or a replayed op (which starts above the checkpoint).
    /// Returns `(prev_rc, new_rc)`.
    pub fn stage_unskippable(
        &self,
        bfg: Bfg,
        pba: Pba,
        delta: i64,
        lsn: Lsn,
    ) -> Result<(u32, u32)> {
        self.stage_inner(bfg, pba, delta, lsn, false, false)
    }

    /// Shared body for [`stage`] / [`stage_unskippable`]. `replay_skip`
    /// gates the `!any && page_lsn >= lsn` early-return (on for WAL ops,
    /// off for the non-WAL structural deltas). Returns the cumulative
    /// `(prev_rc, new_rc)`; a decref past zero is a benign double-decref
    /// left at its floor (overflow on `delta >= 0` is fatal).
    fn stage_inner(
        &self,
        bfg: Bfg,
        pba: Pba,
        delta: i64,
        lsn: Lsn,
        replay_skip: bool,
        zero_decref_is_noop: bool,
    ) -> Result<(u32, u32)> {
        let open_idx = slot_index(bfg);
        loop {
            let epoch_before = self.fold_epoch.load(Ordering::Acquire);
            let (base, page_lsn) = self.array.get_with_page_lsn(pba)?;

            #[cfg(test)]
            let test_hook = self.stage_batch_test_hook.lock().clone();
            #[cfg(test)]
            if let Some(hook) = test_hook {
                hook.after_lookup();
            }

            let fold = self.fold_lock.read();
            if self.fold_epoch.load(Ordering::Acquire) != epoch_before {
                drop(fold);
                continue;
            }

            // Close the validation-to-slot-snapshot gap exactly as
            // `stage_batch` does. A checkpoint cannot publish+clear while the
            // read guard is held, and after all slot guards are acquired it
            // cannot clear the matching pending delta until this merge ends.
            let mut slots: Vec<_> = self.delta_slots.iter().map(|slot| slot.lock()).collect();
            drop(fold);
            let (net, max_lsn, any) = scan_active_locked_pending(&slots, ALL_SLOT_MASK, pba);
            return merge_staged_action(
                &mut slots[open_idx],
                pba,
                delta,
                lsn,
                replay_skip,
                base,
                page_lsn,
                net,
                max_lsn,
                any,
                "stage",
                zero_decref_is_noop,
            );
        }
    }

    /// Fold ONLY the Syncing slot (`bfg & 3`). Caller has frozen the slot
    /// by promoting `bfg` to Syncing (`inflight == 0`). After the fold rc
    /// is durable up to that BFG's `wal_checkpoint` (the slot held exactly
    /// this BFG's deltas), so the flush's `wal_checkpoint.max(prev)`
    /// durable_seq is correct — no separate watermark to record.
    pub fn begin_checkpoint(&self, bfg: Bfg) -> Result<RcCheckpoint> {
        self.checkpoint_slots(&[slot_index(bfg)], false, &[])
    }

    /// Production threads-on refcount checkpoint. The frozen Syncing slot is
    /// folded in bounded data-page chunks. Each chunk performs the in-memory
    /// publish + exact slot removal under `fold_lock.write()`, then writes its
    /// sealed pages outside the lock and drops every payload Arc as soon as the
    /// write CQEs complete. The checkpoint retained by the manifest phase is
    /// compact metadata only.
    pub(crate) fn begin_checkpoint_streaming(&self, bfg: Bfg) -> Result<RcCheckpoint> {
        self.begin_checkpoint_streaming_capped(bfg, STAGE_BASE_READ_BATCH_PAGES)
    }

    fn begin_checkpoint_streaming_capped(
        &self,
        bfg: Bfg,
        max_unique_pages: usize,
    ) -> Result<RcCheckpoint> {
        let restore_slot = slot_index(bfg);
        let mut drained: Vec<(Pba, Pending)> = {
            let slot = self.delta_slots[restore_slot].lock();
            slot.iter().map(|(pba, pending)| (*pba, *pending)).collect()
        };
        if drained.is_empty() {
            return Ok(RcCheckpoint {
                staged: StagedDeltas {
                    pages: Vec::new(),
                    max_lsn: 0,
                },
                streamed_pages: Vec::new(),
                fold_lock_wait_us: 0,
                fold_service_us: 0,
                streaming_write_stats: RcStreamingWriteStats::default(),
                streaming: true,
                drained_deltas: Vec::new(),
                restore_slot,
                snapshot_page_table: self.array.page_table_snapshot(),
                snapshot_meta_chain: self.array.meta_chain_snapshot(),
            });
        }

        drained.sort_by_key(|(pba, _)| (*pba as usize) / ENTRIES_PER_PAGE);
        let max_unique_pages = max_unique_pages.max(1);
        let mut cursor = 0;
        let mut streamed_pages = Vec::new();
        let mut fold_lock_wait_us = 0u64;
        let mut fold_service_us = 0u64;
        let mut streaming_write_stats = RcStreamingWriteStats::default();

        #[cfg(test)]
        let test_hook = self.streaming_checkpoint_test_hook.lock().clone();

        while cursor < drained.len() {
            let mut end = cursor;
            let mut page_count = 0;
            while end < drained.len() && page_count < max_unique_pages {
                let page_idx = (drained[end].0 as usize) / ENTRIES_PER_PAGE;
                page_count += 1;
                end += 1;
                while end < drained.len()
                    && (drained[end].0 as usize) / ENTRIES_PER_PAGE == page_idx
                {
                    end += 1;
                }
            }

            let fold_lock_wait_started = Instant::now();
            let fold = self.fold_lock.write();
            fold_lock_wait_us = fold_lock_wait_us.saturating_add(
                fold_lock_wait_started
                    .elapsed()
                    .as_micros()
                    .min(u128::from(u64::MAX)) as u64,
            );
            let fold_started = Instant::now();
            let epoch_guard = FoldEpochGuard {
                epoch: &self.fold_epoch,
            };

            let staged_result = (|| {
                // The Syncing slot is frozen. Validate the exact snapshot before
                // publishing so a violated BFG invariant cannot make us delete a
                // newer or unrelated pending delta.
                {
                    let slot = self.delta_slots[restore_slot].lock();
                    for &(pba, pending) in &drained[cursor..end] {
                        if slot.get(pba) != Some(pending) {
                            return Err(MetaDbError::Corruption(format!(
                                "streaming refcount checkpoint slot changed for pba {pba}"
                            )));
                        }
                    }
                }

                // `stage_deltas_in_memory_preserving` is failure-atomic for this
                // chunk: on error it removes every overlay/fresh reservation it
                // published. The slot is still intact until the call succeeds.
                let staged = self.array.stage_deltas_in_memory_preserving(
                    &mut drained[cursor..end],
                    false,
                    &[],
                )?;

                let mut slot = self.delta_slots[restore_slot].lock();
                let mut removed = Vec::with_capacity(end - cursor);
                for &(pba, pending) in &drained[cursor..end] {
                    match slot.remove(pba) {
                        Some(actual) if actual == pending => removed.push((pba, actual)),
                        actual => {
                            if let Some(actual) = actual {
                                slot.merge_pending(pba, actual);
                            }
                            for (removed_pba, removed_pending) in removed {
                                slot.merge_pending(removed_pba, removed_pending);
                            }
                            drop(slot);
                            self.array.abort_staged_deltas(&staged, 0);
                            return Err(MetaDbError::Corruption(format!(
                                "streaming refcount checkpoint failed exact removal for pba {pba}"
                            )));
                        }
                    }
                }
                drop(slot);
                Ok(staged)
            })();
            drop(epoch_guard);
            drop(fold);
            fold_service_us =
                fold_service_us.saturating_add(
                    fold_started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64,
                );

            let staged = match staged_result {
                Ok(staged) => staged,
                Err(err) => {
                    self.abort_partial_streamed_fresh_pages(
                        restore_slot,
                        &drained,
                        &streamed_pages,
                    );
                    return Err(err);
                }
            };

            #[cfg(test)]
            if let Some(hook) = &test_hook {
                hook.capture_staged(&staged, self.array.staged_overlay_len());
            }

            // PageStore waits for all write CQEs before returning. Keep the
            // overlay through that wait, then remove it so this chunk's Arcs
            // can be dropped before the next chunk is built.
            // On write error this chunk has already been published and removed
            // from the frozen slot. It is deliberately not reconstructed in
            // place: the enclosing sync cycle poisons persistence, and every
            // subsequent non-empty commit/forced sync fails until restart.
            // Recovery starts from the prior manifest and replays the WAL.
            // The still-published overlay retains at most this bounded chunk.
            let write_started = Instant::now();
            #[cfg(test)]
            let write_result = match &test_hook {
                Some(hook) => hook
                    .before_write()
                    .and_then(|()| self.array.write_staged_page_runs(&staged)),
                None => self.array.write_staged_page_runs(&staged),
            };
            #[cfg(not(test))]
            let write_result = self.array.write_staged_page_runs(&staged);
            if let Err(err) = write_result {
                streamed_pages.extend(staged.compact_pages());
                self.abort_partial_streamed_fresh_pages(restore_slot, &drained, &streamed_pages);
                return Err(err);
            }
            streaming_write_stats.record_chunk(staged.pages.len(), write_started.elapsed());
            let compact = staged.compact_pages();
            self.array.clear_staged(&staged);
            streamed_pages.extend(compact);
            drop(staged);

            #[cfg(test)]
            if let Some(hook) = &test_hook {
                hook.after_chunk();
            }

            cursor = end;
        }

        Ok(RcCheckpoint {
            staged: StagedDeltas {
                pages: Vec::new(),
                max_lsn: 0,
            },
            streamed_pages,
            fold_lock_wait_us,
            fold_service_us,
            streaming_write_stats,
            streaming: true,
            drained_deltas: drained,
            restore_slot,
            snapshot_page_table: self.array.page_table_snapshot(),
            snapshot_meta_chain: self.array.meta_chain_snapshot(),
        })
    }

    /// A streaming sample can fail after earlier chunks completed their data
    /// writes but before any meta-chain page was submitted. Existing page ids
    /// remain valid in place; fresh ids are unreachable from the stable chain
    /// and must be detached, freed, and restored to the frozen slot.
    fn abort_partial_streamed_fresh_pages(
        &self,
        restore_slot: usize,
        drained: &[(Pba, Pending)],
        pages: &[StagedPageMeta],
    ) {
        let mut fresh_page_idxs: Vec<usize> = pages
            .iter()
            .filter(|page| page.is_fresh)
            .map(|page| page.page_idx)
            .collect();
        if fresh_page_idxs.is_empty() {
            return;
        }
        fresh_page_idxs.sort_unstable();
        fresh_page_idxs.dedup();

        let fold = self.fold_lock.write();
        let epoch_guard = FoldEpochGuard {
            epoch: &self.fold_epoch,
        };
        self.array.abort_streamed_fresh_pages(pages, 0);
        let mut slot = self.delta_slots[restore_slot].lock();
        for &(pba, pending) in drained {
            let page_idx = (pba as usize) / ENTRIES_PER_PAGE;
            if fresh_page_idxs.binary_search(&page_idx).is_ok() && slot.get(pba).is_none() {
                slot.merge_pending(pba, pending);
            }
        }
        drop(slot);
        drop(epoch_guard);
        drop(fold);
    }

    /// Like [`Self::begin_checkpoint`] but additionally force-increfs
    /// `force_increfs` (an unconditional `+1` that always applies and does
    /// NOT raise any data page's generation). Used by the L2P-page-rc fold for
    /// snapshot-root increfs: the incref rides this cycle's page-rc checkpoint,
    /// durable atomically with the manifest commit that records the
    /// `SnapshotEntry`. See
    /// [`PagedRefcountArray::stage_deltas_in_memory_with_force_increfs`].
    pub fn begin_checkpoint_with_increfs(
        &self,
        bfg: Bfg,
        force_increfs: &[Pba],
    ) -> Result<RcCheckpoint> {
        self.checkpoint_slots(&[slot_index(bfg)], false, force_increfs)
    }

    /// Fold every slot. Used by the threads-OFF inline flush and by
    /// recovery's post-replay path (rc analogue of
    /// `force_compact_l2p_buffers`). `force` bypasses the per-fold
    /// replay-skip — `true` only on the cold `RcShard::flush` lifecycle
    /// path (see [`PagedRefcountArray::stage_deltas_in_memory_force`]);
    /// the hot run_sync_cycle drainer passes `false`.
    pub fn begin_checkpoint_all_slots(&self, force: bool) -> Result<RcCheckpoint> {
        self.checkpoint_slots(&[0, 1, 2, 3], force, &[])
    }

    /// All-slot fold (threads-OFF inline flush) that also force-increfs
    /// `force_increfs` — the threads-off analogue of
    /// [`Self::begin_checkpoint_with_increfs`].
    pub fn begin_checkpoint_all_slots_with_increfs(
        &self,
        force: bool,
        force_increfs: &[Pba],
    ) -> Result<RcCheckpoint> {
        self.checkpoint_slots(&[0, 1, 2, 3], force, force_increfs)
    }

    /// Shared fold body (publish-before-clear). `force_increfs` is the
    /// unconditional-`+1` set (snapshot-root incref) folded alongside the
    /// drained slot deltas — see [`Self::begin_checkpoint_with_increfs`].
    fn checkpoint_slots(
        &self,
        slots: &[usize],
        force: bool,
        force_increfs: &[Pba],
    ) -> Result<RcCheckpoint> {
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

        if drained.is_empty() && force_increfs.is_empty() {
            return Ok(RcCheckpoint {
                staged: StagedDeltas {
                    pages: Vec::new(),
                    max_lsn: 0,
                },
                streamed_pages: Vec::new(),
                fold_lock_wait_us: 0,
                fold_service_us: 0,
                streaming_write_stats: RcStreamingWriteStats::default(),
                streaming: false,
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
        // still can, by design — see the field doc). Durable page writes happen
        // later, outside this lock. Building staged pages may still need base
        // reads, which is why `stage_batch` never holds the read side across its
        // own base-page lookup.
        let fold_lock_wait_started = Instant::now();
        let fold = self.fold_lock.write();
        let fold_lock_wait_us = fold_lock_wait_started
            .elapsed()
            .as_micros()
            .min(u128::from(u64::MAX)) as u64;
        let fold_started = Instant::now();
        // Declared after `fold`, so `?` paths bump the epoch before releasing
        // the write guard. Readers can never validate a partially-moved view.
        let epoch_guard = FoldEpochGuard {
            epoch: &self.fold_epoch,
        };

        // Fold + publish into the array (cache + page_table) WITHOUT
        // clearing the slots yet. `force` (cold lifecycle flush) applies
        // every delta despite the per-page replay-skip generation guard;
        // `force_increfs` is the always-apply / no-gen-bump snapshot incref.
        let staged =
            self.array
                .stage_deltas_in_memory_preserving(&mut drained, force, force_increfs)?;

        // Publish is visible now; clear the folded slots. They are frozen
        // (Syncing / threads-off quiesce) so a fresh `DeltaMap` discards
        // exactly what we folded with no risk of dropping a concurrent
        // insert.
        for &i in slots {
            *self.delta_slots[i].lock() = DeltaMap::new();
        }
        drop(epoch_guard);
        drop(fold);
        let fold_service_us = fold_started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64;

        let snapshot_page_table = self.array.page_table_snapshot();
        let snapshot_meta_chain = self.array.meta_chain_snapshot();
        Ok(RcCheckpoint {
            staged,
            streamed_pages: Vec::new(),
            fold_lock_wait_us,
            fold_service_us,
            streaming_write_stats: RcStreamingWriteStats::default(),
            streaming: false,
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

    /// Roll back a checkpoint only while the global page write has not started.
    /// Cold checkpoints retain their page Arcs and restore every delta.
    /// Streaming checkpoints have already completed their data-page writes,
    /// but their stable meta-chain is still old at this phase: existing pages
    /// remain folded, while unreachable fresh pages can be freed and restored
    /// from compact metadata. Post-write errors must retain authority instead.
    pub fn abort_checkpoint(&self, ckpt: RcCheckpoint, free_lsn: Lsn) {
        if ckpt.is_empty() {
            return;
        }
        let fold = self.fold_lock.write();
        let epoch_guard = FoldEpochGuard {
            epoch: &self.fold_epoch,
        };
        self.array.abort_staged_deltas(&ckpt.staged, free_lsn);
        if ckpt.streaming {
            self.array
                .abort_streamed_fresh_pages(&ckpt.streamed_pages, free_lsn);
        }
        let fresh_page_idxs: Vec<usize> = if ckpt.streaming {
            ckpt.streamed_pages
                .iter()
                .filter(|page| page.is_fresh)
                .map(|page| page.page_idx)
                .collect()
        } else {
            Vec::new()
        };
        let mut slot = self.delta_slots[ckpt.restore_slot].lock();
        for (pba, pending) in ckpt.drained_deltas {
            if !ckpt.streaming
                || fresh_page_idxs
                    .binary_search(&((pba as usize) / ENTRIES_PER_PAGE))
                    .is_ok()
            {
                slot.merge_pending(pba, pending);
            }
        }
        drop(slot);
        drop(epoch_guard);
        drop(fold);
    }

    /// Synchronous flush for non-checkpoint callers (cold path). Folds
    /// every slot to disk and rotates the meta chain.
    pub fn flush(&self) -> Result<()> {
        // Cold path: force-apply (lifecycle-serialized, no in-place retry;
        // folds the snapshot incref whose `last_lsn == created_lsn` equals
        // the array page generation).
        let ckpt = self.begin_checkpoint_all_slots(true)?;
        if ckpt.is_empty() {
            return Ok(());
        }
        if let Err(err) = self.array.write_staged_pages(&ckpt.staged) {
            self.abort_checkpoint(ckpt, 0);
            return Err(err);
        }
        // Build is still an in-memory/allocation phase: if it fails, the
        // stable meta head was not touched and the checkpoint can be rolled
        // back. Keep this boundary explicit instead of using `write_chain`,
        // whose Err cannot say whether stable-head write submission started.
        let (new_chain, chain_pages, to_free) = match self.build_meta_chain(&ckpt) {
            Ok(built) => built,
            Err(err) => {
                self.abort_checkpoint(ckpt, 0);
                return Err(err);
            }
        };
        // After this call starts, the stable head may already reference fresh
        // data pages. Never abort/free them on error; retain the in-memory
        // authority and let the enclosing operation fail closed.
        if let Err(err) = self
            .array
            .write_built_meta_chain_external(chain_pages, to_free, 0)
        {
            drop(ckpt);
            return Err(err);
        }
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
