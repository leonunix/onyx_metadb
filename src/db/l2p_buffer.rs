//! BFG_SIZE-slot BFG ring buffer for L2P updates.
//!
//! Commits stamped to BFG `n` insert into slot `n & (BFG_SIZE - 1)`. The
//! background [`crate::db::bfg_sync::BfgSyncThread`] drains the slot
//! corresponding to the currently-Syncing BFG into the on-disk paged
//! radix tree; `flush_with_gate`'s inline path
//! ([`crate::db::Db::force_compact_l2p_buffers`]) drains every slot
//! when `bfg_threads_enabled = false`.
//!
//! ## Slot semantics
//!
//! Ring index = `bfg & (BFG_SIZE - 1)`.
//!
//! - **Open slot** (state machine's open_bfg): commits insert here.
//! - **Quiescing slot(s)** (below open_bfg): no new inserts; in-flight
//!   commits on these BFGs have not yet dropped their guards. Legacy mode:
//!   at most one Quiescing slot. Pipeline mode: a FIFO of frozen Quiescing
//!   generations.
//! - **Syncing slot** (typically open_bfg - 2): frozen; `BfgSyncThread`
//!   borrows the move-frozen immutable generation and clears it only after
//!   the folded tree view is published.
//! - Any ring slots the walk doesn't reach are Empty.
//!
//! ## Lookup ordering
//!
//! [`L2pBuffer::lookup_for_open_bfg`] walks slots in newest-first order:
//! `open & (BFG_SIZE - 1)` → `(open-1) & (BFG_SIZE - 1)` → ..., visiting at
//! most `BFG_SIZE - 1` slots. Any slots it doesn't reach are Empty and are
//! skipped.
//!
//! Tombstone semantics: a tombstone in a newer slot suppresses any
//! value in older slots; readers translate Tombstone → "absent"
//! without falling through to the tree.
//!
//! ## `compacted_lsn`
//!
//! Highest LSN whose buffer entry has been folded into the tree on
//! this shard.
//! [`crate::db::lifecycle::flush::Db::compute_min_last_flushed_lsn`]
//! and `compute_min_last_flushed_lsn_after` use it as the durability
//! lower bound for shards that took the buffered-commit path. Only
//! advanced via [`L2pBuffer::note_compacted`] (max-monotonic).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::bfg::BFG_SIZE;
use crate::paged::L2pValue;
use crate::types::{Bfg, Lba, Lsn};

const BFG_INDEX_MASK: u64 = (BFG_SIZE as u64) - 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BufferEntry {
    pub value: L2pValue,
    pub lsn: Lsn,
    pub tombstone: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BufferLookup {
    Present(L2pValue),
    Tombstone,
    Absent,
}

enum SlotState {
    Mutable(HashMap<Lba, BufferEntry>),
    Frozen {
        bfg: Bfg,
        entries: Arc<HashMap<Lba, BufferEntry>>,
    },
}

impl SlotState {
    fn len(&self) -> usize {
        match self {
            Self::Mutable(entries) => entries.len(),
            Self::Frozen { entries, .. } => entries.len(),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub struct L2pBuffer {
    slots: [Mutex<SlotState>; BFG_SIZE],
    compacted_lsn: AtomicU64,
}

impl L2pBuffer {
    pub fn new(initial_compacted_lsn: Lsn) -> Self {
        Self {
            slots: std::array::from_fn(|_| Mutex::new(SlotState::Mutable(HashMap::new()))),
            compacted_lsn: AtomicU64::new(initial_compacted_lsn),
        }
    }

    /// Insert into the slot indexed by `bfg & (BFG_SIZE - 1)`. Caller
    /// must hold a [`crate::bfg::BfgGuard`] for `bfg`, ensuring the
    /// slot's state stays Open / Quiescing for the duration of the
    /// insert.
    pub fn insert_at_bfg(&self, bfg: Bfg, lba: Lba, value: L2pValue, lsn: Lsn) {
        let mut slot = self.slots[(bfg & BFG_INDEX_MASK) as usize].lock();
        let SlotState::Mutable(entries) = &mut *slot else {
            panic!("attempted L2P insert into frozen BFG slot {bfg}");
        };
        entries.insert(
            lba,
            BufferEntry {
                value,
                lsn,
                tombstone: false,
            },
        );
    }

    pub fn insert_tombstone_at_bfg(&self, bfg: Bfg, lba: Lba, lsn: Lsn) {
        let mut slot = self.slots[(bfg & BFG_INDEX_MASK) as usize].lock();
        let SlotState::Mutable(entries) = &mut *slot else {
            panic!("attempted L2P tombstone insert into frozen BFG slot {bfg}");
        };
        entries.insert(
            lba,
            BufferEntry {
                value: L2pValue::ZERO,
                lsn,
                tombstone: true,
            },
        );
    }

    /// Apply a bucket of final LBA states while taking the Open-slot mutex
    /// once. Callers collapse repeated writes to the same LBA before calling
    /// this method; `None` represents a tombstone.
    pub fn apply_batch_at_bfg(&self, bfg: Bfg, entries: &[(Lba, Option<L2pValue>)], lsn: Lsn) {
        if entries.is_empty() {
            return;
        }
        let mut slot = self.slots[(bfg & BFG_INDEX_MASK) as usize].lock();
        let SlotState::Mutable(open) = &mut *slot else {
            panic!("attempted L2P batch insert into frozen BFG slot {bfg}");
        };
        for &(lba, value) in entries {
            open.insert(
                lba,
                BufferEntry {
                    value: value.unwrap_or(L2pValue::ZERO),
                    lsn,
                    tombstone: value.is_none(),
                },
            );
        }
    }

    /// Walk slots newest-first starting at `open_bfg`: `open_bfg & (BFG_SIZE - 1)`
    /// → `(open_bfg-1) & (BFG_SIZE - 1)` → ..., visiting at most `BFG_SIZE - 1`
    /// slots. Any ring slots it doesn't reach are Empty (state machine
    /// invariant) and are skipped.
    ///
    /// Returns the first hit; tombstones suppress fallthrough to the
    /// tree.
    pub fn lookup_for_open_bfg(&self, open_bfg: Bfg, lba: Lba) -> BufferLookup {
        // `open_bfg` is at least 1 by construction (`BfgStateMachine`
        // starts at checkpoint + 1 >= 1), but be defensive about the
        // lower-bound subtraction for slots open-1 and open-2.
        for delta in 0..(BFG_SIZE as u64 - 1) {
            let visited = open_bfg.saturating_sub(delta);
            if visited == 0 && delta > 0 {
                break;
            }
            let idx = (visited & BFG_INDEX_MASK) as usize;
            let slot = self.slots[idx].lock();
            let entry = match &*slot {
                SlotState::Mutable(entries) => entries.get(&lba).copied(),
                SlotState::Frozen { entries, .. } => entries.get(&lba).copied(),
            };
            if let Some(entry) = entry {
                return entry_to_lookup(entry);
            }
        }
        BufferLookup::Absent
    }

    /// Batched form of [`Self::lookup_for_open_bfg`]. Each live BFG slot is
    /// locked once for the whole bucket, preserving newest-first and
    /// tombstone semantics while amortizing the mutex traffic of random
    /// write batches.
    pub fn lookup_many_for_open_bfg(&self, open_bfg: Bfg, lbas: &[Lba]) -> Vec<BufferLookup> {
        let mut out = vec![BufferLookup::Absent; lbas.len()];
        let mut unresolved: Vec<usize> = (0..lbas.len()).collect();
        for delta in 0..(BFG_SIZE as u64 - 1) {
            if unresolved.is_empty() {
                break;
            }
            let visited = open_bfg.saturating_sub(delta);
            if visited == 0 && delta > 0 {
                break;
            }
            let idx = (visited & BFG_INDEX_MASK) as usize;
            let slot = self.slots[idx].lock();
            unresolved.retain(|&out_idx| {
                let entry = match &*slot {
                    SlotState::Mutable(entries) => entries.get(&lbas[out_idx]).copied(),
                    SlotState::Frozen { entries, .. } => entries.get(&lbas[out_idx]).copied(),
                };
                if let Some(entry) = entry {
                    out[out_idx] = entry_to_lookup(entry);
                    false
                } else {
                    true
                }
            });
        }
        out
    }

    /// Drain the slot indexed by `bfg & (BFG_SIZE - 1)`. Caller (the
    /// `BfgSyncThread`) must have already promoted the slot to Syncing
    /// in the state machine — that's what makes the move safe: no new
    /// commits can target the slot while it's Syncing.
    pub fn take_syncing_slot(&self, bfg: Bfg) -> HashMap<Lba, BufferEntry> {
        let mut slot = self.slots[(bfg & BFG_INDEX_MASK) as usize].lock();
        match std::mem::replace(&mut *slot, SlotState::Mutable(HashMap::new())) {
            SlotState::Mutable(entries) => entries,
            SlotState::Frozen { entries, .. } => {
                Arc::try_unwrap(entries).unwrap_or_else(|entries| (*entries).clone())
            }
        }
    }

    /// Move a Syncing slot into an immutable generation and borrow it by Arc.
    /// The slot retains the Arc for lookups until [`finish_syncing_slot`] runs,
    /// preserving publish-before-clear without cloning the whole HashMap.
    /// Repeated calls for a failed/retried BFG return the same generation.
    pub fn borrow_syncing_slot(&self, bfg: Bfg) -> Arc<HashMap<Lba, BufferEntry>> {
        let mut slot = self.slots[(bfg & BFG_INDEX_MASK) as usize].lock();
        match &*slot {
            SlotState::Frozen {
                bfg: frozen_bfg,
                entries,
            } => {
                assert_eq!(
                    *frozen_bfg, bfg,
                    "BFG slot reused before frozen drain completed"
                );
                return entries.clone();
            }
            SlotState::Mutable(_) => {}
        }
        let mutable = match std::mem::replace(&mut *slot, SlotState::Mutable(HashMap::new())) {
            SlotState::Mutable(entries) => entries,
            SlotState::Frozen { .. } => unreachable!(),
        };
        let entries = Arc::new(mutable);
        *slot = SlotState::Frozen {
            bfg,
            entries: entries.clone(),
        };
        entries
    }

    /// Clear a successfully folded generation after its tree read-view has
    /// been published. If a lifecycle drain already consumed the slot, this is
    /// a no-op; pointer identity prevents clearing a different generation.
    pub fn finish_syncing_slot(&self, bfg: Bfg, borrowed: &Arc<HashMap<Lba, BufferEntry>>) {
        let mut slot = self.slots[(bfg & BFG_INDEX_MASK) as usize].lock();
        match &*slot {
            SlotState::Frozen {
                bfg: frozen_bfg,
                entries,
            } => {
                assert_eq!(*frozen_bfg, bfg, "finishing the wrong frozen BFG");
                assert!(
                    Arc::ptr_eq(entries, borrowed),
                    "frozen BFG generation changed"
                );
                *slot = SlotState::Mutable(HashMap::new());
            }
            SlotState::Mutable(entries) if entries.is_empty() => {}
            SlotState::Mutable(_) => {
                panic!("BFG slot {bfg} was reused before frozen generation finished")
            }
        }
    }

    /// Visit every live (non-tombstone) entry across all ring slots. The
    /// caller tolerates duplicates and stale-superseded entries across
    /// slots — used by the reclaim reference check
    /// ([`crate::db::Db::scan_l2p_buffer_values`]), which only ORs
    /// references and conservatively over-retains. Each slot is cloned
    /// under its lock and iterated unlocked so a concurrent commit isn't
    /// blocked for the duration of the callback.
    pub fn for_each_live<F: FnMut(Lba, L2pValue)>(&self, mut f: F) {
        for slot in &self.slots {
            let snap = {
                let slot = slot.lock();
                match &*slot {
                    SlotState::Mutable(entries) => Arc::new(entries.clone()),
                    SlotState::Frozen { entries, .. } => entries.clone(),
                }
            };
            for (&lba, &entry) in snap.iter() {
                if !entry.tombstone {
                    f(lba, entry.value);
                }
            }
        }
    }

    /// Drain every slot and merge into a single map. Caller (the
    /// `flush_with_gate` inline path under `apply_gate.write()`) is
    /// the sole writer in this case — no commit can be stamping a BFG
    /// concurrently. Used when `bfg_threads_enabled = false` and by
    /// snapshot / range_delete to fold every buffer entry into the
    /// tree before reading it.
    ///
    /// Conflicts (same LBA across slots) resolve by highest LSN, so
    /// the post-drain tree state matches the lookup order
    /// (newest-first across the ring's slots).
    pub fn drain_all_slots(&self) -> HashMap<Lba, BufferEntry> {
        let mut merged: HashMap<Lba, BufferEntry> = HashMap::new();
        for slot in &self.slots {
            let mut slot = slot.lock();
            let drained = match std::mem::replace(&mut *slot, SlotState::Mutable(HashMap::new())) {
                SlotState::Mutable(entries) => entries,
                SlotState::Frozen { entries, .. } => {
                    Arc::try_unwrap(entries).unwrap_or_else(|entries| (*entries).clone())
                }
            };
            for (lba, entry) in drained {
                merged
                    .entry(lba)
                    .and_modify(|existing| {
                        if entry.lsn > existing.lsn {
                            *existing = entry;
                        }
                    })
                    .or_insert(entry);
            }
        }
        merged
    }

    /// Sum of all slot lengths.
    pub fn slot_total_len(&self) -> usize {
        self.slots.iter().map(|s| s.lock().len()).sum()
    }

    /// Number of entries currently in the slot indexed by `bfg & (BFG_SIZE - 1)`.
    pub fn slot_len(&self, bfg: Bfg) -> usize {
        self.slots[(bfg & BFG_INDEX_MASK) as usize].lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.iter().all(|s| s.lock().is_empty())
    }

    pub fn compacted_lsn(&self) -> Lsn {
        self.compacted_lsn.load(Ordering::Acquire)
    }

    /// Max-monotonic update of `compacted_lsn`. Called by every drain
    /// path with the highest LSN that just got folded into the tree.
    /// Concurrent callers (sync thread draining its BFG slot vs. an
    /// `apply_gate.write()` holder draining all slots) cannot race on
    /// the same shard because both take `shard.tree.write()`; the
    /// CAS loop is still kept to make the max semantics explicit for
    /// future maintainers.
    pub fn note_compacted(&self, lsn: Lsn) {
        let mut current = self.compacted_lsn.load(Ordering::Acquire);
        while lsn > current {
            match self.compacted_lsn.compare_exchange_weak(
                current,
                lsn,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(actual) => current = actual,
            }
        }
    }
}

fn entry_to_lookup(entry: BufferEntry) -> BufferLookup {
    if entry.tombstone {
        BufferLookup::Tombstone
    } else {
        BufferLookup::Present(entry.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn val(byte: u8) -> L2pValue {
        L2pValue::from_slice(&[byte])
    }

    #[test]
    fn lookup_absent_on_empty() {
        let b = L2pBuffer::new(0);
        assert_eq!(b.lookup_for_open_bfg(1, 42), BufferLookup::Absent);
    }

    #[test]
    fn insert_at_bfg_indexes_by_bfg_mod() {
        let b = L2pBuffer::new(0);
        // Each BFG in [0, BFG_SIZE) maps to its own slot.
        for i in 0..BFG_SIZE as u64 {
            b.insert_at_bfg(i, i + 1, val((i + 1) as u8), 100 * (i + 1));
            assert_eq!(b.slot_len(i), 1);
        }
        // BFG == BFG_SIZE wraps back to slot 0 (shares it with BFG 0).
        b.insert_at_bfg(BFG_SIZE as u64, 999, val(0), 9999);
        assert_eq!(b.slot_len(BFG_SIZE as u64), 2);
        assert_eq!(b.slot_total_len(), BFG_SIZE + 1);
    }

    #[test]
    fn lookup_for_open_bfg_walks_newest_first() {
        let b = L2pBuffer::new(0);
        b.insert_at_bfg(1, 42, val(1), 101);
        b.insert_at_bfg(2, 42, val(2), 102);
        b.insert_at_bfg(3, 42, val(3), 103);
        assert_eq!(b.lookup_for_open_bfg(3, 42), BufferLookup::Present(val(3)));
        assert_eq!(b.lookup_for_open_bfg(2, 42), BufferLookup::Present(val(2)));
        assert_eq!(b.lookup_for_open_bfg(1, 42), BufferLookup::Present(val(1)));
    }

    #[test]
    fn batch_lookup_and_apply_match_scalar_semantics() {
        let b = L2pBuffer::new(0);
        b.insert_at_bfg(1, 10, val(1), 101);
        b.insert_at_bfg(2, 10, val(2), 102);
        b.insert_at_bfg(2, 20, val(3), 102);
        b.insert_tombstone_at_bfg(3, 20, 103);

        let lbas = [10, 20, 30, 10];
        let batch = b.lookup_many_for_open_bfg(3, &lbas);
        let scalar: Vec<_> = lbas
            .iter()
            .map(|&lba| b.lookup_for_open_bfg(3, lba))
            .collect();
        assert_eq!(batch, scalar);

        b.apply_batch_at_bfg(3, &[(10, Some(val(9))), (30, None)], 104);
        assert_eq!(b.lookup_for_open_bfg(3, 10), BufferLookup::Present(val(9)));
        assert_eq!(b.lookup_for_open_bfg(3, 30), BufferLookup::Tombstone);
    }

    #[test]
    fn lookup_for_open_bfg_skips_wraparound_stale_slot() {
        let b = L2pBuffer::new(0);
        // Plant in slot 0 with BFG=4 (stale from open=1's perspective).
        b.insert_at_bfg(4, 42, val(9), 400);
        b.insert_at_bfg(1, 42, val(1), 101);
        // open=4 walks slot 0 (bfg=4), slot 3, slot 2; should see val(9).
        assert_eq!(b.lookup_for_open_bfg(4, 42), BufferLookup::Present(val(9)));
    }

    #[test]
    fn take_syncing_slot_drains_named_slot_only() {
        let b = L2pBuffer::new(0);
        b.insert_at_bfg(0, 1, val(1), 100);
        b.insert_at_bfg(1, 2, val(2), 200);
        b.insert_at_bfg(2, 3, val(3), 300);
        let drained = b.take_syncing_slot(1);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained.get(&2).map(|e| e.value), Some(val(2)));
        assert_eq!(b.slot_len(1), 0);
        assert_eq!(b.slot_len(0), 1);
        assert_eq!(b.slot_len(2), 1);
    }

    #[test]
    fn frozen_slot_stays_visible_until_exact_generation_finishes() {
        let b = L2pBuffer::new(0);
        b.insert_at_bfg(2, 42, val(7), 202);

        let first = b.borrow_syncing_slot(2);
        let retry = b.borrow_syncing_slot(2);
        assert!(Arc::ptr_eq(&first, &retry));
        assert_eq!(first.get(&42).map(|e| e.value), Some(val(7)));
        assert_eq!(
            b.lookup_for_open_bfg(2, 42),
            BufferLookup::Present(val(7)),
            "publish-before-clear requires lookup to retain the frozen value"
        );

        b.finish_syncing_slot(2, &first);
        assert_eq!(b.slot_len(2), 0);
        assert_eq!(b.lookup_for_open_bfg(2, 42), BufferLookup::Absent);
    }

    #[test]
    fn lifecycle_drain_can_consume_a_frozen_slot() {
        let b = L2pBuffer::new(0);
        b.insert_at_bfg(2, 42, val(7), 202);
        let frozen = b.borrow_syncing_slot(2);

        let drained = b.drain_all_slots();
        assert_eq!(drained.get(&42).map(|e| e.value), Some(val(7)));
        assert!(b.is_empty());

        // The BFG worker may finish after a lifecycle drain consumed the same
        // frozen generation. It must observe the empty slot and do nothing.
        b.finish_syncing_slot(2, &frozen);
        assert!(b.is_empty());
    }

    #[test]
    fn tombstone_at_bfg_observed_by_lookup_for_open_bfg() {
        let b = L2pBuffer::new(0);
        b.insert_at_bfg(1, 42, val(7), 101);
        b.insert_tombstone_at_bfg(2, 42, 202);
        assert_eq!(b.lookup_for_open_bfg(2, 42), BufferLookup::Tombstone);
    }

    #[test]
    fn for_each_live_yields_non_tombstone_across_slots_skips_tombstones() {
        // Backs the buffer-aware reclaim reference check: a committed-but-
        // unfolded remap in ANY slot must be visible, and a tombstone (delete)
        // must NOT count as a live reference.
        let b = L2pBuffer::new(0);
        b.insert_at_bfg(0, 1, val(1), 100);
        b.insert_at_bfg(1, 2, val(2), 200);
        b.insert_at_bfg(2, 3, val(3), 300);
        b.insert_tombstone_at_bfg(3, 4, 400);
        let mut seen: Vec<(Lba, L2pValue)> = Vec::new();
        b.for_each_live(|lba, value| seen.push((lba, value)));
        seen.sort_by_key(|(lba, _)| *lba);
        assert_eq!(seen, vec![(1, val(1)), (2, val(2)), (3, val(3))]);
        assert!(
            !seen.iter().any(|(lba, _)| *lba == 4),
            "tombstoned LBA must not be yielded as a live reference"
        );
    }

    #[test]
    fn lookup_during_bootstrap_does_not_underflow() {
        // open_bfg=1 visits slot 1, slot 0; no underflow.
        let b = L2pBuffer::new(0);
        b.insert_at_bfg(1, 42, val(1), 101);
        assert_eq!(b.lookup_for_open_bfg(1, 42), BufferLookup::Present(val(1)));
        assert_eq!(b.lookup_for_open_bfg(1, 99), BufferLookup::Absent);
    }

    #[test]
    fn drain_all_slots_merges_and_picks_highest_lsn() {
        let b = L2pBuffer::new(0);
        b.insert_at_bfg(1, 42, val(1), 101);
        b.insert_at_bfg(2, 42, val(2), 202);
        b.insert_at_bfg(3, 99, val(9), 303);
        let merged = b.drain_all_slots();
        assert_eq!(merged.len(), 2);
        assert_eq!(merged.get(&42).map(|e| e.value), Some(val(2)));
        assert_eq!(merged.get(&42).map(|e| e.lsn), Some(202));
        assert_eq!(merged.get(&99).map(|e| e.value), Some(val(9)));
        // Every slot now empty.
        assert_eq!(b.slot_total_len(), 0);
        assert!(b.is_empty());
    }

    #[test]
    fn drain_all_slots_empty_returns_empty_map() {
        let b = L2pBuffer::new(0);
        let merged = b.drain_all_slots();
        assert!(merged.is_empty());
    }

    #[test]
    fn note_compacted_is_max_monotonic() {
        let b = L2pBuffer::new(50);
        assert_eq!(b.compacted_lsn(), 50);
        b.note_compacted(100);
        assert_eq!(b.compacted_lsn(), 100);
        // Older value must not regress.
        b.note_compacted(75);
        assert_eq!(b.compacted_lsn(), 100);
        b.note_compacted(100);
        assert_eq!(b.compacted_lsn(), 100);
        b.note_compacted(200);
        assert_eq!(b.compacted_lsn(), 200);
    }

    #[test]
    fn initial_compacted_lsn_preserved() {
        let b = L2pBuffer::new(42);
        assert_eq!(b.compacted_lsn(), 42);
    }
}
