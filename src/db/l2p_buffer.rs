//! 4-slot BFG ring buffer for L2P updates.
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
//! Ring index = `bfg & (BFG_SIZE - 1)` (currently `bfg & 3`).
//!
//! - **Open slot** (state machine's open_bfg): commits insert here.
//! - **Quiescing slot** (open_bfg - 1): no new inserts; in-flight commits
//!   on this BFG have not yet dropped their guards.
//! - **Syncing slot** (typically open_bfg - 2): frozen; `BfgSyncThread`
//!   owns it; [`L2pBuffer::take_syncing_slot`] moves the map out.
//! - The 4th slot is always Empty.
//!
//! ## Lookup ordering
//!
//! [`L2pBuffer::lookup_for_open_bfg`] walks slots in newest-first order:
//! `open & 3` → `(open-1) & 3` → `(open-2) & 3`. The fourth slot (the
//! wrap-around) is always Empty and is skipped.
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

pub struct L2pBuffer {
    slots: [Mutex<HashMap<Lba, BufferEntry>>; BFG_SIZE],
    compacted_lsn: AtomicU64,
}

impl L2pBuffer {
    pub fn new(initial_compacted_lsn: Lsn) -> Self {
        Self {
            slots: [
                Mutex::new(HashMap::new()),
                Mutex::new(HashMap::new()),
                Mutex::new(HashMap::new()),
                Mutex::new(HashMap::new()),
            ],
            compacted_lsn: AtomicU64::new(initial_compacted_lsn),
        }
    }

    /// Insert into the slot indexed by `bfg & (BFG_SIZE - 1)`. Caller
    /// must hold a [`crate::bfg::BfgGuard`] for `bfg`, ensuring the
    /// slot's state stays Open / Quiescing for the duration of the
    /// insert.
    pub fn insert_at_bfg(&self, bfg: Bfg, lba: Lba, value: L2pValue, lsn: Lsn) {
        self.slots[(bfg & BFG_INDEX_MASK) as usize].lock().insert(
            lba,
            BufferEntry {
                value,
                lsn,
                tombstone: false,
            },
        );
    }

    pub fn insert_tombstone_at_bfg(&self, bfg: Bfg, lba: Lba, lsn: Lsn) {
        self.slots[(bfg & BFG_INDEX_MASK) as usize].lock().insert(
            lba,
            BufferEntry {
                value: L2pValue::ZERO,
                lsn,
                tombstone: true,
            },
        );
    }

    /// Walk slots newest-first starting at `open_bfg`: `open_bfg & 3` →
    /// `(open_bfg-1) & 3` → `(open_bfg-2) & 3`. The 4th slot is always
    /// Empty (state machine invariant) and is skipped.
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
            if let Some(entry) = self.slots[idx].lock().get(&lba).copied() {
                return entry_to_lookup(entry);
            }
        }
        BufferLookup::Absent
    }

    /// Drain the slot indexed by `bfg & (BFG_SIZE - 1)`. Caller (the
    /// `BfgSyncThread`) must have already promoted the slot to Syncing
    /// in the state machine — that's what makes the move safe: no new
    /// commits can target the slot while it's Syncing.
    pub fn take_syncing_slot(&self, bfg: Bfg) -> HashMap<Lba, BufferEntry> {
        std::mem::take(&mut *self.slots[(bfg & BFG_INDEX_MASK) as usize].lock())
    }

    /// Clone (without removing) the entries of the slot indexed by
    /// `bfg & (BFG_SIZE - 1)`. Used by the threads-on per-syncing-slot
    /// drain ([`crate::db::Db::drain_syncing_slot_into_trees`]) to fold
    /// the entries into the tree and PUBLISH the read view *before*
    /// clearing the slot — so a concurrent `lookup_for_open_bfg` (a
    /// commit's prev-value read or a user read, both of which lock this
    /// slot during their walk) never observes the gap "slot already
    /// emptied but tree/read_view not yet updated" and falsely reports
    /// `prev = None`. Correct only when the slot is frozen (Syncing
    /// state, no concurrent inserts) so the clone equals the subsequent
    /// [`Self::take_syncing_slot`] clear.
    pub fn snapshot_syncing_slot(&self, bfg: Bfg) -> HashMap<Lba, BufferEntry> {
        self.slots[(bfg & BFG_INDEX_MASK) as usize].lock().clone()
    }

    /// Visit every live (non-tombstone) entry across all four slots. The
    /// caller tolerates duplicates and stale-superseded entries across
    /// slots — used by the reclaim reference check
    /// ([`crate::db::Db::scan_l2p_buffer_values`]), which only ORs
    /// references and conservatively over-retains. Each slot is cloned
    /// under its lock and iterated unlocked so a concurrent commit isn't
    /// blocked for the duration of the callback.
    pub fn for_each_live<F: FnMut(Lba, L2pValue)>(&self, mut f: F) {
        for slot in &self.slots {
            let snap = slot.lock().clone();
            for (lba, entry) in snap {
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
    /// (newest-first across the four slots).
    pub fn drain_all_slots(&self) -> HashMap<Lba, BufferEntry> {
        let mut merged: HashMap<Lba, BufferEntry> = HashMap::new();
        for slot in &self.slots {
            let drained = std::mem::take(&mut *slot.lock());
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

    /// Number of entries currently in the slot indexed by `bfg & 3`.
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
        b.insert_at_bfg(0, 1, val(1), 100);
        assert_eq!(b.slot_len(0), 1);
        b.insert_at_bfg(1, 2, val(2), 200);
        assert_eq!(b.slot_len(1), 1);
        b.insert_at_bfg(2, 3, val(3), 300);
        assert_eq!(b.slot_len(2), 1);
        b.insert_at_bfg(3, 4, val(4), 400);
        assert_eq!(b.slot_len(3), 1);
        // BFG 4 wraps to slot 0.
        b.insert_at_bfg(4, 5, val(5), 500);
        assert_eq!(b.slot_len(4), 2);
        assert_eq!(b.slot_total_len(), 5);
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
