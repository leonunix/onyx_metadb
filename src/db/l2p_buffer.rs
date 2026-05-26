//! 4-slot TXG ring buffer for L2P updates — the front half of the buffered-
//! commit path. Commits insert into the slot indexed by their TXG; the
//! background `TxgSyncThread` (Phase 4) drains the slot for the currently-
//! Syncing TXG into the on-disk paged radix tree.
//!
//! ## Phase 4 migration shim (slot-0 legacy API)
//!
//! Phase 4 of the ZFS-TXG-clone is monolithic but lands in sub-steps; until
//! `TxgQuiesceThread`/`TxgSyncThread` replace the old `L2pCompactor` (Step
//! 5+ in [`zfs-txg-clone.md`]), the existing
//!
//! - `insert(lba, value, lsn)`
//! - `insert_tombstone(lba, lsn)`
//! - `lookup(lba)`
//! - `swap_for_compaction()` / `with_draining()` / `finish_compaction(lsn)`
//!
//! API surface stays alive. Those methods all read/write slot 0 plus the
//! legacy `draining` intermediate — so without a running `TxgStateMachine`
//! every commit lands in slot 0 and `L2pCompactor` continues to drain it
//! unchanged.
//!
//! The new TXG-aware methods —
//!
//! - `insert_at_txg(txg, lba, value, lsn)`
//! - `insert_tombstone_at_txg(txg, lba, lsn)`
//! - `lookup_for_open_txg(open_txg, lba)`
//! - `take_syncing_slot(txg)`
//!
//! are the long-term API that the new sync thread consumes. Step 4 retargets
//! the commit path at them; Step 8 deletes the legacy shim.
//!
//! ## Slot semantics
//!
//! Ring index = `txg & (TXG_SIZE - 1)` (currently `txg & 3`).
//!
//! - **Open slot** (state machine's open_txg): commits insert here.
//! - **Quiescing slot** (open_txg - 1): no new inserts; in-flight commits
//!   on this TXG have not yet dropped their guards.
//! - **Syncing slot** (typically open_txg - 2): frozen; `TxgSyncThread`
//!   owns it; `take_syncing_slot` moves the map out.
//! - The 4th slot is always Empty.
//!
//! ## Lookup ordering
//!
//! `lookup_for_open_txg(open, lba)` walks slots in newest-first order:
//! `open & 3` → `(open-1) & 3` → `(open-2) & 3`. The fourth slot (the
//! wrap-around) is always Empty and is skipped. After the three TXG slots
//! the legacy `draining` slot is consulted so the migration window stays
//! consistent — once the compactor is retired the draining slot will be
//! removed.
//!
//! Tombstone semantics from the 2-state design carry over unchanged: a
//! tombstone in a newer slot suppresses any value in older slots; readers
//! translate Tombstone → "absent" without falling through to the tree.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::paged::L2pValue;
use crate::txg::TXG_SIZE;
use crate::types::{Lba, Lsn, Txg};

const TXG_INDEX_MASK: u64 = (TXG_SIZE as u64) - 1;

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
    slots: [Mutex<HashMap<Lba, BufferEntry>>; TXG_SIZE],
    /// Legacy 2-state draining intermediate. Used only by the old
    /// `L2pCompactor` while it still runs (sub-step gating); retired in
    /// Phase 4 Step 8 when the compactor is replaced by `TxgSyncThread`.
    legacy_draining: Mutex<Option<HashMap<Lba, BufferEntry>>>,
    compacted_lsn: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
pub struct SwapHandle {
    pub max_lsn: Lsn,
    pub count: usize,
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
            legacy_draining: Mutex::new(None),
            compacted_lsn: AtomicU64::new(initial_compacted_lsn),
        }
    }

    // -------- New TXG-aware API (consumed by Step 4+) --------

    /// Insert into the slot indexed by `txg & (TXG_SIZE - 1)`. Caller must
    /// hold a [`crate::txg::TxgGuard`] for `txg`, ensuring the slot's state
    /// stays Open / Quiescing for the duration of the insert.
    pub fn insert_at_txg(&self, txg: Txg, lba: Lba, value: L2pValue, lsn: Lsn) {
        self.slots[(txg & TXG_INDEX_MASK) as usize].lock().insert(
            lba,
            BufferEntry {
                value,
                lsn,
                tombstone: false,
            },
        );
    }

    pub fn insert_tombstone_at_txg(&self, txg: Txg, lba: Lba, lsn: Lsn) {
        self.slots[(txg & TXG_INDEX_MASK) as usize].lock().insert(
            lba,
            BufferEntry {
                value: L2pValue::ZERO,
                lsn,
                tombstone: true,
            },
        );
    }

    /// Walk slots newest-first starting at `open_txg`: `open_txg & 3` →
    /// `(open_txg-1) & 3` → `(open_txg-2) & 3` → legacy_draining.
    ///
    /// Returns the first hit; tombstones suppress fallthrough to the tree.
    pub fn lookup_for_open_txg(&self, open_txg: Txg, lba: Lba) -> BufferLookup {
        // open_txg is at least 1 by construction (TxgStateMachine starts at
        // checkpoint + 1 >= 1), but be defensive about the lower-bound
        // subtraction for slots open-1 and open-2.
        for delta in 0..(TXG_SIZE as u64 - 1) {
            // Saturate at 0 so that during early bootstrap (open_txg == 1)
            // we don't underflow checking slots (-1) & 3.
            let visited = open_txg.saturating_sub(delta);
            if visited == 0 && delta > 0 {
                break;
            }
            let idx = (visited & TXG_INDEX_MASK) as usize;
            if let Some(entry) = self.slots[idx].lock().get(&lba).copied() {
                return entry_to_lookup(entry);
            }
        }
        if let Some(draining) = self.legacy_draining.lock().as_ref() {
            if let Some(entry) = draining.get(&lba).copied() {
                return entry_to_lookup(entry);
            }
        }
        BufferLookup::Absent
    }

    /// Drain the slot indexed by `txg & (TXG_SIZE - 1)`. Caller (the
    /// `TxgSyncThread`) must have already promoted the slot to Syncing in
    /// the state machine — that's what makes the move safe: no new commits
    /// can target the slot while it's Syncing.
    pub fn take_syncing_slot(&self, txg: Txg) -> HashMap<Lba, BufferEntry> {
        std::mem::take(&mut *self.slots[(txg & TXG_INDEX_MASK) as usize].lock())
    }

    /// Sum of all slot lengths (excludes legacy_draining).
    pub fn slot_total_len(&self) -> usize {
        self.slots.iter().map(|s| s.lock().len()).sum()
    }

    /// Number of entries currently in the slot indexed by `txg & 3`.
    pub fn slot_len(&self, txg: Txg) -> usize {
        self.slots[(txg & TXG_INDEX_MASK) as usize].lock().len()
    }

    // -------- Legacy 2-state API (slot 0; retired in Step 8) --------

    /// Insert into slot 0. Phase 4 Step 4 retargets all current callers at
    /// `insert_at_txg`; until then this is the migration shim that lets
    /// the existing commit path compile unchanged.
    pub fn insert(&self, lba: Lba, value: L2pValue, lsn: Lsn) {
        self.insert_at_txg(0, lba, value, lsn);
    }

    pub fn insert_tombstone(&self, lba: Lba, lsn: Lsn) {
        self.insert_tombstone_at_txg(0, lba, lsn);
    }

    /// Returns:
    /// - `Present(v)` — buffer has a non-tombstone value
    /// - `Tombstone` — buffer recorded a delete; caller must NOT fall through
    ///   to the tree
    /// - `Absent` — buffer has nothing for this LBA; caller SHOULD fall
    ///   through to the tree read view
    ///
    /// Phase 4 transitional behaviour: walks every TXG slot (newest-first
    /// is not yet observable because callers don't pass an `open_txg`) then
    /// the legacy draining slot. Step 4 retargets read-path callers at
    /// `lookup_for_open_txg` so the walk order becomes meaningful.
    pub fn lookup(&self, lba: Lba) -> BufferLookup {
        for slot in &self.slots {
            if let Some(entry) = slot.lock().get(&lba).copied() {
                return entry_to_lookup(entry);
            }
        }
        if let Some(draining) = self.legacy_draining.lock().as_ref() {
            if let Some(entry) = draining.get(&lba).copied() {
                return entry_to_lookup(entry);
            }
        }
        BufferLookup::Absent
    }

    pub fn len(&self) -> usize {
        self.slot_total_len() + self.legacy_draining.lock().as_ref().map_or(0, |m| m.len())
    }

    pub fn active_len(&self) -> usize {
        // Legacy callers (the L2pCompactor) ask "how many entries are
        // waiting to be compacted". After Step 4 the commit path stamps
        // its writes against the TxgStateMachine's open_txg (slot index
        // == open_txg & 3, which can be ANY of the four slots while the
        // compactor still runs), so the answer is "sum across every TXG
        // slot". swap_for_compaction below merges them. This method goes
        // away with the compactor in Step 8.
        self.slot_total_len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.iter().all(|s| s.lock().is_empty())
            && self
                .legacy_draining
                .lock()
                .as_ref()
                .map_or(true, |m| m.is_empty())
    }

    pub fn compacted_lsn(&self) -> Lsn {
        self.compacted_lsn.load(Ordering::Acquire)
    }

    /// Merge every TXG slot into the legacy `draining` slot. The
    /// `L2pCompactor` then applies the drained map to the tree, publishes
    /// the read view, and calls `finish_compaction` to drop the slot and
    /// stamp `compacted_lsn`. Retired with the compactor in Step 8.
    ///
    /// Phase 4 migration: the compactor used to drain only slot 0 because
    /// every commit landed there. After Step 4 commits stamp the slot
    /// indexed by `TxgStateMachine::open_txg & 3`, which is `slot 1` on a
    /// fresh open. Until the compactor is replaced by `TxgSyncThread`,
    /// this method merges all four slots so no committed data is left
    /// stranded.
    pub fn swap_for_compaction(&self) -> Option<SwapHandle> {
        let mut draining = self.legacy_draining.lock();
        if draining.is_some() {
            // Another caller (background `L2pCompactor` vs explicit
            // `force_compact_l2p_buffers` from `flush_with_gate`, or two
            // `test_force_compact_pass` callers in a proptest harness)
            // is already mid-cycle on this shard. Return `None` so the
            // current caller treats this as a no-work cycle; the winning
            // caller will eventually call `finish_compaction` and the
            // next sweep will pick up any newly-inserted entries.
            //
            // Pre-Step-3 the legacy 2-state path panicked here on the
            // theory that the compactor pipeline was strictly serial.
            // After Step 3 merged all four ring slots into the legacy
            // draining intermediate the swap critical section widened
            // and the assertion became flaky under concurrent
            // background-compactor / explicit-flush races.
            return None;
        }
        let mut merged: HashMap<Lba, BufferEntry> = HashMap::new();
        for slot in &self.slots {
            let drained = std::mem::take(&mut *slot.lock());
            for (lba, entry) in drained {
                merged
                    .entry(lba)
                    .and_modify(|existing| {
                        // newest LSN wins on conflict (tombstone-aware)
                        if entry.lsn > existing.lsn {
                            *existing = entry;
                        }
                    })
                    .or_insert(entry);
            }
        }
        if merged.is_empty() {
            return None;
        }
        let max_lsn = merged.values().map(|e| e.lsn).max().unwrap_or(0);
        let count = merged.len();
        *draining = Some(merged);
        Some(SwapHandle { max_lsn, count })
    }

    /// Iterate entries currently in the legacy `draining` slot. Retired
    /// with the compactor in Step 8.
    pub fn with_draining<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<&HashMap<Lba, BufferEntry>>) -> R,
    {
        let guard = self.legacy_draining.lock();
        f(guard.as_ref())
    }

    /// Drop the legacy `draining` slot and stamp `compacted_lsn = max_lsn`.
    /// Retired with the compactor in Step 8.
    pub fn finish_compaction(&self, max_lsn: Lsn) {
        *self.legacy_draining.lock() = None;
        self.compacted_lsn.store(max_lsn, Ordering::Release);
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
        assert_eq!(b.lookup(42), BufferLookup::Absent);
    }

    #[test]
    fn insert_then_lookup_present() {
        let b = L2pBuffer::new(0);
        b.insert(42, val(7), 100);
        assert_eq!(b.lookup(42), BufferLookup::Present(val(7)));
    }

    #[test]
    fn tombstone_suppresses_tree_fallback() {
        let b = L2pBuffer::new(0);
        b.insert_tombstone(42, 100);
        assert_eq!(b.lookup(42), BufferLookup::Tombstone);
    }

    #[test]
    fn later_insert_overrides_earlier_in_active() {
        let b = L2pBuffer::new(0);
        b.insert(42, val(1), 100);
        b.insert(42, val(2), 101);
        assert_eq!(b.lookup(42), BufferLookup::Present(val(2)));
    }

    #[test]
    fn tombstone_overrides_prior_value_in_active() {
        let b = L2pBuffer::new(0);
        b.insert(42, val(7), 100);
        b.insert_tombstone(42, 101);
        assert_eq!(b.lookup(42), BufferLookup::Tombstone);
    }

    #[test]
    fn lookup_walks_active_then_draining() {
        let b = L2pBuffer::new(0);
        b.insert(42, val(1), 100);
        let swap = b.swap_for_compaction().unwrap();
        assert_eq!(swap.max_lsn, 100);
        assert_eq!(swap.count, 1);
        assert_eq!(b.lookup(42), BufferLookup::Present(val(1)));
    }

    #[test]
    fn active_shadows_draining_after_swap() {
        let b = L2pBuffer::new(0);
        b.insert(42, val(1), 100);
        b.swap_for_compaction().unwrap();
        b.insert(42, val(2), 200);
        assert_eq!(b.lookup(42), BufferLookup::Present(val(2)));
    }

    #[test]
    fn finish_compaction_clears_draining_and_stamps_lsn() {
        let b = L2pBuffer::new(0);
        b.insert(42, val(1), 100);
        let swap = b.swap_for_compaction().unwrap();
        b.finish_compaction(swap.max_lsn);
        assert_eq!(b.compacted_lsn(), 100);
        assert_eq!(b.lookup(42), BufferLookup::Absent);
    }

    #[test]
    fn swap_empty_returns_none() {
        let b = L2pBuffer::new(0);
        assert!(b.swap_for_compaction().is_none());
    }

    #[test]
    fn double_swap_without_finish_returns_none() {
        // Pre-Step-3 the second concurrent swap panicked; with the
        // graceful-skip change in Step 3 it returns None so the loser
        // simply yields back to the winning caller.
        let b = L2pBuffer::new(0);
        b.insert(42, val(1), 100);
        let first = b.swap_for_compaction();
        assert!(first.is_some());
        b.insert(43, val(2), 101);
        let second = b.swap_for_compaction();
        assert!(second.is_none(), "second swap should yield None while draining is occupied");
    }

    #[test]
    fn len_includes_all_slots_and_draining() {
        let b = L2pBuffer::new(0);
        assert_eq!(b.len(), 0);
        b.insert(1, val(1), 100);
        b.insert(2, val(2), 101);
        assert_eq!(b.len(), 2);
        b.swap_for_compaction().unwrap();
        b.insert(3, val(3), 102);
        assert_eq!(b.len(), 3);
        assert_eq!(b.active_len(), 1);
    }

    #[test]
    fn initial_compacted_lsn_preserved() {
        let b = L2pBuffer::new(42);
        assert_eq!(b.compacted_lsn(), 42);
    }

    #[test]
    fn with_draining_observes_entries_during_apply() {
        let b = L2pBuffer::new(0);
        b.insert(1, val(1), 100);
        b.insert(2, val(2), 101);
        b.swap_for_compaction().unwrap();

        let seen = b.with_draining(|map| {
            let m = map.expect("draining present");
            (
                m.contains_key(&1),
                m.contains_key(&2),
                m.get(&1).map(|e| e.lsn),
                m.get(&2).map(|e| e.lsn),
            )
        });
        assert_eq!(seen, (true, true, Some(100), Some(101)));
    }

    #[test]
    fn with_draining_returns_none_outside_cycle() {
        let b = L2pBuffer::new(0);
        let observed = b.with_draining(|m| m.is_none());
        assert!(observed);
    }

    #[test]
    fn tombstone_in_draining_is_observable() {
        let b = L2pBuffer::new(0);
        b.insert_tombstone(42, 100);
        b.swap_for_compaction().unwrap();
        assert_eq!(b.lookup(42), BufferLookup::Tombstone);
    }

    // -------- Phase 4 4-slot ring tests --------

    #[test]
    fn insert_at_txg_indexes_by_txg_mod() {
        let b = L2pBuffer::new(0);
        // TXG 0 → slot 0
        b.insert_at_txg(0, 1, val(1), 100);
        assert_eq!(b.slot_len(0), 1);
        // TXG 1 → slot 1
        b.insert_at_txg(1, 2, val(2), 200);
        assert_eq!(b.slot_len(1), 1);
        // TXG 2 → slot 2
        b.insert_at_txg(2, 3, val(3), 300);
        assert_eq!(b.slot_len(2), 1);
        // TXG 3 → slot 3
        b.insert_at_txg(3, 4, val(4), 400);
        assert_eq!(b.slot_len(3), 1);
        // TXG 4 → wraps to slot 0
        b.insert_at_txg(4, 5, val(5), 500);
        assert_eq!(b.slot_len(4), 2); // slot 0 now has LBA 1 and 5
        assert_eq!(b.slot_total_len(), 5);
    }

    #[test]
    fn lookup_for_open_txg_walks_newest_first() {
        let b = L2pBuffer::new(0);
        // Same LBA written across 3 consecutive TXGs.
        b.insert_at_txg(1, 42, val(1), 101);
        b.insert_at_txg(2, 42, val(2), 102);
        b.insert_at_txg(3, 42, val(3), 103);
        // With open_txg = 3 the walk goes slot 3 → slot 2 → slot 1.
        // Newest wins.
        assert_eq!(b.lookup_for_open_txg(3, 42), BufferLookup::Present(val(3)));
        // With open_txg = 2 the walk should NOT see slot 3 (that TXG hasn't
        // happened yet from open's perspective). It should see slot 2.
        assert_eq!(b.lookup_for_open_txg(2, 42), BufferLookup::Present(val(2)));
        assert_eq!(b.lookup_for_open_txg(1, 42), BufferLookup::Present(val(1)));
    }

    #[test]
    fn lookup_for_open_txg_skips_wraparound_stale_slot() {
        let b = L2pBuffer::new(0);
        // Plant an entry in slot 0 with a TXG that's 3 less than `open`.
        // That's the wraparound slot that's always Empty at TxgStateMachine
        // level; for the buffer it's just a slot. Walking from open=4
        // (slot 0) should NOT visit (4-3)=1 (slot 1) — only slots 4,3,2 → 0,3,2.
        b.insert_at_txg(4, 42, val(9), 400); // slot 0
        b.insert_at_txg(1, 42, val(1), 101); // slot 1 — stale to open=4
        assert_eq!(b.lookup_for_open_txg(4, 42), BufferLookup::Present(val(9)));
    }

    #[test]
    fn take_syncing_slot_drains_named_slot_only() {
        let b = L2pBuffer::new(0);
        b.insert_at_txg(0, 1, val(1), 100);
        b.insert_at_txg(1, 2, val(2), 200);
        b.insert_at_txg(2, 3, val(3), 300);
        let drained = b.take_syncing_slot(1);
        assert_eq!(drained.len(), 1);
        assert_eq!(drained.get(&2).map(|e| e.value), Some(val(2)));
        // Slot 1 now empty; slots 0 and 2 untouched.
        assert_eq!(b.slot_len(1), 0);
        assert_eq!(b.slot_len(0), 1);
        assert_eq!(b.slot_len(2), 1);
    }

    #[test]
    fn tombstone_at_txg_observed_by_lookup_for_open_txg() {
        let b = L2pBuffer::new(0);
        b.insert_at_txg(1, 42, val(7), 101);
        b.insert_tombstone_at_txg(2, 42, 202);
        // With open_txg = 2 the tombstone in slot 2 shadows the value in
        // slot 1.
        assert_eq!(b.lookup_for_open_txg(2, 42), BufferLookup::Tombstone);
    }

    #[test]
    fn lookup_during_bootstrap_does_not_underflow() {
        // open_txg can be 1 at first open (checkpoint_txg = 0). The walk
        // visits slots 1, 0, then stops (TXG -1 is invalid).
        let b = L2pBuffer::new(0);
        b.insert_at_txg(1, 42, val(1), 101);
        assert_eq!(b.lookup_for_open_txg(1, 42), BufferLookup::Present(val(1)));
        assert_eq!(b.lookup_for_open_txg(1, 99), BufferLookup::Absent);
    }

    #[test]
    fn legacy_lookup_walks_every_slot() {
        // Phase 4 transitional: legacy `lookup` doesn't know `open_txg` so
        // it must walk every slot. Important during the migration window
        // where legacy callers haven't been retargeted yet.
        let b = L2pBuffer::new(0);
        b.insert_at_txg(2, 42, val(2), 200);
        assert_eq!(b.lookup(42), BufferLookup::Present(val(2)));
    }
}
