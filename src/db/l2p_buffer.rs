//! In-memory write buffer for L2P updates — the front half of the B2
//! buffered-commit path. Commits insert here in O(1); a background
//! compactor periodically folds the buffer into the on-disk paged radix
//! tree.
//!
//! ## Concurrency model
//!
//! - `active`: the current write target. Commits acquire `active.lock()`
//!   briefly to insert.
//! - `draining`: holds the previous `active` while the compactor applies
//!   it to the tree and republishes the read view. Readers consult
//!   `draining` after `active` so a swap-in-flight entry stays visible.
//! - `finish_compaction(max_lsn)` clears `draining` and stamps
//!   `compacted_lsn`. MUST be called AFTER the compactor has called
//!   `publish_l2p_read_view`; otherwise readers can observe `active.lookup
//!   = None ∧ draining.lookup = None ∧ old read_view does not contain
//!   entry`, a false "no mapping" result.
//!
//! ## Tombstones
//!
//! L2pDelete records a tombstone entry instead of removing the LBA from
//! the buffer outright. Readers translate Tombstone → "absent" without
//! falling through to the tree (the tree may still hold the pre-delete
//! value). The compactor replays the tombstone as a real tree delete.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;

use crate::paged::L2pValue;
use crate::types::{Lba, Lsn};

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
    active: Mutex<HashMap<Lba, BufferEntry>>,
    draining: Mutex<Option<HashMap<Lba, BufferEntry>>>,
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
            active: Mutex::new(HashMap::new()),
            draining: Mutex::new(None),
            compacted_lsn: AtomicU64::new(initial_compacted_lsn),
        }
    }

    /// Returns:
    /// - `Present(v)` — buffer has a non-tombstone value
    /// - `Tombstone` — buffer recorded a delete; caller must NOT fall through
    ///   to the tree
    /// - `Absent` — buffer has nothing for this LBA; caller SHOULD fall
    ///   through to the tree read view
    pub fn lookup(&self, lba: Lba) -> BufferLookup {
        if let Some(entry) = self.active.lock().get(&lba).copied() {
            return entry_to_lookup(entry);
        }
        if let Some(draining) = self.draining.lock().as_ref() {
            if let Some(entry) = draining.get(&lba).copied() {
                return entry_to_lookup(entry);
            }
        }
        BufferLookup::Absent
    }

    pub fn insert(&self, lba: Lba, value: L2pValue, lsn: Lsn) {
        self.active.lock().insert(
            lba,
            BufferEntry {
                value,
                lsn,
                tombstone: false,
            },
        );
    }

    pub fn insert_tombstone(&self, lba: Lba, lsn: Lsn) {
        self.active.lock().insert(
            lba,
            BufferEntry {
                value: L2pValue::ZERO,
                lsn,
                tombstone: true,
            },
        );
    }

    pub fn len(&self) -> usize {
        self.active.lock().len() + self.draining.lock().as_ref().map_or(0, |m| m.len())
    }

    pub fn active_len(&self) -> usize {
        self.active.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.active.lock().is_empty() && self.draining.lock().as_ref().map_or(true, |m| m.is_empty())
    }

    pub fn compacted_lsn(&self) -> Lsn {
        self.compacted_lsn.load(Ordering::Acquire)
    }

    /// Atomic swap: move `active` into the `draining` slot. The caller
    /// (compactor) is now responsible for applying the returned entries
    /// to the tree and calling `finish_compaction`.
    ///
    /// Returns `None` if `active` is empty.
    /// Panics if `draining` is already in use — `finish_compaction` must
    /// be called between cycles.
    pub fn swap_for_compaction(&self) -> Option<SwapHandle> {
        let mut active = self.active.lock();
        if active.is_empty() {
            return None;
        }
        let mut draining = self.draining.lock();
        assert!(
            draining.is_none(),
            "L2pBuffer::swap_for_compaction called with draining slot occupied; \
             finish_compaction was not invoked between cycles"
        );
        let entries: HashMap<Lba, BufferEntry> = std::mem::take(&mut *active);
        let max_lsn = entries.values().map(|e| e.lsn).max().unwrap_or(0);
        let count = entries.len();
        *draining = Some(entries);
        Some(SwapHandle { max_lsn, count })
    }

    /// Iterate entries currently in the `draining` slot. Holds `draining`
    /// lock for the duration of the closure; concurrent readers' calls to
    /// `lookup` traversing the draining map block briefly behind this
    /// lock.
    pub fn with_draining<F, R>(&self, f: F) -> R
    where
        F: FnOnce(Option<&HashMap<Lba, BufferEntry>>) -> R,
    {
        let guard = self.draining.lock();
        f(guard.as_ref())
    }

    /// Drop the draining map and stamp `compacted_lsn = max_lsn`.
    ///
    /// Must be called AFTER the compactor has published a new read view
    /// covering the just-applied entries (see module docs).
    pub fn finish_compaction(&self, max_lsn: Lsn) {
        *self.draining.lock() = None;
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
    #[should_panic(expected = "draining slot occupied")]
    fn double_swap_without_finish_panics() {
        let b = L2pBuffer::new(0);
        b.insert(42, val(1), 100);
        b.swap_for_compaction().unwrap();
        b.insert(43, val(2), 101);
        b.swap_for_compaction();
    }

    #[test]
    fn len_includes_active_and_draining() {
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
}
