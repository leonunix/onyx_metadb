//! Delta map: pending refcount mutations awaiting flush to the array.
//!
//! Concurrency model: a `Mutex<DeltaMap>` per shard. The apply lane
//! holds the lock to merge a batch of `(pba, signed_delta, lsn)` tuples
//! and to drain on flush. Readers hold the lock briefly to peek pending
//! values before falling back to the on-disk array.
//!
//! WAL ops batched here are NOT durable — they have already been
//! WAL-fsynced by `commit_with_outcomes`; the delta map is purely an
//! in-memory accumulator that lets us amortise per-op cost (replacing
//! the O(log N) B+tree write per op with O(1) HashMap merge).

use std::collections::HashMap;

use crate::types::{Lsn, Pba};
use crate::u64_hash::U64BuildHasher;

/// One pending (signed) accumulator + the highest LSN that contributed
/// to it. The LSN is needed to (a) stamp the array page on flush so
/// replay can skip already-applied ops, and (b) seed `birth_lsn` on a
/// 0→1 transition in the merged result.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Pending {
    pub delta: i64,
    pub last_lsn: Lsn,
}

#[derive(Default)]
pub struct DeltaMap {
    inner: HashMap<Pba, Pending, U64BuildHasher>,
}

impl DeltaMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn merge(&mut self, pba: Pba, delta: i64, lsn: Lsn) {
        let slot = self.inner.entry(pba).or_default();
        slot.delta = slot.delta.saturating_add(delta);
        if lsn > slot.last_lsn {
            slot.last_lsn = lsn;
        }
    }

    pub fn get(&self, pba: Pba) -> Option<Pending> {
        self.inner.get(&pba).copied()
    }

    /// Remove one exact PBA from a frozen checkpoint slot.
    ///
    /// Production streaming checkpoints call this only after publishing the
    /// corresponding array page under `fold_lock.write()`. The Syncing slot is
    /// frozen, so no concurrent insert can race the removal.
    pub fn remove(&mut self, pba: Pba) -> Option<Pending> {
        self.inner.remove(&pba)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Drain all entries. Used by the lane worker at flush time to
    /// move accumulated deltas into the array under the array lock.
    pub fn drain(&mut self) -> impl Iterator<Item = (Pba, Pending)> + '_ {
        self.inner.drain()
    }

    /// Take ownership of all entries by swapping in an empty map.
    /// Used by the priority-3 background drainer in its cycle's
    /// "swap out delta_active" step — bounded `O(1)` move under the
    /// caller's `delta_active` lock, so concurrent `stage()` callers
    /// re-acquire the (now-empty) map immediately after.
    pub fn take(&mut self) -> Self {
        std::mem::take(self)
    }

    /// Re-merge a previously taken/drained entry back into the map.
    /// Used by `RcShard::abort_checkpoint` to restore drained-but-
    /// unflushed deltas, and by the backpressure fallback path that
    /// rolls a draining batch back into `delta_active`.
    pub fn merge_pending(&mut self, pba: Pba, pending: Pending) {
        let slot = self.inner.entry(pba).or_default();
        slot.delta = slot.delta.saturating_add(pending.delta);
        if pending.last_lsn > slot.last_lsn {
            slot.last_lsn = pending.last_lsn;
        }
    }

    /// Iterate without consuming. Used by the drainer to clone the
    /// drained batch when it needs to keep the data accessible
    /// through `delta_draining` while the heavy build runs.
    pub fn iter(&self) -> impl Iterator<Item = (&Pba, &Pending)> + '_ {
        self.inner.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_accumulates_signed_delta() {
        let mut d = DeltaMap::new();
        d.merge(10, 1, 100);
        d.merge(10, 2, 101);
        d.merge(10, -1, 102);
        let p = d.get(10).unwrap();
        assert_eq!(p.delta, 2);
        assert_eq!(p.last_lsn, 102);
    }

    #[test]
    fn merge_keeps_max_lsn() {
        let mut d = DeltaMap::new();
        d.merge(10, 1, 200);
        d.merge(10, 1, 100); // older
        let p = d.get(10).unwrap();
        assert_eq!(p.delta, 2);
        assert_eq!(p.last_lsn, 200);
    }

    #[test]
    fn drain_empties_map() {
        let mut d = DeltaMap::new();
        d.merge(1, 1, 1);
        d.merge(2, 2, 2);
        let drained: Vec<_> = d.drain().collect();
        assert_eq!(drained.len(), 2);
        assert!(d.is_empty());
    }

    #[test]
    fn get_missing_returns_none() {
        let d = DeltaMap::new();
        assert_eq!(d.get(99), None);
    }

    #[test]
    fn aligned_pbas_preserve_merge_take_and_drain_semantics() {
        const ENTRIES: u64 = 4096;
        let mut deltas = DeltaMap::new();
        for index in 0..ENTRIES {
            let pba = index << 20;
            deltas.merge(pba, 2, index + 1);
            deltas.merge(pba, -1, ENTRIES + index + 1);
        }

        assert_eq!(deltas.len(), ENTRIES as usize);
        for index in 0..ENTRIES {
            assert_eq!(
                deltas.get(index << 20),
                Some(Pending {
                    delta: 1,
                    last_lsn: ENTRIES + index + 1,
                })
            );
        }

        let mut taken = deltas.take();
        assert!(deltas.is_empty());
        assert_eq!(taken.len(), ENTRIES as usize);

        let drained: HashMap<_, _> = taken.drain().collect();
        assert!(taken.is_empty());
        assert_eq!(drained.len(), ENTRIES as usize);
        for index in 0..ENTRIES {
            assert_eq!(drained[&(index << 20)].delta, 1);
        }
    }
}
