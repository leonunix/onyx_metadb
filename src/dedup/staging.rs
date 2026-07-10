//! Per-shard pending dedup-index staging — the in-RAM accumulator that
//! lets the cuckoo `put`/`delete` move OFF the commit/apply critical
//! path (see `/root/.claude/plans/mighty-pondering-truffle.md`).
//!
//! # Responsibility + concurrency model
//!
//! Each dedup shard owns an `active` map of pending `(hash → mutation)`
//! plus a `draining` slot. The hot-path apply arms call
//! [`DedupStaging::merge_put`] / [`merge_delete`] (an O(1) HashMap merge)
//! instead of the blocking 4 KiB cuckoo page write; a background drainer
//! () folds the staged mutations into the on-disk cuckoo outside
//! the apply gate.
//!
//! ## Merge rule = last-LSN-wins (NOT additive)
//!
//! Unlike the refcount `DeltaMap` (additive signed deltas, order-free),
//! dedup mutations are last-writer-wins: the latest mutation for a hash
//! supersedes earlier ones. Two concurrent `stage_ops` on the same hash
//! finish in arbitrary wall-clock order, so the merge compares LSNs and
//! keeps the higher (`new_lsn >= existing_lsn`). This single rule is the
//! correctness anchor for the deferral: it guarantees a newer `Delete`
//! can never be shadowed by an older `Put`, which is what keeps the
//! commit-time `old_pba` backfill (and therefore the inline refcount
//! deltas) exact.
//!
//! ## Lock order: `active` before `draining`
//!
//! Readers ([`lookup`]) and the swap ([`swap_active_to_draining`]) both
//! acquire `active` before `draining`. The swap holds **both** locks
//! simultaneously while moving the map across, so a staged entry is
//! never momentarily invisible (in neither slot) — a reader would
//! otherwise fall through to the not-yet-written cuckoo and miss it.
//! The drainer keeps the `draining` map populated while it applies it to
//! the cuckoo, clearing the slot only after the cuckoo write completes,
//! so a staged entry is visible (via `draining`) right up until it is
//! durable in the cuckoo.

use std::collections::HashMap;

use parking_lot::Mutex;

use crate::dedup_types::{DedupValue, Hash8};
use crate::types::Lsn;

/// One pending dedup-index mutation for a hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StagedMutation {
    Put { value: DedupValue, lsn: Lsn },
    Delete { lsn: Lsn },
}

impl StagedMutation {
    #[inline]
    fn lsn(&self) -> Lsn {
        match self {
            StagedMutation::Put { lsn, .. } => *lsn,
            StagedMutation::Delete { lsn } => *lsn,
        }
    }
}

/// Result of consulting the staging layer for a hash.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StagedLookup {
    /// A staged `Put` is the latest mutation — the dedup_index value is
    /// `value`, shadowing whatever the on-disk cuckoo holds.
    Present(DedupValue),
    /// A staged `Delete` is the latest mutation — the hash is logically
    /// absent regardless of the on-disk cuckoo.
    Tombstone,
    /// Nothing staged for this hash — fall through to L1 / cuckoo.
    Absent,
}

struct StagingShard {
    active: Mutex<HashMap<Hash8, StagedMutation>>,
    draining: Mutex<Option<HashMap<Hash8, StagedMutation>>>,
}

impl StagingShard {
    fn new() -> Self {
        Self {
            active: Mutex::new(HashMap::new()),
            draining: Mutex::new(None),
        }
    }
}

#[inline]
fn merge_into(map: &mut HashMap<Hash8, StagedMutation>, hash: Hash8, m: StagedMutation) {
    use std::collections::hash_map::Entry;
    match map.entry(hash) {
        Entry::Occupied(mut e) => {
            // Last-LSN-wins. `>=` (not `>`) so an equal-LSN follow-up in
            // the same batch keeps the later op, matching the cuckoo's
            // `put_many` "keep last duplicate" semantics.
            if m.lsn() >= e.get().lsn() {
                *e.get_mut() = m;
            }
        }
        Entry::Vacant(e) => {
            e.insert(m);
        }
    }
}

/// All dedup staging shards. Routed by the low-32-bit fingerprint so a
/// hash always lands on the same staging shard (a stable, cheap
/// partition independent of the cuckoo's seeded bucket hashing).
pub(crate) struct DedupStaging {
    shards: Box<[StagingShard]>,
}

impl DedupStaging {
    /// `shard_count` is `cfg.dedup_shards` (validated power-of-two, ≥ 1).
    pub(crate) fn new(shard_count: usize) -> Self {
        let shard_count = shard_count.max(1);
        let shards = (0..shard_count)
            .map(|_| StagingShard::new())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self { shards }
    }

    #[inline]
    pub(crate) fn shard_count(&self) -> usize {
        self.shards.len()
    }

    #[inline]
    fn shard_idx(&self, hash: &Hash8) -> usize {
        // Power-of-two count → mask; falls back to `%` defensively if a
        // future non-power-of-two slips through validation.
        let n = self.shards.len();
        let fp = super::fp_of(hash) as usize;
        if n.is_power_of_two() {
            fp & (n - 1)
        } else {
            fp % n
        }
    }

    pub(crate) fn merge_put(&self, hash: Hash8, value: DedupValue, lsn: Lsn) {
        let sid = self.shard_idx(&hash);
        let mut active = self.shards[sid].active.lock();
        merge_into(&mut active, hash, StagedMutation::Put { value, lsn });
    }

    pub(crate) fn merge_delete(&self, hash: Hash8, lsn: Lsn) {
        let sid = self.shard_idx(&hash);
        let mut active = self.shards[sid].active.lock();
        merge_into(&mut active, hash, StagedMutation::Delete { lsn });
    }

    /// Re-merge an already-built mutation back into `active`
    /// (last-LSN-wins). Used by the drainer's error-rollback path to
    /// restore a snapshot whose cuckoo write failed; entries staged after
    /// the swap keep priority via their higher LSNs.
    pub(crate) fn merge_mutation(&self, hash: Hash8, m: StagedMutation) {
        let sid = self.shard_idx(&hash);
        let mut active = self.shards[sid].active.lock();
        merge_into(&mut active, hash, m);
    }

    /// Consult `active` then `draining` (lock order), picking the
    /// higher-LSN mutation. The two locks are taken sequentially (not
    /// held together): correctness relies on the swap holding both locks
    /// while moving the map and on the drainer clearing `draining` only
    /// after the cuckoo write — so a hash is always visible in at least
    /// one slot until it is durable in the cuckoo.
    pub(crate) fn lookup(&self, hash: &Hash8) -> StagedLookup {
        let sid = self.shard_idx(hash);
        let from_active = self.shards[sid].active.lock().get(hash).copied();
        let from_draining = self.shards[sid]
            .draining
            .lock()
            .as_ref()
            .and_then(|m| m.get(hash).copied());
        let chosen = match (from_active, from_draining) {
            (Some(a), Some(d)) => {
                if a.lsn() >= d.lsn() {
                    a
                } else {
                    d
                }
            }
            (Some(a), None) => a,
            (None, Some(d)) => d,
            (None, None) => return StagedLookup::Absent,
        };
        match chosen {
            StagedMutation::Put { value, .. } => StagedLookup::Present(value),
            StagedMutation::Delete { .. } => StagedLookup::Tombstone,
        }
    }

    /// Number of pending entries in a shard's `active` map (drives
    /// threshold wakeups + backpressure).
    pub(crate) fn active_len(&self, shard: usize) -> usize {
        self.shards[shard].active.lock().len()
    }

    pub(crate) fn total_active_len(&self) -> usize {
        self.shards.iter().map(|s| s.active.lock().len()).sum()
    }

    /// Move `active` into the empty `draining` slot, holding both locks
    /// so no entry is momentarily invisible. Returns the moved entries
    /// for the caller to apply to the cuckoo (the entries stay visible
    /// to readers via the `draining` slot until [`clear_draining`]).
    /// Returns an empty vec when there is nothing to drain.
    pub(crate) fn swap_active_to_draining(&self, shard: usize) -> Vec<(Hash8, StagedMutation)> {
        let s = &self.shards[shard];
        let mut active = s.active.lock();
        if active.is_empty() {
            return Vec::new();
        }
        let mut draining = s.draining.lock();
        debug_assert!(
            draining.is_none(),
            "swap_active_to_draining called with a non-empty draining slot"
        );
        let taken = std::mem::take(&mut *active);
        let snapshot: Vec<(Hash8, StagedMutation)> = taken.iter().map(|(h, m)| (*h, *m)).collect();
        *draining = Some(taken);
        snapshot
    }

    /// Clear the `draining` slot. The caller MUST have already applied
    /// every draining entry to the cuckoo (so reads now find them on
    /// disk) before calling this.
    pub(crate) fn clear_draining(&self, shard: usize) {
        *self.shards[shard].draining.lock() = None;
    }

    /// Snapshot every staged mutation across all shards (active +
    /// draining, last-LSN-wins) for callers that need a complete view of
    /// the dedup_index including not-yet-drained entries (e.g. the soak
    /// reference model via `DedupIndex::iter`). Cold path.
    pub(crate) fn snapshot_all(&self) -> HashMap<Hash8, StagedMutation> {
        let mut out: HashMap<Hash8, StagedMutation> = HashMap::new();
        for s in self.shards.iter() {
            // draining first, then active overlays it (active is newer).
            if let Some(d) = s.draining.lock().as_ref() {
                for (h, m) in d.iter() {
                    merge_into(&mut out, *h, *m);
                }
            }
            for (h, m) in s.active.lock().iter() {
                merge_into(&mut out, *h, *m);
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(b: u8) -> Hash8 {
        [b; 8]
    }
    fn dv(b: u8) -> DedupValue {
        let mut x = [0u8; 28];
        x[0] = b;
        DedupValue(x)
    }

    #[test]
    fn put_then_lookup() {
        let s = DedupStaging::new(4);
        s.merge_put(h(1), dv(7), 100);
        assert_eq!(s.lookup(&h(1)), StagedLookup::Present(dv(7)));
        assert_eq!(s.lookup(&h(2)), StagedLookup::Absent);
    }

    #[test]
    fn delete_tombstones() {
        let s = DedupStaging::new(4);
        s.merge_put(h(1), dv(7), 100);
        s.merge_delete(h(1), 101);
        assert_eq!(s.lookup(&h(1)), StagedLookup::Tombstone);
    }

    #[test]
    fn last_lsn_wins_regardless_of_call_order() {
        // A higher-LSN op already staged must NOT be overwritten by a
        // lower-LSN op arriving later (the concurrent-out-of-order case).
        let s = DedupStaging::new(4);
        s.merge_put(h(1), dv(9), 200); // higher LSN first
        s.merge_put(h(1), dv(7), 100); // lower LSN arrives later
        assert_eq!(s.lookup(&h(1)), StagedLookup::Present(dv(9)));
        // Equal-LSN follow-up keeps the later op.
        s.merge_delete(h(1), 200);
        assert_eq!(s.lookup(&h(1)), StagedLookup::Tombstone);
    }

    #[test]
    fn swap_keeps_entries_visible_then_clears() {
        let s = DedupStaging::new(1);
        s.merge_put(h(1), dv(7), 100);
        let snap = s.swap_active_to_draining(0);
        assert_eq!(
            snap,
            vec![(
                h(1),
                StagedMutation::Put {
                    value: dv(7),
                    lsn: 100
                }
            )]
        );
        // Still visible via the draining slot.
        assert_eq!(s.lookup(&h(1)), StagedLookup::Present(dv(7)));
        // A newer mutation lands in the fresh active map and shadows.
        s.merge_delete(h(1), 101);
        assert_eq!(s.lookup(&h(1)), StagedLookup::Tombstone);
        s.clear_draining(0);
        // active still has the newer delete.
        assert_eq!(s.lookup(&h(1)), StagedLookup::Tombstone);
    }

    #[test]
    fn snapshot_all_merges_active_over_draining() {
        let s = DedupStaging::new(2);
        s.merge_put(h(1), dv(1), 100);
        s.merge_put(h(2), dv(2), 100);
        s.swap_active_to_draining(s.shard_idx(&h(1)));
        s.swap_active_to_draining(s.shard_idx(&h(2)));
        // h(1) gets a newer value in active after the swap.
        s.merge_put(h(1), dv(9), 200);
        let snap = s.snapshot_all();
        assert_eq!(
            snap.get(&h(1)),
            Some(&StagedMutation::Put {
                value: dv(9),
                lsn: 200
            })
        );
        assert_eq!(
            snap.get(&h(2)),
            Some(&StagedMutation::Put {
                value: dv(2),
                lsn: 100
            })
        );
    }
}
