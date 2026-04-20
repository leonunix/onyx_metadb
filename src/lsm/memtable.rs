//! In-memory sorted buffer of pending LSM ops.
//!
//! # Model
//!
//! Two slots:
//! - **active**: accepts writes from the commit path.
//! - **frozen**: a previous `active` that is being flushed to an L0 SST.
//!   The frozen slot is visible to readers (they consult active, then
//!   frozen) so a freshly frozen memtable is never invisible to point
//!   lookups.
//!
//! [`Memtable::freeze`] moves the active `BTreeMap` into the frozen slot
//! and leaves a fresh empty active behind. [`Memtable::release_frozen`] is
//! called by the flusher once the corresponding SST is durable.
//!
//! Only one frozen slot at a time — the flusher has to finish before the
//! next freeze. A second freeze returns [`MetaDbError::InvalidArgument`]
//! rather than blocking, because `Memtable` intentionally does not
//! coordinate with the flush scheduler.
//!
//! # Concurrency
//!
//! One `RwLock` per `Memtable`. Reads take the shared side, writes
//! (including `freeze` / `release_frozen`) take the exclusive side. A
//! point lookup touches two `BTreeMap::get` calls under the read lock —
//! short enough that lock contention is a non-issue until we push past
//! multi-hundred-thousand ops/s. If/when it matters, we can swap in a
//! skip-list; the public API was designed to allow that.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::error::{MetaDbError, Result};

use super::format::{DedupValue, Hash32, LSM_RECORD_SIZE};

/// One logical op as stored in the memtable.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DedupOp {
    Put(DedupValue),
    Delete,
}

/// Result of a single-level point lookup.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum LookupResult {
    /// Key is live with the given value.
    Hit(DedupValue),
    /// Key is explicitly tombstoned at this level. Callers MUST NOT
    /// consult older levels — the tombstone shadows them.
    Tombstone,
    /// Key is absent at this level. Callers should consult older levels.
    Miss,
}

/// Summary counters for the memtable. All counts are snapshots taken
/// under the read lock; numbers can drift by the time the caller uses
/// them.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct MemtableStats {
    pub active_entries: usize,
    pub frozen_entries: usize,
}

impl MemtableStats {
    /// Approximate serialized size of the active memtable, in bytes.
    /// Used to decide when to freeze.
    pub fn active_bytes(&self) -> usize {
        self.active_entries * LSM_RECORD_SIZE
    }

    /// Approximate serialized size of the frozen memtable, in bytes.
    pub fn frozen_bytes(&self) -> usize {
        self.frozen_entries * LSM_RECORD_SIZE
    }
}

/// An active-plus-frozen sorted map of pending ops.
#[derive(Debug)]
pub struct Memtable {
    inner: RwLock<Inner>,
    bytes_limit: usize,
}

#[derive(Debug)]
struct Inner {
    active: BTreeMap<Hash32, DedupOp>,
    frozen: Option<Arc<BTreeMap<Hash32, DedupOp>>>,
}

impl Memtable {
    /// Build an empty memtable with the given freeze threshold.
    ///
    /// `bytes_limit` is checked against the approximate serialized size
    /// of the active map (`active_entries * LSM_RECORD_SIZE`); the
    /// caller is expected to flush once [`Memtable::should_freeze`]
    /// returns true.
    pub fn new(bytes_limit: usize) -> Self {
        Self {
            inner: RwLock::new(Inner {
                active: BTreeMap::new(),
                frozen: None,
            }),
            bytes_limit,
        }
    }

    /// Record a `hash → value` put.
    pub fn put(&self, hash: Hash32, value: DedupValue) {
        let mut inner = self.inner.write();
        inner.active.insert(hash, DedupOp::Put(value));
    }

    /// Record a tombstone for `hash`.
    pub fn delete(&self, hash: Hash32) {
        let mut inner = self.inner.write();
        inner.active.insert(hash, DedupOp::Delete);
    }

    /// Point lookup across active then frozen. The frozen slot is older
    /// than active, so active wins whenever both have an entry.
    pub fn get(&self, hash: &Hash32) -> LookupResult {
        let inner = self.inner.read();
        if let Some(op) = inner.active.get(hash) {
            return op_to_lookup(*op);
        }
        if let Some(frozen) = &inner.frozen {
            if let Some(op) = frozen.get(hash) {
                return op_to_lookup(*op);
            }
        }
        LookupResult::Miss
    }

    /// Whether the active memtable has reached its freeze threshold.
    pub fn should_freeze(&self) -> bool {
        let inner = self.inner.read();
        inner.active.len() * LSM_RECORD_SIZE >= self.bytes_limit
    }

    /// Whether a frozen memtable is currently awaiting flush.
    pub fn has_frozen(&self) -> bool {
        self.inner.read().frozen.is_some()
    }

    /// Whether active and frozen are both empty.
    pub fn is_empty(&self) -> bool {
        let inner = self.inner.read();
        inner.active.is_empty() && inner.frozen.is_none()
    }

    /// Snapshot summary counters.
    pub fn stats(&self) -> MemtableStats {
        let inner = self.inner.read();
        MemtableStats {
            active_entries: inner.active.len(),
            frozen_entries: inner.frozen.as_ref().map(|m| m.len()).unwrap_or(0),
        }
    }

    /// Freeze the active map.
    ///
    /// Returns a shared handle to the frozen `BTreeMap` for the flusher
    /// to iterate and serialize. Fails with
    /// [`MetaDbError::InvalidArgument`] if a previous frozen memtable
    /// hasn't been released yet.
    pub fn freeze(&self) -> Result<Arc<BTreeMap<Hash32, DedupOp>>> {
        let mut inner = self.inner.write();
        if inner.frozen.is_some() {
            return Err(MetaDbError::InvalidArgument(
                "memtable already has a frozen slot awaiting flush".into(),
            ));
        }
        let active = std::mem::take(&mut inner.active);
        let frozen = Arc::new(active);
        inner.frozen = Some(frozen.clone());
        Ok(frozen)
    }

    /// Drop the frozen slot. Called by the flusher after the SST is
    /// durable on disk and registered in the manifest.
    pub fn release_frozen(&self) -> Result<()> {
        let mut inner = self.inner.write();
        if inner.frozen.take().is_none() {
            return Err(MetaDbError::InvalidArgument(
                "no frozen memtable to release".into(),
            ));
        }
        Ok(())
    }

    /// Borrow the frozen map for the flusher. Returns `None` if no
    /// freeze is pending.
    pub fn frozen_snapshot(&self) -> Option<Arc<BTreeMap<Hash32, DedupOp>>> {
        self.inner.read().frozen.clone()
    }
}

fn op_to_lookup(op: DedupOp) -> LookupResult {
    match op {
        DedupOp::Put(v) => LookupResult::Hit(v),
        DedupOp::Delete => LookupResult::Tombstone,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(n: u8) -> Hash32 {
        let mut x = [0u8; 32];
        x[0] = n;
        x
    }

    fn v(n: u8) -> DedupValue {
        let mut x = [0u8; 28];
        x[0] = n;
        DedupValue(x)
    }

    #[test]
    fn empty_lookup_misses() {
        let m = Memtable::new(1_024);
        assert_eq!(m.get(&h(0)), LookupResult::Miss);
        assert!(m.is_empty());
        assert!(!m.has_frozen());
    }

    #[test]
    fn put_then_get_hit() {
        let m = Memtable::new(1_024);
        m.put(h(1), v(42));
        assert_eq!(m.get(&h(1)), LookupResult::Hit(v(42)));
        assert_eq!(m.get(&h(2)), LookupResult::Miss);
    }

    #[test]
    fn put_overwrites_previous_value() {
        let m = Memtable::new(1_024);
        m.put(h(1), v(1));
        m.put(h(1), v(2));
        assert_eq!(m.get(&h(1)), LookupResult::Hit(v(2)));
    }

    #[test]
    fn delete_yields_tombstone() {
        let m = Memtable::new(1_024);
        m.put(h(1), v(1));
        m.delete(h(1));
        assert_eq!(m.get(&h(1)), LookupResult::Tombstone);
    }

    #[test]
    fn put_after_delete_yields_hit() {
        let m = Memtable::new(1_024);
        m.delete(h(1));
        m.put(h(1), v(99));
        assert_eq!(m.get(&h(1)), LookupResult::Hit(v(99)));
    }

    #[test]
    fn should_freeze_tracks_threshold() {
        let m = Memtable::new(3 * LSM_RECORD_SIZE);
        assert!(!m.should_freeze());
        m.put(h(1), v(1));
        m.put(h(2), v(2));
        assert!(!m.should_freeze());
        m.put(h(3), v(3));
        assert!(m.should_freeze());
    }

    #[test]
    fn stats_report_entry_counts() {
        let m = Memtable::new(1_024);
        m.put(h(1), v(1));
        m.put(h(2), v(2));
        m.delete(h(3));
        let s = m.stats();
        assert_eq!(s.active_entries, 3);
        assert_eq!(s.frozen_entries, 0);
        assert_eq!(s.active_bytes(), 3 * LSM_RECORD_SIZE);
    }

    #[test]
    fn freeze_moves_entries_and_leaves_active_empty() {
        let m = Memtable::new(1_024);
        for i in 0..5u8 {
            m.put(h(i), v(i));
        }
        let frozen = m.freeze().unwrap();
        assert_eq!(frozen.len(), 5);
        assert!(m.has_frozen());
        assert_eq!(m.stats().active_entries, 0);
        assert_eq!(m.stats().frozen_entries, 5);
        // Reads still see the pre-freeze entries because the lookup
        // walks active → frozen.
        assert_eq!(m.get(&h(3)), LookupResult::Hit(v(3)));
    }

    #[test]
    fn active_wins_over_frozen() {
        let m = Memtable::new(1_024);
        m.put(h(1), v(1));
        m.freeze().unwrap();
        m.put(h(1), v(2));
        // Active should shadow the stale frozen entry.
        assert_eq!(m.get(&h(1)), LookupResult::Hit(v(2)));
    }

    #[test]
    fn tombstone_in_active_shadows_frozen_put() {
        let m = Memtable::new(1_024);
        m.put(h(1), v(1));
        m.freeze().unwrap();
        m.delete(h(1));
        assert_eq!(m.get(&h(1)), LookupResult::Tombstone);
    }

    #[test]
    fn double_freeze_fails_before_release() {
        let m = Memtable::new(1_024);
        m.put(h(1), v(1));
        m.freeze().unwrap();
        match m.freeze().unwrap_err() {
            MetaDbError::InvalidArgument(_) => {}
            e => panic!("{e}"),
        }
    }

    #[test]
    fn release_frozen_without_freeze_fails() {
        let m = Memtable::new(1_024);
        match m.release_frozen().unwrap_err() {
            MetaDbError::InvalidArgument(_) => {}
            e => panic!("{e}"),
        }
    }

    #[test]
    fn freeze_release_freeze_round_trip() {
        let m = Memtable::new(1_024);
        m.put(h(1), v(1));
        m.freeze().unwrap();
        m.release_frozen().unwrap();
        assert!(!m.has_frozen());
        // Frozen entry must be gone after release.
        assert_eq!(m.get(&h(1)), LookupResult::Miss);
        m.put(h(2), v(2));
        m.freeze().unwrap();
    }

    #[test]
    fn frozen_snapshot_is_shared_arc() {
        let m = Memtable::new(1_024);
        m.put(h(1), v(1));
        let a = m.freeze().unwrap();
        let b = m.frozen_snapshot().unwrap();
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn frozen_snapshot_missing_returns_none() {
        let m = Memtable::new(1_024);
        assert!(m.frozen_snapshot().is_none());
    }
}
