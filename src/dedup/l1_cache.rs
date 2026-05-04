//! L1: in-memory hot cache mapping `fp → (full hash, DedupValue)`.
//!
//! Sits between [L0](crate::dedup::FpSketch) and the on-disk cuckoo
//! table. Workflow:
//!
//! 1. `lookup(fp, full_hash)` returns the cached `DedupValue` only
//!    when the cached *full* hash matches the caller's hash; an L1
//!    hit on a colliding fingerprint reports `Miss` so the caller
//!    falls through to the on-disk index.
//! 2. `put(fp, full_hash, value)` records a fresh entry, evicting
//!    the LRU tail when the bound is reached.
//! 3. `evict(fp)` (on dedup delete) drops the cached entry. Because
//!    the cache is keyed by `fp` and an fp collision could have two
//!    distinct cached entries fighting for the same slot, callers
//!    should treat L1 as a best-effort optimisation — correctness
//!    must always be backed by L3.
//!
//! Implementation reuses the `lru` crate that's already a metadb
//! dependency.

use parking_lot::Mutex;

use crate::dedup_types::{DedupValue, Hash8};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CachedHit {
    pub hash: Hash8,
    pub value: DedupValue,
}

pub enum LookupResult {
    /// Cache hit and the cached full hash matches the caller's hash.
    Hit(DedupValue),
    /// Cache held a different hash for this fingerprint (collision)
    /// or no entry at all.
    Miss,
}

pub struct L1HotCache {
    inner: Mutex<lru::LruCache<u32, CachedHit>>,
}

impl L1HotCache {
    pub fn new(capacity_entries: usize) -> Self {
        let cap = std::num::NonZeroUsize::new(capacity_entries.max(1))
            .expect("L1HotCache capacity is at least 1");
        Self {
            inner: Mutex::new(lru::LruCache::new(cap)),
        }
    }

    /// Look up the cached entry for `fp`. Returns [`LookupResult::Hit`]
    /// only when both the fingerprint *and* the full hash match the
    /// caller; otherwise reports a miss. The matching variant
    /// promotes the entry to the LRU head; the miss variant does
    /// nothing.
    pub fn lookup(&self, fp: u32, full_hash: &Hash8) -> LookupResult {
        let mut cache = self.inner.lock();
        match cache.get(&fp) {
            Some(entry) if &entry.hash == full_hash => LookupResult::Hit(entry.value),
            _ => LookupResult::Miss,
        }
    }

    /// Batched form of [`lookup`]. Holds the LRU mutex across `pairs`
    /// so callers checking N fingerprints pay one lock acquisition.
    /// Each hit promotes its entry to the LRU head, matching the
    /// per-call semantics.
    pub fn lookup_batch(&self, pairs: &[(u32, Hash8)]) -> Vec<LookupResult> {
        let mut cache = self.inner.lock();
        pairs
            .iter()
            .map(|(fp, full_hash)| match cache.get(fp) {
                Some(entry) if &entry.hash == full_hash => LookupResult::Hit(entry.value),
                _ => LookupResult::Miss,
            })
            .collect()
    }

    /// Insert / refresh the cached entry for `fp`. Overwrites any
    /// previous entry (including a colliding-fingerprint one — the
    /// new owner wins because it is the most recently accessed).
    pub fn put(&self, fp: u32, hash: Hash8, value: DedupValue) {
        self.inner.lock().put(fp, CachedHit { hash, value });
    }

    /// Drop the cached entry for `fp`. Called from the dedup delete
    /// path. If the cached entry belonged to a *different* hash than
    /// the one being deleted (fp collision case), the eviction loses
    /// a still-valid cache line; the next lookup will repopulate it
    /// from L3, which is functionally correct.
    pub fn evict(&self, fp: u32) {
        self.inner.lock().pop(&fp);
    }

    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().is_empty()
    }

    /// Reset to empty. Used during `Db::open` once L3 is loaded so
    /// the cache starts cold, or when the operator forces a reset.
    pub fn clear(&self) {
        self.inner.lock().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dv(b: u8) -> DedupValue {
        let mut x = [0u8; 28];
        x[0] = b;
        DedupValue(x)
    }

    fn hash_for(byte: u8) -> Hash8 {
        let mut h = [0u8; 8];
        h.fill(byte);
        h
    }

    #[test]
    fn empty_lookup_is_miss() {
        let c = L1HotCache::new(8);
        assert!(matches!(c.lookup(1, &hash_for(0xAA)), LookupResult::Miss));
    }

    #[test]
    fn put_then_lookup_hits() {
        let c = L1HotCache::new(8);
        let h = hash_for(0xAA);
        c.put(1, h, dv(7));
        match c.lookup(1, &h) {
            LookupResult::Hit(v) => assert_eq!(v, dv(7)),
            LookupResult::Miss => panic!("expected hit"),
        }
    }

    #[test]
    fn collision_is_reported_as_miss() {
        let c = L1HotCache::new(8);
        let h_a = hash_for(0xAA);
        let h_b = hash_for(0xBB);
        c.put(1, h_a, dv(7));
        // Same fp, different hash — must report Miss so caller goes
        // to L3.
        assert!(matches!(c.lookup(1, &h_b), LookupResult::Miss));
    }

    #[test]
    fn put_overwrites_collision() {
        let c = L1HotCache::new(8);
        let h_a = hash_for(0xAA);
        let h_b = hash_for(0xBB);
        c.put(1, h_a, dv(7));
        c.put(1, h_b, dv(9));
        assert!(matches!(c.lookup(1, &h_a), LookupResult::Miss));
        match c.lookup(1, &h_b) {
            LookupResult::Hit(v) => assert_eq!(v, dv(9)),
            LookupResult::Miss => panic!("h_b should hit after overwrite"),
        }
    }

    #[test]
    fn evict_drops_entry() {
        let c = L1HotCache::new(8);
        let h = hash_for(0xAA);
        c.put(1, h, dv(7));
        c.evict(1);
        assert!(matches!(c.lookup(1, &h), LookupResult::Miss));
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn lru_eviction_when_full() {
        let c = L1HotCache::new(2);
        c.put(1, hash_for(1), dv(1));
        c.put(2, hash_for(2), dv(2));
        // Insert a 3rd entry — fp=1 is the LRU and should fall out.
        c.put(3, hash_for(3), dv(3));
        assert!(matches!(c.lookup(1, &hash_for(1)), LookupResult::Miss));
        // Touching fp=2 promotes it; fp=3 stays.
        match c.lookup(2, &hash_for(2)) {
            LookupResult::Hit(v) => assert_eq!(v, dv(2)),
            _ => panic!("fp=2 should still be cached"),
        }
        match c.lookup(3, &hash_for(3)) {
            LookupResult::Hit(v) => assert_eq!(v, dv(3)),
            _ => panic!("fp=3 should still be cached"),
        }
    }

    #[test]
    fn clear_drops_everything() {
        let c = L1HotCache::new(8);
        c.put(1, hash_for(1), dv(1));
        c.put(2, hash_for(2), dv(2));
        c.clear();
        assert!(c.is_empty());
    }

    #[test]
    fn evict_missing_is_noop() {
        let c = L1HotCache::new(8);
        c.evict(99);
        assert!(c.is_empty());
    }
}
