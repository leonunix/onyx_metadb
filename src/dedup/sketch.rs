//! L0: in-memory 32-bit fingerprint set with reference counting.
//!
//! # Why ref count
//!
//! A naive `HashSet<u32>` works for `insert` but breaks on `remove`.
//! With ~50 M entries in a 32-bit fingerprint space the birthday
//! probability puts ≈ 0.3% of fingerprints at a collision. If we
//! remove the fingerprint on the first `delete` and another hash with
//! the same fp is still live, the next L0 lookup for *that* hash
//! short-circuits to MISS — the caller would then write a duplicate
//! PBA for content that was already deduped. Tracking how many live
//! hashes share each fp keeps remove correct in the face of
//! collisions; the per-entry overhead is one `u32` (12 B total per
//! entry vs 4 B for `HashSet`). At 50 M entries that's 600 MiB,
//! comfortably within the dedup_l0_sketch_bytes budget.
//!
//! Stage 3.x can swap the implementation for a cuckoo filter to cut
//! memory ~3× without losing remove correctness; the public API is
//! intentionally minimal so the swap is private to this module.

use std::collections::HashMap;

use parking_lot::RwLock;

pub struct FpSketch {
    inner: RwLock<HashMap<u32, u32>>,
}

impl FpSketch {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: RwLock::new(HashMap::with_capacity(cap)),
        }
    }

    /// `true` iff at least one live hash currently has this fingerprint.
    pub fn contains(&self, fp: u32) -> bool {
        self.inner.read().contains_key(&fp)
    }

    /// Increment the reference count for `fp`.
    pub fn insert(&self, fp: u32) {
        *self.inner.write().entry(fp).or_insert(0) += 1;
    }

    /// Decrement the reference count for `fp`; remove the entry when
    /// the count reaches zero. Calling `remove` for a fingerprint
    /// that was never inserted is a no-op (matches the LSM dedup
    /// `delete` semantics — tombstoning a missing key is fine).
    pub fn remove(&self, fp: u32) {
        let mut g = self.inner.write();
        let drop_entry = if let Some(count) = g.get_mut(&fp) {
            if *count > 1 {
                *count -= 1;
                false
            } else {
                true
            }
        } else {
            false
        };
        if drop_entry {
            g.remove(&fp);
        }
    }

    /// Number of distinct fingerprints currently tracked. Note:
    /// `len() < live_hash_count` whenever any fp collisions exist.
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// Approximate in-memory cost (HashMap overhead + entry size).
    /// Used by the operator-facing status report.
    pub fn approx_bytes(&self) -> usize {
        // HashMap<u32,u32> Rust default hasher: ~24 B / entry once
        // load factor and bucket overhead are counted.
        self.len() * 24
    }

    /// Reset to empty. Used when a fresh database is created or
    /// when L0 is being rebuilt from L3.
    pub fn clear(&self) {
        self.inner.write().clear();
    }
}

impl Default for FpSketch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_sketch_contains_nothing() {
        let s = FpSketch::new();
        assert!(!s.contains(0xDEAD_BEEF));
        assert_eq!(s.len(), 0);
        assert!(s.is_empty());
    }

    #[test]
    fn insert_then_contains() {
        let s = FpSketch::new();
        s.insert(0xDEAD_BEEF);
        assert!(s.contains(0xDEAD_BEEF));
        assert!(!s.contains(0xCAFE_F00D));
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn remove_decrements_then_drops() {
        let s = FpSketch::new();
        s.insert(7);
        s.insert(7);
        assert!(s.contains(7));
        s.remove(7);
        assert!(s.contains(7), "still one ref alive");
        s.remove(7);
        assert!(!s.contains(7), "ref count reached zero");
    }

    #[test]
    fn remove_missing_is_noop() {
        let s = FpSketch::new();
        s.remove(42);
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn collision_survives_partial_remove() {
        let s = FpSketch::new();
        // Two distinct hashes share fp = 1.
        s.insert(1);
        s.insert(1);
        assert!(s.contains(1));
        // Drop one — the other is still live.
        s.remove(1);
        assert!(s.contains(1), "L0 must keep fp while any live hash uses it");
        s.remove(1);
        assert!(!s.contains(1));
    }

    #[test]
    fn many_distinct_fps_grow_len() {
        let s = FpSketch::new();
        for i in 0..1000u32 {
            s.insert(i);
        }
        assert_eq!(s.len(), 1000);
        for i in 0..500u32 {
            s.remove(i);
        }
        assert_eq!(s.len(), 500);
    }

    #[test]
    fn clear_drops_everything() {
        let s = FpSketch::new();
        s.insert(1);
        s.insert(2);
        s.clear();
        assert!(s.is_empty());
    }
}
