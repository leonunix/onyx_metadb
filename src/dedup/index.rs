//! `DedupIndex`: four-tier dedup_index facade.
//!
//! Composes [L0 sketch](super::FpSketch), [L1 hot
//! cache](super::L1HotCache), and the [L3 cuckoo table](super::CuckooHash)
//! into a single `(hash → value)` store with the same shape as the
//! legacy `ShardedLsm` it replaces.
//!
//! # Read flow
//!
//! ```text
//! lookup(hash):
//!   fp = fp_of(hash)
//!   if !L0.contains(fp) → return None              // 99% all-miss path
//!   match L1.lookup(fp, hash):
//!     Hit(value) → return Some(value)
//!     Miss → consult L3
//!   match L3.get(hash):
//!     Some(value) → L1.put(fp, hash, value); return Some(value)
//!     None → return None
//! ```
//!
//! # Write flow
//!
//! ```text
//! put(hash, value):
//!   L3.put(hash, value, lsn)?
//!   L0.insert(fp(hash))
//!   L1.put(fp(hash), hash, value)
//!
//! delete(hash):
//!   L3.delete(hash, lsn)?
//!   L0.remove(fp(hash))
//!   L1.evict(fp(hash))
//! ```
//!
//! # Open
//!
//! `open()` walks L3 once with [`CuckooHash::iter`] to repopulate the
//! L0 sketch so `lookup` can short-circuit the all-miss path
//! immediately. L1 starts empty and warms up under traffic.

use std::sync::Arc;

use crate::cache::PageCache;
use crate::dedup_types::{DedupValue, Hash8};
use crate::error::Result;
use crate::page_store::PageStore;
use crate::types::{Lsn, PageId};

use super::cuckoo::{CuckooHash, ENTRIES_PER_BUCKET};
use super::fp_of;
use super::l1_cache::{L1HotCache, LookupResult};
use super::sketch::FpSketch;

/// L0 capacity to use given the on-disk cuckoo bucket count. Mirrors
/// L3 max capacity (`bucket_count × ENTRIES_PER_BUCKET`) so the filter
/// can hold every fingerprint L3 can store without saturating.
fn l0_capacity_for(cuckoo_bucket_count: u64) -> usize {
    (cuckoo_bucket_count as usize).saturating_mul(ENTRIES_PER_BUCKET)
}

pub struct DedupIndex {
    sketch: FpSketch,
    l1: L1HotCache,
    cuckoo: CuckooHash,
}

impl DedupIndex {
    /// Build a fresh dedup index. `bucket_count` sizes the on-disk
    /// cuckoo table; pick `entries_target / (4 × load_factor_target)`
    /// where `load_factor_target` is typically 0.85. `l1_capacity`
    /// is the maximum number of `(fp → hash, value)` entries kept in
    /// the in-memory L1 LRU.
    pub fn create(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        bucket_count: u64,
        l1_capacity: usize,
        seed1: u64,
        seed2: u64,
    ) -> Result<Self> {
        let cuckoo = CuckooHash::create(page_store, page_cache, bucket_count, seed1, seed2)?;
        let l0_capacity = l0_capacity_for(cuckoo.bucket_count());
        Ok(Self {
            sketch: FpSketch::with_capacity(l0_capacity),
            l1: L1HotCache::new(l1_capacity),
            cuckoo,
        })
    }

    /// Reopen at `meta_page_id` (recorded in the manifest). Walks L3
    /// once to repopulate L0; L1 starts empty.
    pub fn open(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        meta_page_id: PageId,
        l1_capacity: usize,
    ) -> Result<Self> {
        let cuckoo = CuckooHash::open(page_store, page_cache, meta_page_id)?;
        // Size L0 to mirror the on-disk cuckoo capacity rather than the
        // current load. The cuckoo filter cannot grow once allocated;
        // sizing it at 4 × bucket_count keeps load < 0.95 even after
        // L3 fills up, avoiding the saturation fallback.
        let sketch = FpSketch::with_capacity(l0_capacity_for(cuckoo.bucket_count()));
        let l1 = L1HotCache::new(l1_capacity);
        let me = Self { sketch, l1, cuckoo };
        me.rebuild_l0_from_l3()?;
        Ok(me)
    }

    fn rebuild_l0_from_l3(&self) -> Result<()> {
        self.cuckoo.for_each(|hash, _value| {
            self.sketch.insert(fp_of(&hash));
            Ok(())
        })
    }

    pub fn meta_page_id(&self) -> PageId {
        self.cuckoo.meta_page_id()
    }

    pub fn bucket_count(&self) -> u64 {
        self.cuckoo.bucket_count()
    }

    /// Single-key lookup. Walks L0 → L1 → L3. Promotes L3 hits into
    /// L1 so the next lookup short-circuits to memory.
    pub fn get(&self, hash: &Hash8) -> Result<Option<DedupValue>> {
        let fp = fp_of(hash);
        if !self.sketch.contains(fp) {
            return Ok(None);
        }
        if let LookupResult::Hit(value) = self.l1.lookup(fp, hash) {
            return Ok(Some(value));
        }
        match self.cuckoo.get(hash)? {
            Some(value) => {
                self.l1.put(fp, *hash, value);
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Batched lookup. Output order matches input order. Holds the L0
    /// read lock once across every fingerprint check, then the L1 mutex
    /// once across the surviving candidates, before falling through to
    /// L3 only for the residual misses. Most workloads see 90 %+ of
    /// hashes short-circuit in the L0 sketch, so this collapses N ×
    /// (L0 + L1) lock pairs to two.
    pub fn multi_get(&self, hashes: &[Hash8]) -> Result<Vec<Option<DedupValue>>> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let fps: Vec<u32> = hashes.iter().map(fp_of).collect();
        let in_l0 = self.sketch.contains_batch(&fps);

        // Collect indices that survived L0 — these are the only ones
        // worth touching L1 / L3 for.
        let mut l1_pairs: Vec<(u32, Hash8)> = Vec::new();
        let mut l1_indices: Vec<usize> = Vec::new();
        for (i, &alive) in in_l0.iter().enumerate() {
            if alive {
                l1_pairs.push((fps[i], hashes[i]));
                l1_indices.push(i);
            }
        }
        let l1_results = self.l1.lookup_batch(&l1_pairs);

        let mut out: Vec<Option<DedupValue>> = vec![None; hashes.len()];
        for ((idx, l1_result), pair) in l1_indices.iter().zip(l1_results).zip(l1_pairs.iter()) {
            match l1_result {
                LookupResult::Hit(value) => out[*idx] = Some(value),
                LookupResult::Miss => match self.cuckoo.get(&pair.1)? {
                    Some(value) => {
                        self.l1.put(pair.0, pair.1, value);
                        out[*idx] = Some(value);
                    }
                    None => out[*idx] = None,
                },
            }
        }
        Ok(out)
    }

    pub fn put(&self, hash: Hash8, value: DedupValue, lsn: Lsn) -> Result<()> {
        self.cuckoo.put(hash, value, lsn)?;
        let fp = fp_of(&hash);
        self.sketch.insert(fp);
        self.l1.put(fp, hash, value);
        Ok(())
    }

    pub fn delete(&self, hash: &Hash8, lsn: Lsn) -> Result<()> {
        // Order matters: clear L3 first so a concurrent reader that
        // sees fp ∈ L0 falls through to L3 and gets `None`. After
        // L3 returns clear, removing fp from L0 is safe.
        //
        // Only update L0 / L1 when L3 actually had this entry. The L0
        // sketch is reference-counted by fingerprint; multiple distinct
        // hashes sharing the low 32 bits land on the same fp slot, so
        // an unconditional `sketch.remove` for an absent hash would
        // evict L0 reservations belonging to live siblings.
        let removed = self.cuckoo.delete(hash, lsn)?;
        if removed {
            let fp = fp_of(hash);
            self.sketch.remove(fp);
            self.l1.evict(fp);
        }
        Ok(())
    }

    pub fn flush_meta(&self) -> Result<bool> {
        self.cuckoo.flush_meta()
    }

    pub fn iter(&self) -> Result<Vec<(Hash8, DedupValue)>> {
        self.cuckoo.iter()
    }

    /// Walk every allocated data page id (used by verifier).
    pub fn data_page_ids(&self) -> Vec<PageId> {
        self.cuckoo.data_page_ids()
    }

    /// Approximate live entry count. Tracks the cuckoo's running
    /// counter; for an exact figure call [`recount`].
    pub fn approx_len(&self) -> u64 {
        self.cuckoo.approx_len()
    }

    pub fn recount(&self) -> Result<u64> {
        self.cuckoo.recount()
    }

    /// In-memory tier sizes for status / soak metrics.
    pub fn tier_sizes(&self) -> TierSizes {
        TierSizes {
            l0_distinct_fps: self.sketch.len(),
            l0_approx_bytes: self.sketch.approx_bytes(),
            l1_entries: self.l1.len(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TierSizes {
    pub l0_distinct_fps: usize,
    pub l0_approx_bytes: usize,
    pub l1_entries: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_index() -> (TempDir, DedupIndex) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let page_store = Arc::new(PageStore::create(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let idx = DedupIndex::create(page_store, page_cache, 64, 16, 0xDEAD, 0xBEEF).unwrap();
        (dir, idx)
    }

    fn h(byte: u8) -> Hash8 {
        let mut x = [0u8; 8];
        x.fill(byte);
        x
    }

    fn dv(byte: u8) -> DedupValue {
        let mut x = [0u8; 28];
        x[0] = byte;
        DedupValue(x)
    }

    #[test]
    fn empty_get_short_circuits_at_l0() {
        let (_d, idx) = make_index();
        assert_eq!(idx.get(&h(0xAA)).unwrap(), None);
        assert_eq!(idx.tier_sizes().l0_distinct_fps, 0);
    }

    #[test]
    fn put_then_get_round_trip() {
        let (_d, idx) = make_index();
        idx.put(h(0xAA), dv(7), 100).unwrap();
        assert_eq!(idx.get(&h(0xAA)).unwrap(), Some(dv(7)));
        // L0 + L1 populated.
        assert_eq!(idx.tier_sizes().l0_distinct_fps, 1);
        assert_eq!(idx.tier_sizes().l1_entries, 1);
    }

    #[test]
    fn delete_of_absent_hash_does_not_evict_fp_collision_sibling() {
        // Regression for db_phase6_proptest::db_vs_reference_with_reopens.
        // The L0 sketch is reference-counted by 32-bit fingerprint
        // (`fp_of(hash) = u32::from_le_bytes(hash[..4])`). Two distinct
        // hashes that share the same low 4 bytes also share an L0
        // counter slot. Deleting an *absent* hash must not decrement
        // that counter — otherwise the live sibling becomes invisible
        // because L0 short-circuits to `None` first.
        //
        // Pick `a` and `b` that differ only past byte 3 so they share
        // a fingerprint.
        let a = [0u8, 0, 0, 0, 0, 0, 0, 0];
        let b = [0u8, 0, 0, 0, 0, 0, 0, 1];
        assert_eq!(fp_of(&a), fp_of(&b), "test setup must collide");

        let (_d, idx) = make_index();
        idx.put(a, dv(0xAA), 100).unwrap();
        // `b` was never inserted; deleting it is a no-op at L3 and
        // must leave L0 / L1 entries for `a` intact.
        idx.delete(&b, 101).unwrap();
        assert_eq!(
            idx.get(&a).unwrap(),
            Some(dv(0xAA)),
            "fp-collision sibling must still be observable after no-op delete"
        );
    }

    #[test]
    fn put_then_get_all_zero_hash_round_trips() {
        // Regression: the all-zero hash (`Hash8([0; 8])`) maps to fp_of() = 0,
        // and an all-zero DedupValue is a valid payload. Both must round-trip
        // through L0 (sketch) + L1 (cache) + L3 (cuckoo) get.
        let (_d, idx) = make_index();
        let zero_hash = [0u8; 8];
        let zero_value = DedupValue([0u8; 28]);
        idx.put(zero_hash, zero_value, 100).unwrap();
        assert_eq!(
            idx.get(&zero_hash).unwrap(),
            Some(zero_value),
            "all-zero hash + all-zero value must be observable post-put"
        );
        // Also validate via multi_get (fast path used by lookup_dedup_hits).
        let multi = idx.multi_get(&[zero_hash]).unwrap();
        assert_eq!(multi, vec![Some(zero_value)]);
    }

    #[test]
    fn delete_removes_all_tiers() {
        let (_d, idx) = make_index();
        idx.put(h(0xAA), dv(7), 100).unwrap();
        idx.delete(&h(0xAA), 101).unwrap();
        assert_eq!(idx.get(&h(0xAA)).unwrap(), None);
        assert_eq!(idx.tier_sizes().l0_distinct_fps, 0);
        assert_eq!(idx.tier_sizes().l1_entries, 0);
    }

    #[test]
    fn miss_does_not_warm_l1() {
        let (_d, idx) = make_index();
        idx.put(h(0xAA), dv(7), 100).unwrap();
        // Lookup a hash that isn't there: L0 will reject (fp differs),
        // L1 stays empty for it.
        assert_eq!(idx.get(&h(0xBB)).unwrap(), None);
        assert_eq!(idx.tier_sizes().l1_entries, 1, "only the one put hit L1");
    }

    #[test]
    fn batched_multi_get_preserves_order() {
        let (_d, idx) = make_index();
        idx.put(h(1), dv(1), 100).unwrap();
        idx.put(h(2), dv(2), 101).unwrap();
        let got = idx.multi_get(&[h(2), h(99), h(1), h(2)]).unwrap();
        assert_eq!(got, vec![Some(dv(2)), None, Some(dv(1)), Some(dv(2))]);
    }

    #[test]
    fn put_overwrites_value() {
        let (_d, idx) = make_index();
        idx.put(h(0xAA), dv(7), 100).unwrap();
        idx.put(h(0xAA), dv(9), 101).unwrap();
        assert_eq!(idx.get(&h(0xAA)).unwrap(), Some(dv(9)));
        assert_eq!(idx.approx_len(), 1);
    }

    #[test]
    fn open_rebuilds_l0_but_l1_starts_cold() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let meta_page_id;
        {
            let page_store = Arc::new(PageStore::create(&path).unwrap());
            let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
            let idx = DedupIndex::create(page_store, page_cache, 64, 16, 0xDEAD, 0xBEEF).unwrap();
            meta_page_id = idx.meta_page_id();
            for i in 0..30u8 {
                idx.put(h(i), dv(i), (100 + i as u64) as Lsn).unwrap();
            }
            idx.flush_meta().unwrap();
        }
        let page_store = Arc::new(PageStore::open(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let idx = DedupIndex::open(page_store, page_cache, meta_page_id, 16).unwrap();
        // L0 fully restored.
        assert!(idx.tier_sizes().l0_distinct_fps >= 30);
        // L1 starts empty.
        assert_eq!(idx.tier_sizes().l1_entries, 0);
        // Lookups still work; fill L1 along the way.
        for i in 0..30u8 {
            assert_eq!(idx.get(&h(i)).unwrap(), Some(dv(i)));
        }
        assert!(idx.tier_sizes().l1_entries > 0);
    }

    #[test]
    fn iter_yields_all_pairs_in_disk_order() {
        let (_d, idx) = make_index();
        for i in 0..20u8 {
            idx.put(h(i), dv(i), 100).unwrap();
        }
        let mut live = idx.iter().unwrap();
        live.sort_by_key(|(h, _)| h[0]);
        assert_eq!(live.len(), 20);
        for (i, (hash, value)) in live.iter().enumerate() {
            assert_eq!(*hash, h(i as u8));
            assert_eq!(*value, dv(i as u8));
        }
    }
}
