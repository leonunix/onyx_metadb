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
use crate::error::Result;
use crate::lsm::{DedupValue, Hash32};
use crate::page_store::PageStore;
use crate::types::{Lsn, PageId};

use super::cuckoo::CuckooHash;
use super::l1_cache::{L1HotCache, LookupResult};
use super::sketch::FpSketch;
use super::fp_of;

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
        Ok(Self {
            sketch: FpSketch::new(),
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
        let sketch = FpSketch::with_capacity(cuckoo.approx_len() as usize);
        let l1 = L1HotCache::new(l1_capacity);
        let me = Self { sketch, l1, cuckoo };
        me.rebuild_l0_from_l3()?;
        Ok(me)
    }

    fn rebuild_l0_from_l3(&self) -> Result<()> {
        for (hash, _value) in self.cuckoo.iter()? {
            self.sketch.insert(fp_of(&hash));
        }
        Ok(())
    }

    pub fn meta_page_id(&self) -> PageId {
        self.cuckoo.meta_page_id()
    }

    pub fn bucket_count(&self) -> u64 {
        self.cuckoo.bucket_count()
    }

    /// Single-key lookup. Walks L0 → L1 → L3. Promotes L3 hits into
    /// L1 so the next lookup short-circuits to memory.
    pub fn get(&self, hash: &Hash32) -> Result<Option<DedupValue>> {
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

    /// Batched lookup. Output order matches input order.
    pub fn multi_get(&self, hashes: &[Hash32]) -> Result<Vec<Option<DedupValue>>> {
        hashes.iter().map(|h| self.get(h)).collect()
    }

    pub fn put(&self, hash: Hash32, value: DedupValue, lsn: Lsn) -> Result<()> {
        self.cuckoo.put(hash, value, lsn)?;
        let fp = fp_of(&hash);
        self.sketch.insert(fp);
        self.l1.put(fp, hash, value);
        Ok(())
    }

    pub fn delete(&self, hash: &Hash32, lsn: Lsn) -> Result<()> {
        // Order matters: clear L3 first so a concurrent reader that
        // sees fp ∈ L0 falls through to L3 and gets `None`. After
        // L3 returns clear, removing fp from L0 is safe.
        self.cuckoo.delete(hash, lsn)?;
        let fp = fp_of(hash);
        self.sketch.remove(fp);
        self.l1.evict(fp);
        Ok(())
    }

    pub fn flush_meta(&self) -> Result<bool> {
        self.cuckoo.flush_meta()
    }

    pub fn iter(&self) -> Result<Vec<(Hash32, DedupValue)>> {
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

    fn h(byte: u8) -> Hash32 {
        let mut x = [0u8; 32];
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
            let idx =
                DedupIndex::create(page_store, page_cache, 64, 16, 0xDEAD, 0xBEEF).unwrap();
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
