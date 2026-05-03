//! L3: on-disk cuckoo hash table for dedup_index.
//!
//! # Layout
//!
//! - Total bucket count `N` is fixed at `create()` time and recorded
//!   in the meta page. Buckets are grouped into 4 KiB data pages; each
//!   page holds [`SLOTS_PER_PAGE`] slots and acts as one associative
//!   candidate set.
//! - Buckets are packed into 4 KiB data pages — [`BUCKETS_PER_PAGE`]
//!   buckets per page. Page index `i` covers buckets
//!   `[i * BUCKETS_PER_PAGE, (i+1) * BUCKETS_PER_PAGE)`.
//! - The meta page records the bucket count, the two hash seeds,
//!   and the on-disk PageId for every data page (sparse — pages are
//!   allocated lazily on first write).
//!
//! # Lookup
//!
//! Two candidate buckets per key, computed by `xxh3_with_seed(hash,
//! seed_i) % N`. Each candidate bucket identifies a 4 KiB data page;
//! the entry can live in any of that page's 64 slots. This keeps the
//! on-disk layout unchanged while avoiding short cuckoo loops caused
//! by treating each 4-slot sub-bucket as independently full.
//!
//! # Insert
//!
//! Idempotent: if `hash` already lives in either candidate page,
//! `insert` overwrites the value and returns. Otherwise it places
//! the entry in the first empty slot it finds across the two
//! candidate pages. If both are full, it triggers a cuckoo
//! eviction chain (random victim from b1, swap, retry from victim's
//! alternate page, …) bounded by [`MAX_CUCKOO_CHAIN`]. Exceeding
//! the chain returns [`MetaDbError::Corruption`] with a "table full"
//! message — sizing for steady-state load factor below 0.85 makes
//! this practically unreachable.
//!
//! # Delete
//!
//! Walk both candidate pages; clear the slot whose stored hash
//! matches.

use std::sync::Arc;

use parking_lot::Mutex;
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::cache::PageCache;
use crate::dedup_types::{DedupValue, Hash32};
use crate::error::{MetaDbError, Result};
use crate::page::{PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use crate::page_store::PageStore;
use crate::paged_meta;
use crate::types::{Lsn, PageId};

pub const ENTRY_BYTES: usize = 60; // Hash32 (32) + DedupValue (28)
pub const ENTRIES_PER_BUCKET: usize = 4;
pub const BUCKET_BYTES: usize = ENTRY_BYTES * ENTRIES_PER_BUCKET; // 240
pub const BUCKETS_PER_PAGE: usize = (PAGE_PAYLOAD_SIZE - PRESENCE_BITMAP_BYTES) / BUCKET_BYTES; // 16
pub const SLOTS_PER_PAGE: usize = BUCKETS_PER_PAGE * ENTRIES_PER_BUCKET; // 64
pub const PRESENCE_BITMAP_BYTES: usize = 8;
pub const MAX_CUCKOO_CHAIN: usize = 64;

/// Bytes the head meta page reserves for module-specific data:
/// `bucket_count u64 + seed1 u64 + seed2 u64`.
const HEAD_EXTRA_BYTES: usize = 24;

const META_KEY_COUNT_MARKER: u16 = 0xFFFF;
const DATA_KEY_COUNT_MARKER: u16 = 0;

const _: () = {
    assert!(BUCKETS_PER_PAGE * BUCKET_BYTES + PRESENCE_BITMAP_BYTES <= PAGE_PAYLOAD_SIZE);
    assert!(SLOTS_PER_PAGE == 64);
};

/// Number of per-page mutexes used to serialise concurrent
/// read-modify-write on cuckoo data pages. A power of two makes the
/// `page_idx & (N - 1)` mask cheap. 64 is enough to spread the load
/// for production sizing (10⁵+ data pages) while staying in a couple
/// of cache lines.
const BUCKET_LOCK_SHARDS: usize = 64;

pub struct CuckooHash {
    bucket_count: u64,
    seed1: u64,
    seed2: u64,
    meta_page_id: PageId,
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    inner: Mutex<Inner>,
    /// Per-page-shard mutexes guarding the data-page read-modify-write
    /// sequence. Hashing by `page_idx` so that two concurrent writes
    /// to the same data page serialise (otherwise their bitmap
    /// updates would clobber each other), but writes to different
    /// pages run in parallel.
    bucket_locks: [Mutex<()>; BUCKET_LOCK_SHARDS],
}

struct Inner {
    /// `page_table[page_idx] = on-disk PageId`, or 0 if no page is
    /// allocated yet for that bucket range.
    page_table: Vec<PageId>,
    /// Meta page chain in order, head first. The head pid is fixed
    /// (recorded in the manifest); continuation pages are added /
    /// removed as `page_table` grows or shrinks.
    meta_chain: Vec<PageId>,
    meta_dirty: bool,
    /// Approximate number of occupied slots; used for `len()` and
    /// load-factor reporting. Recomputed exactly during `iter()`.
    approx_len: u64,
}

impl CuckooHash {
    pub fn create(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        bucket_count: u64,
        seed1: u64,
        seed2: u64,
    ) -> Result<Self> {
        if bucket_count == 0 {
            return Err(MetaDbError::InvalidArgument(
                "cuckoo bucket_count must be > 0".into(),
            ));
        }
        let total_pages = bucket_count.div_ceil(BUCKETS_PER_PAGE as u64) as usize;
        let meta_page_id = page_store.allocate()?;
        let me = Self {
            bucket_count,
            seed1,
            seed2,
            meta_page_id,
            page_store,
            page_cache,
            inner: Mutex::new(Inner {
                page_table: vec![0; total_pages],
                meta_chain: vec![meta_page_id],
                meta_dirty: false,
                approx_len: 0,
            }),
            bucket_locks: std::array::from_fn(|_| Mutex::new(())),
        };
        let mut guard = me.inner.lock();
        me.flush_meta_locked(&mut guard)?;
        drop(guard);
        Ok(me)
    }

    pub fn open(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        meta_page_id: PageId,
    ) -> Result<Self> {
        let read = paged_meta::read_chain(
            &page_store,
            meta_page_id,
            PageType::CuckooData,
            META_KEY_COUNT_MARKER,
            HEAD_EXTRA_BYTES,
        )?;
        let bucket_count = u64::from_le_bytes(read.head_extra[0..8].try_into().unwrap());
        let seed1 = u64::from_le_bytes(read.head_extra[8..16].try_into().unwrap());
        let seed2 = u64::from_le_bytes(read.head_extra[16..24].try_into().unwrap());
        // approx_len is recomputed lazily; leave as 0 until the first
        // explicit len() call (or rebuild on Open path).
        Ok(Self {
            bucket_count,
            seed1,
            seed2,
            meta_page_id,
            page_store,
            page_cache,
            inner: Mutex::new(Inner {
                page_table: read.page_table,
                meta_chain: read.chain_pids,
                meta_dirty: false,
                approx_len: 0,
            }),
            bucket_locks: std::array::from_fn(|_| Mutex::new(())),
        })
    }

    pub fn meta_page_id(&self) -> PageId {
        self.meta_page_id
    }

    pub fn bucket_count(&self) -> u64 {
        self.bucket_count
    }

    pub fn seeds(&self) -> (u64, u64) {
        (self.seed1, self.seed2)
    }

    /// Approximate live entry count. Tracks insert / delete deltas;
    /// callers that need an exact figure should use [`recount`].
    pub fn approx_len(&self) -> u64 {
        self.inner.lock().approx_len
    }

    /// Look up `hash`. Returns `Some(value)` if the hash lives in
    /// either candidate bucket, else `None`.
    pub fn get(&self, hash: &Hash32) -> Result<Option<DedupValue>> {
        let (b1, b2) = self.candidate_buckets(hash);
        if let Some(v) = self.read_bucket_for(hash, b1)? {
            return Ok(Some(v));
        }
        if b2 != b1 {
            if let Some(v) = self.read_bucket_for(hash, b2)? {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    /// Insert / overwrite `hash → value`. Returns `Ok(())` on
    /// success; `Err(Corruption("cuckoo full"))` only if the
    /// eviction chain exceeds [`MAX_CUCKOO_CHAIN`].
    pub fn put(&self, hash: Hash32, value: DedupValue, lsn: Lsn) -> Result<()> {
        // Idempotent overwrite: if the entry already exists, replace
        // the value in place and return. This keeps semantics
        // identical to the LSM `put` we are replacing.
        let (b1, b2) = self.candidate_buckets(&hash);
        for bucket in [b1, b2].into_iter().filter(|b| *b == b1 || *b != b1) {
            if self.update_existing(bucket, &hash, value, lsn)? {
                return Ok(());
            }
        }
        // Try empty slot insertion in either candidate page.
        for bucket in [b1, b2] {
            if self.try_insert_empty_in_page(bucket, hash, value, lsn)? {
                self.bump_len(1);
                return Ok(());
            }
        }
        // Both buckets full — kick off a cuckoo chain starting from b1.
        self.evict_and_insert(b1, hash, value, lsn)?;
        self.bump_len(1);
        Ok(())
    }

    /// Remove the entry for `hash`. No-op if absent.
    pub fn delete(&self, hash: &Hash32, lsn: Lsn) -> Result<()> {
        let (b1, b2) = self.candidate_buckets(hash);
        if self.try_clear_in_bucket(b1, hash, lsn)? {
            self.bump_len(-1);
            return Ok(());
        }
        if b2 != b1 && self.try_clear_in_bucket(b2, hash, lsn)? {
            self.bump_len(-1);
        }
        Ok(())
    }

    /// Iterate every live `(hash, value)` pair. Order is page-index
    /// then bucket then slot (deterministic but not lexicographic on
    /// hash). Used by the verifier and offline tools.
    pub fn iter(&self) -> Result<Vec<(Hash32, DedupValue)>> {
        let mut out = Vec::new();
        self.for_each(|hash, value| {
            out.push((hash, value));
            Ok(())
        })?;
        Ok(out)
    }

    /// Streaming walk over every live `(hash, value)` pair without
    /// materialising the result vec. The L0 rebuild path uses this
    /// so a 10 M-entry dedup index doesn't allocate ~600 MiB of
    /// `(Hash32, DedupValue)` tuples just to extract the
    /// fingerprints.
    pub fn for_each<F>(&self, mut visit: F) -> Result<()>
    where
        F: FnMut(Hash32, DedupValue) -> Result<()>,
    {
        let snapshot = {
            let inner = self.inner.lock();
            inner.page_table.clone()
        };
        for &pid in &snapshot {
            if pid == 0 {
                continue;
            }
            let page = self.page_cache.get(pid)?;
            let bitmap = read_bitmap(&page);
            for slot in 0..SLOTS_PER_PAGE {
                if bitmap & (1u64 << slot) != 0 {
                    let (h, v) = read_slot(&page, slot);
                    visit(h, v)?;
                }
            }
        }
        Ok(())
    }

    /// Recompute `approx_len` from the current on-disk state. O(N).
    pub fn recount(&self) -> Result<u64> {
        let mut count = 0u64;
        let snapshot = {
            let inner = self.inner.lock();
            inner.page_table.clone()
        };
        for &pid in &snapshot {
            if pid == 0 {
                continue;
            }
            let page = self.page_cache.get(pid)?;
            count += read_bitmap(&page).count_ones() as u64;
        }
        self.inner.lock().approx_len = count;
        Ok(count)
    }

    /// Persist the meta chain if dirty. Returns `true` if a write
    /// happened, `false` if the chain was already clean.
    pub fn flush_meta(&self) -> Result<bool> {
        let mut inner = self.inner.lock();
        if !inner.meta_dirty {
            return Ok(false);
        }
        self.flush_meta_locked(&mut inner)?;
        inner.meta_dirty = false;
        Ok(true)
    }

    /// Walk every allocated data page id (used by verifier).
    pub fn data_page_ids(&self) -> Vec<PageId> {
        let inner = self.inner.lock();
        inner
            .page_table
            .iter()
            .copied()
            .filter(|&pid| pid != 0)
            .collect()
    }

    // ---- private ---------------------------------------------------

    fn flush_meta_locked(&self, inner: &mut parking_lot::MutexGuard<'_, Inner>) -> Result<()> {
        let mut head_extra = [0u8; HEAD_EXTRA_BYTES];
        head_extra[0..8].copy_from_slice(&self.bucket_count.to_le_bytes());
        head_extra[8..16].copy_from_slice(&self.seed1.to_le_bytes());
        head_extra[16..24].copy_from_slice(&self.seed2.to_le_bytes());
        let new_chain = paged_meta::write_chain(
            &self.page_store,
            &self.page_cache,
            PageType::CuckooData,
            META_KEY_COUNT_MARKER,
            &head_extra,
            &inner.page_table,
            &inner.meta_chain,
            0,
        )?;
        inner.meta_chain = new_chain;
        Ok(())
    }

    fn candidate_buckets(&self, hash: &Hash32) -> (u64, u64) {
        let h1 = xxh3_64_with_seed(hash, self.seed1);
        let h2 = xxh3_64_with_seed(hash, self.seed2);
        let b1 = h1 % self.bucket_count;
        let b2 = h2 % self.bucket_count;
        (b1, b2)
    }

    fn alternate_bucket_for_page(&self, hash: &Hash32, current: u64) -> u64 {
        let (b1, b2) = self.candidate_buckets(hash);
        let current_page = bucket_offset(current).0;
        let b1_page = bucket_offset(b1).0;
        let b2_page = bucket_offset(b2).0;
        if current_page == b1_page && current_page != b2_page {
            b2
        } else if current_page == b2_page && current_page != b1_page {
            b1
        } else if current == b1 {
            b2
        } else {
            b1
        }
    }

    fn read_bucket_for(&self, hash: &Hash32, bucket_id: u64) -> Result<Option<DedupValue>> {
        let (page_idx, bucket_in_page) = bucket_offset(bucket_id);
        let pid = {
            let inner = self.inner.lock();
            inner.page_table.get(page_idx).copied().unwrap_or(0)
        };
        if pid == 0 {
            return Ok(None);
        }
        let page = self.page_cache.get(pid)?;
        let bitmap = read_bitmap(&page);
        let start = bucket_in_page * ENTRIES_PER_BUCKET;
        for offset in 0..SLOTS_PER_PAGE {
            let slot = (start + offset) % SLOTS_PER_PAGE;
            if bitmap & (1u64 << slot) != 0 {
                let (stored_hash, value) = read_slot(&page, slot);
                if &stored_hash == hash {
                    return Ok(Some(value));
                }
            }
        }
        Ok(None)
    }

    fn update_existing(
        &self,
        bucket_id: u64,
        hash: &Hash32,
        value: DedupValue,
        lsn: Lsn,
    ) -> Result<bool> {
        self.with_existing_bucket_mut(bucket_id, lsn, |bitmap, page, bucket_in_page| {
            let start = bucket_in_page * ENTRIES_PER_BUCKET;
            for offset in 0..SLOTS_PER_PAGE {
                let slot = (start + offset) % SLOTS_PER_PAGE;
                if *bitmap & (1u64 << slot) != 0 {
                    let (stored_hash, _) = read_slot(page, slot);
                    if &stored_hash == hash {
                        write_slot(page, slot, hash, &value);
                        return Ok((true, true));
                    }
                }
            }
            Ok((false, false))
        })
        .map(|result| result.unwrap_or(false))
    }

    fn try_insert_empty_in_page(
        &self,
        bucket_id: u64,
        hash: Hash32,
        value: DedupValue,
        lsn: Lsn,
    ) -> Result<bool> {
        self.with_bucket_mut(bucket_id, lsn, |bitmap, page, bucket_in_page| {
            let start = bucket_in_page * ENTRIES_PER_BUCKET;
            for offset in 0..SLOTS_PER_PAGE {
                let slot = (start + offset) % SLOTS_PER_PAGE;
                if *bitmap & (1u64 << slot) == 0 {
                    write_slot(page, slot, &hash, &value);
                    *bitmap |= 1u64 << slot;
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }

    fn try_clear_in_bucket(&self, bucket_id: u64, hash: &Hash32, lsn: Lsn) -> Result<bool> {
        self.with_existing_bucket_mut(bucket_id, lsn, |bitmap, page, bucket_in_page| {
            let start = bucket_in_page * ENTRIES_PER_BUCKET;
            for offset in 0..SLOTS_PER_PAGE {
                let slot = (start + offset) % SLOTS_PER_PAGE;
                if *bitmap & (1u64 << slot) != 0 {
                    let (stored_hash, _) = read_slot(page, slot);
                    if &stored_hash == hash {
                        clear_slot(page, slot);
                        *bitmap &= !(1u64 << slot);
                        return Ok((true, true));
                    }
                }
            }
            Ok((false, false))
        })
        .map(|result| result.unwrap_or(false))
    }

    fn evict_and_insert(
        &self,
        start_bucket: u64,
        mut hash: Hash32,
        mut value: DedupValue,
        lsn: Lsn,
    ) -> Result<()> {
        let mut current_bucket = start_bucket;
        for step in 0..MAX_CUCKOO_CHAIN {
            // Try empty slot first (unlikely but cheap to check).
            if self.try_insert_empty_in_page(current_bucket, hash, value, lsn)? {
                return Ok(());
            }
            let victim = self.swap_into_victim_slot(current_bucket, hash, value, step, lsn)?;
            hash = victim.0;
            value = victim.1;
            current_bucket = self.alternate_bucket_for_page(&hash, current_bucket);
        }
        Err(MetaDbError::Corruption(format!(
            "cuckoo eviction chain exceeded MAX_CUCKOO_CHAIN={MAX_CUCKOO_CHAIN}; \
             load factor too high — increase bucket_count and rebuild"
        )))
    }

    fn swap_into_victim_slot(
        &self,
        bucket_id: u64,
        hash: Hash32,
        value: DedupValue,
        step: usize,
        lsn: Lsn,
    ) -> Result<(Hash32, DedupValue)> {
        self.with_bucket_mut(bucket_id, lsn, |_bitmap, page, bucket_in_page| {
            let start = bucket_in_page * ENTRIES_PER_BUCKET;
            let seed = hash[step % hash.len()] as usize;
            let slot = (start + seed + step) % SLOTS_PER_PAGE;
            let (victim_hash, victim_value) = read_slot(page, slot);
            write_slot(page, slot, &hash, &value);
            Ok((victim_hash, victim_value))
        })
    }

    fn with_bucket_mut<T>(
        &self,
        bucket_id: u64,
        lsn: Lsn,
        f: impl FnOnce(&mut u64, &mut Page, usize) -> Result<T>,
    ) -> Result<T> {
        let (page_idx, bucket_in_page) = bucket_offset(bucket_id);
        // Lock order: per-page shard FIRST, then the meta mutex.
        // Holding the shard before resolving / allocating the page id
        // is what makes per-page sharding race-free: a concurrent
        // writer for the same page_idx blocks here (same shard) until
        // the first writer has completed its read-modify-write,
        // including the `replace_or_insert` that publishes the freshly
        // allocated page bytes to the cache. Otherwise a second writer
        // could observe the page-table entry under the meta mutex,
        // race ahead of the first writer's IO, and read all-zero
        // bytes via `get_for_modify`.
        let _shard = self.bucket_locks[page_idx & (BUCKET_LOCK_SHARDS - 1)].lock();
        let (page_id, freshly_allocated) = {
            let mut inner = self.inner.lock();
            if page_idx >= inner.page_table.len() {
                return Err(MetaDbError::Corruption(format!(
                    "cuckoo bucket_id {bucket_id} maps to page_idx {page_idx} but page_table \
                     is only {} entries",
                    inner.page_table.len(),
                )));
            }
            if inner.page_table[page_idx] == 0 {
                let pid = self.page_store.allocate()?;
                inner.page_table[page_idx] = pid;
                inner.meta_dirty = true;
                (pid, true)
            } else {
                (inner.page_table[page_idx], false)
            }
        };
        let mut page = if freshly_allocated {
            new_data_page()
        } else {
            self.page_cache.get_for_modify(page_id)?
        };
        let mut bitmap = read_bitmap(&page);
        let result = f(&mut bitmap, &mut page, bucket_in_page)?;
        write_bitmap(&mut page, bitmap);
        let mut header = page.header()?;
        header.generation = lsn.max(header.generation);
        page.write_header(&header);
        page.seal();
        self.page_store.write_page(page_id, &page)?;
        self.page_cache.replace_or_insert(page_id, Arc::new(page));
        Ok(result)
    }

    fn with_existing_bucket_mut<T>(
        &self,
        bucket_id: u64,
        lsn: Lsn,
        f: impl FnOnce(&mut u64, &mut Page, usize) -> Result<(T, bool)>,
    ) -> Result<Option<T>> {
        let (page_idx, bucket_in_page) = bucket_offset(bucket_id);
        let _shard = self.bucket_locks[page_idx & (BUCKET_LOCK_SHARDS - 1)].lock();
        let page_id = {
            let inner = self.inner.lock();
            if page_idx >= inner.page_table.len() {
                return Err(MetaDbError::Corruption(format!(
                    "cuckoo bucket_id {bucket_id} maps to page_idx {page_idx} but page_table \
                     is only {} entries",
                    inner.page_table.len(),
                )));
            }
            inner.page_table[page_idx]
        };
        if page_id == 0 {
            return Ok(None);
        }
        let mut page = (*self.page_cache.get(page_id)?).clone();
        let mut bitmap = read_bitmap(&page);
        let (result, dirty) = f(&mut bitmap, &mut page, bucket_in_page)?;
        if dirty {
            write_bitmap(&mut page, bitmap);
            let mut header = page.header()?;
            header.generation = lsn.max(header.generation);
            page.write_header(&header);
            page.seal();
            self.page_store.write_page(page_id, &page)?;
            self.page_cache.replace_or_insert(page_id, Arc::new(page));
        }
        Ok(Some(result))
    }

    fn bump_len(&self, delta: i64) {
        let mut inner = self.inner.lock();
        if delta >= 0 {
            inner.approx_len = inner.approx_len.saturating_add(delta as u64);
        } else {
            inner.approx_len = inner.approx_len.saturating_sub((-delta) as u64);
        }
    }
}

#[inline]
fn bucket_offset(bucket_id: u64) -> (usize, usize) {
    let id = bucket_id as usize;
    (id / BUCKETS_PER_PAGE, id % BUCKETS_PER_PAGE)
}

#[inline]
fn read_bitmap(page: &Page) -> u64 {
    let payload = page.payload();
    u64::from_le_bytes(payload[0..PRESENCE_BITMAP_BYTES].try_into().unwrap())
}

#[inline]
fn write_bitmap(page: &mut Page, bitmap: u64) {
    let payload = page.payload_mut();
    payload[0..PRESENCE_BITMAP_BYTES].copy_from_slice(&bitmap.to_le_bytes());
}

#[inline]
fn slot_offset(slot: usize) -> usize {
    PRESENCE_BITMAP_BYTES + slot * ENTRY_BYTES
}

#[inline]
fn read_slot(page: &Page, slot: usize) -> (Hash32, DedupValue) {
    let off = slot_offset(slot);
    let payload = page.payload();
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&payload[off..off + 32]);
    let mut value = [0u8; 28];
    value.copy_from_slice(&payload[off + 32..off + 60]);
    (hash, DedupValue(value))
}

#[inline]
fn write_slot(page: &mut Page, slot: usize, hash: &Hash32, value: &DedupValue) {
    let off = slot_offset(slot);
    let payload = page.payload_mut();
    payload[off..off + 32].copy_from_slice(hash);
    payload[off + 32..off + 60].copy_from_slice(value.as_bytes());
}

#[inline]
fn clear_slot(page: &mut Page, slot: usize) {
    let off = slot_offset(slot);
    let payload = page.payload_mut();
    payload[off..off + 60].fill(0);
}

fn new_data_page() -> Page {
    Page::new(PageHeader {
        page_type: PageType::CuckooData,
        version: crate::page::PAGE_VERSION,
        key_count: DATA_KEY_COUNT_MARKER,
        flags: 0,
        generation: 0,
        refcount: 1,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_index(bucket_count: u64) -> (TempDir, CuckooHash) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let page_store = Arc::new(PageStore::create(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let cuckoo =
            CuckooHash::create(page_store, page_cache, bucket_count, 0xDEAD, 0xBEEF).unwrap();
        (dir, cuckoo)
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
    fn empty_get_returns_none() {
        let (_d, c) = make_index(64);
        assert_eq!(c.get(&h(0xAA)).unwrap(), None);
    }

    #[test]
    fn put_get_round_trip() {
        let (_d, c) = make_index(64);
        c.put(h(0xAA), dv(7), 100).unwrap();
        assert_eq!(c.get(&h(0xAA)).unwrap(), Some(dv(7)));
        assert_eq!(c.get(&h(0xBB)).unwrap(), None);
    }

    #[test]
    fn put_overwrites_same_key() {
        let (_d, c) = make_index(64);
        c.put(h(0xAA), dv(7), 100).unwrap();
        c.put(h(0xAA), dv(9), 101).unwrap();
        assert_eq!(c.get(&h(0xAA)).unwrap(), Some(dv(9)));
        assert_eq!(c.approx_len(), 1);
    }

    #[test]
    fn delete_clears_entry() {
        let (_d, c) = make_index(64);
        c.put(h(0xAA), dv(7), 100).unwrap();
        c.delete(&h(0xAA), 101).unwrap();
        assert_eq!(c.get(&h(0xAA)).unwrap(), None);
    }

    #[test]
    fn delete_missing_is_noop() {
        let (_d, c) = make_index(64);
        c.delete(&h(0xAA), 100).unwrap();
        assert_eq!(c.approx_len(), 0);
    }

    #[test]
    fn many_inserts_below_load_factor() {
        // 64 buckets × 4 = 256 capacity; 200 inserts ≈ 0.78 load
        // factor — well within reach without an eviction-chain
        // overflow.
        let (_d, c) = make_index(64);
        for i in 0..200u8 {
            c.put(h(i), dv(i), (100 + i as u64) as Lsn).unwrap();
        }
        for i in 0..200u8 {
            assert_eq!(c.get(&h(i)).unwrap(), Some(dv(i)), "i={i}");
        }
        assert_eq!(c.approx_len(), 200);
    }

    #[test]
    fn iter_returns_all_live_pairs() {
        let (_d, c) = make_index(64);
        for i in 0..50u8 {
            c.put(h(i), dv(i), 100).unwrap();
        }
        let live = c.iter().unwrap();
        assert_eq!(live.len(), 50);
    }

    #[test]
    fn recount_matches_iter_length() {
        let (_d, c) = make_index(64);
        for i in 0..30u8 {
            c.put(h(i), dv(i), 100).unwrap();
        }
        for i in 0..10u8 {
            c.delete(&h(i), 200).unwrap();
        }
        let exact = c.recount().unwrap();
        assert_eq!(exact, 20);
        assert_eq!(c.approx_len(), 20);
    }

    #[test]
    fn round_trip_via_open() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let meta_page_id;
        {
            let page_store = Arc::new(PageStore::create(&path).unwrap());
            let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
            let c = CuckooHash::create(page_store.clone(), page_cache, 32, 0xAAAA, 0xBBBB).unwrap();
            meta_page_id = c.meta_page_id();
            for i in 0..40u8 {
                c.put(h(i), dv(i), (100 + i as u64) as Lsn).unwrap();
            }
            c.flush_meta().unwrap();
        }
        let page_store = Arc::new(PageStore::open(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let c = CuckooHash::open(page_store, page_cache, meta_page_id).unwrap();
        assert_eq!(c.bucket_count(), 32);
        assert_eq!(c.seeds(), (0xAAAA, 0xBBBB));
        for i in 0..40u8 {
            assert_eq!(c.get(&h(i)).unwrap(), Some(dv(i)), "i={i}");
        }
        assert_eq!(c.recount().unwrap(), 40);
    }

    #[test]
    fn bucket_count_zero_rejected() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let page_store = Arc::new(PageStore::create(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 4 * 1024 * 1024));
        let err = CuckooHash::create(page_store, page_cache, 0, 1, 2)
            .err()
            .unwrap();
        assert!(matches!(err, MetaDbError::InvalidArgument(_)));
    }

    #[test]
    fn bucket_count_past_one_meta_page_chains_a_continuation() {
        // Pick a bucket count whose page_table needs > one head meta
        // page. The chain must extend instead of erroring out.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let page_store = Arc::new(PageStore::create(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 8 * 1024 * 1024));
        let head_cap = paged_meta::head_capacity(HEAD_EXTRA_BYTES);
        let big_buckets = ((head_cap + 5) * BUCKETS_PER_PAGE) as u64;
        let cuckoo = CuckooHash::create(page_store, page_cache, big_buckets, 1, 2).unwrap();
        // Force a meta flush so the chain is materialised on disk.
        cuckoo.flush_meta().unwrap();
        assert!(cuckoo.inner.lock().meta_chain.len() >= 2);
    }

    #[test]
    fn concurrent_writers_across_pages() {
        // Drive ~1000 puts from 8 threads. Buckets are sized so that
        // each thread's hashes spread across many data pages, so the
        // per-page-shard locking sees genuine concurrency. Verify
        // every key round-trips after the storm.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let page_store = Arc::new(PageStore::create(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        // 4096 buckets * 4 entries = 16384 slots, plenty of headroom
        // for 8 * 1000 = 8000 inserts.
        let cuckoo =
            Arc::new(CuckooHash::create(page_store, page_cache, 4096, 0xDEAD, 0xBEEF).unwrap());
        let threads: Vec<_> = (0..8u8)
            .map(|tid| {
                let c = cuckoo.clone();
                std::thread::spawn(move || {
                    for i in 0..1000u32 {
                        let mut hash = [0u8; 32];
                        hash[0] = tid;
                        hash[1..5].copy_from_slice(&i.to_le_bytes());
                        c.put(hash, dv(tid), 100 + i as Lsn).unwrap();
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        for tid in 0..8u8 {
            for i in 0..1000u32 {
                let mut hash = [0u8; 32];
                hash[0] = tid;
                hash[1..5].copy_from_slice(&i.to_le_bytes());
                assert_eq!(cuckoo.get(&hash).unwrap(), Some(dv(tid)));
            }
        }
        assert_eq!(cuckoo.recount().unwrap(), 8000);
    }

    #[test]
    fn page_associative_insert_avoids_sub_bucket_false_full() {
        let (_d, c) = make_index(32);
        for i in 0..64u64 {
            let mut hash = [0u8; 32];
            hash[..8].copy_from_slice(&i.to_be_bytes());
            c.put(hash, dv((i & 0xff) as u8), 100 + i).unwrap();
        }
        assert_eq!(c.recount().unwrap(), 64);
        for i in 0..64u64 {
            let mut hash = [0u8; 32];
            hash[..8].copy_from_slice(&i.to_be_bytes());
            assert_eq!(c.get(&hash).unwrap(), Some(dv((i & 0xff) as u8)));
        }
    }
}
