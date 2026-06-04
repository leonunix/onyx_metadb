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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::{Mutex, MutexGuard};
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::cache::PageCache;
use crate::dedup_types::{DEDUP_VALUE_SIZE, DedupValue, HASH_SIZE, Hash8};
use crate::error::{MetaDbError, Result};
use crate::metrics::DedupPutStageTimings;
use crate::page::{PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use crate::page_store::{IoLaneClass, PageStore};
use crate::paged_meta;
use crate::types::{Lsn, PageId};

pub const ENTRY_BYTES: usize = HASH_SIZE + DEDUP_VALUE_SIZE;
pub const ENTRIES_PER_BUCKET: usize = 4;
pub const BUCKET_BYTES: usize = ENTRY_BYTES * ENTRIES_PER_BUCKET; // 144
/// Bytes reserved at the front of every data page for the presence
/// bitmap (1 bit per slot). At BUCKETS_PER_PAGE = 28 and 4 slots each
/// we have 112 slots, fitting in 14 bytes; round up to 16 for
/// alignment.
pub const PRESENCE_BITMAP_BYTES: usize = 16;
pub const BUCKETS_PER_PAGE: usize = (PAGE_PAYLOAD_SIZE - PRESENCE_BITMAP_BYTES) / BUCKET_BYTES;
pub const SLOTS_PER_PAGE: usize = BUCKETS_PER_PAGE * ENTRIES_PER_BUCKET;
pub const MAX_CUCKOO_CHAIN: usize = 64;

/// Bytes the head meta page reserves for module-specific data:
/// `bucket_count u64 + seed1 u64 + seed2 u64`.
const HEAD_EXTRA_BYTES: usize = 24;

const META_KEY_COUNT_MARKER: u16 = 0xFFFF;
const DATA_KEY_COUNT_MARKER: u16 = 0;

const _: () = {
    assert!(BUCKETS_PER_PAGE * BUCKET_BYTES + PRESENCE_BITMAP_BYTES <= PAGE_PAYLOAD_SIZE);
    // Sanity: presence bitmap must cover every slot.
    assert!(SLOTS_PER_PAGE <= PRESENCE_BITMAP_BYTES * 8);
};

/// Number of per-page mutexes used to serialise concurrent
/// read-modify-write on cuckoo data pages. A power of two makes the
/// `page_idx & (N - 1)` mask cheap. 64 is enough to spread the load
/// for production sizing (10⁵+ data pages) while staying in a couple
/// of cache lines.
const BUCKET_LOCK_SHARDS: usize = 8192;

#[derive(Clone, Copy)]
pub(crate) struct CuckooPutEntry {
    pub hash: Hash8,
    pub value: DedupValue,
}

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

struct BatchPageState {
    page_id: PageId,
    page: Page,
    bitmap: u128,
    dirty: bool,
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
    pub fn get(&self, hash: &Hash8) -> Result<Option<DedupValue>> {
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
    pub fn put(&self, hash: Hash8, value: DedupValue, lsn: Lsn) -> Result<()> {
        let mut timings = DedupPutStageTimings::default();
        self.put_with_metrics(hash, value, lsn, &mut timings)
    }

    pub(crate) fn put_with_metrics(
        &self,
        hash: Hash8,
        value: DedupValue,
        lsn: Lsn,
        timings: &mut DedupPutStageTimings,
    ) -> Result<()> {
        // Idempotent overwrite: if the entry already exists, replace
        // the value in place and return. This keeps semantics
        // identical to the LSM `put` we are replacing.
        let (b1, b2) = self.candidate_buckets(&hash);
        let candidate_count = if bucket_offset(b1).0 == bucket_offset(b2).0 {
            1
        } else {
            2
        };
        let candidates = [b1, b2];
        for bucket in candidates[..candidate_count].iter().copied() {
            let started = std::time::Instant::now();
            let updated = self.update_existing(bucket, &hash, value, lsn, Some(&mut *timings))?;
            timings.cuckoo_update_existing += started.elapsed();
            if updated {
                return Ok(());
            }
        }

        // Try the emptier candidate page first. With random all-miss
        // inserts, always preferring b1 creates local full pages well
        // before the global load factor is high; balancing by page
        // occupancy keeps the common path insertion-only and avoids
        // expensive cuckoo chains under Onyx packed-slot registration.
        let mut insert_order = candidates;
        let mut free_slots = [0usize; 2];
        for i in 0..candidate_count {
            let started = std::time::Instant::now();
            free_slots[i] = self.free_slots_in_page(insert_order[i])?;
            timings.cuckoo_free_slots += started.elapsed();
        }
        if candidate_count == 2 && free_slots[1] > free_slots[0] {
            insert_order.swap(0, 1);
            free_slots.swap(0, 1);
        }
        for i in 0..candidate_count {
            if free_slots[i] == 0 {
                continue;
            }
            let bucket = insert_order[i];
            let started = std::time::Instant::now();
            let inserted =
                self.try_insert_empty_in_page(bucket, hash, value, lsn, Some(&mut *timings))?;
            timings.cuckoo_try_insert_empty += started.elapsed();
            if inserted {
                self.bump_len(1);
                return Ok(());
            }
        }
        // Both candidate pages are full — kick off a cuckoo chain.
        let started = std::time::Instant::now();
        self.evict_and_insert(insert_order[0], hash, value, lsn, Some(&mut *timings))?;
        timings.cuckoo_evict_and_insert += started.elapsed();
        self.bump_len(1);
        Ok(())
    }

    pub(crate) fn put_many_with_metrics(
        &self,
        entries: &[CuckooPutEntry],
        lsn: Lsn,
        timings: &mut DedupPutStageTimings,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut positions: HashMap<Hash8, usize> = HashMap::new();
        let mut unique = Vec::with_capacity(entries.len());
        for entry in entries {
            if let Some(&idx) = positions.get(&entry.hash) {
                unique[idx] = *entry;
            } else {
                positions.insert(entry.hash, unique.len());
                unique.push(*entry);
            }
        }

        let mut affected_pages = HashSet::new();
        for entry in &unique {
            let (b1, b2) = self.candidate_buckets(&entry.hash);
            affected_pages.insert(bucket_offset(b1).0);
            affected_pages.insert(bucket_offset(b2).0);
        }
        let use_batch = affected_pages.len().saturating_mul(2) <= unique.len();
        if !use_batch {
            for entry in &unique {
                self.put_with_metrics(entry.hash, entry.value, lsn, timings)?;
            }
            return Ok(());
        }

        self.put_many_grouped_by_page(&unique, lsn, timings)
    }

    /// Remove the entry for `hash`. Returns `true` iff a matching slot
    /// was actually cleared. Returns `false` (no error) if the entry
    /// was absent — callers use this to gate the L0 sketch / L1 cache
    /// updates so they don't decrement reference counts for hashes
    /// that were never inserted (which would otherwise evict L0
    /// entries belonging to other hashes that share the fingerprint).
    pub fn delete(&self, hash: &Hash8, lsn: Lsn) -> Result<bool> {
        let (b1, b2) = self.candidate_buckets(hash);
        if self.try_clear_in_bucket(b1, hash, lsn)? {
            self.bump_len(-1);
            return Ok(true);
        }
        if b2 != b1 && self.try_clear_in_bucket(b2, hash, lsn)? {
            self.bump_len(-1);
            return Ok(true);
        }
        Ok(false)
    }

    /// Iterate every live `(hash, value)` pair. Order is page-index
    /// then bucket then slot (deterministic but not lexicographic on
    /// hash). Used by the verifier and offline tools.
    pub fn iter(&self) -> Result<Vec<(Hash8, DedupValue)>> {
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
    /// `(Hash8, DedupValue)` tuples just to extract the
    /// fingerprints.
    pub fn for_each<F>(&self, mut visit: F) -> Result<()>
    where
        F: FnMut(Hash8, DedupValue) -> Result<()>,
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
                if bitmap & (1u128 << slot) != 0 {
                    let (h, v) = read_slot(&page, slot);
                    visit(h, v)?;
                }
            }
        }
        Ok(())
    }

    /// Resumable, bounded variant of [`Self::for_each`]: collect up to `limit`
    /// live `(hash, value)` pairs starting at the cursor `(page_idx, slot)`,
    /// returning the entries plus the cursor to resume from next call and a
    /// `wrapped` flag that is true when the walk reached the end of the page
    /// table (a full pass completed; the returned cursor is `(0, 0)`).
    ///
    /// O(`limit`) — does NOT materialise the whole index, so a background sweep
    /// can lap a multi-billion-entry dedup index in bounded per-call steps where
    /// [`Self::iter`] would allocate hundreds of GiB. A single call walks a
    /// contiguous run of pages and stops at either `limit` entries or the end of
    /// the table, whichever comes first (so it never laps more than once per
    /// call). The page-table snapshot is taken per call; concurrent
    /// growth/shrink between calls only reshuffles which entries a given cursor
    /// lands on (best-effort coverage, like the L2P cold-tail cursor) — callers
    /// must not rely on exactly-once visitation, which is correct for the
    /// orphan-reclaim sweep (its safety is the Gate-2 confirm scan + guarded
    /// delete, not scan completeness).
    pub fn scan_from(
        &self,
        page_idx: usize,
        slot: usize,
        limit: usize,
    ) -> Result<(Vec<(Hash8, DedupValue)>, usize, usize, bool)> {
        let snapshot = {
            let inner = self.inner.lock();
            inner.page_table.clone()
        };
        let n = snapshot.len();
        let mut out = Vec::new();
        if n == 0 || limit == 0 {
            return Ok((out, 0, 0, true));
        }
        // Out-of-range cursor (e.g. the table shrank since last call) → restart.
        let mut pi = if page_idx >= n { 0 } else { page_idx };
        let mut sl = if page_idx >= n { 0 } else { slot.min(SLOTS_PER_PAGE) };
        loop {
            let pid = snapshot[pi];
            if pid != 0 {
                let page = self.page_cache.get(pid)?;
                let bitmap = read_bitmap(&page);
                while sl < SLOTS_PER_PAGE {
                    if bitmap & (1u128 << sl) != 0 {
                        out.push(read_slot(&page, sl));
                        if out.len() >= limit {
                            // Resume after this slot; normalise a page overflow.
                            sl += 1;
                            if sl >= SLOTS_PER_PAGE {
                                pi += 1;
                                sl = 0;
                            }
                            let wrapped = pi >= n;
                            return Ok((out, if wrapped { 0 } else { pi }, sl, wrapped));
                        }
                    }
                    sl += 1;
                }
            }
            // Page exhausted → advance. End of table ⇒ wrapped, resume at (0,0).
            pi += 1;
            sl = 0;
            if pi >= n {
                return Ok((out, 0, 0, true));
            }
        }
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

    fn candidate_buckets(&self, hash: &Hash8) -> (u64, u64) {
        let h1 = xxh3_64_with_seed(hash, self.seed1);
        let h2 = xxh3_64_with_seed(hash, self.seed2);
        let b1 = h1 % self.bucket_count;
        let b2 = h2 % self.bucket_count;
        (b1, b2)
    }

    fn alternate_bucket_for_page(&self, hash: &Hash8, current: u64) -> u64 {
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

    fn read_bucket_for(&self, hash: &Hash8, bucket_id: u64) -> Result<Option<DedupValue>> {
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
            if bitmap & (1u128 << slot) != 0 {
                let (stored_hash, value) = read_slot(&page, slot);
                if &stored_hash == hash {
                    return Ok(Some(value));
                }
            }
        }
        Ok(None)
    }

    fn free_slots_in_page(&self, bucket_id: u64) -> Result<usize> {
        let (page_idx, _bucket_in_page) = bucket_offset(bucket_id);
        // Same lock order as `with_bucket_mut` / `with_existing_bucket_mut`:
        // per-page shard FIRST, then the meta mutex. Without holding the
        // shard lock here a writer that has just published a freshly
        // allocated `page_table[page_idx] = pid` but not yet completed its
        // `write_page` + `replace_or_insert` could leave us reading raw
        // zeros from disk via `page_cache.get → page_store.read_page`,
        // surfacing as `PageMagicMismatch`. The race was effectively
        // invisible while `write_page` was a single ~30µs pwrite, but
        // routing it through the centralised io_submitter widens the
        // window enough to hit it under realistic load.
        let _shard = self.bucket_locks[page_idx & (BUCKET_LOCK_SHARDS - 1)].lock();
        let pid = {
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
        if pid == 0 {
            return Ok(SLOTS_PER_PAGE);
        }
        let page = self.page_cache.get(pid)?;
        Ok(SLOTS_PER_PAGE - read_bitmap(&page).count_ones() as usize)
    }

    fn update_existing(
        &self,
        bucket_id: u64,
        hash: &Hash8,
        value: DedupValue,
        lsn: Lsn,
        timings: Option<&mut DedupPutStageTimings>,
    ) -> Result<bool> {
        self.with_existing_bucket_mut(bucket_id, lsn, timings, |bitmap, page, bucket_in_page| {
            let start = bucket_in_page * ENTRIES_PER_BUCKET;
            for offset in 0..SLOTS_PER_PAGE {
                let slot = (start + offset) % SLOTS_PER_PAGE;
                if *bitmap & (1u128 << slot) != 0 {
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
        hash: Hash8,
        value: DedupValue,
        lsn: Lsn,
        timings: Option<&mut DedupPutStageTimings>,
    ) -> Result<bool> {
        self.with_bucket_mut(bucket_id, lsn, timings, |bitmap, page, bucket_in_page| {
            let start = bucket_in_page * ENTRIES_PER_BUCKET;
            for offset in 0..SLOTS_PER_PAGE {
                let slot = (start + offset) % SLOTS_PER_PAGE;
                if *bitmap & (1u128 << slot) == 0 {
                    write_slot(page, slot, &hash, &value);
                    *bitmap |= 1u128 << slot;
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }

    fn try_clear_in_bucket(&self, bucket_id: u64, hash: &Hash8, lsn: Lsn) -> Result<bool> {
        self.with_existing_bucket_mut(bucket_id, lsn, None, |bitmap, page, bucket_in_page| {
            let start = bucket_in_page * ENTRIES_PER_BUCKET;
            for offset in 0..SLOTS_PER_PAGE {
                let slot = (start + offset) % SLOTS_PER_PAGE;
                if *bitmap & (1u128 << slot) != 0 {
                    let (stored_hash, _) = read_slot(page, slot);
                    if &stored_hash == hash {
                        clear_slot(page, slot);
                        *bitmap &= !(1u128 << slot);
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
        mut hash: Hash8,
        mut value: DedupValue,
        lsn: Lsn,
        mut timings: Option<&mut DedupPutStageTimings>,
    ) -> Result<()> {
        let mut current_bucket = start_bucket;
        for step in 0..MAX_CUCKOO_CHAIN {
            // Try empty slot first (unlikely but cheap to check).
            let inserted = match timings.as_deref_mut() {
                Some(t) => {
                    self.try_insert_empty_in_page(current_bucket, hash, value, lsn, Some(t))?
                }
                None => self.try_insert_empty_in_page(current_bucket, hash, value, lsn, None)?,
            };
            if inserted {
                return Ok(());
            }
            let victim = match timings.as_deref_mut() {
                Some(t) => {
                    self.swap_into_victim_slot(current_bucket, hash, value, step, lsn, Some(t))?
                }
                None => self.swap_into_victim_slot(current_bucket, hash, value, step, lsn, None)?,
            };
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
        hash: Hash8,
        value: DedupValue,
        step: usize,
        lsn: Lsn,
        timings: Option<&mut DedupPutStageTimings>,
    ) -> Result<(Hash8, DedupValue)> {
        self.with_bucket_mut(bucket_id, lsn, timings, |_bitmap, page, bucket_in_page| {
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
        mut timings: Option<&mut DedupPutStageTimings>,
        f: impl FnOnce(&mut u128, &mut Page, usize) -> Result<T>,
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
        let lock_started = std::time::Instant::now();
        let _shard = self.bucket_locks[page_idx & (BUCKET_LOCK_SHARDS - 1)].lock();
        if let Some(t) = timings.as_deref_mut() {
            t.cuckoo_bucket_lock_wait += lock_started.elapsed();
        }
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
                let alloc_started = std::time::Instant::now();
                let pid = self.page_store.allocate()?;
                if let Some(t) = timings.as_deref_mut() {
                    t.cuckoo_page_alloc += alloc_started.elapsed();
                }
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
            let read_started = std::time::Instant::now();
            let page = self.page_cache.get_for_modify(page_id)?;
            if let Some(t) = timings.as_deref_mut() {
                t.cuckoo_page_read_cache_wait += read_started.elapsed();
            }
            page
        };
        let mut bitmap = read_bitmap(&page);
        let result = f(&mut bitmap, &mut page, bucket_in_page)?;
        write_bitmap(&mut page, bitmap);
        let mut header = page.header()?;
        header.generation = lsn.max(header.generation);
        page.write_header(&header);
        page.seal();
        let publish_started = std::time::Instant::now();
        self.page_store
            .write_page_for_class(page_id, &page, IoLaneClass::Dedup)?;
        self.page_cache.replace_or_insert(page_id, Arc::new(page));
        if let Some(t) = timings.as_deref_mut() {
            t.cuckoo_page_write_publish += publish_started.elapsed();
        }
        Ok(result)
    }

    fn with_existing_bucket_mut<T>(
        &self,
        bucket_id: u64,
        lsn: Lsn,
        mut timings: Option<&mut DedupPutStageTimings>,
        f: impl FnOnce(&mut u128, &mut Page, usize) -> Result<(T, bool)>,
    ) -> Result<Option<T>> {
        let (page_idx, bucket_in_page) = bucket_offset(bucket_id);
        let lock_started = std::time::Instant::now();
        let _shard = self.bucket_locks[page_idx & (BUCKET_LOCK_SHARDS - 1)].lock();
        if let Some(t) = timings.as_deref_mut() {
            t.cuckoo_bucket_lock_wait += lock_started.elapsed();
        }
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
        let read_started = std::time::Instant::now();
        let mut page = (*self.page_cache.get(page_id)?).clone();
        if let Some(t) = timings.as_deref_mut() {
            t.cuckoo_page_read_cache_wait += read_started.elapsed();
        }
        let mut bitmap = read_bitmap(&page);
        let (result, dirty) = f(&mut bitmap, &mut page, bucket_in_page)?;
        if dirty {
            write_bitmap(&mut page, bitmap);
            let mut header = page.header()?;
            header.generation = lsn.max(header.generation);
            page.write_header(&header);
            page.seal();
            let publish_started = std::time::Instant::now();
            self.page_store
                .write_page_for_class(page_id, &page, IoLaneClass::Dedup)?;
            self.page_cache.replace_or_insert(page_id, Arc::new(page));
            if let Some(t) = timings.as_deref_mut() {
                t.cuckoo_page_write_publish += publish_started.elapsed();
            }
        }
        Ok(Some(result))
    }

    fn put_many_grouped_by_page(
        &self,
        entries: &[CuckooPutEntry],
        lsn: Lsn,
        timings: &mut DedupPutStageTimings,
    ) -> Result<()> {
        let mut page_ops: HashMap<usize, Vec<CuckooPutEntry>> = HashMap::new();
        for entry in entries {
            let (b1, b2) = self.candidate_buckets(&entry.hash);
            page_ops
                .entry(bucket_offset(b1).0)
                .or_default()
                .push(*entry);
            if bucket_offset(b2).0 != bucket_offset(b1).0 {
                page_ops
                    .entry(bucket_offset(b2).0)
                    .or_default()
                    .push(*entry);
            }
        }

        let mut page_indices: Vec<usize> = page_ops.keys().copied().collect();
        page_indices.sort_unstable();
        let mut lock_shards: Vec<usize> = page_indices
            .iter()
            .map(|page_idx| page_idx & (BUCKET_LOCK_SHARDS - 1))
            .collect();
        lock_shards.sort_unstable();
        lock_shards.dedup();
        let lock_started = std::time::Instant::now();
        let _guards: Vec<MutexGuard<'_, ()>> = lock_shards
            .iter()
            .map(|&shard| self.bucket_locks[shard].lock())
            .collect();
        timings.cuckoo_bucket_lock_wait += lock_started.elapsed();

        let mut pages: HashMap<usize, BatchPageState> = HashMap::new();
        let mut applied: HashSet<Hash8> = HashSet::new();
        for page_idx in page_indices.iter().copied() {
            let Some(candidates) = page_ops.get(&page_idx) else {
                continue;
            };
            for hash in self
                .update_put_candidates_on_page_locked(page_idx, candidates, timings, &mut pages)?
            {
                applied.insert(hash);
            }
        }

        let mut inserted = 0u64;
        for page_idx in page_indices.iter().copied() {
            let Some(candidates) = page_ops.get(&page_idx) else {
                continue;
            };
            let (inserted_here, hashes) = self.insert_put_candidates_on_page_locked(
                page_idx, candidates, &applied, timings, &mut pages,
            )?;
            inserted = inserted.saturating_add(inserted_here);
            for hash in hashes {
                applied.insert(hash);
            }
        }

        let publish_started = std::time::Instant::now();
        let mut dirty_pages = Vec::new();
        for state in pages.values_mut() {
            if !state.dirty {
                continue;
            }
            write_bitmap(&mut state.page, state.bitmap);
            let mut header = state.page.header()?;
            header.generation = lsn.max(header.generation);
            state.page.write_header(&header);
            state.page.seal();
            dirty_pages.push((state.page_id, Arc::new(state.page.clone())));
        }
        self.page_store
            .write_sealed_page_runs(dirty_pages.clone())?;
        for (page_id, page) in dirty_pages {
            self.page_cache.replace_or_insert(page_id, page);
        }
        timings.cuckoo_page_write_publish += publish_started.elapsed();
        let remaining: Vec<CuckooPutEntry> = entries
            .iter()
            .copied()
            .filter(|entry| !applied.contains(&entry.hash))
            .collect();
        drop(_guards);
        if inserted != 0 {
            self.bump_len(inserted as i64);
        }
        for entry in remaining {
            self.put_with_metrics(entry.hash, entry.value, lsn, timings)?;
        }
        Ok(())
    }

    fn update_put_candidates_on_page_locked(
        &self,
        page_idx: usize,
        candidates: &[CuckooPutEntry],
        timings: &mut DedupPutStageTimings,
        pages: &mut HashMap<usize, BatchPageState>,
    ) -> Result<Vec<Hash8>> {
        let page_id = {
            let inner = self.inner.lock();
            if page_idx >= inner.page_table.len() {
                return Err(MetaDbError::Corruption(format!(
                    "cuckoo page_idx {page_idx} is outside page_table len {}",
                    inner.page_table.len(),
                )));
            }
            inner.page_table[page_idx]
        };
        if page_id == 0 {
            return Ok(Vec::new());
        }

        self.ensure_batch_page_loaded(page_idx, page_id, timings, pages)?;
        let mut applied = Vec::new();

        for entry in candidates {
            let Some(state) = pages.get_mut(&page_idx) else {
                unreachable!("batch page state must exist after ensure");
            };
            let (b1, b2) = self.candidate_buckets(&entry.hash);
            let mut buckets = [b1, b2];
            let count = if bucket_offset(b1).0 == bucket_offset(b2).0 {
                1
            } else {
                2
            };
            if bucket_offset(buckets[0]).0 != page_idx {
                buckets.swap(0, 1);
            }
            for &bucket in &buckets[..count] {
                let (candidate_page_idx, bucket_in_page) = bucket_offset(bucket);
                if candidate_page_idx != page_idx {
                    continue;
                }
                let started = std::time::Instant::now();
                if update_existing_in_loaded_page(
                    &mut state.page,
                    &state.bitmap,
                    bucket_in_page,
                    &entry.hash,
                    &entry.value,
                ) {
                    timings.cuckoo_update_existing += started.elapsed();
                    state.dirty = true;
                    applied.push(entry.hash);
                    break;
                }
                timings.cuckoo_update_existing += started.elapsed();
            }
        }
        Ok(applied)
    }

    fn insert_put_candidates_on_page_locked(
        &self,
        page_idx: usize,
        candidates: &[CuckooPutEntry],
        already_applied: &HashSet<Hash8>,
        timings: &mut DedupPutStageTimings,
        pages: &mut HashMap<usize, BatchPageState>,
    ) -> Result<(u64, Vec<Hash8>)> {
        if candidates
            .iter()
            .all(|entry| already_applied.contains(&entry.hash))
        {
            return Ok((0, Vec::new()));
        }

        let (page_id, freshly_allocated) = {
            let mut inner = self.inner.lock();
            if page_idx >= inner.page_table.len() {
                return Err(MetaDbError::Corruption(format!(
                    "cuckoo page_idx {page_idx} is outside page_table len {}",
                    inner.page_table.len(),
                )));
            }
            if inner.page_table[page_idx] == 0 {
                let alloc_started = std::time::Instant::now();
                let pid = self.page_store.allocate()?;
                timings.cuckoo_page_alloc += alloc_started.elapsed();
                inner.page_table[page_idx] = pid;
                inner.meta_dirty = true;
                (pid, true)
            } else {
                (inner.page_table[page_idx], false)
            }
        };

        if freshly_allocated {
            pages.entry(page_idx).or_insert_with(|| BatchPageState {
                page_id,
                page: new_data_page(),
                bitmap: 0,
                dirty: false,
            });
        } else {
            self.ensure_batch_page_loaded(page_idx, page_id, timings, pages)?;
        }
        let mut inserted = 0u64;
        let mut applied = Vec::new();

        for entry in candidates {
            if already_applied.contains(&entry.hash) {
                continue;
            }
            let Some(state) = pages.get_mut(&page_idx) else {
                unreachable!("batch page state must exist after ensure");
            };
            let (b1, b2) = self.candidate_buckets(&entry.hash);
            let mut buckets = [b1, b2];
            let count = if bucket_offset(b1).0 == bucket_offset(b2).0 {
                1
            } else {
                2
            };
            if bucket_offset(buckets[0]).0 != page_idx {
                buckets.swap(0, 1);
            }
            for &bucket in &buckets[..count] {
                let (candidate_page_idx, bucket_in_page) = bucket_offset(bucket);
                if candidate_page_idx != page_idx {
                    continue;
                }
                let started = std::time::Instant::now();
                if let Some(slot) = first_empty_slot_in_loaded_page(&state.bitmap, bucket_in_page) {
                    write_slot(&mut state.page, slot, &entry.hash, &entry.value);
                    state.bitmap |= 1u128 << slot;
                    state.dirty = true;
                    timings.cuckoo_try_insert_empty += started.elapsed();
                    inserted += 1;
                    applied.push(entry.hash);
                    break;
                }
                timings.cuckoo_try_insert_empty += started.elapsed();
            }
        }

        Ok((inserted, applied))
    }

    fn ensure_batch_page_loaded(
        &self,
        page_idx: usize,
        page_id: PageId,
        timings: &mut DedupPutStageTimings,
        pages: &mut HashMap<usize, BatchPageState>,
    ) -> Result<()> {
        if pages.contains_key(&page_idx) {
            return Ok(());
        }
        let read_started = std::time::Instant::now();
        let page = self.page_cache.get_for_modify(page_id)?;
        timings.cuckoo_page_read_cache_wait += read_started.elapsed();
        let bitmap = read_bitmap(&page);
        pages.insert(
            page_idx,
            BatchPageState {
                page_id,
                page,
                bitmap,
                dirty: false,
            },
        );
        Ok(())
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
fn read_bitmap(page: &Page) -> u128 {
    let payload = page.payload();
    u128::from_le_bytes(payload[0..PRESENCE_BITMAP_BYTES].try_into().unwrap())
}

#[inline]
fn write_bitmap(page: &mut Page, bitmap: u128) {
    let payload = page.payload_mut();
    payload[0..PRESENCE_BITMAP_BYTES].copy_from_slice(&bitmap.to_le_bytes());
}

#[inline]
fn slot_offset(slot: usize) -> usize {
    PRESENCE_BITMAP_BYTES + slot * ENTRY_BYTES
}

#[inline]
fn read_slot(page: &Page, slot: usize) -> (Hash8, DedupValue) {
    let off = slot_offset(slot);
    let payload = page.payload();
    let mut hash = [0u8; 8];
    hash.copy_from_slice(&payload[off..off + HASH_SIZE]);
    let mut value = [0u8; DEDUP_VALUE_SIZE];
    value.copy_from_slice(&payload[off + HASH_SIZE..off + HASH_SIZE + DEDUP_VALUE_SIZE]);
    (hash, DedupValue(value))
}

fn update_existing_in_loaded_page(
    page: &mut Page,
    bitmap: &u128,
    bucket_in_page: usize,
    hash: &Hash8,
    value: &DedupValue,
) -> bool {
    let start = bucket_in_page * ENTRIES_PER_BUCKET;
    for offset in 0..SLOTS_PER_PAGE {
        let slot = (start + offset) % SLOTS_PER_PAGE;
        if *bitmap & (1u128 << slot) != 0 {
            let (stored_hash, _) = read_slot(page, slot);
            if &stored_hash == hash {
                write_slot(page, slot, hash, value);
                return true;
            }
        }
    }
    false
}

fn first_empty_slot_in_loaded_page(bitmap: &u128, bucket_in_page: usize) -> Option<usize> {
    let start = bucket_in_page * ENTRIES_PER_BUCKET;
    for offset in 0..SLOTS_PER_PAGE {
        let slot = (start + offset) % SLOTS_PER_PAGE;
        if *bitmap & (1u128 << slot) == 0 {
            return Some(slot);
        }
    }
    None
}

#[inline]
fn write_slot(page: &mut Page, slot: usize, hash: &Hash8, value: &DedupValue) {
    let off = slot_offset(slot);
    let payload = page.payload_mut();
    payload[off..off + HASH_SIZE].copy_from_slice(hash);
    payload[off + HASH_SIZE..off + HASH_SIZE + DEDUP_VALUE_SIZE].copy_from_slice(value.as_bytes());
}

#[inline]
fn clear_slot(page: &mut Page, slot: usize) {
    let off = slot_offset(slot);
    let payload = page.payload_mut();
    payload[off..off + ENTRY_BYTES].fill(0);
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
mod tests;
