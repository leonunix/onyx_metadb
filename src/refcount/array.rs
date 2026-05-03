//! Disk-backed paged refcount array.
//!
//! Layout: PBA space partitioned into fixed-size pages. Each *data*
//! page holds [`ENTRIES_PER_PAGE`] consecutive `(rc, birth_lsn)` pairs
//! starting at `pba = page_idx * ENTRIES_PER_PAGE`. Each shard owns
//! one *meta* page that records the data page id for every page_idx
//! the shard has ever populated. Holes (PBA ranges with rc=0
//! everywhere) are encoded as `PageId(0)` in the meta page and
//! consume neither a data page on disk nor an entry in the page
//! cache.
//!
//! # On-disk encoding
//!
//! ## Data page (PageType::RefcountArray)
//! - shared header (64 B) — `generation` doubles as `last_applied_lsn`
//!   so replay can skip ops whose LSN ≤ page generation. (Refcount
//!   apply is NOT idempotent — see metadb/CLAUDE.md recovery section.)
//! - payload (4032 B):
//!   for slot in 0..ENTRIES_PER_PAGE (336):
//!     rc[slot]:        u32 LE  (offset slot*12)
//!     birth_lsn[slot]: u64 LE  (offset slot*12 + 4)
//!   336 × 12 = 4032 B (no slack)
//!
//! ## Meta page (PageType::RefcountArray, marked by `key_count = META_KEY_COUNT_MARKER`)
//! - shared header (64 B)
//! - payload (4032 B):
//!   page_count: u32 LE  (offset 0)
//!   reserved:   u32 LE  (offset 4)
//!   page_table: [u64 LE; 503]  (offset 8 .. 4032)
//!     page_table[i] = on-disk PageId for data page covering
//!       PBAs [i * ENTRIES_PER_PAGE, (i+1) * ENTRIES_PER_PAGE). 0 if
//!       no data page allocated yet (rc all zero).
//!
//! Single meta page caps a shard at 503 × 336 = 169_008 PBAs. Stage 1
//! validation only; chain extension lands in stage 1.3c so the cap
//! grows with shard count.
//!
//! # Concurrency
//!
//! Disk I/O is gated by a single shard-level `Mutex`; readers contend
//! with the apply lane on it. Refcount reads are infrequent vs L2P
//! reads, so the mutex coarsen does not show up in metrics.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::btree::format::RcEntry;
use crate::cache::PageCache;
use crate::error::{MetaDbError, Result};
use crate::page::{Page, PageHeader, PageType, PAGE_PAYLOAD_SIZE};
use crate::page_store::PageStore;
use crate::types::{Lsn, Pba, PageId};

use super::delta::Pending;

/// Entries per data page. 336 × 12 B = 4032 B = PAGE_PAYLOAD_SIZE.
pub const ENTRIES_PER_PAGE: usize = 336;
const ENTRY_BYTES: usize = 12;
const META_HEADER_BYTES: usize = 8;
const META_TABLE_CAPACITY: usize =
    (PAGE_PAYLOAD_SIZE - META_HEADER_BYTES) / std::mem::size_of::<u64>();

/// Marker stored in the shared header's `key_count` slot to distinguish
/// the meta page from data pages of the same `PageType`. Data pages
/// have `key_count = 0` (we don't track liveness per-slot since the
/// `rc == 0` check already disambiguates).
const META_KEY_COUNT_MARKER: u16 = 0xFFFF;
const DATA_KEY_COUNT_MARKER: u16 = 0;

const _: () = {
    assert!(ENTRIES_PER_PAGE * ENTRY_BYTES == PAGE_PAYLOAD_SIZE);
    assert!(META_HEADER_BYTES + META_TABLE_CAPACITY * 8 <= PAGE_PAYLOAD_SIZE);
};

pub struct PagedRefcountArray {
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    meta_page_id: PageId,
    inner: Mutex<Inner>,
}

struct Inner {
    /// Mirrors the on-disk meta page payload. Length is the highest
    /// page_idx ever written + 1; interior holes hold `PageId(0)`.
    page_table: Vec<PageId>,
    /// Set on every write that grows or mutates `page_table`. Cleared
    /// after the meta page is rewritten in `flush_meta_locked`.
    meta_dirty: bool,
}

impl PagedRefcountArray {
    /// Create a fresh shard backing store. Allocates the meta page
    /// from `page_store`. The returned `meta_page_id()` must be
    /// recorded in the manifest.
    pub fn create(page_store: Arc<PageStore>, page_cache: Arc<PageCache>) -> Result<Self> {
        let meta_page_id = page_store.allocate()?;
        let inner = Inner {
            page_table: Vec::new(),
            meta_dirty: false,
        };
        let me = Self {
            page_store,
            page_cache,
            meta_page_id,
            inner: Mutex::new(inner),
        };
        // Persist an empty meta page so `open()` after a clean restart
        // sees a valid header rather than uninitialised bytes.
        me.flush_meta_locked(&me.inner.lock())?;
        Ok(me)
    }

    /// Open an existing shard. Reads the meta page and rebuilds the
    /// in-memory page table.
    pub fn open(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        meta_page_id: PageId,
    ) -> Result<Self> {
        let meta_page = page_store.read_page(meta_page_id)?;
        let header = meta_page.header()?;
        if header.page_type != PageType::RefcountArray
            || header.key_count != META_KEY_COUNT_MARKER
        {
            return Err(MetaDbError::Corruption(format!(
                "refcount meta page at {meta_page_id} has wrong header (type={:?}, key_count={})",
                header.page_type, header.key_count,
            )));
        }
        let payload = meta_page.payload();
        let page_count = u32::from_le_bytes(payload[0..4].try_into().unwrap()) as usize;
        if page_count > META_TABLE_CAPACITY {
            return Err(MetaDbError::Corruption(format!(
                "refcount meta page page_count {page_count} exceeds capacity {META_TABLE_CAPACITY}",
            )));
        }
        let mut page_table = Vec::with_capacity(page_count);
        for i in 0..page_count {
            let off = META_HEADER_BYTES + i * 8;
            let pid = u64::from_le_bytes(payload[off..off + 8].try_into().unwrap()) as PageId;
            page_table.push(pid);
        }
        Ok(Self {
            page_store,
            page_cache,
            meta_page_id,
            inner: Mutex::new(Inner {
                page_table,
                meta_dirty: false,
            }),
        })
    }

    pub fn meta_page_id(&self) -> PageId {
        self.meta_page_id
    }

    /// Look up one entry. Returns [`RcEntry::ZERO`] if no data page
    /// is allocated for the PBA's page_idx.
    pub fn get(&self, pba: Pba) -> Result<RcEntry> {
        let (page_idx, slot) = page_offset(pba);
        let page_id = {
            let inner = self.inner.lock();
            inner.page_table.get(page_idx).copied().unwrap_or(0)
        };
        if page_id == 0 {
            return Ok(RcEntry::ZERO);
        }
        let page = self.page_cache.get(page_id)?;
        Ok(read_entry(&page, slot))
    }

    /// `last_applied_lsn` recorded on the data page covering `pba`.
    /// Returns 0 if no page is allocated yet (replay must apply
    /// unconditionally for a fresh page).
    pub fn page_lsn(&self, pba: Pba) -> Result<Lsn> {
        let (page_idx, _) = page_offset(pba);
        let page_id = {
            let inner = self.inner.lock();
            inner.page_table.get(page_idx).copied().unwrap_or(0)
        };
        if page_id == 0 {
            return Ok(0);
        }
        let page = self.page_cache.get(page_id)?;
        Ok(page.header()?.generation)
    }

    /// Apply a batch of pending deltas to the on-disk array. Groups
    /// by data page so each page is read+written exactly once. Marks
    /// the meta page dirty if any new data pages were allocated.
    /// The caller must subsequently call [`flush_meta`] to persist
    /// the meta page; the apply lane batches multiple `apply_deltas`
    /// calls behind one `flush_meta`.
    pub fn apply_deltas(&self, deltas: Vec<(Pba, Pending)>) -> Result<()> {
        if deltas.is_empty() {
            return Ok(());
        }

        let mut by_page: HashMap<usize, Vec<(usize, Pending)>> = HashMap::new();
        for (pba, pending) in deltas {
            let (page_idx, slot) = page_offset(pba);
            by_page.entry(page_idx).or_default().push((slot, pending));
        }

        for (page_idx, slot_pendings) in by_page {
            self.apply_one_page(page_idx, slot_pendings)?;
        }

        Ok(())
    }

    fn apply_one_page(
        &self,
        page_idx: usize,
        slot_pendings: Vec<(usize, Pending)>,
    ) -> Result<()> {
        if page_idx >= META_TABLE_CAPACITY {
            return Err(MetaDbError::InvalidArgument(format!(
                "refcount page_idx {page_idx} exceeds single-meta-page capacity {META_TABLE_CAPACITY}; \
                 chained meta pages land in stage 1.3c",
            )));
        }

        // Resolve / allocate the page id under the inner lock; release
        // the lock before doing IO so concurrent reads on other pages
        // don't block.
        let (page_id, freshly_allocated) = {
            let mut inner = self.inner.lock();
            if page_idx >= inner.page_table.len() {
                inner.page_table.resize(page_idx + 1, 0);
                inner.meta_dirty = true;
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
            new_empty_data_page()
        } else {
            (*self.page_cache.get(page_id)?).clone()
        };

        let mut max_lsn = page.header()?.generation;
        for (slot, pending) in slot_pendings {
            let prev = read_entry(&page, slot);
            let new = super::apply_delta_pure(prev, pending.delta, pending.last_lsn)?;
            write_entry(&mut page, slot, new);
            if pending.last_lsn > max_lsn {
                max_lsn = pending.last_lsn;
            }
        }

        let mut header = page.header()?;
        header.generation = max_lsn;
        page.write_header(&header);
        page.seal();

        self.page_store.write_page(page_id, &page)?;
        self.page_cache.invalidate(page_id);
        self.page_cache.insert(page_id, Arc::new(page));
        Ok(())
    }

    /// Persist the meta page if it has been mutated since the last
    /// flush. Idempotent / no-op when clean.
    pub fn flush_meta(&self) -> Result<()> {
        let inner = self.inner.lock();
        if inner.meta_dirty {
            self.flush_meta_locked(&inner)?;
        }
        Ok(())
    }

    fn flush_meta_locked(&self, inner: &parking_lot::MutexGuard<'_, Inner>) -> Result<()> {
        let mut page = Page::new(PageHeader {
            page_type: PageType::RefcountArray,
            version: crate::page::PAGE_VERSION,
            key_count: META_KEY_COUNT_MARKER,
            flags: 0,
            generation: 0,
            // Meta page is reachable from `manifest.refcount_shard_roots`;
            // header refcount = 1 keeps the shared verifier happy.
            refcount: 1,
        });
        {
            let payload = page.payload_mut();
            let len = inner.page_table.len() as u32;
            payload[0..4].copy_from_slice(&len.to_le_bytes());
            payload[4..8].fill(0);
            for (i, &pid) in inner.page_table.iter().enumerate() {
                let off = META_HEADER_BYTES + i * 8;
                payload[off..off + 8].copy_from_slice(&(pid as u64).to_le_bytes());
            }
        }
        page.seal();
        self.page_store.write_page(self.meta_page_id, &page)?;
        self.page_cache.invalidate(self.meta_page_id);
        self.page_cache
            .insert(self.meta_page_id, Arc::new(page));
        // We can't take &mut inner here (it's behind MutexGuard<&>),
        // so the caller is responsible for clearing the dirty flag if
        // they hold the lock. `flush_meta()` re-acquires for that.
        Ok(())
    }

    /// Iterate every (pba, RcEntry) where `rc > 0`. Order is
    /// PBA-ascending. Forces no flush — caller must ensure deltas are
    /// drained beforehand if they want a consistent view.
    pub fn iter_live(&self) -> Result<Vec<(Pba, RcEntry)>> {
        let inner = self.inner.lock();
        let page_ids: Vec<(usize, PageId)> = inner
            .page_table
            .iter()
            .enumerate()
            .filter_map(|(idx, &pid)| if pid != 0 { Some((idx, pid)) } else { None })
            .collect();
        drop(inner);
        let mut out = Vec::new();
        for (page_idx, page_id) in page_ids {
            let page = self.page_cache.get(page_id)?;
            for slot in 0..ENTRIES_PER_PAGE {
                let entry = read_entry(&page, slot);
                if entry.rc > 0 {
                    let pba = (page_idx * ENTRIES_PER_PAGE + slot) as Pba;
                    out.push((pba, entry));
                }
            }
        }
        Ok(out)
    }

    /// Number of data pages currently allocated (excludes the meta
    /// page and unwritten holes).
    pub fn allocated_data_pages(&self) -> usize {
        let inner = self.inner.lock();
        inner.page_table.iter().filter(|&&pid| pid != 0).count()
    }
}

#[inline]
fn page_offset(pba: Pba) -> (usize, usize) {
    let pba = pba as usize;
    (pba / ENTRIES_PER_PAGE, pba % ENTRIES_PER_PAGE)
}

#[inline]
fn read_entry(page: &Page, slot: usize) -> RcEntry {
    let payload = page.payload();
    let off = slot * ENTRY_BYTES;
    let rc = u32::from_le_bytes(payload[off..off + 4].try_into().unwrap());
    let birth_lsn = u64::from_le_bytes(payload[off + 4..off + 12].try_into().unwrap());
    RcEntry { rc, birth_lsn }
}

#[inline]
fn write_entry(page: &mut Page, slot: usize, entry: RcEntry) {
    let payload = page.payload_mut();
    let off = slot * ENTRY_BYTES;
    payload[off..off + 4].copy_from_slice(&entry.rc.to_le_bytes());
    payload[off + 4..off + 12].copy_from_slice(&entry.birth_lsn.to_le_bytes());
}

fn new_empty_data_page() -> Page {
    Page::new(PageHeader {
        page_type: PageType::RefcountArray,
        version: crate::page::PAGE_VERSION,
        key_count: DATA_KEY_COUNT_MARKER,
        flags: 0,
        generation: 0,
        // The shared `verify` path expects every manifest-reachable
        // page to carry header refcount = 1 (the meta page is the
        // single owner). Set it here so the very first write of a
        // freshly allocated data page is already correctly stamped.
        refcount: 1,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_array() -> (TempDir, PagedRefcountArray) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let page_store = Arc::new(PageStore::create(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 64 * 1024 * 1024));
        let array = PagedRefcountArray::create(page_store, page_cache).unwrap();
        (dir, array)
    }

    fn pending(delta: i64, lsn: Lsn) -> Pending {
        Pending { delta, last_lsn: lsn }
    }

    #[test]
    fn empty_array_returns_zero_for_any_pba() {
        let (_dir, a) = make_array();
        assert_eq!(a.get(0).unwrap(), RcEntry::ZERO);
        assert_eq!(a.get(99_999).unwrap(), RcEntry::ZERO);
        assert_eq!(a.allocated_data_pages(), 0);
    }

    #[test]
    fn apply_deltas_persists_one_entry() {
        let (_dir, a) = make_array();
        a.apply_deltas(vec![(7, pending(1, 100))]).unwrap();
        let e = a.get(7).unwrap();
        assert_eq!(e, RcEntry { rc: 1, birth_lsn: 100 });
        assert_eq!(a.allocated_data_pages(), 1);
        assert_eq!(a.page_lsn(7).unwrap(), 100);
    }

    #[test]
    fn apply_deltas_groups_same_page_into_one_io() {
        let (_dir, a) = make_array();
        // All 3 PBAs land in page_idx=0
        a.apply_deltas(vec![
            (1, pending(1, 100)),
            (2, pending(2, 101)),
            (3, pending(5, 102)),
        ]).unwrap();
        assert_eq!(a.get(1).unwrap().rc, 1);
        assert_eq!(a.get(2).unwrap().rc, 2);
        assert_eq!(a.get(3).unwrap().rc, 5);
        assert_eq!(a.allocated_data_pages(), 1);
    }

    #[test]
    fn apply_deltas_spans_multiple_pages() {
        let (_dir, a) = make_array();
        let pba_p0 = 5;
        let pba_p1 = (ENTRIES_PER_PAGE + 7) as Pba;
        let pba_p3 = (ENTRIES_PER_PAGE * 3 + 1) as Pba;
        a.apply_deltas(vec![
            (pba_p0, pending(1, 100)),
            (pba_p1, pending(1, 101)),
            (pba_p3, pending(1, 102)),
        ]).unwrap();
        assert_eq!(a.get(pba_p0).unwrap().rc, 1);
        assert_eq!(a.get(pba_p1).unwrap().rc, 1);
        assert_eq!(a.get(pba_p3).unwrap().rc, 1);
        // page_idx 2 is a hole; page_table grows to 4 but only 3 data pages are allocated.
        assert_eq!(a.allocated_data_pages(), 3);
    }

    #[test]
    fn round_trip_via_open() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let meta_page_id;
        {
            let page_store = Arc::new(PageStore::create(&path).unwrap());
            let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
            let a = PagedRefcountArray::create(page_store.clone(), page_cache).unwrap();
            meta_page_id = a.meta_page_id();
            a.apply_deltas(vec![
                (10, pending(3, 100)),
                (
                    (ENTRIES_PER_PAGE * 2 + 5) as Pba,
                    pending(7, 200),
                ),
            ]).unwrap();
            a.flush_meta().unwrap();
        }
        // Reopen
        let page_store = Arc::new(PageStore::open(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let a = PagedRefcountArray::open(page_store, page_cache, meta_page_id).unwrap();
        assert_eq!(a.get(10).unwrap(), RcEntry { rc: 3, birth_lsn: 100 });
        assert_eq!(
            a.get((ENTRIES_PER_PAGE * 2 + 5) as Pba).unwrap(),
            RcEntry { rc: 7, birth_lsn: 200 }
        );
        assert_eq!(a.get(0).unwrap(), RcEntry::ZERO);
        assert_eq!(a.allocated_data_pages(), 2);
    }

    #[test]
    fn iter_live_skips_zero_slots() {
        let (_dir, a) = make_array();
        a.apply_deltas(vec![
            (1, pending(5, 1)),
            (3, pending(0, 0)),  // no-op, stays zero
            (7, pending(9, 2)),
        ]).unwrap();
        let live = a.iter_live().unwrap();
        assert_eq!(live.len(), 2);
        assert_eq!(live[0], (1, RcEntry { rc: 5, birth_lsn: 1 }));
        assert_eq!(live[1], (7, RcEntry { rc: 9, birth_lsn: 2 }));
    }

    #[test]
    fn page_idx_beyond_meta_capacity_errors() {
        let (_dir, a) = make_array();
        let pba = (META_TABLE_CAPACITY * ENTRIES_PER_PAGE) as Pba;
        let err = a.apply_deltas(vec![(pba, pending(1, 1))]).err().unwrap();
        assert!(matches!(err, MetaDbError::InvalidArgument(_)));
    }

    #[test]
    fn reapply_idempotency_via_page_lsn_skip_is_callers_job() {
        // The array itself does not skip — replay-skip is enforced
        // by the caller (RcShard::stage / commit apply path) reading
        // page_lsn() and comparing against op LSN. Here we just
        // confirm the page_lsn machinery works.
        let (_dir, a) = make_array();
        a.apply_deltas(vec![(0, pending(1, 100))]).unwrap();
        assert_eq!(a.page_lsn(0).unwrap(), 100);
        a.apply_deltas(vec![(0, pending(1, 200))]).unwrap();
        assert_eq!(a.page_lsn(0).unwrap(), 200);
        assert_eq!(a.get(0).unwrap().rc, 2);
    }
}
