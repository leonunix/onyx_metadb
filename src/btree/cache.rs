//! Per-BTree page buffer layered on top of the shared [`PageCache`].
//!
//! Clean pages come from the shared cache as `Arc<Page>` handles; dirty
//! pages stay private to the `PageBuf` until [`flush`](PageBuf::flush),
//! at which point they are written through the underlying [`PageStore`]
//! and reinserted into the shared cache as clean entries.
//!
//! Phase 6.5b stripped the COW / refcount / drop_subtree machinery:
//! snapshots are an L2P concept now, and L2P moved to [`crate::paged`].
//! The refcount B+tree this cache backs does in-place updates only, so
//! `incref` / `decref` / `cow_for_write` / `DecrefOutcome` are gone.
//!
//! Concurrency is out of scope — `PageBuf` is `&mut self` only.

use std::sync::Arc;

use crate::btree::format::{init_internal, init_leaf};
use crate::cache::{DEFAULT_PAGE_CACHE_BYTES, PageCache};
use crate::error::Result;
use crate::page::Page;
use crate::page_store::PageStore;
use crate::paged::read_view::{PageIdMap, PageIdSet};
use crate::types::{Lsn, PageId};

const LOCAL_ALLOC_RUN_PAGES: usize = 256;

/// Page buffer.
pub struct PageBuf {
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    alloc_run_next: PageId,
    alloc_run_end: PageId,
    pages: PageIdMap<Slot>,
    clean_pages: PageIdSet,
}

enum Slot {
    Clean(Arc<Page>),
    Dirty(Page),
}

pub(crate) struct DirtySnapshot {
    page_cache: Arc<PageCache>,
    pages: Vec<DirtySnapshotPage>,
}

pub(crate) struct FlushedSnapshot {
    pages: Vec<FlushedSnapshotPage>,
}

impl FlushedSnapshot {
    pub(crate) fn pages_count(&self) -> usize {
        self.pages.len()
    }

    pub(crate) fn append_sealed_pages(&self, out: &mut Vec<(PageId, Arc<Page>)>) {
        out.extend(
            self.pages
                .iter()
                .map(|page| (page.pid, page.sealed.clone())),
        );
    }
}

struct DirtySnapshotPage {
    pid: PageId,
    original: Page,
}

struct FlushedSnapshotPage {
    pid: PageId,
    original: Page,
    sealed: Arc<Page>,
}

impl Slot {
    fn page(&self) -> &Page {
        match self {
            Self::Clean(page) => page,
            Self::Dirty(page) => page,
        }
    }

    fn is_dirty(&self) -> bool {
        matches!(self, Self::Dirty(_))
    }
}

impl PageBuf {
    /// New buffer with a private page cache of default size.
    pub fn new(page_store: Arc<PageStore>) -> Self {
        let page_cache = Arc::new(PageCache::new(page_store.clone(), DEFAULT_PAGE_CACHE_BYTES));
        Self::with_cache(page_store, page_cache)
    }

    /// New buffer sharing an existing `PageCache`.
    pub fn with_cache(page_store: Arc<PageStore>, page_cache: Arc<PageCache>) -> Self {
        Self {
            page_store,
            page_cache,
            alloc_run_next: 0,
            alloc_run_end: 0,
            pages: PageIdMap::default(),
            clean_pages: PageIdSet::default(),
        }
    }

    fn allocate_local(&mut self) -> Result<PageId> {
        if self.alloc_run_next < self.alloc_run_end {
            let pid = self.alloc_run_next;
            self.alloc_run_next += 1;
            return Ok(pid);
        }
        let start = self.page_store.allocate_run(LOCAL_ALLOC_RUN_PAGES)?;
        self.alloc_run_next = start + 1;
        self.alloc_run_end = start + LOCAL_ALLOC_RUN_PAGES as u64;
        Ok(start)
    }

    fn pages_insert(&mut self, pid: PageId, slot: Slot) {
        let is_clean = matches!(slot, Slot::Clean(_));
        let old = self.pages.insert(pid, slot);
        if matches!(old, Some(Slot::Clean(_))) {
            self.clean_pages.remove(&pid);
        }
        if is_clean {
            self.clean_pages.insert(pid);
        }
    }

    fn pages_remove(&mut self, pid: PageId) -> Option<Slot> {
        let old = self.pages.remove(&pid);
        if matches!(old, Some(Slot::Clean(_))) {
            self.clean_pages.remove(&pid);
        }
        old
    }

    /// Page store underlying this buffer.
    pub fn page_store(&self) -> &Arc<PageStore> {
        &self.page_store
    }

    /// Read-only page access. Load from `PageCache` on miss.
    pub fn read(&mut self, pid: PageId) -> Result<&Page> {
        self.ensure_loaded(pid)?;
        Ok(self.pages[&pid].page())
    }

    /// Mutable page access. Loads the page if not cached and marks it
    /// dirty. **Does not stamp `page.generation`** — that field is
    /// reserved for WAL-apply idempotency markers; see
    /// [`PagedPageBuf::modify`](crate::paged::PageBuf::modify) for the
    /// full rationale (the same invariant applies here because the
    /// refcount BTree's pages flow through the same WAL-apply paths).
    pub fn modify(&mut self, pid: PageId, _generation: Lsn) -> Result<&mut Page> {
        let page = match self.pages_remove(pid) {
            Some(Slot::Dirty(page)) => page,
            Some(Slot::Clean(page)) => {
                self.page_cache.invalidate(pid);
                (*page).clone()
            }
            None => self.page_cache.get_for_modify(pid)?,
        };
        self.pages_insert(pid, Slot::Dirty(page));
        match self.pages.get_mut(&pid).unwrap() {
            Slot::Dirty(page) => Ok(page),
            Slot::Clean(_) => unreachable!("modify always stores a dirty page"),
        }
    }

    /// Allocate a fresh page id and copy `src` into it as a private
    /// dirty page. The source page is left untouched; callers use this
    /// to path-copy protected checkpoint pages before mutating them.
    pub fn clone_private(&mut self, src: PageId, generation: Lsn) -> Result<PageId> {
        let new_pid = self.allocate_local()?;
        let mut page = self.read(src)?.clone();
        page.set_generation(generation);
        page.set_refcount(1);
        self.pages_insert(new_pid, Slot::Dirty(page));
        Ok(new_pid)
    }

    /// Allocate a brand-new leaf page, initialize its header, cache as
    /// dirty, and return its page id. Stamps `page.generation = 0`;
    /// see [`modify`](Self::modify) for why.
    pub fn alloc_leaf(&mut self, _generation: Lsn) -> Result<PageId> {
        let pid = self.allocate_local()?;
        let mut page = Page::zeroed();
        init_leaf(&mut page, 0);
        self.pages_insert(pid, Slot::Dirty(page));
        Ok(pid)
    }

    /// Allocate a brand-new internal page with a single child and no
    /// separator keys. Cached as dirty.
    pub fn alloc_internal(&mut self, _generation: Lsn, first_child: PageId) -> Result<PageId> {
        let pid = self.allocate_local()?;
        let mut page = Page::zeroed();
        init_internal(&mut page, 0, first_child);
        self.pages_insert(pid, Slot::Dirty(page));
        Ok(pid)
    }

    /// Drop a page from the cache. Does *not* free the page in the
    /// underlying store — use [`free`](Self::free) for that.
    pub fn forget(&mut self, pid: PageId) {
        self.pages_remove(pid);
    }

    /// Return a page to the underlying page store's free list,
    /// stamping the freed page with `generation`. Low-level primitive:
    /// the caller is responsible for making sure nothing else still
    /// references the page. In the no-snapshot world this is trivially
    /// safe because every live page has exactly one parent.
    pub fn free(&mut self, pid: PageId, generation: Lsn) -> Result<()> {
        self.pages_remove(pid);
        self.page_cache.invalidate(pid);
        self.page_store.free(pid, generation)?;
        Ok(())
    }

    /// Remove a page from this tree-local buffer because the caller is
    /// about to enqueue it for deferred free outside the tree lock.
    pub(crate) fn detach_for_free(&mut self, pid: PageId) {
        self.pages_remove(pid);
    }

    /// Whether `pid` is currently in the cache.
    pub fn contains(&self, pid: PageId) -> bool {
        self.pages.contains_key(&pid)
    }

    /// Number of pages in the cache (clean + dirty).
    pub fn len(&self) -> usize {
        self.pages.len()
    }

    /// True iff no pages are cached.
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    /// Dirty pages pending flush.
    pub fn dirty_count(&self) -> usize {
        self.pages.values().filter(|s| s.is_dirty()).count()
    }

    /// Drop every clean page from the private buffer. Clean state
    /// remains available through the shared [`PageCache`]; this only
    /// prevents the long-lived owning tree from keeping an unbounded
    /// second copy in `self.pages`.
    pub fn evict_clean_pages(&mut self) {
        if self.clean_pages.is_empty() {
            return;
        }
        let clean_pages = std::mem::take(&mut self.clean_pages);
        for pid in clean_pages {
            if matches!(self.pages.get(&pid), Some(Slot::Clean(_))) {
                self.pages.remove(&pid);
            }
        }
    }

    /// Seal every dirty page, write through the page store in ascending
    /// page-id order, then fsync once at the end. Clean pages are left
    /// untouched. After this returns, all pages in the cache are clean.
    pub fn flush(&mut self) -> Result<()> {
        let mut dirty: Vec<PageId> = self
            .pages
            .iter()
            .filter_map(|(pid, slot)| if slot.is_dirty() { Some(*pid) } else { None })
            .collect();
        if dirty.is_empty() {
            return Ok(());
        }
        dirty.sort_unstable();
        let mut flushed: Vec<(PageId, Arc<Page>)> = Vec::with_capacity(dirty.len());
        for pid in &dirty {
            let mut page = self.pages[pid].page().clone();
            page.seal();
            self.page_store.write_page(*pid, &page)?;
            flushed.push((*pid, Arc::new(page)));
        }
        self.page_store.sync()?;
        for (pid, page) in flushed {
            self.page_cache.insert(pid, page.clone());
            self.pages_insert(pid, Slot::Clean(page));
        }
        Ok(())
    }

    pub(crate) fn dirty_snapshot(&self) -> DirtySnapshot {
        let mut pages: Vec<_> = self
            .pages
            .iter()
            .filter_map(|(pid, slot)| match slot {
                Slot::Dirty(page) => Some(DirtySnapshotPage {
                    pid: *pid,
                    original: page.clone(),
                }),
                Slot::Clean(_) => None,
            })
            .collect();
        pages.sort_unstable_by_key(|page| page.pid);
        DirtySnapshot {
            page_cache: self.page_cache.clone(),
            pages,
        }
    }

    pub(crate) fn install_flushed_snapshot_page(
        &mut self,
        flushed: &FlushedSnapshot,
        page_idx: usize,
    ) -> Option<(PageId, bool)> {
        let page = flushed.pages.get(page_idx)?;
        let Some(Slot::Dirty(current)) = self.pages.get(&page.pid) else {
            return Some((page.pid, true));
        };
        if current.bytes() != page.original.bytes() {
            return Some((page.pid, false));
        }
        // DirtySnapshot::write() has already refreshed the shared cache
        // outside the shard lock. Drop the tree-local Dirty copy so install
        // does not defer a large clean-page retain scan to finish_op.
        self.pages_remove(page.pid);
        Some((page.pid, true))
    }

    fn ensure_loaded(&mut self, pid: PageId) -> Result<()> {
        if self.pages.contains_key(&pid) {
            return Ok(());
        }
        let page = self.page_cache.get(pid)?;
        self.pages_insert(pid, Slot::Clean(page));
        Ok(())
    }
}

impl DirtySnapshot {
    pub(crate) fn write(&self) -> Result<FlushedSnapshot> {
        if self.pages.is_empty() {
            return Ok(FlushedSnapshot { pages: Vec::new() });
        }
        let mut flushed = Vec::with_capacity(self.pages.len());
        let mut sealed_pages = Vec::with_capacity(self.pages.len());
        for page in &self.pages {
            let mut sealed = page.original.clone();
            sealed.seal();
            let sealed = Arc::new(sealed);
            sealed_pages.push((page.pid, sealed.clone()));
            flushed.push(FlushedSnapshotPage {
                pid: page.pid,
                original: page.original.clone(),
                sealed,
            });
        }
        for (pid, sealed) in sealed_pages {
            self.page_cache.insert(pid, sealed);
        }
        Ok(FlushedSnapshot { pages: flushed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::format::{RcEntry, leaf_insert, leaf_key_at, leaf_key_count, leaf_value_at};
    use crate::page::PageType;
    use tempfile::TempDir;

    fn mk_store() -> (TempDir, Arc<PageStore>) {
        let dir = TempDir::new().unwrap();
        let ps = Arc::new(PageStore::create(dir.path().join("p.onyx_meta")).unwrap());
        (dir, ps)
    }

    #[test]
    fn alloc_leaf_is_in_cache_and_dirty() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps);
        let pid = buf.alloc_leaf(1).unwrap();
        assert!(buf.contains(pid));
        assert_eq!(buf.len(), 1);
        assert_eq!(buf.dirty_count(), 1);
        let p = buf.read(pid).unwrap();
        assert_eq!(p.header().unwrap().page_type, PageType::L2pLeaf);
    }

    #[test]
    fn flush_persists_dirty_pages() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps.clone());
        let pid = buf.alloc_leaf(7).unwrap();
        leaf_insert(
            buf.modify(pid, 7).unwrap(),
            0,
            42,
            RcEntry {
                rc: 99,
                birth_lsn: 7,
            },
        )
        .unwrap();
        buf.flush().unwrap();
        assert_eq!(buf.dirty_count(), 0);

        let mut buf2 = PageBuf::new(ps);
        let p = buf2.read(pid).unwrap();
        assert_eq!(leaf_key_count(p), 1);
        assert_eq!(leaf_key_at(p, 0), 42);
        assert_eq!(
            leaf_value_at(p, 0),
            RcEntry {
                rc: 99,
                birth_lsn: 7,
            },
        );
    }

    #[test]
    fn install_flushed_snapshot_page_detaches_dirty_copy() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps);
        let pid = buf.alloc_leaf(7).unwrap();
        leaf_insert(
            buf.modify(pid, 7).unwrap(),
            0,
            42,
            RcEntry {
                rc: 99,
                birth_lsn: 7,
            },
        )
        .unwrap();

        let flushed = buf.dirty_snapshot().write().unwrap();
        assert_eq!(buf.dirty_count(), 1);
        assert_eq!(
            buf.install_flushed_snapshot_page(&flushed, 0),
            Some((pid, true))
        );

        assert!(!buf.contains(pid));
        assert_eq!(buf.dirty_count(), 0);
        let page = buf.read(pid).unwrap();
        assert_eq!(leaf_key_at(page, 0), 42);
        assert_eq!(
            leaf_value_at(page, 0),
            RcEntry {
                rc: 99,
                birth_lsn: 7,
            },
        );
    }

    #[test]
    fn install_flushed_snapshot_page_keeps_newer_dirty_copy() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps);
        let pid = buf.alloc_leaf(7).unwrap();
        leaf_insert(
            buf.modify(pid, 7).unwrap(),
            0,
            42,
            RcEntry {
                rc: 99,
                birth_lsn: 7,
            },
        )
        .unwrap();

        let snapshot = buf.dirty_snapshot();
        leaf_insert(
            buf.modify(pid, 8).unwrap(),
            1,
            43,
            RcEntry {
                rc: 12,
                birth_lsn: 8,
            },
        )
        .unwrap();
        let flushed = snapshot.write().unwrap();

        assert_eq!(
            buf.install_flushed_snapshot_page(&flushed, 0),
            Some((pid, false))
        );
        assert!(buf.contains(pid));
        assert_eq!(buf.dirty_count(), 1);
        let page = buf.read(pid).unwrap();
        assert_eq!(leaf_key_count(page), 2);
        assert_eq!(leaf_key_at(page, 0), 42);
        assert_eq!(leaf_key_at(page, 1), 43);
    }

    #[test]
    fn modify_bumps_generation_and_dirties() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps);
        let pid = buf.alloc_leaf(5).unwrap();
        buf.flush().unwrap();
        assert_eq!(buf.dirty_count(), 0);
        let p = buf.modify(pid, 9).unwrap();
        // `modify` no longer stamps generation (reserved for WAL apply
        // markers); alloc_leaf stamped 0 regardless of the `5` we
        // passed in. See `PageBuf::modify` doc.
        assert_eq!(p.header().unwrap().generation, 0);
        assert_eq!(buf.dirty_count(), 1);
    }

    #[test]
    fn read_miss_loads_from_disk() {
        let (_d, ps) = mk_store();
        let pid = {
            let mut buf = PageBuf::new(ps.clone());
            let pid = buf.alloc_leaf(1).unwrap();
            buf.flush().unwrap();
            pid
        };
        let mut buf2 = PageBuf::new(ps);
        assert!(!buf2.contains(pid));
        let _ = buf2.read(pid).unwrap();
        assert!(buf2.contains(pid));
        assert_eq!(buf2.dirty_count(), 0);
    }

    #[test]
    fn forget_removes_from_cache_only() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps.clone());
        let pid = buf.alloc_leaf(1).unwrap();
        buf.flush().unwrap();
        buf.forget(pid);
        assert!(!buf.contains(pid));
        // Page is still on disk and readable.
        let _ = buf.read(pid).unwrap();
    }

    #[test]
    fn free_removes_from_cache_and_frees_on_disk() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps.clone());
        let pid = buf.alloc_leaf(1).unwrap();
        buf.flush().unwrap();
        let free_before = ps.free_list_len();
        buf.free(pid, 2).unwrap();
        assert!(!buf.contains(pid));
        // Defer-free: physical Free-stamp + free-list push only happens
        // when reclaim runs.
        ps.try_reclaim().unwrap();
        assert_eq!(ps.free_list_len(), free_before + 1);
    }

    #[test]
    fn flush_is_idempotent_when_nothing_dirty() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps);
        buf.flush().unwrap(); // no-op
        let pid = buf.alloc_leaf(1).unwrap();
        buf.flush().unwrap();
        buf.flush().unwrap(); // second flush is no-op
        assert_eq!(buf.dirty_count(), 0);
        let _ = pid;
    }
}
