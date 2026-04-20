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

use std::collections::HashMap;
use std::sync::Arc;

use crate::btree::format::{init_internal, init_leaf};
use crate::cache::{DEFAULT_PAGE_CACHE_BYTES, PageCache};
use crate::error::Result;
use crate::page::Page;
use crate::page_store::PageStore;
use crate::types::{Lsn, PageId};

/// Page buffer.
pub struct PageBuf {
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    pages: HashMap<PageId, Slot>,
}

enum Slot {
    Clean(Arc<Page>),
    Dirty(Page),
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
            pages: HashMap::new(),
        }
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

    /// Mutable page access. The returned page is stamped with
    /// `generation` and marked dirty.
    pub fn modify(&mut self, pid: PageId, generation: Lsn) -> Result<&mut Page> {
        let page = match self.pages.remove(&pid) {
            Some(Slot::Dirty(mut page)) => {
                page.set_generation(generation);
                page
            }
            Some(Slot::Clean(page)) => {
                self.page_cache.invalidate(pid);
                let mut page = (*page).clone();
                page.set_generation(generation);
                page
            }
            None => {
                let mut page = self.page_cache.get_for_modify(pid)?;
                page.set_generation(generation);
                page
            }
        };
        self.pages.insert(pid, Slot::Dirty(page));
        match self.pages.get_mut(&pid).unwrap() {
            Slot::Dirty(page) => Ok(page),
            Slot::Clean(_) => unreachable!("modify always stores a dirty page"),
        }
    }

    /// Allocate a brand-new leaf page, initialize its header, cache as
    /// dirty, and return its page id.
    pub fn alloc_leaf(&mut self, generation: Lsn) -> Result<PageId> {
        let pid = self.page_store.allocate()?;
        let mut page = Page::zeroed();
        init_leaf(&mut page, generation);
        self.pages.insert(pid, Slot::Dirty(page));
        Ok(pid)
    }

    /// Allocate a brand-new internal page with a single child and no
    /// separator keys. Cached as dirty.
    pub fn alloc_internal(&mut self, generation: Lsn, first_child: PageId) -> Result<PageId> {
        let pid = self.page_store.allocate()?;
        let mut page = Page::zeroed();
        init_internal(&mut page, generation, first_child);
        self.pages.insert(pid, Slot::Dirty(page));
        Ok(pid)
    }

    /// Drop a page from the cache. Does *not* free the page in the
    /// underlying store — use [`free`](Self::free) for that.
    pub fn forget(&mut self, pid: PageId) {
        self.pages.remove(&pid);
    }

    /// Return a page to the underlying page store's free list,
    /// stamping the freed page with `generation`. Low-level primitive:
    /// the caller is responsible for making sure nothing else still
    /// references the page. In the no-snapshot world this is trivially
    /// safe because every live page has exactly one parent.
    pub fn free(&mut self, pid: PageId, generation: Lsn) -> Result<()> {
        self.pages.remove(&pid);
        self.page_cache.invalidate(pid);
        self.page_store.free(pid, generation)?;
        Ok(())
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
            self.pages.insert(pid, Slot::Clean(page));
        }
        Ok(())
    }

    fn ensure_loaded(&mut self, pid: PageId) -> Result<()> {
        if self.pages.contains_key(&pid) {
            return Ok(());
        }
        let page = self.page_cache.get(pid)?;
        self.pages.insert(pid, Slot::Clean(page));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::format::{leaf_insert, leaf_key_at, leaf_key_count, leaf_value_at};
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
        leaf_insert(buf.modify(pid, 7).unwrap(), 0, 42, 99).unwrap();
        buf.flush().unwrap();
        assert_eq!(buf.dirty_count(), 0);

        let mut buf2 = PageBuf::new(ps);
        let p = buf2.read(pid).unwrap();
        assert_eq!(leaf_key_count(p), 1);
        assert_eq!(leaf_key_at(p, 0), 42);
        assert_eq!(leaf_value_at(p, 0), 99);
    }

    #[test]
    fn modify_bumps_generation_and_dirties() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps);
        let pid = buf.alloc_leaf(5).unwrap();
        buf.flush().unwrap();
        assert_eq!(buf.dirty_count(), 0);
        let p = buf.modify(pid, 9).unwrap();
        assert_eq!(p.header().unwrap().generation, 9);
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
