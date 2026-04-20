//! Per-BTree page buffer: a simple HashMap-based cache with dirty
//! tracking.
//!
//! This is the phase-2, single-writer scratch space. Every read and
//! write performed by a `BTree` goes through one `PageBuf`; pages live
//! in memory until [`PageBuf::flush`], which seals and writes every
//! dirty page through the underlying [`PageStore`] and then `fsync`s.
//!
//! There is no eviction: the buffer grows to hold every page touched
//! in a session. For phase 2 that is bounded by the working set of
//! the active operation (tree depth × constant factor), so a HashMap
//! suffices. Phase 8 will replace this with a clock-pro cache with
//! pinning semantics.
//!
//! Concurrency is out of scope here — `PageBuf` is `&mut self` only.

use std::collections::HashMap;
use std::sync::Arc;

use crate::btree::format::{init_internal, init_leaf};
use crate::error::Result;
use crate::page::Page;
use crate::page_store::PageStore;
use crate::types::{Lsn, PageId};

/// Page buffer.
pub struct PageBuf {
    page_store: Arc<PageStore>,
    pages: HashMap<PageId, Slot>,
}

struct Slot {
    page: Page,
    dirty: bool,
}

impl PageBuf {
    /// Construct an empty buffer backed by `page_store`.
    pub fn new(page_store: Arc<PageStore>) -> Self {
        Self {
            page_store,
            pages: HashMap::new(),
        }
    }

    /// Page store underlying this buffer.
    pub fn page_store(&self) -> &Arc<PageStore> {
        &self.page_store
    }

    /// Read-only access to a page. Loads from disk on miss and caches
    /// the result as clean.
    pub fn read(&mut self, pid: PageId) -> Result<&Page> {
        self.ensure_loaded(pid)?;
        Ok(&self.pages[&pid].page)
    }

    /// Mutable access to a page. The returned page is stamped with
    /// `generation` and marked dirty.
    pub fn modify(&mut self, pid: PageId, generation: Lsn) -> Result<&mut Page> {
        self.ensure_loaded(pid)?;
        let slot = self.pages.get_mut(&pid).unwrap();
        slot.page.set_generation(generation);
        slot.dirty = true;
        Ok(&mut slot.page)
    }

    /// Allocate a brand-new leaf page, initialize its header, cache as
    /// dirty, and return its page id. Caller must [`flush`](Self::flush)
    /// to make the allocation durable.
    pub fn alloc_leaf(&mut self, generation: Lsn) -> Result<PageId> {
        let pid = self.page_store.allocate()?;
        let mut page = Page::zeroed();
        init_leaf(&mut page, generation);
        self.pages.insert(pid, Slot { page, dirty: true });
        Ok(pid)
    }

    /// Allocate a brand-new internal page containing a single child
    /// and no separator keys. Cached as dirty.
    pub fn alloc_internal(&mut self, generation: Lsn, first_child: PageId) -> Result<PageId> {
        let pid = self.page_store.allocate()?;
        let mut page = Page::zeroed();
        init_internal(&mut page, generation, first_child);
        self.pages.insert(pid, Slot { page, dirty: true });
        Ok(pid)
    }

    /// Drop a page from the cache. Does *not* free the page in the
    /// underlying store — use [`free`](Self::free) for that.
    pub fn forget(&mut self, pid: PageId) {
        self.pages.remove(&pid);
    }

    /// Return a page to the underlying page store's free list,
    /// stamping the freed page with `generation`. The page is removed
    /// from the cache whether or not it was present.
    pub fn free(&mut self, pid: PageId, generation: Lsn) -> Result<()> {
        self.pages.remove(&pid);
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

    /// Number of dirty pages currently pending flush.
    pub fn dirty_count(&self) -> usize {
        self.pages.values().filter(|s| s.dirty).count()
    }

    /// Seal every dirty page, write through the page store in
    /// ascending page-id order, then fsync once at the end. Clean
    /// pages are left untouched. After this returns, all pages in the
    /// cache are clean.
    pub fn flush(&mut self) -> Result<()> {
        let mut dirty: Vec<PageId> = self
            .pages
            .iter()
            .filter_map(|(pid, slot)| if slot.dirty { Some(*pid) } else { None })
            .collect();
        if dirty.is_empty() {
            return Ok(());
        }
        dirty.sort_unstable();
        for pid in &dirty {
            let slot = self.pages.get_mut(pid).unwrap();
            slot.page.seal();
            self.page_store.write_page(*pid, &slot.page)?;
            slot.dirty = false;
        }
        self.page_store.sync()?;
        Ok(())
    }

    fn ensure_loaded(&mut self, pid: PageId) -> Result<()> {
        if self.pages.contains_key(&pid) {
            return Ok(());
        }
        let page = self.page_store.read_page(pid)?;
        self.pages.insert(pid, Slot { page, dirty: false });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::format::{L2pValue, leaf_insert, leaf_key_at, leaf_key_count, leaf_value_at};
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
        leaf_insert(buf.modify(pid, 7).unwrap(), 0, 42, &L2pValue::ZERO).unwrap();
        buf.flush().unwrap();
        assert_eq!(buf.dirty_count(), 0);

        // Read it back via a fresh PageBuf / fresh PageStore handle.
        let mut buf2 = PageBuf::new(ps);
        let p = buf2.read(pid).unwrap();
        assert_eq!(leaf_key_count(p), 1);
        assert_eq!(leaf_key_at(p, 0), 42);
        assert_eq!(leaf_value_at(p, 0), L2pValue::ZERO);
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

    #[test]
    fn alloc_internal_starts_with_one_child() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps);
        let leaf_pid = buf.alloc_leaf(1).unwrap();
        let internal_pid = buf.alloc_internal(1, leaf_pid).unwrap();
        let p = buf.read(internal_pid).unwrap();
        assert_eq!(p.header().unwrap().page_type, PageType::L2pInternal);
        // child[0] is the leaf we passed in.
        use crate::btree::format::internal_child_at;
        assert_eq!(internal_child_at(p, 0), leaf_pid);
    }
}
