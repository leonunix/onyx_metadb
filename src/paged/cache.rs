//! Per-`PagedL2p` page buffer layered on top of the shared [`PageCache`].
//!
//! Structurally parallel to `btree::cache::PageBuf` but trimmed for the
//! paged tree's needs: the cascade in [`PageBuf::decref`] walks through
//! paged-index children rather than B+tree-internal children. Keeping a
//! separate buffer type avoids a knot of generic callbacks that would
//! otherwise have to parameterise the B+tree cache over page-type-
//! specific "collect children" logic.
//!
//! Concurrency is out of scope — the buffer is `&mut self` only, and
//! the owning `PagedL2p` is wrapped in a `Mutex` one level up.

use std::collections::HashMap;
use std::sync::Arc;

use crate::cache::{DEFAULT_PAGE_CACHE_BYTES, PageCache};
use crate::error::{MetaDbError, Result};
use crate::page::{Page, PageType};
use crate::page_store::PageStore;
use crate::paged::format::{index_collect_children, init_index, init_leaf, page_level};
use crate::types::{Lsn, NULL_PAGE, PageId};

/// Cache entry.
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

/// Reported outcome of a top-level [`PageBuf::decref`] call. Cascading
/// frees are not individually reported.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DecrefOutcome {
    /// Refcount decremented but still > 0; page remains live.
    Decremented,
    /// Refcount reached zero; page was freed (and any children it
    /// uniquely owned were cascaded).
    Freed,
}

/// Private buffer of pages a `PagedL2p` is reading / mutating. Clean
/// pages come from the shared `PageCache`; dirty pages live here until
/// [`flush`](Self::flush).
pub struct PageBuf {
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    pages: HashMap<PageId, Slot>,
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

    /// Underlying page store handle.
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

    /// Allocate a fresh leaf, initialize it, cache as dirty.
    pub fn alloc_leaf(&mut self, generation: Lsn) -> Result<PageId> {
        let pid = self.page_store.allocate()?;
        let mut page = Page::zeroed();
        init_leaf(&mut page, generation);
        self.pages.insert(pid, Slot::Dirty(page));
        Ok(pid)
    }

    /// Allocate a fresh index page at `level`, initialize it (all children
    /// NULL_PAGE), cache as dirty.
    pub fn alloc_index(&mut self, generation: Lsn, level: u8) -> Result<PageId> {
        let pid = self.page_store.allocate()?;
        let mut page = Page::zeroed();
        init_index(&mut page, generation, level);
        self.pages.insert(pid, Slot::Dirty(page));
        Ok(pid)
    }

    /// Drop from cache without freeing the underlying page. Cheap way
    /// to reclaim buffer memory for pages we know we won't touch again
    /// in this transaction.
    pub fn forget(&mut self, pid: PageId) {
        self.pages.remove(&pid);
    }

    /// Return `pid` to the page store's free list, stamping with
    /// `generation`. Low-level — skips refcount accounting. Use
    /// [`decref`](Self::decref) instead for shared pages.
    pub fn free(&mut self, pid: PageId, generation: Lsn) -> Result<()> {
        self.pages.remove(&pid);
        self.page_cache.invalidate(pid);
        self.page_store.free(pid, generation)?;
        Ok(())
    }

    /// Increment the refcount on `pid`. Returns the new refcount.
    pub fn incref(&mut self, pid: PageId, generation: Lsn) -> Result<u32> {
        let page = self.modify(pid, generation)?;
        let new_rc = page.refcount().checked_add(1).ok_or_else(|| {
            MetaDbError::Corruption(format!("paged: refcount overflow on page {pid}"))
        })?;
        page.set_refcount(new_rc);
        Ok(new_rc)
    }

    /// Decrement the refcount on `pid`. If it hits zero, the page is
    /// freed and — if it's a `PagedIndex` — its non-null children are
    /// recursively decref'd via an explicit worklist.
    pub fn decref(&mut self, pid: PageId, generation: Lsn) -> Result<DecrefOutcome> {
        let mut top: Option<DecrefOutcome> = None;
        let mut worklist: Vec<PageId> = vec![pid];
        while let Some(p) = worklist.pop() {
            let (new_rc, children) = {
                let page = self.modify(p, generation)?;
                let rc = page.refcount();
                if rc == 0 {
                    return Err(MetaDbError::Corruption(format!(
                        "paged: decref on page {p} with refcount 0"
                    )));
                }
                let new_rc = rc - 1;
                page.set_refcount(new_rc);
                let children = if new_rc == 0 {
                    match page.header()?.page_type {
                        PageType::PagedIndex => index_collect_children(page),
                        PageType::PagedLeaf => Vec::new(),
                        other => {
                            return Err(MetaDbError::Corruption(format!(
                                "paged: decref on non-paged page type {other:?} at {p}"
                            )));
                        }
                    }
                } else {
                    Vec::new()
                };
                (new_rc, children)
            };
            if top.is_none() {
                top = Some(if new_rc == 0 {
                    DecrefOutcome::Freed
                } else {
                    DecrefOutcome::Decremented
                });
            }
            if new_rc == 0 {
                worklist.extend(children);
                self.pages.remove(&p);
                self.page_cache.invalidate(p);
                self.page_store.free(p, generation)?;
            }
        }
        Ok(top.expect("worklist was non-empty"))
    }

    /// Copy-on-write: if `pid` has refcount 1, return `pid` unchanged.
    /// Otherwise allocate a fresh copy, decrement the original's rc,
    /// and bump each of the new copy's children's refcounts so the old
    /// tree is still internally consistent.
    pub fn cow_for_write(&mut self, pid: PageId, generation: Lsn) -> Result<PageId> {
        debug_assert!(pid != NULL_PAGE, "cow_for_write called on NULL_PAGE");
        let (current_rc, children, page_type) = {
            let page = self.read(pid)?;
            let header = page.header()?;
            if header.refcount <= 1 {
                return Ok(pid);
            }
            let children = match header.page_type {
                PageType::PagedIndex => index_collect_children(page),
                PageType::PagedLeaf => Vec::new(),
                other => {
                    return Err(MetaDbError::Corruption(format!(
                        "paged: cow_for_write on non-paged page type {other:?} at {pid}"
                    )));
                }
            };
            (header.refcount, children, header.page_type)
        };

        // Allocate a fresh page and clone bytes.
        let new_pid = self.page_store.allocate()?;
        let mut new_page = Page::zeroed();
        new_page
            .bytes_mut()
            .copy_from_slice(self.read(pid)?.bytes());
        new_page.set_generation(generation);
        new_page.set_refcount(1);
        self.pages.insert(new_pid, Slot::Dirty(new_page));

        // The new copy is an additional parent of every non-null child.
        for c in &children {
            self.incref(*c, generation)?;
        }

        // The original has one fewer parent.
        {
            let old = self.modify(pid, generation)?;
            old.set_refcount(current_rc - 1);
        }

        let _ = page_type; // (kept in scope for potential debug assertions)
        Ok(new_pid)
    }

    /// Whether `pid` is cached.
    pub fn contains(&self, pid: PageId) -> bool {
        self.pages.contains_key(&pid)
    }

    /// Total pages in the buffer (clean + dirty).
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

    /// Drop every clean page from the private buffer. The shared
    /// [`PageCache`] still retains them; this just prevents a long-
    /// lived owner from keeping an unbounded duplicate copy of clean
    /// pages alongside the bounded shared cache.
    pub fn evict_clean_pages(&mut self) {
        self.pages.retain(|_, slot| slot.is_dirty());
    }

    /// Seal + write + fsync every dirty page in ascending page-id order,
    /// then reinsert them into the shared `PageCache` as clean.
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

    /// Helper for tests / `PagedL2p::root_level`: read a page's level
    /// via the shared decoder.
    pub fn read_level(&mut self, pid: PageId) -> Result<u8> {
        let page = self.read(pid)?;
        page_level(page)
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
    use crate::paged::format::{L2pValue, index_child_at, index_set_child, leaf_bit_set, leaf_set};
    use tempfile::TempDir;

    fn mk_store() -> (TempDir, Arc<PageStore>) {
        let dir = TempDir::new().unwrap();
        let ps = Arc::new(PageStore::create(dir.path().join("p.onyx_meta")).unwrap());
        (dir, ps)
    }

    #[test]
    fn alloc_leaf_and_index_tagged_correctly() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps);
        let leaf = buf.alloc_leaf(1).unwrap();
        let idx = buf.alloc_index(1, 2).unwrap();
        assert_eq!(buf.read_level(leaf).unwrap(), 0);
        assert_eq!(buf.read_level(idx).unwrap(), 2);
    }

    #[test]
    fn flush_persists_leaf_content() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps.clone());
        let pid = buf.alloc_leaf(1).unwrap();
        let v = L2pValue([0x42u8; 28]);
        leaf_set(buf.modify(pid, 1).unwrap(), 5, &v);
        buf.flush().unwrap();

        let mut buf2 = PageBuf::new(ps);
        let p = buf2.read(pid).unwrap();
        assert!(leaf_bit_set(p, 5));
    }

    #[test]
    fn decref_cascades_into_index_children() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps.clone());
        let leaf0 = buf.alloc_leaf(1).unwrap();
        let leaf1 = buf.alloc_leaf(1).unwrap();
        let idx = buf.alloc_index(1, 1).unwrap();
        index_set_child(buf.modify(idx, 1).unwrap(), 0, leaf0);
        index_set_child(buf.modify(idx, 1).unwrap(), 42, leaf1);
        buf.flush().unwrap();

        let before = ps.free_list_len();
        let out = buf.decref(idx, 2).unwrap();
        assert_eq!(out, DecrefOutcome::Freed);
        // index + leaf0 + leaf1 all hit the free list.
        assert_eq!(ps.free_list_len(), before + 3);
    }

    #[test]
    fn decref_on_shared_index_stops_at_rc_decrement() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps);
        let leaf = buf.alloc_leaf(1).unwrap();
        let idx = buf.alloc_index(1, 1).unwrap();
        index_set_child(buf.modify(idx, 1).unwrap(), 0, leaf);
        buf.incref(idx, 2).unwrap(); // rc = 2
        let out = buf.decref(idx, 3).unwrap();
        assert_eq!(out, DecrefOutcome::Decremented);
        assert_eq!(buf.read(idx).unwrap().refcount(), 1);
        // Leaf is still reachable via the surviving reference.
        assert!(buf.contains(leaf) || buf.read(leaf).is_ok());
    }

    #[test]
    fn cow_on_shared_index_bumps_children() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps);
        let leaf = buf.alloc_leaf(1).unwrap();
        let idx = buf.alloc_index(1, 1).unwrap();
        index_set_child(buf.modify(idx, 1).unwrap(), 7, leaf);
        buf.incref(idx, 1).unwrap(); // rc(idx) = 2, rc(leaf) = 1
        let new_idx = buf.cow_for_write(idx, 2).unwrap();
        assert_ne!(new_idx, idx);
        assert_eq!(buf.read(idx).unwrap().refcount(), 1);
        assert_eq!(buf.read(new_idx).unwrap().refcount(), 1);
        assert_eq!(
            index_child_at(buf.read(new_idx).unwrap(), 7),
            leaf,
            "new index should share the same child pointer"
        );
        // Leaf picked up the new parent: rc went from 1 → 2.
        assert_eq!(buf.read(leaf).unwrap().refcount(), 2);
    }

    #[test]
    fn cow_on_unique_page_is_noop() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps);
        let pid = buf.alloc_leaf(1).unwrap();
        let out = buf.cow_for_write(pid, 2).unwrap();
        assert_eq!(out, pid);
        assert_eq!(buf.read(pid).unwrap().refcount(), 1);
    }
}
