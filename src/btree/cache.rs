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

use crate::btree::format::{init_internal, init_leaf, internal_child_at, internal_key_count};
use crate::error::{MetaDbError, Result};
use crate::page::{Page, PageType};
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

/// Reported outcome of a [`PageBuf::decref`] call on the top-level
/// page id. Cascading frees inside the call are not individually
/// reported; callers that need them should walk the subtree or
/// observe the page store's free list.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DecrefOutcome {
    /// Refcount decremented but still > 0; page remains live.
    Decremented,
    /// Refcount reached zero; page was freed (and any children it
    /// uniquely owned were cascaded).
    Freed,
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
    ///
    /// Low-level primitive: skips all refcount accounting. Callers
    /// that care about shared pages should use [`decref`](Self::decref)
    /// instead.
    pub fn free(&mut self, pid: PageId, generation: Lsn) -> Result<()> {
        self.pages.remove(&pid);
        self.page_store.free(pid, generation)?;
        Ok(())
    }

    // -------- reference-counting + CoW -----------------------------------

    /// Bump the refcount of `pid` by one, stamping the mutation with
    /// `generation`. Returns the new refcount.
    pub fn incref(&mut self, pid: PageId, generation: Lsn) -> Result<u32> {
        let page = self.modify(pid, generation)?;
        let new_rc = page
            .refcount()
            .checked_add(1)
            .ok_or_else(|| MetaDbError::Corruption(format!("refcount overflow on page {pid}")))?;
        page.set_refcount(new_rc);
        Ok(new_rc)
    }

    /// Decrement the refcount of `pid` by one, freeing the page (and
    /// recursively decref'ing its children if it's an internal node)
    /// when the count hits zero. Uses an explicit worklist so deep
    /// trees don't blow the stack.
    ///
    /// Returns [`DecrefOutcome::Decremented`] if the top-level `pid`
    /// still has references, or [`DecrefOutcome::Freed`] if it was
    /// released.
    pub fn decref(&mut self, pid: PageId, generation: Lsn) -> Result<DecrefOutcome> {
        let mut top_outcome: Option<DecrefOutcome> = None;
        let mut worklist: Vec<PageId> = vec![pid];
        while let Some(p) = worklist.pop() {
            let (new_rc, children) = {
                let page = self.modify(p, generation)?;
                let rc = page.refcount();
                if rc == 0 {
                    return Err(MetaDbError::Corruption(format!(
                        "decref on page {p} with refcount already zero",
                    )));
                }
                let new_rc = rc - 1;
                page.set_refcount(new_rc);
                let children = if new_rc == 0 && page.header()?.page_type == PageType::L2pInternal {
                    let kc = internal_key_count(page);
                    (0..=kc).map(|i| internal_child_at(page, i)).collect()
                } else {
                    Vec::new()
                };
                (new_rc, children)
            };
            if top_outcome.is_none() {
                top_outcome = Some(if new_rc == 0 {
                    DecrefOutcome::Freed
                } else {
                    DecrefOutcome::Decremented
                });
            }
            if new_rc == 0 {
                worklist.extend(children);
                self.pages.remove(&p);
                self.page_store.free(p, generation)?;
            }
        }
        Ok(top_outcome.expect("worklist was non-empty"))
    }

    /// Ensure the caller can modify the page at `pid` without
    /// disturbing any other page that shares it (e.g., a snapshot).
    ///
    /// - If `pid`'s refcount is 1, returns `pid`: no one else points
    ///   here, in-place edits are safe.
    /// - If `pid`'s refcount is > 1, copies the page to a newly-
    ///   allocated page, leaves the original at refcount - 1 (it's
    ///   still reachable from the other references), bumps each of
    ///   the new copy's children's refcount (the new page is now an
    ///   additional parent of each), and returns the new page id.
    ///
    /// The caller is responsible for updating the parent that used to
    /// point at `pid` so it now points at the returned page id.
    pub fn cow_for_write(&mut self, pid: PageId, generation: Lsn) -> Result<PageId> {
        // Snapshot just the bits we need from the source page under a
        // short borrow.
        let (current_rc, children) = {
            let page = self.read(pid)?;
            let header = page.header()?;
            if header.refcount <= 1 {
                return Ok(pid);
            }
            let children = if header.page_type == PageType::L2pInternal {
                let kc = internal_key_count(page);
                (0..=kc).map(|i| internal_child_at(page, i)).collect()
            } else {
                Vec::new()
            };
            (header.refcount, children)
        };

        // Allocate a fresh page, copy the source verbatim, reset
        // generation + refcount.
        let new_pid = self.page_store.allocate()?;
        let mut new_page = Page::zeroed();
        new_page
            .bytes_mut()
            .copy_from_slice(self.read(pid)?.bytes());
        new_page.set_generation(generation);
        new_page.set_refcount(1);
        self.pages.insert(
            new_pid,
            Slot {
                page: new_page,
                dirty: true,
            },
        );

        // The new copy is an additional parent of every child.
        for c in &children {
            self.incref(*c, generation)?;
        }

        // The original has one fewer parent (the caller used to hold it).
        {
            let old = self.modify(pid, generation)?;
            old.set_refcount(current_rc - 1);
        }

        Ok(new_pid)
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

    // -------- refcount + CoW tests -------------------------------------

    #[test]
    fn incref_bumps_and_persists_after_flush() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps.clone());
        let pid = buf.alloc_leaf(1).unwrap();
        assert_eq!(buf.read(pid).unwrap().refcount(), 1);
        assert_eq!(buf.incref(pid, 2).unwrap(), 2);
        assert_eq!(buf.read(pid).unwrap().refcount(), 2);
        buf.flush().unwrap();
        let mut buf2 = PageBuf::new(ps);
        assert_eq!(buf2.read(pid).unwrap().refcount(), 2);
    }

    #[test]
    fn decref_on_unique_page_frees_it() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps.clone());
        let pid = buf.alloc_leaf(1).unwrap();
        buf.flush().unwrap();
        let free_before = ps.free_list_len();
        let out = buf.decref(pid, 2).unwrap();
        assert_eq!(out, DecrefOutcome::Freed);
        assert_eq!(ps.free_list_len(), free_before + 1);
        assert!(!buf.contains(pid));
    }

    #[test]
    fn decref_on_shared_page_just_decrements() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps);
        let pid = buf.alloc_leaf(1).unwrap();
        buf.incref(pid, 2).unwrap(); // rc = 2
        let out = buf.decref(pid, 3).unwrap();
        assert_eq!(out, DecrefOutcome::Decremented);
        assert_eq!(buf.read(pid).unwrap().refcount(), 1);
    }

    #[test]
    fn decref_on_internal_cascades_to_children_when_freed() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps.clone());
        let leaf0 = buf.alloc_leaf(1).unwrap();
        let leaf1 = buf.alloc_leaf(1).unwrap();
        let internal = buf.alloc_internal(1, leaf0).unwrap();
        crate::btree::format::internal_insert(buf.modify(internal, 2).unwrap(), 0, 42, leaf1)
            .unwrap();
        buf.flush().unwrap();
        let before = ps.free_list_len();
        let out = buf.decref(internal, 3).unwrap();
        assert_eq!(out, DecrefOutcome::Freed);
        // internal + leaf0 + leaf1 all went to the free list.
        assert_eq!(ps.free_list_len(), before + 3);
    }

    #[test]
    fn decref_on_internal_with_shared_children_stops_at_child() {
        // internal is unique (rc=1); its two children are each shared
        // (rc=2). Dropping internal frees internal only; leaves drop
        // to rc=1 (still live).
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps.clone());
        let leaf0 = buf.alloc_leaf(1).unwrap();
        buf.incref(leaf0, 1).unwrap();
        let leaf1 = buf.alloc_leaf(1).unwrap();
        buf.incref(leaf1, 1).unwrap();
        let internal = buf.alloc_internal(1, leaf0).unwrap();
        crate::btree::format::internal_insert(buf.modify(internal, 2).unwrap(), 0, 42, leaf1)
            .unwrap();
        buf.flush().unwrap();
        let before = ps.free_list_len();
        buf.decref(internal, 3).unwrap();
        // Only the internal was freed.
        assert_eq!(ps.free_list_len(), before + 1);
        assert_eq!(buf.read(leaf0).unwrap().refcount(), 1);
        assert_eq!(buf.read(leaf1).unwrap().refcount(), 1);
    }

    #[test]
    fn cow_on_unique_page_is_noop() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps);
        let pid = buf.alloc_leaf(1).unwrap();
        let out = buf.cow_for_write(pid, 2).unwrap();
        assert_eq!(out, pid, "unique page must be modified in place");
        assert_eq!(buf.read(pid).unwrap().refcount(), 1);
    }

    #[test]
    fn cow_on_shared_leaf_allocates_new() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps);
        let pid = buf.alloc_leaf(7).unwrap();
        // Pre-load some distinctive payload so we can verify the copy.
        crate::btree::format::leaf_insert(
            buf.modify(pid, 7).unwrap(),
            0,
            99,
            &crate::btree::L2pValue::from_slice(&[0xAB]),
        )
        .unwrap();
        buf.incref(pid, 8).unwrap(); // rc=2: snapshot holds one edge.
        let new_pid = buf.cow_for_write(pid, 9).unwrap();
        assert_ne!(new_pid, pid);
        // Old page kept its content at refcount 1.
        assert_eq!(buf.read(pid).unwrap().refcount(), 1);
        assert_eq!(
            crate::btree::format::leaf_key_count(buf.read(pid).unwrap()),
            1
        );
        // New page has a fresh copy, refcount 1, stamped with generation 9.
        assert_eq!(buf.read(new_pid).unwrap().refcount(), 1);
        assert_eq!(buf.read(new_pid).unwrap().generation(), 9);
        assert_eq!(
            crate::btree::format::leaf_key_at(buf.read(new_pid).unwrap(), 0),
            99,
        );
    }

    #[test]
    fn cow_on_shared_internal_bumps_children() {
        let (_d, ps) = mk_store();
        let mut buf = PageBuf::new(ps);
        let leaf0 = buf.alloc_leaf(1).unwrap();
        let leaf1 = buf.alloc_leaf(1).unwrap();
        let internal = buf.alloc_internal(1, leaf0).unwrap();
        crate::btree::format::internal_insert(buf.modify(internal, 2).unwrap(), 0, 42, leaf1)
            .unwrap();
        // Share internal.
        buf.incref(internal, 3).unwrap();
        assert_eq!(buf.read(internal).unwrap().refcount(), 2);
        assert_eq!(buf.read(leaf0).unwrap().refcount(), 1);
        assert_eq!(buf.read(leaf1).unwrap().refcount(), 1);
        // CoW the internal: children get bumped.
        let new_internal = buf.cow_for_write(internal, 4).unwrap();
        assert_ne!(new_internal, internal);
        assert_eq!(buf.read(internal).unwrap().refcount(), 1);
        assert_eq!(buf.read(new_internal).unwrap().refcount(), 1);
        assert_eq!(buf.read(leaf0).unwrap().refcount(), 2);
        assert_eq!(buf.read(leaf1).unwrap().refcount(), 2);
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
