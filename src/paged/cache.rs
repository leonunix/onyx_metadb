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
    /// Live count of `Slot::Clean` entries in `pages`. Updated in
    /// lockstep with every mutation of `pages` so `evict_clean_pages`
    /// can short-circuit the `HashMap::retain` scan when nothing is
    /// clean. Prefill/write-heavy workloads leave every entry dirty
    /// after each op, so without the short-circuit every insert pays
    /// an O(N) scan over a thousands-large HashMap.
    clean_count: usize,
    /// In-memory rc-delta accumulator for the current op.
    ///
    /// [`cow_for_write`](Self::cow_for_write) no longer mutates rc on
    /// disk during the descent — it feeds a `(pid, delta)` entry here
    /// instead, and a single batch commit at end-of-op
    /// ([`commit_rc_deltas`](Self::commit_rc_deltas)) routes every
    /// non-zero net delta through
    /// [`PageStore::atomic_rc_delta_with_gen`]. This design:
    ///
    /// - cancels the `+1` (parent cow's incref) against the matching
    ///   `-1` (child's own cow's decref) on pages the descent cow's
    ///   multiple times in the same op — they net to zero and never
    ///   hit disk, avoiding a gen-stamp false-skip that would otherwise
    ///   corrupt rc;
    /// - keeps cross-tree atomicity (the batch commit uses the
    ///   disk-direct per-pid-locked RMW so a sibling tree's next op
    ///   sees a consistent post-commit view);
    /// - preserves WAL-replay idempotency (the commit's gen-stamp
    ///   guard skips deltas the crashed prior attempt already landed).
    pending_rc: HashMap<PageId, i32>,
    rc_delta_lsn: Lsn,
    rc_delta_ordinal: u32,
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
            clean_count: 0,
            pending_rc: HashMap::new(),
            rc_delta_lsn: 0,
            rc_delta_ordinal: 0,
        }
    }

    /// Insert a slot, keeping `clean_count` consistent with the
    /// Clean/Dirty delta relative to any existing entry at `pid`.
    /// All mutation of `self.pages` must go through one of these
    /// helpers; direct `.insert` / `.remove` calls on `self.pages`
    /// will drift the counter.
    fn pages_insert(&mut self, pid: PageId, slot: Slot) {
        let is_clean = matches!(slot, Slot::Clean(_));
        let old = self.pages.insert(pid, slot);
        let was_clean = matches!(old, Some(Slot::Clean(_)));
        match (was_clean, is_clean) {
            (true, false) => self.clean_count -= 1,
            (false, true) => self.clean_count += 1,
            _ => {}
        }
    }

    /// Remove a slot, keeping `clean_count` consistent.
    fn pages_remove(&mut self, pid: PageId) -> Option<Slot> {
        let old = self.pages.remove(&pid);
        if matches!(old, Some(Slot::Clean(_))) {
            self.clean_count -= 1;
        }
        old
    }

    /// Underlying page store handle.
    pub fn page_store(&self) -> &Arc<PageStore> {
        &self.page_store
    }

    /// Shared page cache handle. Exposed so tree-level warmup paths
    /// (`PagedL2p::warmup_index_pages`) can pin pages without
    /// round-tripping through `PageBuf`'s per-op scratch storage.
    pub fn page_cache(&self) -> &Arc<PageCache> {
        &self.page_cache
    }

    /// Read-only page access. Load from `PageCache` on miss.
    pub fn read(&mut self, pid: PageId) -> Result<&Page> {
        self.ensure_loaded(pid)?;
        Ok(self.pages[&pid].page())
    }

    /// Mutable page access. Loads the page if not cached and marks it
    /// dirty. **Does not stamp `page.generation`** — that field is
    /// reserved for WAL-apply idempotency markers
    /// ([`apply_drop_snapshot_pages`](crate::db) /
    /// [`apply_clone_volume_incref`](crate::db)); tree-internal cow
    /// scratches should never overwrite it, or the gen-based
    /// `>= lsn` guard in those apply paths would spuriously fire.
    /// The `_generation` argument is kept for API continuity and to
    /// make call-site LSN-awareness visible, but is intentionally
    /// ignored.
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
    /// dirty page. The source page and its on-disk refcount are left
    /// untouched; the tree layer uses this for checkpoint shadowing.
    pub fn clone_private(&mut self, src: PageId, generation: Lsn) -> Result<PageId> {
        let new_pid = self.page_store.allocate()?;
        let mut page = self.read(src)?.clone();
        page.set_generation(generation);
        page.set_refcount(1);
        self.pages_insert(new_pid, Slot::Dirty(page));
        Ok(new_pid)
    }

    /// Allocate a fresh leaf, initialize it, cache as dirty. Stamps
    /// `page.generation = 0` so the WAL-apply idempotency guard treats
    /// newly-allocated tree pages as untouched by any WAL op.
    pub fn alloc_leaf(&mut self, _generation: Lsn) -> Result<PageId> {
        let pid = self.page_store.allocate()?;
        let mut page = Page::zeroed();
        init_leaf(&mut page, 0);
        self.pages_insert(pid, Slot::Dirty(page));
        Ok(pid)
    }

    /// Allocate a fresh index page at `level`, initialize it (all children
    /// NULL_PAGE), cache as dirty. See [`alloc_leaf`](Self::alloc_leaf)
    /// for why the generation is stamped as 0.
    pub fn alloc_index(&mut self, _generation: Lsn, level: u8) -> Result<PageId> {
        let pid = self.page_store.allocate()?;
        let mut page = Page::zeroed();
        init_index(&mut page, 0, level);
        self.pages_insert(pid, Slot::Dirty(page));
        Ok(pid)
    }

    /// Drop from cache without freeing the underlying page. Cheap way
    /// to reclaim buffer memory for pages we know we won't touch again
    /// in this transaction.
    pub fn forget(&mut self, pid: PageId) {
        self.pages_remove(pid);
    }

    /// Drop every page from the local cache without touching the shared
    /// `PageCache` or the on-disk refcounts. Used by `attach_subtree_root`
    /// (Phase 7) when the tree's root is swapped out from under it: every
    /// page pid held in `self.pages` is about to refer to a different
    /// subtree, so the dirty-flag tracking would be wrong. The caller is
    /// responsible for making sure the old root was already flushed.
    pub fn forget_all(&mut self) {
        self.pages.clear();
        self.clean_count = 0;
    }

    /// Return `pid` to the page store's free list, stamping with
    /// `generation`. Low-level — skips refcount accounting. Use
    /// [`decref`](Self::decref) instead for shared pages.
    pub fn free(&mut self, pid: PageId, generation: Lsn) -> Result<()> {
        self.pages_remove(pid);
        self.page_cache.invalidate(pid);
        self.page_store.free(pid, generation)?;
        Ok(())
    }

    /// Effective refcount of `pid` combining the on-disk header with
    /// this op's queued [`pending_rc`](Self::pending_rc) delta. Used by
    /// [`PagedL2p::insert_at_lsn_with_share_info`](crate::paged::PagedL2p::insert_at_lsn_with_share_info)
    /// to capture whether a leaf page was shared before the current op
    /// COW'd it — the check `effective_rc > 1` matches the same test
    /// [`cow_for_write`](Self::cow_for_write) uses when deciding whether
    /// to clone.
    ///
    /// Returning `i64` mirrors [`cow_for_write`]'s internal arithmetic:
    /// pending deltas may temporarily push the effective count below 1
    /// mid-op, and we forward the raw value so callers can `> 1` compare
    /// against the same convention.
    pub fn effective_rc(&mut self, pid: PageId) -> Result<i64> {
        let pending = self.pending_rc.get(&pid).copied().unwrap_or(0);
        let page = self.read(pid)?;
        let header = page.header()?;
        Ok(i64::from(header.refcount) + i64::from(pending))
    }

    /// Cross-tree-safe incref via per-pid-locked disk-direct RMW.
    /// Persists any Dirty copy of `pid` to disk first so
    /// [`PageStore::atomic_rc_delta`] reads the latest bytes, then
    /// invalidates both the local buffer and the shared [`PageCache`]
    /// entry so subsequent reads pull the post-RMW bytes.
    ///
    /// Leaves `page.generation` untouched: snapshot take/drop callers
    /// are not WAL-replayed and must not collide with a future WAL
    /// op's LSN (using `atomic_rc_delta_with_gen` here with a
    /// non-WAL stamp would spuriously skip a later op at the same LSN).
    /// Cross-tree safety comes purely from the per-pid mutex, which
    /// both `atomic_rc_delta` and `atomic_rc_delta_with_gen` hold.
    pub fn atomic_incref(&mut self, pid: PageId) -> Result<u32> {
        self.persist_if_dirty(pid)?;
        let new_rc = self.page_store.atomic_rc_delta(pid, 1)?;
        self.pages_remove(pid);
        self.page_cache.invalidate(pid);
        Ok(new_rc)
    }

    /// Non-cascading single-page cross-tree-safe decref. Decrements
    /// `pid`'s refcount via the per-pid-locked disk-direct RMW; if the
    /// new rc is zero, frees the page and returns its children (for
    /// the caller to cascade into). On `Decremented` the second
    /// element is empty.
    ///
    /// Callers that want automatic cascade should use
    /// [`atomic_decref`](Self::atomic_decref) instead;
    /// [`PagedL2p::drop_subtree`](crate::paged::PagedL2p::drop_subtree)
    /// uses this variant so it can collect per-page leaf values before
    /// the free.
    pub fn atomic_decref_one(
        &mut self,
        pid: PageId,
    ) -> Result<(DecrefOutcome, Vec<PageId>)> {
        // Snapshot children + page type before the RMW — if rc hits
        // zero we need them for cascade. Read via PageBuf so a Dirty
        // copy wins; `persist_if_dirty` then flushes it to disk so
        // the atomic RMW reads fresh bytes.
        let children = {
            let page = self.read(pid)?;
            let header = page.header()?;
            match header.page_type {
                PageType::PagedIndex => index_collect_children(page),
                PageType::PagedLeaf => Vec::new(),
                other => {
                    return Err(MetaDbError::Corruption(format!(
                        "paged: atomic_decref_one on non-paged page type {other:?} at {pid}"
                    )));
                }
            }
        };
        self.persist_if_dirty(pid)?;
        let new_rc = self.page_store.atomic_rc_delta(pid, -1)?;
        self.pages_remove(pid);
        self.page_cache.invalidate(pid);
        if new_rc == 0 {
            // Not `free_idempotent` — this path is not WAL-replayed,
            // so a double-free is a genuine bug, not a re-apply.
            self.page_store.free(pid, 0)?;
            Ok((DecrefOutcome::Freed, children))
        } else {
            Ok((DecrefOutcome::Decremented, Vec::new()))
        }
    }

    /// Cross-tree-safe decref with cascading free, peer of
    /// [`atomic_incref`](Self::atomic_incref). Walks children through
    /// an explicit worklist; every rc mutation and every free routes
    /// through [`atomic_decref_one`](Self::atomic_decref_one).
    pub fn atomic_decref(&mut self, pid: PageId) -> Result<DecrefOutcome> {
        let mut top: Option<DecrefOutcome> = None;
        let mut worklist: Vec<PageId> = vec![pid];
        while let Some(p) = worklist.pop() {
            let (outcome, children) = self.atomic_decref_one(p)?;
            if top.is_none() {
                top = Some(outcome);
            }
            worklist.extend(children);
        }
        Ok(top.expect("worklist was non-empty"))
    }

    /// Seal + write `pid` to disk if it is currently Dirty in this
    /// buffer, leaving the PageBuf/PageCache entries untouched. Used by
    /// [`cow_for_write`](Self::cow_for_write) before a disk-direct
    /// atomic rc RMW so the read inside
    /// [`PageStore::atomic_rc_delta_with_gen`] sees the latest bytes.
    /// The RMW then overwrites the page with the post-delta state; the
    /// caller is expected to drop `pid` from both `pages` and
    /// `page_cache` afterwards so nothing observes the stale pre-RMW
    /// copy.
    fn persist_if_dirty(&mut self, pid: PageId) -> Result<()> {
        if let Some(Slot::Dirty(page)) = self.pages.get_mut(&pid) {
            page.seal();
            self.page_store.write_page(pid, page)?;
        }
        Ok(())
    }

    /// Copy-on-write: if `pid` has refcount 1, return `pid` unchanged.
    /// Otherwise allocate a fresh copy, decrement the original's rc,
    /// and bump each of the new copy's children's refcounts so the old
    /// tree is still internally consistent.
    ///
    /// For the shared-page case (rc > 1), rc mutations on the old
    /// page and its children go through
    /// [`PageStore::atomic_rc_delta_with_gen`] — disk-direct
    /// read-modify-write under a per-pid mutex, gated by
    /// `page.generation >= lsn` for WAL-replay idempotency. `lsn` is
    /// the current WAL op's LSN; same stamp protocol as
    /// [`crate::db::apply_drop_snapshot_pages`] and
    /// [`crate::db::apply_clone_volume_incref`]. Without the disk-direct
    /// path, two [`PagedL2p`](crate::paged::PagedL2p) instances sharing
    /// `pid` post-`clone_volume` would each read the same pre-decrement
    /// rc into their own `PageBuf` and lose one decrement when both
    /// flush back.
    pub fn cow_for_write(&mut self, pid: PageId, lsn: Lsn) -> Result<PageId> {
        debug_assert!(pid != NULL_PAGE, "cow_for_write called on NULL_PAGE");
        let pending = self.pending_rc.get(&pid).copied().unwrap_or(0);
        let (children, _page_type) = {
            let page = self.read(pid)?;
            let header = page.header()?;
            // Effective rc = disk rc + pending delta. The accumulator
            // holds rc deltas that haven't been flushed to disk yet
            // (batched for end-of-op commit); if an earlier cow in
            // this op bumped `pid` the disk read wouldn't see it,
            // so we fold the pending delta in before the sharedness
            // check.
            let effective_rc: i64 = i64::from(header.refcount) + i64::from(pending);
            // Early return only when genuinely unshared. `effective_rc<=1`
            // alone isn't enough under WAL replay: a crashed prior
            // attempt of THIS op may have durably decremented rc on
            // disk while leaving the manifest's pre-op root in place,
            // so the page is still referenced by a sibling volume.
            // The commit-time gen-stamp guard makes the re-run's rc
            // deltas idempotent, so we proceed with cow any time this
            // page's disk header already carries `lsn`.
            if effective_rc <= 1 && header.generation < lsn {
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
            (children, header.page_type)
        };

        // Allocate the clone and copy bytes. `page.generation = 0`: the
        // new pid is untouched by any WAL op, and any future rc mutation
        // on it will be stamped with the op's lsn at that point.
        let new_pid = self.page_store.allocate()?;
        let mut new_page = Page::zeroed();
        new_page
            .bytes_mut()
            .copy_from_slice(self.read(pid)?.bytes());
        new_page.set_generation(0);
        new_page.set_refcount(1);
        self.pages_insert(new_pid, Slot::Dirty(new_page));

        // Queue rc deltas for end-of-op commit instead of hitting disk
        // per-edge. Within one descent, a child is first incref'd here
        // (gaining the clone as a second parent) and later decref'd by
        // its own cow; those `+1` / `-1` entries net to zero in the
        // accumulator and never touch disk.
        for c in &children {
            *self.pending_rc.entry(*c).or_insert(0) += 1;
        }
        *self.pending_rc.entry(pid).or_insert(0) -= 1;

        Ok(new_pid)
    }

    /// Flush this op's accumulated rc deltas to disk, stamping each
    /// touched page with `lsn` for WAL-replay idempotency. Entries
    /// whose net delta is zero (e.g. a child that was both incref'd
    /// by a parent cow and decref'd by its own cow in the same op)
    /// are skipped without any disk write. Any Dirty in-memory copy
    /// of a committed page is invalidated so subsequent reads pull
    /// the post-commit bytes.
    ///
    /// Callers: [`crate::paged::PagedL2p`]'s write paths
    /// (`insert_with_lsn`, `delete_with_lsn`) invoke this immediately
    /// before `finish_op`.
    pub fn commit_rc_deltas(&mut self, lsn: Lsn) -> Result<()> {
        if self.pending_rc.is_empty() {
            return Ok(());
        }
        if self.rc_delta_lsn != lsn {
            self.rc_delta_lsn = lsn;
            self.rc_delta_ordinal = 0;
        }
        // Drain into a sorted Vec so commit order is deterministic —
        // reproduces cleanly across WAL replays and makes debugging
        // easier. Sort by pid.
        let mut entries: Vec<(PageId, i32)> = self.pending_rc.drain().collect();
        entries.sort_unstable_by_key(|(pid, _)| *pid);
        for (pid, delta) in entries {
            if delta == 0 {
                continue;
            }
            self.persist_if_dirty(pid)?;
            self.rc_delta_ordinal = self.rc_delta_ordinal.checked_add(1).ok_or_else(|| {
                MetaDbError::Corruption("paged: rc delta ordinal overflow".into())
            })?;
            self.page_store
                .atomic_rc_delta_with_gen(pid, delta, lsn, self.rc_delta_ordinal)?;
            self.pages_remove(pid);
            self.page_cache.invalidate(pid);
        }
        Ok(())
    }

    /// Forget any pending rc deltas without applying them. Used only
    /// on error paths — a successful op must call
    /// [`commit_rc_deltas`](Self::commit_rc_deltas).
    pub fn clear_rc_deltas(&mut self) {
        self.pending_rc.clear();
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
    ///
    /// Fast-path: when `clean_count == 0` (every entry is Dirty, the
    /// common case during write-heavy batches) the retain scan is
    /// skipped outright. Without this guard every `insert` / `delete`
    /// at the tree layer would walk the whole HashMap on every op
    /// just to find nothing to drop — which is the dominant cost of
    /// the apply phase once the tree has thousands of dirty pages.
    pub fn evict_clean_pages(&mut self) {
        if self.clean_count == 0 {
            return;
        }
        self.pages.retain(|_, slot| slot.is_dirty());
        self.clean_count = 0;
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
            self.pages_insert(pid, Slot::Clean(page));
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
        self.pages_insert(pid, Slot::Clean(page));
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
        let out = buf.atomic_decref(idx).unwrap();
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
        buf.flush().unwrap();
        buf.atomic_incref(idx).unwrap(); // rc = 2
        let out = buf.atomic_decref(idx).unwrap();
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
        buf.flush().unwrap();
        buf.atomic_incref(idx).unwrap(); // rc(idx) = 2, rc(leaf) = 1
        let new_idx = buf.cow_for_write(idx, 2).unwrap();
        // rc deltas are batched; apply them like the tree write path
        // would so the assertions below see the post-commit state.
        buf.commit_rc_deltas(2).unwrap();
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
