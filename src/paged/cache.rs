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

use std::sync::Arc;

use crate::cache::{DEFAULT_PAGE_CACHE_BYTES, PageCache};
use crate::error::{MetaDbError, Result};
use crate::page::{Page, PageType};
use crate::page_store::PageStore;
use crate::paged::format::{LEAF_BITMAP_BYTES, init_index, init_leaf, page_level};
use crate::paged::leaf_compact;
use crate::paged::read_view::{PageIdMap, PageIdSet, ReadOverlay, ReadOverlayShard};
use crate::types::{Lsn, NULL_PAGE, PageId};

const LOCAL_ALLOC_RUN_PAGES: usize = 256;

/// Cache entry. Both variants carry `Arc<Page>` so dirty pages can be
/// shared with `ReadView` overlays at apply-publish time without copying
/// 4 KiB. Mutation of a `Dirty` slot uses `Arc::make_mut` so an Arc
/// shared with an in-flight ReadView snapshot gets cloned-on-write —
/// the snapshot keeps its bytes, the live tree continues mutating.
enum Slot {
    Clean(Arc<Page>),
    Dirty(Arc<Page>),
}

pub(crate) struct DirtySnapshot {
    pages: Vec<DirtySnapshotPage>,
}

impl DirtySnapshot {
    pub(crate) fn pages_count(&self) -> usize {
        self.pages.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    pub(crate) fn seal(&self) -> Result<FlushedSnapshot> {
        let mut flushed = Vec::with_capacity(self.pages.len());
        for page in &self.pages {
            let mut sealed = (*page.original).clone();
            sealed.seal();
            flushed.push(FlushedSnapshotPage {
                pid: page.pid,
                original: page.original.clone(),
                sealed: Arc::new(sealed),
            });
        }
        Ok(FlushedSnapshot { pages: flushed })
    }
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

    fn sealed_page(&self, page_idx: usize) -> Option<&FlushedSnapshotPage> {
        self.pages.get(page_idx)
    }
}

struct DirtySnapshotPage {
    pid: PageId,
    original: Arc<Page>,
}

struct FlushedSnapshotPage {
    pid: PageId,
    original: Arc<Page>,
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

/// Private buffer of pages a `PagedL2p` is reading / mutating. Clean
/// pages come from the shared `PageCache`; dirty pages live here until
/// [`flush`](Self::flush).
pub struct PageBuf {
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    alloc_pool: Vec<PageId>,
    pages: PageIdMap<Slot>,
    read_overlay_shards: Vec<Arc<ReadOverlayShard>>,
    read_overlay_updates: PageIdSet,
    exclusive_read_overlay_mutation: bool,
    /// Live set of `Slot::Clean` entries in `pages`. This makes
    /// `evict_clean_pages` O(number of clean pages) instead of scanning
    /// the whole dirty overlay on every tree op.
    clean_pages: PageIdSet,
}

impl PageBuf {
    /// New standalone buffer: private page cache. Used by
    /// `PagedL2p::create`/`open` and unit tests.
    pub fn new(page_store: Arc<PageStore>) -> Self {
        let page_cache = Arc::new(PageCache::new(page_store.clone(), DEFAULT_PAGE_CACHE_BYTES));
        Self::with_cache(page_store, page_cache)
    }

    /// Buffer sharing an existing `PageCache`. The sole non-private ctor:
    /// both the Db path and standalone/test trees use this — per-L2P-page
    /// refcounting was deleted (ZFS port S3), so there is no longer a
    /// page-rc handle to thread.
    pub fn with_cache(page_store: Arc<PageStore>, page_cache: Arc<PageCache>) -> Self {
        Self {
            page_store,
            page_cache,
            alloc_pool: Vec::new(),
            pages: PageIdMap::default(),
            read_overlay_shards: ReadOverlay::empty_shards(),
            read_overlay_updates: PageIdSet::default(),
            exclusive_read_overlay_mutation: false,
            clean_pages: PageIdSet::default(),
        }
    }

    /// Insert a slot, keeping `clean_pages` consistent with the
    /// Clean/Dirty delta relative to any existing entry at `pid`.
    /// All mutation of `self.pages` must go through one of these
    /// helpers; direct `.insert` / `.remove` calls on `self.pages`
    /// will drift the clean-page index.
    fn pages_insert(&mut self, pid: PageId, slot: Slot) {
        let is_clean = matches!(slot, Slot::Clean(_));
        let is_dirty = matches!(slot, Slot::Dirty(_));
        let old = self.pages.insert(pid, slot);
        let was_clean = matches!(old, Some(Slot::Clean(_)));
        let was_dirty = matches!(old, Some(Slot::Dirty(_)));
        if was_clean {
            self.clean_pages.remove(&pid);
        }
        if is_clean {
            self.clean_pages.insert(pid);
        }
        if is_dirty || was_dirty {
            self.read_overlay_updates.insert(pid);
        }
    }

    /// Remove a slot, keeping `clean_pages` consistent.
    fn pages_remove(&mut self, pid: PageId) -> Option<Slot> {
        let old = self.pages.remove(&pid);
        if matches!(old, Some(Slot::Clean(_))) {
            self.clean_pages.remove(&pid);
        }
        if matches!(old, Some(Slot::Dirty(_))) {
            self.read_overlay_updates.insert(pid);
        }
        old
    }

    fn read_overlay_insert(&mut self, pid: PageId, page: Arc<Page>) {
        let idx = ReadOverlay::shard_idx(pid);
        Arc::make_mut(&mut self.read_overlay_shards[idx]).insert(pid, page);
    }

    fn read_overlay_remove(&mut self, pid: PageId) {
        let idx = ReadOverlay::shard_idx(pid);
        Arc::make_mut(&mut self.read_overlay_shards[idx]).remove(&pid);
    }

    pub(crate) fn set_exclusive_read_overlay_mutation(&mut self, enabled: bool) {
        self.exclusive_read_overlay_mutation = enabled;
    }

    fn detach_from_read_overlay_before_mutation(&mut self, pid: PageId) {
        if self.exclusive_read_overlay_mutation {
            self.read_overlay_remove(pid);
        }
    }

    pub(crate) fn flush_read_overlay_updates_budget(&mut self, max_updates: usize) -> usize {
        let updates: Vec<_> = self
            .read_overlay_updates
            .iter()
            .take(max_updates)
            .copied()
            .collect();
        let processed = updates.len();
        for pid in updates {
            self.read_overlay_updates.remove(&pid);
            match self.pages.get(&pid) {
                Some(Slot::Dirty(arc)) => self.read_overlay_insert(pid, arc.clone()),
                Some(Slot::Clean(_)) | None => self.read_overlay_remove(pid),
            }
        }
        processed
    }

    pub(crate) fn has_read_overlay_updates(&self) -> bool {
        !self.read_overlay_updates.is_empty()
    }

    pub(crate) fn flush_read_overlay_updates(&mut self) {
        while self.has_read_overlay_updates() {
            self.flush_read_overlay_updates_budget(usize::MAX);
        }
    }

    pub(crate) fn read_overlay(&self) -> ReadOverlay {
        ReadOverlay::from_shards(self.read_overlay_shards.clone())
    }

    fn allocate_local(&mut self) -> Result<PageId> {
        let pid = if let Some(pid) = self.alloc_pool.pop() {
            pid
        } else {
            self.alloc_pool = self.page_store.allocate_batch(LOCAL_ALLOC_RUN_PAGES)?;
            self.alloc_pool.pop().ok_or_else(|| {
                MetaDbError::Corruption("paged page allocator returned an empty batch".into())
            })?
        };
        // Page ids are recycled by the page store. A previous incarnation may
        // still be resident, and index pages may be pinned outside the LRU.
        // New allocations must therefore evict any shared-cache copy before
        // installing the freshly initialized dirty page in this PageBuf.
        self.pages_remove(pid);
        self.page_cache.invalidate(pid);
        Ok(pid)
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

    /// Read a page without mutating this per-tree buffer.
    ///
    /// This is used by the DB hot read path while holding only a shard
    /// read lock. Dirty pages already present in the tree buffer must
    /// win over the shared cache, because they may contain committed
    /// in-memory state that has not been checkpointed yet.
    pub fn with_page_read_only<T>(
        &self,
        pid: PageId,
        f: impl FnOnce(&Page) -> Result<T>,
    ) -> Result<T> {
        if let Some(slot) = self.pages.get(&pid) {
            return f(slot.page());
        }
        let page = self.page_cache.get(pid)?;
        f(&page)
    }

    /// Mutable page access. Loads the page if not cached and marks it
    /// dirty. **Does not stamp `page.generation`** — that field is
    /// reserved for WAL-apply idempotency markers
    /// ([`apply_drop_snapshot_pages`](crate::db)); tree-internal cow
    /// scratches should never overwrite it, or the gen-based
    /// `>= lsn` guard in those apply paths would spuriously fire.
    /// The `_generation` argument is kept for API continuity and to
    /// make call-site LSN-awareness visible, but is intentionally
    /// ignored.
    pub fn modify(&mut self, pid: PageId, _generation: Lsn) -> Result<&mut Page> {
        let arc: Arc<Page> = match self.pages_remove(pid) {
            Some(Slot::Dirty(arc)) => arc,
            Some(Slot::Clean(arc)) => {
                self.page_cache.invalidate(pid);
                arc
            }
            None => Arc::new(self.page_cache.get_for_modify(pid)?),
        };
        self.detach_from_read_overlay_before_mutation(pid);
        self.pages_insert(pid, Slot::Dirty(arc));
        // `Arc::make_mut` clones the page if a `ReadView` overlay
        // still holds this Arc — the published snapshot keeps the
        // pre-mutation bytes; the live tree mutates a fresh copy.
        match self.pages.get_mut(&pid).unwrap() {
            Slot::Dirty(arc) => Ok(Arc::make_mut(arc)),
            Slot::Clean(_) => unreachable!("modify always stores a dirty page"),
        }
    }

    /// Allocate a fresh page id and copy `src` into it as a private
    /// dirty page. The source page and its on-disk refcount are left
    /// untouched; the tree layer uses this for checkpoint shadowing.
    pub fn clone_private(&mut self, src: PageId, generation: Lsn) -> Result<PageId> {
        let new_pid = self.allocate_local()?;
        let mut page = self.read(src)?.clone();
        if matches!(page.header()?.page_type, PageType::PagedLeaf) {
            let payload = page.payload();
            let version = payload[LEAF_BITMAP_BYTES + 1];
            if version != leaf_compact::COMPACT_VERSION {
                return Err(MetaDbError::Corruption(format!(
                    "paged clone_private source leaf {src} -> {new_pid} lsn {generation}: compact leaf version {version} != {} (key_count={}, page_gen={}, birth={}, payload0={:02x?})",
                    leaf_compact::COMPACT_VERSION,
                    page.key_count(),
                    page.generation(),
                    page.birth_lsn(),
                    &payload[..32],
                )));
            }
            let unit_count = payload[LEAF_BITMAP_BYTES] as usize;
            let cap = leaf_compact::max_units_per_payload(payload.len())
                .min(leaf_compact::MAX_UNITS_PER_LEAF);
            if unit_count > cap {
                return Err(MetaDbError::Corruption(format!(
                    "paged clone_private source leaf {src} -> {new_pid} lsn {generation}: compact leaf unit_count {unit_count} exceeds payload capacity {cap} (key_count={}, page_gen={}, birth={}, payload0={:02x?})",
                    page.key_count(),
                    page.generation(),
                    page.birth_lsn(),
                    &payload[..32],
                )));
            }
        }
        page.set_generation(generation);
        // The clone is a NEW page version born at this COW lsn — stamp its
        // immutable birth-txg.
        page.set_birth_lsn(generation);
        self.pages_insert(new_pid, Slot::Dirty(Arc::new(page)));
        Ok(new_pid)
    }

    /// Allocate a fresh leaf, initialize it, cache as dirty. Stamps
    /// `page.generation = 0` so the WAL-apply idempotency guard treats
    /// newly-allocated tree pages as untouched by any WAL op; stamps the
    /// immutable `birth_lsn = generation` (the creating LSN of this page
    /// version, ZFS birth-txg analogue — read by the birth-shadow verify
    /// invariant, never by the hot path in Phase 1).
    pub fn alloc_leaf(&mut self, generation: Lsn) -> Result<PageId> {
        let pid = self.allocate_local()?;
        let mut page = Page::zeroed();
        init_leaf(&mut page, 0);
        page.set_birth_lsn(generation);
        self.pages_insert(pid, Slot::Dirty(Arc::new(page)));
        Ok(pid)
    }

    /// Allocate a fresh index page at `level`, initialize it (all children
    /// NULL_PAGE), cache as dirty. See [`alloc_leaf`](Self::alloc_leaf)
    /// for why the generation is stamped as 0 and birth_lsn as `generation`.
    pub fn alloc_index(&mut self, generation: Lsn, level: u8) -> Result<PageId> {
        let pid = self.allocate_local()?;
        let mut page = Page::zeroed();
        init_index(&mut page, 0, level);
        page.set_birth_lsn(generation);
        self.pages_insert(pid, Slot::Dirty(Arc::new(page)));
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
        self.read_overlay_shards = ReadOverlay::empty_shards();
        self.read_overlay_updates.clear();
        self.clean_pages.clear();
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

    /// Remove a page from this tree-local buffer because the caller is
    /// about to enqueue it for deferred free outside the tree lock.
    pub(crate) fn detach_for_free(&mut self, pid: PageId) {
        self.pages_remove(pid);
    }

    /// Copy-on-write: allocate a fresh copy of `pid`, leaving the
    /// original untouched (it is preserved for a snapshot). Returns the
    /// new pid. Per-L2P-page refcounting was deleted (ZFS port S3): the
    /// snapshot-pin decision is made by the caller
    /// ([`PagedL2p::cow_for_write`]) on the birth-txg, so this layer
    /// always copies and never reads/writes a page refcount.
    pub fn cow_for_write(&mut self, pid: PageId, lsn: Lsn) -> Result<PageId> {
        debug_assert!(pid != NULL_PAGE, "cow_for_write called on NULL_PAGE");
        {
            let page = self.read(pid)?;
            let header = page.header()?;
            // Validate the page type; non-paged types are corruption. (The
            // children are no longer needed — page-rc child increfs were
            // deleted with the per-page refcount.)
            match header.page_type {
                PageType::PagedIndex | PageType::PagedLeaf => {}
                other => {
                    return Err(MetaDbError::Corruption(format!(
                        "paged: cow_for_write on non-paged page type {other:?} at {pid}"
                    )));
                }
            }
        }

        // Allocate the clone and copy bytes. `page.generation = 0`: the
        // new pid is untouched by any WAL op.
        let new_pid = self.allocate_local()?;
        if new_pid == pid {
            return Err(MetaDbError::Corruption(format!(
                "paged: allocator returned live page {pid} for COW clone"
            )));
        }
        let mut new_page = Page::zeroed();
        new_page
            .bytes_mut()
            .copy_from_slice(self.read(pid)?.bytes());
        new_page.set_generation(0);
        // This clone is a NEW page version born at this COW lsn — stamp its
        // immutable birth-txg, overriding the copied source birth.
        new_page.set_birth_lsn(lsn);
        self.pages_insert(new_pid, Slot::Dirty(Arc::new(new_page)));

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
    ///
    /// Fast-path: when `clean_pages` is empty (every entry is Dirty, the
    /// common case during write-heavy batches) cleanup returns outright.
    /// When clean pages exist, remove only those tracked pids instead of
    /// scanning the whole dirty overlay on every tree op.
    pub fn evict_clean_pages_budget(&mut self, max_pages: usize) -> usize {
        if self.clean_pages.is_empty() {
            return 0;
        }
        let clean_pages: Vec<_> = self.clean_pages.iter().take(max_pages).copied().collect();
        let processed = clean_pages.len();
        for pid in clean_pages {
            self.clean_pages.remove(&pid);
            if matches!(self.pages.get(&pid), Some(Slot::Clean(_))) {
                self.pages.remove(&pid);
            }
        }
        processed
    }

    pub fn has_clean_pages(&self) -> bool {
        !self.clean_pages.is_empty()
    }

    pub fn evict_clean_pages(&mut self) {
        while self.has_clean_pages() {
            self.evict_clean_pages_budget(usize::MAX);
        }
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
            let Some(Slot::Dirty(arc)) = self.pages.get_mut(pid) else {
                unreachable!("dirty list mismatched pages content");
            };
            let page = Arc::make_mut(arc);
            page.seal();
            flushed.push((*pid, arc.clone()));
        }
        self.page_store.write_sealed_page_runs(flushed.clone())?;
        self.page_store.sync()?;
        for (pid, page) in flushed {
            // L2P index pages are tiny (≤1/256 of leaf bytes for a typical
            // tree) and every L2P walk dereferences them. Try to pin them
            // outside the LRU so heavy leaf / dedup_index churn cannot
            // evict the path. `pin` returns false when the budget is full,
            // which is fine — we fall back to the regular LRU insert and
            // rely on warmup_index_pages on the next reopen to top off.
            //
            // `warmup_index_pages` only runs at `open()`; for a fresh
            // `create()` the tree is empty and never gets retroactively
            // pinned, so without this on-demand path `pinned_pages` stays
            // at 0 forever. Soak (2026-04-27) showed that exact failure
            // mode: 1 GiB pin budget, 0 pages pinned, l2p_remap tail ramp
            // from 20 µs avg to 38 SECONDS as cache pressure ate the
            // hot index.
            let is_index = matches!(page.header().map(|h| h.page_type), Ok(PageType::PagedIndex));
            if is_index && self.page_cache.pin(pid, page.clone()) {
                // Pinned — skip LRU insert. The pinned table shadows LRU
                // on lookup, so a subsequent `insert` would only waste
                // capacity.
            } else {
                self.page_cache.insert(pid, page.clone());
            }
            self.pages_insert(pid, Slot::Clean(page));
        }
        Ok(())
    }

    pub(crate) fn dirty_snapshot(&self) -> DirtySnapshot {
        let mut pages: Vec<_> = self
            .pages
            .iter()
            .filter_map(|(pid, slot)| match slot {
                Slot::Dirty(arc) => Some(DirtySnapshotPage {
                    pid: *pid,
                    original: arc.clone(),
                }),
                Slot::Clean(_) => None,
            })
            .collect();
        pages.sort_unstable_by_key(|page| page.pid);
        DirtySnapshot { pages }
    }

    /// Streaming-writeback variant: gather at most `max` dirty pages
    /// (in ascending pid order so writeback writes coalesce into
    /// contiguous `IORING_OP_WRITEV` runs). Bounds the per-cycle work:
    /// caller can iterate, seal, write, and install in small batches
    /// so each `install_writeback` only holds `tree.write()` long
    /// enough for ~`max` pages, leaving room for foreground commit
    /// apply on the same shard.
    pub(crate) fn dirty_snapshot_capped(&self, max: usize) -> DirtySnapshot {
        if max == 0 {
            return DirtySnapshot { pages: Vec::new() };
        }
        let mut pages: Vec<_> = self
            .pages
            .iter()
            .filter_map(|(pid, slot)| match slot {
                Slot::Dirty(arc) => Some(DirtySnapshotPage {
                    pid: *pid,
                    original: arc.clone(),
                }),
                Slot::Clean(_) => None,
            })
            .collect();
        pages.sort_unstable_by_key(|page| page.pid);
        if pages.len() > max {
            pages.truncate(max);
        }
        DirtySnapshot { pages }
    }

    pub(crate) fn install_flushed_snapshot_page(
        &mut self,
        flushed: &FlushedSnapshot,
        page_idx: usize,
    ) -> Option<(PageId, bool)> {
        let page = flushed.sealed_page(page_idx)?;
        let Some(Slot::Dirty(current)) = self.pages.get(&page.pid) else {
            return Some((page.pid, true));
        };
        if !Arc::ptr_eq(current, &page.original) {
            return Some((page.pid, false));
        }
        // Db::flush has already written and synced the sealed page and
        // manifest install is now durable. Only at this point is it safe
        // to refresh PageCache: doing it during the IO phase would expose
        // future bytes to older ReadViews that still reference a recycled
        // pid with its previous page level.
        let is_index = matches!(
            page.sealed.header().map(|h| h.page_type),
            Ok(PageType::PagedIndex)
        );
        if is_index && self.page_cache.pin(page.pid, page.sealed.clone()) {
            // Pinned pages shadow LRU lookups.
        } else if is_index {
            self.page_cache.insert(page.pid, page.sealed.clone());
        } else {
            // Leaf pages are the hot write working set under Onyx's random
            // L2P load. Refresh them atomically too: this evicts any stale
            // recycled incarnation and keeps subsequent applies from falling
            // back to PageStore misses immediately after each checkpoint.
            self.page_cache
                .replace_or_insert(page.pid, page.sealed.clone());
        }
        self.pages_remove(page.pid);
        Some((page.pid, true))
    }

    pub fn iter_dirty(&self) -> impl Iterator<Item = (PageId, Arc<Page>)> + '_ {
        self.pages.iter().filter_map(|(pid, slot)| match slot {
            Slot::Dirty(arc) => Some((*pid, arc.clone())),
            Slot::Clean(_) => None,
        })
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

impl DirtySnapshot {
    pub(crate) fn write(&self) -> Result<FlushedSnapshot> {
        if self.pages.is_empty() {
            return Ok(FlushedSnapshot { pages: Vec::new() });
        }
        let flushed = self.seal()?;
        Ok(flushed)
    }
}

#[cfg(test)]
mod tests;
