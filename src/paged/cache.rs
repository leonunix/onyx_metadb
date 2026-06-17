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
use crate::l2p_page_rc::L2pPageRc;
use crate::page::{Page, PageType};
use crate::page_store::PageStore;
use crate::paged::format::{
    LEAF_BITMAP_BYTES, index_collect_children, init_index, init_leaf, page_level,
};
use crate::paged::leaf_compact;
use crate::paged::read_view::{PageIdMap, PageIdSet, ReadOverlay, ReadOverlayShard};
use crate::types::{Lsn, NULL_PAGE, PageId, Txg};

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
    alloc_pool: Vec<PageId>,
    pages: PageIdMap<Slot>,
    read_overlay_shards: Vec<Arc<ReadOverlayShard>>,
    read_overlay_updates: PageIdSet,
    exclusive_read_overlay_mutation: bool,
    /// Live set of `Slot::Clean` entries in `pages`. This makes
    /// `evict_clean_pages` O(number of clean pages) instead of scanning
    /// the whole dirty overlay on every tree op.
    clean_pages: PageIdSet,
    /// In-memory rc-delta accumulator for the current op.
    ///
    /// A3: [`cow_for_write`](Self::cow_for_write) (and `alloc_*` /
    /// `clone_private` / the clear-on-alloc reset) feed `(pid, delta)`
    /// entries here; a single end-of-op batch
    /// ([`commit_rc_deltas`](Self::commit_rc_deltas)) stages every
    /// non-zero net delta into the shared [`L2pPageRc`] array's current
    /// TXG ring slot. This design:
    ///
    /// - cancels the `+1` (parent cow's incref) against the matching
    ///   `-1` (child's own cow's decref) on pages the descent cow's
    ///   multiple times in the same op — they net to zero and never
    ///   reach the array;
    /// - keeps cross-tree consistency (the array is shared across every
    ///   L2P shard, so a sibling tree's next op sees the post-commit view);
    /// - preserves replay idempotency (the slot fold's `page_lsn >= lsn`
    ///   guard skips deltas a crashed prior attempt already landed).
    pending_rc: PageIdMap<i32>,
    /// Snapshot-scaling A3: the PageId-keyed page-refcount array. The
    /// Db path injects the one shared store (routing is by global
    /// `PageId`, so a COW in any L2P shard can touch any page-rc shard);
    /// standalone ctors build a private single-shard store. **A3
    /// plumbing landed — not yet authoritative**: header rc is still the
    /// source of truth; the cutover routes `effective_rc` / `cow_for_write`
    /// / `commit_rc_deltas` / snapshot+clone+drop rc through this.
    page_rc: Arc<L2pPageRc>,
    /// Current op's TXG. Set per mutation at the `PagedL2p` entry points
    /// (foreground apply uses the commit txg, the sync-cycle buffer drain
    /// uses the sync txg — see plan §0); consumed by the cutover's
    /// `commit_rc_deltas` / incref / decref `stage(txg, …)` calls.
    current_txg: Txg,
}

impl PageBuf {
    /// New standalone buffer: private page cache + private single-shard
    /// page-rc store. Used by `PagedL2p::create`/`open` and unit tests
    /// (the only callers of the non-`_rc` ctors — see
    /// [`with_cache`](Self::with_cache)).
    pub fn new(page_store: Arc<PageStore>) -> Self {
        let page_cache = Arc::new(PageCache::new(page_store.clone(), DEFAULT_PAGE_CACHE_BYTES));
        Self::with_cache(page_store, page_cache)
    }

    /// Standalone buffer sharing an existing `PageCache` but building its
    /// own private single-shard page-rc store. Kept at the original
    /// 2-arg signature so unit tests and the standalone `PagedL2p` ctors
    /// don't have to thread a page-rc handle; the Db path uses
    /// [`with_cache_rc`](Self::with_cache_rc) to inject the one shared
    /// store instead.
    pub fn with_cache(page_store: Arc<PageStore>, page_cache: Arc<PageCache>) -> Self {
        // Standalone: a private 1-shard page-rc store keyed off the same
        // page_store. `create` is infallible here in practice (it only
        // allocates one meta page); surface a panic on the impossible IO
        // error rather than poison every test ctor with a `Result`.
        let (page_rc, _roots) =
            L2pPageRc::create(page_store.clone(), page_cache.clone(), 1)
                .expect("standalone PageBuf: private page-rc create");
        Self::with_cache_rc(page_store, page_cache, Arc::new(page_rc))
    }

    /// Db-path buffer: shares both the `PageCache` AND the one global
    /// `L2pPageRc` store (page-rc routes by global `PageId`, so every L2P
    /// shard's COW must reach the same sharded store).
    pub fn with_cache_rc(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        page_rc: Arc<L2pPageRc>,
    ) -> Self {
        Self {
            page_store,
            page_cache,
            alloc_pool: Vec::new(),
            pages: PageIdMap::default(),
            read_overlay_shards: ReadOverlay::empty_shards(),
            read_overlay_updates: PageIdSet::default(),
            exclusive_read_overlay_mutation: false,
            clean_pages: PageIdSet::default(),
            pending_rc: PageIdMap::default(),
            page_rc,
            current_txg: 0,
        }
    }

    /// The page-rc store backing this buffer. Exposed for the cutover's
    /// read/free-decision paths and for `verify`.
    pub fn page_rc(&self) -> &Arc<L2pPageRc> {
        &self.page_rc
    }

    /// Set the current op's TXG. Called at every `PagedL2p` mutation
    /// entry point before the op stages page-rc deltas.
    pub fn set_current_txg(&mut self, txg: Txg) {
        self.current_txg = txg;
    }

    /// Current op's TXG (consumed by the cutover's `stage` calls).
    pub fn current_txg(&self) -> Txg {
        self.current_txg
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
        // A3 clear-on-alloc (CRC line): the page-rc array does NOT live in
        // the page, so freeing a page leaves a stale array rc behind. Reset
        // it to 0 HERE — the single point that hands out (possibly recycled)
        // pids — by queuing `-stale` into the op accumulator so it nets
        // against the fresh page's `+1` and commits under the op's lsn at
        // `commit_rc_deltas`. (Going through the accumulator, rather than a
        // direct stage, lets the reset ride the op's real lsn so the fold
        // applies it; a `lsn = 0` stage would be permanently skipped by the
        // fold's `page_generation >= last_lsn` guard.) This is the array
        // analogue of the old in-page header rc vanishing when the page was
        // returned to the free list. A fold-consistent read drives the
        // reset so a transient fold window can't leave the recycled page
        // mis-reading as shared. (A freed-but-not-yet-recycled page's stale
        // rc is harmless — verify and orphan-reclaim only look at pages
        // reachable from a live root.)
        let stale = self.page_rc.get_consistent(pid)?;
        if stale > 0 {
            *self.pending_rc.entry(pid).or_insert(0) -= stale as i32;
        }
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
    /// ([`apply_drop_snapshot_pages`](crate::db) /
    /// [`apply_clone_volume_incref`](crate::db)); tree-internal cow
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
                    "paged clone_private source leaf {src} -> {new_pid} lsn {generation}: compact leaf version {version} != {} (key_count={}, page_gen={}, rc={}, payload0={:02x?})",
                    leaf_compact::COMPACT_VERSION,
                    page.key_count(),
                    page.generation(),
                    page.refcount(),
                    &payload[..32],
                )));
            }
            let unit_count = payload[LEAF_BITMAP_BYTES] as usize;
            let cap = leaf_compact::max_units_per_payload(payload.len())
                .min(leaf_compact::MAX_UNITS_PER_LEAF);
            if unit_count > cap {
                return Err(MetaDbError::Corruption(format!(
                    "paged clone_private source leaf {src} -> {new_pid} lsn {generation}: compact leaf unit_count {unit_count} exceeds payload capacity {cap} (key_count={}, page_gen={}, rc={}, payload0={:02x?})",
                    page.key_count(),
                    page.generation(),
                    page.refcount(),
                    &payload[..32],
                )));
            }
        }
        page.set_generation(generation);
        // A3 cutover: page rc lives in the `L2pPageRc` array. Write the
        // reserved header byte 0 (format stability) and stage the clone's
        // +1 into the op accumulator instead of stamping the header.
        page.set_refcount(0);
        self.pages_insert(new_pid, Slot::Dirty(Arc::new(page)));
        *self.pending_rc.entry(new_pid).or_insert(0) += 1;
        Ok(new_pid)
    }

    /// Allocate a fresh leaf, initialize it, cache as dirty. Stamps
    /// `page.generation = 0` so the WAL-apply idempotency guard treats
    /// newly-allocated tree pages as untouched by any WAL op.
    pub fn alloc_leaf(&mut self, _generation: Lsn) -> Result<PageId> {
        let pid = self.allocate_local()?;
        let mut page = Page::zeroed();
        init_leaf(&mut page, 0);
        self.pages_insert(pid, Slot::Dirty(Arc::new(page)));
        // A3: a fresh page has one (incoming) reference, mirroring the old
        // in-page header rc of 1 from `PageHeader::new`. Stage that +1 into
        // the page-rc array via the op accumulator.
        *self.pending_rc.entry(pid).or_insert(0) += 1;
        Ok(pid)
    }

    /// Allocate a fresh index page at `level`, initialize it (all children
    /// NULL_PAGE), cache as dirty. See [`alloc_leaf`](Self::alloc_leaf)
    /// for why the generation is stamped as 0.
    pub fn alloc_index(&mut self, _generation: Lsn, level: u8) -> Result<PageId> {
        let pid = self.allocate_local()?;
        let mut page = Page::zeroed();
        init_index(&mut page, 0, level);
        self.pages_insert(pid, Slot::Dirty(Arc::new(page)));
        // A3: fresh page → one reference (mirrors header init rc 1). See
        // [`alloc_leaf`](Self::alloc_leaf).
        *self.pending_rc.entry(pid).or_insert(0) += 1;
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
        // A3 cutover: the page refcount lives in the PageId-keyed
        // `L2pPageRc` array now, not the page header. `get` sums the TXG
        // ring slots + array base, so within-txg and cross-txg visibility
        // matches the old immediate-header semantics. No page read needed.
        Ok(i64::from(self.page_rc.get(pid)?) + i64::from(pending))
    }

    /// Cross-tree-safe incref for the snapshot-take root pin.
    ///
    /// A3 cutover: page rc lives in the shared `L2pPageRc` array, so the
    /// incref is a single `stage_unskippable` into the current op's TXG
    /// slot — no disk-direct RMW, no page persist/evict (the page bytes
    /// are untouched). `stage_unskippable` merges with `lsn = 0` so this
    /// non-WAL incref neither carries a replay-skip stamp nor raises the
    /// array page generation (the faithful analogue of the old
    /// `atomic_rc_delta` no-gen RMW). Cross-tree safety comes from the
    /// shared array + the caller's serialisation (`apply_gate.write` /
    /// `drop_gate.write`). Returns the new rc.
    pub fn atomic_incref(&mut self, pid: PageId, lsn: Lsn) -> Result<u32> {
        let (_prev, new) = self
            .page_rc
            .stage_unskippable(self.current_txg, pid, 1, lsn)?;
        Ok(new)
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
        lsn: Lsn,
    ) -> Result<(DecrefOutcome, Vec<PageId>)> {
        // Snapshot children + page type before the RMW — if rc hits
        // zero we need them for cascade. Read via PageBuf so a Dirty
        // copy wins; `persist_if_dirty` then flushes it to disk so
        // the atomic RMW reads fresh bytes.
        let children = self.read_children_for_decref(pid)?;
        self.persist_if_dirty(pid)?;
        self.finish_decref_after_persist(pid, children, lsn)
    }

    /// Identical to [`atomic_decref_one`] but skips the
    /// `persist_if_dirty` step. Used by [`atomic_decref`]'s batched
    /// cascade after a single `persist_dirty_pages` covered every
    /// Dirty page the cascade plans to touch — replaces N synchronous
    /// `write_page` round-trips with one io_uring batch.
    fn atomic_decref_one_already_persisted(
        &mut self,
        pid: PageId,
        lsn: Lsn,
    ) -> Result<(DecrefOutcome, Vec<PageId>)> {
        let children = self.read_children_for_decref(pid)?;
        self.finish_decref_after_persist(pid, children, lsn)
    }

    fn read_children_for_decref(&mut self, pid: PageId) -> Result<Vec<PageId>> {
        let page = self.read(pid)?;
        let header = page.header()?;
        match header.page_type {
            PageType::PagedIndex => Ok(index_collect_children(page)),
            PageType::PagedLeaf => Ok(Vec::new()),
            other => Err(MetaDbError::Corruption(format!(
                "paged: atomic_decref on non-paged page type {other:?} at {pid}"
            ))),
        }
    }

    fn finish_decref_after_persist(
        &mut self,
        pid: PageId,
        children: Vec<PageId>,
        lsn: Lsn,
    ) -> Result<(DecrefOutcome, Vec<PageId>)> {
        // A3 cutover: decref the page-rc array (non-WAL drop cascade →
        // `stage_unskippable`, no replay-skip stamp). The page bytes are
        // untouched, so on `Decremented` there is no stale-byte copy to
        // evict (unlike the old disk-direct RMW). On a 1→0 transition
        // confirm with a fold-consistent read before the IRREVERSIBLE
        // free (R2): the cheap staged `new` can straddle a concurrent
        // fold's publish/clear window and floor a live rc to a spurious 0.
        let (prev, new) = self
            .page_rc
            .stage_unskippable(self.current_txg, pid, -1, lsn)?;
        if new == 0 && prev > 0 && self.page_rc.get_consistent(pid)? == 0 {
            self.pages_remove(pid);
            self.page_cache.invalidate(pid);
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
    /// an explicit worklist; every rc mutation routes through the
    /// disk-direct RMW path.
    ///
    /// Within each outer-loop iteration we first pre-flush every
    /// currently-worklist-resident Dirty page in **one** batched
    /// `write_sealed_page_runs` call, then drain the worklist with the
    /// already-persisted helper. New children whose disk bytes are
    /// not affected by the parent's RMW don't need to be re-persisted
    /// — children's RMWs read each child's own pid, which the parent
    /// never wrote. The batch loop re-acquires the outer iteration
    /// only when a popped child is itself Dirty in the local buf,
    /// keeping the read-after-write invariant the original per-step
    /// `persist_if_dirty` enforced.
    pub fn atomic_decref(&mut self, pid: PageId, lsn: Lsn) -> Result<DecrefOutcome> {
        let mut top: Option<DecrefOutcome> = None;
        let mut worklist: Vec<PageId> = vec![pid];
        while !worklist.is_empty() {
            // Single batched persist covering every Dirty pid currently
            // on the worklist. Replaces N synchronous per-step writes.
            let dirty: Vec<PageId> = worklist
                .iter()
                .copied()
                .filter(|p| matches!(self.pages.get(p), Some(Slot::Dirty(_))))
                .collect();
            if !dirty.is_empty() {
                self.persist_dirty_pages(&dirty)?;
            }
            while let Some(p) = worklist.pop() {
                let (outcome, children) = self.atomic_decref_one_already_persisted(p, lsn)?;
                if top.is_none() {
                    top = Some(outcome);
                }
                let has_dirty_child = children
                    .iter()
                    .any(|c| matches!(self.pages.get(c), Some(Slot::Dirty(_))));
                worklist.extend(children);
                if has_dirty_child {
                    // New Dirty pages joined the worklist; break out so
                    // the outer loop rebatch-persists them before their
                    // RMW reads disk.
                    break;
                }
            }
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
        self.detach_from_read_overlay_before_mutation(pid);
        if let Some(Slot::Dirty(arc)) = self.pages.get_mut(&pid) {
            let page = Arc::make_mut(arc);
            page.seal();
            self.page_store.write_page(pid, page)?;
        }
        Ok(())
    }

    fn persist_dirty_pages(&mut self, pids: &[PageId]) -> Result<()> {
        let mut sealed_pages = Vec::new();
        for &pid in pids {
            self.detach_from_read_overlay_before_mutation(pid);
            if let Some(Slot::Dirty(arc)) = self.pages.get_mut(&pid) {
                let page = Arc::make_mut(arc);
                page.seal();
                sealed_pages.push((pid, arc.clone()));
            }
        }
        self.page_store.write_sealed_page_runs(sealed_pages)
    }

    /// Copy-on-write: if `pid` has refcount 1, return `pid` unchanged.
    /// Otherwise allocate a fresh copy, decrement the original's rc,
    /// and bump each of the new copy's children's refcounts so the old
    /// tree is still internally consistent.
    ///
    /// A3: rc deltas (old page `-1`, children `+1`, the clone `+1`) are
    /// queued in `pending_rc` and staged into the shared [`L2pPageRc`]
    /// array at [`commit_rc_deltas`](Self::commit_rc_deltas); the array's
    /// `page_lsn >= lsn` slot-fold guard gives WAL-replay idempotency.
    /// Cross-tree consistency comes from the array being shared across
    /// every L2P shard — two [`PagedL2p`](crate::paged::PagedL2p)
    /// instances sharing `pid` post-`clone_volume` read the same array
    /// entry, not two private header copies. `generation` (still read
    /// from the page header) drives the `effective_rc <= 1 &&
    /// generation < lsn` early-return only.
    pub fn cow_for_write(&mut self, pid: PageId, lsn: Lsn) -> Result<PageId> {
        debug_assert!(pid != NULL_PAGE, "cow_for_write called on NULL_PAGE");
        let pending = self.pending_rc.get(&pid).copied().unwrap_or(0);
        // A3 cutover: page rc lives in the `L2pPageRc` array, read here
        // before the `self.read(pid)` borrow. `get` sums the TXG ring
        // slots + array base.
        let array_rc = self.page_rc.get(pid)?;
        let (children, _page_type) = {
            let page = self.read(pid)?;
            let header = page.header()?;
            // Effective rc = array rc + pending delta. The accumulator
            // holds rc deltas that haven't been staged to the array yet
            // (batched for end-of-op commit); if an earlier cow in
            // this op bumped `pid` the array read wouldn't see it,
            // so we fold the pending delta in before the sharedness
            // check.
            let effective_rc: i64 = i64::from(array_rc) + i64::from(pending);
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
        // A3 cutover: write the reserved header rc byte 0 (the source's
        // copied byte is meaningless now); the clone's rc is staged to
        // the array via the `pending_rc` accumulator below.
        new_page.set_refcount(0);
        self.pages_insert(new_pid, Slot::Dirty(Arc::new(new_page)));

        // Queue rc deltas for end-of-op commit instead of staging the
        // array per-edge. Within one descent, a child is first incref'd
        // here (gaining the clone as a second parent) and later decref'd
        // by its own cow; those `+1` / `-1` entries net to zero in the
        // accumulator and never reach the array. The clone gets +1
        // (was `set_refcount(1)`); the original loses the live-tree edge
        // that moved to the clone (-1).
        for c in &children {
            *self.pending_rc.entry(*c).or_insert(0) += 1;
        }
        *self.pending_rc.entry(new_pid).or_insert(0) += 1;
        *self.pending_rc.entry(pid).or_insert(0) -= 1;

        Ok(new_pid)
    }

    /// A3: stage this op's accumulated rc deltas into the `L2pPageRc`
    /// array's current TXG slot, stamping each with `lsn`. Net-zero
    /// entries (a child both incref'd by a parent cow and decref'd by its
    /// own cow in the same op) are skipped. The slot merge sums repeated
    /// deltas, so the old same-lsn/same-pid `ordinal` disambiguation is
    /// gone (one net delta per pid per op). No page persist/evict — the
    /// page bytes are untouched by an array stage, so the COW'd pages stay
    /// dirty in the buffer and flush at the next checkpoint. Replay
    /// idempotency rides `stage`'s `page_lsn >= lsn` skip.
    ///
    /// Callers: [`crate::paged::PagedL2p`]'s write paths
    /// (`insert_with_lsn`, `delete_with_lsn`) invoke this immediately
    /// before `finish_op`.
    pub fn commit_rc_deltas(&mut self, lsn: Lsn) -> Result<()> {
        self.commit_pending_rc(lsn, true)
    }

    /// Like [`commit_rc_deltas`] but WITHOUT the per-op replay-skip, for
    /// non-WAL structural stages — currently the tree-create root's `+1`,
    /// where committing through [`commit_rc_deltas`] at `lsn = 0` would be
    /// swallowed by `stage`'s `page_lsn(fresh) == 0 >= 0` skip. A
    /// re-created tree on recovery re-allocates a fresh root pid and
    /// re-stages, so no replay idempotency is needed here.
    pub fn commit_rc_deltas_unskippable(&mut self, lsn: Lsn) -> Result<()> {
        self.commit_pending_rc(lsn, false)
    }

    /// Shared body: drain `pending_rc` into a pid-sorted vec (deterministic
    /// stage order across replays) and stage each non-zero net delta. `wal`
    /// selects [`L2pPageRc::stage`] (replay-skip) vs
    /// [`L2pPageRc::stage_unskippable`].
    fn commit_pending_rc(&mut self, lsn: Lsn, wal: bool) -> Result<()> {
        if self.pending_rc.is_empty() {
            return Ok(());
        }
        let mut entries: Vec<(PageId, i32)> = self.pending_rc.drain().collect();
        entries.sort_unstable_by_key(|(pid, _)| *pid);
        let txg = self.current_txg;
        for (pid, delta) in entries {
            if delta == 0 {
                continue;
            }
            if wal {
                self.page_rc.stage(txg, pid, i64::from(delta), lsn)?;
            } else {
                self.page_rc
                    .stage_unskippable(txg, pid, i64::from(delta), lsn)?;
            }
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
