//! `PagedL2p`: radix-tree L2P index over a [`PageStore`], one per shard.
//!
//! API parity with `btree::BTree` (get/insert/delete/range/flush plus
//! snapshot helpers) so `Db` can swap the implementation in place. See
//! [`format`](super::format) for the on-disk layout and addressing
//! scheme.
//!
//! # Refcount + CoW model
//!
//! Identical semantics to the B+tree: every page carries a refcount,
//! snapshot take bumps the root's rc, any write path `cow_for_write`s
//! each page it touches. When a delete empties a leaf (or empties an
//! index after an upward cleanup), the emptied page is freed and the
//! parent's slot is nulled out. Root is never freed — its level stays
//! pinned for the lifetime of the tree.

use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use crate::cache::{DEFAULT_PAGE_CACHE_BYTES, PageCache};
use crate::error::{MetaDbError, Result};
use crate::page_store::PageStore;
use crate::paged::cache::{DirtySnapshot, FlushedSnapshot, PageBuf};
use crate::paged::format::{
    INDEX_FANOUT, INDEX_SHIFT, L2pValue, LEAF_ENTRY_COUNT, LEAF_MASK, LEAF_SHIFT, MAX_INDEX_LEVEL,
    index_child_at, index_child_count, index_set_child, leaf_bit_set, leaf_clear, leaf_entry_count,
    leaf_set, leaf_value_at, max_leaf_idx_at_level, page_level, slot_in_index,
};
use crate::paged::read_view::PageIdSet;
use crate::types::{Lsn, NULL_PAGE, PageId};

mod helpers;
mod types;

use helpers::*;
use types::OwnedRange;
pub use types::{DiffEntry, PagedRangeIter, WarmupStats};

/// BFG: process-global count of non-clone COW-kill
/// decisions where the birth-LSN said "recycle" (page NOT snapshot-pinned) but
/// the retained page-rc still read `effective_rc > 1` ("shared"). Under a
/// correct `birth_lsn` stamp (enforced HARD by the offline
/// `verify::check_birth_shadow`) the page is genuinely unpinned and the page-rc
/// read is a force-fold-transient / stale artifact — birth is authoritative.
/// This counter + a `warn!` are the page-rc soft-warn inverted shadow: hot-path
/// observability only, NEVER a HARD assert (page-rc is force-fold-prone; a HARD
/// assert would crash mid-soak). Read by tests / soak to confirm the flip's
/// divergence profile.
pub static BIRTH_SHADOW_DANGEROUS_DIVERGENCES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// One paged L2P index tree. Not `Send` across threads without external
/// synchronisation — `Db` wraps it in `Mutex`.
pub struct PagedL2p {
    buf: PageBuf,
    root: PageId,
    root_level: u8,
    next_gen: Lsn,
    private_pages: PageIdSet,
    retired_pages: PageIdSet,
    checkpoint_protected: PageIdSet,
    /// Page-deadlist witness: L2P pages displaced off the head by a shared COW
    /// in this op. `rc > 1` means the old page is still pinned by a snapshot, so
    /// it died off the head but survives for the snapshot. Drained per op by the
    /// apply / fold layer via [`take_cow_displaced`](Self::take_cow_displaced);
    /// cleared on the op error path.
    cow_displaced: Vec<crate::deadlist::DeadRecord>,
    /// BFG sticky per-clone capture threshold. `Some(B)` =
    /// this shard belongs to a clone branched at `B = branched_at_lsn`, so
    /// COW/alloc/free of its clone-private pages (`birth > B`) emit
    /// `LiveRecord`s into `live_events`; `None` = non-clone, zero overhead.
    /// Set at clone build / reopen and **never cleared** (survives promotion
    /// — promoted ex-clones keep recording, which needs).
    clone_birth_lsn: Option<Lsn>,
    /// BFG page-livelist witness: ALLOC/FREE events for this
    /// clone's clone-private L2P pages, accumulated per op and drained by
    /// the apply / fold layer via [`take_live_events`](Self::take_live_events);
    /// cleared on the op error path (mirrors `cow_displaced`). Empty unless
    /// `clone_birth_lsn` is set.
    live_events: Vec<crate::livelist::LiveRecord>,
    /// BFG: the `capture_watermark`s of the live
    /// snapshots of the NON-CLONE volume owning this shard, SORTED ascending.
    /// The birth-authoritative COW-kill decision in
    /// [`cow_for_write`](Self::cow_for_write) treats a page COW'd (dying) at lsn
    /// `D` as snapshot-pinned iff some snapshot captured it, i.e.
    /// `birth_lsn(P) <= youngest_snap_below(D)` where `youngest_snap_below(D) =
    /// max{wm : wm < D}` (snapshots with `wm >= D` were captured AFTER the page
    /// died, so they cannot reference it — critical when a death is folded LATE,
    /// e.g. by `force_compact`, after newer snapshots exist). Set per-op by the
    /// apply / fold layer (mirrors [`set_current_bfg`]) from the durable
    /// `SnapshotEntry::capture_watermark`s so the decision is identical live and
    /// on replay. For clones, `snapshot_wms` stays C's OWN snapshots — it gates
    /// the page-deadlist drain classification (`drain_page_deaths_into`), NOT the
    /// clone COW-kill (that reads `clone_cow_pinners`). Defaults to empty.
    snapshot_wms: Vec<Lsn>,
    /// BFG: the page-rc-INDEPENDENT pinner-LSN set for
    /// the CLONE COW-kill, SORTED ascending. Only meaningful when
    /// `clone_birth_lsn.is_some()`. The set is `{B_C} ∪ {capture_watermark(S) : S
    /// a live snapshot of C} ∪ {branched_at_lsn(V) : V another clone-lineage
    /// volume, branched_at_lsn(V) > B_C}` (built by `Db::clone_cow_pinners`). A
    /// clone page P dying at `D` is snapshot-pinned iff some pinner `p` has
    /// `birth_lsn(P) <= p < D` — i.e. `birth_lsn(P) <= youngest_clone_pinner_below(D)`.
    /// Kept SEPARATE from `snapshot_wms` so widening it for the COW-kill does not
    /// perturb the deadlist drain (which must stay C-own-snapshots). Set per-op by
    /// the apply / fold layer alongside `set_snapshot_wms`; empty on replay
    /// (durable pages are `checkpoint_protected`, so the term must not fire) and
    /// for non-clones. The `effective_rc > 1` page-rc floor is kept ALONGSIDE this
    /// term until page-rc removal (inverted shadow), so the term can only ADD preservation.
    clone_cow_pinners: Vec<Lsn>,
}

pub(crate) struct Checkpoint {
    pub(crate) root: PageId,
    dirty: DirtySnapshot,
    private_pages: PageIdSet,
    retired_pages: PageIdSet,
}

impl Checkpoint {
    pub(crate) fn write_dirty_pages(&self) -> Result<FlushedSnapshot> {
        let flushed = self.dirty.write()?;
        Ok(flushed)
    }

    /// Sample-phase dirty page count for this shard — number of pages
    /// the in-gate `begin_checkpoint` snapshotted to be written out by
    /// the post-gate IO phase. Drives the
    /// `flush_sample_l2p_dirty_pages` metric.
    pub(crate) fn dirty_pages_count(&self) -> usize {
        self.dirty.pages_count()
    }

    pub(crate) fn private_pages(&self) -> Vec<PageId> {
        let mut pages: Vec<_> = self.private_pages.iter().copied().collect();
        pages.sort_unstable();
        pages
    }

    pub(crate) fn retired_pages(&self) -> Vec<PageId> {
        let mut pages: Vec<_> = self.retired_pages.iter().copied().collect();
        pages.sort_unstable();
        pages
    }
}

impl PagedL2p {
    fn finish_op<T>(&mut self, result: Result<T>) -> Result<T> {
        self.buf.flush_read_overlay_updates();
        self.buf.evict_clean_pages();
        result
    }

    pub(crate) fn finish_batch_apply(&mut self) -> Result<()> {
        self.finish_op(Ok(()))
    }

    pub(crate) fn set_exclusive_read_overlay_mutation(&mut self, enabled: bool) {
        self.buf.set_exclusive_read_overlay_mutation(enabled);
    }

    /// Fresh empty tree. Allocates one leaf as the root, level 0.
    /// Standalone/test trees birth the root at lsn 1.
    pub fn create(page_store: Arc<PageStore>) -> Result<Self> {
        let page_cache = Arc::new(PageCache::new(page_store.clone(), DEFAULT_PAGE_CACHE_BYTES));
        Self::create_with_cache(page_store, page_cache, 1)
    }

    /// Create a fresh empty tree sharing an existing `PageCache`. Births
    /// the empty root at `root_lsn` (the volume's `created_lsn`). Birthing
    /// the page header at the volume's `created_lsn` keeps the "a
    /// snapshot's created_lsn >= birth_lsn of every page it captures"
    /// invariant the page-deadlist recording filter relies on
    /// (`drain_page_deaths_into`: record a page death iff
    /// `birth <= youngest_snapshot.created`). The bootstrap volume has
    /// `created_lsn == 0`, so birthing its roots at lsn 1 would put them
    /// above any snapshot taken before the first op (`created_lsn == 0`),
    /// dropping the death record at the snapshot's drop. (Per-L2P-page
    /// refcounting was deleted, BFG.)
    pub fn create_with_cache(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        root_lsn: Lsn,
    ) -> Result<Self> {
        let mut buf = PageBuf::with_cache(page_store, page_cache);
        let root = buf.alloc_leaf(root_lsn)?;
        buf.flush()?;
        Ok(Self {
            buf,
            root,
            root_level: 0,
            // Keep the generation watermark strictly above the root's
            // birth so later stamps stay monotonic.
            next_gen: root_lsn + 1,
            private_pages: PageIdSet::default(),
            retired_pages: PageIdSet::default(),
            checkpoint_protected: PageIdSet::default(),
            cow_displaced: Vec::new(),
            clone_birth_lsn: None,
            live_events: Vec::new(),
            snapshot_wms: Vec::new(),
            clone_cow_pinners: Vec::new(),
        })
    }

    /// Re-attach to an existing tree whose root is at `root`. Derives
    /// `root_level` by reading the root page's type header.
    pub fn open(page_store: Arc<PageStore>, root: PageId, next_gen: Lsn) -> Result<Self> {
        let page_cache = Arc::new(PageCache::new(page_store.clone(), DEFAULT_PAGE_CACHE_BYTES));
        Self::open_with_cache(page_store, page_cache, root, next_gen)
    }

    pub fn open_with_cache(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        root: PageId,
        next_gen: Lsn,
    ) -> Result<Self> {
        let mut buf = PageBuf::with_cache(page_store, page_cache);
        let root_level = buf.read_level(root)?;
        if root_level > MAX_INDEX_LEVEL {
            return Err(MetaDbError::Corruption(format!(
                "paged: root {root} has level {root_level} exceeding max {MAX_INDEX_LEVEL}"
            )));
        }
        Ok(Self {
            buf,
            root,
            root_level,
            next_gen,
            private_pages: PageIdSet::default(),
            retired_pages: PageIdSet::default(),
            checkpoint_protected: PageIdSet::default(),
            cow_displaced: Vec::new(),
            clone_birth_lsn: None,
            live_events: Vec::new(),
            snapshot_wms: Vec::new(),
            clone_cow_pinners: Vec::new(),
        })
    }

    /// Current root page id.
    pub fn root(&self) -> PageId {
        self.root
    }

    /// Current root level (0 = leaf, 1..=MAX_INDEX_LEVEL = index).
    pub fn root_level(&self) -> u8 {
        self.root_level
    }

    /// Next unused generation / LSN stamp. Exposed so the `Db`
    /// aggregate can compute the max generation across shards for
    /// manifest commits.
    pub fn next_generation(&self) -> Lsn {
        self.next_gen
    }

    /// `birth_lsn` of the current root page = the highest lsn folded into this
    /// shard's tree (the root is re-COW'd with the op's lsn on every fold), i.e.
    /// the exact fold-watermark of the captured root. The snapshot capture uses
    /// `max` over the shard roots as the durable `SnapshotEntry::capture_watermark`
    /// (the birth COW-kill oracle's operand — NOT `last_applied_lsn`, which races
    /// ahead of the fold under concurrency). `NULL_PAGE` root (empty shard) → 0.
    /// Read-only (`buf.read` needs `&mut self` for cache bookkeeping only).
    pub(crate) fn root_birth_lsn(&mut self) -> Result<Lsn> {
        if self.root == NULL_PAGE {
            return Ok(0);
        }
        Ok(self.buf.read(self.root)?.birth_lsn())
    }

    /// Bump `next_gen` if the caller's LSN watermark has advanced past
    /// it. Called from `Db` when a commit's LSN exceeds the tree's
    /// counter so subsequent page stamps stay monotonic.
    pub fn advance_next_gen(&mut self, lsn: Lsn) {
        if lsn >= self.next_gen {
            self.next_gen = lsn + 1;
        }
    }

    /// No-op retained for call-site stability. Per-L2P-page refcounting was
    /// deleted (BFG), so the op's BFG no longer threads down to a
    /// page-rc ring slot. The argument is ignored.
    pub fn set_current_bfg(&mut self, _bfg: crate::types::Bfg) {}

    /// Underlying page store handle (shared with `Db` for free-list
    /// inspection, etc.).
    pub fn page_store(&self) -> &Arc<PageStore> {
        self.buf.page_store()
    }

    pub fn page_cache(&self) -> &Arc<crate::cache::PageCache> {
        self.buf.page_cache()
    }

    /// Diagnostic snapshot of in-memory bookkeeping sizes per shard.
    /// Cheap (`HashSet/HashMap::len`); intended for OOM triage / soak
    /// dashboards. Returns (private_pages, retired_pages, pagebuf_total,
    /// pagebuf_dirty).
    pub fn growth_summary(&self) -> (usize, usize, usize, usize) {
        (
            self.private_pages.len(),
            self.retired_pages.len(),
            self.buf.len(),
            self.buf.dirty_count(),
        )
    }

    /// Capture a `ReadView` snapshot of the current tree state plus
    /// every still-dirty page Arc. Apply calls this before dropping
    /// `shard.tree.write()` so lock-free readers see post-apply state
    /// — the overlay must include pages COW'd by this apply because
    /// `PagedL2p::flush` (which is what makes them visible via
    /// `page_cache`) only runs every 50 ms from onyx's
    /// `durability-watermark`.
    pub fn snapshot_read_view(&self) -> super::ReadView {
        super::ReadView::new(
            self.root,
            self.root_level,
            self.buf.read_overlay(),
            self.buf.page_cache().clone(),
        )
    }

    fn alloc_leaf_private(&mut self, generation: Lsn) -> Result<PageId> {
        let pid = self.buf.alloc_leaf(generation)?;
        self.private_pages.insert(pid);
        // v19 livelist: a fresh clone-private leaf is born at `generation`.
        self.push_live_alloc(pid, generation);
        Ok(pid)
    }

    fn alloc_index_private(&mut self, generation: Lsn, level: u8) -> Result<PageId> {
        let pid = self.buf.alloc_index(generation, level)?;
        self.private_pages.insert(pid);
        // v19 livelist: a fresh clone-private index page is born at `generation`.
        self.push_live_alloc(pid, generation);
        Ok(pid)
    }

    fn cow_for_write(&mut self, pid: PageId, lsn: Lsn) -> Result<PageId> {
        // Read the page's immutable `birth_lsn` ONCE, only when needed: clones
        // need it for the livelist FREE classification; non-clones with a live
        // snapshot need it for the snapshot watermark birth COW-kill decision; and a non-clone
        // CLONE SOURCE (page-rc removal: `clone_cow_pinners` non-empty even with no own
        // snapshot — see `clone_cow_pinners_from`) needs it so the clone-source
        // pin compares the REAL birth, not 0. Omitting `clone_cow_pinners` here
        // would read `birth = 0`, making `0 <= youngest_clone_pinner` ALWAYS
        // true (over-COW every page) AND stamping `birth_lsn = 0` into the
        // `cow_displaced` deadlist record → a later non-clone deadlist drop
        // mis-classifies it (`dl_record_freed`) = premature free / completeness
        // hole. A truly independent non-clone with no snapshots and no clones in
        // the system keeps the read-free hot path (`clone_cow_pinners` empty).
        let need_birth = self.clone_birth_lsn.is_some()
            || !self.snapshot_wms.is_empty()
            || !self.clone_cow_pinners.is_empty();
        let birth = if need_birth {
            self.buf.read(pid)?.birth_lsn()
        } else {
            0
        };
        let old_birth = if self.clone_birth_lsn.is_some() {
            Some(birth)
        } else {
            None
        };

        // BFG: the COW-kill decision. A page is
        // "snapshot-pinned" — its old version must be PRESERVED on overwrite
        // (it "dies off the head" and stays alive for a snapshot) — vs PRIVATE
        // to the live volume (recyclable in place).
        //   * CLONE (`clone_birth_lsn.is_some()`): clone COW-kill — page-rc-INDEPENDENT
        //     pinner-LSN operand, `birth <= youngest_clone_pinner_below(lsn)`.
        //     The pinner set
        //     (`clone_cow_pinners`, fed by `Db::clone_cow_pinners`) is
        //     `{B_C} ∪ {C's live snapshot capture_watermarks} ∪ {descendant
        //     branch points}` — the page-rc-independent completion of "P is
        //     shared with a survivor". The `{B_C}` term pins origin pages
        //     (`birth <= B_C`, shared with the parent); the descendant branch
        //     points pin born>B_C pages a survivor clone (incl. a PROMOTED
        //     ex-clone whose `parent_vol_ord` is cleared) still references after
        //     C's own snapshot is dropped — the descendant-share premature-free case a pure-birth
        //     `max(B_C, youngest_snap(C))` operand misses (`clone_birth_shadow`).
        //     page-rc removal dropped the legacy `effective_rc > 1` floor (page-rc deleted):
        //     the pinner-set term is now the sole, authoritative clone operand.
        //     Benign origin-fallthrough over-COW: an origin page that
        //     became C-exclusive (`birth <= B_C`, rc==1) is preserved rather than
        //     recycled — correctness-safe, reclaimed by orphan-reclaim; the
        //     shadow does NOT flag this direction. NO drain perturbation: the
        //     deadlist classification keeps reading `snapshot_wms` (C's own
        //     snapshots), not this wider set.
        //   * NON-CLONE: birth-authoritative. A page COW'd (dying) at `lsn` is
        //     snapshot-pinned iff some snapshot captured it, i.e.
        //     `birth_lsn(P) <= youngest_snap_below(lsn)` where
        //     `youngest_snap_below(lsn) = max{wm : wm < lsn}` over the live
        //     snapshots' `capture_watermark`s. Filtering by `wm < lsn` is
        //     LOAD-BEARING: a snapshot with `wm >= lsn` was CAPTURED AFTER this
        //     page died (its roots fold past the page's death), so it cannot
        //     reference the old page — including it over-pins a HEAD-only
        //     transient whose death is folded LATE (e.g. `force_compact` after
        //     newer snapshots exist), leaking a deadlist orphan the drop shadow
        //     rejects. `None` (no snapshot with wm<lsn) = never pinned; `wm==0`
        //     is a real genesis snapshot.
        //
        //     `!private_pages.contains(pid)` is LOAD-BEARING and the birth-model
        //     equal of legacy `effective_rc > 1` for the lazy-incref case: a page
        //     PRIVATE to the current (uncommitted) batch was COW'd/alloc'd this
        //     cycle, so no committed snapshot can reference it — a snapshot only
        //     ever captures the committed root. Without this gate an intra-fold
        //     INTERMEDIATE page trips a false pin: when one fold cycle folds two
        //     leaf ops that share an index page, op#1 COWs the index → a private
        //     pid stamped `birth = op#1.inserts_max_lsn` (an OLD write lsn), then
        //     op#2 re-COWs it. That intermediate pid is born+retired entirely
        //     within a fold cycle LATER than the youngest snapshot's capture, so
        //     it is reachable from NO snapshot — yet its stale `birth <= youngest`
        //     would send it to the SHARED (preserve+deadlist) arm instead of
        //     RECYCLE, leaking an orphan the drop-time page-deadlist later tries
        //     to free (the premature-free the structural shadow rejects). page-rc
        //     never hit this: a private page had `effective_rc == 1`.
        // page-rc removal COW-kill (non-clone): the birth-pin `birth <= youngest_snap_below(lsn)
        // && !private` is the SOLE operand — the legacy `effective_rc > 1` page-rc
        // floor is DELETED. The birth term equals the old `rc > 1` pinning on
        // every reachable state: a snapshot `S` captures page `P` iff
        // `birth(P) <= capture_watermark(S)` (= max of S's captured root births,
        // sampled under `tree.write()` after the take-snapshot quiesce), and the
        // per-op `snapshot_wms` is fetched FRESH under `apply_gate`/`drop_gate`
        // each op — warmed by `finish_pending_snapshots` before any op that could
        // fold a death past S's watermark runs — so the cache cannot lag a
        // snapshot able to pin a not-yet-folded death (the only under-pin the
        // floor ever guarded). The `!private` gate above already excludes the
        // intra-batch transients the old `rc == 1` exclusion covered. The
        // page-rc-INDEPENDENT `check_birth_shadow` / `check_page_deadlist_shadow`
        // oracles are the HARD tripwire now that the inverted shadow is gone.
        let snapshot_pinned = match self.clone_birth_lsn {
            // clone COW-kill CLONE arm: the page-rc-independent clone pinner-set term.
            // `youngest_clone_pinner_below` reads `clone_cow_pinners` ({B_C} ∪
            // own-snaps ∪ descendant branches), NOT `snapshot_wms`. page-rc removal dropped
            // the legacy `effective_rc > 1` floor (page-rc is deleted): the
            // pinner-set is now the sole, authoritative clone COW-kill operand.
            Some(_) => match self.youngest_clone_pinner_below(lsn) {
                None => false,
                Some(s) => birth <= s && !self.private_pages.contains(&pid),
            },
            // NON-CLONE arm: birth-authoritative. page-rc removal dropped the `effective_rc
            // > 1` floor; the pin is `birth <= max(youngest_snap_below(lsn),
            // youngest_clone_pinner_below(lsn)) && !private`. The clone term is
            // the page-rc removal replacement for the rc floor's clone-SOURCE coverage: a
            // non-clone volume that shares L2P pages with a clone (of a possibly
            // already-dropped snapshot) is fed every clone's branch point in
            // `clone_cow_pinners` (see `clone_cow_pinners_from`), so an origin
            // page born at/before a clone's branch is preserved, not recycled
            // under it (repro_drop_free). Empty when no clones exist.
            None => {
                let pin = self
                    .youngest_snap_below(lsn)
                    .into_iter()
                    .chain(self.youngest_clone_pinner_below(lsn))
                    .max();
                match pin {
                    None => false,
                    Some(s) => birth <= s && !self.private_pages.contains(&pid),
                }
            }
        };

        if !snapshot_pinned {
            // RECYCLE arm: the page is private to the live volume. The sub-case
            // is chosen by `private_pages` / `checkpoint_protected` /
            // `already_touched_by_lsn` — NOT by rc — so these are unchanged from
            // the legacy `effective_rc <= 1` arms; only the top gate moved to
            // `snapshot_pinned`.
            if self.private_pages.contains(&pid) {
                // `checkpoint_protected` is a DURABILITY pin orthogonal to BOTH
                // snapshots and page-rc: the just-committed on-disk checkpoint
                // still points at this exact `pid`, so clobbering its bytes in
                // place would corrupt the tree a crash would replay from. Copy
                // + retire UNCONDITIONALLY — must never be gated on birth/rc.
                if self.checkpoint_protected.contains(&pid) {
                    let new_pid = self.buf.clone_private(pid, lsn)?;
                    self.private_pages.remove(&pid);
                    self.private_pages.insert(new_pid);
                    self.retired_pages.insert(pid);
                    if let Some(ob) = old_birth {
                        self.push_live_free(pid, ob, lsn);
                    }
                    self.push_live_alloc(new_pid, lsn);
                    return Ok(new_pid);
                }
                return Ok(pid);
            }
            let already_touched_by_lsn = self.buf.read(pid)?.generation() >= lsn;
            if already_touched_by_lsn {
                let new_pid = self.buf.cow_for_write(pid, lsn)?;
                if new_pid != pid {
                    self.private_pages.remove(&pid);
                    self.private_pages.insert(new_pid);
                    if let Some(ob) = old_birth {
                        self.push_live_free(pid, ob, lsn);
                    }
                    self.push_live_alloc(new_pid, lsn);
                }
                return Ok(new_pid);
            }
            let new_pid = self.buf.clone_private(pid, lsn)?;
            self.private_pages.remove(&pid);
            self.private_pages.insert(new_pid);
            self.retired_pages.insert(pid);
            if let Some(ob) = old_birth {
                self.push_live_free(pid, ob, lsn);
            }
            self.push_live_alloc(new_pid, lsn);
            return Ok(new_pid);
        }

        // SHARED (snapshot_pinned): `pid` is pinned by a snapshot, so cloning it
        // leaves the old version alive for that snapshot — it "dies off the
        // head" here. Capture `(pid, birth, death=lsn)` BEFORE the COW consumes
        // the slot; the apply / fold layer drains it into the HEAD page-deadlist.
        // `PageBuf::cow_for_write` always copies now (per-L2P-page refcounting
        // was deleted, BFG): the birth decision above is the sole,
        // authoritative "shared" test, so the copy is unconditional — `new_pid`
        // is always a fresh page (`new_pid != pid`), the `cow_displaced` record
        // is always captured, and the old version is preserved for the snapshot.
        let new_pid = self.buf.cow_for_write(pid, lsn)?;
        if new_pid != pid {
            self.private_pages.insert(new_pid);
            self.cow_displaced.push(crate::deadlist::DeadRecord {
                pba: pid,
                birth_lsn: birth,
                death_lsn: lsn,
            });
            // v19 livelist: if the OLD shared page was itself clone-private
            // (`birth > B`) it just left the clone's live tree → FREE. Origin
            // pages (`birth <= B`) are not livelist members (they ride the
            // snapshot deadlist via `cow_displaced`). The new copy is born at
            // `lsn > B` → ALLOC. (Both no-ops for non-clones.)
            self.push_live_free(pid, birth, lsn);
            self.push_live_alloc(new_pid, lsn);
        }
        Ok(new_pid)
    }

    /// Drain the page-deadlist witness accumulated by this op's shared
    /// COWs (BFG). Called by the apply / compactor-fold layer
    /// after the op's rc deltas commit; each record whose
    /// `birth_lsn <= youngest_snap` is appended to the volume's HEAD
    /// page-deadlist. Records carry the dying `PageId` in `DeadRecord.pba`.
    pub(crate) fn take_cow_displaced(&mut self) -> Vec<crate::deadlist::DeadRecord> {
        std::mem::take(&mut self.cow_displaced)
    }

    /// BFG: arm (or disarm) this shard's per-clone livelist
    /// capture threshold. `Some(B)` makes alloc/COW/free of its clone-private
    /// pages (`birth > B`) emit `LiveRecord`s. Sticky: set once at clone build
    /// / reopen, never cleared (covers promoted ex-clones). Idempotent.
    pub(crate) fn set_clone_birth_lsn(&mut self, threshold: Option<Lsn>) {
        self.clone_birth_lsn = threshold;
    }

    /// True iff this shard belongs to a clone-lineage volume (the COW-kill keeps
    /// page-rc; the page-deadlist drain must separate snapshot deaths from
    /// clone-private deaths by birth).
    #[inline]
    pub(crate) fn is_clone(&self) -> bool {
        self.clone_birth_lsn.is_some()
    }

    /// BFG: set this shard's live-snapshot
    /// `capture_watermark`s (the operand of the birth-authoritative non-clone
    /// COW-kill decision in [`cow_for_write`](Self::cow_for_write)). Caller
    /// passes the watermarks in any order; stored SORTED ascending for
    /// [`youngest_snap_below`](Self::youngest_snap_below). Set per-op by the
    /// apply / fold layer (alongside [`set_current_bfg`]) from the durable
    /// `SnapshotEntry::capture_watermark`s so the COW decision is identical live
    /// and on replay. No-op effect for clones (the COW path keeps page-rc when
    /// `clone_birth_lsn.is_some()`).
    pub(crate) fn set_snapshot_wms(&mut self, mut wms: Vec<Lsn>) {
        wms.sort_unstable();
        self.snapshot_wms = wms;
    }

    /// `max{wm : wm < lsn}` over the live snapshot watermarks, or `None`. This
    /// is the youngest snapshot that was CAPTURED before a page dying at `lsn`
    /// died — the only ones that could reference the old page (see
    /// [`cow_for_write`](Self::cow_for_write)). `snapshot_wms` is sorted, so the
    /// answer is the entry just below the `partition_point`.
    #[inline]
    pub(crate) fn youngest_snap_below(&self, lsn: Lsn) -> Option<Lsn> {
        let i = self.snapshot_wms.partition_point(|&w| w < lsn);
        if i == 0 {
            None
        } else {
            Some(self.snapshot_wms[i - 1])
        }
    }

    /// BFG: set this clone shard's COW-kill pinner-LSN
    /// set (`{B_C} ∪ own-snap watermarks ∪ descendant branch points`, built by
    /// [`Db::clone_cow_pinners`]). Stored SORTED ascending for
    /// [`youngest_clone_pinner_below`](Self::youngest_clone_pinner_below). Fed
    /// per-op alongside [`set_snapshot_wms`](Self::set_snapshot_wms); empty for
    /// non-clones and on replay. SEPARATE from `snapshot_wms` so the deadlist
    /// drain (`drain_page_deaths_into`, C-own-snapshots) is untouched.
    pub(crate) fn set_clone_cow_pinners(&mut self, mut pinners: Vec<Lsn>) {
        pinners.sort_unstable();
        self.clone_cow_pinners = pinners;
    }

    /// `max{p : p < lsn}` over [`clone_cow_pinners`](Self::clone_cow_pinners), or
    /// `None`. The clone COW-kill (`cow_for_write`) preserves a page dying at
    /// `lsn` iff `birth <= youngest_clone_pinner_below(lsn)` — i.e. some pinner
    /// `p` with `birth <= p < lsn` (a pinner created at/after `lsn` sees the new
    /// version, not the old). Sorted, so the answer is just below the
    /// `partition_point`.
    #[inline]
    pub(crate) fn youngest_clone_pinner_below(&self, lsn: Lsn) -> Option<Lsn> {
        let i = self.clone_cow_pinners.partition_point(|&p| p < lsn);
        if i == 0 {
            None
        } else {
            Some(self.clone_cow_pinners[i - 1])
        }
    }

    /// Drain the page-livelist witness accumulated by this op's clone-private
    /// allocs / COWs / frees (BFG). Called by the apply / fold
    /// layer after the op's rc deltas commit; each record is appended to the
    /// clone's in-memory `page_live_list`. Always empty for non-clones.
    pub(crate) fn take_live_events(&mut self) -> Vec<crate::livelist::LiveRecord> {
        std::mem::take(&mut self.live_events)
    }

    /// Record the birth of a clone-private page (`birth = lsn > B`). No-op for
    /// non-clones (`clone_birth_lsn == None`).
    fn push_live_alloc(&mut self, new_pid: PageId, lsn: Lsn) {
        if let Some(b) = self.clone_birth_lsn
            && lsn > b
        {
            self.live_events.push(crate::livelist::LiveRecord {
                pid: new_pid,
                birth_lsn: lsn,
                event_lsn: lsn,
                kind: crate::livelist::LiveKind::Alloc,
            });
        }
    }

    /// Record the death of a page version iff it was itself clone-private
    /// (`old_birth > B`); origin pages (`birth <= B`) are not in the livelist
    /// (they ride the snapshot deadlist). No-op for non-clones.
    fn push_live_free(&mut self, old_pid: PageId, old_birth: Lsn, event_lsn: Lsn) {
        if let Some(b) = self.clone_birth_lsn
            && old_birth > b
        {
            self.live_events.push(crate::livelist::LiveRecord {
                pid: old_pid,
                birth_lsn: old_birth,
                event_lsn,
                kind: crate::livelist::LiveKind::Free,
            });
        }
    }

    fn free_detached(&mut self, pid: PageId, generation: Lsn) -> Result<()> {
        // v19 livelist: a page being detached/freed (delete emptied it) leaves
        // the clone's live tree. If it was clone-private (`birth > B`) emit a
        // FREE. Read birth before any forget; clones only (zero overhead else).
        if self.clone_birth_lsn.is_some() {
            let birth = self.buf.read(pid)?.birth_lsn();
            self.push_live_free(pid, birth, generation);
        }
        if self.checkpoint_protected.contains(&pid) {
            self.private_pages.remove(&pid);
            self.retired_pages.insert(pid);
            self.buf.forget(pid);
        } else if self.private_pages.remove(&pid) {
            self.buf.free(pid, generation)?;
        } else {
            self.retired_pages.insert(pid);
            self.buf.forget(pid);
        }
        Ok(())
    }

    pub fn checkpoint_committed(&mut self, generation: Lsn) -> Result<()> {
        let retired: Vec<PageId> = self.retired_pages.iter().copied().collect();
        for pid in retired {
            self.buf.free(pid, generation)?;
            self.retired_pages.remove(&pid);
        }
        self.private_pages.clear();
        self.finish_op(Ok(()))
    }

    pub(crate) fn begin_checkpoint(&mut self) -> Checkpoint {
        let private_pages = self.private_pages.clone();
        let retired_pages = self.retired_pages.clone();
        self.checkpoint_protected
            .extend(private_pages.iter().copied());
        Checkpoint {
            root: self.root,
            dirty: self.buf.dirty_snapshot(),
            private_pages,
            retired_pages,
        }
    }

    pub(crate) fn install_flushed_checkpoint_page(
        &mut self,
        flushed: &FlushedSnapshot,
        page_idx: usize,
    ) -> Option<(PageId, bool)> {
        self.buf.install_flushed_snapshot_page(flushed, page_idx)
    }

    /// Snapshot currently-dirty pages for an out-of-band writeback pass.
    ///
    /// Cheap: clones each `Slot::Dirty` Arc into the returned snapshot
    /// without touching `private_pages` / `retired_pages` /
    /// `checkpoint_protected`. The caller may drop the per-shard tree
    /// lock between this and `install_writeback` so the seal + IO step
    /// runs without serialising commit traffic.
    ///
    /// Background writeback is a *content-only* optimisation: pages get
    /// pushed to disk earlier than the next `flush()` would, so the
    /// flush itself sees a much smaller dirty set. Lifecycle bookkeeping
    /// (private/retired sets, checkpoint_lsn, manifest commit) stays in
    /// `Db::flush` — writeback never advances the durable checkpoint.
    pub(crate) fn writeback_dirty_snapshot(&self) -> DirtySnapshot {
        self.buf.dirty_snapshot()
    }

    /// Bounded variant of [`writeback_dirty_snapshot`]: gather at most
    /// `max` dirty pages. The streaming flusher uses this so each
    /// `install_writeback` only holds `tree.write()` long enough for
    /// `max` pages, keeping the install lock-hold below the level that
    /// would starve foreground commit apply on the same shard.
    pub(crate) fn writeback_dirty_snapshot_capped(&self, max: usize) -> DirtySnapshot {
        self.buf.dirty_snapshot_capped(max)
    }

    /// Install a writeback that has already been written and synced.
    ///
    /// For each page whose `Slot::Dirty(Arc)` still pointer-equals the
    /// snapshot's clone, drop the dirty entry (subsequent reads fault
    /// from disk / shared `PageCache`). Pages whose Arc was re-mutated
    /// during the IO window stay dirty for the next pass to retry.
    /// Returns `(promoted, kept_dirty)`.
    pub(crate) fn install_writeback(&mut self, flushed: &FlushedSnapshot) -> (usize, usize) {
        let mut promoted = 0;
        let mut kept = 0;
        for idx in 0..flushed.pages_count() {
            match self.buf.install_flushed_snapshot_page(flushed, idx) {
                Some((_, true)) => promoted += 1,
                Some((_, false)) => kept += 1,
                None => {}
            }
        }
        (promoted, kept)
    }

    /// Number of pages currently in `Slot::Dirty`. Used by writeback
    /// schedulers to skip shards with nothing to flush.
    pub(crate) fn dirty_page_count(&self) -> usize {
        self.buf.dirty_count()
    }

    pub(crate) fn checkpoint_retired_page_committed(&mut self, pid: PageId) -> Option<PageId> {
        if self.retired_pages.remove(&pid) {
            self.buf.detach_for_free(pid);
            Some(pid)
        } else {
            None
        }
    }

    pub(crate) fn checkpoint_private_page_committed(&mut self, pid: PageId, flushed_clean: bool) {
        if flushed_clean {
            self.private_pages.remove(&pid);
        }
        self.checkpoint_protected.remove(&pid);
    }

    pub(crate) fn finish_checkpoint_commit_step(&mut self, mut budget: usize) -> Result<bool> {
        let processed = self.buf.flush_read_overlay_updates_budget(budget);
        budget = budget.saturating_sub(processed);
        if self.buf.has_read_overlay_updates() {
            return Ok(false);
        }

        if budget > 0 {
            self.buf.evict_clean_pages_budget(budget);
        }
        Ok(!self.buf.has_clean_pages())
    }

    pub(crate) fn abort_checkpoint(&mut self, checkpoint: &Checkpoint) {
        for pid in &checkpoint.private_pages {
            self.checkpoint_protected.remove(pid);
        }
    }

    /// Run the structural checker over the whole tree.
    pub fn check_invariants(&self) -> Result<()> {
        crate::paged::invariants::check_tree(self.buf.page_store(), self.root)
    }

    /// Walk every index page reachable from the current root and pin
    /// it in the shared page cache. Leaves are skipped — their total
    /// byte count typically exceeds available memory, and random-get
    /// latency is bounded by index misses, not leaf misses.
    ///
    /// Warmup stops early and returns `skipped_budget = true` once
    /// `PageCache::pin` refuses a pin (global budget exhausted). The
    /// caller can inspect the returned stats to decide whether to
    /// raise `Config::index_pin_bytes`.
    ///
    /// Idempotent: calling twice pins the same pages twice (the
    /// second call is a no-op in `PageCache::pin` replace path). Cost
    /// is `O(total_index_pages)` read IO — usually < 1/256 of the
    /// tree's total bytes, so measured in tens of MiB for a 200M-key
    /// tree.
    pub fn warmup_index_pages(&mut self) -> Result<WarmupStats> {
        let mut stats = WarmupStats::default();
        if self.root == NULL_PAGE || self.root_level == 0 {
            // Either no tree at all, or the root is a leaf and there
            // are no index pages to pin.
            return Ok(stats);
        }
        let cache = self.buf.page_cache().clone();
        // BFS. A Vec acts as the frontier — we don't need FIFO order.
        let mut frontier: Vec<(PageId, u8)> = Vec::with_capacity(64);
        frontier.push((self.root, self.root_level));
        while let Some((pid, level)) = frontier.pop() {
            if level == 0 {
                // Leaf — do not pin.
                continue;
            }
            let page = cache.get(pid)?;
            expect_page_level(&page, pid, level, "paged::warmup_index_pages")?;
            if !cache.pin(pid, page.clone()) {
                stats.skipped_budget = true;
                // Do not enqueue children once the budget is out: the
                // whole point is to pin a dense reachable index set,
                // not scatter pins across disconnected subtrees.
                continue;
            }
            stats.pages_pinned += 1;
            for slot in 0..INDEX_FANOUT {
                let child = index_child_at(&page, slot);
                if child != NULL_PAGE {
                    frontier.push((child, level - 1));
                }
            }
        }
        Ok(stats)
    }

    // -------- read path --------------------------------------------------

    /// Point lookup. `None` if `lba` is not mapped.
    pub fn get(&mut self, lba: u64) -> Result<Option<L2pValue>> {
        let result = self.get_at_level(self.root, self.root_level, lba);
        self.finish_op(result)
    }

    /// Point lookup that does not mutate the tree-local page buffer.
    ///
    /// Unlike a pure page-cache read, this still observes dirty pages
    /// already held by the tree, so callers see committed in-memory
    /// state before the next checkpoint flushes it.
    pub fn get_read_only(&self, lba: u64) -> Result<Option<L2pValue>> {
        self.get_at_level_read_only(self.root, self.root_level, lba)
    }

    /// Batched point lookup for read-side callers. Keeps the shard on
    /// a shared lock and avoids one local-buffer cleanup per LBA.
    pub fn multi_get_read_only(&self, lbas: &[u64]) -> Result<Vec<Option<L2pValue>>> {
        let mut out = Vec::with_capacity(lbas.len());
        for &lba in lbas {
            out.push(self.get_at_level_read_only(self.root, self.root_level, lba)?);
        }
        Ok(out)
    }

    /// Point lookup against a snapshot's root. Reads the level from the
    /// root page header so callers don't need to track it separately.
    pub fn get_at(&mut self, root: PageId, lba: u64) -> Result<Option<L2pValue>> {
        let level = self.buf.read_level(root)?;
        let result = self.get_at_level(root, level, lba);
        self.finish_op(result)
    }

    /// Point lookup against a snapshot root without mutating the
    /// tree-local page buffer.
    pub fn get_at_read_only(&self, root: PageId, lba: u64) -> Result<Option<L2pValue>> {
        let level = self
            .buf
            .with_page_read_only(root, |page| page_level(page))?;
        self.get_at_level_read_only(root, level, lba)
    }

    fn get_at_level(&mut self, root: PageId, root_level: u8, lba: u64) -> Result<Option<L2pValue>> {
        let leaf_idx = lba >> LEAF_SHIFT;
        let bit = (lba & LEAF_MASK) as usize;
        if leaf_idx > max_leaf_idx_at_level(root_level) {
            return Ok(None);
        }
        let mut current = root;
        let mut level = root_level;
        while level > 0 {
            let slot = slot_in_index(leaf_idx, level);
            let page = self.buf.read(current)?;
            expect_page_level(page, current, level, "paged::get index")?;
            let child = index_child_at(page, slot);
            if child == NULL_PAGE {
                return Ok(None);
            }
            current = child;
            level -= 1;
        }
        let leaf = self.buf.read(current)?;
        expect_page_level(leaf, current, 0, "paged::get leaf")?;
        if !leaf_bit_set(leaf, bit) {
            return Ok(None);
        }
        leaf_value_at(leaf, bit)
    }

    fn get_at_level_read_only(
        &self,
        root: PageId,
        root_level: u8,
        lba: u64,
    ) -> Result<Option<L2pValue>> {
        let leaf_idx = lba >> LEAF_SHIFT;
        let bit = (lba & LEAF_MASK) as usize;
        if leaf_idx > max_leaf_idx_at_level(root_level) {
            return Ok(None);
        }
        let mut current = root;
        let mut level = root_level;
        while level > 0 {
            let slot = slot_in_index(leaf_idx, level);
            let child = self.buf.with_page_read_only(current, |page| {
                expect_page_level(page, current, level, "paged::get_read_only index")?;
                Ok(index_child_at(page, slot))
            })?;
            if child == NULL_PAGE {
                return Ok(None);
            }
            current = child;
            level -= 1;
        }
        self.buf.with_page_read_only(current, |leaf| {
            expect_page_level(leaf, current, 0, "paged::get_read_only leaf")?;
            if !leaf_bit_set(leaf, bit) {
                return Ok(None);
            }
            leaf_value_at(leaf, bit)
        })
    }

    // -------- write path -------------------------------------------------

    /// Insert or overwrite `lba`. Returns the previous value if the
    /// slot was mapped. Uses a tree-internal monotonic stamp for cow's
    /// gen-guard; callers running under a WAL apply should prefer
    /// [`insert_at_lsn`](Self::insert_at_lsn) so cross-tree cow on a
    /// page shared post-`clone_volume` uses the op's real WAL LSN and
    /// gets both cross-tree safety and replay idempotency.
    pub fn insert(&mut self, lba: u64, value: L2pValue) -> Result<Option<L2pValue>> {
        let generation = self.advance_gen();
        self.insert_with_lsn(lba, value, generation)
    }

    /// Variant of [`insert`](Self::insert) that stamps cow rc deltas
    /// with the given WAL LSN. Used by the WAL apply path so a replay
    /// of the same op observes matching gen stamps and skips already-
    /// applied deltas (the gen-stamped page-rc replay-skip). The
    /// tree's internal `next_gen` is bumped past `lsn` to keep the
    /// manifest's `max_generation` invariant.
    pub fn insert_at_lsn(
        &mut self,
        lba: u64,
        value: L2pValue,
        lsn: Lsn,
    ) -> Result<Option<L2pValue>> {
        self.advance_next_gen(lsn);
        self.insert_with_lsn(lba, value, lsn)
    }

    pub(crate) fn insert_at_lsn_deferred_finish(
        &mut self,
        lba: u64,
        value: L2pValue,
        lsn: Lsn,
    ) -> Result<Option<L2pValue>> {
        self.advance_next_gen(lsn);
        let result = self.insert_with_lsn_inner(lba, value, lsn);
        self.finalize_rc_deltas_deferred_finish(lsn, result)
    }

    pub(crate) fn insert_leaf_run_at_lsn_deferred_finish(
        &mut self,
        entries: &[(u64, L2pValue)],
        lsn: Lsn,
    ) -> Result<Vec<Option<L2pValue>>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        self.advance_next_gen(lsn);
        let result = (|| -> Result<Vec<Option<L2pValue>>> {
            let leaf_idx = entries[0].0 >> LEAF_SHIFT;
            for (lba, _) in entries.iter().skip(1) {
                let next_leaf_idx = *lba >> LEAF_SHIFT;
                if next_leaf_idx != leaf_idx {
                    return Err(MetaDbError::Corruption(format!(
                        "paged::insert leaf run crossed leaf boundary: first_leaf={leaf_idx} lba={lba} leaf={next_leaf_idx}"
                    )));
                }
            }

            while leaf_idx > max_leaf_idx_at_level(self.root_level) {
                self.grow_root(lsn)?;
            }

            let new_root = self.cow_for_write(self.root, lsn)?;
            let mut current = new_root;
            let mut level = self.root_level;
            while level > 0 {
                let slot = slot_in_index(leaf_idx, level);
                let child = index_child_at(self.buf.read(current)?, slot);
                let new_child = if child == NULL_PAGE {
                    if level == 1 {
                        self.alloc_leaf_private(lsn)?
                    } else {
                        self.alloc_index_private(lsn, level - 1)?
                    }
                } else {
                    self.cow_for_write(child, lsn)?
                };
                index_set_child(self.buf.modify(current, lsn)?, slot, new_child);
                current = new_child;
                level -= 1;
            }

            let leaf = self.buf.modify(current, lsn)?;
            let mut old_values = Vec::with_capacity(entries.len());
            for (lba, value) in entries {
                let bit = (*lba & LEAF_MASK) as usize;
                let old = leaf_set(leaf, bit, value).map_err(|err| {
                    MetaDbError::Corruption(format!(
                        "paged::insert leaf run leaf {current} slot {bit} lba {lba} lsn {lsn}: {err}"
                    ))
                })?;
                old_values.push(old);
            }
            self.root = new_root;
            Ok(old_values)
        })();
        self.finalize_rc_deltas_deferred_finish(lsn, result)
    }

    fn insert_with_lsn(&mut self, lba: u64, value: L2pValue, lsn: Lsn) -> Result<Option<L2pValue>> {
        let result = self.insert_with_lsn_inner(lba, value, lsn);
        self.finalize_rc_deltas(lsn, result)
    }

    fn insert_with_lsn_inner(
        &mut self,
        lba: u64,
        value: L2pValue,
        lsn: Lsn,
    ) -> Result<Option<L2pValue>> {
        let leaf_idx = lba >> LEAF_SHIFT;
        let bit = (lba & LEAF_MASK) as usize;

        // Grow root up to whatever level covers `leaf_idx`.
        while leaf_idx > max_leaf_idx_at_level(self.root_level) {
            self.grow_root(lsn)?;
        }

        // COW walk down. Missing slots get freshly-allocated children.
        let new_root = self.cow_for_write(self.root, lsn)?;
        let mut current = new_root;
        let mut level = self.root_level;
        while level > 0 {
            let slot = slot_in_index(leaf_idx, level);
            let child = index_child_at(self.buf.read(current)?, slot);
            let new_child = if child == NULL_PAGE {
                if level == 1 {
                    self.alloc_leaf_private(lsn)?
                } else {
                    self.alloc_index_private(lsn, level - 1)?
                }
            } else {
                self.cow_for_write(child, lsn)?
            };
            index_set_child(self.buf.modify(current, lsn)?, slot, new_child);
            current = new_child;
            level -= 1;
        }

        let old = leaf_set(self.buf.modify(current, lsn)?, bit, &value).map_err(|err| {
            MetaDbError::Corruption(format!(
                "paged::insert leaf {current} slot {bit} lba {lba} lsn {lsn}: {err}"
            ))
        })?;
        self.root = new_root;
        Ok(old)
    }

    /// Remove `lba`'s mapping. Returns the previous value, or `None` if
    /// the slot was unmapped. Frees pages along the path that become
    /// empty as a result. See [`insert`](Self::insert) on why the WAL
    /// apply path should call [`delete_at_lsn`](Self::delete_at_lsn)
    /// instead.
    pub fn delete(&mut self, lba: u64) -> Result<Option<L2pValue>> {
        if self.get(lba)?.is_none() {
            return Ok(None);
        }
        let generation = self.advance_gen();
        self.delete_with_lsn(lba, generation, true)
    }

    /// Variant of [`delete`](Self::delete) stamped with the WAL op's
    /// LSN; see [`insert_at_lsn`](Self::insert_at_lsn) for the
    /// replay-idempotency story.
    pub fn delete_at_lsn(&mut self, lba: u64, lsn: Lsn) -> Result<Option<L2pValue>> {
        if self.get(lba)?.is_none() {
            return Ok(None);
        }
        self.advance_next_gen(lsn);
        self.delete_with_lsn(lba, lsn, false)
    }

    pub(crate) fn delete_at_lsn_deferred_finish(
        &mut self,
        lba: u64,
        lsn: Lsn,
    ) -> Result<Option<L2pValue>> {
        if self.get(lba)?.is_none() {
            return Ok(None);
        }
        self.advance_next_gen(lsn);
        let result = self.delete_with_lsn_inner(lba, lsn, false);
        self.finalize_rc_deltas_deferred_finish(lsn, result)
    }

    fn delete_with_lsn(
        &mut self,
        lba: u64,
        lsn: Lsn,
        free_empty_pages: bool,
    ) -> Result<Option<L2pValue>> {
        let result = self.delete_with_lsn_inner(lba, lsn, free_empty_pages);
        self.finalize_rc_deltas(lsn, result)
    }

    fn delete_with_lsn_inner(
        &mut self,
        lba: u64,
        lsn: Lsn,
        free_empty_pages: bool,
    ) -> Result<Option<L2pValue>> {
        let leaf_idx = lba >> LEAF_SHIFT;
        let bit = (lba & LEAF_MASK) as usize;

        let new_root = self.cow_for_write(self.root, lsn)?;
        let mut current = new_root;
        let mut level = self.root_level;
        // Record (parent_pid, slot_in_parent) for upward pruning.
        let mut path: Vec<(PageId, usize)> = Vec::with_capacity(self.root_level as usize);
        while level > 0 {
            let slot = slot_in_index(leaf_idx, level);
            let child = index_child_at(self.buf.read(current)?, slot);
            debug_assert!(
                child != NULL_PAGE,
                "paged::delete: pre-check said key exists but slot is null"
            );
            let new_child = self.cow_for_write(child, lsn)?;
            index_set_child(self.buf.modify(current, lsn)?, slot, new_child);
            path.push((current, slot));
            current = new_child;
            level -= 1;
        }

        let old = leaf_clear(self.buf.modify(current, lsn)?, bit).map_err(|err| {
            MetaDbError::Corruption(format!(
                "paged::delete leaf {current} slot {bit} lba {lba} lsn {lsn}: {err}"
            ))
        })?;
        debug_assert!(old.is_some(), "paged::delete: pre-check said bit was set");

        // Prune upward. Stop at the root or at the first non-empty ancestor.
        let mut empty_child = if leaf_entry_count(self.buf.read(current)?) == 0 {
            Some(current)
        } else {
            None
        };
        while let Some(empty_id) = empty_child.take() {
            let (parent, slot_in_parent) = match path.pop() {
                Some(p) => p,
                None => break, // empty_id is the root; never freed.
            };
            // `empty_id` is exclusively owned by this op post-COW —
            // either a fresh allocation with in-memory rc=1 (disk
            // bytes stale) or an unshared original cow_for_write
            // returned unchanged. Skip the 1→0 RMW and free directly;
            // any upstream shared-page deltas flow through the
            // pending_rc accumulator committed at op end.
            if free_empty_pages {
                self.free_detached(empty_id, lsn)?;
            } else if self.private_pages.contains(&empty_id) {
                self.free_detached(empty_id, lsn)?;
            }
            index_set_child(self.buf.modify(parent, lsn)?, slot_in_parent, NULL_PAGE);
            if index_child_count(self.buf.read(parent)?) == 0 {
                empty_child = Some(parent);
            }
        }

        self.root = new_root;
        Ok(old)
    }

    /// Finalize the op: on success, return `result` after `finish_op`
    /// bookkeeping; on error, drop the COW-death / livelist witnesses so a
    /// retry won't replay them. (Per-L2P-page refcount delta commit was
    /// deleted, BFG — `lsn` is now unused by the success arm but
    /// kept for signature stability with the deferred variants.)
    fn finalize_rc_deltas<T>(&mut self, lsn: Lsn, result: Result<T>) -> Result<T> {
        let result = self.finalize_rc_deltas_deferred_finish(lsn, result);
        self.finish_op(result)
    }

    fn finalize_rc_deltas_deferred_finish<T>(&mut self, _lsn: Lsn, result: Result<T>) -> Result<T> {
        match result {
            Ok(v) => Ok(v),
            Err(e) => {
                // The op aborted: its COW page-deaths never became durable,
                // so drop the witness. A successful op leaves it for the
                // apply / fold layer.
                self.cow_displaced.clear();
                // v19: same for the page-livelist witness — an aborted op's
                // clone-private allocs/frees never landed.
                self.live_events.clear();
                Err(e)
            }
        }
    }

    fn grow_root(&mut self, generation: Lsn) -> Result<()> {
        if self.root_level >= MAX_INDEX_LEVEL {
            return Err(MetaDbError::Corruption(format!(
                "paged: tree growth would exceed MAX_INDEX_LEVEL={MAX_INDEX_LEVEL}"
            )));
        }
        let new_level = self.root_level + 1;
        let new_root = self.alloc_index_private(generation, new_level)?;
        // `index_set_child` doesn't touch refcounts; the old root moves
        // from being pointed to by "the live tree" to being pointed to
        // by the new root at slot 0 — same single live-tree parent.
        index_set_child(self.buf.modify(new_root, generation)?, 0, self.root);
        self.root = new_root;
        self.root_level = new_level;
        Ok(())
    }

    // -------- range scan -------------------------------------------------

    /// Range scan against the current root.
    pub fn range<R: RangeBounds<u64>>(&mut self, range: R) -> Result<PagedRangeIter> {
        self.range_at(self.root, OwnedRange::from_bounds(range))
    }

    /// Range scan against a snapshot root. Used by `Db`'s
    /// `collect_range_for_roots`.
    pub fn range_at<R: RangeBounds<u64>>(
        &mut self,
        root: PageId,
        range: R,
    ) -> Result<PagedRangeIter> {
        let range = OwnedRange::from_bounds(range);
        let root_level = self.buf.read_level(root)?;
        let mut items = Vec::new();
        self.collect_range(root, root_level, 0, &range, &mut items)?;
        self.finish_op(Ok(PagedRangeIter::new(items)))
    }

    fn collect_range(
        &mut self,
        pid: PageId,
        level: u8,
        base_lba: u64,
        range: &OwnedRange,
        out: &mut Vec<(u64, L2pValue)>,
    ) -> Result<()> {
        if level == 0 {
            // Leaf: iterate set bits and filter by range.
            let page = self.buf.read(pid)?;
            expect_page_level(page, pid, 0, "paged::collect_range leaf")?;
            for i in 0..LEAF_ENTRY_COUNT {
                if !leaf_bit_set(page, i) {
                    continue;
                }
                let lba = base_lba + i as u64;
                if !range.contains(&lba) {
                    continue;
                }
                if let Some(value) = leaf_value_at(page, i)? {
                    out.push((lba, value));
                }
            }
            return Ok(());
        }

        // Index: snapshot the non-null children first so we can recurse
        // without holding a borrow on `self.buf`.
        let children: Vec<(usize, PageId)> = {
            let page = self.buf.read(pid)?;
            expect_page_level(page, pid, level, "paged::collect_range index")?;
            (0..INDEX_FANOUT)
                .filter_map(|i| {
                    let c = index_child_at(page, i);
                    (c != NULL_PAGE).then_some((i, c))
                })
                .collect()
        };
        let slot_span = slot_span_for_level(level);
        for (slot, child) in children {
            let child_base = base_lba + (slot as u64) * slot_span;
            let child_end = child_base.saturating_add(slot_span - 1);
            if !overlaps(range, child_base, child_end) {
                continue;
            }
            self.collect_range(child, level - 1, child_base, range, out)?;
        }
        Ok(())
    }

    // -------- snapshot helpers ------------------------------------------

    /// Compute the diff between two subtrees. Onyx does not use this
    /// on the hot path — callers are snapshot diff tools — so the
    /// implementation is a simple "collect both subtrees, merge sorted
    /// streams". Returns entries in ascending key order.
    pub fn diff_subtrees(&mut self, a: PageId, b: PageId) -> Result<Vec<DiffEntry>> {
        let a_items: Vec<(u64, L2pValue)> = self.range_at(a, ..)?.collect::<Result<Vec<_>>>()?;
        let b_items: Vec<(u64, L2pValue)> = self.range_at(b, ..)?.collect::<Result<Vec<_>>>()?;
        let mut out = Vec::new();
        merge_diff_into(&a_items, &b_items, &mut out);
        self.finish_op(Ok(out))
    }

    /// Build an rc-dependent drop plan rooted at `snap_root`. The walk
    /// cascades: the root always contributes, and a page's children
    /// contribute only if the page's structural refcount would hit 0 after
    /// the (hypothetical) decrement. No mutations happen — this is a
    /// read-only simulation.
    ///
    /// Returns the ordered list of pages to decrement. Safe under
    /// concurrent writers ONLY if the caller holds a lock that
    /// excludes concurrent `cow_for_write`; a COW landing between plan
    /// and apply can bump a shared page's rc and invalidate the
    /// cascade decisions here. `Db::drop_snapshot` takes
    /// `drop_gate.write()` for exactly that reason.
    ///
    /// `NULL_PAGE` input returns an empty vec (empty shard). `structural_rc`
    /// is the live-global-graph in-edge map (see
    /// [`collect_drop_pages_with_birth`](Self::collect_drop_pages_with_birth)).
    pub fn collect_drop_pages(
        &mut self,
        snap_root: PageId,
        structural_rc: &std::collections::BTreeMap<PageId, u32>,
    ) -> Result<Vec<PageId>> {
        Ok(self
            .collect_drop_pages_with_birth(snap_root, structural_rc)?
            .into_iter()
            .map(|(pid, _, _)| pid)
            .collect())
    }

    /// `collect_drop_pages`, but also returning each visited page's immutable
    /// `birth_lsn` and its STRUCTURAL refcount at plan time. The birth rides
    /// the page read the cascade already performs (no extra IO); the rc lets
    /// the caller recover the *freed* subset — pages with `rc == 1` whose `-1`
    /// decref reaches zero are freed by `apply_drop_*_pages`, while `rc > 1`
    /// entries are the decref-only shared boundary (a page still pinned by
    /// another root, decremented but kept).
    ///
    /// BFG: per-L2P-page refcounting was DELETED, so the rc no longer
    /// comes from a page-rc array. The caller supplies `structural_rc` — the
    /// number of parent edges pointing at each page in the live global L2P
    /// graph (every surviving volume head + every snapshot root, plus this
    /// dying volume's roots), computed once by
    /// [`collect_paged_refcounts_for_roots`](crate::db::apply::collect_paged_refcounts_for_roots).
    /// Because each COW tree is a tree (one parent per page within a single
    /// root), a page's combined in-edge count is exactly the number of live
    /// roots whose tree routes through it — i.e. the old global page-rc for
    /// every page reachable from a live root. `structural_rc == 1` therefore
    /// equals "reachable from this volume only" (C-exclusive), so the
    /// `rc == 1` cascade gating and the
    /// [`Db::check_clone_livelist_shadow`](crate::db) `structural_free` LHS
    /// stay identical to the page-rc era.
    pub fn collect_drop_pages_with_birth(
        &mut self,
        snap_root: PageId,
        structural_rc: &std::collections::BTreeMap<PageId, u32>,
    ) -> Result<Vec<(PageId, Lsn, u32)>> {
        use crate::page::PageType;
        if snap_root == NULL_PAGE {
            return self.finish_op(Ok(Vec::new()));
        }
        let mut out: Vec<(PageId, Lsn, u32)> = Vec::new();
        let mut worklist: Vec<PageId> = vec![snap_root];
        while let Some(pid) = worklist.pop() {
            // Structural in-edge count over the live global graph supplied by
            // the caller; a page reachable from this dying volume is always
            // present (the map was built over a root-set that includes this
            // volume's roots). The page read supplies the type + children for
            // the cascade walk and the immutable birth_lsn for the livelist
            // shadow partition.
            let rc = structural_rc.get(&pid).copied().ok_or_else(|| {
                MetaDbError::Corruption(format!(
                    "paged::collect_drop_pages: page {pid} missing from the structural refcount map"
                ))
            })?;
            let (page_type, birth, children) = {
                let page = self.buf.read(pid)?;
                let header = page.header()?;
                let children = match header.page_type {
                    PageType::PagedIndex => crate::paged::format::index_collect_children(page),
                    PageType::PagedLeaf => Vec::new(),
                    other => {
                        return self.finish_op(Err(MetaDbError::Corruption(format!(
                            "paged::collect_drop_pages: unexpected page type {other:?} at {pid}"
                        ))));
                    }
                };
                (header.page_type, header.birth_lsn, children)
            };
            if rc == 0 {
                return self.finish_op(Err(MetaDbError::Corruption(format!(
                    "paged::collect_drop_pages: page {pid} already at refcount 0"
                ))));
            }
            out.push((pid, birth, rc));
            // Only recurse into children if the decrement would free
            // this page — matches the old page-rc cascade.
            if rc == 1 && matches!(page_type, PageType::PagedIndex) {
                worklist.extend(children);
            }
        }
        self.finish_op(Ok(out))
    }

    /// Evict `pid` from this tree's local page buffer so the next
    /// read goes back to the shared page cache / disk. Used by
    /// `Db::drop_snapshot` after the WAL-apply path writes pages via
    /// the bare `PageStore`, which bypasses `PageBuf`. If `pid` was
    /// never cached here the call is a no-op.
    pub fn forget_page(&mut self, pid: PageId) {
        self.buf.forget(pid);
    }

    /// Replace the in-memory root pointer + level. Called by `Db`
    /// during snapshot restore / WAL replay when `Db` computes the new
    /// root from the manifest. `level` is not re-derived from the page;
    /// the caller is expected to have already read it from the root
    /// page header (via `PageBuf::read_level` or `page_level`).
    pub fn reset_root(&mut self, root: PageId, level: u8) {
        self.root = root;
        self.root_level = level;
        self.private_pages.clear();
        self.retired_pages.clear();
        self.buf.forget_all();
    }

    /// Swap the tree's root to a foreign page that is already durable on
    /// disk, dropping the local page cache so stale dirty-bit tracking
    /// can't misroute a later write.
    ///
    /// Used by `CloneVolume` apply: the clone target's shard is
    /// initialised pointing at one of the source snapshot's shard roots,
    /// with the page-store-level refcount incref already performed by the
    /// caller (so `pid`'s on-disk header carries the updated rc). The
    /// first write into the clone will then `cow_for_write` down from
    /// `pid`, leaving the snapshot's view of the subtree intact.
    ///
    /// `level` must match the page's actual level (caller reads it via
    /// `PageBuf::read_level`); this function does not re-derive it from
    /// disk so the caller can reuse a level it already has on hand. If
    /// `level > MAX_INDEX_LEVEL` the call is rejected.
    pub fn attach_subtree_root(&mut self, pid: PageId, level: u8) -> Result<()> {
        if level > MAX_INDEX_LEVEL {
            return Err(MetaDbError::InvalidArgument(format!(
                "paged::attach_subtree_root: level {level} exceeds max {MAX_INDEX_LEVEL}"
            )));
        }
        self.buf.forget_all();
        self.private_pages.clear();
        self.retired_pages.clear();
        self.root = pid;
        self.root_level = level;
        Ok(())
    }

    /// Streaming range scan. **Today this materialises its result upfront**
    /// (same implementation as [`PagedL2p::range`]) — the public surface is
    /// exposed now so callers can code against the "stream" API while a
    /// buffer-journal replay commit swaps the body to a lazy frame-stack walker without
    /// touching any callsite. The `PagedRangeIter` yields items in
    /// ascending key order either way.
    ///
    /// Semantically equivalent to `range` for now; the distinction is
    /// forward-looking.
    pub fn range_stream<R: RangeBounds<u64>>(&mut self, range: R) -> Result<PagedRangeIter> {
        self.range(range)
    }

    // -------- lifecycle --------------------------------------------------

    /// Persist all dirty pages. Must be called before the caller
    /// commits a new root pointer to the manifest.
    pub fn flush(&mut self) -> Result<()> {
        let result = self.buf.flush();
        self.finish_op(result)
    }

    #[cfg(test)]
    fn cached_pages_for_test(&self) -> usize {
        self.buf.len()
    }

    fn advance_gen(&mut self) -> Lsn {
        let g = self.next_gen;
        self.next_gen = self
            .next_gen
            .checked_add(1)
            .expect("paged::next_gen overflowed u64");
        g
    }
}

fn expect_page_level(
    page: &crate::page::Page,
    pid: PageId,
    expected_level: u8,
    context: &'static str,
) -> Result<()> {
    let actual = page_level(page)?;
    if actual != expected_level {
        return Err(MetaDbError::Corruption(format!(
            "{context}: page {pid} has level {actual}, expected {expected_level}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
