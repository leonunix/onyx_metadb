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

use std::collections::HashSet;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use crate::cache::{DEFAULT_PAGE_CACHE_BYTES, PageCache};
use crate::error::{MetaDbError, Result};
use crate::page_store::PageStore;
use crate::paged::cache::PageBuf;
use crate::paged::format::{
    INDEX_FANOUT, INDEX_SHIFT, L2pValue, LEAF_ENTRY_COUNT, LEAF_MASK, LEAF_SHIFT, MAX_INDEX_LEVEL,
    index_child_at, index_child_count, index_set_child, leaf_bit_set, leaf_clear, leaf_entry_count,
    leaf_set, leaf_value_at, max_leaf_idx_at_level, slot_in_index,
};
use crate::types::{Lsn, NULL_PAGE, PageId};

mod helpers;
mod types;

use helpers::*;
use types::OwnedRange;
pub use types::{DeleteOutcome, DiffEntry, InsertOutcome, PagedRangeIter, WarmupStats};

/// One paged L2P index tree. Not `Send` across threads without external
/// synchronisation — `Db` wraps it in `Mutex`.
pub struct PagedL2p {
    buf: PageBuf,
    root: PageId,
    root_level: u8,
    next_gen: Lsn,
    private_pages: HashSet<PageId>,
    retired_pages: HashSet<PageId>,
}

impl PagedL2p {
    fn finish_op<T>(&mut self, result: Result<T>) -> Result<T> {
        self.buf.evict_clean_pages();
        result
    }

    /// Fresh empty tree. Allocates one leaf as the root, level 0.
    pub fn create(page_store: Arc<PageStore>) -> Result<Self> {
        let page_cache = Arc::new(PageCache::new(page_store.clone(), DEFAULT_PAGE_CACHE_BYTES));
        Self::create_with_cache(page_store, page_cache)
    }

    pub fn create_with_cache(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
    ) -> Result<Self> {
        let mut buf = PageBuf::with_cache(page_store, page_cache);
        let root = buf.alloc_leaf(1)?;
        buf.flush()?;
        Ok(Self {
            buf,
            root,
            root_level: 0,
            next_gen: 2,
            private_pages: HashSet::new(),
            retired_pages: HashSet::new(),
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
            private_pages: HashSet::new(),
            retired_pages: HashSet::new(),
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

    /// Bump `next_gen` if the caller's LSN watermark has advanced past
    /// it. Called from `Db` when a commit's LSN exceeds the tree's
    /// counter so subsequent page stamps stay monotonic.
    pub fn advance_next_gen(&mut self, lsn: Lsn) {
        if lsn >= self.next_gen {
            self.next_gen = lsn + 1;
        }
    }

    /// Underlying page store handle (shared with `Db` for free-list
    /// inspection, etc.).
    pub fn page_store(&self) -> &Arc<PageStore> {
        self.buf.page_store()
    }

    fn alloc_leaf_private(&mut self, generation: Lsn) -> Result<PageId> {
        let pid = self.buf.alloc_leaf(generation)?;
        self.private_pages.insert(pid);
        Ok(pid)
    }

    fn alloc_index_private(&mut self, generation: Lsn, level: u8) -> Result<PageId> {
        let pid = self.buf.alloc_index(generation, level)?;
        self.private_pages.insert(pid);
        Ok(pid)
    }

    fn cow_for_write(&mut self, pid: PageId, lsn: Lsn) -> Result<PageId> {
        let effective_rc = self.buf.effective_rc(pid)?;
        if self.private_pages.contains(&pid) && effective_rc <= 1 {
            return Ok(pid);
        }
        if effective_rc <= 1 {
            let already_touched_by_lsn = self.buf.read(pid)?.generation() >= lsn;
            if already_touched_by_lsn {
                let new_pid = self.buf.cow_for_write(pid, lsn)?;
                if new_pid != pid {
                    self.private_pages.insert(new_pid);
                }
                return Ok(new_pid);
            }
            let new_pid = self.buf.clone_private(pid, lsn)?;
            self.private_pages.insert(new_pid);
            self.retired_pages.insert(pid);
            return Ok(new_pid);
        }
        let new_pid = self.buf.cow_for_write(pid, lsn)?;
        if new_pid != pid {
            self.private_pages.insert(new_pid);
        }
        Ok(new_pid)
    }

    fn free_detached(&mut self, pid: PageId, generation: Lsn) -> Result<()> {
        if self.private_pages.remove(&pid) {
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

    /// Point lookup against a snapshot's root. Reads the level from the
    /// root page header so callers don't need to track it separately.
    pub fn get_at(&mut self, root: PageId, lba: u64) -> Result<Option<L2pValue>> {
        let level = self.buf.read_level(root)?;
        let result = self.get_at_level(root, level, lba);
        self.finish_op(result)
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
            let child = index_child_at(self.buf.read(current)?, slot);
            if child == NULL_PAGE {
                return Ok(None);
            }
            current = child;
            level -= 1;
        }
        let leaf = self.buf.read(current)?;
        if !leaf_bit_set(leaf, bit) {
            return Ok(None);
        }
        Ok(Some(leaf_value_at(leaf, bit)))
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
    /// applied deltas ([`PageStore::atomic_rc_delta_with_gen`]). The
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

    /// Variant of [`insert_at_lsn`](Self::insert_at_lsn) that also
    /// reports whether the leaf holding `lba` was shared with another
    /// tree (snapshot / clone) at the moment this op descended to it.
    ///
    /// Used exclusively by the onyx adapter apply path for
    /// `WalOp::L2pRemap`: the decref decision table in SPEC §3.1
    /// ("leaf-rc-suppress") depends on this bit to avoid double-freeing
    /// a PBA that the snapshot still references. The flag is captured
    /// before the leaf's `cow_for_write` runs, so it reflects the
    /// pre-op sharing state regardless of how the op's own COW cascade
    /// reshapes the tree afterwards.
    pub fn insert_at_lsn_with_share_info(
        &mut self,
        lba: u64,
        value: L2pValue,
        lsn: Lsn,
    ) -> Result<InsertOutcome> {
        self.advance_next_gen(lsn);
        let result = self.insert_with_lsn_and_share(lba, value, lsn);
        self.finalize_rc_deltas(lsn, result)
    }

    fn insert_with_lsn(&mut self, lba: u64, value: L2pValue, lsn: Lsn) -> Result<Option<L2pValue>> {
        let result = self
            .insert_with_lsn_inner(lba, value, lsn)
            .map(|outcome| outcome.prev);
        self.finalize_rc_deltas(lsn, result)
    }

    fn insert_with_lsn_and_share(
        &mut self,
        lba: u64,
        value: L2pValue,
        lsn: Lsn,
    ) -> Result<InsertOutcome> {
        self.insert_with_lsn_inner(lba, value, lsn)
    }

    fn insert_with_lsn_inner(
        &mut self,
        lba: u64,
        value: L2pValue,
        lsn: Lsn,
    ) -> Result<InsertOutcome> {
        let leaf_idx = lba >> LEAF_SHIFT;
        let bit = (lba & LEAF_MASK) as usize;

        // Root-is-leaf path: tree hasn't grown past level 0, so the
        // leaf we're about to mutate IS the root. Capture its pre-COW
        // effective rc here — the walk-down loop never reaches
        // level==1 in this case, and by the time `grow_root` runs the
        // leaf's refcount has already been bumped by the newly-
        // allocated parent index.
        let mut leaf_was_shared = self.root_level == 0 && self.buf.effective_rc(self.root)? > 1;

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
                // Missing slot: fabricated leaf has no pre-op reference
                // from any tree, so `leaf_was_shared` stays at whatever
                // the root-is-leaf branch set (it's only true when the
                // ORIGINAL root was a shared leaf, which cannot coexist
                // with a missing slot in an index that didn't exist
                // yet — this branch therefore leaves the flag at false).
                if level == 1 {
                    self.alloc_leaf_private(lsn)?
                } else {
                    self.alloc_index_private(lsn, level - 1)?
                }
            } else {
                if level == 1 {
                    // Capture the pre-COW effective rc so the caller
                    // can distinguish "snapshot still references old
                    // leaf via this page" from "op can safely mutate
                    // in place". `cow_for_write` below may fold in
                    // pending rc deltas and clone the page; we take
                    // the decision before that mutation.
                    leaf_was_shared = self.buf.effective_rc(child)? > 1;
                }
                self.cow_for_write(child, lsn)?
            };
            index_set_child(self.buf.modify(current, lsn)?, slot, new_child);
            current = new_child;
            level -= 1;
        }

        let old = leaf_set(self.buf.modify(current, lsn)?, bit, &value);
        self.root = new_root;
        Ok(InsertOutcome {
            prev: old,
            leaf_was_shared,
        })
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

    /// Variant of [`delete_at_lsn`](Self::delete_at_lsn) that also
    /// reports whether the leaf holding `lba` was shared with another
    /// tree in the pre-op state. Used by `WalOp::L2pRangeDelete`'s
    /// apply path for the same leaf-rc-suppress decision
    /// [`insert_at_lsn_with_share_info`](Self::insert_at_lsn_with_share_info)
    /// serves on the write side (SPEC §4.4).
    ///
    /// Returns `DeleteOutcome { prev: None, leaf_was_shared: false }`
    /// when `lba` is unmapped; the caller should treat that as a
    /// no-op and not emit a refcount decref.
    pub fn delete_at_lsn_with_share_info(&mut self, lba: u64, lsn: Lsn) -> Result<DeleteOutcome> {
        if self.get(lba)?.is_none() {
            return Ok(DeleteOutcome {
                prev: None,
                leaf_was_shared: false,
            });
        }
        self.advance_next_gen(lsn);
        let result = self.delete_with_lsn_inner_with_share(lba, lsn, false);
        self.finalize_rc_deltas(lsn, result)
    }

    fn delete_with_lsn(
        &mut self,
        lba: u64,
        lsn: Lsn,
        free_empty_pages: bool,
    ) -> Result<Option<L2pValue>> {
        let result = self
            .delete_with_lsn_inner_with_share(lba, lsn, free_empty_pages)
            .map(|outcome| outcome.prev);
        self.finalize_rc_deltas(lsn, result)
    }

    fn delete_with_lsn_inner_with_share(
        &mut self,
        lba: u64,
        lsn: Lsn,
        free_empty_pages: bool,
    ) -> Result<DeleteOutcome> {
        let leaf_idx = lba >> LEAF_SHIFT;
        let bit = (lba & LEAF_MASK) as usize;

        // Root-is-leaf path: capture share-ness before any mutation
        // advances the tree. Same rule as the insert side (see
        // `insert_with_lsn_inner`): when `root_level == 0` the leaf
        // about to be mutated IS the root, and the walk-down loop
        // never reaches `level == 1`.
        let mut leaf_was_shared = self.root_level == 0 && self.buf.effective_rc(self.root)? > 1;

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
            if level == 1 {
                // Pre-COW effective rc for the leaf — same capture
                // point the insert path uses.
                leaf_was_shared = self.buf.effective_rc(child)? > 1;
            }
            let new_child = self.cow_for_write(child, lsn)?;
            index_set_child(self.buf.modify(current, lsn)?, slot, new_child);
            path.push((current, slot));
            current = new_child;
            level -= 1;
        }

        let old = leaf_clear(self.buf.modify(current, lsn)?, bit);
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
        Ok(DeleteOutcome {
            prev: old,
            leaf_was_shared,
        })
    }

    /// Commit the op's batched rc deltas on success, or drop them on
    /// error. `result` is returned as-is (after `finish_op` bookkeeping).
    /// If commit itself fails the commit error wins over a successful
    /// `result`; any queued deltas for a failing op are discarded so a
    /// retry won't double-apply.
    fn finalize_rc_deltas<T>(&mut self, lsn: Lsn, result: Result<T>) -> Result<T> {
        match result {
            Ok(v) => {
                let commit = self.buf.commit_rc_deltas(lsn);
                self.finish_op(commit.map(|()| v))
            }
            Err(e) => {
                self.buf.clear_rc_deltas();
                self.finish_op(Err(e))
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
            for i in 0..LEAF_ENTRY_COUNT {
                if !leaf_bit_set(page, i) {
                    continue;
                }
                let lba = base_lba + i as u64;
                if !range.contains(&lba) {
                    continue;
                }
                out.push((lba, leaf_value_at(page, i)));
            }
            return Ok(());
        }

        // Index: snapshot the non-null children first so we can recurse
        // without holding a borrow on `self.buf`.
        let children: Vec<(usize, PageId)> = {
            let page = self.buf.read(pid)?;
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

    /// Bump the root's refcount so a caller (snapshot take) holds a
    /// separate reference. Idempotent in the sense that every call adds
    /// exactly one ref — pair it with a `decref` on snapshot drop.
    ///
    /// Routes through [`PageBuf::atomic_incref`]'s disk-direct
    /// per-pid-locked RMW so a sibling volume's concurrent
    /// `cow_for_write` against the same root (post-`clone_volume`)
    /// can't race and drop the incref.
    pub fn incref_root_for_snapshot(&mut self) -> Result<()> {
        self.buf.atomic_incref(self.root)?;
        self.finish_op(Ok(()))
    }

    /// Decref `root` and cascade through any uniquely-owned subtree.
    /// Used by snapshot drop to release a snapshot's grip on a tree.
    /// Routes through [`PageBuf::atomic_decref`] for the same
    /// cross-tree safety reason as
    /// [`incref_root_for_snapshot`](Self::incref_root_for_snapshot).
    pub fn decref_root(&mut self, root: PageId) -> Result<()> {
        self.buf.atomic_decref(root)?;
        self.finish_op(Ok(()))
    }

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

    /// Release a subtree held by a snapshot, returning every leaf value
    /// that was freed in the process. Matches `BTree::drop_subtree`'s
    /// semantics so `Db::drop_snapshot` can use either tree type
    /// interchangeably.
    ///
    /// The walk visits each page once, decrements rc by 1, and for
    /// pages that hit rc=0 collects leaf values (or recurses into index
    /// children). Pages still shared after the decrement are left alone.
    ///
    /// Rc mutations route through
    /// [`PageStore::atomic_rc_delta`](crate::page_store::PageStore::atomic_rc_delta)'s
    /// per-pid-locked disk-direct RMW so a sibling volume's concurrent
    /// `cow_for_write` against a shared page can't race. Leaves
    /// `page.generation` untouched — this path is not WAL-replayed.
    pub fn drop_subtree(&mut self, snap_root: PageId) -> Result<Vec<L2pValue>> {
        use crate::page::PageType;
        use crate::paged::cache::DecrefOutcome;
        let mut collected: Vec<L2pValue> = Vec::new();
        let mut worklist: Vec<PageId> = vec![snap_root];
        while let Some(pid) = worklist.pop() {
            // If this page is a leaf that will be freed, we need its
            // values *before* the atomic RMW clears the page from our
            // cache. Index pages contribute no values; children come
            // back from `atomic_decref_one` when rc hits 0.
            let pending_values: Vec<L2pValue> = {
                let page = self.buf.read(pid)?;
                let header = page.header()?;
                match header.page_type {
                    PageType::PagedLeaf => (0..LEAF_ENTRY_COUNT)
                        .filter(|i| leaf_bit_set(page, *i))
                        .map(|i| leaf_value_at(page, i))
                        .collect(),
                    PageType::PagedIndex => Vec::new(),
                    other => {
                        return Err(MetaDbError::Corruption(format!(
                            "paged::drop_subtree: unexpected page type {other:?} at {pid}"
                        )));
                    }
                }
            };
            let (outcome, children) = self.buf.atomic_decref_one(pid)?;
            if matches!(outcome, DecrefOutcome::Freed) {
                collected.extend(pending_values);
                worklist.extend(children);
            }
        }
        self.finish_op(Ok(collected))
    }

    /// Build an rc-dependent drop plan rooted at `snap_root`. The walk
    /// mirrors [`drop_subtree`](Self::drop_subtree)'s cascading
    /// decrement: the root always contributes, and a page's children
    /// contribute only if the page's refcount would hit 0 after the
    /// (hypothetical) decrement. No mutations happen — this is a
    /// read-only simulation.
    ///
    /// Returns the ordered list of pages to decrement. Safe under
    /// concurrent writers ONLY if the caller holds a lock that
    /// excludes concurrent `cow_for_write`; a COW landing between plan
    /// and apply can bump a shared page's rc and invalidate the
    /// cascade decisions here. `Db::drop_snapshot` takes
    /// `drop_gate.write()` for exactly that reason.
    ///
    /// `NULL_PAGE` input returns an empty vec (empty shard).
    pub fn collect_drop_pages(&mut self, snap_root: PageId) -> Result<Vec<PageId>> {
        use crate::page::PageType;
        if snap_root == NULL_PAGE {
            return self.finish_op(Ok(Vec::new()));
        }
        let mut out: Vec<PageId> = Vec::new();
        let mut worklist: Vec<PageId> = vec![snap_root];
        while let Some(pid) = worklist.pop() {
            let (rc, page_type, children) = {
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
                (header.refcount, header.page_type, children)
            };
            if rc == 0 {
                return self.finish_op(Err(MetaDbError::Corruption(format!(
                    "paged::collect_drop_pages: page {pid} already at refcount 0"
                ))));
            }
            out.push(pid);
            // Only recurse into children if the decrement would free
            // this page — matches `drop_subtree`'s cascade.
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
    /// Used by Phase 7 `CloneVolume` apply: the clone target's shard is
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
    /// Phase C commit swaps the body to a lazy frame-stack walker without
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

#[cfg(test)]
mod tests;
