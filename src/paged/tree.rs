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
use crate::paged::cache::PageBuf;
use crate::paged::format::{
    INDEX_FANOUT, INDEX_SHIFT, L2pValue, LEAF_ENTRY_COUNT, LEAF_MASK, LEAF_SHIFT, MAX_INDEX_LEVEL,
    index_child_at, index_child_count, index_set_child, leaf_bit_set, leaf_clear, leaf_entry_count,
    leaf_set, leaf_value_at, max_leaf_idx_at_level, slot_in_index,
};
use crate::types::{Lsn, NULL_PAGE, PageId};

/// Owned range for multi-root scans. Mirrors `db::OwnedRange` (kept
/// crate-private there) so we can accept it in `range_at` without
/// reaching into `Db`'s private types.
#[derive(Clone, Debug)]
struct OwnedRange {
    start: Bound<u64>,
    end: Bound<u64>,
}

impl OwnedRange {
    fn from_bounds<R: RangeBounds<u64>>(range: R) -> Self {
        Self {
            start: clone_bound(range.start_bound()),
            end: clone_bound(range.end_bound()),
        }
    }
}

impl RangeBounds<u64> for OwnedRange {
    fn start_bound(&self) -> Bound<&u64> {
        ref_bound(&self.start)
    }
    fn end_bound(&self) -> Bound<&u64> {
        ref_bound(&self.end)
    }
}

fn clone_bound(b: Bound<&u64>) -> Bound<u64> {
    match b {
        Bound::Included(v) => Bound::Included(*v),
        Bound::Excluded(v) => Bound::Excluded(*v),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn ref_bound(b: &Bound<u64>) -> Bound<&u64> {
    match b {
        Bound::Included(v) => Bound::Included(v),
        Bound::Excluded(v) => Bound::Excluded(v),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// One entry in the delta between two subtrees. Emitted by
/// [`PagedL2p::diff_subtrees`] and surfaced via `Db::diff`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum DiffEntry {
    /// Key exists in B but not in A.
    AddedInB { key: u64, new: L2pValue },
    /// Key exists in A but not in B.
    RemovedInB { key: u64, old: L2pValue },
    /// Key exists in both; value changed.
    Changed {
        key: u64,
        old: L2pValue,
        new: L2pValue,
    },
}

impl DiffEntry {
    /// Key this diff entry concerns.
    pub fn key(&self) -> u64 {
        match self {
            Self::AddedInB { key, .. }
            | Self::RemovedInB { key, .. }
            | Self::Changed { key, .. } => *key,
        }
    }
}

/// Iterator returned by [`PagedL2p::range`]. All items are materialised
/// up-front — range scans on Onyx's L2P are rare (snapshot exports,
/// debug tools) so eager collection keeps the implementation simple.
pub struct PagedRangeIter {
    inner: std::vec::IntoIter<(u64, L2pValue)>,
}

impl PagedRangeIter {
    fn new(mut items: Vec<(u64, L2pValue)>) -> Self {
        items.sort_unstable_by_key(|(k, _)| *k);
        Self {
            inner: items.into_iter(),
        }
    }
}

impl Iterator for PagedRangeIter {
    type Item = Result<(u64, L2pValue)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Ok)
    }
}

/// One paged L2P index tree. Not `Send` across threads without external
/// synchronisation — `Db` wraps it in `Mutex`.
pub struct PagedL2p {
    buf: PageBuf,
    root: PageId,
    root_level: u8,
    next_gen: Lsn,
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

    /// Run the structural checker over the whole tree.
    pub fn check_invariants(&self) -> Result<()> {
        crate::paged::invariants::check_tree(self.buf.page_store(), self.root)
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
    /// slot was mapped.
    pub fn insert(&mut self, lba: u64, value: L2pValue) -> Result<Option<L2pValue>> {
        let generation = self.advance_gen();
        let leaf_idx = lba >> LEAF_SHIFT;
        let bit = (lba & LEAF_MASK) as usize;

        // Grow root up to whatever level covers `leaf_idx`.
        while leaf_idx > max_leaf_idx_at_level(self.root_level) {
            self.grow_root(generation)?;
        }

        // COW walk down. Missing slots get freshly-allocated children.
        let new_root = self.buf.cow_for_write(self.root, generation)?;
        let mut current = new_root;
        let mut level = self.root_level;
        while level > 0 {
            let slot = slot_in_index(leaf_idx, level);
            let child = index_child_at(self.buf.read(current)?, slot);
            let new_child = if child == NULL_PAGE {
                if level == 1 {
                    self.buf.alloc_leaf(generation)?
                } else {
                    self.buf.alloc_index(generation, level - 1)?
                }
            } else {
                self.buf.cow_for_write(child, generation)?
            };
            index_set_child(self.buf.modify(current, generation)?, slot, new_child);
            current = new_child;
            level -= 1;
        }

        let old = leaf_set(self.buf.modify(current, generation)?, bit, &value);
        self.root = new_root;
        self.finish_op(Ok(old))
    }

    /// Remove `lba`'s mapping. Returns the previous value, or `None` if
    /// the slot was unmapped. Frees pages along the path that become
    /// empty as a result.
    pub fn delete(&mut self, lba: u64) -> Result<Option<L2pValue>> {
        // Pre-check before we start COW'ing: avoid allocating on misses.
        // The read-only descent populates the page cache, so the
        // subsequent COW walk hits warm pages either way.
        if self.get(lba)?.is_none() {
            return Ok(None);
        }

        let generation = self.advance_gen();
        let leaf_idx = lba >> LEAF_SHIFT;
        let bit = (lba & LEAF_MASK) as usize;

        let new_root = self.buf.cow_for_write(self.root, generation)?;
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
            let new_child = self.buf.cow_for_write(child, generation)?;
            index_set_child(self.buf.modify(current, generation)?, slot, new_child);
            path.push((current, slot));
            current = new_child;
            level -= 1;
        }

        let old = leaf_clear(self.buf.modify(current, generation)?, bit);
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
            self.buf.decref(empty_id, generation)?;
            index_set_child(
                self.buf.modify(parent, generation)?,
                slot_in_parent,
                NULL_PAGE,
            );
            if index_child_count(self.buf.read(parent)?) == 0 {
                empty_child = Some(parent);
            }
        }

        self.root = new_root;
        self.finish_op(Ok(old))
    }

    fn grow_root(&mut self, generation: Lsn) -> Result<()> {
        if self.root_level >= MAX_INDEX_LEVEL {
            return Err(MetaDbError::Corruption(format!(
                "paged: tree growth would exceed MAX_INDEX_LEVEL={MAX_INDEX_LEVEL}"
            )));
        }
        let new_level = self.root_level + 1;
        let new_root = self.buf.alloc_index(generation, new_level)?;
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
    pub fn incref_root_for_snapshot(&mut self) -> Result<()> {
        let generation = self.advance_gen();
        self.buf.incref(self.root, generation)?;
        self.finish_op(Ok(()))
    }

    /// Decref `root` and cascade through any uniquely-owned subtree.
    /// Used by snapshot drop to release a snapshot's grip on a tree.
    pub fn decref_root(&mut self, root: PageId) -> Result<()> {
        let generation = self.advance_gen();
        self.buf.decref(root, generation)?;
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
    pub fn drop_subtree(&mut self, snap_root: PageId) -> Result<Vec<L2pValue>> {
        let generation = self.advance_gen();
        let mut collected: Vec<L2pValue> = Vec::new();
        let mut worklist: Vec<PageId> = vec![snap_root];
        while let Some(pid) = worklist.pop() {
            let (new_rc, page_type, children, values) = {
                let page = self.buf.modify(pid, generation)?;
                let rc = page.refcount();
                if rc == 0 {
                    return Err(MetaDbError::Corruption(format!(
                        "paged::drop_subtree: page {pid} already at refcount 0"
                    )));
                }
                let new_rc = rc - 1;
                page.set_refcount(new_rc);
                let page_type = page.header()?.page_type;
                if new_rc == 0 {
                    use crate::page::PageType;
                    match page_type {
                        PageType::PagedLeaf => {
                            let vs: Vec<L2pValue> = (0..LEAF_ENTRY_COUNT)
                                .filter(|i| leaf_bit_set(page, *i))
                                .map(|i| leaf_value_at(page, i))
                                .collect();
                            (new_rc, page_type, Vec::new(), vs)
                        }
                        PageType::PagedIndex => {
                            let cs = crate::paged::format::index_collect_children(page);
                            (new_rc, page_type, cs, Vec::new())
                        }
                        other => {
                            return Err(MetaDbError::Corruption(format!(
                                "paged::drop_subtree: unexpected page type {other:?} at {pid}"
                            )));
                        }
                    }
                } else {
                    (new_rc, page_type, Vec::new(), Vec::new())
                }
            };
            if new_rc == 0 {
                use crate::page::PageType;
                if !matches!(page_type, PageType::PagedLeaf | PageType::PagedIndex) {
                    return Err(MetaDbError::Corruption(format!(
                        "paged::drop_subtree: refusing to free {pid} with type {page_type:?}"
                    )));
                }
                collected.extend(values);
                worklist.extend(children);
                self.buf.free(pid, generation)?;
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

fn slot_span_for_level(level: u8) -> u64 {
    debug_assert!((1..=MAX_INDEX_LEVEL).contains(&level));
    // Each level-L index slot covers INDEX_FANOUT^(L-1) leaves, each
    // covering LEAF_ENTRY_COUNT LBAs. Exponents are safe up to level 4
    // (= 2^39 LBAs).
    1u64 << (LEAF_SHIFT + INDEX_SHIFT * (level as u32 - 1))
}

/// Merge two ascending `(key, value)` streams into `DiffEntry` items in
/// ascending key order. Same shape as `btree::merge_diff_into` so tests
/// that assert diff ordering don't need special casing.
fn merge_diff_into(a: &[(u64, L2pValue)], b: &[(u64, L2pValue)], out: &mut Vec<DiffEntry>) {
    let mut i = 0usize;
    let mut j = 0usize;
    while i < a.len() && j < b.len() {
        match a[i].0.cmp(&b[j].0) {
            std::cmp::Ordering::Less => {
                out.push(DiffEntry::RemovedInB {
                    key: a[i].0,
                    old: a[i].1,
                });
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(DiffEntry::AddedInB {
                    key: b[j].0,
                    new: b[j].1,
                });
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                if a[i].1 != b[j].1 {
                    out.push(DiffEntry::Changed {
                        key: a[i].0,
                        old: a[i].1,
                        new: b[j].1,
                    });
                }
                i += 1;
                j += 1;
            }
        }
    }
    while i < a.len() {
        out.push(DiffEntry::RemovedInB {
            key: a[i].0,
            old: a[i].1,
        });
        i += 1;
    }
    while j < b.len() {
        out.push(DiffEntry::AddedInB {
            key: b[j].0,
            new: b[j].1,
        });
        j += 1;
    }
}

fn overlaps(range: &OwnedRange, lo: u64, hi: u64) -> bool {
    let lo_ok = match range.end_bound() {
        Bound::Included(&end) => lo <= end,
        Bound::Excluded(&end) => lo < end,
        Bound::Unbounded => true,
    };
    let hi_ok = match range.start_bound() {
        Bound::Included(&start) => hi >= start,
        Bound::Excluded(&start) => hi > start,
        Bound::Unbounded => true,
    };
    lo_ok && hi_ok
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn mk_store() -> (TempDir, Arc<PageStore>) {
        let dir = TempDir::new().unwrap();
        let ps = Arc::new(PageStore::create(dir.path().join("p.onyx_meta")).unwrap());
        (dir, ps)
    }

    fn v(n: u8) -> L2pValue {
        L2pValue([n; 28])
    }

    #[test]
    fn empty_tree_starts_as_leaf() {
        let (_d, ps) = mk_store();
        let t = PagedL2p::create(ps).unwrap();
        assert_eq!(t.root_level(), 0);
    }

    #[test]
    fn insert_single_lba_stays_at_level_zero() {
        let (_d, ps) = mk_store();
        let mut t = PagedL2p::create(ps).unwrap();
        assert_eq!(t.insert(5, v(1)).unwrap(), None);
        assert_eq!(t.get(5).unwrap(), Some(v(1)));
        assert_eq!(t.root_level(), 0);
    }

    #[test]
    fn insert_beyond_leaf_grows_to_index() {
        let (_d, ps) = mk_store();
        let mut t = PagedL2p::create(ps).unwrap();
        assert_eq!(t.insert(200, v(42)).unwrap(), None);
        // 200 >> 7 = 1, which exceeds level-0 capacity (max_leaf_idx=0).
        assert_eq!(t.root_level(), 1);
        assert_eq!(t.get(200).unwrap(), Some(v(42)));
        // Entries in the original leaf slot survive the promotion.
        t.insert(5, v(5)).unwrap();
        assert_eq!(t.get(5).unwrap(), Some(v(5)));
    }

    #[test]
    fn insert_far_lba_grows_multiple_levels() {
        let (_d, ps) = mk_store();
        let mut t = PagedL2p::create(ps).unwrap();
        // leaf_idx needs ~17 bits → level 3 (covers 24 bits of leaf_idx).
        let far = 200_000u64 << LEAF_SHIFT;
        t.insert(far, v(9)).unwrap();
        assert!(t.root_level() >= 3);
        assert_eq!(t.get(far).unwrap(), Some(v(9)));
    }

    #[test]
    fn overwrite_returns_previous_value() {
        let (_d, ps) = mk_store();
        let mut t = PagedL2p::create(ps).unwrap();
        assert_eq!(t.insert(17, v(1)).unwrap(), None);
        assert_eq!(t.insert(17, v(2)).unwrap(), Some(v(1)));
        assert_eq!(t.get(17).unwrap(), Some(v(2)));
    }

    #[test]
    fn delete_missing_key_is_noop() {
        let (_d, ps) = mk_store();
        let mut t = PagedL2p::create(ps).unwrap();
        assert_eq!(t.delete(42).unwrap(), None);
        t.insert(10, v(1)).unwrap();
        // 10 is in a different leaf bit than 42 but same leaf pre-growth.
        assert_eq!(t.delete(42).unwrap(), None);
        assert_eq!(t.get(10).unwrap(), Some(v(1)));
    }

    #[test]
    fn delete_returns_and_removes_value() {
        let (_d, ps) = mk_store();
        let mut t = PagedL2p::create(ps).unwrap();
        t.insert(3, v(7)).unwrap();
        t.insert(4, v(8)).unwrap();
        assert_eq!(t.delete(3).unwrap(), Some(v(7)));
        assert_eq!(t.get(3).unwrap(), None);
        assert_eq!(t.get(4).unwrap(), Some(v(8)));
    }

    #[test]
    fn delete_empties_leaf_then_parent() {
        let (_d, ps) = mk_store();
        let mut t = PagedL2p::create(ps.clone()).unwrap();
        // Force level 2+: two distant LBAs.
        let a = 0u64;
        let b = 1_000_000u64;
        t.insert(a, v(1)).unwrap();
        t.insert(b, v(2)).unwrap();
        let lvl_before = t.root_level();
        assert!(lvl_before >= 2);

        t.flush().unwrap();
        let free_before = ps.free_list_len();
        // Deleting `b` should empty its sub-tree and free its leaf +
        // ancestors up to (but not including) the root.
        assert_eq!(t.delete(b).unwrap(), Some(v(2)));
        t.flush().unwrap();
        // At least some pages should have been freed (leaf plus any
        // dedicated index pages on b's path).
        assert!(ps.free_list_len() > free_before);

        // `a` remains.
        assert_eq!(t.get(a).unwrap(), Some(v(1)));
        // `b` is gone.
        assert_eq!(t.get(b).unwrap(), None);
    }

    #[test]
    fn flush_persists_state_across_reopen() {
        let (_d, ps) = mk_store();
        let root_pid;
        let next_gen;
        {
            let mut t = PagedL2p::create(ps.clone()).unwrap();
            t.insert(1, v(11)).unwrap();
            t.insert(500_000, v(22)).unwrap();
            t.flush().unwrap();
            root_pid = t.root();
            next_gen = 100;
        }
        let mut t = PagedL2p::open(ps, root_pid, next_gen).unwrap();
        assert_eq!(t.get(1).unwrap(), Some(v(11)));
        assert_eq!(t.get(500_000).unwrap(), Some(v(22)));
        assert_eq!(t.get(999).unwrap(), None);
    }

    #[test]
    fn flush_and_reads_do_not_leave_private_clean_pages_resident() {
        let (_d, ps) = mk_store();
        let mut t = PagedL2p::create(ps).unwrap();
        for i in 0..1024u64 {
            t.insert(i * 1024, v((i % 255) as u8)).unwrap();
        }
        assert!(t.cached_pages_for_test() > 0);
        t.flush().unwrap();
        assert_eq!(t.cached_pages_for_test(), 0);

        for i in 0..256u64 {
            let _ = t.get(i * 1024).unwrap();
            assert_eq!(t.cached_pages_for_test(), 0);
        }
    }

    #[test]
    fn snapshot_incref_preserves_old_view_under_writes() {
        let (_d, ps) = mk_store();
        let mut t = PagedL2p::create(ps).unwrap();
        t.insert(3, v(1)).unwrap();
        t.insert(300, v(2)).unwrap(); // forces level 1+
        t.flush().unwrap();

        // "Take a snapshot" — bump root rc and remember the root id.
        t.incref_root_for_snapshot().unwrap();
        let snap_root = t.root();

        // Mutate the live tree.
        t.insert(3, v(99)).unwrap();
        t.insert(300, v(88)).unwrap();
        assert_eq!(t.get(3).unwrap(), Some(v(99)));

        // The snapshot root still sees the old values.
        assert_eq!(t.get_at(snap_root, 3).unwrap(), Some(v(1)));
        assert_eq!(t.get_at(snap_root, 300).unwrap(), Some(v(2)));
    }

    #[test]
    fn range_scan_returns_sorted_hits_within_bounds() {
        let (_d, ps) = mk_store();
        let mut t = PagedL2p::create(ps).unwrap();
        for k in [1u64, 5, 200, 500, 10_000, 1_000_000] {
            t.insert(k, v((k % 255) as u8)).unwrap();
        }
        let got: Vec<u64> = t.range(5..=500).unwrap().map(|r| r.unwrap().0).collect();
        assert_eq!(got, vec![5, 200, 500]);

        let got_all: Vec<u64> = t.range(..).unwrap().map(|r| r.unwrap().0).collect();
        assert_eq!(got_all, vec![1, 5, 200, 500, 10_000, 1_000_000]);

        let got_hi: Vec<u64> = t.range(600..).unwrap().map(|r| r.unwrap().0).collect();
        assert_eq!(got_hi, vec![10_000, 1_000_000]);
    }

    #[test]
    fn growth_caps_at_max_level() {
        let (_d, ps) = mk_store();
        let mut t = PagedL2p::create(ps).unwrap();
        // The largest LBA we can insert sits at level 4. Just below the
        // overflow boundary: leaf_idx = 2^32 - 1 → lba = (2^32 - 1) << 7.
        let max_leaf_idx_at_level4: u64 = max_leaf_idx_at_level(4);
        let max_lba = (max_leaf_idx_at_level4 << LEAF_SHIFT) | LEAF_MASK;
        t.insert(max_lba, v(1)).unwrap();
        assert_eq!(t.root_level(), 4);
        assert_eq!(t.get(max_lba).unwrap(), Some(v(1)));

        // One past that (requires level 5) must refuse.
        let out_of_range = max_lba.wrapping_add(1);
        if out_of_range > max_lba {
            // Only test if it didn't wrap to 0.
            assert!(t.insert(out_of_range, v(2)).is_err());
        }
    }

    #[test]
    fn many_inserts_deletes_preserve_invariants() {
        let (_d, ps) = mk_store();
        let mut t = PagedL2p::create(ps).unwrap();
        // Interleave hot leaves across levels.
        let keys: Vec<u64> = (0..512u64).map(|i| i * 2000).collect();
        for (i, k) in keys.iter().enumerate() {
            t.insert(*k, v(i as u8)).unwrap();
        }
        for (i, k) in keys.iter().enumerate() {
            assert_eq!(t.get(*k).unwrap(), Some(v(i as u8)), "key {k}");
        }
        for k in &keys {
            t.delete(*k).unwrap();
        }
        for k in &keys {
            assert_eq!(t.get(*k).unwrap(), None, "key {k} not deleted");
        }
    }
}
