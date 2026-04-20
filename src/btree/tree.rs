//! Single-writer, in-place B+tree over a [`PageStore`].
//!
//! Phase 2 scope:
//! - One tree per partition (metadb will hand-manage many trees; each
//!   owns its own `BTree` instance plus a `PageBuf`).
//! - In-place page modifications (no COW — that lands in phase 3).
//! - Insert supports full leaf / internal split, including root
//!   promotion when the tree grows a level.
//! - Get descends the tree without modifying anything.
//! - Delete, range scan, and WAL integration are follow-up slices.
//!
//! # Generations
//!
//! Every mutation is stamped with a fresh generation LSN from a
//! monotonic counter on the `BTree`. In phase 2 this is just a page-
//! freshness marker; phase 6 will align the counter with the WAL LSN
//! assignment order so replay can reconstruct the state.

use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use crate::btree::cache::PageBuf;
use crate::btree::format::{
    L2pValue, LEAF_ENTRY_SIZE, MAX_INTERNAL_KEYS, MAX_LEAF_ENTRIES, internal_child_at,
    internal_insert, internal_key_at, internal_key_count, internal_pop_front, internal_push_front,
    internal_remove, internal_search, internal_set_first_child, internal_set_key_at, leaf_insert,
    leaf_key_at, leaf_key_count, leaf_remove, leaf_search, leaf_set_entry, leaf_value_at,
};
use crate::error::{MetaDbError, Result};
use crate::page::PageType;
use crate::page_store::PageStore;
use crate::types::{Lsn, PageId};

/// Minimum leaf fill before a delete triggers a borrow-or-merge. Set
/// to half of [`MAX_LEAF_ENTRIES`] so two underflowing leaves always
/// merge into one; any higher threshold would force borrowing in
/// cases where a merge would fit.
const LEAF_UNDERFLOW_THRESHOLD: usize = MAX_LEAF_ENTRIES / 2;

/// Minimum internal fill before a delete triggers a borrow-or-merge.
/// Set to half of [`MAX_INTERNAL_KEYS`] so two underflowing internals
/// plus a pivot key always fit in one merged page.
const INTERNAL_UNDERFLOW_THRESHOLD: usize = MAX_INTERNAL_KEYS / 2;

/// Single-partition B+tree. Not `Send` across threads without external
/// synchronization — phase 4 will shard + wrap this type.
pub struct BTree {
    buf: PageBuf,
    root: PageId,
    next_gen: Lsn,
}

impl BTree {
    /// Create a fresh empty tree on `page_store`. Allocates one empty
    /// leaf, persists it, and records its page id as the root.
    pub fn create(page_store: Arc<PageStore>) -> Result<Self> {
        let mut buf = PageBuf::new(page_store);
        let root = buf.alloc_leaf(1)?;
        buf.flush()?;
        Ok(Self {
            buf,
            root,
            next_gen: 2,
        })
    }

    /// Attach to an already-persisted tree rooted at `root`. The
    /// caller is responsible for knowing which page the root lives at
    /// (phase 3+ reads it from the manifest's partition table).
    pub fn open(page_store: Arc<PageStore>, root: PageId, next_gen: Lsn) -> Result<Self> {
        let mut buf = PageBuf::new(page_store);
        // Sanity: the root must decode as either a leaf or an internal.
        let page = buf.read(root)?;
        match page.header()?.page_type {
            PageType::L2pLeaf | PageType::L2pInternal => {}
            other => {
                return Err(MetaDbError::Corruption(format!(
                    "root page {root} is not an L2P page: {other:?}",
                )));
            }
        }
        Ok(Self {
            buf,
            root,
            next_gen,
        })
    }

    /// Current root page id. Callers persist this via the manifest at
    /// commit time.
    pub fn root(&self) -> PageId {
        self.root
    }

    /// Next generation LSN. Advanced by every mutation.
    pub fn next_generation(&self) -> Lsn {
        self.next_gen
    }

    /// Persist every dirty page and fsync. Must be called before the
    /// tree handle is dropped or the process exits for writes to
    /// survive. Phase 6 will fold this into WAL group-commit.
    pub fn flush(&mut self) -> Result<()> {
        self.buf.flush()
    }

    fn new_gen(&mut self) -> Lsn {
        let g = self.next_gen;
        self.next_gen = self
            .next_gen
            .checked_add(1)
            .expect("BTree generation counter exhausted");
        g
    }

    // -------- get --------------------------------------------------------

    /// Point lookup. Returns `Some(value)` on a hit, `None` on a miss.
    pub fn get(&mut self, key: u64) -> Result<Option<L2pValue>> {
        let mut current = self.root;
        loop {
            let probe = {
                let page = self.buf.read(current)?;
                let header = page.header()?;
                match header.page_type {
                    PageType::L2pLeaf => match leaf_search(page, key) {
                        Ok(i) => GetProbe::Hit(leaf_value_at(page, i)),
                        Err(_) => GetProbe::Miss,
                    },
                    PageType::L2pInternal => {
                        let slot = internal_search(page, key);
                        GetProbe::Descend(internal_child_at(page, slot))
                    }
                    other => {
                        return Err(MetaDbError::Corruption(format!(
                            "unexpected page type {other:?} at page {current}",
                        )));
                    }
                }
            };
            match probe {
                GetProbe::Hit(v) => return Ok(Some(v)),
                GetProbe::Miss => return Ok(None),
                GetProbe::Descend(child) => current = child,
            }
        }
    }

    // -------- insert -----------------------------------------------------

    /// Insert or update `(key, value)`. Returns `Some(old_value)` if
    /// the key was already present; `None` if it was newly inserted.
    pub fn insert(&mut self, key: u64, value: L2pValue) -> Result<Option<L2pValue>> {
        let generation = self.new_gen();
        let mut path: Vec<(PageId, usize)> = Vec::new();
        let mut current = self.root;

        loop {
            // Probe the current page under a short read borrow, decide
            // what to do, drop the borrow, then act.
            let action = {
                let page = self.buf.read(current)?;
                let header = page.header()?;
                match header.page_type {
                    PageType::L2pLeaf => match leaf_search(page, key) {
                        Ok(i) => Action::Update {
                            pos: i,
                            old: leaf_value_at(page, i),
                        },
                        Err(pos) => {
                            if leaf_key_count(page) < MAX_LEAF_ENTRIES {
                                Action::Insert { pos }
                            } else {
                                Action::Split { pos }
                            }
                        }
                    },
                    PageType::L2pInternal => {
                        let child_slot = internal_search(page, key);
                        let child_pid = internal_child_at(page, child_slot);
                        Action::Descend {
                            child_pid,
                            child_slot,
                        }
                    }
                    other => {
                        return Err(MetaDbError::Corruption(format!(
                            "unexpected page type {other:?} at page {current}",
                        )));
                    }
                }
            };

            match action {
                Action::Update { pos, old } => {
                    let page = self.buf.modify(current, generation)?;
                    leaf_set_entry(page, pos, key, &value);
                    return Ok(Some(old));
                }
                Action::Insert { pos } => {
                    let page = self.buf.modify(current, generation)?;
                    leaf_insert(page, pos, key, &value)?;
                    return Ok(None);
                }
                Action::Split { pos } => {
                    let (sep_key, new_right) = split_leaf_and_insert(
                        &mut self.buf,
                        current,
                        pos,
                        key,
                        &value,
                        generation,
                    )?;
                    self.propagate_split_up(&path, sep_key, new_right, generation)?;
                    return Ok(None);
                }
                Action::Descend {
                    child_pid,
                    child_slot,
                } => {
                    path.push((current, child_slot));
                    current = child_pid;
                }
            }
        }
    }

    // -------- delete -----------------------------------------------------

    /// Remove `key`. Returns `Some(old)` if the key was present,
    /// `None` otherwise. Rebalances the tree via sibling borrow or
    /// merge; collapses a single-child root if one emerges.
    pub fn delete(&mut self, key: u64) -> Result<Option<L2pValue>> {
        let generation = self.new_gen();
        let mut path: Vec<(PageId, usize)> = Vec::new();
        let mut current = self.root;

        loop {
            let action = {
                let page = self.buf.read(current)?;
                match page.header()?.page_type {
                    PageType::L2pLeaf => match leaf_search(page, key) {
                        Ok(pos) => DeleteAction::Remove {
                            pos,
                            old: leaf_value_at(page, pos),
                        },
                        Err(_) => DeleteAction::Miss,
                    },
                    PageType::L2pInternal => {
                        let slot = internal_search(page, key);
                        let child = internal_child_at(page, slot);
                        DeleteAction::Descend { child, slot }
                    }
                    other => {
                        return Err(MetaDbError::Corruption(format!(
                            "unexpected page type {other:?} at page {current}",
                        )));
                    }
                }
            };

            match action {
                DeleteAction::Miss => return Ok(None),
                DeleteAction::Remove { pos, old } => {
                    {
                        let page = self.buf.modify(current, generation)?;
                        leaf_remove(page, pos)?;
                    }
                    self.rebalance_after_remove(path, current, generation)?;
                    return Ok(Some(old));
                }
                DeleteAction::Descend { child, slot } => {
                    path.push((current, slot));
                    current = child;
                }
            }
        }
    }

    /// Walk the path from leaf toward root, rebalancing pages that
    /// dropped below the underflow threshold. Handles root collapse
    /// when the root is a childless internal at the end.
    fn rebalance_after_remove(
        &mut self,
        path: Vec<(PageId, usize)>,
        leaf_pid: PageId,
        generation: Lsn,
    ) -> Result<()> {
        let mut current_pid = leaf_pid;
        for (parent_pid, child_slot) in path.into_iter().rev() {
            let (underflow, is_leaf) = self.check_underflow(current_pid)?;
            if !underflow {
                return Ok(());
            }
            let merged =
                self.rebalance_child(parent_pid, child_slot, current_pid, is_leaf, generation)?;
            if !merged {
                return Ok(());
            }
            current_pid = parent_pid;
        }
        // current_pid is self.root; collapse if it's a childless internal.
        self.maybe_collapse_root(current_pid, generation)?;
        Ok(())
    }

    fn check_underflow(&mut self, pid: PageId) -> Result<(bool, bool)> {
        let page = self.buf.read(pid)?;
        let header = page.header()?;
        match header.page_type {
            PageType::L2pLeaf => {
                let count = leaf_key_count(page);
                Ok((count < LEAF_UNDERFLOW_THRESHOLD, true))
            }
            PageType::L2pInternal => {
                let count = internal_key_count(page);
                Ok((count < INTERNAL_UNDERFLOW_THRESHOLD, false))
            }
            other => Err(MetaDbError::Corruption(format!(
                "unexpected page type {other:?} at {pid}",
            ))),
        }
    }

    /// Try to rebalance `child_pid` within `parent_pid` by borrowing
    /// from or merging with an immediate sibling. Returns `true` if a
    /// merge occurred (so the parent lost a key), `false` if a borrow
    /// made the child non-underflow.
    fn rebalance_child(
        &mut self,
        parent_pid: PageId,
        child_slot: usize,
        child_pid: PageId,
        child_is_leaf: bool,
        generation: Lsn,
    ) -> Result<bool> {
        let parent_n = internal_key_count(self.buf.read(parent_pid)?);
        let has_left = child_slot > 0;
        let has_right = child_slot < parent_n;
        let threshold = if child_is_leaf {
            LEAF_UNDERFLOW_THRESHOLD
        } else {
            INTERNAL_UNDERFLOW_THRESHOLD
        };

        // Prefer right borrow for predictability; fall back to left.
        if has_right {
            let right_pid = internal_child_at(self.buf.read(parent_pid)?, child_slot + 1);
            let right_count = self.page_count(right_pid, child_is_leaf)?;
            if right_count > threshold {
                if child_is_leaf {
                    self.borrow_from_right_leaf(
                        parent_pid, child_slot, child_pid, right_pid, generation,
                    )?;
                } else {
                    self.borrow_from_right_internal(
                        parent_pid, child_slot, child_pid, right_pid, generation,
                    )?;
                }
                return Ok(false);
            }
        }
        if has_left {
            let left_pid = internal_child_at(self.buf.read(parent_pid)?, child_slot - 1);
            let left_count = self.page_count(left_pid, child_is_leaf)?;
            if left_count > threshold {
                if child_is_leaf {
                    self.borrow_from_left_leaf(
                        parent_pid,
                        child_slot - 1,
                        left_pid,
                        child_pid,
                        generation,
                    )?;
                } else {
                    self.borrow_from_left_internal(
                        parent_pid,
                        child_slot - 1,
                        left_pid,
                        child_pid,
                        generation,
                    )?;
                }
                return Ok(false);
            }
        }
        // Can't borrow. Merge with whichever sibling exists (preferring
        // right so `child_pid` is the absorber and stays alive).
        if has_right {
            let right_pid = internal_child_at(self.buf.read(parent_pid)?, child_slot + 1);
            if child_is_leaf {
                self.merge_leaves(parent_pid, child_slot, child_pid, right_pid, generation)?;
            } else {
                self.merge_internals(parent_pid, child_slot, child_pid, right_pid, generation)?;
            }
            return Ok(true);
        }
        if has_left {
            let left_pid = internal_child_at(self.buf.read(parent_pid)?, child_slot - 1);
            if child_is_leaf {
                self.merge_leaves(parent_pid, child_slot - 1, left_pid, child_pid, generation)?;
            } else {
                self.merge_internals(parent_pid, child_slot - 1, left_pid, child_pid, generation)?;
            }
            return Ok(true);
        }
        // No siblings — parent has exactly 1 child (only possible when
        // parent is root). Nothing to rebalance here; caller handles
        // root collapse after the loop.
        Ok(false)
    }

    fn page_count(&mut self, pid: PageId, is_leaf: bool) -> Result<usize> {
        let page = self.buf.read(pid)?;
        Ok(if is_leaf {
            leaf_key_count(page)
        } else {
            internal_key_count(page)
        })
    }

    fn borrow_from_right_leaf(
        &mut self,
        parent_pid: PageId,
        pivot_slot: usize,
        left_pid: PageId,
        right_pid: PageId,
        generation: Lsn,
    ) -> Result<()> {
        let (k, v) = {
            let right = self.buf.read(right_pid)?;
            (leaf_key_at(right, 0), leaf_value_at(right, 0))
        };
        {
            let right = self.buf.modify(right_pid, generation)?;
            leaf_remove(right, 0)?;
        }
        {
            let left = self.buf.modify(left_pid, generation)?;
            let n = leaf_key_count(left);
            leaf_insert(left, n, k, &v)?;
        }
        let new_pivot = leaf_key_at(self.buf.read(right_pid)?, 0);
        let parent = self.buf.modify(parent_pid, generation)?;
        internal_set_key_at(parent, pivot_slot, new_pivot);
        Ok(())
    }

    fn borrow_from_left_leaf(
        &mut self,
        parent_pid: PageId,
        pivot_slot: usize,
        left_pid: PageId,
        right_pid: PageId,
        generation: Lsn,
    ) -> Result<()> {
        let (k, v) = {
            let left = self.buf.read(left_pid)?;
            let n = leaf_key_count(left);
            (leaf_key_at(left, n - 1), leaf_value_at(left, n - 1))
        };
        {
            let left = self.buf.modify(left_pid, generation)?;
            let n = leaf_key_count(left);
            leaf_remove(left, n - 1)?;
        }
        {
            let right = self.buf.modify(right_pid, generation)?;
            leaf_insert(right, 0, k, &v)?;
        }
        let parent = self.buf.modify(parent_pid, generation)?;
        internal_set_key_at(parent, pivot_slot, k);
        Ok(())
    }

    fn borrow_from_right_internal(
        &mut self,
        parent_pid: PageId,
        pivot_slot: usize,
        left_pid: PageId,
        right_pid: PageId,
        generation: Lsn,
    ) -> Result<()> {
        let (right_first_key, right_first_child) = {
            let right = self.buf.read(right_pid)?;
            (internal_key_at(right, 0), internal_child_at(right, 0))
        };
        let old_pivot = internal_key_at(self.buf.read(parent_pid)?, pivot_slot);
        {
            let left = self.buf.modify(left_pid, generation)?;
            let n = internal_key_count(left);
            internal_insert(left, n, old_pivot, right_first_child)?;
        }
        {
            let right = self.buf.modify(right_pid, generation)?;
            internal_pop_front(right)?;
        }
        let parent = self.buf.modify(parent_pid, generation)?;
        internal_set_key_at(parent, pivot_slot, right_first_key);
        Ok(())
    }

    fn borrow_from_left_internal(
        &mut self,
        parent_pid: PageId,
        pivot_slot: usize,
        left_pid: PageId,
        right_pid: PageId,
        generation: Lsn,
    ) -> Result<()> {
        let (left_last_key, left_last_child) = {
            let left = self.buf.read(left_pid)?;
            let n = internal_key_count(left);
            (internal_key_at(left, n - 1), internal_child_at(left, n))
        };
        let old_pivot = internal_key_at(self.buf.read(parent_pid)?, pivot_slot);
        {
            let left = self.buf.modify(left_pid, generation)?;
            let n = internal_key_count(left);
            internal_remove(left, n - 1)?;
        }
        {
            let right = self.buf.modify(right_pid, generation)?;
            internal_push_front(right, old_pivot, left_last_child)?;
        }
        let parent = self.buf.modify(parent_pid, generation)?;
        internal_set_key_at(parent, pivot_slot, left_last_key);
        Ok(())
    }

    fn merge_leaves(
        &mut self,
        parent_pid: PageId,
        pivot_slot: usize,
        left_pid: PageId,
        right_pid: PageId,
        generation: Lsn,
    ) -> Result<()> {
        let moved: Vec<(u64, L2pValue)> = {
            let right = self.buf.read(right_pid)?;
            (0..leaf_key_count(right))
                .map(|i| (leaf_key_at(right, i), leaf_value_at(right, i)))
                .collect()
        };
        {
            let left = self.buf.modify(left_pid, generation)?;
            let start = leaf_key_count(left);
            for (offset, (k, v)) in moved.into_iter().enumerate() {
                leaf_insert(left, start + offset, k, &v)?;
            }
        }
        {
            let parent = self.buf.modify(parent_pid, generation)?;
            internal_remove(parent, pivot_slot)?;
        }
        self.buf.free(right_pid, generation)?;
        Ok(())
    }

    fn merge_internals(
        &mut self,
        parent_pid: PageId,
        pivot_slot: usize,
        left_pid: PageId,
        right_pid: PageId,
        generation: Lsn,
    ) -> Result<()> {
        let pivot_key = internal_key_at(self.buf.read(parent_pid)?, pivot_slot);
        let (right_keys, right_children) = {
            let right = self.buf.read(right_pid)?;
            let n = internal_key_count(right);
            let keys: Vec<u64> = (0..n).map(|i| internal_key_at(right, i)).collect();
            let children: Vec<PageId> = (0..=n).map(|i| internal_child_at(right, i)).collect();
            (keys, children)
        };
        {
            let left = self.buf.modify(left_pid, generation)?;
            let left_n = internal_key_count(left);
            internal_insert(left, left_n, pivot_key, right_children[0])?;
            for (i, k) in right_keys.iter().enumerate() {
                let n_now = internal_key_count(left);
                internal_insert(left, n_now, *k, right_children[i + 1])?;
            }
        }
        {
            let parent = self.buf.modify(parent_pid, generation)?;
            internal_remove(parent, pivot_slot)?;
        }
        self.buf.free(right_pid, generation)?;
        Ok(())
    }

    fn maybe_collapse_root(&mut self, root_pid: PageId, generation: Lsn) -> Result<()> {
        let (should_collapse, new_root) = {
            let page = self.buf.read(root_pid)?;
            let header = page.header()?;
            if header.page_type == PageType::L2pInternal && internal_key_count(page) == 0 {
                (true, internal_child_at(page, 0))
            } else {
                (false, 0)
            }
        };
        if should_collapse {
            self.buf.free(root_pid, generation)?;
            self.root = new_root;
        }
        Ok(())
    }

    fn propagate_split_up(
        &mut self,
        path: &[(PageId, usize)],
        mut sep_key: u64,
        mut new_right: PageId,
        generation: Lsn,
    ) -> Result<()> {
        for &(parent_pid, child_slot) in path.iter().rev() {
            let has_space = {
                let parent = self.buf.read(parent_pid)?;
                internal_key_count(parent) < MAX_INTERNAL_KEYS
            };
            if has_space {
                let parent = self.buf.modify(parent_pid, generation)?;
                internal_insert(parent, child_slot, sep_key, new_right)?;
                return Ok(());
            }
            let (new_sep, new_right_parent) = split_internal_and_insert(
                &mut self.buf,
                parent_pid,
                child_slot,
                sep_key,
                new_right,
                generation,
            )?;
            sep_key = new_sep;
            new_right = new_right_parent;
        }

        // Path exhausted → root split. Build a new internal with
        // children [old_root, new_right] and a single separator key.
        let new_root = self.buf.alloc_internal(generation, self.root)?;
        let page = self.buf.modify(new_root, generation)?;
        internal_insert(page, 0, sep_key, new_right)?;
        self.root = new_root;
        Ok(())
    }

    // -------- range scan --------------------------------------------------

    /// Open an in-order iterator over entries whose keys match `range`.
    /// Holds a `&mut` borrow on `self` for its lifetime — any mutation
    /// of the tree is rejected at compile time while iteration is
    /// active.
    ///
    /// `range` accepts every [`RangeBounds<u64>`] type: `..`, `a..b`,
    /// `a..=b`, `..b`, `..=b`, `a..`. Unbounded on either side is
    /// valid.
    pub fn range<R: RangeBounds<u64>>(&mut self, range: R) -> Result<RangeIter<'_>> {
        RangeIter::start(self, range)
    }

    // -------- diagnostics (public for tests) ------------------------------

    /// Tree depth: 1 if the root is a leaf, 2 if the root is an
    /// internal pointing at leaves, and so on.
    pub fn depth(&mut self) -> Result<usize> {
        let mut depth = 0usize;
        let mut current = self.root;
        loop {
            depth += 1;
            let header = self.buf.read(current)?.header()?;
            match header.page_type {
                PageType::L2pLeaf => return Ok(depth),
                PageType::L2pInternal => {
                    current = internal_child_at(self.buf.read(current)?, 0);
                }
                other => {
                    return Err(MetaDbError::Corruption(format!(
                        "unexpected type {other:?}"
                    )));
                }
            }
        }
    }

    /// Number of (key, value) entries in the tree. Walks every leaf.
    /// O(keys); intended for tests and verification, not hot paths.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&mut self) -> Result<usize> {
        let mut total = 0usize;
        let leaves = self.collect_leaf_ids()?;
        for pid in leaves {
            total += leaf_key_count(self.buf.read(pid)?);
        }
        Ok(total)
    }

    fn collect_leaf_ids(&mut self) -> Result<Vec<PageId>> {
        let mut leaves = Vec::new();
        let mut stack = vec![self.root];
        while let Some(pid) = stack.pop() {
            let page = self.buf.read(pid)?;
            let header = page.header()?;
            match header.page_type {
                PageType::L2pLeaf => leaves.push(pid),
                PageType::L2pInternal => {
                    let n = internal_key_count(page);
                    for i in (0..=n).rev() {
                        stack.push(internal_child_at(page, i));
                    }
                }
                other => {
                    return Err(MetaDbError::Corruption(format!(
                        "unexpected type {other:?}"
                    )));
                }
            }
        }
        Ok(leaves)
    }

    /// Diagnostic accessor: number of pages currently cached.
    pub fn cached_pages(&self) -> usize {
        self.buf.len()
    }

    /// Run the structural invariant checker. Returns `Err(Corruption)`
    /// on violation. Intended for tests and the `metadb-verify` tool;
    /// calling this on the hot path defeats the point.
    pub fn check_invariants(&mut self) -> Result<()> {
        crate::btree::invariants::check_tree(&mut self.buf, self.root)
    }
}

enum GetProbe {
    Hit(L2pValue),
    Miss,
    Descend(PageId),
}

// -------- range iterator --------------------------------------------------

/// Ascending iterator over (key, value) pairs within a range.
///
/// Traversal is stack-based over the tree with no sibling pointers:
/// each stack frame tracks an internal ancestor and the next child
/// slot to visit when the current subtree is exhausted. Pages are
/// read through the BTree's `PageBuf`; no entries are copied until
/// the iterator yields them.
///
/// Errors are surfaced via the `Result<Item, _>` payload; the
/// iterator stops after the first error.
pub struct RangeIter<'a> {
    tree: &'a mut BTree,
    stack: Vec<StackFrame>,
    leaf: Option<LeafCursor>,
    upper: Bound<u64>,
    done: bool,
}

#[derive(Copy, Clone, Debug)]
struct StackFrame {
    pid: PageId,
    next_child_slot: usize,
}

#[derive(Copy, Clone, Debug)]
struct LeafCursor {
    pid: PageId,
    pos: usize,
    count: usize,
}

impl<'a> RangeIter<'a> {
    fn start<R: RangeBounds<u64>>(tree: &'a mut BTree, range: R) -> Result<Self> {
        // Copy the raw Bound<&u64> out so we own the endpoints without
        // a borrow on `range`.
        let lower = cloned_bound(range.start_bound());
        let upper = cloned_bound(range.end_bound());

        // Effective key to descend on: Excluded(k) and Included(k)
        // both descend on k; Unbounded descends on 0 (leftmost).
        let start_key = match lower {
            Bound::Included(k) | Bound::Excluded(k) => k,
            Bound::Unbounded => 0,
        };

        let mut stack = Vec::new();
        let mut current = tree.root;

        let leaf = loop {
            let header = tree.buf.read(current)?.header()?;
            match header.page_type {
                PageType::L2pLeaf => {
                    let page = tree.buf.read(current)?;
                    let count = leaf_key_count(page);
                    let mut pos = match leaf_search(page, start_key) {
                        Ok(i) => i,
                        Err(i) => i,
                    };
                    if matches!(lower, Bound::Excluded(_))
                        && pos < count
                        && leaf_key_at(page, pos) == start_key
                    {
                        pos += 1;
                    }
                    break LeafCursor {
                        pid: current,
                        pos,
                        count,
                    };
                }
                PageType::L2pInternal => {
                    let page = tree.buf.read(current)?;
                    let slot = internal_search(page, start_key);
                    let child = internal_child_at(page, slot);
                    stack.push(StackFrame {
                        pid: current,
                        next_child_slot: slot + 1,
                    });
                    current = child;
                }
                other => {
                    return Err(MetaDbError::Corruption(format!(
                        "unexpected page type {other:?} at {current}",
                    )));
                }
            }
        };

        Ok(Self {
            tree,
            stack,
            leaf: Some(leaf),
            upper,
            done: false,
        })
    }

    fn advance_to_next_leaf(&mut self) -> Result<bool> {
        while let Some(frame) = self.stack.last().copied() {
            let n_children = internal_key_count(self.tree.buf.read(frame.pid)?) + 1;
            if frame.next_child_slot < n_children {
                let child =
                    internal_child_at(self.tree.buf.read(frame.pid)?, frame.next_child_slot);
                self.stack.last_mut().unwrap().next_child_slot += 1;
                self.descend_leftmost(child)?;
                return Ok(true);
            }
            self.stack.pop();
        }
        Ok(false)
    }

    fn descend_leftmost(&mut self, start: PageId) -> Result<()> {
        let mut current = start;
        loop {
            let header = self.tree.buf.read(current)?.header()?;
            match header.page_type {
                PageType::L2pLeaf => {
                    let count = leaf_key_count(self.tree.buf.read(current)?);
                    self.leaf = Some(LeafCursor {
                        pid: current,
                        pos: 0,
                        count,
                    });
                    return Ok(());
                }
                PageType::L2pInternal => {
                    let child = internal_child_at(self.tree.buf.read(current)?, 0);
                    self.stack.push(StackFrame {
                        pid: current,
                        next_child_slot: 1,
                    });
                    current = child;
                }
                other => {
                    return Err(MetaDbError::Corruption(format!(
                        "unexpected page type {other:?} at {current}",
                    )));
                }
            }
        }
    }
}

fn cloned_bound(b: Bound<&u64>) -> Bound<u64> {
    match b {
        Bound::Included(k) => Bound::Included(*k),
        Bound::Excluded(k) => Bound::Excluded(*k),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl<'a> Iterator for RangeIter<'a> {
    type Item = Result<(u64, L2pValue)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        loop {
            if let Some(leaf) = self.leaf {
                if leaf.pos < leaf.count {
                    let entry = {
                        let page = match self.tree.buf.read(leaf.pid) {
                            Ok(p) => p,
                            Err(e) => {
                                self.done = true;
                                return Some(Err(e));
                            }
                        };
                        (leaf_key_at(page, leaf.pos), leaf_value_at(page, leaf.pos))
                    };
                    let past_upper = match self.upper {
                        Bound::Included(u) => entry.0 > u,
                        Bound::Excluded(u) => entry.0 >= u,
                        Bound::Unbounded => false,
                    };
                    if past_upper {
                        self.done = true;
                        return None;
                    }
                    self.leaf = Some(LeafCursor {
                        pos: leaf.pos + 1,
                        ..leaf
                    });
                    return Some(Ok(entry));
                }
                self.leaf = None;
            }
            match self.advance_to_next_leaf() {
                Ok(true) => continue,
                Ok(false) => {
                    self.done = true;
                    return None;
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}

enum Action {
    Update {
        pos: usize,
        old: L2pValue,
    },
    Insert {
        pos: usize,
    },
    Split {
        pos: usize,
    },
    Descend {
        child_pid: PageId,
        child_slot: usize,
    },
}

enum DeleteAction {
    Miss,
    Remove { pos: usize, old: L2pValue },
    Descend { child: PageId, slot: usize },
}

// -------- split helpers (module-private) ----------------------------------

/// Split a full leaf at `left_pid` into (left, new_right) and insert
/// `(key, value)` at `pos` in the combined (pre-split + one) list.
///
/// Returns `(separator_key, new_right_page_id)` so the caller can
/// insert the new separator into the parent (or create a new root).
fn split_leaf_and_insert(
    buf: &mut PageBuf,
    left_pid: PageId,
    pos: usize,
    key: u64,
    value: &L2pValue,
    generation: Lsn,
) -> Result<(u64, PageId)> {
    const SPLIT_POINT: usize = MAX_LEAF_ENTRIES / 2;

    // Collect the tail entries to move to the right sibling. Must
    // snapshot before we start mutating `left_pid`.
    let moved: Vec<(u64, L2pValue)> = {
        let left = buf.read(left_pid)?;
        debug_assert!(leaf_key_count(left) == MAX_LEAF_ENTRIES);
        (SPLIT_POINT..MAX_LEAF_ENTRIES)
            .map(|i| (leaf_key_at(left, i), leaf_value_at(left, i)))
            .collect()
    };

    // Allocate the new right leaf and populate it.
    let right_pid = buf.alloc_leaf(generation)?;
    {
        let right = buf.modify(right_pid, generation)?;
        for (i, (k, v)) in moved.iter().enumerate() {
            leaf_insert(right, i, *k, v)?;
        }
    }

    // Shrink left: zero the vacated tail so the CRC reflects the
    // logical state.
    {
        let left = buf.modify(left_pid, generation)?;
        left.set_key_count(SPLIT_POINT as u16);
        let zero_off = SPLIT_POINT * LEAF_ENTRY_SIZE;
        left.payload_mut()[zero_off..].fill(0);
    }

    // Place the new entry on whichever side it belongs.
    if pos <= SPLIT_POINT {
        let left = buf.modify(left_pid, generation)?;
        leaf_insert(left, pos, key, value)?;
    } else {
        let right = buf.modify(right_pid, generation)?;
        leaf_insert(right, pos - SPLIT_POINT, key, value)?;
    }

    let sep_key = {
        let right = buf.read(right_pid)?;
        leaf_key_at(right, 0)
    };
    Ok((sep_key, right_pid))
}

/// Split a full internal at `left_pid`, inserting `(new_key,
/// new_right_child)` at position `pos` in the combined list.
///
/// The middle key of the combined list is *promoted* — returned as
/// `sep_key` for the caller to insert into the grandparent. Left
/// keeps the lower half of keys/children; the new right internal gets
/// the upper half.
fn split_internal_and_insert(
    buf: &mut PageBuf,
    left_pid: PageId,
    pos: usize,
    new_key: u64,
    new_right_child: PageId,
    generation: Lsn,
) -> Result<(u64, PageId)> {
    // Snapshot the full keys + children arrays of the original page.
    let (mut keys, mut children) = {
        let page = buf.read(left_pid)?;
        debug_assert!(internal_key_count(page) == MAX_INTERNAL_KEYS);
        let keys: Vec<u64> = (0..MAX_INTERNAL_KEYS)
            .map(|i| internal_key_at(page, i))
            .collect();
        let children: Vec<PageId> = (0..=MAX_INTERNAL_KEYS)
            .map(|i| internal_child_at(page, i))
            .collect();
        (keys, children)
    };

    // Virtual insert at `pos`.
    keys.insert(pos, new_key);
    children.insert(pos + 1, new_right_child);
    // keys now has MAX_INTERNAL_KEYS + 1 entries; children has +2.

    let total_keys = keys.len();
    let split_idx = total_keys / 2;

    // Populate the right page with keys[split_idx+1..] and
    // children[split_idx+1..].
    let right_pid = buf.alloc_internal(generation, children[split_idx + 1])?;
    {
        let right = buf.modify(right_pid, generation)?;
        for i in (split_idx + 1)..total_keys {
            let k = keys[i];
            let c = children[i + 1];
            internal_insert(right, i - split_idx - 1, k, c)?;
        }
    }

    // Rewrite left with keys[0..split_idx] and children[0..split_idx+1].
    {
        let left = buf.modify(left_pid, generation)?;
        // Zero the payload region so the page holds only the new
        // content; the header stays (modify just bumped generation +
        // dirty flag).
        left.payload_mut().fill(0);
        left.set_key_count(0);
        internal_set_first_child(left, children[0]);
        for i in 0..split_idx {
            internal_insert(left, i, keys[i], children[i + 1])?;
        }
    }

    // Promoted key is keys[split_idx], not placed in either side.
    Ok((keys[split_idx], right_pid))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn mk_tree() -> (TempDir, BTree) {
        let dir = TempDir::new().unwrap();
        let ps = Arc::new(PageStore::create(dir.path().join("p.onyx_meta")).unwrap());
        let tree = BTree::create(ps).unwrap();
        (dir, tree)
    }

    fn v(n: u8) -> L2pValue {
        let mut x = [0u8; 28];
        x[0] = n;
        L2pValue(x)
    }

    // -------- basic get/insert ----

    #[test]
    fn fresh_tree_is_empty() {
        let (_d, mut t) = mk_tree();
        assert_eq!(t.get(42).unwrap(), None);
        assert_eq!(t.depth().unwrap(), 1);
        assert_eq!(t.len().unwrap(), 0);
    }

    #[test]
    fn insert_get_round_trip_single() {
        let (_d, mut t) = mk_tree();
        assert_eq!(t.insert(42, v(7)).unwrap(), None);
        assert_eq!(t.get(42).unwrap(), Some(v(7)));
        assert_eq!(t.get(41).unwrap(), None);
        assert_eq!(t.get(43).unwrap(), None);
        assert_eq!(t.len().unwrap(), 1);
    }

    #[test]
    fn insert_returns_old_value_on_update() {
        let (_d, mut t) = mk_tree();
        assert_eq!(t.insert(10, v(1)).unwrap(), None);
        assert_eq!(t.insert(10, v(2)).unwrap(), Some(v(1)));
        assert_eq!(t.get(10).unwrap(), Some(v(2)));
        assert_eq!(t.len().unwrap(), 1);
    }

    #[test]
    fn insert_many_within_single_leaf() {
        let (_d, mut t) = mk_tree();
        for i in 0u64..MAX_LEAF_ENTRIES as u64 {
            assert_eq!(t.insert(i, v(i as u8)).unwrap(), None);
        }
        assert_eq!(t.depth().unwrap(), 1);
        assert_eq!(t.len().unwrap(), MAX_LEAF_ENTRIES);
        for i in 0u64..MAX_LEAF_ENTRIES as u64 {
            assert_eq!(t.get(i).unwrap(), Some(v(i as u8)));
        }
    }

    // -------- split --------------

    #[test]
    fn leaf_split_triggers_on_overflow() {
        let (_d, mut t) = mk_tree();
        for i in 0u64..(MAX_LEAF_ENTRIES as u64) {
            t.insert(i, v(i as u8)).unwrap();
        }
        assert_eq!(t.depth().unwrap(), 1);
        // One more insert must split the leaf and make the root internal.
        t.insert(MAX_LEAF_ENTRIES as u64, v(MAX_LEAF_ENTRIES as u8))
            .unwrap();
        assert_eq!(t.depth().unwrap(), 2);
        assert_eq!(t.len().unwrap(), MAX_LEAF_ENTRIES + 1);
        // Every key is still retrievable.
        for i in 0u64..=(MAX_LEAF_ENTRIES as u64) {
            assert_eq!(t.get(i).unwrap(), Some(v(i as u8)));
        }
    }

    #[test]
    fn split_keeps_all_keys_for_reverse_insert() {
        let (_d, mut t) = mk_tree();
        // Insert in reverse order so the split logic is exercised at
        // both ends of the leaf.
        for i in (0..(MAX_LEAF_ENTRIES + 50)).rev() {
            t.insert(i as u64, v(i as u8)).unwrap();
        }
        for i in 0..(MAX_LEAF_ENTRIES + 50) {
            assert_eq!(t.get(i as u64).unwrap(), Some(v(i as u8)));
        }
    }

    #[test]
    fn split_keeps_all_keys_for_random_insert() {
        use rand::SeedableRng;
        use rand::seq::SliceRandom;
        use rand_chacha::ChaCha8Rng;

        let (_d, mut t) = mk_tree();
        let mut keys: Vec<u64> = (0..(MAX_LEAF_ENTRIES as u64 * 3)).collect();
        let mut rng = ChaCha8Rng::seed_from_u64(12345);
        keys.shuffle(&mut rng);
        for &k in &keys {
            t.insert(k, v((k & 0xFF) as u8)).unwrap();
        }
        assert!(t.depth().unwrap() >= 2);
        for &k in &keys {
            assert_eq!(t.get(k).unwrap(), Some(v((k & 0xFF) as u8)));
        }
    }

    #[test]
    fn tree_grows_past_two_levels() {
        let (_d, mut t) = mk_tree();
        // Enough keys to force internal splits. 251 separator keys
        // per internal means ~252 leaves before the root internal is
        // full. Push beyond that.
        const N: u64 = 251 * 113 + 7;
        for i in 0..N {
            t.insert(i, v(0)).unwrap();
        }
        let depth = t.depth().unwrap();
        assert!(depth >= 3, "expected depth >= 3, got {depth}");
        assert_eq!(t.len().unwrap(), N as usize);
        // Spot-check a handful.
        for k in [0u64, 1, N / 2, N - 1] {
            assert!(t.get(k).unwrap().is_some(), "missing {k}");
        }
    }

    // -------- persistence --------

    #[test]
    fn flush_and_reopen_preserves_keys() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("p.onyx_meta");
        let root = {
            let ps = Arc::new(PageStore::create(&path).unwrap());
            let mut t = BTree::create(ps).unwrap();
            for i in 0u64..(MAX_LEAF_ENTRIES as u64 + 100) {
                t.insert(i, v((i & 0xFF) as u8)).unwrap();
            }
            t.flush().unwrap();
            t.root()
        };
        let ps = Arc::new(PageStore::open(&path).unwrap());
        let mut t = BTree::open(ps, root, 999).unwrap();
        for i in 0u64..(MAX_LEAF_ENTRIES as u64 + 100) {
            assert_eq!(t.get(i).unwrap(), Some(v((i & 0xFF) as u8)));
        }
    }

    #[test]
    fn root_reassigned_after_split() {
        let (_d, mut t) = mk_tree();
        let initial_root = t.root();
        for i in 0u64..(MAX_LEAF_ENTRIES as u64 + 1) {
            t.insert(i, v(0)).unwrap();
        }
        assert_ne!(t.root(), initial_root, "root must change after split");
    }

    #[test]
    fn generation_counter_monotonic() {
        let (_d, mut t) = mk_tree();
        let g0 = t.next_generation();
        t.insert(1, v(0)).unwrap();
        assert!(t.next_generation() > g0);
        let g1 = t.next_generation();
        t.insert(2, v(0)).unwrap();
        assert!(t.next_generation() > g1);
    }

    #[test]
    fn update_does_not_split_or_grow() {
        let (_d, mut t) = mk_tree();
        for i in 0u64..(MAX_LEAF_ENTRIES as u64) {
            t.insert(i, v(1)).unwrap();
        }
        let before_pages = t.cached_pages();
        for i in 0u64..(MAX_LEAF_ENTRIES as u64) {
            t.insert(i, v(2)).unwrap();
        }
        assert_eq!(t.depth().unwrap(), 1);
        // Updates only touch the single root leaf.
        assert_eq!(t.cached_pages(), before_pages);
    }

    // -------- delete --------

    #[test]
    fn delete_miss_returns_none() {
        let (_d, mut t) = mk_tree();
        assert_eq!(t.delete(42).unwrap(), None);
        t.insert(1, v(1)).unwrap();
        assert_eq!(t.delete(42).unwrap(), None);
        assert_eq!(t.get(1).unwrap(), Some(v(1)));
    }

    #[test]
    fn delete_hit_returns_old_value() {
        let (_d, mut t) = mk_tree();
        t.insert(10, v(7)).unwrap();
        assert_eq!(t.delete(10).unwrap(), Some(v(7)));
        assert_eq!(t.get(10).unwrap(), None);
        assert_eq!(t.len().unwrap(), 0);
    }

    #[test]
    fn delete_all_from_single_leaf() {
        let (_d, mut t) = mk_tree();
        for i in 0u64..(MAX_LEAF_ENTRIES as u64) {
            t.insert(i, v(i as u8)).unwrap();
        }
        for i in 0u64..(MAX_LEAF_ENTRIES as u64) {
            assert_eq!(t.delete(i).unwrap(), Some(v(i as u8)));
        }
        assert_eq!(t.len().unwrap(), 0);
        assert_eq!(t.depth().unwrap(), 1);
    }

    #[test]
    fn delete_triggers_leaf_merge_and_root_collapse() {
        let (_d, mut t) = mk_tree();
        let n = MAX_LEAF_ENTRIES as u64 + 5;
        for i in 0u64..n {
            t.insert(i, v(0)).unwrap();
        }
        assert_eq!(t.depth().unwrap(), 2);
        // Delete enough keys to make the second leaf underflow and
        // merge back; the tree should collapse to depth 1.
        for i in 0u64..n {
            t.delete(i).unwrap();
        }
        assert_eq!(t.len().unwrap(), 0);
        assert_eq!(t.depth().unwrap(), 1);
    }

    #[test]
    fn delete_exercises_borrow_path() {
        let (_d, mut t) = mk_tree();
        // Fill the tree so we have at least 3 leaves under one parent.
        let n = (MAX_LEAF_ENTRIES as u64) * 3;
        for i in 0u64..n {
            t.insert(i, v(0)).unwrap();
        }
        assert!(t.depth().unwrap() >= 2);
        // Delete a few keys from the middle leaf until it would
        // underflow, forcing a borrow from a sibling.
        for i in (MAX_LEAF_ENTRIES as u64)..(MAX_LEAF_ENTRIES as u64 + 60) {
            t.delete(i).unwrap();
        }
        // All remaining keys still retrievable.
        for i in 0u64..(MAX_LEAF_ENTRIES as u64) {
            assert_eq!(t.get(i).unwrap(), Some(v(0)));
        }
        for i in (MAX_LEAF_ENTRIES as u64 + 60)..n {
            assert_eq!(t.get(i).unwrap(), Some(v(0)));
        }
    }

    #[test]
    fn insert_delete_insert_round_trip_seeded() {
        use rand::SeedableRng;
        use rand::seq::SliceRandom;
        use rand_chacha::ChaCha8Rng;

        let (_d, mut t) = mk_tree();
        let n: u64 = 5000;
        let mut keys: Vec<u64> = (0..n).collect();
        let mut rng = ChaCha8Rng::seed_from_u64(999);
        keys.shuffle(&mut rng);
        for &k in &keys {
            t.insert(k, v((k & 0xFF) as u8)).unwrap();
        }
        keys.shuffle(&mut rng);
        // Delete the first half.
        for &k in &keys[..(keys.len() / 2)] {
            assert_eq!(t.delete(k).unwrap(), Some(v((k & 0xFF) as u8)));
        }
        // The second half is still retrievable.
        for &k in &keys[(keys.len() / 2)..] {
            assert_eq!(t.get(k).unwrap(), Some(v((k & 0xFF) as u8)));
        }
        // The first half misses.
        for &k in &keys[..(keys.len() / 2)] {
            assert_eq!(t.get(k).unwrap(), None);
        }
        assert_eq!(t.len().unwrap(), keys.len() / 2);
    }

    #[test]
    fn full_drain_returns_empty_tree_with_root_leaf() {
        let (_d, mut t) = mk_tree();
        let n = 2000u64;
        for i in 0..n {
            t.insert(i, v(0)).unwrap();
        }
        for i in 0..n {
            t.delete(i).unwrap();
        }
        assert_eq!(t.len().unwrap(), 0);
        assert_eq!(t.depth().unwrap(), 1);
        // And we can still insert fresh keys.
        t.insert(42, v(1)).unwrap();
        assert_eq!(t.get(42).unwrap(), Some(v(1)));
    }

    #[test]
    fn reopen_after_deletes_preserves_state() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("p.onyx_meta");
        let root = {
            let ps = Arc::new(PageStore::create(&path).unwrap());
            let mut t = BTree::create(ps).unwrap();
            for i in 0u64..500 {
                t.insert(i, v(0)).unwrap();
            }
            for i in 0u64..300 {
                t.delete(i).unwrap();
            }
            t.flush().unwrap();
            t.root()
        };
        let ps = Arc::new(PageStore::open(&path).unwrap());
        let mut t = BTree::open(ps, root, 9999).unwrap();
        assert_eq!(t.len().unwrap(), 200);
        for i in 0u64..300 {
            assert_eq!(t.get(i).unwrap(), None);
        }
        for i in 300u64..500 {
            assert_eq!(t.get(i).unwrap(), Some(v(0)));
        }
    }

    // -------- range --------

    fn collect_range<R: RangeBounds<u64>>(t: &mut BTree, range: R) -> Vec<(u64, L2pValue)> {
        t.range(range).unwrap().collect::<Result<Vec<_>>>().unwrap()
    }

    #[test]
    fn range_on_empty_tree() {
        let (_d, mut t) = mk_tree();
        assert_eq!(collect_range(&mut t, ..), vec![]);
        assert_eq!(collect_range(&mut t, 0..100), vec![]);
    }

    #[test]
    fn range_full_single_leaf() {
        let (_d, mut t) = mk_tree();
        for i in 0u64..10 {
            t.insert(i, v(i as u8)).unwrap();
        }
        let got = collect_range(&mut t, ..);
        assert_eq!(got.len(), 10);
        for (i, (k, val)) in got.iter().enumerate() {
            assert_eq!(*k, i as u64);
            assert_eq!(*val, v(i as u8));
        }
    }

    #[test]
    fn range_bounded_inclusive() {
        let (_d, mut t) = mk_tree();
        for i in 0u64..20 {
            t.insert(i, v(0)).unwrap();
        }
        let ks: Vec<u64> = collect_range(&mut t, 5u64..=10)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(ks, vec![5, 6, 7, 8, 9, 10]);
    }

    #[test]
    fn range_bounded_exclusive() {
        let (_d, mut t) = mk_tree();
        for i in 0u64..20 {
            t.insert(i, v(0)).unwrap();
        }
        let ks: Vec<u64> = collect_range(&mut t, 5u64..10)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(ks, vec![5, 6, 7, 8, 9]);
    }

    #[test]
    fn range_lower_excluded() {
        use std::ops::Bound::{Excluded, Unbounded};
        let (_d, mut t) = mk_tree();
        for i in 0u64..10 {
            t.insert(i, v(0)).unwrap();
        }
        let ks: Vec<u64> = t
            .range((Excluded(5u64), Unbounded))
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(ks, vec![6, 7, 8, 9]);
    }

    #[test]
    fn range_unbounded_upper() {
        let (_d, mut t) = mk_tree();
        for i in 0u64..10 {
            t.insert(i, v(0)).unwrap();
        }
        let ks: Vec<u64> = collect_range(&mut t, 7u64..)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(ks, vec![7, 8, 9]);
    }

    #[test]
    fn range_unbounded_lower() {
        let (_d, mut t) = mk_tree();
        for i in 0u64..10 {
            t.insert(i, v(0)).unwrap();
        }
        let ks: Vec<u64> = collect_range(&mut t, ..4u64)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(ks, vec![0, 1, 2, 3]);
    }

    #[test]
    fn range_spans_multiple_leaves() {
        let (_d, mut t) = mk_tree();
        let n = (MAX_LEAF_ENTRIES as u64) * 4;
        for i in 0..n {
            t.insert(i, v((i & 0xFF) as u8)).unwrap();
        }
        assert!(t.depth().unwrap() >= 2);
        let got = collect_range(&mut t, ..);
        assert_eq!(got.len(), n as usize);
        for i in 0..n {
            assert_eq!(got[i as usize].0, i);
        }
    }

    #[test]
    fn range_spans_multiple_levels_with_bounds() {
        let (_d, mut t) = mk_tree();
        let n = (MAX_LEAF_ENTRIES as u64) * 5;
        for i in 0..n {
            t.insert(i, v(0)).unwrap();
        }
        let lo = (MAX_LEAF_ENTRIES as u64) + 50;
        let hi = lo + 200;
        let ks: Vec<u64> = collect_range(&mut t, lo..hi)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(ks, (lo..hi).collect::<Vec<_>>());
    }

    #[test]
    fn range_empty_range_yields_nothing() {
        let (_d, mut t) = mk_tree();
        for i in 0u64..10 {
            t.insert(i, v(0)).unwrap();
        }
        // Reversed bounds → empty.
        assert_eq!(collect_range(&mut t, 5u64..5).len(), 0);
        // Entirely past-the-end.
        assert_eq!(collect_range(&mut t, 100u64..200).len(), 0);
    }

    #[test]
    fn range_after_deletes() {
        let (_d, mut t) = mk_tree();
        for i in 0u64..200 {
            t.insert(i, v(0)).unwrap();
        }
        for i in 0u64..200 {
            if i % 3 == 0 {
                t.delete(i).unwrap();
            }
        }
        let ks: Vec<u64> = collect_range(&mut t, ..)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        let expected: Vec<u64> = (0u64..200).filter(|i| i % 3 != 0).collect();
        assert_eq!(ks, expected);
    }

    #[test]
    fn range_exact_match_on_boundary() {
        let (_d, mut t) = mk_tree();
        for i in [10u64, 20, 30, 40, 50] {
            t.insert(i, v(0)).unwrap();
        }
        // Included lower matches exactly.
        let ks: Vec<u64> = collect_range(&mut t, 20u64..=40)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(ks, vec![20, 30, 40]);
        // Excluded upper drops exact match.
        let ks: Vec<u64> = collect_range(&mut t, 20u64..40)
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(ks, vec![20, 30]);
    }

    #[test]
    fn open_rejects_non_btree_root() {
        // Open a page store and point BTree::open at a manifest slot —
        // clearly not a btree page.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("p.onyx_meta");
        {
            let _ = PageStore::create(&path).unwrap();
        }
        let ps = Arc::new(PageStore::open(&path).unwrap());
        assert!(BTree::open(ps, 0, 1).is_err());
    }
}
