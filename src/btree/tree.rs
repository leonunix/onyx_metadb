//! Refcount B+tree: single-writer, in-place, no snapshots.
//!
//! Keys are pbas; values are [`RcEntry`] (rc + birth_lsn). Snapshots
//! are an L2P concept, and L2P now lives in [`crate::paged`], so this
//! tree does not need COW, per-page refcounts, or `drop_subtree`.
//! Writes mutate pages in place; splits and merges run the standard
//! B+tree way.
//!
//! Concurrency: `BTree` is `!Sync` in practice — its `PageBuf` is
//! `&mut self` only. `Db` wraps one per shard in a `Mutex`.

use std::collections::HashSet;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use crate::btree::cache::PageBuf;
use crate::btree::format::{
    LEAF_ENTRY_SIZE, MAX_INTERNAL_KEYS, MAX_LEAF_ENTRIES, RcEntry, internal_child_at,
    internal_insert, internal_key_at, internal_key_count, internal_pop_front, internal_push_front,
    internal_remove, internal_search, internal_set_child, internal_set_first_child,
    internal_set_key_at, leaf_insert, leaf_key_at, leaf_key_count, leaf_remove, leaf_search,
    leaf_set_entry, leaf_value_at,
};
use crate::cache::{DEFAULT_PAGE_CACHE_BYTES, PageCache};
use crate::error::{MetaDbError, Result};
use crate::page::PageType;
use crate::page_store::PageStore;
use crate::types::{Lsn, PageId};

/// Minimum leaf fill before a delete triggers a borrow-or-merge.
const LEAF_UNDERFLOW_THRESHOLD: usize = MAX_LEAF_ENTRIES / 2;

/// Minimum internal fill before a delete triggers a borrow-or-merge.
const INTERNAL_UNDERFLOW_THRESHOLD: usize = MAX_INTERNAL_KEYS / 2;

/// Single-shard refcount B+tree.
pub struct BTree {
    buf: PageBuf,
    root: PageId,
    next_gen: Lsn,
    private_pages: HashSet<PageId>,
    retired_pages: HashSet<PageId>,
}

impl BTree {
    fn finish_op<T>(&mut self, result: Result<T>) -> Result<T> {
        self.buf.evict_clean_pages();
        result
    }

    /// Fresh empty tree on `page_store`. Allocates one empty leaf as
    /// the root and persists it.
    pub fn create(page_store: Arc<PageStore>) -> Result<Self> {
        let page_cache = Arc::new(PageCache::new(page_store.clone(), DEFAULT_PAGE_CACHE_BYTES));
        Self::create_with_cache(page_store, page_cache)
    }

    /// Fresh empty tree sharing an existing page cache.
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
            next_gen: 2,
            private_pages: HashSet::new(),
            retired_pages: HashSet::new(),
        })
    }

    /// Reattach to an existing tree whose root is at `root`. `next_gen`
    /// is the generation stamp subsequent mutations will use.
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
        let buf = PageBuf::with_cache(page_store, page_cache);
        Ok(Self {
            buf,
            root,
            next_gen,
            private_pages: HashSet::new(),
            retired_pages: HashSet::new(),
        })
    }

    /// Current root page id.
    pub fn root(&self) -> PageId {
        self.root
    }

    /// Next unused generation stamp.
    pub fn next_generation(&self) -> Lsn {
        self.next_gen
    }

    /// Bump the generation counter if `lsn` has advanced past it.
    /// Called by `Db` after a commit to keep page stamps monotonic.
    pub fn advance_next_gen(&mut self, lsn: Lsn) {
        if lsn >= self.next_gen {
            self.next_gen = lsn + 1;
        }
    }

    /// Underlying page store handle.
    pub fn page_store(&self) -> &Arc<PageStore> {
        self.buf.page_store()
    }

    fn alloc_leaf_private(&mut self, generation: Lsn) -> Result<PageId> {
        let pid = self.buf.alloc_leaf(generation)?;
        self.private_pages.insert(pid);
        Ok(pid)
    }

    fn alloc_internal_private(&mut self, generation: Lsn, first_child: PageId) -> Result<PageId> {
        let pid = self.buf.alloc_internal(generation, first_child)?;
        self.private_pages.insert(pid);
        Ok(pid)
    }

    fn ensure_private(&mut self, pid: PageId, generation: Lsn) -> Result<PageId> {
        if self.private_pages.contains(&pid) {
            return Ok(pid);
        }
        let new_pid = self.buf.clone_private(pid, generation)?;
        self.private_pages.insert(new_pid);
        self.retired_pages.insert(pid);
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

    fn make_path_private(
        &mut self,
        path: &[(PageId, usize, PageId)],
        generation: Lsn,
    ) -> Result<(PageId, Vec<(PageId, usize)>)> {
        let mut current = self.ensure_private(self.root, generation)?;
        self.root = current;
        let mut private_path = Vec::with_capacity(path.len());
        for &(_, slot, child) in path {
            let private_child = self.ensure_private(child, generation)?;
            internal_set_child(self.buf.modify(current, generation)?, slot, private_child);
            private_path.push((current, slot));
            current = private_child;
        }
        Ok((current, private_path))
    }

    /// Persist every dirty page. Must be called before committing a
    /// new root pointer to the manifest.
    pub fn flush(&mut self) -> Result<()> {
        let result = self.buf.flush();
        self.finish_op(result)
    }

    /// Called after the manifest that points at this tree's current
    /// root is durable. Pages copied away during the checkpoint window
    /// can now be released; on crash before this point they remain
    /// reachable from the old manifest or are reclaimed as orphans at
    /// next open.
    pub fn checkpoint_committed(&mut self, generation: Lsn) -> Result<()> {
        let retired: Vec<PageId> = self.retired_pages.iter().copied().collect();
        for pid in retired {
            self.buf.free(pid, generation)?;
            self.retired_pages.remove(&pid);
        }
        self.private_pages.clear();
        self.finish_op(Ok(()))
    }

    // -------- read path --------------------------------------------------

    /// Point lookup. `None` if `key` is not present.
    pub fn get(&mut self, key: u64) -> Result<Option<RcEntry>> {
        let result = (|| {
            let mut current = self.root;
            loop {
                let page = self.buf.read(current)?;
                match page.header()?.page_type {
                    PageType::L2pLeaf => {
                        return Ok(match leaf_search(page, key) {
                            Ok(i) => Some(leaf_value_at(page, i)),
                            Err(_) => None,
                        });
                    }
                    PageType::L2pInternal => {
                        let slot = internal_search(page, key);
                        let child = internal_child_at(page, slot);
                        current = child;
                    }
                    other => {
                        return Err(MetaDbError::Corruption(format!(
                            "btree::get: unexpected page type {other:?} at {current}"
                        )));
                    }
                }
            }
        })();
        self.finish_op(result)
    }

    /// Range scan. Materialises the matching (key, value) pairs up
    /// front — range over refcount is an admin / debug tool, not a hot
    /// path.
    pub fn range<R: RangeBounds<u64>>(&mut self, range: R) -> Result<RangeIter> {
        let lo = bound_lower(range.start_bound());
        let hi = bound_upper(range.end_bound());
        let mut items: Vec<(u64, RcEntry)> = Vec::new();
        self.collect_range(self.root, lo, hi, &mut items)?;
        self.finish_op(Ok(RangeIter {
            inner: items.into_iter(),
        }))
    }

    /// Iterate the entire tree. Same materialisation caveat as
    /// [`BTree::range`] today — the `_stream` suffix reserves space for
    /// a Phase C swap to a lazy walker without touching call sites.
    /// Phase 7 uses this to expose `Db::iter_refcounts`.
    pub fn iter_stream(&mut self) -> Result<RangeIter> {
        self.range(..)
    }

    fn collect_range(
        &mut self,
        pid: PageId,
        lo: Bound<u64>,
        hi: Bound<u64>,
        out: &mut Vec<(u64, RcEntry)>,
    ) -> Result<()> {
        let page_type = self.buf.read(pid)?.header()?.page_type;
        match page_type {
            PageType::L2pLeaf => {
                let page = self.buf.read(pid)?;
                let n = leaf_key_count(page);
                for i in 0..n {
                    let k = leaf_key_at(page, i);
                    if !lower_ok(lo, k) {
                        continue;
                    }
                    if !upper_ok(hi, k) {
                        break;
                    }
                    out.push((k, leaf_value_at(page, i)));
                }
            }
            PageType::L2pInternal => {
                // Snapshot children so we can recurse without a live
                // borrow on self.buf.
                let (keys, children): (Vec<u64>, Vec<PageId>) = {
                    let page = self.buf.read(pid)?;
                    let n = internal_key_count(page);
                    let keys: Vec<u64> = (0..n).map(|i| internal_key_at(page, i)).collect();
                    let children: Vec<PageId> =
                        (0..=n).map(|i| internal_child_at(page, i)).collect();
                    (keys, children)
                };
                for (i, child) in children.into_iter().enumerate() {
                    let child_lo = if i == 0 { u64::MIN } else { keys[i - 1] };
                    let child_hi = if i == keys.len() {
                        u64::MAX
                    } else {
                        keys[i] - 1
                    };
                    // Prune: skip subtrees whose entire key range is
                    // outside [lo, hi].
                    if !range_overlaps(lo, hi, child_lo, child_hi) {
                        continue;
                    }
                    self.collect_range(child, lo, hi, out)?;
                }
            }
            other => {
                return Err(MetaDbError::Corruption(format!(
                    "btree::range: unexpected page type {other:?} at {pid}"
                )));
            }
        }
        Ok(())
    }

    // -------- insert -----------------------------------------------------

    /// Insert or overwrite `(key, value)`. Returns the previous value
    /// if the key was already present.
    pub fn insert(&mut self, key: u64, value: RcEntry) -> Result<Option<RcEntry>> {
        let generation = self.advance_gen();
        // Walk down from root, recording the path for upward split.
        let mut path: Vec<(PageId, usize, PageId)> = Vec::new();
        let mut current = self.root;
        loop {
            let page_type = self.buf.read(current)?.header()?.page_type;
            match page_type {
                PageType::L2pLeaf => break,
                PageType::L2pInternal => {
                    let slot = internal_search(self.buf.read(current)?, key);
                    let child = internal_child_at(self.buf.read(current)?, slot);
                    path.push((current, slot, child));
                    current = child;
                }
                other => {
                    return Err(MetaDbError::Corruption(format!(
                        "btree::insert: unexpected page type {other:?} at {current}"
                    )));
                }
            }
        }

        // `current` is a leaf. Either overwrite in place (return old)
        // or insert new entry (possibly triggering split).
        let (leaf, mut path) = self.make_path_private(&path, generation)?;
        match leaf_search(self.buf.read(leaf)?, key) {
            Ok(pos) => {
                let old = leaf_value_at(self.buf.read(leaf)?, pos);
                leaf_set_entry(self.buf.modify(leaf, generation)?, pos, key, value);
                self.finish_op(Ok(Some(old)))
            }
            Err(pos) => {
                let count = leaf_key_count(self.buf.read(leaf)?);
                if count < MAX_LEAF_ENTRIES {
                    leaf_insert(self.buf.modify(leaf, generation)?, pos, key, value)?;
                } else {
                    self.split_leaf_and_insert(leaf, pos, key, value, generation, &mut path)?;
                }
                self.finish_op(Ok(None))
            }
        }
    }

    /// Split a full leaf, insert the new (key, value) into whichever
    /// side it belongs to, and propagate the split key up the tree.
    fn split_leaf_and_insert(
        &mut self,
        leaf: PageId,
        insert_pos: usize,
        key: u64,
        value: RcEntry,
        generation: Lsn,
        path: &mut Vec<(PageId, usize)>,
    ) -> Result<()> {
        // Split at the halfway point. The new entry goes into whichever
        // half it belongs to after the split.
        let split_source = MAX_LEAF_ENTRIES / 2;
        let source_count = MAX_LEAF_ENTRIES;
        let new_leaf = self.alloc_leaf_private(generation)?;
        // Copy entries [split_source..source_count] from leaf to new_leaf.
        // Then insert the new entry either into leaf (left) or new_leaf (right).
        let src_bytes = {
            let page = self.buf.read(leaf)?;
            let start = split_source * LEAF_ENTRY_SIZE;
            let end = source_count * LEAF_ENTRY_SIZE;
            page.payload()[start..end].to_vec()
        };
        {
            let np = self.buf.modify(new_leaf, generation)?;
            let dst_end = src_bytes.len();
            np.payload_mut()[..dst_end].copy_from_slice(&src_bytes);
            np.set_key_count((source_count - split_source) as u16);
        }
        // Shrink the original leaf to [0..split_source].
        {
            let lp = self.buf.modify(leaf, generation)?;
            // Zero the tail for deterministic CRC / verifier behaviour.
            let tail_start = split_source * LEAF_ENTRY_SIZE;
            let tail_end = source_count * LEAF_ENTRY_SIZE;
            for b in &mut lp.payload_mut()[tail_start..tail_end] {
                *b = 0;
            }
            lp.set_key_count(split_source as u16);
        }

        if insert_pos <= split_source {
            leaf_insert(self.buf.modify(leaf, generation)?, insert_pos, key, value)?;
        } else {
            leaf_insert(
                self.buf.modify(new_leaf, generation)?,
                insert_pos - split_source,
                key,
                value,
            )?;
        }

        // Promote the first key of new_leaf as the separator.
        let separator_key = leaf_key_at(self.buf.read(new_leaf)?, 0);
        self.propagate_split(separator_key, new_leaf, generation, path)
    }

    /// Walk the path upward installing `(sep_key, right_child)`. Each
    /// parent inserts the separator after its own slot and shifts.
    /// When the top of the path is full, split it and keep walking. On
    /// an empty path (root was a leaf and just split), grow the tree.
    fn propagate_split(
        &mut self,
        mut sep_key: u64,
        mut right_child: PageId,
        generation: Lsn,
        path: &mut Vec<(PageId, usize)>,
    ) -> Result<()> {
        while let Some((parent, child_slot)) = path.pop() {
            let count = internal_key_count(self.buf.read(parent)?);
            if count < MAX_INTERNAL_KEYS {
                internal_insert(
                    self.buf.modify(parent, generation)?,
                    child_slot,
                    sep_key,
                    right_child,
                )?;
                return Ok(());
            }
            // Parent is full. Split it.
            let (new_parent, promote_key) =
                self.split_internal(parent, child_slot, sep_key, right_child, generation)?;
            sep_key = promote_key;
            right_child = new_parent;
        }
        // Fell off the root: grow the tree by one level.
        let old_root = self.root;
        let new_root = self.alloc_internal_private(generation, old_root)?;
        internal_insert(
            self.buf.modify(new_root, generation)?,
            0,
            sep_key,
            right_child,
        )?;
        self.root = new_root;
        Ok(())
    }

    /// Split a full internal after inserting `(sep, right_child)` at
    /// `insert_pos`. Returns the new right-sibling's id and the key to
    /// promote.
    fn split_internal(
        &mut self,
        parent: PageId,
        insert_pos: usize,
        sep: u64,
        right_child: PageId,
        generation: Lsn,
    ) -> Result<(PageId, u64)> {
        // Gather current state: keys[0..n] and children[0..=n].
        let (mut keys, mut children) = {
            let page = self.buf.read(parent)?;
            let n = internal_key_count(page);
            let keys: Vec<u64> = (0..n).map(|i| internal_key_at(page, i)).collect();
            let children: Vec<PageId> = (0..=n).map(|i| internal_child_at(page, i)).collect();
            (keys, children)
        };
        // Insert the new separator + right child at insert_pos.
        keys.insert(insert_pos, sep);
        children.insert(insert_pos + 1, right_child);

        // Pick split point: keys[mid] gets promoted; left keeps keys[..mid]
        // and children[..=mid]; right gets keys[mid+1..] and children[mid+1..].
        let mid = keys.len() / 2;
        let promote_key = keys[mid];
        let left_keys = keys[..mid].to_vec();
        let left_children = children[..=mid].to_vec();
        let right_keys = keys[mid + 1..].to_vec();
        let right_children = children[mid + 1..].to_vec();

        // Rewrite the original parent with the left half.
        {
            let p = self.buf.modify(parent, generation)?;
            p.payload_mut().fill(0);
            internal_set_first_child(p, left_children[0]);
            p.set_key_count(0);
        }
        for (i, k) in left_keys.iter().enumerate() {
            internal_insert(
                self.buf.modify(parent, generation)?,
                i,
                *k,
                left_children[i + 1],
            )?;
        }

        // Build a new internal with the right half.
        let new_parent = self.alloc_internal_private(generation, right_children[0])?;
        for (i, k) in right_keys.iter().enumerate() {
            internal_insert(
                self.buf.modify(new_parent, generation)?,
                i,
                *k,
                right_children[i + 1],
            )?;
        }
        Ok((new_parent, promote_key))
    }

    // -------- delete -----------------------------------------------------

    /// Remove `key`. Returns the previous value if it was present.
    pub fn delete(&mut self, key: u64) -> Result<Option<RcEntry>> {
        let generation = self.advance_gen();
        // Walk down to the leaf, recording the path.
        let mut path: Vec<(PageId, usize, PageId)> = Vec::new();
        let mut current = self.root;
        loop {
            let page_type = self.buf.read(current)?.header()?.page_type;
            match page_type {
                PageType::L2pLeaf => break,
                PageType::L2pInternal => {
                    let slot = internal_search(self.buf.read(current)?, key);
                    let child = internal_child_at(self.buf.read(current)?, slot);
                    path.push((current, slot, child));
                    current = child;
                }
                other => {
                    return Err(MetaDbError::Corruption(format!(
                        "btree::delete: unexpected page type {other:?} at {current}"
                    )));
                }
            }
        }

        let leaf = current;
        let pos = match leaf_search(self.buf.read(leaf)?, key) {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };
        let (leaf, path) = self.make_path_private(&path, generation)?;
        let old = leaf_value_at(self.buf.read(leaf)?, pos);
        leaf_remove(self.buf.modify(leaf, generation)?, pos)?;

        self.rebalance_after_remove(leaf, generation, path)?;
        self.finish_op(Ok(Some(old)))
    }

    /// After removing from a leaf, walk up the path and borrow / merge
    /// with siblings to restore the fill invariants. Root shrinks to
    /// its single child when it degenerates to a one-child internal.
    fn rebalance_after_remove(
        &mut self,
        mut child: PageId,
        generation: Lsn,
        mut path: Vec<(PageId, usize)>,
    ) -> Result<()> {
        while let Some((parent, child_slot)) = path.pop() {
            let page_type = self.buf.read(child)?.header()?.page_type;
            let fill = match page_type {
                PageType::L2pLeaf => leaf_key_count(self.buf.read(child)?),
                PageType::L2pInternal => internal_key_count(self.buf.read(child)?),
                _ => {
                    return Err(MetaDbError::Corruption(format!(
                        "btree::rebalance: bad page type at {child}"
                    )));
                }
            };
            let threshold = if page_type == PageType::L2pLeaf {
                LEAF_UNDERFLOW_THRESHOLD
            } else {
                INTERNAL_UNDERFLOW_THRESHOLD
            };
            if fill >= threshold {
                return Ok(());
            }

            // Find a sibling. Prefer borrowing from the left sibling,
            // fall back to the right, merge as last resort.
            let parent_key_count = internal_key_count(self.buf.read(parent)?);
            let left_idx = if child_slot > 0 {
                Some(child_slot - 1)
            } else {
                None
            };
            let right_idx = if child_slot < parent_key_count {
                Some(child_slot + 1)
            } else {
                None
            };

            if let Some(lidx) = left_idx {
                let left_sibling = internal_child_at(self.buf.read(parent)?, lidx);
                if self.can_borrow_from(left_sibling, page_type)? {
                    let left_sibling = self.ensure_private(left_sibling, generation)?;
                    internal_set_child(self.buf.modify(parent, generation)?, lidx, left_sibling);
                    self.borrow_from_left(parent, child_slot, left_sibling, child, generation)?;
                    return Ok(());
                }
            }
            if let Some(ridx) = right_idx {
                let right_sibling = internal_child_at(self.buf.read(parent)?, ridx);
                if self.can_borrow_from(right_sibling, page_type)? {
                    let right_sibling = self.ensure_private(right_sibling, generation)?;
                    internal_set_child(self.buf.modify(parent, generation)?, ridx, right_sibling);
                    self.borrow_from_right(parent, child_slot, child, right_sibling, generation)?;
                    return Ok(());
                }
            }

            // Merge with a sibling.
            if let Some(lidx) = left_idx {
                let left_sibling = internal_child_at(self.buf.read(parent)?, lidx);
                // Merge `child` into `left_sibling`, drop the separator
                // at `lidx`, drop `child`'s slot from the parent.
                let left_sibling = self.ensure_private(left_sibling, generation)?;
                internal_set_child(self.buf.modify(parent, generation)?, lidx, left_sibling);
                self.merge_into_left(parent, child_slot, left_sibling, child, generation)?;
            } else if let Some(ridx) = right_idx {
                let right_sibling = internal_child_at(self.buf.read(parent)?, ridx);
                // Merge `right_sibling` into `child`, drop separator at
                // `child_slot`. `child` survives.
                self.merge_into_left(parent, ridx, child, right_sibling, generation)?;
            } else {
                // Parent has no siblings: parent must be a single-child
                // internal. Root collapse below handles it on the next
                // iteration.
                return Ok(());
            }

            // After merging, the parent lost one separator. Climb and
            // re-check its fill.
            child = parent;
        }

        // Reached the root. If root is an internal with a single
        // child, collapse it.
        let root_type = self.buf.read(self.root)?.header()?.page_type;
        if root_type == PageType::L2pInternal {
            let n = internal_key_count(self.buf.read(self.root)?);
            if n == 0 {
                let only_child = internal_child_at(self.buf.read(self.root)?, 0);
                let old_root = self.root;
                self.root = only_child;
                self.free_detached(old_root, generation)?;
            }
        }
        Ok(())
    }

    fn can_borrow_from(&mut self, sibling: PageId, page_type: PageType) -> Result<bool> {
        let page = self.buf.read(sibling)?;
        let fill = match page_type {
            PageType::L2pLeaf => leaf_key_count(page),
            PageType::L2pInternal => internal_key_count(page),
            _ => unreachable!(),
        };
        let threshold = if page_type == PageType::L2pLeaf {
            LEAF_UNDERFLOW_THRESHOLD
        } else {
            INTERNAL_UNDERFLOW_THRESHOLD
        };
        Ok(fill > threshold)
    }

    fn borrow_from_left(
        &mut self,
        parent: PageId,
        child_slot: usize,
        left_sibling: PageId,
        child: PageId,
        generation: Lsn,
    ) -> Result<()> {
        let page_type = self.buf.read(child)?.header()?.page_type;
        match page_type {
            PageType::L2pLeaf => {
                // Take the last entry from left_sibling, prepend to child.
                let last = leaf_key_count(self.buf.read(left_sibling)?) - 1;
                let k = leaf_key_at(self.buf.read(left_sibling)?, last);
                let v = leaf_value_at(self.buf.read(left_sibling)?, last);
                leaf_insert(self.buf.modify(child, generation)?, 0, k, v)?;
                leaf_remove(self.buf.modify(left_sibling, generation)?, last)?;
                // Update parent separator to child's new first key.
                let new_sep = leaf_key_at(self.buf.read(child)?, 0);
                internal_set_key_at(
                    self.buf.modify(parent, generation)?,
                    child_slot - 1,
                    new_sep,
                );
            }
            PageType::L2pInternal => {
                // Take left_sibling's last child + its last key; combine
                // with the parent separator; prepend to child.
                let sep = internal_key_at(self.buf.read(parent)?, child_slot - 1);
                let last_key_idx = internal_key_count(self.buf.read(left_sibling)?) - 1;
                let last_child = internal_child_at(self.buf.read(left_sibling)?, last_key_idx + 1);
                let last_key = internal_key_at(self.buf.read(left_sibling)?, last_key_idx);
                internal_push_front(self.buf.modify(child, generation)?, sep, last_child)?;
                internal_remove(self.buf.modify(left_sibling, generation)?, last_key_idx)?;
                internal_set_key_at(
                    self.buf.modify(parent, generation)?,
                    child_slot - 1,
                    last_key,
                );
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn borrow_from_right(
        &mut self,
        parent: PageId,
        child_slot: usize,
        child: PageId,
        right_sibling: PageId,
        generation: Lsn,
    ) -> Result<()> {
        let page_type = self.buf.read(child)?.header()?.page_type;
        match page_type {
            PageType::L2pLeaf => {
                let k = leaf_key_at(self.buf.read(right_sibling)?, 0);
                let v = leaf_value_at(self.buf.read(right_sibling)?, 0);
                let count = leaf_key_count(self.buf.read(child)?);
                leaf_insert(self.buf.modify(child, generation)?, count, k, v)?;
                leaf_remove(self.buf.modify(right_sibling, generation)?, 0)?;
                let new_sep = leaf_key_at(self.buf.read(right_sibling)?, 0);
                internal_set_key_at(self.buf.modify(parent, generation)?, child_slot, new_sep);
            }
            PageType::L2pInternal => {
                let sep = internal_key_at(self.buf.read(parent)?, child_slot);
                let first_child = internal_child_at(self.buf.read(right_sibling)?, 0);
                let first_key = internal_key_at(self.buf.read(right_sibling)?, 0);
                let child_count = internal_key_count(self.buf.read(child)?);
                internal_insert(
                    self.buf.modify(child, generation)?,
                    child_count,
                    sep,
                    first_child,
                )?;
                internal_pop_front(self.buf.modify(right_sibling, generation)?)?;
                internal_set_key_at(self.buf.modify(parent, generation)?, child_slot, first_key);
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    /// Merge `right_page` into `left_page`; drop the separator at
    /// `left_slot_index` in `parent` (which is the separator between
    /// left and right in the parent's keys array). Free the right
    /// page. `left_slot_index` is the index in parent.keys where the
    /// separator currently lives.
    ///
    /// Layout: parent.children[left_idx] = left_page, parent.children[left_idx+1] = right_page,
    /// separator is parent.keys[left_idx].
    fn merge_into_left(
        &mut self,
        parent: PageId,
        right_slot_in_parent: usize,
        left_page: PageId,
        right_page: PageId,
        generation: Lsn,
    ) -> Result<()> {
        // right_slot_in_parent is the child-slot index of `right_page`.
        // Separator index in parent.keys is `right_slot_in_parent - 1`.
        let sep_idx = right_slot_in_parent - 1;
        let page_type = self.buf.read(left_page)?.header()?.page_type;
        match page_type {
            PageType::L2pLeaf => {
                let right_count = leaf_key_count(self.buf.read(right_page)?);
                for i in 0..right_count {
                    let k = leaf_key_at(self.buf.read(right_page)?, i);
                    let v = leaf_value_at(self.buf.read(right_page)?, i);
                    let insert_pos = leaf_key_count(self.buf.read(left_page)?);
                    leaf_insert(self.buf.modify(left_page, generation)?, insert_pos, k, v)?;
                }
            }
            PageType::L2pInternal => {
                let sep_key = internal_key_at(self.buf.read(parent)?, sep_idx);
                let first_right_child = internal_child_at(self.buf.read(right_page)?, 0);
                let left_count = internal_key_count(self.buf.read(left_page)?);
                internal_insert(
                    self.buf.modify(left_page, generation)?,
                    left_count,
                    sep_key,
                    first_right_child,
                )?;
                let right_key_count = internal_key_count(self.buf.read(right_page)?);
                for i in 0..right_key_count {
                    let k = internal_key_at(self.buf.read(right_page)?, i);
                    let c = internal_child_at(self.buf.read(right_page)?, i + 1);
                    let insert_pos = internal_key_count(self.buf.read(left_page)?);
                    internal_insert(self.buf.modify(left_page, generation)?, insert_pos, k, c)?;
                }
            }
            _ => unreachable!(),
        }
        // Remove the separator and the right child pointer from parent.
        internal_remove(self.buf.modify(parent, generation)?, sep_idx)?;
        self.free_detached(right_page, generation)?;
        // `internal_set_first_child` is irrelevant: left_page is already
        // at the correct slot, we just shortened the parent by one.
        let _ = internal_set_first_child;
        Ok(())
    }

    fn advance_gen(&mut self) -> Lsn {
        let g = self.next_gen;
        self.next_gen = self
            .next_gen
            .checked_add(1)
            .expect("btree: next_gen overflow");
        g
    }

    /// Run the invariant checker over the whole tree. Used by
    /// property tests and the verifier tool.
    pub fn check_invariants(&mut self) -> Result<()> {
        let result = crate::btree::invariants::check_tree(&mut self.buf, self.root);
        self.finish_op(result)
    }

    #[cfg(test)]
    fn cached_pages_for_test(&self) -> usize {
        self.buf.len()
    }
}

/// Ascending iterator returned by [`BTree::range`]. Entries are
/// collected eagerly; the caller can call `.collect()` without
/// worrying about partial iteration.
pub struct RangeIter {
    inner: std::vec::IntoIter<(u64, RcEntry)>,
}

impl Iterator for RangeIter {
    type Item = Result<(u64, RcEntry)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Ok)
    }
}

fn bound_lower(b: Bound<&u64>) -> Bound<u64> {
    match b {
        Bound::Included(v) => Bound::Included(*v),
        Bound::Excluded(v) => Bound::Excluded(*v),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn bound_upper(b: Bound<&u64>) -> Bound<u64> {
    match b {
        Bound::Included(v) => Bound::Included(*v),
        Bound::Excluded(v) => Bound::Excluded(*v),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn lower_ok(lo: Bound<u64>, k: u64) -> bool {
    match lo {
        Bound::Included(v) => k >= v,
        Bound::Excluded(v) => k > v,
        Bound::Unbounded => true,
    }
}

fn upper_ok(hi: Bound<u64>, k: u64) -> bool {
    match hi {
        Bound::Included(v) => k <= v,
        Bound::Excluded(v) => k < v,
        Bound::Unbounded => true,
    }
}

fn range_overlaps(lo: Bound<u64>, hi: Bound<u64>, child_lo: u64, child_hi: u64) -> bool {
    // child range is [child_lo, child_hi]. Check for non-empty
    // intersection with [lo, hi] (bounds).
    let lo_ok = match hi {
        Bound::Included(v) => child_lo <= v,
        Bound::Excluded(v) => child_lo < v,
        Bound::Unbounded => true,
    };
    let hi_ok = match lo {
        Bound::Included(v) => child_hi >= v,
        Bound::Excluded(v) => child_hi > v,
        Bound::Unbounded => true,
    };
    lo_ok && hi_ok
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

    fn rc(rc: u32) -> RcEntry {
        RcEntry { rc, birth_lsn: 1 }
    }

    #[test]
    fn insert_then_get() {
        let (_d, mut t) = mk_tree();
        assert_eq!(t.insert(42, rc(100)).unwrap(), None);
        assert_eq!(t.get(42).unwrap(), Some(rc(100)));
    }

    #[test]
    fn overwrite_returns_old_value() {
        let (_d, mut t) = mk_tree();
        t.insert(1, rc(10)).unwrap();
        assert_eq!(t.insert(1, rc(20)).unwrap(), Some(rc(10)));
        assert_eq!(t.get(1).unwrap(), Some(rc(20)));
    }

    #[test]
    fn delete_returns_value_and_removes() {
        let (_d, mut t) = mk_tree();
        t.insert(7, rc(77)).unwrap();
        assert_eq!(t.delete(7).unwrap(), Some(rc(77)));
        assert_eq!(t.get(7).unwrap(), None);
        assert_eq!(t.delete(7).unwrap(), None);
    }

    #[test]
    fn many_inserts_cause_splits() {
        let (_d, mut t) = mk_tree();
        // Enough to overflow one leaf (MAX_LEAF_ENTRIES = 201).
        for i in 0..1_000u64 {
            t.insert(i, rc(i as u32 + 1)).unwrap();
        }
        for i in 0..1_000u64 {
            assert_eq!(t.get(i).unwrap(), Some(rc(i as u32 + 1)));
        }
    }

    #[test]
    fn many_deletes_cause_merges_without_losing_keys() {
        let (_d, mut t) = mk_tree();
        for i in 0..1_000u64 {
            t.insert(i, rc(1)).unwrap();
        }
        // Delete half.
        for i in 0..1_000u64 {
            if i % 2 == 0 {
                t.delete(i).unwrap();
            }
        }
        for i in 0..1_000u64 {
            let expect = if i % 2 == 0 { None } else { Some(rc(1)) };
            assert_eq!(t.get(i).unwrap(), expect, "key {i}");
        }
    }

    #[test]
    fn range_returns_sorted_hits() {
        let (_d, mut t) = mk_tree();
        for i in 0..100u64 {
            t.insert(i, rc(i as u32)).unwrap();
        }
        let got: Vec<(u64, RcEntry)> = t.range(10..=20).unwrap().map(|r| r.unwrap()).collect();
        let expected: Vec<(u64, RcEntry)> = (10..=20).map(|i| (i, rc(i as u32))).collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn flush_persists_across_reopen() {
        let dir = TempDir::new().unwrap();
        let ps = Arc::new(PageStore::create(dir.path().join("p.onyx_meta")).unwrap());
        let root;
        {
            let mut t = BTree::create(ps.clone()).unwrap();
            t.insert(100, rc(500)).unwrap();
            t.insert(200, rc(600)).unwrap();
            t.flush().unwrap();
            root = t.root();
        }
        let mut t = BTree::open(ps, root, 100).unwrap();
        assert_eq!(t.get(100).unwrap(), Some(rc(500)));
        assert_eq!(t.get(200).unwrap(), Some(rc(600)));
    }

    #[test]
    fn flush_and_reads_do_not_leave_private_clean_pages_resident() {
        let (_d, mut t) = mk_tree();
        for i in 0..2048u64 {
            t.insert(i, rc(i as u32)).unwrap();
        }
        assert!(t.cached_pages_for_test() > 0);
        t.flush().unwrap();
        assert_eq!(t.cached_pages_for_test(), 0);

        for i in 0..256u64 {
            let _ = t.get(i).unwrap();
            assert_eq!(t.cached_pages_for_test(), 0);
        }
    }

    #[test]
    fn iter_stream_visits_every_key_in_order() {
        let (_d, mut t) = mk_tree();
        let keys: Vec<u64> = (0..512u64).map(|i| i * 13).collect();
        for k in &keys {
            t.insert(*k, rc(*k as u32)).unwrap();
        }
        let got: Vec<(u64, RcEntry)> = t.iter_stream().unwrap().map(|r| r.unwrap()).collect();
        let mut expected: Vec<(u64, RcEntry)> = keys.iter().map(|k| (*k, rc(*k as u32))).collect();
        expected.sort_unstable_by_key(|(k, _)| *k);
        assert_eq!(got, expected);
    }

    #[test]
    fn iter_stream_matches_full_range() {
        let (_d, mut t) = mk_tree();
        for i in 0..128u64 {
            t.insert(i * 7, rc(i as u32)).unwrap();
        }
        let via_range: Vec<_> = t.range(..).unwrap().map(|r| r.unwrap()).collect();
        let via_iter: Vec<_> = t.iter_stream().unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(via_range, via_iter);
    }
}
