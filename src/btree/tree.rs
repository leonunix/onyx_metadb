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

use std::sync::Arc;

use crate::btree::cache::PageBuf;
use crate::btree::format::{
    L2pValue, LEAF_ENTRY_SIZE, MAX_INTERNAL_KEYS, MAX_LEAF_ENTRIES, internal_child_at,
    internal_insert, internal_key_at, internal_key_count, internal_search,
    internal_set_first_child, leaf_insert, leaf_key_at, leaf_key_count, leaf_search,
    leaf_set_entry, leaf_value_at,
};
use crate::error::{MetaDbError, Result};
use crate::page::PageType;
use crate::page_store::PageStore;
use crate::types::{Lsn, PageId};

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
                    let (sep_key, new_right) =
                        split_leaf_and_insert(&mut self.buf, current, pos, key, &value, generation)?;
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
                    return Err(MetaDbError::Corruption(format!("unexpected type {other:?}")));
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
                    return Err(MetaDbError::Corruption(format!("unexpected type {other:?}")));
                }
            }
        }
        Ok(leaves)
    }

    /// Diagnostic accessor: number of pages currently cached.
    pub fn cached_pages(&self) -> usize {
        self.buf.len()
    }
}

enum GetProbe {
    Hit(L2pValue),
    Miss,
    Descend(PageId),
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
        use rand::seq::SliceRandom;
        use rand::SeedableRng;
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
