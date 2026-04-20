//! Structural invariant checker for a B+tree rooted in a [`PageBuf`].
//!
//! Used by property tests and the `metadb-verify` binary (future
//! phase). Walks every page reachable from the root and checks:
//!
//! - Each page decodes cleanly (magic, version, page type).
//! - Within a leaf: keys strictly ascending; count in bounds.
//! - Within an internal: keys strictly ascending; count in bounds;
//!   `key_count + 1` children; separator keys bracket every subtree
//!   correctly (keys in `children[i+1]` are >= `keys[i]` and <
//!   `keys[i+1]`).
//! - All leaves are at the same depth.
//! - Fill thresholds: non-root pages are at least half-full.
//! - Root sanity: a root internal must have at least one key (else
//!   the delete path should have collapsed it).
//!
//! Violations are surfaced as [`MetaDbError::Corruption`] with an
//! explanatory message. A clean check is `Ok(())`.

use crate::btree::cache::PageBuf;
use crate::btree::format::{
    MAX_INTERNAL_KEYS, MAX_LEAF_ENTRIES, internal_child_at, internal_key_at, internal_key_count,
    leaf_key_at, leaf_key_count,
};
use crate::error::{MetaDbError, Result};
use crate::page::PageType;
use crate::types::PageId;

/// Underflow threshold for a non-root leaf.
pub const LEAF_UNDERFLOW: usize = MAX_LEAF_ENTRIES / 2;

/// Underflow threshold for a non-root internal.
pub const INTERNAL_UNDERFLOW: usize = MAX_INTERNAL_KEYS / 2;

/// Verify every structural invariant of the B+tree rooted at `root`,
/// reading pages via `buf`.  Returns `Ok(())` on success.
pub fn check_tree(buf: &mut PageBuf, root: PageId) -> Result<()> {
    let mut leaf_depths: Vec<usize> = Vec::new();
    check_subtree(buf, root, None, None, 1, true, &mut leaf_depths)?;
    if leaf_depths.is_empty() {
        return Err(MetaDbError::Corruption("tree has no leaves".into()));
    }
    let d0 = leaf_depths[0];
    for (i, d) in leaf_depths.iter().enumerate() {
        if *d != d0 {
            return Err(MetaDbError::Corruption(format!(
                "leaves at different depths: leaf #{i} at depth {d}, first at {d0}",
            )));
        }
    }
    Ok(())
}

fn check_subtree(
    buf: &mut PageBuf,
    pid: PageId,
    min_incl: Option<u64>,
    max_excl: Option<u64>,
    depth: usize,
    is_root: bool,
    leaf_depths: &mut Vec<usize>,
) -> Result<()> {
    let page_type = buf.read(pid)?.header()?.page_type;
    match page_type {
        PageType::L2pLeaf => {
            let count = leaf_key_count(buf.read(pid)?);
            if count > MAX_LEAF_ENTRIES {
                return Err(MetaDbError::Corruption(format!(
                    "leaf {pid} overflow: {count} > {MAX_LEAF_ENTRIES}",
                )));
            }
            if !is_root && count < LEAF_UNDERFLOW {
                return Err(MetaDbError::Corruption(format!(
                    "leaf {pid} underflow: {count} < {LEAF_UNDERFLOW}",
                )));
            }
            let mut prev: Option<u64> = None;
            for i in 0..count {
                let k = leaf_key_at(buf.read(pid)?, i);
                if let Some(pk) = prev {
                    if k <= pk {
                        return Err(MetaDbError::Corruption(format!(
                            "leaf {pid} keys not strictly ascending at index {i}: {k} <= {pk}",
                        )));
                    }
                }
                if let Some(mn) = min_incl {
                    if k < mn {
                        return Err(MetaDbError::Corruption(format!(
                            "leaf {pid} key {k} < separator lower bound {mn}",
                        )));
                    }
                }
                if let Some(mx) = max_excl {
                    if k >= mx {
                        return Err(MetaDbError::Corruption(format!(
                            "leaf {pid} key {k} >= separator upper bound {mx}",
                        )));
                    }
                }
                prev = Some(k);
            }
            leaf_depths.push(depth);
            Ok(())
        }
        PageType::L2pInternal => {
            let count = internal_key_count(buf.read(pid)?);
            if count > MAX_INTERNAL_KEYS {
                return Err(MetaDbError::Corruption(format!(
                    "internal {pid} overflow: {count} > {MAX_INTERNAL_KEYS}",
                )));
            }
            if !is_root && count < INTERNAL_UNDERFLOW {
                return Err(MetaDbError::Corruption(format!(
                    "internal {pid} underflow: {count} < {INTERNAL_UNDERFLOW}",
                )));
            }
            if is_root && count == 0 {
                return Err(MetaDbError::Corruption(format!(
                    "root internal {pid} has zero separator keys (collapse missed)",
                )));
            }
            // Snapshot keys + children (releases the page borrow so
            // recursive reads can reuse buf).
            let keys: Vec<u64> = (0..count)
                .map(|i| internal_key_at(buf.read(pid).unwrap(), i))
                .collect();
            let children: Vec<PageId> = (0..=count)
                .map(|i| internal_child_at(buf.read(pid).unwrap(), i))
                .collect();
            for i in 1..count {
                if keys[i] <= keys[i - 1] {
                    return Err(MetaDbError::Corruption(format!(
                        "internal {pid} keys not strictly ascending at index {i}",
                    )));
                }
            }
            for i in 0..=count {
                let child_min = if i == 0 { min_incl } else { Some(keys[i - 1]) };
                let child_max = if i == count { max_excl } else { Some(keys[i]) };
                check_subtree(
                    buf,
                    children[i],
                    child_min,
                    child_max,
                    depth + 1,
                    false,
                    leaf_depths,
                )?;
            }
            Ok(())
        }
        other => Err(MetaDbError::Corruption(format!(
            "page {pid} has unexpected type {other:?} while walking btree",
        ))),
    }
}

#[cfg(test)]
mod tests {
    use crate::btree::BTree;
    use crate::page_store::PageStore;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn mk_tree() -> (TempDir, BTree) {
        let dir = TempDir::new().unwrap();
        let ps = Arc::new(PageStore::create(dir.path().join("p")).unwrap());
        (dir, BTree::create(ps).unwrap())
    }

    #[test]
    fn empty_tree_passes() {
        let (_d, mut t) = mk_tree();
        t.check_invariants().unwrap();
    }

    #[test]
    fn populated_tree_passes() {
        let (_d, mut t) = mk_tree();
        for i in 0u64..2000 {
            t.insert(i, 0).unwrap();
        }
        t.check_invariants().unwrap();
    }

    #[test]
    fn invariants_hold_after_deletes() {
        let (_d, mut t) = mk_tree();
        for i in 0u64..1000 {
            t.insert(i, 0).unwrap();
        }
        for i in (0u64..1000).step_by(3) {
            t.delete(i).unwrap();
        }
        t.check_invariants().unwrap();
    }
}
