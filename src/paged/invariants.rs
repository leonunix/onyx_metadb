//! Structural checks for the paged L2P radix tree.

use std::collections::HashSet;

use crate::error::{MetaDbError, Result};
use crate::page::PageType;
use crate::page_store::PageStore;
use crate::paged::format::{
    INDEX_FANOUT, LEAF_ENTRY_COUNT, index_child_at, leaf_bit_set, leaf_entry_count, page_level,
};
use crate::types::{NULL_PAGE, PageId};

/// Verify the tree rooted at `root`.
pub fn check_tree(page_store: &PageStore, root: PageId) -> Result<()> {
    if root == NULL_PAGE {
        return Err(MetaDbError::Corruption(
            "paged tree root cannot be NULL_PAGE".into(),
        ));
    }
    let root_page = page_store.read_page(root)?;
    let root_level = page_level(&root_page)?;
    let mut seen = HashSet::new();
    check_subtree(page_store, root, root_level, &mut seen)
}

fn check_subtree(
    page_store: &PageStore,
    pid: PageId,
    expected_level: u8,
    seen: &mut HashSet<PageId>,
) -> Result<()> {
    if !seen.insert(pid) {
        return Ok(());
    }
    let page = page_store.read_page(pid)?;
    let level = page_level(&page)?;
    if level != expected_level {
        return Err(MetaDbError::Corruption(format!(
            "paged page {pid} has level {level}, expected {expected_level}",
        )));
    }
    match page.header()?.page_type {
        PageType::PagedLeaf => {
            if expected_level != 0 {
                return Err(MetaDbError::Corruption(format!(
                    "paged leaf {pid} reached at nonzero level {expected_level}",
                )));
            }
            let bitmap_count = (0..LEAF_ENTRY_COUNT)
                .filter(|slot| leaf_bit_set(&page, *slot))
                .count();
            if bitmap_count != leaf_entry_count(&page) as usize {
                return Err(MetaDbError::Corruption(format!(
                    "paged leaf {pid} bitmap count {} disagrees with header {}",
                    bitmap_count,
                    leaf_entry_count(&page),
                )));
            }
        }
        PageType::PagedIndex => {
            if expected_level == 0 {
                return Err(MetaDbError::Corruption(format!(
                    "paged index {pid} reached at level 0",
                )));
            }
            let mut non_null = 0usize;
            for slot in 0..INDEX_FANOUT {
                let child = index_child_at(&page, slot);
                if child == NULL_PAGE {
                    continue;
                }
                non_null += 1;
                check_subtree(page_store, child, expected_level - 1, seen)?;
            }
            if non_null != page.key_count() as usize {
                return Err(MetaDbError::Corruption(format!(
                    "paged index {pid} child count {} disagrees with header {}",
                    non_null,
                    page.key_count(),
                )));
            }
        }
        other => {
            return Err(MetaDbError::Corruption(format!(
                "page {pid} has unexpected type {other:?} in paged tree",
            )));
        }
    }
    Ok(())
}
