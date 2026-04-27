//! Lock-free L2P read snapshot. Reader path: clone the published
//! `Arc<ReadView>`, walk via `overlay → page_cache` without taking
//! any shard lock.
//!
//! Two non-obvious invariants:
//! - **Overlay must include unflushed dirty pages.** Apply COWs new
//!   pages into `PagedL2p.buf.pages` but `flush()` only runs every
//!   50 ms (onyx `durability-watermark`). Between publish and flush
//!   the new pid isn't on disk and isn't in `page_cache`; if the
//!   reader cache-missed it, `page_store.read_page` would decode
//!   garbage. Snapshot safety against later live mutations comes
//!   from `Slot::Dirty(Arc<Page>)` + `Arc::make_mut` in
//!   `PageBuf::modify`.
//! - **Reader must hold `apply_gate.read()`.** Flush is the only
//!   path that physically frees L2P pages
//!   (`tree.checkpoint_committed` → `page_store.free`). Holding the
//!   read gate keeps flush from reclaiming pids while a stale
//!   ReadView still references them.

use std::collections::HashMap;
use std::sync::Arc;

use crate::cache::PageCache;
use crate::error::Result;
use crate::page::Page;
use crate::paged::format::{
    L2pValue, LEAF_MASK, LEAF_SHIFT, index_child_at, leaf_bit_set, leaf_value_at,
    max_leaf_idx_at_level, page_level, slot_in_index,
};
use crate::types::{NULL_PAGE, PageId};

/// Cheap-to-clone (`Arc`-wrapped) lock-free snapshot of a paged L2P
/// tree, capturing both the root pointer and any dirty pages the apply
/// hasn't yet flushed.
#[derive(Clone)]
pub struct ReadView {
    root: PageId,
    root_level: u8,
    overlay: Arc<HashMap<PageId, Arc<Page>>>,
    page_cache: Arc<PageCache>,
}

impl ReadView {
    pub fn new(
        root: PageId,
        root_level: u8,
        overlay: Arc<HashMap<PageId, Arc<Page>>>,
        page_cache: Arc<PageCache>,
    ) -> Self {
        Self {
            root,
            root_level,
            overlay,
            page_cache,
        }
    }

    pub fn root(&self) -> PageId {
        self.root
    }

    pub fn root_level(&self) -> u8 {
        self.root_level
    }

    pub fn overlay_len(&self) -> usize {
        self.overlay.len()
    }

    pub fn page_cache(&self) -> &Arc<PageCache> {
        &self.page_cache
    }

    /// Point lookup. `None` if `lba` is not mapped at this snapshot.
    pub fn get(&self, lba: u64) -> Result<Option<L2pValue>> {
        let leaf_idx = lba >> LEAF_SHIFT;
        let bit = (lba & LEAF_MASK) as usize;
        if leaf_idx > max_leaf_idx_at_level(self.root_level) {
            return Ok(None);
        }
        let mut current = self.root;
        let mut level = self.root_level;
        while level > 0 {
            let child = self.with_page(current, |page| {
                Ok(index_child_at(page, slot_in_index(leaf_idx, level)))
            })?;
            if child == NULL_PAGE {
                return Ok(None);
            }
            current = child;
            level -= 1;
        }
        self.with_page(current, |leaf| {
            if !leaf_bit_set(leaf, bit) {
                Ok(None)
            } else {
                Ok(Some(leaf_value_at(leaf, bit)))
            }
        })
    }

    /// Batched point lookup. Walks each LBA independently; callers
    /// route through this when they have an already-bucketed slice for
    /// one shard.
    pub fn multi_get(&self, lbas: &[u64]) -> Result<Vec<Option<L2pValue>>> {
        let mut out = Vec::with_capacity(lbas.len());
        for &lba in lbas {
            out.push(self.get(lba)?);
        }
        Ok(out)
    }

    fn with_page<T>(&self, pid: PageId, f: impl FnOnce(&Page) -> Result<T>) -> Result<T> {
        if let Some(arc) = self.overlay.get(&pid) {
            return f(arc.as_ref());
        }
        let page = self.page_cache.get(pid)?;
        f(page.as_ref())
    }

    pub fn read_root_level(page_cache: &PageCache, root: PageId) -> Result<u8> {
        let page = page_cache.get(root)?;
        page_level(page.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::DEFAULT_PAGE_CACHE_BYTES;
    use crate::page_store::PageStore;
    use crate::paged::PagedL2p;
    use crate::paged::format::L2pValue;
    use tempfile::TempDir;

    fn fresh() -> (TempDir, Arc<PageStore>, Arc<PageCache>) {
        let dir = TempDir::new().unwrap();
        let ps = Arc::new(PageStore::create(dir.path().join("p.onyx_meta")).unwrap());
        let pc = Arc::new(PageCache::new(ps.clone(), DEFAULT_PAGE_CACHE_BYTES));
        (dir, ps, pc)
    }

    fn open_tree(ps: &Arc<PageStore>, pc: &Arc<PageCache>) -> PagedL2p {
        PagedL2p::create_with_cache(ps.clone(), pc.clone()).unwrap()
    }

    fn empty_view(tree: &PagedL2p, pc: Arc<PageCache>) -> ReadView {
        ReadView::new(
            tree.root(),
            tree.root_level(),
            Arc::new(HashMap::new()),
            pc,
        )
    }

    fn val(byte: u8) -> L2pValue {
        L2pValue([byte; 28])
    }

    #[test]
    fn empty_view_returns_none() {
        let (_d, ps, pc) = fresh();
        let mut tree = open_tree(&ps, &pc);
        tree.flush().unwrap();
        let view = empty_view(&tree, pc);
        assert!(view.get(0).unwrap().is_none());
        assert!(view.get(99).unwrap().is_none());
    }

    #[test]
    fn view_sees_flushed_writes_via_cache() {
        let (_d, ps, pc) = fresh();
        let mut tree = open_tree(&ps, &pc);
        tree.insert_at_lsn(7u64, val(0x42), 1).unwrap();
        tree.flush().unwrap();
        let view = empty_view(&tree, pc);
        assert_eq!(view.get(7).unwrap(), Some(val(0x42)));
        assert!(view.get(8).unwrap().is_none());
    }

    #[test]
    fn view_sees_unflushed_writes_via_overlay() {
        // Apply mutates tree but never flushes — ReadView must resolve
        // the new mapping via the dirty-page overlay, not page_cache.
        let (_d, ps, pc) = fresh();
        let mut tree = open_tree(&ps, &pc);
        tree.flush().unwrap();
        tree.insert_at_lsn(123_456u64, val(0x99), 7).unwrap();
        let view = tree.snapshot_read_view();
        assert!(view.overlay_len() > 0);
        assert_eq!(view.get(123_456).unwrap(), Some(val(0x99)));
    }

    #[test]
    fn make_mut_protects_snapshot() {
        // The snapshot's overlay shares Arc<Page> with the live tree;
        // a subsequent live mutation must Arc::make_mut a fresh page
        // rather than touch the snapshot's bytes.
        let (_d, ps, pc) = fresh();
        let mut tree = open_tree(&ps, &pc);
        tree.flush().unwrap();
        tree.insert_at_lsn(5u64, val(0x11), 1).unwrap();
        let snap = tree.snapshot_read_view();
        assert_eq!(snap.get(5).unwrap(), Some(val(0x11)));

        tree.insert_at_lsn(5u64, val(0x22), 2).unwrap();
        assert_eq!(snap.get(5).unwrap(), Some(val(0x11)));

        let live = tree.snapshot_read_view();
        assert_eq!(live.get(5).unwrap(), Some(val(0x22)));
    }
}
