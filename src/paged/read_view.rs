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

use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::{Arc, OnceLock};

use crate::cache::PageCache;
use crate::error::Result;
use crate::page::Page;
use crate::paged::format::{
    L2pValue, LEAF_MASK, LEAF_SHIFT, index_child_at, leaf_bit_set, leaf_value_at,
    max_leaf_idx_at_level, page_level, slot_in_index,
};
use crate::types::{NULL_PAGE, PageId};

#[derive(Default)]
pub struct PageIdHasher(u64);

impl Hasher for PageIdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        let mut hash = 0xcbf29ce484222325u64;
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        self.0 = hash;
    }

    fn write_u64(&mut self, value: u64) {
        let mut x = value.wrapping_add(0x9e3779b97f4a7c15);
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
        self.0 = x ^ (x >> 31);
    }
}

pub const READ_OVERLAY_SHARDS: usize = 1024;
const READ_OVERLAY_SHARD_MASK: u64 = READ_OVERLAY_SHARDS as u64 - 1;

pub type PageIdMap<V> = HashMap<PageId, V, BuildHasherDefault<PageIdHasher>>;
pub type PageIdSet = HashSet<PageId, BuildHasherDefault<PageIdHasher>>;
pub type ReadOverlayShard = PageIdMap<Arc<Page>>;

#[derive(Clone)]
pub struct ReadOverlay {
    shards: Arc<Vec<Arc<ReadOverlayShard>>>,
}

impl Default for ReadOverlay {
    fn default() -> Self {
        Self::from_shards(Self::empty_shards())
    }
}

impl ReadOverlay {
    pub fn empty_shared() -> Self {
        static EMPTY: OnceLock<ReadOverlay> = OnceLock::new();
        EMPTY
            .get_or_init(|| Self::from_shards(Self::empty_shards()))
            .clone()
    }

    pub fn empty_shards() -> Vec<Arc<ReadOverlayShard>> {
        let mut shards = Vec::with_capacity(READ_OVERLAY_SHARDS);
        for _ in 0..READ_OVERLAY_SHARDS {
            shards.push(Arc::new(ReadOverlayShard::default()));
        }
        shards
    }

    pub fn from_shards(shards: Vec<Arc<ReadOverlayShard>>) -> Self {
        debug_assert_eq!(shards.len(), READ_OVERLAY_SHARDS);
        Self {
            shards: Arc::new(shards),
        }
    }

    pub fn shard_idx(pid: PageId) -> usize {
        (pid & READ_OVERLAY_SHARD_MASK) as usize
    }

    pub fn len(&self) -> usize {
        self.shards.iter().map(|shard| shard.len()).sum()
    }

    pub fn contains_key(&self, pid: PageId) -> bool {
        self.shards[Self::shard_idx(pid)].contains_key(&pid)
    }

    pub fn get(&self, pid: PageId) -> Option<&Arc<Page>> {
        self.shards[Self::shard_idx(pid)].get(&pid)
    }
}

/// Cheap-to-clone (`Arc`-wrapped) lock-free snapshot of a paged L2P
/// tree, capturing both the root pointer and any dirty pages the apply
/// hasn't yet flushed.
#[derive(Clone)]
pub struct ReadView {
    root: PageId,
    root_level: u8,
    overlay: ReadOverlay,
    page_cache: Arc<PageCache>,
}

impl ReadView {
    pub fn new(
        root: PageId,
        root_level: u8,
        overlay: ReadOverlay,
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
        #[derive(Clone, Copy)]
        struct Walk {
            out_idx: usize,
            leaf_idx: u64,
            bit: usize,
            current: PageId,
        }

        let mut out = vec![None; lbas.len()];
        let mut active = Vec::with_capacity(lbas.len());
        for (out_idx, &lba) in lbas.iter().enumerate() {
            let leaf_idx = lba >> LEAF_SHIFT;
            if leaf_idx > max_leaf_idx_at_level(self.root_level) {
                continue;
            }
            active.push(Walk {
                out_idx,
                leaf_idx,
                bit: (lba & LEAF_MASK) as usize,
                current: self.root,
            });
        }

        let mut level = self.root_level;
        while level > 0 && !active.is_empty() {
            let disk_pids: Vec<PageId> = active
                .iter()
                .filter_map(|walk| {
                    if self.overlay.contains_key(walk.current) {
                        None
                    } else {
                        Some(walk.current)
                    }
                })
                .collect();
            let disk_pages = self.page_cache.get_many(&disk_pids)?;
            let mut disk_iter = disk_pages.into_iter();
            let mut next = Vec::with_capacity(active.len());
            for walk in active {
                let slot = slot_in_index(walk.leaf_idx, level);
                let child = if let Some(page) = self.overlay.get(walk.current) {
                    index_child_at(page, slot)
                } else {
                    let page = disk_iter.next().ok_or_else(|| {
                        crate::error::MetaDbError::Corruption(
                            "paged read_view multi_get disk page iterator underflow".into(),
                        )
                    })?;
                    index_child_at(page.as_ref(), slot)
                };
                if child != NULL_PAGE {
                    next.push(Walk {
                        current: child,
                        ..walk
                    });
                }
            }
            active = next;
            level -= 1;
        }

        if !active.is_empty() {
            let disk_pids: Vec<PageId> = active
                .iter()
                .filter_map(|walk| {
                    if self.overlay.contains_key(walk.current) {
                        None
                    } else {
                        Some(walk.current)
                    }
                })
                .collect();
            let disk_pages = self.page_cache.get_many(&disk_pids)?;
            let mut disk_iter = disk_pages.into_iter();
            for walk in active {
                if let Some(page) = self.overlay.get(walk.current) {
                    if leaf_bit_set(page, walk.bit) {
                        out[walk.out_idx] = Some(leaf_value_at(page, walk.bit));
                    }
                } else {
                    let page = disk_iter.next().ok_or_else(|| {
                        crate::error::MetaDbError::Corruption(
                            "paged read_view multi_get leaf iterator underflow".into(),
                        )
                    })?;
                    if leaf_bit_set(page.as_ref(), walk.bit) {
                        out[walk.out_idx] = Some(leaf_value_at(page.as_ref(), walk.bit));
                    }
                }
            }
        }
        Ok(out)
    }

    fn with_page<T>(&self, pid: PageId, f: impl FnOnce(&Page) -> Result<T>) -> Result<T> {
        if let Some(arc) = self.overlay.get(pid) {
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
        ReadView::new(tree.root(), tree.root_level(), ReadOverlay::default(), pc)
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
