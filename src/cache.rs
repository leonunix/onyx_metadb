//! Shared page cache for B+tree and LSM reads.
//!
//! The cache is sharded to keep mutex contention low and uses a simple
//! per-shard LRU eviction policy. Every cached object is one sealed
//! 4 KiB [`Page`], so capacity accounting is page-count-based even
//! though the public config is expressed in bytes.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use lru::LruCache;
use parking_lot::Mutex;

use crate::config::PAGE_SIZE;
use crate::error::Result;
use crate::page::Page;
use crate::page_store::PageStore;
use crate::types::PageId;

/// Default cache budget used by standalone `BTree` / `Lsm` helpers that
/// are not constructed through [`crate::Db`].
pub const DEFAULT_PAGE_CACHE_BYTES: u64 = 512 * 1024 * 1024;

const CACHE_SHARDS: usize = 16;

/// Snapshot of cache counters.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct PageCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub current_pages: u64,
    pub current_bytes: u64,
    pub capacity_bytes: u64,
}

/// Shared cache over one [`PageStore`].
pub struct PageCache {
    page_store: Arc<PageStore>,
    shards: Vec<Mutex<Shard>>,
    capacity_bytes: u64,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    current_pages: AtomicU64,
}

impl std::fmt::Debug for PageCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageCache")
            .field("capacity_bytes", &self.capacity_bytes)
            .field("stats", &self.stats())
            .finish()
    }
}

#[derive(Debug)]
struct Shard {
    capacity_pages: usize,
    lru: Option<LruCache<PageId, Arc<Page>>>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum InsertOutcome {
    NotStored,
    Replaced,
    Inserted { evicted: bool },
}

impl Shard {
    fn new(capacity_pages: usize) -> Self {
        Self {
            capacity_pages,
            lru: NonZeroUsize::new(capacity_pages).map(LruCache::new),
        }
    }

    fn get(&mut self, page_id: PageId) -> Option<Arc<Page>> {
        self.lru.as_mut()?.get(&page_id).cloned()
    }

    fn pop(&mut self, page_id: PageId) -> Option<Arc<Page>> {
        self.lru.as_mut()?.pop(&page_id)
    }

    fn insert(&mut self, page_id: PageId, page: Arc<Page>) -> InsertOutcome {
        let Some(lru) = self.lru.as_mut() else {
            return InsertOutcome::NotStored;
        };
        if lru.pop(&page_id).is_some() {
            lru.put(page_id, page);
            return InsertOutcome::Replaced;
        }
        let evicted = if lru.len() == self.capacity_pages {
            lru.pop_lru().is_some()
        } else {
            false
        };
        lru.put(page_id, page);
        InsertOutcome::Inserted { evicted }
    }
}

impl PageCache {
    /// Construct a shared page cache for `page_store`.
    pub fn new(page_store: Arc<PageStore>, capacity_bytes: u64) -> Self {
        let total_pages = (capacity_bytes / PAGE_SIZE as u64) as usize;
        let base = total_pages / CACHE_SHARDS;
        let remainder = total_pages % CACHE_SHARDS;
        let mut shards = Vec::with_capacity(CACHE_SHARDS);
        for i in 0..CACHE_SHARDS {
            let cap = base + usize::from(i < remainder);
            shards.push(Mutex::new(Shard::new(cap)));
        }
        Self {
            page_store,
            shards,
            capacity_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            current_pages: AtomicU64::new(0),
        }
    }

    /// Underlying page store.
    pub fn page_store(&self) -> &Arc<PageStore> {
        &self.page_store
    }

    /// Read `page_id` through the shared LRU.
    pub fn get(&self, page_id: PageId) -> Result<Arc<Page>> {
        let shard_idx = self.shard_idx(page_id);
        if let Some(page) = self.shards[shard_idx].lock().get(page_id) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(page);
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        let page = Arc::new(self.page_store.read_page(page_id)?);

        let mut shard = self.shards[shard_idx].lock();
        if let Some(existing) = shard.get(page_id) {
            return Ok(existing);
        }
        self.apply_insert_outcome(shard.insert(page_id, page.clone()));
        Ok(page)
    }

    /// Load a page for mutation.
    ///
    /// If the page is currently cached, the clean entry is removed so a
    /// caller can keep a private dirty copy without exposing stale data
    /// through the shared cache.
    pub fn get_for_modify(&self, page_id: PageId) -> Result<Page> {
        let shard_idx = self.shard_idx(page_id);
        if let Some(page) = self.shards[shard_idx].lock().pop(page_id) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            self.current_pages.fetch_sub(1, Ordering::Relaxed);
            return Ok((*page).clone());
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        self.page_store.read_page(page_id)
    }

    /// Read `page_id` without touching the LRU. Used by long scans so a
    /// compaction pass does not evict the hot working set.
    pub fn get_bypass(&self, page_id: PageId) -> Result<Page> {
        self.page_store.read_page(page_id)
    }

    /// Insert or refresh a clean page in the cache.
    pub fn insert(&self, page_id: PageId, page: Arc<Page>) {
        let shard_idx = self.shard_idx(page_id);
        let outcome = self.shards[shard_idx].lock().insert(page_id, page);
        self.apply_insert_outcome(outcome);
    }

    /// Remove `page_id` from the cache if present.
    pub fn invalidate(&self, page_id: PageId) {
        let shard_idx = self.shard_idx(page_id);
        if self.shards[shard_idx].lock().pop(page_id).is_some() {
            self.current_pages.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Remove a contiguous run of pages from the cache.
    pub fn invalidate_run(&self, start: PageId, count: u32) {
        for page_id in start..start + count as u64 {
            self.invalidate(page_id);
        }
    }

    /// Snapshot cache counters.
    pub fn stats(&self) -> PageCacheStats {
        let current_pages = self.current_pages.load(Ordering::Relaxed);
        PageCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            current_pages,
            current_bytes: current_pages.saturating_mul(PAGE_SIZE as u64),
            capacity_bytes: self.capacity_bytes,
        }
    }

    fn shard_idx(&self, page_id: PageId) -> usize {
        (page_id as usize) % self.shards.len()
    }

    fn apply_insert_outcome(&self, outcome: InsertOutcome) {
        match outcome {
            InsertOutcome::NotStored | InsertOutcome::Replaced => {}
            InsertOutcome::Inserted { evicted } => {
                if evicted {
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.current_pages.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::{PageHeader, PageType};
    use tempfile::TempDir;

    fn mk_cache(capacity_pages: u64) -> (TempDir, Arc<PageStore>, PageCache) {
        let dir = TempDir::new().unwrap();
        let ps = Arc::new(PageStore::create(dir.path().join("p.onyx_meta")).unwrap());
        (
            dir,
            ps.clone(),
            PageCache::new(ps, capacity_pages * PAGE_SIZE as u64),
        )
    }

    fn write_page(ps: &PageStore, pid: PageId, generation: u64) {
        let mut page = Page::new(PageHeader::new(PageType::L2pLeaf, generation));
        page.seal();
        ps.write_page(pid, &page).unwrap();
    }

    #[test]
    fn repeated_get_hits_after_first_miss() {
        let (_dir, ps, cache) = mk_cache(8);
        let pid = ps.allocate().unwrap();
        write_page(&ps, pid, 1);

        let _ = cache.get(pid).unwrap();
        let _ = cache.get(pid).unwrap();

        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.current_pages, 1);
    }

    #[test]
    fn insertions_evict_when_capacity_is_full() {
        let (_dir, ps, cache) = mk_cache(16);
        let p0 = ps.allocate().unwrap();
        for _ in 0..15 {
            let _ = ps.allocate().unwrap();
        }
        let p1 = ps.allocate().unwrap();
        write_page(&ps, p0, 1);
        write_page(&ps, p1, 2);

        let _ = cache.get(p0).unwrap();
        let _ = cache.get(p1).unwrap();

        let stats = cache.stats();
        assert_eq!(stats.current_pages, 1);
        assert_eq!(stats.evictions, 1);
    }

    #[test]
    fn get_for_modify_removes_page_from_shared_cache() {
        let (_dir, ps, cache) = mk_cache(4);
        let pid = ps.allocate().unwrap();
        write_page(&ps, pid, 7);
        let _ = cache.get(pid).unwrap();
        assert_eq!(cache.stats().current_pages, 1);

        let page = cache.get_for_modify(pid).unwrap();
        assert_eq!(page.generation(), 7);
        assert_eq!(cache.stats().current_pages, 0);
    }

    #[test]
    fn bypass_reads_do_not_populate_cache() {
        let (_dir, ps, cache) = mk_cache(4);
        let pid = ps.allocate().unwrap();
        write_page(&ps, pid, 3);

        let page = cache.get_bypass(pid).unwrap();
        assert_eq!(page.generation(), 3);
        assert_eq!(cache.stats().current_pages, 0);
        assert_eq!(cache.stats().hits, 0);
        assert_eq!(cache.stats().misses, 0);
    }
}
