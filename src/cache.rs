//! Shared page cache for B+tree and LSM reads.
//!
//! The cache is sharded to keep mutex contention low and uses a simple
//! per-shard LRU eviction policy. Every cached object is one sealed
//! 4 KiB [`Page`], so capacity accounting is page-count-based even
//! though the public config is expressed in bytes.
//!
//! # Pinned pages
//!
//! Each shard also holds a `pinned: HashMap<PageId, Arc<Page>>` that
//! lives outside the LRU. Pages placed via [`PageCache::pin`] never
//! get evicted and do not count against the LRU's capacity, at the
//! cost of a fixed `pin_budget_bytes` ceiling enforced at pin time.
//! `get` checks the pinned table before the LRU; `invalidate` removes
//! from both. Intended for L2P index pages (≤ 1/256 of leaf bytes, so
//! practically always in-cache regardless of cache pressure).

use std::collections::HashMap;
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
    pub pinned_pages: u64,
    pub pinned_bytes: u64,
    pub pin_budget_bytes: u64,
}

/// Shared cache over one [`PageStore`].
pub struct PageCache {
    page_store: Arc<PageStore>,
    shards: Vec<Mutex<Shard>>,
    capacity_bytes: u64,
    pin_budget_bytes: u64,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    current_pages: AtomicU64,
    pinned_pages: AtomicU64,
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
    /// Pages held outside the LRU. Every entry here is immune to
    /// eviction and is the first thing `get` checks. Budget is
    /// enforced by the enclosing `PageCache` at pin time.
    pinned: HashMap<PageId, Arc<Page>>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum InsertOutcome {
    /// Shard has no LRU capacity (`capacity_pages == 0`) and the page
    /// is not pinned; nothing was stored.
    NotStored,
    /// Replaced an existing entry (same pid, whether in pinned or LRU).
    Replaced,
    /// Net-new entry in the LRU.
    Inserted { evicted: bool },
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct PinOutcome {
    /// New entry added to the pinned table (vs. replacing an
    /// existing pin).
    inserted: bool,
    /// An entry for the same pid lived in the LRU and was evicted
    /// so it doesn't double-count.
    evicted_from_lru: bool,
}

impl Shard {
    fn new(capacity_pages: usize) -> Self {
        Self {
            capacity_pages,
            lru: NonZeroUsize::new(capacity_pages).map(LruCache::new),
            pinned: HashMap::new(),
        }
    }

    fn get(&mut self, page_id: PageId) -> Option<Arc<Page>> {
        if let Some(page) = self.pinned.get(&page_id) {
            return Some(page.clone());
        }
        self.lru.as_mut()?.get(&page_id).cloned()
    }

    /// Pinned-table peek without LRU promotion. Returns a clone of
    /// the pinned `Arc<Page>` so callers that want owned bytes can
    /// clone the page without disturbing the pin.
    fn get_pinned(&self, page_id: PageId) -> Option<Arc<Page>> {
        self.pinned.get(&page_id).cloned()
    }

    /// Pop from the LRU only; never touches the pinned table. Used
    /// by `get_for_modify` after it has ruled out a pinned entry.
    fn pop_lru(&mut self, page_id: PageId) -> Option<Arc<Page>> {
        self.lru.as_mut()?.pop(&page_id)
    }

    fn insert(&mut self, page_id: PageId, page: Arc<Page>) -> InsertOutcome {
        // Content updates for a pinned page replace the pinned entry
        // so subsequent `get` observes the new bytes. The pin counter
        // does not change.
        if let Some(existing) = self.pinned.get_mut(&page_id) {
            *existing = page;
            return InsertOutcome::Replaced;
        }
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

    /// Add or replace `page_id` in the pinned set. Removes any LRU
    /// copy so the page isn't double-counted across LRU and pinned.
    fn pin(&mut self, page_id: PageId, page: Arc<Page>) -> PinOutcome {
        let evicted_from_lru = self
            .lru
            .as_mut()
            .and_then(|lru| lru.pop(&page_id))
            .is_some();
        let inserted = self.pinned.insert(page_id, page).is_none();
        PinOutcome {
            inserted,
            evicted_from_lru,
        }
    }

    /// Remove from pinned and return the page if present. Does not
    /// re-insert into LRU — the next `get` will repopulate naturally.
    fn unpin(&mut self, page_id: PageId) -> Option<Arc<Page>> {
        self.pinned.remove(&page_id)
    }

    /// Remove from both pinned and LRU. Returns whether pinned had an
    /// entry (so `PageCache` can decrement the pin counter) and
    /// whether LRU had an entry (so it can decrement current_pages).
    fn invalidate(&mut self, page_id: PageId) -> (bool, bool) {
        let had_pinned = self.pinned.remove(&page_id).is_some();
        let had_lru = self
            .lru
            .as_mut()
            .and_then(|lru| lru.pop(&page_id))
            .is_some();
        (had_pinned, had_lru)
    }
}

impl PageCache {
    /// Construct a shared page cache for `page_store` without a pin
    /// budget. Equivalent to `new_with_pin_budget(store, cap, 0)`.
    pub fn new(page_store: Arc<PageStore>, capacity_bytes: u64) -> Self {
        Self::new_with_pin_budget(page_store, capacity_bytes, 0)
    }

    /// Construct a shared page cache. `capacity_bytes` is the LRU
    /// budget; `pin_budget_bytes` caps how many bytes may live in the
    /// per-shard pinned tables (sum across shards, enforced at pin
    /// time). A zero `pin_budget_bytes` disables pinning — every
    /// `pin()` returns `false` and callers stay on the LRU path.
    pub fn new_with_pin_budget(
        page_store: Arc<PageStore>,
        capacity_bytes: u64,
        pin_budget_bytes: u64,
    ) -> Self {
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
            pin_budget_bytes,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            current_pages: AtomicU64::new(0),
            pinned_pages: AtomicU64::new(0),
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
    /// If the page is cached in the LRU, the clean entry is removed so
    /// the caller can keep a private dirty copy without exposing stale
    /// data through the shared cache. Pinned pages stay pinned — the
    /// caller receives a cloned copy, and when the mutated page is
    /// eventually written back via [`PageCache::insert`], the pinned
    /// entry is refreshed in place.
    pub fn get_for_modify(&self, page_id: PageId) -> Result<Page> {
        let shard_idx = self.shard_idx(page_id);
        {
            let shard = self.shards[shard_idx].lock();
            if let Some(page) = shard.get_pinned(page_id) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                return Ok((*page).clone());
            }
        }
        if let Some(page) = self.shards[shard_idx].lock().pop_lru(page_id) {
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

    /// Remove `page_id` from the cache if present — both the LRU and
    /// the pinned table. Called by the COW path whenever a page is
    /// freed (so a pinned stale entry cannot shadow a reallocated
    /// pid).
    pub fn invalidate(&self, page_id: PageId) {
        let shard_idx = self.shard_idx(page_id);
        let (was_pinned, was_in_lru) = self.shards[shard_idx].lock().invalidate(page_id);
        if was_pinned {
            self.pinned_pages.fetch_sub(1, Ordering::Relaxed);
        }
        if was_in_lru {
            self.current_pages.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Remove a contiguous run of pages from the cache.
    pub fn invalidate_run(&self, start: PageId, count: u32) {
        for page_id in start..start + count as u64 {
            self.invalidate(page_id);
        }
    }

    /// Pin `page_id` in the cache so it is never evicted by LRU
    /// pressure. Returns `true` if the page was pinned (or was
    /// already pinned, in which case the stored content is refreshed),
    /// or `false` if the per-cache pin budget would be exceeded. On
    /// `false` the caller should fall back to the regular LRU path —
    /// pin failure is not fatal, just a cache-policy decision.
    pub fn pin(&self, page_id: PageId, page: Arc<Page>) -> bool {
        if self.pin_budget_bytes == 0 {
            return false;
        }
        let budget_pages = self.pin_budget_bytes / PAGE_SIZE as u64;
        let shard_idx = self.shard_idx(page_id);
        let mut shard = self.shards[shard_idx].lock();
        let already_pinned = shard.pinned.contains_key(&page_id);
        if !already_pinned {
            // Budget is a snapshot read under the shard lock — any
            // concurrent pin on a different shard can still complete,
            // which is fine; the bound is approximate, not precise.
            // Replacements pass through regardless of budget so
            // write-back paths refresh pinned content.
            let current = self.pinned_pages.load(Ordering::Relaxed);
            if current >= budget_pages {
                return false;
            }
        }
        let outcome = shard.pin(page_id, page);
        drop(shard);
        if outcome.inserted {
            self.pinned_pages.fetch_add(1, Ordering::Relaxed);
        }
        if outcome.evicted_from_lru {
            self.current_pages.fetch_sub(1, Ordering::Relaxed);
        }
        true
    }

    /// Remove `page_id` from the pinned set. No-op if the page was
    /// not pinned. Does not re-insert into the LRU; the next `get`
    /// populates naturally if the page is still live on disk.
    pub fn unpin(&self, page_id: PageId) {
        let shard_idx = self.shard_idx(page_id);
        if self.shards[shard_idx].lock().unpin(page_id).is_some() {
            self.pinned_pages.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Current number of pinned pages across all shards.
    pub fn pinned_pages(&self) -> u64 {
        self.pinned_pages.load(Ordering::Relaxed)
    }

    /// Current bytes held in the pinned set. Derived from
    /// [`PageCache::pinned_pages`] and the fixed page size.
    pub fn pinned_bytes(&self) -> u64 {
        self.pinned_pages() * PAGE_SIZE as u64
    }

    /// Per-cache pin budget (bytes). `0` means pinning is disabled.
    pub fn pin_budget_bytes(&self) -> u64 {
        self.pin_budget_bytes
    }

    /// Snapshot cache counters.
    pub fn stats(&self) -> PageCacheStats {
        let current_pages = self.current_pages.load(Ordering::Relaxed);
        let pinned_pages = self.pinned_pages.load(Ordering::Relaxed);
        PageCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            current_pages,
            current_bytes: current_pages.saturating_mul(PAGE_SIZE as u64),
            capacity_bytes: self.capacity_bytes,
            pinned_pages,
            pinned_bytes: pinned_pages.saturating_mul(PAGE_SIZE as u64),
            pin_budget_bytes: self.pin_budget_bytes,
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

    // -------- P2: pinned pages ------------------------------------------

    fn mk_cache_with_pin(
        cache_pages: u64,
        pin_pages: u64,
    ) -> (TempDir, Arc<PageStore>, PageCache) {
        let dir = TempDir::new().unwrap();
        let ps = Arc::new(PageStore::create(dir.path().join("p.onyx_meta")).unwrap());
        let cache = PageCache::new_with_pin_budget(
            ps.clone(),
            cache_pages * PAGE_SIZE as u64,
            pin_pages * PAGE_SIZE as u64,
        );
        (dir, ps, cache)
    }

    #[test]
    fn pin_stores_page_outside_lru() {
        let (_dir, ps, cache) = mk_cache_with_pin(2, 2);
        let pid = ps.allocate().unwrap();
        write_page(&ps, pid, 7);
        let page = Arc::new(ps.read_page(pid).unwrap());

        assert!(cache.pin(pid, page.clone()));
        let stats = cache.stats();
        assert_eq!(stats.pinned_pages, 1);
        assert_eq!(stats.current_pages, 0, "pinned must not count against LRU");
    }

    #[test]
    fn get_returns_pinned_before_lru() {
        let (_dir, ps, cache) = mk_cache_with_pin(2, 2);
        let pid = ps.allocate().unwrap();
        write_page(&ps, pid, 1);
        // Pre-populate LRU with stale content...
        let _ = cache.get(pid).unwrap();
        // ... then rewrite the page on disk and pin the new content.
        let mut fresh = Page::new(PageHeader::new(PageType::L2pLeaf, 99));
        fresh.seal();
        let pinned = Arc::new(fresh);
        assert!(cache.pin(pid, pinned.clone()));
        // get returns the pinned content, not the LRU entry.
        let got = cache.get(pid).unwrap();
        assert_eq!(got.generation(), 99);
    }

    #[test]
    fn pin_ejects_existing_lru_entry_without_double_counting() {
        let (_dir, ps, cache) = mk_cache_with_pin(4, 4);
        let pid = ps.allocate().unwrap();
        write_page(&ps, pid, 1);
        let _ = cache.get(pid).unwrap();
        assert_eq!(cache.stats().current_pages, 1);
        let page = Arc::new(ps.read_page(pid).unwrap());
        assert!(cache.pin(pid, page));
        let stats = cache.stats();
        assert_eq!(stats.pinned_pages, 1);
        assert_eq!(stats.current_pages, 0, "pin must pop the LRU copy");
    }

    #[test]
    fn pin_disabled_when_budget_is_zero() {
        let (_dir, ps, cache) = mk_cache_with_pin(4, 0);
        let pid = ps.allocate().unwrap();
        write_page(&ps, pid, 1);
        let page = Arc::new(ps.read_page(pid).unwrap());
        assert!(!cache.pin(pid, page));
        assert_eq!(cache.stats().pinned_pages, 0);
    }

    #[test]
    fn pin_respects_budget_but_still_refreshes_existing_pins() {
        let (_dir, ps, cache) = mk_cache_with_pin(4, 1);
        let p0 = ps.allocate().unwrap();
        let p1 = ps.allocate().unwrap();
        write_page(&ps, p0, 10);
        write_page(&ps, p1, 20);
        let pg0 = Arc::new(ps.read_page(p0).unwrap());
        let pg1 = Arc::new(ps.read_page(p1).unwrap());
        assert!(cache.pin(p0, pg0.clone()));
        assert!(!cache.pin(p1, pg1), "budget (1 page) exhausted by p0");
        // Replacing an existing pin succeeds even when at capacity.
        assert!(cache.pin(p0, pg0));
        assert_eq!(cache.stats().pinned_pages, 1);
    }

    #[test]
    fn invalidate_removes_pinned_and_lru_entries() {
        let (_dir, ps, cache) = mk_cache_with_pin(4, 4);
        let pid = ps.allocate().unwrap();
        write_page(&ps, pid, 1);
        let page = Arc::new(ps.read_page(pid).unwrap());
        assert!(cache.pin(pid, page));
        assert_eq!(cache.stats().pinned_pages, 1);
        cache.invalidate(pid);
        let stats = cache.stats();
        assert_eq!(stats.pinned_pages, 0);
        assert_eq!(stats.current_pages, 0);
    }

    #[test]
    fn unpin_removes_from_pinned_only() {
        let (_dir, ps, cache) = mk_cache_with_pin(4, 4);
        let pid = ps.allocate().unwrap();
        write_page(&ps, pid, 1);
        let page = Arc::new(ps.read_page(pid).unwrap());
        cache.pin(pid, page);
        cache.unpin(pid);
        assert_eq!(cache.stats().pinned_pages, 0);
        // Next get falls through to page_store and repopulates LRU.
        let _ = cache.get(pid).unwrap();
        assert_eq!(cache.stats().current_pages, 1);
    }

    #[test]
    fn pinned_page_survives_lru_eviction_pressure() {
        // LRU holds 1 page, pinned budget holds 1 page.
        let (_dir, ps, cache) = mk_cache_with_pin(1, 1);
        let pinned_pid = ps.allocate().unwrap();
        write_page(&ps, pinned_pid, 42);
        let pinned_page = Arc::new(ps.read_page(pinned_pid).unwrap());
        assert!(cache.pin(pinned_pid, pinned_page));
        // Thrash the LRU with a bunch of other pages.
        for _ in 0..8 {
            let other = ps.allocate().unwrap();
            write_page(&ps, other, 1);
            let _ = cache.get(other).unwrap();
        }
        // Pinned page is still resident.
        let got = cache.get(pinned_pid).unwrap();
        assert_eq!(got.generation(), 42);
    }

    #[test]
    fn insert_refreshes_pinned_page_content() {
        let (_dir, ps, cache) = mk_cache_with_pin(4, 4);
        let pid = ps.allocate().unwrap();
        write_page(&ps, pid, 1);
        let pg_v1 = Arc::new(ps.read_page(pid).unwrap());
        cache.pin(pid, pg_v1);
        // Simulate a write-back: fresh content, same pid.
        let mut fresh = Page::new(PageHeader::new(PageType::L2pLeaf, 2));
        fresh.seal();
        cache.insert(pid, Arc::new(fresh));
        // Reading returns the updated content (pinned entry replaced
        // in place, no LRU growth).
        let got = cache.get(pid).unwrap();
        assert_eq!(got.generation(), 2);
        let stats = cache.stats();
        assert_eq!(stats.pinned_pages, 1);
        assert_eq!(stats.current_pages, 0);
    }

    #[test]
    fn get_for_modify_on_pinned_returns_clone_keeps_pin() {
        let (_dir, ps, cache) = mk_cache_with_pin(4, 4);
        let pid = ps.allocate().unwrap();
        write_page(&ps, pid, 1);
        let page = Arc::new(ps.read_page(pid).unwrap());
        cache.pin(pid, page);
        let _owned = cache.get_for_modify(pid).unwrap();
        assert_eq!(cache.stats().pinned_pages, 1, "pin must survive get_for_modify");
    }
}
