//! Shared page cache for B+tree and LSM reads.
//!
//! The cache is sharded to keep mutex contention low and uses a simple
//! per-shard LRU-ish eviction policy. Every cached object is one sealed
//! 4 KiB [`Page`], so capacity accounting is page-count-based even
//! though the public config is expressed in bytes.
//!
//! Hot read hits intentionally do not promote LRU position. This lets
//! read hits take only a shard read lock; cache mutation is reserved for
//! misses, insertions, pin/unpin, invalidation, and get-for-modify.
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

use std::collections::{HashMap, hash_map::Entry};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use lru::LruCache;
use parking_lot::RwLock;

use crate::config::PAGE_SIZE;
use crate::error::{MetaDbError, Result};
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
    shards: Vec<RwLock<Shard>>,
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

    fn get(&self, page_id: PageId) -> Option<Arc<Page>> {
        if let Some(page) = self.pinned.get(&page_id) {
            return Some(page.clone());
        }
        self.lru.as_ref()?.peek(&page_id).cloned()
    }

    fn get_ref(&self, page_id: PageId) -> Option<&Page> {
        if let Some(page) = self.pinned.get(&page_id) {
            return Some(page.as_ref());
        }
        self.lru.as_ref()?.peek(&page_id).map(Arc::as_ref)
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
            shards.push(RwLock::new(Shard::new(cap)));
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
        if let Some(page) = self.shards[shard_idx].read().get(page_id) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(page);
        }

        self.misses.fetch_add(1, Ordering::Relaxed);
        let page = Arc::new(self.page_store.read_page(page_id)?);

        // L2P index pages discovered via cold-read (cache miss + reload)
        // skip the LRU and go straight to the pinned table. They're
        // tiny (≤1/256 of leaf bytes) and on every L2P walk's path; once
        // pinned the path stays hot regardless of leaf / dedup_index
        // pressure. Mirror of the `PageBuf::flush` post-write pin in
        // [paged/cache.rs]; both paths exist because pages enter the
        // shared cache via two routes (post-flush insert + read miss).
        let is_index = matches!(
            page.header().map(|h| h.page_type),
            Ok(crate::page::PageType::PagedIndex)
        );
        if is_index {
            if let Some(existing) = self.shards[shard_idx].read().get(page_id) {
                return Ok(existing);
            }
            if self.pin(page_id, page.clone()) {
                return Ok(page);
            }
            // Pin budget exhausted — fall through to the regular LRU
            // insert below so the page is still cached.
        }

        let mut shard = self.shards[shard_idx].write();
        if let Some(existing) = shard.get(page_id) {
            return Ok(existing);
        }
        self.apply_insert_outcome(shard.insert(page_id, page.clone()));
        Ok(page)
    }

    /// Read several pages through the shared cache, batching cache misses into
    /// one page-store request. Returned pages follow `page_ids` order.
    pub(crate) fn get_many(&self, page_ids: &[PageId]) -> Result<Vec<Arc<Page>>> {
        let mut out: Vec<Option<Arc<Page>>> = vec![None; page_ids.len()];
        let mut positions: HashMap<PageId, Vec<usize>> = HashMap::new();
        let mut unique_page_ids = Vec::new();
        for (idx, &page_id) in page_ids.iter().enumerate() {
            match positions.entry(page_id) {
                Entry::Occupied(mut entry) => entry.get_mut().push(idx),
                Entry::Vacant(entry) => {
                    unique_page_ids.push(page_id);
                    entry.insert(vec![idx]);
                }
            }
        }

        let mut unique_misses = Vec::new();

        for &page_id in &unique_page_ids {
            let shard_idx = self.shard_idx(page_id);
            if let Some(page) = self.shards[shard_idx].read().get(page_id) {
                if let Some(idxs) = positions.get(&page_id) {
                    self.hits.fetch_add(idxs.len() as u64, Ordering::Relaxed);
                    for &idx in idxs {
                        out[idx] = Some(page.clone());
                    }
                }
                continue;
            }
            unique_misses.push(page_id);
        }

        if !unique_misses.is_empty() {
            self.misses
                .fetch_add(unique_misses.len() as u64, Ordering::Relaxed);
            let loaded = self.page_store.read_pages(&unique_misses)?;
            for (page_id, page) in unique_misses.into_iter().zip(loaded) {
                let shard_idx = self.shard_idx(page_id);
                let arc = if let Some(existing) = self.shards[shard_idx].read().get(page_id) {
                    existing
                } else {
                    let page = Arc::new(page);
                    let is_index = matches!(
                        page.header().map(|h| h.page_type),
                        Ok(crate::page::PageType::PagedIndex)
                    );
                    if is_index && self.pin(page_id, page.clone()) {
                        page
                    } else {
                        let mut shard = self.shards[shard_idx].write();
                        if let Some(existing) = shard.get(page_id) {
                            existing
                        } else {
                            self.apply_insert_outcome(shard.insert(page_id, page.clone()));
                            page
                        }
                    }
                };
                if let Some(idxs) = positions.remove(&page_id) {
                    for idx in idxs {
                        out[idx] = Some(arc.clone());
                    }
                }
            }
        }

        out.into_iter()
            .map(|page| {
                page.ok_or_else(|| {
                    MetaDbError::Corruption("page_cache get_many left an empty result".into())
                })
            })
            .collect()
    }

    /// Visit distinct page runs while preserving cache statistics in logical
    /// request units. Cached pages are borrowed under one read lock per shard,
    /// avoiding an `Arc` clone for every covered refcount entry. Misses retain
    /// the normal batched read, post-read cache recheck, pin, and insertion
    /// semantics.
    ///
    /// `requests[i].1` is the number of logical entries covered by page run
    /// `i`. `visit` may be invoked in cache-shard order, so callers must use the
    /// supplied request index rather than relying on callback order.
    pub(crate) fn visit_many_weighted(
        &self,
        requests: &[(PageId, usize)],
        mut visit: impl FnMut(usize, &Page) -> Result<()>,
    ) -> Result<()> {
        if requests.is_empty() {
            return Ok(());
        }
        #[cfg(debug_assertions)]
        {
            let mut seen = std::collections::HashSet::new();
            for &(page_id, weight) in requests {
                debug_assert!(weight > 0, "page-cache run weight must be positive");
                debug_assert!(
                    seen.insert(page_id),
                    "PageCache::visit_many_weighted requires distinct page ids"
                );
            }
        }

        let mut shard_counts = [0usize; CACHE_SHARDS];
        for &(page_id, _) in requests {
            shard_counts[self.shard_idx(page_id)] += 1;
        }
        let mut shard_offsets = [0usize; CACHE_SHARDS + 1];
        for shard_idx in 0..CACHE_SHARDS {
            shard_offsets[shard_idx + 1] = shard_offsets[shard_idx] + shard_counts[shard_idx];
        }
        let mut shard_next = shard_offsets;
        let mut shard_request_indices = vec![0usize; requests.len()];
        for (request_idx, &(page_id, _)) in requests.iter().enumerate() {
            let shard_idx = self.shard_idx(page_id);
            shard_request_indices[shard_next[shard_idx]] = request_idx;
            shard_next[shard_idx] += 1;
        }

        let mut hit_count = 0u64;
        let mut miss_request_indices = Vec::new();
        for shard_idx in 0..CACHE_SHARDS {
            let indices =
                &shard_request_indices[shard_offsets[shard_idx]..shard_offsets[shard_idx + 1]];
            if indices.is_empty() {
                continue;
            }
            let shard = self.shards[shard_idx].read();
            for &request_idx in indices {
                let (page_id, weight) = requests[request_idx];
                if let Some(page) = shard.get_ref(page_id) {
                    hit_count += weight as u64;
                    if let Err(err) = visit(request_idx, page) {
                        self.hits.fetch_add(hit_count, Ordering::Relaxed);
                        return Err(err);
                    }
                } else {
                    miss_request_indices.push(request_idx);
                }
            }
        }
        if hit_count != 0 {
            self.hits.fetch_add(hit_count, Ordering::Relaxed);
        }

        if miss_request_indices.is_empty() {
            return Ok(());
        }
        miss_request_indices.sort_unstable();
        self.misses
            .fetch_add(miss_request_indices.len() as u64, Ordering::Relaxed);
        let miss_page_ids: Vec<PageId> = miss_request_indices
            .iter()
            .map(|&request_idx| requests[request_idx].0)
            .collect();
        let loaded = self.page_store.read_pages(&miss_page_ids)?;
        if loaded.len() != miss_page_ids.len() {
            return Err(MetaDbError::Corruption(format!(
                "page_cache weighted read returned {} pages for {} requests",
                loaded.len(),
                miss_page_ids.len()
            )));
        }
        let mut loaded_pages = Vec::with_capacity(loaded.len());
        for ((request_idx, page_id), page) in miss_request_indices
            .into_iter()
            .zip(miss_page_ids)
            .zip(loaded)
        {
            let shard_idx = self.shard_idx(page_id);
            let arc = if let Some(existing) = self.shards[shard_idx].read().get(page_id) {
                existing
            } else {
                let page = Arc::new(page);
                let is_index = matches!(
                    page.header().map(|h| h.page_type),
                    Ok(crate::page::PageType::PagedIndex)
                );
                if is_index && self.pin(page_id, page.clone()) {
                    page
                } else {
                    let mut shard = self.shards[shard_idx].write();
                    if let Some(existing) = shard.get(page_id) {
                        existing
                    } else {
                        self.apply_insert_outcome(shard.insert(page_id, page.clone()));
                        page
                    }
                }
            };
            loaded_pages.push((request_idx, arc));
        }
        for (request_idx, page) in loaded_pages {
            visit(request_idx, &page)?;
        }
        Ok(())
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
        let mut shard = self.shards[shard_idx].write();
        if let Some(page) = shard.get_pinned(page_id) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            return Ok((*page).clone());
        }
        if let Some(page) = shard.pop_lru(page_id) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            self.current_pages.fetch_sub(1, Ordering::Relaxed);
            return Ok((*page).clone());
        }
        drop(shard);
        self.misses.fetch_add(1, Ordering::Relaxed);
        self.page_store.read_page(page_id)
    }

    /// Read `page_id` without touching the LRU. Used by long scans so a
    /// compaction pass does not evict the hot working set.
    pub fn get_bypass(&self, page_id: PageId) -> Result<Page> {
        self.page_store.read_page(page_id)
    }

    /// Read a run without touching the LRU. Used by long scans so a
    /// compaction pass can preserve the hot working set while still
    /// issuing page-store batch reads.
    pub(crate) fn get_many_bypass(&self, page_ids: &[PageId]) -> Result<Vec<Page>> {
        self.page_store.read_pages(page_ids)
    }

    /// Insert or refresh a clean page in the cache.
    pub fn insert(&self, page_id: PageId, page: Arc<Page>) {
        let shard_idx = self.shard_idx(page_id);
        let outcome = self.shards[shard_idx].write().insert(page_id, page);
        self.apply_insert_outcome(outcome);
    }

    /// Atomic invalidate-then-insert under one shard write lock. Use
    /// this when a page is being rewritten in place (paged-meta flush,
    /// cuckoo bucket rewrite, refcount data-page apply) — it eliminates
    /// the gap between [`invalidate`] and [`insert`] where a concurrent
    /// reader could miss the cache, hit the page store, and race with
    /// the new bytes.
    pub fn replace_or_insert(&self, page_id: PageId, page: Arc<Page>) {
        let shard_idx = self.shard_idx(page_id);
        let mut shard = self.shards[shard_idx].write();
        let (was_pinned, was_in_lru) = shard.invalidate(page_id);
        if was_pinned {
            self.pinned_pages.fetch_sub(1, Ordering::Relaxed);
        }
        if was_in_lru {
            self.current_pages.fetch_sub(1, Ordering::Relaxed);
        }
        let outcome = shard.insert(page_id, page);
        drop(shard);
        self.apply_insert_outcome(outcome);
    }

    /// Remove `page_id` from the cache if present — both the LRU and
    /// the pinned table. Called by the COW path whenever a page is
    /// freed (so a pinned stale entry cannot shadow a reallocated
    /// pid).
    pub fn invalidate(&self, page_id: PageId) {
        let shard_idx = self.shard_idx(page_id);
        let (was_pinned, was_in_lru) = self.shards[shard_idx].write().invalidate(page_id);
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
        let mut shard = self.shards[shard_idx].write();
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
        if self.shards[shard_idx].write().unpin(page_id).is_some() {
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
    fn weighted_visitor_decodes_hits_and_misses_in_request_slots() {
        let (_dir, ps, cache) = mk_cache(16);
        let p0 = ps.allocate().unwrap();
        let p1 = ps.allocate().unwrap();
        let p2 = ps.allocate().unwrap();
        write_page(&ps, p0, 10);
        write_page(&ps, p1, 20);
        write_page(&ps, p2, 30);
        cache.get(p0).unwrap();
        cache.get(p1).unwrap();
        let before = cache.stats();

        let requests = [(p1, 3), (p2, 2), (p0, 4)];
        let mut generations = vec![0; requests.len()];
        cache
            .visit_many_weighted(&requests, |request_idx, page| {
                generations[request_idx] = page.header()?.generation;
                Ok(())
            })
            .unwrap();

        assert_eq!(generations, vec![20, 30, 10]);
        let after = cache.stats();
        assert_eq!(after.hits - before.hits, 7);
        assert_eq!(after.misses - before.misses, 1);
        assert_eq!(after.current_pages, 3);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "visit_many_weighted requires distinct page ids")]
    fn weighted_visitor_debug_contract_rejects_duplicate_ids() {
        let (_dir, ps, cache) = mk_cache(8);
        let page_id = ps.allocate().unwrap();
        cache
            .visit_many_weighted(&[(page_id, 1), (page_id, 2)], |_, _| Ok(()))
            .unwrap();
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
    fn read_hits_do_not_promote_lru_position() {
        // 32 total pages = 2 pages per cache shard. Allocate pages 16
        // apart so all three land in the same shard.
        let (_dir, ps, cache) = mk_cache(32);
        let p0 = ps.allocate().unwrap();
        for _ in 0..15 {
            let _ = ps.allocate().unwrap();
        }
        let p1 = ps.allocate().unwrap();
        for _ in 0..15 {
            let _ = ps.allocate().unwrap();
        }
        let p2 = ps.allocate().unwrap();
        write_page(&ps, p0, 1);
        write_page(&ps, p1, 2);
        write_page(&ps, p2, 3);

        let _ = cache.get(p0).unwrap();
        let _ = cache.get(p1).unwrap();
        let _ = cache.get(p0).unwrap(); // hit, but intentionally no LRU promotion
        let _ = cache.get(p2).unwrap(); // evicts p0, not p1
        let _ = cache.get(p0).unwrap(); // miss again if hit did not promote

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 4);
        assert_eq!(stats.evictions, 2);
        assert_eq!(stats.current_pages, 2);
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

    fn mk_cache_with_pin(cache_pages: u64, pin_pages: u64) -> (TempDir, Arc<PageStore>, PageCache) {
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
        assert_eq!(
            cache.stats().pinned_pages,
            1,
            "pin must survive get_for_modify"
        );
    }
}
