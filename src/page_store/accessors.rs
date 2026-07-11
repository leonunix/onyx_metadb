use super::*;
use std::sync::atomic::Ordering;

impl PageStore {
    /// Path the store was opened from.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Attach the parent `Db`'s metrics handle so page-store IO paths
    /// can record `meta_io_*` counters. Idempotent: calling twice with
    /// the same handle is fine; calling with different handles is a
    /// no-op after the first.
    pub fn attach_metrics(&self, metrics: Arc<MetaMetrics>) {
        self.device.attach_metrics(Arc::clone(&metrics));
        let _ = self.metrics.set(metrics);
    }

    pub(super) fn metrics(&self) -> Option<&Arc<MetaMetrics>> {
        self.metrics.get()
    }

    /// Shared epoch coordinator. Lock-free L2P readers `pin()` here
    /// before walking; deferred-free reclaim respects every active pin.
    pub fn epoch(&self) -> &Arc<EpochManager> {
        &self.epoch
    }

    /// Number of pages currently waiting for an epoch-safe reclaim.
    /// Useful for tests and the metrics layer; do not gate behaviour on
    /// this — production callers use [`try_reclaim`] which atomically
    /// drains.
    pub fn deferred_free_len(&self) -> usize {
        self.deferred_free_pages.load(Ordering::Relaxed)
    }

    /// Next page id that will be handed out by `allocate` if the free list
    /// is empty. Also equals the file's length in pages.
    pub fn high_water(&self) -> u64 {
        self.high_water_pages.load(Ordering::Relaxed)
    }

    /// Number of pages currently on the free list.
    pub fn free_list_len(&self) -> usize {
        self.free_list_pages.load(Ordering::Relaxed)
    }

    /// Atomically snapshot `(high_water, free_bitmap)` under `inner`. The
    /// bitmap is maintained incrementally by allocation/reclaim, so this copies
    /// one bit per managed page instead of cloning and re-encoding the full
    /// `Vec<PageId>` on every manifest commit.
    pub fn snapshot_free_bitmap_and_high_water(&self) -> (u64, Vec<u8>) {
        let inner = self.inner.lock();
        (inner.high_water, inner.free_bitmap.clone())
    }

    /// Install a recovered `(high_water, free_list)` directly, bypassing the
    /// open-time scan. The device path uses this after loading the persisted
    /// free-list bitmap (see `db::lifecycle::open`); it is the no-scan
    /// counterpart to [`rebuild_free_list_bounded`](Self::rebuild_free_list_bounded).
    pub fn install_free_list(&self, high_water: u64, free_list: Vec<PageId>) {
        let free_list_len = free_list.len();
        let free_bitmap = build_free_bitmap(high_water, &free_list);
        let mut inner = self.inner.lock();
        inner.high_water = high_water;
        inner.free_list = free_list;
        inner.free_bitmap = free_bitmap;
        self.high_water_pages.store(high_water, Ordering::Relaxed);
        self.free_list_pages.store(free_list_len, Ordering::Relaxed);
    }

    /// Fixed capacity in pages for a device-backed store (`Some`), or `None`
    /// for a growable file. Feeds the meta-region water-level status.
    pub fn capacity_pages(&self) -> Option<u64> {
        self.device.capacity_pages()
    }

    /// Widen the fixed device ceiling online to `new_pages` after the host
    /// extended the backing store (meta LD `extend_ld`). Errors on a device
    /// that cannot grow in place (see [`PageDevice::grow_capacity_pages`]). The
    /// in-memory `high_water` is unaffected — this only lifts the ceiling that
    /// `ensure_covers` enforces, so stalled allocations resume.
    pub fn grow_device_capacity(&self, new_pages: u64) -> Result<()> {
        self.device.grow_capacity_pages(new_pages)
    }

    pub(super) fn check_in_range(&self, page_id: PageId) -> Result<()> {
        let inner = self.inner.lock();
        if page_id >= inner.high_water {
            return Err(MetaDbError::PageOutOfRange(page_id));
        }
        Ok(())
    }
}
