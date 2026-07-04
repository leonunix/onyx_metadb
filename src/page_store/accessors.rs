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

    /// Fixed capacity in pages for a device-backed store (`Some`), or `None`
    /// for a growable file. Feeds the meta-region water-level status.
    pub fn capacity_pages(&self) -> Option<u64> {
        self.device.capacity_pages()
    }

    pub(super) fn check_in_range(&self, page_id: PageId) -> Result<()> {
        let inner = self.inner.lock();
        if page_id >= inner.high_water {
            return Err(MetaDbError::PageOutOfRange(page_id));
        }
        Ok(())
    }
}
