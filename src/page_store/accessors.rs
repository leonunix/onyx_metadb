use super::*;

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
        for submitter in self.io_submitters.iter() {
            submitter.attach_metrics(Arc::clone(&metrics));
        }
        let _ = self.metrics.set(metrics);
    }

    pub(super) fn metrics(&self) -> Option<&Arc<MetaMetrics>> {
        self.metrics.get()
    }

    /// Pick an `IoSubmitter` for a write of `pid`. Returns `None` when
    /// io_uring is unavailable (callers fall back to pwrite).
    ///
    /// Legacy hash routing kept for callers that have no natural lane
    /// class (recovery, verifier tooling, single-writer test
    /// fixtures). Production hot writers should go through
    /// [`Self::io_submitter_for_class`] so they pin to their own
    /// submitter and cannot saturate the SQ of a sibling lane.
    pub(super) fn io_submitter_for(&self, pid: PageId) -> Option<&IoSubmitter> {
        if self.io_submitters.is_empty() {
            None
        } else {
            let idx = (pid as usize) % self.io_submitters.len();
            Some(&self.io_submitters[idx])
        }
    }

    /// Pick the [`IoSubmitter`] reserved for `class`. Returns `None`
    /// when io_uring is unavailable, or when the pool is smaller than
    /// expected (pre-upgrade installs / test fixtures that build
    /// `PageStore` with a forced pool size). In both fallbacks the
    /// caller drops to `pwrite`, preserving the on-disk contract.
    pub(super) fn io_submitter_for_class(&self, class: IoLaneClass) -> Option<&IoSubmitter> {
        self.io_submitters.get(class.index())
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
        self.deferred_free.lock().len()
    }

    /// Next page id that will be handed out by `allocate` if the free list
    /// is empty. Also equals the file's length in pages.
    pub fn high_water(&self) -> u64 {
        self.inner.lock().high_water
    }

    /// Number of pages currently on the free list.
    pub fn free_list_len(&self) -> usize {
        self.inner.lock().free_list.len()
    }

    pub(super) fn check_in_range(&self, page_id: PageId) -> Result<()> {
        let inner = self.inner.lock();
        if page_id >= inner.high_water {
            return Err(MetaDbError::PageOutOfRange(page_id));
        }
        Ok(())
    }
}
