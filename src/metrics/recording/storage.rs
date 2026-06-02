use std::sync::atomic::Ordering;
use std::time::Duration;

use super::super::*;

impl MetaMetrics {
    pub(crate) fn record_flush_attempt(&self, kind: FlushKind) {
        self.flush_calls.fetch_add(1, Ordering::Relaxed);
        match kind {
            FlushKind::Steady => self.flush_calls_steady.fetch_add(1, Ordering::Relaxed),
            FlushKind::Forced => self.flush_calls_forced.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub(crate) fn record_flush_total(&self, kind: FlushKind, total: Duration) {
        record_duration(&self.flush_total_us, &self.flush_total_max_us, total);
        let (us_slot, max_slot) = match kind {
            FlushKind::Steady => (&self.flush_total_us_steady, &self.flush_total_max_us_steady),
            FlushKind::Forced => (&self.flush_total_us_forced, &self.flush_total_max_us_forced),
        };
        record_duration(us_slot, max_slot, total);
    }

    pub(crate) fn record_flush_gate_wait(&self, elapsed: Duration) {
        record_duration(
            &self.flush_gate_wait_us,
            &self.flush_gate_wait_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_flush_sample(&self, kind: FlushKind, elapsed: Duration) {
        record_duration(&self.flush_sample_us, &self.flush_sample_max_us, elapsed);
        let (us_slot, max_slot) = match kind {
            FlushKind::Steady => (
                &self.flush_sample_us_steady,
                &self.flush_sample_max_us_steady,
            ),
            FlushKind::Forced => (
                &self.flush_sample_us_forced,
                &self.flush_sample_max_us_forced,
            ),
        };
        record_duration(us_slot, max_slot, elapsed);
    }

    /// Sample-phase sub-breakdown. `lock` is the time waiting for every
    /// L2P shard write lock; `l2p_walk` is the dirty-set snapshot loop
    /// over all `(volume, shard)` pairs; `rc_drain` is the per-shard
    /// `RcShard::begin_checkpoint` loop. All three add up to
    /// `flush_sample_us` (modulo a few ns of timer overhead); a sum that
    /// diverges by more than that means the instrumentation is missing
    /// a region.
    pub(crate) fn record_flush_sample_breakdown(
        &self,
        lock: Duration,
        l2p_walk: Duration,
        rc_drain: Duration,
    ) {
        record_duration(
            &self.flush_sample_lock_us,
            &self.flush_sample_lock_max_us,
            lock,
        );
        record_duration(
            &self.flush_sample_l2p_walk_us,
            &self.flush_sample_l2p_walk_max_us,
            l2p_walk,
        );
        record_duration(
            &self.flush_sample_rc_drain_us,
            &self.flush_sample_rc_drain_max_us,
            rc_drain,
        );
    }

    /// Record sample-phase workload size for one flush. Called from the
    /// in-gate sample loop with the totals of L2P dirty pages observed,
    /// refcount delta entries drained, and freshly-allocated refcount
    /// data pages produced. Kind-agnostic — pair with `flush_calls_*`
    /// to compute per-flush averages, or watch the `_max` siblings to
    /// see whether sample-phase workload is itself growing over time.
    pub(crate) fn record_flush_sample_workload(
        &self,
        l2p_dirty_pages: usize,
        rc_drained_deltas: usize,
        rc_fresh_pages: usize,
    ) {
        let l2p = l2p_dirty_pages as u64;
        let drained = rc_drained_deltas as u64;
        let fresh = rc_fresh_pages as u64;
        self.flush_sample_l2p_dirty_pages
            .fetch_add(l2p, Ordering::Relaxed);
        fetch_max(&self.flush_sample_l2p_dirty_pages_max, l2p);
        self.flush_sample_rc_drained_deltas
            .fetch_add(drained, Ordering::Relaxed);
        fetch_max(&self.flush_sample_rc_drained_deltas_max, drained);
        self.flush_sample_rc_fresh_pages
            .fetch_add(fresh, Ordering::Relaxed);
        fetch_max(&self.flush_sample_rc_fresh_pages_max, fresh);
    }

    pub(crate) fn record_flush_io(&self, elapsed: Duration, pages: usize) {
        record_duration(&self.flush_io_us, &self.flush_io_max_us, elapsed);
        self.flush_pages_written
            .fetch_add(pages as u64, Ordering::Relaxed);
        let bytes = (pages as u64).saturating_mul(crate::config::PAGE_SIZE as u64);
        self.flush_io_bytes_total
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub(crate) fn record_meta_io_write_batch(&self, ops: usize, bytes: usize, elapsed: Duration) {
        if ops == 0 {
            return;
        }
        self.meta_io_write_calls.fetch_add(1, Ordering::Relaxed);
        self.meta_io_write_ops
            .fetch_add(ops as u64, Ordering::Relaxed);
        self.meta_io_write_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        record_duration(&self.meta_io_write_us, &self.meta_io_write_max_us, elapsed);
        fetch_max(&self.meta_io_write_batch_ops_max, ops as u64);
        fetch_max(&self.meta_io_write_batch_bytes_max, bytes as u64);
    }

    pub(crate) fn record_meta_io_read_batch(&self, ops: usize, bytes: usize, elapsed: Duration) {
        if ops == 0 {
            return;
        }
        self.meta_io_read_calls.fetch_add(1, Ordering::Relaxed);
        self.meta_io_read_ops
            .fetch_add(ops as u64, Ordering::Relaxed);
        self.meta_io_read_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        record_duration(&self.meta_io_read_us, &self.meta_io_read_max_us, elapsed);
        fetch_max(&self.meta_io_read_batch_ops_max, ops as u64);
    }

    pub(crate) fn record_meta_io_fsync(&self, elapsed: Duration) {
        self.meta_io_fsync_calls.fetch_add(1, Ordering::Relaxed);
        record_duration(&self.meta_io_fsync_us, &self.meta_io_fsync_max_us, elapsed);
    }

    pub(crate) fn record_meta_io_write_uring_lock_wait(&self, elapsed: Duration) {
        self.meta_io_write_uring_lock_acquires
            .fetch_add(1, Ordering::Relaxed);
        record_duration(
            &self.meta_io_write_uring_lock_wait_us,
            &self.meta_io_write_uring_lock_wait_max_us,
            elapsed,
        );
    }

    /// Called by the centralised `IoSubmitter` once per submit-and-wait
    /// iteration. `pulled` = SQEs pushed *this iteration*, `inflight` =
    /// in-flight ops at submit time, `channel_pending` = `rx.len()`
    /// before the drain. Iterations with no new SQEs (pure CQE harvest
    /// rounds) still bump `iterations` so the average batch is accurate.
    pub(crate) fn record_io_submitter_iteration(
        &self,
        pulled: usize,
        inflight: usize,
        channel_pending: usize,
    ) {
        self.io_submitter_iterations.fetch_add(1, Ordering::Relaxed);
        if pulled > 0 {
            self.io_submitter_sqes_submitted
                .fetch_add(pulled as u64, Ordering::Relaxed);
        }
        fetch_max(&self.io_submitter_submit_batch_size_max, pulled as u64);
        fetch_max(&self.io_submitter_inflight_max, inflight as u64);
        fetch_max(
            &self.io_submitter_channel_pending_max,
            channel_pending as u64,
        );
    }

    /// Called once per bg op the submitter has to park in its deferred
    /// queue because `inflight_bg` is at the cap.
    pub(crate) fn record_io_submitter_bg_deferred(&self) {
        self.io_submitter_bg_deferred
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Sampled once per submitter iteration. Tracks the bg admission
    /// gate's high-water marks.
    pub(crate) fn record_io_submitter_bg_inflight(&self, inflight_bg: usize, deferred: usize) {
        fetch_max(&self.io_submitter_bg_inflight_max, inflight_bg as u64);
        fetch_max(&self.io_submitter_bg_deferred_max, deferred as u64);
    }

    pub(crate) fn record_l2p_writeback_cycle(&self, did_work: bool) {
        self.l2p_writeback_cycles.fetch_add(1, Ordering::Relaxed);
        if did_work {
            self.l2p_writeback_cycles_with_work
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_l2p_writeback_seal(&self, elapsed: Duration, pages: usize) {
        record_duration(
            &self.l2p_writeback_seal_us,
            &self.l2p_writeback_seal_max_us,
            elapsed,
        );
        self.l2p_writeback_shards_flushed
            .fetch_add(1, Ordering::Relaxed);
        if pages > 0 {
            self.l2p_writeback_pages_written
                .fetch_add(pages as u64, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_l2p_writeback_io(&self, elapsed: Duration, bytes: usize) {
        record_duration(
            &self.l2p_writeback_io_us,
            &self.l2p_writeback_io_max_us,
            elapsed,
        );
        if bytes > 0 {
            self.l2p_writeback_io_bytes_total
                .fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_l2p_writeback_install(
        &self,
        elapsed: Duration,
        promoted: usize,
        kept: usize,
    ) {
        record_duration(
            &self.l2p_writeback_install_us,
            &self.l2p_writeback_install_max_us,
            elapsed,
        );
        if promoted > 0 {
            self.l2p_writeback_pages_promoted
                .fetch_add(promoted as u64, Ordering::Relaxed);
        }
        if kept > 0 {
            self.l2p_writeback_pages_kept_dirty
                .fetch_add(kept as u64, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_l2p_writeback_error(&self) {
        self.l2p_writeback_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_l2p_writeback_target_skip(&self) {
        self.l2p_writeback_target_skips
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_flush_io_seal(&self, elapsed: Duration) {
        record_duration(&self.flush_io_seal_us, &self.flush_io_seal_max_us, elapsed);
    }

    pub(crate) fn record_flush_io_page_write(&self, elapsed: Duration) {
        record_duration(
            &self.flush_io_page_write_us,
            &self.flush_io_page_write_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_flush_io_rc_meta(&self, elapsed: Duration) {
        record_duration(
            &self.flush_io_rc_meta_us,
            &self.flush_io_rc_meta_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_flush_io_sync(&self, elapsed: Duration) {
        record_duration(&self.flush_io_sync_us, &self.flush_io_sync_max_us, elapsed);
    }

    pub(crate) fn record_flush_manifest(&self, elapsed: Duration) {
        record_duration(
            &self.flush_manifest_us,
            &self.flush_manifest_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_flush_install(&self, elapsed: Duration) {
        record_duration(&self.flush_install_us, &self.flush_install_max_us, elapsed);
    }

    pub(crate) fn record_flush_reclaim(&self, elapsed: Duration) {
        record_duration(&self.flush_reclaim_us, &self.flush_reclaim_max_us, elapsed);
    }

    pub(crate) fn record_flush_reclaim_pages(
        &self,
        budget: usize,
        selected: usize,
        reclaimed: usize,
        blocked: usize,
    ) {
        self.flush_reclaim_budget_pages
            .fetch_add(budget as u64, Ordering::Relaxed);
        self.flush_reclaim_selected_pages
            .fetch_add(selected as u64, Ordering::Relaxed);
        self.flush_reclaim_reclaimed_pages
            .fetch_add(reclaimed as u64, Ordering::Relaxed);
        self.flush_reclaim_blocked_pages
            .fetch_add(blocked as u64, Ordering::Relaxed);
    }

    /// One background reclaim cycle completed. `selected` /
    /// `reclaimed` are the counts returned by the page store;
    /// `elapsed` is the wall-time of the worker iteration. Tracks
    /// the cost we moved off the `flush_with_gate` critical path
    /// — `async_reclaim_cycle_max_us` is what `flush_reclaim_max_us`
    /// used to be, just on a background thread.
    pub(crate) fn record_async_reclaim_cycle(
        &self,
        selected: usize,
        reclaimed: usize,
        elapsed: Duration,
    ) {
        self.async_reclaim_cycles.fetch_add(1, Ordering::Relaxed);
        self.async_reclaim_selected_pages
            .fetch_add(selected as u64, Ordering::Relaxed);
        self.async_reclaim_reclaimed_pages
            .fetch_add(reclaimed as u64, Ordering::Relaxed);
        record_duration(
            &self.async_reclaim_cycle_us,
            &self.async_reclaim_cycle_max_us,
            elapsed,
        );
    }

    /// One L2P buffer compaction cycle completed. `entries` is the
    /// number of `(lba, value, lsn)` tuples drained from the
    /// `draining` slot into the paged radix tree on this shard cycle.
    pub(crate) fn record_l2p_buffer_compaction(&self, entries: usize, elapsed: Duration) {
        self.l2p_buffer_compaction_cycles
            .fetch_add(1, Ordering::Relaxed);
        self.l2p_buffer_compaction_entries
            .fetch_add(entries as u64, Ordering::Relaxed);
        record_duration(
            &self.l2p_buffer_compaction_us,
            &self.l2p_buffer_compaction_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_l2p_buffer_lookup_hit(&self) {
        self.l2p_buffer_lookup_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_l2p_buffer_lookup_miss(&self) {
        self.l2p_buffer_lookup_misses_to_tree
            .fetch_add(1, Ordering::Relaxed);
    }

    /// One refcount drainer cycle completed. `entries`/`pages` are the
    /// drained delta entries and the sealed pages produced; `elapsed`
    /// is the cycle wall-time; `overlay_size` is the post-publish
    /// overlay size for the high-water mark.
    pub(crate) fn record_rc_drainer_cycle(
        &self,
        entries: usize,
        pages: usize,
        elapsed: Duration,
        overlay_size: usize,
    ) {
        self.rc_drainer_cycles.fetch_add(1, Ordering::Relaxed);
        self.rc_drainer_drained_entries
            .fetch_add(entries as u64, Ordering::Relaxed);
        self.rc_drainer_pages_built
            .fetch_add(pages as u64, Ordering::Relaxed);
        record_duration(
            &self.rc_drainer_cycle_us,
            &self.rc_drainer_cycle_max_us,
            elapsed,
        );
        fetch_max(&self.rc_drainer_overlay_size_max_pages, overlay_size as u64);
    }

    pub(crate) fn record_rc_drainer_checkpoint_wait(&self, elapsed: Duration) {
        record_duration(
            &self.rc_drainer_checkpoint_wait_us,
            &self.rc_drainer_checkpoint_wait_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_rc_drainer_pool_refill(&self) {
        self.rc_drainer_pool_refills.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_rc_drainer_wake(&self) {
        self.rc_drainer_wakes.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_rc_drainer_preempt(&self) {
        self.rc_drainer_preempts.fetch_add(1, Ordering::Relaxed);
    }

    // --- async dedup-index drainer (mirrors rc_drainer_*) ---

    pub(crate) fn record_dedup_drainer_cycle(&self, entries: usize, elapsed: Duration) {
        self.dedup_drainer_cycles.fetch_add(1, Ordering::Relaxed);
        self.dedup_drainer_drained_entries
            .fetch_add(entries as u64, Ordering::Relaxed);
        record_duration(
            &self.dedup_drainer_cycle_us,
            &self.dedup_drainer_cycle_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_dedup_drainer_wake(&self) {
        self.dedup_drainer_wakes.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_dedup_drainer_preempt(&self) {
        self.dedup_drainer_preempts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_dedup_drainer_checkpoint_wait(&self, elapsed: Duration) {
        record_duration(
            &self.dedup_drainer_checkpoint_wait_us,
            &self.dedup_drainer_checkpoint_wait_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_dedup_drainer_staged_active(&self, total_active: usize) {
        fetch_max(&self.dedup_drainer_staged_active_max, total_active as u64);
    }

    pub(crate) fn record_dedup_drainer_backpressure_drain(&self) {
        self.dedup_drainer_backpressure_drains
            .fetch_add(1, Ordering::Relaxed);
    }
}
