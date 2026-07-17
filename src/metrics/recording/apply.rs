use std::sync::atomic::Ordering;
use std::time::Duration;

use super::super::*;

impl MetaMetrics {
    pub(crate) fn record_apply_l2p_put(&self, elapsed: Duration) {
        self.apply_l2p_put_count.fetch_add(1, Ordering::Relaxed);
        record_duration(&self.apply_l2p_put_us, &self.apply_l2p_put_max_us, elapsed);
    }

    pub(crate) fn record_apply_l2p_put_batch(&self, count: u64, elapsed: Duration) {
        if count == 0 {
            return;
        }
        self.apply_l2p_put_count.fetch_add(count, Ordering::Relaxed);
        record_batch_duration(
            &self.apply_l2p_put_us,
            &self.apply_l2p_put_max_us,
            count,
            elapsed,
        );
    }

    pub(crate) fn record_apply_l2p_delete(&self, elapsed: Duration) {
        self.apply_l2p_delete_count.fetch_add(1, Ordering::Relaxed);
        record_duration(
            &self.apply_l2p_delete_us,
            &self.apply_l2p_delete_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_apply_l2p_delete_batch(&self, count: u64, elapsed: Duration) {
        if count == 0 {
            return;
        }
        self.apply_l2p_delete_count
            .fetch_add(count, Ordering::Relaxed);
        record_batch_duration(
            &self.apply_l2p_delete_us,
            &self.apply_l2p_delete_max_us,
            count,
            elapsed,
        );
    }

    pub(crate) fn record_apply_l2p_remap(&self, elapsed: Duration) {
        self.apply_l2p_remap_count.fetch_add(1, Ordering::Relaxed);
        record_duration(
            &self.apply_l2p_remap_us,
            &self.apply_l2p_remap_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_apply_l2p_remap_batch(&self, count: u64, elapsed: Duration) {
        if count == 0 {
            return;
        }
        self.apply_l2p_remap_count
            .fetch_add(count, Ordering::Relaxed);
        record_batch_duration(
            &self.apply_l2p_remap_us,
            &self.apply_l2p_remap_max_us,
            count,
            elapsed,
        );
    }

    pub(crate) fn record_apply_l2p_range_delete(&self, elapsed: Duration) {
        self.apply_l2p_range_delete_count
            .fetch_add(1, Ordering::Relaxed);
        record_duration(
            &self.apply_l2p_range_delete_us,
            &self.apply_l2p_range_delete_max_us,
            elapsed,
        );
    }

    /// Record the apply of one `WalOp::L2pRemapRange` covering `lba_count`
    /// LBAs. `count` (ops) increments once per range op; `lbas` accumulates
    /// the total LBA work amortized over those ops, so dashboards can
    /// compare `us / count` (per-range-op cost) vs `us / lbas` (per-LBA cost).
    pub(crate) fn record_apply_l2p_remap_range(&self, lba_count: u64, elapsed: Duration) {
        self.apply_l2p_remap_range_count
            .fetch_add(1, Ordering::Relaxed);
        self.apply_l2p_remap_range_lbas
            .fetch_add(lba_count, Ordering::Relaxed);
        record_duration(
            &self.apply_l2p_remap_range_us,
            &self.apply_l2p_remap_range_max_us,
            elapsed,
        );
    }

    /// Laned-path attribution helper: bump op count by `ops` without touching
    /// lbas/us. Paired with `record_apply_l2p_remap_range_lane_work` so an
    /// op may produce one count + multiple shard-bucket work contributions.
    pub(crate) fn record_apply_l2p_remap_range_lane_ops(&self, ops: u64) {
        self.apply_l2p_remap_range_count
            .fetch_add(ops, Ordering::Relaxed);
    }

    /// Laned-path attribution helper: a single shard bucket's range-LBA
    /// contribution. Adds `lbas` and prorated `elapsed` without bumping the
    /// op count (one range op can span multiple shards, so shards must not
    /// each count themselves).
    pub(crate) fn record_apply_l2p_remap_range_lane_work(&self, lbas: u64, elapsed: Duration) {
        if lbas == 0 {
            return;
        }
        self.apply_l2p_remap_range_lbas
            .fetch_add(lbas, Ordering::Relaxed);
        record_duration(
            &self.apply_l2p_remap_range_us,
            &self.apply_l2p_remap_range_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_apply_l2p_bucket_stages(
        &self,
        ops: u64,
        total: Duration,
        tree_lock_wait: Duration,
        read_view_prepare: Duration,
        ops_elapsed: Duration,
        finish: Duration,
        publish: Duration,
    ) {
        self.apply_l2p_bucket_count.fetch_add(1, Ordering::Relaxed);
        self.apply_l2p_bucket_ops.fetch_add(ops, Ordering::Relaxed);
        record_duration(
            &self.apply_l2p_bucket_total_us,
            &self.apply_l2p_bucket_total_max_us,
            total,
        );
        record_duration(
            &self.apply_l2p_bucket_tree_lock_wait_us,
            &self.apply_l2p_bucket_tree_lock_wait_max_us,
            tree_lock_wait,
        );
        record_duration(
            &self.apply_l2p_bucket_read_view_prepare_us,
            &self.apply_l2p_bucket_read_view_prepare_max_us,
            read_view_prepare,
        );
        record_duration(
            &self.apply_l2p_bucket_ops_us,
            &self.apply_l2p_bucket_ops_max_us,
            ops_elapsed,
        );
        record_duration(
            &self.apply_l2p_bucket_finish_us,
            &self.apply_l2p_bucket_finish_max_us,
            finish,
        );
        record_duration(
            &self.apply_l2p_bucket_publish_us,
            &self.apply_l2p_bucket_publish_max_us,
            publish,
        );
    }

    pub(crate) fn record_apply_refcount(&self, elapsed: Duration) {
        self.apply_refcount_count.fetch_add(1, Ordering::Relaxed);
        record_duration(
            &self.apply_refcount_us,
            &self.apply_refcount_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_apply_refcount_batch(&self, ops: u64, elapsed: Duration) {
        if ops == 0 {
            return;
        }
        self.apply_refcount_count.fetch_add(ops, Ordering::Relaxed);
        record_duration(
            &self.apply_refcount_us,
            &self.apply_refcount_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_apply_refcount_batch_breakdown(
        &self,
        actions: u64,
        pbas: u64,
        pba_grouping: Duration,
        base_page_lookup: Duration,
        fold_lock_wait: Duration,
        slot_lock_wait: Duration,
        pending_slot_scan: Duration,
        delta_merge: Duration,
        base_lookup_attempts: u64,
        epoch_retries: u64,
        sampled_pbas: u64,
    ) {
        self.apply_refcount_batch_count
            .fetch_add(1, Ordering::Relaxed);
        self.apply_refcount_batch_actions
            .fetch_add(actions, Ordering::Relaxed);
        self.apply_refcount_batch_pbas
            .fetch_add(pbas, Ordering::Relaxed);
        self.apply_refcount_base_lookup_attempts
            .fetch_add(base_lookup_attempts, Ordering::Relaxed);
        self.apply_refcount_epoch_retries
            .fetch_add(epoch_retries, Ordering::Relaxed);
        record_duration(
            &self.apply_refcount_pba_grouping_us,
            &self.apply_refcount_pba_grouping_max_us,
            pba_grouping,
        );
        if sampled_pbas == 0 {
            return;
        }
        self.apply_refcount_breakdown_sampled_pbas
            .fetch_add(sampled_pbas, Ordering::Relaxed);
        record_duration(
            &self.apply_refcount_base_page_lookup_us,
            &self.apply_refcount_base_page_lookup_max_us,
            base_page_lookup,
        );
        record_duration(
            &self.apply_refcount_fold_lock_wait_us,
            &self.apply_refcount_fold_lock_wait_max_us,
            fold_lock_wait,
        );
        record_duration(
            &self.apply_refcount_slot_lock_wait_us,
            &self.apply_refcount_slot_lock_wait_max_us,
            slot_lock_wait,
        );
        record_duration(
            &self.apply_refcount_pending_slot_scan_us,
            &self.apply_refcount_pending_slot_scan_max_us,
            pending_slot_scan,
        );
        record_duration(
            &self.apply_refcount_delta_merge_us,
            &self.apply_refcount_delta_merge_max_us,
            delta_merge,
        );
    }

    pub(crate) fn record_apply_dedup(&self, elapsed: Duration) {
        self.apply_dedup_count.fetch_add(1, Ordering::Relaxed);
        record_duration(&self.apply_dedup_us, &self.apply_dedup_max_us, elapsed);
    }

    pub(crate) fn record_apply_dedup_batch(&self, ops: u64, elapsed: Duration) {
        if ops == 0 {
            return;
        }
        self.apply_dedup_count.fetch_add(ops, Ordering::Relaxed);
        record_duration(&self.apply_dedup_us, &self.apply_dedup_max_us, elapsed);
    }

    pub(crate) fn record_dedup_lane_task(
        &self,
        ops: u64,
        ready_queue_wait: Duration,
        exec: Duration,
    ) {
        self.dedup_lane_tasks.fetch_add(1, Ordering::Relaxed);
        self.dedup_lane_ops.fetch_add(ops, Ordering::Relaxed);
        record_duration(
            &self.dedup_lane_ready_queue_wait_us,
            &self.dedup_lane_ready_queue_wait_max_us,
            ready_queue_wait,
        );
        record_duration(&self.dedup_lane_exec_us, &self.dedup_lane_exec_max_us, exec);
    }

    /// Record one completed L2P apply lane task. `queue_wait` is the
    /// time from `enqueue_task()` to when the worker popped the task.
    /// `exec` is the wall time spent inside the work() closure. `shard`
    /// is the lane ordinal; per-shard arrays use it to attribute the
    /// task without collapsing into the aggregate.
    pub(crate) fn record_l2p_apply_lane_task(
        &self,
        shard: usize,
        queue_wait: Duration,
        exec: Duration,
    ) {
        self.l2p_apply_lane_tasks.fetch_add(1, Ordering::Relaxed);
        record_duration(
            &self.l2p_apply_lane_queue_wait_us,
            &self.l2p_apply_lane_queue_wait_max_us,
            queue_wait,
        );
        record_duration(
            &self.l2p_apply_lane_exec_us,
            &self.l2p_apply_lane_exec_max_us,
            exec,
        );
        if let Some(slot) = self.l2p_apply_lane_shard_tasks.get(shard) {
            slot.fetch_add(1, Ordering::Relaxed);
            record_duration(
                &self.l2p_apply_lane_shard_queue_wait_us[shard],
                &self.l2p_apply_lane_shard_queue_wait_max_us[shard],
                queue_wait,
            );
            record_duration(
                &self.l2p_apply_lane_shard_exec_us[shard],
                &self.l2p_apply_lane_shard_exec_max_us[shard],
                exec,
            );
        }
    }

    pub(crate) fn record_l2p_apply_lane_idle(&self, shard: usize, idle: Duration) {
        record_duration(
            &self.l2p_apply_lane_idle_us,
            &self.l2p_apply_lane_idle_max_us,
            idle,
        );
        if let Some(slot) = self.l2p_apply_lane_shard_idle_us.get(shard) {
            let us = idle.as_micros().min(u128::from(u64::MAX)) as u64;
            slot.fetch_add(us, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_l2p_apply_lane_queue_depth(&self, shard: usize, depth: usize) {
        fetch_max(&self.l2p_apply_lane_queue_depth_max, depth as u64);
        if let Some(slot) = self.l2p_apply_lane_shard_queue_depth_max.get(shard) {
            fetch_max(slot, depth as u64);
        }
    }

    /// Record one completed refcount apply lane task. `pending_set_wait`
    /// is the time the worker spent in `slot.take()` waiting for the
    /// commit thread to call `set()`; ~0 for ready (non-pending) tasks.
    /// `shard` indexes the per-shard breakdown.
    pub(crate) fn record_rc_apply_lane_task(
        &self,
        shard: usize,
        queue_wait: Duration,
        pending_set_wait: Duration,
        exec: Duration,
    ) {
        self.rc_apply_lane_tasks.fetch_add(1, Ordering::Relaxed);
        record_duration(
            &self.rc_apply_lane_queue_wait_us,
            &self.rc_apply_lane_queue_wait_max_us,
            queue_wait,
        );
        record_duration(
            &self.rc_apply_lane_pending_set_wait_us,
            &self.rc_apply_lane_pending_set_wait_max_us,
            pending_set_wait,
        );
        record_duration(
            &self.rc_apply_lane_exec_us,
            &self.rc_apply_lane_exec_max_us,
            exec,
        );
        if let Some(slot) = self.rc_apply_lane_shard_tasks.get(shard) {
            slot.fetch_add(1, Ordering::Relaxed);
            record_duration(
                &self.rc_apply_lane_shard_queue_wait_us[shard],
                &self.rc_apply_lane_shard_queue_wait_max_us[shard],
                queue_wait,
            );
            record_duration(
                &self.rc_apply_lane_shard_pending_set_wait_us[shard],
                &self.rc_apply_lane_shard_pending_set_wait_max_us[shard],
                pending_set_wait,
            );
            record_duration(
                &self.rc_apply_lane_shard_exec_us[shard],
                &self.rc_apply_lane_shard_exec_max_us[shard],
                exec,
            );
        }
    }

    pub(crate) fn record_rc_apply_lane_idle(&self, shard: usize, idle: Duration) {
        record_duration(
            &self.rc_apply_lane_idle_us,
            &self.rc_apply_lane_idle_max_us,
            idle,
        );
        if let Some(slot) = self.rc_apply_lane_shard_idle_us.get(shard) {
            let us = idle.as_micros().min(u128::from(u64::MAX)) as u64;
            slot.fetch_add(us, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_rc_apply_lane_queue_depth(&self, shard: usize, depth: usize) {
        fetch_max(&self.rc_apply_lane_queue_depth_max, depth as u64);
        if let Some(slot) = self.rc_apply_lane_shard_queue_depth_max.get(shard) {
            fetch_max(slot, depth as u64);
        }
    }

    pub(crate) fn record_rc_apply_lane_reserved_hold_start(&self) {
        let active = self
            .rc_apply_lane_reserved_hold_active
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        fetch_max(&self.rc_apply_lane_reserved_hold_active_max, active);
    }

    pub(crate) fn record_rc_apply_lane_reserved_hold_end(&self, elapsed: Duration) {
        record_duration(
            &self.rc_apply_lane_reserved_hold_us,
            &self.rc_apply_lane_reserved_hold_max_us,
            elapsed,
        );
        self.rc_apply_lane_reserved_hold_active
            .fetch_sub(1, Ordering::Relaxed);
    }

    /// Record one batch of cvar wakeups observed while the L2P apply lane
    /// was hunting for its next task. `wakeups` counts every `cvar.wait()`
    /// return (productive or spurious); `empty_wakeups` is the subset that
    /// resumed with both queues still empty.
    pub(crate) fn record_l2p_apply_lane_wakeups(
        &self,
        shard: usize,
        wakeups: u64,
        empty_wakeups: u64,
    ) {
        if wakeups == 0 && empty_wakeups == 0 {
            return;
        }
        self.l2p_apply_lane_wakeups
            .fetch_add(wakeups, Ordering::Relaxed);
        self.l2p_apply_lane_empty_wakeups
            .fetch_add(empty_wakeups, Ordering::Relaxed);
        if let Some(slot) = self.l2p_apply_lane_shard_wakeups.get(shard) {
            slot.fetch_add(wakeups, Ordering::Relaxed);
        }
        if let Some(slot) = self.l2p_apply_lane_shard_empty_wakeups.get(shard) {
            slot.fetch_add(empty_wakeups, Ordering::Relaxed);
        }
    }

    /// Record one completed burst (tasks processed since the previous
    /// idle wait) on the L2P apply lane. Together with `wakeups` this
    /// gives the avg/max number of tasks the lane drains per wakeup.
    pub(crate) fn record_l2p_apply_lane_burst(&self, shard: usize, burst: u64) {
        if burst == 0 {
            return;
        }
        self.l2p_apply_lane_burst_total
            .fetch_add(burst, Ordering::Relaxed);
        fetch_max(&self.l2p_apply_lane_burst_max, burst);
        if let Some(slot) = self.l2p_apply_lane_shard_burst_total.get(shard) {
            slot.fetch_add(burst, Ordering::Relaxed);
        }
        if let Some(slot) = self.l2p_apply_lane_shard_burst_max.get(shard) {
            fetch_max(slot, burst);
        }
    }

    pub(crate) fn record_rc_apply_lane_wakeups(
        &self,
        shard: usize,
        wakeups: u64,
        empty_wakeups: u64,
    ) {
        if wakeups == 0 && empty_wakeups == 0 {
            return;
        }
        self.rc_apply_lane_wakeups
            .fetch_add(wakeups, Ordering::Relaxed);
        self.rc_apply_lane_empty_wakeups
            .fetch_add(empty_wakeups, Ordering::Relaxed);
        if let Some(slot) = self.rc_apply_lane_shard_wakeups.get(shard) {
            slot.fetch_add(wakeups, Ordering::Relaxed);
        }
        if let Some(slot) = self.rc_apply_lane_shard_empty_wakeups.get(shard) {
            slot.fetch_add(empty_wakeups, Ordering::Relaxed);
        }
    }

    pub(crate) fn record_rc_apply_lane_burst(&self, shard: usize, burst: u64) {
        if burst == 0 {
            return;
        }
        self.rc_apply_lane_burst_total
            .fetch_add(burst, Ordering::Relaxed);
        fetch_max(&self.rc_apply_lane_burst_max, burst);
        if let Some(slot) = self.rc_apply_lane_shard_burst_total.get(shard) {
            slot.fetch_add(burst, Ordering::Relaxed);
        }
        if let Some(slot) = self.rc_apply_lane_shard_burst_max.get(shard) {
            fetch_max(slot, burst);
        }
    }

    /// Dedup-lane wakeup/idle/burst counters. Aggregate-only (no per-shard
    /// breakdown) because the dedup task closure already records its own
    /// per-task timings via [`record_dedup_lane_task`].
    pub(crate) fn record_dedup_lane_idle(&self, idle: Duration) {
        record_duration(&self.dedup_lane_idle_us, &self.dedup_lane_idle_max_us, idle);
    }

    pub(crate) fn record_dedup_lane_wakeups(&self, wakeups: u64, empty_wakeups: u64) {
        if wakeups == 0 && empty_wakeups == 0 {
            return;
        }
        self.dedup_lane_wakeups
            .fetch_add(wakeups, Ordering::Relaxed);
        self.dedup_lane_empty_wakeups
            .fetch_add(empty_wakeups, Ordering::Relaxed);
    }

    pub(crate) fn record_dedup_lane_burst(&self, burst: u64) {
        if burst == 0 {
            return;
        }
        self.dedup_lane_burst_total
            .fetch_add(burst, Ordering::Relaxed);
        fetch_max(&self.dedup_lane_burst_max, burst);
    }

    pub(crate) fn record_dedup_lane_queue_depth(&self, depth: usize) {
        fetch_max(&self.dedup_lane_queue_depth_max, depth as u64);
    }

    pub(crate) fn record_dedup_forward_put(&self, elapsed: Duration) {
        self.dedup_apply_forward_put_count
            .fetch_add(1, Ordering::Relaxed);
        record_duration(
            &self.dedup_apply_forward_put_us,
            &self.dedup_apply_forward_put_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_dedup_forward_put_batch(&self, ops: u64, elapsed: Duration) {
        if ops == 0 {
            return;
        }
        self.dedup_apply_forward_put_count
            .fetch_add(ops, Ordering::Relaxed);
        record_duration(
            &self.dedup_apply_forward_put_us,
            &self.dedup_apply_forward_put_max_us,
            elapsed,
        );
    }

    /// Dedup promotes dropped because the cuckoo table was saturated
    /// (`MAX_CUCKOO_CHAIN` exceeded). See the field doc in `metrics/mod.rs`.
    pub(crate) fn record_dedup_promote_dropped_saturated(&self, n: u64) {
        if n == 0 {
            return;
        }
        self.dedup_promote_dropped_saturated
            .fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn record_dedup_put_stages(&self, timings: DedupPutStageTimings) {
        record_duration(
            &self.dedup_put_l0_insert_us,
            &self.dedup_put_l0_insert_max_us,
            timings.l0_insert,
        );
        record_duration(
            &self.dedup_put_l1_put_us,
            &self.dedup_put_l1_put_max_us,
            timings.l1_put,
        );
        record_duration(
            &self.dedup_put_cuckoo_update_existing_us,
            &self.dedup_put_cuckoo_update_existing_max_us,
            timings.cuckoo_update_existing,
        );
        record_duration(
            &self.dedup_put_cuckoo_free_slots_us,
            &self.dedup_put_cuckoo_free_slots_max_us,
            timings.cuckoo_free_slots,
        );
        record_duration(
            &self.dedup_put_cuckoo_try_insert_empty_us,
            &self.dedup_put_cuckoo_try_insert_empty_max_us,
            timings.cuckoo_try_insert_empty,
        );
        record_duration(
            &self.dedup_put_cuckoo_evict_and_insert_us,
            &self.dedup_put_cuckoo_evict_and_insert_max_us,
            timings.cuckoo_evict_and_insert,
        );
        record_duration(
            &self.dedup_put_cuckoo_page_read_cache_wait_us,
            &self.dedup_put_cuckoo_page_read_cache_wait_max_us,
            timings.cuckoo_page_read_cache_wait,
        );
        record_duration(
            &self.dedup_put_cuckoo_page_alloc_us,
            &self.dedup_put_cuckoo_page_alloc_max_us,
            timings.cuckoo_page_alloc,
        );
        record_duration(
            &self.dedup_put_cuckoo_page_write_publish_us,
            &self.dedup_put_cuckoo_page_write_publish_max_us,
            timings.cuckoo_page_write_publish,
        );
        record_duration(
            &self.dedup_put_cuckoo_bucket_lock_wait_us,
            &self.dedup_put_cuckoo_bucket_lock_wait_max_us,
            timings.cuckoo_bucket_lock_wait,
        );
    }

    pub(crate) fn record_dedup_guard(&self, elapsed: Duration) {
        self.dedup_apply_guard_count.fetch_add(1, Ordering::Relaxed);
        record_duration(
            &self.dedup_apply_guard_us,
            &self.dedup_apply_guard_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_dedup_forward_delete(&self, elapsed: Duration) {
        self.dedup_apply_forward_delete_count
            .fetch_add(1, Ordering::Relaxed);
        record_duration(
            &self.dedup_apply_forward_delete_us,
            &self.dedup_apply_forward_delete_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_dedup_reverse_put_batch(&self, ops: u64, elapsed: Duration) {
        if ops == 0 {
            return;
        }
        self.dedup_apply_reverse_put_count
            .fetch_add(ops, Ordering::Relaxed);
        record_batch_duration(
            &self.dedup_apply_reverse_put_us,
            &self.dedup_apply_reverse_put_max_us,
            ops,
            elapsed,
        );
    }

    pub(crate) fn record_dedup_reverse_delete(&self, elapsed: Duration) {
        self.dedup_apply_reverse_delete_count
            .fetch_add(1, Ordering::Relaxed);
        record_duration(
            &self.dedup_apply_reverse_delete_us,
            &self.dedup_apply_reverse_delete_max_us,
            elapsed,
        );
    }
}
