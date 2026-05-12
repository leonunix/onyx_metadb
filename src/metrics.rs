use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::config::MAX_APPLY_LANE_SHARDS;

/// Newtype around `[AtomicU64; MAX_APPLY_LANE_SHARDS]` so the H2
/// per-shard counter arrays can opt into a hand-written `Default` —
/// the std-library `Default` impl on raw arrays only covers lengths
/// up to 32, and arrays of `AtomicU64` aren't `Copy` so we can't use
/// `[AtomicU64::new(0); N]` either.
#[derive(Debug)]
struct PerShardCounters([AtomicU64; MAX_APPLY_LANE_SHARDS]);

impl Default for PerShardCounters {
    fn default() -> Self {
        Self(std::array::from_fn(|_| AtomicU64::new(0)))
    }
}

impl std::ops::Deref for PerShardCounters {
    type Target = [AtomicU64; MAX_APPLY_LANE_SHARDS];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Default)]
pub(crate) struct DedupPutStageTimings {
    pub l0_insert: Duration,
    pub l1_put: Duration,
    pub cuckoo_update_existing: Duration,
    pub cuckoo_free_slots: Duration,
    pub cuckoo_try_insert_empty: Duration,
    pub cuckoo_evict_and_insert: Duration,
    pub cuckoo_page_read_cache_wait: Duration,
    pub cuckoo_page_alloc: Duration,
    pub cuckoo_page_write_publish: Duration,
    pub cuckoo_bucket_lock_wait: Duration,
}

#[derive(Debug, Default)]
pub struct MetaMetrics {
    commit_attempts: AtomicU64,
    commit_success: AtomicU64,
    commit_errors: AtomicU64,
    commit_empty: AtomicU64,
    commit_ops: AtomicU64,
    commit_ops_per_tx_max: AtomicU64,
    commit_wal_body_bytes: AtomicU64,
    commit_wal_body_bytes_max: AtomicU64,
    commit_total_us: AtomicU64,
    commit_total_max_us: AtomicU64,
    commit_wal_submit_us: AtomicU64,
    commit_wal_submit_max_us: AtomicU64,
    commit_drop_gate_wait_us: AtomicU64,
    commit_drop_gate_wait_max_us: AtomicU64,
    commit_apply_wait_us: AtomicU64,
    commit_apply_wait_max_us: AtomicU64,
    commit_apply_gate_wait_us: AtomicU64,
    commit_apply_gate_wait_max_us: AtomicU64,
    commit_apply_us: AtomicU64,
    commit_apply_max_us: AtomicU64,

    // Per-phase split of `commit_apply_us`. `apply_ops_laned` runs
    // L2P → refcount → dedup as three sequential lane-barrier
    // phases; each blocks commit ack. Use these counters to
    // attribute `commit_apply_max_us` to the specific lane class
    // that dominates the long tail.
    commit_apply_l2p_wait_us: AtomicU64,
    commit_apply_l2p_wait_max_us: AtomicU64,
    commit_apply_rc_enqueue_us: AtomicU64,
    commit_apply_rc_enqueue_max_us: AtomicU64,
    commit_apply_rc_wait_us: AtomicU64,
    commit_apply_rc_wait_max_us: AtomicU64,
    commit_apply_dedup_enqueue_us: AtomicU64,
    commit_apply_dedup_enqueue_max_us: AtomicU64,
    commit_apply_dedup_wait_us: AtomicU64,
    commit_apply_dedup_wait_max_us: AtomicU64,
    // Time spent inside `finish_global_apply` waiting for the previous
    // LSN to bump `last_applied_lsn`. Lives outside `commit_apply_us`
    // because it follows the per-lane apply work; previously only
    // surfaced in the >1s slow-commit warning. Captures the LSN-ordered
    // ack chain that runs in parallel with apply across shards but
    // serialises the final bump and notify.
    commit_finish_global_wait_us: AtomicU64,
    commit_finish_global_wait_max_us: AtomicU64,

    wal_submit_calls: AtomicU64,
    wal_submit_wait_us: AtomicU64,
    wal_submit_wait_max_us: AtomicU64,
    wal_batches: AtomicU64,
    wal_records: AtomicU64,
    wal_bytes: AtomicU64,
    wal_rotates: AtomicU64,
    wal_write_us: AtomicU64,
    wal_write_max_us: AtomicU64,
    wal_fsyncs: AtomicU64,
    wal_fsync_us: AtomicU64,
    wal_fsync_max_us: AtomicU64,
    wal_batch_records_max: AtomicU64,
    wal_batch_bytes_max: AtomicU64,

    range_delete_calls: AtomicU64,
    range_delete_success: AtomicU64,
    range_delete_errors: AtomicU64,
    range_delete_noop: AtomicU64,
    range_delete_captured_entries: AtomicU64,
    range_delete_chunks: AtomicU64,
    range_delete_total_us: AtomicU64,
    range_delete_total_max_us: AtomicU64,
    range_delete_scan_us: AtomicU64,
    range_delete_scan_max_us: AtomicU64,
    range_delete_wal_us: AtomicU64,
    range_delete_wal_max_us: AtomicU64,
    range_delete_apply_wait_us: AtomicU64,
    range_delete_apply_wait_max_us: AtomicU64,
    range_delete_apply_us: AtomicU64,
    range_delete_apply_max_us: AtomicU64,
    range_delete_drop_gate_wait_us: AtomicU64,
    range_delete_drop_gate_wait_max_us: AtomicU64,
    range_delete_apply_gate_wait_us: AtomicU64,
    range_delete_apply_gate_wait_max_us: AtomicU64,

    cleanup_calls: AtomicU64,
    cleanup_success: AtomicU64,
    cleanup_errors: AtomicU64,
    cleanup_noop: AtomicU64,
    cleanup_pbas: AtomicU64,
    cleanup_hashes_found: AtomicU64,
    cleanup_forward_checks: AtomicU64,
    cleanup_tombstones_emitted: AtomicU64,
    cleanup_tx_ops: AtomicU64,
    cleanup_total_us: AtomicU64,
    cleanup_total_max_us: AtomicU64,
    cleanup_scan_us: AtomicU64,
    cleanup_scan_max_us: AtomicU64,
    cleanup_forward_check_us: AtomicU64,
    cleanup_forward_check_max_us: AtomicU64,
    cleanup_commit_us: AtomicU64,
    cleanup_commit_max_us: AtomicU64,

    // Per-WalOp-variant apply timing. `commit_apply_us` is the sum of
    // these (plus the bucket bookkeeping in `apply_ops_grouped`); the
    // per-variant breakdown lets callers see whether L2P, refcount, or
    // dedup is dominating apply growth as state size grows.
    apply_l2p_put_count: AtomicU64,
    apply_l2p_put_us: AtomicU64,
    apply_l2p_put_max_us: AtomicU64,
    apply_l2p_delete_count: AtomicU64,
    apply_l2p_delete_us: AtomicU64,
    apply_l2p_delete_max_us: AtomicU64,
    apply_l2p_remap_count: AtomicU64,
    apply_l2p_remap_us: AtomicU64,
    apply_l2p_remap_max_us: AtomicU64,
    apply_l2p_range_delete_count: AtomicU64,
    apply_l2p_range_delete_us: AtomicU64,
    apply_l2p_range_delete_max_us: AtomicU64,
    apply_l2p_bucket_count: AtomicU64,
    apply_l2p_bucket_ops: AtomicU64,
    apply_l2p_bucket_total_us: AtomicU64,
    apply_l2p_bucket_total_max_us: AtomicU64,
    apply_l2p_bucket_tree_lock_wait_us: AtomicU64,
    apply_l2p_bucket_tree_lock_wait_max_us: AtomicU64,
    apply_l2p_bucket_read_view_prepare_us: AtomicU64,
    apply_l2p_bucket_read_view_prepare_max_us: AtomicU64,
    apply_l2p_bucket_ops_us: AtomicU64,
    apply_l2p_bucket_ops_max_us: AtomicU64,
    apply_l2p_bucket_finish_us: AtomicU64,
    apply_l2p_bucket_finish_max_us: AtomicU64,
    apply_l2p_bucket_publish_us: AtomicU64,
    apply_l2p_bucket_publish_max_us: AtomicU64,
    apply_refcount_count: AtomicU64,
    apply_refcount_us: AtomicU64,
    apply_refcount_max_us: AtomicU64,
    apply_dedup_count: AtomicU64,
    apply_dedup_us: AtomicU64,
    apply_dedup_max_us: AtomicU64,
    dedup_lane_tasks: AtomicU64,
    dedup_lane_ops: AtomicU64,
    dedup_lane_ready_queue_wait_us: AtomicU64,
    dedup_lane_ready_queue_wait_max_us: AtomicU64,
    dedup_lane_exec_us: AtomicU64,
    dedup_lane_exec_max_us: AtomicU64,
    // Per-task instrumentation for the L2P / refcount apply lanes.
    // Aggregated across every shard's lane of the kind. See
    // [`apply_lane_worker`] in `db.rs` for capture sites.
    //   - `tasks` counts each apply task that finishes work().
    //   - `queue_depth_max` is `state.queue.len() + state.maintenance.len()`
    //     observed at enqueue time — peak backlog at any one lane.
    //   - `queue_wait_us` is the gap between `enqueue_task()` push and
    //     the worker popping the task (lane scheduling latency).
    //   - `exec_us` is the wall time spent inside the work() closure.
    //   - `idle_us` is time the worker spent blocked in `cvar.wait`
    //     waiting for a task to arrive.
    //   - `pending_set_wait_us` is RC-specific: time between worker
    //     popping a pending task and the commit thread calling `set()`
    //     to fill in the deferred refcount delta. ~0 for L2P
    //     (enqueue_ready fills the slot up front).
    l2p_apply_lane_tasks: AtomicU64,
    l2p_apply_lane_queue_depth_max: AtomicU64,
    l2p_apply_lane_queue_wait_us: AtomicU64,
    l2p_apply_lane_queue_wait_max_us: AtomicU64,
    l2p_apply_lane_exec_us: AtomicU64,
    l2p_apply_lane_exec_max_us: AtomicU64,
    l2p_apply_lane_idle_us: AtomicU64,
    l2p_apply_lane_idle_max_us: AtomicU64,
    // H2 wakeup-shape counters (worker-internal view):
    //   - `wakeups` = number of times the lane worker exited `cvar.wait()`
    //     looking for a task. Total tasks / wakeups gives the average burst
    //     size between idle periods.
    //   - `empty_wakeups` = subset of `wakeups` where the worker resumed
    //     but both the ready queue and maintenance queue were still empty
    //     (spurious wake or a wake racing with another consumer). Non-zero
    //     means lock-contention / spurious-wake noise.
    //   - `burst_total` / `burst_max` = sum and tail of "tasks processed
    //     between two adjacent `cvar.wait()` calls", emitted right before
    //     the next wait or at shutdown. Distinguishes "lane stays hot,
    //     pops many tasks per wakeup" from "lane wakes per task".
    l2p_apply_lane_wakeups: AtomicU64,
    l2p_apply_lane_empty_wakeups: AtomicU64,
    l2p_apply_lane_burst_total: AtomicU64,
    l2p_apply_lane_burst_max: AtomicU64,
    rc_apply_lane_tasks: AtomicU64,
    rc_apply_lane_queue_depth_max: AtomicU64,
    rc_apply_lane_queue_wait_us: AtomicU64,
    rc_apply_lane_queue_wait_max_us: AtomicU64,
    rc_apply_lane_exec_us: AtomicU64,
    rc_apply_lane_exec_max_us: AtomicU64,
    rc_apply_lane_idle_us: AtomicU64,
    rc_apply_lane_idle_max_us: AtomicU64,
    rc_apply_lane_pending_set_wait_us: AtomicU64,
    rc_apply_lane_pending_set_wait_max_us: AtomicU64,
    rc_apply_lane_wakeups: AtomicU64,
    rc_apply_lane_empty_wakeups: AtomicU64,
    rc_apply_lane_burst_total: AtomicU64,
    rc_apply_lane_burst_max: AtomicU64,
    // Dedup apply lanes share `apply_lane_worker` but record their own
    // task-level metrics from inside the closure (`record_dedup_lane_task`).
    // The wakeup-shape and idle counters live here so all three lane kinds
    // have the same H2 view of "how often did the worker have to wait?".
    dedup_lane_idle_us: AtomicU64,
    dedup_lane_idle_max_us: AtomicU64,
    dedup_lane_wakeups: AtomicU64,
    dedup_lane_empty_wakeups: AtomicU64,
    dedup_lane_burst_total: AtomicU64,
    dedup_lane_burst_max: AtomicU64,
    dedup_lane_queue_depth_max: AtomicU64,
    // Per-shard breakdown of the H2 apply-lane counters, indexed by lane
    // ordinal. Populated alongside the aggregates above; ordinals at or
    // beyond [`MAX_APPLY_LANE_SHARDS`] only show up in the aggregates.
    // Used to spot a single hot shard hiding inside a healthy-looking
    // average (e.g. one PBA-bucket monopolizing rc apply while the
    // other 15 lanes idle).
    l2p_apply_lane_shard_tasks: PerShardCounters,
    l2p_apply_lane_shard_queue_depth_max: PerShardCounters,
    l2p_apply_lane_shard_queue_wait_us: PerShardCounters,
    l2p_apply_lane_shard_queue_wait_max_us: PerShardCounters,
    l2p_apply_lane_shard_exec_us: PerShardCounters,
    l2p_apply_lane_shard_exec_max_us: PerShardCounters,
    l2p_apply_lane_shard_idle_us: PerShardCounters,
    l2p_apply_lane_shard_wakeups: PerShardCounters,
    l2p_apply_lane_shard_empty_wakeups: PerShardCounters,
    l2p_apply_lane_shard_burst_total: PerShardCounters,
    l2p_apply_lane_shard_burst_max: PerShardCounters,
    rc_apply_lane_shard_tasks: PerShardCounters,
    rc_apply_lane_shard_queue_depth_max: PerShardCounters,
    rc_apply_lane_shard_queue_wait_us: PerShardCounters,
    rc_apply_lane_shard_queue_wait_max_us: PerShardCounters,
    rc_apply_lane_shard_exec_us: PerShardCounters,
    rc_apply_lane_shard_exec_max_us: PerShardCounters,
    rc_apply_lane_shard_idle_us: PerShardCounters,
    rc_apply_lane_shard_pending_set_wait_us: PerShardCounters,
    rc_apply_lane_shard_pending_set_wait_max_us: PerShardCounters,
    rc_apply_lane_shard_wakeups: PerShardCounters,
    rc_apply_lane_shard_empty_wakeups: PerShardCounters,
    rc_apply_lane_shard_burst_total: PerShardCounters,
    rc_apply_lane_shard_burst_max: PerShardCounters,
    dedup_apply_guard_count: AtomicU64,
    dedup_apply_guard_us: AtomicU64,
    dedup_apply_guard_max_us: AtomicU64,
    dedup_apply_forward_put_count: AtomicU64,
    dedup_apply_forward_put_us: AtomicU64,
    dedup_apply_forward_put_max_us: AtomicU64,
    dedup_apply_forward_delete_count: AtomicU64,
    dedup_apply_forward_delete_us: AtomicU64,
    dedup_apply_forward_delete_max_us: AtomicU64,
    dedup_apply_reverse_put_count: AtomicU64,
    dedup_apply_reverse_put_us: AtomicU64,
    dedup_apply_reverse_put_max_us: AtomicU64,
    dedup_apply_reverse_delete_count: AtomicU64,
    dedup_apply_reverse_delete_us: AtomicU64,
    dedup_apply_reverse_delete_max_us: AtomicU64,
    dedup_put_l0_insert_us: AtomicU64,
    dedup_put_l0_insert_max_us: AtomicU64,
    dedup_put_l1_put_us: AtomicU64,
    dedup_put_l1_put_max_us: AtomicU64,
    dedup_put_cuckoo_update_existing_us: AtomicU64,
    dedup_put_cuckoo_update_existing_max_us: AtomicU64,
    dedup_put_cuckoo_free_slots_us: AtomicU64,
    dedup_put_cuckoo_free_slots_max_us: AtomicU64,
    dedup_put_cuckoo_try_insert_empty_us: AtomicU64,
    dedup_put_cuckoo_try_insert_empty_max_us: AtomicU64,
    dedup_put_cuckoo_evict_and_insert_us: AtomicU64,
    dedup_put_cuckoo_evict_and_insert_max_us: AtomicU64,
    dedup_put_cuckoo_page_read_cache_wait_us: AtomicU64,
    dedup_put_cuckoo_page_read_cache_wait_max_us: AtomicU64,
    dedup_put_cuckoo_page_alloc_us: AtomicU64,
    dedup_put_cuckoo_page_alloc_max_us: AtomicU64,
    dedup_put_cuckoo_page_write_publish_us: AtomicU64,
    dedup_put_cuckoo_page_write_publish_max_us: AtomicU64,
    dedup_put_cuckoo_bucket_lock_wait_us: AtomicU64,
    dedup_put_cuckoo_bucket_lock_wait_max_us: AtomicU64,

    // L2P read-path split. `l2p_get_lock_wait_us` is time spent blocked
    // acquiring the shard tree read lock (i.e. an apply or another writer
    // holds it); `l2p_get_tree_walk_us` is time spent inside the tree
    // traversal itself once the lock is held. Used to prove or rule out
    // apply-vs-read lock contention.
    l2p_get_calls: AtomicU64,
    l2p_get_lock_wait_us: AtomicU64,
    l2p_get_lock_wait_max_us: AtomicU64,
    l2p_get_tree_walk_us: AtomicU64,
    l2p_get_tree_walk_max_us: AtomicU64,
    l2p_multi_get_calls: AtomicU64,
    l2p_multi_get_lbas: AtomicU64,
    l2p_multi_get_pin_us: AtomicU64,
    l2p_multi_get_pin_max_us: AtomicU64,
    l2p_multi_get_volume_us: AtomicU64,
    l2p_multi_get_volume_max_us: AtomicU64,
    l2p_multi_get_sort_us: AtomicU64,
    l2p_multi_get_sort_max_us: AtomicU64,
    l2p_multi_get_view_us: AtomicU64,
    l2p_multi_get_view_max_us: AtomicU64,
    l2p_multi_get_tree_us: AtomicU64,
    l2p_multi_get_tree_max_us: AtomicU64,

    // Per-phase flush timing. Splits `Db::flush()` into the gate /
    // sample / IO / manifest / install phases so we can tell which one
    // is the long pole when buffer fills up faster than checkpoint
    // drains. `flush_calls` counts completed (success or error) flush
    // invocations.
    //
    // Aggregate counters (kind-agnostic) preserve backwards compat.
    // The `_steady` / `_forced` siblings split the same numbers by
    // [`FlushKind`] so dashboards can separate the running-period
    // checkpoint cadence from forced / shutdown-drain flushes.
    flush_calls: AtomicU64,
    flush_calls_steady: AtomicU64,
    flush_calls_forced: AtomicU64,
    flush_total_us: AtomicU64,
    flush_total_max_us: AtomicU64,
    flush_total_us_steady: AtomicU64,
    flush_total_max_us_steady: AtomicU64,
    flush_total_us_forced: AtomicU64,
    flush_total_max_us_forced: AtomicU64,
    flush_gate_wait_us: AtomicU64,
    flush_gate_wait_max_us: AtomicU64,
    flush_sample_us: AtomicU64,
    flush_sample_max_us: AtomicU64,
    flush_sample_us_steady: AtomicU64,
    flush_sample_max_us_steady: AtomicU64,
    flush_sample_us_forced: AtomicU64,
    flush_sample_max_us_forced: AtomicU64,
    flush_io_us: AtomicU64,
    flush_io_max_us: AtomicU64,
    flush_io_seal_us: AtomicU64,
    flush_io_seal_max_us: AtomicU64,
    flush_io_page_write_us: AtomicU64,
    flush_io_page_write_max_us: AtomicU64,
    flush_io_rc_meta_us: AtomicU64,
    flush_io_rc_meta_max_us: AtomicU64,
    flush_io_sync_us: AtomicU64,
    flush_io_sync_max_us: AtomicU64,
    flush_manifest_us: AtomicU64,
    flush_manifest_max_us: AtomicU64,
    flush_install_us: AtomicU64,
    flush_install_max_us: AtomicU64,
    flush_reclaim_us: AtomicU64,
    flush_reclaim_max_us: AtomicU64,
    flush_pages_written: AtomicU64,
    flush_reclaim_budget_pages: AtomicU64,
    flush_reclaim_selected_pages: AtomicU64,
    flush_reclaim_reclaimed_pages: AtomicU64,
    flush_reclaim_blocked_pages: AtomicU64,
    // Sample-phase workload size. Together with `flush_calls`, these
    // let dashboards compute per-flush averages and watch trajectories
    // of the dirty / drained / freshly-allocated counts that drive the
    // sample-phase hold time. Independent of [`FlushKind`].
    flush_sample_l2p_dirty_pages: AtomicU64,
    flush_sample_l2p_dirty_pages_max: AtomicU64,
    flush_sample_rc_drained_deltas: AtomicU64,
    flush_sample_rc_drained_deltas_max: AtomicU64,
    flush_sample_rc_fresh_pages: AtomicU64,
    flush_sample_rc_fresh_pages_max: AtomicU64,
    // Refcount drainer (priority 3). Background per-shard threads that
    // absorb `RcShard.delta` into a sealed-page staging overlay outside
    // `apply_gate.write()`. All zero when
    // `Config::refcount_drainer_enabled = false`.
    rc_drainer_cycles: AtomicU64,
    rc_drainer_drained_entries: AtomicU64,
    rc_drainer_pages_built: AtomicU64,
    rc_drainer_cycle_us: AtomicU64,
    rc_drainer_cycle_max_us: AtomicU64,
    /// Peak overlay size observed at end of any drainer cycle.
    rc_drainer_overlay_size_max_pages: AtomicU64,
    /// Time `begin_checkpoint` spent waiting for the in-flight drainer
    /// cycle to complete after preempt was set.
    rc_drainer_checkpoint_wait_us: AtomicU64,
    rc_drainer_checkpoint_wait_max_us: AtomicU64,
    /// Count of `begin_checkpoint` invocations that fell back to the
    /// priority-1 in-gate drain path because overlay/delta exceeded
    /// `Config::refcount_drainer_backpressure_pages`.
    rc_drainer_backpressure_fallbacks: AtomicU64,
    /// Count of `PageStore::allocate_run` calls made by drainers to
    /// refill their per-shard `PagePool`.
    rc_drainer_pool_refills: AtomicU64,

    // H4: bandwidth counter paired with `flush_pages_written` so a
    // window snapshot can compute writeback MB/s without assuming
    // page size. Bumped from `record_flush_io`.
    flush_io_bytes_total: AtomicU64,

    // H5: page_store io_uring batch counters. `meta_io_write_*`
    // covers every io_uring write submit (single page, run, sealed
    // writev, parallel runs); `meta_io_read_*` covers io_uring batch
    // reads. `*_batch_ops_max` / `*_batch_bytes_max` are the largest
    // batch the writer/reader has ever submitted in one
    // `submit_and_wait` round, useful for distinguishing "shallow IO"
    // (small batch) from "slow IO" (big batch but high `_us`).
    meta_io_write_calls: AtomicU64,
    meta_io_write_ops: AtomicU64,
    meta_io_write_bytes: AtomicU64,
    meta_io_write_us: AtomicU64,
    meta_io_write_max_us: AtomicU64,
    meta_io_write_batch_ops_max: AtomicU64,
    meta_io_write_batch_bytes_max: AtomicU64,

    meta_io_read_calls: AtomicU64,
    meta_io_read_ops: AtomicU64,
    meta_io_read_bytes: AtomicU64,
    meta_io_read_us: AtomicU64,
    meta_io_read_max_us: AtomicU64,
    meta_io_read_batch_ops_max: AtomicU64,

    meta_io_fsync_calls: AtomicU64,
    meta_io_fsync_us: AtomicU64,
    meta_io_fsync_max_us: AtomicU64,

    // H5 lock contention probe. The page_store currently funnels every
    // io_uring write through a single global `write_uring: Mutex<...>`;
    // when this is the serialization point, lock_wait_max_us approaches
    // meta_io_write_max_us and lock_wait_us / meta_io_write_us is high.
    // Once the mutex is split per-role these counters drop, which is the
    // before/after signal we want.
    meta_io_write_uring_lock_acquires: AtomicU64,
    meta_io_write_uring_lock_wait_us: AtomicU64,
    meta_io_write_uring_lock_wait_max_us: AtomicU64,

    // Centralised IoSubmitter telemetry (Phase 1 observability).
    //
    // `io_submitter_iterations` is incremented once per submitter loop
    // iteration that actually called `submit_and_wait(1)` (i.e. once
    // per ring transition). `io_submitter_sqes_submitted` totals every
    // SQE pushed by that iteration (all the new ops drained from the
    // channel before the wait). Their ratio gives the average batch.
    //
    // `_max` counters track per-iteration peaks:
    //  - `channel_pending_max` = `rx.len()` observed at the start of
    //    the loop, *before* draining into the SQ. High values say
    //    producers are queueing faster than the submitter drains.
    //  - `submit_batch_size_max` = SQEs pushed *this iteration*. If
    //    this stays small (≪ SQ_ENTRIES) under pressure, batching is
    //    not happening — either producers are too rate-limited, or
    //    the submitter is harvesting CQEs too quickly.
    //  - `inflight_max` = ops with an outstanding CQE at peak. Tracks
    //    real device-level write/fsync queue depth.
    io_submitter_iterations: AtomicU64,
    io_submitter_sqes_submitted: AtomicU64,
    io_submitter_channel_pending_max: AtomicU64,
    io_submitter_submit_batch_size_max: AtomicU64,
    io_submitter_inflight_max: AtomicU64,

    // L2P streaming writeback (`Config::l2p_writeback_enabled`).
    // Background worker that continuously seals dirty L2P pages and
    // writes them through `IoSubmitter`, *outside* `apply_gate.write()`,
    // so the next `Db::flush` samples a small dirty set and its gate
    // hold time stays small under high write load.
    //
    // `pages_promoted` = pages whose `Arc::ptr_eq` matched on install
    //   (no commit re-mutated them during the IO window). These are
    //   moved out of the per-shard dirty set; subsequent reads fault
    //   from disk / shared `PageCache`.
    // `pages_kept_dirty` = pages whose Arc was re-mutated between
    //   snapshot and install. Their bytes are still on disk (stale
    //   version), but the live tree continues mutating; next cycle
    //   catches them again. Equivalent to a wasted write.
    // `io_bytes_total` / `seal_us` / `install_us` are the usual time/
    // bandwidth break-downs. `cycles_with_work` increments once per
    // round that actually flushed at least one shard.
    l2p_writeback_cycles: AtomicU64,
    l2p_writeback_cycles_with_work: AtomicU64,
    l2p_writeback_shards_flushed: AtomicU64,
    l2p_writeback_pages_promoted: AtomicU64,
    l2p_writeback_pages_kept_dirty: AtomicU64,
    l2p_writeback_pages_written: AtomicU64,
    l2p_writeback_io_bytes_total: AtomicU64,
    l2p_writeback_seal_us: AtomicU64,
    l2p_writeback_seal_max_us: AtomicU64,
    l2p_writeback_io_us: AtomicU64,
    l2p_writeback_io_max_us: AtomicU64,
    l2p_writeback_install_us: AtomicU64,
    l2p_writeback_install_max_us: AtomicU64,
    l2p_writeback_errors: AtomicU64,
}

/// Why this `Db::flush()` invocation is happening. Tags the metrics so
/// dashboards can separate the steady-state checkpoint cadence (driven
/// by the periodic `try_flush()` background ticker) from forced flushes
/// (`Db::flush()` — explicit `force_checkpoint`, snapshot operations,
/// shutdown drain, etc.). Cheap copy, used purely for metric routing.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum FlushKind {
    /// Best-effort background checkpoint via `try_flush()`. Skips when
    /// commits are actively applying.
    Steady,
    /// Blocking `flush()` — caller wants the checkpoint to land before
    /// returning. Includes shutdown drain, explicit force_checkpoint,
    /// and snapshot/drop_volume internal flushes.
    Forced,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetaMetricsSnapshot {
    pub commit_attempts: u64,
    pub commit_success: u64,
    pub commit_errors: u64,
    pub commit_empty: u64,
    pub commit_ops: u64,
    pub commit_ops_per_tx_max: u64,
    pub commit_wal_body_bytes: u64,
    pub commit_wal_body_bytes_max: u64,
    pub commit_total_us: u64,
    pub commit_total_max_us: u64,
    pub commit_wal_submit_us: u64,
    pub commit_wal_submit_max_us: u64,
    pub commit_drop_gate_wait_us: u64,
    pub commit_drop_gate_wait_max_us: u64,
    pub commit_apply_wait_us: u64,
    pub commit_apply_wait_max_us: u64,
    pub commit_apply_gate_wait_us: u64,
    pub commit_apply_gate_wait_max_us: u64,
    pub commit_finish_global_wait_us: u64,
    pub commit_finish_global_wait_max_us: u64,
    pub commit_apply_us: u64,
    pub commit_apply_max_us: u64,
    pub commit_apply_l2p_wait_us: u64,
    pub commit_apply_l2p_wait_max_us: u64,
    pub commit_apply_rc_enqueue_us: u64,
    pub commit_apply_rc_enqueue_max_us: u64,
    pub commit_apply_rc_wait_us: u64,
    pub commit_apply_rc_wait_max_us: u64,
    pub commit_apply_dedup_enqueue_us: u64,
    pub commit_apply_dedup_enqueue_max_us: u64,
    pub commit_apply_dedup_wait_us: u64,
    pub commit_apply_dedup_wait_max_us: u64,
    pub wal_submit_calls: u64,
    pub wal_submit_wait_us: u64,
    pub wal_submit_wait_max_us: u64,
    pub wal_batches: u64,
    pub wal_records: u64,
    pub wal_bytes: u64,
    pub wal_rotates: u64,
    pub wal_write_us: u64,
    pub wal_write_max_us: u64,
    pub wal_fsyncs: u64,
    pub wal_fsync_us: u64,
    pub wal_fsync_max_us: u64,
    pub wal_batch_records_max: u64,
    pub wal_batch_bytes_max: u64,
    pub range_delete_calls: u64,
    pub range_delete_success: u64,
    pub range_delete_errors: u64,
    pub range_delete_noop: u64,
    pub range_delete_captured_entries: u64,
    pub range_delete_chunks: u64,
    pub range_delete_total_us: u64,
    pub range_delete_total_max_us: u64,
    pub range_delete_scan_us: u64,
    pub range_delete_scan_max_us: u64,
    pub range_delete_wal_us: u64,
    pub range_delete_wal_max_us: u64,
    pub range_delete_apply_wait_us: u64,
    pub range_delete_apply_wait_max_us: u64,
    pub range_delete_apply_us: u64,
    pub range_delete_apply_max_us: u64,
    pub range_delete_drop_gate_wait_us: u64,
    pub range_delete_drop_gate_wait_max_us: u64,
    pub range_delete_apply_gate_wait_us: u64,
    pub range_delete_apply_gate_wait_max_us: u64,
    pub cleanup_calls: u64,
    pub cleanup_success: u64,
    pub cleanup_errors: u64,
    pub cleanup_noop: u64,
    pub cleanup_pbas: u64,
    pub cleanup_hashes_found: u64,
    pub cleanup_forward_checks: u64,
    pub cleanup_tombstones_emitted: u64,
    pub cleanup_tx_ops: u64,
    pub cleanup_total_us: u64,
    pub cleanup_total_max_us: u64,
    pub cleanup_scan_us: u64,
    pub cleanup_scan_max_us: u64,
    pub cleanup_forward_check_us: u64,
    pub cleanup_forward_check_max_us: u64,
    pub cleanup_commit_us: u64,
    pub cleanup_commit_max_us: u64,
    pub apply_l2p_put_count: u64,
    pub apply_l2p_put_us: u64,
    pub apply_l2p_put_max_us: u64,
    pub apply_l2p_delete_count: u64,
    pub apply_l2p_delete_us: u64,
    pub apply_l2p_delete_max_us: u64,
    pub apply_l2p_remap_count: u64,
    pub apply_l2p_remap_us: u64,
    pub apply_l2p_remap_max_us: u64,
    pub apply_l2p_range_delete_count: u64,
    pub apply_l2p_range_delete_us: u64,
    pub apply_l2p_range_delete_max_us: u64,
    pub apply_l2p_bucket_count: u64,
    pub apply_l2p_bucket_ops: u64,
    pub apply_l2p_bucket_total_us: u64,
    pub apply_l2p_bucket_total_max_us: u64,
    pub apply_l2p_bucket_tree_lock_wait_us: u64,
    pub apply_l2p_bucket_tree_lock_wait_max_us: u64,
    pub apply_l2p_bucket_read_view_prepare_us: u64,
    pub apply_l2p_bucket_read_view_prepare_max_us: u64,
    pub apply_l2p_bucket_ops_us: u64,
    pub apply_l2p_bucket_ops_max_us: u64,
    pub apply_l2p_bucket_finish_us: u64,
    pub apply_l2p_bucket_finish_max_us: u64,
    pub apply_l2p_bucket_publish_us: u64,
    pub apply_l2p_bucket_publish_max_us: u64,
    pub apply_refcount_count: u64,
    pub apply_refcount_us: u64,
    pub apply_refcount_max_us: u64,
    pub apply_dedup_count: u64,
    pub apply_dedup_us: u64,
    pub apply_dedup_max_us: u64,
    pub dedup_lane_tasks: u64,
    pub dedup_lane_ops: u64,
    pub dedup_lane_ready_queue_wait_us: u64,
    pub dedup_lane_ready_queue_wait_max_us: u64,
    pub dedup_lane_exec_us: u64,
    pub dedup_lane_exec_max_us: u64,
    pub dedup_lane_idle_us: u64,
    pub dedup_lane_idle_max_us: u64,
    pub dedup_lane_wakeups: u64,
    pub dedup_lane_empty_wakeups: u64,
    pub dedup_lane_burst_total: u64,
    pub dedup_lane_burst_max: u64,
    pub dedup_lane_queue_depth_max: u64,
    pub l2p_apply_lane_tasks: u64,
    pub l2p_apply_lane_queue_depth_max: u64,
    pub l2p_apply_lane_queue_wait_us: u64,
    pub l2p_apply_lane_queue_wait_max_us: u64,
    pub l2p_apply_lane_exec_us: u64,
    pub l2p_apply_lane_exec_max_us: u64,
    pub l2p_apply_lane_idle_us: u64,
    pub l2p_apply_lane_idle_max_us: u64,
    pub l2p_apply_lane_wakeups: u64,
    pub l2p_apply_lane_empty_wakeups: u64,
    pub l2p_apply_lane_burst_total: u64,
    pub l2p_apply_lane_burst_max: u64,
    pub rc_apply_lane_tasks: u64,
    pub rc_apply_lane_queue_depth_max: u64,
    pub rc_apply_lane_queue_wait_us: u64,
    pub rc_apply_lane_queue_wait_max_us: u64,
    pub rc_apply_lane_exec_us: u64,
    pub rc_apply_lane_exec_max_us: u64,
    pub rc_apply_lane_idle_us: u64,
    pub rc_apply_lane_idle_max_us: u64,
    pub rc_apply_lane_pending_set_wait_us: u64,
    pub rc_apply_lane_pending_set_wait_max_us: u64,
    pub rc_apply_lane_wakeups: u64,
    pub rc_apply_lane_empty_wakeups: u64,
    pub rc_apply_lane_burst_total: u64,
    pub rc_apply_lane_burst_max: u64,
    pub l2p_apply_lane_shard_tasks: Vec<u64>,
    pub l2p_apply_lane_shard_queue_depth_max: Vec<u64>,
    pub l2p_apply_lane_shard_queue_wait_us: Vec<u64>,
    pub l2p_apply_lane_shard_queue_wait_max_us: Vec<u64>,
    pub l2p_apply_lane_shard_exec_us: Vec<u64>,
    pub l2p_apply_lane_shard_exec_max_us: Vec<u64>,
    pub l2p_apply_lane_shard_idle_us: Vec<u64>,
    pub l2p_apply_lane_shard_wakeups: Vec<u64>,
    pub l2p_apply_lane_shard_empty_wakeups: Vec<u64>,
    pub l2p_apply_lane_shard_burst_total: Vec<u64>,
    pub l2p_apply_lane_shard_burst_max: Vec<u64>,
    pub rc_apply_lane_shard_tasks: Vec<u64>,
    pub rc_apply_lane_shard_queue_depth_max: Vec<u64>,
    pub rc_apply_lane_shard_queue_wait_us: Vec<u64>,
    pub rc_apply_lane_shard_queue_wait_max_us: Vec<u64>,
    pub rc_apply_lane_shard_exec_us: Vec<u64>,
    pub rc_apply_lane_shard_exec_max_us: Vec<u64>,
    pub rc_apply_lane_shard_idle_us: Vec<u64>,
    pub rc_apply_lane_shard_pending_set_wait_us: Vec<u64>,
    pub rc_apply_lane_shard_pending_set_wait_max_us: Vec<u64>,
    pub rc_apply_lane_shard_wakeups: Vec<u64>,
    pub rc_apply_lane_shard_empty_wakeups: Vec<u64>,
    pub rc_apply_lane_shard_burst_total: Vec<u64>,
    pub rc_apply_lane_shard_burst_max: Vec<u64>,
    pub dedup_apply_guard_count: u64,
    pub dedup_apply_guard_us: u64,
    pub dedup_apply_guard_max_us: u64,
    pub dedup_apply_forward_put_count: u64,
    pub dedup_apply_forward_put_us: u64,
    pub dedup_apply_forward_put_max_us: u64,
    pub dedup_apply_forward_delete_count: u64,
    pub dedup_apply_forward_delete_us: u64,
    pub dedup_apply_forward_delete_max_us: u64,
    pub dedup_apply_reverse_put_count: u64,
    pub dedup_apply_reverse_put_us: u64,
    pub dedup_apply_reverse_put_max_us: u64,
    pub dedup_apply_reverse_delete_count: u64,
    pub dedup_apply_reverse_delete_us: u64,
    pub dedup_apply_reverse_delete_max_us: u64,
    pub dedup_put_l0_insert_us: u64,
    pub dedup_put_l0_insert_max_us: u64,
    pub dedup_put_l1_put_us: u64,
    pub dedup_put_l1_put_max_us: u64,
    pub dedup_put_cuckoo_update_existing_us: u64,
    pub dedup_put_cuckoo_update_existing_max_us: u64,
    pub dedup_put_cuckoo_free_slots_us: u64,
    pub dedup_put_cuckoo_free_slots_max_us: u64,
    pub dedup_put_cuckoo_try_insert_empty_us: u64,
    pub dedup_put_cuckoo_try_insert_empty_max_us: u64,
    pub dedup_put_cuckoo_evict_and_insert_us: u64,
    pub dedup_put_cuckoo_evict_and_insert_max_us: u64,
    pub dedup_put_cuckoo_page_read_cache_wait_us: u64,
    pub dedup_put_cuckoo_page_read_cache_wait_max_us: u64,
    pub dedup_put_cuckoo_page_alloc_us: u64,
    pub dedup_put_cuckoo_page_alloc_max_us: u64,
    pub dedup_put_cuckoo_page_write_publish_us: u64,
    pub dedup_put_cuckoo_page_write_publish_max_us: u64,
    pub dedup_put_cuckoo_bucket_lock_wait_us: u64,
    pub dedup_put_cuckoo_bucket_lock_wait_max_us: u64,
    pub l2p_get_calls: u64,
    pub l2p_get_lock_wait_us: u64,
    pub l2p_get_lock_wait_max_us: u64,
    pub l2p_get_tree_walk_us: u64,
    pub l2p_get_tree_walk_max_us: u64,
    pub l2p_multi_get_calls: u64,
    pub l2p_multi_get_lbas: u64,
    pub l2p_multi_get_pin_us: u64,
    pub l2p_multi_get_pin_max_us: u64,
    pub l2p_multi_get_volume_us: u64,
    pub l2p_multi_get_volume_max_us: u64,
    pub l2p_multi_get_sort_us: u64,
    pub l2p_multi_get_sort_max_us: u64,
    pub l2p_multi_get_view_us: u64,
    pub l2p_multi_get_view_max_us: u64,
    pub l2p_multi_get_tree_us: u64,
    pub l2p_multi_get_tree_max_us: u64,
    pub flush_calls: u64,
    pub flush_calls_steady: u64,
    pub flush_calls_forced: u64,
    pub flush_total_us: u64,
    pub flush_total_max_us: u64,
    pub flush_total_us_steady: u64,
    pub flush_total_max_us_steady: u64,
    pub flush_total_us_forced: u64,
    pub flush_total_max_us_forced: u64,
    pub flush_gate_wait_us: u64,
    pub flush_gate_wait_max_us: u64,
    pub flush_sample_us: u64,
    pub flush_sample_max_us: u64,
    pub flush_sample_us_steady: u64,
    pub flush_sample_max_us_steady: u64,
    pub flush_sample_us_forced: u64,
    pub flush_sample_max_us_forced: u64,
    pub flush_io_us: u64,
    pub flush_io_max_us: u64,
    pub flush_io_seal_us: u64,
    pub flush_io_seal_max_us: u64,
    pub flush_io_page_write_us: u64,
    pub flush_io_page_write_max_us: u64,
    pub flush_io_rc_meta_us: u64,
    pub flush_io_rc_meta_max_us: u64,
    pub flush_io_sync_us: u64,
    pub flush_io_sync_max_us: u64,
    pub flush_manifest_us: u64,
    pub flush_manifest_max_us: u64,
    pub flush_install_us: u64,
    pub flush_install_max_us: u64,
    pub flush_reclaim_us: u64,
    pub flush_reclaim_max_us: u64,
    pub flush_pages_written: u64,
    pub flush_reclaim_budget_pages: u64,
    pub flush_reclaim_selected_pages: u64,
    pub flush_reclaim_reclaimed_pages: u64,
    pub flush_reclaim_blocked_pages: u64,
    pub flush_sample_l2p_dirty_pages: u64,
    pub flush_sample_l2p_dirty_pages_max: u64,
    pub flush_sample_rc_drained_deltas: u64,
    pub flush_sample_rc_drained_deltas_max: u64,
    pub flush_sample_rc_fresh_pages: u64,
    pub flush_sample_rc_fresh_pages_max: u64,
    pub rc_drainer_cycles: u64,
    pub rc_drainer_drained_entries: u64,
    pub rc_drainer_pages_built: u64,
    pub rc_drainer_cycle_us: u64,
    pub rc_drainer_cycle_max_us: u64,
    pub rc_drainer_overlay_size_max_pages: u64,
    pub rc_drainer_checkpoint_wait_us: u64,
    pub rc_drainer_checkpoint_wait_max_us: u64,
    pub rc_drainer_backpressure_fallbacks: u64,
    pub rc_drainer_pool_refills: u64,

    pub flush_io_bytes_total: u64,

    pub meta_io_write_calls: u64,
    pub meta_io_write_ops: u64,
    pub meta_io_write_bytes: u64,
    pub meta_io_write_us: u64,
    pub meta_io_write_max_us: u64,
    pub meta_io_write_batch_ops_max: u64,
    pub meta_io_write_batch_bytes_max: u64,

    pub meta_io_read_calls: u64,
    pub meta_io_read_ops: u64,
    pub meta_io_read_bytes: u64,
    pub meta_io_read_us: u64,
    pub meta_io_read_max_us: u64,
    pub meta_io_read_batch_ops_max: u64,

    pub meta_io_fsync_calls: u64,
    pub meta_io_fsync_us: u64,
    pub meta_io_fsync_max_us: u64,

    pub meta_io_write_uring_lock_acquires: u64,
    pub meta_io_write_uring_lock_wait_us: u64,
    pub meta_io_write_uring_lock_wait_max_us: u64,

    pub io_submitter_iterations: u64,
    pub io_submitter_sqes_submitted: u64,
    pub io_submitter_channel_pending_max: u64,
    pub io_submitter_submit_batch_size_max: u64,
    pub io_submitter_inflight_max: u64,

    pub l2p_writeback_cycles: u64,
    pub l2p_writeback_cycles_with_work: u64,
    pub l2p_writeback_shards_flushed: u64,
    pub l2p_writeback_pages_promoted: u64,
    pub l2p_writeback_pages_kept_dirty: u64,
    pub l2p_writeback_pages_written: u64,
    pub l2p_writeback_io_bytes_total: u64,
    pub l2p_writeback_seal_us: u64,
    pub l2p_writeback_seal_max_us: u64,
    pub l2p_writeback_io_us: u64,
    pub l2p_writeback_io_max_us: u64,
    pub l2p_writeback_install_us: u64,
    pub l2p_writeback_install_max_us: u64,
    pub l2p_writeback_errors: u64,
}

impl MetaMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> MetaMetricsSnapshot {
        MetaMetricsSnapshot {
            commit_attempts: load(&self.commit_attempts),
            commit_success: load(&self.commit_success),
            commit_errors: load(&self.commit_errors),
            commit_empty: load(&self.commit_empty),
            commit_ops: load(&self.commit_ops),
            commit_ops_per_tx_max: load(&self.commit_ops_per_tx_max),
            commit_wal_body_bytes: load(&self.commit_wal_body_bytes),
            commit_wal_body_bytes_max: load(&self.commit_wal_body_bytes_max),
            commit_total_us: load(&self.commit_total_us),
            commit_total_max_us: load(&self.commit_total_max_us),
            commit_wal_submit_us: load(&self.commit_wal_submit_us),
            commit_wal_submit_max_us: load(&self.commit_wal_submit_max_us),
            commit_drop_gate_wait_us: load(&self.commit_drop_gate_wait_us),
            commit_drop_gate_wait_max_us: load(&self.commit_drop_gate_wait_max_us),
            commit_apply_wait_us: load(&self.commit_apply_wait_us),
            commit_apply_wait_max_us: load(&self.commit_apply_wait_max_us),
            commit_apply_gate_wait_us: load(&self.commit_apply_gate_wait_us),
            commit_apply_gate_wait_max_us: load(&self.commit_apply_gate_wait_max_us),
            commit_finish_global_wait_us: load(&self.commit_finish_global_wait_us),
            commit_finish_global_wait_max_us: load(&self.commit_finish_global_wait_max_us),
            commit_apply_us: load(&self.commit_apply_us),
            commit_apply_max_us: load(&self.commit_apply_max_us),
            commit_apply_l2p_wait_us: load(&self.commit_apply_l2p_wait_us),
            commit_apply_l2p_wait_max_us: load(&self.commit_apply_l2p_wait_max_us),
            commit_apply_rc_enqueue_us: load(&self.commit_apply_rc_enqueue_us),
            commit_apply_rc_enqueue_max_us: load(&self.commit_apply_rc_enqueue_max_us),
            commit_apply_rc_wait_us: load(&self.commit_apply_rc_wait_us),
            commit_apply_rc_wait_max_us: load(&self.commit_apply_rc_wait_max_us),
            commit_apply_dedup_enqueue_us: load(&self.commit_apply_dedup_enqueue_us),
            commit_apply_dedup_enqueue_max_us: load(&self.commit_apply_dedup_enqueue_max_us),
            commit_apply_dedup_wait_us: load(&self.commit_apply_dedup_wait_us),
            commit_apply_dedup_wait_max_us: load(&self.commit_apply_dedup_wait_max_us),
            wal_submit_calls: load(&self.wal_submit_calls),
            wal_submit_wait_us: load(&self.wal_submit_wait_us),
            wal_submit_wait_max_us: load(&self.wal_submit_wait_max_us),
            wal_batches: load(&self.wal_batches),
            wal_records: load(&self.wal_records),
            wal_bytes: load(&self.wal_bytes),
            wal_rotates: load(&self.wal_rotates),
            wal_write_us: load(&self.wal_write_us),
            wal_write_max_us: load(&self.wal_write_max_us),
            wal_fsyncs: load(&self.wal_fsyncs),
            wal_fsync_us: load(&self.wal_fsync_us),
            wal_fsync_max_us: load(&self.wal_fsync_max_us),
            wal_batch_records_max: load(&self.wal_batch_records_max),
            wal_batch_bytes_max: load(&self.wal_batch_bytes_max),
            range_delete_calls: load(&self.range_delete_calls),
            range_delete_success: load(&self.range_delete_success),
            range_delete_errors: load(&self.range_delete_errors),
            range_delete_noop: load(&self.range_delete_noop),
            range_delete_captured_entries: load(&self.range_delete_captured_entries),
            range_delete_chunks: load(&self.range_delete_chunks),
            range_delete_total_us: load(&self.range_delete_total_us),
            range_delete_total_max_us: load(&self.range_delete_total_max_us),
            range_delete_scan_us: load(&self.range_delete_scan_us),
            range_delete_scan_max_us: load(&self.range_delete_scan_max_us),
            range_delete_wal_us: load(&self.range_delete_wal_us),
            range_delete_wal_max_us: load(&self.range_delete_wal_max_us),
            range_delete_apply_wait_us: load(&self.range_delete_apply_wait_us),
            range_delete_apply_wait_max_us: load(&self.range_delete_apply_wait_max_us),
            range_delete_apply_us: load(&self.range_delete_apply_us),
            range_delete_apply_max_us: load(&self.range_delete_apply_max_us),
            range_delete_drop_gate_wait_us: load(&self.range_delete_drop_gate_wait_us),
            range_delete_drop_gate_wait_max_us: load(&self.range_delete_drop_gate_wait_max_us),
            range_delete_apply_gate_wait_us: load(&self.range_delete_apply_gate_wait_us),
            range_delete_apply_gate_wait_max_us: load(&self.range_delete_apply_gate_wait_max_us),
            cleanup_calls: load(&self.cleanup_calls),
            cleanup_success: load(&self.cleanup_success),
            cleanup_errors: load(&self.cleanup_errors),
            cleanup_noop: load(&self.cleanup_noop),
            cleanup_pbas: load(&self.cleanup_pbas),
            cleanup_hashes_found: load(&self.cleanup_hashes_found),
            cleanup_forward_checks: load(&self.cleanup_forward_checks),
            cleanup_tombstones_emitted: load(&self.cleanup_tombstones_emitted),
            cleanup_tx_ops: load(&self.cleanup_tx_ops),
            cleanup_total_us: load(&self.cleanup_total_us),
            cleanup_total_max_us: load(&self.cleanup_total_max_us),
            cleanup_scan_us: load(&self.cleanup_scan_us),
            cleanup_scan_max_us: load(&self.cleanup_scan_max_us),
            cleanup_forward_check_us: load(&self.cleanup_forward_check_us),
            cleanup_forward_check_max_us: load(&self.cleanup_forward_check_max_us),
            cleanup_commit_us: load(&self.cleanup_commit_us),
            cleanup_commit_max_us: load(&self.cleanup_commit_max_us),
            apply_l2p_put_count: load(&self.apply_l2p_put_count),
            apply_l2p_put_us: load(&self.apply_l2p_put_us),
            apply_l2p_put_max_us: load(&self.apply_l2p_put_max_us),
            apply_l2p_delete_count: load(&self.apply_l2p_delete_count),
            apply_l2p_delete_us: load(&self.apply_l2p_delete_us),
            apply_l2p_delete_max_us: load(&self.apply_l2p_delete_max_us),
            apply_l2p_remap_count: load(&self.apply_l2p_remap_count),
            apply_l2p_remap_us: load(&self.apply_l2p_remap_us),
            apply_l2p_remap_max_us: load(&self.apply_l2p_remap_max_us),
            apply_l2p_range_delete_count: load(&self.apply_l2p_range_delete_count),
            apply_l2p_range_delete_us: load(&self.apply_l2p_range_delete_us),
            apply_l2p_range_delete_max_us: load(&self.apply_l2p_range_delete_max_us),
            apply_l2p_bucket_count: load(&self.apply_l2p_bucket_count),
            apply_l2p_bucket_ops: load(&self.apply_l2p_bucket_ops),
            apply_l2p_bucket_total_us: load(&self.apply_l2p_bucket_total_us),
            apply_l2p_bucket_total_max_us: load(&self.apply_l2p_bucket_total_max_us),
            apply_l2p_bucket_tree_lock_wait_us: load(&self.apply_l2p_bucket_tree_lock_wait_us),
            apply_l2p_bucket_tree_lock_wait_max_us: load(
                &self.apply_l2p_bucket_tree_lock_wait_max_us,
            ),
            apply_l2p_bucket_read_view_prepare_us: load(
                &self.apply_l2p_bucket_read_view_prepare_us,
            ),
            apply_l2p_bucket_read_view_prepare_max_us: load(
                &self.apply_l2p_bucket_read_view_prepare_max_us,
            ),
            apply_l2p_bucket_ops_us: load(&self.apply_l2p_bucket_ops_us),
            apply_l2p_bucket_ops_max_us: load(&self.apply_l2p_bucket_ops_max_us),
            apply_l2p_bucket_finish_us: load(&self.apply_l2p_bucket_finish_us),
            apply_l2p_bucket_finish_max_us: load(&self.apply_l2p_bucket_finish_max_us),
            apply_l2p_bucket_publish_us: load(&self.apply_l2p_bucket_publish_us),
            apply_l2p_bucket_publish_max_us: load(&self.apply_l2p_bucket_publish_max_us),
            apply_refcount_count: load(&self.apply_refcount_count),
            apply_refcount_us: load(&self.apply_refcount_us),
            apply_refcount_max_us: load(&self.apply_refcount_max_us),
            apply_dedup_count: load(&self.apply_dedup_count),
            apply_dedup_us: load(&self.apply_dedup_us),
            apply_dedup_max_us: load(&self.apply_dedup_max_us),
            dedup_lane_tasks: load(&self.dedup_lane_tasks),
            dedup_lane_ops: load(&self.dedup_lane_ops),
            dedup_lane_ready_queue_wait_us: load(&self.dedup_lane_ready_queue_wait_us),
            dedup_lane_ready_queue_wait_max_us: load(&self.dedup_lane_ready_queue_wait_max_us),
            dedup_lane_exec_us: load(&self.dedup_lane_exec_us),
            dedup_lane_exec_max_us: load(&self.dedup_lane_exec_max_us),
            dedup_lane_idle_us: load(&self.dedup_lane_idle_us),
            dedup_lane_idle_max_us: load(&self.dedup_lane_idle_max_us),
            dedup_lane_wakeups: load(&self.dedup_lane_wakeups),
            dedup_lane_empty_wakeups: load(&self.dedup_lane_empty_wakeups),
            dedup_lane_burst_total: load(&self.dedup_lane_burst_total),
            dedup_lane_burst_max: load(&self.dedup_lane_burst_max),
            dedup_lane_queue_depth_max: load(&self.dedup_lane_queue_depth_max),
            l2p_apply_lane_tasks: load(&self.l2p_apply_lane_tasks),
            l2p_apply_lane_queue_depth_max: load(&self.l2p_apply_lane_queue_depth_max),
            l2p_apply_lane_queue_wait_us: load(&self.l2p_apply_lane_queue_wait_us),
            l2p_apply_lane_queue_wait_max_us: load(&self.l2p_apply_lane_queue_wait_max_us),
            l2p_apply_lane_exec_us: load(&self.l2p_apply_lane_exec_us),
            l2p_apply_lane_exec_max_us: load(&self.l2p_apply_lane_exec_max_us),
            l2p_apply_lane_idle_us: load(&self.l2p_apply_lane_idle_us),
            l2p_apply_lane_idle_max_us: load(&self.l2p_apply_lane_idle_max_us),
            l2p_apply_lane_wakeups: load(&self.l2p_apply_lane_wakeups),
            l2p_apply_lane_empty_wakeups: load(&self.l2p_apply_lane_empty_wakeups),
            l2p_apply_lane_burst_total: load(&self.l2p_apply_lane_burst_total),
            l2p_apply_lane_burst_max: load(&self.l2p_apply_lane_burst_max),
            rc_apply_lane_tasks: load(&self.rc_apply_lane_tasks),
            rc_apply_lane_queue_depth_max: load(&self.rc_apply_lane_queue_depth_max),
            rc_apply_lane_queue_wait_us: load(&self.rc_apply_lane_queue_wait_us),
            rc_apply_lane_queue_wait_max_us: load(&self.rc_apply_lane_queue_wait_max_us),
            rc_apply_lane_exec_us: load(&self.rc_apply_lane_exec_us),
            rc_apply_lane_exec_max_us: load(&self.rc_apply_lane_exec_max_us),
            rc_apply_lane_idle_us: load(&self.rc_apply_lane_idle_us),
            rc_apply_lane_idle_max_us: load(&self.rc_apply_lane_idle_max_us),
            rc_apply_lane_pending_set_wait_us: load(&self.rc_apply_lane_pending_set_wait_us),
            rc_apply_lane_pending_set_wait_max_us: load(
                &self.rc_apply_lane_pending_set_wait_max_us,
            ),
            rc_apply_lane_wakeups: load(&self.rc_apply_lane_wakeups),
            rc_apply_lane_empty_wakeups: load(&self.rc_apply_lane_empty_wakeups),
            rc_apply_lane_burst_total: load(&self.rc_apply_lane_burst_total),
            rc_apply_lane_burst_max: load(&self.rc_apply_lane_burst_max),
            l2p_apply_lane_shard_tasks: load_shards(&self.l2p_apply_lane_shard_tasks),
            l2p_apply_lane_shard_queue_depth_max: load_shards(
                &self.l2p_apply_lane_shard_queue_depth_max,
            ),
            l2p_apply_lane_shard_queue_wait_us: load_shards(
                &self.l2p_apply_lane_shard_queue_wait_us,
            ),
            l2p_apply_lane_shard_queue_wait_max_us: load_shards(
                &self.l2p_apply_lane_shard_queue_wait_max_us,
            ),
            l2p_apply_lane_shard_exec_us: load_shards(&self.l2p_apply_lane_shard_exec_us),
            l2p_apply_lane_shard_exec_max_us: load_shards(&self.l2p_apply_lane_shard_exec_max_us),
            l2p_apply_lane_shard_idle_us: load_shards(&self.l2p_apply_lane_shard_idle_us),
            l2p_apply_lane_shard_wakeups: load_shards(&self.l2p_apply_lane_shard_wakeups),
            l2p_apply_lane_shard_empty_wakeups: load_shards(
                &self.l2p_apply_lane_shard_empty_wakeups,
            ),
            l2p_apply_lane_shard_burst_total: load_shards(&self.l2p_apply_lane_shard_burst_total),
            l2p_apply_lane_shard_burst_max: load_shards(&self.l2p_apply_lane_shard_burst_max),
            rc_apply_lane_shard_tasks: load_shards(&self.rc_apply_lane_shard_tasks),
            rc_apply_lane_shard_queue_depth_max: load_shards(
                &self.rc_apply_lane_shard_queue_depth_max,
            ),
            rc_apply_lane_shard_queue_wait_us: load_shards(&self.rc_apply_lane_shard_queue_wait_us),
            rc_apply_lane_shard_queue_wait_max_us: load_shards(
                &self.rc_apply_lane_shard_queue_wait_max_us,
            ),
            rc_apply_lane_shard_exec_us: load_shards(&self.rc_apply_lane_shard_exec_us),
            rc_apply_lane_shard_exec_max_us: load_shards(&self.rc_apply_lane_shard_exec_max_us),
            rc_apply_lane_shard_idle_us: load_shards(&self.rc_apply_lane_shard_idle_us),
            rc_apply_lane_shard_pending_set_wait_us: load_shards(
                &self.rc_apply_lane_shard_pending_set_wait_us,
            ),
            rc_apply_lane_shard_pending_set_wait_max_us: load_shards(
                &self.rc_apply_lane_shard_pending_set_wait_max_us,
            ),
            rc_apply_lane_shard_wakeups: load_shards(&self.rc_apply_lane_shard_wakeups),
            rc_apply_lane_shard_empty_wakeups: load_shards(&self.rc_apply_lane_shard_empty_wakeups),
            rc_apply_lane_shard_burst_total: load_shards(&self.rc_apply_lane_shard_burst_total),
            rc_apply_lane_shard_burst_max: load_shards(&self.rc_apply_lane_shard_burst_max),
            dedup_apply_guard_count: load(&self.dedup_apply_guard_count),
            dedup_apply_guard_us: load(&self.dedup_apply_guard_us),
            dedup_apply_guard_max_us: load(&self.dedup_apply_guard_max_us),
            dedup_apply_forward_put_count: load(&self.dedup_apply_forward_put_count),
            dedup_apply_forward_put_us: load(&self.dedup_apply_forward_put_us),
            dedup_apply_forward_put_max_us: load(&self.dedup_apply_forward_put_max_us),
            dedup_apply_forward_delete_count: load(&self.dedup_apply_forward_delete_count),
            dedup_apply_forward_delete_us: load(&self.dedup_apply_forward_delete_us),
            dedup_apply_forward_delete_max_us: load(&self.dedup_apply_forward_delete_max_us),
            dedup_apply_reverse_put_count: load(&self.dedup_apply_reverse_put_count),
            dedup_apply_reverse_put_us: load(&self.dedup_apply_reverse_put_us),
            dedup_apply_reverse_put_max_us: load(&self.dedup_apply_reverse_put_max_us),
            dedup_apply_reverse_delete_count: load(&self.dedup_apply_reverse_delete_count),
            dedup_apply_reverse_delete_us: load(&self.dedup_apply_reverse_delete_us),
            dedup_apply_reverse_delete_max_us: load(&self.dedup_apply_reverse_delete_max_us),
            dedup_put_l0_insert_us: load(&self.dedup_put_l0_insert_us),
            dedup_put_l0_insert_max_us: load(&self.dedup_put_l0_insert_max_us),
            dedup_put_l1_put_us: load(&self.dedup_put_l1_put_us),
            dedup_put_l1_put_max_us: load(&self.dedup_put_l1_put_max_us),
            dedup_put_cuckoo_update_existing_us: load(&self.dedup_put_cuckoo_update_existing_us),
            dedup_put_cuckoo_update_existing_max_us: load(
                &self.dedup_put_cuckoo_update_existing_max_us,
            ),
            dedup_put_cuckoo_free_slots_us: load(&self.dedup_put_cuckoo_free_slots_us),
            dedup_put_cuckoo_free_slots_max_us: load(&self.dedup_put_cuckoo_free_slots_max_us),
            dedup_put_cuckoo_try_insert_empty_us: load(&self.dedup_put_cuckoo_try_insert_empty_us),
            dedup_put_cuckoo_try_insert_empty_max_us: load(
                &self.dedup_put_cuckoo_try_insert_empty_max_us,
            ),
            dedup_put_cuckoo_evict_and_insert_us: load(&self.dedup_put_cuckoo_evict_and_insert_us),
            dedup_put_cuckoo_evict_and_insert_max_us: load(
                &self.dedup_put_cuckoo_evict_and_insert_max_us,
            ),
            dedup_put_cuckoo_page_read_cache_wait_us: load(
                &self.dedup_put_cuckoo_page_read_cache_wait_us,
            ),
            dedup_put_cuckoo_page_read_cache_wait_max_us: load(
                &self.dedup_put_cuckoo_page_read_cache_wait_max_us,
            ),
            dedup_put_cuckoo_page_alloc_us: load(&self.dedup_put_cuckoo_page_alloc_us),
            dedup_put_cuckoo_page_alloc_max_us: load(&self.dedup_put_cuckoo_page_alloc_max_us),
            dedup_put_cuckoo_page_write_publish_us: load(
                &self.dedup_put_cuckoo_page_write_publish_us,
            ),
            dedup_put_cuckoo_page_write_publish_max_us: load(
                &self.dedup_put_cuckoo_page_write_publish_max_us,
            ),
            dedup_put_cuckoo_bucket_lock_wait_us: load(&self.dedup_put_cuckoo_bucket_lock_wait_us),
            dedup_put_cuckoo_bucket_lock_wait_max_us: load(
                &self.dedup_put_cuckoo_bucket_lock_wait_max_us,
            ),
            l2p_get_calls: load(&self.l2p_get_calls),
            l2p_get_lock_wait_us: load(&self.l2p_get_lock_wait_us),
            l2p_get_lock_wait_max_us: load(&self.l2p_get_lock_wait_max_us),
            l2p_get_tree_walk_us: load(&self.l2p_get_tree_walk_us),
            l2p_get_tree_walk_max_us: load(&self.l2p_get_tree_walk_max_us),
            l2p_multi_get_calls: load(&self.l2p_multi_get_calls),
            l2p_multi_get_lbas: load(&self.l2p_multi_get_lbas),
            l2p_multi_get_pin_us: load(&self.l2p_multi_get_pin_us),
            l2p_multi_get_pin_max_us: load(&self.l2p_multi_get_pin_max_us),
            l2p_multi_get_volume_us: load(&self.l2p_multi_get_volume_us),
            l2p_multi_get_volume_max_us: load(&self.l2p_multi_get_volume_max_us),
            l2p_multi_get_sort_us: load(&self.l2p_multi_get_sort_us),
            l2p_multi_get_sort_max_us: load(&self.l2p_multi_get_sort_max_us),
            l2p_multi_get_view_us: load(&self.l2p_multi_get_view_us),
            l2p_multi_get_view_max_us: load(&self.l2p_multi_get_view_max_us),
            l2p_multi_get_tree_us: load(&self.l2p_multi_get_tree_us),
            l2p_multi_get_tree_max_us: load(&self.l2p_multi_get_tree_max_us),
            flush_calls: load(&self.flush_calls),
            flush_calls_steady: load(&self.flush_calls_steady),
            flush_calls_forced: load(&self.flush_calls_forced),
            flush_total_us: load(&self.flush_total_us),
            flush_total_max_us: load(&self.flush_total_max_us),
            flush_total_us_steady: load(&self.flush_total_us_steady),
            flush_total_max_us_steady: load(&self.flush_total_max_us_steady),
            flush_total_us_forced: load(&self.flush_total_us_forced),
            flush_total_max_us_forced: load(&self.flush_total_max_us_forced),
            flush_gate_wait_us: load(&self.flush_gate_wait_us),
            flush_gate_wait_max_us: load(&self.flush_gate_wait_max_us),
            flush_sample_us: load(&self.flush_sample_us),
            flush_sample_max_us: load(&self.flush_sample_max_us),
            flush_sample_us_steady: load(&self.flush_sample_us_steady),
            flush_sample_max_us_steady: load(&self.flush_sample_max_us_steady),
            flush_sample_us_forced: load(&self.flush_sample_us_forced),
            flush_sample_max_us_forced: load(&self.flush_sample_max_us_forced),
            flush_io_us: load(&self.flush_io_us),
            flush_io_max_us: load(&self.flush_io_max_us),
            flush_io_seal_us: load(&self.flush_io_seal_us),
            flush_io_seal_max_us: load(&self.flush_io_seal_max_us),
            flush_io_page_write_us: load(&self.flush_io_page_write_us),
            flush_io_page_write_max_us: load(&self.flush_io_page_write_max_us),
            flush_io_rc_meta_us: load(&self.flush_io_rc_meta_us),
            flush_io_rc_meta_max_us: load(&self.flush_io_rc_meta_max_us),
            flush_io_sync_us: load(&self.flush_io_sync_us),
            flush_io_sync_max_us: load(&self.flush_io_sync_max_us),
            flush_manifest_us: load(&self.flush_manifest_us),
            flush_manifest_max_us: load(&self.flush_manifest_max_us),
            flush_install_us: load(&self.flush_install_us),
            flush_install_max_us: load(&self.flush_install_max_us),
            flush_reclaim_us: load(&self.flush_reclaim_us),
            flush_reclaim_max_us: load(&self.flush_reclaim_max_us),
            flush_pages_written: load(&self.flush_pages_written),
            flush_reclaim_budget_pages: load(&self.flush_reclaim_budget_pages),
            flush_reclaim_selected_pages: load(&self.flush_reclaim_selected_pages),
            flush_reclaim_reclaimed_pages: load(&self.flush_reclaim_reclaimed_pages),
            flush_reclaim_blocked_pages: load(&self.flush_reclaim_blocked_pages),
            flush_sample_l2p_dirty_pages: load(&self.flush_sample_l2p_dirty_pages),
            flush_sample_l2p_dirty_pages_max: load(&self.flush_sample_l2p_dirty_pages_max),
            flush_sample_rc_drained_deltas: load(&self.flush_sample_rc_drained_deltas),
            flush_sample_rc_drained_deltas_max: load(&self.flush_sample_rc_drained_deltas_max),
            flush_sample_rc_fresh_pages: load(&self.flush_sample_rc_fresh_pages),
            flush_sample_rc_fresh_pages_max: load(&self.flush_sample_rc_fresh_pages_max),
            rc_drainer_cycles: load(&self.rc_drainer_cycles),
            rc_drainer_drained_entries: load(&self.rc_drainer_drained_entries),
            rc_drainer_pages_built: load(&self.rc_drainer_pages_built),
            rc_drainer_cycle_us: load(&self.rc_drainer_cycle_us),
            rc_drainer_cycle_max_us: load(&self.rc_drainer_cycle_max_us),
            rc_drainer_overlay_size_max_pages: load(&self.rc_drainer_overlay_size_max_pages),
            rc_drainer_checkpoint_wait_us: load(&self.rc_drainer_checkpoint_wait_us),
            rc_drainer_checkpoint_wait_max_us: load(&self.rc_drainer_checkpoint_wait_max_us),
            rc_drainer_backpressure_fallbacks: load(&self.rc_drainer_backpressure_fallbacks),
            rc_drainer_pool_refills: load(&self.rc_drainer_pool_refills),

            flush_io_bytes_total: load(&self.flush_io_bytes_total),

            meta_io_write_calls: load(&self.meta_io_write_calls),
            meta_io_write_ops: load(&self.meta_io_write_ops),
            meta_io_write_bytes: load(&self.meta_io_write_bytes),
            meta_io_write_us: load(&self.meta_io_write_us),
            meta_io_write_max_us: load(&self.meta_io_write_max_us),
            meta_io_write_batch_ops_max: load(&self.meta_io_write_batch_ops_max),
            meta_io_write_batch_bytes_max: load(&self.meta_io_write_batch_bytes_max),

            meta_io_read_calls: load(&self.meta_io_read_calls),
            meta_io_read_ops: load(&self.meta_io_read_ops),
            meta_io_read_bytes: load(&self.meta_io_read_bytes),
            meta_io_read_us: load(&self.meta_io_read_us),
            meta_io_read_max_us: load(&self.meta_io_read_max_us),
            meta_io_read_batch_ops_max: load(&self.meta_io_read_batch_ops_max),

            meta_io_fsync_calls: load(&self.meta_io_fsync_calls),
            meta_io_fsync_us: load(&self.meta_io_fsync_us),
            meta_io_fsync_max_us: load(&self.meta_io_fsync_max_us),

            meta_io_write_uring_lock_acquires: load(&self.meta_io_write_uring_lock_acquires),
            meta_io_write_uring_lock_wait_us: load(&self.meta_io_write_uring_lock_wait_us),
            meta_io_write_uring_lock_wait_max_us: load(&self.meta_io_write_uring_lock_wait_max_us),

            io_submitter_iterations: load(&self.io_submitter_iterations),
            io_submitter_sqes_submitted: load(&self.io_submitter_sqes_submitted),
            io_submitter_channel_pending_max: load(&self.io_submitter_channel_pending_max),
            io_submitter_submit_batch_size_max: load(&self.io_submitter_submit_batch_size_max),
            io_submitter_inflight_max: load(&self.io_submitter_inflight_max),

            l2p_writeback_cycles: load(&self.l2p_writeback_cycles),
            l2p_writeback_cycles_with_work: load(&self.l2p_writeback_cycles_with_work),
            l2p_writeback_shards_flushed: load(&self.l2p_writeback_shards_flushed),
            l2p_writeback_pages_promoted: load(&self.l2p_writeback_pages_promoted),
            l2p_writeback_pages_kept_dirty: load(&self.l2p_writeback_pages_kept_dirty),
            l2p_writeback_pages_written: load(&self.l2p_writeback_pages_written),
            l2p_writeback_io_bytes_total: load(&self.l2p_writeback_io_bytes_total),
            l2p_writeback_seal_us: load(&self.l2p_writeback_seal_us),
            l2p_writeback_seal_max_us: load(&self.l2p_writeback_seal_max_us),
            l2p_writeback_io_us: load(&self.l2p_writeback_io_us),
            l2p_writeback_io_max_us: load(&self.l2p_writeback_io_max_us),
            l2p_writeback_install_us: load(&self.l2p_writeback_install_us),
            l2p_writeback_install_max_us: load(&self.l2p_writeback_install_max_us),
            l2p_writeback_errors: load(&self.l2p_writeback_errors),
        }
    }

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

    pub(crate) fn record_commit_empty(&self) {
        self.commit_empty.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_commit_attempt(&self, ops: usize) {
        self.commit_attempts.fetch_add(1, Ordering::Relaxed);
        self.commit_ops.fetch_add(ops as u64, Ordering::Relaxed);
        fetch_max(&self.commit_ops_per_tx_max, ops as u64);
    }

    pub(crate) fn record_commit_wal_body_bytes(&self, bytes: usize) {
        self.commit_wal_body_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        fetch_max(&self.commit_wal_body_bytes_max, bytes as u64);
    }

    pub(crate) fn record_commit_success(&self, total: Duration) {
        self.commit_success.fetch_add(1, Ordering::Relaxed);
        record_duration(&self.commit_total_us, &self.commit_total_max_us, total);
    }

    pub(crate) fn record_commit_error(&self, total: Duration) {
        self.commit_errors.fetch_add(1, Ordering::Relaxed);
        record_duration(&self.commit_total_us, &self.commit_total_max_us, total);
    }

    pub(crate) fn record_commit_wal_submit(&self, elapsed: Duration) {
        record_duration(
            &self.commit_wal_submit_us,
            &self.commit_wal_submit_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_commit_drop_gate_wait(&self, elapsed: Duration) {
        record_duration(
            &self.commit_drop_gate_wait_us,
            &self.commit_drop_gate_wait_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_commit_apply_wait(&self, elapsed: Duration) {
        record_duration(
            &self.commit_apply_wait_us,
            &self.commit_apply_wait_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_commit_apply_gate_wait(&self, elapsed: Duration) {
        record_duration(
            &self.commit_apply_gate_wait_us,
            &self.commit_apply_gate_wait_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_commit_finish_global_wait(&self, elapsed: Duration) {
        record_duration(
            &self.commit_finish_global_wait_us,
            &self.commit_finish_global_wait_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_commit_apply(&self, elapsed: Duration) {
        record_duration(&self.commit_apply_us, &self.commit_apply_max_us, elapsed);
    }

    pub(crate) fn record_commit_apply_laned(
        &self,
        l2p_wait: Duration,
        rc_enqueue: Duration,
        rc_wait: Duration,
        dedup_enqueue: Duration,
        dedup_wait: Duration,
    ) {
        record_duration(
            &self.commit_apply_l2p_wait_us,
            &self.commit_apply_l2p_wait_max_us,
            l2p_wait,
        );
        record_duration(
            &self.commit_apply_rc_enqueue_us,
            &self.commit_apply_rc_enqueue_max_us,
            rc_enqueue,
        );
        record_duration(
            &self.commit_apply_rc_wait_us,
            &self.commit_apply_rc_wait_max_us,
            rc_wait,
        );
        record_duration(
            &self.commit_apply_dedup_enqueue_us,
            &self.commit_apply_dedup_enqueue_max_us,
            dedup_enqueue,
        );
        record_duration(
            &self.commit_apply_dedup_wait_us,
            &self.commit_apply_dedup_wait_max_us,
            dedup_wait,
        );
    }

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

    pub(crate) fn record_l2p_get(&self, lock_wait: Duration, tree_walk: Duration) {
        self.l2p_get_calls.fetch_add(1, Ordering::Relaxed);
        record_duration(
            &self.l2p_get_lock_wait_us,
            &self.l2p_get_lock_wait_max_us,
            lock_wait,
        );
        record_duration(
            &self.l2p_get_tree_walk_us,
            &self.l2p_get_tree_walk_max_us,
            tree_walk,
        );
    }

    pub(crate) fn record_l2p_multi_get_call(&self, lbas: usize) {
        self.l2p_multi_get_calls.fetch_add(1, Ordering::Relaxed);
        self.l2p_multi_get_lbas
            .fetch_add(lbas as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_l2p_multi_get_pin(&self, elapsed: Duration) {
        record_duration(
            &self.l2p_multi_get_pin_us,
            &self.l2p_multi_get_pin_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_l2p_multi_get_volume(&self, elapsed: Duration) {
        record_duration(
            &self.l2p_multi_get_volume_us,
            &self.l2p_multi_get_volume_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_l2p_multi_get_sort(&self, elapsed: Duration) {
        record_duration(
            &self.l2p_multi_get_sort_us,
            &self.l2p_multi_get_sort_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_l2p_multi_get_view(&self, elapsed: Duration) {
        record_duration(
            &self.l2p_multi_get_view_us,
            &self.l2p_multi_get_view_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_l2p_multi_get_tree(&self, elapsed: Duration) {
        record_duration(
            &self.l2p_multi_get_tree_us,
            &self.l2p_multi_get_tree_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_wal_submit_wait(&self, elapsed: Duration) {
        self.wal_submit_calls.fetch_add(1, Ordering::Relaxed);
        record_duration(
            &self.wal_submit_wait_us,
            &self.wal_submit_wait_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_wal_rotate(&self) {
        self.wal_rotates.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_wal_batch(&self, records: usize, bytes: usize) {
        self.wal_batches.fetch_add(1, Ordering::Relaxed);
        self.wal_records
            .fetch_add(records as u64, Ordering::Relaxed);
        self.wal_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        fetch_max(&self.wal_batch_records_max, records as u64);
        fetch_max(&self.wal_batch_bytes_max, bytes as u64);
    }

    pub(crate) fn record_wal_write(&self, elapsed: Duration) {
        record_duration(&self.wal_write_us, &self.wal_write_max_us, elapsed);
    }

    pub(crate) fn record_wal_fsync(&self, elapsed: Duration) {
        self.wal_fsyncs.fetch_add(1, Ordering::Relaxed);
        record_duration(&self.wal_fsync_us, &self.wal_fsync_max_us, elapsed);
    }

    pub(crate) fn record_range_delete_call(&self) {
        self.range_delete_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_range_delete_success(&self, total: Duration) {
        self.range_delete_success.fetch_add(1, Ordering::Relaxed);
        record_duration(
            &self.range_delete_total_us,
            &self.range_delete_total_max_us,
            total,
        );
    }

    pub(crate) fn record_range_delete_error(&self, total: Duration) {
        self.range_delete_errors.fetch_add(1, Ordering::Relaxed);
        record_duration(
            &self.range_delete_total_us,
            &self.range_delete_total_max_us,
            total,
        );
    }

    pub(crate) fn record_range_delete_noop(&self) {
        self.range_delete_noop.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_range_delete_scan(&self, elapsed: Duration, captured: usize) {
        self.range_delete_captured_entries
            .fetch_add(captured as u64, Ordering::Relaxed);
        record_duration(
            &self.range_delete_scan_us,
            &self.range_delete_scan_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_range_delete_chunks(&self, chunks: usize) {
        self.range_delete_chunks
            .fetch_add(chunks as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_range_delete_wal(&self, elapsed: Duration) {
        record_duration(
            &self.range_delete_wal_us,
            &self.range_delete_wal_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_range_delete_apply_wait(&self, elapsed: Duration) {
        record_duration(
            &self.range_delete_apply_wait_us,
            &self.range_delete_apply_wait_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_range_delete_apply(&self, elapsed: Duration) {
        record_duration(
            &self.range_delete_apply_us,
            &self.range_delete_apply_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_range_delete_drop_gate_wait(&self, elapsed: Duration) {
        record_duration(
            &self.range_delete_drop_gate_wait_us,
            &self.range_delete_drop_gate_wait_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_range_delete_apply_gate_wait(&self, elapsed: Duration) {
        record_duration(
            &self.range_delete_apply_gate_wait_us,
            &self.range_delete_apply_gate_wait_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_cleanup_call(&self, pbas: usize) {
        self.cleanup_calls.fetch_add(1, Ordering::Relaxed);
        self.cleanup_pbas.fetch_add(pbas as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_cleanup_success(&self, total: Duration) {
        self.cleanup_success.fetch_add(1, Ordering::Relaxed);
        record_duration(&self.cleanup_total_us, &self.cleanup_total_max_us, total);
    }

    pub(crate) fn record_cleanup_error(&self, total: Duration) {
        self.cleanup_errors.fetch_add(1, Ordering::Relaxed);
        record_duration(&self.cleanup_total_us, &self.cleanup_total_max_us, total);
    }

    pub(crate) fn record_cleanup_noop(&self) {
        self.cleanup_noop.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_cleanup_scan(&self, elapsed: Duration, hashes_found: usize) {
        self.cleanup_hashes_found
            .fetch_add(hashes_found as u64, Ordering::Relaxed);
        record_duration(&self.cleanup_scan_us, &self.cleanup_scan_max_us, elapsed);
    }

    pub(crate) fn record_cleanup_forward_checks(&self, elapsed: Duration, checks: usize) {
        if checks == 0 {
            return;
        }
        self.cleanup_forward_checks
            .fetch_add(checks as u64, Ordering::Relaxed);
        record_duration(
            &self.cleanup_forward_check_us,
            &self.cleanup_forward_check_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_cleanup_tombstones(&self, forward_tombstones: usize, tx_ops: usize) {
        self.cleanup_tombstones_emitted
            .fetch_add(forward_tombstones as u64, Ordering::Relaxed);
        self.cleanup_tx_ops
            .fetch_add(tx_ops as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_cleanup_commit(&self, elapsed: Duration) {
        record_duration(
            &self.cleanup_commit_us,
            &self.cleanup_commit_max_us,
            elapsed,
        );
    }
}

impl MetaMetricsSnapshot {
    pub fn to_json(&self) -> String {
        format!(
            concat!(
                "{{",
                "\"commit_attempts\":{},",
                "\"commit_success\":{},",
                "\"commit_errors\":{},",
                "\"commit_empty\":{},",
                "\"commit_ops\":{},",
                "\"commit_ops_per_tx_max\":{},",
                "\"commit_wal_body_bytes\":{},",
                "\"commit_wal_body_bytes_max\":{},",
                "\"commit_total_us\":{},",
                "\"commit_total_max_us\":{},",
                "\"commit_wal_submit_us\":{},",
                "\"commit_wal_submit_max_us\":{},",
                "\"commit_drop_gate_wait_us\":{},",
                "\"commit_drop_gate_wait_max_us\":{},",
                "\"commit_apply_wait_us\":{},",
                "\"commit_apply_wait_max_us\":{},",
                "\"commit_apply_gate_wait_us\":{},",
                "\"commit_apply_gate_wait_max_us\":{},",
                "\"commit_finish_global_wait_us\":{},",
                "\"commit_finish_global_wait_max_us\":{},",
                "\"commit_apply_us\":{},",
                "\"commit_apply_max_us\":{},",
                "\"commit_apply_l2p_wait_us\":{},",
                "\"commit_apply_l2p_wait_max_us\":{},",
                "\"commit_apply_rc_enqueue_us\":{},",
                "\"commit_apply_rc_enqueue_max_us\":{},",
                "\"commit_apply_rc_wait_us\":{},",
                "\"commit_apply_rc_wait_max_us\":{},",
                "\"commit_apply_dedup_enqueue_us\":{},",
                "\"commit_apply_dedup_enqueue_max_us\":{},",
                "\"commit_apply_dedup_wait_us\":{},",
                "\"commit_apply_dedup_wait_max_us\":{},",
                "\"wal_submit_calls\":{},",
                "\"wal_submit_wait_us\":{},",
                "\"wal_submit_wait_max_us\":{},",
                "\"wal_batches\":{},",
                "\"wal_records\":{},",
                "\"wal_bytes\":{},",
                "\"wal_rotates\":{},",
                "\"wal_write_us\":{},",
                "\"wal_write_max_us\":{},",
                "\"wal_fsyncs\":{},",
                "\"wal_fsync_us\":{},",
                "\"wal_fsync_max_us\":{},",
                "\"wal_batch_records_max\":{},",
                "\"wal_batch_bytes_max\":{},",
                "\"range_delete_calls\":{},",
                "\"range_delete_success\":{},",
                "\"range_delete_errors\":{},",
                "\"range_delete_noop\":{},",
                "\"range_delete_captured_entries\":{},",
                "\"range_delete_chunks\":{},",
                "\"range_delete_total_us\":{},",
                "\"range_delete_total_max_us\":{},",
                "\"range_delete_scan_us\":{},",
                "\"range_delete_scan_max_us\":{},",
                "\"range_delete_wal_us\":{},",
                "\"range_delete_wal_max_us\":{},",
                "\"range_delete_apply_wait_us\":{},",
                "\"range_delete_apply_wait_max_us\":{},",
                "\"range_delete_apply_us\":{},",
                "\"range_delete_apply_max_us\":{},",
                "\"range_delete_drop_gate_wait_us\":{},",
                "\"range_delete_drop_gate_wait_max_us\":{},",
                "\"range_delete_apply_gate_wait_us\":{},",
                "\"range_delete_apply_gate_wait_max_us\":{},",
                "\"cleanup_calls\":{},",
                "\"cleanup_success\":{},",
                "\"cleanup_errors\":{},",
                "\"cleanup_noop\":{},",
                "\"cleanup_pbas\":{},",
                "\"cleanup_hashes_found\":{},",
                "\"cleanup_forward_checks\":{},",
                "\"cleanup_tombstones_emitted\":{},",
                "\"cleanup_tx_ops\":{},",
                "\"cleanup_total_us\":{},",
                "\"cleanup_total_max_us\":{},",
                "\"cleanup_scan_us\":{},",
                "\"cleanup_scan_max_us\":{},",
                "\"cleanup_forward_check_us\":{},",
                "\"cleanup_forward_check_max_us\":{},",
                "\"cleanup_commit_us\":{},",
                "\"cleanup_commit_max_us\":{},",
                "\"apply_l2p_put_count\":{},",
                "\"apply_l2p_put_us\":{},",
                "\"apply_l2p_put_max_us\":{},",
                "\"apply_l2p_delete_count\":{},",
                "\"apply_l2p_delete_us\":{},",
                "\"apply_l2p_delete_max_us\":{},",
                "\"apply_l2p_remap_count\":{},",
                "\"apply_l2p_remap_us\":{},",
                "\"apply_l2p_remap_max_us\":{},",
                "\"apply_l2p_range_delete_count\":{},",
                "\"apply_l2p_range_delete_us\":{},",
                "\"apply_l2p_range_delete_max_us\":{},",
                "\"apply_l2p_bucket_count\":{},",
                "\"apply_l2p_bucket_ops\":{},",
                "\"apply_l2p_bucket_total_us\":{},",
                "\"apply_l2p_bucket_total_max_us\":{},",
                "\"apply_l2p_bucket_tree_lock_wait_us\":{},",
                "\"apply_l2p_bucket_tree_lock_wait_max_us\":{},",
                "\"apply_l2p_bucket_read_view_prepare_us\":{},",
                "\"apply_l2p_bucket_read_view_prepare_max_us\":{},",
                "\"apply_l2p_bucket_ops_us\":{},",
                "\"apply_l2p_bucket_ops_max_us\":{},",
                "\"apply_l2p_bucket_finish_us\":{},",
                "\"apply_l2p_bucket_finish_max_us\":{},",
                "\"apply_l2p_bucket_publish_us\":{},",
                "\"apply_l2p_bucket_publish_max_us\":{},",
                "\"apply_refcount_count\":{},",
                "\"apply_refcount_us\":{},",
                "\"apply_refcount_max_us\":{},",
                "\"apply_dedup_count\":{},",
                "\"apply_dedup_us\":{},",
                "\"apply_dedup_max_us\":{},",
                "\"dedup_lane_tasks\":{},",
                "\"dedup_lane_ops\":{},",
                "\"dedup_lane_ready_queue_wait_us\":{},",
                "\"dedup_lane_ready_queue_wait_max_us\":{},",
                "\"dedup_lane_exec_us\":{},",
                "\"dedup_lane_exec_max_us\":{},",
                "\"dedup_lane_idle_us\":{},",
                "\"dedup_lane_idle_max_us\":{},",
                "\"dedup_lane_wakeups\":{},",
                "\"dedup_lane_empty_wakeups\":{},",
                "\"dedup_lane_burst_total\":{},",
                "\"dedup_lane_burst_max\":{},",
                "\"dedup_lane_queue_depth_max\":{},",
                "\"l2p_apply_lane_tasks\":{},",
                "\"l2p_apply_lane_queue_depth_max\":{},",
                "\"l2p_apply_lane_queue_wait_us\":{},",
                "\"l2p_apply_lane_queue_wait_max_us\":{},",
                "\"l2p_apply_lane_exec_us\":{},",
                "\"l2p_apply_lane_exec_max_us\":{},",
                "\"l2p_apply_lane_idle_us\":{},",
                "\"l2p_apply_lane_idle_max_us\":{},",
                "\"l2p_apply_lane_wakeups\":{},",
                "\"l2p_apply_lane_empty_wakeups\":{},",
                "\"l2p_apply_lane_burst_total\":{},",
                "\"l2p_apply_lane_burst_max\":{},",
                "\"rc_apply_lane_tasks\":{},",
                "\"rc_apply_lane_queue_depth_max\":{},",
                "\"rc_apply_lane_queue_wait_us\":{},",
                "\"rc_apply_lane_queue_wait_max_us\":{},",
                "\"rc_apply_lane_exec_us\":{},",
                "\"rc_apply_lane_exec_max_us\":{},",
                "\"rc_apply_lane_idle_us\":{},",
                "\"rc_apply_lane_idle_max_us\":{},",
                "\"rc_apply_lane_pending_set_wait_us\":{},",
                "\"rc_apply_lane_pending_set_wait_max_us\":{},",
                "\"rc_apply_lane_wakeups\":{},",
                "\"rc_apply_lane_empty_wakeups\":{},",
                "\"rc_apply_lane_burst_total\":{},",
                "\"rc_apply_lane_burst_max\":{},",
                "\"dedup_apply_guard_count\":{},",
                "\"dedup_apply_guard_us\":{},",
                "\"dedup_apply_guard_max_us\":{},",
                "\"dedup_apply_forward_put_count\":{},",
                "\"dedup_apply_forward_put_us\":{},",
                "\"dedup_apply_forward_put_max_us\":{},",
                "\"dedup_apply_forward_delete_count\":{},",
                "\"dedup_apply_forward_delete_us\":{},",
                "\"dedup_apply_forward_delete_max_us\":{},",
                "\"dedup_apply_reverse_put_count\":{},",
                "\"dedup_apply_reverse_put_us\":{},",
                "\"dedup_apply_reverse_put_max_us\":{},",
                "\"dedup_apply_reverse_delete_count\":{},",
                "\"dedup_apply_reverse_delete_us\":{},",
                "\"dedup_apply_reverse_delete_max_us\":{},",
                "\"dedup_put_l0_insert_us\":{},",
                "\"dedup_put_l0_insert_max_us\":{},",
                "\"dedup_put_l1_put_us\":{},",
                "\"dedup_put_l1_put_max_us\":{},",
                "\"dedup_put_cuckoo_update_existing_us\":{},",
                "\"dedup_put_cuckoo_update_existing_max_us\":{},",
                "\"dedup_put_cuckoo_free_slots_us\":{},",
                "\"dedup_put_cuckoo_free_slots_max_us\":{},",
                "\"dedup_put_cuckoo_try_insert_empty_us\":{},",
                "\"dedup_put_cuckoo_try_insert_empty_max_us\":{},",
                "\"dedup_put_cuckoo_evict_and_insert_us\":{},",
                "\"dedup_put_cuckoo_evict_and_insert_max_us\":{},",
                "\"dedup_put_cuckoo_page_read_cache_wait_us\":{},",
                "\"dedup_put_cuckoo_page_read_cache_wait_max_us\":{},",
                "\"dedup_put_cuckoo_page_alloc_us\":{},",
                "\"dedup_put_cuckoo_page_alloc_max_us\":{},",
                "\"dedup_put_cuckoo_page_write_publish_us\":{},",
                "\"dedup_put_cuckoo_page_write_publish_max_us\":{},",
                "\"dedup_put_cuckoo_bucket_lock_wait_us\":{},",
                "\"dedup_put_cuckoo_bucket_lock_wait_max_us\":{},",
                "\"l2p_get_calls\":{},",
                "\"l2p_get_lock_wait_us\":{},",
                "\"l2p_get_lock_wait_max_us\":{},",
                "\"l2p_get_tree_walk_us\":{},",
                "\"l2p_get_tree_walk_max_us\":{},",
                "\"l2p_multi_get_calls\":{},",
                "\"l2p_multi_get_lbas\":{},",
                "\"l2p_multi_get_pin_us\":{},",
                "\"l2p_multi_get_pin_max_us\":{},",
                "\"l2p_multi_get_volume_us\":{},",
                "\"l2p_multi_get_volume_max_us\":{},",
                "\"l2p_multi_get_sort_us\":{},",
                "\"l2p_multi_get_sort_max_us\":{},",
                "\"l2p_multi_get_view_us\":{},",
                "\"l2p_multi_get_view_max_us\":{},",
                "\"l2p_multi_get_tree_us\":{},",
                "\"l2p_multi_get_tree_max_us\":{},",
                "\"flush_calls\":{},",
                "\"flush_calls_steady\":{},",
                "\"flush_calls_forced\":{},",
                "\"flush_total_us\":{},",
                "\"flush_total_max_us\":{},",
                "\"flush_total_us_steady\":{},",
                "\"flush_total_max_us_steady\":{},",
                "\"flush_total_us_forced\":{},",
                "\"flush_total_max_us_forced\":{},",
                "\"flush_gate_wait_us\":{},",
                "\"flush_gate_wait_max_us\":{},",
                "\"flush_sample_us\":{},",
                "\"flush_sample_max_us\":{},",
                "\"flush_sample_us_steady\":{},",
                "\"flush_sample_max_us_steady\":{},",
                "\"flush_sample_us_forced\":{},",
                "\"flush_sample_max_us_forced\":{},",
                "\"flush_io_us\":{},",
                "\"flush_io_max_us\":{},",
                "\"flush_io_seal_us\":{},",
                "\"flush_io_seal_max_us\":{},",
                "\"flush_io_page_write_us\":{},",
                "\"flush_io_page_write_max_us\":{},",
                "\"flush_io_rc_meta_us\":{},",
                "\"flush_io_rc_meta_max_us\":{},",
                "\"flush_io_sync_us\":{},",
                "\"flush_io_sync_max_us\":{},",
                "\"flush_manifest_us\":{},",
                "\"flush_manifest_max_us\":{},",
                "\"flush_install_us\":{},",
                "\"flush_install_max_us\":{},",
                "\"flush_reclaim_us\":{},",
                "\"flush_reclaim_max_us\":{},",
                "\"flush_pages_written\":{},",
                "\"flush_reclaim_budget_pages\":{},",
                "\"flush_reclaim_selected_pages\":{},",
                "\"flush_reclaim_reclaimed_pages\":{},",
                "\"flush_reclaim_blocked_pages\":{},",
                "\"flush_sample_l2p_dirty_pages\":{},",
                "\"flush_sample_l2p_dirty_pages_max\":{},",
                "\"flush_sample_rc_drained_deltas\":{},",
                "\"flush_sample_rc_drained_deltas_max\":{},",
                "\"flush_sample_rc_fresh_pages\":{},",
                "\"flush_sample_rc_fresh_pages_max\":{},",
                "\"rc_drainer_cycles\":{},",
                "\"rc_drainer_drained_entries\":{},",
                "\"rc_drainer_pages_built\":{},",
                "\"rc_drainer_cycle_us\":{},",
                "\"rc_drainer_cycle_max_us\":{},",
                "\"rc_drainer_overlay_size_max_pages\":{},",
                "\"rc_drainer_checkpoint_wait_us\":{},",
                "\"rc_drainer_checkpoint_wait_max_us\":{},",
                "\"rc_drainer_backpressure_fallbacks\":{},",
                "\"rc_drainer_pool_refills\":{},",
                "\"flush_io_bytes_total\":{},",
                "\"meta_io_write_calls\":{},",
                "\"meta_io_write_ops\":{},",
                "\"meta_io_write_bytes\":{},",
                "\"meta_io_write_us\":{},",
                "\"meta_io_write_max_us\":{},",
                "\"meta_io_write_batch_ops_max\":{},",
                "\"meta_io_write_batch_bytes_max\":{},",
                "\"meta_io_read_calls\":{},",
                "\"meta_io_read_ops\":{},",
                "\"meta_io_read_bytes\":{},",
                "\"meta_io_read_us\":{},",
                "\"meta_io_read_max_us\":{},",
                "\"meta_io_read_batch_ops_max\":{},",
                "\"meta_io_fsync_calls\":{},",
                "\"meta_io_fsync_us\":{},",
                "\"meta_io_fsync_max_us\":{},",
                "\"meta_io_write_uring_lock_acquires\":{},",
                "\"meta_io_write_uring_lock_wait_us\":{},",
                "\"meta_io_write_uring_lock_wait_max_us\":{},",
                "\"io_submitter_iterations\":{},",
                "\"io_submitter_sqes_submitted\":{},",
                "\"io_submitter_channel_pending_max\":{},",
                "\"io_submitter_submit_batch_size_max\":{},",
                "\"io_submitter_inflight_max\":{},",
                "\"l2p_writeback_cycles\":{},",
                "\"l2p_writeback_cycles_with_work\":{},",
                "\"l2p_writeback_shards_flushed\":{},",
                "\"l2p_writeback_pages_promoted\":{},",
                "\"l2p_writeback_pages_kept_dirty\":{},",
                "\"l2p_writeback_pages_written\":{},",
                "\"l2p_writeback_io_bytes_total\":{},",
                "\"l2p_writeback_seal_us\":{},",
                "\"l2p_writeback_seal_max_us\":{},",
                "\"l2p_writeback_io_us\":{},",
                "\"l2p_writeback_io_max_us\":{},",
                "\"l2p_writeback_install_us\":{},",
                "\"l2p_writeback_install_max_us\":{},",
                "\"l2p_writeback_errors\":{}",
                "}}"
            ),
            self.commit_attempts,
            self.commit_success,
            self.commit_errors,
            self.commit_empty,
            self.commit_ops,
            self.commit_ops_per_tx_max,
            self.commit_wal_body_bytes,
            self.commit_wal_body_bytes_max,
            self.commit_total_us,
            self.commit_total_max_us,
            self.commit_wal_submit_us,
            self.commit_wal_submit_max_us,
            self.commit_drop_gate_wait_us,
            self.commit_drop_gate_wait_max_us,
            self.commit_apply_wait_us,
            self.commit_apply_wait_max_us,
            self.commit_apply_gate_wait_us,
            self.commit_apply_gate_wait_max_us,
            self.commit_finish_global_wait_us,
            self.commit_finish_global_wait_max_us,
            self.commit_apply_us,
            self.commit_apply_max_us,
            self.commit_apply_l2p_wait_us,
            self.commit_apply_l2p_wait_max_us,
            self.commit_apply_rc_enqueue_us,
            self.commit_apply_rc_enqueue_max_us,
            self.commit_apply_rc_wait_us,
            self.commit_apply_rc_wait_max_us,
            self.commit_apply_dedup_enqueue_us,
            self.commit_apply_dedup_enqueue_max_us,
            self.commit_apply_dedup_wait_us,
            self.commit_apply_dedup_wait_max_us,
            self.wal_submit_calls,
            self.wal_submit_wait_us,
            self.wal_submit_wait_max_us,
            self.wal_batches,
            self.wal_records,
            self.wal_bytes,
            self.wal_rotates,
            self.wal_write_us,
            self.wal_write_max_us,
            self.wal_fsyncs,
            self.wal_fsync_us,
            self.wal_fsync_max_us,
            self.wal_batch_records_max,
            self.wal_batch_bytes_max,
            self.range_delete_calls,
            self.range_delete_success,
            self.range_delete_errors,
            self.range_delete_noop,
            self.range_delete_captured_entries,
            self.range_delete_chunks,
            self.range_delete_total_us,
            self.range_delete_total_max_us,
            self.range_delete_scan_us,
            self.range_delete_scan_max_us,
            self.range_delete_wal_us,
            self.range_delete_wal_max_us,
            self.range_delete_apply_wait_us,
            self.range_delete_apply_wait_max_us,
            self.range_delete_apply_us,
            self.range_delete_apply_max_us,
            self.range_delete_drop_gate_wait_us,
            self.range_delete_drop_gate_wait_max_us,
            self.range_delete_apply_gate_wait_us,
            self.range_delete_apply_gate_wait_max_us,
            self.cleanup_calls,
            self.cleanup_success,
            self.cleanup_errors,
            self.cleanup_noop,
            self.cleanup_pbas,
            self.cleanup_hashes_found,
            self.cleanup_forward_checks,
            self.cleanup_tombstones_emitted,
            self.cleanup_tx_ops,
            self.cleanup_total_us,
            self.cleanup_total_max_us,
            self.cleanup_scan_us,
            self.cleanup_scan_max_us,
            self.cleanup_forward_check_us,
            self.cleanup_forward_check_max_us,
            self.cleanup_commit_us,
            self.cleanup_commit_max_us,
            self.apply_l2p_put_count,
            self.apply_l2p_put_us,
            self.apply_l2p_put_max_us,
            self.apply_l2p_delete_count,
            self.apply_l2p_delete_us,
            self.apply_l2p_delete_max_us,
            self.apply_l2p_remap_count,
            self.apply_l2p_remap_us,
            self.apply_l2p_remap_max_us,
            self.apply_l2p_range_delete_count,
            self.apply_l2p_range_delete_us,
            self.apply_l2p_range_delete_max_us,
            self.apply_l2p_bucket_count,
            self.apply_l2p_bucket_ops,
            self.apply_l2p_bucket_total_us,
            self.apply_l2p_bucket_total_max_us,
            self.apply_l2p_bucket_tree_lock_wait_us,
            self.apply_l2p_bucket_tree_lock_wait_max_us,
            self.apply_l2p_bucket_read_view_prepare_us,
            self.apply_l2p_bucket_read_view_prepare_max_us,
            self.apply_l2p_bucket_ops_us,
            self.apply_l2p_bucket_ops_max_us,
            self.apply_l2p_bucket_finish_us,
            self.apply_l2p_bucket_finish_max_us,
            self.apply_l2p_bucket_publish_us,
            self.apply_l2p_bucket_publish_max_us,
            self.apply_refcount_count,
            self.apply_refcount_us,
            self.apply_refcount_max_us,
            self.apply_dedup_count,
            self.apply_dedup_us,
            self.apply_dedup_max_us,
            self.dedup_lane_tasks,
            self.dedup_lane_ops,
            self.dedup_lane_ready_queue_wait_us,
            self.dedup_lane_ready_queue_wait_max_us,
            self.dedup_lane_exec_us,
            self.dedup_lane_exec_max_us,
            self.dedup_lane_idle_us,
            self.dedup_lane_idle_max_us,
            self.dedup_lane_wakeups,
            self.dedup_lane_empty_wakeups,
            self.dedup_lane_burst_total,
            self.dedup_lane_burst_max,
            self.dedup_lane_queue_depth_max,
            self.l2p_apply_lane_tasks,
            self.l2p_apply_lane_queue_depth_max,
            self.l2p_apply_lane_queue_wait_us,
            self.l2p_apply_lane_queue_wait_max_us,
            self.l2p_apply_lane_exec_us,
            self.l2p_apply_lane_exec_max_us,
            self.l2p_apply_lane_idle_us,
            self.l2p_apply_lane_idle_max_us,
            self.l2p_apply_lane_wakeups,
            self.l2p_apply_lane_empty_wakeups,
            self.l2p_apply_lane_burst_total,
            self.l2p_apply_lane_burst_max,
            self.rc_apply_lane_tasks,
            self.rc_apply_lane_queue_depth_max,
            self.rc_apply_lane_queue_wait_us,
            self.rc_apply_lane_queue_wait_max_us,
            self.rc_apply_lane_exec_us,
            self.rc_apply_lane_exec_max_us,
            self.rc_apply_lane_idle_us,
            self.rc_apply_lane_idle_max_us,
            self.rc_apply_lane_pending_set_wait_us,
            self.rc_apply_lane_pending_set_wait_max_us,
            self.rc_apply_lane_wakeups,
            self.rc_apply_lane_empty_wakeups,
            self.rc_apply_lane_burst_total,
            self.rc_apply_lane_burst_max,
            self.dedup_apply_guard_count,
            self.dedup_apply_guard_us,
            self.dedup_apply_guard_max_us,
            self.dedup_apply_forward_put_count,
            self.dedup_apply_forward_put_us,
            self.dedup_apply_forward_put_max_us,
            self.dedup_apply_forward_delete_count,
            self.dedup_apply_forward_delete_us,
            self.dedup_apply_forward_delete_max_us,
            self.dedup_apply_reverse_put_count,
            self.dedup_apply_reverse_put_us,
            self.dedup_apply_reverse_put_max_us,
            self.dedup_apply_reverse_delete_count,
            self.dedup_apply_reverse_delete_us,
            self.dedup_apply_reverse_delete_max_us,
            self.dedup_put_l0_insert_us,
            self.dedup_put_l0_insert_max_us,
            self.dedup_put_l1_put_us,
            self.dedup_put_l1_put_max_us,
            self.dedup_put_cuckoo_update_existing_us,
            self.dedup_put_cuckoo_update_existing_max_us,
            self.dedup_put_cuckoo_free_slots_us,
            self.dedup_put_cuckoo_free_slots_max_us,
            self.dedup_put_cuckoo_try_insert_empty_us,
            self.dedup_put_cuckoo_try_insert_empty_max_us,
            self.dedup_put_cuckoo_evict_and_insert_us,
            self.dedup_put_cuckoo_evict_and_insert_max_us,
            self.dedup_put_cuckoo_page_read_cache_wait_us,
            self.dedup_put_cuckoo_page_read_cache_wait_max_us,
            self.dedup_put_cuckoo_page_alloc_us,
            self.dedup_put_cuckoo_page_alloc_max_us,
            self.dedup_put_cuckoo_page_write_publish_us,
            self.dedup_put_cuckoo_page_write_publish_max_us,
            self.dedup_put_cuckoo_bucket_lock_wait_us,
            self.dedup_put_cuckoo_bucket_lock_wait_max_us,
            self.l2p_get_calls,
            self.l2p_get_lock_wait_us,
            self.l2p_get_lock_wait_max_us,
            self.l2p_get_tree_walk_us,
            self.l2p_get_tree_walk_max_us,
            self.l2p_multi_get_calls,
            self.l2p_multi_get_lbas,
            self.l2p_multi_get_pin_us,
            self.l2p_multi_get_pin_max_us,
            self.l2p_multi_get_volume_us,
            self.l2p_multi_get_volume_max_us,
            self.l2p_multi_get_sort_us,
            self.l2p_multi_get_sort_max_us,
            self.l2p_multi_get_view_us,
            self.l2p_multi_get_view_max_us,
            self.l2p_multi_get_tree_us,
            self.l2p_multi_get_tree_max_us,
            self.flush_calls,
            self.flush_calls_steady,
            self.flush_calls_forced,
            self.flush_total_us,
            self.flush_total_max_us,
            self.flush_total_us_steady,
            self.flush_total_max_us_steady,
            self.flush_total_us_forced,
            self.flush_total_max_us_forced,
            self.flush_gate_wait_us,
            self.flush_gate_wait_max_us,
            self.flush_sample_us,
            self.flush_sample_max_us,
            self.flush_sample_us_steady,
            self.flush_sample_max_us_steady,
            self.flush_sample_us_forced,
            self.flush_sample_max_us_forced,
            self.flush_io_us,
            self.flush_io_max_us,
            self.flush_io_seal_us,
            self.flush_io_seal_max_us,
            self.flush_io_page_write_us,
            self.flush_io_page_write_max_us,
            self.flush_io_rc_meta_us,
            self.flush_io_rc_meta_max_us,
            self.flush_io_sync_us,
            self.flush_io_sync_max_us,
            self.flush_manifest_us,
            self.flush_manifest_max_us,
            self.flush_install_us,
            self.flush_install_max_us,
            self.flush_reclaim_us,
            self.flush_reclaim_max_us,
            self.flush_pages_written,
            self.flush_reclaim_budget_pages,
            self.flush_reclaim_selected_pages,
            self.flush_reclaim_reclaimed_pages,
            self.flush_reclaim_blocked_pages,
            self.flush_sample_l2p_dirty_pages,
            self.flush_sample_l2p_dirty_pages_max,
            self.flush_sample_rc_drained_deltas,
            self.flush_sample_rc_drained_deltas_max,
            self.flush_sample_rc_fresh_pages,
            self.flush_sample_rc_fresh_pages_max,
            self.rc_drainer_cycles,
            self.rc_drainer_drained_entries,
            self.rc_drainer_pages_built,
            self.rc_drainer_cycle_us,
            self.rc_drainer_cycle_max_us,
            self.rc_drainer_overlay_size_max_pages,
            self.rc_drainer_checkpoint_wait_us,
            self.rc_drainer_checkpoint_wait_max_us,
            self.rc_drainer_backpressure_fallbacks,
            self.rc_drainer_pool_refills,
            self.flush_io_bytes_total,
            self.meta_io_write_calls,
            self.meta_io_write_ops,
            self.meta_io_write_bytes,
            self.meta_io_write_us,
            self.meta_io_write_max_us,
            self.meta_io_write_batch_ops_max,
            self.meta_io_write_batch_bytes_max,
            self.meta_io_read_calls,
            self.meta_io_read_ops,
            self.meta_io_read_bytes,
            self.meta_io_read_us,
            self.meta_io_read_max_us,
            self.meta_io_read_batch_ops_max,
            self.meta_io_fsync_calls,
            self.meta_io_fsync_us,
            self.meta_io_fsync_max_us,
            self.meta_io_write_uring_lock_acquires,
            self.meta_io_write_uring_lock_wait_us,
            self.meta_io_write_uring_lock_wait_max_us,
            self.io_submitter_iterations,
            self.io_submitter_sqes_submitted,
            self.io_submitter_channel_pending_max,
            self.io_submitter_submit_batch_size_max,
            self.io_submitter_inflight_max,
            self.l2p_writeback_cycles,
            self.l2p_writeback_cycles_with_work,
            self.l2p_writeback_shards_flushed,
            self.l2p_writeback_pages_promoted,
            self.l2p_writeback_pages_kept_dirty,
            self.l2p_writeback_pages_written,
            self.l2p_writeback_io_bytes_total,
            self.l2p_writeback_seal_us,
            self.l2p_writeback_seal_max_us,
            self.l2p_writeback_io_us,
            self.l2p_writeback_io_max_us,
            self.l2p_writeback_install_us,
            self.l2p_writeback_install_max_us,
            self.l2p_writeback_errors,
        )
    }
}

fn load(value: &AtomicU64) -> u64 {
    value.load(Ordering::Relaxed)
}

fn load_shards(values: &PerShardCounters) -> Vec<u64> {
    values.iter().map(load).collect()
}

fn record_duration(total: &AtomicU64, max: &AtomicU64, elapsed: Duration) {
    let us = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
    total.fetch_add(us, Ordering::Relaxed);
    fetch_max(max, us);
}

fn record_batch_duration(total: &AtomicU64, max: &AtomicU64, count: u64, elapsed: Duration) {
    let us = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
    total.fetch_add(us, Ordering::Relaxed);
    fetch_max(max, us / count.max(1));
}

fn fetch_max(slot: &AtomicU64, value: u64) {
    let mut current = slot.load(Ordering::Relaxed);
    while value > current {
        match slot.compare_exchange_weak(current, value, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(next) => current = next,
        }
    }
}
