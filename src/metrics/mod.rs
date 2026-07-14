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
    // Explicit backpressure at the per-BFG L2P work bound. Kept separate
    // from WAL/apply waits so a smaller checkpoint cohort cannot hide a new
    // foreground admission stall inside `commit_total_us`.
    commit_bfg_admission_wait_us: AtomicU64,
    commit_bfg_admission_wait_max_us: AtomicU64,
    commit_apply_wait_us: AtomicU64,
    commit_apply_wait_max_us: AtomicU64,
    commit_apply_gate_wait_us: AtomicU64,
    commit_apply_gate_wait_max_us: AtomicU64,
    commit_apply_us: AtomicU64,
    commit_apply_max_us: AtomicU64,

    // BFG:
    // counts and timings for the L2P-only fast path that skips
    // `enqueue_lane_plan` + `apply_ops_laned`. `commit_direct_apply_count`
    // = number of commits that took the fast path;
    // `commit_direct_apply_us` = cumulative wall time spent in
    // `apply_l2p_direct` for those commits (subset of
    // `commit_apply_us`). When this counter is rising and
    // `commit_apply_l2p_wait_us` stays flat, the fast path is
    // carrying the bulk of traffic.
    commit_direct_apply_count: AtomicU64,
    commit_direct_apply_us: AtomicU64,
    commit_direct_apply_max_us: AtomicU64,

    // BFG. Each commit that
    // routes through `commit_ops_deferred` parks its `Vec<ApplyOutcome>`
    // in the [`DeferredOutcomeAggregator`] keyed by LSN. The L2P
    // compactor's step-6 drain releases each entry once every
    // `(volume, shard)` it touched has been folded into the on-disk
    // tree. `commit_deferred_outcomes_count` counts entries staged,
    // `commit_deferred_outcomes_released_total` counts entries the
    // compactor has released, `commit_deferred_outcomes_pending` is a
    // gauge written on every stage / drain (last value wins —
    // intentional, since pending depth is a steady-state property).
    commit_deferred_outcomes_count: AtomicU64,
    commit_deferred_outcomes_released_total: AtomicU64,
    commit_deferred_outcomes_pending: AtomicU64,
    commit_deferred_outcomes_pending_max: AtomicU64,

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

    // lifecycle journal diagnostic counters — previously only surfaced in slow-commit
    // WARN logs (>=1s), now aggregated so the avg-case 37 ms gap between
    // `commit_total_us` and the sum of recorded phases is attributable.
    //
    // - `commit_plan_*`: `build_lane_dispatch_plan` (hashmap bucket + sort)
    // - `commit_encode_*`: `try_encode_body` (WAL serialise)
    // - `commit_unlogged_gate_wait_*`: time to acquire `unlogged_commit_gate`
    //   (read for unlogged path, write for logged path)
    // - `commit_checkpoint_unlogged_*`: time spent inside
    //   `checkpoint_unlogged_before_wal_commit` (logged path only; calls
    //   `flush()` if any unlogged LSN is pending — the suspected smoking
    //   gun behind multi-second logged commits)
    // - `commit_read_held_*`: total wall time a commit holds
    //   `apply_gate.read()` (from `acquire_commit_apply_gate` return to
    //   `drop(apply_guard)`). Single long-held read is what blocks flush's
    //   write acquisition, cascading into the `apply_gate_wait` spike.
    commit_plan_us: AtomicU64,
    commit_plan_max_us: AtomicU64,
    commit_encode_us: AtomicU64,
    commit_encode_max_us: AtomicU64,
    commit_unlogged_gate_wait_us: AtomicU64,
    commit_unlogged_gate_wait_max_us: AtomicU64,
    commit_checkpoint_unlogged_us: AtomicU64,
    commit_checkpoint_unlogged_max_us: AtomicU64,
    commit_read_held_us: AtomicU64,
    commit_read_held_max_us: AtomicU64,

    wal_submit_calls: AtomicU64,
    wal_submit_wait_us: AtomicU64,
    wal_submit_wait_max_us: AtomicU64,
    /// `wal_submit_wait_us` decomposed into three segments. Each
    /// submit contributes one sample to every segment. Sum across
    /// segments approximates `wal_submit_wait_us` modulo a handful of
    /// nanoseconds of timestamp slop.
    /// - queue_wait: caller's `Op::Submit` send → writer dequeue
    /// - writer_busy: writer dequeue → writer ack send
    /// - wake_roundtrip: writer ack send → caller's `recv` return
    wal_queue_wait_us: AtomicU64,
    wal_queue_wait_max_us: AtomicU64,
    wal_writer_busy_us: AtomicU64,
    wal_writer_busy_max_us: AtomicU64,
    wal_wake_roundtrip_us: AtomicU64,
    wal_wake_roundtrip_max_us: AtomicU64,
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

    /// BFG: cumulative count of submits acked
    /// without a per-batch fsync (the writer thread skipped
    /// `seg.sync` because every submit in the batch had
    /// `synchronous=false`). Stays zero when
    /// `Config::wal_async_commits_enabled = false`.
    wal_async_acks_total: AtomicU64,
    /// BFG: cumulative bytes written to WAL
    /// segments via async-only batches (the bytes that landed in OS
    /// page cache but had no inline fsync). Diff against the
    /// post-FsyncAll snapshot to estimate the inflight window. Stays
    /// zero when async WAL is off.
    wal_async_pending_bytes_total: AtomicU64,
    /// BFG: cumulative time spent inside
    /// `WalSet::fsync_all_lanes` (the BFG-sync barrier in
    /// `flush_with_gate`). With sync-only submits this is the cost
    /// of a no-op double-fsync; with async submits it is the only
    /// fsync the batch ever takes.
    wal_fsync_at_bfg_sync_us: AtomicU64,
    wal_fsync_at_bfg_sync_max_us: AtomicU64,

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
    /// Per-range-op stats for `WalOp::L2pRemapRange`. `count` is the
    /// number of range ops applied; `lbas` is the total LBA count
    /// across all of them. `us_per_op = us / count` shows the
    /// commit-side bucket-assembly + WAL-decode amortization; `us_per_lba`
    /// vs `apply_l2p_remap_us / apply_l2p_remap_count` shows the
    /// per-LBA work cost difference (the part still scaling with N).
    apply_l2p_remap_range_count: AtomicU64,
    apply_l2p_remap_range_lbas: AtomicU64,
    apply_l2p_remap_range_us: AtomicU64,
    apply_l2p_remap_range_max_us: AtomicU64,
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
    apply_refcount_batch_count: AtomicU64,
    apply_refcount_batch_actions: AtomicU64,
    apply_refcount_batch_pbas: AtomicU64,
    apply_refcount_breakdown_sampled_pbas: AtomicU64,
    apply_refcount_pba_grouping_us: AtomicU64,
    apply_refcount_pba_grouping_max_us: AtomicU64,
    apply_refcount_base_page_lookup_us: AtomicU64,
    apply_refcount_base_page_lookup_max_us: AtomicU64,
    apply_refcount_base_lookup_attempts: AtomicU64,
    apply_refcount_epoch_retries: AtomicU64,
    apply_refcount_fold_lock_wait_us: AtomicU64,
    apply_refcount_fold_lock_wait_max_us: AtomicU64,
    apply_refcount_slot_lock_wait_us: AtomicU64,
    apply_refcount_slot_lock_wait_max_us: AtomicU64,
    apply_refcount_pending_slot_scan_us: AtomicU64,
    apply_refcount_pending_slot_scan_max_us: AtomicU64,
    apply_refcount_delta_merge_us: AtomicU64,
    apply_refcount_delta_merge_max_us: AtomicU64,
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
    // Dedup promotes DROPPED because the on-disk cuckoo was saturated
    // (`MAX_CUCKOO_CHAIN` exceeded). The apply path degrades a saturated
    // promote to a future dedup miss instead of failing the commit; a
    // rising value means the modulus needs to grow (Step 2 online resize).
    dedup_promote_dropped_saturated: AtomicU64,
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
    // 0 = threads-off/all-slots, 1 = threads-on legacy one-shot,
    // 2 = threads-on bounded streaming. Set once when Db is opened.
    rc_checkpoint_mode: AtomicU64,
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
    flush_gate_hold_us: AtomicU64,
    flush_gate_hold_max_us: AtomicU64,
    flush_dedup_drain_us: AtomicU64,
    flush_dedup_drain_max_us: AtomicU64,
    flush_l2p_fold_us: AtomicU64,
    flush_l2p_fold_max_us: AtomicU64,
    flush_sample_us: AtomicU64,
    flush_sample_max_us: AtomicU64,
    flush_sample_us_steady: AtomicU64,
    flush_sample_max_us_steady: AtomicU64,
    flush_sample_us_forced: AtomicU64,
    flush_sample_max_us_forced: AtomicU64,
    // Sample-phase sub-breakdown (kind-agnostic). These regions run outside
    // `apply_gate.write()`; use them to separate L2P tree-lock acquisition,
    // dirty-index traversal, and refcount delta drain from publish gate time.
    flush_sample_lock_us: AtomicU64,
    flush_sample_lock_max_us: AtomicU64,
    flush_sample_l2p_walk_us: AtomicU64,
    flush_sample_l2p_walk_max_us: AtomicU64,
    flush_sample_rc_drain_us: AtomicU64,
    flush_sample_rc_drain_max_us: AtomicU64,
    // Per-checkpoint sum of each selected RC shard's `fold_lock.write()` wait.
    // The total accumulates those per-cycle sums; max is the largest one-cycle
    // sum. It is nested inside `flush_sample_rc_drain_us` (parallel wall).
    flush_rc_fold_lock_wait_us: AtomicU64,
    flush_rc_fold_lock_wait_max_us: AtomicU64,
    // Sum of per-shard RC fold service after the fold lock is acquired,
    // excluding streaming data-page writes. Compare with
    // `flush_sample_rc_drain_us` (parallel checkpoint wall), the fold-lock wait
    // sum above, and `flush_rc_stream_service_us` (write service).
    flush_rc_fold_service_us: AtomicU64,
    flush_rc_fold_service_max_us: AtomicU64,
    // Data-page IO performed inside bounded streaming RC checkpoint chunks,
    // before the lifecycle's global `flush_io` timer starts.
    flush_rc_stream_calls: AtomicU64,
    flush_rc_stream_pages: AtomicU64,
    flush_rc_stream_service_us: AtomicU64,
    flush_rc_stream_max_chunk_us: AtomicU64,
    flush_rc_stream_max_chunk_pages: AtomicU64,
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
    flush_manifest_stage_us: AtomicU64,
    flush_manifest_stage_max_us: AtomicU64,
    flush_manifest_publish_us: AtomicU64,
    flush_manifest_publish_max_us: AtomicU64,
    flush_manifest_cleanup_us: AtomicU64,
    flush_manifest_cleanup_max_us: AtomicU64,
    flush_publish_barrier_wait_us: AtomicU64,
    flush_publish_barrier_wait_max_us: AtomicU64,
    flush_install_us: AtomicU64,
    flush_install_max_us: AtomicU64,
    flush_reclaim_us: AtomicU64,
    flush_reclaim_max_us: AtomicU64,
    flush_pages_written: AtomicU64,
    flush_reclaim_budget_pages: AtomicU64,
    flush_reclaim_selected_pages: AtomicU64,
    flush_reclaim_reclaimed_pages: AtomicU64,
    flush_reclaim_blocked_pages: AtomicU64,
    // Background reclaim worker (default-on). Same physical work
    // as the old in-line `flush_with_gate` reclaim, just off the
    // critical path.
    async_reclaim_cycles: AtomicU64,
    async_reclaim_selected_pages: AtomicU64,
    async_reclaim_reclaimed_pages: AtomicU64,
    async_reclaim_cycle_us: AtomicU64,
    async_reclaim_cycle_max_us: AtomicU64,
    // Lineage GC head-advance attribution (    // ). `gc_plan_head_advance` evaluates a volume's head dead-list
    // segment and either advances it (every record unpinned + rc==0) or
    // bails the WHOLE segment on the first pinned/rc>0 record. These
    // counters attribute each per-volume plan call so we can tell *why*
    // FreePbas stops being surfaced under sustained overwrite:
    //   - `*_advanced`        : plan returned, head advanced
    //   - `*_dead_pbas`       : dead PBAs surfaced by those advances (cumulative)
    //   - `*_skipped_snap`    : bailed — a record is pinned by an active snapshot
    //   - `*_skipped_descendant`: bailed — a record is pinned by a clone branch point
    //   - `*_skipped_rc`      : bailed — a record's PBA still has refcount > 0
    //   - `*_blocked_rc0_pbas`: on an rc>0 bail, how many OTHER records in the
    //     same segment were rc==0 (freeable but stuck behind the rc>0 sibling
    //     because the bail leaves the whole segment intact). This is the
    //     reclaim debt the whole-segment bail creates under dedup.
    lineage_gc_head_advanced: AtomicU64,
    lineage_gc_head_dead_pbas: AtomicU64,
    lineage_gc_head_skipped_snap: AtomicU64,
    lineage_gc_head_skipped_descendant: AtomicU64,
    lineage_gc_head_skipped_rc: AtomicU64,
    lineage_gc_head_blocked_rc0_pbas: AtomicU64,
    //   - `*_dropped_dedup_shared`: under `lineage_gc_drop_dedup_shared`, an
    //     rc>0 (dedup-membership) record was DROPPED so the head could advance
    //     past it (instead of the whole-segment bail). Reclaim of that PBA is
    //     left to the client's dedup orphan-reclaim path. Rising together with
    //     `*_advanced` = the guarded Option-3 reclaim-lag fix is working.
    lineage_gc_head_dropped_dedup_shared: AtomicU64,
    // B2 L2P buffer (default-off). When enabled, commits insert into
    // an in-memory hashmap and a background compactor periodically
    // folds it into the paged radix tree. These counters are summed
    // across shards.
    l2p_buffer_active_entries: AtomicU64,
    l2p_buffer_compaction_cycles: AtomicU64,
    l2p_buffer_compaction_entries: AtomicU64,
    l2p_buffer_compaction_us: AtomicU64,
    l2p_buffer_compaction_max_us: AtomicU64,
    l2p_buffer_compaction_leaves: AtomicU64,
    l2p_buffer_compaction_chunks: AtomicU64,
    l2p_buffer_compaction_plan_us: AtomicU64,
    l2p_buffer_compaction_plan_max_us: AtomicU64,
    l2p_buffer_compaction_tree_wait_us: AtomicU64,
    l2p_buffer_compaction_tree_wait_max_us: AtomicU64,
    l2p_buffer_compaction_apply_us: AtomicU64,
    l2p_buffer_compaction_apply_max_us: AtomicU64,
    l2p_buffer_compaction_publish_us: AtomicU64,
    l2p_buffer_compaction_publish_max_us: AtomicU64,
    l2p_buffer_compaction_finish_us: AtomicU64,
    l2p_buffer_compaction_finish_max_us: AtomicU64,
    l2p_prefold_attempts: AtomicU64,
    l2p_prefold_completed: AtomicU64,
    l2p_prefold_skipped: AtomicU64,
    l2p_prefold_errors: AtomicU64,
    l2p_prefold_us: AtomicU64,
    l2p_prefold_max_us: AtomicU64,
    l2p_prefold_wait_us: AtomicU64,
    l2p_prefold_wait_max_us: AtomicU64,
    l2p_buffer_lookup_hits: AtomicU64,
    l2p_buffer_lookup_misses_to_tree: AtomicU64,
    l2p_buffer_backpressure_blocks: AtomicU64,
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
    flush_sample_rc_staged_pages: AtomicU64,
    flush_sample_rc_staged_pages_max: AtomicU64,
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
    /// Worker park-loop wake events: how many times the worker exited
    /// the park to attempt a cycle. Compare against `rc_drainer_cycles`:
    /// `wakes > cycles` → worker is alive but most wakes saw empty
    /// `delta_active` (likely preempted by checkpoint between ticks).
    rc_drainer_wakes: AtomicU64,
    /// Worker observed `preempt=true` and entered the preempt branch.
    /// Each checkpoint typically yields exactly one preempt per shard;
    /// large counts → checkpoint cadence dominates the drainer schedule.
    rc_drainer_preempts: AtomicU64,

    // --- async dedup-index drainer (mirrors rc_drainer_*) ---
    dedup_drainer_cycles: AtomicU64,
    dedup_drainer_drained_entries: AtomicU64,
    dedup_drainer_cycle_us: AtomicU64,
    dedup_drainer_cycle_max_us: AtomicU64,
    dedup_drainer_wakes: AtomicU64,
    dedup_drainer_preempts: AtomicU64,
    dedup_drainer_checkpoint_wait_us: AtomicU64,
    dedup_drainer_checkpoint_wait_max_us: AtomicU64,
    /// High-water mark of total staged (active) dedup entries across all
    /// shards — the deferral backlog gauge.
    dedup_drainer_staged_active_max: AtomicU64,
    /// `stage_*` hit the per-shard backpressure threshold and drained
    /// synchronously ().
    dedup_drainer_backpressure_drains: AtomicU64,

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

    // Centralised IoSubmitter telemetry (observability).
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
    /// Background-priority ops the submitter parked in its deferred
    /// queue because `inflight_bg` had reached the configured cap.
    /// Compare against `io_submitter_sqes_submitted` (specifically the
    /// bg subset) to see how often the gate fired.
    io_submitter_bg_deferred: AtomicU64,
    /// Peak `inflight_bg` count observed across all submitter loop
    /// iterations. Approaches the configured cap whenever a writeback
    /// burst saturates the gate.
    io_submitter_bg_inflight_max: AtomicU64,
    /// Peak depth of the deferred-bg queue observed inside the
    /// submitter. A non-zero peak means writeback producers ran ahead
    /// of bg admission for at least one iteration.
    io_submitter_bg_deferred_max: AtomicU64,

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
    /// Cycles the streaming-flush worker skipped because the global
    /// `dirty_pages_target` gate was not crossed. Compare against
    /// `l2p_writeback_cycles` to see how often the target gate kept
    /// the worker quiet vs. how often it actually swept shards.
    l2p_writeback_target_skips: AtomicU64,
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

mod json;
mod recording;
mod snapshot;

pub use snapshot::MetaMetricsSnapshot;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bfg_admission_wait_reaches_snapshot_and_json() {
        let metrics = MetaMetrics::default();
        metrics.record_commit_bfg_admission_wait(Duration::from_micros(17));
        metrics.record_commit_bfg_admission_wait(Duration::from_micros(41));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.commit_bfg_admission_wait_us, 58);
        assert_eq!(snapshot.commit_bfg_admission_wait_max_us, 41);
        let json = snapshot.to_json();
        assert!(json.contains("\"commit_bfg_admission_wait_us\":58"));
        assert!(json.contains("\"commit_bfg_admission_wait_max_us\":41"));
    }

    #[test]
    fn refcount_batch_breakdown_reaches_snapshot_and_json() {
        let metrics = MetaMetrics::default();
        metrics.record_apply_refcount_batch_breakdown(
            11,
            7,
            Duration::from_micros(13),
            Duration::from_micros(17),
            Duration::from_micros(19),
            Duration::from_micros(23),
            Duration::from_micros(29),
            Duration::from_micros(31),
            1,
            0,
            7,
        );
        metrics.record_apply_refcount_batch_breakdown(
            5,
            3,
            Duration::from_micros(101),
            Duration::from_micros(103),
            Duration::from_micros(107),
            Duration::from_micros(109),
            Duration::from_micros(113),
            Duration::from_micros(127),
            3,
            2,
            0,
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.apply_refcount_batch_count, 2);
        assert_eq!(snapshot.apply_refcount_batch_actions, 16);
        assert_eq!(snapshot.apply_refcount_batch_pbas, 10);
        assert_eq!(snapshot.apply_refcount_breakdown_sampled_pbas, 7);
        assert_eq!(snapshot.apply_refcount_pba_grouping_us, 114);
        assert_eq!(snapshot.apply_refcount_pba_grouping_max_us, 101);
        assert_eq!(snapshot.apply_refcount_base_page_lookup_us, 17);
        assert_eq!(snapshot.apply_refcount_base_page_lookup_max_us, 17);
        assert_eq!(snapshot.apply_refcount_base_lookup_attempts, 4);
        assert_eq!(snapshot.apply_refcount_epoch_retries, 2);
        assert_eq!(snapshot.apply_refcount_fold_lock_wait_us, 19);
        assert_eq!(snapshot.apply_refcount_fold_lock_wait_max_us, 19);
        assert_eq!(snapshot.apply_refcount_slot_lock_wait_us, 23);
        assert_eq!(snapshot.apply_refcount_slot_lock_wait_max_us, 23);
        assert_eq!(snapshot.apply_refcount_pending_slot_scan_us, 29);
        assert_eq!(snapshot.apply_refcount_pending_slot_scan_max_us, 29);
        assert_eq!(snapshot.apply_refcount_delta_merge_us, 31);
        let json = snapshot.to_json();
        assert!(json.contains("\"apply_refcount_batch_count\":2"));
        assert!(json.contains("\"apply_refcount_breakdown_sampled_pbas\":7"));
        assert!(json.contains("\"apply_refcount_base_lookup_attempts\":4"));
        assert!(json.contains("\"apply_refcount_epoch_retries\":2"));
        assert!(json.contains("\"apply_refcount_fold_lock_wait_us\":19"));
        assert!(json.contains("\"apply_refcount_slot_lock_wait_us\":23"));
        assert!(json.contains("\"apply_refcount_delta_merge_us\":31"));
    }

    #[test]
    fn flush_rc_streaming_metrics_reach_snapshot_and_json() {
        let metrics = MetaMetrics::default();
        metrics.set_rc_checkpoint_mode(true, true);
        metrics.record_flush_rc_stream(3, 17, 41, 19, 8);
        metrics.record_flush_rc_stream(2, 9, 23, 29, 4);
        metrics.record_flush_rc_fold_lock_wait(11);
        metrics.record_flush_rc_fold_lock_wait(13);
        metrics.record_flush_rc_fold_service(31);
        metrics.record_flush_rc_fold_service(47);
        metrics.record_flush_io_pages(26);
        metrics.record_flush_sample_workload(5, 101, 0, 12);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.rc_checkpoint_mode, 2);
        assert_eq!(snapshot.flush_rc_stream_calls, 5);
        assert_eq!(snapshot.flush_rc_stream_pages, 26);
        assert_eq!(snapshot.flush_rc_stream_service_us, 64);
        assert_eq!(snapshot.flush_rc_stream_max_chunk_us, 29);
        assert_eq!(snapshot.flush_rc_stream_max_chunk_pages, 8);
        assert_eq!(snapshot.flush_rc_fold_lock_wait_us, 24);
        assert_eq!(snapshot.flush_rc_fold_lock_wait_max_us, 13);
        assert_eq!(snapshot.flush_rc_fold_service_us, 78);
        assert_eq!(snapshot.flush_rc_fold_service_max_us, 47);
        assert_eq!(snapshot.flush_pages_written, 26);
        assert_eq!(
            snapshot.flush_io_bytes_total,
            26 * crate::config::PAGE_SIZE as u64
        );
        assert_eq!(snapshot.flush_sample_rc_staged_pages, 12);
        assert_eq!(snapshot.flush_sample_rc_staged_pages_max, 12);

        let json = snapshot.to_json();
        assert!(json.contains("\"rc_checkpoint_mode\":2"));
        assert!(json.contains("\"flush_rc_stream_calls\":5"));
        assert!(json.contains("\"flush_rc_stream_pages\":26"));
        assert!(json.contains("\"flush_rc_stream_service_us\":64"));
        assert!(json.contains("\"flush_rc_stream_max_chunk_us\":29"));
        assert!(json.contains("\"flush_rc_stream_max_chunk_pages\":8"));
        assert!(json.contains("\"flush_rc_fold_lock_wait_us\":24"));
        assert!(json.contains("\"flush_rc_fold_lock_wait_max_us\":13"));
        assert!(json.contains("\"flush_rc_fold_service_us\":78"));
        assert!(json.contains("\"flush_rc_fold_service_max_us\":47"));
        assert!(json.contains("\"flush_sample_rc_staged_pages\":12"));
    }
}
