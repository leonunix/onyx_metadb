use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[derive(Debug, Default)]
pub struct MetaMetrics {
    commit_attempts: AtomicU64,
    commit_success: AtomicU64,
    commit_errors: AtomicU64,
    commit_empty: AtomicU64,
    commit_ops: AtomicU64,
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
    apply_refcount_count: AtomicU64,
    apply_refcount_us: AtomicU64,
    apply_refcount_max_us: AtomicU64,
    apply_dedup_count: AtomicU64,
    apply_dedup_us: AtomicU64,
    apply_dedup_max_us: AtomicU64,

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
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetaMetricsSnapshot {
    pub commit_attempts: u64,
    pub commit_success: u64,
    pub commit_errors: u64,
    pub commit_empty: u64,
    pub commit_ops: u64,
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
    pub commit_apply_us: u64,
    pub commit_apply_max_us: u64,
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
    pub apply_refcount_count: u64,
    pub apply_refcount_us: u64,
    pub apply_refcount_max_us: u64,
    pub apply_dedup_count: u64,
    pub apply_dedup_us: u64,
    pub apply_dedup_max_us: u64,
    pub l2p_get_calls: u64,
    pub l2p_get_lock_wait_us: u64,
    pub l2p_get_lock_wait_max_us: u64,
    pub l2p_get_tree_walk_us: u64,
    pub l2p_get_tree_walk_max_us: u64,
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
            commit_apply_us: load(&self.commit_apply_us),
            commit_apply_max_us: load(&self.commit_apply_max_us),
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
            apply_refcount_count: load(&self.apply_refcount_count),
            apply_refcount_us: load(&self.apply_refcount_us),
            apply_refcount_max_us: load(&self.apply_refcount_max_us),
            apply_dedup_count: load(&self.apply_dedup_count),
            apply_dedup_us: load(&self.apply_dedup_us),
            apply_dedup_max_us: load(&self.apply_dedup_max_us),
            l2p_get_calls: load(&self.l2p_get_calls),
            l2p_get_lock_wait_us: load(&self.l2p_get_lock_wait_us),
            l2p_get_lock_wait_max_us: load(&self.l2p_get_lock_wait_max_us),
            l2p_get_tree_walk_us: load(&self.l2p_get_tree_walk_us),
            l2p_get_tree_walk_max_us: load(&self.l2p_get_tree_walk_max_us),
        }
    }

    pub(crate) fn record_commit_empty(&self) {
        self.commit_empty.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_commit_attempt(&self, ops: usize) {
        self.commit_attempts.fetch_add(1, Ordering::Relaxed);
        self.commit_ops.fetch_add(ops as u64, Ordering::Relaxed);
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

    pub(crate) fn record_commit_apply(&self, elapsed: Duration) {
        record_duration(&self.commit_apply_us, &self.commit_apply_max_us, elapsed);
    }

    pub(crate) fn record_apply_l2p_put(&self, elapsed: Duration) {
        self.apply_l2p_put_count.fetch_add(1, Ordering::Relaxed);
        record_duration(
            &self.apply_l2p_put_us,
            &self.apply_l2p_put_max_us,
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

    pub(crate) fn record_apply_l2p_remap(&self, elapsed: Duration) {
        self.apply_l2p_remap_count.fetch_add(1, Ordering::Relaxed);
        record_duration(
            &self.apply_l2p_remap_us,
            &self.apply_l2p_remap_max_us,
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

    pub(crate) fn record_cleanup_forward_check(&self, elapsed: Duration) {
        self.cleanup_forward_checks.fetch_add(1, Ordering::Relaxed);
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
                "\"commit_apply_us\":{},",
                "\"commit_apply_max_us\":{},",
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
                "\"apply_refcount_count\":{},",
                "\"apply_refcount_us\":{},",
                "\"apply_refcount_max_us\":{},",
                "\"apply_dedup_count\":{},",
                "\"apply_dedup_us\":{},",
                "\"apply_dedup_max_us\":{},",
                "\"l2p_get_calls\":{},",
                "\"l2p_get_lock_wait_us\":{},",
                "\"l2p_get_lock_wait_max_us\":{},",
                "\"l2p_get_tree_walk_us\":{},",
                "\"l2p_get_tree_walk_max_us\":{}",
                "}}"
            ),
            self.commit_attempts,
            self.commit_success,
            self.commit_errors,
            self.commit_empty,
            self.commit_ops,
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
            self.commit_apply_us,
            self.commit_apply_max_us,
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
            self.apply_refcount_count,
            self.apply_refcount_us,
            self.apply_refcount_max_us,
            self.apply_dedup_count,
            self.apply_dedup_us,
            self.apply_dedup_max_us,
            self.l2p_get_calls,
            self.l2p_get_lock_wait_us,
            self.l2p_get_lock_wait_max_us,
            self.l2p_get_tree_walk_us,
            self.l2p_get_tree_walk_max_us,
        )
    }
}

fn load(value: &AtomicU64) -> u64 {
    value.load(Ordering::Relaxed)
}

fn record_duration(total: &AtomicU64, max: &AtomicU64, elapsed: Duration) {
    let us = elapsed.as_micros().min(u128::from(u64::MAX)) as u64;
    total.fetch_add(us, Ordering::Relaxed);
    fetch_max(max, us);
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
