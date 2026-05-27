use std::sync::atomic::Ordering;
use std::time::Duration;

use super::super::*;

impl MetaMetrics {
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

    /// Caller-side `Op::Submit` send → writer dequeue. Recorded by
    /// the writer thread when it first sees each submit. One sample
    /// per submit (counts should match `wal_submit_calls`).
    pub(crate) fn record_wal_queue_wait(&self, elapsed: Duration) {
        record_duration(
            &self.wal_queue_wait_us,
            &self.wal_queue_wait_max_us,
            elapsed,
        );
    }

    /// Writer dequeue → writer ack send (per submit). Captures
    /// batch-assembly delay, encode, write, and any fsync amortised
    /// over the batch. Recorded by the writer thread just before each
    /// ack is sent. One sample per submit.
    pub(crate) fn record_wal_writer_busy(&self, elapsed: Duration) {
        record_duration(
            &self.wal_writer_busy_us,
            &self.wal_writer_busy_max_us,
            elapsed,
        );
    }

    /// Writer ack send → caller's `recv` return. Captures channel
    /// wakeup latency. Recorded by the caller after `ack.recv()`
    /// returns. One sample per submit.
    pub(crate) fn record_wal_wake_roundtrip(&self, elapsed: Duration) {
        record_duration(
            &self.wal_wake_roundtrip_us,
            &self.wal_wake_roundtrip_max_us,
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

    /// ZFS-TXG-clone Phase 3: per-async-batch counters. `acks` is the
    /// number of submitters in the batch (every one of them returned
    /// without seeing an inline fsync); `bytes` is the encoded batch
    /// size. Both stay zero when `wal_async_commits_enabled = false`
    /// because no batch ever takes the async path.
    pub(crate) fn record_wal_async_batch(&self, acks: usize, bytes: usize) {
        self.wal_async_acks_total
            .fetch_add(acks as u64, Ordering::Relaxed);
        self.wal_async_pending_bytes_total
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    /// ZFS-TXG-clone Phase 3: TXG-sync barrier latency.
    /// `WalSet::fsync_all_lanes` accumulates wall-clock here so the
    /// reader can see how the TXG fsync amortises against the bytes
    /// that landed in `wal_async_pending_bytes_total` since the last
    /// flush.
    pub(crate) fn record_wal_fsync_at_txg_sync(&self, elapsed: Duration) {
        record_duration(
            &self.wal_fsync_at_txg_sync_us,
            &self.wal_fsync_at_txg_sync_max_us,
            elapsed,
        );
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
