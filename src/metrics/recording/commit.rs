use std::sync::atomic::Ordering;
use std::time::Duration;

use super::super::*;

impl MetaMetrics {
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

    /// ZFS-TXG-clone Phase 1: tracks the L2P-only direct-apply fast
    /// path. Wall time recorded is a subset of `commit_apply_us`
    /// (this path replaces the lane-recv stretch). Counter increments
    /// per commit taking the fast path.
    pub(crate) fn record_commit_direct_apply(&self, elapsed: Duration) {
        self.commit_direct_apply_count
            .fetch_add(1, Ordering::Relaxed);
        record_duration(
            &self.commit_direct_apply_us,
            &self.commit_direct_apply_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_commit_plan(&self, elapsed: Duration) {
        record_duration(&self.commit_plan_us, &self.commit_plan_max_us, elapsed);
    }

    pub(crate) fn record_commit_encode(&self, elapsed: Duration) {
        record_duration(&self.commit_encode_us, &self.commit_encode_max_us, elapsed);
    }

    pub(crate) fn record_commit_unlogged_gate_wait(&self, elapsed: Duration) {
        record_duration(
            &self.commit_unlogged_gate_wait_us,
            &self.commit_unlogged_gate_wait_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_commit_checkpoint_unlogged(&self, elapsed: Duration) {
        record_duration(
            &self.commit_checkpoint_unlogged_us,
            &self.commit_checkpoint_unlogged_max_us,
            elapsed,
        );
    }

    pub(crate) fn record_commit_read_held(&self, elapsed: Duration) {
        record_duration(
            &self.commit_read_held_us,
            &self.commit_read_held_max_us,
            elapsed,
        );
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
}
