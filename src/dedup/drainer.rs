//! Background per-dedup-shard drainer: folds staged `(hash → mutation)`
//! entries into the on-disk cuckoo OFF the commit/apply critical path.
//!
//! Mirrors the refcount `DrainerWorker` run-loop (`refcount/shard.rs`)
//! and reuses the proven `refcount::overlay::DrainerState` preempt /
//! shutdown protocol verbatim. The only dedup-specific differences:
//! - the "cycle" is [`DedupIndex::drain_shard_once`] (swap active →
//!   draining, write the cuckoo, clear draining) — no sealed-page
//!   overlay (the cuckoo write is self-installing) and no
//!   page-generation replay-skip (dedup writes are idempotent).
//! - park wakeups key off the staging `active` length, not a `DeltaMap`.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::metrics::MetaMetrics;
use crate::refcount::overlay::DrainerState;

use super::index::DedupIndex;

pub(crate) struct DedupDrainerWorker {
    pub(crate) shard_idx: usize,
    pub(crate) dedup_index: Arc<DedupIndex>,
    pub(crate) interval_ms: u64,
    pub(crate) threshold_entries: usize,
    pub(crate) state: Arc<DrainerState>,
    pub(crate) metrics: Arc<MetaMetrics>,
}

impl DedupDrainerWorker {
    pub(crate) fn run(self) {
        let interval = Duration::from_millis(self.interval_ms.max(1));
        loop {
            // Park until tick / threshold / preempt / shutdown.
            {
                let mut guard = self.state.mu.lock();
                loop {
                    if self.state.shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    if self.state.preempt.load(Ordering::Acquire) {
                        self.metrics.record_dedup_drainer_preempt();
                        self.state.in_cycle.store(false, Ordering::Release);
                        // Re-assert `preempt_done=true` every iteration —
                        // see `refcount/overlay.rs::preempt_and_wait` for
                        // the linearisation race this closes (a concurrent
                        // caller's `false`-store can win against the
                        // worker's earlier `true`-store).
                        while {
                            self.state.preempt_done.store(true, Ordering::Release);
                            self.state.cv.notify_all();
                            self.state.preempt.load(Ordering::Acquire)
                                && !self.state.shutdown.load(Ordering::Acquire)
                        } {
                            self.state.cv.wait(&mut guard);
                        }
                        continue;
                    }
                    if self.dedup_index.staging_active_len(self.shard_idx) >= self.threshold_entries
                    {
                        break;
                    }
                    let _ = self.state.cv.wait_for(&mut guard, interval);
                    if self.state.shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    if self.state.preempt.load(Ordering::Acquire) {
                        continue;
                    }
                    if self.dedup_index.staging_active_len(self.shard_idx) > 0 {
                        break;
                    }
                }
            }

            self.metrics.record_dedup_drainer_wake();
            self.state.in_cycle.store(true, Ordering::Release);
            let cycle_started = Instant::now();
            match self.dedup_index.drain_shard_once(self.shard_idx) {
                Ok(0) => {}
                Ok(n) => self
                    .metrics
                    .record_dedup_drainer_cycle(n, cycle_started.elapsed()),
                Err(err) => {
                    // drain_shard_once rolled the snapshot back into
                    // `active` (idempotent cuckoo writes), so the next
                    // cycle retries. Surface the rate via the warn log.
                    tracing::warn!(
                        shard = self.shard_idx,
                        error = %err,
                        "dedup-drainer: drain_shard_once failed; rolled back for retry"
                    );
                }
            }
            self.state.in_cycle.store(false, Ordering::Release);
            self.state.cv.notify_all();
        }
    }
}
