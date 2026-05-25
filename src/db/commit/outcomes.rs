//! ZFS-TXG-clone Phase 2 — deferred commit outcome plumbing.
//!
//! ## Responsibility
//!
//! `Db::commit_ops` returns `(Lsn, Vec<ApplyOutcome>)` synchronously. The
//! deferred entry point `Db::commit_ops_deferred` returns
//! `(Lsn, DeferredOutcomeHandle)` as soon as WAL is durable and the
//! L2pBuffer insertions have completed. The outcome `Vec` is staged in
//! the [`DeferredOutcomeAggregator`] keyed by LSN, and the L2P
//! compactor's per-pass drain releases each entry whose LSN is at or
//! below the watermark the compactor observed at the start of its
//! pass. That watermark is the contiguous `last_applied_lsn` — every
//! commit at or below it has fully completed `apply`, including its
//! L2pBuffer inserts, before the compactor takes its first swap. The
//! pass itself acts as the TXG-sync boundary for outcome delivery.
//!
//! ## Why a single global watermark
//!
//! The first iteration of this module tracked the exact
//! `(volume, shard)` set each commit touched and held a staged
//! outcome until every one of those shards had folded past the
//! commit's LSN. That extra precision was load-bearing for
//! correctness *if* outcomes depended on tree state, but outcomes
//! are computed at commit time and stored in the aggregator
//! immediately — they never change after `stage`. A staged outcome
//! can be released as soon as we can say "the caller would have
//! observed equivalent state if the call had been synchronous".
//! That is true for every LSN at or below the contiguous
//! `last_applied_lsn` snapshot taken before the pass: those commits
//! have already finished `apply` (which mutates the L2pBuffer), so
//! sync-mode callers would already see the same state on a fresh
//! `get`. The simpler model also dodges a deadlock where a commit's
//! ops are entirely seq_guard-rejected — the L2pBuffer never gets
//! an insert, the shard's `compacted_lsn` never advances past the
//! rejected commit, and a per-shard watermark would hang forever.
//!
//! ## Concurrency model
//!
//! - `stage` is called from the commit thread, holding no caller-visible
//!   lock other than the aggregator's own mutex (briefly).
//! - `drain_up_to_lsn` is called from the L2P compactor's per-pass
//!   loop, *after* `publish_l2p_read_view` + `finish_compaction` on
//!   every shard. Holding the aggregator mutex while iterating
//!   `pending` is bounded — depth is the number of commits in flight
//!   between two compactor cycles, which the compactor cap on
//!   `max_interval_ms` keeps small.
//! - `poison_all` is called from `Db::drop` after the compactor has
//!   joined, so no further drain races with the wipe.
//!
//! The handle's receiver is capacity-1: the sender is invoked exactly
//! once (on drain or poison), the receiver is consumed exactly once
//! (`recv` or `try_recv`-then-drop). If the handle is dropped
//! without recv'ing, the receiver side just closes — the sender's
//! send returns `Err`, which we ignore.

use std::collections::BTreeMap;
use std::sync::Arc;

use crossbeam_channel::{Receiver, Sender, bounded};
use parking_lot::Mutex;

use crate::db::l2p_compactor::CompactorNotifier;
use crate::error::{MetaDbError, Result};
use crate::metrics::MetaMetrics;
use crate::testing::faults::{FaultController, FaultPoint};
use crate::tx::ApplyOutcome;
use crate::types::Lsn;

/// One-shot receiver returned by [`crate::Db::commit_ops_deferred`].
/// Outcomes arrive after the next L2P compactor pass whose start
/// watermark is at or above the commit's LSN.
///
/// The handle is single-use: `recv` consumes it. If the consumer no
/// longer cares about outcomes (cleanup-path optimisation), dropping
/// the handle is safe — the aggregator's send will fail silently when
/// the compactor eventually drains the entry.
pub struct DeferredOutcomeHandle {
    rx: Receiver<Result<Vec<ApplyOutcome>>>,
    lsn: Lsn,
}

impl DeferredOutcomeHandle {
    /// Build a handle whose channel is already populated. Used by
    /// `commit_ops_deferred` when the deferred-outcome flag is off, or
    /// when the commit batch did not touch any L2P shard (e.g.
    /// lifecycle / dedup-only / refcount-only ops). The receiver side
    /// resolves on the first `recv` / `try_recv` without ever blocking.
    pub(crate) fn ready(lsn: Lsn, value: Result<Vec<ApplyOutcome>>) -> Self {
        let (tx, rx) = bounded(1);
        // Send must succeed because the channel has capacity 1 and we
        // own both ends. Drop the sender so a follow-up recv on the
        // receiver sees the value rather than blocking on the sender.
        let _ = tx.send(value);
        drop(tx);
        Self { rx, lsn }
    }

    pub fn lsn(&self) -> Lsn {
        self.lsn
    }

    /// Non-consuming readiness probe. Returns `true` when the channel
    /// has the staged outcome (or has been disconnected — `recv` /
    /// `try_recv` will then resolve without blocking). Used by the
    /// onyx commit_worker pipeline to opportunistically drain ready
    /// chunks without paying for a blocking `recv`.
    pub fn is_ready(&self) -> bool {
        !self.rx.is_empty()
    }

    /// Non-blocking probe. Returns `Some(outcomes)` if the compactor
    /// drain has already released this entry; `None` if still pending.
    /// Consumes the handle on `Some` (channel is one-shot).
    pub fn try_recv(self) -> std::result::Result<Result<Vec<ApplyOutcome>>, Self> {
        match self.rx.try_recv() {
            Ok(value) => Ok(value),
            Err(crossbeam_channel::TryRecvError::Empty) => Err(self),
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                Ok(Err(MetaDbError::InvalidArgument(
                    "deferred outcome channel disconnected".into(),
                )))
            }
        }
    }

    /// Blocking wait for the staged outcome. Returns `Err` if the
    /// aggregator was poisoned (e.g. db shutdown) before the drain
    /// reached this entry.
    pub fn recv(self) -> Result<Vec<ApplyOutcome>> {
        match self.rx.recv() {
            Ok(value) => value,
            Err(_) => Err(MetaDbError::InvalidArgument(
                "deferred outcome channel disconnected".into(),
            )),
        }
    }
}

struct StagedOutcome {
    outcomes: Vec<ApplyOutcome>,
    sender: Sender<Result<Vec<ApplyOutcome>>>,
}

/// Tracks in-flight deferred commits keyed by LSN. The drain step
/// (called from the L2P compactor) snapshots the per-shard
/// `compacted_lsn` cursors and releases every staged commit whose
/// touched shards are all caught up.
pub(crate) struct DeferredOutcomeAggregator {
    pending: Mutex<BTreeMap<Lsn, StagedOutcome>>,
    metrics: Arc<MetaMetrics>,
    faults: Arc<FaultController>,
    /// Wake the L2P compactor on every `stage()` so its
    /// per-pass drain runs at commit frequency. Without this the
    /// compactor relies on its `max_interval_ms` timer (default 30 s);
    /// the onyx commit_worker pipeline fills `pending_q` to depth_cap
    /// within a few commits and then every subsequent issue blocks on
    /// `handle.recv()` for the full timer interval. LV3 throughput
    /// collapses to 0 MB/s — this is the regression documented in
    /// memory `phase3_async_wal_inflight_handoff`. `None` only during
    /// the brief window where the aggregator exists but the compactor
    /// has not been started yet (l2p_buffer disabled); in that mode
    /// the deferred-outcome path is never exercised.
    compactor_notifier: Option<Arc<CompactorNotifier>>,
}

impl DeferredOutcomeAggregator {
    pub(crate) fn new(
        metrics: Arc<MetaMetrics>,
        faults: Arc<FaultController>,
        compactor_notifier: Option<Arc<CompactorNotifier>>,
    ) -> Self {
        Self {
            pending: Mutex::new(BTreeMap::new()),
            metrics,
            faults,
            compactor_notifier,
        }
    }

    /// Park `(lsn, outcomes)` and return the handle the caller will
    /// consume. Staging the same LSN twice is a programming error and
    /// panics in debug.
    pub(crate) fn stage(
        &self,
        lsn: Lsn,
        outcomes: Vec<ApplyOutcome>,
    ) -> DeferredOutcomeHandle {
        let (tx, rx) = bounded(1);
        let staged = StagedOutcome {
            outcomes,
            sender: tx,
        };
        let depth = {
            let mut pending = self.pending.lock();
            debug_assert!(
                !pending.contains_key(&lsn),
                "deferred outcomes already staged for lsn {lsn}"
            );
            pending.insert(lsn, staged);
            pending.len()
        };
        self.metrics.record_deferred_outcomes_staged();
        self.metrics.record_deferred_outcomes_pending(depth);
        // Wake the compactor so its next pass runs at commit
        // frequency rather than after `max_interval_ms`. Cheap: one
        // Mutex acquire + `notify_one`; the worker collapses bursts
        // by clearing the flag on each pass.
        if let Some(notifier) = self.compactor_notifier.as_ref() {
            notifier.notify();
        }
        DeferredOutcomeHandle { rx, lsn }
    }

    /// Release every staged entry whose LSN is at or below `watermark`.
    /// The compactor calls this with the contiguous `last_applied_lsn`
    /// it snapshotted before its pass; every commit at or below that
    /// LSN has fully completed `apply`, so its outcome (already
    /// resident in the aggregator) is safe to deliver.
    pub(crate) fn drain_up_to_lsn(&self, watermark: Lsn) -> usize {
        let mut released_senders: Vec<StagedOutcome> = Vec::new();
        let depth_after;
        {
            let mut pending = self.pending.lock();
            let ready_lsns: Vec<Lsn> = pending
                .range(..=watermark)
                .map(|(lsn, _)| *lsn)
                .collect();
            for lsn in ready_lsns {
                if let Some(staged) = pending.remove(&lsn) {
                    released_senders.push(staged);
                }
            }
            depth_after = pending.len();
        }
        self.metrics
            .record_deferred_outcomes_pending(depth_after);
        let released_count = released_senders.len();
        // ZFS-TXG-clone Phase 2 fault: simulate a crash between the
        // pending-map drain and outcome delivery. The senders we
        // already popped get dropped on unwind, so every waiter
        // resolves with the channel-disconnected error. Apply state
        // on-disk is unaffected because `commit_ops_deferred`
        // populated `outcomes` and stamped `last_applied_lsn` before
        // staging. Action::Error is treated as Panic here because
        // `drain_up_to_lsn` returns `usize`. Gated on a non-empty
        // drain: the compactor now wakes on every `stage()` (see
        // `CompactorNotifier`) so empty-drain passes are common, and
        // injecting on those would burn the trigger before any real
        // staged outcome is in flight.
        if released_count > 0 {
            if let Err(err) = self.faults.inject(FaultPoint::DeferredOutcomeDrainMidway) {
                panic!("metadb: {} (mid-drain fault)", err);
            }
        }
        for staged in released_senders {
            // Receiver may have been dropped; ignore the send error.
            let _ = staged.sender.send(Ok(staged.outcomes));
        }
        if released_count > 0 {
            self.metrics
                .record_deferred_outcomes_released(released_count as u64);
        }
        released_count
    }

    /// Returns the number of staged entries still awaiting drain. Used
    /// by tests and snapshot reporting.
    pub(crate) fn pending_depth(&self) -> usize {
        self.pending.lock().len()
    }

    /// Drain every staged entry, sending each waiter an error. Called
    /// from `Db::close` after the apply gate is exclusive so no new
    /// `stage` races with the wipe. Each pending entry's `sender` is
    /// dropped after a `send`, which causes the receiver's `recv` to
    /// resolve with the error message rather than block forever.
    pub(crate) fn poison_all(&self, msg: &str) {
        let drained: Vec<StagedOutcome> = {
            let mut pending = self.pending.lock();
            std::mem::take(&mut *pending).into_values().collect()
        };
        self.metrics.record_deferred_outcomes_pending(0);
        for staged in drained {
            let _ = staged
                .sender
                .send(Err(MetaDbError::InvalidArgument(msg.to_string())));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metrics() -> Arc<MetaMetrics> {
        Arc::new(MetaMetrics::default())
    }

    fn fresh_agg() -> DeferredOutcomeAggregator {
        DeferredOutcomeAggregator::new(metrics(), FaultController::disabled(), None)
    }

    fn agg_with_notifier(notifier: Arc<CompactorNotifier>) -> DeferredOutcomeAggregator {
        DeferredOutcomeAggregator::new(
            metrics(),
            FaultController::disabled(),
            Some(notifier),
        )
    }

    #[test]
    fn stage_and_drain_releases_at_or_below_watermark() {
        let agg = fresh_agg();
        let h10 = agg.stage(10, vec![ApplyOutcome::L2pPrev(None)]);
        let h20 = agg.stage(20, vec![ApplyOutcome::L2pPrev(None)]);
        assert_eq!(h10.lsn(), 10);
        assert_eq!(h20.lsn(), 20);
        assert_eq!(agg.pending_depth(), 2);
        // Watermark only covers lsn=10.
        let released = agg.drain_up_to_lsn(10);
        assert_eq!(released, 1);
        assert_eq!(agg.pending_depth(), 1);
        let outcomes = h10.recv().expect("lsn=10 delivered");
        assert_eq!(outcomes.len(), 1);
        // Now drain past 20.
        let released = agg.drain_up_to_lsn(25);
        assert_eq!(released, 1);
        assert_eq!(agg.pending_depth(), 0);
        let outcomes = h20.recv().expect("lsn=20 delivered");
        assert_eq!(outcomes.len(), 1);
    }

    #[test]
    fn drain_below_watermark_holds_back_entry() {
        let agg = fresh_agg();
        let h = agg.stage(50, vec![ApplyOutcome::L2pPrev(None)]);
        let released = agg.drain_up_to_lsn(49);
        assert_eq!(released, 0);
        assert_eq!(agg.pending_depth(), 1);
        // Hold the handle to keep the channel alive long enough for
        // the second drain to send into it.
        let _ = h;
    }

    #[test]
    fn poison_all_unblocks_waiters() {
        let agg = fresh_agg();
        let handle = agg.stage(30, vec![ApplyOutcome::L2pPrev(None)]);
        agg.poison_all("test shutdown");
        let result = handle.recv();
        assert!(result.is_err(), "poisoned handle resolves to Err");
        assert_eq!(agg.pending_depth(), 0);
    }

    #[test]
    fn dropped_handle_does_not_block_drain() {
        let agg = fresh_agg();
        let handle = agg.stage(40, vec![ApplyOutcome::L2pPrev(None)]);
        drop(handle);
        // Drain still releases the entry; the send fails silently.
        let released = agg.drain_up_to_lsn(40);
        assert_eq!(released, 1);
    }

    #[test]
    fn try_recv_returns_handle_when_not_ready() {
        let agg = fresh_agg();
        let handle = agg.stage(50, vec![ApplyOutcome::L2pPrev(None)]);
        let handle = match handle.try_recv() {
            Err(handle) => handle,
            Ok(_) => panic!("expected handle still pending"),
        };
        let released = agg.drain_up_to_lsn(50);
        assert_eq!(released, 1);
        let outcomes = handle.recv().expect("outcomes delivered");
        assert_eq!(outcomes.len(), 1);
    }

    /// Regression for the Phase 2 drain stall: `stage()` must wake the
    /// compactor immediately, not on the next `max_interval_ms` timer.
    /// Without this wake, the onyx commit_worker pipeline blocks on
    /// `handle.recv()` for 30 s after each chunk hits the depth cap.
    /// We assert the wake here by having a consumer thread park on the
    /// notifier with a long timeout, then verifying it returns
    /// promptly after `stage()`.
    #[test]
    fn stage_wakes_compactor_notifier() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::thread;
        use std::time::{Duration, Instant};

        let notifier = Arc::new(CompactorNotifier::new());
        let agg = agg_with_notifier(notifier.clone());

        // Park on the notifier with a 5 s timeout (much longer than
        // the wall time we expect — a successful wake completes in
        // microseconds).
        let parked_done = Arc::new(AtomicBool::new(false));
        let parked_done_thread = parked_done.clone();
        let notifier_thread = notifier.clone();
        let waiter = thread::spawn(move || {
            let started = Instant::now();
            notifier_thread.wait(Duration::from_secs(5));
            parked_done_thread.store(true, Ordering::Release);
            started.elapsed()
        });

        // Brief yield so the waiter parks on the cvar before we stage.
        thread::sleep(Duration::from_millis(20));
        assert!(
            !parked_done.load(Ordering::Acquire),
            "waiter resolved before stage — wake test is racy"
        );

        let handle = agg.stage(99, vec![ApplyOutcome::L2pPrev(None)]);
        let elapsed = waiter.join().expect("waiter joined");
        assert!(
            elapsed < Duration::from_secs(1),
            "stage() must wake the compactor notifier, but wait took {:?}",
            elapsed
        );
        // Drop the handle so the receiver side closes cleanly.
        drop(handle);
    }

    /// Multiple `stage()` calls between two passes must coalesce into
    /// a single wake — the worker observes one flag set regardless of
    /// burst size.
    #[test]
    fn stage_bursts_coalesce_into_single_wake() {
        let notifier = Arc::new(CompactorNotifier::new());
        let agg = agg_with_notifier(notifier.clone());
        let _h1 = agg.stage(1, vec![ApplyOutcome::L2pPrev(None)]);
        let _h2 = agg.stage(2, vec![ApplyOutcome::L2pPrev(None)]);
        let _h3 = agg.stage(3, vec![ApplyOutcome::L2pPrev(None)]);
        // First wait observes the burst and returns immediately.
        let started = std::time::Instant::now();
        notifier.wait(std::time::Duration::from_secs(1));
        assert!(
            started.elapsed() < std::time::Duration::from_millis(100),
            "wait after burst must return immediately"
        );
        // Subsequent wait parks for the full timeout because the flag
        // was cleared.
        let started = std::time::Instant::now();
        notifier.wait(std::time::Duration::from_millis(50));
        assert!(
            started.elapsed() >= std::time::Duration::from_millis(40),
            "second wait should park for ~50 ms, got {:?}",
            started.elapsed()
        );
    }
}
