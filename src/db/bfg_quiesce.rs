//! BFG quiesce thread.
//!
//! `BfgQuiesceThread` is the single producer of BFG rolls. Each cycle:
//!
//! 1. Wait `bfg_timeout` (default 5 s). `force_roll()` (used by
//!    `flush_with_gate`) cuts the wait short. Future hooks: dirty-data
//!    threshold from the L2P buffer.
//! 2. `state.roll_to_quiescing()` — closes the current Open BFG to new
//!    commits, waits for in-flight commits to drop their `BfgGuard`s,
//!    then advances `open_bfg`.
//! 3. `state.promote_to_syncing(bfg)` — flip Quiescing → Syncing.
//! 4. `sync_notifier.notify()` — wake the [`super::bfg_sync::BfgSyncThread`].
//!
//! Only one quiesce in flight at a time (single thread). Legacy mode: this
//! enforces "at most one BFG in Quiescing" by construction. Pipeline mode
//! (`bfg_admission_pipeline_enabled`): each roll still runs one at a time,
//! but rolls no longer block on the fold, so several frozen Quiescing
//! generations can queue in a FIFO behind the single Syncing one.
//!
//! Idempotent shutdown via `Drop` → `stop` → `join`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::bfg::BfgStateMachine;
use crate::metrics::MetaMetrics;
use crate::testing::faults::{FaultController, FaultPoint};
use crate::types::Bfg;

use super::bfg_sync::SyncNotifier;

#[derive(Clone, Copy, Debug)]
pub struct QuiesceParams {
    /// How long the worker waits between rolls when no force_roll fires.
    /// Default 5000 ms.
    pub bfg_timeout_ms: u64,
}

impl Default for QuiesceParams {
    fn default() -> Self {
        Self {
            bfg_timeout_ms: 5000,
        }
    }
}

/// Producer/consumer wake handle shared between the quiesce worker and
/// any caller that wants to drive a roll immediately (e.g.
/// `flush_with_gate`).
pub struct QuiesceNotifier {
    /// Highest Open-BFG generation requested by callers. A generation-tagged
    /// request can be discarded if a timer or earlier request already rolled
    /// it; a stale wake must never roll the next generation.
    force_target: Mutex<Option<Bfg>>,
    cv: Condvar,
}

impl QuiesceNotifier {
    pub fn new() -> Self {
        Self {
            force_target: Mutex::new(None),
            cv: Condvar::new(),
        }
    }

    /// Tell the quiesce worker to roll the current Open BFG immediately.
    /// Returns the BFG number that the worker WILL move to Quiescing.
    /// The caller can `state.wait_until_synced(returned_bfg)` to block
    /// until the manifest commit for that BFG is durable.
    ///
    /// `target_open_bfg` is the caller's read of `state.open_bfg()` at
    /// call time — pass `state.open_bfg()`. Returned for echo so the
    /// caller doesn't need to read it twice.
    pub fn signal_force(&self, target_open_bfg: Bfg) -> Bfg {
        let mut target = self.force_target.lock();
        *target = Some(target.map_or(target_open_bfg, |pending| pending.max(target_open_bfg)));
        self.cv.notify_one();
        target_open_bfg
    }

    /// Park up to `timeout` waiting for a force-roll notification.
    /// Returns the requested generation, or `None` when the timeout elapsed.
    fn wait_with_timeout(&self, timeout: Duration) -> Option<Bfg> {
        let mut target = self.force_target.lock();
        if target.is_some() {
            return target.take();
        }
        let _ = self.cv.wait_for(&mut target, timeout);
        target.take()
    }

    fn wake_all_for_shutdown(&self) {
        let _target = self.force_target.lock();
        self.cv.notify_all();
    }
}

impl Default for QuiesceNotifier {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BfgQuiesceThread {
    inner: Arc<Inner>,
    handle: Option<JoinHandle<()>>,
}

struct Inner {
    state: Arc<BfgStateMachine>,
    notifier: Arc<QuiesceNotifier>,
    sync_notifier: Arc<SyncNotifier>,
    params: QuiesceParams,
    shutdown: AtomicBool,
    #[allow(dead_code)]
    metrics: Arc<MetaMetrics>,
    faults: Arc<FaultController>,
}

impl BfgQuiesceThread {
    pub fn start(
        state: Arc<BfgStateMachine>,
        notifier: Arc<QuiesceNotifier>,
        sync_notifier: Arc<SyncNotifier>,
        params: QuiesceParams,
        metrics: Arc<MetaMetrics>,
        faults: Arc<FaultController>,
    ) -> Self {
        let inner = Arc::new(Inner {
            state,
            notifier,
            sync_notifier,
            params,
            shutdown: AtomicBool::new(false),
            metrics,
            faults,
        });
        let worker = Arc::clone(&inner);
        let handle = thread::Builder::new()
            .name("metadb-bfg-quiesce".into())
            .spawn(move || run_worker(worker))
            .expect("metadb: failed to spawn bfg quiesce worker");
        Self {
            inner,
            handle: Some(handle),
        }
    }

    pub fn notifier(&self) -> Arc<QuiesceNotifier> {
        self.inner.notifier.clone()
    }

    pub fn stop(&mut self) {
        if self.inner.shutdown.swap(true, Ordering::Release) {
            return;
        }
        // Wake both: the quiesce worker (so it exits the wait_with_timeout
        // loop) and the state machine (so any blocked
        // `roll_to_quiescing` returns early without advancing).
        self.inner.notifier.wake_all_for_shutdown();
        self.inner.state.shutdown();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for BfgQuiesceThread {
    fn drop(&mut self) {
        self.stop();
    }
}

fn run_worker(inner: Arc<Inner>) {
    // Pin to the BfgSync CPU set (ordinal 1 so it lands on a different
    // CPU than the heavy sync worker at ordinal 0 when the set has >1
    // CPU). The quiesce worker is light (rolls on a timer + brief
    // inflight-drain wait) but must not float onto the hot front-end
    // cores where its roll's `closing_open` window would interleave
    // with the commit path.
    crate::affinity::bind_current(crate::affinity::ThreadRole::BfgSync, 1);
    let timeout = Duration::from_millis(inner.params.bfg_timeout_ms.max(1));
    let mut deadline = Instant::now() + timeout;
    while !inner.shutdown.load(Ordering::Acquire) {
        // Wait either for the bfg_timeout to elapse or a force-roll
        // notification. Stale generation-tagged wakes keep the original
        // deadline, so a stream of delayed callers cannot starve the timer.
        let forced_target = inner
            .notifier
            .wait_with_timeout(deadline.saturating_duration_since(Instant::now()));
        if inner.shutdown.load(Ordering::Acquire) {
            break;
        }
        // A failed sync cycle leaves the Syncing slot stuck forever. Stop
        // rolling/notifying so the worker does not keep waking the sync thread
        // for a group that has already poisoned the process.
        if inner.state.is_aborted() {
            break;
        }
        if let Some(target) = forced_target {
            let current = inner.state.open_bfg();
            if current != target {
                tracing::debug!(
                    target,
                    current,
                    "metadb: discard stale BFG force-roll request"
                );
                continue;
            }
        } else if Instant::now() < deadline {
            // Condvars may wake spuriously. A wake without a force target is a
            // timer event only after the current deadline has elapsed.
            continue;
        }
        // Roll → promote → notify sync. roll_to_quiescing handles its own
        // shutdown check (returns current open_bfg without advancing) so
        // we re-check shutdown afterwards before promoting.
        let target_bfg = inner.state.open_bfg();
        inner.metrics.record_checkpoint_quiesce_phase(
            target_bfg,
            crate::metrics::CheckpointQuiescePhase::Quiesce,
        );
        let bfg = inner.state.roll_to_quiescing();
        if inner.state.is_aborted() {
            inner.metrics.record_checkpoint_quiesce_phase(
                bfg,
                crate::metrics::CheckpointQuiescePhase::Error,
            );
            break;
        }
        if inner.shutdown.load(Ordering::Acquire) {
            inner
                .metrics
                .record_checkpoint_quiesce_phase(bfg, crate::metrics::CheckpointQuiescePhase::Idle);
            break;
        }
        // Pipeline mode: promote without blocking on the prior fold. The rolled
        // generation is frozen in the ring FIFO; `try_promote_next` promotes the
        // oldest one only if nothing is Syncing, then the worker immediately
        // loops to roll the next generation instead of parking in
        // `promote_to_syncing`. The sync worker pulls further frozen generations
        // after each `mark_synced`. This is the whole point of the pipeline: a
        // soft-limit crossing during a fold no longer parks commits for the
        // fold's duration.
        if inner.state.pipeline_enabled() {
            // NO post-roll "is `bfg` still Quiescing?" check here. `bfg` may
            // ALREADY have been promoted (or even synced) by the sync worker's
            // own post-`mark_synced` `try_promote_next` in the window between
            // this roll returning and here — that is normal pipeline progress,
            // NOT an invariant violation. A `break` on that (as an earlier
            // revision did) kills the sole roller and wedges admission forever.
            // Shutdown/abort were already handled above, so the roll succeeded
            // and pushed `bfg` onto the FIFO; promotion is the sync path's job.
            //
            // Fault window between the Open -> Quiescing flip and promotion.
            // Recovery rebuilds from the durable manifest and ignores this
            // in-memory half-roll.
            if let Err(err) = inner.faults.inject(FaultPoint::BfgQuiesceMidway) {
                tracing::error!(error = %err, bfg, "metadb: BfgQuiesceThread fault-injected midway; skipping promote");
                inner.metrics.record_checkpoint_quiesce_phase(
                    bfg,
                    crate::metrics::CheckpointQuiescePhase::Error,
                );
                continue;
            }
            if inner.state.try_promote_next().is_some() {
                inner.sync_notifier.notify();
            }
            inner
                .metrics
                .record_checkpoint_quiesce_phase(bfg, crate::metrics::CheckpointQuiescePhase::Idle);
            deadline = Instant::now() + timeout;
            continue;
        }

        // Shutdown and abort were handled above. Any remaining mismatch is an
        // invariant violation; stop this producer rather than notifying sync
        // for a generation that was never promoted.
        let snapshot = inner.state.snapshot();
        if snapshot.quiescing_bfg != Some(bfg) {
            tracing::error!(
                bfg,
                quiescing_bfg = ?snapshot.quiescing_bfg,
                "metadb: BfgQuiesceThread roll returned without a Quiescing BFG"
            );
            inner.metrics.record_checkpoint_quiesce_phase(
                bfg,
                crate::metrics::CheckpointQuiescePhase::Error,
            );
            break;
        }
        // Fault window between the Open -> Quiescing flip and
        // `promote_to_syncing`. Soak runs can crash here; recovery must rebuild
        // from the durable manifest and ignore this in-memory half-roll.
        if let Err(err) = inner.faults.inject(FaultPoint::BfgQuiesceMidway) {
            tracing::error!(error = %err, bfg, "metadb: BfgQuiesceThread fault-injected midway; skipping promote");
            inner.metrics.record_checkpoint_quiesce_phase(
                bfg,
                crate::metrics::CheckpointQuiescePhase::Error,
            );
            continue;
        }
        inner.metrics.record_checkpoint_quiesce_phase(
            bfg,
            crate::metrics::CheckpointQuiescePhase::AwaitSync,
        );
        inner.state.promote_to_syncing(bfg);
        let promoted = inner.state.snapshot().syncing_bfg == Some(bfg);
        if !promoted {
            if inner.state.is_aborted() {
                inner.metrics.record_checkpoint_quiesce_phase(
                    bfg,
                    crate::metrics::CheckpointQuiescePhase::Error,
                );
            } else if inner.shutdown.load(Ordering::Acquire) {
                inner.metrics.record_checkpoint_quiesce_phase(
                    bfg,
                    crate::metrics::CheckpointQuiescePhase::Idle,
                );
            } else {
                tracing::error!(
                    bfg,
                    "metadb: BFG promotion returned without installing Syncing"
                );
                inner.metrics.record_checkpoint_quiesce_phase(
                    bfg,
                    crate::metrics::CheckpointQuiescePhase::Error,
                );
            }
            break;
        }
        inner.sync_notifier.notify();
        inner
            .metrics
            .record_checkpoint_quiesce_phase(bfg, crate::metrics::CheckpointQuiescePhase::Idle);
        deadline = Instant::now() + timeout;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn noop_metrics() -> Arc<MetaMetrics> {
        Arc::new(MetaMetrics::new())
    }

    fn disabled_faults() -> Arc<FaultController> {
        FaultController::disabled()
    }

    /// Stand-up a paired sync worker that just acks every notification,
    /// so the quiesce side sees full state machine transitions.
    fn spawn_ack_sync(
        state: Arc<BfgStateMachine>,
        sync_notifier: Arc<SyncNotifier>,
    ) -> super::super::bfg_sync::BfgSyncThread {
        use super::super::bfg_sync::{BfgSyncThread, SyncWorkFn};
        let work: SyncWorkFn = Arc::new(|_| Ok(()));
        BfgSyncThread::start(state, sync_notifier, work, noop_metrics())
    }

    #[test]
    fn force_roll_kicks_off_immediate_cycle() {
        let state = Arc::new(BfgStateMachine::new(0));
        let sync_n = Arc::new(SyncNotifier::new());
        let _sync = spawn_ack_sync(Arc::clone(&state), Arc::clone(&sync_n));
        let notifier = Arc::new(QuiesceNotifier::new());
        let mut q = BfgQuiesceThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            Arc::clone(&sync_n),
            QuiesceParams {
                bfg_timeout_ms: 60_000, // never via timer in this test
            },
            noop_metrics(),
            disabled_faults(),
        );
        let target = state.open_bfg();
        let ret = notifier.signal_force(target);
        assert_eq!(ret, target);
        // Sync should mark BFG `target` as synced (checkpoint_bfg = target).
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && state.checkpoint_bfg() < target {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(state.checkpoint_bfg(), target);
        q.stop();
    }

    #[test]
    fn stale_force_generation_does_not_roll_next_open_bfg() {
        let state = Arc::new(BfgStateMachine::new(0));
        let sync_n = Arc::new(SyncNotifier::new());
        let _sync = spawn_ack_sync(Arc::clone(&state), Arc::clone(&sync_n));
        let notifier = Arc::new(QuiesceNotifier::new());
        let mut q = BfgQuiesceThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            Arc::clone(&sync_n),
            QuiesceParams {
                bfg_timeout_ms: 60_000,
            },
            noop_metrics(),
            disabled_faults(),
        );

        notifier.signal_force(1);
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && state.checkpoint_bfg() < 1 {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(state.open_bfg(), 2);

        // This models an admission crossing whose wake was delayed until a
        // timer had already rolled its generation. It must be consumed as
        // stale, not interpreted as a request to roll BFG 2.
        notifier.signal_force(1);
        thread::sleep(Duration::from_millis(50));
        assert_eq!(state.open_bfg(), 2);
        assert_eq!(state.checkpoint_bfg(), 1);

        notifier.signal_force(2);
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && state.checkpoint_bfg() < 2 {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(state.checkpoint_bfg(), 2);
        q.stop();
    }

    #[test]
    fn stale_force_generations_do_not_starve_timer_roll() {
        let state = Arc::new(BfgStateMachine::new(0));
        let sync_n = Arc::new(SyncNotifier::new());
        let _sync = spawn_ack_sync(Arc::clone(&state), Arc::clone(&sync_n));
        let notifier = Arc::new(QuiesceNotifier::new());
        let mut q = BfgQuiesceThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            sync_n,
            QuiesceParams {
                bfg_timeout_ms: 120,
            },
            noop_metrics(),
            disabled_faults(),
        );

        // Keep waking the worker with a generation that was never Open. If
        // each stale wake restarted the timer, BFG 1 could not sync until at
        // least 120 ms after this 320 ms stream ends.
        let sender = thread::spawn(move || {
            for _ in 0..8 {
                thread::sleep(Duration::from_millis(40));
                notifier.signal_force(0);
            }
        });
        sender.join().unwrap();
        let deadline = Instant::now() + Duration::from_millis(80);
        while Instant::now() < deadline && state.checkpoint_bfg() < 1 {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(
            state.checkpoint_bfg() >= 1,
            "stale force wakes must preserve the original timer deadline"
        );
        q.stop();
    }

    #[test]
    fn timer_drives_rolls_on_idle() {
        let state = Arc::new(BfgStateMachine::new(0));
        let sync_n = Arc::new(SyncNotifier::new());
        let _sync = spawn_ack_sync(Arc::clone(&state), Arc::clone(&sync_n));
        let notifier = Arc::new(QuiesceNotifier::new());
        let mut q = BfgQuiesceThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            Arc::clone(&sync_n),
            QuiesceParams { bfg_timeout_ms: 30 }, // fast for tests
            noop_metrics(),
            disabled_faults(),
        );
        // Within ~150 ms we should see at least 3 BFGs sync'd.
        thread::sleep(Duration::from_millis(180));
        assert!(
            state.checkpoint_bfg() >= 3,
            "expected >= 3 syncs in 180 ms with 30ms timer, got {}",
            state.checkpoint_bfg()
        );
        q.stop();
    }

    #[test]
    fn force_roll_with_inflight_commit_blocks_until_drop() {
        let state = Arc::new(BfgStateMachine::new(0));
        let sync_n = Arc::new(SyncNotifier::new());
        let _sync = spawn_ack_sync(Arc::clone(&state), Arc::clone(&sync_n));
        let notifier = Arc::new(QuiesceNotifier::new());
        let mut q = BfgQuiesceThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            Arc::clone(&sync_n),
            QuiesceParams {
                bfg_timeout_ms: 60_000,
            },
            noop_metrics(),
            disabled_faults(),
        );
        // Pin BFG 1 with an open guard, then force-roll.
        let guard = state.enter();
        let target = state.open_bfg();
        notifier.signal_force(target);
        // Sync cannot finish BFG 1 until the guard drops.
        thread::sleep(Duration::from_millis(50));
        assert_eq!(state.checkpoint_bfg(), 0);
        drop(guard);
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && state.checkpoint_bfg() < target {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(state.checkpoint_bfg(), target);
        q.stop();
    }

    #[test]
    fn stop_idempotent_and_quick() {
        let state = Arc::new(BfgStateMachine::new(0));
        let sync_n = Arc::new(SyncNotifier::new());
        let _sync = spawn_ack_sync(Arc::clone(&state), Arc::clone(&sync_n));
        let notifier = Arc::new(QuiesceNotifier::new());
        let mut q = BfgQuiesceThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            Arc::clone(&sync_n),
            QuiesceParams::default(),
            noop_metrics(),
            disabled_faults(),
        );
        let started = Instant::now();
        q.stop();
        // Should join in well under the 5 s bfg_timeout because shutdown
        // wakes the wait.
        assert!(started.elapsed() < Duration::from_millis(500));
        q.stop(); // idempotent
    }

    #[test]
    fn fault_midway_skips_promote_and_sync_notify() {
        // FaultPoint::BfgQuiesceMidway fires between roll_to_quiescing
        // and promote_to_syncing. With FaultAction::Error the worker
        // logs and continues to the next iteration without notifying
        // the sync thread — the open_bfg has advanced but the now-
        // Quiescing BFG stays Quiescing; production fault runs terminate or
        // restart before this half-roll can be reused.
        use crate::testing::faults::FaultAction;
        let state = Arc::new(BfgStateMachine::new(0));
        let sync_n = Arc::new(SyncNotifier::new());
        let metrics = noop_metrics();
        let sync_calls = Arc::new(AtomicU64::new(0));
        let calls = Arc::clone(&sync_calls);
        let work: super::super::bfg_sync::SyncWorkFn = Arc::new(move |_| {
            calls.fetch_add(1, Ordering::AcqRel);
            Ok(())
        });
        let _sync = super::super::bfg_sync::BfgSyncThread::start(
            Arc::clone(&state),
            Arc::clone(&sync_n),
            work,
            noop_metrics(),
        );
        let faults = FaultController::new();
        faults.install(FaultPoint::BfgQuiesceMidway, 1, FaultAction::Error);
        let notifier = Arc::new(QuiesceNotifier::new());
        let mut q = BfgQuiesceThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            Arc::clone(&sync_n),
            QuiesceParams {
                bfg_timeout_ms: 60_000,
            },
            Arc::clone(&metrics),
            Arc::clone(&faults),
        );
        notifier.signal_force(state.open_bfg());
        // Wait for the fault to fire.
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline && faults.hits(FaultPoint::BfgQuiesceMidway) == 0 {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(faults.fired(FaultPoint::BfgQuiesceMidway));
        // Slot 1 should be in Quiescing (roll advanced) but never
        // promoted to Syncing, and the sync worker was never called.
        thread::sleep(Duration::from_millis(20));
        let s = state.snapshot();
        assert_eq!(s.quiescing_bfg, Some(1));
        assert!(s.syncing_bfg.is_none());
        assert_eq!(state.checkpoint_bfg(), 0);
        assert_eq!(sync_calls.load(Ordering::Acquire), 0);
        let phase = metrics.snapshot();
        assert_eq!(phase.checkpoint_quiesce_bfg, 1);
        assert_eq!(
            phase.checkpoint_quiesce_phase,
            crate::metrics::CheckpointQuiescePhase::Error as u64
        );
        // Clean shutdown still works.
        q.stop();
    }

    #[test]
    fn abort_while_waiting_for_sync_slot_keeps_error_and_does_not_notify() {
        let state = Arc::new(BfgStateMachine::new(0));
        let first = state.roll_to_quiescing();
        state.promote_to_syncing(first);
        assert_eq!(state.snapshot().syncing_bfg, Some(1));

        let sync_n = Arc::new(SyncNotifier::new());
        let metrics = noop_metrics();
        let notifier = Arc::new(QuiesceNotifier::new());
        let mut q = BfgQuiesceThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            Arc::clone(&sync_n),
            QuiesceParams {
                bfg_timeout_ms: 60_000,
            },
            Arc::clone(&metrics),
            disabled_faults(),
        );

        notifier.signal_force(state.open_bfg());
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline
            && metrics.snapshot().checkpoint_quiesce_phase
                != crate::metrics::CheckpointQuiescePhase::AwaitSync as u64
        {
            thread::sleep(Duration::from_millis(5));
        }
        let awaiting = metrics.snapshot();
        assert_eq!(awaiting.checkpoint_quiesce_bfg, 2);
        assert_eq!(
            awaiting.checkpoint_quiesce_phase,
            crate::metrics::CheckpointQuiescePhase::AwaitSync as u64
        );
        assert_eq!(state.snapshot().quiescing_bfg, Some(2));
        assert!(!sync_n.has_pending_wake());

        state.mark_aborted();
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline
            && metrics.snapshot().checkpoint_quiesce_phase
                != crate::metrics::CheckpointQuiescePhase::Error as u64
        {
            thread::sleep(Duration::from_millis(5));
        }
        let failed = metrics.snapshot();
        assert_eq!(failed.checkpoint_quiesce_bfg, 2);
        assert_eq!(
            failed.checkpoint_quiesce_phase,
            crate::metrics::CheckpointQuiescePhase::Error as u64
        );
        assert_eq!(state.snapshot().syncing_bfg, Some(1));
        assert_eq!(state.snapshot().quiescing_bfg, Some(2));
        assert!(!sync_n.has_pending_wake());

        q.stop();
        assert_eq!(
            metrics.snapshot().checkpoint_quiesce_phase,
            crate::metrics::CheckpointQuiescePhase::Error as u64,
            "shutdown must not erase the abort terminal state"
        );
    }

    #[test]
    fn shutdown_during_roll_does_not_promote_or_notify_sync() {
        let state = Arc::new(BfgStateMachine::new(0));
        let sync_n = Arc::new(SyncNotifier::new());
        let sync_calls = Arc::new(AtomicU64::new(0));
        let calls = Arc::clone(&sync_calls);
        let work: super::super::bfg_sync::SyncWorkFn = Arc::new(move |_| {
            calls.fetch_add(1, Ordering::AcqRel);
            Ok(())
        });
        let _sync = super::super::bfg_sync::BfgSyncThread::start(
            Arc::clone(&state),
            Arc::clone(&sync_n),
            work,
            noop_metrics(),
        );
        let notifier = Arc::new(QuiesceNotifier::new());
        // Pin BFG so the quiesce roll blocks in its inflight-drain wait.
        let guard = state.enter();
        let mut q = BfgQuiesceThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            Arc::clone(&sync_n),
            QuiesceParams { bfg_timeout_ms: 30 },
            noop_metrics(),
            disabled_faults(),
        );
        // Worker will fire its timer at ~30 ms and block on the drain.
        thread::sleep(Duration::from_millis(50));
        // Now shut down. Roll returns without advancing.
        q.stop();
        // The guard can drop now.
        drop(guard);
        // No sync should ever have been called.
        assert_eq!(sync_calls.load(Ordering::Acquire), 0);
        assert_eq!(state.checkpoint_bfg(), 0);
    }
}
