//! ZFS-TXG-clone Phase 4 quiesce thread.
//!
//! `TxgQuiesceThread` is the single producer of TXG rolls. Each cycle:
//!
//! 1. Wait `txg_timeout` (default 5 s, mirrors ZFS `zfs_txg_timeout`).
//!    `force_roll()` (used by `flush_with_gate` after Step 7) cuts the
//!    wait short. Future hooks: dirty-data threshold from the L2P
//!    buffer.
//! 2. `state.roll_to_quiescing()` — closes the current Open TXG to new
//!    commits, waits for in-flight commits to drop their `TxgGuard`s,
//!    then advances `open_txg`.
//! 3. `state.promote_to_syncing(txg)` — flip Quiescing → Syncing.
//! 4. `sync_notifier.notify()` — wake the [`super::txg_sync::TxgSyncThread`].
//!
//! Only one quiesce in flight at a time (single thread), so the
//! "at most one TXG in Quiescing" invariant is enforced by construction.
//!
//! Idempotent shutdown via `Drop` → `stop` → `join`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use crate::metrics::MetaMetrics;
use crate::testing::faults::{FaultController, FaultPoint};
use crate::txg::TxgStateMachine;
use crate::types::Txg;

use super::txg_sync::SyncNotifier;

#[derive(Clone, Copy, Debug)]
pub struct QuiesceParams {
    /// How long the worker waits between rolls when no force_roll fires
    /// (ZFS `zfs_txg_timeout`). Default 5000 ms.
    pub txg_timeout_ms: u64,
}

impl Default for QuiesceParams {
    fn default() -> Self {
        Self {
            txg_timeout_ms: 5000,
        }
    }
}

/// Producer/consumer wake handle shared between the quiesce worker and
/// any caller that wants to drive a roll immediately (e.g.
/// `flush_with_gate`).
pub struct QuiesceNotifier {
    force: Mutex<bool>,
    cv: Condvar,
}

impl QuiesceNotifier {
    pub fn new() -> Self {
        Self {
            force: Mutex::new(false),
            cv: Condvar::new(),
        }
    }

    /// Tell the quiesce worker to roll the current Open TXG immediately.
    /// Returns the TXG number that the worker WILL move to Quiescing.
    /// The caller can `state.wait_until_synced(returned_txg)` to block
    /// until the manifest commit for that TXG is durable.
    ///
    /// `target_open_txg` is the caller's read of `state.open_txg()` at
    /// call time — pass `state.open_txg()`. Returned for echo so the
    /// caller doesn't need to read it twice.
    pub fn signal_force(&self, target_open_txg: Txg) -> Txg {
        let mut force = self.force.lock();
        *force = true;
        self.cv.notify_one();
        target_open_txg
    }

    /// Park up to `timeout` waiting for a force-roll notification.
    /// Returns `true` if a force notification was consumed; `false` if
    /// the timeout elapsed.
    fn wait_with_timeout(&self, timeout: Duration) -> bool {
        let mut force = self.force.lock();
        if *force {
            *force = false;
            return true;
        }
        let res = self.cv.wait_for(&mut force, timeout);
        let woken = *force;
        *force = false;
        // `parking_lot::Condvar::wait_for` returns `WaitTimeoutResult`;
        // either branch (woken vs timeout) cleared the flag above.
        let _ = res;
        woken
    }

    fn wake_all_for_shutdown(&self) {
        let mut force = self.force.lock();
        *force = true;
        self.cv.notify_all();
    }
}

impl Default for QuiesceNotifier {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TxgQuiesceThread {
    inner: Arc<Inner>,
    handle: Option<JoinHandle<()>>,
}

struct Inner {
    state: Arc<TxgStateMachine>,
    notifier: Arc<QuiesceNotifier>,
    sync_notifier: Arc<SyncNotifier>,
    params: QuiesceParams,
    shutdown: AtomicBool,
    #[allow(dead_code)]
    metrics: Arc<MetaMetrics>,
    faults: Arc<FaultController>,
}

impl TxgQuiesceThread {
    pub fn start(
        state: Arc<TxgStateMachine>,
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
            .name("metadb-txg-quiesce".into())
            .spawn(move || run_worker(worker))
            .expect("metadb: failed to spawn txg quiesce worker");
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

impl Drop for TxgQuiesceThread {
    fn drop(&mut self) {
        self.stop();
    }
}

fn run_worker(inner: Arc<Inner>) {
    let timeout = Duration::from_millis(inner.params.txg_timeout_ms.max(1));
    while !inner.shutdown.load(Ordering::Acquire) {
        // Wait either for the txg_timeout to elapse or a force-roll
        // notification. Either path drops into the same roll body.
        inner.notifier.wait_with_timeout(timeout);
        if inner.shutdown.load(Ordering::Acquire) {
            break;
        }
        // Roll → promote → notify sync. roll_to_quiescing handles its own
        // shutdown check (returns current open_txg without advancing) so
        // we re-check shutdown afterwards before promoting.
        let txg = inner.state.roll_to_quiescing();
        if inner.shutdown.load(Ordering::Acquire) {
            break;
        }
        // If roll observed shutdown it returns `open_txg` without flipping
        // states — promote would panic in that case. Re-fetch snapshot to
        // be safe; if quiescing_txg isn't set, skip.
        let snapshot = inner.state.snapshot();
        if snapshot.quiescing_txg != Some(txg) {
            // Roll did not advance (shutdown raced).
            continue;
        }
        // ZFS-TXG-clone Phase 4 fault: between the open→quiescing flip
        // (in roll_to_quiescing) and promote_to_syncing. Soak runs
        // configure `Panic` here to crash mid-quiesce; recovery must
        // reconstruct from the durable manifest without observing
        // mid-state from a non-promoted TXG.
        if let Err(err) = inner.faults.inject(FaultPoint::TxgQuiesceMidway) {
            tracing::error!(error = %err, txg, "metadb: TxgQuiesceThread fault-injected midway; skipping promote");
            continue;
        }
        inner.state.promote_to_syncing(txg);
        inner.sync_notifier.notify();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    fn noop_metrics() -> Arc<MetaMetrics> {
        Arc::new(MetaMetrics::new())
    }

    fn disabled_faults() -> Arc<FaultController> {
        FaultController::disabled()
    }

    /// Stand-up a paired sync worker that just acks every notification,
    /// so the quiesce side sees full state machine transitions.
    fn spawn_ack_sync(
        state: Arc<TxgStateMachine>,
        sync_notifier: Arc<SyncNotifier>,
    ) -> super::super::txg_sync::TxgSyncThread {
        use super::super::txg_sync::{SyncWorkFn, TxgSyncThread};
        let work: SyncWorkFn = Arc::new(|_| Ok(()));
        TxgSyncThread::start(state, sync_notifier, work, noop_metrics())
    }

    #[test]
    fn force_roll_kicks_off_immediate_cycle() {
        let state = Arc::new(TxgStateMachine::new(0));
        let sync_n = Arc::new(SyncNotifier::new());
        let _sync = spawn_ack_sync(Arc::clone(&state), Arc::clone(&sync_n));
        let notifier = Arc::new(QuiesceNotifier::new());
        let mut q = TxgQuiesceThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            Arc::clone(&sync_n),
            QuiesceParams {
                txg_timeout_ms: 60_000, // never via timer in this test
            },
            noop_metrics(),
            disabled_faults(),
        );
        let target = state.open_txg();
        let ret = notifier.signal_force(target);
        assert_eq!(ret, target);
        // Sync should mark TXG `target` as synced (checkpoint_txg = target).
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && state.checkpoint_txg() < target {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(state.checkpoint_txg(), target);
        q.stop();
    }

    #[test]
    fn timer_drives_rolls_on_idle() {
        let state = Arc::new(TxgStateMachine::new(0));
        let sync_n = Arc::new(SyncNotifier::new());
        let _sync = spawn_ack_sync(Arc::clone(&state), Arc::clone(&sync_n));
        let notifier = Arc::new(QuiesceNotifier::new());
        let mut q = TxgQuiesceThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            Arc::clone(&sync_n),
            QuiesceParams { txg_timeout_ms: 30 }, // fast for tests
            noop_metrics(),
            disabled_faults(),
        );
        // Within ~150 ms we should see at least 3 TXGs sync'd.
        thread::sleep(Duration::from_millis(180));
        assert!(
            state.checkpoint_txg() >= 3,
            "expected >= 3 syncs in 180 ms with 30ms timer, got {}",
            state.checkpoint_txg()
        );
        q.stop();
    }

    #[test]
    fn force_roll_with_inflight_commit_blocks_until_drop() {
        let state = Arc::new(TxgStateMachine::new(0));
        let sync_n = Arc::new(SyncNotifier::new());
        let _sync = spawn_ack_sync(Arc::clone(&state), Arc::clone(&sync_n));
        let notifier = Arc::new(QuiesceNotifier::new());
        let mut q = TxgQuiesceThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            Arc::clone(&sync_n),
            QuiesceParams {
                txg_timeout_ms: 60_000,
            },
            noop_metrics(),
            disabled_faults(),
        );
        // Pin TXG 1 with an open guard, then force-roll.
        let guard = state.enter();
        let target = state.open_txg();
        notifier.signal_force(target);
        // Sync cannot finish TXG 1 until the guard drops.
        thread::sleep(Duration::from_millis(50));
        assert_eq!(state.checkpoint_txg(), 0);
        drop(guard);
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline && state.checkpoint_txg() < target {
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(state.checkpoint_txg(), target);
        q.stop();
    }

    #[test]
    fn stop_idempotent_and_quick() {
        let state = Arc::new(TxgStateMachine::new(0));
        let sync_n = Arc::new(SyncNotifier::new());
        let _sync = spawn_ack_sync(Arc::clone(&state), Arc::clone(&sync_n));
        let notifier = Arc::new(QuiesceNotifier::new());
        let mut q = TxgQuiesceThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            Arc::clone(&sync_n),
            QuiesceParams::default(),
            noop_metrics(),
            disabled_faults(),
        );
        let started = Instant::now();
        q.stop();
        // Should join in well under the 5 s txg_timeout because shutdown
        // wakes the wait.
        assert!(started.elapsed() < Duration::from_millis(500));
        q.stop(); // idempotent
    }

    #[test]
    fn fault_midway_skips_promote_and_sync_notify() {
        // FaultPoint::TxgQuiesceMidway fires between roll_to_quiescing
        // and promote_to_syncing. With FaultAction::Error the worker
        // logs and continues to the next iteration without notifying
        // the sync thread — the open_txg has advanced but the now-
        // Quiescing TXG stays Quiescing (it'll be promoted on a
        // subsequent successful cycle).
        use crate::testing::faults::FaultAction;
        let state = Arc::new(TxgStateMachine::new(0));
        let sync_n = Arc::new(SyncNotifier::new());
        let sync_calls = Arc::new(AtomicU64::new(0));
        let calls = Arc::clone(&sync_calls);
        let work: super::super::txg_sync::SyncWorkFn = Arc::new(move |_| {
            calls.fetch_add(1, Ordering::AcqRel);
            Ok(())
        });
        let _sync = super::super::txg_sync::TxgSyncThread::start(
            Arc::clone(&state),
            Arc::clone(&sync_n),
            work,
            noop_metrics(),
        );
        let faults = FaultController::new();
        faults.install(FaultPoint::TxgQuiesceMidway, 1, FaultAction::Error);
        let notifier = Arc::new(QuiesceNotifier::new());
        let mut q = TxgQuiesceThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            Arc::clone(&sync_n),
            QuiesceParams {
                txg_timeout_ms: 60_000,
            },
            noop_metrics(),
            Arc::clone(&faults),
        );
        notifier.signal_force(state.open_txg());
        // Wait for the fault to fire.
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline && faults.hits(FaultPoint::TxgQuiesceMidway) == 0 {
            thread::sleep(Duration::from_millis(5));
        }
        assert!(faults.fired(FaultPoint::TxgQuiesceMidway));
        // Slot 1 should be in Quiescing (roll advanced) but never
        // promoted to Syncing, and the sync worker was never called.
        thread::sleep(Duration::from_millis(20));
        let s = state.snapshot();
        assert_eq!(s.quiescing_txg, Some(1));
        assert!(s.syncing_txg.is_none());
        assert_eq!(state.checkpoint_txg(), 0);
        assert_eq!(sync_calls.load(Ordering::Acquire), 0);
        // Clean shutdown still works.
        q.stop();
    }

    #[test]
    fn shutdown_during_roll_does_not_promote_or_notify_sync() {
        let state = Arc::new(TxgStateMachine::new(0));
        let sync_n = Arc::new(SyncNotifier::new());
        let sync_calls = Arc::new(AtomicU64::new(0));
        let calls = Arc::clone(&sync_calls);
        let work: super::super::txg_sync::SyncWorkFn = Arc::new(move |_| {
            calls.fetch_add(1, Ordering::AcqRel);
            Ok(())
        });
        let _sync = super::super::txg_sync::TxgSyncThread::start(
            Arc::clone(&state),
            Arc::clone(&sync_n),
            work,
            noop_metrics(),
        );
        let notifier = Arc::new(QuiesceNotifier::new());
        // Pin TXG so the quiesce roll blocks in its inflight-drain wait.
        let guard = state.enter();
        let mut q = TxgQuiesceThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            Arc::clone(&sync_n),
            QuiesceParams { txg_timeout_ms: 30 },
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
        assert_eq!(state.checkpoint_txg(), 0);
    }
}
