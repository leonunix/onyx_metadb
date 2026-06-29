//! Background-drainer preempt/resume primitive.
//!
//! [`DrainerState`] + [`DrainerHandle`] are a generic "park a background
//! worker, let a checkpoint preempt it, then resume it" pair. The
//! refcount shard no longer runs a background drainer (rc folds inline
//! per-BFG; see [`crate::refcount::shard`]), but the async dedup-index
//! drainer ([`crate::dedup::drainer`]) reuses this proven primitive, so
//! it lives here.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Instant;

use parking_lot::{Condvar, Mutex};

/// Drainer thread state shared between the spawning owner and the
/// worker. The owner signals the worker via the condition variable;
/// the worker sets `shutdown`-driven exit so the `JoinHandle` join is
/// safe.
pub struct DrainerState {
    /// Drainer thread should exit at the next park boundary.
    pub shutdown: AtomicBool,
    /// A checkpoint set this to interrupt an in-flight cycle (or prevent
    /// a new one from starting) so the in-gate phase can take a
    /// consistent snapshot. The worker observes it at swap-time and at
    /// cycle-completion; it sets `preempt_done=true` to wake the caller
    /// waiting on `cv`.
    pub preempt: AtomicBool,
    pub preempt_done: AtomicBool,
    /// True while the worker is actively running a cycle. The caller of
    /// `preempt_and_wait` polls this together with `preempt_done` to know
    /// when the cycle has fully wound down.
    pub in_cycle: AtomicBool,
    pub cv: Condvar,
    pub mu: Mutex<()>,
}

impl DrainerState {
    pub fn new() -> Self {
        Self {
            shutdown: AtomicBool::new(false),
            preempt: AtomicBool::new(false),
            preempt_done: AtomicBool::new(false),
            in_cycle: AtomicBool::new(false),
            cv: Condvar::new(),
            mu: Mutex::new(()),
        }
    }

    /// Wake the worker, e.g., from a stage call that pushed staging past
    /// its threshold. Cheap; safe to call from the commit-apply hot path.
    pub fn notify(&self) {
        self.cv.notify_one();
    }
}

impl Default for DrainerState {
    fn default() -> Self {
        Self::new()
    }
}

/// Handle to a per-owner drainer thread. Drop joins the thread (after
/// signalling shutdown).
pub struct DrainerHandle {
    state: Arc<DrainerState>,
    join: Option<JoinHandle<()>>,
}

impl DrainerHandle {
    /// Construct a handle that owns the running thread + shared state.
    pub fn new(state: Arc<DrainerState>, join: JoinHandle<()>) -> Self {
        Self {
            state,
            join: Some(join),
        }
    }

    pub fn state(&self) -> &Arc<DrainerState> {
        &self.state
    }

    /// Set preempt; wait for the worker to finish any in-flight cycle.
    /// Returns the wall-time spent waiting (for metric recording).
    pub fn preempt_and_wait(&self) -> std::time::Duration {
        let started = Instant::now();
        // Lock-then-store ordering is load-bearing. The worker's
        // `preempt_done.store(true)` at the head of its preempt handler
        // runs under `self.state.mu`. If we set `preempt_done=false`
        // outside the mutex, the two stores race: when the false-store
        // wins ordering against the worker's earlier true-store, the
        // caller observes `false` forever even though the worker has
        // already entered its `while preempt { cv.wait }` parking loop
        // and won't run another preempt handler.
        let mut guard = self.state.mu.lock();
        self.state.preempt.store(true, Ordering::Release);
        self.state.preempt_done.store(false, Ordering::Release);
        // Wake the worker so it can observe `preempt` even if it was
        // parked on the timer.
        self.state.cv.notify_all();
        // Wait until the worker either finishes its cycle (sets
        // `preempt_done=true`) or confirms it was idle when preempt was
        // set (also sets `preempt_done=true` immediately).
        loop {
            if self.state.preempt_done.load(Ordering::Acquire)
                && !self.state.in_cycle.load(Ordering::Acquire)
            {
                break;
            }
            self.state.cv.wait(&mut guard);
        }
        started.elapsed()
    }

    /// Re-arm the drainer after a checkpoint has finished its in-gate
    /// work. Clears `preempt` so the next cycle can start.
    pub fn resume(&self) {
        self.state.preempt.store(false, Ordering::Release);
        self.state.preempt_done.store(false, Ordering::Release);
        self.state.cv.notify_all();
    }

    /// Signal shutdown and join the worker. Idempotent.
    pub fn shutdown(&mut self) {
        self.state.shutdown.store(true, Ordering::Release);
        self.state.cv.notify_all();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for DrainerHandle {
    fn drop(&mut self) {
        self.shutdown();
    }
}
