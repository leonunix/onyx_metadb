//! Background driver for Phase 5 FreePbas-emitting Lineage GC.
//!
//! [`Db::run_lineage_gc_cycle_inner`](super::Db) is the only path that
//! surfaces dead LV3 PBAs to onyx: per advancing volume it commits a
//! `FreePbas` record (via `commit_free_pbas` → the `FreedPbas` sink) and
//! then truncates the dead-list chain head. The `async_reclaim` worker
//! deliberately holds no `Arc<Db>` and so cannot emit FreePbas; before
//! this module the FreePbas driver had **no production trigger at all**
//! (only `test_run_lineage_gc_cycle`), so dead-list chains grew without
//! bound and `gc_lineage_freed_blocks` stayed 0 under sustained
//! overwrite.
//!
//! ## Concurrency
//!
//! This worker is the single production mutator of every volume's
//! `dead_list_head_pid` (the GC execute phase re-acquires
//! `apply_gate.write()` to serialise against flush). It mirrors the
//! [`crate::db::txg_sync::TxgSyncThread`] `Weak<Db>` pattern so the
//! thread never extends `Db`'s lifetime: `Db::drop` calls
//! [`LineageGcWorker::stop`] (joining the thread) before `page_store` /
//! refcount / dedup teardown, and the strong refcount the caller holds
//! keeps `weak.upgrade()` valid for every cycle that races shutdown.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use super::Db;

#[derive(Clone, Copy, Debug)]
pub(crate) struct LineageGcParams {
    /// Idle park between wakes when nothing more can advance.
    pub interval_ms: u64,
    /// Cycles per wake (each advances ≤1 dead-list segment per volume).
    pub max_cycles_per_wake: usize,
}

pub(crate) struct LineageGcWorker {
    shutdown: Arc<AtomicBool>,
    wake: Arc<(Mutex<bool>, Condvar)>,
    handle: Option<JoinHandle<()>>,
}

impl LineageGcWorker {
    pub(crate) fn start(weak_db: Weak<Db>, params: LineageGcParams) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let wake = Arc::new((Mutex::new(false), Condvar::new()));
        let sd = shutdown.clone();
        let wk = wake.clone();
        let handle = thread::Builder::new()
            .name("metadb-lineage-gc".into())
            .spawn(move || run_worker(weak_db, sd, wk, params))
            .expect("metadb: failed to spawn lineage GC worker");
        Self {
            shutdown,
            wake,
            handle: Some(handle),
        }
    }

    /// Wake the worker early (e.g. after a flush appended a dead-list
    /// segment). Idempotent — the condvar coalesces signals.
    pub(crate) fn notify(&self) {
        let (lock, cv) = &*self.wake;
        *lock.lock() = true;
        cv.notify_one();
    }

    pub(crate) fn stop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        let (lock, cv) = &*self.wake;
        *lock.lock() = true;
        cv.notify_all();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for LineageGcWorker {
    fn drop(&mut self) {
        if self.handle.is_some() {
            self.stop();
        }
    }
}

fn run_worker(
    weak_db: Weak<Db>,
    shutdown: Arc<AtomicBool>,
    wake: Arc<(Mutex<bool>, Condvar)>,
    params: LineageGcParams,
) {
    let idle = Duration::from_millis(params.interval_ms.max(1));
    let max_cycles = params.max_cycles_per_wake.max(1);
    while !shutdown.load(Ordering::Acquire) {
        // Drive GC cycles until no volume's head advances (caught up) or
        // the per-wake budget is hit. Each cycle advances at most one
        // dead-list segment per volume, so a backlog drains over several
        // iterations without one wake monopolising `apply_gate.write()`.
        let mut total_advanced = 0usize;
        {
            let Some(db) = weak_db.upgrade() else {
                return;
            };
            for _ in 0..max_cycles {
                if shutdown.load(Ordering::Acquire) {
                    break;
                }
                match db.run_lineage_gc_cycle_inner() {
                    Ok(0) => break,
                    Ok(n) => total_advanced += n,
                    Err(err) => {
                        tracing::warn!(error = %err, "metadb: lineage GC cycle failed");
                        break;
                    }
                }
            }
            // Drop the strong ref before parking so the worker never
            // extends `Db`'s lifetime across the sleep.
        }
        if total_advanced > 0 {
            tracing::debug!(
                segments_advanced = total_advanced,
                "metadb: lineage GC wake drained dead-list segments"
            );
        }

        // Park until notified or the idle interval elapses.
        let (lock, cv) = &*wake;
        let mut signaled = lock.lock();
        if !*signaled && !shutdown.load(Ordering::Acquire) {
            cv.wait_for(&mut signaled, idle);
        }
        *signaled = false;
    }
}
