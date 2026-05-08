//! Refcount staging overlay (priority 3).
//!
//! A per-shard sealed-page overlay that the background drainer thread
//! populates outside `apply_gate.write()`. `RcShard::stage` / `get`
//! consult the overlay before falling back to the on-disk array, so a
//! page absorbed by the drainer appears immediately in the read path
//! without waiting for the next checkpoint.
//!
//! At `begin_checkpoint`, the (small) `delta_active` + `delta_draining`
//! catch-up is applied to the overlay under `apply_gate.write()` and
//! the overlay is atomically harvested into a `RcCheckpoint`. Heavy
//! work (cache misses, page clone, apply, seal) all moves off the
//! commit-blocking gate.
//!
//! Overlay is RAM-only — gone on crash. WAL replay re-runs `stage()`,
//! which feeds `delta_active`; the drainer (started after replay
//! completes, never during) warms the overlay from there. Replay-skip
//! semantics are unchanged because `effective_page_lsn` falls back to
//! on-disk `page_lsn` when the overlay is empty.
//!
//! See `metadb/src/refcount/CLAUDE.md`-equivalent doc in the priority-3
//! plan file `compiled-rolling-porcupine.md`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::error::Result;
use crate::metrics::MetaMetrics;
use crate::page::Page;
use crate::page_store::PageStore;
use crate::types::PageId;

/// One sealed data page captured by the drainer. Mirrors
/// `array::StagedPage` field-for-field; kept as a distinct type so the
/// overlay can store it independently of the priority-1 in-gate path.
#[derive(Clone)]
pub struct OverlayEntry {
    pub page_id: PageId,
    pub page_idx: usize,
    pub sealed: Arc<Page>,
    /// True if `page_id` was freshly allocated by the drainer for a
    /// previously-hole `page_idx`. Used by abort to feed the id back
    /// to the per-shard `PagePool` instead of the global free list.
    pub is_fresh: bool,
}

/// Per-shard staging overlay. Reads check overlay first; the drainer
/// publishes new sealed pages here; `begin_checkpoint` atomically
/// harvests the entire overlay.
pub struct StagingOverlay {
    pages: Mutex<HashMap<usize /*page_idx*/, OverlayEntry>>,
    /// Atomic peek of `pages.len()` — read by the backpressure check
    /// in `begin_checkpoint` without taking the mutex on the hot path.
    size: AtomicUsize,
}

impl StagingOverlay {
    pub fn new() -> Self {
        Self {
            pages: Mutex::new(HashMap::new()),
            size: AtomicUsize::new(0),
        }
    }

    /// Look up the staged entry for a `page_idx` if any. Clones the
    /// `Arc<Page>` under the mutex; reader unlocks immediately after.
    pub fn get(&self, page_idx: usize) -> Option<OverlayEntry> {
        self.pages.lock().get(&page_idx).cloned()
    }

    /// Insert / replace the staged entry for one `page_idx`.
    pub fn insert(&self, entry: OverlayEntry) {
        let mut pages = self.pages.lock();
        let was_present = pages.insert(entry.page_idx, entry).is_some();
        if !was_present {
            self.size.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Insert/replace many entries under a single mutex acquire. Used by
    /// the drainer's transition 2 (publish to overlay) so the publish is
    /// atomic relative to readers — every entry from a cycle becomes
    /// visible together, no in-flight invisible window.
    pub fn bulk_insert(&self, entries: impl IntoIterator<Item = OverlayEntry>) {
        let mut pages = self.pages.lock();
        let mut new = 0usize;
        for entry in entries {
            let was_present = pages.insert(entry.page_idx, entry).is_some();
            if !was_present {
                new += 1;
            }
        }
        if new > 0 {
            self.size.fetch_add(new, Ordering::Relaxed);
        }
    }

    /// Read-only snapshot of all entries. Cheap because each entry is
    /// `Arc<Page>` clone. The drainer uses this to fold a new batch on
    /// top of the previous overlay state without taking the lock for
    /// the duration of the heavy build.
    pub fn snapshot(&self) -> HashMap<usize, OverlayEntry> {
        self.pages.lock().clone()
    }

    /// Atomic harvest — replace the overlay with an empty map and
    /// return the old contents to the caller. Used by
    /// `begin_checkpoint` to take ownership of the staged pages.
    pub fn take(&self) -> HashMap<usize, OverlayEntry> {
        let mut pages = self.pages.lock();
        let taken = std::mem::take(&mut *pages);
        self.size.store(0, Ordering::Relaxed);
        taken
    }

    /// Lock-free size estimate. Off by at most one mutator at any
    /// instant; used for backpressure decisions where exact precision
    /// is not required.
    pub fn approx_size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }
}

/// Per-shard pool of pre-allocated `PageId`s the drainer hands out
/// when sealing a page for a previously-hole `page_idx`. Refilled in
/// batches via `PageStore::allocate_run` to amortise the global
/// `page_store.inner` lock; a single peak cycle (~21k fresh pages
/// across 16 shards) collapses from O(N) lock acquisitions to
/// `ceil(N / refill_size)` per shard.
pub struct PagePool {
    page_store: Arc<PageStore>,
    free: Vec<PageId>,
    refill_count: usize,
    metrics: Arc<MetaMetrics>,
}

impl PagePool {
    pub fn new(page_store: Arc<PageStore>, refill_count: usize, metrics: Arc<MetaMetrics>) -> Self {
        Self {
            page_store,
            free: Vec::new(),
            refill_count: refill_count.max(1),
            metrics,
        }
    }

    /// Pop one page id, refilling from `PageStore::allocate_batch` if
    /// the pool is empty. Refill metric increments once per refill so
    /// dashboards can watch page-allocator pressure.
    pub fn alloc(&mut self) -> Result<PageId> {
        if let Some(pid) = self.free.pop() {
            return Ok(pid);
        }
        let mut batch = self.page_store.allocate_batch(self.refill_count)?;
        self.metrics.record_rc_drainer_pool_refill();
        // `allocate_batch` returns a stack-ordered Vec; pop the first
        // we hand out, push the rest into our pool.
        let head = batch.pop().ok_or_else(|| {
            crate::error::MetaDbError::InvalidArgument("allocate_batch returned empty vec".into())
        })?;
        self.free.append(&mut batch);
        Ok(head)
    }

    /// Return an unused page id back to the pool. Called by the abort
    /// path so a checkpoint that fails after the drainer allocated
    /// pages does not leak them to verify-time orphan reclamation.
    pub fn release(&mut self, pid: PageId) {
        self.free.push(pid);
    }

    /// Drain pool contents into a Vec for caller-driven reclamation
    /// (e.g., `Db::drop` returning leftover ids to `PageStore`'s
    /// global free list). The pool itself ends up empty.
    pub fn drain(&mut self) -> Vec<PageId> {
        std::mem::take(&mut self.free)
    }
}

/// Drainer thread state shared between the spawning shard and the
/// worker. The shard signals the worker via the condition variable;
/// the worker sets `running=false` on shutdown so the shard's
/// `JoinHandle` join is safe.
pub struct DrainerState {
    /// Drainer thread should exit at the next park boundary.
    pub shutdown: AtomicBool,
    /// `begin_checkpoint` set this to interrupt an in-flight cycle (or
    /// prevent a new one from starting) so the in-gate phase can take
    /// a consistent snapshot. The worker observes it at swap-time and
    /// at cycle-completion; it sets `preempt_done=true` to wake the
    /// caller waiting on `cv`.
    pub preempt: AtomicBool,
    pub preempt_done: AtomicBool,
    /// True while the worker is actively running a cycle (between
    /// swapping `delta_active` and publishing the overlay). The
    /// caller of `preempt_and_wait` polls this together with
    /// `preempt_done` to know when the cycle has fully wound down.
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

    /// Wake the worker, e.g., from a `stage()` call that pushed
    /// `delta_active` past `threshold_entries`. Cheap; safe to call
    /// from the commit-apply hot path.
    pub fn notify(&self) {
        self.cv.notify_one();
    }
}

/// Handle to a per-shard drainer thread. The shard owns one. Drop
/// joins the thread (after signalling shutdown).
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
    /// Safe to call from `begin_checkpoint` while the caller holds
    /// `apply_gate.write()`.
    pub fn preempt_and_wait(&self) -> Duration {
        let started = Instant::now();
        self.state.preempt.store(true, Ordering::Release);
        self.state.preempt_done.store(false, Ordering::Release);
        // Wake the worker so it can observe `preempt` even if it was
        // parked on the timer.
        self.state.cv.notify_all();
        // Wait until the worker either finishes its cycle (sets
        // `preempt_done=true`) or confirms it was idle when preempt
        // was set (also sets `preempt_done=true` immediately).
        let mut guard = self.state.mu.lock();
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

    /// Re-arm the drainer after `begin_checkpoint` has finished its
    /// in-gate work. Clears `preempt` so the next cycle can start.
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
