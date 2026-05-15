//! Background reclaim of the `page_store.deferred_free` queue.
//!
//! Goal: keep deferred-free draining off the `flush_with_gate`
//! critical path. The in-line reclaim used to occupy 23 s of
//! `flush_total_max` on nvme-box (mostly NVMe writes for the
//! zero-stamped Free pages plus FALLOCATE_PUNCH_HOLE syscalls) and
//! monopolise the flush thread so the dispatcher couldn't start
//! the next flush. Moving reclaim to a background worker lets the
//! next flush run while reclaim catches up, raising overall flush
//! cadence without changing what reclaim does or its correctness
//! guarantees.
//!
//! ## Correctness
//!
//! The worker just calls `page_store.try_reclaim_limit(...)` and
//! invalidates the returned page-cache entries — the same two
//! operations the in-line path did. Order and ownership rules are
//! unchanged:
//!
//! 1. `try_reclaim_limit` writes the zero-filled Free pages, then
//!    extends `inner.free_list` with the reclaimed pids.
//! 2. We then `page_cache.invalidate(pid)` so any stale cached
//!    page bytes can't shadow the new content after the allocator
//!    hands the pid back out.
//!
//! Concurrent flushes still see consistent state — flush
//! allocators pop already-reclaimed pids from the free list,
//! deferred-free entries that haven't been reclaimed yet stay in
//! `deferred_free` and don't enter `free_list` until the worker
//! processes them. The only observable change is "when": instead
//! of "right at the end of this flush" it's "soon, on the
//! background thread."
//!
//! ## Lifecycle
//!
//! Started by `Db::start_async_reclaim()` after the rest of `Db`
//! is wired up. Stopped by `Db::drop` (or `Db::stop_async_reclaim`)
//! before `page_store` / `page_cache` are torn down. The worker
//! reads its own clones of `Arc<PageStore>` and `Arc<PageCache>`,
//! so no `Arc<Db>` cycle.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::cache::PageCache;
use crate::metrics::MetaMetrics;
use crate::page_store::PageStore;

#[derive(Clone, Copy, Debug)]
pub(super) struct AsyncReclaimParams {
    /// Cap on pages reclaimed per worker cycle. Larger means fewer
    /// cycles, longer per-cycle wall time and longer NVMe burst;
    /// smaller means more cycles, smoother but higher per-page
    /// overhead.
    pub max_pages_per_cycle: usize,
    /// Maximum time the worker parks between cycles when
    /// `deferred_free` is empty. Notifications from
    /// `flush_with_gate` cut this short via the condvar.
    pub idle_interval_ms: u64,
}

pub(super) struct AsyncReclaim {
    inner: Arc<AsyncReclaimInner>,
    handle: Option<JoinHandle<()>>,
}

struct AsyncReclaimInner {
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    metrics: Arc<MetaMetrics>,
    params: AsyncReclaimParams,
    shutdown: AtomicBool,
    /// Signalling state for `notify()` / worker wake-up. Bumped
    /// each `notify()`; the worker captures and resets it so it
    /// can detect signals raced past its previous wait.
    signal: Mutex<u64>,
    signal_cvar: Condvar,
    /// Last-completed cycle's wall time. Surfaced via metrics
    /// only — not part of correctness — but lets dashboards
    /// confirm the worker is actually running.
    last_cycle_us: AtomicU64,
}

impl AsyncReclaim {
    pub(super) fn start(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        metrics: Arc<MetaMetrics>,
        params: AsyncReclaimParams,
    ) -> Self {
        let inner = Arc::new(AsyncReclaimInner {
            page_store,
            page_cache,
            metrics,
            params,
            shutdown: AtomicBool::new(false),
            signal: Mutex::new(0),
            signal_cvar: Condvar::new(),
            last_cycle_us: AtomicU64::new(0),
        });
        let inner_thread = inner.clone();
        let handle = thread::Builder::new()
            .name("metadb-async-reclaim".into())
            .spawn(move || run_worker(inner_thread))
            .expect("metadb: failed to spawn async reclaim worker");
        Self {
            inner,
            handle: Some(handle),
        }
    }

    pub(super) fn notify(&self) {
        let mut sig = self.inner.signal.lock();
        *sig = sig.wrapping_add(1);
        self.inner.signal_cvar.notify_one();
    }

    pub(super) fn stop(&mut self) {
        self.inner.shutdown.store(true, Ordering::Release);
        // Kick the worker out of `wait_for` so it can observe shutdown
        // promptly instead of riding the next idle tick.
        self.inner.signal_cvar.notify_all();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for AsyncReclaim {
    fn drop(&mut self) {
        if self.handle.is_some() {
            self.stop();
        }
    }
}

fn run_worker(inner: Arc<AsyncReclaimInner>) {
    let idle = Duration::from_millis(inner.params.idle_interval_ms.max(1));
    let mut last_seen_signal = 0u64;
    while !inner.shutdown.load(Ordering::Acquire) {
        // ONE cycle per wakeup (notify or idle tick). Tight-looping
        // here would burn through NVMe bandwidth continuously and
        // starve foreground flush IO — `flush_with_gate` calls
        // `notify_async_reclaim` once per flush, so this matches
        // the inline-reclaim pace (one budget-sized chunk per
        // flush) while letting the flush thread return early.
        // Drain backlog naturally over many flush notifications,
        // not within a single worker burst.
        if inner.page_store.deferred_free_len() > 0 {
            let started = Instant::now();
            match inner
                .page_store
                .try_reclaim_limit(inner.params.max_pages_per_cycle)
            {
                Ok(outcome) => {
                    for pid in &outcome.reclaimed {
                        inner.page_cache.invalidate(*pid);
                    }
                    let cycle_us =
                        u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
                    inner.last_cycle_us.store(cycle_us, Ordering::Relaxed);
                    inner.metrics.record_async_reclaim_cycle(
                        outcome.selected,
                        outcome.reclaimed.len(),
                        started.elapsed(),
                    );
                }
                Err(err) => {
                    tracing::error!(error = %err, "metadb: async reclaim cycle failed");
                    // Persistent page-store errors fall through to
                    // the park below — same back-off as a normal
                    // empty deferred queue.
                }
            }
        }
        // Park until notified or until the idle interval elapses.
        let mut sig = inner.signal.lock();
        if *sig == last_seen_signal {
            inner.signal_cvar.wait_for(&mut sig, idle);
        }
        last_seen_signal = *sig;
    }
}
