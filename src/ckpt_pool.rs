//! Persistent worker pool for the two checkpoint fan-outs.
//!
//! # The measurement this replaces
//!
//! Both fan-outs used `std::thread::scope`: `drain_syncing_slot_into_trees`
//! spawns `parallel_l2p_drain_workers` threads for the L2P fold, and the rc
//! sample phase spawns one per selected refcount shard. They run **sequentially**
//! within a cycle (the `L2pFold` phase `?`-propagates before
//! `SampleWaitRefcount` starts), so on the box that was 8 + 8 = **16 threads
//! created and destroyed per checkpoint cycle**, ~7.2 creations/s.
//!
//! The cost is not the `clone3`. It is what jemalloc does when the thread
//! **exits**: `tsd_cleanup` -> `arena_decay` -> `pac_decay_all` purges every
//! dirty extent that thread accumulated, and a checkpoint worker accumulates a
//! lot (page folds, segment builds). Measured 2026-09-25 on nvme-box:
//! **~954 `madvise(MADV_DONTNEED)` per exiting thread, 3,815/s**, and because
//! all ~258 engine threads share one `mm`, each one broadcasts a TLB-shootdown
//! IPI to ~39 CPUs. That was 37.8% of the engine's residual madvise rate and
//! ~0.5 CPUs of the machine (memory `perf_inside_engine_first_cpu_ledger`,
//! `lv2_span_arena_kills_madvise_storm`).
//!
//! A persistent pool removes the exit, so the extents stay resident and get
//! reused by the next cycle instead of being purged and re-faulted.
//!
//! # Why rayon and not a channel of `Box<dyn FnOnce + 'static>`
//!
//! The L2P jobs borrow `&L2pShard`, `&vol.page_dead_list`, `&vol.page_live_list`
//! and `&MetaMetrics`. Making them `'static` would mean Arc-ifying those
//! structures — a refactor of the drain path, not a thread-pool change. rayon's
//! pool is **scoped**, so borrowed closures stay legal with no unsafe of ours,
//! and it is already a direct dependency of both onyx-storage and onyx-chunklet.
//!
//! ⛔ **Never call a `par_iter` outside [`CheckpointPool::install`].** rayon's
//! free functions use its GLOBAL pool, which sizes itself from
//! `available_parallelism` — 96 threads on this box, against a codebase whose
//! whole thread budget is a measured quantity. Every entry point here goes
//! through our own `ThreadPool`.

use std::sync::atomic::{AtomicBool, Ordering};

/// Runtime arm switch. `true` is the shipped behaviour.
///
/// Read per fan-out rather than captured at open, so the A/B alternates inside
/// ONE process: on the perf box an arm-per-restart comparison measures
/// run-order drift instead of the knob.
static POOL_ENABLED: AtomicBool = AtomicBool::new(true);

/// Whether the checkpoint fan-outs use the persistent pool **right now**.
pub fn checkpoint_pool_enabled() -> bool {
    POOL_ENABLED.load(Ordering::Relaxed)
}

/// Flip the persistent pool on a running engine (onyx IPC `metadb-ckpt-pool`).
///
/// Safe at any moment: it selects how the NEXT fan-out is executed, and both
/// paths have identical semantics (same jobs, same input-ordered results, same
/// bounded concurrency). A fan-out already in flight finishes on the path it
/// started on.
pub fn set_checkpoint_pool_enabled(enabled: bool) {
    POOL_ENABLED.store(enabled, Ordering::Relaxed);
}

/// A metadb-owned rayon pool, sized for the widest checkpoint fan-out.
pub(crate) struct CheckpointPool {
    pool: rayon::ThreadPool,
    threads: usize,
}

impl CheckpointPool {
    /// Build a pool of `threads` workers.
    ///
    /// `threads` must be the MAX of the two fan-outs' natural widths
    /// (`parallel_l2p_drain_workers` and the refcount shard count): they never
    /// overlap, so one pool serves both, but sizing it to the narrower one
    /// would make the wider phase run in two waves and lengthen the checkpoint.
    ///
    /// Workers are bound once, at start, by worker index. `bind_for_l2p_drain`
    /// takes a shard index today, but that index only selects anything in NUMA
    /// `pods` mode; confine mode returns `Inherit` and an explicit per-role
    /// layout returns one CPU *set* for every shard. So binding per worker is
    /// equivalent in both shipped modes, and in `pods` mode it is arguably
    /// better: a worker stays on one pod instead of rebinding per job.
    pub(crate) fn new(threads: usize) -> Option<Self> {
        let threads = threads.max(1);
        match rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|idx| format!("metadb-ckpt-{idx}"))
            .start_handler(|idx| crate::affinity::bind_for_l2p_drain(idx))
            .build()
        {
            Ok(pool) => Some(Self { pool, threads }),
            Err(err) => {
                // Not fatal: the callers keep their `std::thread::scope` path,
                // which is what every release before this one used.
                tracing::warn!(
                    threads,
                    error = %err,
                    "checkpoint worker pool unavailable; falling back to scoped threads"
                );
                None
            }
        }
    }

    pub(crate) fn threads(&self) -> usize {
        self.threads
    }

    /// Run `op` on this pool, so any nested rayon call inside it uses these
    /// workers and not the global pool.
    pub(crate) fn install<R: Send>(&self, op: impl FnOnce() -> R + Send) -> R {
        self.pool.install(op)
    }
}
