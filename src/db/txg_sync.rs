//! ZFS-TXG-clone Phase 4 sync thread.
//!
//! `TxgSyncThread` is the worker driven by [`super::txg_quiesce::TxgQuiesceThread`].
//! Each cycle persists one Syncing TXG by invoking the `sync_work`
//! callback (the body of [`crate::db::Db::run_sync_cycle`]) which:
//!
//! 1. Drains the syncing slot of every L2P shard into the tree
//!    (`L2pBuffer::take_syncing_slot(txg)`) — the slot is frozen by
//!    quiesce so no commit can insert into it.
//! 2. Performs the refcount + dead-list checkpoint (per-shard
//!    `begin_checkpoint`).
//! 3. Page IO + WAL fsync barrier.
//! 4. Manifest commit (briefly holds `apply_gate.write`).
//!
//! After a successful callback the worker calls
//! [`TxgStateMachine::mark_synced`] to advance `checkpoint_txg` and
//! wake any [`TxgStateMachine::wait_until_synced`] waiters (in
//! particular `flush_with_gate`'s threaded path).
//!
//! Concurrency:
//! - Single worker thread, parked on a `Condvar`. A wake from
//!   `TxgQuiesceThread` (or shutdown) is the only event the worker
//!   responds to.
//! - The wake is **edge-triggered + level-checked**: every wake-up the
//!   worker re-reads `txg.snapshot().syncing_txg` to find work, so a
//!   stale notification (no Syncing TXG) is a cheap no-op.
//! - `Drop` issues a shutdown signal and joins the worker.
//!
//! This module also owns [`compact_drain_into_tree`], the per-shard
//! buffer → tree fold helper that both the `TxgSyncThread`'s
//! per-slot drain and the inline
//! [`crate::db::Db::force_compact_l2p_buffers`] path share.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use parking_lot::{Condvar, Mutex};

use crate::error::Result;
use crate::metrics::MetaMetrics;
use crate::txg::TxgStateMachine;
use crate::types::Txg;

/// One leaf's worth of fold work, prepared off-lock by
/// [`build_drain_plan`]. Inserts are pre-sorted by lba and all share
/// the same leaf, so [`apply_drain_ops`] can hand them straight to
/// `insert_leaf_run_at_lsn_deferred_finish` (one CoW per leaf).
pub(crate) struct LeafDrainOp {
    inserts: Vec<(u64, crate::paged::L2pValue)>,
    inserts_max_lsn: crate::types::Lsn,
    tombstones: Vec<(u64, crate::types::Lsn)>,
}

impl LeafDrainOp {
    pub(crate) fn entry_count(&self) -> usize {
        self.inserts.len() + self.tombstones.len()
    }
}

/// Group `draining` by leaf and sort, producing the fold plan in
/// leaf-idx order (determinism / page locality). Pure CPU on the
/// caller's snapshot — callers fold under `tree.write()`, so doing the
/// grouping (and freeing its transient maps) here keeps that
/// allocation churn outside the lock hold.
pub(crate) fn build_drain_plan(
    draining: &HashMap<u64, super::l2p_buffer::BufferEntry>,
) -> Vec<LeafDrainOp> {
    use crate::paged::format::LEAF_SHIFT;
    if draining.is_empty() {
        return Vec::new();
    }
    // (leaf_idx) -> (insert run, tombstone list)
    let mut by_leaf: HashMap<
        u64,
        (
            Vec<(u64, crate::paged::L2pValue, crate::types::Lsn)>,
            Vec<(u64, crate::types::Lsn)>,
        ),
    > = HashMap::with_capacity(draining.len() / 32 + 1);
    for (lba, entry) in draining {
        let leaf_idx = *lba >> LEAF_SHIFT;
        let bucket = by_leaf.entry(leaf_idx).or_default();
        if entry.tombstone {
            bucket.1.push((*lba, entry.lsn));
        } else {
            bucket.0.push((*lba, entry.value, entry.lsn));
        }
    }
    let mut leaf_indices: Vec<u64> = by_leaf.keys().copied().collect();
    leaf_indices.sort_unstable();
    let mut plan = Vec::with_capacity(leaf_indices.len());
    for leaf_idx in leaf_indices {
        let (mut inserts, tombstones) = by_leaf.remove(&leaf_idx).expect("leaf present");
        inserts.sort_unstable_by_key(|(lba, _, _)| *lba);
        // All inserts in this group share `leaf_idx`. Use max LSN of the
        // group as the run's stamp so any prior-LSN replay of these
        // entries is correctly suppressed by `page.generation >= lsn`.
        let inserts_max_lsn = inserts.iter().map(|(_, _, l)| *l).max().unwrap_or(0);
        plan.push(LeafDrainOp {
            inserts: inserts.iter().map(|(lba, v, _)| (*lba, *v)).collect(),
            inserts_max_lsn,
            tombstones,
        });
    }
    plan
}

/// Apply a (sub)slice of a [`build_drain_plan`] plan into `tree`.
/// Tombstones go through `delete_at_lsn_deferred_finish` individually
/// because no leaf-run delete API exists today; deletes are rare on
/// the L2P hot path so this is acceptable. Finalises deferred RC
/// deltas in `finish_batch_apply` at the end, so every chunk leaves
/// the tree + read overlay in a consistent state.
pub(crate) fn apply_drain_ops(
    tree: &mut crate::paged::PagedL2p,
    ops: &[LeafDrainOp],
    txg: Txg,
) -> Result<()> {
    // A3 cutover: the buffer-drain COW stages page-rc deltas into this
    // sync cycle's TXG slot (so the page-rc fold `begin_checkpoint(txg)`
    // captures them, on the SAME boundary as the L2P + PBA-rc folds).
    tree.set_current_txg(txg);
    for op in ops {
        if !op.inserts.is_empty() {
            tree.insert_leaf_run_at_lsn_deferred_finish(&op.inserts, op.inserts_max_lsn)?;
        }
        for &(lba, lsn) in &op.tombstones {
            tree.delete_at_lsn_deferred_finish(lba, lsn)?;
        }
    }
    tree.finish_batch_apply()?;
    Ok(())
}

/// Apply every `draining` entry into `tree` in one shot. Groups by
/// leaf so that inserts touching the same leaf share one CoW (matches
/// the laned bucket's `insert_leaf_run_at_lsn_deferred_finish`
/// optimisation).
///
/// Used by the "merge every slot" inline drain
/// (`force_compact_l2p_buffers`), whose callers run quiesced (no
/// tree-lock contention to bound). The per-TXG syncing-slot drain
/// instead builds the plan off-lock and folds it in bounded chunks —
/// see `Db::drain_one_syncing_shard`.
pub(crate) fn compact_drain_into_tree(
    tree: &mut crate::paged::PagedL2p,
    draining: &HashMap<u64, super::l2p_buffer::BufferEntry>,
    txg: Txg,
) -> Result<()> {
    if draining.is_empty() {
        return Ok(());
    }
    apply_drain_ops(tree, &build_drain_plan(draining), txg)
}

/// Callback that performs the actual per-TXG sync work for one cycle.
///
/// Returns `Ok(())` on success; the worker calls
/// [`TxgStateMachine::mark_synced`] after a successful invocation.
/// `Err` is logged and the TXG is left in Syncing state so a subsequent
/// notify can retry (a degraded mode used for transient IO errors).
pub type SyncWorkFn = Arc<dyn Fn(Txg) -> Result<()> + Send + Sync>;

/// Notifier shared between the quiesce side (producer of wake-ups) and
/// the sync worker (consumer). Edge-triggered with a level recheck
/// inside the worker loop.
pub struct SyncNotifier {
    wake: Mutex<bool>,
    cv: Condvar,
}

impl SyncNotifier {
    pub fn new() -> Self {
        Self {
            wake: Mutex::new(false),
            cv: Condvar::new(),
        }
    }

    /// Wake the sync worker. Multiple notifies between two wake-ups
    /// collapse into one.
    pub fn notify(&self) {
        let mut wake = self.wake.lock();
        *wake = true;
        self.cv.notify_one();
    }

    fn wait(&self) {
        let mut wake = self.wake.lock();
        while !*wake {
            self.cv.wait(&mut wake);
        }
        *wake = false;
    }

    fn wake_all_for_shutdown(&self) {
        let mut wake = self.wake.lock();
        *wake = true;
        self.cv.notify_all();
    }
}

impl Default for SyncNotifier {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TxgSyncThread {
    inner: Arc<Inner>,
    handle: Option<JoinHandle<()>>,
}

struct Inner {
    state: Arc<TxgStateMachine>,
    notifier: Arc<SyncNotifier>,
    sync_work: SyncWorkFn,
    shutdown: AtomicBool,
    #[allow(dead_code)]
    metrics: Arc<MetaMetrics>,
}

impl TxgSyncThread {
    /// Spawn the sync worker.
    ///
    /// `sync_work` is invoked by the worker thread once per notification
    /// (when a Syncing TXG is present). It must:
    ///
    /// - drain the L2P shards for the given TXG into the trees,
    /// - perform refcount checkpoint + page IO + WAL fsync,
    /// - briefly hold `apply_gate.write()` to commit the manifest
    ///   (updating both `checkpoint_lsn` and `checkpoint_txg`),
    ///
    /// after which this worker calls
    /// [`TxgStateMachine::mark_synced`] to advance the in-memory
    /// `checkpoint_txg` and notify ring-full / `wait_until_synced`
    /// waiters.
    pub fn start(
        state: Arc<TxgStateMachine>,
        notifier: Arc<SyncNotifier>,
        sync_work: SyncWorkFn,
        metrics: Arc<MetaMetrics>,
    ) -> Self {
        let inner = Arc::new(Inner {
            state,
            notifier,
            sync_work,
            shutdown: AtomicBool::new(false),
            metrics,
        });
        let worker = Arc::clone(&inner);
        let handle = thread::Builder::new()
            .name("metadb-txg-sync".into())
            .spawn(move || run_worker(worker))
            .expect("metadb: failed to spawn txg sync worker");
        Self {
            inner,
            handle: Some(handle),
        }
    }

    pub fn notifier(&self) -> Arc<SyncNotifier> {
        self.inner.notifier.clone()
    }

    pub fn stop(&mut self) {
        if self.inner.shutdown.swap(true, Ordering::Release) {
            // Already stopped.
            return;
        }
        self.inner.notifier.wake_all_for_shutdown();
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

impl Drop for TxgSyncThread {
    fn drop(&mut self) {
        self.stop();
    }
}

fn run_worker(inner: Arc<Inner>) {
    // Pin to the dedicated TxgSync CPU set (`l2p_compactor_cpus`). This
    // is the heavy drain+checkpoint worker; without pinning the kernel
    // co-locates it on the hot front-end / apply-lane CPUs and its
    // `write_dirty_pages` work steals cycles from the commit path. In
    // the threads-off model this work ran on the (pinned)
    // metadb-checkpoint thread, so binding here restores that placement.
    crate::affinity::bind_current(crate::affinity::ThreadRole::TxgSync, 0);
    while !inner.shutdown.load(Ordering::Acquire) {
        inner.notifier.wait();
        if inner.shutdown.load(Ordering::Acquire) {
            break;
        }
        // Level check: is there actually a Syncing TXG to process?
        let Some(txg) = inner.state.snapshot().syncing_txg else {
            continue;
        };
        match (inner.sync_work)(txg) {
            Ok(()) => {
                inner.state.mark_synced(txg);
            }
            Err(err) => {
                tracing::error!(error = %err, txg, "metadb: TxgSyncThread cycle failed; TXG stays Syncing");
                // Do not advance checkpoint_txg. A subsequent notify
                // will retry. The quiesce thread will eventually time
                // out and surface as a soak alarm.
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    fn noop_metrics() -> Arc<MetaMetrics> {
        Arc::new(MetaMetrics::new())
    }

    #[test]
    fn wakes_and_processes_syncing_txg() {
        let state = Arc::new(TxgStateMachine::new(0));
        let notifier = Arc::new(SyncNotifier::new());
        let processed = Arc::new(AtomicU64::new(0));
        let processed_clone = Arc::clone(&processed);
        let work: SyncWorkFn = Arc::new(move |txg| {
            processed_clone.store(txg, Ordering::Release);
            Ok(())
        });
        let mut sync = TxgSyncThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            work,
            noop_metrics(),
        );
        // Set up a Syncing TXG.
        let q = state.roll_to_quiescing();
        state.promote_to_syncing(q);
        notifier.notify();
        // Wait for worker.
        for _ in 0..100 {
            if state.checkpoint_txg() == q {
                break;
            }
            thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(state.checkpoint_txg(), q);
        assert_eq!(processed.load(Ordering::Acquire), q);
        sync.stop();
    }

    #[test]
    fn spurious_notify_with_no_syncing_txg_is_noop() {
        let state = Arc::new(TxgStateMachine::new(0));
        let notifier = Arc::new(SyncNotifier::new());
        let calls = Arc::new(AtomicU64::new(0));
        let calls_clone = Arc::clone(&calls);
        let work: SyncWorkFn = Arc::new(move |_txg| {
            calls_clone.fetch_add(1, Ordering::AcqRel);
            Ok(())
        });
        let mut sync = TxgSyncThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            work,
            noop_metrics(),
        );
        // No syncing_txg, just notify.
        notifier.notify();
        thread::sleep(Duration::from_millis(30));
        assert_eq!(calls.load(Ordering::Acquire), 0);
        assert_eq!(state.checkpoint_txg(), 0);
        sync.stop();
    }

    #[test]
    fn failed_sync_leaves_txg_in_syncing_state() {
        let state = Arc::new(TxgStateMachine::new(0));
        let notifier = Arc::new(SyncNotifier::new());
        let work: SyncWorkFn = Arc::new(|_txg| {
            Err(crate::error::MetaDbError::Corruption(
                "test-induced failure".into(),
            ))
        });
        let mut sync = TxgSyncThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            work,
            noop_metrics(),
        );
        let q = state.roll_to_quiescing();
        state.promote_to_syncing(q);
        notifier.notify();
        thread::sleep(Duration::from_millis(40));
        // checkpoint_txg did NOT advance because mark_synced was not called.
        assert_eq!(state.checkpoint_txg(), 0);
        // syncing_txg is still set so a retry can pick it up.
        assert_eq!(state.snapshot().syncing_txg, Some(q));
        sync.stop();
    }

    #[test]
    fn stop_joins_worker_cleanly() {
        let state = Arc::new(TxgStateMachine::new(0));
        let notifier = Arc::new(SyncNotifier::new());
        let work: SyncWorkFn = Arc::new(|_| Ok(()));
        let mut sync = TxgSyncThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            work,
            noop_metrics(),
        );
        sync.stop();
        // Calling stop a second time is a no-op (idempotent).
        sync.stop();
    }

    #[test]
    fn multiple_txg_cycles_advance_checkpoint() {
        let state = Arc::new(TxgStateMachine::new(0));
        let notifier = Arc::new(SyncNotifier::new());
        let work: SyncWorkFn = Arc::new(|_| Ok(()));
        let mut sync = TxgSyncThread::start(
            Arc::clone(&state),
            Arc::clone(&notifier),
            work,
            noop_metrics(),
        );
        for expected_cp in 1..=5u64 {
            let q = state.roll_to_quiescing();
            state.promote_to_syncing(q);
            notifier.notify();
            for _ in 0..100 {
                if state.checkpoint_txg() == expected_cp {
                    break;
                }
                thread::sleep(Duration::from_millis(5));
            }
            assert_eq!(state.checkpoint_txg(), expected_cp);
        }
        sync.stop();
    }
}
