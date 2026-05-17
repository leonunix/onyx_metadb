//! Background compactor for the B2 L2P buffer path. Walks every L2P
//! shard, swaps each shard's `active` map into `draining` when soft
//! trigger conditions are met, applies the drained entries to the
//! on-disk paged radix tree, republishes the read view, and stamps
//! `buffer.compacted_lsn`.
//!
//! ## Trigger model
//!
//! - **Soft (size)**: when any shard's `active.len()` crosses
//!   `params.soft_entries`, the worker wakes immediately via
//!   `notify()`.
//! - **Time floor**: even without size trigger, the worker fires at
//!   least once every `params.max_interval_ms`.
//!
//! ## Step ordering (load-bearing)
//!
//! Per shard, per cycle:
//! 1. `swap_for_compaction()` — atomic swap `active → draining`
//! 2. `tree.write()` — exclusive tree mutation
//! 3. iterate `draining` entries, call `tree.insert_at_lsn` (or
//!    `tree.delete_at_lsn` for tombstones)
//! 4. `publish_l2p_read_view(shard, &tree)` — make tree state visible
//! 5. drop `tree.write()`
//! 6. `buffer.finish_compaction(max_lsn)` — drop draining + stamp
//!    `compacted_lsn`
//!
//! Steps 4 and 6 must run in this order: clearing draining before
//! publish leaves a reader unable to find an entry that has been
//! removed from draining but is not yet visible through the published
//! read view. See [`crate::db::l2p_buffer`].

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex, RwLock};

use crate::affinity::{self, ThreadRole};
use crate::metrics::MetaMetrics;
use crate::types::VolumeOrdinal;

use super::Volume;
use super::apply::publish_l2p_read_view;

/// Apply every `draining` entry into `tree`. Groups by leaf so that
/// inserts touching the same leaf share one CoW (matches the laned
/// bucket's `insert_leaf_run_at_lsn_deferred_finish` optimisation).
/// Tombstones go through `delete_at_lsn_deferred_finish` individually
/// because no leaf-run delete API exists today; deletes are rare on
/// the L2P hot path so this is acceptable. Finalises deferred RC
/// deltas in `finish_batch_apply` at the end.
pub(super) fn compact_drain_into_tree(
    tree: &mut crate::paged::PagedL2p,
    draining: &HashMap<u64, super::l2p_buffer::BufferEntry>,
) -> crate::Result<()> {
    use crate::paged::format::LEAF_SHIFT;
    if draining.is_empty() {
        return Ok(());
    }
    // (leaf_idx) -> (insert run sorted by lba, tombstone list)
    let mut by_leaf: HashMap<u64, (Vec<(u64, crate::paged::L2pValue, crate::types::Lsn)>, Vec<(u64, crate::types::Lsn)>)> =
        HashMap::with_capacity(draining.len() / 32 + 1);
    for (lba, entry) in draining {
        let leaf_idx = *lba >> LEAF_SHIFT;
        let bucket = by_leaf.entry(leaf_idx).or_default();
        if entry.tombstone {
            bucket.1.push((*lba, entry.lsn));
        } else {
            bucket.0.push((*lba, entry.value, entry.lsn));
        }
    }
    // Process in leaf-idx order for determinism / better page locality.
    let mut leaf_indices: Vec<u64> = by_leaf.keys().copied().collect();
    leaf_indices.sort_unstable();
    for leaf_idx in leaf_indices {
        let (mut inserts, tombstones) = by_leaf.remove(&leaf_idx).expect("leaf present");
        if !inserts.is_empty() {
            inserts.sort_unstable_by_key(|(lba, _, _)| *lba);
            // All inserts in this group share `leaf_idx`. Use max LSN
            // of the group as the run's stamp so any prior-LSN replay
            // of these entries is correctly suppressed by
            // `page.generation >= lsn`.
            let max_lsn = inserts.iter().map(|(_, _, l)| *l).max().unwrap_or(0);
            let entries: Vec<(u64, crate::paged::L2pValue)> =
                inserts.iter().map(|(lba, v, _)| (*lba, *v)).collect();
            tree.insert_leaf_run_at_lsn_deferred_finish(&entries, max_lsn)?;
        }
        for (lba, lsn) in tombstones {
            tree.delete_at_lsn_deferred_finish(lba, lsn)?;
        }
    }
    tree.finish_batch_apply()?;
    Ok(())
}

#[derive(Clone, Copy)]
pub(super) struct L2pCompactorParams {
    pub soft_entries: usize,
    pub max_interval_ms: u64,
}

pub(super) struct L2pCompactor {
    inner: Arc<L2pCompactorInner>,
    handle: Option<JoinHandle<()>>,
}

struct L2pCompactorInner {
    volumes: Arc<RwLock<HashMap<VolumeOrdinal, Arc<Volume>>>>,
    metrics: Arc<MetaMetrics>,
    params: L2pCompactorParams,
    shutdown: AtomicBool,
    wakeup: Mutex<bool>,
    wakeup_cvar: Condvar,
}

impl L2pCompactor {
    pub(super) fn start(
        volumes: Arc<RwLock<HashMap<VolumeOrdinal, Arc<Volume>>>>,
        metrics: Arc<MetaMetrics>,
        params: L2pCompactorParams,
    ) -> Self {
        let inner = Arc::new(L2pCompactorInner {
            volumes,
            metrics,
            params,
            shutdown: AtomicBool::new(false),
            wakeup: Mutex::new(false),
            wakeup_cvar: Condvar::new(),
        });
        let inner_thread = inner.clone();
        let handle = thread::Builder::new()
            .name("metadb-l2p-compactor".into())
            .spawn(move || run_worker(inner_thread))
            .expect("metadb: failed to spawn l2p compactor worker");
        Self {
            inner,
            handle: Some(handle),
        }
    }

    pub(super) fn notify(&self) {
        let mut wakeup = self.inner.wakeup.lock();
        *wakeup = true;
        self.inner.wakeup_cvar.notify_one();
    }

    pub(super) fn stop(&mut self) {
        self.inner.shutdown.store(true, Ordering::Release);
        let mut wakeup = self.inner.wakeup.lock();
        *wakeup = true;
        self.inner.wakeup_cvar.notify_all();
        drop(wakeup);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }

    /// Synchronously compact every shard's buffer into its tree. Used
    /// by `flush_with_gate` to make `buffer.compacted_lsn` advance
    /// past every committed entry before the manifest checkpoint.
    /// Holds the same locks the background worker would have.
    pub(super) fn force_compact_all(&self) {
        compact_one_pass(&self.inner, /* size_gated = */ false);
    }
}

impl Drop for L2pCompactor {
    fn drop(&mut self) {
        if self.handle.is_some() {
            self.stop();
        }
    }
}

fn run_worker(inner: Arc<L2pCompactorInner>) {
    affinity::bind_current(ThreadRole::L2pCompactor, 0);
    let max_interval = Duration::from_millis(inner.params.max_interval_ms.max(1));
    while !inner.shutdown.load(Ordering::Acquire) {
        compact_one_pass(&inner, /* size_gated = */ true);
        if inner.shutdown.load(Ordering::Acquire) {
            break;
        }
        let mut wakeup = inner.wakeup.lock();
        if !*wakeup {
            inner.wakeup_cvar.wait_for(&mut wakeup, max_interval);
        }
        *wakeup = false;
    }
}

/// One sweep over every L2P shard. When `size_gated`, a shard is only
/// compacted if `active.len() >= soft_entries`; when false, every shard
/// with non-empty active is compacted (used by `force_compact_all`).
///
/// Shards are processed **serially**. A parallel implementation was
/// tried (std::thread::scope fan-out) but with `flush_dirty_pages_threshold`
/// gating, the simultaneous wave of dirty tree pages from 16 shards
/// triggered flush immediately and crashed throughput (-67% IOPS,
/// onyx buffer hard-backpressured). Serial pacing distributes dirty
/// page production over the sweep wall time, giving flush a chance to
/// stay ahead.
fn compact_one_pass(inner: &L2pCompactorInner, size_gated: bool) {
    let vols: Vec<Arc<Volume>> = {
        let map = inner.volumes.read();
        let mut out: Vec<Arc<Volume>> = map.values().cloned().collect();
        out.sort_by_key(|v| v.ord);
        out
    };

    for vol in vols {
        for shard in &vol.shards {
            if inner.shutdown.load(Ordering::Acquire) {
                return;
            }
            let active_len = shard.l2p_buffer.active_len();
            if size_gated && active_len < inner.params.soft_entries {
                continue;
            }
            if active_len == 0 {
                continue;
            }
            compact_shard(inner, shard);
        }
    }
}

fn compact_shard(inner: &L2pCompactorInner, shard: &super::L2pShard) {
    let started = Instant::now();
    let swap = match shard.l2p_buffer.swap_for_compaction() {
        Some(handle) => handle,
        None => return,
    };
    let mut tree = shard.tree.write();
    let apply_result: crate::Result<()> =
        shard.l2p_buffer.with_draining(|draining| -> crate::Result<()> {
            let draining = match draining {
                Some(d) => d,
                None => return Ok(()),
            };
            compact_drain_into_tree(&mut tree, draining)
        });
    match apply_result {
        Ok(()) => {
            publish_l2p_read_view(shard, &tree);
            drop(tree);
            shard.l2p_buffer.finish_compaction(swap.max_lsn);
            inner.metrics.record_l2p_buffer_compaction(
                swap.count,
                started.elapsed(),
            );
        }
        Err(err) => {
            drop(tree);
            tracing::error!(error = %err, swap_count = swap.count,
                "metadb: l2p compactor apply failed; leaving draining in place");
            // Draining stays populated; next swap will assert. This is
            // a fatal-class error (page allocation / IO failure); the
            // caller (Db) should treat repeated failures here as a
            // shutdown condition. Phase 3 MVP: log + leave; Phase 5
            // hardens.
        }
    }
}
