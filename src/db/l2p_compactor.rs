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

use crate::metrics::MetaMetrics;
use crate::types::VolumeOrdinal;

use super::Volume;
use super::apply::publish_l2p_read_view;

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
            let mut entries: Vec<(u64, &super::l2p_buffer::BufferEntry)> =
                draining.iter().map(|(lba, e)| (*lba, e)).collect();
            entries.sort_by_key(|(lba, _)| *lba);
            for (lba, entry) in entries {
                if entry.tombstone {
                    tree.delete_at_lsn(lba, entry.lsn)?;
                } else {
                    tree.insert_at_lsn(lba, entry.value, entry.lsn)?;
                }
            }
            Ok(())
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
