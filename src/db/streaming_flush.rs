//! Background L2P streaming writeback.
//!
//! Goal: keep the on-disk image of every dirty L2P page within "a few
//! tens of milliseconds" of the live tree by continuously sealing and
//! writing dirty pages through the centralised
//! [`IoSubmitter`](crate::io_submitter::IoSubmitter) **outside**
//! `apply_gate.write()`. The next `Db::flush()` then samples a small
//! dirty set, so its gate-hold time stays bounded by the lifecycle
//! bookkeeping (private/retired sets, manifest commit) rather than by
//! the clone/seal of accumulated dirty pages.
//!
//! ## Correctness
//!
//! Writeback is a *content-only* optimisation, as documented on
//! [`PagedL2p::writeback_dirty_snapshot`](crate::paged::PagedL2p::writeback_dirty_snapshot):
//!
//! - It does NOT touch `private_pages` / `retired_pages` /
//!   `checkpoint_protected` — those remain under `Db::flush`'s control.
//! - It does NOT advance the durable `manifest.checkpoint_lsn`. The
//!   manifest is only committed by `Db::flush`. Any page bytes the
//!   writeback path puts on disk are recoverable: WAL replay from the
//!   last committed `checkpoint_lsn` re-applies every commit that
//!   touched the page, restoring in-memory state regardless of which
//!   sealed version the disk happens to carry.
//! - `install_writeback` uses `Arc::ptr_eq` to detect commits that
//!   mutated the page between snapshot and install. On a mismatch the
//!   page stays in the dirty set; the next cycle picks it up. The race
//!   never corrupts state — at worst it wastes a write of the older
//!   sealed bytes.
//!
//! ## Concurrency vs `Db::flush`
//!
//! Both writeback and `Db::flush`'s IO phase can write to the same
//! `(pid, sealed_bytes)` location, but in practice they never race on
//! the same pid because:
//!
//! - When `Db::flush` takes `apply_gate.write`, the writeback worker
//!   can still hold its own shard-level lock and finish an in-flight
//!   install. Inside `install_writeback`, the page is removed from
//!   `pages` (the dirty map) — so when `Db::flush`'s `begin_checkpoint`
//!   subsequently calls `dirty_snapshot`, that pid is no longer there.
//!   Pages the writeback path didn't manage to install before the gate
//!   was taken are still dirty and become part of the checkpoint
//!   sample.
//! - In the reverse order (`Db::flush` writes a pid, then writeback
//!   writes the same pid later), the writeback sees a Dirty slot whose
//!   Arc differs from its snapshot (the post-checkpoint reads bumped
//!   the cache and the next mutation made a new Arc), so
//!   `install_writeback` returns kept_dirty and the writeback bytes
//!   simply layer on top of the checkpoint bytes — both are valid
//!   sealed images of the page at distinct LSNs.
//!
//! ## Performance shape
//!
//! Per cycle, per shard:
//!   - `dirty_page_count()` under `tree.read()` (fast)
//!   - `writeback_dirty_snapshot()` under `tree.read()` (Arc clones)
//!   - `DirtySnapshot::seal()` outside any lock (memcpy + header update)
//!   - `page_store.write_sealed_page_runs(...)` + `sync()` (IoSubmitter
//!     parallel writes, SQ=1024)
//!   - `install_writeback(...)` under `tree.write()` (Arc::ptr_eq +
//!     `pages_remove` per page; promoted pages dropped from dirty map)
//!
//! The only step that contends with foreground commits on the same
//! shard is `install_writeback`. The size of that contention window is
//! capped by `Config::l2p_writeback_max_pages_per_cycle`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::RwLock;

use crate::metrics::MetaMetrics;
use crate::page::Page;
use crate::page_store::PageStore;
use crate::types::{PageId, VolumeOrdinal};

use super::Volume;

#[derive(Clone, Copy)]
pub(super) struct StreamingFlushParams {
    pub idle_sleep_us: u64,
    pub min_dirty_pages: usize,
    pub max_pages_per_cycle: usize,
    /// Global L2P-dirty-page target. While the sum of every shard's
    /// `dirty_page_count()` stays below this value, the worker parks at
    /// `idle_sleep_us` without scanning shards. 0 disables the gate
    /// (preserves the per-shard `min_dirty_pages` behaviour).
    pub dirty_pages_target: usize,
}

pub(super) struct StreamingFlusher {
    inner: Arc<StreamingFlusherInner>,
    handle: Option<JoinHandle<()>>,
}

struct StreamingFlusherInner {
    volumes: Arc<RwLock<HashMap<VolumeOrdinal, Arc<Volume>>>>,
    page_store: Arc<PageStore>,
    metrics: Arc<MetaMetrics>,
    params: StreamingFlushParams,
    shutdown: AtomicBool,
}

impl StreamingFlusher {
    pub(super) fn start(
        volumes: Arc<RwLock<HashMap<VolumeOrdinal, Arc<Volume>>>>,
        page_store: Arc<PageStore>,
        metrics: Arc<MetaMetrics>,
        params: StreamingFlushParams,
    ) -> Self {
        let inner = Arc::new(StreamingFlusherInner {
            volumes,
            page_store,
            metrics,
            params,
            shutdown: AtomicBool::new(false),
        });
        let inner_thread = inner.clone();
        let handle = thread::Builder::new()
            .name("metadb-l2p-writeback".into())
            .spawn(move || run_worker(inner_thread))
            .expect("metadb: failed to spawn l2p writeback worker");
        Self {
            inner,
            handle: Some(handle),
        }
    }

    pub(super) fn stop(&mut self) {
        self.inner.shutdown.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for StreamingFlusher {
    fn drop(&mut self) {
        if self.handle.is_some() {
            self.stop();
        }
    }
}

fn run_worker(inner: Arc<StreamingFlusherInner>) {
    let idle_sleep = Duration::from_micros(inner.params.idle_sleep_us.max(1));
    while !inner.shutdown.load(Ordering::Acquire) {
        let did_work = flush_one_pass(&inner);
        inner.metrics.record_l2p_writeback_cycle(did_work);
        if !did_work {
            // Park briefly to avoid busy-spinning when every shard is
            // below the dirty threshold. Active cycles loop without
            // sleeping so writeback follows commit pace.
            thread::sleep(idle_sleep);
        }
    }
}

fn flush_one_pass(inner: &StreamingFlusherInner) -> bool {
    // Snapshot the current volume list. Holding the read guard beyond
    // this point would block `create_volume` / `drop_volume`, so clone
    // the Arcs out and release.
    let vols: Vec<Arc<Volume>> = {
        let map = inner.volumes.read();
        let mut out: Vec<Arc<Volume>> = map.values().cloned().collect();
        out.sort_by_key(|v| v.ord);
        out
    };

    // Global target gate: keep the worker quiet while the dirty backlog
    // is small enough that the next foreground checkpoint can absorb it
    // cheaply. Skips both the per-shard work below AND the per-shard
    // `tree.read().dirty_page_count()` call inside the loop when the
    // sum is below target. The pre-pass cost is one read-lock per
    // shard, the same shape as the existing loop's first action — so
    // this stays cheap even when the gate trips.
    let target = inner.params.dirty_pages_target;
    if target > 0 {
        let mut total: usize = 0;
        for vol in &vols {
            for shard in vol.shards.iter() {
                if inner.shutdown.load(Ordering::Acquire) {
                    return false;
                }
                total = total.saturating_add(shard.tree.read().dirty_page_count());
                if total >= target {
                    break;
                }
            }
            if total >= target {
                break;
            }
        }
        if total < target {
            inner.metrics.record_l2p_writeback_target_skip();
            return false;
        }
    }

    let mut did_work = false;
    for vol in &vols {
        for shard in vol.shards.iter() {
            if inner.shutdown.load(Ordering::Acquire) {
                return did_work;
            }
            let dirty_count = { shard.tree.read().dirty_page_count() };
            if dirty_count < inner.params.min_dirty_pages {
                continue;
            }
            if flush_one_shard_chunk(inner, shard) {
                did_work = true;
            }
        }
    }
    did_work
}

/// Snapshot + seal + write + install for at most
/// `params.max_pages_per_cycle` pages on this shard. Returns `true` if
/// any pages were submitted for write.
///
/// The cap on snapshot size translates directly to install-lock hold
/// time, which is the only writeback step contending with foreground
/// commit apply on the same shard. A higher cap = wider IO batches
/// (better NVMe utilisation) but longer install holds; the default
/// `l2p_writeback_max_pages_per_cycle` is tuned to keep individual
/// holds in the low-millisecond range. Each pass round-robins every
/// shard once, so a backed-up shard catches up across many passes
/// rather than monopolising the worker with one giant install.
fn flush_one_shard_chunk(inner: &StreamingFlusherInner, shard: &super::L2pShard) -> bool {
    let cap = inner.params.max_pages_per_cycle.max(1);
    let snapshot = { shard.tree.read().writeback_dirty_snapshot_capped(cap) };
    if snapshot.is_empty() {
        return false;
    }

    let seal_started = Instant::now();
    let flushed = match snapshot.seal() {
        Ok(f) => f,
        Err(err) => {
            inner.metrics.record_l2p_writeback_error();
            tracing::warn!(?err, "metadb: l2p writeback seal failed");
            return false;
        }
    };
    let pages_count = flushed.pages_count();
    inner
        .metrics
        .record_l2p_writeback_seal(seal_started.elapsed(), pages_count);

    let mut page_runs: Vec<(PageId, Arc<Page>)> = Vec::with_capacity(pages_count);
    flushed.append_sealed_pages(&mut page_runs);

    let io_started = Instant::now();
    if let Err(err) = inner
        .page_store
        .write_sealed_page_runs_background(page_runs)
    {
        inner.metrics.record_l2p_writeback_error();
        tracing::warn!(?err, "metadb: l2p writeback write_sealed_page_runs failed");
        return false;
    }
    if let Err(err) = inner.page_store.sync() {
        inner.metrics.record_l2p_writeback_error();
        tracing::warn!(?err, "metadb: l2p writeback sync failed");
        return false;
    }
    let io_bytes = pages_count.saturating_mul(crate::config::PAGE_SIZE);
    inner
        .metrics
        .record_l2p_writeback_io(io_started.elapsed(), io_bytes);

    // try_write rather than write: writeback yields the install lock
    // to foreground commit apply whenever the shard is busy. The
    // sealed pages are already durable on disk; they'll be picked up
    // by the next cycle's install (the Arc::ptr_eq check still works
    // because re-mutation in the meantime simply flips the result
    // from promoted to kept_dirty without losing correctness). This
    // priority inversion guard is what keeps fio p99 tail bounded
    // under heavy commit pressure.
    let install_started = Instant::now();
    let (promoted, kept) = {
        match shard.tree.try_write() {
            Some(mut guard) => guard.install_writeback(&flushed),
            None => {
                inner.metrics.record_l2p_writeback_install(
                    install_started.elapsed(),
                    0,
                    pages_count,
                );
                return true;
            }
        }
    };
    inner
        .metrics
        .record_l2p_writeback_install(install_started.elapsed(), promoted, kept);
    true
}
