//! Background per-clone page-livelist condense (BFG).
//!
//! Under clone-overwrite churn each overwrite of a clone-private L2P page
//! emits an ALLOC then later a FREE, so the on-disk livelist chain grows
//! unboundedly; [`crate::livelist::live_allocs`] only cancels matched pairs
//! at READ time. This worker rewrites a clone's whole persisted chain down
//! to ONE segment holding only the surviving live-ALLOC set, bounding chain
//! growth. It changes NO free decision — page-rc stays authoritative and the
//! livelist is SHADOW through — so verify's `check_clone_livelist`
//! equality still holds after a condense.
//!
//! ## Why a dedicated worker
//!
//! [`crate::db::async_reclaim`] is the obvious host, but it is gated behind
//! `Config::async_reclaim_enabled`, which is default-OFF and on the
//! "do not re-enable" list. Condense must run independently of that knob, so
//! it owns its own thread + condvar, mirroring the async-reclaim lifecycle
//! (`start` / `notify` / `stop` / `Drop`) and gated by its own
//! `Config::livelist_condense_min_segments`.
//!
//! ## Atomicity (mirrors `async_reclaim::advance_head_pid_durable`)
//!
//! All IO — chain read, new-segment build, sync — happens OUTSIDE any gate.
//! The re-anchor takes `apply_gate.write()`, which `flush_with_gate` also
//! holds for its livelist-tail store + manifest commit and which
//! `drop_volume` holds across its whole cascade, so condense excludes both.
//! Under the gate it re-reads the tail atomic and BAILS if a flush appended
//! since the read (the condensed segment would be missing those records). A
//! missing manifest entry => `drop_volume` raced and removed the clone =>
//! clean abort, NOT corruption. Crash between segment-write and commit leaves
//! the new segment an orphan; crash between commit and free leaves the old
//! chain orphans — both swept by `reclaim_orphan_pages` on the next open.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::{Condvar, Mutex, RwLock};

use super::{ManifestState, Volume};
use crate::apply_gate::ApplyGate;
use crate::cache::PageCache;
use crate::error::Result;
use crate::page::Page;
use crate::page_store::PageStore;
use crate::testing::faults::{FaultController, FaultPoint};
use crate::types::{NULL_PAGE, PageId, VolumeOrdinal};

#[derive(Clone, Copy, Debug)]
pub(super) struct LivelistCondenseParams {
    /// Condense a clone's chain only once it reaches at least this many
    /// segments. 0 disables the worker (it is never started).
    pub min_segments: usize,
    /// Maximum idle park between scans; `notify()` cuts it short.
    pub idle_interval_ms: u64,
}

pub(super) struct LivelistCondenser {
    inner: Arc<Inner>,
    handle: Option<JoinHandle<()>>,
}

struct Inner {
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    manifest_state: Arc<Mutex<ManifestState>>,
    apply_gate: Arc<ApplyGate>,
    volumes: Arc<RwLock<HashMap<VolumeOrdinal, Arc<Volume>>>>,
    faults: Arc<FaultController>,
    params: LivelistCondenseParams,
    shutdown: AtomicBool,
    /// Bumped by `notify()`; the worker captures + resets so a signal that
    /// raced past its previous wait isn't lost.
    signal: Mutex<u64>,
    signal_cvar: Condvar,
}

impl LivelistCondenser {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn start(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        manifest_state: Arc<Mutex<ManifestState>>,
        apply_gate: Arc<ApplyGate>,
        volumes: Arc<RwLock<HashMap<VolumeOrdinal, Arc<Volume>>>>,
        faults: Arc<FaultController>,
        params: LivelistCondenseParams,
    ) -> Self {
        let inner = Arc::new(Inner {
            page_store,
            page_cache,
            manifest_state,
            apply_gate,
            volumes,
            faults,
            params,
            shutdown: AtomicBool::new(false),
            signal: Mutex::new(0),
            signal_cvar: Condvar::new(),
        });
        let inner_thread = inner.clone();
        let handle = thread::Builder::new()
            .name("metadb-livelist-condense".into())
            .spawn(move || run_worker(inner_thread))
            .expect("metadb: failed to spawn livelist condense worker");
        Self {
            inner,
            handle: Some(handle),
        }
    }

    pub(super) fn stop(&mut self) {
        self.inner.shutdown.store(true, Ordering::Release);
        self.inner.signal_cvar.notify_all();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for LivelistCondenser {
    fn drop(&mut self) {
        if self.handle.is_some() {
            self.stop();
        }
    }
}

fn run_worker(inner: Arc<Inner>) {
    let idle = Duration::from_millis(inner.params.idle_interval_ms.max(1));
    let mut last_seen_signal = 0u64;
    while !inner.shutdown.load(Ordering::Acquire) {
        if let Err(err) = condense_scan(&inner) {
            tracing::warn!(error = %err, "metadb: livelist condense scan failed");
        }
        let mut sig = inner.signal.lock();
        if *sig == last_seen_signal {
            inner.signal_cvar.wait_for(&mut sig, idle);
        }
        last_seen_signal = *sig;
    }
}

/// One scan: snapshot the volume handles, and for every clone whose
/// persisted livelist chain has grown past the segment threshold, rewrite
/// it to a single condensed segment. Per-volume failures are logged and
/// don't abort the scan. Shutdown is checked between volumes so a stop
/// doesn't wait out a long scan.
fn condense_scan(inner: &Arc<Inner>) -> Result<()> {
    let vol_handles: Vec<(VolumeOrdinal, Arc<Volume>)> = {
        let guard = inner.volumes.read();
        guard.iter().map(|(k, v)| (*k, v.clone())).collect()
    };
    for (vol_ord, vol) in vol_handles {
        if inner.shutdown.load(Ordering::Acquire) {
            break;
        }
        // Cheap gate: non-clone volumes never have a livelist chain
        // (tail == NULL), and a chain below the threshold isn't worth a
        // rewrite. `chain_has_at_least_segments` reads at most
        // `min_segments` head pages.
        let tail0 = vol.page_live_list_tail_pid.load(Ordering::Acquire);
        if tail0 == NULL_PAGE {
            continue;
        }
        let over_threshold = crate::livelist::chain_has_at_least_segments(
            tail0,
            inner.params.min_segments,
            |pid| inner.page_store.read_page(pid),
        )?;
        if !over_threshold {
            continue;
        }
        if let Err(err) = condense_one(inner, vol_ord, &vol, tail0) {
            tracing::warn!(
                vol_ord = vol_ord,
                error = %err,
                "metadb: livelist condense for volume failed"
            );
        }
    }
    Ok(())
}

/// Rewrite `vol`'s persisted livelist chain (anchored at `tail0`) into one
/// condensed segment holding only `live_allocs(chain)`. Returns `true` iff a
/// condense committed; `false` for a clean abort (flush raced / clone
/// dropped / empty live set already single-segment).
fn condense_one(
    inner: &Arc<Inner>,
    vol_ord: VolumeOrdinal,
    vol: &Arc<Volume>,
    tail0: PageId,
) -> Result<bool> {
    // 1. Read the chain + cancel matched ALLOC/FREE pairs. No gate: the
    //    persisted segments [head0..tail0] are immutable once written; a
    //    concurrent flush only appends a NEW tail (caught by the re-check
    //    below), never mutates these.
    let records = crate::livelist::read_chain_records(tail0, |p| inner.page_store.read_page(p))?;
    let before = records.len();
    let live = crate::livelist::live_allocs(records)?;

    // 2. Build the condensed single segment (prev = NULL) OUTSIDE the gate.
    //    Empty live set => collapse the chain to NULL.
    let checkpoint_lsn = inner.manifest_state.lock().manifest.checkpoint_lsn;
    let (anchor, new_seg_pids) = if live.is_empty() {
        (NULL_PAGE, Vec::new())
    } else {
        let page_count = crate::livelist::segment_pages_for(live.len());
        let start = inner.page_store.allocate_run(page_count)?;
        let pages =
            crate::livelist::build_segment_pages(start, &live, NULL_PAGE, checkpoint_lsn);
        let sealed: Vec<(PageId, Arc<Page>)> =
            pages.into_iter().map(|(p, pg)| (p, Arc::new(pg))).collect();
        inner.page_store.write_sealed_page_runs(sealed)?;
        // Durable before the not-yet-committed anchor can reference it.
        inner.page_store.sync()?;
        let pids: Vec<PageId> = (0..page_count as u64).map(|i| start + i).collect();
        (start, pids)
    };

    // 3. Re-anchor under apply_gate.write() — excludes flush + drop_volume.
    let gate = inner.apply_gate.write();
    inner
        .faults
        .inject(FaultPoint::LivelistCondensePostSegWriteBeforeManifest)?;
    let cur_tail = vol.page_live_list_tail_pid.load(Ordering::Acquire);
    if cur_tail != tail0 {
        // A flush appended a new tail segment since step 1 — the condensed
        // segment would be missing its records. Abort and free the new
        // segment; the next scan retries against the grown chain.
        drop(gate);
        free_new_segment(inner, &new_seg_pids, checkpoint_lsn);
        return Ok(false);
    }

    // 4. Commit the re-anchored manifest. A missing entry means drop_volume
    //    raced and removed the clone (it leaves the tail atomic untouched, so
    //    the cur_tail check above passes) — a CLEAN abort, never corruption.
    let mut manifest_for_commit = {
        let mut mstate = inner.manifest_state.lock();
        let entry = match mstate.manifest.volumes.iter_mut().find(|v| v.ord == vol_ord) {
            Some(entry) => entry,
            None => {
                drop(mstate);
                drop(gate);
                free_new_segment(inner, &new_seg_pids, checkpoint_lsn);
                return Ok(false);
            }
        };
        entry.page_live_list_head_pid = anchor;
        entry.page_live_list_tail_pid = anchor;
        mstate.manifest.clone()
    };
    {
        let mut mstate = inner.manifest_state.lock();
        mstate.store.commit(&mut manifest_for_commit)?;
    }

    // 5. Manifest durable — promote the in-memory anchors so the next flush
    //    links its append off the condensed segment.
    vol.page_live_list_head_pid.store(anchor, Ordering::Release);
    vol.page_live_list_tail_pid.store(anchor, Ordering::Release);
    drop(gate);

    // 6. Free the old chain. Read the free generation AFTER the commit
    //    (a concurrent flush only raises checkpoint_lsn, keeping it >= the
    //    old pages' generations). free_idempotent is crash-safe: a crash here
    //    leaves the old pages as orphans reclaimed on the next open.
    inner
        .faults
        .inject(FaultPoint::LivelistCondensePostManifestBeforeFree)?;
    let free_gen = inner.manifest_state.lock().manifest.checkpoint_lsn;
    let old_pids =
        crate::livelist::walk_chain_pages(tail0, |p| inner.page_store.read_page(p))?;
    for pid in old_pids {
        inner.page_store.free_idempotent(pid, free_gen)?;
        inner.page_cache.invalidate(pid);
    }
    tracing::debug!(
        vol_ord = vol_ord,
        records_before = before,
        records_after = live.len(),
        "metadb: livelist condensed clone chain"
    );
    Ok(true)
}

/// Free the just-written condensed segment on an abort path (flush raced /
/// clone dropped). Best-effort: a failure here only leaks the new segment as
/// an orphan that `reclaim_orphan_pages` sweeps on the next open.
fn free_new_segment(inner: &Arc<Inner>, pids: &[PageId], generation: crate::types::Lsn) {
    for &pid in pids {
        if let Err(err) = inner.page_store.free_idempotent(pid, generation) {
            tracing::warn!(pid = pid, error = %err, "metadb: livelist condense abort free failed");
        }
        inner.page_cache.invalidate(pid);
    }
}

/// Test/one-shot synchronous driver: run a single condense scan on the
/// caller thread (no background worker), so tests can assert chain
/// shrinkage deterministically without racing the worker. Mirrors
/// `async_reclaim::test_run_lineage_gc_cycle`.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(in crate::db) fn run_condense_scan_once(
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    manifest_state: Arc<Mutex<ManifestState>>,
    apply_gate: Arc<ApplyGate>,
    volumes: Arc<RwLock<HashMap<VolumeOrdinal, Arc<Volume>>>>,
    faults: Arc<FaultController>,
    params: LivelistCondenseParams,
) -> Result<()> {
    let inner = Arc::new(Inner {
        page_store,
        page_cache,
        manifest_state,
        apply_gate,
        volumes,
        faults,
        params,
        shutdown: AtomicBool::new(false),
        signal: Mutex::new(0),
        signal_cvar: Condvar::new(),
    });
    condense_scan(&inner)
}
