//! Background reclaim of `page_store.deferred_free`.
//!
//! ## Two passes per cycle
//!
//! 1. **Deferred-free reclaim**: keeps the page_store's deferred_free
//!    queue draining off the `flush_with_gate` critical path. Same
//!    semantics as the original priority-1 worker.
//! 2. **Lineage GC planning**: cannot use the historical
//!    background worker path because it only advanced the dead-list chain
//!    and had no `Db` handle to emit `commit_free_pbas`. With rc-neutral
//!    L2P remaps, that would drop the only retire signal. The worker
//!    therefore refuses to run chain-truncation-only lineage GC in .
//!
//! ## Correctness
//!
//! Reclaim pass: unchanged. `try_reclaim_limit` writes zero-filled
//! Free pages then extends `inner.free_list`; we follow with
//! `page_cache.invalidate(pid)` so stale cache bytes can't shadow
//! the new content after the allocator hands the pid back out.
//!
//! Lineage GC pass: `head_pid` is only mutated by this worker, so
//! the read/decide/commit sequence is single-threaded. New segments
//! are appended at `tail_pid` by `flush_with_gate` under
//! `apply_gate.write()`; the GC commit re-acquires
//! `apply_gate.write()` to serialize against flush so that locating
//! "the segment newer than head" reads a stable tail. Manifest
//! commits go through the same `manifest_state.store.commit` path
//! that `flush_with_gate` uses, so a crash mid-pass leaves the
//! chain in a state recovery's manifest walk can decode.
//!
//! ## Lifecycle
//!
//! Started by [`Db::start_async_reclaim`] after the rest of `Db` is
//! wired up. Stopped by `Db::drop` before `page_store` / `page_cache`
//! tear down. The worker holds clones of all the Arcs it needs
//! (page_store, page_cache, metrics, optional LineageGcCtx fields);
//! it never holds an `Arc<Db>` reference, so the cycle stays clean.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex, RwLock};

use super::Volume;
use crate::apply_gate::ApplyGate;
use crate::cache::PageCache;
use crate::deadlist::{SegmentHeader, read_segment_records, walk_chain_segments};
use crate::error::{MetaDbError, Result};
use crate::metrics::MetaMetrics;
use crate::page_store::PageStore;
use crate::refcount::RcShard;
use crate::testing::faults::{FaultController, FaultPoint};
use crate::types::{Lsn, NULL_PAGE, PageId, Pba, VolumeOrdinal};

/// Background-worker view of the Db state 's Lineage GC pass
/// needs. All fields are `Arc`-cloneable so the worker can hold its
/// own handles without an `Arc<Db>` cycle.
pub(super) struct LineageGcCtx {
    pub volumes: Arc<RwLock<HashMap<VolumeOrdinal, Arc<Volume>>>>,
    pub manifest_state: Arc<Mutex<crate::db::ManifestState>>,
    pub apply_gate: Arc<ApplyGate>,
    /// `rc.clone()` for every refcount shard, in shard-index order. The
    /// GC pass reads `rc.get(pba)` to skip records that are still shared by a
    /// dedup hit or clone-promotion edge. Once the count reaches 0, lineage GC
    /// can emit `FreePbas` for the retired PBA.
    pub refcount_shards_rc: Vec<Arc<RcShard>>,
    pub faults: Arc<FaultController>,
    /// Metrics sink so `gc_plan_head_advance` can attribute every
    /// head-advance decision (advanced / snap-pinned / descendant-pinned
    /// / rc>0 bail + the rc==0 reclaim debt stuck behind an rc>0 sibling).
    pub metrics: Arc<MetaMetrics>,
    /// When true, the FreePbas-emitting GC driver
    /// (`Db::run_lineage_gc_cycle_inner`, reachable from
    /// `Db::test_run_lineage_gc_cycle`) emits a `FreePbas` lifecycle-style commit
    /// for every dead record it retires before truncating the chain. This
    /// module's background-only `lineage_gc_cycle` cannot reach `Db::commit_ops`,
    /// so callers must not use that path when this flag is set.
    pub emit_freepbas: bool,
    /// When true, head-advance drops `rc > 0` (dedup-membership) records and
    /// advances past them, surfacing only the `rc == 0` exclusive records,
    /// instead of bailing the whole segment on the first `rc > 0`. Only sound
    /// when the DB never creates snapshots/clones (every `rc > 0` is then a
    /// dedup target owned by the client's orphan-reclaim path). See
    /// `Config::lineage_gc_drop_dedup_shared`.
    pub drop_dedup_shared: bool,
}

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
    /// Optional lineage GC context. uses this only as a guard:
    /// the old background worker cannot emit FreePbas, so it must not
    /// advance chains while `emit_freepbas` is true.
    lineage_gc: Option<LineageGcCtx>,
}

impl AsyncReclaim {
    pub(super) fn start(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        metrics: Arc<MetaMetrics>,
        params: AsyncReclaimParams,
    ) -> Self {
        Self::start_with_lineage_gc(page_store, page_cache, metrics, params, None)
    }

    pub(super) fn start_with_lineage_gc(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        metrics: Arc<MetaMetrics>,
        params: AsyncReclaimParams,
        lineage_gc: Option<LineageGcCtx>,
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
            lineage_gc,
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
        // PASS 1: drain deferred_free. ONE cycle per wakeup; tight
        // looping would burn NVMe bandwidth and starve foreground
        // flush IO. Backlog drains naturally over multiple
        // notifications.
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
        // PASS 2: Historical lineage GC is disabled in .
        // It can only truncate dead-list chains; it cannot emit FreePbas
        // because this worker intentionally doesn't hold an Arc<Db>. Since
        // rc-neutral L2P remaps rely on FreePbas as the retire signal, do
        // not run the old chain-truncation-only path when emit_freepbas is
        // required.
        if let Some(ctx) = &inner.lineage_gc {
            if !ctx.emit_freepbas {
                if let Err(err) = lineage_gc_cycle(&inner.page_store, ctx) {
                    tracing::warn!(error = %err, "metadb: lineage GC cycle failed");
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

/// Historical Lineage GC: try to advance every volume's
/// `dead_list_head_pid` by one segment when the head segment is
/// fully reclaim-eligible. Returns the number of volumes whose head
/// advanced. Per-volume failures are logged and don't abort the
/// cycle — chain advancement is best-effort.
fn lineage_gc_cycle(page_store: &Arc<PageStore>, ctx: &LineageGcCtx) -> Result<usize> {
    let vol_handles: Vec<(VolumeOrdinal, Arc<Volume>)> = {
        let guard = ctx.volumes.read();
        guard.iter().map(|(k, v)| (*k, v.clone())).collect()
    };
    let mut advanced = 0;
    for (vol_ord, vol) in vol_handles {
        match try_advance_head_one(page_store, ctx, &vol, vol_ord) {
            Ok(true) => advanced += 1,
            Ok(false) => continue,
            Err(err) => {
                tracing::warn!(
                    vol_ord = vol_ord,
                    error = %err,
                    "lineage GC: per-volume cycle failed"
                );
                continue;
            }
        }
    }
    Ok(advanced)
}

/// Output of [`gc_plan_head_advance`]. Carries everything
/// [`gc_execute_head_advance`] needs to actually advance the chain,
/// plus the list of PBAs that the FreePbas-emitting driver wants
/// to surface to onyx before the chain is truncated.
///
/// `dead_pbas` is collected in segment order from the head segment's
/// records, with no dedup. currently retires whole segments
/// at a time, so duplicates within a single segment are not expected;
/// downstream `apply_free_pbas` is set-typed regardless.
pub(super) struct HeadAdvancePlan {
    pub old_head: PageId,
    pub head_page_count: u32,
    pub tail: PageId,
    pub dead_pbas: Vec<Pba>,
}

/// Plan phase of one volume's Lineage GC step. Reads the head segment,
/// captures the snapshot / descendant pin set, and verifies that every
/// record's refcount has already reached 0. Returns `Some(plan)` iff
/// the head can be advanced; `None` means "skip this volume this
/// cycle" (head empty, snap/descendant pin, or any record still
/// rc>0). Does **not** mutate state and does **not** take
/// `apply_gate`; safe to call from any context including the
/// FreePbas driver, which needs to commit a lifecycle record between plan
/// and execute.
pub(super) fn gc_plan_head_advance(
    page_store: &Arc<PageStore>,
    ctx: &LineageGcCtx,
    vol: &Volume,
    vol_ord: VolumeOrdinal,
) -> Result<Option<HeadAdvancePlan>> {
    let head = vol.dead_list_head_pid.load(Ordering::Acquire);
    if head == NULL_PAGE {
        return Ok(None);
    }

    // 1. Read the head segment's pages.
    let head_page = page_store.read_page(head)?;
    if head_page.header()?.page_type != crate::page::PageType::DeadListSegment {
        return Err(MetaDbError::Corruption(format!(
            "lineage GC: head_pid {head} is not a DeadListSegment page"
        )));
    }
    let header = SegmentHeader::decode(head_page.payload())?;
    let seg_page_count = header.seg_page_count as u64;
    let mut cont_pages = Vec::with_capacity(seg_page_count as usize - 1);
    for i in 1..seg_page_count {
        cont_pages.push(page_store.read_page(head + i)?);
    }
    let cont_refs: Vec<&[u8]> = cont_pages.iter().map(|p| &p.payload()[..]).collect();
    let records = read_segment_records(head_page.payload(), &cont_refs)?;

    // Fault A: between segment read and snapshot-list capture. A
    // crash here is identical to "GC was about to start but never
    // ran" — no side effect.
    ctx.faults.inject(FaultPoint::LineageGcMidSegmentRead)?;

    // 2. Snapshot the active snapshot LSNs for this volume AND the
    // `branched_at_lsn`s of every still-attached descendant. New
    // snapshots that arrive after this read have `created_lsn >
    // current_lsn > death_lsn` for every record here, so they
    // can't pin anything we're about to decide on; missing them is
    // safe. `drop_snapshot` and `clone_volume` both run under
    // `drop_gate.write()` and are serialized with
    // `manifest_state.lock()` for commits, so both the snapshot list
    // and the descendant set are stable during this critical
    // section.
    //
    // a descendant whose `parent_vol_ord == Some(vol_ord)`
    // has not yet had its background promotion walker complete; the
    // global rc has therefore not yet been bumped for the parent's
    // shared PBAs. The descendant still observes the parent's L2P
    // state at the moment of clone (`branched_at_lsn`), so any
    // parent dead-list record whose `[birth, death)` interval
    // contains that LSN must stay pinned until the descendant
    // either (a) finishes its promotion walker — which clears
    // `parent_vol_ord` — or (b) is itself dropped.
    let (snap_lsns, descendant_branch_lsns): (Vec<Lsn>, Vec<Lsn>) = {
        let mst = ctx.manifest_state.lock();
        let snap_lsns = mst
            .manifest
            .snapshots
            .iter()
            .filter(|s| s.vol_ord == vol_ord)
            .map(|s| s.created_lsn)
            .collect::<Vec<_>>();
        let descendant_branch_lsns = mst
            .manifest
            .volumes
            .iter()
            .filter(|e| e.parent_vol_ord == Some(vol_ord))
            .map(|e| e.branched_at_lsn)
            .collect::<Vec<_>>();
        (snap_lsns, descendant_branch_lsns)
    };

    // 3. Check every record: must be unpinned by any active
    // snapshot, unpinned by any descendant's branch point, AND
    // no longer blocked by the global refcount event ledger. If any
    // record fails any check, leave the segment alone — it'll be
    // re-evaluated on the next cycle (e.g. after a drop_snapshot,
    // a PromotionComplete, or further FreePbas/DedupDelete events).
    let n_shards = ctx.refcount_shards_rc.len();
    debug_assert!(n_shards > 0, "lineage GC: refcount_shards_rc must be non-empty");
    let mut dead_pbas: Vec<Pba> = Vec::with_capacity(records.len());
    for rec in &records {
        if snap_pinned(&snap_lsns, rec.birth_lsn, rec.death_lsn) {
            ctx.metrics.record_lineage_gc_head_skipped_snap();
            return Ok(None);
        }
        if snap_pinned(&descendant_branch_lsns, rec.birth_lsn, rec.death_lsn) {
            ctx.metrics.record_lineage_gc_head_skipped_descendant();
            return Ok(None);
        }
        let sid = (xxhash_rust::xxh3::xxh3_64(&rec.pba.to_be_bytes()) as usize) % n_shards;
        let rc = ctx.refcount_shards_rc[sid].get(rec.pba)?;
        if rc > 0 {
            if ctx.drop_dedup_shared {
                // Guarded Option 3 (`Config::lineage_gc_drop_dedup_shared`):
                // in a DB that never creates snapshots/clones, every rc>0
                // dead-list record is a dedup-membership PBA (bumps rc
                // only via DedupPut/PromotionChunk, and PromotionChunk needs a
                // clone, which this mode forbids). Its reclaim is owned by the
                // client's dedup orphan-reclaim path (DedupDelete → rc 0 →
                // retire → confirm-scan free), so the dead-list record is
                // redundant: DROP it — do NOT surface, do NOT decref (the
                // FreePbas rc>0 decref would double-count the dedup ledger) —
                // and keep scanning. The head then advances past it instead of
                // the whole-segment bail that stranded the rc==0 siblings (the
                // reclaim-lag bug). The snap/descendant pins above still bail
                // the whole volume, so an ACTIVE clone is never reached here.
                ctx.metrics.record_lineage_gc_head_dropped_dedup_shared();
                continue;
            }
            // Whole-segment bail: this rc>0 record leaves the entire head
            // segment intact, so every OTHER rc==0 record here is freeable
            // but stuck. Tally that reclaim debt for diagnosis. Records
            // before this point are already confirmed rc==0 (in `dead_pbas`);
            // scan the remainder. Instrumentation-only: a get() error here
            // is swallowed (the plan decision is already "bail").
            let mut blocked_rc0 = dead_pbas.len();
            for later in &records[dead_pbas.len() + 1..] {
                let lsid =
                    (xxhash_rust::xxh3::xxh3_64(&later.pba.to_be_bytes()) as usize) % n_shards;
                if matches!(ctx.refcount_shards_rc[lsid].get(later.pba), Ok(0)) {
                    blocked_rc0 += 1;
                }
            }
            ctx.metrics.record_lineage_gc_head_skipped_rc(blocked_rc0);
            return Ok(None);
        }
        dead_pbas.push(rec.pba);
    }
    ctx.metrics.record_lineage_gc_head_advanced(dead_pbas.len());

    // 4. Locate the tail anchor up front so the execute phase can
    // walk the chain under the gate. tail being NULL with head set
    // is a invariant violation — bail rather than execute
    // a corrupt manifest.
    let tail = vol.dead_list_tail_pid.load(Ordering::Acquire);
    if tail == NULL_PAGE {
        return Err(MetaDbError::Corruption(format!(
            "lineage GC: vol_ord {vol_ord} has head_pid={head} but tail_pid=NULL_PAGE"
        )));
    }

    Ok(Some(HeadAdvancePlan {
        old_head: head,
        head_page_count: seg_page_count as u32,
        tail,
        dead_pbas,
    }))
}

/// Execute phase of one volume's Lineage GC step. Takes
/// `apply_gate.write()`, re-reads the head atomic, walks the chain to
/// find the segment "newer than head", commits the manifest, promotes
/// the new head/tail atomics, and frees the old head segment's pages.
///
/// The plan-then-execute split exists so the FreePbas-emitting driver can
/// call `commit_free_pbas(vol_ord, …)` between the two phases without
/// deadlocking against `apply_gate` — `commit_free_pbas` takes
/// `apply_gate.read()` internally. A crash between FreePbas commit and execute is safe: the chain
/// is still intact, next GC cycle re-runs both phases and
/// `apply_free_pbas` re-surfaces the same PBAs (onyx absorbs the
/// duplicate idempotently in `PbaLifecycle::free_lineage_gc_proven`).
/// A crash inside execute (after manifest commit, before
/// page_store free) leaks segment pages to recovery's free-list
/// scan, same as .
pub(super) fn gc_execute_head_advance(
    page_store: &Arc<PageStore>,
    ctx: &LineageGcCtx,
    vol: &Volume,
    vol_ord: VolumeOrdinal,
    plan: &HeadAdvancePlan,
) -> Result<()> {
    advance_head_pid_durable(
        page_store,
        ctx,
        vol,
        vol_ord,
        plan.old_head,
        plan.head_page_count,
        plan.tail,
    )
}

fn try_advance_head_one(
    page_store: &Arc<PageStore>,
    ctx: &LineageGcCtx,
    vol: &Volume,
    vol_ord: VolumeOrdinal,
) -> Result<bool> {
    let Some(plan) = gc_plan_head_advance(page_store, ctx, vol, vol_ord)? else {
        return Ok(false);
    };
    // worker path: no FreePbas emission, chain truncation only.
    // callers must not reach this path with emit_freepbas=true.
    if ctx.emit_freepbas {
        return Err(MetaDbError::InvalidArgument(
            "rc-neutral lineage GC forbids background chain truncation without FreePbas emission"
                .into(),
        ));
    }
    gc_execute_head_advance(page_store, ctx, vol, vol_ord, &plan)?;
    Ok(true)
}

fn snap_pinned(snap_lsns: &[Lsn], birth_lsn: Lsn, death_lsn: Lsn) -> bool {
    snap_lsns
        .iter()
        .any(|&s| s >= birth_lsn && s < death_lsn)
}

fn advance_head_pid_durable(
    page_store: &Arc<PageStore>,
    ctx: &LineageGcCtx,
    vol: &Volume,
    vol_ord: VolumeOrdinal,
    old_head: PageId,
    head_page_count: u32,
    tail_pid: PageId,
) -> Result<()> {
    // Take apply_gate.write to exclude flush_with_gate — both
    // touch VolumeEntry's `dead_list_*_pid` fields and the same
    // manifest commit slot.
    let _gate = ctx.apply_gate.write();

    // Re-read head under the gate. Only this worker mutates
    // head_pid, so it should be unchanged — but if another GC
    // cycle (or a future step) advanced it, bail out
    // cleanly rather than overwrite.
    let cur_head = vol.dead_list_head_pid.load(Ordering::Acquire);
    if cur_head != old_head {
        return Ok(());
    }

    // Walk the chain under the gate so the tail is stable. Bound
    // by `old_head` so we don't try to read freed pages from a
    // prior GC advance (the new head's `prev_seg_pid` still points
    // at the freed older segment until a future flush rewrites the
    // segment header, which doesn't do). If the chain has
    // a single segment (head == tail) we'll be returning both
    // anchors to NULL_PAGE.
    let segs = walk_chain_segments(tail_pid, old_head, |pid| page_store.read_page(pid))?;
    let last = segs.last().ok_or_else(|| {
        MetaDbError::Corruption(format!(
            "lineage GC: chain walk for vol_ord {vol_ord} returned no segments"
        ))
    })?;
    if last.page_id != old_head {
        return Err(MetaDbError::Corruption(format!(
            "lineage GC: chain walk's oldest segment {} doesn't match head {}",
            last.page_id, old_head
        )));
    }
    // segs is tail-first (newest-first); the segment "newer than head"
    // is segs[len-2] when present.
    let new_head_pid = if segs.len() < 2 {
        NULL_PAGE
    } else {
        segs[segs.len() - 2].page_id
    };

    // Mutate the manifest copy, commit, then promote atomics.
    let manifest_for_commit = {
        let mut mstate = ctx.manifest_state.lock();
        let entry = mstate
            .manifest
            .volumes
            .iter_mut()
            .find(|v| v.ord == vol_ord)
            .ok_or_else(|| {
                MetaDbError::Corruption(format!(
                    "lineage GC: vol_ord {vol_ord} not in manifest"
                ))
            })?;
        entry.dead_list_head_pid = new_head_pid;
        if new_head_pid == NULL_PAGE {
            // Chain becomes empty; tail goes too.
            entry.dead_list_tail_pid = NULL_PAGE;
        }
        mstate.manifest.clone()
    };

    // Fault: pages-to-reclaim already chosen, manifest body
    // assembled, but the durable commit hasn't gone through yet.
    // Recovery sees the old head_pid (intact) and the segment
    // pages still live in page_store — no leak, just GC pass
    // didn't make progress.
    ctx.faults
        .inject(FaultPoint::LineageGcPostFreePbasBeforeManifest)?;

    {
        let mut mstate = ctx.manifest_state.lock();
        mstate.store.commit(&manifest_for_commit)?;
    }

    // Manifest commit is durable. Promote atomics so subsequent
    // flushes see the new head/tail. Release ordering pairs with
    // the Acquire reads in flush_with_gate / record_dead.
    vol.dead_list_head_pid
        .store(new_head_pid, Ordering::Release);
    if new_head_pid == NULL_PAGE {
        vol.dead_list_tail_pid.store(NULL_PAGE, Ordering::Release);
    }

    // Fault: manifest committed, atomics promoted, but segment
    // pages haven't been deferred-freed yet. Recovery sees the
    // new head_pid in the manifest; the old segment pages are
    // orphans in the page_store free-list scan and get reclaimed
    // at next open. page_store GC reconciliation closes
    // this leak window properly; accepts it.
    ctx.faults
        .inject(FaultPoint::LineageGcPostHeadAdvanceBeforeFree)?;

    // Drop the gate before the page_store.free_many call — it goes
    // through `deferred_free` which has its own internal lock and
    // doesn't need apply_gate held.
    drop(_gate);

    // Free the old head segment's pages. The generation argument
    // is the page's own birth generation (read out of the head
    // page header earlier) — using a stale value is safe because
    // the manifest commit makes the new head_pid durable, so no
    // subsequent reader will load this segment again.
    let generation = mstate_checkpoint_lsn(ctx);
    let pids: Vec<PageId> = (0..head_page_count as u64)
        .map(|i| old_head + i)
        .collect();
    page_store.free_many(&pids, generation)?;

    Ok(())
}

/// Read the manifest's current `checkpoint_lsn` for use as a
/// page_store.free generation. Held briefly under
/// `manifest_state.lock()`; correctness here only requires the value
/// be >= the segment pages' own generation, which is always true
/// because the segment was written by a flush whose
/// `checkpoint_lsn` predates the most recent manifest commit.
fn mstate_checkpoint_lsn(ctx: &LineageGcCtx) -> Lsn {
    ctx.manifest_state.lock().manifest.checkpoint_lsn
}

/// Test-only synchronous driver for the Lineage GC pass.
/// Identical to what the background worker runs once per cycle;
/// exposed so `db::tests::lineage_gc` can assert head_pid
/// advancement without racing the worker.
#[cfg(test)]
pub(super) fn test_run_lineage_gc_cycle(
    page_store: &Arc<PageStore>,
    ctx: &LineageGcCtx,
) -> Result<usize> {
    lineage_gc_cycle(page_store, ctx)
}
