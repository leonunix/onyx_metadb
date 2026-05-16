//! Refcount shard: composes [`PagedRefcountArray`] + [`DeltaMap`] + a
//! priority-3 staging overlay (drainer-mode).
//!
//! ## Read path (`stage` / `get`)
//!
//! Three layers, consulted in order:
//!
//! 1. `delta_active` — pending ops accumulated since the drainer's
//!    last cycle.
//! 2. `delta_draining` — ops the drainer swapped out of `delta_active`
//!    and is currently building overlay pages from. Visible to readers
//!    until the drainer atomically replaces them with overlay entries.
//! 3. `overlay` — sealed pages produced by the drainer; live until
//!    `begin_checkpoint` harvests them.
//! 4. `array` — on-disk fallback (priority-1 path).
//!
//! ## Apply path (`begin_checkpoint`)
//!
//! Drainer-disabled (priority-1): drains `delta_active`, builds sealed
//! pages in-gate, returns `RcCheckpoint`.
//!
//! Drainer-enabled (priority-3): preempts the drainer, applies any
//! `delta_active` + `delta_draining` final-batch to the overlay, then
//! atomically harvests the overlay into a `RcCheckpoint`. Heavy work
//! (cache miss, page clone, apply, seal) was already performed by the
//! drainer outside `apply_gate.write()`.
//!
//! ## Lock order
//!
//! `delta_active` → `delta_draining` → `overlay.inner` → `array.inner`.
//! Drainer never holds `delta_active` during heavy work.

use std::sync::Arc;
use std::time::Instant;

use parking_lot::Mutex;

use super::RcEntry;
use super::apply_delta_pure;
use super::array::{PagedRefcountArray, StagedDeltas, StagedPage};
use super::delta::{DeltaMap, Pending};
use super::overlay::{DrainerHandle, OverlayEntry, PagePool, StagingOverlay};
use crate::cache::PageCache;
use crate::error::Result;
use crate::metrics::MetaMetrics;
use crate::page::Page;
use crate::page_store::PageStore;
use crate::types::{Lsn, PageId, Pba};

pub struct RcShard {
    /// Live accumulator. New `stage()` ops always merge here.
    delta_active: Mutex<DeltaMap>,
    /// Set by the drainer between swap-out and overlay-publish so
    /// concurrent readers can still see the swapped-out entries.
    /// Always `None` when the drainer is disabled, so the priority-1
    /// path skips the lookup with a single mutex acquire + None peek.
    delta_draining: Mutex<Option<DeltaMap>>,
    /// Sealed pages from the drainer. Empty between drainer cycles
    /// (and always empty when drainer is disabled).
    overlay: Arc<StagingOverlay>,
    pub(super) array: PagedRefcountArray,
    /// Per-shard pool the drainer pulls fresh page ids from. Unused
    /// when drainer is disabled.
    page_pool: Arc<Mutex<PagePool>>,
    /// Drainer thread handle (Some when the drainer is running).
    drainer: Mutex<Option<DrainerHandle>>,
    /// Metrics handle stamped at `attach_drainer` time so
    /// `begin_checkpoint` and `abort_checkpoint` can record drainer-
    /// related counters. None when drainer is disabled.
    metrics: Mutex<Option<Arc<MetaMetrics>>>,
}

/// Checkpoint produced by [`RcShard::begin_checkpoint`]. Carries the
/// sealed pages, the snapshots needed to drive `paged_meta::write_chain`
/// outside the apply gate, and the rollback state for
/// [`RcShard::abort_checkpoint`].
pub struct RcCheckpoint {
    pub(super) staged: StagedDeltas,
    /// Drained delta entries — restored by `abort_checkpoint` so a
    /// retry redoes the work. In drainer-mode this is empty (the
    /// overlay was the source of truth; abort restores the overlay
    /// instead via `prior_overlay_entries`).
    drained_deltas: Vec<(Pba, Pending)>,
    /// In drainer-mode: entries to put back into the overlay on abort
    /// (fresh page ids end up back in the page pool; pre-existing
    /// pages get their pre-stage `OverlayEntry` reinstated).
    prior_overlay_entries: Vec<OverlayEntry>,
    snapshot_page_table: Vec<PageId>,
    snapshot_meta_chain: Vec<PageId>,
    /// True if this checkpoint was produced via the drainer-mode path
    /// (overlay harvest). Determines abort semantics.
    drainer_mode: bool,
}

impl RcCheckpoint {
    /// Empty checkpoint — fast path when nothing was drained / staged.
    pub fn is_empty(&self) -> bool {
        self.staged.is_empty()
    }

    /// Append sealed pages to a shared write-out vec; lifecycle.rs uses
    /// this to fold refcount writes into the same
    /// `page_store.write_sealed_page_runs` + `sync` as L2P.
    pub fn append_sealed_pages(&self, out: &mut Vec<(PageId, Arc<Page>)>) {
        self.staged.append_sealed_pages(out);
    }

    pub(super) fn snapshot_page_table(&self) -> &[PageId] {
        &self.snapshot_page_table
    }

    pub(super) fn snapshot_meta_chain(&self) -> &[PageId] {
        &self.snapshot_meta_chain
    }

    /// Number of delta-map entries the sample phase processed.
    pub fn drained_deltas_count(&self) -> usize {
        // In drainer-mode, the deltas were processed across multiple
        // cycles; here we report the staged-pages count as a proxy
        // (priority-1 callers used this only for metrics).
        if self.drainer_mode {
            self.staged.pages.len()
        } else {
            self.drained_deltas.len()
        }
    }

    /// Number of freshly-allocated data pages this checkpoint produced.
    pub fn fresh_pages_count(&self) -> usize {
        self.staged.pages.iter().filter(|p| p.is_fresh).count()
    }

    #[cfg(test)]
    pub(crate) fn fresh_page_ids(&self) -> Vec<(usize, PageId)> {
        self.staged
            .pages
            .iter()
            .filter(|p| p.is_fresh)
            .map(|p| (p.page_idx, p.page_id))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn touched_existing_page_ids(&self) -> Vec<PageId> {
        self.staged
            .pages
            .iter()
            .filter(|p| !p.is_fresh)
            .map(|p| p.page_id)
            .collect()
    }
}

impl RcShard {
    /// Create a fresh shard. The drainer is NOT spawned by this
    /// constructor; `Db::open` / `Db::create` calls
    /// [`attach_drainer`](Self::attach_drainer) after WAL replay
    /// completes (deterministic recovery).
    pub fn create(page_store: Arc<PageStore>, page_cache: Arc<PageCache>) -> Result<Self> {
        let array = PagedRefcountArray::create(page_store.clone(), page_cache)?;
        Ok(Self::new_with_array(page_store, array))
    }

    /// Open an existing shard at `meta_page_id` (read from the manifest).
    pub fn open(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        meta_page_id: PageId,
    ) -> Result<Self> {
        let array = PagedRefcountArray::open(page_store.clone(), page_cache, meta_page_id)?;
        Ok(Self::new_with_array(page_store, array))
    }

    fn new_with_array(page_store: Arc<PageStore>, array: PagedRefcountArray) -> Self {
        // `MetaMetrics::default()` is fine here — `attach_drainer`
        // re-constructs the pool with the real metrics handle.
        let pool_metrics = Arc::new(MetaMetrics::default());
        let pool = PagePool::new(page_store, 1, pool_metrics);
        Self {
            delta_active: Mutex::new(DeltaMap::new()),
            delta_draining: Mutex::new(None),
            overlay: Arc::new(StagingOverlay::new()),
            array,
            page_pool: Arc::new(Mutex::new(pool)),
            drainer: Mutex::new(None),
            metrics: Mutex::new(None),
        }
    }

    pub fn meta_page_id(&self) -> PageId {
        self.array.meta_page_id()
    }

    /// Logical refcount. ~50 ns hot path: peek `delta_active`, peek
    /// `delta_draining` if needed, peek `overlay` for the page, fall
    /// back to on-disk via `array`.
    pub fn get(&self, pba: Pba) -> Result<u32> {
        let entry = self.lookup_entry(pba)?;
        Ok(entry.rc)
    }

    /// Full entry (rc + birth_lsn). Internal use only — public callers
    /// usually want [`get`].
    pub fn get_entry(&self, pba: Pba) -> Result<RcEntry> {
        self.lookup_entry(pba)
    }

    fn lookup_entry(&self, pba: Pba) -> Result<RcEntry> {
        // Fast path: drainer disabled (priority-1 semantics). Avoids
        // the extra mutex acquires on `delta_draining` and `overlay`
        // — both are guaranteed empty/None when no drainer is
        // attached. Mirrors the original priority-1 read path
        // verbatim.
        let drainer_attached = self.drainer.lock().is_some();
        if !drainer_attached {
            let pending = self.delta_active.lock().get(pba);
            let base = self.array.get(pba)?;
            return match pending {
                Some(p) => apply_delta_pure(base, p.delta, p.last_lsn),
                None => Ok(base),
            };
        }

        // Drainer-mode: hold `delta_active.lock` AND `delta_draining.lock`
        // across the entire peek so the (pa, pd, overlay_entry) snapshot
        // is consistent against drainer transitions:
        //
        //   - Transition 1 (active → draining): drainer takes both
        //     locks atomically. Reader holds active first, so drainer
        //     blocks until reader releases — reader sees full
        //     pre-state or full post-state.
        //
        //   - Transition 2 (draining → overlay): drainer takes
        //     draining.lock, calls `overlay.bulk_insert`, clears
        //     draining, then releases. Reader holds draining.lock
        //     during the overlay peek, so the drainer cannot publish
        //     the new sealed pages while reader is mid-snapshot. This
        //     prevents the "pd=Some(drained) + overlay=new-sealed
        //     (already encodes drained)" double-count bug.
        //
        // Lock order: active → draining → overlay → array (matches
        // both transitions; no deadlock risk).
        let active = self.delta_active.lock();
        let pa = active.get(pba);
        let draining_guard = self.delta_draining.lock();
        let pd = draining_guard.as_ref().and_then(|m| m.get(pba));
        let (_overlay_entry, base) = self.lookup_base(pba)?;
        drop(draining_guard);
        drop(active);

        let after_pd = match pd {
            Some(p) => apply_delta_pure(base, p.delta, p.last_lsn)?,
            None => base,
        };
        match pa {
            Some(p) => apply_delta_pure(after_pd, p.delta, p.last_lsn),
            None => Ok(after_pd),
        }
    }

    /// Resolve the on-disk-or-overlay base for `pba`. Returns the
    /// matching overlay entry alongside the base so callers that also
    /// need `effective_page_lsn` can avoid a second mutex acquire.
    fn lookup_base(&self, pba: Pba) -> Result<(Option<OverlayEntry>, RcEntry)> {
        let (page_idx, slot) = page_offset(pba);
        if let Some(entry) = self.overlay.get(page_idx) {
            // Read the slot from the staged sealed page directly.
            let rc_entry = read_entry_from_sealed(&entry.sealed, slot);
            return Ok((Some(entry), rc_entry));
        }
        let base = self.array.get(pba)?;
        Ok((None, base))
    }

    /// Stage one op into the pending delta. Returns `(prev_rc, new_rc)`.
    pub fn stage(&self, pba: Pba, delta: i64, lsn: Lsn) -> Result<(u32, u32)> {
        // Fast path when drainer is disabled — verbatim priority-1
        // semantics, single mutex acquire.
        let drainer_attached = self.drainer.lock().is_some();
        if !drainer_attached {
            let mut d = self.delta_active.lock();
            let prev_pending = d.get(pba);
            let base = self.array.get(pba)?;
            let merged_prev = match prev_pending {
                Some(p) => apply_delta_pure(base, p.delta, p.last_lsn)?,
                None => base,
            };
            if prev_pending.is_none() && self.array.page_lsn(pba)? >= lsn {
                return Ok((base.rc, base.rc));
            }
            match apply_delta_pure(merged_prev, delta, lsn) {
                Ok(post) => {
                    d.merge(pba, delta, lsn);
                    return Ok((merged_prev.rc, post.rc));
                }
                Err(err) => {
                    // P0 diagnostic: dump every input that fed into the
                    // failed merge so the caller can correlate the
                    // underflow with the L2P op that produced it. Filter
                    // via `RUST_LOG=onyx_metadb::refcount=error`.
                    let page_lsn = self.array.page_lsn(pba).unwrap_or(0);
                    tracing::error!(
                        target: "onyx_metadb::refcount::stage_underflow",
                        pba,
                        delta,
                        lsn,
                        base_rc = base.rc,
                        base_birth_lsn = base.birth_lsn,
                        page_lsn,
                        pa_delta = prev_pending.map(|p| p.delta).unwrap_or(0),
                        pa_lsn = prev_pending.map(|p| p.last_lsn).unwrap_or(0),
                        merged_prev_rc = merged_prev.rc,
                        merged_prev_birth_lsn = merged_prev.birth_lsn,
                        drainer_mode = false,
                        "rc.stage failed: refcount underflow / overflow"
                    );
                    return Err(err);
                }
            }
        }

        // Drainer-mode: hold `delta_active.lock` for the entire op
        // (snapshot → underflow check → merge), and additionally hold
        // `delta_draining.lock` during the `pd` peek + `lookup_base`
        // peek so transition 2 cannot publish a new sealed page while
        // we're mid-snapshot (which would cause `pd=Some(drained) +
        // overlay=new-sealed-already-includes-drained` double count).
        //
        // Lock order: active → draining → overlay → array (matches the
        // drainer's lock acquisition order; no deadlock).
        let mut active = self.delta_active.lock();
        let pa = active.get(pba);
        let draining_guard = self.delta_draining.lock();
        let pd = draining_guard.as_ref().and_then(|m| m.get(pba));
        let (overlay_entry, base) = self.lookup_base(pba)?;
        drop(draining_guard);

        let after_pd = match pd {
            Some(p) => apply_delta_pure(base, p.delta, p.last_lsn)?,
            None => base,
        };
        let merged_prev = match pa {
            Some(p) => apply_delta_pure(after_pd, p.delta, p.last_lsn)?,
            None => after_pd,
        };
        // Replay-skip in drainer-mode: only when there's no pending
        // delta in either layer AND the effective on-disk-or-overlay
        // LSN STRICTLY exceeds this op's LSN. Use `>` not `>=` because
        // `effective_lsn` is the page-wide generation but a single tx
        // at lsn=N can stage ops on multiple slots in the same page;
        // if the rc bucket calls `rc.stage` for those slots back-to-
        // back and the drainer fires its transition-1 swap *between*
        // the stages, the tx ends up split across two cycles. Cycle K
        // builds page_gen=N from slot X; cycle K+1 sees overlay_gen=N
        // when staging slot Y's pending at lsn=N — with `>=` the
        // pending is silently dropped (real soak repro at lsn=18583
        // pba=119434, see `nvme-box:.dev/fio-dedupe-compress-soak/
        // 20260507T-trace-v2/`). The drainer only runs after WAL
        // replay completes (priority-3 contract), so steady-state
        // stage calls always see fresh ops with monotonically
        // increasing LSNs — `effective_lsn == lsn` cannot mean
        // "already applied", it means "another slot in the same
        // page+tx beat us by one drainer cycle". Strict `>` is sound.
        // The drainer-disabled (priority-1) replay-skip above keeps
        // `>=` because that path runs during WAL replay where same-
        // LSN re-application is the actual concern.
        if pa.is_none() && pd.is_none() {
            let effective_lsn = match overlay_entry.as_ref() {
                Some(entry) => entry.sealed.header()?.generation,
                None => self.array.page_lsn(pba)?,
            };
            if effective_lsn > lsn {
                return Ok((base.rc, base.rc));
            }
        }
        let post = match apply_delta_pure(merged_prev, delta, lsn) {
            Ok(p) => p,
            Err(err) => {
                // P0 diagnostic: dump every layer that fed into the
                // drainer-mode merge. `RUST_LOG=onyx_metadb::refcount=error`.
                let page_lsn = self.array.page_lsn(pba).unwrap_or(0);
                let overlay_gen = overlay_entry
                    .as_ref()
                    .and_then(|e| e.sealed.header().ok().map(|h| h.generation));
                tracing::error!(
                    target: "onyx_metadb::refcount::stage_underflow",
                    pba,
                    delta,
                    lsn,
                    base_rc = base.rc,
                    base_birth_lsn = base.birth_lsn,
                    page_lsn,
                    pa_delta = pa.map(|p| p.delta).unwrap_or(0),
                    pa_lsn = pa.map(|p| p.last_lsn).unwrap_or(0),
                    pd_delta = pd.map(|p| p.delta).unwrap_or(0),
                    pd_lsn = pd.map(|p| p.last_lsn).unwrap_or(0),
                    overlay_hit = overlay_entry.is_some(),
                    overlay_gen,
                    merged_prev_rc = merged_prev.rc,
                    merged_prev_birth_lsn = merged_prev.birth_lsn,
                    drainer_mode = true,
                    "rc.stage failed: refcount underflow / overflow"
                );
                return Err(err);
            }
        };
        active.merge(pba, delta, lsn);
        let len = active.len();
        drop(active);
        self.maybe_notify_drainer(len);
        Ok((merged_prev.rc, post.rc))
    }

    fn maybe_notify_drainer(&self, active_len: usize) {
        let drainer = self.drainer.lock();
        if let Some(handle) = drainer.as_ref() {
            // The drainer's threshold is captured in its config; a
            // simple "non-zero work present" notify is also fine
            // because the drainer re-checks the threshold itself.
            let _ = active_len;
            handle.state().notify();
        }
    }

    /// Spawn the per-shard drainer thread. Idempotent (no-op if the
    /// drainer is already attached or if the cfg has it disabled).
    /// Caller `Db::open` / `Db::create` invokes this AFTER WAL replay
    /// completes — guarantees the drainer never observes mid-replay
    /// state.
    pub fn attach_drainer(
        self: &Arc<Self>,
        page_store: Arc<PageStore>,
        cfg: &crate::config::Config,
        metrics: Arc<MetaMetrics>,
        shard_idx: usize,
    ) {
        // Log every entry so an attach failure (config disabled,
        // already attached, or never called) is visible in engine.log
        // without having to grep for negative space.
        if !cfg.refcount_drainer_enabled {
            tracing::info!(
                shard = shard_idx,
                "rc-drainer: attach skipped — refcount_drainer_enabled=false"
            );
            return;
        }
        if self.drainer.lock().is_some() {
            tracing::info!(
                shard = shard_idx,
                "rc-drainer: attach skipped — already attached"
            );
            return;
        }
        tracing::info!(
            shard = shard_idx,
            interval_ms = cfg.refcount_drainer_interval_ms,
            threshold = cfg.refcount_drainer_threshold_entries,
            "rc-drainer: attaching drainer thread"
        );
        // Re-construct the pool with the real metrics handle so
        // `record_rc_drainer_pool_refill` surfaces correctly.
        {
            let mut pool = self.page_pool.lock();
            *pool = PagePool::new(
                page_store,
                cfg.refcount_drainer_alloc_run_size,
                metrics.clone(),
            );
        }
        *self.metrics.lock() = Some(metrics.clone());

        let state = Arc::new(super::overlay::DrainerState::new());
        let worker = DrainerWorker {
            shard_idx,
            shard: self.clone(),
            interval_ms: cfg.refcount_drainer_interval_ms,
            threshold_entries: cfg.refcount_drainer_threshold_entries,
            max_entries_per_cycle: cfg.refcount_drainer_max_entries_per_cycle,
            state: state.clone(),
            metrics,
        };
        let join = std::thread::Builder::new()
            .name(format!("rc-drainer-{shard_idx}"))
            .spawn(move || {
                crate::affinity::bind_current(
                    crate::affinity::ThreadRole::RefcountDrainer,
                    shard_idx,
                );
                worker.run();
            })
            .expect("failed to spawn refcount drainer thread");
        let mut slot = self.drainer.lock();
        *slot = Some(DrainerHandle::new(state, join));
    }

    /// Stop the drainer (if any). Idempotent. Called by `Db::drop`.
    pub fn detach_drainer(&self) {
        let mut slot = self.drainer.lock();
        if let Some(mut handle) = slot.take() {
            handle.shutdown();
        }
        // Return any leftover pool pids to the page store's free
        // list so they aren't leaked. `PageStore::free_many` would
        // be ideal but isn't exposed here in a freelist-friendly
        // way; for now we just drop them — `rebuild_free_list_on_open`
        // recovers them on the next `Db::open`. TODO: explicit return.
        let _ = self.page_pool.lock().drain();
    }

    /// `begin_checkpoint` for both drainer-mode and disabled-mode.
    /// Caller holds `apply_gate.write()`.
    pub fn begin_checkpoint(&self) -> Result<RcCheckpoint> {
        let drainer_mode = self.drainer.lock().is_some();
        if !drainer_mode {
            return self.begin_checkpoint_priority1();
        }
        // Drainer-mode: preempt drainer, final-drain active+draining
        // onto the overlay, atomically harvest the overlay.
        //
        // We deliberately do NOT have a "fall back to priority-1" path
        // here. The priority-1 path (`stage_deltas_in_memory`) reads
        // base values from `inner.page_table` + page_cache only — it
        // doesn't consult the overlay. With drainer-built overlay
        // entries that are NOT yet installed into `page_table` (the
        // sealed pages live only in `StagingOverlay` until the next
        // checkpoint harvest installs them), priority-1 would
        // (a) allocate a brand-new `pid_B` for the same `page_idx` the
        // overlay holds with `pid_A`, (b) start from rc=0 (or stale
        // disk content), losing the drainer's accumulated
        // contribution, and (c) leave the overlay+page_table referring
        // to two different pids for the same idx. The next main-path
        // begin_checkpoint then overwrites `page_table[idx]` back to
        // `pid_A`, orphaning `pid_B`'s priority-1 contribution and
        // producing reader-visible "0 - N" decref underflows on PBAs
        // whose increfs went through pid_B.
        //
        // The main path below already handles arbitrary sizes: it
        // synchronously processes whatever active+draining accrued
        // since the drainer was preempted, and folds onto the
        // overlay's sealed pages without losing any contribution.
        // Large bursts make the in-gate phase longer but never
        // incorrect.
        let drainer_handle = self.drainer.lock();
        let handle = drainer_handle
            .as_ref()
            .expect("drainer was Some on the entry check");
        let wait_started = Instant::now();
        let _wait = handle.preempt_and_wait();
        // Drop the drainer handle lock to avoid holding it during the
        // final-drain heavy work.
        drop(drainer_handle);

        // Final drain: drain delta_active + delta_draining and apply
        // them to the overlay synchronously. Bounded by whatever
        // accrued since the drainer's last cycle.
        let final_drained: Vec<(Pba, Pending)> = {
            let mut active = self.delta_active.lock();
            let mut draining = self.delta_draining.lock();
            let mut out: Vec<(Pba, Pending)> = active.drain().collect();
            if let Some(d) = draining.take() {
                for (pba, pending) in d.iter() {
                    out.push((*pba, *pending));
                }
            }
            out
        };

        if !final_drained.is_empty() {
            let prior = self.overlay.snapshot();
            let build_result = {
                let mut pool = self.page_pool.lock();
                self.array
                    .build_overlay_pages(final_drained.clone(), &mut pool, &prior)
            };
            match build_result {
                Ok(entries) => {
                    self.overlay.bulk_insert(entries);
                }
                Err(err) => {
                    // Restore final_drained back into delta_active so
                    // a retry redoes the work. Without this restore,
                    // those deltas would be silently dropped (they
                    // were already taken out of active+draining
                    // above).
                    let mut active = self.delta_active.lock();
                    for (pba, pending) in final_drained {
                        active.merge_pending(pba, pending);
                    }
                    return Err(err);
                }
            }
        }

        // Atomic harvest.
        let harvested = self.overlay.take();
        let prior_overlay_entries: Vec<OverlayEntry> = harvested.values().cloned().collect();
        let staged_pages: Vec<StagedPage> = harvested
            .into_values()
            .map(|e| StagedPage {
                page_id: e.page_id,
                page_idx: e.page_idx,
                sealed: e.sealed,
                is_fresh: e.is_fresh,
            })
            .collect();

        // Install the harvested page-table mutations into `inner`. This
        // is the in-gate moment that priority 1 did during sample for
        // each individual fresh page.
        let max_lsn = match self
            .array
            .install_overlay_into_page_table(prior_overlay_entries.iter())
        {
            Ok(lsn) => lsn,
            Err(err) => {
                // Roll back the harvest: restore overlay so the next
                // checkpoint sees the same state we started with.
                self.overlay.bulk_insert(prior_overlay_entries);
                return Err(err);
            }
        };

        let snapshot_page_table = self.array.page_table_snapshot();
        let snapshot_meta_chain = self.array.meta_chain_snapshot();

        if let Some(metrics) = self.drainer_metrics() {
            metrics.record_rc_drainer_checkpoint_wait(wait_started.elapsed());
        }

        Ok(RcCheckpoint {
            staged: StagedDeltas {
                pages: staged_pages,
                max_lsn,
            },
            drained_deltas: Vec::new(),
            prior_overlay_entries,
            snapshot_page_table,
            snapshot_meta_chain,
            drainer_mode: true,
        })
    }

    fn drainer_metrics(&self) -> Option<Arc<MetaMetrics>> {
        self.metrics.lock().clone()
    }

    /// Priority-1 in-gate sample path. Only used when the drainer is
    /// disabled. Drainer-enabled callers always go through the main
    /// path above which folds the overlay correctly; the backpressure
    /// fallback that used to call this in drainer-mode was removed
    /// because it lost overlay contributions (see the comment in
    /// `begin_checkpoint`).
    fn begin_checkpoint_priority1(&self) -> Result<RcCheckpoint> {
        let drained: Vec<(Pba, Pending)> = {
            let mut d = self.delta_active.lock();
            d.drain().collect()
        };
        if drained.is_empty() {
            return Ok(RcCheckpoint {
                staged: StagedDeltas {
                    pages: Vec::new(),
                    max_lsn: 0,
                },
                drained_deltas: Vec::new(),
                prior_overlay_entries: Vec::new(),
                snapshot_page_table: self.array.page_table_snapshot(),
                snapshot_meta_chain: self.array.meta_chain_snapshot(),
                drainer_mode: false,
            });
        }
        let staged = match self.array.stage_deltas_in_memory(drained.clone()) {
            Ok(s) => s,
            Err(err) => {
                let mut d = self.delta_active.lock();
                for (pba, pending) in drained {
                    d.merge(pba, pending.delta, pending.last_lsn);
                }
                return Err(err);
            }
        };
        let snapshot_page_table = self.array.page_table_snapshot();
        let snapshot_meta_chain = self.array.meta_chain_snapshot();
        Ok(RcCheckpoint {
            staged,
            drained_deltas: drained,
            prior_overlay_entries: Vec::new(),
            snapshot_page_table,
            snapshot_meta_chain,
            drainer_mode: false,
        })
    }

    /// Outside-gate IO: write a fresh meta chain. Same semantics as
    /// priority 1; drainer-mode and disabled-mode share this path.
    ///
    /// Cold-path shim (`RcShard::flush`, snapshot / drop_volume). The
    /// flush hot path uses [`Self::build_meta_chain`] + folds the
    /// sealed pages into the global checkpoint batch.
    pub fn write_meta_chain(&self, ckpt: &RcCheckpoint, free_lsn: Lsn) -> Result<Vec<PageId>> {
        if ckpt.is_empty() {
            return Ok(ckpt.snapshot_meta_chain.to_vec());
        }
        self.array.write_meta_chain_external(
            ckpt.snapshot_page_table(),
            ckpt.snapshot_meta_chain(),
            free_lsn,
        )
    }

    /// Outside-gate, **no-IO** companion of [`Self::write_meta_chain`]:
    /// builds + seals every page in the new chain entirely in memory
    /// and returns the chain layout. Callers (currently
    /// `flush_with_gate`) drive one batched
    /// [`PageStore::write_sealed_page_runs`] across every shard's
    /// sealed pages, then walk the per-shard `to_free` lists +
    /// `install_meta_chain` after the manifest commit is durable.
    pub fn build_meta_chain(
        &self,
        ckpt: &RcCheckpoint,
    ) -> Result<(Vec<PageId>, Vec<(PageId, Arc<crate::page::Page>)>, Vec<PageId>)> {
        if ckpt.is_empty() {
            return Ok((ckpt.snapshot_meta_chain.to_vec(), Vec::new(), Vec::new()));
        }
        self.array.build_meta_chain_external(
            ckpt.snapshot_page_table(),
            ckpt.snapshot_meta_chain(),
        )
    }

    /// Install the new meta chain. Briefly takes the array's inner
    /// lock. Drainer-mode finishes here too; the post-install resume
    /// of the drainer is the caller's responsibility (`Db::flush`).
    pub fn install_meta_chain(&self, new_chain: Vec<PageId>) {
        self.array.install_meta_chain(new_chain);
    }

    /// Resume the drainer after `begin_checkpoint` + IO + install
    /// have finished. No-op if no drainer is attached.
    pub fn resume_drainer(&self) {
        if let Some(handle) = self.drainer.lock().as_ref() {
            handle.resume();
        }
    }

    /// Roll back a checkpoint that failed before install. Restores
    /// drained deltas (priority-1 path) or overlay entries (drainer
    /// path) so a retry redoes the work.
    pub fn abort_checkpoint(&self, ckpt: RcCheckpoint, free_lsn: Lsn) {
        if ckpt.is_empty() {
            return;
        }
        if ckpt.drainer_mode {
            // Restore the harvested entries back into the overlay.
            // The drainer will publish them again on its next cycle —
            // OR begin_checkpoint will harvest them on retry. Either
            // way the work isn't lost. Fresh page ids stay valid (we
            // already allocated them; install_overlay_into_page_table
            // wrote them into inner.page_table).
            for entry in ckpt.prior_overlay_entries {
                self.overlay.insert(entry);
            }
        } else {
            // Priority-1 abort: restore drained deltas + free fresh
            // page ids + invalidate touched cache entries.
            self.array.abort_staged_deltas(&ckpt.staged, free_lsn);
            let mut d = self.delta_active.lock();
            for (pba, pending) in ckpt.drained_deltas {
                d.merge(pba, pending.delta, pending.last_lsn);
            }
        }
    }

    /// Synchronous flush for non-checkpoint callers. Preempts the
    /// drainer (no-op if disabled), drains everything to disk, rotates
    /// the meta chain. Cold path.
    pub fn flush(&self) -> Result<()> {
        let ckpt = self.begin_checkpoint()?;
        if ckpt.is_empty() {
            self.resume_drainer();
            return Ok(());
        }
        if let Err(err) = self.array.write_staged_pages(&ckpt.staged) {
            self.abort_checkpoint(ckpt, 0);
            self.resume_drainer();
            return Err(err);
        }
        let new_chain = match self.write_meta_chain(&ckpt, 0) {
            Ok(c) => c,
            Err(err) => {
                self.abort_checkpoint(ckpt, 0);
                self.resume_drainer();
                return Err(err);
            }
        };
        self.install_meta_chain(new_chain);
        self.resume_drainer();
        Ok(())
    }

    /// Iterate every live entry. Forces a flush first.
    pub fn iter_live_flushed(&self) -> Result<Vec<(Pba, RcEntry)>> {
        self.flush()?;
        self.array.iter_live()
    }

    /// Iterate every live entry already present in the backing array.
    /// The caller is responsible for checkpointing or otherwise
    /// draining pending deltas first.
    pub fn iter_live(&self) -> Result<Vec<(Pba, RcEntry)>> {
        self.array.iter_live()
    }

    /// Number of data pages currently on disk for this shard.
    pub fn allocated_data_pages(&self) -> usize {
        self.array.allocated_data_pages()
    }

    /// Best-effort count of in-memory rc deltas awaiting a checkpoint
    /// drain. Used by the watermark thread to threshold-trigger
    /// `try_flush` so a single sample doesn't accumulate millions of
    /// deltas (sample_max scales linearly with this count — see
    /// [[parallel-rc-drain-landed]]).
    ///
    /// `try_lock` rather than `lock` so a slow shard doesn't stall
    /// the diag/watermark path; an undercounted shard just means
    /// the trigger lags by one tick.
    pub fn pending_delta_count(&self) -> usize {
        self.delta_active
            .try_lock()
            .map(|d| d.len())
            .unwrap_or(0)
    }
}

impl Drop for RcShard {
    fn drop(&mut self) {
        // Best-effort drainer shutdown; idempotent.
        self.detach_drainer();
    }
}

/// Background drainer worker. Owns an `Arc<RcShard>` so it can reach
/// into per-shard state without duplicating Arc handles for each
/// field.
struct DrainerWorker {
    shard_idx: usize,
    shard: Arc<RcShard>,
    interval_ms: u64,
    threshold_entries: usize,
    max_entries_per_cycle: usize,
    state: Arc<super::overlay::DrainerState>,
    metrics: Arc<MetaMetrics>,
}

impl DrainerWorker {
    fn run(self) {
        use std::sync::atomic::Ordering;
        use std::time::Duration;
        let interval = Duration::from_millis(self.interval_ms);
        tracing::info!(
            shard = self.shard_idx,
            interval_ms = self.interval_ms,
            threshold = self.threshold_entries,
            "rc-drainer: worker thread entered run loop"
        );
        // Track local cycle count so we can emit a one-shot "first
        // cycle completed" log per shard (confirms the cycle path
        // actually fires, not just the wake path).
        let mut cycle_count: u64 = 0;
        loop {
            // Park until tick / threshold / preempt / shutdown.
            {
                let mut guard = self.state.mu.lock();
                loop {
                    if self.state.shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    if self.state.preempt.load(Ordering::Acquire) {
                        self.metrics.record_rc_drainer_preempt();
                        self.state.in_cycle.store(false, Ordering::Release);
                        self.state.preempt_done.store(true, Ordering::Release);
                        self.state.cv.notify_all();
                        while self.state.preempt.load(Ordering::Acquire)
                            && !self.state.shutdown.load(Ordering::Acquire)
                        {
                            self.state.cv.wait(&mut guard);
                        }
                        continue;
                    }
                    if self.shard.delta_active.lock().len() >= self.threshold_entries {
                        break;
                    }
                    let _ = self.state.cv.wait_for(&mut guard, interval);
                    if self.state.shutdown.load(Ordering::Acquire) {
                        return;
                    }
                    if self.state.preempt.load(Ordering::Acquire) {
                        continue;
                    }
                    if !self.shard.delta_active.lock().is_empty() {
                        break;
                    }
                }
            }

            // Exited the park: we are about to attempt a cycle.
            // Comparing `wakes` to `cycles` discriminates "worker dead"
            // (wakes=0) vs "spinning, never finds work" (wakes>0,
            // cycles=0) vs "preempted out" (preempts ≈ wakes).
            self.metrics.record_rc_drainer_wake();

            // Run one cycle.
            self.state.in_cycle.store(true, Ordering::Release);
            let cycle_started = Instant::now();

            // Bounded-drain optimization (max_entries_per_cycle cap)
            // is deferred — current `DeltaMap` doesn't expose
            // entry removal; full take is the conservative path. Big
            // bursts surface as a longer in-gate phase at the next
            // `begin_checkpoint` (which folds the entire overlay) but
            // stay correct.
            let _ = self.max_entries_per_cycle;

            // ── Transition 1: active → draining ───────────────────────
            // Hold both `delta_active` and `delta_draining` locks at
            // the same time so concurrent readers (which take
            // `delta_active` first) cannot witness the in-flight gap
            // where entries belong to neither layer. From the reader's
            // perspective this transition is atomic: they observe
            // either pre-state (active=Some, draining=None) or
            // post-state (active=None, draining=Some).
            let drained: DeltaMap;
            let drained_len: usize;
            {
                let mut active = self.shard.delta_active.lock();
                if active.is_empty() {
                    drop(active);
                    self.state.in_cycle.store(false, Ordering::Release);
                    continue;
                }
                let mut draining = self.shard.delta_draining.lock();
                let d = std::mem::take(&mut *active);
                drained_len = d.len();
                let clone: DeltaMap = {
                    let mut c = DeltaMap::new();
                    for (pba, pending) in d.iter() {
                        c.merge(*pba, pending.delta, pending.last_lsn);
                    }
                    c
                };
                *draining = Some(clone);
                drained = d;
                // Both locks drop here.
            }

            // ── Heavy work: build sealed pages outside any shard lock ─
            let entries: Vec<(Pba, Pending)> = drained.iter().map(|(p, pp)| (*p, *pp)).collect();
            let prior = self.shard.overlay.snapshot();
            let build_result = {
                let mut pool = self.shard.page_pool.lock();
                self.shard
                    .array
                    .build_overlay_pages(entries, &mut pool, &prior)
            };
            let new_pages = match build_result {
                Ok(p) => p,
                Err(err) => {
                    tracing::warn!(
                        shard = self.shard_idx,
                        error = %err,
                        "rc-drainer: build_overlay_pages failed; rolling drained back"
                    );
                    // ── Error rollback: atomic restore ──
                    // Restore drained back into active AND clear
                    // draining under both locks so readers cannot
                    // double-count (pa=Some + pd=Some on the same
                    // entries) or undercount (pa=None + pd=None).
                    {
                        let mut active = self.shard.delta_active.lock();
                        let mut draining = self.shard.delta_draining.lock();
                        for (pba, pending) in drained.iter() {
                            active.merge(*pba, pending.delta, pending.last_lsn);
                        }
                        *draining = None;
                    }
                    self.state.in_cycle.store(false, Ordering::Release);
                    continue;
                }
            };
            let pages_built = new_pages.len();

            // ── Transition 2: draining → overlay ─────────────────────
            // Hold `delta_draining.lock` for the entire publish-and-
            // clear so no reader observes (pd=Some AND overlay_entry
            // for the same page) — that combination would double-count
            // because the sealed page already encodes the drained
            // contributions. `bulk_insert` performs the overlay write
            // under a single overlay-mutex acquire so the publish is
            // atomic on its own; the outer `delta_draining` lock makes
            // the overall pre/post atomic from the reader's view.
            {
                let mut draining = self.shard.delta_draining.lock();
                self.shard.overlay.bulk_insert(new_pages);
                *draining = None;
            }

            let elapsed = cycle_started.elapsed();
            let overlay_size = self.shard.overlay.approx_size();
            self.metrics
                .record_rc_drainer_cycle(drained_len, pages_built, elapsed, overlay_size);
            cycle_count += 1;
            // One-shot log per shard confirming the cycle path actually
            // fires (the bare metric counter doesn't distinguish "first
            // ever cycle" from "ongoing"; this lets us catch a shard
            // whose worker thread started but never reached a cycle).
            if cycle_count == 1 {
                tracing::info!(
                    shard = self.shard_idx,
                    drained_len,
                    pages_built,
                    elapsed_us = elapsed.as_micros() as u64,
                    "rc-drainer: first cycle completed"
                );
            }
            self.state.in_cycle.store(false, Ordering::Release);
            self.state.cv.notify_all();
        }
    }
}

/// Read one slot from a sealed `Arc<Page>` produced by the drainer or
/// by `stage_deltas_in_memory`. Mirrors `array::read_entry` but takes
/// a `Page` rather than `&Page` for sealed-from-Arc convenience.
fn read_entry_from_sealed(page: &Arc<Page>, slot: usize) -> RcEntry {
    let payload = page.payload();
    let off = slot * super::array::ENTRY_BYTES;
    let rc = u32::from_le_bytes(payload[off..off + 4].try_into().unwrap());
    let birth_lsn = u64::from_le_bytes(payload[off + 4..off + 12].try_into().unwrap());
    RcEntry { rc, birth_lsn }
}

#[inline]
fn page_offset(pba: Pba) -> (usize, usize) {
    let pba = pba as usize;
    let entries = super::array::ENTRIES_PER_PAGE;
    (pba / entries, pba % entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_shard() -> (TempDir, RcShard) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let page_store = Arc::new(PageStore::create(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let s = RcShard::create(page_store, page_cache).unwrap();
        (dir, s)
    }

    #[test]
    fn stage_then_get_sees_pending() {
        let (_d, s) = make_shard();
        assert_eq!(s.stage(10, 1, 100).unwrap(), (0, 1));
        assert_eq!(s.get(10).unwrap(), 1);
    }

    #[test]
    fn stage_accumulates_across_ops() {
        let (_d, s) = make_shard();
        s.stage(10, 1, 100).unwrap();
        s.stage(10, 2, 101).unwrap();
        s.stage(10, -1, 102).unwrap();
        assert_eq!(s.get(10).unwrap(), 2);
    }

    #[test]
    fn flush_moves_pending_to_array() {
        let (_d, s) = make_shard();
        s.stage(10, 5, 100).unwrap();
        s.flush().unwrap();
        assert_eq!(s.get(10).unwrap(), 5);
        assert_eq!(
            s.get_entry(10).unwrap(),
            RcEntry {
                rc: 5,
                birth_lsn: 100
            }
        );
    }

    #[test]
    fn stage_underflow_does_not_corrupt_delta() {
        let (_d, s) = make_shard();
        s.stage(10, 1, 1).unwrap();
        assert!(s.stage(10, -2, 2).is_err());
        assert_eq!(s.get(10).unwrap(), 1);
    }

    #[test]
    fn zero_to_one_to_zero_to_one_birth_lsn() {
        let (_d, s) = make_shard();
        s.stage(10, 1, 100).unwrap();
        s.flush().unwrap();
        s.stage(10, -1, 101).unwrap();
        s.flush().unwrap();
        assert_eq!(s.get_entry(10).unwrap(), RcEntry::ZERO);
        s.stage(10, 1, 200).unwrap();
        s.flush().unwrap();
        assert_eq!(
            s.get_entry(10).unwrap(),
            RcEntry {
                rc: 1,
                birth_lsn: 200
            }
        );
    }

    #[test]
    fn iter_live_flushed_skips_zero() {
        let (_d, s) = make_shard();
        s.stage(1, 1, 1).unwrap();
        s.stage(2, 1, 1).unwrap();
        s.stage(2, -1, 2).unwrap();
        s.stage(3, 3, 3).unwrap();
        let live = s.iter_live_flushed().unwrap();
        assert_eq!(live.len(), 2);
        assert_eq!(live[0].0, 1);
        assert_eq!(live[1].0, 3);
    }

    #[test]
    fn round_trip_via_open() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let meta_page_id;
        {
            let page_store = Arc::new(PageStore::create(&path).unwrap());
            let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
            let s = RcShard::create(page_store, page_cache).unwrap();
            meta_page_id = s.meta_page_id();
            s.stage(10, 5, 100).unwrap();
            s.stage(20, 2, 200).unwrap();
            s.flush().unwrap();
        }
        let page_store = Arc::new(PageStore::open(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let s = RcShard::open(page_store, page_cache, meta_page_id).unwrap();
        assert_eq!(s.get(10).unwrap(), 5);
        assert_eq!(s.get(20).unwrap(), 2);
        assert_eq!(s.get_entry(10).unwrap().birth_lsn, 100);
    }

    #[test]
    fn begin_checkpoint_drains_delta_and_stages_without_overwriting_disk() {
        // Drainer-disabled path (priority-1) — verifies the original
        // semantics still hold.
        let (_d, s) = make_shard();
        s.stage(10, 5, 100).unwrap();
        s.stage(20, 3, 100).unwrap();

        let ckpt = s.begin_checkpoint().unwrap();
        assert_eq!(s.get(10).unwrap(), 5, "stage value visible via cache");
        assert_eq!(s.get(20).unwrap(), 3);
        assert!(s.allocated_data_pages() >= 1);
        assert!(!ckpt.is_empty());
        s.abort_checkpoint(ckpt, 0);
        assert_eq!(s.get(10).unwrap(), 5);
        assert_eq!(s.get(20).unwrap(), 3);
    }

    #[test]
    fn checkpoint_pipeline_round_trips_through_disk() {
        let (_d, s) = make_shard();
        s.stage(7, 2, 50).unwrap();
        s.stage(800, 4, 60).unwrap();

        let ckpt = s.begin_checkpoint().unwrap();
        assert!(!ckpt.is_empty());
        assert_eq!(ckpt.fresh_page_ids().len(), 2);

        s.array.write_staged_pages(&ckpt.staged).unwrap();
        let new_chain = s.write_meta_chain(&ckpt, 0).unwrap();
        s.install_meta_chain(new_chain);
        assert_eq!(s.get(7).unwrap(), 2);
        assert_eq!(s.get(800).unwrap(), 4);
    }

    #[test]
    fn abort_then_retry_does_not_double_apply_via_replay_skip() {
        let (_d, s) = make_shard();
        s.stage(10, 5, 100).unwrap();

        let ckpt = s.begin_checkpoint().unwrap();
        s.array.write_staged_pages(&ckpt.staged).unwrap();
        let _ = s.write_meta_chain(&ckpt, 0).unwrap();
        s.abort_checkpoint(ckpt, 0);
        assert_eq!(s.get(10).unwrap(), 5, "value still observable post-abort");
        s.flush().unwrap();
        assert_eq!(s.get(10).unwrap(), 5, "no double-apply on retry");
        assert_eq!(s.get_entry(10).unwrap().birth_lsn, 100);
    }

    #[test]
    fn empty_checkpoint_is_no_op() {
        let (_d, s) = make_shard();
        let ckpt = s.begin_checkpoint().unwrap();
        assert!(ckpt.is_empty());
        let new_chain = s.write_meta_chain(&ckpt, 0).unwrap();
        s.install_meta_chain(new_chain);
        let ckpt2 = s.begin_checkpoint().unwrap();
        s.abort_checkpoint(ckpt2, 0);
    }

    #[test]
    fn many_ops_one_shard_correctness() {
        use std::collections::HashMap;
        let (_d, s) = make_shard();
        let mut model: HashMap<Pba, i64> = HashMap::new();
        let ops: Vec<(Pba, i64, Lsn)> = (1u64..1000)
            .map(|i| {
                let pba = (i * 7) % (super::super::ENTRIES_PER_PAGE as u64 * 5);
                let delta = if i % 3 == 0 { -1i64 } else { 1i64 };
                (pba, delta, i)
            })
            .collect();
        for &(pba, delta, lsn) in &ops {
            let model_prev = *model.get(&pba).unwrap_or(&0);
            let model_new = model_prev + delta;
            if model_new < 0 {
                continue;
            }
            let (_prev, new) = s.stage(pba, delta, lsn).unwrap();
            assert_eq!(new, model_new as u32);
            model.insert(pba, model_new);
        }
        s.flush().unwrap();
        for (&pba, &expected) in &model {
            assert_eq!(s.get(pba).unwrap(), expected as u32, "pba {pba}");
        }
    }
}
