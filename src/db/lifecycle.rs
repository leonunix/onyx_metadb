use super::*;

mod flush;
mod manifest_refresh;
mod open;

const FLUSH_RECLAIM_MIN_BUDGET_PAGES: usize = 4_096;
const FLUSH_RECLAIM_MAX_BUDGET_PAGES: usize = 1_048_576;
const FLUSH_RECLAIM_BACKLOG_HARD_CAP_PAGES: usize = 16 * 1_048_576;
const FLUSH_INSTALL_PAGE_BUDGET: usize = 64;
const FLUSH_INSTALL_CLEANUP_BUDGET: usize = 64;
const FLUSH_INSTALL_STEP_WARN_US: u64 = 100_000;
const TERMINAL_RECLAIM_CHUNK_PAGES: usize = 65_536;
const TERMINAL_RECLAIM_MAX_ROUNDS: usize = 16;

fn micros(duration: std::time::Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn flush_reclaim_budget(pending_reclaim_pages: usize, pages_written: usize) -> usize {
    let write_scaled = pages_written.saturating_mul(8);
    let backlog_scaled = pending_reclaim_pages / 2;
    let pressure_cap = if pending_reclaim_pages >= FLUSH_RECLAIM_BACKLOG_HARD_CAP_PAGES {
        FLUSH_RECLAIM_MAX_BUDGET_PAGES
    } else if pending_reclaim_pages >= 4 * 1_048_576 {
        FLUSH_RECLAIM_MAX_BUDGET_PAGES / 2
    } else {
        FLUSH_RECLAIM_MAX_BUDGET_PAGES / 8
    };
    FLUSH_RECLAIM_MIN_BUDGET_PAGES
        .max(write_scaled)
        .max(backlog_scaled)
        .min(pressure_cap)
}

/// Per-flush selection of shards that this `flush_with_gate`
/// invocation will sample. `l2p[v][s]` mirrors
/// `volumes[v].shards[s]`; `rc[i]` mirrors `refcount_shards[i]`.
/// True = sample this shard this round, write a new sealed root +
/// bump its `last_flushed_lsn`. False = leave it alone; its root
/// in the manifest and its in-memory dirty pages carry over to the
/// next flush.
#[derive(Debug, Clone)]
struct SelectedShards {
    l2p: Vec<Vec<bool>>,
    rc: Vec<bool>,
}

impl SelectedShards {
    fn l2p_any(&self) -> bool {
        self.l2p.iter().any(|v| v.iter().any(|s| *s))
    }
    fn rc_any(&self) -> bool {
        self.rc.iter().any(|s| *s)
    }
    fn is_empty(&self) -> bool {
        !self.l2p_any() && !self.rc_any()
    }
}

struct CheckpointInstallReceiver {
    kind: &'static str,
    vol_ord: Option<VolumeOrdinal>,
    shard: usize,
    rx: crossbeam_channel::Receiver<Result<Vec<PageId>>>,
}

/// One volume's drained dead-list records during a flush round.
/// Carried from the drain step (with `death_lsn <= wal_checkpoint`
/// filter — no sample-phase gate any more, so this filter is the
/// load-bearing bound on what gets folded into a segment) through
/// segment build + IO + manifest commit. On any failure between drain
/// and commit, `records` is moved back into the volume's buffer via
/// `restore_front` (see
/// [`crate::deadlist::DeadListState::restore_front`]).
struct DeadListDrainEntry {
    vol: Arc<Volume>,
    records: DrainRecords,
    old_head: PageId,
    old_tail: PageId,
    kind: DeadListKind,
    /// H1: per-shard provenance of a `Page` entry's drained records. The
    /// `records` field is the concatenation of these (one merged segment per
    /// volume keeps the chain single), but rollback must restore each sub-vec
    /// into `page_dead_list[shard_idx]` — a flat restore into shard 0 would
    /// permanently mis-bound a non-shard-0 death (shard 0's next-cycle
    /// `root_birth_lsn` could exclude it forever). Empty for `Pba` / `Live`.
    page_provenance: Vec<(usize, Vec<crate::deadlist::DeadRecord>)>,
}

/// Drained records for one chain. PBA / page deadlists carry
/// [`crate::deadlist::DeadRecord`]s; the per-clone page-livelist (BFG
/// ) carries [`crate::livelist::LiveRecord`]s (ALLOC/FREE + kind
/// byte). Kept as one enum so the shared flush drain→build→write→
/// rollback→promote machinery dispatches on it rather than duplicating
/// ~10 rollback call sites.
enum DrainRecords {
    Dead(Vec<crate::deadlist::DeadRecord>),
    Live(Vec<crate::livelist::LiveRecord>),
}

impl DrainRecords {
    fn is_empty(&self) -> bool {
        match self {
            DrainRecords::Dead(r) => r.is_empty(),
            DrainRecords::Live(r) => r.is_empty(),
        }
    }
}

/// Which of a volume's independent dead/live-list chains a drain
/// entry / segment plan belongs to. The PBA chain (`Pba`) records
/// data-block deaths for lineage GC; the page chain (`Page`, BFG
/// ) records L2P-metadata-page deaths for `drop_snapshot`; both
/// reuse the `DeadListSegment` codec and differ only in the buffer
/// drained + the manifest anchors promoted. The `Live` chain (BFG
/// ) is the per-clone page-livelist (ALLOC/FREE of clone-private
/// pages) using the separate `LiveListSegment` codec + the
/// `page_live_list_*_pid` anchors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeadListKind {
    Pba,
    Page,
    Live,
}

/// One volume's allocated segment in the IO phase: the contiguous page
/// run handed out by `page_store.allocate_run`. Used both for tracking
/// what to free on rollback and what to commit into the volume's
/// `dead_list_*_pid` atomics + manifest after sync succeeds.
struct DeadListSegmentPlan {
    vol: Arc<Volume>,
    start_pid: PageId,
    page_count: u32,
    old_head: PageId,
    old_tail: PageId,
    kind: DeadListKind,
}

/// RAII counterpart for the async dedup-index drainers: the flush
/// checkpoint barrier preempts them + final-drains staging, and this
/// guard re-arms them on every flush exit path. `resume_drainers` is
/// idempotent (no-op when the drainer is disabled / not attached).
struct DedupDrainerResumeGuard<'a> {
    dedup_index: &'a std::sync::Arc<crate::dedup::DedupIndex>,
}

impl Drop for DedupDrainerResumeGuard<'_> {
    fn drop(&mut self) {
        self.dedup_index.resume_drainers();
    }
}

struct CheckpointInstallState<F> {
    flushed: F,
    flushed_pages: usize,
    next_flushed_page: usize,
    flushed_private: HashMap<PageId, bool>,
    retired_pages: Vec<PageId>,
    next_retired_page: usize,
    private_pages: Vec<PageId>,
    next_private_page: usize,
    checkpoint_frees: Vec<PageId>,
    steps_started: u64,
}

impl<F> CheckpointInstallState<F> {
    fn new(
        flushed: F,
        flushed_pages: usize,
        retired_pages: Vec<PageId>,
        private_pages: Vec<PageId>,
    ) -> Self {
        Self {
            flushed,
            flushed_pages,
            next_flushed_page: 0,
            flushed_private: HashMap::new(),
            retired_pages,
            next_retired_page: 0,
            private_pages,
            next_private_page: 0,
            checkpoint_frees: Vec::new(),
            steps_started: 0,
        }
    }

    fn page_phases_finished(&self) -> bool {
        self.next_flushed_page >= self.flushed_pages
            && self.next_retired_page >= self.retired_pages.len()
            && self.next_private_page >= self.private_pages.len()
    }
}

fn enqueue_l2p_checkpoint_install_step(
    lane: ApplyLaneHandle,
    volume: Arc<Volume>,
    sid: usize,
    state: Arc<Mutex<CheckpointInstallState<crate::paged::cache::FlushedSnapshot>>>,
    tx: crossbeam_channel::Sender<Result<Vec<PageId>>>,
) {
    let next_lane = lane.clone();
    let enqueued_at = std::time::Instant::now();
    lane.enqueue_maintenance(Box::new(move || {
        match run_l2p_checkpoint_install_step(
            volume.clone(),
            sid,
            state.clone(),
            enqueued_at.elapsed(),
        ) {
            Ok(Some(frees)) => {
                let _ = tx.send(Ok(frees));
            }
            Ok(None) => {
                enqueue_l2p_checkpoint_install_step(next_lane, volume, sid, state, tx);
            }
            Err(err) => {
                let _ = tx.send(Err(err));
            }
        }
    }));
}

fn run_l2p_checkpoint_install_step(
    volume: Arc<Volume>,
    sid: usize,
    state: Arc<Mutex<CheckpointInstallState<crate::paged::cache::FlushedSnapshot>>>,
    queue_wait: std::time::Duration,
) -> Result<Option<Vec<PageId>>> {
    let total_started = std::time::Instant::now();
    let state_lock_started = std::time::Instant::now();
    let mut state = state.lock();
    let state_lock_elapsed = state_lock_started.elapsed();
    state.steps_started += 1;
    let step = state.steps_started;
    let start_flushed = state.next_flushed_page;
    let start_retired = state.next_retired_page;
    let start_private = state.next_private_page;
    let tree_lock_started = std::time::Instant::now();
    let mut tree = volume.shards[sid].tree.write();
    let tree_lock_elapsed = tree_lock_started.elapsed();
    let mut budget = FLUSH_INSTALL_PAGE_BUDGET;

    let pages_started = std::time::Instant::now();
    while budget > 0 && state.next_flushed_page < state.flushed_pages {
        let page_idx = state.next_flushed_page;
        if let Some((pid, clean)) = tree.install_flushed_checkpoint_page(&state.flushed, page_idx) {
            state.flushed_private.insert(pid, clean);
        }
        state.next_flushed_page += 1;
        budget -= 1;
    }

    while budget > 0 && state.next_retired_page < state.retired_pages.len() {
        let pid = state.retired_pages[state.next_retired_page];
        if let Some(pid) = tree.checkpoint_retired_page_committed(pid) {
            state.checkpoint_frees.push(pid);
        }
        state.next_retired_page += 1;
        budget -= 1;
    }

    while budget > 0 && state.next_private_page < state.private_pages.len() {
        let pid = state.private_pages[state.next_private_page];
        let flushed_clean = state.flushed_private.get(&pid).copied().unwrap_or(true);
        tree.checkpoint_private_page_committed(pid, flushed_clean);
        state.next_private_page += 1;
        budget -= 1;
    }
    let pages_elapsed = pages_started.elapsed();

    let cleanup_started = std::time::Instant::now();
    let cleanup_done = tree.finish_checkpoint_commit_step(FLUSH_INSTALL_CLEANUP_BUDGET)?;
    let cleanup_elapsed = cleanup_started.elapsed();
    let page_phases_finished = state.page_phases_finished();
    let done = page_phases_finished && cleanup_done;
    let checkpoint_frees = state.checkpoint_frees.len();
    let result = if !done {
        None
    } else {
        Some(std::mem::take(&mut state.checkpoint_frees))
    };
    let total_elapsed = total_started.elapsed();
    let queue_wait_us = micros(queue_wait);
    let total_us = micros(total_elapsed);
    let state_lock_us = micros(state_lock_elapsed);
    let tree_lock_us = micros(tree_lock_elapsed);
    let pages_us = micros(pages_elapsed);
    let cleanup_us = micros(cleanup_elapsed);
    if queue_wait_us >= FLUSH_INSTALL_STEP_WARN_US
        || total_us >= FLUSH_INSTALL_STEP_WARN_US
        || state_lock_us >= FLUSH_INSTALL_STEP_WARN_US
        || tree_lock_us >= FLUSH_INSTALL_STEP_WARN_US
        || pages_us >= FLUSH_INSTALL_STEP_WARN_US
        || cleanup_us >= FLUSH_INSTALL_STEP_WARN_US
    {
        tracing::warn!(
            kind = "l2p",
            vol_ord = volume.ord,
            shard = sid,
            step,
            queue_wait_us,
            total_us,
            state_lock_us,
            tree_lock_us,
            pages_us,
            cleanup_us,
            flushed_done = state.next_flushed_page,
            flushed_total = state.flushed_pages,
            retired_done = state.next_retired_page,
            retired_total = state.retired_pages.len(),
            private_done = state.next_private_page,
            private_total = state.private_pages.len(),
            flushed_step = state.next_flushed_page.saturating_sub(start_flushed),
            retired_step = state.next_retired_page.saturating_sub(start_retired),
            private_step = state.next_private_page.saturating_sub(start_private),
            cleanup_done,
            page_phases_finished,
            done,
            checkpoint_frees,
            "metadb: slow checkpoint install step"
        );
    }
    if let Some(frees) = result {
        Ok(Some(frees))
    } else {
        return Ok(None);
    }
}

impl Db {
    /// . Subscribe to
    /// `commit_free_pbas` apply outcomes produced by metadb's internal
    /// lineage-GC commit path. The sink is invoked exactly once per
    /// successful internal `commit_free_pbas(vol_ord, ..)` call with
    /// the volume ordinal of the GC cycle and the non-empty
    /// `ApplyOutcome::FreePbas.freed_pbas` vector (PBAs whose rc
    /// transitioned to 0 plus exclusive PBAs that arrived at rc=0).
    ///
    /// Onyx registers a sink at startup so the engine-side allocator
    /// retire + dedup candidate-cache invalidation can reclaim PBAs
    /// surfaced by the metadb-internal GC cycle. The sink runs
    /// **synchronously** on the GC driver thread and must not call
    /// back into metadb's commit path (re-entrance would deadlock the
    /// apply gate). For long-running work, push onto a channel and
    /// drain elsewhere.
    ///
    /// Replacing an existing sink is allowed (the most recent caller
    /// wins). Pass an `Arc` so the same closure can be cloned and
    /// installed back after a teardown without re-allocating.
    pub fn set_freed_pbas_sink(&self, sink: crate::FreedPbasSink) {
        *self.freed_pbas_sink.lock() = Some(sink);
    }

    /// Drop any subscribed sink. Subsequent FreePbas commits surface
    /// no callback. Used by onyx teardown so the channel sender does
    /// not outlive the receiver during shutdown.
    pub fn clear_freed_pbas_sink(&self) {
        *self.freed_pbas_sink.lock() = None;
    }

    /// Stamp the buffer-backed journal watermark that the
    /// next checkpoint will copy into `manifest.last_processed_buffer_seq`.
    ///
    /// Onyx calls this from the flusher's `post_commit` path with the
    /// highest LV2 buffer entry seq whose mutations are now in metadb's
    /// in-memory state. Monotonic `fetch_max` so out-of-order callers
    /// (multiple commit_workers) cannot regress the watermark.
    ///
    /// Returns the previous value for caller observability; the new
    /// effective watermark is `max(prev, seq)`.
    pub fn set_buffer_applied_watermark(&self, seq: u64) -> u64 {
        self.buffer_applied_watermark
            .fetch_max(seq, std::sync::atomic::Ordering::Release)
    }

    /// Read the watermark that the next checkpoint will persist. Used
    /// by tests and by the checkpoint commit hook in `flush_with_gate`.
    pub fn buffer_applied_watermark(&self) -> u64 {
        self.buffer_applied_watermark
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Buffer replay frontier committed in the currently durable manifest.
    /// Unlike [`buffer_applied_watermark`](Self::buffer_applied_watermark),
    /// this never reports in-memory work that a checkpoint has not published.
    pub fn durable_buffer_applied_watermark(&self) -> u64 {
        self.manifest_state
            .lock()
            .manifest
            .last_processed_buffer_seq
    }

    /// Same contract as [`set_buffer_applied_watermark`] but for the
    /// lifecycle journal. Called after a lifecycle-log apply lands in
    /// metadb's in-memory state.
    pub fn set_lifecycle_applied_watermark(&self, seq: u64) -> u64 {
        self.lifecycle_applied_watermark
            .fetch_max(seq, std::sync::atomic::Ordering::Release)
    }

    /// Read the lifecycle-log replay watermark.
    pub fn lifecycle_applied_watermark(&self) -> u64 {
        self.lifecycle_applied_watermark
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Copy the live buffer + lifecycle replay watermarks into a manifest
    /// about to be committed. [`Db::run_sync_cycle_body`] does this for
    /// the checkpoint it commits (`flush.rs`); lifecycle ops that commit a
    /// manifest WITHOUT first driving a forced BFG sync (`take_snapshot` /
    /// `drop_snapshot` after buffer-backed journal) must do the same, or the manifest
    /// keeps a stale `lifecycle_replay_seq` and recovery re-replays
    /// already-folded lifecycle ops (e.g. a `PromotionChunk` incref) on
    /// top of the durable refcount array, double-counting them.
    ///
    /// Callers MUST hold `apply_gate.write()` so no concurrent apply
    /// advances either watermark past the state this manifest's flushed
    /// pages actually cover. The not-yet-submitted lifecycle op of the
    /// caller itself (drop_snapshot's `DropSnapshot`) is submitted AFTER
    /// this commit, so its seq is correctly excluded here and replayed
    /// on the next open.
    pub(super) fn stamp_replay_watermarks(&self, manifest: &mut crate::manifest::Manifest) {
        use std::sync::atomic::Ordering;
        manifest.last_processed_buffer_seq = manifest
            .last_processed_buffer_seq
            .max(self.buffer_applied_watermark.load(Ordering::Acquire));
        manifest.lifecycle_replay_seq = self.lifecycle_applied_watermark.load(Ordering::Acquire);
    }

    /// Internal helper: pluck `ApplyOutcome::FreePbas` entries out of a
    /// commit-ops result and forward their `freed_pbas` to the
    /// registered sink (if any). Non-`FreePbas` outcomes and
    /// `freed_pbas.is_empty()` cases are skipped. The sink call
    /// happens with the sink mutex released so a sink can safely
    /// re-enter unrelated metadb APIs (it still must not re-enter
    /// commit_ops; see [`Db::set_freed_pbas_sink`]).
    pub(crate) fn dispatch_freed_pbas_outcomes(
        &self,
        vol_ord: crate::types::VolumeOrdinal,
        outcomes: Vec<crate::tx::ApplyOutcome>,
    ) {
        let sink = match self.freed_pbas_sink.lock().clone() {
            Some(s) => s,
            None => return,
        };
        for outcome in outcomes {
            if let crate::tx::ApplyOutcome::FreePbas { freed_pbas } = outcome {
                if !freed_pbas.is_empty() {
                    sink(vol_ord, freed_pbas.into_vec());
                }
            }
        }
    }

    /// Start the background reclaim worker. Caller (`Db::create`
    /// / `Db::open`) checks `cfg.async_reclaim_enabled` before
    /// invoking; this method assumes the knob was true so it can
    /// stay on `&self` without re-reading config.
    ///
    /// passes a lineage context only so the worker can refuse the
    /// historical chain-truncation-only path. FreePbas-emitting Lineage GC
    /// must run through `Db::run_lineage_gc_cycle_inner`, where a `Db`
    /// handle can commit the retire record before advancing the chain.
    fn start_async_reclaim(&self, params: super::async_reclaim::AsyncReclaimParams) {
        // Page reclaim is deliberately single-purpose. Lineage GC has its own
        // Db-aware worker and must never be coupled to checkpoint completion.
        let worker = super::async_reclaim::AsyncReclaim::start(
            self.page_store.clone(),
            self.page_cache.clone(),
            self.metrics.clone(),
            params,
        );
        *self.async_reclaim.lock() = Some(worker);
    }

    /// BFG: start the background per-clone page-livelist
    /// condense worker. Caller (`Db::create` / `Db::open`) checks
    /// `cfg.livelist_condense_min_segments > 0` first. Independent of
    /// `async_reclaim_enabled` — condense only rewrites the SHADOW livelist
    /// and changes no page-rc free decision.
    fn start_livelist_condense(&self, params: super::livelist_condense::LivelistCondenseParams) {
        let worker = super::livelist_condense::LivelistCondenser::start(
            self.page_store.clone(),
            self.page_cache.clone(),
            self.manifest_state.clone(),
            self.apply_gate.clone(),
            self.volumes.clone(),
            self.faults.clone(),
            params,
        );
        *self.livelist_condense.lock() = Some(worker);
    }

    /// Start the background Lineage GC driver — the production trigger for
    /// FreePbas-emitting PBA reclaim. Caller (`Db::create` / `Db::open`)
    /// checks `cfg.lineage_gc_enabled` first. Takes `self: &Arc<Self>` so
    /// the worker can hold a `Weak<Db>` (mirrors `start_bfg_threads`) and
    /// never extend `Db`'s lifetime past `Drop`.
    fn start_lineage_gc_worker(self: &Arc<Self>, params: super::lineage_gc::LineageGcParams) {
        let worker = super::lineage_gc::LineageGcWorker::start(Arc::downgrade(self), params);
        *self.lineage_gc_worker.lock() = Some(worker);
    }

    /// Wake the background reclaim worker (if any). Called from
    /// `flush_with_gate` once a flush makes new
    /// `deferred_free` entries safe to reclaim. Idempotent — the
    /// worker condvar coalesces multiple notifications.
    pub(super) fn notify_async_reclaim(&self) {
        if let Some(worker) = &*self.async_reclaim.lock() {
            worker.notify();
        }
    }

    /// True iff the background reclaim worker is active. Used by
    /// `flush_with_gate` to decide between inline and async
    /// reclaim paths.
    fn async_reclaim_active(&self) -> bool {
        self.async_reclaim.lock().is_some()
    }

    /// Return every unused per-L2P-shard allocation batch to PageStore after a
    /// terminal flush. Refcount array pages are allocated one-at-a-time and do
    /// not keep a local reserve, so there is no corresponding RC drain.
    fn release_terminal_allocation_reserves(&self, generation: Lsn) -> Result<usize> {
        let mut volumes: Vec<Arc<Volume>> = self.volumes.read().values().cloned().collect();
        volumes.sort_by_key(|volume| volume.ord);

        let mut released = 0usize;
        for volume in volumes {
            for shard in &volume.shards {
                released = released
                    .saturating_add(shard.tree.write().release_unused_allocations(generation)?);
            }
        }
        Ok(released)
    }

    /// Terminally quiesce metadata background writers, drain every deferred
    /// page free, and make both the Free stamps and the device-path persisted
    /// free-list bitmap durable.
    ///
    /// This is a shutdown-only operation: callers must have stopped external
    /// writers and must not issue more Db mutations after it returns. A single
    /// reclaim pass is insufficient on the fixed-device path because the
    /// manifest's free-list bitmap is sampled before post-publish checkpoint
    /// pages are retired. Each round therefore forces a manifest checkpoint,
    /// synchronously drains the pages that checkpoint made reclaimable, and
    /// repeats. The first round with no reclaimed or deferred pages proves that
    /// its just-published bitmap covers every preceding reclaim.
    pub fn drain_deferred_reclaim_durable(&self) -> Result<usize> {
        // Stop independent page writers/producers first. Keep the BFG quiesce +
        // sync pair alive until the convergence loop is complete because
        // `flush()` uses them in the production threads-on configuration.
        if let Some(mut flusher) = self.l2p_writeback.lock().take() {
            flusher.stop();
        }
        if let Some(mut worker) = self.lineage_gc_worker.lock().take() {
            worker.stop();
        }
        if let Some(mut worker) = self.livelist_condense.lock().take() {
            worker.stop();
        }
        self.dedup_index.detach_drainers();
        self.wait_apply_idle();

        // Stop, but retain, the worker handle in the Option. While it remains
        // present `flush_with_gate` keeps using the async-notify branch instead
        // of reclaiming after its bitmap snapshot behind our accounting.
        if let Some(worker) = self.async_reclaim.lock().as_mut() {
            worker.stop();
        }

        let mut total_reclaimed = 0usize;
        let mut converged = false;
        for round in 1..=TERMINAL_RECLAIM_MAX_ROUNDS {
            let inline_before = self.metrics.snapshot().flush_reclaim_reclaimed_pages;
            self.flush()?;
            let inline_after = self.metrics.snapshot().flush_reclaim_reclaimed_pages;
            // Async reclaim can be disabled in standalone metadb configs. In
            // that mode flush reclaims inline after publishing its bitmap, so
            // count that work and require another round as well.
            let inline_reclaimed = inline_after.saturating_sub(inline_before) as usize;

            // PageBuf reserves ids in 256-page batches. The unused tail is not
            // tree-reachable and has never entered deferred_free, so reclaim
            // cannot discover it on its own. Release it only after this forced
            // flush has installed every L2P checkpoint; the next loop round
            // persists the resulting free bitmap before shutdown can go clean.
            let released_reservations =
                self.release_terminal_allocation_reserves(self.last_applied_lsn())?;

            let mut explicit_reclaimed = 0usize;
            loop {
                let pending = self.page_store.deferred_free_len();
                if pending == 0 {
                    break;
                }
                let started = std::time::Instant::now();
                let outcome = self
                    .page_store
                    .try_reclaim_limit(TERMINAL_RECLAIM_CHUNK_PAGES)?;
                for &pid in &outcome.reclaimed {
                    self.page_cache.invalidate(pid);
                }
                self.metrics.record_async_reclaim_cycle(
                    outcome.selected,
                    outcome.reclaimed.len(),
                    started.elapsed(),
                );
                if outcome.selected == 0 {
                    return Err(MetaDbError::Corruption(format!(
                        "terminal page reclaim made no progress with {pending} deferred pages (safe_below={})",
                        outcome.safe_below
                    )));
                }
                explicit_reclaimed = explicit_reclaimed.saturating_add(outcome.reclaimed.len());
            }

            total_reclaimed = total_reclaimed
                .saturating_add(inline_reclaimed)
                .saturating_add(explicit_reclaimed);
            let deferred = self.page_store.deferred_free_len();
            tracing::info!(
                round,
                inline_reclaimed,
                released_reservations,
                explicit_reclaimed,
                deferred,
                "metadb: terminal deferred-page reclaim round"
            );
            if inline_reclaimed == 0 && explicit_reclaimed == 0 && deferred == 0 {
                converged = true;
                break;
            }
        }
        if !converged {
            return Err(MetaDbError::Corruption(format!(
                "terminal page reclaim did not converge after {TERMINAL_RECLAIM_MAX_ROUNDS} rounds (deferred={})",
                self.page_store.deferred_free_len()
            )));
        }

        // No more flush is needed. Stop the BFG pair so a timeout-triggered
        // checkpoint cannot retire another page after the converged bitmap.
        self.stop_bfg_threads();
        if self.page_store.deferred_free_len() != 0 {
            return Err(MetaDbError::Corruption(format!(
                "terminal page reclaim gained {} deferred pages while stopping BFG workers",
                self.page_store.deferred_free_len()
            )));
        }
        // Free-stamp writes wait for CQEs, but only sync_all makes both their
        // content and file metadata (tail truncation / hole punching) durable.
        self.page_store.sync_all()?;
        Ok(total_reclaimed)
    }

    /// BFG: spawn the quiesce + sync workers
    /// when `cfg.bfg_threads_enabled = true`. The sync worker drives
    /// [`Db::run_sync_cycle`], the real per-BFG flush body extracted
    /// from `flush_with_gate`.
    ///
    /// The closure captures a [`Weak<Db>`] so the worker thread does
    /// not extend `Db`'s lifetime — `Drop` calls `stop_bfg_threads`
    /// before the strong refcount falls to zero (the strong refcount
    /// is what the caller holds via `Arc<Db>`), so a successful
    /// `weak.upgrade()` inside the closure always sees a live `Db`.
    /// A failed upgrade (would only happen if shutdown raced with a
    /// stale wake-up; the worker checks `shutdown` before calling)
    /// is a no-op success: the BFG just stays in Syncing and the
    /// shutdown path observes it.
    fn start_bfg_threads(self: &Arc<Self>, bfg_timeout_ms: u64) {
        let state = self.bfg.clone();
        let sync_notifier = self.bfg_sync_notifier.clone();
        let metrics = self.metrics.clone();
        let weak_db = Arc::downgrade(self);
        let sync_work: super::bfg_sync::SyncWorkFn = Arc::new(move |bfg| {
            let Some(db) = weak_db.upgrade() else {
                return Ok(());
            };
            // On a non-recoverable cycle failure, poison the sync subsystem
            // (sets sync_poison + aborts the BFG state machine + fails queued
            // snapshot tasks) so parked `wait_until_synced` callers get a
            // restart-required error instead of hanging, and the worker loop
            // stops re-driving the stuck Syncing slot. Mirrors the threads-off
            // `flush_with_gate` Err arm.
            let r = db.run_sync_cycle(bfg, crate::metrics::FlushKind::Forced);
            if let Err(err) = &r {
                db.poison_sync(err);
            }
            r
        });
        let sync = super::bfg_sync::BfgSyncThread::start(
            state.clone(),
            sync_notifier.clone(),
            sync_work,
            metrics.clone(),
        );
        *self.bfg_sync.lock() = Some(sync);

        let quiesce = super::bfg_quiesce::BfgQuiesceThread::start(
            state,
            self.bfg_quiesce_notifier.clone(),
            sync_notifier,
            super::bfg_quiesce::QuiesceParams { bfg_timeout_ms },
            metrics,
            self.faults.clone(),
        );
        *self.bfg_quiesce.lock() = Some(quiesce);

        if self.l2p_checkpoint_pipeline_enabled {
            let weak_db = Arc::downgrade(self);
            let work: super::l2p_prefold::PrefoldWorkFn = Arc::new(move |bfg| {
                let Some(db) = weak_db.upgrade() else {
                    return Ok(false);
                };
                db.run_l2p_prefold(bfg)
            });
            *self.l2p_prefold.lock() = Some(super::l2p_prefold::L2pPrefoldWorker::start(work));
        }
    }

    /// BFG: stop the BFG worker pair, quiesce
    /// first then sync, so no new BFG enters Syncing during teardown
    /// and the sync side drains its current cycle before exiting.
    /// Idempotent: a second call is a no-op.
    pub(super) fn stop_bfg_threads(&self) {
        if let Some(mut p) = self.l2p_prefold.lock().take() {
            p.stop();
        }
        if let Some(mut q) = self.bfg_quiesce.lock().take() {
            q.stop();
        }
        if let Some(mut s) = self.bfg_sync.lock().take() {
            s.stop();
        }
    }

    fn run_l2p_prefold(&self, bfg: crate::types::Bfg) -> Result<bool> {
        let started = std::time::Instant::now();
        let result = self.run_l2p_prefold_inner(bfg);
        self.metrics.record_l2p_prefold(&result, started.elapsed());
        result
    }

    fn run_l2p_prefold_inner(&self, bfg: crate::types::Bfg) -> Result<bool> {
        if !self.l2p_checkpoint_pipeline_enabled {
            return Ok(false);
        }

        // Lifecycle writers need exclusive drop_gate access. Holding the read
        // side across the speculative fold prevents a snapshot/clone boundary
        // from appearing after the eligibility checks below.
        let _drop_guard = self.drop_gate.read();
        let state = self.bfg.snapshot();
        if state.quiescing_bfg != Some(bfg)
            || state.syncing_bfg != bfg.checked_sub(1)
            || self.bfg.is_aborted()
        {
            return Ok(false);
        }
        if !self.pending_sync_tasks.lock().is_empty()
            || self
                .snap_info_cache
                .lock()
                .values()
                .any(|snapshots| !snapshots.is_empty())
        {
            return Ok(false);
        }
        {
            let volumes = self.volumes.read();
            if volumes
                .values()
                .any(|volume| volume.parent_vol_ord.read().is_some())
            {
                return Ok(false);
            }
        }

        // The Quiescing slot has drained all BfgGuards and accepts no new
        // inserts, so it has the same immutable-slot contract as Syncing. This
        // folds and publish-before-clears only L2P data; the BFG state remains
        // Quiescing until the current checkpoint completes.
        self.drain_syncing_slot_into_trees(bfg)?;
        Ok(true)
    }

    pub(super) fn request_l2p_prefold(
        &self,
        current_bfg: crate::types::Bfg,
    ) -> Option<super::l2p_prefold::PrefoldTicket> {
        if !self.l2p_checkpoint_pipeline_enabled {
            return None;
        }
        let successor = current_bfg.checked_add(1)?;
        let state = self.bfg.snapshot();
        if state.syncing_bfg != Some(current_bfg) || state.quiescing_bfg != Some(successor) {
            return None;
        }
        self.l2p_prefold.lock().as_ref()?.request(successor)
    }

    pub(super) fn wait_l2p_prefold(
        &self,
        ticket: super::l2p_prefold::PrefoldTicket,
    ) -> Result<bool> {
        let worker_guard = self.l2p_prefold.lock();
        let Some(worker) = worker_guard.as_ref() else {
            return Ok(false);
        };
        worker.wait(ticket)
    }

    /// Force-fold every L2P shard's BFG ring buffer into its on-disk
    /// tree synchronously. Used by:
    ///
    /// - [`Db::flush_with_gate`]'s inline path (when
    ///   `bfg_threads_enabled = false`). The sample-phase no longer
    ///   holds `apply_gate.write`; serialisation against concurrent
    ///   commits' `cow_for_write` falls on the per-shard `tree.write()`
    ///   this helper takes.
    /// - The snapshot / `range_delete` / `drop_volume` / `drop_snapshot`
    ///   paths. `take_snapshot` / `drop_snapshot` / `range_delete`
    ///   (buffer-backed journal) no longer force-sync at entry, so this drain is
    ///   LOAD-BEARING there: it folds every buffered L2P op into the tree
    ///   so the root sample / refresh / range scan observes all applied
    ///   ops. `drop_gate.write` held by the lifecycle op keeps the slots
    ///   from refilling. (`drop_volume` still force-syncs at entry, so for
    ///   it the drain stays a defensive no-op.)
    /// - The post-replay path in `open_with_config_and_faults`,
    ///   before any commit can race the recovered buffer state.
    ///
    /// When `bfg_threads_enabled = true`, the `BfgSyncThread` is the
    /// regular drainer (one slot per BFG cycle). Lifecycle ops still
    /// call this helper as a belt-and-braces defensive drain; the
    /// per-shard `tree.write()` serialises it against the sync thread
    /// so the two drain paths cannot conflict.
    ///
    /// Returns the first error encountered; any successfully drained
    /// shards have already advanced their `compacted_lsn`.
    pub(super) fn force_compact_l2p_buffers(&self) -> Result<()> {
        use std::time::Instant;
        if !self.l2p_buffer_enabled {
            return Ok(());
        }
        let vols: Vec<Arc<Volume>> = {
            let map = self.volumes.read();
            let mut out: Vec<Arc<Volume>> = map.values().cloned().collect();
            out.sort_by_key(|v| v.ord);
            out
        };
        for vol in vols {
            self.compact_volume_buffers(&vol)?;
        }
        Ok(())
    }

    /// Volume-scoped variant of [`force_compact_l2p_buffers`]: fold only
    /// `vol_ord`'s L2P shard buffers into its tree. Used by `range_delete`,
    /// which only reads + mutates a single volume's range and so must not
    /// pay (and hold `drop_gate.write` / `apply_gate.write` across) the
    /// fold of EVERY volume's buffer. Folding all volumes there was an
    /// N-volume inflation of the lifecycle op's pipeline-blocking window
    /// (`drop_gate.write` parks every `commit_ops` writer for the hold).
    /// Correctness is identical to the full drain — volumes are
    /// independent, and range_delete touches only `vol_ord`. No-op when
    /// the buffer is disabled or the volume is gone.
    pub(super) fn force_compact_l2p_buffers_for_volume(
        &self,
        vol_ord: VolumeOrdinal,
    ) -> Result<()> {
        if !self.l2p_buffer_enabled {
            return Ok(());
        }
        let vol = {
            let map = self.volumes.read();
            map.get(&vol_ord).cloned()
        };
        if let Some(vol) = vol {
            self.compact_volume_buffers(&vol)?;
        }
        Ok(())
    }

    /// Fold every BFG slot of one volume's L2P shard buffers into its
    /// tree. Shared body of [`force_compact_l2p_buffers`] (all volumes)
    /// and [`force_compact_l2p_buffers_for_volume`] (one volume). Callers
    /// hold `drop_gate.write` (lifecycle ops) or run pre-commit
    /// (recovery / threads-off inline flush), so the slots are quiesced.
    fn compact_volume_buffers(&self, vol: &Arc<Volume>) -> Result<()> {
        use std::time::Instant;
        let snapshot_wms = self.snapshot_wms(vol.ord);
        // BFG: clone COW-kill pinner set (empty for non-clones).
        let clone_cow_pinners = self.clone_cow_pinners(vol.ord);
        for (shard_idx, shard) in vol.shards.iter().enumerate() {
            if !shard.use_buffer {
                continue;
            }
            let started = Instant::now();
            let mut tree = shard.tree.write();
            // Drain ALL four BFG slots in one shot. Used only by
            // paths that need every slot folded NOW: lifecycle ops
            // (which hold `drop_gate.write`, so the slots are
            // quiesced), the post-replay open, and the threads-OFF
            // inline flush. The regular threads-ON per-BFG sync uses
            // `drain_syncing_slot_into_trees` instead (drains only
            // the frozen syncing slot, publish-before-clear). Note:
            // this drain is NOT publish-before-clear, so on the
            // threads-OFF inline-flush path a concurrent commit can
            // still hit the (rare) stale-`prev` race — acceptable
            // there because that path is being retired.
            let drained = shard.l2p_buffer.drain_all_slots();
            if drained.is_empty() {
                drop(tree);
                continue;
            }
            let count = drained.len();
            let max_lsn = drained.values().map(|e| e.lsn).max().unwrap_or(0);
            // This all-slots drain folds page-rc deltas into the current Open
            // BFG slot; lifecycle/recovery callers fold every page-rc slot
            // afterwards (RcShard all-slots flush /
            // `begin_checkpoint_all_slots`), so any live slot is correct here.
            let apply_result = super::bfg_sync::compact_drain_into_tree(
                &mut tree,
                &drained,
                self.bfg.open_bfg(),
                snapshot_wms.clone(),
                clone_cow_pinners.clone(),
            );
            match apply_result {
                Ok(()) => {
                    // BFG: harvest page-deaths from this
                    // all-slots fold before releasing the tree lock.
                    // H1: route to this shard's own accumulator.
                    super::apply::drain_page_deaths_into(&vol.page_dead_list[shard_idx], &mut tree);
                    // BFG: same fold site for the per-clone
                    // page-livelist witness (empty for non-clones).
                    super::apply::drain_live_events_into(&vol.page_live_list, &mut tree);
                    super::apply::publish_l2p_read_view(shard, &tree);
                    drop(tree);
                    shard.l2p_buffer.note_compacted(max_lsn);
                    self.metrics.record_l2p_buffer_compaction(
                        count,
                        0,
                        1,
                        started.elapsed(),
                        std::time::Duration::ZERO,
                        std::time::Duration::ZERO,
                        started.elapsed(),
                        std::time::Duration::ZERO,
                        std::time::Duration::ZERO,
                    );
                }
                Err(err) => {
                    drop(tree);
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    /// Threads-ON BFG sync drain: fold only the frozen syncing slot (`bfg & 3`)
    /// of every L2P shard into its tree. Open and Quiescing slots are left
    /// untouched and drain on their own future sync cycles, so each cycle's work
    /// is bounded by one group's writes (roughly `bfg_timeout`) instead of the
    /// whole accumulated backlog.
    ///
    /// **Publish-before-clear** is the load-bearing correctness rule.
    /// `lookup_for_open_bfg` (used by both a commit's prev-value read in
    /// `apply_l2p_bucket_buffer` and a user read) walks `open .. open-2`,
    /// which includes the syncing slot, and falls through to `read_view`
    /// only on a miss. If we cleared the slot before folding+publishing,
    /// a concurrent lookup could miss the (now empty) slot and read the
    /// stale `read_view`, falsely reporting `prev = None` → a refcount /
    /// space leak on the onyx side. So per shard:
    ///   1. move-freeze the syncing slot into an immutable `Arc<HashMap>`;
    ///      readers still hit that same generation during the fold;
    ///   2. fold the frozen generation into the tree;
    ///   3. `publish_l2p_read_view` — the tree/read_view now carry the
    ///      entries;
    ///   4. `finish_syncing_slot` (clear). A lookup that acquires the slot
    ///      lock after this sees it empty, and the slot-lock release/
    ///      acquire edge guarantees it then observes the published view.
    /// The slot being Syncing (no concurrent inserts) is what makes the
    /// step-1 frozen generation equal the step-4 clear.
    ///
    /// The move-freeze in step 1 is O(1): neither the slot lock nor a full-map
    /// clone is held across fold planning or tree mutation. Failed drains keep
    /// the same frozen generation for an idempotent retry.
    pub(super) fn drain_syncing_slot_into_trees(&self, bfg: crate::types::Bfg) -> Result<()> {
        if !self.l2p_buffer_enabled {
            return Ok(());
        }
        let vols: Vec<Arc<Volume>> = {
            let map = self.volumes.read();
            let mut out: Vec<Arc<Volume>> = map.values().cloned().collect();
            out.sort_by_key(|v| v.ord);
            out
        };
        // Each L2P shard is independent: its own `l2p_buffer`, its own `tree`
        // lock, and its COW page allocations draw from a per-shard `PageBuf`
        // `alloc_pool` that batch-refills from `page_store` (so the global
        // allocate mutex is touched only at refill granularity, not per page).
        // The only shared resources — `page_store` and `self.metrics` — are
        // internally synchronized. The previously-serial fold was the bfg-sync
        // drain bottleneck (on-CPU: ~74% of the `metadb-bfg-sync` thread in
        // `compact_drain_into_tree`); serializing 16 shards' COW folds on one
        // thread bounded single-volume write throughput, so by default we fan
        // out across shards (mirrors the refcount `begin_checkpoint` fan-out in
        // `run_sync_cycle_body`). The `parallel_l2p_drain_enabled=false` path
        // preserves the serial fold for A/B and as a fallback.
        let metrics = &self.metrics;
        let chunk_entries = self.l2p_drain_chunk_entries;
        if self.parallel_l2p_drain_enabled {
            let results: Vec<Result<()>> = std::thread::scope(|scope| {
                let mut handles = Vec::new();
                for vol in &vols {
                    // Youngest snapshot pinning this volume's pages; computed
                    // once per volume and captured by per-shard fold tasks for
                    // the page-deadlist birth gate.
                    let snapshot_wms = self.snapshot_wms(vol.ord);
                    // Clone COW-kill pinner set, computed once per volume
                    // (empty for non-clones).
                    let clone_cow_pinners = self.clone_cow_pinners(vol.ord);
                    let page_dead_list = &vol.page_dead_list;
                    let page_live_list = &vol.page_live_list;
                    for (shard_idx, shard) in vol.shards.iter().enumerate() {
                        if !shard.use_buffer {
                            continue;
                        }
                        let snapshot_wms = snapshot_wms.clone();
                        let clone_cow_pinners = clone_cow_pinners.clone();
                        handles.push(scope.spawn(move || {
                            // Keep checkpoint work inside its background CPU
                            // domain. Confine mode inherits the parent's full
                            // background mask; explicit layouts bind to the BFG
                            // home pod or compactor CPU set.
                            crate::affinity::bind_for_l2p_drain(shard_idx);
                            Self::drain_one_syncing_shard(
                                shard,
                                bfg,
                                metrics,
                                chunk_entries,
                                // H1: this shard's own page-death accumulator.
                                &page_dead_list[shard_idx],
                                page_live_list,
                                snapshot_wms,
                                clone_cow_pinners,
                            )
                        }));
                    }
                }
                handles
                    .into_iter()
                    .map(|h| h.join().unwrap_or_else(|p| std::panic::resume_unwind(p)))
                    .collect()
            });
            // First error wins. Every shard that could drain has drained and
            // cleared its frozen slot, so a retry of this BFG only re-processes
            // the shard(s) that errored (their slots are still populated).
            for r in results {
                r?;
            }
        } else {
            for vol in &vols {
                let snapshot_wms = self.snapshot_wms(vol.ord);
                let clone_cow_pinners = self.clone_cow_pinners(vol.ord);
                for (shard_idx, shard) in vol.shards.iter().enumerate() {
                    if !shard.use_buffer {
                        continue;
                    }
                    Self::drain_one_syncing_shard(
                        shard,
                        bfg,
                        metrics,
                        chunk_entries,
                        // H1: this shard's own page-death accumulator.
                        &vol.page_dead_list[shard_idx],
                        &vol.page_live_list,
                        snapshot_wms.clone(),
                        clone_cow_pinners.clone(),
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Drain one L2P shard's frozen syncing slot into its tree. Shared by the
    /// parallel (scope-spawned, one task per shard) and serial drain paths in
    /// `drain_syncing_slot_into_trees`. Each shard is independent (its own
    /// `l2p_buffer`, its own `tree` lock, per-shard `PageBuf` alloc pool), so
    /// this is safe to run concurrently across shards.
    ///
    /// The fold holds `tree.write()` in BOUNDED chunks
    /// (`chunk_entries` buffered entries per acquisition, 0 = one shot)
    /// instead of one hold for the whole slot. The unbounded hold was a
    /// proven multi-second commit stall: `apply_l2p_remap` takes the
    /// same `tree.write()` per op and dedup/read multi_gets take
    /// `tree.read()`, so every commit worker and reader on the shard
    /// parked for the fold's full duration. Releasing between chunks is
    /// safe because the slot stays populated until `finish_syncing_slot`
    /// below (publish-before-clear, see `drain_syncing_slot_into_trees`
    /// doc): a concurrent `lookup_for_open_bfg` hits the live slot
    /// entries and never observes the partially-folded tree, and every
    /// chunk ends with `finish_batch_apply` so interleaving lock takers
    /// see a consistent tree + read overlay. Re-folding after a
    /// mid-chunk error stays idempotent via `page.generation >= lsn`,
    /// same as the one-shot retry contract.
    #[allow(clippy::too_many_arguments)]
    fn drain_one_syncing_shard(
        shard: &super::L2pShard,
        bfg: crate::types::Bfg,
        metrics: &crate::metrics::MetaMetrics,
        chunk_entries: usize,
        page_dead_list: &crate::deadlist::DeadListState,
        page_live_list: &crate::livelist::LiveListState,
        snapshot_wms: Vec<Lsn>,
        clone_cow_pinners: Vec<Lsn>,
    ) -> Result<()> {
        let started = std::time::Instant::now();
        // O(1) move-freeze: the slot retains an Arc for concurrent lookups while
        // fold planning and tree mutation use the same immutable generation.
        let entries = shard.l2p_buffer.borrow_syncing_slot(bfg);
        if entries.is_empty() {
            shard.l2p_buffer.finish_syncing_slot(bfg, &entries);
            return Ok(());
        }
        let count = entries.len();
        let max_lsn = entries.values().map(|e| e.lsn).max().unwrap_or(0);
        // Build the fold plan (leaf grouping + sorting) off-lock: it is
        // pure CPU over the frozen snapshot, and its transient
        // allocations live and die outside the lock hold.
        let plan_started = std::time::Instant::now();
        let plan = super::bfg_sync::build_drain_plan(&entries);
        let plan_elapsed = plan_started.elapsed();
        let leaves = plan.len();
        let mut chunks = 0usize;
        let mut tree_wait = std::time::Duration::ZERO;
        let mut apply_elapsed = std::time::Duration::ZERO;
        let mut publish_elapsed = std::time::Duration::ZERO;
        let mut start = 0;
        while start < plan.len() {
            // Chunk boundary: whole leaves only, >= chunk_entries entries.
            let mut end = start;
            let mut budget = 0usize;
            while end < plan.len() {
                budget += plan[end].entry_count();
                end += 1;
                if chunk_entries != 0 && budget >= chunk_entries {
                    break;
                }
            }
            let tree_wait_started = std::time::Instant::now();
            let mut tree = shard.tree.write();
            tree_wait += tree_wait_started.elapsed();
            let apply_started = std::time::Instant::now();
            super::bfg_sync::apply_drain_ops(
                &mut tree,
                &plan[start..end],
                bfg,
                snapshot_wms.clone(),
                clone_cow_pinners.clone(),
            )?;
            // BFG: this fold COW'd L2P pages off the head;
            // record the snapshot-pinned ones into the HEAD page-deadlist
            // (buffer mode's only COW point — the apply-time witness was
            // empty for buffered writes).
            super::apply::drain_page_deaths_into(page_dead_list, &mut tree);
            // BFG: per-clone page-livelist witness (empty for
            // non-clones).
            super::apply::drain_live_events_into(page_live_list, &mut tree);
            apply_elapsed += apply_started.elapsed();
            if end == plan.len() {
                // Publish BEFORE clearing the slot (see method doc), under
                // the final chunk's hold like the one-shot fold did.
                let publish_started = std::time::Instant::now();
                super::apply::publish_l2p_read_view(shard, &tree);
                publish_elapsed += publish_started.elapsed();
            }
            drop(tree);
            start = end;
            chunks += 1;
        }
        // Clear only the exact generation just folded and published.
        let finish_started = std::time::Instant::now();
        shard.l2p_buffer.finish_syncing_slot(bfg, &entries);
        shard.l2p_buffer.note_compacted(max_lsn);
        let finish_elapsed = finish_started.elapsed();
        metrics.record_l2p_buffer_compaction(
            count,
            leaves,
            chunks,
            started.elapsed(),
            plan_elapsed,
            tree_wait,
            apply_elapsed,
            publish_elapsed,
            finish_elapsed,
        );
        Ok(())
    }

    pub(super) fn l2p_buffer_enabled(&self) -> bool {
        self.l2p_buffer_enabled
    }

    /// Current cached manifest (as of the last durable manifest commit).
    pub fn manifest(&self) -> Manifest {
        self.manifest_state.lock().manifest.clone()
    }

    /// Enumerate all registered snapshots.
    pub fn snapshots(&self) -> Vec<SnapshotEntry> {
        self.manifest_state.lock().manifest.snapshots.clone()
    }

    /// Enumerate snapshots pinned to volume `vol_ord`. Returns an empty
    /// vec for unknown ordinals (the concept of "snapshots on a volume
    /// that doesn't exist" is well-defined: there are none).
    pub fn snapshots_for(&self, vol_ord: VolumeOrdinal) -> Vec<SnapshotEntry> {
        self.manifest_state
            .lock()
            .manifest
            .snapshots
            .iter()
            .filter(|s| s.vol_ord == vol_ord)
            .cloned()
            .collect()
    }

    /// Number of shards in this database. This reports
    /// the bootstrap volume's shard count; every volume in the map is
    /// created with the same shard count, so this remains the right answer
    /// once multi-volume support lands.
    pub fn shard_count(&self) -> usize {
        self.volume_zero().shards.len()
    }

    /// Number of pages currently allocated in the page store.
    pub fn high_water(&self) -> u64 {
        self.page_store.high_water()
    }

    /// Widen the page-window ceiling online after the host extended the meta LD
    /// (`extend_ld`). `new_window_bytes` is the new page-window size in bytes
    /// (onyx recomputes it from the grown LD capacity + OMET layout). Lifts the
    /// `CapacityExhausted` ceiling so commits that were stalling on a full
    /// device resume. On the file backend / a device that cannot grow in place
    /// this errors (`PageDevice::grow_capacity_pages`).
    pub fn grow_device_capacity(&self, new_window_bytes: u64) -> Result<()> {
        let new_pages = new_window_bytes / crate::config::PAGE_SIZE as u64;
        self.page_store.grow_device_capacity(new_pages)
    }

    /// Number of reclaimed pages currently available for reuse.
    pub fn free_list_len(&self) -> usize {
        self.page_store.free_list_len()
    }

    /// Snapshot shared page-cache counters.
    pub fn cache_stats(&self) -> PageCacheStats {
        self.page_cache.stats()
    }

    pub fn metrics_snapshot(&self) -> MetaMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn dedup_lsm_stats(&self) -> crate::LsmStats {
        // Dedup index moved off LSM; report zeroed stats so the onyx
        // status formatter still has a value to format. Use
        // [`Db::dedup_tier_sizes`] for the cuckoo L0/L1 occupancy.
        crate::LsmStats::default()
    }

    /// Per-shard dedup stats are unavailable: the cuckoo dedup_index
    /// has no shard concept. Returns an empty vec.
    pub fn dedup_lsm_stats_per_shard(&self) -> Vec<crate::LsmStats> {
        Vec::new()
    }

    /// Cuckoo dedup_index L0/L1 tier occupancy snapshot.
    pub fn dedup_tier_sizes(&self) -> crate::dedup::TierSizes {
        self.dedup_index.tier_sizes()
    }

    pub fn dedup_tier_sizes_best_effort(&self) -> crate::dedup::TierSizes {
        self.dedup_index.tier_sizes_best_effort()
    }

    /// Diagnostic snapshot of in-memory bookkeeping that can grow
    /// unbounded if its drain path stalls (deferred reclaim, dispatch
    /// FIFO, per-shard apply lane queues, per-shard COW retired/private
    /// page sets, page-buf totals). Cheap: each field is a single
    /// `len()` call. Intended for OOM triage during soak — these are
    /// the structures most likely to leak when a worker thread falls
    /// behind. Aggregates across all volumes' L2P shards plus refcount
    /// shards plus the dedup lane.
    ///
    /// **Non-blocking**: this is called by the onyx status socket
    /// handler. Per-shard tree locks can be held by `flush.install`
    /// for seconds at a time; using a blocking `tree.read()` /
    /// `tree.lock()` here would freeze the status socket for the same
    /// duration. Instead, try-acquire each lock and skip the shard's
    /// contribution if it's contended. The result is best-effort and
    /// undercounts during install; that's acceptable for diagnostics.
    pub fn pending_state(&self) -> PendingState {
        let dispatch_pending = self.dispatch_state.lock().pending.len();
        let deferred_free = self.page_store.deferred_free_len();
        let dedup_lane_queue: usize = self.dedup_lanes.iter().map(|lane| lane.queue_len()).sum();
        let mut l2p_apply_queue = 0usize;
        let mut l2p_private_pages = 0usize;
        let mut l2p_retired_pages = 0usize;
        let mut l2p_pagebuf_total = 0usize;
        let mut l2p_pagebuf_dirty = 0usize;
        for volume in self.volumes.read().values() {
            for shard in &volume.shards {
                l2p_apply_queue += shard.apply_lane.queue_len();
                if let Some(tree) = shard.tree.try_read() {
                    let (priv_p, ret_p, total, dirty) = tree.growth_summary();
                    l2p_private_pages += priv_p;
                    l2p_retired_pages += ret_p;
                    l2p_pagebuf_total += total;
                    l2p_pagebuf_dirty += dirty;
                }
            }
        }
        let mut rc_apply_queue = 0usize;
        let mut rc_private_pages = 0usize;
        let rc_retired_pages = 0usize;
        let rc_pagebuf_total = 0usize;
        let rc_pagebuf_dirty = 0usize;
        let mut rc_pending_deltas = 0usize;
        for shard in &self.refcount_shards {
            rc_apply_queue += shard.apply_lane.queue_len();
            // Paged-array refcount has no COW / private / retired
            // page concept (in-place mutation, no snapshots). Report
            // the data-page count as `private_pages` so the operator
            // still sees a "how big is this shard" gauge; the other
            // BTree-specific dials stay zero.
            rc_private_pages += shard.rc.allocated_data_pages();
            rc_pending_deltas += shard.rc.pending_delta_count();
        }
        PendingState {
            dispatch_pending,
            deferred_free,
            dedup_lane_queue,
            l2p_apply_queue,
            l2p_private_pages,
            l2p_retired_pages,
            l2p_pagebuf_total,
            l2p_pagebuf_dirty,
            rc_apply_queue,
            rc_private_pages,
            rc_retired_pages,
            rc_pagebuf_total,
            rc_pagebuf_dirty,
            rc_pending_deltas,
        }
    }

    /// Best-effort variant for external status/metrics surfaces. It must
    /// never wait behind hot-path locks: contended fields are skipped and
    /// therefore may undercount during heavy apply/checkpoint activity.
    pub fn pending_state_best_effort(&self) -> PendingState {
        let dispatch_pending = self
            .dispatch_state
            .try_lock()
            .map(|state| state.pending.len())
            .unwrap_or(0);
        let deferred_free = self.page_store.deferred_free_len();
        let dedup_lane_queue: usize = self
            .dedup_lanes
            .iter()
            .filter_map(|lane| lane.try_queue_len())
            .sum();
        let mut l2p_apply_queue = 0usize;
        let mut l2p_private_pages = 0usize;
        let mut l2p_retired_pages = 0usize;
        let mut l2p_pagebuf_total = 0usize;
        let mut l2p_pagebuf_dirty = 0usize;
        if let Some(volumes) = self.volumes.try_read() {
            for volume in volumes.values() {
                for shard in &volume.shards {
                    l2p_apply_queue += shard.apply_lane.try_queue_len().unwrap_or(0);
                    if let Some(tree) = shard.tree.try_read() {
                        let (priv_p, ret_p, total, dirty) = tree.growth_summary();
                        l2p_private_pages += priv_p;
                        l2p_retired_pages += ret_p;
                        l2p_pagebuf_total += total;
                        l2p_pagebuf_dirty += dirty;
                    }
                }
            }
        }
        let mut rc_apply_queue = 0usize;
        let mut rc_private_pages = 0usize;
        let rc_retired_pages = 0usize;
        let rc_pagebuf_total = 0usize;
        let rc_pagebuf_dirty = 0usize;
        let mut rc_pending_deltas = 0usize;
        for shard in &self.refcount_shards {
            rc_apply_queue += shard.apply_lane.try_queue_len().unwrap_or(0);
            rc_private_pages += shard.rc.allocated_data_pages();
            rc_pending_deltas += shard.rc.pending_delta_count();
        }
        PendingState {
            dispatch_pending,
            deferred_free,
            dedup_lane_queue,
            l2p_apply_queue,
            l2p_private_pages,
            l2p_retired_pages,
            l2p_pagebuf_total,
            l2p_pagebuf_dirty,
            rc_apply_queue,
            rc_private_pages,
            rc_retired_pages,
            rc_pagebuf_total,
            rc_pagebuf_dirty,
            rc_pending_deltas,
        }
    }

    /// Estimated total dirty work the next flush sample would have
    /// to drain: L2P dirty page buffer + in-memory RC deltas. Used
    /// by the watermark thread to decide whether to trigger an early
    /// checkpoint ahead of the periodic 1s tick, capping single-flush
    /// sample/IO cost.
    pub fn dirty_pages_estimate(&self) -> usize {
        let pending = self.pending_state();
        pending
            .l2p_pagebuf_dirty
            .saturating_add(pending.rc_pending_deltas)
    }

    pub fn metrics_json(&self) -> String {
        let cache = self.cache_stats();
        let metrics = self.metrics_snapshot();
        let pending = self.pending_state();
        let dedup_index = self.dedup_lsm_stats();
        format!(
            concat!(
                "{{",
                "\"last_applied_lsn\":{},",
                "\"high_water\":{},",
                "\"free_list\":{},",
                "\"dedup_index\":{{",
                "\"levels\":{},",
                "\"ssts\":{},",
                "\"records\":{},",
                "\"active_entries\":{},",
                "\"frozen_entries\":{}",
                "}},",
                "\"cache\":{{",
                "\"hits\":{},",
                "\"misses\":{},",
                "\"evictions\":{},",
                "\"current_pages\":{},",
                "\"current_bytes\":{},",
                "\"capacity_bytes\":{},",
                "\"pinned_pages\":{},",
                "\"pinned_bytes\":{},",
                "\"pin_budget_bytes\":{}",
                "}},",
                "\"pending\":{{",
                "\"dispatch\":{},",
                "\"deferred_free\":{},",
                "\"dedup_lane_queue\":{},",
                "\"l2p_apply_queue\":{},",
                "\"l2p_private_pages\":{},",
                "\"l2p_retired_pages\":{},",
                "\"l2p_pagebuf_total\":{},",
                "\"l2p_pagebuf_dirty\":{},",
                "\"rc_apply_queue\":{},",
                "\"rc_private_pages\":{},",
                "\"rc_retired_pages\":{},",
                "\"rc_pagebuf_total\":{},",
                "\"rc_pagebuf_dirty\":{}",
                "}},",
                "\"meta\":{}",
                "}}"
            ),
            self.last_applied_lsn(),
            self.high_water(),
            self.free_list_len(),
            dedup_index.level_count,
            dedup_index.total_ssts,
            dedup_index.total_records,
            dedup_index.memtable.active_entries,
            dedup_index.memtable.frozen_entries,
            cache.hits,
            cache.misses,
            cache.evictions,
            cache.current_pages,
            cache.current_bytes,
            cache.capacity_bytes,
            cache.pinned_pages,
            cache.pinned_bytes,
            cache.pin_budget_bytes,
            pending.dispatch_pending,
            pending.deferred_free,
            pending.dedup_lane_queue,
            pending.l2p_apply_queue,
            pending.l2p_private_pages,
            pending.l2p_retired_pages,
            pending.l2p_pagebuf_total,
            pending.l2p_pagebuf_dirty,
            pending.rc_apply_queue,
            pending.rc_private_pages,
            pending.rc_retired_pages,
            pending.rc_pagebuf_total,
            pending.rc_pagebuf_dirty,
            metrics.to_json(),
        )
    }
}

#[cfg(test)]
mod tests;
