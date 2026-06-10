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
    records: Vec<crate::deadlist::DeadRecord>,
    old_head: PageId,
    old_tail: PageId,
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
    /// [[no-refcount-hot-path-design]] Phase 4 Step 7. Subscribe to
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

    /// Buffer-as-sole-journal Phase B / C: stamp the watermark the
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
    /// Phase 5 passes a lineage context only so the worker can refuse the
    /// historical chain-truncation-only path. FreePbas-emitting Lineage GC
    /// must run through `Db::run_lineage_gc_cycle_inner`, where a `Db`
    /// handle can commit the retire record before advancing the chain.
    fn start_async_reclaim(&self, params: super::async_reclaim::AsyncReclaimParams) {
        let lineage_gc = super::async_reclaim::LineageGcCtx {
            volumes: self.volumes.clone(),
            manifest_state: self.manifest_state.clone(),
            apply_gate: self.apply_gate.clone(),
            refcount_shards_rc: self
                .refcount_shards
                .iter()
                .map(|shard| shard.rc.clone())
                .collect(),
            faults: self.faults.clone(),
            metrics: self.metrics.clone(),
            emit_freepbas: self.lineage_gc_emit_freepbas,
            drop_dedup_shared: self.lineage_gc_drop_dedup_shared,
        };
        let worker = super::async_reclaim::AsyncReclaim::start_with_lineage_gc(
            self.page_store.clone(),
            self.page_cache.clone(),
            self.metrics.clone(),
            params,
            Some(lineage_gc),
        );
        *self.async_reclaim.lock() = Some(worker);
    }

    /// Start the background Lineage GC driver — the production trigger for
    /// FreePbas-emitting PBA reclaim. Caller (`Db::create` / `Db::open`)
    /// checks `cfg.lineage_gc_enabled` first. Takes `self: &Arc<Self>` so
    /// the worker can hold a `Weak<Db>` (mirrors `start_txg_threads`) and
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

    /// ZFS-TXG-clone Phase 4 Step 8: spawn the quiesce + sync workers
    /// when `cfg.txg_threads_enabled = true`. Step 7 shipped an inert
    /// `sync_work` callback; Step 8 retargets it at
    /// [`Db::run_sync_cycle`], the real per-TXG flush body extracted
    /// from `flush_with_gate`.
    ///
    /// The closure captures a [`Weak<Db>`] so the worker thread does
    /// not extend `Db`'s lifetime — `Drop` calls `stop_txg_threads`
    /// before the strong refcount falls to zero (the strong refcount
    /// is what the caller holds via `Arc<Db>`), so a successful
    /// `weak.upgrade()` inside the closure always sees a live `Db`.
    /// A failed upgrade (would only happen if shutdown raced with a
    /// stale wake-up; the worker checks `shutdown` before calling)
    /// is a no-op success: the TXG just stays in Syncing and the
    /// shutdown path observes it.
    fn start_txg_threads(self: &Arc<Self>, txg_timeout_ms: u64) {
        let state = self.txg.clone();
        let sync_notifier = self.txg_sync_notifier.clone();
        let metrics = self.metrics.clone();
        let weak_db = Arc::downgrade(self);
        let sync_work: super::txg_sync::SyncWorkFn = Arc::new(move |txg| {
            let Some(db) = weak_db.upgrade() else {
                return Ok(());
            };
            db.run_sync_cycle(txg, crate::metrics::FlushKind::Forced)
        });
        let sync = super::txg_sync::TxgSyncThread::start(
            state.clone(),
            sync_notifier.clone(),
            sync_work,
            metrics.clone(),
        );
        *self.txg_sync.lock() = Some(sync);

        let quiesce = super::txg_quiesce::TxgQuiesceThread::start(
            state,
            self.txg_quiesce_notifier.clone(),
            sync_notifier,
            super::txg_quiesce::QuiesceParams { txg_timeout_ms },
            metrics,
            self.faults.clone(),
        );
        *self.txg_quiesce.lock() = Some(quiesce);
    }

    /// ZFS-TXG-clone Phase 4 Step 7: stop the TXG worker pair, quiesce
    /// first then sync, so no new TXG enters Syncing during teardown
    /// and the sync side drains its current cycle before exiting.
    /// Idempotent: a second call is a no-op.
    pub(super) fn stop_txg_threads(&self) {
        if let Some(mut q) = self.txg_quiesce.lock().take() {
            q.stop();
        }
        if let Some(mut s) = self.txg_sync.lock().take() {
            s.stop();
        }
    }

    /// Force-fold every L2P shard's TXG ring buffer into its on-disk
    /// tree synchronously. Used by:
    ///
    /// - [`Db::flush_with_gate`]'s inline path (when
    ///   `txg_threads_enabled = false`). The sample-phase no longer
    ///   holds `apply_gate.write`; serialisation against concurrent
    ///   commits' `cow_for_write` falls on the per-shard `tree.write()`
    ///   this helper takes.
    /// - The snapshot / `range_delete` / `drop_volume` / `clone_volume`
    ///   / `drop_snapshot` paths. Those paths now drive a forced TXG
    ///   sync via `flush_with_gate(Forced)` at entry, so by the time
    ///   they call this helper the slots are already drained — the
    ///   call becomes a defensive no-op. `drop_gate.write` held by
    ///   the lifecycle op then keeps slots empty.
    /// - The post-replay path in `open_with_config_and_faults`,
    ///   before any commit can race the recovered buffer state.
    ///
    /// When `txg_threads_enabled = true`, the `TxgSyncThread` is the
    /// regular drainer (one slot per TXG cycle). Lifecycle ops still
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
            for shard in &vol.shards {
                if !shard.use_buffer {
                    continue;
                }
                let started = Instant::now();
                let mut tree = shard.tree.write();
                // Drain ALL four TXG slots in one shot. Used only by
                // paths that need every slot folded NOW: lifecycle ops
                // (which hold `drop_gate.write`, so the slots are
                // quiesced), the post-replay open, and the threads-OFF
                // inline flush. The regular threads-ON per-TXG sync uses
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
                let apply_result =
                    super::txg_sync::compact_drain_into_tree(&mut tree, &drained);
                match apply_result {
                    Ok(()) => {
                        super::apply::publish_l2p_read_view(shard, &tree);
                        drop(tree);
                        shard.l2p_buffer.note_compacted(max_lsn);
                        self.metrics
                            .record_l2p_buffer_compaction(count, started.elapsed());
                    }
                    Err(err) => {
                        drop(tree);
                        return Err(err);
                    }
                }
            }
        }
        Ok(())
    }

    /// Threads-ON per-TXG sync drain: fold ONLY the frozen syncing slot
    /// (`txg & 3`) of every L2P shard into its tree, the ZFS-faithful
    /// per-TXG separation the 4-slot ring was built for. The Open and
    /// Quiescing slots are left untouched — they drain on their own
    /// future sync cycles, so each cycle's work is bounded by one TXG's
    /// writes (≈ `txg_timeout`) instead of the whole accumulated backlog.
    ///
    /// **Publish-before-clear** is the load-bearing correctness rule.
    /// `lookup_for_open_txg` (used by both a commit's prev-value read in
    /// `apply_l2p_bucket_buffer` and a user read) walks `open .. open-2`,
    /// which includes the syncing slot, and falls through to `read_view`
    /// only on a miss. If we cleared the slot before folding+publishing,
    /// a concurrent lookup could miss the (now empty) slot and read the
    /// stale `read_view`, falsely reporting `prev = None` → a refcount /
    /// space leak on the onyx side. So per shard, under `tree.write()`:
    ///   1. snapshot the syncing slot (clone, NOT clear) — readers still
    ///      hit the live slot entries during the fold;
    ///   2. fold the snapshot into the tree;
    ///   3. `publish_l2p_read_view` — the tree/read_view now carry the
    ///      entries;
    ///   4. `take_syncing_slot` (clear). A lookup that acquires the slot
    ///      lock after this sees it empty, and the slot-lock release/
    ///      acquire edge guarantees it then observes the published view.
    /// The slot being frozen (Syncing state, no concurrent inserts) is
    /// what makes the step-1 snapshot equal the step-4 clear.
    ///
    /// We clone in step 1 rather than hold the slot lock across the fold
    /// so a concurrent `lookup_for_open_txg` is never blocked for the
    /// fold's duration (read-latency protection).
    pub(super) fn drain_syncing_slot_into_trees(&self, txg: crate::types::Txg) -> Result<()> {
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
        // internally synchronized. The previously-serial fold was the txg-sync
        // drain bottleneck (on-CPU: ~74% of the `metadb-txg-sync` thread in
        // `compact_drain_into_tree`); serializing 16 shards' COW folds on one
        // thread bounded single-volume write throughput, so by default we fan
        // out across shards (mirrors the refcount `begin_checkpoint` fan-out in
        // `run_sync_cycle_body`). The `parallel_l2p_drain_enabled=false` path
        // preserves the serial fold for A/B and as a fallback.
        let metrics = &self.metrics;
        if self.parallel_l2p_drain_enabled {
            let results: Vec<Result<()>> = std::thread::scope(|scope| {
                let mut handles = Vec::new();
                for vol in &vols {
                    for shard in &vol.shards {
                        if !shard.use_buffer {
                            continue;
                        }
                        handles.push(scope.spawn(move || {
                            // Clear the single-CPU affinity inherited from the
                            // pinned `metadb-txg-sync` parent so these workers
                            // actually spread across cores (ZFS `dp_sync_taskq`
                            // runs unpinned at normal priority). Without this all
                            // 16 pile onto one core → slower than serial.
                            crate::affinity::unbind_current();
                            Self::drain_one_syncing_shard(shard, txg, metrics)
                        }));
                    }
                }
                handles
                    .into_iter()
                    .map(|h| h.join().unwrap_or_else(|p| std::panic::resume_unwind(p)))
                    .collect()
            });
            // First error wins. Every shard that could drain has drained and
            // cleared its frozen slot, so a retry of this TXG only re-processes
            // the shard(s) that errored (their slots are still populated).
            for r in results {
                r?;
            }
        } else {
            for vol in &vols {
                for shard in &vol.shards {
                    if !shard.use_buffer {
                        continue;
                    }
                    Self::drain_one_syncing_shard(shard, txg, metrics)?;
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
    fn drain_one_syncing_shard(
        shard: &super::L2pShard,
        txg: crate::types::Txg,
        metrics: &crate::metrics::MetaMetrics,
    ) -> Result<()> {
        let started = std::time::Instant::now();
        // Snapshot (clone) the frozen syncing slot WITHOUT the tree lock so the
        // brief slot-lock is not held across the fold. Empty → nothing to do.
        let entries = shard.l2p_buffer.snapshot_syncing_slot(txg);
        if entries.is_empty() {
            return Ok(());
        }
        let count = entries.len();
        let max_lsn = entries.values().map(|e| e.lsn).max().unwrap_or(0);
        let mut tree = shard.tree.write();
        super::txg_sync::compact_drain_into_tree(&mut tree, &entries)?;
        // Publish BEFORE clearing the slot (see method doc).
        super::apply::publish_l2p_read_view(shard, &tree);
        drop(tree);
        // Clear the now-folded+published slot. Frozen, so this equals `entries`;
        // the return value is discarded.
        let _ = shard.l2p_buffer.take_syncing_slot(txg);
        shard.l2p_buffer.note_compacted(max_lsn);
        metrics.record_l2p_buffer_compaction(count, started.elapsed());
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

    /// Number of shards in this database. In Phase B commit 5 this reports
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
