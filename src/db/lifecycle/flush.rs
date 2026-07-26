use super::manifest_refresh::{refresh_manifest_durable_seq, refresh_manifest_from_checkpoints};
use super::*;

const SLOW_SYNC_CYCLE_WARN_US: u64 = 1_000_000;

#[derive(Debug, Default)]
struct SyncCycleTrace {
    terminal_phase: &'static str,
    bfg_l2p_work: u64,
    dedup_drain_us: u64,
    l2p_fold_us: u64,
    sample_wall_us: u64,
    sample_lock_us: u64,
    sample_l2p_walk_us: u64,
    rc_drain_wall_us: u64,
    rc_fold_wait_sum_us: u64,
    rc_fold_service_sum_us: u64,
    rc_stream_write_sum_us: u64,
    io_us: u64,
    publish_barrier_wait_us: u64,
    manifest_us: u64,
    install_us: u64,
    prefold_wait_us: u64,
    reclaim_us: u64,
    selected_l2p_shards: u64,
    dirty_l2p_shards: u64,
    selected_rc_shards: u64,
    dirty_rc_shards: u64,
    l2p_dirty_pages: u64,
    rc_drained_deltas: u64,
    rc_fresh_pages: u64,
    rc_staged_pages: u64,
    rc_stream_pages: u64,
    nonstream_write_pages: u64,
}

impl SyncCycleTrace {
    fn enter_phase(
        &mut self,
        db: &Db,
        bfg: crate::types::Bfg,
        kind: crate::metrics::FlushKind,
        phase: crate::metrics::CheckpointSyncPhase,
    ) {
        self.terminal_phase = phase.as_str();
        db.metrics.record_checkpoint_sync_phase(
            bfg,
            kind,
            phase,
            Some(db.last_applied_lsn_best_effort()),
        );
    }

    fn warn_if_slow(
        &self,
        bfg: crate::types::Bfg,
        kind: crate::metrics::FlushKind,
        threaded: bool,
        rc_streaming: bool,
        total: std::time::Duration,
        result: &Result<()>,
    ) {
        let total_us = micros(total);
        if total_us < SLOW_SYNC_CYCLE_WARN_US {
            return;
        }

        let outcome = if result.is_ok() { "ok" } else { "error" };
        let error = result
            .as_ref()
            .err()
            .map(ToString::to_string)
            .unwrap_or_default();
        tracing::warn!(
            bfg,
            kind = ?kind,
            threaded,
            rc_streaming,
            outcome,
            error = %error,
            total_us,
            terminal_phase = self.terminal_phase,
            bfg_l2p_work = self.bfg_l2p_work,
            dedup_drain_us = self.dedup_drain_us,
            l2p_fold_us = self.l2p_fold_us,
            sample_wall_us = self.sample_wall_us,
            sample_lock_us = self.sample_lock_us,
            sample_l2p_walk_us = self.sample_l2p_walk_us,
            rc_drain_wall_us = self.rc_drain_wall_us,
            rc_fold_wait_sum_us = self.rc_fold_wait_sum_us,
            rc_fold_service_sum_us = self.rc_fold_service_sum_us,
            rc_stream_write_sum_us = self.rc_stream_write_sum_us,
            io_us = self.io_us,
            publish_barrier_wait_us = self.publish_barrier_wait_us,
            manifest_us = self.manifest_us,
            install_us = self.install_us,
            prefold_wait_us = self.prefold_wait_us,
            reclaim_us = self.reclaim_us,
            selected_l2p_shards = self.selected_l2p_shards,
            dirty_l2p_shards = self.dirty_l2p_shards,
            selected_rc_shards = self.selected_rc_shards,
            dirty_rc_shards = self.dirty_rc_shards,
            l2p_dirty_pages = self.l2p_dirty_pages,
            rc_drained_deltas = self.rc_drained_deltas,
            rc_fresh_pages = self.rc_fresh_pages,
            rc_staged_pages = self.rc_staged_pages,
            rc_stream_pages = self.rc_stream_pages,
            nonstream_write_pages = self.nonstream_write_pages,
            "metadb: slow checkpoint cycle"
        );
    }
}

impl Db {
    /// Persist dirty shard pages and commit a fresh manifest with the
    /// current per-shard roots + checkpoint LSN.
    ///
    /// `checkpoint_lsn` is set to the WAL LSN of the most-recently-
    /// applied commit, so after `open` replay can correctly begin at
    /// `checkpoint_lsn + 1`.
    pub fn flush(&self) -> Result<()> {
        self.flush_with_gate(crate::metrics::FlushKind::Forced)
            .map(|_| ())
    }

    /// Block until every apply lane has drained the work currently in
    /// its queue. Provides a read-your-writes sync point for the async
    /// dedup commit path: after a commit returns, `DedupPut` /
    /// `DedupPutGuarded` / `DedupDelete` ops may still be queued on a
    /// dedup lane worker; calling this method waits for them to apply
    /// so a subsequent `get_dedup` observes the put.
    ///
    /// Per-lane drain semantics: snapshot each lane's
    /// `last_enqueued_lsn` and wait for `last_applied_lsn` to catch
    /// up. Enqueues that arrive after the snapshot are not waited for,
    /// so this method is bounded even with concurrent writers.
    pub fn wait_apply_idle(&self) {
        for volume in self.volumes.read().values() {
            for shard in &volume.shards {
                shard.apply_lane.wait_for_drain();
            }
        }
        for shard in self.refcount_shards.iter() {
            shard.apply_lane.wait_for_drain();
        }
        for lane in self.dedup_lanes.iter() {
            lane.wait_for_drain();
        }
        for lane in self.dedup_maintenance_lanes.iter() {
            lane.wait_for_drain();
        }
    }

    /// Sum of every shard's `last_flushed_lsn`'s minimum. WAL prune /
    /// `manifest.checkpoint_lsn` must not advance past this — any op
    /// at a later LSN may still be pending on at least one
    /// unflushed shard, and WAL is the only source recovery has for
    /// it. Walks all L2P shards (across all volumes) plus every RC
    /// shard, taking the minimum. The min naturally falls to 0 if
    /// any shard has never been flushed (fresh DB at create-time).
    pub fn compute_min_last_flushed_lsn(&self) -> Lsn {
        use std::sync::atomic::Ordering;
        let mut min_lsn = Lsn::MAX;
        for volume in self.volumes.read().values() {
            for shard in &volume.shards {
                let lsn = shard.last_flushed_lsn.load(Ordering::Acquire);
                if lsn < min_lsn {
                    min_lsn = lsn;
                }
                // B2: buffer-compaction term. See
                // `compute_min_last_flushed_lsn_after` for the rationale.
                if shard.use_buffer {
                    let buf_lsn = shard.l2p_buffer.compacted_lsn();
                    if buf_lsn < min_lsn {
                        min_lsn = buf_lsn;
                    }
                }
            }
        }
        for shard in &self.refcount_shards {
            let lsn = shard.last_flushed_lsn.load(Ordering::Acquire);
            if lsn < min_lsn {
                min_lsn = lsn;
            }
        }
        if min_lsn == Lsn::MAX { 0 } else { min_lsn }
    }

    /// Choose which L2P + RC shards this `flush_with_gate` invocation
    /// will sample. The selection walks all shards in round-robin
    /// order starting from `flush_cursor`, accepting shards with
    /// non-zero dirty work until the cumulative work crosses
    /// `flush_select_budget`. Shards past the budget keep their
    /// previous root + `last_flushed_lsn`; `manifest.checkpoint_lsn`
    /// is set to the global min so WAL prune stays safe.
    ///
    /// Budget == 0 (or larger than the live working set) yields a
    /// full-sample selection, preserving the pre-partial behaviour.
    /// `force_all = true` (forced flush / snapshot / shutdown drain)
    /// also returns a full selection regardless of budget.
    ///
    /// Every volume in `force_volume_ords` gets all of its L2P shards selected
    /// unconditionally. A pending `take_snapshot` task needs the target
    /// volume's roots flushed durable this cycle so the committed
    /// `SnapshotEntry` references on-disk pages; the cycle that drains the task
    /// is not guaranteed to be `Forced`.
    fn select_shards_for_flush(
        &self,
        volumes: &[Arc<Volume>],
        force_all: bool,
        force_volume_ords: &std::collections::HashSet<VolumeOrdinal>,
    ) -> SelectedShards {
        use std::sync::atomic::Ordering;
        let mut l2p: Vec<Vec<bool>> = volumes
            .iter()
            .map(|v| vec![false; v.shards.len()])
            .collect();
        let mut rc = vec![false; self.refcount_shards.len()];

        if force_all || self.flush_select_budget == 0 {
            for entry in l2p.iter_mut() {
                for slot in entry.iter_mut() {
                    *slot = true;
                }
            }
            for slot in rc.iter_mut() {
                *slot = true;
            }
            return SelectedShards { l2p, rc };
        }

        // Force-select the L2P shards of every volume with a pending
        // sync task this round (roots must be flushed durable before the
        // manifest entry references them). RC shards are NOT forced: the
        // snapshot incref stages into the OPEN BFG slot and folds in a
        // future cycle, so it does not need this cycle's rc fold.
        let force_l2p = |l2p: &mut Vec<Vec<bool>>| {
            if force_volume_ords.is_empty() {
                return;
            }
            for (v_idx, vol) in volumes.iter().enumerate() {
                if force_volume_ords.contains(&vol.ord) {
                    for slot in l2p[v_idx].iter_mut() {
                        *slot = true;
                    }
                }
            }
        };

        // Build a single flat round-robin order across (kind, volume,
        // shard_idx). Each entry carries the dirty-work estimate so a
        // single budget pass can stop without revisiting.
        #[derive(Clone, Copy)]
        enum Kind {
            L2p(usize, usize), // volume_idx, shard_idx
            Rc(usize),
        }
        let mut order: Vec<(Kind, usize)> = Vec::new();
        for (v_idx, vol) in volumes.iter().enumerate() {
            for (s_idx, shard) in vol.shards.iter().enumerate() {
                let dirty = shard
                    .tree
                    .try_read()
                    .map(|tree| tree.growth_summary().3)
                    .unwrap_or(0);
                order.push((Kind::L2p(v_idx, s_idx), dirty));
            }
        }
        // v27: a shard with un-condensed segments but an empty slot has zero
        // pending yet still needs selecting so its condense (K segments / overlay
        // cap) can fire — otherwise the directory never drains (verdict g).
        let rc_condense_k = self.rc_condense_interval_cycles;
        let rc_overlay_per_shard_cap =
            (self.rc_segment_overlay_max_entries / self.refcount_shards.len().max(1)).max(1);
        let persist = self.rc_delta_run_persist_enabled;
        for (s_idx, shard) in self.refcount_shards.iter().enumerate() {
            let mut dirty = shard.rc.pending_delta_count();
            if persist {
                let segments = shard.rc.segment_count() as u64;
                if (rc_condense_k > 0 && segments >= rc_condense_k)
                    || shard.rc.segment_overlay_entries() as u64 > rc_overlay_per_shard_cap as u64
                {
                    dirty += 1;
                }
            }
            order.push((Kind::Rc(s_idx), dirty));
        }

        if order.is_empty() {
            force_l2p(&mut l2p);
            return SelectedShards { l2p, rc };
        }

        let start = self.flush_cursor.load(Ordering::Relaxed) % order.len();
        let budget = self.flush_select_budget;
        let mut taken = 0usize;
        let mut work = 0usize;
        let mut cursor_advance = 0usize;
        for step in 0..order.len() {
            let idx = (start + step) % order.len();
            let (kind, dirty) = order[idx];
            if dirty == 0 {
                continue;
            }
            // First accept always lands even if a single shard alone
            // exceeds the budget — otherwise a hot shard would never
            // be drained. After the first, stop once we've exceeded.
            if taken > 0 && work.saturating_add(dirty) > budget {
                break;
            }
            match kind {
                Kind::L2p(v, s) => l2p[v][s] = true,
                Kind::Rc(s) => rc[s] = true,
            }
            work = work.saturating_add(dirty);
            taken += 1;
            cursor_advance = step + 1;
        }

        if taken > 0 {
            let next = (start + cursor_advance) % order.len();
            self.flush_cursor.store(next, Ordering::Relaxed);
        }
        // Note: if every shard had `dirty == 0`, the selection is
        // empty. `flush_with_gate` short-circuits that case as a
        // no-op flush (still bumps `last_applied_lsn` checkpoint via
        // the global min, which is a no-op when nothing changed).

        force_l2p(&mut l2p);
        SelectedShards { l2p, rc }
    }

    /// Best-effort checkpoint for background maintenance. If commits are
    /// currently applying, this returns `Ok(false)` without setting the
    /// apply gate's writer-pending bit, so foreground commit readers keep
    /// flowing and the caller can retry on the next interval.
    pub fn try_flush(&self) -> Result<bool> {
        self.flush_with_gate(crate::metrics::FlushKind::Steady)
    }

    /// BFG: `flush_with_gate` is now a thin
    /// shell. The actual per-BFG sync work lives in
    /// [`Db::run_sync_cycle`]. Two modes:
    ///
    /// - **`bfg_threads_enabled = true`** (production default): force
    ///   the [`crate::db::bfg_quiesce::BfgQuiesceThread`] to roll the
    ///   current Open BFG immediately, then park on
    ///   [`crate::bfg::BfgStateMachine::wait_until_synced`] until the
    ///   [`crate::db::bfg_sync::BfgSyncThread`] has finished
    ///   `run_sync_cycle` for that BFG (which is what
    ///   `start_bfg_threads` wired the worker's `sync_work` callback
    ///   to).
    /// - **`bfg_threads_enabled = false`**: the worker threads are not
    ///   spawned. `flush_with_gate` drives the BFG state machine and
    ///   `run_sync_cycle` synchronously on the caller thread,
    ///   recreating the legacy stop-the-world flush semantics.
    ///
    /// Returns `Ok(false)` only on the threads-off `Steady` path when
    /// `apply_gate.try_write()` does not immediately succeed (the same
    /// best-effort behaviour `try_flush` had before threaded sync).
    pub(in crate::db) fn flush_with_gate(&self, kind: crate::metrics::FlushKind) -> Result<bool> {
        // A prior forced sync failed non-recoverably and poisoned the
        // subsystem. Fail fast before touching the BFG state machine: its slot
        // is stuck in Syncing forever, so rolling or promoting would block.
        // Covers every forced-sync entry (take_snapshot, create_volume,
        // restore, plain flush) in both modes.
        if let Some(err) = self.sync_poison_error() {
            return Err(err);
        }
        if self.bfg_threads_enabled {
            // Threads-on: hand off to the sync thread and wait.
            // `signal_force` only sets the quiesce notifier flag; the
            // quiesce worker then calls `roll_to_quiescing`, which
            // does its own inflight-drain wait. No
            // `apply_gate.try_write` race here — the threaded path
            // always runs to completion.
            let target = self.bfg.open_bfg();
            self.bfg_quiesce_notifier.signal_force(target);
            if !self.bfg.wait_until_synced(target) {
                // Aborted: the sync thread's cycle failed and poisoned the
                // subsystem. Surface the restart-required error instead of
                // reporting a clean flush.
                return Err(self.sync_poison_error().unwrap_or_else(|| {
                    MetaDbError::Corruption("bfg sync aborted; restart required".into())
                }));
            }
            self.metrics.record_flush_attempt(kind);
            // Wall-time accounting kept consistent with the inline
            // path: every threaded `flush()` records as a completed
            // forced-flush (Steady never lands here because Steady is
            // only emitted by `try_flush`, which routes the threads-on
            // side through `wait_until_synced` regardless).
            self.metrics
                .record_flush_total(kind, std::time::Duration::from_micros(0));
            return Ok(true);
        }
        // Threads-off: drive the BFG state machine ourselves so the
        // sync metadata bookkeeping (slot.max_lsn, open_bfg advance,
        // checkpoint_bfg / wait_until_synced cv release) stays in
        // lock-step with what the threaded path would have done.
        // `roll_to_quiescing` waits for any in-flight commits' bfg
        // guards to drop, mirroring the apply-gate barrier the body
        // would have taken anyway. After `mark_synced` returns,
        // `wait_until_synced(target)` (used by any concurrent
        // threads-off caller) wakes up.
        //
        // Steady-kind try_flush keeps best-effort semantics: skip the
        // roll entirely when `apply_gate.try_write()` fails. Forced
        // kind always rolls.
        let blocking_gate = matches!(kind, crate::metrics::FlushKind::Forced);
        if !blocking_gate && let None = self.apply_gate.try_write() {
            self.metrics.record_flush_attempt(kind);
            self.metrics
                .record_flush_total(kind, std::time::Duration::ZERO);
            return Ok(false);
        }
        // We don't actually hold `apply_gate.write()` here — the
        // run_sync_cycle body re-acquires it for the sample phase.
        // The try-write above is purely a "should we even bother
        // rolling" gate matching the legacy try_flush behaviour.
        let target = self.bfg.roll_to_quiescing();
        // `roll_to_quiescing` is idempotent under shutdown (returns
        // current open without advancing). Re-check whether the slot
        // actually moved to Quiescing before promoting; mismatch
        // means shutdown raced and we should skip.
        if self.bfg.snapshot().quiescing_bfg != Some(target) {
            return Ok(false);
        }
        self.bfg.promote_to_syncing(target);
        // If a concurrent forced-sync cycle aborted the subsystem while we were
        // parked in `promote_to_syncing`, the slot may still be Quiescing rather
        // than Syncing. Driving `run_sync_cycle(target)` would then mark the
        // wrong state as synced and corrupt the ring, so re-check and return the
        // restart-required error before touching the cycle.
        if self.bfg.is_aborted() {
            return Err(self.sync_poison_error().unwrap_or_else(|| {
                MetaDbError::Corruption("bfg sync aborted; restart required".into())
            }));
        }
        let result = self.run_sync_cycle(target, kind);
        match result {
            Ok(()) => {
                self.bfg.mark_synced(target);
                self.metrics.record_checkpoint_sync_phase(
                    target,
                    kind,
                    crate::metrics::CheckpointSyncPhase::Idle,
                    Some(self.last_applied_lsn_best_effort()),
                );
                Ok(true)
            }
            Err(err) => {
                // The sync cycle failed and may have left deferred RC apply
                // state un-retryable. Leave the slot in Syncing (preserves the
                // `failed_sync_leaves_bfg_in_syncing_state` contract), but
                // poison the sync subsystem so the next forced flush /
                // take_snapshot fails fast instead of hanging on the stuck slot.
                // Recovery is process restart.
                self.poison_sync(&err);
                Err(err)
            }
        }
    }

    /// Per-BFG sync work: drain L2P buffers, sample + IO + manifest
    /// commit. Shared by the inline path and the `BfgSyncThread` callback so
    /// the `BfgSyncThread`'s `sync_work` callback can drive it via
    /// `Weak<Db>` upgrade.
    ///
    /// `bfg` identifies the slot the caller has already promoted to
    /// Syncing in the state machine.
    ///
    /// The body NO LONGER holds `apply_gate.write()` across the sample
    /// phase. Every lifecycle op (`take_snapshot`, `drop_snapshot`,
    /// `drop_volume`, `clone_volume`, `range_delete`, `create_volume`)
    /// now drives a forced BFG sync at entry, so by the time it does
    /// any rc-mutating whole-page write, the in-flight flush IO phase
    /// has finished and `drop_gate.write` is preventing new Dirty Arcs
    /// from forming. The remaining serialisation is per-shard
    /// `tree.write()` (held across `begin_checkpoint`) plus the
    /// internal `RcShard` delta-lock orchestration.
    ///
    /// `apply_gate.write()` is still re-acquired briefly around the
    /// manifest commit + post-manifest atomics bump so the on-disk
    /// `(manifest, last_flushed_lsn)` pair stays consistent against
    /// other `apply_gate.write()` holders.
    ///
    /// `bfg` is threaded through to the body. `wal_checkpoint` is now
    /// read from `self.bfg.slot_max_lsn(bfg)` (was
    /// `last_applied_lsn`). Reasons:
    ///
    /// - `slot_max_lsn(bfg)` is **frozen** for the Syncing slot:
    ///   `promote_to_syncing` precondition is `inflight == 0` and
    ///   `record_lsn` cannot fire on a non-Open slot. By contrast
    ///   `last_applied_lsn` can advance during the gateless sample if
    ///   a commit stamped to a later BFG completes its apply.
    /// - `slot_max_lsn(bfg)` precisely delineates "the LSNs in THIS
    ///   BFG"; any commit stamped to BFG > `bfg` belongs to a future
    ///   sync, not this one.
    ///
    /// Post-recovery: `apply_replay_batch` does NOT go through
    /// `BfgGuard::record_lsn`, so without intervention
    /// `slot_max_lsn(open_bfg)` would be 0 after replay.
    /// [`Db::open_with_config_and_faults`] stamps the open slot with
    /// the post-replay `last_applied` LSN right after constructing the
    /// `BfgStateMachine`, closing that gap. The
    /// `compute_min_last_flushed_lsn_after` projection still uses
    /// `max(wal_checkpoint, prev)` per shard as defense-in-depth.
    ///
    /// Lifecycle ops (drop_snapshot / drop_volume / clone_volume /
    /// create_volume / range_delete) symmetrically enter
    /// `self.bfg.enter()` after their forced sync barrier and call
    /// `_bfg_guard.record_lsn(lsn)` after `submit_wal_ops`, so their
    /// LSNs land in `slot_max_lsn(open_bfg)` exactly like the
    /// `commit_ops` hot path. Without that, the lifecycle WAL records
    /// would never reach `wal_checkpoint` and `prune_all_segments`
    /// would leave their segments alive forever.
    pub(crate) fn run_sync_cycle(
        &self,
        bfg: crate::types::Bfg,
        kind: crate::metrics::FlushKind,
    ) -> Result<()> {
        let started = std::time::Instant::now();
        let mut trace = SyncCycleTrace::default();
        trace.enter_phase(
            self,
            bfg,
            kind,
            crate::metrics::CheckpointSyncPhase::CycleStart,
        );
        // Capture before the syncing-slot drain clears buffered entries. The
        // slow-cycle trace can then prove the admission cohort bound directly.
        trace.bfg_l2p_work = self.bfg.slot_l2p_work(bfg) as u64;
        let result = self.run_sync_cycle_body(bfg, kind, &mut trace);
        trace.warn_if_slow(
            bfg,
            kind,
            self.bfg_threads_enabled,
            self.rc_checkpoint_streaming_enabled,
            started.elapsed(),
            &result,
        );
        if result.is_err() {
            trace.enter_phase(self, bfg, kind, crate::metrics::CheckpointSyncPhase::Error);
        }
        result
    }

    fn run_sync_cycle_body(
        &self,
        bfg: crate::types::Bfg,
        kind: crate::metrics::FlushKind,
        trace: &mut SyncCycleTrace,
    ) -> Result<()> {
        // BFG gate-shrink (final): the SAMPLE phase
        // runs gateless. The serialisation it used to get from
        // `apply_gate.write()` is now provided by:
        //
        // - Lifecycle ops hold `drop_gate.write` for the duration, which
        //   keeps the syncing slot empty (no new Dirty Arcs form). BFG
        //   page-rc removal deleted per-L2P-page refcounting, so lifecycle ops no longer
        //   RMW any whole-page rc — a concurrent flush IO phase has nothing
        //   to clobber, which is why `take_snapshot` / `drop_snapshot` /
        //   `clone_volume` no longer force-sync at entry (buffer-backed journal).
        //   (`range_delete` / `drop_volume` / `create_volume` still
        //   force-sync; their removal is a later step.)
        // - Per-shard `tree.write()` held during this flush's
        //   `lock_selected_l2p_shards_for` excludes lifecycle's
        //   `lock_all_l2p_shards_for` from racing the sample.
        // - `RcShard`'s internal delta-lock orchestration handles the
        //   commit-side `RcShard::stage` race (see
        //   [`refcount_drainer_layer_atomicity`](memory) for the case
        //   invariants).
        // - `apply_gate.write()` is RE-acquired around the manifest
        //   commit + post-manifest atomics bump so the on-disk
        //   `(manifest, last_flushed_lsn)` pair stays consistent
        //   against lifecycle ops' own manifest commits.
        // - Dead-list drain takes only records with
        //   `death_lsn <= wal_checkpoint`, keeping the
        //   chain-monotonicity invariant correct even though no
        //   sample-phase gate covers `wal_checkpoint`'s read.
        // - `compute_min_last_flushed_lsn_after` and
        //   `refresh_manifest_durable_seq` use `max(wal_checkpoint,
        //   prev)` per shard, and the post-manifest atomic store is
        //   `fetch_max` — never regresses an individual shard's
        //   durability.
        //
        // `kind` separates the steady-state `try_flush()` cadence from
        // forced `flush()` (shutdown drain, explicit force_checkpoint)
        // in the metrics — `flush_sample_max_us_steady` excludes the
        // shutdown blast that otherwise dominates the aggregate max.
        self.metrics.record_flush_attempt(kind);
        let flush_started = std::time::Instant::now();
        // No sample-phase gate wait under the new model. Record 0 so
        // dashboards still see a sample observation per flush; the
        // gate-wait metric continues to be meaningful at the
        // manifest-commit gate acquisition below.
        self.metrics
            .record_flush_gate_wait(std::time::Duration::ZERO);

        // The refcount fold is now inline + per-BFG-slot (no background rc
        // drainer to preempt/resume — see `refcount::shard`), so no RAII
        // resume guard is needed here. The dedup-index drainer below still
        // has its own.

        // Dedup checkpoint barrier: preempt the async dedup drainers and
        // synchronously final-drain all staged cuckoo mutations BEFORE
        // the rc/L2P sample, the dedup manifest update / `flush_meta`,
        // and the buffer-seq sample below. The BFG syncing slot is
        // frozen (quiesce waited for all its commits, so every staged
        // entry <= `checkpoint_lsn` is present and drained here); newer
        // open-bfg commits (lsn > checkpoint_lsn) stay staged for the
        // next checkpoint, so `checkpoint_lsn` never advances past an
        // undrained staged entry. Placed before any checkpoint is taken
        // so a drain IO error returns cleanly with nothing to abort; the
        // RAII guard re-arms the drainers on every exit path. No-op when
        // `dedup_drainer_enabled = false` (staging is empty then).
        let _dedup_drainer_resume_guard = DedupDrainerResumeGuard {
            dedup_index: &self.dedup_index,
        };
        trace.enter_phase(
            self,
            bfg,
            kind,
            crate::metrics::CheckpointSyncPhase::DedupDrain,
        );
        let dedup_drain_started = std::time::Instant::now();
        let dedup_drain_result = self
            .dedup_index
            .preempt_and_drain_for_checkpoint(&self.metrics);
        let dedup_drain_elapsed = dedup_drain_started.elapsed();
        trace.dedup_drain_us = micros(dedup_drain_elapsed);
        self.metrics.record_flush_dedup_drain(dedup_drain_elapsed);
        dedup_drain_result?;

        // Fold the buffered L2P updates into the tree so the sample
        // phase observes them. Two drains:
        //
        // - **threads-ON** (`BfgSyncThread` driving this for one BFG group):
        //   `drain_syncing_slot_into_trees(bfg)` folds ONLY the frozen
        //   syncing slot, publish-before-clear. Open/Quiescing slots stay
        //   buffered and drain on their own future cycles, bounding each sync
        //   to one group's writes.
        // - **threads-OFF** (inline `flush_with_gate` is the sole
        //   drainer): `force_compact_l2p_buffers` folds ALL slots so a
        //   single flush persists everything. Lifecycle ops also use the
        //   drain-all path under `drop_gate.write`.
        trace.enter_phase(
            self,
            bfg,
            kind,
            crate::metrics::CheckpointSyncPhase::L2pFold,
        );
        let l2p_fold_started = std::time::Instant::now();
        let l2p_fold_result = if self.bfg_threads_enabled {
            self.drain_syncing_slot_into_trees(bfg)
        } else {
            self.force_compact_l2p_buffers()
        };
        let l2p_fold_elapsed = l2p_fold_started.elapsed();
        trace.l2p_fold_us = micros(l2p_fold_elapsed);
        self.metrics.record_flush_l2p_fold(l2p_fold_elapsed);
        l2p_fold_result?;

        trace.enter_phase(self, bfg, kind, crate::metrics::CheckpointSyncPhase::Sample);
        let sample_started = std::time::Instant::now();
        // `slot_max_lsn(bfg)` is the BFG-frozen high-water LSN of
        // commits stamped to the Syncing slot. `promote_to_syncing`
        // closed the slot to new `record_lsn` calls before this body
        // runs, and `roll_to_quiescing` already drained the inflight
        // commits (each commit holds its `BfgGuard` across submit +
        // apply + finish_global_apply), so by the time we read this
        // every commit in BFG `bfg` has completed its tree mutation
        // and the value is stable for the rest of the sync cycle.
        //
        // We deliberately do NOT read `self.last_applied_lsn` here —
        // it can include commits stamped to BFGs > `bfg` (the new
        // Open slot opened by `roll_to_quiescing`), which belong to a
        // future sync, not this one. Using `slot_max_lsn(bfg)` keeps
        // each BFG's WAL prune watermark cleanly bounded by its own
        // stamps.
        //
        // Recovery: `apply_replay_batch` does NOT call `record_lsn`
        // on a BfgGuard, so without intervention `slot_max_lsn(open_bfg)`
        // would be 0 after replay. `Db::open_with_config_and_faults`
        // stamps the open slot with the post-replay `last_applied` LSN
        // (see Cut 2 below). The `compute_min_last_flushed_lsn_after`
        // projection still uses `max(wal_checkpoint, prev)` per shard
        // as defense-in-depth.
        //
        // Snapshot-root increfs are folded into this cycle's page-rc checkpoint.
        // They always apply and never bump a page generation, so they need no
        // reserved LSN, no `finish_global_apply` hole, and no dependence on
        // `wal_checkpoint`.
        let wal_checkpoint = self.bfg.slot_max_lsn(bfg);
        // `mut` so the gate-window reconciliation below can prune volumes
        // dropped between this gateless sample and the manifest commit.
        let mut volumes = self.volumes_snapshot();
        // Volumes with a `take_snapshot` task queued for this bfg. Their L2P
        // shards are force-selected below so the roots the snapshot entry will
        // reference are flushed durable this cycle.
        let snapshot_force_ords: std::collections::HashSet<VolumeOrdinal> = {
            let tasks = self.pending_sync_tasks.lock();
            tasks
                .iter()
                .filter(|t| t.target_bfg == bfg)
                .map(|t| match &t.op {
                    crate::db::SyncTaskOp::TakeSnapshot { vol_ord } => *vol_ord,
                })
                .collect()
        };
        // Decide which shards this round samples. Forced flushes
        // (`flush()`, snapshot, shutdown) always select everything;
        // steady-state `try_flush()` honours the budget cap.
        // `mut` so the gate-window reconciliation below can prune the
        // per-volume `selected.l2p` rows of volumes dropped mid-cycle.
        let mut selected = self.select_shards_for_flush(
            &volumes,
            matches!(kind, crate::metrics::FlushKind::Forced),
            &snapshot_force_ords,
        );
        trace.selected_l2p_shards = selected
            .l2p
            .iter()
            .flatten()
            .filter(|selected| **selected)
            .count() as u64;
        trace.selected_rc_shards = selected.rc.iter().filter(|selected| **selected).count() as u64;
        // Drain per-volume dead-list buffers. `DeadListState`'s internal
        // `Mutex<Vec<_>>` makes push/drain atomic. No sample-phase gate
        // holds new pushes back, so the `drain_up_to_lsn(wal_checkpoint)`
        // filter is what keeps the "chain must be strictly older going
        // backward" invariant in [`crate::verify`] correct
        // (segment `min_lsn` must exceed the prior segment's `max_lsn`).
        //
        // `manifest_state.lock()` (acquired in the gate window below)
        // serialises flush-vs-flush so the `old_head` / `old_tail`
        // snapshot here matches what gets promoted post-commit.
        let mut drained_deadlists: Vec<DeadListDrainEntry> = Vec::new();
        for vol in &volumes {
            let records = vol.dead_list.drain_up_to_lsn(wal_checkpoint);
            if records.is_empty() {
                continue;
            }
            drained_deadlists.push(DeadListDrainEntry {
                vol: vol.clone(),
                records: DrainRecords::Dead(records),
                old_head: vol
                    .dead_list_head_pid
                    .load(std::sync::atomic::Ordering::Acquire),
                old_tail: vol
                    .dead_list_tail_pid
                    .load(std::sync::atomic::Ordering::Acquire),
                kind: DeadListKind::Pba,
                page_provenance: Vec::new(),
            });
        }
        // BFG + H1: the second, independent chain — L2P page
        // deaths — is drained LATER, after the per-shard L2P root capture
        // loop below. Unlike the PBA / Live chains (bounded by the single
        // `wal_checkpoint`), each page accumulator must be sealed under its
        // OWN shard's `root_birth_lsn` (the gate-free sample can fold a
        // shard's root past `wal_checkpoint`; a `wal_checkpoint` bound would
        // leave those durable-in-root deaths unsealed → lost on crash). That
        // watermark is only available once the guard walk has sampled each
        // selected shard's root, so the drain is deferred to just after it.
        // BFG: the THIRD, independent chain — the per-clone
        // page-livelist (ALLOC/FREE of clone-private L2P pages). Same
        // `drain_up_to_lsn` filter + segment build / IO / rollback /
        // promotion machinery, tagged `Live` so it routes to the
        // `LiveListSegment` codec + the `page_live_list_*_pid` anchors.
        // Only clones ever accumulate records here (non-clone trees have
        // `clone_birth_lsn == None`), so this loop is a no-op for them.
        for vol in &volumes {
            let records = vol.page_live_list.drain_up_to_lsn(wal_checkpoint);
            if records.is_empty() {
                continue;
            }
            drained_deadlists.push(DeadListDrainEntry {
                vol: vol.clone(),
                records: DrainRecords::Live(records),
                old_head: vol
                    .page_live_list_head_pid
                    .load(std::sync::atomic::Ordering::Acquire),
                old_tail: vol
                    .page_live_list_tail_pid
                    .load(std::sync::atomic::Ordering::Acquire),
                kind: DeadListKind::Live,
                page_provenance: Vec::new(),
            });
        }
        if selected.is_empty() && drained_deadlists.is_empty() {
            // Nothing dirty enough to flush this round. Bail out
            // before locking any shards. `_drainer_resume_guard`
            // drops at scope end.
            let sample_elapsed = sample_started.elapsed();
            trace.sample_wall_us = micros(sample_elapsed);
            self.metrics.record_flush_sample(kind, sample_elapsed);
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            trace.enter_phase(
                self,
                bfg,
                kind,
                crate::metrics::CheckpointSyncPhase::Complete,
            );
            return Ok(());
        }
        let lock_started = std::time::Instant::now();
        let mut l2p_guards = lock_selected_l2p_shards_for(&volumes, &selected.l2p);
        let lock_elapsed = lock_started.elapsed();
        trace.sample_lock_us = micros(lock_elapsed);
        let tree_generation = max_generation_from_two_groups(&l2p_guards, &self.refcount_shards);
        let l2p_walk_started = std::time::Instant::now();
        // Sparse per-(volume, shard) checkpoint vector. `None`
        // entries are unselected shards: their root in the manifest
        // carries over and their `last_flushed_lsn` is unchanged.
        let mut l2p_checkpoints: Vec<Vec<Option<crate::paged::tree::Checkpoint>>> =
            Vec::with_capacity(volumes.len());
        // Capture the per-shard roots of every snapshot-target volume, in shard
        // order, under the same `tree.write()` this loop holds. These are
        // exactly the roots this cycle flushes durable and the snapshot entry
        // references. The volume was force-selected, so every one of its shards yields a
        // guard here.
        let mut snapshot_roots: std::collections::HashMap<VolumeOrdinal, Vec<PageId>> =
            std::collections::HashMap::new();
        // BFG: per-snapshot-target-volume fold watermark =
        // max root `birth_lsn` over its captured shards = the exact highest lsn
        // folded into the captured roots. Stamped as the snapshot's durable
        // `capture_watermark` (the birth COW-kill oracle's operand, NOT
        // `last_applied_lsn`). Captured under the same `tree.write()` the roots
        // are sampled from, so it is consistent with the roots.
        let mut snapshot_watermarks: std::collections::HashMap<VolumeOrdinal, Lsn> =
            std::collections::HashMap::new();
        let mut guard_iter = l2p_guards.drain(..);
        for (v_idx, volume) in volumes.iter().enumerate() {
            let mut per_volume: Vec<Option<crate::paged::tree::Checkpoint>> =
                Vec::with_capacity(volume.shards.len());
            let capture_roots = snapshot_force_ords.contains(&volume.ord);
            // H1: per-shard page-death drain merged into ONE entry per volume so
            // the page-deadlist chain stays single (one segment / anchor
            // promotion, reusing the PBA/Live machinery). `page_provenance`
            // records which shard each sub-vec came from so a rollback restores
            // it to the right accumulator.
            let mut page_merged: Vec<crate::deadlist::DeadRecord> = Vec::new();
            let mut page_provenance: Vec<(usize, Vec<crate::deadlist::DeadRecord>)> = Vec::new();
            for s_idx in 0..volume.shards.len() {
                if selected.l2p[v_idx][s_idx] {
                    let mut guard = guard_iter.next().expect(
                        "lock_selected_l2p_shards_for must hand out one guard per selected shard",
                    );
                    if capture_roots {
                        snapshot_roots
                            .entry(volume.ord)
                            .or_default()
                            .push(guard.root());
                        // capture_watermark = the shard's true FOLD watermark =
                        // max lsn folded into this tree = `next_generation() - 1`.
                        // NOT `root_birth_lsn()`: the root is mutated IN PLACE
                        // once it forks private off a prior snapshot (the RECYCLE
                        // arm returns the same pid without re-stamping birth, and
                        // `PageBuf::modify` ignores the op lsn), so `root.birth` is
                        // the FORK lsn, which UNDER-covers leaves born in
                        // `(fork_lsn, created_lsn]` that are still reachable from
                        // the captured root. An undercover watermark made the
                        // clone-branch `drain_page_deaths_into` filter
                        // (`birth <= youngest_snap_below(death)`) DROP a
                        // snapshot-pinned death on a promoted ex-clone, while the
                        // (non-clone) drop shadow still expected it → COMPLETENESS
                        // HOLE. `next_generation()` is advanced only at fold time
                        // (insert/delete/COW), never for buffered-but-unfolded
                        // writes, so it is tight: threads-ON it equals the syncing
                        // slot's max lsn (open-slot writes stay unfolded ⇒ no snapshot watermark
                        // over-pin); threads-OFF the force-fold drains every slot
                        // ⇒ it covers every reachable page. Strictly
                        // `>= root_birth_lsn` (the root was folded at its own
                        // birth), so it never regresses the prior bound.
                        let wm = guard.next_generation().saturating_sub(1);
                        let e = snapshot_watermarks.entry(volume.ord).or_insert(0);
                        if wm > *e {
                            *e = wm;
                        }
                    }
                    // H1 — THE FIX: drain this shard's ENTIRE page-death
                    // accumulator while the tree guard is held, atomic with the
                    // `begin_checkpoint` root capture below. Every death in the
                    // accumulator was pushed AFTER its COW under this same
                    // `tree.write()`, so it is folded into the root this cycle
                    // makes durable — sealing all of them is exactly crash-complete
                    // and never premature. A concurrent open-bfg fold cannot race a
                    // post-checkpoint death into the drain because it would need
                    // this very guard. (The earlier `root_birth_lsn`-bounded
                    // attempt under-sealed: once a shard root forks private off the
                    // snapshot it is mutated IN PLACE without bumping its birth, so
                    // a later snapshot-pinned leaf death can sit ABOVE the root
                    // birth — `root_birth_lsn` is the fork lsn, not a fold
                    // watermark.) Unselected shards are left buffered: their root
                    // is not made durable, so their deaths are not in any durable
                    // root and are re-recorded by serial replay on crash.
                    let recs = volume.page_dead_list[s_idx].drain();
                    if !recs.is_empty() {
                        page_merged.extend_from_slice(&recs);
                        page_provenance.push((s_idx, recs));
                    }
                    per_volume.push(Some(guard.begin_checkpoint()));
                } else {
                    per_volume.push(None);
                }
            }
            l2p_checkpoints.push(per_volume);
            if !page_merged.is_empty() {
                drained_deadlists.push(DeadListDrainEntry {
                    vol: volume.clone(),
                    records: DrainRecords::Dead(page_merged),
                    old_head: volume
                        .page_dead_list_head_pid
                        .load(std::sync::atomic::Ordering::Acquire),
                    old_tail: volume
                        .page_dead_list_tail_pid
                        .load(std::sync::atomic::Ordering::Acquire),
                    kind: DeadListKind::Page,
                    page_provenance,
                });
            }
        }
        debug_assert!(guard_iter.next().is_none());
        // Release the per-shard tree guards (and the `&volumes` borrow their
        // type carries) now that the L2P sample is captured — the gate-window
        // reconciliation below mutates `volumes` to prune mid-cycle drops.
        drop(guard_iter);
        drop(l2p_guards);
        let l2p_walk_elapsed = l2p_walk_started.elapsed();
        trace.sample_l2p_walk_us = micros(l2p_walk_elapsed);
        // Refcount sample: threads-OFF keeps the legacy in-memory-only fold.
        // Threads-ON normally uses the bounded streaming fold: each shard
        // stages at most 4096 unique data pages under its fold lock, writes
        // that chunk after releasing the lock, then drops its payload Arcs.
        // The diagnostic A/B switch can select the legacy one-shot fold while
        // keeping the same BFG boundary. Meta-chain rewrite and the
        // checkpoint-wide sync still happen below.
        //
        // Per-shard `begin_checkpoint` is independent — each shard
        // touches only its own DeltaMap, page_table, and overlay. Allocation
        // and bounded write submission are internally synchronized by the
        // shared PageStore. Parallelizing across shards preserves IO depth
        // while each shard retains at most one streaming chunk.
        //
        // Runs WITHOUT the sample-phase `apply_gate.write()`. The
        // commit-side `RcShard::stage` invariants (delta_active +
        // delta_draining locks held across `lookup_base`,
        // transition-1/2 ordering, strict `>` replay-skip in
        // drainer-mode) are what carry the race; the gate was
        // redundant. See `refcount_drainer_layer_atomicity` memory
        // for the case invariants.
        //
        // Sparse over `selected.rc`: only spawn threads for selected
        // shards; unselected slots remain `None`. Failure handling
        // preserves the "first error wins, the rest are individually
        // aborted" semantics from the pre-partial code.
        // BFG-slot fold: threads-ON folds ONLY this sync's frozen Syncing
        // slot (`begin_checkpoint(bfg, wal_checkpoint)`), so rc durability
        // is keyed to the same per-BFG `checkpoint_lsn` prefix as L2P's
        // `drain_syncing_slot_into_trees`. threads-OFF (inline flush is the
        // sole drainer) folds every slot, mirroring `force_compact_l2p_buffers`.
        let bfg_threads_enabled = self.bfg_threads_enabled;
        let rc_checkpoint_streaming_enabled = self.rc_checkpoint_streaming_enabled;
        let rc_delta_run_shadow_enabled = self.rc_delta_run_shadow_enabled;
        let rc_delta_run_persist_enabled = self.rc_delta_run_persist_enabled;
        trace.enter_phase(
            self,
            bfg,
            kind,
            crate::metrics::CheckpointSyncPhase::SampleWaitRefcount,
        );
        let rc_drain_started = std::time::Instant::now();
        // v27 condense triggers, captured for the per-shard dispatch closure.
        let rc_condense_k = self.rc_condense_interval_cycles;
        let rc_overlay_per_shard_cap =
            (self.rc_segment_overlay_max_entries / self.refcount_shards.len().max(1)).max(1);
        let forced_flush = matches!(kind, crate::metrics::FlushKind::Forced);
        let rc_results: Vec<Option<Result<crate::refcount::shard::RcCheckpoint>>> =
            std::thread::scope(|scope| {
                let mut handles: Vec<(usize, std::thread::ScopedJoinHandle<_>)> = Vec::new();
                for (s_idx, shard) in self.refcount_shards.iter().enumerate() {
                    if selected.rc[s_idx] {
                        let h = scope.spawn(move || {
                            if bfg_threads_enabled
                                && rc_checkpoint_streaming_enabled
                                && rc_delta_run_persist_enabled
                            {
                                // v27: append a durable delta-run segment instead
                                // of folding the base array — UNLESS condense is
                                // due (K segments, overlay cap, or a Forced sweep
                                // with any un-condensed segment), in which case
                                // fold every segment + the frozen slot back into
                                // the base and empty the directory.
                                let segments = shard.rc.segment_count() as u64;
                                let condense_due = (rc_condense_k > 0
                                    && segments >= rc_condense_k)
                                    || shard.rc.segment_overlay_entries() as u64
                                        > rc_overlay_per_shard_cap as u64
                                    // Forced sweep drains a shard that has
                                    // segments but nothing new to append (an
                                    // append would no-op, leaving them un-drained).
                                    || (forced_flush
                                        && segments > 0
                                        && shard.rc.frozen_slot_is_empty(bfg));
                                if condense_due {
                                    shard.rc.condense(&[(bfg & (crate::bfg::BFG_SIZE as u64 - 1)) as usize])
                                } else {
                                    shard.rc.begin_checkpoint_streaming_persist(
                                        bfg,
                                        shard.routing.manifest_version(),
                                        s_idx as u32,
                                        wal_checkpoint,
                                    )
                                }
                            } else if bfg_threads_enabled && rc_checkpoint_streaming_enabled {
                                shard.rc.begin_checkpoint_streaming_shadow(
                                    bfg,
                                    shard.routing.manifest_version(),
                                    s_idx as u32,
                                    wal_checkpoint,
                                    rc_delta_run_shadow_enabled,
                                )
                            } else if bfg_threads_enabled {
                                shard.rc.begin_checkpoint(bfg)
                            } else {
                                shard.rc.begin_checkpoint_all_slots(false)
                            }
                        });
                        handles.push((s_idx, h));
                    }
                }
                let mut out: Vec<Option<Result<crate::refcount::shard::RcCheckpoint>>> =
                    (0..self.refcount_shards.len()).map(|_| None).collect();
                for (s_idx, h) in handles {
                    let result = h.join().unwrap_or_else(|p| std::panic::resume_unwind(p));
                    out[s_idx] = Some(result);
                }
                out
            });
        let rc_drain_elapsed = rc_drain_started.elapsed();
        trace.rc_drain_wall_us = micros(rc_drain_elapsed);
        let mut refcount_checkpoints: Vec<Option<crate::refcount::shard::RcCheckpoint>> =
            (0..self.refcount_shards.len()).map(|_| None).collect();
        let mut rc_stream_stats = crate::refcount::shard::RcStreamingWriteStats::default();
        let mut rc_delta_shadow_stats = crate::refcount::shard::RcDeltaShadowStats::default();
        let mut rc_fold_lock_wait_us = 0u64;
        let mut rc_fold_service_us = 0u64;
        let mut sample_err: Option<MetaDbError> = None;
        let mut tail_to_abort: Vec<(usize, crate::refcount::shard::RcCheckpoint)> = Vec::new();
        for (idx, result) in rc_results.into_iter().enumerate() {
            match result {
                None => continue,
                Some(Ok(ckpt)) => {
                    rc_stream_stats.merge(ckpt.streaming_write_stats());
                    rc_delta_shadow_stats.merge(ckpt.delta_shadow_stats());
                    rc_fold_lock_wait_us =
                        rc_fold_lock_wait_us.saturating_add(ckpt.fold_lock_wait_us());
                    rc_fold_service_us = rc_fold_service_us.saturating_add(ckpt.fold_service_us());
                    if sample_err.is_some() {
                        tail_to_abort.push((idx, ckpt));
                    } else {
                        refcount_checkpoints[idx] = Some(ckpt);
                    }
                }
                Some(Err(err)) => {
                    if sample_err.is_none() {
                        sample_err = Some(err);
                    }
                }
            }
        }
        for (idx, ckpt) in tail_to_abort {
            self.refcount_shards[idx]
                .rc
                .abort_checkpoint(ckpt, wal_checkpoint);
        }
        self.metrics.record_flush_rc_stream(
            rc_stream_stats.calls,
            rc_stream_stats.pages,
            rc_stream_stats.service_us,
            rc_stream_stats.max_chunk_us,
            rc_stream_stats.max_chunk_pages,
        );
        self.metrics.record_flush_rc_delta_shadow(
            rc_delta_shadow_stats.runs,
            rc_delta_shadow_stats.records,
            rc_delta_shadow_stats.pages,
            rc_delta_shadow_stats.payload_bytes,
            rc_delta_shadow_stats.encode_us,
            rc_delta_shadow_stats.max_encode_us,
            rc_delta_shadow_stats.verify_us,
            rc_delta_shadow_stats.max_verify_us,
            rc_delta_shadow_stats.errors,
        );
        self.metrics
            .record_flush_rc_fold_lock_wait(rc_fold_lock_wait_us);
        self.metrics
            .record_flush_rc_fold_service(rc_fold_service_us);
        trace.rc_fold_wait_sum_us = rc_fold_lock_wait_us;
        trace.rc_fold_service_sum_us = rc_fold_service_us;
        trace.rc_stream_write_sum_us = rc_stream_stats.service_us;
        trace.rc_stream_pages = rc_stream_stats.pages;
        // Streaming RC page writes completed before `io_started`, so count
        // their physical work without adding their service time to
        // `flush_io_us`.
        self.metrics.record_flush_io_pages(rc_stream_stats.pages);
        let sample_elapsed = sample_started.elapsed();
        trace.sample_wall_us = micros(sample_elapsed);
        self.metrics.record_flush_sample(kind, sample_elapsed);
        self.metrics.record_flush_sample_breakdown(
            lock_elapsed,
            l2p_walk_elapsed,
            rc_drain_elapsed,
        );
        // Sample workload size: L2P dirty pages snapshotted, refcount
        // delta entries drained, all refcount data pages staged, and the
        // freshly allocated subset.
        // Lets dashboards correlate sample wall-time growth with
        // workload-size growth.
        let l2p_dirty_pages: usize = l2p_checkpoints
            .iter()
            .flat_map(|cps| cps.iter())
            .filter_map(|cp| cp.as_ref())
            .map(|cp| cp.dirty_pages_count())
            .sum();
        let rc_drained_deltas: usize = refcount_checkpoints
            .iter()
            .filter_map(|c| c.as_ref())
            .map(|c| c.drained_deltas_count())
            .sum();
        let rc_fresh_pages: usize = refcount_checkpoints
            .iter()
            .filter_map(|c| c.as_ref())
            .map(|c| c.fresh_pages_count())
            .sum();
        let rc_staged_pages: usize = refcount_checkpoints
            .iter()
            .filter_map(|c| c.as_ref())
            .map(|c| c.data_pages_count())
            .sum();
        trace.dirty_l2p_shards = l2p_checkpoints
            .iter()
            .flat_map(|checkpoints| checkpoints.iter())
            .filter_map(|checkpoint| checkpoint.as_ref())
            .filter(|checkpoint| checkpoint.dirty_pages_count() != 0)
            .count() as u64;
        trace.dirty_rc_shards = refcount_checkpoints
            .iter()
            .filter_map(|checkpoint| checkpoint.as_ref())
            .filter(|checkpoint| {
                checkpoint.drained_deltas_count() != 0 || checkpoint.data_pages_count() != 0
            })
            .count() as u64;
        trace.l2p_dirty_pages = l2p_dirty_pages as u64;
        trace.rc_drained_deltas = rc_drained_deltas as u64;
        trace.rc_fresh_pages = rc_fresh_pages as u64;
        trace.rc_staged_pages = rc_staged_pages as u64;
        self.metrics.record_flush_sample_workload(
            l2p_dirty_pages,
            rc_drained_deltas,
            rc_fresh_pages,
            rc_staged_pages,
        );
        if let Some(err) = sample_err {
            // Roll back every L2P + RC checkpoint that this partial
            // sample successfully produced; the unselected `None`
            // slots are no-ops in the sparse aborters.
            self.abort_rc_checkpoints_sparse(refcount_checkpoints, wal_checkpoint);
            self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            return Err(err);
        }

        // Process each pending `take_snapshot`: capacity-probe, write its
        // SnapshotRoots page, build its `SnapshotEntry`, and return the
        // per-page-rc-shard set of root pids to force-incref. Runs after the
        // L2P/refcount sample and before the page-rc `begin_checkpoint` below,
        // so the increfs fold into this cycle's page-rc checkpoint and become
        // durable atomically with the manifest entry. On error, roll
        // back the L2P + refcount checkpoints (nothing page-rc folded yet).
        if let Err(err) =
            self.prepare_pending_snapshot_entries(bfg, &snapshot_roots, &snapshot_watermarks)
        {
            self.abort_rc_checkpoints_sparse(refcount_checkpoints, wal_checkpoint);
            self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            return Err(err);
        }

        // BFG: per-L2P-page refcounting was deleted, so there is no
        // page-rc checkpoint fold here. Snapshot roots stay referenced because
        // the source volume's tree keeps pointing at the shared roots (COW
        // preserves them on the next write); the snapshot's pages are reachable
        // from its `SnapshotEntry` roots in the manifest.

        // No sample-phase gate to drop. The remaining IO phase proceeds
        // straight from the sample: concurrent commits never blocked
        // on us, and L2P dirty page Arcs plus compact RC checkpoints make
        // the IO independent of further tree mutations. The
        // manifest commit + atomics bump below acquires
        // `apply_gate.write()` for its narrow window.
        let prefold_ticket = self.request_l2p_prefold(bfg);

        trace.enter_phase(self, bfg, kind, crate::metrics::CheckpointSyncPhase::Io);
        let io_started = std::time::Instant::now();
        let mut total_pages_written = 0usize;
        let mut sealed_pages = Vec::new();
        // Same sparse layout as `l2p_checkpoints` — `None` for
        // unselected shards (no IO for them this round).
        let mut flushed_l2p: Vec<Vec<Option<crate::paged::cache::FlushedSnapshot>>> =
            Vec::with_capacity(l2p_checkpoints.len());
        for checkpoints in &l2p_checkpoints {
            let mut flushed: Vec<Option<crate::paged::cache::FlushedSnapshot>> =
                Vec::with_capacity(checkpoints.len());
            for checkpoint_opt in checkpoints {
                match checkpoint_opt {
                    Some(checkpoint) => {
                        let seal_started = std::time::Instant::now();
                        match checkpoint.write_dirty_pages() {
                            Ok(pages) => {
                                self.metrics.record_flush_io_seal(seal_started.elapsed());
                                total_pages_written += pages.pages_count();
                                pages.append_sealed_pages(&mut sealed_pages);
                                flushed.push(Some(pages));
                            }
                            Err(err) => {
                                self.metrics.record_flush_io_seal(seal_started.elapsed());
                                let io_elapsed = io_started.elapsed();
                                trace.io_us = micros(io_elapsed);
                                trace.nonstream_write_pages = total_pages_written as u64;
                                self.metrics
                                    .record_flush_io(io_elapsed, total_pages_written);
                                self.metrics
                                    .record_flush_total(kind, flush_started.elapsed());
                                self.abort_rc_checkpoints_sparse(
                                    refcount_checkpoints,
                                    wal_checkpoint,
                                );
                                self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
                                return Err(err);
                            }
                        }
                    }
                    None => flushed.push(None),
                }
            }
            flushed_l2p.push(flushed);
        }
        // Threads-OFF refcount data pages still flow through this global write
        // batch. Threads-ON data pages were already written chunk-by-chunk and
        // append nothing; their meta-chain pages are built below and remain in
        // the final global batch. Only selected RC shards contribute.
        for ckpt_opt in &refcount_checkpoints {
            if let Some(ckpt) = ckpt_opt {
                let before = sealed_pages.len();
                ckpt.append_sealed_pages(&mut sealed_pages);
                // Streaming checkpoints already wrote their data pages in
                // bounded chunks and retain no payload Arcs. Cold checkpoints
                // still append here. `flush_io` begins after streaming writes,
                // so count only pages actually appended inside this timer.
                total_pages_written += sealed_pages.len() - before;
            }
        }
        // The L2P-page-rc fold can stage real data pages too; fold them into
        // the same write batch or freshly-allocated array data pages would be
        // lost on reopen.
        // Build the per-shard meta chains in memory (no IO) and fold
        // every sealed chain page into the global `sealed_pages` batch.
        // The shard's head meta page id is stable across rewrites
        // (`paged_meta::build_chain_pages` reuses `existing_chain[0]`),
        // so the manifest needs no per-flush update for refcount
        // roots. Sparse: only selected shards rebuild their chain;
        // unselected shards keep their existing on-disk chain.
        let mut rc_new_chains: Vec<Option<Vec<PageId>>> =
            (0..self.refcount_shards.len()).map(|_| None).collect();
        // Trailing continuation pages from the previous chain that we
        // must release **after** the new chain is durable and the
        // manifest has committed. `wal_checkpoint` is the durable LSN
        // used to stamp the deferred-free entry.
        let mut rc_to_free: Vec<PageId> = Vec::new();
        // (pid, sealed Arc) for the meta-chain pages of every shard;
        // re-published into the page cache after `write_sealed_page_runs`
        // succeeds so subsequent reads don't need to hit disk for the
        // new chain head/continuation pages.
        let mut rc_chain_cache_inserts: Vec<(PageId, Arc<crate::page::Page>)> = Vec::new();
        // v27: per-shard NEW segment-directory head staged in the manifest —
        // `Some` for shards whose directory was rewritten in place this cycle
        // (append or condense). It equals the shard's STABLE reserved head, so
        // staging it is an idempotent re-stamp (mirrors `refcount_shard_roots`).
        let mut rc_segment_dir_new_heads: Vec<Option<PageId>> =
            (0..self.refcount_shards.len()).map(|_| None).collect();
        // v27: per-shard NEW framing chain, cached on the shard after the commit
        // lands so the next in-place rewrite reuses the head + continuation pids.
        let mut rc_segment_dir_new_chains: Vec<Option<Vec<PageId>>> =
            (0..self.refcount_shards.len()).map(|_| None).collect();
        // v27: trailing directory-continuation framing pids freed after the commit
        // (the directory shrank). The STABLE head + re-listed data pages stay live.
        let mut rc_segment_dir_framing_free: Vec<PageId> = Vec::new();
        // v27 authoritative segment metrics accumulated across shards this flush.
        let mut rc_seg_appends = 0u64;
        let mut rc_seg_pages = 0u64;
        let mut rc_seg_bytes = 0u64;
        let mut rc_seg_dir_pages = 0u64;
        let mut rc_seg_overlay_max = 0u64;
        let mut rc_seg_condenses = 0u64;
        // v27: segment DATA pages a condense folded — freed after the commit
        // (the segments are no longer referenced by any manifest).
        let mut rc_condense_segment_free: Vec<PageId> = Vec::new();
        for (s_idx, ckpt_opt) in refcount_checkpoints.iter().enumerate() {
            let Some(ckpt) = ckpt_opt else { continue };
            let shard = &self.refcount_shards[s_idx];
            let rc_meta_started = std::time::Instant::now();
            match shard.rc.build_meta_chain(ckpt) {
                Ok((chain, chain_sealed, free_pids)) => {
                    let added = chain_sealed.len();
                    rc_chain_cache_inserts.extend(chain_sealed.iter().cloned());
                    sealed_pages.extend(chain_sealed);
                    total_pages_written += added;
                    rc_to_free.extend(free_pids);
                    self.metrics
                        .record_flush_io_rc_meta(rc_meta_started.elapsed());
                    rc_new_chains[s_idx] = Some(chain);
                }
                Err(err) => {
                    self.metrics
                        .record_flush_io_rc_meta(rc_meta_started.elapsed());
                    let io_elapsed = io_started.elapsed();
                    trace.io_us = micros(io_elapsed);
                    trace.nonstream_write_pages = total_pages_written as u64;
                    self.metrics
                        .record_flush_io(io_elapsed, total_pages_written);
                    self.metrics
                        .record_flush_total(kind, flush_started.elapsed());
                    self.abort_rc_checkpoints_sparse(refcount_checkpoints, wal_checkpoint);
                    self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
                    return Err(err);
                }
            }
            // v27: rewrite the shard's segment directory IN PLACE at its STABLE
            // reserved head for any persist checkpoint that changed it — an APPEND
            // (descriptors now include the new segment) or a CONDENSE that folded
            // segments (descriptors now empty, so the rewrite empties the directory
            // in place while keeping the head). The head + segment DATA pages are
            // already durable at the global `sync()` below, so a crash before the
            // manifest commit leaves them reachable from the manifest-recorded head.
            // Runs only on the Ok path (the base meta-chain Err arm already returned).
            let is_append = ckpt.segment().is_some();
            let is_condense_with_segments =
                ckpt.is_condense() && !ckpt.condense_segment_pages().is_empty();
            if is_append || is_condense_with_segments {
                if let Some(segment) = ckpt.segment() {
                    rc_seg_appends += 1;
                    rc_seg_pages += segment.data_pids.len() as u64;
                    rc_seg_bytes += (segment.data_pids.len() as u64)
                        .saturating_mul(crate::page::PAGE_PAYLOAD_SIZE as u64);
                    rc_seg_overlay_max =
                        rc_seg_overlay_max.max(shard.rc.segment_overlay_entries() as u64);
                } else {
                    rc_seg_condenses += 1;
                    rc_condense_segment_free.extend(ckpt.condense_segment_pages());
                }
                let existing = shard.rc.segment_dir_chain_snapshot();
                if existing.is_empty() {
                    // Reservation at open guarantees a stable head before any
                    // persist checkpoint. A missing head is an invariant violation
                    // (persist cannot be enabled mid-run); fail cleanly rather than
                    // allocate a fresh head that a crash would orphan.
                    self.metrics.record_flush_total(kind, flush_started.elapsed());
                    self.abort_rc_checkpoints_sparse(refcount_checkpoints, wal_checkpoint);
                    self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
                    return Err(MetaDbError::Corruption(format!(
                        "rc shard {s_idx} persist checkpoint with no reserved segment-directory head"
                    )));
                }
                let descriptors = shard.rc.segment_descriptors_snapshot();
                match crate::refcount::segment_dir::build_directory_chain(
                    &self.page_store,
                    &existing,
                    &descriptors,
                    wal_checkpoint,
                ) {
                    Ok((dir_chain, dir_sealed, dir_to_free)) => {
                        rc_seg_dir_pages += dir_sealed.len() as u64;
                        total_pages_written += dir_sealed.len();
                        sealed_pages.extend(dir_sealed);
                        rc_segment_dir_new_heads[s_idx] = Some(dir_chain[0]);
                        rc_segment_dir_new_chains[s_idx] = Some(dir_chain);
                        rc_segment_dir_framing_free.extend(dir_to_free);
                    }
                    Err(err) => {
                        // Pre-global-write: abort every RC checkpoint (frees this
                        // shard's segment data pages + un-publishes its overlay)
                        // and the L2P checkpoints. The already-built directory
                        // sealed pages are dropped unwritten (reclaimed by verify).
                        let io_elapsed = io_started.elapsed();
                        trace.io_us = micros(io_elapsed);
                        trace.nonstream_write_pages = total_pages_written as u64;
                        self.metrics
                            .record_flush_io(io_elapsed, total_pages_written);
                        self.metrics
                            .record_flush_total(kind, flush_started.elapsed());
                        self.abort_rc_checkpoints_sparse(refcount_checkpoints, wal_checkpoint);
                        self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
                        return Err(err);
                    }
                }
            }
        }
        self.metrics.record_flush_rc_segment(
            rc_seg_appends,
            rc_seg_pages,
            rc_seg_bytes,
            rc_seg_dir_pages,
            rc_seg_overlay_max,
            rc_seg_condenses,
        );
        // Build per-volume dead-list segments and fold them into the
        // same `sealed_pages` batch as L2P + RC. Allocate contiguous
        // runs per volume .
        // The plan vector is owned across the IO + manifest section
        // so failures can restore drained records back into the
        // buffers and free the allocated page runs.
        let mut dead_list_plans: Vec<DeadListSegmentPlan> =
            Vec::with_capacity(drained_deadlists.len());
        let mut dead_list_alloc_err: Option<MetaDbError> = None;
        for entry in &drained_deadlists {
            // Dead (PBA/page) and Live (per-clone livelist) records use
            // separate codecs but the same allocate-run → build → seal flow.
            let page_count = match &entry.records {
                DrainRecords::Dead(recs) => crate::deadlist::segment_pages_for(recs.len()),
                DrainRecords::Live(recs) => crate::livelist::segment_pages_for(recs.len()),
            };
            if page_count == 0 {
                continue;
            }
            match self.page_store.allocate_run(page_count) {
                Ok(start_pid) => {
                    let pages = match &entry.records {
                        DrainRecords::Dead(recs) => crate::deadlist::build_segment_pages(
                            start_pid,
                            recs,
                            entry.old_tail,
                            wal_checkpoint,
                        ),
                        DrainRecords::Live(recs) => crate::livelist::build_segment_pages(
                            start_pid,
                            recs,
                            entry.old_tail,
                            wal_checkpoint,
                        ),
                    };
                    for (pid, page) in pages {
                        sealed_pages.push((pid, Arc::new(page)));
                    }
                    total_pages_written += page_count;
                    dead_list_plans.push(DeadListSegmentPlan {
                        vol: entry.vol.clone(),
                        start_pid,
                        page_count: page_count as u32,
                        old_head: entry.old_head,
                        old_tail: entry.old_tail,
                        kind: entry.kind,
                    });
                }
                Err(err) => {
                    dead_list_alloc_err = Some(err);
                    break;
                }
            }
        }
        if let Some(err) = dead_list_alloc_err {
            self.rollback_dead_list_drain(&mut drained_deadlists, &dead_list_plans, wal_checkpoint);
            let io_elapsed = io_started.elapsed();
            trace.io_us = micros(io_elapsed);
            trace.nonstream_write_pages = total_pages_written as u64;
            self.metrics
                .record_flush_io(io_elapsed, total_pages_written);
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            self.abort_rc_checkpoints_sparse(refcount_checkpoints, wal_checkpoint);
            self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
            return Err(err);
        }

        // RC rollback boundary: once this call starts, the stable refcount
        // meta-chain head may be partially or fully overwritten with a table
        // that references this checkpoint's fresh data pages. Every error from
        // here through manifest publication must retain RC authority and rely
        // on the enclosing sync-poison/restart contract.
        let page_write_started = std::time::Instant::now();
        if let Err(err) = self.page_store.write_sealed_page_runs(sealed_pages) {
            self.metrics
                .record_flush_io_page_write(page_write_started.elapsed());
            self.rollback_dead_list_drain(&mut drained_deadlists, &dead_list_plans, wal_checkpoint);
            let io_elapsed = io_started.elapsed();
            trace.io_us = micros(io_elapsed);
            trace.nonstream_write_pages = total_pages_written as u64;
            self.metrics
                .record_flush_io(io_elapsed, total_pages_written);
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            self.retain_rc_checkpoints_after_global_write(refcount_checkpoints);
            self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
            return Err(err);
        }
        self.metrics
            .record_flush_io_page_write(page_write_started.elapsed());
        let sync_started = std::time::Instant::now();
        if let Err(err) = self.page_store.sync() {
            self.metrics.record_flush_io_sync(sync_started.elapsed());
            self.rollback_dead_list_drain(&mut drained_deadlists, &dead_list_plans, wal_checkpoint);
            let io_elapsed = io_started.elapsed();
            trace.io_us = micros(io_elapsed);
            trace.nonstream_write_pages = total_pages_written as u64;
            self.metrics
                .record_flush_io(io_elapsed, total_pages_written);
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            self.retain_rc_checkpoints_after_global_write(refcount_checkpoints);
            self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
            return Err(err);
        }
        self.metrics.record_flush_io_sync(sync_started.elapsed());
        let io_elapsed = io_started.elapsed();
        trace.io_us = micros(io_elapsed);
        trace.nonstream_write_pages = total_pages_written as u64;
        self.metrics
            .record_flush_io(io_elapsed, total_pages_written);
        // RC staged data pages are durable on disk now — drop the
        // dirty-staged overlay so rc reads go back to cache/disk. Until
        // this point the overlay is the ONLY eviction-proof copy of the
        // staged pages (the LRU insert in `stage_one_page` can be
        // evicted; a fresh page's disk backing is unwritten zeros). The
        // Any error below retains RC authority: the stable meta-chain head was
        // part of the global write and may already reference fresh data pages.
        // Rollback is no longer legal once write submission has started.
        for (s_idx, ckpt_opt) in refcount_checkpoints.iter().enumerate() {
            if let Some(ckpt) = ckpt_opt {
                self.refcount_shards[s_idx].rc.mark_staged_durable(ckpt);
            }
        }
        // Dead-list segment pages are now durable on disk. The
        // manifest still references the OLD tails until the commit
        // below; if it fails we restore drained records to the
        // buffer and free the just-allocated runs. leaves
        // the on-disk segment bytes as orphans if the manifest
        // never commits (page_store GC reconciliation).
        if let Err(err) = self
            .faults
            .inject(FaultPoint::DeadListPostSegWriteBeforeManifest)
        {
            self.rollback_dead_list_drain(&mut drained_deadlists, &dead_list_plans, wal_checkpoint);
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            self.retain_rc_checkpoints_after_global_write(refcount_checkpoints);
            self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
            return Err(err);
        }

        // RC meta-chain pages are now durable on disk. Re-publish the
        // sealed bytes into the shared page cache so subsequent reads
        // hit cache instead of re-reading the just-written pages.
        // Counterpart to what `paged_meta::write_chain` does inline.
        for (pid, page) in rc_chain_cache_inserts {
            self.page_cache.replace_or_insert(pid, page);
        }

        // Build the dead-list head/tail override map. Atomic stores
        // on each volume's `dead_list_*_pid` are deferred until the
        // manifest commit succeeds — failure paths between here and
        // commit only need `rollback_dead_list_drain` (restore buffer
        // + free pages), they do NOT need to revert atomics.
        // Split the plans into the two chains' override maps. Each map
        // is `vol_ord -> (new_head, new_tail)`: a fresh chain (old_head ==
        // NULL_PAGE) starts its head at the new segment; an existing
        // chain keeps its head and advances only the tail.
        let build_overrides = |kind: DeadListKind| -> HashMap<VolumeOrdinal, (PageId, PageId)> {
            dead_list_plans
                .iter()
                .filter(|plan| plan.kind == kind)
                .map(|plan| {
                    let new_head = if plan.old_head == crate::types::NULL_PAGE {
                        plan.start_pid
                    } else {
                        plan.old_head
                    };
                    (plan.vol.ord, (new_head, plan.start_pid))
                })
                .collect()
        };
        let mut dead_list_overrides = build_overrides(DeadListKind::Pba);
        let mut page_dead_list_overrides = build_overrides(DeadListKind::Page);
        // BFG: per-clone page-livelist anchor overrides.
        // `mut` because Bug 3's gate-window re-validation may drop a vol whose
        // tail a concurrent condenser re-anchored (see
        // `bail_condenser_raced_live_lists`).
        let mut page_live_list_overrides = build_overrides(DeadListKind::Live);
        // BFG: seal this cycle's pending snapshots' page
        // deadlists. Each take inherits the head volume's page-deadlist
        // chain (rewriting its override to (NULL,NULL) so the head resets)
        // and stamps the inherited tail onto its pending `SnapshotEntry`.
        // Returns the volumes whose in-memory atomics to reset post-commit.
        let page_seal_resets =
            self.seal_pending_snapshot_page_deadlists(bfg, &volumes, &mut page_dead_list_overrides);

        // Background reclaim/writeback is explicitly quiesced before taking
        // apply_gate. This matters on fixed PageBlockIo devices, where the
        // io_uring priority class is unavailable and background writes share
        // chunklet stripe locks with the final manifest page. Any wait belongs
        // here, outside both gate wait and gate hold.
        trace.enter_phase(
            self,
            bfg,
            kind,
            crate::metrics::CheckpointSyncPhase::PublishBarrier,
        );
        let publish_barrier_started = std::time::Instant::now();
        let publish_io_guard = self.page_store.checkpoint_publish_io_guard();
        let publish_barrier_elapsed = publish_barrier_started.elapsed();
        trace.publish_barrier_wait_us = micros(publish_barrier_elapsed);
        self.metrics
            .record_flush_publish_barrier_wait(publish_barrier_elapsed);
        trace.enter_phase(
            self,
            bfg,
            kind,
            crate::metrics::CheckpointSyncPhase::Manifest,
        );
        let manifest_started = std::time::Instant::now();
        // BFG gate-shrink: manifest publication uses two short
        // `apply_gate.write()` windows. This first window reconciles raced
        // lifecycle state and freezes the manifest generation. A writer
        // reservation then admits data-plane readers while catalog/free-list
        // pages are encoded, written, and synced. The second window publishes
        // one manifest slot and promotes the matching in-memory atomics.
        //
        // - Serializes the manifest commit against other
        //   `apply_gate.write()` holders (snapshot / range_delete /
        //   drop_volume / drop_snapshot / `async_reclaim` GC) so the
        //   on-disk `(manifest, atomics)` pair stays consistent and
        //   so readers never observe a transient half-bumped state.
        // - The frozen BFG roots and replay watermarks remain a strict prefix
        //   even when newer Open-BFG commits apply during the stage. Those
        //   commits allocate pages referenced only by a future manifest.
        // - Releases before IO-heavy post-manifest work (RC meta
        //   install, L2P checkpoint install via apply lanes, reclaim,
        //   WAL prune) — those paths take their own per-shard locks.
        let gate_started = std::time::Instant::now();
        let apply_guard = self.apply_gate.write();
        self.metrics.record_flush_gate_wait(gate_started.elapsed());
        let gate_hold_started = std::time::Instant::now();
        macro_rules! release_apply_guard {
            () => {{
                self.metrics
                    .record_flush_gate_hold(gate_hold_started.elapsed());
                drop(apply_guard);
            }};
        }
        // BFG — Bug 3 (+ lineage-GC extension): now that we hold the gate
        // (which excludes both the `LivelistCondenser` and the
        // `LineageGcWorker`), re-validate every drained chain whose committed
        // segments have a concurrent background freer against a re-anchor / head
        // advance that may have committed since our GATELESS head/tail sample.
        // A raced vol is dropped from THIS flush's commit before
        // `refresh_manifest_from_checkpoints` reads the override maps.
        self.bail_raced_chain_appends(
            &mut drained_deadlists,
            &mut dead_list_plans,
            &mut dead_list_overrides,
            &mut page_live_list_overrides,
            wal_checkpoint,
        );
        // BFG — drop-volume TOCTOU: the volume set is sampled GATELESS at
        // the top of this cycle (`volumes_snapshot()` above). A `drop_volume`
        // that committed between that sample and this `apply_gate.write()`
        // acquisition removed its entry from BOTH `self.volumes` and the
        // durable manifest. But our sampled `volumes` (and the volume-parallel
        // `l2p_checkpoints` / `flushed_l2p` / `selected.l2p`) still carry the
        // dropped volume, so `refresh_manifest_from_checkpoints` below — which
        // rebuilds `manifest.volumes` wholesale from this sample — would
        // RESURRECT its `VolumeEntry`. That ghost re-pins a descendant clone's
        // `parent_vol_ord`, so the next `drop_volume(parent)` is refused by the
        // descendant gate (the failure `meta_only_snapshot_lifecycle` hit), and
        // leaves a stale catalog entry generally. `drop_volume` holds
        // `apply_gate.write` for its whole body, so under this gate
        // `self.volumes` is the authoritative live set: prune every sampled
        // volume no longer live from the volume-parallel arrays in lockstep so
        // the committed manifest names exactly the survivors. A pruned volume's
        // already-sealed L2P pages (if any — an empty/idle drop has all-`None`
        // checkpoints) become orphans the next open reclaims, identical to the
        // crash-after-seal path.
        {
            let live_ords: std::collections::HashSet<VolumeOrdinal> =
                self.volumes.read().keys().copied().collect();
            if volumes.iter().any(|v| !live_ords.contains(&v.ord)) {
                let keep: Vec<bool> = volumes.iter().map(|v| live_ords.contains(&v.ord)).collect();
                let dropped: Vec<VolumeOrdinal> = volumes
                    .iter()
                    .filter(|v| !live_ords.contains(&v.ord))
                    .map(|v| v.ord)
                    .collect();
                tracing::debug!(
                    ?dropped,
                    "flush: pruning volumes dropped between gateless sample and manifest commit"
                );
                // Drop the same positions from every volume-parallel array.
                // `retain` visits in order, so a shared boolean mask keeps them
                // in lockstep even for the move-only checkpoint element types.
                fn retain_by_mask<T>(v: &mut Vec<T>, keep: &[bool]) {
                    debug_assert_eq!(v.len(), keep.len());
                    let mut it = keep.iter();
                    v.retain(|_| *it.next().unwrap_or(&false));
                }
                retain_by_mask(&mut volumes, &keep);
                retain_by_mask(&mut l2p_checkpoints, &keep);
                retain_by_mask(&mut flushed_l2p, &keep);
                retain_by_mask(&mut selected.l2p, &keep);
            }
        }
        let mut manifest_state = self.manifest_state.lock();
        let dedup_update = match self
            .prepare_dedup_manifest_update(&mut manifest_state.manifest, tree_generation)
        {
            Ok(update) => update,
            Err(err) => {
                let manifest_elapsed = manifest_started.elapsed();
                trace.manifest_us = micros(manifest_elapsed);
                self.metrics.record_flush_manifest(manifest_elapsed);
                self.metrics
                    .record_flush_total(kind, flush_started.elapsed());
                drop(manifest_state);
                release_apply_guard!();
                self.rollback_dead_list_drain(
                    &mut drained_deadlists,
                    &dead_list_plans,
                    wal_checkpoint,
                );
                self.retain_rc_checkpoints_after_global_write(refcount_checkpoints);
                self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
                return Err(err);
            }
        };
        if let Err(err) = self
            .faults
            .inject(FaultPoint::FlushPostLevelRewriteBeforeManifest)
        {
            let manifest_elapsed = manifest_started.elapsed();
            trace.manifest_us = micros(manifest_elapsed);
            self.metrics.record_flush_manifest(manifest_elapsed);
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            drop(manifest_state);
            release_apply_guard!();
            self.rollback_dead_list_drain(&mut drained_deadlists, &dead_list_plans, wal_checkpoint);
            self.retain_rc_checkpoints_after_global_write(refcount_checkpoints);
            self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
            return Err(err);
        }

        if let Err(err) = refresh_manifest_from_checkpoints(
            &mut manifest_state.manifest,
            &volumes,
            &l2p_checkpoints,
            &dead_list_overrides,
            &page_dead_list_overrides,
            &page_live_list_overrides,
        ) {
            let manifest_elapsed = manifest_started.elapsed();
            trace.manifest_us = micros(manifest_elapsed);
            self.metrics.record_flush_manifest(manifest_elapsed);
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            drop(manifest_state);
            release_apply_guard!();
            self.rollback_dead_list_drain(&mut drained_deadlists, &dead_list_plans, wal_checkpoint);
            self.retain_rc_checkpoints_after_global_write(refcount_checkpoints);
            self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
            return Err(err);
        }
        // Insert each processed `take_snapshot`'s `SnapshotEntry` into the
        // manifest about to be committed. The matching per-root incref was
        // already staged and folded into this cycle's page-rc checkpoint, so
        // the entry lands durable atomically with the incref + roots.
        // Infallible (a pure push); id-idempotent on this body's abort-retry.
        // The take is manifest-only — no lifecycle
        // journal record, so `lifecycle_replay_seq` below is unaffected.
        self.add_pending_snapshot_entries(&mut manifest_state.manifest, bfg);
        // Compute the new global checkpoint_lsn as
        // `min(per-shard last_flushed_lsn)`, treating every shard
        // selected this round as if its atomic were already at
        // `wal_checkpoint`. We can't store to the atomics yet —
        // that has to wait until after the manifest commit succeeds
        // — but the computation must reflect the durable state we're
        // ABOUT to commit. WAL prune downstream uses this value, so
        // it bounds what we can drop and must stay correct under
        // partial sampling.
        //
        // Full-sample / forced flushes have every selected[*] == true
        // and the result is simply `wal_checkpoint`, matching the
        // pre-partial behaviour.
        let new_checkpoint_lsn =
            self.compute_min_last_flushed_lsn_after(&volumes, &selected, wal_checkpoint);
        manifest_state.manifest.checkpoint_lsn = new_checkpoint_lsn;
        // Persist the buffer and
        // lifecycle replay watermarks alongside `checkpoint_lsn`. Both
        // are sampled here, inside `apply_gate.write()`, so the values
        // are exactly consistent with the page roots this checkpoint
        // commits — there are no in-flight applies that could advance
        // either watermark past what the manifest will record.
        //
        // The atomics are zero on the legacy `MetadbJournalMode::Wal`
        // path (onyx never publishes), so this is effectively a no-op
        // there: the manifest field stays 0 and recovery still falls
        // back to `checkpoint_lsn` semantics.
        manifest_state.manifest.last_processed_buffer_seq =
            manifest_state.manifest.last_processed_buffer_seq.max(
                self.buffer_applied_watermark
                    .load(std::sync::atomic::Ordering::Acquire),
            );
        let lifecycle_prune_seq = self
            .lifecycle_applied_watermark
            .load(std::sync::atomic::Ordering::Acquire);
        manifest_state.manifest.lifecycle_replay_seq = lifecycle_prune_seq;
        // Ring lifecycle journal (device path): stamp the head the ring will
        // advance to when we prune AFTER this commit is durable. Computing the
        // target here (non-mutating) and freeing the blocks only post-commit is
        // the crash-safe ordering — see `RingJournal::prune_target`. The file
        // journal returns `None` (segment journals persist no ring head), so
        // `journal_ring_head` stays 0 there. `do_lifecycle_prune` gates the
        // post-commit free: if we cannot compute the target here we must NOT
        // advance the in-memory head later, or it would run ahead of the durable
        // head (the reuse-below-durable-head hazard). A skipped prune only leaks
        // journal space and self-heals next checkpoint.
        let mut do_lifecycle_prune = false;
        if let Some(journal) = self.lifecycle_journal.as_ref() {
            match journal.lock().prune_target(lifecycle_prune_seq) {
                Ok(Some(head)) => {
                    manifest_state.manifest.journal_ring_head = head;
                    do_lifecycle_prune = true;
                }
                Ok(None) => do_lifecycle_prune = true, // file journal: segment delete
                Err(err) => tracing::warn!(
                    error = %err,
                    "run_sync_cycle_body: lifecycle prune_target failed; skipping prune this cycle"
                ),
            }
        }
        // BFG: persist `checkpoint_bfg = bfg` — the
        // BFG this sync cycle is actually syncing. The pages this
        // manifest commits carry exactly `bfg`'s data (the syncing slot
        // folded above + everything ≤ `bfg` already on disk), so
        // `checkpoint_bfg` must name `bfg`.
        //
        // Do NOT use `open_bfg() - 1`: under `bfg_threads_enabled` the
        // background `BfgQuiesceThread` can roll the Open BFG forward
        // while this body runs, so `open_bfg - 1` may be `bfg + 1` even
        // though this manifest only persisted `bfg`'s pages. Persisting
        // `bfg + 1` would make a re-open's `BfgStateMachine::new(bfg+1)`
        // skip `bfg+1`'s accounting. `bfg` keeps the manifest invariant
        // `checkpoint_bfg + 1 <= open_bfg` and stays consistent with the
        // `checkpoint_lsn` computed from this same BFG's `slot_max_lsn`.
        // (Threads-off is unaffected: there `open_bfg - 1 == bfg` because
        // nothing rolls between promote and mark_synced.)
        manifest_state.manifest.checkpoint_bfg = bfg;
        // per-shard durable-seq durable-seq rollout: persist per-shard durable_seq alongside
        // the global checkpoint_lsn. Same inputs as the min()
        // computation above, but expanded into per-shard arrays.
        // `assert_durable_seq_invariant` (called from `encode`) will
        // confirm `min(durable_seq[]) == checkpoint_lsn` before the
        // page is written to disk.
        if let Err(err) = refresh_manifest_durable_seq(
            &mut manifest_state.manifest,
            &volumes,
            &self.refcount_shards,
            &selected,
            wal_checkpoint,
        ) {
            let manifest_elapsed = manifest_started.elapsed();
            trace.manifest_us = micros(manifest_elapsed);
            self.metrics.record_flush_manifest(manifest_elapsed);
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            drop(manifest_state);
            release_apply_guard!();
            self.rollback_dead_list_drain(&mut drained_deadlists, &dead_list_plans, wal_checkpoint);
            self.retain_rc_checkpoints_after_global_write(refcount_checkpoints);
            self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
            return Err(err);
        }
        // v27: stage the delta-run segment-directory heads for shards that
        // appended a segment this cycle (others keep their prior committed head)
        // and lazily upgrade the manifest to v27 on the first persist commit
        // (byte-identical to v26 when persist is off). Done AFTER
        // `refresh_manifest_durable_seq` so the manifest is otherwise fully shaped
        // and BEFORE the store commit so the heads + version land on disk.
        if rc_delta_run_persist_enabled {
            if manifest_state.manifest.body_version
                < crate::manifest::DELTA_RUN_MANIFEST_BODY_VERSION
            {
                manifest_state.manifest.body_version =
                    crate::manifest::DELTA_RUN_MANIFEST_BODY_VERSION;
            }
            if manifest_state.manifest.refcount_delta_run_heads.len()
                != self.refcount_shards.len()
            {
                manifest_state.manifest.refcount_delta_run_heads =
                    vec![crate::types::NULL_PAGE; self.refcount_shards.len()].into_boxed_slice();
            }
            for (s_idx, new_head) in rc_segment_dir_new_heads.iter().enumerate() {
                if let Some(head) = new_head {
                    manifest_state.manifest.refcount_delta_run_heads[s_idx] = *head;
                }
            }
        }
        // v27 crash window: the segment DATA pages + the in-place directory rewrite
        // are durable (the global batch synced above) but the manifest commit
        // hasn't advanced `checkpoint_lsn`/`durable_seq`. This is POST-global-write,
        // so it RETAINS (sync-poison contract) exactly like the durable_seq /
        // BfgSync faults below. Because the directory head is STABLE (reserved in
        // the manifest before any append), a crash here leaves the just-written
        // segment REACHABLE from the still-durable head — `Db::open` re-reads it and
        // `condense_on_open` folds it into the base (restoring rc for standalone
        // metadb, and stamping `generation` so an onyx LV2 re-drive of the same
        // commits is dropped by the `page_lsn >= lsn` replay-skip: no double count).
        if rc_seg_appends > 0 || rc_seg_condenses > 0 {
            let point = if rc_seg_condenses > 0 {
                FaultPoint::RcCondensePostWriteBeforeManifest
            } else {
                FaultPoint::RcDeltaRunPostSegWriteBeforeManifest
            };
            if let Err(err) = self.faults.inject(point) {
                let manifest_elapsed = manifest_started.elapsed();
                trace.manifest_us = micros(manifest_elapsed);
                self.metrics.record_flush_manifest(manifest_elapsed);
                self.metrics
                    .record_flush_total(kind, flush_started.elapsed());
                drop(manifest_state);
                release_apply_guard!();
                self.rollback_dead_list_drain(&mut drained_deadlists, &dead_list_plans, wal_checkpoint);
                self.retain_rc_checkpoints_after_global_write(refcount_checkpoints);
                self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
                return Err(err);
            }
        }
        // Buffer-as-sole-journal lifecycle journal cutover retired the WAL BFG-sync
        // barrier: there is no WAL page cache to flush. The lifecycle
        // journal already fsyncs each record at append time, and
        // data-plane durability rides on onyx's LV2 buffer (which
        // restarts replay from its own state on crash).
        if let Err(err) = self.faults.inject(FaultPoint::BfgSyncMidway) {
            let manifest_elapsed = manifest_started.elapsed();
            trace.manifest_us = micros(manifest_elapsed);
            self.metrics.record_flush_manifest(manifest_elapsed);
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            drop(manifest_state);
            release_apply_guard!();
            self.rollback_dead_list_drain(&mut drained_deadlists, &dead_list_plans, wal_checkpoint);
            self.retain_rc_checkpoints_after_global_write(refcount_checkpoints);
            self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
            return Err(err);
        }
        let mut manifest = manifest_state.manifest.clone();

        // Freeze complete. Keep writer ordering reserved while allowing normal
        // data-plane readers to enter during catalog/free-bitmap IO. A lifecycle
        // writer that arrives here queues behind the reservation and therefore
        // cannot mutate the manifest generation we are preparing.
        self.metrics
            .record_flush_gate_hold(gate_hold_started.elapsed());
        let reservation = apply_guard.suspend();
        let manifest_stage_started = std::time::Instant::now();
        let manifest_stage_result = manifest_state.store.prepare_commit(&mut manifest);
        self.metrics
            .record_flush_manifest_stage(manifest_stage_started.elapsed());
        let prepared = match manifest_stage_result {
            Ok(prepared) => prepared,
            Err(err) => {
                let manifest_elapsed = manifest_started.elapsed();
                trace.manifest_us = micros(manifest_elapsed);
                self.metrics.record_flush_manifest(manifest_elapsed);
                self.metrics
                    .record_flush_total(kind, flush_started.elapsed());
                drop(manifest_state);
                drop(reservation);
                self.rollback_dead_list_drain(
                    &mut drained_deadlists,
                    &dead_list_plans,
                    wal_checkpoint,
                );
                self.retain_rc_checkpoints_after_global_write(refcount_checkpoints);
                self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
                return Err(err);
            }
        };

        // Stop new readers and wait only for readers that entered during the
        // stage. The final gate window writes one 4 KiB manifest slot, syncs it,
        // and promotes the matching in-memory atomics.
        let publish_gate_started = std::time::Instant::now();
        let apply_guard = reservation.resume();
        self.metrics
            .record_flush_gate_wait(publish_gate_started.elapsed());
        let gate_hold_started = std::time::Instant::now();
        macro_rules! release_apply_guard {
            () => {{
                self.metrics
                    .record_flush_gate_hold(gate_hold_started.elapsed());
                drop(apply_guard);
            }};
        }
        let manifest_publish_started = std::time::Instant::now();
        let manifest_publish_result = manifest_state
            .store
            .publish_prepared_deferred_cleanup(prepared);
        self.metrics
            .record_flush_manifest_publish(manifest_publish_started.elapsed());
        let manifest_cleanup = match manifest_publish_result {
            Ok(cleanup) => cleanup,
            Err(err) => {
                let manifest_elapsed = manifest_started.elapsed();
                trace.manifest_us = micros(manifest_elapsed);
                self.metrics.record_flush_manifest(manifest_elapsed);
                self.metrics
                    .record_flush_total(kind, flush_started.elapsed());
                drop(manifest_state);
                release_apply_guard!();
                self.rollback_dead_list_drain(
                    &mut drained_deadlists,
                    &dead_list_plans,
                    wal_checkpoint,
                );
                self.retain_rc_checkpoints_after_global_write(refcount_checkpoints);
                self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
                return Err(err);
            }
        };
        // `prepare_commit` fills device-derived fields on the staged clone
        // (catalog/free-list heads and page_high_water). Once publication is
        // durable, make that exact generation the cached manifest too.
        manifest_state.manifest = manifest;
        drop(publish_io_guard);
        let manifest_elapsed = manifest_started.elapsed();
        trace.manifest_us = micros(manifest_elapsed);
        self.metrics.record_flush_manifest(manifest_elapsed);

        // Manifest commit is durable. Promote per-volume dead-list
        // tail/head atomics so subsequent apply ops link new
        // segments off the new tail. This must happen AFTER
        // `manifest_state.store.commit` succeeds — if it failed and
        // we'd already stored, the in-memory atomic would race ahead
        // of the durable manifest and the next flush would link a
        // new segment to an orphaned tail.
        for plan in &dead_list_plans {
            let (head_atomic, tail_atomic) = match plan.kind {
                DeadListKind::Pba => (&plan.vol.dead_list_head_pid, &plan.vol.dead_list_tail_pid),
                DeadListKind::Page => (
                    &plan.vol.page_dead_list_head_pid,
                    &plan.vol.page_dead_list_tail_pid,
                ),
                DeadListKind::Live => (
                    &plan.vol.page_live_list_head_pid,
                    &plan.vol.page_live_list_tail_pid,
                ),
            };
            if plan.old_head == crate::types::NULL_PAGE {
                head_atomic.store(plan.start_pid, std::sync::atomic::Ordering::Release);
            }
            tail_atomic.store(plan.start_pid, std::sync::atomic::Ordering::Release);
        }
        // BFG: snapshots sealed this cycle transferred the
        // head's page-deadlist chain to themselves, so reset the head's
        // in-memory page anchors to NULL (the manifest VolumeEntry already
        // carries NULL via the (NULL,NULL) override). Runs AFTER the
        // promotion above so a snapshotted volume that also had a fresh
        // page segment this cycle ends at NULL, not the promoted tail.
        // Crash before this is recovered: reopen loads the atomics from
        // the durable (NULL) manifest anchors.
        for vol_ord in &page_seal_resets {
            if let Some(vol) = volumes.iter().find(|v| v.ord == *vol_ord) {
                vol.page_dead_list_head_pid.store(
                    crate::types::NULL_PAGE,
                    std::sync::atomic::Ordering::Release,
                );
                vol.page_dead_list_tail_pid.store(
                    crate::types::NULL_PAGE,
                    std::sync::atomic::Ordering::Release,
                );
            }
        }
        self.faults
            .inject(FaultPoint::DeadListPostManifestBeforeNextFlush)?;
        // Manifest commit is durable. Bump the per-shard
        // `last_flushed_lsn` for every shard we just committed —
        // future calls to `compute_min_last_flushed_lsn` will read
        // these values back. Release ordering pairs with the
        // Acquire load in `compute_min_last_flushed_lsn`.
        //
        // `fetch_max` (rather than `store`) preserves monotonicity
        // when `wal_checkpoint = slot_max_lsn(bfg) = 0` (no commits
        // in this Syncing slot): the shard's atomic stays at the
        // higher value a previous flush established. Matches the
        // `wal_checkpoint.max(prev)` projection in
        // `compute_min_last_flushed_lsn_after`.
        {
            use std::sync::atomic::Ordering;
            for (v_idx, vol) in volumes.iter().enumerate() {
                for (s_idx, shard) in vol.shards.iter().enumerate() {
                    if selected.l2p[v_idx][s_idx] {
                        shard
                            .last_flushed_lsn
                            .fetch_max(wal_checkpoint, Ordering::Release);
                    }
                }
            }
            for (s_idx, shard) in self.refcount_shards.iter().enumerate() {
                if selected.rc[s_idx] {
                    shard
                        .last_flushed_lsn
                        .fetch_max(wal_checkpoint, Ordering::Release);
                }
            }
        }
        {
            let mut unlogged = self.unlogged_pending_lsn.lock();
            if unlogged.is_some_and(|lsn| lsn <= wal_checkpoint) {
                *unlogged = None;
            }
        }
        // Post-commit snapshot task drain: the manifest with the new
        // `SnapshotEntry`s is durable now, so report success to queued
        // `take_snapshot` callers, warm the per-volume `SnapInfo` cache, and
        // dequeue the tasks.
        //
        // Must run inside the `apply_gate.write()` window (before
        // the drop below), NOT after it. The COW-kill reads each volume's live
        // snapshot watermarks from `snap_info_cache` (`snapshot_wms`), and page-rc removal
        // deleted the page-rc force-incref + `effective_rc > 1` floor that used
        // to pin a freshly-taken snapshot's root regardless of when the cache
        // warmed. If this warm ran AFTER the gate dropped, a backlogged commit
        // could acquire `apply_gate.read()` in the gap, COW the just-snapshotted
        // volume's head against a COLD cache (`youngest_snap_below == None`),
        // and recycle/retire the snapshot's still-referenced root → premature
        // free / snapshot corruption. Warming under the gate makes every apply
        // that runs after the drop observe the new snapshot. The warm takes only
        // the `snap_info_cache` / `pending_sync_tasks` leaf mutexes (same
        // `apply_gate -> snap_info_cache` order the read path uses — no
        // inversion), and the woken `take_snapshot` caller does not re-acquire
        // the gate.
        self.finish_pending_snapshots(bfg);

        // End of the narrow `apply_gate.write()` window. Everything
        // below this point (RC meta install, L2P checkpoint install
        // via apply lanes, reclaim, WAL prune) takes its own per-shard
        // locks and does not need the global gate.
        release_apply_guard!();

        let manifest_cleanup_started = std::time::Instant::now();
        manifest_state.store.cleanup_published(manifest_cleanup);
        self.metrics
            .record_flush_manifest_cleanup(manifest_cleanup_started.elapsed());

        // Lifecycle journal prune. The manifest carrying
        // `lifecycle_replay_seq`/`journal_ring_head` is now durable, so it is
        // safe to free the covered journal blocks/segments. Ring: advances the
        // in-memory head to exactly the head we stamped above (frees blocks for
        // reuse) — re-deriving from the same `lifecycle_prune_seq` yields that
        // same head. File: deletes segments wholly covered by the checkpoint
        // (the production caller the file journal never had until now — segments
        // used to accumulate unbounded). Gated on `do_lifecycle_prune`: if the
        // pre-commit `prune_target` failed we did NOT stamp a new head, so we
        // must leave the in-memory head where it is (never ahead of the durable
        // head). Best-effort: a prune failure only leaks journal space (the
        // durable head already names the boundary; a lagging in-memory head is
        // the conservative direction and self-heals next checkpoint), so it must
        // not fail an already-durable flush.
        if do_lifecycle_prune {
            if let Some(journal) = self.lifecycle_journal.as_ref() {
                if let Err(err) = journal.lock().prune(lifecycle_prune_seq) {
                    tracing::warn!(
                        checkpoint_seq = lifecycle_prune_seq,
                        error = %err,
                        "run_sync_cycle_body: lifecycle journal prune failed (space leak only)"
                    );
                }
            }
        }

        // Manifest is durable. Install refcount meta chains in memory
        // so subsequent `begin_checkpoint` sees the new chain when
        // computing `existing_chain` for paged_meta::build_chain_pages.
        // Direct inner.lock() — no race with `RcShard::stage` which
        // never reads `inner.meta_chain`. Sparse: only selected RC
        // shards get a new chain installed; unselected shards keep
        // their existing in-memory chain.
        for (s_idx, chain_opt) in rc_new_chains.into_iter().enumerate() {
            if let Some(new_chain) = chain_opt {
                self.refcount_shards[s_idx].rc.install_meta_chain(new_chain);
            }
        }
        // v27: the manifest recording the (stable) segment-directory heads is
        // durable. Cache each rewritten shard's new framing chain (keeps the
        // shard's `durable_segment_dir_head == chain[0]`) so the next in-place
        // rewrite reuses the head + continuation pids.
        for (s_idx, chain_opt) in rc_segment_dir_new_chains.into_iter().enumerate() {
            if let Some(chain) = chain_opt {
                self.refcount_shards[s_idx].rc.set_segment_dir_chain(chain);
            }
        }
        // v27 crash window: the manifest is durable with the stable heads, but the
        // trailing directory-continuation framing + folded segment DATA pages
        // haven't been freed. A fired fault here skips the frees (they orphan,
        // reclaimed on the next open); the durable state is correct.
        if rc_seg_appends > 0 || rc_seg_condenses > 0 {
            let point = if rc_seg_condenses > 0 {
                FaultPoint::RcCondensePostManifestBeforeFree
            } else {
                FaultPoint::RcDeltaRunPostManifestBeforeFree
            };
            self.faults.inject(point)?;
        }
        // v27: trailing continuation framing pids the in-place directory rewrite
        // shed (the directory shrank). The stable head + re-listed data pages stay
        // live, so only these trailing pids are freed.
        for pid in rc_segment_dir_framing_free {
            self.page_cache.invalidate(pid);
            if let Err(err) = self.page_store.free(pid, wal_checkpoint) {
                tracing::warn!(
                    page_id = pid,
                    error = %err,
                    "flush_with_gate: failed to free retired rc segment-dir continuation page"
                );
            }
        }
        // v27: a condense folded every segment into the base + emptied the
        // directory in place, so the segment DATA pages are unreferenced — free them.
        for pid in rc_condense_segment_free {
            self.page_cache.invalidate(pid);
            if let Err(err) = self.page_store.free(pid, wal_checkpoint) {
                tracing::warn!(
                    page_id = pid,
                    error = %err,
                    "flush_with_gate: failed to free condensed rc segment data page"
                );
            }
        }
        // Trailing continuation pages from the old chains can be
        // released now that the new chains are installed and durable.
        // Free order: invalidate the cache entry first so a concurrent
        // reader can't observe a stale page after the pid is recycled.
        // Errors are logged but don't fail the flush — the pages are
        // already orphaned in-memory and any leak gets reclaimed at
        // next open via the free-list walker. Matches the original
        // `paged_meta::write_chain` semantics.
        for pid in rc_to_free {
            self.page_cache.invalidate(pid);
            if let Err(err) = self.page_store.free(pid, wal_checkpoint) {
                tracing::warn!(
                    page_id = pid,
                    error = %err,
                    "flush_with_gate: failed to free retired rc-meta chain page"
                );
            }
        }
        // Drained deltas have been durably applied + meta chain rotated.
        // Drop checkpoints to release the snapshot bookkeeping; abort
        // is no longer reachable for these.
        drop(refcount_checkpoints);

        trace.enter_phase(
            self,
            bfg,
            kind,
            crate::metrics::CheckpointSyncPhase::Install,
        );
        let install_started = std::time::Instant::now();
        let mut install_receivers = Vec::new();
        for (volume, (checkpoints, flushed)) in volumes
            .iter()
            .zip(l2p_checkpoints.into_iter().zip(flushed_l2p.into_iter()))
        {
            for (sid, (checkpoint_opt, flushed_opt)) in
                checkpoints.into_iter().zip(flushed.into_iter()).enumerate()
            {
                // Sparse: unselected shards have `None` on both
                // sides (the L2P walk pushed None when not selected,
                // and the IO loop preserved that shape). Skip them
                // entirely — there's nothing to install when no
                // dirty pages were sealed for this shard.
                let (Some(checkpoint), Some(flushed)) = (checkpoint_opt, flushed_opt) else {
                    continue;
                };
                let flushed_pages = flushed.pages_count();
                let state = Arc::new(Mutex::new(CheckpointInstallState::new(
                    flushed,
                    flushed_pages,
                    checkpoint.retired_pages(),
                    checkpoint.private_pages(),
                )));
                let (tx, rx) = crossbeam_channel::bounded(1);
                enqueue_l2p_checkpoint_install_step(
                    volume.shards[sid].apply_lane.handle(),
                    volume.clone(),
                    sid,
                    state,
                    tx,
                );
                install_receivers.push(CheckpointInstallReceiver {
                    kind: "l2p",
                    vol_ord: Some(volume.ord),
                    shard: sid,
                    rx,
                });
            }
        }
        // Refcount install already happened above (direct inner.lock
        // after manifest commit); nothing to enqueue here.
        let mut checkpoint_frees = Vec::new();
        for receiver in install_receivers {
            let recv_started = std::time::Instant::now();
            match receiver.rx.recv() {
                Ok(Ok(mut frees)) => checkpoint_frees.append(&mut frees),
                Ok(Err(err)) => {
                    drop(manifest_state);
                    let install_elapsed = install_started.elapsed();
                    trace.install_us = micros(install_elapsed);
                    self.metrics.record_flush_install(install_elapsed);
                    self.metrics
                        .record_flush_total(kind, flush_started.elapsed());
                    return Err(err);
                }
                Err(_) => {
                    drop(manifest_state);
                    let install_elapsed = install_started.elapsed();
                    trace.install_us = micros(install_elapsed);
                    self.metrics.record_flush_install(install_elapsed);
                    self.metrics
                        .record_flush_total(kind, flush_started.elapsed());
                    return Err(MetaDbError::Corruption(
                        "checkpoint install lane worker exited before reporting".into(),
                    ));
                }
            }
            let recv_elapsed = recv_started.elapsed();
            let recv_us = micros(recv_elapsed);
            if recv_us >= FLUSH_INSTALL_STEP_WARN_US {
                tracing::warn!(
                    kind = receiver.kind,
                    vol_ord = receiver.vol_ord.map(u32::from).unwrap_or(u32::MAX),
                    shard = receiver.shard,
                    recv_us,
                    install_elapsed_us = micros(install_started.elapsed()),
                    "metadb: slow checkpoint install receiver wait"
                );
            }
        }
        self.finish_dedup_manifest_update(dedup_update, tree_generation)?;
        drop(manifest_state);
        let install_elapsed = install_started.elapsed();
        trace.install_us = micros(install_elapsed);
        self.metrics.record_flush_install(install_elapsed);

        if !checkpoint_frees.is_empty() {
            self.page_store
                .free_many(&checkpoint_frees, tree_generation)?;
            for pid in checkpoint_frees {
                self.page_cache.invalidate(pid);
            }
        }

        if let Some(ticket) = prefold_ticket {
            trace.enter_phase(
                self,
                bfg,
                kind,
                crate::metrics::CheckpointSyncPhase::PrefoldWait,
            );
            let successor = ticket.bfg;
            let prefold_wait_started = std::time::Instant::now();
            let prefold_result = self.wait_l2p_prefold(ticket);
            let prefold_wait_elapsed = prefold_wait_started.elapsed();
            trace.prefold_wait_us = micros(prefold_wait_elapsed);
            self.metrics.record_l2p_prefold_wait(prefold_wait_elapsed);
            match prefold_result {
                Ok(true) => tracing::debug!(
                    current_bfg = bfg,
                    prefold_bfg = successor,
                    "metadb: overlapped successor L2P fold with checkpoint IO"
                ),
                Ok(false) => {}
                Err(err) => {
                    self.metrics
                        .record_flush_total(kind, flush_started.elapsed());
                    return Err(err);
                }
            }
        }

        trace.enter_phase(
            self,
            bfg,
            kind,
            crate::metrics::CheckpointSyncPhase::Reclaim,
        );
        let reclaim_started = std::time::Instant::now();
        let deferred_before = self.page_store.deferred_free_len();
        if self.async_reclaim_active() {
            // Background worker owns the actual reclaim. Just
            // wake it; the wall-time recorded under
            // `flush_reclaim_max_us` becomes the notify cost (µs)
            // and the real reclaim cost is tracked under
            // `async_reclaim_cycle_max_us` in the worker. This
            // removes the ~35 % slice of `flush_total_max` that
            // the inline reclaim used to own — the dispatcher
            // can fire the next flush as soon as WAL prune
            // returns.
            self.notify_async_reclaim();
        } else {
            // `flush_io` intentionally excludes RC pages written before its
            // timer, but reclaim scaling is based on the checkpoint's total
            // write work and must retain the pre-streaming behavior.
            let streamed_pages = usize::try_from(rc_stream_stats.pages).unwrap_or(usize::MAX);
            let reclaim_budget = flush_reclaim_budget(
                deferred_before,
                total_pages_written.saturating_add(streamed_pages),
            );
            let reclaim_outcome = self.reclaim_freed_pages_budget(reclaim_budget)?;
            let deferred_after = self.page_store.deferred_free_len();
            let blocked = reclaim_budget
                .min(deferred_before)
                .saturating_sub(reclaim_outcome.selected);
            self.metrics.record_flush_reclaim_pages(
                reclaim_budget,
                reclaim_outcome.selected,
                reclaim_outcome.reclaimed.len(),
                blocked,
            );
            if deferred_before >= 1_048_576
                && reclaim_outcome.selected < (reclaim_budget / 4).min(deferred_before)
            {
                tracing::debug!(
                    deferred_before,
                    deferred_after,
                    reclaim_budget,
                    selected = reclaim_outcome.selected,
                    reclaimed = reclaim_outcome.reclaimed.len(),
                    safe_below = reclaim_outcome.safe_below,
                    "metadb: reclaim selected few pages under backlog"
                );
            }
        }
        let reclaim_elapsed = reclaim_started.elapsed();
        trace.reclaim_us = micros(reclaim_elapsed);
        self.metrics.record_flush_reclaim(reclaim_elapsed);
        self.metrics
            .record_flush_total(kind, flush_started.elapsed());
        trace.enter_phase(
            self,
            bfg,
            kind,
            crate::metrics::CheckpointSyncPhase::Complete,
        );
        Ok(())
    }

    /// Sparse-checkpoint variant of the L2P abort path: walks the
    /// `Option<Checkpoint>` matrix and only aborts shards that
    /// actually produced a checkpoint this round. Unselected slots
    /// (`None`) are no-ops — those shards never began a sample, so
    /// there's nothing to roll back.
    fn abort_checkpoints_sparse(
        &self,
        volumes: &[Arc<Volume>],
        l2p_checkpoints: &[Vec<Option<crate::paged::tree::Checkpoint>>],
    ) {
        for (volume, checkpoints) in volumes.iter().zip(l2p_checkpoints.iter()) {
            for (shard, checkpoint_opt) in volume.shards.iter().zip(checkpoints.iter()) {
                if let Some(checkpoint) = checkpoint_opt {
                    shard.tree.write().abort_checkpoint(checkpoint);
                }
            }
        }
    }

    /// BFG — Bug 3 (+ lineage-GC extension): under `apply_gate.write()`, drop
    /// any drained chain whose committed segments have a concurrent background
    /// freer and whose head/tail anchor moved since this flush's GATELESS
    /// sample. Two such chains exist:
    ///
    /// - `Live` (per-clone page-livelist): the [`LivelistCondenser`] samples the
    ///   live tail gatelessly, then under the SAME gate this flush now holds it
    ///   commits a re-anchored (head=tail=condensed) manifest, promotes the
    ///   in-memory anchors, drops the gate, and frees the OLD chain.
    /// - `Pba` (PBA dead-list): the [`LineageGcWorker`]'s
    ///   `advance_head_pid_durable` walks the chain under the gate, commits an
    ///   advanced (or emptied) head, promotes the anchors, drops the gate, and
    ///   `free_many`s the reclaimed segment(s) — INCLUDING, when it empties the
    ///   chain, the `old_tail` this flush captured.
    ///
    /// In both cases, if the background op won the gate first, committing our
    /// override would link `S_flush.prev = old_tail (now freed)` — a dangling
    /// chain that walks into a `Free` page (`wrong page_type Free`) — and/or
    /// promote a stale `old_head` (a freed page) into the manifest, wedging the
    /// head/tail bookkeeping. This mirrors the condenser's own symmetric bail
    /// (`livelist_condense.rs`: `cur_tail != tail0`): whichever side loses the
    /// gate race detects the other's promotion and backs its append out.
    ///
    /// For each raced plan: restore its records to the accumulator (a later
    /// flush re-drains them off the advanced/condensed anchor), free this
    /// flush's now-orphan `S_flush` segment, drop its override (so
    /// [`refresh_manifest_from_checkpoints`] keeps the background op's committed
    /// anchor via the live-atomic fallback) and drop its plan (so the
    /// post-commit promotion loop leaves the atomic at that anchor).
    ///
    /// `Page` (L2P page dead-list) is NOT checked: its committed segments are
    /// transferred to snapshots (never freed) under the same gate, so a
    /// gateless-sampled append is always safe to commit. The check is
    /// conservative — any head/tail change defers the append to a later flush,
    /// which is harmless — so it never needs to prove the background op is the
    /// *only* possible writer. Must run AFTER `apply_gate.write()` and BEFORE
    /// `refresh_manifest_from_checkpoints`.
    fn bail_raced_chain_appends(
        &self,
        drained: &mut Vec<DeadListDrainEntry>,
        plans: &mut Vec<DeadListSegmentPlan>,
        dead_list_overrides: &mut HashMap<VolumeOrdinal, (PageId, PageId)>,
        page_live_list_overrides: &mut HashMap<VolumeOrdinal, (PageId, PageId)>,
        free_lsn: Lsn,
    ) {
        use std::sync::atomic::Ordering;
        // A plan is stale iff its chain's live head OR tail anchor moved since
        // the gateless drain. Tail moves catch a full reclaim (chain emptied) /
        // condenser re-anchor; head moves catch a partial lineage-GC advance
        // (head advanced past the freed oldest segment, tail unchanged) whose
        // override would otherwise promote the freed `old_head`.
        let raced: Vec<(VolumeOrdinal, DeadListKind)> = plans
            .iter()
            .filter(|p| match p.kind {
                DeadListKind::Live => {
                    p.vol.page_live_list_tail_pid.load(Ordering::Acquire) != p.old_tail
                        || p.vol.page_live_list_head_pid.load(Ordering::Acquire) != p.old_head
                }
                DeadListKind::Pba => {
                    p.vol.dead_list_tail_pid.load(Ordering::Acquire) != p.old_tail
                        || p.vol.dead_list_head_pid.load(Ordering::Acquire) != p.old_head
                }
                DeadListKind::Page => false,
            })
            .map(|p| (p.vol.ord, p.kind))
            .collect();
        if raced.is_empty() {
            return;
        }
        for (vol_ord, kind) in raced {
            // Keep the background op's committed anchor: dropping the override
            // makes `refresh_manifest_from_checkpoints` fall back to the (now
            // advanced/condensed) live atomic for this vol.
            match kind {
                DeadListKind::Live => page_live_list_overrides.remove(&vol_ord),
                DeadListKind::Pba => dead_list_overrides.remove(&vol_ord),
                DeadListKind::Page => None,
            };
            // Free this flush's now-orphan new segment and drop its plan so the
            // post-commit promotion loop never stores its stale tail.
            // `free_idempotent` (vs `free`) is defensive: the run was just
            // allocated + synced this flush so a plain free would suffice, but
            // idempotence keeps a second pass a no-op rather than a hard error.
            plans.retain(|p| {
                let drop_it = p.kind == kind && p.vol.ord == vol_ord;
                if drop_it {
                    for i in 0..p.page_count as u64 {
                        if let Err(err) = self.page_store.free_idempotent(p.start_pid + i, free_lsn)
                        {
                            tracing::warn!(
                                page_id = p.start_pid + i,
                                vol_ord = u32::from(vol_ord),
                                kind = ?kind,
                                error = %err,
                                "flush_with_gate: failed to free background-raced chain segment"
                            );
                        }
                    }
                }
                !drop_it
            });
            // Restore the drained records to the accumulator and remove the
            // entry so no later abort path re-processes it. `restore_front`
            // pushes them back at the head so ordering is preserved for the
            // next flush's drain.
            if let Some(pos) = drained
                .iter()
                .position(|e| e.kind == kind && e.vol.ord == vol_ord)
            {
                let mut entry = drained.swap_remove(pos);
                match (kind, &mut entry.records) {
                    (DeadListKind::Live, DrainRecords::Live(recs)) => {
                        let records = std::mem::take(recs);
                        if !records.is_empty() {
                            entry.vol.page_live_list.restore_front(records);
                        }
                    }
                    (DeadListKind::Pba, DrainRecords::Dead(recs)) => {
                        let records = std::mem::take(recs);
                        if !records.is_empty() {
                            entry.vol.dead_list.restore_front(records);
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    /// Restore drained dead-list records back to their volume buffers
    /// and free the allocated segment runs. Used by every failure path
    /// in [`flush_with_gate`] between the drain step and a successful
    /// manifest commit. Best-effort: `page_store.free` errors are
    /// logged but not propagated (the failing flush's error is already
    /// the primary report). The drained `entry.records` is moved out
    /// via `mem::take` so a second rollback call is a no-op.
    fn rollback_dead_list_drain(
        &self,
        drained: &mut [DeadListDrainEntry],
        plans: &[DeadListSegmentPlan],
        free_lsn: Lsn,
    ) {
        for entry in drained.iter_mut() {
            // Restore each drained chain's records back into its buffer so a
            // later flush re-attempts them. Records are moved out via
            // `mem::take` so a second rollback call is a no-op.
            match entry.kind {
                DeadListKind::Pba => {
                    if let DrainRecords::Dead(recs) = &mut entry.records {
                        let records = std::mem::take(recs);
                        if !records.is_empty() {
                            entry.vol.dead_list.restore_front(records);
                        }
                    }
                }
                DeadListKind::Page => {
                    // H1: restore each sub-vec to ITS shard accumulator —
                    // `page_provenance` is authoritative. A flat restore into
                    // shard 0 would permanently mis-bound a non-shard-0 death
                    // (shard 0's next-cycle `root_birth_lsn` could exclude it).
                    // The merged `records` is just the concatenation; drop it.
                    if let DrainRecords::Dead(recs) = &mut entry.records {
                        let _ = std::mem::take(recs);
                    }
                    for (s_idx, recs) in std::mem::take(&mut entry.page_provenance) {
                        if !recs.is_empty() {
                            entry.vol.page_dead_list[s_idx].restore_front(recs);
                        }
                    }
                }
                DeadListKind::Live => {
                    if let DrainRecords::Live(recs) = &mut entry.records {
                        let records = std::mem::take(recs);
                        if !records.is_empty() {
                            entry.vol.page_live_list.restore_front(records);
                        }
                    }
                }
            }
        }
        for plan in plans {
            for i in 0..plan.page_count as u64 {
                if let Err(err) = self.page_store.free(plan.start_pid + i, free_lsn) {
                    tracing::warn!(
                        page_id = plan.start_pid + i,
                        vol_ord = u32::from(plan.vol.ord),
                        error = %err,
                        "flush_with_gate: failed to free rolled-back dead-list page"
                    );
                }
            }
        }
    }

    /// Sparse-checkpoint variant of the RC abort path. Same shape
    /// as the L2P version: walk the `Option<RcCheckpoint>` slice
    /// indexed by shard, abort only `Some(...)` entries.
    fn abort_rc_checkpoints_sparse(
        &self,
        checkpoints: Vec<Option<crate::refcount::shard::RcCheckpoint>>,
        free_lsn: Lsn,
    ) {
        for (s_idx, ckpt_opt) in checkpoints.into_iter().enumerate() {
            if let Some(ckpt) = ckpt_opt {
                self.refcount_shards[s_idx]
                    .rc
                    .abort_checkpoint(ckpt, free_lsn);
            }
        }
    }

    /// Once the global page write has been submitted, the stable RC meta-chain
    /// head may already contain the new page table. Rolling back fresh RC pages
    /// after that point can turn a reachable page into `Free` (especially via
    /// async reclaim) and make reopen follow a dangling chain. Preserve the
    /// array/page-table authority and let the enclosing sync error poison the
    /// process; restart reopens whichever sealed generation reached disk.
    fn retain_rc_checkpoints_after_global_write(
        &self,
        checkpoints: Vec<Option<crate::refcount::shard::RcCheckpoint>>,
    ) {
        drop(checkpoints);
    }

    /// Project what `compute_min_last_flushed_lsn` will return after
    /// this round's atomic stores land — substitute `wal_checkpoint`
    /// for selected shards (which we're about to commit), keep the
    /// current atomic for unselected shards. Lets us compute
    /// `manifest.checkpoint_lsn` correctly while still holding the
    /// manifest lock, before storing the new atomics (the stores
    /// can't happen before the commit because a commit failure
    /// would leave them lying).
    ///
    /// gate-shrink note: with `wal_checkpoint =
    /// bfg.slot_max_lsn(bfg)`, a flush whose Syncing slot had no
    /// commits gets `wal_checkpoint = 0`. The selected-shard
    /// projection therefore takes `max(wal_checkpoint, prev_atomic)`
    /// so the per-shard contribution to `min_last_flushed_lsn` never
    /// regresses below the durability previous flushes already
    /// established. Recovery on the next open relies on
    /// `checkpoint_lsn` being monotonic.
    fn compute_min_last_flushed_lsn_after(
        &self,
        volumes: &[Arc<Volume>],
        selected: &SelectedShards,
        wal_checkpoint: Lsn,
    ) -> Lsn {
        use std::sync::atomic::Ordering;
        let mut min_lsn = Lsn::MAX;
        for (v_idx, vol) in volumes.iter().enumerate() {
            for (s_idx, shard) in vol.shards.iter().enumerate() {
                let prev = shard.last_flushed_lsn.load(Ordering::Acquire);
                let candidate = if selected.l2p[v_idx][s_idx] {
                    wal_checkpoint.max(prev)
                } else {
                    prev
                };
                if candidate < min_lsn {
                    min_lsn = candidate;
                }
                // B2 buffer-compaction term: any uncompacted entry
                // in this shard's buffer represents a committed LSN
                // not yet durable in the tree. Crash recovery will
                // rebuild it from WAL, so `checkpoint_lsn` must not
                // advance past `buffer.compacted_lsn`.
                if shard.use_buffer {
                    let buf_lsn = shard.l2p_buffer.compacted_lsn();
                    if buf_lsn < min_lsn {
                        min_lsn = buf_lsn;
                    }
                }
            }
        }
        for (s_idx, shard) in self.refcount_shards.iter().enumerate() {
            let prev = shard.last_flushed_lsn.load(Ordering::Acquire);
            let candidate = if selected.rc[s_idx] {
                wal_checkpoint.max(prev)
            } else {
                prev
            };
            if candidate < min_lsn {
                min_lsn = candidate;
            }
            // No rc buffer-term: the per-BFG-slot fold makes a selected rc
            // shard durable to `wal_checkpoint` (and an unselected one stays
            // at `prev`), exactly like a non-buffered L2P shard — so
            // `candidate` already bounds rc correctly. See `refcount::shard`.
        }
        if min_lsn == Lsn::MAX { 0 } else { min_lsn }
    }

    /// Drain everything currently safe to physically free (i.e. tagged
    /// at an epoch below every active reader pin) and invalidate the
    /// shared page cache for each reclaimed pid.
    ///
    /// Cache invalidation matters: during the deferred window a stale
    /// reader that fell through to disk may have re-populated the cache
    /// with the page's pre-free bytes. Without this step a subsequent
    /// allocator that hands the pid back out for a different page would
    /// hit the stale cached entry instead of fetching the new content.
    pub(crate) fn reclaim_freed_pages(&self) -> Result<()> {
        let outcome = self.page_store.try_reclaim()?;
        self.invalidate_reclaimed_pages(&outcome.reclaimed);
        Ok(())
    }

    pub(crate) fn reclaim_freed_pages_budget(
        &self,
        max_pages: usize,
    ) -> Result<crate::page_store::ReclaimOutcome> {
        let outcome = self.page_store.try_reclaim_limit(max_pages)?;
        self.invalidate_reclaimed_pages(&outcome.reclaimed);
        Ok(outcome)
    }

    fn invalidate_reclaimed_pages(&self, reclaimed: &[crate::types::PageId]) {
        for pid in reclaimed {
            self.page_cache.invalidate(*pid);
        }
    }
}
