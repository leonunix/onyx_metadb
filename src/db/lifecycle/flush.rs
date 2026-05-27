use super::manifest_refresh::{refresh_manifest_durable_seq, refresh_manifest_from_checkpoints};
use super::*;

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
    fn select_shards_for_flush(&self, volumes: &[Arc<Volume>], force_all: bool) -> SelectedShards {
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
        for (s_idx, shard) in self.refcount_shards.iter().enumerate() {
            let dirty = shard.rc.pending_delta_count();
            order.push((Kind::Rc(s_idx), dirty));
        }

        if order.is_empty() {
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

        SelectedShards { l2p, rc }
    }

    /// Best-effort checkpoint for background maintenance. If commits are
    /// currently applying, this returns `Ok(false)` without setting the
    /// apply gate's writer-pending bit, so foreground commit readers keep
    /// flowing and the caller can retry on the next interval.
    pub fn try_flush(&self) -> Result<bool> {
        self.flush_with_gate(crate::metrics::FlushKind::Steady)
    }

    /// ZFS-TXG-clone Phase 4 Step 8: `flush_with_gate` is now a thin
    /// shell. The actual per-TXG sync work lives in
    /// [`Db::run_sync_cycle`]. Two modes:
    ///
    /// - **`txg_threads_enabled = true`** (production default): force
    ///   the [`crate::db::txg_quiesce::TxgQuiesceThread`] to roll the
    ///   current Open TXG immediately, then park on
    ///   [`crate::txg::TxgStateMachine::wait_until_synced`] until the
    ///   [`crate::db::txg_sync::TxgSyncThread`] has finished
    ///   `run_sync_cycle` for that TXG (which is what
    ///   `start_txg_threads` wired the worker's `sync_work` callback
    ///   to).
    /// - **`txg_threads_enabled = false`**: the worker threads are not
    ///   spawned. `flush_with_gate` drives the TXG state machine and
    ///   `run_sync_cycle` synchronously on the caller thread,
    ///   recreating the legacy stop-the-world flush semantics.
    ///
    /// Returns `Ok(false)` only on the threads-off `Steady` path when
    /// `apply_gate.try_write()` does not immediately succeed (the same
    /// best-effort behaviour `try_flush` had pre-Step-8).
    pub(in crate::db) fn flush_with_gate(
        &self,
        kind: crate::metrics::FlushKind,
    ) -> Result<bool> {
        if self.txg_threads_enabled {
            // Threads-on: hand off to the sync thread and wait.
            // `signal_force` only sets the quiesce notifier flag; the
            // quiesce worker then calls `roll_to_quiescing`, which
            // does its own inflight-drain wait. No
            // `apply_gate.try_write` race here — the threaded path
            // always runs to completion.
            let target = self.txg.open_txg();
            self.txg_quiesce_notifier.signal_force(target);
            self.txg.wait_until_synced(target);
            self.metrics
                .record_flush_attempt(kind);
            // Wall-time accounting kept consistent with the inline
            // path: every threaded `flush()` records as a completed
            // forced-flush (Steady never lands here because Steady is
            // only emitted by `try_flush`, which routes the threads-on
            // side through `wait_until_synced` regardless).
            self.metrics
                .record_flush_total(kind, std::time::Duration::from_micros(0));
            return Ok(true);
        }
        // Threads-off: drive the TXG state machine ourselves so the
        // sync metadata bookkeeping (slot.max_lsn, open_txg advance,
        // checkpoint_txg / wait_until_synced cv release) stays in
        // lock-step with what the threaded path would have done.
        // `roll_to_quiescing` waits for any in-flight commits' txg
        // guards to drop, mirroring the apply-gate barrier the body
        // would have taken anyway. After `mark_synced` returns,
        // `wait_until_synced(target)` (used by any concurrent
        // threads-off caller) wakes up.
        //
        // Steady-kind try_flush keeps best-effort semantics: skip the
        // roll entirely when `apply_gate.try_write()` fails. Forced
        // kind always rolls.
        let blocking_gate = matches!(kind, crate::metrics::FlushKind::Forced);
        if !blocking_gate
            && let None = self.apply_gate.try_write()
        {
            self.metrics.record_flush_attempt(kind);
            self.metrics
                .record_flush_total(kind, std::time::Duration::ZERO);
            return Ok(false);
        }
        // We don't actually hold `apply_gate.write()` here — the
        // run_sync_cycle body re-acquires it for the sample phase.
        // The try-write above is purely a "should we even bother
        // rolling" gate matching pre-Step-8 try_flush behaviour.
        let target = self.txg.roll_to_quiescing();
        // `roll_to_quiescing` is idempotent under shutdown (returns
        // current open without advancing). Re-check whether the slot
        // actually moved to Quiescing before promoting; mismatch
        // means shutdown raced and we should skip.
        if self.txg.snapshot().quiescing_txg != Some(target) {
            return Ok(false);
        }
        self.txg.promote_to_syncing(target);
        let result = self.run_sync_cycle(target, kind);
        match result {
            Ok(()) => {
                self.txg.mark_synced(target);
                Ok(true)
            }
            Err(err) => {
                // Sync work failed; leave the slot in Syncing so a
                // subsequent flush can retry (matches the threaded
                // path's `failed_sync_leaves_txg_in_syncing_state`
                // semantics). The next inline `flush_with_gate` will
                // hit `quiescing_txg != Some(...)` and short-circuit
                // — recovery from a failed inline flush requires a
                // process restart, same as the pre-Step-8 behaviour
                // (a sample-phase err there would have left the
                // shards' deferred RC apply state inconsistent).
                Err(err)
            }
        }
    }

    /// Per-TXG sync work: drain L2P buffers, sample + IO + manifest
    /// commit. Extracted from `flush_with_gate`'s pre-Step-8 body so
    /// the `TxgSyncThread`'s `sync_work` callback can drive it via
    /// `Weak<Db>` upgrade.
    ///
    /// `txg` identifies the slot the caller has already promoted to
    /// Syncing in the state machine.
    ///
    /// The body NO LONGER holds `apply_gate.write()` across the sample
    /// phase. Every lifecycle op (`take_snapshot`, `drop_snapshot`,
    /// `drop_volume`, `clone_volume`, `range_delete`, `create_volume`)
    /// now drives a forced TXG sync at entry, so by the time it does
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
    /// `txg` is threaded through to the body. `wal_checkpoint` is now
    /// read from `self.txg.slot_max_lsn(txg)` (was
    /// `last_applied_lsn`). Reasons:
    ///
    /// - `slot_max_lsn(txg)` is **frozen** for the Syncing slot:
    ///   `promote_to_syncing` precondition is `inflight == 0` and
    ///   `record_lsn` cannot fire on a non-Open slot. By contrast
    ///   `last_applied_lsn` can advance during the gateless sample if
    ///   a commit stamped to a later TXG completes its apply.
    /// - `slot_max_lsn(txg)` precisely delineates "the LSNs in THIS
    ///   TXG"; any commit stamped to TXG > `txg` belongs to a future
    ///   sync, not this one.
    ///
    /// Post-recovery: `apply_replay_batch` does NOT go through
    /// `TxgGuard::record_lsn`, so without intervention
    /// `slot_max_lsn(open_txg)` would be 0 after replay.
    /// [`Db::open_with_config_and_faults`] stamps the open slot with
    /// the post-replay `last_applied` LSN right after constructing the
    /// `TxgStateMachine`, closing that gap. The
    /// `compute_min_last_flushed_lsn_after` projection still uses
    /// `max(wal_checkpoint, prev)` per shard as defense-in-depth.
    ///
    /// Lifecycle ops (drop_snapshot / drop_volume / clone_volume /
    /// create_volume / range_delete) symmetrically enter
    /// `self.txg.enter()` after their forced sync barrier and call
    /// `_txg_guard.record_lsn(lsn)` after `submit_wal_ops`, so their
    /// LSNs land in `slot_max_lsn(open_txg)` exactly like the
    /// `commit_ops` hot path. Without that, the lifecycle WAL records
    /// would never reach `wal_checkpoint` and `prune_all_segments`
    /// would leave their segments alive forever.
    pub(crate) fn run_sync_cycle(
        &self,
        txg: crate::types::Txg,
        kind: crate::metrics::FlushKind,
    ) -> Result<()> {
        self.run_sync_cycle_body(txg, kind)?;
        Ok(())
    }

    fn run_sync_cycle_body(
        &self,
        txg: crate::types::Txg,
        kind: crate::metrics::FlushKind,
    ) -> Result<()> {
        // ZFS-TXG-clone Phase 4 gate-shrink (final): the SAMPLE phase
        // runs gateless. The serialisation it used to get from
        // `apply_gate.write()` is now provided by:
        //
        // - Every lifecycle op (snapshot / drop_snapshot / drop_volume /
        //   clone_volume / range_delete / create_volume) drives a
        //   forced TXG sync (`flush_with_gate(Forced)`) at entry while
        //   holding `drop_gate.write`. After that sync returns, every
        //   pre-existing L2P Dirty Arc is durable on disk; no flush IO
        //   phase is in flight that could clobber a subsequent rc
        //   RMW from `atomic_incref` / `apply_drop_snapshot_pages` /
        //   `apply_clone_volume_incref` / etc. `drop_gate.write` then
        //   keeps the syncing slot empty for the rest of the lifecycle
        //   op, so no new Dirty Arcs form either.
        // - Per-shard `tree.write()` held during this flush's
        //   `lock_selected_l2p_shards_for` excludes lifecycle's
        //   `lock_all_l2p_shards_for` from racing the sample.
        // - `RcShard`'s internal delta-lock orchestration handles the
        //   commit-side `RcShard::stage` race (see
        //   [`refcount_drainer_layer_atomicity`](memory) for the P0
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

        // RAII guard: every refcount shard's `begin_checkpoint` below
        // preempts that shard's drainer (priority-3 drainer-mode). The
        // drainer is left parked and must be resumed before flush
        // returns — otherwise `delta_active` accumulates indefinitely
        // across flushes, eventually amplifying into stalls and (after
        // the prior backpressure-fallback bug was fixed) just slow
        // commits. The guard runs on every exit path (success and
        // every `return Err(...)` below) because Rust's `Drop` fires
        // at scope end. `resume_drainer` is idempotent — safe to call
        // even on shards whose `begin_checkpoint` failed before
        // preempting (sample_err mid-loop) or wasn't running a
        // drainer at all.
        let _drainer_resume_guard = RcDrainerResumeGuard {
            shards: &self.refcount_shards,
        };

        // B2: force-compact all L2P buffers so the sample phase
        // observes a tree that reflects every committed LSN up to
        // `last_applied_lsn`. With the sample-phase gate dropped,
        // concurrent commits could in principle re-populate slots
        // mid-drain. In practice every lifecycle op (the only path
        // that genuinely needs a TXG-frozen view) drives its own
        // forced sync under `drop_gate.write`; that drain is what
        // this call inside lifecycle is a defensive no-op of.
        // For ordinary `try_flush` / `flush` callers, residual
        // racing inserts are handled the same way they are at the
        // shard-tree level: per-shard `tree.write()` serialises the
        // compactor against any commit's `cow_for_write`.
        self.force_compact_l2p_buffers()?;

        let sample_started = std::time::Instant::now();
        // `slot_max_lsn(txg)` is the TXG-frozen high-water LSN of
        // commits stamped to the Syncing slot. `promote_to_syncing`
        // closed the slot to new `record_lsn` calls before this body
        // runs, and `roll_to_quiescing` already drained the inflight
        // commits (each commit holds its `TxgGuard` across submit +
        // apply + finish_global_apply), so by the time we read this
        // every commit in TXG `txg` has completed its tree mutation
        // and the value is stable for the rest of the sync cycle.
        //
        // We deliberately do NOT read `self.last_applied_lsn` here —
        // it can include commits stamped to TXGs > `txg` (the new
        // Open slot opened by `roll_to_quiescing`), which belong to a
        // future sync, not this one. Using `slot_max_lsn(txg)` keeps
        // each TXG's WAL prune watermark cleanly bounded by its own
        // stamps.
        //
        // Recovery: `apply_replay_batch` does NOT call `record_lsn`
        // on a TxgGuard, so without intervention `slot_max_lsn(open_txg)`
        // would be 0 after replay. `Db::open_with_config_and_faults`
        // stamps the open slot with the post-replay `last_applied` LSN
        // (see Cut 2 below). The `compute_min_last_flushed_lsn_after`
        // projection still uses `max(wal_checkpoint, prev)` per shard
        // as defense-in-depth.
        let wal_checkpoint = self.txg.slot_max_lsn(txg);
        let volumes = self.volumes_snapshot();
        // Decide which shards this round samples. Forced flushes
        // (`flush()`, snapshot, shutdown) always select everything;
        // steady-state `try_flush()` honours the budget cap.
        let selected = self
            .select_shards_for_flush(&volumes, matches!(kind, crate::metrics::FlushKind::Forced));
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
                records,
                old_head: vol
                    .dead_list_head_pid
                    .load(std::sync::atomic::Ordering::Acquire),
                old_tail: vol
                    .dead_list_tail_pid
                    .load(std::sync::atomic::Ordering::Acquire),
            });
        }
        if selected.is_empty() && drained_deadlists.is_empty() {
            // Nothing dirty enough to flush this round. Bail out
            // before locking any shards. `_drainer_resume_guard`
            // drops at scope end.
            self.metrics
                .record_flush_sample(kind, sample_started.elapsed());
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            return Ok(());
        }
        let lock_started = std::time::Instant::now();
        let mut l2p_guards = lock_selected_l2p_shards_for(&volumes, &selected.l2p);
        let lock_elapsed = lock_started.elapsed();
        let tree_generation = max_generation_from_two_groups(&l2p_guards, &self.refcount_shards);
        let l2p_walk_started = std::time::Instant::now();
        // Sparse per-(volume, shard) checkpoint vector. `None`
        // entries are unselected shards: their root in the manifest
        // carries over and their `last_flushed_lsn` is unchanged.
        let mut l2p_checkpoints: Vec<Vec<Option<crate::paged::tree::Checkpoint>>> =
            Vec::with_capacity(volumes.len());
        let mut guard_iter = l2p_guards.drain(..);
        for (v_idx, volume) in volumes.iter().enumerate() {
            let mut per_volume: Vec<Option<crate::paged::tree::Checkpoint>> =
                Vec::with_capacity(volume.shards.len());
            for s_idx in 0..volume.shards.len() {
                if selected.l2p[v_idx][s_idx] {
                    let mut guard = guard_iter.next().expect(
                        "lock_selected_l2p_shards_for must hand out one guard per selected shard",
                    );
                    per_volume.push(Some(guard.begin_checkpoint()));
                } else {
                    per_volume.push(None);
                }
            }
            l2p_checkpoints.push(per_volume);
        }
        debug_assert!(guard_iter.next().is_none());
        let l2p_walk_elapsed = l2p_walk_started.elapsed();
        // Refcount sample: drain delta + stage sealed pages in memory.
        // No disk IO. Meta-chain rewrite + page writes happen in the
        // IO phase below; install runs post-manifest.
        //
        // Per-shard `begin_checkpoint` is independent — each shard
        // touches only its own DeltaMap, page_table, overlay, and
        // page_pool. The only shared resource is `page_store.allocate`
        // (priority-1 path) / `page_store.allocate_batch` (drainer
        // mode), both under a short global mutex that is far from
        // saturation at the observed ~7.5k alloc/s. Parallelizing across
        // shards collapses the previously-serial 16× cost into one
        // shard's worth (modulo any skew).
        //
        // Runs WITHOUT the sample-phase `apply_gate.write()`. The
        // commit-side `RcShard::stage` invariants (delta_active +
        // delta_draining locks held across `lookup_base`,
        // transition-1/2 ordering, strict `>` replay-skip in
        // drainer-mode) are what carry the race; the gate was
        // redundant. See `refcount_drainer_layer_atomicity` memory
        // for the P0 invariants.
        //
        // Sparse over `selected.rc`: only spawn threads for selected
        // shards; unselected slots remain `None`. Failure handling
        // preserves the "first error wins, the rest are individually
        // aborted" semantics from the pre-partial code.
        let rc_drain_started = std::time::Instant::now();
        let rc_results: Vec<Option<Result<crate::refcount::shard::RcCheckpoint>>> =
            std::thread::scope(|scope| {
                let mut handles: Vec<(usize, std::thread::ScopedJoinHandle<_>)> = Vec::new();
                for (s_idx, shard) in self.refcount_shards.iter().enumerate() {
                    if selected.rc[s_idx] {
                        let h = scope.spawn(move || shard.rc.begin_checkpoint());
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
        let mut refcount_checkpoints: Vec<Option<crate::refcount::shard::RcCheckpoint>> =
            (0..self.refcount_shards.len()).map(|_| None).collect();
        let mut sample_err: Option<MetaDbError> = None;
        let mut tail_to_abort: Vec<(usize, crate::refcount::shard::RcCheckpoint)> = Vec::new();
        for (idx, result) in rc_results.into_iter().enumerate() {
            match result {
                None => continue,
                Some(Ok(ckpt)) => {
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
        self.metrics
            .record_flush_sample(kind, sample_started.elapsed());
        self.metrics.record_flush_sample_breakdown(
            lock_elapsed,
            l2p_walk_elapsed,
            rc_drain_elapsed,
        );
        // Sample workload size: L2P dirty pages snapshotted, refcount
        // delta entries drained, fresh refcount data pages allocated.
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
        self.metrics.record_flush_sample_workload(
            l2p_dirty_pages,
            rc_drained_deltas,
            rc_fresh_pages,
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
        // No sample-phase gate to drop. The IO phase below proceeds
        // straight from the sample: concurrent commits never blocked
        // on us, and the dirty page Arcs we hold in the checkpoints
        // make the IO independent of further tree mutations. The
        // manifest commit + atomics bump below acquires
        // `apply_gate.write()` for its narrow window.

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
                                self.metrics
                                    .record_flush_io(io_started.elapsed(), total_pages_written);
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
        // Refcount sealed pages flow through the same write_sealed_page_runs
        // batch as L2P. Refcount meta-chain pages also fold into the
        // same batch via `build_meta_chain` (below) — one io_uring
        // submission covers L2P + RC data + RC meta. Only selected
        // (= `Some(...)`) RC shards contribute pages this round.
        for ckpt_opt in &refcount_checkpoints {
            if let Some(ckpt) = ckpt_opt {
                let before = sealed_pages.len();
                ckpt.append_sealed_pages(&mut sealed_pages);
                total_pages_written += sealed_pages.len() - before;
            }
        }
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
                    self.metrics
                        .record_flush_io(io_started.elapsed(), total_pages_written);
                    self.metrics
                        .record_flush_total(kind, flush_started.elapsed());
                    self.abort_rc_checkpoints_sparse(refcount_checkpoints, wal_checkpoint);
                    self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
                    return Err(err);
                }
            }
        }
        // Build per-volume dead-list segments and fold them into the
        // same `sealed_pages` batch as L2P + RC. Allocate contiguous
        // runs per volume (Phase 2 / [[no-refcount-hot-path-design]]).
        // The plan vector is owned across the IO + manifest section
        // so failures can restore drained records back into the
        // buffers and free the allocated page runs.
        let mut dead_list_plans: Vec<DeadListSegmentPlan> =
            Vec::with_capacity(drained_deadlists.len());
        let mut dead_list_alloc_err: Option<MetaDbError> = None;
        for entry in &drained_deadlists {
            let page_count = crate::deadlist::segment_pages_for(entry.records.len());
            if page_count == 0 {
                continue;
            }
            match self.page_store.allocate_run(page_count) {
                Ok(start_pid) => {
                    let pages = crate::deadlist::build_segment_pages(
                        start_pid,
                        &entry.records,
                        entry.old_tail,
                        wal_checkpoint,
                    );
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
            self.metrics
                .record_flush_io(io_started.elapsed(), total_pages_written);
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            self.abort_rc_checkpoints_sparse(refcount_checkpoints, wal_checkpoint);
            self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
            return Err(err);
        }

        let page_write_started = std::time::Instant::now();
        if let Err(err) = self.page_store.write_sealed_page_runs(sealed_pages) {
            self.metrics
                .record_flush_io_page_write(page_write_started.elapsed());
            self.rollback_dead_list_drain(&mut drained_deadlists, &dead_list_plans, wal_checkpoint);
            self.metrics
                .record_flush_io(io_started.elapsed(), total_pages_written);
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            self.abort_rc_checkpoints_sparse(refcount_checkpoints, wal_checkpoint);
            self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
            return Err(err);
        }
        self.metrics
            .record_flush_io_page_write(page_write_started.elapsed());
        let sync_started = std::time::Instant::now();
        if let Err(err) = self.page_store.sync() {
            self.metrics.record_flush_io_sync(sync_started.elapsed());
            self.rollback_dead_list_drain(&mut drained_deadlists, &dead_list_plans, wal_checkpoint);
            self.metrics
                .record_flush_io(io_started.elapsed(), total_pages_written);
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            self.abort_rc_checkpoints_sparse(refcount_checkpoints, wal_checkpoint);
            self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
            return Err(err);
        }
        self.metrics.record_flush_io_sync(sync_started.elapsed());
        self.metrics
            .record_flush_io(io_started.elapsed(), total_pages_written);
        // Dead-list segment pages are now durable on disk. The
        // manifest still references the OLD tails until the commit
        // below; if it fails we restore drained records to the
        // buffer and free the just-allocated runs. Phase 2 leaves
        // the on-disk segment bytes as orphans if the manifest
        // never commits (Phase 5 page_store GC reconciliation).
        if let Err(err) = self
            .faults
            .inject(FaultPoint::DeadListPostSegWriteBeforeManifest)
        {
            self.rollback_dead_list_drain(&mut drained_deadlists, &dead_list_plans, wal_checkpoint);
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            self.abort_rc_checkpoints_sparse(refcount_checkpoints, wal_checkpoint);
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
        let dead_list_overrides: HashMap<VolumeOrdinal, (PageId, PageId)> = dead_list_plans
            .iter()
            .map(|plan| {
                let new_head = if plan.old_head == crate::types::NULL_PAGE {
                    plan.start_pid
                } else {
                    plan.old_head
                };
                (plan.vol.ord, (new_head, plan.start_pid))
            })
            .collect();

        let manifest_started = std::time::Instant::now();
        // ZFS-TXG-clone Phase 4 gate-shrink: this is the only point in
        // `run_sync_cycle_body` that acquires `apply_gate.write()`. The
        // window covers manifest commit prep, `wal.fsync_all_lanes()`,
        // `manifest_state.store.commit`, dead-list head/tail promotion,
        // the per-shard `last_flushed_lsn.store(wal_checkpoint, ...)`
        // loop, and the `unlogged_pending_lsn` clear. Rationale:
        //
        // - Serializes the manifest commit against other
        //   `apply_gate.write()` holders (snapshot / range_delete /
        //   drop_volume / drop_snapshot / `async_reclaim` GC) so the
        //   on-disk `(manifest, atomics)` pair stays consistent and
        //   so readers never observe a transient half-bumped state.
        // - Blocks new apply across `fsync_all_lanes` — so no commit
        //   at LSN > `wal_checkpoint` can complete its apply between
        //   the fsync and the manifest commit (which would violate
        //   "manifest checkpoint_lsn reflects a strict prefix of
        //   applied ops").
        // - Releases before IO-heavy post-manifest work (RC meta
        //   install, L2P checkpoint install via apply lanes, reclaim,
        //   WAL prune) — those paths take their own per-shard locks.
        let gate_started = std::time::Instant::now();
        let apply_guard = self.apply_gate.write();
        self.metrics.record_flush_gate_wait(gate_started.elapsed());
        let mut manifest_state = self.manifest_state.lock();
        let dedup_update = match self
            .prepare_dedup_manifest_update(&mut manifest_state.manifest, tree_generation)
        {
            Ok(update) => update,
            Err(err) => {
                self.metrics
                    .record_flush_manifest(manifest_started.elapsed());
                self.metrics
                    .record_flush_total(kind, flush_started.elapsed());
                drop(manifest_state);
                drop(apply_guard);
                self.rollback_dead_list_drain(
                    &mut drained_deadlists,
                    &dead_list_plans,
                    wal_checkpoint,
                );
                self.abort_rc_checkpoints_sparse(refcount_checkpoints, wal_checkpoint);
                self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
                return Err(err);
            }
        };
        if let Err(err) = self
            .faults
            .inject(FaultPoint::FlushPostLevelRewriteBeforeManifest)
        {
            self.metrics
                .record_flush_manifest(manifest_started.elapsed());
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            drop(manifest_state);
            drop(apply_guard);
            self.rollback_dead_list_drain(&mut drained_deadlists, &dead_list_plans, wal_checkpoint);
            self.abort_rc_checkpoints_sparse(refcount_checkpoints, wal_checkpoint);
            self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
            return Err(err);
        }

        if let Err(err) = refresh_manifest_from_checkpoints(
            &mut manifest_state.manifest,
            &volumes,
            &l2p_checkpoints,
            &dead_list_overrides,
        ) {
            self.metrics
                .record_flush_manifest(manifest_started.elapsed());
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            drop(manifest_state);
            drop(apply_guard);
            self.rollback_dead_list_drain(&mut drained_deadlists, &dead_list_plans, wal_checkpoint);
            self.abort_rc_checkpoints_sparse(refcount_checkpoints, wal_checkpoint);
            self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
            return Err(err);
        }
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
        // Buffer-as-sole-journal Phase B / C: persist the buffer +
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
        manifest_state.manifest.last_processed_buffer_seq = self
            .buffer_applied_watermark
            .load(std::sync::atomic::Ordering::Acquire);
        manifest_state.manifest.lifecycle_replay_seq = self
            .lifecycle_applied_watermark
            .load(std::sync::atomic::Ordering::Acquire);
        // ZFS-TXG-clone Phase 4: persist `checkpoint_txg = open_txg - 1`
        // so a re-open's `TxgStateMachine::new(checkpoint_txg)` resumes
        // with `open_txg = checkpoint_txg + 1` and the new open slot
        // sits where the previous open left off. We use `open_txg - 1`
        // rather than `checkpoint_txg()` because the live state machine
        // only advances `checkpoint_txg` when `TxgSyncThread::mark_synced`
        // is called — Phase 4 Step 7 ships the threads default-off, so
        // `mark_synced` never fires unless the operator enables them.
        // Saturating sub guards the bootstrap path where `open_txg == 1`
        // and `checkpoint_txg = 0` is the correct durable value.
        manifest_state.manifest.checkpoint_txg = self.txg.open_txg().saturating_sub(1);
        // Tier 2.B Stage 1: persist per-shard durable_seq alongside
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
            self.metrics
                .record_flush_manifest(manifest_started.elapsed());
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            drop(manifest_state);
            drop(apply_guard);
            self.rollback_dead_list_drain(&mut drained_deadlists, &dead_list_plans, wal_checkpoint);
            self.abort_rc_checkpoints_sparse(refcount_checkpoints, wal_checkpoint);
            self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
            return Err(err);
        }
        // Buffer-as-sole-journal Phase D.5b retired the WAL TXG-sync
        // barrier: there is no WAL page cache to flush. The lifecycle
        // journal already fsyncs each record at append time, and
        // data-plane durability rides on onyx's LV2 buffer (which
        // restarts replay from its own state on crash).
        if let Err(err) = self.faults.inject(FaultPoint::TxgSyncMidway) {
            self.metrics
                .record_flush_manifest(manifest_started.elapsed());
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            drop(manifest_state);
            drop(apply_guard);
            self.rollback_dead_list_drain(&mut drained_deadlists, &dead_list_plans, wal_checkpoint);
            self.abort_rc_checkpoints_sparse(refcount_checkpoints, wal_checkpoint);
            self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
            return Err(err);
        }
        let manifest = manifest_state.manifest.clone();
        if let Err(err) = manifest_state.store.commit(&manifest) {
            self.metrics
                .record_flush_manifest(manifest_started.elapsed());
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            drop(manifest_state);
            drop(apply_guard);
            self.rollback_dead_list_drain(&mut drained_deadlists, &dead_list_plans, wal_checkpoint);
            self.abort_rc_checkpoints_sparse(refcount_checkpoints, wal_checkpoint);
            self.abort_checkpoints_sparse(&volumes, &l2p_checkpoints);
            return Err(err);
        }
        self.metrics
            .record_flush_manifest(manifest_started.elapsed());

        // Manifest commit is durable. Promote per-volume dead-list
        // tail/head atomics so subsequent apply ops link new
        // segments off the new tail. This must happen AFTER
        // `manifest_state.store.commit` succeeds — if it failed and
        // we'd already stored, the in-memory atomic would race ahead
        // of the durable manifest and the next flush would link a
        // new segment to an orphaned tail.
        for plan in &dead_list_plans {
            if plan.old_head == crate::types::NULL_PAGE {
                plan.vol
                    .dead_list_head_pid
                    .store(plan.start_pid, std::sync::atomic::Ordering::Release);
            }
            plan.vol
                .dead_list_tail_pid
                .store(plan.start_pid, std::sync::atomic::Ordering::Release);
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
        // when `wal_checkpoint = slot_max_lsn(txg) = 0` (no commits
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
        // End of the narrow `apply_gate.write()` window. Everything
        // below this point (RC meta install, L2P checkpoint install
        // via apply lanes, reclaim, WAL prune) takes its own per-shard
        // locks and does not need the global gate.
        drop(apply_guard);

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
                    self.metrics.record_flush_install(install_started.elapsed());
                    self.metrics
                        .record_flush_total(kind, flush_started.elapsed());
                    return Err(err);
                }
                Err(_) => {
                    drop(manifest_state);
                    self.metrics.record_flush_install(install_started.elapsed());
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
        self.metrics.record_flush_install(install_started.elapsed());

        if !checkpoint_frees.is_empty() {
            self.page_store
                .free_many(&checkpoint_frees, tree_generation)?;
            for pid in checkpoint_frees {
                self.page_cache.invalidate(pid);
            }
        }

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
            let reclaim_budget = flush_reclaim_budget(deferred_before, total_pages_written);
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
        self.metrics.record_flush_reclaim(reclaim_started.elapsed());
        self.metrics
            .record_flush_total(kind, flush_started.elapsed());
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
            let records = std::mem::take(&mut entry.records);
            if !records.is_empty() {
                entry.vol.dead_list.restore_front(records);
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

    /// Project what `compute_min_last_flushed_lsn` will return after
    /// this round's atomic stores land — substitute `wal_checkpoint`
    /// for selected shards (which we're about to commit), keep the
    /// current atomic for unselected shards. Lets us compute
    /// `manifest.checkpoint_lsn` correctly while still holding the
    /// manifest lock, before storing the new atomics (the stores
    /// can't happen before the commit because a commit failure
    /// would leave them lying).
    ///
    /// Phase 4 gate-shrink note: with `wal_checkpoint =
    /// txg.slot_max_lsn(txg)`, a flush whose Syncing slot had no
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
