use super::*;

/// Max rollback ops packed into one restore transaction. Diffs at or below
/// this size restore atomically in a single WAL record; larger diffs chunk
/// across transactions (each atomic, the whole restore re-runnable).
const RESTORE_MAX_OPS_PER_TX: usize = 16_384;

/// Which surviving entity inherits a dropped snapshot's page-deadlist
/// (ZFS port Phase 2a MERGE). The youngest snapshot's `S_next` is the live
/// HEAD; otherwise the next-younger surviving snapshot.
#[derive(Clone, Copy)]
enum MergeTarget {
    Head,
    Snapshot(SnapshotId),
}

/// Shared inputs the `drop_snapshot` page-deadlist work derives once: the
/// previous surviving snapshot's `created_lsn`, the entity that inherits
/// S's deaths, and that inheritor's current deadlist records (`DL_next`).
/// Always derived (ZFS port Phase 4 S2c decoupled FREE from CHAIN
/// maintenance): the MERGE runs on EVERY drop — incl. clone-involved ones —
/// to keep the per-volume chain complete across routing flips, while the
/// authoritative FREE-set comes from the deadlist (non-clone) or the
/// reachability difference (clone-involved). The derivation is single-volume
/// (`vol_ord == vol`), correct for a clone source. Reused by
/// `check_page_deadlist_shadow` and `plan_page_deadlist_merge`.
struct InheritorContext {
    s_prev_created: Lsn,
    target: MergeTarget,
    dl_next: Vec<crate::deadlist::DeadRecord>,
    /// `created_lsn`s of every OTHER surviving snapshot of this volume
    /// (excludes S, the snapshot being dropped), sorted ascending. The
    /// free decision for a `dl_next` record `(birth, death)` is exact:
    /// dropping S frees the record iff NO surviving snapshot still pins
    /// the page, i.e. no `created_lsn` here lies in `[birth, death)`.
    /// This subsumes the older `birth > s_prev` proxy, which missed
    /// same-`created_lsn` siblings (a page born after `s_prev` can still
    /// be pinned by a sibling captured at the exact same lsn) and could
    /// drop chain records a later-dropped sibling/snapshot still needs.
    other_created_sorted: Vec<Lsn>,
}

/// Planned page-deadlist MERGE result: the inheritor and its new chain
/// anchor. The carried set always collapses to a single segment, so head
/// and tail are the same page (`NULL_PAGE` when nothing carries forward).
struct PageDeadlistMerge {
    target: MergeTarget,
    anchor: PageId,
}

impl Db {
    // -------- snapshot operations -----------------------------------------

    /// Take a snapshot of volume `vol_ord`'s L2P state. Returns the new
    /// snapshot id. Unknown volume ordinals surface as `InvalidArgument`.
    ///
    /// Refcount state is global (Phase 6.5b retired per-snapshot refcount
    /// roots), so the snapshot only captures the target volume's L2P
    /// shard roots + an incref on each so the snapshot's view outlives
    /// subsequent COW writes on the target volume.
    ///
    /// ZFS `dsl_sync_task` port: this does NOT block the write path. It
    /// enqueues a [`SyncTaskOp::TakeSnapshot`] task against the current
    /// open TXG (under a `txg.enter()` guard so the TXG can't promote to
    /// syncing before the task is queued), forces that TXG to sync, and
    /// blocks for exactly that one TXG. The sync cycle does the real work
    /// in syncing context ([`Db::stage_pending_snapshots`]): it captures
    /// the roots it is flushing this cycle, journals a
    /// `LifecycleOp::TakeSnapshot` (which mints a strictly-monotone lsn
    /// for the gen-idempotent root incref + makes the op crash-replayable),
    /// stages the incref into the open TXG slot, and inserts the
    /// `SnapshotEntry` — all durable atomically with that TXG's manifest
    /// commit (= ZFS uberblock). Foreground writers join the next open TXG
    /// and never block. The outcome (id or error) comes back through the
    /// task's result slot.
    pub fn take_snapshot(&self, vol_ord: VolumeOrdinal) -> Result<SnapshotId> {
        // Validate the volume up front so an unknown ordinal short-circuits
        // with a clean `InvalidArgument` before anything is queued.
        let _ = self.volume(vol_ord)?;

        // ZFS port Part B: a prior forced sync failed non-recoverably and
        // poisoned the subsystem (the TXG it left in Syncing can never
        // complete). Reject new snapshots fast in BOTH threads modes instead of
        // queueing a task that would hang in `wait_until_synced` / `flush_with_gate`.
        if let Some(err) = self.sync_poison_error() {
            return Err(err);
        }

        // Enqueue the snapshot as a TXG sync task and force the open TXG to
        // sync. The actual work — capture the volume's just-flushed roots,
        // journal a `LifecycleOp::TakeSnapshot`, stage the per-root incref,
        // and insert the `SnapshotEntry` — runs in syncing context
        // ([`Db::stage_pending_snapshots`]), durable atomically with that
        // TXG's manifest commit. Foreground writers join the NEXT open TXG
        // and never block on us (no `drop_gate.write`, no synchronous
        // all-shard flush).
        let result: Arc<Mutex<Option<Result<SnapshotId>>>> = Arc::new(Mutex::new(None));
        let target = {
            // Hold `txg.enter()` across the push so the open TXG cannot
            // promote-to-syncing before our task is queued; otherwise the
            // cycle that syncs `target` could miss it (the enqueue/roll
            // race). Dropping the guard before forcing the sync below
            // avoids the reverse wait (sync waits for the guard).
            let guard = self.txg.enter();
            let target = guard.txg();
            self.pending_sync_tasks.lock().push(PendingSyncTask {
                op: SyncTaskOp::TakeSnapshot { vol_ord },
                target_txg: target,
                result: result.clone(),
                committed_entry: None,
            });
            target
        };

        // Drive `target` to sync and block until it is durable.
        if self.txg_threads_enabled {
            self.txg_quiesce_notifier.signal_force(target);
            if !self.txg.wait_until_synced(target) {
                // ZFS port Part B: the sync thread's cycle failed and aborted
                // the subsystem. Drop our orphaned task and surface the
                // restart-required error instead of hanging. (poison_sync also
                // drains pending tasks, so the remove is belt-and-suspenders.)
                self.remove_pending_sync_task(&result);
                return Err(self.sync_poison_error().unwrap_or_else(|| {
                    MetaDbError::Corruption("txg sync aborted; restart required".into())
                }));
            }
        } else if let Err(err) = self.flush_with_gate(crate::metrics::FlushKind::Forced) {
            // Threads-off: we drove the cycle inline and it failed. Remove
            // our orphaned task so a later flush doesn't resurrect it, then
            // surface the error.
            self.remove_pending_sync_task(&result);
            return Err(err);
        }

        // The cycle filled the result before advancing `checkpoint_txg`
        // (threads-on) / returning (threads-off), so it is set now.
        match result.lock().take() {
            Some(outcome) => outcome,
            None => {
                // `target` synced without processing our task. The
                // `txg.enter()` guard makes this unreachable in normal
                // operation (the task is always queued before `target`
                // promotes-to-syncing); treat it defensively.
                self.remove_pending_sync_task(&result);
                Err(MetaDbError::Corruption(
                    "take_snapshot: sync cycle completed without processing the queued task".into(),
                ))
            }
        }
    }

    /// Remove a still-queued sync task (matched by result-`Arc` identity).
    /// Used by the threads-off `take_snapshot` error path to drop a task
    /// whose inline cycle failed before processing it.
    fn remove_pending_sync_task(&self, result: &Arc<Mutex<Option<Result<SnapshotId>>>>) {
        self.pending_sync_tasks
            .lock()
            .retain(|t| !Arc::ptr_eq(&t.result, result));
    }

    /// Open a read-only view of the data as it existed when `id` was taken.
    /// Returns `None` if the snapshot id is unknown.
    pub fn snapshot_view(&self, id: SnapshotId) -> Option<SnapshotView<'_>> {
        let guard = self.snapshot_views.read();
        let entry = {
            let manifest_state = self.manifest_state.lock();
            manifest_state.manifest.find_snapshot(id).cloned()
        }?;
        Some(SnapshotView {
            db: self,
            entry,
            _guard: guard,
        })
    }

    /// Compute the diff between two snapshots. Both snapshots must
    /// belong to the same volume; cross-volume diff is rejected.
    pub fn diff(&self, a: SnapshotId, b: SnapshotId) -> Result<Vec<DiffEntry>> {
        let _guard = self.snapshot_views.read();
        let (vol_ord, a_roots, b_roots) =
            {
                let manifest_state = self.manifest_state.lock();
                let a_entry = manifest_state.manifest.find_snapshot(a).ok_or_else(|| {
                    MetaDbError::InvalidArgument(format!("unknown snapshot id {a}"))
                })?;
                let b_entry = manifest_state.manifest.find_snapshot(b).ok_or_else(|| {
                    MetaDbError::InvalidArgument(format!("unknown snapshot id {b}"))
                })?;
                if a_entry.vol_ord != b_entry.vol_ord {
                    return Err(MetaDbError::InvalidArgument(format!(
                        "cannot diff snapshots across volumes: {a} on vol {} vs {b} on vol {}",
                        a_entry.vol_ord, b_entry.vol_ord,
                    )));
                }
                (
                    a_entry.vol_ord,
                    a_entry.l2p_shard_roots.clone(),
                    b_entry.l2p_shard_roots.clone(),
                )
            };
        self.diff_roots(vol_ord, &a_roots, &b_roots)
    }

    /// Diff a snapshot against the owning volume's current tree.
    pub fn diff_with_current(&self, snap: SnapshotId) -> Result<Vec<DiffEntry>> {
        let _guard = self.snapshot_views.read();
        let (vol_ord, snap_roots) = {
            let manifest_state = self.manifest_state.lock();
            let entry = manifest_state.manifest.find_snapshot(snap).ok_or_else(|| {
                MetaDbError::InvalidArgument(format!("unknown snapshot id {snap}"))
            })?;
            (entry.vol_ord, entry.l2p_shard_roots.clone())
        };

        let volume = self.volume(vol_ord)?;
        if snap_roots.len() != volume.shards.len() {
            return Err(MetaDbError::Corruption(format!(
                "snapshot {snap} has {} roots, volume {vol_ord} has {} shards",
                snap_roots.len(),
                volume.shards.len(),
            )));
        }
        let mut guards: Vec<_> = volume.shards.iter().map(|s| s.tree.write()).collect();
        let mut out = Vec::new();
        for (tree, snap_root) in guards.iter_mut().zip(snap_roots.iter().copied()) {
            let current_root = tree.root();
            out.extend(tree.diff_subtrees(snap_root, current_root)?);
        }
        out.sort_unstable_by_key(DiffEntry::key);
        Ok(out)
    }

    /// Restore the volume owning `snap_id` back to the state captured by that
    /// snapshot, discarding every change made since. Computes the
    /// snapshot→current diff and replays the inverse through the normal
    /// `commit_ops` remap path: each changed LBA is re-pointed at its snapshot
    /// value, and LBAs that did not exist in the snapshot are deleted. The
    /// replaced (diverged) PBAs are dead-listed and reclaimed by the usual
    /// lineage-GC path, exactly like an overwrite — so no physical PBA leaks
    /// and no new freed-PBA surfacing is needed.
    ///
    /// Atomicity: applied as a single transaction (one WAL record + fsync)
    /// when the diff fits in one chunk, so a crash leaves either the pre- or
    /// post-restore state. Larger diffs are chunked at `RESTORE_MAX_OPS_PER_TX`
    /// ops/tx; each chunk is atomic and the whole restore is idempotent — a
    /// re-run re-diffs and converges.
    ///
    /// The caller MUST quiesce the volume (no concurrent writes) for the
    /// duration; onyx stops the volume and drains its write buffer first.
    /// Unknown snapshot ids surface as `InvalidArgument`.
    pub fn restore_volume_to_snapshot(&self, snap_id: SnapshotId) -> Result<RestoreReport> {
        let vol_ord = {
            let manifest_state = self.manifest_state.lock();
            manifest_state
                .manifest
                .find_snapshot(snap_id)
                .ok_or_else(|| {
                    MetaDbError::InvalidArgument(format!("unknown snapshot id {snap_id}"))
                })?
                .vol_ord
        };

        // Settle everything before diffing: `diff_with_current` reads each
        // shard's committed `tree.root()`, which excludes both unfolded
        // l2p_buffer entries AND staged commits (onyx's flusher commits via the
        // staged path, whose tree fold is deferred to the next TXG sync). A
        // forced sync applies the staged commits and folds the buffer into the
        // tree, so the diff sees the volume's full committed state; without it
        // the very overwrites we must roll back are invisible and the restore
        // silently no-ops. The caller's quiescence guarantee means no new writes
        // form between the sync and the diff.
        self.flush_with_gate(crate::metrics::FlushKind::Forced)?;
        self.force_compact_l2p_buffers()?;

        // `diff_with_current` returns A=snapshot vs B=current, so:
        //   Changed/RemovedInB -> set the LBA back to the snapshot value (`old`)
        //   AddedInB           -> delete the LBA (absent in the snapshot)
        let diff = self.diff_with_current(snap_id)?;

        let mut lbas_remapped: u64 = 0;
        let mut lbas_deleted: u64 = 0;
        let mut last_lsn = self.last_applied_lsn();

        for chunk in diff.chunks(RESTORE_MAX_OPS_PER_TX) {
            let mut tx = self.begin();
            for entry in chunk {
                match entry {
                    DiffEntry::Changed { key, old, .. }
                    | DiffEntry::RemovedInB { key, old } => {
                        // Strip the seq: the snapshot value carries its original
                        // (low) commit seq, and `seq_guard_rejects` would drop a
                        // remap whose seq is below the current entry's. Restore is
                        // a deliberate authoritative rollback, so seq=0 (the
                        // guard-bypass sentinel) makes it apply unconditionally;
                        // later real writes get fresh higher seqs and still win.
                        tx.l2p_remap(vol_ord, *key, old.with_seq(0), None);
                        lbas_remapped += 1;
                    }
                    DiffEntry::AddedInB { key, .. } => {
                        tx.delete(vol_ord, *key);
                        lbas_deleted += 1;
                    }
                }
            }
            last_lsn = tx.commit()?;
        }

        Ok(RestoreReport {
            snapshot_id: snap_id,
            vol_ord,
            lbas_remapped,
            lbas_deleted,
            lsn: last_lsn,
        })
    }

    /// Drop a snapshot. The drop is logged as `LifecycleOp::DropSnapshot`
    /// so the page-refcount work (decref every page the snapshot shares
    /// with the current tree, free any page that hits rc=0) and the
    /// in-memory manifest update are atomic against process crash:
    /// after the lifecycle-journal fsync, recovery replays the op and
    /// re-drives the work to completion.
    ///
    /// Serialisation:
    /// - `drop_gate.write()` — excludes every `commit_ops` / `stage_ops`
    ///   path. The rc-dependent plan walk
    ///   ([`collect_paged_refcounts_for_roots`]) counts parent pointers
    ///   over the manifest-visible page graph, so it relies on no
    ///   concurrent `cow_for_write` moving an edge between the plan and
    ///   the `apply_drop_snapshot_pages` decref. This gate (NOT the
    ///   forced sync) is what freezes that graph; it stays.
    /// - `apply_gate.write()` — serialises our WAL submit + apply +
    ///   manifest-side bookkeeping against
    ///   [`Db::run_sync_cycle_body`]'s manifest-commit window.
    /// - `snapshot_views.write()` — waits for outstanding
    ///   [`SnapshotView`]s to drop before any page is freed.
    ///
    /// Phase B (A3 follow-up): the **forced TXG sync** that used to run
    /// at entry is GONE. It drained in-flight flush IO so the per-page
    /// decref's whole-page rc write could not be clobbered. ZFS port S3
    /// deleted per-L2P-page refcounting: `apply_drop_snapshot_pages` now
    /// frees exactly the explicit, structurally-computed free-set the
    /// producer froze under the held gates, so there is no whole-page rc
    /// write left to clobber and no need to
    /// quiesce flush IO. The refreshed-manifest commit below still makes
    /// the surviving roots durable; the decrefs ride the `DropSnapshot`
    /// WAL record (gen-stamped idempotent on replay).
    ///
    /// Crash semantics:
    /// - Before WAL fsync: no effect observable.
    /// - After WAL fsync, before/during apply: recovery replays the op
    ///   using the durable plan + per-page generation stamp for
    ///   idempotency, yielding the same final state as a clean run.
    ///
    /// No manifest commit happens inside this function; the next
    /// natural [`flush`](Self::flush) captures the new snapshot list.
    pub fn drop_snapshot(&self, id: SnapshotId) -> Result<Option<DropReport>> {
        let _drop_guard = self.drop_gate.write();
        // Phase B (A3 follow-up): no forced TXG sync. The page-rc decrefs
        // now `stage` into the array and free under a fold-consistent
        // read, so the flush-IO-drain barrier is unnecessary. `txg.enter()`
        // pins the current Open TXG; `closing_open` makes it wait out (not
        // race) a concurrent background roll, so entering without first
        // rolling the TXG ourselves is safe.
        //
        // Phase 4 gate-shrink: `_txg_guard.record_lsn(lsn)` (below)
        // records this lifecycle op's WAL LSN into `slot_max_lsn(open_txg)`
        // so `run_sync_cycle_body`'s `wal_checkpoint = slot_max_lsn(txg)`
        // watermark reflects it and the WAL segment is eventually pruned.
        let _txg_guard = self.txg.enter();
        let _apply_guard = self.apply_gate.write();
        let _view_guard = self.snapshot_views.write();

        let (entry, other_snapshots) = {
            let manifest_state = self.manifest_state.lock();
            let Some(entry) = manifest_state
                .manifest
                .snapshots
                .iter()
                .find(|e| e.id == id)
                .cloned()
            else {
                return Ok(None);
            };
            let others = manifest_state
                .manifest
                .snapshots
                .iter()
                .filter(|snapshot| snapshot.id != id)
                .cloned()
                .collect::<Vec<_>>();
            (entry, others)
        };
        // v6 SnapshotEntry no longer carries refcount state (Phase 6.5b
        // retired it), so there's nothing to assert about refcount here.

        // Commit 9: snapshots are per-volume, so page collection +
        // cache invalidation target the source volume only. The
        // entry's vol_ord is load-bearing here — `drop_volume` refuses
        // to drop a volume while any snapshot pins it, so this lookup
        // cannot miss in a well-formed manifest.
        let source_volume = self.volume(entry.vol_ord).map_err(|_| {
            MetaDbError::Corruption(format!(
                "drop_snapshot: snapshot {id} references unknown volume ord {}",
                entry.vol_ord,
            ))
        })?;
        if entry.l2p_shard_roots.len() != source_volume.shards.len() {
            return Err(MetaDbError::Corruption(format!(
                "drop_snapshot: snapshot {id} has {} roots but volume {} has {} shards",
                entry.l2p_shard_roots.len(),
                entry.vol_ord,
                source_volume.shards.len(),
            )));
        }
        // Lock ALL volumes' shards and refcount shards so we can flush +
        // refresh manifest before the page-refcount cascade.
        //
        // Refreshing the manifest here is load-bearing: prior
        // `commit_ops` cows may have advanced each volume's root
        // without updating `manifest.volumes`. If we went straight to
        // the cascade and froze `rc(pid)=0` for any page that was
        // already cow'd away from by a live volume but is still
        // referenced by this snapshot, the on-disk manifest would
        // still list that freed pid as the volume's root — the next
        // open would then fail inside `open_l2p_shards` reading a
        // Free page. Commit a refreshed manifest (snapshot still
        // present) so reopen always finds current roots.
        //
        // B2: drain L2P buffer first for checkpoint_lsn safety: this path advances
        // `manifest.checkpoint_lsn = last_applied_lsn`; if any buffer
        // entry sits at LSN ≤ last_applied_lsn the recovery cursor
        // would skip past it. Compaction here advances each shard's
        // `compacted_lsn` ≥ last_applied_lsn (apply_gate.write held),
        // closing the gap. No-op when buffer disabled.
        self.force_compact_l2p_buffers()?;
        let volumes_snap = self.volumes_snapshot();
        let mut l2p_guards = lock_all_l2p_shards_for(&volumes_snap);
        flush_locked_l2p_shards(&mut l2p_guards)?;
        self.flush_all_refcount_shards()?;

        let checkpoint_lsn = *self.last_applied_lsn.lock();
        // Crash-recovery completeness (G1): this commit advances
        // `checkpoint_lsn` to `last_applied_lsn` and makes the
        // `force_compact`-folded roots durable. Seal every volume's pending
        // page-deadlist accumulator into its HEAD chain FIRST so no death the
        // committed roots imply free is left only in volatile RAM. The bumped
        // HEAD anchors fold into this commit via `refresh_manifest_from_locked`.
        // (Empty accumulators are a no-op; clone volumes accumulate few/none.)
        for vol in &volumes_snap {
            self.seal_page_dead_list_accumulator(vol, checkpoint_lsn)?;
        }
        // Tripwire (Step E): after the G1 seal, no volume may still hold a
        // page-death at or below the checkpoint this commit advances to —
        // that exact state (durable roots imply a free the deadlist never
        // sealed) is the crash-recovery completeness bug this fixes.
        #[cfg(debug_assertions)]
        for vol in &volumes_snap {
            debug_assert!(
                vol.page_dead_list
                    .iter()
                    .flat_map(|a| a.peek())
                    .all(|r| r.death_lsn > checkpoint_lsn),
                "drop_snapshot G1: volume {} retains a page-death <= checkpoint_lsn {checkpoint_lsn} after seal",
                vol.ord,
            );
        }
        let dedup_generation = max_generation_from_two_groups(&l2p_guards, &self.refcount_shards);
        let dedup_update = {
            let mut mstate = self.manifest_state.lock();
            let dedup_update =
                self.prepare_dedup_manifest_update(&mut mstate.manifest, dedup_generation)?;
            self.refresh_manifest_from_locked(
                &mut mstate.manifest,
                &volumes_snap,
                &l2p_guards,
                Some(checkpoint_lsn),
            )?;
            mstate.manifest.checkpoint_lsn = checkpoint_lsn;
            // Phase B: no forced TXG sync ran ahead of this commit, so it
            // is the checkpoint that makes the flushes above durable and
            // must advance the lifecycle/buffer replay watermarks. The
            // `DropSnapshot` op is submitted AFTER this, so its seq is
            // correctly excluded and replayed on the next open. Held under
            // `apply_gate.write`. See `stamp_replay_watermarks`.
            self.stamp_replay_watermarks(&mut mstate.manifest);
            let manifest = mstate.manifest.clone();
            mstate.store.commit(&manifest)?;
            dedup_update
        };
        commit_l2p_checkpoint(&mut l2p_guards, dedup_generation)?;
        commit_refcount_checkpoint(&self.refcount_shards, dedup_generation)?;
        self.finish_dedup_manifest_update(dedup_update, dedup_generation)?;

        let all_current_roots: Vec<PageId> = l2p_guards.iter().map(|tree| tree.root()).collect();
        // Phase 5: do not emit PBA decrefs for snapshot-only logical
        // mappings. Global PBA rc is no longer a per-live-LBA counter
        // (ordinary remaps and dedup hits are rc-neutral), so walking the
        // snapshot/current diff and subtracting one per LBA can drive rc to
        // zero while another live volume/LBA still references the same PBA.
        // The L2P page-refcount cascade below still releases metadata pages;
        // physical PBA reclaim is handled by dead-list / retired-extent
        // confirmation paths.
        let pba_decrefs: Vec<Pba> = Vec::new();
        drop(l2p_guards);

        // Page refcounts are physical-page ownership counts, not
        // per-volume logical counts. `clone_volume` can make any live
        // volume (and snapshots of that volume) share the same paged L2P
        // pages as the snapshot being dropped. Build the decrement plan
        // from the complete manifest-visible page graph so a page is
        // decremented exactly when removing this snapshot removes one
        // physical incoming edge. PBA-level reclaim is deliberately absent
        // here in Phase 5; dead-list / retired-extent confirmation owns it.
        let mut roots_before = all_current_roots.clone();
        roots_before.extend(entry.l2p_shard_roots.iter().copied());
        roots_before.extend(
            other_snapshots
                .iter()
                .flat_map(|snapshot| snapshot.l2p_shard_roots.iter().copied()),
        );
        let mut roots_after = all_current_roots.clone();
        roots_after.extend(
            other_snapshots
                .iter()
                .flat_map(|snapshot| snapshot.l2p_shard_roots.iter().copied()),
        );
        let before_refs = collect_paged_refcounts_for_roots(&self.page_store, &roots_before)?;
        let after_refs = collect_paged_refcounts_for_roots(&self.page_store, &roots_after)?;
        let mut pages: Vec<PageId> = Vec::new();
        for (&pid, &before) in &before_refs {
            let after = after_refs.get(&pid).copied().unwrap_or(0);
            match before.checked_sub(after) {
                Some(1) => pages.push(pid),
                Some(0) => {}
                Some(delta) => {
                    return Err(MetaDbError::Corruption(format!(
                        "drop_snapshot page-ref delta for {pid} was {delta}, expected 0 or 1"
                    )));
                }
                None => {
                    return Err(MetaDbError::Corruption(format!(
                        "drop_snapshot page-ref underflow for {pid}: before={before} after={after}"
                    )));
                }
            }
        }
        // ZFS port Phase 2a/S2c: derive the inheritor context (S_prev/S_next +
        // DL_next) once. It always drives the MERGE below (single-volume chain
        // bookkeeping, run on every drop); the NON-clone FREE path also feeds
        // it to `check_page_deadlist_shadow`, while the clone-involved FREE path
        // uses reachability instead. The page-rc apply between here and the
        // merge frees only L2P pages, never deadlist segments, and we hold
        // apply_gate.write + drop_gate.write, so `DL_next` (and the HEAD
        // accumulator it peeked) stays valid for both consumers.
        let inheritor_ctx =
            self.page_deadlist_inheritor_context(&entry, &other_snapshots, &source_volume)?;
        // ZFS port Phase 4 S2/S2c: the authoritative, page-rc-independent
        // free-set. NON-clone drops use the single-volume deadlist (the shadow
        // returns it, == structural_to_free on Ok). CLONE-INVOLVED drops
        // (`snapshot_drop_clone_involved`, sticky CLONE_LINEAGE flag) use the
        // structural reachability difference instead — the single-vol deadlist
        // can't model cross-volume page sharing (R5). Either way the result is
        // frozen into the WAL op so S3 can delete the page-rc cascade.
        let free_pages: Option<Vec<PageId>> =
            Some(if self.snapshot_drop_clone_involved(&source_volume) {
                self.check_clone_drop_reachability_shadow(
                    id,
                    &entry,
                    &pages,
                    &after_refs,
                    &all_current_roots,
                    &other_snapshots,
                )?
            } else {
                self.check_page_deadlist_shadow(id, &entry, &pages, &after_refs, &inheritor_ctx)?
            });

        // Crash-recovery completeness (G2): plan the page-deadlist MERGE
        // (write the durable carried segment) BEFORE the WAL submit, and carry
        // the resulting re-anchor in the `DropSnapshot` op. Applying the
        // re-anchor from the op (live AND replay) makes it ATOMIC with the
        // snapshot removal + the page frees — closing the window where a crash
        // after the op was durable but before the next flush lost the
        // (previously in-memory-only) merge, orphaning S's carried deaths. The
        // carried segment is synced here, so it is durable before the op's
        // fsync. `checkpoint_lsn` (== last_applied) stamps the segment: it is
        // >= every carried `death_lsn` and < any post-drop death, preserving
        // the chain-ordering invariant.
        //
        // S2c: the MERGE runs on EVERY drop, INCLUDING clone-involved ones.
        // It is single-volume bookkeeping (per `inheritor_ctx`, keyed on V's
        // own snapshots) and frees no L2P data pages — only rewrites V's
        // deadlist chain. Skipping it on clone-involved drops (the old `None`
        // arm) drops S's chain records instead of forwarding them, so when the
        // last clone-lineage volume is dropped and routing flips back to the
        // deadlist free-source, the inheritor chain diverges from the
        // structural free-set → a legal non-clone drop fires a HARD
        // `Corruption` in `check_page_deadlist_shadow` (observed as PREMATURE in
        // `s2c_merge_runs_on_clone_routed_drop_no_missing_after_flip`; the
        // missing-forward direction can also surface as a COMPLETENESS HOLE).
        // Running it always keeps the chain consistent across routing flips
        // (empty carry → NULL anchor, a no-op).
        let merge_plan = Some(self.plan_page_deadlist_merge(&entry, &inheritor_ctx, checkpoint_lsn)?);
        let merge: Option<(crate::lifecycle_log::DropMergeTarget, PageId)> =
            merge_plan.as_ref().map(|m| {
                let target = match m.target {
                    MergeTarget::Head => crate::lifecycle_log::DropMergeTarget::Head {
                        vol_ord: entry.vol_ord,
                    },
                    MergeTarget::Snapshot(sid) => {
                        crate::lifecycle_log::DropMergeTarget::Snapshot { id: sid }
                    }
                };
                (target, m.anchor)
            });

        // NOTE on `entry.l2p_roots_page`: this SnapshotRoots page is
        // referenced only by the manifest's snapshot entry, so it
        // *logically* becomes unreferenced when we apply the drop.
        // We deliberately leave it alone here though: the on-disk
        // manifest still has the snapshot entry (no commit happens
        // inside drop_snapshot), and an older-than-expected open would
        // call `load_snapshot_roots` on it — turning it Free would
        // break decode and deadlock recovery. The page becomes a
        // genuine orphan only after the next flush persists a
        // snapshot-less manifest; `reclaim_orphan_pages` (run after
        // WAL replay in `Db::open`) picks it up from there.

        // Submit + apply inline without going through commit_ops'
        // cvar queue. We hold drop_gate.write + apply_gate.write, so
        // no one else can submit, and no other apply is in flight.
        // LSN ordering: the WAL writer assigns LSNs in submit order.
        // Under drop_gate.write, no concurrent submits have been
        // accepted (they'd be waiting on drop_gate.read), so our
        // submission gets the next LSN in sequence.
        let lifecycle_op = crate::lifecycle_log::LifecycleOp::DropSnapshot {
            id,
            pages: pages.clone(),
            pba_decrefs: pba_decrefs.clone(),
            free_pages: free_pages.clone(),
            merge,
        };
        let lsn = self.submit_lifecycle_op(&lifecycle_op)?;
        _txg_guard.record_lsn(lsn);
        self.faults.inject(FaultPoint::CommitPostWalBeforeApply)?;

        // Block until every prior LSN has applied. Under our locks,
        // `last_applied_lsn` can only move forward when a commit
        // completes — and since we hold drop_gate.write, no new
        // commits have entered, so this wait is bounded by whatever
        // was in flight at the moment we took the gate.
        self.wait_for_global_apply_turn(lsn)?;

        let outcome = apply_drop_snapshot_pages_and_decrefs(
            &self.page_store,
            &self.refcount_shards,
            lsn,
            &pages,
            &pba_decrefs,
            free_pages.as_deref(),
        )?;
        self.faults
            .inject(FaultPoint::CommitPostApplyBeforeLsnBump)?;

        // Apply's page writes went straight through page_store; any
        // PageBuf that cached one of these physical pages now has a
        // stale refcount. Sweep every live volume, not just the source:
        // clone_volume intentionally shares L2P pages across volumes,
        // and a later write through a stale Clean copy can otherwise
        // resurrect the pre-drop rc and leak a page ref.
        let all_volumes: Vec<Arc<Volume>> = self.volumes.read().values().cloned().collect();
        for &pid in &pages {
            self.page_cache.invalidate(pid);
            for volume in &all_volumes {
                for shard in &volume.shards {
                    shard.tree.write().forget_page(pid);
                }
            }
        }

        // Crash-recovery completeness (G2): apply the page-deadlist MERGE
        // re-anchor (planned + sealed durably BEFORE the WAL submit above)
        // atomically with S's removal. This is the LIVE application of the
        // exact re-anchor the `DropSnapshot` op carries; WAL replay re-applies
        // it from the op (see open.rs), so a crash on either side leaves
        // {S present, old chains} or {S gone, merged chain} — never a
        // half-applied merge lost before the next flush. The carried set is
        // one segment, so head == tail == anchor.
        {
            let mut mstate = self.manifest_state.lock();
            if let Some(merge) = &merge_plan {
                match merge.target {
                    MergeTarget::Head => {
                        use std::sync::atomic::Ordering;
                        // The carried segment already absorbed the HEAD's deaths
                        // (the G1 seal drained the accumulator at the commit
                        // above; `plan_page_deadlist_merge` then read the
                        // G1-extended HEAD chain). Just re-anchor.
                        source_volume
                            .page_dead_list_head_pid
                            .store(merge.anchor, Ordering::Release);
                        source_volume
                            .page_dead_list_tail_pid
                            .store(merge.anchor, Ordering::Release);
                    }
                    MergeTarget::Snapshot(sid) => {
                        if let Some(sn) =
                            mstate.manifest.snapshots.iter_mut().find(|s| s.id == sid)
                        {
                            sn.page_dead_list_tail_pid = merge.anchor;
                        }
                    }
                }
            }
            mstate.manifest.snapshots.retain(|s| s.id != id);
        }
        // Source volume just lost a snap; recompute its oldest-snap-lsn
        // entry. May go from Some(lsn) to None (last snap on this vol)
        // or to a later lsn (the next-oldest survives).
        self.recompute_snap_info(entry.vol_ord);

        self.finish_global_apply(lsn)?;
        self.advance_dispatch_lsn(lsn);

        let (freed_leaf_values, pages_freed, freed_pbas) = match outcome {
            ApplyOutcome::DropSnapshot {
                freed_leaf_values,
                pages_freed,
                freed_pbas,
            } => (freed_leaf_values, pages_freed, freed_pbas),
            other => {
                return Err(MetaDbError::Corruption(format!(
                    "DropSnapshot apply returned unexpected outcome: {other:?}"
                )));
            }
        };

        // Drain whatever the apply just queued for deferred reclaim. We
        // still hold drop_gate.write + apply_gate.write here; live readers
        // (which only take epoch pins) can be active, and any of their
        // pins held back from the snapshot pages will keep tags alive
        // across this call. That's correct: those pages stay parked
        // until the long reader departs.
        self.reclaim_freed_pages()?;

        // Wake reclaim bookkeeping — dropping a snapshot may have
        // un-pinned dead-list records whose `[birth, death)` interval
        // overlapped the snapshot's `created_lsn`. The FreePbas-emitting
        // lineage driver can then advance past those segments.
        self.notify_async_reclaim();

        Ok(Some(DropReport {
            snapshot_id: id,
            freed_leaf_values,
            pages_freed,
            freed_pbas,
        }))
    }

    /// ZFS port Phase 2a — derive the shared inputs the drop's page-deadlist
    /// shadow check and MERGE both need: the previous surviving snapshot's
    /// `created_lsn` (`S_prev`), the entity that inherits S's deaths
    /// (`S_next`, or the live HEAD when S is youngest), and that inheritor's
    /// current records (`DL_next` — its sealed chain, plus the HEAD's
    /// not-yet-drained accumulator when the inheritor is the HEAD). Reading
    /// `DL_next` here keeps the S_prev/S_next selection and the
    /// HEAD-vs-snapshot chain read in one place.
    ///
    /// S2c: derived for EVERY drop (no clone early-return). The MERGE runs
    /// unconditionally so the per-volume chain stays complete even while a
    /// clone exists and the FREE-set comes from reachability (see
    /// `drop_snapshot`). The derivation is single-volume (`vol_ord == vol`)
    /// and correct for a clone source — clones add cross-volume page
    /// *sharing*, which changes the FREE decision, not this volume's own
    /// snapshot chain bookkeeping.
    fn page_deadlist_inheritor_context(
        &self,
        entry: &SnapshotEntry,
        other_snapshots: &[SnapshotEntry],
        source_volume: &Volume,
    ) -> Result<InheritorContext> {
        let vol = entry.vol_ord;
        // Order snapshots by the TOTAL order `(created_lsn, id)`, not
        // `created_lsn` alone. `id` is monotonic with creation, so this
        // breaks `created_lsn` ties (two snapshots taken with no committed
        // op between them share a `created_lsn`) in creation order. Using
        // the total order makes S_prev/S_next the true chain neighbours:
        // a tie-sibling is a neighbour, not excluded by a strict `<`/`>`.
        // Excluding it (the old code) sent a dropped tie-sibling's deaths
        // to the live HEAD instead of its sibling, where a later snapshot
        // re-sealed them — the completeness hole — and made the sibling's
        // own pinned pages look free (premature). The free DECISION is the
        // exact `dl_record_freed` predicate below; this only picks the
        // inheritor whose chain absorbs S's deaths.
        let key = (entry.created_lsn, entry.id);
        let s_prev_created: Lsn = other_snapshots
            .iter()
            .filter(|s| s.vol_ord == vol && (s.created_lsn, s.id) < key)
            .map(|s| s.created_lsn)
            .max()
            .unwrap_or(0);
        let s_next = other_snapshots
            .iter()
            .filter(|s| s.vol_ord == vol && (s.created_lsn, s.id) > key)
            .min_by_key(|s| (s.created_lsn, s.id));
        let (target, dl_next) = match s_next {
            Some(sn) => (
                MergeTarget::Snapshot(sn.id),
                crate::deadlist::read_chain_records(sn.page_dead_list_tail_pid, |p| {
                    self.page_store.read_page(p)
                })?,
            ),
            None => {
                let mut recs = crate::deadlist::read_chain_records(
                    source_volume
                        .page_dead_list_tail_pid
                        .load(std::sync::atomic::Ordering::Acquire),
                    |p| self.page_store.read_page(p),
                )?;
                // H1: the accumulator fans out per shard — peek them all.
                recs.extend(source_volume.page_dead_list.iter().flat_map(|a| a.peek()));
                (MergeTarget::Head, recs)
            }
        };
        let mut other_created_sorted: Vec<Lsn> = other_snapshots
            .iter()
            .filter(|s| s.vol_ord == vol)
            .map(|s| s.created_lsn)
            .collect();
        other_created_sorted.sort_unstable();
        Ok(InheritorContext {
            s_prev_created,
            target,
            dl_next,
            other_created_sorted,
        })
    }

    /// ZFS port Phase 4 S2c — is this snapshot drop "clone-involved", i.e.
    /// could any other volume share the dropped snapshot's L2P pages so the
    /// single-volume page-deadlist would mis-predict the free-set? Routes the
    /// drop to the reachability free-source ([`check_clone_drop_reachability_shadow`])
    /// instead of the deadlist shadow.
    ///
    /// Uses the STICKY `VOLUME_FLAG_CLONE_LINEAGE` (set at clone create, never
    /// cleared — survives promotion), mirroring the `drop_volume` Step-3 gate
    /// and `clone_cow_pinners_from`. `parent_vol_ord.is_some()` (the old gate)
    /// misses promoted ex-clones, which is exactly the dense-soak false-premature.
    ///
    /// True iff V itself is clone-lineage OR any OTHER live volume is — page
    /// sharing only arises within a clone lineage, but promotion erases the
    /// parent link, so we cannot cheaply prove an unrelated promoted ex-clone
    /// does not reach V's pages. Over-routing is always safe (reachability is
    /// authoritative and is already computed at every drop) and never
    /// under-routes (any cross-volume share keeps the sticky flag forever).
    fn snapshot_drop_clone_involved(&self, source_volume: &Volume) -> bool {
        use std::sync::atomic::Ordering::Relaxed;
        let flag = crate::manifest::VOLUME_FLAG_CLONE_LINEAGE;
        if source_volume.flags.load(Relaxed) & flag != 0 {
            return true;
        }
        self.volumes
            .read()
            .values()
            .any(|v| v.ord != source_volume.ord && v.flags.load(Relaxed) & flag != 0)
    }

    /// ZFS port Phase 4 S2c — the page-rc-INDEPENDENT free-source for a
    /// CLONE-INVOLVED snapshot drop. The single-volume page-deadlist
    /// (`check_page_deadlist_shadow`) cannot model the cross-volume page
    /// sharing a clone DAG introduces, so the authoritative free-set is the
    /// structural reachability difference
    /// `structural_to_free = { pid ∈ pages : after_refs[pid] == 0 }` —
    /// already computed by the prologue's `collect_paged_refcounts_for_roots`
    /// walk over `roots_after` (every live volume head incl. promoted
    /// ex-clones ∪ all OTHER snapshots' roots), so a page is freed iff S was
    /// its last incoming edge and NO surviving root reaches it. That walk
    /// never reads `l2p_page_rc`, so it survives the S3 page-rc deletion.
    ///
    /// HARD teeth: an INDEPENDENT reachability set-difference
    /// `reachable(S) \ reachable(survivors)` (via `reachable_l2p_pages`, the
    /// `drop_volume` clone path's oracle) must equal `structural_to_free`.
    /// `survivor_roots` is built IDENTICALLY to the prologue's `roots_after`
    /// (R1 — a different root set would false-fire). Premature
    /// (`structural_to_free \ exclusive`: we'd free a survivor-reachable page,
    /// the P0) and missing (`exclusive \ structural_to_free`) are both HARD
    /// `Corruption`. page-rc stays the apply-side bookkeeping decref + the
    /// offline inverted-shadow oracles, not a hot-path read here — the drop
    /// runs quiesced (apply_gate.write + drop_gate.write held, post
    /// force_compact/flush), the no-force-fold-hazard point.
    fn check_clone_drop_reachability_shadow(
        &self,
        id: SnapshotId,
        entry: &SnapshotEntry,
        pages: &[PageId],
        after_refs: &std::collections::BTreeMap<PageId, u32>,
        all_current_roots: &[PageId],
        other_snapshots: &[SnapshotEntry],
    ) -> Result<Vec<PageId>> {
        use std::collections::HashSet;
        let vol = entry.vol_ord;

        // Authoritative free-set: structural reachability difference, page-rc
        // independent (== check_page_deadlist_shadow's structural_to_free).
        let structural_to_free: HashSet<PageId> = pages
            .iter()
            .copied()
            .filter(|pid| after_refs.get(pid).copied().unwrap_or(0) == 0)
            .collect();

        // Independent oracle: reachable(S) \ reachable(survivors). survivor_roots
        // MUST mirror the prologue's `roots_after` (all live heads + all OTHER
        // snapshots' `l2p_shard_roots`) so the two walks see the same graph.
        let mut survivor_roots: Vec<PageId> = all_current_roots.to_vec();
        survivor_roots.extend(
            other_snapshots
                .iter()
                .flat_map(|s| s.l2p_shard_roots.iter().copied()),
        );
        let survivor_reachable =
            crate::verify::reachable_l2p_pages(&self.page_store, &survivor_roots)?;
        let snap_reachable =
            crate::verify::reachable_l2p_pages(&self.page_store, &entry.l2p_shard_roots)?;
        let exclusive: HashSet<PageId> = snap_reachable
            .difference(&survivor_reachable)
            .copied()
            .collect();

        let mut premature: Vec<PageId> =
            structural_to_free.difference(&exclusive).copied().collect();
        if !premature.is_empty() {
            premature.sort_unstable();
            return Err(MetaDbError::Corruption(format!(
                "drop_snapshot {id} (vol {vol}, clone-involved): structural free-set names {} \
                 page(s) still reachable from a surviving root (PREMATURE FREE): {premature:?} \
                 (structural_to_free={} exclusive={})",
                premature.len(),
                structural_to_free.len(),
                exclusive.len(),
            )));
        }
        let mut missing: Vec<PageId> =
            exclusive.difference(&structural_to_free).copied().collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            return Err(MetaDbError::Corruption(format!(
                "drop_snapshot {id} (vol {vol}, clone-involved): reachability frees {} page(s) \
                 the structural free-set omits (COMPLETENESS HOLE): {missing:?} \
                 (structural_to_free={} exclusive={})",
                missing.len(),
                structural_to_free.len(),
                exclusive.len(),
            )));
        }

        // Both directions clean ⇒ structural_to_free == exclusive. Hand it
        // back sorted (deterministic WAL `free_pages`).
        let mut free_pages: Vec<PageId> = structural_to_free.into_iter().collect();
        free_pages.sort_unstable();
        Ok(free_pages)
    }

    /// Test shim: drive [`Db::check_clone_drop_reachability_shadow`] with
    /// caller-crafted `pages`/`after_refs` so the premature/missing detectors
    /// can be exercised directly (the production call site only ever passes a
    /// self-consistent free-set).
    #[cfg(test)]
    pub(in crate::db) fn test_check_clone_drop_reachability_shadow(
        &self,
        id: SnapshotId,
        entry: &SnapshotEntry,
        pages: &[PageId],
        after_refs: &std::collections::BTreeMap<PageId, u32>,
        all_current_roots: &[PageId],
        other_snapshots: &[SnapshotEntry],
    ) -> Result<Vec<PageId>> {
        self.check_clone_drop_reachability_shadow(
            id,
            entry,
            pages,
            after_refs,
            all_current_roots,
            other_snapshots,
        )
    }

    /// Exact free predicate for a `dl_next` death record when dropping S:
    /// the page is freed iff NO surviving snapshot of this volume still
    /// pins it. A snapshot with `created_lsn == c` pins a page that was
    /// live over `[birth, death)` iff `birth <= c < death`. So the record
    /// is freed iff `other_created_sorted` has no entry in `[birth, death)`.
    /// (S itself is excluded from that list, and a record in S_next's
    /// deadlist always satisfies `birth <= S.created < death`, so S always
    /// pinned it — this predicate answers "does anyone ELSE pin it".)
    fn dl_record_freed(other_created_sorted: &[Lsn], birth: Lsn, death: Lsn) -> bool {
        // First created_lsn >= birth.
        let i = other_created_sorted.partition_point(|&c| c < birth);
        // Pinned iff that entry exists and is strictly below death.
        !(i < other_created_sorted.len() && other_created_sorted[i] < death)
    }

    /// ZFS port Phase 2 (2a, SHADOW): cross-check the page-deadlist model
    /// against the structural ground truth that page-rc still drives the
    /// real free from. `structural_to_free` = the L2P pages this drop
    /// releases (lose their last incoming edge: in `pages` with
    /// `after_refs == 0`). The deadlist side follows ZFS
    /// `process_old_deadlist` (dsl_destroy.c): destroying S frees from
    /// **S_next's** deadlist (deaths in `(S, S_next]`) the entries born
    /// after S_prev — NOT S's own chain (deaths in `(S_prev, S]`, which S
    /// never referenced). An entry is freed iff `birth > S_prev.created`
    /// (born after the previous surviving snapshot, so only S pinned it);
    /// else it is still pinned by S_prev and merges forward. `S_next` is the
    /// youngest surviving snapshot of this volume newer than S, or the live
    /// HEAD when S is the youngest (its deaths sit in the volume's drained
    /// segment chain + the not-yet-drained in-memory accumulator).
    ///
    /// Scoped to non-clone volumes (clone DAGs add cross-volume sharing the
    /// single-vol invariant doesn't model — R5/Phase 3). Phase 2b: BOTH
    /// directions are HARD `Corruption` — premature (a deadlist entry mapping
    /// to a still-referenced page) and missing (a structurally-freed page the
    /// deadlist did not predict). With the MERGE maintaining the inheritor
    /// chains the two sets are provably equal at every drop, the invariant
    /// Phase 4 needs to make the deadlist the sole free source.
    /// Returns the deadlist-derived free-set (`deadlist_to_free`). With both
    /// shadow directions HARD, on `Ok` it equals `structural_to_free`, so the
    /// ZFS port Phase 4 S2 flip inlines it as the `DropSnapshot.free_pages`
    /// authoritative, page-rc-independent free-set.
    fn check_page_deadlist_shadow(
        &self,
        id: SnapshotId,
        entry: &SnapshotEntry,
        pages: &[PageId],
        after_refs: &std::collections::BTreeMap<PageId, u32>,
        ctx: &InheritorContext,
    ) -> Result<Vec<PageId>> {
        let vol = entry.vol_ord;
        let s_prev_created = ctx.s_prev_created;
        let structural_to_free: std::collections::HashSet<PageId> = pages
            .iter()
            .copied()
            .filter(|pid| after_refs.get(pid).copied().unwrap_or(0) == 0)
            .collect();
        // FREE partition of the inheritor's records: exact — a record is
        // freed iff no OTHER surviving snapshot of this volume still pins
        // its page over `[birth, death)`. See `dl_record_freed` /
        // `InheritorContext::other_created_sorted`.
        let deadlist_to_free: std::collections::HashSet<PageId> = ctx
            .dl_next
            .iter()
            .filter(|r| Self::dl_record_freed(&ctx.other_created_sorted, r.birth_lsn, r.death_lsn))
            .map(|r| r.pba)
            .collect();
        // PREMATURE-FREE (P0) direction: a deadlist entry that maps to a
        // page the structural graph still references. The page-deadlist read
        // above is ZFS-faithful, so this set must be empty — a non-empty
        // premature set is a real soundness bug (the exact class the whole
        // port exists to kill), so it is a HARD `Corruption`.
        let mut premature: Vec<PageId> =
            deadlist_to_free.difference(&structural_to_free).copied().collect();
        if !premature.is_empty() {
            premature.sort();
            return Err(MetaDbError::Corruption(format!(
                "drop_snapshot {id} (vol {vol}): page-deadlist would free {} page(s) \
                 the structural graph still references (PREMATURE FREE): {premature:?} \
                 (s_prev={s_prev_created} deadlist_to_free={} structural_to_free={})",
                premature.len(),
                deadlist_to_free.len(),
                structural_to_free.len(),
            )));
        }
        // MISSING (completeness) direction: a structurally-freed page the
        // deadlist did not predict. With the cross-drop MERGE maintaining
        // every inheritor chain, this set is empty across all churn/drop
        // tests (panic-probe verified) and the snapshot-churn soak's verify
        // cycles. Phase 2b makes it a HARD `Corruption` alongside premature:
        // the deadlist now PROVABLY equals the page-rc free-set at every
        // drop, which is the invariant Phase 4 relies on when it deletes
        // page-rc and the deadlist becomes the sole free source.
        let mut missing: Vec<PageId> =
            structural_to_free.difference(&deadlist_to_free).copied().collect();
        if !missing.is_empty() {
            missing.sort();
            return Err(MetaDbError::Corruption(format!(
                "drop_snapshot {id} (vol {vol}): page-deadlist MISSED {} page(s) the \
                 structural graph frees (COMPLETENESS HOLE): {missing:?} \
                 (s_prev={s_prev_created} deadlist_to_free={} structural_to_free={})",
                missing.len(),
                deadlist_to_free.len(),
                structural_to_free.len(),
            )));
        }
        // Both directions clean ⇒ deadlist_to_free == structural_to_free.
        // Hand it back sorted (deterministic WAL `free_pages`) as the S2
        // authoritative free-set.
        let mut free_pages: Vec<PageId> = deadlist_to_free.into_iter().collect();
        free_pages.sort_unstable();
        Ok(free_pages)
    }

    /// Crash-recovery completeness (S2 cutover prerequisite, "G1"): seal a
    /// volume's in-memory page-deadlist accumulator (COW deaths drained into
    /// it by `force_compact_l2p_buffers`, all with `death_lsn <= cut`) into a
    /// durable segment EXTENDING the HEAD chain, and bump the volume's HEAD
    /// anchors. `drop_snapshot` calls this for every volume BEFORE the commit
    /// that advances `checkpoint_lsn` — otherwise that commit makes the
    /// post-`force_compact` roots durable (which structurally imply those
    /// pages free) while their death records sit only in volatile RAM, lost on
    /// a hard crash and un-re-recordable on replay (the durable root already
    /// folded the COW → the replayed op generation-skips → no witness). Setting
    /// the atomics here lets `refresh_manifest_from_locked` fold the new
    /// anchors into the SAME commit (it reads them, see `refresh_manifest_entries`).
    /// Mirrors the flush seal (`drain_up_to_lsn` + `build_segment_pages` +
    /// anchor bump). Restores the accumulator on IO failure so the aborted
    /// drop is retryable.
    fn seal_page_dead_list_accumulator(&self, vol: &Volume, cut: Lsn) -> Result<()> {
        use std::sync::atomic::Ordering;
        // H1: drain EVERY shard accumulator at the uniform `cut`. Sound here
        // (unlike the steady flush, which must use a per-shard bound) because
        // drop_snapshot's `force_compact` folded every shard to
        // `last_applied == cut` under `drop_gate.write`, so each shard's durable
        // root reflects all its deaths <= cut. The per-shard drains merge into
        // ONE segment (single chain); `provenance` lets the IO-failure path
        // restore each sub-vec to its own accumulator so the drop is retryable.
        let mut records: Vec<crate::deadlist::DeadRecord> = Vec::new();
        let mut provenance: Vec<(usize, Vec<crate::deadlist::DeadRecord>)> = Vec::new();
        for (s_idx, acc) in vol.page_dead_list.iter().enumerate() {
            let recs = acc.drain_up_to_lsn(cut);
            if !recs.is_empty() {
                records.extend_from_slice(&recs);
                provenance.push((s_idx, recs));
            }
        }
        if records.is_empty() {
            return Ok(());
        }
        let old_tail = vol.page_dead_list_tail_pid.load(Ordering::Acquire);
        let old_head = vol.page_dead_list_head_pid.load(Ordering::Acquire);
        let sealed = (|| -> Result<PageId> {
            let page_count = crate::deadlist::segment_pages_for(records.len());
            let start = self.page_store.allocate_run(page_count)?;
            let pages = crate::deadlist::build_segment_pages(start, &records, old_tail, cut);
            let sealed_pages: Vec<(PageId, Arc<crate::page::Page>)> =
                pages.into_iter().map(|(p, pg)| (p, Arc::new(pg))).collect();
            self.page_store.write_sealed_page_runs(sealed_pages)?;
            // Durable before the anchor bump so the about-to-be-committed
            // anchor can never reference an unsynced segment.
            self.page_store.sync()?;
            Ok(start)
        })();
        match sealed {
            Ok(start) => {
                let new_head = if old_head == crate::types::NULL_PAGE {
                    start
                } else {
                    old_head
                };
                vol.page_dead_list_head_pid.store(new_head, Ordering::Release);
                vol.page_dead_list_tail_pid.store(start, Ordering::Release);
                Ok(())
            }
            Err(e) => {
                // Restore each shard's drained records to its OWN accumulator so
                // the aborted drop can be retried without losing or mis-binding
                // them (a flat restore into shard 0 would corrupt the per-shard
                // seal bound on the next attempt).
                for (s_idx, recs) in provenance {
                    vol.page_dead_list[s_idx].restore_front(recs);
                }
                Err(e)
            }
        }
    }

    /// H1 (ZFS-faithful): seal EVERY listed volume's page-deadlist accumulator
    /// into its durable HEAD chain at `cut`, so a checkpoint-advancing manifest
    /// commit that makes these volumes' L2P roots durable ALSO makes the
    /// page-deaths those roots imply durable IN THE SAME COMMIT (the bumped
    /// HEAD anchors fold via `refresh_manifest_from_locked` /
    /// `refresh_manifest_entries`). This is the generalization of the
    /// `drop_snapshot` G1 seal: EVERY lifecycle op that `force_compact`s the
    /// buffer before a checkpoint-advancing commit (`drop_volume`, recovery's
    /// post-replay commit, in-place restore) MUST call this with
    /// `cut = checkpoint_lsn` first — otherwise a folded death becomes
    /// durable-dead with no durable death record and a later `drop_snapshot`
    /// fires a COMPLETENESS HOLE (the produced-then-lost crash-recovery bug).
    /// `flush` (per-shard drain-under-guard) and `drop_snapshot` already seal
    /// inline; this serves the other sites. Call BEFORE the manifest commit so
    /// the anchors fold into the same commit; on IO failure each accumulator is
    /// restored (per-shard) so the operation stays retryable.
    pub(in crate::db) fn seal_all_page_dead_lists(
        &self,
        volumes: &[std::sync::Arc<Volume>],
        cut: Lsn,
    ) -> Result<()> {
        for vol in volumes {
            self.seal_page_dead_list_accumulator(vol, cut)?;
        }
        Ok(())
    }

    /// H1 tripwire: after a checkpoint-advancing commit, no volume may retain a
    /// page-death with `death_lsn <= checkpoint_lsn` — that exact state (durable
    /// roots imply a free the durable deadlist never sealed) is the
    /// crash-recovery completeness bug. Debug-only; converts the
    /// (multi-hour-soak-only) latent leak into a deterministic in-process test
    /// failure so any future checkpoint-advancing commit that forgets to seal
    /// fails immediately.
    #[cfg(debug_assertions)]
    pub(in crate::db) fn debug_assert_page_deaths_sealed(
        &self,
        volumes: &[std::sync::Arc<Volume>],
        checkpoint_lsn: Lsn,
    ) {
        for vol in volumes {
            for (s, acc) in vol.page_dead_list.iter().enumerate() {
                debug_assert!(
                    acc.peek().iter().all(|r| r.death_lsn > checkpoint_lsn),
                    "checkpoint_lsn={checkpoint_lsn} committed but volume {} shard {s} retains an \
                     unsealed page-death death_lsn<=checkpoint_lsn (deadlist-seal gap): {:?}",
                    vol.ord,
                    acc.peek().iter().map(|r| r.death_lsn).filter(|&d| d <= checkpoint_lsn).collect::<Vec<_>>(),
                );
            }
        }
    }

    /// ZFS port Phase 2a — plan the page-deadlist MERGE for dropping
    /// snapshot S, following ZFS `process_old_deadlist` (dsl_destroy.c).
    /// Builds the new single-segment chain for the entity that inherits S's
    /// deaths (`ctx.target` — S_next, or the live HEAD when S is youngest).
    ///
    /// `carried = DL_S ∪ {r ∈ DL_next : still pinned by a surviving snap}`:
    /// S's own chain (deaths in `(S_prev, S]`, all still pinned by S_prev or
    /// an older snap) plus the S_next entries some surviving snapshot still
    /// pins (exact `dl_record_freed` predicate — kept iff a surviving
    /// `created_lsn` lies in `[birth, death)`). The freed entries are dropped
    /// — page-rc already released those pages; the records simply vanish.
    /// Keeping by the exact predicate (not the old `birth <= S_prev` proxy)
    /// is what preserves records a same-`created_lsn` sibling, or a snapshot
    /// only reachable through the chain, still needs — the omission that
    /// produced both premature frees (tie) and completeness holes (chain).
    /// The carried set is written to ONE fresh durable segment (head==tail,
    /// `prev=NULL`), so the inheritor's chain collapses to a single segment.
    /// All IO (chain reads + segment write + sync) happens here, outside the
    /// `manifest_state` lock; the caller applies the returned anchors in the
    /// same critical section that removes S. The old S_next / S segment
    /// chains become orphans once that (next-flush) manifest commit lands and
    /// are swept by `reclaim_orphan_pages` on the following open — the same
    /// deferral the dropped snapshot's `l2p_roots_page` already rides.
    fn plan_page_deadlist_merge(
        &self,
        entry: &SnapshotEntry,
        ctx: &InheritorContext,
        flush_lsn: Lsn,
    ) -> Result<PageDeadlistMerge> {
        // carried = DL_S ++ KEEP(DL_next). DL_S deaths all fall in
        // (S_prev, S]; KEEP deaths in (S, S_next] — disjoint ranges, so no
        // dedup is needed. Sort by death_lsn for a deterministic segment.
        let mut carried = crate::deadlist::read_chain_records(entry.page_dead_list_tail_pid, |p| {
            self.page_store.read_page(p)
        })?;
        carried.extend(
            ctx.dl_next
                .iter()
                .copied()
                .filter(|r| !Self::dl_record_freed(&ctx.other_created_sorted, r.birth_lsn, r.death_lsn)),
        );
        carried.sort_by_key(|r| (r.death_lsn, r.pba, r.birth_lsn));

        let anchor = if carried.is_empty() {
            crate::types::NULL_PAGE
        } else {
            let page_count = crate::deadlist::segment_pages_for(carried.len());
            let start = self.page_store.allocate_run(page_count)?;
            let pages = crate::deadlist::build_segment_pages(
                start,
                &carried,
                crate::types::NULL_PAGE,
                flush_lsn,
            );
            let sealed: Vec<(PageId, Arc<crate::page::Page>)> =
                pages.into_iter().map(|(p, pg)| (p, Arc::new(pg))).collect();
            self.page_store.write_sealed_page_runs(sealed)?;
            // Durable now so the not-yet-committed anchor (persisted by the
            // next flush) can never reference an unsynced segment.
            self.page_store.sync()?;
            // Single segment: its first page is both head and tail.
            start
        };
        Ok(PageDeadlistMerge {
            target: ctx.target,
            anchor,
        })
    }

    /// ZFS `dsl_sync_task` (path B) — called AFTER the L2P `begin_checkpoint`
    /// loop (tree guards released), inside the sync cycle's manifest window.
    /// For each queued [`SyncTaskOp::TakeSnapshot`] targeting `txg`:
    /// capacity-probe + assign id + write its SnapshotRoots page + build the
    /// `SnapshotEntry` (into `committed_entry`) the FIRST time it is seen.
    ///
    /// ZFS port S3: per-L2P-page refcounting was deleted, so this no longer
    /// collects per-page-rc-shard root pids to force-incref. The take is
    /// manifest-only (no lifecycle journal): a crash before the manifest
    /// commit simply loses the (never-committed) snapshot, and the snapshot's
    /// pages stay referenced because the source volume's tree still points at
    /// the shared roots (COW preserves them on the next write).
    pub(crate) fn prepare_pending_snapshot_entries(
        &self,
        txg: crate::types::Txg,
        snapshot_roots: &std::collections::HashMap<VolumeOrdinal, Vec<PageId>>,
        snapshot_watermarks: &std::collections::HashMap<VolumeOrdinal, Lsn>,
    ) -> Result<()> {
        let mut tasks = self.pending_sync_tasks.lock();
        for task in tasks.iter_mut().filter(|t| t.target_txg == txg) {
            // Build the committed entry once; a retry with `committed_entry`
            // already set is a no-op (the snapshot definition is stable).
            if task.committed_entry.is_none() {
                let SyncTaskOp::TakeSnapshot { vol_ord } = &task.op;
                let vol_ord = *vol_ord;
                let roots = snapshot_roots.get(&vol_ord).cloned().unwrap_or_default();
                if roots.is_empty() {
                    // Force-selected volume must have captured roots; empty ⇒
                    // it vanished / had no shards — reject before side effects.
                    *task.result.lock() = Some(Err(MetaDbError::InvalidArgument(format!(
                        "take_snapshot: no roots captured for volume ordinal {vol_ord}"
                    ))));
                    continue;
                }
                // `created_lsn` = the most recently applied op's lsn (a safe
                // UPPER bound on the captured roots' content for birth/death
                // suppression + lineage). Same monotone space as lite's
                // `*last_applied_lsn.lock()`.
                let created_lsn = *self.last_applied_lsn.lock();
                // v21 (S1): the EXACT fold-watermark of the captured roots
                // (`max(root.birth_lsn)`), computed under each shard's
                // `tree.write()` in `run_sync_cycle_body` and passed in here.
                // Feeds the birth COW-kill oracle (NOT `created_lsn`). See
                // `SnapshotEntry::capture_watermark` / `youngest_snap_lsn`.
                let capture_watermark =
                    snapshot_watermarks.get(&vol_ord).copied().unwrap_or(created_lsn);
                // Assign the id + capacity-probe under `manifest_state`.
                let id = {
                    let mut ms = self.manifest_state.lock();
                    let id = ms.manifest.next_snapshot_id;
                    let mut probe = ms.manifest.clone();
                    probe.snapshots.push(SnapshotEntry {
                        id,
                        vol_ord,
                        l2p_roots_page: FIRST_DATA_PAGE,
                        created_lsn,
                        l2p_shard_roots: roots.clone().into_boxed_slice(),
                        // Worst-case probe: a non-NULL tail makes the row
                        // the same size as a sealed one (the field is
                        // fixed-width, so this is cosmetic, but keep it
                        // honest).
                        page_dead_list_tail_pid: crate::types::NULL_PAGE,
                        // Fixed-width field; value cosmetic for the probe.
                        capture_watermark,
                    });
                    probe.next_snapshot_id = id.saturating_add(1);
                    if let Err(e) = probe.check_encodable() {
                        *task.result.lock() = Some(Err(e));
                        continue;
                    }
                    ms.manifest.next_snapshot_id = id.saturating_add(1);
                    id
                };
                // Roots are durable: this cycle's IO phase writes + syncs the
                // sealed L2P pages before the manifest commit.
                let l2p_roots_page =
                    write_snapshot_roots_page(&self.page_store, &roots, created_lsn)?;
                task.committed_entry = Some(SnapshotEntry {
                    id,
                    vol_ord,
                    l2p_roots_page,
                    created_lsn,
                    l2p_shard_roots: roots.clone().into_boxed_slice(),
                    // v18: filled by the page-deadlist seal step; an empty
                    // chain (no page-deaths yet) stays `NULL_PAGE`.
                    page_dead_list_tail_pid: crate::types::NULL_PAGE,
                    // v21 (S1): fold-watermark of these captured roots.
                    capture_watermark,
                });
            }
        }
        Ok(())
    }

    /// ZFS `dsl_sync_task` — phase A4, called in
    /// [`Db::run_sync_cycle_body`]'s manifest window (under
    /// `apply_gate.write()` + `manifest_state.lock()`). Insert each
    /// processed take's `SnapshotEntry` into the manifest about to be
    /// committed. Id-idempotent on the cycle's abort-retry.
    pub(crate) fn add_pending_snapshot_entries(
        &self,
        manifest: &mut crate::manifest::Manifest,
        txg: crate::types::Txg,
    ) {
        let tasks = self.pending_sync_tasks.lock();
        for task in tasks.iter().filter(|t| t.target_txg == txg) {
            if let Some(entry) = &task.committed_entry {
                if !manifest.snapshots.iter().any(|s| s.id == entry.id) {
                    let next = entry.id.saturating_add(1);
                    manifest.snapshots.push(entry.clone());
                    manifest.next_snapshot_id = manifest.next_snapshot_id.max(next);
                }
            }
        }
    }

    /// ZFS port Phase 2 — seal each pending take's page-deadlist. Called
    /// in `run_sync_cycle_body`'s manifest window AFTER the page dead-list
    /// override map is built and BEFORE `refresh_manifest_from_checkpoints`.
    ///
    /// The new snapshot inherits the head volume's page-deadlist chain as
    /// it stands after this cycle's drain — the chain tail is the override
    /// tail when a segment was written this round, else the pre-existing
    /// durable anchor. The head then resets to a fresh empty chain (so
    /// deaths after this snapshot accumulate into the NEXT snapshot's
    /// deadlist), mirroring ZFS handing the head's `ds_deadlist` to the
    /// new snapshot. The reset is split across the commit boundary like
    /// the normal anchor promotion: this writes `(NULL,NULL)` into the
    /// override map so `refresh` stamps NULL manifest anchors, and returns
    /// the volume ordinals whose in-memory atomics the caller must reset
    /// to `NULL_PAGE` post-commit. The inherited tail is stamped onto the
    /// pending `SnapshotEntry` (committed atomically by
    /// `add_pending_snapshot_entries`).
    pub(crate) fn seal_pending_snapshot_page_deadlists(
        &self,
        txg: crate::types::Txg,
        volumes: &[Arc<Volume>],
        page_dead_list_overrides: &mut std::collections::HashMap<VolumeOrdinal, (PageId, PageId)>,
    ) -> Vec<VolumeOrdinal> {
        use std::sync::atomic::Ordering;
        let mut resets: Vec<VolumeOrdinal> = Vec::new();
        let mut tasks = self.pending_sync_tasks.lock();
        for task in tasks.iter_mut().filter(|t| t.target_txg == txg) {
            let Some(entry) = task.committed_entry.as_mut() else {
                continue;
            };
            let SyncTaskOp::TakeSnapshot { vol_ord } = &task.op;
            let vol_ord = *vol_ord;
            let inherited_tail = page_dead_list_overrides
                .get(&vol_ord)
                .map(|(_, t)| *t)
                .unwrap_or_else(|| {
                    volumes
                        .iter()
                        .find(|v| v.ord == vol_ord)
                        .map(|v| v.page_dead_list_tail_pid.load(Ordering::Acquire))
                        .unwrap_or(crate::types::NULL_PAGE)
                });
            entry.page_dead_list_tail_pid = inherited_tail;
            page_dead_list_overrides
                .insert(vol_ord, (crate::types::NULL_PAGE, crate::types::NULL_PAGE));
            resets.push(vol_ord);
        }
        resets
    }

    /// ZFS `dsl_sync_task` — post-manifest-commit. The manifest with the new
    /// `SnapshotEntry`s is durable, so report `Ok(id)` to each queued
    /// `take_snapshot` caller, warm the per-volume `SnapInfo` cache, and dequeue.
    /// Capacity-rejected tasks (`result` already `Err`) are dequeued too.
    ///
    /// ZFS port S3: the caller (`run_sync_cycle_body`) now invokes this INSIDE
    /// the `apply_gate.write()` window (after the manifest commit, before the
    /// drop) — the `SnapInfo` cache warm MUST be visible to every apply that
    /// runs once the gate releases, or a concurrent COW could recycle the new
    /// snapshot's root against a cold cache (page-rc floor that masked this is
    /// gone). Takes only the `snap_info_cache` + `pending_sync_tasks` leaf
    /// mutexes (no `manifest_state`); lock order `apply_gate -> snap_info_cache`
    /// matches the read path.
    pub(crate) fn finish_pending_snapshots(&self, txg: crate::types::Txg) {
        let mut cache = self.snap_info_cache.lock();
        let mut tasks = self.pending_sync_tasks.lock();
        tasks.retain(|t| {
            if t.target_txg != txg {
                return true;
            }
            if let Some(entry) = &t.committed_entry {
                cache.entry(entry.vol_ord).or_default().push(SnapInfo {
                    created_lsn: entry.created_lsn,
                    capture_watermark: entry.capture_watermark,
                    l2p_shard_roots: entry.l2p_shard_roots.clone(),
                });
                *t.result.lock() = Some(Ok(entry.id));
                return false;
            }
            // Capacity-rejected (result already Err) → dequeue. Anything
            // else (unprocessed) stays queued for a later cycle.
            t.result.lock().is_none()
        });
    }
}

/// Result of [`Db::restore_volume_to_snapshot`].
#[derive(Clone, Debug)]
pub struct RestoreReport {
    /// Snapshot the volume was restored to.
    pub snapshot_id: SnapshotId,
    /// Volume that was restored.
    pub vol_ord: VolumeOrdinal,
    /// Count of LBAs re-pointed back to their snapshot value.
    pub lbas_remapped: u64,
    /// Count of LBAs deleted (live now, absent in the snapshot).
    pub lbas_deleted: u64,
    /// LSN of the final rollback commit (equals the prior `last_applied_lsn`
    /// when the diff was empty / nothing to do).
    pub lsn: Lsn,
}

/// Result of [`Db::drop_snapshot`].
#[derive(Clone, Debug)]
pub struct DropReport {
    /// Id of the snapshot that was dropped.
    pub snapshot_id: SnapshotId,
    /// Every value stored in leaves that were uniquely owned by this
    /// snapshot.
    pub freed_leaf_values: Vec<L2pValue>,
    /// Number of metadb pages released back to the page store.
    pub pages_freed: usize,
    /// SPEC §3.3 leaf-rc-suppress compensation output: every pba whose
    /// refcount hit zero during the drop. Adapter hands these to
    /// [`Db::cleanup_dedup_for_dead_pbas`] and its `SpaceAllocator`.
    pub freed_pbas: Vec<Pba>,
}

/// Read-only view of the tree as it existed when a snapshot was taken.
pub struct SnapshotView<'a> {
    db: &'a Db,
    entry: SnapshotEntry,
    _guard: RwLockReadGuard<'a, ()>,
}

impl<'a> SnapshotView<'a> {
    /// Snapshot id this view is bound to.
    pub fn id(&self) -> SnapshotId {
        self.entry.id
    }

    /// LSN at which the snapshot was taken.
    pub fn created_lsn(&self) -> Lsn {
        self.entry.created_lsn
    }

    /// Ordinal of the volume this snapshot captures.
    pub fn vol_ord(&self) -> VolumeOrdinal {
        self.entry.vol_ord
    }

    /// Point lookup as of the snapshot's LSN. Lock-free via
    /// [`crate::paged::ReadView`] rooted at the snapshot's per-shard
    /// pid; the overlay is empty because `take_snapshot` always
    /// flushes first. The epoch pin keeps deferred-reclaim from
    /// physically freeing pages we're walking; `snapshot_views.read()`
    /// (held by `_guard`) excludes `drop_snapshot` from removing the
    /// snapshot entry while a view is alive.
    pub fn get(&self, lba: Lba) -> Result<Option<L2pValue>> {
        let _pin = self.db.page_store.epoch().pin();
        let volume = self.db.volume(self.entry.vol_ord)?;
        let sid = shard_for_key_l2p(&volume.shards, lba);
        let snap_root = self.entry.l2p_shard_roots[sid];
        let page_cache = volume.shards[sid].read_view.read().page_cache().clone();
        let snap_level = crate::paged::ReadView::read_root_level(&page_cache, snap_root)?;
        let snap_view = crate::paged::ReadView::new(
            snap_root,
            snap_level,
            crate::paged::ReadOverlay::default(),
            page_cache,
        );
        snap_view.get(lba)
    }

    /// Range scan as of the snapshot's LSN.
    pub fn range<R: RangeBounds<Lba>>(&self, range: R) -> Result<DbRangeIter> {
        self.db.collect_range_for_roots(
            self.entry.vol_ord,
            &self.entry.l2p_shard_roots,
            OwnedRange::new(range),
        )
    }
}
