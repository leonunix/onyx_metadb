use super::*;

/// Max rollback ops packed into one restore transaction. Diffs at or below
/// this size restore atomically in a single WAL record; larger diffs chunk
/// across transactions (each atomic, the whole restore re-runnable).
const RESTORE_MAX_OPS_PER_TX: usize = 16_384;

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
            self.txg.wait_until_synced(target);
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
    /// decref's whole-page rc write could not be clobbered. A3 moved the
    /// page refcount into the [`L2pPageRc`](crate::l2p_page_rc) array:
    /// `apply_drop_snapshot_pages` now `stage`s `-1` deltas and gates the
    /// irreversible free on a fold-consistent `get_consistent` read (R2),
    /// so there is no whole-page rc write left to clobber and no need to
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
        // A3: fold the page-rc array so the manifest committed below
        // records a durable `l2p_page_rc_durable_seq = checkpoint_lsn`
        // (the prior commits' page-rc deltas; this drop's own decrefs are
        // at lsn > checkpoint_lsn and ride the WAL-replayed apply).
        self.l2p_page_rc.flush()?;

        let checkpoint_lsn = *self.last_applied_lsn.lock();
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
            &self.l2p_page_rc,
            lsn,
            _txg_guard.txg(),
            &pages,
            &pba_decrefs,
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

        {
            let mut mstate = self.manifest_state.lock();
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

    /// ZFS `dsl_sync_task` (path B) — called AFTER the L2P `begin_checkpoint`
    /// loop (tree guards released) and BEFORE the page-rc `begin_checkpoint`.
    /// For each queued [`SyncTaskOp::TakeSnapshot`] targeting `txg`:
    /// capacity-probe + assign id + write its SnapshotRoots page + build the
    /// `SnapshotEntry` (into `committed_entry`) the FIRST time it is seen;
    /// then, every attempt, collect its root pids grouped by page-rc shard.
    /// Returns that per-page-rc-shard set of pids to FORCE-incref this cycle
    /// (see [`crate::refcount::RcShard::begin_checkpoint_with_increfs`]).
    ///
    /// The force-incref always applies and never bumps a page generation,
    /// so it needs NO reserved lsn. Idempotency across the cycle's
    /// abort-retry rides the page-rc checkpoint rollback: an aborted cycle
    /// rolls back the fold (nothing durable), and the NEXT attempt
    /// RECOMPUTES the increfs here — so the pids are collected every call,
    /// even when `committed_entry` is already set (the entry/probe/roots-page
    /// are done once; only the fold repeats). The take is manifest-only
    /// (no lifecycle journal), so a crash before the manifest commit simply
    /// loses the (never-committed) snapshot.
    pub(crate) fn stage_pending_snapshot_increfs(
        &self,
        txg: crate::types::Txg,
        snapshot_roots: &std::collections::HashMap<VolumeOrdinal, Vec<PageId>>,
    ) -> Result<Vec<Vec<Pba>>> {
        let mut force_increfs: Vec<Vec<Pba>> =
            (0..self.l2p_page_rc.shard_count()).map(|_| Vec::new()).collect();
        let mut tasks = self.pending_sync_tasks.lock();
        for task in tasks.iter_mut().filter(|t| t.target_txg == txg) {
            // Roots: reuse the already-built entry's on a retry (stable
            // snapshot definition); otherwise process the task fresh.
            let roots: Vec<PageId> = if let Some(entry) = &task.committed_entry {
                entry.l2p_shard_roots.to_vec()
            } else {
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
                });
                roots
            };
            // Collect the force-increfs (every attempt — see the doc).
            for &root in &roots {
                if root != crate::types::NULL_PAGE {
                    force_increfs[self.l2p_page_rc.shard_for(root)].push(root);
                }
            }
        }
        Ok(force_increfs)
    }

    /// ZFS `dsl_sync_task` — phase A4, called in
    /// [`Db::run_sync_cycle_body`]'s manifest window (under
    /// `apply_gate.write()` + `manifest_state.lock()`). Insert each
    /// processed take's `SnapshotEntry` into the manifest about to be
    /// committed, so it lands atomically with the page-rc fold (A3) that
    /// made the incref durable. Id-idempotent on the cycle's abort-retry.
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

    /// ZFS `dsl_sync_task` — post-commit. The manifest with the
    /// new `SnapshotEntry`s is durable, so report `Ok(id)` to each queued
    /// `take_snapshot` caller, warm the per-volume `SnapInfo` cache, and
    /// dequeue. Capacity-rejected tasks (`result` already `Err`) are
    /// dequeued too. Needs neither `apply_gate` nor `manifest_state`.
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
