use super::*;

impl Db {
    // -------- snapshot operations -----------------------------------------

    /// Take a snapshot of volume `vol_ord`'s L2P state. Returns the new
    /// snapshot id. Persisted immediately via a manifest commit. Unknown
    /// volume ordinals surface as `InvalidArgument`.
    ///
    /// Refcount state is global (Phase 6.5b retired per-snapshot refcount
    /// roots), so the snapshot only captures the target volume's L2P
    /// shard roots + an incref on each of them so the snapshot's view
    /// outlives subsequent COW writes on the target volume.
    ///
    /// Takes `apply_gate.write()` so `last_applied_lsn` and the shard
    /// roots we sample below describe the same LSN point. Holds shard
    /// mutexes for every volume for the flush-before-manifest-commit
    /// step — checkpoint_lsn advances across all volumes, so every
    /// volume's dirty pages must be on disk before the commit fsyncs
    /// the manifest slot.
    pub fn take_snapshot(&self, vol_ord: VolumeOrdinal) -> Result<SnapshotId> {
        // Exclude in-flight apply phases so `last_applied_lsn` and the
        // per-shard roots we sample below describe the same LSN point.
        let _apply_guard = self.apply_gate.write();
        // Resolve the target volume before touching manifest state so an
        // unknown ordinal short-circuits with a clean `InvalidArgument`.
        let target = self.volume(vol_ord)?;
        let mut manifest_state = self.manifest_state.lock();
        let volumes = self.volumes_snapshot();
        let mut l2p_guards = lock_all_l2p_shards_for(&volumes);
        let mut refcount_guards = self.lock_all_refcount_shards();

        // Locate the contiguous range of `l2p_guards` that belongs to
        // `target`. `lock_all_l2p_shards_for` iterates `volumes` in
        // ordinal order, so we walk preceding volumes and sum their
        // shard counts.
        let mut target_start = 0usize;
        for vol in &volumes {
            if vol.ord == vol_ord {
                break;
            }
            target_start += vol.shards.len();
        }
        let target_end = target_start + target.shards.len();

        let id = manifest_state.manifest.next_snapshot_id;
        let next_snapshot_id = id
            .checked_add(1)
            .ok_or_else(|| MetaDbError::Corruption("snapshot id overflow".into()))?;

        // Sample roots without incref'ing. The actual incref lands only
        // after the projected manifest passes a dry-run encode below —
        // see commit-ordering rationale on the pre-check.
        //
        // Phase 6.5b: refcount is a running tally, not point-in-time
        // state. Snapshots only capture L2P. We still hold refcount
        // guards for `max_generation_from_two_groups` and
        // `refresh_manifest_from_locked`, but skip the per-tree
        // snapshot incref.
        let l2p_roots: Vec<PageId> = l2p_guards[target_start..target_end]
            .iter()
            .map(|tree| tree.root())
            .collect();
        // `created_lsn` is the WAL lsn of the most recently applied op
        // at snap time. Birth/death LSN suppression compares
        // `pba.birth_lsn` (also WAL lsn) against this value, so both
        // sides must live in the same monotonic space. Held under
        // apply_gate.write so no concurrent commit advances
        // `last_applied_lsn` mid-sample.
        let created_lsn = *self.last_applied_lsn.lock();

        // Bring the manifest up to the version we intend to commit,
        // minus the new snapshot entry. Running prepare/refresh first
        // means the capacity pre-check below sees the exact dedup /
        // volume layout `encode()` will see at commit time.
        let dedup_update =
            self.prepare_dedup_manifest_update(&mut manifest_state.manifest, created_lsn)?;
        self.refresh_manifest_from_locked(
            &mut manifest_state.manifest,
            &volumes,
            &l2p_guards,
            &refcount_guards,
        )?;

        // Pre-check: does the projected manifest (with the new snapshot
        // entry appended) still fit in one page? Failures after this
        // point would persist shard-root refcount bumps without a
        // matching snapshot entry — offline verify then reports
        // orphan rc. Why: `manifest_state.store.commit(...)` below calls
        // `Manifest::encode`, which rejects a too-full snapshot table
        // with `InvalidArgument`. Running the same encode against a
        // probe page here turns that failure into a clean early return
        // before any irreversible side effect (incref / page write /
        // shard flush). Use a non-NULL placeholder for `l2p_roots_page`
        // — encode only rejects `NULL_PAGE`, any other value passes.
        {
            let mut probe = manifest_state.manifest.clone();
            probe.checkpoint_lsn = *self.last_applied_lsn.lock();
            probe.snapshots.push(SnapshotEntry {
                id,
                vol_ord,
                l2p_roots_page: FIRST_DATA_PAGE,
                created_lsn,
                l2p_shard_roots: l2p_roots.clone().into_boxed_slice(),
            });
            probe.next_snapshot_id = next_snapshot_id;
            probe.check_encodable()?;
        }

        // Pre-check passed; safe to make irreversible changes.
        let l2p_roots_page = write_snapshot_roots_page(&self.page_store, &l2p_roots, created_lsn)?;
        for tree in &mut l2p_guards[target_start..target_end] {
            tree.incref_root_for_snapshot()?;
        }
        flush_locked_l2p_shards(&mut l2p_guards)?;
        self.flush_locked_refcount_shards(&mut refcount_guards)?;

        manifest_state.manifest.checkpoint_lsn = *self.last_applied_lsn.lock();
        let snap_roots: Box<[PageId]> = l2p_roots.into_boxed_slice();
        manifest_state.manifest.snapshots.push(SnapshotEntry {
            id,
            vol_ord,
            l2p_roots_page,
            created_lsn,
            l2p_shard_roots: snap_roots.clone(),
        });
        manifest_state.manifest.next_snapshot_id = next_snapshot_id;
        let manifest = manifest_state.manifest.clone();
        manifest_state.store.commit(&manifest)?;
        commit_l2p_checkpoint(&mut l2p_guards, created_lsn)?;
        commit_refcount_checkpoint(&mut refcount_guards, created_lsn)?;
        self.finish_dedup_manifest_update(dedup_update, created_lsn)?;
        // Append the new snap to the per-volume cache. Inline update —
        // manifest_state is still held above, and `recompute_snap_info`
        // would re-lock.
        {
            let mut cache = self.snap_info_cache.lock();
            cache.entry(vol_ord).or_default().push(SnapInfo {
                created_lsn,
                l2p_shard_roots: snap_roots,
            });
        }
        Ok(id)
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

    /// Drop a snapshot. The drop is logged as `WalOp::DropSnapshot` so
    /// the page-refcount work (decref every page the snapshot shares
    /// with the current tree, free any page that hits rc=0) and the
    /// in-memory manifest update are atomic against process crash:
    /// after the WAL fsync, recovery replays the op and re-drives the
    /// work to completion.
    ///
    /// Serialisation:
    /// - `drop_gate.write()` — excludes every `commit_ops` path. The
    ///   rc-dependent plan walk relies on no concurrent `cow_for_write`
    ///   moving rcs out from under us.
    /// - `apply_gate.write()` — excludes `flush` / `take_snapshot`.
    /// - `snapshot_views.write()` — waits for outstanding
    ///   [`SnapshotView`]s to drop before any page is freed.
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
        // refresh manifest before the decref cascade.
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
        let volumes_snap = self.volumes_snapshot();
        let mut l2p_guards = lock_all_l2p_shards_for(&volumes_snap);
        let mut refcount_guards = self.lock_all_refcount_shards();
        flush_locked_l2p_shards(&mut l2p_guards)?;
        self.flush_locked_refcount_shards(&mut refcount_guards)?;

        let checkpoint_lsn = *self.last_applied_lsn.lock();
        let dedup_generation = max_generation_from_two_groups(&l2p_guards, &refcount_guards);
        let dedup_update = {
            let mut mstate = self.manifest_state.lock();
            let dedup_update =
                self.prepare_dedup_manifest_update(&mut mstate.manifest, dedup_generation)?;
            self.refresh_manifest_from_locked(
                &mut mstate.manifest,
                &volumes_snap,
                &l2p_guards,
                &refcount_guards,
            )?;
            mstate.manifest.checkpoint_lsn = checkpoint_lsn;
            let manifest = mstate.manifest.clone();
            mstate.store.commit(&manifest)?;
            dedup_update
        };
        commit_l2p_checkpoint(&mut l2p_guards, dedup_generation)?;
        commit_refcount_checkpoint(&mut refcount_guards, dedup_generation)?;
        self.finish_dedup_manifest_update(dedup_update, dedup_generation)?;

        // Locate the source volume's shard range within l2p_guards.
        let mut source_start = 0usize;
        for vol in &volumes_snap {
            if vol.ord == entry.vol_ord {
                break;
            }
            source_start += vol.shards.len();
        }
        let source_end = source_start + source_volume.shards.len();

        let all_current_roots: Vec<PageId> = l2p_guards.iter().map(|tree| tree.root()).collect();
        // SPEC §3.3 pba_decrefs: for every lba where the snapshot has a
        // value but the current tree diverged, emit one decref against
        // the snap-side pba. Computed while we still hold the source
        // volume's shard guards so `diff_subtrees` sees a coherent snap
        // root vs current root pair. The `RemovedInB` / `Changed` arms
        // cover "snap has it, current doesn't" and "snap has it, current
        // has a different value" respectively; `AddedInB` is "current
        // has it, snap doesn't" which has nothing for us to compensate.
        let mut raw_pba_decrefs: Vec<(Lba, L2pValue)> = Vec::new();
        for (tree, &snap_root) in l2p_guards[source_start..source_end]
            .iter_mut()
            .zip(entry.l2p_shard_roots.iter())
        {
            if snap_root == crate::types::NULL_PAGE {
                continue;
            }
            let current_root = tree.root();
            for diff in tree.diff_subtrees(snap_root, current_root)? {
                match diff {
                    DiffEntry::RemovedInB { key, old } | DiffEntry::Changed { key, old, .. } => {
                        raw_pba_decrefs.push((key, old));
                    }
                    DiffEntry::AddedInB { .. } => {}
                }
            }
        }
        // Filter out pbas whose refcount is already 0. This happens when
        // the volume was written via raw `insert` / `delete` (no
        // refcount tracking) — production flows via `l2p_remap` always
        // maintain `rc(snap_pba) >= 1` for every lba the snapshot
        // references, so the filter is a no-op there. Without this
        // guard the apply decref would underflow and corrupt state.
        //
        // We hold `refcount_guards` here, so reads are consistent with
        // the subsequent apply under `apply_gate.write()`. Pending
        // decref entries on the same pba within this same diff are
        // accounted for via `pending[pba]` so the N-th appearance only
        // survives if `rc(pba) > pending` — matches the apply's
        // sequential decrefs.
        let mut pending: HashMap<Pba, u32> = HashMap::new();
        let mut pba_decrefs: Vec<Pba> = Vec::with_capacity(raw_pba_decrefs.len());
        for (_lba, value) in raw_pba_decrefs {
            let pba = value.head_pba();
            let sid = shard_for_key(&self.refcount_shards, pba);
            let rc = refcount_guards[sid].get(pba)?.map(|e| e.rc).unwrap_or(0);
            let taken = pending.entry(pba).or_insert(0);
            if *taken < rc {
                *taken += 1;
                pba_decrefs.push(pba);
            }
        }
        drop(l2p_guards);
        drop(refcount_guards);

        // Page refcounts are physical-page ownership counts, not
        // per-volume logical counts. `clone_volume` can make any live
        // volume (and snapshots of that volume) share the same paged L2P
        // pages as the snapshot being dropped. Build the decrement plan
        // from the complete manifest-visible page graph so a page is
        // decremented exactly when removing this snapshot removes one
        // physical incoming edge. The PBA compensation above stays
        // volume-scoped because it reasons about this snapshot's logical
        // LBA values, not page-parent edges.
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
        let op = WalOp::DropSnapshot {
            id,
            pages: pages.clone(),
            pba_decrefs: pba_decrefs.clone(),
        };
        let body = encode_body(std::slice::from_ref(&op));
        let lsn = self.wal.submit(body)?;
        self.faults.inject(FaultPoint::CommitPostWalBeforeApply)?;

        // Block until every prior LSN has applied. Under our locks,
        // `last_applied_lsn` can only move forward when a commit
        // completes — and since we hold drop_gate.write, no new
        // commits have entered, so this wait is bounded by whatever
        // was in flight at the moment we took the gate.
        {
            let mut applied = self.last_applied_lsn.lock();
            while *applied + 1 < lsn {
                self.commit_cvar.wait(&mut applied);
            }
        }

        let volumes_map = self.volumes.read().clone();
        let snap_lookup = |vol: VolumeOrdinal| -> Vec<SnapInfo> { self.snap_info_for_vol(vol) };
        let outcome = apply_op_bare(
            &volumes_map,
            &self.refcount_shards,
            &self.dedup_index,
            &self.dedup_reverse,
            &self.page_store,
            lsn,
            &op,
            &snap_lookup,
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

        {
            let mut applied = self.last_applied_lsn.lock();
            *applied = lsn;
            self.commit_cvar.notify_all();
        }

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

        Ok(Some(DropReport {
            snapshot_id: id,
            freed_leaf_values,
            pages_freed,
            freed_pbas,
        }))
    }
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

    /// Point lookup as of the snapshot's LSN. Routes through the
    /// owning volume's shard layout — commit 6 always stamps
    /// `vol_ord = 0`, commit 9 switches that to the real source.
    pub fn get(&self, lba: Lba) -> Result<Option<L2pValue>> {
        let volume = self.db.volume(self.entry.vol_ord)?;
        let sid = shard_for_key_l2p(&volume.shards, lba);
        let tree = volume.shards[sid].tree.read();
        tree.get_at_read_only(self.entry.l2p_shard_roots[sid], lba)
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
