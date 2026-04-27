use super::*;

impl Db {
    // -------- volume lifecycle ------------------------------------------

    /// Mint a new volume. Returns the freshly-assigned ordinal. Uses the
    /// same shard count as the bootstrap volume, matching the "every
    /// volume has the same shard count" invariant documented on
    /// [`Volume`].
    ///
    /// Serialisation:
    /// - `drop_gate.write()` — blocks new `commit_ops` callers and waits
    ///   for in-flight commits to finish so our subsequent WAL submit +
    ///   `commit_cvar` wait cannot deadlock behind an LSN assigned to a
    ///   commit that hasn't reached apply yet.
    /// - `apply_gate.write()` — excludes `flush` / `take_snapshot` /
    ///   `drop_snapshot`.
    ///
    /// Crash semantics:
    /// - Before WAL fsync: no effect observable.
    /// - After WAL fsync, before/during apply: recovery re-applies the
    ///   `CreateVolume` record. The in-memory `volumes.contains_key(ord)`
    ///   guard plus deterministic page allocation make replay idempotent.
    ///
    /// No manifest commit happens inside this function; the next natural
    /// [`flush`](Self::flush) captures the new volumes table.
    pub fn create_volume(&self) -> Result<VolumeOrdinal> {
        let _drop_guard = self.drop_gate.write();
        let _apply_guard = self.apply_gate.write();

        let (ord, shard_count) = {
            let mstate = self.manifest_state.lock();
            if (mstate.manifest.volumes.len() as u32) >= self.max_volumes {
                return Err(MetaDbError::InvalidArgument(format!(
                    "max_volumes ({}) reached",
                    self.max_volumes,
                )));
            }
            let ord = mstate.manifest.next_volume_ord;
            let shard_count = self.volume_zero().shards.len() as u32;
            // Probe encode: adding a volume shrinks the per-page snapshot
            // budget. If the existing snapshot table no longer fits once
            // we grow `volumes`, reject now — otherwise the overflow
            // would surface at the next flush / snapshot commit with no
            // way to roll back the intervening WAL ops. Matches the
            // probe `take_snapshot` runs before its own irreversible
            // side effects.
            let mut probe = mstate.manifest.clone();
            probe.volumes.push(VolumeEntry {
                ord,
                shard_count,
                l2p_shard_roots: vec![crate::types::NULL_PAGE; shard_count as usize]
                    .into_boxed_slice(),
                created_lsn: 0,
                flags: 0,
            });
            probe.check_encodable()?;
            (ord, shard_count)
        };

        let op = WalOp::CreateVolume { ord, shard_count };
        let body = encode_body(std::slice::from_ref(&op));
        let lsn = self.wal.submit(body)?;
        self.faults.inject(FaultPoint::CommitPostWalBeforeApply)?;

        // Under our two write gates no other commit is between submit
        // and apply, so last_applied_lsn + 1 == lsn already. The cvar
        // wait keeps the pattern symmetric with `drop_snapshot`.
        {
            let mut applied = self.last_applied_lsn.lock();
            while *applied + 1 < lsn {
                self.commit_cvar.wait(&mut applied);
            }
        }

        let (shards, roots) = apply_create_volume(&self.page_store, &self.page_cache, shard_count)?;
        self.faults
            .inject(FaultPoint::CommitPostApplyBeforeLsnBump)?;

        {
            let mut volumes_map = self.volumes.write();
            if volumes_map.contains_key(&ord) {
                return Err(MetaDbError::Corruption(format!(
                    "create_volume: ord {ord} already present"
                )));
            }
            volumes_map.insert(ord, Arc::new(Volume::new(ord, shards, lsn)));
        }

        // Window exposed to fault-injection tests: WAL record is durable
        // + the in-memory volumes map is populated, but the manifest's
        // volumes table hasn't been extended. A crash here is recovered
        // on reopen via the CreateVolume replay arm.
        self.faults
            .inject(FaultPoint::CreateVolumePostWalBeforeManifest)?;

        {
            let mut mstate = self.manifest_state.lock();
            mstate.manifest.volumes.push(VolumeEntry {
                ord,
                shard_count,
                l2p_shard_roots: roots,
                created_lsn: lsn,
                flags: 0,
            });
            mstate.manifest.next_volume_ord = ord
                .checked_add(1)
                .ok_or_else(|| MetaDbError::Corruption("volume ord overflow".into()))?;
        }

        {
            let mut applied = self.last_applied_lsn.lock();
            *applied = lsn;
            self.commit_cvar.notify_all();
        }

        Ok(ord)
    }

    /// Drop the volume at `vol_ord`. Refuses to drop the bootstrap
    /// volume (ord 0) or any volume with a live snapshot pinning it.
    /// Unknown ordinals return `Ok(None)` to mirror `drop_snapshot`'s
    /// idempotent shape.
    ///
    /// Serialisation:
    /// - `drop_gate.write()` — excludes every `commit_ops` path. The
    ///   rc-dependent drop plan relies on no concurrent `cow_for_write`
    ///   moving rcs out from under us.
    /// - `apply_gate.write()` — excludes `flush` / `take_snapshot` /
    ///   `drop_snapshot` / `create_volume`.
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
    /// natural [`flush`](Self::flush) captures the new volumes list.
    pub fn drop_volume(&self, vol_ord: VolumeOrdinal) -> Result<Option<DropVolumeReport>> {
        if vol_ord == BOOTSTRAP_VOLUME_ORD {
            return Err(MetaDbError::InvalidArgument(
                "cannot drop the bootstrap volume (ord=0)".into(),
            ));
        }
        let _drop_guard = self.drop_gate.write();
        let _apply_guard = self.apply_gate.write();
        let _view_guard = self.snapshot_views.write();

        // Live snapshots on the dying volume would outlive their source
        // trees' roots. Reject and let the caller drop them first.
        {
            let mstate = self.manifest_state.lock();
            if mstate
                .manifest
                .snapshots
                .iter()
                .any(|s| s.vol_ord == vol_ord)
            {
                return Err(MetaDbError::InvalidArgument(format!(
                    "cannot drop volume {vol_ord} with live snapshots"
                )));
            }
        }

        let volume = match self.volumes.read().get(&vol_ord).cloned() {
            Some(v) => v,
            None => return Ok(None),
        };

        // Lock ALL volumes' shards + refcount shards so we can flush
        // them and later commit a refreshed manifest.
        let volumes_snap = self.volumes_snapshot();
        let mut l2p_guards = lock_all_l2p_shards_for(&volumes_snap);
        let mut refcount_guards = self.lock_all_refcount_shards();
        flush_locked_l2p_shards(&mut l2p_guards)?;
        self.flush_locked_refcount_shards(&mut refcount_guards)?;

        // Locate the dying volume's shard range within l2p_guards.
        let mut target_start = 0usize;
        for vol in &volumes_snap {
            if vol.ord == vol_ord {
                break;
            }
            target_start += vol.shards.len();
        }
        let target_end = target_start + volume.shards.len();

        let mut pages: Vec<PageId> = Vec::new();
        for tree in &mut l2p_guards[target_start..target_end] {
            let root = tree.root();
            if root == crate::types::NULL_PAGE {
                continue;
            }
            pages.extend(tree.collect_drop_pages(root)?);
        }

        // Commit a manifest that:
        //   (a) reflects current roots for every surviving volume
        //       (prior commit_ops cows may have moved their roots
        //       without touching the on-disk manifest),
        //   (b) no longer lists this volume, and
        //   (c) has the dedup memtables flushed so the new
        //       checkpoint_lsn doesn't skip in-RAM-only dedup rows
        //       during WAL replay.
        //
        // Doing this BEFORE the page-freeing cascade is load-bearing:
        // on crash between commit and cascade, reopen sees no vol_ord
        // entry and simply leaves the tree pages as orphans for
        // `reclaim_orphan_pages` to collect. If we instead committed
        // with `vol_ord` still present (its roots about to be freed),
        // a crash between cascade and a *later* commit would leave
        // the on-disk manifest pointing at Free pages, and
        // `open_l2p_shards` would fail at the next open.
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
            mstate.manifest.volumes.retain(|v| v.ord != vol_ord);
            mstate.manifest.checkpoint_lsn = checkpoint_lsn;
            let manifest = mstate.manifest.clone();
            mstate.store.commit(&manifest)?;
            dedup_update
        };
        commit_l2p_checkpoint(&mut l2p_guards, dedup_generation)?;
        commit_refcount_checkpoint(&mut refcount_guards, dedup_generation)?;
        self.finish_dedup_manifest_update(dedup_update, dedup_generation)?;
        drop(l2p_guards);
        drop(refcount_guards);

        let op = WalOp::DropVolume {
            ord: vol_ord,
            pages: pages.clone(),
        };
        let body = encode_body(std::slice::from_ref(&op));
        let lsn = self.wal.submit(body)?;
        self.faults.inject(FaultPoint::CommitPostWalBeforeApply)?;
        // Fault window specific to drop_volume: WAL record durable, no
        // page decref has touched disk yet. Recovery re-drives the full
        // cascade from the WAL op's inlined `pages` list.
        self.faults
            .inject(FaultPoint::DropVolumePostWalBeforeApply)?;
        {
            let mut applied = self.last_applied_lsn.lock();
            while *applied + 1 < lsn {
                self.commit_cvar.wait(&mut applied);
            }
        }

        let pages_freed = apply_drop_volume(&self.page_store, lsn, &pages)?;
        self.faults
            .inject(FaultPoint::CommitPostApplyBeforeLsnBump)?;

        // apply_drop_snapshot_pages wrote pages through page_store,
        // bypassing shard-local PageBuf + shared page_cache — invalidate
        // both so the next cow_for_write / lookup pulls fresh bytes.
        for &pid in &pages {
            self.page_cache.invalidate(pid);
            for shard in &volume.shards {
                shard.tree.write().forget_page(pid);
            }
        }
        drop(volume);

        {
            let mut volumes_map = self.volumes.write();
            volumes_map.remove(&vol_ord);
        }
        {
            let mut mstate = self.manifest_state.lock();
            mstate.manifest.volumes.retain(|v| v.ord != vol_ord);
        }
        // Volume is gone; drop any cached oldest-snap-lsn entry. Note:
        // `drop_volume` rejects volumes with live snapshots (checked at
        // entry), so the cache slot here is normally already empty —
        // this is just defensive cleanup.
        self.forget_snap_info(vol_ord);

        {
            let mut applied = self.last_applied_lsn.lock();
            *applied = lsn;
            self.commit_cvar.notify_all();
        }

        // Drain everything apply_drop_volume queued for deferred reclaim.
        // Snapshot views are excluded above (`snapshot_views.write()`),
        // and live readers can only have non-this-volume views, so any
        // page tags here are eligible the moment min_active_pin advances
        // past them.
        self.reclaim_freed_pages()?;

        Ok(Some(DropVolumeReport {
            vol_ord,
            pages_freed,
        }))
    }

    /// VDO-style writable clone of snapshot `src_snap_id`. The new volume's
    /// initial state mirrors the snapshot: each shard's root points at the
    /// corresponding source root, page-store refcount bumped by one so
    /// subsequent COW writes on either side copy pages instead of
    /// clobbering shared state. Returns the freshly-assigned ordinal.
    ///
    /// The source snapshot must still be alive at call time —
    /// [`drop_snapshot`](Self::drop_snapshot) after the clone is fine
    /// (the clone's incref keeps the shared pages pinned), but dropping
    /// before the clone leaves no valid `src_shard_roots` to inline into
    /// the WAL record.
    ///
    /// Serialisation mirrors [`create_volume`](Self::create_volume):
    /// - `drop_gate.write()` — waits for all in-flight commits to finish
    ///   so our LSN sits right after `last_applied_lsn`.
    /// - `apply_gate.write()` — excludes flush / take_snapshot /
    ///   drop_snapshot.
    ///
    /// Crash semantics:
    /// - Before WAL fsync: no effect observable.
    /// - After WAL fsync, before/during apply: recovery replays the op.
    ///   The incref half is idempotent via `page.generation >= lsn`; the
    ///   volume-map insertion short-circuits on `volumes.contains_key(new_ord)`.
    ///
    /// No manifest commit happens inside this function; the next natural
    /// [`flush`](Self::flush) — or, on crash, the post-replay commit in
    /// [`open`](Self::open) — captures the new volumes table.
    pub fn clone_volume(&self, src_snap_id: SnapshotId) -> Result<VolumeOrdinal> {
        let _drop_guard = self.drop_gate.write();
        let _apply_guard = self.apply_gate.write();

        // Resolve the snapshot entry + allocate the new ord under the
        // manifest mutex so two concurrent clones can't hand out the
        // same ordinal.
        let (src_ord, src_shard_roots, new_ord) = {
            let mstate = self.manifest_state.lock();
            let entry = mstate
                .manifest
                .snapshots
                .iter()
                .find(|s| s.id == src_snap_id)
                .ok_or_else(|| {
                    MetaDbError::InvalidArgument(format!("unknown snapshot id {src_snap_id}"))
                })?;
            if (mstate.manifest.volumes.len() as u32) >= self.max_volumes {
                return Err(MetaDbError::InvalidArgument(format!(
                    "max_volumes ({}) reached",
                    self.max_volumes,
                )));
            }
            let new_ord = mstate.manifest.next_volume_ord;
            let shard_count = entry.l2p_shard_roots.len();
            // Probe encode: same rationale as `create_volume` — growing
            // the volume table can squeeze the snapshot table out of
            // capacity, so reject before any irreversible WAL submit /
            // page refcount bump.
            let mut probe = mstate.manifest.clone();
            probe.volumes.push(VolumeEntry {
                ord: new_ord,
                shard_count: shard_count as u32,
                l2p_shard_roots: vec![crate::types::NULL_PAGE; shard_count].into_boxed_slice(),
                created_lsn: 0,
                flags: 0,
            });
            probe.check_encodable()?;
            (entry.vol_ord, entry.l2p_shard_roots.to_vec(), new_ord)
        };

        let op = WalOp::CloneVolume {
            src_ord,
            new_ord,
            src_snap_id,
            src_shard_roots: src_shard_roots.clone(),
        };
        let body = encode_body(std::slice::from_ref(&op));
        let lsn = self.wal.submit(body)?;
        self.faults.inject(FaultPoint::CommitPostWalBeforeApply)?;

        // Under our two write gates no other commit sits between submit
        // and apply; the cvar wait is defensive and matches
        // `create_volume` / `drop_snapshot`.
        {
            let mut applied = self.last_applied_lsn.lock();
            while *applied + 1 < lsn {
                self.commit_cvar.wait(&mut applied);
            }
        }

        apply_clone_volume_incref(&self.page_store, &self.faults, lsn, &src_shard_roots)?;
        // `apply_clone_volume_incref` writes through `page_store`, so the
        // shared `page_cache` *and* every in-memory `PageBuf` that holds
        // a stale pre-incref copy of one of these roots need to drop it.
        // It's not enough to invalidate only the source volume: every
        // previously-created clone of the same snapshot already has its
        // own PageBuf with the root cached at the pre-incref rc; that
        // stale Clean copy can be dirtied by a later `incref_root_for_snapshot`
        // (take_snapshot on a clone) or promoted to a `cow_for_write`
        // fast-path decision, both of which would then flush an incorrect
        // refcount back over the disk-direct rc we just wrote. Invalidate
        // the page in every volume's PageBuf — `forget_page` is a no-op
        // on volumes that don't share the pid, so the sweep is safe.
        // `build_clone_volume_shards` below opens fresh `PagedL2p`s for
        // the clone, which read straight from disk.
        let all_volumes: Vec<Arc<Volume>> = self.volumes.read().values().cloned().collect();
        for &pid in &src_shard_roots {
            if pid == crate::types::NULL_PAGE {
                continue;
            }
            self.page_cache.invalidate(pid);
            for vol in &all_volumes {
                for shard in &vol.shards {
                    shard.tree.write().forget_page(pid);
                }
            }
        }
        self.faults
            .inject(FaultPoint::CommitPostApplyBeforeLsnBump)?;

        let (shards, actual_roots) =
            build_clone_volume_shards(&src_shard_roots, &self.page_store, &self.page_cache, lsn)?;
        let shard_count = shards.len() as u32;

        {
            let mut volumes_map = self.volumes.write();
            if volumes_map.contains_key(&new_ord) {
                return Err(MetaDbError::Corruption(format!(
                    "clone_volume: ord {new_ord} already present"
                )));
            }
            volumes_map.insert(new_ord, Arc::new(Volume::new(new_ord, shards, lsn)));
        }

        {
            let mut mstate = self.manifest_state.lock();
            mstate.manifest.volumes.push(VolumeEntry {
                ord: new_ord,
                shard_count,
                l2p_shard_roots: actual_roots,
                created_lsn: lsn,
                flags: 0,
            });
            mstate.manifest.next_volume_ord = new_ord
                .checked_add(1)
                .ok_or_else(|| MetaDbError::Corruption("volume ord overflow".into()))?;
        }

        {
            let mut applied = self.last_applied_lsn.lock();
            *applied = lsn;
            self.commit_cvar.notify_all();
        }

        Ok(new_ord)
    }

    /// Sorted list of live volume ordinals.
    pub fn volumes(&self) -> Vec<VolumeOrdinal> {
        let mut ords: Vec<VolumeOrdinal> = self.volumes.read().keys().copied().collect();
        ords.sort_unstable();
        ords
    }

    /// Sorted snapshot of the volume set. Callers clone the `Arc<Volume>`s
    /// out so shard mutexes can be acquired without the `volumes` read
    /// guard lingering, and sorting by ordinal gives every caller the same
    /// lock order when they grab shard mutexes from multiple volumes.
    pub(super) fn volumes_snapshot(&self) -> Vec<Arc<Volume>> {
        let mut vols: Vec<Arc<Volume>> = self.volumes.read().values().cloned().collect();
        vols.sort_by_key(|v| v.ord);
        vols
    }

    /// Clone out the bootstrap volume. Panics if it is missing — it is
    /// inserted at create / open time and Phase B never removes it.
    pub(super) fn volume_zero(&self) -> Arc<Volume> {
        self.volumes
            .read()
            .get(&BOOTSTRAP_VOLUME_ORD)
            .expect("bootstrap volume must always exist")
            .clone()
    }

    /// Look up volume `vol_ord` and clone its `Arc<Volume>` out of the
    /// map. Unknown ordinals surface as `InvalidArgument` — commit 6's
    /// apply path reports missing volumes as `Corruption` when they
    /// come off the WAL, but the public read/write API treats them as a
    /// caller error.
    pub(super) fn volume(&self, vol_ord: VolumeOrdinal) -> Result<Arc<Volume>> {
        self.volumes
            .read()
            .get(&vol_ord)
            .cloned()
            .ok_or_else(|| MetaDbError::InvalidArgument(format!("unknown volume ord {vol_ord}")))
    }

    /// Read-side: cloned snapshot info for `vol`. Empty Vec when no
    /// snap is live on the volume. Used by [`apply_l2p_remap`] /
    /// [`apply_l2p_range_delete`] to gate decref decisions.
    pub(super) fn snap_info_for_vol(&self, vol: VolumeOrdinal) -> Vec<SnapInfo> {
        self.snap_info_cache
            .lock()
            .get(&vol)
            .cloned()
            .unwrap_or_default()
    }

    /// Recompute the cache entry for `vol` from `manifest.snapshots`.
    /// Callers must already hold a manifest lock or be in a state where
    /// the snapshot list is stable for `vol` (typically `apply_gate` or
    /// `drop_gate` write-side).
    pub(super) fn recompute_snap_info(&self, vol: VolumeOrdinal) {
        let infos: Vec<SnapInfo> = self
            .manifest_state
            .lock()
            .manifest
            .snapshots
            .iter()
            .filter(|s| s.vol_ord == vol)
            .map(|s| SnapInfo {
                created_lsn: s.created_lsn,
                l2p_shard_roots: s.l2p_shard_roots.clone(),
            })
            .collect();
        let mut cache = self.snap_info_cache.lock();
        if infos.is_empty() {
            cache.remove(&vol);
        } else {
            cache.insert(vol, infos);
        }
    }

    /// Bulk-rebuild the cache from the current manifest. Used at open /
    /// recovery and after batched lifecycle replay.
    pub(super) fn recompute_all_snap_infos(&self) {
        let mut by_vol: BTreeMap<VolumeOrdinal, Vec<SnapInfo>> = BTreeMap::new();
        for snap in &self.manifest_state.lock().manifest.snapshots {
            by_vol.entry(snap.vol_ord).or_default().push(SnapInfo {
                created_lsn: snap.created_lsn,
                l2p_shard_roots: snap.l2p_shard_roots.clone(),
            });
        }
        let mut cache = self.snap_info_cache.lock();
        cache.clear();
        cache.extend(by_vol);
    }

    /// Drop the cache entry for `vol` (used by `drop_volume`).
    pub(super) fn forget_snap_info(&self, vol: VolumeOrdinal) {
        self.snap_info_cache.lock().remove(&vol);
    }

    pub(super) fn refcount_shard_for(&self, pba: Pba) -> usize {
        debug_assert!(!self.refcount_shards.is_empty());
        (xxh3_64(&pba.to_be_bytes()) as usize) % self.refcount_shards.len()
    }

    pub(super) fn prepare_dedup_manifest_update(
        &self,
        manifest: &mut Manifest,
        generation: Lsn,
    ) -> Result<DedupManifestUpdate> {
        // Any manifest commit that advances checkpoint_lsn must first
        // make the dedup memtables durable, otherwise replay would skip
        // WAL-applied rows that only existed in RAM.
        self.dedup_index.flush_memtable(generation)?;
        self.dedup_reverse.flush_memtable(generation)?;

        let old_dedup_heads = manifest.dedup_level_heads.to_vec();
        let old_dedup_reverse_heads = manifest.dedup_reverse_level_heads.to_vec();
        manifest.dedup_level_heads = self
            .dedup_index
            .persist_levels(generation)?
            .into_boxed_slice();
        manifest.dedup_reverse_level_heads = self
            .dedup_reverse
            .persist_levels(generation)?
            .into_boxed_slice();
        Ok(DedupManifestUpdate {
            old_dedup_heads,
            old_dedup_reverse_heads,
        })
    }

    pub(super) fn finish_dedup_manifest_update(
        &self,
        update: DedupManifestUpdate,
        generation: Lsn,
    ) -> Result<()> {
        self.dedup_index
            .free_old_level_heads(&update.old_dedup_heads, generation)?;
        self.dedup_reverse
            .free_old_level_heads(&update.old_dedup_reverse_heads, generation)?;
        Ok(())
    }

    pub(super) fn lock_all_refcount_shards(&self) -> Vec<MutexGuard<'_, BTree>> {
        self.refcount_shards
            .iter()
            .map(|shard| shard.tree.lock())
            .collect()
    }

    pub(super) fn flush_locked_refcount_shards(
        &self,
        guards: &mut [MutexGuard<'_, BTree>],
    ) -> Result<()> {
        for tree in guards {
            tree.flush()?;
        }
        Ok(())
    }

    /// Refresh manifest fields that mirror in-memory state.
    ///
    /// Does NOT touch `checkpoint_lsn` — that is the durable-WAL LSN
    /// cursor and is only ever advanced by code paths that have taken
    /// `apply_gate.write()` (flush / take_snapshot / drop_snapshot) and
    /// therefore have an authoritative reading of `last_applied_lsn`.
    pub(super) fn refresh_manifest_from_locked(
        &self,
        manifest: &mut Manifest,
        volumes: &[Arc<Volume>],
        l2p_guards: &[RwLockWriteGuard<'_, PagedL2p>],
        refcount_guards: &[MutexGuard<'_, BTree>],
    ) -> Result<()> {
        refresh_manifest_entries(manifest, volumes, l2p_guards, refcount_guards)
    }

    pub(super) fn current_generation(&self) -> Lsn {
        let volumes = self.volumes_snapshot();
        let l2p = lock_all_l2p_shards_for(&volumes);
        let refcount = self.lock_all_refcount_shards();
        max_generation_from_two_groups(&l2p, &refcount)
    }

    pub(super) fn collect_range_for_roots(
        &self,
        vol_ord: VolumeOrdinal,
        roots: &[PageId],
        range: OwnedRange,
    ) -> Result<DbRangeIter> {
        let volume = self.volume(vol_ord)?;
        if roots.len() != volume.shards.len() {
            return Err(MetaDbError::Corruption(format!(
                "snapshot root count {} does not match shard count {} for volume {vol_ord}",
                roots.len(),
                volume.shards.len(),
            )));
        }
        let mut items = Vec::new();
        for (root, shard) in roots.iter().copied().zip(&volume.shards) {
            let mut tree = shard.tree.write();
            items.extend(
                tree.range_at(root, range.clone())?
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        items.sort_unstable_by_key(|(k, _)| *k);
        Ok(DbRangeIter::new(items))
    }

    pub(super) fn diff_roots(
        &self,
        vol_ord: VolumeOrdinal,
        a: &[PageId],
        b: &[PageId],
    ) -> Result<Vec<DiffEntry>> {
        let volume = self.volume(vol_ord)?;
        if a.len() != volume.shards.len() || b.len() != volume.shards.len() {
            return Err(MetaDbError::Corruption(format!(
                "diff root counts ({}, {}) do not match shard count {} for volume {vol_ord}",
                a.len(),
                b.len(),
                volume.shards.len(),
            )));
        }
        let mut out = Vec::new();
        for ((a_root, b_root), shard) in
            a.iter().copied().zip(b.iter().copied()).zip(&volume.shards)
        {
            let mut tree = shard.tree.write();
            out.extend(tree.diff_subtrees(a_root, b_root)?);
        }
        out.sort_unstable_by_key(DiffEntry::key);
        Ok(out)
    }
}

/// Result of [`Db::drop_volume`].
#[derive(Clone, Debug)]
pub struct DropVolumeReport {
    /// Ordinal of the volume that was dropped.
    pub vol_ord: VolumeOrdinal,
    /// Number of metadb pages released back to the page store.
    pub pages_freed: usize,
}
