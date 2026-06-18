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
    /// - **Forced TXG sync** — kept for consistency with the other
    ///   lifecycle ops, so future maintainers do not have to reason
    ///   about a mixed model. `create_volume` itself does not perform
    ///   any whole-page rc RMW, so the sync is not strictly required
    ///   for correctness.
    /// - `apply_gate.write()` — taken AFTER the forced sync. Serialises
    ///   the WAL submit + apply against
    ///   [`Db::run_sync_cycle_body`]'s manifest-commit window.
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
        // Forced sync mirrors the other lifecycle ops. Not strictly
        // required (no whole-page rc RMW happens here) but kept so the
        // lifecycle API has a uniform shape.
        self.flush_with_gate(crate::metrics::FlushKind::Forced)?;
        // Phase 4 gate-shrink: record this lifecycle op's WAL LSN into
        // `slot_max_lsn(open_txg)` so `run_sync_cycle_body`'s
        // `wal_checkpoint = slot_max_lsn(txg)` watermark reflects it.
        // Must be entered AFTER `flush_with_gate(Forced)` returns —
        // that call rolls the current Open TXG; entering before would
        // race `roll_to_quiescing` waiting for `inflight == 0`.
        let _txg_guard = self.txg.enter();
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
            // Probe durable_seq matches the probe manifest's
            // checkpoint_lsn so the v11 `min(durable_seq[]) ==
            // checkpoint_lsn` invariant fires only on the real
            // capacity miss (the size check above), not on a
            // synthetic per-shard mismatch the probe itself created.
            let probe_lsn = probe.checkpoint_lsn;
            probe.volumes.push(VolumeEntry {
                ord,
                shard_count,
                l2p_shard_roots: vec![crate::types::NULL_PAGE; shard_count as usize]
                    .into_boxed_slice(),
                l2p_shard_durable_seq: vec![probe_lsn; shard_count as usize].into_boxed_slice(),
                created_lsn: 0,
                flags: 0,
                dead_list_head_pid: crate::types::NULL_PAGE,
                dead_list_tail_pid: crate::types::NULL_PAGE,
                parent_vol_ord: None,
                branched_at_lsn: 0,
                promotion_cursor: None,
            });
            probe.check_encodable()?;
            (ord, shard_count)
        };

        let lifecycle_op = crate::lifecycle_log::LifecycleOp::CreateVolume { ord, shard_count };
        let lsn = self.submit_lifecycle_op(&lifecycle_op)?;
        _txg_guard.record_lsn(lsn);
        self.faults.inject(FaultPoint::CommitPostWalBeforeApply)?;

        // Under our two write gates no other commit is between submit
        // and apply, so last_applied_lsn + 1 == lsn already. The cvar
        // wait keeps the pattern symmetric with `drop_snapshot`.
        self.wait_for_global_apply_turn(lsn)?;

        let (shards, roots) = apply_create_volume(
            &self.page_store,
            &self.page_cache,
            &self.l2p_page_rc,
            shard_count,
            self.metrics.clone(),
            self.l2p_buffer_enabled,
            lsn,
        )?;
        // A3 cutover fix (2026-06-17): `apply_create_volume` only STAGES
        // each shard root's page-rc `+1` into the array slot. Every other
        // lifecycle op that mutates page rc — take_snapshot, drop_snapshot,
        // clone_volume, and the bootstrap volume at `Db::create` — force-
        // folds it durable with `l2p_page_rc.flush()`. create_volume was the
        // sole exception: if the manifest commit / checkpoint advances past
        // this CreateVolume LSN before the staged `+1` is folded (e.g. the
        // standalone `create-volume` CLI process exits, or `created_lsn==0`
        // makes the hot fold's `page_generation >= last_lsn` skip it), the
        // root reopens with array rc=0. A later snapshot then drops it 1→0
        // and frees a still-live shard root → page-type corruption under
        // snapshot churn (nvme-box soak 2026-06-17). Fold it here, durable,
        // before the manifest records this volume.
        self.l2p_page_rc.flush()?;
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
            let durable_seqs = vec![lsn; shard_count as usize].into_boxed_slice();
            mstate.manifest.volumes.push(VolumeEntry {
                ord,
                shard_count,
                l2p_shard_roots: roots,
                // Fresh volume: every L2P shard is durable at the
                // CreateVolume LSN (manifest commit is about to make
                // it so). Subsequent flushes will advance these.
                l2p_shard_durable_seq: durable_seqs,
                created_lsn: lsn,
                flags: 0,
                dead_list_head_pid: crate::types::NULL_PAGE,
                dead_list_tail_pid: crate::types::NULL_PAGE,
                // Top-level volume: no parent lineage and no in-flight
                // promotion walker. `clone_volume` is the only path that
                // sets `parent_vol_ord` / `branched_at_lsn` / arms the
                // walker.
                parent_vol_ord: None,
                branched_at_lsn: 0,
                promotion_cursor: None,
            });
            mstate.manifest.next_volume_ord = ord
                .checked_add(1)
                .ok_or_else(|| MetaDbError::Corruption("volume ord overflow".into()))?;
        }

        self.finish_global_apply(lsn)?;
        self.advance_dispatch_lsn(lsn);

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
    /// - **Forced TXG sync** — drains pre-existing L2P Dirty Arcs to
    ///   disk so `apply_drop_volume`'s page-decref whole-page writes
    ///   cannot be clobbered by a concurrent flush IO phase. See
    ///   [`Db::take_snapshot`] for the full rationale.
    /// - `apply_gate.write()` — taken AFTER the forced sync. Serialises
    ///   the WAL submit + apply against
    ///   [`Db::run_sync_cycle_body`]'s manifest-commit window.
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
        // Drive a forced TXG sync BEFORE acquiring apply_gate.write —
        // otherwise the sync thread's manifest-commit gate would
        // deadlock against our outer hold.
        self.flush_with_gate(crate::metrics::FlushKind::Forced)?;
        // Phase 4 gate-shrink: record this lifecycle op's WAL LSN into
        // `slot_max_lsn(open_txg)` so `run_sync_cycle_body`'s
        // `wal_checkpoint = slot_max_lsn(txg)` watermark reflects it.
        // Must be entered AFTER `flush_with_gate(Forced)` returns —
        // that call rolls the current Open TXG; entering before would
        // race `roll_to_quiescing` waiting for `inflight == 0`.
        let _txg_guard = self.txg.enter();
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
            // [[no-refcount-hot-path-design]] Phase 4 Step 6: descendant
            // clones whose `parent_vol_ord == vol_ord` rely on the
            // parent's COW-shared L2P pages until their promotion walker
            // finishes. Dropping the parent now would free those pages
            // out from under live descendants. Cross-volume snap_pin
            // (Step 2) keeps the parent's PBA-level data observable,
            // but the L2P page tree itself is per-volume — once we
            // collect-and-free the parent's pages there's nothing for
            // the descendant to COW from. Reject and let the caller
            // either promote the descendants to independence first or
            // drop them. `parent_vol_ord` is cleared by
            // `LifecycleOp::PromotionComplete`, so this naturally accepts
            // post-promotion descendants.
            if let Some(child) = mstate
                .manifest
                .volumes
                .iter()
                .find(|v| v.parent_vol_ord == Some(vol_ord))
            {
                return Err(MetaDbError::InvalidArgument(format!(
                    "cannot drop volume {vol_ord} with descendant clone {} pending promotion \
                     (parent_vol_ord still set)",
                    child.ord
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
        flush_locked_l2p_shards(&mut l2p_guards)?;
        self.flush_all_refcount_shards()?;
        // A3: fold the page-rc array so the refreshed manifest below
        // records a durable `l2p_page_rc_durable_seq = checkpoint_lsn`.
        self.l2p_page_rc.flush()?;

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
        // Phase 2 dead-list: walk the volume's segment chain backward
        // from its tail and add every chain page id to the drop
        // payload, so `apply_drop_volume` releases them via the same
        // `free_idempotent` path that reclaims tree pages. The chain
        // also picks up any in-memory buffer entries; those records
        // are now obsolete and discarded (the volume itself is gone).
        let tail = volume
            .dead_list_tail_pid
            .load(std::sync::atomic::Ordering::Acquire);
        if tail != crate::types::NULL_PAGE {
            let page_store = self.page_store.clone();
            let chain_pids = crate::deadlist::walk_chain_pages(tail, |pid| {
                page_store.read_page(pid)
            })?;
            pages.extend(chain_pids);
        }
        // Discard the volume's outstanding dead-list buffer — it
        // describes overwrites that targeted this volume's own LBAs,
        // which are about to disappear.
        let _ = volume.dead_list.drain();

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
            mstate.manifest.volumes.retain(|v| v.ord != vol_ord);
            mstate.manifest.checkpoint_lsn = checkpoint_lsn;
            let manifest = mstate.manifest.clone();
            mstate.store.commit(&manifest)?;
            dedup_update
        };
        commit_l2p_checkpoint(&mut l2p_guards, dedup_generation)?;
        commit_refcount_checkpoint(&self.refcount_shards, dedup_generation)?;
        self.finish_dedup_manifest_update(dedup_update, dedup_generation)?;
        drop(l2p_guards);

        let lifecycle_op = crate::lifecycle_log::LifecycleOp::DropVolume {
            ord: vol_ord,
            pages: pages.clone(),
        };
        let lsn = self.submit_lifecycle_op(&lifecycle_op)?;
        _txg_guard.record_lsn(lsn);
        self.faults.inject(FaultPoint::CommitPostWalBeforeApply)?;
        // Fault window specific to drop_volume: WAL record durable, no
        // page decref has touched disk yet. Recovery re-drives the full
        // cascade from the WAL op's inlined `pages` list.
        self.faults
            .inject(FaultPoint::DropVolumePostWalBeforeApply)?;
        self.wait_for_global_apply_turn(lsn)?;

        let pages_freed =
            apply_drop_volume(&self.page_store, &self.l2p_page_rc, lsn, _txg_guard.txg(), &pages)?;
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

        self.finish_global_apply(lsn)?;
        self.advance_dispatch_lsn(lsn);

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
    /// - `apply_gate.write()` — serialises the WAL submit + apply against
    ///   [`Db::run_sync_cycle_body`]'s manifest-commit window.
    ///
    /// Phase B (A3 follow-up): the **forced TXG sync** that used to run
    /// at entry is GONE. Its sole job was to drain in-flight flush IO so
    /// `apply_clone_volume_incref`'s whole-page rc RMW could not be
    /// clobbered by a concurrent whole-page flush write. A3 relocated the
    /// page refcount into the [`L2pPageRc`](crate::l2p_page_rc) array, so
    /// the incref is now a sharded-delta `stage` (concurrency-safe with
    /// flush, exactly like PBA rc) and the page bytes are never touched —
    /// there is nothing left to clobber. Removing the forced sync stops
    /// every clone from driving + waiting on a full sync cycle while
    /// holding `drop_gate.write` (the snapshot-scaling barrier). Crash
    /// durability is unchanged: the `CloneVolume` WAL record replays the
    /// incref (gen-stamped idempotent), and the next natural flush commits
    /// the manifest.
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
        // Phase B (A3 follow-up): no forced TXG sync here. The page-rc
        // incref is now a `stage` into the array (clobber-free), so the
        // sync-drain barrier is unnecessary. `txg.enter()` pins the
        // current Open TXG; `TxgStateMachine`'s `closing_open` flag makes
        // it wait out (not race) a concurrent background roll, so it is
        // safe to enter without first rolling the TXG ourselves.
        //
        // Phase 4 gate-shrink: `_txg_guard.record_lsn(lsn)` (below)
        // records this lifecycle op's WAL LSN into `slot_max_lsn(open_txg)`
        // so `run_sync_cycle_body`'s `wal_checkpoint = slot_max_lsn(txg)`
        // watermark reflects it and the WAL segment is eventually pruned.
        let _txg_guard = self.txg.enter();
        let _apply_guard = self.apply_gate.write();

        // Resolve the snapshot entry + allocate the new ord under the
        // manifest mutex so two concurrent clones can't hand out the
        // same ordinal.
        let (src_ord, src_shard_roots, new_ord, branched_at_lsn) = {
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
            // Same probe-vs-real-encode rationale as `create_volume`:
            // stamp probe durable_seq to the probe's checkpoint_lsn
            // so the invariant guards the snapshot-table capacity
            // check rather than a synthetic mismatch.
            let probe_lsn = probe.checkpoint_lsn;
            probe.volumes.push(VolumeEntry {
                ord: new_ord,
                shard_count: shard_count as u32,
                l2p_shard_roots: vec![crate::types::NULL_PAGE; shard_count].into_boxed_slice(),
                l2p_shard_durable_seq: vec![probe_lsn; shard_count].into_boxed_slice(),
                created_lsn: 0,
                flags: 0,
                dead_list_head_pid: crate::types::NULL_PAGE,
                dead_list_tail_pid: crate::types::NULL_PAGE,
                parent_vol_ord: Some(entry.vol_ord),
                branched_at_lsn: entry.created_lsn,
                promotion_cursor: None,
            });
            probe.check_encodable()?;
            (
                entry.vol_ord,
                entry.l2p_shard_roots.to_vec(),
                new_ord,
                entry.created_lsn,
            )
        };

        let lifecycle_op = crate::lifecycle_log::LifecycleOp::CloneVolume {
            src_ord,
            new_ord,
            src_snap_id,
            src_shard_roots: src_shard_roots.clone(),
        };
        let lsn = self.submit_lifecycle_op(&lifecycle_op)?;
        _txg_guard.record_lsn(lsn);
        self.faults.inject(FaultPoint::CommitPostWalBeforeApply)?;

        // Under our two write gates no other commit sits between submit
        // and apply; the cvar wait is defensive and matches
        // `create_volume` / `drop_snapshot`.
        self.wait_for_global_apply_turn(lsn)?;

        apply_clone_volume_incref(
            &self.l2p_page_rc,
            &self.faults,
            lsn,
            _txg_guard.txg(),
            &src_shard_roots,
        )?;
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

        let (shards, actual_roots) = build_clone_volume_shards(
            &src_shard_roots,
            &self.page_store,
            &self.page_cache,
            &self.l2p_page_rc,
            lsn,
            self.metrics.clone(),
            self.l2p_buffer_enabled,
        )?;
        let shard_count = shards.len() as u32;

        {
            let mut volumes_map = self.volumes.write();
            if volumes_map.contains_key(&new_ord) {
                return Err(MetaDbError::Corruption(format!(
                    "clone_volume: ord {new_ord} already present"
                )));
            }
            // Use `Volume::with_lineage` so the in-memory volume carries
            // `parent_vol_ord` / `branched_at_lsn` straight away. The
            // next `flush` reads these back through
            // `refresh_manifest_entries`; if we left them at the
            // `Volume::new` defaults they would overwrite the manifest
            // commit's correctly-set lineage trio on the very next
            // checkpoint.
            volumes_map.insert(
                new_ord,
                Arc::new(Volume::with_lineage(
                    new_ord,
                    shards,
                    lsn,
                    crate::types::NULL_PAGE,
                    crate::types::NULL_PAGE,
                    Some(src_ord),
                    branched_at_lsn,
                    None,
                )),
            );
        }

        {
            let mut mstate = self.manifest_state.lock();
            let durable_seqs = vec![lsn; shard_count as usize].into_boxed_slice();
            mstate.manifest.volumes.push(VolumeEntry {
                ord: new_ord,
                shard_count,
                l2p_shard_roots: actual_roots,
                // CloneVolume creates a fresh per-volume L2P at the
                // CloneVolume LSN; the manifest commit makes that
                // durable. Future flushes advance per-shard.
                l2p_shard_durable_seq: durable_seqs,
                created_lsn: lsn,
                flags: 0,
                // Clones start with an empty dead-list. The parent's
                // chain still correctly describes the parent's overwrite
                // history; the clone's L2P starts fresh so its first
                // overwrite is recorded against its own chain (not the
                // parent's).
                dead_list_head_pid: crate::types::NULL_PAGE,
                dead_list_tail_pid: crate::types::NULL_PAGE,
                // Phase 4 lineage tracking. `parent_vol_ord` + `branched_at_lsn`
                // mark this volume as a clone whose Lineage GC must consult the
                // parent (cross-volume snap_pin: Step 2). `promotion_cursor`
                // stays `None` until Step 5 arms the background walker that
                // increfs the global rc for each shared PBA, which is what
                // ultimately lets the parent be dropped independently.
                parent_vol_ord: Some(src_ord),
                branched_at_lsn,
                promotion_cursor: None,
            });
            mstate.manifest.next_volume_ord = new_ord
                .checked_add(1)
                .ok_or_else(|| MetaDbError::Corruption("volume ord overflow".into()))?;
        }

        self.finish_global_apply(lsn)?;
        self.advance_dispatch_lsn(lsn);

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
    /// snap is live on the volume. Phase 5 RangeDelete is PBA
    /// rc-neutral, so it does not consult this cache for refcount work.
    pub(super) fn snap_info_for_vol(&self, vol: VolumeOrdinal) -> Vec<SnapInfo> {
        self.snap_info_cache
            .lock()
            .get(&vol)
            .cloned()
            .unwrap_or_default()
    }

    /// Youngest live snapshot's `created_lsn` for `vol` (ZFS `prev_snap_txg`
    /// analogue), or `0` when the volume has no live snapshot. This is the
    /// birth-txg threshold for the COW kill decision in the birth-txg port:
    /// a page reachable from the head with `birth_lsn <= youngest_snap` is
    /// still pinned by that snapshot. Phase 1 reads it only for the
    /// birth-shadow audit; the COW kill decision stays page-rc-authoritative.
    // Phase 1 ships the accessor + its unit test ahead of its first runtime
    // caller (the Phase 2 birth-authoritative kill decision); allow until then.
    #[allow(dead_code)]
    pub(crate) fn youngest_snap(&self, vol: VolumeOrdinal) -> Lsn {
        self.snap_info_cache
            .lock()
            .get(&vol)
            .map(|infos| infos.iter().map(|s| s.created_lsn).max().unwrap_or(0))
            .unwrap_or(0)
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

    /// Public L2P-shard routing for clients (onyx) that want to
    /// pre-bucket commit batches per L2P shard before issuing the
    /// metadb call. Mirrors `shard_for_key_l2p(&volume.shards, lba)`
    /// from `apply.rs:332` but uses `refcount_shards.len()` as the
    /// modulus — L2P shards and refcount shards both come from
    /// `Config::shards_per_partition`, so their counts are equal for
    /// every volume in this partition (the bootstrap volume and every
    /// volume created via `CreateVolume`).
    ///
    /// The hash math (xxh3_64 of `lba >> LEAF_SHIFT`) MUST stay in
    /// lockstep with `shard_for_key_l2p` and the dispatch planner —
    /// divergence between client-side bucketing and apply-side routing
    /// would cause sub-commits to claim L2P shards they don't actually
    /// touch (and miss the ones they do), breaking the dispatch
    /// footprint invariant. Tested by
    /// `l2p_shard_for_matches_shard_for_key_l2p` in
    /// `commit/tests.rs`.
    pub fn l2p_shard_for(&self, lba: Lba) -> usize {
        debug_assert!(!self.refcount_shards.is_empty());
        let leaf_idx = lba >> crate::paged::format::LEAF_SHIFT;
        (xxh3_64(&leaf_idx.to_be_bytes()) as usize) % self.refcount_shards.len()
    }

    pub(super) fn prepare_dedup_manifest_update(
        &self,
        manifest: &mut Manifest,
        generation: Lsn,
    ) -> Result<DedupManifestUpdate> {
        // Cuckoo dedup_index writes data pages synchronously per op;
        // only its meta page can be dirty here. The meta page id is
        // stable across opens — the manifest slot only needs to be
        // re-stamped to the same value. (The legacy `dedup_reverse`
        // / `paged_reverse` half is gone as of manifest v9.)
        self.dedup_index.flush_meta()?;
        let _ = generation;
        manifest.dedup_index_shard_heads =
            vec![vec![self.dedup_index.meta_page_id()].into_boxed_slice()].into_boxed_slice();
        Ok(DedupManifestUpdate {
            old_dedup_heads: Vec::new(),
        })
    }

    pub(super) fn finish_dedup_manifest_update(
        &self,
        update: DedupManifestUpdate,
        _generation: Lsn,
    ) -> Result<()> {
        // Cuckoo dedup_index: nothing to reclaim — the meta page id
        // is stable across opens and data pages are owned inline.
        let _ = update.old_dedup_heads;
        Ok(())
    }

    /// Paged-array refcount no longer needs per-shard guards held
    /// across snapshot / flush prepare windows: each `RcShard` has
    /// its own internal mutex covering apply + read, and snapshot
    /// semantics are unchanged because refcount is not snapshotted.
    /// Callers that previously asked for guards now operate directly
    /// on the shards via [`flush_all_refcount_shards`] /
    /// [`refresh_manifest_from_shards`].
    pub(super) fn flush_all_refcount_shards(&self) -> Result<()> {
        for shard in &self.refcount_shards {
            shard.rc.flush()?;
        }
        Ok(())
    }

    /// Refresh manifest fields that mirror in-memory state.
    ///
    /// Does NOT touch `checkpoint_lsn` — that is the durable-WAL LSN
    /// cursor and is only ever advanced by code paths that have taken
    /// `apply_gate.write()` (flush / take_snapshot / drop_snapshot) and
    /// therefore have an authoritative reading of `last_applied_lsn`.
    ///
    /// `durable_override` is Stage 1 (Tier 2.B) plumbing: callers
    /// that just flushed every shard and are about to write
    /// `manifest.checkpoint_lsn = last_applied_lsn` pass `Some(lsn)`
    /// so the persisted per-shard `durable_seq` arrays match the new
    /// checkpoint. Passing `None` reads atomics (used by `flush_with_gate`
    /// post-commit and by `open` recovery).
    pub(super) fn refresh_manifest_from_locked(
        &self,
        manifest: &mut Manifest,
        volumes: &[Arc<Volume>],
        l2p_guards: &[RwLockWriteGuard<'_, PagedL2p>],
        durable_override: Option<Lsn>,
    ) -> Result<()> {
        refresh_manifest_entries(
            manifest,
            volumes,
            l2p_guards,
            &self.refcount_shards,
            &self.l2p_page_rc,
            durable_override,
        )
    }

    pub(super) fn current_generation(&self) -> Lsn {
        let volumes = self.volumes_snapshot();
        let l2p = lock_all_l2p_shards_for(&volumes);
        max_generation_from_two_groups(&l2p, &self.refcount_shards)
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
