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
    /// - **Forced BFG sync** — kept for consistency with the other
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
        // gate-shrink: record this lifecycle op's WAL LSN into
        // `slot_max_lsn(open_bfg)` so `run_sync_cycle_body`'s
        // `wal_checkpoint = slot_max_lsn(bfg)` watermark reflects it.
        // Must be entered AFTER `flush_with_gate(Forced)` returns —
        // that call rolls the current Open BFG; entering before would
        // race `roll_to_quiescing` waiting for `inflight == 0`.
        let _bfg_guard = self.bfg.enter();
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
                page_dead_list_head_pid: crate::types::NULL_PAGE,
                page_dead_list_tail_pid: crate::types::NULL_PAGE,
                page_live_list_head_pid: crate::types::NULL_PAGE,
                page_live_list_tail_pid: crate::types::NULL_PAGE,
                promoted_log_head_pid: crate::types::NULL_PAGE,
                promoted_log_tail_pid: crate::types::NULL_PAGE,
            });
            probe.check_encodable()?;
            (ord, shard_count)
        };

        let lifecycle_op = crate::lifecycle_log::LifecycleOp::CreateVolume { ord, shard_count };
        let lsn = self.submit_lifecycle_op(&lifecycle_op)?;
        _bfg_guard.record_lsn(lsn);
        self.faults.inject(FaultPoint::CommitPostWalBeforeApply)?;

        // Under our two write gates no other commit is between submit
        // and apply, so last_applied_lsn + 1 == lsn already. The cvar
        // wait keeps the pattern symmetric with `drop_snapshot`.
        self.wait_for_global_apply_turn(lsn)?;

        let (shards, roots) = apply_create_volume(
            &self.page_store,
            &self.page_cache,
            shard_count,
            self.metrics.clone(),
            self.l2p_buffer_enabled,
            lsn,
        )?;
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
                page_dead_list_head_pid: crate::types::NULL_PAGE,
                page_dead_list_tail_pid: crate::types::NULL_PAGE,
                page_live_list_head_pid: crate::types::NULL_PAGE,
                page_live_list_tail_pid: crate::types::NULL_PAGE,
                promoted_log_head_pid: crate::types::NULL_PAGE,
                promoted_log_tail_pid: crate::types::NULL_PAGE,
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
    /// - No forced BFG sync at entry. The drop frees pages from an explicit
    ///   structural reachability set (per-L2P-page
    ///   refcounting was deleted, BFG), so there is no whole-page rc
    ///   write left for a concurrent flush IO phase to
    ///   clobber. The `force_compact_l2p_buffers` + `flush_locked_l2p_shards`
    ///   below are the only drain this path needs (mirrors
    ///   `drop_snapshot` / `range_delete`). Holding `drop_gate.write` across
    ///   a full forced sync (which waits on the background quiesce/sync
    ///   pipeline) was the range_delete-class pipeline stall.
    /// - `apply_gate.write()` — serialises the WAL submit + apply against
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
    ///
    /// Collect every surviving L2P root for a `drop_volume` of the shard range
    /// `target_start..target_end`: every OTHER volume's live root from the
    /// locked `l2p_guards` (NOT the on-disk manifest — surviving volumes' COW'd
    /// roots are only refreshed into it at the drop's commit, so a stale
    /// manifest root would manufacture a false free) ∪ every snapshot's roots
    /// (snapshots always survive a volume drop). Shared by the CLONE_LINEAGE
    /// reachability shadow (`check_clone_livelist_shadow` RHS + the promoted-PBA reclaim
    /// promoted-PBA gate) and the non-clone (page-rc removal / non-clone reachability) reachability free-set.
    fn collect_surviving_roots(
        &self,
        l2p_guards: &[RwLockWriteGuard<'_, PagedL2p>],
        target_start: usize,
        target_end: usize,
    ) -> Result<Vec<PageId>> {
        let mut surviving_roots: Vec<PageId> = Vec::new();
        for (i, tree) in l2p_guards.iter().enumerate() {
            if i >= target_start && i < target_end {
                continue;
            }
            let r = tree.root();
            if r != crate::types::NULL_PAGE {
                surviving_roots.push(r);
            }
        }
        let mstate = self.manifest_state.lock();
        for snap in &mstate.manifest.snapshots {
            let roots = crate::verify::snapshot_roots(
                &self.page_store,
                snap.l2p_roots_page,
                &snap.l2p_shard_roots,
            )?;
            surviving_roots.extend(
                roots
                    .iter()
                    .copied()
                    .filter(|&r| r != crate::types::NULL_PAGE),
            );
        }
        Ok(surviving_roots)
    }

    pub fn drop_volume(&self, vol_ord: VolumeOrdinal) -> Result<Option<DropVolumeReport>> {
        if vol_ord == BOOTSTRAP_VOLUME_ORD {
            return Err(MetaDbError::InvalidArgument(
                "cannot drop the bootstrap volume (ord=0)".into(),
            ));
        }
        let _drop_guard = self.drop_gate.write();
        // No forced BFG sync at entry (mirrors take_snapshot / drop_snapshot /
        // clone_volume / range_delete). Per-L2P-page refcounting is gone, so
        // the drop frees pages from an explicit structural reachability set
        // with no whole-page rc write for a concurrent flush IO phase to
        // clobber; the targeted `force_compact_l2p_buffers` +
        // `flush_locked_l2p_shards` below are the only drain this path needs.
        // `bfg.enter()` pins the current Open BFG; `closing_open` makes it
        // wait out (not race) a concurrent background roll, so entering the
        // current Open BFG without rolling it ourselves is safe.
        // `_bfg_guard.record_lsn(lsn)` (after the WAL submit below) records
        // this op's LSN into `slot_max_lsn(open_bfg)` so WAL prune sees it.
        let _bfg_guard = self.bfg.enter();
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
            // descendant
            // clones whose `parent_vol_ord == vol_ord` rely on the
            // parent's COW-shared L2P pages until their promotion walker
            // finishes. Dropping the parent now would free those pages
            // out from under live descendants. Cross-volume snap_pin
            // (lineage pinning) keeps the parent's PBA-level data observable,
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
        // them and later commit a refreshed manifest. With the entry forced
        // sync removed (buffer-backed journal), drain the L2P buffer into the trees first
        // so `collect_drop_pages_with_birth` / the root sample observe every
        // applied op and `checkpoint_lsn` stays safe (mirrors drop_snapshot).
        // All volumes (not scoped): the manifest refresh below covers every
        // surviving volume's roots. `drop_gate.write` keeps slots from refilling.
        self.force_compact_l2p_buffers()?;
        let volumes_snap = self.volumes_snapshot();
        let mut l2p_guards = lock_all_l2p_shards_for(&volumes_snap);
        flush_locked_l2p_shards(&mut l2p_guards)?;
        self.flush_all_refcount_shards()?;

        // Locate the dying volume's shard range within l2p_guards.
        let mut target_start = 0usize;
        for vol in &volumes_snap {
            if vol.ord == vol_ord {
                break;
            }
            target_start += vol.shards.len();
        }
        let target_end = target_start + volume.shards.len();

        // BFG: collect each dropped L2P page with its
        // immutable birth + STRUCTURAL refcount, so the clone-drop livelist
        // shadow below can recover the freed subset (rc == 1) and the
        // origin/clone-private partition. `clone_roots` feeds the shadow's
        // reachability RHS.
        let mut clone_roots: Vec<PageId> = Vec::new();
        for tree in &l2p_guards[target_start..target_end] {
            let root = tree.root();
            if root != crate::types::NULL_PAGE {
                clone_roots.push(root);
            }
        }
        // page-rc removal (page-rc deleted): the per-page plan-time refcount now comes from
        // an explicit STRUCTURAL in-edge count over the live global L2P graph —
        // this dying volume's roots ∪ every surviving volume head ∪ every
        // snapshot root. In a forest of COW trees (one parent per page within
        // any single root) a page's combined in-edge count equals the number of
        // live roots whose tree routes through it, i.e. the old global page-rc
        // for every page reachable from a live root. `rc == 1` therefore still
        // means "reachable from this volume only" (C-exclusive), keeping the
        // cascade gating and `check_clone_livelist_shadow.structural_free`
        // identical to the page-rc era. Frozen here under the held gates.
        let structural_rc = {
            let surviving = self.collect_surviving_roots(&l2p_guards, target_start, target_end)?;
            let mut all_roots = clone_roots.clone();
            all_roots.extend_from_slice(&surviving);
            crate::db::apply::collect_paged_refcounts_for_roots(&self.page_store, &all_roots)?
        };
        let mut pages_with_birth: Vec<(PageId, Lsn, u32)> = Vec::new();
        for tree in &mut l2p_guards[target_start..target_end] {
            let root = tree.root();
            if root == crate::types::NULL_PAGE {
                continue;
            }
            pages_with_birth.extend(tree.collect_drop_pages_with_birth(root, &structural_rc)?);
        }
        let mut pages: Vec<PageId> = pages_with_birth.iter().map(|(p, _, _)| *p).collect();
        // dead-list: walk the volume's segment chain backward from its
        // tail. BFG: these segment pages are `allocate_run`-allocated and
        // are NEVER page-rc/tree-reachable, so the post-page-rc removal apply (which frees only
        // the `free_pages` tree set, not `pages`) cannot reclaim them — collect
        // them for the EAGER `free_idempotent` loop below (mirroring the v19
        // page-livelist chain), NOT into `pages`. (Pre-page-rc removal they were added to
        // `pages` but the page-rc cascade skipped them as untracked `prev == 0`,
        // so they already rode orphan-reclaim — the old comment claiming eager
        // apply-time release was inaccurate even then.) The chain also picks up
        // any in-memory buffer entries; those records are now obsolete and
        // discarded (the volume itself is gone).
        let deadlist_chain_pids: Vec<PageId> = {
            let tail = volume
                .dead_list_tail_pid
                .load(std::sync::atomic::Ordering::Acquire);
            if tail == crate::types::NULL_PAGE {
                Vec::new()
            } else {
                let page_store = self.page_store.clone();
                crate::deadlist::walk_chain_pages(tail, |pid| page_store.read_page(pid))?
            }
        };
        // Discard the volume's outstanding dead-list buffer — it
        // describes overwrites that targeted this volume's own LBAs,
        // which are about to disappear.
        let _ = volume.dead_list.drain();
        // BFG: collect this clone's page-livelist (v19) segment
        // chain so it can be EAGERLY freed once the manifest commit below
        // drops the entry. Segment pages are allocated via `allocate_run` and
        // are never page-rc-tracked, so routing them through `pages` / the
        // page-rc decref cascade is a no-op (`apply_drop_snapshot_pages` floors
        // a 0→-1 decref and skips at `prev == 0`). They must be freed DIRECTLY
        // — the same `free_idempotent` path condense and lineage GC use for
        // their old segments. Read the tail atomic NOW, before the commit
        // clears the entry; a crash between the commit and the free leaves the
        // segments as orphans `reclaim_orphan_pages` sweeps on the next open
        // (the same backstop the tree pages ride). The page-deadlist (v18)
        // chain is intentionally left on its existing path. The in-memory
        // livelist buffer is dropped with `volume`.
        let livelist_chain_pids: Vec<PageId> = {
            let tail = volume
                .page_live_list_tail_pid
                .load(std::sync::atomic::Ordering::Acquire);
            if tail == crate::types::NULL_PAGE {
                Vec::new()
            } else {
                let page_store = self.page_store.clone();
                crate::livelist::walk_chain_pages(tail, |pid| page_store.read_page(pid))?
            }
        };

        // BFG: for a CLONE, cross-check the page-rc
        // free-set against an independent C-exclusive reachability walk
        // BEFORE the irreversible cascade. Surviving roots = every OTHER
        // volume's live shard roots (read from the locked, just-flushed trees
        // in `l2p_guards`, NOT the on-disk manifest — other volumes' COW'd
        // roots are only refreshed into it at the commit below, so a stale
        // manifest root would manufacture a false premature) plus every
        // snapshot's roots. The drop+apply+snapshot_views write gates plus the
        // `force_compact_l2p_buffers` + `l2p_page_rc.flush()` above keep the
        // graph + page-rc quiescent and fold-consistent (the entry forced sync
        // is gone, buffer-backed journal; the page-rc `get_consistent` read carries the
        // fold-consistency); running before the WAL submit / manifest commit
        // means a Corruption abort leaves no half-applied drop. Page-rc stays
        // authoritative — this is a shadow assertion, NOT the free decision.
        // gated on the sticky
        // `VOLUME_FLAG_CLONE_LINEAGE` flag — set once at clone creation, never
        // cleared, surviving `PromotionComplete` (which clears `parent_vol_ord`)
        // — so a *promoted* ex-clone is ALSO covered. Promotion is lite (bumps
        // only the global PBA refcount + clears `parent_vol_ord`, does NOT
        // COW-divide the page tree or touch the L2P page-rc), so a promoted
        // ex-clone may still page-rc-SHARE L2P pages with its lineage; lineage tracking's
        // narrower `parent_vol_ord.is_some()` gate skipped it. Widening is
        // strictly additive (the flag is 0 on plain volumes, sticky on clones)
        // and SOUND: the reachability `exclusive` set below is birth-agnostic
        // C-exclusivity, so it handles the DAG hazards natively — descendant-share (a born>B
        // page shared with a promoted descendant E stays reachable from E, a
        // survivor → kept) and origin-fallthrough (a born≤B origin page sole-owned to this
        // volume → freed, and drops out of both livelist sides by birth). The
        // `missing→HARD` precondition (surviving_roots complete) is re-proven
        // for the promoted case on `check_clone_livelist_shadow` below. Page-rc
        // stays authoritative — still a shadow assertion, NOT the free decision.
        // The promoted-PBA decref (promoted-PBA reclaim) and the manifest commit both stamp this
        // LSN; read it once here — stable under the held `apply_gate.write` (no
        // commit advances `last_applied_lsn` while we hold it).
        let checkpoint_lsn = *self.last_applied_lsn.lock();
        // promoted-PBA reclaim: clone promotion-edge PBAs whose rc reached 0 and that no survivor
        // maps, surfaced to onyx. Declared here so it is in scope at the return
        // (stays empty for non-clones / rc-authoritative).
        let mut surfaced_freed: Vec<Pba> = Vec::new();
        // BFG: the authoritative, page-rc-independent free-set
        // frozen into the WAL op. Always `Some` now: a CLONE_LINEAGE drop gets
        // the reachability `exclusive` set cross-checked by
        // `check_clone_livelist_shadow` (the `if` arm); a non-clone (unflagged)
        // drop gets the same reachability `exclusive` set directly (the `else`
        // arm, page-rc removal / non-clone reachability — replaced the legacy page-rc cascade that page-rc removal deletes).
        let mut free_pages: Option<Vec<PageId>> = None;
        // promoted-PBA reclaim: the promoted-PBA edges to decref + the survivor data-PBA set are
        // COMPUTED here (read-only, under the held gates) but APPLIED post-commit
        // (see the post-commit decref block) — the refcount array writes data
        // pages in place, so a pre-commit decref-flush would make the decref
        // durable while the clone is still in the not-yet-committed manifest, and
        // a crash there is an rc undercount = premature free. Stays empty for
        // non-clones / rc-authoritative / no-promotion.
        let mut s0_promoted: Vec<Pba> = Vec::new();
        let mut s0_survivor_pbas: std::collections::HashSet<Pba> = std::collections::HashSet::new();
        if (volume.flags.load(std::sync::atomic::Ordering::Relaxed)
            & crate::manifest::VOLUME_FLAG_CLONE_LINEAGE)
            != 0
        {
            let surviving_roots =
                self.collect_surviving_roots(&l2p_guards, target_start, target_end)?;
            // reconstruct this clone's persistent page-livelist
            // (v19) ALLOC/FREE log as the THIRD independent ground truth the
            // shadow cross-checks (alongside the page-rc cascade and the
            // reachability walk). The on-disk segment chain alone UNDER-counts:
            // `force_compact_l2p_buffers` above drained the tree's `live_events`
            // witness into the in-memory `LiveListState` buffer but did NOT seal
            // it to the chain, and `drop_volume` never seals (the volume dies),
            // so records written since the last checkpoint flush live only in
            // the buffer. Union the sealed chain with a non-destructive `peek()`
            // of that buffer — exactly as `drop_snapshot` unions the source
            // volume's `page_dead_list.peek()` for its inheritor merge. The
            // drop+apply+snapshot_views write gates are held, so no concurrent
            // apply pushes records: `peek()` is a stable snapshot here.
            let live_records: Vec<crate::livelist::LiveRecord> = {
                let tail = volume
                    .page_live_list_tail_pid
                    .load(std::sync::atomic::Ordering::Acquire);
                let mut recs = if tail == crate::types::NULL_PAGE {
                    Vec::new()
                } else {
                    let page_store = self.page_store.clone();
                    crate::livelist::read_chain_records(tail, |p| page_store.read_page(p))?
                };
                recs.extend(volume.page_live_list.peek());
                recs
            };
            free_pages = Some(self.check_clone_livelist_shadow(
                vol_ord,
                volume.branched_at_lsn,
                &pages_with_birth,
                &clone_roots,
                &surviving_roots,
                &live_records,
            )?);

            // BFG: close the clone-promotion global
            // PBA-rc over-pin. The promotion walker incref'd +1 per clone-
            // referenced head_pba (incl. parent-COW-shared PBAs); under             // rc-neutral writes nothing ever decref'd them, so onyx's
            // delete-time "free iff rc==0" never reclaims the LV3 block. We undo
            // each logged +1, SURVIVOR-GATED: a promoted PBA still reachable from
            // a surviving root (any other volume's current L2P or any snapshot's
            // L2P — exactly `surviving_roots`) is decref'd but NEVER surfaced (a
            // survivor still maps it; surfacing it = PBA-level premature free,
            // the CRC class this port exists to kill).
            //
            // Excluded under rc-authoritative reclaim: there the hot path already
            // increfs per mapping, so onyx's `delete_blockmap_range` (run BEFORE
            // this drop) already decrefs the clone's mapping edges via
            // `range_delete`; a second promotion-edge decref here would drive a
            // still-parent-mapped PBA to 0 → premature free under the
            // rc-authoritative GC authority. The over-pin this actuator fixes
            // exists ONLY in the rc-neutral () regime.
            //
            // This block only READS (under the held gates, after the shadow
            // `?`): the promoted-PBA log → the multiset of edges to decref, and
            // `survivor_pbas` → the surface gate. The actual rc DECREF is applied
            // post-commit (the refcount array writes data pages in place, so a
            // pre-commit decref-flush would be durable while the clone is still
            // in the un-committed manifest — an rc undercount = premature free on
            // a crash there). See the post-commit decref block below.
            if !self.rc_authoritative_reclaim {
                let promoted: Vec<Pba> = {
                    let tail = volume
                        .promoted_log_tail_pid
                        .load(std::sync::atomic::Ordering::Acquire);
                    if tail == crate::types::NULL_PAGE {
                        Vec::new()
                    } else {
                        // Multiplicity preserved: `promotion.rs` emits one Alloc
                        // record per +1 edge (1:1), so a PBA logged K times is
                        // decref'd K times. The log is fsync'd synchronously on
                        // the live commit path (no un-sealed buffer to union,
                        // unlike the livelist) but is NOT re-emitted on WAL replay
                        // (`promotion.rs` faithfulness note), so a
                        // promote-then-crash-without-flush leaves a subset of
                        // edges un-logged → still over-pinned: a data-safe
                        // residual strictly smaller than the pre-promoted-PBA reclaim leak.
                        let page_store = self.page_store.clone();
                        crate::livelist::read_chain_records(tail, |p| page_store.read_page(p))?
                            .into_iter()
                            .filter(|r| r.kind == crate::livelist::LiveKind::Alloc)
                            .map(|r| r.pid) // the `pid` slot carries the raw Pba
                            .collect()
                    }
                };
                if !promoted.is_empty() {
                    // Survivor data-PBA set: head_pbas reachable from every
                    // surviving root (other volumes' current roots + every
                    // snapshot root — `surviving_roots`, frozen under the held
                    // gates). Reuses the warm shared page_cache (no per-root cache
                    // allocation) and the page_store walk — no l2p_guards index.
                    s0_survivor_pbas = crate::verify::reachable_l2p_head_pbas(
                        &self.page_store,
                        &self.page_cache,
                        &surviving_roots,
                    )?;
                    s0_promoted = promoted;
                }
            }
        } else {
            // BFG (non-clone reachability): non-clone (unflagged) volume drop. The
            // page-rc cascade was the legacy free-source here (`free_pages =
            // None`); page-rc removal deletes it, so freeze the page-rc-INDEPENDENT
            // reachability free-set instead — exactly the birth-agnostic
            // exclusivity the CLONE_LINEAGE arm computes, minus the livelist
            // cross-check (non-clones emit no livelist records). `exclusive`
            // = pages reachable only from the dying volume = pages no survivor
            // maps. An unflagged volume that was a clone *source* still shares
            // pages with a promoted ex-clone at rc>1; `surviving_roots` includes
            // that ex-clone's roots, so the shared pages drop out of `exclusive`
            // (they stay live), matching what the rc cascade did (decref 2→1, no
            // free). For a truly private volume every page is exclusive. The
            // result is a subset of `pages` (the rc-cascade frontier): an
            // A-exclusive page is rc==1, reached via the rc==1 edges
            // `collect_drop_pages_with_birth` recurses, so it is in `pages` —
            // satisfying the `set ⊆ pages` apply invariant.
            let surviving_roots =
                self.collect_surviving_roots(&l2p_guards, target_start, target_end)?;
            let reachable_clone = crate::verify::reachable_l2p_pages(&self.page_store, &clone_roots)?;
            let reachable_surv =
                crate::verify::reachable_l2p_pages(&self.page_store, &surviving_roots)?;
            let mut exclusive: Vec<PageId> = reachable_clone
                .difference(&reachable_surv)
                .copied()
                .collect();
            exclusive.sort_unstable();
            free_pages = Some(exclusive);
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
        // `force_compact_l2p_buffers()` above folded every surviving volume's
        // buffer, pushing snapshot-pinned page-deaths into their accumulators.
        // This commit makes those folded roots durable and advances
        // `checkpoint_lsn`, so the deaths those roots imply must be sealed into
        // the same commit. Otherwise they survive only in volatile RAM, and a
        // hard crash loses them before a later `drop_snapshot` can free them.
        // below. The dropped volume itself is skipped (its deadlist is freed by
        // the cascade after the commit).
        let survivors: Vec<Arc<Volume>> = volumes_snap
            .iter()
            .filter(|v| v.ord != vol_ord)
            .cloned()
            .collect();
        self.seal_all_page_dead_lists(&survivors, checkpoint_lsn)?;
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
            // buffer-backed journal: no forced BFG sync ran ahead of this commit, so it is the
            // checkpoint that makes the flushes above durable and must advance
            // the lifecycle/buffer replay watermarks. CRITICAL for clone drops:
            // the `flush_all_refcount_shards()` above force-folded the clone's
            // PromotionChunk increfs into the durable refcount array — without
            // bumping `lifecycle_replay_seq` here, reopen would re-replay those
            // increfs on top of the now-durable array and double-count them. (The
            // promoted-PBA reclaim promotion-edge DECREFS are applied post-commit and are NOT
            // lifecycle ops, so they are never replayed either way.) The
            // `DropVolume` op is submitted AFTER this commit, so its seq is
            // correctly excluded here and replayed on the next open. Held under
            // `apply_gate.write`. See `stamp_replay_watermarks` (its doc names
            // this exact PromotionChunk hazard).
            self.stamp_replay_watermarks(&mut mstate.manifest);
            let manifest = mstate.manifest.clone();
            mstate.store.commit(&manifest)?;
            dedup_update
        };
        commit_l2p_checkpoint(&mut l2p_guards, dedup_generation)?;
        commit_refcount_checkpoint(&self.refcount_shards, dedup_generation)?;
        self.finish_dedup_manifest_update(dedup_update, dedup_generation)?;
        drop(l2p_guards);
        // H1 tripwire: this commit advanced checkpoint_lsn; no surviving volume
        // may retain a page-death the durable roots imply (death_lsn <= cut).
        #[cfg(debug_assertions)]
        self.debug_assert_page_deaths_sealed(&survivors, checkpoint_lsn);

        let lifecycle_op = crate::lifecycle_log::LifecycleOp::DropVolume {
            ord: vol_ord,
            pages: pages.clone(),
            free_pages: free_pages.clone(),
        };
        let lsn = self.submit_lifecycle_op(&lifecycle_op)?;
        _bfg_guard.record_lsn(lsn);
        self.faults.inject(FaultPoint::CommitPostWalBeforeApply)?;
        // Fault window specific to drop_volume: WAL record durable, no
        // page decref has touched disk yet. Recovery re-drives the full
        // cascade from the WAL op's inlined `pages` list.
        self.faults
            .inject(FaultPoint::DropVolumePostWalBeforeApply)?;
        self.wait_for_global_apply_turn(lsn)?;

        let pages_freed = apply_drop_volume(
            &self.page_store,
            lsn,
            &pages,
            free_pages.as_deref(),
        )?;
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
        // BFG: eagerly free the clone's page-livelist segment
        // chain (collected above, before the manifest commit dropped the
        // entry). `free_idempotent` is crash-safe + idempotent — an already-
        // freed pid (crash mid-loop) returns `Ok(false)`, and any pid not yet
        // freed on a crash is an orphan `reclaim_orphan_pages` sweeps on open.
        // Invalidate the cache so a stale segment byte can't shadow a recycled
        // allocation. The freed pages drain via `reclaim_freed_pages` below.
        for &pid in &livelist_chain_pids {
            self.page_store.free_idempotent(pid, lsn)?;
            self.page_cache.invalidate(pid);
        }
        // BFG: eagerly free the volume's page-deadlist segment chain too
        // (collected above) — same crash-safe `free_idempotent` + invalidate as
        // the livelist chain. The dropped volume has no live snapshots (drop
        // refuses them), so the chain is exclusively its own; post-page-rc removal apply no
        // longer reaches these untracked segment pages. Without this they leak to
        // `reclaim_orphan_pages` on the next open (the pre-page-rc removal behavior).
        for &pid in &deadlist_chain_pids {
            self.page_store.free_idempotent(pid, lsn)?;
            self.page_cache.invalidate(pid);
        }

        // BFG: APPLY the promoted-PBA decrefs now —
        // POST-commit. The refcount array writes data pages IN PLACE (only
        // previously-hole `page_idx` slots get fresh pids), so staging the decref
        // + flushing it BEFORE the manifest commit would make it durable while
        // the clone is still listed in the not-yet-committed manifest; a crash
        // there is an rc undercount for a live clone = premature free (the CRC
        // class this port kills). Applying it after `store.commit` removed the
        // clone makes the dangerous "decref durable + clone present" state
        // unreachable: the only crash states are (clone present, no decref)
        // [before the commit] and (clone gone, decref maybe-durable) [here]. A
        // crash mid-flush loses the decref = data-safe over-pin (never
        // corruption), strictly smaller than the pre-promoted-PBA reclaim always-leak. On the
        // no-crash path the committed manifest's refcount roots point at the
        // in-place-updated data pages, so a completed flush is durable + correct
        // (the decref is non-WAL and not replay-reconstructed). The drop/apply/
        // snapshot_views write gates are still held (function scope), so the
        // survivor state is unchanged since the pre-commit `s0_survivor_pbas`
        // walk. `checkpoint_lsn` matches the committed manifest's
        // `refcount_durable_seq`, so the folded page generation stays consistent
        // with the manifest watermark.
        if !s0_promoted.is_empty() {
            for &pba in &s0_promoted {
                let sid = crate::db::apply::shard_for_key(&self.refcount_shards, pba);
                let (prev, new) =
                    self.refcount_shards[sid]
                        .rc
                        .stage_unskippable(_bfg_guard.bfg(), pba, -1, checkpoint_lsn)?;
                // Surface iff this decref reached 0 from a positive count AND no
                // survivor maps it. `prev > 0` matches `stage`'s documented
                // `freed_pba` contract and guards against a floored decref-past-0
                // (which returns new==0 with prev==0) being surfaced as a
                // spurious free. `new==0` ⇒ no other rc edge survives (a sibling
                // clone's promotion edge or a dedup membership would keep rc>0);
                // a survivor mapping it via an rc-neutral exclusive write is
                // caught by `s0_survivor_pbas` → never surfaced.
                if new == 0 && prev > 0 && !s0_survivor_pbas.contains(&pba) {
                    surfaced_freed.push(pba);
                }
            }
            self.flush_all_refcount_shards()?;
        }
        // Free the clone's promoted-PBA log segment chain. Like the page-livelist
        // chain above, these segments are page-rc-UNtracked and not in `pages`,
        // so they must be freed DIRECTLY (else they leak until the next open's
        // `reclaim_orphan_pages`). `free_idempotent` is crash-safe (a crash mid-
        // loop leaves the rest as orphans reclaimed on open). Read the tail off
        // `volume` before the `drop(volume)` below.
        {
            let tail = volume
                .promoted_log_tail_pid
                .load(std::sync::atomic::Ordering::Acquire);
            if tail != crate::types::NULL_PAGE {
                let page_store = self.page_store.clone();
                let chain = crate::livelist::walk_chain_pages(tail, |p| page_store.read_page(p))?;
                for pid in chain {
                    self.page_store.free_idempotent(pid, lsn)?;
                    self.page_cache.invalidate(pid);
                }
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
            freed_pbas: surfaced_freed,
        }))
    }

    /// BFG: cross-check the per-clone page-livelist
    /// free model against the structural ground truth `drop_volume` still
    /// frees from — the page-rc `collect_drop_pages` cascade. Page-rc stays
    /// AUTHORITATIVE; this only observes and aborts on a soundness divergence,
    /// so a clone-churn soak can prove the livelist model equals page-rc
    /// before deletes page-rc and makes the livelist the sole free
    /// source.
    ///
    /// For a clone C with `B = branched_at_lsn`, the set of L2P pages this
    /// drop releases — `freed_with_birth` entries with `rc == 1`, the pages
    /// whose `-1` decref reaches zero (`rc > 1` are the decref-only shared
    /// boundary, kept) — must equal the set of pages reachable from C's roots
    /// that are **C-exclusive**: reachable from no *surviving* manifest root
    /// (any other volume head or any snapshot, including snapshots/clones of
    /// C). `birth_lsn` does NOT gate the free for clones — cross-volume DAG
    /// sharing makes the single-vol `birth > B` predicate unsound in both
    /// directions (the audit found premature-free and leak
    /// counterexamples, both at a *legal* drop after `PromotionComplete`
    /// clears `parent_vol_ord` while leaving page-rc sharing intact). Birth is
    /// carried only to partition the freed set into origin (`birth <= B`) vs
    /// clone-private (`birth > B`) for diagnostics and the livelist
    /// substrate.
    ///
    /// The RHS is computed by an INDEPENDENT reachability walk
    /// ([`crate::verify::reachable_l2p_pages`], a set-difference), a different
    /// code path from the page-rc cascade so the assertion is
    /// non-tautological. Posture mirrors the `drop_snapshot` page-deadlist
    /// shadow before its escalation:
    ///   - **premature** (page-rc would free a page still reachable from a
    ///     surviving root — the premature-free case the port exists to kill) →
    ///     HARD [`MetaDbError::Corruption`]. Only fires on a real page-rc
    ///     under-count: a reachability-walk error can only shrink this set
    ///     (an omitted survivor edge enlarges `exclusive`, never the
    ///     premature difference), so the HARD direction can't false-fire.
    ///   - **missing** (the exclusivity walk frees a page page-rc keeps live)
    ///     → HARD [`MetaDbError::Corruption`] (escalation, was a
    ///     soft warn). **Soundness precondition**: `surviving_roots` is COMPLETE
    ///     for a legal drop of any `VOLUME_FLAG_CLONE_LINEAGE` volume — every
    ///     OTHER live volume's just-flushed root + every snapshot root, frozen
    ///     under drop_gate.write + apply_gate.write + snapshot_views.write. An
    ///     incomplete `surviving_roots` would over-enlarge `exclusive` and
    ///     manufacture a false `missing`. **re-proof for the
    ///     widened (promoted-ex-clone) gate**: promotion (`apply/promotion.rs`)
    ///     edits only the `parent_vol_ord`/`promotion_cursor` RwLocks and NEVER
    ///     removes a volume from `self.volumes`, so a promoted ex-clone's
    ///     descendants AND its former parent are all still in
    ///     `volumes_snapshot()` → their roots are in `surviving_roots` →
    ///     correctly subtracted. A promoted descendant escaping the
    ///     `parent_vol_ord == Some(self)` entry-rejection is harmless (it is
    ///     still a survivor). reachability is transitive, so arbitrary DAG depth
    ///     is covered. Checked AFTER premature so `structural_free == exclusive`
    ///     holds for the livelist reduction below.
    ///   - **livelist cross-check** (, NEW) → HARD. With the two
    ///     checks above passing, `structural_free == exclusive`, so the
    ///     clone-private part of the free-set is `exclusive ∩ {birth > B}`.
    ///     The persistent v19 livelist's live-ALLOC set (`LA`, an INDEPENDENT
    ///     on-disk source) must reproduce exactly that: `LA \ surviving_roots
    ///     == { (pid,birth) ∈ structural_free : birth > B }`. Keyed on
    ///     `(pid, birth)` (a pid can recur across births). This re-validates
    ///     the substrate invariant `LA == reachable(C) ∩ {birth > B}` through
    ///     the persistent chain at drop time — disk vs reachability vs page-rc,
    ///     three independent ground truths. The born≤B origin-fallthrough
    ///     pages page-rc frees are NOT in the livelist by construction and drop
    ///     out of both sides.
    /// Returns the reachability `exclusive` set (sorted). With premature and
    /// missing cases both treated as hard errors, `Ok` means it equals
    /// `structural_free`, making it the authoritative page-rc-independent
    /// free-set for CLONE_LINEAGE drops.
    fn check_clone_livelist_shadow(
        &self,
        vol_ord: VolumeOrdinal,
        branched_at_lsn: Lsn,
        freed_with_birth: &[(PageId, Lsn, u32)],
        clone_roots: &[PageId],
        surviving_roots: &[PageId],
        live_records: &[crate::livelist::LiveRecord],
    ) -> Result<Vec<PageId>> {
        use std::collections::HashSet;

        // LHS — page-rc structural FREE set (authoritative): pages whose -1
        // decref reaches zero (rc == 1 at plan time). rc > 1 entries are the
        // decref-only shared boundary, kept, so excluded here.
        let structural_free: HashSet<PageId> = freed_with_birth
            .iter()
            .filter(|(_, _, rc)| *rc == 1)
            .map(|(pid, _, _)| *pid)
            .collect();

        // RHS — independent "C-exclusive reachable" set: reachable from C's
        // roots and from NO surviving root.
        let survivor_reachable =
            crate::verify::reachable_l2p_pages(&self.page_store, surviving_roots)?;
        let clone_reachable = crate::verify::reachable_l2p_pages(&self.page_store, clone_roots)?;
        let exclusive: HashSet<PageId> = clone_reachable
            .difference(&survivor_reachable)
            .copied()
            .collect();

        // origin vs clone-private partition of the freed set (diagnostics
        // only; does not gate the free).
        let (origin, clone_private) = freed_with_birth
            .iter()
            .filter(|(_, _, rc)| *rc == 1)
            .fold((0usize, 0usize), |(o, c), (_, birth, _)| {
                if *birth <= branched_at_lsn {
                    (o + 1, c)
                } else {
                    (o, c + 1)
                }
            });

        let mut premature: Vec<PageId> =
            structural_free.difference(&exclusive).copied().collect();
        if !premature.is_empty() {
            premature.sort_unstable();
            return Err(MetaDbError::Corruption(format!(
                "drop_volume {vol_ord} (clone, B={branched_at_lsn}): page-rc would free {} \
                 page(s) still reachable from a surviving root (PREMATURE FREE): {premature:?} \
                 (free={} exclusive={} origin={origin} clone_private={clone_private})",
                premature.len(),
                structural_free.len(),
                exclusive.len(),
            )));
        }
        let mut missing: Vec<PageId> =
            exclusive.difference(&structural_free).copied().collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            return Err(MetaDbError::Corruption(format!(
                "drop_volume {vol_ord} (clone, B={branched_at_lsn}): C-exclusive reachability \
                 frees {} page(s) page-rc keeps live (COMPLETENESS HOLE / page-rc leak): \
                 {missing:?} (free={} exclusive={} origin={origin} clone_private={clone_private})",
                missing.len(),
                structural_free.len(),
                exclusive.len(),
            )));
        }

        // Livelist cross-check (). `LA` = the persistent v19
        // live-ALLOC set, keyed on (pid, birth) to match the substrate
        // invariant (`live_allocs` can legitimately carry one pid twice across
        // births — a pid-only projection would mask a real (pid,birth)
        // divergence). `live_allocs` also HARD-errors on a FREE with no prior
        // ALLOC (chain corruption).
        let la = crate::livelist::live_allocs(live_records.to_vec())?;
        let la_exclusive: HashSet<(PageId, Lsn)> = la
            .iter()
            .filter(|r| !survivor_reachable.contains(&r.pid))
            .map(|r| (r.pid, r.birth_lsn))
            .collect();
        let clone_private_freed: HashSet<(PageId, Lsn)> = freed_with_birth
            .iter()
            .filter(|(_, birth, rc)| *rc == 1 && *birth > branched_at_lsn)
            .map(|(pid, birth, _)| (*pid, *birth))
            .collect();
        if la_exclusive != clone_private_freed {
            let mut over: Vec<(PageId, Lsn)> = la_exclusive
                .difference(&clone_private_freed)
                .copied()
                .collect();
            let mut under: Vec<(PageId, Lsn)> = clone_private_freed
                .difference(&la_exclusive)
                .copied()
                .collect();
            over.sort_unstable();
            under.sort_unstable();
            return Err(MetaDbError::Corruption(format!(
                "drop_volume {vol_ord} (clone, B={branched_at_lsn}): page-livelist disagrees with \
                 the page-rc clone-private free-set. over-logged (livelist names, page-rc keeps): \
                 {over:?}; under-logged (page-rc frees, livelist omits): {under:?} \
                 (la_exclusive={} clone_private_freed={})",
                la_exclusive.len(),
                clone_private_freed.len(),
            )));
        }
        // All checks clean ⇒ exclusive == structural_free. Hand it back
        // sorted (deterministic WAL `free_pages`) as the free-set authoritative
        // free-set.
        let mut free_pages: Vec<PageId> = exclusive.into_iter().collect();
        free_pages.sort_unstable();
        Ok(free_pages)
    }

    /// Test shim: drive [`Db::check_clone_livelist_shadow`] with
    /// caller-crafted inputs so the divergence detectors can be exercised
    /// directly (the production call site only ever passes a self-consistent
    /// free-set). `db::tests` is a sibling module and cannot reach the
    /// private method otherwise.
    #[cfg(test)]
    pub(in crate::db) fn test_check_clone_livelist_shadow(
        &self,
        vol_ord: VolumeOrdinal,
        branched_at_lsn: Lsn,
        freed_with_birth: &[(PageId, Lsn, u32)],
        clone_roots: &[PageId],
        surviving_roots: &[PageId],
        live_records: &[crate::livelist::LiveRecord],
    ) -> Result<()> {
        self.check_clone_livelist_shadow(
            vol_ord,
            branched_at_lsn,
            freed_with_birth,
            clone_roots,
            surviving_roots,
            live_records,
        )
        .map(|_| ())
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
    /// The forced BFG sync that used to run at entry is gone. Its sole job was
    /// to drain in-flight flush IO so a whole-page rc RMW could not be clobbered
    /// by a concurrent whole-page flush write. BFG deleted per-L2P-page refcounting entirely:
    /// the clone no longer touches any page-rc, and the source pages it
    /// shares are never modified by the clone op — there is nothing left to
    /// clobber. Removing the forced sync stops every clone from driving +
    /// waiting on a full sync cycle while holding `drop_gate.write` (the
    /// snapshot-scaling barrier). Crash durability is unchanged: the
    /// `CloneVolume` WAL record replays the clone-shard build, and the next
    /// natural flush commits the manifest.
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
        // No forced BFG sync here. The page-rc incref is now a `stage` into the
        // array (clobber-free), so the
        // sync-drain barrier is unnecessary. `bfg.enter()` pins the
        // current Open BFG; `BfgStateMachine`'s `closing_open` flag makes
        // it wait out (not race) a concurrent background roll, so it is
        // safe to enter without first rolling the BFG ourselves.
        //
        // gate-shrink: `_bfg_guard.record_lsn(lsn)` (below)
        // records this lifecycle op's WAL LSN into `slot_max_lsn(open_bfg)`
        // so `run_sync_cycle_body`'s `wal_checkpoint = slot_max_lsn(bfg)`
        // watermark reflects it and the WAL segment is eventually pruned.
        let _bfg_guard = self.bfg.enter();
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
                // v19: sticky clone-lineage flag — this volume is a clone, so
                // it owns a per-clone page-livelist. Set in the probe too so
                // the capacity check sizes the real (flagged) row (cosmetic —
                // the flag does not change the row width, but keep it honest).
                flags: crate::manifest::VOLUME_FLAG_CLONE_LINEAGE,
                dead_list_head_pid: crate::types::NULL_PAGE,
                dead_list_tail_pid: crate::types::NULL_PAGE,
                parent_vol_ord: Some(entry.vol_ord),
                branched_at_lsn: entry.created_lsn,
                promotion_cursor: None,
                page_dead_list_head_pid: crate::types::NULL_PAGE,
                page_dead_list_tail_pid: crate::types::NULL_PAGE,
                page_live_list_head_pid: crate::types::NULL_PAGE,
                page_live_list_tail_pid: crate::types::NULL_PAGE,
                promoted_log_head_pid: crate::types::NULL_PAGE,
                promoted_log_tail_pid: crate::types::NULL_PAGE,
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
        _bfg_guard.record_lsn(lsn);
        self.faults.inject(FaultPoint::CommitPostWalBeforeApply)?;

        // Under our two write gates no other commit sits between submit
        // and apply; the cvar wait is defensive and matches
        // `create_volume` / `drop_snapshot`.
        self.wait_for_global_apply_turn(lsn)?;

        // BFG: per-L2P-page refcounting is deleted, so the clone no
        // longer increfs the source shard roots' page-rc. The source pages are
        // not modified by the clone op (the clone just opens fresh `PagedL2p`s
        // pointing at the shared source roots, and COWs down on first write), so
        // there is no stale-byte hazard and no cache invalidation to perform.
        self.faults
            .inject(FaultPoint::CommitPostApplyBeforeLsnBump)?;

        let (shards, actual_roots) = build_clone_volume_shards(
            &src_shard_roots,
            &self.page_store,
            &self.page_cache,
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
                    crate::manifest::VOLUME_FLAG_CLONE_LINEAGE,
                    crate::types::NULL_PAGE,
                    crate::types::NULL_PAGE,
                    Some(src_ord),
                    branched_at_lsn,
                    None,
                    crate::types::NULL_PAGE,
                    crate::types::NULL_PAGE,
                    crate::types::NULL_PAGE,
                    crate::types::NULL_PAGE,
                    // v20 promoted-PBA log anchors (NULL until a promotion
                    // walker emits — fresh clone has none).
                    crate::types::NULL_PAGE,
                    crate::types::NULL_PAGE,
                    Some(branched_at_lsn),
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
                // v19: sticky clone-lineage flag (BFG). Never
                // cleared by promotion, so the per-clone page-livelist keeps
                // recording for promoted ex-clones too.
                flags: crate::manifest::VOLUME_FLAG_CLONE_LINEAGE,
                // Clones start with an empty dead-list. The parent's
                // chain still correctly describes the parent's overwrite
                // history; the clone's L2P starts fresh so its first
                // overwrite is recorded against its own chain (not the
                // parent's).
                dead_list_head_pid: crate::types::NULL_PAGE,
                dead_list_tail_pid: crate::types::NULL_PAGE,
                // lineage tracking. `parent_vol_ord` + `branched_at_lsn`
                // mark this volume as a clone whose Lineage GC must consult the
                // parent (cross-volume snap_pin: lineage pinning). `promotion_cursor`
                // stays `None` until promotion walker arms the background walker that
                // increfs the global rc for each shared PBA, which is what
                // ultimately lets the parent be dropped independently.
                parent_vol_ord: Some(src_ord),
                branched_at_lsn,
                promotion_cursor: None,
                page_dead_list_head_pid: crate::types::NULL_PAGE,
                page_dead_list_tail_pid: crate::types::NULL_PAGE,
                page_live_list_head_pid: crate::types::NULL_PAGE,
                page_live_list_tail_pid: crate::types::NULL_PAGE,
                promoted_log_head_pid: crate::types::NULL_PAGE,
                promoted_log_tail_pid: crate::types::NULL_PAGE,
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
    /// inserted at create / open time and buffer-backed journal never removes it.
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
    /// snap is live on the volume. RangeDelete is PBA
    /// rc-neutral, so it does not consult this cache for refcount work.
    pub(super) fn snap_info_for_vol(&self, vol: VolumeOrdinal) -> Vec<SnapInfo> {
        self.snap_info_cache
            .lock()
            .get(&vol)
            .cloned()
            .unwrap_or_default()
    }

    /// Youngest live snapshot's `created_lsn` for `vol`, or `None` when the
    /// volume has no live snapshot. This is the birth threshold for the COW kill
    /// decision: a page reachable from the head with
    /// `birth_lsn <= youngest_snap` is still pinned by that snapshot.
    /// `Some(0)` (a snapshot taken before the first op on the bootstrap
    /// volume) is distinct from `None` (no snapshot) — the former still
    /// pins genesis pages born at lsn 0.
    pub(crate) fn youngest_snap(&self, vol: VolumeOrdinal) -> Option<Lsn> {
        self.snap_info_cache
            .lock()
            .get(&vol)
            .and_then(|infos| infos.iter().map(|s| s.created_lsn).max())
    }

    /// v21 (snapshot watermark): the live snapshots' `capture_watermark`s for `vol`, the
    /// operand of the birth COW-kill (`tree.set_snapshot_wms`). The fold path
    /// passes this in; the COW-kill filters it per dying-page lsn
    /// (`youngest_snap_below`). Empty when the volume has no live snapshot.
    pub(crate) fn snapshot_wms(&self, vol: VolumeOrdinal) -> Vec<Lsn> {
        self.snap_info_cache
            .lock()
            .get(&vol)
            .map(|infos| infos.iter().map(|s| s.capture_watermark).collect())
            .unwrap_or_default()
    }

    /// v21 (BFG): the page-rc-INDEPENDENT pinner-LSN set
    /// for `vol`'s CLONE COW-kill (`tree.set_clone_cow_pinners`). Empty for a
    /// non-clone (it never reads this — its COW-kill uses `snapshot_wms`). For a
    /// clone C with `B_C = branched_at_lsn`:
    ///
    /// ```text
    /// {B_C} ∪ {capture_watermark(S) : S a live snapshot of C}
    ///       ∪ {branched_at_lsn(V) : V another clone-lineage volume, branched_at_lsn(V) > B_C}
    /// ```
    ///
    /// * `{B_C}` pins origin pages (`birth <= B_C`, shared with the parent / an
    ///   ancestor snapshot).
    /// * C's own snapshot watermarks (reused from `snapshot_wms`) pin pages C
    ///   captured.
    /// * the descendant branch points pin born>B_C pages a surviving descendant
    ///   clone still references. We use the CONSERVATIVE superset "all other
    ///   clone-lineage volumes that branched after B_C" rather than
    ///   `parent_vol_ord == Some(C)`, because a PROMOTED ex-descendant has its
    ///   `parent_vol_ord` cleared and can no longer be identified as C's
    ///   descendant — yet it still shares C's pages (promotion bumps only global
    ///   PBA rc, never restructures the page tree). Missing it is the descendant-share
    ///   premature-free case. Every true descendant `V` has
    ///   `branched_at_lsn(V) = (a snapshot in C's lineage).created_lsn > B_C`, and
    ///   `branched_at_lsn(V) >= birth(P)` for every C-page P that V shares, so the
    ///   set is a complete superset; including unrelated clones only over-pins
    ///   (benign). Scanned from the in-memory `self.volumes` (carries
    ///   `branched_at_lsn` + the sticky CLONE_LINEAGE flag) so no `manifest_state`
    ///   lock is taken inside the fold.
    pub(crate) fn clone_cow_pinners(&self, vol: VolumeOrdinal) -> Vec<Lsn> {
        // Compute the clone's own snapshot watermarks FIRST (takes
        // `snap_info_cache`), then take `volumes.read()` for `B_C` + descendant
        // branch points — the two locks are never held simultaneously.
        let own_snap_wms = self.snapshot_wms(vol);
        let volumes = self.volumes.read();
        clone_cow_pinners_from(&volumes, vol, own_snap_wms)
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
                capture_watermark: s.capture_watermark,
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
                capture_watermark: snap.capture_watermark,
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
    /// `durable_override` is durable-seq rollout (per-shard durable-seq) plumbing: callers
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

/// Build the clone COW-kill clone COW-kill pinner-LSN set from a borrowed volume map plus
/// the clone's own snapshot watermarks (`own_snap_wms`). Free helper so both
/// [`Db::clone_cow_pinners`] (locks `self.volumes`) and `apply_op_bare` (already
/// holds a `&HashMap`) share one definition without a double `volumes.read()`.
/// Returns empty for a non-clone — its COW-kill never reads this set. See
/// [`Db::clone_cow_pinners`] for the completeness argument. `own_snap_wms` is
/// consumed and extended (no extra allocation).
pub(in crate::db) fn clone_cow_pinners_from(
    volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
    vol: VolumeOrdinal,
    mut own_snap_wms: Vec<Lsn>,
) -> Vec<Lsn> {
    use std::sync::atomic::Ordering;
    let Some(target) = volumes.get(&vol) else {
        return Vec::new();
    };
    if target.flags.load(Ordering::Relaxed) & crate::manifest::VOLUME_FLAG_CLONE_LINEAGE != 0 {
        // CLONE arm (clone COW-kill): `{B_C} ∪ own-snap watermarks ∪ {descendant branch
        // points > B_C}`.
        let b_c = target.branched_at_lsn;
        own_snap_wms.push(b_c);
        for (&other_ord, other) in volumes.iter() {
            if other_ord != vol
                && other.flags.load(Ordering::Relaxed) & crate::manifest::VOLUME_FLAG_CLONE_LINEAGE
                    != 0
                && other.branched_at_lsn > b_c
            {
                own_snap_wms.push(other.branched_at_lsn);
            }
        }
        return own_snap_wms;
    }
    // BFG (clone-SOURCE arm — replaces the deleted page-rc `rc > 1`
    // floor for `repro_drop_free`): a NON-clone volume can still share L2P pages
    // with a clone of one of its (possibly already-dropped) snapshots — onyx
    // allows dropping a snapshot that has live clones, and promotion is lite
    // (clears `parent_vol_ord`, never COW-divides the page tree), so after the
    // linking snapshot is gone nothing else pins those shared pages. Without a
    // pin the non-clone COW-kill would recycle a page a clone still reads =
    // premature free. Pin conservatively on EVERY clone's branch point: a clone
    // born at `B` shares all origin pages born `<= B`, and a precise lineage walk
    // can't see a promoted ex-clone (sticky flag, cleared `parent_vol_ord`), so —
    // exactly as the CLONE arm's descendant term — take the all-clone superset.
    // Empty when no clones exist → ZERO change to the common non-clone path. The
    // resulting over-COW on an unrelated volume is benign: it only fires while a
    // clone is live, and then every snapshot drop routes to the clone-involved snapshot drop reachability
    // free-set (computed from current structure, idempotent free), never a
    // deadlist double-free.
    let mut clone_branches = Vec::new();
    for other in volumes.values() {
        if other.flags.load(Ordering::Relaxed) & crate::manifest::VOLUME_FLAG_CLONE_LINEAGE != 0 {
            clone_branches.push(other.branched_at_lsn);
        }
    }
    clone_branches
}

/// Result of [`Db::drop_volume`].
#[derive(Clone, Debug)]
pub struct DropVolumeReport {
    /// Ordinal of the volume that was dropped.
    pub vol_ord: VolumeOrdinal,
    /// Number of metadb pages released back to the page store.
    pub pages_freed: usize,
    /// BFG: clone promotion-edge PBAs whose global
    /// PBA-rc reached 0 during the survivor-gated decref AND that no surviving
    /// root still maps. Surfaced to onyx as `pba_freed` cleanups so the LV3
    /// block is reclaimed (closing the clone-promotion over-pin leak). Empty
    /// for non-clone volumes and under rc-authoritative reclaim.
    pub freed_pbas: Vec<Pba>,
}
