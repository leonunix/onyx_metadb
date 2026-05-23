use super::*;

impl Db {
    /// Create a fresh database in `root_dir` using the default config.
    pub fn create(root_dir: &Path) -> Result<Self> {
        Self::create_with_config(Config::new(root_dir))
    }

    /// Create a fresh database with an explicit config.
    pub fn create_with_config(cfg: Config) -> Result<Self> {
        Self::create_with_config_and_faults(cfg, FaultController::disabled())
    }

    /// As [`create`](Self::create) but with an injectable fault controller.
    pub fn create_with_faults(root_dir: &Path, faults: Arc<FaultController>) -> Result<Self> {
        Self::create_with_config_and_faults(Config::new(root_dir), faults)
    }

    /// As [`create_with_config`](Self::create_with_config) but with an
    /// injectable fault controller.
    pub fn create_with_config_and_faults(
        cfg: Config,
        faults: Arc<FaultController>,
    ) -> Result<Self> {
        let shard_count = validate_shard_count(cfg.shards_per_partition)?;
        let dedup_shards = validate_dedup_shards(cfg.dedup_shards)?;
        std::fs::create_dir_all(&cfg.path)?;
        let pages_path = page_file(&cfg.path);
        let page_store = Arc::new(PageStore::create_with_grow_chunk_and_bg_cap(
            &pages_path,
            cfg.page_grow_chunk_pages,
            cfg.io_submitter_bg_inflight_cap,
        )?);
        let page_cache = Arc::new(PageCache::new_with_pin_budget(
            page_store.clone(),
            cfg.page_cache_bytes,
            cfg.index_pin_bytes,
        ));
        let metrics = Arc::new(MetaMetrics::new());
        page_store.attach_metrics(metrics.clone());
        let (mut manifest_store, mut manifest) =
            ManifestStore::open_or_create(page_store.clone(), faults.clone())?;
        let (l2p_shards, l2p_roots) = create_l2p_shards(
            page_store.clone(),
            page_cache.clone(),
            shard_count,
            metrics.clone(),
            cfg.l2p_buffer_enabled,
        )?;
        let (refcount_shards, refcount_roots) = create_shards(
            page_store.clone(),
            page_cache.clone(),
            shard_count,
            metrics.clone(),
        )?;
        let dedup_index = Arc::new(crate::dedup::DedupIndex::create(
            page_store.clone(),
            page_cache.clone(),
            cfg.dedup_cuckoo_buckets,
            cfg.dedup_l1_cache_entries,
            // Two stable seeds — recorded in the meta page so reopen
            // sees the same hash partitioning. Any non-zero distinct
            // pair works.
            0xDEAD_BEEF_CAFE_F00D,
            0x1234_5678_ABCD_EF01,
        )?);
        manifest.body_version = MANIFEST_BODY_VERSION;
        let refcount_count = refcount_roots.len();
        manifest.refcount_shard_roots = refcount_roots;
        // Fresh database: no flush has happened yet, every shard's
        // durable_seq is 0 (matches the empty `checkpoint_lsn`).
        manifest.refcount_durable_seq = vec![0; refcount_count].into_boxed_slice();
        manifest.dedup_shards = dedup_shards;
        // dedup_index: cuckoo meta page id stored under the legacy
        // `dedup_index_shard_heads` slot, single-element box for
        // compat with the existing decoder. The retired
        // `dedup_reverse_shard_heads` slot is gone (schema v9).
        manifest.dedup_index_shard_heads =
            vec![vec![dedup_index.meta_page_id()].into_boxed_slice()].into_boxed_slice();
        // Seed the bootstrap volume so open() / flush() can route
        // through the same volumes table the live `Db` manages.
        let bootstrap_shard_count = l2p_roots.len();
        manifest.volumes = vec![VolumeEntry {
            ord: BOOTSTRAP_VOLUME_ORD,
            shard_count: bootstrap_shard_count as u32,
            l2p_shard_roots: l2p_roots,
            l2p_shard_durable_seq: vec![0; bootstrap_shard_count].into_boxed_slice(),
            created_lsn: 0,
            flags: 0,
            dead_list_head_pid: crate::types::NULL_PAGE,
            dead_list_tail_pid: crate::types::NULL_PAGE,
            parent_vol_ord: None,
            branched_at_lsn: 0,
            promotion_cursor: None,
        }];
        manifest.next_volume_ord = BOOTSTRAP_VOLUME_ORD + 1;
        manifest_store.commit(&manifest)?;

        let wal = WalSet::create_with_metrics(
            &wal_dir(&cfg.path),
            &cfg,
            manifest.checkpoint_lsn + 1,
            faults.clone(),
            metrics.clone(),
        )?;

        let volume_zero = Arc::new(Volume::new(BOOTSTRAP_VOLUME_ORD, l2p_shards, 0));
        let mut volumes = HashMap::with_capacity(1);
        volumes.insert(BOOTSTRAP_VOLUME_ORD, volume_zero);

        let drainer_cfg = cfg.clone();
        let page_store_for_drainers = page_store.clone();
        let metrics_for_drainers = metrics.clone();
        let page_store_for_writeback = page_store.clone();
        let metrics_for_writeback = metrics.clone();
        let writeback_enabled = cfg.l2p_writeback_enabled;
        let writeback_params = super::streaming_flush::StreamingFlushParams {
            idle_sleep_us: cfg.l2p_writeback_idle_sleep_us,
            min_dirty_pages: cfg.l2p_writeback_min_dirty_pages,
            max_pages_per_cycle: cfg.l2p_writeback_max_pages_per_cycle,
            dirty_pages_target: cfg.flush_dirty_pages_target,
        };
        let async_reclaim_enabled = cfg.async_reclaim_enabled;
        let async_reclaim_params = super::async_reclaim::AsyncReclaimParams {
            max_pages_per_cycle: cfg.async_reclaim_max_pages_per_cycle,
            idle_interval_ms: cfg.async_reclaim_idle_interval_ms,
        };
        let dedup_lanes = build_dedup_lanes(
            0,
            dedup_shards as usize,
            ApplyLaneKind::Dedup,
            metrics.clone(),
        );
        let dedup_maintenance_lanes = build_dedup_lanes(
            0,
            dedup_shards as usize,
            ApplyLaneKind::DedupMaintenance,
            metrics.clone(),
        );
        let db = Self {
            page_store,
            page_cache,
            metrics,
            manifest_state: Arc::new(Mutex::new(ManifestState {
                store: manifest_store,
                manifest,
            })),
            volumes: Arc::new(RwLock::new(volumes)),
            refcount_shards,
            dedup_index,
            dedup_lanes,
            dedup_maintenance_lanes,
            dedup_maintenance_queued: build_dedup_queued_flags(dedup_shards as usize),
            wal,
            unlogged_pending_lsn: Mutex::new(None),
            unlogged_commit_gate: RwLock::new(()),
            unlogged_commits_enabled: cfg.unlogged_commits_enabled,
            commit_direct_apply_enabled: cfg.commit_direct_apply_enabled,
            apply_gate: Arc::new(ApplyGate::new()),
            last_applied_lsn: Mutex::new(0),
            commit_cvar: Condvar::new(),
            applied_set: Mutex::new(BTreeSet::new()),
            active_apply_lsns: Mutex::new(BTreeSet::new()),
            commit_poison: Mutex::new(None),
            dispatch_state: Mutex::new(DispatchState::default()),
            dispatch_cvar: Condvar::new(),
            snapshot_views: RwLock::new(()),
            drop_gate: RwLock::new(()),
            snap_info_cache: Mutex::new(BTreeMap::new()),
            max_volumes: cfg.max_volumes,
            faults,
            db_path: cfg.path,
            l2p_writeback: Mutex::new(None),
            flush_cursor: AtomicUsize::new(0),
            flush_select_budget: cfg.flush_select_budget,
            async_reclaim: Mutex::new(None),
            l2p_compactor: Mutex::new(None),
            l2p_buffer_enabled: cfg.l2p_buffer_enabled,
            l2p_compactor_params: crate::db::l2p_compactor::L2pCompactorParams {
                soft_entries: cfg.l2p_buffer_soft_entries,
                max_interval_ms: cfg.l2p_buffer_max_interval_ms,
            },
            lineage_gc_emit_freepbas: cfg.lineage_gc_emit_freepbas,
            freed_pbas_sink: Mutex::new(None),
        };
        // Spawn refcount drainers (priority 3) — fresh DB has no
        // replay to worry about, so we can spawn unconditionally
        // after construction. No-op when
        // `cfg.refcount_drainer_enabled = false`.
        for (idx, shard) in db.refcount_shards.iter().enumerate() {
            shard.rc.attach_drainer(
                page_store_for_drainers.clone(),
                &drainer_cfg,
                metrics_for_drainers.clone(),
                idx,
            );
        }
        // Spawn the L2P streaming writeback worker. Default-on; the
        // config toggle disables it for benchmarking / debugging
        // checkpoint behaviour without it.
        if writeback_enabled {
            let flusher = super::streaming_flush::StreamingFlusher::start(
                db.volumes.clone(),
                page_store_for_writeback,
                metrics_for_writeback,
                writeback_params,
            );
            *db.l2p_writeback.lock() = Some(flusher);
        }
        // Spawn the async reclaim worker. Drop will stop it before
        // `page_store` / `page_cache` are torn down.
        if async_reclaim_enabled {
            db.start_async_reclaim(async_reclaim_params);
        }
        // Spawn the B2 L2P buffer compactor when enabled. A fresh DB
        // has no replay so we can spawn unconditionally — the worker
        // is a no-op until commits start populating the buffer.
        if db.l2p_buffer_enabled {
            db.start_l2p_compactor();
        }
        Ok(db)
    }

    /// Open an existing database from `root_dir` using the default config.
    pub fn open(root_dir: &Path) -> Result<Self> {
        Self::open_with_config(Config::new(root_dir))
    }

    /// Open an existing database with an explicit config.
    pub fn open_with_config(cfg: Config) -> Result<Self> {
        Self::open_with_config_and_faults(cfg, FaultController::disabled())
    }

    /// As [`open`](Self::open) but with an injectable fault controller.
    pub fn open_with_faults(root_dir: &Path, faults: Arc<FaultController>) -> Result<Self> {
        Self::open_with_config_and_faults(Config::new(root_dir), faults)
    }

    /// As [`open_with_config`](Self::open_with_config) but with an
    /// injectable fault controller.
    pub fn open_with_config_and_faults(cfg: Config, faults: Arc<FaultController>) -> Result<Self> {
        let pages_path = page_file(&cfg.path);
        let page_store = Arc::new(if cfg.rebuild_free_list_on_open {
            PageStore::open_with_grow_chunk_and_bg_cap(
                &pages_path,
                cfg.page_grow_chunk_pages,
                cfg.io_submitter_bg_inflight_cap,
            )?
        } else {
            PageStore::open_fast_with_grow_chunk_and_bg_cap(
                &pages_path,
                cfg.page_grow_chunk_pages,
                cfg.io_submitter_bg_inflight_cap,
            )?
        });
        let page_cache = Arc::new(PageCache::new_with_pin_budget(
            page_store.clone(),
            cfg.page_cache_bytes,
            cfg.index_pin_bytes,
        ));
        let metrics = Arc::new(MetaMetrics::new());
        page_store.attach_metrics(metrics.clone());
        let (mut manifest_store, mut manifest) =
            ManifestStore::open_existing(page_store.clone(), faults.clone())?;
        if manifest.volumes.is_empty() {
            return Err(MetaDbError::Corruption(
                "manifest has no volume entries; database was not initialized".into(),
            ));
        }
        if !manifest
            .volumes
            .iter()
            .any(|v| v.ord == BOOTSTRAP_VOLUME_ORD)
        {
            return Err(MetaDbError::Corruption(
                "manifest is missing the bootstrap (ord=0) volume entry".into(),
            ));
        }

        // dedup_shards is part of the on-disk layout, not a runtime
        // tunable. Refuse to open with a mismatched config rather than
        // silently using one or the other.
        let cfg_dedup_shards = validate_dedup_shards(cfg.dedup_shards)?;
        if cfg_dedup_shards != manifest.dedup_shards {
            return Err(MetaDbError::Corruption(format!(
                "manifest dedup_shards={} but cfg.dedup_shards={}; this value is part \
                 of the on-disk layout — recreate the database to change shard count",
                manifest.dedup_shards, cfg_dedup_shards,
            )));
        }

        let next_gen = manifest.checkpoint_lsn.max(1) + 1;

        // Phase 7 commit 8: open every volume recorded in the manifest.
        // Earlier versions of the manifest (v3/v4/v5) are not readable —
        // Phase 7 is fresh-install only, and `Manifest::decode` rejects
        // them at the page-layer.
        // v11 manifest persists per-shard `durable_seq` so we can
        // restore each shard's `last_flushed_lsn` atomic
        // independently rather than collapsing through the global
        // `checkpoint_lsn`. v10 manifests upgrade lazily: `decode_v10`
        // synthesised every shard's value as `checkpoint_lsn`, so
        // this path is identical-by-construction on first open of an
        // older database and the next flush re-encodes as v11 with
        // real per-shard values.
        let mut volumes: HashMap<VolumeOrdinal, Arc<Volume>> =
            HashMap::with_capacity(manifest.volumes.len());
        for entry in &manifest.volumes {
            let shards = open_l2p_shards(
                page_store.clone(),
                page_cache.clone(),
                &entry.l2p_shard_roots,
                next_gen,
                metrics.clone(),
                &entry.l2p_shard_durable_seq,
                cfg.l2p_buffer_enabled,
            )?;
            volumes.insert(
                entry.ord,
                Arc::new(Volume::with_lineage(
                    entry.ord,
                    shards,
                    entry.created_lsn,
                    entry.dead_list_head_pid,
                    entry.dead_list_tail_pid,
                    entry.parent_vol_ord,
                    entry.branched_at_lsn,
                    entry.promotion_cursor,
                )),
            );
        }
        let refcount_shards = open_shards(
            page_store.clone(),
            page_cache.clone(),
            &manifest.refcount_shard_roots,
            &manifest.refcount_durable_seq,
            metrics.clone(),
        )?;
        let dedup_index_meta_pid: PageId = manifest
            .dedup_index_shard_heads
            .first()
            .and_then(|s| s.first().copied())
            .unwrap_or(0);
        let dedup_index = Arc::new(crate::dedup::DedupIndex::open(
            page_store.clone(),
            page_cache.clone(),
            dedup_index_meta_pid,
            cfg.dedup_l1_cache_entries,
        )?);

        // Replay WAL segments forward from checkpoint_lsn+1 onto the
        // freshly-opened in-memory state. Applies every op exactly the
        // way a live commit would. The result tells us the LSN of the
        // last cleanly-decoded record so the new WAL can resume there.
        //
        // `DropSnapshot` replay also mutates `manifest.snapshots`; that
        // is handled in the closure after `apply_op_bare` does the page
        // work, mirroring the live path in `Db::apply_op`.
        //
        // `CreateVolume` / `DropVolume` mutate the volumes map + the
        // manifest's volumes table, so they're dispatched ahead of
        // `apply_op_bare` (whose volume-lifecycle arm is still
        // `Err(Corruption)` — commit 8 routes live traffic through
        // `Db::create_volume` / `Db::drop_volume`, which bypass
        // `commit_ops` entirely).
        let wal_path = wal_dir(&cfg.path);
        let from_lsn = manifest.checkpoint_lsn + 1;
        let mut replayed_drop = false;
        let mut mutated_volumes = false;
        let replay_outcome =
            crate::recovery::replay_wal_set_records_into(&wal_path, from_lsn, |lsn, ops| {
                if !batch_contains_lifecycle_op(ops) {
                    let snap_lookup = |vol: VolumeOrdinal| -> Vec<SnapInfo> {
                        manifest
                            .snapshots
                            .iter()
                            .filter(|s| s.vol_ord == vol)
                            .map(|s| SnapInfo {
                                created_lsn: s.created_lsn,
                                l2p_shard_roots: s.l2p_shard_roots.clone(),
                            })
                            .collect()
                    };
                    Self::apply_replay_batch(
                        &volumes,
                        &refcount_shards,
                        &dedup_index,
                        &page_store,
                        &metrics,
                        lsn,
                        ops,
                        &snap_lookup,
                    )?;
                    return Ok(());
                }

                for op in ops {
                    match op {
                        WalOp::CreateVolume { ord, shard_count } => {
                            if !volumes.contains_key(ord) {
                                let (shards, roots) = apply_create_volume(
                                    &page_store,
                                    &page_cache,
                                    *shard_count,
                                    metrics.clone(),
                                    cfg.l2p_buffer_enabled,
                                )?;
                                volumes.insert(*ord, Arc::new(Volume::new(*ord, shards, lsn)));
                                let durable_seqs =
                                    vec![lsn; *shard_count as usize].into_boxed_slice();
                                manifest.volumes.push(VolumeEntry {
                                    ord: *ord,
                                    shard_count: *shard_count,
                                    l2p_shard_roots: roots,
                                    l2p_shard_durable_seq: durable_seqs,
                                    created_lsn: lsn,
                                    flags: 0,
                                    dead_list_head_pid: crate::types::NULL_PAGE,
                                    dead_list_tail_pid: crate::types::NULL_PAGE,
                                    parent_vol_ord: None,
                                    branched_at_lsn: 0,
                                    promotion_cursor: None,
                                });
                                mutated_volumes = true;
                            }
                            manifest.next_volume_ord = manifest
                                .next_volume_ord
                                .max(ord.checked_add(1).unwrap_or(u16::MAX));
                        }
                        WalOp::DropVolume { ord, pages } => {
                            if volumes.contains_key(ord) {
                                apply_drop_volume(&page_store, lsn, pages)?;
                                volumes.remove(ord);
                                manifest.volumes.retain(|v| v.ord != *ord);
                                mutated_volumes = true;
                            }
                        }
                        WalOp::CloneVolume {
                            src_ord,
                            new_ord,
                            src_snap_id,
                            src_shard_roots,
                        } => {
                            if !volumes.contains_key(new_ord) {
                                apply_clone_volume_incref(
                                    &page_store,
                                    &faults,
                                    lsn,
                                    src_shard_roots,
                                )?;
                                // Same stale-buffer hazard as `Db::clone_volume`:
                                // every volume whose PagedL2p was opened above
                                // may hold a pre-incref Clean copy of one of
                                // these roots — not just the source. Sweep all
                                // volumes so a later `incref_root_for_snapshot`
                                // or `cow_for_write` during replay can't flush a
                                // stale rc back over our disk-direct bump.
                                let all_vols: Vec<Arc<Volume>> =
                                    volumes.values().cloned().collect();
                                for &pid in src_shard_roots {
                                    if pid == crate::types::NULL_PAGE {
                                        continue;
                                    }
                                    page_cache.invalidate(pid);
                                    for vol in &all_vols {
                                        for shard in &vol.shards {
                                            shard.tree.write().forget_page(pid);
                                        }
                                    }
                                }
                                let (shards, actual_roots) = build_clone_volume_shards(
                                    src_shard_roots,
                                    &page_store,
                                    &page_cache,
                                    lsn,
                                    metrics.clone(),
                                    cfg.l2p_buffer_enabled,
                                )?;
                                let shard_count = shards.len() as u32;
                                // Phase 4: recover the snapshot's
                                // `created_lsn` so the clone's
                                // `branched_at_lsn` round-trips through
                                // replay the same way as the live
                                // `clone_volume` path. `TakeSnapshot` is
                                // serialized before its `CloneVolume`
                                // in the WAL, so the snapshot is
                                // already in `manifest.snapshots`
                                // here unless the source was a
                                // previously-checkpointed snapshot
                                // that survived in the manifest from
                                // the prior incarnation.
                                let branched_at_lsn = manifest
                                    .snapshots
                                    .iter()
                                    .find(|s| s.id == *src_snap_id)
                                    .map(|s| s.created_lsn)
                                    .ok_or_else(|| {
                                        MetaDbError::Corruption(format!(
                                            "CloneVolume replay: snapshot {src_snap_id} \
                                             missing from manifest at lsn {lsn}"
                                        ))
                                    })?;
                                volumes.insert(
                                    *new_ord,
                                    Arc::new(Volume::with_lineage(
                                        *new_ord,
                                        shards,
                                        lsn,
                                        crate::types::NULL_PAGE,
                                        crate::types::NULL_PAGE,
                                        Some(*src_ord),
                                        branched_at_lsn,
                                        None,
                                    )),
                                );
                                let durable_seqs =
                                    vec![lsn; shard_count as usize].into_boxed_slice();
                                manifest.volumes.push(VolumeEntry {
                                    ord: *new_ord,
                                    shard_count,
                                    l2p_shard_roots: actual_roots,
                                    l2p_shard_durable_seq: durable_seqs,
                                    created_lsn: lsn,
                                    flags: 0,
                                    dead_list_head_pid: crate::types::NULL_PAGE,
                                    dead_list_tail_pid: crate::types::NULL_PAGE,
                                    parent_vol_ord: Some(*src_ord),
                                    branched_at_lsn,
                                    promotion_cursor: None,
                                });
                                mutated_volumes = true;
                            }
                            manifest.next_volume_ord = manifest
                                .next_volume_ord
                                .max(new_ord.checked_add(1).unwrap_or(u16::MAX));
                        }
                        _ => {
                            let snap_lookup = |vol: VolumeOrdinal| -> Vec<SnapInfo> {
                                manifest
                                    .snapshots
                                    .iter()
                                    .filter(|s| s.vol_ord == vol)
                                    .map(|s| SnapInfo {
                                        created_lsn: s.created_lsn,
                                        l2p_shard_roots: s.l2p_shard_roots.clone(),
                                    })
                                    .collect()
                            };
                            apply_op_bare(
                                &volumes,
                                &refcount_shards,
                                &dedup_index,
                                &page_store,
                                lsn,
                                op,
                                &snap_lookup,
                            )?;
                            if let WalOp::DropSnapshot { id, .. } = op {
                                manifest.snapshots.retain(|s| s.id != *id);
                                replayed_drop = true;
                            }
                        }
                    }
                }
                Ok(())
            })?;
        let last_applied = replay_outcome
            .merged
            .last_lsn
            .unwrap_or(manifest.checkpoint_lsn);
        // If the last segment ended torn, truncate it to the last clean
        // record before handing the directory to the new Wal.
        crate::recovery::truncate_wal_set_torn_tails(&replay_outcome)?;
        let wal = WalSet::create_with_metrics(
            &wal_path,
            &cfg,
            last_applied + 1,
            faults.clone(),
            metrics.clone(),
        )?;

        // If anything was replayed, flush every tree + dedup memtable,
        // refresh the manifest from the post-replay in-memory roots,
        // advance `checkpoint_lsn`, and commit. This is important
        // because the subsequent `reclaim_orphan_pages` walks
        // `manifest.volumes` to decide which pages are still reachable:
        // without refreshing the roots here, any page the replay
        // allocated (e.g. a `cow_for_write` target during `L2pPut`
        // apply) is not yet on disk AND is not reachable from the
        // pre-replay manifest roots — reclaim would free it, then the
        // next allocation would hand out that same pid and two shards
        // would end up sharing a leaf. Flushing first also guarantees
        // every tree page physically exists on disk before reclaim's
        // scan runs. Dedup memtable + level heads follow the same rule
        // (`prepare_dedup_manifest_update`'s invariant): advancing
        // `checkpoint_lsn` past a DedupPut replay without flushing the
        // memtable loses the entry.
        //
        // Skipping this block when nothing was replayed keeps the
        // common "close + reopen with no WAL tail" path zero-cost.
        let replayed_anything = replay_outcome.merged.last_lsn.is_some();
        if replayed_anything || replayed_drop || mutated_volumes {
            let sorted: Vec<Arc<Volume>> = {
                let mut v: Vec<Arc<Volume>> = volumes.values().cloned().collect();
                v.sort_by_key(|vol| vol.ord);
                v
            };
            let mut l2p_guards = lock_all_l2p_shards_for(&sorted);
            flush_locked_l2p_shards(&mut l2p_guards)?;
            for shard in refcount_shards.iter() {
                shard.rc.flush()?;
            }

            // Cuckoo dedup_index + paged-array dedup_reverse both
            // write data pages synchronously per op; only their meta
            // pages need a flush here. Both meta page ids are stable
            // across opens, so the manifest slots only need to be
            // re-stamped to the same value (kept for layout
            // consistency).
            dedup_index.flush_meta()?;
            manifest.dedup_index_shard_heads =
                vec![vec![dedup_index.meta_page_id()].into_boxed_slice()].into_boxed_slice();

            // Post-replay manifest refresh: everything ≤ `last_applied`
            // has just been re-applied and is durable as soon as
            // `manifest_store.commit` below succeeds. The per-shard
            // atomics aren't bumped on the recovery path, so override
            // with `last_applied` so the v11 invariant
            // `min(durable_seq[]) == checkpoint_lsn` holds.
            refresh_manifest_entries(
                &mut manifest,
                &sorted,
                &l2p_guards,
                &refcount_shards,
                Some(last_applied),
            )?;
            manifest.checkpoint_lsn = last_applied;
            manifest_store.commit(&manifest)?;
            commit_l2p_checkpoint(&mut l2p_guards, last_applied.max(1) + 1)?;
            commit_refcount_checkpoint(&refcount_shards, last_applied.max(1) + 1)?;

            // Open-path counterpart of `Db::reclaim_freed_pages`: no
            // readers can be pinned yet, so every deferred entry queued
            // by the checkpoint above is reclaimable in one pass. Done
            // before `verify::reclaim_orphan_pages` so the orphan walk
            // sees the post-replay free list as the live state.
            page_store.try_reclaim()?;
        }

        if cfg.reclaim_orphans_on_open {
            // Reclaim orphan pages AFTER replay + post-replay commit:
            // WAL-replayed DropSnapshot ops have already mutated
            // `manifest.snapshots` and freed snapshot-exclusive tree
            // pages, so the walk now sees the post-replay manifest
            // instead of a stale snapshot list that would try to
            // traverse already-freed pages.
            let reclaim_generation = last_applied.max(manifest.checkpoint_lsn).max(1) + 1;
            verify::reclaim_orphan_pages(&page_store, &manifest, reclaim_generation)?;

            // Drain everything verify + post-replay commits queued. No
            // readers exist yet (Db isn't returned until below), so the
            // epoch barrier is trivially satisfied; this just turns the
            // deferred entries into actual on-disk Free pages + free-list
            // entries before we hand the page store to the live Db. Cache
            // invalidation is unnecessary because the page cache is fresh
            // for this open.
            page_store.try_reclaim()?;
        } else {
            tracing::info!(
                last_applied_lsn = last_applied,
                high_water_pages = page_store.high_water(),
                "metadb open skipped orphan-page reclaim"
            );
        }

        // Warm the pinned index-page set across every volume. Walks
        // each shard's tree once; stops at the first pin refusal so a
        // small `cfg.index_pin_bytes` does not end up scattered across
        // disjoint subtrees. Runs after WAL replay so the index shape
        // is final for this open — no subsequent COW needs to update
        // the pinned set during bootstrap.
        if cfg.index_pin_bytes > 0 {
            for volume in volumes.values() {
                for shard in &volume.shards {
                    let mut tree = shard.tree.write();
                    tree.warmup_index_pages()?;
                }
            }
        }

        // Capture before `manifest` is moved into `ManifestState` below.
        let manifest_dedup_shards = manifest.dedup_shards as usize;
        let drainer_cfg = cfg.clone();
        let page_store_for_drainers = page_store.clone();
        let metrics_for_drainers = metrics.clone();
        let page_store_for_writeback = page_store.clone();
        let metrics_for_writeback = metrics.clone();
        let writeback_enabled = cfg.l2p_writeback_enabled;
        let writeback_params = super::streaming_flush::StreamingFlushParams {
            idle_sleep_us: cfg.l2p_writeback_idle_sleep_us,
            min_dirty_pages: cfg.l2p_writeback_min_dirty_pages,
            max_pages_per_cycle: cfg.l2p_writeback_max_pages_per_cycle,
            dirty_pages_target: cfg.flush_dirty_pages_target,
        };
        let async_reclaim_enabled = cfg.async_reclaim_enabled;
        let async_reclaim_params = super::async_reclaim::AsyncReclaimParams {
            max_pages_per_cycle: cfg.async_reclaim_max_pages_per_cycle,
            idle_interval_ms: cfg.async_reclaim_idle_interval_ms,
        };
        let dedup_lanes = build_dedup_lanes(
            last_applied,
            manifest_dedup_shards,
            ApplyLaneKind::Dedup,
            metrics.clone(),
        );
        let dedup_maintenance_lanes = build_dedup_lanes(
            last_applied,
            manifest_dedup_shards,
            ApplyLaneKind::DedupMaintenance,
            metrics.clone(),
        );

        let db = Self {
            page_store,
            page_cache,
            metrics,
            manifest_state: Arc::new(Mutex::new(ManifestState {
                store: manifest_store,
                manifest,
            })),
            volumes: Arc::new(RwLock::new(volumes)),
            refcount_shards,
            dedup_index,
            dedup_lanes,
            dedup_maintenance_lanes,
            dedup_maintenance_queued: build_dedup_queued_flags(manifest_dedup_shards),
            wal,
            unlogged_pending_lsn: Mutex::new(None),
            unlogged_commit_gate: RwLock::new(()),
            unlogged_commits_enabled: cfg.unlogged_commits_enabled,
            commit_direct_apply_enabled: cfg.commit_direct_apply_enabled,
            apply_gate: Arc::new(ApplyGate::new()),
            last_applied_lsn: Mutex::new(last_applied),
            commit_cvar: Condvar::new(),
            applied_set: Mutex::new(BTreeSet::new()),
            active_apply_lsns: Mutex::new(BTreeSet::new()),
            commit_poison: Mutex::new(None),
            dispatch_state: Mutex::new(DispatchState::default()),
            dispatch_cvar: Condvar::new(),
            snapshot_views: RwLock::new(()),
            drop_gate: RwLock::new(()),
            snap_info_cache: Mutex::new(BTreeMap::new()),
            max_volumes: cfg.max_volumes,
            faults,
            db_path: cfg.path,
            l2p_writeback: Mutex::new(None),
            flush_cursor: AtomicUsize::new(0),
            flush_select_budget: cfg.flush_select_budget,
            async_reclaim: Mutex::new(None),
            l2p_compactor: Mutex::new(None),
            l2p_buffer_enabled: cfg.l2p_buffer_enabled,
            l2p_compactor_params: crate::db::l2p_compactor::L2pCompactorParams {
                soft_entries: cfg.l2p_buffer_soft_entries,
                max_interval_ms: cfg.l2p_buffer_max_interval_ms,
            },
            lineage_gc_emit_freepbas: cfg.lineage_gc_emit_freepbas,
            freed_pbas_sink: Mutex::new(None),
        };
        db.recompute_all_snap_infos();
        // Spawn refcount drainers AFTER WAL replay finished above so
        // the drainer never observes mid-replay state.
        for (idx, shard) in db.refcount_shards.iter().enumerate() {
            shard.rc.attach_drainer(
                page_store_for_drainers.clone(),
                &drainer_cfg,
                metrics_for_drainers.clone(),
                idx,
            );
        }
        // Spawn the L2P streaming writeback worker. Same as create:
        // default-on. Started AFTER WAL replay so it doesn't observe
        // mid-replay state.
        if writeback_enabled {
            let flusher = super::streaming_flush::StreamingFlusher::start(
                db.volumes.clone(),
                page_store_for_writeback,
                metrics_for_writeback,
                writeback_params,
            );
            *db.l2p_writeback.lock() = Some(flusher);
        }
        // Spawn the async reclaim worker AFTER WAL replay so the
        // reclaim epoch state reflects the post-replay shard
        // visibility. Drop joins it before page_store teardown.
        if async_reclaim_enabled {
            db.start_async_reclaim(async_reclaim_params);
        }
        // Spawn the B2 L2P buffer compactor AFTER WAL replay so the
        // worker only observes post-replay state. Recovery's
        // `apply_op_bare` populates the buffer; the post-replay
        // flush at the bottom of `open_with_config_and_faults` is
        // expected to force-compact via the same path, but we start
        // the worker afterwards so the compactor itself doesn't
        // race the post-replay flush's `tree.write()`.
        if db.l2p_buffer_enabled {
            db.start_l2p_compactor();
        }
        Ok(db)
    }
}
