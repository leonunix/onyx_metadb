use super::*;

impl Db {
    /// Create a fresh database in `root_dir` using the default config.
    pub fn create(root_dir: &Path) -> Result<Arc<Self>> {
        Self::create_with_config(Config::new(root_dir))
    }

    /// Create a fresh database with an explicit config.
    pub fn create_with_config(cfg: Config) -> Result<Arc<Self>> {
        Self::create_with_config_and_faults(cfg, FaultController::disabled())
    }

    /// As [`create`](Self::create) but with an injectable fault controller.
    pub fn create_with_faults(root_dir: &Path, faults: Arc<FaultController>) -> Result<Arc<Self>> {
        Self::create_with_config_and_faults(Config::new(root_dir), faults)
    }

    /// As [`create_with_config`](Self::create_with_config) but with an
    /// injectable fault controller.
    pub fn create_with_config_and_faults(
        cfg: Config,
        faults: Arc<FaultController>,
    ) -> Result<Arc<Self>> {
        validate_phase5_refcount_mode(&cfg)?;
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
            dedup_shards as usize,
            cfg.dedup_drainer_enabled,
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

        let lsn_alloc = LsnAllocator::new(manifest.checkpoint_lsn + 1);

        // Open a fresh lifecycle journal. Fresh DB → no segments
        // yet, so `next_seq` is 1 and `LifecycleJournal::open` simply
        // creates the directory and its first segment.
        let lifecycle_journal = Some(Mutex::new(
            crate::lifecycle_log::LifecycleJournal::open(
                &lifecycle_log_dir(&cfg.path),
                1,
                cfg.wal_segment_bytes,
            )?,
        ));

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
        let deferred_outcomes = Arc::new(
            crate::db::commit::DeferredOutcomeAggregator::new(
                metrics.clone(),
                faults.clone(),
            ),
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
            lsn_alloc,
            unlogged_pending_lsn: Mutex::new(None),
            unlogged_commit_gate: RwLock::new(()),
            unlogged_commits_enabled: cfg.unlogged_commits_enabled,
            journal_mode: cfg.journal_mode,
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
            lineage_gc_worker: Mutex::new(None),
            l2p_buffer_enabled: cfg.l2p_buffer_enabled,
            lineage_gc_emit_freepbas: cfg.lineage_gc_emit_freepbas,
            lineage_gc_drop_dedup_shared: cfg.lineage_gc_drop_dedup_shared,
            freed_pbas_sink: Mutex::new(None),
            deferred_outcomes,
            commit_deferred_outcomes_enabled: cfg.commit_deferred_outcomes_enabled,
            wal_async_commits_enabled: cfg.wal_async_commits_enabled,
            // Phase 4 Step 4: fresh DB starts with checkpoint_txg = 0 so the
            // first Open TXG is 1.
            txg: Arc::new(crate::txg::TxgStateMachine::new(0)),
            txg_threads_enabled: cfg.txg_threads_enabled,
            parallel_l2p_drain_enabled: cfg.parallel_l2p_drain_enabled,
            rc_authoritative_reclaim: cfg.rc_authoritative_reclaim,
            // Phase 4 Step 7: notifiers allocated regardless; the worker
            // threads are spawned conditionally below.
            txg_quiesce_notifier: Arc::new(crate::db::txg_quiesce::QuiesceNotifier::new()),
            txg_sync_notifier: Arc::new(crate::db::txg_sync::SyncNotifier::new()),
            txg_quiesce: Mutex::new(None),
            txg_sync: Mutex::new(None),
            buffer_applied_watermark: AtomicU64::new(0),
            lifecycle_applied_watermark: AtomicU64::new(0),
            lifecycle_journal,
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
        // Spawn the async dedup-index drainers (no-op when
        // `dedup_drainer_enabled = false`). Fresh DB → no replay.
        db.dedup_index
            .attach_drainers(&drainer_cfg, metrics_for_drainers.clone());
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
        // ZFS-TXG-clone Phase 4 Step 8: the background L2P drainer
        // is now the `TxgSyncThread`, spawned below when
        // `txg_threads_enabled` is true. When the threads are off,
        // `flush_with_gate`'s inline path drives the per-shard drain
        // via `force_compact_l2p_buffers` synchronously.
        let db = Arc::new(db);
        if db.txg_threads_enabled {
            Self::start_txg_threads(&db, cfg.txg_timeout_ms);
        }
        // The sole production trigger for FreePbas-emitting PBA reclaim.
        // Started after `Arc::new` so it can hold a `Weak<Db>`; Drop joins
        // it before page_store / refcount / dedup teardown.
        if cfg.lineage_gc_enabled {
            db.start_lineage_gc_worker(crate::db::lineage_gc::LineageGcParams {
                interval_ms: cfg.lineage_gc_interval_ms,
                max_cycles_per_wake: cfg.lineage_gc_max_cycles_per_wake,
            });
        }
        Ok(db)
    }

    /// Open an existing database from `root_dir` using the default config.
    pub fn open(root_dir: &Path) -> Result<Arc<Self>> {
        Self::open_with_config(Config::new(root_dir))
    }

    /// Open an existing database with an explicit config.
    pub fn open_with_config(cfg: Config) -> Result<Arc<Self>> {
        Self::open_with_config_and_faults(cfg, FaultController::disabled())
    }

    /// As [`open`](Self::open) but with an injectable fault controller.
    pub fn open_with_faults(root_dir: &Path, faults: Arc<FaultController>) -> Result<Arc<Self>> {
        Self::open_with_config_and_faults(Config::new(root_dir), faults)
    }

    /// As [`open_with_config`](Self::open_with_config) but with an
    /// injectable fault controller.
    pub fn open_with_config_and_faults(cfg: Config, faults: Arc<FaultController>) -> Result<Arc<Self>> {
        validate_phase5_refcount_mode(&cfg)?;
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
            manifest.dedup_shards as usize,
            cfg.dedup_drainer_enabled,
        )?);

        // Buffer-as-sole-journal Phase D.5b retired the WAL writer.
        // Recovery now starts from `manifest.checkpoint_lsn` (the last
        // durable manifest commit) and folds the lifecycle journal
        // forward; data-plane state is rebuilt from onyx's LV2 buffer
        // on its own replay path, so this open() body only worries
        // about lifecycle records sitting above the manifest
        // watermark.
        //
        // ZFS-TXG-clone Phase 4 Step 8a: capture the durable TXG so
        // every replayed lifecycle record folds into `checkpoint_txg
        // + 1` — the Open TXG the new TxgStateMachine will be
        // constructed with below.
        let replay_open_txg = manifest.checkpoint_txg + 1;
        let mut replayed_drop = false;
        let mut mutated_volumes = false;

        // Buffer-as-sole-journal Phase C.4: replay any uncovered
        // lifecycle records into the freshly-opened in-memory state.
        // Lifecycle records carry a monotonic `seq` but not an LSN; we
        // synthesise replay LSNs sequentially starting from
        // `manifest.checkpoint_lsn + 1` so every page generation
        // stamp the original apply wrote is strictly >= our replay
        // LSN, and the `header.generation >= lsn` idempotency guard
        // in `apply_drop_snapshot_pages` / `apply_clone_volume_incref`
        // correctly fires SKIP on a page already processed before the
        // crash.
        let mut last_applied = manifest.checkpoint_lsn;
        let mut lifecycle_max_seq = manifest.lifecycle_replay_seq;
        let mut lifecycle_replayed_anything = false;
        let lifecycle_dir = lifecycle_log_dir(&cfg.path);
        if lifecycle_dir.exists() {
            let from_seq = manifest.lifecycle_replay_seq;
            let outcome = replay_lifecycle_journal_into(
                &lifecycle_dir,
                &mut manifest,
                &mut volumes,
                &refcount_shards,
                &dedup_index,
                &page_store,
                &page_cache,
                &faults,
                metrics.clone(),
                last_applied,
                replay_open_txg,
                cfg.l2p_buffer_enabled,
                cfg.rc_authoritative_reclaim,
                from_seq,
            )?;
            lifecycle_max_seq = outcome.max_seq;
            last_applied = last_applied.saturating_add(outcome.lsns_consumed);
            lifecycle_replayed_anything =
                outcome.lsns_consumed > 0 || outcome.replayed_drop_snapshot;
            if outcome.mutated_volumes {
                mutated_volumes = true;
            }
            if outcome.replayed_drop_snapshot {
                replayed_drop = true;
            }
        }

        let lsn_alloc = LsnAllocator::new(last_applied + 1);

        // Open the lifecycle journal for further append at the seq
        // immediately after whatever replay just folded in.
        // `lifecycle_max_seq` is the manifest watermark when replay was a
        // no-op (no segments / nothing beyond the checkpoint) or the
        // highest seq observed otherwise; in either case `next_seq =
        // lifecycle_max_seq + 1` is the next free slot. The post-replay
        // manifest commit below stamps this value into
        // `manifest.lifecycle_replay_seq`.
        let lifecycle_journal = {
            let next_seq = lifecycle_max_seq.saturating_add(1);
            Some(Mutex::new(crate::lifecycle_log::LifecycleJournal::open(
                &lifecycle_dir,
                next_seq,
                cfg.wal_segment_bytes,
            )?))
        };

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
        if replayed_drop || mutated_volumes || lifecycle_replayed_anything {
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
            // Phase C.4: stamp the lifecycle replay watermark alongside
            // `checkpoint_lsn` so any lifecycle records we just folded
            // in are now "covered" by the durable manifest. Without
            // this stamp the next open would re-replay the same
            // records (legal: replays are idempotent via page
            // generation guards) but would also synthesise fresh LSNs
            // that overlap with later WAL traffic.
            manifest.lifecycle_replay_seq = lifecycle_max_seq;
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
        let manifest_checkpoint_txg = manifest.checkpoint_txg;
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

        let deferred_outcomes = Arc::new(
            crate::db::commit::DeferredOutcomeAggregator::new(
                metrics.clone(),
                faults.clone(),
            ),
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
            lsn_alloc,
            unlogged_pending_lsn: Mutex::new(None),
            unlogged_commit_gate: RwLock::new(()),
            unlogged_commits_enabled: cfg.unlogged_commits_enabled,
            journal_mode: cfg.journal_mode,
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
            lineage_gc_worker: Mutex::new(None),
            l2p_buffer_enabled: cfg.l2p_buffer_enabled,
            lineage_gc_emit_freepbas: cfg.lineage_gc_emit_freepbas,
            lineage_gc_drop_dedup_shared: cfg.lineage_gc_drop_dedup_shared,
            freed_pbas_sink: Mutex::new(None),
            deferred_outcomes,
            commit_deferred_outcomes_enabled: cfg.commit_deferred_outcomes_enabled,
            wal_async_commits_enabled: cfg.wal_async_commits_enabled,
            // Phase 4 Step 4: resume TXG accounting at the manifest's last
            // synced TXG. open_txg will be checkpoint_txg + 1 — replayed
            // ops fold into that TXG so the next quiesce/sync persists
            // them with a fresh checkpoint_txg.
            txg: Arc::new(crate::txg::TxgStateMachine::new(manifest_checkpoint_txg)),
            txg_threads_enabled: cfg.txg_threads_enabled,
            parallel_l2p_drain_enabled: cfg.parallel_l2p_drain_enabled,
            rc_authoritative_reclaim: cfg.rc_authoritative_reclaim,
            txg_quiesce_notifier: Arc::new(crate::db::txg_quiesce::QuiesceNotifier::new()),
            txg_sync_notifier: Arc::new(crate::db::txg_sync::SyncNotifier::new()),
            txg_quiesce: Mutex::new(None),
            txg_sync: Mutex::new(None),
            buffer_applied_watermark: AtomicU64::new(0),
            // Phase C.4: resume the lifecycle watermark from whatever
            // the post-replay manifest commit just persisted. Future
            // lifecycle ops `fetch_max` onto this value so the next
            // checkpoint stamps a monotonic `lifecycle_replay_seq`
            // rather than regressing it to 0.
            lifecycle_applied_watermark: AtomicU64::new(lifecycle_max_seq),
            lifecycle_journal,
        };
        // Stamp `slot_max_lsn(open_txg)` with the post-replay
        // `last_applied`. `apply_replay_batch` and `apply_op_bare` fold
        // every replayed op into `manifest.checkpoint_txg + 1` without
        // going through `TxgGuard::record_lsn`, so without this stamp
        // `slot_max_lsn(open_txg)` would be 0 — which would regress
        // `manifest.checkpoint_lsn` to 0 on the next flush body that
        // reads `wal_checkpoint = self.txg.slot_max_lsn(txg)`.
        //
        // Stamping is safe before any commit thread starts: the
        // commit path's own `record_lsn` is max-monotonic, so a later
        // commit with `lsn > last_applied` correctly bumps the slot.
        // We also stamp when nothing was replayed (so `last_applied =
        // manifest.checkpoint_lsn`); that keeps the very first flush's
        // `wal_checkpoint` >= the durable manifest's `checkpoint_lsn`
        // even on a clean open with no commits yet.
        db.txg.record_lsn(db.txg.open_txg(), last_applied);
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
        // Spawn the async dedup-index drainers AFTER replay (so they
        // never observe mid-replay state), mirroring the rc drainers.
        // No-op when `dedup_drainer_enabled = false`.
        db.dedup_index
            .attach_drainers(&drainer_cfg, metrics_for_drainers.clone());
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
        // ZFS-TXG-clone Phase 4 Step 8: the background L2P drainer
        // is now the `TxgSyncThread`, spawned below when
        // `txg_threads_enabled` is true. With the legacy
        // `L2pCompactor` retired, the post-replay flush at the
        // bottom of `open_with_config_and_faults` is the sole
        // initial drainer; subsequent drains come from either the
        // sync thread (threads on) or `flush_with_gate`'s inline
        // path (threads off).
        let db = Arc::new(db);
        if db.txg_threads_enabled {
            Self::start_txg_threads(&db, cfg.txg_timeout_ms);
        }
        // The sole production trigger for FreePbas-emitting PBA reclaim.
        // Started after `Arc::new` (so it can hold a `Weak<Db>`) and after
        // WAL replay + the post-replay flush, so it never observes
        // mid-replay dead-list state. Drop joins it before page_store /
        // refcount / dedup teardown.
        if cfg.lineage_gc_enabled {
            db.start_lineage_gc_worker(crate::db::lineage_gc::LineageGcParams {
                interval_ms: cfg.lineage_gc_interval_ms,
                max_cycles_per_wake: cfg.lineage_gc_max_cycles_per_wake,
            });
        }
        Ok(db)
    }
}

fn validate_phase5_refcount_mode(cfg: &Config) -> Result<()> {
    if !cfg.lineage_gc_emit_freepbas {
        return Err(MetaDbError::InvalidArgument(
            "lineage_gc_emit_freepbas=false is no longer supported: Phase 5 rc-neutral \
             L2P remaps require Lineage GC to emit FreePbas retire events"
                .into(),
        ));
    }
    Ok(())
}

/// Result of [`replay_lifecycle_journal_into`]: how far we got and
/// what kinds of state changed, so the post-replay flush block knows
/// whether it has to run and which gauges to bump.
struct LifecycleReplayOutcome {
    /// Highest seq we folded in. Equals the caller-supplied `from_seq`
    /// when the journal had no uncovered records.
    max_seq: u64,
    /// Number of synthetic LSNs consumed (== number of records replayed).
    /// Callers advance `last_applied` by this much before creating the
    /// new WAL so its `next_lsn` does not collide with these LSNs.
    lsns_consumed: u64,
    /// True if at least one CreateVolume / DropVolume / CloneVolume
    /// record was applied — caller folds this into the
    /// `mutated_volumes` flag that gates the post-replay flush.
    mutated_volumes: bool,
    /// True if at least one DropSnapshot record was applied — caller
    /// folds this into the `replayed_drop` flag (same gate as the WAL
    /// replay arm).
    replayed_drop_snapshot: bool,
}

/// Phase C.4: replay every lifecycle-log record above
/// `manifest.lifecycle_replay_seq` against the post-WAL-replay
/// in-memory state. Mirrors the per-op dispatch in the WAL-replay
/// closure for the variants that have migrated to the lifecycle
/// journal in Phase C.3 (CreateVolume / DropVolume / CloneVolume /
/// DropSnapshot / PromotionChunk / PromotionComplete).
///
/// LSN assignment: each record gets `starting_lsn + i + 1` (`i` = its
/// position in the seq-ordered iteration). The original apply
/// reserved its LSN via `wal.reserve_unlogged` AFTER the WAL's
/// `next_lsn` was already >= `starting_lsn + 1`, so every page
/// generation stamp on disk is strictly >= the LSN we use here. That
/// keeps the `header.generation >= lsn` idempotency guards in
/// `apply_drop_snapshot_pages` / `apply_clone_volume_incref` firing
/// SKIP correctly when the original apply finished before the crash.
#[allow(clippy::too_many_arguments)]
fn replay_lifecycle_journal_into(
    dir: &Path,
    manifest: &mut Manifest,
    volumes: &mut HashMap<VolumeOrdinal, Arc<Volume>>,
    refcount_shards: &[Shard],
    dedup_index: &Arc<crate::dedup::DedupIndex>,
    page_store: &Arc<PageStore>,
    page_cache: &Arc<PageCache>,
    faults: &Arc<FaultController>,
    metrics: Arc<MetaMetrics>,
    starting_lsn: Lsn,
    replay_open_txg: crate::types::Txg,
    l2p_buffer_enabled: bool,
    rc_authoritative: bool,
    from_seq: u64,
) -> Result<LifecycleReplayOutcome> {
    use crate::lifecycle_log::{LifecycleJournal, op as lifecycle_op};
    let mut outcome = LifecycleReplayOutcome {
        max_seq: from_seq,
        lsns_consumed: 0,
        mutated_volumes: false,
        replayed_drop_snapshot: false,
    };
    LifecycleJournal::replay(dir, from_seq, |rec| {
        if rec.seq <= from_seq {
            // `LifecycleJournal::replay` already filters at this
            // threshold; the defensive check keeps the apply path
            // immune to a future relaxation of that contract.
            return Ok(());
        }
        let op = lifecycle_op::decode(&rec.body)?;
        outcome.lsns_consumed = outcome
            .lsns_consumed
            .checked_add(1)
            .ok_or(MetaDbError::OutOfSpace)?;
        let lsn = starting_lsn
            .checked_add(outcome.lsns_consumed)
            .ok_or(MetaDbError::OutOfSpace)?;
        apply_lifecycle_record_replay(
            manifest,
            volumes,
            refcount_shards,
            dedup_index,
            page_store,
            page_cache,
            faults,
            &metrics,
            lsn,
            replay_open_txg,
            l2p_buffer_enabled,
            rc_authoritative,
            &op,
            &mut outcome,
        )?;
        outcome.max_seq = outcome.max_seq.max(rec.seq);
        Ok(())
    })?;
    Ok(outcome)
}

#[allow(clippy::too_many_arguments)]
fn apply_lifecycle_record_replay(
    manifest: &mut Manifest,
    volumes: &mut HashMap<VolumeOrdinal, Arc<Volume>>,
    refcount_shards: &[Shard],
    dedup_index: &Arc<crate::dedup::DedupIndex>,
    page_store: &Arc<PageStore>,
    page_cache: &Arc<PageCache>,
    faults: &Arc<FaultController>,
    metrics: &Arc<MetaMetrics>,
    lsn: Lsn,
    replay_open_txg: crate::types::Txg,
    l2p_buffer_enabled: bool,
    rc_authoritative: bool,
    op: &crate::lifecycle_log::LifecycleOp,
    outcome: &mut LifecycleReplayOutcome,
) -> Result<()> {
    use crate::lifecycle_log::LifecycleOp;
    match op {
        LifecycleOp::CreateVolume { ord, shard_count } => {
            // Idempotent: a prior replay (or the original apply
            // before the crash) may already have inserted this
            // volume into our in-memory map. In that case the
            // shards are already materialised and the manifest
            // already lists the entry — nothing more to do.
            if !volumes.contains_key(ord) {
                let (shards, roots) = apply_create_volume(
                    page_store,
                    page_cache,
                    *shard_count,
                    metrics.clone(),
                    l2p_buffer_enabled,
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
                outcome.mutated_volumes = true;
            }
            manifest.next_volume_ord = manifest
                .next_volume_ord
                .max(ord.checked_add(1).unwrap_or(u16::MAX));
        }
        LifecycleOp::DropVolume { ord, pages } => {
            if volumes.contains_key(ord) {
                apply_drop_volume(page_store, lsn, pages)?;
                volumes.remove(ord);
                manifest.volumes.retain(|v| v.ord != *ord);
                outcome.mutated_volumes = true;
            }
        }
        LifecycleOp::CloneVolume {
            src_ord,
            new_ord,
            src_snap_id,
            src_shard_roots,
        } => {
            if !volumes.contains_key(new_ord) {
                apply_clone_volume_incref(page_store, faults, lsn, src_shard_roots)?;
                // Same stale-buffer hazard as the WAL replay arm for
                // CloneVolume: every PagedL2p opened above may hold
                // a pre-incref Clean copy of one of these roots.
                // Sweep all volumes so a later cow_for_write during
                // continued replay can't flush a stale rc back over
                // our disk-direct bump.
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
                    page_store,
                    page_cache,
                    lsn,
                    metrics.clone(),
                    l2p_buffer_enabled,
                )?;
                let shard_count = shards.len() as u32;
                let branched_at_lsn = manifest
                    .snapshots
                    .iter()
                    .find(|s| s.id == *src_snap_id)
                    .map(|s| s.created_lsn)
                    .ok_or_else(|| {
                        MetaDbError::Corruption(format!(
                            "CloneVolume lifecycle replay: snapshot \
                             {src_snap_id} missing from manifest at \
                             lsn {lsn}"
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
                outcome.mutated_volumes = true;
            }
            manifest.next_volume_ord = manifest
                .next_volume_ord
                .max(new_ord.checked_add(1).unwrap_or(u16::MAX));
        }
        LifecycleOp::DropSnapshot {
            id,
            pages,
            pba_decrefs,
        } => {
            // Drive the same page-free + per-pba decref cascade the
            // live `Db::drop_snapshot` path uses. Page generations
            // gate idempotency across re-applies.
            apply_drop_snapshot_pages_and_decrefs(
                page_store,
                refcount_shards,
                lsn,
                pages,
                pba_decrefs,
            )?;
            manifest.snapshots.retain(|s| s.id != *id);
            outcome.replayed_drop_snapshot = true;
        }
        LifecycleOp::PromotionChunk {
            vol_ord,
            pba_increfs,
            next_cursor,
        } => {
            // `apply_promotion_chunk` increfs each PBA in
            // `pba_increfs` and stamps the in-memory
            // `promotion_cursor`. The manifest mirror is rebuilt by
            // the post-replay `refresh_manifest_entries` call.
            // Idempotency rides on the cursor: if a prior apply
            // already advanced past `next_cursor` we still re-incref
            // (the rc is staged per-record, with replay scope
            // bounded by `checkpoint_lsn`); the cursor write is
            // last-writer-wins and `next_cursor` is the durable
            // value from the record.
            apply_promotion_chunk(
                volumes,
                refcount_shards,
                lsn,
                *vol_ord,
                pba_increfs,
                *next_cursor,
            )?;
        }
        LifecycleOp::PromotionComplete { vol_ord } => {
            apply_promotion_complete(volumes, *vol_ord)?;
        }
        LifecycleOp::Discard {
            vol_ord,
            start_lba,
            count,
        } => {
            // Phase D.1 replay: the record only carries the range —
            // rescan the L2P at apply time to rebuild the captured
            // list, then drive the same `apply_l2p_range_delete` the
            // live path uses. The rescan naturally observes whatever
            // pre-crash apply progress had already deleted (those
            // LBAs are gone from the tree), so the call is
            // idempotent across repeated replays.
            let volume = volumes.get(vol_ord).ok_or_else(|| {
                MetaDbError::Corruption(format!(
                    "lifecycle replay: Discard for unknown volume ord {vol_ord}"
                ))
            })?;
            let end = (*start_lba)
                .checked_add(*count as u64)
                .ok_or_else(|| {
                    MetaDbError::Corruption(format!(
                        "lifecycle replay: Discard range overflow on vol \
                         {vol_ord}: start={start_lba} count={count}"
                    ))
                })?;
            let captured = scan_l2p_range(volume, *start_lba, end)?;
            if !captured.is_empty() {
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
                apply_l2p_range_delete(
                    volumes,
                    refcount_shards,
                    lsn,
                    *vol_ord,
                    &captured,
                    &snap_lookup(*vol_ord),
                    rc_authoritative,
                )?;
            }
        }
        // `TakeSnapshot` is still manifest-only in Phase D — no
        // journal entry today. A record on disk indicates the
        // on-disk schema has moved ahead of this binary; surface
        // it as corruption rather than silently misapplying.
        LifecycleOp::TakeSnapshot { .. } => {
            return Err(MetaDbError::Corruption(format!(
                "lifecycle replay: TakeSnapshot (tag 0x{:02x}) is not \
                 yet routed through the lifecycle journal",
                op.tag(),
            )));
        }
    }
    Ok(())
}
