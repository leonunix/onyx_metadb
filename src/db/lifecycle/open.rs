use super::*;
use crate::lifecycle_log::{JournalDevice, LifecycleWriter, RingJournal};
use crate::page_store::PageDevice;

/// Selects the physical backing for a `Db`'s persistent metadata. The
/// create/open cores are backing-agnostic except at a few divergent points
/// (page-store construction, lifecycle-journal construction/replay, and — on
/// the device path only — the bounded free-list rebuild). Everything else
/// (manifest, shards, dedup, recovery flush, worker spawn) is identical.
enum MetaBacking {
    /// Default path: a flat page file + directory of journal segments under
    /// `cfg.path`.
    File,
    /// Device path (onyx over a chunklet meta LogicalDisk): a fixed-capacity
    /// page window + a fixed-ring journal window. `cfg.path` is not touched.
    Device {
        page_device: Arc<dyn PageDevice>,
        journal_device: Arc<dyn JournalDevice>,
    },
}

impl MetaBacking {
    fn is_device(&self) -> bool {
        matches!(self, MetaBacking::Device { .. })
    }
}

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
        Self::create_core(cfg, faults, MetaBacking::File)
    }

    /// Create a fresh database over caller-supplied devices (the fixed-capacity
    /// device path — onyx over a chunklet meta LogicalDisk). `page_device`
    /// backs the page store; `journal_device` backs the lifecycle-journal ring.
    /// `cfg.path` is ignored for persistence (no files are created). The manifest
    /// A/B slots + all metadata live inside `page_device`; the caller frames the
    /// two windows (page window, journal ring) out of the meta LD.
    pub fn create_on_device(
        cfg: Config,
        page_device: Arc<dyn PageDevice>,
        journal_device: Arc<dyn JournalDevice>,
    ) -> Result<Arc<Self>> {
        Self::create_on_device_with_faults(
            cfg,
            FaultController::disabled(),
            page_device,
            journal_device,
        )
    }

    /// As [`create_on_device`](Self::create_on_device) but with an injectable
    /// fault controller (device fault-injection tests).
    pub fn create_on_device_with_faults(
        cfg: Config,
        faults: Arc<FaultController>,
        page_device: Arc<dyn PageDevice>,
        journal_device: Arc<dyn JournalDevice>,
    ) -> Result<Arc<Self>> {
        Self::create_core(
            cfg,
            faults,
            MetaBacking::Device {
                page_device,
                journal_device,
            },
        )
    }

    fn create_core(
        cfg: Config,
        faults: Arc<FaultController>,
        backing: MetaBacking,
    ) -> Result<Arc<Self>> {
        validate_rc_neutral_refcount_mode(&cfg)?;
        let shard_count = validate_shard_count(cfg.shards_per_partition)?;
        let dedup_shards = validate_dedup_shards(cfg.dedup_shards)?;
        let page_store = Arc::new(match &backing {
            MetaBacking::File => {
                std::fs::create_dir_all(&cfg.path)?;
                let pages_path = page_file(&cfg.path);
                PageStore::create_with_grow_chunk_and_bg_cap(
                    &pages_path,
                    cfg.page_grow_chunk_pages,
                    cfg.io_submitter_bg_inflight_cap,
                )?
            }
            MetaBacking::Device { page_device, .. } => {
                PageStore::create_on_device(page_device.clone())?
            }
        });
        let page_cache = Arc::new(PageCache::new_with_pin_budget(
            page_store.clone(),
            cfg.page_cache_bytes,
            cfg.index_pin_bytes,
        ));
        let metrics = Arc::new(MetaMetrics::new());
        metrics
            .set_rc_checkpoint_mode(cfg.bfg_threads_enabled, cfg.rc_checkpoint_streaming_enabled);
        page_store.attach_metrics(metrics.clone());
        let (mut manifest_store, mut manifest) =
            ManifestStore::open_or_create(page_store.clone(), page_cache.clone(), faults.clone())?;
        let (l2p_shards, l2p_roots) = create_l2p_shards(
            page_store.clone(),
            page_cache.clone(),
            shard_count,
            metrics.clone(),
            cfg.l2p_buffer_enabled,
            // Bootstrap volume `created_lsn` is 0.
            0,
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
            page_dead_list_head_pid: crate::types::NULL_PAGE,
            page_dead_list_tail_pid: crate::types::NULL_PAGE,
            page_live_list_head_pid: crate::types::NULL_PAGE,
            page_live_list_tail_pid: crate::types::NULL_PAGE,
            promoted_log_head_pid: crate::types::NULL_PAGE,
            promoted_log_tail_pid: crate::types::NULL_PAGE,
        }];
        manifest.next_volume_ord = BOOTSTRAP_VOLUME_ORD + 1;
        manifest_store.commit(&mut manifest)?;

        let lsn_alloc = LsnAllocator::new(manifest.checkpoint_lsn + 1);

        // Open a fresh lifecycle journal. Fresh DB → no records yet, so
        // `next_seq` is 1. File: creates the directory + first segment. Ring:
        // opens at `ring_head = 0` over a freshly-framed (all-zero) journal
        // window; `RingJournal::open` scans from 0, finds the first block
        // never-written, and starts the tail there.
        let lifecycle_journal = Some(Mutex::new(match &backing {
            MetaBacking::File => {
                LifecycleWriter::File(crate::lifecycle_log::LifecycleJournal::open(
                    &lifecycle_log_dir(&cfg.path),
                    1,
                    cfg.wal_segment_bytes,
                )?)
            }
            MetaBacking::Device { journal_device, .. } => {
                LifecycleWriter::Ring(RingJournal::open(journal_device.clone(), 0, 1)?)
            }
        }));

        let volume_zero = Arc::new(Volume::new(BOOTSTRAP_VOLUME_ORD, l2p_shards, 0));
        let mut volumes = HashMap::with_capacity(1);
        volumes.insert(BOOTSTRAP_VOLUME_ORD, volume_zero);

        let drainer_cfg = cfg.clone();
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
        let livelist_condense_min_segments = cfg.livelist_condense_min_segments;
        let livelist_condense_params = super::livelist_condense::LivelistCondenseParams {
            min_segments: cfg.livelist_condense_min_segments,
            idle_interval_ms: cfg.livelist_condense_idle_interval_ms,
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
        let deferred_outcomes = Arc::new(crate::db::commit::DeferredOutcomeAggregator::new(
            metrics.clone(),
            faults.clone(),
        ));
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
            last_applied_lsn_observed: AtomicU64::new(0),
            commit_cvar: Condvar::new(),
            applied_set: Mutex::new(BTreeSet::new()),
            active_apply_lsns: Mutex::new(BTreeSet::new()),
            commit_poison: Mutex::new(None),
            commit_poisoned: Arc::new(AtomicBool::new(false)),
            sync_poison: Mutex::new(None),
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
            livelist_condense: Mutex::new(None),
            lineage_gc_worker: Mutex::new(None),
            l2p_buffer_enabled: cfg.l2p_buffer_enabled,
            lineage_gc_emit_freepbas: cfg.lineage_gc_emit_freepbas,
            lineage_gc_drop_dedup_shared: cfg.lineage_gc_drop_dedup_shared,
            freed_pbas_sink: Mutex::new(None),
            deferred_outcomes,
            commit_deferred_outcomes_enabled: cfg.commit_deferred_outcomes_enabled,
            wal_async_commits_enabled: cfg.wal_async_commits_enabled,
            // fresh DB starts with checkpoint_bfg = 0 so the
            // first Open BFG is 1.
            bfg: Arc::new(crate::bfg::BfgStateMachine::new(0)),
            bfg_threads_enabled: cfg.bfg_threads_enabled,
            rc_checkpoint_streaming_enabled: cfg.rc_checkpoint_streaming_enabled,
            parallel_l2p_drain_enabled: cfg.parallel_l2p_drain_enabled,
            parallel_l2p_drain_workers: cfg.parallel_l2p_drain_workers,
            l2p_drain_chunk_entries: cfg.l2p_drain_chunk_entries,
            l2p_checkpoint_pipeline_enabled: cfg.l2p_checkpoint_pipeline_enabled,
            bfg_l2p_work_limit: cfg.l2p_buffer_soft_entries,
            rc_authoritative_reclaim: cfg.rc_authoritative_reclaim,
            // notifiers allocated regardless; the worker
            // threads are spawned conditionally below.
            bfg_quiesce_notifier: Arc::new(crate::db::bfg_quiesce::QuiesceNotifier::new()),
            bfg_sync_notifier: Arc::new(crate::db::bfg_sync::SyncNotifier::new()),
            bfg_quiesce: Mutex::new(None),
            bfg_sync: Mutex::new(None),
            l2p_prefold: Mutex::new(None),
            buffer_applied_watermark: AtomicU64::new(0),
            lifecycle_applied_watermark: AtomicU64::new(0),
            lifecycle_journal,
            pending_sync_tasks: Mutex::new(Vec::new()),
        };
        // The refcount fold is inline + per-BFG-slot (no background rc
        // drainer to spawn — see `refcount::shard`).
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
        // BFG: spawn the background livelist-condense worker
        // (independent of async_reclaim). Drop stops it before page_store /
        // page_cache teardown.
        if livelist_condense_min_segments > 0 {
            db.start_livelist_condense(livelist_condense_params);
        }
        // BFG: the background L2P drainer
        // is now the `BfgSyncThread`, spawned below when
        // `bfg_threads_enabled` is true. When the threads are off,
        // `flush_with_gate`'s inline path drives the per-shard drain
        // via `force_compact_l2p_buffers` synchronously.
        let db = Arc::new(db);
        if db.bfg_threads_enabled {
            Self::start_bfg_threads(&db, cfg.bfg_timeout_ms);
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
    pub fn open_with_config_and_faults(
        cfg: Config,
        faults: Arc<FaultController>,
    ) -> Result<Arc<Self>> {
        Self::open_core(cfg, faults, MetaBacking::File)
    }

    /// Open an existing database over caller-supplied devices (the fixed-capacity
    /// device path — onyx over a chunklet meta LogicalDisk). Mirror of
    /// [`create_on_device`](Self::create_on_device). `cfg.path` is ignored for
    /// persistence. The device path always rebuilds the free list by a bounded
    /// scan (a fixed LD has no EOF to trust and no hole-punch to reclaim leaked
    /// pages), so `cfg.rebuild_free_list_on_open` is not consulted here.
    pub fn open_on_device(
        cfg: Config,
        page_device: Arc<dyn PageDevice>,
        journal_device: Arc<dyn JournalDevice>,
    ) -> Result<Arc<Self>> {
        Self::open_on_device_with_faults(
            cfg,
            FaultController::disabled(),
            page_device,
            journal_device,
        )
    }

    /// As [`open_on_device`](Self::open_on_device) but with an injectable fault
    /// controller (device fault-injection tests).
    pub fn open_on_device_with_faults(
        cfg: Config,
        faults: Arc<FaultController>,
        page_device: Arc<dyn PageDevice>,
        journal_device: Arc<dyn JournalDevice>,
    ) -> Result<Arc<Self>> {
        Self::open_core(
            cfg,
            faults,
            MetaBacking::Device {
                page_device,
                journal_device,
            },
        )
    }

    fn open_core(
        cfg: Config,
        faults: Arc<FaultController>,
        backing: MetaBacking,
    ) -> Result<Arc<Self>> {
        validate_rc_neutral_refcount_mode(&cfg)?;
        let page_store = Arc::new(match &backing {
            MetaBacking::File => {
                let pages_path = page_file(&cfg.path);
                if cfg.rebuild_free_list_on_open {
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
                }
            }
            // Trust-capacity open: high_water starts at device capacity so the
            // manifest + its catalog chains (which live anywhere below capacity)
            // are addressable. The true frontier + free list are recovered by the
            // bounded scan below, AFTER the manifest + dedup index are loaded.
            MetaBacking::Device { page_device, .. } => {
                PageStore::open_on_device(page_device.clone())?
            }
        });
        let page_cache = Arc::new(PageCache::new_with_pin_budget(
            page_store.clone(),
            cfg.page_cache_bytes,
            cfg.index_pin_bytes,
        ));
        let metrics = Arc::new(MetaMetrics::new());
        metrics
            .set_rc_checkpoint_mode(cfg.bfg_threads_enabled, cfg.rc_checkpoint_streaming_enabled);
        page_store.attach_metrics(metrics.clone());
        let (mut manifest_store, mut manifest) =
            ManifestStore::open_existing(page_store.clone(), page_cache.clone(), faults.clone())?;
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

        // commit 8: open every volume recorded in the manifest.
        // Earlier versions of the manifest (v3/v4/v5) are not readable —
        // is fresh-install only, and `Manifest::decode` rejects
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
                    entry.flags,
                    entry.dead_list_head_pid,
                    entry.dead_list_tail_pid,
                    entry.parent_vol_ord,
                    entry.branched_at_lsn,
                    entry.promotion_cursor,
                    entry.page_dead_list_head_pid,
                    entry.page_dead_list_tail_pid,
                    entry.page_live_list_head_pid,
                    entry.page_live_list_tail_pid,
                    entry.promoted_log_head_pid,
                    entry.promoted_log_tail_pid,
                    // v19: sticky clone-lineage flag arms the livelist
                    // capture threshold — covers promoted ex-clones too
                    // (flag persists past `PromotionComplete`).
                    (entry.flags & crate::manifest::VOLUME_FLAG_CLONE_LINEAGE != 0)
                        .then_some(entry.branched_at_lsn),
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
        // v25: a non-NULL `dedup_migration_old_head` means the last durable
        // manifest was mid online-modulus-resize (Growing). Open BOTH tables and
        // resume; else open the single (Single-phase) table. The device-frontier
        // protected set below uses union-aware `referenced_page_ids()`, so BOTH
        // tables' pages are protected from reallocation.
        let dedup_index = if manifest.dedup_migration_old_head != crate::types::NULL_PAGE {
            Arc::new(crate::dedup::DedupIndex::open_growing(
                page_store.clone(),
                page_cache.clone(),
                dedup_index_meta_pid,
                manifest.dedup_migration_old_head,
                cfg.dedup_l1_cache_entries,
                manifest.dedup_shards as usize,
                cfg.dedup_drainer_enabled,
            )?)
        } else {
            Arc::new(crate::dedup::DedupIndex::open(
                page_store.clone(),
                page_cache.clone(),
                dedup_index_meta_pid,
                cfg.dedup_l1_cache_entries,
                manifest.dedup_shards as usize,
                cfg.dedup_drainer_enabled,
            )?)
        };

        // Device path only: recover the true frontier + free list by a bounded
        // scan. `open_on_device` above set high_water to device capacity so the
        // manifest + shard + dedup opens could address their pages; now that the
        // manifest and dedup index are loaded we can lower high_water to the real
        // frontier and reclaim freed interior pages (a fixed LD has no EOF to
        // trust and no hole-punch — without this the meta region leaks until
        // full).
        //
        // The ceiling is the maximum of `page_high_water` and every durable page
        // reachable through a generation-stable, in-place-mutated root. Dedup and
        // refcount can both make fresh pages durable before the next manifest
        // publish, so a crash-to-older-generation may leave either above the
        // committed high-water. The protected set also includes both manifest
        // slots' catalog/free-list runs: they remain owned across the A/B
        // generations and must never be reissued from either slot's bitmap.
        // Must run BEFORE lifecycle replay, which allocates pages (`cow_for_write`
        // during L2pPut apply).
        if backing.is_device() {
            let mut protected_pages: HashSet<PageId> =
                crate::manifest::catalog_chain_pids_all_slots(&page_store)
                    .into_iter()
                    .collect();
            for &meta_pid in manifest.refcount_shard_roots.iter() {
                if meta_pid == crate::types::NULL_PAGE {
                    continue;
                }
                protected_pages.extend(crate::refcount::PagedRefcountArray::referenced_page_ids(
                    &page_store,
                    meta_pid,
                )?);
            }
            protected_pages.extend(dedup_index.referenced_page_ids());
            let protected_max = protected_pages.iter().copied().max().unwrap_or(0);
            let frontier = manifest
                .page_high_water
                .max(protected_max.saturating_add(1));
            if manifest.free_list_head != crate::types::NULL_PAGE {
                // Fast path: the free list was persisted as a bitmap
                // (`free_list_head`), so load it in O(bitmap pages) instead of a
                // ~O(meta size) page scan (~75 s → <5 s on a large meta LD). See
                // `load_persisted_free_list` for the dedup reconciliation that
                // preserves the frontier invariant.
                let started = std::time::Instant::now();
                let free_list = load_persisted_free_list(
                    &page_store,
                    manifest.free_list_head,
                    manifest.page_high_water,
                    frontier,
                    &protected_pages,
                )?;
                let free_list_len = free_list.len();
                page_store.install_free_list(frontier, free_list);
                tracing::info!(
                    page_high_water = manifest.page_high_water,
                    protected_max_ref = protected_max,
                    protected_pages = protected_pages.len(),
                    frontier,
                    free_list_len,
                    elapsed_ms = started.elapsed().as_millis(),
                    "metadb device open: persisted free-list load complete (no scan)"
                );
            } else {
                // Fallback (old db / never-persisted): bounded scan, as before.
                page_store.rebuild_free_list_bounded(frontier)?;
                tracing::info!(
                    page_high_water = manifest.page_high_water,
                    protected_max_ref = protected_max,
                    protected_pages = protected_pages.len(),
                    frontier,
                    recovered_high_water = page_store.high_water(),
                    "metadb device open: bounded free-list rebuild complete"
                );
            }
        }

        // Buffer-as-sole-journal lifecycle journal cutover retired the WAL writer.
        // Recovery now starts from `manifest.checkpoint_lsn` (the last
        // durable manifest commit) and folds the lifecycle journal
        // forward; data-plane state is rebuilt from onyx's LV2 buffer
        // on its own replay path, so this open() body only worries
        // about lifecycle records sitting above the manifest
        // watermark.
        //
        // BFG: capture the durable BFG so
        // every replayed lifecycle record folds into `checkpoint_bfg
        // + 1` — the Open BFG the new BfgStateMachine will be
        // constructed with below.
        let replay_open_bfg = manifest.checkpoint_bfg + 1;
        let mut replayed_drop = false;
        let mut mutated_volumes = false;

        // Buffer-as-sole-journal lifecycle replay: replay any uncovered
        // lifecycle records into the freshly-opened in-memory state.
        // Lifecycle records carry a monotonic `seq` but not an LSN; we
        // synthesise replay LSNs sequentially starting from
        // `manifest.checkpoint_lsn + 1` so every page generation
        // stamp the original apply wrote is strictly >= our replay
        // LSN, and the `header.generation >= lsn` idempotency guard
        // in `apply_drop_snapshot_pages` correctly fires SKIP on a page
        // already processed before the crash.
        let mut last_applied = manifest.checkpoint_lsn;
        let mut lifecycle_max_seq = manifest.lifecycle_replay_seq;
        let mut lifecycle_replayed_anything = false;
        // File: replay only when the segment directory exists (a fresh DB opened
        // with no lifecycle ops yet has none). Device: the journal ring always
        // exists inside the meta LD, so always attempt replay (an empty ring
        // scans to zero records).
        let should_replay = match &backing {
            MetaBacking::File => lifecycle_log_dir(&cfg.path).exists(),
            MetaBacking::Device { .. } => true,
        };
        if should_replay {
            let from_seq = manifest.lifecycle_replay_seq;
            let ring_head = manifest.journal_ring_head;
            let outcome = replay_lifecycle_journal_into(
                &backing,
                &cfg.path,
                ring_head,
                &mut manifest,
                &mut volumes,
                &refcount_shards,
                &dedup_index,
                &page_store,
                &page_cache,
                &faults,
                metrics.clone(),
                last_applied,
                replay_open_bfg,
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

        // `lsn_alloc` is constructed below, AFTER the post-replay commit
        // + orphan reclaim and AFTER the recovery LSN-frontier bump (those
        // consume the un-bumped `last_applied`; the allocator must resume at
        // the bumped frontier). See the bump just before `build_dedup_lanes`.

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
            Some(Mutex::new(match &backing {
                MetaBacking::File => {
                    LifecycleWriter::File(crate::lifecycle_log::LifecycleJournal::open(
                        &lifecycle_log_dir(&cfg.path),
                        next_seq,
                        cfg.wal_segment_bytes,
                    )?)
                }
                // Reopen the ring at the persisted prune boundary; the tail +
                // live-block count are rediscovered by scanning from there.
                MetaBacking::Device { journal_device, .. } => {
                    LifecycleWriter::Ring(RingJournal::open(
                        journal_device.clone(),
                        manifest.journal_ring_head,
                        next_seq,
                    )?)
                }
            }))
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
            // lifecycle replay: stamp the lifecycle replay watermark alongside
            // `checkpoint_lsn` so any lifecycle records we just folded
            // in are now "covered" by the durable manifest. Without
            // this stamp the next open would re-replay the same
            // records (legal: replays are idempotent via page
            // generation guards) but would also synthesise fresh LSNs
            // that overlap with later WAL traffic.
            manifest.lifecycle_replay_seq = lifecycle_max_seq;
            manifest_store.commit(&mut manifest)?;
            commit_l2p_checkpoint(&mut l2p_guards, last_applied.max(1) + 1)?;
            commit_refcount_checkpoint(&refcount_shards, last_applied.max(1) + 1)?;

            // This commit advanced `checkpoint_lsn` to `last_applied` and made
            // every L2P root durable. Every page-death those durable roots
            // imply (`death_lsn <= last_applied`) must already be sealed durably
            // in this same commit. If a volume still holds such a death only in its volatile
            // accumulator, the durable deadlist is INCOMPLETE w.r.t. the durable
            // roots and a later `drop_snapshot` fires a COMPLETENESS HOLE. This
            // is the recovery-commit analogue of the drop_snapshot accumulator-seal tripwire;
            // it converts the (multi-hour-soak-only) latent leak into a
            // deterministic in-process failure.
            #[cfg(debug_assertions)]
            for vol in &sorted {
                for (s, acc) in vol.page_dead_list.iter().enumerate() {
                    debug_assert!(
                        acc.peek().iter().all(|r| r.death_lsn > last_applied),
                        "recovery post-replay commit advanced checkpoint_lsn={last_applied} but \
                         volume {} shard {s} retains an unsealed page-death death_lsn<=checkpoint_lsn \
                         (recovery deadlist-seal gap): {:?}",
                        vol.ord,
                        acc.peek()
                            .iter()
                            .map(|r| r.death_lsn)
                            .filter(|&d| d <= last_applied)
                            .collect::<Vec<_>>(),
                    );
                }
            }

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

        // A large, explicitly provisioned metadata cache should not start
        // empty and force the first foreground overwrite burst to synchronously
        // fault old L2P and refcount pages through the metadata RAID. Only do a
        // full data-page warmup when the complete allocated page frontier fits
        // in the cache; otherwise a scan would evict its own beginning and add
        // startup IO without producing a resident working set.
        const DATA_PREWARM_MIN_CACHE_BYTES: u64 = 8 * 1024 * 1024 * 1024;
        let high_water_bytes = page_store
            .high_water()
            .saturating_mul(crate::config::PAGE_SIZE as u64);
        if cfg.page_cache_bytes >= DATA_PREWARM_MIN_CACHE_BYTES
            && high_water_bytes <= cfg.page_cache_bytes
        {
            let started = std::time::Instant::now();
            let mut l2p_pages = 0u64;
            for volume in volumes.values() {
                for shard in &volume.shards {
                    l2p_pages += shard.tree.read().warmup_all_pages()?;
                }
            }
            let mut rc_pages = 0u64;
            for shard in &refcount_shards {
                rc_pages += shard.rc.warmup_data_pages()?;
            }
            let dedup_page_ids = dedup_index.data_page_ids();
            for chunk in dedup_page_ids.chunks(4096) {
                page_cache.get_many(chunk)?;
            }
            tracing::info!(
                l2p_pages,
                rc_pages,
                dedup_pages = dedup_page_ids.len(),
                elapsed_ms = started.elapsed().as_millis(),
                cache_bytes = cfg.page_cache_bytes,
                high_water_bytes,
                "metadb data-page cache prewarm complete"
            );
        } else {
            tracing::info!(
                cache_bytes = cfg.page_cache_bytes,
                high_water_bytes,
                "metadb data-page cache prewarm skipped: allocated frontier does not fit"
            );
        }

        // Recovery LSN frontier bump. `last_applied` so far is the recovery
        // FLOOR (`checkpoint_lsn = min(durable_seq)`, then advanced by any
        // lifecycle replay). In buffer mode the floor can sit far below an
        // active shard's `durable_seq` when buffer shards are uneven /
        // uncompacted, yet that active shard holds durable array/tree pages
        // whose `generation == its durable_seq`. If the new-op allocator
        // resumed at floor+1 it would hand out LSNs BELOW those generations,
        // and a fresh op's stage-time replay-skip (`page_lsn >= lsn`,
        // `refcount/shard.rs`) would wrongly drop it (page-rc over-count /
        // leak). Resume instead above the durable FRONTIER = `max(durable_seq)`
        // (>= every durable page generation; see `Manifest::max_durable_seq`).
        //
        // `checkpoint_lsn` (the recovery/prune floor) is deliberately NOT
        // touched — recovery still replays from `checkpoint_lsn + 1`; only
        // the volatile allocator/apply watermark moves up. The bump runs
        // AFTER the post-replay commit + orphan reclaim (which consume the
        // un-bumped `last_applied` as a durable_seq override / reclaim
        // generation) and BEFORE the dedup lanes, `lsn_alloc`,
        // `last_applied_lsn`, and `bfg.record_lsn` (all seeded from the
        // bumped value). In the replayed path the post-replay commit set
        // every shard's durable_seq to `last_applied`, so the frontier
        // equals `last_applied` and the bump is a no-op; it only acts on the
        // skipped-journal path where the durable manifest already carries
        // uneven per-shard durable_seq above the floor. Pure function of the
        // durable manifest ⇒ idempotent across reopens.
        last_applied = last_applied.max(manifest.max_durable_seq());
        let lsn_alloc = LsnAllocator::new(last_applied + 1);

        // Capture before `manifest` is moved into `ManifestState` below.
        let manifest_dedup_shards = manifest.dedup_shards as usize;
        let manifest_checkpoint_bfg = manifest.checkpoint_bfg;
        let manifest_buffer_applied_watermark = manifest.last_processed_buffer_seq;
        let drainer_cfg = cfg.clone();
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
        let livelist_condense_min_segments = cfg.livelist_condense_min_segments;
        let livelist_condense_params = super::livelist_condense::LivelistCondenseParams {
            min_segments: cfg.livelist_condense_min_segments,
            idle_interval_ms: cfg.livelist_condense_idle_interval_ms,
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

        let deferred_outcomes = Arc::new(crate::db::commit::DeferredOutcomeAggregator::new(
            metrics.clone(),
            faults.clone(),
        ));
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
            last_applied_lsn_observed: AtomicU64::new(last_applied),
            commit_cvar: Condvar::new(),
            applied_set: Mutex::new(BTreeSet::new()),
            active_apply_lsns: Mutex::new(BTreeSet::new()),
            commit_poison: Mutex::new(None),
            commit_poisoned: Arc::new(AtomicBool::new(false)),
            sync_poison: Mutex::new(None),
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
            livelist_condense: Mutex::new(None),
            lineage_gc_worker: Mutex::new(None),
            l2p_buffer_enabled: cfg.l2p_buffer_enabled,
            lineage_gc_emit_freepbas: cfg.lineage_gc_emit_freepbas,
            lineage_gc_drop_dedup_shared: cfg.lineage_gc_drop_dedup_shared,
            freed_pbas_sink: Mutex::new(None),
            deferred_outcomes,
            commit_deferred_outcomes_enabled: cfg.commit_deferred_outcomes_enabled,
            wal_async_commits_enabled: cfg.wal_async_commits_enabled,
            // resume BFG accounting at the manifest's last
            // synced BFG. open_bfg will be checkpoint_bfg + 1 — replayed
            // ops fold into that BFG so the next quiesce/sync persists
            // them with a fresh checkpoint_bfg.
            bfg: Arc::new(crate::bfg::BfgStateMachine::new(manifest_checkpoint_bfg)),
            bfg_threads_enabled: cfg.bfg_threads_enabled,
            rc_checkpoint_streaming_enabled: cfg.rc_checkpoint_streaming_enabled,
            parallel_l2p_drain_enabled: cfg.parallel_l2p_drain_enabled,
            parallel_l2p_drain_workers: cfg.parallel_l2p_drain_workers,
            l2p_drain_chunk_entries: cfg.l2p_drain_chunk_entries,
            l2p_checkpoint_pipeline_enabled: cfg.l2p_checkpoint_pipeline_enabled,
            bfg_l2p_work_limit: cfg.l2p_buffer_soft_entries,
            rc_authoritative_reclaim: cfg.rc_authoritative_reclaim,
            bfg_quiesce_notifier: Arc::new(crate::db::bfg_quiesce::QuiesceNotifier::new()),
            bfg_sync_notifier: Arc::new(crate::db::bfg_sync::SyncNotifier::new()),
            bfg_quiesce: Mutex::new(None),
            bfg_sync: Mutex::new(None),
            l2p_prefold: Mutex::new(None),
            buffer_applied_watermark: AtomicU64::new(manifest_buffer_applied_watermark),
            // lifecycle replay: resume the lifecycle watermark from whatever
            // the post-replay manifest commit just persisted. Future
            // lifecycle ops `fetch_max` onto this value so the next
            // checkpoint stamps a monotonic `lifecycle_replay_seq`
            // rather than regressing it to 0.
            lifecycle_applied_watermark: AtomicU64::new(lifecycle_max_seq),
            lifecycle_journal,
            pending_sync_tasks: Mutex::new(Vec::new()),
        };
        // Stamp `slot_max_lsn(open_bfg)` with the post-replay
        // `last_applied`. `apply_replay_batch` and `apply_op_bare` fold
        // every replayed op into `manifest.checkpoint_bfg + 1` without
        // going through `BfgGuard::record_lsn`, so without this stamp
        // `slot_max_lsn(open_bfg)` would be 0 — which would regress
        // `manifest.checkpoint_lsn` to 0 on the next flush body that
        // reads `wal_checkpoint = self.bfg.slot_max_lsn(bfg)`.
        //
        // Stamping is safe before any commit thread starts: the
        // commit path's own `record_lsn` is max-monotonic, so a later
        // commit with `lsn > last_applied` correctly bumps the slot.
        // We also stamp when nothing was replayed (so `last_applied =
        // manifest.checkpoint_lsn`); that keeps the very first flush's
        // `wal_checkpoint` >= the durable manifest's `checkpoint_lsn`
        // even on a clean open with no commits yet.
        db.bfg.record_lsn(db.bfg.open_bfg(), last_applied);
        db.recompute_all_snap_infos();
        // The refcount fold is inline + per-BFG-slot (no background rc
        // drainer — see `refcount::shard`).
        // Spawn the async dedup-index drainers AFTER replay (so they
        // never observe mid-replay state). No-op when
        // `dedup_drainer_enabled = false`.
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
        // BFG: spawn the background livelist-condense worker
        // (independent of async_reclaim). Drop stops it before page_store /
        // page_cache teardown.
        if livelist_condense_min_segments > 0 {
            db.start_livelist_condense(livelist_condense_params);
        }
        // BFG: the background L2P drainer
        // is now the `BfgSyncThread`, spawned below when
        // `bfg_threads_enabled` is true. With the legacy
        // `L2pCompactor` retired, the post-replay flush at the
        // bottom of `open_with_config_and_faults` is the sole
        // initial drainer; subsequent drains come from either the
        // sync thread (threads on) or `flush_with_gate`'s inline
        // path (threads off).
        let db = Arc::new(db);
        if db.bfg_threads_enabled {
            Self::start_bfg_threads(&db, cfg.bfg_timeout_ms);
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

/// Recover the device page-store free list from the persisted bitmap chain
/// (`free_list_head`) instead of scanning every page in the meta region.
///
/// The bitmap covers `[FIRST_DATA_PAGE, page_high_water)`; the tiny tail
/// `[page_high_water, frontier)` (non-empty only after a crash where an
/// in-place stable root grew past the committed high-water) is recovered with a
/// bounded scan. Finally every protected page is removed from the result.
/// Protected pages include both manifest slots' catalog/free-list runs and the
/// durable pages reachable through the stable dedup/refcount meta heads. Those
/// roots can outlive the bitmap generation or be mutated in place before a
/// newer manifest is published, so the persisted bitmap is not authoritative
/// for their ownership.
fn load_persisted_free_list(
    page_store: &Arc<PageStore>,
    free_list_head: PageId,
    page_high_water: u64,
    frontier: u64,
    protected_pages: &HashSet<PageId>,
) -> Result<Vec<PageId>> {
    let bitmap = crate::manifest::catalog::read_free_list_run(page_store, free_list_head)?;
    let mut free: Vec<PageId> =
        crate::manifest::catalog::decode_free_list_bitmap(&bitmap, page_high_water);
    // Tail above the bitmap's range (usually empty).
    if frontier > page_high_water {
        free.extend(page_store.scan_free_range(page_high_water, frontier)?);
    }
    // Reconcile: an owned page must never be reusable even if an older bitmap
    // still carries its bit.
    free.retain(|pid| *pid < frontier && !protected_pages.contains(pid));
    Ok(free)
}

fn validate_rc_neutral_refcount_mode(cfg: &Config) -> Result<()> {
    if !cfg.lineage_gc_emit_freepbas {
        return Err(MetaDbError::InvalidArgument(
            "lineage_gc_emit_freepbas=false is no longer supported: rc-neutral \
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

/// lifecycle replay: replay every lifecycle-log record above
/// `manifest.lifecycle_replay_seq` against the post-WAL-replay
/// in-memory state. Mirrors the per-op dispatch in the WAL-replay
/// closure for the variants that have migrated to the lifecycle
/// journal in lifecycle journal split (CreateVolume / DropVolume / CloneVolume /
/// DropSnapshot / PromotionChunk / PromotionComplete).
///
/// LSN assignment: each record gets `starting_lsn + i + 1` (`i` = its
/// position in the seq-ordered iteration). The original apply
/// reserved its LSN via `wal.reserve_unlogged` AFTER the WAL's
/// `next_lsn` was already >= `starting_lsn + 1`, so every page
/// generation stamp on disk is strictly >= the LSN we use here. That
/// keeps the `header.generation >= lsn` idempotency guard in
/// `apply_drop_snapshot_pages` firing SKIP correctly when the original
/// apply finished before the crash.
#[allow(clippy::too_many_arguments)]
fn replay_lifecycle_journal_into(
    backing: &MetaBacking,
    root_path: &Path,
    ring_head: u64,
    manifest: &mut Manifest,
    volumes: &mut HashMap<VolumeOrdinal, Arc<Volume>>,
    refcount_shards: &[Shard],
    dedup_index: &Arc<crate::dedup::DedupIndex>,
    page_store: &Arc<PageStore>,
    page_cache: &Arc<PageCache>,
    faults: &Arc<FaultController>,
    metrics: Arc<MetaMetrics>,
    starting_lsn: Lsn,
    replay_open_bfg: crate::types::Bfg,
    l2p_buffer_enabled: bool,
    rc_authoritative: bool,
    from_seq: u64,
) -> Result<LifecycleReplayOutcome> {
    use crate::lifecycle_log::{LifecycleJournal, LifecycleRecord, op as lifecycle_op};
    let mut outcome = LifecycleReplayOutcome {
        max_seq: from_seq,
        lsns_consumed: 0,
        mutated_volumes: false,
        replayed_drop_snapshot: false,
    };
    // The per-record apply is backend-agnostic; only the driver (segment-file
    // walk vs block-ring scan) differs. `&mut apply` (an `&mut FnMut`) is itself
    // `FnMut`, so the same closure feeds either driver.
    let mut apply = |rec: LifecycleRecord| -> Result<()> {
        if rec.seq <= from_seq {
            // The driver already filters at this threshold; the defensive check
            // keeps the apply path immune to a future relaxation of that contract.
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
            replay_open_bfg,
            l2p_buffer_enabled,
            rc_authoritative,
            &op,
            &mut outcome,
        )?;
        outcome.max_seq = outcome.max_seq.max(rec.seq);
        Ok(())
    };
    match backing {
        MetaBacking::File => {
            LifecycleJournal::replay(&lifecycle_log_dir(root_path), from_seq, &mut apply)?;
        }
        MetaBacking::Device { journal_device, .. } => {
            RingJournal::replay(journal_device, ring_head, from_seq, &mut apply)?;
        }
    }
    Ok(outcome)
}

#[allow(clippy::too_many_arguments)]
fn apply_lifecycle_record_replay(
    manifest: &mut Manifest,
    volumes: &mut HashMap<VolumeOrdinal, Arc<Volume>>,
    refcount_shards: &[Shard],
    // BFG: page-rc deleted, so the clone/drop replay arms no longer
    // consult these. Kept in the signature for call-site stability.
    _dedup_index: &Arc<crate::dedup::DedupIndex>,
    page_store: &Arc<PageStore>,
    page_cache: &Arc<PageCache>,
    _faults: &Arc<FaultController>,
    metrics: &Arc<MetaMetrics>,
    lsn: Lsn,
    replay_open_bfg: crate::types::Bfg,
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
                    lsn,
                )?;
                volumes.insert(*ord, Arc::new(Volume::new(*ord, shards, lsn)));
                let durable_seqs = vec![lsn; *shard_count as usize].into_boxed_slice();
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
                    page_dead_list_head_pid: crate::types::NULL_PAGE,
                    page_dead_list_tail_pid: crate::types::NULL_PAGE,
                    page_live_list_head_pid: crate::types::NULL_PAGE,
                    page_live_list_tail_pid: crate::types::NULL_PAGE,
                    promoted_log_head_pid: crate::types::NULL_PAGE,
                    promoted_log_tail_pid: crate::types::NULL_PAGE,
                });
                outcome.mutated_volumes = true;
            }
            manifest.next_volume_ord = manifest
                .next_volume_ord
                .max(ord.checked_add(1).unwrap_or(u16::MAX));
        }
        LifecycleOp::DropVolume {
            ord,
            pages,
            free_pages,
        } => {
            // NB: this arm is normally a no-op on replay — `drop_volume`
            // commits the volume-removed manifest BEFORE the WAL submit, so
            // `volumes.contains_key(ord)` is false here and the crash backstop
            // is `reclaim_orphan_pages`. The free-set `free_pages` only ever drives
            // the live path; it is threaded through for completeness.
            if volumes.contains_key(ord) {
                apply_drop_volume(page_store, lsn, pages, free_pages.as_deref())?;
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
                // BFG: per-L2P-page refcounting is deleted, so the
                // clone replay no longer increfs the source roots (and there
                // is no stale-rc buffer hazard to sweep — the source pages are
                // unmodified). Just rebuild the clone's shards pointing at the
                // shared source roots.
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
                        crate::manifest::VOLUME_FLAG_CLONE_LINEAGE,
                        crate::types::NULL_PAGE,
                        crate::types::NULL_PAGE,
                        Some(*src_ord),
                        branched_at_lsn,
                        None,
                        crate::types::NULL_PAGE,
                        crate::types::NULL_PAGE,
                        crate::types::NULL_PAGE,
                        crate::types::NULL_PAGE,
                        // v20 promoted-PBA log anchors (NULL at clone replay).
                        crate::types::NULL_PAGE,
                        crate::types::NULL_PAGE,
                        Some(branched_at_lsn),
                    )),
                );
                let durable_seqs = vec![lsn; shard_count as usize].into_boxed_slice();
                manifest.volumes.push(VolumeEntry {
                    ord: *new_ord,
                    shard_count,
                    l2p_shard_roots: actual_roots,
                    l2p_shard_durable_seq: durable_seqs,
                    created_lsn: lsn,
                    // v19: sticky clone-lineage flag (BFG).
                    flags: crate::manifest::VOLUME_FLAG_CLONE_LINEAGE,
                    dead_list_head_pid: crate::types::NULL_PAGE,
                    dead_list_tail_pid: crate::types::NULL_PAGE,
                    parent_vol_ord: Some(*src_ord),
                    branched_at_lsn,
                    promotion_cursor: None,
                    page_dead_list_head_pid: crate::types::NULL_PAGE,
                    page_dead_list_tail_pid: crate::types::NULL_PAGE,
                    page_live_list_head_pid: crate::types::NULL_PAGE,
                    page_live_list_tail_pid: crate::types::NULL_PAGE,
                    promoted_log_head_pid: crate::types::NULL_PAGE,
                    promoted_log_tail_pid: crate::types::NULL_PAGE,
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
            free_pages,
            merge,
        } => {
            // Drive the same page-free + per-pba decref the live
            // `Db::drop_snapshot` path uses. Page generations + the page-rc
            // `stage` replay-skip gate idempotency across re-applies. free-set:
            // `free_pages` carries the frozen authoritative free-set (Some
            // for non-clone drops, None for clone-involved); the apply core
            // honours it exactly as on the live path.
            apply_drop_snapshot_pages_and_decrefs(
                page_store,
                refcount_shards,
                lsn,
                pages,
                pba_decrefs,
                free_pages.as_deref(),
            )?;
            // Crash-recovery completeness (merge-reanchor): re-apply the page-deadlist
            // MERGE re-anchor from the op ATOMICALLY with the snapshot removal
            // below — mirrors the live `drop_snapshot`. The carried segment is
            // durable (synced before the op). Head → bump the source volume's
            // HEAD anchors (+ its manifest entry; the post-replay
            // `refresh_manifest_entries` also folds the atomics); Snapshot →
            // set the inheritor snapshot's tail. (accumulator-seal's accumulator seal rode
            // the pre-WAL commit, so it is already durable.)
            if let Some((target, anchor)) = merge {
                use std::sync::atomic::Ordering;
                match target {
                    crate::lifecycle_log::DropMergeTarget::Head { vol_ord } => {
                        if let Some(v) = volumes.get(vol_ord) {
                            v.page_dead_list_head_pid.store(*anchor, Ordering::Release);
                            v.page_dead_list_tail_pid.store(*anchor, Ordering::Release);
                        }
                        if let Some(e) = manifest.volumes.iter_mut().find(|e| e.ord == *vol_ord) {
                            e.page_dead_list_head_pid = *anchor;
                            e.page_dead_list_tail_pid = *anchor;
                        }
                    }
                    crate::lifecycle_log::DropMergeTarget::Snapshot { id: sid } => {
                        if let Some(sn) = manifest.snapshots.iter_mut().find(|s| s.id == *sid) {
                            sn.page_dead_list_tail_pid = *anchor;
                        }
                    }
                }
            }
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
                replay_open_bfg,
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
            // lifecycle discard support replay: the record only carries the range —
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
            let end = (*start_lba).checked_add(*count as u64).ok_or_else(|| {
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
                            capture_watermark: s.capture_watermark,
                            l2p_shard_roots: s.l2p_shard_roots.clone(),
                        })
                        .collect()
                };
                apply_l2p_range_delete(
                    volumes,
                    refcount_shards,
                    lsn,
                    replay_open_bfg,
                    *vol_ord,
                    &captured,
                    &snap_lookup(*vol_ord),
                    rc_authoritative,
                )?;
            }
        }
        // `take_snapshot` is manifest-only: the snapshot-root incref folds
        // inside the take's own BFG sync cycle, atomic with the manifest commit,
        // with no lifecycle-journal record. A `TakeSnapshot` record on disk
        // therefore indicates the on-disk schema has moved ahead of this binary;
        // surface it as corruption rather than silently misapplying.
        LifecycleOp::TakeSnapshot { .. } => {
            return Err(MetaDbError::Corruption(format!(
                "lifecycle replay: TakeSnapshot (tag 0x{:02x}) is not \
                 routed through the lifecycle journal",
                op.tag(),
            )));
        }
    }
    Ok(())
}
