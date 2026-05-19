use super::*;

const FLUSH_RECLAIM_MIN_BUDGET_PAGES: usize = 4_096;
const FLUSH_RECLAIM_MAX_BUDGET_PAGES: usize = 1_048_576;
const FLUSH_RECLAIM_BACKLOG_HARD_CAP_PAGES: usize = 16 * 1_048_576;
const FLUSH_INSTALL_PAGE_BUDGET: usize = 64;
const FLUSH_INSTALL_CLEANUP_BUDGET: usize = 64;
const FLUSH_INSTALL_STEP_WARN_US: u64 = 100_000;

fn micros(duration: std::time::Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn flush_reclaim_budget(pending_reclaim_pages: usize, pages_written: usize) -> usize {
    let write_scaled = pages_written.saturating_mul(8);
    let backlog_scaled = pending_reclaim_pages / 2;
    let pressure_cap = if pending_reclaim_pages >= FLUSH_RECLAIM_BACKLOG_HARD_CAP_PAGES {
        FLUSH_RECLAIM_MAX_BUDGET_PAGES
    } else if pending_reclaim_pages >= 4 * 1_048_576 {
        FLUSH_RECLAIM_MAX_BUDGET_PAGES / 2
    } else {
        FLUSH_RECLAIM_MAX_BUDGET_PAGES / 8
    };
    FLUSH_RECLAIM_MIN_BUDGET_PAGES
        .max(write_scaled)
        .max(backlog_scaled)
        .min(pressure_cap)
}

/// Per-flush selection of shards that this `flush_with_gate`
/// invocation will sample. `l2p[v][s]` mirrors
/// `volumes[v].shards[s]`; `rc[i]` mirrors `refcount_shards[i]`.
/// True = sample this shard this round, write a new sealed root +
/// bump its `last_flushed_lsn`. False = leave it alone; its root
/// in the manifest and its in-memory dirty pages carry over to the
/// next flush.
#[derive(Debug, Clone)]
struct SelectedShards {
    l2p: Vec<Vec<bool>>,
    rc: Vec<bool>,
}

impl SelectedShards {
    fn l2p_any(&self) -> bool {
        self.l2p.iter().any(|v| v.iter().any(|s| *s))
    }
    fn rc_any(&self) -> bool {
        self.rc.iter().any(|s| *s)
    }
    fn is_empty(&self) -> bool {
        !self.l2p_any() && !self.rc_any()
    }
}

struct CheckpointInstallReceiver {
    kind: &'static str,
    vol_ord: Option<VolumeOrdinal>,
    shard: usize,
    rx: crossbeam_channel::Receiver<Result<Vec<PageId>>>,
}

/// One volume's drained dead-list records during a flush round.
/// Carried from the drain step (under `apply_gate.write()`) through
/// segment build + IO + manifest commit. On any failure between drain
/// and commit, `records` is moved back into the volume's buffer via
/// `restore_front` (see [`crate::deadlist::DeadListState::restore_front`]).
struct DeadListDrainEntry {
    vol: Arc<Volume>,
    records: Vec<crate::deadlist::DeadRecord>,
    old_head: PageId,
    old_tail: PageId,
}

/// One volume's allocated segment in the IO phase: the contiguous page
/// run handed out by `page_store.allocate_run`. Used both for tracking
/// what to free on rollback and what to commit into the volume's
/// `dead_list_*_pid` atomics + manifest after sync succeeds.
struct DeadListSegmentPlan {
    vol: Arc<Volume>,
    start_pid: PageId,
    page_count: u32,
    old_head: PageId,
    old_tail: PageId,
}

/// Drop guard installed at the top of `Db::flush_with_gate`. Every
/// per-shard `RcShard::begin_checkpoint` preempts that shard's
/// priority-3 drainer thread; the drainer is left parked and must be
/// resumed before the flush returns. We park-everywhere / resume-once
/// using a guard so every error-return path in flush_with_gate
/// resumes correctly without each `return Err(...)` having to call
/// `resume_drainer` explicitly. `resume_drainer` is idempotent — no-op
/// on shards where the drainer wasn't preempted (or isn't attached).
struct RcDrainerResumeGuard<'a> {
    shards: &'a [super::Shard],
}

impl Drop for RcDrainerResumeGuard<'_> {
    fn drop(&mut self) {
        for shard in self.shards {
            shard.rc.resume_drainer();
        }
    }
}

struct CheckpointInstallState<F> {
    flushed: F,
    flushed_pages: usize,
    next_flushed_page: usize,
    flushed_private: HashMap<PageId, bool>,
    retired_pages: Vec<PageId>,
    next_retired_page: usize,
    private_pages: Vec<PageId>,
    next_private_page: usize,
    checkpoint_frees: Vec<PageId>,
    steps_started: u64,
}

impl<F> CheckpointInstallState<F> {
    fn new(
        flushed: F,
        flushed_pages: usize,
        retired_pages: Vec<PageId>,
        private_pages: Vec<PageId>,
    ) -> Self {
        Self {
            flushed,
            flushed_pages,
            next_flushed_page: 0,
            flushed_private: HashMap::new(),
            retired_pages,
            next_retired_page: 0,
            private_pages,
            next_private_page: 0,
            checkpoint_frees: Vec::new(),
            steps_started: 0,
        }
    }

    fn page_phases_finished(&self) -> bool {
        self.next_flushed_page >= self.flushed_pages
            && self.next_retired_page >= self.retired_pages.len()
            && self.next_private_page >= self.private_pages.len()
    }
}

fn enqueue_l2p_checkpoint_install_step(
    lane: ApplyLaneHandle,
    volume: Arc<Volume>,
    sid: usize,
    state: Arc<Mutex<CheckpointInstallState<crate::paged::cache::FlushedSnapshot>>>,
    tx: crossbeam_channel::Sender<Result<Vec<PageId>>>,
) {
    let next_lane = lane.clone();
    let enqueued_at = std::time::Instant::now();
    lane.enqueue_maintenance(Box::new(move || {
        match run_l2p_checkpoint_install_step(
            volume.clone(),
            sid,
            state.clone(),
            enqueued_at.elapsed(),
        ) {
            Ok(Some(frees)) => {
                let _ = tx.send(Ok(frees));
            }
            Ok(None) => {
                enqueue_l2p_checkpoint_install_step(next_lane, volume, sid, state, tx);
            }
            Err(err) => {
                let _ = tx.send(Err(err));
            }
        }
    }));
}

fn run_l2p_checkpoint_install_step(
    volume: Arc<Volume>,
    sid: usize,
    state: Arc<Mutex<CheckpointInstallState<crate::paged::cache::FlushedSnapshot>>>,
    queue_wait: std::time::Duration,
) -> Result<Option<Vec<PageId>>> {
    let total_started = std::time::Instant::now();
    let state_lock_started = std::time::Instant::now();
    let mut state = state.lock();
    let state_lock_elapsed = state_lock_started.elapsed();
    state.steps_started += 1;
    let step = state.steps_started;
    let start_flushed = state.next_flushed_page;
    let start_retired = state.next_retired_page;
    let start_private = state.next_private_page;
    let tree_lock_started = std::time::Instant::now();
    let mut tree = volume.shards[sid].tree.write();
    let tree_lock_elapsed = tree_lock_started.elapsed();
    let mut budget = FLUSH_INSTALL_PAGE_BUDGET;

    let pages_started = std::time::Instant::now();
    while budget > 0 && state.next_flushed_page < state.flushed_pages {
        let page_idx = state.next_flushed_page;
        if let Some((pid, clean)) = tree.install_flushed_checkpoint_page(&state.flushed, page_idx) {
            state.flushed_private.insert(pid, clean);
        }
        state.next_flushed_page += 1;
        budget -= 1;
    }

    while budget > 0 && state.next_retired_page < state.retired_pages.len() {
        let pid = state.retired_pages[state.next_retired_page];
        if let Some(pid) = tree.checkpoint_retired_page_committed(pid) {
            state.checkpoint_frees.push(pid);
        }
        state.next_retired_page += 1;
        budget -= 1;
    }

    while budget > 0 && state.next_private_page < state.private_pages.len() {
        let pid = state.private_pages[state.next_private_page];
        let flushed_clean = state.flushed_private.get(&pid).copied().unwrap_or(true);
        tree.checkpoint_private_page_committed(pid, flushed_clean);
        state.next_private_page += 1;
        budget -= 1;
    }
    let pages_elapsed = pages_started.elapsed();

    let cleanup_started = std::time::Instant::now();
    let cleanup_done = tree.finish_checkpoint_commit_step(FLUSH_INSTALL_CLEANUP_BUDGET)?;
    let cleanup_elapsed = cleanup_started.elapsed();
    let page_phases_finished = state.page_phases_finished();
    let done = page_phases_finished && cleanup_done;
    let checkpoint_frees = state.checkpoint_frees.len();
    let result = if !done {
        None
    } else {
        Some(std::mem::take(&mut state.checkpoint_frees))
    };
    let total_elapsed = total_started.elapsed();
    let queue_wait_us = micros(queue_wait);
    let total_us = micros(total_elapsed);
    let state_lock_us = micros(state_lock_elapsed);
    let tree_lock_us = micros(tree_lock_elapsed);
    let pages_us = micros(pages_elapsed);
    let cleanup_us = micros(cleanup_elapsed);
    if queue_wait_us >= FLUSH_INSTALL_STEP_WARN_US
        || total_us >= FLUSH_INSTALL_STEP_WARN_US
        || state_lock_us >= FLUSH_INSTALL_STEP_WARN_US
        || tree_lock_us >= FLUSH_INSTALL_STEP_WARN_US
        || pages_us >= FLUSH_INSTALL_STEP_WARN_US
        || cleanup_us >= FLUSH_INSTALL_STEP_WARN_US
    {
        tracing::warn!(
            kind = "l2p",
            vol_ord = volume.ord,
            shard = sid,
            step,
            queue_wait_us,
            total_us,
            state_lock_us,
            tree_lock_us,
            pages_us,
            cleanup_us,
            flushed_done = state.next_flushed_page,
            flushed_total = state.flushed_pages,
            retired_done = state.next_retired_page,
            retired_total = state.retired_pages.len(),
            private_done = state.next_private_page,
            private_total = state.private_pages.len(),
            flushed_step = state.next_flushed_page.saturating_sub(start_flushed),
            retired_step = state.next_retired_page.saturating_sub(start_retired),
            private_step = state.next_private_page.saturating_sub(start_private),
            cleanup_done,
            page_phases_finished,
            done,
            checkpoint_frees,
            "metadb: slow checkpoint install step"
        );
    }
    if let Some(frees) = result {
        Ok(Some(frees))
    } else {
        return Ok(None);
    }
}

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
            manifest_state: Mutex::new(ManifestState {
                store: manifest_store,
                manifest,
            }),
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
            apply_gate: ApplyGate::new(),
            last_applied_lsn: Mutex::new(0),
            commit_cvar: Condvar::new(),
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
                Arc::new(Volume::with_dead_list_anchor(
                    entry.ord,
                    shards,
                    entry.created_lsn,
                    entry.dead_list_head_pid,
                    entry.dead_list_tail_pid,
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
                            src_ord: _,
                            new_ord,
                            src_snap_id: _,
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
                                volumes
                                    .insert(*new_ord, Arc::new(Volume::new(*new_ord, shards, lsn)));
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
            manifest_state: Mutex::new(ManifestState {
                store: manifest_store,
                manifest,
            }),
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
            apply_gate: ApplyGate::new(),
            last_applied_lsn: Mutex::new(last_applied),
            commit_cvar: Condvar::new(),
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

    /// Start the background reclaim worker. Caller (`Db::create`
    /// / `Db::open`) checks `cfg.async_reclaim_enabled` before
    /// invoking; this method assumes the knob was true so it can
    /// stay on `&self` without re-reading config.
    fn start_async_reclaim(&self, params: super::async_reclaim::AsyncReclaimParams) {
        let worker = super::async_reclaim::AsyncReclaim::start(
            self.page_store.clone(),
            self.page_cache.clone(),
            self.metrics.clone(),
            params,
        );
        *self.async_reclaim.lock() = Some(worker);
    }

    /// Wake the background reclaim worker (if any). Called from
    /// `flush_with_gate` once a flush makes new
    /// `deferred_free` entries safe to reclaim. Idempotent — the
    /// worker condvar coalesces multiple notifications.
    fn notify_async_reclaim(&self) {
        if let Some(worker) = &*self.async_reclaim.lock() {
            worker.notify();
        }
    }

    /// True iff the background reclaim worker is active. Used by
    /// `flush_with_gate` to decide between inline and async
    /// reclaim paths.
    fn async_reclaim_active(&self) -> bool {
        self.async_reclaim.lock().is_some()
    }

    /// Start the B2 L2P buffer compactor. Caller (`Db::create` /
    /// `Db::open`) checks `cfg.l2p_buffer_enabled`.
    fn start_l2p_compactor(&self) {
        let worker = super::l2p_compactor::L2pCompactor::start(
            self.volumes.clone(),
            self.metrics.clone(),
            self.l2p_compactor_params,
        );
        *self.l2p_compactor.lock() = Some(worker);
    }

    /// Force-compact every L2P shard's buffer into its tree
    /// synchronously. Called from `flush_with_gate` (with
    /// `apply_gate.write()` held) and from the post-replay path in
    /// `open_with_config_and_faults` (before the background compactor
    /// is started). Skips shards whose buffer is empty.
    ///
    /// Returns the first error encountered; any successfully
    /// compacted shards have already advanced their `compacted_lsn`.
    pub(super) fn force_compact_l2p_buffers(&self) -> Result<()> {
        use std::time::Instant;
        if !self.l2p_buffer_enabled {
            return Ok(());
        }
        let vols: Vec<Arc<Volume>> = {
            let map = self.volumes.read();
            let mut out: Vec<Arc<Volume>> = map.values().cloned().collect();
            out.sort_by_key(|v| v.ord);
            out
        };
        for vol in vols {
            for shard in &vol.shards {
                if !shard.use_buffer {
                    continue;
                }
                let swap = match shard.l2p_buffer.swap_for_compaction() {
                    Some(h) => h,
                    None => continue,
                };
                let started = Instant::now();
                let mut tree = shard.tree.write();
                let apply_result: Result<()> = shard.l2p_buffer.with_draining(|d| -> Result<()> {
                    let draining = match d {
                        Some(map) => map,
                        None => return Ok(()),
                    };
                    super::l2p_compactor::compact_drain_into_tree(&mut tree, draining)
                });
                match apply_result {
                    Ok(()) => {
                        super::apply::publish_l2p_read_view(shard, &tree);
                        drop(tree);
                        shard.l2p_buffer.finish_compaction(swap.max_lsn);
                        self.metrics.record_l2p_buffer_compaction(
                            swap.count,
                            started.elapsed(),
                        );
                    }
                    Err(err) => {
                        drop(tree);
                        return Err(err);
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) fn l2p_buffer_enabled(&self) -> bool {
        self.l2p_buffer_enabled
    }

    /// Current cached manifest (as of the last durable manifest commit).
    pub fn manifest(&self) -> Manifest {
        self.manifest_state.lock().manifest.clone()
    }

    /// Enumerate all registered snapshots.
    pub fn snapshots(&self) -> Vec<SnapshotEntry> {
        self.manifest_state.lock().manifest.snapshots.clone()
    }

    /// Enumerate snapshots pinned to volume `vol_ord`. Returns an empty
    /// vec for unknown ordinals (the concept of "snapshots on a volume
    /// that doesn't exist" is well-defined: there are none).
    pub fn snapshots_for(&self, vol_ord: VolumeOrdinal) -> Vec<SnapshotEntry> {
        self.manifest_state
            .lock()
            .manifest
            .snapshots
            .iter()
            .filter(|s| s.vol_ord == vol_ord)
            .cloned()
            .collect()
    }

    /// Number of shards in this database. In Phase B commit 5 this reports
    /// the bootstrap volume's shard count; every volume in the map is
    /// created with the same shard count, so this remains the right answer
    /// once multi-volume support lands.
    pub fn shard_count(&self) -> usize {
        self.volume_zero().shards.len()
    }

    /// Number of pages currently allocated in the page store.
    pub fn high_water(&self) -> u64 {
        self.page_store.high_water()
    }

    /// Number of reclaimed pages currently available for reuse.
    pub fn free_list_len(&self) -> usize {
        self.page_store.free_list_len()
    }

    /// Snapshot shared page-cache counters.
    pub fn cache_stats(&self) -> PageCacheStats {
        self.page_cache.stats()
    }

    pub fn metrics_snapshot(&self) -> MetaMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn dedup_lsm_stats(&self) -> crate::LsmStats {
        // Dedup index moved off LSM; report zeroed stats so the onyx
        // status formatter still has a value to format. Use
        // [`Db::dedup_tier_sizes`] for the cuckoo L0/L1 occupancy.
        crate::LsmStats::default()
    }

    /// Per-shard dedup stats are unavailable: the cuckoo dedup_index
    /// has no shard concept. Returns an empty vec.
    pub fn dedup_lsm_stats_per_shard(&self) -> Vec<crate::LsmStats> {
        Vec::new()
    }

    /// Cuckoo dedup_index L0/L1 tier occupancy snapshot.
    pub fn dedup_tier_sizes(&self) -> crate::dedup::TierSizes {
        self.dedup_index.tier_sizes()
    }

    /// Diagnostic snapshot of in-memory bookkeeping that can grow
    /// unbounded if its drain path stalls (deferred reclaim, dispatch
    /// FIFO, per-shard apply lane queues, per-shard COW retired/private
    /// page sets, page-buf totals). Cheap: each field is a single
    /// `len()` call. Intended for OOM triage during soak — these are
    /// the structures most likely to leak when a worker thread falls
    /// behind. Aggregates across all volumes' L2P shards plus refcount
    /// shards plus the dedup lane.
    ///
    /// **Non-blocking**: this is called by the onyx status socket
    /// handler. Per-shard tree locks can be held by `flush.install`
    /// for seconds at a time; using a blocking `tree.read()` /
    /// `tree.lock()` here would freeze the status socket for the same
    /// duration. Instead, try-acquire each lock and skip the shard's
    /// contribution if it's contended. The result is best-effort and
    /// undercounts during install; that's acceptable for diagnostics.
    pub fn pending_state(&self) -> PendingState {
        let dispatch_pending = self.dispatch_state.lock().pending.len();
        let deferred_free = self.page_store.deferred_free_len();
        let dedup_lane_queue: usize = self.dedup_lanes.iter().map(|lane| lane.queue_len()).sum();
        let mut l2p_apply_queue = 0usize;
        let mut l2p_private_pages = 0usize;
        let mut l2p_retired_pages = 0usize;
        let mut l2p_pagebuf_total = 0usize;
        let mut l2p_pagebuf_dirty = 0usize;
        for volume in self.volumes.read().values() {
            for shard in &volume.shards {
                l2p_apply_queue += shard.apply_lane.queue_len();
                if let Some(tree) = shard.tree.try_read() {
                    let (priv_p, ret_p, total, dirty) = tree.growth_summary();
                    l2p_private_pages += priv_p;
                    l2p_retired_pages += ret_p;
                    l2p_pagebuf_total += total;
                    l2p_pagebuf_dirty += dirty;
                }
            }
        }
        let mut rc_apply_queue = 0usize;
        let mut rc_private_pages = 0usize;
        let rc_retired_pages = 0usize;
        let rc_pagebuf_total = 0usize;
        let rc_pagebuf_dirty = 0usize;
        let mut rc_pending_deltas = 0usize;
        for shard in &self.refcount_shards {
            rc_apply_queue += shard.apply_lane.queue_len();
            // Paged-array refcount has no COW / private / retired
            // page concept (in-place mutation, no snapshots). Report
            // the data-page count as `private_pages` so the operator
            // still sees a "how big is this shard" gauge; the other
            // BTree-specific dials stay zero.
            rc_private_pages += shard.rc.allocated_data_pages();
            rc_pending_deltas += shard.rc.pending_delta_count();
        }
        PendingState {
            dispatch_pending,
            deferred_free,
            dedup_lane_queue,
            l2p_apply_queue,
            l2p_private_pages,
            l2p_retired_pages,
            l2p_pagebuf_total,
            l2p_pagebuf_dirty,
            rc_apply_queue,
            rc_private_pages,
            rc_retired_pages,
            rc_pagebuf_total,
            rc_pagebuf_dirty,
            rc_pending_deltas,
        }
    }

    /// Estimated total dirty work the next flush sample would have
    /// to drain: L2P dirty page buffer + in-memory RC deltas. Used
    /// by the watermark thread to decide whether to trigger an early
    /// checkpoint ahead of the periodic 1s tick, capping single-flush
    /// sample/IO cost.
    pub fn dirty_pages_estimate(&self) -> usize {
        let pending = self.pending_state();
        pending
            .l2p_pagebuf_dirty
            .saturating_add(pending.rc_pending_deltas)
    }

    pub fn metrics_json(&self) -> String {
        let cache = self.cache_stats();
        let metrics = self.metrics_snapshot();
        let pending = self.pending_state();
        let dedup_index = self.dedup_lsm_stats();
        format!(
            concat!(
                "{{",
                "\"last_applied_lsn\":{},",
                "\"high_water\":{},",
                "\"free_list\":{},",
                "\"dedup_index\":{{",
                "\"levels\":{},",
                "\"ssts\":{},",
                "\"records\":{},",
                "\"active_entries\":{},",
                "\"frozen_entries\":{}",
                "}},",
                "\"cache\":{{",
                "\"hits\":{},",
                "\"misses\":{},",
                "\"evictions\":{},",
                "\"current_pages\":{},",
                "\"current_bytes\":{},",
                "\"capacity_bytes\":{},",
                "\"pinned_pages\":{},",
                "\"pinned_bytes\":{},",
                "\"pin_budget_bytes\":{}",
                "}},",
                "\"pending\":{{",
                "\"dispatch\":{},",
                "\"deferred_free\":{},",
                "\"dedup_lane_queue\":{},",
                "\"l2p_apply_queue\":{},",
                "\"l2p_private_pages\":{},",
                "\"l2p_retired_pages\":{},",
                "\"l2p_pagebuf_total\":{},",
                "\"l2p_pagebuf_dirty\":{},",
                "\"rc_apply_queue\":{},",
                "\"rc_private_pages\":{},",
                "\"rc_retired_pages\":{},",
                "\"rc_pagebuf_total\":{},",
                "\"rc_pagebuf_dirty\":{}",
                "}},",
                "\"meta\":{}",
                "}}"
            ),
            self.last_applied_lsn(),
            self.high_water(),
            self.free_list_len(),
            dedup_index.level_count,
            dedup_index.total_ssts,
            dedup_index.total_records,
            dedup_index.memtable.active_entries,
            dedup_index.memtable.frozen_entries,
            cache.hits,
            cache.misses,
            cache.evictions,
            cache.current_pages,
            cache.current_bytes,
            cache.capacity_bytes,
            cache.pinned_pages,
            cache.pinned_bytes,
            cache.pin_budget_bytes,
            pending.dispatch_pending,
            pending.deferred_free,
            pending.dedup_lane_queue,
            pending.l2p_apply_queue,
            pending.l2p_private_pages,
            pending.l2p_retired_pages,
            pending.l2p_pagebuf_total,
            pending.l2p_pagebuf_dirty,
            pending.rc_apply_queue,
            pending.rc_private_pages,
            pending.rc_retired_pages,
            pending.rc_pagebuf_total,
            pending.rc_pagebuf_dirty,
            metrics.to_json(),
        )
    }

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
    fn select_shards_for_flush(
        &self,
        volumes: &[Arc<Volume>],
        force_all: bool,
    ) -> SelectedShards {
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

    fn flush_with_gate(&self, kind: crate::metrics::FlushKind) -> Result<bool> {
        // Exclude every in-flight apply phase only while sampling the
        // checkpoint boundary. Each tree protects the private pages in
        // the sampled roots before we drop its shard lock; later commits
        // COW away from those pages, so dirty page IO can run without
        // holding either the global gate or every shard lock.
        //
        // `kind` separates the steady-state `try_flush()` cadence from
        // forced `flush()` (shutdown drain, explicit force_checkpoint)
        // in the metrics — `flush_sample_max_us_steady` excludes the
        // shutdown blast that otherwise dominates the aggregate max.
        let blocking_gate = matches!(kind, crate::metrics::FlushKind::Forced);
        self.metrics.record_flush_attempt(kind);
        let flush_started = std::time::Instant::now();
        let gate_started = std::time::Instant::now();
        let Some(apply_guard) = (if blocking_gate {
            Some(self.apply_gate.write())
        } else {
            self.apply_gate.try_write()
        }) else {
            self.metrics.record_flush_gate_wait(gate_started.elapsed());
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            return Ok(false);
        };
        self.metrics.record_flush_gate_wait(gate_started.elapsed());

        // B2: force-compact all L2P buffers so the sample phase
        // observes a tree that reflects every committed LSN up to
        // `last_applied_lsn`. After this call, each shard's
        // `buffer.compacted_lsn` matches its tree's last applied
        // generation, so `compute_min_last_flushed_lsn_after` can
        // safely use `wal_checkpoint` as a per-shard projected LSN
        // without underestimating durability. Holding
        // `apply_gate.write()` above ensures no concurrent commits
        // can re-populate the buffer between this call and the
        // sample step. No-op when `l2p_buffer_enabled = false`.
        self.force_compact_l2p_buffers()?;

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
        let sample_started = std::time::Instant::now();
        let volumes = self.volumes_snapshot();
        // Decide which shards this round samples. Forced flushes
        // (`flush()`, snapshot, shutdown) always select everything;
        // steady-state `try_flush()` honours the budget cap.
        let selected = self.select_shards_for_flush(
            &volumes,
            matches!(kind, crate::metrics::FlushKind::Forced),
        );
        // Drain per-volume dead-list buffers while still holding
        // `apply_gate.write()` — late drainers would race new apply
        // ops pushing into the same buffer. The drained records are
        // either flushed to a new segment below (then committed via
        // the manifest tail/head atomics post-sync) or restored to
        // the front of the buffer if any subsequent step fails.
        let mut drained_deadlists: Vec<DeadListDrainEntry> = Vec::new();
        for vol in &volumes {
            let records = vol.dead_list.drain();
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
            // before locking any shards — release the gate and
            // record an empty sample. Caller will retry next tick.
            drop(apply_guard);
            self.metrics
                .record_flush_sample(kind, sample_started.elapsed());
            self.metrics
                .record_flush_total(kind, flush_started.elapsed());
            return Ok(true);
        }
        let lock_started = std::time::Instant::now();
        let mut l2p_guards = lock_selected_l2p_shards_for(&volumes, &selected.l2p);
        let lock_elapsed = lock_started.elapsed();
        let tree_generation = max_generation_from_two_groups(&l2p_guards, &self.refcount_shards);
        let wal_checkpoint = *self.last_applied_lsn.lock();
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
                    let mut guard = guard_iter
                        .next()
                        .expect("lock_selected_l2p_shards_for must hand out one guard per selected shard");
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
        // No disk IO under the gate. Meta-chain rewrite + page writes
        // happen in the IO phase below; install runs post-manifest.
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
        drop(apply_guard);
        self.metrics
            .record_flush_sample(kind, sample_started.elapsed());
        self.metrics.record_flush_sample_breakdown(
            lock_elapsed,
            l2p_walk_elapsed,
            rc_drain_elapsed,
        );
        // Sample workload size: L2P dirty pages snapshotted, refcount
        // delta entries drained, fresh refcount data pages allocated.
        // Recorded after gate release so the cost of these accessors
        // doesn't extend the gate hold time. Lets dashboards correlate
        // sample wall-time growth with workload-size growth.
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
                                self.abort_rc_checkpoints_sparse(refcount_checkpoints, wal_checkpoint);
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
            self.rollback_dead_list_drain(
                &mut drained_deadlists,
                &dead_list_plans,
                wal_checkpoint,
            );
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
            self.rollback_dead_list_drain(
                &mut drained_deadlists,
                &dead_list_plans,
                wal_checkpoint,
            );
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
            self.rollback_dead_list_drain(
                &mut drained_deadlists,
                &dead_list_plans,
                wal_checkpoint,
            );
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
            self.rollback_dead_list_drain(
                &mut drained_deadlists,
                &dead_list_plans,
                wal_checkpoint,
            );
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
            self.rollback_dead_list_drain(
                &mut drained_deadlists,
                &dead_list_plans,
                wal_checkpoint,
            );
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
            self.rollback_dead_list_drain(
                &mut drained_deadlists,
                &dead_list_plans,
                wal_checkpoint,
            );
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
        let new_checkpoint_lsn = self.compute_min_last_flushed_lsn_after(
            &volumes,
            &selected,
            wal_checkpoint,
        );
        manifest_state.manifest.checkpoint_lsn = new_checkpoint_lsn;
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
            self.rollback_dead_list_drain(
                &mut drained_deadlists,
                &dead_list_plans,
                wal_checkpoint,
            );
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
            self.rollback_dead_list_drain(
                &mut drained_deadlists,
                &dead_list_plans,
                wal_checkpoint,
            );
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
        {
            use std::sync::atomic::Ordering;
            for (v_idx, vol) in volumes.iter().enumerate() {
                for (s_idx, shard) in vol.shards.iter().enumerate() {
                    if selected.l2p[v_idx][s_idx] {
                        shard
                            .last_flushed_lsn
                            .store(wal_checkpoint, Ordering::Release);
                    }
                }
            }
            for (s_idx, shard) in self.refcount_shards.iter().enumerate() {
                if selected.rc[s_idx] {
                    shard
                        .last_flushed_lsn
                        .store(wal_checkpoint, Ordering::Release);
                }
            }
        }
        {
            let mut unlogged = self.unlogged_pending_lsn.lock();
            if unlogged.is_some_and(|lsn| lsn <= wal_checkpoint) {
                *unlogged = None;
            }
        }

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
            // WAL prune is fast (file removal) and depends on
            // `wal_checkpoint`, which this flush just made
            // durable via the manifest commit. Keep it inline so
            // recovery's replay-from-`checkpoint_lsn` doesn't
            // chase stale segments.
            crate::wal::set::prune_all_segments(&wal_dir(&self.db_path), wal_checkpoint)?;
        } else {
            let reclaim_budget = flush_reclaim_budget(deferred_before, total_pages_written);
            let reclaim_outcome = self.reclaim_freed_pages_budget(reclaim_budget)?;
            crate::wal::set::prune_all_segments(&wal_dir(&self.db_path), wal_checkpoint)?;
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
        Ok(true)
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
                let candidate = if selected.l2p[v_idx][s_idx] {
                    wal_checkpoint
                } else {
                    shard.last_flushed_lsn.load(Ordering::Acquire)
                };
                if candidate < min_lsn {
                    min_lsn = candidate;
                }
                // B2 buffer-compaction term: any uncompacted entry
                // in this shard's buffer represents a committed LSN
                // not yet durable in the tree. Crash recovery will
                // rebuild it from WAL, so `checkpoint_lsn` must not
                // advance past `buffer.compacted_lsn`. When
                // `flush_with_gate` force-compacts at the top, this
                // term equals `last_applied_lsn` and doesn't bind;
                // it's the safety net for the (future) path where
                // force-compact is skipped.
                if shard.use_buffer {
                    let buf_lsn = shard.l2p_buffer.compacted_lsn();
                    if buf_lsn < min_lsn {
                        min_lsn = buf_lsn;
                    }
                }
            }
        }
        for (s_idx, shard) in self.refcount_shards.iter().enumerate() {
            let candidate = if selected.rc[s_idx] {
                wal_checkpoint
            } else {
                shard.last_flushed_lsn.load(Ordering::Acquire)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flush_reclaim_budget_scales_with_writes_and_backlog() {
        assert_eq!(flush_reclaim_budget(0, 0), FLUSH_RECLAIM_MIN_BUDGET_PAGES);
        assert_eq!(
            flush_reclaim_budget(0, FLUSH_RECLAIM_MIN_BUDGET_PAGES),
            FLUSH_RECLAIM_MIN_BUDGET_PAGES * 8
        );
        assert_eq!(
            flush_reclaim_budget(8 * 1_048_576, 1),
            FLUSH_RECLAIM_MAX_BUDGET_PAGES / 2
        );
        assert_eq!(
            flush_reclaim_budget(FLUSH_RECLAIM_BACKLOG_HARD_CAP_PAGES, 1),
            FLUSH_RECLAIM_MAX_BUDGET_PAGES
        );
    }
}

fn refresh_manifest_from_checkpoints(
    manifest: &mut Manifest,
    volumes: &[Arc<Volume>],
    l2p_checkpoints: &[Vec<Option<crate::paged::tree::Checkpoint>>],
    dead_list_overrides: &HashMap<VolumeOrdinal, (PageId, PageId)>,
) -> Result<()> {
    manifest.body_version = MANIFEST_BODY_VERSION;
    if volumes.len() != l2p_checkpoints.len() {
        return Err(MetaDbError::Corruption(format!(
            "checkpoint volume count {} does not match checkpoint groups {}",
            volumes.len(),
            l2p_checkpoints.len()
        )));
    }
    // Snapshot the prior root for every (vol_ord, shard) so we can
    // fall back on it for unselected shards. Vec lookup is O(volumes)
    // but the live volume count is small (≤ max_volumes).
    let mut new_entries = Vec::with_capacity(volumes.len());
    for (volume, checkpoints) in volumes.iter().zip(l2p_checkpoints.iter()) {
        if volume.shards.len() != checkpoints.len() {
            return Err(MetaDbError::Corruption(format!(
                "checkpoint shard count {} does not match volume {} shard count {}",
                checkpoints.len(),
                volume.ord,
                volume.shards.len()
            )));
        }
        // The previous manifest already records this volume's roots
        // (volume create / clone pushed an entry before any flush
        // committed). Borrow that prior root slice for unselected
        // shards so the manifest reflects "this shard wasn't
        // re-flushed this round". A volume that's brand-new and
        // genuinely missing from the prior manifest falls back to
        // the in-memory tree root (must be readable while holding
        // `apply_gate.write()`).
        let prior_roots: Option<&[PageId]> = manifest
            .volumes
            .iter()
            .find(|e| e.ord == volume.ord)
            .map(|e| e.l2p_shard_roots.as_ref());
        let mut roots = Vec::with_capacity(volume.shards.len());
        for (s_idx, ck_opt) in checkpoints.iter().enumerate() {
            let root = match ck_opt {
                Some(ck) => ck.root,
                None => prior_roots
                    .and_then(|pr| pr.get(s_idx).copied())
                    .unwrap_or_else(|| {
                        volume.shards[s_idx]
                            .tree
                            .try_read()
                            .map(|t| t.root())
                            .unwrap_or(crate::types::NULL_PAGE)
                    }),
            };
            roots.push(root);
        }
        // Preserve the prior per-shard durable_seq for the moment;
        // `refresh_manifest_durable_seq` below overwrites each entry
        // with the post-flush per-shard value (selected →
        // wal_checkpoint, unselected → prior atomic). We start from
        // the prior manifest's array (or zeros if missing) so
        // unselected shards have a sensible value even if the live
        // atomics haven't been advanced yet for this round.
        let prior_durable_seq: Option<&[Lsn]> = manifest
            .volumes
            .iter()
            .find(|e| e.ord == volume.ord)
            .map(|e| e.l2p_shard_durable_seq.as_ref());
        let durable_seq: Box<[Lsn]> = match prior_durable_seq {
            Some(seqs) if seqs.len() == volume.shards.len() => seqs.to_vec().into_boxed_slice(),
            _ => vec![0; volume.shards.len()].into_boxed_slice(),
        };
        let (dead_list_head_pid, dead_list_tail_pid) =
            match dead_list_overrides.get(&volume.ord) {
                Some((h, t)) => (*h, *t),
                None => (
                    volume
                        .dead_list_head_pid
                        .load(std::sync::atomic::Ordering::Acquire),
                    volume
                        .dead_list_tail_pid
                        .load(std::sync::atomic::Ordering::Acquire),
                ),
            };
        new_entries.push(VolumeEntry {
            ord: volume.ord,
            shard_count: volume.shards.len() as u32,
            l2p_shard_roots: roots.into_boxed_slice(),
            l2p_shard_durable_seq: durable_seq,
            created_lsn: volume.created_lsn,
            flags: volume.flags.load(std::sync::atomic::Ordering::Relaxed),
            dead_list_head_pid,
            dead_list_tail_pid,
        });
    }
    manifest.volumes = new_entries;
    // refcount_shard_roots are stamped at create/open time and never
    // change across flushes (paged-array meta page id is stable);
    // leave whatever the manifest already carries untouched.
    Ok(())
}

/// Tier 2.B Stage 1: rewrite every per-shard `durable_seq` in the
/// manifest to reflect the durable state we're about to commit. This
/// mirrors the inputs to [`Db::compute_min_last_flushed_lsn_after`]:
/// selected shards advance to `wal_checkpoint`; unselected shards keep
/// their existing atomic. Must be called AFTER
/// [`refresh_manifest_from_checkpoints`] so the per-volume entries are
/// already shaped correctly, and BEFORE the manifest store commit so
/// the new arrays land on disk.
///
/// Stage 1 invariant: `checkpoint_lsn == min(all durable_seq[])`. The
/// `Manifest::assert_durable_seq_invariant` tripwire fires on the next
/// `encode` if this is violated.
fn refresh_manifest_durable_seq(
    manifest: &mut Manifest,
    volumes: &[Arc<Volume>],
    refcount_shards: &[Shard],
    selected: &SelectedShards,
    wal_checkpoint: Lsn,
) -> Result<()> {
    use std::sync::atomic::Ordering;
    if manifest.volumes.len() != volumes.len() {
        return Err(MetaDbError::Corruption(format!(
            "refresh_manifest_durable_seq: manifest volume count {} != live count {}",
            manifest.volumes.len(),
            volumes.len(),
        )));
    }
    for (v_idx, vol) in volumes.iter().enumerate() {
        let entry = &mut manifest.volumes[v_idx];
        if entry.ord != vol.ord {
            return Err(MetaDbError::Corruption(format!(
                "refresh_manifest_durable_seq: manifest volume[{v_idx}] ord {} != live ord {}",
                entry.ord, vol.ord,
            )));
        }
        if entry.l2p_shard_durable_seq.len() != vol.shards.len() {
            entry.l2p_shard_durable_seq = vec![0; vol.shards.len()].into_boxed_slice();
        }
        for (s_idx, shard) in vol.shards.iter().enumerate() {
            let tree_lsn = if selected.l2p[v_idx][s_idx] {
                wal_checkpoint
            } else {
                shard.last_flushed_lsn.load(Ordering::Acquire)
            };
            // Same B2 buffer term as `compute_min_last_flushed_lsn_after`:
            // any uncompacted buffer entry represents committed-but-
            // not-tree-durable state; WAL replay rebuilds it, so the
            // per-shard durable_seq must not advance past
            // `buffer.compacted_lsn`. `flush_with_gate` force-compacts
            // before this runs so the term equals `last_applied_lsn`
            // in normal flushes; it's the safety net for paths that
            // skip force-compact.
            let lsn = if shard.use_buffer {
                tree_lsn.min(shard.l2p_buffer.compacted_lsn())
            } else {
                tree_lsn
            };
            entry.l2p_shard_durable_seq[s_idx] = lsn;
        }
    }
    if manifest.refcount_durable_seq.len() != refcount_shards.len() {
        manifest.refcount_durable_seq =
            vec![0; refcount_shards.len()].into_boxed_slice();
    }
    for (s_idx, shard) in refcount_shards.iter().enumerate() {
        let lsn = if selected.rc[s_idx] {
            wal_checkpoint
        } else {
            shard.last_flushed_lsn.load(Ordering::Acquire)
        };
        manifest.refcount_durable_seq[s_idx] = lsn;
    }
    Ok(())
}
