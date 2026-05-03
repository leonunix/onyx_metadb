use super::*;

const FLUSH_RECLAIM_MIN_BUDGET_PAGES: usize = 1024;
const FLUSH_RECLAIM_MAX_BUDGET_PAGES: usize = 16_384;
const FLUSH_INSTALL_PAGE_BUDGET: usize = 64;
const FLUSH_INSTALL_CLEANUP_BUDGET: usize = 64;
const FLUSH_INSTALL_STEP_WARN_US: u64 = 100_000;

fn micros(duration: std::time::Duration) -> u64 {
    duration.as_micros().min(u128::from(u64::MAX)) as u64
}

fn flush_reclaim_budget(pending_reclaim_pages: usize, pages_written: usize) -> usize {
    let write_scaled = pages_written.saturating_mul(2);
    let backlog_scaled = pending_reclaim_pages / 4;
    FLUSH_RECLAIM_MIN_BUDGET_PAGES
        .max(write_scaled)
        .max(backlog_scaled)
        .min(FLUSH_RECLAIM_MAX_BUDGET_PAGES)
}

struct CheckpointInstallReceiver {
    kind: &'static str,
    vol_ord: Option<VolumeOrdinal>,
    shard: usize,
    rx: crossbeam_channel::Receiver<Result<Vec<PageId>>>,
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

// `enqueue_refcount_checkpoint_install_step` / `run_refcount_checkpoint_install_step`
// were the deferred-flush install drivers for the old BTree-backed
// refcount path. The paged-array refcount writes synchronously during
// sample-phase `RcShard::flush` and has no install step, so the
// drivers are dead code as of Stage 1 of metadb-restructure-v9.

#[allow(dead_code)]
fn run_refcount_checkpoint_install_step(
    sid: usize,
    tree: Arc<Mutex<crate::btree::BTree>>,
    state: Arc<Mutex<CheckpointInstallState<crate::btree::cache::FlushedSnapshot>>>,
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
    let mut tree = tree.lock();
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
            kind = "refcount",
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
        let page_store = Arc::new(PageStore::create_with_grow_chunk(
            &pages_path,
            cfg.page_grow_chunk_pages,
        )?);
        let page_cache = Arc::new(PageCache::new_with_pin_budget(
            page_store.clone(),
            cfg.page_cache_bytes,
            cfg.index_pin_bytes,
        ));
        let metrics = Arc::new(MetaMetrics::new());
        let lsm_config = lsm_config_from_cfg(&cfg);
        let (mut manifest_store, mut manifest) =
            ManifestStore::open_or_create(page_store.clone(), faults.clone())?;
        let (l2p_shards, l2p_roots) =
            create_l2p_shards(page_store.clone(), page_cache.clone(), shard_count)?;
        let (refcount_shards, refcount_roots) =
            create_shards(page_store.clone(), page_cache.clone(), shard_count)?;
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
        let dedup_reverse =
            Arc::new(crate::paged_reverse::PagedReverse::create(page_store.clone(), page_cache.clone())?);
        manifest.body_version = MANIFEST_BODY_VERSION;
        manifest.refcount_shard_roots = refcount_roots;
        manifest.dedup_shards = dedup_shards;
        // dedup_index: cuckoo meta page id stored under the legacy
        // `dedup_index_shard_heads` slot, single-element box for
        // compat with the existing decoder (same pattern as
        // dedup_reverse below).
        manifest.dedup_index_shard_heads =
            vec![vec![dedup_index.meta_page_id()].into_boxed_slice()].into_boxed_slice();
        // dedup_reverse: paged-array stores its meta page id under the
        // legacy field name, single-element box for compat with
        // existing decode logic.
        manifest.dedup_reverse_shard_heads =
            vec![vec![dedup_reverse.meta_page_id()].into_boxed_slice()].into_boxed_slice();
        // Seed the bootstrap volume so open() / flush() can route
        // through the same volumes table the live `Db` manages.
        manifest.volumes = vec![VolumeEntry {
            ord: BOOTSTRAP_VOLUME_ORD,
            shard_count: l2p_roots.len() as u32,
            l2p_shard_roots: l2p_roots,
            created_lsn: 0,
            flags: 0,
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

        Ok(Self {
            page_store,
            page_cache,
            metrics,
            manifest_state: Mutex::new(ManifestState {
                store: manifest_store,
                manifest,
            }),
            volumes: RwLock::new(volumes),
            refcount_shards,
            dedup_index,
            dedup_reverse,
            dedup_lanes: build_dedup_lanes(0, dedup_shards as usize, ApplyLaneKind::Dedup),
            dedup_maintenance_lanes: build_dedup_lanes(
                0,
                dedup_shards as usize,
                ApplyLaneKind::DedupMaintenance,
            ),
            dedup_maintenance_queued: build_dedup_queued_flags(dedup_shards as usize),
            wal,
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
        })
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
            PageStore::open_with_grow_chunk(&pages_path, cfg.page_grow_chunk_pages)?
        } else {
            PageStore::open_fast_with_grow_chunk(&pages_path, cfg.page_grow_chunk_pages)?
        });
        let page_cache = Arc::new(PageCache::new_with_pin_budget(
            page_store.clone(),
            cfg.page_cache_bytes,
            cfg.index_pin_bytes,
        ));
        let metrics = Arc::new(MetaMetrics::new());
        let lsm_config = lsm_config_from_cfg(&cfg);
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
        let mut volumes: HashMap<VolumeOrdinal, Arc<Volume>> =
            HashMap::with_capacity(manifest.volumes.len());
        for entry in &manifest.volumes {
            let shards = open_l2p_shards(
                page_store.clone(),
                page_cache.clone(),
                &entry.l2p_shard_roots,
                next_gen,
            )?;
            volumes.insert(
                entry.ord,
                Arc::new(Volume::new(entry.ord, shards, entry.created_lsn)),
            );
        }
        let refcount_shards = open_shards(
            page_store.clone(),
            page_cache.clone(),
            &manifest.refcount_shard_roots,
            next_gen,
        )?;
        let dedup_index_meta_pid: PageId = manifest
            .dedup_index_shard_heads
            .first()
            .and_then(|s| s.first().copied())
            .unwrap_or(0);
        let dedup_reverse_meta_pid: PageId = manifest
            .dedup_reverse_shard_heads
            .first()
            .and_then(|s| s.first().copied())
            .unwrap_or(0);
        let dedup_index = Arc::new(crate::dedup::DedupIndex::open(
            page_store.clone(),
            page_cache.clone(),
            dedup_index_meta_pid,
            cfg.dedup_l1_cache_entries,
        )?);
        let dedup_reverse = Arc::new(crate::paged_reverse::PagedReverse::open(
            page_store.clone(),
            page_cache.clone(),
            dedup_reverse_meta_pid,
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
                        &dedup_reverse,
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
                                let (shards, roots) =
                                    apply_create_volume(&page_store, &page_cache, *shard_count)?;
                                volumes.insert(*ord, Arc::new(Volume::new(*ord, shards, lsn)));
                                manifest.volumes.push(VolumeEntry {
                                    ord: *ord,
                                    shard_count: *shard_count,
                                    l2p_shard_roots: roots,
                                    created_lsn: lsn,
                                    flags: 0,
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
                                )?;
                                let shard_count = shards.len() as u32;
                                volumes
                                    .insert(*new_ord, Arc::new(Volume::new(*new_ord, shards, lsn)));
                                manifest.volumes.push(VolumeEntry {
                                    ord: *new_ord,
                                    shard_count,
                                    l2p_shard_roots: actual_roots,
                                    created_lsn: lsn,
                                    flags: 0,
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
                                &dedup_reverse,
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
            dedup_reverse.flush_meta()?;
            manifest.dedup_index_shard_heads =
                vec![vec![dedup_index.meta_page_id()].into_boxed_slice()].into_boxed_slice();
            manifest.dedup_reverse_shard_heads =
                vec![vec![dedup_reverse.meta_page_id()].into_boxed_slice()].into_boxed_slice();

            refresh_manifest_entries(&mut manifest, &sorted, &l2p_guards, &refcount_shards)?;
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

        let db = Self {
            page_store,
            page_cache,
            metrics,
            manifest_state: Mutex::new(ManifestState {
                store: manifest_store,
                manifest,
            }),
            volumes: RwLock::new(volumes),
            refcount_shards,
            dedup_index,
            dedup_reverse,
            dedup_lanes: build_dedup_lanes(
                last_applied,
                manifest_dedup_shards,
                ApplyLaneKind::Dedup,
            ),
            dedup_maintenance_lanes: build_dedup_lanes(
                last_applied,
                manifest_dedup_shards,
                ApplyLaneKind::DedupMaintenance,
            ),
            dedup_maintenance_queued: build_dedup_queued_flags(manifest_dedup_shards),
            wal,
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
        };
        db.recompute_all_snap_infos();
        Ok(db)
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

    pub fn dedup_lsm_stats(&self) -> (LsmStats, LsmStats) {
        // Both dedup index and dedup reverse moved off LSM in v9;
        // report zeroed stats so the status formatter keeps its
        // `(forward, reverse)` shape until the operator-facing
        // surface is updated. Use `dedup_tier_sizes` for the cuckoo
        // L0/L1 occupancy.
        (LsmStats::default(), LsmStats::default())
    }

    /// Per-shard dedup stats are unavailable: the cuckoo dedup_index
    /// has no shard concept and paged-array dedup_reverse uses a
    /// single page table. Returns empty vecs.
    pub fn dedup_lsm_stats_per_shard(&self) -> (Vec<LsmStats>, Vec<LsmStats>) {
        (Vec::new(), Vec::new())
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
        let mut rc_retired_pages = 0usize;
        let mut rc_pagebuf_total = 0usize;
        let mut rc_pagebuf_dirty = 0usize;
        for shard in &self.refcount_shards {
            rc_apply_queue += shard.apply_lane.queue_len();
            // Paged-array refcount has no COW / private / retired
            // page concept (in-place mutation, no snapshots). Report
            // the data-page count as `private_pages` so the operator
            // still sees a "how big is this shard" gauge; the other
            // BTree-specific dials stay zero.
            rc_private_pages += shard.rc.allocated_data_pages();
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
        }
    }

    pub fn metrics_json(&self) -> String {
        let cache = self.cache_stats();
        let metrics = self.metrics_snapshot();
        let pending = self.pending_state();
        let (dedup_index, dedup_reverse) = self.dedup_lsm_stats();
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
                "\"dedup_reverse\":{{",
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
            dedup_reverse.level_count,
            dedup_reverse.total_ssts,
            dedup_reverse.total_records,
            dedup_reverse.memtable.active_entries,
            dedup_reverse.memtable.frozen_entries,
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
        self.flush_with_gate(true).map(|_| ())
    }

    /// Best-effort checkpoint for background maintenance. If commits are
    /// currently applying, this returns `Ok(false)` without setting the
    /// apply gate's writer-pending bit, so foreground commit readers keep
    /// flowing and the caller can retry on the next interval.
    pub fn try_flush(&self) -> Result<bool> {
        self.flush_with_gate(false)
    }

    fn flush_with_gate(&self, blocking_gate: bool) -> Result<bool> {
        // Exclude every in-flight apply phase only while sampling the
        // checkpoint boundary. Each tree protects the private pages in
        // the sampled roots before we drop its shard lock; later commits
        // COW away from those pages, so dirty page IO can run without
        // holding either the global gate or every shard lock.
        self.metrics.record_flush_attempt();
        let flush_started = std::time::Instant::now();
        let gate_started = std::time::Instant::now();
        let Some(apply_guard) = (if blocking_gate {
            Some(self.apply_gate.write())
        } else {
            self.apply_gate.try_write()
        }) else {
            self.metrics.record_flush_gate_wait(gate_started.elapsed());
            self.metrics.record_flush_total(flush_started.elapsed());
            return Ok(false);
        };
        self.metrics.record_flush_gate_wait(gate_started.elapsed());
        let sample_started = std::time::Instant::now();
        let volumes = self.volumes_snapshot();
        let mut l2p_guards = lock_all_l2p_shards_for(&volumes);
        let tree_generation = max_generation_from_two_groups(&l2p_guards, &self.refcount_shards);
        let wal_checkpoint = *self.last_applied_lsn.lock();
        let mut l2p_checkpoints = Vec::with_capacity(volumes.len());
        for volume in &volumes {
            let mut checkpoints = Vec::with_capacity(volume.shards.len());
            for _ in 0..volume.shards.len() {
                checkpoints.push(l2p_guards.remove(0).begin_checkpoint());
            }
            l2p_checkpoints.push(checkpoints);
        }
        // Paged-array refcount has no deferred-flush / checkpoint
        // model — `RcShard::flush` directly writes touched pages
        // and the meta page during commit-boundary apply. Drain any
        // delta still pending here so the manifest commit below sees
        // a settled on-disk state.
        self.flush_all_refcount_shards()?;
        let refcount_checkpoints: Vec<()> = Vec::new();
        drop(l2p_guards);
        drop(apply_guard);
        self.metrics.record_flush_sample(sample_started.elapsed());

        let io_started = std::time::Instant::now();
        let mut total_pages_written = 0usize;
        let mut sealed_pages = Vec::new();
        let mut flushed_l2p = Vec::with_capacity(l2p_checkpoints.len());
        for checkpoints in &l2p_checkpoints {
            let mut flushed = Vec::with_capacity(checkpoints.len());
            for checkpoint in checkpoints {
                match checkpoint.write_dirty_pages() {
                    Ok(pages) => {
                        total_pages_written += pages.pages_count();
                        pages.append_sealed_pages(&mut sealed_pages);
                        flushed.push(pages);
                    }
                    Err(err) => {
                        self.metrics
                            .record_flush_io(io_started.elapsed(), total_pages_written);
                        self.metrics.record_flush_total(flush_started.elapsed());
                        self.abort_checkpoints(&volumes, &l2p_checkpoints, &refcount_checkpoints);
                        return Err(err);
                    }
                }
            }
            flushed_l2p.push(flushed);
        }
        // Refcount checkpoint write_dirty_pages: paged-array has no
        // deferred dirty pages (flush already happened in sample
        // phase), so this loop is a no-op stub.
        let flushed_refcount: Vec<()> = Vec::new();
        if let Err(err) = self.page_store.write_sealed_page_runs(sealed_pages) {
            self.metrics
                .record_flush_io(io_started.elapsed(), total_pages_written);
            self.metrics.record_flush_total(flush_started.elapsed());
            self.abort_checkpoints(&volumes, &l2p_checkpoints, &refcount_checkpoints);
            return Err(err);
        }
        if let Err(err) = self.page_store.sync() {
            self.metrics
                .record_flush_io(io_started.elapsed(), total_pages_written);
            self.metrics.record_flush_total(flush_started.elapsed());
            self.abort_checkpoints(&volumes, &l2p_checkpoints, &refcount_checkpoints);
            return Err(err);
        }
        self.metrics
            .record_flush_io(io_started.elapsed(), total_pages_written);

        let manifest_started = std::time::Instant::now();
        let mut manifest_state = self.manifest_state.lock();
        let dedup_update = match self
            .prepare_dedup_manifest_update(&mut manifest_state.manifest, tree_generation)
        {
            Ok(update) => update,
            Err(err) => {
                self.metrics
                    .record_flush_manifest(manifest_started.elapsed());
                self.metrics.record_flush_total(flush_started.elapsed());
                self.abort_checkpoints(&volumes, &l2p_checkpoints, &refcount_checkpoints);
                return Err(err);
            }
        };
        if let Err(err) = self
            .faults
            .inject(FaultPoint::FlushPostLevelRewriteBeforeManifest)
        {
            self.metrics
                .record_flush_manifest(manifest_started.elapsed());
            self.metrics.record_flush_total(flush_started.elapsed());
            self.abort_checkpoints(&volumes, &l2p_checkpoints, &refcount_checkpoints);
            return Err(err);
        }

        if let Err(err) = refresh_manifest_from_checkpoints(
            &mut manifest_state.manifest,
            &volumes,
            &l2p_checkpoints,
            &refcount_checkpoints,
        ) {
            self.metrics
                .record_flush_manifest(manifest_started.elapsed());
            self.metrics.record_flush_total(flush_started.elapsed());
            self.abort_checkpoints(&volumes, &l2p_checkpoints, &refcount_checkpoints);
            return Err(err);
        }
        // The tree generation is a local monotonic counter; checkpoint
        // LSN must be the durable WAL LSN, not the tree counter.
        manifest_state.manifest.checkpoint_lsn = wal_checkpoint;
        let manifest = manifest_state.manifest.clone();
        if let Err(err) = manifest_state.store.commit(&manifest) {
            self.metrics
                .record_flush_manifest(manifest_started.elapsed());
            self.metrics.record_flush_total(flush_started.elapsed());
            self.abort_checkpoints(&volumes, &l2p_checkpoints, &refcount_checkpoints);
            return Err(err);
        }
        self.metrics
            .record_flush_manifest(manifest_started.elapsed());

        let install_started = std::time::Instant::now();
        let mut install_receivers = Vec::new();
        for (volume, (checkpoints, flushed)) in volumes
            .iter()
            .zip(l2p_checkpoints.into_iter().zip(flushed_l2p.into_iter()))
        {
            for (sid, (checkpoint, flushed)) in
                checkpoints.into_iter().zip(flushed.into_iter()).enumerate()
            {
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
        // Refcount install: paged-array writes are already on disk
        // after sample-phase flush, and meta_page_id is stable across
        // flushes so the manifest needs no per-flush update for
        // refcount roots. Nothing to enqueue here.
        let _ = flushed_refcount;
        let mut checkpoint_frees = Vec::new();
        for receiver in install_receivers {
            let recv_started = std::time::Instant::now();
            match receiver.rx.recv() {
                Ok(Ok(mut frees)) => checkpoint_frees.append(&mut frees),
                Ok(Err(err)) => {
                    drop(manifest_state);
                    self.metrics.record_flush_install(install_started.elapsed());
                    self.metrics.record_flush_total(flush_started.elapsed());
                    return Err(err);
                }
                Err(_) => {
                    drop(manifest_state);
                    self.metrics.record_flush_install(install_started.elapsed());
                    self.metrics.record_flush_total(flush_started.elapsed());
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
        let reclaim_budget =
            flush_reclaim_budget(self.page_store.deferred_free_len(), total_pages_written);
        self.reclaim_freed_pages_budget(reclaim_budget)?;
        crate::wal::set::prune_all_segments(&wal_dir(&self.db_path), wal_checkpoint)?;
        self.metrics.record_flush_reclaim(reclaim_started.elapsed());
        self.metrics.record_flush_total(flush_started.elapsed());
        Ok(true)
    }

    fn abort_checkpoints(
        &self,
        volumes: &[Arc<Volume>],
        l2p_checkpoints: &[Vec<crate::paged::tree::Checkpoint>],
        _refcount_checkpoints: &[()],
    ) {
        for (volume, checkpoints) in volumes.iter().zip(l2p_checkpoints.iter()) {
            for (shard, checkpoint) in volume.shards.iter().zip(checkpoints.iter()) {
                shard.tree.write().abort_checkpoint(checkpoint);
            }
        }
        // Paged-array refcount has nothing to abort: changes land
        // synchronously during sample-phase flush.
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
        let reclaimed = self.page_store.try_reclaim()?;
        self.invalidate_reclaimed_pages(reclaimed);
        Ok(())
    }

    pub(crate) fn reclaim_freed_pages_budget(&self, max_pages: usize) -> Result<()> {
        let reclaimed = self.page_store.try_reclaim_limit(max_pages)?;
        self.invalidate_reclaimed_pages(reclaimed);
        Ok(())
    }

    fn invalidate_reclaimed_pages(&self, reclaimed: Vec<crate::types::PageId>) {
        for pid in reclaimed {
            self.page_cache.invalidate(pid);
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
            FLUSH_RECLAIM_MIN_BUDGET_PAGES * 2
        );
        assert_eq!(
            flush_reclaim_budget(FLUSH_RECLAIM_MAX_BUDGET_PAGES * 8, 1),
            FLUSH_RECLAIM_MAX_BUDGET_PAGES
        );
    }
}

fn refresh_manifest_from_checkpoints(
    manifest: &mut Manifest,
    volumes: &[Arc<Volume>],
    l2p_checkpoints: &[Vec<crate::paged::tree::Checkpoint>],
    _refcount_checkpoints: &[()],
) -> Result<()> {
    manifest.body_version = MANIFEST_BODY_VERSION;
    if volumes.len() != l2p_checkpoints.len() {
        return Err(MetaDbError::Corruption(format!(
            "checkpoint volume count {} does not match checkpoint groups {}",
            volumes.len(),
            l2p_checkpoints.len()
        )));
    }
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
        new_entries.push(VolumeEntry {
            ord: volume.ord,
            shard_count: volume.shards.len() as u32,
            l2p_shard_roots: checkpoints
                .iter()
                .map(|checkpoint| checkpoint.root)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            created_lsn: volume.created_lsn,
            flags: volume.flags.load(std::sync::atomic::Ordering::Relaxed),
        });
    }
    manifest.volumes = new_entries;
    // refcount_shard_roots are stamped at create/open time and never
    // change across flushes (paged-array meta page id is stable);
    // leave whatever the manifest already carries untouched.
    Ok(())
}
