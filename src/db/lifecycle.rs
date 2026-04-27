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
        let dedup_index =
            Lsm::create_with_cache(page_store.clone(), page_cache.clone(), lsm_config.clone());
        let dedup_reverse =
            Lsm::create_with_cache(page_store.clone(), page_cache.clone(), lsm_config);
        manifest.body_version = MANIFEST_BODY_VERSION;
        manifest.refcount_shard_roots = refcount_roots;
        manifest.dedup_level_heads = Vec::new().into_boxed_slice();
        manifest.dedup_reverse_level_heads = Vec::new().into_boxed_slice();
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

        let wal = Wal::create_with_metrics(
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
            wal,
            apply_gate: RwLock::new(()),
            last_applied_lsn: Mutex::new(0),
            commit_cvar: Condvar::new(),
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
        let page_store = Arc::new(PageStore::open_with_grow_chunk(
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
        let dedup_index = Lsm::open_with_cache(
            page_store.clone(),
            page_cache.clone(),
            lsm_config.clone(),
            &manifest.dedup_level_heads,
        )?;
        let dedup_reverse = Lsm::open_with_cache(
            page_store.clone(),
            page_cache.clone(),
            lsm_config,
            &manifest.dedup_reverse_level_heads,
        )?;

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
        let replay_outcome = crate::recovery::replay_into(&wal_path, from_lsn, |lsn, op| {
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
                    Ok(ApplyOutcome::Dedup)
                }
                WalOp::DropVolume { ord, pages } => {
                    if volumes.contains_key(ord) {
                        apply_drop_volume(&page_store, lsn, pages)?;
                        volumes.remove(ord);
                        manifest.volumes.retain(|v| v.ord != *ord);
                        mutated_volumes = true;
                    }
                    Ok(ApplyOutcome::Dedup)
                }
                WalOp::CloneVolume {
                    src_ord: _,
                    new_ord,
                    src_snap_id: _,
                    src_shard_roots,
                } => {
                    if !volumes.contains_key(new_ord) {
                        apply_clone_volume_incref(&page_store, &faults, lsn, src_shard_roots)?;
                        // Same stale-buffer hazard as `Db::clone_volume`:
                        // every volume whose PagedL2p was opened above
                        // may hold a pre-incref Clean copy of one of
                        // these roots — not just the source. Sweep all
                        // volumes so a later `incref_root_for_snapshot`
                        // or `cow_for_write` during replay can't flush a
                        // stale rc back over our disk-direct bump.
                        let all_vols: Vec<Arc<Volume>> = volumes.values().cloned().collect();
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
                        volumes.insert(*new_ord, Arc::new(Volume::new(*new_ord, shards, lsn)));
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
                    Ok(ApplyOutcome::Dedup)
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
                    let outcome = apply_op_bare(
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
                    Ok(outcome)
                }
            }
        })?;
        let last_applied = replay_outcome.last_lsn.unwrap_or(manifest.checkpoint_lsn);
        // If the last segment ended torn, truncate it to the last clean
        // record before handing the directory to the new Wal.
        crate::recovery::truncate_torn_tail(&wal_path, &replay_outcome)?;
        let wal = Wal::create_with_metrics(
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
        let replayed_anything = replay_outcome.last_lsn.is_some();
        if replayed_anything || replayed_drop || mutated_volumes {
            let sorted: Vec<Arc<Volume>> = {
                let mut v: Vec<Arc<Volume>> = volumes.values().cloned().collect();
                v.sort_by_key(|vol| vol.ord);
                v
            };
            let mut l2p_guards = lock_all_l2p_shards_for(&sorted);
            let mut refcount_guards: Vec<MutexGuard<'_, BTree>> =
                refcount_shards.iter().map(|s| s.tree.lock()).collect();
            flush_locked_l2p_shards(&mut l2p_guards)?;
            for tree in refcount_guards.iter_mut() {
                tree.flush()?;
            }

            // Dedup memtable / level heads: mirror what
            // `Db::flush` does via `prepare_dedup_manifest_update`.
            let dedup_generation = last_applied.max(1) + 1;
            dedup_index.flush_memtable(dedup_generation)?;
            dedup_reverse.flush_memtable(dedup_generation)?;
            let old_dedup_heads = manifest.dedup_level_heads.to_vec();
            let old_dedup_reverse_heads = manifest.dedup_reverse_level_heads.to_vec();
            manifest.dedup_level_heads = dedup_index
                .persist_levels(dedup_generation)?
                .into_boxed_slice();
            manifest.dedup_reverse_level_heads = dedup_reverse
                .persist_levels(dedup_generation)?
                .into_boxed_slice();

            refresh_manifest_entries(&mut manifest, &sorted, &l2p_guards, &refcount_guards)?;
            manifest.checkpoint_lsn = last_applied;
            manifest_store.commit(&manifest)?;
            commit_l2p_checkpoint(&mut l2p_guards, last_applied.max(1) + 1)?;
            commit_refcount_checkpoint(&mut refcount_guards, last_applied.max(1) + 1)?;

            dedup_index.free_old_level_heads(&old_dedup_heads, dedup_generation)?;
            dedup_reverse.free_old_level_heads(&old_dedup_reverse_heads, dedup_generation)?;

            // Open-path counterpart of `Db::reclaim_freed_pages`: no
            // readers can be pinned yet, so every deferred entry queued
            // by the checkpoint above is reclaimable in one pass. Done
            // before `verify::reclaim_orphan_pages` so the orphan walk
            // sees the post-replay free list as the live state.
            page_store.try_reclaim()?;
        }

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
            wal,
            apply_gate: RwLock::new(()),
            last_applied_lsn: Mutex::new(last_applied),
            commit_cvar: Condvar::new(),
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

    /// Snapshot shared page-cache counters.
    pub fn cache_stats(&self) -> PageCacheStats {
        self.page_cache.stats()
    }

    pub fn metrics_snapshot(&self) -> MetaMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn metrics_json(&self) -> String {
        let cache = self.cache_stats();
        let metrics = self.metrics_snapshot();
        format!(
            concat!(
                "{{",
                "\"last_applied_lsn\":{},",
                "\"high_water\":{},",
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
                "\"meta\":{}",
                "}}"
            ),
            self.last_applied_lsn(),
            self.high_water(),
            cache.hits,
            cache.misses,
            cache.evictions,
            cache.current_pages,
            cache.current_bytes,
            cache.capacity_bytes,
            cache.pinned_pages,
            cache.pinned_bytes,
            cache.pin_budget_bytes,
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
        // Exclude every in-flight apply phase: after `apply_gate.write()`
        // returns, no commit is between "touched a tree" and "bumped
        // last_applied_lsn", so the LSN we sample below matches exactly
        // the state the trees will have when we flush them.
        let _apply_guard = self.apply_gate.write();
        let mut manifest_state = self.manifest_state.lock();
        let volumes = self.volumes_snapshot();
        let mut l2p_guards = lock_all_l2p_shards_for(&volumes);
        let mut refcount_guards = self.lock_all_refcount_shards();
        flush_locked_l2p_shards(&mut l2p_guards)?;
        self.flush_locked_refcount_shards(&mut refcount_guards)?;

        let tree_generation = max_generation_from_two_groups(&l2p_guards, &refcount_guards);
        let wal_checkpoint = *self.last_applied_lsn.lock();
        let dedup_update =
            self.prepare_dedup_manifest_update(&mut manifest_state.manifest, tree_generation)?;
        self.faults
            .inject(FaultPoint::FlushPostLevelRewriteBeforeManifest)?;

        self.refresh_manifest_from_locked(
            &mut manifest_state.manifest,
            &volumes,
            &l2p_guards,
            &refcount_guards,
        )?;
        // The tree generation is a local monotonic counter; checkpoint
        // LSN must be the durable WAL LSN, not the tree counter.
        manifest_state.manifest.checkpoint_lsn = wal_checkpoint;
        let manifest = manifest_state.manifest.clone();
        manifest_state.store.commit(&manifest)?;
        commit_l2p_checkpoint(&mut l2p_guards, tree_generation)?;
        commit_refcount_checkpoint(&mut refcount_guards, tree_generation)?;
        self.finish_dedup_manifest_update(dedup_update, tree_generation)?;
        // Drop locks before reclaiming so concurrent readers (which
        // hold no apply guard in the epoch design) can keep walking.
        drop(l2p_guards);
        drop(refcount_guards);
        drop(manifest_state);
        drop(_apply_guard);
        self.reclaim_freed_pages()?;
        crate::wal::prune_segments(&wal_dir(&self.db_path), wal_checkpoint)?;
        Ok(())
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
        for pid in reclaimed {
            self.page_cache.invalidate(pid);
        }
        Ok(())
    }
}
