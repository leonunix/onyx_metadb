use super::*;

/// Lock every L2P shard for write across the given volume set, in
/// (`volumes` order, shard index) order. Callers that reach multiple
/// volumes pass the sorted output of `Db::volumes_snapshot` so every
/// caller agrees on a single lock order, preventing the shard locks
/// from deadlocking against each other. Volumes are passed in as a slice
/// of `Arc<Volume>` so the clones keep the locks alive for the guard
/// lifetime.
pub(super) fn lock_all_l2p_shards_for<'v>(
    volumes: &'v [Arc<Volume>],
) -> Vec<RwLockWriteGuard<'v, PagedL2p>> {
    volumes
        .iter()
        .flat_map(|vol| vol.shards.iter().map(|shard| shard.tree.write()))
        .collect()
}

/// Partial-sample variant of [`lock_all_l2p_shards_for`]: only
/// write-locks `(volume_idx, shard_idx)` pairs whose corresponding
/// `selected[v][s]` is `true`. Lock order is identical (volume then
/// shard index ascending) so this can interleave with full-sample
/// callers without deadlock. The returned vector is flat in
/// `(volume, shard)` ascending order, and only contains guards for
/// selected shards — callers must zip it against the same
/// `selected` matrix to map back to `(v, s)` indices.
pub(super) fn lock_selected_l2p_shards_for<'v>(
    volumes: &'v [Arc<Volume>],
    selected: &[Vec<bool>],
) -> Vec<RwLockWriteGuard<'v, PagedL2p>> {
    debug_assert_eq!(selected.len(), volumes.len());
    let mut guards = Vec::new();
    for (vol, mask) in volumes.iter().zip(selected.iter()) {
        debug_assert_eq!(mask.len(), vol.shards.len());
        for (shard_idx, shard) in vol.shards.iter().enumerate() {
            if mask[shard_idx] {
                guards.push(shard.tree.write());
            }
        }
    }
    guards
}

/// Rebuild `manifest.volumes` and `manifest.refcount_shard_roots` from
/// the live tree roots held by the supplied guard slices. Used by
/// `Db::flush` / `Db::take_snapshot` (via the `&self` wrapper) and by
/// `Db::open` post-replay to sync the on-disk manifest with in-memory
/// state before any page reclaim.
pub(super) fn refresh_manifest_entries(
    manifest: &mut Manifest,
    volumes: &[Arc<Volume>],
    l2p_guards: &[RwLockWriteGuard<'_, PagedL2p>],
    refcount_shards: &[Shard],
    l2p_page_rc: &crate::l2p_page_rc::L2pPageRc,
    durable_override: Option<Lsn>,
) -> Result<()> {
    manifest.body_version = MANIFEST_BODY_VERSION;
    let expected_total: usize = volumes.iter().map(|v| v.shards.len()).sum();
    if expected_total != l2p_guards.len() {
        return Err(MetaDbError::Corruption(format!(
            "refresh_manifest_entries: shard guard count {} does not match \
             sum of volume shard counts {expected_total}",
            l2p_guards.len(),
        )));
    }
    let mut guard_cursor = 0usize;
    let mut new_entries = Vec::with_capacity(volumes.len());
    for vol in volumes {
        let mut roots = Vec::with_capacity(vol.shards.len());
        let mut durable_seqs = Vec::with_capacity(vol.shards.len());
        for shard in vol.shards.iter() {
            roots.push(l2p_guards[guard_cursor].root());
            // Capture the per-L2P-shard durable_seq. v11 (Tier 2.B
            // Stage 1) persists this alongside `l2p_shard_roots` so
            // each shard's watermark survives crash recovery without
            // collapsing through the global `checkpoint_lsn`.
            //
            // `durable_override` is supplied by drop_volume /
            // take_snapshot / drop_snapshot, which have flushed every
            // shard inline and are about to write `manifest
            // .checkpoint_lsn = last_applied_lsn`. In that context
            // every shard IS durable up to the override LSN even
            // though the per-shard atomics aren't bumped yet (they're
            // bumped only by `flush_with_gate`'s post-commit loop).
            // Reading atomics would otherwise leave the manifest's
            // durable_seq lagging the new checkpoint_lsn, violating
            // the Stage 1 invariant.
            let seq = durable_override.unwrap_or_else(|| {
                shard
                    .last_flushed_lsn
                    .load(std::sync::atomic::Ordering::Acquire)
            });
            durable_seqs.push(seq);
            guard_cursor += 1;
        }
        new_entries.push(VolumeEntry {
            ord: vol.ord,
            shard_count: vol.shards.len() as u32,
            l2p_shard_roots: roots.into_boxed_slice(),
            l2p_shard_durable_seq: durable_seqs.into_boxed_slice(),
            created_lsn: vol.created_lsn,
            flags: vol.flags.load(std::sync::atomic::Ordering::Relaxed),
            dead_list_head_pid: vol
                .dead_list_head_pid
                .load(std::sync::atomic::Ordering::Acquire),
            dead_list_tail_pid: vol
                .dead_list_tail_pid
                .load(std::sync::atomic::Ordering::Acquire),
            parent_vol_ord: *vol.parent_vol_ord.read(),
            branched_at_lsn: vol.branched_at_lsn,
            promotion_cursor: *vol.promotion_cursor.read(),
            page_dead_list_head_pid: vol
                .page_dead_list_head_pid
                .load(std::sync::atomic::Ordering::Acquire),
            page_dead_list_tail_pid: vol
                .page_dead_list_tail_pid
                .load(std::sync::atomic::Ordering::Acquire),
        });
    }
    manifest.volumes = new_entries;
    manifest.refcount_shard_roots = refcount_shards
        .iter()
        .map(|shard| shard.rc.meta_page_id())
        .collect::<Vec<_>>()
        .into_boxed_slice();
    manifest.refcount_durable_seq = refcount_shards
        .iter()
        .map(|shard| {
            durable_override.unwrap_or_else(|| {
                shard
                    .last_flushed_lsn
                    .load(std::sync::atomic::Ordering::Acquire)
            })
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    // v17: L2P-page-rc roots (stable head meta page, like refcount) +
    // per-shard durable_seq. Same override semantics as refcount: a
    // `durable_override` caller (drop_volume / take_snapshot /
    // drop_snapshot / open recovery) has just flushed every shard and is
    // about to set `manifest.checkpoint_lsn = override`, so the page-rc
    // durable_seq must move with it to keep the
    // `min(durable_seq[]) == checkpoint_lsn` invariant.
    manifest.l2p_page_rc_shard_roots = l2p_page_rc.roots().into_boxed_slice();
    manifest.l2p_page_rc_durable_seq = (0..l2p_page_rc.shard_count())
        .map(|idx| durable_override.unwrap_or_else(|| l2p_page_rc.last_flushed_lsn(idx)))
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Ok(())
}

pub(super) fn flush_locked_l2p_shards(guards: &mut [RwLockWriteGuard<'_, PagedL2p>]) -> Result<()> {
    for tree in guards {
        tree.flush()?;
    }
    Ok(())
}

pub(super) fn commit_l2p_checkpoint(
    guards: &mut [RwLockWriteGuard<'_, PagedL2p>],
    generation: Lsn,
) -> Result<()> {
    for tree in guards {
        tree.checkpoint_committed(generation)?;
    }
    Ok(())
}

/// No-op shim: paged-array refcount has no checkpoint to commit
/// (sample-phase `RcShard::flush` already wrote everything synchronously).
/// Kept for call-site stability while the open / flush paths still mention
/// "refcount checkpoint".
pub(super) fn commit_refcount_checkpoint(_shards: &[Shard], _generation: Lsn) -> Result<()> {
    Ok(())
}

pub(super) fn validate_shard_count(shards_per_partition: u32) -> Result<usize> {
    let shard_count = usize::try_from(shards_per_partition)
        .map_err(|_| MetaDbError::InvalidArgument("shard count does not fit usize".into()))?;
    if shard_count == 0 {
        return Err(MetaDbError::InvalidArgument(
            "shards_per_partition must be greater than zero".into(),
        ));
    }
    Ok(shard_count)
}

/// Build N independent dedup apply lanes, one per shard. The lane
/// ordinal feeds into the worker thread's affinity binding so each
/// shard's apply thread can pin to a different CPU on supported
/// platforms.
pub(super) fn build_dedup_lanes(
    last_applied: Lsn,
    shard_count: usize,
    kind: ApplyLaneKind,
    metrics: Arc<MetaMetrics>,
) -> Box<[ApplyLane]> {
    (0..shard_count)
        .map(|sid| ApplyLane::new(last_applied, kind, sid, metrics.clone()))
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

/// Per-shard double-queue guards for the dedup maintenance lane.
pub(super) fn build_dedup_queued_flags(shard_count: usize) -> Box<[Arc<AtomicBool>]> {
    (0..shard_count)
        .map(|_| Arc::new(AtomicBool::new(false)))
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

/// Validate `cfg.dedup_shards` and return it on success. The dedup
/// shard count must be a power of two in `[1, MAX_DEDUP_SHARDS]`.
pub(super) fn validate_dedup_shards(dedup_shards: u32) -> Result<u32> {
    if dedup_shards == 0 {
        return Err(MetaDbError::InvalidArgument(
            "cfg.dedup_shards must be greater than zero".into(),
        ));
    }
    if !(dedup_shards as usize).is_power_of_two() {
        return Err(MetaDbError::InvalidArgument(format!(
            "cfg.dedup_shards must be a power of two; got {dedup_shards}",
        )));
    }
    if dedup_shards > crate::config::MAX_DEDUP_SHARDS {
        return Err(MetaDbError::InvalidArgument(format!(
            "cfg.dedup_shards={dedup_shards} exceeds MAX_DEDUP_SHARDS={}",
            crate::config::MAX_DEDUP_SHARDS,
        )));
    }
    Ok(dedup_shards)
}

pub(super) fn create_shards(
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    shard_count: usize,
    metrics: Arc<MetaMetrics>,
) -> Result<(Vec<Shard>, Box<[PageId]>)> {
    let mut shards = Vec::with_capacity(shard_count);
    let mut roots = Vec::with_capacity(shard_count);
    for shard_idx in 0..shard_count {
        let rc = crate::refcount::RcShard::create(page_store.clone(), page_cache.clone())?;
        roots.push(rc.meta_page_id());
        shards.push(Shard {
            rc: Arc::new(rc),
            apply_lane: ApplyLane::new(0, ApplyLaneKind::Refcount, shard_idx, metrics.clone()),
            last_flushed_lsn: AtomicU64::new(0),
        });
    }
    Ok((shards, roots.into_boxed_slice()))
}

pub(super) fn open_shards(
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    roots: &[PageId],
    initial_last_flushed_lsn: &[Lsn],
    metrics: Arc<MetaMetrics>,
) -> Result<Vec<Shard>> {
    if initial_last_flushed_lsn.len() != roots.len() {
        return Err(MetaDbError::Corruption(format!(
            "open_shards: per-shard last_flushed_lsn slice length {} does not match \
             roots length {}",
            initial_last_flushed_lsn.len(),
            roots.len(),
        )));
    }
    let mut shards = Vec::with_capacity(roots.len());
    for (shard_idx, &meta_page_id) in roots.iter().enumerate() {
        let rc =
            crate::refcount::RcShard::open(page_store.clone(), page_cache.clone(), meta_page_id)?;
        shards.push(Shard {
            rc: Arc::new(rc),
            apply_lane: ApplyLane::new(0, ApplyLaneKind::Refcount, shard_idx, metrics.clone()),
            last_flushed_lsn: AtomicU64::new(initial_last_flushed_lsn[shard_idx]),
        });
    }
    Ok(shards)
}

/// Wrap a freshly-built `PagedL2p` in an `L2pShard`, seeding the
/// read-view with an empty overlay. The empty overlay is correct for
/// any newly-opened tree because `create_with_cache` / `open_with_cache`
/// leave no dirty pages pending publish.
pub(super) fn make_l2p_shard(
    tree: PagedL2p,
    page_cache: &Arc<PageCache>,
    shard_idx: usize,
    metrics: Arc<MetaMetrics>,
    initial_last_flushed_lsn: Lsn,
    use_buffer: bool,
) -> L2pShard {
    let view = crate::paged::ReadView::new(
        tree.root(),
        tree.root_level(),
        crate::paged::ReadOverlay::default(),
        page_cache.clone(),
    );
    L2pShard {
        tree: RwLock::new(tree),
        read_view: RwLock::new(Arc::new(view)),
        active_readers: std::sync::atomic::AtomicUsize::new(0),
        apply_lane: ApplyLane::new(0, ApplyLaneKind::L2p, shard_idx, metrics),
        last_flushed_lsn: AtomicU64::new(initial_last_flushed_lsn),
        l2p_buffer: Arc::new(crate::db::l2p_buffer::L2pBuffer::new(initial_last_flushed_lsn)),
        use_buffer,
    }
}

pub(super) fn create_l2p_shards(
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    page_rc: Arc<crate::l2p_page_rc::L2pPageRc>,
    shard_count: usize,
    metrics: Arc<MetaMetrics>,
    use_buffer: bool,
    // A3: the volume's `created_lsn` — stamps each shard root's page-rc
    // `+1` so the fold applies it (≤ the volume's first write op).
    root_lsn: Lsn,
) -> Result<(Vec<L2pShard>, Box<[PageId]>)> {
    let mut shards = Vec::with_capacity(shard_count);
    let mut roots = Vec::with_capacity(shard_count);
    for shard_idx in 0..shard_count {
        let tree = PagedL2p::create_with_cache_rc(
            page_store.clone(),
            page_cache.clone(),
            page_rc.clone(),
            root_lsn,
        )?;
        roots.push(tree.root());
        shards.push(make_l2p_shard(
            tree,
            &page_cache,
            shard_idx,
            metrics.clone(),
            0,
            use_buffer,
        ));
    }
    Ok((shards, roots.into_boxed_slice()))
}

pub(super) fn open_l2p_shards(
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    page_rc: Arc<crate::l2p_page_rc::L2pPageRc>,
    roots: &[PageId],
    next_gen: Lsn,
    metrics: Arc<MetaMetrics>,
    initial_last_flushed_lsn: &[Lsn],
    use_buffer: bool,
) -> Result<Vec<L2pShard>> {
    if initial_last_flushed_lsn.len() != roots.len() {
        return Err(MetaDbError::Corruption(format!(
            "open_l2p_shards: per-shard last_flushed_lsn slice length {} does not match \
             roots length {}",
            initial_last_flushed_lsn.len(),
            roots.len(),
        )));
    }
    let mut shards = Vec::with_capacity(roots.len());
    for (shard_idx, &root) in roots.iter().enumerate() {
        let tree = PagedL2p::open_with_cache_rc(
            page_store.clone(),
            page_cache.clone(),
            page_rc.clone(),
            root,
            next_gen,
        )?;
        shards.push(make_l2p_shard(
            tree,
            &page_cache,
            shard_idx,
            metrics.clone(),
            initial_last_flushed_lsn[shard_idx],
            use_buffer,
        ));
    }
    Ok(shards)
}

pub(super) fn max_generation_from_locked_l2p(guards: &[RwLockWriteGuard<'_, PagedL2p>]) -> Lsn {
    guards
        .iter()
        .map(|tree| tree.next_generation())
        .max()
        .unwrap_or(0)
}

/// Paged-array refcount has no per-shard `next_generation` cursor; only
/// L2P supplies one, so the merged max collapses to the L2P max.
pub(super) fn max_generation_from_two_groups(
    a: &[RwLockWriteGuard<'_, PagedL2p>],
    _refcount_shards: &[Shard],
) -> Lsn {
    max_generation_from_locked_l2p(a)
}

// `encode_reverse_entry` / `decode_reverse_hash` were retired alongside
// the `paged_reverse` module + `DedupReverse*` WAL ops (schema v9 /
// WAL 0xB3). The promote-on-verified-hit cleanup path uses
// old-mapping read-back instead, so no in-tree caller still needs
// these helpers.
