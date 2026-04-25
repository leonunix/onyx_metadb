use super::*;

/// Lock every L2P shard mutex across the given volume set, in
/// (`volumes` order, shard index) order. Callers that reach multiple
/// volumes pass the sorted output of `Db::volumes_snapshot` so every
/// caller agrees on a single lock order, preventing the shard mutexes
/// from deadlocking against each other. Volumes are passed in as a slice
/// of `Arc<Volume>` so the clones keep the mutexes alive for the guard
/// lifetime.
pub(super) fn lock_all_l2p_shards_for<'v>(
    volumes: &'v [Arc<Volume>],
) -> Vec<MutexGuard<'v, PagedL2p>> {
    volumes
        .iter()
        .flat_map(|vol| vol.shards.iter().map(|shard| shard.tree.lock()))
        .collect()
}

/// Rebuild `manifest.volumes` and `manifest.refcount_shard_roots` from
/// the live tree roots held by the supplied guard slices. Used by
/// `Db::flush` / `Db::take_snapshot` (via the `&self` wrapper) and by
/// `Db::open` post-replay to sync the on-disk manifest with in-memory
/// state before any page reclaim.
pub(super) fn refresh_manifest_entries(
    manifest: &mut Manifest,
    volumes: &[Arc<Volume>],
    l2p_guards: &[MutexGuard<'_, PagedL2p>],
    refcount_guards: &[MutexGuard<'_, BTree>],
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
        for _ in 0..vol.shards.len() {
            roots.push(l2p_guards[guard_cursor].root());
            guard_cursor += 1;
        }
        new_entries.push(VolumeEntry {
            ord: vol.ord,
            shard_count: vol.shards.len() as u32,
            l2p_shard_roots: roots.into_boxed_slice(),
            created_lsn: vol.created_lsn,
            flags: vol.flags.load(std::sync::atomic::Ordering::Relaxed),
        });
    }
    manifest.volumes = new_entries;
    manifest.refcount_shard_roots = refcount_guards
        .iter()
        .map(|tree| tree.root())
        .collect::<Vec<_>>()
        .into_boxed_slice();
    Ok(())
}

pub(super) fn flush_locked_l2p_shards(guards: &mut [MutexGuard<'_, PagedL2p>]) -> Result<()> {
    for tree in guards {
        tree.flush()?;
    }
    Ok(())
}

pub(super) fn commit_l2p_checkpoint(
    guards: &mut [MutexGuard<'_, PagedL2p>],
    generation: Lsn,
) -> Result<()> {
    for tree in guards {
        tree.checkpoint_committed(generation)?;
    }
    Ok(())
}

pub(super) fn commit_refcount_checkpoint(
    guards: &mut [MutexGuard<'_, BTree>],
    generation: Lsn,
) -> Result<()> {
    for tree in guards {
        tree.checkpoint_committed(generation)?;
    }
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

pub(super) fn create_shards(
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    shard_count: usize,
) -> Result<(Vec<Shard>, Box<[PageId]>)> {
    let mut shards = Vec::with_capacity(shard_count);
    let mut roots = Vec::with_capacity(shard_count);
    for _ in 0..shard_count {
        let tree = BTree::create_with_cache(page_store.clone(), page_cache.clone())?;
        roots.push(tree.root());
        shards.push(Shard {
            tree: Mutex::new(tree),
        });
    }
    Ok((shards, roots.into_boxed_slice()))
}

pub(super) fn open_shards(
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    roots: &[PageId],
    next_gen: Lsn,
) -> Result<Vec<Shard>> {
    let mut shards = Vec::with_capacity(roots.len());
    for &root in roots {
        shards.push(Shard {
            tree: Mutex::new(BTree::open_with_cache(
                page_store.clone(),
                page_cache.clone(),
                root,
                next_gen,
            )?),
        });
    }
    Ok(shards)
}

pub(super) fn create_l2p_shards(
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    shard_count: usize,
) -> Result<(Vec<L2pShard>, Box<[PageId]>)> {
    let mut shards = Vec::with_capacity(shard_count);
    let mut roots = Vec::with_capacity(shard_count);
    for _ in 0..shard_count {
        let tree = PagedL2p::create_with_cache(page_store.clone(), page_cache.clone())?;
        roots.push(tree.root());
        shards.push(L2pShard {
            tree: Mutex::new(tree),
        });
    }
    Ok((shards, roots.into_boxed_slice()))
}

pub(super) fn open_l2p_shards(
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    roots: &[PageId],
    next_gen: Lsn,
) -> Result<Vec<L2pShard>> {
    let mut shards = Vec::with_capacity(roots.len());
    for &root in roots {
        shards.push(L2pShard {
            tree: Mutex::new(PagedL2p::open_with_cache(
                page_store.clone(),
                page_cache.clone(),
                root,
                next_gen,
            )?),
        });
    }
    Ok(shards)
}

pub(super) fn lsm_config_from_cfg(cfg: &Config) -> LsmConfig {
    let target_sst_records =
        ((cfg.lsm_memtable_bytes as usize) / crate::lsm::LSM_RECORD_SIZE).max(1);
    LsmConfig {
        memtable_bytes: cfg.lsm_memtable_bytes as usize,
        bits_per_entry: cfg.lsm_bloom_bits_per_entry,
        l0_sst_count_trigger: cfg.lsm_l0_sst_count_trigger as usize,
        target_sst_records,
        level_ratio: cfg.lsm_level_ratio,
    }
}

pub(super) fn max_generation_from_locked(guards: &[MutexGuard<'_, BTree>]) -> Lsn {
    guards
        .iter()
        .map(|tree| tree.next_generation())
        .max()
        .unwrap_or(0)
}

pub(super) fn max_generation_from_locked_l2p(guards: &[MutexGuard<'_, PagedL2p>]) -> Lsn {
    guards
        .iter()
        .map(|tree| tree.next_generation())
        .max()
        .unwrap_or(0)
}

pub(super) fn max_generation_from_two_groups(
    a: &[MutexGuard<'_, PagedL2p>],
    b: &[MutexGuard<'_, BTree>],
) -> Lsn {
    max_generation_from_locked_l2p(a).max(max_generation_from_locked(b))
}

/// Encode a `(pba, hash)` pair for storage in the `dedup_reverse` LSM.
///
/// The LSM key is 32 bytes (`Hash32`). We pack the 8-byte big-endian
/// PBA into the first 8 bytes so prefix scans by PBA become range
/// scans, and the first 24 bytes of the content hash into the
/// remaining 24 bytes of the key. The remaining 8 bytes of the hash
/// live in the value; decoders recover the full 32-byte hash by
/// concatenating `key[8..32]` with `value[0..8]`.
pub(crate) fn encode_reverse_entry(pba: Pba, hash: &Hash32) -> (Hash32, DedupValue) {
    let mut key = [0u8; 32];
    key[..8].copy_from_slice(&pba.to_be_bytes());
    key[8..32].copy_from_slice(&hash[..24]);
    let mut value = [0u8; 28];
    value[..8].copy_from_slice(&hash[24..]);
    (key, DedupValue(value))
}

/// Reconstruct the full 32-byte hash from a `dedup_reverse` key/value
/// pair written by [`encode_reverse_entry`].
pub(crate) fn decode_reverse_hash(key: &Hash32, value: &DedupValue) -> Hash32 {
    let mut hash = [0u8; 32];
    hash[..24].copy_from_slice(&key[8..32]);
    hash[24..].copy_from_slice(&value.0[..8]);
    hash
}
