//! Sharded embedded metadata database: the glue between `PageStore`,
//! `ManifestStore`, and one `BTree` per shard.
//!
//! Phase 4 scope:
//! - N independent COW B+tree shards behind one `Db`
//! - xxh3-based shard router
//! - thread-safe point writes via one mutex per shard
//! - fan-out range / diff / snapshot operations

use std::collections::HashMap;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU8;

use parking_lot::{Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard};
use xxhash_rust::xxh3::xxh3_64;

use crate::btree::BTree;
use crate::cache::{PageCache, PageCacheStats};
use crate::config::Config;
use crate::error::{MetaDbError, Result};
use crate::lsm::{DedupValue, Hash32, Lsm, LsmConfig};
use crate::manifest::{
    MANIFEST_BODY_VERSION, Manifest, ManifestStore, SnapshotEntry, VolumeEntry,
    write_snapshot_roots_page,
};
use crate::page_store::PageStore;
use crate::paged::PagedL2p;
use crate::paged::{DiffEntry, L2pValue};
use crate::testing::faults::{FaultController, FaultPoint};
use crate::tx::{ApplyOutcome, Transaction};
use crate::types::{FIRST_DATA_PAGE, Lba, Lsn, PageId, Pba, SnapshotId, VolumeOrdinal};
use crate::verify;
use crate::wal::{Wal, WalOp, encode_body};

/// Ordinal of the always-present bootstrap volume. Phase B commit 5 keeps the
/// surface API single-volume, so every L2P routing decision lands here. Later
/// commits take per-volume arguments from callers and route through the map
/// directly.
const BOOTSTRAP_VOLUME_ORD: VolumeOrdinal = 0;

/// Embedded metadata database.
pub struct Db {
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    manifest_state: Mutex<ManifestState>,
    /// Per-volume L2P paged radix-tree shard groups. Phase B commit 5 always
    /// contains exactly one entry for [`BOOTSTRAP_VOLUME_ORD`]; commit 6/7
    /// introduce real `create_volume` / `drop_volume` / `clone_volume` traffic
    /// that mutates this map. The map lives behind an `RwLock` so the hot
    /// path (commit / get / range) takes `.read()` — contention happens only
    /// against the rare volume-lifecycle writer.
    ///
    /// Each volume owns its own `Vec<L2pShard>`; xxh3 routing divides by
    /// `volume.shards.len()`, so shard routing is identical to the pre-7
    /// flat-shard layout as long as every volume is created with the same
    /// shard count.
    volumes: RwLock<HashMap<VolumeOrdinal, Arc<Volume>>>,
    /// PBA refcount B+tree shards (PBA → first 4 bytes = u32 big-endian
    /// refcount, remaining 24 bytes reserved). Refcount is a global running
    /// tally — not per-volume — and stays at the top level for that reason.
    refcount_shards: Vec<Shard>,
    /// Global dedup index: 32-byte SHA-256 content hash → 28-byte opaque
    /// `DedupValue`.
    dedup_index: Lsm,
    /// Reverse index: key = `[pba: 8B BE][hash_first_24B]`, value =
    /// `[hash_last_8B | zero padding]`. Used by PBA refcount → 0 to
    /// discover and clean up the `dedup_index` entries whose PBA is
    /// going away. Prefix-scan by 8-byte PBA locates every matching
    /// row.
    dedup_reverse: Lsm,
    /// Write-ahead log. All mutations route through here so they survive
    /// crash between checkpoints.
    wal: Wal,
    /// Excludes apply phases from flush / snapshot. Commit takes
    /// `.read()` across the apply + bump; flush / take_snapshot /
    /// drop_snapshot take `.write()` so they observe a quiescent tree
    /// state matching `last_applied_lsn`. Replaces the phase-6
    /// `commit_lock`: submission to the WAL now happens **outside** any
    /// lock, so concurrent submitters land in the same WAL group-commit
    /// batch. Apply order is restored by the LSN-ordered condvar queue
    /// below, not by serialising WAL submits.
    apply_gate: RwLock<()>,
    /// LSN of the most recent op applied to in-memory state. Initialised
    /// from `manifest.checkpoint_lsn` on open (the manifest promises that
    /// every LSN at or below this value is already reflected in the
    /// trees / SSTs) and bumped on every commit. Paired with
    /// [`commit_cvar`](Self::commit_cvar) to form the apply-order queue.
    last_applied_lsn: Mutex<Lsn>,
    /// Notified whenever `last_applied_lsn` advances. Commit threads
    /// wait on this after WAL submit returns with their assigned LSN,
    /// re-checking `*last_applied_lsn + 1 == lsn` on each wakeup. Every
    /// LSN is unique, so at most one thread waits for any given
    /// predecessor value.
    commit_cvar: Condvar,
    /// Snapshot readers hold a shared guard; `drop_snapshot` takes the
    /// exclusive side so it can't free pages still visible to a live view.
    snapshot_views: RwLock<()>,
    /// Serialises `drop_snapshot` against *all* other mutations.
    /// Every write path in [`commit_ops`](Self::commit_ops) (user writes,
    /// incref/decref, dedup puts) takes the read side; `drop_snapshot`
    /// takes the write side. This is necessary because the drop plan
    /// is rc-dependent — a concurrent `cow_for_write` landing between
    /// plan computation and WAL apply can change the rcs of pages
    /// shared with the snapshot, invalidating the cascade decisions
    /// baked into the plan. The normal `apply_gate` is insufficient
    /// because concurrent submitters queue WAL records *before*
    /// taking the apply gate, so their ops can sneak between our plan
    /// and our apply.
    ///
    /// Lock order: `drop_gate` → `apply_gate` → `volumes` →
    /// `manifest_state` → shard mutex → `snapshot_views`. Everyone
    /// entering `commit_ops` or `drop_snapshot` respects this prefix;
    /// internal reads that don't mutate skip `drop_gate` entirely. The
    /// `volumes` link is read-only in the hot path — `.read()` is taken
    /// just long enough to clone the `Arc<Volume>` out so shard mutexes
    /// can be acquired without keeping the map guard alive.
    drop_gate: RwLock<()>,
    /// Runtime cap on the volumes table size. Seeded from
    /// [`Config::max_volumes`] at create / open. `create_volume` refuses
    /// to mint a new ordinal once the live volume count hits this value.
    max_volumes: u32,
    #[allow(dead_code)]
    faults: Arc<FaultController>,
    #[allow(dead_code)]
    db_path: PathBuf,
}

struct ManifestState {
    store: ManifestStore,
    manifest: Manifest,
}

struct Shard {
    tree: Mutex<BTree>,
}

struct L2pShard {
    tree: Mutex<PagedL2p>,
}

/// L2P home for one user-facing volume. Owns its own shard group; shard
/// routing inside a volume uses `xxh3_64(lba) % shards.len()`, identical to
/// the pre-7 flat layout.
///
/// Fields beyond `shards` are placeholders for commit 6/7 semantics:
/// `created_lsn` will be stamped by `CreateVolume` / `CloneVolume` so
/// recovery can skip L2P ops for volumes that hadn't been created yet at a
/// given LSN; `flags` is reserved for the drop-pending bit.
#[allow(dead_code)]
struct Volume {
    ord: VolumeOrdinal,
    shards: Vec<L2pShard>,
    created_lsn: Lsn,
    flags: AtomicU8,
}

impl Volume {
    fn new(ord: VolumeOrdinal, shards: Vec<L2pShard>, created_lsn: Lsn) -> Self {
        Self {
            ord,
            shards,
            created_lsn,
            flags: AtomicU8::new(0),
        }
    }
}

struct DedupManifestUpdate {
    old_dedup_heads: Vec<PageId>,
    old_dedup_reverse_heads: Vec<PageId>,
}

/// Iterator over a globally key-ordered range scan assembled from all
/// shards.
pub struct DbRangeIter {
    inner: std::vec::IntoIter<(u64, L2pValue)>,
}

impl DbRangeIter {
    fn new(items: Vec<(u64, L2pValue)>) -> Self {
        Self {
            inner: items.into_iter(),
        }
    }
}

impl Iterator for DbRangeIter {
    type Item = Result<(u64, L2pValue)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Ok)
    }
}

/// Iterator over every `(Pba, refcount)` pair in the global refcount
/// table, in Pba order. Currently materialised upfront across all
/// refcount shards; the `impl Iterator` surface lets future commits
/// swap the body for a lazy walker without touching call sites.
pub struct DbRefcountIter {
    inner: std::vec::IntoIter<(Pba, u32)>,
}

impl Iterator for DbRefcountIter {
    type Item = Result<(Pba, u32)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Ok)
    }
}

/// Iterator over every live `(Hash32, DedupValue)` entry in the
/// dedup forward index. Tombstoned rows are hidden. Output is sorted
/// by hash.
pub struct DbDedupIter {
    inner: std::vec::IntoIter<(Hash32, DedupValue)>,
}

impl Iterator for DbDedupIter {
    type Item = Result<(Hash32, DedupValue)>;
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(Ok)
    }
}

#[derive(Clone, Debug)]
struct OwnedRange {
    start: Bound<u64>,
    end: Bound<u64>,
}

impl OwnedRange {
    fn new<R: RangeBounds<u64>>(range: R) -> Self {
        Self {
            start: clone_bound(range.start_bound()),
            end: clone_bound(range.end_bound()),
        }
    }
}

impl RangeBounds<u64> for OwnedRange {
    fn start_bound(&self) -> Bound<&u64> {
        ref_bound(&self.start)
    }

    fn end_bound(&self) -> Bound<&u64> {
        ref_bound(&self.end)
    }
}

/// Paths used by a `Db` on disk.
fn page_file(root: &Path) -> PathBuf {
    root.join("pages.onyx_meta")
}

/// Directory that holds the WAL segments.
fn wal_dir(root: &Path) -> PathBuf {
    root.join("wal")
}

fn clone_bound(bound: Bound<&u64>) -> Bound<u64> {
    match bound {
        Bound::Included(v) => Bound::Included(*v),
        Bound::Excluded(v) => Bound::Excluded(*v),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn ref_bound(bound: &Bound<u64>) -> Bound<&u64> {
    match bound {
        Bound::Included(v) => Bound::Included(v),
        Bound::Excluded(v) => Bound::Excluded(v),
        Bound::Unbounded => Bound::Unbounded,
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
        std::fs::create_dir_all(&cfg.path)?;
        let pages_path = page_file(&cfg.path);
        let page_store = Arc::new(PageStore::create(&pages_path)?);
        let page_cache = Arc::new(PageCache::new(page_store.clone(), cfg.page_cache_bytes));
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

        let wal = Wal::create(
            &wal_dir(&cfg.path),
            &cfg,
            manifest.checkpoint_lsn + 1,
            faults.clone(),
        )?;

        let volume_zero = Arc::new(Volume::new(BOOTSTRAP_VOLUME_ORD, l2p_shards, 0));
        let mut volumes = HashMap::with_capacity(1);
        volumes.insert(BOOTSTRAP_VOLUME_ORD, volume_zero);

        Ok(Self {
            page_store,
            page_cache,
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
        let page_store = Arc::new(PageStore::open(&pages_path)?);
        let page_cache = Arc::new(PageCache::new(page_store.clone(), cfg.page_cache_bytes));
        let lsm_config = lsm_config_from_cfg(&cfg);
        let (mut manifest_store, mut manifest) =
            ManifestStore::open_existing(page_store.clone(), faults.clone())?;
        if manifest.volumes.is_empty() {
            return Err(MetaDbError::Corruption(
                "manifest has no volume entries; database was not initialized".into(),
            ));
        }
        if !manifest.volumes.iter().any(|v| v.ord == BOOTSTRAP_VOLUME_ORD) {
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
            volumes.insert(entry.ord, Arc::new(Volume::new(entry.ord, shards, entry.created_lsn)));
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
                        let (shards, roots) = apply_create_volume(
                            &page_store,
                            &page_cache,
                            *shard_count,
                        )?;
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
                                    shard.tree.lock().forget_page(pid);
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
                        volumes.insert(
                            *new_ord,
                            Arc::new(Volume::new(*new_ord, shards, lsn)),
                        );
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
                    let outcome = apply_op_bare(
                        &volumes,
                        &refcount_shards,
                        &dedup_index,
                        &dedup_reverse,
                        &page_store,
                        lsn,
                        op,
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
        let wal = Wal::create(&wal_path, &cfg, last_applied + 1, faults.clone())?;

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

            refresh_manifest_entries(
                &mut manifest,
                &sorted,
                &l2p_guards,
                &refcount_guards,
            )?;
            manifest.checkpoint_lsn = last_applied;
            manifest_store.commit(&manifest)?;

            dedup_index.free_old_level_heads(&old_dedup_heads, dedup_generation)?;
            dedup_reverse.free_old_level_heads(&old_dedup_reverse_heads, dedup_generation)?;
        }

        // Reclaim orphan pages AFTER replay + post-replay commit:
        // WAL-replayed DropSnapshot ops have already mutated
        // `manifest.snapshots` and freed snapshot-exclusive tree
        // pages, so the walk now sees the post-replay manifest
        // instead of a stale snapshot list that would try to
        // traverse already-freed pages.
        let reclaim_generation = last_applied.max(manifest.checkpoint_lsn).max(1) + 1;
        verify::reclaim_orphan_pages(&page_store, &manifest, reclaim_generation)?;

        Ok(Self {
            page_store,
            page_cache,
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
            max_volumes: cfg.max_volumes,
            faults,
            db_path: cfg.path,
        })
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
        self.finish_dedup_manifest_update(dedup_update, tree_generation)?;
        Ok(())
    }

    // -------- transaction / commit --------------------------------------

    /// Start a new transaction that buffers ops until `commit()`.
    pub fn begin(&self) -> Transaction<'_> {
        Transaction::new(self)
    }

    /// LSN of the most recent successful commit.
    pub fn last_applied_lsn(&self) -> Lsn {
        *self.last_applied_lsn.lock()
    }

    /// Internal: submit a set of ops to the WAL, apply them to indexes,
    /// and return the assigned LSN plus any per-op outcomes.
    ///
    /// Concurrency: WAL submission runs **outside** any Db-level lock so
    /// multiple submitters coalesce into one group-commit batch at the
    /// WAL writer. Apply order is re-serialised after submit via the
    /// `last_applied_lsn` + `commit_cvar` queue — each commit waits
    /// until `*last_applied_lsn + 1 == lsn`, applies under
    /// `apply_gate.read()`, then bumps `last_applied_lsn` **before**
    /// dropping the gate so flush / snapshot never observe trees whose
    /// state is ahead of `last_applied_lsn`.
    pub(crate) fn commit_ops(&self, ops: &[WalOp]) -> Result<(Lsn, Vec<ApplyOutcome>)> {
        if ops.is_empty() {
            return Ok((self.last_applied_lsn(), Vec::new()));
        }
        // `drop_gate.read()` pairs with `drop_snapshot`'s write acquire.
        // Held across submit + apply so a concurrent drop can't insert
        // itself between our LSN assignment and our apply — its
        // rc-dependent plan would otherwise be invalidated by our
        // cow_for_write bumps, and vice versa.
        let _drop_guard = self.drop_gate.read();
        let body = encode_body(ops);
        let lsn = self.wal.submit(body)?;
        self.faults.inject(FaultPoint::CommitPostWalBeforeApply)?;

        // Wait until every lower LSN has applied. LSNs are unique and
        // assigned in submit order by the WAL writer, so exactly one
        // waiter is unblocked by each bump.
        {
            let mut applied = self.last_applied_lsn.lock();
            while *applied + 1 < lsn {
                self.commit_cvar.wait(&mut applied);
            }
        }

        let apply_guard = self.apply_gate.read();
        // Clone out the volume map once per commit — HashMap + `Arc`
        // clones are cheap, and keeping the Arcs live for the whole
        // apply loop avoids holding `volumes.read()` across apply.
        // That matters because commit 8+ will acquire `volumes.write()`
        // on the lifecycle path, and a long-held reader would stall it.
        let volumes = self.volumes.read().clone();
        let mut outcomes = Vec::with_capacity(ops.len());
        for op in ops {
            outcomes.push(self.apply_op(&volumes, lsn, op)?);
        }
        self.faults
            .inject(FaultPoint::CommitPostApplyBeforeLsnBump)?;

        // Bump BEFORE dropping the gate: if we released the gate first
        // a concurrent flush could observe `last_applied_lsn = lsn - 1`
        // while trees already contain op `lsn`, causing recovery to
        // double-apply on restart (refcount incref is not idempotent).
        {
            let mut applied = self.last_applied_lsn.lock();
            *applied = lsn;
            self.commit_cvar.notify_all();
        }
        drop(apply_guard);
        Ok((lsn, outcomes))
    }

    fn apply_op(
        &self,
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        lsn: Lsn,
        op: &WalOp,
    ) -> Result<ApplyOutcome> {
        let outcome = apply_op_bare(
            volumes,
            &self.refcount_shards,
            &self.dedup_index,
            &self.dedup_reverse,
            &self.page_store,
            lsn,
            op,
        )?;
        // DropSnapshot also mutates the in-memory manifest's snapshot
        // list; the page work lives in apply_op_bare so it can be
        // shared with the replay path. Lock order (apply_gate.read →
        // manifest_state) matches every other call site.
        if let WalOp::DropSnapshot { id, .. } = op {
            let mut mstate = self.manifest_state.lock();
            mstate.manifest.snapshots.retain(|s| s.id != *id);
        }
        Ok(outcome)
    }

    // -------- refcount + dedup ops --------------------------------------

    /// Return the current refcount for `pba`, or 0 if no entry exists.
    pub fn get_refcount(&self, pba: Pba) -> Result<u32> {
        let sid = self.refcount_shard_for(pba);
        let mut tree = self.refcount_shards[sid].tree.lock();
        Ok(tree.get(pba)?.unwrap_or(0))
    }

    /// Batched refcount lookup. Groups `pbas` by shard, locks each shard
    /// once, and reads every PBA that falls to it before moving on. Output
    /// order matches input order; duplicates produce repeated results.
    /// Unmapped PBAs read back as `0`, same as [`get_refcount`].
    pub fn multi_get_refcount(&self, pbas: &[Pba]) -> Result<Vec<u32>> {
        if pbas.is_empty() {
            return Ok(Vec::new());
        }
        let shard_count = self.refcount_shards.len();
        let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); shard_count];
        for (idx, pba) in pbas.iter().enumerate() {
            buckets[self.refcount_shard_for(*pba)].push(idx);
        }
        let mut out: Vec<u32> = vec![0; pbas.len()];
        for (sid, idxs) in buckets.into_iter().enumerate() {
            if idxs.is_empty() {
                continue;
            }
            let mut tree = self.refcount_shards[sid].tree.lock();
            for idx in idxs {
                out[idx] = tree.get(pbas[idx])?.unwrap_or(0);
            }
        }
        Ok(out)
    }

    /// Increment `pba`'s refcount by `delta`. Returns the new value.
    /// `delta == 0` is a no-op that still performs a lookup.
    pub fn incref_pba(&self, pba: Pba, delta: u32) -> Result<u32> {
        let mut tx = self.begin();
        tx.incref_pba(pba, delta);
        let (_, outcomes) = tx.commit_with_outcomes()?;
        match outcomes.into_iter().next().unwrap() {
            ApplyOutcome::RefcountNew(v) => Ok(v),
            _ => unreachable!("incref produces RefcountNew"),
        }
    }

    /// Decrement `pba`'s refcount by `delta`. Returns the new value.
    /// Decrementing below zero is an error. When the new value hits
    /// zero the row is removed entirely, so the caller is responsible
    /// for cleaning up the corresponding dedup entry.
    pub fn decref_pba(&self, pba: Pba, delta: u32) -> Result<u32> {
        let mut tx = self.begin();
        tx.decref_pba(pba, delta);
        let (_, outcomes) = tx.commit_with_outcomes()?;
        match outcomes.into_iter().next().unwrap() {
            ApplyOutcome::RefcountNew(v) => Ok(v),
            _ => unreachable!("decref produces RefcountNew"),
        }
    }

    /// Record a `hash → value` entry in the dedup index (WAL-logged).
    pub fn put_dedup(&self, hash: Hash32, value: DedupValue) -> Result<Lsn> {
        let mut tx = self.begin();
        tx.put_dedup(hash, value);
        tx.commit()
    }

    /// Tombstone `hash` in the dedup index (WAL-logged).
    pub fn delete_dedup(&self, hash: Hash32) -> Result<Lsn> {
        let mut tx = self.begin();
        tx.delete_dedup(hash);
        tx.commit()
    }

    /// Point-lookup `hash` in the dedup index.
    pub fn get_dedup(&self, hash: &Hash32) -> Result<Option<DedupValue>> {
        self.dedup_index.get(hash)
    }

    /// Batched dedup index lookup. Shares one LSM reader-drain and one
    /// `levels` snapshot across all hashes. Output order matches input
    /// order; duplicates produce repeated results.
    pub fn multi_get_dedup(&self, hashes: &[Hash32]) -> Result<Vec<Option<DedupValue>>> {
        self.dedup_index.multi_get(hashes)
    }

    // -------- dedup_reverse operations ----------------------------------

    /// Register `hash` as mapped to `pba` in the reverse index. This
    /// is an LSM put, not a modification of the forward dedup index.
    /// Callers typically pair it with `put_dedup(hash, value)` inside
    /// one `begin() / commit()` transaction so both land atomically.
    pub fn register_dedup_reverse(&self, pba: Pba, hash: Hash32) -> Result<Lsn> {
        let mut tx = self.begin();
        tx.register_dedup_reverse(pba, hash);
        tx.commit()
    }

    /// Remove the `(pba, hash)` entry from the reverse index.
    pub fn unregister_dedup_reverse(&self, pba: Pba, hash: Hash32) -> Result<Lsn> {
        let mut tx = self.begin();
        tx.unregister_dedup_reverse(pba, hash);
        tx.commit()
    }

    /// Every full 32-byte hash currently registered for `pba` in the
    /// reverse index. Does **not** include tombstoned entries.
    ///
    /// Cost: scans every `dedup_reverse` SST whose min/max range
    /// intersects the 8-byte PBA prefix, plus the memtable. Fine for
    /// the decref-to-zero cleanup path (rare, per-PBA); not suitable
    /// for hot-path queries.
    pub fn scan_dedup_reverse_for_pba(&self, pba: Pba) -> Result<Vec<Hash32>> {
        let prefix = pba.to_be_bytes();
        let rows = self.dedup_reverse.scan_prefix(&prefix)?;
        Ok(rows
            .into_iter()
            .map(|(key, value)| decode_reverse_hash(&key, &value))
            .collect())
    }

    /// Batched `dedup_reverse` prefix scan: one call per PBA, one
    /// reader-drain acquisition and one `levels` snapshot shared across
    /// all PBAs. Returns one `Vec<Hash32>` per input PBA, in input order.
    ///
    /// Intended caller: writer / dedup cleanup path sweeping dead PBAs
    /// in a single batch (see onyx-storage `cleanup_dedup_for_pbas_batch`).
    pub fn multi_scan_dedup_reverse_for_pba(
        &self,
        pbas: &[Pba],
    ) -> Result<Vec<Vec<Hash32>>> {
        if pbas.is_empty() {
            return Ok(Vec::new());
        }
        let prefixes: Vec<[u8; 8]> = pbas.iter().map(|pba| pba.to_be_bytes()).collect();
        let prefix_refs: Vec<&[u8]> = prefixes.iter().map(|p| p.as_slice()).collect();
        let rows_per_pba = self.dedup_reverse.multi_scan_prefix(&prefix_refs)?;
        Ok(rows_per_pba
            .into_iter()
            .map(|rows| {
                rows.into_iter()
                    .map(|(key, value)| decode_reverse_hash(&key, &value))
                    .collect()
            })
            .collect())
    }

    /// `true` if the dedup memtable has reached its freeze threshold.
    pub fn dedup_should_flush(&self) -> bool {
        self.dedup_index.should_flush()
    }

    /// Flush the dedup memtable to a fresh L0 SST. Returns `None` if
    /// the memtable is empty.
    pub fn flush_dedup_memtable(&self) -> Result<bool> {
        let generation = self.current_generation();
        Ok(self.dedup_index.flush_memtable(generation)?.is_some())
    }

    /// Run one round of dedup compaction. Returns `true` if any work was
    /// performed.
    pub fn compact_dedup_once(&self) -> Result<bool> {
        let generation = self.current_generation();
        Ok(self.dedup_index.compact_once(generation)?.is_some())
    }

    /// Iterate every `(Pba, refcount)` pair across all refcount shards,
    /// sorted by Pba. Refcount is a running tally (global), so there is
    /// no per-volume filtering — callers doing volume-scoped audits
    /// cross-reference with [`range`](Self::range) output themselves.
    ///
    /// Currently materialised upfront; the `impl Iterator` surface is
    /// exposed so future commits can swap the body for a lazy walker
    /// without touching call sites.
    pub fn iter_refcounts(&self) -> Result<DbRefcountIter> {
        let mut all: Vec<(Pba, u32)> = Vec::new();
        for shard in &self.refcount_shards {
            let mut tree = shard.tree.lock();
            for rec in tree.iter_stream()? {
                all.push(rec?);
            }
        }
        all.sort_unstable_by_key(|(pba, _)| *pba);
        Ok(DbRefcountIter {
            inner: all.into_iter(),
        })
    }

    /// Iterate every live `(Hash32, DedupValue)` entry in the dedup
    /// forward index, sorted by hash. Tombstoned hashes are hidden.
    /// Materialised via the LSM's prefix-scan path with an empty prefix;
    /// shares one `reader_drain` and one `levels` snapshot with any
    /// concurrent readers.
    pub fn iter_dedup(&self) -> Result<DbDedupIter> {
        let all = self.dedup_index.scan_prefix(&[])?;
        Ok(DbDedupIter {
            inner: all.into_iter(),
        })
    }

    // -------- tree operations --------------------------------------------

    /// Point lookup in volume `vol_ord`'s L2P tree.
    pub fn get(&self, vol_ord: VolumeOrdinal, lba: Lba) -> Result<Option<L2pValue>> {
        let volume = self.volume(vol_ord)?;
        let sid = shard_for_key_l2p(&volume.shards, lba);
        let mut tree = volume.shards[sid].tree.lock();
        tree.get(lba)
    }

    /// Batched L2P lookup inside volume `vol_ord`. Groups `lbas` by shard,
    /// locks each shard once, and reads every lba that falls to it before
    /// moving on. Output order matches input order; duplicates produce
    /// repeated results.
    pub fn multi_get(
        &self,
        vol_ord: VolumeOrdinal,
        lbas: &[Lba],
    ) -> Result<Vec<Option<L2pValue>>> {
        if lbas.is_empty() {
            return Ok(Vec::new());
        }
        let volume = self.volume(vol_ord)?;
        let shard_count = volume.shards.len();
        let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); shard_count];
        for (idx, lba) in lbas.iter().enumerate() {
            buckets[shard_for_key_l2p(&volume.shards, *lba)].push(idx);
        }
        let mut out: Vec<Option<L2pValue>> = vec![None; lbas.len()];
        for (sid, idxs) in buckets.into_iter().enumerate() {
            if idxs.is_empty() {
                continue;
            }
            let mut tree = volume.shards[sid].tree.lock();
            for idx in idxs {
                out[idx] = tree.get(lbas[idx])?;
            }
        }
        Ok(out)
    }

    /// Insert `lba → value` in volume `vol_ord`, returning the previous
    /// value if any. Auto-commits as a one-op transaction.
    pub fn insert(
        &self,
        vol_ord: VolumeOrdinal,
        lba: Lba,
        value: L2pValue,
    ) -> Result<Option<L2pValue>> {
        let mut tx = self.begin();
        tx.insert(vol_ord, lba, value);
        let (_, outcomes) = tx.commit_with_outcomes()?;
        match outcomes.into_iter().next().unwrap() {
            ApplyOutcome::L2pPrev(prev) => Ok(prev),
            _ => unreachable!("insert produces L2pPrev"),
        }
    }

    /// Delete `lba` from volume `vol_ord`, returning the previous value
    /// if any. Auto-commits as a one-op transaction.
    pub fn delete(&self, vol_ord: VolumeOrdinal, lba: Lba) -> Result<Option<L2pValue>> {
        let mut tx = self.begin();
        tx.delete(vol_ord, lba);
        let (_, outcomes) = tx.commit_with_outcomes()?;
        match outcomes.into_iter().next().unwrap() {
            ApplyOutcome::L2pPrev(prev) => Ok(prev),
            _ => unreachable!("delete produces L2pPrev"),
        }
    }

    /// Range scan within volume `vol_ord`. Returns globally-key-ordered
    /// `(lba, value)` pairs by locking every shard of the volume and
    /// merging their individual range scans.
    pub fn range<R: RangeBounds<Lba>>(
        &self,
        vol_ord: VolumeOrdinal,
        range: R,
    ) -> Result<DbRangeIter> {
        let range = OwnedRange::new(range);
        let volume = self.volume(vol_ord)?;
        let mut guards: Vec<MutexGuard<'_, PagedL2p>> =
            volume.shards.iter().map(|s| s.tree.lock()).collect();
        let mut items = Vec::new();
        for tree in &mut guards {
            items.extend(tree.range(range.clone())?.collect::<Result<Vec<_>>>()?);
        }
        items.sort_unstable_by_key(|(k, _)| *k);
        Ok(DbRangeIter::new(items))
    }

    /// Streaming variant of [`range`](Self::range). Currently an alias —
    /// the body delegates to `range`'s eager materialisation so every
    /// caller already gets a stable iterator surface, and a future commit
    /// can swap the body for a lazy frame-stack walker without touching
    /// call sites.
    pub fn range_stream<R: RangeBounds<Lba>>(
        &self,
        vol_ord: VolumeOrdinal,
        range: R,
    ) -> Result<DbRangeIter> {
        self.range(vol_ord, range)
    }

    // -------- snapshot operations -----------------------------------------

    /// Take a snapshot of volume `vol_ord`'s L2P state. Returns the new
    /// snapshot id. Persisted immediately via a manifest commit. Unknown
    /// volume ordinals surface as `InvalidArgument`.
    ///
    /// Refcount state is global (Phase 6.5b retired per-snapshot refcount
    /// roots), so the snapshot only captures the target volume's L2P
    /// shard roots + an incref on each of them so the snapshot's view
    /// outlives subsequent COW writes on the target volume.
    ///
    /// Takes `apply_gate.write()` so `last_applied_lsn` and the shard
    /// roots we sample below describe the same LSN point. Holds shard
    /// mutexes for every volume for the flush-before-manifest-commit
    /// step — checkpoint_lsn advances across all volumes, so every
    /// volume's dirty pages must be on disk before the commit fsyncs
    /// the manifest slot.
    pub fn take_snapshot(&self, vol_ord: VolumeOrdinal) -> Result<SnapshotId> {
        // Exclude in-flight apply phases so `last_applied_lsn` and the
        // per-shard roots we sample below describe the same LSN point.
        let _apply_guard = self.apply_gate.write();
        // Resolve the target volume before touching manifest state so an
        // unknown ordinal short-circuits with a clean `InvalidArgument`.
        let target = self.volume(vol_ord)?;
        let mut manifest_state = self.manifest_state.lock();
        let volumes = self.volumes_snapshot();
        let mut l2p_guards = lock_all_l2p_shards_for(&volumes);
        let mut refcount_guards = self.lock_all_refcount_shards();

        // Locate the contiguous range of `l2p_guards` that belongs to
        // `target`. `lock_all_l2p_shards_for` iterates `volumes` in
        // ordinal order, so we walk preceding volumes and sum their
        // shard counts.
        let mut target_start = 0usize;
        for vol in &volumes {
            if vol.ord == vol_ord {
                break;
            }
            target_start += vol.shards.len();
        }
        let target_end = target_start + target.shards.len();

        let id = manifest_state.manifest.next_snapshot_id;
        let next_snapshot_id = id
            .checked_add(1)
            .ok_or_else(|| MetaDbError::Corruption("snapshot id overflow".into()))?;

        // Sample roots without incref'ing. The actual incref lands only
        // after the projected manifest passes a dry-run encode below —
        // see commit-ordering rationale on the pre-check.
        //
        // Phase 6.5b: refcount is a running tally, not point-in-time
        // state. Snapshots only capture L2P. We still hold refcount
        // guards for `max_generation_from_two_groups` and
        // `refresh_manifest_from_locked`, but skip the per-tree
        // snapshot incref.
        let l2p_roots: Vec<PageId> = l2p_guards[target_start..target_end]
            .iter()
            .map(|tree| tree.root())
            .collect();
        let created_lsn = max_generation_from_two_groups(&l2p_guards, &refcount_guards);

        // Bring the manifest up to the version we intend to commit,
        // minus the new snapshot entry. Running prepare/refresh first
        // means the capacity pre-check below sees the exact dedup /
        // volume layout `encode()` will see at commit time.
        let dedup_update =
            self.prepare_dedup_manifest_update(&mut manifest_state.manifest, created_lsn)?;
        self.refresh_manifest_from_locked(
            &mut manifest_state.manifest,
            &volumes,
            &l2p_guards,
            &refcount_guards,
        )?;

        // Pre-check: does the projected manifest (with the new snapshot
        // entry appended) still fit in one page? Failures after this
        // point would persist shard-root refcount bumps without a
        // matching snapshot entry — offline verify then reports
        // orphan rc. Why: `manifest_state.store.commit(...)` below calls
        // `Manifest::encode`, which rejects a too-full snapshot table
        // with `InvalidArgument`. Running the same encode against a
        // probe page here turns that failure into a clean early return
        // before any irreversible side effect (incref / page write /
        // shard flush). Use a non-NULL placeholder for `l2p_roots_page`
        // — encode only rejects `NULL_PAGE`, any other value passes.
        {
            let mut probe = manifest_state.manifest.clone();
            probe.checkpoint_lsn = *self.last_applied_lsn.lock();
            probe.snapshots.push(SnapshotEntry {
                id,
                vol_ord,
                l2p_roots_page: FIRST_DATA_PAGE,
                created_lsn,
                l2p_shard_roots: l2p_roots.clone().into_boxed_slice(),
            });
            probe.next_snapshot_id = next_snapshot_id;
            probe.check_encodable()?;
        }

        // Pre-check passed; safe to make irreversible changes.
        let l2p_roots_page = write_snapshot_roots_page(&self.page_store, &l2p_roots, created_lsn)?;
        for tree in &mut l2p_guards[target_start..target_end] {
            tree.incref_root_for_snapshot()?;
        }
        flush_locked_l2p_shards(&mut l2p_guards)?;
        self.flush_locked_refcount_shards(&mut refcount_guards)?;

        manifest_state.manifest.checkpoint_lsn = *self.last_applied_lsn.lock();
        manifest_state.manifest.snapshots.push(SnapshotEntry {
            id,
            vol_ord,
            l2p_roots_page,
            created_lsn,
            l2p_shard_roots: l2p_roots.into_boxed_slice(),
        });
        manifest_state.manifest.next_snapshot_id = next_snapshot_id;
        let manifest = manifest_state.manifest.clone();
        manifest_state.store.commit(&manifest)?;
        self.finish_dedup_manifest_update(dedup_update, created_lsn)?;
        Ok(id)
    }

    /// Open a read-only view of the data as it existed when `id` was taken.
    /// Returns `None` if the snapshot id is unknown.
    pub fn snapshot_view(&self, id: SnapshotId) -> Option<SnapshotView<'_>> {
        let guard = self.snapshot_views.read();
        let entry = {
            let manifest_state = self.manifest_state.lock();
            manifest_state.manifest.find_snapshot(id).cloned()
        }?;
        Some(SnapshotView {
            db: self,
            entry,
            _guard: guard,
        })
    }

    /// Compute the diff between two snapshots. Both snapshots must
    /// belong to the same volume; cross-volume diff is rejected.
    pub fn diff(&self, a: SnapshotId, b: SnapshotId) -> Result<Vec<DiffEntry>> {
        let _guard = self.snapshot_views.read();
        let (vol_ord, a_roots, b_roots) = {
            let manifest_state = self.manifest_state.lock();
            let a_entry = manifest_state
                .manifest
                .find_snapshot(a)
                .ok_or_else(|| MetaDbError::InvalidArgument(format!("unknown snapshot id {a}")))?;
            let b_entry = manifest_state
                .manifest
                .find_snapshot(b)
                .ok_or_else(|| MetaDbError::InvalidArgument(format!("unknown snapshot id {b}")))?;
            if a_entry.vol_ord != b_entry.vol_ord {
                return Err(MetaDbError::InvalidArgument(format!(
                    "cannot diff snapshots across volumes: {a} on vol {} vs {b} on vol {}",
                    a_entry.vol_ord, b_entry.vol_ord,
                )));
            }
            (
                a_entry.vol_ord,
                a_entry.l2p_shard_roots.clone(),
                b_entry.l2p_shard_roots.clone(),
            )
        };
        self.diff_roots(vol_ord, &a_roots, &b_roots)
    }

    /// Diff a snapshot against the owning volume's current tree.
    pub fn diff_with_current(&self, snap: SnapshotId) -> Result<Vec<DiffEntry>> {
        let _guard = self.snapshot_views.read();
        let (vol_ord, snap_roots) = {
            let manifest_state = self.manifest_state.lock();
            let entry = manifest_state
                .manifest
                .find_snapshot(snap)
                .ok_or_else(|| MetaDbError::InvalidArgument(format!("unknown snapshot id {snap}")))?;
            (entry.vol_ord, entry.l2p_shard_roots.clone())
        };

        let volume = self.volume(vol_ord)?;
        if snap_roots.len() != volume.shards.len() {
            return Err(MetaDbError::Corruption(format!(
                "snapshot {snap} has {} roots, volume {vol_ord} has {} shards",
                snap_roots.len(),
                volume.shards.len(),
            )));
        }
        let mut guards: Vec<MutexGuard<'_, PagedL2p>> =
            volume.shards.iter().map(|s| s.tree.lock()).collect();
        let mut out = Vec::new();
        for (tree, snap_root) in guards.iter_mut().zip(snap_roots.iter().copied()) {
            let current_root = tree.root();
            out.extend(tree.diff_subtrees(snap_root, current_root)?);
        }
        out.sort_unstable_by_key(DiffEntry::key);
        Ok(out)
    }

    /// Drop a snapshot. The drop is logged as `WalOp::DropSnapshot` so
    /// the page-refcount work (decref every page the snapshot shares
    /// with the current tree, free any page that hits rc=0) and the
    /// in-memory manifest update are atomic against process crash:
    /// after the WAL fsync, recovery replays the op and re-drives the
    /// work to completion.
    ///
    /// Serialisation:
    /// - `drop_gate.write()` — excludes every `commit_ops` path. The
    ///   rc-dependent plan walk relies on no concurrent `cow_for_write`
    ///   moving rcs out from under us.
    /// - `apply_gate.write()` — excludes `flush` / `take_snapshot`.
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
    /// natural [`flush`](Self::flush) captures the new snapshot list.
    pub fn drop_snapshot(&self, id: SnapshotId) -> Result<Option<DropReport>> {
        let _drop_guard = self.drop_gate.write();
        let _apply_guard = self.apply_gate.write();
        let _view_guard = self.snapshot_views.write();

        let entry = {
            let manifest_state = self.manifest_state.lock();
            let Some(entry) = manifest_state
                .manifest
                .snapshots
                .iter()
                .find(|e| e.id == id)
                .cloned()
            else {
                return Ok(None);
            };
            entry
        };
        // v6 SnapshotEntry no longer carries refcount state (Phase 6.5b
        // retired it), so there's nothing to assert about refcount here.

        // Commit 9: snapshots are per-volume, so page collection +
        // cache invalidation target the source volume only. The
        // entry's vol_ord is load-bearing here — `drop_volume` refuses
        // to drop a volume while any snapshot pins it, so this lookup
        // cannot miss in a well-formed manifest.
        let source_volume = self.volume(entry.vol_ord).map_err(|_| {
            MetaDbError::Corruption(format!(
                "drop_snapshot: snapshot {id} references unknown volume ord {}",
                entry.vol_ord,
            ))
        })?;
        if entry.l2p_shard_roots.len() != source_volume.shards.len() {
            return Err(MetaDbError::Corruption(format!(
                "drop_snapshot: snapshot {id} has {} roots but volume {} has {} shards",
                entry.l2p_shard_roots.len(),
                entry.vol_ord,
                source_volume.shards.len(),
            )));
        }
        // Lock ALL volumes' shards and refcount shards so we can flush +
        // refresh manifest before the decref cascade.
        //
        // Refreshing the manifest here is load-bearing: prior
        // `commit_ops` cows may have advanced each volume's root
        // without updating `manifest.volumes`. If we went straight to
        // the cascade and froze `rc(pid)=0` for any page that was
        // already cow'd away from by a live volume but is still
        // referenced by this snapshot, the on-disk manifest would
        // still list that freed pid as the volume's root — the next
        // open would then fail inside `open_l2p_shards` reading a
        // Free page. Commit a refreshed manifest (snapshot still
        // present) so reopen always finds current roots.
        let volumes_snap = self.volumes_snapshot();
        let mut l2p_guards = lock_all_l2p_shards_for(&volumes_snap);
        let mut refcount_guards = self.lock_all_refcount_shards();
        flush_locked_l2p_shards(&mut l2p_guards)?;
        self.flush_locked_refcount_shards(&mut refcount_guards)?;

        let checkpoint_lsn = *self.last_applied_lsn.lock();
        let dedup_generation = max_generation_from_two_groups(&l2p_guards, &refcount_guards);
        let dedup_update = {
            let mut mstate = self.manifest_state.lock();
            let dedup_update =
                self.prepare_dedup_manifest_update(&mut mstate.manifest, dedup_generation)?;
            self.refresh_manifest_from_locked(
                &mut mstate.manifest,
                &volumes_snap,
                &l2p_guards,
                &refcount_guards,
            )?;
            mstate.manifest.checkpoint_lsn = checkpoint_lsn;
            let manifest = mstate.manifest.clone();
            mstate.store.commit(&manifest)?;
            dedup_update
        };
        self.finish_dedup_manifest_update(dedup_update, dedup_generation)?;

        // Locate the source volume's shard range within l2p_guards.
        let mut source_start = 0usize;
        for vol in &volumes_snap {
            if vol.ord == entry.vol_ord {
                break;
            }
            source_start += vol.shards.len();
        }
        let source_end = source_start + source_volume.shards.len();

        let mut pages: Vec<PageId> = Vec::new();
        for (tree, &root) in l2p_guards[source_start..source_end]
            .iter_mut()
            .zip(entry.l2p_shard_roots.iter())
        {
            if root == crate::types::NULL_PAGE {
                continue;
            }
            pages.extend(tree.collect_drop_pages(root)?);
        }
        drop(l2p_guards);
        drop(refcount_guards);
        // NOTE on `entry.l2p_roots_page`: this SnapshotRoots page is
        // referenced only by the manifest's snapshot entry, so it
        // *logically* becomes unreferenced when we apply the drop.
        // We deliberately leave it alone here though: the on-disk
        // manifest still has the snapshot entry (no commit happens
        // inside drop_snapshot), and an older-than-expected open would
        // call `load_snapshot_roots` on it — turning it Free would
        // break decode and deadlock recovery. The page becomes a
        // genuine orphan only after the next flush persists a
        // snapshot-less manifest; `reclaim_orphan_pages` (run after
        // WAL replay in `Db::open`) picks it up from there.

        // Submit + apply inline without going through commit_ops'
        // cvar queue. We hold drop_gate.write + apply_gate.write, so
        // no one else can submit, and no other apply is in flight.
        // LSN ordering: the WAL writer assigns LSNs in submit order.
        // Under drop_gate.write, no concurrent submits have been
        // accepted (they'd be waiting on drop_gate.read), so our
        // submission gets the next LSN in sequence.
        let op = WalOp::DropSnapshot {
            id,
            pages: pages.clone(),
        };
        let body = encode_body(std::slice::from_ref(&op));
        let lsn = self.wal.submit(body)?;
        self.faults.inject(FaultPoint::CommitPostWalBeforeApply)?;

        // Block until every prior LSN has applied. Under our locks,
        // `last_applied_lsn` can only move forward when a commit
        // completes — and since we hold drop_gate.write, no new
        // commits have entered, so this wait is bounded by whatever
        // was in flight at the moment we took the gate.
        {
            let mut applied = self.last_applied_lsn.lock();
            while *applied + 1 < lsn {
                self.commit_cvar.wait(&mut applied);
            }
        }

        let volumes_map = self.volumes.read().clone();
        let outcome = apply_op_bare(
            &volumes_map,
            &self.refcount_shards,
            &self.dedup_index,
            &self.dedup_reverse,
            &self.page_store,
            lsn,
            &op,
        )?;
        self.faults
            .inject(FaultPoint::CommitPostApplyBeforeLsnBump)?;

        // Apply's page writes went straight through page_store; the
        // source volume's shards' PageBuf caches still hold stale
        // refcounts for anything apply touched. Invalidate every one
        // of those pids so the next cow_for_write / lookup pulls the
        // fresh bytes from disk. Other volumes' PageBufs never cached
        // these pages (COW ownership is per-tree), so skipping them is
        // safe.
        for &pid in &pages {
            self.page_cache.invalidate(pid);
            for shard in &source_volume.shards {
                shard.tree.lock().forget_page(pid);
            }
        }

        {
            let mut mstate = self.manifest_state.lock();
            mstate.manifest.snapshots.retain(|s| s.id != id);
        }

        {
            let mut applied = self.last_applied_lsn.lock();
            *applied = lsn;
            self.commit_cvar.notify_all();
        }

        let (freed_leaf_values, pages_freed) = match outcome {
            ApplyOutcome::DropSnapshot {
                freed_leaf_values,
                pages_freed,
            } => (freed_leaf_values, pages_freed),
            other => {
                return Err(MetaDbError::Corruption(format!(
                    "DropSnapshot apply returned unexpected outcome: {other:?}"
                )));
            }
        };

        Ok(Some(DropReport {
            snapshot_id: id,
            freed_leaf_values,
            pages_freed,
        }))
    }

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
    /// - `apply_gate.write()` — excludes `flush` / `take_snapshot` /
    ///   `drop_snapshot`.
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
            probe.volumes.push(VolumeEntry {
                ord,
                shard_count,
                l2p_shard_roots: vec![crate::types::NULL_PAGE; shard_count as usize]
                    .into_boxed_slice(),
                created_lsn: 0,
                flags: 0,
            });
            probe.check_encodable()?;
            (ord, shard_count)
        };

        let op = WalOp::CreateVolume { ord, shard_count };
        let body = encode_body(std::slice::from_ref(&op));
        let lsn = self.wal.submit(body)?;
        self.faults.inject(FaultPoint::CommitPostWalBeforeApply)?;

        // Under our two write gates no other commit is between submit
        // and apply, so last_applied_lsn + 1 == lsn already. The cvar
        // wait keeps the pattern symmetric with `drop_snapshot`.
        {
            let mut applied = self.last_applied_lsn.lock();
            while *applied + 1 < lsn {
                self.commit_cvar.wait(&mut applied);
            }
        }

        let (shards, roots) =
            apply_create_volume(&self.page_store, &self.page_cache, shard_count)?;
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
            mstate.manifest.volumes.push(VolumeEntry {
                ord,
                shard_count,
                l2p_shard_roots: roots,
                created_lsn: lsn,
                flags: 0,
            });
            mstate.manifest.next_volume_ord = ord
                .checked_add(1)
                .ok_or_else(|| MetaDbError::Corruption("volume ord overflow".into()))?;
        }

        {
            let mut applied = self.last_applied_lsn.lock();
            *applied = lsn;
            self.commit_cvar.notify_all();
        }

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
    /// - `apply_gate.write()` — excludes `flush` / `take_snapshot` /
    ///   `drop_snapshot` / `create_volume`.
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
        }

        let volume = match self.volumes.read().get(&vol_ord).cloned() {
            Some(v) => v,
            None => return Ok(None),
        };

        // Lock ALL volumes' shards + refcount shards so we can flush
        // them and later commit a refreshed manifest.
        let volumes_snap = self.volumes_snapshot();
        let mut l2p_guards = lock_all_l2p_shards_for(&volumes_snap);
        let mut refcount_guards = self.lock_all_refcount_shards();
        flush_locked_l2p_shards(&mut l2p_guards)?;
        self.flush_locked_refcount_shards(&mut refcount_guards)?;

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
        let dedup_generation = max_generation_from_two_groups(&l2p_guards, &refcount_guards);
        let dedup_update = {
            let mut mstate = self.manifest_state.lock();
            let dedup_update =
                self.prepare_dedup_manifest_update(&mut mstate.manifest, dedup_generation)?;
            self.refresh_manifest_from_locked(
                &mut mstate.manifest,
                &volumes_snap,
                &l2p_guards,
                &refcount_guards,
            )?;
            mstate.manifest.volumes.retain(|v| v.ord != vol_ord);
            mstate.manifest.checkpoint_lsn = checkpoint_lsn;
            let manifest = mstate.manifest.clone();
            mstate.store.commit(&manifest)?;
            dedup_update
        };
        self.finish_dedup_manifest_update(dedup_update, dedup_generation)?;
        drop(l2p_guards);
        drop(refcount_guards);

        let op = WalOp::DropVolume {
            ord: vol_ord,
            pages: pages.clone(),
        };
        let body = encode_body(std::slice::from_ref(&op));
        let lsn = self.wal.submit(body)?;
        self.faults.inject(FaultPoint::CommitPostWalBeforeApply)?;
        // Fault window specific to drop_volume: WAL record durable, no
        // page decref has touched disk yet. Recovery re-drives the full
        // cascade from the WAL op's inlined `pages` list.
        self.faults
            .inject(FaultPoint::DropVolumePostWalBeforeApply)?;
        {
            let mut applied = self.last_applied_lsn.lock();
            while *applied + 1 < lsn {
                self.commit_cvar.wait(&mut applied);
            }
        }

        let pages_freed = apply_drop_volume(&self.page_store, lsn, &pages)?;
        self.faults
            .inject(FaultPoint::CommitPostApplyBeforeLsnBump)?;

        // apply_drop_snapshot_pages wrote pages through page_store,
        // bypassing shard-local PageBuf + shared page_cache — invalidate
        // both so the next cow_for_write / lookup pulls fresh bytes.
        for &pid in &pages {
            self.page_cache.invalidate(pid);
            for shard in &volume.shards {
                shard.tree.lock().forget_page(pid);
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

        {
            let mut applied = self.last_applied_lsn.lock();
            *applied = lsn;
            self.commit_cvar.notify_all();
        }

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
    /// - `apply_gate.write()` — excludes flush / take_snapshot /
    ///   drop_snapshot.
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
        let _apply_guard = self.apply_gate.write();

        // Resolve the snapshot entry + allocate the new ord under the
        // manifest mutex so two concurrent clones can't hand out the
        // same ordinal.
        let (src_ord, src_shard_roots, new_ord) = {
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
            probe.volumes.push(VolumeEntry {
                ord: new_ord,
                shard_count: shard_count as u32,
                l2p_shard_roots: vec![crate::types::NULL_PAGE; shard_count].into_boxed_slice(),
                created_lsn: 0,
                flags: 0,
            });
            probe.check_encodable()?;
            (
                entry.vol_ord,
                entry.l2p_shard_roots.to_vec(),
                new_ord,
            )
        };

        let op = WalOp::CloneVolume {
            src_ord,
            new_ord,
            src_snap_id,
            src_shard_roots: src_shard_roots.clone(),
        };
        let body = encode_body(std::slice::from_ref(&op));
        let lsn = self.wal.submit(body)?;
        self.faults.inject(FaultPoint::CommitPostWalBeforeApply)?;

        // Under our two write gates no other commit sits between submit
        // and apply; the cvar wait is defensive and matches
        // `create_volume` / `drop_snapshot`.
        {
            let mut applied = self.last_applied_lsn.lock();
            while *applied + 1 < lsn {
                self.commit_cvar.wait(&mut applied);
            }
        }

        apply_clone_volume_incref(&self.page_store, &self.faults, lsn, &src_shard_roots)?;
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
                    shard.tree.lock().forget_page(pid);
                }
            }
        }
        self.faults
            .inject(FaultPoint::CommitPostApplyBeforeLsnBump)?;

        let (shards, actual_roots) = build_clone_volume_shards(
            &src_shard_roots,
            &self.page_store,
            &self.page_cache,
            lsn,
        )?;
        let shard_count = shards.len() as u32;

        {
            let mut volumes_map = self.volumes.write();
            if volumes_map.contains_key(&new_ord) {
                return Err(MetaDbError::Corruption(format!(
                    "clone_volume: ord {new_ord} already present"
                )));
            }
            volumes_map.insert(new_ord, Arc::new(Volume::new(new_ord, shards, lsn)));
        }

        {
            let mut mstate = self.manifest_state.lock();
            mstate.manifest.volumes.push(VolumeEntry {
                ord: new_ord,
                shard_count,
                l2p_shard_roots: actual_roots,
                created_lsn: lsn,
                flags: 0,
            });
            mstate.manifest.next_volume_ord = new_ord
                .checked_add(1)
                .ok_or_else(|| MetaDbError::Corruption("volume ord overflow".into()))?;
        }

        {
            let mut applied = self.last_applied_lsn.lock();
            *applied = lsn;
            self.commit_cvar.notify_all();
        }

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
    fn volumes_snapshot(&self) -> Vec<Arc<Volume>> {
        let mut vols: Vec<Arc<Volume>> = self.volumes.read().values().cloned().collect();
        vols.sort_by_key(|v| v.ord);
        vols
    }

    /// Clone out the bootstrap volume. Panics if it is missing — it is
    /// inserted at create / open time and Phase B never removes it.
    fn volume_zero(&self) -> Arc<Volume> {
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
    fn volume(&self, vol_ord: VolumeOrdinal) -> Result<Arc<Volume>> {
        self.volumes
            .read()
            .get(&vol_ord)
            .cloned()
            .ok_or_else(|| MetaDbError::InvalidArgument(format!("unknown volume ord {vol_ord}")))
    }

    fn refcount_shard_for(&self, pba: Pba) -> usize {
        debug_assert!(!self.refcount_shards.is_empty());
        (xxh3_64(&pba.to_be_bytes()) as usize) % self.refcount_shards.len()
    }

    fn prepare_dedup_manifest_update(
        &self,
        manifest: &mut Manifest,
        generation: Lsn,
    ) -> Result<DedupManifestUpdate> {
        // Any manifest commit that advances checkpoint_lsn must first
        // make the dedup memtables durable, otherwise replay would skip
        // WAL-applied rows that only existed in RAM.
        self.dedup_index.flush_memtable(generation)?;
        self.dedup_reverse.flush_memtable(generation)?;

        let old_dedup_heads = manifest.dedup_level_heads.to_vec();
        let old_dedup_reverse_heads = manifest.dedup_reverse_level_heads.to_vec();
        manifest.dedup_level_heads = self
            .dedup_index
            .persist_levels(generation)?
            .into_boxed_slice();
        manifest.dedup_reverse_level_heads = self
            .dedup_reverse
            .persist_levels(generation)?
            .into_boxed_slice();
        Ok(DedupManifestUpdate {
            old_dedup_heads,
            old_dedup_reverse_heads,
        })
    }

    fn finish_dedup_manifest_update(
        &self,
        update: DedupManifestUpdate,
        generation: Lsn,
    ) -> Result<()> {
        self.dedup_index
            .free_old_level_heads(&update.old_dedup_heads, generation)?;
        self.dedup_reverse
            .free_old_level_heads(&update.old_dedup_reverse_heads, generation)?;
        Ok(())
    }

    fn lock_all_refcount_shards(&self) -> Vec<MutexGuard<'_, BTree>> {
        self.refcount_shards
            .iter()
            .map(|shard| shard.tree.lock())
            .collect()
    }

    fn flush_locked_refcount_shards(&self, guards: &mut [MutexGuard<'_, BTree>]) -> Result<()> {
        for tree in guards {
            tree.flush()?;
        }
        Ok(())
    }

    /// Refresh manifest fields that mirror in-memory state.
    ///
    /// Does NOT touch `checkpoint_lsn` — that is the durable-WAL LSN
    /// cursor and is only ever advanced by code paths that have taken
    /// `apply_gate.write()` (flush / take_snapshot / drop_snapshot) and
    /// therefore have an authoritative reading of `last_applied_lsn`.
    fn refresh_manifest_from_locked(
        &self,
        manifest: &mut Manifest,
        volumes: &[Arc<Volume>],
        l2p_guards: &[MutexGuard<'_, PagedL2p>],
        refcount_guards: &[MutexGuard<'_, BTree>],
    ) -> Result<()> {
        refresh_manifest_entries(manifest, volumes, l2p_guards, refcount_guards)
    }

    fn current_generation(&self) -> Lsn {
        let volumes = self.volumes_snapshot();
        let l2p = lock_all_l2p_shards_for(&volumes);
        let refcount = self.lock_all_refcount_shards();
        max_generation_from_two_groups(&l2p, &refcount)
    }

    fn collect_range_for_roots(
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
            let mut tree = shard.tree.lock();
            items.extend(
                tree.range_at(root, range.clone())?
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        items.sort_unstable_by_key(|(k, _)| *k);
        Ok(DbRangeIter::new(items))
    }

    fn diff_roots(
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
            let mut tree = shard.tree.lock();
            out.extend(tree.diff_subtrees(a_root, b_root)?);
        }
        out.sort_unstable_by_key(DiffEntry::key);
        Ok(out)
    }
}

/// Result of [`Db::drop_snapshot`].
#[derive(Clone, Debug)]
pub struct DropReport {
    /// Id of the snapshot that was dropped.
    pub snapshot_id: SnapshotId,
    /// Every value stored in leaves that were uniquely owned by this
    /// snapshot.
    pub freed_leaf_values: Vec<L2pValue>,
    /// Number of metadb pages released back to the page store.
    pub pages_freed: usize,
}

/// Result of [`Db::drop_volume`].
#[derive(Clone, Debug)]
pub struct DropVolumeReport {
    /// Ordinal of the volume that was dropped.
    pub vol_ord: VolumeOrdinal,
    /// Number of metadb pages released back to the page store.
    pub pages_freed: usize,
}

/// Read-only view of the tree as it existed when a snapshot was taken.
pub struct SnapshotView<'a> {
    db: &'a Db,
    entry: SnapshotEntry,
    _guard: RwLockReadGuard<'a, ()>,
}

impl<'a> SnapshotView<'a> {
    /// Snapshot id this view is bound to.
    pub fn id(&self) -> SnapshotId {
        self.entry.id
    }

    /// LSN at which the snapshot was taken.
    pub fn created_lsn(&self) -> Lsn {
        self.entry.created_lsn
    }

    /// Ordinal of the volume this snapshot captures.
    pub fn vol_ord(&self) -> VolumeOrdinal {
        self.entry.vol_ord
    }

    /// Point lookup as of the snapshot's LSN. Routes through the
    /// owning volume's shard layout — commit 6 always stamps
    /// `vol_ord = 0`, commit 9 switches that to the real source.
    pub fn get(&self, lba: Lba) -> Result<Option<L2pValue>> {
        let volume = self.db.volume(self.entry.vol_ord)?;
        let sid = shard_for_key_l2p(&volume.shards, lba);
        let mut tree = volume.shards[sid].tree.lock();
        tree.get_at(self.entry.l2p_shard_roots[sid], lba)
    }

    /// Range scan as of the snapshot's LSN.
    pub fn range<R: RangeBounds<Lba>>(&self, range: R) -> Result<DbRangeIter> {
        self.db.collect_range_for_roots(
            self.entry.vol_ord,
            &self.entry.l2p_shard_roots,
            OwnedRange::new(range),
        )
    }
}

/// Lock every L2P shard mutex across the given volume set, in
/// (`volumes` order, shard index) order. Callers that reach multiple
/// volumes pass the sorted output of `Db::volumes_snapshot` so every
/// caller agrees on a single lock order, preventing the shard mutexes
/// from deadlocking against each other. Volumes are passed in as a slice
/// of `Arc<Volume>` so the clones keep the mutexes alive for the guard
/// lifetime.
fn lock_all_l2p_shards_for<'v>(volumes: &'v [Arc<Volume>]) -> Vec<MutexGuard<'v, PagedL2p>> {
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
fn refresh_manifest_entries(
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

fn flush_locked_l2p_shards(guards: &mut [MutexGuard<'_, PagedL2p>]) -> Result<()> {
    for tree in guards {
        tree.flush()?;
    }
    Ok(())
}

fn validate_shard_count(shards_per_partition: u32) -> Result<usize> {
    let shard_count = usize::try_from(shards_per_partition)
        .map_err(|_| MetaDbError::InvalidArgument("shard count does not fit usize".into()))?;
    if shard_count == 0 {
        return Err(MetaDbError::InvalidArgument(
            "shards_per_partition must be greater than zero".into(),
        ));
    }
    Ok(shard_count)
}

fn create_shards(
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

fn open_shards(
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

fn create_l2p_shards(
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

fn open_l2p_shards(
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

fn lsm_config_from_cfg(cfg: &Config) -> LsmConfig {
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

fn max_generation_from_locked(guards: &[MutexGuard<'_, BTree>]) -> Lsn {
    guards
        .iter()
        .map(|tree| tree.next_generation())
        .max()
        .unwrap_or(0)
}

fn max_generation_from_locked_l2p(guards: &[MutexGuard<'_, PagedL2p>]) -> Lsn {
    guards
        .iter()
        .map(|tree| tree.next_generation())
        .max()
        .unwrap_or(0)
}

fn max_generation_from_two_groups(
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

/// Apply one [`WalOp`] to raw `Db` state. Used by both the live commit
/// path (through `self.apply_op`) and the WAL-replay path (before
/// `Self` exists). Takes individual references so it can run against
/// locally-constructed state during `open`. Private to this module
/// because `Shard` is.
///
/// `lsn` is the WAL record LSN the op is applying at; used only by
/// `DropSnapshot` to stamp page generations for idempotent replay.
/// Callers that don't have an exact LSN (e.g. `replay_into`, which
/// passes the enclosing record's LSN) just pass the best available
/// value — the only correctness requirement is that `lsn` strictly
/// increases across apply invocations, which is already guaranteed by
/// WAL LSN monotonicity.
///
/// `DropSnapshot` mutates the in-memory manifest; callers that need
/// that side effect must handle the `manifest.snapshots.retain(...)`
/// themselves after calling this function. This split keeps
/// `apply_op_bare` usable in the replay path (which owns a bare
/// `Manifest`) and the live path (which owns a `Mutex<ManifestState>`).
fn apply_op_bare(
    volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
    refcount_shards: &[Shard],
    dedup_index: &Lsm,
    dedup_reverse: &Lsm,
    page_store: &Arc<PageStore>,
    lsn: Lsn,
    op: &WalOp,
) -> Result<ApplyOutcome> {
    match op {
        WalOp::L2pPut {
            vol_ord,
            lba,
            value,
        } => {
            let volume = volumes.get(vol_ord).ok_or_else(|| {
                MetaDbError::Corruption(format!("L2pPut for unknown volume ord {vol_ord}"))
            })?;
            let sid = shard_for_key_l2p(&volume.shards, *lba);
            let mut tree = volume.shards[sid].tree.lock();
            let prev = tree.insert_at_lsn(*lba, *value, lsn)?;
            Ok(ApplyOutcome::L2pPrev(prev))
        }
        WalOp::L2pDelete { vol_ord, lba } => {
            let volume = volumes.get(vol_ord).ok_or_else(|| {
                MetaDbError::Corruption(format!("L2pDelete for unknown volume ord {vol_ord}"))
            })?;
            let sid = shard_for_key_l2p(&volume.shards, *lba);
            let mut tree = volume.shards[sid].tree.lock();
            let prev = tree.delete_at_lsn(*lba, lsn)?;
            Ok(ApplyOutcome::L2pPrev(prev))
        }
        WalOp::DedupPut { hash, value } => {
            dedup_index.put(*hash, *value);
            Ok(ApplyOutcome::Dedup)
        }
        WalOp::DedupDelete { hash } => {
            dedup_index.delete(*hash);
            Ok(ApplyOutcome::Dedup)
        }
        WalOp::DedupReversePut { pba, hash } => {
            let (key, value) = encode_reverse_entry(*pba, hash);
            dedup_reverse.put(key, value);
            Ok(ApplyOutcome::Dedup)
        }
        WalOp::DedupReverseDelete { pba, hash } => {
            let (key, _) = encode_reverse_entry(*pba, hash);
            dedup_reverse.delete(key);
            Ok(ApplyOutcome::Dedup)
        }
        WalOp::Incref { pba, delta } => {
            let sid = shard_for_key(refcount_shards, *pba);
            let mut tree = refcount_shards[sid].tree.lock();
            let current = tree.get(*pba)?.unwrap_or(0);
            let new = current.checked_add(*delta).ok_or_else(|| {
                MetaDbError::InvalidArgument(format!("refcount overflow for pba {pba}"))
            })?;
            if new != 0 {
                tree.insert(*pba, new)?;
            }
            Ok(ApplyOutcome::RefcountNew(new))
        }
        WalOp::Decref { pba, delta } => {
            let sid = shard_for_key(refcount_shards, *pba);
            let mut tree = refcount_shards[sid].tree.lock();
            let current = tree.get(*pba)?.unwrap_or(0);
            let new = current.checked_sub(*delta).ok_or_else(|| {
                MetaDbError::InvalidArgument(format!(
                    "decref underflow for pba {pba}: {current} - {delta}",
                ))
            })?;
            if new == 0 {
                tree.delete(*pba)?;
            } else {
                tree.insert(*pba, new)?;
            }
            Ok(ApplyOutcome::RefcountNew(new))
        }
        WalOp::DropSnapshot { id: _, pages } => {
            apply_drop_snapshot_pages(page_store, lsn, pages)
        }
        // Phase 7 per-volume lifecycle ops: decodable since Phase A, but
        // their apply semantics land with commit 8/9. Commit 6 still
        // expects to see `vol_ord = 0` on L2P ops only; any of these
        // three tags in the WAL means either a mixed-binary recovery
        // attempt or a logic bug in the caller.
        WalOp::CreateVolume { ord, .. }
        | WalOp::DropVolume { ord, .. }
        | WalOp::CloneVolume { new_ord: ord, .. } => Err(MetaDbError::Corruption(format!(
            "Phase 7 volume-lifecycle WAL op for ord {ord} hit the commit-6 apply path; \
             commit 8/9 implements these — this binary is too old to replay it"
        ))),
    }
}

/// Allocate a fresh shard group for a `CreateVolume` apply. Delegates
/// to [`create_l2p_shards`]; kept separate so the Db public API and
/// the recovery replay closure share one call site.
fn apply_create_volume(
    page_store: &Arc<PageStore>,
    page_cache: &Arc<PageCache>,
    shard_count: u32,
) -> Result<(Vec<L2pShard>, Box<[PageId]>)> {
    let n = validate_shard_count(shard_count)?;
    create_l2p_shards(page_store.clone(), page_cache.clone(), n)
}

/// Apply a `DropVolume` op's page-decref cascade. Reuses
/// [`apply_drop_snapshot_pages`]; `DropVolume` has the same
/// per-page semantics (decref, free at rc=0, idempotent via
/// `page.generation >= lsn`) and just doesn't need the freed-leaf-values
/// vec the snapshot path surfaces in its report.
fn apply_drop_volume(
    page_store: &Arc<PageStore>,
    lsn: Lsn,
    pages: &[PageId],
) -> Result<usize> {
    match apply_drop_snapshot_pages(page_store, lsn, pages)? {
        ApplyOutcome::DropSnapshot { pages_freed, .. } => Ok(pages_freed),
        other => Err(MetaDbError::Corruption(format!(
            "apply_drop_volume: unexpected outcome {other:?}"
        ))),
    }
}

/// Increment the on-disk refcount of each shard root that a cloned
/// volume pins. Idempotent across replays: pages already stamped with
/// `page.generation >= lsn` are skipped (same guard pattern
/// [`apply_drop_snapshot_pages`] uses). `NULL_PAGE` roots — empty
/// source shards — are ignored because the clone materialises fresh
/// empty trees for those shards (see [`build_clone_volume_shards`]).
fn apply_clone_volume_incref(
    page_store: &Arc<PageStore>,
    faults: &FaultController,
    lsn: Lsn,
    src_shard_roots: &[PageId],
) -> Result<()> {
    for (idx, &pid) in src_shard_roots.iter().enumerate() {
        if pid == crate::types::NULL_PAGE {
            continue;
        }
        let mut page = page_store.read_page_unchecked(pid)?;
        page.verify(pid)?;
        let header = page.header()?;
        if header.generation >= lsn {
            // Already incref'd by a prior apply of this same CloneVolume
            // op (replay-after-crash case); skip.
            continue;
        }
        let new_rc = header.refcount.checked_add(1).ok_or_else(|| {
            MetaDbError::Corruption(format!(
                "clone_volume: refcount overflow on source root page {pid}"
            ))
        })?;
        page.set_refcount(new_rc);
        page.set_generation(lsn);
        page.seal();
        page_store.write_page(pid, &page)?;
        // Fault injection window: fires after the first root is durably
        // incref'd but before subsequent ones. Recovery's generation-stamp
        // guard skips the pre-fault root and completes the rest.
        if idx == 0 {
            faults.inject(FaultPoint::CloneVolumeMidIncref)?;
        }
    }
    page_store.sync()?;
    Ok(())
}

/// Build the new volume's shard group for a clone. Each source root
/// becomes the initial root of a fresh [`PagedL2p`]; empty source
/// shards (`NULL_PAGE` root) get a freshly-allocated empty leaf so the
/// tree is always operable. Caller must have already incref'd the
/// non-null roots via [`apply_clone_volume_incref`].
fn build_clone_volume_shards(
    src_shard_roots: &[PageId],
    page_store: &Arc<PageStore>,
    page_cache: &Arc<PageCache>,
    created_lsn: Lsn,
) -> Result<(Vec<L2pShard>, Box<[PageId]>)> {
    let mut shards = Vec::with_capacity(src_shard_roots.len());
    let mut actual_roots = Vec::with_capacity(src_shard_roots.len());
    for &root in src_shard_roots {
        let tree = if root == crate::types::NULL_PAGE {
            PagedL2p::create_with_cache(page_store.clone(), page_cache.clone())?
        } else {
            PagedL2p::open_with_cache(
                page_store.clone(),
                page_cache.clone(),
                root,
                created_lsn + 1,
            )?
        };
        actual_roots.push(tree.root());
        shards.push(L2pShard {
            tree: Mutex::new(tree),
        });
    }
    Ok((shards, actual_roots.into_boxed_slice()))
}

/// Core of the `DropSnapshot` apply. Iterates `pages`, decrements each
/// page's refcount by 1, stamps `generation = lsn`, and frees any page
/// that hits rc=0. Idempotent on replay via the generation check.
fn apply_drop_snapshot_pages(
    page_store: &Arc<PageStore>,
    lsn: Lsn,
    pages: &[PageId],
) -> Result<ApplyOutcome> {
    use crate::page::PageType;

    let mut freed_leaf_values: Vec<L2pValue> = Vec::new();
    let mut pages_freed: usize = 0;

    for &pid in pages {
        let mut page = page_store.read_page_unchecked(pid)?;
        // verify before we do anything; `read_page_unchecked` skipped it
        page.verify(pid)?;
        let header = page.header()?;
        if header.generation >= lsn {
            // already processed by a prior apply of this same DropSnapshot
            // (replay-after-crash case); skip to keep the overall apply
            // idempotent.
            if header.page_type == PageType::Free {
                // count it so the outcome is stable across replays — but
                // skip the write.
                pages_freed += 1;
            }
            continue;
        }
        let rc = header.refcount;
        if rc == 0 {
            return Err(MetaDbError::Corruption(format!(
                "DropSnapshot apply: page {pid} already at refcount 0"
            )));
        }
        let new_rc = rc - 1;
        if new_rc == 0 {
            if matches!(header.page_type, PageType::PagedLeaf) {
                for i in 0..crate::paged::format::LEAF_ENTRY_COUNT {
                    if crate::paged::format::leaf_bit_set(&page, i) {
                        freed_leaf_values.push(crate::paged::format::leaf_value_at(&page, i));
                    }
                }
            }
            // free_idempotent stamps generation=lsn via the Free header.
            let pushed = page_store.free_idempotent(pid, lsn)?;
            if pushed {
                pages_freed += 1;
            }
        } else {
            page.set_refcount(new_rc);
            page.set_generation(lsn);
            page.seal();
            page_store.write_page(pid, &page)?;
        }
    }

    page_store.sync()?;

    Ok(ApplyOutcome::DropSnapshot {
        freed_leaf_values,
        pages_freed,
    })
}

fn shard_for_key(shards: &[Shard], key: u64) -> usize {
    (xxh3_64(&key.to_be_bytes()) as usize) % shards.len()
}

fn shard_for_key_l2p(shards: &[L2pShard], key: u64) -> usize {
    (xxh3_64(&key.to_be_bytes()) as usize) % shards.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn v(n: u8) -> L2pValue {
        let mut x = [0u8; 28];
        x[0] = n;
        L2pValue(x)
    }

    fn mk_db() -> (TempDir, Db) {
        let dir = TempDir::new().unwrap();
        let db = Db::create(dir.path()).unwrap();
        (dir, db)
    }

    fn mk_db_with_shards(shards: u32) -> (TempDir, Db) {
        let dir = TempDir::new().unwrap();
        let mut cfg = Config::new(dir.path());
        cfg.shards_per_partition = shards;
        let db = Db::create_with_config(cfg).unwrap();
        (dir, db)
    }

    fn mk_db_with_cache_bytes(page_cache_bytes: u64) -> (TempDir, Db) {
        let dir = TempDir::new().unwrap();
        let mut cfg = Config::new(dir.path());
        cfg.page_cache_bytes = page_cache_bytes;
        let db = Db::create_with_config(cfg).unwrap();
        (dir, db)
    }

    #[test]
    fn fresh_db_is_empty() {
        let (_d, db) = mk_db();
        assert_eq!(db.get(0,42).unwrap(), None);
        assert!(db.snapshots().is_empty());
        assert_eq!(db.manifest().next_snapshot_id, 1);
    }

    #[test]
    fn create_with_config_uses_requested_shards() {
        let (_d, db) = mk_db_with_shards(4);
        assert_eq!(db.shard_count(), 4);
        let manifest = db.manifest();
        assert_eq!(manifest.refcount_shard_roots.len(), 4);
        let boot = manifest
            .volumes
            .iter()
            .find(|v| v.ord == BOOTSTRAP_VOLUME_ORD)
            .expect("bootstrap volume entry present");
        assert_eq!(boot.shard_count, 4);
        assert_eq!(boot.l2p_shard_roots.len(), 4);
    }

    #[test]
    fn insert_get_round_trip() {
        let (_d, db) = mk_db();
        db.insert(0,10, v(7)).unwrap();
        assert_eq!(db.get(0,10).unwrap(), Some(v(7)));
    }

    #[test]
    fn flush_persists_tree_state_via_manifest() {
        let dir = TempDir::new().unwrap();
        {
            let db = Db::create(dir.path()).unwrap();
            for i in 0u64..500 {
                db.insert(0,i, v(i as u8)).unwrap();
            }
            db.flush().unwrap();
        }
        let db = Db::open(dir.path()).unwrap();
        for i in 0u64..500 {
            assert_eq!(db.get(0,i).unwrap(), Some(v(i as u8)));
        }
    }

    #[test]
    fn take_snapshot_assigns_monotonic_ids() {
        let (_d, db) = mk_db();
        let a = db.take_snapshot(0).unwrap();
        let b = db.take_snapshot(0).unwrap();
        let c = db.take_snapshot(0).unwrap();
        assert_eq!(a, 1);
        assert_eq!(b, 2);
        assert_eq!(c, 3);
        assert_eq!(db.snapshots().len(), 3);
    }

    #[test]
    fn snapshot_view_sees_state_at_take_time() {
        let (_d, db) = mk_db();
        for i in 0u64..100 {
            db.insert(0,i, v(1)).unwrap();
        }
        let snap = db.take_snapshot(0).unwrap();

        for i in 0u64..100 {
            db.insert(0,i, v(2)).unwrap();
        }
        db.insert(0,999, v(9)).unwrap();
        db.delete(0,50).unwrap();

        assert_eq!(db.get(0,0).unwrap(), Some(v(2)));
        assert_eq!(db.get(0,50).unwrap(), None);
        assert_eq!(db.get(0,999).unwrap(), Some(v(9)));

        let view = db.snapshot_view(snap).unwrap();
        for i in 0u64..100 {
            assert_eq!(view.get(i).unwrap(), Some(v(1)));
        }
        assert_eq!(view.get(999).unwrap(), None);
    }

    #[test]
    fn snapshot_view_range_scan() {
        let (_d, db) = mk_db();
        for i in 0u64..50 {
            db.insert(0,i, v(i as u8)).unwrap();
        }
        let snap = db.take_snapshot(0).unwrap();
        for i in 0u64..50 {
            db.insert(0,i, v(99)).unwrap();
        }

        let view = db.snapshot_view(snap).unwrap();
        let items: Vec<(u64, L2pValue)> = view
            .range(10u64..20)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        for (i, (k, val)) in items.iter().enumerate() {
            assert_eq!(*k, 10 + i as u64);
            assert_eq!(*val, v((10 + i) as u8));
        }
    }

    #[test]
    fn snapshot_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let snap_id = {
            let db = Db::create(dir.path()).unwrap();
            for i in 0u64..200 {
                db.insert(0,i, v(1)).unwrap();
            }
            let id = db.take_snapshot(0).unwrap();
            for i in 0u64..200 {
                db.insert(0,i, v(2)).unwrap();
            }
            db.flush().unwrap();
            id
        };

        let db = Db::open(dir.path()).unwrap();
        let snaps = db.snapshots();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].id, snap_id);

        let view = db.snapshot_view(snap_id).unwrap();
        for i in 0u64..200 {
            assert_eq!(view.get(i).unwrap(), Some(v(1)));
        }
        for i in 0u64..200 {
            assert_eq!(db.get(0,i).unwrap(), Some(v(2)));
        }
    }

    #[test]
    fn snapshot_view_missing_id_returns_none() {
        let (_d, db) = mk_db();
        assert!(db.snapshot_view(999).is_none());
    }

    #[test]
    fn diff_detects_added_removed_changed() {
        let (_d, db) = mk_db();
        for i in 0u64..10 {
            db.insert(0,i, v(1)).unwrap();
        }
        let a = db.take_snapshot(0).unwrap();

        db.insert(0,5, v(99)).unwrap();
        db.delete(0,3).unwrap();
        db.insert(0,42, v(7)).unwrap();

        let b = db.take_snapshot(0).unwrap();
        let diff = db.diff(a, b).unwrap();
        assert_eq!(diff.len(), 3);
        match diff[0] {
            DiffEntry::RemovedInB { key: 3, old } => assert_eq!(old, v(1)),
            ref other => panic!("{other:?}"),
        }
        match diff[1] {
            DiffEntry::Changed { key: 5, old, new } => {
                assert_eq!(old, v(1));
                assert_eq!(new, v(99));
            }
            ref other => panic!("{other:?}"),
        }
        match diff[2] {
            DiffEntry::AddedInB { key: 42, new } => assert_eq!(new, v(7)),
            ref other => panic!("{other:?}"),
        }
    }

    #[test]
    fn diff_with_current_reflects_unsaved_writes() {
        let (_d, db) = mk_db();
        for i in 0u64..10 {
            db.insert(0,i, v(1)).unwrap();
        }
        let a = db.take_snapshot(0).unwrap();
        db.insert(0,100, v(5)).unwrap();
        let diff = db.diff_with_current(a).unwrap();
        assert_eq!(diff.len(), 1);
        match diff[0] {
            DiffEntry::AddedInB { key: 100, new } => assert_eq!(new, v(5)),
            ref other => panic!("{other:?}"),
        }
    }

    #[test]
    fn drop_snapshot_returns_none_for_unknown_id() {
        let (_d, db) = mk_db();
        assert!(db.drop_snapshot(999).unwrap().is_none());
    }

    #[test]
    fn drop_snapshot_reclaims_uniquely_owned_pages() {
        let (_d, db) = mk_db();
        for i in 0u64..1000 {
            db.insert(0,i, v(1)).unwrap();
        }
        db.flush().unwrap();
        let free_before_snap = db.high_water();
        let s = db.take_snapshot(0).unwrap();

        for i in 0u64..1000 {
            db.insert(0,i, v(2)).unwrap();
        }
        db.flush().unwrap();
        let hw_after_writes = db.high_water();
        assert!(hw_after_writes > free_before_snap);

        let report = db.drop_snapshot(s).unwrap().unwrap();
        assert!(report.pages_freed > 0);
        assert_eq!(report.freed_leaf_values.len(), 1000);
        assert!(report.freed_leaf_values.iter().all(|val| *val == v(1)));
        for i in 0u64..1000 {
            assert_eq!(db.get(0,i).unwrap(), Some(v(2)));
        }
    }

    #[test]
    fn multiple_snapshots_isolated() {
        let (_d, db) = mk_db();
        for i in 0u64..20 {
            db.insert(0,i, v(1)).unwrap();
        }
        let s1 = db.take_snapshot(0).unwrap();
        for i in 0u64..20 {
            db.insert(0,i, v(2)).unwrap();
        }
        let s2 = db.take_snapshot(0).unwrap();
        for i in 0u64..20 {
            db.insert(0,i, v(3)).unwrap();
        }

        {
            let v1 = db.snapshot_view(s1).unwrap();
            assert_eq!(v1.get(5).unwrap(), Some(v(1)));
        }
        {
            let v2 = db.snapshot_view(s2).unwrap();
            assert_eq!(v2.get(5).unwrap(), Some(v(2)));
        }
        assert_eq!(db.get(0,5).unwrap(), Some(v(3)));
    }

    // -------- phase 7 commit 8: volume lifecycle --------

    #[test]
    fn fresh_db_volumes_lists_only_bootstrap() {
        let (_d, db) = mk_db();
        assert_eq!(db.volumes(), vec![BOOTSTRAP_VOLUME_ORD]);
        assert_eq!(db.manifest().next_volume_ord, BOOTSTRAP_VOLUME_ORD + 1);
    }

    #[test]
    fn create_volume_assigns_monotonic_ords() {
        let (_d, db) = mk_db();
        let a = db.create_volume().unwrap();
        let b = db.create_volume().unwrap();
        let c = db.create_volume().unwrap();
        assert_eq!((a, b, c), (1, 2, 3));
        assert_eq!(db.volumes(), vec![0, 1, 2, 3]);
        let m = db.manifest();
        assert_eq!(m.next_volume_ord, 4);
        assert_eq!(m.volumes.len(), 4);
    }

    #[test]
    fn create_volume_respects_max_volumes() {
        let dir = TempDir::new().unwrap();
        let mut cfg = Config::new(dir.path());
        cfg.max_volumes = 2; // bootstrap + one
        let db = Db::create_with_config(cfg).unwrap();
        let _a = db.create_volume().unwrap();
        match db.create_volume().unwrap_err() {
            MetaDbError::InvalidArgument(msg) => {
                assert!(msg.contains("max_volumes"), "msg={msg}");
            }
            e => panic!("unexpected error {e:?}"),
        }
    }

    #[test]
    fn new_volume_is_isolated_from_bootstrap() {
        let (_d, db) = mk_db();
        let ord = db.create_volume().unwrap();
        db.insert(0, 7, v(1)).unwrap();
        db.insert(ord, 7, v(2)).unwrap();
        assert_eq!(db.get(0, 7).unwrap(), Some(v(1)));
        assert_eq!(db.get(ord, 7).unwrap(), Some(v(2)));
    }

    #[test]
    fn drop_volume_bootstrap_refused() {
        let (_d, db) = mk_db();
        match db.drop_volume(0).unwrap_err() {
            MetaDbError::InvalidArgument(msg) => {
                assert!(msg.contains("bootstrap"), "msg={msg}");
            }
            e => panic!("unexpected error {e:?}"),
        }
        // The bootstrap volume is still live.
        assert_eq!(db.volumes(), vec![0]);
    }

    #[test]
    fn drop_volume_unknown_returns_none() {
        let (_d, db) = mk_db();
        assert!(db.drop_volume(99).unwrap().is_none());
    }

    #[test]
    fn drop_volume_removes_ord_from_map_and_manifest() {
        let (_d, db) = mk_db();
        let ord = db.create_volume().unwrap();
        for i in 0u64..32 {
            db.insert(ord, i, v(i as u8)).unwrap();
        }
        let report = db.drop_volume(ord).unwrap().unwrap();
        assert_eq!(report.vol_ord, ord);
        assert!(report.pages_freed > 0);
        assert_eq!(db.volumes(), vec![0]);
        assert!(!db.manifest().volumes.iter().any(|v| v.ord == ord));
        // Reads against the dropped ord surface as InvalidArgument.
        match db.get(ord, 0).unwrap_err() {
            MetaDbError::InvalidArgument(_) => {}
            e => panic!("unexpected error {e:?}"),
        }
    }

    #[test]
    fn volume_lifecycle_survives_reopen_without_flush() {
        let dir = TempDir::new().unwrap();
        let ord;
        {
            let db = Db::create(dir.path()).unwrap();
            ord = db.create_volume().unwrap();
            db.insert(ord, 100, v(42)).unwrap();
            // No flush — rely on WAL replay.
        }
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.volumes(), vec![0, ord]);
        assert_eq!(db.get(ord, 100).unwrap(), Some(v(42)));
        assert_eq!(db.manifest().next_volume_ord, ord + 1);
    }

    #[test]
    fn drop_volume_lifecycle_survives_reopen_without_flush() {
        let dir = TempDir::new().unwrap();
        {
            let db = Db::create(dir.path()).unwrap();
            let ord = db.create_volume().unwrap();
            db.insert(ord, 5, v(7)).unwrap();
            db.drop_volume(ord).unwrap().unwrap();
        }
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.volumes(), vec![0]);
        // next_volume_ord must stay bumped past the dropped ord.
        assert!(db.manifest().next_volume_ord >= 2);
    }

    #[test]
    fn drop_snapshot_lifecycle_survives_reopen_without_flush() {
        // Drop path commits manifest (with refreshed volume roots, snapshot
        // still present) before WAL submit + apply; apply is idempotent via
        // `page.generation >= lsn`; Db::open replays the DropSnapshot and
        // removes the snapshot entry in the replay closure before
        // `reclaim_orphan_pages` runs. Together these close the
        // crash-without-flush window that previously forced
        // `db_volume_proptest.rs::Op::Reopen` to pre-flush.
        let dir = TempDir::new().unwrap();
        {
            let db = Db::create(dir.path()).unwrap();
            for lba in 0u64..16 {
                db.insert(0, lba, v(lba as u8)).unwrap();
            }
            let snap = db.take_snapshot(0).unwrap();
            // Cow the whole tree so the snapshot holds roots the live
            // volume no longer owns — exactly the shape where a late
            // manifest refresh on the snapshot entry would see Free pages.
            for lba in 0u64..16 {
                db.insert(0, lba, v((lba + 1) as u8)).unwrap();
            }
            db.drop_snapshot(snap).unwrap().unwrap();
            // No flush — rely on WAL replay + the drop-path's pre-apply
            // manifest commit.
        }
        let db = Db::open(dir.path()).unwrap();
        assert!(db.snapshots().is_empty());
        for lba in 0u64..16 {
            assert_eq!(db.get(0, lba).unwrap(), Some(v((lba + 1) as u8)));
        }
    }

    #[test]
    fn create_volume_persists_through_flush_and_reopen() {
        let dir = TempDir::new().unwrap();
        let ord;
        {
            let db = Db::create(dir.path()).unwrap();
            ord = db.create_volume().unwrap();
            db.insert(ord, 1, v(9)).unwrap();
            db.flush().unwrap();
        }
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.volumes(), vec![0, ord]);
        assert_eq!(db.get(ord, 1).unwrap(), Some(v(9)));
    }

    // -------- phase 7 commit 9: per-volume snapshot --------

    #[test]
    fn take_snapshot_stamps_vol_ord_on_entry() {
        let (_d, db) = mk_db();
        let ord = db.create_volume().unwrap();
        db.insert(ord, 1, v(7)).unwrap();
        let snap = db.take_snapshot(ord).unwrap();
        let entry = db
            .snapshots()
            .into_iter()
            .find(|s| s.id == snap)
            .unwrap();
        assert_eq!(entry.vol_ord, ord);
        // Incref happened on exactly the target volume's shards.
        assert_eq!(entry.l2p_shard_roots.len(), db.shard_count());
    }

    #[test]
    fn take_snapshot_unknown_vol_ord_is_invalid_argument() {
        let (_d, db) = mk_db();
        match db.take_snapshot(99).unwrap_err() {
            MetaDbError::InvalidArgument(msg) => assert!(msg.contains("unknown volume")),
            e => panic!("unexpected error {e:?}"),
        }
    }

    #[test]
    fn snapshots_for_filters_by_vol_ord() {
        let (_d, db) = mk_db();
        let a = db.create_volume().unwrap();
        let b = db.create_volume().unwrap();
        db.insert(a, 1, v(1)).unwrap();
        db.insert(b, 2, v(2)).unwrap();

        let s_a = db.take_snapshot(a).unwrap();
        let s_b = db.take_snapshot(b).unwrap();
        let s_boot = db.take_snapshot(0).unwrap();

        let on_a: Vec<_> = db.snapshots_for(a).into_iter().map(|e| e.id).collect();
        let on_b: Vec<_> = db.snapshots_for(b).into_iter().map(|e| e.id).collect();
        let on_boot: Vec<_> = db.snapshots_for(0).into_iter().map(|e| e.id).collect();

        assert_eq!(on_a, vec![s_a]);
        assert_eq!(on_b, vec![s_b]);
        assert_eq!(on_boot, vec![s_boot]);
        assert_eq!(db.snapshots_for(99), Vec::<SnapshotEntry>::new());
        assert_eq!(db.snapshots().len(), 3);
    }

    #[test]
    fn snapshot_view_reads_from_target_volume() {
        let (_d, db) = mk_db();
        let a = db.create_volume().unwrap();
        let b = db.create_volume().unwrap();
        db.insert(a, 42, v(1)).unwrap();
        db.insert(b, 42, v(2)).unwrap();
        let snap_a = db.take_snapshot(a).unwrap();
        // Subsequent writes on either volume must not affect the snapshot.
        db.insert(a, 42, v(9)).unwrap();
        db.insert(b, 42, v(9)).unwrap();
        let view = db.snapshot_view(snap_a).unwrap();
        assert_eq!(view.vol_ord(), a);
        assert_eq!(view.get(42).unwrap(), Some(v(1)));
        // Current state reflects the post-snapshot write.
        assert_eq!(db.get(a, 42).unwrap(), Some(v(9)));
    }

    #[test]
    fn drop_snapshot_of_one_volume_leaves_others_intact() {
        let (_d, db) = mk_db();
        let a = db.create_volume().unwrap();
        let b = db.create_volume().unwrap();
        for i in 0u64..16 {
            db.insert(a, i, v(1)).unwrap();
            db.insert(b, i, v(2)).unwrap();
        }
        let snap_a = db.take_snapshot(a).unwrap();
        for i in 0u64..16 {
            db.insert(a, i, v(3)).unwrap();
        }

        let _ = db.drop_snapshot(snap_a).unwrap().unwrap();

        for i in 0u64..16 {
            assert_eq!(db.get(a, i).unwrap(), Some(v(3)));
            assert_eq!(db.get(b, i).unwrap(), Some(v(2)));
        }
        assert!(db.snapshots().is_empty());
    }

    #[test]
    fn drop_volume_with_live_snapshot_refused() {
        let (_d, db) = mk_db();
        let ord = db.create_volume().unwrap();
        db.insert(ord, 0, v(1)).unwrap();
        let _snap = db.take_snapshot(ord).unwrap();
        match db.drop_volume(ord).unwrap_err() {
            MetaDbError::InvalidArgument(msg) => {
                assert!(msg.contains("snapshot"), "msg={msg}");
            }
            e => panic!("unexpected error {e:?}"),
        }
        assert_eq!(db.volumes(), vec![0, ord]);
    }

    #[test]
    fn dropping_bootstrap_snapshot_does_not_affect_other_volume_snapshots() {
        let (_d, db) = mk_db();
        let a = db.create_volume().unwrap();
        db.insert(0, 1, v(0)).unwrap();
        db.insert(a, 1, v(1)).unwrap();
        let s_boot = db.take_snapshot(0).unwrap();
        let s_a = db.take_snapshot(a).unwrap();
        let _ = db.drop_snapshot(s_boot).unwrap().unwrap();
        // Snapshot on volume `a` still readable post-drop of bootstrap snapshot.
        let view = db.snapshot_view(s_a).unwrap();
        assert_eq!(view.get(1).unwrap(), Some(v(1)));
    }

    #[test]
    fn diff_across_volumes_rejected() {
        let (_d, db) = mk_db();
        let a = db.create_volume().unwrap();
        db.insert(0, 1, v(0)).unwrap();
        db.insert(a, 1, v(1)).unwrap();
        let s_boot = db.take_snapshot(0).unwrap();
        let s_a = db.take_snapshot(a).unwrap();
        match db.diff(s_boot, s_a).unwrap_err() {
            MetaDbError::InvalidArgument(msg) => {
                assert!(msg.contains("across volumes"), "msg={msg}");
            }
            e => panic!("unexpected error {e:?}"),
        }
    }

    // -------- phase 7 commit 10: clone_volume --------

    #[test]
    fn clone_volume_produces_readable_copy() {
        let (_d, db) = mk_db();
        let src = db.create_volume().unwrap();
        db.insert(src, 1, v(10)).unwrap();
        db.insert(src, 2, v(20)).unwrap();
        let snap = db.take_snapshot(src).unwrap();
        let clone = db.clone_volume(snap).unwrap();
        assert!(clone > src);
        assert_eq!(db.get(clone, 1).unwrap(), Some(v(10)));
        assert_eq!(db.get(clone, 2).unwrap(), Some(v(20)));
        assert_eq!(db.volumes(), vec![0, src, clone]);
    }

    #[test]
    fn clone_volume_unknown_snapshot_is_invalid_argument() {
        let (_d, db) = mk_db();
        match db.clone_volume(999).unwrap_err() {
            MetaDbError::InvalidArgument(msg) => assert!(msg.contains("unknown snapshot")),
            e => panic!("unexpected error {e:?}"),
        }
    }

    #[test]
    fn clone_volume_respects_max_volumes() {
        let dir = TempDir::new().unwrap();
        let mut cfg = Config::new(dir.path());
        cfg.max_volumes = 2; // bootstrap + one more
        let db = Db::create_with_config(cfg).unwrap();
        let src = db.create_volume().unwrap();
        let snap = db.take_snapshot(src).unwrap();
        match db.clone_volume(snap).unwrap_err() {
            MetaDbError::InvalidArgument(msg) => {
                assert!(msg.contains("max_volumes"), "msg={msg}");
            }
            e => panic!("unexpected error {e:?}"),
        }
    }

    #[test]
    fn clone_is_writable_and_diverges_from_source() {
        let (_d, db) = mk_db();
        let src = db.create_volume().unwrap();
        db.insert(src, 1, v(1)).unwrap();
        let snap = db.take_snapshot(src).unwrap();
        let clone = db.clone_volume(snap).unwrap();

        db.insert(src, 1, v(2)).unwrap();
        db.insert(clone, 1, v(3)).unwrap();
        assert_eq!(db.get(src, 1).unwrap(), Some(v(2)));
        assert_eq!(db.get(clone, 1).unwrap(), Some(v(3)));
        // Snapshot unchanged by either write.
        assert_eq!(db.snapshot_view(snap).unwrap().get(1).unwrap(), Some(v(1)));
    }

    #[test]
    fn dropping_source_snapshot_keeps_clone_alive() {
        let (_d, db) = mk_db();
        let src = db.create_volume().unwrap();
        for i in 0u64..32 {
            db.insert(src, i, v(i as u8)).unwrap();
        }
        let snap = db.take_snapshot(src).unwrap();
        let clone = db.clone_volume(snap).unwrap();
        let _ = db.drop_snapshot(snap).unwrap().unwrap();
        for i in 0u64..32 {
            assert_eq!(db.get(clone, i).unwrap(), Some(v(i as u8)));
        }
        assert_eq!(db.snapshots(), Vec::<SnapshotEntry>::new());
    }

    #[test]
    fn clone_assigns_fresh_monotonic_ord() {
        let (_d, db) = mk_db();
        let src = db.create_volume().unwrap();
        db.insert(src, 1, v(1)).unwrap();
        let snap = db.take_snapshot(src).unwrap();
        let clone_a = db.clone_volume(snap).unwrap();
        let clone_b = db.clone_volume(snap).unwrap();
        assert!(clone_b > clone_a);
        assert_eq!(db.manifest().next_volume_ord, clone_b + 1);
    }

    #[test]
    fn clone_survives_reopen_wal_replay() {
        let dir = TempDir::new().unwrap();
        let (src, snap, clone) = {
            let db = Db::create(dir.path()).unwrap();
            let src = db.create_volume().unwrap();
            db.insert(src, 5, v(50)).unwrap();
            let snap = db.take_snapshot(src).unwrap();
            let clone = db.clone_volume(snap).unwrap();
            db.insert(clone, 10, v(100)).unwrap();
            (src, snap, clone)
        };
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.volumes(), vec![0, src, clone]);
        assert_eq!(db.get(clone, 5).unwrap(), Some(v(50)));
        assert_eq!(db.get(clone, 10).unwrap(), Some(v(100)));
        // Source volume unaffected by clone divergence.
        assert_eq!(db.get(src, 5).unwrap(), Some(v(50)));
        assert_eq!(db.snapshot_view(snap).unwrap().get(5).unwrap(), Some(v(50)));
    }

    #[test]
    fn clone_of_empty_volume_is_empty_and_writable() {
        let (_d, db) = mk_db();
        let src = db.create_volume().unwrap();
        let snap = db.take_snapshot(src).unwrap();
        let clone = db.clone_volume(snap).unwrap();
        assert_eq!(db.get(clone, 1).unwrap(), None);
        db.insert(clone, 1, v(7)).unwrap();
        assert_eq!(db.get(clone, 1).unwrap(), Some(v(7)));
    }

    // -------- phase 7 commit 11: streaming iterators --------

    #[test]
    fn iter_refcounts_empty_db_returns_empty() {
        let (_d, db) = mk_db();
        let items: Vec<_> = db
            .iter_refcounts()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(items.is_empty());
    }

    #[test]
    fn iter_refcounts_emits_all_entries_sorted_by_pba() {
        let (_d, db) = mk_db();
        for (pba, delta) in [(100u64, 7u32), (50, 3), (200, 1), (10, 5)] {
            db.incref_pba(pba, delta).unwrap();
        }
        let items: Vec<(Pba, u32)> = db
            .iter_refcounts()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(items, vec![(10, 5), (50, 3), (100, 7), (200, 1)]);
    }

    #[test]
    fn iter_refcounts_hides_decremented_to_zero() {
        let (_d, db) = mk_db();
        db.incref_pba(42, 2).unwrap();
        db.decref_pba(42, 2).unwrap(); // rc back to 0 → row removed
        db.incref_pba(99, 1).unwrap();
        let items: Vec<(Pba, u32)> = db
            .iter_refcounts()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(items, vec![(99, 1)]);
    }

    #[test]
    fn iter_dedup_empty_db_returns_empty() {
        let (_d, db) = mk_db();
        let items: Vec<_> = db
            .iter_dedup()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(items.is_empty());
    }

    #[test]
    fn iter_dedup_emits_live_puts_and_hides_tombstones() {
        let (_d, db) = mk_db();
        let h1 = h(1);
        let h2 = h(2);
        let h3 = h(3);
        db.put_dedup(h1, dv(1)).unwrap();
        db.put_dedup(h2, dv(2)).unwrap();
        db.put_dedup(h3, dv(3)).unwrap();
        db.delete_dedup(h2).unwrap();
        let items: Vec<_> = db
            .iter_dedup()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let keys: Vec<Hash32> = items.iter().map(|(k, _)| *k).collect();
        assert!(keys.contains(&h1));
        assert!(!keys.contains(&h2));
        assert!(keys.contains(&h3));
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn iter_dedup_survives_flush_and_reopen() {
        let dir = TempDir::new().unwrap();
        let h1 = h(7);
        let h2 = h(8);
        {
            let db = Db::create(dir.path()).unwrap();
            db.put_dedup(h1, dv(1)).unwrap();
            db.put_dedup(h2, dv(2)).unwrap();
            db.flush().unwrap();
        }
        let db = Db::open(dir.path()).unwrap();
        let items: Vec<_> = db
            .iter_dedup()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn range_stream_matches_range() {
        let (_d, db) = mk_db();
        for i in 0u64..20 {
            db.insert(0, i, v(i as u8)).unwrap();
        }
        let lazy: Vec<_> = db
            .range_stream(0, 5..15)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let eager: Vec<_> = db
            .range(0, 5..15)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(lazy, eager);
        assert_eq!(lazy.len(), 10);
    }

    #[test]
    fn range_stream_routes_per_volume() {
        let (_d, db) = mk_db();
        let a = db.create_volume().unwrap();
        db.insert(0, 1, v(0)).unwrap();
        db.insert(a, 1, v(1)).unwrap();
        let on_boot: Vec<_> = db
            .range_stream(0, ..)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let on_a: Vec<_> = db
            .range_stream(a, ..)
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert_eq!(on_boot, vec![(1, v(0))]);
        assert_eq!(on_a, vec![(1, v(1))]);
    }

    // -------- phase 5e: refcount + dedup integration --------

    fn h(n: u64) -> crate::lsm::Hash32 {
        let mut out = [0u8; 32];
        out[..8].copy_from_slice(&n.to_be_bytes());
        out
    }

    fn dv(n: u8) -> crate::lsm::DedupValue {
        let mut x = [0u8; 28];
        x[0] = n;
        crate::lsm::DedupValue(x)
    }

    #[test]
    fn refcount_fresh_pba_reads_as_zero() {
        let (_d, db) = mk_db();
        assert_eq!(db.get_refcount(1234).unwrap(), 0);
    }

    #[test]
    fn incref_and_decref_roundtrip() {
        let (_d, db) = mk_db();
        assert_eq!(db.incref_pba(42, 1).unwrap(), 1);
        assert_eq!(db.incref_pba(42, 1).unwrap(), 2);
        assert_eq!(db.incref_pba(42, 3).unwrap(), 5);
        assert_eq!(db.get_refcount(42).unwrap(), 5);
        assert_eq!(db.decref_pba(42, 2).unwrap(), 3);
        assert_eq!(db.decref_pba(42, 3).unwrap(), 0);
        // Row should be gone.
        assert_eq!(db.get_refcount(42).unwrap(), 0);
    }

    #[test]
    fn decref_underflow_errors() {
        let (_d, db) = mk_db();
        db.incref_pba(1, 2).unwrap();
        assert!(matches!(
            db.decref_pba(1, 3).unwrap_err(),
            MetaDbError::InvalidArgument(_)
        ));
    }

    #[test]
    fn incref_overflow_errors() {
        let (_d, db) = mk_db();
        db.incref_pba(1, u32::MAX - 1).unwrap();
        assert_eq!(db.incref_pba(1, 1).unwrap(), u32::MAX);
        assert!(matches!(
            db.incref_pba(1, 1).unwrap_err(),
            MetaDbError::InvalidArgument(_)
        ));
    }

    #[test]
    fn refcount_survives_flush_and_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let db = Db::create(dir.path()).unwrap();
            for pba in 0u64..100 {
                db.incref_pba(pba, (pba as u32 % 7) + 1).unwrap();
            }
            db.flush().unwrap();
        }
        let db = Db::open(dir.path()).unwrap();
        for pba in 0u64..100 {
            assert_eq!(
                db.get_refcount(pba).unwrap(),
                (pba as u32 % 7) + 1,
                "pba {pba} mismatch after reopen",
            );
        }
    }

    #[test]
    fn dedup_put_get_roundtrip_via_memtable() {
        let (_d, db) = mk_db();
        db.put_dedup(h(1), dv(10)).unwrap();
        db.put_dedup(h(2), dv(20)).unwrap();
        assert_eq!(db.get_dedup(&h(1)).unwrap(), Some(dv(10)));
        assert_eq!(db.get_dedup(&h(2)).unwrap(), Some(dv(20)));
        assert_eq!(db.get_dedup(&h(3)).unwrap(), None);
    }

    #[test]
    fn dedup_delete_tombstones_key() {
        let (_d, db) = mk_db();
        db.put_dedup(h(1), dv(10)).unwrap();
        db.delete_dedup(h(1)).unwrap();
        assert_eq!(db.get_dedup(&h(1)).unwrap(), None);
    }

    #[test]
    fn dedup_flush_to_l0_then_read() {
        let (_d, db) = mk_db();
        for i in 0u64..50 {
            db.put_dedup(h(i), dv(i as u8)).unwrap();
        }
        assert!(db.flush_dedup_memtable().unwrap());
        for i in 0u64..50 {
            assert_eq!(db.get_dedup(&h(i)).unwrap(), Some(dv(i as u8)));
        }
    }

    #[test]
    fn cache_stats_show_hits_evictions_and_respect_budget() {
        let cache_budget = 1024 * 1024;
        let (_d, db) = mk_db_with_cache_bytes(cache_budget);

        for i in 0u64..40_000 {
            db.put_dedup(h(i), dv((i % 251) as u8)).unwrap();
        }
        assert!(db.flush_dedup_memtable().unwrap());

        let cold = db.cache_stats();
        assert_eq!(
            db.get_dedup(&h(12_345)).unwrap(),
            Some(dv((12_345 % 251) as u8))
        );
        let after_first = db.cache_stats();
        assert!(after_first.misses > cold.misses);

        assert_eq!(
            db.get_dedup(&h(12_345)).unwrap(),
            Some(dv((12_345 % 251) as u8))
        );
        let after_second = db.cache_stats();
        assert!(after_second.hits > after_first.hits);

        for i in (0u64..40_000).step_by(crate::lsm::RECORDS_PER_PAGE) {
            assert_eq!(db.get_dedup(&h(i)).unwrap(), Some(dv((i % 251) as u8)));
        }
        let after_sweep = db.cache_stats();
        assert!(after_sweep.evictions > 0);
        assert!(after_sweep.current_bytes <= cache_budget);
    }

    #[test]
    fn dedup_survives_flush_and_reopen() {
        let dir = TempDir::new().unwrap();
        {
            let db = Db::create(dir.path()).unwrap();
            for i in 0u64..100 {
                db.put_dedup(h(i), dv(i as u8)).unwrap();
            }
            // Force a couple of L0 SSTs so persistence is exercised.
            assert!(db.flush_dedup_memtable().unwrap());
            for i in 100u64..200 {
                db.put_dedup(h(i), dv((i % 255) as u8)).unwrap();
            }
            assert!(db.flush_dedup_memtable().unwrap());
            db.flush().unwrap();
        }
        let db = Db::open(dir.path()).unwrap();
        for i in 0u64..100 {
            assert_eq!(db.get_dedup(&h(i)).unwrap(), Some(dv(i as u8)));
        }
        for i in 100u64..200 {
            assert_eq!(db.get_dedup(&h(i)).unwrap(), Some(dv((i % 255) as u8)));
        }
    }

    #[test]
    fn take_snapshot_captures_refcount_state() {
        let (_d, db) = mk_db();
        for pba in 0u64..50 {
            db.incref_pba(pba, 1).unwrap();
        }
        let _snap = db.take_snapshot(0).unwrap();
        // Overwrite refcount state after the snapshot.
        for pba in 0u64..50 {
            db.incref_pba(pba, 1).unwrap();
        }
        for pba in 0u64..50 {
            assert_eq!(db.get_refcount(pba).unwrap(), 2);
        }
        // Phase 6.5b retired refcount snapshots; v6 SnapshotEntry no
        // longer carries refcount fields at all. L2P roots page is still
        // allocated because L2P tree IS snapshotted.
        let snap_entry = &db.snapshots()[0];
        assert_ne!(snap_entry.l2p_roots_page, crate::types::NULL_PAGE);
        assert_eq!(snap_entry.vol_ord, BOOTSTRAP_VOLUME_ORD);
    }

    #[test]
    fn drop_snapshot_releases_refcount_state() {
        let (_d, db) = mk_db();
        for pba in 0u64..200 {
            db.incref_pba(pba, 1).unwrap();
        }
        db.flush().unwrap();
        let hw_before_snap = db.high_water();
        let s = db.take_snapshot(0).unwrap();
        for pba in 0u64..200 {
            db.incref_pba(pba, 1).unwrap();
        }
        db.flush().unwrap();
        let hw_after = db.high_water();
        assert!(hw_after > hw_before_snap);
        // This test has no L2P inserts between take/drop, so the L2P
        // tree never diverges and no tree pages hit rc=0 during the
        // drop. The WAL-logged drop just decrements the shared root's
        // rc back to 1; `pages_freed` is 0 because the only page that
        // becomes orphan (`entry.l2p_roots_page`) is deliberately left
        // for the next flush + reclaim pass rather than freed inline
        // (see `drop_snapshot` for why). The invariant we still care
        // about is that refcount state is preserved end-to-end.
        let _ = db.drop_snapshot(s).unwrap().unwrap();
        for pba in 0u64..200 {
            assert_eq!(db.get_refcount(pba).unwrap(), 2);
        }
    }

    #[test]
    fn dedup_compaction_can_be_triggered_from_db() {
        let (_d, db) = mk_db();
        for batch in 0..4u64 {
            for i in 0u64..10 {
                db.put_dedup(h(batch * 100 + i), dv(batch as u8)).unwrap();
            }
            db.flush_dedup_memtable().unwrap();
        }
        // With 4 L0 SSTs we're at the default trigger; compaction
        // should do something.
        assert!(db.compact_dedup_once().unwrap());
        for batch in 0..4u64 {
            for i in 0u64..10 {
                assert_eq!(
                    db.get_dedup(&h(batch * 100 + i)).unwrap(),
                    Some(dv(batch as u8)),
                );
            }
        }
    }

    // -------- phase 6: WAL durability --------

    #[test]
    fn writes_without_flush_survive_reopen_via_wal_replay() {
        let dir = TempDir::new().unwrap();
        {
            let db = Db::create(dir.path()).unwrap();
            for i in 0u64..50 {
                db.insert(0,i, v(i as u8)).unwrap();
            }
            // NO flush() before drop — only WAL is durable.
        }
        let db = Db::open(dir.path()).unwrap();
        for i in 0u64..50 {
            assert_eq!(db.get(0,i).unwrap(), Some(v(i as u8)));
        }
    }

    #[test]
    fn refcount_writes_survive_reopen_without_flush() {
        let dir = TempDir::new().unwrap();
        {
            let db = Db::create(dir.path()).unwrap();
            for pba in 0u64..100 {
                db.incref_pba(pba, (pba as u32 % 3) + 1).unwrap();
            }
        }
        let db = Db::open(dir.path()).unwrap();
        for pba in 0u64..100 {
            assert_eq!(db.get_refcount(pba).unwrap(), (pba as u32 % 3) + 1);
        }
    }

    #[test]
    fn dedup_writes_survive_reopen_without_flush() {
        let dir = TempDir::new().unwrap();
        {
            let db = Db::create(dir.path()).unwrap();
            for i in 0u64..30 {
                db.put_dedup(h(i), dv(i as u8)).unwrap();
            }
        }
        let db = Db::open(dir.path()).unwrap();
        for i in 0u64..30 {
            assert_eq!(db.get_dedup(&h(i)).unwrap(), Some(dv(i as u8)));
        }
    }

    #[test]
    fn multi_op_tx_commits_atomically_and_all_ops_visible() {
        let (_d, db) = mk_db();
        let mut tx = db.begin();
        tx.insert(0,1, v(1));
        tx.insert(0,2, v(2));
        tx.incref_pba(10, 3);
        tx.put_dedup(h(1), dv(9));
        let lsn = tx.commit().unwrap();
        assert!(lsn >= 1);
        assert_eq!(db.get(0,1).unwrap(), Some(v(1)));
        assert_eq!(db.get(0,2).unwrap(), Some(v(2)));
        assert_eq!(db.get_refcount(10).unwrap(), 3);
        assert_eq!(db.get_dedup(&h(1)).unwrap(), Some(dv(9)));
    }

    #[test]
    fn multi_op_tx_survives_reopen_all_or_nothing() {
        let dir = TempDir::new().unwrap();
        {
            let db = Db::create(dir.path()).unwrap();
            let mut tx = db.begin();
            tx.insert(0,1, v(1));
            tx.insert(0,2, v(2));
            tx.incref_pba(10, 3);
            tx.put_dedup(h(1), dv(9));
            tx.commit().unwrap();
        }
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.get(0,1).unwrap(), Some(v(1)));
        assert_eq!(db.get(0,2).unwrap(), Some(v(2)));
        assert_eq!(db.get_refcount(10).unwrap(), 3);
        assert_eq!(db.get_dedup(&h(1)).unwrap(), Some(dv(9)));
    }

    #[test]
    fn last_applied_lsn_advances_per_commit() {
        let (_d, db) = mk_db();
        let before = db.last_applied_lsn();
        db.insert(0,1, v(1)).unwrap();
        let after_one = db.last_applied_lsn();
        assert!(after_one > before);
        db.incref_pba(100, 1).unwrap();
        let after_two = db.last_applied_lsn();
        assert!(after_two > after_one);
    }

    #[test]
    fn checkpoint_advances_on_flush() {
        let dir = TempDir::new().unwrap();
        let db = Db::create(dir.path()).unwrap();
        for i in 0u64..10 {
            db.insert(0,i, v(i as u8)).unwrap();
        }
        let applied = db.last_applied_lsn();
        assert_eq!(db.manifest().checkpoint_lsn, 0);
        db.flush().unwrap();
        assert_eq!(db.manifest().checkpoint_lsn, applied);
    }

    #[test]
    fn empty_tx_commit_is_noop() {
        let (_d, db) = mk_db();
        let tx = db.begin();
        let lsn = tx.commit().unwrap();
        assert_eq!(lsn, db.last_applied_lsn());
        assert_eq!(db.last_applied_lsn(), 0);
    }

    #[test]
    fn reopen_replay_advances_last_applied() {
        let dir = TempDir::new().unwrap();
        let committed_lsn = {
            let db = Db::create(dir.path()).unwrap();
            for i in 0u64..5 {
                db.insert(0,i, v(i as u8)).unwrap();
            }
            db.last_applied_lsn()
        };
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.last_applied_lsn(), committed_lsn);
        // New commits after reopen start at committed_lsn + 1.
        db.insert(0,100, v(0)).unwrap();
        assert_eq!(db.last_applied_lsn(), committed_lsn + 1);
    }

    // -------- phase 6e: dedup_reverse --------

    fn hash_full(high: u64, low: u64) -> Hash32 {
        // Build a 32B hash with arbitrary but distinct bytes so we can
        // verify reverse round-trip preserves all 32 bytes (not just
        // the first 24 that fit in the reverse key).
        let mut h = [0u8; 32];
        h[..8].copy_from_slice(&high.to_be_bytes());
        h[8..16].copy_from_slice(&(high.wrapping_mul(7)).to_be_bytes());
        h[16..24].copy_from_slice(&(low.wrapping_mul(11)).to_be_bytes());
        h[24..].copy_from_slice(&low.to_be_bytes());
        h
    }

    #[test]
    fn reverse_entry_round_trip_recovers_full_hash() {
        let hash = hash_full(0xAABB_CCDD_1111_2222, 0xDEAD_BEEF_CAFE_F00D);
        let (key, value) = encode_reverse_entry(42, &hash);
        assert_eq!(&key[..8], &42u64.to_be_bytes());
        let back = decode_reverse_hash(&key, &value);
        assert_eq!(back, hash);
    }

    #[test]
    fn register_and_scan_by_pba() {
        let (_d, db) = mk_db();
        let h1 = hash_full(1, 100);
        let h2 = hash_full(2, 200);
        db.register_dedup_reverse(42, h1).unwrap();
        db.register_dedup_reverse(42, h2).unwrap();
        db.register_dedup_reverse(99, hash_full(3, 300)).unwrap();

        let mut found = db.scan_dedup_reverse_for_pba(42).unwrap();
        found.sort();
        let mut expected = vec![h1, h2];
        expected.sort();
        assert_eq!(found, expected);

        assert_eq!(
            db.scan_dedup_reverse_for_pba(12345).unwrap(),
            Vec::<Hash32>::new()
        );
    }

    #[test]
    fn unregister_removes_from_scan() {
        let (_d, db) = mk_db();
        let h1 = hash_full(10, 1);
        let h2 = hash_full(10, 2);
        db.register_dedup_reverse(7, h1).unwrap();
        db.register_dedup_reverse(7, h2).unwrap();
        db.unregister_dedup_reverse(7, h1).unwrap();
        let found = db.scan_dedup_reverse_for_pba(7).unwrap();
        assert_eq!(found, vec![h2]);
    }

    #[test]
    fn scan_sees_entries_after_flush_to_sst() {
        let (_d, db) = mk_db();
        for i in 0u64..25 {
            db.register_dedup_reverse(17, hash_full(i, i)).unwrap();
        }
        // Flush the dedup_reverse memtable so the next scan reads SST data.
        let lsn = db.last_applied_lsn();
        db.dedup_reverse.flush_memtable(lsn).unwrap();
        let found = db.scan_dedup_reverse_for_pba(17).unwrap();
        assert_eq!(found.len(), 25);
    }

    #[test]
    fn tx_atomically_registers_dedup_index_and_reverse() {
        let (_d, db) = mk_db();
        let hash = hash_full(5, 5);
        let mut tx = db.begin();
        tx.put_dedup(hash, dv(7));
        tx.register_dedup_reverse(999, hash);
        tx.incref_pba(999, 1);
        tx.commit().unwrap();
        assert_eq!(db.get_dedup(&hash).unwrap(), Some(dv(7)));
        assert_eq!(db.scan_dedup_reverse_for_pba(999).unwrap(), vec![hash]);
        assert_eq!(db.get_refcount(999).unwrap(), 1);
    }

    #[test]
    fn dedup_reverse_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let h_a = hash_full(77, 1);
        let h_b = hash_full(77, 2);
        {
            let db = Db::create(dir.path()).unwrap();
            db.register_dedup_reverse(77, h_a).unwrap();
            db.register_dedup_reverse(77, h_b).unwrap();
            // flush half so we exercise both WAL-replay AND
            // SST-reload paths.
            let lsn = db.last_applied_lsn();
            db.dedup_reverse.flush_memtable(lsn).unwrap();
            db.flush().unwrap();
            db.register_dedup_reverse(77, hash_full(77, 3)).unwrap();
        }
        let db = Db::open(dir.path()).unwrap();
        let mut found = db.scan_dedup_reverse_for_pba(77).unwrap();
        found.sort();
        let mut expected = vec![h_a, h_b, hash_full(77, 3)];
        expected.sort();
        assert_eq!(found, expected);
    }

    // -------- batch read API --------------------------------------------

    #[test]
    fn multi_get_empty_input_returns_empty() {
        let (_d, db) = mk_db();
        assert!(db.multi_get(0,&[]).unwrap().is_empty());
        assert!(db.multi_get_refcount(&[]).unwrap().is_empty());
        assert!(db.multi_get_dedup(&[]).unwrap().is_empty());
        assert!(db.multi_scan_dedup_reverse_for_pba(&[]).unwrap().is_empty());
    }

    #[test]
    fn multi_get_matches_single_gets_across_shards() {
        // 4 shards so we actually exercise the bucket + group logic.
        let (_d, db) = mk_db_with_shards(4);
        for i in 0u64..200 {
            db.insert(0,i, v((i as u8).wrapping_mul(3))).unwrap();
        }
        // Mix mapped + unmapped + duplicate keys, in non-sorted order.
        let keys = vec![199, 5000, 0, 199, 42, 10_000, 1, 42];
        let got = db.multi_get(0,&keys).unwrap();
        assert_eq!(got.len(), keys.len());
        for (i, key) in keys.iter().enumerate() {
            assert_eq!(got[i], db.get(0,*key).unwrap(), "key {key} mismatch");
        }
    }

    #[test]
    fn multi_get_refcount_matches_single_gets_across_shards() {
        let (_d, db) = mk_db_with_shards(4);
        for pba in 0u64..100 {
            db.incref_pba(pba, (pba as u32 % 5) + 1).unwrap();
        }
        let pbas: Vec<Pba> = vec![99, 0, 50, 9999, 42, 50, 1, 2, 9999];
        let got = db.multi_get_refcount(&pbas).unwrap();
        assert_eq!(got.len(), pbas.len());
        for (i, pba) in pbas.iter().enumerate() {
            assert_eq!(
                got[i],
                db.get_refcount(*pba).unwrap(),
                "pba {pba} mismatch",
            );
        }
    }

    #[test]
    fn multi_get_dedup_hits_memtable_and_sst() {
        let (_d, db) = mk_db();
        // First half lands in L0 after the flush; second half stays in
        // the memtable. Makes sure the multi-key path walks both.
        for i in 0u64..40 {
            db.put_dedup(h(i), dv(i as u8)).unwrap();
        }
        assert!(db.flush_dedup_memtable().unwrap());
        for i in 40u64..80 {
            db.put_dedup(h(i), dv(i as u8)).unwrap();
        }
        // Include a tombstoned key and an unknown key.
        db.delete_dedup(h(5)).unwrap();
        let hashes = vec![h(0), h(5), h(39), h(40), h(79), h(999), h(0)];
        let got = db.multi_get_dedup(&hashes).unwrap();
        assert_eq!(got.len(), hashes.len());
        for (i, hash) in hashes.iter().enumerate() {
            assert_eq!(got[i], db.get_dedup(hash).unwrap(), "hash {i} mismatch");
        }
    }

    #[test]
    fn multi_scan_dedup_reverse_preserves_order_and_per_pba_rows() {
        let (_d, db) = mk_db();
        // pba=10 has two hashes, pba=20 has three, pba=30 has zero.
        // Flush some to force the cross-layer code path (memtable +
        // SST) for at least one PBA.
        db.register_dedup_reverse(10, hash_full(10, 1)).unwrap();
        db.register_dedup_reverse(10, hash_full(10, 2)).unwrap();
        let flush_lsn = db.last_applied_lsn();
        db.dedup_reverse.flush_memtable(flush_lsn).unwrap();
        db.register_dedup_reverse(20, hash_full(20, 1)).unwrap();
        db.register_dedup_reverse(20, hash_full(20, 2)).unwrap();
        db.register_dedup_reverse(20, hash_full(20, 3)).unwrap();

        // Include a repeated PBA to make sure the batched impl doesn't
        // de-duplicate or collapse results.
        let pbas: Vec<Pba> = vec![30, 20, 10, 20];
        let batched = db.multi_scan_dedup_reverse_for_pba(&pbas).unwrap();
        assert_eq!(batched.len(), pbas.len());
        for (i, pba) in pbas.iter().enumerate() {
            let mut expected = db.scan_dedup_reverse_for_pba(*pba).unwrap();
            let mut got = batched[i].clone();
            expected.sort();
            got.sort();
            assert_eq!(got, expected, "pba {pba} mismatch");
        }
    }

    #[test]
    fn multi_scan_dedup_reverse_hides_tombstoned_rows_explicitly() {
        let (_d, db) = mk_db();
        let p10_a = hash_full(10, 1);
        let p10_b = hash_full(10, 2);
        let p10_c = hash_full(10, 3);
        let p20_a = hash_full(20, 1);

        // Oldest persisted layer.
        db.register_dedup_reverse(10, p10_a).unwrap();
        db.register_dedup_reverse(10, p10_b).unwrap();
        db.register_dedup_reverse(20, p20_a).unwrap();
        let flush_lsn = db.last_applied_lsn();
        db.dedup_reverse.flush_memtable(flush_lsn).unwrap();

        // Newer memtable updates: remove one old row, add a new one.
        db.unregister_dedup_reverse(10, p10_a).unwrap();
        db.register_dedup_reverse(10, p10_c).unwrap();

        let pbas: Vec<Pba> = vec![10, 20, 30, 10];
        let mut got = db.multi_scan_dedup_reverse_for_pba(&pbas).unwrap();
        for rows in &mut got {
            rows.sort();
        }

        assert_eq!(got[0], vec![p10_b, p10_c]);
        assert_eq!(got[1], vec![p20_a]);
        assert!(got[2].is_empty());
        assert_eq!(got[3], vec![p10_b, p10_c]);
    }
}
