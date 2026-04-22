//! Sharded embedded metadata database: the glue between `PageStore`,
//! `ManifestStore`, and one `BTree` per shard.
//!
//! Phase 4 scope:
//! - N independent COW B+tree shards behind one `Db`
//! - xxh3-based shard router
//! - thread-safe point writes via one mutex per shard
//! - fan-out range / diff / snapshot operations

use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::{Condvar, Mutex, MutexGuard, RwLock, RwLockReadGuard};
use xxhash_rust::xxh3::xxh3_64;

use crate::btree::BTree;
use crate::cache::{PageCache, PageCacheStats};
use crate::config::Config;
use crate::error::{MetaDbError, Result};
use crate::lsm::{DedupValue, Hash32, Lsm, LsmConfig};
use crate::manifest::{
    MANIFEST_BODY_VERSION, Manifest, ManifestStore, SnapshotEntry, materialize_snapshot_root_pages,
    write_snapshot_roots_page,
};
use crate::page_store::PageStore;
use crate::paged::PagedL2p;
use crate::paged::{DiffEntry, L2pValue};
use crate::testing::faults::{FaultController, FaultPoint};
use crate::tx::{ApplyOutcome, Transaction};
use crate::types::{Lsn, PageId, Pba, SnapshotId};
use crate::verify;
use crate::wal::{Wal, WalOp, encode_body};

/// Embedded metadata database.
pub struct Db {
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    manifest_state: Mutex<ManifestState>,
    /// L2P paged radix-tree shards (LBA → `L2pValue`). Replaced the old
    /// per-shard B+tree in phase 6.5a: LBA keys are dense u64s so the
    /// paged table saves both the per-entry key storage and the tree-
    /// descent key comparisons that a B+tree would otherwise pay.
    shards: Vec<L2pShard>,
    /// PBA refcount B+tree shards (PBA → first 4 bytes = u32 big-endian
    /// refcount, remaining 24 bytes reserved).
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
    /// Lock order: `drop_gate` → `apply_gate` → `manifest_state`
    /// → `snapshot_views`. Everyone entering `commit_ops` or
    /// `drop_snapshot` respects this prefix; internal reads that don't
    /// mutate skip `drop_gate` entirely.
    drop_gate: RwLock<()>,
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
        manifest.shard_roots = l2p_roots;
        manifest.refcount_shard_roots = refcount_roots;
        manifest.dedup_level_heads = Vec::new().into_boxed_slice();
        manifest.dedup_reverse_level_heads = Vec::new().into_boxed_slice();
        manifest_store.commit(&manifest)?;

        let wal = Wal::create(
            &wal_dir(&cfg.path),
            &cfg,
            manifest.checkpoint_lsn + 1,
            faults.clone(),
        )?;

        Ok(Self {
            page_store,
            page_cache,
            manifest_state: Mutex::new(ManifestState {
                store: manifest_store,
                manifest,
            }),
            shards: l2p_shards,
            refcount_shards,
            dedup_index,
            dedup_reverse,
            wal,
            apply_gate: RwLock::new(()),
            last_applied_lsn: Mutex::new(0),
            commit_cvar: Condvar::new(),
            snapshot_views: RwLock::new(()),
            drop_gate: RwLock::new(()),
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
        if manifest.shard_roots.is_empty() {
            return Err(MetaDbError::Corruption(
                "manifest has no shard roots; database was not initialized".into(),
            ));
        }

        let next_gen = manifest.checkpoint_lsn.max(1) + 1;

        // v3 upgrade: manifest didn't carry refcount / dedup state.
        // Materialize them so open returns a fully-populated v4 layout.
        let mut manifest_dirty = false;
        if manifest.refcount_shard_roots.is_empty() {
            let (_owned, roots) = create_shards(
                page_store.clone(),
                page_cache.clone(),
                manifest.shard_count(),
            )?;
            manifest.refcount_shard_roots = roots;
            manifest_dirty = true;
            // Note: the Shard handles are discarded here; we rebuild
            // them via `open_shards` below once the manifest is stable,
            // to share the same codepath with normal opens.
        }
        if materialize_snapshot_root_pages(&page_store, &mut manifest, next_gen)? {
            manifest_dirty = true;
        }
        if manifest_dirty {
            manifest_store.commit(&manifest)?;
        }

        let l2p_shards = open_l2p_shards(
            page_store.clone(),
            page_cache.clone(),
            &manifest.shard_roots,
            next_gen,
        )?;
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
        let wal_path = wal_dir(&cfg.path);
        let from_lsn = manifest.checkpoint_lsn + 1;
        let mut replayed_drop = false;
        let replay_outcome = crate::recovery::replay_into(&wal_path, from_lsn, |lsn, op| {
            let outcome = apply_op_bare(
                &l2p_shards,
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
        })?;
        let last_applied = replay_outcome.last_lsn.unwrap_or(manifest.checkpoint_lsn);
        // If the last segment ended torn, truncate it to the last clean
        // record before handing the directory to the new Wal.
        crate::recovery::truncate_torn_tail(&wal_path, &replay_outcome)?;
        let wal = Wal::create(&wal_path, &cfg, last_applied + 1, faults.clone())?;

        // If replay removed any snapshots from the in-memory manifest,
        // persist the updated list before we start freeing pages. The
        // on-disk manifest would otherwise still reference those
        // dropped snapshots' metadata pages, and `reclaim_orphan_pages`
        // below would mark them as orphan (nothing in-memory points at
        // them) and free them — then a subsequent offline decode of
        // the stale on-disk manifest would fail on the now-Free pages.
        // By committing here we keep disk and memory in sync.
        if replayed_drop {
            manifest.checkpoint_lsn = last_applied;
            manifest_store.commit(&manifest)?;
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
            shards: l2p_shards,
            refcount_shards,
            dedup_index,
            dedup_reverse,
            wal,
            apply_gate: RwLock::new(()),
            last_applied_lsn: Mutex::new(last_applied),
            commit_cvar: Condvar::new(),
            snapshot_views: RwLock::new(()),
            drop_gate: RwLock::new(()),
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

    /// Number of shards in this database.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
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
        let mut l2p_guards = self.lock_all_l2p_shards();
        let mut refcount_guards = self.lock_all_refcount_shards();
        self.flush_locked_l2p_shards(&mut l2p_guards)?;
        self.flush_locked_refcount_shards(&mut refcount_guards)?;

        let tree_generation = max_generation_from_two_groups(&l2p_guards, &refcount_guards);
        let wal_checkpoint = *self.last_applied_lsn.lock();
        let dedup_update =
            self.prepare_dedup_manifest_update(&mut manifest_state.manifest, tree_generation)?;
        self.faults
            .inject(FaultPoint::FlushPostLevelRewriteBeforeManifest)?;

        self.refresh_manifest_from_locked(
            &mut manifest_state.manifest,
            &l2p_guards,
            &refcount_guards,
        );
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
        let mut outcomes = Vec::with_capacity(ops.len());
        for op in ops {
            outcomes.push(self.apply_op(lsn, op)?);
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

    fn apply_op(&self, lsn: Lsn, op: &WalOp) -> Result<ApplyOutcome> {
        let outcome = apply_op_bare(
            &self.shards,
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

    // -------- tree operations --------------------------------------------

    pub fn get(&self, key: u64) -> Result<Option<L2pValue>> {
        let sid = self.shard_for(key);
        let mut tree = self.shards[sid].tree.lock();
        tree.get(key)
    }

    /// Batched L2P lookup. Groups `keys` by shard, locks each shard once,
    /// and reads every key that falls to it before moving on. Output
    /// order matches input order; duplicates produce repeated results.
    pub fn multi_get(&self, keys: &[u64]) -> Result<Vec<Option<L2pValue>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let shard_count = self.shards.len();
        let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); shard_count];
        for (idx, key) in keys.iter().enumerate() {
            buckets[self.shard_for(*key)].push(idx);
        }
        let mut out: Vec<Option<L2pValue>> = vec![None; keys.len()];
        for (sid, idxs) in buckets.into_iter().enumerate() {
            if idxs.is_empty() {
                continue;
            }
            let mut tree = self.shards[sid].tree.lock();
            for idx in idxs {
                out[idx] = tree.get(keys[idx])?;
            }
        }
        Ok(out)
    }

    pub fn insert(&self, key: u64, value: L2pValue) -> Result<Option<L2pValue>> {
        let mut tx = self.begin();
        tx.insert(key, value);
        let (_, outcomes) = tx.commit_with_outcomes()?;
        match outcomes.into_iter().next().unwrap() {
            ApplyOutcome::L2pPrev(prev) => Ok(prev),
            _ => unreachable!("insert produces L2pPrev"),
        }
    }

    pub fn delete(&self, key: u64) -> Result<Option<L2pValue>> {
        let mut tx = self.begin();
        tx.delete(key);
        let (_, outcomes) = tx.commit_with_outcomes()?;
        match outcomes.into_iter().next().unwrap() {
            ApplyOutcome::L2pPrev(prev) => Ok(prev),
            _ => unreachable!("delete produces L2pPrev"),
        }
    }

    pub fn range<R: RangeBounds<u64>>(&self, range: R) -> Result<DbRangeIter> {
        let range = OwnedRange::new(range);
        let mut guards = self.lock_all_l2p_shards();
        let mut items = Vec::new();
        for tree in &mut guards {
            items.extend(tree.range(range.clone())?.collect::<Result<Vec<_>>>()?);
        }
        items.sort_unstable_by_key(|(k, _)| *k);
        Ok(DbRangeIter::new(items))
    }

    // -------- snapshot operations -----------------------------------------

    /// Take a snapshot of both the L2P and refcount sharded trees.
    /// Returns the new snapshot id. Persisted immediately via a
    /// manifest commit.
    pub fn take_snapshot(&self) -> Result<SnapshotId> {
        // Exclude in-flight apply phases so `last_applied_lsn` and the
        // per-shard roots we sample below describe the same LSN point.
        let _apply_guard = self.apply_gate.write();
        let mut manifest_state = self.manifest_state.lock();
        let mut l2p_guards = self.lock_all_l2p_shards();
        let mut refcount_guards = self.lock_all_refcount_shards();

        let id = manifest_state.manifest.next_snapshot_id;
        let mut l2p_roots = Vec::with_capacity(l2p_guards.len());
        for tree in &mut l2p_guards {
            tree.incref_root_for_snapshot()?;
            l2p_roots.push(tree.root());
        }
        // Phase 6.5b: refcount is a running tally, not point-in-time
        // state. Snapshots only capture L2P. We still need `refcount_guards`
        // below for `max_generation_from_two_groups` and
        // `refresh_manifest_from_locked`, but skip the per-tree
        // snapshot incref.
        let created_lsn = max_generation_from_two_groups(&l2p_guards, &refcount_guards);
        let l2p_roots_page = write_snapshot_roots_page(&self.page_store, &l2p_roots, created_lsn)?;

        self.flush_locked_l2p_shards(&mut l2p_guards)?;
        self.flush_locked_refcount_shards(&mut refcount_guards)?;
        let dedup_update =
            self.prepare_dedup_manifest_update(&mut manifest_state.manifest, created_lsn)?;
        self.refresh_manifest_from_locked(
            &mut manifest_state.manifest,
            &l2p_guards,
            &refcount_guards,
        );
        manifest_state.manifest.checkpoint_lsn = *self.last_applied_lsn.lock();
        manifest_state.manifest.snapshots.push(SnapshotEntry {
            id,
            l2p_roots_page,
            refcount_roots_page: crate::types::NULL_PAGE,
            created_lsn,
            l2p_shard_roots: l2p_roots.into_boxed_slice(),
            refcount_shard_roots: Vec::new().into_boxed_slice(),
        });
        manifest_state.manifest.next_snapshot_id = id
            .checked_add(1)
            .ok_or_else(|| MetaDbError::Corruption("snapshot id overflow".into()))?;
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

    /// Compute the diff between two snapshots.
    pub fn diff(&self, a: SnapshotId, b: SnapshotId) -> Result<Vec<DiffEntry>> {
        let _guard = self.snapshot_views.read();
        let (a_roots, b_roots) = {
            let manifest_state = self.manifest_state.lock();
            let a_roots = manifest_state
                .manifest
                .find_snapshot(a)
                .ok_or_else(|| MetaDbError::InvalidArgument(format!("unknown snapshot id {a}")))?
                .l2p_shard_roots
                .clone();
            let b_roots = manifest_state
                .manifest
                .find_snapshot(b)
                .ok_or_else(|| MetaDbError::InvalidArgument(format!("unknown snapshot id {b}")))?
                .l2p_shard_roots
                .clone();
            (a_roots, b_roots)
        };
        self.diff_roots(&a_roots, &b_roots)
    }

    /// Diff a snapshot against the current tree.
    pub fn diff_with_current(&self, snap: SnapshotId) -> Result<Vec<DiffEntry>> {
        let _guard = self.snapshot_views.read();
        let snap_roots = {
            let manifest_state = self.manifest_state.lock();
            manifest_state
                .manifest
                .find_snapshot(snap)
                .ok_or_else(|| MetaDbError::InvalidArgument(format!("unknown snapshot id {snap}")))?
                .l2p_shard_roots
                .clone()
        };

        let mut guards = self.lock_all_l2p_shards();
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
        // Phase 6.5b: refcount is not snapshotted — we should never be
        // looking at legacy snapshot entries with non-empty refcount
        // roots, because the v4→v5 on-open upgrade path rejects them.
        debug_assert!(entry.refcount_shard_roots.is_empty());

        let mut pages: Vec<PageId> = Vec::new();
        {
            let mut l2p_guards = self.lock_all_l2p_shards();
            // Flush any dirty tree pages (including the root's
            // post-take_snapshot rc bump) so apply_op_bare reads the
            // live values by pid via page_store.
            self.flush_locked_l2p_shards(&mut l2p_guards)?;
            for (tree, &root) in l2p_guards.iter_mut().zip(entry.l2p_shard_roots.iter()) {
                if root == crate::types::NULL_PAGE {
                    continue;
                }
                pages.extend(tree.collect_drop_pages(root)?);
            }
        }
        // NOTE on `entry.l2p_roots_page`: this SnapshotRoots page is
        // referenced only by the manifest's snapshot entry, so it
        // *logically* becomes unreferenced when we apply the drop.
        // We deliberately leave it alone here though: the on-disk
        // manifest still has the snapshot entry (no commit happens
        // inside drop_snapshot), and `Manifest::decode_v5` would call
        // `load_snapshot_roots` on it during any subsequent open —
        // turning it Free would break decode and deadlock recovery.
        // The page becomes a genuine orphan only after the next flush
        // persists a snapshot-less manifest; `reclaim_orphan_pages`
        // (run after WAL replay in `Db::open`) picks it up from there.

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

        let outcome = apply_op_bare(
            &self.shards,
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
        // shards' PageBuf caches still hold stale refcounts for
        // anything apply touched. Invalidate every one of those pids
        // so the next cow_for_write / lookup pulls the fresh bytes
        // from disk.
        for &pid in &pages {
            self.page_cache.invalidate(pid);
            for shard in &self.shards {
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

    fn shard_for(&self, key: u64) -> usize {
        debug_assert!(!self.shards.is_empty());
        (xxh3_64(&key.to_be_bytes()) as usize) % self.shards.len()
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

    fn lock_all_l2p_shards(&self) -> Vec<MutexGuard<'_, PagedL2p>> {
        self.shards.iter().map(|shard| shard.tree.lock()).collect()
    }

    fn lock_all_refcount_shards(&self) -> Vec<MutexGuard<'_, BTree>> {
        self.refcount_shards
            .iter()
            .map(|shard| shard.tree.lock())
            .collect()
    }

    fn flush_locked_l2p_shards(&self, guards: &mut [MutexGuard<'_, PagedL2p>]) -> Result<()> {
        for tree in guards {
            tree.flush()?;
        }
        Ok(())
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
        l2p_guards: &[MutexGuard<'_, PagedL2p>],
        refcount_guards: &[MutexGuard<'_, BTree>],
    ) {
        manifest.body_version = MANIFEST_BODY_VERSION;
        manifest.shard_roots = l2p_guards
            .iter()
            .map(|tree| tree.root())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        manifest.refcount_shard_roots = refcount_guards
            .iter()
            .map(|tree| tree.root())
            .collect::<Vec<_>>()
            .into_boxed_slice();
    }

    fn current_generation(&self) -> Lsn {
        let l2p = self.lock_all_l2p_shards();
        let refcount = self.lock_all_refcount_shards();
        max_generation_from_two_groups(&l2p, &refcount)
    }

    fn collect_range_for_roots(&self, roots: &[PageId], range: OwnedRange) -> Result<DbRangeIter> {
        if roots.len() != self.shards.len() {
            return Err(MetaDbError::Corruption(format!(
                "snapshot root count {} does not match shard count {}",
                roots.len(),
                self.shards.len(),
            )));
        }
        let mut items = Vec::new();
        for (root, shard) in roots.iter().copied().zip(&self.shards) {
            let mut tree = shard.tree.lock();
            items.extend(
                tree.range_at(root, range.clone())?
                    .collect::<Result<Vec<_>>>()?,
            );
        }
        items.sort_unstable_by_key(|(k, _)| *k);
        Ok(DbRangeIter::new(items))
    }

    fn diff_roots(&self, a: &[PageId], b: &[PageId]) -> Result<Vec<DiffEntry>> {
        if a.len() != self.shards.len() || b.len() != self.shards.len() {
            return Err(MetaDbError::Corruption(format!(
                "diff root counts ({}, {}) do not match shard count {}",
                a.len(),
                b.len(),
                self.shards.len(),
            )));
        }
        let mut out = Vec::new();
        for ((a_root, b_root), shard) in a.iter().copied().zip(b.iter().copied()).zip(&self.shards)
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

    /// Point lookup as of the snapshot's LSN.
    pub fn get(&self, key: u64) -> Result<Option<L2pValue>> {
        let sid = self.db.shard_for(key);
        let mut tree = self.db.shards[sid].tree.lock();
        tree.get_at(self.entry.l2p_shard_roots[sid], key)
    }

    /// Range scan as of the snapshot's LSN.
    pub fn range<R: RangeBounds<u64>>(&self, range: R) -> Result<DbRangeIter> {
        self.db
            .collect_range_for_roots(&self.entry.l2p_shard_roots, OwnedRange::new(range))
    }
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
    l2p_shards: &[L2pShard],
    refcount_shards: &[Shard],
    dedup_index: &Lsm,
    dedup_reverse: &Lsm,
    page_store: &Arc<PageStore>,
    lsn: Lsn,
    op: &WalOp,
) -> Result<ApplyOutcome> {
    match op {
        WalOp::L2pPut { lba, value } => {
            let sid = shard_for_key_l2p(l2p_shards, *lba);
            let mut tree = l2p_shards[sid].tree.lock();
            let prev = tree.insert(*lba, *value)?;
            Ok(ApplyOutcome::L2pPrev(prev))
        }
        WalOp::L2pDelete { lba } => {
            let sid = shard_for_key_l2p(l2p_shards, *lba);
            let mut tree = l2p_shards[sid].tree.lock();
            let prev = tree.delete(*lba)?;
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
    }
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
        assert_eq!(db.get(42).unwrap(), None);
        assert!(db.snapshots().is_empty());
        assert_eq!(db.manifest().next_snapshot_id, 1);
    }

    #[test]
    fn create_with_config_uses_requested_shards() {
        let (_d, db) = mk_db_with_shards(4);
        assert_eq!(db.shard_count(), 4);
        assert_eq!(db.manifest().shard_roots.len(), 4);
    }

    #[test]
    fn insert_get_round_trip() {
        let (_d, db) = mk_db();
        db.insert(10, v(7)).unwrap();
        assert_eq!(db.get(10).unwrap(), Some(v(7)));
    }

    #[test]
    fn flush_persists_tree_state_via_manifest() {
        let dir = TempDir::new().unwrap();
        {
            let db = Db::create(dir.path()).unwrap();
            for i in 0u64..500 {
                db.insert(i, v(i as u8)).unwrap();
            }
            db.flush().unwrap();
        }
        let db = Db::open(dir.path()).unwrap();
        for i in 0u64..500 {
            assert_eq!(db.get(i).unwrap(), Some(v(i as u8)));
        }
    }

    #[test]
    fn take_snapshot_assigns_monotonic_ids() {
        let (_d, db) = mk_db();
        let a = db.take_snapshot().unwrap();
        let b = db.take_snapshot().unwrap();
        let c = db.take_snapshot().unwrap();
        assert_eq!(a, 1);
        assert_eq!(b, 2);
        assert_eq!(c, 3);
        assert_eq!(db.snapshots().len(), 3);
    }

    #[test]
    fn snapshot_view_sees_state_at_take_time() {
        let (_d, db) = mk_db();
        for i in 0u64..100 {
            db.insert(i, v(1)).unwrap();
        }
        let snap = db.take_snapshot().unwrap();

        for i in 0u64..100 {
            db.insert(i, v(2)).unwrap();
        }
        db.insert(999, v(9)).unwrap();
        db.delete(50).unwrap();

        assert_eq!(db.get(0).unwrap(), Some(v(2)));
        assert_eq!(db.get(50).unwrap(), None);
        assert_eq!(db.get(999).unwrap(), Some(v(9)));

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
            db.insert(i, v(i as u8)).unwrap();
        }
        let snap = db.take_snapshot().unwrap();
        for i in 0u64..50 {
            db.insert(i, v(99)).unwrap();
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
                db.insert(i, v(1)).unwrap();
            }
            let id = db.take_snapshot().unwrap();
            for i in 0u64..200 {
                db.insert(i, v(2)).unwrap();
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
            assert_eq!(db.get(i).unwrap(), Some(v(2)));
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
            db.insert(i, v(1)).unwrap();
        }
        let a = db.take_snapshot().unwrap();

        db.insert(5, v(99)).unwrap();
        db.delete(3).unwrap();
        db.insert(42, v(7)).unwrap();

        let b = db.take_snapshot().unwrap();
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
            db.insert(i, v(1)).unwrap();
        }
        let a = db.take_snapshot().unwrap();
        db.insert(100, v(5)).unwrap();
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
            db.insert(i, v(1)).unwrap();
        }
        db.flush().unwrap();
        let free_before_snap = db.high_water();
        let s = db.take_snapshot().unwrap();

        for i in 0u64..1000 {
            db.insert(i, v(2)).unwrap();
        }
        db.flush().unwrap();
        let hw_after_writes = db.high_water();
        assert!(hw_after_writes > free_before_snap);

        let report = db.drop_snapshot(s).unwrap().unwrap();
        assert!(report.pages_freed > 0);
        assert_eq!(report.freed_leaf_values.len(), 1000);
        assert!(report.freed_leaf_values.iter().all(|val| *val == v(1)));
        for i in 0u64..1000 {
            assert_eq!(db.get(i).unwrap(), Some(v(2)));
        }
    }

    #[test]
    fn multiple_snapshots_isolated() {
        let (_d, db) = mk_db();
        for i in 0u64..20 {
            db.insert(i, v(1)).unwrap();
        }
        let s1 = db.take_snapshot().unwrap();
        for i in 0u64..20 {
            db.insert(i, v(2)).unwrap();
        }
        let s2 = db.take_snapshot().unwrap();
        for i in 0u64..20 {
            db.insert(i, v(3)).unwrap();
        }

        {
            let v1 = db.snapshot_view(s1).unwrap();
            assert_eq!(v1.get(5).unwrap(), Some(v(1)));
        }
        {
            let v2 = db.snapshot_view(s2).unwrap();
            assert_eq!(v2.get(5).unwrap(), Some(v(2)));
        }
        assert_eq!(db.get(5).unwrap(), Some(v(3)));
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
        let _snap = db.take_snapshot().unwrap();
        // Overwrite refcount state after the snapshot.
        for pba in 0u64..50 {
            db.incref_pba(pba, 1).unwrap();
        }
        for pba in 0u64..50 {
            assert_eq!(db.get_refcount(pba).unwrap(), 2);
        }
        // Phase 6.5b: refcount is a running tally, not a point-in-time
        // value — snapshots only capture L2P. The entry's refcount
        // fields are empty / NULL by design.
        let snap_entry = &db.snapshots()[0];
        assert_ne!(snap_entry.l2p_roots_page, crate::types::NULL_PAGE);
        assert_eq!(snap_entry.refcount_roots_page, crate::types::NULL_PAGE);
        assert!(snap_entry.refcount_shard_roots.is_empty());
    }

    #[test]
    fn drop_snapshot_releases_refcount_state() {
        let (_d, db) = mk_db();
        for pba in 0u64..200 {
            db.incref_pba(pba, 1).unwrap();
        }
        db.flush().unwrap();
        let hw_before_snap = db.high_water();
        let s = db.take_snapshot().unwrap();
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
                db.insert(i, v(i as u8)).unwrap();
            }
            // NO flush() before drop — only WAL is durable.
        }
        let db = Db::open(dir.path()).unwrap();
        for i in 0u64..50 {
            assert_eq!(db.get(i).unwrap(), Some(v(i as u8)));
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
        tx.insert(1, v(1));
        tx.insert(2, v(2));
        tx.incref_pba(10, 3);
        tx.put_dedup(h(1), dv(9));
        let lsn = tx.commit().unwrap();
        assert!(lsn >= 1);
        assert_eq!(db.get(1).unwrap(), Some(v(1)));
        assert_eq!(db.get(2).unwrap(), Some(v(2)));
        assert_eq!(db.get_refcount(10).unwrap(), 3);
        assert_eq!(db.get_dedup(&h(1)).unwrap(), Some(dv(9)));
    }

    #[test]
    fn multi_op_tx_survives_reopen_all_or_nothing() {
        let dir = TempDir::new().unwrap();
        {
            let db = Db::create(dir.path()).unwrap();
            let mut tx = db.begin();
            tx.insert(1, v(1));
            tx.insert(2, v(2));
            tx.incref_pba(10, 3);
            tx.put_dedup(h(1), dv(9));
            tx.commit().unwrap();
        }
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.get(1).unwrap(), Some(v(1)));
        assert_eq!(db.get(2).unwrap(), Some(v(2)));
        assert_eq!(db.get_refcount(10).unwrap(), 3);
        assert_eq!(db.get_dedup(&h(1)).unwrap(), Some(dv(9)));
    }

    #[test]
    fn last_applied_lsn_advances_per_commit() {
        let (_d, db) = mk_db();
        let before = db.last_applied_lsn();
        db.insert(1, v(1)).unwrap();
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
            db.insert(i, v(i as u8)).unwrap();
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
                db.insert(i, v(i as u8)).unwrap();
            }
            db.last_applied_lsn()
        };
        let db = Db::open(dir.path()).unwrap();
        assert_eq!(db.last_applied_lsn(), committed_lsn);
        // New commits after reopen start at committed_lsn + 1.
        db.insert(100, v(0)).unwrap();
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
        assert!(db.multi_get(&[]).unwrap().is_empty());
        assert!(db.multi_get_refcount(&[]).unwrap().is_empty());
        assert!(db.multi_get_dedup(&[]).unwrap().is_empty());
        assert!(db.multi_scan_dedup_reverse_for_pba(&[]).unwrap().is_empty());
    }

    #[test]
    fn multi_get_matches_single_gets_across_shards() {
        // 4 shards so we actually exercise the bucket + group logic.
        let (_d, db) = mk_db_with_shards(4);
        for i in 0u64..200 {
            db.insert(i, v((i as u8).wrapping_mul(3))).unwrap();
        }
        // Mix mapped + unmapped + duplicate keys, in non-sorted order.
        let keys = vec![199, 5000, 0, 199, 42, 10_000, 1, 42];
        let got = db.multi_get(&keys).unwrap();
        assert_eq!(got.len(), keys.len());
        for (i, key) in keys.iter().enumerate() {
            assert_eq!(got[i], db.get(*key).unwrap(), "key {key} mismatch");
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
