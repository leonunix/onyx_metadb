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

use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard};
use xxhash_rust::xxh3::xxh3_64;

use crate::btree::{BTree, DiffEntry, L2pValue};
use crate::config::Config;
use crate::error::{MetaDbError, Result};
use crate::lsm::{DedupValue, Hash32, Lsm, LsmConfig};
use crate::manifest::{
    MANIFEST_BODY_VERSION, Manifest, ManifestStore, SnapshotEntry, materialize_snapshot_root_pages,
    write_snapshot_roots_page,
};
use crate::page_store::PageStore;
use crate::testing::faults::FaultController;
use crate::tx::{ApplyOutcome, Transaction};
use crate::types::{Lsn, PageId, Pba, SnapshotId};
use crate::wal::{Wal, WalOp, encode_body};

/// Embedded metadata database.
pub struct Db {
    page_store: Arc<PageStore>,
    manifest_state: Mutex<ManifestState>,
    /// L2P B+tree shards (LBA → `L2pValue`).
    shards: Vec<Shard>,
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
    /// Serialises WAL submit + apply so LSN order trivially equals apply
    /// order. See [`tx`](crate::tx) for the reasoning.
    commit_lock: Mutex<()>,
    /// LSN of the most recent op applied to in-memory state. Initialised
    /// from `manifest.checkpoint_lsn` on open (the manifest promises that
    /// every LSN at or below this value is already reflected in the
    /// trees / SSTs) and bumped on every commit.
    last_applied_lsn: Mutex<Lsn>,
    /// Snapshot readers hold a shared guard; `drop_snapshot` takes the
    /// exclusive side so it can't free pages still visible to a live view.
    snapshot_views: RwLock<()>,
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
        let (mut manifest_store, mut manifest) =
            ManifestStore::open_or_create(page_store.clone(), faults.clone())?;
        let (l2p_shards, l2p_roots) = create_shards(page_store.clone(), shard_count)?;
        let (refcount_shards, refcount_roots) = create_shards(page_store.clone(), shard_count)?;
        let dedup_index = Lsm::create(page_store.clone(), LsmConfig::default());
        let dedup_reverse = Lsm::create(page_store.clone(), LsmConfig::default());
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
            manifest_state: Mutex::new(ManifestState {
                store: manifest_store,
                manifest,
            }),
            shards: l2p_shards,
            refcount_shards,
            dedup_index,
            dedup_reverse,
            wal,
            commit_lock: Mutex::new(()),
            last_applied_lsn: Mutex::new(0),
            snapshot_views: RwLock::new(()),
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
        let (mut manifest_store, mut manifest) =
            ManifestStore::open_or_create(page_store.clone(), faults.clone())?;
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
            let (_owned, roots) = create_shards(page_store.clone(), manifest.shard_count())?;
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

        let l2p_shards = open_shards(page_store.clone(), &manifest.shard_roots, next_gen)?;
        let refcount_shards =
            open_shards(page_store.clone(), &manifest.refcount_shard_roots, next_gen)?;
        let dedup_index = Lsm::open(
            page_store.clone(),
            LsmConfig::default(),
            &manifest.dedup_level_heads,
        )?;
        let dedup_reverse = Lsm::open(
            page_store.clone(),
            LsmConfig::default(),
            &manifest.dedup_reverse_level_heads,
        )?;

        // Replay WAL segments forward from checkpoint_lsn+1 onto the
        // freshly-opened in-memory state. Applies every op exactly the
        // way a live commit would. The result tells us the LSN of the
        // last cleanly-decoded record so the new WAL can resume there.
        let wal_path = wal_dir(&cfg.path);
        let from_lsn = manifest.checkpoint_lsn + 1;
        let replay_outcome = crate::recovery::replay_into(&wal_path, from_lsn, |op| {
            apply_op_bare(
                &l2p_shards,
                &refcount_shards,
                &dedup_index,
                &dedup_reverse,
                op,
            )
        })?;
        let last_applied = replay_outcome.last_lsn.unwrap_or(manifest.checkpoint_lsn);
        // If the last segment ended torn, truncate it to the last clean
        // record before handing the directory to the new Wal.
        crate::recovery::truncate_torn_tail(&wal_path, &replay_outcome)?;
        let wal = Wal::create(&wal_path, &cfg, last_applied + 1, faults.clone())?;

        Ok(Self {
            page_store,
            manifest_state: Mutex::new(ManifestState {
                store: manifest_store,
                manifest,
            }),
            shards: l2p_shards,
            refcount_shards,
            dedup_index,
            dedup_reverse,
            wal,
            commit_lock: Mutex::new(()),
            last_applied_lsn: Mutex::new(last_applied),
            snapshot_views: RwLock::new(()),
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

    /// Persist dirty shard pages and commit a fresh manifest with the
    /// current per-shard roots + checkpoint LSN.
    ///
    /// `checkpoint_lsn` is set to the WAL LSN of the most-recently-
    /// applied commit, so after `open` replay can correctly begin at
    /// `checkpoint_lsn + 1`.
    pub fn flush(&self) -> Result<()> {
        // Serialise with any in-flight commit: reading `last_applied_lsn`
        // outside of `commit_lock` could race with `commit_ops` between
        // "apply to trees" and "bump last_applied". Taking the lock
        // ensures all applied-but-not-yet-bumped LSNs are accounted for.
        let _commit_guard = self.commit_lock.lock();
        let mut manifest_state = self.manifest_state.lock();
        let mut l2p_guards = self.lock_all_l2p_shards();
        let mut refcount_guards = self.lock_all_refcount_shards();
        self.flush_locked_shards(&mut l2p_guards)?;
        self.flush_locked_shards(&mut refcount_guards)?;

        let tree_generation = max_generation_from_two_groups(&l2p_guards, &refcount_guards);
        let wal_checkpoint = *self.last_applied_lsn.lock();

        // Flush each LSM's memtable to an L0 SST BEFORE advancing
        // `checkpoint_lsn`. Without this, a memtable put at LSN
        // `L <= checkpoint_lsn` would be reflected neither in SSTs
        // (memtable never spilled) nor in WAL replay (replay starts at
        // `checkpoint_lsn + 1`), and the write would silently vanish
        // on reopen.
        self.dedup_index.flush_memtable(tree_generation)?;
        self.dedup_reverse.flush_memtable(tree_generation)?;

        let old_dedup_heads: Vec<PageId> = manifest_state.manifest.dedup_level_heads.to_vec();
        let old_dedup_reverse_heads: Vec<PageId> =
            manifest_state.manifest.dedup_reverse_level_heads.to_vec();
        let new_dedup_heads = self.dedup_index.persist_levels(tree_generation)?;
        let new_dedup_reverse_heads = self.dedup_reverse.persist_levels(tree_generation)?;

        self.refresh_manifest_from_locked(
            &mut manifest_state.manifest,
            &l2p_guards,
            &refcount_guards,
        );
        // The tree generation is a local monotonic counter; checkpoint
        // LSN must be the durable WAL LSN, not the tree counter.
        manifest_state.manifest.checkpoint_lsn = wal_checkpoint;
        manifest_state.manifest.dedup_level_heads = new_dedup_heads.clone().into_boxed_slice();
        manifest_state.manifest.dedup_reverse_level_heads =
            new_dedup_reverse_heads.clone().into_boxed_slice();
        let manifest = manifest_state.manifest.clone();
        manifest_state.store.commit(&manifest)?;

        // Commit succeeded; free the old level chains for both LSMs.
        self.dedup_index
            .free_old_level_heads(&old_dedup_heads, tree_generation)?;
        self.dedup_reverse
            .free_old_level_heads(&old_dedup_reverse_heads, tree_generation)?;
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
    pub(crate) fn commit_ops(&self, ops: &[WalOp]) -> Result<(Lsn, Vec<ApplyOutcome>)> {
        if ops.is_empty() {
            return Ok((self.last_applied_lsn(), Vec::new()));
        }
        let body = encode_body(ops);
        let _guard = self.commit_lock.lock();
        let lsn = self.wal.submit(body)?;
        let mut outcomes = Vec::with_capacity(ops.len());
        for op in ops {
            outcomes.push(self.apply_op(op)?);
        }
        *self.last_applied_lsn.lock() = lsn;
        Ok((lsn, outcomes))
    }

    fn apply_op(&self, op: &WalOp) -> Result<ApplyOutcome> {
        apply_op_bare(
            &self.shards,
            &self.refcount_shards,
            &self.dedup_index,
            &self.dedup_reverse,
            op,
        )
    }

    // -------- refcount + dedup ops --------------------------------------

    /// Return the current refcount for `pba`, or 0 if no entry exists.
    pub fn get_refcount(&self, pba: Pba) -> Result<u32> {
        let sid = self.refcount_shard_for(pba);
        let mut tree = self.refcount_shards[sid].tree.lock();
        Ok(tree.get(pba)?.map(refcount_from_value).unwrap_or(0))
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
        // Serialise with in-flight commits so `last_applied_lsn` is
        // stable while we sample tree roots and write the manifest.
        let _commit_guard = self.commit_lock.lock();
        let mut manifest_state = self.manifest_state.lock();
        let mut l2p_guards = self.lock_all_l2p_shards();
        let mut refcount_guards = self.lock_all_refcount_shards();

        let id = manifest_state.manifest.next_snapshot_id;
        let mut l2p_roots = Vec::with_capacity(l2p_guards.len());
        for tree in &mut l2p_guards {
            tree.incref_root_for_snapshot()?;
            l2p_roots.push(tree.root());
        }
        let mut refcount_roots = Vec::with_capacity(refcount_guards.len());
        for tree in &mut refcount_guards {
            tree.incref_root_for_snapshot()?;
            refcount_roots.push(tree.root());
        }
        let created_lsn = max_generation_from_two_groups(&l2p_guards, &refcount_guards);
        let l2p_roots_page = write_snapshot_roots_page(&self.page_store, &l2p_roots, created_lsn)?;
        let refcount_roots_page =
            write_snapshot_roots_page(&self.page_store, &refcount_roots, created_lsn)?;

        self.flush_locked_shards(&mut l2p_guards)?;
        self.flush_locked_shards(&mut refcount_guards)?;
        self.refresh_manifest_from_locked(
            &mut manifest_state.manifest,
            &l2p_guards,
            &refcount_guards,
        );
        manifest_state.manifest.checkpoint_lsn = *self.last_applied_lsn.lock();
        manifest_state.manifest.snapshots.push(SnapshotEntry {
            id,
            l2p_roots_page,
            refcount_roots_page,
            created_lsn,
            l2p_shard_roots: l2p_roots.into_boxed_slice(),
            refcount_shard_roots: refcount_roots.into_boxed_slice(),
        });
        manifest_state.manifest.next_snapshot_id = id
            .checked_add(1)
            .ok_or_else(|| MetaDbError::Corruption("snapshot id overflow".into()))?;
        let manifest = manifest_state.manifest.clone();
        manifest_state.store.commit(&manifest)?;
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

    /// Drop a snapshot. The manifest entry is removed first; the page
    /// reclamation work then runs while shard locks are still held. This
    /// prefers leaks over exposing a dropped snapshot that points at freed
    /// pages.
    pub fn drop_snapshot(&self, id: SnapshotId) -> Result<Option<DropReport>> {
        let _commit_guard = self.commit_lock.lock();
        let _drop_guard = self.snapshot_views.write();
        let mut manifest_state = self.manifest_state.lock();
        let Some(pos) = manifest_state
            .manifest
            .snapshots
            .iter()
            .position(|entry| entry.id == id)
        else {
            return Ok(None);
        };
        let entry = manifest_state.manifest.snapshots[pos].clone();
        let free_before = self.page_store.free_list_len();

        let mut l2p_guards = self.lock_all_l2p_shards();
        let mut refcount_guards = self.lock_all_refcount_shards();
        self.flush_locked_shards(&mut l2p_guards)?;
        self.flush_locked_shards(&mut refcount_guards)?;
        manifest_state.manifest.snapshots.remove(pos);
        self.refresh_manifest_from_locked(
            &mut manifest_state.manifest,
            &l2p_guards,
            &refcount_guards,
        );
        manifest_state.manifest.checkpoint_lsn = *self.last_applied_lsn.lock();
        let manifest = manifest_state.manifest.clone();
        manifest_state.store.commit(&manifest)?;

        let mut values = Vec::new();
        for (tree, root) in l2p_guards
            .iter_mut()
            .zip(entry.l2p_shard_roots.iter().copied())
        {
            values.extend(tree.drop_subtree(root)?);
        }
        if !entry.refcount_shard_roots.is_empty() {
            for (tree, root) in refcount_guards
                .iter_mut()
                .zip(entry.refcount_shard_roots.iter().copied())
            {
                // Refcount subtree values are discarded; the caller
                // doesn't care about refcount-at-snapshot values, only
                // about freeing the pages.
                let _ = tree.drop_subtree(root)?;
            }
        }
        self.flush_locked_shards(&mut l2p_guards)?;
        self.flush_locked_shards(&mut refcount_guards)?;
        self.refresh_manifest_from_locked(
            &mut manifest_state.manifest,
            &l2p_guards,
            &refcount_guards,
        );
        manifest_state.manifest.checkpoint_lsn = *self.last_applied_lsn.lock();

        let generation = max_generation_from_two_groups(&l2p_guards, &refcount_guards);
        self.page_store.free(entry.l2p_roots_page, generation)?;
        if entry.refcount_roots_page != crate::types::NULL_PAGE {
            self.page_store
                .free(entry.refcount_roots_page, generation)?;
        }
        let pages_freed = self.page_store.free_list_len().saturating_sub(free_before);

        Ok(Some(DropReport {
            snapshot_id: id,
            freed_leaf_values: values,
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

    fn lock_all_l2p_shards(&self) -> Vec<MutexGuard<'_, BTree>> {
        self.shards.iter().map(|shard| shard.tree.lock()).collect()
    }

    fn lock_all_refcount_shards(&self) -> Vec<MutexGuard<'_, BTree>> {
        self.refcount_shards
            .iter()
            .map(|shard| shard.tree.lock())
            .collect()
    }

    fn flush_locked_shards(&self, guards: &mut [MutexGuard<'_, BTree>]) -> Result<()> {
        for tree in guards {
            tree.flush()?;
        }
        Ok(())
    }

    /// Refresh manifest fields that mirror in-memory state.
    ///
    /// Does NOT touch `checkpoint_lsn` — that is the durable-WAL LSN
    /// cursor and is only ever advanced by code paths that have taken
    /// `commit_lock` and therefore have an authoritative reading of
    /// `last_applied_lsn`.
    fn refresh_manifest_from_locked(
        &self,
        manifest: &mut Manifest,
        l2p_guards: &[MutexGuard<'_, BTree>],
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
    shard_count: usize,
) -> Result<(Vec<Shard>, Box<[PageId]>)> {
    let mut shards = Vec::with_capacity(shard_count);
    let mut roots = Vec::with_capacity(shard_count);
    for _ in 0..shard_count {
        let tree = BTree::create(page_store.clone())?;
        roots.push(tree.root());
        shards.push(Shard {
            tree: Mutex::new(tree),
        });
    }
    Ok((shards, roots.into_boxed_slice()))
}

fn open_shards(page_store: Arc<PageStore>, roots: &[PageId], next_gen: Lsn) -> Result<Vec<Shard>> {
    let mut shards = Vec::with_capacity(roots.len());
    for &root in roots {
        shards.push(Shard {
            tree: Mutex::new(BTree::open(page_store.clone(), root, next_gen)?),
        });
    }
    Ok(shards)
}

fn max_generation_from_locked(guards: &[MutexGuard<'_, BTree>]) -> Lsn {
    guards
        .iter()
        .map(|tree| tree.next_generation())
        .max()
        .unwrap_or(0)
}

fn max_generation_from_two_groups(a: &[MutexGuard<'_, BTree>], b: &[MutexGuard<'_, BTree>]) -> Lsn {
    max_generation_from_locked(a).max(max_generation_from_locked(b))
}

fn refcount_from_value(v: L2pValue) -> u32 {
    u32::from_be_bytes([v.0[0], v.0[1], v.0[2], v.0[3]])
}

fn refcount_to_value(count: u32) -> L2pValue {
    let mut bytes = [0u8; 28];
    bytes[..4].copy_from_slice(&count.to_be_bytes());
    L2pValue(bytes)
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
fn apply_op_bare(
    l2p_shards: &[Shard],
    refcount_shards: &[Shard],
    dedup_index: &Lsm,
    dedup_reverse: &Lsm,
    op: &WalOp,
) -> Result<ApplyOutcome> {
    match *op {
        WalOp::L2pPut { lba, value } => {
            let sid = shard_for_key(l2p_shards, lba);
            let mut tree = l2p_shards[sid].tree.lock();
            let prev = tree.insert(lba, value)?;
            Ok(ApplyOutcome::L2pPrev(prev))
        }
        WalOp::L2pDelete { lba } => {
            let sid = shard_for_key(l2p_shards, lba);
            let mut tree = l2p_shards[sid].tree.lock();
            let prev = tree.delete(lba)?;
            Ok(ApplyOutcome::L2pPrev(prev))
        }
        WalOp::DedupPut { hash, value } => {
            dedup_index.put(hash, value);
            Ok(ApplyOutcome::Dedup)
        }
        WalOp::DedupDelete { hash } => {
            dedup_index.delete(hash);
            Ok(ApplyOutcome::Dedup)
        }
        WalOp::DedupReversePut { pba, hash } => {
            let (key, value) = encode_reverse_entry(pba, &hash);
            dedup_reverse.put(key, value);
            Ok(ApplyOutcome::Dedup)
        }
        WalOp::DedupReverseDelete { pba, hash } => {
            let (key, _) = encode_reverse_entry(pba, &hash);
            dedup_reverse.delete(key);
            Ok(ApplyOutcome::Dedup)
        }
        WalOp::Incref { pba, delta } => {
            let sid = shard_for_key(refcount_shards, pba);
            let mut tree = refcount_shards[sid].tree.lock();
            let current = tree.get(pba)?.map(refcount_from_value).unwrap_or(0);
            let new = current.checked_add(delta).ok_or_else(|| {
                MetaDbError::InvalidArgument(format!("refcount overflow for pba {pba}"))
            })?;
            if new != 0 {
                tree.insert(pba, refcount_to_value(new))?;
            }
            Ok(ApplyOutcome::RefcountNew(new))
        }
        WalOp::Decref { pba, delta } => {
            let sid = shard_for_key(refcount_shards, pba);
            let mut tree = refcount_shards[sid].tree.lock();
            let current = tree.get(pba)?.map(refcount_from_value).unwrap_or(0);
            let new = current.checked_sub(delta).ok_or_else(|| {
                MetaDbError::InvalidArgument(format!(
                    "decref underflow for pba {pba}: {current} - {delta}",
                ))
            })?;
            if new == 0 {
                tree.delete(pba)?;
            } else {
                tree.insert(pba, refcount_to_value(new))?;
            }
            Ok(ApplyOutcome::RefcountNew(new))
        }
    }
}

fn shard_for_key(shards: &[Shard], key: u64) -> usize {
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
        // Snapshot-view access to refcount tree isn't yet exposed, but
        // we can at least verify the snapshot was persisted with both
        // root pages recorded.
        let snap_entry = &db.snapshots()[0];
        assert_ne!(snap_entry.l2p_roots_page, crate::types::NULL_PAGE);
        assert_ne!(snap_entry.refcount_roots_page, crate::types::NULL_PAGE);
        assert_eq!(snap_entry.refcount_shard_roots.len(), db.shard_count());
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
        let report = db.drop_snapshot(s).unwrap().unwrap();
        assert!(report.pages_freed > 0);
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
}
