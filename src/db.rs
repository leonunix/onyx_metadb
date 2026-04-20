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
use crate::manifest::{
    MANIFEST_BODY_VERSION, Manifest, ManifestStore, SnapshotEntry, materialize_snapshot_root_pages,
    write_snapshot_roots_page,
};
use crate::page_store::PageStore;
use crate::testing::faults::FaultController;
use crate::types::{Lsn, PageId, SnapshotId};

/// Embedded metadata database.
pub struct Db {
    page_store: Arc<PageStore>,
    manifest_state: Mutex<ManifestState>,
    shards: Vec<Shard>,
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
        let (shards, roots) = create_shards(page_store.clone(), shard_count)?;
        manifest.body_version = MANIFEST_BODY_VERSION;
        manifest.shard_roots = roots;
        manifest_store.commit(&manifest)?;
        Ok(Self {
            page_store,
            manifest_state: Mutex::new(ManifestState {
                store: manifest_store,
                manifest,
            }),
            shards,
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
        if materialize_snapshot_root_pages(&page_store, &mut manifest, next_gen)? {
            manifest_store.commit(&manifest)?;
        }
        let shards = open_shards(page_store.clone(), &manifest.shard_roots, next_gen)?;

        Ok(Self {
            page_store,
            manifest_state: Mutex::new(ManifestState {
                store: manifest_store,
                manifest,
            }),
            shards,
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
    pub fn flush(&self) -> Result<()> {
        let mut manifest_state = self.manifest_state.lock();
        let mut guards = self.lock_all_shards();
        self.flush_locked_shards(&mut guards)?;
        self.refresh_manifest_from_locked(&mut manifest_state.manifest, &guards);
        let manifest = manifest_state.manifest.clone();
        manifest_state.store.commit(&manifest)?;
        Ok(())
    }

    // -------- tree operations --------------------------------------------

    pub fn get(&self, key: u64) -> Result<Option<L2pValue>> {
        let sid = self.shard_for(key);
        let mut tree = self.shards[sid].tree.lock();
        tree.get(key)
    }

    pub fn insert(&self, key: u64, value: L2pValue) -> Result<Option<L2pValue>> {
        let sid = self.shard_for(key);
        let mut tree = self.shards[sid].tree.lock();
        tree.insert(key, value)
    }

    pub fn delete(&self, key: u64) -> Result<Option<L2pValue>> {
        let sid = self.shard_for(key);
        let mut tree = self.shards[sid].tree.lock();
        tree.delete(key)
    }

    pub fn range<R: RangeBounds<u64>>(&self, range: R) -> Result<DbRangeIter> {
        let range = OwnedRange::new(range);
        let mut guards = self.lock_all_shards();
        let mut items = Vec::new();
        for tree in &mut guards {
            items.extend(tree.range(range.clone())?.collect::<Result<Vec<_>>>()?);
        }
        items.sort_unstable_by_key(|(k, _)| *k);
        Ok(DbRangeIter::new(items))
    }

    // -------- snapshot operations -----------------------------------------

    /// Take a snapshot of the current sharded tree. Returns the new
    /// snapshot id. Persisted immediately via a manifest commit.
    pub fn take_snapshot(&self) -> Result<SnapshotId> {
        let mut manifest_state = self.manifest_state.lock();
        let mut guards = self.lock_all_shards();

        let id = manifest_state.manifest.next_snapshot_id;
        let mut roots = Vec::with_capacity(guards.len());
        for tree in &mut guards {
            tree.incref_root_for_snapshot()?;
            roots.push(tree.root());
        }
        let created_lsn = max_generation_from_locked(&guards);
        let roots_page = write_snapshot_roots_page(&self.page_store, &roots, created_lsn)?;

        self.flush_locked_shards(&mut guards)?;
        self.refresh_manifest_from_locked(&mut manifest_state.manifest, &guards);
        manifest_state.manifest.snapshots.push(SnapshotEntry {
            id,
            roots_page,
            created_lsn,
            shard_roots: roots.into_boxed_slice(),
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
                .shard_roots
                .clone();
            let b_roots = manifest_state
                .manifest
                .find_snapshot(b)
                .ok_or_else(|| MetaDbError::InvalidArgument(format!("unknown snapshot id {b}")))?
                .shard_roots
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
                .shard_roots
                .clone()
        };

        let mut guards = self.lock_all_shards();
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

        let mut guards = self.lock_all_shards();
        self.flush_locked_shards(&mut guards)?;
        manifest_state.manifest.snapshots.remove(pos);
        self.refresh_manifest_from_locked(&mut manifest_state.manifest, &guards);
        let manifest = manifest_state.manifest.clone();
        manifest_state.store.commit(&manifest)?;

        let mut values = Vec::new();
        for (tree, root) in guards.iter_mut().zip(entry.shard_roots.iter().copied()) {
            values.extend(tree.drop_subtree(root)?);
        }
        self.flush_locked_shards(&mut guards)?;
        self.refresh_manifest_from_locked(&mut manifest_state.manifest, &guards);

        let generation = max_generation_from_locked(&guards);
        self.page_store.free(entry.roots_page, generation)?;
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

    fn lock_all_shards(&self) -> Vec<MutexGuard<'_, BTree>> {
        self.shards.iter().map(|shard| shard.tree.lock()).collect()
    }

    fn flush_locked_shards(&self, guards: &mut [MutexGuard<'_, BTree>]) -> Result<()> {
        for tree in guards {
            tree.flush()?;
        }
        Ok(())
    }

    fn refresh_manifest_from_locked(
        &self,
        manifest: &mut Manifest,
        guards: &[MutexGuard<'_, BTree>],
    ) {
        manifest.body_version = MANIFEST_BODY_VERSION;
        manifest.shard_roots = guards
            .iter()
            .map(|tree| tree.root())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        manifest.checkpoint_lsn = max_generation_from_locked(guards);
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
        tree.get_at(self.entry.shard_roots[sid], key)
    }

    /// Range scan as of the snapshot's LSN.
    pub fn range<R: RangeBounds<u64>>(&self, range: R) -> Result<DbRangeIter> {
        self.db
            .collect_range_for_roots(&self.entry.shard_roots, OwnedRange::new(range))
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
}
