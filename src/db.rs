//! Single-partition embedded database: the glue between `PageStore`,
//! `ManifestStore`, and `BTree`.
//!
//! Phase 3 scope:
//! - One tree per `Db`. Multi-partition arrives in a later phase.
//! - No WAL integration yet; durability is provided by `flush()`, which
//!   writes all dirty pages through the page store and commits a fresh
//!   manifest.
//! - `take_snapshot` and `snapshot_view.get` / `.range`; drop_snapshot
//!   and diff land in slices 3d and 3e.

use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::btree::{BTree, L2pValue, RangeIter};
use crate::error::{MetaDbError, Result};
use crate::manifest::{Manifest, ManifestStore, SnapshotEntry};
use crate::page_store::PageStore;
use crate::testing::faults::FaultController;
use crate::types::{Lsn, SnapshotId};

/// Embedded metadata database.
pub struct Db {
    page_store: Arc<PageStore>,
    manifest_store: ManifestStore,
    /// Cached in-memory view of the most-recently-committed manifest.
    manifest: Manifest,
    tree: BTree,
    #[allow(dead_code)]
    faults: Arc<FaultController>,
    #[allow(dead_code)]
    db_path: PathBuf,
}

/// Paths used by a `Db` on disk. Kept internal so the caller only
/// sees a root directory.
fn page_file(root: &Path) -> PathBuf {
    root.join("pages.onyx_meta")
}

impl Db {
    /// Create a fresh database in `root_dir`. The directory is created
    /// if it doesn't already exist.
    pub fn create(root_dir: &Path) -> Result<Self> {
        Self::create_with_faults(root_dir, FaultController::disabled())
    }

    /// As [`create`](Self::create) but with an injectable fault
    /// controller.
    pub fn create_with_faults(root_dir: &Path, faults: Arc<FaultController>) -> Result<Self> {
        std::fs::create_dir_all(root_dir)?;
        let pages_path = page_file(root_dir);
        let page_store = Arc::new(PageStore::create(&pages_path)?);
        let (mut manifest_store, mut manifest) =
            ManifestStore::open_or_create(page_store.clone(), faults.clone())?;
        let tree = BTree::create(page_store.clone())?;
        manifest.partition_root = tree.root();
        manifest_store.commit(&manifest)?;
        Ok(Self {
            page_store,
            manifest_store,
            manifest,
            tree,
            faults,
            db_path: root_dir.to_path_buf(),
        })
    }

    /// Open an existing database from `root_dir`.
    pub fn open(root_dir: &Path) -> Result<Self> {
        Self::open_with_faults(root_dir, FaultController::disabled())
    }

    /// As [`open`](Self::open) but with an injectable fault controller.
    pub fn open_with_faults(root_dir: &Path, faults: Arc<FaultController>) -> Result<Self> {
        let pages_path = page_file(root_dir);
        let page_store = Arc::new(PageStore::open(&pages_path)?);
        let (manifest_store, manifest) =
            ManifestStore::open_or_create(page_store.clone(), faults.clone())?;
        // Pick a next_gen one past the highest generation we've
        // observed in the manifest.
        let next_gen = manifest.checkpoint_lsn.max(1) + 1;
        let tree = BTree::open(page_store.clone(), manifest.partition_root, next_gen)?;
        Ok(Self {
            page_store,
            manifest_store,
            manifest,
            tree,
            faults,
            db_path: root_dir.to_path_buf(),
        })
    }

    /// Current cached manifest (as of the last `flush`).
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Enumerate all registered snapshots.
    pub fn snapshots(&self) -> &[SnapshotEntry] {
        &self.manifest.snapshots
    }

    /// Number of pages currently allocated in the page store.
    pub fn high_water(&self) -> u64 {
        self.page_store.high_water()
    }

    /// Persist dirty tree pages to disk and commit a fresh manifest
    /// with the current tree root + checkpoint LSN.
    pub fn flush(&mut self) -> Result<()> {
        self.tree.flush()?;
        self.manifest.partition_root = self.tree.root();
        self.manifest.checkpoint_lsn = self.tree.next_generation();
        self.manifest_store.commit(&self.manifest)?;
        Ok(())
    }

    // -------- tree operations --------------------------------------------

    pub fn get(&mut self, key: u64) -> Result<Option<L2pValue>> {
        self.tree.get(key)
    }

    pub fn insert(&mut self, key: u64, value: L2pValue) -> Result<Option<L2pValue>> {
        self.tree.insert(key, value)
    }

    pub fn delete(&mut self, key: u64) -> Result<Option<L2pValue>> {
        self.tree.delete(key)
    }

    pub fn range<R: RangeBounds<u64>>(&mut self, range: R) -> Result<RangeIter<'_>> {
        self.tree.range(range)
    }

    // -------- snapshot operations -----------------------------------------

    /// Take a snapshot of the current tree. Returns the new snapshot
    /// id. The snapshot pins the current root page; subsequent writes
    /// CoW the descent path, so the snapshot keeps its historical
    /// view. Persisted immediately via a manifest commit.
    pub fn take_snapshot(&mut self) -> Result<SnapshotId> {
        let id = self.manifest.next_snapshot_id;
        let root = self.tree.root();
        let created_lsn = self.tree.next_generation();

        // Bump the root's refcount so CoW descent recognises it as
        // shared. Must happen before the manifest commit so a crash
        // mid-operation leaves disk state consistent: either (a) pre-
        // commit — snapshot not durable, extra refcount on root
        // (harmless, dropped at next flush) — or (b) post-commit —
        // snapshot durable, refcount and commit aligned.
        self.tree.incref_root_for_snapshot()?;

        self.manifest.snapshots.push(SnapshotEntry {
            id,
            root,
            created_lsn,
        });
        self.manifest.next_snapshot_id = id
            .checked_add(1)
            .ok_or_else(|| MetaDbError::Corruption("snapshot id overflow".into()))?;

        // Flush tree + manifest so the snapshot is durable.
        self.flush()?;
        Ok(id)
    }

    /// Open a read-only view of the data as it existed when `id` was
    /// taken. Returns `None` if the snapshot id is unknown.
    pub fn snapshot_view(&mut self, id: SnapshotId) -> Option<SnapshotView<'_>> {
        let entry = self.manifest.find_snapshot(id).copied()?;
        Some(SnapshotView {
            tree: &mut self.tree,
            entry,
        })
    }
}

/// Read-only view of the tree as it existed when a snapshot was
/// taken.
pub struct SnapshotView<'a> {
    tree: &'a mut BTree,
    entry: SnapshotEntry,
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
    pub fn get(&mut self, key: u64) -> Result<Option<L2pValue>> {
        self.tree.get_at(self.entry.root, key)
    }

    /// Range scan as of the snapshot's LSN.
    pub fn range<R: RangeBounds<u64>>(&mut self, range: R) -> Result<RangeIter<'_>> {
        self.tree.range_at(self.entry.root, range)
    }
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

    #[test]
    fn fresh_db_is_empty() {
        let (_d, mut db) = mk_db();
        assert_eq!(db.get(42).unwrap(), None);
        assert!(db.snapshots().is_empty());
        assert_eq!(db.manifest().next_snapshot_id, 1);
    }

    #[test]
    fn insert_get_round_trip() {
        let (_d, mut db) = mk_db();
        db.insert(10, v(7)).unwrap();
        assert_eq!(db.get(10).unwrap(), Some(v(7)));
    }

    #[test]
    fn flush_persists_tree_state_via_manifest() {
        let dir = TempDir::new().unwrap();
        {
            let mut db = Db::create(dir.path()).unwrap();
            for i in 0u64..500 {
                db.insert(i, v(i as u8)).unwrap();
            }
            db.flush().unwrap();
        }
        let mut db = Db::open(dir.path()).unwrap();
        for i in 0u64..500 {
            assert_eq!(db.get(i).unwrap(), Some(v(i as u8)));
        }
    }

    #[test]
    fn take_snapshot_assigns_monotonic_ids() {
        let (_d, mut db) = mk_db();
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
        let (_d, mut db) = mk_db();
        for i in 0u64..100 {
            db.insert(i, v(1)).unwrap();
        }
        let snap = db.take_snapshot().unwrap();

        // Mutate after snapshot.
        for i in 0u64..100 {
            db.insert(i, v(2)).unwrap();
        }
        db.insert(999, v(9)).unwrap();
        db.delete(50).unwrap();

        // Current view reflects the mutations.
        assert_eq!(db.get(0).unwrap(), Some(v(2)));
        assert_eq!(db.get(50).unwrap(), None);
        assert_eq!(db.get(999).unwrap(), Some(v(9)));

        // Snapshot view sees the pre-mutation state.
        let mut view = db.snapshot_view(snap).unwrap();
        for i in 0u64..100 {
            assert_eq!(view.get(i).unwrap(), Some(v(1)));
        }
        assert_eq!(view.get(999).unwrap(), None);
    }

    #[test]
    fn snapshot_view_range_scan() {
        let (_d, mut db) = mk_db();
        for i in 0u64..50 {
            db.insert(i, v(i as u8)).unwrap();
        }
        let snap = db.take_snapshot().unwrap();
        // Mutate some entries.
        for i in 0u64..50 {
            db.insert(i, v(99)).unwrap();
        }

        let mut view = db.snapshot_view(snap).unwrap();
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
            let mut db = Db::create(dir.path()).unwrap();
            for i in 0u64..200 {
                db.insert(i, v(1)).unwrap();
            }
            let id = db.take_snapshot().unwrap();
            // Mutate after snapshot.
            for i in 0u64..200 {
                db.insert(i, v(2)).unwrap();
            }
            db.flush().unwrap();
            id
        };

        let mut db = Db::open(dir.path()).unwrap();
        let snaps = db.snapshots().to_vec();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].id, snap_id);

        let mut view = db.snapshot_view(snap_id).unwrap();
        for i in 0u64..200 {
            assert_eq!(view.get(i).unwrap(), Some(v(1)));
        }

        // Current state unchanged by reopen.
        for i in 0u64..200 {
            assert_eq!(db.get(i).unwrap(), Some(v(2)));
        }
    }

    #[test]
    fn snapshot_view_missing_id_returns_none() {
        let (_d, mut db) = mk_db();
        assert!(db.snapshot_view(999).is_none());
    }

    #[test]
    fn multiple_snapshots_isolated() {
        let (_d, mut db) = mk_db();
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
            let mut v1 = db.snapshot_view(s1).unwrap();
            assert_eq!(v1.get(5).unwrap(), Some(v(1)));
        }
        {
            let mut v2 = db.snapshot_view(s2).unwrap();
            assert_eq!(v2.get(5).unwrap(), Some(v(2)));
        }
        assert_eq!(db.get(5).unwrap(), Some(v(3)));
    }
}
