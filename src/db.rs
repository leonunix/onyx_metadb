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

use crate::btree::{BTree, DiffEntry, L2pValue, RangeIter};
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

    /// Compute the diff between two snapshots. Returns
    /// `Err(InvalidArgument)` if either id is unknown. Empty diff
    /// means the two snapshots see identical keyspace content.
    pub fn diff(&mut self, a: SnapshotId, b: SnapshotId) -> Result<Vec<DiffEntry>> {
        let a_root = self
            .manifest
            .find_snapshot(a)
            .ok_or_else(|| MetaDbError::InvalidArgument(format!("unknown snapshot id {a}")))?
            .root;
        let b_root = self
            .manifest
            .find_snapshot(b)
            .ok_or_else(|| MetaDbError::InvalidArgument(format!("unknown snapshot id {b}")))?
            .root;
        self.tree.diff_subtrees(a_root, b_root)
    }

    /// Diff a snapshot against the current tree.
    pub fn diff_with_current(&mut self, snap: SnapshotId) -> Result<Vec<DiffEntry>> {
        let snap_root = self
            .manifest
            .find_snapshot(snap)
            .ok_or_else(|| MetaDbError::InvalidArgument(format!("unknown snapshot id {snap}")))?
            .root;
        let cur_root = self.tree.root();
        self.tree.diff_subtrees(snap_root, cur_root)
    }

    /// Drop a snapshot. Walks pages uniquely owned by the snapshot,
    /// freeing them; pages still reachable from the current tree or
    /// another snapshot are left alone. Returns a [`DropReport`] with
    /// the values from freed leaves so the caller (e.g. Onyx) can
    /// decrement its own PBA refcounts on the data those values
    /// pointed at.
    pub fn drop_snapshot(&mut self, id: SnapshotId) -> Result<Option<DropReport>> {
        let Some(entry) = self.manifest.find_snapshot(id).copied() else {
            return Ok(None);
        };
        let free_before = self.page_store.free_list_len();
        let values = self.tree.drop_subtree(entry.root)?;
        let pages_freed = self.page_store.free_list_len().saturating_sub(free_before);

        self.manifest.snapshots.retain(|e| e.id != id);
        self.flush()?;

        Ok(Some(DropReport {
            snapshot_id: id,
            freed_leaf_values: values,
            pages_freed,
        }))
    }
}

/// Result of [`Db::drop_snapshot`]. The embedder uses this to
/// propagate page-level reclamation into its own higher-level
/// storage accounting (for Onyx: the PBA refcount table and
/// data-plane allocator).
#[derive(Clone, Debug)]
pub struct DropReport {
    /// Id of the snapshot that was dropped.
    pub snapshot_id: SnapshotId,
    /// Every value stored in leaves that were uniquely owned by this
    /// snapshot (and therefore freed). These values have lost their
    /// reference from the metadb layer; the embedder decides what to
    /// do with the PBAs they encode.
    pub freed_leaf_values: Vec<L2pValue>,
    /// Number of metadb pages (both leaves and internals) released
    /// back to the page store's free list by this drop.
    pub pages_freed: usize,
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

    // -------- diff --------

    #[test]
    fn diff_identical_snapshots_is_empty() {
        let (_d, mut db) = mk_db();
        for i in 0u64..100 {
            db.insert(i, v(1)).unwrap();
        }
        let a = db.take_snapshot().unwrap();
        let b = db.take_snapshot().unwrap();
        let diff = db.diff(a, b).unwrap();
        assert!(diff.is_empty());
    }

    #[test]
    fn diff_detects_added_removed_changed() {
        let (_d, mut db) = mk_db();
        for i in 0u64..10 {
            db.insert(i, v(1)).unwrap();
        }
        let a = db.take_snapshot().unwrap();

        // Current diffs from snapshot A.
        db.insert(5, v(99)).unwrap(); // changed
        db.delete(3).unwrap(); // removed
        db.insert(42, v(7)).unwrap(); // added

        let b = db.take_snapshot().unwrap();
        let diff = db.diff(a, b).unwrap();
        // Diff is key-ordered.
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
        let (_d, mut db) = mk_db();
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
    fn diff_on_large_mostly_unchanged_tree() {
        // Take a snapshot after filling a deep tree, then change ten
        // keys. The diff must return exactly those ten entries.
        let (_d, mut db) = mk_db();
        for i in 0u64..2000 {
            db.insert(i, v(1)).unwrap();
        }
        let a = db.take_snapshot().unwrap();
        for k in [0u64, 10, 50, 200, 500, 800, 999, 1234, 1800, 1999] {
            db.insert(k, v(99)).unwrap();
        }
        let b = db.take_snapshot().unwrap();
        let diff = db.diff(a, b).unwrap();
        assert_eq!(diff.len(), 10);
        for entry in diff {
            match entry {
                DiffEntry::Changed { old, new, .. } => {
                    assert_eq!(old, v(1));
                    assert_eq!(new, v(99));
                }
                other => panic!("unexpected: {other:?}"),
            }
        }
    }

    #[test]
    fn diff_rejects_unknown_snapshot() {
        let (_d, mut db) = mk_db();
        let a = db.take_snapshot().unwrap();
        assert!(db.diff(a, 999).is_err());
        assert!(db.diff(999, a).is_err());
    }

    // -------- drop_snapshot --------

    #[test]
    fn drop_snapshot_returns_none_for_unknown_id() {
        let (_d, mut db) = mk_db();
        assert!(db.drop_snapshot(999).unwrap().is_none());
    }

    #[test]
    fn drop_snapshot_on_untouched_snapshot_reclaims_no_pages() {
        // A snapshot taken on an empty tree and dropped immediately
        // (before any writes) shares its only page with the current
        // tree, so dropping shouldn't free anything.
        let (_d, mut db) = mk_db();
        let s = db.take_snapshot().unwrap();
        let report = db.drop_snapshot(s).unwrap().unwrap();
        assert_eq!(report.snapshot_id, s);
        // No unique pages.
        assert_eq!(report.pages_freed, 0);
        assert!(report.freed_leaf_values.is_empty());
        assert!(db.snapshots().is_empty());
    }

    #[test]
    fn drop_snapshot_reclaims_uniquely_owned_pages() {
        // 1000 keys, take snapshot, overwrite every key so every leaf
        // gets CoW'd. Drop snapshot; every old leaf becomes unique
        // and must be freed.
        let (_d, mut db) = mk_db();
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
        // Writes must have allocated new pages (CoW).
        assert!(hw_after_writes > free_before_snap);

        let report = db.drop_snapshot(s).unwrap().unwrap();
        assert!(
            report.pages_freed > 0,
            "expected freed pages, got {}",
            report.pages_freed,
        );
        assert_eq!(report.freed_leaf_values.len(), 1000);
        assert!(
            report.freed_leaf_values.iter().all(|val| *val == v(1)),
            "all freed values should be the pre-snapshot value",
        );

        // Current tree is still intact.
        for i in 0u64..1000 {
            assert_eq!(db.get(i).unwrap(), Some(v(2)));
        }
    }

    #[test]
    fn drop_snapshot_survives_reopen() {
        let dir = TempDir::new().unwrap();
        let s = {
            let mut db = Db::create(dir.path()).unwrap();
            for i in 0u64..100 {
                db.insert(i, v(1)).unwrap();
            }
            let s = db.take_snapshot().unwrap();
            for i in 0u64..100 {
                db.insert(i, v(2)).unwrap();
            }
            db.flush().unwrap();
            s
        };
        {
            let mut db = Db::open(dir.path()).unwrap();
            assert_eq!(db.snapshots().len(), 1);
            let report = db.drop_snapshot(s).unwrap().unwrap();
            assert!(!report.freed_leaf_values.is_empty());
            assert!(db.snapshots().is_empty());
            db.flush().unwrap();
        }
        let db = Db::open(dir.path()).unwrap();
        assert!(db.snapshots().is_empty());
    }

    #[test]
    fn drop_one_of_two_snapshots_preserves_the_other() {
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

        // Drop s1. s2 should still see its state.
        db.drop_snapshot(s1).unwrap().unwrap();
        {
            let mut view = db.snapshot_view(s2).unwrap();
            for i in 0u64..20 {
                assert_eq!(view.get(i).unwrap(), Some(v(2)));
            }
        }
        // Current tree also still sees its state.
        for i in 0u64..20 {
            assert_eq!(db.get(i).unwrap(), Some(v(3)));
        }

        // Drop s2 too. Current tree untouched.
        db.drop_snapshot(s2).unwrap().unwrap();
        for i in 0u64..20 {
            assert_eq!(db.get(i).unwrap(), Some(v(3)));
        }
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
