use super::*;

#[test]
fn fresh_db_is_empty() {
    let (_d, db) = mk_db();
    assert_eq!(db.get(0, 42).unwrap(), None);
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
    db.insert(0, 10, v(7)).unwrap();
    assert_eq!(db.get(0, 10).unwrap(), Some(v(7)));
}

#[test]
fn flush_persists_tree_state_via_manifest() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        for i in 0u64..500 {
            db.insert(0, i, v(i as u8)).unwrap();
        }
        db.flush().unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    for i in 0u64..500 {
        assert_eq!(db.get(0, i).unwrap(), Some(v(i as u8)));
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
        db.insert(0, i, v(1)).unwrap();
    }
    let snap = db.take_snapshot(0).unwrap();

    for i in 0u64..100 {
        db.insert(0, i, v(2)).unwrap();
    }
    db.insert(0, 999, v(9)).unwrap();
    db.delete(0, 50).unwrap();

    assert_eq!(db.get(0, 0).unwrap(), Some(v(2)));
    assert_eq!(db.get(0, 50).unwrap(), None);
    assert_eq!(db.get(0, 999).unwrap(), Some(v(9)));

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
        db.insert(0, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(0).unwrap();
    for i in 0u64..50 {
        db.insert(0, i, v(99)).unwrap();
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
            db.insert(0, i, v(1)).unwrap();
        }
        let id = db.take_snapshot(0).unwrap();
        for i in 0u64..200 {
            db.insert(0, i, v(2)).unwrap();
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
        assert_eq!(db.get(0, i).unwrap(), Some(v(2)));
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
        db.insert(0, i, v(1)).unwrap();
    }
    let a = db.take_snapshot(0).unwrap();

    db.insert(0, 5, v(99)).unwrap();
    db.delete(0, 3).unwrap();
    db.insert(0, 42, v(7)).unwrap();

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
        db.insert(0, i, v(1)).unwrap();
    }
    let a = db.take_snapshot(0).unwrap();
    db.insert(0, 100, v(5)).unwrap();
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
        db.insert(0, i, v(1)).unwrap();
    }
    db.flush().unwrap();
    let free_before_snap = db.high_water();
    let s = db.take_snapshot(0).unwrap();

    for i in 0u64..1000 {
        db.insert(0, i, v(2)).unwrap();
    }
    db.flush().unwrap();
    let hw_after_writes = db.high_water();
    assert!(hw_after_writes > free_before_snap);

    let report = db.drop_snapshot(s).unwrap().unwrap();
    assert!(report.pages_freed > 0);
    assert_eq!(report.freed_leaf_values.len(), 1000);
    assert!(report.freed_leaf_values.iter().all(|val| *val == v(1)));
    for i in 0u64..1000 {
        assert_eq!(db.get(0, i).unwrap(), Some(v(2)));
    }
}

#[test]
fn multiple_snapshots_isolated() {
    let (_d, db) = mk_db();
    for i in 0u64..20 {
        db.insert(0, i, v(1)).unwrap();
    }
    let s1 = db.take_snapshot(0).unwrap();
    for i in 0u64..20 {
        db.insert(0, i, v(2)).unwrap();
    }
    let s2 = db.take_snapshot(0).unwrap();
    for i in 0u64..20 {
        db.insert(0, i, v(3)).unwrap();
    }

    {
        let v1 = db.snapshot_view(s1).unwrap();
        assert_eq!(v1.get(5).unwrap(), Some(v(1)));
    }
    {
        let v2 = db.snapshot_view(s2).unwrap();
        assert_eq!(v2.get(5).unwrap(), Some(v(2)));
    }
    assert_eq!(db.get(0, 5).unwrap(), Some(v(3)));
}

// -------- phase 7 commit 8: volume lifecycle --------
