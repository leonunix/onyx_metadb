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
    let entry = db.snapshots().into_iter().find(|s| s.id == snap).unwrap();
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
fn dropping_source_snapshot_after_clone_keeps_page_refcounts_balanced() {
    let (dir, db) = mk_db();
    let src = db.create_volume().unwrap();
    for i in 0u64..256 {
        db.insert(src, i, v((i % 251) as u8)).unwrap();
    }
    let snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(snap).unwrap();

    let _ = db.drop_snapshot(snap).unwrap().unwrap();
    for i in 0u64..256 {
        assert_eq!(db.get(clone, i).unwrap(), Some(v((i % 251) as u8)));
    }

    drop(db);
    let db = Db::open(dir.path()).unwrap();
    for i in 0u64..256 {
        assert_eq!(db.get(clone, i).unwrap(), Some(v((i % 251) as u8)));
    }
    db.flush().unwrap();
    let report =
        crate::verify::verify_path(dir.path(), crate::verify::VerifyOptions { strict: true })
            .unwrap();
    assert!(report.is_clean(), "verify issues: {:?}", report.issues);
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
            db.insert(0, i, v(i as u8)).unwrap();
        }
        // NO flush() before drop — only WAL is durable.
    }
    let db = Db::open(dir.path()).unwrap();
    for i in 0u64..50 {
        assert_eq!(db.get(0, i).unwrap(), Some(v(i as u8)));
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
    tx.insert(0, 1, v(1));
    tx.insert(0, 2, v(2));
    tx.incref_pba(10, 3);
    tx.put_dedup(h(1), dv(9));
    let lsn = tx.commit().unwrap();
    assert!(lsn >= 1);
    assert_eq!(db.get(0, 1).unwrap(), Some(v(1)));
    assert_eq!(db.get(0, 2).unwrap(), Some(v(2)));
    assert_eq!(db.get_refcount(10).unwrap(), 3);
    assert_eq!(db.get_dedup(&h(1)).unwrap(), Some(dv(9)));
}

#[test]
fn multi_op_tx_survives_reopen_all_or_nothing() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        let mut tx = db.begin();
        tx.insert(0, 1, v(1));
        tx.insert(0, 2, v(2));
        tx.incref_pba(10, 3);
        tx.put_dedup(h(1), dv(9));
        tx.commit().unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(0, 1).unwrap(), Some(v(1)));
    assert_eq!(db.get(0, 2).unwrap(), Some(v(2)));
    assert_eq!(db.get_refcount(10).unwrap(), 3);
    assert_eq!(db.get_dedup(&h(1)).unwrap(), Some(dv(9)));
}

#[test]
fn last_applied_lsn_advances_per_commit() {
    let (_d, db) = mk_db();
    let before = db.last_applied_lsn();
    db.insert(0, 1, v(1)).unwrap();
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
        db.insert(0, i, v(i as u8)).unwrap();
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
            db.insert(0, i, v(i as u8)).unwrap();
        }
        db.last_applied_lsn()
    };
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.last_applied_lsn(), committed_lsn);
    // New commits after reopen start at committed_lsn + 1.
    db.insert(0, 100, v(0)).unwrap();
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
    assert!(db.multi_get(0, &[]).unwrap().is_empty());
    assert!(db.multi_get_refcount(&[]).unwrap().is_empty());
    assert!(db.multi_get_dedup(&[]).unwrap().is_empty());
    assert!(db.multi_scan_dedup_reverse_for_pba(&[]).unwrap().is_empty());
}

#[test]
fn multi_get_matches_single_gets_across_shards() {
    // 4 shards so we actually exercise the bucket + group logic.
    let (_d, db) = mk_db_with_shards(4);
    for i in 0u64..200 {
        db.insert(0, i, v((i as u8).wrapping_mul(3))).unwrap();
    }
    // Mix mapped + unmapped + duplicate keys, in non-sorted order.
    let keys = vec![199, 5000, 0, 199, 42, 10_000, 1, 42];
    let got = db.multi_get(0, &keys).unwrap();
    assert_eq!(got.len(), keys.len());
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(got[i], db.get(0, *key).unwrap(), "key {key} mismatch");
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
        assert_eq!(got[i], db.get_refcount(*pba).unwrap(), "pba {pba} mismatch",);
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

// -------- P1: bucketed batch apply ----------------------------------

fn hash_bytes(high: u64, low: u64) -> Hash32 {
    let mut h = [0u8; 32];
    h[..8].copy_from_slice(&high.to_be_bytes());
    h[24..].copy_from_slice(&low.to_be_bytes());
    h
}

fn dedup_val(n: u8) -> DedupValue {
    let mut d = [0u8; 28];
    d[0] = n;
    DedupValue(d)
}

/// Bucketed apply must produce the same per-op outcomes and the
/// same final state as the serial path, regardless of shard
/// routing. Batch is sized above BUCKET_THRESHOLD so the bucket
/// branch is exercised.
#[test]
fn bucketed_apply_matches_serial_for_mixed_batch() {
    let (_d, db) = mk_db_with_shards(4);
    let mut tx = db.begin();
    // 16 L2P puts across likely many shards.
    for i in 0..16u64 {
        tx.insert(BOOTSTRAP_VOLUME_ORD, i * 37, v((i & 0xff) as u8));
    }
    // 12 refcount ops; Incref first so Decref has something to
    // subtract from on the same pba.
    for pba in 0..12u64 {
        tx.incref_pba(pba, 3);
    }
    for pba in 0..6u64 {
        tx.decref_pba(pba, 1);
    }
    // 4 dedup ops.
    for i in 0..4u64 {
        tx.put_dedup(hash_bytes(0xAAAA, i), dedup_val(i as u8));
    }
    let (_lsn, outcomes) = tx.commit_with_outcomes().unwrap();
    // 16 L2P + 12 Incref + 6 Decref + 4 Dedup
    assert_eq!(outcomes.len(), 16 + 18 + 4);
    // Verify a representative subset of the resulting state.
    for i in 0..16u64 {
        assert_eq!(
            db.get(BOOTSTRAP_VOLUME_ORD, i * 37).unwrap(),
            Some(v((i & 0xff) as u8)),
        );
    }
    for pba in 0..6u64 {
        assert_eq!(db.get_refcount(pba).unwrap(), 2); // 3 - 1
    }
    for pba in 6..12u64 {
        assert_eq!(db.get_refcount(pba).unwrap(), 3);
    }
}

/// Same-shard puts in the same batch must apply in caller order,
/// not reordered. Two puts to the same (vol, lba) in order A, B
/// must leave the value = B.
#[test]
fn bucketed_apply_preserves_intra_bucket_order() {
    let (_d, db) = mk_db_with_shards(2);
    let mut tx = db.begin();
    // Pad with other L2P ops so the batch size crosses the
    // bucketing threshold.
    for i in 0..12u64 {
        tx.insert(BOOTSTRAP_VOLUME_ORD, i + 1_000, v(i as u8));
    }
    // Three puts to the same key in order.
    tx.insert(BOOTSTRAP_VOLUME_ORD, 9_999, v(0x10));
    tx.insert(BOOTSTRAP_VOLUME_ORD, 9_999, v(0x20));
    tx.insert(BOOTSTRAP_VOLUME_ORD, 9_999, v(0x30));
    tx.commit().unwrap();
    assert_eq!(
        db.get(BOOTSTRAP_VOLUME_ORD, 9_999).unwrap(),
        Some(v(0x30)),
        "intra-bucket order must preserve last-write-wins",
    );
}

/// Same-pba incref/decref in the same batch must apply in caller
/// order. A decref before its paired incref would underflow.
#[test]
fn bucketed_apply_preserves_intra_bucket_rc_order() {
    let (_d, db) = mk_db_with_shards(2);
    let mut tx = db.begin();
    // Pad so batch >= threshold.
    for pba in 0..10u64 {
        tx.incref_pba(pba + 100, 1);
    }
    // Incref then Decref same pba — ordering must be preserved.
    tx.incref_pba(77, 5);
    tx.decref_pba(77, 2);
    tx.commit().unwrap();
    assert_eq!(db.get_refcount(77).unwrap(), 3);
}

/// Batches smaller than the bucket threshold fall through to the
/// serial path. This test is a behavioural smoke check (the small
/// batch must still apply correctly).
#[test]
fn small_batch_falls_through_serial_path() {
    let (_d, db) = mk_db_with_shards(2);
    let mut tx = db.begin();
    // Below BUCKET_THRESHOLD (= 8).
    for i in 0..4u64 {
        tx.insert(BOOTSTRAP_VOLUME_ORD, i, v(i as u8));
    }
    tx.commit().unwrap();
    for i in 0..4u64 {
        assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, i).unwrap(), Some(v(i as u8)));
    }
}

/// Large pure-L2P batch — the main target of the optimisation.
/// Each shard is locked once per commit, not once per op.
#[test]
fn bucketed_apply_large_pure_l2p_batch() {
    let (_d, db) = mk_db_with_shards(8);
    let mut tx = db.begin();
    // 1024 keys with a prime stride so shard routing (xxh3-based)
    // is non-trivial while staying within the legal LBA range
    // (paged tree caps at MAX_INDEX_LEVEL=4).
    for i in 0..1024u64 {
        let lba = i.wrapping_mul(7919);
        tx.insert(BOOTSTRAP_VOLUME_ORD, lba, v((i & 0xff) as u8));
    }
    tx.commit().unwrap();
    for i in 0..1024u64 {
        let lba = i.wrapping_mul(7919);
        assert_eq!(
            db.get(BOOTSTRAP_VOLUME_ORD, lba).unwrap(),
            Some(v((i & 0xff) as u8)),
        );
    }
}

/// Helper used by `batch_contains_lifecycle_op` covers every
/// lifecycle variant — spot-check via direct unit tests since
/// these ops cannot be pushed through Transaction.
#[test]
fn lifecycle_predicate_detects_every_lifecycle_variant() {
    assert!(!batch_contains_lifecycle_op(&[WalOp::L2pPut {
        vol_ord: 0,
        lba: 0,
        value: v(0),
    }]));
    assert!(batch_contains_lifecycle_op(&[WalOp::DropSnapshot {
        id: 1,
        pages: Vec::new(),
        pba_decrefs: Vec::new(),
    }]));
    assert!(batch_contains_lifecycle_op(&[WalOp::CreateVolume {
        ord: 1,
        shard_count: 1,
    }]));
    assert!(batch_contains_lifecycle_op(&[WalOp::DropVolume {
        ord: 1,
        pages: Vec::new(),
    }]));
    assert!(batch_contains_lifecycle_op(&[WalOp::CloneVolume {
        src_ord: 0,
        new_ord: 1,
        src_snap_id: 0,
        src_shard_roots: Vec::new(),
    }]));
    // L2pRemap is forced through the serial path because it
    // straddles L2P and refcount shards — see apply_l2p_remap.
    assert!(batch_contains_lifecycle_op(&[WalOp::L2pRemap {
        vol_ord: 0,
        lba: 0,
        new_value: v(0),
        guard: None,
    }]));
}

// ---------------- L2pRemap apply (SPEC §3.1) ---------------------

/// Build an `L2pValue` whose head 8 bytes encode `pba` (matches the
/// `BlockmapValue` contract used by onyx's apply path). The
/// remaining 20 bytes carry `tag` in byte 8 so tests can
/// distinguish otherwise-identical values that share a pba.
fn remap_val(pba: Pba, tag: u8) -> L2pValue {
    let mut v = [0u8; 28];
    v[..8].copy_from_slice(&pba.to_be_bytes());
    v[8] = tag;
    L2pValue(v)
}

fn remap(db: &Db, lba: Lba, new_value: L2pValue, guard: Option<(Pba, u32)>) -> ApplyOutcome {
    let mut tx = db.begin();
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, lba, new_value, guard);
    let (_, outcomes) = tx.commit_with_outcomes().unwrap();
    assert_eq!(outcomes.len(), 1, "one op in, one outcome out");
    outcomes.into_iter().next().unwrap()
}

fn assert_remap_applied(outcome: ApplyOutcome) -> (Option<L2pValue>, Option<Pba>) {
    match outcome {
        ApplyOutcome::L2pRemap {
            applied: true,
            prev,
            freed_pba,
        } => (prev, freed_pba),
        other => panic!("expected applied L2pRemap, got {other:?}"),
    }
}

#[test]
fn l2p_remap_first_write_increfs_new_pba() {
    let (_d, db) = mk_db();
    let outcome = remap(&db, 10, remap_val(100, 1), None);
    assert_eq!(assert_remap_applied(outcome), (None, None));
    assert_eq!(db.get_refcount(100).unwrap(), 1);
    assert_eq!(
        db.get(BOOTSTRAP_VOLUME_ORD, 10).unwrap(),
        Some(remap_val(100, 1))
    );
}

#[test]
fn l2p_remap_same_pba_in_place_overwrite_net_zero() {
    // L2pPrev == new, same pba, leaf exclusive: no decref, no
    // incref (net 0). The "self_decrement" invariant from onyx's
    // atomic_batch_write_packed.
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 1), None);
    assert_eq!(db.get_refcount(100).unwrap(), 1);
    let outcome = remap(&db, 10, remap_val(100, 2), None);
    let (prev, freed) = assert_remap_applied(outcome);
    assert_eq!(prev, Some(remap_val(100, 1)));
    assert_eq!(freed, None);
    assert_eq!(
        db.get_refcount(100).unwrap(),
        1,
        "same-pba exclusive overwrite must not touch refcount"
    );
}

#[test]
fn l2p_remap_same_pba_leaf_shared_increfs_new() {
    // take_snapshot → leaf shared → remap to same pba should
    // incref (not no-op): the snapshot's leaf bytes still
    // reference the pba via the OLD mapping, and the COW leaf
    // will reference it again via the NEW mapping.
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 1), None);
    db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    let outcome = remap(&db, 10, remap_val(100, 2), None);
    let (prev, freed) = assert_remap_applied(outcome);
    assert_eq!(prev, Some(remap_val(100, 1)));
    assert_eq!(freed, None);
    assert_eq!(
        db.get_refcount(100).unwrap(),
        2,
        "same-pba on shared leaf: old leaf + new leaf both reference pba"
    );
}

#[test]
fn l2p_remap_different_pba_exclusive_decrefs_old_increfs_new() {
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 1), None);
    assert_eq!(db.get_refcount(100).unwrap(), 1);
    let outcome = remap(&db, 10, remap_val(200, 1), None);
    let (prev, freed) = assert_remap_applied(outcome);
    assert_eq!(prev, Some(remap_val(100, 1)));
    assert_eq!(freed, Some(100), "decref drove refcount(100) to 0");
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    assert_eq!(db.get_refcount(200).unwrap(), 1);
}

#[test]
fn l2p_remap_different_pba_leaf_shared_suppresses_decref() {
    // Snapshot holds old leaf → do NOT decref old pba.
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 1), None);
    db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    let outcome = remap(&db, 10, remap_val(200, 1), None);
    let (prev, freed) = assert_remap_applied(outcome);
    assert_eq!(prev, Some(remap_val(100, 1)));
    assert_eq!(
        freed, None,
        "leaf shared with snapshot: decref suppressed, no pba freed"
    );
    assert_eq!(db.get_refcount(100).unwrap(), 1, "snapshot still refs 100");
    assert_eq!(db.get_refcount(200).unwrap(), 1);
}

#[test]
fn l2p_remap_different_pba_decref_not_to_zero_reports_no_freed() {
    // Two independent LBAs share pba=100; remap one → refcount
    // drops 2→1, not to zero, so freed_pba = None even though we
    // did decref.
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 1), None);
    remap(&db, 11, remap_val(100, 1), None);
    assert_eq!(db.get_refcount(100).unwrap(), 2);
    let outcome = remap(&db, 10, remap_val(200, 1), None);
    let (_, freed) = assert_remap_applied(outcome);
    assert_eq!(freed, None);
    assert_eq!(db.get_refcount(100).unwrap(), 1);
}

#[test]
fn l2p_remap_guard_pass_applies_and_increfs() {
    // Target pba has live refcount > 0; guard passes; op applies.
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 1), None); // seed rc(100)=1
    // guard on 100 with min_rc=1 should pass.
    let outcome = remap(&db, 11, remap_val(100, 1), Some((100, 1)));
    let (prev, freed) = assert_remap_applied(outcome);
    assert_eq!(prev, None);
    assert_eq!(freed, None);
    assert_eq!(db.get_refcount(100).unwrap(), 2);
}

#[test]
fn l2p_remap_guard_fail_rejects_op_without_touching_state() {
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 1), None);
    // guard on 100 requires rc ≥ 5; current is 1; op is a no-op.
    let before_rc_100 = db.get_refcount(100).unwrap();
    let before_rc_200 = db.get_refcount(200).unwrap();
    let before_lba_11 = db.get(BOOTSTRAP_VOLUME_ORD, 11).unwrap();
    let mut tx = db.begin();
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 11, remap_val(200, 1), Some((100, 5)));
    let (_, outcomes) = tx.commit_with_outcomes().unwrap();
    match outcomes.into_iter().next().unwrap() {
        ApplyOutcome::L2pRemap {
            applied: false,
            prev,
            freed_pba,
        } => {
            assert_eq!(prev, None);
            assert_eq!(freed_pba, None);
        }
        other => panic!("expected rejected L2pRemap, got {other:?}"),
    }
    assert_eq!(db.get_refcount(100).unwrap(), before_rc_100);
    assert_eq!(db.get_refcount(200).unwrap(), before_rc_200);
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 11).unwrap(), before_lba_11);
}

#[test]
fn l2p_remap_guard_fail_when_pba_never_registered() {
    // guard on an unused pba (rc=0) with min_rc=1 fails.
    let (_d, db) = mk_db();
    let mut tx = db.begin();
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 10, remap_val(100, 1), Some((999, 1)));
    let (_, outcomes) = tx.commit_with_outcomes().unwrap();
    match outcomes.into_iter().next().unwrap() {
        ApplyOutcome::L2pRemap { applied: false, .. } => {}
        other => panic!("expected rejected L2pRemap, got {other:?}"),
    }
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 10).unwrap(), None);
}

#[test]
fn l2p_remap_packed_slot_multi_lba_refcount_aggregates_correctly() {
    // Simulate three LBAs pointing at one packed-slot pba=100:
    // each remap bumps refcount(100) by 1. Remapping each one
    // away individually should drive refcount 3→2→1→0 with the
    // final remap reporting freed_pba=100.
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 0), None);
    remap(&db, 11, remap_val(100, 1), None);
    remap(&db, 12, remap_val(100, 2), None);
    assert_eq!(db.get_refcount(100).unwrap(), 3);

    let (_, f0) = assert_remap_applied(remap(&db, 10, remap_val(200, 0), None));
    assert_eq!(f0, None);
    assert_eq!(db.get_refcount(100).unwrap(), 2);

    let (_, f1) = assert_remap_applied(remap(&db, 11, remap_val(201, 0), None));
    assert_eq!(f1, None);
    assert_eq!(db.get_refcount(100).unwrap(), 1);

    let (_, f2) = assert_remap_applied(remap(&db, 12, remap_val(202, 0), None));
    assert_eq!(f2, Some(100));
    assert_eq!(db.get_refcount(100).unwrap(), 0);
}

#[test]
fn l2p_remap_survives_restart_via_wal_replay() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        let mut tx = db.begin();
        tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 10, remap_val(100, 7), None);
        tx.commit_with_outcomes().unwrap();
        // Crash without flush: only WAL persists.
    }
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(
        db.get(BOOTSTRAP_VOLUME_ORD, 10).unwrap(),
        Some(remap_val(100, 7))
    );
    assert_eq!(db.get_refcount(100).unwrap(), 1);
}

#[test]
fn l2p_remap_guarded_survives_restart_with_same_decision() {
    // guard=Some with min_rc=2 while rc=1 at commit time → op
    // rejected; on replay, refcount is still 1 so replay also
    // rejects and the final state matches the live outcome.
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        remap(&db, 10, remap_val(100, 1), None);
        // rc(100)=1; guard needs rc(100)≥2 → reject.
        let mut tx = db.begin();
        tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 11, remap_val(100, 2), Some((100, 2)));
        tx.commit_with_outcomes().unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(
        db.get_refcount(100).unwrap(),
        1,
        "guard rejected on replay too"
    );
    assert_eq!(
        db.get(BOOTSTRAP_VOLUME_ORD, 10).unwrap(),
        Some(remap_val(100, 1))
    );
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 11).unwrap(), None);
}

#[test]
fn l2p_remap_freed_pba_round_trips_through_replay() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        remap(&db, 10, remap_val(100, 1), None);
        remap(&db, 10, remap_val(200, 1), None);
        assert_eq!(db.get_refcount(100).unwrap(), 0);
    }
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    assert_eq!(db.get_refcount(200).unwrap(), 1);
    assert_eq!(
        db.get(BOOTSTRAP_VOLUME_ORD, 10).unwrap(),
        Some(remap_val(200, 1))
    );
}

// ---------------- L2pRangeDelete apply (SPEC §3.2 / §4.7) -------

/// Helper that pre-seeds N consecutive lbas with the given pba.
/// Used to set up range_delete test fixtures without dragging in
/// the full remap decision table.
fn seed_remaps(db: &Db, start: Lba, count: usize, pba: Pba, tag: u8) {
    for i in 0..count {
        remap(db, start + i as u64, remap_val(pba, tag), None);
    }
}

#[test]
fn range_delete_empty_range_is_noop() {
    let (_d, db) = mk_db();
    remap(&db, 5, remap_val(100, 1), None);
    let rc_before = db.get_refcount(100).unwrap();
    let lsn_before = db.last_applied_lsn();
    // start >= end short-circuits.
    let lsn = db.range_delete(BOOTSTRAP_VOLUME_ORD, 10, 10).unwrap();
    assert_eq!(lsn, lsn_before);
    assert_eq!(db.get_refcount(100).unwrap(), rc_before);
    assert_eq!(
        db.get(BOOTSTRAP_VOLUME_ORD, 5).unwrap(),
        Some(remap_val(100, 1))
    );
}

#[test]
fn range_delete_with_no_live_mappings_is_noop() {
    let (_d, db) = mk_db();
    remap(&db, 100, remap_val(500, 1), None);
    let lsn_before = db.last_applied_lsn();
    // Live mapping at lba=100 is outside the deleted range.
    let lsn = db.range_delete(BOOTSTRAP_VOLUME_ORD, 0, 10).unwrap();
    assert_eq!(lsn, lsn_before, "scan found nothing → no WAL record");
    assert_eq!(db.get_refcount(500).unwrap(), 1);
    assert_eq!(
        db.get(BOOTSTRAP_VOLUME_ORD, 100).unwrap(),
        Some(remap_val(500, 1))
    );
}

#[test]
fn range_delete_removes_mappings_and_decrefs() {
    let (_d, db) = mk_db();
    // Three distinct pbas across three lbas.
    remap(&db, 10, remap_val(100, 0), None);
    remap(&db, 11, remap_val(200, 0), None);
    remap(&db, 12, remap_val(300, 0), None);
    db.range_delete(BOOTSTRAP_VOLUME_ORD, 10, 13).unwrap();
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 10).unwrap(), None);
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 11).unwrap(), None);
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 12).unwrap(), None);
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    assert_eq!(db.get_refcount(200).unwrap(), 0);
    assert_eq!(db.get_refcount(300).unwrap(), 0);
}

#[test]
fn range_delete_half_open_interval_excludes_end() {
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 0), None);
    remap(&db, 11, remap_val(200, 0), None);
    remap(&db, 12, remap_val(300, 0), None);
    // Range [10, 12) keeps lba=12.
    db.range_delete(BOOTSTRAP_VOLUME_ORD, 10, 12).unwrap();
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 10).unwrap(), None);
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 11).unwrap(), None);
    assert_eq!(
        db.get(BOOTSTRAP_VOLUME_ORD, 12).unwrap(),
        Some(remap_val(300, 0)),
        "end-exclusive: lba=12 survives",
    );
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    assert_eq!(db.get_refcount(200).unwrap(), 0);
    assert_eq!(db.get_refcount(300).unwrap(), 1);
}

#[test]
fn range_delete_dedup_multiple_lbas_same_pba_aggregates_decrefs() {
    // SPEC §4.7: captured may have multiple (lba, pba) pairs with
    // the same pba (dedup/packed-slot case). Apply must emit one
    // decref per entry so refcount correctly hits zero.
    let (_d, db) = mk_db();
    seed_remaps(&db, 10, 4, 777, 0);
    assert_eq!(db.get_refcount(777).unwrap(), 4);
    db.range_delete(BOOTSTRAP_VOLUME_ORD, 10, 14).unwrap();
    assert_eq!(db.get_refcount(777).unwrap(), 0);
    for lba in 10..14 {
        assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, lba).unwrap(), None);
    }
}

#[test]
fn range_delete_with_live_snapshot_suppresses_decref() {
    // Snapshot holds the pre-op leaf → all decrefs for shared
    // leaves are suppressed. Refcount survives until S4's
    // drop_snapshot compensates.
    let (_d, db) = mk_db();
    seed_remaps(&db, 10, 3, 500, 0);
    assert_eq!(db.get_refcount(500).unwrap(), 3);
    db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    db.range_delete(BOOTSTRAP_VOLUME_ORD, 10, 13).unwrap();
    for lba in 10..13 {
        assert_eq!(
            db.get(BOOTSTRAP_VOLUME_ORD, lba).unwrap(),
            None,
            "current tree no longer has lba={lba}"
        );
    }
    assert_eq!(
        db.get_refcount(500).unwrap(),
        3,
        "leaf shared with snapshot: all decrefs suppressed",
    );
}

#[test]
fn range_delete_mixed_shared_and_exclusive_leaves() {
    // Seed a wide range, then snapshot, then write more LBAs that
    // are exclusive to the current tree. The second range_delete
    // should suppress decrefs for shared-leaf entries but not for
    // exclusive ones.
    let (_d, db) = mk_db();
    // LBA 10..13 exist pre-snapshot; pba=500.
    seed_remaps(&db, 10, 3, 500, 0);
    db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    // A large gap so 10_000..10_003 land in a different leaf
    // (LEAF_ENTRY_COUNT=128 → leaf_idx differs). Those lbas are
    // fresh post-snapshot → leaf exclusive to current tree.
    seed_remaps(&db, 10_000, 3, 600, 0);
    assert_eq!(db.get_refcount(500).unwrap(), 3);
    assert_eq!(db.get_refcount(600).unwrap(), 3);

    // Delete both ranges together in one call.
    db.range_delete(BOOTSTRAP_VOLUME_ORD, 10, 10_010).unwrap();
    // Shared leaf: suppressed.
    assert_eq!(db.get_refcount(500).unwrap(), 3);
    // Exclusive leaf: decref went through.
    assert_eq!(db.get_refcount(600).unwrap(), 0);
}

#[test]
fn range_delete_survives_restart_via_wal_replay() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        seed_remaps(&db, 10, 4, 100, 0);
        db.range_delete(BOOTSTRAP_VOLUME_ORD, 10, 14).unwrap();
        // Crash without flush.
    }
    let db = Db::open(dir.path()).unwrap();
    for lba in 10..14 {
        assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, lba).unwrap(), None);
    }
    assert_eq!(db.get_refcount(100).unwrap(), 0);
}

#[test]
fn range_delete_auto_splits_above_cap() {
    // Force captured.len() to exceed MAX_RANGE_DELETE_CAPTURED so
    // the auto-split path runs. Seed 2 * MAX + 37 entries; expect
    // three WAL records + three final applies.
    let cap = crate::wal::op::MAX_RANGE_DELETE_CAPTURED;
    let total = 2 * cap + 37;
    let (_d, db) = mk_db();
    for i in 0..total {
        remap(&db, i as u64, remap_val(100 + i as u64, 0), None);
    }
    let pre_lsn = db.last_applied_lsn();
    let lsn = db
        .range_delete(BOOTSTRAP_VOLUME_ORD, 0, total as u64)
        .unwrap();
    // Three chunks → three WAL records → LSN bumped by 3.
    assert_eq!(
        lsn,
        pre_lsn + 3,
        "auto-split emitted exactly three WAL records",
    );
    for i in 0..total {
        assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, i as u64).unwrap(), None);
    }
    // Spot-check a few refcounts.
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    assert_eq!(db.get_refcount(100 + cap as u64).unwrap(), 0);
    assert_eq!(db.get_refcount(100 + (total - 1) as u64).unwrap(), 0);
}

#[test]
fn range_delete_crosses_shard_boundaries() {
    // With the default shard count (> 1), a contiguous LBA range
    // hits multiple shards. Make sure the apply path visits each
    // shard's tree and every mapping in the range is removed.
    let (_d, db) = mk_db_with_shards(8);
    for i in 0..200u64 {
        remap(&db, i, remap_val(1_000 + i, 0), None);
    }
    db.range_delete(BOOTSTRAP_VOLUME_ORD, 0, 200).unwrap();
    for i in 0..200u64 {
        assert_eq!(
            db.get(BOOTSTRAP_VOLUME_ORD, i).unwrap(),
            None,
            "lba={i} should be unmapped after range_delete",
        );
        assert_eq!(db.get_refcount(1_000 + i).unwrap(), 0);
    }
}

#[test]
fn range_delete_dedup_with_snapshot_suppresses_all() {
    // Combined SPEC §4.7 (dedup aggregation) + §4.4 (leaf shared).
    // Four lbas on the same pba, then snapshot, then range_delete:
    // all four should have their decrefs suppressed.
    let (_d, db) = mk_db();
    seed_remaps(&db, 10, 4, 777, 0);
    assert_eq!(db.get_refcount(777).unwrap(), 4);
    db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    db.range_delete(BOOTSTRAP_VOLUME_ORD, 10, 14).unwrap();
    assert_eq!(
        db.get_refcount(777).unwrap(),
        4,
        "all four lbas under a shared leaf: every decref suppressed",
    );
}

#[test]
fn l2p_remap_leaf_shared_plus_drop_snapshot_ends_at_correct_refcount() {
    // SPEC §4.4 symmetry: take → N writes → drop must leave
    // refcount identical to "same N writes without snapshot".
    // S2's leaf-rc-suppress deliberately under-decrefs while the
    // snapshot is live; S4's drop_snapshot pba_decrefs completes
    // the balance.
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 1), None); // rc(100)=1
    let snap = db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    remap(&db, 10, remap_val(200, 1), None); // snapshot suppresses decref(100); rc(100)=1, rc(200)=1
    assert_eq!(db.get_refcount(100).unwrap(), 1);
    assert_eq!(db.get_refcount(200).unwrap(), 1);
    let report = db.drop_snapshot(snap).unwrap().unwrap();
    // drop_snapshot compensates via pba_decrefs: snap had 100 at
    // lba 10, current has 200 → decref(100) → refcount hits 0.
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    assert_eq!(db.get_refcount(200).unwrap(), 1);
    assert_eq!(report.freed_pbas, vec![100]);
}

// ---------------- S4 drop_snapshot extended tests (SPEC §4.4 / §4.5) ---

#[test]
fn drop_snapshot_symmetric_with_no_snapshot_refcounts() {
    // SPEC §4.4: "take → N writes → drop" ≡ "N writes without
    // snapshot" on refcount. Build two identical DBs and compare
    // refcount + L2P state after the snapshot dance.
    let (_d1, db_snap) = mk_db();
    let (_d2, db_plain) = mk_db();
    // Both: initial writes.
    for lba in 0..8u64 {
        remap(&db_snap, lba, remap_val(100 + lba, 1), None);
        remap(&db_plain, lba, remap_val(100 + lba, 1), None);
    }
    let s = db_snap.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    // Current-tree writes: change half of them to new pbas.
    for lba in 0..8u64 {
        if lba % 2 == 0 {
            remap(&db_snap, lba, remap_val(200 + lba, 1), None);
            remap(&db_plain, lba, remap_val(200 + lba, 1), None);
        }
    }
    // Before drop, refcount diverges: snap side has rc(100+even)=1
    // from leaf-rc-suppress. After drop it should match plain.
    db_snap.drop_snapshot(s).unwrap();
    for lba in 0..8u64 {
        assert_eq!(
            db_snap.get_refcount(100 + lba).unwrap(),
            db_plain.get_refcount(100 + lba).unwrap(),
            "rc divergence for pba {} after drop_snapshot",
            100 + lba,
        );
        assert_eq!(
            db_snap.get_refcount(200 + lba).unwrap(),
            db_plain.get_refcount(200 + lba).unwrap(),
            "rc divergence for pba {} after drop_snapshot",
            200 + lba,
        );
    }
}

#[test]
fn drop_snapshot_freed_pbas_covers_dedup_multi_lba_share() {
    // Onyx packed-slot pattern: 4 lbas share pba=777 pre-snapshot.
    // Post-snapshot, all 4 lbas are remapped away → rc(777) drops
    // by 4, hitting zero. Report should list pba=777 exactly once
    // (newly_zeroed strict semantics, SPEC §4.1).
    let (_d, db) = mk_db();
    for lba in 10u64..14 {
        remap(&db, lba, remap_val(777, (lba - 10) as u8), None);
    }
    assert_eq!(db.get_refcount(777).unwrap(), 4);
    let snap = db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    for lba in 10u64..14 {
        remap(&db, lba, remap_val(888 + lba, 0), None);
    }
    // Leaf shared: decrefs suppressed, rc(777) still 4.
    assert_eq!(db.get_refcount(777).unwrap(), 4);
    let report = db.drop_snapshot(snap).unwrap().unwrap();
    assert_eq!(db.get_refcount(777).unwrap(), 0);
    let freed: std::collections::HashSet<Pba> = report.freed_pbas.iter().copied().collect();
    assert!(
        freed.contains(&777),
        "pba 777 should be in freed_pbas (hit zero)",
    );
    // The four new pbas still have rc=1 and are NOT in freed.
    for lba in 10u64..14 {
        let pba = 888 + lba;
        assert_eq!(db.get_refcount(pba).unwrap(), 1);
        assert!(!freed.contains(&pba));
    }
}

#[test]
fn drop_snapshot_and_pages_commit_atomically_via_wal() {
    // SPEC §3.3: pages release and pba_decrefs share one WAL record.
    // Crash before apply → replay must reconstruct both effects.
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        remap(&db, 10, remap_val(100, 1), None);
        let snap = db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
        remap(&db, 10, remap_val(200, 1), None);
        assert_eq!(db.get_refcount(100).unwrap(), 1);
        db.drop_snapshot(snap).unwrap();
        assert_eq!(db.get_refcount(100).unwrap(), 0);
        // Close without a flush — the drop is only in the WAL.
    }
    let db = Db::open(dir.path()).unwrap();
    // Replay must re-run the pba_decref.
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    assert_eq!(db.get_refcount(200).unwrap(), 1);
    assert!(
        db.snapshots().is_empty(),
        "snapshot list must stay empty after replay",
    );
}

#[test]
fn drop_snapshot_skips_decref_when_pba_refcount_already_zero() {
    // Non-refcount path (raw `insert`, without incref): plan must
    // filter these out so apply doesn't underflow. Already covered
    // by `drop_snapshot_reclaims_uniquely_owned_pages`; this test
    // asserts the filter directly.
    let (_d, db) = mk_db();
    // Raw inserts → no refcount touched.
    db.insert(BOOTSTRAP_VOLUME_ORD, 10, remap_val(500, 1))
        .unwrap();
    let snap = db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    db.insert(BOOTSTRAP_VOLUME_ORD, 10, remap_val(600, 1))
        .unwrap();
    assert_eq!(db.get_refcount(500).unwrap(), 0);
    let report = db.drop_snapshot(snap).unwrap().unwrap();
    assert!(
        report.freed_pbas.is_empty(),
        "filter dropped decrefs for rc=0 snap pba",
    );
}

#[test]
fn cleanup_dedup_for_dead_pbas_empty_input_is_noop() {
    let (_d, db) = mk_db();
    let lsn_before = db.last_applied_lsn();
    let lsn = db.cleanup_dedup_for_dead_pbas(&[]).unwrap();
    assert_eq!(lsn, lsn_before);
}

#[test]
fn cleanup_dedup_for_dead_pbas_removes_reverse_and_forward() {
    let (_d, db) = mk_db();
    // Register hash→pba in both tables, as onyx does on dedup miss
    // via a single Transaction.
    let h = hash_bytes(1, 2);
    let val = dedup_val_with_pba(500, 0xAB);
    {
        let mut tx = db.begin();
        tx.put_dedup(h, val).register_dedup_reverse(500, h);
        tx.commit().unwrap();
    }
    assert_eq!(db.get_dedup(&h).unwrap(), Some(val));
    assert_eq!(db.scan_dedup_reverse_for_pba(500).unwrap(), vec![h]);

    db.cleanup_dedup_for_dead_pbas(&[500]).unwrap();
    assert_eq!(db.get_dedup(&h).unwrap(), None);
    assert!(db.scan_dedup_reverse_for_pba(500).unwrap().is_empty());
}

#[test]
fn cleanup_dedup_for_dead_pbas_race_preserves_new_registration() {
    // SPEC §4.5 race protection: hash was re-registered to a
    // different pba between the dedup miss and cleanup. Cleanup
    // must not delete the forward entry that now points at pba=600.
    let (_d, db) = mk_db();
    let h = hash_bytes(7, 8);
    // Initial registration: hash→pba=500.
    {
        let mut tx = db.begin();
        tx.put_dedup(h, dedup_val_with_pba(500, 0))
            .register_dedup_reverse(500, h);
        tx.commit().unwrap();
    }
    // Concurrent writer re-registered to pba=600 (forward only
    // — reverse index still carries the stale (500, h) entry).
    {
        let mut tx = db.begin();
        tx.put_dedup(h, dedup_val_with_pba(600, 0))
            .register_dedup_reverse(600, h);
        tx.commit().unwrap();
    }
    assert_eq!(
        db.get_dedup(&h).unwrap().unwrap().head_pba(),
        600,
        "forward index re-registered to pba=600",
    );

    // Cleanup for pba=500. Must leave the forward entry alone
    // (it points at 600) and remove the reverse entry for 500.
    db.cleanup_dedup_for_dead_pbas(&[500]).unwrap();
    assert_eq!(
        db.get_dedup(&h).unwrap().unwrap().head_pba(),
        600,
        "forward entry for 600 must survive cleanup of 500",
    );
    assert!(
        db.scan_dedup_reverse_for_pba(500).unwrap().is_empty(),
        "reverse entry (500, h) removed",
    );
    // Reverse entry for the new pba=600 is still there.
    assert_eq!(db.scan_dedup_reverse_for_pba(600).unwrap(), vec![h]);
}

#[test]
fn cleanup_dedup_for_dead_pbas_is_idempotent() {
    // Replay safety: running cleanup twice is a no-op the second
    // time. Matches the tombstone-based design (SPEC §2.2).
    let (_d, db) = mk_db();
    let h = hash_bytes(9, 9);
    {
        let mut tx = db.begin();
        tx.put_dedup(h, dedup_val_with_pba(555, 0))
            .register_dedup_reverse(555, h);
        tx.commit().unwrap();
    }
    db.cleanup_dedup_for_dead_pbas(&[555]).unwrap();
    // Second run has nothing to do → WAL unchanged.
    let lsn_before = db.last_applied_lsn();
    db.cleanup_dedup_for_dead_pbas(&[555]).unwrap();
    assert_eq!(
        db.last_applied_lsn(),
        lsn_before,
        "replay is idempotent (no WAL record emitted)",
    );
    assert_eq!(db.get_dedup(&h).unwrap(), None);
}

#[test]
fn cleanup_dedup_for_dead_pbas_batches_multiple_pbas() {
    // One WAL record per invocation — the batching contract from
    // SPEC §2.2. Even when many (pba, hash) pairs are involved.
    let (_d, db) = mk_db();
    let mut pbas = Vec::new();
    for i in 0..5u64 {
        let pba = 1000 + i;
        let h = hash_bytes(0, i);
        let mut tx = db.begin();
        tx.put_dedup(h, dedup_val_with_pba(pba, i as u8))
            .register_dedup_reverse(pba, h);
        tx.commit().unwrap();
        pbas.push(pba);
    }
    let lsn_before = db.last_applied_lsn();
    db.cleanup_dedup_for_dead_pbas(&pbas).unwrap();
    assert_eq!(
        db.last_applied_lsn(),
        lsn_before + 1,
        "all tombstones in one atomic WAL record",
    );
    for (i, pba) in pbas.iter().enumerate() {
        let h = hash_bytes(0, i as u64);
        assert_eq!(db.get_dedup(&h).unwrap(), None);
        assert!(db.scan_dedup_reverse_for_pba(*pba).unwrap().is_empty());
    }
}

/// Build a DedupValue whose head-8B PBA is `pba` (Onyx contract —
/// see `DedupValue::head_pba`).
fn dedup_val_with_pba(pba: Pba, tag: u8) -> DedupValue {
    let mut v = [0u8; 28];
    v[..8].copy_from_slice(&pba.to_be_bytes());
    v[8] = tag;
    DedupValue(v)
}
