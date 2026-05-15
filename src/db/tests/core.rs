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
    // v2 caps `MAX_UNITS_PER_LEAF` at 100, so a leaf (128 LBAs) cannot
    // hold 128 distinct UnitMetas — share value bytes across 32-LBA
    // groups to keep ≤ 4 distinct units per leaf. The test's
    // invariant is "values round-trip through flush + reopen", and
    // that still holds with shared values.
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        for i in 0u64..500 {
            db.insert(0, i, v((i / 32) as u8)).unwrap();
        }
        db.flush().unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    for i in 0u64..500 {
        assert_eq!(db.get(0, i).unwrap(), Some(v((i / 32) as u8)));
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

    let s = db.take_snapshot(0).unwrap();

    for i in 0u64..1000 {
        db.insert(0, i, v(2)).unwrap();
    }
    db.flush().unwrap();

    // Snapshot still observes the pre-snapshot value while the live
    // view advanced to `v(2)` — proves COW forked each overwritten
    // LBA's path. Earlier revisions compared `high_water` before vs
    // after the second batch of writes, but the per-shard
    // `LOCAL_ALLOC_RUN_PAGES = 256` pre-allocation pool means freshly
    // opened shards hold ~16 × 256 unused pages already counted in
    // `high_water`; COW draws from that pool without bumping the
    // counter.
    let view = db.snapshot_view(s).expect("snapshot view");
    for i in 0u64..1000 {
        assert_eq!(view.get(i).unwrap(), Some(v(1)));
    }
    drop(view);

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

// -------- B2: L2P buffer + periodic compaction --------

fn mk_db_with_buffer() -> (TempDir, Db) {
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    // Use a very small soft trigger so the compactor runs frequently
    // in tests, exercising swap → apply → publish quickly.
    cfg.l2p_buffer_soft_entries = 4;
    cfg.l2p_buffer_max_interval_ms = 50;
    let db = Db::create_with_config(cfg).unwrap();
    (dir, db)
}

#[test]
fn b2_buffer_insert_visible_to_get() {
    let (_d, db) = mk_db_with_buffer();
    db.insert(0, 10, v(7)).unwrap();
    assert_eq!(db.get(0, 10).unwrap(), Some(v(7)));
}

#[test]
fn b2_buffer_overwrite_visible_to_get() {
    let (_d, db) = mk_db_with_buffer();
    db.insert(0, 10, v(7)).unwrap();
    db.insert(0, 10, v(8)).unwrap();
    assert_eq!(db.get(0, 10).unwrap(), Some(v(8)));
}

#[test]
fn b2_buffer_delete_visible_to_get() {
    let (_d, db) = mk_db_with_buffer();
    db.insert(0, 10, v(7)).unwrap();
    db.delete(0, 10).unwrap();
    assert_eq!(db.get(0, 10).unwrap(), None);
}

#[test]
fn b2_buffer_multi_get_partial_buffer_partial_tree() {
    let (_d, db) = mk_db_with_buffer();
    // Insert 200 entries (well above soft threshold of 4) so the
    // compactor will fold some into the tree while the most recent
    // ones may still live in the buffer.
    for i in 0u64..200 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    let mut keys: Vec<u64> = (0..200).collect();
    keys.reverse();
    let values = db.multi_get(0, &keys).unwrap();
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(values[i], Some(v(*key as u8)));
    }
}

#[test]
fn b2_buffer_flush_then_get_consistent() {
    let (_d, db) = mk_db_with_buffer();
    for i in 0u64..50 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    db.flush().unwrap();
    for i in 0u64..50 {
        assert_eq!(db.get(0, i).unwrap(), Some(v(i as u8)));
    }
}

#[test]
fn b2_buffer_flush_reopen_round_trip() {
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    cfg.l2p_buffer_soft_entries = 4;
    cfg.l2p_buffer_max_interval_ms = 50;
    {
        let db = Db::create_with_config(cfg.clone()).unwrap();
        for i in 0u64..100 {
            db.insert(0, i, v(i as u8)).unwrap();
        }
        db.flush().unwrap();
    }
    let db = Db::open_with_config(cfg).unwrap();
    for i in 0u64..100 {
        assert_eq!(db.get(0, i).unwrap(), Some(v(i as u8)));
    }
}

#[test]
fn b2_buffer_no_flush_reopen_replays_from_wal() {
    // Skip the explicit flush so the buffer's contents are
    // recreated only from WAL replay on reopen.
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    cfg.l2p_buffer_soft_entries = 4;
    cfg.l2p_buffer_max_interval_ms = 50;
    {
        let db = Db::create_with_config(cfg.clone()).unwrap();
        for i in 0u64..30 {
            db.insert(0, i, v(i as u8)).unwrap();
        }
        // No flush — close while buffer may still hold entries.
    }
    let db = Db::open_with_config(cfg).unwrap();
    for i in 0u64..30 {
        assert_eq!(db.get(0, i).unwrap(), Some(v(i as u8)));
    }
}

// -------- B2 Phase 4: snapshot + range_delete with buffer --------

#[test]
fn b2_buffer_take_snapshot_force_compacts_target() {
    // Snapshot must see post-commit / pre-compaction writes that
    // still live in the L2P buffer. `take_snapshot` calls
    // `force_compact_l2p_buffers` so the sampled roots reflect every
    // committed LSN.
    let (_d, db) = mk_db_with_buffer();
    for i in 0u64..200 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(0).unwrap();
    // Post-snapshot writes land in buffer; snapshot view must not see them.
    for i in 0u64..200 {
        db.insert(0, i, v(99)).unwrap();
    }
    db.insert(0, 999, v(7)).unwrap();
    let view = db.snapshot_view(snap).unwrap();
    for i in 0u64..200 {
        assert_eq!(view.get(i).unwrap(), Some(v(i as u8)));
    }
    assert_eq!(view.get(999).unwrap(), None);
}

#[test]
fn b2_buffer_snapshot_survives_reopen() {
    // The post-snapshot buffer must survive WAL replay across reopen,
    // and the snapshot view itself must still resolve correctly
    // against the rebuilt tree.
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    cfg.l2p_buffer_soft_entries = 4;
    cfg.l2p_buffer_max_interval_ms = 50;
    let snap_id = {
        let db = Db::create_with_config(cfg.clone()).unwrap();
        for i in 0u64..100 {
            db.insert(0, i, v(1)).unwrap();
        }
        let id = db.take_snapshot(0).unwrap();
        for i in 0u64..100 {
            db.insert(0, i, v(2)).unwrap();
        }
        db.flush().unwrap();
        id
    };
    let db = Db::open_with_config(cfg).unwrap();
    let view = db.snapshot_view(snap_id).unwrap();
    for i in 0u64..100 {
        assert_eq!(view.get(i).unwrap(), Some(v(1)));
    }
    for i in 0u64..100 {
        assert_eq!(db.get(0, i).unwrap(), Some(v(2)));
    }
}

#[test]
fn b2_buffer_drop_snapshot_with_buffer_overwrites() {
    // Drop must compute decrefs correctly even when post-snapshot
    // overwrites still live in the buffer (i.e., the snapshot's old
    // PBA refcount drops to zero because no live mapping references
    // it anymore — `diff_subtrees` must see the post-buffer current
    // root, which `force_compact_l2p_buffers` guarantees).
    let (_d, db) = mk_db_with_buffer();
    for i in 0u64..1000 {
        db.insert(0, i, v(1)).unwrap();
    }
    db.flush().unwrap();
    let s = db.take_snapshot(0).unwrap();
    for i in 0u64..1000 {
        db.insert(0, i, v(2)).unwrap();
    }
    // Snapshot still observes pre-snapshot values; live observes post.
    let view = db.snapshot_view(s).unwrap();
    for i in 0u64..1000 {
        assert_eq!(view.get(i).unwrap(), Some(v(1)));
    }
    drop(view);
    let report = db.drop_snapshot(s).unwrap().unwrap();
    assert!(report.pages_freed > 0);
    assert_eq!(report.freed_leaf_values.len(), 1000);
    assert!(report.freed_leaf_values.iter().all(|val| *val == v(1)));
    for i in 0u64..1000 {
        assert_eq!(db.get(0, i).unwrap(), Some(v(2)));
    }
}

#[test]
fn b2_buffer_range_delete_drains_buffer() {
    // `range_delete` calls `force_compact_l2p_buffers` so the scan
    // sees buffer-only entries that haven't compacted yet. Use the
    // L2pRemap path so RC bookkeeping matches what `range_delete`
    // expects when it decref's captured values.
    fn remap_val(pba: Pba, tag: u8) -> L2pValue {
        let mut v = [0u8; 36];
        v[..8].copy_from_slice(&pba.to_be_bytes());
        v[8] = tag;
        L2pValue(v)
    }
    let (_d, db) = mk_db_with_buffer();
    for i in 0u64..50 {
        let mut tx = db.begin();
        tx.l2p_remap(0, i, remap_val(100 + i, i as u8), None);
        tx.commit_with_outcomes().unwrap();
    }
    // No flush — many writes likely still live in the buffer.
    db.range_delete(0, 10, 40).unwrap();
    for i in 0u64..10 {
        assert_eq!(db.get(0, i).unwrap(), Some(remap_val(100 + i, i as u8)));
    }
    for i in 10u64..40 {
        assert_eq!(db.get(0, i).unwrap(), None);
    }
    for i in 40u64..50 {
        assert_eq!(db.get(0, i).unwrap(), Some(remap_val(100 + i, i as u8)));
    }
    // RC reflects deletions: surviving pbas keep rc=1, deleted ones go to 0.
    for i in 0u64..10 {
        assert_eq!(db.get_refcount(100 + i).unwrap(), 1);
    }
    for i in 10u64..40 {
        assert_eq!(db.get_refcount(100 + i).unwrap(), 0);
    }
    for i in 40u64..50 {
        assert_eq!(db.get_refcount(100 + i).unwrap(), 1);
    }
}

// -------- phase 7 commit 8: volume lifecycle --------
