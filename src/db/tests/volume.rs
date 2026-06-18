use super::*;

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

// Phase D.5: `volume_lifecycle_survives_reopen_without_flush` exercised
// data-plane WAL replay, which is gone. Buffer-mode reopens of an
// unflushed `create_volume` are covered by
// `tests/db_buffer_journal_replay.rs::replay_recovers_create_volume_without_flush`.

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
    // v2 MAX_UNITS_PER_LEAF = 100; share value bytes across 32-LBA
    // groups so the leaf (128 LBAs) only references ≤ 4 distinct
    // UnitMetas. The invariant under test is refcount-balance, not
    // unit-distinctness.
    let (dir, db) = mk_db();
    let src = db.create_volume().unwrap();
    let val = |i: u64| -> L2pValue { v(((i / 32) % 251) as u8) };
    for i in 0u64..256 {
        db.insert(src, i, val(i)).unwrap();
    }
    let snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(snap).unwrap();

    let _ = db.drop_snapshot(snap).unwrap().unwrap();
    for i in 0u64..256 {
        assert_eq!(db.get(clone, i).unwrap(), Some(val(i)));
    }

    drop(db);
    let db = Db::open(dir.path()).unwrap();
    let val = |i: u64| -> L2pValue { v(((i / 32) % 251) as u8) };
    for i in 0u64..256 {
        assert_eq!(db.get(clone, i).unwrap(), Some(val(i)));
    }
    db.flush().unwrap();
    let report = crate::verify::verify_path(
        dir.path(),
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
        },
    )
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

// Phase D.5: `clone_survives_reopen_wal_replay` exercised data-plane
// WAL replay across a clone barrier; covered for Buffer mode by
// `tests/db_buffer_journal_replay.rs::replay_recovers_clone_volume_without_flush`.

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

// -------- phase 4 step 1: lineage tracking on VolumeEntry --------

// Fresh top-level volumes (bootstrap + `create_volume`) must record
// `parent_vol_ord = None` / `branched_at_lsn = 0` / `promotion_cursor = None`.
// That keeps the v14 schema honest: only `clone_volume` is allowed to
// arm the lineage fields, so downstream Lineage GC / cross-volume
// snap_pin doesn't pick up stray parent edges.
#[test]
fn phase4_top_level_volume_has_no_lineage() {
    let (_d, db) = mk_db();
    let ord = db.create_volume().unwrap();
    let m = db.manifest();
    for e in &m.volumes {
        assert_eq!(e.parent_vol_ord, None, "ord={}", e.ord);
        assert_eq!(e.branched_at_lsn, 0, "ord={}", e.ord);
        assert_eq!(e.promotion_cursor, None, "ord={}", e.ord);
    }
    let _ = ord;
}

// A fast clone (`clone_volume`) must record `parent_vol_ord = Some(src)`
// and `branched_at_lsn = snapshot.created_lsn`. `promotion_cursor` stays
// `None` in Step 1 — Step 5's background walker is what arms it.
// Round-trips through flush + reopen.
#[test]
fn phase4_clone_records_parent_and_branched_lsn() {
    let dir = TempDir::new().unwrap();
    let (src, clone, expected_branched_at) = {
        let db = Db::create(dir.path()).unwrap();
        let src = db.create_volume().unwrap();
        // Drive a write so the snapshot's `created_lsn` is non-zero
        // and distinguishable from the source volume's `created_lsn`.
        db.insert(src, 0, v(1)).unwrap();
        let snap = db.take_snapshot(src).unwrap();
        let snap_lsn = db
            .manifest()
            .snapshots
            .iter()
            .find(|s| s.id == snap)
            .map(|s| s.created_lsn)
            .unwrap();
        assert!(snap_lsn > 0, "snapshot's created_lsn should be non-zero");
        let clone = db.clone_volume(snap).unwrap();

        // Live view: source has no parent; clone points at source +
        // snapshot LSN; cursor still idle.
        let m = db.manifest();
        let src_entry = m.volumes.iter().find(|v| v.ord == src).unwrap();
        assert_eq!(src_entry.parent_vol_ord, None);
        assert_eq!(src_entry.branched_at_lsn, 0);
        let clone_entry = m.volumes.iter().find(|v| v.ord == clone).unwrap();
        assert_eq!(clone_entry.parent_vol_ord, Some(src));
        assert_eq!(clone_entry.branched_at_lsn, snap_lsn);
        assert_eq!(clone_entry.promotion_cursor, None);

        // Persist the manifest so the reopen check below is meaningful.
        db.flush().unwrap();
        (src, clone, snap_lsn)
    };

    // Reopen — lineage fields survive both the manifest round-trip and
    // the Volume reconstruction that seeds them back from `VolumeEntry`.
    let db = Db::open(dir.path()).unwrap();
    let m = db.manifest();
    let clone_entry = m.volumes.iter().find(|v| v.ord == clone).unwrap();
    assert_eq!(clone_entry.parent_vol_ord, Some(src));
    assert_eq!(clone_entry.branched_at_lsn, expected_branched_at);
    assert_eq!(clone_entry.promotion_cursor, None);
}

// -------- phase 7 commit 11: streaming iterators --------
