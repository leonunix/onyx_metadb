//! Phase 7 commit 10 integration: VDO-style `clone_volume`. Covers the
//! read-path (clone mirrors snapshot state), CoW divergence on writes,
//! drop ordering between snapshots, source volumes, and clones, and
//! crash-recovery after WAL replay.

use onyx_metadb::{Db, L2pValue};
use tempfile::TempDir;

fn v(n: u8) -> L2pValue {
    let mut x = [0u8; 28];
    x[0] = n;
    L2pValue(x)
}

#[test]
fn clone_mirrors_snapshot_then_diverges() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let src = db.create_volume().unwrap();
    for i in 0u64..64 {
        db.insert(src, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(snap).unwrap();
    for i in 0u64..64 {
        assert_eq!(db.get(clone, i).unwrap(), Some(v(i as u8)));
    }

    // Overwrite on both sides; neither should leak into the other.
    for i in 0u64..64 {
        db.insert(src, i, v(0xA0 | (i as u8))).unwrap();
        db.insert(clone, i, v(0xC0 | (i as u8))).unwrap();
    }
    for i in 0u64..64 {
        assert_eq!(db.get(src, i).unwrap(), Some(v(0xA0 | (i as u8))));
        assert_eq!(db.get(clone, i).unwrap(), Some(v(0xC0 | (i as u8))));
        // Snapshot still anchored to the pre-write state.
        let view = db.snapshot_view(snap).unwrap();
        assert_eq!(view.get(i).unwrap(), Some(v(i as u8)));
    }
}

#[test]
fn clone_survives_reopen_after_flush() {
    let dir = TempDir::new().unwrap();
    let (src, snap, clone) = {
        let db = Db::create(dir.path()).unwrap();
        let src = db.create_volume().unwrap();
        for i in 0u64..16 {
            db.insert(src, i, v(i as u8)).unwrap();
        }
        let snap = db.take_snapshot(src).unwrap();
        let clone = db.clone_volume(snap).unwrap();
        for i in 0u64..16 {
            db.insert(clone, i, v(0x80 | (i as u8))).unwrap();
        }
        db.flush().unwrap();
        (src, snap, clone)
    };
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.volumes(), vec![0, src, clone]);
    for i in 0u64..16 {
        assert_eq!(db.get(src, i).unwrap(), Some(v(i as u8)));
        assert_eq!(db.get(clone, i).unwrap(), Some(v(0x80 | (i as u8))));
    }
    // Snapshot entry still pinned to the source volume.
    let snaps = db.snapshots();
    assert_eq!(snaps.len(), 1);
    assert_eq!(snaps[0].id, snap);
    assert_eq!(snaps[0].vol_ord, src);
}

#[test]
fn clone_survives_reopen_via_wal_replay() {
    let dir = TempDir::new().unwrap();
    let (src, clone) = {
        let db = Db::create(dir.path()).unwrap();
        let src = db.create_volume().unwrap();
        for i in 0u64..8 {
            db.insert(src, i, v(i as u8)).unwrap();
        }
        let snap = db.take_snapshot(src).unwrap();
        let clone = db.clone_volume(snap).unwrap();
        db.insert(clone, 0, v(0xFF)).unwrap();
        // No flush — recovery must replay CloneVolume from the WAL.
        (src, clone)
    };
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.volumes(), vec![0, src, clone]);
    assert_eq!(db.get(clone, 0).unwrap(), Some(v(0xFF)));
    for i in 1u64..8 {
        assert_eq!(db.get(clone, i).unwrap(), Some(v(i as u8)));
        assert_eq!(db.get(src, i).unwrap(), Some(v(i as u8)));
    }
    assert!(db.manifest().next_volume_ord >= clone + 1);
}

#[test]
fn drop_snapshot_does_not_affect_live_clone() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let src = db.create_volume().unwrap();
    for i in 0u64..32 {
        db.insert(src, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(snap).unwrap();

    // Drop the source snapshot after the clone is established.
    let _ = db.drop_snapshot(snap).unwrap().unwrap();
    assert!(db.snapshots().is_empty());

    for i in 0u64..32 {
        assert_eq!(db.get(clone, i).unwrap(), Some(v(i as u8)));
    }

    // Subsequent writes on the clone still work.
    db.insert(clone, 0, v(0xEE)).unwrap();
    assert_eq!(db.get(clone, 0).unwrap(), Some(v(0xEE)));
}

#[test]
fn drop_clone_leaves_source_intact() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let src = db.create_volume().unwrap();
    for i in 0u64..16 {
        db.insert(src, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(snap).unwrap();
    db.insert(clone, 0, v(0xC0)).unwrap();

    // Dropping the snapshot is blocked while the clone is live? No —
    // snapshot pins source-volume pages independently of the clone.
    // We drop the snapshot first, then the clone; source stays valid.
    let _ = db.drop_snapshot(snap).unwrap().unwrap();
    let _ = db.drop_volume(clone).unwrap().unwrap();

    for i in 0u64..16 {
        assert_eq!(db.get(src, i).unwrap(), Some(v(i as u8)));
    }
    assert_eq!(db.volumes(), vec![0, src]);
}

#[test]
fn chained_clones_each_diverge_independently() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let src = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(src, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(src).unwrap();
    let clone_a = db.clone_volume(snap).unwrap();
    let clone_b = db.clone_volume(snap).unwrap();

    db.insert(clone_a, 0, v(0xAA)).unwrap();
    db.insert(clone_b, 0, v(0xBB)).unwrap();

    assert_eq!(db.get(src, 0).unwrap(), Some(v(0)));
    assert_eq!(db.get(clone_a, 0).unwrap(), Some(v(0xAA)));
    assert_eq!(db.get(clone_b, 0).unwrap(), Some(v(0xBB)));
    for i in 1u64..8 {
        assert_eq!(db.get(clone_a, i).unwrap(), Some(v(i as u8)));
        assert_eq!(db.get(clone_b, i).unwrap(), Some(v(i as u8)));
    }
}

#[test]
fn clone_of_empty_snapshot_is_empty_and_writable() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let src = db.create_volume().unwrap();
    let snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(snap).unwrap();
    assert_eq!(db.get(clone, 0).unwrap(), None);
    db.insert(clone, 0, v(7)).unwrap();
    assert_eq!(db.get(clone, 0).unwrap(), Some(v(7)));
    assert_eq!(db.get(src, 0).unwrap(), None);
}

#[test]
fn take_snapshot_on_clone_diffs_from_source_snapshot() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let src = db.create_volume().unwrap();
    db.insert(src, 1, v(1)).unwrap();
    let src_snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(src_snap).unwrap();
    db.insert(clone, 1, v(2)).unwrap();
    let clone_snap = db.take_snapshot(clone).unwrap();

    // Each snapshot is pinned to its own volume.
    let src_entry = db
        .snapshots()
        .into_iter()
        .find(|s| s.id == src_snap)
        .unwrap();
    let clone_entry = db
        .snapshots()
        .into_iter()
        .find(|s| s.id == clone_snap)
        .unwrap();
    assert_eq!(src_entry.vol_ord, src);
    assert_eq!(clone_entry.vol_ord, clone);

    // Diff across different source volumes is still rejected.
    assert!(db.diff(src_snap, clone_snap).is_err());
}
