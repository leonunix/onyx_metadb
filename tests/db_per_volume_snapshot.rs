//! Phase 7 commit 9 integration: per-volume `take_snapshot` / routing
//! through `entry.vol_ord`. Covers snapshot isolation across volumes,
//! drop-snapshot side effects staying scoped to the source volume, and
//! crash recovery with snapshots on non-bootstrap volumes.

use onyx_metadb::{Config, Db, L2pValue, VerifyOptions, verify_path};
use tempfile::TempDir;

fn v(n: u8) -> L2pValue {
    let mut x = [0u8; 28];
    x[0] = n;
    L2pValue(x)
}

#[test]
fn snapshots_on_different_volumes_are_isolated() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let a = db.create_volume().unwrap();
    let b = db.create_volume().unwrap();

    db.insert(a, 1, v(0xAA)).unwrap();
    db.insert(b, 1, v(0xBB)).unwrap();

    let snap_a = db.take_snapshot(a).unwrap();
    let snap_b = db.take_snapshot(b).unwrap();

    // Mutate both volumes after the snapshots.
    db.insert(a, 1, v(0x01)).unwrap();
    db.insert(b, 1, v(0x02)).unwrap();

    let view_a = db.snapshot_view(snap_a).unwrap();
    let view_b = db.snapshot_view(snap_b).unwrap();
    assert_eq!(view_a.vol_ord(), a);
    assert_eq!(view_b.vol_ord(), b);
    assert_eq!(view_a.get(1).unwrap(), Some(v(0xAA)));
    assert_eq!(view_b.get(1).unwrap(), Some(v(0xBB)));
    assert_eq!(db.get(a, 1).unwrap(), Some(v(0x01)));
    assert_eq!(db.get(b, 1).unwrap(), Some(v(0x02)));
}

#[test]
fn snapshot_on_non_bootstrap_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let (a, snap) = {
        let db = Db::create(dir.path()).unwrap();
        let a = db.create_volume().unwrap();
        for i in 0u64..64 {
            db.insert(a, i, v(i as u8)).unwrap();
        }
        let snap = db.take_snapshot(a).unwrap();
        for i in 0u64..64 {
            db.insert(a, i, v(0xFF)).unwrap();
        }
        db.flush().unwrap();
        (a, snap)
    };

    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.volumes(), vec![0, a]);
    let entry = db
        .snapshots()
        .into_iter()
        .find(|s| s.id == snap)
        .unwrap();
    assert_eq!(entry.vol_ord, a);

    let view = db.snapshot_view(snap).unwrap();
    for i in 0u64..64 {
        assert_eq!(view.get(i).unwrap(), Some(v(i as u8)));
    }
    for i in 0u64..64 {
        assert_eq!(db.get(a, i).unwrap(), Some(v(0xFF)));
    }
}

#[test]
fn dropping_snapshot_does_not_disturb_unrelated_volume() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let a = db.create_volume().unwrap();
    let b = db.create_volume().unwrap();

    for i in 0u64..32 {
        db.insert(a, i, v(i as u8 | 0x80)).unwrap();
        db.insert(b, i, v(i as u8)).unwrap();
    }
    let snap_a = db.take_snapshot(a).unwrap();
    // Write-over to make the drop actually free pages instead of just
    // touching the root.
    for i in 0u64..32 {
        db.insert(a, i, v(0)).unwrap();
    }

    let _ = db.drop_snapshot(snap_a).unwrap().unwrap();

    for i in 0u64..32 {
        assert_eq!(db.get(b, i).unwrap(), Some(v(i as u8)));
    }
}

#[test]
fn snapshots_for_reports_only_target_volume() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let a = db.create_volume().unwrap();
    let b = db.create_volume().unwrap();

    let sa1 = db.take_snapshot(a).unwrap();
    let sa2 = db.take_snapshot(a).unwrap();
    let sb1 = db.take_snapshot(b).unwrap();

    let mut on_a: Vec<_> = db.snapshots_for(a).into_iter().map(|e| e.id).collect();
    on_a.sort_unstable();
    assert_eq!(on_a, vec![sa1, sa2]);
    assert_eq!(
        db.snapshots_for(b).into_iter().map(|e| e.id).collect::<Vec<_>>(),
        vec![sb1]
    );
}

#[test]
fn drop_volume_while_snapshot_exists_is_refused() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let ord = db.create_volume().unwrap();
    db.insert(ord, 1, v(1)).unwrap();
    let _snap = db.take_snapshot(ord).unwrap();

    assert!(db.drop_volume(ord).is_err());
    assert_eq!(db.volumes(), vec![0, ord]);
}

/// Reproducer for the soak failure at cycle 2801 (20260422T145020Z):
/// `take_snapshot` incref'd + flushed shard root refcounts before
/// `store.commit(&manifest)` ran, so when the manifest encode failed
/// the capacity check the refcount bumps stayed on disk as orphans.
/// Offline verify then reported every L2P shard root as
/// `header=N+1, expected=N`.
///
/// To trigger the capacity-check failure deterministically we dial
/// `shards_per_partition` up to 240 — with one bootstrap volume that
/// leaves room for exactly 4 snapshot entries in the v6 manifest.
/// The 5th `take_snapshot` must fail *atomically*: page refcounts
/// must not leak.
#[test]
fn take_snapshot_capacity_failure_does_not_leak_refcount() {
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.shards_per_partition = 240;
    cfg.direct_io = false;
    let db = Db::create_with_config(cfg).unwrap();

    // Take snapshots on the bootstrap volume (ord 0) until the cap is
    // hit. With 240 shards and 1 volume, max_snapshots = 4.
    let mut taken = 0usize;
    let err = loop {
        match db.take_snapshot(0) {
            Ok(_) => {
                taken += 1;
                assert!(taken <= 64, "snapshot cap should trigger well before 64");
            }
            Err(e) => break e,
        }
    };
    assert!(
        taken >= 1,
        "should have taken at least one snapshot before hitting cap"
    );
    let msg = err.to_string();
    assert!(
        msg.contains("snapshot count") && msg.contains("exceeds capacity"),
        "expected capacity-exceeded error, got {msg:?}"
    );

    // Drop the db so verify_path can open the store cleanly.
    drop(db);

    let report = verify_path(dir.path(), VerifyOptions::default()).unwrap();
    let rc_issues: Vec<&String> = report
        .issues
        .iter()
        .filter(|i| i.contains("refcount mismatch"))
        .collect();
    assert!(
        rc_issues.is_empty(),
        "take_snapshot failed after flush — orphan refcount bumps on disk: {rc_issues:#?}"
    );
}

#[test]
fn diff_between_same_volume_snapshots_works() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let ord = db.create_volume().unwrap();

    for i in 0u64..8 {
        db.insert(ord, i, v(1)).unwrap();
    }
    let a = db.take_snapshot(ord).unwrap();
    db.insert(ord, 0, v(2)).unwrap();
    let b = db.take_snapshot(ord).unwrap();

    let diff = db.diff(a, b).unwrap();
    assert_eq!(diff.len(), 1, "only key 0 changed");
}
