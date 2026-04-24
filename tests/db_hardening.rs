use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use onyx_metadb::testing::faults::{FaultAction, FaultController, FaultPoint};
use onyx_metadb::{Db, DedupValue, Hash32, L2pValue, VerifyOptions, verify_path};
use tempfile::TempDir;

fn l2p(n: u8) -> L2pValue {
    let mut x = [0u8; 28];
    x[0] = n;
    L2pValue(x)
}

fn dval(n: u8) -> DedupValue {
    let mut x = [0u8; 28];
    x[0] = n;
    DedupValue(x)
}

fn h(n: u64) -> Hash32 {
    let mut x = [0u8; 32];
    x[..8].copy_from_slice(&n.to_be_bytes());
    x
}

#[test]
fn clean_db_passes_verifier() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    for i in 0..32u64 {
        db.insert(0,i, l2p(i as u8)).unwrap();
        db.put_dedup(h(10_000 + i), dval(i as u8)).unwrap();
        db.incref_pba(20_000 + i, 1).unwrap();
    }
    db.flush().unwrap();
    drop(db);

    let report = verify_path(dir.path(), VerifyOptions { strict: true }).unwrap();
    assert!(
        report.is_clean(),
        "verifier issues: {:?}, warnings: {:?}",
        report.issues,
        report.warnings
    );
}

#[test]
fn crash_after_wal_before_apply_recovers_committed_tx() {
    let dir = TempDir::new().unwrap();
    let faults = FaultController::new();
    let _ = std::panic::catch_unwind(AssertUnwindSafe({
        let faults = faults.clone();
        let path = dir.path().to_path_buf();
        move || {
            let db = Db::create_with_faults(&path, faults.clone()).unwrap();
            faults.install(FaultPoint::CommitPostWalBeforeApply, 1, FaultAction::Panic);
            let mut tx = db.begin();
            tx.insert(0,1, l2p(9));
            tx.put_dedup(h(7), dval(7));
            tx.incref_pba(42, 2);
            let _ = tx.commit();
        }
    }));

    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(0,1).unwrap(), Some(l2p(9)));
    assert_eq!(db.get_dedup(&h(7)).unwrap(), Some(dval(7)));
    assert_eq!(db.get_refcount(42).unwrap(), 2);
}

#[test]
fn crash_after_apply_before_lsn_bump_does_not_double_apply_on_reopen() {
    let dir = TempDir::new().unwrap();
    let faults = FaultController::new();
    let _ = std::panic::catch_unwind(AssertUnwindSafe({
        let faults = faults.clone();
        let path = dir.path().to_path_buf();
        move || {
            let db = Db::create_with_faults(&path, faults.clone()).unwrap();
            db.incref_pba(100, 5).unwrap();
            faults.install(
                FaultPoint::CommitPostApplyBeforeLsnBump,
                1,
                FaultAction::Panic,
            );
            let mut tx = db.begin();
            tx.insert(0,1, l2p(1));
            tx.put_dedup(h(99), dval(9));
            tx.incref_pba(100, 2);
            let _ = tx.commit();
        }
    }));

    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(0,1).unwrap(), Some(l2p(1)));
    assert_eq!(db.get_dedup(&h(99)).unwrap(), Some(dval(9)));
    assert_eq!(db.get_refcount(100).unwrap(), 7);
}

#[test]
fn manifest_swap_crash_reclaims_orphans_on_open() {
    let dir = TempDir::new().unwrap();
    let faults = FaultController::new();

    let _ = std::panic::catch_unwind(AssertUnwindSafe({
        let faults = Arc::clone(&faults);
        let path = dir.path().to_path_buf();
        move || {
            let db = Db::create_with_faults(&path, faults.clone()).unwrap();
            for i in 0..64u64 {
                db.insert(0,i, l2p(i as u8)).unwrap();
                db.put_dedup(h(i), dval(i as u8)).unwrap();
            }
            db.flush().unwrap();
            for i in 64..128u64 {
                db.insert(0,i, l2p(i as u8)).unwrap();
                db.put_dedup(h(i), dval(i as u8)).unwrap();
            }
            faults.install(
                FaultPoint::FlushPostLevelRewriteBeforeManifest,
                1,
                FaultAction::Panic,
            );
            let _ = db.flush();
        }
    }));

    let db = Db::open(dir.path()).unwrap();
    for i in 0..128u64 {
        assert_eq!(db.get(0,i).unwrap(), Some(l2p(i as u8)));
        assert_eq!(db.get_dedup(&h(i)).unwrap(), Some(dval(i as u8)));
    }
    drop(db);

    let report = verify_path(dir.path(), VerifyOptions { strict: true }).unwrap();
    assert!(
        report.is_clean(),
        "verifier issues: {:?}, warnings: {:?}",
        report.issues,
        report.warnings
    );
}

#[test]
fn snapshot_checkpoint_does_not_drop_unflushed_dedup_rows() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        db.put_dedup(h(777), dval(7)).unwrap();
        let _ = db.take_snapshot(0).unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get_dedup(&h(777)).unwrap(), Some(dval(7)));
}

#[test]
fn drop_snapshot_checkpoint_does_not_drop_unflushed_dedup_rows() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        let snap = db.take_snapshot(0).unwrap();
        db.put_dedup(h(888), dval(8)).unwrap();
        let _ = db.drop_snapshot(snap).unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get_dedup(&h(888)).unwrap(), Some(dval(8)));
}

/// Regression: `drop_snapshot` used to commit the manifest *before* running
/// `drop_subtree` to decref pages that the snapshot was sharing with the
/// current tree. A `manifest.fsync.before` fault during that commit would
/// leave the new (snapshot-removed) manifest durable in the OS page cache
/// while none of the decrefs reached the page headers, producing
/// `page N refcount mismatch: header=2, expected=1` at the next open.
///
/// Reproduces the soak failure from 20260421T135914Z cycle 86.
///
/// Post-fix, `drop_snapshot` is WAL-logged: after `wal.submit` durably
/// records the op, a crash before apply completes is recovered on
/// reopen via replay. `CommitPostWalBeforeApply` panics right after WAL
/// fsync and before apply has touched any page — the worst case for the
/// old non-WAL design, since the intent is durable but no page work has
/// happened yet.
#[test]
fn drop_snapshot_crash_at_manifest_commit_preserves_refcount_consistency() {
    let dir = TempDir::new().unwrap();
    let faults = FaultController::new();

    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe({
        let faults = faults.clone();
        let path = dir.path().to_path_buf();
        move || {
            let db = Db::create_with_faults(&path, faults.clone()).unwrap();

            // Populate enough entries to force the L2P tree to span
            // multiple index/leaf pages so the snapshot genuinely shares
            // pages with the current tree (not just a single root).
            for i in 0..512u64 {
                db.insert(0,i, l2p(i as u8)).unwrap();
            }
            db.flush().unwrap();

            let snap = db.take_snapshot(0).unwrap();

            // Fire on the first post-WAL-fsync injection — guaranteed to
            // be inside drop_snapshot's commit_ops because no other
            // commits happen after take_snapshot above.
            faults.install(FaultPoint::CommitPostWalBeforeApply, 1, FaultAction::Panic);

            let _ = db.drop_snapshot(snap);
            // Unreachable: the fault panics before drop_snapshot
            // returns. If we do reach this point the fault didn't fire,
            // meaning drop_snapshot short-circuited before submitting
            // the WAL op — update the regression once the code shape
            // changes.
            panic!("drop_snapshot returned but fault should have panicked");
        }
    }));

    // Reopen + run the offline verifier. A correct WAL-logged drop
    // leaves either (a) the WAL record durable and replay re-drives the
    // apply on open, or (b) the record is a torn tail and the snapshot
    // is still intact — either way every live page has the refcount the
    // tree structure implies.
    let db = Db::open(dir.path()).unwrap();
    for i in 0..512u64 {
        assert_eq!(db.get(0,i).unwrap(), Some(l2p(i as u8)));
    }
    drop(db);

    let report = verify_path(dir.path(), VerifyOptions::default()).unwrap();
    assert!(
        report.is_clean(),
        "verifier reported issues after drop_snapshot crash: {:?}",
        report.issues
    );
}

// ---- Phase 7 commit 12: volume-lifecycle fault-injection regressions ----

/// `create_volume` durably records the `CreateVolume` WAL op before
/// committing the manifest. A crash at
/// `FaultPoint::CreateVolumePostWalBeforeManifest` simulates the
/// window where the WAL record is fsync'd but the in-memory volumes
/// table hasn't been updated. Recovery must see the WAL record on
/// reopen and reconstruct the volume via the replay arm.
#[test]
fn create_volume_crash_after_wal_before_manifest_reconstructs() {
    let dir = TempDir::new().unwrap();
    let faults = FaultController::new();

    let _ = std::panic::catch_unwind(AssertUnwindSafe({
        let faults = faults.clone();
        let path = dir.path().to_path_buf();
        move || {
            let db = Db::create_with_faults(&path, faults.clone()).unwrap();
            faults.install(
                FaultPoint::CreateVolumePostWalBeforeManifest,
                1,
                FaultAction::Panic,
            );
            let _ = db.create_volume();
            panic!("create_volume returned but fault should have panicked");
        }
    }));

    let db = Db::open(dir.path()).unwrap();
    // Post-recovery, the clone/creation op replays and the new volume
    // should appear in `volumes()`.
    let vols = db.volumes();
    assert_eq!(vols, vec![0, 1], "volumes after recovery: {vols:?}");
    // The new volume is operable end-to-end.
    db.insert(1, 42, l2p(7)).unwrap();
    assert_eq!(db.get(1, 42).unwrap(), Some(l2p(7)));
    drop(db);

    let report = verify_path(dir.path(), VerifyOptions::default()).unwrap();
    assert!(
        report.is_clean(),
        "verifier reported issues after create_volume crash: {:?}",
        report.issues
    );
}

/// `drop_volume`'s WAL record is durable before
/// `apply_drop_volume` touches any page refcount.
/// `FaultPoint::DropVolumePostWalBeforeApply` panics in that window;
/// reopen must replay the op and free the volume's pages.
#[test]
fn drop_volume_crash_after_wal_before_apply_recovers() {
    let dir = TempDir::new().unwrap();
    let faults = FaultController::new();

    // Set up: create a volume and populate it, then simulate a crash
    // during its drop.
    let _ = std::panic::catch_unwind(AssertUnwindSafe({
        let faults = faults.clone();
        let path = dir.path().to_path_buf();
        move || {
            let db = Db::create_with_faults(&path, faults.clone()).unwrap();
            let ord = db.create_volume().unwrap();
            for i in 0u64..32 {
                db.insert(ord, i, l2p(i as u8)).unwrap();
            }
            db.flush().unwrap();

            faults.install(
                FaultPoint::DropVolumePostWalBeforeApply,
                1,
                FaultAction::Panic,
            );
            let _ = db.drop_volume(ord);
            panic!("drop_volume returned but fault should have panicked");
        }
    }));

    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.volumes(), vec![0], "dropped volume must be gone after recovery");
    drop(db);

    let report = verify_path(dir.path(), VerifyOptions::default()).unwrap();
    assert!(
        report.is_clean(),
        "verifier reported issues after drop_volume crash: {:?}",
        report.issues
    );
}

/// Mid-incref crash inside `apply_clone_volume_incref` — one shard
/// root has its refcount bumped + generation stamped on disk, the rest
/// have not. Recovery's generation guard must skip the already-stamped
/// root and finish the remaining ones. Final clone should read back
/// the source snapshot's data end-to-end.
#[test]
fn clone_volume_crash_mid_incref_completes_on_recovery() {
    let dir = TempDir::new().unwrap();
    let faults = FaultController::new();

    let snap_id;
    let setup = |path: &std::path::Path| {
        let db = Db::create(path).unwrap();
        for i in 0u64..16 {
            db.insert(0, i, l2p(i as u8)).unwrap();
        }
        let sid = db.take_snapshot(0).unwrap();
        db.flush().unwrap();
        sid
    };
    snap_id = setup(dir.path());

    let _ = std::panic::catch_unwind(AssertUnwindSafe({
        let faults = faults.clone();
        let path = dir.path().to_path_buf();
        move || {
            let db = Db::open_with_faults(&path, faults.clone()).unwrap();
            faults.install(FaultPoint::CloneVolumeMidIncref, 1, FaultAction::Panic);
            let _ = db.clone_volume(snap_id);
            panic!("clone_volume returned but fault should have panicked");
        }
    }));

    let db = Db::open(dir.path()).unwrap();
    // The clone completed via replay.
    let vols = db.volumes();
    assert!(
        vols.len() >= 2 && vols.contains(&1),
        "expected clone vol=1 after recovery, got {vols:?}",
    );
    let clone = 1;
    for i in 0u64..16 {
        assert_eq!(db.get(clone, i).unwrap(), Some(l2p(i as u8)));
    }
    drop(db);

    let report = verify_path(dir.path(), VerifyOptions::default()).unwrap();
    assert!(
        report.is_clean(),
        "verifier reported issues after clone_volume mid-incref crash: {:?}",
        report.issues
    );
}

// ----------------- L2pRemap crash-safety (SPEC §5.3) ---------------

/// Encode a pba + tag into the 28-byte `L2pValue` the same way tests
/// in `db::tests` do: head 8 bytes are the target pba BE (Onyx's
/// `BlockmapValue` contract), byte 8 is a discriminator tag.
fn remap_val(pba: u64, tag: u8) -> L2pValue {
    let mut v = [0u8; 28];
    v[..8].copy_from_slice(&pba.to_be_bytes());
    v[8] = tag;
    L2pValue(v)
}

/// Crash right after the WAL record landed but before apply ran.
/// Recovery must replay the `L2pRemap` so both the L2P mapping and
/// the refcount side-effects end up on disk. Regression test for the
/// SPEC §4.3 guard-atomicity invariant: no half-applied state is
/// allowed.
#[test]
fn crash_after_wal_before_apply_replays_l2p_remap() {
    let dir = TempDir::new().unwrap();
    let faults = FaultController::new();
    let _ = std::panic::catch_unwind(AssertUnwindSafe({
        let faults = faults.clone();
        let path = dir.path().to_path_buf();
        move || {
            let db = Db::create_with_faults(&path, faults.clone()).unwrap();
            // Seed rc(100)=1 so guard tests observe a live target.
            let mut tx = db.begin();
            tx.l2p_remap(0, 10, remap_val(100, 1), None);
            tx.commit().unwrap();
            faults.install(FaultPoint::CommitPostWalBeforeApply, 1, FaultAction::Panic);
            let mut tx = db.begin();
            tx.l2p_remap(0, 11, remap_val(100, 2), Some((100, 1)));
            let _ = tx.commit();
        }
    }));

    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(0, 10).unwrap(), Some(remap_val(100, 1)));
    assert_eq!(
        db.get(0, 11).unwrap(),
        Some(remap_val(100, 2)),
        "replayed L2pRemap restored the lba=11 mapping",
    );
    assert_eq!(
        db.get_refcount(100).unwrap(),
        2,
        "refcount incref from replayed L2pRemap applied exactly once",
    );
}

/// Crash between apply and last_applied_lsn bump. On replay metadb
/// must not double-apply the refcount decref/incref embedded in the
/// L2pRemap. The `page.generation >= lsn` guard on L2P pages plus the
/// checkpoint-lsn ordering rule in `commit_ops` is what keeps the
/// refcount side consistent.
#[test]
fn crash_after_apply_before_lsn_bump_l2p_remap_idempotent() {
    let dir = TempDir::new().unwrap();
    let faults = FaultController::new();
    let _ = std::panic::catch_unwind(AssertUnwindSafe({
        let faults = faults.clone();
        let path = dir.path().to_path_buf();
        move || {
            let db = Db::create_with_faults(&path, faults.clone()).unwrap();
            // Pre-seed rc(100)=2 via two independent remaps so the
            // post-crash remap decrefs without reaching zero — this
            // is the "decref not to zero" path where a double-apply
            // would silently under-count.
            let mut tx = db.begin();
            tx.l2p_remap(0, 10, remap_val(100, 1), None);
            tx.commit().unwrap();
            let mut tx = db.begin();
            tx.l2p_remap(0, 11, remap_val(100, 2), None);
            tx.commit().unwrap();
            assert_eq!(db.get_refcount(100).unwrap(), 2);

            faults.install(
                FaultPoint::CommitPostApplyBeforeLsnBump,
                1,
                FaultAction::Panic,
            );
            let mut tx = db.begin();
            tx.l2p_remap(0, 10, remap_val(200, 1), None);
            let _ = tx.commit();
        }
    }));

    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(0, 10).unwrap(), Some(remap_val(200, 1)));
    assert_eq!(db.get(0, 11).unwrap(), Some(remap_val(100, 2)));
    assert_eq!(
        db.get_refcount(100).unwrap(),
        1,
        "replay must not double-decref; rc should be 1 after one decref",
    );
    assert_eq!(db.get_refcount(200).unwrap(), 1);
}

/// Guard-reject path: the op's WAL record contains `guard = Some`
/// with a threshold that isn't met. Live apply returns `applied =
/// false` and leaves state untouched. A crash immediately after WAL
/// fsync must replay the same decision — guard still rejects, no
/// state change.
#[test]
fn crash_after_wal_with_rejecting_guard_stays_a_no_op_on_replay() {
    let dir = TempDir::new().unwrap();
    let faults = FaultController::new();
    let _ = std::panic::catch_unwind(AssertUnwindSafe({
        let faults = faults.clone();
        let path = dir.path().to_path_buf();
        move || {
            let db = Db::create_with_faults(&path, faults.clone()).unwrap();
            // rc(100) stays 0: guard min_rc=1 must fail.
            faults.install(FaultPoint::CommitPostWalBeforeApply, 1, FaultAction::Panic);
            let mut tx = db.begin();
            tx.l2p_remap(0, 10, remap_val(200, 1), Some((100, 1)));
            let _ = tx.commit();
        }
    }));

    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(0, 10).unwrap(), None, "guard rejected op on replay");
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    assert_eq!(
        db.get_refcount(200).unwrap(),
        0,
        "no incref ran because the op was a no-op",
    );
}
