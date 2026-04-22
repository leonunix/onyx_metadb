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
        let _ = db.take_snapshot().unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get_dedup(&h(777)).unwrap(), Some(dval(7)));
}

#[test]
fn drop_snapshot_checkpoint_does_not_drop_unflushed_dedup_rows() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        let snap = db.take_snapshot().unwrap();
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

            let snap = db.take_snapshot().unwrap();

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
