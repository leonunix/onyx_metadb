//! Regression: successive `clone_volume` calls on the same snapshot used
//! to leave the first clone's `PagedL2p::PageBuf` with a stale Clean copy
//! of the shared root, because `apply_clone_volume_incref` only
//! invalidated the source volume's PageBuf. A later
//! `take_snapshot`/`cow_for_write` on the first clone would then flush
//! the stale rc back over the disk-direct bump, losing one refcount per
//! subsequent clone. Eventually `drop_volume` on one of the clones
//! decrements rc to 0 and frees a page the surviving clone still points
//! at — reopen then fails with
//! `paged format: expected PagedLeaf/PagedIndex, got Free`.
//!
//! Crashes-without-flush variant: the same sequence, but without a
//! pre-reopen flush. Exercises the drop-path's pre-apply manifest
//! commit + WAL replay idempotency that closes the crash window the
//! old `db_volume_proptest.rs::Op::Reopen` workaround used to dodge.

use onyx_metadb::{Db, L2pValue};
use tempfile::TempDir;

fn v(n: u8) -> L2pValue {
    let mut x = [0u8; onyx_metadb::paged::LEAF_VALUE_SIZE];
    // v5: store n in the LOW byte of the big-endian u64 base_pba so
    // distinct v(n) values stay within u32 of each other.
    x[7] = n;
    x[onyx_metadb::paged::LEAF_VALUE_SIZE - 1] = 1;
    L2pValue(x)
}

#[test]
fn clone_snapshot_take_drop_then_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();

    {
        let db = Db::create(path).unwrap();
        let s1 = db.take_snapshot(0).unwrap();
        let c1 = db.clone_volume(s1).unwrap();
        let _c2 = db.clone_volume(s1).unwrap();
        let s2 = db.take_snapshot(c1).unwrap();
        db.drop_snapshot(s1).unwrap().unwrap();
        db.drop_snapshot(s2).unwrap().unwrap();
        db.insert(0, 0, v(0)).unwrap();
        db.insert(0, 0, v(0)).unwrap();
        db.drop_volume(c1).unwrap().unwrap();
        db.flush().unwrap();
    }

    Db::open(path).expect("reopen should succeed after drop_volume on a clone");
}

#[test]
fn clone_snapshot_take_drop_then_crash_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();

    {
        let db = Db::create(path).unwrap();
        let s1 = db.take_snapshot(0).unwrap();
        let c1 = db.clone_volume(s1).unwrap();
        let _c2 = db.clone_volume(s1).unwrap();
        let s2 = db.take_snapshot(c1).unwrap();
        db.drop_snapshot(s1).unwrap().unwrap();
        db.drop_snapshot(s2).unwrap().unwrap();
        db.insert(0, 0, v(0)).unwrap();
        db.insert(0, 0, v(0)).unwrap();
        db.drop_volume(c1).unwrap().unwrap();
        // No flush: drop the Db to simulate mid-session crash.
    }

    Db::open(path)
        .expect("reopen after drop_volume on a clone must succeed without an intervening flush");
}

/// Regression for the BFG `need_birth` gap (found in /code-review): a
/// NON-clone volume that shares L2P pages with a clone — after the linking
/// snapshot is dropped — must read each page's REAL `birth_lsn` when COW-killing,
/// not `0`. The clone-source pin (`clone_cow_pinners` on the non-clone arm)
/// replaces the deleted page-rc floor; if `need_birth` omits
/// `clone_cow_pinners`, the overwrite reads `birth = 0`, stamping a
/// `birth_lsn = 0` record into the source volume's page-deadlist. A LATER
/// non-clone deadlist drop (after the clone is gone → routing flips off
/// reachability back to the deadlist) then feeds `birth_lsn = 0` into
/// `dl_record_freed`, mis-classifying the record → HARD page-deadlist shadow
/// Corruption (premature free / completeness hole). Drives the full sequence;
/// the assertion is that the drops + reopen all succeed.
#[test]
fn nonclone_clone_source_overwrite_then_deadlist_drop_no_corruption() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();

    {
        let db = Db::create(path).unwrap();
        // Give vol0 real pages (born at lsns > 0) across more than one leaf.
        for i in 0..40u64 {
            db.insert(0, i * 64, v((i + 1) as u8)).unwrap();
        }
        let s1 = db.take_snapshot(0).unwrap();
        let c1 = db.clone_volume(s1).unwrap();
        // Drop the linking snapshot while the clone lives: vol0 and c1 now share
        // pages with NO snapshot anchor (vol0.snapshot_wms empty, clone_cow_pinners
        // = {c1.branch}). This is the path `need_birth` must still read birth on.
        db.drop_snapshot(s1).unwrap().unwrap();
        // Overwrite the shared pages on the non-clone SOURCE — the buggy COW.
        for i in 0..40u64 {
            db.insert(0, i * 64, v((i + 100) as u8)).unwrap();
        }
        db.flush().unwrap();
        // Only clone gone → no CLONE_LINEAGE volume → snapshot drops route to the
        // NON-clone page-deadlist free-source (HARD `check_page_deadlist_shadow`).
        db.drop_volume(c1).unwrap().unwrap();
        // vol0 snapshot churn → processes vol0's page-deadlist (with the records
        // from the clone-source overwrite above). The HARD shadow fires here if a
        // birth_lsn=0 record mis-classifies.
        let s3 = db.take_snapshot(0).unwrap();
        for i in 0..40u64 {
            db.insert(0, i * 64, v((i + 200) as u8)).unwrap();
        }
        db.drop_snapshot(s3).unwrap().unwrap();
        db.flush().unwrap();
    }

    Db::open(path).expect("reopen after clone-source overwrite + non-clone deadlist drop");
}
