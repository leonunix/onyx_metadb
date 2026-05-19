//! Phase 3 (no-refcount-hot-path) Lineage GC end-to-end tests.
//!
//! These exercise the head_pid advance path that the background
//! `metadb-async-reclaim` worker normally drives. Tests use
//! `Db::test_run_lineage_gc_cycle()` so they can assert state without
//! racing the worker thread.

use super::{mk_db, v};
use crate::Db;
use crate::types::NULL_PAGE;

fn dead_list_anchors(db: &Db, vol_ord: u16) -> (u64, u64) {
    db.test_dead_list_anchors(vol_ord)
        .expect("test helper expects volume to exist")
}

#[test]
fn lineage_gc_noop_on_empty_chain() {
    // Fresh Db, no dead-list writes, no segments.
    let (_d, db) = mk_db();
    let advanced = db.test_run_lineage_gc_cycle().unwrap();
    assert_eq!(advanced, 0);
    let (head, tail) = dead_list_anchors(&db, 0);
    assert_eq!(head, NULL_PAGE);
    assert_eq!(tail, NULL_PAGE);
}

#[test]
fn lineage_gc_advances_head_when_rc_zero_and_no_snapshot() {
    // Write + overwrite a few LBAs so the dead-list buffer fills, then
    // flush to materialize a segment. The hot path's RC decref on
    // overwrite drives each old PBA's rc to 0 before the GC runs, so
    // every record in the head segment passes both filters.
    let (_d, db) = mk_db();
    for i in 0u64..32 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    for i in 0u64..32 {
        db.insert(0, i, v((i as u8).wrapping_add(1))).unwrap();
    }
    db.flush().unwrap();
    let (head_before, tail_before) = dead_list_anchors(&db, 0);
    assert_ne!(head_before, NULL_PAGE);
    assert_eq!(head_before, tail_before, "single-segment chain");

    // GC should advance head past the only segment, clearing both
    // anchors back to NULL_PAGE.
    let advanced = db.test_run_lineage_gc_cycle().unwrap();
    assert_eq!(advanced, 1);
    let (head_after, tail_after) = dead_list_anchors(&db, 0);
    assert_eq!(head_after, NULL_PAGE);
    assert_eq!(tail_after, NULL_PAGE);
}

#[test]
fn lineage_gc_defers_when_snapshot_pins_record() {
    // Sequence: write, snapshot (pins the original birth_lsn through
    // its created_lsn), overwrite (records the death). The dead
    // record's [birth, death) window overlaps the snapshot's
    // created_lsn, so the snap-pin filter blocks GC.
    let (_d, db) = mk_db();
    for i in 0u64..16 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    // Snapshot now. created_lsn > all current birth_lsns; later
    // overwrites will give the dead records death_lsn > snapshot's
    // created_lsn, putting created_lsn inside [birth, death).
    let _snap = db.take_snapshot(0).unwrap();
    for i in 0u64..16 {
        db.insert(0, i, v((i as u8).wrapping_add(1))).unwrap();
    }
    db.flush().unwrap();
    let (head_before, _) = dead_list_anchors(&db, 0);
    assert_ne!(head_before, NULL_PAGE);

    // GC should refuse to advance — the snapshot still pins the
    // PBAs in the head segment.
    let advanced = db.test_run_lineage_gc_cycle().unwrap();
    assert_eq!(advanced, 0);
    let (head_after, _) = dead_list_anchors(&db, 0);
    assert_eq!(
        head_after, head_before,
        "snapshot-pinned head must not advance"
    );
}

#[test]
fn lineage_gc_resumes_after_snapshot_drops() {
    let (_d, db) = mk_db();
    for i in 0u64..16 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(0).unwrap();
    for i in 0u64..16 {
        db.insert(0, i, v((i as u8).wrapping_add(1))).unwrap();
    }
    db.flush().unwrap();

    // Pinned: cycle 1 doesn't advance.
    let advanced_pinned = db.test_run_lineage_gc_cycle().unwrap();
    assert_eq!(advanced_pinned, 0);

    // Drop the snapshot. The cascade decrefs the snapshot's PBAs;
    // post-drop, none of the head segment's records are pinned and
    // their rc has been forced to 0 by drop_snapshot's apply.
    db.drop_snapshot(snap).unwrap();

    // Cycle 2 should now advance head_pid to NULL_PAGE.
    let advanced_after = db.test_run_lineage_gc_cycle().unwrap();
    assert_eq!(advanced_after, 1);
    let (head_after, tail_after) = dead_list_anchors(&db, 0);
    assert_eq!(head_after, NULL_PAGE);
    assert_eq!(tail_after, NULL_PAGE);
}

#[test]
fn lineage_gc_advances_one_segment_at_a_time() {
    // Two flushes → two segments chained tail → head. The first GC
    // cycle reclaims the older (head) segment; head_pid moves to the
    // newer segment's pid, tail_pid stays put. A second cycle then
    // reclaims the remaining segment, leaving the chain empty.
    //
    // Use disjoint per-LBA value bytes across the three writes (each
    // LBA gets bytes 0x10|i, 0x20|i, 0x30|i) so no PBA is shared
    // between LBAs and every hot-path decref takes rc straight to 0.
    let (_d, db) = mk_db();
    for i in 0u64..16 {
        db.insert(0, i, v(0x10 | i as u8)).unwrap();
    }
    for i in 0u64..16 {
        db.insert(0, i, v(0x20 | i as u8)).unwrap();
    }
    db.flush().unwrap();
    let (head_after_first_flush, tail_after_first_flush) = dead_list_anchors(&db, 0);
    assert_eq!(
        head_after_first_flush, tail_after_first_flush,
        "single-segment after first flush"
    );

    for i in 0u64..16 {
        db.insert(0, i, v(0x30 | i as u8)).unwrap();
    }
    db.flush().unwrap();
    let (head_after_second_flush, tail_after_second_flush) = dead_list_anchors(&db, 0);
    assert_eq!(
        head_after_second_flush, head_after_first_flush,
        "head_pid pins to oldest segment"
    );
    assert_ne!(
        tail_after_second_flush, head_after_second_flush,
        "tail moved forward to newer segment"
    );

    // Cycle 1: reclaim the old head; head_pid jumps to the second
    // segment's pid, tail_pid unchanged.
    let advanced_first = db.test_run_lineage_gc_cycle().unwrap();
    assert_eq!(advanced_first, 1);
    let (head_mid, tail_mid) = dead_list_anchors(&db, 0);
    assert_eq!(head_mid, tail_after_second_flush);
    assert_eq!(tail_mid, tail_after_second_flush);

    // Cycle 2: reclaim the now-only segment.
    let advanced_second = db.test_run_lineage_gc_cycle().unwrap();
    assert_eq!(advanced_second, 1);
    let (head_end, tail_end) = dead_list_anchors(&db, 0);
    assert_eq!(head_end, NULL_PAGE);
    assert_eq!(tail_end, NULL_PAGE);
}

#[test]
fn lineage_gc_idempotent_when_chain_already_empty() {
    let (_d, db) = mk_db();
    for i in 0u64..16 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    for i in 0u64..16 {
        db.insert(0, i, v((i as u8).wrapping_add(1))).unwrap();
    }
    db.flush().unwrap();
    // First cycle advances + clears.
    assert_eq!(db.test_run_lineage_gc_cycle().unwrap(), 1);
    // Subsequent cycles are no-ops.
    assert_eq!(db.test_run_lineage_gc_cycle().unwrap(), 0);
    assert_eq!(db.test_run_lineage_gc_cycle().unwrap(), 0);
}

#[test]
fn lineage_gc_advance_survives_reopen() {
    use tempfile::TempDir;
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        for i in 0u64..16 {
            db.insert(0, i, v(i as u8)).unwrap();
        }
        for i in 0u64..16 {
            db.insert(0, i, v((i as u8).wrapping_add(1))).unwrap();
        }
        db.flush().unwrap();
        let (head_before, _) = db.test_dead_list_anchors(0).unwrap();
        assert_ne!(head_before, NULL_PAGE);
        assert_eq!(db.test_run_lineage_gc_cycle().unwrap(), 1);
        let (head_after, tail_after) = db.test_dead_list_anchors(0).unwrap();
        assert_eq!(head_after, NULL_PAGE);
        assert_eq!(tail_after, NULL_PAGE);
        // GC's manifest commit must be durable — but the GC pass
        // ran AFTER flush, so the chain anchors landed in the
        // current manifest slot. Drop forces the wait for any
        // pending background work.
    }
    let db = Db::open(dir.path()).unwrap();
    let (head_reopen, tail_reopen) = db.test_dead_list_anchors(0).unwrap();
    // Either NULL_PAGE (GC committed) or the original chain (GC
    // didn't commit). Phase 3 plumbing-only: we asserted GC ran
    // pre-close so the manifest must reflect that.
    assert_eq!(head_reopen, NULL_PAGE);
    assert_eq!(tail_reopen, NULL_PAGE);
}
