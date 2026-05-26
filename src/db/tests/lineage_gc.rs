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

// ── Phase 4 Step 2: cross-volume snap_pin via descendant.branched_at_lsn ──

// A clone's `branched_at_lsn` must pin parent dead-list records whose
// `[birth, death)` window contains it — otherwise the parent's GC
// would free a PBA the clone still observes through its COW-shared
// L2P (the background promotion walker has not yet run, so the global
// rc hasn't been bumped for the clone's lineage).
#[test]
fn phase4_lineage_gc_pins_parent_pba_via_descendant_branched_lsn() {
    use super::BOOTSTRAP_VOLUME_ORD;

    let (_d, db) = mk_db();
    // Write a batch of LBAs on the parent. These birth_lsns will be
    // bracketed by the snapshot we take next.
    for i in 0u64..16 {
        db.insert(BOOTSTRAP_VOLUME_ORD, i, v(i as u8)).unwrap();
    }
    // Snapshot + clone establish the lineage. The clone's
    // `branched_at_lsn` equals the snapshot's `created_lsn`, which
    // sits strictly above every prior birth_lsn.
    let snap = db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    let _clone = db.clone_volume(snap).unwrap();
    // Drop the snapshot — without Phase 4's descendant pin, the
    // parent's GC would now happily free the PBAs that the clone
    // still references via the COW L2P. Phase 4 must hold them
    // because `clone.parent_vol_ord == Some(parent)` and the
    // clone's `branched_at_lsn ∈ [birth, death)` for every record
    // about to be emitted by the parent's overwrites.
    db.drop_snapshot(snap).unwrap();

    // Now overwrite the parent's LBAs — each L2pRemap emits a dead
    // record into the bootstrap volume's chain with `death_lsn`
    // strictly above the snapshot's `created_lsn` (= clone's
    // branched_at_lsn).
    for i in 0u64..16 {
        db.insert(BOOTSTRAP_VOLUME_ORD, i, v((i as u8).wrapping_add(1)))
            .unwrap();
    }
    db.flush().unwrap();
    let (head_before, _) = dead_list_anchors(&db, BOOTSTRAP_VOLUME_ORD);
    assert_ne!(head_before, NULL_PAGE);

    let advanced = db.test_run_lineage_gc_cycle().unwrap();
    assert_eq!(
        advanced, 0,
        "cross-volume snap_pin (descendant.branched_at_lsn) must \
         block the parent's GC even though no snapshot is left"
    );
    let (head_after, _) = dead_list_anchors(&db, BOOTSTRAP_VOLUME_ORD);
    assert_eq!(head_after, head_before, "head_pid must not advance");
}

// ── Phase 4 Step 4: GC emits WalOp::FreePbas when flag is on ──

fn mk_db_with_emit_freepbas(flag: bool) -> (tempfile::TempDir, std::sync::Arc<Db>) {
    use crate::Config;
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.lineage_gc_emit_freepbas = flag;
    let db = Db::create_with_config(cfg).unwrap();
    (dir, db)
}

// Flag ON: GC emits a `WalOp::FreePbas` for each advancing volume.
// We can't observe the outcome directly (the driver discards it), so
// the contract test is "the cycle advanced AND the LSN advanced past
// the writes' last LSN", which is only possible if GC committed at
// least one WAL record.
#[test]
fn gc_emits_freepbas_when_flag_on() {
    let (_d, db) = mk_db_with_emit_freepbas(true);
    for i in 0u64..32 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    for i in 0u64..32 {
        db.insert(0, i, v((i as u8).wrapping_add(1))).unwrap();
    }
    db.flush().unwrap();

    let lsn_before_gc = db.last_applied_lsn();
    let (head_before, _) = dead_list_anchors(&db, 0);
    assert_ne!(head_before, NULL_PAGE, "writes should have built a segment");

    let advanced = db.test_run_lineage_gc_cycle().unwrap();
    assert_eq!(advanced, 1);

    // Chain truncated (same as flag-OFF) AND the FreePbas commit
    // advanced last_applied_lsn (only happens under flag ON).
    let (head_after, tail_after) = dead_list_anchors(&db, 0);
    assert_eq!(head_after, NULL_PAGE);
    assert_eq!(tail_after, NULL_PAGE);
    let lsn_after_gc = db.last_applied_lsn();
    assert!(
        lsn_after_gc > lsn_before_gc,
        "flag-ON GC must commit a WalOp::FreePbas (lsn {lsn_before_gc} -> {lsn_after_gc})"
    );
}

// Flag OFF (the default): GC does chain truncation only — no WAL
// record, no LSN advance.
#[test]
fn gc_does_not_emit_freepbas_when_flag_off() {
    let (_d, db) = mk_db_with_emit_freepbas(false);
    for i in 0u64..32 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    for i in 0u64..32 {
        db.insert(0, i, v((i as u8).wrapping_add(1))).unwrap();
    }
    db.flush().unwrap();

    let lsn_before_gc = db.last_applied_lsn();
    let (head_before, _) = dead_list_anchors(&db, 0);
    assert_ne!(head_before, NULL_PAGE);

    let advanced = db.test_run_lineage_gc_cycle().unwrap();
    assert_eq!(advanced, 1);

    let (head_after, tail_after) = dead_list_anchors(&db, 0);
    assert_eq!(head_after, NULL_PAGE);
    assert_eq!(tail_after, NULL_PAGE);
    let lsn_after_gc = db.last_applied_lsn();
    assert_eq!(
        lsn_after_gc, lsn_before_gc,
        "flag-OFF GC must not commit any WAL record"
    );
}

// Phase 4 Step 7: a registered `freed_pbas_sink` receives the
// `ApplyOutcome::FreePbas.freed_pbas` set produced by the GC cycle's
// internal `WalOp::FreePbas` apply. Verifies the sink fires exactly
// once with a non-empty list and is keyed by the GC'd volume ordinal.
#[test]
fn freed_pbas_sink_receives_lineage_gc_outcomes() {
    use std::sync::Arc;
    use std::sync::Mutex;

    let (_d, db) = mk_db_with_emit_freepbas(true);
    for i in 0u64..16 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    for i in 0u64..16 {
        db.insert(0, i, v((i as u8).wrapping_add(1))).unwrap();
    }
    db.flush().unwrap();

    let captured: Arc<Mutex<Vec<(crate::types::VolumeOrdinal, Vec<crate::types::Pba>)>>> =
        Arc::new(Mutex::new(Vec::new()));
    let captured_clone = captured.clone();
    db.set_freed_pbas_sink(Arc::new(move |vol_ord, pbas| {
        captured_clone.lock().unwrap().push((vol_ord, pbas));
    }));

    let advanced = db.test_run_lineage_gc_cycle().unwrap();
    assert_eq!(advanced, 1);

    let captured = captured.lock().unwrap();
    assert_eq!(
        captured.len(),
        1,
        "sink should fire exactly once per advancing volume, got {captured:?}"
    );
    let (vol_ord, pbas) = &captured[0];
    assert_eq!(*vol_ord, 0);
    assert!(!pbas.is_empty(), "GC cycle freed nothing: {pbas:?}");
}

// Flag OFF: no `WalOp::FreePbas` is committed, so the sink must not
// fire even when registered. Guards against accidentally surfacing
// chain-truncation work as if it were a retire signal.
#[test]
fn freed_pbas_sink_silent_when_flag_off() {
    use std::sync::Arc;
    use std::sync::Mutex;

    let (_d, db) = mk_db_with_emit_freepbas(false);
    for i in 0u64..16 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    for i in 0u64..16 {
        db.insert(0, i, v((i as u8).wrapping_add(1))).unwrap();
    }
    db.flush().unwrap();

    let calls: Arc<Mutex<usize>> = Arc::new(Mutex::new(0));
    let calls_clone = calls.clone();
    db.set_freed_pbas_sink(Arc::new(move |_v, _p| {
        *calls_clone.lock().unwrap() += 1;
    }));

    let advanced = db.test_run_lineage_gc_cycle().unwrap();
    assert_eq!(advanced, 1);
    assert_eq!(*calls.lock().unwrap(), 0);
}

// Once the clone's `parent_vol_ord` is cleared (the Step 5 background
// promotion walker's last act after PromotionComplete), the parent's
// GC must resume — the descendant no longer counts as a pin point.
#[test]
fn phase4_lineage_gc_resumes_after_promotion_clears_parent_vol_ord() {
    use super::BOOTSTRAP_VOLUME_ORD;

    let (_d, db) = mk_db();
    for i in 0u64..16 {
        db.insert(BOOTSTRAP_VOLUME_ORD, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    let clone = db.clone_volume(snap).unwrap();
    db.drop_snapshot(snap).unwrap();
    for i in 0u64..16 {
        db.insert(BOOTSTRAP_VOLUME_ORD, i, v((i as u8).wrapping_add(1)))
            .unwrap();
    }
    db.flush().unwrap();

    // First cycle: descendant pin still active, parent GC blocked.
    assert_eq!(db.test_run_lineage_gc_cycle().unwrap(), 0);

    // Simulate the background promotion walker finishing: clear the
    // descendant's parent edge. Phase 5 wires this through a real
    // `WalOp::PromotionComplete` apply; for Step 2 the test helper
    // mutates manifest + in-memory state directly.
    db.test_clear_parent_vol_ord(clone);

    // Second cycle: parent now has no pinning descendant, so head
    // advance is unblocked.
    let advanced = db.test_run_lineage_gc_cycle().unwrap();
    assert_eq!(advanced, 1);
    let (head_after, tail_after) = dead_list_anchors(&db, BOOTSTRAP_VOLUME_ORD);
    assert_eq!(head_after, NULL_PAGE);
    assert_eq!(tail_after, NULL_PAGE);
}
