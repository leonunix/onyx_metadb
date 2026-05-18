use super::*;

/// Build an `L2pValue` whose head 8 bytes encode `pba` (matches the
/// `BlockmapValue` contract used by onyx's apply path). The
/// remaining 20 bytes carry `tag` in byte 8 so tests can
/// distinguish otherwise-identical values that share a pba.
fn remap_val(pba: Pba, tag: u8) -> L2pValue {
    let mut v = [0u8; 36];
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
fn l2p_remap_guard_reject_does_not_replay_after_later_refcount_growth() {
    let dir = TempDir::new().unwrap();
    {
        let mut db = Db::create(dir.path()).unwrap();

        // First checkpoint a baseline so the final reopen replays only
        // the guarded reject and later refcount growth.
        remap(&db, 10, remap_val(100, 1), None);
        db = Db::open(dir.path()).unwrap();

        // rc(100)=1, so this guarded remap is rejected live.
        let mut tx = db.begin();
        tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 11, remap_val(100, 2), Some((100, 2)));
        let (_, outcomes) = tx.commit_with_outcomes().unwrap();
        assert!(matches!(
            outcomes.as_slice(),
            [ApplyOutcome::L2pRemap { applied: false, .. }]
        ));

        // Later state would satisfy the old guard. WAL replay must not
        // resurrect the rejected remap at lba=11.
        remap(&db, 12, remap_val(100, 3), None);
        assert_eq!(db.get_refcount(100).unwrap(), 2);

        // Audits call iter_refcounts(); that must checkpoint through
        // Db::flush() rather than persisting refcount pages ahead of
        // the manifest WAL checkpoint.
        let refs: Vec<_> = db
            .iter_refcounts()
            .unwrap()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        assert!(refs.contains(&(100, 2)));
    }

    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get_refcount(100).unwrap(), 2);
    assert_eq!(
        db.get(BOOTSTRAP_VOLUME_ORD, 10).unwrap(),
        Some(remap_val(100, 1))
    );
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 11).unwrap(), None);
    assert_eq!(
        db.get(BOOTSTRAP_VOLUME_ORD, 12).unwrap(),
        Some(remap_val(100, 3))
    );
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

fn seed_distinct_remaps_batched(db: &Db, total: usize) {
    const OPS_PER_COMMIT: usize = 4096;
    // Group LBAs into 32-LBA "compression units" so each leaf (128
    // entries) only references 4 distinct units — well under v2's
    // MAX_UNITS_PER_LEAF = 100. The test only needs `total` distinct
    // mappings, not distinct PBAs.
    const LBAS_PER_UNIT: u64 = 32;
    for chunk_start in (0..total).step_by(OPS_PER_COMMIT) {
        let chunk_end = (chunk_start + OPS_PER_COMMIT).min(total);
        let mut tx = db.begin();
        for i in chunk_start..chunk_end {
            tx.l2p_remap(
                BOOTSTRAP_VOLUME_ORD,
                i as u64,
                remap_val(100 + (i as u64 / LBAS_PER_UNIT), 0),
                None,
            );
        }
        tx.commit().unwrap();
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
    // the auto-split path runs. Seed MAX + 37 entries; expect two WAL
    // records + two final applies. Use batched seed commits so this
    // regression test exercises range_delete instead of spending most
    // of its time on single-op WAL round trips.
    let cap = crate::wal::op::MAX_RANGE_DELETE_CAPTURED;
    let total = cap + 37;
    let (_d, db) = mk_db();
    seed_distinct_remaps_batched(&db, total);
    let pre_lsn = db.last_applied_lsn();
    let lsn = db
        .range_delete(BOOTSTRAP_VOLUME_ORD, 0, total as u64)
        .unwrap();
    // Two chunks → two WAL records → LSN bumped by 2.
    assert_eq!(
        lsn,
        pre_lsn + 2,
        "auto-split emitted exactly two WAL records",
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
    //
    // v2 caps `MAX_UNITS_PER_LEAF` at 100, so we share PBA across
    // 32-LBA chunks to keep ≤ 4 distinct units per 128-LBA leaf.
    // The shard-cross invariant we care about is "every LBA gets
    // unmapped and all refcounts settle" — the PBA grouping is
    // orthogonal to that.
    let (_d, db) = mk_db_with_shards(8);
    for i in 0..200u64 {
        remap(&db, i, remap_val(1_000 + (i / 32), 0), None);
    }
    db.range_delete(BOOTSTRAP_VOLUME_ORD, 0, 200).unwrap();
    for i in 0..200u64 {
        assert_eq!(
            db.get(BOOTSTRAP_VOLUME_ORD, i).unwrap(),
            None,
            "lba={i} should be unmapped after range_delete",
        );
    }
    // Refcounts for the (potentially shared) PBAs all collapse to 0.
    for unit in 0..((200 + 31) / 32) {
        assert_eq!(db.get_refcount(1_000 + unit).unwrap(), 0);
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

// ---------- WalOp::L2pRemapRange (range-shaped remap) ----------

fn remap_range(
    db: &Db,
    start_lba: Lba,
    values: Vec<L2pValue>,
) -> (Box<[bool]>, Box<[Option<L2pValue>]>, Vec<Pba>) {
    let mut tx = db.begin();
    tx.l2p_remap_range(BOOTSTRAP_VOLUME_ORD, start_lba, values.into_boxed_slice());
    let (_, outcomes) = tx.commit_with_outcomes().unwrap();
    assert_eq!(outcomes.len(), 1, "one range op in, one outcome out");
    match outcomes.into_iter().next().unwrap() {
        ApplyOutcome::L2pRemapRange {
            applied,
            prevs,
            freed_pbas,
        } => (applied, prevs, freed_pbas),
        other => panic!("expected L2pRemapRange outcome, got {other:?}"),
    }
}

#[test]
fn l2p_remap_range_writes_each_lba_and_increfs_distinct_pbas() {
    let (_d, db) = mk_db();
    let values = (0..4u8).map(|i| remap_val(100 + i as u64, i)).collect();
    let (applied, prevs, freed) = remap_range(&db, 10, values);
    assert_eq!(applied.len(), 4);
    assert!(applied.iter().all(|a| *a));
    assert!(prevs.iter().all(|p| p.is_none()));
    assert!(freed.is_empty());
    for i in 0..4u8 {
        let want = remap_val(100 + i as u64, i);
        assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 10 + i as u64).unwrap(), Some(want));
        assert_eq!(db.get_refcount(100 + i as u64).unwrap(), 1);
    }
}

#[test]
fn l2p_remap_range_overwrite_collects_freed_pbas() {
    let (_d, db) = mk_db();
    // Seed each LBA with a distinct pba (refcount=1 each).
    let first: Vec<_> = (0..4u8).map(|i| remap_val(100 + i as u64, 1)).collect();
    let _ = remap_range(&db, 10, first.clone());
    for i in 0..4u8 {
        assert_eq!(db.get_refcount(100 + i as u64).unwrap(), 1);
    }

    // Overwrite the same 4 LBAs with new distinct pbas. Each old
    // pba's refcount should drop to 0 and be reported in freed_pbas.
    let second: Vec<_> = (0..4u8).map(|i| remap_val(200 + i as u64, 2)).collect();
    let (applied, prevs, freed) = remap_range(&db, 10, second);
    assert!(applied.iter().all(|a| *a));
    assert_eq!(prevs.len(), 4);
    for (i, prev) in prevs.iter().enumerate() {
        assert_eq!(*prev, Some(first[i]));
    }
    let mut freed_sorted = freed.clone();
    freed_sorted.sort();
    assert_eq!(freed_sorted, vec![100, 101, 102, 103]);
    for i in 0..4u8 {
        assert_eq!(db.get_refcount(100 + i as u64).unwrap(), 0);
        assert_eq!(db.get_refcount(200 + i as u64).unwrap(), 1);
    }
}

#[test]
fn l2p_remap_range_crosses_l2p_shard_boundary() {
    // LEAF_SHIFT=7 → 128 LBAs per leaf, and shard_for_key_l2p hashes
    // leaf_idx. Starting at LBA 120, a count-32 range walks across
    // leaf 0 (LBAs [120..128)) and leaf 1 (LBAs [128..152)) — which
    // typically routes to two different shards. The range op must
    // handle both shards.
    let (_d, db) = mk_db();
    let values: Vec<_> = (0..32u8).map(|i| remap_val(500 + i as u64, i)).collect();
    let (applied, prevs, freed) = remap_range(&db, 120, values);
    assert!(applied.iter().all(|a| *a));
    assert!(prevs.iter().all(|p| p.is_none()));
    assert!(freed.is_empty());
    for i in 0..32u8 {
        let want = remap_val(500 + i as u64, i);
        assert_eq!(
            db.get(BOOTSTRAP_VOLUME_ORD, 120 + i as u64).unwrap(),
            Some(want)
        );
        assert_eq!(db.get_refcount(500 + i as u64).unwrap(), 1);
    }
}

#[test]
fn l2p_remap_range_snapshot_pin_suppresses_decref_per_lba() {
    let (_d, db) = mk_db();
    // Seed LBAs 10..14 each with distinct pbas.
    let seed: Vec<_> = (0..4u8).map(|i| remap_val(700 + i as u64, 1)).collect();
    let _ = remap_range(&db, 10, seed.clone());
    let snap = db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    // Now remap all 4 to a single new pba (range with shared head_pba
    // in the new values is legal). Snapshot pins each old pba → no
    // freed_pbas; new pba refcount counts all 4.
    let new_values: Vec<_> = (0..4).map(|i| remap_val(900, 10 + i)).collect();
    let (_applied, _prevs, freed) = remap_range(&db, 10, new_values);
    assert!(
        freed.is_empty(),
        "snapshot pins old pbas; decref must be suppressed per LBA"
    );
    for i in 0..4u8 {
        assert_eq!(db.get_refcount(700 + i as u64).unwrap(), 1);
    }
    assert_eq!(db.get_refcount(900).unwrap(), 4);
    // Drop snapshot → old pbas can be reclaimed.
    db.drop_snapshot(snap).unwrap();
    for i in 0..4u8 {
        assert_eq!(db.get_refcount(700 + i as u64).unwrap(), 0);
    }
}

#[test]
fn l2p_remap_range_stale_seq_per_lba_rejection() {
    // The seq_guard runs per LBA. If one LBA's new_value has a stale
    // seq it gets rejected (applied[i] = false, prev[i] = the current
    // value) while sibling LBAs in the same range apply normally.
    let (_d, db) = mk_db();
    // First write: seq=10 for all 4 LBAs.
    let fresh: Vec<_> = (0..4)
        .map(|i| {
            let mut v = remap_val(100 + i as u64, 1).0;
            // L2P_SEQ_OFFSET is the trailing 8 B (bytes 28..36) per
            // SPEC §3.1; matches L2pValue::seq().
            v[28..36].copy_from_slice(&10u64.to_be_bytes());
            L2pValue(v)
        })
        .collect();
    let _ = remap_range(&db, 10, fresh);
    // Second write: seq=5 on LBA 1, seq=20 on the rest.
    let mixed: Vec<_> = (0..4)
        .map(|i| {
            let mut v = remap_val(200 + i as u64, 2).0;
            let seq: u64 = if i == 1 { 5 } else { 20 };
            v[28..36].copy_from_slice(&seq.to_be_bytes());
            L2pValue(v)
        })
        .collect();
    let (applied, _prevs, _freed) = remap_range(&db, 10, mixed);
    assert_eq!(applied[0], true, "fresh seq accepted on LBA 10");
    assert_eq!(applied[1], false, "stale seq rejected on LBA 11");
    assert_eq!(applied[2], true);
    assert_eq!(applied[3], true);
}
