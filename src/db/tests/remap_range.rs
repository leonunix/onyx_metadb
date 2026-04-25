use super::*;

/// Build an `L2pValue` whose head 8 bytes encode `pba` (matches the
/// `BlockmapValue` contract used by onyx's apply path). The
/// remaining 20 bytes carry `tag` in byte 8 so tests can
/// distinguish otherwise-identical values that share a pba.
fn remap_val(pba: Pba, tag: u8) -> L2pValue {
    let mut v = [0u8; 28];
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
    // the auto-split path runs. Seed 2 * MAX + 37 entries; expect
    // three WAL records + three final applies.
    let cap = crate::wal::op::MAX_RANGE_DELETE_CAPTURED;
    let total = 2 * cap + 37;
    let (_d, db) = mk_db();
    for i in 0..total {
        remap(&db, i as u64, remap_val(100 + i as u64, 0), None);
    }
    let pre_lsn = db.last_applied_lsn();
    let lsn = db
        .range_delete(BOOTSTRAP_VOLUME_ORD, 0, total as u64)
        .unwrap();
    // Three chunks → three WAL records → LSN bumped by 3.
    assert_eq!(
        lsn,
        pre_lsn + 3,
        "auto-split emitted exactly three WAL records",
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
    let (_d, db) = mk_db_with_shards(8);
    for i in 0..200u64 {
        remap(&db, i, remap_val(1_000 + i, 0), None);
    }
    db.range_delete(BOOTSTRAP_VOLUME_ORD, 0, 200).unwrap();
    for i in 0..200u64 {
        assert_eq!(
            db.get(BOOTSTRAP_VOLUME_ORD, i).unwrap(),
            None,
            "lba={i} should be unmapped after range_delete",
        );
        assert_eq!(db.get_refcount(1_000 + i).unwrap(), 0);
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
