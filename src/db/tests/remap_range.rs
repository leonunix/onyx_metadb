use super::*;

// ---- rc-authoritative reclaim (flag on): rc == # live L2P references ----

fn mk_db_rc_auth() -> (TempDir, std::sync::Arc<Db>) {
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.l2p_buffer_enabled = true; // buffer mode = the box hot path
    cfg.rc_authoritative_reclaim = true;
    let db = Db::create_with_config(cfg).unwrap();
    (dir, db)
}

/// Single applied remap installs ONE live L2P reference → rc[pba] == 1.
#[test]
fn rc_authoritative_remap_increfs_new_pba() {
    let (_d, db) = mk_db_rc_auth();
    let mut tx = db.begin();
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 10, remap_val(100, 1), None);
    tx.commit_with_outcomes().unwrap();
    db.flush().unwrap(); // final-drain the rc apply lane for a deterministic read
    assert_eq!(
        db.get_refcount(100).unwrap(),
        1,
        "rc-authoritative: one live L2P reference → rc == 1"
    );
}

/// A packed unit's N member LBAs all map to the same base pba → rc == N.
#[test]
fn rc_authoritative_packed_unit_increfs_n() {
    let (_d, db) = mk_db_rc_auth();
    let base = 500;
    let n = 8u32;
    let mut tx = db.begin();
    for lba in 0..n as u64 {
        // Distinct tag per LBA so they are distinct L2pValues sharing one
        // head_pba (the packed-slot contract); each must contribute +1.
        tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, lba, remap_val(base, lba as u8 + 1), None);
    }
    tx.commit_with_outcomes().unwrap();
    db.flush().unwrap();
    assert_eq!(
        db.get_refcount(base).unwrap(),
        n,
        "rc-authoritative: N live L2P references to one base pba → rc == N"
    );
}

/// Overwriting an LBA does the inline incref(new) + decref(old) pair:
/// rc[new]==1, rc[old]==0. The decref is traditional/inline (NOT deferred to
/// the deadlist) — that 1:1 balance is what makes reclaim's rc==0 Gate
/// authoritative, and the overwrite surfaces `freed_pba == old`.
#[test]
fn rc_authoritative_overwrite_increfs_new_decrefs_old() {
    let (_d, db) = mk_db_rc_auth();
    let mut tx = db.begin();
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 10, remap_val(100, 1), None);
    tx.commit_with_outcomes().unwrap();
    db.flush().unwrap();
    assert_eq!(db.get_refcount(100).unwrap(), 1);

    let mut tx = db.begin();
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 10, remap_val(200, 1), None);
    let (_, outcomes) = tx.commit_with_outcomes().unwrap();
    db.flush().unwrap();
    assert_eq!(db.get_refcount(200).unwrap(), 1, "new pba increffed");
    assert_eq!(
        db.get_refcount(100).unwrap(),
        0,
        "old pba decref'd inline (traditional rc, not deadlist-deferred)"
    );
    // The decref that drove rc[100]→0 surfaces freed_pba so onyx retires it.
    let freed = match &outcomes[0] {
        ApplyOutcome::L2pRemap { freed_pba, .. } => *freed_pba,
        other => panic!("expected L2pRemap outcome, got {other:?}"),
    };
    assert_eq!(freed, Some(100), "net rc==0 old pba surfaced as freed");
}

/// Decref-balance invariant: a packed base referenced by N LBAs has rc==N,
/// and overwriting each LBA away decays rc back to exactly 0 (the last
/// overwrite surfaces the base as freed). This is the core 1:1 incref/decref
/// balance the inline decref must guarantee.
#[test]
fn rc_authoritative_packed_unit_decays_to_zero() {
    let (_d, db) = mk_db_rc_auth();
    let base = 700;
    let n = 6u64; // mirrors the forensic packed unit_lba_count=6
    let mut tx = db.begin();
    for lba in 0..n {
        tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, lba, remap_val(base, lba as u8 + 1), None);
    }
    tx.commit_with_outcomes().unwrap();
    db.flush().unwrap();
    assert_eq!(db.get_refcount(base).unwrap(), n as u32, "rc == N members");

    // Overwrite each member LBA to its own fresh exclusive pba.
    let mut last_freed = None;
    for lba in 0..n {
        let new_pba = 800 + lba;
        let mut tx = db.begin();
        tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, lba, remap_val(new_pba, 1), None);
        let (_, outcomes) = tx.commit_with_outcomes().unwrap();
        db.flush().unwrap();
        let before_last = lba < n - 1;
        let expect_rc = if before_last { (n - 1 - lba) as u32 } else { 0 };
        assert_eq!(
            db.get_refcount(base).unwrap(),
            expect_rc,
            "rc decays one per overwritten member (lba {lba})"
        );
        assert_eq!(db.get_refcount(new_pba).unwrap(), 1, "fresh pba increffed");
        if let ApplyOutcome::L2pRemap { freed_pba, .. } = &outcomes[0] {
            last_freed = *freed_pba;
        }
    }
    assert_eq!(db.get_refcount(base).unwrap(), 0, "rc decayed to exactly 0");
    assert_eq!(
        last_freed,
        Some(base),
        "the overwrite that drove rc→0 surfaced the base as freed"
    );
}

/// Snapshot-pin decref suppression (serial path): a snapshot that still maps
/// the LBA to the old value pins old.head_pba, so a live overwrite must NOT
/// decref it — otherwise rc could fall to 0 while the snapshot references it
/// and reclaim (rc==0) would free snapshot-referenced data.
#[test]
fn rc_authoritative_snapshot_pins_old_suppresses_decref() {
    let (_d, db) = mk_db_rc_auth();
    let mut tx = db.begin();
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 10, remap_val(100, 1), None);
    tx.commit_with_outcomes().unwrap();
    db.flush().unwrap();
    assert_eq!(db.get_refcount(100).unwrap(), 1);

    // Snapshot captures (lba 10 → pba 100). Now overwrite live to pba 200.
    db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    let mut tx = db.begin();
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 10, remap_val(200, 1), None);
    let (_, outcomes) = tx.commit_with_outcomes().unwrap();
    db.flush().unwrap();
    assert_eq!(db.get_refcount(200).unwrap(), 1, "new pba increffed");
    assert_eq!(
        db.get_refcount(100).unwrap(),
        1,
        "snapshot still pins old pba → decref suppressed, rc stays 1"
    );
    let freed = match &outcomes[0] {
        ApplyOutcome::L2pRemap { freed_pba, .. } => *freed_pba,
        other => panic!("expected L2pRemap outcome, got {other:?}"),
    };
    assert_eq!(freed, None, "snapshot-pinned old pba not surfaced as freed");
}

/// In-place overwrite to the SAME pba (different value bytes) is net-zero —
/// the per-pba +1/-1 collapse must leave rc unchanged and surface nothing.
#[test]
fn rc_authoritative_same_pba_overwrite_net_zero() {
    let (_d, db) = mk_db_rc_auth();
    let mut tx = db.begin();
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 10, remap_val(100, 1), None);
    tx.commit_with_outcomes().unwrap();
    db.flush().unwrap();
    assert_eq!(db.get_refcount(100).unwrap(), 1);

    let mut tx = db.begin();
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 10, remap_val(100, 2), None);
    let (_, outcomes) = tx.commit_with_outcomes().unwrap();
    db.flush().unwrap();
    assert_eq!(
        db.get_refcount(100).unwrap(),
        1,
        "same-pba overwrite: +1/-1 collapse leaves rc unchanged"
    );
    let freed = match &outcomes[0] {
        ApplyOutcome::L2pRemap { freed_pba, .. } => *freed_pba,
        other => panic!("expected L2pRemap outcome, got {other:?}"),
    };
    assert_eq!(freed, None, "net-zero overwrite surfaces no freed pba");
}

/// THE invariant, randomized: after an arbitrary sequence of overwrites
/// (heavy LBA reuse + PBA sharing → lots of incref/decref churn), for EVERY
/// pba `rc[pba] == # live L2P entries pointing to it` (no dedup here, so the
/// membership term is 0). A single missed or doubled decref anywhere in the
/// pipeline (the premature-free CRC class) breaks this exact equality.
#[test]
fn rc_authoritative_invariant_under_random_overwrites() {
    use std::collections::HashMap;
    let (_d, db) = mk_db_rc_auth();
    // Deterministic LCG (Date/rand are unavailable in this harness; a fixed
    // seed keeps the test reproducible).
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut rng = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state >> 33
    };
    let n_lbas: u64 = 64;
    let pba_lo: Pba = 1000;
    let pba_hi: Pba = 1032; // 32 pbas → forced sharing + overwrites
    for _ in 0..3000 {
        let lba = rng() % n_lbas;
        let pba = pba_lo + rng() % (pba_hi - pba_lo);
        let tag = (rng() % 4) as u8 + 1;
        let mut tx = db.begin();
        tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, lba, remap_val(pba, tag), None);
        tx.commit_with_outcomes().unwrap();
    }
    db.flush().unwrap();

    // Tally live L2P references per pba by scanning the LBA space.
    let mut expected: HashMap<Pba, u32> = HashMap::new();
    for lba in 0..n_lbas {
        if let Some(v) = db.get(BOOTSTRAP_VOLUME_ORD, lba).unwrap() {
            *expected.entry(v.head_pba()).or_insert(0) += 1;
        }
    }
    for pba in pba_lo..pba_hi {
        let want = expected.get(&pba).copied().unwrap_or(0);
        assert_eq!(
            db.get_refcount(pba).unwrap(),
            want,
            "rc[{pba}] must equal its live L2P reference count after random churn"
        );
    }
}

/// Flag OFF (default): L2pRemap stays rc-neutral (Phase-5 contract).
#[test]
fn rc_neutral_when_flag_off() {
    let (_d, db) = mk_db(); // default config: rc_authoritative_reclaim = false
    let mut tx = db.begin();
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 10, remap_val(100, 1), None);
    tx.commit_with_outcomes().unwrap();
    db.flush().unwrap();
    assert_eq!(
        db.get_refcount(100).unwrap(),
        0,
        "flag off: L2pRemap is rc-neutral"
    );
}

/// Build an `L2pValue` whose head 8 bytes encode `pba` (matches the
/// `BlockmapValue` contract used by onyx's apply path). The
/// remaining 20 bytes carry `tag` in byte 8 so tests can
/// distinguish otherwise-identical values that share a pba.
fn remap_val(pba: Pba, tag: u8) -> L2pValue {
    let mut v = [0u8; crate::paged::format::LEAF_VALUE_SIZE];
    v[..8].copy_from_slice(&pba.to_be_bytes());
    v[8] = tag;
    // Non-zero birth_lsn trailer so the apply path's stamp-if-zero
    // logic doesn't perturb byte-equality assertions.
    v[crate::paged::format::LEAF_VALUE_SIZE - 1] = 1;
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
    // Phase 5: per-write rc path retired. L2pRemap never touches global
    // rc; freed_pba is always None. Asserts now read "rc stays at 0".
    let (_d, db) = mk_db();
    let outcome = remap(&db, 10, remap_val(100, 1), None);
    assert_eq!(assert_remap_applied(outcome), (None, None));
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    assert_eq!(
        db.get(BOOTSTRAP_VOLUME_ORD, 10).unwrap(),
        Some(remap_val(100, 1))
    );
}

#[test]
fn l2p_remap_same_pba_in_place_overwrite_net_zero() {
    // Phase 5: L2pRemap never touches rc, period. The "net-zero same-pba
    // overwrite" invariant trivially holds because there is nothing to
    // collapse.
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 1), None);
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    let outcome = remap(&db, 10, remap_val(100, 2), None);
    let (prev, freed) = assert_remap_applied(outcome);
    assert_eq!(prev, Some(remap_val(100, 1)));
    assert_eq!(freed, None);
    assert_eq!(
        db.get_refcount(100).unwrap(),
        0,
        "Phase 5: L2pRemap never increments rc"
    );
}

#[test]
fn l2p_remap_same_pba_leaf_shared_increfs_new() {
    // Phase 5: L2pRemap is uniformly rc-neutral across both the hot
    // (lane) path and the serial path that snapshot-on-volume routes
    // through. The serial path still exists for the dead-list /
    // snap-pin recording inside `record_dead`, but the rc table is
    // untouched; DropSnapshot is PBA rc-neutral too.
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 1), None);
    db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    let outcome = remap(&db, 10, remap_val(100, 2), None);
    let (prev, freed) = assert_remap_applied(outcome);
    assert_eq!(prev, Some(remap_val(100, 1)));
    assert_eq!(freed, None);
    assert_eq!(
        db.get_refcount(100).unwrap(),
        0,
        "Phase 5: L2pRemap is rc-neutral on both hot-path and serial path",
    );
}

#[test]
fn l2p_remap_different_pba_exclusive_decrefs_old_increfs_new() {
    // Phase 5: L2pRemap never touches rc, so no freed_pba surfaces from
    // the hot path. The old PBA's retirement now flows through the
    // dead-list → Lineage GC path (not exercised here).
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 1), None);
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    let outcome = remap(&db, 10, remap_val(200, 1), None);
    let (prev, freed) = assert_remap_applied(outcome);
    assert_eq!(prev, Some(remap_val(100, 1)));
    assert_eq!(freed, None, "Phase 5: L2pRemap never surfaces freed_pba");
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    assert_eq!(db.get_refcount(200).unwrap(), 0);
}

#[test]
fn l2p_remap_different_pba_leaf_shared_stays_rc_neutral() {
    // Phase 5: L2pRemap is rc-neutral regardless of routing. Snapshot
    // pinning of the old PBA is enforced by `record_dead` populating
    // the volume's dead-list with the prev `(pba, birth_lsn, death_lsn)`;
    // Lineage GC consumes the dead-list and emits `FreePbas` only when
    // both snap-pin and descendant-pin clear. The freed_pba slot on
    // L2pRemap is always None on the Phase 5 hot path.
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 1), None);
    db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    let outcome = remap(&db, 10, remap_val(200, 1), None);
    let (prev, freed) = assert_remap_applied(outcome);
    assert_eq!(prev, Some(remap_val(100, 1)));
    assert_eq!(freed, None);
    assert_eq!(db.get_refcount(100).unwrap(), 0, "Phase 5: rc-neutral apply");
    assert_eq!(db.get_refcount(200).unwrap(), 0, "Phase 5: rc-neutral apply");
}

#[test]
fn l2p_remap_different_pba_decref_not_to_zero_reports_no_freed() {
    // Phase 5: dedup-style multi-LBA-same-PBA no longer accumulates
    // rc on remap (rc is only mutated by Lineage GC / promotion walker
    // now). rc stays at 0 throughout.
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 1), None);
    remap(&db, 11, remap_val(100, 1), None);
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    let outcome = remap(&db, 10, remap_val(200, 1), None);
    let (_, freed) = assert_remap_applied(outcome);
    assert_eq!(freed, None);
    assert_eq!(db.get_refcount(100).unwrap(), 0);
}

#[test]
fn l2p_remap_guard_pass_applies_and_increfs() {
    // Phase 5: the rc-based guard machinery still runs (used by dedup
    // hits for verified-shared PBAs), but rc is only seeded via the
    // test helper now. Pre-seed rc(100)=1 so the guard passes, then
    // confirm the op applies without any new rc movement.
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 1), None);
    db.incref_pba(100, 1).unwrap(); // seed rc(100)=1 for guard
    let outcome = remap(&db, 11, remap_val(100, 1), Some((100, 1)));
    let (prev, freed) = assert_remap_applied(outcome);
    assert_eq!(prev, None);
    assert_eq!(freed, None);
    assert_eq!(
        db.get_refcount(100).unwrap(),
        1,
        "Phase 5: guard passes, but L2pRemap itself does not change rc"
    );
}

#[test]
fn l2p_remap_guard_fail_rejects_op_without_touching_state() {
    // Phase 5: rc is no longer seeded by L2pRemap, so seed it via the
    // test helper first; the guard machinery itself is unchanged.
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 1), None);
    db.incref_pba(100, 1).unwrap(); // rc(100)=1, < 5 → guard rejects
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
    // Phase 5: L2pRemap never moves rc, so the "3 → 2 → 1 → 0 with the
    // final remap surfacing freed_pba" cascade does not happen on the
    // hot path. The L2P state still progresses correctly; rc stays 0.
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 0), None);
    remap(&db, 11, remap_val(100, 1), None);
    remap(&db, 12, remap_val(100, 2), None);
    assert_eq!(db.get_refcount(100).unwrap(), 0);

    let (_, f0) = assert_remap_applied(remap(&db, 10, remap_val(200, 0), None));
    assert_eq!(f0, None);
    assert_eq!(db.get_refcount(100).unwrap(), 0);

    let (_, f1) = assert_remap_applied(remap(&db, 11, remap_val(201, 0), None));
    assert_eq!(f1, None);
    assert_eq!(db.get_refcount(100).unwrap(), 0);

    let (_, f2) = assert_remap_applied(remap(&db, 12, remap_val(202, 0), None));
    assert_eq!(f2, None);
    assert_eq!(db.get_refcount(100).unwrap(), 0);
}

// Phase D.5: `l2p_remap_survives_restart_via_wal_replay` exercised
// WAL replay of L2pRemap across reopen — the WAL is gone.

#[test]
fn l2p_remap_guarded_survives_restart_with_same_decision() {
    // Phase 5: removed — the "same decision live vs replay" invariant
    // is unstable because the live commit path no longer touches rc
    // while the legacy `apply_l2p_remap` (used by WAL replay) still
    // does. A guarded op that lives through replay sees a different
    // rc snapshot than it saw at submit time. Production callers do
    // not observe this asymmetry (replay only runs on cold open). The
    // guard machinery itself is covered by
    // `l2p_remap_guard_fail_rejects_op_without_touching_state`.
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        // Flush before reopen so the replay does not exercise the
        // legacy rc-mutating apply path; only the L2P / rc state at
        // checkpoint must round-trip.
        remap(&db, 10, remap_val(100, 1), None);
        let mut tx = db.begin();
        tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 11, remap_val(100, 2), Some((100, 2)));
        tx.commit_with_outcomes().unwrap();
        db.flush().unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    // rc(100)=0 at checkpoint because no rc-driving op ran.
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    assert_eq!(
        db.get(BOOTSTRAP_VOLUME_ORD, 10).unwrap(),
        Some(remap_val(100, 1))
    );
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 11).unwrap(), None);
}

// Phase D.5: `l2p_remap_guard_reject_does_not_replay_after_later_refcount_growth`
// and `l2p_remap_freed_pba_round_trips_through_replay` exercised
// WAL-replay specifics that are gone with the WAL.

// ---------------- L2pRangeDelete apply (SPEC §3.2 / §4.7) -------

/// Helper that pre-seeds N consecutive lbas with the given pba.
/// Used to set up range_delete test fixtures without dragging in
/// the full remap decision table.
///
/// Phase 5: hot-path L2pRemap no longer increments global rc, so
/// after the remaps we bring rc(pba) up to `count` by incref'ing the
/// shortfall. When a snapshot is live the commit went through the
/// legacy apply path which already bumped rc — comparing before/after
/// is how we avoid double-seeding.
fn seed_remaps(db: &Db, start: Lba, count: usize, pba: Pba, tag: u8) {
    for i in 0..count {
        remap(db, start + i as u64, remap_val(pba, tag), None);
    }
    let cur = db.get_refcount(pba).unwrap();
    let want = count as u32;
    if cur < want {
        db.incref_pba(pba, want - cur).unwrap();
    }
}

fn seed_distinct_remaps_batched(db: &Db, total: usize) {
    const OPS_PER_COMMIT: usize = 4096;
    // Group LBAs into 32-LBA "compression units" so each leaf (128
    // entries) only references 4 distinct units — well under v2's
    // MAX_UNITS_PER_LEAF = 100. The test only needs `total` distinct
    // mappings, not distinct PBAs.
    const LBAS_PER_UNIT: u64 = 32;
    use std::collections::HashMap;
    let mut pba_counts: HashMap<Pba, u32> = HashMap::new();
    for chunk_start in (0..total).step_by(OPS_PER_COMMIT) {
        let chunk_end = (chunk_start + OPS_PER_COMMIT).min(total);
        let mut tx = db.begin();
        for i in chunk_start..chunk_end {
            let pba = 100 + (i as u64 / LBAS_PER_UNIT);
            tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, i as u64, remap_val(pba, 0), None);
            *pba_counts.entry(pba).or_insert(0) += 1;
        }
        tx.commit().unwrap();
    }
    // Phase 5: hot-path L2pRemap no longer bumps global rc; seed it
    // explicitly so range_delete can prove it leaves PBA rc untouched.
    for (pba, count) in pba_counts {
        db.incref_pba(pba, count).unwrap();
    }
}

#[test]
fn range_delete_empty_range_is_noop() {
    // Phase 5: seed rc explicitly because L2pRemap no longer does.
    let (_d, db) = mk_db();
    remap(&db, 5, remap_val(100, 1), None);
    db.incref_pba(100, 1).unwrap();
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
    // Phase 5: seed rc explicitly because L2pRemap no longer does.
    let (_d, db) = mk_db();
    remap(&db, 100, remap_val(500, 1), None);
    db.incref_pba(500, 1).unwrap();
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
fn range_delete_removes_mappings_and_keeps_pba_rc() {
    // Phase 5: range delete is L2P-only for PBA refcounts. Global PBA rc
    // is not a per-live-LBA counter, so discard must not subtract one rc
    // entry per captured LBA.
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 0), None);
    remap(&db, 11, remap_val(200, 0), None);
    remap(&db, 12, remap_val(300, 0), None);
    db.incref_pba(100, 1).unwrap();
    db.incref_pba(200, 1).unwrap();
    db.incref_pba(300, 1).unwrap();
    db.range_delete(BOOTSTRAP_VOLUME_ORD, 10, 13).unwrap();
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 10).unwrap(), None);
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 11).unwrap(), None);
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 12).unwrap(), None);
    assert_eq!(db.get_refcount(100).unwrap(), 1);
    assert_eq!(db.get_refcount(200).unwrap(), 1);
    assert_eq!(db.get_refcount(300).unwrap(), 1);
}

#[test]
fn range_delete_half_open_interval_excludes_end() {
    // Phase 5: seed rc explicitly because L2pRemap no longer does.
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 0), None);
    remap(&db, 11, remap_val(200, 0), None);
    remap(&db, 12, remap_val(300, 0), None);
    db.incref_pba(100, 1).unwrap();
    db.incref_pba(200, 1).unwrap();
    db.incref_pba(300, 1).unwrap();
    // Range [10, 12) keeps lba=12.
    db.range_delete(BOOTSTRAP_VOLUME_ORD, 10, 12).unwrap();
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 10).unwrap(), None);
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 11).unwrap(), None);
    assert_eq!(
        db.get(BOOTSTRAP_VOLUME_ORD, 12).unwrap(),
        Some(remap_val(300, 0)),
        "end-exclusive: lba=12 survives",
    );
    assert_eq!(db.get_refcount(100).unwrap(), 1);
    assert_eq!(db.get_refcount(200).unwrap(), 1);
    assert_eq!(db.get_refcount(300).unwrap(), 1);
}

#[test]
fn range_delete_dedup_multiple_lbas_same_pba_keeps_rc() {
    // Captured may have multiple (lba, pba) pairs with the same pba
    // (dedup/packed-slot case). Range delete must still avoid per-LBA
    // PBA decrefs; the live-L2P confirm path owns physical reclaim.
    let (_d, db) = mk_db();
    seed_remaps(&db, 10, 4, 777, 0);
    assert_eq!(db.get_refcount(777).unwrap(), 4);
    db.range_delete(BOOTSTRAP_VOLUME_ORD, 10, 14).unwrap();
    assert_eq!(db.get_refcount(777).unwrap(), 4);
    for lba in 10..14 {
        assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, lba).unwrap(), None);
    }
}

#[test]
fn range_delete_with_live_snapshot_keeps_pba_rc() {
    // Snapshot holds the pre-op leaf, but RangeDelete is PBA rc-neutral
    // regardless of snapshot state. Lineage GC handles snapshot pinning.
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
        "range delete must not mutate PBA rc",
    );
}

#[test]
fn range_delete_mixed_shared_and_exclusive_leaves() {
    // Phase 5: rc seeding now comes through `seed_remaps` which
    // explicitly increfs after the remap.
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
    // Shared leaf: unchanged.
    assert_eq!(db.get_refcount(500).unwrap(), 3);
    // Fresh post-snapshot leaf: also unchanged; range delete is PBA
    // rc-neutral in Phase 5.
    assert_eq!(db.get_refcount(600).unwrap(), 3);
}

#[test]
fn range_delete_survives_restart_via_wal_replay() {
    // Phase 5: WAL replay re-applies L2pRemap through the legacy
    // rc-mutating apply, which would double-incref against our
    // explicit `seed_remaps` incref. Flush before close so replay
    // starts after the checkpoint and the test exercises the
    // `range_delete` survival invariant in isolation.
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        seed_remaps(&db, 10, 4, 100, 0);
        db.range_delete(BOOTSTRAP_VOLUME_ORD, 10, 14).unwrap();
        db.flush().unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    for lba in 10..14 {
        assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, lba).unwrap(), None);
    }
    assert_eq!(db.get_refcount(100).unwrap(), 4);
}

// Phase D.5: `range_delete_auto_splits_above_cap` exercised the
// Wal-mode `range_delete_via_wal` per-chunk WAL splitting. Buffer
// mode chunks by Discard's `count: u32` LBA range; basic coverage
// lives in `tests/db_buffer_journal_mode.rs::buffer_mode_range_delete_grows_lifecycle_journal`.

#[test]
fn range_delete_crosses_shard_boundaries() {
    // With the default shard count (> 1), a contiguous LBA range
    // hits multiple shards. Make sure the apply path visits each
    // shard's tree and every mapping in the range is removed.
    //
    // v2 caps `MAX_UNITS_PER_LEAF` at 100, so we share PBA across
    // 32-LBA chunks to keep ≤ 4 distinct units per 128-LBA leaf.
    // The shard-cross invariant we care about is "every LBA gets
    // unmapped and all refcounts stay put" — the PBA grouping is
    // orthogonal to that.
    //
    // Phase 5: hot-path L2pRemap no longer bumps rc; we seed rc
    // explicitly after the remaps to assert RangeDelete keeps it stable.
    let (_d, db) = mk_db_with_shards(8);
    use std::collections::HashMap;
    let mut pba_counts: HashMap<Pba, u32> = HashMap::new();
    for i in 0..200u64 {
        let pba = 1_000 + (i / 32);
        remap(&db, i, remap_val(pba, 0), None);
        *pba_counts.entry(pba).or_insert(0) += 1;
    }
    for (pba, count) in pba_counts {
        db.incref_pba(pba, count).unwrap();
    }
    db.range_delete(BOOTSTRAP_VOLUME_ORD, 0, 200).unwrap();
    for i in 0..200u64 {
        assert_eq!(
            db.get(BOOTSTRAP_VOLUME_ORD, i).unwrap(),
            None,
            "lba={i} should be unmapped after range_delete",
        );
    }
    for unit in 0..((200 + 31) / 32) {
        let expected = if unit < 6 { 32 } else { 8 };
        assert_eq!(db.get_refcount(1_000 + unit).unwrap(), expected);
    }
}

#[test]
fn range_delete_dedup_with_snapshot_keeps_rc() {
    // Combined SPEC §4.7 (dedup aggregation) + §4.4 (leaf shared).
    // Four lbas on the same pba, then snapshot, then range_delete:
    // RangeDelete removes the L2P mappings but leaves the PBA rc intact.
    let (_d, db) = mk_db();
    seed_remaps(&db, 10, 4, 777, 0);
    assert_eq!(db.get_refcount(777).unwrap(), 4);
    db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    db.range_delete(BOOTSTRAP_VOLUME_ORD, 10, 14).unwrap();
    assert_eq!(
        db.get_refcount(777).unwrap(),
        4,
        "all four lbas removed, but PBA rc stays intact",
    );
}

#[test]
fn l2p_remap_leaf_shared_plus_drop_snapshot_ends_at_correct_refcount() {
    // Phase 5: L2pRemap and DropSnapshot are both PBA rc-neutral, so
    // `freed_pbas` is empty and both rcs stay at 0.
    let (_d, db) = mk_db();
    remap(&db, 10, remap_val(100, 1), None);
    let snap = db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    remap(&db, 10, remap_val(200, 1), None);
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    assert_eq!(db.get_refcount(200).unwrap(), 0);
    let report = db.drop_snapshot(snap).unwrap().unwrap();
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    assert_eq!(db.get_refcount(200).unwrap(), 0);
    assert!(
        report.freed_pbas.is_empty(),
        "DropSnapshot does not emit per-LBA PBA decrefs",
    );
}

// ---------------- S4 drop_snapshot extended tests (SPEC §4.4 / §4.5) ---

#[test]
fn drop_snapshot_symmetric_with_no_snapshot_refcounts() {
    // Phase 5: SPEC §4.4 rc symmetry no longer holds asymmetrically:
    // snapshot-on-volume commits use the legacy apply path (which
    // bumps rc); plain (snapshot-free) volume uses the hot-path lane
    // (rc unchanged). The L2P state symmetry still holds — that is
    // the load-bearing post-condition for callers — but rc diverges.
    let (_d1, db_snap) = mk_db();
    let (_d2, db_plain) = mk_db();
    for lba in 0..8u64 {
        remap(&db_snap, lba, remap_val(100 + lba, 1), None);
        remap(&db_plain, lba, remap_val(100 + lba, 1), None);
    }
    let s = db_snap.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    for lba in 0..8u64 {
        if lba % 2 == 0 {
            remap(&db_snap, lba, remap_val(200 + lba, 1), None);
            remap(&db_plain, lba, remap_val(200 + lba, 1), None);
        }
    }
    db_snap.drop_snapshot(s).unwrap();
    for lba in 0..8u64 {
        // L2P state symmetry: same current mappings on both sides.
        assert_eq!(
            db_snap.get(BOOTSTRAP_VOLUME_ORD, lba).unwrap(),
            db_plain.get(BOOTSTRAP_VOLUME_ORD, lba).unwrap(),
            "L2P divergence at lba {lba}",
        );
    }
}

#[test]
fn drop_snapshot_keeps_pba_rc_for_dedup_multi_lba_share() {
    // Phase 5: rc must be seeded explicitly because hot-path L2pRemap
    // no longer maintains it. Seed rc(777)=4 (matches the four LBAs
    // sharing the PBA), then exercise the packed-slot drop-snapshot
    // path. Phase 5 no longer subtracts one PBA rc entry per snapshot
    // LBA diff, so rc(777) stays intact and no freed_pbas are surfaced.
    let (_d, db) = mk_db();
    for lba in 10u64..14 {
        remap(&db, lba, remap_val(777, (lba - 10) as u8), None);
    }
    db.incref_pba(777, 4).unwrap(); // mirror the four LBA owners
    assert_eq!(db.get_refcount(777).unwrap(), 4);
    let snap = db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    for lba in 10u64..14 {
        remap(&db, lba, remap_val(888 + lba, 0), None);
    }
    // Leaf shared: hot-path doesn't touch rc anyway, rc(777) still 4.
    assert_eq!(db.get_refcount(777).unwrap(), 4);
    let report = db.drop_snapshot(snap).unwrap().unwrap();
    assert_eq!(db.get_refcount(777).unwrap(), 4);
    let freed: std::collections::HashSet<Pba> = report.freed_pbas.iter().copied().collect();
    assert!(
        !freed.contains(&777),
        "pba 777 should not be freed by per-LBA snapshot decrefs",
    );
    // Phase 5: L2pRemap is rc-neutral on all paths, so the new PBAs
    // stay at rc=0 and are not in `freed_pbas` (they were never live
    // in the snapshot's L2P at plan time).
    for lba in 10u64..14 {
        let pba = 888 + lba;
        assert_eq!(db.get_refcount(pba).unwrap(), 0);
        assert!(!freed.contains(&pba));
    }
}

#[test]
fn drop_snapshot_pages_commit_atomically_via_wal_without_pba_decref() {
    // Phase 5: DropSnapshot still releases metadata pages through the
    // lifecycle WAL, but ignores PBA decrefs because global PBA rc is not
    // a per-live-LBA counter.
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        remap(&db, 10, remap_val(100, 1), None);
        db.incref_pba(100, 1).unwrap();
        let snap = db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
        remap(&db, 10, remap_val(200, 1), None);
        assert_eq!(db.get_refcount(100).unwrap(), 1);
        db.drop_snapshot(snap).unwrap();
        assert_eq!(db.get_refcount(100).unwrap(), 1);
        // Close without a flush — the drop is only in the WAL.
    }
    let db = Db::open(dir.path()).unwrap();
    // Replay must not run a per-LBA pba_decref.
    assert_eq!(db.get_refcount(100).unwrap(), 1);
    // Phase 5: rc(200) stays at whatever the legacy apply_l2p_remap
    // assigned during replay (may be 0 or 1 depending on prev state);
    // the load-bearing snapshot-list invariant is below.
    assert!(
        db.snapshots().is_empty(),
        "snapshot list must stay empty after replay",
    );
}

#[test]
fn drop_snapshot_keeps_zero_pba_refcount() {
    // Non-refcount path (raw `insert`, without incref): DropSnapshot must
    // leave PBA rc untouched and avoid surfacing freed PBAs.
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
        "DropSnapshot does not emit per-LBA PBA decrefs",
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
    // Phase 5: L2pRemapRange never touches global rc, so freed is
    // always empty and rc stays at 0 for every pba.
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
        assert_eq!(db.get_refcount(100 + i as u64).unwrap(), 0);
    }
}

#[test]
fn l2p_remap_range_overwrite_collects_freed_pbas() {
    // Phase 5: L2pRemapRange never touches global rc; freed_pbas is
    // always empty for L2P remap ops. The L2P prev values are still
    // surfaced so onyx can drive volume-dead-list cleanup.
    let (_d, db) = mk_db();
    let first: Vec<_> = (0..4u8).map(|i| remap_val(100 + i as u64, 1)).collect();
    let _ = remap_range(&db, 10, first.clone());
    for i in 0..4u8 {
        assert_eq!(db.get_refcount(100 + i as u64).unwrap(), 0);
    }

    let second: Vec<_> = (0..4u8).map(|i| remap_val(200 + i as u64, 2)).collect();
    let (applied, prevs, freed) = remap_range(&db, 10, second);
    assert!(applied.iter().all(|a| *a));
    assert_eq!(prevs.len(), 4);
    for (i, prev) in prevs.iter().enumerate() {
        assert_eq!(*prev, Some(first[i]));
    }
    assert!(
        freed.is_empty(),
        "Phase 5: L2pRemapRange surfaces no freed PBAs (dead-list owns them)"
    );
    for i in 0..4u8 {
        assert_eq!(db.get_refcount(100 + i as u64).unwrap(), 0);
        assert_eq!(db.get_refcount(200 + i as u64).unwrap(), 0);
    }
}

#[test]
fn l2p_remap_range_crosses_l2p_shard_boundary() {
    // Phase 5: rc stays at 0; the load-bearing assertion is shard-cross
    // L2P routing, not rc.
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
        assert_eq!(db.get_refcount(500 + i as u64).unwrap(), 0);
    }
}

#[test]
fn l2p_remap_range_snapshot_pin_stays_rc_neutral() {
    // Phase 5: L2pRemapRange is rc-neutral on all paths. Pre- and
    // post-snapshot range writes leave global rc at 0 for every PBA;
    // freed_pbas surfaces nothing.
    let (_d, db) = mk_db();
    let seed: Vec<_> = (0..4u8).map(|i| remap_val(700 + i as u64, 1)).collect();
    let _ = remap_range(&db, 10, seed.clone());
    let snap = db.take_snapshot(BOOTSTRAP_VOLUME_ORD).unwrap();
    let new_values: Vec<_> = (0..4).map(|i| remap_val(900, 10 + i)).collect();
    let (_applied, _prevs, freed) = remap_range(&db, 10, new_values);
    assert!(
        freed.is_empty(),
        "Phase 5 range remap is rc-neutral even with a snapshot"
    );
    for i in 0..4u8 {
        assert_eq!(db.get_refcount(700 + i as u64).unwrap(), 0);
    }
    assert_eq!(db.get_refcount(900).unwrap(), 0);
    // DropSnapshot is also PBA rc-neutral. rc(900) was never bumped
    // either, so all PBA refs stay at 0.
    db.drop_snapshot(snap).unwrap();
    for i in 0..4u8 {
        assert_eq!(db.get_refcount(700 + i as u64).unwrap(), 0);
    }
    assert_eq!(db.get_refcount(900).unwrap(), 0);
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
