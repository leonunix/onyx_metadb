use super::*;

fn footprint(lanes: impl IntoIterator<Item = DispatchLaneKey>) -> DispatchFootprint {
    DispatchFootprint {
        global: false,
        lanes: lanes.into_iter().collect(),
    }
}

fn entry(footprint: DispatchFootprint, durable: bool) -> DispatchEntry {
    DispatchEntry { footprint, durable }
}

#[test]
fn dispatch_scheduler_allows_disjoint_higher_lsn_to_bypass() {
    let mut state = DispatchState::default();
    state
        .pending
        .insert(10, entry(footprint([DispatchLaneKey::L2p(0, 0)]), false));
    state
        .pending
        .insert(11, entry(footprint([DispatchLaneKey::L2p(0, 1)]), true));

    assert!(
        dispatch_ready(&state, 11),
        "lower undurable work on another shard must not block dispatch"
    );
}

#[test]
fn dispatch_scheduler_blocks_conflicting_higher_lsn() {
    let mut state = DispatchState::default();
    state
        .pending
        .insert(10, entry(footprint([DispatchLaneKey::Refcount(2)]), false));
    state
        .pending
        .insert(11, entry(footprint([DispatchLaneKey::Refcount(2)]), true));

    assert!(
        !dispatch_ready(&state, 11),
        "same lane must preserve WAL LSN dispatch order"
    );
    state.pending.remove(&10);
    assert!(dispatch_ready(&state, 11));
}

#[test]
fn dispatch_scheduler_treats_global_as_conflicting_with_every_lane() {
    let mut state = DispatchState::default();
    state
        .pending
        .insert(10, entry(DispatchFootprint::global(), true));
    state
        .pending
        .insert(11, entry(footprint([DispatchLaneKey::Dedup(0)]), true));

    assert!(
        !dispatch_ready(&state, 11),
        "global serial work must retain the old FIFO barrier"
    );
}

#[test]
fn small_remap_batches_use_lane_dispatch_not_global_serial() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let mut raw = [0u8; crate::paged::format::LEAF_VALUE_SIZE];
    raw[..8].copy_from_slice(&123_u64.to_be_bytes());

    let ops = [WalOp::L2pRemap {
        vol_ord: BOOTSTRAP_VOLUME_ORD,
        lba: 7,
        new_value: L2pValue(raw),
        guard: None,
    }];

    assert!(
        !db.batch_uses_serial_apply(&ops),
        "tiny remap commits must keep precise L2P/refcount footprints so they do not block dedup dispatch globally"
    );
    let plan = db
        .build_lane_dispatch_plan(&db.volumes.read().clone(), &ops)
        .unwrap();
    let footprint = DispatchFootprint::from_lane_plan(&plan);
    assert!(!footprint.global);
    assert!(
        footprint
            .lanes
            .iter()
            .any(|lane| matches!(lane, DispatchLaneKey::L2p(BOOTSTRAP_VOLUME_ORD, _)))
    );
    assert!(
        !footprint
            .lanes
            .iter()
            .any(|lane| matches!(lane, DispatchLaneKey::Dedup(_))),
        "remap-only commits should not conflict with dedup shards"
    );
}

// ------------------------------------------------------------------------
// Precision tests for the dispatch-footprint refcount-shard computation.
//
// Pre-the planner did `rc_enqueued.fill(true)` on any remap,
// claiming all 16 refcount lanes regardless of whether any op actually
// touched rc. With making L2pRemap/L2pRemapRange rc-neutral on
// the hot path, that blanket was the dominant serialization cause for
// concurrent commit_workers on a single volume — every commit's
// footprint conflicted with every other commit on the rc lanes.
//
// These tests pin the precise contract:
//   - L2pRemapRange (always unguarded, rc-neutral): zero rc lanes
//   - L2pRemap { guard: None }: zero rc lanes
//   - L2pRemap { guard: Some((P, _)) }: exactly rc_shard_of_pba(P)
//   - DedupPut / DedupPutGuarded / DedupDelete / DedupCompareDelete /
//     DedupComparePut: precisely the rc shards reachable from the op's
//     PBA fields, matching the lane apply's stage_rc{,_decref_if_live}
//     touch set in lanes/dedup.rs.
//
// Two disjoint L2pRemapRange commits must NOT conflict via rc — this is
// the regression gate for the seqwrite path's commit fan-out.
// ------------------------------------------------------------------------

fn rc_lanes(footprint: &DispatchFootprint) -> Vec<usize> {
    footprint
        .lanes
        .iter()
        .filter_map(|lane| match lane {
            DispatchLaneKey::Refcount(sid) => Some(*sid),
            _ => None,
        })
        .collect()
}

fn l2p_value_with_pba(pba: u64) -> L2pValue {
    let mut raw = [0u8; crate::paged::format::LEAF_VALUE_SIZE];
    raw[..8].copy_from_slice(&pba.to_be_bytes());
    raw[crate::paged::format::LEAF_VALUE_SIZE - 1] = 1;
    L2pValue(raw)
}

fn dedup_value_with_pba(pba: u64) -> crate::dedup_types::DedupValue {
    let mut raw = [0u8; 28];
    raw[..8].copy_from_slice(&pba.to_be_bytes());
    crate::dedup_types::DedupValue(raw)
}

fn hash_for(n: u64) -> crate::dedup_types::Hash8 {
    let mut x = [0u8; 8];
    x[..].copy_from_slice(&n.to_be_bytes());
    x
}

fn hash_for_dedup_lane(lane: u32, shards: u32) -> crate::dedup_types::Hash8 {
    assert!(lane < shards && shards.is_power_of_two());
    let mut hash = hash_for(u64::from(lane) + 1);
    let shift = 8 - shards.trailing_zeros();
    hash[0] = (lane << shift) as u8;
    assert_eq!(crate::dedup_types::shard_for_hash(&hash, shards), lane);
    hash
}

fn spin_until(timeout: std::time::Duration, mut predicate: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + timeout;
    while !predicate() {
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for concurrent test state"
        );
        std::thread::yield_now();
    }
}

#[test]
fn remap_range_only_commits_have_no_rc_footprint() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();

    let values: Box<[L2pValue]> = (0..32u64).map(l2p_value_with_pba).collect();
    let ops = [WalOp::L2pRemapRange {
        vol_ord: BOOTSTRAP_VOLUME_ORD,
        start_lba: 1024,
        values,
    }];

    let plan = db
        .build_lane_dispatch_plan(&db.volumes.read().clone(), &ops)
        .unwrap();
    let footprint = DispatchFootprint::from_lane_plan(&plan);
    assert_eq!(
        rc_lanes(&footprint),
        Vec::<usize>::new(),
        "L2pRemapRange is always unguarded and rc-neutral; must not claim any rc lane"
    );
}

#[test]
fn unguarded_remap_commits_have_no_rc_footprint() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();

    let ops: Vec<WalOp> = (0..10u64)
        .map(|i| WalOp::L2pRemap {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            lba: i,
            new_value: l2p_value_with_pba(100 + i),
            guard: None,
        })
        .collect();

    let plan = db
        .build_lane_dispatch_plan(&db.volumes.read().clone(), &ops)
        .unwrap();
    let footprint = DispatchFootprint::from_lane_plan(&plan);
    assert_eq!(
        rc_lanes(&footprint),
        Vec::<usize>::new(),
        "Unguarded L2pRemap is rc-neutral; must not claim any rc lane"
    );
}

#[test]
fn guarded_remap_adds_only_guard_pba_rc_shard() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let guard_pba: Pba = 0xABCDE;
    let expected_sid = super::lanes::rc_shard_of_pba(guard_pba, db.refcount_shards.len());

    let ops = [WalOp::L2pRemap {
        vol_ord: BOOTSTRAP_VOLUME_ORD,
        lba: 42,
        new_value: l2p_value_with_pba(7777),
        guard: Some((guard_pba, 2)),
    }];

    let plan = db
        .build_lane_dispatch_plan(&db.volumes.read().clone(), &ops)
        .unwrap();
    let footprint = DispatchFootprint::from_lane_plan(&plan);
    assert_eq!(
        rc_lanes(&footprint),
        vec![expected_sid],
        "Guarded L2pRemap reads rc[guard.0] in apply_l2p_bucket; only that one rc shard must appear"
    );
}

#[test]
fn dedup_put_commit_has_precise_rc_footprint() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let num_rc = db.refcount_shards.len();
    let old_pba: Pba = 0x100;
    let new_pba: Pba = 0x200;
    let sid_old = super::lanes::rc_shard_of_pba(old_pba, num_rc);
    let sid_new = super::lanes::rc_shard_of_pba(new_pba, num_rc);
    // Guard against the test accidentally picking colliding PBAs — if
    // shard count changes the test would lose its precision check.
    assert_ne!(sid_old, sid_new, "test setup picked colliding rc shards");

    let ops = [WalOp::DedupPut {
        hash: hash_for(42),
        value: dedup_value_with_pba(new_pba),
        old_pba: Some(old_pba),
    }];

    let plan = db
        .build_lane_dispatch_plan(&db.volumes.read().clone(), &ops)
        .unwrap();
    let footprint = DispatchFootprint::from_lane_plan(&plan);
    let mut got = rc_lanes(&footprint);
    got.sort();
    let mut expected = vec![sid_old, sid_new];
    expected.sort();
    assert_eq!(
        got, expected,
        "DedupPut must claim rc shards for both old_pba and new_pba (lanes/dedup.rs::stage_rc paths)"
    );
}

#[test]
fn l2p_shard_for_matches_shard_for_key_l2p() {
    // The public `Db::l2p_shard_for` MUST agree with the internal
    // `shard_for_key_l2p(&volume.shards, lba)` for every LBA. Onyx
    // pre-shards passthrough commits per L2P shard based on the
    // public API; if the two routing functions diverge, sub-commits
    // would target shards they don't actually touch in apply, breaking
    // the dispatch footprint invariant and creating phantom races.
    let dir = tempfile::TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let volumes = db.volumes.read().clone();
    let volume = volumes
        .get(&BOOTSTRAP_VOLUME_ORD)
        .expect("bootstrap volume present");
    for lba in [0u64, 1, 127, 128, 255, 256, 1024, 1_000_000, u64::MAX / 2] {
        assert_eq!(
            db.l2p_shard_for(lba),
            shard_for_key_l2p(&volume.shards, lba),
            "l2p_shard_for / shard_for_key_l2p disagree at lba={lba}",
        );
    }
}

#[test]
fn two_disjoint_l2p_remap_range_commits_do_not_conflict() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();

    // Build two L2pRemapRange ops whose LBA spans hash to different L2P
    // shards. With making rc-neutral and Component A removing
    // the blanket rc_enqueued.fill, two commits on disjoint L2P shards
    // must have disjoint footprints — this is the regression gate that
    // the seqwrite hot path can finally fan out across commit_workers.
    let volumes_snapshot = db.volumes.read().clone();
    let volume = volumes_snapshot
        .get(&BOOTSTRAP_VOLUME_ORD)
        .expect("bootstrap volume present");

    // Find two LBAs that map to different L2P shards. LEAF_SHIFT=7
    // groups 128 consecutive LBAs in one shard, so step by 128.
    let mut sid_a = 0usize;
    let mut start_a = 0u64;
    let mut start_b = 0u64;
    let mut sid_b = sid_a;
    'outer: for candidate in (0u64..2048u64).step_by(128) {
        let sid = shard_for_key_l2p(&volume.shards, candidate);
        if sid_a == 0 && start_a == 0 {
            sid_a = sid;
            start_a = candidate;
            continue;
        }
        if sid != sid_a {
            sid_b = sid;
            start_b = candidate;
            break 'outer;
        }
    }
    assert_ne!(
        sid_a, sid_b,
        "test setup failed to find disjoint L2P shards"
    );

    let make_range = |start_lba: u64| -> WalOp {
        // One 32-LBA range inside a single 128-LBA leaf → one shard.
        let values: Box<[L2pValue]> = (0..32u64)
            .map(|i| l2p_value_with_pba(start_lba + i))
            .collect();
        WalOp::L2pRemapRange {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            start_lba,
            values,
        }
    };

    let plan_a = db
        .build_lane_dispatch_plan(&volumes_snapshot, &[make_range(start_a)])
        .unwrap();
    let plan_b = db
        .build_lane_dispatch_plan(&volumes_snapshot, &[make_range(start_b)])
        .unwrap();
    let fp_a = DispatchFootprint::from_lane_plan(&plan_a);
    let fp_b = DispatchFootprint::from_lane_plan(&plan_b);

    assert!(
        !fp_a.conflicts(&fp_b),
        "disjoint-shard L2pRemapRange commits must not conflict via rc lanes (a={:?}, b={:?})",
        fp_a.lanes,
        fp_b.lanes,
    );
}

// -------- BFG: direct L2P apply fast path --------

/// Build a Db with both `l2p_buffer_enabled` and
/// `commit_direct_apply_enabled` on. The direct-apply path requires
/// `use_buffer` per shard, which only happens when the embedder
/// enables the buffer at create time. The buffer's compactor is
/// configured with a tiny soft trigger so any state mutation is
/// quickly folded into the tree, exercising the lookup-fallthrough
/// (read_view) leg of `apply_l2p_bucket_buffer` as well as the
/// buffer hit leg.
fn mk_direct_apply_db() -> (tempfile::TempDir, std::sync::Arc<Db>) {
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = crate::config::Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    cfg.commit_direct_apply_enabled = true;
    cfg.l2p_buffer_soft_entries = 4;
    cfg.l2p_buffer_max_interval_ms = 50;
    let db = Db::create_with_config(cfg).unwrap();
    (dir, db)
}

/// Same harness but with `commit_direct_apply_enabled = false`. Used
/// for the equivalence test that compares direct- vs lane-path
/// outcomes byte-for-byte.
fn mk_lane_only_db() -> (tempfile::TempDir, std::sync::Arc<Db>) {
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = crate::config::Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    cfg.commit_direct_apply_enabled = false;
    cfg.l2p_buffer_soft_entries = 4;
    cfg.l2p_buffer_max_interval_ms = 50;
    let db = Db::create_with_config(cfg).unwrap();
    (dir, db)
}

#[test]
fn direct_apply_eligibility_l2p_only_with_buffer_passes() {
    let (_d, db) = mk_direct_apply_db();
    let volumes = db.volumes.read().clone();

    let ops: Vec<WalOp> = (0..10u64)
        .map(|i| WalOp::L2pRemap {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            lba: i,
            new_value: l2p_value_with_pba(100 + i),
            guard: None,
        })
        .collect();

    let plan = db.build_lane_dispatch_plan(&volumes, &ops).unwrap();
    assert!(
        Db::plan_is_l2p_direct_eligible(&plan, &volumes),
        "L2P-only unguarded-remap plan must be direct-apply-eligible \
         when use_buffer is set on every target shard"
    );
}

#[test]
fn direct_apply_eligibility_no_buffer_fails() {
    // Default config has `l2p_buffer_enabled = false`, so use_buffer
    // is false on every shard, and the direct path must refuse to
    // carry the commit even though no rc / dedup work is present.
    let dir = tempfile::TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let volumes = db.volumes.read().clone();

    let ops = [WalOp::L2pPut {
        vol_ord: BOOTSTRAP_VOLUME_ORD,
        lba: 42,
        value: l2p_value_with_pba(7),
    }];
    let plan = db.build_lane_dispatch_plan(&volumes, &ops).unwrap();
    assert!(
        !Db::plan_is_l2p_direct_eligible(&plan, &volumes),
        "use_buffer=false on target shard must force the lane path"
    );
}

#[test]
fn direct_apply_eligibility_dedup_present_fails() {
    let (_d, db) = mk_direct_apply_db();
    let volumes = db.volumes.read().clone();

    let ops = vec![
        WalOp::L2pPut {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            lba: 1,
            value: l2p_value_with_pba(11),
        },
        WalOp::DedupPut {
            hash: hash_for(42),
            value: dedup_value_with_pba(99),
            old_pba: None,
        },
    ];
    let plan = db.build_lane_dispatch_plan(&volumes, &ops).unwrap();
    assert!(
        !Db::plan_is_l2p_direct_eligible(&plan, &volumes),
        "any dedup op in the batch must force the lane path"
    );
}

#[test]
fn direct_apply_eligibility_guarded_remap_fails() {
    // A guarded L2pRemap claims the rc shard of its guard pba (lanes.rs:91),
    // so `rc_enqueued` is non-empty. The direct path must defer to the
    // lane path so the guard's rc.get() runs under the standard lane
    // ordering rather than racing inline on the commit thread.
    let (_d, db) = mk_direct_apply_db();
    let volumes = db.volumes.read().clone();

    let ops = [WalOp::L2pRemap {
        vol_ord: BOOTSTRAP_VOLUME_ORD,
        lba: 42,
        new_value: l2p_value_with_pba(7777),
        guard: Some((0xABCDE, 1)),
    }];
    let plan = db.build_lane_dispatch_plan(&volumes, &ops).unwrap();
    assert!(
        !Db::plan_is_l2p_direct_eligible(&plan, &volumes),
        "guarded L2pRemap (rc-touching) must force the lane path"
    );
}

#[test]
fn direct_apply_path_increments_counter_and_serves_reads() {
    let (_d, db) = mk_direct_apply_db();

    let baseline_count = db.metrics_snapshot().commit_direct_apply_count;

    // Pure L2P-only commit; should take the direct path.
    let mut tx = db.begin();
    for i in 0u64..16 {
        tx.insert(0, i, l2p_value_with_pba(100 + i));
    }
    tx.commit().unwrap();

    let post_count = db.metrics_snapshot().commit_direct_apply_count;
    assert!(
        post_count > baseline_count,
        "direct_apply counter must increment for an L2P-only commit \
         (before={baseline_count}, after={post_count})"
    );

    // Reads must see the just-committed values via the buffer overlay.
    for i in 0u64..16 {
        let got = db.get(0, i).unwrap();
        assert_eq!(
            got,
            Some(l2p_value_with_pba(100 + i)),
            "direct-applied LBA {i} must be readable"
        );
    }
}

#[test]
fn direct_apply_equivalent_to_lane_path() {
    // Drive identical L2P-only workloads through both paths and
    // assert the post-commit reads return identical values. This is
    // the load-bearing safety check for the direct path is
    // only safe if it produces byte-equivalent state.
    let (_d1, db_direct) = mk_direct_apply_db();
    let (_d2, db_lane) = mk_lane_only_db();

    let lbas: Vec<u64> = (0..64u64).collect();
    let values: Vec<L2pValue> = lbas
        .iter()
        .map(|&i| l2p_value_with_pba(0xC0DE + i))
        .collect();

    // First commit: L2pPut burst.
    let mut tx_d = db_direct.begin();
    let mut tx_l = db_lane.begin();
    for (i, v) in lbas.iter().zip(values.iter()) {
        tx_d.insert(0, *i, *v);
        tx_l.insert(0, *i, *v);
    }
    tx_d.commit().unwrap();
    tx_l.commit().unwrap();

    // Second commit: overwrite half with L2pRemap (unguarded) to
    // exercise the prev-lookup + remap branch on the direct path.
    let new_values: Vec<L2pValue> = lbas
        .iter()
        .map(|&i| l2p_value_with_pba(0xBABE + i))
        .collect();
    let mut tx_d = db_direct.begin();
    let mut tx_l = db_lane.begin();
    for i in 0..32u64 {
        tx_d.l2p_remap(0, i, new_values[i as usize], None);
        tx_l.l2p_remap(0, i, new_values[i as usize], None);
    }
    tx_d.commit().unwrap();
    tx_l.commit().unwrap();

    // Snapshot final state.
    for i in lbas {
        let got_d = db_direct.get(0, i).unwrap();
        let got_l = db_lane.get(0, i).unwrap();
        assert_eq!(
            got_d, got_l,
            "direct-apply and lane-apply diverged at lba {i}: direct={got_d:?} lane={got_l:?}"
        );
    }

    let snap_d = db_direct.metrics_snapshot();
    let snap_l = db_lane.metrics_snapshot();
    assert!(
        snap_d.commit_direct_apply_count > 0,
        "direct db must have taken the direct path at least once"
    );
    assert_eq!(
        snap_l.commit_direct_apply_count, 0,
        "lane-only db must never take the direct path"
    );
}

// -------- BFG: deferred outcome API --------

fn mk_deferred_apply_db(deferred: bool) -> (tempfile::TempDir, std::sync::Arc<Db>) {
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = crate::config::Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    cfg.commit_direct_apply_enabled = true;
    cfg.commit_deferred_outcomes_enabled = deferred;
    // Quick compactor so the deferred drain runs promptly during tests.
    cfg.l2p_buffer_soft_entries = 1;
    cfg.l2p_buffer_max_interval_ms = 25;
    let db = Db::create_with_config(cfg).unwrap();
    (dir, db)
}

/// commit_ops_deferred MUST produce the same `(lsn, Vec<ApplyOutcome>)`
/// as commit_ops on master would have. We confirm equivalence by
/// running the same op stream through two databases — one
/// deferred=false (immediate-release handle), one deferred=true (drain
/// at compactor) — and comparing every outcome variant byte-for-byte.
#[test]
fn commit_ops_deferred_l2p_only_equivalence() {
    let (_d_sync, db_sync) = mk_deferred_apply_db(false);
    let (_d_def, db_def) = mk_deferred_apply_db(true);

    let ops: Vec<WalOp> = (0..32u64)
        .map(|i| WalOp::L2pPut {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            lba: i,
            value: l2p_value_with_pba(0xDEED + i),
        })
        .collect();

    let (lsn_sync, h_sync) = db_sync.commit_ops_deferred(&ops).unwrap();
    let (lsn_def, h_def) = db_def.commit_ops_deferred(&ops).unwrap();
    assert_eq!(lsn_sync, lsn_def);

    // Sync mode resolves immediately (no compactor wait).
    let outs_sync = h_sync.recv().unwrap();
    // Deferred mode waits for the compactor's per-pass drain. Drive
    // one pass on the caller thread instead of sleeping — tests
    // remain deterministic under heavy parallel runs (`cargo test`
    // default test_threads != 1).
    db_def.test_force_compact_pass();
    let outs_def = h_def.recv().unwrap();
    assert_eq!(outs_sync.len(), outs_def.len());
    for (i, (a, b)) in outs_sync.iter().zip(outs_def.iter()).enumerate() {
        assert_eq!(
            format!("{a:?}"),
            format!("{b:?}"),
            "deferred vs sync outcome mismatch at idx {i}"
        );
    }
}

/// Deferred outcomes carry the same seq_guard reject information as
/// the sync path: an `L2pRemap` with `new_value.seq() <= cur.seq()`
/// surfaces `applied=false, prev=Some(cur), freed_pba=None`.
#[test]
fn commit_ops_deferred_seqguard_reject_carries_prev() {
    let (_d, db) = mk_deferred_apply_db(true);

    // Seed an entry with seq=10 at lba 10. seq_guard_rejects treats
    // new_seq=0 and cur.seq=0 as never-reject (recovery-friendly
    // accept-on-equality), so we need both seqs strictly positive.
    let seeded = l2p_value_with_pba(0x100).with_seq(10);
    let mut tx = db.begin();
    tx.insert(BOOTSTRAP_VOLUME_ORD, 10, seeded);
    tx.commit().unwrap();
    let cur = db.get(BOOTSTRAP_VOLUME_ORD, 10).unwrap().unwrap();
    assert_eq!(cur.seq(), 10);

    // Attempt remap with seq < cur.seq → seq_guard rejects.
    let rejected = l2p_value_with_pba(0x200).with_seq(5);
    let (_, handle) = db
        .commit_ops_deferred(&[WalOp::L2pRemap {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            lba: 10,
            new_value: rejected,
            guard: None,
        }])
        .unwrap();
    db.test_force_compact_pass();
    let outcomes = handle.recv().unwrap();
    assert_eq!(outcomes.len(), 1);
    match &outcomes[0] {
        ApplyOutcome::L2pRemap {
            applied,
            prev,
            freed_pba,
        } => {
            assert!(!*applied, "lower-seq remap must be rejected");
            assert_eq!(prev.unwrap(), cur);
            assert!(freed_pba.is_none());
        }
        other => panic!("expected L2pRemap outcome, got {other:?}"),
    }
}

/// Drop the handle before recv. The aggregator's drain still releases
/// the entry; the underlying send fails silently. The aggregator must
/// not leak the staged outcomes, and a subsequent commit must
/// continue to function (no aggregator state corruption).
#[test]
fn commit_ops_deferred_handle_drop_does_not_leak_sender() {
    let (_d, db) = mk_deferred_apply_db(true);

    let (_, handle) = db
        .commit_ops_deferred(&[WalOp::L2pPut {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            lba: 1,
            value: l2p_value_with_pba(0xABCD),
        }])
        .unwrap();
    drop(handle);

    // Drive one compactor pass deterministically.
    db.test_force_compact_pass();

    // Aggregator should be empty (drained).
    let pending = db.deferred_outcomes.pending_depth();
    assert_eq!(pending, 0, "aggregator drained the dropped entry");

    // Follow-up commit works fine.
    let (_, h2) = db
        .commit_ops_deferred(&[WalOp::L2pPut {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            lba: 2,
            value: l2p_value_with_pba(0xDCBA),
        }])
        .unwrap();
    db.test_force_compact_pass();
    let outs = h2.recv().unwrap();
    assert_eq!(outs.len(), 1);
}

// -------- BFG onyx-side stager (`Db::stage_ops`) --------

/// Empty batch returns `(last_applied_lsn, [])` and bumps the
/// `commit_empty` counter — same observable behaviour as
/// `commit_ops_unlogged` / `commit_ops_deferred`.
#[test]
fn stage_ops_empty_batch_is_noop() {
    let (_d, db) = mk_deferred_apply_db(true);
    let pre = db.last_applied_lsn();
    let (lsn, outs) = db.stage_ops(&[]).unwrap();
    assert_eq!(lsn, pre);
    assert!(outs.is_empty());
}

/// L2P-only batch: stage_ops applies into the buffer slot and reads
/// observe the value immediately through `multi_get` (which consults
/// `lookup_for_open_bfg` before falling through to the tree).
#[test]
fn stage_ops_l2p_put_visible_to_reads() {
    let (_d, db) = mk_deferred_apply_db(true);
    let ops: Vec<WalOp> = (0..32u64)
        .map(|i| WalOp::L2pPut {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            lba: 1_000 + i,
            value: l2p_value_with_pba(0xCAFE + i),
        })
        .collect();
    let (lsn, outs) = db.stage_ops(&ops).unwrap();
    assert!(lsn > 0);
    assert_eq!(outs.len(), 32);
    for i in 0..32 {
        let got = db.get(BOOTSTRAP_VOLUME_ORD, 1_000 + i).unwrap();
        assert_eq!(got, Some(l2p_value_with_pba(0xCAFE + i)));
    }
}

#[test]
fn stage_ops_runs_refcount_work_on_shard_lanes() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = crate::config::Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    cfg.commit_direct_apply_enabled = true;
    cfg.rc_authoritative_reclaim = true;
    let db = Db::create_with_config(cfg).unwrap();
    let before = db.metrics_snapshot();
    let ops: Vec<WalOp> = (0..256u64)
        .map(|i| WalOp::L2pPut {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            lba: 20_000 + i,
            value: l2p_value_with_pba(0x20_000 + i),
        })
        .collect();

    let (_, outcomes) = db.stage_ops(&ops).unwrap();
    let after = db.metrics_snapshot();

    assert_eq!(outcomes.len(), ops.len());
    assert!(
        after.rc_apply_lane_tasks > before.rc_apply_lane_tasks,
        "staged refcount work must execute on the per-shard lanes"
    );
    assert!(
        after.apply_refcount_count > before.apply_refcount_count,
        "staged installs must still update refcounts"
    );
}

#[test]
fn stage_ops_retains_rc_dispatch_until_dedup_lane_finishes() {
    use std::sync::Arc;
    use std::time::Duration;

    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = crate::config::Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    cfg.dedup_shards = 2;
    let db = Arc::new(Db::create_with_config(cfg).unwrap());
    let hash = hash_for_dedup_lane(0, 2);
    let lane = crate::dedup_types::shard_for_hash(&hash, 2) as usize;
    let pba = 0x51_000;

    let (started_tx, started_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    db.dedup_lanes[lane].enqueue_maintenance(Box::new(move || {
        let _ = started_tx.send(());
        let _ = release_rx.recv();
    }));
    started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("dedup maintenance blocker did not start");

    let commit_db = Arc::clone(&db);
    let commit = std::thread::spawn(move || {
        commit_db.stage_ops(&[WalOp::DedupPut {
            hash,
            value: dedup_value_with_pba(pba),
            old_pba: None,
        }])
    });

    spin_until(Duration::from_secs(2), || {
        let state = db.dedup_lanes[lane].inner.state.lock();
        state.queue.front().is_some_and(|task| task.slot.is_ready())
    });
    {
        let state = db.dispatch_state.lock();
        assert_eq!(
            state.pending.len(),
            1,
            "derived RC work must retain its dispatch reservation"
        );
        assert!(
            state.pending.values().all(|entry| entry.durable),
            "the staged commit must be dispatch-ready before its dedup lane runs"
        );
    }

    release_tx.send(()).unwrap();
    let (lsn, outcomes) = commit.join().unwrap().unwrap();
    assert!(lsn > 0);
    assert_eq!(outcomes.len(), 1);
    assert!(db.dispatch_state.lock().pending.is_empty());
    assert_eq!(db.get_refcount(pba).unwrap(), 1);
}

#[test]
fn stage_ops_preserves_cross_dedup_shard_rc_order() {
    use std::sync::Arc;
    use std::time::Duration;

    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = crate::config::Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    cfg.dedup_shards = 2;
    let db = Arc::new(Db::create_with_config(cfg).unwrap());
    let first_hash = hash_for_dedup_lane(0, 2);
    let guarded_hash = hash_for_dedup_lane(1, 2);
    let guard_pba = 0x61_000;
    let guarded_pba = 0x62_000;

    let (started_tx, started_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    db.dedup_lanes[0].enqueue_maintenance(Box::new(move || {
        let _ = started_tx.send(());
        let _ = release_rx.recv();
    }));
    started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("dedup maintenance blocker did not start");

    let commit_db = Arc::clone(&db);
    let commit = std::thread::spawn(move || {
        commit_db.stage_ops(&[
            WalOp::DedupPut {
                hash: first_hash,
                value: dedup_value_with_pba(guard_pba),
                old_pba: None,
            },
            WalOp::DedupPutGuarded {
                hash: guarded_hash,
                value: dedup_value_with_pba(guarded_pba),
                pba_guard: guard_pba,
                min_rc: 1,
                old_pba: None,
            },
        ])
    });

    // The second dedup lane has crossed this LSN while lane 0 is blocked. In
    // the ordered implementation it only executes a follower no-op; the
    // coordinator still applies both real ops in their original order.
    spin_until(Duration::from_secs(2), || {
        db.dedup_lanes[1].last_applied_lsn() > 0
    });
    release_tx.send(()).unwrap();
    let (_, outcomes) = commit.join().unwrap().unwrap();

    assert_eq!(outcomes.len(), 2);
    assert_eq!(
        db.get_dedup(&guarded_hash).unwrap(),
        Some(dedup_value_with_pba(guarded_pba)),
        "the guarded op must observe the earlier cross-shard refcount update"
    );
    assert_eq!(db.get_refcount(guard_pba).unwrap(), 1);
    assert_eq!(db.get_refcount(guarded_pba).unwrap(), 1);
}

/// stage_ops + commit_ops_deferred operate on the same database
/// state — a value written by stage is visible to a later
/// deferred-commit batch's read, and vice-versa.
#[test]
fn stage_ops_interleaves_with_commit_ops_deferred() {
    let (_d, db) = mk_deferred_apply_db(true);

    // Stage at lba=5, then deferred-commit at lba=6.
    let staged = l2p_value_with_pba(0x1111).with_seq(7);
    let committed = l2p_value_with_pba(0x2222).with_seq(7);
    let (lsn_a, _) = db
        .stage_ops(&[WalOp::L2pPut {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            lba: 5,
            value: staged,
        }])
        .unwrap();
    let (lsn_b, h) = db
        .commit_ops_deferred(&[WalOp::L2pPut {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            lba: 6,
            value: committed,
        }])
        .unwrap();
    db.test_force_compact_pass();
    let _ = h.recv().unwrap();
    assert!(lsn_b > lsn_a, "later commit must get a higher LSN");

    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 5).unwrap(), Some(staged));
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 6).unwrap(), Some(committed));
}

/// stage_ops respects the L2pValue.seq guard: a later stage with a
/// stale seq is rejected, leaving the on-disk value unchanged. This is
/// what carries idempotency under onyx LV2 buffer replay (a re-staged
/// op with the same per-LBA monotonic seq is silently dropped).
#[test]
fn stage_ops_seq_guard_rejects_stale_replay() {
    let (_d, db) = mk_deferred_apply_db(true);

    // Seed lba=42 with seq=10.
    let seeded = l2p_value_with_pba(0xAAAA).with_seq(10);
    let (_, _) = db
        .stage_ops(&[WalOp::L2pPut {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            lba: 42,
            value: seeded,
        }])
        .unwrap();
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 42).unwrap(), Some(seeded));

    // Replay with seq=5 → guard rejects, value stays at seeded.
    let stale = l2p_value_with_pba(0xBBBB).with_seq(5);
    let (_, outs) = db
        .stage_ops(&[WalOp::L2pRemap {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            lba: 42,
            new_value: stale,
            guard: None,
        }])
        .unwrap();
    assert_eq!(outs.len(), 1);
    match &outs[0] {
        ApplyOutcome::L2pRemap { applied, prev, .. } => {
            assert!(!*applied, "stale-seq remap must be rejected");
            assert_eq!(*prev, Some(seeded));
        }
        other => panic!("expected L2pRemap outcome, got {other:?}"),
    }
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 42).unwrap(), Some(seeded));
}

/// Concurrent stage_ops from many threads land with distinct LSNs and
/// every write is observable. Exercises the LsnAllocator + BfgGuard
/// monotonicity invariant under contention.
#[test]
fn stage_ops_concurrent_writers_all_visible() {
    use std::sync::Arc;
    use std::thread;

    let (_d, db) = mk_deferred_apply_db(true);
    let db = Arc::new(db);
    let n_threads = 8;
    let per_thread = 64;
    let handles: Vec<_> = (0..n_threads)
        .map(|t| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                let mut lsns = Vec::with_capacity(per_thread);
                for i in 0..per_thread {
                    let lba = (t as u64) * 10_000 + i as u64;
                    let val = l2p_value_with_pba(0x10_000 + lba);
                    let (lsn, _) = db
                        .stage_ops(&[WalOp::L2pPut {
                            vol_ord: BOOTSTRAP_VOLUME_ORD,
                            lba,
                            value: val,
                        }])
                        .unwrap();
                    lsns.push(lsn);
                }
                lsns
            })
        })
        .collect();
    let mut all_lsns: Vec<u64> = Vec::new();
    for h in handles {
        all_lsns.extend(h.join().unwrap());
    }
    // Every assigned LSN must be unique (LsnAllocator under-mutex
    // increment).
    let unique: std::collections::HashSet<u64> = all_lsns.iter().copied().collect();
    assert_eq!(unique.len(), all_lsns.len(), "LSNs must be unique");

    // Every write must be observable post-stage.
    for t in 0..n_threads {
        for i in 0..per_thread {
            let lba = (t as u64) * 10_000 + i as u64;
            assert_eq!(
                db.get(BOOTSTRAP_VOLUME_ORD, lba).unwrap(),
                Some(l2p_value_with_pba(0x10_000 + lba)),
                "lost stage for (t={t}, i={i}, lba={lba})"
            );
        }
    }
}

/// stage_ops uses lane dispatch, but every reservation must be retired before
/// it returns. Otherwise a later commit would block forever on
/// `mark_wal_durable_and_wait_for_dispatch`.
#[test]
fn stage_ops_retires_dispatch_intent_before_return() {
    let (_d, db) = mk_deferred_apply_db(true);
    let ops: Vec<WalOp> = (0..16u64)
        .map(|i| WalOp::L2pPut {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            lba: 7_000 + i,
            value: l2p_value_with_pba(0x9000 + i),
        })
        .collect();
    let _ = db.stage_ops(&ops).unwrap();
    let pending = db.dispatch_state.lock().pending.len();
    assert_eq!(
        pending, 0,
        "stage_ops must retire dispatch reservations before returning"
    );

    // Sanity: a subsequent commit_ops_deferred call completes without
    // hanging on dispatch.
    let (_, h) = db
        .commit_ops_deferred(&[WalOp::L2pPut {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            lba: 7_999,
            value: l2p_value_with_pba(0xDEAD),
        }])
        .unwrap();
    db.test_force_compact_pass();
    let outs = h.recv().unwrap();
    assert_eq!(outs.len(), 1);
}

/// stage_ops + force flush + reopen: the value persists through a
/// metadb checkpoint cycle. This is the durability handover from the
/// onyx LV2 buffer (which the caller would have already fsynced)
/// to the metadb on-disk tree.
#[test]
fn stage_ops_persists_across_flush_and_reopen() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = crate::config::Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    cfg.commit_direct_apply_enabled = true;
    cfg.commit_deferred_outcomes_enabled = true;
    let db = Db::create_with_config(cfg).unwrap();

    let val = l2p_value_with_pba(0xC0DE_C0DE).with_seq(1);
    let (_, _) = db
        .stage_ops(&[WalOp::L2pPut {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            lba: 99,
            value: val,
        }])
        .unwrap();
    db.flush().unwrap();
    drop(db);

    let cfg2 = crate::config::Config::new(dir.path());
    let db2 = Db::open_with_config(cfg2).unwrap();
    assert_eq!(db2.get(BOOTSTRAP_VOLUME_ORD, 99).unwrap(), Some(val));
}

/// Deferred mode delivers the L2pRemapRange merged outcome shape
/// correctly (cross-shard fan-in via `merge_l2p_outcome`).
#[test]
fn commit_ops_deferred_remap_range_round_trip() {
    let (_d, db) = mk_deferred_apply_db(true);
    let start = 0u64;
    let values: Box<[L2pValue]> = (0..64u64).map(|i| l2p_value_with_pba(0xF000 + i)).collect();
    let (_lsn, handle) = db
        .commit_ops_deferred(&[WalOp::L2pRemapRange {
            vol_ord: BOOTSTRAP_VOLUME_ORD,
            start_lba: start,
            values,
        }])
        .unwrap();
    db.test_force_compact_pass();
    let outs = handle.recv().unwrap();
    assert_eq!(outs.len(), 1);
    match &outs[0] {
        ApplyOutcome::L2pRemapRange {
            applied,
            prevs,
            freed_pbas,
        } => {
            assert_eq!(applied.len(), 64);
            assert_eq!(prevs.len(), 64);
            // All fresh writes: every applied bit is true.
            for (i, bit) in applied.iter().enumerate() {
                assert!(*bit, "fresh remap-range write at off {i} must apply");
            }
            // default: freed_pbas always empty (Lineage GC
            // owns freed-PBA delivery now).
            assert!(freed_pbas.is_empty());
        }
        other => panic!("expected L2pRemapRange outcome, got {other:?}"),
    }
}
