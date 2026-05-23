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
// Pre-Phase-5 the planner did `rc_enqueued.fill(true)` on any remap,
// claiming all 16 refcount lanes regardless of whether any op actually
// touched rc. With Phase 5 making L2pRemap/L2pRemapRange rc-neutral on
// the hot path, that blanket was the dominant serialization cause for
// concurrent commit_workers on a single volume — every commit's
// footprint conflicted with every other commit on the rc lanes.
//
// These tests pin the precise contract:
//   - L2pRemapRange (always unguarded, Phase 5 rc-neutral): zero rc lanes
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
        "L2pRemapRange is always unguarded + Phase 5 rc-neutral; must not claim any rc lane"
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
        "Unguarded L2pRemap is Phase 5 rc-neutral; must not claim any rc lane"
    );
}

#[test]
fn guarded_remap_adds_only_guard_pba_rc_shard() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let guard_pba: Pba = 0xABCDE;
    let expected_sid =
        super::lanes::rc_shard_of_pba(guard_pba, db.refcount_shards.len());

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
fn two_disjoint_l2p_remap_range_commits_do_not_conflict() {
    let dir = tempfile::TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();

    // Build two L2pRemapRange ops whose LBA spans hash to different L2P
    // shards. With Phase 5 making rc-neutral and Component A removing
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
        let values: Box<[L2pValue]> = (0..32u64).map(|i| l2p_value_with_pba(start_lba + i)).collect();
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
