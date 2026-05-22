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
