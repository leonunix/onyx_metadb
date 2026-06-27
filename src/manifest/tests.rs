use super::*;
use crate::config::PAGE_SIZE;
use crate::page_store::PageStore;
use crate::testing::faults::FaultAction;
use tempfile::TempDir;

fn mk_store(dir: &TempDir) -> Arc<PageStore> {
    Arc::new(PageStore::create(dir.path().join("pages.onyx_meta")).unwrap())
}

fn reopen(dir: &TempDir) -> Arc<PageStore> {
    Arc::new(PageStore::open(dir.path().join("pages.onyx_meta")).unwrap())
}

fn bx(v: &[PageId]) -> Box<[PageId]> {
    v.to_vec().into_boxed_slice()
}

fn one_shard(v: &[PageId]) -> Box<[Box<[PageId]>]> {
    vec![bx(v)].into_boxed_slice()
}

fn snap(
    ps: &PageStore,
    id: SnapshotId,
    vol_ord: VolumeOrdinal,
    l2p: &[PageId],
    lsn: Lsn,
) -> SnapshotEntry {
    let l2p_page = write_snapshot_roots_page(ps, l2p, lsn).unwrap();
    SnapshotEntry {
        id,
        vol_ord,
        l2p_roots_page: l2p_page,
        created_lsn: lsn,
        l2p_shard_roots: bx(l2p),
        page_dead_list_tail_pid: NULL_PAGE,
        capture_watermark: lsn,
    }
}

fn boot_vol(shard_count: u32, roots: &[PageId]) -> VolumeEntry {
    boot_vol_at(shard_count, roots, 0)
}

/// `boot_vol` variant that stamps every per-shard durable_seq slot to
/// `checkpoint_lsn`, satisfying [`Manifest::assert_durable_seq_invariant`]
/// when the surrounding manifest's `checkpoint_lsn` is non-zero.
fn boot_vol_at(shard_count: u32, roots: &[PageId], checkpoint_lsn: Lsn) -> VolumeEntry {
    VolumeEntry {
        ord: 0,
        shard_count,
        l2p_shard_roots: bx(roots),
        l2p_shard_durable_seq: vec![checkpoint_lsn; shard_count as usize].into_boxed_slice(),
        created_lsn: 0,
        flags: 0,
        dead_list_head_pid: NULL_PAGE,
        dead_list_tail_pid: NULL_PAGE,
        parent_vol_ord: None,
        branched_at_lsn: 0,
        promotion_cursor: None,
        page_dead_list_head_pid: NULL_PAGE,
        page_dead_list_tail_pid: NULL_PAGE,
        page_live_list_head_pid: NULL_PAGE,
        page_live_list_tail_pid: NULL_PAGE,
        promoted_log_head_pid: NULL_PAGE,
        promoted_log_tail_pid: NULL_PAGE,
    }
}

#[test]
fn fresh_open_creates_empty_manifest_at_sequence_1() {
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let faults = FaultController::new();
    let (store, manifest) = ManifestStore::open_or_create(ps, faults).unwrap();
    assert_eq!(manifest, Manifest::empty());
    assert_eq!(store.sequence(), 1);
    assert_eq!(store.next_slot(), MANIFEST_PAGE_B);
}

#[test]
fn commit_then_reopen_recovers_manifest() {
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let faults = FaultController::new();
    let (mut store, _) = ManifestStore::open_or_create(ps.clone(), faults).unwrap();

    let m = Manifest {
        body_version: MANIFEST_BODY_VERSION,
        checkpoint_lsn: 1234,
        checkpoint_txg: 0,
        last_processed_buffer_seq: 0,
        lifecycle_replay_seq: 0,
        free_list_head: 99,
        refcount_shard_roots: bx(&[17, 18, 19, 20]),
        refcount_durable_seq: bx(&[1234, 1234, 1234, 1234]),
        dedup_shards: 1,
        dedup_index_shard_heads: one_shard(&[NULL_PAGE, NULL_PAGE]),
        next_snapshot_id: 5,
        next_volume_ord: 1,
        snapshots: vec![snap(&ps, 1, 0, &[11, 12, 13, 14], 100)],
        volumes: vec![boot_vol_at(4, &[7, 8, 9, 10], 1234)],
    };
    store.commit(&m).unwrap();
    drop(store);

    let ps2 = reopen(&dir);
    let (store2, loaded) = ManifestStore::open_or_create(ps2, FaultController::new()).unwrap();
    assert_eq!(loaded, m);
    assert_eq!(store2.sequence(), 2);
}

#[test]
fn commits_alternate_slots() {
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let faults = FaultController::new();
    let (mut store, _) = ManifestStore::open_or_create(ps, faults).unwrap();
    let m = Manifest::empty();
    for expected_next in [
        MANIFEST_PAGE_A,
        MANIFEST_PAGE_B,
        MANIFEST_PAGE_A,
        MANIFEST_PAGE_B,
    ] {
        store.commit(&m).unwrap();
        assert_eq!(store.next_slot(), expected_next);
    }
    assert_eq!(store.sequence(), 5);
}

#[test]
fn higher_sequence_wins_on_open() {
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let (mut store, _) = ManifestStore::open_or_create(ps, FaultController::new()).unwrap();

    for lsn in [1u64, 2, 3, 4, 5] {
        let mut m = Manifest::empty();
        m.checkpoint_lsn = lsn;
        store.commit(&m).unwrap();
    }
    drop(store);

    let (_, loaded) = ManifestStore::open_or_create(reopen(&dir), FaultController::new()).unwrap();
    assert_eq!(loaded.checkpoint_lsn, 5);
}

#[test]
fn corrupt_slot_a_falls_back_to_slot_b() {
    let dir = TempDir::new().unwrap();
    let page_path = dir.path().join("pages.onyx_meta");
    let ps = mk_store(&dir);

    let faults = FaultController::new();
    let (mut store, _) = ManifestStore::open_or_create(ps, faults).unwrap();
    let mut target = Manifest::empty();
    target.checkpoint_lsn = 777;
    store.commit(&target).unwrap();
    drop(store);

    {
        use std::os::unix::fs::FileExt;
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&page_path)
            .unwrap();
        let off = MANIFEST_PAGE_B * PAGE_SIZE as u64 + 500;
        f.write_all_at(&[0xFF], off).unwrap();
        f.sync_all().unwrap();
    }

    let (store2, loaded) =
        ManifestStore::open_or_create(reopen(&dir), FaultController::new()).unwrap();
    assert_eq!(loaded, Manifest::empty());
    assert_eq!(store2.sequence(), 1);
    assert_eq!(store2.next_slot(), MANIFEST_PAGE_B);
}

#[test]
fn both_slots_corrupt_rewrites_fresh_empty() {
    let dir = TempDir::new().unwrap();
    let page_path = dir.path().join("pages.onyx_meta");
    {
        let ps = mk_store(&dir);
        let (_, _) = ManifestStore::open_or_create(ps, FaultController::new()).unwrap();
    }
    {
        use std::os::unix::fs::FileExt;
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(&page_path)
            .unwrap();
        f.write_all_at(&[0xFFu8; 8192], 0).unwrap();
        f.sync_all().unwrap();
    }
    let (store, manifest) =
        ManifestStore::open_or_create(reopen(&dir), FaultController::new()).unwrap();
    assert_eq!(manifest, Manifest::empty());
    assert_eq!(store.sequence(), 1);
}

#[test]
fn fsync_before_error_does_not_advance_state() {
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let faults = FaultController::new();
    let (mut store, _) = ManifestStore::open_or_create(ps, faults.clone()).unwrap();
    let start_seq = store.sequence();
    let start_slot = store.next_slot();

    faults.install(FaultPoint::ManifestFsyncBefore, 1, FaultAction::Error);
    let mut m = Manifest::empty();
    m.checkpoint_lsn = 42;
    assert!(store.commit(&m).is_err());
    assert_eq!(store.sequence(), start_seq);
    assert_eq!(store.next_slot(), start_slot);
}

#[test]
fn fsync_after_error_durably_wrote_but_callers_sees_err() {
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let faults = FaultController::new();
    let (mut store, _) = ManifestStore::open_or_create(ps, faults.clone()).unwrap();

    faults.install(FaultPoint::ManifestFsyncAfter, 1, FaultAction::Error);
    let mut m = Manifest::empty();
    m.checkpoint_lsn = 42;
    assert!(store.commit(&m).is_err());
    drop(store);

    let (_, loaded) = ManifestStore::open_or_create(reopen(&dir), FaultController::new()).unwrap();
    assert_eq!(loaded, m);
}

#[test]
fn body_decode_rejects_wrong_version() {
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
    let p = page.payload_mut();
    p[0..4].copy_from_slice(&99u32.to_le_bytes());
    page.seal();
    assert!(matches!(
        Manifest::decode(&page, &ps).unwrap_err(),
        MetaDbError::Corruption(_)
    ));
}

#[test]
fn encode_decode_round_trip_with_refcount_and_dedup() {
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let m = Manifest {
        body_version: MANIFEST_BODY_VERSION,
        checkpoint_lsn: 0xDEAD_BEEF_CAFE,
        checkpoint_txg: 0,
        last_processed_buffer_seq: 0,
        lifecycle_replay_seq: 0,
        free_list_head: 1234,
        refcount_shard_roots: bx(&[142, 143, 144, 145]),
        refcount_durable_seq: bx(&[
            0xDEAD_BEEF_CAFE,
            0xDEAD_BEEF_CAFE,
            0xDEAD_BEEF_CAFE,
            0xDEAD_BEEF_CAFE,
        ]),
        dedup_shards: 1,
        dedup_index_shard_heads: one_shard(&[NULL_PAGE, 200, 300]),
        next_snapshot_id: 99,
        next_volume_ord: 1,
        snapshots: vec![
            snap(&ps, 1, 0, &[10, 11, 12, 13], 100),
            snap(&ps, 5, 0, &[20, 21, 22, 23], 500),
        ],
        volumes: vec![boot_vol_at(4, &[42, 43, 44, 45], 0xDEAD_BEEF_CAFE)],
    };
    let mut page = Page::new(PageHeader::new(PageType::Manifest, 7));
    m.encode(&mut page).unwrap();
    page.seal();
    page.verify(MANIFEST_PAGE_A).unwrap();
    let decoded = Manifest::decode(&page, &ps).unwrap();
    assert_eq!(decoded, m);
}

#[test]
fn encode_rejects_oversized_snapshot_table() {
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let mut m = Manifest::empty();
    m.refcount_shard_roots = bx(&[1, 2, 3, 4]);
    m.refcount_durable_seq = bx(&[0, 0, 0, 0]);
    let cap = max_snapshots_for_shards(m.shard_count());
    assert!(cap > 0);
    for i in 0..(cap + 1) as u64 {
        m.snapshots.push(snap(&ps, i, 0, &[10, 11, 12, 13], i));
    }
    let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
    assert!(m.encode(&mut page).is_err());
}

#[test]
fn find_snapshot_locates_by_id() {
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let mut m = Manifest::empty();
    m.refcount_shard_roots = bx(&[1, 2, 3, 4]);
    m.refcount_durable_seq = bx(&[0, 0, 0, 0]);
    m.snapshots.push(snap(&ps, 7, 0, &[42, 43, 44, 45], 100));
    assert_eq!(m.find_snapshot(7).unwrap().id, 7);
    assert_eq!(
        m.find_snapshot(7).unwrap().l2p_shard_roots.as_ref(),
        &[42, 43, 44, 45]
    );
    assert!(m.find_snapshot(99).is_none());
}

#[test]
fn decode_rejects_pre_v6_body_versions() {
    // v5 and v4 are no longer supported — Phase 7 is fresh-install
    // only. Any body_version other than v6 reports `Corruption`.
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    for bad_version in [3u32, 4, 5] {
        let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
        {
            let p = page.payload_mut();
            p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4].copy_from_slice(&bad_version.to_le_bytes());
        }
        page.seal();
        match Manifest::decode(&page, &ps).unwrap_err() {
            MetaDbError::Corruption(msg) => {
                assert!(msg.contains("unsupported manifest body version"), "{msg}");
            }
            e => panic!("expected corruption, got {e}"),
        }
    }
}

#[test]
fn v6_volumes_table_round_trip() {
    // Exercise multi-volume encode/decode with non-zero ords and a
    // drop-pending flag so any per-entry alignment slip would show.
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let m = Manifest {
        body_version: MANIFEST_BODY_VERSION,
        checkpoint_lsn: 10,
        checkpoint_txg: 0,
        last_processed_buffer_seq: 0,
        lifecycle_replay_seq: 0,
        free_list_head: NULL_PAGE,
        refcount_shard_roots: bx(&[50, 51]),
        refcount_durable_seq: bx(&[10, 10]),
        dedup_shards: 1,
        dedup_index_shard_heads: one_shard(&[]),
        next_snapshot_id: 2,
        next_volume_ord: 3,
        snapshots: vec![snap(&ps, 1, 2, &[100, 101], 10)],
        volumes: vec![
            boot_vol_at(2, &[200, 201], 10),
            VolumeEntry {
                ord: 2,
                shard_count: 2,
                l2p_shard_roots: bx(&[300, 301]),
                l2p_shard_durable_seq: bx(&[10, 10]),
                created_lsn: 7,
                flags: VOLUME_FLAG_DROP_PENDING,
                dead_list_head_pid: NULL_PAGE,
                dead_list_tail_pid: NULL_PAGE,
                parent_vol_ord: None,
                branched_at_lsn: 0,
                promotion_cursor: None,
                page_dead_list_head_pid: NULL_PAGE,
                page_dead_list_tail_pid: NULL_PAGE,
                page_live_list_head_pid: NULL_PAGE,
                page_live_list_tail_pid: NULL_PAGE,
                promoted_log_head_pid: NULL_PAGE,
                promoted_log_tail_pid: NULL_PAGE,
            },
        ],
    };
    let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
    m.encode(&mut page).unwrap();
    page.seal();
    let decoded = Manifest::decode(&page, &ps).unwrap();
    assert_eq!(decoded, m);
}

#[test]
fn v6_rejects_volume_count_exceeding_capacity() {
    // Cram the volume table with entries until encode has to
    // complain. Uses tiny volumes (shard_count = 1) so the failure
    // comes from the snapshot-table capacity check, not from an
    // inline-volume-entry budget mismatch.
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let mut m = Manifest::empty();
    m.refcount_shard_roots = bx(&[1]);
    m.refcount_durable_seq = bx(&[0]);
    // Start with just the bootstrap — this sets the baseline volume budget.
    m.volumes.push(boot_vol(1, &[10]));
    let baseline_budget: usize = m
        .volumes
        .iter()
        .map(|v| volume_entry_inline_size(v.shard_count as usize))
        .sum();
    // The current cuckoo/paged-reverse layout stores one dedup meta-head
    // group per index, independent of dedup_shards/apply-lane count.
    let cap = max_snapshots_for_layout(m.shard_count(), 1, 0, baseline_budget);
    for i in 0..(cap + 1) as u64 {
        m.snapshots.push(snap(&ps, i, 0, &[10], i));
    }
    let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
    assert!(m.encode(&mut page).is_err());
}

#[test]
fn dedup_n4_encode_decode_round_trip() {
    // Cuckoo dedup_index stores one meta-head group even when
    // dedup_shards = 4 keeps four apply lanes.
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let m = Manifest {
        body_version: MANIFEST_BODY_VERSION,
        checkpoint_lsn: 100,
        checkpoint_txg: 0,
        last_processed_buffer_seq: 0,
        lifecycle_replay_seq: 0,
        free_list_head: NULL_PAGE,
        refcount_shard_roots: bx(&[1, 2]),
        refcount_durable_seq: bx(&[100, 100]),
        dedup_shards: 4,
        dedup_index_shard_heads: one_shard(&[10]),
        next_snapshot_id: 1,
        next_volume_ord: 1,
        snapshots: Vec::new(),
        volumes: vec![boot_vol_at(2, &[100, 101], 100)],
    };
    let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
    m.encode(&mut page).unwrap();
    page.seal();
    let decoded = Manifest::decode(&page, &ps).unwrap();
    assert_eq!(decoded, m);
}

#[test]
fn dedup_encode_rejects_non_power_of_two_shards() {
    let dir = TempDir::new().unwrap();
    let _ps = mk_store(&dir);
    let mut m = Manifest::empty();
    m.refcount_shard_roots = bx(&[1, 2]);
    m.refcount_durable_seq = bx(&[0, 0]);
    m.volumes = vec![boot_vol(2, &[100, 101])];
    m.dedup_shards = 3;
    let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
    let err = m.encode(&mut page).unwrap_err();
    match err {
        MetaDbError::InvalidArgument(msg) => {
            assert!(msg.contains("power of two"), "{msg}");
        }
        e => panic!("expected InvalidArgument, got {e}"),
    }
}

#[test]
fn dedup_encode_rejects_meta_head_outer_length_mismatch() {
    let dir = TempDir::new().unwrap();
    let _ps = mk_store(&dir);
    let mut m = Manifest::empty();
    m.refcount_shard_roots = bx(&[1, 2]);
    m.refcount_durable_seq = bx(&[0, 0]);
    m.volumes = vec![boot_vol(2, &[100, 101])];
    m.dedup_shards = 4;
    // Cuckoo uses one meta-head group regardless of
    // dedup_shards/apply-lane count.
    m.dedup_index_shard_heads = vec![bx(&[]); 2].into_boxed_slice();
    let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
    let err = m.encode(&mut page).unwrap_err();
    match err {
        MetaDbError::InvalidArgument(msg) => {
            assert!(msg.contains("dedup meta-head outer length"), "{msg}");
        }
        e => panic!("expected InvalidArgument, got {e}"),
    }
}

// `decode_v7_body_opens_as_dedup_shards_one` retired alongside the
// v7/v8 → v9 schema break: v7/v8 carried the now-retired
// `dedup_reverse_shard_heads`, and v9 hard-rejects both versions.

#[test]
fn volume_entry_inline_round_trip() {
    let entry = VolumeEntry {
        ord: 42,
        shard_count: 4,
        l2p_shard_roots: bx(&[100, 101, 102, 103]),
        l2p_shard_durable_seq: bx(&[10, 11, 12, 13]),
        created_lsn: 0xABCD_1234,
        flags: VOLUME_FLAG_DROP_PENDING,
        dead_list_head_pid: NULL_PAGE,
        dead_list_tail_pid: NULL_PAGE,
        parent_vol_ord: None,
        branched_at_lsn: 0,
        promotion_cursor: None,
        page_dead_list_head_pid: NULL_PAGE,
        page_dead_list_tail_pid: NULL_PAGE,
        page_live_list_head_pid: NULL_PAGE,
        page_live_list_tail_pid: NULL_PAGE,
        promoted_log_head_pid: NULL_PAGE,
        promoted_log_tail_pid: NULL_PAGE,
    };
    let mut buf = vec![0u8; volume_entry_inline_size(entry.shard_count as usize)];
    let mut off = 0;
    encode_volume_entry_inline(&entry, &mut buf, &mut off).unwrap();
    assert_eq!(off, buf.len());
    let mut off = 0;
    let decoded = decode_volume_entry_inline(&buf, &mut off, MANIFEST_BODY_VERSION).unwrap();
    assert_eq!(decoded, entry);
    assert_eq!(off, buf.len());
}

#[test]
fn volume_entry_inline_rejects_shard_count_mismatch() {
    let entry = VolumeEntry {
        ord: 1,
        shard_count: 2,
        l2p_shard_roots: bx(&[7]), // length 1, but shard_count 2
        l2p_shard_durable_seq: bx(&[0, 0]),
        created_lsn: 10,
        flags: 0,
        dead_list_head_pid: NULL_PAGE,
        dead_list_tail_pid: NULL_PAGE,
        parent_vol_ord: None,
        branched_at_lsn: 0,
        promotion_cursor: None,
        page_dead_list_head_pid: NULL_PAGE,
        page_dead_list_tail_pid: NULL_PAGE,
        page_live_list_head_pid: NULL_PAGE,
        page_live_list_tail_pid: NULL_PAGE,
        promoted_log_head_pid: NULL_PAGE,
        promoted_log_tail_pid: NULL_PAGE,
    };
    let mut buf = vec![0u8; 256];
    let mut off = 0;
    assert!(matches!(
        encode_volume_entry_inline(&entry, &mut buf, &mut off),
        Err(MetaDbError::InvalidArgument(_))
    ));
}

#[test]
fn volume_entry_inline_rejects_buffer_too_small() {
    let entry = VolumeEntry {
        ord: 9,
        shard_count: 16,
        l2p_shard_roots: bx(&[1; 16]),
        l2p_shard_durable_seq: bx(&[0; 16]),
        created_lsn: 0,
        flags: 0,
        dead_list_head_pid: NULL_PAGE,
        dead_list_tail_pid: NULL_PAGE,
        parent_vol_ord: None,
        branched_at_lsn: 0,
        promotion_cursor: None,
        page_dead_list_head_pid: NULL_PAGE,
        page_dead_list_tail_pid: NULL_PAGE,
        page_live_list_head_pid: NULL_PAGE,
        page_live_list_tail_pid: NULL_PAGE,
        promoted_log_head_pid: NULL_PAGE,
        promoted_log_tail_pid: NULL_PAGE,
    };
    let mut buf = vec![0u8; VOLUME_ENTRY_FIXED_SIZE + 8]; // one root worth
    let mut off = 0;
    assert!(matches!(
        encode_volume_entry_inline(&entry, &mut buf, &mut off),
        Err(MetaDbError::InvalidArgument(_))
    ));
}

#[test]
fn volume_entry_decode_rejects_truncated_roots() {
    // Encode a legit entry, then lop the final durable_seq off the buffer.
    let entry = VolumeEntry {
        ord: 3,
        shard_count: 3,
        l2p_shard_roots: bx(&[11, 22, 33]),
        l2p_shard_durable_seq: bx(&[1, 2, 3]),
        created_lsn: 7,
        flags: 0,
        dead_list_head_pid: NULL_PAGE,
        dead_list_tail_pid: NULL_PAGE,
        parent_vol_ord: None,
        branched_at_lsn: 0,
        promotion_cursor: None,
        page_dead_list_head_pid: NULL_PAGE,
        page_dead_list_tail_pid: NULL_PAGE,
        page_live_list_head_pid: NULL_PAGE,
        page_live_list_tail_pid: NULL_PAGE,
        promoted_log_head_pid: NULL_PAGE,
        promoted_log_tail_pid: NULL_PAGE,
    };
    let mut buf = vec![0u8; volume_entry_inline_size(3)];
    let mut off = 0;
    encode_volume_entry_inline(&entry, &mut buf, &mut off).unwrap();
    buf.truncate(buf.len() - 8);
    let mut off = 0;
    assert!(matches!(
        decode_volume_entry_inline(&buf, &mut off, MANIFEST_BODY_VERSION),
        Err(MetaDbError::Corruption(_))
    ));
}

#[test]
fn volume_entry_many_back_to_back() {
    // Write several entries contiguously into a single buffer, decode
    // them all, verify the sliding offset + round-trip equality.
    let entries = vec![
        VolumeEntry {
            ord: 0,
            shard_count: 2,
            l2p_shard_roots: bx(&[10, 11]),
            l2p_shard_durable_seq: bx(&[1, 2]),
            created_lsn: 100,
            flags: 0,
            dead_list_head_pid: NULL_PAGE,
            dead_list_tail_pid: NULL_PAGE,
            parent_vol_ord: None,
            branched_at_lsn: 0,
            promotion_cursor: None,
            page_dead_list_head_pid: NULL_PAGE,
            page_dead_list_tail_pid: NULL_PAGE,
            page_live_list_head_pid: NULL_PAGE,
            page_live_list_tail_pid: NULL_PAGE,
            promoted_log_head_pid: NULL_PAGE,
            promoted_log_tail_pid: NULL_PAGE,
        },
        VolumeEntry {
            ord: 1,
            shard_count: 4,
            l2p_shard_roots: bx(&[20, 21, 22, 23]),
            l2p_shard_durable_seq: bx(&[3, 4, 5, 6]),
            created_lsn: 200,
            flags: VOLUME_FLAG_DROP_PENDING,
            dead_list_head_pid: NULL_PAGE,
            dead_list_tail_pid: NULL_PAGE,
            parent_vol_ord: None,
            branched_at_lsn: 0,
            promotion_cursor: None,
            page_dead_list_head_pid: NULL_PAGE,
            page_dead_list_tail_pid: NULL_PAGE,
            page_live_list_head_pid: NULL_PAGE,
            page_live_list_tail_pid: NULL_PAGE,
            promoted_log_head_pid: NULL_PAGE,
            promoted_log_tail_pid: NULL_PAGE,
        },
        VolumeEntry {
            ord: 65534,
            shard_count: 1,
            l2p_shard_roots: bx(&[NULL_PAGE]),
            l2p_shard_durable_seq: bx(&[7]),
            created_lsn: 300,
            flags: 0,
            dead_list_head_pid: NULL_PAGE,
            dead_list_tail_pid: NULL_PAGE,
            parent_vol_ord: None,
            branched_at_lsn: 0,
            promotion_cursor: None,
            page_dead_list_head_pid: NULL_PAGE,
            page_dead_list_tail_pid: NULL_PAGE,
            page_live_list_head_pid: NULL_PAGE,
            page_live_list_tail_pid: NULL_PAGE,
            promoted_log_head_pid: NULL_PAGE,
            promoted_log_tail_pid: NULL_PAGE,
        },
    ];
    let total: usize = entries
        .iter()
        .map(|e| volume_entry_inline_size(e.shard_count as usize))
        .sum();
    let mut buf = vec![0u8; total];
    let mut off = 0;
    for entry in &entries {
        encode_volume_entry_inline(entry, &mut buf, &mut off).unwrap();
    }
    assert_eq!(off, total);
    let mut off = 0;
    for expected in &entries {
        let got = decode_volume_entry_inline(&buf, &mut off, MANIFEST_BODY_VERSION).unwrap();
        assert_eq!(&got, expected);
    }
    assert_eq!(off, total);
}

// ── Tier 2.B Stage 1: per-shard durable_seq[] tests ─────────────────

/// Diverging per-shard `durable_seq` values must round-trip through
/// v11 encode/decode. `checkpoint_lsn` is set to `min(per-shard)` so
/// the `assert_durable_seq_invariant` tripwire stays satisfied.
#[test]
fn v11_per_shard_durable_seq_round_trip() {
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    // Refcount shards have diverging durable_seq values 5, 7, 6, 9.
    // Bootstrap volume's L2P shards: 8, 5, 11, 6. min = 5.
    let m = Manifest {
        body_version: MANIFEST_BODY_VERSION,
        checkpoint_lsn: 5,
        checkpoint_txg: 0,
        last_processed_buffer_seq: 0,
        lifecycle_replay_seq: 0,
        free_list_head: NULL_PAGE,
        refcount_shard_roots: bx(&[10, 11, 12, 13]),
        refcount_durable_seq: bx(&[5, 7, 6, 9]),
        dedup_shards: 1,
        dedup_index_shard_heads: one_shard(&[]),
        next_snapshot_id: 1,
        next_volume_ord: 1,
        snapshots: Vec::new(),
        volumes: vec![VolumeEntry {
            ord: 0,
            shard_count: 4,
            l2p_shard_roots: bx(&[20, 21, 22, 23]),
            l2p_shard_durable_seq: bx(&[8, 5, 11, 6]),
            created_lsn: 0,
            flags: 0,
            dead_list_head_pid: NULL_PAGE,
            dead_list_tail_pid: NULL_PAGE,
            parent_vol_ord: None,
            branched_at_lsn: 0,
            promotion_cursor: None,
            page_dead_list_head_pid: NULL_PAGE,
            page_dead_list_tail_pid: NULL_PAGE,
            page_live_list_head_pid: NULL_PAGE,
            page_live_list_tail_pid: NULL_PAGE,
            promoted_log_head_pid: NULL_PAGE,
            promoted_log_tail_pid: NULL_PAGE,
        }],
    };
    let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
    m.encode(&mut page).unwrap();
    page.seal();
    let decoded = Manifest::decode(&page, &ps).unwrap();
    assert_eq!(decoded, m);
    assert_eq!(decoded.refcount_durable_seq.as_ref(), &[5, 7, 6, 9]);
    assert_eq!(
        decoded.volumes[0].l2p_shard_durable_seq.as_ref(),
        &[8, 5, 11, 6]
    );
}

/// Encoding refuses to commit a manifest whose `min(durable_seq[]) !=
/// checkpoint_lsn`. The probe encoder catches drift before the page
/// hits disk, so the soak gate fails fast on any consumer that
/// forgets to keep the two in sync.
#[test]
fn encode_rejects_durable_seq_drift_from_checkpoint_lsn() {
    let dir = TempDir::new().unwrap();
    let _ps = mk_store(&dir);
    let m = Manifest {
        body_version: MANIFEST_BODY_VERSION,
        checkpoint_lsn: 42,
        checkpoint_txg: 0,
        last_processed_buffer_seq: 0,
        lifecycle_replay_seq: 0,
        free_list_head: NULL_PAGE,
        refcount_shard_roots: bx(&[1, 2]),
        // Intentionally lower than checkpoint_lsn — drift the
        // invariant should catch.
        refcount_durable_seq: bx(&[42, 7]),
        // Consistent (>= checkpoint_lsn) so the drift surfaces from
        // refcount, not from page-rc.
        dedup_shards: 1,
        dedup_index_shard_heads: one_shard(&[]),
        next_snapshot_id: 1,
        next_volume_ord: 1,
        snapshots: Vec::new(),
        volumes: vec![boot_vol_at(2, &[3, 4], 42)],
    };
    let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
    let err = m.encode(&mut page).unwrap_err();
    match err {
        MetaDbError::Corruption(msg) => {
            assert!(msg.contains("durable_seq invariant broken"), "{msg}");
        }
        e => panic!("expected Corruption, got {e}"),
    }
}

/// Encoding also rejects a length mismatch between
/// `refcount_shard_roots` and `refcount_durable_seq` — the two arrays
/// must stay paired.
#[test]
fn encode_rejects_refcount_durable_seq_length_mismatch() {
    let dir = TempDir::new().unwrap();
    let _ps = mk_store(&dir);
    let m = Manifest {
        body_version: MANIFEST_BODY_VERSION,
        checkpoint_lsn: 0,
        checkpoint_txg: 0,
        last_processed_buffer_seq: 0,
        lifecycle_replay_seq: 0,
        free_list_head: NULL_PAGE,
        refcount_shard_roots: bx(&[1, 2, 3]),
        refcount_durable_seq: bx(&[0, 0]), // wrong length
        // Consistent so the refcount length check is the one that fires.
        dedup_shards: 1,
        dedup_index_shard_heads: one_shard(&[]),
        next_snapshot_id: 1,
        next_volume_ord: 1,
        snapshots: Vec::new(),
        volumes: vec![boot_vol(2, &[10, 11])],
    };
    let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
    let err = m.encode(&mut page).unwrap_err();
    match err {
        MetaDbError::Corruption(msg) => {
            assert!(msg.contains("refcount_durable_seq length"), "{msg}");
        }
        e => panic!("expected Corruption, got {e}"),
    }
}

/// Hand-rolled v10 manifest encoder so we can verify the v10→v11
/// upgrade path without keeping the deprecated encoder in production.
/// Mirrors `Manifest::encode` minus the `refcount_durable_seq` tail
/// and minus each volume's `l2p_shard_durable_seq` tail.
fn encode_v10_for_test(m: &Manifest, page: &mut Page) {
    let p = page.payload_mut();
    p.fill(0);
    p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4].copy_from_slice(&10u32.to_le_bytes());
    p[OFF_CHECKPOINT_LSN..OFF_CHECKPOINT_LSN + 8]
        .copy_from_slice(&m.checkpoint_lsn.to_le_bytes());
    p[OFF_FREE_LIST_HEAD..OFF_FREE_LIST_HEAD + 8].copy_from_slice(&m.free_list_head.to_le_bytes());
    p[OFF_SHARD_COUNT..OFF_SHARD_COUNT + 4]
        .copy_from_slice(&(m.refcount_shard_roots.len() as u32).to_le_bytes());
    p[OFF_DEDUP_SHARDS..OFF_DEDUP_SHARDS + 4].copy_from_slice(&m.dedup_shards.to_le_bytes());
    p[OFF_NEXT_SNAPSHOT_ID..OFF_NEXT_SNAPSHOT_ID + 8]
        .copy_from_slice(&m.next_snapshot_id.to_le_bytes());
    p[OFF_NEXT_VOLUME_ORD..OFF_NEXT_VOLUME_ORD + 2]
        .copy_from_slice(&m.next_volume_ord.to_le_bytes());
    p[OFF_SNAPSHOT_COUNT..OFF_SNAPSHOT_COUNT + 4]
        .copy_from_slice(&(m.snapshots.len() as u32).to_le_bytes());
    p[OFF_VOLUME_COUNT..OFF_VOLUME_COUNT + 4]
        .copy_from_slice(&(m.volumes.len() as u32).to_le_bytes());
    let mut off = OFF_VARIABLE_START;
    for root in m.refcount_shard_roots.iter().copied() {
        p[off..off + 8].copy_from_slice(&root.to_le_bytes());
        off += 8;
    }
    // v10 omits refcount_durable_seq here.
    for shard_heads in m.dedup_index_shard_heads.iter() {
        p[off..off + 4].copy_from_slice(&(shard_heads.len() as u32).to_le_bytes());
        off += 4;
        for head in shard_heads.iter().copied() {
            p[off..off + 8].copy_from_slice(&head.to_le_bytes());
            off += 8;
        }
    }
    for entry in &m.snapshots {
        p[off..off + 8].copy_from_slice(&entry.id.to_le_bytes());
        p[off + 8..off + 10].copy_from_slice(&entry.vol_ord.to_le_bytes());
        p[off + 16..off + 24].copy_from_slice(&entry.l2p_roots_page.to_le_bytes());
        p[off + 24..off + 32].copy_from_slice(&entry.created_lsn.to_le_bytes());
        off += SNAPSHOT_ENTRY_SIZE;
    }
    // v10 volume entry inline: fixed header + roots tail, no
    // durable_seq tail.
    for entry in &m.volumes {
        p[off..off + 2].copy_from_slice(&entry.ord.to_le_bytes());
        p[off + 2..off + 6].copy_from_slice(&entry.shard_count.to_le_bytes());
        p[off + 6..off + 14].copy_from_slice(&entry.created_lsn.to_le_bytes());
        p[off + 14] = entry.flags;
        p[off + 15] = 0;
        off += VOLUME_ENTRY_FIXED_SIZE;
        for root in entry.l2p_shard_roots.iter().copied() {
            p[off..off + 8].copy_from_slice(&root.to_le_bytes());
            off += 8;
        }
    }
}

/// A v10 manifest (no on-disk per-shard durable_seq arrays) must
/// v10 manifests carry compact leaf v3 in the paged tree, which is
/// wire-incompatible with v4. The Phase 1 flag-day cutover (manifest
/// v12 ↔ leaf v4) hard-rejects v10/v11 instead of lazy-upgrading.
#[test]
fn v10_manifest_is_rejected_after_flag_day_to_v12() {
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let m = Manifest {
        body_version: MANIFEST_BODY_VERSION,
        checkpoint_lsn: 99,
        checkpoint_txg: 0,
        last_processed_buffer_seq: 0,
        lifecycle_replay_seq: 0,
        free_list_head: NULL_PAGE,
        refcount_shard_roots: bx(&[1, 2, 3]),
        refcount_durable_seq: bx(&[]),
        // v10 hand-encoder ignores these; present only to satisfy the
        // struct literal (this manifest is never validated by encode()).
        dedup_shards: 1,
        dedup_index_shard_heads: one_shard(&[]),
        next_snapshot_id: 1,
        next_volume_ord: 1,
        snapshots: Vec::new(),
        volumes: vec![VolumeEntry {
            ord: 0,
            shard_count: 2,
            l2p_shard_roots: bx(&[10, 11]),
            l2p_shard_durable_seq: bx(&[]),
            created_lsn: 0,
            flags: 0,
            dead_list_head_pid: NULL_PAGE,
            dead_list_tail_pid: NULL_PAGE,
            parent_vol_ord: None,
            branched_at_lsn: 0,
            promotion_cursor: None,
            page_dead_list_head_pid: NULL_PAGE,
            page_dead_list_tail_pid: NULL_PAGE,
            page_live_list_head_pid: NULL_PAGE,
            page_live_list_tail_pid: NULL_PAGE,
            promoted_log_head_pid: NULL_PAGE,
            promoted_log_tail_pid: NULL_PAGE,
        }],
    };
    let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
    encode_v10_for_test(&m, &mut page);
    page.seal();

    match Manifest::decode(&page, &ps).unwrap_err() {
        MetaDbError::Corruption(msg) => {
            assert!(
                msg.contains("body version") || msg.contains("v12"),
                "expected v10-rejection message, got: {msg}"
            );
        }
        e => panic!("expected Corruption from v10 manifest, got {e}"),
    }
}

// ── Phase 4 Step 1: lineage fields on VolumeEntry ───────────────────

/// A v14 `VolumeEntry` with a non-trivial lineage trio must round-trip
/// inline encode/decode without losing `parent_vol_ord` /
/// `branched_at_lsn` / `promotion_cursor`. Exercises every Option
/// variant for the two sentinel-encoded fields.
#[test]
fn v14_volume_entry_round_trip_carries_lineage_fields() {
    let entry = VolumeEntry {
        ord: 17,
        shard_count: 3,
        l2p_shard_roots: bx(&[100, 101, 102]),
        l2p_shard_durable_seq: bx(&[5, 6, 7]),
        created_lsn: 0xCAFE_BABE,
        flags: VOLUME_FLAG_DROP_PENDING,
        dead_list_head_pid: 0x1111_2222_3333_4444,
        dead_list_tail_pid: 0x5555_6666_7777_8888,
        parent_vol_ord: Some(9),
        branched_at_lsn: 0xDEAD_BEEF,
        promotion_cursor: Some(0x1234),
        page_dead_list_head_pid: 0x9999_AAAA_BBBB_CCCC,
        page_dead_list_tail_pid: 0xDDDD_EEEE_FFFF_0001,
        page_live_list_head_pid: 0x0102_0304_0506_0708,
        page_live_list_tail_pid: 0x1112_1314_1516_1718,
        promoted_log_head_pid: 0x2122_2324_2526_2728,
        promoted_log_tail_pid: 0x3132_3334_3536_3738,
    };
    let size = volume_entry_inline_size(entry.shard_count as usize);
    let mut buf = vec![0u8; size];
    let mut off = 0;
    encode_volume_entry_inline(&entry, &mut buf, &mut off).unwrap();
    assert_eq!(off, size);
    let mut off = 0;
    let decoded = decode_volume_entry_inline(&buf, &mut off, MANIFEST_BODY_VERSION).unwrap();
    assert_eq!(decoded, entry);
    assert_eq!(off, size);

    // The Option::None encoding (INVALID_VOLUME / PROMOTION_CURSOR_NONE
    // sentinels) must round-trip as `None`, not as `Some(sentinel_value)`.
    let bare = VolumeEntry {
        ord: 0,
        shard_count: 1,
        l2p_shard_roots: bx(&[NULL_PAGE]),
        l2p_shard_durable_seq: bx(&[0]),
        created_lsn: 0,
        flags: 0,
        dead_list_head_pid: NULL_PAGE,
        dead_list_tail_pid: NULL_PAGE,
        parent_vol_ord: None,
        branched_at_lsn: 0,
        promotion_cursor: None,
        page_dead_list_head_pid: NULL_PAGE,
        page_dead_list_tail_pid: NULL_PAGE,
        page_live_list_head_pid: NULL_PAGE,
        page_live_list_tail_pid: NULL_PAGE,
        promoted_log_head_pid: NULL_PAGE,
        promoted_log_tail_pid: NULL_PAGE,
    };
    let size = volume_entry_inline_size(bare.shard_count as usize);
    let mut buf = vec![0u8; size];
    let mut off = 0;
    encode_volume_entry_inline(&bare, &mut buf, &mut off).unwrap();
    let mut off = 0;
    let decoded = decode_volume_entry_inline(&buf, &mut off, MANIFEST_BODY_VERSION).unwrap();
    assert_eq!(decoded, bare);
    assert!(decoded.parent_vol_ord.is_none());
    assert!(decoded.promotion_cursor.is_none());
}

/// Old (pre-Phase 4) v13 manifests are flag-day rejected by the v14
/// decoder so a downgraded binary that ran briefly on the same on-disk
/// state can't be silently re-promoted.
#[test]
fn v13_manifest_is_rejected_after_flag_day_to_v14() {
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
    {
        let p = page.payload_mut();
        // Plant a v13 version stamp; the rest of the body can stay zero —
        // the version dispatch fires before any of it is read.
        p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4].copy_from_slice(&13u32.to_le_bytes());
    }
    page.seal();
    match Manifest::decode(&page, &ps).unwrap_err() {
        MetaDbError::Corruption(msg) => {
            assert!(
                msg.contains("unsupported manifest body version") && msg.contains("v22"),
                "expected v13-rejection message mentioning v22, got: {msg}"
            );
        }
        e => panic!("expected Corruption from v13 manifest, got {e}"),
    }
}

#[test]
fn v14_manifest_is_rejected_after_flag_day_to_v15() {
    // ZFS-TXG-clone Phase 4 flag-day: a v14 body has no `checkpoint_txg`
    // slot. The decoder must reject it before it ever reaches the new
    // OFF_CHECKPOINT_TXG read.
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
    {
        let p = page.payload_mut();
        p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4].copy_from_slice(&14u32.to_le_bytes());
    }
    page.seal();
    match Manifest::decode(&page, &ps).unwrap_err() {
        MetaDbError::Corruption(msg) => {
            assert!(
                msg.contains("unsupported manifest body version") && msg.contains("v22"),
                "expected v14-rejection message mentioning v22, got: {msg}"
            );
        }
        e => panic!("expected Corruption from v14 manifest, got {e}"),
    }
}

#[test]
fn v15_round_trip_carries_checkpoint_txg() {
    // ZFS-TXG-clone Phase 4: a real-shaped manifest round-trips its
    // `checkpoint_txg` field byte-equivalent through encode + decode.
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let m = Manifest {
        body_version: MANIFEST_BODY_VERSION,
        checkpoint_lsn: 1234,
        checkpoint_txg: 42,
        last_processed_buffer_seq: 0,
        lifecycle_replay_seq: 0,
        free_list_head: NULL_PAGE,
        refcount_shard_roots: bx(&[10, 20, 30, 40]),
        refcount_durable_seq: bx(&[1234, 1234, 1234, 1234]),
        dedup_shards: 1,
        dedup_index_shard_heads: one_shard(&[NULL_PAGE]),
        next_snapshot_id: 1,
        next_volume_ord: 1,
        snapshots: Vec::new(),
        volumes: vec![boot_vol_at(4, &[1, 2, 3, 4], 1234)],
    };
    let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
    m.encode(&mut page).unwrap();
    page.seal();
    let decoded = Manifest::decode(&page, &ps).unwrap();
    assert_eq!(decoded.checkpoint_txg, 42);
    assert_eq!(decoded.checkpoint_lsn, 1234);
    assert_eq!(decoded, m);
}

#[test]
fn v15_checkpoint_txg_zero_round_trips() {
    // Empty manifest (checkpoint_txg = 0) must encode + decode cleanly —
    // the codepath new databases hit at first open.
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let m = Manifest {
        body_version: MANIFEST_BODY_VERSION,
        checkpoint_lsn: 0,
        checkpoint_txg: 0,
        last_processed_buffer_seq: 0,
        lifecycle_replay_seq: 0,
        free_list_head: NULL_PAGE,
        refcount_shard_roots: bx(&[NULL_PAGE; 4]),
        refcount_durable_seq: bx(&[0; 4]),
        dedup_shards: 1,
        dedup_index_shard_heads: one_shard(&[NULL_PAGE]),
        next_snapshot_id: 1,
        next_volume_ord: 1,
        snapshots: Vec::new(),
        volumes: vec![boot_vol_at(4, &[NULL_PAGE; 4], 0)],
    };
    let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
    m.encode(&mut page).unwrap();
    page.seal();
    let decoded = Manifest::decode(&page, &ps).unwrap();
    assert_eq!(decoded.checkpoint_txg, 0);
    assert_eq!(decoded, m);
}

// ── ZFS port S3 (v22): l2p_page_rc shard group DELETED ──────────────

/// A v21 manifest (still carried the now-deleted l2p_page_rc shard group)
/// is flag-day rejected by the v22 decoder — backcompat is waived
/// pre-release (onyx rebuilds metadb on schema change).
#[test]
fn v21_manifest_is_rejected_after_flag_day_to_v22() {
    let dir = TempDir::new().unwrap();
    let ps = mk_store(&dir);
    let mut page = Page::new(PageHeader::new(PageType::Manifest, 1));
    {
        let p = page.payload_mut();
        p[OFF_BODY_VERSION..OFF_BODY_VERSION + 4].copy_from_slice(&21u32.to_le_bytes());
    }
    page.seal();
    match Manifest::decode(&page, &ps).unwrap_err() {
        MetaDbError::Corruption(msg) => {
            assert!(
                msg.contains("unsupported manifest body version") && msg.contains("v22"),
                "expected v21-rejection message mentioning v22, got: {msg}"
            );
        }
        e => panic!("expected Corruption from v21 manifest, got {e}"),
    }
}
