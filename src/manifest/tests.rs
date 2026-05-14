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
    }
}

fn boot_vol(shard_count: u32, roots: &[PageId]) -> VolumeEntry {
    VolumeEntry {
        ord: 0,
        shard_count,
        l2p_shard_roots: bx(roots),
        created_lsn: 0,
        flags: 0,
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
        free_list_head: 99,
        refcount_shard_roots: bx(&[17, 18, 19, 20]),
        dedup_shards: 1,
        dedup_index_shard_heads: one_shard(&[NULL_PAGE, NULL_PAGE]),
        next_snapshot_id: 5,
        next_volume_ord: 1,
        snapshots: vec![snap(&ps, 1, 0, &[11, 12, 13, 14], 100)],
        volumes: vec![boot_vol(4, &[7, 8, 9, 10])],
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
        free_list_head: 1234,
        refcount_shard_roots: bx(&[142, 143, 144, 145]),
        dedup_shards: 1,
        dedup_index_shard_heads: one_shard(&[NULL_PAGE, 200, 300]),
        next_snapshot_id: 99,
        next_volume_ord: 1,
        snapshots: vec![
            snap(&ps, 1, 0, &[10, 11, 12, 13], 100),
            snap(&ps, 5, 0, &[20, 21, 22, 23], 500),
        ],
        volumes: vec![boot_vol(4, &[42, 43, 44, 45])],
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
        free_list_head: NULL_PAGE,
        refcount_shard_roots: bx(&[50, 51]),
        dedup_shards: 1,
        dedup_index_shard_heads: one_shard(&[]),
        next_snapshot_id: 2,
        next_volume_ord: 3,
        snapshots: vec![snap(&ps, 1, 2, &[100, 101], 10)],
        volumes: vec![
            boot_vol(2, &[200, 201]),
            VolumeEntry {
                ord: 2,
                shard_count: 2,
                l2p_shard_roots: bx(&[300, 301]),
                created_lsn: 7,
                flags: VOLUME_FLAG_DROP_PENDING,
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
        free_list_head: NULL_PAGE,
        refcount_shard_roots: bx(&[1, 2]),
        dedup_shards: 4,
        dedup_index_shard_heads: one_shard(&[10]),
        next_snapshot_id: 1,
        next_volume_ord: 1,
        snapshots: Vec::new(),
        volumes: vec![boot_vol(2, &[100, 101])],
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
        created_lsn: 0xABCD_1234,
        flags: VOLUME_FLAG_DROP_PENDING,
    };
    let mut buf = vec![0u8; volume_entry_inline_size(entry.shard_count as usize)];
    let mut off = 0;
    encode_volume_entry_inline(&entry, &mut buf, &mut off).unwrap();
    assert_eq!(off, buf.len());
    let mut off = 0;
    let decoded = decode_volume_entry_inline(&buf, &mut off).unwrap();
    assert_eq!(decoded, entry);
    assert_eq!(off, buf.len());
}

#[test]
fn volume_entry_inline_rejects_shard_count_mismatch() {
    let entry = VolumeEntry {
        ord: 1,
        shard_count: 2,
        l2p_shard_roots: bx(&[7]), // length 1, but shard_count 2
        created_lsn: 10,
        flags: 0,
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
        created_lsn: 0,
        flags: 0,
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
    // Encode a legit entry, then lop the final root off the buffer.
    let entry = VolumeEntry {
        ord: 3,
        shard_count: 3,
        l2p_shard_roots: bx(&[11, 22, 33]),
        created_lsn: 7,
        flags: 0,
    };
    let mut buf = vec![0u8; volume_entry_inline_size(3)];
    let mut off = 0;
    encode_volume_entry_inline(&entry, &mut buf, &mut off).unwrap();
    buf.truncate(buf.len() - 8);
    let mut off = 0;
    assert!(matches!(
        decode_volume_entry_inline(&buf, &mut off),
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
            created_lsn: 100,
            flags: 0,
        },
        VolumeEntry {
            ord: 1,
            shard_count: 4,
            l2p_shard_roots: bx(&[20, 21, 22, 23]),
            created_lsn: 200,
            flags: VOLUME_FLAG_DROP_PENDING,
        },
        VolumeEntry {
            ord: 65534,
            shard_count: 1,
            l2p_shard_roots: bx(&[NULL_PAGE]),
            created_lsn: 300,
            flags: 0,
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
        let got = decode_volume_entry_inline(&buf, &mut off).unwrap();
        assert_eq!(&got, expected);
    }
    assert_eq!(off, total);
}
