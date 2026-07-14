use super::*;

#[test]
fn fresh_db_is_empty() {
    let (_d, db) = mk_db();
    assert_eq!(db.get(0, 42).unwrap(), None);
    assert!(db.snapshots().is_empty());
    assert_eq!(db.manifest().next_snapshot_id, 1);
}

#[test]
fn create_with_config_uses_requested_shards() {
    let (_d, db) = mk_db_with_shards(4);
    assert_eq!(db.shard_count(), 4);
    let manifest = db.manifest();
    assert_eq!(manifest.refcount_shard_roots.len(), 4);
    let boot = manifest
        .volumes
        .iter()
        .find(|v| v.ord == BOOTSTRAP_VOLUME_ORD)
        .expect("bootstrap volume entry present");
    assert_eq!(boot.shard_count, 4);
    assert_eq!(boot.l2p_shard_roots.len(), 4);
}

#[test]
fn insert_get_round_trip() {
    let (_d, db) = mk_db();
    db.insert(0, 10, v(7)).unwrap();
    assert_eq!(db.get(0, 10).unwrap(), Some(v(7)));
}

#[test]
fn flush_persists_tree_state_via_manifest() {
    // v2 caps `MAX_UNITS_PER_LEAF` at 100, so a leaf (128 LBAs) cannot
    // hold 128 distinct UnitMetas — share value bytes across 32-LBA
    // groups to keep ≤ 4 distinct units per leaf. The test's
    // invariant is "values round-trip through flush + reopen", and
    // that still holds with shared values.
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        for i in 0u64..500 {
            db.insert(0, i, v((i / 32) as u8)).unwrap();
        }
        db.flush().unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    for i in 0u64..500 {
        assert_eq!(db.get(0, i).unwrap(), Some(v((i / 32) as u8)));
    }
}

#[test]
fn buffer_applied_watermark_reports_only_committed_manifest_frontier() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        db.set_buffer_applied_watermark(41);
        assert_eq!(db.buffer_applied_watermark(), 41);
        assert_eq!(db.durable_buffer_applied_watermark(), 0);

        db.flush().unwrap();
        assert_eq!(db.durable_buffer_applied_watermark(), 41);
    }

    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.buffer_applied_watermark(), 41);
    assert_eq!(db.durable_buffer_applied_watermark(), 41);

    // A stale caller cannot regress the replay boundary on a later manifest.
    db.set_buffer_applied_watermark(7);
    db.flush().unwrap();
    assert_eq!(db.durable_buffer_applied_watermark(), 41);
}

#[test]
fn async_page_reclaim_drains_bounded_cycles_without_checkpoint_waiting() {
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.async_reclaim_enabled = true;
    cfg.async_reclaim_max_pages_per_cycle = 2;
    cfg.async_reclaim_idle_interval_ms = 1;
    let db = Db::create_with_config(cfg).unwrap();

    let pages: Vec<PageId> = (0..8).map(|_| db.page_store.allocate().unwrap()).collect();
    db.page_store.free_many(&pages, 1).unwrap();
    db.notify_async_reclaim();

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while db.page_store.deferred_free_len() != 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "background page reclaim did not drain its bounded backlog"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

#[test]
fn terminal_reclaim_persists_freed_pages_across_device_reopen() {
    let dir = TempDir::new().unwrap();
    let page_device = Arc::new(crate::page_store::MemDevice::new(16_384));
    let journal_device = Arc::new(crate::lifecycle_log::MemJournalDevice::new(512));
    let mut cfg = Config::new(dir.path());
    cfg.dedup_cuckoo_buckets = 256;
    cfg.async_reclaim_enabled = true;
    cfg.async_reclaim_max_pages_per_cycle = 1;
    cfg.async_reclaim_idle_interval_ms = 60_000;
    cfg.bfg_threads_enabled = true;
    cfg.bfg_timeout_ms = 60_000;
    cfg.lineage_gc_enabled = false;
    cfg.livelist_condense_min_segments = 0;
    cfg.dedup_drainer_enabled = false;
    cfg.l2p_writeback_enabled = false;
    cfg.reclaim_orphans_on_open = false;

    let db = Db::create_on_device_with_faults(
        cfg.clone(),
        FaultController::disabled(),
        page_device.clone(),
        journal_device.clone(),
    )
    .unwrap();
    let mut retired = Vec::new();
    for generation in 1..=32u64 {
        let pid = db.page_store.allocate().unwrap();
        let mut page = crate::page::Page::new(crate::page::PageHeader::new(
            crate::page::PageType::PagedLeaf,
            generation,
        ));
        page.seal();
        db.page_store.write_page(pid, &page).unwrap();
        retired.push(pid);
    }
    // Keep the reclaimed pages interior so reopen must recover them from the
    // persisted bitmap rather than merely lowering the allocation frontier.
    let sentinel = db.page_store.allocate().unwrap();
    let mut sentinel_page = crate::page::Page::new(crate::page::PageHeader::new(
        crate::page::PageType::PagedLeaf,
        99,
    ));
    sentinel_page.seal();
    db.page_store.write_page(sentinel, &sentinel_page).unwrap();
    let pin = db.page_store.epoch().pin();
    db.page_store.free_many(&retired, 100).unwrap();
    drop(pin);

    let sequence_before = db.manifest_state.lock().store.sequence();
    db.drain_deferred_reclaim_durable().unwrap();
    let sequence_after = db.manifest_state.lock().store.sequence();
    assert_eq!(
        sequence_after - sequence_before,
        2,
        "one reclaiming flush plus one zero-reclaim flush must converge"
    );
    assert_eq!(db.page_store.deferred_free_len(), 0);
    drop(db);

    let reopened = Db::open_on_device_with_faults(
        cfg,
        FaultController::disabled(),
        page_device,
        journal_device,
    )
    .unwrap();
    let (high_water, bitmap) = reopened.page_store.snapshot_free_bitmap_and_high_water();
    for pid in retired {
        assert!(
            pid < high_water,
            "reclaimed pid must remain below the frontier"
        );
        let bit = (pid - FIRST_DATA_PAGE) as usize;
        assert_ne!(
            bitmap[bit / 8] & (1 << (bit % 8)),
            0,
            "reclaimed pid {pid} was lost from the persisted free-list bitmap"
        );
    }
}

#[test]
fn terminal_reclaim_persists_unused_l2p_allocation_batch_across_device_reopen() {
    let dir = TempDir::new().unwrap();
    let page_device = Arc::new(crate::page_store::MemDevice::new(16_384));
    let journal_device = Arc::new(crate::lifecycle_log::MemJournalDevice::new(512));
    let mut cfg = Config::new(dir.path());
    cfg.shards_per_partition = 1;
    cfg.dedup_cuckoo_buckets = 256;
    cfg.async_reclaim_enabled = true;
    cfg.async_reclaim_max_pages_per_cycle = 1;
    cfg.async_reclaim_idle_interval_ms = 60_000;
    cfg.bfg_threads_enabled = true;
    cfg.bfg_timeout_ms = 60_000;
    cfg.lineage_gc_enabled = false;
    cfg.livelist_condense_min_segments = 0;
    cfg.dedup_drainer_enabled = false;
    cfg.l2p_writeback_enabled = false;
    cfg.reclaim_orphans_on_open = false;

    let db = Db::create_on_device_with_faults(
        cfg.clone(),
        FaultController::disabled(),
        page_device.clone(),
        journal_device.clone(),
    )
    .unwrap();
    let unused = {
        let volumes = db.volumes.read();
        let volume = volumes.get(&BOOTSTRAP_VOLUME_ORD).unwrap();
        let ids = volume.shards[0].tree.read().unused_allocation_ids();
        assert_eq!(ids.len(), crate::paged::cache::LOCAL_ALLOC_RUN_PAGES - 1);
        ids
    };

    db.drain_deferred_reclaim_durable().unwrap();
    assert!(
        db.volumes.read()[&BOOTSTRAP_VOLUME_ORD].shards[0]
            .tree
            .read()
            .unused_allocation_ids()
            .is_empty()
    );
    drop(db);

    let reopened = Db::open_on_device_with_faults(
        cfg,
        FaultController::disabled(),
        page_device,
        journal_device,
    )
    .unwrap();
    let (high_water, bitmap) = reopened.page_store.snapshot_free_bitmap_and_high_water();
    let mut persisted_free = 0usize;
    for pid in &unused {
        assert!(*pid < high_water, "reserved pid {pid} must remain interior");
        let raw = reopened.page_store.read_page_unchecked(*pid).unwrap();
        let is_free = raw.bytes().iter().all(|byte| *byte == 0)
            || matches!(
                raw.header(),
                Ok(header) if header.page_type == crate::page::PageType::Free
            );
        assert!(is_free, "reserved pid {pid} was not physically reclaimed");
        persisted_free += 1;
        let bit = (*pid - FIRST_DATA_PAGE) as usize;
        assert_ne!(
            bitmap[bit / 8] & (1 << (bit % 8)),
            0,
            "free reserved pid {pid} was lost from the persisted free-list bitmap"
        );
    }
    assert_eq!(persisted_free, unused.len());
    let report = reopened
        .verify(crate::verify::VerifyOptions {
            strict: true,
            ..crate::verify::VerifyOptions::default()
        })
        .unwrap();
    assert!(
        report.is_clean(),
        "strict verification failed: {:?}",
        report.issues
    );
    assert!(report.orphan_pages.is_empty());
}

#[test]
fn take_snapshot_assigns_monotonic_ids() {
    let (_d, db) = mk_db();
    let a = db.take_snapshot(0).unwrap();
    let b = db.take_snapshot(0).unwrap();
    let c = db.take_snapshot(0).unwrap();
    assert_eq!(a, 1);
    assert_eq!(b, 2);
    assert_eq!(c, 3);
    assert_eq!(db.snapshots().len(), 3);
}

#[test]
fn snapshot_view_sees_state_at_take_time() {
    let (_d, db) = mk_db();
    for i in 0u64..100 {
        db.insert(0, i, v(1)).unwrap();
    }
    let snap = db.take_snapshot(0).unwrap();

    for i in 0u64..100 {
        db.insert(0, i, v(2)).unwrap();
    }
    db.insert(0, 999, v(9)).unwrap();
    db.delete(0, 50).unwrap();

    assert_eq!(db.get(0, 0).unwrap(), Some(v(2)));
    assert_eq!(db.get(0, 50).unwrap(), None);
    assert_eq!(db.get(0, 999).unwrap(), Some(v(9)));

    let view = db.snapshot_view(snap).unwrap();
    for i in 0u64..100 {
        assert_eq!(view.get(i).unwrap(), Some(v(1)));
    }
    assert_eq!(view.get(999).unwrap(), None);
}

#[test]
fn snapshot_view_range_scan() {
    let (_d, db) = mk_db();
    for i in 0u64..50 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(0).unwrap();
    for i in 0u64..50 {
        db.insert(0, i, v(99)).unwrap();
    }

    let view = db.snapshot_view(snap).unwrap();
    let items: Vec<(u64, L2pValue)> = view
        .range(10u64..20)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    for (i, (k, val)) in items.iter().enumerate() {
        assert_eq!(*k, 10 + i as u64);
        assert_eq!(*val, v((10 + i) as u8));
    }
}

#[test]
fn snapshot_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let snap_id = {
        let db = Db::create(dir.path()).unwrap();
        for i in 0u64..200 {
            db.insert(0, i, v(1)).unwrap();
        }
        let id = db.take_snapshot(0).unwrap();
        for i in 0u64..200 {
            db.insert(0, i, v(2)).unwrap();
        }
        db.flush().unwrap();
        id
    };

    let db = Db::open(dir.path()).unwrap();
    let snaps = db.snapshots();
    assert_eq!(snaps.len(), 1);
    assert_eq!(snaps[0].id, snap_id);

    let view = db.snapshot_view(snap_id).unwrap();
    for i in 0u64..200 {
        assert_eq!(view.get(i).unwrap(), Some(v(1)));
    }
    for i in 0u64..200 {
        assert_eq!(db.get(0, i).unwrap(), Some(v(2)));
    }
}

#[test]
fn snapshot_view_missing_id_returns_none() {
    let (_d, db) = mk_db();
    assert!(db.snapshot_view(999).is_none());
}

#[test]
fn diff_detects_added_removed_changed() {
    let (_d, db) = mk_db();
    for i in 0u64..10 {
        db.insert(0, i, v(1)).unwrap();
    }
    let a = db.take_snapshot(0).unwrap();

    db.insert(0, 5, v(99)).unwrap();
    db.delete(0, 3).unwrap();
    db.insert(0, 42, v(7)).unwrap();

    let b = db.take_snapshot(0).unwrap();
    let diff = db.diff(a, b).unwrap();
    assert_eq!(diff.len(), 3);
    match diff[0] {
        DiffEntry::RemovedInB { key: 3, old } => assert_eq!(old, v(1)),
        ref other => panic!("{other:?}"),
    }
    match diff[1] {
        DiffEntry::Changed { key: 5, old, new } => {
            assert_eq!(old, v(1));
            assert_eq!(new, v(99));
        }
        ref other => panic!("{other:?}"),
    }
    match diff[2] {
        DiffEntry::AddedInB { key: 42, new } => assert_eq!(new, v(7)),
        ref other => panic!("{other:?}"),
    }
}

#[test]
fn diff_with_current_reflects_unsaved_writes() {
    let (_d, db) = mk_db();
    for i in 0u64..10 {
        db.insert(0, i, v(1)).unwrap();
    }
    let a = db.take_snapshot(0).unwrap();
    db.insert(0, 100, v(5)).unwrap();
    let diff = db.diff_with_current(a).unwrap();
    assert_eq!(diff.len(), 1);
    match diff[0] {
        DiffEntry::AddedInB { key: 100, new } => assert_eq!(new, v(5)),
        ref other => panic!("{other:?}"),
    }
}

#[test]
fn drop_snapshot_returns_none_for_unknown_id() {
    let (_d, db) = mk_db();
    assert!(db.drop_snapshot(999).unwrap().is_none());
}

#[test]
fn drop_snapshot_reclaims_uniquely_owned_pages() {
    let (_d, db) = mk_db();
    for i in 0u64..1000 {
        db.insert(0, i, v(1)).unwrap();
    }
    db.flush().unwrap();

    let s = db.take_snapshot(0).unwrap();

    for i in 0u64..1000 {
        db.insert(0, i, v(2)).unwrap();
    }
    db.flush().unwrap();

    // Snapshot still observes the pre-snapshot value while the live
    // view advanced to `v(2)` — proves COW forked each overwritten
    // LBA's path. Earlier revisions compared `high_water` before vs
    // after the second batch of writes, but the per-shard
    // `LOCAL_ALLOC_RUN_PAGES = 256` pre-allocation pool means freshly
    // opened shards hold ~16 × 256 unused pages already counted in
    // `high_water`; COW draws from that pool without bumping the
    // counter.
    let view = db.snapshot_view(s).expect("snapshot view");
    for i in 0u64..1000 {
        assert_eq!(view.get(i).unwrap(), Some(v(1)));
    }
    drop(view);

    let report = db.drop_snapshot(s).unwrap().unwrap();
    assert!(report.pages_freed > 0);
    assert_eq!(report.freed_leaf_values.len(), 1000);
    assert!(report.freed_leaf_values.iter().all(|val| *val == v(1)));
    for i in 0u64..1000 {
        assert_eq!(db.get(0, i).unwrap(), Some(v(2)));
    }
}

#[test]
fn multiple_snapshots_isolated() {
    let (_d, db) = mk_db();
    for i in 0u64..20 {
        db.insert(0, i, v(1)).unwrap();
    }
    let s1 = db.take_snapshot(0).unwrap();
    for i in 0u64..20 {
        db.insert(0, i, v(2)).unwrap();
    }
    let s2 = db.take_snapshot(0).unwrap();
    for i in 0u64..20 {
        db.insert(0, i, v(3)).unwrap();
    }

    {
        let v1 = db.snapshot_view(s1).unwrap();
        assert_eq!(v1.get(5).unwrap(), Some(v(1)));
    }
    {
        let v2 = db.snapshot_view(s2).unwrap();
        assert_eq!(v2.get(5).unwrap(), Some(v(2)));
    }
    assert_eq!(db.get(0, 5).unwrap(), Some(v(3)));
}

// -------- B2: L2P buffer + periodic compaction --------

fn mk_db_with_buffer() -> (TempDir, std::sync::Arc<Db>) {
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    // Use a very small soft trigger so the compactor runs frequently
    // in tests, exercising swap → apply → publish quickly.
    cfg.l2p_buffer_soft_entries = 4;
    cfg.l2p_buffer_max_interval_ms = 50;
    let db = Db::create_with_config(cfg).unwrap();
    (dir, db)
}

#[test]
fn b2_buffer_insert_visible_to_get() {
    let (_d, db) = mk_db_with_buffer();
    db.insert(0, 10, v(7)).unwrap();
    assert_eq!(db.get(0, 10).unwrap(), Some(v(7)));
}

#[test]
fn b2_buffer_overwrite_visible_to_get() {
    let (_d, db) = mk_db_with_buffer();
    db.insert(0, 10, v(7)).unwrap();
    db.insert(0, 10, v(8)).unwrap();
    assert_eq!(db.get(0, 10).unwrap(), Some(v(8)));
}

// Repro for the page-rc staging page-rc cutover premature-free found by the nvme-box
// snapshot-churn soak (2026-06-17). In buffer mode the COW increfs of
// pages that become shared with a live snapshot are staged into the
// `L2pPageRc` array; if any of those increfs is wrongly dropped (the
// `RcShard` replay-skip / fold-skip is array-DATA-PAGE granular, not
// per-pid), the page is under-counted and `drop_snapshot` frees it while
// the live volume still points at it — surfacing as a lost mapping or a
// page-type corruption (`page X has level/type ...`). NO test combined
// buffer mode + snapshots before this, which is exactly why the cutover's
// unit suite stayed green while the soak corrupted in ~4 minutes.
#[test]
fn buffer_mode_snapshot_churn_no_premature_free() {
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    // Tiny soft trigger → the compactor (and the page-rc fold) runs
    // constantly, the way the prod drain does, so the array-page
    // generation keeps racing the COW increfs.
    cfg.l2p_buffer_soft_entries = 4;
    cfg.l2p_buffer_max_interval_ms = 50;
    let db = Db::create_with_config(cfg).unwrap();

    // Span many leaves/index pages so many pids land on one page-rc
    // array data page. Share values across 32-LBA groups (≤4 distinct
    // units/leaf, MAX_UNITS_PER_LEAF=100) and shift per round so every
    // overwrite genuinely COWs.
    const N: u64 = 4096;
    let val = |i: u64, round: u64| -> L2pValue { v((((i / 32) + round) % 251) as u8) };

    for i in 0..N {
        db.insert(0, i, val(i, 0)).unwrap();
    }
    db.flush().unwrap();

    for round in 1u64..8 {
        let snap = db.take_snapshot(0).unwrap();
        // Overwrite the whole volume while the snapshot is live → COW
        // clones the touched path and increfs the now-shared siblings.
        for i in 0..N {
            db.insert(0, i, val(i, round)).unwrap();
        }
        db.flush().unwrap();
        db.drop_snapshot(snap).unwrap().unwrap();
        // Every live LBA must still resolve to this round's value. A
        // premature free shows up as a lost mapping (None), a wrong
        // value, or an Err from a corrupted page walk.
        for i in 0..N {
            assert_eq!(
                db.get(0, i).unwrap(),
                Some(val(i, round)),
                "round {round} lba {i}: live mapping lost (page-rc premature free)"
            );
        }
    }

    db.flush().unwrap();
    // Reopen so `reclaim_orphan_pages` sweeps the benign post-drop
    // `SnapshotRoots` pages (drop_snapshot leaves them allocated until
    // a snapshot-less manifest is persisted + replayed — see the NOTE in
    // drop_snapshot); without this, strict verify trips on those.
    drop(db);
    let db = Db::open(dir.path()).unwrap();
    for i in 0..N {
        assert_eq!(db.get(0, i).unwrap(), Some(val(i, 7)), "reopen lba {i}");
    }
    let report = crate::verify::verify_path(
        dir.path(),
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
            check_clone_livelist: false,
            check_clone_birth_shadow: false,
        },
    )
    .unwrap();
    assert!(report.is_clean(), "verify issues: {:?}", report.issues);
}

// Direct repro of the nvme-box premature-free: the soak snapshots a
// USER volume (create_volume), not the bootstrap volume (ord 0). The box
// trace showed the fresh user-volume shard roots had page-rc array rc=0
// (`SNAP_INCREF array_before=0`) — create_volume stages each root's +1 but
// never `l2p_page_rc.flush()`es it (unlike take/drop/clone/bootstrap), so a
// separate create-volume process exits before it's durable and `start`
// reopens with rc=0. The snapshot incref then bumps 0->1 (should be 1->2)
// and drop's stage(-1) floors it to 0, freeing a still-live root.
#[test]
fn create_volume_root_page_rc_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let vol;
    {
        let mut cfg = Config::new(dir.path());
        cfg.l2p_buffer_enabled = true;
        let db = Db::create_with_config(cfg).unwrap();
        // Mirror the `create-volume` CLI: make the volume, then the
        // process exits — NO writes, NO explicit page-rc fold.
        vol = db.create_volume().unwrap();
        drop(db);
    }
    // Mirror `start`: reopen the store.
    let db = Db::open(dir.path()).unwrap();
    // Snapshot the fresh user volume immediately (created_lsn=0, exactly
    // like the soak's first churn), then write + drop. If the root's array
    // rc was lost at create, drop frees it and the live data is gone.
    let snap = db.take_snapshot(vol).unwrap();
    for i in 0..256u64 {
        db.insert(vol, i, v((i / 32) as u8)).unwrap();
    }
    db.flush().unwrap();
    db.drop_snapshot(snap).unwrap().unwrap();
    for i in 0..256u64 {
        assert_eq!(
            db.get(vol, i).unwrap(),
            Some(v((i / 32) as u8)),
            "lba {i}: live mapping lost (shard-root page-rc under-counted at create_volume)"
        );
    }
    db.flush().unwrap();
    drop(db);
    let db = Db::open(dir.path()).unwrap();
    for i in 0..256u64 {
        assert_eq!(
            db.get(vol, i).unwrap(),
            Some(v((i / 32) as u8)),
            "reopen lba {i}"
        );
    }
    let report = crate::verify::verify_path(
        dir.path(),
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
            check_clone_livelist: false,
            check_clone_birth_shadow: false,
        },
    )
    .unwrap();
    assert!(report.is_clean(), "verify issues: {:?}", report.issues);
}

// BFG sync-task regression: `take_snapshot` stages the
// snapshot-root incref into the OPEN BFG, so it folds in a LATER sync
// cycle — NOT the take's own cycle. If the process closes right after
// `take_snapshot` (before that fold), the incref is durable ONLY via the
// journaled `LifecycleOp::TakeSnapshot` record + replay. A reopen MUST
// re-apply it — and must do so even though the snapshot entry is already
// in the committed manifest (the take cycle committed the entry before
// the incref folded). The first cut gated the replay incref on
// "entry not yet present", so it was skipped: the root page reopened at
// rc=1 (volume only), the next overwrite's COW treated it as exclusive
// and FREED it, and a subsequent `write_snapshot_roots_page` reused the
// pid → "page N has unexpected type SnapshotRoots in paged tree walk".
#[test]
fn take_snapshot_incref_survives_close_before_fold() {
    let dir = TempDir::new().unwrap();
    {
        let mut cfg = Config::new(dir.path());
        cfg.l2p_buffer_enabled = true;
        let db = Db::create_with_config(cfg).unwrap();
        for i in 0..256u64 {
            db.insert(0, i, v((i / 32) as u8)).unwrap();
        }
        db.flush().unwrap();
        let _s = db.take_snapshot(0).unwrap();
        // Deliberately NO flush after take → the incref is staged in the
        // open BFG, never folded. Closing here is the crash-equivalent.
        drop(db);
    }
    // Reopen: lifecycle replay must restore the incref BEFORE the writes
    // below COW the shared root. Take a second snapshot too, so a reused
    // root pid would be caught as a SnapshotRoots-page aliasing.
    let db = Db::open(dir.path()).unwrap();
    for i in 0..256u64 {
        db.insert(0, i, v((i / 32 + 1) as u8)).unwrap();
    }
    db.flush().unwrap();
    let _s2 = db.take_snapshot(0).unwrap();
    for i in 0..256u64 {
        db.insert(0, i, v((i / 32 + 2) as u8)).unwrap();
    }
    db.flush().unwrap();
    drop(db);

    let db = Db::open(dir.path()).unwrap();
    for i in 0..256u64 {
        assert_eq!(
            db.get(0, i).unwrap(),
            Some(v((i / 32 + 2) as u8)),
            "reopen lba {i}"
        );
    }
    drop(db);
    let report = crate::verify::verify_path(
        dir.path(),
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
            check_clone_livelist: false,
            check_clone_birth_shadow: false,
        },
    )
    .unwrap();
    assert!(report.is_clean(), "verify issues: {:?}", report.issues);
}

// Concurrent version — this is the shape the nvme-box soak actually ran
// (writers hammering the volume while snapshots are taken/dropped). The
// serial test above proved quiesced page-rc staging is correct; the soak's
// premature-free needs live COW racing the snapshot force-flush / buffer
// drain. Writers `.unwrap()` so a corruption-class Err (a freed page
// reused as the wrong type) fails the test in the writer thread; the
// final reopen + strict verify catches silent leaks / undercounts.
#[test]
fn buffer_mode_concurrent_snapshot_churn_no_corruption() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    cfg.l2p_buffer_soft_entries = 4;
    cfg.l2p_buffer_max_interval_ms = 5;
    // Mirror the nvme-box soak config: background BFG sync threads make
    // the page-rc fold/drain run CONCURRENTLY with commits + the
    // snapshot force-flush — the race the serial path can't hit.
    cfg.bfg_threads_enabled = true;
    cfg.rc_authoritative_reclaim = true;
    let db = Db::create_with_config(cfg).unwrap();

    const N: u64 = 4096;
    const WRITERS: u64 = 4;
    for i in 0..N {
        db.insert(0, i, v((i / 32) as u8)).unwrap();
    }
    db.flush().unwrap();

    let stop = std::sync::Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();
    for t in 0..WRITERS {
        let db = db.clone();
        let stop = stop.clone();
        handles.push(std::thread::spawn(move || {
            let mut round = 1u64;
            while !stop.load(Ordering::Relaxed) {
                let mut i = t;
                while i < N {
                    db.insert(0, i, v((((i / 32) + round) % 251) as u8))
                        .expect("insert during snapshot churn");
                    i += WRITERS;
                }
                round += 1;
            }
        }));
    }

    // Churn snapshots while the writers run, keeping a ROLLING WINDOW of
    // up to 4 live snapshots (mirrors the soak's --snapshot-max-live 4).
    // Multiple overlapping snapshots pinning the same shard roots is the
    // condition the box repro showed: the first drop under-counted exactly
    // the 16 shard roots by 1.
    let mut live = std::collections::VecDeque::new();
    for _ in 0..400 {
        let snap = db.take_snapshot(0).expect("take_snapshot");
        live.push_back(snap);
        std::thread::sleep(std::time::Duration::from_millis(1));
        if live.len() > 4 {
            let old = live.pop_front().unwrap();
            db.drop_snapshot(old).expect("drop_snapshot");
        }
    }
    while let Some(old) = live.pop_front() {
        db.drop_snapshot(old).expect("drop_snapshot drain");
    }

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    db.flush().unwrap();
    drop(db);
    let db = Db::open(dir.path()).unwrap();
    // Values are nondeterministic across racing writers, so only assert
    // structural integrity: every LBA still resolves without error and
    // strict verify finds no leaked/undercounted pages.
    for i in 0..N {
        db.get(0, i).expect("get after churn");
    }
    let report = crate::verify::verify_path(
        dir.path(),
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
            check_clone_livelist: false,
            check_clone_birth_shadow: false,
        },
    )
    .unwrap();
    assert!(report.is_clean(), "verify issues: {:?}", report.issues);
}

#[test]
fn b2_buffer_delete_visible_to_get() {
    let (_d, db) = mk_db_with_buffer();
    db.insert(0, 10, v(7)).unwrap();
    db.delete(0, 10).unwrap();
    assert_eq!(db.get(0, 10).unwrap(), None);
}

#[test]
fn b2_buffer_multi_get_partial_buffer_partial_tree() {
    let (_d, db) = mk_db_with_buffer();
    // Insert 200 entries (well above soft threshold of 4) so the
    // compactor will fold some into the tree while the most recent
    // ones may still live in the buffer.
    for i in 0u64..200 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    let mut keys: Vec<u64> = (0..200).collect();
    keys.reverse();
    let values = db.multi_get(0, &keys).unwrap();
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(values[i], Some(v(*key as u8)));
    }
}

#[test]
fn b2_buffer_flush_then_get_consistent() {
    let (_d, db) = mk_db_with_buffer();
    for i in 0u64..50 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    db.flush().unwrap();
    for i in 0u64..50 {
        assert_eq!(db.get(0, i).unwrap(), Some(v(i as u8)));
    }
}

#[test]
fn b2_buffer_flush_reopen_round_trip() {
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    cfg.l2p_buffer_soft_entries = 4;
    cfg.l2p_buffer_max_interval_ms = 50;
    {
        let db = Db::create_with_config(cfg.clone()).unwrap();
        for i in 0u64..100 {
            db.insert(0, i, v(i as u8)).unwrap();
        }
        db.flush().unwrap();
    }
    let db = Db::open_with_config(cfg).unwrap();
    for i in 0u64..100 {
        assert_eq!(db.get(0, i).unwrap(), Some(v(i as u8)));
    }
}

// WAL-free recovery: `b2_buffer_no_flush_reopen_replays_from_wal` exercised
// WAL replay of an unflushed L2P buffer — covered for the Buffer
// path by `tests/db_buffer_journal_replay.rs`'s lifecycle journal
// tests + the embedder-side LV2 buffer replay. Test deleted.

// -------- B2 snapshot + range_delete with buffer --------

#[test]
fn b2_buffer_take_snapshot_force_compacts_target() {
    // Snapshot must see post-commit / pre-compaction writes that
    // still live in the L2P buffer. `take_snapshot` calls
    // `force_compact_l2p_buffers` so the sampled roots reflect every
    // committed LSN.
    //
    // NB: capped at 100 LBAs (rather than 200) so leaf 0 never holds
    // more than MAX_UNITS_PER_LEAF=128 unique units — see
    // `leaf_compact::MAX_UNITS_PER_LEAF` docs for the v5 cap (restored
    // to LEAF_ENTRY_COUNT via the base_pba delta after v4 had to
    // tighten to 110 for the birth_delta).
    let (_d, db) = mk_db_with_buffer();
    for i in 0u64..100 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(0).unwrap();
    // Post-snapshot writes land in buffer; snapshot view must not see them.
    for i in 0u64..100 {
        db.insert(0, i, v(99)).unwrap();
    }
    db.insert(0, 999, v(7)).unwrap();
    let view = db.snapshot_view(snap).unwrap();
    for i in 0u64..100 {
        assert_eq!(view.get(i).unwrap(), Some(v(i as u8)));
    }
    assert_eq!(view.get(999).unwrap(), None);
}

#[test]
fn b2_buffer_snapshot_survives_reopen() {
    // The post-snapshot buffer must survive WAL replay across reopen,
    // and the snapshot view itself must still resolve correctly
    // against the rebuilt tree.
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    cfg.l2p_buffer_soft_entries = 4;
    cfg.l2p_buffer_max_interval_ms = 50;
    let snap_id = {
        let db = Db::create_with_config(cfg.clone()).unwrap();
        for i in 0u64..100 {
            db.insert(0, i, v(1)).unwrap();
        }
        let id = db.take_snapshot(0).unwrap();
        for i in 0u64..100 {
            db.insert(0, i, v(2)).unwrap();
        }
        db.flush().unwrap();
        id
    };
    let db = Db::open_with_config(cfg).unwrap();
    let view = db.snapshot_view(snap_id).unwrap();
    for i in 0u64..100 {
        assert_eq!(view.get(i).unwrap(), Some(v(1)));
    }
    for i in 0u64..100 {
        assert_eq!(db.get(0, i).unwrap(), Some(v(2)));
    }
}

#[test]
fn b2_buffer_drop_snapshot_with_buffer_overwrites() {
    // Drop must compute decrefs correctly even when post-snapshot
    // overwrites still live in the buffer (i.e., the snapshot's old
    // PBA refcount drops to zero because no live mapping references
    // it anymore — `diff_subtrees` must see the post-buffer current
    // root, which `force_compact_l2p_buffers` guarantees).
    let (_d, db) = mk_db_with_buffer();
    for i in 0u64..1000 {
        db.insert(0, i, v(1)).unwrap();
    }
    db.flush().unwrap();
    let s = db.take_snapshot(0).unwrap();
    for i in 0u64..1000 {
        db.insert(0, i, v(2)).unwrap();
    }
    // Snapshot still observes pre-snapshot values; live observes post.
    let view = db.snapshot_view(s).unwrap();
    for i in 0u64..1000 {
        assert_eq!(view.get(i).unwrap(), Some(v(1)));
    }
    drop(view);
    let report = db.drop_snapshot(s).unwrap().unwrap();
    assert!(report.pages_freed > 0);
    assert_eq!(report.freed_leaf_values.len(), 1000);
    assert!(report.freed_leaf_values.iter().all(|val| *val == v(1)));
    for i in 0u64..1000 {
        assert_eq!(db.get(0, i).unwrap(), Some(v(2)));
    }
}

#[test]
fn b2_buffer_range_delete_drains_buffer() {
    // `range_delete` calls `force_compact_l2p_buffers` so the scan
    // sees buffer-only entries that haven't compacted yet.
    //
    // hot-path `L2pRemap` no longer maintains global rc, and
    // RangeDelete is PBA rc-neutral too. Seed rc explicitly so the test
    // can assert the delete drains buffer-only L2P entries without
    // mutating PBA rc.
    fn remap_val(pba: Pba, tag: u8) -> L2pValue {
        let mut v = [0u8; crate::paged::format::LEAF_VALUE_SIZE];
        v[..8].copy_from_slice(&pba.to_be_bytes());
        v[8] = tag;
        L2pValue(v)
    }
    let (_d, db) = mk_db_with_buffer();
    for i in 0u64..50 {
        let mut tx = db.begin();
        tx.l2p_remap(0, i, remap_val(100 + i, i as u8), None);
        tx.commit_with_outcomes().unwrap();
        db.incref_pba(100 + i, 1).unwrap();
    }
    // No flush — many writes likely still live in the buffer.
    db.range_delete(0, 10, 40).unwrap();
    for i in 0u64..10 {
        assert_eq!(db.get(0, i).unwrap(), Some(remap_val(100 + i, i as u8)));
    }
    for i in 10u64..40 {
        assert_eq!(db.get(0, i).unwrap(), None);
    }
    for i in 40u64..50 {
        assert_eq!(db.get(0, i).unwrap(), Some(remap_val(100 + i, i as u8)));
    }
    // RangeDelete is PBA rc-neutral — it drains the L2P entries but
    // does NOT decref PBA rc. So every seeded pba stays rc=1, *including* the
    // deleted range. This is exactly the shared/dedup-safety property: deleting
    // one LBA mapping must never knock a (possibly shared) PBA's rc toward 0.
    for i in 0u64..50 {
        assert_eq!(db.get_refcount(100 + i).unwrap(), 1);
    }
}

// =====================================================================
// REPRO: force-fold double-decref over a buffered page-rc backlog.
//
// The existing buffer+snapshot tests above all use a TINY
// `l2p_buffer_soft_entries` (4), so the L2P buffer drains essentially
// every commit and the page-rc COW deltas fold into the `L2pPageRc`
// array immediately — there is never a *backlog* of un-folded page-rc
// deltas sitting in the `RcShard` slots when a force=true fold runs.
//
// The nvme-box soak's trigger was different: a snapshot DELETE
// (`drop_snapshot` → `L2pPageRc::flush()` → `RcShard::flush()` →
// `begin_checkpoint_all_slots(force=true)`) firing over a slot holding
// MANY un-folded buffered COW decrefs (large buffer backlog). The
// force=true fold bypasses the per-array-DATA-PAGE replay-skip
// (`!force && page_generation >= pending.last_lsn`, array.rs ~447), so
// if any of those backlogged decrefs targets an `L2pPageRc` array page
// whose generation already reflects that decref (the non-monotone-lsn
// hazard: buffer-mode COW stages page-rc deltas in radix-key order at
// DRAIN time, so the lsns are not monotone vs the array page
// generation), the force fold RE-APPLIES the decref the array already
// reflects → a still-live L2P page's rc is driven to 0 → the page is
// freed → its PageId is reused (e.g. as dedup CuckooData) → a later
// L2P tree walk reads the wrong page type
// (`expected PagedLeaf/PagedIndex, got CuckooData` /
// `page N has level 1, expected 0`).
//
// To reproduce we must BUILD THE BACKLOG: a large `soft_entries` +
// `bfg_threads_enabled = false` so the background compactor does not
// drain, churn many COW overwrites of pages shared with a live
// snapshot across several explicit `flush()`es (each flush folds the
// L2P buffer + advances the page-rc array generation, but leaves later
// drains' decrefs in fresh slots), then DROP the snapshot to fire the
// force-fold over that mixed-generation backlog.
//
// PASS (today's intent) = no corruption: every live LBA still
// resolves, reopen + strict verify is clean, and no benign-double-
// decref underflow drove a live page to 0. On the buggy code this
// FAILS with a lost mapping / wrong value / corrupted-page Err / a
// strict-verify leak-or-undercount issue.
#[test]
fn drop_snapshot_force_fold_over_buffer_backlog_no_premature_free() {
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    // LARGE soft trigger + NO background threads: the compactor never
    // auto-drains, so page-rc COW deltas pile up un-folded in the
    // RcShard slots until an explicit flush / lifecycle op drains them.
    // This is the "large buffer backlog" the soak hit, which the
    // soft_entries=4 tests can never build.
    cfg.l2p_buffer_soft_entries = 1_000_000;
    cfg.l2p_buffer_hard_entries = 4_000_000;
    cfg.l2p_buffer_max_interval_ms = 600_000;
    cfg.bfg_threads_enabled = false;
    let db = Db::create_with_config(cfg).unwrap();

    // Span many leaves + interior/index pages so many pids land on each
    // page-rc array DATA page (page-rc is keyed by PageId; one array data
    // page covers a contiguous PageId run → the force-fold's per-page
    // generation guard is exactly the granularity that mis-fires).
    const N: u64 = 8192;
    let val = |i: u64, round: u64| -> L2pValue { v((((i / 32) + round) % 251) as u8) };

    for i in 0..N {
        db.insert(0, i, val(i, 0)).unwrap();
    }
    db.flush().unwrap();

    // Snapshot: shares every L2P page with the live volume (page-rc 2).
    let snap = db.take_snapshot(0).unwrap();

    // Heavy overwrite churn of the shared pages, building a backlog of
    // page-rc decrefs across SEVERAL drains with non-monotone lsns. Each
    // `flush()` folds one drain into the page-rc array (advancing some
    // array pages' generation); the NEXT round's decrefs then land in a
    // fresh slot with lsns that may sit at/below an already-advanced
    // array-page generation. Interleave overwrites in a non-monotone LBA
    // order within each round so the drain's radix-key-ordered page-rc
    // staging produces the non-monotone lsns the force-fold mis-skips.
    for round in 1u64..=12 {
        // Walk LBAs in a strided / reversed order so the per-commit lsn
        // (monotone) does not match the radix-key drain order — the
        // exact condition that makes the staged page-rc delta lsns
        // non-monotone relative to the page-rc array page generation.
        let mut i = if round % 2 == 0 { N - 1 } else { 0 };
        let mut done = 0u64;
        while done < N {
            db.insert(0, i, val(i, round)).unwrap();
            done += 1;
            if round % 2 == 0 {
                if i == 0 {
                    break;
                }
                i -= 1;
            } else {
                i += 1;
                if i >= N {
                    break;
                }
            }
        }
        // Fold THIS round's drain into the page-rc array (advances array
        // page generation) but leave a fresh slot for the next round's
        // decrefs — building the mixed-generation backlog.
        db.flush().unwrap();
    }

    // Now DROP the snapshot. This is the soak trigger:
    //   force_compact_l2p_buffers (drain remaining buffered COW into the
    //     open BFG slot → fresh page-rc decrefs)
    //   then l2p_page_rc.flush() → RcShard::flush()
    //     → begin_checkpoint_all_slots(force=true)
    //   which RE-APPLIES every slot delta bypassing the
    //   `page_generation >= last_lsn` replay-skip.
    db.drop_snapshot(snap).unwrap().unwrap();

    // Every live LBA must still resolve to round 12's value. A premature
    // free shows up as a lost mapping (None), a wrong value, or an Err
    // from a corrupted page walk.
    for i in 0..N {
        assert_eq!(
            db.get(0, i).unwrap(),
            Some(val(i, 12)),
            "lba {i}: live mapping lost (force-fold double-decref premature free)"
        );
    }

    db.flush().unwrap();
    drop(db);
    let db = Db::open(dir.path()).unwrap();
    for i in 0..N {
        assert_eq!(db.get(0, i).unwrap(), Some(val(i, 12)), "reopen lba {i}");
    }
    let report = crate::verify::verify_path(
        dir.path(),
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
            check_clone_livelist: false,
            check_clone_birth_shadow: false,
        },
    )
    .unwrap();
    assert!(report.is_clean(), "verify issues: {:?}", report.issues);
}

// =====================================================================
// BIRTH-LSN SHADOW (BFG: These prove birth-shadow equivalence: the
// immutable `birth_lsn` is a reliable birth-LSN substrate — for every
// head-reachable L2P page P, `birth_lsn(P) <= youngest_snap(V)` ⟺ P is
// reachable from the youngest snapshot's tree (the exact COW kill decision
// will use). The oracle is `verify_path(.., check_birth_shadow)`.
// Page-rc stays authoritative throughout (zero behavior change).
//
// All run the check WITH A LIVE SNAPSHOT and a PARTIAL overwrite, so the
// tree carries both shared-unmodified leaves (birth <= youngest, in the
// snapshot) and diverged leaves (birth > youngest, head-only) — the
// invariant must agree on both. They reopen before verify so birth is also
// proven to survive recovery (R2).

fn assert_birth_shadow_clean(path: &std::path::Path) {
    let report = crate::verify::verify_path(
        path,
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
            check_clone_livelist: false,
            check_clone_birth_shadow: false,
        },
    )
    .unwrap();
    assert!(
        report.is_clean(),
        "birth-shadow verify issues: {:?}",
        report.issues
    );
}

// 32 leaves (128 LBAs each) under an index; value shared across 32-LBA
// groups so a leaf holds <= 4 distinct units (MAX_UNITS_PER_LEAF=100).
const BIRTH_N: u64 = 4096;
fn bval(i: u64, round: u64) -> L2pValue {
    v((((i / 32) + round) % 251) as u8)
}

#[test]
fn birth_shadow_partial_overwrite_live_snapshot_direct() {
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.shards_per_partition = 1; // one tree → LBA/128 = leaf index, predictable
    let db = Db::create_with_config(cfg).unwrap();

    for i in 0..BIRTH_N {
        db.insert(0, i, bval(i, 0)).unwrap();
    }
    db.flush().unwrap();
    let _s = db.take_snapshot(0).unwrap(); // KEEP LIVE

    // Overwrite only the first two leaves (0..256); the other 30 leaves
    // stay shared with the snapshot (birth <= youngest_snap, in both trees).
    for i in 0..256 {
        db.insert(0, i, bval(i, 1)).unwrap();
    }
    db.flush().unwrap();

    for i in 0..256 {
        assert_eq!(db.get(0, i).unwrap(), Some(bval(i, 1)));
    }
    for i in 256..BIRTH_N {
        assert_eq!(db.get(0, i).unwrap(), Some(bval(i, 0)));
    }
    db.flush().unwrap();
    drop(db);

    // Reopen (recovery replay + frontier) so birth is also proven durable.
    let db = Db::open(dir.path()).unwrap();
    for i in 0..256 {
        assert_eq!(db.get(0, i).unwrap(), Some(bval(i, 1)), "reopen lba {i}");
    }
    drop(db);
    assert_birth_shadow_clean(dir.path());
}

#[test]
fn birth_shadow_partial_overwrite_live_snapshot_buffer() {
    // BUFFER mode is the make-or-break for birth-shadow equivalence: COW (and thus birth
    // stamping) happens at DRAIN time, in radix-key order with per-entry
    // (non-monotone) lsns. If birth were stamped from the drain lsn or a
    // per-leaf-max instead of the page version's creating lsn, a shared
    // unmodified leaf would mispredict and the shadow check would trip.
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.shards_per_partition = 1;
    cfg.l2p_buffer_enabled = true;
    cfg.l2p_buffer_soft_entries = 4;
    cfg.l2p_buffer_max_interval_ms = 50;
    let db = Db::create_with_config(cfg).unwrap();

    for i in 0..BIRTH_N {
        db.insert(0, i, bval(i, 0)).unwrap();
    }
    db.flush().unwrap();
    let _s = db.take_snapshot(0).unwrap();
    for i in 0..256 {
        db.insert(0, i, bval(i, 1)).unwrap();
    }
    db.flush().unwrap();
    drop(db);

    let db = Db::open(dir.path()).unwrap();
    for i in 0..256 {
        assert_eq!(db.get(0, i).unwrap(), Some(bval(i, 1)), "reopen lba {i}");
    }
    for i in 256..BIRTH_N {
        assert_eq!(db.get(0, i).unwrap(), Some(bval(i, 0)), "reopen lba {i}");
    }
    drop(db);
    assert_birth_shadow_clean(dir.path());
}

#[test]
fn birth_shadow_non_monotone_drain_large_backlog() {
    // The force-fold case's exact condition: a large un-folded page-rc
    // backlog with NON-MONOTONE lsns (strided/reversed writes drained in
    // radix-key order), under a LIVE snapshot. The birth substrate must
    // stay correct regardless of drain/fold order.
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.shards_per_partition = 1;
    cfg.l2p_buffer_enabled = true;
    cfg.l2p_buffer_soft_entries = 1_000_000; // never auto-drain
    cfg.l2p_buffer_hard_entries = 4_000_000;
    cfg.l2p_buffer_max_interval_ms = 600_000;
    cfg.bfg_threads_enabled = false;
    let db = Db::create_with_config(cfg).unwrap();

    for i in 0..BIRTH_N {
        db.insert(0, i, bval(i, 0)).unwrap();
    }
    db.flush().unwrap();
    let _s = db.take_snapshot(0).unwrap(); // KEEP LIVE

    // Churn the first half across several rounds in strided/reversed order,
    // flushing each round (each fold advances some array pages' generation),
    // leaving the second half shared with the snapshot.
    for round in 1u64..=8 {
        let mut i = if round % 2 == 0 { 2047 } else { 0 };
        let mut done = 0u64;
        while done < 2048 {
            db.insert(0, i, bval(i, round)).unwrap();
            done += 1;
            if round % 2 == 0 {
                if i == 0 {
                    break;
                }
                i -= 1;
            } else {
                i += 1;
                if i >= 2048 {
                    break;
                }
            }
        }
        db.flush().unwrap();
    }

    for i in 0..2048 {
        assert_eq!(db.get(0, i).unwrap(), Some(bval(i, 8)));
    }
    for i in 2048..BIRTH_N {
        assert_eq!(db.get(0, i).unwrap(), Some(bval(i, 0)));
    }
    db.flush().unwrap();
    drop(db);

    let db = Db::open(dir.path()).unwrap();
    for i in 0..2048 {
        assert_eq!(db.get(0, i).unwrap(), Some(bval(i, 8)), "reopen lba {i}");
    }
    drop(db);
    assert_birth_shadow_clean(dir.path());
}

#[test]
fn birth_shadow_layered_snapshots_youngest_is_threshold() {
    // Two LIVE snapshots at different times + layered partial overwrites,
    // so the head carries three page generations (pre-s1, between s1/s2,
    // post-s2). The invariant must use the YOUNGEST snapshot as the
    // threshold (a head page in any snapshot is in the youngest).
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.shards_per_partition = 1;
    let db = Db::create_with_config(cfg).unwrap();

    for i in 0..BIRTH_N {
        db.insert(0, i, bval(i, 0)).unwrap();
    }
    db.flush().unwrap();
    let _s1 = db.take_snapshot(0).unwrap();

    // Overwrite the first half, snapshot again.
    for i in 0..2048 {
        db.insert(0, i, bval(i, 1)).unwrap();
    }
    db.flush().unwrap();
    let _s2 = db.take_snapshot(0).unwrap();

    // Overwrite the first quarter only; s1 and s2 stay live.
    for i in 0..1024 {
        db.insert(0, i, bval(i, 2)).unwrap();
    }
    db.flush().unwrap();
    drop(db);

    let db = Db::open(dir.path()).unwrap();
    for i in 0..1024 {
        assert_eq!(db.get(0, i).unwrap(), Some(bval(i, 2)), "reopen lba {i}");
    }
    for i in 1024..2048 {
        assert_eq!(db.get(0, i).unwrap(), Some(bval(i, 1)), "reopen lba {i}");
    }
    for i in 2048..BIRTH_N {
        assert_eq!(db.get(0, i).unwrap(), Some(bval(i, 0)), "reopen lba {i}");
    }
    drop(db);
    assert_birth_shadow_clean(dir.path());
}

#[test]
fn youngest_snap_tracks_max_created_lsn() {
    // `Db::youngest_snap` is the COW kill
    // threshold the BFG birth tracking () will read. It must return the
    // max live snapshot `created_lsn`, `None` when none, and never decrease
    // while snapshots accumulate.
    let (_d, db) = mk_db();
    assert_eq!(db.youngest_snap(0), None, "no snapshot → None");
    db.insert(0, 1, v(1)).unwrap();
    let _s1 = db.take_snapshot(0).unwrap();
    let y1 = db
        .youngest_snap(0)
        .expect("after first snapshot youngest_snap is Some");
    db.insert(0, 2, v(2)).unwrap();
    let s2 = db.take_snapshot(0).unwrap();
    let y2 = db.youngest_snap(0).expect("Some after second snapshot");
    assert!(
        y2 >= y1,
        "youngest_snap must not decrease as snapshots accrue"
    );
    // Dropping the youngest reverts the threshold to the older snapshot.
    db.drop_snapshot(s2).unwrap().unwrap();
    let y3 = db
        .youngest_snap(0)
        .expect("Some after dropping the youngest");
    assert!(
        y3 <= y2 && y3 >= y1,
        "after dropping the youngest, threshold falls back"
    );
}

// -------- commit 8: volume lifecycle --------
