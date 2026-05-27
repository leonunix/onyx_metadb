use super::*;

#[test]
fn iter_refcounts_empty_db_returns_empty() {
    let (_d, db) = mk_db();
    let items: Vec<_> = db
        .iter_refcounts()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert!(items.is_empty());
}

#[test]
fn iter_refcounts_emits_all_entries_sorted_by_pba() {
    let (_d, db) = mk_db();
    for (pba, delta) in [(100u64, 7u32), (50, 3), (200, 1), (10, 5)] {
        db.incref_pba(pba, delta).unwrap();
    }
    let items: Vec<(Pba, u32)> = db
        .iter_refcounts()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(items, vec![(10, 5), (50, 3), (100, 7), (200, 1)]);
}

#[test]
fn iter_refcounts_hides_decremented_to_zero() {
    let (_d, db) = mk_db();
    db.incref_pba(42, 2).unwrap();
    db.decref_pba(42, 2).unwrap(); // rc back to 0 → row removed
    db.incref_pba(99, 1).unwrap();
    let items: Vec<(Pba, u32)> = db
        .iter_refcounts()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(items, vec![(99, 1)]);
}

#[test]
fn iter_dedup_empty_db_returns_empty() {
    let (_d, db) = mk_db();
    let items: Vec<_> = db
        .iter_dedup()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert!(items.is_empty());
}

#[test]
fn iter_dedup_emits_live_puts_and_hides_tombstones() {
    let (_d, db) = mk_db();
    let h1 = h(1);
    let h2 = h(2);
    let h3 = h(3);
    db.put_dedup(h1, dv(1)).unwrap();
    db.put_dedup(h2, dv(2)).unwrap();
    db.put_dedup(h3, dv(3)).unwrap();
    db.delete_dedup(h2).unwrap();
    let items: Vec<_> = db
        .iter_dedup()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let keys: Vec<Hash8> = items.iter().map(|(k, _)| *k).collect();
    assert!(keys.contains(&h1));
    assert!(!keys.contains(&h2));
    assert!(keys.contains(&h3));
    assert_eq!(keys.len(), 2);
}

#[test]
fn compare_delete_dedup_requires_exact_current_value() {
    let (_d, db) = mk_db();
    let hash = h(11);
    let old = dv(1);
    let other = dv(2);

    db.put_dedup(hash, old).unwrap();
    assert!(!db.compare_delete_dedup(hash, other).unwrap());
    assert_eq!(db.get_dedup(&hash).unwrap(), Some(old));

    assert!(db.compare_delete_dedup(hash, old).unwrap());
    assert_eq!(db.get_dedup(&hash).unwrap(), None);
}

#[test]
fn compare_put_dedup_requires_exact_current_value() {
    let (_d, db) = mk_db();
    let hash = h(12);
    let old = dv(3);
    let new = dv(4);
    let other = dv(5);

    db.put_dedup(hash, old).unwrap();
    assert!(!db.compare_put_dedup(hash, other, new).unwrap());
    assert_eq!(db.get_dedup(&hash).unwrap(), Some(old));

    assert!(db.compare_put_dedup(hash, old, new).unwrap());
    assert_eq!(db.get_dedup(&hash).unwrap(), Some(new));
}

#[test]
fn iter_dedup_survives_flush_and_reopen() {
    let dir = TempDir::new().unwrap();
    let h1 = h(7);
    let h2 = h(8);
    {
        let db = Db::create(dir.path()).unwrap();
        db.put_dedup(h1, dv(1)).unwrap();
        db.put_dedup(h2, dv(2)).unwrap();
        db.flush().unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    let items: Vec<_> = db
        .iter_dedup()
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(items.len(), 2);
}

// Phase D.5: `flush_prunes_checkpointed_wal_segments` tested
// post-checkpoint WAL segment pruning — the WAL is gone.

#[test]
fn range_stream_matches_range() {
    let (_d, db) = mk_db();
    for i in 0u64..20 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    let lazy: Vec<_> = db
        .range_stream(0, 5..15)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let eager: Vec<_> = db
        .range(0, 5..15)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(lazy, eager);
    assert_eq!(lazy.len(), 10);
}

#[test]
fn range_stream_routes_per_volume() {
    let (_d, db) = mk_db();
    let a = db.create_volume().unwrap();
    db.insert(0, 1, v(0)).unwrap();
    db.insert(a, 1, v(1)).unwrap();
    let on_boot: Vec<_> = db
        .range_stream(0, ..)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let on_a: Vec<_> = db
        .range_stream(a, ..)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(on_boot, vec![(1, v(0))]);
    assert_eq!(on_a, vec![(1, v(1))]);
}

#[test]
fn scan_range_unordered_visits_range_without_materialising_sorted_iter() {
    let (_d, db) = mk_db();
    for i in 0u64..64 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    let mut got = Vec::new();
    db.scan_range_unordered(0, 7..23, |lba, value| {
        got.push((lba, value));
        Ok(())
    })
    .unwrap();
    got.sort_unstable_by_key(|(lba, _)| *lba);
    let expected: Vec<_> = (7u64..23).map(|lba| (lba, v(lba as u8))).collect();
    assert_eq!(got, expected);
}

#[test]
fn scan_range_unordered_chunked_visits_each_chunk() {
    let (_d, db) = mk_db();
    for i in 0u64..64 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    let mut got = Vec::new();
    db.scan_range_unordered_chunked(0, 7, 23, 5, |lba, value| {
        got.push((lba, value));
        Ok(())
    })
    .unwrap();
    got.sort_unstable_by_key(|(lba, _)| *lba);
    let expected: Vec<_> = (7u64..23).map(|lba| (lba, v(lba as u8))).collect();
    assert_eq!(got, expected);
}

// -------- phase 5e: refcount + dedup integration --------

#[test]
fn refcount_fresh_pba_reads_as_zero() {
    let (_d, db) = mk_db();
    assert_eq!(db.get_refcount(1234).unwrap(), 0);
}

#[test]
fn incref_and_decref_roundtrip() {
    let (_d, db) = mk_db();
    assert_eq!(db.incref_pba(42, 1).unwrap(), 1);
    assert_eq!(db.incref_pba(42, 1).unwrap(), 2);
    assert_eq!(db.incref_pba(42, 3).unwrap(), 5);
    assert_eq!(db.get_refcount(42).unwrap(), 5);
    assert_eq!(db.decref_pba(42, 2).unwrap(), 3);
    assert_eq!(db.decref_pba(42, 3).unwrap(), 0);
    // Row should be gone.
    assert_eq!(db.get_refcount(42).unwrap(), 0);
}

// Phase 5: `decref_underflow_errors` removed — `Db::decref_pba` now
// drives `commit_free_pbas` which never underflows (rc>0 → decref by 1;
// rc==0 → exclusive surface, no error). The underlying refcount
// overflow/underflow guard is covered by the rc shard's own unit tests
// in `src/refcount/`.
//
// `incref_overflow_errors` removed — `Db::incref_pba` now drives
// `commit_promotion_chunk` so driving u32::MAX worth of staged ops
// requires hundreds of lifecycle records and would take minutes. The
// overflow guard sits in `RcShard::stage` / `apply_delta_pure` and is
// covered by `refcount::apply_delta_tests::overflow_errors`.

#[test]
fn refcount_survives_flush_and_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        for pba in 0u64..100 {
            db.incref_pba(pba, (pba as u32 % 7) + 1).unwrap();
        }
        db.flush().unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    for pba in 0u64..100 {
        assert_eq!(
            db.get_refcount(pba).unwrap(),
            (pba as u32 % 7) + 1,
            "pba {pba} mismatch after reopen",
        );
    }
}

#[test]
fn dedup_put_get_roundtrip_via_memtable() {
    let (_d, db) = mk_db();
    db.put_dedup(h(1), dv(10)).unwrap();
    db.put_dedup(h(2), dv(20)).unwrap();
    assert_eq!(db.get_dedup(&h(1)).unwrap(), Some(dv(10)));
    assert_eq!(db.get_dedup(&h(2)).unwrap(), Some(dv(20)));
    assert_eq!(db.get_dedup(&h(3)).unwrap(), None);
}

#[test]
fn dedup_put_guarded_respects_refcount_guard() {
    let (_d, db) = mk_db();
    let live_hash = h(10);
    let dead_hash = h(11);
    db.incref_pba(123, 1).unwrap();

    let mut live_tx = db.begin();
    live_tx.put_dedup_guarded(live_hash, dv(10), 123, 1);
    live_tx.commit().unwrap();

    let mut dead_tx = db.begin();
    dead_tx.put_dedup_guarded(dead_hash, dv(11), 124, 1);
    dead_tx.commit().unwrap();

    assert_eq!(db.get_dedup(&live_hash).unwrap(), Some(dv(10)));
    assert_eq!(db.get_dedup(&dead_hash).unwrap(), None);
}

#[test]
fn dedup_delete_tombstones_key() {
    let (_d, db) = mk_db();
    db.put_dedup(h(1), dv(10)).unwrap();
    db.delete_dedup(h(1)).unwrap();
    assert_eq!(db.get_dedup(&h(1)).unwrap(), None);
}

#[test]
fn dedup_flush_to_l0_then_read() {
    let (_d, db) = mk_db();
    for i in 0u64..50 {
        db.put_dedup(h(i), dv(i as u8)).unwrap();
    }
    db.flush().unwrap();
    for i in 0u64..50 {
        assert_eq!(db.get_dedup(&h(i)).unwrap(), Some(dv(i as u8)));
    }
}

// `dedup_flush_to_l0_flushes_reverse_index_too` retired alongside the
// paged_reverse module (manifest v9 / WAL 0xB3).

#[test]
fn dedup_survives_flush_and_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        for i in 0u64..100 {
            db.put_dedup(h(i), dv(i as u8)).unwrap();
        }
        db.flush().unwrap();
        for i in 100u64..200 {
            db.put_dedup(h(i), dv((i % 255) as u8)).unwrap();
        }
        db.flush().unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    for i in 0u64..100 {
        assert_eq!(db.get_dedup(&h(i)).unwrap(), Some(dv(i as u8)));
    }
    for i in 100u64..200 {
        assert_eq!(db.get_dedup(&h(i)).unwrap(), Some(dv((i % 255) as u8)));
    }
}

#[test]
fn take_snapshot_captures_refcount_state() {
    let (_d, db) = mk_db();
    for pba in 0u64..50 {
        db.incref_pba(pba, 1).unwrap();
    }
    let _snap = db.take_snapshot(0).unwrap();
    // Overwrite refcount state after the snapshot.
    for pba in 0u64..50 {
        db.incref_pba(pba, 1).unwrap();
    }
    for pba in 0u64..50 {
        assert_eq!(db.get_refcount(pba).unwrap(), 2);
    }
    // Phase 6.5b retired refcount snapshots; v6 SnapshotEntry no
    // longer carries refcount fields at all. L2P roots page is still
    // allocated because L2P tree IS snapshotted.
    let snap_entry = &db.snapshots()[0];
    assert_ne!(snap_entry.l2p_roots_page, crate::types::NULL_PAGE);
    assert_eq!(snap_entry.vol_ord, BOOTSTRAP_VOLUME_ORD);
}

#[test]
fn drop_snapshot_releases_refcount_state() {
    let (_d, db) = mk_db();
    for pba in 0u64..200 {
        db.incref_pba(pba, 1).unwrap();
    }
    db.flush().unwrap();
    let s = db.take_snapshot(0).unwrap();
    for pba in 0u64..200 {
        db.incref_pba(pba, 1).unwrap();
    }
    db.flush().unwrap();
    // This test has no L2P inserts between take/drop, so the L2P
    // tree never diverges and no tree pages hit rc=0 during the
    // drop. The WAL-logged drop just decrements the shared root's
    // rc back to 1; `pages_freed` is 0 because the only page that
    // becomes orphan (`entry.l2p_roots_page`) is deliberately left
    // for the next flush + reclaim pass rather than freed inline
    // (see `drop_snapshot` for why). The invariant we still care
    // about is that refcount state is preserved end-to-end.
    let _ = db.drop_snapshot(s).unwrap().unwrap();
    for pba in 0u64..200 {
        assert_eq!(db.get_refcount(pba).unwrap(), 2);
    }
}

// -------- phase 6: post-flush durability --------
//
// Phase D.5: the historical `*_survive_reopen_without_flush` tests
// exercised WAL replay across reopen and have been removed alongside
// the WAL subsystem. `refcount_writes_survive_reopen_without_flush`
// stays because refcount shards persist data pages synchronously per
// op and survive reopen without any journal involvement.

#[test]
fn refcount_writes_survive_reopen_without_flush() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        for pba in 0u64..100 {
            db.incref_pba(pba, (pba as u32 % 3) + 1).unwrap();
        }
    }
    let db = Db::open(dir.path()).unwrap();
    for pba in 0u64..100 {
        assert_eq!(db.get_refcount(pba).unwrap(), (pba as u32 % 3) + 1);
    }
}

#[test]
fn multi_op_tx_commits_atomically_and_all_ops_visible() {
    let (_d, db) = mk_db();
    let mut tx = db.begin();
    tx.insert(0, 1, v(1));
    tx.insert(0, 2, v(2));
    tx.put_dedup(h(1), dv(9));
    let lsn = tx.commit().unwrap();
    assert!(lsn >= 1);
    // The hot-path commit emits `DedupPut` alongside L2P ops, which
    // routes the dedup apply through the async lane (production
    // semantic is eventual-consistency for the dedup hint table).
    // Drain so the read-after-write assertion below observes the put.
    db.wait_apply_idle();
    assert_eq!(db.get(0, 1).unwrap(), Some(v(1)));
    assert_eq!(db.get(0, 2).unwrap(), Some(v(2)));
    assert_eq!(db.get_dedup(&h(1)).unwrap(), Some(dv(9)));
}

// Phase D.5: `multi_op_tx_survives_reopen_all_or_nothing` tested
// WAL-record atomicity across reopen — the WAL is gone. Multi-op tx
// atomicity within a single commit is still covered by
// `multi_op_tx_commits_atomically_and_all_ops_visible` (the
// in-memory variant directly above).

#[test]
fn last_applied_lsn_advances_per_commit() {
    let (_d, db) = mk_db();
    let before = db.last_applied_lsn();
    db.insert(0, 1, v(1)).unwrap();
    let after_one = db.last_applied_lsn();
    assert!(after_one > before);
    db.incref_pba(100, 1).unwrap();
    let after_two = db.last_applied_lsn();
    assert!(after_two > after_one);
}

#[test]
fn checkpoint_advances_on_flush() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    for i in 0u64..10 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    let applied = db.last_applied_lsn();
    assert_eq!(db.manifest().checkpoint_lsn, 0);
    db.flush().unwrap();
    assert_eq!(db.manifest().checkpoint_lsn, applied);
}

#[test]
fn unlogged_commit_survives_reopen_after_flush() {
    let dir = TempDir::new().unwrap();
    let applied = {
        let mut cfg = Config::new(dir.path());
        cfg.unlogged_commits_enabled = true;
        let db = Db::create_with_config(cfg).unwrap();
        let mut tx = db.begin();
        tx.insert(0, 7, v(7));
        let (lsn, outcomes) = tx.commit_unlogged_with_outcomes().unwrap();
        assert_eq!(outcomes.len(), 1);
        assert_eq!(db.last_applied_lsn(), lsn);
        db.flush().unwrap();
        assert_eq!(db.manifest().checkpoint_lsn, lsn);
        lsn
    };
    let mut cfg = Config::new(dir.path());
    cfg.unlogged_commits_enabled = true;
    let db = Db::open_with_config(cfg).unwrap();
    assert_eq!(db.last_applied_lsn(), applied);
    assert_eq!(db.get(0, 7).unwrap(), Some(v(7)));
}

#[test]
fn empty_tx_commit_is_noop() {
    let (_d, db) = mk_db();
    let tx = db.begin();
    let lsn = tx.commit().unwrap();
    assert_eq!(lsn, db.last_applied_lsn());
    assert_eq!(db.last_applied_lsn(), 0);
}

// Phase D.5: `reopen_replay_advances_last_applied` exercised WAL
// replay of `last_applied_lsn` advancement across reopen — the WAL
// is gone. Buffer-mode reopen resumes at `manifest.checkpoint_lsn`,
// which is tested by `checkpoint_advances_on_flush` (live commit)
// and the flush + reopen pattern across `db_buffer_journal_*` tests.

#[test]
fn apply_lane_h2_metrics_record_wakeups_and_bursts() {
    // Smoke test for the H2 apply-lane wakeup / burst counters added on
    // 2026-05-10. After at least one commit, the L2P lane must have been
    // woken at least once (the worker starts idle).
    //
    // Phase 5: refcount lane no longer wakes from L2pRemap commits
    // (hot-path RC was removed). `Db::incref_pba` drives
    // `commit_promotion_chunk` through `apply_op_bare`, not through the
    // per-shard rc apply lane; the RC lane wakeup assertion has been
    // dropped. The L2P lane assertions still hold.
    let (_d, db) = mk_db();
    for i in 0u64..16 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    // Wait a beat so the per-shard lane workers go back to idle on the
    // cvar. Without this, the burst we just produced hasn't been closed
    // by a wait and `burst_total` may still be zero.
    std::thread::sleep(std::time::Duration::from_millis(50));
    // One more op forces a wakeup-then-pop, which records the burst
    // that closed when the lane parked above.
    db.insert(0, 1_000, v(0)).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(50));

    let m = db.metrics_snapshot();
    assert!(
        m.l2p_apply_lane_wakeups > 0,
        "L2P lane should wake from cvar at least once: {}",
        m.l2p_apply_lane_wakeups,
    );
    assert!(
        m.l2p_apply_lane_burst_total > 0,
        "L2P burst total must include closed burst(s): {}",
        m.l2p_apply_lane_burst_total,
    );
    assert!(
        m.l2p_apply_lane_burst_max > 0,
        "L2P burst max must be set: {}",
        m.l2p_apply_lane_burst_max,
    );
    assert!(
        m.l2p_apply_lane_tasks >= m.l2p_apply_lane_burst_total,
        "tasks ({}) cannot be lower than sum of bursts ({})",
        m.l2p_apply_lane_tasks,
        m.l2p_apply_lane_burst_total,
    );
}

// The "phase 6e: dedup_reverse" test block (round_trip / register_and_scan /
// unregister / scan_sees_entries / tx_atomically / survives_reopen) retired
// alongside the paged_reverse module + DedupReverse WAL ops (manifest v9 /
// WAL 0xB3). Onyx no longer registers reverse entries — promote-on-verified-hit
// uses old-mapping read-back instead.

// -------- batch read API --------------------------------------------
