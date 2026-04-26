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
    let keys: Vec<Hash32> = items.iter().map(|(k, _)| *k).collect();
    assert!(keys.contains(&h1));
    assert!(!keys.contains(&h2));
    assert!(keys.contains(&h3));
    assert_eq!(keys.len(), 2);
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

#[test]
fn flush_prunes_checkpointed_wal_segments() {
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.wal_segment_bytes = 512;
    let wal_dir = dir.path().join("wal");

    {
        let db = Db::create_with_config(cfg.clone()).unwrap();
        for lba in 0..100u64 {
            db.insert(0, lba, v(lba as u8)).unwrap();
        }

        let before = crate::wal::list_segments(&wal_dir).unwrap();
        assert!(
            before.len() > 1,
            "small WAL segment size should force rotation"
        );

        db.flush().unwrap();

        let after = crate::wal::list_segments(&wal_dir).unwrap();
        assert_eq!(after.len(), 1, "checkpointed WAL segments should be pruned");
    }

    let db = Db::open_with_config(cfg).unwrap();
    assert_eq!(db.get(0, 99).unwrap(), Some(v(99)));
}

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

#[test]
fn decref_underflow_errors() {
    let (_d, db) = mk_db();
    db.incref_pba(1, 2).unwrap();
    assert!(matches!(
        db.decref_pba(1, 3).unwrap_err(),
        MetaDbError::InvalidArgument(_)
    ));
}

#[test]
fn incref_overflow_errors() {
    let (_d, db) = mk_db();
    db.incref_pba(1, u32::MAX - 1).unwrap();
    assert_eq!(db.incref_pba(1, 1).unwrap(), u32::MAX);
    assert!(matches!(
        db.incref_pba(1, 1).unwrap_err(),
        MetaDbError::InvalidArgument(_)
    ));
}

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
    assert!(db.flush_dedup_memtable().unwrap());
    for i in 0u64..50 {
        assert_eq!(db.get_dedup(&h(i)).unwrap(), Some(dv(i as u8)));
    }
}

#[test]
fn cache_stats_show_hits_evictions_and_respect_budget() {
    let cache_budget = 1024 * 1024;
    let (_d, db) = mk_db_with_cache_bytes(cache_budget);

    for i in 0u64..40_000 {
        db.put_dedup(h(i), dv((i % 251) as u8)).unwrap();
    }
    assert!(db.flush_dedup_memtable().unwrap());

    let cold = db.cache_stats();
    assert_eq!(
        db.get_dedup(&h(12_345)).unwrap(),
        Some(dv((12_345 % 251) as u8))
    );
    let after_first = db.cache_stats();
    assert!(after_first.misses > cold.misses);

    assert_eq!(
        db.get_dedup(&h(12_345)).unwrap(),
        Some(dv((12_345 % 251) as u8))
    );
    let after_second = db.cache_stats();
    assert!(after_second.hits > after_first.hits);

    for i in (0u64..40_000).step_by(crate::lsm::RECORDS_PER_PAGE) {
        assert_eq!(db.get_dedup(&h(i)).unwrap(), Some(dv((i % 251) as u8)));
    }
    let after_sweep = db.cache_stats();
    assert!(after_sweep.evictions > 0);
    assert!(after_sweep.current_bytes <= cache_budget);
}

#[test]
fn dedup_survives_flush_and_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        for i in 0u64..100 {
            db.put_dedup(h(i), dv(i as u8)).unwrap();
        }
        // Force a couple of L0 SSTs so persistence is exercised.
        assert!(db.flush_dedup_memtable().unwrap());
        for i in 100u64..200 {
            db.put_dedup(h(i), dv((i % 255) as u8)).unwrap();
        }
        assert!(db.flush_dedup_memtable().unwrap());
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
    let hw_before_snap = db.high_water();
    let s = db.take_snapshot(0).unwrap();
    for pba in 0u64..200 {
        db.incref_pba(pba, 1).unwrap();
    }
    db.flush().unwrap();
    let hw_after = db.high_water();
    assert!(hw_after > hw_before_snap);
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

#[test]
fn dedup_compaction_can_be_triggered_from_db() {
    let (_d, db) = mk_db();
    for batch in 0..4u64 {
        for i in 0u64..10 {
            db.put_dedup(h(batch * 100 + i), dv(batch as u8)).unwrap();
        }
        db.flush_dedup_memtable().unwrap();
    }
    // With 4 L0 SSTs we're at the default trigger; compaction
    // should do something.
    assert!(db.compact_dedup_once().unwrap());
    for batch in 0..4u64 {
        for i in 0u64..10 {
            assert_eq!(
                db.get_dedup(&h(batch * 100 + i)).unwrap(),
                Some(dv(batch as u8)),
            );
        }
    }
}

// -------- phase 6: WAL durability --------

#[test]
fn writes_without_flush_survive_reopen_via_wal_replay() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        for i in 0u64..50 {
            db.insert(0, i, v(i as u8)).unwrap();
        }
        // NO flush() before drop — only WAL is durable.
    }
    let db = Db::open(dir.path()).unwrap();
    for i in 0u64..50 {
        assert_eq!(db.get(0, i).unwrap(), Some(v(i as u8)));
    }
}

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
fn dedup_writes_survive_reopen_without_flush() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        for i in 0u64..30 {
            db.put_dedup(h(i), dv(i as u8)).unwrap();
        }
    }
    let db = Db::open(dir.path()).unwrap();
    for i in 0u64..30 {
        assert_eq!(db.get_dedup(&h(i)).unwrap(), Some(dv(i as u8)));
    }
}

#[test]
fn multi_op_tx_commits_atomically_and_all_ops_visible() {
    let (_d, db) = mk_db();
    let mut tx = db.begin();
    tx.insert(0, 1, v(1));
    tx.insert(0, 2, v(2));
    tx.incref_pba(10, 3);
    tx.put_dedup(h(1), dv(9));
    let lsn = tx.commit().unwrap();
    assert!(lsn >= 1);
    assert_eq!(db.get(0, 1).unwrap(), Some(v(1)));
    assert_eq!(db.get(0, 2).unwrap(), Some(v(2)));
    assert_eq!(db.get_refcount(10).unwrap(), 3);
    assert_eq!(db.get_dedup(&h(1)).unwrap(), Some(dv(9)));
}

#[test]
fn multi_op_tx_survives_reopen_all_or_nothing() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        let mut tx = db.begin();
        tx.insert(0, 1, v(1));
        tx.insert(0, 2, v(2));
        tx.incref_pba(10, 3);
        tx.put_dedup(h(1), dv(9));
        tx.commit().unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get(0, 1).unwrap(), Some(v(1)));
    assert_eq!(db.get(0, 2).unwrap(), Some(v(2)));
    assert_eq!(db.get_refcount(10).unwrap(), 3);
    assert_eq!(db.get_dedup(&h(1)).unwrap(), Some(dv(9)));
}

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
fn empty_tx_commit_is_noop() {
    let (_d, db) = mk_db();
    let tx = db.begin();
    let lsn = tx.commit().unwrap();
    assert_eq!(lsn, db.last_applied_lsn());
    assert_eq!(db.last_applied_lsn(), 0);
}

#[test]
fn reopen_replay_advances_last_applied() {
    let dir = TempDir::new().unwrap();
    let committed_lsn = {
        let db = Db::create(dir.path()).unwrap();
        for i in 0u64..5 {
            db.insert(0, i, v(i as u8)).unwrap();
        }
        db.last_applied_lsn()
    };
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.last_applied_lsn(), committed_lsn);
    // New commits after reopen start at committed_lsn + 1.
    db.insert(0, 100, v(0)).unwrap();
    assert_eq!(db.last_applied_lsn(), committed_lsn + 1);
}

// -------- phase 6e: dedup_reverse --------

#[test]
fn reverse_entry_round_trip_recovers_full_hash() {
    let hash = hash_full(0xAABB_CCDD_1111_2222, 0xDEAD_BEEF_CAFE_F00D);
    let (key, value) = encode_reverse_entry(42, &hash);
    assert_eq!(&key[..8], &42u64.to_be_bytes());
    let back = decode_reverse_hash(&key, &value);
    assert_eq!(back, hash);
}

#[test]
fn register_and_scan_by_pba() {
    let (_d, db) = mk_db();
    let h1 = hash_full(1, 100);
    let h2 = hash_full(2, 200);
    db.register_dedup_reverse(42, h1).unwrap();
    db.register_dedup_reverse(42, h2).unwrap();
    db.register_dedup_reverse(99, hash_full(3, 300)).unwrap();

    let mut found = db.scan_dedup_reverse_for_pba(42).unwrap();
    found.sort();
    let mut expected = vec![h1, h2];
    expected.sort();
    assert_eq!(found, expected);

    assert_eq!(
        db.scan_dedup_reverse_for_pba(12345).unwrap(),
        Vec::<Hash32>::new()
    );
}

#[test]
fn unregister_removes_from_scan() {
    let (_d, db) = mk_db();
    let h1 = hash_full(10, 1);
    let h2 = hash_full(10, 2);
    db.register_dedup_reverse(7, h1).unwrap();
    db.register_dedup_reverse(7, h2).unwrap();
    db.unregister_dedup_reverse(7, h1).unwrap();
    let found = db.scan_dedup_reverse_for_pba(7).unwrap();
    assert_eq!(found, vec![h2]);
}

#[test]
fn scan_sees_entries_after_flush_to_sst() {
    let (_d, db) = mk_db();
    for i in 0u64..25 {
        db.register_dedup_reverse(17, hash_full(i, i)).unwrap();
    }
    // Flush the dedup_reverse memtable so the next scan reads SST data.
    let lsn = db.last_applied_lsn();
    db.dedup_reverse.flush_memtable(lsn).unwrap();
    let found = db.scan_dedup_reverse_for_pba(17).unwrap();
    assert_eq!(found.len(), 25);
}

#[test]
fn tx_atomically_registers_dedup_index_and_reverse() {
    let (_d, db) = mk_db();
    let hash = hash_full(5, 5);
    let mut tx = db.begin();
    tx.put_dedup(hash, dv(7));
    tx.register_dedup_reverse(999, hash);
    tx.incref_pba(999, 1);
    tx.commit().unwrap();
    assert_eq!(db.get_dedup(&hash).unwrap(), Some(dv(7)));
    assert_eq!(db.scan_dedup_reverse_for_pba(999).unwrap(), vec![hash]);
    assert_eq!(db.get_refcount(999).unwrap(), 1);
}

#[test]
fn dedup_reverse_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let h_a = hash_full(77, 1);
    let h_b = hash_full(77, 2);
    {
        let db = Db::create(dir.path()).unwrap();
        db.register_dedup_reverse(77, h_a).unwrap();
        db.register_dedup_reverse(77, h_b).unwrap();
        // flush half so we exercise both WAL-replay AND
        // SST-reload paths.
        let lsn = db.last_applied_lsn();
        db.dedup_reverse.flush_memtable(lsn).unwrap();
        db.flush().unwrap();
        db.register_dedup_reverse(77, hash_full(77, 3)).unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    let mut found = db.scan_dedup_reverse_for_pba(77).unwrap();
    found.sort();
    let mut expected = vec![h_a, h_b, hash_full(77, 3)];
    expected.sort();
    assert_eq!(found, expected);
}

// -------- batch read API --------------------------------------------
