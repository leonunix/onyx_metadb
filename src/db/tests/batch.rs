use super::*;

#[test]
fn multi_get_empty_input_returns_empty() {
    let (_d, db) = mk_db();
    assert!(db.multi_get(0, &[]).unwrap().is_empty());
    assert!(db.multi_get_refcount(&[]).unwrap().is_empty());
    assert!(db.multi_get_dedup(&[]).unwrap().is_empty());
    assert!(db.multi_scan_dedup_reverse_for_pba(&[]).unwrap().is_empty());
}

#[test]
fn multi_get_matches_single_gets_across_shards() {
    // 4 shards so we actually exercise the bucket + group logic.
    let (_d, db) = mk_db_with_shards(4);
    for i in 0u64..200 {
        db.insert(0, i, v((i as u8).wrapping_mul(3))).unwrap();
    }
    // Mix mapped + unmapped + duplicate keys, in non-sorted order.
    let keys = vec![199, 5000, 0, 199, 42, 10_000, 1, 42];
    let got = db.multi_get(0, &keys).unwrap();
    assert_eq!(got.len(), keys.len());
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(got[i], db.get(0, *key).unwrap(), "key {key} mismatch");
    }
}

#[test]
fn multi_get_refcount_matches_single_gets_across_shards() {
    let (_d, db) = mk_db_with_shards(4);
    for pba in 0u64..100 {
        db.incref_pba(pba, (pba as u32 % 5) + 1).unwrap();
    }
    let pbas: Vec<Pba> = vec![99, 0, 50, 9999, 42, 50, 1, 2, 9999];
    let got = db.multi_get_refcount(&pbas).unwrap();
    assert_eq!(got.len(), pbas.len());
    for (i, pba) in pbas.iter().enumerate() {
        assert_eq!(got[i], db.get_refcount(*pba).unwrap(), "pba {pba} mismatch",);
    }
}

#[test]
fn multi_get_dedup_hits_memtable_and_sst() {
    let (_d, db) = mk_db();
    // First half lands in L0 after the flush; second half stays in
    // the memtable. Makes sure the multi-key path walks both.
    for i in 0u64..40 {
        db.put_dedup(h(i), dv(i as u8)).unwrap();
    }
    assert!(db.flush_dedup_memtable().unwrap());
    for i in 40u64..80 {
        db.put_dedup(h(i), dv(i as u8)).unwrap();
    }
    // Include a tombstoned key and an unknown key.
    db.delete_dedup(h(5)).unwrap();
    let hashes = vec![h(0), h(5), h(39), h(40), h(79), h(999), h(0)];
    let got = db.multi_get_dedup(&hashes).unwrap();
    assert_eq!(got.len(), hashes.len());
    for (i, hash) in hashes.iter().enumerate() {
        assert_eq!(got[i], db.get_dedup(hash).unwrap(), "hash {i} mismatch");
    }
}

#[test]
fn multi_scan_dedup_reverse_preserves_order_and_per_pba_rows() {
    let (_d, db) = mk_db();
    // pba=10 has two hashes, pba=20 has three, pba=30 has zero.
    // Flush some to force the cross-layer code path (memtable +
    // SST) for at least one PBA.
    db.register_dedup_reverse(10, hash_full(10, 1)).unwrap();
    db.register_dedup_reverse(10, hash_full(10, 2)).unwrap();
    let flush_lsn = db.last_applied_lsn();
    db.dedup_reverse.flush_memtable(flush_lsn).unwrap();
    db.register_dedup_reverse(20, hash_full(20, 1)).unwrap();
    db.register_dedup_reverse(20, hash_full(20, 2)).unwrap();
    db.register_dedup_reverse(20, hash_full(20, 3)).unwrap();

    // Include a repeated PBA to make sure the batched impl doesn't
    // de-duplicate or collapse results.
    let pbas: Vec<Pba> = vec![30, 20, 10, 20];
    let batched = db.multi_scan_dedup_reverse_for_pba(&pbas).unwrap();
    assert_eq!(batched.len(), pbas.len());
    for (i, pba) in pbas.iter().enumerate() {
        let mut expected = db.scan_dedup_reverse_for_pba(*pba).unwrap();
        let mut got = batched[i].clone();
        expected.sort();
        got.sort();
        assert_eq!(got, expected, "pba {pba} mismatch");
    }
}

#[test]
fn multi_scan_dedup_reverse_hides_tombstoned_rows_explicitly() {
    let (_d, db) = mk_db();
    let p10_a = hash_full(10, 1);
    let p10_b = hash_full(10, 2);
    let p10_c = hash_full(10, 3);
    let p20_a = hash_full(20, 1);

    // Oldest persisted layer.
    db.register_dedup_reverse(10, p10_a).unwrap();
    db.register_dedup_reverse(10, p10_b).unwrap();
    db.register_dedup_reverse(20, p20_a).unwrap();
    let flush_lsn = db.last_applied_lsn();
    db.dedup_reverse.flush_memtable(flush_lsn).unwrap();

    // Newer memtable updates: remove one old row, add a new one.
    db.unregister_dedup_reverse(10, p10_a).unwrap();
    db.register_dedup_reverse(10, p10_c).unwrap();

    let pbas: Vec<Pba> = vec![10, 20, 30, 10];
    let mut got = db.multi_scan_dedup_reverse_for_pba(&pbas).unwrap();
    for rows in &mut got {
        rows.sort();
    }

    assert_eq!(got[0], vec![p10_b, p10_c]);
    assert_eq!(got[1], vec![p20_a]);
    assert!(got[2].is_empty());
    assert_eq!(got[3], vec![p10_b, p10_c]);
}

// -------- P1: bucketed batch apply ----------------------------------

/// Bucketed apply must produce the same per-op outcomes and the
/// same final state as the serial path, regardless of shard
/// routing. Batch is sized above BUCKET_THRESHOLD so the bucket
/// branch is exercised.
#[test]
fn bucketed_apply_matches_serial_for_mixed_batch() {
    let (_d, db) = mk_db_with_shards(4);
    let mut tx = db.begin();
    // 16 L2P puts across likely many shards.
    for i in 0..16u64 {
        tx.insert(BOOTSTRAP_VOLUME_ORD, i * 37, v((i & 0xff) as u8));
    }
    // 12 refcount ops; Incref first so Decref has something to
    // subtract from on the same pba.
    for pba in 0..12u64 {
        tx.incref_pba(pba, 3);
    }
    for pba in 0..6u64 {
        tx.decref_pba(pba, 1);
    }
    // 4 dedup ops.
    for i in 0..4u64 {
        tx.put_dedup(hash_bytes(0xAAAA, i), dedup_val(i as u8));
    }
    let (_lsn, outcomes) = tx.commit_with_outcomes().unwrap();
    // 16 L2P + 12 Incref + 6 Decref + 4 Dedup
    assert_eq!(outcomes.len(), 16 + 18 + 4);
    // Verify a representative subset of the resulting state.
    for i in 0..16u64 {
        assert_eq!(
            db.get(BOOTSTRAP_VOLUME_ORD, i * 37).unwrap(),
            Some(v((i & 0xff) as u8)),
        );
    }
    for pba in 0..6u64 {
        assert_eq!(db.get_refcount(pba).unwrap(), 2); // 3 - 1
    }
    for pba in 6..12u64 {
        assert_eq!(db.get_refcount(pba).unwrap(), 3);
    }
}

/// Same-shard puts in the same batch must apply in caller order,
/// not reordered. Two puts to the same (vol, lba) in order A, B
/// must leave the value = B.
#[test]
fn bucketed_apply_preserves_intra_bucket_order() {
    let (_d, db) = mk_db_with_shards(2);
    let mut tx = db.begin();
    // Pad with other L2P ops so the batch size crosses the
    // bucketing threshold.
    for i in 0..12u64 {
        tx.insert(BOOTSTRAP_VOLUME_ORD, i + 1_000, v(i as u8));
    }
    // Three puts to the same key in order.
    tx.insert(BOOTSTRAP_VOLUME_ORD, 9_999, v(0x10));
    tx.insert(BOOTSTRAP_VOLUME_ORD, 9_999, v(0x20));
    tx.insert(BOOTSTRAP_VOLUME_ORD, 9_999, v(0x30));
    tx.commit().unwrap();
    assert_eq!(
        db.get(BOOTSTRAP_VOLUME_ORD, 9_999).unwrap(),
        Some(v(0x30)),
        "intra-bucket order must preserve last-write-wins",
    );
}

/// Same-pba incref/decref in the same batch must apply in caller
/// order. A decref before its paired incref would underflow.
#[test]
fn bucketed_apply_preserves_intra_bucket_rc_order() {
    let (_d, db) = mk_db_with_shards(2);
    let mut tx = db.begin();
    // Pad so batch >= threshold.
    for pba in 0..10u64 {
        tx.incref_pba(pba + 100, 1);
    }
    // Incref then Decref same pba — ordering must be preserved.
    tx.incref_pba(77, 5);
    tx.decref_pba(77, 2);
    tx.commit().unwrap();
    assert_eq!(db.get_refcount(77).unwrap(), 3);
}

/// Batches smaller than the bucket threshold fall through to the
/// serial path. This test is a behavioural smoke check (the small
/// batch must still apply correctly).
#[test]
fn small_batch_falls_through_serial_path() {
    let (_d, db) = mk_db_with_shards(2);
    let mut tx = db.begin();
    // Below BUCKET_THRESHOLD (= 8).
    for i in 0..4u64 {
        tx.insert(BOOTSTRAP_VOLUME_ORD, i, v(i as u8));
    }
    tx.commit().unwrap();
    for i in 0..4u64 {
        assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, i).unwrap(), Some(v(i as u8)));
    }
}

/// Large pure-L2P batch — the main target of the optimisation.
/// Each shard is locked once per commit, not once per op.
#[test]
fn bucketed_apply_large_pure_l2p_batch() {
    let (_d, db) = mk_db_with_shards(8);
    let mut tx = db.begin();
    // 1024 keys with a prime stride so shard routing (xxh3-based)
    // is non-trivial while staying within the legal LBA range
    // (paged tree caps at MAX_INDEX_LEVEL=4).
    for i in 0..1024u64 {
        let lba = i.wrapping_mul(7919);
        tx.insert(BOOTSTRAP_VOLUME_ORD, lba, v((i & 0xff) as u8));
    }
    tx.commit().unwrap();
    for i in 0..1024u64 {
        let lba = i.wrapping_mul(7919);
        assert_eq!(
            db.get(BOOTSTRAP_VOLUME_ORD, lba).unwrap(),
            Some(v((i & 0xff) as u8)),
        );
    }
}

/// Helper used by `batch_contains_lifecycle_op` covers every
/// lifecycle variant — spot-check via direct unit tests since
/// these ops cannot be pushed through Transaction.
#[test]
fn lifecycle_predicate_detects_every_lifecycle_variant() {
    assert!(!batch_contains_lifecycle_op(&[WalOp::L2pPut {
        vol_ord: 0,
        lba: 0,
        value: v(0),
    }]));
    assert!(batch_contains_lifecycle_op(&[WalOp::DropSnapshot {
        id: 1,
        pages: Vec::new(),
        pba_decrefs: Vec::new(),
    }]));
    assert!(batch_contains_lifecycle_op(&[WalOp::CreateVolume {
        ord: 1,
        shard_count: 1,
    }]));
    assert!(batch_contains_lifecycle_op(&[WalOp::DropVolume {
        ord: 1,
        pages: Vec::new(),
    }]));
    assert!(batch_contains_lifecycle_op(&[WalOp::CloneVolume {
        src_ord: 0,
        new_ord: 1,
        src_snap_id: 0,
        src_shard_roots: Vec::new(),
    }]));
    // L2pRemap is forced through the serial path because it
    // straddles L2P and refcount shards — see apply_l2p_remap.
    assert!(batch_contains_lifecycle_op(&[WalOp::L2pRemap {
        vol_ord: 0,
        lba: 0,
        new_value: v(0),
        guard: None,
    }]));
}

// ---------------- L2pRemap apply (SPEC §3.1) ---------------------
