use super::*;

#[test]
fn multi_get_empty_input_returns_empty() {
    let (_d, db) = mk_db();
    assert!(db.multi_get(0, &[]).unwrap().is_empty());
    assert!(db.multi_get_refcount(&[]).unwrap().is_empty());
    assert!(db.multi_get_dedup(&[]).unwrap().is_empty());
}

#[test]
fn multi_get_matches_single_gets_across_shards() {
    // 4 shards so we actually exercise the bucket + group logic.
    let (_d, db) = mk_db_with_shards(4);
    // v2 MAX_UNITS_PER_LEAF = 100; share value bytes across 32-LBA
    // groups so each 128-LBA leaf only references ≤ 4 distinct units.
    for i in 0u64..200 {
        db.insert(0, i, v(((i / 32) as u8).wrapping_mul(3))).unwrap();
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
        let delta = (pba as u32 % 5) + 1;
        db.incref_pba(pba, delta).unwrap();
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
    db.flush().unwrap();
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
fn multi_dedup_entries_are_live_matches_forward_and_refcount() {
    // `put_dedup` issues `WalOp::DedupPut`, whose apply
    // increfs the head PBA's rc by 1. The "zero_rc" arm therefore
    // has to drive the rc back down explicitly via FreePbas (the
    // `decref_pba` test helper) to model a forward entry whose
    // underlying PBA has been retired.
    let (_d, db) = mk_db_with_shards(4);
    let live = h(1);
    let zero_rc = h(2);
    let replaced = h(3);
    let missing = h(4);
    let live_value = dv(10);
    let zero_value = dv(20);
    let old_value = dv(30);
    let new_value = dv(31);
    let missing_value = dv(40);

    db.put_dedup(live, live_value).unwrap();
    db.put_dedup(zero_rc, zero_value).unwrap();
    db.put_dedup(replaced, old_value).unwrap();
    db.put_dedup(replaced, new_value).unwrap();
    db.incref_pba(live_value.head_pba(), 1).unwrap();
    db.incref_pba(old_value.head_pba(), 1).unwrap();
    db.incref_pba(new_value.head_pba(), 1).unwrap();
    db.incref_pba(missing_value.head_pba(), 1).unwrap();
    // Drive rc(zero_value.head_pba()) back to 0 to model a stale
    // forward entry whose PBA has been retired.
    db.decref_pba(zero_value.head_pba(), 1).unwrap();

    let got = db
        .multi_dedup_entries_are_live(&[
            (live, live_value),
            (zero_rc, zero_value),
            (replaced, old_value),
            (missing, missing_value),
            (live, live_value),
        ])
        .unwrap();

    assert_eq!(got, vec![true, false, false, false, true]);
}

// The `multi_scan_dedup_reverse_*` tests retired alongside the
// paged_reverse module + DedupReverse WAL ops (manifest v9 / WAL 0xB3).

// -------- P1: bucketed batch apply ----------------------------------

/// Bucketed apply must produce the same per-op outcomes and the
/// same final state as the serial path, regardless of shard
/// routing. Batch is sized above BUCKET_THRESHOLD so the bucket
/// branch is exercised. retired the standalone Incref /
/// Decref WAL ops, so the mixed batch here covers L2P + dedup only.
#[test]
fn bucketed_apply_matches_serial_for_mixed_batch() {
    let (_d, db) = mk_db_with_shards(4);
    let mut tx = db.begin();
    // 16 L2P puts across likely many shards.
    for i in 0..16u64 {
        tx.insert(BOOTSTRAP_VOLUME_ORD, i * 37, v((i & 0xff) as u8));
    }
    // 4 dedup ops.
    for i in 0..4u64 {
        tx.put_dedup(hash_bytes(0xAAAA, i), dedup_val(i as u8));
    }
    let (_lsn, outcomes) = tx.commit_with_outcomes().unwrap();
    // 16 L2P + 4 Dedup
    assert_eq!(outcomes.len(), 16 + 4);
    // Verify a representative subset of the resulting state.
    for i in 0..16u64 {
        assert_eq!(
            db.get(BOOTSTRAP_VOLUME_ORD, i * 37).unwrap(),
            Some(v((i & 0xff) as u8)),
        );
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

// retired the standalone Incref / Decref WAL ops, so the
// "intra-bucket rc order" test no longer has a code path to exercise
// — refcount mutations now arrive only via PromotionChunk / FreePbas /
// volume-lifecycle ops, and each of those orders its own work.

/// Small batches also use the lane path now; this is a behavioural
/// smoke check that the low-overhead path still applies correctly.
#[test]
fn small_batch_lane_path_applies_correctly() {
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

fn rv(pba: Pba, tag: u8) -> L2pValue {
    let mut raw = [0u8; crate::paged::format::LEAF_VALUE_SIZE];
    raw[..8].copy_from_slice(&pba.to_be_bytes());
    raw[8] = tag;
    L2pValue(raw)
}

#[test]
fn bucketed_apply_large_remap_batch_updates_refcounts_and_freed_outcome() {
    // L2pRemap no longer touches global rc; freed_pba is
    // always None. The L2P state still progresses through the bucketed
    // apply correctly — that is the load-bearing invariant for batch
    // dispatch.
    let (_d, db) = mk_db_with_shards(8);
    for i in 0..16u64 {
        let mut tx = db.begin();
        tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, i * 37, rv(100, i as u8), None);
        tx.commit().unwrap();
    }
    assert_eq!(db.get_refcount(100).unwrap(), 0);

    let mut tx = db.begin();
    for i in 0..16u64 {
        tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, i * 37, rv(1_000 + i, i as u8), None);
    }
    let (_, outcomes) = tx.commit_with_outcomes().unwrap();
    assert_eq!(outcomes.len(), 16);

    for outcome in outcomes {
        match outcome {
            ApplyOutcome::L2pRemap {
                applied, freed_pba, ..
            } => {
                assert!(applied);
                assert_eq!(freed_pba, None, "L2pRemap never surfaces freed_pba");
            }
            other => panic!("expected L2pRemap outcome, got {other:?}"),
        }
    }
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    for i in 0..16u64 {
        assert_eq!(db.get_refcount(1_000 + i).unwrap(), 0);
        // L2P state must reflect the overwrite.
        assert_eq!(
            db.get(BOOTSTRAP_VOLUME_ORD, i * 37).unwrap(),
            Some(rv(1_000 + i, i as u8))
        );
    }
}

#[test]
fn bucketed_apply_remap_preserves_same_lba_order() {
    // rc unchanged; the test verifies last-write-wins L2P
    // ordering inside the bucketed apply.
    let (_d, db) = mk_db_with_shards(4);
    let mut tx = db.begin();
    for i in 0..8u64 {
        tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 10_000 + i, rv(10_000 + i, 0), None);
    }
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 7, rv(10, 1), None);
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 7, rv(20, 1), None);
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 7, rv(30, 1), None);
    tx.commit().unwrap();

    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 7).unwrap(), Some(rv(30, 1)));
    assert_eq!(db.get_refcount(10).unwrap(), 0);
    assert_eq!(db.get_refcount(20).unwrap(), 0);
    assert_eq!(db.get_refcount(30).unwrap(), 0);
}

#[test]
fn bucketed_apply_remap_preserves_same_lba_order_when_leaf_grouped() {
    // rc unchanged across bucket apply.
    let (_d, db) = mk_db_with_shards(1);
    let mut tx = db.begin();
    for i in 0..8u64 {
        tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 20_000 + i, rv(20_000 + i, 0), None);
    }
    // These target the same leaf but are deliberately not adjacent in
    // the WAL order. The bucket fast path may group them by leaf; it
    // still has to preserve intra-LBA last-write-wins semantics.
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 512, rv(10, 1), None);
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 513, rv(11, 1), None);
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 512, rv(20, 1), None);
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 514, rv(12, 1), None);
    tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, 512, rv(30, 1), None);
    tx.commit().unwrap();

    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 512).unwrap(), Some(rv(30, 1)));
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 513).unwrap(), Some(rv(11, 1)));
    assert_eq!(db.get(BOOTSTRAP_VOLUME_ORD, 514).unwrap(), Some(rv(12, 1)));
    assert_eq!(db.get_refcount(10).unwrap(), 0);
    assert_eq!(db.get_refcount(20).unwrap(), 0);
    assert_eq!(db.get_refcount(30).unwrap(), 0);
}

#[test]
fn bucketed_apply_remap_batch_handles_shared_new_pba() {
    // rc unchanged; the test verifies all 32 LBAs land on the
    // shared PBA after the bucketed apply.
    let (_d, db) = mk_db_with_shards(8);
    let mut tx = db.begin();
    for i in 0..32u64 {
        tx.l2p_remap(BOOTSTRAP_VOLUME_ORD, i * 101, rv(777, i as u8), None);
    }
    tx.commit().unwrap();

    assert_eq!(db.get_refcount(777).unwrap(), 0);
    for i in 0..32u64 {
        assert_eq!(
            db.get(BOOTSTRAP_VOLUME_ORD, i * 101).unwrap(),
            Some(rv(777, i as u8))
        );
    }
}

/// `commit_ops` only ever receives data-plane ops in Buffer-mode
/// (lifecycle journal cutover). `batch_contains_lifecycle_op` is kept as a defensive
/// `false`-returning shim so future variants don't silently bypass the
/// fallback; this test pins that behaviour.
#[test]
fn lifecycle_predicate_returns_false_for_dataplane_ops() {
    assert!(!batch_contains_lifecycle_op(&[WalOp::L2pPut {
        vol_ord: 0,
        lba: 0,
        value: v(0),
    }]));
    assert!(!batch_contains_lifecycle_op(&[WalOp::L2pRemap {
        vol_ord: 0,
        lba: 0,
        new_value: v(0),
        guard: None,
    }]));
}

// ---------------- L2pRemap apply (SPEC §3.1) ---------------------
