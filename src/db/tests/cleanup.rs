use super::*;

#[test]
fn cleanup_dedup_for_dead_pbas_empty_input_is_noop() {
    let (_d, db) = mk_db();
    let lsn_before = db.last_applied_lsn();
    let lsn = db.cleanup_dedup_for_dead_pbas(&[]).unwrap();
    assert_eq!(lsn, lsn_before);
}

#[test]
fn cleanup_dedup_for_dead_pbas_removes_reverse_and_forward() {
    let (_d, db) = mk_db();
    // Register hash→pba in both tables, as onyx does on dedup miss
    // via a single Transaction.
    let h = hash_bytes(1, 2);
    let val = dedup_val_with_pba(500, 0xAB);
    {
        let mut tx = db.begin();
        tx.put_dedup(h, val).register_dedup_reverse(500, h);
        tx.commit().unwrap();
    }
    assert_eq!(db.get_dedup(&h).unwrap(), Some(val));
    assert_eq!(db.scan_dedup_reverse_for_pba(500).unwrap(), vec![h]);

    db.cleanup_dedup_for_dead_pbas(&[500]).unwrap();
    assert_eq!(db.get_dedup(&h).unwrap(), None);
    assert!(db.scan_dedup_reverse_for_pba(500).unwrap().is_empty());
}

#[test]
fn cleanup_dedup_for_dead_pbas_race_preserves_new_registration() {
    // SPEC §4.5 race protection: hash was re-registered to a
    // different pba between the dedup miss and cleanup. Cleanup
    // must not delete the forward entry that now points at pba=600.
    let (_d, db) = mk_db();
    let h = hash_bytes(7, 8);
    // Initial registration: hash→pba=500.
    {
        let mut tx = db.begin();
        tx.put_dedup(h, dedup_val_with_pba(500, 0))
            .register_dedup_reverse(500, h);
        tx.commit().unwrap();
    }
    // Concurrent writer re-registered to pba=600 (forward only
    // — reverse index still carries the stale (500, h) entry).
    {
        let mut tx = db.begin();
        tx.put_dedup(h, dedup_val_with_pba(600, 0))
            .register_dedup_reverse(600, h);
        tx.commit().unwrap();
    }
    assert_eq!(
        db.get_dedup(&h).unwrap().unwrap().head_pba(),
        600,
        "forward index re-registered to pba=600",
    );

    // Cleanup for pba=500. Must leave the forward entry alone
    // (it points at 600) and remove the reverse entry for 500.
    db.cleanup_dedup_for_dead_pbas(&[500]).unwrap();
    assert_eq!(
        db.get_dedup(&h).unwrap().unwrap().head_pba(),
        600,
        "forward entry for 600 must survive cleanup of 500",
    );
    assert!(
        db.scan_dedup_reverse_for_pba(500).unwrap().is_empty(),
        "reverse entry (500, h) removed",
    );
    // Reverse entry for the new pba=600 is still there.
    assert_eq!(db.scan_dedup_reverse_for_pba(600).unwrap(), vec![h]);
}

#[test]
fn cleanup_dedup_for_dead_pbas_is_idempotent() {
    // Replay safety: running cleanup twice is a no-op the second
    // time. Matches the tombstone-based design (SPEC §2.2).
    let (_d, db) = mk_db();
    let h = hash_bytes(9, 9);
    {
        let mut tx = db.begin();
        tx.put_dedup(h, dedup_val_with_pba(555, 0))
            .register_dedup_reverse(555, h);
        tx.commit().unwrap();
    }
    db.cleanup_dedup_for_dead_pbas(&[555]).unwrap();
    // Second run has nothing to do → WAL unchanged.
    let lsn_before = db.last_applied_lsn();
    db.cleanup_dedup_for_dead_pbas(&[555]).unwrap();
    assert_eq!(
        db.last_applied_lsn(),
        lsn_before,
        "replay is idempotent (no WAL record emitted)",
    );
    assert_eq!(db.get_dedup(&h).unwrap(), None);
}

#[test]
fn cleanup_dedup_for_dead_pbas_batches_multiple_pbas() {
    // One WAL record per invocation — the batching contract from
    // SPEC §2.2. Even when many (pba, hash) pairs are involved.
    let (_d, db) = mk_db();
    let mut pbas = Vec::new();
    for i in 0..5u64 {
        let pba = 1000 + i;
        let h = hash_bytes(0, i);
        let mut tx = db.begin();
        tx.put_dedup(h, dedup_val_with_pba(pba, i as u8))
            .register_dedup_reverse(pba, h);
        tx.commit().unwrap();
        pbas.push(pba);
    }
    let lsn_before = db.last_applied_lsn();
    db.cleanup_dedup_for_dead_pbas(&pbas).unwrap();
    assert_eq!(
        db.last_applied_lsn(),
        lsn_before + 1,
        "all tombstones in one atomic WAL record",
    );
    for (i, pba) in pbas.iter().enumerate() {
        let h = hash_bytes(0, i as u64);
        assert_eq!(db.get_dedup(&h).unwrap(), None);
        assert!(db.scan_dedup_reverse_for_pba(*pba).unwrap().is_empty());
    }
}

/// Build a DedupValue whose head-8B PBA is `pba` (Onyx contract —
/// see `DedupValue::head_pba`).
fn dedup_val_with_pba(pba: Pba, tag: u8) -> DedupValue {
    let mut v = [0u8; 28];
    v[..8].copy_from_slice(&pba.to_be_bytes());
    v[8] = tag;
    DedupValue(v)
}
