//! Phase 3 (no-refcount-hot-path) plumbing: WalOp::FreePbas + apply
//! idempotency.
//!
//! The hot path is unchanged in Phase 3, so a real Lineage GC worker
//! that emits these ops live alongside `apply_l2p_remap` would
//! generate FreePbas records whose target PBAs are usually already at
//! refcount 0 (hot path freed them inline). These tests verify the
//! apply path handles that case correctly (idempotent skip) and that
//! a PBA whose refcount is brought to 0 by FreePbas itself is
//! surfaced in `ApplyOutcome::FreePbas.freed_pbas` for onyx-side
//! retire.

use super::mk_db;
use crate::ApplyOutcome;
use crate::types::Pba;

fn assert_free_outcome(outcome: &ApplyOutcome, expected_freed: &[Pba]) {
    match outcome {
        ApplyOutcome::FreePbas { freed_pbas } => {
            let mut got: Vec<Pba> = freed_pbas.iter().copied().collect();
            got.sort();
            let mut want = expected_freed.to_vec();
            want.sort();
            assert_eq!(got, want, "ApplyOutcome::FreePbas mismatch");
        }
        other => panic!("expected ApplyOutcome::FreePbas, got {other:?}"),
    }
}

#[test]
fn free_pbas_empty_is_no_op() {
    let (_d, db) = mk_db();
    let mut tx = db.begin();
    tx.free_pbas(0, Box::<[u64]>::default());
    let (_lsn, outcomes) = tx.commit_with_outcomes().unwrap();
    assert_eq!(outcomes.len(), 1);
    assert_free_outcome(&outcomes[0], &[]);
}

#[test]
fn free_pbas_unknown_pba_skipped() {
    // PBA with no refcount entry (rc=0): apply_free_pbas's `cur == 0`
    // guard skips the decref entirely. No underflow error is raised
    // and the PBA is not surfaced in `freed_pbas` (the hot path or
    // a prior GC cycle has already retired it, or it was never
    // allocated).
    let (_d, db) = mk_db();
    let mut tx = db.begin();
    tx.free_pbas(0, vec![999u64].into_boxed_slice());
    let (_lsn, outcomes) = tx.commit_with_outcomes().unwrap();
    assert_free_outcome(&outcomes[0], &[]);
}

#[test]
fn free_pbas_brings_rc_to_zero_and_surfaces_pba() {
    let (_d, db) = mk_db();
    // Seed rc=1 for pba 42 via the existing Incref tx API.
    let mut tx = db.begin();
    tx.incref_pba(42, 1);
    tx.commit().unwrap();
    assert_eq!(db.get_refcount(42).unwrap(), 1);

    // FreePbas drives rc=1 → rc=0; the pba surfaces.
    let mut tx = db.begin();
    tx.free_pbas(0, vec![42u64].into_boxed_slice());
    let (_lsn, outcomes) = tx.commit_with_outcomes().unwrap();
    assert_eq!(db.get_refcount(42).unwrap(), 0);
    assert_free_outcome(&outcomes[0], &[42]);
}

#[test]
fn free_pbas_leaves_positive_rc_alone() {
    // Phase 3's hot path still maintains refcount, so packed slots
    // see rc > 1. A GC-emitted FreePbas for one of these decrements
    // by 1 and surfaces nothing (rc stays > 0).
    let (_d, db) = mk_db();
    let mut tx = db.begin();
    tx.incref_pba(7, 3); // three "owners"
    tx.commit().unwrap();
    assert_eq!(db.get_refcount(7).unwrap(), 3);

    let mut tx = db.begin();
    tx.free_pbas(0, vec![7u64].into_boxed_slice());
    let (_lsn, outcomes) = tx.commit_with_outcomes().unwrap();
    // Decremented to 2; no PBA surfaced for retire.
    assert_eq!(db.get_refcount(7).unwrap(), 2);
    assert_free_outcome(&outcomes[0], &[]);
}

#[test]
fn free_pbas_is_idempotent_on_already_zero() {
    // Apply FreePbas twice for the same pba. First brings rc to 0
    // and surfaces it; second finds rc already 0, surfaces it again
    // (onyx side dedups via retire set semantics).
    let (_d, db) = mk_db();
    let mut tx = db.begin();
    tx.incref_pba(11, 1);
    tx.commit().unwrap();

    let mut tx1 = db.begin();
    tx1.free_pbas(0, vec![11u64].into_boxed_slice());
    let (_lsn1, outcomes1) = tx1.commit_with_outcomes().unwrap();
    assert_free_outcome(&outcomes1[0], &[11]);
    assert_eq!(db.get_refcount(11).unwrap(), 0);

    let mut tx2 = db.begin();
    tx2.free_pbas(0, vec![11u64].into_boxed_slice());
    let (_lsn2, outcomes2) = tx2.commit_with_outcomes().unwrap();
    // Idempotent: no panic, no underflow, still rc=0. Second pass
    // sees `cur == 0`, skips entirely, surfaces nothing.
    assert_eq!(db.get_refcount(11).unwrap(), 0);
    assert_free_outcome(&outcomes2[0], &[]);
}

#[test]
fn free_pbas_batched_mixed_outcomes() {
    let (_d, db) = mk_db();
    // pba 100: rc=1 (will free)
    // pba 200: rc=2 (will decrement to 1, not surfaced)
    // pba 300: never inserted (ghost-free, surfaced)
    let mut tx = db.begin();
    tx.incref_pba(100, 1);
    tx.incref_pba(200, 2);
    tx.commit().unwrap();

    let mut tx = db.begin();
    tx.free_pbas(0, vec![100u64, 200, 300].into_boxed_slice());
    let (_lsn, outcomes) = tx.commit_with_outcomes().unwrap();
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    assert_eq!(db.get_refcount(200).unwrap(), 1);
    assert_eq!(db.get_refcount(300).unwrap(), 0);
    // pba=100 surfaces (rc 1→0). pba=200 doesn't (rc 2→1). pba=300
    // doesn't (skipped, was never refcounted).
    assert_free_outcome(&outcomes[0], &[100]);
}
