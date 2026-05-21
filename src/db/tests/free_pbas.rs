//! [[no-refcount-hot-path-design]] Phase 4 Step 3: WalOp::FreePbas
//! apply path with exclusive/shared split.
//!
//! Contract (post Step 3): `apply_free_pbas` classifies each PBA by
//! current refcount.
//!
//! - `rc > 0` ⇒ **shared** (referenced by dedup_index or by a
//!   pending hot-path incref/decref pair). Decref by 1; surface if it
//!   reaches 0.
//! - `rc == 0` ⇒ **exclusive** (never put_dedup'd; hot-path either
//!   already decref'd it to 0 in Phase 4, or never touched rc in
//!   Phase 5). Surface directly; do not touch rc.
//!
//! Phase 3's old "skip on rc=0" defensive branch is gone — that
//! branch collapsed Phase 5's exclusive surface (the primary retire
//! signal once hot-path RC goes away) with Phase 4's "already
//! retired by hot path" no-op. Step 3 separates them so onyx-side
//! retire is the union of L2pRemap surfaces (Phase 4) **plus**
//! exclusive FreePbas surfaces (Phase 5).

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
fn free_pbas_unknown_pba_surfaces_as_exclusive() {
    // PBA with no refcount entry (rc=0). Phase 4 Step 3 classifies it
    // as exclusive and surfaces it without touching rc. (Phase 3 used
    // to skip this case entirely; Step 3 changes the contract.)
    let (_d, db) = mk_db();
    let mut tx = db.begin();
    tx.free_pbas(0, vec![999u64].into_boxed_slice());
    let (_lsn, outcomes) = tx.commit_with_outcomes().unwrap();
    assert_eq!(db.get_refcount(999).unwrap(), 0);
    assert_free_outcome(&outcomes[0], &[999]);
}

#[test]
fn free_pbas_shared_decrefs_global_rc() {
    // Shared PBA (rc>0 from clone promotion): FreePbas takes the
    // shared branch, decref by 1, rc→0 surfaces it. This is the
    // Phase-5 dedup-retire path exercised through the apply API.
    let (_d, db) = mk_db();
    db.incref_pba(42, 1).unwrap();
    assert_eq!(db.get_refcount(42).unwrap(), 1);

    let mut tx = db.begin();
    tx.free_pbas(0, vec![42u64].into_boxed_slice());
    let (_lsn, outcomes) = tx.commit_with_outcomes().unwrap();
    assert_eq!(db.get_refcount(42).unwrap(), 0);
    assert_free_outcome(&outcomes[0], &[42]);
}

#[test]
fn free_pbas_exclusive_bypasses_rc() {
    // Exclusive PBA (rc=0 from the start, no dedup entry): Step 3
    // surfaces it without touching rc. Important for Phase 5 where
    // exclusive PBAs never get their rc bumped by the hot path —
    // GC must surface them via FreePbas without underflowing.
    let (_d, db) = mk_db();
    assert_eq!(db.get_refcount(7777).unwrap(), 0);

    let mut tx = db.begin();
    tx.free_pbas(0, vec![7777u64].into_boxed_slice());
    let (_lsn, outcomes) = tx.commit_with_outcomes().unwrap();

    // rc untouched (stays 0; no underflow surfaced).
    assert_eq!(db.get_refcount(7777).unwrap(), 0);
    assert_free_outcome(&outcomes[0], &[7777]);
}

#[test]
fn free_pbas_leaves_positive_rc_alone() {
    // Shared PBAs (lineage rc > 1) lose one ref per FreePbas. A
    // GC-emitted FreePbas for one of these decrements by 1 and
    // surfaces nothing (rc stays > 0).
    let (_d, db) = mk_db();
    db.incref_pba(7, 3).unwrap(); // three "owners"
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
    // Apply FreePbas twice for the same pba. First takes the shared
    // branch (rc=1→0) and surfaces; second finds rc=0 and surfaces
    // again via the exclusive branch. Onyx-side retire dedups via
    // set semantics so duplicate surfaces are harmless.
    let (_d, db) = mk_db();
    db.incref_pba(11, 1).unwrap();

    let mut tx1 = db.begin();
    tx1.free_pbas(0, vec![11u64].into_boxed_slice());
    let (_lsn1, outcomes1) = tx1.commit_with_outcomes().unwrap();
    assert_free_outcome(&outcomes1[0], &[11]);
    assert_eq!(db.get_refcount(11).unwrap(), 0);

    let mut tx2 = db.begin();
    tx2.free_pbas(0, vec![11u64].into_boxed_slice());
    let (_lsn2, outcomes2) = tx2.commit_with_outcomes().unwrap();
    // No panic, no underflow; rc stays 0. Second pass classifies the
    // PBA as exclusive and re-surfaces.
    assert_eq!(db.get_refcount(11).unwrap(), 0);
    assert_free_outcome(&outcomes2[0], &[11]);
}

#[test]
fn free_pbas_batched_mixed_outcomes() {
    let (_d, db) = mk_db();
    // pba 100: rc=1 (shared, decref to 0, surfaces)
    // pba 200: rc=2 (shared, decref to 1, NOT surfaced)
    // pba 300: rc=0 (exclusive, surfaces directly without touching rc)
    db.incref_pba(100, 1).unwrap();
    db.incref_pba(200, 2).unwrap();

    let mut tx = db.begin();
    tx.free_pbas(0, vec![100u64, 200, 300].into_boxed_slice());
    let (_lsn, outcomes) = tx.commit_with_outcomes().unwrap();
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    assert_eq!(db.get_refcount(200).unwrap(), 1);
    assert_eq!(db.get_refcount(300).unwrap(), 0);
    assert_free_outcome(&outcomes[0], &[100, 300]);
}
