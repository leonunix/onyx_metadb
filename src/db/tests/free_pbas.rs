//! FreePbas apply path
//! with exclusive/shared split.
//!
//! Contract (post FreePbas split): `apply_free_pbas` classifies each PBA by
//! current refcount.
//!
//! - `rc > 0` ⇒ **shared** (referenced by dedup_index or by a
//!   pending hot-path incref/decref pair). Decref by 1; surface if it
//!   reaches 0.
//! - `rc == 0` ⇒ **exclusive** (never put_dedup'd; hot-path either
//!   already decref'd it to 0 in , or never touched rc in
//!   ). Surface directly; do not touch rc.
//!
//! 's old "skip on rc=0" defensive branch is gone — that
//! branch collapsed 's exclusive surface (the primary retire
//! signal once hot-path RC goes away) with 's "already
//! retired by hot path" no-op. FreePbas split separates them so onyx-side
//! retire is the union of L2pRemap surfaces () **plus**
//! exclusive FreePbas surfaces ().
//!
//! lifecycle journal cutover: `FreePbas` lives entirely outside the WAL — `commit_ops`
//! never carries it. The unit tests drive `Db::commit_free_pbas`
//! directly, which is the same entry point production GC uses.

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
    let outcome = db.commit_free_pbas(0, &[]).unwrap();
    assert_free_outcome(&outcome, &[]);
}

#[test]
fn free_pbas_unknown_pba_surfaces_as_exclusive() {
    let (_d, db) = mk_db();
    let outcome = db.commit_free_pbas(0, &[999u64]).unwrap();
    assert_eq!(db.get_refcount(999).unwrap(), 0);
    assert_free_outcome(&outcome, &[999]);
}

#[test]
fn free_pbas_shared_decrefs_global_rc() {
    let (_d, db) = mk_db();
    db.incref_pba(42, 1).unwrap();
    assert_eq!(db.get_refcount(42).unwrap(), 1);

    let outcome = db.commit_free_pbas(0, &[42u64]).unwrap();
    assert_eq!(db.get_refcount(42).unwrap(), 0);
    assert_free_outcome(&outcome, &[42]);
}

#[test]
fn free_pbas_exclusive_bypasses_rc() {
    let (_d, db) = mk_db();
    assert_eq!(db.get_refcount(7777).unwrap(), 0);

    let outcome = db.commit_free_pbas(0, &[7777u64]).unwrap();

    assert_eq!(db.get_refcount(7777).unwrap(), 0);
    assert_free_outcome(&outcome, &[7777]);
}

#[test]
fn free_pbas_leaves_positive_rc_alone() {
    let (_d, db) = mk_db();
    db.incref_pba(7, 3).unwrap();
    assert_eq!(db.get_refcount(7).unwrap(), 3);

    let outcome = db.commit_free_pbas(0, &[7u64]).unwrap();
    assert_eq!(db.get_refcount(7).unwrap(), 2);
    assert_free_outcome(&outcome, &[]);
}

#[test]
fn free_pbas_is_idempotent_on_already_zero() {
    let (_d, db) = mk_db();
    db.incref_pba(11, 1).unwrap();

    let outcome1 = db.commit_free_pbas(0, &[11u64]).unwrap();
    assert_free_outcome(&outcome1, &[11]);
    assert_eq!(db.get_refcount(11).unwrap(), 0);

    let outcome2 = db.commit_free_pbas(0, &[11u64]).unwrap();
    assert_eq!(db.get_refcount(11).unwrap(), 0);
    assert_free_outcome(&outcome2, &[11]);
}

#[test]
fn free_pbas_batched_mixed_outcomes() {
    let (_d, db) = mk_db();
    db.incref_pba(100, 1).unwrap();
    db.incref_pba(200, 2).unwrap();

    let outcome = db.commit_free_pbas(0, &[100u64, 200, 300]).unwrap();
    assert_eq!(db.get_refcount(100).unwrap(), 0);
    assert_eq!(db.get_refcount(200).unwrap(), 1);
    assert_eq!(db.get_refcount(300).unwrap(), 0);
    assert_free_outcome(&outcome, &[100, 300]);
}
