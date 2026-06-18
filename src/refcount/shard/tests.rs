use super::*;
use tempfile::TempDir;

fn make_shard() -> (TempDir, RcShard) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages");
    let page_store = Arc::new(PageStore::create(&path).unwrap());
    let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
    let s = RcShard::create(page_store, page_cache).unwrap();
    (dir, s)
}

// Most tests don't care which TXG a stage lands in — use slot 0 (txg=0).
const T0: crate::types::Txg = 0;

#[test]
fn stage_then_get_sees_pending() {
    let (_d, s) = make_shard();
    assert_eq!(s.stage(T0, 10, 1, 100).unwrap(), (0, 1));
    assert_eq!(s.get(10).unwrap(), 1);
}

#[test]
fn stage_accumulates_across_ops() {
    let (_d, s) = make_shard();
    s.stage(T0, 10, 1, 100).unwrap();
    s.stage(T0, 10, 2, 101).unwrap();
    s.stage(T0, 10, -1, 102).unwrap();
    assert_eq!(s.get(10).unwrap(), 2);
}

#[test]
fn flush_moves_pending_to_array() {
    let (_d, s) = make_shard();
    s.stage(T0, 10, 5, 100).unwrap();
    s.flush().unwrap();
    assert_eq!(s.get(10).unwrap(), 5);
    assert_eq!(
        s.get_entry(10).unwrap(),
        RcEntry {
            rc: 5,
            birth_lsn: 100
        }
    );
}

fn clamped() -> u64 {
    crate::refcount::underflow_clamped_total()
}

#[test]
fn stage_decref_past_zero_is_skipped_not_errored() {
    let (_d, s) = make_shard();
    let c0 = clamped();
    s.stage(T0, 10, 1, 1).unwrap(); // rc 0 -> 1
    // Decref by 2 when only 1 ref exists would underflow. Instead of
    // erroring the redundant decref is skipped and the count is left
    // unchanged.
    assert_eq!(s.stage(T0, 10, -2, 2).unwrap(), (1, 1));
    assert_eq!(s.get(10).unwrap(), 1);
    assert_eq!(clamped() - c0, 1);
    // The delta map is not corrupted: a subsequent legitimate decref
    // still works.
    assert_eq!(s.stage(T0, 10, -1, 3).unwrap(), (1, 0));
    assert_eq!(s.get(10).unwrap(), 0);
}

#[test]
fn stage_double_decref_of_freed_pba_is_benign() {
    // The production race: two ops remove the same last reference. The
    // first takes the count to 0; the second finds it already 0 and is
    // skipped — not an error, never negative.
    let (_d, s) = make_shard();
    let c0 = clamped();
    s.stage(T0, 20, 1, 1).unwrap(); // rc 0 -> 1
    assert_eq!(s.stage(T0, 20, -1, 2).unwrap(), (1, 0)); // legit last decref
    assert_eq!(s.stage(T0, 20, -1, 3).unwrap(), (0, 0)); // redundant -> skipped
    assert_eq!(s.get(20).unwrap(), 0);
    assert_eq!(clamped() - c0, 1);
}

fn read_floored() -> u64 {
    crate::refcount::read_underflow_floored_total()
}

#[test]
fn get_and_stage_floor_read_underflow_instead_of_erroring() {
    // Reproduce the torn `(pending, array)` state a concurrent rc fold
    // can leave: the array base for P has already folded a pending decref
    // (rc=0), but a stale `-1` pending still sits in a slot. The logical
    // rc floor is 0; `merge_read_or_floor` floors instead of erroring.
    let (_d, s) = make_shard();
    let p = 4242;
    let floored0 = read_floored();
    // array(p) defaults to rc=0; inject the stale -1 pending into slot 0.
    s.delta_slots[0].lock().merge(p, -1, 500);

    // `get` must floor (base 0 + pending -1 -> 0), NOT return Err.
    assert_eq!(s.get(p).unwrap(), 0);
    assert!(read_floored() > floored0, "get should record a read floor");

    // `stage`'s merged_prev computation must also floor on the same torn
    // state: a further decref floors the read to 0, then the final-apply
    // clamp absorbs the redundant decref as a benign no-op — never Err.
    let floored1 = read_floored();
    assert_eq!(s.stage(T0, p, -1, 501).unwrap(), (0, 0));
    assert!(read_floored() > floored1, "stage merged_prev should floor too");
    assert_eq!(s.get(p).unwrap(), 0);
}

#[test]
fn stage_overflow_still_errors() {
    let (_d, s) = make_shard();
    let c0 = clamped();
    s.stage(T0, 30, i64::from(u32::MAX), 1).unwrap(); // rc -> u32::MAX
    assert!(s.stage(T0, 30, 1, 2).is_err());
    assert_eq!(clamped() - c0, 0);
}

#[test]
fn zero_to_one_to_zero_to_one_birth_lsn() {
    let (_d, s) = make_shard();
    s.stage(T0, 10, 1, 100).unwrap();
    s.flush().unwrap();
    s.stage(T0, 10, -1, 101).unwrap();
    s.flush().unwrap();
    assert_eq!(s.get_entry(10).unwrap(), RcEntry::ZERO);
    s.stage(T0, 10, 1, 200).unwrap();
    s.flush().unwrap();
    assert_eq!(
        s.get_entry(10).unwrap(),
        RcEntry {
            rc: 1,
            birth_lsn: 200
        }
    );
}

#[test]
fn iter_live_flushed_skips_zero() {
    let (_d, s) = make_shard();
    s.stage(T0, 1, 1, 1).unwrap();
    s.stage(T0, 2, 1, 1).unwrap();
    s.stage(T0, 2, -1, 2).unwrap();
    s.stage(T0, 3, 3, 3).unwrap();
    let live = s.iter_live_flushed().unwrap();
    assert_eq!(live.len(), 2);
    assert_eq!(live[0].0, 1);
    assert_eq!(live[1].0, 3);
}

#[test]
fn round_trip_via_open() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages");
    let meta_page_id;
    {
        let page_store = Arc::new(PageStore::create(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let s = RcShard::create(page_store, page_cache).unwrap();
        meta_page_id = s.meta_page_id();
        s.stage(T0, 10, 5, 100).unwrap();
        s.stage(T0, 20, 2, 200).unwrap();
        s.flush().unwrap();
    }
    let page_store = Arc::new(PageStore::open(&path).unwrap());
    let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
    let s = RcShard::open(page_store, page_cache, meta_page_id).unwrap();
    assert_eq!(s.get(10).unwrap(), 5);
    assert_eq!(s.get(20).unwrap(), 2);
    assert_eq!(s.get_entry(10).unwrap().birth_lsn, 100);
}

#[test]
fn begin_checkpoint_all_slots_stages_without_overwriting_disk() {
    let (_d, s) = make_shard();
    s.stage(T0, 10, 5, 100).unwrap();
    s.stage(T0, 20, 3, 100).unwrap();

    let ckpt = s.begin_checkpoint_all_slots(false).unwrap();
    assert_eq!(s.get(10).unwrap(), 5, "stage value visible via cache");
    assert_eq!(s.get(20).unwrap(), 3);
    assert!(s.allocated_data_pages() >= 1);
    assert!(!ckpt.is_empty());
    s.abort_checkpoint(ckpt, 0);
    assert_eq!(s.get(10).unwrap(), 5);
    assert_eq!(s.get(20).unwrap(), 3);
}

#[test]
fn checkpoint_pipeline_round_trips_through_disk() {
    let (_d, s) = make_shard();
    s.stage(T0, 7, 2, 50).unwrap();
    s.stage(T0, 800, 4, 60).unwrap();

    let ckpt = s.begin_checkpoint_all_slots(false).unwrap();
    assert!(!ckpt.is_empty());
    assert_eq!(ckpt.fresh_page_ids().len(), 2);

    s.array.write_staged_pages(&ckpt.staged).unwrap();
    let new_chain = s.write_meta_chain(&ckpt, 0).unwrap();
    s.install_meta_chain(new_chain);
    assert_eq!(s.get(7).unwrap(), 2);
    assert_eq!(s.get(800).unwrap(), 4);
}

#[test]
fn abort_then_retry_does_not_double_apply_via_replay_skip() {
    let (_d, s) = make_shard();
    s.stage(T0, 10, 5, 100).unwrap();

    let ckpt = s.begin_checkpoint_all_slots(false).unwrap();
    s.array.write_staged_pages(&ckpt.staged).unwrap();
    let _ = s.write_meta_chain(&ckpt, 0).unwrap();
    s.abort_checkpoint(ckpt, 0);
    assert_eq!(s.get(10).unwrap(), 5, "value still observable post-abort");
    s.flush().unwrap();
    assert_eq!(s.get(10).unwrap(), 5, "no double-apply on retry");
    assert_eq!(s.get_entry(10).unwrap().birth_lsn, 100);
}

#[test]
fn empty_checkpoint_is_no_op() {
    let (_d, s) = make_shard();
    let ckpt = s.begin_checkpoint_all_slots(false).unwrap();
    assert!(ckpt.is_empty());
    let new_chain = s.write_meta_chain(&ckpt, 0).unwrap();
    s.install_meta_chain(new_chain);
    let ckpt2 = s.begin_checkpoint_all_slots(false).unwrap();
    s.abort_checkpoint(ckpt2, 0);
}

#[test]
fn many_ops_one_shard_correctness() {
    use std::collections::HashMap;
    let (_d, s) = make_shard();
    let mut model: HashMap<Pba, i64> = HashMap::new();
    let ops: Vec<(Pba, i64, Lsn)> = (1u64..1000)
        .map(|i| {
            let pba = (i * 7) % (super::super::ENTRIES_PER_PAGE as u64 * 5);
            let delta = if i % 3 == 0 { -1i64 } else { 1i64 };
            (pba, delta, i)
        })
        .collect();
    for &(pba, delta, lsn) in &ops {
        let model_prev = *model.get(&pba).unwrap_or(&0);
        let model_new = model_prev + delta;
        if model_new < 0 {
            continue;
        }
        let (_prev, new) = s.stage(T0, pba, delta, lsn).unwrap();
        assert_eq!(new, model_new as u32);
        model.insert(pba, model_new);
    }
    s.flush().unwrap();
    for (&pba, &expected) in &model {
        assert_eq!(s.get(pba).unwrap(), expected as u32, "pba {pba}");
    }
}

// ── TXG-slot ring tests ──────────────────────────────────────────────

#[test]
fn stage_routes_to_txg_slot_and_reads_sum_across_slots() {
    let (_d, s) = make_shard();
    // Two TXGs touch the same PBA, landing in different ring slots.
    s.stage(0, 10, 1, 100).unwrap(); // slot 0
    s.stage(1, 10, 1, 200).unwrap(); // slot 1
    // The deltas live in distinct slots…
    assert_eq!(s.delta_slots[0].lock().get(10).unwrap().delta, 1);
    assert_eq!(s.delta_slots[1].lock().get(10).unwrap().delta, 1);
    assert!(s.delta_slots[2].lock().get(10).is_none());
    // …and the cumulative read sums them.
    assert_eq!(s.get(10).unwrap(), 2);
}

#[test]
fn begin_checkpoint_folds_only_the_syncing_slot() {
    let (_d, s) = make_shard();
    s.stage(0, 10, 3, 100).unwrap(); // slot 0
    s.stage(1, 20, 5, 200).unwrap(); // slot 1

    // Fold ONLY slot 0 (txg=0).
    let ckpt0 = s.begin_checkpoint(0).unwrap();
    assert!(!ckpt0.is_empty());
    // Slot 0 is now empty; slot 1 still pending.
    assert!(s.delta_slots[0].lock().get(10).is_none());
    assert_eq!(s.delta_slots[1].lock().get(20).unwrap().delta, 5);
    // Both PBAs still read correctly (10 from array, 20 from slot 1).
    assert_eq!(s.get(10).unwrap(), 3);
    assert_eq!(s.get(20).unwrap(), 5);

    // Fold slot 1 (txg=1).
    let ckpt1 = s.begin_checkpoint(1).unwrap();
    assert!(!ckpt1.is_empty());
    assert_eq!(s.get(20).unwrap(), 5);
}

#[test]
fn begin_checkpoint_empty_slot_is_no_op() {
    let (_d, s) = make_shard();
    // No deltas for txg=2's slot → empty checkpoint.
    let ckpt = s.begin_checkpoint(2).unwrap();
    assert!(ckpt.is_empty());
}

#[test]
fn cross_txg_incref_then_decref_same_pba_folds_to_correct_count() {
    let (_d, s) = make_shard();
    // TXG 0 increfs P to 1; TXG 1 decrefs it back to 0.
    s.stage(0, 42, 1, 100).unwrap();
    s.stage(1, 42, -1, 200).unwrap();
    // Cumulative read across both slots = 0.
    assert_eq!(s.get(42).unwrap(), 0);

    // Fold txg 0 → array has rc=1 for P, slot 1 still holds the -1.
    let _c0 = s.begin_checkpoint(0).unwrap();
    assert_eq!(s.get(42).unwrap(), 0, "array(1) + slot1(-1) = 0");

    // Fold txg 1 → the decref lands, rc back to 0.
    let _c1 = s.begin_checkpoint(1).unwrap();
    assert_eq!(s.get(42).unwrap(), 0);
    // Persist + reopen-style check via flush: still 0.
    s.flush().unwrap();
    assert_eq!(s.get(42).unwrap(), 0);
}

#[test]
fn freed_pba_surfaces_on_cumulative_zero_not_per_slot() {
    // P has a durable rc=1 (folded). A decref in a later TXG slot takes
    // the cumulative to 0 → stage returns (1, 0) so the caller surfaces
    // freed_pba. The decref before fold reads the array base (1).
    let (_d, s) = make_shard();
    s.stage(0, 77, 1, 100).unwrap();
    s.begin_checkpoint(0).unwrap(); // P durable at rc=1
    assert_eq!(s.get(77).unwrap(), 1);
    // Decref in TXG 1's slot.
    assert_eq!(s.stage(1, 77, -1, 200).unwrap(), (1, 0));
    assert_eq!(s.get(77).unwrap(), 0);
}

#[test]
fn pending_delta_count_sums_all_slots() {
    let (_d, s) = make_shard();
    s.stage(0, 1, 1, 1).unwrap();
    s.stage(1, 2, 1, 1).unwrap();
    s.stage(2, 3, 1, 1).unwrap();
    assert_eq!(s.pending_delta_count(), 3);
}

/// REPRO: the cold-path force-fold (`RcShard::flush()` →
/// `begin_checkpoint_all_slots(force=true)`) RE-APPLIES a decref the array
/// already reflects, driving a still-live rc below its true floor.
///
/// This is the page-rc premature-free the nvme-box buffer-mode snapshot
/// soak hit. In buffer mode the COW page-rc deltas are staged at DRAIN
/// time in radix-key order, so their lsns are NON-MONOTONE relative to the
/// per-array-DATA-PAGE `generation` (= max folded lsn). The fold's only
/// idempotency is `stage_one_page`'s `!force && page_generation >=
/// pending.last_lsn` replay-skip — but `flush()` calls
/// `begin_checkpoint_all_slots(force=true)`, which BYPASSES it. So a decref
/// that the array has ALREADY folded, if it reappears in a slot with a
/// `last_lsn <= page_generation` (the non-monotone hazard, or a recovery /
/// abort-retry residue), is applied a SECOND time by the force fold.
///
/// Setup mirrors the array's per-page generation mechanism directly:
///   1. incref P by 2 at lsn 100, fold → array rc=2, page generation=100.
///   2. decref P by 1 at lsn 200, fold → array rc=1, page generation=200.
///      (This is the legitimate, already-durable decref.)
///   3. re-inject that SAME `-1` decref into a fresh slot at lsn 150
///      (`last_lsn <= page_generation` of 200 — the non-monotone /
///      replay-residue shape). A NORMAL fold (`begin_checkpoint(false)`)
///      MUST replay-skip it (gen 200 >= 150) and leave rc=1.
///   4. `flush()` force-folds → on the BUGGY path it re-applies the `-1`,
///      flooring the live rc=1 to 0 — a premature free. The
///      benign-double-decref clamp (`note_decref_underflow_skip` /
///      `stage_underflow`) fires when the array base is already 0, which is
///      exactly the warning flood the soak logged before the corruption.
///
/// PASS (today's intent): rc stays at 1 after the force-fold (the fix keeps
/// the live page referenced). FAIL (buggy force path): rc drops to 0 and
/// `flush()` would surface P as freed → reuse → page-type corruption.
#[test]
fn force_fold_does_not_reapply_decref_array_already_reflects() {
    let (_d, s) = make_shard();
    let p: Pba = 4242;

    // 1. incref P by 2 @ lsn 100, fold → array rc=2, page gen=100.
    s.stage(0, p, 2, 100).unwrap();
    s.begin_checkpoint(0).unwrap();
    assert_eq!(s.get(p).unwrap(), 2);

    // 2. legitimate decref -1 @ lsn 200, fold → array rc=1, page gen=200.
    s.stage(1, p, -1, 200).unwrap();
    s.begin_checkpoint(1).unwrap();
    assert_eq!(s.get(p).unwrap(), 1, "live rc after the legitimate decref");

    // 3. The SAME -1 reappears in a fresh slot with a NON-MONOTONE lsn
    //    (150 <= page generation 200) — the buffer-drain radix-order
    //    hazard / recovery residue. Inject it straight into the slot so we
    //    drive the fold directly (a `stage` here would itself clamp on the
    //    cumulative read; the soak's delta arrives via the COW drain, which
    //    stages unconditionally into the open TXG slot).
    s.delta_slots[2].lock().merge(p, -1, 150);

    // A NORMAL (non-force) fold of that slot MUST replay-skip it (page
    // generation 200 >= 150) and leave the live rc at 1.
    let c_norm = s.begin_checkpoint(2).unwrap();
    drop(c_norm);
    assert_eq!(
        s.get(p).unwrap(),
        1,
        "non-force fold must replay-skip the stale decref (gen 200 >= lsn 150)"
    );

    // Re-inject the same stale -1 again (the force-fold path the soak takes
    // is `RcShard::flush()`; recreate the slot residue it would force-fold).
    s.delta_slots[3].lock().merge(p, -1, 150);

    let clamps_before = crate::refcount::underflow_clamped_total();
    // 4. The cold-path force-fold (`flush()` →
    //    begin_checkpoint_all_slots(true)) RE-APPLIES the stale decref,
    //    bypassing the per-page replay-skip.
    s.flush().unwrap();
    let clamps_after = crate::refcount::underflow_clamped_total();

    assert_eq!(
        s.get(p).unwrap(),
        1,
        "force-fold re-applied a decref the array already reflects: live rc \
         floored to {} (premature page free). underflow clamps fired: {}",
        s.get(p).unwrap(),
        clamps_after - clamps_before,
    );
}

/// Regression for the rc_authoritative premature-free CRC (2026-06-12 r2
/// soak, pba 661307): a fold's publish-before-clear window lets a cumulative
/// read straddle it and double-count a NET-DECREF slot, flooring a still-live
/// rc to a spurious 0. Under `rc_authoritative_reclaim` the GC reclaim Gate-1
/// treats that 0 as proof to irreversibly free the PBA (Gate-2 reverify is
/// skipped) → reuse → read CRC. `get_consistent` holds `fold_lock` so it can
/// never observe the torn state.
///
/// The writer keeps the TRUE cumulative rc oscillating in {4, 8} — NEVER 0 —
/// while folding a net `-4` every iteration (the exact tear-prone shape). The
/// reader asserts `get_consistent` never dips below the true floor of 4.
/// Flip `get_consistent` → `get` and this fails (the plain read tears to 0).
#[test]
fn get_consistent_never_reads_spurious_zero_under_concurrent_fold() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    let (_d, s) = make_shard();
    let s = Arc::new(s);
    let pba: Pba = 4242;

    // Durable base rc = 8 (folded). True cumulative rc stays in {4, 8}.
    s.stage(0, pba, 8, 1).unwrap();
    s.begin_checkpoint(0).unwrap();
    assert_eq!(s.get(pba).unwrap(), 8);

    let stop = Arc::new(AtomicBool::new(false));
    // Count how often the PLAIN (racy) read tears below the floor — proves
    // the window is actually exercised on this run (observational, not an
    // assertion, so it can't be flaky).
    let plain_torn = Arc::new(AtomicU32::new(0));

    let writer = {
        let s = s.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut lsn = 100u64;
            let mut txg = 1u64;
            while !stop.load(Ordering::Relaxed) {
                // decref 4 then fold (array 8 -> 4): net-decref publish.
                s.stage(txg, pba, -4, lsn).unwrap();
                lsn += 1;
                s.begin_checkpoint(txg).unwrap();
                txg += 1;
                // incref 4 back then fold (array 4 -> 8).
                s.stage(txg, pba, 4, lsn).unwrap();
                lsn += 1;
                s.begin_checkpoint(txg).unwrap();
                txg += 1;
            }
        })
    };

    let mut min_consistent = u32::MAX;
    for _ in 0..200_000 {
        let rc = s.get_consistent(pba).unwrap();
        min_consistent = min_consistent.min(rc);
        assert!(
            rc >= 4,
            "get_consistent observed spurious rc={rc} (true rc always in {{4,8}})"
        );
        // Observe the plain read's tearing without gating the test on it.
        if s.get(pba).unwrap() < 4 {
            plain_torn.fetch_add(1, Ordering::Relaxed);
        }
    }

    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    assert!(
        min_consistent == 4 || min_consistent == 8,
        "expected to observe both arms; min_consistent={min_consistent}"
    );
    eprintln!(
        "plain (racy) get torn below floor {} / 200000 times",
        plain_torn.load(Ordering::Relaxed)
    );
}
