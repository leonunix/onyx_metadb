//! ZFS port Phase 3a: per-clone page-livelist SHADOW (`drop_volume`
//! clone-drop cross-check). Page-rc stays authoritative; the shadow asserts
//! that the page-rc free-set (`collect_drop_pages` rc==1) equals an
//! independent C-exclusive reachability walk, aborting HARD on a premature
//! free (page-rc would free a page still reachable from a surviving root)
//! and warning soft on a completeness hole.
//!
//! The load-bearing cases are the audit's counterexamples to the *naive*
//! single-volume `birth > branched_at_lsn` predicate: a clone-of-clone where
//! a `birth > B` page is still shared with a promoted descendant (premature),
//! and a parent-diverged origin page that falls sole-owned to the clone
//! (`birth <= B` but page-rc frees it). The exclusive-reachability RHS gets
//! both right where `birth > B` would not — these tests pin that.

use super::{mk_db, mk_db_with_shards, v};
use crate::Db;
use crate::db::promotion::PromotionStep;
use crate::error::MetaDbError;
use crate::livelist::{LiveKind, LiveRecord};

/// Build synthetic ALLOC `LiveRecord`s from a `(pid, birth)` set so the
/// livelist-cross-check teeth can feed a crafted live-ALLOC set into the
/// shim. `event_lsn` only affects `live_allocs`' sort, not the resulting
/// `(pid, birth)` keys, so `birth` doubles as the event.
fn allocs(pairs: &[(crate::types::PageId, crate::types::Lsn)]) -> Vec<LiveRecord> {
    pairs
        .iter()
        .map(|&(pid, birth)| LiveRecord {
            pid,
            birth_lsn: birth,
            event_lsn: birth,
            kind: LiveKind::Alloc,
        })
        .collect()
}

/// G2/G3/G4: sibling clones share the snapshot's pages, each writes a private
/// page, then both are dropped in each order. The shadow must stay green
/// (premature never fires; the shared pages a survivor still references are
/// not freed) regardless of drop order.
#[test]
fn sibling_clones_drop_both_orders_shadow_stays_green() {
    for drop_a_first in [true, false] {
        let (_d, db) = mk_db();
        let src = db.create_volume().unwrap();
        for i in 0u64..8 {
            db.insert(src, i, v(i as u8)).unwrap();
        }
        let snap = db.take_snapshot(src).unwrap();
        let a = db.clone_volume(snap).unwrap();
        let b = db.clone_volume(snap).unwrap();
        db.insert(a, 0, v(0xAA)).unwrap();
        db.insert(b, 0, v(0xBB)).unwrap();

        let (first, second) = if drop_a_first { (a, b) } else { (b, a) };
        // Each drop runs the clone-livelist shadow; a premature divergence
        // would surface as `Err(Corruption)` from drop_volume.
        assert!(db.drop_volume(first).unwrap().is_some());
        assert!(db.drop_volume(second).unwrap().is_some());

        // Source + snapshot data survive both clone drops untouched.
        for i in 0u64..8 {
            assert_eq!(db.get(src, i).unwrap(), Some(v(i as u8)));
        }
        let view = db.snapshot_view(snap).unwrap();
        for i in 0u64..8 {
            assert_eq!(view.get(i).unwrap(), Some(v(i as u8)));
        }
    }
}

/// G6 (PREMATURE load-bearing): clone-of-clone. C diverges (clone-private
/// pages born > B1), a snapshot S2 of C is cloned into D, then S2 is dropped
/// and D is promoted — making `drop_volume(C)` *legal* while D still shares
/// C's `birth > B1` pages. The exclusive-reachability shadow must NOT fire
/// premature (those pages are reachable from D, a survivor, so page-rc keeps
/// them); a naive `birth > B` predicate WOULD free them = the premature-free
/// P0 the port exists to kill. D's data must survive C's drop.
#[test]
fn clone_of_clone_drop_middle_after_promote_shadow_stays_green() {
    let (_d, db) = mk_db();
    let p = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(p, i, v(i as u8)).unwrap();
    }
    let s1 = db.take_snapshot(p).unwrap();
    let c = db.clone_volume(s1).unwrap();
    // C diverges: every page on C's path is COW'd to a clone-private version
    // born > B1 = s1.created_lsn.
    for i in 0u64..8 {
        db.insert(c, i, v(0xC0 | i as u8)).unwrap();
    }
    let s2 = db.take_snapshot(c).unwrap();
    let d = db.clone_volume(s2).unwrap();
    // D shares C's born>B1 pages via S2's roots.
    for i in 0u64..8 {
        assert_eq!(db.get(d, i).unwrap(), Some(v(0xC0 | i as u8)));
    }

    // Legalize C's drop: drop S2 (C has no live snapshot) + promote D (clears
    // D.parent_vol_ord so C has no pending descendant) — while D still
    // physically shares C's born>B1 L2P pages (page-rc, invisible to birth).
    let _ = db.drop_snapshot(s2).unwrap().unwrap();
    match db.test_run_promotion_chunk(d).unwrap() {
        PromotionStep::Completed => {}
        other => panic!("expected D promotion Completed, got {other:?}"),
    }

    // The shadow must stay green: C's born>B1 pages are reachable from D
    // (a survivor) => not C-exclusive => page-rc keeps them => no premature.
    let report = db.drop_volume(c).unwrap();
    assert!(report.is_some(), "drop_volume(C) should succeed, not abort premature");

    // D's data survived C's drop (the shared born>B1 pages were not freed).
    for i in 0u64..8 {
        assert_eq!(db.get(d, i).unwrap(), Some(v(0xC0 | i as u8)));
    }
}

/// G8-shape: the parent diverges over a shared LBA and the source snapshot is
/// dropped, so some origin pages (`birth <= B`) fall sole-owned to the clone.
/// Dropping the clone must stay GREEN under all three detectors: those origin
/// pages are freed by page-rc (rc==1) AND C-exclusive by reachability (no
/// survivor references them), so they sit in both sets — no premature, no
/// missing (now HARD). Being born ≤ B they are not in the livelist and drop
/// out of both sides of the livelist cross-check. The clone-private write and
/// the parent both stay intact.
#[test]
fn clone_drop_after_parent_diverges_and_snapshot_dropped_no_premature() {
    let (_d, db) = mk_db();
    let p = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(p, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(p).unwrap();
    let c = db.clone_volume(snap).unwrap();
    // Parent diverges over lba0 — the old origin path becomes shared only by
    // {snap, C}.
    db.insert(p, 0, v(0xF0)).unwrap();
    // C writes a private page of its own as well.
    db.insert(c, 7, v(0xC7)).unwrap();
    // Drop the source snapshot: the old origin pages now fall to C alone.
    let _ = db.drop_snapshot(snap).unwrap().unwrap();

    // drop_volume(C) runs the shadow; premature would Err. Missing may warn.
    assert!(db.drop_volume(c).unwrap().is_some());

    // Parent survives: its divergent write and the untouched LBAs are intact.
    assert_eq!(db.get(p, 0).unwrap(), Some(v(0xF0)));
    for i in 1u64..8 {
        assert_eq!(db.get(p, i).unwrap(), Some(v(i as u8)));
    }
}

/// G10: a clone that diverged and outlived its source snapshot survives a
/// crash-style close + reopen, and the clone-livelist shadow stays green on
/// the post-reopen drop.
#[test]
fn clone_reopen_then_drop_shadow_stays_green() {
    let dir = tempfile::TempDir::new().unwrap();
    let (src, clone) = {
        let db = Db::create(dir.path()).unwrap();
        let src = db.create_volume().unwrap();
        for i in 0u64..8 {
            db.insert(src, i, v(i as u8)).unwrap();
        }
        let snap = db.take_snapshot(src).unwrap();
        let clone = db.clone_volume(snap).unwrap();
        for i in 0u64..8 {
            db.insert(clone, i, v(0xC0 | i as u8)).unwrap();
        }
        let _ = db.drop_snapshot(snap).unwrap().unwrap();
        db.flush().unwrap();
        (src, clone)
    };
    let db = Db::open(dir.path()).unwrap();
    assert!(db.drop_volume(clone).unwrap().is_some());
    for i in 0u64..8 {
        assert_eq!(db.get(src, i).unwrap(), Some(v(i as u8)));
    }
}

/// TEETH (premature): prove the premature detector actually fires. Feed
/// `check_clone_livelist_shadow` a free-set that wrongly claims a
/// survivor-reachable page (the source volume's live root) is freed — the
/// independent exclusivity walk sees it reachable from the surviving root, so
/// it is NOT C-exclusive, and the shadow must return HARD `Corruption`. A
/// self-consistent call on the same clone (real clone-private free-set + its
/// real live-ALLOC set) must pass.
#[test]
fn clone_drop_shadow_fires_on_premature_divergence() {
    let (_d, db) = mk_db_with_shards(1);
    let src = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(src, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(snap).unwrap();
    for i in 0u64..8 {
        db.insert(clone, i, v(0xC0 | i as u8)).unwrap();
    }
    // Flush so the reachability walk reads the durable trees.
    db.flush().unwrap();

    let m = db.manifest();
    let src_root = m
        .volumes
        .iter()
        .find(|e| e.ord == src)
        .unwrap()
        .l2p_shard_roots[0];
    let clone_entry = m.volumes.iter().find(|e| e.ord == clone).unwrap();
    let clone_root = clone_entry.l2p_shard_roots[0];
    let b = clone_entry.branched_at_lsn;

    // Premature: src_root is reachable from the surviving src root, so a
    // free-set claiming it (rc==1) must abort HARD (checked before the
    // livelist cross-check, so an empty live-ALLOC set is fine).
    let err = db
        .test_check_clone_livelist_shadow(
            clone,
            b,
            &[(src_root, 0, 1)],
            &[clone_root],
            &[src_root],
            &[],
        )
        .unwrap_err();
    assert!(
        matches!(err, MetaDbError::Corruption(_)),
        "expected premature Corruption, got {err:?}"
    );

    // Self-consistent: the clone's real clone-private free-set (page-rc rc==1,
    // birth > B) cross-checked against its real live-ALLOC set must pass all
    // three detectors. `la` = reachable(C) ∩ {birth > B}; with src the only
    // survivor those are exactly the C-exclusive freed pages.
    let la: Vec<(crate::types::PageId, crate::types::Lsn)> =
        db.test_clone_live_allocs(clone).unwrap().into_iter().collect();
    let freed: Vec<(crate::types::PageId, crate::types::Lsn, u32)> =
        la.iter().map(|&(pid, birth)| (pid, birth, 1)).collect();
    db.test_check_clone_livelist_shadow(
        clone,
        b,
        &freed,
        &[clone_root],
        &[src_root],
        &allocs(&la),
    )
    .unwrap();
}

/// TEETH (missing → HARD): a free-set that frees NOTHING while the clone has
/// C-exclusive reachable pages is a page-rc leak — the C-exclusive reachability
/// walk frees pages page-rc keeps live. Phase 4 Step 1 escalates this from a
/// soft warn to HARD `Corruption` (its soundness rests on `surviving_roots`
/// being complete, which it is for a still-clone drop).
#[test]
fn clone_drop_shadow_fires_on_missing_completeness_hole() {
    let (_d, db) = mk_db_with_shards(1);
    let src = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(src, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(snap).unwrap();
    for i in 0u64..8 {
        db.insert(clone, i, v(0xC0 | i as u8)).unwrap();
    }
    db.flush().unwrap();

    let m = db.manifest();
    let src_root = m
        .volumes
        .iter()
        .find(|e| e.ord == src)
        .unwrap()
        .l2p_shard_roots[0];
    let clone_entry = m.volumes.iter().find(|e| e.ord == clone).unwrap();
    let clone_root = clone_entry.l2p_shard_roots[0];
    let b = clone_entry.branched_at_lsn;

    // Empty free-set + a diverged clone with exclusive private pages → missing.
    let err = db
        .test_check_clone_livelist_shadow(clone, b, &[], &[clone_root], &[src_root], &[])
        .unwrap_err();
    assert!(
        matches!(err, MetaDbError::Corruption(ref m) if m.contains("COMPLETENESS HOLE")),
        "expected missing/completeness Corruption, got {err:?}"
    );
}

/// TEETH (livelist cross-check → HARD): with premature + missing both clean
/// (free-set == C-exclusive reachable), the persistent livelist's live-ALLOC
/// set must reproduce the clone-private free-set exactly. Perturbing ONLY the
/// live-ALLOC input — dropping a record (under-log) or adding a spurious one
/// (over-log) — must abort HARD, in both directions.
#[test]
fn clone_drop_shadow_fires_on_livelist_disagreement() {
    let (_d, db) = mk_db_with_shards(1);
    let src = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(src, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(snap).unwrap();
    for i in 0u64..8 {
        db.insert(clone, i, v(0xC0 | i as u8)).unwrap();
    }
    db.flush().unwrap();

    let m = db.manifest();
    let src_root = m
        .volumes
        .iter()
        .find(|e| e.ord == src)
        .unwrap()
        .l2p_shard_roots[0];
    let clone_entry = m.volumes.iter().find(|e| e.ord == clone).unwrap();
    let clone_root = clone_entry.l2p_shard_roots[0];
    let b = clone_entry.branched_at_lsn;

    // Real, self-consistent baseline: free-set == live-ALLOC == clone-private.
    let la: Vec<(crate::types::PageId, crate::types::Lsn)> =
        db.test_clone_live_allocs(clone).unwrap().into_iter().collect();
    assert!(!la.is_empty(), "diverged clone must have clone-private pages");
    let freed: Vec<(crate::types::PageId, crate::types::Lsn, u32)> =
        la.iter().map(|&(pid, birth)| (pid, birth, 1)).collect();

    // Under-log: live-ALLOC omits a page page-rc frees → HARD.
    let short: Vec<_> = la[1..].to_vec();
    let err = db
        .test_check_clone_livelist_shadow(
            clone,
            b,
            &freed,
            &[clone_root],
            &[src_root],
            &allocs(&short),
        )
        .unwrap_err();
    assert!(
        matches!(err, MetaDbError::Corruption(ref m) if m.contains("under-logged")),
        "expected under-log Corruption, got {err:?}"
    );

    // Over-log: live-ALLOC names an extra C-exclusive page page-rc does NOT
    // free → HARD. The sentinel pid is unreachable from the src survivor and
    // born after the branch, so it lands in `la_exclusive` only.
    let mut over = la.clone();
    over.push((u64::MAX / 2, b + 1000));
    let err = db
        .test_check_clone_livelist_shadow(
            clone,
            b,
            &freed,
            &[clone_root],
            &[src_root],
            &allocs(&over),
        )
        .unwrap_err();
    assert!(
        matches!(err, MetaDbError::Corruption(ref m) if m.contains("over-logged")),
        "expected over-log Corruption, got {err:?}"
    );
}

/// D regression (make-or-break): a clone written then dropped with NO
/// intervening `flush()` has live-ALLOC records ONLY in the in-memory buffer —
/// nothing is sealed to the segment chain. `drop_volume` must union the chain
/// with `page_live_list.peek()`; reading the chain alone would under-count and
/// the livelist cross-check would false-HARD on a legal drop. The asserts pin
/// that the chain really is empty + the buffer really is non-empty at drop
/// time, so the union is load-bearing, not decorative.
#[test]
fn clone_drop_without_flush_unions_unsealed_livelist_buffer() {
    let (_d, db) = mk_db_with_shards(1);
    let src = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(src, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(snap).unwrap();
    // Diverge the clone but DO NOT flush — records stay in the buffer.
    for i in 0u64..8 {
        db.insert(clone, i, v(0xC0 | i as u8)).unwrap();
    }

    // Chain is empty (nothing sealed), buffer is non-empty: chain-only LA would
    // be a strict subset of the true LA → assertion #3 would false-HARD.
    let (head, tail) = db.test_page_live_list_anchors(clone).unwrap();
    assert_eq!(head, crate::types::NULL_PAGE, "no segment sealed without flush");
    assert_eq!(tail, crate::types::NULL_PAGE, "no segment sealed without flush");
    assert!(
        db.test_page_live_list_len(clone).unwrap() > 0,
        "un-sealed records must be sitting in the buffer"
    );

    // The real drop reconstructs LA from chain ∪ buffer and must succeed.
    assert!(
        db.drop_volume(clone).unwrap().is_some(),
        "legal drop must not abort; the buffer union is required"
    );

    // Source survives untouched.
    for i in 0u64..8 {
        assert_eq!(db.get(src, i).unwrap(), Some(v(i as u8)));
    }
}

/// Deterministic clone-churn STRESS: hammers create / write / snapshot /
/// clone / sibling-clone / clone-of-clone / promotion / drop in varied
/// orders, so every legal `drop_volume(clone)` runs the livelist shadow.
/// The failure signal is the shadow HARD-aborting on a LEGAL drop (a false
/// premature) — `drop_volume` would return `Err(Corruption)` and the
/// `.unwrap()` would panic. Plus a write-then-read-back integrity spot-check.
/// Bounded + fixed-seed so it runs in-sandbox; the long nvme soak (with a
/// public promotion-churn op) remains the release gate.
#[test]
fn clone_churn_shadow_stress() {
    use crate::SnapshotId;
    use crate::types::VolumeOrdinal;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;
    use std::collections::{HashMap, HashSet};

    // Base/clone separation keeps this CLONE-shadow stress isolated from the
    // SNAPSHOT page-deadlist shadow (Phase 2b), whose two known/deferred,
    // data-safe bugs (created_lsn tie → premature, chain → missing; see memory
    // zfs_port_phase2b_deadlist_two_bugs) trip on snapshot DROPS. So this test
    // NEVER drops a snapshot: a small fixed set of base volumes + base
    // snapshots lives forever; the churn only creates/writes/promotes/DROPS
    // CLONES, whose drops run `check_clone_livelist_shadow`. The failure signal
    // is a false-premature abort (drop_volume → Err(Corruption) → unwrap panic)
    // or a write-then-read mismatch. The long nvme soak (with a public
    // promotion-churn op AND the snapshot-deadlist bugs fixed) is the real gate.
    // 1 shard keeps VolumeEntry / SnapshotEntry rows small so the most
    // volumes + snapshots fit the single-page manifest (the capacity wall);
    // capacity hits are still skipped defensively below.
    let (_d, db) = mk_db_with_shards(1);
    let mut rng = ChaCha8Rng::seed_from_u64(0xC10E_5EED);

    // Helper: is this Err the known manifest single-page capacity wall (a
    // documented limit, not a correctness bug) — skip rather than fail.
    fn is_capacity(e: &MetaDbError) -> bool {
        matches!(e, MetaDbError::InvalidArgument(m) if m.contains("exceeds capacity"))
    }

    // Base layer: 3 volumes, each diverged + snapshotted (snapshots are the
    // clone sources, kept forever).
    let mut base_snaps: Vec<SnapshotId> = Vec::new();
    for b in 0..3u64 {
        let vol = db.create_volume().unwrap();
        for i in 0u64..48 {
            db.insert(vol, i, v((b as u8) << 4 | i as u8)).unwrap();
        }
        base_snaps.push(db.take_snapshot(vol).unwrap());
    }

    // Clone bookkeeping: ord -> (parent_vol_ord, promoted). A clone is
    // droppable iff it has no live snapshot of its own and no pending-promotion
    // child. We snapshot clones rarely; those snapshots are never dropped, so
    // the snapshotted clone stays pinned (undroppable) — capped to avoid the
    // manifest snapshot-capacity wall.
    let mut clones: HashSet<VolumeOrdinal> = HashSet::new();
    let mut parent: HashMap<VolumeOrdinal, Option<VolumeOrdinal>> = HashMap::new();
    let mut clone_snap_src: HashSet<VolumeOrdinal> = HashSet::new();
    let mut clone_snaps: Vec<(SnapshotId, VolumeOrdinal)> = Vec::new();
    let mut data: HashMap<VolumeOrdinal, HashMap<u64, crate::paged::L2pValue>> = HashMap::new();

    let snap_cap = 12; // base (3) + clone snapshots, < manifest capacity (~21)

    for _ in 0..2000 {
        match rng.r#gen::<u8>() % 12 {
            // clone from a base snapshot OR a clone snapshot (clone-of-clone)
            0 | 1 | 2 if clones.len() < 10 => {
                let from_clone = rng.r#gen::<bool>() && !clone_snaps.is_empty();
                let sid = if from_clone {
                    clone_snaps[rng.r#gen::<usize>() % clone_snaps.len()].0
                } else {
                    base_snaps[rng.r#gen::<usize>() % base_snaps.len()]
                };
                let ord = match db.clone_volume(sid) {
                    Ok(ord) => ord,
                    Err(ref e) if is_capacity(e) => continue, // capacity wall: skip
                    Err(e) => panic!("clone_volume: {e:?}"),
                };
                let src = db
                    .manifest()
                    .volumes
                    .iter()
                    .find(|e| e.ord == ord)
                    .and_then(|e| e.parent_vol_ord);
                parent.insert(ord, src);
                clones.insert(ord);
            }
            // write to a clone + read-back integrity check
            3 | 4 | 5 if !clones.is_empty() => {
                let cs: Vec<_> = clones.iter().copied().collect();
                let c = cs[rng.r#gen::<usize>() % cs.len()];
                let k = rng.r#gen::<u64>() % 48;
                let val = v(rng.r#gen::<u8>());
                db.insert(c, k, val).unwrap();
                data.entry(c).or_default().insert(k, val);
                assert_eq!(db.get(c, k).unwrap(), Some(val));
            }
            // snapshot a clone (enables clone-of-clone); capped, never dropped
            6 if clone_snaps.len() + base_snaps.len() < snap_cap && !clones.is_empty() => {
                let cs: Vec<_> = clones.iter().copied().collect();
                let c = cs[rng.r#gen::<usize>() % cs.len()];
                match db.take_snapshot(c) {
                    Ok(id) => {
                        clone_snaps.push((id, c));
                        clone_snap_src.insert(c);
                    }
                    Err(ref e) if is_capacity(e) => {} // capacity wall: skip
                    Err(e) => panic!("take_snapshot: {e:?}"),
                }
            }
            // promote a pending clone to completion (clears parent_vol_ord)
            7 | 8 => {
                let pending: Vec<VolumeOrdinal> = clones
                    .iter()
                    .copied()
                    .filter(|c| parent.get(c).copied().flatten().is_some())
                    .collect();
                if !pending.is_empty() {
                    let c = pending[rng.r#gen::<usize>() % pending.len()];
                    for _ in 0..128 {
                        match db.test_run_promotion_chunk(c).unwrap() {
                            PromotionStep::Completed | PromotionStep::NotApplicable => break,
                            _ => {}
                        }
                    }
                    parent.insert(c, None);
                }
            }
            // drop a droppable clone — runs check_clone_livelist_shadow.
            // Droppable = no live snapshot of its own + no pending-promotion
            // child clone. A false-premature shadow abort surfaces as Err.
            9 | 10 | 11 => {
                let droppable: Vec<VolumeOrdinal> = clones
                    .iter()
                    .copied()
                    .filter(|&c| {
                        !clone_snap_src.contains(&c)
                            && !clones.iter().any(|x| {
                                parent.get(x).copied().flatten() == Some(c)
                            })
                    })
                    .collect();
                if !droppable.is_empty() {
                    let victim = droppable[rng.r#gen::<usize>() % droppable.len()];
                    db.drop_volume(victim).unwrap();
                    clones.remove(&victim);
                    parent.remove(&victim);
                    data.remove(&victim);
                }
            }
            _ => {}
        }
    }

    // Final: every surviving clone reads back its last-written values + no IO
    // error on a full range scan.
    for (&c, kv) in &data {
        if !clones.contains(&c) {
            continue;
        }
        for (&k, &val) in kv {
            assert_eq!(db.get(c, k).unwrap(), Some(val), "clone {c} key {k}");
        }
    }
    for &c in &clones {
        let _ = db
            .range(c, ..)
            .unwrap()
            .collect::<crate::Result<Vec<_>>>()
            .unwrap();
    }
}

// ---------------------------------------------------------------------------
// Phase 4 Step 3 (DAG correctness): the clone-drop shadow gate widened from
// `parent_vol_ord.is_some()` (still-clones) to the sticky
// `VOLUME_FLAG_CLONE_LINEAGE` flag, so dropping a PROMOTED ex-clone also runs
// the 3-arm cross-check. Promotion is lite (clears parent_vol_ord, bumps only
// global PBA rc, never restructures the page tree), so a promoted ex-clone may
// still page-rc-SHARE L2P pages with its lineage. These tests prove the widened
// gate fires for promoted ex-clones and that the birth-agnostic reachability
// arms handle the DAG hazards (G6 descendant-share, G8 origin-fallthrough)
// without false-firing. page-rc stays authoritative — shadow only.
// ---------------------------------------------------------------------------

/// Drive the promotion walker to completion for a clone. Panics if the volume
/// is not a clone (NotApplicable) or does not complete in a bounded number of
/// chunks.
fn promote_to_completion(db: &Db, vol: crate::types::VolumeOrdinal) {
    for _ in 0..256 {
        match db.test_run_promotion_chunk(vol).unwrap() {
            PromotionStep::Completed => return,
            PromotionStep::NotApplicable => panic!("vol {vol} is not a clone (NotApplicable)"),
            _ => {}
        }
    }
    panic!("promotion of vol {vol} did not complete in 256 chunks");
}

/// The widened gate fires for a promoted ex-clone (sticky flag set,
/// parent_vol_ord cleared) and the 3-arm shadow stays green on a legal drop.
/// C diverges only lba0, so lba1..7 stay page-rc-shared with the survivor
/// parent — exercising the rc>1-shared-kept path through the widened gate.
#[test]
fn promoted_exclone_drop_shadow_runs_and_stays_green() {
    let (_d, db) = mk_db();
    let p = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(p, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(p).unwrap();
    let c = db.clone_volume(snap).unwrap();
    db.insert(c, 0, v(0xC0)).unwrap(); // diverge only lba0; lba1..7 shared with P/snap
    promote_to_completion(&db, c);

    // We are genuinely in the newly-covered category: the Step-1 gate
    // (parent_vol_ord.is_some()) would SKIP this; the Step-3 flag gate runs it.
    let entry = db
        .manifest()
        .volumes
        .iter()
        .find(|e| e.ord == c)
        .cloned()
        .unwrap();
    assert!(
        entry.parent_vol_ord.is_none(),
        "C must be a promoted ex-clone (parent_vol_ord cleared)"
    );
    assert!(
        entry.flags & crate::manifest::VOLUME_FLAG_CLONE_LINEAGE != 0,
        "CLONE_LINEAGE flag must stay sticky so the widened gate fires"
    );

    assert!(
        db.drop_volume(c).unwrap().is_some(),
        "promoted-ex-clone drop must run the shadow and stay green"
    );
    // Shared origin pages kept: parent + snapshot survive intact.
    for i in 0u64..8 {
        assert_eq!(db.get(p, i).unwrap(), Some(v(i as u8)));
    }
    let view = db.snapshot_view(snap).unwrap();
    for i in 0u64..8 {
        assert_eq!(view.get(i).unwrap(), Some(v(i as u8)));
    }
}

/// G6 for a PROMOTED ex-clone: C's born>B1 pages are still shared with a
/// promoted descendant D. Dropping the promoted ex-clone C must NOT premature —
/// those pages are reachable from survivor D, so `exclusive` excludes them and
/// page-rc keeps them (rc>1). Extends the still-clone middle-drop test to
/// dropping a PROMOTED ancestor.
#[test]
fn promoted_exclone_g6_descendant_share_no_premature() {
    let (_d, db) = mk_db();
    let p = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(p, i, v(i as u8)).unwrap();
    }
    let s1 = db.take_snapshot(p).unwrap();
    let c = db.clone_volume(s1).unwrap();
    for i in 0u64..8 {
        db.insert(c, i, v(0xC0 | i as u8)).unwrap(); // C born > B1
    }
    let s2 = db.take_snapshot(c).unwrap();
    let d = db.clone_volume(s2).unwrap(); // D shares C's born>B1 pages via s2
    for i in 0u64..8 {
        assert_eq!(db.get(d, i).unwrap(), Some(v(0xC0 | i as u8)));
    }
    // Legalize C's drop AND promote C into an ex-clone while D still shares.
    let _ = db.drop_snapshot(s2).unwrap().unwrap();
    promote_to_completion(&db, d); // clears D.parent_vol_ord == C
    promote_to_completion(&db, c); // C becomes a promoted ex-clone

    assert!(
        db.drop_volume(c).unwrap().is_some(),
        "promoted-ex-clone drop must not abort premature (pages shared with survivor D)"
    );
    for i in 0u64..8 {
        assert_eq!(db.get(d, i).unwrap(), Some(v(0xC0 | i as u8)));
    }
}

/// G8 for a PROMOTED ex-clone: a born<=B origin page falls sole-owned to C
/// after the parent diverges + the source snapshot is dropped. Dropping the
/// promoted ex-clone must NOT abort missing — the page is in both
/// `structural_free` (rc==1) and `exclusive` (no survivor), and being born<=B it
/// drops out of both livelist sides. Extends the still-clone G8 test to the
/// promoted case (now that missing is HARD).
#[test]
fn promoted_exclone_g8_origin_fallthrough_no_missing() {
    let (_d, db) = mk_db();
    let p = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(p, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(p).unwrap();
    let c = db.clone_volume(snap).unwrap();
    db.insert(p, 0, v(0xF0)).unwrap(); // parent diverges over lba0
    db.insert(c, 7, v(0xC7)).unwrap(); // C private page (born > B)
    let _ = db.drop_snapshot(snap).unwrap().unwrap(); // old origin pages fall to C
    promote_to_completion(&db, c);

    assert!(
        db.drop_volume(c).unwrap().is_some(),
        "promoted-ex-clone G8 drop must not abort missing"
    );
    assert_eq!(db.get(p, 0).unwrap(), Some(v(0xF0)));
    for i in 1u64..8 {
        assert_eq!(db.get(p, i).unwrap(), Some(v(i as u8)));
    }
}

/// Two promoted sibling ex-clones of the same snapshot, dropped in both orders.
/// Shared origin pages are kept while the sibling lives (it is a survivor) and
/// freed when sole-owned — order-independent through the widened gate.
#[test]
fn promoted_sibling_exclones_drop_both_orders() {
    for drop_a_first in [true, false] {
        let (_d, db) = mk_db();
        let src = db.create_volume().unwrap();
        for i in 0u64..8 {
            db.insert(src, i, v(i as u8)).unwrap();
        }
        let snap = db.take_snapshot(src).unwrap();
        let a = db.clone_volume(snap).unwrap();
        let b = db.clone_volume(snap).unwrap();
        db.insert(a, 0, v(0xAA)).unwrap();
        db.insert(b, 0, v(0xBB)).unwrap();
        promote_to_completion(&db, a);
        promote_to_completion(&db, b);

        let (first, second) = if drop_a_first { (a, b) } else { (b, a) };
        assert!(db.drop_volume(first).unwrap().is_some());
        assert!(db.drop_volume(second).unwrap().is_some());
        for i in 0u64..8 {
            assert_eq!(db.get(src, i).unwrap(), Some(v(i as u8)));
        }
    }
}

/// Surviving_roots completeness re-proof in test form: promote D (clearing
/// D.parent_vol_ord so the former parent P becomes droppable), then drop P.
/// Pages P shares with the survivor promoted ex-clone D must NOT be freed
/// (page-rc keeps them rc>1). D diverges only lba0, so lba1..7 stay shared.
#[test]
fn drop_promoted_exclones_former_parent() {
    let (_d, db) = mk_db();
    let p = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(p, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(p).unwrap();
    let d = db.clone_volume(snap).unwrap();
    db.insert(d, 0, v(0xD0)).unwrap(); // D diverges only lba0; lba1..7 shared with P
    promote_to_completion(&db, d); // clears D.parent_vol_ord -> P droppable
    let _ = db.drop_snapshot(snap).unwrap().unwrap(); // P has no live snapshot now

    assert!(
        db.drop_volume(p).unwrap().is_some(),
        "former parent drop must succeed; survivor D keeps the shared pages"
    );
    assert_eq!(db.get(d, 0).unwrap(), Some(v(0xD0)));
    for i in 1u64..8 {
        assert_eq!(db.get(d, i).unwrap(), Some(v(i as u8)));
    }
}

/// A promoted ex-clone survives close + reopen: the sticky flag is rehydrated
/// (so the widened gate still fires) and `clone_birth_lsn` re-arms, and the
/// post-reopen drop stays green.
#[test]
fn promoted_exclone_reopen_then_drop() {
    let dir = tempfile::TempDir::new().unwrap();
    let (src, clone) = {
        let db = Db::create(dir.path()).unwrap();
        let src = db.create_volume().unwrap();
        for i in 0u64..8 {
            db.insert(src, i, v(i as u8)).unwrap();
        }
        let snap = db.take_snapshot(src).unwrap();
        let clone = db.clone_volume(snap).unwrap();
        for i in 0u64..8 {
            db.insert(clone, i, v(0xC0 | i as u8)).unwrap();
        }
        let _ = db.drop_snapshot(snap).unwrap().unwrap();
        promote_to_completion(&db, clone);
        db.flush().unwrap();
        (src, clone)
    };
    let db = Db::open(dir.path()).unwrap();
    let entry = db
        .manifest()
        .volumes
        .iter()
        .find(|e| e.ord == clone)
        .cloned()
        .unwrap();
    assert!(entry.parent_vol_ord.is_none(), "still a promoted ex-clone after reopen");
    assert!(
        entry.flags & crate::manifest::VOLUME_FLAG_CLONE_LINEAGE != 0,
        "sticky flag must survive reopen so the widened gate fires"
    );
    assert!(db.drop_volume(clone).unwrap().is_some());
    for i in 0u64..8 {
        assert_eq!(db.get(src, i).unwrap(), Some(v(i as u8)));
    }
}

/// Negative control: the HARD detectors still fire on a promoted ex-clone's
/// inputs (the widened gate did not silently disable them). Driven via the
/// shim so a divergence can be crafted; uses a genuinely-promoted ex-clone's
/// roots/branch.
#[test]
fn teeth_still_fire_through_widened_gate() {
    let (_d, db) = mk_db_with_shards(1);
    let src = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(src, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(snap).unwrap();
    for i in 0u64..8 {
        db.insert(clone, i, v(0xC0 | i as u8)).unwrap();
    }
    promote_to_completion(&db, clone);
    db.flush().unwrap();

    let m = db.manifest();
    let src_root = m
        .volumes
        .iter()
        .find(|e| e.ord == src)
        .unwrap()
        .l2p_shard_roots[0];
    let clone_entry = m.volumes.iter().find(|e| e.ord == clone).unwrap();
    let clone_root = clone_entry.l2p_shard_roots[0];
    let b = clone_entry.branched_at_lsn;
    assert!(
        clone_entry.parent_vol_ord.is_none(),
        "must exercise a promoted ex-clone"
    );

    // Premature still fires: claim a survivor-reachable page (src root) freed.
    let err = db
        .test_check_clone_livelist_shadow(
            clone,
            b,
            &[(src_root, 0, 1)],
            &[clone_root],
            &[src_root],
            &[],
        )
        .unwrap_err();
    assert!(
        matches!(err, MetaDbError::Corruption(_)),
        "expected premature Corruption on promoted-ex-clone inputs, got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// Phase 4 Step 4 (S1c): READ-ONLY clone COW-kill birth-operand shadow. page-rc
// stays authoritative; these characterize where the candidate post-page-rc
// clone COW-kill operand `birth(P) <= max(branched_at_lsn, youngest_snap(C))`
// is safe vs insufficient (it cannot see cross-volume DAG sharing).
// ---------------------------------------------------------------------------

/// A simple clone (diverged, source snapshot alive, no descendants) has a SAFE
/// pure-birth operand: shared origin pages (born<=B) are pinned by the snapshot
/// and born<=B_eff (COW'd); clone-private pages (born>B) are unpinned (benign
/// over-COW). The safety direction must be clean.
#[test]
fn clone_birth_shadow_clean_on_simple_clone() {
    let (_d, db) = mk_db();
    let p = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(p, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(p).unwrap();
    let c = db.clone_volume(snap).unwrap();
    db.insert(c, 0, v(0xC0)).unwrap(); // diverge lba0 only; lba1..7 stay shared
    db.flush().unwrap();

    let findings = db.test_clone_birth_shadow_findings().unwrap();
    assert!(
        findings.is_empty(),
        "pure-birth clone COW-kill operand must be safe for a simple clone: {findings:?}"
    );
}

/// KEY FINDING (S1c): the pure-birth clone COW-kill operand is INSUFFICIENT for
/// the DAG. C's born>B1 pages are still shared with a survivor descendant D, but
/// after C's own snapshot s2 is dropped, B_eff(C) falls back to B1 (< their
/// birth) — so the operand would write them in place, clobbering pages D reads.
/// page-rc prevents this today (rc>1); the birth model alone cannot. The shadow
/// must FLAG it, proving S1c's clone COW-kill must consult reachability, not
/// birth. (Read-only — page-rc authoritative; nothing is actually clobbered.)
#[test]
fn clone_birth_shadow_flags_descendant_pinned_born_after_branch() {
    let (_d, db) = mk_db();
    let p = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(p, i, v(i as u8)).unwrap();
    }
    let s1 = db.take_snapshot(p).unwrap();
    let c = db.clone_volume(s1).unwrap();
    for i in 0u64..8 {
        db.insert(c, i, v(0xC0 | i as u8)).unwrap(); // C born > B1
    }
    let s2 = db.take_snapshot(c).unwrap();
    let d = db.clone_volume(s2).unwrap(); // D shares C's born>B1 pages
    for i in 0u64..8 {
        assert_eq!(db.get(d, i).unwrap(), Some(v(0xC0 | i as u8)));
    }
    // Drop C's own snapshot -> youngest_snap(C) falls below C's born>B1 page
    // births -> B_eff(C) == B1, yet D still pins those pages.
    let _ = db.drop_snapshot(s2).unwrap().unwrap();
    db.flush().unwrap();

    let findings = db.test_clone_birth_shadow_findings().unwrap();
    assert!(
        findings.iter().any(|f| f.contains(&format!("vol {c} "))),
        "pure-birth clone operand is insufficient here (C's born>B1 pages pinned by survivor D, \
         B_eff(C) fell below their birth) — S1c must use reachability, not birth. findings={findings:?}"
    );
}
