//! BFG: per-clone page-livelist SHADOW (`drop_volume`
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

/// merge-reanchor/G3/G4: sibling clones share the snapshot's pages, each writes a private
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

/// descendant-share (PREMATURE load-bearing): clone-of-clone. C diverges
/// (clone-private pages born > B1), a snapshot `s2` of C is cloned into D, then
/// `s2` is dropped
/// and D is promoted — making `drop_volume(C)` *legal* while D still shares
/// C's `birth > B1` pages. The exclusive-reachability shadow must NOT fire
/// premature (those pages are reachable from D, a survivor, so page-rc keeps
/// them); a naive `birth > B` predicate WOULD free them = the premature-free
/// case the port exists to kill. D's data must survive C's drop.
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
    // D shares C's born>B1 pages via s2's roots.
    for i in 0u64..8 {
        assert_eq!(db.get(d, i).unwrap(), Some(v(0xC0 | i as u8)));
    }

    // Legalize C's drop: drop s2 (C has no live snapshot) + promote D (clears
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
    assert!(
        report.is_some(),
        "drop_volume(C) should succeed, not abort premature"
    );

    // D's data survived C's drop (the shared born>B1 pages were not freed).
    for i in 0u64..8 {
        assert_eq!(db.get(d, i).unwrap(), Some(v(0xC0 | i as u8)));
    }
}

/// origin-fallthrough-shape: the parent diverges over a shared LBA and the source snapshot is
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
    let la: Vec<(crate::types::PageId, crate::types::Lsn)> = db
        .test_clone_live_allocs(clone)
        .unwrap()
        .into_iter()
        .collect();
    let freed: Vec<(crate::types::PageId, crate::types::Lsn, u32)> =
        la.iter().map(|&(pid, birth)| (pid, birth, 1)).collect();
    db.test_check_clone_livelist_shadow(clone, b, &freed, &[clone_root], &[src_root], &allocs(&la))
        .unwrap();
}

/// TEETH (missing → HARD): a free-set that frees NOTHING while the clone has
/// C-exclusive reachable pages is a page-rc leak — the C-exclusive reachability
/// walk frees pages page-rc keeps live. escalates this from a
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
    let la: Vec<(crate::types::PageId, crate::types::Lsn)> = db
        .test_clone_live_allocs(clone)
        .unwrap()
        .into_iter()
        .collect();
    assert!(
        !la.is_empty(),
        "diverged clone must have clone-private pages"
    );
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
    assert_eq!(
        head,
        crate::types::NULL_PAGE,
        "no segment sealed without flush"
    );
    assert_eq!(
        tail,
        crate::types::NULL_PAGE,
        "no segment sealed without flush"
    );
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
    // SNAPSHOT page-deadlist shadow (), whose two known/deferred,
    // data-safe bugs (created_lsn tie → premature, chain → missing; see memory
    // the earlier snapshot-deadlist regressions trip on snapshot DROPS. So this test
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
                            && !clones
                                .iter()
                                .any(|x| parent.get(x).copied().flatten() == Some(c))
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

    // clone COW-kill: the page-rc-INDEPENDENT clone COW-kill operand must stay a complete
    // superset of "pinned" across the whole clone+promote+drop churn — every page
    // a surviving pinner reaches is COW'd by the operand (no clobber). Flush so the
    // manifest reflects committed roots, then run the shadow over the final state.
    db.flush().unwrap();
    let findings = db.test_clone_birth_shadow_findings().unwrap();
    assert!(
        findings.is_empty(),
        "clone-birth-shadow must be clean across clone churn (operand under-pinned): {findings:?}"
    );
}

// ---------------------------------------------------------------------------
// the clone-drop shadow gate widened from
// `parent_vol_ord.is_some()` (still-clones) to the sticky
// `VOLUME_FLAG_CLONE_LINEAGE` flag, so dropping a PROMOTED ex-clone also runs
// the 3-arm cross-check. Promotion is lite (clears parent_vol_ord, bumps only
// global PBA rc, never restructures the page tree), so a promoted ex-clone may
// still page-rc-SHARE L2P pages with its lineage. These tests prove the widened
// gate fires for promoted ex-clones and that the birth-agnostic reachability
// arms handle the DAG hazards (descendant-share descendant-share, origin-fallthrough origin-fallthrough)
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

    // We are genuinely in the promoted-ex-clone category: the old
    // `parent_vol_ord.is_some()` gate would skip this; the sticky clone-lineage
    // flag gate runs it.
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

/// descendant-share for a PROMOTED ex-clone: C's born>B1 pages are still shared with a
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

/// origin-fallthrough for a PROMOTED ex-clone: a born<=B origin page falls sole-owned to C
/// after the parent diverges + the source snapshot is dropped. Dropping the
/// promoted ex-clone must NOT abort missing — the page is in both
/// `structural_free` (rc==1) and `exclusive` (no survivor), and being born<=B it
/// drops out of both livelist sides. Extends the still-clone origin-fallthrough test to the
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
        "promoted-ex-clone origin-fallthrough drop must not abort missing"
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

/// BFG: a CLONE drop frees the reachability `exclusive` set
/// via `DropVolume.free_pages = Some(...)` (the flip), NOT the implicit page-rc
/// rc→0 cascade. End-to-end durable read-back: the clone's C-exclusive pages
/// are freed, the surviving source keeps every shared page, and reopen + strict
/// verify (incl. the page-rc array audit + clone-livelist) is clean — proving
/// the flip frees exactly the right set with no leak or premature free.
///
/// NB the clone's exclusive pages are freed on the LIVE path; the DropVolume
/// *replay* arm is a no-op (the volume-removed manifest commits before the WAL
/// submit, so `volumes.contains_key` is false on reopen) and the crash backstop
/// is `reclaim_orphan_pages`. We flush after the drop so the page-rc decref of
/// the kept boundary pages folds durably (a no-flush volume drop leaves that
/// decref unfolded — a pre-existing, data-safe over-count unrelated to this
/// free-set path).
#[test]
fn s2_drop_clone_frees_exclusive_reachability_set() {
    let dir = tempfile::TempDir::new().unwrap();
    let src = {
        let db = Db::create(dir.path()).unwrap();
        let src = db.create_volume().unwrap();
        for i in 0u64..8 {
            db.insert(src, i, v(i as u8)).unwrap();
        }
        let snap = db.take_snapshot(src).unwrap();
        let clone = db.clone_volume(snap).unwrap();
        // C diverges every lba so it owns clone-private (C-exclusive) pages.
        for i in 0u64..8 {
            db.insert(clone, i, v(0xC0 | i as u8)).unwrap();
        }
        // Drop the source snapshot (so it is not a survivor pinning the shared
        // pages), then drop the clone — the flip frees C's exclusive set.
        let _ = db.drop_snapshot(snap).unwrap().unwrap();
        let report = db.drop_volume(clone).unwrap().expect("drop clone");
        assert!(
            report.pages_freed > 0,
            "S2 reachability free_pages must free C-exclusive pages"
        );
        db.flush().unwrap();
        assert!(!db.volumes().contains(&clone), "clone gone after drop");
        for i in 0u64..8 {
            assert_eq!(
                db.get(src, i).unwrap(),
                Some(v(i as u8)),
                "source lba {i} kept"
            );
        }
        src
    };
    let db = Db::open(dir.path()).unwrap();
    for i in 0u64..8 {
        assert_eq!(
            db.get(src, i).unwrap(),
            Some(v(i as u8)),
            "source lba {i} after reopen"
        );
    }
    drop(db);
    let report = crate::verify::verify_path(
        dir.path(),
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
            check_clone_livelist: true,
            check_clone_birth_shadow: true,
        },
    )
    .unwrap();
    assert!(
        report.is_clean(),
        "verify issues after clone flip drop: {:?}",
        report.issues
    );
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
    assert!(
        entry.parent_vol_ord.is_none(),
        "still a promoted ex-clone after reopen"
    );
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
// READ-ONLY clone COW-kill birth-operand shadow. page-rc
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

/// clone COW-kill (the fix): the operand now consults DESCENDANT BRANCH POINTS, so the descendant-share
/// shape the pure-birth operand mishandled is CLEAN. C's born>B1 pages are shared
/// with a survivor descendant D; after C's own snapshot s2 is dropped, the
/// pure-birth `B_eff = max(B1, youngest_snap(C))` falls to B1 (< their birth) and
/// would clobber them — but D's branch point (= s2.created_lsn > those births) is
/// in C's pinner set, so `B_eff` stays above the births and the operand COWs them.
/// This is the exact case `clone_cow_pinners`' descendant term exists to cover.
#[test]
fn clone_birth_shadow_clean_under_descendant_pinner() {
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
    // births, but D's branch point (s2.created_lsn) keeps them pinned.
    let _ = db.drop_snapshot(s2).unwrap().unwrap();
    db.flush().unwrap();

    let findings = db.test_clone_birth_shadow_findings().unwrap();
    assert!(
        findings.is_empty(),
        "clone COW-kill operand consults descendant branch points, so C's born>B1 pages shared with survivor \
         D must stay COW'd (no clobber). findings={findings:?}"
    );
}

/// clone COW-kill END-TO-END (the data-survival proof): the runtime clone COW-kill must
/// PRESERVE a born>B page a survivor descendant still references when the clone
/// overwrites it in place — even after the clone's own snapshot is dropped. This
/// is the descendant-share shape driven through the real write path: build C, snapshot s2,
/// clone D from s2 (D shares C's born>B pages), drop s2, then OVERWRITE those
/// LBAs on C and assert D still reads the OLD values. (With the page-rc floor
/// kept this also passes via rc, but the structural `clone_birth_shadow` gate
/// below proves the page-rc-INDEPENDENT operand is what holds it.)
#[test]
fn clone_cow_kill_preserves_descendant_shared_page_after_snap_drop() {
    let (_d, db) = mk_db();
    let p = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(p, i, v(i as u8)).unwrap();
    }
    let s1 = db.take_snapshot(p).unwrap();
    let c = db.clone_volume(s1).unwrap();
    for i in 0u64..8 {
        db.insert(c, i, v(0xC0 | i as u8)).unwrap(); // C-private, born > B_C
    }
    let s2 = db.take_snapshot(c).unwrap();
    let d = db.clone_volume(s2).unwrap(); // D shares C's born>B_C pages via s2
    // Drop C's own snapshot: the only remaining pinner of C's born>B_C pages is
    // the survivor descendant D (its branch point), NOT a live snapshot.
    let _ = db.drop_snapshot(s2).unwrap().unwrap();
    db.flush().unwrap();
    // C overwrites every born>B_C page IN PLACE. The clone COW-kill must preserve
    // the old versions (D reads them); a clobber corrupts D.
    for i in 0u64..8 {
        db.insert(c, i, v(0xE0 | i as u8)).unwrap();
    }
    db.flush().unwrap();
    for i in 0u64..8 {
        assert_eq!(
            db.get(d, i).unwrap(),
            Some(v(0xC0 | i as u8)),
            "D's view of lba {i} was clobbered by C's in-place overwrite (clone COW-kill under-pinned)"
        );
        assert_eq!(
            db.get(c, i).unwrap(),
            Some(v(0xE0 | i as u8)),
            "C's own overwrite of lba {i} lost"
        );
    }
    // The page-rc-independent operand agrees (no clobber would have been allowed).
    let findings = db.test_clone_birth_shadow_findings().unwrap();
    assert!(
        findings.is_empty(),
        "clone-birth-shadow not clean: {findings:?}"
    );
}

/// TEETH: the `clone_birth_shadow` HARD gate must FIRE when a descendant pin is
/// missing from the operand. Build the descendant-share DAG (clean), then corrupt D's manifest
/// `branched_at_lsn` DOWN to B_C — dropping D from C's operand pinner set WITHOUT
/// changing the reachability ground truth (D's roots still reach C's born>B
/// pages). The operand now under-pins → the safety direction `pinned ∧ ¬COW`
/// fires. Proves the gate has teeth (an under-pin is not silently passed).
#[test]
fn clone_birth_shadow_fires_when_descendant_pinner_missing() {
    let (_d, db) = mk_db();
    let p = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(p, i, v(i as u8)).unwrap();
    }
    let s1 = db.take_snapshot(p).unwrap();
    let c = db.clone_volume(s1).unwrap();
    for i in 0u64..8 {
        db.insert(c, i, v(0xC0 | i as u8)).unwrap(); // C born > B_C
    }
    let s2 = db.take_snapshot(c).unwrap();
    let d = db.clone_volume(s2).unwrap(); // D shares C's born>B_C pages
    let _ = db.drop_snapshot(s2).unwrap().unwrap();
    db.flush().unwrap();
    // Clean while D's real branch point (> the births) pins them.
    assert!(
        db.test_clone_birth_shadow_findings().unwrap().is_empty(),
        "precondition: should be clean with the real descendant pin"
    );
    // Simulate a regression that drops D's pin: lower its branch point to 0 so the
    // `branched_at_lsn > B_C` filter excludes it from C's operand pinner set.
    // Reachability ground truth (D's roots still reach C's born>B pages) unchanged.
    db.test_set_manifest_branched_at_lsn(d, 0);
    let findings = db.test_clone_birth_shadow_findings().unwrap();
    assert!(
        findings.iter().any(|f| f.contains(&format!("vol {c} "))),
        "gate must fire: C's born>B pages are reachable from D but the operand lost D's pin. \
         findings={findings:?}"
    );
}

// ============================================================================
// BFG — clone-involved snapshot-drop free-source (cross-volume clone sharing).
// drop_snapshot of a clone-involved volume now frees the page-rc-independent
// reachability difference (not the single-vol deadlist, which can't model
// cross-volume page sharing), routed by the sticky CLONE_LINEAGE flag (covers
// promoted ex-clones the old `parent_vol_ord` gate missed), and runs the MERGE
// unconditionally so the per-volume chain stays complete across routing flips.
// ============================================================================

/// Verify options with every clone/birth/deadlist oracle on.
fn s2c_verify_all(dir: &std::path::Path) -> crate::verify::VerifyReport {
    crate::verify::verify_path(
        dir,
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
            check_clone_livelist: true,
            check_clone_birth_shadow: true,
        },
    )
    .unwrap()
}

fn s2c_snap_entry(db: &Db, id: crate::SnapshotId) -> crate::manifest::SnapshotEntry {
    db.manifest()
        .snapshots
        .iter()
        .find(|e| e.id == id)
        .cloned()
        .unwrap_or_else(|| panic!("snapshot {id} not found in manifest"))
}

/// T1/T2 (teeth + data-survival, RED-without-fix): drop a snapshot s_v of a
/// plain origin volume P whose pinned pages a PROMOTED ex-clone D still shares,
/// where P has overwritten them after s_v (so they are DEAD in P's own deadlist
/// chain). The OLD `parent_vol_ord` detection routes it to the single-vol
/// deadlist (`is_clone(P)`=false origin; `has_clone_child(P)`=false because D is
/// promoted → parent cleared) → the deadlist frees P's dead pages while D still
/// maps them → PREMATURE FREE Corruption. The NEW sticky-flag routing's "any
/// other clone-lineage volume" disjunct catches the promoted D → reachability
/// free-source → keeps the D-shared pages → success. This is exactly dense-soak
/// issue #2 ("dropping a snapshot a [promoted] clone branched from").
///
/// Verified RED-without-fix: reverting `snapshot_drop_clone_involved` to the old
/// `is_clone || has_clone_child` gate makes `drop_snapshot(s_v)` fire PREMATURE.
#[test]
fn s2c_promoted_exclone_sharer_routes_drop_to_reachability_not_deadlist() {
    let (dir, db) = mk_db();
    let p = db.create_volume().unwrap(); // plain origin (no CLONE_LINEAGE flag)
    for i in 0u64..8 {
        db.insert(p, i, v(i as u8)).unwrap();
    }
    let s_v = db.take_snapshot(p).unwrap(); // s_v pins P's value-i pages
    let d = db.clone_volume(s_v).unwrap(); // D shares P's value-i pages via s_v
    promote_to_completion(&db, d); // D.parent_vol_ord cleared → has_clone_child(P)=false
    for i in 0u64..8 {
        db.insert(p, i, v(0xD0 | i as u8)).unwrap(); // P's value-i pages DIE in P's deadlist; D still maps them
    }
    db.flush().unwrap();

    // We are in the newly-covered category: P is a plain origin (old is_clone
    // false), D is promoted (old has_clone_child false), yet D shares s_v's pages.
    let pentry = db
        .manifest()
        .volumes
        .iter()
        .find(|e| e.ord == p)
        .cloned()
        .unwrap();
    assert!(
        pentry.flags & crate::manifest::VOLUME_FLAG_CLONE_LINEAGE == 0,
        "P must be a plain origin"
    );
    let dentry = db
        .manifest()
        .volumes
        .iter()
        .find(|e| e.ord == d)
        .cloned()
        .unwrap();
    assert!(
        dentry.parent_vol_ord.is_none(),
        "D must be promoted so old has_clone_child(P)=false"
    );
    assert!(
        dentry.flags & crate::manifest::VOLUME_FLAG_CLONE_LINEAGE != 0,
        "D keeps the sticky flag"
    );

    // Old code: routes to the single-vol deadlist → PREMATURE (s_v's pages are
    // dead in P's chain but still reachable from promoted survivor D). New code:
    // the "any other clone-lineage volume" disjunct → reachability → success.
    db.drop_snapshot(s_v)
        .unwrap()
        .expect("clone-involved snapshot drop must use reachability, not false-premature");
    for i in 0u64..8 {
        assert_eq!(
            db.get(d, i).unwrap(),
            Some(v(i as u8)),
            "D lost shared lba {i}"
        );
        assert_eq!(
            db.get(p, i).unwrap(),
            Some(v(0xD0 | i as u8)),
            "P head lba {i}"
        );
    }
    db.flush().unwrap();
    drop(db);
    let db = crate::Db::open(dir.path()).unwrap();
    for i in 0u64..8 {
        assert_eq!(db.get(d, i).unwrap(), Some(v(i as u8)), "reopen D lba {i}");
        assert_eq!(
            db.get(p, i).unwrap(),
            Some(v(0xD0 | i as u8)),
            "reopen P lba {i}"
        );
    }
    drop(db);
    let report = s2c_verify_all(dir.path());
    assert!(
        report.is_clean(),
        "verify after clone-involved drop: {:?}",
        report.issues
    );
}

/// T3 (frees): a promoted-ex-clone snapshot whose pinned pages NOTHING else
/// shares is actually freed by the reachability free-source.
#[test]
fn s2c_clone_involved_snapshot_drop_frees_exclusive_pages() {
    let (dir, db) = mk_db();
    let p = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(p, i, v(i as u8)).unwrap();
    }
    let s1 = db.take_snapshot(p).unwrap();
    let c = db.clone_volume(s1).unwrap();
    for i in 0u64..8 {
        db.insert(c, i, v(0xC0 | i as u8)).unwrap(); // C born > B
    }
    promote_to_completion(&db, c); // promoted ex-clone → clone-involved via sticky flag
    let sc = db.take_snapshot(c).unwrap();
    for i in 0u64..8 {
        db.insert(c, i, v(0xD0 | i as u8)).unwrap(); // diverge after sc → sc-exclusive pages
    }
    db.flush().unwrap();

    let report = db.drop_snapshot(sc).unwrap().expect("drop sc");
    assert!(
        report.pages_freed > 0,
        "clone-involved snapshot drop must free the snapshot-exclusive pages via reachability"
    );
    for i in 0u64..8 {
        assert_eq!(
            db.get(c, i).unwrap(),
            Some(v(0xD0 | i as u8)),
            "C head lost lba {i}"
        );
    }
    db.flush().unwrap();
    drop(db);
    // Reopen so `reclaim_orphan_pages` sweeps the drop's deferred orphans (old
    // deadlist segment chains + the snapshot's SnapshotRoots page) before the
    // offline verify — the same flush→drop→reopen→verify flow the deadlist test
    // uses.
    let db = crate::Db::open(dir.path()).unwrap();
    for i in 0u64..8 {
        assert_eq!(
            db.get(c, i).unwrap(),
            Some(v(0xD0 | i as u8)),
            "reopen C lba {i}"
        );
    }
    drop(db);
    let report = s2c_verify_all(dir.path());
    assert!(
        report.is_clean(),
        "verify after clone-involved exclusive free: {:?}",
        report.issues
    );
}

/// T5 (the make-or-break, RED-without-the-always-merge): the MERGE must run on
/// clone-routed drops so the per-volume deadlist chain stays complete when
/// routing flips back to the deadlist free-source. V takes 3 snapshots while a
/// clone exists; dropping the two oldest is clone-routed (reachability free +
/// merge); after the clone is dropped the youngest drop is non-clone-routed
/// (deadlist) and MUST NOT fire MISSING/COMPLETENESS-HOLE. Reverting the merge
/// to conditional (skip on clone-routed) makes the final drop fire MISSING.
#[test]
fn s2c_merge_runs_on_clone_routed_drop_keeps_chain_consistent_after_flip() {
    // The hole needs a page pinned by a SURVIVING OLDER snapshot whose death is
    // recorded in a MIDDLE snapshot's chain. Drop the MIDDLE snapshot
    // clone-routed (merge must forward those deaths to S_c); then flip routing
    // and drop the OLDER snapshot non-clone — its deadlist must still account
    // the forwarded deaths or `check_page_deadlist_shadow` fires a HARD
    // `Corruption` (this shape surfaces it as PREMATURE; the dropped-forward
    // direction can also surface as a COMPLETENESS HOLE). Layout:
    //   value-i pages born < s_a (s_a pins them)
    //   overwrite +1 between s_a and s_b → value-i deaths land in s_b's window
    //   overwrite +2 between s_b and s_c; overwrite +3 after s_c
    let (dir, db) = mk_db();
    let vv = db.create_volume().unwrap();
    for i in 0u64..16 {
        db.insert(vv, i, v(i as u8)).unwrap(); // value-i pages
    }
    db.flush().unwrap();
    let sa = db.take_snapshot(vv).unwrap(); // s_a pins value-i pages
    for i in 0u64..16 {
        db.insert(vv, i, v((i as u8).wrapping_add(1))).unwrap(); // value-i dies in (s_a, s_b]
    }
    db.flush().unwrap();
    let sb = db.take_snapshot(vv).unwrap();
    for i in 0u64..16 {
        db.insert(vv, i, v((i as u8).wrapping_add(2))).unwrap();
    }
    db.flush().unwrap();
    let sc = db.take_snapshot(vv).unwrap();
    for i in 0u64..16 {
        db.insert(vv, i, v((i as u8).wrapping_add(3))).unwrap();
    }
    db.flush().unwrap();

    // A clone exists → V's snapshot drops are clone-involved (reachability + merge).
    let wc = db.clone_volume(sc).unwrap();
    // Drop the MIDDLE snapshot s_b clone-routed: its chain carries the value-i
    // deaths that surviving s_a still pins → the merge must forward them to s_c.
    db.drop_snapshot(sb)
        .unwrap()
        .expect("clone-routed drop of middle snapshot s_b");

    // Drop the clone → no clone-lineage volume remains → routing flips back.
    db.drop_volume(wc).unwrap().expect("drop clone wc");

    // Drop the OLDER snapshot s_a non-clone-routed: it frees the value-i pages
    // (pinned only by s_a now). Its deadlist must account the value-i deaths the
    // s_b drop forwarded; without the merge the chain diverges → HARD Corruption.
    db.drop_snapshot(sa)
        .unwrap()
        .expect("non-clone drop of s_a after flip must not fire a deadlist Corruption (merge kept chain consistent)");
    for i in 0u64..16 {
        assert_eq!(
            db.get(vv, i).unwrap(),
            Some(v((i as u8).wrapping_add(3))),
            "V head lba {i}"
        );
    }
    db.flush().unwrap();
    drop(db);
    let db = crate::Db::open(dir.path()).unwrap();
    for i in 0u64..16 {
        assert_eq!(
            db.get(vv, i).unwrap(),
            Some(v((i as u8).wrapping_add(3))),
            "reopen lba {i}"
        );
    }
    drop(db);
    let report = s2c_verify_all(dir.path());
    assert!(
        report.is_clean(),
        "verify after merge-across-flip: {:?}",
        report.issues
    );
}

/// T7 (over-routing harmless): an UNRELATED clone (off a different volume)
/// over-routes V's snapshot drop to the reachability path. It still frees V's
/// exclusive pages and leaves the unrelated volumes intact.
#[test]
fn s2c_unrelated_clone_over_routes_harmless() {
    let (dir, db) = mk_db();
    let vv = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(vv, i, v(i as u8)).unwrap();
    }
    db.flush().unwrap();
    let s = db.take_snapshot(vv).unwrap();
    for i in 0u64..8 {
        db.insert(vv, i, v((i as u8).wrapping_add(1))).unwrap(); // s-exclusive old pages
    }
    db.flush().unwrap();

    // Unrelated lineage: U + clone W off U. V never shared with them.
    let u = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(u, i, v(0xE0 | i as u8)).unwrap();
    }
    let su = db.take_snapshot(u).unwrap();
    let w = db.clone_volume(su).unwrap();

    // V's drop is over-routed to reachability (W is clone-lineage), still correct.
    let report = db
        .drop_snapshot(s)
        .unwrap()
        .expect("over-routed drop of V's snapshot");
    assert!(
        report.pages_freed > 0,
        "V's exclusive pages must still be freed"
    );
    for i in 0u64..8 {
        assert_eq!(
            db.get(vv, i).unwrap(),
            Some(v((i as u8).wrapping_add(1))),
            "V lba {i}"
        );
        assert_eq!(db.get(u, i).unwrap(), Some(v(0xE0 | i as u8)), "U lba {i}");
        assert_eq!(db.get(w, i).unwrap(), Some(v(0xE0 | i as u8)), "W lba {i}");
    }
    db.flush().unwrap();
    drop(db);
    // Reopen so deferred orphan reclaim runs before the offline verify.
    let db = crate::Db::open(dir.path()).unwrap();
    for i in 0u64..8 {
        assert_eq!(
            db.get(vv, i).unwrap(),
            Some(v((i as u8).wrapping_add(1))),
            "reopen V lba {i}"
        );
    }
    drop(db);
    let report = s2c_verify_all(dir.path());
    assert!(
        report.is_clean(),
        "verify after unrelated-clone over-route: {:?}",
        report.issues
    );
}

/// T9 (shim teeth): the reachability shadow fires PREMATURE when the structural
/// free-set names a page that is actually reachable from a surviving root.
/// Craft `after_refs` claiming the snapshot root has 0 survivor refs while the
/// live head (no divergence) still reaches it.
#[test]
fn s2c_reachability_shadow_fires_premature_on_crafted_survivor_reachable() {
    use std::collections::BTreeMap;
    let (_d, db) = mk_db();
    let vv = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(vv, i, v(i as u8)).unwrap();
    }
    let s = db.take_snapshot(vv).unwrap(); // no divergence: head == snapshot roots
    let entry = s2c_snap_entry(&db, s);
    let root0 = entry.l2p_shard_roots[0];

    // Survivor roots = the (un-diverged) live head, which == the snapshot roots,
    // so root0 IS survivor-reachable. Lie that after_refs[root0]==0 → it lands in
    // structural_to_free but NOT in `exclusive` → PREMATURE.
    let pages = vec![root0];
    let mut after_refs: BTreeMap<crate::types::PageId, u32> = BTreeMap::new();
    after_refs.insert(root0, 0);
    let all_current_roots: Vec<crate::types::PageId> = entry.l2p_shard_roots.to_vec();
    let other: Vec<crate::manifest::SnapshotEntry> = Vec::new();

    let err = db
        .test_check_clone_drop_reachability_shadow(
            s,
            &entry,
            &pages,
            &after_refs,
            &all_current_roots,
            &other,
        )
        .expect_err("crafted survivor-reachable free must fire PREMATURE");
    assert!(
        matches!(err, MetaDbError::Corruption(ref m) if m.contains("PREMATURE FREE")),
        "expected PREMATURE Corruption, got: {err:?}"
    );
}
