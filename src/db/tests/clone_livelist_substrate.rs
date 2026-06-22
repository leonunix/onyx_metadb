//! ZFS port Phase 3b: per-clone page-livelist SUBSTRATE + POPULATION
//! (SHADOW). The clone's COW/alloc/free witness logs ALLOC/FREE `LiveRecord`s
//! for its clone-private L2P pages (`birth > branched_at_lsn`) into a
//! persistent `LiveListSegment` chain. Page-rc stays authoritative; these
//! tests prove the substrate is populated, survives reopen, costs nothing for
//! non-clones, covers promoted ex-clones, and — the make-or-break invariant —
//! its live-ALLOC set (ALLOC minus matched FREE) equals the clone-private
//! reachable subtree (`reachable(C) ∩ {birth > B}`), the equality Phase 4
//! relies on to free a dropped clone's private pages without page-rc.

use std::sync::Arc;

use super::{mk_db, mk_db_with_shards, v};
use crate::Db;
use crate::db::promotion::PromotionStep;
use crate::page::PageType;
use crate::testing::faults::{FaultAction, FaultController, FaultPoint};
use crate::types::VolumeOrdinal;
use crate::verify::{VerifyOptions, verify_path};

/// Build a db with the background condense worker OFF (so tests drive condense
/// synchronously via `test_run_livelist_condense`) and a clone whose persisted
/// livelist chain spans several segments. Returns the dir, db, faults handle,
/// and the clone ord. Each flush round COWs the clone-private leaves afresh →
/// one appended `LiveListSegment` per round.
fn multi_segment_clone() -> (tempfile::TempDir, Arc<Db>, Arc<FaultController>, VolumeOrdinal) {
    let dir = tempfile::TempDir::new().unwrap();
    let faults = FaultController::new();
    let mut cfg = crate::config::Config::new(dir.path());
    cfg.shards_per_partition = 1;
    cfg.livelist_condense_min_segments = 0;
    let db = Db::create_with_config_and_faults(cfg, faults.clone()).unwrap();
    let base = db.create_volume().unwrap();
    for i in 0u64..16 {
        db.insert(base, i * 256, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(base).unwrap();
    let clone = db.clone_volume(snap).unwrap();
    for round in 0u64..5 {
        for i in 0u64..16 {
            db.insert(clone, i * 256, v((0xC0u8).wrapping_add(round as u8) | (i as u8 & 0x0F)))
                .unwrap();
        }
        db.flush().unwrap();
    }
    (dir, db, faults, clone)
}

/// Offline audit with the clone-livelist equality check on. Asserts clean.
fn assert_livelist_clean(path: &std::path::Path) {
    let report = verify_path(
        path,
        VerifyOptions {
            strict: false,
            check_birth_shadow: true,
            check_clone_livelist: true,
        },
    )
    .unwrap();
    assert!(
        report.is_clean(),
        "verify --clone-livelist issues: {:?}",
        report.issues
    );
}

/// A clone that COWs its shared pages populates the livelist with ALLOC
/// records; a plain non-clone volume on the same db records nothing.
#[test]
fn livelist_populated_by_clone_cow() {
    let (_d, db) = mk_db();
    let src = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(src, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(snap).unwrap();

    // Before any write the clone shares the snapshot's pages (born <= B), so
    // the livelist is empty.
    assert_eq!(db.test_clone_live_allocs(clone).unwrap().len(), 0);

    // Diverge the clone: every COW allocates clone-private (`birth > B`) pages.
    for i in 0u64..8 {
        db.insert(clone, i, v(0xC0 | i as u8)).unwrap();
    }
    let live = db.test_clone_live_allocs(clone).unwrap();
    assert!(!live.is_empty(), "clone COW did not populate the livelist");

    // The non-clone source records nothing (its trees have no capture threshold).
    assert_eq!(db.test_page_live_list_len(src), Some(0));

    db.flush().unwrap();
    drop(db); // verify_path opens its own store; close ours first.
    assert_livelist_clean(_d.path());
}

/// Overwriting a clone-private page emits ALLOC then FREE; the cancelled
/// version drops out of the live-ALLOC set, which keeps matching the current
/// reachable subtree.
#[test]
fn livelist_alloc_then_free_on_clone_overwrite() {
    let (_d, db) = mk_db_with_shards(1);
    let src = db.create_volume().unwrap();
    for i in 0u64..4 {
        db.insert(src, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(snap).unwrap();

    db.insert(clone, 0, v(0xA0)).unwrap();
    let after_first = db.test_clone_live_allocs(clone).unwrap();
    assert!(!after_first.is_empty());

    // Overwrite the same LBA repeatedly: each COWs the (now clone-private)
    // leaf to a fresh page → old version FREE, new version ALLOC. The
    // live-ALLOC set must stay equal to the clone-private reachable subtree,
    // i.e. NOT grow unboundedly with every overwrite.
    for _ in 0..20 {
        db.insert(clone, 0, v(0xA0)).unwrap();
    }
    let after_churn = db.test_clone_live_allocs(clone).unwrap();
    assert!(
        after_churn.len() <= after_first.len() + 2,
        "live-ALLOC set grew with overwrite churn (FREE not cancelling ALLOC): \
         first={} churn={}",
        after_first.len(),
        after_churn.len()
    );

    db.flush().unwrap();
    drop(db);
    assert_livelist_clean(_d.path());
}

/// The livelist chain + anchors survive a close/reopen, the segments stay
/// typed as `LiveListSegment`, and the equality audit passes post-reopen.
#[test]
fn livelist_segments_survive_reopen() {
    let dir = tempfile::TempDir::new().unwrap();
    let clone = {
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
        db.flush().unwrap();
        let (_h, tail) = db.test_page_live_list_anchors(clone).unwrap();
        assert_ne!(tail, crate::types::NULL_PAGE, "livelist chain empty after clone COW");
        // The tail segment must be a LiveListSegment.
        let page = db.test_read_page(tail).unwrap();
        assert_eq!(page.header().unwrap().page_type, PageType::LiveListSegment);
        clone
    };
    let db = Db::open(dir.path()).unwrap();
    // Anchors reloaded from the manifest.
    let (_h, tail) = db.test_page_live_list_anchors(clone).unwrap();
    assert_ne!(tail, crate::types::NULL_PAGE, "livelist tail lost across reopen");
    let live = db.test_clone_live_allocs(clone).unwrap();
    assert!(!live.is_empty(), "live-ALLOC set empty after reopen");
    drop(db);
    assert_livelist_clean(dir.path());
}

/// Make-or-break: across mixed clone churn (clone-of-clone, sibling clones,
/// overwrites, deletes), the live-ALLOC set equals the clone-private
/// reachable subtree for every clone — proven by the offline equality audit
/// (`verify --clone-livelist`). Any missed ALLOC/FREE capture site or a
/// double-record would surface as an `extra`/`missing` divergence here.
#[test]
fn livelist_equals_clone_private_reachable() {
    let dir = tempfile::TempDir::new().unwrap();
    {
        let db = Db::create_with_config({
            let mut cfg = crate::config::Config::new(dir.path());
            cfg.shards_per_partition = 1;
            cfg
        })
        .unwrap();
        let base = db.create_volume().unwrap();
        for i in 0u64..32 {
            db.insert(base, i, v(i as u8)).unwrap();
        }
        let snap = db.take_snapshot(base).unwrap();

        let a = db.clone_volume(snap).unwrap();
        let b = db.clone_volume(snap).unwrap();
        for i in 0u64..32 {
            db.insert(a, i, v(0xA0 | (i as u8 & 0xF))).unwrap();
        }
        for i in (0u64..32).step_by(2) {
            db.insert(b, i, v(0xB0 | (i as u8 & 0xF))).unwrap();
        }
        // clone-of-clone: snapshot A, clone it, diverge.
        let sa = db.take_snapshot(a).unwrap();
        let c = db.clone_volume(sa).unwrap();
        for i in 0u64..16 {
            db.insert(c, i, v(0xC0 | (i as u8 & 0xF))).unwrap();
        }
        // deletes on a clone (leaf-prune frees clone-private pages).
        for i in (0u64..32).step_by(3) {
            db.delete(b, i).unwrap();
        }
        db.flush().unwrap();
    }
    // Equality audit over every clone (a, b, c).
    assert_livelist_clean(dir.path());
}

/// A promoted ex-clone keeps recording (the capture threshold is sticky past
/// `PromotionComplete`, which only clears `parent_vol_ord`). The equality
/// audit still holds for it — the Phase-4 prerequisite "cover promoted
/// ex-clones".
#[test]
fn livelist_covers_promoted_ex_clone() {
    let dir = tempfile::TempDir::new().unwrap();
    let clone = {
        let db = Db::create(dir.path()).unwrap();
        let src = db.create_volume().unwrap();
        for i in 0u64..8 {
            db.insert(src, i, v(i as u8)).unwrap();
        }
        let snap = db.take_snapshot(src).unwrap();
        let clone = db.clone_volume(snap).unwrap();
        // Leaf-spaced LBAs (each `>= LEAF_ENTRY_COUNT` apart) so every write
        // COWs/allocates a distinct clone-private leaf rather than mutating
        // one shared leaf in place.
        for i in 0u64..8 {
            db.insert(clone, i * 256, v(0xC0 | i as u8)).unwrap();
        }
        // Promote to completion: clears parent_vol_ord, keeps the sticky
        // clone-lineage flag + capture threshold.
        for _ in 0..64 {
            match db.test_run_promotion_chunk(clone).unwrap() {
                PromotionStep::Completed | PromotionStep::NotApplicable => break,
                _ => {}
            }
        }
        let before = db.test_clone_live_allocs(clone).unwrap().len();
        assert!(before > 0, "pre-promotion writes did not populate the livelist");
        // Post-promotion writes must STILL record clone-private allocs (fresh
        // leaves at new leaf indices).
        for i in 0u64..8 {
            db.insert(clone, 4096 + i * 256, v(0xD0 | i as u8)).unwrap();
        }
        let after = db.test_clone_live_allocs(clone).unwrap().len();
        assert!(
            after > before,
            "promoted ex-clone stopped recording livelist (before={before} after={after})"
        );
        db.flush().unwrap();
        clone
    };
    // The promoted ex-clone (parent_vol_ord cleared, CLONE_LINEAGE flag kept)
    // is still audited and equal.
    let db = Db::open(dir.path()).unwrap();
    assert!(db.test_clone_live_allocs(clone).unwrap().len() >= 8);
    drop(db);
    assert_livelist_clean(dir.path());
}

/// A non-clone volume never accumulates livelist records and keeps NULL
/// anchors, even under heavy snapshot+overwrite churn (the page-deadlist
/// path, which a non-clone DOES exercise, must not bleed into the livelist).
#[test]
fn non_clone_volume_emits_no_livelist() {
    let (dir, db) = mk_db();
    let vol = db.create_volume().unwrap();
    for i in 0u64..64 {
        db.insert(vol, i, v(i as u8)).unwrap();
    }
    let _snap = db.take_snapshot(vol).unwrap();
    for i in 0u64..64 {
        db.insert(vol, i, v(0x80 | i as u8)).unwrap();
    }
    db.flush().unwrap();
    assert_eq!(db.test_page_live_list_len(vol), Some(0));
    assert_eq!(
        db.test_page_live_list_anchors(vol),
        Some((crate::types::NULL_PAGE, crate::types::NULL_PAGE))
    );
    drop(db);
    assert_livelist_clean(dir.path());
}

/// ZFS port Phase 3b condense: a multi-segment chain is rewritten to ONE
/// segment holding exactly `live_allocs(chain)`. The live-ALLOC set is
/// unchanged (condense makes no free decision) and the next flush links off
/// the condensed segment, staying verify-clean.
#[test]
fn livelist_condense_shrinks_chain_preserves_live_allocs() {
    let (dir, db, _faults, clone) = multi_segment_clone();
    let (head, tail) = db.test_page_live_list_anchors(clone).unwrap();
    assert_ne!(head, tail, "expected a multi-segment chain before condense");
    let before = db.test_clone_live_allocs(clone).unwrap();
    assert!(!before.is_empty());

    db.test_run_livelist_condense(2).unwrap();

    let (head2, tail2) = db.test_page_live_list_anchors(clone).unwrap();
    assert_eq!(head2, tail2, "condense did not collapse the chain to one segment");
    assert_ne!(
        tail2,
        crate::types::NULL_PAGE,
        "a non-empty live set must keep a single segment"
    );
    let after = db.test_clone_live_allocs(clone).unwrap();
    assert_eq!(before, after, "condense changed the live-ALLOC set");

    // The next flush appends off the condensed segment; audit stays clean.
    db.insert(clone, 0, v(0xEE)).unwrap();
    db.flush().unwrap();
    drop(db);
    assert_livelist_clean(dir.path());
}

/// Condense of a chain whose ALLOC/FREE all cancel collapses the anchors to
/// NULL. (`head == tail` holds regardless; the NULL assertion only fires when
/// the deletes actually emptied the clone-private set.)
#[test]
fn livelist_condense_empty_collapses_to_null() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = crate::config::Config::new(dir.path());
    cfg.shards_per_partition = 1;
    cfg.livelist_condense_min_segments = 0;
    let db = Db::create_with_config(cfg).unwrap();
    let base = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(base, i * 256, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(base).unwrap();
    let clone = db.clone_volume(snap).unwrap();
    for i in 0u64..8 {
        db.insert(clone, i * 256, v(0xC0 | i as u8)).unwrap();
    }
    db.flush().unwrap();
    for i in 0u64..8 {
        db.delete(clone, i * 256).unwrap();
    }
    db.flush().unwrap();

    let live = db.test_clone_live_allocs(clone).unwrap();
    db.test_run_livelist_condense(1).unwrap();
    let (head, tail) = db.test_page_live_list_anchors(clone).unwrap();
    assert_eq!(head, tail, "condense must collapse to at most one segment");
    if live.is_empty() {
        assert_eq!(
            (head, tail),
            (crate::types::NULL_PAGE, crate::types::NULL_PAGE),
            "an empty live-ALLOC set must collapse the chain to NULL anchors"
        );
    }
    drop(db);
    assert_livelist_clean(dir.path());
}

/// Crash between condense's new-segment sync and the re-anchor commit: the
/// chain is unchanged (abort before commit), the new segment is an orphan
/// reclaimed on the next open, and the equality audit stays clean.
#[test]
fn livelist_condense_fault_post_seg_write_is_recoverable() {
    let (dir, db, faults, clone) = multi_segment_clone();
    let before = db.test_clone_live_allocs(clone).unwrap();
    let anchors_before = db.test_page_live_list_anchors(clone).unwrap();

    faults.install(
        FaultPoint::LivelistCondensePostSegWriteBeforeManifest,
        1,
        FaultAction::Error,
    );
    // condense_scan swallows the per-volume error (background-worker
    // semantics); confirm the path actually hit the fault via `fired`.
    db.test_run_livelist_condense(2).unwrap();
    assert!(
        faults.fired(FaultPoint::LivelistCondensePostSegWriteBeforeManifest),
        "condense did not reach the post-seg-write fault"
    );
    faults.clear();

    // Aborted before commit → chain + live set unchanged.
    assert_eq!(db.test_page_live_list_anchors(clone).unwrap(), anchors_before);
    assert_eq!(db.test_clone_live_allocs(clone).unwrap(), before);
    drop(db);

    // Reopen reclaims the orphaned new segment; audit clean, set preserved.
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.test_clone_live_allocs(clone).unwrap(), before);
    drop(db);
    assert_livelist_clean(dir.path());
}

/// Crash between condense's re-anchor commit and the old-chain free: the chain
/// is already condensed (commit landed + atomics promoted), the OLD chain is
/// an orphan reclaimed on the next open, and the equality audit stays clean.
#[test]
fn livelist_condense_fault_post_manifest_is_recoverable() {
    let (dir, db, faults, clone) = multi_segment_clone();
    let before = db.test_clone_live_allocs(clone).unwrap();

    faults.install(
        FaultPoint::LivelistCondensePostManifestBeforeFree,
        1,
        FaultAction::Error,
    );
    db.test_run_livelist_condense(2).unwrap();
    assert!(
        faults.fired(FaultPoint::LivelistCondensePostManifestBeforeFree),
        "condense did not reach the post-manifest fault"
    );
    faults.clear();

    // Commit landed → chain re-anchored to one segment; old chain orphaned.
    let (head2, tail2) = db.test_page_live_list_anchors(clone).unwrap();
    assert_eq!(head2, tail2, "condense commit did not re-anchor to one segment");
    assert_eq!(db.test_clone_live_allocs(clone).unwrap(), before);
    drop(db);

    // Reopen reclaims the orphaned old chain; audit clean, set preserved.
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.test_clone_live_allocs(clone).unwrap(), before);
    drop(db);
    assert_livelist_clean(dir.path());
}

/// Part A: dropping a clone eagerly frees its page-livelist segment chain (the
/// page-rc decref cascade can't reach segment pages — rc 0). The volume's
/// anchors are gone and the store stays verify-clean.
#[test]
fn drop_volume_frees_livelist_chain() {
    let (dir, db, _faults, clone) = multi_segment_clone();
    let (_h, tail) = db.test_page_live_list_anchors(clone).unwrap();
    assert_ne!(tail, crate::types::NULL_PAGE, "clone has no livelist chain to free");
    db.drop_volume(clone).unwrap().expect("drop returns a report");
    // Volume gone → no anchors. The chain pages were eagerly freed; orphan
    // reclaim is only the crash backstop.
    assert_eq!(db.test_page_live_list_anchors(clone), None);
    db.flush().unwrap();
    drop(db);
    assert_livelist_clean(dir.path());
}

/// The public `promote_volume` drives the clone-promotion walker to
/// completion: a clone is promoted in one call (clearing `parent_vol_ord`), a
/// second call is a no-op, and a non-clone is never applicable.
#[test]
fn promote_volume_public_api_drives_to_completion() {
    let (_d, db) = mk_db();
    let src = db.create_volume().unwrap();
    db.insert(src, 0, v(1)).unwrap();
    db.insert(src, 256, v(2)).unwrap();
    let snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(snap).unwrap();
    assert!(
        db.promote_volume(clone).unwrap(),
        "an un-promoted clone should promote to independence"
    );
    assert!(
        !db.promote_volume(clone).unwrap(),
        "an already-promoted clone is NotApplicable"
    );
    assert!(
        !db.promote_volume(src).unwrap(),
        "a non-clone volume is never applicable"
    );
}

/// Combined deterministic churn — the in-process analogue of the
/// clone+promotion-churn soak, exercising the three Phase-3b additions
/// TOGETHER: clone / write / overwrite / flush (append livelist segments) /
/// synchronous CONDENSE / public `promote_volume` / clone DROP (Part A chain
/// free). Read-back integrity guards data; a false-premature clone-drop shadow
/// surfaces as `drop_volume → Err`; the final `verify --clone-livelist` proves
/// the live-ALLOC set stayed equal to the clone-private reachable subtree
/// across all of it (condense never drifted it, drop-free never corrupted).
/// A forever-base-snapshot is the only clone source (no snapshot DROPS) so the
/// run never trips the deferred Phase-2b snapshot-deadlist churn.
#[test]
fn clone_promote_condense_drop_churn() {
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    let dir = tempfile::TempDir::new().unwrap();
    {
        let mut cfg = crate::config::Config::new(dir.path());
        cfg.shards_per_partition = 1;
        cfg.livelist_condense_min_segments = 0; // drive condense synchronously
        let db = Db::create_with_config(cfg).unwrap();
        let mut rng = ChaCha8Rng::seed_from_u64(0x3B_C0DE_5EED);

        let base = db.create_volume().unwrap();
        for i in 0u64..48 {
            db.insert(base, i * 16, v(i as u8)).unwrap();
        }
        let base_snap = db.take_snapshot(base).unwrap();

        let mut clones: Vec<VolumeOrdinal> = Vec::new();
        for _ in 0..1200 {
            match rng.gen_range(0..100u32) {
                0..=19 if clones.len() < 8 => {
                    if let Ok(c) = db.clone_volume(base_snap) {
                        clones.push(c);
                    }
                }
                20..=49 if !clones.is_empty() => {
                    let c = clones[rng.gen_range(0..clones.len())];
                    let lba = rng.gen_range(0..48u64) * 16;
                    let val = v(rng.r#gen::<u8>());
                    db.insert(c, lba, val).unwrap();
                    assert_eq!(db.get(c, lba).unwrap(), Some(val), "clone read-back mismatch");
                }
                50..=59 => {
                    db.flush().unwrap();
                }
                60..=74 => {
                    db.test_run_livelist_condense(2).unwrap();
                }
                75..=86 if !clones.is_empty() => {
                    let c = clones[rng.gen_range(0..clones.len())];
                    let _ = db.promote_volume(c).unwrap();
                }
                _ if !clones.is_empty() => {
                    let idx = rng.gen_range(0..clones.len());
                    let c = clones[idx];
                    // Drops an un-promoted clone WITH the clone-drop shadow,
                    // or a promoted ex-clone without it; a false-premature
                    // would be Err here.
                    db.drop_volume(c)
                        .unwrap_or_else(|e| panic!("clone drop {c} false-premature / error: {e:?}"));
                    clones.remove(idx);
                }
                _ => {}
            }
        }
        db.flush().unwrap();
    }
    assert_livelist_clean(dir.path());
}

/// ZFS port Phase 4 Step 4 (S0) substrate: promoting a clone records every
/// PBA the promotion walker increfs into the per-volume promoted-PBA log; the
/// chain + anchors survive a reopen, and verify keeps the segment pages live
/// (no orphan-reclaim corruption). `drop_volume` (S0 actuator, next step) will
/// read this log to decref those PBAs survivor-gated, closing the promotion
/// over-pin leak.
#[test]
fn promoted_pba_log_records_promotion_and_survives_reopen() {
    let dir = tempfile::TempDir::new().unwrap();
    let clone_ord;
    let logged;
    {
        let db = Db::create(dir.path()).unwrap();
        let base = db.create_volume().unwrap();
        for i in 0u64..16 {
            db.insert(base, i, v(i as u8)).unwrap();
        }
        let snap = db.take_snapshot(base).unwrap();
        let clone = db.clone_volume(snap).unwrap();
        for i in 0u64..16 {
            db.insert(clone, i, v(0xC0 | i as u8)).unwrap();
        }
        // Before promotion the log is empty.
        assert_eq!(
            db.test_promoted_log_anchors(clone),
            Some((crate::types::NULL_PAGE, crate::types::NULL_PAGE)),
            "no promoted-PBA log before a promotion walker runs"
        );
        // Promote: the walker increfs + logs the head_pba of every live mapping.
        db.promote_volume(clone).unwrap();
        let pbas = db.test_promoted_log_pbas(clone).unwrap();
        assert_eq!(
            pbas.len(),
            16,
            "promotion must record one PBA per live clone mapping, got {}",
            pbas.len()
        );
        let (head, tail) = db.test_promoted_log_anchors(clone).unwrap();
        assert_ne!(head, crate::types::NULL_PAGE);
        assert_ne!(tail, crate::types::NULL_PAGE);
        db.flush().unwrap();
        clone_ord = clone;
        logged = pbas;
    }
    // Reopen: the durable chain survives + verify did not orphan-reclaim it.
    let db = Db::open(dir.path()).unwrap();
    let pbas = db.test_promoted_log_pbas(clone_ord).unwrap();
    assert_eq!(
        pbas, logged,
        "promoted-PBA log must survive reopen byte-for-byte"
    );
    // Offline verify (incl. orphan check) stays clean — the chain pages are
    // marked live by collect_live_pages.
    assert_livelist_clean(dir.path());
}
