//! ZFS port Phase 3b: per-clone page-livelist SUBSTRATE + POPULATION
//! (SHADOW). The clone's COW/alloc/free witness logs ALLOC/FREE `LiveRecord`s
//! for its clone-private L2P pages (`birth > branched_at_lsn`) into a
//! persistent `LiveListSegment` chain. Page-rc stays authoritative; these
//! tests prove the substrate is populated, survives reopen, costs nothing for
//! non-clones, covers promoted ex-clones, and — the make-or-break invariant —
//! its live-ALLOC set (ALLOC minus matched FREE) equals the clone-private
//! reachable subtree (`reachable(C) ∩ {birth > B}`), the equality Phase 4
//! relies on to free a dropped clone's private pages without page-rc.

use super::{mk_db, mk_db_with_shards, v};
use crate::Db;
use crate::db::promotion::PromotionStep;
use crate::page::PageType;
use crate::verify::{VerifyOptions, verify_path};

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
