//! [[no-refcount-hot-path-design]] Phase 4 Step 5 promotion walker
//! end-to-end tests.
//!
//! These exercise [`Db::run_promotion_chunk`] through the
//! `test_run_promotion_chunk` shim so we can step the walker
//! deterministically without racing a background thread.
//!
//! Contract under test:
//! - The walker bumps the global refcount of every PBA the clone's
//!   L2P references, by 1, exactly once.
//! - Completion clears both `parent_vol_ord` and `promotion_cursor`
//!   on the live volume and its in-memory manifest mirror.
//! - State persists across a crash-style close + reopen via WAL replay.
//! - A mapping the clone has already overwritten replaces the parent's
//!   PBA in the walker's view — so old parent-exclusive PBAs do not get
//!   incref'd when the clone's own write overwrote them before the
//!   walker reached that LBA.

use super::mk_db;
use crate::Db;
use crate::db::promotion::PromotionStep;
use crate::paged::L2pValue;
use crate::paged::format::LEAF_VALUE_SIZE;
use crate::types::{Pba, VolumeOrdinal};

/// Build an `L2pValue` with a small explicit PBA in the first 8 bytes
/// (big-endian, matching the `head_pba` contract). The shared `v(n)`
/// helper would put `n` into byte 0, yielding `(n as u64) << 56` —
/// fine for value-equality assertions but ruinous for the refcount
/// array which sizes its page-table to `pba / 336 + 1` and would
/// try to allocate hundreds of petabytes on resize. Tests that drive
/// the rc path must use `lv(pba)` instead.
fn lv(pba: Pba) -> L2pValue {
    let mut bytes = [0u8; LEAF_VALUE_SIZE];
    bytes[..8].copy_from_slice(&pba.to_be_bytes());
    // Same trick as `v(n)`: stamp a non-zero birth_lsn trailer so the
    // apply path's "stamp on 0 sentinel" doesn't perturb value bytes.
    bytes[LEAF_VALUE_SIZE - 1] = 1;
    L2pValue(bytes)
}

fn parent_of(db: &Db, vol_ord: VolumeOrdinal) -> Option<VolumeOrdinal> {
    db.manifest()
        .volumes
        .iter()
        .find(|e| e.ord == vol_ord)
        .expect("clone volume present in manifest")
        .parent_vol_ord
}

fn cursor_of(db: &Db, vol_ord: VolumeOrdinal) -> Option<u64> {
    db.manifest()
        .volumes
        .iter()
        .find(|e| e.ord == vol_ord)
        .expect("clone volume present in manifest")
        .promotion_cursor
}

#[test]
fn promotion_walker_completes_makes_clone_independent() {
    let (_d, db) = mk_db();
    let src = db.create_volume().unwrap();
    // Pick distinct, small PBAs so each LBA targets its own row in the
    // refcount table without ballooning the page-table.
    let pbas: [Pba; 8] = [101, 102, 103, 104, 105, 106, 107, 108];
    for (lba, pba) in pbas.iter().enumerate() {
        db.insert(src, lba as u64, lv(*pba)).unwrap();
    }

    let snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(snap).unwrap();
    assert_eq!(parent_of(&db, clone), Some(src));
    assert_eq!(cursor_of(&db, clone), None);
    for pba in pbas.iter() {
        assert_eq!(db.get_refcount(*pba).unwrap(), 0);
    }

    // A single walker step exhausts the clone's L2P (8 entries fits in
    // one chunk well under MAX_PROMOTION_CHUNK_PBAS). The walker emits
    // both PromotionChunk + the trailing PromotionComplete in that
    // call, so this returns `Completed` straight away.
    match db.test_run_promotion_chunk(clone).unwrap() {
        PromotionStep::Completed => {}
        other => panic!("expected Completed, got {other:?}"),
    }

    // Every PBA referenced by the clone got +1 from the walker.
    for pba in pbas.iter() {
        assert_eq!(db.get_refcount(*pba).unwrap(), 1);
    }
    // Lineage edge is cleared in both Volume RwLocks and the manifest
    // mirror (the apply_op wrapper updates both atomically).
    assert_eq!(parent_of(&db, clone), None);
    assert_eq!(cursor_of(&db, clone), None);

    // Re-invoking the walker on an already-promoted volume is a no-op:
    // parent_vol_ord is now None so the walker returns NotApplicable
    // without committing anything (no double-incref).
    match db.test_run_promotion_chunk(clone).unwrap() {
        PromotionStep::NotApplicable => {}
        other => panic!("expected NotApplicable after promotion, got {other:?}"),
    }
    for pba in pbas.iter() {
        assert_eq!(db.get_refcount(*pba).unwrap(), 1);
    }
}

#[test]
fn promotion_walker_resume_after_crash() {
    // Drive a partial walk by committing a manually-constructed
    // PromotionChunk with `next_cursor=Some(4)` (so apply leaves the
    // cursor mid-volume), close the Db without flushing, reopen — the
    // lifecycle journal replay restores `promotion_cursor=Some(4)` and
    // `parent_vol_ord=Some(src)` — then let the walker finish from the
    // resume point and assert every PBA got exactly one incref total.
    let dir = tempfile::TempDir::new().unwrap();
    let pbas: [Pba; 8] = [201, 202, 203, 204, 205, 206, 207, 208];
    let src;
    let clone;
    {
        let db = Db::create(dir.path()).unwrap();
        src = db.create_volume().unwrap();
        for (lba, pba) in pbas.iter().enumerate() {
            db.insert(src, lba as u64, lv(*pba)).unwrap();
        }
        let snap = db.take_snapshot(src).unwrap();
        clone = db.clone_volume(snap).unwrap();

        // Manually commit a partial chunk for the first 4 PBAs. The
        // walker would normally emit one chunk per call; driving the
        // lifecycle commit helper directly lets the test simulate
        // "crashed mid-walk" without requiring
        // MAX_PROMOTION_CHUNK_PBAS-many entries.
        let partial_pbas: Vec<Pba> = pbas[..4].to_vec();
        db.commit_promotion_chunk(clone, partial_pbas, Some(4))
            .unwrap();

        // First half of the PBAs is now incref'd to 1; the rest are
        // still at 0; cursor sits at 4 (mid-volume).
        for (i, pba) in pbas.iter().enumerate() {
            let want = if i < 4 { 1 } else { 0 };
            assert_eq!(db.get_refcount(*pba).unwrap(), want);
        }
        assert_eq!(cursor_of(&db, clone), Some(4));
        assert_eq!(parent_of(&db, clone), Some(src));
        // No flush — recovery must restore state from WAL replay.
    }

    let db = Db::open(dir.path()).unwrap();
    // Lifecycle journal replay restored the in-memory manifest mirror.
    assert_eq!(parent_of(&db, clone), Some(src));
    assert_eq!(cursor_of(&db, clone), Some(4));

    // Resume from cursor=4; this final call exhausts the remaining
    // mappings and emits the trailing PromotionComplete.
    match db.test_run_promotion_chunk(clone).unwrap() {
        PromotionStep::Completed => {}
        other => panic!("expected Completed on resume, got {other:?}"),
    }

    // Every PBA was incref'd exactly once across the pre-reopen
    // partial chunk + the post-reopen resume chunk.
    for pba in pbas.iter() {
        assert_eq!(db.get_refcount(*pba).unwrap(), 1);
    }
    assert_eq!(parent_of(&db, clone), None);
    assert_eq!(cursor_of(&db, clone), None);
}

#[test]
fn promotion_walker_skips_lbas_overwritten_by_clone() {
    // After cloning, the clone overwrites one LBA. The walker walks
    // the clone's L2P (not the parent's), so the overwritten LBA's
    // new PBA gets incref'd; the parent's old PBA at that LBA stays
    // at rc=0 — the walker never references it.
    let (_d, db) = mk_db();
    let src = db.create_volume().unwrap();
    let parent_pba_lba0: Pba = 301;
    let parent_pba_lba1: Pba = 302;
    db.insert(src, 0, lv(parent_pba_lba0)).unwrap();
    db.insert(src, 1, lv(parent_pba_lba1)).unwrap();
    let snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(snap).unwrap();

    // Clone diverges at LBA 0 — its L2P now points at a new PBA.
    // LBA 1 stays COW-shared with the parent.
    let clone_pba_lba0: Pba = 303;
    db.insert(clone, 0, lv(clone_pba_lba0)).unwrap();

    // Baseline: insert path (L2pPut) doesn't touch rc.
    assert_eq!(db.get_refcount(parent_pba_lba0).unwrap(), 0);
    assert_eq!(db.get_refcount(parent_pba_lba1).unwrap(), 0);
    assert_eq!(db.get_refcount(clone_pba_lba0).unwrap(), 0);

    match db.test_run_promotion_chunk(clone).unwrap() {
        PromotionStep::Completed => {}
        other => panic!("expected Completed, got {other:?}"),
    }

    // The walker saw the *clone's* L2P, so:
    // - clone's new PBA at LBA 0 got +1 (referenced by clone)
    // - shared PBA at LBA 1 got +1 (still referenced by clone)
    // - parent's old PBA at LBA 0 did NOT get bumped — the clone no
    //   longer references it, and the walker only walks the clone.
    assert_eq!(db.get_refcount(clone_pba_lba0).unwrap(), 1);
    assert_eq!(db.get_refcount(parent_pba_lba1).unwrap(), 1);
    assert_eq!(db.get_refcount(parent_pba_lba0).unwrap(), 0);

    // Lineage edge cleared.
    assert_eq!(parent_of(&db, clone), None);
}

#[test]
fn drop_parent_with_descendant_in_promotion_rejects() {
    // Phase 4 Step 6: while a clone still names `parent_vol_ord =
    // Some(parent)`, dropping the parent must be rejected. Otherwise
    // we'd free the COW-shared L2P pages the clone still relies on
    // for any LBA it hasn't yet diverged from.
    let (_d, db) = mk_db();
    let src = db.create_volume().unwrap();
    db.insert(src, 0, lv(401)).unwrap();
    let snap = db.take_snapshot(src).unwrap();
    // Holding the snapshot would already block drop_volume; tear it
    // down so this test exercises *only* the descendant gate.
    let clone = db.clone_volume(snap).unwrap();
    db.drop_snapshot(snap).unwrap().unwrap();
    assert_eq!(parent_of(&db, clone), Some(src));

    match db.drop_volume(src).unwrap_err() {
        crate::error::MetaDbError::InvalidArgument(msg) => {
            assert!(
                msg.contains("descendant clone") && msg.contains("pending promotion"),
                "unexpected reject reason: {msg}"
            );
        }
        e => panic!("expected InvalidArgument, got {e:?}"),
    }
    // Both volumes survive the rejected drop.
    let live: Vec<_> = db.volumes();
    assert!(live.contains(&src), "src still live");
    assert!(live.contains(&clone), "clone still live");
}

#[test]
fn drop_parent_after_promotion_complete_succeeds() {
    // Once the promotion walker has cleared `parent_vol_ord` on the
    // clone, the parent has no more L2P pages a descendant needs to
    // COW from. Dropping the parent must succeed.
    let (_d, db) = mk_db();
    let src = db.create_volume().unwrap();
    db.insert(src, 0, lv(501)).unwrap();
    db.insert(src, 1, lv(502)).unwrap();
    let snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(snap).unwrap();
    db.drop_snapshot(snap).unwrap().unwrap();
    assert_eq!(parent_of(&db, clone), Some(src));

    // Drive the walker to completion — clears clone.parent_vol_ord.
    loop {
        match db.test_run_promotion_chunk(clone).unwrap() {
            PromotionStep::Completed | PromotionStep::NotApplicable => break,
            PromotionStep::ChunkApplied { .. } => continue,
        }
    }
    assert_eq!(parent_of(&db, clone), None);

    // Now the parent is droppable. The clone keeps reading the shared
    // PBAs through the global rc bumps the walker installed.
    db.drop_volume(src).unwrap().unwrap();
    assert!(!db.volumes().contains(&src));
    assert!(db.volumes().contains(&clone));
    assert_eq!(db.get(clone, 0).unwrap(), Some(lv(501)));
    assert_eq!(db.get(clone, 1).unwrap(), Some(lv(502)));
}
