//! Buffer-as-sole-journal lifecycle replay: lifecycle journal replay tests.
//!
//! These tests crash + reopen a Buffer-mode database whose lifecycle
//! journal has uncovered records, and assert that the open path folds
//! each record back into the in-memory + manifest state before
//! returning. Together with `db_buffer_journal_mode.rs` (which only
//! exercises the live-append path) they pin down the C.4 contract: a
//! lifecycle op's effect survives a crash iff `journal.append`
//! returned successfully — no second fsync of the manifest is
//! required for durability.

use onyx_metadb::paged::format::LEAF_VALUE_SIZE;
use onyx_metadb::{Config, Db, L2pValue, MetaDbJournalMode, Pba};
use tempfile::TempDir;

fn buffer_cfg(dir: &TempDir) -> Config {
    let mut cfg = Config::new(dir.path());
    cfg.journal_mode = MetaDbJournalMode::Buffer;
    cfg
}

fn lv(pba: Pba) -> L2pValue {
    let mut bytes = [0u8; LEAF_VALUE_SIZE];
    bytes[..8].copy_from_slice(&pba.to_be_bytes());
    bytes[LEAF_VALUE_SIZE - 1] = 1;
    L2pValue(bytes)
}

/// `create_volume` is durable as soon as its lifecycle record is
/// fsync'd. Drop the Db without flushing the manifest, then reopen —
/// the volume must reappear (and remain operable).
#[test]
fn replay_recovers_create_volume_without_flush() {
    let dir = TempDir::new().unwrap();
    let watermark_after_create;
    let ord;
    {
        let db = Db::create_with_config(buffer_cfg(&dir)).unwrap();
        ord = db.create_volume().unwrap();
        watermark_after_create = db.lifecycle_applied_watermark();
        // Do NOT flush — the manifest is still pre-lifecycle.
        assert_eq!(db.manifest().lifecycle_replay_seq, 0);
        drop(db);
    }
    let db = Db::open_with_config(buffer_cfg(&dir)).unwrap();
    let vols = db.volumes();
    assert!(
        vols.contains(&ord),
        "volume {ord} should have been recovered via lifecycle replay; \
         volumes after replay: {vols:?}"
    );
    // The persisted watermark should match the live watermark from
    // before the crash — the post-replay manifest commit folded it
    // into `lifecycle_replay_seq`.
    let m = db.manifest();
    assert_eq!(m.lifecycle_replay_seq, watermark_after_create);
    assert_eq!(db.lifecycle_applied_watermark(), watermark_after_create);
    // The recovered volume is operable end-to-end.
    db.insert(ord, 7, lv(101)).unwrap();
    assert_eq!(db.get(ord, 7).unwrap(), Some(lv(101)));
}

/// `drop_volume`'s lifecycle record carries the frozen page list.
/// Crashing after the record is fsync'd but before the next manifest
/// commit must replay the cascade and leave the volume removed from
/// the manifest on reopen.
#[test]
fn replay_recovers_drop_volume_without_flush() {
    let dir = TempDir::new().unwrap();
    let watermark_after_drop;
    let surviving_ord;
    {
        let db = Db::create_with_config(buffer_cfg(&dir)).unwrap();
        let v1 = db.create_volume().unwrap();
        let v2 = db.create_volume().unwrap();
        surviving_ord = v1;
        for lba in 0..16u64 {
            db.insert(v2, lba, lv(2000 + lba)).unwrap();
        }
        db.flush().unwrap(); // baseline manifest with both volumes
        let _ = db.drop_volume(v2).unwrap();
        watermark_after_drop = db.lifecycle_applied_watermark();
        // Skip the second flush so the drop survives only via the
        // lifecycle journal.
        drop(db);
    }
    let db = Db::open_with_config(buffer_cfg(&dir)).unwrap();
    let vols = db.volumes();
    assert!(
        !vols.contains(&2),
        "dropped volume 2 should not reappear; volumes after replay: {vols:?}"
    );
    assert!(
        vols.contains(&surviving_ord),
        "surviving volume should still be present; volumes: {vols:?}"
    );
    let m = db.manifest();
    assert_eq!(m.lifecycle_replay_seq, watermark_after_drop);
    // Volume 2 is gone; rejecting future reads on it is the correct
    // surface behaviour.
    assert!(db.get(2, 0).is_err());
}

/// `clone_volume`'s lifecycle record inlines the source shard roots,
/// so replay can rebuild the clone even if the source snapshot is
/// dropped after the clone but before the manifest is committed.
#[test]
fn replay_recovers_clone_volume_without_flush() {
    let dir = TempDir::new().unwrap();
    let clone_ord;
    let post_clone_watermark;
    let src_ord;
    {
        let db = Db::create_with_config(buffer_cfg(&dir)).unwrap();
        src_ord = db.create_volume().unwrap();
        for lba in 0..8u64 {
            db.insert(src_ord, lba, lv(500 + lba)).unwrap();
        }
        db.flush().unwrap();
        let snap = db.take_snapshot(src_ord).unwrap();
        clone_ord = db.clone_volume(snap).unwrap();
        post_clone_watermark = db.lifecycle_applied_watermark();
        drop(db);
    }
    let db = Db::open_with_config(buffer_cfg(&dir)).unwrap();
    let vols = db.volumes();
    assert!(
        vols.contains(&clone_ord),
        "clone {clone_ord} should reappear; volumes: {vols:?}"
    );
    // The clone shares L2P with the source until promotion finishes —
    // reading from the clone should return the source's data.
    for lba in 0..8u64 {
        assert_eq!(db.get(clone_ord, lba).unwrap(), Some(lv(500 + lba)));
    }
    assert_eq!(db.manifest().lifecycle_replay_seq, post_clone_watermark);
}

/// `drop_snapshot`'s lifecycle record drives the page-decref cascade
/// + pba_decrefs list. Reopen must run the same cascade.
#[test]
fn replay_recovers_drop_snapshot_without_flush() {
    let dir = TempDir::new().unwrap();
    let post_drop_watermark;
    {
        let db = Db::create_with_config(buffer_cfg(&dir)).unwrap();
        let ord = db.create_volume().unwrap();
        for lba in 0..64u64 {
            db.insert(ord, lba, lv(900 + lba)).unwrap();
        }
        db.flush().unwrap();
        let snap = db.take_snapshot(ord).unwrap();
        // Mutate so drop_snapshot has work to do (shared pages to
        // decref + pba decrefs to apply).
        for lba in 0..16u64 {
            db.insert(ord, lba, lv(7000 + lba)).unwrap();
        }
        db.drop_snapshot(snap).unwrap();
        post_drop_watermark = db.lifecycle_applied_watermark();
        // No final flush — drop_snapshot survives only via the
        // lifecycle journal.
        drop(db);
    }
    let db = Db::open_with_config(buffer_cfg(&dir)).unwrap();
    let m = db.manifest();
    assert!(
        m.snapshots.is_empty(),
        "dropped snapshot should not reappear; manifest snapshots: {:?}",
        m.snapshots
    );
    assert_eq!(m.lifecycle_replay_seq, post_drop_watermark);
}

/// Replay must be idempotent under repeated reopen-without-flush
/// cycles: each open folds the same records in, but page-generation
/// guards keep the apply work bounded so the on-disk refcounts /
/// manifest converge.
#[test]
fn replay_is_idempotent_across_reopen_without_flush() {
    let dir = TempDir::new().unwrap();
    let ord;
    {
        let db = Db::create_with_config(buffer_cfg(&dir)).unwrap();
        ord = db.create_volume().unwrap();
        drop(db);
    }
    // First reopen: lifecycle record above the (zero) manifest
    // watermark must be folded in.
    {
        let db = Db::open_with_config(buffer_cfg(&dir)).unwrap();
        assert!(db.volumes().contains(&ord));
        // No flush — drop without committing the manifest.
        drop(db);
    }
    // Second reopen: the previous open's post-replay flush bumped
    // the manifest watermark, so this open should see no uncovered
    // records — the lifecycle ops are now "covered" by the durable
    // manifest from the first reopen.
    {
        let db = Db::open_with_config(buffer_cfg(&dir)).unwrap();
        assert!(db.volumes().contains(&ord));
        // The persisted watermark must reflect the first reopen's
        // folded-in records.
        let m = db.manifest();
        assert!(
            m.lifecycle_replay_seq > 0,
            "lifecycle_replay_seq should be advanced after the first reopen; got {}",
            m.lifecycle_replay_seq,
        );
    }
}

/// Replay should be a no-op when the manifest already covers every
/// record on disk (steady-state reopen): no extra watermark bump, no
/// shard regeneration. Pins the "common reopen" fast path.
#[test]
fn replay_is_noop_when_manifest_already_covers_journal() {
    let dir = TempDir::new().unwrap();
    let baseline_seq;
    {
        let db = Db::create_with_config(buffer_cfg(&dir)).unwrap();
        let _v1 = db.create_volume().unwrap();
        db.flush().unwrap();
        baseline_seq = db.manifest().lifecycle_replay_seq;
        drop(db);
    }
    let db = Db::open_with_config(buffer_cfg(&dir)).unwrap();
    assert_eq!(
        db.manifest().lifecycle_replay_seq,
        baseline_seq,
        "no uncovered records — open must not advance the watermark"
    );
    assert_eq!(
        db.lifecycle_applied_watermark(),
        baseline_seq,
        "live watermark must resume at the persisted value, not 0",
    );
}

/// `range_delete` in Buffer mode writes a `LifecycleOp::Discard`
/// record. Replay rescans the L2P range and re-runs the same
/// `apply_l2p_range_delete` path the live commit used — so a crash
/// after the Discard fsync but before the manifest commit must leave
/// the range empty on reopen.
#[test]
fn replay_recovers_range_delete_without_flush() {
    let dir = TempDir::new().unwrap();
    let post_discard_watermark;
    let ord;
    {
        let db = Db::create_with_config(buffer_cfg(&dir)).unwrap();
        ord = db.create_volume().unwrap();
        for lba in 0..64u64 {
            db.insert(ord, lba, lv(2000 + lba)).unwrap();
        }
        db.flush().unwrap(); // baseline with the live mappings
        let _ = db.range_delete(ord, 8, 40).unwrap();
        post_discard_watermark = db.lifecycle_applied_watermark();
        // Sanity: visible to live reads pre-crash.
        for lba in 8..40u64 {
            assert_eq!(db.get(ord, lba).unwrap(), None);
        }
        // Drop WITHOUT flushing — the Discard record is durable but
        // the manifest still lists the pre-discard L2P roots.
        drop(db);
    }
    let db = Db::open_with_config(buffer_cfg(&dir)).unwrap();
    // Range [8, 40) must be empty post-replay; the surrounding
    // ranges must be intact.
    for lba in 0..8u64 {
        assert_eq!(
            db.get(ord, lba).unwrap(),
            Some(lv(2000 + lba)),
            "lba {lba} (below discard range) must survive replay"
        );
    }
    for lba in 8..40u64 {
        assert_eq!(
            db.get(ord, lba).unwrap(),
            None,
            "lba {lba} (inside discard range) must stay deleted after replay"
        );
    }
    for lba in 40..64u64 {
        assert_eq!(
            db.get(ord, lba).unwrap(),
            Some(lv(2000 + lba)),
            "lba {lba} (above discard range) must survive replay"
        );
    }
    assert_eq!(db.manifest().lifecycle_replay_seq, post_discard_watermark);
}

/// Promotion records routed via `commit_ops` in Buffer mode also live
/// in the lifecycle journal; replay must restore the cursor advance.
#[test]
fn replay_recovers_promotion_chunk() {
    let dir = TempDir::new().unwrap();
    let clone;
    let post_promote_watermark;
    {
        let db = Db::create_with_config(buffer_cfg(&dir)).unwrap();
        let src = db.create_volume().unwrap();
        let pbas: [Pba; 4] = [701, 702, 703, 704];
        for (lba, pba) in pbas.iter().enumerate() {
            db.insert(src, lba as u64, lv(*pba)).unwrap();
        }
        db.flush().unwrap();
        let snap = db.take_snapshot(src).unwrap();
        clone = db.clone_volume(snap).unwrap();
        db.test_commit_promotion_chunk(clone, pbas.to_vec(), None)
            .unwrap();
        db.test_commit_promotion_complete(clone).unwrap();
        post_promote_watermark = db.lifecycle_applied_watermark();
        // Drop without flushing — promotion records survive only in
        // the lifecycle journal.
        drop(db);
    }
    let db = Db::open_with_config(buffer_cfg(&dir)).unwrap();
    assert_eq!(db.manifest().lifecycle_replay_seq, post_promote_watermark);
    let entry = db
        .manifest()
        .volumes
        .iter()
        .find(|v| v.ord == clone)
        .cloned()
        .expect("clone volume should reappear after replay");
    assert!(
        entry.parent_vol_ord.is_none(),
        "PromotionComplete should have cleared parent_vol_ord on \
         clone {clone}; entry: {entry:?}"
    );
    assert!(
        entry.promotion_cursor.is_none(),
        "PromotionComplete should have cleared promotion_cursor on \
         clone {clone}; entry: {entry:?}"
    );
}
