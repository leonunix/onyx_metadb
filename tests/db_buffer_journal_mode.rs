//! Buffer-as-sole-journal Phase C: commit_ops smoke tests in Buffer mode.
//!
//! In Buffer mode the hot commit path skips `wal.submit` entirely.
//! These tests pin the contract from the metadb client side:
//!
//! - Commits return LSNs and apply in memory exactly like the WAL
//!   path (basic puts and reads).
//! - Multiple commits stack monotonically; `last_applied_lsn` advances
//!   on every commit_ops.
//! - The manifest's `last_processed_buffer_seq` only moves when the
//!   embedder publishes a watermark — Buffer mode doesn't auto-populate
//!   it (that's the embedder's responsibility).
//!
//! These tests do NOT exercise crash + buffer-replay; that requires
//! the onyx-side flusher and is covered in
//! `src/buffer/flush/tests/replay.rs`.

use onyx_metadb::paged::format::LEAF_VALUE_SIZE;
use onyx_metadb::{Config, Db, L2pValue, MetaDbJournalMode, Pba};
use tempfile::TempDir;

fn v(n: u8) -> L2pValue {
    let mut x = [0u8; onyx_metadb::paged::LEAF_VALUE_SIZE];
    x[7] = n;
    x[onyx_metadb::paged::LEAF_VALUE_SIZE - 1] = 1;
    L2pValue(x)
}

/// Build an L2pValue whose head 8B encode `pba` (big-endian, matches
/// the head_pba contract used by the promotion walker). Avoids the
/// `v(n)` helper's `(n as u64) << 56` PBA which would balloon the
/// refcount page-table.
fn lv(pba: Pba) -> L2pValue {
    let mut bytes = [0u8; LEAF_VALUE_SIZE];
    bytes[..8].copy_from_slice(&pba.to_be_bytes());
    bytes[LEAF_VALUE_SIZE - 1] = 1;
    L2pValue(bytes)
}

fn buffer_cfg(dir: &TempDir) -> Config {
    let mut cfg = Config::new(dir.path());
    cfg.journal_mode = MetaDbJournalMode::Buffer;
    cfg
}

#[test]
fn buffer_mode_commit_then_read_roundtrip() {
    let dir = TempDir::new().unwrap();
    let db = Db::create_with_config(buffer_cfg(&dir)).unwrap();
    let ord = db.create_volume().unwrap();
    db.insert(ord, 7, v(42)).unwrap();
    let got = db.get(ord, 7).unwrap();
    assert_eq!(got, Some(v(42)));
}

#[test]
fn buffer_mode_commits_advance_last_applied_lsn() {
    let dir = TempDir::new().unwrap();
    let db = Db::create_with_config(buffer_cfg(&dir)).unwrap();
    let ord = db.create_volume().unwrap();
    let before = db.last_applied_lsn();
    db.insert(ord, 100, v(1)).unwrap();
    let after_1 = db.last_applied_lsn();
    db.insert(ord, 101, v(2)).unwrap();
    let after_2 = db.last_applied_lsn();
    assert!(after_1 > before, "first insert must advance LSN");
    assert!(after_2 > after_1, "second insert must advance LSN further");
}

#[test]
fn buffer_mode_manifest_watermark_only_moves_when_published() {
    let dir = TempDir::new().unwrap();
    let db = Db::create_with_config(buffer_cfg(&dir)).unwrap();
    let ord = db.create_volume().unwrap();
    db.insert(ord, 0, v(1)).unwrap();
    db.flush().unwrap();
    assert_eq!(
        db.manifest().last_processed_buffer_seq,
        0,
        "Buffer mode without an explicit watermark publish must leave the manifest field at 0"
    );

    db.set_buffer_applied_watermark(42);
    db.insert(ord, 1, v(2)).unwrap();
    db.flush().unwrap();
    assert_eq!(db.manifest().last_processed_buffer_seq, 42);
}

#[test]
fn buffer_mode_data_plane_commits_do_not_grow_wal_records() {
    // Smoke check that data-plane commits in Buffer mode don't write
    // WAL records. Phase C.3 also moves lifecycle ops out of the WAL,
    // so after a fresh `create_volume` the WAL counter is still 0 —
    // no need to anchor on a post-setup baseline.
    let dir = TempDir::new().unwrap();
    let db = Db::create_with_config(buffer_cfg(&dir)).unwrap();
    let ord = db.create_volume().unwrap();
    assert_eq!(
        db.metrics_snapshot().wal_records,
        0,
        "Phase C.3: Buffer-mode create_volume must not write a WAL record"
    );

    for k in 0..32u64 {
        db.insert(ord, k, v(k as u8)).unwrap();
    }
    db.flush().unwrap();

    let after = db.metrics_snapshot().wal_records;
    assert_eq!(
        after, 0,
        "Buffer-mode data-plane commits must not grow wal_records (after: {after})"
    );
}

#[test]
fn buffer_mode_lifecycle_ops_grow_lifecycle_journal_only() {
    // Phase C.3: each lifecycle op should bump
    // `lifecycle_applied_watermark` by exactly one and leave
    // `wal_records` at zero. We exercise create + clone + snapshot +
    // drop_snapshot + drop_volume from a single Db instance so the
    // watermark deltas are observable per op.
    let dir = TempDir::new().unwrap();
    let db = Db::create_with_config(buffer_cfg(&dir)).unwrap();

    // create_volume #1
    let baseline_seq = db.lifecycle_applied_watermark();
    let v1 = db.create_volume().unwrap();
    let after_create = db.lifecycle_applied_watermark();
    assert_eq!(
        after_create,
        baseline_seq + 1,
        "create_volume must advance lifecycle seq by 1 \
         (was {baseline_seq}, now {after_create})"
    );
    assert_eq!(db.metrics_snapshot().wal_records, 0);

    // Insert something so take_snapshot has data to capture and so
    // drop_snapshot has a real cascade.
    db.insert(v1, 7, v(11)).unwrap();
    let snap_id = db.take_snapshot(v1).unwrap();
    // `take_snapshot` is manifest-only today (no WAL, no lifecycle
    // journal entry — see lifecycle_log/op.rs comment on TakeSnapshot).
    // Confirm the seq did not advance.
    assert_eq!(
        db.lifecycle_applied_watermark(),
        after_create,
        "take_snapshot is still manifest-only in Phase C.3"
    );

    // clone_volume from the live snapshot.
    let pre_clone = db.lifecycle_applied_watermark();
    let v2 = db.clone_volume(snap_id).unwrap();
    assert_eq!(db.lifecycle_applied_watermark(), pre_clone + 1);

    // drop_snapshot — `take_snapshot` was the v1 baseline so this
    // cascade has real page-decref work for apply to drive.
    let pre_drop_snap = db.lifecycle_applied_watermark();
    db.drop_snapshot(snap_id).unwrap();
    assert_eq!(db.lifecycle_applied_watermark(), pre_drop_snap + 1);

    // drop_volume on the clone (parent v1 still has a live PBA ref).
    let pre_drop_vol = db.lifecycle_applied_watermark();
    db.drop_volume(v2).unwrap();
    assert_eq!(db.lifecycle_applied_watermark(), pre_drop_vol + 1);

    assert_eq!(
        db.metrics_snapshot().wal_records,
        0,
        "no Buffer-mode lifecycle path should have touched the WAL"
    );
}

#[test]
fn buffer_mode_promotion_ops_grow_lifecycle_journal_only() {
    // Phase C.3: promotion records routed through `commit_ops` must
    // also land in the lifecycle journal in Buffer mode (not the WAL).
    // The clone scenario gives us a volume with `parent_vol_ord` set
    // so a synthetic `PromotionChunk` + `PromotionComplete` round-trip
    // through the apply path.
    let dir = TempDir::new().unwrap();
    let db = Db::create_with_config(buffer_cfg(&dir)).unwrap();
    let src = db.create_volume().unwrap();
    let pbas: [Pba; 4] = [301, 302, 303, 304];
    for (lba, pba) in pbas.iter().enumerate() {
        db.insert(src, lba as u64, lv(*pba)).unwrap();
    }
    let snap = db.take_snapshot(src).unwrap();
    let clone = db.clone_volume(snap).unwrap();

    let before = db.lifecycle_applied_watermark();
    let mut tx = db.begin();
    tx.promotion_chunk(clone, Box::new(pbas), None);
    tx.commit().unwrap();
    let after_chunk = db.lifecycle_applied_watermark();
    assert_eq!(
        after_chunk,
        before + 1,
        "PromotionChunk via commit_ops must append one lifecycle record \
         (before: {before}, after: {after_chunk})"
    );

    let mut tx = db.begin();
    tx.promotion_complete(clone);
    tx.commit().unwrap();
    let after_complete = db.lifecycle_applied_watermark();
    assert_eq!(
        after_complete,
        after_chunk + 1,
        "PromotionComplete via commit_ops must append one lifecycle record \
         (after chunk: {after_chunk}, after complete: {after_complete})"
    );

    assert_eq!(
        db.metrics_snapshot().wal_records,
        0,
        "promotion ops in Buffer mode must not touch the WAL"
    );
}

#[test]
fn buffer_mode_lifecycle_watermark_persists_through_flush() {
    // The next `flush` after each lifecycle op must copy
    // `lifecycle_applied_watermark` into `manifest.lifecycle_replay_seq`
    // so a re-open (Phase C.4) can decide which records are already
    // covered by the manifest checkpoint.
    let dir = TempDir::new().unwrap();
    let db = Db::create_with_config(buffer_cfg(&dir)).unwrap();
    let _ord = db.create_volume().unwrap();
    let live_watermark = db.lifecycle_applied_watermark();
    assert!(
        live_watermark > 0,
        "create_volume must have stamped the live watermark"
    );
    assert_eq!(
        db.manifest().lifecycle_replay_seq,
        0,
        "pre-flush manifest watermark is still 0"
    );
    db.flush().unwrap();
    assert_eq!(
        db.manifest().lifecycle_replay_seq,
        live_watermark,
        "flush must publish the live lifecycle watermark"
    );
}
