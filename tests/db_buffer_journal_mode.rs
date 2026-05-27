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

use onyx_metadb::{Config, Db, L2pValue, MetaDbJournalMode};
use tempfile::TempDir;

fn v(n: u8) -> L2pValue {
    let mut x = [0u8; onyx_metadb::paged::LEAF_VALUE_SIZE];
    x[7] = n;
    x[onyx_metadb::paged::LEAF_VALUE_SIZE - 1] = 1;
    L2pValue(x)
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
    // WAL records. Lifecycle ops (e.g. `create_volume`) still emit
    // WAL records until Phase C.3 routes them to the lifecycle
    // journal, so we capture the count AFTER setup and compare it to
    // the count after the data-plane inserts.
    let dir = TempDir::new().unwrap();
    let db = Db::create_with_config(buffer_cfg(&dir)).unwrap();
    let ord = db.create_volume().unwrap();
    let setup_wal_records = db.metrics_snapshot().wal_records;

    for k in 0..32u64 {
        db.insert(ord, k, v(k as u8)).unwrap();
    }
    db.flush().unwrap();

    let after = db.metrics_snapshot().wal_records;
    assert_eq!(
        after, setup_wal_records,
        "Buffer-mode data-plane commits must not grow wal_records \
         (setup: {setup_wal_records}, after: {after})"
    );
}
