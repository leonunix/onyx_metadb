//! Buffer-as-sole-journal Phase B.2: watermark plumbing tests.
//!
//! The flusher publishes `last_processed_buffer_seq` (and the
//! lifecycle-log equivalent) via `Db::set_buffer_applied_watermark`.
//! The next `Db::flush` copies the watermark into the manifest before
//! committing. A reopen must surface the persisted value so recovery
//! can scope buffer replay correctly.

use onyx_metadb::{Config, Db, L2pValue, MetaDbJournalMode};
use tempfile::TempDir;

fn wal_cfg(dir: &TempDir) -> Config {
    let mut cfg = Config::new(dir.path());
    cfg.journal_mode = MetaDbJournalMode::Wal;
    cfg
}

fn v(n: u8) -> L2pValue {
    let mut x = [0u8; onyx_metadb::paged::LEAF_VALUE_SIZE];
    x[7] = n;
    x[onyx_metadb::paged::LEAF_VALUE_SIZE - 1] = 1;
    L2pValue(x)
}

#[test]
fn watermarks_default_to_zero_on_fresh_create() {
    // Phase D.4: this test pins the *Wal-mode* invariant that
    // neither watermark moves when the embedder runs against the
    // legacy WAL-authoritative path. The post-D.4 default is Buffer
    // mode, where `create_volume` legitimately bumps
    // `lifecycle_replay_seq`, so we construct the Db explicitly in
    // Wal mode here.
    let dir = TempDir::new().unwrap();
    let db = Db::create_with_config(wal_cfg(&dir)).unwrap();
    let ord = db.create_volume().unwrap();
    db.insert(ord, 0, v(1)).unwrap();
    db.flush().unwrap();

    let m = db.manifest();
    assert_eq!(
        m.last_processed_buffer_seq, 0,
        "WAL-authoritative path must leave last_processed_buffer_seq at 0"
    );
    assert_eq!(
        m.lifecycle_replay_seq, 0,
        "WAL-authoritative path must leave lifecycle_replay_seq at 0"
    );
}

#[test]
fn published_watermarks_persist_across_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        let ord = db.create_volume().unwrap();
        db.insert(ord, 0, v(1)).unwrap();
        db.set_buffer_applied_watermark(7654);
        db.set_lifecycle_applied_watermark(321);
        db.flush().unwrap();
        let m = db.manifest();
        assert_eq!(m.last_processed_buffer_seq, 7654);
        assert_eq!(m.lifecycle_replay_seq, 321);
    }
    let db = Db::open(dir.path()).unwrap();
    let m = db.manifest();
    assert_eq!(m.last_processed_buffer_seq, 7654);
    assert_eq!(m.lifecycle_replay_seq, 321);
}

#[test]
fn watermarks_advance_monotonically_under_fetch_max() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let prev = db.set_buffer_applied_watermark(100);
    assert_eq!(prev, 0);
    let prev = db.set_buffer_applied_watermark(250);
    assert_eq!(prev, 100);
    // Setting a lower value must not regress the watermark.
    let prev = db.set_buffer_applied_watermark(50);
    assert_eq!(prev, 250);
    assert_eq!(db.buffer_applied_watermark(), 250);
    // After flush, the manifest carries the highest published value.
    let ord = db.create_volume().unwrap();
    db.insert(ord, 0, v(1)).unwrap();
    db.flush().unwrap();
    assert_eq!(db.manifest().last_processed_buffer_seq, 250);
}
