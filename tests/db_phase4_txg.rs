//! ZFS-TXG-clone Phase 4 integration tests.
//!
//! Covers the Db-level wiring of the TXG state machine added in
//! Phase 4: spawn/stop with `txg_threads_enabled`, `checkpoint_txg`
//! persistence across reopen, and v15 manifest rejection of v14.

use std::thread;
use std::time::Duration;

use onyx_metadb::{Config, Db, L2pValue};
use tempfile::TempDir;

fn v(n: u8) -> L2pValue {
    let mut x = [0u8; onyx_metadb::paged::LEAF_VALUE_SIZE];
    x[7] = n;
    x[onyx_metadb::paged::LEAF_VALUE_SIZE - 1] = 1;
    L2pValue(x)
}

fn cfg_threads_enabled(dir: &TempDir, txg_timeout_ms: u64) -> Config {
    let mut cfg = Config::new(dir.path());
    cfg.txg_threads_enabled = true;
    cfg.txg_timeout_ms = txg_timeout_ms;
    cfg
}

#[test]
fn open_close_with_txg_threads_enabled_is_clean() {
    let dir = TempDir::new().unwrap();
    let db = Db::create_with_config(cfg_threads_enabled(&dir, 5_000)).unwrap();
    // Make sure a basic operation still works with the threads spawned.
    let ord = db.create_volume().unwrap();
    db.insert(ord, 0, v(1)).unwrap();
    db.flush().unwrap();
    // Drop closes the threads (quiesce first, then sync).
    drop(db);
    // Reopen with threads on succeeds.
    let _db = Db::open_with_config(cfg_threads_enabled(&dir, 5_000)).unwrap();
}

#[test]
fn checkpoint_txg_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let after_first_flush: u64;
    {
        // ZFS-TXG-clone Phase 4 Step 8: `flush_with_gate` is now a
        // thin shell that drives `roll_to_quiescing` → `mark_synced`
        // on every successful flush, even with threads OFF. So
        // `checkpoint_txg` advances per flush instead of staying at
        // the initial 0. Pre-Step-8 the test asserted == 0; the
        // post-Step-8 invariant is "advances monotonically and
        // persists across reopen".
        let db = Db::create(dir.path()).unwrap();
        let ord = db.create_volume().unwrap();
        db.insert(ord, 0, v(1)).unwrap();
        db.flush().unwrap();
        after_first_flush = db.manifest().checkpoint_txg;
        assert!(
            after_first_flush >= 1,
            "first flush should advance checkpoint_txg past the initial 0, got {after_first_flush}"
        );
    }
    let db = Db::open(dir.path()).unwrap();
    // checkpoint_txg persisted across reopen.
    assert_eq!(db.manifest().checkpoint_txg, after_first_flush);
    // TxgStateMachine resumes at open_txg = checkpoint_txg + 1, so a
    // fresh flush rolls forward one more TXG and increments the
    // persisted value by exactly 1.
    let ord = db.volumes().into_iter().find(|o| *o != 0).unwrap();
    db.insert(ord, 1, v(2)).unwrap();
    db.flush().unwrap();
    assert_eq!(db.manifest().checkpoint_txg, after_first_flush + 1);
}

#[test]
fn quiesce_thread_advances_checkpoint_txg_over_time() {
    // Threads enabled with a fast timer; the in-memory checkpoint_txg
    // should advance even without manual triggers. The default sync_work
    // is a no-op (Step 7 wiring), so checkpoint_txg advances purely as
    // a state-machine artifact — but it does advance, and that confirms
    // the quiesce → sync thread plumbing works end-to-end.
    use onyx_metadb::Db;
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.txg_threads_enabled = true;
    cfg.txg_timeout_ms = 20; // fast for tests
    let db = Db::create_with_config(cfg).unwrap();
    // Wait up to 600 ms for at least 3 TXGs to cycle (3 × 20 ms = 60 ms
    // worst-case, plus scheduling jitter and the inflight-drain wait).
    let deadline = std::time::Instant::now() + Duration::from_millis(600);
    while std::time::Instant::now() < deadline {
        if db.txg_checkpoint_for_test() >= 3 {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        db.txg_checkpoint_for_test() >= 3,
        "expected checkpoint_txg >= 3 in 600ms, got {}",
        db.txg_checkpoint_for_test()
    );
}
