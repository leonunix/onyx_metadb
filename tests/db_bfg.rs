//! BFG integration tests.
//!
//! Covers the Db-level wiring of the BFG state machine added in
//! spawn/stop with `bfg_threads_enabled`, `checkpoint_bfg`
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

fn cfg_threads_enabled(dir: &TempDir, bfg_timeout_ms: u64) -> Config {
    let mut cfg = Config::new(dir.path());
    cfg.bfg_threads_enabled = true;
    cfg.bfg_timeout_ms = bfg_timeout_ms;
    cfg
}

/// Threads on + buffer mode: this is the onyx production shape, where
/// the background sync drains only the frozen syncing slot per cycle
/// (`drain_syncing_slot_into_trees`, publish-before-clear).
fn cfg_threads_and_buffer(dir: &TempDir, bfg_timeout_ms: u64) -> Config {
    let mut cfg = cfg_threads_enabled(dir, bfg_timeout_ms);
    cfg.l2p_buffer_enabled = true;
    cfg
}

#[test]
fn open_close_with_bfg_threads_enabled_is_clean() {
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
fn checkpoint_bfg_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let after_first_flush: u64;
    {
        // BFG: `flush_with_gate` is now a
        // thin shell that drives `roll_to_quiescing` → `mark_synced`
        // on every successful flush, even with threads OFF. So
        // `checkpoint_bfg` advances per flush instead of staying at the
        // initial 0. The invariant is "advances monotonically and persists
        // across reopen".
        let db = Db::create(dir.path()).unwrap();
        let ord = db.create_volume().unwrap();
        db.insert(ord, 0, v(1)).unwrap();
        db.flush().unwrap();
        after_first_flush = db.manifest().checkpoint_bfg;
        assert!(
            after_first_flush >= 1,
            "first flush should advance checkpoint_bfg past the initial 0, got {after_first_flush}"
        );
    }
    let db = Db::open(dir.path()).unwrap();
    // checkpoint_bfg persisted across reopen.
    assert_eq!(db.manifest().checkpoint_bfg, after_first_flush);
    // BfgStateMachine resumes at open_bfg = checkpoint_bfg + 1, so a
    // fresh flush rolls forward one more BFG and increments the
    // persisted value by exactly 1.
    let ord = db.volumes().into_iter().find(|o| *o != 0).unwrap();
    db.insert(ord, 1, v(2)).unwrap();
    db.flush().unwrap();
    assert_eq!(db.manifest().checkpoint_bfg, after_first_flush + 1);
}

#[test]
fn quiesce_thread_advances_checkpoint_bfg_over_time() {
    // Threads enabled with a fast timer; the in-memory checkpoint_bfg
    // should advance even without manual triggers. `sync_work` is the
    // real `run_sync_cycle`, and with no dirty L2P
    // shards each cycle is a near-no-op sync — but checkpoint_bfg still
    // advances, confirming the quiesce → sync thread plumbing works
    // end-to-end.
    use onyx_metadb::Db;
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.bfg_threads_enabled = true;
    cfg.bfg_timeout_ms = 20; // fast for tests
    let db = Db::create_with_config(cfg).unwrap();
    // Wait up to 600 ms for at least 3 BFGs to cycle (3 × 20 ms = 60 ms
    // worst-case, plus scheduling jitter and the inflight-drain wait).
    let deadline = std::time::Instant::now() + Duration::from_millis(600);
    while std::time::Instant::now() < deadline {
        if db.bfg_checkpoint_for_test() >= 3 {
            break;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        db.bfg_checkpoint_for_test() >= 3,
        "expected checkpoint_bfg >= 3 in 600ms, got {}",
        db.bfg_checkpoint_for_test()
    );
}

#[test]
fn l2p_work_budget_rolls_bfg_before_long_timer() {
    let dir = TempDir::new().unwrap();
    let mut cfg = cfg_threads_and_buffer(&dir, 60_000);
    cfg.l2p_buffer_soft_entries = 4;
    let db = Db::create_with_config(cfg).unwrap();
    let ord = db.create_volume().unwrap();
    let initial = db.bfg_checkpoint_for_test();

    for lba in 0..4 {
        db.insert(ord, lba, v(lba as u8 + 1)).unwrap();
    }

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while std::time::Instant::now() < deadline && db.bfg_checkpoint_for_test() == initial {
        thread::sleep(Duration::from_millis(5));
    }
    assert!(
        db.bfg_checkpoint_for_test() > initial,
        "L2P work budget did not roll BFG before the 60s timer"
    );
    for lba in 0..4 {
        assert_eq!(db.get(ord, lba).unwrap(), Some(v(lba as u8 + 1)));
    }
}

#[test]
fn threads_on_buffer_per_slot_drain_read_your_writes_and_recovery() {
    // onyx production shape: threads on + buffer mode. The background
    // BfgSyncThread drains only the frozen syncing slot per cycle
    // (`drain_syncing_slot_into_trees`, publish-before-clear). Across
    // many writes spread over several BFG cycles, every value must stay
    // readable (read-your-writes through buffer → drain → tree), and a
    // flush()+reopen must recover them all (the in-order sync chain
    // started by flush drains every slot ≤ the forced BFG).
    let dir = TempDir::new().unwrap();
    const BATCHES: u64 = 6;
    const PER_BATCH: u64 = 50;
    let ord;
    {
        // Fast timer so the quiesce/sync threads roll repeatedly while
        // we write — exercising the per-slot drain against live inserts.
        let db = Db::create_with_config(cfg_threads_and_buffer(&dir, 30)).unwrap();
        ord = db.create_volume().unwrap();
        for b in 0..BATCHES {
            for i in 0..PER_BATCH {
                let lba = b * PER_BATCH + i;
                db.insert(ord, lba, v((lba % 250) as u8 + 1)).unwrap();
            }
            // Let the background threads roll + drain this batch's slot
            // before the next batch, so reads span buffered + folded.
            thread::sleep(Duration::from_millis(45));
        }
        // Read-your-writes: every LBA still resolves, whether its entry
        // is still in a buffer slot or already folded into the tree by a
        // background sync.
        for lba in 0..(BATCHES * PER_BATCH) {
            let got = db.get(ord, lba).unwrap();
            assert_eq!(
                got,
                Some(v((lba % 250) as u8 + 1)),
                "read-your-writes failed for lba {lba} before flush"
            );
        }
        // Forced flush drains every slot ≤ the forced BFG via the
        // in-order sync chain, making all writes durable before drop.
        db.flush().unwrap();
    }
    // Reopen: only durable (folded) state survives in standalone metadb
    // (the RAM buffer is gone). The flush above must have folded
    // everything, so all values recover.
    let db = Db::open_with_config(cfg_threads_and_buffer(&dir, 30)).unwrap();
    for lba in 0..(BATCHES * PER_BATCH) {
        let got = db.get(ord, lba).unwrap();
        assert_eq!(
            got,
            Some(v((lba % 250) as u8 + 1)),
            "value for lba {lba} lost across flush+reopen"
        );
    }
}

#[test]
fn threads_on_buffer_checkpoint_bfg_consistent_across_cycles() {
    // Guards the flush.rs `checkpoint_bfg = bfg` fix: under threads-on a
    // background roll can advance open_bfg past the syncing BFG while
    // run_sync_cycle_body runs, so persisting `open_bfg - 1` would drift
    // checkpoint_bfg ahead of the data this manifest committed. Across
    // several write+flush cycles + reopens, checkpoint_bfg must advance
    // monotonically, persist, and never desync from recoverable data.
    let dir = TempDir::new().unwrap();
    let ord;
    let mut last_cp: u64 = 0;
    {
        let db = Db::create_with_config(cfg_threads_and_buffer(&dir, 30)).unwrap();
        ord = db.create_volume().unwrap();
        for cycle in 0..4u64 {
            for i in 0..20u64 {
                let lba = cycle * 20 + i;
                db.insert(ord, lba, v((lba % 250) as u8 + 1)).unwrap();
            }
            db.flush().unwrap();
            let cp = db.manifest().checkpoint_bfg;
            assert!(
                cp >= last_cp,
                "checkpoint_bfg regressed: {cp} < {last_cp} (cycle {cycle})"
            );
            last_cp = cp;
        }
    }
    // Reopen resumes at the persisted checkpoint_bfg and all data is
    // present; a further flush advances checkpoint_bfg by exactly one
    // BFG from the resumed open (checkpoint_bfg + 1).
    let db = Db::open_with_config(cfg_threads_and_buffer(&dir, 30)).unwrap();
    assert_eq!(db.manifest().checkpoint_bfg, last_cp);
    for lba in 0..80u64 {
        assert_eq!(
            db.get(ord, lba).unwrap(),
            Some(v((lba % 250) as u8 + 1)),
            "value for lba {lba} lost across reopen"
        );
    }
}

/// Chunked syncing-slot fold (`l2p_drain_chunk_entries`): with a tiny
/// odd chunk budget the per-BFG drain re-acquires `tree.write()` many
/// times per slot, exercising chunk boundaries, the final-chunk
/// publish-before-clear, and interleaved lookups against the
/// partially-folded tree (the live slot must keep winning). Mixes
/// overwrites and tombstones so chunks carry insert runs and deletes.
#[test]
fn chunked_fold_tiny_chunks_read_your_writes_and_recovery() {
    let dir = TempDir::new().unwrap();
    const BATCHES: u64 = 6;
    const PER_BATCH: u64 = 50;
    let ord;
    {
        let mut cfg = cfg_threads_and_buffer(&dir, 30);
        cfg.l2p_drain_chunk_entries = 7;
        let db = Db::create_with_config(cfg).unwrap();
        ord = db.create_volume().unwrap();
        for b in 0..BATCHES {
            for i in 0..PER_BATCH {
                let lba = b * PER_BATCH + i;
                db.insert(ord, lba, v((lba % 250) as u8 + 1)).unwrap();
            }
            // Overwrite a stripe of the previous batch and delete every
            // 10th of it, so later slots fold updates + tombstones over
            // already-folded leaves.
            if b > 0 {
                for i in 0..PER_BATCH {
                    let lba = (b - 1) * PER_BATCH + i;
                    if i % 10 == 0 {
                        db.delete(ord, lba).unwrap();
                    } else {
                        db.insert(ord, lba, v((lba % 200) as u8 + 2)).unwrap();
                    }
                }
            }
            // Let the background threads roll + chunk-fold this batch's
            // slot while the next batch's inserts run against the lock.
            thread::sleep(Duration::from_millis(45));
        }
        for lba in 0..(BATCHES * PER_BATCH) {
            let got = db.get(ord, lba).unwrap();
            let expect = expected_chunked(lba, BATCHES, PER_BATCH);
            assert_eq!(got, expect, "read-your-writes failed for lba {lba}");
        }
        db.flush().unwrap();
    }
    let mut cfg = cfg_threads_and_buffer(&dir, 30);
    cfg.l2p_drain_chunk_entries = 7;
    let db = Db::open_with_config(cfg).unwrap();
    for lba in 0..(BATCHES * PER_BATCH) {
        let got = db.get(ord, lba).unwrap();
        let expect = expected_chunked(lba, BATCHES, PER_BATCH);
        assert_eq!(got, expect, "value for lba {lba} wrong across flush+reopen");
    }
}

fn expected_chunked(lba: u64, batches: u64, per_batch: u64) -> Option<L2pValue> {
    let b = lba / per_batch;
    let i = lba % per_batch;
    if b == batches - 1 {
        // Last batch never got the overwrite/delete pass.
        return Some(v((lba % 250) as u8 + 1));
    }
    if i % 10 == 0 {
        None
    } else {
        Some(v((lba % 200) as u8 + 2))
    }
}

/// `l2p_drain_chunk_entries = 0` keeps the legacy one-shot fold path
/// working (A/B fallback).
#[test]
fn chunk_entries_zero_one_shot_fold_still_works() {
    let dir = TempDir::new().unwrap();
    let ord;
    {
        let mut cfg = cfg_threads_and_buffer(&dir, 30);
        cfg.l2p_drain_chunk_entries = 0;
        let db = Db::create_with_config(cfg).unwrap();
        ord = db.create_volume().unwrap();
        for lba in 0..300u64 {
            db.insert(ord, lba, v((lba % 250) as u8 + 1)).unwrap();
        }
        thread::sleep(Duration::from_millis(90));
        for lba in 0..300u64 {
            assert_eq!(db.get(ord, lba).unwrap(), Some(v((lba % 250) as u8 + 1)));
        }
        db.flush().unwrap();
    }
    let mut cfg = cfg_threads_and_buffer(&dir, 30);
    cfg.l2p_drain_chunk_entries = 0;
    let db = Db::open_with_config(cfg).unwrap();
    for lba in 0..300u64 {
        assert_eq!(db.get(ord, lba).unwrap(), Some(v((lba % 250) as u8 + 1)));
    }
}
