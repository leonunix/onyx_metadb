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

/// Threads on + buffer mode: this is the onyx production shape, where
/// the background sync drains only the frozen syncing slot per cycle
/// (`drain_syncing_slot_into_trees`, publish-before-clear).
fn cfg_threads_and_buffer(dir: &TempDir, txg_timeout_ms: u64) -> Config {
    let mut cfg = cfg_threads_enabled(dir, txg_timeout_ms);
    cfg.l2p_buffer_enabled = true;
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
    // should advance even without manual triggers. `sync_work` is the
    // real `run_sync_cycle` (Step 8 wiring), and with no dirty L2P
    // shards each cycle is a near-no-op sync — but checkpoint_txg still
    // advances, confirming the quiesce → sync thread plumbing works
    // end-to-end.
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

#[test]
fn threads_on_buffer_per_slot_drain_read_your_writes_and_recovery() {
    // onyx production shape: threads on + buffer mode. The background
    // TxgSyncThread drains only the frozen syncing slot per cycle
    // (`drain_syncing_slot_into_trees`, publish-before-clear). Across
    // many writes spread over several TXG cycles, every value must stay
    // readable (read-your-writes through buffer → drain → tree), and a
    // flush()+reopen must recover them all (the in-order sync chain
    // started by flush drains every slot ≤ the forced TXG).
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
        // Forced flush drains every slot ≤ the forced TXG via the
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
fn threads_on_buffer_checkpoint_txg_consistent_across_cycles() {
    // Guards the flush.rs `checkpoint_txg = txg` fix: under threads-on a
    // background roll can advance open_txg past the syncing TXG while
    // run_sync_cycle_body runs, so persisting `open_txg - 1` would drift
    // checkpoint_txg ahead of the data this manifest committed. Across
    // several write+flush cycles + reopens, checkpoint_txg must advance
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
            let cp = db.manifest().checkpoint_txg;
            assert!(
                cp >= last_cp,
                "checkpoint_txg regressed: {cp} < {last_cp} (cycle {cycle})"
            );
            last_cp = cp;
        }
    }
    // Reopen resumes at the persisted checkpoint_txg and all data is
    // present; a further flush advances checkpoint_txg by exactly one
    // TXG from the resumed open (checkpoint_txg + 1).
    let db = Db::open_with_config(cfg_threads_and_buffer(&dir, 30)).unwrap();
    assert_eq!(db.manifest().checkpoint_txg, last_cp);
    for lba in 0..80u64 {
        assert_eq!(
            db.get(ord, lba).unwrap(),
            Some(v((lba % 250) as u8 + 1)),
            "value for lba {lba} lost across reopen"
        );
    }
}

/// Chunked syncing-slot fold (`l2p_drain_chunk_entries`): with a tiny
/// odd chunk budget the per-TXG drain re-acquires `tree.write()` many
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
