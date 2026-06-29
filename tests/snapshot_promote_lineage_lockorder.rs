//! Deadlock regression for the `take_snapshot` `drop_gate.write` fix's
//! lock-order hazard (found in /code-review of `5f5696e`).
//!
//! `take_snapshot` / `create_volume` hold `drop_gate.write()` across a forced
//! BFG sync whose `roll_to_quiescing` waits for `slots[cur].inflight == 0`.
//! Three commit paths — `commit_free_pbas` (autonomous LineageGcWorker),
//! `commit_promotion_chunk`, and `commit_promotion_complete` (both on the
//! `promote_volume` drive path) — used to acquire `bfg.enter()` (which bumps
//! inflight) BEFORE `drop_gate.read()`. So a commit that entered just before a
//! `take_snapshot` grabbed `drop_gate.write` would pin inflight while parked at
//! `drop_gate.read`, and the snapshot's roll could never drain → hard 3-way
//! deadlock. The fix swaps all three to `drop_gate.read()` BEFORE `bfg.enter()`
//! (matching `commit_ops`), so a parked commit holds no inflight.
//!
//! This test drives the exact concurrency the 8h dense soak missed (its
//! snapshot/promote ran on one serialized control thread, and the bg lineage
//! GC never overlapped a snapshot): a `take_snapshot` thread vs a
//! `clone+promote+drop` thread vs an autonomous LineageGcWorker (interval 5ms)
//! vs a vol0 writer feeding it dead pages. With the fix it completes quickly;
//! a regression to enter-before-read hangs, which the watchdog converts into a
//! loud abort rather than an infinite CI hang.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use onyx_metadb::{Config, Db, L2pValue};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tempfile::TempDir;

fn v(n: u8) -> L2pValue {
    let mut x = [0u8; onyx_metadb::paged::LEAF_VALUE_SIZE];
    x[7] = n;
    x[onyx_metadb::paged::LEAF_VALUE_SIZE - 1] = 1;
    L2pValue(x)
}

const SEED_KEYS: u64 = 512;

#[test]
fn snapshot_vs_promote_and_lineage_gc_no_deadlock() {
    // Generous wall-clock ceiling; with the fix the workload finishes in a few
    // seconds. A lock-order regression deadlocks → watchdog aborts loudly.
    const WATCHDOG_SECS: u64 = 60;
    const SNAP_ITERS: usize = 120;
    const PROMOTE_ITERS: usize = 60;

    let dir = TempDir::new().unwrap();

    let mut cfg = Config::new(dir.path());
    cfg.shards_per_partition = 2;
    // Autonomously drive `commit_free_pbas` (one of the inverted paths) every
    // few ms so it overlaps the snapshot/promote churn below.
    cfg.lineage_gc_enabled = true;
    cfg.lineage_gc_interval_ms = 5;
    let db = Db::create_with_config(cfg).unwrap();

    // Seed vol0 across leaves so overwrites COW (producing dead pages the
    // lineage GC reclaims via commit_free_pbas) and clones share real pages.
    for i in 0..SEED_KEYS {
        db.insert(0, i, v((i % 251) as u8)).unwrap();
    }
    db.flush().unwrap();

    let done = Arc::new(AtomicBool::new(false));

    // Watchdog: abort the whole test process if the workload hasn't finished
    // within the ceiling — a deadlock would otherwise hang forever.
    {
        let done = Arc::clone(&done);
        thread::spawn(move || {
            for _ in 0..(WATCHDOG_SECS * 10) {
                thread::sleep(Duration::from_millis(100));
                if done.load(Ordering::Relaxed) {
                    return;
                }
            }
            eprintln!(
                "DEADLOCK: snapshot_vs_promote_and_lineage_gc_no_deadlock did not finish \
                 in {WATCHDOG_SECS}s — likely a bfg.enter()-before-drop_gate.read() \
                 lock-order regression (commit_free_pbas / commit_promotion_chunk / \
                 commit_promotion_complete)."
            );
            std::process::abort();
        });
    }

    let stop_writer = Arc::new(AtomicBool::new(false));

    // Writer: overwrite vol0 to keep generating dead pages for the lineage GC.
    let writer = {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop_writer);
        thread::spawn(move || {
            let mut rng = ChaCha8Rng::seed_from_u64(0xDEAD_0001);
            while !stop.load(Ordering::Relaxed) {
                let key = rng.r#gen::<u64>() % SEED_KEYS;
                db.insert(0, key, v(rng.r#gen::<u8>())).unwrap();
            }
        })
    };

    // Snapshot thread: the force-sync / drop_gate.write counterparty.
    let snapshotter = {
        let db = Arc::clone(&db);
        thread::spawn(move || {
            let mut live = std::collections::VecDeque::new();
            for _ in 0..SNAP_ITERS {
                let s = db.take_snapshot(0).unwrap();
                live.push_back(s);
                if live.len() > 2 {
                    let old = live.pop_front().unwrap();
                    db.drop_snapshot(old).unwrap().expect("snapshot must drop");
                }
            }
            for s in live {
                db.drop_snapshot(s).unwrap().expect("drain snapshot");
            }
        })
    };

    // Promote thread: drives commit_promotion_chunk + commit_promotion_complete
    // concurrently with the snapshotter's forced syncs.
    let promoter = {
        let db = Arc::clone(&db);
        thread::spawn(move || {
            for round in 0..PROMOTE_ITERS {
                let s = db.take_snapshot(0).unwrap();
                let c = db.clone_volume(s).unwrap();
                // Give the clone private mappings so promotion has PBAs to
                // incref (drives commit_promotion_chunk, not just _complete).
                for i in 0..32u64 {
                    db.insert(c, i, v((round as u8).wrapping_add(i as u8))).unwrap();
                }
                db.promote_volume(c).unwrap();
                db.drop_volume(c).unwrap().expect("clone must drop");
                db.drop_snapshot(s).unwrap().expect("linking snapshot must drop");
            }
        })
    };

    snapshotter.join().unwrap();
    promoter.join().unwrap();
    stop_writer.store(true, Ordering::Relaxed);
    writer.join().unwrap();

    // Reached only if nothing deadlocked.
    done.store(true, Ordering::Relaxed);

    db.flush().unwrap();
    drop(db);
    Db::open(dir.path()).expect("reopen after snapshot/promote/lineage-GC churn");
}
