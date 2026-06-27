//! ZFS port Phase 4 S3 — CONCURRENT coverage the phase-serialized soak misses,
//! driven under onyx's REAL concurrency contract.
//!
//! The `--onyx-concurrent-mix` soak PHASE-SERIALIZES its Phase-2 worker writes
//! against the Phase-3 snapshot/clone admin verbs, so it never interleaves a
//! commit (a `cow_for_write` against a freshly-snapshotted / clone-shared
//! volume) with the snapshot/clone lifecycle the way a live engine does. These
//! tests close that gap for the two paths S3 made load-bearing:
//!
//!  1. **flush snapshot-cache-warm** (the /code-review fix): the per-volume
//!     `SnapInfo` cache that feeds `snapshot_wms` is warmed by
//!     `finish_pending_snapshots`, which S3 moved INSIDE the `apply_gate.write`
//!     window so every commit after a snapshot finalize observes it.
//!  2. **interleaved clone-source overwrite** (M2: dropping the `effective_rc>1`
//!     COW-kill floor): a NON-clone volume that is a clone SOURCE keeps sharing
//!     L2P pages with its clone after the linking snapshot is dropped (promotion
//!     is lite). The replacement pin is `clone_cow_pinners`. A regression there
//!     premature-frees a page the clone still reads; `drop_volume` on the clone
//!     runs `check_clone_livelist_shadow` INLINE and returns `Err(Corruption)`.
//!
//! ## Concurrency contract (why the writer holds a read lock)
//!
//! metadb's `take_snapshot` is intentionally commit-concurrent (it takes NO
//! `drop_gate.write`), so at the metadb layer a writer thread CAN overlap a
//! `take_snapshot` mid-flight. onyx never drives that overlap: its
//! `VolumeLifecycleManager` makes every flusher metadb commit hold
//! `with_read_lock(volume)` while `create_snapshot` / `delete_snapshot` hold
//! `with_write_lock(volume)` — so a commit to V and a snapshot-admin op on V are
//! mutually exclusive (commits interleave BETWEEN admin ops, never DURING one).
//! These tests model that exact contract with a shared `RwLock`: the writer
//! takes the read side per op, each admin op takes the write side. That is the
//! realistic concurrency the S3 warm-fix + M2 pin must survive. The companion
//! `concurrent_snapshot_clone_warm_unguarded.rs` DROPS this caller-side lock to
//! prove metadb is now self-sufficiently safe (a true mid-`take_snapshot`
//! overlap stays sound because `take_snapshot` holds `drop_gate.write`).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use onyx_metadb::{Config, Db, L2pValue, SnapshotId, VerifyOptions, VolumeOrdinal};
use parking_lot::RwLock;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tempfile::TempDir;

/// Distinct value whose low base_pba byte carries `n` (mirrors the shared
/// `v()` helper in the other suites; keeps every value within u32 of each
/// other so the leaf codec stays dense).
fn v(n: u8) -> L2pValue {
    let mut x = [0u8; onyx_metadb::paged::LEAF_VALUE_SIZE];
    x[7] = n;
    x[onyx_metadb::paged::LEAF_VALUE_SIZE - 1] = 1;
    L2pValue(x)
}

fn db_with_shards(dir: &TempDir, shards: u32) -> Arc<Db> {
    let mut cfg = Config::new(dir.path());
    cfg.shards_per_partition = shards;
    Db::create_with_config(cfg).unwrap()
}

/// Full S3 HARD oracle set: `check_birth_shadow` also drives
/// `check_page_deadlist`; the two clone oracles cover the clone-lineage paths.
/// `strict:false` keeps the bounded benign orphan leak a warning (matches the
/// dead_list suite); a real premature free still trips `issues`.
fn verify_all_oracles(dir: &TempDir) {
    let report = onyx_metadb::verify_path(
        dir.path(),
        VerifyOptions {
            strict: false,
            check_birth_shadow: true,
            check_clone_livelist: true,
            check_clone_birth_shadow: true,
        },
    )
    .unwrap();
    assert!(
        report.is_clean(),
        "post-concurrency verify issues: {:?} (warnings: {:?})",
        report.issues,
        report.warnings,
    );
}

const SEED_KEYS: u64 = 1024;

/// The shared per-volume contract lock (models `VolumeLifecycleManager`): data
/// writes hold the read side, snapshot/clone/drop admin ops hold the write side.
type ContractLock = Arc<RwLock<()>>;

/// Run a snapshot/clone/drop admin op holding the write side of the contract
/// lock — the writer's commits cannot overlap it (they hold the read side).
fn admin<T>(lock: &ContractLock, f: impl FnOnce() -> T) -> T {
    let _g = lock.write();
    f()
}

/// Spawn a thread that hammers overwrites on `vol` until `stop` is set, holding
/// the read side of the contract lock per op so it can interleave BETWEEN admin
/// ops but never DURING one (onyx's flusher-commit-vs-snapshot serialization).
fn spawn_overwriter(
    db: &Arc<Db>,
    lock: &ContractLock,
    stop: &Arc<AtomicBool>,
    vol: VolumeOrdinal,
    seed: u64,
) -> thread::JoinHandle<u64> {
    let db = Arc::clone(db);
    let lock = Arc::clone(lock);
    let stop = Arc::clone(stop);
    thread::spawn(move || {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let mut ops = 0u64;
        while !stop.load(Ordering::Relaxed) {
            let key = rng.r#gen::<u64>() % SEED_KEYS;
            {
                let _g = lock.read();
                // Overwrites only: the key set is pre-seeded so every write is a
                // remap that must COW any page a live snapshot/clone still pins.
                db.insert(vol, key, v(rng.r#gen::<u8>())).unwrap();
            }
            ops += 1;
            if ops % 128 == 0 {
                thread::yield_now();
            }
        }
        ops
    })
}

/// Test 1 — writer COWs vol0 (read-locked) interleaved with main-thread
/// take_snapshot(vol0) + flush (the warm finalize) + bounded-window
/// drop_snapshot (the page-deadlist drop path), each under the write lock.
#[test]
fn concurrent_writer_vs_snapshot_flush_warm_stays_sound() {
    const ROUNDS: usize = 40;
    const LIVE_SNAP_CAP: usize = 2;

    let dir = TempDir::new().unwrap();
    {
        let db = db_with_shards(&dir, 4);
        let lock: ContractLock = Arc::new(RwLock::new(()));
        // Seed vol0 across many leaves, then flush so the writer's overwrites
        // are all remaps that COW against the snapshots taken below.
        for i in 0..SEED_KEYS {
            db.insert(0, i, v((i % 251) as u8)).unwrap();
        }
        db.flush().unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        let writer = spawn_overwriter(&db, &lock, &stop, 0, 0xC0DE_0001);

        let mut live: VecDeque<SnapshotId> = VecDeque::new();
        for round in 0..ROUNDS {
            // take_snapshot + its forced-sync warm finalize, holding the write
            // lock: no vol0 commit overlaps, exactly as onyx's create_snapshot.
            let snap = admin(&lock, || {
                let snap = db.take_snapshot(0).unwrap();
                db.flush().unwrap();
                snap
            });
            live.push_back(snap);

            // Release the lock between admin ops: the writer now COWs vol0
            // against the just-warmed snapshot root + adds a main-thread burst.
            thread::yield_now();
            for i in 0..32u64 {
                let _g = lock.read();
                db.insert(0, (i * 7) % SEED_KEYS, v((round as u8).wrapping_add(i as u8)))
                    .unwrap();
            }

            if live.len() > LIVE_SNAP_CAP {
                let old = live.pop_front().unwrap();
                admin(&lock, || {
                    db.drop_snapshot(old).unwrap().expect("snapshot must drop");
                    db.flush().unwrap();
                });
            }
        }

        stop.store(true, Ordering::Relaxed);
        let ops = writer.join().unwrap();
        assert!(ops > 0, "writer must have made progress");

        for snap in live {
            admin(&lock, || db.drop_snapshot(snap).unwrap().expect("drain live snapshot"));
        }
        db.flush().unwrap();
    }

    Db::open(dir.path()).expect("reopen after concurrent writer vs snapshot+flush churn");
    verify_all_oracles(&dir);
}

/// Test 2 — writer COWs the clone SOURCE (vol0, read-locked) interleaved with
/// clone_volume / drop_snapshot(linking) / promote / drop_volume churn (each
/// write-locked). The M2 clone-source pin (`clone_cow_pinners`) must hold across
/// the dropped linking snapshot while the source keeps being overwritten.
#[test]
fn concurrent_clone_source_overwrite_stays_sound() {
    const ROUNDS: usize = 24;

    let dir = TempDir::new().unwrap();
    {
        let db = db_with_shards(&dir, 4);
        let lock: ContractLock = Arc::new(RwLock::new(()));
        for i in 0..SEED_KEYS {
            db.insert(0, i, v((i % 251) as u8)).unwrap();
        }
        db.flush().unwrap();

        let stop = Arc::new(AtomicBool::new(false));
        // The source overwriter: vol0 is the clone SOURCE; these read-locked
        // writes are the M2 path that must COW against pages a live clone still
        // shares after the linking snapshot is dropped.
        let src_writer = spawn_overwriter(&db, &lock, &stop, 0, 0xC0DE_0002);

        for round in 0..ROUNDS {
            let c1: VolumeOrdinal = admin(&lock, || {
                let s1 = db.take_snapshot(0).unwrap();
                db.flush().unwrap();
                let c1 = db.clone_volume(s1).unwrap();
                // Churn the clone side so it owns private pages too (mixed shared
                // / private subtree — the clone-livelist shadow's exact input).
                for i in 0..64u64 {
                    db.insert(c1, i, v((round as u8).wrapping_add((i + 1) as u8)))
                        .unwrap();
                }
                db.flush().unwrap();
                // Drop the linking snapshot WHILE the clone lives — vol0 and c1
                // now share pages with no snapshot anchor; only
                // `clone_cow_pinners` pins them.
                db.drop_snapshot(s1).unwrap().expect("drop linking snapshot");
                c1
            });

            // Let the source writer COW vol0 while it still shares pages with c1.
            thread::yield_now();

            admin(&lock, || {
                // Promote on some rounds → clears parent_vol_ord (promoted-ex-
                // clone path the conservative all-clone pinner set must cover).
                if round % 3 == 0 {
                    db.promote_volume(c1).unwrap();
                }
                db.flush().unwrap();
                // drop_volume on a clone runs check_clone_livelist_shadow INLINE:
                // a premature free of a still-shared page returns Err(Corruption),
                // which unwrap() turns into a hard test failure right here.
                db.drop_volume(c1).unwrap().expect("clone volume must drop");
                db.flush().unwrap();
            });
        }

        stop.store(true, Ordering::Relaxed);
        let ops = src_writer.join().unwrap();
        assert!(ops > 0, "source writer must have made progress");
        db.flush().unwrap();
    }

    Db::open(dir.path()).expect("reopen after concurrent clone-source overwrite churn");
    verify_all_oracles(&dir);
}
