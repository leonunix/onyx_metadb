//! Regression tests for the `take_snapshot` `drop_gate.write` fix: with NO
//! caller-side serialization, a writer commit that overlaps a mid-flight
//! `take_snapshot` (and the forced sync / reclaim it triggers) must NOT
//! premature-free a page the new snapshot / a clone still references.
//!
//! ## History
//! Before the fix, metadb's `take_snapshot` ran commit-concurrent (no
//! `drop_gate.write`, a Part-B latency choice), so a writer COULD overlap a
//! `take_snapshot` in flight and that cycle's reclaim freed a still-referenced
//! page → reopen / drop-shadow `Corruption("… got Free")`. The gap was
//! pre-existing (reproduced IDENTICALLY on pre-S3 `c6c3377` — the deleted
//! page-rc never guarded it, so it is NOT an S3 regression). onyx masked it in
//! production by serializing every flusher commit (`with_read_lock(volume)`)
//! against `create_snapshot`/`delete_snapshot` (`with_write_lock(volume)`) in
//! its `VolumeLifecycleManager`.
//!
//! ## What these tests now prove
//! `take_snapshot` holds `drop_gate.write()` for its capture/force-sync window
//! (symmetric with `drop_snapshot`), excluding concurrent commits. So metadb is
//! self-sufficiently safe under a truly-concurrent writer — these tests DROP
//! the caller-side contract lock that `concurrent_snapshot_clone_warm.rs` uses
//! and still stay sound. Run:
//!   cargo test --test concurrent_snapshot_clone_warm_unguarded -- --test-threads=1

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use onyx_metadb::{Config, Db, L2pValue, SnapshotId, VolumeOrdinal};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tempfile::TempDir;

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

const SEED_KEYS: u64 = 1024;

/// Unguarded overwriter: no contract lock, so commits overlap mid-flight
/// `take_snapshot` — the window onyx's per-volume RwLock excludes.
fn spawn_unguarded_overwriter(
    db: &Arc<Db>,
    stop: &Arc<AtomicBool>,
    vol: VolumeOrdinal,
    seed: u64,
) -> thread::JoinHandle<u64> {
    let db = Arc::clone(db);
    let stop = Arc::clone(stop);
    thread::spawn(move || {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let mut ops = 0u64;
        while !stop.load(Ordering::Relaxed) {
            let key = rng.r#gen::<u64>() % SEED_KEYS;
            db.insert(vol, key, v(rng.r#gen::<u8>())).unwrap();
            ops += 1;
            if ops % 256 == 0 {
                thread::yield_now();
            }
        }
        ops
    })
}

#[test]
fn unguarded_writer_vs_snapshot_flush_warm_stays_sound() {
    const ROUNDS: usize = 40;
    const LIVE_SNAP_CAP: usize = 2;

    let dir = TempDir::new().unwrap();
    let db = db_with_shards(&dir, 4);
    for i in 0..SEED_KEYS {
        db.insert(0, i, v((i % 251) as u8)).unwrap();
    }
    db.flush().unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let writer = spawn_unguarded_overwriter(&db, &stop, 0, 0xC0DE_0001);

    let mut live: VecDeque<SnapshotId> = VecDeque::new();
    for round in 0..ROUNDS {
        let snap = db.take_snapshot(0).unwrap();
        thread::yield_now();
        db.flush().unwrap();
        live.push_back(snap);
        for i in 0..32u64 {
            db.insert(0, (i * 7) % SEED_KEYS, v((round as u8).wrapping_add(i as u8)))
                .unwrap();
        }
        if live.len() > LIVE_SNAP_CAP {
            let old = live.pop_front().unwrap();
            db.drop_snapshot(old).unwrap().expect("snapshot must drop");
            db.flush().unwrap();
        }
    }

    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    for snap in live {
        db.drop_snapshot(snap).unwrap().expect("drain live snapshot");
    }
    db.flush().unwrap();
    drop(db);

    Db::open(dir.path()).expect("reopen after concurrent writer vs snapshot+flush churn");
}

#[test]
fn unguarded_clone_source_overwrite_stays_sound() {
    const ROUNDS: usize = 24;

    let dir = TempDir::new().unwrap();
    let db = db_with_shards(&dir, 4);
    for i in 0..SEED_KEYS {
        db.insert(0, i, v((i % 251) as u8)).unwrap();
    }
    db.flush().unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let src_writer = spawn_unguarded_overwriter(&db, &stop, 0, 0xC0DE_0002);

    for round in 0..ROUNDS {
        let s1 = db.take_snapshot(0).unwrap();
        db.flush().unwrap();
        let c1 = db.clone_volume(s1).unwrap();
        for i in 0..64u64 {
            db.insert(c1, i, v((round as u8).wrapping_add((i + 1) as u8)))
                .unwrap();
        }
        db.flush().unwrap();
        db.drop_snapshot(s1).unwrap().expect("drop linking snapshot");
        thread::yield_now();
        if round % 3 == 0 {
            db.promote_volume(c1).unwrap();
        }
        db.flush().unwrap();
        db.drop_volume(c1).unwrap().expect("clone volume must drop");
        db.flush().unwrap();
    }

    stop.store(true, Ordering::Relaxed);
    src_writer.join().unwrap();
    db.flush().unwrap();
    drop(db);

    Db::open(dir.path()).expect("reopen after concurrent clone-source overwrite churn");
}
