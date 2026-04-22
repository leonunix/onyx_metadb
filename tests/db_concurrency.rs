use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use onyx_metadb::testing::faults::{FaultAction, FaultController, FaultPoint};
use onyx_metadb::{Config, Db, L2pValue, Result, SnapshotId};
use parking_lot::Mutex;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use tempfile::TempDir;
use xxhash_rust::xxh3::xxh3_64;

fn v(n: u8) -> L2pValue {
    let mut x = [0u8; 28];
    x[0] = n;
    L2pValue(x)
}

fn route_key(key: u64, shard_count: usize) -> usize {
    (xxh3_64(&key.to_be_bytes()) as usize) % shard_count
}

fn keys_for_shard(shard_id: usize, shard_count: usize, want: usize, start: u64) -> Vec<u64> {
    let mut out = Vec::with_capacity(want);
    let mut key = start;
    while out.len() < want {
        if route_key(key, shard_count) == shard_id {
            out.push(key);
        }
        key += shard_count as u64 + 1;
    }
    out
}

fn db_with_shards(dir: &TempDir, shards: u32) -> Db {
    let mut cfg = Config::new(dir.path());
    cfg.shards_per_partition = shards;
    Db::create_with_config(cfg).unwrap()
}

#[test]
fn multi_writer_stress_finishes_and_reopens_cleanly() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(db_with_shards(&dir, 4));

    let mut handles = Vec::new();
    for tid in 0..16u64 {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            let mut rng = ChaCha8Rng::seed_from_u64(0xD00D_0000 + tid);
            for _ in 0..2_000 {
                let key = rng.r#gen::<u64>() % 2_048;
                match rng.r#gen::<u8>() % 5 {
                    0..=2 => {
                        db.insert(0,key, v(rng.r#gen::<u8>())).unwrap();
                    }
                    3 => {
                        let _ = db.delete(0,key).unwrap();
                    }
                    _ => {
                        let _ = db.get(0,key).unwrap();
                    }
                }
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    db.flush().unwrap();
    let before_reopen: Vec<(u64, L2pValue)> =
        db.range(0,..).unwrap().collect::<Result<Vec<_>>>().unwrap();
    assert!(
        before_reopen.windows(2).all(|w| w[0].0 < w[1].0),
        "range scan must stay globally ordered",
    );

    drop(db);
    let reopened = Db::open(dir.path()).unwrap();
    let after_reopen: Vec<(u64, L2pValue)> = reopened
        .range(0, ..)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(after_reopen, before_reopen);
}

#[test]
fn snapshots_match_reference_during_multi_writer_rounds() {
    const SHARDS: usize = 4;
    const WRITERS: usize = 8;
    const ROUNDS: usize = 6;
    const OPS_PER_ROUND: usize = 200;

    let dir = TempDir::new().unwrap();
    let db = Arc::new(db_with_shards(&dir, SHARDS as u32));
    let model = Arc::new(Mutex::new(BTreeMap::<u64, L2pValue>::new()));
    let barrier = Arc::new(std::sync::Barrier::new(WRITERS + 1));

    let mut handles = Vec::new();
    for tid in 0..WRITERS {
        let db = Arc::clone(&db);
        let model = Arc::clone(&model);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            let shard_id = tid % SHARDS;
            let keys = keys_for_shard(shard_id, SHARDS, 128, (tid as u64 + 1) * 100_000);
            let mut rng = ChaCha8Rng::seed_from_u64(0xFACE_0000 + tid as u64);

            for _round in 0..ROUNDS {
                for _ in 0..OPS_PER_ROUND {
                    let key = keys[rng.r#gen::<usize>() % keys.len()];
                    if rng.r#gen::<u8>() % 4 == 0 {
                        let db_old = db.delete(0,key).unwrap();
                        let ref_old = model.lock().remove(&key);
                        assert_eq!(db_old, ref_old);
                    } else {
                        let value = v(rng.r#gen::<u8>());
                        let db_old = db.insert(0,key, value).unwrap();
                        let ref_old = model.lock().insert(key, value);
                        assert_eq!(db_old, ref_old);
                    }
                }
                barrier.wait();
                barrier.wait();
            }
        }));
    }

    let mut snapshots: Vec<(SnapshotId, BTreeMap<u64, L2pValue>)> = Vec::new();
    for _round in 0..ROUNDS {
        barrier.wait();
        let expected = model.lock().clone();
        let snap = db.take_snapshot().unwrap();
        snapshots.push((snap, expected));
        barrier.wait();
        thread::sleep(Duration::from_millis(5));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    for (snap, expected) in snapshots {
        let view = db.snapshot_view(snap).unwrap();
        let got: Vec<(u64, L2pValue)> =
            view.range(..).unwrap().collect::<Result<Vec<_>>>().unwrap();
        let want: Vec<(u64, L2pValue)> = expected.into_iter().collect();
        assert_eq!(got, want, "snapshot {snap} diverged from reference");
    }

    let current: Vec<(u64, L2pValue)> = db.range(0,..).unwrap().collect::<Result<Vec<_>>>().unwrap();
    let want_current: Vec<(u64, L2pValue)> = model.lock().iter().map(|(k, v)| (*k, *v)).collect();
    assert_eq!(current, want_current);
}

/// Phase 8b regression: `commit_ops` submits to the WAL *without* any
/// Db-level lock, so concurrent commits from N threads land in the same
/// group-commit batch at the WAL writer and amortise one fsync across
/// many records. Under the old `commit_lock` path every submit was
/// serialised before reaching the WAL, so the writer only ever saw one
/// record at a time and fsync count tracked record count 1:1.
///
/// We install `FaultPoint::WalFsyncBefore` with `fire_on_hit = u64::MAX`
/// so it never triggers but still counts hits via `hits(...)`. The
/// assertion requires fsyncs to be strictly less than commits by a
/// healthy margin — any regression that re-serialises WAL submit would
/// push the ratio back to 1.
#[test]
fn concurrent_commits_coalesce_into_wal_group_batches() {
    const WRITERS: usize = 8;
    const PER_THREAD: usize = 500;

    let dir = TempDir::new().unwrap();
    let faults = FaultController::new();
    faults.install(FaultPoint::WalFsyncBefore, u64::MAX, FaultAction::Error);

    let mut cfg = Config::new(dir.path());
    cfg.shards_per_partition = 4;
    let db = Arc::new(Db::create_with_config_and_faults(cfg, faults.clone()).unwrap());

    let barrier = Arc::new(std::sync::Barrier::new(WRITERS));
    let mut handles = Vec::new();
    for tid in 0..WRITERS {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let base = (tid as u64 + 1) * 1_000_000;
            for i in 0..PER_THREAD {
                db.insert(0,base + i as u64, v(tid as u8)).unwrap();
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }

    let fsyncs = faults.hits(FaultPoint::WalFsyncBefore);
    let commits = (WRITERS * PER_THREAD) as u64;
    // Roof at 80% so the test tolerates scheduling jitter on loaded CI
    // hosts but still catches any regression that puts WAL submit back
    // under a global lock (which would force 1:1).
    assert!(
        fsyncs * 5 < commits * 4,
        "expected WAL group-commit batching: {fsyncs} fsyncs for {commits} commits",
    );

    let applied = db.last_applied_lsn();
    assert_eq!(applied, commits, "LSN must match total committed records");
}
