//! Informational bench: foreground insert tail latency while the
//! background BFG syncing-slot fold runs, one-shot hold
//! (`l2p_drain_chunk_entries = 0`) vs bounded chunks (default 4096).
//!
//! The fold holds each shard's `tree.write()`; `Db::insert` commits
//! through direct apply which takes the same lock, so an unbounded
//! fold shows up directly as insert tail latency. A long
//! `bfg_timeout_ms` + few shards concentrates a large slot per shard,
//! approximating the sustained-load fold size seen on hardware. Run:
//!
//! ```text
//! cargo test --release --test fold_stall_bench -- --ignored --nocapture
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use onyx_metadb::{Config, Db, L2pValue};
use tempfile::TempDir;

fn v(n: u8) -> L2pValue {
    let mut x = [0u8; onyx_metadb::paged::LEAF_VALUE_SIZE];
    x[7] = n;
    x[onyx_metadb::paged::LEAF_VALUE_SIZE - 1] = 1;
    L2pValue(x)
}

#[derive(Default)]
struct Lat {
    max_us: AtomicU64,
    total_us: AtomicU64,
    over_1ms: AtomicU64,
    over_10ms: AtomicU64,
    over_100ms: AtomicU64,
    count: AtomicU64,
}

impl Lat {
    fn record(&self, us: u64) {
        self.max_us.fetch_max(us, Ordering::Relaxed);
        self.total_us.fetch_add(us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        if us >= 1_000 {
            self.over_1ms.fetch_add(1, Ordering::Relaxed);
        }
        if us >= 10_000 {
            self.over_10ms.fetch_add(1, Ordering::Relaxed);
        }
        if us >= 100_000 {
            self.over_100ms.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn run_arm(chunk_entries: usize, secs: u64, writers: usize) -> String {
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.bfg_threads_enabled = true;
    cfg.l2p_buffer_enabled = true;
    // Long BFG + few shards → each syncing slot carries a large
    // per-shard backlog, so the fold's lock hold is big enough to
    // measure (mirrors the sustained-load shape on hardware).
    cfg.bfg_timeout_ms = 3_000;
    cfg.shards_per_partition = 2;
    cfg.l2p_drain_chunk_entries = chunk_entries;
    let db = Db::create_with_config(cfg).unwrap();
    let ord = db.create_volume().unwrap();

    let stop = AtomicBool::new(false);
    let lat = Lat::default();
    thread::scope(|scope| {
        for w in 0..writers {
            let db = &db;
            let stop = &stop;
            let lat = &lat;
            scope.spawn(move || {
                // Disjoint dense lba stripes per writer → big leaf runs
                // in every slot, the worst case for the fold's hold.
                let mut lba = (w as u64) << 32;
                while !stop.load(Ordering::Relaxed) {
                    let t = Instant::now();
                    db.insert(ord, lba, v((lba % 250) as u8 + 1)).unwrap();
                    lat.record(t.elapsed().as_micros() as u64);
                    lba += 1;
                }
            });
        }
        thread::sleep(Duration::from_secs(secs));
        stop.store(true, Ordering::Relaxed);
    });
    let count = lat.count.load(Ordering::Relaxed).max(1);
    format!(
        "inserts={count:>9} avg={:>7.1}us max={:>8.1}ms >1ms={} >10ms={} >100ms={}",
        lat.total_us.load(Ordering::Relaxed) as f64 / count as f64,
        lat.max_us.load(Ordering::Relaxed) as f64 / 1000.0,
        lat.over_1ms.load(Ordering::Relaxed),
        lat.over_10ms.load(Ordering::Relaxed),
        lat.over_100ms.load(Ordering::Relaxed),
    )
}

#[test]
#[ignore] // informational; run with `cargo test --release -- --ignored --nocapture`
fn fold_stall_one_shot_vs_chunked() {
    const SECS: u64 = 20;
    const WRITERS: usize = 8;
    for (label, chunk) in [("one-shot (chunk=0)", 0usize), ("chunked (4096)", 4096)] {
        let r = run_arm(chunk, SECS, WRITERS);
        println!("{label:20} {r}");
    }
}
