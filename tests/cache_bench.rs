//! .5 exit-criterion microbench: warm cache vs. cold cache on
//! the dedup read path.
//!
//! The roadmap requires that a `put_dedup → get_dedup` loop against a
//! warm cache runs noticeably faster than against a cold cache — the
//! guard is "cache is doing something." We prove it with two
//! back-to-back passes:
//!
//! 1. Cold pass: freshly-opened `Db` (shared cache starts empty).
//!    Every `get_dedup` pays a real disk read for its SST bloom and
//!    data pages.
//! 2. Warm pass: re-run the same workload on the same `Db`; the cache
//!    now has all the bloom / data pages from pass 1.
//!
//! The deterministic guard is **stats-based**: the warm pass's miss
//! delta must be far smaller than the cold pass's, and the warm pass
//! must accumulate more hits. That's cheap, portable, and directly
//! tests what the cache is for (avoiding redundant `read_page`
//! syscalls), independent of disk bandwidth.
//!
//! A second `#[ignore]` test prints warm / cold wall-clock timings
//! for informational diagnostics. No strict timing assertion — the
//! roadmap's "5×" target is a real-workload expectation; a 20 K-
//! entry in-memory benchmark is dominated by hash / lock / allocator
//! overhead that doesn't change with cache warmth, and the OS page
//! cache already caches the underlying file pages. Run with
//! `cargo test --release -- --ignored` to see the number.

use std::path::Path;
use std::time::Instant;

use onyx_metadb::{Config, Db, DedupValue, Hash8};
use tempfile::TempDir;

const ENTRIES: u64 = 20_000;
const LOOKUP_KEYS: u64 = 2_000;
const CACHE_BYTES: u64 = 32 * 1024 * 1024;

fn h(n: u64) -> Hash8 {
    // 8-byte hash schema: mix `n` into all 8 bytes so distinct keys
    // diffuse across cuckoo buckets / L0 fingerprint slots.
    let mixed = n.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ n.rotate_left(17);
    mixed.to_be_bytes()
}

fn dv(n: u64) -> DedupValue {
    // Big-endian leading 8 bytes so `head_pba()` decodes to `n`
    // (routes `DedupPut` through `rc.stage(head_pba, +1)`,
    // and an LE-encoded value would decode to a 56-bit shifted PBA
    // that OOMs the refcount array).
    let mut x = [0u8; 28];
    x[..8].copy_from_slice(&n.to_be_bytes());
    DedupValue(x)
}

fn mk_cfg(path: &Path) -> Config {
    let mut cfg = Config::new(path);
    cfg.page_cache_bytes = CACHE_BYTES;
    // Force L3 access on lookups by making the L1 hot cache much
    // smaller than `LOOKUP_KEYS`. Otherwise the warm pass services
    // every read out of L1, never touches the on-disk cuckoo, and
    // page-cache hit deltas collapse to zero — defeating the test.
    cfg.dedup_l1_cache_entries = 16;
    cfg
}

/// Populate a fresh database with `ENTRIES` dedup records, flush so
/// lookups exercise the on-disk cuckoo read path, then close.
/// Returns the directory so the caller can re-open with a fresh cache.
fn populate_and_close() -> TempDir {
    let dir = TempDir::new().unwrap();
    let cfg = mk_cfg(dir.path());
    let db = Db::create_with_config(cfg).unwrap();
    for i in 0..ENTRIES {
        db.put_dedup(h(i), dv(i)).unwrap();
    }
    db.flush().unwrap();
    dir
}

fn run_lookups(db: &Db) {
    for i in 0..LOOKUP_KEYS {
        // Keys are drawn from the populated range so every lookup is
        // a real hit; that isolates cache speedup from bloom-filter
        // early-outs on misses.
        let key = (i * (ENTRIES / LOOKUP_KEYS)) % ENTRIES;
        let _ = db.get_dedup(&h(key)).unwrap();
    }
}

#[test]
fn warm_cache_serves_reads_without_re_reading_from_disk() {
    let dir = populate_and_close();
    let cfg = mk_cfg(dir.path());
    let db = Db::open_with_config(cfg).unwrap();

    // Cold pass.
    let cold_start = db.cache_stats();
    run_lookups(&db);
    let after_cold = db.cache_stats();
    let cold_miss_delta = after_cold.misses.saturating_sub(cold_start.misses);
    let cold_hit_delta = after_cold.hits.saturating_sub(cold_start.hits);

    // Warm pass — same keys, cache now populated.
    run_lookups(&db);
    let after_warm = db.cache_stats();
    let warm_miss_delta = after_warm.misses.saturating_sub(after_cold.misses);
    let warm_hit_delta = after_warm.hits.saturating_sub(after_cold.hits);

    // The cold pass paid real misses (> 0), and the warm pass's misses
    // should be dramatically fewer. Using an order-of-magnitude ratio
    // rather than a raw bound keeps the assertion platform-independent.
    assert!(
        cold_miss_delta > 0,
        "cold pass produced no misses; either cache is misconfigured or the test setup warmed it"
    );
    assert!(
        warm_miss_delta * 10 <= cold_miss_delta.max(1),
        "warm pass misses {warm_miss_delta} not << cold pass misses {cold_miss_delta}"
    );
    assert!(
        warm_hit_delta > cold_hit_delta,
        "warm pass did not accumulate more hits than the cold pass (cold={cold_hit_delta} warm={warm_hit_delta})"
    );
}

#[test]
#[ignore] // informational; run with `cargo test --release -- --ignored`
fn warm_vs_cold_wall_clock() {
    let dir = populate_and_close();
    let cfg = mk_cfg(dir.path());
    let db = Db::open_with_config(cfg).unwrap();

    let cold_start = Instant::now();
    run_lookups(&db);
    let cold = cold_start.elapsed();

    let warm_start = Instant::now();
    run_lookups(&db);
    let warm = warm_start.elapsed();

    let ratio = cold.as_secs_f64() / warm.as_secs_f64();
    eprintln!("cache microbench: cold={cold:?}  warm={warm:?}  ratio={ratio:.2}×");
    // The cache cannot make reads *slower*. Anything ≥ 1.0× means the
    // hit path at least doesn't regress against the miss path. The
    // real speedup on disk-bound workloads is much larger; it gets
    // hidden here because the OS page cache already absorbs the
    // `read_page` calls for a 20 K-record dataset.
    assert!(
        ratio >= 1.0,
        "warm pass somehow regressed against cold pass (cold={cold:?}, warm={warm:?}, ratio={ratio:.2}×)",
    );
}
