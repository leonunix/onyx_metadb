//! Standalone metadb stress test that mirrors onyx's flush-writer +
//! AsyncCheckpoint workload. Lets you iterate on metadb perf without
//! restarting the whole onyx engine.
//!
//! Modeled after the production load captured at 2026-04-28T02:56Z
//! (post writer-priority apply_gate fix, dedup off):
//!
//! - 4 writer lanes, each running `tx.commit()` continuously
//! - ~500 `l2p_remap` ops per commit (matches onyx flush_writer batch
//!   size); each remap implicitly produces decref(old_pba) +
//!   incref(new_pba) on the apply path
//! - 1 background flusher firing `db.flush()` every 5 s (mirrors the
//!   onyx `DurabilityWatermarkHandle` + `AsyncCheckpoint` checkpoint cadence)
//! - 4 reader threads doing `db.multi_get()` of 8 LBAs at high QPS
//!   (covers epoch-pin / ReadView path)
//! - 1 range scanner periodically walking `Db::range()` (mirrors onyx GC
//!   blockmap scans and catches scan-vs-apply lock contention)
//!
//! Prints a metrics line every 5 s and a PASS/FAIL summary at the end.
//! Pass criteria are tunable via CLI flags; defaults reflect "metadb
//! must keep up with onyx's real load and not degrade over time."
//!
//! Usage:
//!     metadb-onyx-soak [--db PATH] [--writers 4] [--readers 4]
//!         [--ops-per-commit 500] [--flush-interval-ms 5000]
//!         [--duration-secs 300] [--warmup-secs 30]
//!         [--lba-space 16000000] [--reset]
//!         [--wal-lanes 1] [--group-commit-timeout-us 1]
//!         [--range-scanners 1] [--range-scan-interval-ms 5000]
//!         [--range-scan-lbas 0]
//!         [--dedup-shards 8]
//!         [--dedup-hit-pct 30] [--cleanup-batch 256]
//!         [--dedup-register-writers 0] [--dedup-register-batch 8192]
//!         [--target-install-max-ms 2000]
//!         [--target-commit-p99-ms 500]

use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

use onyx_metadb::testing::onyx_model::{onyx_dedup_value, onyx_hash, onyx_l2p_value};
use onyx_metadb::{
    ApplyOutcome, Config, Db, Hash32, MetaMetricsSnapshot, Pba, PendingState, VolumeOrdinal,
};

const PAGE_FILE: &str = "pages.onyx_meta";
const VOL: VolumeOrdinal = 0;

struct Args {
    db_path: PathBuf,
    writers: usize,
    readers: usize,
    ops_per_commit: usize,
    reader_batch: usize,
    flush_interval_ms: u64,
    duration_secs: u64,
    warmup_secs: u64,
    lba_space: u64,
    reset: bool,
    wal_lanes: u32,
    group_commit_timeout_us: u64,
    range_scanners: usize,
    range_scan_interval_ms: u64,
    range_scan_lbas: u64,
    dedup_shards: u32,
    dedup_enabled: bool,
    dedup_hit_pct: u8,
    cleanup_batch: usize,
    dedup_register_writers: usize,
    dedup_register_batch: usize,
    target_install_max_ms: u64,
    target_commit_p99_ms: u64,
    target_ops_per_sec: u64,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut a = Self {
            db_path: PathBuf::from("/tmp/metadb-onyx-soak"),
            writers: 4,
            readers: 4,
            ops_per_commit: 500,
            reader_batch: 8,
            flush_interval_ms: 5000,
            duration_secs: 300,
            warmup_secs: 30,
            lba_space: 16_000_000,
            reset: false,
            wal_lanes: 1,
            group_commit_timeout_us: 1,
            range_scanners: 1,
            range_scan_interval_ms: 5000,
            range_scan_lbas: 0,
            dedup_shards: 8,
            dedup_enabled: false,
            dedup_hit_pct: 0,
            cleanup_batch: 256,
            dedup_register_writers: 0,
            dedup_register_batch: 8192,
            target_install_max_ms: 2000,
            target_commit_p99_ms: 500,
            target_ops_per_sec: 0,
        };
        let mut it = env::args().skip(1);
        while let Some(arg) = it.next() {
            match arg.as_str() {
                "--db" => a.db_path = PathBuf::from(it.next().ok_or("--db needs value")?),
                "--writers" => a.writers = parse_usize(it.next(), "--writers")?,
                "--readers" => a.readers = parse_usize(it.next(), "--readers")?,
                "--ops-per-commit" => {
                    a.ops_per_commit = parse_usize(it.next(), "--ops-per-commit")?
                }
                "--reader-batch" => a.reader_batch = parse_usize(it.next(), "--reader-batch")?,
                "--flush-interval-ms" => {
                    a.flush_interval_ms = parse_u64(it.next(), "--flush-interval-ms")?
                }
                "--duration-secs" => a.duration_secs = parse_u64(it.next(), "--duration-secs")?,
                "--warmup-secs" => a.warmup_secs = parse_u64(it.next(), "--warmup-secs")?,
                "--lba-space" => a.lba_space = parse_u64(it.next(), "--lba-space")?,
                "--reset" => a.reset = true,
                "--wal-lanes" => {
                    let v = parse_u64(it.next(), "--wal-lanes")?;
                    if v == 0 || v > u32::MAX as u64 {
                        return Err("--wal-lanes must be 1..=u32::MAX".into());
                    }
                    a.wal_lanes = v as u32;
                }
                "--group-commit-timeout-us" => {
                    let v = parse_u64(it.next(), "--group-commit-timeout-us")?;
                    if v == 0 {
                        return Err("--group-commit-timeout-us must be > 0".into());
                    }
                    a.group_commit_timeout_us = v;
                }
                "--range-scanners" => {
                    a.range_scanners = parse_usize(it.next(), "--range-scanners")?
                }
                "--range-scan-interval-ms" => {
                    a.range_scan_interval_ms = parse_u64(it.next(), "--range-scan-interval-ms")?
                }
                "--range-scan-lbas" => {
                    a.range_scan_lbas = parse_u64(it.next(), "--range-scan-lbas")?
                }
                "--dedup-shards" => {
                    let v = parse_u64(it.next(), "--dedup-shards")?;
                    if v == 0 || v > 64 || !v.is_power_of_two() {
                        return Err("--dedup-shards must be a power of two in 1..=64".into());
                    }
                    a.dedup_shards = v as u32;
                }
                "--dedup-hit-pct" => {
                    let v = parse_u64(it.next(), "--dedup-hit-pct")?;
                    if v > 100 {
                        return Err("--dedup-hit-pct must be 0..=100".into());
                    }
                    a.dedup_enabled = true;
                    a.dedup_hit_pct = v as u8;
                }
                "--cleanup-batch" => a.cleanup_batch = parse_usize(it.next(), "--cleanup-batch")?,
                "--dedup-register-writers" => {
                    a.dedup_register_writers = parse_usize(it.next(), "--dedup-register-writers")?
                }
                "--dedup-register-batch" => {
                    a.dedup_register_batch = parse_usize(it.next(), "--dedup-register-batch")?;
                    if a.dedup_register_batch == 0 {
                        return Err("--dedup-register-batch must be > 0".into());
                    }
                }
                "--target-install-max-ms" => {
                    a.target_install_max_ms = parse_u64(it.next(), "--target-install-max-ms")?
                }
                "--target-commit-p99-ms" => {
                    a.target_commit_p99_ms = parse_u64(it.next(), "--target-commit-p99-ms")?
                }
                "--target-ops-per-sec" => {
                    a.target_ops_per_sec = parse_u64(it.next(), "--target-ops-per-sec")?
                }
                "-h" | "--help" => {
                    print_help();
                    return Err(String::new());
                }
                other => return Err(format!("unknown flag {other}")),
            }
        }
        Ok(a)
    }
}

fn parse_u64(v: Option<String>, name: &str) -> Result<u64, String> {
    v.ok_or_else(|| format!("{name} needs value"))?
        .parse()
        .map_err(|e| format!("{name}: {e}"))
}
fn parse_usize(v: Option<String>, name: &str) -> Result<usize, String> {
    Ok(parse_u64(v, name)? as usize)
}

fn print_help() {
    eprintln!(
        "metadb-onyx-soak: stress metadb with onyx-shape concurrent commits + periodic flush\n\
        \n\
        Defaults match the 2026-04-28 production trace shape after checkpoint\n\
        decoupling (4 writers, ~500 ops/commit, 5s checkpoint cadence,\n\
        16M LBA space, 1us WAL group wait) plus one periodic full-range\n\
        scan mirroring onyx GC blockmap scans.\n\
        \n\
        Pass criteria (override with --target-*): \n\
          install_max < 2000 ms, commit P99 < 500 ms, throughput steady (last\n\
          quartile within 80%% of middle quartile)."
    );
}

fn main() -> ExitCode {
    match run() {
        Ok(true) => {
            eprintln!("PASS");
            ExitCode::from(0)
        }
        Ok(false) => {
            eprintln!("FAIL");
            ExitCode::from(1)
        }
        Err(e) => {
            if !e.is_empty() {
                eprintln!("metadb-onyx-soak: {e}");
            }
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<bool, String> {
    let args = Args::parse()?;
    if args.reset && args.db_path.exists() {
        std::fs::remove_dir_all(&args.db_path).map_err(|e| format!("reset: {e}"))?;
    }
    std::fs::create_dir_all(&args.db_path).map_err(|e| format!("mkdir: {e}"))?;
    let mut cfg = Config::new(&args.db_path);
    cfg.shards_per_partition = 4;
    cfg.wal_lanes = args.wal_lanes;
    cfg.group_commit_timeout_us = args.group_commit_timeout_us;
    cfg.dedup_shards = args.dedup_shards;
    cfg.page_cache_bytes = 4 * 1024 * 1024 * 1024; // 4 GiB, matches onyx prod
    cfg.rebuild_free_list_on_open = false;
    cfg.reclaim_orphans_on_open = false;
    let db = Arc::new(if args.db_path.join(PAGE_FILE).exists() {
        Db::open_with_config(cfg).map_err(|e| format!("open: {e}"))?
    } else {
        Db::create_with_config(cfg).map_err(|e| format!("create: {e}"))?
    });

    let stop = Arc::new(AtomicBool::new(false));
    let writer_stats: Vec<Arc<WriterStats>> = (0..args.writers)
        .map(|_| Arc::new(WriterStats::new()))
        .collect();
    let reader_stats: Vec<Arc<ReaderStats>> = (0..args.readers)
        .map(|_| Arc::new(ReaderStats::new()))
        .collect();
    let dedup_register_stats: Vec<Arc<DedupRegisterStats>> = (0..args.dedup_register_writers)
        .map(|_| Arc::new(DedupRegisterStats::new()))
        .collect();
    let range_scan_stats = Arc::new(RangeScanStats::new());

    let writer_handles: Vec<_> = (0..args.writers)
        .map(|wid| {
            let db = db.clone();
            let stop = stop.clone();
            let stats = writer_stats[wid].clone();
            let cfg = WriterCfg {
                lba_space: args.lba_space,
                writers: args.writers as u64,
                ops_per_commit: args.ops_per_commit,
                dedup_enabled: args.dedup_enabled,
                dedup_hit_pct: args.dedup_hit_pct,
                cleanup_batch: args.cleanup_batch,
            };
            thread::Builder::new()
                .name(format!("writer-{wid}"))
                .spawn(move || writer_loop(wid, db, stop, stats, cfg))
                .unwrap()
        })
        .collect();

    let dedup_register_handles: Vec<_> = (0..args.dedup_register_writers)
        .map(|rid| {
            let db = db.clone();
            let stop = stop.clone();
            let stats = dedup_register_stats[rid].clone();
            let batch = args.dedup_register_batch;
            thread::Builder::new()
                .name(format!("dedup-register-{rid}"))
                .spawn(move || dedup_register_loop(rid, db, stop, stats, batch))
                .unwrap()
        })
        .collect();

    let reader_handles: Vec<_> = (0..args.readers)
        .map(|rid| {
            let db = db.clone();
            let stop = stop.clone();
            let stats = reader_stats[rid].clone();
            let lba_space = args.lba_space;
            let batch = args.reader_batch;
            thread::Builder::new()
                .name(format!("reader-{rid}"))
                .spawn(move || reader_loop(rid, db, stop, stats, lba_space, batch))
                .unwrap()
        })
        .collect();

    let range_scan_handles: Vec<_> = (0..args.range_scanners)
        .map(|sid| {
            let db = db.clone();
            let stop = stop.clone();
            let stats = range_scan_stats.clone();
            let lba_space = args.lba_space;
            let scan_lbas = args.range_scan_lbas;
            let interval = Duration::from_millis(args.range_scan_interval_ms);
            thread::Builder::new()
                .name(format!("range-scanner-{sid}"))
                .spawn(move || {
                    range_scan_loop(sid, db, stop, stats, lba_space, scan_lbas, interval)
                })
                .unwrap()
        })
        .collect();

    let flusher_stats = Arc::new(FlusherStats::new());
    let flusher_handle = {
        let db = db.clone();
        let stop = stop.clone();
        let stats = flusher_stats.clone();
        let interval = Duration::from_millis(args.flush_interval_ms);
        thread::Builder::new()
            .name("flusher".into())
            .spawn(move || flusher_loop(db, stop, stats, interval))
            .unwrap()
    };

    eprintln!(
        "metadb-onyx-soak: writers={} readers={} ops/commit={} flush={}ms duration={}s warmup={}s lba_space={} wal_lanes={} group_commit_timeout={}us range_scanners={} range_scan_interval={}ms range_scan_lbas={} dedup_shards={} dedup={} dedup_hit={}%% cleanup_batch={} dedup_register_writers={} dedup_register_batch={}",
        args.writers,
        args.readers,
        args.ops_per_commit,
        args.flush_interval_ms,
        args.duration_secs,
        args.warmup_secs,
        args.lba_space,
        args.wal_lanes,
        args.group_commit_timeout_us,
        args.range_scanners,
        args.range_scan_interval_ms,
        args.range_scan_lbas,
        args.dedup_shards,
        if args.dedup_enabled { "on" } else { "off" },
        args.dedup_hit_pct,
        args.cleanup_batch,
        args.dedup_register_writers,
        args.dedup_register_batch,
    );
    eprintln!(
        "targets: install_max < {} ms, commit P99 < {} ms{}",
        args.target_install_max_ms,
        args.target_commit_p99_ms,
        if args.target_ops_per_sec > 0 {
            format!(", avg_ops/s ≥ {}", args.target_ops_per_sec)
        } else {
            String::new()
        }
    );

    // Sample loop
    let started = Instant::now();
    let total = Duration::from_secs(args.duration_secs);
    let warmup = Duration::from_secs(args.warmup_secs);
    let mut last_sample = Sample::take(
        &db,
        &writer_stats,
        &reader_stats,
        &dedup_register_stats,
        &range_scan_stats,
        &flusher_stats,
    );
    let mut series: Vec<WindowStats> = Vec::new();
    while started.elapsed() < total {
        thread::sleep(Duration::from_secs(5));
        let now = Sample::take(
            &db,
            &writer_stats,
            &reader_stats,
            &dedup_register_stats,
            &range_scan_stats,
            &flusher_stats,
        );
        let w = WindowStats::between(&last_sample, &now);
        let elapsed = started.elapsed();
        let total_dedup = w.dedup_hits + w.dedup_misses;
        let hit_pct = if total_dedup == 0 {
            0.0
        } else {
            w.dedup_hits as f64 * 100.0 / total_dedup as f64
        };
        let read_items_per_sec = (w.reads as f64 * args.reader_batch as f64) / w.secs;
        eprintln!(
            "[t={:>5.1}s] commits={:>6} ({:>5.0}/s) ops={:>7} ({:>6.0}/s) bulk_dedup={:>7} ({:>4}c {:>7.0}/s p99={:>6.1}ms) commit_p50={:>6.1}ms p99={:>7.1}ms max={:>7.1}ms | commit wal={:>5.1}ms wait={:>5.1}ms gate={:>4.1}ms apply={:>5.1}ms | wal batches={} fsyncs={} batch_max={} write_avg={:>5.1}ms fsync_avg={:>5.1}ms submit_avg={:>5.1}ms | op_us l2p={:>4.1} rc={:>4.1} dedup={:>4.1} | flush={:>3} pages={} io_max={:>7}us manifest_max={:>7}us install_max={:>7}us reclaim_max={:>7}us total_max={:>7}us | l2p_buf={}/{} rc_buf={}/{} apply_q={:>3} cache={:>3}% | dedup hit={:>4.1}%% cleanup={}/{} scan={:>5.1}ms fwd={:>5.1}ms cmt={:>5.1}ms | reads={:>5} ({:>5.0}/s items={:>7.0}/s) read_p99={:>5.1}ms | range scans={} entries={} scan_p99={:>6.1}ms scan_max={:>6.1}ms",
            elapsed.as_secs_f64(),
            w.commits,
            w.commits as f64 / w.secs,
            w.ops,
            w.ops as f64 / w.secs,
            w.dedup_register_ops,
            w.dedup_register_commits,
            w.dedup_register_ops as f64 / w.secs,
            w.dedup_register_p99_ms,
            w.commit_p50_ms,
            w.commit_p99_ms,
            w.commit_max_ms,
            w.commit_wal_avg_ms,
            w.commit_apply_wait_avg_ms,
            w.commit_apply_gate_avg_ms,
            w.commit_apply_avg_ms,
            w.wal_batches,
            w.wal_fsyncs,
            w.wal_batch_records_max,
            w.wal_write_avg_ms,
            w.wal_fsync_avg_ms,
            w.wal_submit_avg_ms,
            w.apply_l2p_remap_avg_us,
            w.apply_refcount_avg_us,
            w.apply_dedup_avg_us,
            w.flushes,
            w.flush_pages,
            w.flush_io_max_us,
            w.flush_manifest_max_us,
            w.flush_install_max_us,
            w.flush_reclaim_max_us,
            w.flush_total_max_us,
            w.l2p_pagebuf_dirty,
            w.l2p_pagebuf_total,
            w.rc_pagebuf_dirty,
            w.rc_pagebuf_total,
            w.l2p_apply_q,
            w.cache_pct,
            hit_pct,
            w.cleanup_calls,
            w.cleanup_pbas,
            w.cleanup_scan_avg_ms,
            w.cleanup_forward_check_avg_ms,
            w.cleanup_commit_avg_ms,
            w.reads,
            w.reads as f64 / w.secs,
            read_items_per_sec,
            w.read_p99_ms,
            w.range_scans,
            w.range_entries,
            w.range_scan_p99_ms,
            w.range_scan_max_ms,
        );
        if elapsed >= warmup {
            series.push(w);
        }
        last_sample = now;
    }
    stop.store(true, Ordering::Relaxed);
    for h in writer_handles {
        let _ = h.join();
    }
    for h in reader_handles {
        let _ = h.join();
    }
    for h in dedup_register_handles {
        let _ = h.join();
    }
    for h in range_scan_handles {
        let _ = h.join();
    }
    let _ = flusher_handle.join();

    eprintln!("\n--- summary (post-warmup) ---");
    let pass = report(&series, &args);
    Ok(pass)
}

// ───────────────────── workload threads ─────────────────────

struct WriterStats {
    commits: AtomicU64,
    ops: AtomicU64,
    dedup_hits: AtomicU64,
    dedup_misses: AtomicU64,
    cleanup_calls: AtomicU64,
    cleanup_pbas: AtomicU64,
    /// Per-commit latency histogram (us) — Mutex<Vec<u64>>; flushed
    /// every sample.
    latencies: Mutex<Vec<u64>>,
}
impl WriterStats {
    fn new() -> Self {
        Self {
            commits: AtomicU64::new(0),
            ops: AtomicU64::new(0),
            dedup_hits: AtomicU64::new(0),
            dedup_misses: AtomicU64::new(0),
            cleanup_calls: AtomicU64::new(0),
            cleanup_pbas: AtomicU64::new(0),
            latencies: Mutex::new(Vec::with_capacity(1 << 14)),
        }
    }
}

#[derive(Clone, Copy)]
struct WriterCfg {
    lba_space: u64,
    writers: u64,
    ops_per_commit: usize,
    dedup_enabled: bool,
    dedup_hit_pct: u8,
    cleanup_batch: usize,
}

/// Tracks live (hash, pba) pairs the writer can reference for dedup-hit
/// commits. Bounded so memory doesn't grow without bound — eviction is
/// random-replacement once the cap is hit.
struct DedupPool {
    by_pba: HashMap<Pba, (Hash32, usize)>, // pba → (hash, index in pbas)
    pbas: Vec<Pba>,
    cap: usize,
}
impl DedupPool {
    fn new(cap: usize) -> Self {
        Self {
            by_pba: HashMap::with_capacity(cap),
            pbas: Vec::with_capacity(cap),
            cap,
        }
    }
    fn len(&self) -> usize {
        self.pbas.len()
    }
    fn insert(&mut self, hash: Hash32, pba: Pba, rng: &mut ChaCha8Rng) {
        if self.by_pba.contains_key(&pba) {
            return;
        }
        if self.pbas.len() >= self.cap {
            // Random eviction: swap the victim with the last entry, pop.
            let victim_idx = rng.gen_range(0..self.pbas.len());
            let victim_pba = self.pbas[victim_idx];
            self.by_pba.remove(&victim_pba);
            self.pbas.swap_remove(victim_idx);
            // The element previously at the end now occupies victim_idx;
            // patch its stored index.
            if victim_idx < self.pbas.len() {
                if let Some(slot) = self.by_pba.get_mut(&self.pbas[victim_idx]) {
                    slot.1 = victim_idx;
                }
            }
        }
        let idx = self.pbas.len();
        self.pbas.push(pba);
        self.by_pba.insert(pba, (hash, idx));
    }
    fn remove(&mut self, pba: Pba) -> Option<Hash32> {
        let (hash, idx) = self.by_pba.remove(&pba)?;
        self.pbas.swap_remove(idx);
        if idx < self.pbas.len() {
            if let Some(slot) = self.by_pba.get_mut(&self.pbas[idx]) {
                slot.1 = idx;
            }
        }
        Some(hash)
    }
    fn sample(&self, rng: &mut ChaCha8Rng) -> Option<(Hash32, Pba)> {
        if self.pbas.is_empty() {
            return None;
        }
        let pba = self.pbas[rng.gen_range(0..self.pbas.len())];
        Some((self.by_pba.get(&pba).unwrap().0, pba))
    }
}

struct ReaderStats {
    reads: AtomicU64,
    latencies: Mutex<Vec<u64>>,
}
impl ReaderStats {
    fn new() -> Self {
        Self {
            reads: AtomicU64::new(0),
            latencies: Mutex::new(Vec::with_capacity(1 << 14)),
        }
    }
}

struct DedupRegisterStats {
    commits: AtomicU64,
    ops: AtomicU64,
    latencies: Mutex<Vec<u64>>,
}
impl DedupRegisterStats {
    fn new() -> Self {
        Self {
            commits: AtomicU64::new(0),
            ops: AtomicU64::new(0),
            latencies: Mutex::new(Vec::with_capacity(1 << 14)),
        }
    }
}

struct RangeScanStats {
    scans: AtomicU64,
    entries: AtomicU64,
    latencies: Mutex<Vec<u64>>,
}
impl RangeScanStats {
    fn new() -> Self {
        Self {
            scans: AtomicU64::new(0),
            entries: AtomicU64::new(0),
            latencies: Mutex::new(Vec::with_capacity(1024)),
        }
    }
}

struct FlusherStats {
    flushes: AtomicU64,
}
impl FlusherStats {
    fn new() -> Self {
        Self {
            flushes: AtomicU64::new(0),
        }
    }
}

fn writer_loop(
    wid: usize,
    db: Arc<Db>,
    stop: Arc<AtomicBool>,
    stats: Arc<WriterStats>,
    cfg: WriterCfg,
) {
    let mut rng = ChaCha8Rng::seed_from_u64(0xA5A5_5A5A_C0FF_EE00 ^ (wid as u64));
    // Each writer owns a disjoint LBA slice (mimics zone routing —
    // distinct writers don't fight over the same LBA, but their LBAs
    // hash into all 4 metadb shards uniformly).
    let stripe = cfg.lba_space / cfg.writers;
    let lba_lo = stripe * (wid as u64);
    let lba_hi = lba_lo + stripe;
    // Per-writer monotonic PBA stream (top byte = writer id so PBAs
    // never collide across writers).
    let mut next_pba: u64 = ((wid as u64) << 56) | 1;
    let mut salt: u64 = (wid as u64) << 40;
    let mut hash_seq: u64 = (wid as u64) << 40;
    // Cap dedup pool at 4× cleanup_batch — large enough that we get
    // realistic dedup-hit selection without unbounded memory growth.
    let mut pool = DedupPool::new((cfg.cleanup_batch * 4).max(256));
    let mut pending_cleanup: Vec<Pba> = Vec::with_capacity(cfg.cleanup_batch * 2);

    while !stop.load(Ordering::Relaxed) {
        let mut tx = db.begin();
        // Track which (hash, pba) pairs were inserted this commit so we
        // can register them in the pool only after a successful commit.
        let mut staged_inserts: Vec<(Hash32, Pba)> = Vec::new();
        let mut commit_hits = 0u64;
        let mut commit_misses = 0u64;
        for _ in 0..cfg.ops_per_commit {
            let lba = lba_lo + rng.gen_range(0..(lba_hi - lba_lo));
            let try_hit = cfg.dedup_enabled
                && cfg.dedup_hit_pct > 0
                && pool.len() > 0
                && rng.gen_range(0..100u8) < cfg.dedup_hit_pct;
            if try_hit {
                let (_existing_hash, existing_pba) = pool.sample(&mut rng).unwrap();
                salt = salt.wrapping_add(1);
                tx.l2p_remap(
                    VOL,
                    lba,
                    onyx_l2p_value(existing_pba, salt),
                    Some((existing_pba, 1)),
                );
                commit_hits += 1;
            } else if cfg.dedup_enabled {
                let pba = next_pba;
                next_pba = next_pba.wrapping_add(1);
                salt = salt.wrapping_add(1);
                hash_seq = hash_seq.wrapping_add(1);
                let hash = onyx_hash(hash_seq);
                tx.l2p_remap(VOL, lba, onyx_l2p_value(pba, salt), None);
                tx.put_dedup(hash, onyx_dedup_value(pba, salt));
                tx.register_dedup_reverse(pba, hash);
                staged_inserts.push((hash, pba));
                commit_misses += 1;
            } else {
                let pba = next_pba;
                next_pba = next_pba.wrapping_add(1);
                salt = salt.wrapping_add(1);
                tx.l2p_remap(VOL, lba, onyx_l2p_value(pba, salt), None);
            }
        }
        let started = Instant::now();
        let outcomes = match tx.commit_with_outcomes() {
            Ok((_, outcomes)) => outcomes,
            Err(e) => {
                eprintln!("writer-{wid} commit error: {e}");
                return;
            }
        };
        let elapsed_us = started.elapsed().as_micros() as u64;
        stats.commits.fetch_add(1, Ordering::Relaxed);
        stats
            .ops
            .fetch_add(cfg.ops_per_commit as u64, Ordering::Relaxed);
        stats.dedup_hits.fetch_add(commit_hits, Ordering::Relaxed);
        stats
            .dedup_misses
            .fetch_add(commit_misses, Ordering::Relaxed);
        stats.latencies.lock().push(elapsed_us);

        // Promote miss-path inserts to the pool only if the apply
        // landed (every staged insert is unconditional, so no need to
        // check outcomes for these — but if the commit succeeded, they
        // are now live).
        if cfg.dedup_enabled {
            for (hash, pba) in staged_inserts {
                pool.insert(hash, pba, &mut rng);
            }
        }
        // Collect freed PBAs so we can run the dedup cleanup pipeline
        // in batches, mirroring onyx's writer flow.
        if cfg.dedup_enabled {
            for outcome in &outcomes {
                if let ApplyOutcome::L2pRemap {
                    freed_pba: Some(pba),
                    ..
                } = outcome
                {
                    if pool.remove(*pba).is_some() {
                        pending_cleanup.push(*pba);
                    }
                }
            }
        }
        if cfg.dedup_enabled && pending_cleanup.len() >= cfg.cleanup_batch {
            let drained: Vec<Pba> = pending_cleanup.drain(..).collect();
            let n = drained.len() as u64;
            if let Err(e) = db.cleanup_dedup_for_dead_pbas(&drained) {
                eprintln!("writer-{wid} cleanup error: {e}");
                return;
            }
            stats.cleanup_calls.fetch_add(1, Ordering::Relaxed);
            stats.cleanup_pbas.fetch_add(n, Ordering::Relaxed);
        }
    }

    // Final cleanup drain on shutdown.
    if cfg.dedup_enabled && !pending_cleanup.is_empty() {
        let n = pending_cleanup.len() as u64;
        let _ = db.cleanup_dedup_for_dead_pbas(&pending_cleanup);
        stats.cleanup_calls.fetch_add(1, Ordering::Relaxed);
        stats.cleanup_pbas.fetch_add(n, Ordering::Relaxed);
    }
}

fn reader_loop(
    rid: usize,
    db: Arc<Db>,
    stop: Arc<AtomicBool>,
    stats: Arc<ReaderStats>,
    lba_space: u64,
    batch: usize,
) {
    let mut rng = ChaCha8Rng::seed_from_u64(0xDEAD_BEEF_BADD_CAFE ^ (rid as u64));
    let mut lbas = vec![0u64; batch];
    while !stop.load(Ordering::Relaxed) {
        for slot in lbas.iter_mut() {
            *slot = rng.gen_range(0..lba_space);
        }
        let started = Instant::now();
        match db.multi_get(VOL, &lbas) {
            Ok(_) => {
                let elapsed_us = started.elapsed().as_micros() as u64;
                stats.reads.fetch_add(1, Ordering::Relaxed);
                stats.latencies.lock().push(elapsed_us);
            }
            Err(e) => {
                eprintln!("reader-{rid} multi_get error: {e}");
                return;
            }
        }
    }
}

fn dedup_register_loop(
    rid: usize,
    db: Arc<Db>,
    stop: Arc<AtomicBool>,
    stats: Arc<DedupRegisterStats>,
    batch: usize,
) {
    let mut hash_seq: u64 = (rid as u64) << 48;
    let mut pba: u64 = ((rid as u64) << 56) | 0x0080_0000_0000;
    let mut salt: u64 = (rid as u64) << 40;
    while !stop.load(Ordering::Relaxed) {
        let mut tx = db.begin();
        for _ in 0..batch {
            hash_seq = hash_seq.wrapping_add(1);
            pba = pba.wrapping_add(1);
            salt = salt.wrapping_add(1);
            let hash = onyx_hash(hash_seq);
            tx.put_dedup(hash, onyx_dedup_value(pba, salt));
            tx.register_dedup_reverse(pba, hash);
        }
        let started = Instant::now();
        if let Err(e) = tx.commit() {
            eprintln!("dedup-register-{rid} commit error: {e}");
            return;
        }
        let elapsed_us = started.elapsed().as_micros() as u64;
        stats.commits.fetch_add(1, Ordering::Relaxed);
        stats.ops.fetch_add(batch as u64, Ordering::Relaxed);
        stats.latencies.lock().push(elapsed_us);
    }
}

fn flusher_loop(db: Arc<Db>, stop: Arc<AtomicBool>, stats: Arc<FlusherStats>, interval: Duration) {
    while !stop.load(Ordering::Relaxed) {
        thread::sleep(interval);
        if let Err(e) = db.flush() {
            eprintln!("flusher error: {e}");
            return;
        }
        stats.flushes.fetch_add(1, Ordering::Relaxed);
    }
}

fn range_scan_loop(
    sid: usize,
    db: Arc<Db>,
    stop: Arc<AtomicBool>,
    stats: Arc<RangeScanStats>,
    lba_space: u64,
    scan_lbas: u64,
    interval: Duration,
) {
    let mut rng = ChaCha8Rng::seed_from_u64(0x515C_A11E_D00D_F00D ^ (sid as u64));
    while !stop.load(Ordering::Relaxed) {
        if interval.as_nanos() > 0 {
            thread::sleep(interval);
            if stop.load(Ordering::Relaxed) {
                break;
            }
        }
        let (start, end) = range_scan_bounds(&mut rng, lba_space, scan_lbas);
        let started = Instant::now();
        let mut entries = 0u64;
        match db.scan_range_unordered(VOL, start..end, |_, _| {
            entries += 1;
            Ok(())
        }) {
            Ok(()) => {}
            Err(e) => {
                eprintln!("range-scanner-{sid} range error: {e}");
                return;
            }
        }
        let elapsed_us = started.elapsed().as_micros() as u64;
        stats.scans.fetch_add(1, Ordering::Relaxed);
        stats.entries.fetch_add(entries, Ordering::Relaxed);
        stats.latencies.lock().push(elapsed_us);
    }
}

fn range_scan_bounds(rng: &mut ChaCha8Rng, lba_space: u64, scan_lbas: u64) -> (u64, u64) {
    if lba_space == 0 {
        return (0, 0);
    }
    let width = if scan_lbas == 0 || scan_lbas >= lba_space {
        lba_space
    } else {
        scan_lbas
    };
    if width >= lba_space {
        (0, lba_space)
    } else {
        let start = rng.gen_range(0..=(lba_space - width));
        (start, start + width)
    }
}

// ───────────────────── stats sampling ─────────────────────

struct Sample {
    metrics: MetaMetricsSnapshot,
    pending: PendingState,
    cache_pages: u64,
    cache_capacity: u64,
    commits: u64,
    ops: u64,
    dedup_hits: u64,
    dedup_misses: u64,
    dedup_register_commits: u64,
    dedup_register_ops: u64,
    dedup_register_lats_us: Vec<u64>,
    cleanup_calls: u64,
    cleanup_pbas: u64,
    commit_lats_us: Vec<u64>,
    flushes: u64,
    reads: u64,
    read_lats_us: Vec<u64>,
    range_scans: u64,
    range_entries: u64,
    range_lats_us: Vec<u64>,
    at: Instant,
}
impl Sample {
    fn take(
        db: &Arc<Db>,
        wstats: &[Arc<WriterStats>],
        rstats: &[Arc<ReaderStats>],
        dstats: &[Arc<DedupRegisterStats>],
        range_stats: &Arc<RangeScanStats>,
        fstats: &Arc<FlusherStats>,
    ) -> Self {
        let metrics = db.metrics_snapshot();
        let pending = db.pending_state();
        let cache = db.cache_stats();
        let mut commits = 0u64;
        let mut ops = 0u64;
        let mut dedup_hits = 0u64;
        let mut dedup_misses = 0u64;
        let mut cleanup_calls = 0u64;
        let mut cleanup_pbas = 0u64;
        let mut commit_lats_us: Vec<u64> = Vec::new();
        for s in wstats {
            commits += s.commits.load(Ordering::Relaxed);
            ops += s.ops.load(Ordering::Relaxed);
            dedup_hits += s.dedup_hits.load(Ordering::Relaxed);
            dedup_misses += s.dedup_misses.load(Ordering::Relaxed);
            cleanup_calls += s.cleanup_calls.load(Ordering::Relaxed);
            cleanup_pbas += s.cleanup_pbas.load(Ordering::Relaxed);
            let mut lats = s.latencies.lock();
            commit_lats_us.append(&mut lats);
        }
        let mut dedup_register_commits = 0u64;
        let mut dedup_register_ops = 0u64;
        let mut dedup_register_lats_us = Vec::new();
        for s in dstats {
            dedup_register_commits += s.commits.load(Ordering::Relaxed);
            dedup_register_ops += s.ops.load(Ordering::Relaxed);
            let mut lats = s.latencies.lock();
            dedup_register_lats_us.append(&mut lats);
        }
        let mut reads = 0u64;
        let mut read_lats_us: Vec<u64> = Vec::new();
        for s in rstats {
            reads += s.reads.load(Ordering::Relaxed);
            let mut lats = s.latencies.lock();
            read_lats_us.append(&mut lats);
        }
        let range_scans = range_stats.scans.load(Ordering::Relaxed);
        let range_entries = range_stats.entries.load(Ordering::Relaxed);
        let mut range_lats_us = Vec::new();
        {
            let mut lats = range_stats.latencies.lock();
            range_lats_us.append(&mut lats);
        }
        Self {
            metrics,
            pending,
            cache_pages: cache.current_bytes,
            cache_capacity: cache.capacity_bytes,
            commits,
            ops,
            dedup_hits,
            dedup_misses,
            dedup_register_commits,
            dedup_register_ops,
            dedup_register_lats_us,
            cleanup_calls,
            cleanup_pbas,
            commit_lats_us,
            flushes: fstats.flushes.load(Ordering::Relaxed),
            reads,
            read_lats_us,
            range_scans,
            range_entries,
            range_lats_us,
            at: Instant::now(),
        }
    }
}

#[derive(Clone, Copy)]
struct WindowStats {
    secs: f64,
    commits: u64,
    ops: u64,
    dedup_hits: u64,
    dedup_misses: u64,
    dedup_register_commits: u64,
    dedup_register_ops: u64,
    dedup_register_p99_ms: f64,
    cleanup_calls: u64,
    cleanup_pbas: u64,
    cleanup_scan_avg_ms: f64,
    cleanup_forward_check_avg_ms: f64,
    cleanup_commit_avg_ms: f64,
    commit_p50_ms: f64,
    commit_p99_ms: f64,
    commit_max_ms: f64,
    commit_wal_avg_ms: f64,
    commit_apply_wait_avg_ms: f64,
    commit_apply_gate_avg_ms: f64,
    commit_apply_avg_ms: f64,
    wal_batches: u64,
    wal_fsyncs: u64,
    wal_batch_records_max: u64,
    wal_write_avg_ms: f64,
    wal_fsync_avg_ms: f64,
    wal_submit_avg_ms: f64,
    apply_l2p_remap_avg_us: f64,
    apply_refcount_avg_us: f64,
    apply_dedup_avg_us: f64,
    flushes: u64,
    flush_gate_max_us: u64,
    flush_io_max_us: u64,
    flush_manifest_max_us: u64,
    flush_install_max_us: u64,
    flush_reclaim_max_us: u64,
    flush_total_max_us: u64,
    flush_pages: u64,
    l2p_pagebuf_total: u64,
    l2p_pagebuf_dirty: u64,
    rc_pagebuf_total: u64,
    rc_pagebuf_dirty: u64,
    l2p_apply_q: u64,
    cache_pct: u64,
    reads: u64,
    read_p99_ms: f64,
    range_scans: u64,
    range_entries: u64,
    range_scan_p99_ms: f64,
    range_scan_max_ms: f64,
}
impl WindowStats {
    fn between(prev: &Sample, now: &Sample) -> Self {
        let secs = now.at.duration_since(prev.at).as_secs_f64().max(0.001);
        // Latency vec was DRAINED on prev.take() (it appended to its own
        // commit_lats_us). The latencies in `now` are everything since
        // the previous sample.
        let mut lats = now.commit_lats_us.clone();
        lats.sort_unstable();
        let commit_p50_ms = pct(&lats, 50) as f64 / 1000.0;
        let commit_p99_ms = pct(&lats, 99) as f64 / 1000.0;
        let commit_max_ms = lats.last().copied().unwrap_or(0) as f64 / 1000.0;
        let mut dedup_lats = now.dedup_register_lats_us.clone();
        dedup_lats.sort_unstable();
        let dedup_register_p99_ms = pct(&dedup_lats, 99) as f64 / 1000.0;
        let mut rl = now.read_lats_us.clone();
        rl.sort_unstable();
        let read_p99_ms = pct(&rl, 99) as f64 / 1000.0;
        let mut scan_lats = now.range_lats_us.clone();
        scan_lats.sort_unstable();
        let range_scan_p99_ms = pct(&scan_lats, 99) as f64 / 1000.0;
        let range_scan_max_ms = scan_lats.last().copied().unwrap_or(0) as f64 / 1000.0;
        let cache_pct = if now.cache_capacity == 0 {
            0
        } else {
            now.cache_pages * 100 / now.cache_capacity
        };
        let wal_batches = now
            .metrics
            .wal_batches
            .saturating_sub(prev.metrics.wal_batches);
        let wal_fsyncs = now
            .metrics
            .wal_fsyncs
            .saturating_sub(prev.metrics.wal_fsyncs);
        let wal_write_us = now
            .metrics
            .wal_write_us
            .saturating_sub(prev.metrics.wal_write_us);
        let wal_fsync_us = now
            .metrics
            .wal_fsync_us
            .saturating_sub(prev.metrics.wal_fsync_us);
        let wal_submit_calls = now
            .metrics
            .wal_submit_calls
            .saturating_sub(prev.metrics.wal_submit_calls);
        let wal_submit_wait_us = now
            .metrics
            .wal_submit_wait_us
            .saturating_sub(prev.metrics.wal_submit_wait_us);
        let commits = now.commits.saturating_sub(prev.commits);
        let commit_wal_us = now
            .metrics
            .commit_wal_submit_us
            .saturating_sub(prev.metrics.commit_wal_submit_us);
        let commit_apply_wait_us = now
            .metrics
            .commit_apply_wait_us
            .saturating_sub(prev.metrics.commit_apply_wait_us);
        let commit_apply_gate_wait_us = now
            .metrics
            .commit_apply_gate_wait_us
            .saturating_sub(prev.metrics.commit_apply_gate_wait_us);
        let commit_apply_us = now
            .metrics
            .commit_apply_us
            .saturating_sub(prev.metrics.commit_apply_us);
        let apply_l2p_remap_count = now
            .metrics
            .apply_l2p_remap_count
            .saturating_sub(prev.metrics.apply_l2p_remap_count);
        let apply_l2p_remap_us = now
            .metrics
            .apply_l2p_remap_us
            .saturating_sub(prev.metrics.apply_l2p_remap_us);
        let apply_refcount_count = now
            .metrics
            .apply_refcount_count
            .saturating_sub(prev.metrics.apply_refcount_count);
        let apply_refcount_us = now
            .metrics
            .apply_refcount_us
            .saturating_sub(prev.metrics.apply_refcount_us);
        let apply_dedup_count = now
            .metrics
            .apply_dedup_count
            .saturating_sub(prev.metrics.apply_dedup_count);
        let apply_dedup_us = now
            .metrics
            .apply_dedup_us
            .saturating_sub(prev.metrics.apply_dedup_us);
        let cleanup_calls = now.cleanup_calls.saturating_sub(prev.cleanup_calls);
        let cleanup_scan_us = now
            .metrics
            .cleanup_scan_us
            .saturating_sub(prev.metrics.cleanup_scan_us);
        let cleanup_forward_check_us = now
            .metrics
            .cleanup_forward_check_us
            .saturating_sub(prev.metrics.cleanup_forward_check_us);
        let cleanup_commit_us = now
            .metrics
            .cleanup_commit_us
            .saturating_sub(prev.metrics.cleanup_commit_us);
        Self {
            secs,
            commits,
            ops: now.ops.saturating_sub(prev.ops),
            dedup_hits: now.dedup_hits.saturating_sub(prev.dedup_hits),
            dedup_misses: now.dedup_misses.saturating_sub(prev.dedup_misses),
            dedup_register_commits: now
                .dedup_register_commits
                .saturating_sub(prev.dedup_register_commits),
            dedup_register_ops: now
                .dedup_register_ops
                .saturating_sub(prev.dedup_register_ops),
            dedup_register_p99_ms,
            cleanup_calls,
            cleanup_pbas: now.cleanup_pbas.saturating_sub(prev.cleanup_pbas),
            cleanup_scan_avg_ms: avg_ms(cleanup_scan_us, cleanup_calls),
            cleanup_forward_check_avg_ms: avg_ms(cleanup_forward_check_us, cleanup_calls),
            cleanup_commit_avg_ms: avg_ms(cleanup_commit_us, cleanup_calls),
            commit_p50_ms,
            commit_p99_ms,
            commit_max_ms,
            commit_wal_avg_ms: avg_ms(commit_wal_us, commits),
            commit_apply_wait_avg_ms: avg_ms(commit_apply_wait_us, commits),
            commit_apply_gate_avg_ms: avg_ms(commit_apply_gate_wait_us, commits),
            commit_apply_avg_ms: avg_ms(commit_apply_us, commits),
            wal_batches,
            wal_fsyncs,
            wal_batch_records_max: now.metrics.wal_batch_records_max,
            wal_write_avg_ms: avg_ms(wal_write_us, wal_batches),
            wal_fsync_avg_ms: avg_ms(wal_fsync_us, wal_fsyncs),
            wal_submit_avg_ms: avg_ms(wal_submit_wait_us, wal_submit_calls),
            apply_l2p_remap_avg_us: avg_us(apply_l2p_remap_us, apply_l2p_remap_count),
            apply_refcount_avg_us: avg_us(apply_refcount_us, apply_refcount_count),
            apply_dedup_avg_us: avg_us(apply_dedup_us, apply_dedup_count),
            flushes: now.flushes.saturating_sub(prev.flushes),
            // flush metrics are cumulative MAX across the run; we report
            // the running max here. A per-window max would require
            // per-call samples we don't have.
            flush_gate_max_us: now.metrics.flush_gate_wait_max_us,
            flush_io_max_us: now.metrics.flush_io_max_us,
            flush_manifest_max_us: now.metrics.flush_manifest_max_us,
            flush_install_max_us: now.metrics.flush_install_max_us,
            flush_reclaim_max_us: now.metrics.flush_reclaim_max_us,
            flush_total_max_us: now.metrics.flush_total_max_us,
            flush_pages: now
                .metrics
                .flush_pages_written
                .saturating_sub(prev.metrics.flush_pages_written),
            l2p_pagebuf_total: now.pending.l2p_pagebuf_total as u64,
            l2p_pagebuf_dirty: now.pending.l2p_pagebuf_dirty as u64,
            rc_pagebuf_total: now.pending.rc_pagebuf_total as u64,
            rc_pagebuf_dirty: now.pending.rc_pagebuf_dirty as u64,
            l2p_apply_q: now.pending.l2p_apply_queue as u64,
            cache_pct,
            reads: now.reads.saturating_sub(prev.reads),
            read_p99_ms,
            range_scans: now.range_scans.saturating_sub(prev.range_scans),
            range_entries: now.range_entries.saturating_sub(prev.range_entries),
            range_scan_p99_ms,
            range_scan_max_ms,
        }
    }
}

fn pct(sorted: &[u64], p: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = (sorted.len() * p / 100).min(sorted.len() - 1);
    sorted[idx]
}

fn avg_ms(total_us: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        total_us as f64 / count as f64 / 1000.0
    }
}

fn avg_us(total_us: u64, count: u64) -> f64 {
    if count == 0 {
        0.0
    } else {
        total_us as f64 / count as f64
    }
}

fn report(series: &[WindowStats], args: &Args) -> bool {
    if series.is_empty() {
        eprintln!("(no post-warmup samples — duration too short)");
        return false;
    }
    let total_commits: u64 = series.iter().map(|w| w.commits).sum();
    let total_ops: u64 = series.iter().map(|w| w.ops).sum();
    let total_bulk_dedup_ops: u64 = series.iter().map(|w| w.dedup_register_ops).sum();
    let total_reads: u64 = series.iter().map(|w| w.reads).sum();
    let total_secs: f64 = series.iter().map(|w| w.secs).sum();
    let final_install_max = series.last().unwrap().flush_install_max_us;
    let final_gate_max = series.last().unwrap().flush_gate_max_us;
    let max_commit_p99 = series
        .iter()
        .map(|w| w.commit_p99_ms)
        .fold(0.0_f64, f64::max);
    let max_range_scan_p99 = series
        .iter()
        .map(|w| w.range_scan_p99_ms)
        .fold(0.0_f64, f64::max);

    // Throughput stability: avg ops/sec in last quarter vs middle quarter.
    let n = series.len();
    let mid = if n >= 4 {
        let q = n / 4;
        let mid_slice = &series[q..n - q];
        avg_ops_per_sec(mid_slice)
    } else {
        avg_ops_per_sec(series)
    };
    let last = if n >= 4 {
        let q = n / 4;
        avg_ops_per_sec(&series[n - q..])
    } else {
        avg_ops_per_sec(series)
    };
    let stability = if mid == 0.0 { 1.0 } else { last / mid };

    eprintln!(
        "samples={} duration={:.1}s commits={} ops={} avg_ops/s={:.0} bulk_dedup_ops={} bulk_dedup_ops/s={:.0}",
        series.len(),
        total_secs,
        total_commits,
        total_ops,
        total_ops as f64 / total_secs.max(0.001),
        total_bulk_dedup_ops,
        total_bulk_dedup_ops as f64 / total_secs.max(0.001)
    );
    if total_reads > 0 {
        eprintln!(
            "reader calls/s={:.0} reader items/s={:.0}",
            total_reads as f64 / total_secs.max(0.001),
            (total_reads as f64 * args.reader_batch as f64) / total_secs.max(0.001)
        );
    }
    eprintln!("commit p99 max across windows = {:.1} ms", max_commit_p99);
    eprintln!(
        "range scan p99 max across windows = {:.1} ms",
        max_range_scan_p99
    );
    eprintln!(
        "throughput last/mid = {:.0}/{:.0} ops/s ({:.0}%)",
        last,
        mid,
        stability * 100.0
    );
    eprintln!(
        "flush install_max(running) = {} ms   gate_wait_max(running) = {} ms",
        final_install_max / 1000,
        final_gate_max / 1000
    );

    let avg_ops_per_sec = if total_secs > 0.0 {
        total_ops as f64 / total_secs
    } else {
        0.0
    };
    let install_pass = final_install_max / 1000 < args.target_install_max_ms;
    let p99_pass = max_commit_p99 < args.target_commit_p99_ms as f64;
    let stability_pass = stability >= 0.80;
    let throughput_pass =
        args.target_ops_per_sec == 0 || avg_ops_per_sec >= args.target_ops_per_sec as f64;

    eprintln!();
    eprintln!(
        "  [{}] install_max < {} ms  (got {} ms)",
        mark(install_pass),
        args.target_install_max_ms,
        final_install_max / 1000
    );
    eprintln!(
        "  [{}] commit P99   < {} ms  (got {:.1} ms)",
        mark(p99_pass),
        args.target_commit_p99_ms,
        max_commit_p99
    );
    eprintln!(
        "  [{}] throughput stability >= 80%%  (got {:.0}%%)",
        mark(stability_pass),
        stability * 100.0
    );
    if args.target_ops_per_sec > 0 {
        eprintln!(
            "  [{}] avg_ops/s   >= {}  (got {:.0})",
            mark(throughput_pass),
            args.target_ops_per_sec,
            avg_ops_per_sec
        );
    }

    install_pass && p99_pass && stability_pass && throughput_pass
}

fn avg_ops_per_sec(slice: &[WindowStats]) -> f64 {
    let ops: u64 = slice.iter().map(|w| w.ops).sum();
    let secs: f64 = slice.iter().map(|w| w.secs).sum();
    if secs == 0.0 { 0.0 } else { ops as f64 / secs }
}

fn mark(b: bool) -> &'static str {
    if b { "PASS" } else { "FAIL" }
}
