use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use onyx_metadb::{Config, Db, DedupValue, Hash32, L2pValue, PageCacheStats, Pba};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

fn main() -> std::process::ExitCode {
    match run() {
        Ok(code) => code,
        Err(err) => {
            if !err.is_empty() {
                eprintln!("metadb-bench: {err}");
            }
            std::process::ExitCode::from(2)
        }
    }
}

fn run() -> Result<std::process::ExitCode, String> {
    let cfg = BenchConfig::parse(env::args().skip(1))?;
    let report = run_benchmark(&cfg)?;
    if cfg.json {
        print_json(&report);
    } else {
        print_human(&report);
    }
    Ok(std::process::ExitCode::SUCCESS)
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    L2pPrefill,
    L2pPut,
    L2pGet,
    L2pMultiGet,
    MetaTx,
}

impl Scenario {
    fn as_str(self) -> &'static str {
        match self {
            Self::L2pPrefill => "l2p-prefill",
            Self::L2pPut => "l2p-put",
            Self::L2pGet => "l2p-get",
            Self::L2pMultiGet => "l2p-multi-get",
            Self::MetaTx => "meta-tx",
        }
    }

    fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "l2p-prefill" => Ok(Self::L2pPrefill),
            "l2p-put" => Ok(Self::L2pPut),
            "l2p-get" => Ok(Self::L2pGet),
            "l2p-multi-get" => Ok(Self::L2pMultiGet),
            "meta-tx" => Ok(Self::MetaTx),
            _ => Err(format!("unknown benchmark scenario `{raw}`")),
        }
    }
}

#[derive(Clone, Debug)]
struct BenchConfig {
    scenario: Scenario,
    path: PathBuf,
    reset: bool,
    ops: u64,
    threads: usize,
    shards: u32,
    key_space: u64,
    prefill_keys: u64,
    prefill_bytes: u64,
    batch_size: usize,
    warmup_ops: u64,
    overwrite_pct: u8,
    dedup_hit_pct: u8,
    page_cache_bytes: u64,
    seed: u64,
    reuse_existing: bool,
    skip_prefill: bool,
    json: bool,
}

impl BenchConfig {
    fn parse<I>(mut args: I) -> Result<Self, String>
    where
        I: Iterator<Item = String>,
    {
        let Some(first) = args.next() else {
            print_usage();
            return Err(String::new());
        };
        if first == "-h" || first == "--help" {
            print_usage();
            return Err(String::new());
        }

        let scenario = Scenario::parse(&first)?;
        let mut path = None;
        let mut reset = false;
        let mut ops = 100_000u64;
        let mut threads = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(16);
        let mut shards = 16u32;
        let mut key_space = 100_000u64;
        let mut prefill_keys = 0u64;
        let mut prefill_bytes = 0u64;
        let mut batch_size = 32usize;
        let mut warmup_ops = 10_000u64;
        let mut overwrite_pct = 30u8;
        let mut dedup_hit_pct = 20u8;
        let mut page_cache_bytes = 512 * 1024 * 1024u64;
        let mut seed = 0xBEEF_5EEDu64;
        let mut reuse_existing = false;
        let mut skip_prefill = false;
        let mut json = false;

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--path" => {
                    path = Some(PathBuf::from(
                        args.next().ok_or_else(|| "--path needs a value".to_string())?,
                    ));
                }
                "--reset" => reset = true,
                "--ops" => ops = parse_u64(args.next(), "--ops")?,
                "--threads" => threads = parse_u64(args.next(), "--threads")? as usize,
                "--shards" => shards = parse_u64(args.next(), "--shards")? as u32,
                "--key-space" => key_space = parse_u64(args.next(), "--key-space")?,
                "--prefill-keys" => prefill_keys = parse_u64(args.next(), "--prefill-keys")?,
                "--prefill-bytes" => {
                    prefill_bytes = parse_u64(args.next(), "--prefill-bytes")?
                }
                "--batch-size" => batch_size = parse_u64(args.next(), "--batch-size")? as usize,
                "--warmup-ops" => warmup_ops = parse_u64(args.next(), "--warmup-ops")?,
                "--overwrite-pct" => overwrite_pct = parse_pct(args.next(), "--overwrite-pct")?,
                "--dedup-hit-pct" => {
                    dedup_hit_pct = parse_pct(args.next(), "--dedup-hit-pct")?
                }
                "--cache-mb" => {
                    page_cache_bytes = parse_u64(args.next(), "--cache-mb")? * 1024 * 1024
                }
                "--seed" => seed = parse_u64(args.next(), "--seed")?,
                "--reuse-existing" => reuse_existing = true,
                "--skip-prefill" => skip_prefill = true,
                "--json" => json = true,
                "-h" | "--help" => {
                    print_usage();
                    return Err(String::new());
                }
                _ => return Err(format!("unknown flag `{arg}`")),
            }
        }

        let path = path.unwrap_or_else(|| default_bench_path(scenario));
        Ok(Self {
            scenario,
            path,
            reset,
            ops,
            threads: threads.max(1),
            shards: shards.max(1),
            key_space: key_space.max(1),
            prefill_keys,
            prefill_bytes,
            batch_size: batch_size.max(1),
            warmup_ops,
            overwrite_pct,
            dedup_hit_pct,
            page_cache_bytes,
            seed,
            reuse_existing,
            skip_prefill,
            json,
        })
    }
}

#[derive(Clone, Debug)]
struct BenchReport {
    scenario: &'static str,
    path: PathBuf,
    ops: u64,
    items: u64,
    threads: usize,
    prefill_keys: u64,
    prefill_bytes: u64,
    prefill_elapsed_secs: f64,
    prefill_ops_per_sec: f64,
    prefill_bytes_per_sec: f64,
    elapsed_secs: f64,
    ops_per_sec: f64,
    items_per_sec: f64,
    latency_us: LatencySummary,
    cache_delta: Option<PageCacheDelta>,
}

#[derive(Clone, Debug)]
struct LatencySummary {
    count: usize,
    avg: f64,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
}

#[derive(Clone, Debug)]
struct PageCacheDelta {
    hits: u64,
    misses: u64,
    evictions: u64,
    current_pages: u64,
    current_bytes: u64,
}

#[derive(Default)]
struct WorkerResult {
    latencies_us: Vec<u64>,
    ops: u64,
    items: u64,
}

#[derive(Clone, Copy, Debug, Default)]
struct PrefillStats {
    keys: u64,
    bytes: u64,
    elapsed_secs: f64,
}

impl PrefillStats {
    fn none() -> Self {
        Self::default()
    }
}

#[derive(Clone, Debug)]
struct Mapping {
    pba: Pba,
    dedup_idx: usize,
}

#[derive(Clone, Debug)]
struct DedupEntry {
    pba: Pba,
    hash: Hash32,
    live_refs: u32,
}

fn run_benchmark(cfg: &BenchConfig) -> Result<BenchReport, String> {
    prepare_dir(&cfg.path, cfg.reset, cfg.reuse_existing)?;
    let db_cfg = db_config(&cfg.path, cfg.shards, cfg.page_cache_bytes);
    let db = Arc::new(
        if cfg.reuse_existing {
            Db::open_with_config(db_cfg.clone()).map_err(|e| e.to_string())?
        } else {
            Db::create_with_config(db_cfg.clone()).map_err(|e| e.to_string())?
        },
    );

    match cfg.scenario {
        Scenario::L2pPrefill => run_l2p_prefill(cfg, db_cfg),
        Scenario::L2pPut => run_l2p_put(cfg, db),
        Scenario::L2pGet => run_l2p_get(cfg, db_cfg),
        Scenario::L2pMultiGet => run_l2p_multi_get(cfg, db_cfg),
        Scenario::MetaTx => run_meta_tx(cfg, db),
    }
}

fn run_l2p_prefill(cfg: &BenchConfig, db_cfg: Config) -> Result<BenchReport, String> {
    let prefill_keys = effective_prefill_keys(cfg, cfg.key_space.max(cfg.ops).max(1), 28);
    let prefill = prefill_l2p(&db_cfg, prefill_keys)?;
    Ok(build_report(
        cfg,
        prefill,
        Duration::from_secs(0),
        WorkerResult::default(),
        None,
    ))
}

fn run_l2p_put(cfg: &BenchConfig, db: Arc<Db>) -> Result<BenchReport, String> {
    let barrier = Arc::new(Barrier::new(cfg.threads + 1));
    let mut handles = Vec::with_capacity(cfg.threads);
    let ops_per_thread = split_ops(cfg.ops, cfg.threads);
    let key_stride = cfg.ops.max(1);

    for (tid, thread_ops) in ops_per_thread.into_iter().enumerate() {
        let db = db.clone();
        let barrier = barrier.clone();
        let seed = cfg.seed ^ ((tid as u64 + 1) << 17);
        let key_base = tid as u64 * key_stride;
        handles.push(thread::spawn(move || {
            let mut out = WorkerResult::default();
            let mut _rng = ChaCha8Rng::seed_from_u64(seed);
            barrier.wait();
            for i in 0..thread_ops {
                let key = key_base + i;
                let value = l2p_value_from_pba(key);
                let start = Instant::now();
                db.insert(key, value).unwrap();
                out.latencies_us.push(start.elapsed().as_micros() as u64);
                out.ops += 1;
                out.items += 1;
            }
            out
        }));
    }

    let start = Instant::now();
    barrier.wait();
    let merged = merge_worker_results(handles)?;
    let elapsed = start.elapsed();
    Ok(build_report(
        cfg,
        PrefillStats::none(),
        elapsed,
        merged,
        None,
    ))
}

fn run_l2p_get(cfg: &BenchConfig, db_cfg: Config) -> Result<BenchReport, String> {
    let prefill_keys = effective_prefill_keys(cfg, cfg.key_space.max(cfg.ops), 28);
    let prefill = if cfg.skip_prefill {
        PrefillStats::none()
    } else {
        prefill_l2p(&db_cfg, prefill_keys)?
    };
    let db = Arc::new(Db::open_with_config(db_cfg).map_err(|e| e.to_string())?);

    // Warm cache before measuring steady-state read latency.
    if cfg.warmup_ops > 0 {
        warmup_l2p_reads(&db, cfg.threads, cfg.warmup_ops, cfg.key_space, cfg.seed)?;
    }
    let cache_before = db.cache_stats();

    let barrier = Arc::new(Barrier::new(cfg.threads + 1));
    let mut handles = Vec::with_capacity(cfg.threads);
    let ops_per_thread = split_ops(cfg.ops, cfg.threads);
    for (tid, thread_ops) in ops_per_thread.into_iter().enumerate() {
        let db = db.clone();
        let barrier = barrier.clone();
        let key_space = cfg.key_space;
        let seed = cfg.seed ^ ((tid as u64 + 1) << 17);
        handles.push(thread::spawn(move || {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            let mut out = WorkerResult::default();
            barrier.wait();
            for _ in 0..thread_ops {
                let key = rng.gen_range(0..key_space);
                let start = Instant::now();
                let got = db.get(key).unwrap();
                out.latencies_us.push(start.elapsed().as_micros() as u64);
                debug_assert!(got.is_some());
                out.ops += 1;
                out.items += 1;
            }
            out
        }));
    }

    let start = Instant::now();
    barrier.wait();
    let merged = merge_worker_results(handles)?;
    let elapsed = start.elapsed();
    let cache_after = db.cache_stats();
    Ok(build_report(
        cfg,
        prefill,
        elapsed,
        merged,
        Some(cache_delta(cache_before, cache_after)),
    ))
}

fn run_l2p_multi_get(cfg: &BenchConfig, db_cfg: Config) -> Result<BenchReport, String> {
    let total_items = cfg.ops.saturating_mul(cfg.batch_size as u64);
    let prefill_keys = effective_prefill_keys(cfg, cfg.key_space.max(total_items), 28);
    let prefill = if cfg.skip_prefill {
        PrefillStats::none()
    } else {
        prefill_l2p(&db_cfg, prefill_keys)?
    };
    let db = Arc::new(Db::open_with_config(db_cfg).map_err(|e| e.to_string())?);

    if cfg.warmup_ops > 0 {
        warmup_l2p_multi_reads(
            &db,
            cfg.threads,
            cfg.warmup_ops,
            cfg.batch_size,
            cfg.key_space,
            cfg.seed,
        )?;
    }
    let cache_before = db.cache_stats();

    let barrier = Arc::new(Barrier::new(cfg.threads + 1));
    let mut handles = Vec::with_capacity(cfg.threads);
    let ops_per_thread = split_ops(cfg.ops, cfg.threads);
    for (tid, thread_ops) in ops_per_thread.into_iter().enumerate() {
        let db = db.clone();
        let barrier = barrier.clone();
        let key_space = cfg.key_space;
        let batch_size = cfg.batch_size;
        let seed = cfg.seed ^ ((tid as u64 + 1) << 17);
        handles.push(thread::spawn(move || {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            let mut out = WorkerResult::default();
            let mut keys = vec![0u64; batch_size];
            barrier.wait();
            for _ in 0..thread_ops {
                for key in &mut keys {
                    *key = rng.gen_range(0..key_space);
                }
                let start = Instant::now();
                let got = db.multi_get(&keys).unwrap();
                out.latencies_us.push(start.elapsed().as_micros() as u64);
                debug_assert_eq!(got.len(), keys.len());
                out.ops += 1;
                out.items += batch_size as u64;
            }
            out
        }));
    }

    let start = Instant::now();
    barrier.wait();
    let merged = merge_worker_results(handles)?;
    let elapsed = start.elapsed();
    let cache_after = db.cache_stats();
    Ok(build_report(
        cfg,
        prefill,
        elapsed,
        merged,
        Some(cache_delta(cache_before, cache_after)),
    ))
}

fn run_meta_tx(cfg: &BenchConfig, db: Arc<Db>) -> Result<BenchReport, String> {
    let barrier = Arc::new(Barrier::new(cfg.threads + 1));
    let mut handles = Vec::with_capacity(cfg.threads);
    let ops_per_thread = split_ops(cfg.ops, cfg.threads);

    for (tid, thread_ops) in ops_per_thread.into_iter().enumerate() {
        let db = db.clone();
        let barrier = barrier.clone();
        let thread_cfg = cfg.clone();
        handles.push(thread::spawn(move || {
            let mut rng = ChaCha8Rng::seed_from_u64(thread_cfg.seed ^ ((tid as u64 + 1) << 17));
            let mut out = WorkerResult::default();
            let working_set = (thread_cfg.key_space / thread_cfg.threads as u64).max(1) as usize;
            let mut slots: Vec<Option<Mapping>> = vec![None; working_set];
            let mut free_slots: Vec<usize> = (0..working_set).collect();
            let mut used_slots: Vec<usize> = Vec::new();
            let mut dedup_entries: Vec<DedupEntry> = Vec::new();
            let mut active_dedup: Vec<usize> = Vec::new();
            let mut next_pba: u64 = ((tid as u64) << 48) | 1;
            let mut next_hash_seq: u64 = 1;
            let lba_base = tid as u64 * working_set as u64;

            barrier.wait();
            for _ in 0..thread_ops {
                let slot_idx = if !free_slots.is_empty()
                    && (used_slots.is_empty()
                        || rng.gen_range(0..100u8) >= thread_cfg.overwrite_pct)
                {
                    let idx = rng.gen_range(0..free_slots.len());
                    let slot = free_slots.swap_remove(idx);
                    used_slots.push(slot);
                    slot
                } else {
                    used_slots[rng.gen_range(0..used_slots.len())]
                };

                let lba = lba_base + slot_idx as u64;
                let mut tx = db.begin();

                if let Some(old) = slots[slot_idx].take() {
                    tx.decref_pba(old.pba, 1);
                    let old_entry = &mut dedup_entries[old.dedup_idx];
                    old_entry.live_refs -= 1;
                    if old_entry.live_refs == 0 {
                        tx.delete_dedup(old_entry.hash);
                        tx.unregister_dedup_reverse(old_entry.pba, old_entry.hash);
                        if let Some(pos) = active_dedup.iter().position(|idx| *idx == old.dedup_idx)
                        {
                            active_dedup.swap_remove(pos);
                        }
                    }
                }

                let use_dedup_hit = !active_dedup.is_empty()
                    && rng.gen_range(0..100u8) < thread_cfg.dedup_hit_pct;

                let mapping = if use_dedup_hit {
                    let idx = active_dedup[rng.gen_range(0..active_dedup.len())];
                    let entry = &mut dedup_entries[idx];
                    entry.live_refs += 1;
                    tx.insert(lba, l2p_value_from_pba(entry.pba));
                    tx.incref_pba(entry.pba, 1);
                    Mapping {
                        pba: entry.pba,
                        dedup_idx: idx,
                    }
                } else {
                    let pba = next_pba;
                    next_pba += 1;
                    let hash = hash_from_parts(tid as u64, next_hash_seq);
                    next_hash_seq += 1;
                    tx.insert(lba, l2p_value_from_pba(pba));
                    tx.incref_pba(pba, 1);
                    tx.put_dedup(hash, dedup_value_from_pba(pba));
                    tx.register_dedup_reverse(pba, hash);
                    dedup_entries.push(DedupEntry {
                        pba,
                        hash,
                        live_refs: 1,
                    });
                    let idx = dedup_entries.len() - 1;
                    active_dedup.push(idx);
                    Mapping {
                        pba,
                        dedup_idx: idx,
                    }
                };

                let start = Instant::now();
                tx.commit().unwrap();
                out.latencies_us.push(start.elapsed().as_micros() as u64);
                out.ops += 1;
                out.items += 1;
                slots[slot_idx] = Some(mapping);
            }
            out
        }));
    }

    let start = Instant::now();
    barrier.wait();
    let merged = merge_worker_results(handles)?;
    let elapsed = start.elapsed();
    Ok(build_report(
        cfg,
        PrefillStats::none(),
        elapsed,
        merged,
        None,
    ))
}

fn prepare_dir(path: &Path, reset: bool, reuse_existing: bool) -> Result<(), String> {
    if reset && path.exists() {
        fs::remove_dir_all(path).map_err(|e| format!("remove {}: {e}", path.display()))?;
    }
    if reuse_existing {
        return Ok(());
    }
    if path.exists() {
        let mut entries = fs::read_dir(path).map_err(|e| format!("read {}: {e}", path.display()))?;
        if entries.next().transpose().map_err(|e| e.to_string())?.is_some() {
            return Err(format!(
                "path {} already exists and is not empty; pass --reset or use a fresh path",
                path.display()
            ));
        }
    }
    fs::create_dir_all(path).map_err(|e| format!("create {}: {e}", path.display()))
}

fn db_config(path: &Path, shards: u32, page_cache_bytes: u64) -> Config {
    let mut cfg = Config::new(path);
    cfg.shards_per_partition = shards;
    cfg.page_cache_bytes = page_cache_bytes;
    cfg
}

fn prefill_l2p(cfg: &Config, count: u64) -> Result<PrefillStats, String> {
    let db = Db::open_with_config(cfg.clone())
        .or_else(|_| Db::create_with_config(cfg.clone()))
        .map_err(|e| e.to_string())?;
    let start = Instant::now();
    let progress_every = progress_interval(count);
    for key in 0..count {
        db.insert(key, l2p_value_from_pba(key)).map_err(|e| e.to_string())?;
        if progress_every > 0 && (key + 1) % progress_every == 0 {
            print_prefill_progress("metadb", key + 1, count, start.elapsed());
        }
    }
    db.flush().map_err(|e| e.to_string())?;
    let elapsed_secs = start.elapsed().as_secs_f64().max(f64::EPSILON);
    Ok(PrefillStats {
        keys: count,
        bytes: count.saturating_mul(28),
        elapsed_secs,
    })
}

fn progress_interval(total: u64) -> u64 {
    if total < 10 {
        0
    } else {
        (total / 20).max(1)
    }
}

fn print_prefill_progress(backend: &str, done: u64, total: u64, elapsed: std::time::Duration) {
    let secs = elapsed.as_secs_f64().max(f64::EPSILON);
    let bytes_done = done.saturating_mul(28);
    let bytes_total = total.saturating_mul(28);
    eprintln!(
        "[{backend} prefill] {done}/{total} keys ({:.2}/{:.2} GiB) elapsed={:.1}s rate={:.0} keys/s",
        bytes_done as f64 / 1024.0 / 1024.0 / 1024.0,
        bytes_total as f64 / 1024.0 / 1024.0 / 1024.0,
        secs,
        done as f64 / secs,
    );
}

fn warmup_l2p_reads(
    db: &Arc<Db>,
    threads: usize,
    warmup_ops: u64,
    key_space: u64,
    seed: u64,
) -> Result<(), String> {
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut handles = Vec::new();
    for tid in 0..threads {
        let barrier = barrier.clone();
        let db = db.clone();
        handles.push(thread::spawn(move || {
            let mut rng = ChaCha8Rng::seed_from_u64(seed ^ ((tid as u64 + 1) << 20));
            barrier.wait();
            for _ in 0..warmup_ops {
                let key = rng.gen_range(0..key_space);
                let _ = db.get(key).unwrap();
            }
        }));
    }
    barrier.wait();
    for handle in handles {
        handle.join().map_err(|_| "warmup thread panicked".to_string())?;
    }
    Ok(())
}

fn warmup_l2p_multi_reads(
    db: &Arc<Db>,
    threads: usize,
    warmup_ops: u64,
    batch_size: usize,
    key_space: u64,
    seed: u64,
) -> Result<(), String> {
    let barrier = Arc::new(Barrier::new(threads + 1));
    let mut handles = Vec::new();
    for tid in 0..threads {
        let barrier = barrier.clone();
        let db = db.clone();
        handles.push(thread::spawn(move || {
            let mut rng = ChaCha8Rng::seed_from_u64(seed ^ ((tid as u64 + 1) << 20));
            let mut keys = vec![0u64; batch_size];
            barrier.wait();
            for _ in 0..warmup_ops {
                for key in &mut keys {
                    *key = rng.gen_range(0..key_space);
                }
                let _ = db.multi_get(&keys).unwrap();
            }
        }));
    }
    barrier.wait();
    for handle in handles {
        handle.join().map_err(|_| "warmup thread panicked".to_string())?;
    }
    Ok(())
}

fn merge_worker_results(handles: Vec<thread::JoinHandle<WorkerResult>>) -> Result<WorkerResult, String> {
    let mut merged = WorkerResult::default();
    for handle in handles {
        let result = handle.join().map_err(|_| "benchmark worker panicked".to_string())?;
        merged.ops += result.ops;
        merged.items += result.items;
        merged.latencies_us.extend(result.latencies_us);
    }
    Ok(merged)
}

fn build_report(
    cfg: &BenchConfig,
    prefill: PrefillStats,
    elapsed: std::time::Duration,
    merged: WorkerResult,
    cache_delta_opt: Option<PageCacheDelta>,
) -> BenchReport {
    let elapsed_secs = elapsed.as_secs_f64().max(f64::EPSILON);
    let latency_us = summarize_latencies(merged.latencies_us);
    BenchReport {
        scenario: cfg.scenario.as_str(),
        path: cfg.path.clone(),
        ops: merged.ops,
        items: merged.items,
        threads: cfg.threads,
        prefill_keys: prefill.keys,
        prefill_bytes: prefill.bytes,
        prefill_elapsed_secs: prefill.elapsed_secs,
        prefill_ops_per_sec: if prefill.elapsed_secs > 0.0 {
            prefill.keys as f64 / prefill.elapsed_secs
        } else {
            0.0
        },
        prefill_bytes_per_sec: if prefill.elapsed_secs > 0.0 {
            prefill.bytes as f64 / prefill.elapsed_secs
        } else {
            0.0
        },
        elapsed_secs,
        ops_per_sec: merged.ops as f64 / elapsed_secs,
        items_per_sec: merged.items as f64 / elapsed_secs,
        latency_us,
        cache_delta: cache_delta_opt,
    }
}

fn summarize_latencies(mut values: Vec<u64>) -> LatencySummary {
    if values.is_empty() {
        return LatencySummary {
            count: 0,
            avg: 0.0,
            p50: 0,
            p95: 0,
            p99: 0,
            max: 0,
        };
    }
    let sum: u128 = values.iter().map(|v| *v as u128).sum();
    values.sort_unstable();
    let count = values.len();
    LatencySummary {
        count,
        avg: sum as f64 / count as f64,
        p50: percentile(&values, 0.50),
        p95: percentile(&values, 0.95),
        p99: percentile(&values, 0.99),
        max: *values.last().unwrap(),
    }
}

fn percentile(values: &[u64], pct: f64) -> u64 {
    let idx = ((values.len() - 1) as f64 * pct).round() as usize;
    values[idx.min(values.len() - 1)]
}

fn cache_delta(before: PageCacheStats, after: PageCacheStats) -> PageCacheDelta {
    PageCacheDelta {
        hits: after.hits.saturating_sub(before.hits),
        misses: after.misses.saturating_sub(before.misses),
        evictions: after.evictions.saturating_sub(before.evictions),
        current_pages: after.current_pages,
        current_bytes: after.current_bytes,
    }
}

fn split_ops(total: u64, threads: usize) -> Vec<u64> {
    let base = total / threads as u64;
    let extra = total % threads as u64;
    (0..threads)
        .map(|tid| base + if tid < extra as usize { 1 } else { 0 })
        .collect()
}

fn default_bench_path(scenario: Scenario) -> PathBuf {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    PathBuf::from(format!(".bench/{}-{stamp}", scenario.as_str()))
}

fn effective_prefill_keys(cfg: &BenchConfig, fallback_keys: u64, bytes_per_key: u64) -> u64 {
    if cfg.prefill_keys > 0 {
        return cfg.prefill_keys;
    }
    if cfg.prefill_bytes > 0 {
        return cfg.prefill_bytes.div_ceil(bytes_per_key).max(1);
    }
    fallback_keys
}

fn l2p_value_from_pba(pba: Pba) -> L2pValue {
    let mut out = [0u8; 28];
    out[..8].copy_from_slice(&pba.to_be_bytes());
    out[8..16].copy_from_slice(&pba.rotate_left(13).to_be_bytes());
    out[16..24].copy_from_slice(&pba.rotate_left(29).to_be_bytes());
    out[24..28].copy_from_slice(&(pba as u32).to_be_bytes());
    L2pValue(out)
}

fn dedup_value_from_pba(pba: Pba) -> DedupValue {
    let mut out = [0u8; 28];
    out[..8].copy_from_slice(&pba.to_be_bytes());
    out[8..16].copy_from_slice(&pba.rotate_left(7).to_be_bytes());
    out[16..24].copy_from_slice(&pba.rotate_left(23).to_be_bytes());
    out[24..28].copy_from_slice(&(pba as u32).to_be_bytes());
    DedupValue(out)
}

fn hash_from_parts(thread: u64, seq: u64) -> Hash32 {
    let mut out = [0u8; 32];
    out[..8].copy_from_slice(&thread.to_be_bytes());
    out[8..16].copy_from_slice(&seq.to_be_bytes());
    out[16..24].copy_from_slice(&thread.rotate_left(17).to_be_bytes());
    out[24..32].copy_from_slice(&seq.rotate_left(11).to_be_bytes());
    out
}

fn parse_u64(value: Option<String>, flag: &str) -> Result<u64, String> {
    value
        .ok_or_else(|| format!("{flag} needs a value"))?
        .parse::<u64>()
        .map_err(|e| format!("{flag}: {e}"))
}

fn parse_pct(value: Option<String>, flag: &str) -> Result<u8, String> {
    let parsed = parse_u64(value, flag)?;
    if parsed > 100 {
        return Err(format!("{flag} must be in 0..=100"));
    }
    Ok(parsed as u8)
}

fn print_usage() {
    eprintln!(
        "usage: metadb-bench <l2p-prefill|l2p-put|l2p-get|l2p-multi-get|meta-tx> [--path DIR] [--reset] [--reuse-existing] [--skip-prefill] [--ops N] [--threads N] [--shards N] [--key-space N] [--prefill-keys N] [--prefill-bytes N] [--batch-size N] [--warmup-ops N] [--overwrite-pct N] [--dedup-hit-pct N] [--cache-mb N] [--seed N] [--json]"
    );
}

fn print_human(report: &BenchReport) {
    println!("scenario: {}", report.scenario);
    println!("path: {}", report.path.display());
    println!("threads: {}", report.threads);
    if report.prefill_keys > 0 {
        println!(
            "prefill: keys={} bytes={} elapsed_secs={:.3} keys_per_sec={:.2} bytes_per_sec={:.2}",
            report.prefill_keys,
            report.prefill_bytes,
            report.prefill_elapsed_secs,
            report.prefill_ops_per_sec,
            report.prefill_bytes_per_sec
        );
    }
    println!("ops: {}", report.ops);
    println!("items: {}", report.items);
    println!("elapsed_secs: {:.3}", report.elapsed_secs);
    println!("ops_per_sec: {:.2}", report.ops_per_sec);
    println!("items_per_sec: {:.2}", report.items_per_sec);
    println!(
        "latency_us: avg={:.2} p50={} p95={} p99={} max={}",
        report.latency_us.avg,
        report.latency_us.p50,
        report.latency_us.p95,
        report.latency_us.p99,
        report.latency_us.max
    );
    if let Some(cache) = &report.cache_delta {
        println!(
            "cache_delta: hits={} misses={} evictions={} current_pages={} current_bytes={}",
            cache.hits,
            cache.misses,
            cache.evictions,
            cache.current_pages,
            cache.current_bytes
        );
    }
}

fn print_json(report: &BenchReport) {
    println!("{{");
    println!("  \"scenario\": \"{}\",", report.scenario);
    println!(
        "  \"path\": \"{}\",",
        escape_json(&report.path.display().to_string())
    );
    println!("  \"threads\": {},", report.threads);
    println!("  \"prefill_keys\": {},", report.prefill_keys);
    println!("  \"prefill_bytes\": {},", report.prefill_bytes);
    println!("  \"prefill_elapsed_secs\": {:.6},", report.prefill_elapsed_secs);
    println!("  \"prefill_ops_per_sec\": {:.6},", report.prefill_ops_per_sec);
    println!(
        "  \"prefill_bytes_per_sec\": {:.6},",
        report.prefill_bytes_per_sec
    );
    println!("  \"ops\": {},", report.ops);
    println!("  \"items\": {},", report.items);
    println!("  \"elapsed_secs\": {:.6},", report.elapsed_secs);
    println!("  \"ops_per_sec\": {:.6},", report.ops_per_sec);
    println!("  \"items_per_sec\": {:.6},", report.items_per_sec);
    println!("  \"latency_us\": {{");
    println!("    \"count\": {},", report.latency_us.count);
    println!("    \"avg\": {:.6},", report.latency_us.avg);
    println!("    \"p50\": {},", report.latency_us.p50);
    println!("    \"p95\": {},", report.latency_us.p95);
    println!("    \"p99\": {},", report.latency_us.p99);
    println!("    \"max\": {}", report.latency_us.max);
    println!("  }},");
    match &report.cache_delta {
        Some(cache) => {
            println!("  \"cache_delta\": {{");
            println!("    \"hits\": {},", cache.hits);
            println!("    \"misses\": {},", cache.misses);
            println!("    \"evictions\": {},", cache.evictions);
            println!("    \"current_pages\": {},", cache.current_pages);
            println!("    \"current_bytes\": {}", cache.current_bytes);
            println!("  }}");
        }
        None => println!("  \"cache_delta\": null"),
    }
    println!("}}");
}

fn escape_json(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out
}
