use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBWithThreadMode, MergeOperands,
    MultiThreaded, Options, WriteBatch, WriteOptions,
};

type Hash32 = [u8; 32];
type Pba = u64;

const CF_L2P: &str = "l2p";
const CF_REFCOUNT: &str = "refcount";
const CF_DEDUP: &str = "dedup";
const CF_REVERSE: &str = "dedup_reverse";

fn main() -> std::process::ExitCode {
    match run() {
        Ok(code) => code,
        Err(err) => {
            if !err.is_empty() {
                eprintln!("rocksdb-bench: {err}");
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
    key_space: u64,
    prefill_keys: u64,
    prefill_bytes: u64,
    prefill_batch_size: usize,
    batch_size: usize,
    warmup_ops: u64,
    overwrite_pct: u8,
    dedup_hit_pct: u8,
    cache_bytes: usize,
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
        let mut key_space = 100_000u64;
        let mut prefill_keys = 0u64;
        let mut prefill_bytes = 0u64;
        let mut prefill_batch_size = 1024usize;
        let mut batch_size = 32usize;
        let mut warmup_ops = 10_000u64;
        let mut overwrite_pct = 30u8;
        let mut dedup_hit_pct = 20u8;
        let mut cache_bytes = 512 * 1024 * 1024usize;
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
                "--key-space" => key_space = parse_u64(args.next(), "--key-space")?,
                "--prefill-keys" => prefill_keys = parse_u64(args.next(), "--prefill-keys")?,
                "--prefill-bytes" => {
                    prefill_bytes = parse_u64(args.next(), "--prefill-bytes")?
                }
                "--prefill-batch-size" => {
                    prefill_batch_size =
                        parse_u64(args.next(), "--prefill-batch-size")? as usize
                }
                "--batch-size" => batch_size = parse_u64(args.next(), "--batch-size")? as usize,
                "--warmup-ops" => warmup_ops = parse_u64(args.next(), "--warmup-ops")?,
                "--overwrite-pct" => overwrite_pct = parse_pct(args.next(), "--overwrite-pct")?,
                "--dedup-hit-pct" => {
                    dedup_hit_pct = parse_pct(args.next(), "--dedup-hit-pct")?
                }
                "--cache-mb" => cache_bytes = parse_u64(args.next(), "--cache-mb")? as usize * 1024 * 1024,
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
            key_space: key_space.max(1),
            prefill_keys,
            prefill_bytes,
            prefill_batch_size: prefill_batch_size.max(1),
            batch_size: batch_size.max(1),
            warmup_ops,
            overwrite_pct,
            dedup_hit_pct,
            cache_bytes,
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

type RocksDb = DBWithThreadMode<MultiThreaded>;

fn run_benchmark(cfg: &BenchConfig) -> Result<BenchReport, String> {
    prepare_dir(&cfg.path, cfg.reset, cfg.reuse_existing)?;
    let db = Arc::new(open_db(&cfg.path, cfg.cache_bytes)?);
    match cfg.scenario {
        Scenario::L2pPrefill => run_l2p_prefill(cfg, db),
        Scenario::L2pPut => run_l2p_put(cfg, db),
        Scenario::L2pGet => run_l2p_get(cfg, db),
        Scenario::L2pMultiGet => run_l2p_multi_get(cfg, db),
        Scenario::MetaTx => run_meta_tx(cfg, db),
    }
}

fn run_l2p_prefill(cfg: &BenchConfig, db: Arc<RocksDb>) -> Result<BenchReport, String> {
    let prefill_keys = effective_prefill_keys(cfg, cfg.key_space.max(cfg.ops).max(1), 28);
    let prefill = prefill_l2p(&db, prefill_keys, cfg.prefill_batch_size)?;
    Ok(build_report(
        cfg,
        prefill,
        Duration::from_secs(0),
        WorkerResult::default(),
    ))
}

fn run_l2p_put(cfg: &BenchConfig, db: Arc<RocksDb>) -> Result<BenchReport, String> {
    let barrier = Arc::new(Barrier::new(cfg.threads + 1));
    let mut handles = Vec::with_capacity(cfg.threads);
    let ops_per_thread = split_ops(cfg.ops, cfg.threads);
    let key_stride = cfg.ops.max(1);
    for (tid, thread_ops) in ops_per_thread.into_iter().enumerate() {
        let db = db.clone();
        let barrier = barrier.clone();
        let key_base = tid as u64 * key_stride;
        handles.push(thread::spawn(move || {
            let mut out = WorkerResult::default();
            let cf = db.cf_handle(CF_L2P).unwrap();
            let opts = write_opts_sync();
            barrier.wait();
            for i in 0..thread_ops {
                let key = lba_key(key_base + i);
                let value = l2p_value_from_pba(key_base + i);
                let start = Instant::now();
                db.put_cf_opt(&cf, key, value, &opts).unwrap();
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
    Ok(build_report(cfg, PrefillStats::none(), elapsed, merged))
}

fn run_l2p_get(cfg: &BenchConfig, db: Arc<RocksDb>) -> Result<BenchReport, String> {
    let prefill_keys = effective_prefill_keys(cfg, cfg.key_space.max(cfg.ops), 28);
    let prefill = if cfg.skip_prefill {
        PrefillStats::none()
    } else {
        prefill_l2p(&db, prefill_keys, cfg.prefill_batch_size)?
    };
    if cfg.warmup_ops > 0 {
        warmup_l2p_reads(&db, cfg.threads, cfg.warmup_ops, cfg.key_space, cfg.seed)?;
    }

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
            let cf = db.cf_handle(CF_L2P).unwrap();
            barrier.wait();
            for _ in 0..thread_ops {
                let key_num = rng.gen_range(0..key_space);
                let start = Instant::now();
                let got = db.get_pinned_cf(&cf, lba_key(key_num)).unwrap();
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
    Ok(build_report(cfg, prefill, elapsed, merged))
}

fn run_l2p_multi_get(cfg: &BenchConfig, db: Arc<RocksDb>) -> Result<BenchReport, String> {
    let total_items = cfg.ops.saturating_mul(cfg.batch_size as u64);
    let prefill_keys = effective_prefill_keys(cfg, cfg.key_space.max(total_items), 28);
    let prefill = if cfg.skip_prefill {
        PrefillStats::none()
    } else {
        prefill_l2p(&db, prefill_keys, cfg.prefill_batch_size)?
    };
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
            let cf = db.cf_handle(CF_L2P).unwrap();
            let mut keys: Vec<Vec<u8>> = vec![vec![0u8; 8]; batch_size];
            barrier.wait();
            for _ in 0..thread_ops {
                for key in &mut keys {
                    *key = lba_key(rng.gen_range(0..key_space)).to_vec();
                }
                let start = Instant::now();
                let got = db.multi_get_cf(keys.iter().map(|k| (&cf, k.as_slice())));
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
    Ok(build_report(cfg, prefill, elapsed, merged))
}

fn run_meta_tx(cfg: &BenchConfig, db: Arc<RocksDb>) -> Result<BenchReport, String> {
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
            let l2p_cf = db.cf_handle(CF_L2P).unwrap();
            let refcount_cf = db.cf_handle(CF_REFCOUNT).unwrap();
            let dedup_cf = db.cf_handle(CF_DEDUP).unwrap();
            let reverse_cf = db.cf_handle(CF_REVERSE).unwrap();
            let opts = write_opts_sync();
            let working_set = (thread_cfg.key_space / thread_cfg.threads as u64).max(1) as usize;
            let lba_base = tid as u64 * working_set as u64;
            let mut slots: Vec<Option<Mapping>> = vec![None; working_set];
            let mut free_slots: Vec<usize> = (0..working_set).collect();
            let mut used_slots: Vec<usize> = Vec::new();
            let mut dedup_entries: Vec<DedupEntry> = Vec::new();
            let mut active_dedup: Vec<usize> = Vec::new();
            let mut next_pba: u64 = ((tid as u64) << 48) | 1;
            let mut next_hash_seq: u64 = 1;

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
                let mut batch = WriteBatch::default();

                if let Some(old) = slots[slot_idx].take() {
                    batch.merge_cf(&refcount_cf, pba_key(old.pba), delta_operand(-1));
                    let old_entry = &mut dedup_entries[old.dedup_idx];
                    old_entry.live_refs -= 1;
                    if old_entry.live_refs == 0 {
                        batch.delete_cf(&dedup_cf, old_entry.hash);
                        batch.delete_cf(&reverse_cf, reverse_key(old_entry.pba, &old_entry.hash));
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
                    batch.put_cf(&l2p_cf, lba_key(lba), l2p_value_from_pba(entry.pba));
                    batch.merge_cf(&refcount_cf, pba_key(entry.pba), delta_operand(1));
                    Mapping {
                        pba: entry.pba,
                        dedup_idx: idx,
                    }
                } else {
                    let pba = next_pba;
                    next_pba += 1;
                    let hash = hash_from_parts(tid as u64, next_hash_seq);
                    next_hash_seq += 1;
                    batch.put_cf(&l2p_cf, lba_key(lba), l2p_value_from_pba(pba));
                    batch.merge_cf(&refcount_cf, pba_key(pba), delta_operand(1));
                    batch.put_cf(&dedup_cf, hash, dedup_value_from_pba(pba));
                    batch.put_cf(&reverse_cf, reverse_key(pba, &hash), reverse_value(&hash));
                    dedup_entries.push(DedupEntry {
                        pba,
                        hash,
                        live_refs: 1,
                    });
                    let idx = dedup_entries.len() - 1;
                    active_dedup.push(idx);
                    Mapping { pba, dedup_idx: idx }
                };

                let start = Instant::now();
                db.write_opt(batch, &opts).unwrap();
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
    Ok(build_report(cfg, PrefillStats::none(), elapsed, merged))
}

fn open_db(path: &Path, cache_bytes: usize) -> Result<RocksDb, String> {
    let mut db_opts = Options::default();
    db_opts.create_if_missing(true);
    db_opts.create_missing_column_families(true);
    db_opts.set_keep_log_file_num(4);

    let cache = Cache::new_lru_cache(cache_bytes);
    let mut block_opts = BlockBasedOptions::default();
    block_opts.set_block_size(4096);
    block_opts.set_block_cache(&cache);
    block_opts.set_cache_index_and_filter_blocks(true);

    let mut default_cf = Options::default();
    default_cf.set_block_based_table_factory(&block_opts);

    let mut refcount_cf = Options::default();
    refcount_cf.set_block_based_table_factory(&block_opts);
    refcount_cf.set_merge_operator("refcount_sum", refcount_full_merge, refcount_partial_merge);

    let descriptors = vec![
        ColumnFamilyDescriptor::new("default", Options::default()),
        ColumnFamilyDescriptor::new(CF_L2P, default_cf.clone()),
        ColumnFamilyDescriptor::new(CF_REFCOUNT, refcount_cf),
        ColumnFamilyDescriptor::new(CF_DEDUP, default_cf.clone()),
        ColumnFamilyDescriptor::new(CF_REVERSE, default_cf),
    ];

    RocksDb::open_cf_descriptors(&db_opts, path, descriptors).map_err(|e| e.to_string())
}

fn prefill_l2p(db: &RocksDb, count: u64, batch_size: usize) -> Result<PrefillStats, String> {
    let cf = db.cf_handle(CF_L2P).unwrap();
    let opts = write_opts_sync();
    let start = Instant::now();
    let progress_every = progress_interval(count);
    let mut next_progress = progress_every;
    let mut done = 0u64;
    while done < count {
        let end = (done + batch_size as u64).min(count);
        let mut batch = WriteBatch::default();
        for key in done..end {
            batch.put_cf(&cf, lba_key(key), l2p_value_from_pba(key));
        }
        db.write_opt(batch, &opts).map_err(|e| e.to_string())?;
        done = end;
        if progress_every > 0 && (done >= next_progress || done == count) {
            print_prefill_progress("rocksdb", done, count, start.elapsed());
            while next_progress <= done {
                next_progress = next_progress.saturating_add(progress_every);
            }
        }
    }
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

fn print_prefill_progress(backend: &str, done: u64, total: u64, elapsed: Duration) {
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
    db: &Arc<RocksDb>,
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
            let cf = db.cf_handle(CF_L2P).unwrap();
            barrier.wait();
            for _ in 0..warmup_ops {
                let _ = db.get_pinned_cf(&cf, lba_key(rng.gen_range(0..key_space))).unwrap();
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
    db: &Arc<RocksDb>,
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
            let cf = db.cf_handle(CF_L2P).unwrap();
            let mut keys: Vec<Vec<u8>> = vec![vec![0u8; 8]; batch_size];
            barrier.wait();
            for _ in 0..warmup_ops {
                for key in &mut keys {
                    *key = lba_key(rng.gen_range(0..key_space)).to_vec();
                }
                let _ = db.multi_get_cf(keys.iter().map(|k| (&cf, k.as_slice())));
            }
        }));
    }
    barrier.wait();
    for handle in handles {
        handle.join().map_err(|_| "warmup thread panicked".to_string())?;
    }
    Ok(())
}

fn refcount_full_merge(
    _key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let base: i64 = match existing_val {
        Some(v) if v.len() == 4 => i32::from_be_bytes(v.try_into().ok()?) as i64,
        Some(_) => return None,
        None => 0,
    };
    let mut total = base;
    for op in operands {
        if op.len() != 4 {
            return None;
        }
        total += i32::from_be_bytes(op.try_into().ok()?) as i64;
    }
    Some((total.max(0) as i32).to_be_bytes().to_vec())
}

fn refcount_partial_merge(
    _key: &[u8],
    _existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>> {
    let mut total = 0i32;
    for op in operands {
        if op.len() != 4 {
            return None;
        }
        total = total.saturating_add(i32::from_be_bytes(op.try_into().ok()?));
    }
    Some(total.to_be_bytes().to_vec())
}

fn write_opts_sync() -> WriteOptions {
    let mut opts = WriteOptions::default();
    opts.set_sync(true);
    opts
}

fn lba_key(lba: u64) -> [u8; 8] {
    lba.to_be_bytes()
}

fn pba_key(pba: u64) -> [u8; 8] {
    pba.to_be_bytes()
}

fn delta_operand(delta: i32) -> [u8; 4] {
    delta.to_be_bytes()
}

fn reverse_key(pba: u64, hash: &Hash32) -> [u8; 32] {
    let mut key = [0u8; 32];
    key[..8].copy_from_slice(&pba.to_be_bytes());
    key[8..32].copy_from_slice(&hash[..24]);
    key
}

fn reverse_value(hash: &Hash32) -> [u8; 8] {
    let mut value = [0u8; 8];
    value.copy_from_slice(&hash[24..32]);
    value
}

fn prepare_dir(path: &Path, reset: bool, _reuse_existing: bool) -> Result<(), String> {
    if reset && path.exists() {
        fs::remove_dir_all(path).map_err(|e| format!("remove {}: {e}", path.display()))?;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
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
    elapsed: Duration,
    merged: WorkerResult,
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
    PathBuf::from(format!(".bench/rocksdb-{}-{stamp}", scenario.as_str()))
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

fn l2p_value_from_pba(pba: Pba) -> [u8; 28] {
    let mut out = [0u8; 28];
    out[..8].copy_from_slice(&pba.to_be_bytes());
    out[8..16].copy_from_slice(&pba.rotate_left(13).to_be_bytes());
    out[16..24].copy_from_slice(&pba.rotate_left(29).to_be_bytes());
    out[24..28].copy_from_slice(&(pba as u32).to_be_bytes());
    out
}

fn dedup_value_from_pba(pba: Pba) -> [u8; 28] {
    let mut out = [0u8; 28];
    out[..8].copy_from_slice(&pba.to_be_bytes());
    out[8..16].copy_from_slice(&pba.rotate_left(7).to_be_bytes());
    out[16..24].copy_from_slice(&pba.rotate_left(23).to_be_bytes());
    out[24..28].copy_from_slice(&(pba as u32).to_be_bytes());
    out
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
        "usage: rocksdb-bench <l2p-prefill|l2p-put|l2p-get|l2p-multi-get|meta-tx> [--path DIR] [--reset] [--reuse-existing] [--skip-prefill] [--ops N] [--threads N] [--key-space N] [--prefill-keys N] [--prefill-bytes N] [--prefill-batch-size N] [--batch-size N] [--warmup-ops N] [--overwrite-pct N] [--dedup-hit-pct N] [--cache-mb N] [--seed N] [--json]"
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
    println!("  }}");
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
