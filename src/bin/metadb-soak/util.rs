fn open_or_create_with_faults(
    path: &Path,
    faults: Arc<FaultController>,
) -> onyx_metadb::Result<Arc<Db>> {
    // BFG: let the soak drive the background livelist-condense
    // worker at a low segment threshold so a short run actually exercises it
    // concurrently with flush/drop/promote (the default is 16; a short soak
    // rarely grows a single clone chain that far). Unset = engine default.
    let mut cfg = onyx_metadb::Config::new(path);
    // BFG gate: lower the L2P shard count so more live volumes
    // fit the single-page manifest (the capacity wall caps ~3 volumes at the
    // default 16 shards). With `MAX_LIVE_VOLUMES=4` binding instead, clone /
    // promote / drop churn actually gets dense — required to stress the clone
    // COW-kill operand. Only consulted at CREATE (reopen reads the shard count
    // from the manifest), and the parent's create env is inherited by every
    // child. Unset = engine default (16).
    if let Ok(raw) = std::env::var("METADB_SOAK_SHARDS") {
        if let Ok(n) = raw.parse::<u32>() {
            if n >= 1 {
                cfg.shards_per_partition = n;
            }
        }
    }
    if let Ok(raw) = std::env::var("METADB_SOAK_LIVELIST_CONDENSE_MIN_SEGMENTS") {
        if let Ok(n) = raw.parse::<usize>() {
            cfg.livelist_condense_min_segments = n;
        }
    }
    // Cuckoo online-resize gate: a SMALL initial modulus so the resizer thread
    // (see `spawn_cuckoo_resizer`) actually crosses the grow watermark under the
    // onyx-mix dedup keyspace and drives real two-table migrations against the
    // reference model. Only consulted at CREATE (reopen reads the modulus from
    // the cuckoo meta head); the parent's create env is inherited by children,
    // so a mid-migration child restart resumes the same Growing db. Unset =
    // engine default (1M), which never resizes under the soak keyspace.
    if let Ok(raw) = std::env::var("METADB_SOAK_CUCKOO_BUCKETS") {
        if let Ok(n) = raw.parse::<u64>() {
            if n >= 1 {
                cfg.dedup_cuckoo_buckets = n;
            }
        }
    }
    // BFG gate: the soak defaults to direct-fold
    // (l2p_buffer_enabled=false), but every onyx/production config runs BUFFER
    // mode (l2p_buffer_enabled=true; nvme-detailed.toml etc.). Setting
    // METADB_SOAK_L2P_BUFFER=1 makes the soak match production so the free-set
    // deadlist/reachability flip is validated in the mode it actually ships in.
    // A short max-interval keeps the compactor folding periodically so deaths
    // are sealed (and crash windows exercised) even at low write rates.
    if matches!(std::env::var("METADB_SOAK_L2P_BUFFER").as_deref(), Ok("1") | Ok("true")) {
        cfg.l2p_buffer_enabled = true;
        if let Ok(raw) = std::env::var("METADB_SOAK_L2P_BUFFER_MAX_INTERVAL_MS") {
            if let Ok(n) = raw.parse::<u64>() {
                cfg.l2p_buffer_max_interval_ms = n;
            }
        }
    }
    // Production (nvme-detailed.toml) runs bfg_threads_enabled=true: the fold is
    // the background BfgSyncThread's `drain_syncing_slot_into_trees`, not the
    // inline `force_compact_l2p_buffers`. The soak defaults to threads-OFF, so
    // the production fold path is otherwise never crash/restart-exercised.
    // METADB_SOAK_BFG_THREADS=1 closes that coverage gap (a hard merge gate for
    // the free-set flip alongside the threads-OFF run).
    if matches!(std::env::var("METADB_SOAK_BFG_THREADS").as_deref(), Ok("1") | Ok("true")) {
        cfg.bfg_threads_enabled = true;
    }
    // take_snapshot deadlock fix (commit 6cc1bae) gate: the bg LineageGcWorker
    // drives `commit_free_pbas` (a read-before-enter commit path) autonomously
    // — the production-reachable counterparty to take_snapshot's force-sync
    // `drop_gate.write`. The soak defaults to `lineage_gc_enabled=false`, so
    // commit_free_pbas never fires and the lock-order deadlock window stays shut
    // (exactly why the 8h soak missed the deadlock). METADB_SOAK_LINEAGE_GC=1
    // turns it on with a short interval so it overlaps the snapshot/promote
    // churn; pair with METADB_SOAK_BFG_THREADS=1 for the bg quiesce/sync path
    // that take_snapshot's forced sync drives.
    if matches!(std::env::var("METADB_SOAK_LINEAGE_GC").as_deref(), Ok("1") | Ok("true")) {
        cfg.lineage_gc_enabled = true;
        cfg.lineage_gc_interval_ms = std::env::var("METADB_SOAK_LINEAGE_GC_INTERVAL_MS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(5);
    }
    // v27 L3: METADB_SOAK_RC_DELTA_PERSIST=1 makes delta-run segments the durable
    // refcount representation (append + condense + condense-on-open). Requires
    // the streaming BFG checkpoint, so it force-enables bfg threads + streaming.
    // A small METADB_SOAK_RC_CONDENSE_INTERVAL (default 4) exercises frequent
    // condense + directory drain across the restart/fault matrix — the gate for
    // this feature (`audit_pba_refcounts` under-count is FATAL = premature free).
    if matches!(
        std::env::var("METADB_SOAK_RC_DELTA_PERSIST").as_deref(),
        Ok("1") | Ok("true")
    ) {
        cfg.bfg_threads_enabled = true;
        cfg.rc_checkpoint_streaming_enabled = true;
        cfg.rc_delta_run_persist_enabled = true;
        cfg.rc_condense_interval_cycles = std::env::var("METADB_SOAK_RC_CONDENSE_INTERVAL")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .filter(|&k| k >= 1)
            .unwrap_or(4);
    }
    match Db::open_with_config_and_faults(cfg.clone(), faults.clone()) {
        Ok(db) => Ok(db),
        Err(_) => Db::create_with_config_and_faults(cfg, faults),
    }
}

fn parse_fault_point(raw: &str) -> Result<FaultPoint, String> {
    match raw {
        "wal.fsync.before" => Ok(FaultPoint::WalFsyncBefore),
        "manifest.fsync.before" => Ok(FaultPoint::ManifestFsyncBefore),
        "flush.level_rewrite.before_manifest" => {
            Ok(FaultPoint::FlushPostLevelRewriteBeforeManifest)
        }
        "rc_delta_run.post_seg_write.before_manifest" => {
            Ok(FaultPoint::RcDeltaRunPostSegWriteBeforeManifest)
        }
        "rc_delta_run.post_manifest.before_free" => {
            Ok(FaultPoint::RcDeltaRunPostManifestBeforeFree)
        }
        "rc_condense.post_write.before_manifest" => {
            Ok(FaultPoint::RcCondensePostWriteBeforeManifest)
        }
        "rc_condense.post_manifest.before_free" => {
            Ok(FaultPoint::RcCondensePostManifestBeforeFree)
        }
        other => Err(format!("unsupported fault point `{other}`")),
    }
}

fn parse_fault_action(raw: &str) -> Result<FaultAction, String> {
    match raw {
        "error" => Ok(FaultAction::Error),
        "panic" => Ok(FaultAction::Panic),
        other => Err(format!("unsupported fault action `{other}`")),
    }
}

fn parse_worker_op<'a, I>(mut parts: I) -> Result<WorkerOp, String>
where
    I: Iterator<Item = &'a str>,
{
    let tid = parse_part_u64(parts.next(), "worker tid")? as usize;
    let kind = parts
        .next()
        .ok_or_else(|| "missing worker kind".to_string())?;
    let vol_ord = parse_part_u64(parts.next(), "worker vol_ord")? as VolumeOrdinal;
    let slot = parse_part_u64(parts.next(), "worker slot")?;
    let arg = parse_part_u64(parts.next(), "worker arg")?;
    let kind = match kind {
        "insert" => WorkerOpKind::Insert(arg as u8),
        "delete" => WorkerOpKind::Delete,
        "put_dedup" => WorkerOpKind::PutDedup(arg as u8),
        "delete_dedup" => WorkerOpKind::DeleteDedup,
        "get" => WorkerOpKind::Get,
        "onyx_remap" => WorkerOpKind::OnyxRemap {
            pba: arg,
            salt: parse_part_u64(parts.next(), "onyx salt")?,
            guard: parse_part_u64(parts.next(), "onyx guard")? as u8,
        },
        "onyx_range_delete" => WorkerOpKind::OnyxRangeDelete { len: arg },
        "onyx_dedup_hit" => WorkerOpKind::OnyxDedupHit {
            pba: arg,
            salt: parse_part_u64(parts.next(), "onyx salt")?,
        },
        "onyx_cleanup" => WorkerOpKind::OnyxCleanup { pba: arg },
        other => return Err(format!("unknown worker kind `{other}`")),
    };
    Ok(WorkerOp {
        tid,
        vol_ord,
        slot,
        kind,
    })
}

fn worker_kind_name(kind: &WorkerOpKind) -> &'static str {
    match kind {
        WorkerOpKind::Insert(_) => "insert",
        WorkerOpKind::Delete => "delete",
        WorkerOpKind::PutDedup(_) => "put_dedup",
        WorkerOpKind::DeleteDedup => "delete_dedup",
        WorkerOpKind::Get => "get",
        WorkerOpKind::OnyxRemap { .. } => "onyx_remap",
        WorkerOpKind::OnyxRangeDelete { .. } => "onyx_range_delete",
        WorkerOpKind::OnyxDedupHit { .. } => "onyx_dedup_hit",
        WorkerOpKind::OnyxCleanup { .. } => "onyx_cleanup",
    }
}

fn worker_kind_arg(kind: &WorkerOpKind) -> String {
    match kind {
        WorkerOpKind::Insert(byte) | WorkerOpKind::PutDedup(byte) => byte.to_string(),
        WorkerOpKind::OnyxRemap { pba, salt, guard } => format!("{pba} {salt} {guard}"),
        WorkerOpKind::OnyxRangeDelete { len } => len.to_string(),
        WorkerOpKind::OnyxDedupHit { pba, salt } => format!("{pba} {salt}"),
        WorkerOpKind::OnyxCleanup { pba } => pba.to_string(),
        _ => "0".into(),
    }
}

fn parse_part_u64(part: Option<&str>, label: &str) -> Result<u64, String> {
    part.ok_or_else(|| format!("missing {label}"))?
        .parse::<u64>()
        .map_err(|e| format!("{label}: {e}"))
}

fn parse_u64(value: Option<String>, flag: &str) -> Result<u64, String> {
    value
        .ok_or_else(|| format!("{flag} needs a value"))?
        .parse::<u64>()
        .map_err(|e| format!("{flag}: {e}"))
}

fn parse_duration_arg(raw: String) -> Result<u64, String> {
    if let Some(num) = raw.strip_suffix('h') {
        return num
            .parse::<u64>()
            .map(|v| v.saturating_mul(3600))
            .map_err(|e| e.to_string());
    }
    if let Some(num) = raw.strip_suffix('m') {
        return num
            .parse::<u64>()
            .map(|v| v.saturating_mul(60))
            .map_err(|e| e.to_string());
    }
    if let Some(num) = raw.strip_suffix('s') {
        return num.parse::<u64>().map_err(|e| e.to_string());
    }
    raw.parse::<u64>().map_err(|e| e.to_string())
}

fn l2p_key(tid: usize, slot: u64) -> u64 {
    // Dense per-thread band: each thread owns
    // [tid*KEY_SLOTS_PER_THREAD, (tid+1)*KEY_SLOTS_PER_THREAD), so the
    // reference model stays collision-free per (tid, slot) while the L2P
    // key space stays CONTIGUOUS. The old `(tid<<32)|slot` scattered keys
    // across a ~4-billion-LBA-per-thread space, which deterministically
    // trips the L2P leaf compaction codec ("compact leaf unit_count
    // exceeds payload capacity" → bogus multi-PB allocation → abort).
    // onyx's real LBAs are dense `0..size/4K`, so the sparse scheme was a
    // soak-tooling artifact, not a metadb bug. `slot < KEY_SLOTS_PER_THREAD`
    // by construction (generators use `gen_range(0..KEY_SLOTS_PER_THREAD)`),
    // so the bands never overlap.
    (tid as u64) * KEY_SLOTS_PER_THREAD + slot
}

fn dedup_hash(tid: usize, slot: u64) -> Hash8 {
    // `Hash8` is 8 bytes; the prior tid(8) || slot(8) 16-byte layout predates
    // the Hash8 shrink and panicked (`hash[8..16]` out of range), which is why
    // the legacy workload had gone unused. Pack tid into the high bits + slot
    // into the low 40 so each (tid, slot) stays a distinct dedup model key.
    let mixed = ((tid as u64) << 40) ^ (slot & 0x00FF_FFFF_FFFF);
    mixed.to_be_bytes()
}

fn refcount_pba(tid: usize, slot: u64) -> u64 {
    ((tid as u64) << 24) | slot
}

// head_pba bands for the value helpers. PBAs must be DENSE (within a few
// hundred of each other so the L2P leaf codec's u32 `pba_delta` and the
// PBA-indexed flush arrays stay small) AND must NOT collide with the
// `refcount_pba(tid, slot) = (tid<<24)|slot` keyspace the Legacy verify
// audits — that audit expects 0 for every `(tid<<24)|slot` (the Legacy
// model tracks no refcount), and dedup-put DOES incref a value's head_pba
// in the DB. `(tid<<24)|slot` always has bits [8..24) == 0, so any band
// that sets a bit in [8..24) is provably disjoint from it. The old scheme
// hid this by scattering head_pbas to byte·2^56 (outside the audit window
// but huge → codec/alloc blowups, the actual event-A crashes).
const L2P_PBA_BAND: u64 = 1 << 9; // 512..767  (bit 9 set)
const DEDUP_PBA_BAND: u64 = 1 << 8; // 256..511 (bit 8 set)

fn l2p_value(byte: u8) -> L2pValue {
    let mut value = [0u8; onyx_metadb::paged::format::LEAF_VALUE_SIZE];
    // PBA = head 8 bytes (big-endian). Dense band (512 + byte), disjoint
    // from both the refcount-audit window and the dedup band.
    value[0..8].copy_from_slice(&(L2P_PBA_BAND + byte as u64).to_be_bytes());
    // Pin the birth_lsn trailer non-zero so the apply-time birth stamp
    // (which only replaces the 0 sentinel) leaves the value bytes intact
    // for the reference-model round-trip compare.
    value[onyx_metadb::paged::format::LEAF_VALUE_SIZE - 1] = 1;
    L2pValue(value)
}

fn dedup_value(byte: u8) -> DedupValue {
    let mut value = [0u8; 28];
    // head_pba = first 8 bytes (big-endian). Dense band (256 + byte),
    // disjoint from the refcount-audit window and the L2P band. The old
    // `value[0]=byte` put the byte in the HIGH byte → head_pba ≈ byte·2^56,
    // which blew up a PBA-indexed structure during flush ("memory
    // allocation of N bytes failed").
    value[0..8].copy_from_slice(&(DEDUP_PBA_BAND + byte as u64).to_be_bytes());
    // Non-PBA entropy for value distinctness.
    value[8] = byte.wrapping_mul(7);
    DedupValue(value)
}

struct EventLog {
    writer: BufWriter<std::fs::File>,
}

impl EventLog {
    fn open(path: &Path) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    fn write(&mut self, kind: &str, detail: &str) -> Result<(), String> {
        writeln!(
            self.writer,
            "{{\"kind\":\"{}\",\"detail\":\"{}\"}}",
            escape_json(kind),
            escape_json(detail)
        )
        .map_err(|e| e.to_string())?;
        self.writer.flush().map_err(|e| e.to_string())
    }
}

fn write_summary(path: &Path, summary: &Summary) -> std::io::Result<()> {
    let json = format!(
        concat!(
            "{{\n",
            "  \"path\": \"{}\",\n",
            "  \"duration_secs\": {},\n",
            "  \"cycles\": {},\n",
            "  \"ops\": {},\n",
            "  \"restarts\": {},\n",
            "  \"verifies\": {},\n",
            "  \"fault_cycles\": {},\n",
            "  \"onyx_ops\": {},\n",
            "  \"guard_hit\": {},\n",
            "  \"guard_miss\": {},\n",
            "  \"freed_pbas\": {},\n",
            "  \"cleanup_deleted\": {},\n",
            "  \"refcount_sum_mismatches\": {},\n",
            "  \"deadlock_detected\": {},\n",
            "  \"success\": {},\n",
            "  \"last_error\": {}\n",
            "}}\n"
        ),
        escape_json(&summary.path.display().to_string()),
        summary.duration_secs,
        summary.cycles,
        summary.ops,
        summary.restarts,
        summary.verifies,
        summary.fault_cycles,
        summary.onyx_ops,
        summary.guard_hit,
        summary.guard_miss,
        summary.freed_pbas,
        summary.cleanup_deleted,
        summary.refcount_sum_mismatches,
        summary.deadlock_detected,
        summary.success,
        match &summary.last_error {
            Some(err) => format!("\"{}\"", escape_json(err)),
            None => "null".into(),
        }
    );
    std::fs::write(path, json)
}

fn print_parent_usage() {
    eprintln!(
        "usage: metadb-soak <path> [--duration-secs N|--minutes N|--hours N] [--restart-interval 2h] [--legacy-mix|--onyx-mix|--onyx-concurrent-mix] [--ops-per-cycle N] [--pipeline-depth N] [--threads N] [--cleanup-batch-size N] [--onyx-max-pba N] [--metrics path] [--metrics-interval-secs N] [--seed N] [--fault-density-pct N] [--summary path] [--events path] [--events-summary|--events-ops] [--no-snapshots]"
    );
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
