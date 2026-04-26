fn open_or_create_with_faults(
    path: &Path,
    faults: Arc<FaultController>,
) -> onyx_metadb::Result<Db> {
    match Db::open_with_faults(path, faults.clone()) {
        Ok(db) => Ok(db),
        Err(_) => Db::create_with_faults(path, faults),
    }
}

fn parse_fault_point(raw: &str) -> Result<FaultPoint, String> {
    match raw {
        "wal.fsync.before" => Ok(FaultPoint::WalFsyncBefore),
        "manifest.fsync.before" => Ok(FaultPoint::ManifestFsyncBefore),
        "flush.level_rewrite.before_manifest" => {
            Ok(FaultPoint::FlushPostLevelRewriteBeforeManifest)
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
        "incref" => WorkerOpKind::Incref,
        "decref" => WorkerOpKind::Decref,
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
        WorkerOpKind::Incref => "incref",
        WorkerOpKind::Decref => "decref",
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
    ((tid as u64) << 32) | slot
}

fn dedup_hash(tid: usize, slot: u64) -> Hash32 {
    let mut hash = [0u8; 32];
    hash[..8].copy_from_slice(&(tid as u64).to_be_bytes());
    hash[8..16].copy_from_slice(&slot.to_be_bytes());
    hash
}

fn refcount_pba(tid: usize, slot: u64) -> u64 {
    ((tid as u64) << 24) | slot
}

fn l2p_value(byte: u8) -> L2pValue {
    let mut value = [0u8; 28];
    value[0] = byte;
    value[1] = byte.wrapping_mul(3);
    L2pValue(value)
}

fn dedup_value(byte: u8) -> DedupValue {
    let mut value = [0u8; 28];
    value[0] = byte;
    value[1] = byte.wrapping_mul(7);
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
        "usage: metadb-soak <path> [--duration-secs N|--minutes N|--hours N] [--restart-interval 2h] [--legacy-mix|--onyx-mix|--onyx-concurrent-mix] [--ops-per-cycle N] [--pipeline-depth N] [--threads N] [--metrics path] [--metrics-interval-secs N] [--seed N] [--fault-density-pct N] [--summary path] [--events path] [--events-summary|--events-ops] [--no-snapshots]"
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
