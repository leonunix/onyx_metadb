//! `metadb-dump` — point queries + range scans + manifest / snapshot
//! listings for an on-disk metadb. Opens the database read-mostly (the
//! Db constructor may fold legacy-format manifest upgrades, same as
//! `metadb-verify`), reads the requested state, and prints human-
//! readable or `--json` output.
//!
//! Intended use: Phase 7 adapter debugging ("did this LBA actually
//! land?", "what does the manifest say about snapshot 3?") — not a
//! bulk exporter. LSM full-scans are deliberately out of scope for v1.

use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use onyx_metadb::{
    Db, DedupValue, Hash32, L2pValue, Lba, Manifest, PageId, Pba, SnapshotEntry, SnapshotId,
};

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(err) => {
            if !err.is_empty() {
                eprintln!("metadb-dump: {err}");
            }
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<ExitCode, String> {
    let mut args = env::args().skip(1);
    let Some(sub) = args.next() else {
        print_usage();
        return Ok(ExitCode::from(2));
    };
    if sub == "-h" || sub == "--help" {
        print_usage();
        return Ok(ExitCode::SUCCESS);
    }

    let rest: Vec<String> = args.collect();
    match sub.as_str() {
        "manifest" => cmd_manifest(&rest),
        "lba" => cmd_lba(&rest),
        "l2p" => cmd_l2p(&rest),
        "refcount" => cmd_refcount(&rest),
        "dedup" => cmd_dedup(&rest),
        "dedup-reverse" => cmd_dedup_reverse(&rest),
        "snapshots" => cmd_snapshots(&rest),
        _ => {
            print_usage();
            Err(format!("unknown subcommand `{sub}`"))
        }
    }
}

fn print_usage() {
    eprintln!(
        "usage: metadb-dump <subcommand> <path> [args...] [--json]

subcommands:
  manifest       <path>                         decoded manifest + tree roots
  lba            <path> <lba> [--snapshot ID]   point lookup in L2P
  l2p            <path> [--from LBA] [--to LBA] [--snapshot ID] [--limit N]
                                                range scan over L2P
  refcount       <path> <pba>                   point lookup for PBA refcount
  dedup          <path> <hash-hex>              point lookup in dedup_index
  dedup-reverse  <path> <pba>                   prefix scan dedup_reverse by PBA
  snapshots      <path>                         list registered snapshots"
    );
}

// -------- manifest -------------------------------------------------------

fn cmd_manifest(args: &[String]) -> Result<ExitCode, String> {
    let Parsed { path, json, .. } = parse_flags(args, ParseSpec::path_only())?;
    let db = open_db(&path)?;
    let m = db.manifest();
    let high_water = db.high_water();
    let last_applied = db.last_applied_lsn();
    if json {
        print_manifest_json(&path, &m, high_water, last_applied);
    } else {
        print_manifest_human(&path, &m, high_water, last_applied);
    }
    Ok(ExitCode::SUCCESS)
}

fn print_manifest_human(path: &Path, m: &Manifest, high_water: u64, last_applied: u64) {
    println!("path: {}", path.display());
    println!("body_version: {}", m.body_version);
    println!("checkpoint_lsn: {}", m.checkpoint_lsn);
    println!("last_applied_lsn: {last_applied}");
    println!("high_water: {high_water}");
    println!("free_list_head: {}", fmt_page(m.free_list_head));
    println!("shard_roots ({}):", m.shard_roots.len());
    for (i, p) in m.shard_roots.iter().enumerate() {
        println!("  [{i}] {}", fmt_page(*p));
    }
    println!("refcount_shard_roots ({}):", m.refcount_shard_roots.len());
    for (i, p) in m.refcount_shard_roots.iter().enumerate() {
        println!("  [{i}] {}", fmt_page(*p));
    }
    println!("dedup_level_heads ({}):", m.dedup_level_heads.len());
    for (i, p) in m.dedup_level_heads.iter().enumerate() {
        println!("  L{i}: {}", fmt_page(*p));
    }
    println!(
        "dedup_reverse_level_heads ({}):",
        m.dedup_reverse_level_heads.len()
    );
    for (i, p) in m.dedup_reverse_level_heads.iter().enumerate() {
        println!("  L{i}: {}", fmt_page(*p));
    }
    println!("next_snapshot_id: {}", m.next_snapshot_id);
    println!("snapshots: {}", m.snapshots.len());
    for entry in &m.snapshots {
        print_snapshot_entry_human(entry, "  ");
    }
}

fn print_manifest_json(path: &Path, m: &Manifest, high_water: u64, last_applied: u64) {
    println!("{{");
    println!(
        "  \"path\": \"{}\",",
        escape_json(&path.display().to_string())
    );
    println!("  \"body_version\": {},", m.body_version);
    println!("  \"checkpoint_lsn\": {},", m.checkpoint_lsn);
    println!("  \"last_applied_lsn\": {last_applied},");
    println!("  \"high_water\": {high_water},");
    println!("  \"free_list_head\": {},", page_json(m.free_list_head));
    println!("  \"shard_roots\": {},", page_array_json(&m.shard_roots));
    println!(
        "  \"refcount_shard_roots\": {},",
        page_array_json(&m.refcount_shard_roots)
    );
    println!(
        "  \"dedup_level_heads\": {},",
        page_array_json(&m.dedup_level_heads)
    );
    println!(
        "  \"dedup_reverse_level_heads\": {},",
        page_array_json(&m.dedup_reverse_level_heads)
    );
    println!("  \"next_snapshot_id\": {},", m.next_snapshot_id);
    println!("  \"snapshots\": [");
    for (i, entry) in m.snapshots.iter().enumerate() {
        let trailing = if i + 1 == m.snapshots.len() { "" } else { "," };
        print_snapshot_entry_json(entry, "    ", trailing);
    }
    println!("  ]");
    println!("}}");
}

// -------- L2P lookups ---------------------------------------------------

fn cmd_lba(args: &[String]) -> Result<ExitCode, String> {
    let Parsed {
        path,
        json,
        positional,
        snapshot,
        ..
    } = parse_flags(args, ParseSpec::one_positional().with_snapshot())?;
    let lba = parse_u64(positional.first(), "lba")?;
    let db = open_db(&path)?;

    let result = match snapshot {
        Some(id) => {
            let view = db
                .snapshot_view(id)
                .ok_or_else(|| format!("unknown snapshot id {id}"))?;
            view.get(lba).map_err(|e| e.to_string())?
        }
        None => db.get(lba).map_err(|e| e.to_string())?,
    };

    if json {
        println!("{{");
        println!("  \"lba\": {lba},");
        if let Some(id) = snapshot {
            println!("  \"snapshot\": {id},");
        }
        match result {
            Some(v) => println!("  \"value\": \"{}\"", hex(&v.0)),
            None => println!("  \"value\": null"),
        }
        println!("}}");
    } else {
        println!("lba: {lba}");
        if let Some(id) = snapshot {
            println!("snapshot: {id}");
        }
        match result {
            Some(v) => println!("value: {}", hex(&v.0)),
            None => println!("value: <none>"),
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn cmd_l2p(args: &[String]) -> Result<ExitCode, String> {
    let Parsed {
        path,
        json,
        from,
        to,
        limit,
        snapshot,
        ..
    } = parse_flags(
        args,
        ParseSpec::path_only()
            .with_from_to()
            .with_limit()
            .with_snapshot(),
    )?;
    let db = open_db(&path)?;
    let limit = limit.unwrap_or(256) as usize;

    let rows: Vec<(Lba, L2pValue)> = match snapshot {
        Some(id) => {
            let view = db
                .snapshot_view(id)
                .ok_or_else(|| format!("unknown snapshot id {id}"))?;
            collect_range(
                view.range(bounded_range(from, to))
                    .map_err(|e| e.to_string())?,
                limit,
            )?
        }
        None => collect_range(
            db.range(bounded_range(from, to))
                .map_err(|e| e.to_string())?,
            limit,
        )?,
    };

    if json {
        println!("{{");
        if let Some(id) = snapshot {
            println!("  \"snapshot\": {id},");
        }
        if let Some(x) = from {
            println!("  \"from\": {x},");
        }
        if let Some(x) = to {
            println!("  \"to\": {x},");
        }
        println!("  \"count\": {},", rows.len());
        println!("  \"entries\": [");
        for (i, (lba, value)) in rows.iter().enumerate() {
            let trailing = if i + 1 == rows.len() { "" } else { "," };
            println!(
                "    {{ \"lba\": {lba}, \"value\": \"{}\" }}{trailing}",
                hex(&value.0),
            );
        }
        println!("  ]");
        println!("}}");
    } else {
        println!("count: {}", rows.len());
        for (lba, value) in rows {
            println!("  {lba}  {}", hex(&value.0));
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn collect_range(
    iter: onyx_metadb::DbRangeIter,
    limit: usize,
) -> Result<Vec<(Lba, L2pValue)>, String> {
    let mut rows = Vec::new();
    for row in iter {
        if rows.len() >= limit {
            break;
        }
        rows.push(row.map_err(|e| e.to_string())?);
    }
    Ok(rows)
}

fn bounded_range(
    from: Option<u64>,
    to: Option<u64>,
) -> (std::ops::Bound<u64>, std::ops::Bound<u64>) {
    use std::ops::Bound;
    let start = from.map(Bound::Included).unwrap_or(Bound::Unbounded);
    let end = to.map(Bound::Included).unwrap_or(Bound::Unbounded);
    (start, end)
}

// -------- refcount / dedup ----------------------------------------------

fn cmd_refcount(args: &[String]) -> Result<ExitCode, String> {
    let Parsed {
        path,
        json,
        positional,
        ..
    } = parse_flags(args, ParseSpec::one_positional())?;
    let pba = parse_u64(positional.first(), "pba")? as Pba;
    let db = open_db(&path)?;
    let count = db.get_refcount(pba).map_err(|e| e.to_string())?;
    if json {
        println!("{{ \"pba\": {pba}, \"refcount\": {count} }}");
    } else {
        println!("pba: {pba}");
        println!("refcount: {count}");
    }
    Ok(ExitCode::SUCCESS)
}

fn cmd_dedup(args: &[String]) -> Result<ExitCode, String> {
    let Parsed {
        path,
        json,
        positional,
        ..
    } = parse_flags(args, ParseSpec::one_positional())?;
    let hash_hex = positional
        .first()
        .ok_or_else(|| "missing hash argument".to_string())?;
    let hash = parse_hash32(hash_hex)?;
    let db = open_db(&path)?;
    let value: Option<DedupValue> = db.get_dedup(&hash).map_err(|e| e.to_string())?;
    if json {
        println!("{{");
        println!("  \"hash\": \"{}\",", hex(&hash));
        match value {
            Some(v) => println!("  \"value\": \"{}\"", hex(&v.0)),
            None => println!("  \"value\": null"),
        }
        println!("}}");
    } else {
        println!("hash: {}", hex(&hash));
        match value {
            Some(v) => println!("value: {}", hex(&v.0)),
            None => println!("value: <none>"),
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn cmd_dedup_reverse(args: &[String]) -> Result<ExitCode, String> {
    let Parsed {
        path,
        json,
        positional,
        ..
    } = parse_flags(args, ParseSpec::one_positional())?;
    let pba = parse_u64(positional.first(), "pba")? as Pba;
    let db = open_db(&path)?;
    let hashes: Vec<Hash32> = db
        .scan_dedup_reverse_for_pba(pba)
        .map_err(|e| e.to_string())?;
    if json {
        println!("{{");
        println!("  \"pba\": {pba},");
        println!("  \"count\": {},", hashes.len());
        println!("  \"hashes\": [");
        for (i, h) in hashes.iter().enumerate() {
            let trailing = if i + 1 == hashes.len() { "" } else { "," };
            println!("    \"{}\"{trailing}", hex(h));
        }
        println!("  ]");
        println!("}}");
    } else {
        println!("pba: {pba}");
        println!("count: {}", hashes.len());
        for h in hashes {
            println!("  {}", hex(&h));
        }
    }
    Ok(ExitCode::SUCCESS)
}

// -------- snapshots -----------------------------------------------------

fn cmd_snapshots(args: &[String]) -> Result<ExitCode, String> {
    let Parsed { path, json, .. } = parse_flags(args, ParseSpec::path_only())?;
    let db = open_db(&path)?;
    let entries = db.snapshots();
    if json {
        println!("{{");
        println!("  \"count\": {},", entries.len());
        println!("  \"snapshots\": [");
        for (i, entry) in entries.iter().enumerate() {
            let trailing = if i + 1 == entries.len() { "" } else { "," };
            print_snapshot_entry_json(entry, "    ", trailing);
        }
        println!("  ]");
        println!("}}");
    } else {
        println!("count: {}", entries.len());
        for entry in &entries {
            print_snapshot_entry_human(entry, "  ");
        }
    }
    Ok(ExitCode::SUCCESS)
}

fn print_snapshot_entry_human(entry: &SnapshotEntry, indent: &str) {
    println!("{indent}id: {}", entry.id);
    println!("{indent}  created_lsn: {}", entry.created_lsn);
    println!(
        "{indent}  l2p_shard_roots: [{}]",
        entry
            .l2p_shard_roots
            .iter()
            .map(|p| fmt_page(*p))
            .collect::<Vec<_>>()
            .join(", "),
    );
    if !entry.refcount_shard_roots.is_empty() {
        println!(
            "{indent}  refcount_shard_roots: [{}]  (legacy pre-6.5b)",
            entry
                .refcount_shard_roots
                .iter()
                .map(|p| fmt_page(*p))
                .collect::<Vec<_>>()
                .join(", "),
        );
    }
}

fn print_snapshot_entry_json(entry: &SnapshotEntry, indent: &str, trailing: &str) {
    println!("{indent}{{");
    println!("{indent}  \"id\": {},", entry.id);
    println!("{indent}  \"created_lsn\": {},", entry.created_lsn);
    println!(
        "{indent}  \"l2p_shard_roots\": {},",
        page_array_json(&entry.l2p_shard_roots),
    );
    println!(
        "{indent}  \"refcount_shard_roots\": {}",
        page_array_json(&entry.refcount_shard_roots),
    );
    println!("{indent}}}{trailing}");
}

// -------- argument parsing ----------------------------------------------

struct Parsed {
    path: PathBuf,
    json: bool,
    positional: Vec<String>,
    snapshot: Option<SnapshotId>,
    from: Option<u64>,
    to: Option<u64>,
    limit: Option<u64>,
}

struct ParseSpec {
    positional_count: usize,
    allow_snapshot: bool,
    allow_from_to: bool,
    allow_limit: bool,
}

impl ParseSpec {
    fn path_only() -> Self {
        Self {
            positional_count: 0,
            allow_snapshot: false,
            allow_from_to: false,
            allow_limit: false,
        }
    }

    fn one_positional() -> Self {
        Self {
            positional_count: 1,
            ..Self::path_only()
        }
    }

    fn with_snapshot(mut self) -> Self {
        self.allow_snapshot = true;
        self
    }

    fn with_from_to(mut self) -> Self {
        self.allow_from_to = true;
        self
    }

    fn with_limit(mut self) -> Self {
        self.allow_limit = true;
        self
    }
}

fn parse_flags(args: &[String], spec: ParseSpec) -> Result<Parsed, String> {
    let mut path: Option<PathBuf> = None;
    let mut positional = Vec::new();
    let mut json = false;
    let mut snapshot = None;
    let mut from = None;
    let mut to = None;
    let mut limit = None;
    let mut it = args.iter();
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--json" => json = true,
            "--snapshot" if spec.allow_snapshot => {
                snapshot = Some(parse_u64(it.next(), "--snapshot")?);
            }
            "--from" if spec.allow_from_to => {
                from = Some(parse_u64(it.next(), "--from")?);
            }
            "--to" if spec.allow_from_to => {
                to = Some(parse_u64(it.next(), "--to")?);
            }
            "--limit" if spec.allow_limit => {
                limit = Some(parse_u64(it.next(), "--limit")?);
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            _ if arg.starts_with('-') => return Err(format!("unknown flag `{arg}`")),
            _ if path.is_none() => path = Some(PathBuf::from(arg)),
            _ => positional.push(arg.clone()),
        }
    }
    let Some(path) = path else {
        return Err("missing <path> argument".into());
    };
    if positional.len() != spec.positional_count {
        return Err(format!(
            "expected {} positional arg(s) after path, got {}",
            spec.positional_count,
            positional.len(),
        ));
    }
    Ok(Parsed {
        path,
        json,
        positional,
        snapshot,
        from,
        to,
        limit,
    })
}

fn parse_u64(arg: Option<&String>, name: &str) -> Result<u64, String> {
    let s = arg.ok_or_else(|| format!("missing `{name}` argument"))?;
    let trimmed = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X"));
    match trimmed {
        Some(hex) => u64::from_str_radix(hex, 16)
            .map_err(|e| format!("invalid {name} `{s}`: {e}")),
        None => s.parse::<u64>().map_err(|e| format!("invalid {name} `{s}`: {e}")),
    }
}

fn parse_hash32(s: &str) -> Result<Hash32, String> {
    let stripped = s.strip_prefix("0x").unwrap_or(s);
    if stripped.len() != 64 {
        return Err(format!(
            "hash must be 32 bytes hex (64 chars), got {} chars",
            stripped.len(),
        ));
    }
    let mut out = [0u8; 32];
    for (i, chunk) in stripped.as_bytes().chunks(2).enumerate() {
        let pair = std::str::from_utf8(chunk).map_err(|_| "invalid utf8 in hash".to_string())?;
        out[i] = u8::from_str_radix(pair, 16).map_err(|e| format!("bad hex `{pair}`: {e}"))?;
    }
    Ok(out)
}

// -------- helpers --------------------------------------------------------

fn open_db(path: &Path) -> Result<Db, String> {
    Db::open(path).map_err(|e| e.to_string())
}

fn fmt_page(id: PageId) -> String {
    if id == onyx_metadb::NULL_PAGE {
        "NULL".to_string()
    } else {
        id.to_string()
    }
}

fn page_json(id: PageId) -> String {
    if id == onyx_metadb::NULL_PAGE {
        "null".to_string()
    } else {
        id.to_string()
    }
}

fn page_array_json(ids: &[PageId]) -> String {
    let mut out = String::from("[");
    for (i, id) in ids.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&page_json(*id));
    }
    out.push(']');
    out
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push_str(&format!("{b:02x}"));
    }
    out
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
