//! `metadb-replay` — offline WAL inspector. Walks the WAL segments in a
//! directory and prints each record's LSN, op count, and (optionally)
//! the decoded ops. Read-only: never opens the page store, never writes
//! anything.
//!
//! Intended use: Phase 7 adapter debugging ("what landed after LSN X?",
//! "did this commit's refcount delta make it into the log?") and
//! post-soak forensics. Not a replay-into-new-Db tool — `Db::open`
//! already does that at startup.

use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use onyx_metadb::Lsn;
use onyx_metadb::wal::{
    DecodeError, WalOp, WalRecordIter, decode_body, list_segments, read_segment,
};

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(err) => {
            if !err.is_empty() {
                eprintln!("metadb-replay: {err}");
            }
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<ExitCode, String> {
    let cfg = Config::parse(env::args().skip(1))?;
    let summary = walk_wal(&cfg)?;
    if cfg.summary {
        print_summary(&cfg, &summary);
    } else if cfg.json {
        // Records already streamed as JSON objects; close out with summary.
        print_summary_json_footer(&cfg, &summary);
    } else {
        print_summary_footer(&summary);
    }
    Ok(ExitCode::SUCCESS)
}

struct Config {
    wal_dir: PathBuf,
    from_lsn: Option<Lsn>,
    to_lsn: Option<Lsn>,
    summary: bool,
    json: bool,
    print_ops: bool,
}

impl Config {
    fn parse<I: Iterator<Item = String>>(args: I) -> Result<Self, String> {
        let mut wal_dir: Option<PathBuf> = None;
        let mut from_lsn = None;
        let mut to_lsn = None;
        let mut summary = false;
        let mut json = false;
        let mut print_ops = false;
        let mut it = args;
        while let Some(arg) = it.next() {
            match arg.as_str() {
                "--from-lsn" => from_lsn = Some(parse_u64(it.next(), "--from-lsn")?),
                "--to-lsn" => to_lsn = Some(parse_u64(it.next(), "--to-lsn")?),
                "--summary" => summary = true,
                "--json" => json = true,
                "--ops" => print_ops = true,
                "-h" | "--help" => {
                    print_usage();
                    std::process::exit(0);
                }
                _ if arg.starts_with('-') => return Err(format!("unknown flag `{arg}`")),
                _ if wal_dir.is_none() => wal_dir = Some(PathBuf::from(arg)),
                _ => return Err(format!("unexpected positional arg `{arg}`")),
            }
        }
        let Some(wal_dir) = wal_dir else {
            print_usage();
            return Err("missing <wal-dir> argument".into());
        };
        Ok(Self {
            wal_dir,
            from_lsn,
            to_lsn,
            summary,
            json,
            print_ops,
        })
    }
}

fn print_usage() {
    eprintln!(
        "usage: metadb-replay <wal-dir> [options]

options:
  --from-lsn LSN   skip records with LSN < value
  --to-lsn LSN     skip records with LSN > value
  --summary        print only first/last LSN, record count, op counts, torn tail
  --ops            include fully-decoded ops per record
  --json           JSON output (one record per line + trailing summary object)

The argument is the WAL directory (the `wal/` subdir of a metadb path).
Never writes anything. Safe to run against a live database."
    );
}

// -------- walking --------------------------------------------------------

#[derive(Default)]
struct Summary {
    first_lsn: Option<Lsn>,
    last_lsn: Option<Lsn>,
    records: u64,
    ops: u64,
    counts: OpCounts,
    torn_tail: Option<DecodeError>,
    final_segment: Option<PathBuf>,
    tail_offset: u64,
}

#[derive(Default, Clone)]
struct OpCounts {
    l2p_put: u64,
    l2p_delete: u64,
    l2p_remap: u64,
    dedup_put: u64,
    dedup_delete: u64,
    dedup_reverse_put: u64,
    dedup_reverse_delete: u64,
    incref: u64,
    decref: u64,
    drop_snapshot: u64,
    create_volume: u64,
    drop_volume: u64,
    clone_volume: u64,
}

impl OpCounts {
    fn bump(&mut self, op: &WalOp) {
        match op {
            WalOp::L2pPut { .. } => self.l2p_put += 1,
            WalOp::L2pDelete { .. } => self.l2p_delete += 1,
            WalOp::L2pRemap { .. } => self.l2p_remap += 1,
            WalOp::DedupPut { .. } => self.dedup_put += 1,
            WalOp::DedupDelete { .. } => self.dedup_delete += 1,
            WalOp::DedupReversePut { .. } => self.dedup_reverse_put += 1,
            WalOp::DedupReverseDelete { .. } => self.dedup_reverse_delete += 1,
            WalOp::Incref { .. } => self.incref += 1,
            WalOp::Decref { .. } => self.decref += 1,
            WalOp::DropSnapshot { .. } => self.drop_snapshot += 1,
            WalOp::CreateVolume { .. } => self.create_volume += 1,
            WalOp::DropVolume { .. } => self.drop_volume += 1,
            WalOp::CloneVolume { .. } => self.clone_volume += 1,
        }
    }
}

fn walk_wal(cfg: &Config) -> Result<Summary, String> {
    let segments = list_segments(&cfg.wal_dir).map_err(|e| e.to_string())?;
    let segment_count = segments.len();
    let mut summary = Summary::default();

    for (idx, (_seg_start, path)) in segments.into_iter().enumerate() {
        let is_final = idx + 1 == segment_count;
        let buf = read_segment(&path).map_err(|e| e.to_string())?;
        let mut iter = WalRecordIter::new(&buf);
        for rec in iter.by_ref() {
            if let Some(min) = cfg.from_lsn {
                if rec.lsn < min {
                    continue;
                }
            }
            if let Some(max) = cfg.to_lsn {
                if rec.lsn > max {
                    break;
                }
            }
            let ops = decode_body(rec.body).map_err(|e| e.to_string())?;
            if !cfg.summary {
                if cfg.json {
                    print_record_json(rec.lsn, &ops, cfg.print_ops);
                } else {
                    print_record_human(rec.lsn, &ops, cfg.print_ops);
                }
            }
            if summary.first_lsn.is_none() {
                summary.first_lsn = Some(rec.lsn);
            }
            summary.last_lsn = Some(rec.lsn);
            summary.records += 1;
            summary.ops += ops.len() as u64;
            for op in &ops {
                summary.counts.bump(op);
            }
        }
        if is_final {
            summary.tail_offset = iter.consumed() as u64;
            summary.final_segment = Some(path.clone());
            summary.torn_tail = iter.stopped();
        }
    }
    Ok(summary)
}

// -------- printing -------------------------------------------------------

fn print_record_human(lsn: Lsn, ops: &[WalOp], include_ops: bool) {
    if include_ops {
        println!("lsn {lsn:<8}  {} op(s):", ops.len());
        for op in ops {
            println!("    {}", fmt_op(op));
        }
    } else {
        println!("lsn {lsn:<8}  {} op(s)  [{}]", ops.len(), op_tag_list(ops));
    }
}

fn print_record_json(lsn: Lsn, ops: &[WalOp], include_ops: bool) {
    let mut out = format!("{{\"lsn\":{lsn},\"ops\":{}", ops.len());
    if include_ops {
        out.push_str(",\"items\":[");
        for (i, op) in ops.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(&op_json(op));
        }
        out.push(']');
    }
    out.push('}');
    println!("{out}");
}

fn fmt_op(op: &WalOp) -> String {
    match op {
        WalOp::L2pPut {
            vol_ord,
            lba,
            value,
        } => format!(
            "L2pPut vol={vol_ord} lba={lba} value={}",
            hex(value.0)
        ),
        WalOp::L2pDelete { vol_ord, lba } => format!("L2pDelete vol={vol_ord} lba={lba}"),
        WalOp::L2pRemap {
            vol_ord,
            lba,
            new_value,
            guard,
        } => match guard {
            Some((pba, min_rc)) => format!(
                "L2pRemap vol={vol_ord} lba={lba} new_value={} guard=(pba={pba}, min_rc={min_rc})",
                hex(new_value.0)
            ),
            None => format!(
                "L2pRemap vol={vol_ord} lba={lba} new_value={} guard=None",
                hex(new_value.0)
            ),
        },
        WalOp::DedupPut { hash, value } => {
            format!("DedupPut hash={} value={}", hex(hash), hex(value.0))
        }
        WalOp::DedupDelete { hash } => format!("DedupDelete hash={}", hex(hash)),
        WalOp::DedupReversePut { pba, hash } => {
            format!("DedupReversePut pba={pba} hash={}", hex(hash))
        }
        WalOp::DedupReverseDelete { pba, hash } => {
            format!("DedupReverseDelete pba={pba} hash={}", hex(hash))
        }
        WalOp::Incref { pba, delta } => format!("Incref pba={pba} delta={delta}"),
        WalOp::Decref { pba, delta } => format!("Decref pba={pba} delta={delta}"),
        WalOp::DropSnapshot { id, pages } => {
            format!("DropSnapshot id={id} pages={}", pages.len())
        }
        WalOp::CreateVolume { ord, shard_count } => {
            format!("CreateVolume ord={ord} shard_count={shard_count}")
        }
        WalOp::DropVolume { ord, pages } => {
            format!("DropVolume ord={ord} pages={}", pages.len())
        }
        WalOp::CloneVolume {
            src_ord,
            new_ord,
            src_snap_id,
            src_shard_roots,
        } => format!(
            "CloneVolume src={src_ord} new={new_ord} snap={src_snap_id} shards={}",
            src_shard_roots.len()
        ),
    }
}

fn op_json(op: &WalOp) -> String {
    match op {
        WalOp::L2pPut {
            vol_ord,
            lba,
            value,
        } => {
            format!(
                "{{\"op\":\"L2pPut\",\"vol_ord\":{vol_ord},\"lba\":{lba},\"value\":\"{}\"}}",
                hex(value.0),
            )
        }
        WalOp::L2pDelete { vol_ord, lba } => format!(
            "{{\"op\":\"L2pDelete\",\"vol_ord\":{vol_ord},\"lba\":{lba}}}"
        ),
        WalOp::L2pRemap {
            vol_ord,
            lba,
            new_value,
            guard,
        } => {
            let guard_json = match guard {
                Some((pba, min_rc)) => {
                    format!("{{\"pba\":{pba},\"min_rc\":{min_rc}}}")
                }
                None => "null".to_string(),
            };
            format!(
                "{{\"op\":\"L2pRemap\",\"vol_ord\":{vol_ord},\"lba\":{lba},\"new_value\":\"{}\",\"guard\":{guard_json}}}",
                hex(new_value.0),
            )
        }
        WalOp::DedupPut { hash, value } => format!(
            "{{\"op\":\"DedupPut\",\"hash\":\"{}\",\"value\":\"{}\"}}",
            hex(hash),
            hex(value.0),
        ),
        WalOp::DedupDelete { hash } => {
            format!("{{\"op\":\"DedupDelete\",\"hash\":\"{}\"}}", hex(hash))
        }
        WalOp::DedupReversePut { pba, hash } => format!(
            "{{\"op\":\"DedupReversePut\",\"pba\":{pba},\"hash\":\"{}\"}}",
            hex(hash)
        ),
        WalOp::DedupReverseDelete { pba, hash } => format!(
            "{{\"op\":\"DedupReverseDelete\",\"pba\":{pba},\"hash\":\"{}\"}}",
            hex(hash)
        ),
        WalOp::Incref { pba, delta } => {
            format!("{{\"op\":\"Incref\",\"pba\":{pba},\"delta\":{delta}}}")
        }
        WalOp::Decref { pba, delta } => {
            format!("{{\"op\":\"Decref\",\"pba\":{pba},\"delta\":{delta}}}")
        }
        WalOp::DropSnapshot { id, pages } => {
            format!(
                "{{\"op\":\"DropSnapshot\",\"id\":{id},\"pages\":{}}}",
                pages.len()
            )
        }
        WalOp::CreateVolume { ord, shard_count } => format!(
            "{{\"op\":\"CreateVolume\",\"ord\":{ord},\"shard_count\":{shard_count}}}"
        ),
        WalOp::DropVolume { ord, pages } => format!(
            "{{\"op\":\"DropVolume\",\"ord\":{ord},\"pages\":{}}}",
            pages.len()
        ),
        WalOp::CloneVolume {
            src_ord,
            new_ord,
            src_snap_id,
            src_shard_roots,
        } => format!(
            "{{\"op\":\"CloneVolume\",\"src_ord\":{src_ord},\"new_ord\":{new_ord},\
             \"src_snap_id\":{src_snap_id},\"shards\":{}}}",
            src_shard_roots.len()
        ),
    }
}

fn op_tag_list(ops: &[WalOp]) -> String {
    let mut tags: Vec<&'static str> = ops
        .iter()
        .map(|op| match op {
            WalOp::L2pPut { .. } => "L2pPut",
            WalOp::L2pDelete { .. } => "L2pDelete",
            WalOp::L2pRemap { .. } => "L2pRemap",
            WalOp::DedupPut { .. } => "DedupPut",
            WalOp::DedupDelete { .. } => "DedupDelete",
            WalOp::DedupReversePut { .. } => "DedupReversePut",
            WalOp::DedupReverseDelete { .. } => "DedupReverseDelete",
            WalOp::Incref { .. } => "Incref",
            WalOp::Decref { .. } => "Decref",
            WalOp::DropSnapshot { .. } => "DropSnapshot",
            WalOp::CreateVolume { .. } => "CreateVolume",
            WalOp::DropVolume { .. } => "DropVolume",
            WalOp::CloneVolume { .. } => "CloneVolume",
        })
        .collect();
    tags.dedup();
    tags.join(",")
}

fn print_summary(cfg: &Config, s: &Summary) {
    if cfg.json {
        print_summary_json(cfg, s);
    } else {
        print_summary_footer(s);
    }
}

fn print_summary_footer(s: &Summary) {
    eprintln!();
    eprintln!("--- summary ---");
    eprintln!("records:   {}", s.records);
    eprintln!("total ops: {}", s.ops);
    eprintln!(
        "first_lsn: {}",
        s.first_lsn.map_or("<none>".to_string(), |x| x.to_string()),
    );
    eprintln!(
        "last_lsn:  {}",
        s.last_lsn.map_or("<none>".to_string(), |x| x.to_string()),
    );
    print_op_counts_human(&s.counts);
    if let Some(err) = &s.torn_tail {
        eprintln!(
            "torn_tail: yes ({err}) at offset {} in {}",
            s.tail_offset,
            s.final_segment
                .as_ref()
                .map_or("<unknown>".to_string(), |p| p.display().to_string()),
        );
    } else {
        eprintln!("torn_tail: no");
    }
}

fn print_op_counts_human(c: &OpCounts) {
    eprintln!("op counts:");
    eprintln!("  L2pPut:             {}", c.l2p_put);
    eprintln!("  L2pDelete:          {}", c.l2p_delete);
    eprintln!("  DedupPut:           {}", c.dedup_put);
    eprintln!("  DedupDelete:        {}", c.dedup_delete);
    eprintln!("  DedupReversePut:    {}", c.dedup_reverse_put);
    eprintln!("  DedupReverseDelete: {}", c.dedup_reverse_delete);
    eprintln!("  Incref:             {}", c.incref);
    eprintln!("  Decref:             {}", c.decref);
    eprintln!("  DropSnapshot:       {}", c.drop_snapshot);
}

fn print_summary_json_footer(cfg: &Config, s: &Summary) {
    // Trailing summary on its own line so consumers reading records
    // line-by-line can detect "end of stream" by parsing a top-level
    // `"summary": { ... }` key.
    let _ = cfg; // tolerance for unused
    print_summary_json(cfg, s);
}

fn print_summary_json(_cfg: &Config, s: &Summary) {
    let torn = match &s.torn_tail {
        Some(err) => format!("{{\"error\":\"{}\"}}", escape_json(&err.to_string())),
        None => "null".to_string(),
    };
    let final_seg = match &s.final_segment {
        Some(p) => format!("\"{}\"", escape_json(&p.display().to_string())),
        None => "null".to_string(),
    };
    let first = s.first_lsn.map_or("null".to_string(), |x| x.to_string());
    let last = s.last_lsn.map_or("null".to_string(), |x| x.to_string());
    println!(
        "{{\"summary\":{{\"records\":{},\"ops\":{},\"first_lsn\":{first},\"last_lsn\":{last},\"torn_tail\":{torn},\"final_segment\":{final_seg},\"tail_offset\":{},\"op_counts\":{}}}}}",
        s.records,
        s.ops,
        s.tail_offset,
        op_counts_json(&s.counts),
    );
}

fn op_counts_json(c: &OpCounts) -> String {
    format!(
        "{{\"L2pPut\":{},\"L2pDelete\":{},\"DedupPut\":{},\"DedupDelete\":{},\"DedupReversePut\":{},\"DedupReverseDelete\":{},\"Incref\":{},\"Decref\":{},\"DropSnapshot\":{}}}",
        c.l2p_put,
        c.l2p_delete,
        c.dedup_put,
        c.dedup_delete,
        c.dedup_reverse_put,
        c.dedup_reverse_delete,
        c.incref,
        c.decref,
        c.drop_snapshot,
    )
}

// -------- small helpers --------------------------------------------------

fn parse_u64(arg: Option<String>, name: &str) -> Result<u64, String> {
    let s = arg.ok_or_else(|| format!("missing `{name}` argument"))?;
    let trimmed = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X"));
    match trimmed {
        Some(hex) => u64::from_str_radix(hex, 16)
            .map_err(|e| format!("invalid {name} `{s}`: {e}")),
        None => s.parse::<u64>().map_err(|e| format!("invalid {name} `{s}`: {e}")),
    }
}

fn hex<T: AsRef<[u8]>>(bytes: T) -> String {
    let b = bytes.as_ref();
    let mut out = String::with_capacity(b.len() * 2);
    for byte in b {
        out.push_str(&format!("{byte:02x}"));
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

