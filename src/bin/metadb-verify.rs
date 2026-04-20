use std::env;
use std::process::ExitCode;

use onyx_metadb::{VerifyOptions, VerifyReport, verify_path};

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(err) => {
            eprintln!("metadb-verify: {err}");
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<ExitCode, String> {
    let mut strict = false;
    let mut json = false;
    let mut path = None;

    for arg in env::args().skip(1) {
        match arg.as_str() {
            "--strict" => strict = true,
            "--json" => json = true,
            "-h" | "--help" => {
                print_usage();
                return Ok(ExitCode::SUCCESS);
            }
            _ if arg.starts_with('-') => {
                return Err(format!("unknown flag `{arg}`"));
            }
            _ if path.is_none() => path = Some(arg),
            _ => return Err("expected exactly one database path".into()),
        }
    }

    let Some(path) = path else {
        print_usage();
        return Ok(ExitCode::from(2));
    };

    let report = verify_path(&path, VerifyOptions { strict }).map_err(|e| e.to_string())?;
    if json {
        print_json(&report);
    } else {
        print_human(&report);
    }
    Ok(if report.is_clean() {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

fn print_usage() {
    eprintln!("usage: metadb-verify <path> [--strict] [--json]");
}

fn print_human(report: &VerifyReport) {
    println!("path: {}", report.path.display());
    if let Some(slot) = report.manifest_slot {
        println!("manifest_slot: {slot}");
    }
    if let Some(sequence) = report.manifest_sequence {
        println!("manifest_sequence: {sequence}");
    }
    if let Some(lsn) = report.checkpoint_lsn {
        println!("checkpoint_lsn: {lsn}");
    }
    println!("high_water: {}", report.high_water);
    println!("scanned_pages: {}", report.scanned_pages);
    println!("live_pages: {}", report.live_pages);
    println!("free_pages: {}", report.free_pages);
    println!("orphans: {}", report.orphan_pages.len());
    if !report.warnings.is_empty() {
        println!("warnings:");
        for warning in &report.warnings {
            println!("  - {warning}");
        }
    }
    if !report.issues.is_empty() {
        println!("issues:");
        for issue in &report.issues {
            println!("  - {issue}");
        }
    }
    if report.is_clean() {
        println!("status: clean");
    } else {
        println!("status: failed");
    }
}

fn print_json(report: &VerifyReport) {
    println!("{{");
    println!(
        "  \"path\": \"{}\",",
        escape_json(&report.path.display().to_string())
    );
    print_opt_num("manifest_slot", report.manifest_slot);
    print_opt_num("manifest_sequence", report.manifest_sequence);
    print_opt_num("checkpoint_lsn", report.checkpoint_lsn);
    println!("  \"high_water\": {},", report.high_water);
    println!("  \"scanned_pages\": {},", report.scanned_pages);
    println!("  \"live_pages\": {},", report.live_pages);
    println!("  \"free_pages\": {},", report.free_pages);
    println!(
        "  \"orphan_pages\": {},",
        json_u64_array(&report.orphan_pages)
    );
    println!("  \"warnings\": {},", json_string_array(&report.warnings));
    println!("  \"issues\": {}", json_string_array(&report.issues));
    println!("}}");
}

fn print_opt_num<T: std::fmt::Display>(name: &str, value: Option<T>) {
    match value {
        Some(value) => println!("  \"{name}\": {value},"),
        None => println!("  \"{name}\": null,"),
    }
}

fn json_string_array(items: &[String]) -> String {
    let mut out = String::from("[");
    for (idx, item) in items.iter().enumerate() {
        if idx > 0 {
            out.push_str(", ");
        }
        out.push('"');
        out.push_str(&escape_json(item));
        out.push('"');
    }
    out.push(']');
    out
}

fn json_u64_array(items: &[u64]) -> String {
    let mut out = String::from("[");
    for (idx, item) in items.iter().enumerate() {
        if idx > 0 {
            out.push_str(", ");
        }
        out.push_str(&item.to_string());
    }
    out.push(']');
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
