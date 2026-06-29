//! Reproduction harness for the OnyxConcurrent `range_delete` / bfg
//! stall (the "22-94s stall / buffer head stuck" class the nvme-box
//! concurrent smoke hit; original gdb bt evidence was lost).
//!
//! ROOT CAUSE (confirmed by this harness): `range_delete` holds
//! `drop_gate.write` across a FULL forced BFG sync ([l2p.rs] the
//! `flush_with_gate(Forced)` at entry). Every `commit_ops` writer parks
//! at `drop_gate.read` for the whole duration of that sync, so a single
//! range_delete stalls the entire commit pipeline. At high pipeline
//! depth + a large dirty set the forced sync is multi-second; on
//! nvme-box (96 cores feeding one sync + a wall-clock skew that defeated
//! timeout recovery) it presented as a permanent deadlock. snapshot /
//! drop_snapshot / clone already dropped their entry force-sync in
//! buffer-backed journal (rc mutations stage into the L2pPageRc array, so a
//! concurrent flush IO phase has nothing to clobber); range_delete is
//! the leftover.
//!
//! Rather than depend on a 96-core hard hang, this harness MEASURES the
//! stall: it records the worst single `commit_ops` (`db.insert`) latency
//! while range_delete threads run concurrently. With the force-sync the
//! worst insert latency spikes into the 100s-of-ms / seconds range and
//! scales with the dirty-set size; without it the pipeline stays smooth.
//!
//! `#[ignore]` — run explicitly:
//!   cargo test --release -p onyx-metadb range_delete_concurrent_stall \
//!     -- --ignored --nocapture --test-threads=1
//!
//! Env knobs:
//!   RDSTRESS_WRITERS   (default 48)
//!   RDSTRESS_DELETERS  (default 8)
//!   RDSTRESS_KEYS      (default 131072)  live LBA space (dirty-set size)
//!   RDSTRESS_SECS      (default 40)
//!   RDSTRESS_STALL_MS  (default 20000)   true-hang watchdog window
//!   RDSTRESS_NO_RD     (default 0)       set 1 to run WITHOUT range_delete (A/B baseline)

use super::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn fetch_max(a: &AtomicU64, v: u64) {
    let mut cur = a.load(Ordering::Relaxed);
    while v > cur {
        match a.compare_exchange_weak(cur, v, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => break,
            Err(x) => cur = x,
        }
    }
}

/// Focused repro of the Legacy-soak flush huge-alloc ("3rd issue"):
/// mirror the soak child's ops with default `Config` + the soak's value
/// encodings, then flush. RDSTRESS_DEDUP=1 includes put_dedup with the
/// soak's scattered `dedup_value` to bisect whether dedup is the trigger.
///   cargo test --release -p onyx-metadb legacy_soak_flush_repro -- --ignored --nocapture
#[test]
#[ignore = "manual reproduction harness for the Legacy-soak flush crash"]
fn legacy_soak_flush_repro() {
    use crate::{DedupValue, Hash8};
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap(); // default Config, like the soak
    let v1 = db.create_volume().unwrap();
    let do_dedup = env_u64("RDSTRESS_DEDUP", 0) == 1;

    // soak l2p_key (dense) + l2p_value (dense PBA, pinned trailer).
    let lkey = |tid: u64, slot: u64| tid * 256 + slot;
    let lval = |byte: u8| {
        let mut x = [0u8; crate::paged::format::LEAF_VALUE_SIZE];
        x[7] = byte;
        x[crate::paged::format::LEAF_VALUE_SIZE - 1] = 1;
        L2pValue(x)
    };
    // soak dedup_hash + dedup_value (scattered head_pba — the suspect).
    let dhash = |tid: u64, slot: u64| -> Hash8 {
        (((tid) << 40) ^ (slot & 0x00FF_FFFF_FFFF)).to_be_bytes()
    };
    let dval = |byte: u8| {
        let mut x = [0u8; 28];
        // dense head_pba (low byte) — the fix; mirrors soak `dedup_value`.
        x[7] = byte;
        x[8] = byte.wrapping_mul(7);
        DedupValue(x)
    };

    for round in 0..4u64 {
        for tid in 0..8u64 {
            for slot in 0..256u64 {
                let vol = if slot % 2 == 0 { 0 } else { v1 };
                db.insert(vol, lkey(tid, slot), lval(((slot + round) % 251) as u8))
                    .unwrap();
                if do_dedup {
                    db.put_dedup(dhash(tid, slot), dval(((slot + round) % 251) as u8))
                        .unwrap();
                }
            }
        }
        eprintln!("legacy_soak_flush_repro: round {round} inserts done, flushing...");
        db.flush().unwrap();
        eprintln!("legacy_soak_flush_repro: round {round} flush ok");
    }
    eprintln!("legacy_soak_flush_repro: NO CRASH (dedup={do_dedup})");
}

#[test]
#[ignore = "manual reproduction harness; measures range_delete-induced commit stall"]
fn range_delete_concurrent_stall() {
    let writers = env_u64("RDSTRESS_WRITERS", 48);
    let deleters = if env_u64("RDSTRESS_NO_RD", 0) == 1 {
        0
    } else {
        env_u64("RDSTRESS_DELETERS", 8)
    };
    let keys = env_u64("RDSTRESS_KEYS", 131072);
    let secs = env_u64("RDSTRESS_SECS", 40);
    let stall_ms = env_u64("RDSTRESS_STALL_MS", 20000);
    let nvols = env_u64("RDSTRESS_VOLS", 1).max(1);

    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    cfg.l2p_buffer_soft_entries = 64;
    cfg.l2p_buffer_max_interval_ms = 5;
    cfg.bfg_threads_enabled = true;
    cfg.rc_authoritative_reclaim = true;
    cfg.bfg_timeout_ms = 5;
    let db = Db::create_with_config(cfg).unwrap();

    // Volume 0 exists by default; mint the rest.
    let mut vols: Vec<VolumeOrdinal> = vec![0];
    for _ in 1..nvols {
        vols.push(db.create_volume().unwrap());
    }
    let vols = Arc::new(vols);
    for &vo in vols.iter() {
        for i in 0..keys {
            db.insert(vo, i, v((i % 251) as u8)).unwrap();
        }
    }
    db.flush().unwrap();

    eprintln!(
        "range_delete_concurrent_stall: PID={} writers={writers} deleters={deleters} keys={keys} vols={} secs={secs}",
        std::process::id(),
        vols.len(),
    );

    let stop = Arc::new(AtomicBool::new(false));
    let writes = Arc::new(AtomicU64::new(0));
    let deletes = Arc::new(AtomicU64::new(0));
    // Worst single-call latencies (microseconds) — the stall signal.
    let max_insert_us = Arc::new(AtomicU64::new(0));
    let max_rd_us = Arc::new(AtomicU64::new(0));
    // Count of inserts that blocked > 200ms (a "severe" pipeline stall).
    let severe_inserts = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();

    for t in 0..writers {
        let db = db.clone();
        let stop = stop.clone();
        let writes = writes.clone();
        let max_insert_us = max_insert_us.clone();
        let severe_inserts = severe_inserts.clone();
        let vols = vols.clone();
        handles.push(std::thread::spawn(move || {
            let vo = vols[t as usize % vols.len()];
            let mut i = t % keys;
            let mut round = 1u64;
            while !stop.load(Ordering::Relaxed) {
                let t0 = Instant::now();
                db.insert(vo, i, v(((i + round) % 251) as u8)).expect("insert");
                let us = t0.elapsed().as_micros() as u64;
                fetch_max(&max_insert_us, us);
                if us > 200_000 {
                    severe_inserts.fetch_add(1, Ordering::Relaxed);
                }
                writes.fetch_add(1, Ordering::Relaxed);
                i += 1;
                if i >= keys {
                    i = 0;
                    round += 1;
                }
            }
        }));
    }

    for t in 0..deleters {
        let db = db.clone();
        let stop = stop.clone();
        let deletes = deletes.clone();
        let max_rd_us = max_rd_us.clone();
        let vols = vols.clone();
        handles.push(std::thread::spawn(move || {
            let vo = vols[t as usize % vols.len()];
            let win: u64 = (keys / 16).max(8);
            let mut start = (t * win) % keys;
            while !stop.load(Ordering::Relaxed) {
                let end = (start + win).min(keys);
                let t0 = Instant::now();
                db.range_delete(vo, start, end).expect("range_delete");
                fetch_max(&max_rd_us, t0.elapsed().as_micros() as u64);
                deletes.fetch_add(1, Ordering::Relaxed);
                start = (start + win) % keys;
            }
        }));
    }

    // Watchdog: only for a TRUE hang (no total progress for stall_ms).
    let deadline = Instant::now() + Duration::from_secs(secs);
    let mut last_total = 0u64;
    let mut last_advance = Instant::now();
    loop {
        std::thread::sleep(Duration::from_millis(200));
        let total = writes.load(Ordering::Relaxed) + deletes.load(Ordering::Relaxed);
        if total != last_total {
            last_total = total;
            last_advance = Instant::now();
        }
        if last_advance.elapsed() >= Duration::from_millis(stall_ms) {
            eprintln!(
                "\n*** TRUE HANG DETECTED ***\nPID={}\nattach: gdb -p {} -batch -ex 'thread apply all bt' -ex detach -ex quit\nparking for inspection.",
                std::process::id(),
                std::process::id(),
            );
            loop {
                std::thread::sleep(Duration::from_secs(3600));
            }
        }
        if Instant::now() >= deadline {
            break;
        }
    }

    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    eprintln!(
        "range_delete_concurrent_stall: DONE writes={} deletes={} | MAX_INSERT={}ms MAX_RANGE_DELETE={}ms severe_inserts(>200ms)={}",
        writes.load(Ordering::Relaxed),
        deletes.load(Ordering::Relaxed),
        max_insert_us.load(Ordering::Relaxed) / 1000,
        max_rd_us.load(Ordering::Relaxed) / 1000,
        severe_inserts.load(Ordering::Relaxed),
    );
}
