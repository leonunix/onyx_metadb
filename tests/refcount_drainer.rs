//! Integration tests for the priority-3 refcount staging overlay /
//! drainer thread. Validates that with `refcount_drainer_enabled =
//! true`, the Db produces identical end-state to the priority-1 path.

use onyx_metadb::{Config, Db};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tempfile::TempDir;

fn drainer_cfg(path: &std::path::Path) -> Config {
    let mut cfg = Config::new(path);
    cfg.refcount_drainer_enabled = true;
    // Aggressive cadence so tests don't wait long for the drainer to
    // pick up.
    cfg.refcount_drainer_interval_ms = 5;
    cfg.refcount_drainer_threshold_entries = 4;
    cfg
}

fn wait_for_drainer_cycle() {
    // Drainer runs every 5ms; allow a couple ticks plus build time.
    std::thread::sleep(Duration::from_millis(50));
}

#[test]
fn drainer_enabled_basic_incref_decref_round_trip() {
    let dir = TempDir::new().unwrap();
    let cfg = drainer_cfg(dir.path());
    let db = Db::create_with_config(cfg.clone()).unwrap();
    db.incref_pba(42, 3).unwrap();
    db.incref_pba(7, 1).unwrap();
    db.decref_pba(42, 1).unwrap();
    wait_for_drainer_cycle();
    assert_eq!(db.get_refcount(42).unwrap(), 2);
    assert_eq!(db.get_refcount(7).unwrap(), 1);
    assert_eq!(db.get_refcount(99).unwrap(), 0);
}

#[test]
fn drainer_enabled_flush_round_trips_through_disk() {
    let dir = TempDir::new().unwrap();
    let cfg = drainer_cfg(dir.path());
    {
        let db = Db::create_with_config(cfg.clone()).unwrap();
        for pba in 0u64..50 {
            db.incref_pba(pba, 1).unwrap();
        }
        for pba in 0u64..25 {
            db.incref_pba(pba, 1).unwrap();
        }
        wait_for_drainer_cycle();
        db.flush().unwrap();
        for pba in 0u64..25 {
            assert_eq!(db.get_refcount(pba).unwrap(), 2);
        }
        for pba in 25u64..50 {
            assert_eq!(db.get_refcount(pba).unwrap(), 1);
        }
    }
    let db = Db::open_with_config(cfg).unwrap();
    for pba in 0u64..25 {
        assert_eq!(db.get_refcount(pba).unwrap(), 2);
    }
    for pba in 25u64..50 {
        assert_eq!(db.get_refcount(pba).unwrap(), 1);
    }
}

#[test]
fn drainer_enabled_same_page_lsn_gap_keeps_younger_slot() {
    let dir = TempDir::new().unwrap();
    let cfg = drainer_cfg(dir.path());
    let db = Db::create_with_config(cfg).unwrap();

    db.incref_pba(335, 1).unwrap();
    wait_for_drainer_cycle();
    assert_eq!(db.get_refcount(335).unwrap(), 1);

    db.incref_pba(1, 1).unwrap();
    wait_for_drainer_cycle();
    assert_eq!(db.get_refcount(1).unwrap(), 1);

    db.flush().unwrap();
    assert_eq!(db.get_refcount(1).unwrap(), 1);
    assert_eq!(db.get_refcount(335).unwrap(), 1);
}

#[test]
fn drainer_enabled_recovery_overlay_starts_empty_post_reopen() {
    let dir = TempDir::new().unwrap();
    let cfg = drainer_cfg(dir.path());
    {
        let db = Db::create_with_config(cfg.clone()).unwrap();
        // Stage a bunch of ops without flushing — they live in the
        // delta map only (drainer may have moved some to overlay,
        // but neither is durable).
        for pba in 0u64..200 {
            db.incref_pba(pba, 1).unwrap();
        }
        wait_for_drainer_cycle();
        // Drop without flush.
    }
    // Reopen should replay WAL, rebuilding delta_active. The drainer
    // is spawned AFTER replay, so it never observed mid-replay state.
    let db = Db::open_with_config(cfg).unwrap();
    for pba in 0u64..200 {
        assert_eq!(db.get_refcount(pba).unwrap(), 1);
    }
}

#[test]
fn drainer_enabled_checkpoint_metric_records_calls() {
    let dir = TempDir::new().unwrap();
    let cfg = drainer_cfg(dir.path());
    let db = Db::create_with_config(cfg).unwrap();
    for pba in 0u64..16 {
        db.incref_pba(pba, 1).unwrap();
    }
    wait_for_drainer_cycle();
    db.flush().unwrap();
    let snap = db.metrics_snapshot();
    assert!(snap.flush_calls >= 1);
    // forced flush() bumps `_forced` rather than `_steady`.
    assert!(snap.flush_calls_forced >= 1);
}

/// Regression: concurrent incref/decref on a single PBA mix with
/// the drainer cycling its transitions. Earlier the layer transitions
/// had observable in-flight gaps where readers saw `pa=None & pd=None &
/// overlay=None & base=0` while the drained entries were briefly
/// invisible — producing `apply_delta_pure(0, -N)` underflow on the
/// next decref. This test would surface that race within seconds; it
/// must run without any Err for the priority-3 read path to be safe.
#[test]
fn drainer_enabled_concurrent_incref_decref_no_underflow() {
    let dir = TempDir::new().unwrap();
    let cfg = drainer_cfg(dir.path());
    let db = Arc::new(Db::create_with_config(cfg).unwrap());
    let target_pba: u64 = 4242;

    // Seed a non-zero refcount that's well above the per-thread
    // increment budget so honest decrefs never legitimately underflow.
    // The race we're testing is "the value transiently looks like 0
    // because of layer transitions", not "the value is actually 0".
    let seed: u32 = 1_000_000;
    db.incref_pba(target_pba, seed).unwrap();

    let stop = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let total_inc = Arc::new(AtomicUsize::new(0));
    let total_dec = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for tid in 0..8 {
        let db = db.clone();
        let stop = stop.clone();
        let errors = errors.clone();
        let total_inc = total_inc.clone();
        let total_dec = total_dec.clone();
        handles.push(std::thread::spawn(move || {
            let mut local_inc: u64 = 0;
            let mut local_dec: u64 = 0;
            // Mix increfs and decrefs roughly 50/50 — keeps the
            // logical refcount oscillating in a band the drainer must
            // observe consistently across its swap/publish/clear
            // transitions.
            for i in 0u64.. {
                if stop.load(Ordering::Relaxed) != 0 {
                    break;
                }
                let pick = (tid as u64).wrapping_add(i);
                if pick & 1 == 0 {
                    if let Err(_) = db.incref_pba(target_pba, 1) {
                        errors.fetch_add(1, Ordering::Relaxed);
                    } else {
                        local_inc += 1;
                    }
                } else {
                    if let Err(_) = db.decref_pba(target_pba, 1) {
                        errors.fetch_add(1, Ordering::Relaxed);
                    } else {
                        local_dec += 1;
                    }
                }
            }
            total_inc.fetch_add(local_inc as usize, Ordering::Relaxed);
            total_dec.fetch_add(local_dec as usize, Ordering::Relaxed);
        }));
    }
    // Let the workers churn through enough drainer cycles to expose
    // the transition windows. 1.5 s × 8 threads × ~1M ops/s/thread ≈
    // ~12M ops → ~3000 drainer cycles at threshold=4 / interval=5ms.
    std::thread::sleep(Duration::from_millis(1500));
    stop.store(1, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    let err_count = errors.load(Ordering::Relaxed);
    assert_eq!(
        err_count, 0,
        "drainer-enabled concurrent stage produced {err_count} errors (expected 0)"
    );
    // Sanity: final value should be seed + (inc - dec). Force a
    // checkpoint so all layers fold to disk and `get_refcount`
    // returns the canonical answer.
    db.flush().unwrap();
    let inc = total_inc.load(Ordering::Relaxed) as i64;
    let dec = total_dec.load(Ordering::Relaxed) as i64;
    let expected = seed as i64 + inc - dec;
    let actual = db.get_refcount(target_pba).unwrap() as i64;
    assert_eq!(
        actual, expected,
        "rc mismatch: seed={seed} +inc({inc}) -dec({dec}) = {expected}, got {actual}"
    );
}

/// Regression for the third P0 (2026-05-07): same-LSN multi-slot ops
/// in one tx must not silently lose contributions when the drainer
/// fires between two `rc.stage` calls of that tx.
///
/// `apply_refcount_bucket_to_tree` calls `rc.stage` per pba group in
/// sequence; each stage takes/releases `delta_active.lock` briefly.
/// The drainer's transition-1 swap can fire between two stages of the
/// same tx, splitting the tx's contributions across two drainer
/// cycles. Cycle K builds the page with `gen = lsn=N` from slot X;
/// cycle K+1 has slot Y still pending at lsn=N. Buggy `>=` replay-skip
/// in either `RcShard::stage` (when `effective_lsn >= lsn`) or in
/// `array::build_overlay_pages` (when `page_generation >= pending.last_lsn`)
/// silently dropped slot Y's pending. Real soak repros showed up at
/// `nvme-box:.dev/fio-dedupe-compress-soak/20260507T-bug-repro2/` and
/// `20260507T-trace-v2/`. Fix: strict `>` in both checks.
///
/// This test stresses the same shape: many transactions, each with N
/// incref ops on distinct PBAs that all share one `page_idx` (so they
/// route through the same drainer cycle's build). With the bug, a
/// final `get_refcount` on each PBA would show < the expected value.
#[test]
fn drainer_enabled_same_lsn_multi_slot_same_page_no_loss() {
    let dir = TempDir::new().unwrap();
    let cfg = drainer_cfg(dir.path());
    let db = Arc::new(Db::create_with_config(cfg).unwrap());

    // 7 distinct PBAs all in page_idx=0 (slots 5..170 of 336 entries
    // per page). Multiple PBAs in one page_idx is the precondition for
    // the bug — a single tx that touches multiple of them generates
    // multiple `rc.stage` calls back-to-back inside one rc bucket.
    let pbas: Vec<u64> = vec![5, 30, 60, 90, 120, 150, 170];

    // Pump transactions: each tx increfs all 7 pbas. Repeat 200 times.
    // With drainer interval=5ms and threshold=4, the drainer will
    // fire mid-stream — exactly the timing the bug needs. Phase 5
    // routes increfs through `PromotionChunk` (one chunk = one rc
    // bucket with N stages back-to-back, identical shape to the
    // pre-Phase-5 multi-Incref tx the bug needed).
    for _ in 0..200 {
        let mut tx = db.begin();
        tx.promotion_chunk(
            0,
            pbas.iter()
                .map(|pba| onyx_metadb::Pba::from(*pba))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            None,
        );
        tx.commit().unwrap();
    }

    // Drain delta_active fully so reads reflect the merged state.
    db.flush().unwrap();

    for pba in &pbas {
        let rc = db.get_refcount(onyx_metadb::Pba::from(*pba)).unwrap();
        assert_eq!(
            rc, 200,
            "pba {pba} dropped some incref(s); expected 200, got {rc}"
        );
    }
}

/// Regression for the second P0 (2026-05-06): with the drainer
/// enabled, periodic flushes interleaved with concurrent
/// incref/decref must not lose any contributions. Earlier the
/// `begin_checkpoint` backpressure-fallback path drained
/// `delta_active` via the priority-1 sample WITHOUT folding the
/// overlay's already-built sealed pages; the on-disk state then
/// reflected only the priority-1 contribution while the overlay
/// (still in RAM, with a different `page_id` than the freshly-allocated
/// priority-1 one) held the drainer's contribution. The next main-path
/// `begin_checkpoint` would then overwrite `page_table[idx]` back to
/// the overlay's pid, orphaning the priority-1 page on disk and
/// silently dropping its contribution — eventually surfacing as
/// `apply_delta_pure(0, -N)` underflow on a decref of a PBA whose
/// incref baseline was lost. Forcing flushes during heavy concurrent
/// staging exercises the same code paths the nvme-box fio mix hit.
#[test]
fn drainer_enabled_flush_concurrent_with_stages_no_drift() {
    let dir = TempDir::new().unwrap();
    let cfg = drainer_cfg(dir.path());
    let db = Arc::new(Db::create_with_config(cfg).unwrap());

    // Spread the workload across many PBAs so the drainer builds
    // multiple overlay pages per cycle, raising the chance of a flush
    // landing while the overlay is non-trivially populated.
    let n_pbas: u64 = 4096;
    let seed: u32 = 100;
    for pba in 0..n_pbas {
        db.incref_pba(pba, seed).unwrap();
    }

    let stop = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let total_inc = Arc::new(AtomicUsize::new(0));
    let total_dec = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for tid in 0..6 {
        let db = db.clone();
        let stop = stop.clone();
        let errors = errors.clone();
        let total_inc = total_inc.clone();
        let total_dec = total_dec.clone();
        handles.push(std::thread::spawn(move || {
            let mut local_inc: u64 = 0;
            let mut local_dec: u64 = 0;
            for i in 0u64.. {
                if stop.load(Ordering::Relaxed) != 0 {
                    break;
                }
                let pba = ((tid as u64).wrapping_mul(0x9E3779B1).wrapping_add(i)) % n_pbas;
                if (tid as u64).wrapping_add(i) & 1 == 0 {
                    if db.incref_pba(pba, 1).is_err() {
                        errors.fetch_add(1, Ordering::Relaxed);
                    } else {
                        local_inc += 1;
                    }
                } else {
                    if db.decref_pba(pba, 1).is_err() {
                        errors.fetch_add(1, Ordering::Relaxed);
                    } else {
                        local_dec += 1;
                    }
                }
            }
            total_inc.fetch_add(local_inc as usize, Ordering::Relaxed);
            total_dec.fetch_add(local_dec as usize, Ordering::Relaxed);
        }));
    }

    // While stages churn, force several flushes — each flush preempts
    // every shard's drainer; resume_drainer must fire so the next
    // burst of stages can be drained again. Between flushes the
    // drainer cycles through transitions atomically.
    let flusher_db = db.clone();
    let flusher_stop = stop.clone();
    let flusher = std::thread::spawn(move || {
        let mut count = 0u64;
        while flusher_stop.load(Ordering::Relaxed) == 0 {
            std::thread::sleep(Duration::from_millis(50));
            flusher_db.flush().unwrap();
            count += 1;
        }
        count
    });

    std::thread::sleep(Duration::from_millis(2_000));
    stop.store(1, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    let flush_count = flusher.join().unwrap();
    assert!(
        flush_count >= 5,
        "expected at least 5 background flushes, saw {flush_count}"
    );

    let err_count = errors.load(Ordering::Relaxed);
    assert_eq!(
        err_count, 0,
        "drainer + concurrent flush produced {err_count} errors (expected 0)"
    );

    db.flush().unwrap();
    let inc = total_inc.load(Ordering::Relaxed) as i64;
    let dec = total_dec.load(Ordering::Relaxed) as i64;
    let net_per_pba_seed = (seed as i64) * (n_pbas as i64);
    let mut total: i64 = 0;
    for pba in 0..n_pbas {
        total += db.get_refcount(pba).unwrap() as i64;
    }
    let expected = net_per_pba_seed + inc - dec;
    assert_eq!(
        total, expected,
        "rc total mismatch: seed_total({net_per_pba_seed}) + inc({inc}) - dec({dec}) = {expected}, got {total}"
    );
}

#[test]
fn drainer_disabled_path_still_works() {
    // Sanity: confirm the default-off path doesn't regress.
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.refcount_drainer_enabled = false;
    let db = Db::create_with_config(cfg).unwrap();
    db.incref_pba(1, 5).unwrap();
    db.flush().unwrap();
    assert_eq!(db.get_refcount(1).unwrap(), 5);
}
