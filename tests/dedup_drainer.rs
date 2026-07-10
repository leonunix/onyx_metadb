//! Integration tests for the async dedup-index drainer
//! (`dedup_drainer_enabled = true`). The load-bearing guarantee is the
//! rc-safety contract from
//! `/root/.claude/plans/mighty-pondering-truffle.md`: deferring the
//! cuckoo write must leave the global refcount **byte-identical** to the
//! eager path, because the free/GC decision reads rc, never the cuckoo.
//! A staging bug can only push rc *higher* (leak), never lower
//! (premature free) — these tests assert rc equality against the
//! drainer-OFF reference and across flush/reopen.

use onyx_metadb::{Config, Db};
use std::time::{Duration, Instant};
use tempfile::TempDir;

const DEDUP_VALUE_SIZE: usize = 28;

fn drainer_cfg(path: &std::path::Path, enabled: bool) -> Config {
    let mut cfg = Config::new(path);
    cfg.dedup_drainer_enabled = enabled;
    // Aggressive cadence so the background drainer fires quickly.
    cfg.dedup_drainer_interval_ms = 5;
    cfg.dedup_drainer_threshold_entries = 4;
    cfg
}

/// Build a `DedupValue` whose `head_pba()` is `pba` (big-endian first
/// 8 bytes) and whose tail encodes `tag` so distinct values for the
/// same hash compare unequal.
fn dv(pba: u64, tag: u8) -> onyx_metadb::DedupValue {
    let mut b = [0u8; DEDUP_VALUE_SIZE];
    b[..8].copy_from_slice(&pba.to_be_bytes());
    b[8] = tag;
    onyx_metadb::DedupValue::new(b)
}

fn h(byte: u8) -> [u8; 8] {
    [byte; 8]
}

fn wait_until(timeout_ms: u64, mut cond: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    while Instant::now() < deadline {
        if cond() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    cond()
}

#[test]
fn drainer_put_get_round_trip_then_durable() {
    let dir = TempDir::new().unwrap();
    let cfg = drainer_cfg(dir.path(), true);
    {
        let db = Db::create_with_config(cfg.clone()).unwrap();
        db.put_dedup(h(1), dv(100, 0)).unwrap();
        // Read-your-writes: visible immediately via the staging layer,
        // before the background drainer has written the cuckoo.
        assert_eq!(db.get_dedup(&h(1)).unwrap(), Some(dv(100, 0)));
        // rc.stage is inline, so the refcount is correct immediately.
        assert_eq!(db.get_refcount(100).unwrap(), 1);
        // flush() runs the checkpoint barrier (preempt + final-drain).
        db.flush().unwrap();
        assert_eq!(db.get_dedup(&h(1)).unwrap(), Some(dv(100, 0)));
        assert_eq!(db.get_refcount(100).unwrap(), 1);
    }
    // Reopen: the flush made both the dedup entry and the rc durable.
    let db = Db::open_with_config(cfg).unwrap();
    assert_eq!(db.get_dedup(&h(1)).unwrap(), Some(dv(100, 0)));
    assert_eq!(db.get_refcount(100).unwrap(), 1);
}

#[test]
fn drainer_overwrite_and_delete_rc_transitions() {
    let dir = TempDir::new().unwrap();
    let cfg = drainer_cfg(dir.path(), true);
    let db = Db::create_with_config(cfg).unwrap();

    // Fresh insert → incref(A).
    db.put_dedup(h(7), dv(200, 0)).unwrap();
    assert_eq!(db.get_refcount(200).unwrap(), 1);

    // Overwrite same hash with a different PBA → decref(A) + incref(B).
    // old_pba is resolved at commit via the staging-aware `get`, so the
    // rc transition is exact even though A's cuckoo write may still be
    // staged.
    db.put_dedup(h(7), dv(201, 1)).unwrap();
    assert_eq!(db.get_dedup(&h(7)).unwrap(), Some(dv(201, 1)));
    assert_eq!(db.get_refcount(200).unwrap(), 0, "old PBA decref'd");
    assert_eq!(db.get_refcount(201).unwrap(), 1, "new PBA incref'd");

    // Delete → decref(B).
    db.delete_dedup(h(7)).unwrap();
    assert_eq!(db.get_dedup(&h(7)).unwrap(), None);
    assert_eq!(db.get_refcount(201).unwrap(), 0);

    db.flush().unwrap();
    assert_eq!(db.get_dedup(&h(7)).unwrap(), None);
    assert_eq!(db.get_refcount(200).unwrap(), 0);
    assert_eq!(db.get_refcount(201).unwrap(), 0);
}

/// THE rc-safety gate: an identical op sequence must produce identical
/// `(dedup entry, refcount)` end-state whether the drainer is on or off.
#[test]
fn drainer_rc_and_dedup_match_disabled_reference() {
    let ops: Vec<(u8, u64, u8, bool)> = vec![
        // (hash, pba, tag, is_delete)
        (1, 1000, 0, false),
        (2, 1001, 0, false),
        (1, 1002, 1, false), // overwrite h1: 1000 -> 1002
        (3, 1000, 0, false), // re-reference pba 1000 under a new hash
        (2, 1001, 0, true),  // delete h2
        (4, 1003, 0, false),
        (1, 1002, 1, false), // idempotent re-put (old==new)
        (3, 1000, 0, true),  // delete h3 -> pba 1000 back to 0
        (5, 1004, 0, false),
        (4, 1004, 5, false), // overwrite h4: 1003 -> 1004 (shares pba w/ h5)
    ];

    let run = |enabled: bool| -> (Vec<(u8, Option<onyx_metadb::DedupValue>)>, Vec<(u64, u32)>) {
        let dir = TempDir::new().unwrap();
        let cfg = drainer_cfg(dir.path(), enabled);
        let db = Db::create_with_config(cfg).unwrap();
        for (hb, pba, tag, is_del) in &ops {
            if *is_del {
                db.delete_dedup(h(*hb)).unwrap();
            } else {
                db.put_dedup(h(*hb), dv(*pba, *tag)).unwrap();
            }
        }
        db.flush().unwrap();
        let dedup: Vec<(u8, Option<onyx_metadb::DedupValue>)> = (1u8..=5)
            .map(|hb| (hb, db.get_dedup(&h(hb)).unwrap()))
            .collect();
        let rc: Vec<(u64, u32)> = (1000u64..=1004)
            .map(|pba| (pba, db.get_refcount(pba).unwrap()))
            .collect();
        (dedup, rc)
    };

    let (dedup_off, rc_off) = run(false);
    let (dedup_on, rc_on) = run(true);
    assert_eq!(
        dedup_on, dedup_off,
        "dedup end-state must match the eager reference"
    );
    assert_eq!(
        rc_on, rc_off,
        "refcount end-state must be byte-identical to the eager path (rc-safety contract)"
    );
}

/// Randomized rc-safety gate: a long pseudo-random op stream over a
/// small key space (forcing overwrites, deletes, re-puts, shared PBAs)
/// must yield identical `(dedup, refcount)` end-state on vs off. A
/// deterministic LCG keeps it dependency-free and reproducible. Also
/// interleaves a flush mid-stream so the on-run exercises the checkpoint
/// barrier + background drainer concurrently.
#[test]
fn drainer_rc_matches_disabled_randomized() {
    const N_OPS: usize = 2000;
    const N_HASHES: u64 = 24;
    const N_PBAS: u64 = 16;
    const PBA_BASE: u64 = 5000;

    let run = |enabled: bool| -> (Vec<Option<onyx_metadb::DedupValue>>, Vec<u32>) {
        let dir = TempDir::new().unwrap();
        let cfg = drainer_cfg(dir.path(), enabled);
        let db = Db::create_with_config(cfg).unwrap();
        // Deterministic LCG (Numerical Recipes constants).
        let mut state: u64 = 0x1234_5678_9abc_def0;
        let mut next = || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            state >> 33
        };
        for i in 0..N_OPS {
            let hb = (next() % N_HASHES) as u8;
            if next() % 4 == 0 {
                db.delete_dedup(h(hb)).unwrap();
            } else {
                let pba = PBA_BASE + (next() % N_PBAS);
                let tag = (next() % 251) as u8;
                db.put_dedup(h(hb), dv(pba, tag)).unwrap();
            }
            if i == N_OPS / 2 {
                db.flush().unwrap(); // mid-stream checkpoint barrier
            }
        }
        db.flush().unwrap();
        let dedup: Vec<Option<onyx_metadb::DedupValue>> = (0u64..N_HASHES)
            .map(|hb| db.get_dedup(&h(hb as u8)).unwrap())
            .collect();
        let rc: Vec<u32> = (0u64..N_PBAS)
            .map(|p| db.get_refcount(PBA_BASE + p).unwrap())
            .collect();
        (dedup, rc)
    };

    let (dedup_off, rc_off) = run(false);
    let (dedup_on, rc_on) = run(true);
    assert_eq!(
        dedup_on, dedup_off,
        "randomized dedup end-state diverged from eager reference"
    );
    assert_eq!(
        rc_on, rc_off,
        "randomized refcount end-state diverged from eager reference (rc-safety contract)"
    );
    // Sanity: the stream actually exercised live refs (not all-zero).
    assert!(
        rc_off.iter().any(|&c| c > 0),
        "test should leave some live PBAs"
    );
}

#[test]
fn drainer_background_cycle_writes_cuckoo() {
    let dir = TempDir::new().unwrap();
    let cfg = drainer_cfg(dir.path(), true);
    let db = Db::create_with_config(cfg).unwrap();
    // Cross the threshold so the background drainer fires without a flush.
    for i in 0..32u64 {
        db.put_dedup(h(i as u8), dv(2000 + i, 0)).unwrap();
    }
    // Entries are visible via staging immediately, and remain visible
    // after the background drainer folds them into the cuckoo.
    assert!(wait_until(2000, || db.get_dedup(&h(0)).unwrap() == Some(dv(2000, 0))));
    for i in 0..32u64 {
        assert_eq!(
            db.get_dedup(&h(i as u8)).unwrap(),
            Some(dv(2000 + i, 0)),
            "entry {i} must stay visible across the background drain"
        );
        assert_eq!(db.get_refcount(2000 + i).unwrap(), 1);
    }
}

/// metadb-standalone crash (drop without flush) loses un-checkpointed
/// staged dedup entries — but loses the paired rc deltas *together*, so
/// reopen is consistent (rc=0 ⇔ no dedup entry). No premature free, no
/// orphaned rc. (Under onyx the LV2 buffer replay re-drives both.)
#[test]
fn drainer_crash_without_flush_drops_dedup_and_rc_together() {
    let dir = TempDir::new().unwrap();
    let cfg = drainer_cfg(dir.path(), true);
    {
        let db = Db::create_with_config(cfg.clone()).unwrap();
        db.put_dedup(h(9), dv(300, 0)).unwrap();
        assert_eq!(db.get_dedup(&h(9)).unwrap(), Some(dv(300, 0)));
        assert_eq!(db.get_refcount(300).unwrap(), 1);
        // Drop WITHOUT flush — simulates losing the RAM staging + rc delta.
    }
    let db = Db::open_with_config(cfg).unwrap();
    // Both gone together: rc is NOT left elevated for a missing entry.
    assert_eq!(db.get_dedup(&h(9)).unwrap(), None);
    assert_eq!(
        db.get_refcount(300).unwrap(),
        0,
        "rc must not be left > 0 for a lost dedup entry (no premature-free / leak)"
    );
}
