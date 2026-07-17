//! A faulted forced BFG sync (e.g. `ManifestFsyncBefore`) must not hang the next
//! `take_snapshot`. A failed `run_sync_cycle` leaves its slot stuck in `Syncing`
//! forever (`mark_synced` never runs); before the fix the next forced flush
//! blocked permanently in `promote_to_syncing` (threads-off) or
//! `wait_until_synced` (threads-on). The fix poisons the sync subsystem (BFG
//! `aborted` flag + `sync_poison` latch) so subsequent forced-sync ops fail fast
//! with a "restart required" error, and a reopen recovers cleanly from the prior
//! durable manifest (the faulted commit never toggled the double-buffered
//! manifest slot).
//!
//! There is NO test-harness timeout in this repo, so a regression would HANG
//! the binary rather than fail. Every hang-prone call runs on a spawned thread
//! guarded by an `is_finished()` deadline poll that PANICS on timeout.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use super::v;
use crate::Db;
use crate::testing::faults::{FaultAction, FaultController, FaultPoint};
use crate::types::{SnapshotId, VolumeOrdinal};
use crate::verify::{VerifyOptions, verify_path};

/// Run `take_snapshot` on a spawned thread; PANIC if it does not return within
/// ~3s (a hang means forced-sync poisoning regressed). Returns the snapshot
/// result.
fn take_snapshot_no_hang(
    db: &Arc<Db>,
    vol: VolumeOrdinal,
    what: &str,
) -> crate::error::Result<SnapshotId> {
    let db2 = Arc::clone(db);
    let h = thread::spawn(move || db2.take_snapshot(vol));
    for _ in 0..300 {
        if h.is_finished() {
            return h.join().expect("take_snapshot thread panicked");
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("take_snapshot ({what}) hung > 3s — BFG faulted-sync deadlock regression");
}

fn no_hang_after_faulted_sync(threads: bool) {
    let dir = tempfile::TempDir::new().unwrap();
    let faults = FaultController::new();
    let mut cfg = crate::config::Config::new(dir.path());
    cfg.shards_per_partition = 1;
    cfg.bfg_threads_enabled = threads;
    cfg.async_reclaim_enabled = true;
    cfg.async_reclaim_idle_interval_ms = 1;
    // Long timeout so the background quiesce worker (threads-on) doesn't roll a
    // spurious BFG into the fault window.
    cfg.bfg_timeout_ms = 60_000;
    let db = Db::create_with_config_and_faults(cfg, faults.clone()).unwrap();
    let vol = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(vol, i, v(i as u8)).unwrap();
    }
    db.flush().unwrap();
    assert_eq!(
        db.metrics_snapshot().checkpoint_sync_phase,
        crate::metrics::CheckpointSyncPhase::Idle as u64,
        "successful forced cycle must publish an idle terminal phase"
    );

    // Keep one brand-new RC data page exclusively in the BFG that the faulted
    // sync will fold. The stable RC meta head is rewritten in place before the
    // manifest publish, so a post-write abort must not free this page or
    // restore its delta.
    const FAULT_RC_PBA: u64 = 42;
    assert_eq!(db.incref_pba(FAULT_RC_PBA, 1).unwrap(), 1);

    // Arm a one-shot manifest-fsync fault, then snapshot: its sync cycle commits
    // the manifest -> fault -> Err. Must RETURN (not hang) with Err.
    faults.install(FaultPoint::ManifestFsyncBefore, 1, FaultAction::Error);
    let first = take_snapshot_no_hang(&db, vol, "first/faulted");
    assert!(
        first.is_err(),
        "faulted-sync snapshot must return Err, got {first:?}"
    );
    assert_eq!(
        db.metrics_snapshot().checkpoint_sync_phase,
        crate::metrics::CheckpointSyncPhase::Error as u64,
        "faulted forced cycle must publish an error terminal phase"
    );
    assert!(
        faults.fired(FaultPoint::ManifestFsyncBefore),
        "the manifest-fsync fault must have fired on the snapshot's commit"
    );
    assert!(
        db.metrics_snapshot().flush_sample_rc_fresh_pages_max > 0,
        "faulted cycle must exercise a freshly allocated RC data page"
    );

    // The subsystem is now poisoned (restart required). Clear the fault: the
    // SECOND take_snapshot must FAIL FAST, not hang on the stuck Syncing slot.
    faults.clear();
    let second = take_snapshot_no_hang(&db, vol, "second/post-poison");
    assert!(
        second.is_err(),
        "post-poison snapshot must fail fast, got {second:?}"
    );
    // create_volume also drives a forced sync at entry -> fail fast (no hang).
    assert!(
        db.create_volume().is_err(),
        "create_volume must fail fast once the sync subsystem is poisoned"
    );
    let post_poison_lba = 99;
    assert!(
        db.insert(vol, post_poison_lba, v(99)).is_err(),
        "ordinary commit must fail fast once the sync subsystem is poisoned"
    );
    let post_poison_staged_lba = 100;
    let mut staged = db.begin();
    staged.insert(vol, post_poison_staged_lba, v(100));
    assert!(
        staged.commit_staged_with_outcomes().is_err(),
        "production staged commit must fail fast once the sync subsystem is poisoned"
    );

    // If the post-sync error path incorrectly queued the fresh RC page for
    // free, the enabled reclaim worker will turn it into a `Free` page before
    // shutdown. Reopen below must still follow the stable meta chain to rc=1.
    thread::sleep(Duration::from_millis(50));

    drop(db);

    // CORE durability check: the faulted snapshot's manifest never committed
    // (double-buffered slot un-toggled), so the prior durable DATA is intact and
    // the faulted snapshot is absent. Reopen, confirm data (both modes).
    let db = Db::open(dir.path()).unwrap();
    for i in 0u64..8 {
        assert_eq!(
            db.get(vol, i).unwrap(),
            Some(v(i as u8)),
            "reopen lba {i} lost"
        );
    }
    assert_eq!(
        db.get_refcount(FAULT_RC_PBA).unwrap(),
        1,
        "fresh RC page referenced by the stable meta chain was freed or lost"
    );
    assert_eq!(
        db.get(vol, post_poison_lba).unwrap(),
        None,
        "the rejected post-poison commit must not survive reopen"
    );
    assert_eq!(
        db.get(vol, post_poison_staged_lba).unwrap(),
        None,
        "the rejected post-poison staged commit must not survive reopen"
    );
    // Subsystem usable after restart (fresh constructor reset sync_poison): a
    // snapshot now succeeds.
    let ok = db.take_snapshot(vol);
    assert!(
        ok.is_ok(),
        "post-reopen snapshot must succeed (poison reset by restart), got {ok:?}"
    );

    let report = db.verify(VerifyOptions::default()).unwrap();
    assert!(
        report.issues.is_empty() && report.orphan_pages.is_empty(),
        "faulted manifest publish left reachable/free or orphan RC pages: issues={:?} orphans={:?}",
        report.issues,
        report.orphan_pages
    );

    // Force another fresh RC data page and a longer meta-chain after reopen.
    // If the old manifest's free-list still considered FAULT_RC_PBA's page
    // free, this allocation/flush could recycle and overwrite it.
    const NEXT_RC_PBA: u64 = 1_000_000;
    assert_eq!(db.incref_pba(NEXT_RC_PBA, 2).unwrap(), 2);
    db.flush().unwrap();
    assert_eq!(db.get_refcount(FAULT_RC_PBA).unwrap(), 1);
    assert_eq!(db.get_refcount(NEXT_RC_PBA).unwrap(), 2);
    drop(db);

    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.get_refcount(FAULT_RC_PBA).unwrap(), 1);
    assert_eq!(db.get_refcount(NEXT_RC_PBA).unwrap(), 2);
    let report = db.verify(VerifyOptions::default()).unwrap();
    assert!(
        report.issues.is_empty() && report.orphan_pages.is_empty(),
        "post-reallocation reopen is inconsistent: issues={:?} orphans={:?}",
        report.issues,
        report.orphan_pages
    );
    drop(db);

    // Strict page-rc verify in threads-OFF only — that is the soak/production
    // default (`bfg_threads_enabled` defaults false) and the mode whose
    // take_snapshot deadlock blocks the soak. threads-ON has a SEPARATE,
    // PRE-EXISTING, data-safe page-rc-array under-count on snapshot+reopen that
    // reproduces with NO fault at all (the inverted-shadow page-rc is not
    // authoritative for snapshot frees; DATA + reachability stay correct). It is
    // orthogonal to this deadlock fix — exposed, not caused, by making the
    // threads-on path reachable — so the strict page-rc audit is not asserted
    // here; this guards the faulted-sync deadlock regression.
    if !threads {
        let report = verify_path(
            dir.path(),
            VerifyOptions {
                strict: true,
                check_birth_shadow: true,
                check_clone_livelist: false,
                check_clone_birth_shadow: false,
            },
        )
        .unwrap();
        assert!(
            report.is_clean(),
            "verify after faulted-sync recovery (threads-off): {:?}",
            report.issues
        );
    }
}

#[test]
fn take_snapshot_no_hang_after_faulted_sync_threads_off() {
    no_hang_after_faulted_sync(false);
}

#[test]
fn take_snapshot_no_hang_after_faulted_sync_threads_on() {
    no_hang_after_faulted_sync(true);
}

/// Concurrent (>=2 worker) threads-off variant the HANG-completeness skeptic
/// demanded: a second forced flush that has already passed the entry poison
/// check while the first faults must NOT (a) hang, nor (b) run `run_sync_cycle`
/// on an un-promoted slot and `mark_synced` it (ring corruption). The
/// post-`promote_to_syncing` `is_aborted` re-check in `flush_with_gate` bails
/// the racer. Asserts no hang, >=1 error, and a clean reopen+verify (the corrupt
/// silent-mark_synced state would be caught by verify).
#[test]
fn concurrent_take_snapshots_no_hang_no_corruption_on_faulted_sync() {
    for attempt in 0..4u64 {
        let dir = tempfile::TempDir::new().unwrap();
        let faults = FaultController::new();
        let mut cfg = crate::config::Config::new(dir.path());
        cfg.shards_per_partition = 1;
        cfg.bfg_threads_enabled = false;
        let db = Db::create_with_config_and_faults(cfg, faults.clone()).unwrap();
        let vol = db.create_volume().unwrap();
        for i in 0u64..8 {
            db.insert(vol, i, v(i as u8)).unwrap();
        }
        db.flush().unwrap();

        faults.install(FaultPoint::ManifestFsyncBefore, 1, FaultAction::Error);
        // Two racing snapshot takers; one cycle faults (fire_on_hit=1).
        let d1 = Arc::clone(&db);
        let d2 = Arc::clone(&db);
        let h1 = thread::spawn(move || d1.take_snapshot(vol));
        let h2 = thread::spawn(move || d2.take_snapshot(vol));
        for (n, h) in [("racer-1", &h1), ("racer-2", &h2)] {
            let mut done = false;
            for _ in 0..300 {
                if h.is_finished() {
                    done = true;
                    break;
                }
                thread::sleep(Duration::from_millis(10));
            }
            assert!(
                done,
                "concurrent {n} hung > 3s (attempt {attempt}) — forced-sync poison regression"
            );
        }
        let r1 = h1.join().expect("racer-1 panicked");
        let r2 = h2.join().expect("racer-2 panicked");
        assert!(
            r1.is_err() || r2.is_err(),
            "at least one concurrent snapshot must observe the faulted/poisoned sync (attempt {attempt}): {r1:?} {r2:?}"
        );
        faults.clear();
        drop(db);

        // Reopen + verify: a silent mark_synced of the faulted slot (the
        // ring-corruption the racer fix prevents) would surface here.
        let db = Db::open(dir.path()).unwrap();
        for i in 0u64..8 {
            assert_eq!(
                db.get(vol, i).unwrap(),
                Some(v(i as u8)),
                "reopen lba {i} (attempt {attempt})"
            );
        }
        drop(db);
        let report = verify_path(
            dir.path(),
            VerifyOptions {
                strict: true,
                check_birth_shadow: true,
                check_clone_livelist: false,
                check_clone_birth_shadow: false,
            },
        )
        .unwrap();
        assert!(
            report.is_clean(),
            "verify after concurrent faulted sync (attempt {attempt}): {:?}",
            report.issues
        );
    }
}
