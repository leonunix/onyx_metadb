//! Crash-recovery property test: acked WAL records always survive a
//! writer crash.
//!
//! Each case:
//!
//! 1. Create a WAL, submit `n_before` records cleanly and record
//!    their LSNs — these are guaranteed-durable.
//! 2. Install a fault (`WalFsyncBefore` or `WalFsyncAfter`) with a
//!    chosen action (`Error` or `Panic`), firing on the next commit.
//! 3. Submit one "doomed" record (fires the fault) plus a few
//!    more that latch "writer exited".
//! 4. Drop the WAL — mimics process exit after a writer crash.
//! 5. Replay the WAL dir and verify every acked LSN is present and
//!    decode is clean (possibly with a torn tail after the last
//!    good record).
//!
//! Mid-log corruption is the specific failure we're checking will
//! never appear — that would indicate a framing bug, not a torn
//! tail.

use onyx_metadb::config::Config;
use onyx_metadb::recovery::replay;
use onyx_metadb::testing::faults::{FaultAction, FaultController, FaultPoint};
use onyx_metadb::types::Lsn;
use onyx_metadb::wal::Wal;
use proptest::prelude::*;
use tempfile::TempDir;

fn cfg() -> Config {
    let mut c = Config::new("unused");
    c.group_commit_timeout_us = 50;
    c
}

fn arb_fault_point() -> impl Strategy<Value = FaultPoint> {
    prop_oneof![
        Just(FaultPoint::WalFsyncBefore),
        Just(FaultPoint::WalFsyncAfter),
    ]
}

fn arb_fault_action() -> impl Strategy<Value = FaultAction> {
    prop_oneof![Just(FaultAction::Error), Just(FaultAction::Panic)]
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 64, .. ProptestConfig::default() })]

    #[test]
    fn acked_records_survive_writer_crash(
        n_before in 0u32..80,
        point in arb_fault_point(),
        action in arb_fault_action(),
    ) {
        let dir = TempDir::new().unwrap();
        let mut acked: Vec<Lsn> = Vec::new();
        {
            let faults = FaultController::new();
            let wal = Wal::create(dir.path(), &cfg(), 1, faults.clone()).unwrap();
            for i in 0..n_before {
                acked.push(wal.submit(format!("rec-{i}").into_bytes()).unwrap());
            }
            // Arm the fault; next commit hits it.
            faults.install(point, 1, action);
            let _ = wal.submit(b"doomed".to_vec());
            // A few more submits should see "writer exited" errors.
            for _ in 0..3 {
                let _ = wal.submit(b"post-crash".to_vec());
            }
            // drop(wal) → Drop calls shutdown; writer is gone, so the
            // shutdown ack-rx returns with a channel error.
        }

        let outcome = replay(dir.path(), 1).unwrap();

        // Every acked record must survive.
        prop_assert!(
            outcome.record_count >= acked.len() as u64,
            "replay saw {} records, expected at least {} acked",
            outcome.record_count,
            acked.len(),
        );

        if let Some(&last_acked) = acked.last() {
            let last = outcome.last_lsn.expect("replay missing last_lsn with records");
            prop_assert!(
                last >= last_acked,
                "replay last_lsn {} < last acked {}",
                last,
                last_acked,
            );
        }

        // LSNs that replay saw must include every acked LSN.
        // (Easier way than reading segment bodies: last_lsn covers the
        // upper bound; first_lsn covers the lower; for a contiguous
        // writer starting at LSN 1 with no gaps, last_lsn >= last_acked
        // implies all acked LSNs are covered.)
        if !acked.is_empty() {
            prop_assert_eq!(outcome.first_lsn, Some(1));
        }
    }
}

#[test]
fn clean_shutdown_replays_with_no_torn_tail() {
    // Sanity: with no fault installed, a clean shutdown produces a
    // torn-tail-free replay containing exactly the acked records.
    let dir = TempDir::new().unwrap();
    let n = 50u64;
    let faults = FaultController::new();
    let wal = Wal::create(dir.path(), &cfg(), 1, faults).unwrap();
    for i in 0..n {
        wal.submit(format!("{i}").into_bytes()).unwrap();
    }
    wal.shutdown().unwrap();

    let outcome = replay(dir.path(), 1).unwrap();
    assert_eq!(outcome.record_count, n);
    assert_eq!(outcome.first_lsn, Some(1));
    assert_eq!(outcome.last_lsn, Some(n));
    assert!(outcome.torn_tail.is_none());
}
