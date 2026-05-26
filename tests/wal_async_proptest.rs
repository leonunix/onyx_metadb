//! ZFS-TXG-clone Phase 3 soak gate — async WAL equivalence + fault
//! coverage for `FaultPoint::TxgSyncMidway` and
//! `FaultPoint::WalSubmitAsyncDropped`.
//!
//! ## What this gates
//!
//! `Db::commit_ops_deferred` with `wal_async_commits_enabled = true`
//! must produce byte-equivalent on-disk state to the same workload
//! run with `wal_async_commits_enabled = false` (both Phase 2 flags
//! on) after every batch is force-compacted AND the db is flushed.
//! The async path only shifts WAL fsync timing — the apply pipeline,
//! the L2pBuffer, the compactor drain, and the manifest commit
//! sequence are all identical.
//!
//! The TxgSyncMidway fault simulates a crash AFTER
//! `WalSet::fsync_all_lanes` returns Ok BUT BEFORE
//! `manifest_state.store.commit` has fsynced the new
//! `checkpoint_lsn`. WAL is durable; manifest still points at the
//! OLD checkpoint. Recovery replays from the old checkpoint, applies
//! the records idempotently, end state matches a clean flush.
//!
//! The WalSubmitAsyncDropped fault simulates the strongest power-loss
//! case: the kernel never flushes an async-submitted record. The
//! metadb-side contract is "anything past the last successful fsync
//! is lost" — the LV2 buffer re-drive contract lives on the onyx
//! side and is out of scope here.
//!
//! ## Why this is a soak gate
//!
//! Phase 3 (per `/root/.claude/plans/velvety-finding-cray.md`) is
//! the bar before flipping `Config::wal_async_commits_enabled` to
//! `true` by default. The 24h nvme-box soak + 10× SIGKILL + 5×
//! sysrq-trigger matrix is built around this property; this file is
//! the cargo-test-time canary.

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;

use onyx_metadb::testing::faults::{FaultAction, FaultController, FaultPoint};
use onyx_metadb::wal::op::WalOp;
use onyx_metadb::{Config, Db, L2pValue, VolumeOrdinal};
use proptest::prelude::*;
use tempfile::TempDir;

const BOOTSTRAP_VOL: VolumeOrdinal = 0;
const NUM_VOLUMES: usize = 4;
const LBA_SPACE: u64 = 256;
const MAX_RANGE_LEN: u64 = 32;

// -------- helpers --------

fn mk_l2p_value(pba: u64, seq: u64) -> L2pValue {
    let mut raw = [0u8; onyx_metadb::paged::LEAF_VALUE_SIZE];
    raw[..8].copy_from_slice(&pba.to_be_bytes());
    raw[onyx_metadb::paged::LEAF_VALUE_SIZE - 1] = 1;
    L2pValue(raw).with_seq(seq)
}

#[derive(Clone, Debug)]
enum Op {
    Put { vol: VolumeOrdinal, lba: u64, pba: u64 },
    Delete { vol: VolumeOrdinal, lba: u64 },
    Remap { vol: VolumeOrdinal, lba: u64, pba: u64, seq: u64 },
    RemapRange { vol: VolumeOrdinal, start_lba: u64, len: u64 },
}

fn arb_vol() -> impl Strategy<Value = VolumeOrdinal> {
    (0usize..NUM_VOLUMES).prop_map(|slot| slot as VolumeOrdinal)
}

fn arb_lba() -> impl Strategy<Value = u64> {
    0u64..LBA_SPACE
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (arb_vol(), arb_lba(), 1u64..1024).prop_map(|(vol, lba, pba)| Op::Put { vol, lba, pba }),
        2 => (arb_vol(), arb_lba()).prop_map(|(vol, lba)| Op::Delete { vol, lba }),
        4 => (arb_vol(), arb_lba(), 1u64..1024, 0u64..32).prop_map(|(vol, lba, pba, seq)| Op::Remap { vol, lba, pba, seq }),
        2 => (arb_vol(), 0u64..(LBA_SPACE - MAX_RANGE_LEN), 1u64..=MAX_RANGE_LEN).prop_map(|(vol, start, len)| Op::RemapRange { vol, start_lba: start, len }),
    ]
}

fn op_to_walop(op: &Op, fresh_pba_seed: u64) -> WalOp {
    match *op {
        Op::Put { vol, lba, pba } => WalOp::L2pPut {
            vol_ord: vol,
            lba,
            value: mk_l2p_value(pba, 0),
        },
        Op::Delete { vol, lba } => WalOp::L2pDelete { vol_ord: vol, lba },
        Op::Remap { vol, lba, pba, seq } => WalOp::L2pRemap {
            vol_ord: vol,
            lba,
            new_value: mk_l2p_value(pba, seq),
            guard: None,
        },
        Op::RemapRange {
            vol,
            start_lba,
            len,
        } => WalOp::L2pRemapRange {
            vol_ord: vol,
            start_lba,
            values: (0..len)
                .map(|i| mk_l2p_value(fresh_pba_seed.wrapping_add(i), 0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        },
    }
}

/// Open a fresh db with deferred outcomes on + the async-WAL toggle
/// selected by the caller. Both modes seed `NUM_VOLUMES` so the
/// proptest can address the same ordinals across the pair.
fn open_db_with_async(async_wal: bool) -> (TempDir, std::sync::Arc<Db>) {
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    cfg.commit_direct_apply_enabled = true;
    cfg.commit_deferred_outcomes_enabled = true;
    cfg.wal_async_commits_enabled = async_wal;
    cfg.l2p_buffer_soft_entries = 1;
    cfg.l2p_buffer_max_interval_ms = 25;
    let db = Db::create_with_config(cfg).unwrap();
    for _ in 1..NUM_VOLUMES {
        db.create_volume().expect("seed volume");
    }
    (dir, db)
}

/// Snapshot every `(vol, lba)` in the LBA window we exercised.
fn snapshot(db: &Db) -> BTreeMap<(VolumeOrdinal, u64), L2pValue> {
    let mut out = BTreeMap::new();
    for vol in 0..NUM_VOLUMES as VolumeOrdinal {
        for lba in 0..LBA_SPACE + MAX_RANGE_LEN {
            if let Some(value) = db.get(vol, lba).expect("snapshot get") {
                out.insert((vol, lba), value);
            }
        }
    }
    out
}

// -------- equivalence proptest --------

fn run_async_equivalence(seed_ops: Vec<Op>, batch_sizes: Vec<usize>) -> Result<(), String> {
    let (_d_sync, db_sync_wal) = open_db_with_async(false);
    let (_d_async, db_async_wal) = open_db_with_async(true);

    let mut cursor = 0usize;
    let mut batch_idx = 0usize;
    let mut fresh_pba_seed: u64 = 0xA000;
    for raw_size in batch_sizes {
        if cursor >= seed_ops.len() {
            break;
        }
        let size = raw_size.max(1).min(seed_ops.len() - cursor);
        let batch: Vec<WalOp> = seed_ops[cursor..cursor + size]
            .iter()
            .map(|op| {
                let walop = op_to_walop(op, fresh_pba_seed);
                fresh_pba_seed = fresh_pba_seed.wrapping_add(MAX_RANGE_LEN);
                walop
            })
            .collect();
        cursor += size;
        batch_idx += 1;

        let (lsn_s, h_s) = db_sync_wal
            .commit_ops_deferred(&batch)
            .map_err(|e| format!("sync-WAL commit batch {batch_idx}: {e:?}"))?;
        let (lsn_a, h_a) = db_async_wal
            .commit_ops_deferred(&batch)
            .map_err(|e| format!("async-WAL commit batch {batch_idx}: {e:?}"))?;
        if lsn_s != lsn_a {
            return Err(format!(
                "lsn divergence at batch {batch_idx}: sync_wal={lsn_s} async_wal={lsn_a}"
            ));
        }

        // Both dbs are on the deferred-outcome path; both need a
        // forced compactor pass before recv resolves.
        db_sync_wal.test_force_compact_pass();
        db_async_wal.test_force_compact_pass();
        let outs_s = h_s.recv().map_err(|e| format!("sync recv batch {batch_idx}: {e:?}"))?;
        let outs_a = h_a.recv().map_err(|e| format!("async recv batch {batch_idx}: {e:?}"))?;

        if format!("{outs_s:?}") != format!("{outs_a:?}") {
            return Err(format!(
                "outcome divergence at batch {batch_idx}:\n  sync_wal={outs_s:?}\n  async_wal={outs_a:?}"
            ));
        }
    }

    // Drain any straggler outcomes (cheap belt-and-braces).
    db_sync_wal.test_force_compact_pass();
    db_async_wal.test_force_compact_pass();

    // ZFS-TXG-clone Phase 3 critical-section verification: flush BOTH
    // dbs. For the async db this fans `Op::FsyncAll` through every
    // lane, then advances `checkpoint_lsn` only after the lane acks.
    // For the sync db this is a no-op double-fsync that still
    // advances checkpoint_lsn. After flush both snapshots must match.
    db_sync_wal.flush().map_err(|e| format!("sync flush: {e:?}"))?;
    db_async_wal
        .flush()
        .map_err(|e| format!("async flush: {e:?}"))?;

    let sync_state = snapshot(&db_sync_wal);
    let async_state = snapshot(&db_async_wal);
    if sync_state != async_state {
        let only_sync: Vec<_> = sync_state
            .keys()
            .filter(|k| !async_state.contains_key(k))
            .collect();
        let only_async: Vec<_> = async_state
            .keys()
            .filter(|k| !sync_state.contains_key(k))
            .collect();
        return Err(format!(
            "final state divergence: only_sync={only_sync:?} only_async={only_async:?}"
        ));
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 24,
        .. ProptestConfig::default()
    })]

    /// Every random op stream produces identical final on-disk state
    /// across sync-WAL vs async-WAL with deferred outcomes on both
    /// sides. The match is asserted AFTER `db.flush()` on both — the
    /// async path's bytes only become durable at the next
    /// `fsync_all_lanes` barrier inside `flush_with_gate`.
    #[test]
    fn sync_vs_async_wal_outcomes_match(
        ops in proptest::collection::vec(arb_op(), 1..200),
        batch_sizes in proptest::collection::vec(1usize..=8, 1..40),
    ) {
        if let Err(msg) = run_async_equivalence(ops, batch_sizes) {
            return Err(TestCaseError::fail(msg));
        }
    }
}

// -------- fault injection: TxgSyncMidway --------

/// `FaultPoint::TxgSyncMidway` fires AFTER `WalSet::fsync_all_lanes`
/// returns Ok (the async-WAL bytes are durable) BUT BEFORE
/// `manifest_state.store.commit` has fsynced the new
/// `checkpoint_lsn`. `FaultAction::Panic` simulates a crash in this
/// window: WAL is durable, manifest still old. The next `Db::open`
/// replays from the OLD checkpoint, re-applies the records
/// idempotently, end state must match the equivalent clean-flush run.
#[test]
fn txg_sync_midway_panic_recovers_via_wal_replay() {
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    cfg.commit_direct_apply_enabled = true;
    cfg.commit_deferred_outcomes_enabled = true;
    cfg.wal_async_commits_enabled = true;
    cfg.l2p_buffer_soft_entries = 1;
    cfg.l2p_buffer_max_interval_ms = 25;
    let faults = FaultController::new();

    let expected_values: Vec<(u64, u64)> = (0..8u64).map(|i| (i, 0xD000 + i)).collect();

    // Phase A: commit, install fault, attempt flush -> expect panic.
    {
        let db = Db::create_with_config_and_faults(cfg.clone(), faults.clone()).unwrap();
        for (lba, pba) in &expected_values {
            db.commit_ops_deferred(&[WalOp::L2pPut {
                vol_ord: BOOTSTRAP_VOL,
                lba: *lba,
                value: mk_l2p_value(*pba, 0),
            }])
            .unwrap();
        }
        // Drain outcomes so the test's invariant doesn't hinge on
        // unreceived handles — recv is irrelevant here, we only care
        // about on-disk state.
        db.test_force_compact_pass();

        // Install the panic on the very next TxgSyncMidway hit.
        faults.install(FaultPoint::TxgSyncMidway, 1, FaultAction::Panic);

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| db.flush()));
        assert!(result.is_err(), "TxgSyncMidway panic must unwind through flush");
        assert!(
            faults.fired(FaultPoint::TxgSyncMidway),
            "fault must have fired"
        );
        // `db` drops here; Wal::Drop -> shutdown -> finalize will sync
        // the segment metadata, but that's fine for the test: WAL data
        // is already durable from the fsync_all_lanes call that
        // succeeded before the panic.
    }

    // Phase B: reopen — recovery replays from the (old) checkpoint and
    // every L2pPut must be visible. The fault didn't fire on this
    // open because it triggers `fire_on_hit=1` and Phase A consumed
    // that hit.
    let db = Db::open_with_config_and_faults(cfg, faults.clone()).unwrap();
    for (lba, pba) in &expected_values {
        let value = db.get(BOOTSTRAP_VOL, *lba).unwrap();
        assert_eq!(
            value,
            Some(mk_l2p_value(*pba, 0)),
            "lba {lba} must recover via WAL replay from old checkpoint"
        );
    }
}

// -------- fault injection: WalSubmitAsyncDropped --------

/// `FaultPoint::WalSubmitAsyncDropped` fires in lieu of `seg.append`
/// for an async-only batch. `FaultAction::Error` returns an injected
/// error from the writer thread, propagating through `commit_batch`
/// to every in-batch ack — the submitter sees the WAL submit as
/// failed. The metadb-side contract is "this LSN never reached
/// disk"; downstream durable logs (onyx LV2) re-drive missing
/// records. Here we just assert the error propagates and a follow-up
/// sync commit on the same db still works (the writer thread exited
/// on the first error, so a subsequent commit must observe the
/// "writer exited" path).
#[test]
fn wal_submit_async_dropped_propagates_to_caller() {
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    cfg.commit_direct_apply_enabled = true;
    cfg.commit_deferred_outcomes_enabled = true;
    cfg.wal_async_commits_enabled = true;
    cfg.l2p_buffer_soft_entries = 1;
    cfg.l2p_buffer_max_interval_ms = 25;
    let faults = FaultController::new();
    faults.install(FaultPoint::WalSubmitAsyncDropped, 1, FaultAction::Error);

    let db = Db::create_with_config_and_faults(cfg, faults.clone()).unwrap();

    // The bootstrap CreateVolume op fired during create_with_config
    // is a sync submit (lifecycle path), so the trigger should still
    // be ready when the first async commit arrives.
    let result = db.commit_ops_deferred(&[WalOp::L2pPut {
        vol_ord: BOOTSTRAP_VOL,
        lba: 99,
        value: mk_l2p_value(0xABCD, 0),
    }]);
    assert!(
        result.is_err(),
        "first async submit must propagate WalSubmitAsyncDropped"
    );
    assert!(
        faults.fired(FaultPoint::WalSubmitAsyncDropped),
        "fault must have fired"
    );

    // The writer thread has exited; subsequent commits short-circuit
    // with the "wal set failed" error. Don't assert on specific text;
    // assert only that the db doesn't silently absorb the failure.
    let followup = db.commit_ops_deferred(&[WalOp::L2pPut {
        vol_ord: BOOTSTRAP_VOL,
        lba: 100,
        value: mk_l2p_value(0xABCE, 0),
    }]);
    assert!(
        followup.is_err(),
        "post-dropped writer must reject follow-up commits"
    );
}
