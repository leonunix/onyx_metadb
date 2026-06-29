//! BFG soak gate — sync vs deferred outcome equivalence.
//!
//! ## What this gates
//!
//! `Db::commit_ops_deferred` must produce the same on-disk state and the
//! same `(Lsn, Vec<ApplyOutcome>)` stream as `Db::commit_ops` (the sync
//! path), regardless of:
//!
//! 1. How many commit batches we emit.
//! 2. How those ops mix L2pPut / L2pDelete / L2pRemap (guarded and
//!    unguarded) / L2pRemapRange (random subset slice).
//! 3. Whether seq_guard refuses individual `L2pRemap`s — the deferred
//!    handle must still carry the same `applied=false, prev=Some(cur)`
//!    payload as the sync caller would have observed.
//!
//! `freed_pba` / `freed_pbas` must stay empty under 's
//! `lineage_gc_emit_freepbas = true` default: any populated freed-PBA
//! field is treated as a regression and the proptest fails.
//!
//! ## Why this is a soak gate
//!
//! The plan in `/root/.claude/plans/soft-doodling-snail.md` makes this
//! the bar before flipping `Config::commit_deferred_outcomes_enabled`
//! (metadb) + onyx `FlushConfig::commit_worker_deferred_outcomes` to
//! `true` by default. Both flags must stay opt-in until the 8h nvme-box
//! concurrent soak built around this property passes; see
//! `BFG deferred-outcome follow-up` in memory for the bigger picture.
//!
//! ## follow-up
//!
//! `DeferredOutcomeAggregator` was simplified to inline delivery (the
//! `L2pCompactor` it used to wake is gone). The
//! `FaultPoint::DeferredOutcomeDrainMidway` injection test was removed
//! with the parking lot; the equivalence proptest below is unaffected
//! because it asserts the visible API (`handle.recv()` returns the
//! same outcomes as the sync path), which inline delivery still
//! satisfies.

use std::collections::BTreeMap;

use onyx_metadb::op::WalOp;
use onyx_metadb::{ApplyOutcome, Config, Db, L2pValue, VolumeOrdinal};
use proptest::prelude::*;
use tempfile::TempDir;

const BOOTSTRAP_VOL: VolumeOrdinal = 0;
const NUM_VOLUMES: usize = 4;
const LBA_SPACE: u64 = 256;
const MAX_RANGE_LEN: u64 = 32;

// -------- helpers --------

/// Build a 36 B `L2pValue` from a numeric PBA + seq pair. Encoding
/// matches the convention used by every other onyx-metadb proptest
/// (head 8 B is PBA big-endian; trailer byte = 1 marks the value
/// "present" per LeafCompact v5 rules). Seq is stored via the same
/// channel `L2pValue::with_seq` uses so seq_guard sees a real value.
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
    // We seed N volumes up front. proptest gives us a slot index that
    // we use to pick one — wrapping if the model is smaller than the
    // strategy range. Keep slot strict so shrunken inputs are
    // reproducible.
    (0usize..NUM_VOLUMES).prop_map(|slot| slot as VolumeOrdinal)
}

fn arb_lba() -> impl Strategy<Value = u64> {
    0u64..LBA_SPACE
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        // Heavy on writes so the state churns; remaps drive the
        // seq_guard reject path we specifically care about.
        4 => (arb_vol(), arb_lba(), 1u64..1024).prop_map(|(vol, lba, pba)| Op::Put { vol, lba, pba }),
        2 => (arb_vol(), arb_lba()).prop_map(|(vol, lba)| Op::Delete { vol, lba }),
        4 => (arb_vol(), arb_lba(), 1u64..1024, 0u64..32).prop_map(|(vol, lba, pba, seq)| Op::Remap { vol, lba, pba, seq }),
        2 => (arb_vol(), 0u64..(LBA_SPACE - MAX_RANGE_LEN), 1u64..=MAX_RANGE_LEN).prop_map(|(vol, start, len)| Op::RemapRange { vol, start_lba: start, len }),
    ]
}

/// Convert one synthesized `Op` into the matching `WalOp`. Returns
/// `Some(_)` per generated op — `RemapRange` always carries
/// `len ∈ 1..=MAX_RANGE_LEN`, never empty.
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
            // Random seq drives the seq_guard reject path: when the
            // existing entry at (vol, lba) already has a higher seq,
            // the remap is refused and ApplyOutcome reports
            // applied=false, prev=Some(cur).
            new_value: mk_l2p_value(pba, seq),
            guard: None,
        },
        Op::RemapRange { vol, start_lba, len } => WalOp::L2pRemapRange {
            vol_ord: vol,
            start_lba,
            // Values share a synthetic base PBA so we can still
            // distinguish writes that land at different offsets
            // across runs. Seq=0 = never-reject (matches the
            // recovery-friendly accept-on-equality rule in
            // `seq_guard_rejects`).
            values: (0..len)
                .map(|i| mk_l2p_value(fresh_pba_seed.wrapping_add(i), 0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        },
    }
}

fn open_db(deferred: bool) -> (TempDir, std::sync::Arc<Db>) {
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.l2p_buffer_enabled = true;
    cfg.commit_direct_apply_enabled = true;
    cfg.commit_deferred_outcomes_enabled = deferred;
    // Keep the compactor's soft trigger low so a deferred handle can
    // never sit longer than a single forced pass.
    cfg.l2p_buffer_soft_entries = 1;
    cfg.l2p_buffer_max_interval_ms = 25;
    let db = Db::create_with_config(cfg).unwrap();
    // Seed extra volumes; ordinal 0 is the bootstrap volume that
    // create_with_config installed.
    for _ in 1..NUM_VOLUMES {
        db.create_volume().expect("seed volume");
    }
    (dir, db)
}

fn outcomes_eq(a: &[ApplyOutcome], b: &[ApplyOutcome]) -> Result<(), String> {
    if a.len() != b.len() {
        return Err(format!(
            "outcome stream length mismatch: sync={} deferred={}",
            a.len(),
            b.len()
        ));
    }
    for (i, (oa, ob)) in a.iter().zip(b.iter()).enumerate() {
        if format!("{oa:?}") != format!("{ob:?}") {
            return Err(format!(
                "outcome[{i}] divergence:\n  sync={oa:?}\n  deferred={ob:?}"
            ));
        }
        // invariant: freed_pba / freed_pbas must always be
        // empty under lineage_gc_emit_freepbas = true (the master
        // default). Any populated freed entry is a regression of
        // BFG deferred-outcome rollout.
        match oa {
            ApplyOutcome::L2pRemap { freed_pba, .. } => {
                if freed_pba.is_some() {
                    return Err(format!(
                        "outcome[{i}] L2pRemap.freed_pba populated in rc-neutral mode"
                    ));
                }
            }
            ApplyOutcome::L2pRemapRange { freed_pbas, .. } => {
                if !freed_pbas.is_empty() {
                    return Err(format!(
                        "outcome[{i}] L2pRemapRange.freed_pbas non-empty in rc-neutral mode"
                    ));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Snapshot every `(vol, lba)` in the LBA window we exercised so we
/// can prove sync and deferred dbs converge on the same on-disk state.
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

fn run_equivalence(seed_ops: Vec<Op>, batch_sizes: Vec<usize>) -> Result<(), String> {
    let (_d_sync, db_sync) = open_db(false);
    let (_d_def, db_def) = open_db(true);

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

        let (lsn_sync, h_sync) = db_sync
            .commit_ops_deferred(&batch)
            .map_err(|e| format!("sync commit batch {batch_idx}: {e:?}"))?;
        let (lsn_def, h_def) = db_def
            .commit_ops_deferred(&batch)
            .map_err(|e| format!("deferred commit batch {batch_idx}: {e:?}"))?;
        if lsn_sync != lsn_def {
            return Err(format!(
                "lsn divergence at batch {batch_idx}: sync={lsn_sync} deferred={lsn_def}"
            ));
        }

        let outs_sync = h_sync
            .recv()
            .map_err(|e| format!("sync recv batch {batch_idx}: {e:?}"))?;
        // Only the deferred db needs a compactor pass — sync delivery
        // is in-line. apply_l2p_bucket_buffer's per-Absent re-fetch of
        // `shard.read_view` (vs the previous capture-once pattern)
        // closes the race that previously required equalising both
        // dbs' buffer state to compare outcomes. See
        // [`deep_overlap_remap_range_prevs_match_without_sync_compact`].
        db_def.test_force_compact_pass();
        let outs_def = h_def
            .recv()
            .map_err(|e| format!("deferred recv batch {batch_idx}: {e:?}"))?;

        outcomes_eq(&outs_sync, &outs_def)
            .map_err(|msg| format!("batch {batch_idx}: {msg}"))?;

        let sync_applied = db_sync.last_applied_lsn();
        let def_applied = db_def.last_applied_lsn();
        if sync_applied != def_applied {
            return Err(format!(
                "last_applied_lsn divergence batch {batch_idx}: sync={sync_applied} deferred={def_applied}"
            ));
        }
    }

    // Drain anything still pending on the deferred db (should always
    // be empty by this point thanks to the per-batch force_compact,
    // but cheap insurance against future regressions).
    db_def.test_force_compact_pass();

    let sync_state = snapshot(&db_sync);
    let def_state = snapshot(&db_def);
    if sync_state != def_state {
        let only_sync: Vec<_> = sync_state.keys().filter(|k| !def_state.contains_key(k)).collect();
        let only_def: Vec<_> = def_state.keys().filter(|k| !sync_state.contains_key(k)).collect();
        return Err(format!(
            "final state divergence: only_sync={only_sync:?} only_def={only_def:?}"
        ));
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 24,
        // Equivalence cases here exercise WAL + apply + compact_pass
        // per batch and snapshot the full LBA window at the end. 24
        // cases at ~100 ops apiece keeps a single `cargo test` run
        // under a minute on commodity hardware while still covering
        // the seq_guard reject, range-shaped, and delete corners
        // weighted by `arb_op`.
        .. ProptestConfig::default()
    })]

    /// Every random op stream produces identical outcomes and final
    /// on-disk state on the sync vs deferred paths.
    #[test]
    fn sync_vs_deferred_outcomes_match(
        ops in proptest::collection::vec(arb_op(), 1..200),
        batch_sizes in proptest::collection::vec(1usize..=8, 1..40),
    ) {
        if let Err(msg) = run_equivalence(ops, batch_sizes) {
            return Err(TestCaseError::fail(msg));
        }
    }
}

// -------- fault injection: DeferredOutcomeDrainMidway --------
//
// BFG: the `DeferredOutcomeAggregator` no
// longer parks staged outcomes — `stage()` populates the handle's
// channel inline and returns. There is no later drain to inject a
// fault into, so the `DeferredOutcomeDrainMidway` fault point is
// now dead code; the test that exercised it (and asserted the
// pop-then-panic-then-disconnect chain) has been retired with the
// aggregator's pending map. See `src/db/commit/outcomes.rs` for
// the inline-delivery rationale.

/// Investigation test for BFG deferred-outcome follow-up follow-up
/// #3a: the `sync_vs_deferred_outcomes_match` proptest needed
/// `db_sync.test_force_compact_pass()` to be called between batches
/// alongside the deferred db's pass, otherwise `prev` values for an
/// overlapping L2pRemapRange diverged. This test pins the smallest
/// scenario that surfaces the divergence so the root cause can be
/// isolated without proptest-shrink noise:
///
///   1. Both dbs receive identical L2pRemapRange ops.
///   2. Only `db_def` runs `test_force_compact_pass` between batches.
///   3. A later overlapping L2pRemapRange's `prevs` must match
///      between the two dbs.
///
/// If this assertion fires, the metadb buffer-vs-tree apply layers
/// observe stale state at different timing windows — that would be a
/// real correctness gap independent of . If it passes, the
/// proptest divergence was actually noise from the background
/// compactor racing with apply, and the workaround
/// (`db_sync.test_force_compact_pass()` per batch) is purely a test
/// stability tweak.
#[test]
fn overlap_remap_range_prevs_match_without_sync_compact() {
    let (_d_sync, db_sync) = open_db(false);
    let (_d_def, db_def) = open_db(true);

    let prime: Vec<WalOp> = vec![WalOp::L2pRemapRange {
        vol_ord: BOOTSTRAP_VOL,
        start_lba: 10,
        values: (0..10u64)
            .map(|i| mk_l2p_value(0x1000 + i, 0))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    }];
    let (_, h_sync) = db_sync.commit_ops_deferred(&prime).unwrap();
    let (_, h_def) = db_def.commit_ops_deferred(&prime).unwrap();
    h_sync.recv().unwrap();
    db_def.test_force_compact_pass();
    h_def.recv().unwrap();

    // Overlapping range — lba 5..25 — should see prev values from
    // `prime` at offsets 5..15 (lba 10..20).
    let overlap: Vec<WalOp> = vec![WalOp::L2pRemapRange {
        vol_ord: BOOTSTRAP_VOL,
        start_lba: 5,
        values: (0..20u64)
            .map(|i| mk_l2p_value(0x2000 + i, 0))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
    }];
    let (_, h_sync) = db_sync.commit_ops_deferred(&overlap).unwrap();
    let (_, h_def) = db_def.commit_ops_deferred(&overlap).unwrap();
    let outs_sync = h_sync.recv().unwrap();
    db_def.test_force_compact_pass();
    let outs_def = h_def.recv().unwrap();

    assert_eq!(
        format!("{outs_sync:?}"),
        format!("{outs_def:?}"),
        "single-overlap L2pRemapRange must produce identical prevs"
    );
}

/// Deeper version of [`overlap_remap_range_prevs_match_without_sync_compact`]:
/// stack many overlapping ranges so the sync db's L2pBuffer accumulates
/// hundreds of entries before the next observation. If the bug is in
/// buffer-lookup ordering or eviction, depth makes it more likely to
/// surface.
#[test]
fn deep_overlap_remap_range_prevs_match_without_sync_compact() {
    let (_d_sync, db_sync) = open_db(false);
    let (_d_def, db_def) = open_db(true);

    // 32 overlapping batches, each writing 20 LBAs sliding by 3.
    for batch_idx in 0..32u64 {
        let start = batch_idx * 3;
        let base_pba = 0x10_0000 + batch_idx * 100;
        let ops: Vec<WalOp> = vec![WalOp::L2pRemapRange {
            vol_ord: BOOTSTRAP_VOL,
            start_lba: start,
            values: (0..20u64)
                .map(|i| mk_l2p_value(base_pba + i, 0))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }];
        let (_, h_sync) = db_sync.commit_ops_deferred(&ops).unwrap();
        let (_, h_def) = db_def.commit_ops_deferred(&ops).unwrap();
        let outs_sync = h_sync.recv().unwrap();
        db_def.test_force_compact_pass();
        let outs_def = h_def.recv().unwrap();

        if format!("{outs_sync:?}") != format!("{outs_def:?}") {
            panic!(
                "batch {batch_idx} prev divergence\nsync={outs_sync:?}\ndef ={outs_def:?}"
            );
        }
    }
}
/// controller has no trigger installed — `drain_up_to_lsn` still
/// delivers staged outcomes normally and the panic path stays
/// unreachable.
#[test]
fn drain_midway_noop_without_trigger() {
    let (_d, db) = open_db(true);
    for i in 0..4u64 {
        let (_, handle) = db
            .commit_ops_deferred(&[WalOp::L2pPut {
                vol_ord: BOOTSTRAP_VOL,
                lba: i,
                value: mk_l2p_value(0xC000 + i, 0),
            }])
            .unwrap();
        db.test_force_compact_pass();
        let outs = handle.recv().unwrap();
        assert_eq!(outs.len(), 1);
    }
}
