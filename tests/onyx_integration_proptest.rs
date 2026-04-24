//! Mixed Onyx integration property test for Phase A S5.

use onyx_metadb::testing::onyx_model::{onyx_dedup_value, onyx_hash, onyx_l2p_value, OnyxRefModel};
use onyx_metadb::{ApplyOutcome, Config, Db, Pba, VolumeOrdinal};
use proptest::prelude::*;
use proptest::test_runner::{Config as ProptestConfig, TestRunner};
use tempfile::TempDir;

const MAX_VOL: VolumeOrdinal = 4;
const MAX_LBA: u64 = 512;
const MAX_PBA: Pba = 256;

#[derive(Clone, Debug)]
enum OnyxOp {
    Remap {
        vol_hint: VolumeOrdinal,
        lba: u64,
        pba: Pba,
        salt: u64,
        guard: GuardSpec,
    },
    RangeDelete {
        vol_hint: VolumeOrdinal,
        start: u64,
        len: u64,
    },
    CleanupDedup {
        pba_hint: Pba,
    },
    Reopen,
}

#[derive(Clone, Debug)]
enum GuardSpec {
    None,
    Hit,
    Miss,
}

fn arb_op(include_reopen: bool, include_range_delete: bool) -> impl Strategy<Value = OnyxOp> {
    let reopen_weight = if include_reopen { 1 } else { 0 };
    let range_weight = if include_range_delete { 5 } else { 0 };
    prop_oneof![
        60 => (0u16..MAX_VOL, 0u64..MAX_LBA, 1u64..=MAX_PBA, any::<u64>(), prop_oneof![Just(GuardSpec::None), Just(GuardSpec::Hit), Just(GuardSpec::Miss)])
            .prop_map(|(vol_hint, lba, pba, salt, guard)| OnyxOp::Remap { vol_hint, lba, pba, salt, guard }),
        range_weight => (0u16..MAX_VOL, 0u64..MAX_LBA, 1u64..32).prop_map(|(vol_hint, start, len)| OnyxOp::RangeDelete { vol_hint, start, len }),
        6 => (1u64..=MAX_PBA).prop_map(|pba_hint| OnyxOp::CleanupDedup { pba_hint }),
        reopen_weight => Just(OnyxOp::Reopen),
    ]
}

fn smoke_config() -> ProptestConfig {
    ProptestConfig {
        cases: env_u32("METADB_ONYX_PROPTEST_CASES", 8),
        ..ProptestConfig::default()
    }
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn choose_live(model: &OnyxRefModel, hint: VolumeOrdinal) -> VolumeOrdinal {
    let live = model.live_volumes();
    if live.contains(&hint) {
        hint
    } else {
        live[(hint as usize) % live.len()]
    }
}

fn seed_db(db: &Db, model: &mut OnyxRefModel) {
    for pba in 1..=MAX_PBA {
        db.incref_pba(pba, 1).unwrap();
        model.set_refcount_raw(pba, 1);
    }
    for i in 0..32u64 {
        let pba = (i % MAX_PBA) + 1;
        let hash = onyx_hash(i);
        let value = onyx_dedup_value(pba, i);
        let mut tx = db.begin();
        tx.put_dedup(hash, value);
        tx.register_dedup_reverse(pba, hash);
        tx.commit().unwrap();
        model.put_dedup_raw(hash, value);
    }
}

fn apply_op(
    op: &OnyxOp,
    db: &mut Db,
    model: &mut OnyxRefModel,
    cfg: &Config,
) -> Result<(), TestCaseError> {
    match *op {
        OnyxOp::Remap {
            vol_hint,
            lba,
            pba,
            salt,
            ref guard,
        } => {
            let vol = choose_live(model, vol_hint);
            let value = onyx_l2p_value(pba, salt);
            let guard = match guard {
                GuardSpec::None => None,
                GuardSpec::Hit => Some((pba, model.current_refcount(pba).min(1))),
                GuardSpec::Miss => Some((pba, model.current_refcount(pba).saturating_add(1))),
            };
            let expected = model.apply_l2p_remap(vol, lba, value, guard);
            prop_assert!(!expected.invalid);
            let mut tx = db.begin();
            tx.l2p_remap(vol, lba, value, guard);
            let (_, outcomes) = tx.commit_with_outcomes().unwrap();
            prop_assert_eq!(outcomes.len(), 1);
            prop_assert!(
                expected.matches_apply(&outcomes[0]),
                "op={op:?} outcome={:?} expected={expected:?}",
                outcomes[0]
            );
            if let ApplyOutcome::L2pRemap {
                freed_pba: Some(pba),
                ..
            } = outcomes[0]
            {
                db.cleanup_dedup_for_dead_pbas(&[pba]).unwrap();
                model.cleanup_dedup_for_dead_pbas(&[pba]);
            }
            if expected.applied {
                let hash = onyx_hash(salt);
                let dedup = onyx_dedup_value(pba, salt);
                let mut tx = db.begin();
                tx.put_dedup(hash, dedup);
                tx.register_dedup_reverse(pba, hash);
                tx.commit().unwrap();
                model.put_dedup_raw(hash, dedup);
            }
        }
        OnyxOp::RangeDelete {
            vol_hint,
            start,
            len,
        } => {
            let vol = choose_live(model, vol_hint);
            let end = start.saturating_add(len).min(MAX_LBA + 64);
            let freed = model.apply_range_delete(vol, start, end);
            db.range_delete(vol, start, end).unwrap();
            if !freed.is_empty() {
                db.cleanup_dedup_for_dead_pbas(&freed).unwrap();
                model.cleanup_dedup_for_dead_pbas(&freed);
            }
        }
        OnyxOp::CleanupDedup { pba_hint } => {
            let mut pbas = model.pending_dead_pbas();
            if pbas.is_empty() {
                pbas.push(pba_hint);
            }
            db.cleanup_dedup_for_dead_pbas(&pbas).unwrap();
            model.cleanup_dedup_for_dead_pbas(&pbas);
        }
        OnyxOp::Reopen => {
            let reopened = Db::open_with_config(cfg.clone()).unwrap();
            *db = reopened;
        }
    }
    Ok(())
}

fn run_ops(ops: &[OnyxOp]) -> Result<(), TestCaseError> {
    let dir = TempDir::new().unwrap();
    let cfg = Config::new(dir.path());
    let mut db = Db::create_with_config(cfg.clone()).unwrap();
    let mut model = OnyxRefModel::default();
    seed_db(&db, &mut model);

    for (idx, op) in ops.iter().enumerate() {
        apply_op(op, &mut db, &mut model, &cfg)
            .map_err(|err| TestCaseError::fail(format!("op #{idx} failed: {op:?}: {err}")))?;
        if idx % 250 == 0 {
            model
                .assert_db_matches(&db)
                .map_err(|err| TestCaseError::fail(err.to_string()))?;
        }
    }
    drop(db);
    let db = Db::open_with_config(cfg).unwrap();
    model
        .assert_db_matches(&db)
        .map_err(|err| TestCaseError::fail(err.to_string()))?;
    Ok(())
}

proptest! {
    #![proptest_config(smoke_config())]

    #[test]
    fn onyx_integration_proptest(ops in proptest::collection::vec(arb_op(true, true), 100..300)) {
        run_ops(&ops)?;
    }
}

#[test]
#[ignore = "S5 high-budget run: defaults to 256 cases x 10k..12k ops"]
fn onyx_integration_proptest_high_budget() {
    let cases = env_u32("METADB_ONYX_PROPTEST_CASES", 256);
    let min_ops = env_u32("METADB_ONYX_PROPTEST_MIN_OPS", 10_000) as usize;
    let max_ops = env_u32("METADB_ONYX_PROPTEST_MAX_OPS", 12_000) as usize;
    let strategy = proptest::collection::vec(arb_op(true, true), min_ops..=max_ops);
    let mut runner = TestRunner::new(ProptestConfig {
        cases,
        ..ProptestConfig::default()
    });
    runner.run(&strategy, |ops| run_ops(&ops)).unwrap();
}
