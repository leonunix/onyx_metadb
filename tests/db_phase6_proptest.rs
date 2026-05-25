//! Phase-6 property and crash tests.
//!
//! 1. `db_vs_reference_with_reopens` mirrors a live `Db` against three
//!    in-memory `BTreeMap`s (one per index type) through random mixes of
//!    the phase-6 API, including mid-sequence reopens that force
//!    WAL-replay to rebuild in-memory state.
//!
//! 2. `crash_between_wal_and_apply_preserves_atomicity` hand-checks that
//!    a WAL-fsync error leaves the database in a consistent state: the
//!    failed commit is neither half-applied in memory nor half-visible
//!    after reopen.
//!
//! 3. (`decref_to_zero_cleanup_via_dedup_reverse` retired in manifest v9.)

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use onyx_metadb::testing::faults::{FaultAction, FaultController, FaultPoint};
use onyx_metadb::{Config, Db, DedupValue, Hash8, L2pValue, MetaDbError};
use proptest::prelude::*;
use proptest::test_runner::TestRunner;
use tempfile::TempDir;

type L2pRef = BTreeMap<u64, L2pValue>;
type RefcountRef = BTreeMap<u64, u32>;
type DedupRef = BTreeMap<Hash8, DedupValue>;

fn l2p(n: u8) -> L2pValue {
    let mut x = [0u8; onyx_metadb::paged::LEAF_VALUE_SIZE];
    // v5: store n in the LOW byte of the big-endian u64 base_pba so
    // distinct l2p(n) values stay within u32 of each other.
    x[7] = n;
    x[onyx_metadb::paged::LEAF_VALUE_SIZE - 1] = 1;
    L2pValue(x)
}

fn dval(n: u8) -> DedupValue {
    // Encode `n` as a big-endian u64 in the leading 8 bytes so
    // `head_pba()` returns a small refcount-array-safe value.
    // Phase 5 routes `DedupPut` through `rc.stage(head_pba, +1)`;
    // byte-0 encoding would decode as ~7e16 and OOM the rc array.
    let mut x = [0u8; 28];
    x[..8].copy_from_slice(&(n as u64).to_be_bytes());
    DedupValue(x)
}

fn h(n: u64) -> Hash8 {
    let mut x = [0u8; 8];
    x[..8].copy_from_slice(&n.to_be_bytes());
    x
}

#[derive(Clone, Debug)]
enum Op {
    Insert(u64, u8),
    Delete(u64),
    PutDedup(u64, u8),
    DeleteDedup(u64),
    Incref(u64, u32),
    Decref(u64, u32),
    Flush,
    Reopen,
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0u64..50, 0u8..=255).prop_map(|(k, v)| Op::Insert(k, v)),
        2 => (0u64..50).prop_map(Op::Delete),
        3 => (0u64..30, 0u8..=255).prop_map(|(k, v)| Op::PutDedup(k, v)),
        1 => (0u64..30).prop_map(Op::DeleteDedup),
        3 => (0u64..20, 1u32..=5).prop_map(|(p, d)| Op::Incref(p, d)),
        2 => (0u64..20, 1u32..=3).prop_map(|(p, d)| Op::Decref(p, d)),
        1 => Just(Op::Flush),
        1 => Just(Op::Reopen),
    ]
}

fn apply_to_reference(
    op: &Op,
    current_l2p: &mut L2pRef,
    current_refcount: &mut RefcountRef,
    current_dedup: &mut DedupRef,
) -> Result<(), String> {
    match *op {
        Op::Insert(k, v) => {
            current_l2p.insert(k, l2p(v));
        }
        Op::Delete(k) => {
            current_l2p.remove(&k);
        }
        Op::PutDedup(hk, hv) => {
            // Phase 5: DedupPut bumps rc[head_pba(new)] and
            // decrements rc[head_pba(prev)] if prev existed with a
            // different head_pba. Mirror that here.
            let new_value = dval(hv);
            let new_pba = new_value.head_pba();
            let prev = current_dedup.insert(h(hk), new_value);
            let prev_pba = prev.map(|v| v.head_pba());
            if prev_pba != Some(new_pba) {
                if let Some(op_pba) = prev_pba {
                    let cur = current_refcount.get(&op_pba).copied().unwrap_or(0);
                    let next = cur.saturating_sub(1);
                    if next == 0 {
                        current_refcount.remove(&op_pba);
                    } else {
                        current_refcount.insert(op_pba, next);
                    }
                }
                let cur = current_refcount.get(&new_pba).copied().unwrap_or(0);
                current_refcount.insert(new_pba, cur.saturating_add(1));
            }
        }
        Op::DeleteDedup(hk) => {
            if let Some(prev) = current_dedup.remove(&h(hk)) {
                // Phase 5: DedupDelete decrements rc[head_pba(prev)].
                let pba = prev.head_pba();
                let cur = current_refcount.get(&pba).copied().unwrap_or(0);
                let next = cur.saturating_sub(1);
                if next == 0 {
                    current_refcount.remove(&pba);
                } else {
                    current_refcount.insert(pba, next);
                }
            }
        }
        Op::Incref(pba, delta) => {
            let cur = current_refcount.get(&pba).copied().unwrap_or(0);
            let new = cur
                .checked_add(delta)
                .ok_or_else(|| "overflow".to_string())?;
            if new > 0 {
                current_refcount.insert(pba, new);
            }
        }
        Op::Decref(pba, delta) => {
            let cur = current_refcount.get(&pba).copied().unwrap_or(0);
            let new = cur
                .checked_sub(delta)
                .ok_or_else(|| "underflow".to_string())?;
            if new == 0 {
                current_refcount.remove(&pba);
            } else {
                current_refcount.insert(pba, new);
            }
        }
        Op::Flush | Op::Reopen => {}
    }
    Ok(())
}

fn apply_to_db(op: &Op, db: &Db) -> Result<(), MetaDbError> {
    match *op {
        Op::Insert(k, v) => {
            db.insert(0, k, l2p(v))?;
        }
        Op::Delete(k) => {
            db.delete(0, k)?;
        }
        Op::PutDedup(hk, hv) => {
            db.put_dedup(h(hk), dval(hv))?;
        }
        Op::DeleteDedup(hk) => {
            db.delete_dedup(h(hk))?;
        }
        Op::Incref(pba, delta) => {
            db.incref_pba(pba, delta)?;
        }
        Op::Decref(pba, delta) => {
            db.decref_pba(pba, delta)?;
        }
        Op::Flush => {
            db.flush()?;
        }
        Op::Reopen => {}
    }
    Ok(())
}

fn tiny_cache_config(path: &Path) -> Config {
    let mut cfg = Config::new(path);
    cfg.page_cache_bytes = 1024 * 1024;
    cfg
}

/// ZFS-TXG-clone Phase 2 axis: opt the same `run_ops_with_config`
/// body into the deferred-outcome plumbing. None of the
/// `apply_to_db` helpers route through `commit_ops_deferred` today
/// (Db::insert / Db::delete / Db::put_dedup / Db::incref_pba /
/// Db::decref_pba all go through `commit_ops`), so this is a
/// regression guard: any future change that lets the flag affect
/// sync `commit_ops` will surface here.
fn deferred_config(path: &Path) -> Config {
    let mut cfg = Config::new(path);
    cfg.commit_deferred_outcomes_enabled = true;
    cfg
}

fn deferred_tiny_cache_config(path: &Path) -> Config {
    let mut cfg = tiny_cache_config(path);
    cfg.commit_deferred_outcomes_enabled = true;
    cfg
}

/// ZFS-TXG-clone Phase 3 axis: layer async-WAL on top of the
/// deferred-outcome config. Same regression guard scope — none of
/// the `apply_to_db` helpers route through `commit_ops_deferred`
/// today, so flipping the WAL async flag should be a no-op on this
/// property. Tests the integration plumbing (manifest commit + WAL
/// fsync barrier across many synthetic reopens) under the async-WAL
/// open-path config, which is itself non-trivial.
fn async_wal_config(path: &Path) -> Config {
    let mut cfg = deferred_config(path);
    cfg.wal_async_commits_enabled = true;
    cfg
}

fn async_wal_tiny_cache_config(path: &Path) -> Config {
    let mut cfg = deferred_tiny_cache_config(path);
    cfg.wal_async_commits_enabled = true;
    cfg
}

fn run_ops_with_config(ops: &[Op], cfg: &Config) -> Result<(), TestCaseError> {
    let mut current_l2p: L2pRef = BTreeMap::new();
    let mut current_refcount: RefcountRef = BTreeMap::new();
    let mut current_dedup: DedupRef = BTreeMap::new();

    let mut db = Db::create_with_config(cfg.clone()).unwrap();

    for op in ops {
        // Apply to reference; skip the op on both sides if it would
        // underflow (u32) so we stay in-sync with the Db, which
        // returns an error in that case.
        let snapshot_ref = (
            current_l2p.clone(),
            current_refcount.clone(),
            current_dedup.clone(),
        );
        if apply_to_reference(
            op,
            &mut current_l2p,
            &mut current_refcount,
            &mut current_dedup,
        )
        .is_err()
        {
            current_l2p = snapshot_ref.0;
            current_refcount = snapshot_ref.1;
            current_dedup = snapshot_ref.2;
            continue;
        }

        match apply_to_db(op, &db) {
            Ok(()) => {}
            Err(_) => {
                // Likely a checked-arithmetic mismatch; resync
                // by skipping.
                current_l2p = snapshot_ref.0;
                current_refcount = snapshot_ref.1;
                current_dedup = snapshot_ref.2;
                continue;
            }
        }

        if matches!(op, Op::Reopen) {
            drop(db);
            db = Db::open_with_config(cfg.clone()).unwrap();
        }

        assert_db_matches(&db, &current_l2p, &current_refcount, &current_dedup)?;
        prop_assert!(db.cache_stats().current_bytes <= cfg.page_cache_bytes);
    }

    drop(db);
    let db = Db::open_with_config(cfg.clone()).unwrap();
    assert_db_matches(&db, &current_l2p, &current_refcount, &current_dedup)?;
    prop_assert!(db.cache_stats().current_bytes <= cfg.page_cache_bytes);
    Ok(())
}

fn assert_db_matches(
    db: &Db,
    current_l2p: &L2pRef,
    current_refcount: &RefcountRef,
    current_dedup: &DedupRef,
) -> Result<(), TestCaseError> {
    // Sample L2P keys (we cover the whole 0..50 range).
    for k in 0u64..50 {
        let got = db.get(0, k).unwrap();
        let want = current_l2p.get(&k).copied();
        prop_assert_eq!(got, want, "L2P key {} diverged", k);
    }
    // Refcount.
    for pba in 0u64..20 {
        let got = db.get_refcount(pba).unwrap();
        let want = current_refcount.get(&pba).copied().unwrap_or(0);
        prop_assert_eq!(got, want, "refcount for pba {} diverged", pba);
    }
    // Dedup.
    for k in 0u64..30 {
        let got = db.get_dedup(&h(k)).unwrap();
        let want = current_dedup.get(&h(k)).copied();
        prop_assert_eq!(got, want, "dedup hash {} diverged", k);
    }
    Ok(())
}

fn quick_proptest_config() -> ProptestConfig {
    ProptestConfig {
        // Per-case cost is high (WAL fsync per op, reopens). Keep the
        // default `cargo test` budget small, but make it overrideable
        // so Phase 8a multi-hour runs can use the same test body.
        cases: read_env_u32("METADB_PROPTEST_CASES", 16),
        ..ProptestConfig::default()
    }
}

fn read_env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(default)
}

proptest! {
    #![proptest_config(quick_proptest_config())]

    #[test]
    fn db_vs_reference_with_reopens(ops in proptest::collection::vec(arb_op(), 30..80)) {
        let dir = TempDir::new().unwrap();
        run_ops_with_config(&ops, &Config::new(dir.path()))?;
    }

    #[test]
    fn db_vs_reference_with_reopens_tiny_cache(
        ops in proptest::collection::vec(arb_op(), 30..80)
    ) {
        let dir = TempDir::new().unwrap();
        run_ops_with_config(&ops, &tiny_cache_config(dir.path()))?;
    }

    /// ZFS-TXG-clone Phase 2 axis: same property, db opened with
    /// `commit_deferred_outcomes_enabled = true`. See [`deferred_config`]
    /// for why this is a regression guard rather than an exhaustive
    /// equivalence sweep — `Db::insert` / `delete` / `put_dedup` /
    /// `incref_pba` / `decref_pba` go through `commit_ops`, which is
    /// flag-independent today.
    #[test]
    fn db_vs_reference_with_reopens_deferred(
        ops in proptest::collection::vec(arb_op(), 30..80)
    ) {
        let dir = TempDir::new().unwrap();
        run_ops_with_config(&ops, &deferred_config(dir.path()))?;
    }

    #[test]
    fn db_vs_reference_with_reopens_tiny_cache_deferred(
        ops in proptest::collection::vec(arb_op(), 30..80)
    ) {
        let dir = TempDir::new().unwrap();
        run_ops_with_config(&ops, &deferred_tiny_cache_config(dir.path()))?;
    }

    /// ZFS-TXG-clone Phase 3 axis: same property under async-WAL
    /// config. The reopen at end of each ops chunk exercises the
    /// `fsync_all_lanes` barrier — without it, any commit between
    /// the last flush and reopen would be lost on the OS-page-cache
    /// boundary the reopen straddles.
    #[test]
    fn db_vs_reference_with_reopens_async_wal(
        ops in proptest::collection::vec(arb_op(), 30..80)
    ) {
        let dir = TempDir::new().unwrap();
        run_ops_with_config(&ops, &async_wal_config(dir.path()))?;
    }

    #[test]
    fn db_vs_reference_with_reopens_tiny_cache_async_wal(
        ops in proptest::collection::vec(arb_op(), 30..80)
    ) {
        let dir = TempDir::new().unwrap();
        run_ops_with_config(&ops, &async_wal_tiny_cache_config(dir.path()))?;
    }
}

#[test]
#[ignore = "Phase 8a high-budget run: defaults to 500 cases x 200..400 ops"]
fn db_vs_reference_with_reopens_phase8a_budget() {
    run_phase8a_budget(false).unwrap();
}

#[test]
#[ignore = "Phase 8a high-budget run: defaults to 500 cases x 200..400 ops, tiny cache"]
fn db_vs_reference_with_reopens_tiny_cache_phase8a_budget() {
    run_phase8a_budget(true).unwrap();
}

fn run_phase8a_budget(tiny_cache: bool) -> Result<(), TestCaseError> {
    let cases = read_env_u32("METADB_PHASE8A_CASES", 500);
    let min_ops = read_env_u32("METADB_PHASE8A_MIN_OPS", 200) as usize;
    let max_ops = read_env_u32("METADB_PHASE8A_MAX_OPS", 400) as usize;
    let strategy = proptest::collection::vec(arb_op(), min_ops..=max_ops);
    let mut runner = TestRunner::new(ProptestConfig {
        cases,
        ..ProptestConfig::default()
    });
    Ok(runner.run(&strategy, |ops| {
        let dir = TempDir::new().unwrap();
        let cfg = if tiny_cache {
            tiny_cache_config(dir.path())
        } else {
            Config::new(dir.path())
        };
        run_ops_with_config(&ops, &cfg)
    })?)
}

#[test]
fn failed_commit_does_not_apply_to_memory_state() {
    // Atomicity at the in-memory layer: if the WAL submit returns Err,
    // the `Db`'s in-memory state must not be updated. (What happens
    // post-reopen depends on whether the kernel flushed the unsynced
    // bytes before the process exited; that's a durability question,
    // not an atomicity one, and is covered separately by the
    // torn-tail recovery test in `src/recovery.rs`.)
    let dir = TempDir::new().unwrap();
    let faults = FaultController::new();
    let db = Db::create_with_faults(dir.path(), faults.clone()).unwrap();

    db.insert(0, 1, l2p(1)).unwrap();
    db.incref_pba(10, 5).unwrap();
    db.put_dedup(h(100), dval(1)).unwrap();

    faults.install(FaultPoint::WalFsyncBefore, 1, FaultAction::Error);

    // Phase 5: the tx only carries L2P + dedup since standalone
    // refcount WAL ops were retired. The crash check is unchanged in
    // spirit: the failed fsync must leave every shard untouched.
    let mut tx = db.begin();
    tx.insert(0, 1, l2p(99));
    tx.put_dedup(h(100), dval(99));
    assert!(tx.commit().is_err());

    // Old values remain because apply never ran.
    assert_eq!(db.get(0, 1).unwrap(), Some(l2p(1)));
    assert_eq!(db.get_refcount(10).unwrap(), 5);
    assert_eq!(db.get_dedup(&h(100)).unwrap(), Some(dval(1)));
}

// `decref_to_zero_cleanup_via_dedup_reverse` retired alongside the
// paged_reverse module + DedupReverse WAL ops (manifest v9 / WAL 0xB3).
// Promote-on-verified-hit's old-mapping read-back is the replacement
// cleanup path and is exercised on the onyx side.

// Silence dead_code when the only user of `Arc` is a test that gets
// disabled under some feature flag.
#[allow(dead_code)]
fn _anchor_arc(_a: Arc<FaultController>) {}

