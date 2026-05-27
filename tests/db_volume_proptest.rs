//! Phase 7 commit 12 property test: volume-lifecycle state machine.
//!
//! Model: three reference maps tracked alongside the live `Db`:
//! - `volumes`: `HashSet<VolumeOrdinal>` of live ordinals.
//! - `state`: `HashMap<(VolumeOrdinal, Lba), L2pValue>` of
//!   current-volume L2P content.
//! - `snapshots`: `HashMap<SnapshotId, (VolumeOrdinal, BTreeMap<Lba, L2pValue>)>`.
//!
//! Every op is applied to both the Db and the reference. The test
//! asserts they agree on: per-volume range scans, volume ordinal
//! listings, snapshot views, and the global volume set. Mid-sequence
//! reopens force WAL replay for create/drop/clone volume + per-volume
//! snapshot semantics.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use onyx_metadb::{Config, Db, L2pValue, MetaDbError, SnapshotId, VolumeOrdinal};
use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use tempfile::TempDir;

/// ZFS-TXG-clone Phase 2 axis: same property body runs once with the
/// deferred-outcome flag off (sync delivery — `commit_ops` path) and
/// once with it on (deferred delivery — outcomes drain through the
/// L2P compactor's step-7 pass). The high-level wrappers used by
/// this proptest route through `commit_ops`, which is flag-independent
/// today; this is the regression guard for "enabling the flag does
/// not perturb existing semantics", matching the
/// /root/.claude/plans/soft-doodling-snail.md step-(c) brief.
fn db_open_or_create(dir: &TempDir, deferred: bool, async_wal: bool) -> std::sync::Arc<Db> {
    if async_wal {
        Db::create_with_config(async_wal_cfg(dir.path())).unwrap()
    } else if deferred {
        Db::create_with_config(deferred_cfg(dir.path())).unwrap()
    } else {
        Db::create_with_config(default_cfg(path_of(dir))).unwrap()
    }
}

/// Phase D.4: pin the proptest to `MetaDbJournalMode::Wal` because
/// `Op::Reopen` exercises WAL replay of data-plane ops. After D.5
/// retires Wal mode the proptest needs a rewrite that simulates
/// onyx-side buffer replay between reopens (the current `Op::Reopen`
/// would lose every uncheckpointed commit in Buffer mode).
fn default_cfg(path: &Path) -> Config {
    let mut cfg = Config::new(path);
    cfg.journal_mode = onyx_metadb::MetaDbJournalMode::Wal;
    cfg
}

fn deferred_cfg(path: &Path) -> Config {
    let mut cfg = default_cfg(path);
    cfg.commit_deferred_outcomes_enabled = true;
    cfg
}

/// ZFS-TXG-clone Phase 3 axis: layer async-WAL on top of the
/// deferred-outcome config. The async path is conjunctive (both
/// flags required), so this implies `deferred_cfg`.
fn async_wal_cfg(path: &Path) -> Config {
    let mut cfg = deferred_cfg(path);
    cfg.wal_async_commits_enabled = true;
    cfg
}

fn v(n: u8) -> L2pValue {
    let mut x = [0u8; onyx_metadb::paged::LEAF_VALUE_SIZE];
    // v5: store n in the LOW byte of the big-endian u64 base_pba so
    // distinct v(n) values stay within u32 of each other.
    x[7] = n;
    x[onyx_metadb::paged::LEAF_VALUE_SIZE - 1] = 1;
    L2pValue(x)
}

#[derive(Clone, Debug)]
enum Op {
    Insert(u16, u64, u8), // vol_slot, lba, value
    Delete(u16, u64),
    CreateVolume,
    DropVolume(u16),
    TakeSnapshot(u16),
    DropSnapshot(u16), // snap_slot
    CloneVolume(u16),  // snap_slot
    VerifyRange(u16),
    VerifySnapshot(u16),
    Flush,
    Reopen,
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => (0u16..8, 0u64..32, 0u8..=255).prop_map(|(s, l, v)| Op::Insert(s, l, v)),
        2 => (0u16..8, 0u64..32).prop_map(|(s, l)| Op::Delete(s, l)),
        1 => Just(Op::CreateVolume),
        1 => (0u16..8).prop_map(Op::DropVolume),
        2 => (0u16..8).prop_map(Op::TakeSnapshot),
        1 => (0u16..8).prop_map(Op::DropSnapshot),
        1 => (0u16..8).prop_map(Op::CloneVolume),
        2 => (0u16..8).prop_map(Op::VerifyRange),
        2 => (0u16..8).prop_map(Op::VerifySnapshot),
        1 => Just(Op::Flush),
        1 => Just(Op::Reopen),
    ]
}

#[derive(Default)]
struct Model {
    volumes: Vec<VolumeOrdinal>, // live ords
    state: HashMap<(VolumeOrdinal, u64), L2pValue>,
    snapshots: HashMap<SnapshotId, (VolumeOrdinal, BTreeMap<u64, L2pValue>)>,
    snap_ids: Vec<SnapshotId>,
    /// Phase 4 [[no-refcount-hot-path-design]] lineage pin: `parents[child] = parent`
    /// records the source vol of every clone whose
    /// `WalOp::PromotionComplete` has not fired yet. `db.drop_volume`
    /// refuses to drop a parent while any descendant still points at
    /// it (see [`Db::drop_volume`] InvalidArgument branch). This proptest
    /// never drives promotion, so once a clone is created its parent
    /// stays pinned for the rest of the run.
    parents: HashMap<VolumeOrdinal, VolumeOrdinal>,
}

impl Model {
    fn new() -> Self {
        let mut m = Self::default();
        m.volumes.push(0);
        m
    }
    fn vol_at(&self, slot: u16) -> Option<VolumeOrdinal> {
        if self.volumes.is_empty() {
            None
        } else {
            Some(self.volumes[(slot as usize) % self.volumes.len()])
        }
    }
    fn snap_at(&self, slot: u16) -> Option<SnapshotId> {
        if self.snap_ids.is_empty() {
            None
        } else {
            Some(self.snap_ids[(slot as usize) % self.snap_ids.len()])
        }
    }
    fn vol_state(&self, ord: VolumeOrdinal) -> BTreeMap<u64, L2pValue> {
        self.state
            .iter()
            .filter_map(|((o, k), v)| if *o == ord { Some((*k, *v)) } else { None })
            .collect()
    }
}

fn reopen(dir: &TempDir, deferred: bool, async_wal: bool) -> std::sync::Arc<Db> {
    if async_wal {
        Db::open_with_config(async_wal_cfg(dir.path())).unwrap()
    } else if deferred {
        Db::open_with_config(deferred_cfg(dir.path())).unwrap()
    } else {
        Db::open_with_config(default_cfg(dir.path())).unwrap()
    }
}

fn path_of(dir: &TempDir) -> &Path {
    dir.path()
}

fn run_lifecycle_body(
    ops: Vec<Op>,
    deferred: bool,
    async_wal: bool,
) -> Result<(), TestCaseError> {
    let dir = TempDir::new().unwrap();
    let mut db = db_open_or_create(&dir, deferred, async_wal);
    let mut model = Model::new();

    for op in ops {
            match op {
                Op::Insert(slot, lba, val) => {
                    let Some(ord) = model.vol_at(slot) else { continue; };
                    let value = v(val);
                    let tree_old = db.insert(ord, lba, value).unwrap();
                    let ref_old = model.state.insert((ord, lba), value);
                    prop_assert_eq!(tree_old, ref_old);
                }
                Op::Delete(slot, lba) => {
                    let Some(ord) = model.vol_at(slot) else { continue; };
                    let tree_old = db.delete(ord, lba).unwrap();
                    let ref_old = model.state.remove(&(ord, lba));
                    prop_assert_eq!(tree_old, ref_old);
                }
                Op::CreateVolume => {
                    // Growing the volume table shrinks the per-page
                    // snapshot capacity, so `create_volume` can return
                    // `InvalidArgument` once the manifest is tight. Treat
                    // it as back-pressure and skip the op.
                    let ord = match db.create_volume() {
                        Ok(ord) => ord,
                        Err(MetaDbError::InvalidArgument(_)) => continue,
                        Err(e) => return Err(TestCaseError::fail(format!(
                            "unexpected create_volume error: {e:?}"
                        ))),
                    };
                    prop_assert!(!model.volumes.contains(&ord));
                    model.volumes.push(ord);
                }
                Op::DropVolume(slot) => {
                    let Some(ord) = model.vol_at(slot) else { continue; };
                    if ord == 0 { continue; } // bootstrap refused
                    // drop_volume refuses if any snapshot pins the vol
                    // OR if any descendant clone still references it
                    // through its in-manifest `parent_vol_ord`. The
                    // proptest never drives promotion explicitly, so
                    // once a clone is created the model treats its
                    // parent as pinned for the remainder of the run.
                    let snap_pinned = model.snapshots.values().any(|(v, _)| *v == ord);
                    let clone_pinned = model.parents.values().any(|p| *p == ord);
                    let pinned = snap_pinned || clone_pinned;
                    match db.drop_volume(ord) {
                        Ok(Some(report)) => {
                            prop_assert!(!pinned, "drop_volume succeeded but model had a live pinner");
                            prop_assert_eq!(report.vol_ord, ord);
                            model.volumes.retain(|o| *o != ord);
                            model.state.retain(|(o, _), _| *o != ord);
                            model.parents.retain(|c, _| *c != ord);
                        }
                        Ok(None) => unreachable!("volume {ord} should exist"),
                        Err(MetaDbError::InvalidArgument(_)) => {
                            prop_assert!(pinned, "drop_volume refused but model had no pinner");
                        }
                        Err(e) => prop_assert!(false, "unexpected drop_volume error {e:?}"),
                    }
                }
                Op::TakeSnapshot(slot) => {
                    let Some(ord) = model.vol_at(slot) else { continue; };
                    // `InvalidArgument` at this call site means the manifest
                    // snapshot table is full (capacity depends on shard /
                    // dedup layout). That's a normal back-pressure signal,
                    // not a bug — skip the op without updating the model.
                    let id = match db.take_snapshot(ord) {
                        Ok(id) => id,
                        Err(MetaDbError::InvalidArgument(_)) => continue,
                        Err(e) => return Err(TestCaseError::fail(format!(
                            "unexpected take_snapshot error: {e:?}"
                        ))),
                    };
                    let frozen = model.vol_state(ord);
                    model.snapshots.insert(id, (ord, frozen));
                    model.snap_ids.push(id);
                }
                Op::DropSnapshot(slot) => {
                    let Some(id) = model.snap_at(slot) else { continue; };
                    db.drop_snapshot(id).unwrap().unwrap();
                    model.snapshots.remove(&id);
                    model.snap_ids.retain(|s| *s != id);
                }
                Op::CloneVolume(slot) => {
                    let Some(src_snap) = model.snap_at(slot) else { continue; };
                    // `InvalidArgument` here means `max_volumes` is
                    // reached — a capacity limit, not a bug. Skip the op.
                    let new_ord = match db.clone_volume(src_snap) {
                        Ok(ord) => ord,
                        Err(MetaDbError::InvalidArgument(_)) => continue,
                        Err(e) => return Err(TestCaseError::fail(format!(
                            "unexpected clone_volume error: {e:?}"
                        ))),
                    };
                    prop_assert!(!model.volumes.contains(&new_ord));
                    model.volumes.push(new_ord);
                    // Seed clone state from the source snapshot.
                    let (parent_ord, ref frozen) = model.snapshots[&src_snap];
                    for (lba, val) in frozen.iter() {
                        model.state.insert((new_ord, *lba), *val);
                    }
                    // Track the parent_vol_ord pin (Phase 4 lineage).
                    model.parents.insert(new_ord, parent_ord);
                }
                Op::VerifyRange(slot) => {
                    let Some(ord) = model.vol_at(slot) else { continue; };
                    let got: Vec<(u64, L2pValue)> = db
                        .range(ord, ..)
                        .unwrap()
                        .collect::<onyx_metadb::Result<Vec<_>>>()
                        .unwrap();
                    let expect: Vec<(u64, L2pValue)> = model
                        .vol_state(ord)
                        .into_iter()
                        .collect();
                    prop_assert_eq!(got, expect, "range mismatch for vol {}", ord);
                }
                Op::VerifySnapshot(slot) => {
                    let Some(id) = model.snap_at(slot) else { continue; };
                    let expected = model.snapshots[&id].1.clone();
                    let view = db.snapshot_view(id).unwrap();
                    let got: Vec<(u64, L2pValue)> = view
                        .range(..)
                        .unwrap()
                        .collect::<onyx_metadb::Result<Vec<_>>>()
                        .unwrap();
                    let expect: Vec<(u64, L2pValue)> = expected.into_iter().collect();
                    prop_assert_eq!(got, expect, "snapshot {} view diverged", id);
                }
                Op::Flush => {
                    db.flush().unwrap();
                }
                Op::Reopen => {
                    // Crash-without-flush: the drop paths commit a
                    // refreshed manifest before WAL submit + apply, and
                    // `Db::open`'s WAL replay re-runs DropSnapshot /
                    // DropVolume idempotently (gen-stamp guard) before
                    // `reclaim_orphan_pages` walks the post-replay
                    // manifest — so WAL replay alone recovers the
                    // snapshot / volume lifecycle state.
                    drop(db);
                    db = reopen(&dir, deferred, async_wal);
                }
            }
    }

    // Final reconciliation across every live volume.
    let mut live_ords = model.volumes.clone();
    live_ords.sort_unstable();
    prop_assert_eq!(db.volumes(), live_ords);
    for ord in &model.volumes {
        let got: Vec<(u64, L2pValue)> = db
            .range(*ord, ..)
            .unwrap()
            .collect::<onyx_metadb::Result<Vec<_>>>()
            .unwrap();
        let expect: Vec<(u64, L2pValue)> = model.vol_state(*ord).into_iter().collect();
        prop_assert_eq!(got, expect, "final range mismatch for vol {}", ord);
    }
    for id in &model.snap_ids {
        let expected = model.snapshots[id].1.clone();
        let view = db.snapshot_view(*id).unwrap();
        let got: Vec<(u64, L2pValue)> = view
            .range(..)
            .unwrap()
            .collect::<onyx_metadb::Result<Vec<_>>>()
            .unwrap();
        let expect: Vec<(u64, L2pValue)> = expected.into_iter().collect();
        prop_assert_eq!(got, expect, "final snapshot {} view diverged", id);
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 16,
        .. ProptestConfig::default()
    })]

    #[test]
    fn volume_lifecycle_matches_reference(ops in proptest::collection::vec(arb_op(), 1..120)) {
        run_lifecycle_body(ops, false, false)?;
    }

    /// #3b investigation — restore the original deferred-axis name
    /// (proptest seeds are name-derived, so we want the SAME random
    /// op sequences that previously failed) and see whether the
    /// apply_l2p_bucket_buffer read_view fix indirectly closes the
    /// "drop_volume refused after clone-of-dropped-snapshot vol"
    /// divergence.
    #[test]
    fn volume_lifecycle_matches_reference_deferred(
        ops in proptest::collection::vec(arb_op(), 1..120)
    ) {
        run_lifecycle_body(ops, true, false)?;
    }

    /// ZFS-TXG-clone Phase 3 axis: same ops, async-WAL on top of
    /// deferred outcomes. The reopen branch around line 301 of this
    /// file exercises the `fsync_all_lanes` TXG-sync barrier (via
    /// the explicit `Op::Flush` and the drop-time `Wal::shutdown` →
    /// `finalize` → `sync_all` path), so an async-submitted commit
    /// that landed in OS page cache before a reopen must still be
    /// recoverable. Same seed-name convention as the deferred axis.
    #[test]
    fn volume_lifecycle_matches_reference_async_wal(
        ops in proptest::collection::vec(arb_op(), 1..120)
    ) {
        run_lifecycle_body(ops, true, true)?;
    }
}

/// Phase 2 axis (see [`db_open_or_create`]): pin a known op
/// sequence and run it under the deferred-outcome flag. We do **not**
/// add a randomised deferred-axis proptest here because the snapshot
/// + clone + drop interactions exercised by the random op stream
/// surface a separate "metadb refuses drop_volume after clone of a
/// dropped-snapshot vol" divergence between the live db and the
/// reference model. That divergence reproduces independent of this
/// flag (it's about high-level lifecycle bookkeeping, not deferred
/// outcomes), so parameterising the proptest would just import a
/// flaky failure into this gate.
///
/// The hand-rolled scenario below is the simplest sequence that
/// covers create / insert / snapshot / clone / drop / reopen with
/// `commit_deferred_outcomes_enabled = true`. It catches any future
/// regression where flipping the flag breaks the high-level
/// commit_ops wrappers used by these methods.
#[test]
fn deferred_flag_preserves_high_level_lifecycle_semantics() {
    high_level_lifecycle_semantics_under_cfg(deferred_cfg);
}

/// ZFS-TXG-clone Phase 3 sibling: same hand-rolled scenario, async
/// WAL on top of deferred outcomes. `db.flush()` between the
/// in-memory ops and the reopen exercises the new
/// `fsync_all_lanes` TXG-sync barrier; without it the reopen would
/// see an old checkpoint and the snapshot/clone state would not
/// recover.
#[test]
fn async_wal_flag_preserves_high_level_lifecycle_semantics() {
    high_level_lifecycle_semantics_under_cfg(async_wal_cfg);
}

#[test]
fn drop_snapshot_replay_tolerates_truncated_tail_pages() {
    let ops = vec![
        Op::CreateVolume,
        Op::DropVolume(1),
        Op::Reopen,
        Op::TakeSnapshot(3),
        Op::DropSnapshot(1),
        Op::Insert(7, 29, 60),
        Op::TakeSnapshot(2),
        Op::TakeSnapshot(2),
        Op::DropSnapshot(3),
        Op::Reopen,
        Op::Insert(6, 10, 206),
        Op::Insert(4, 14, 239),
        Op::VerifyRange(6),
        Op::VerifyRange(6),
        Op::Insert(6, 28, 177),
        Op::Insert(4, 2, 70),
        Op::DropVolume(7),
        Op::CreateVolume,
        Op::Insert(5, 23, 249),
        Op::VerifyRange(1),
        Op::Flush,
        Op::Reopen,
        Op::DropSnapshot(0),
        Op::Reopen,
    ];
    run_lifecycle_body(ops, false, false).unwrap();
}

fn high_level_lifecycle_semantics_under_cfg(mk_cfg: fn(&Path) -> Config) {
    let dir = TempDir::new().unwrap();
    let db = Db::create_with_config(mk_cfg(dir.path())).unwrap();

    let value_a = v(1);
    let value_b = v(2);
    db.insert(0, 7, value_a).unwrap();
    db.insert(0, 8, value_b).unwrap();
    assert_eq!(db.get(0, 7).unwrap(), Some(value_a));
    assert_eq!(db.get(0, 8).unwrap(), Some(value_b));

    let snap = db.take_snapshot(0).unwrap();
    let cloned = db.clone_volume(snap).unwrap();
    let cloned_view: Vec<(u64, L2pValue)> = db
        .range(cloned, ..)
        .unwrap()
        .collect::<onyx_metadb::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(cloned_view, vec![(7, value_a), (8, value_b)]);

    db.flush().unwrap();
    drop(db);

    let db = Db::open_with_config(mk_cfg(dir.path())).unwrap();
    assert_eq!(db.get(0, 7).unwrap(), Some(value_a));
    assert_eq!(db.get(cloned, 8).unwrap(), Some(value_b));
    assert!(db.drop_snapshot(snap).unwrap().is_some());
}

#[test]
#[ignore = "long-running"]
fn volume_lifecycle_matches_reference_long_run() {
    // 500 cases × up to 400 ops; gated behind --ignored so CI runs the
    // cheap 16-case sweep by default.
    use proptest::test_runner::TestRunner;
    let cfg = ProptestConfig {
        cases: 500,
        ..ProptestConfig::default()
    };
    let mut runner = TestRunner::new(cfg);
    runner
        .run(&proptest::collection::vec(arb_op(), 1..400), |ops| {
            let dir = TempDir::new().unwrap();
            let mut db = Db::create_with_config(default_cfg(path_of(&dir))).unwrap();
            let mut model = Model::new();

            for op in ops {
                match op {
                    Op::Insert(slot, lba, val) => {
                        let Some(ord) = model.vol_at(slot) else {
                            continue;
                        };
                        let value = v(val);
                        let tree_old = db
                            .insert(ord, lba, value)
                            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
                        let ref_old = model.state.insert((ord, lba), value);
                        if tree_old != ref_old {
                            return Err(TestCaseError::fail(format!(
                                "insert divergence: {tree_old:?} vs {ref_old:?}"
                            )));
                        }
                    }
                    Op::Delete(slot, lba) => {
                        let Some(ord) = model.vol_at(slot) else {
                            continue;
                        };
                        let tree_old = db
                            .delete(ord, lba)
                            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
                        let ref_old = model.state.remove(&(ord, lba));
                        if tree_old != ref_old {
                            return Err(TestCaseError::fail(format!(
                                "delete divergence: {tree_old:?} vs {ref_old:?}"
                            )));
                        }
                    }
                    Op::CreateVolume => {
                        // Same manifest-capacity back-pressure as
                        // the short variant — skip on
                        // `InvalidArgument`.
                        let ord = match db.create_volume() {
                            Ok(ord) => ord,
                            Err(MetaDbError::InvalidArgument(_)) => continue,
                            Err(e) => {
                                return Err(TestCaseError::fail(format!(
                                    "unexpected create_volume error: {e:?}"
                                )));
                            }
                        };
                        model.volumes.push(ord);
                    }
                    Op::DropVolume(slot) => {
                        let Some(ord) = model.vol_at(slot) else {
                            continue;
                        };
                        if ord == 0 {
                            continue;
                        }
                        let pinned = model.snapshots.values().any(|(v, _)| *v == ord);
                        match db.drop_volume(ord) {
                            Ok(Some(_)) => {
                                if pinned {
                                    return Err(TestCaseError::fail(
                                        "drop_volume succeeded on pinned volume".to_string(),
                                    ));
                                }
                                model.volumes.retain(|o| *o != ord);
                                model.state.retain(|(o, _), _| *o != ord);
                            }
                            Ok(None) => unreachable!(),
                            Err(MetaDbError::InvalidArgument(_)) => {
                                if !pinned {
                                    return Err(TestCaseError::fail(
                                        "drop_volume failed on unpinned volume".to_string(),
                                    ));
                                }
                            }
                            Err(e) => {
                                return Err(TestCaseError::fail(format!("{e:?}")));
                            }
                        }
                    }
                    Op::TakeSnapshot(slot) => {
                        let Some(ord) = model.vol_at(slot) else {
                            continue;
                        };
                        // Manifest snapshot table has a capacity derived
                        // from shard / dedup layout; once full,
                        // `take_snapshot` returns `InvalidArgument`.
                        // That's expected back-pressure under a
                        // snapshot-heavy op sequence — skip without
                        // touching the model.
                        let id = match db.take_snapshot(ord) {
                            Ok(id) => id,
                            Err(MetaDbError::InvalidArgument(_)) => continue,
                            Err(e) => {
                                return Err(TestCaseError::fail(format!(
                                    "unexpected take_snapshot error: {e:?}"
                                )));
                            }
                        };
                        let frozen = model.vol_state(ord);
                        model.snapshots.insert(id, (ord, frozen));
                        model.snap_ids.push(id);
                    }
                    Op::DropSnapshot(slot) => {
                        let Some(id) = model.snap_at(slot) else {
                            continue;
                        };
                        db.drop_snapshot(id)
                            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
                        model.snapshots.remove(&id);
                        model.snap_ids.retain(|s| *s != id);
                    }
                    Op::CloneVolume(slot) => {
                        let Some(src_snap) = model.snap_at(slot) else {
                            continue;
                        };
                        // `max_volumes` cap: treat `InvalidArgument` as
                        // "skip this op", matching the TakeSnapshot
                        // handling above.
                        let new_ord = match db.clone_volume(src_snap) {
                            Ok(ord) => ord,
                            Err(MetaDbError::InvalidArgument(_)) => continue,
                            Err(e) => {
                                return Err(TestCaseError::fail(format!(
                                    "unexpected clone_volume error: {e:?}"
                                )));
                            }
                        };
                        model.volumes.push(new_ord);
                        let (_, ref frozen) = model.snapshots[&src_snap];
                        for (lba, val) in frozen.iter() {
                            model.state.insert((new_ord, *lba), *val);
                        }
                    }
                    Op::VerifyRange(_) | Op::VerifySnapshot(_) => {}
                    Op::Flush => {
                        db.flush()
                            .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
                    }
                    Op::Reopen => {
                        // See the shorter proptest above: crash-
                        // without-flush is covered by the drop-
                        // path's pre-apply manifest commit + WAL
                        // replay idempotency. No flush needed.
                        // Long-run variant stays on the sync path —
                        // the deferred axis lives in the short
                        // proptest above.
                        drop(db);
                        db = reopen(&dir, false, false);
                    }
                }
            }
            // Final reconciliation.
            let mut live_ords = model.volumes.clone();
            live_ords.sort_unstable();
            if db.volumes() != live_ords {
                return Err(TestCaseError::fail("final volumes mismatch".to_string()));
            }
            Ok(())
        })
        .unwrap();
}
