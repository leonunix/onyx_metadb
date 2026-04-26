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

use onyx_metadb::{Db, L2pValue, MetaDbError, SnapshotId, VolumeOrdinal};
use proptest::prelude::*;
use tempfile::TempDir;

fn v(n: u8) -> L2pValue {
    let mut x = [0u8; 28];
    x[0] = n;
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

fn reopen(dir: &TempDir) -> Db {
    Db::open(dir.path()).unwrap()
}

fn path_of(dir: &TempDir) -> &Path {
    dir.path()
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 16,
        .. ProptestConfig::default()
    })]

    #[test]
    fn volume_lifecycle_matches_reference(ops in proptest::collection::vec(arb_op(), 1..120)) {
        let dir = TempDir::new().unwrap();
        let mut db = Db::create(path_of(&dir)).unwrap();
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
                    // drop_volume refuses if any snapshot pins the vol.
                    let pinned = model.snapshots.values().any(|(v, _)| *v == ord);
                    match db.drop_volume(ord) {
                        Ok(Some(report)) => {
                            prop_assert!(!pinned, "drop_volume succeeded but model had live snapshot");
                            prop_assert_eq!(report.vol_ord, ord);
                            model.volumes.retain(|o| *o != ord);
                            model.state.retain(|(o, _), _| *o != ord);
                        }
                        Ok(None) => unreachable!("volume {ord} should exist"),
                        Err(MetaDbError::InvalidArgument(_)) => {
                            prop_assert!(pinned, "drop_volume refused but model had no snapshot pinning it");
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
                    let (_, ref frozen) = model.snapshots[&src_snap];
                    for (lba, val) in frozen.iter() {
                        model.state.insert((new_ord, *lba), *val);
                    }
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
                    db = reopen(&dir);
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
    }
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
            let mut db = Db::create(path_of(&dir)).unwrap();
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
                        drop(db);
                        db = reopen(&dir);
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
