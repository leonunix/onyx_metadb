//! Property tests for Db snapshot semantics.
//!
//! Model: two HashMap-backed references tracked alongside the Db:
//! - `current`: what Db::range(..) should yield right now.
//! - `snapshots`: `SnapshotId -> BTreeMap` of what
//!   `snapshot_view(id).range(..)` should yield.
//!
//! Every Db operation is applied to both the Db and the reference;
//! the test asserts they agree on reads and diffs.

use std::collections::{BTreeMap, HashMap};

use onyx_metadb::{DiffEntry, L2pValue, SnapshotId};
use proptest::prelude::*;
use tempfile::TempDir;

type Ref = BTreeMap<u64, L2pValue>;

fn v_of(n: u8) -> L2pValue {
    let mut x = [0u8; 28];
    x[0] = n;
    L2pValue(x)
}

#[derive(Clone, Debug)]
enum Op {
    Insert(u64, u8),
    Delete(u64),
    Snapshot,
    DropSnapshot(u16),
    VerifyCurrent,
    VerifySnapshot(u16),
    DiffPair(u16, u16),
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (0u64..100, 0u8..=255).prop_map(|(k, v)| Op::Insert(k, v)),
        2 => (0u64..100).prop_map(Op::Delete),
        1 => Just(Op::Snapshot),
        1 => (0u16..10).prop_map(Op::DropSnapshot),
        1 => Just(Op::VerifyCurrent),
        1 => (0u16..10).prop_map(Op::VerifySnapshot),
        1 => (0u16..10, 0u16..10).prop_map(|(a, b)| Op::DiffPair(a, b)),
    ]
}

fn naive_diff(a: &Ref, b: &Ref) -> Vec<DiffEntry> {
    use std::cmp::Ordering;
    let a_items: Vec<_> = a.iter().collect();
    let b_items: Vec<_> = b.iter().collect();
    let mut out = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < a_items.len() && j < b_items.len() {
        match a_items[i].0.cmp(b_items[j].0) {
            Ordering::Less => {
                out.push(DiffEntry::RemovedInB {
                    key: *a_items[i].0,
                    old: *a_items[i].1,
                });
                i += 1;
            }
            Ordering::Greater => {
                out.push(DiffEntry::AddedInB {
                    key: *b_items[j].0,
                    new: *b_items[j].1,
                });
                j += 1;
            }
            Ordering::Equal => {
                if a_items[i].1 != b_items[j].1 {
                    out.push(DiffEntry::Changed {
                        key: *a_items[i].0,
                        old: *a_items[i].1,
                        new: *b_items[j].1,
                    });
                }
                i += 1;
                j += 1;
            }
        }
    }
    while i < a_items.len() {
        out.push(DiffEntry::RemovedInB {
            key: *a_items[i].0,
            old: *a_items[i].1,
        });
        i += 1;
    }
    while j < b_items.len() {
        out.push(DiffEntry::AddedInB {
            key: *b_items[j].0,
            new: *b_items[j].1,
        });
        j += 1;
    }
    out
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 32,
        .. ProptestConfig::default()
    })]

    #[test]
    fn db_matches_reference(ops in proptest::collection::vec(arb_op(), 1..400)) {
        let dir = TempDir::new().unwrap();
        let db = onyx_metadb::Db::create(dir.path()).unwrap();
        let mut current: Ref = BTreeMap::new();
        let mut snapshots: HashMap<SnapshotId, Ref> = HashMap::new();
        let mut snap_ids: Vec<SnapshotId> = Vec::new();

        for op in ops {
            match op {
                Op::Insert(k, v) => {
                    let value = v_of(v);
                    let tree_old = db.insert(0,k, value).unwrap();
                    let ref_old = current.insert(k, value);
                    prop_assert_eq!(tree_old, ref_old);
                }
                Op::Delete(k) => {
                    let tree_old = db.delete(0,k).unwrap();
                    let ref_old = current.remove(&k);
                    prop_assert_eq!(tree_old, ref_old);
                }
                Op::Snapshot => {
                    let id = db.take_snapshot().unwrap();
                    snapshots.insert(id, current.clone());
                    snap_ids.push(id);
                }
                Op::DropSnapshot(idx) => {
                    if snap_ids.is_empty() {
                        continue;
                    }
                    let i = (idx as usize) % snap_ids.len();
                    let id = snap_ids.remove(i);
                    let report = db.drop_snapshot(id).unwrap().unwrap();
                    prop_assert_eq!(report.snapshot_id, id);
                    snapshots.remove(&id);
                }
                Op::VerifyCurrent => {
                    let got: Vec<(u64, L2pValue)> = db
                        .range(0, ..)
                        .unwrap()
                        .collect::<onyx_metadb::Result<Vec<_>>>()
                        .unwrap();
                    let expect: Vec<(u64, L2pValue)> =
                        current.iter().map(|(k, v)| (*k, *v)).collect();
                    prop_assert_eq!(got, expect);
                }
                Op::VerifySnapshot(idx) => {
                    if snap_ids.is_empty() {
                        continue;
                    }
                    let i = (idx as usize) % snap_ids.len();
                    let id = snap_ids[i];
                    let expected = snapshots.get(&id).unwrap().clone();
                    let view = db.snapshot_view(id).unwrap();
                    let got: Vec<(u64, L2pValue)> = view
                        .range(..)
                        .unwrap()
                        .collect::<onyx_metadb::Result<Vec<_>>>()
                        .unwrap();
                    let expect: Vec<(u64, L2pValue)> =
                        expected.iter().map(|(k, v)| (*k, *v)).collect();
                    prop_assert_eq!(got, expect);
                }
                Op::DiffPair(a_idx, b_idx) => {
                    if snap_ids.len() < 2 {
                        continue;
                    }
                    let ai = (a_idx as usize) % snap_ids.len();
                    let bi = (b_idx as usize) % snap_ids.len();
                    let a_id = snap_ids[ai];
                    let b_id = snap_ids[bi];
                    let a_ref = snapshots.get(&a_id).unwrap().clone();
                    let b_ref = snapshots.get(&b_id).unwrap().clone();
                    let expected = naive_diff(&a_ref, &b_ref);
                    let got = db.diff(a_id, b_id).unwrap();
                    prop_assert_eq!(got, expected);
                }
            }
        }

        // Final: every live snapshot must still read correctly.
        for id in &snap_ids {
            let expected = snapshots.get(id).unwrap().clone();
            let view = db.snapshot_view(*id).unwrap();
            let got: Vec<(u64, L2pValue)> = view
                .range(..)
                .unwrap()
                .collect::<onyx_metadb::Result<Vec<_>>>()
                .unwrap();
            let expect: Vec<(u64, L2pValue)> =
                expected.iter().map(|(k, v)| (*k, *v)).collect();
            prop_assert_eq!(got, expect);
        }
        // And current state matches.
        let got: Vec<(u64, L2pValue)> = db
            .range(0, ..)
            .unwrap()
            .collect::<onyx_metadb::Result<Vec<_>>>()
            .unwrap();
        let expect: Vec<(u64, L2pValue)> = current.iter().map(|(k, v)| (*k, *v)).collect();
        prop_assert_eq!(got, expect);
    }
}

#[test]
fn deterministic_snapshot_stress() {
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    let dir = TempDir::new().unwrap();
    let db = onyx_metadb::Db::create(dir.path()).unwrap();
    let mut current: Ref = BTreeMap::new();
    let mut snapshots: HashMap<SnapshotId, Ref> = HashMap::new();
    let mut snap_ids: Vec<SnapshotId> = Vec::new();
    let mut rng = ChaCha8Rng::seed_from_u64(0xD_00_0F);

    for _ in 0..5_000 {
        let choice = rng.r#gen::<u8>() % 20;
        match choice {
            0..=9 => {
                let k = rng.r#gen::<u64>() % 500;
                let v = v_of(rng.r#gen::<u8>());
                db.insert(0,k, v).unwrap();
                current.insert(k, v);
            }
            10..=13 => {
                let k = rng.r#gen::<u64>() % 500;
                let a = db.delete(0,k).unwrap();
                let b = current.remove(&k);
                assert_eq!(a, b);
            }
            14 | 15 => {
                // Cap live snapshots well under MAX_SNAPSHOTS_PER_MANIFEST
                // so we don't accidentally hit the manifest limit while
                // stress-testing everything else.
                if snap_ids.len() < 100 {
                    let id = db.take_snapshot().unwrap();
                    snapshots.insert(id, current.clone());
                    snap_ids.push(id);
                }
            }
            16 => {
                if !snap_ids.is_empty() {
                    let idx = rng.r#gen::<usize>() % snap_ids.len();
                    let id = snap_ids.remove(idx);
                    db.drop_snapshot(id).unwrap();
                    snapshots.remove(&id);
                }
            }
            17 => {
                snap_ids.shuffle(&mut rng);
            }
            _ => {
                if snap_ids.len() >= 2 {
                    let (a, b) = (snap_ids[0], snap_ids[1]);
                    let expected =
                        naive_diff(snapshots.get(&a).unwrap(), snapshots.get(&b).unwrap());
                    let got = db.diff(a, b).unwrap();
                    assert_eq!(got, expected);
                }
            }
        }
    }

    // Final reconciliation.
    let cur: Vec<(u64, L2pValue)> = db
        .range(0, ..)
        .unwrap()
        .collect::<onyx_metadb::Result<Vec<_>>>()
        .unwrap();
    let expect: Vec<(u64, L2pValue)> = current.iter().map(|(k, v)| (*k, *v)).collect();
    assert_eq!(cur, expect);
}
