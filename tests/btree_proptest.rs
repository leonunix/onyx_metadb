//! Property test: BTree behaves as an ordered map, identically to
//! `std::collections::BTreeMap`, under any sequence of
//! insert/delete/get/range operations.
//!
//! After every operation the tree's structural invariants are
//! checked. Any regression (lost key, bad ordering, depth imbalance,
//! underflow) fails the current proptest case and gets shrunk to a
//! minimal reproducer.

use std::collections::BTreeMap;
use std::sync::Arc;

use onyx_metadb::btree::{BTree, L2pValue};
use onyx_metadb::page_store::PageStore;
use proptest::prelude::*;
use tempfile::TempDir;

fn v_of(n: u8) -> L2pValue {
    let mut x = [0u8; 28];
    x[0] = n;
    L2pValue(x)
}

#[derive(Clone, Debug)]
enum Op {
    Insert(u64, u8),
    Delete(u64),
    Get(u64),
    Range(u64, u64),
}

fn arb_op() -> impl Strategy<Value = Op> {
    // Keys clustered around a small domain so collisions — and
    // therefore updates / deletes of existing keys — are common.
    prop_oneof![
        3 => (0u64..200, 0u8..=255).prop_map(|(k, v)| Op::Insert(k, v)),
        2 => (0u64..200).prop_map(Op::Delete),
        1 => (0u64..200).prop_map(Op::Get),
        1 => (0u64..200, 0u64..200).prop_map(|(a, b)| Op::Range(a.min(b), a.max(b))),
    ]
}

fn mk_tree() -> (TempDir, BTree) {
    let dir = TempDir::new().unwrap();
    let ps = Arc::new(PageStore::create(dir.path().join("p.onyx_meta")).unwrap());
    let tree = BTree::create(ps).unwrap();
    (dir, tree)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        .. ProptestConfig::default()
    })]

    #[test]
    fn tree_matches_btreemap(ops in proptest::collection::vec(arb_op(), 1..500)) {
        let (_d, mut tree) = mk_tree();
        let mut reference: BTreeMap<u64, L2pValue> = BTreeMap::new();

        for op in ops {
            match op {
                Op::Insert(k, v) => {
                    let value = v_of(v);
                    let tree_old = tree.insert(k, value).unwrap();
                    let ref_old = reference.insert(k, value);
                    prop_assert_eq!(tree_old, ref_old);
                }
                Op::Delete(k) => {
                    let tree_old = tree.delete(k).unwrap();
                    let ref_old = reference.remove(&k);
                    prop_assert_eq!(tree_old, ref_old);
                }
                Op::Get(k) => {
                    let tree_got = tree.get(k).unwrap();
                    let ref_got = reference.get(&k).copied();
                    prop_assert_eq!(tree_got, ref_got);
                }
                Op::Range(lo, hi) => {
                    let tree_range: Vec<(u64, L2pValue)> = tree
                        .range(lo..hi)
                        .unwrap()
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap();
                    let ref_range: Vec<(u64, L2pValue)> = reference
                        .range(lo..hi)
                        .map(|(k, v)| (*k, *v))
                        .collect();
                    prop_assert_eq!(tree_range, ref_range);
                }
            }
            // Structural invariants after every operation.
            tree.check_invariants().map_err(|e| {
                TestCaseError::fail(format!("invariant violation: {e}"))
            })?;
        }

        // Final comparison of full state.
        let tree_items: Vec<(u64, L2pValue)> = tree
            .range(..)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let ref_items: Vec<(u64, L2pValue)> = reference.iter().map(|(k, v)| (*k, *v)).collect();
        prop_assert_eq!(tree_items, ref_items);
    }
}

// --- deterministic stress test that lives outside proptest so it runs
// --- unconditionally on every CI invocation.

#[test]
fn deterministic_stress_matches_btreemap() {
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha8Rng;

    let (_d, mut tree) = mk_tree();
    let mut reference = BTreeMap::new();
    let mut rng = ChaCha8Rng::seed_from_u64(0xB_0001);

    for _ in 0..20_000 {
        let choice: u8 = rng.r#gen::<u8>() % 4;
        match choice {
            0 | 1 => {
                let k: u64 = rng.r#gen::<u64>() % 1_000;
                let v = v_of(rng.r#gen::<u8>());
                let tree_old = tree.insert(k, v).unwrap();
                let ref_old = reference.insert(k, v);
                assert_eq!(tree_old, ref_old);
            }
            2 => {
                let k: u64 = rng.r#gen::<u64>() % 1_000;
                let tree_old = tree.delete(k).unwrap();
                let ref_old = reference.remove(&k);
                assert_eq!(tree_old, ref_old);
            }
            _ => {
                let k: u64 = rng.r#gen::<u64>() % 1_000;
                let tree_got = tree.get(k).unwrap();
                let ref_got = reference.get(&k).copied();
                assert_eq!(tree_got, ref_got);
            }
        }
    }

    tree.check_invariants().unwrap();

    let tree_items: Vec<(u64, L2pValue)> = tree
        .range(..)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let ref_items: Vec<(u64, L2pValue)> = reference.iter().map(|(k, v)| (*k, *v)).collect();
    assert_eq!(tree_items, ref_items);
}
