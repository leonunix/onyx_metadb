//! Property tests for the Onyx-aware compact leaf format.
//!
//! Two layers of coverage:
//!
//! 1. **`encode` round-trip**: any (bitmap, values) input where each
//!    populated slot has Onyx-shape per-unit consistency must
//!    round-trip through `encode` → `decode_at` and `decode_all`.
//!
//! 2. **`leaf_set` / `leaf_clear` sequence vs. oracle**: any sequence
//!    of `Set(slot, unit_id, offset)` / `Clear(slot)` operations
//!    applied via the public `paged::format::leaf_set` / `leaf_clear`
//!    API must match a `BTreeMap<usize, [u8; 28]>` oracle. Between
//!    operations we exercise:
//!      - `leaf_bit_set` / `leaf_value_at` agreement with oracle
//!      - `leaf_entry_count` matches oracle size
//!      - bitmap popcount equals header `key_count`
//!      - cleared slots' 3 B entry record is fully zeroed (CRC hygiene)
//!      - `seal()` + `verify()` pass on the page
//!
//! The sequence test deliberately uses few unit IDs from a small pool
//! (~6) so dedup hits dominate, mirroring Onyx's packer-driven
//! locality. A separate "high cardinality" sub-test pushes >128
//! distinct units through the same leaf to exercise the
//! `compact_in_place` reclamation path.

use std::collections::BTreeMap;

use onyx_metadb::page::Page;
use onyx_metadb::paged::format::{
    LEAF_BITMAP_BYTES, LEAF_ENTRY_COUNT, LEAF_VALUE_SIZE, L2pValue, init_leaf, leaf_bit_set,
    leaf_clear, leaf_entry_count, leaf_set, leaf_value_at,
};
use proptest::prelude::*;

// ---------- shared helpers ------------------------------------------------

/// Encode an Onyx-shape `BlockmapValue` into 28 B. Per-unit fields
/// (everything except `offset_in_unit`) are derived from `unit_id` so
/// values referencing the same unit are byte-identical in those fields,
/// matching Onyx flusher semantics.
fn synth_value(unit_id: u32, offset_in_unit: u16) -> [u8; LEAF_VALUE_SIZE] {
    let pba = 0x1_0000_0000u64 | (unit_id as u64);
    let cmp_sz: u32 = 1024 + (unit_id & 0xFF);
    let org_sz: u32 = 4096 * (((unit_id & 0x1F) + 1) as u32);
    let lba_count: u16 = ((unit_id & 0x1F) + 1) as u16;
    let crc32: u32 = 0xCAFE_0000u32 ^ unit_id;
    let slot_offset: u16 = (unit_id & 0xFFF) as u16;
    let compression: u8 = (unit_id & 3) as u8;
    let flags: u8 = ((unit_id >> 2) & 1) as u8;

    let mut v = [0u8; LEAF_VALUE_SIZE];
    v[0..8].copy_from_slice(&pba.to_be_bytes());
    v[8] = compression;
    v[9..13].copy_from_slice(&cmp_sz.to_be_bytes());
    v[13..17].copy_from_slice(&org_sz.to_be_bytes());
    v[17..19].copy_from_slice(&lba_count.to_be_bytes());
    v[19..21].copy_from_slice(&offset_in_unit.to_be_bytes());
    v[21..25].copy_from_slice(&crc32.to_be_bytes());
    v[25..27].copy_from_slice(&slot_offset.to_be_bytes());
    v[27] = flags;
    v
}

fn fresh_leaf() -> Page {
    let mut p = Page::zeroed();
    init_leaf(&mut p, 1);
    p
}

/// Cross-check the page against the oracle: bitmap, values, count,
/// header consistency. Returns `Ok(())` if all invariants hold,
/// otherwise a descriptive error.
fn check_against_oracle(
    page: &Page,
    oracle: &BTreeMap<usize, [u8; LEAF_VALUE_SIZE]>,
) -> Result<(), String> {
    let count = leaf_entry_count(page);
    if count as usize != oracle.len() {
        return Err(format!(
            "header count={count} but oracle has {} entries",
            oracle.len()
        ));
    }

    // Header counter must equal popcount over the bitmap.
    let bitmap = &page.payload()[..LEAF_BITMAP_BYTES];
    let popcount: u32 = bitmap.iter().map(|b| b.count_ones()).sum();
    if popcount != count as u32 {
        return Err(format!(
            "popcount({popcount}) disagrees with header count({count})"
        ));
    }

    for slot in 0..LEAF_ENTRY_COUNT {
        let bit = leaf_bit_set(page, slot);
        let val = leaf_value_at(page, slot);
        match oracle.get(&slot) {
            Some(expected) => {
                if !bit {
                    return Err(format!("oracle says slot {slot} set, page says clear"));
                }
                if &val.0 != expected {
                    return Err(format!(
                        "slot {slot} value mismatch: page={:?} oracle={:?}",
                        &val.0, expected
                    ));
                }
            }
            None => {
                if bit {
                    return Err(format!("oracle says slot {slot} clear, page says set"));
                }
                if val.0 != [0u8; LEAF_VALUE_SIZE] {
                    return Err(format!(
                        "cleared slot {slot} did not return ZERO: {:?}",
                        &val.0
                    ));
                }
            }
        }
    }
    Ok(())
}

// ---------- proptest 1: leaf_set / leaf_clear sequence ---------------------

#[derive(Debug, Clone)]
enum Op {
    Set { slot: usize, unit_id: u32, offset: u16 },
    Clear { slot: usize },
}

/// Pool size for unit IDs. Small enough that dedup hits dominate;
/// matches Onyx's packer locality (1..32 units per leaf typical).
const UNIT_POOL: u32 = 8;

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        // 4:1 set:clear ratio so the leaf reaches a populated steady
        // state instead of churning empty.
        4 => (0usize..LEAF_ENTRY_COUNT, 0u32..UNIT_POOL, 0u16..32)
            .prop_map(|(slot, unit_id, offset)| Op::Set { slot, unit_id, offset }),
        1 => (0usize..LEAF_ENTRY_COUNT).prop_map(|slot| Op::Clear { slot }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        .. ProptestConfig::default()
    })]

    #[test]
    fn set_clear_sequence_matches_oracle(
        ops in proptest::collection::vec(arb_op(), 1..400),
    ) {
        let mut page = fresh_leaf();
        let mut oracle: BTreeMap<usize, [u8; LEAF_VALUE_SIZE]> = BTreeMap::new();

        for op in ops {
            match op {
                Op::Set { slot, unit_id, offset } => {
                    let v = synth_value(unit_id, offset);
                    let prev_page = leaf_set(&mut page, slot, &L2pValue(v));
                    let prev_oracle = oracle.insert(slot, v);
                    prop_assert_eq!(
                        prev_page.map(|x| x.0),
                        prev_oracle,
                        "set returned wrong previous value at slot {}",
                        slot,
                    );
                }
                Op::Clear { slot } => {
                    let prev_page = leaf_clear(&mut page, slot);
                    let prev_oracle = oracle.remove(&slot);
                    prop_assert_eq!(
                        prev_page.map(|x| x.0),
                        prev_oracle,
                        "clear returned wrong previous value at slot {}",
                        slot,
                    );
                }
            }
            // Cross-check page vs oracle after every operation.
            check_against_oracle(&page, &oracle)
                .map_err(|e| TestCaseError::fail(format!("invariant: {e}")))?;
        }

        // After the sequence, the page must seal + verify cleanly so a
        // soak run that flushes mid-tx doesn't trip CRC over leftover
        // bytes.
        page.seal();
        page.verify(123).expect("seal+verify after sequence");
    }
}

// ---------- proptest 2: high-cardinality stress (compact_in_place) --------

// Insert many distinct units, delete some, insert more — long enough
// to force `compact_in_place` to fire at least once. After every
// step the oracle must still match.
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 16,
        .. ProptestConfig::default()
    })]

    #[test]
    fn high_cardinality_units_round_trip(
        // 800 ops covering 200 distinct units forces several
        // compaction passes through the dict ceiling (~139 units).
        ops in proptest::collection::vec(
            prop_oneof![
                3 => (0usize..LEAF_ENTRY_COUNT, 0u32..200, 0u16..32)
                    .prop_map(|(slot, unit_id, offset)|
                        Op::Set { slot, unit_id, offset }),
                1 => (0usize..LEAF_ENTRY_COUNT).prop_map(|slot| Op::Clear { slot }),
            ],
            1..800,
        ),
    ) {
        let mut page = fresh_leaf();
        let mut oracle: BTreeMap<usize, [u8; LEAF_VALUE_SIZE]> = BTreeMap::new();

        for op in ops {
            match op {
                Op::Set { slot, unit_id, offset } => {
                    let v = synth_value(unit_id, offset);
                    leaf_set(&mut page, slot, &L2pValue(v));
                    oracle.insert(slot, v);
                }
                Op::Clear { slot } => {
                    leaf_clear(&mut page, slot);
                    oracle.remove(&slot);
                }
            }
            check_against_oracle(&page, &oracle)
                .map_err(|e| TestCaseError::fail(format!("invariant: {e}")))?;
        }

        page.seal();
        page.verify(456).expect("seal+verify after high-cardinality");
    }
}

// ---------- proptest 3: random Onyx-shape inputs round-trip ---------------

/// A canonical input: a presence bitmap + a sparse `BTreeMap<slot,
/// (unit_id, offset)>` that drives synth_value. The bitmap is derived
/// from the keys so the two never disagree.
fn arb_canonical_leaf() -> impl Strategy<Value = BTreeMap<usize, (u32, u16)>> {
    proptest::collection::btree_map(0usize..LEAF_ENTRY_COUNT, (0u32..16, 0u16..32), 0..=128)
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 128,
        .. ProptestConfig::default()
    })]

    #[test]
    fn random_input_round_trips_via_format_api(
        input in arb_canonical_leaf(),
    ) {
        let mut page = fresh_leaf();
        for (&slot, &(unit_id, offset)) in &input {
            let v = synth_value(unit_id, offset);
            leaf_set(&mut page, slot, &L2pValue(v));
        }

        // Read back through the public API.
        for slot in 0..LEAF_ENTRY_COUNT {
            let bit = leaf_bit_set(&page, slot);
            let val = leaf_value_at(&page, slot);
            match input.get(&slot) {
                Some(&(unit_id, offset)) => {
                    let expected = synth_value(unit_id, offset);
                    prop_assert!(bit, "slot {} should be set", slot);
                    prop_assert_eq!(val.0, expected, "value mismatch slot {}", slot);
                }
                None => {
                    prop_assert!(!bit, "slot {} should be clear", slot);
                    prop_assert_eq!(val.0, [0u8; LEAF_VALUE_SIZE]);
                }
            }
        }
        prop_assert_eq!(leaf_entry_count(&page) as usize, input.len());

        page.seal();
        page.verify(789).expect("seal+verify");
    }
}

// ---------- proptest 4: unit-dict dedup invariant -------------------------

/// Inserting many values that share a small pool of unit IDs must NOT
/// grow the unit dictionary beyond the pool size. This is the core
/// space-saving invariant of the compact format: per-unit fields
/// fold once even when 100+ slots reference the same unit.
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        .. ProptestConfig::default()
    })]

    #[test]
    fn dedup_keeps_unit_dict_at_pool_size(
        // Each (slot, unit_id, offset). We expect the page's unit dict
        // to never have more than `distinct_units(unit_id)` entries
        // among inserted ops (provided no compact_in_place fires;
        // sequences here are too short to trigger the ceiling).
        ops in proptest::collection::vec(
            (0usize..LEAF_ENTRY_COUNT, 0u32..6, 0u16..32),
            1..120,
        ),
    ) {
        let mut page = fresh_leaf();
        let mut distinct_unit_ids: std::collections::BTreeSet<u32> =
            std::collections::BTreeSet::new();

        for (slot, unit_id, offset) in ops {
            let v = synth_value(unit_id, offset);
            leaf_set(&mut page, slot, &L2pValue(v));
            distinct_unit_ids.insert(unit_id);

            // unit_count lives at offset 16 of the payload.
            let unit_count = page.payload()[LEAF_BITMAP_BYTES] as usize;
            // Ceiling: every unit_id ever inserted yields at most one
            // dict entry (deletes don't shrink, but we never overwrite
            // a slot's value with a *different* unit before clearing,
            // because synth_value(unit_id, _) keeps the unit consistent).
            // So unit_count grows monotonically toward the pool size.
            prop_assert!(
                unit_count <= distinct_unit_ids.len(),
                "unit_count={unit_count} exceeded pool size {} after slot {} unit {}",
                distinct_unit_ids.len(), slot, unit_id,
            );
        }
    }
}
