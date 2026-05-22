use super::*;

/// Build a 36 B L2pValue: bytes [0..28] match onyx-storage's
/// `encode_blockmap_value` byte layout, bytes [28..36] are the
/// seq field (left as zero by this helper — tests that need a
/// specific seq must overwrite bytes [28..36] explicitly).
pub(crate) fn bv(
    pba: u64,
    compression: u8,
    cmp_sz: u32,
    org_sz: u32,
    lba_count: u16,
    offset_in_unit: u16,
    crc32: u32,
    slot_offset: u16,
    flags: u8,
) -> [u8; LEAF_VALUE_SIZE] {
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

fn empty_leaf_input() -> (
    [u8; LEAF_BITMAP_BYTES],
    [[u8; LEAF_VALUE_SIZE]; LEAF_ENTRY_COUNT],
) {
    (
        [0u8; LEAF_BITMAP_BYTES],
        [[0u8; LEAF_VALUE_SIZE]; LEAF_ENTRY_COUNT],
    )
}

fn set(
    bitmap: &mut [u8; LEAF_BITMAP_BYTES],
    values: &mut [[u8; LEAF_VALUE_SIZE]; LEAF_ENTRY_COUNT],
    slot: usize,
    v: [u8; LEAF_VALUE_SIZE],
) {
    bitmap[slot / 8] |= 1u8 << (slot % 8);
    values[slot] = v;
}

#[test]
fn empty_leaf_round_trips() {
    let (bm, vals) = empty_leaf_input();
    let enc = encode(&bm, &vals);
    // 42 header + 896 entries (zero) + 0 unit dict = 938 (v5).
    assert_eq!(enc.len(), 938);
    for s in 0..LEAF_ENTRY_COUNT {
        assert_eq!(decode_at(&enc, s), None);
    }
    assert_eq!(entry_count(&enc), 0);
}

#[test]
fn single_unit_full_leaf_round_trip() {
    let (mut bm, mut vals) = empty_leaf_input();
    for i in 0..LEAF_ENTRY_COUNT {
        let v = bv(
            0x1234_5678_9abc_def0,
            1,
            3000,
            128 * 4096,
            128,
            i as u16,
            0xDEAD_BEEFu32, // unit-level checksum, constant
            0,              // unpacked unit
            0,
        );
        set(&mut bm, &mut vals, i, v);
    }
    let enc = encode(&bm, &vals);
    // 42 + 896 + 24 = 962 (v5: header +8 for base_pba, unit -4 because
    // base_pba is now a u32 delta against the leaf base).
    assert_eq!(enc.len(), 962);
    let cr = DENSE_FOOTPRINT_BYTES as f64 / enc.len() as f64;
    // 1 unit / 128 slots: dense=16+128*44=5648, compact=962 → ~5.87x.
    assert!(cr > 4.0, "CR too low: {cr}");

    for i in 0..LEAF_ENTRY_COUNT {
        let got = decode_at(&enc, i).expect("set");
        assert_eq!(got, vals[i], "slot {i}");
    }
    assert_eq!(entry_count(&enc), LEAF_ENTRY_COUNT);
}

#[test]
fn eight_units_full_leaf_meets_3x_gate() {
    let (mut bm, mut vals) = empty_leaf_input();
    for i in 0..LEAF_ENTRY_COUNT {
        let unit = (i / 16) as u64;
        let v = bv(
            0x1000 + unit * 0x100,
            if unit % 2 == 0 { 1 } else { 2 },
            2000 + (unit as u32) * 100,
            16 * 4096,
            16,
            (i % 16) as u16,
            0xCAFE_0000u32 ^ unit as u32,
            (unit as u16) * 256,
            0,
        );
        set(&mut bm, &mut vals, i, v);
    }
    let enc = encode(&bm, &vals);
    // 42 + 896 + 8*24 = 1130 (v5 layout)
    assert_eq!(enc.len(), 1130);
    let cr = DENSE_FOOTPRINT_BYTES as f64 / enc.len() as f64;
    // 8 units / 128 slots: dense=5648, compact=1130 → ~5.0x.
    assert!(cr >= 3.5, "8-unit CR {cr} below 3.5x");

    let all = decode_all(&enc);
    for i in 0..LEAF_ENTRY_COUNT {
        assert_eq!(all[i], Some(vals[i]), "slot {i}");
    }
}

#[test]
fn sparse_leaf_round_trips() {
    let (mut bm, mut vals) = empty_leaf_input();
    let slots = [0, 7, 31, 63, 64, 65, 127];
    for (i, &s) in slots.iter().enumerate() {
        let v = bv(
            0x2000 + i as u64,
            1,
            512,
            4096,
            1,
            0,
            i as u32,
            0,
            if i % 2 == 0 { 0 } else { 1 },
        );
        set(&mut bm, &mut vals, s, v);
    }
    let enc = encode(&bm, &vals);
    for s in 0..LEAF_ENTRY_COUNT {
        let want = if slots.contains(&s) {
            Some(vals[s])
        } else {
            None
        };
        assert_eq!(decode_at(&enc, s), want, "slot {s}");
    }
    assert_eq!(entry_count(&enc), slots.len());
}

#[test]
fn pathological_distinct_units_fits_payload() {
    // v5 restores MAX_UNITS_PER_LEAF = 128 = LEAF_ENTRY_COUNT
    // (after v4's 110 cap, tightened to make room for birth_delta).
    // The payload-bound cap is (4032 - 938) / 24 = 128 — exactly
    // what we set. 128 distinct units => 42 + 896 + 24*128 =
    // 4010 B, leaving 22 B headroom in the 4032 B payload —
    // identical to v4's headroom at its old N = 110. Synthetic
    // 1-LBA-per-unit workloads now fit exactly. Workloads that
    // legitimately span >4 G PBA blocks within one 128-LBA leaf
    // range will trip the rebase fallback in compact_in_place;
    // see the module-level comment.
    let (mut bm, mut vals) = empty_leaf_input();
    for i in 0..MAX_UNITS_PER_LEAF {
        let v = bv(0x3000 + i as u64, 1, 500, 4096, 1, 0, i as u32, 0, 0);
        set(&mut bm, &mut vals, i, v);
    }
    let enc = encode(&bm, &vals);
    assert_eq!(enc.len(), 4010);
    for s in 0..MAX_UNITS_PER_LEAF {
        assert_eq!(decode_at(&enc, s), Some(vals[s]));
    }
}

#[test]
fn version_byte_round_trips() {
    let (mut bm, mut vals) = empty_leaf_input();
    let v = bv(0x4000, 1, 512, 4096, 1, 0, 0xAA, 0, 0);
    set(&mut bm, &mut vals, 5, v);
    let enc = encode(&bm, &vals);
    assert_eq!(version(&enc), Some(COMPACT_VERSION));
}

#[test]
fn unknown_version_decodes_as_unset() {
    let (mut bm, mut vals) = empty_leaf_input();
    let v = bv(0x4000, 1, 512, 4096, 1, 0, 0xAA, 0, 0);
    set(&mut bm, &mut vals, 5, v);
    let mut enc = encode(&bm, &vals);
    enc[LEAF_BITMAP_BYTES + 1] = 0xFF;
    assert_eq!(decode_at(&enc, 5), None);
    let all = decode_all(&enc);
    assert!(all.iter().all(|e| e.is_none()));
}

#[test]
fn corrupt_unit_idx_fails_safely() {
    let (mut bm, mut vals) = empty_leaf_input();
    let v = bv(0x4000, 1, 512, 4096, 1, 0, 0xAA, 0, 0);
    set(&mut bm, &mut vals, 5, v);
    let mut enc = encode(&bm, &vals);
    // Patch entry@5 to point past the unit dict. The decode must
    // refuse rather than read out of bounds.
    let off = entry_offset(5);
    enc[off] = 0xFF;
    assert_eq!(decode_at(&enc, 5), None);
}

#[test]
fn randomized_round_trip() {
    use rand::{Rng, SeedableRng};
    let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(0xDEC0DE);
    for trial in 0..200 {
        let (mut bm, mut vals) = empty_leaf_input();
        let unit_count = 1 + (trial % 16);
        let unit_pool: Vec<u64> = (0..unit_count).map(|i| 0x5000 + i as u64 * 0x100).collect();
        for s in 0..LEAF_ENTRY_COUNT {
            if rng.r#gen::<bool>() {
                let unit_pba = unit_pool[rng.gen_range(0..unit_count)];
                // Match Onyx's "all per-unit fields constant within
                // a unit" invariant: derive the unit-level fields
                // from unit_pba so they're identical across slots in
                // the same unit.
                let v = bv(
                    unit_pba,
                    (unit_pba & 3) as u8,
                    ((unit_pba & 0xFFF) as u32) + 1024,
                    ((unit_pba & 0xFF) as u32) * 4096 + 4096,
                    ((unit_pba & 0x1F) as u16) + 1,
                    rng.gen_range(0..32),
                    (unit_pba & 0xFFFF_FFFF) as u32,
                    (unit_pba & 0xFFF) as u16,
                    ((unit_pba >> 4) & 1) as u8,
                );
                set(&mut bm, &mut vals, s, v);
            }
        }
        let enc = encode(&bm, &vals);
        for s in 0..LEAF_ENTRY_COUNT {
            let want = if (bm[s / 8] >> (s % 8)) & 1 == 1 {
                Some(vals[s])
            } else {
                None
            };
            assert_eq!(decode_at(&enc, s), want, "trial {trial} slot {s}");
        }
        let pop: usize = bm.iter().map(|b| b.count_ones() as usize).sum();
        assert_eq!(entry_count(&enc), pop, "trial {trial}");
        let decoded_all_set = decode_all(&enc).iter().filter(|e| e.is_some()).count();
        assert_eq!(decoded_all_set, pop, "trial {trial}");
    }
}

// ----- page-level primitive tests -------------------------------

fn fresh_payload() -> Vec<u8> {
    let mut p = vec![0u8; crate::page::PAGE_PAYLOAD_SIZE];
    init_payload(&mut p);
    p
}

#[test]
fn init_payload_writes_version_only() {
    let p = fresh_payload();
    assert_eq!(p[LEAF_BITMAP_BYTES], 0); // unit_count
    assert_eq!(p[LEAF_BITMAP_BYTES + 1], COMPACT_VERSION);
    assert!(p[..LEAF_BITMAP_BYTES].iter().all(|b| *b == 0));
    assert_eq!(read_unit_count(&p), 0);
}

#[test]
fn find_or_append_unit_dedups_identical_units() {
    let mut p = fresh_payload();
    let v1 = bv(0x1000, 1, 500, 4096, 1, 0, 0xAA, 0, 0);
    let v2 = bv(0x1000, 1, 500, 4096, 1, 7, 0xAA, 0, 0); // same unit, different offset
    let (u1, _) = decompose_value(&v1);
    let (u2, _) = decompose_value(&v2);
    assert_eq!(u1, u2);
    let i1 = find_or_append_unit(&mut p, &u1).unwrap();
    let i2 = find_or_append_unit(&mut p, &u2).unwrap();
    assert_eq!(i1, 0);
    assert_eq!(i2, 0); // dedup hit, same idx
    assert_eq!(read_unit_count(&p), 1);
}

#[test]
fn find_or_append_unit_returns_none_when_full() {
    let mut p = fresh_payload();
    let cap = max_units_per_payload(p.len()).min(MAX_UNITS_PER_LEAF);
    // Fill up the dict to MAX_UNITS_PER_LEAF (the binding cap).
    for i in 0..cap {
        let v = bv(0x10_0000 + i as u64, 1, 500, 4096, 1, 0, 0xAA, 0, 0);
        let (u, _) = decompose_value(&v);
        assert!(find_or_append_unit(&mut p, &u).is_some(), "at i={i}");
    }
    // One more is too many.
    let extra = bv(0x10_FFFF, 1, 500, 4096, 1, 0, 0xAA, 0, 0);
    let (u, _) = decompose_value(&extra);
    assert!(find_or_append_unit(&mut p, &u).is_none());
}

#[test]
fn compact_in_place_drops_dead_units_and_renumbers() {
    let mut p = fresh_payload();
    // Insert 4 distinct units (all referenced by one entry each).
    for i in 0..4u64 {
        let v = bv(0x2000 + i * 0x100, 1, 500, 4096, 1, 0, 0xCD, 0, 0);
        let (u, e) = decompose_value(&v);
        let idx = find_or_append_unit(&mut p, &u).unwrap();
        payload_bit_set_true(&mut p, i as usize);
        write_entry(&mut p, i as usize, idx, &e);
    }
    assert_eq!(read_unit_count(&p), 4);

    // "Delete" entries 0 and 2 (clear bitmap + zero entry).
    payload_bit_clear(&mut p, 0);
    zero_entry(&mut p, 0);
    payload_bit_clear(&mut p, 2);
    zero_entry(&mut p, 2);

    // unit_dict still holds 4 units; entries 1 and 3 reference
    // old_idx 1 and 3 respectively.
    compact_in_place(&mut p).unwrap();
    assert_eq!(read_unit_count(&p), 2);

    // Entries 1 and 3 still decode correctly.
    let v1 = payload_decode_at(&p, 1);
    let v3 = payload_decode_at(&p, 3);
    // base_pba is the first 8 bytes of the value.
    assert_eq!(
        u64::from_be_bytes(v1[0..8].try_into().unwrap()),
        0x2000 + 0x100
    );
    assert_eq!(
        u64::from_be_bytes(v3[0..8].try_into().unwrap()),
        0x2000 + 0x300
    );
}

#[test]
fn compact_in_place_idempotent_when_no_dead_units() {
    let mut p = fresh_payload();
    for i in 0..3u64 {
        let v = bv(0x3000 + i * 0x100, 1, 500, 4096, 1, 0, 0xEE, 0, 0);
        let (u, e) = decompose_value(&v);
        let idx = find_or_append_unit(&mut p, &u).unwrap();
        payload_bit_set_true(&mut p, i as usize);
        write_entry(&mut p, i as usize, idx, &e);
    }
    let before = p.clone();
    compact_in_place(&mut p).unwrap();
    // Same dict layout, same entries — payload unchanged.
    assert_eq!(p, before);
}

#[test]
fn compact_in_place_is_idempotent() {
    let mut p = fresh_payload();
    // Sequence: insert 6 units, delete every other, then call
    // compact twice. Result of second call must equal first.
    for i in 0..6u64 {
        let v = bv(0x6000 + i * 0x100, 1, 500, 4096, 1, 0, 0xCD, 0, 0);
        let (u, e) = decompose_value(&v);
        let idx = find_or_append_unit(&mut p, &u).unwrap();
        payload_bit_set_true(&mut p, i as usize);
        write_entry(&mut p, i as usize, idx, &e);
    }
    for i in (0..6).step_by(2) {
        payload_bit_clear(&mut p, i);
        zero_entry(&mut p, i);
    }
    compact_in_place(&mut p).unwrap();
    let after_first = p.clone();
    compact_in_place(&mut p).unwrap();
    assert_eq!(p, after_first);
}

#[test]
fn full_overflow_then_compact_recovers() {
    // Drive the unit dict to its capacity, kill enough live entries
    // to free room, then verify a fresh `find_or_append_unit` works
    // again after `compact_in_place`.
    let mut p = fresh_payload();
    let cap = max_units_per_payload(p.len()).min(MAX_UNITS_PER_LEAF);

    // Fill: each leaf slot 0..min(cap,128) gets its own unit. With
    // v5's 7 B per-slot record + 24 B unit + 42 B header the
    // payload-bound cap is 128, equal to MAX_UNITS_PER_LEAF=128
    // = LEAF_ENTRY_COUNT, so we fill every slot for full coverage.
    let live_slots: Vec<usize> = (0..cap.min(LEAF_ENTRY_COUNT)).collect();
    for &slot in &live_slots {
        let v = bv(
            0x7000 + slot as u64 * 0x100,
            1,
            500,
            4096,
            1,
            0,
            slot as u32,
            0,
            0,
        );
        let (u, e) = decompose_value(&v);
        let idx = find_or_append_unit(&mut p, &u).expect("fits");
        payload_bit_set_true(&mut p, slot);
        write_entry(&mut p, slot, idx, &e);
    }
    // Confirm we filled the dict to its payload-bound cap.
    assert_eq!(
        read_unit_count(&p) as usize,
        cap,
        "regression: did not saturate dict capacity"
    );

    // Now delete half the slots' entries. Their units stay in the
    // dict (dead). Then attempt to reuse those slots with NEW units.
    let dead: Vec<usize> = live_slots.iter().copied().step_by(2).collect();
    for &s in &dead {
        payload_bit_clear(&mut p, s);
        zero_entry(&mut p, s);
    }
    // Insert fresh-distinct units into the now-empty slots until we
    // either succeed within current dict or hit the ceiling.
    for &s in &dead {
        let v = bv(
            0x8000 + s as u64 * 0x100,
            1,
            500,
            4096,
            1,
            0,
            s as u32,
            0,
            0,
        );
        let (u, e) = decompose_value(&v);
        let idx = match find_or_append_unit(&mut p, &u) {
            Some(i) => i,
            None => {
                // Trigger compaction; dead units get reclaimed.
                compact_in_place(&mut p).unwrap();
                find_or_append_unit(&mut p, &u).expect("compact reclaimed enough room")
            }
        };
        payload_bit_set_true(&mut p, s);
        write_entry(&mut p, s, idx, &e);
    }

    // After the dust settles, every live slot decodes to its value.
    for &s in &live_slots {
        let v = payload_decode_at(&p, s);
        let pba = u64::from_be_bytes(v[0..8].try_into().unwrap());
        let expected = if dead.contains(&s) {
            0x8000 + s as u64 * 0x100
        } else {
            0x7000 + s as u64 * 0x100
        };
        assert_eq!(pba, expected, "slot {s}");
    }
}

#[test]
fn checked_decode_rejects_out_of_range_unit_count() {
    let mut p = fresh_payload();
    let v = bv(0x9000, 1, 500, 4096, 1, 0, 0xABCD, 0, 0);
    let (u, e) = decompose_value(&v);
    let idx = find_or_append_unit(&mut p, &u).unwrap();
    payload_bit_set_true(&mut p, 7);
    write_entry(&mut p, 7, idx, &e);

    let too_many_units = (max_units_per_payload(p.len()) + 1) as u8;
    write_unit_count(&mut p, too_many_units);
    let err = payload_decode_at_checked(&p, 7).unwrap_err();
    assert!(err.to_string().contains("unit_count"));
    assert!(compact_in_place(&mut p).is_err());
}

#[test]
fn compact_in_place_zeros_old_dict_tail() {
    let mut p = fresh_payload();
    // Insert 5 units, delete all entries pointing at units 1..4.
    for i in 0..5u64 {
        let v = bv(0x4000 + i * 0x100, 1, 500, 4096, 1, 0, 0xCD, 0, 0);
        let (u, e) = decompose_value(&v);
        let idx = find_or_append_unit(&mut p, &u).unwrap();
        payload_bit_set_true(&mut p, i as usize);
        write_entry(&mut p, i as usize, idx, &e);
    }
    for i in 1..5 {
        payload_bit_clear(&mut p, i);
        zero_entry(&mut p, i);
    }
    compact_in_place(&mut p).unwrap();
    // Old dict spanned units [0..5); after compaction only unit 0
    // remains. Bytes for slots 1..5 must be zero so the page CRC
    // doesn't capture stale unit data.
    let tail_start = unit_offset(1);
    let tail_end = unit_offset(5);
    assert!(p[tail_start..tail_end].iter().all(|b| *b == 0));
}

#[test]
fn fixed_offsets_compile_time_invariants() {
    // Sanity for v5 layout constants. Header: 16 B bitmap + 1 B
    // unit_count + 1 B version + 8 B base_seq + 8 B base_birth_lsn
    // + 8 B base_pba = 42 B. Per-slot record: 7 B (unit_idx +
    // offset + u32 seq_delta). Unit-dict entry: 24 B (28 B from
    // v4 - 4 B because base_pba is now a u32 delta against the
    // leaf base instead of an inline u64). MAX_UNITS_PER_LEAF
    // restores to 128 = LEAF_ENTRY_COUNT so the pathological
    // 128-distinct-unit case fits the 4032 B payload with 22 B
    // headroom (same headroom v4 had at its old N = 110).
    assert_eq!(COMPACT_HEADER_BYTES, 42);
    assert_eq!(COMPACT_BASE_SEQ_OFFSET, 18);
    assert_eq!(COMPACT_BASE_BIRTH_OFFSET, 26);
    assert_eq!(COMPACT_BASE_PBA_OFFSET, 34);
    assert_eq!(COMPACT_ENTRIES_OFFSET, 42);
    assert_eq!(COMPACT_UNIT_DICT_OFFSET, 938);
    assert_eq!(COMPACT_ENTRY_BYTES, 7);
    assert_eq!(COMPACT_UNIT_BYTES, 24);
    assert_eq!(MAX_UNITS_PER_LEAF, 128);
    assert_eq!(max_units_per_payload(crate::page::PAGE_PAYLOAD_SIZE), 128);
    assert_eq!(compact_size(0), 938);
    assert_eq!(compact_size(1), 962);
    assert_eq!(compact_size(8), 1130);
    assert_eq!(compact_size(MAX_UNITS_PER_LEAF), 4010);
    assert!(compact_size(MAX_UNITS_PER_LEAF) <= crate::page::PAGE_PAYLOAD_SIZE);
}
