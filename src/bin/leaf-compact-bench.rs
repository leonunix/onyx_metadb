//! Microbench for the Onyx-aware compact leaf format (Day 1 prototype).
//!
//! Measures encode + decode latency and compression ratio across the
//! leaf shapes Onyx is expected to produce in steady state. The Day 1
//! go/no-go gate is:
//!
//! - encode/decode <500 ns per populated entry, and
//! - typical-shape (1..=8 units per leaf) CR >= 3x vs the dense format.
//!
//! Run:
//!
//! ```sh
//! cargo run --release --bin leaf-compact-bench
//! ```
//!
//! The bench is single-threaded by design — we're characterizing
//! per-leaf encode/decode work, not throughput. The numbers are
//! reproducible enough on a quiet machine to gate format choice; for
//! competitive comparison against the dense format and RocksDB we run
//! the full integration bench in Day 3.

use std::hint::black_box;
use std::time::Instant;

use onyx_metadb::paged::format::{LEAF_BITMAP_BYTES, LEAF_ENTRY_COUNT, LEAF_VALUE_SIZE};
use onyx_metadb::paged::leaf_compact::{DENSE_FOOTPRINT_BYTES, decode_all, decode_at, encode};

/// Build a 28 B `BlockmapValue` matching `onyx_storage::meta::schema::
/// encode_blockmap_value`. Kept here because the production helper lives
/// in onyx-storage, not metadb.
fn bv(
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

type LeafInput = (
    [u8; LEAF_BITMAP_BYTES],
    Box<[[u8; LEAF_VALUE_SIZE]; LEAF_ENTRY_COUNT]>,
);

fn empty() -> LeafInput {
    (
        [0u8; LEAF_BITMAP_BYTES],
        Box::new([[0u8; LEAF_VALUE_SIZE]; LEAF_ENTRY_COUNT]),
    )
}

fn set(input: &mut LeafInput, slot: usize, v: [u8; LEAF_VALUE_SIZE]) {
    input.0[slot / 8] |= 1u8 << (slot % 8);
    input.1[slot] = v;
}

/// Construct a leaf where the 128 LBAs are split across `unit_count`
/// units of equal size. Unit-shared fields (everything except
/// offset_in_unit) are byte-identical within a unit.
fn build_uniform_leaf(unit_count: usize) -> LeafInput {
    assert!(unit_count >= 1 && unit_count <= LEAF_ENTRY_COUNT);
    let lbas_per_unit = LEAF_ENTRY_COUNT / unit_count;
    let mut leaf = empty();
    for slot in 0..LEAF_ENTRY_COUNT {
        let unit = (slot / lbas_per_unit).min(unit_count - 1) as u64;
        let off_in_unit = (slot % lbas_per_unit) as u16;
        let v = bv(
            0x1_0000 + unit * 0x100,
            if unit % 2 == 0 { 1 } else { 2 },
            2000 + unit as u32 * 13,
            (lbas_per_unit as u32) * 4096,
            lbas_per_unit as u16,
            off_in_unit,
            0xCAFE_0000u32 ^ unit as u32,
            (unit as u16) * 256,
            0,
        );
        set(&mut leaf, slot, v);
    }
    leaf
}

/// Worst-case shape: every entry is its own unit. Triggers the dense
/// fallback (`encode` returns `None`).
fn build_pathological_leaf() -> LeafInput {
    let mut leaf = empty();
    for slot in 0..LEAF_ENTRY_COUNT {
        let v = bv(
            0x2_0000 + slot as u64,
            1,
            500,
            4096,
            1,
            0,
            slot as u32,
            0,
            0,
        );
        set(&mut leaf, slot, v);
    }
    leaf
}

/// Sparse leaf: half the slots populated, 4 distinct units.
fn build_sparse_leaf() -> LeafInput {
    let mut leaf = empty();
    for slot in (0..LEAF_ENTRY_COUNT).step_by(2) {
        let unit = (slot / 32) as u64;
        let v = bv(
            0x3_0000 + unit * 0x100,
            1,
            1500,
            32 * 4096,
            32,
            ((slot % 32) / 2) as u16,
            0xBEEF_0000u32 ^ unit as u32,
            (unit as u16) * 128,
            0,
        );
        set(&mut leaf, slot, v);
    }
    leaf
}

fn populated_count(leaf: &LeafInput) -> usize {
    leaf.0.iter().map(|b| b.count_ones() as usize).sum()
}

struct Stats {
    iters: usize,
    total: std::time::Duration,
}

impl Stats {
    fn ns_per_op(&self) -> f64 {
        self.total.as_nanos() as f64 / self.iters as f64
    }
}

/// Repeat `body` for at least `min_dur_ns` to amortize the timer cost,
/// returning per-call wall time.
fn time<F: FnMut()>(mut body: F, min_iters: usize) -> Stats {
    // Warmup
    for _ in 0..(min_iters / 4).max(1) {
        body();
    }
    let start = Instant::now();
    for _ in 0..min_iters {
        body();
    }
    Stats {
        iters: min_iters,
        total: start.elapsed(),
    }
}

fn bench_one_shape(name: &str, leaf: &LeafInput, iters: usize) {
    let pop = populated_count(leaf);
    let encoded = encode(&leaf.0, &leaf.1);
    let cr = DENSE_FOOTPRINT_BYTES as f64 / encoded.len() as f64;

    // encode
    let enc_stats = time(
        || {
            let e = encode(&leaf.0, black_box(&leaf.1));
            black_box(e);
        },
        iters,
    );

    // decode_at over a fixed slot pattern: cycle through populated slots.
    // We pre-compute an index list so the inner loop stays branch-free
    // on the bitmap check.
    let pop_slots: Vec<usize> = (0..LEAF_ENTRY_COUNT)
        .filter(|&s| (leaf.0[s / 8] >> (s % 8)) & 1 == 1)
        .collect();
    let mut cursor = 0usize;
    let dec_at_stats = time(
        || {
            let s = pop_slots[cursor];
            cursor = (cursor + 1) % pop_slots.len();
            let v = decode_at(black_box(&encoded), s);
            black_box(v);
        },
        iters,
    );

    // decode_all (whole-leaf scan; amortized over all populated slots)
    let dec_all_stats = time(
        || {
            let v = decode_all(black_box(&encoded));
            black_box(v);
        },
        iters / 16,
    );

    let encode_per_entry = enc_stats.ns_per_op() / pop as f64;
    let decode_all_per_entry = dec_all_stats.ns_per_op() / pop as f64;

    println!(
        "{name:<24} pop={pop:>3}  size={:>4}B  CR={:>5.2}x  \
         encode={:>7.1} ns/leaf ({:>5.1} ns/entry)  \
         decode_at={:>5.1} ns/op  decode_all={:>7.1} ns/leaf ({:>5.1} ns/entry)",
        encoded.len(),
        cr,
        enc_stats.ns_per_op(),
        encode_per_entry,
        dec_at_stats.ns_per_op(),
        dec_all_stats.ns_per_op(),
        decode_all_per_entry,
    );

    let gate_violation = encode_per_entry > 500.0
        || decode_all_per_entry > 500.0
        || dec_at_stats.ns_per_op() > 500.0;
    if gate_violation {
        eprintln!(
            "  WARN: {name} blew the 500 ns/entry gate \
             (encode={encode_per_entry:.1} decode_at={:.1} decode_all={decode_all_per_entry:.1})",
            dec_at_stats.ns_per_op()
        );
    }
}

fn main() {
    println!("# Onyx compact leaf microbench (release, single-threaded)");
    println!("# dense baseline = {DENSE_FOOTPRINT_BYTES} B (16 B bitmap + 128 × 28 B values)");
    println!();

    // Iters tuned so each measurement runs ~10-50 ms; tweak if a host is
    // unusually slow.
    let iters = 200_000;

    let leaf1 = build_uniform_leaf(1);
    let leaf2 = build_uniform_leaf(2);
    let leaf4 = build_uniform_leaf(4);
    let leaf8 = build_uniform_leaf(8);
    let leaf16 = build_uniform_leaf(16);
    let leaf32 = build_uniform_leaf(32);
    let sparse = build_sparse_leaf();
    let pathological = build_pathological_leaf();

    bench_one_shape("uniform/1-unit", &leaf1, iters);
    bench_one_shape("uniform/2-unit", &leaf2, iters);
    bench_one_shape("uniform/4-unit", &leaf4, iters);
    bench_one_shape("uniform/8-unit", &leaf8, iters);
    bench_one_shape("uniform/16-unit", &leaf16, iters);
    bench_one_shape("uniform/32-unit", &leaf32, iters);
    bench_one_shape("sparse/4-unit-50%", &sparse, iters);
    bench_one_shape("pathological/128", &pathological, iters);
}
