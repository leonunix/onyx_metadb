//! Onyx-aware compact leaf encoding (v3 — per-leaf base_seq + u32 delta).
//!
//! The legacy dense leaf format stored 128 × 28 B `BlockmapValue` records
//! back-to-back. Onyx's packer puts consecutive LBAs into the same
//! compression unit, so a single leaf typically references only 1..8
//! distinct units. That gives us a lot of redundancy to fold out.
//!
//! Per-unit shared fields (7 of 9 BlockmapValue fields — including
//! `crc32`, which is a unit-level checksum, see
//! `onyx_storage::buffer::flush::writer::passthrough` for the
//! construction site):
//!   pba, compression, unit_compressed_size, unit_original_size,
//!   slot_offset, crc32, flags
//!
//! `unit_lba_count` is **not stored on disk** in v3; it is reconstructed
//! on read as `unit_original_size / BLOCK_SIZE_4K`. Onyx's writer always
//! sets `unit_original_size = lba_count * 4096`, so the round trip is
//! exact.
//!
//! Per-entry varying fields:
//!   offset_in_unit  (u16; which 4 KB LBA within the compressed unit)
//!   seq_delta       (u32 BE; the full per-LBA commit seq is
//!                    `base_seq + seq_delta`, where `base_seq` lives in
//!                    the leaf header. Sentinel `u32::MAX` encodes the
//!                    "no guard" seq=0 case, preserving the
//!                    `seq_guard` CAS semantics introduced in 0xB2 WAL.)
//!
//! # On-disk layout (within the 4032 B leaf payload)
//!
//! ```text
//!   [ 0.. 16]  bitmap          128 bits, LE within each byte
//!   [16.. 17]  unit_count      u8 (number of live entries in unit dict)
//!   [17.. 18]  format_version  u8 (= COMPACT_VERSION)
//!   [18.. 26]  base_seq        u64 BE  (leaf-local seq base, set on
//!                                       first non-sentinel insert)
//!   [26..922]  entries         128 × 7 B (slot-indexed dense array)
//!     entry @ slot s lives at offset 26 + s*7
//!       [0..1]  unit_idx       u8 (index into unit dict)
//!       [1..3]  offset_in_unit u16 BE
//!       [3..7]  seq_delta      u32 BE
//!                — `u32::MAX` ⇒ full_seq = 0 ("no guard")
//!                — otherwise   ⇒ full_seq = base_seq + seq_delta
//!     Unset slots are zero (caller checks bitmap before reading).
//!   [922..922+24*N]  unit dict (N = unit_count entries × 24 B)
//!     per unit:
//!       [ 0.. 8]  base_pba           u64 BE
//!       [ 8..12]  unit_compressed_sz u32 BE
//!       [12..16]  unit_original_sz   u32 BE
//!       [16..18]  slot_offset        u16 BE
//!       [18..22]  crc32              u32 BE
//!       [22..23]  compression        u8
//!       [23..24]  flags              u8
//! ```
//!
//! # Why slot-indexed (vs popcount-indexed) entries
//!
//! A popcount-indexed entries region is a few hundred B smaller for
//! sparse leaves but makes `leaf_set` / `leaf_clear` O(N) in the leaf
//! population because every mutation memmoves the entry array's tail.
//! With slot-indexed entries we trade 896 B of fixed-size headroom for
//! O(1) per-slot reads and writes that touch a fixed payload offset.
//! Onyx's L2P leaves are densely populated in steady state (sequential
//! writes; packer fills units), so the overhead only appears for
//! transient sparse states after random deletes.
//!
//! # Worst-case sizes for a 128-entry leaf (v3)
//!
//! - empty leaf:    26 +    0 +  896 =  922 B
//! - 1 unit:        26 +   24 +  896 =  946 B
//! - 8 units:       26 +  192 +  896 = 1114 B
//! - 32 units:      26 +  768 +  896 = 1690 B
//! - 64 units:      26 + 1536 +  896 = 2458 B
//! - 100 units:     26 + 2400 +  896 = 3322 B
//! - 128 units:     26 + 3072 +  896 = 3994 B  (≤ 4032 — payload cap)
//!
//! `MAX_UNITS_PER_LEAF = 128` matches `LEAF_ENTRY_COUNT`, so even a
//! pathologically fragmented leaf (one unique unit per slot) fits.
//! The previous v2 cap of 100 left 28 slots un-storable when every
//! L2pValue was unique — see b-语义路 commit `746ee41` and the
//! `compact_in_place did not free enough room for one unit` failure
//! mode it produced under `--refill_buffers` / db_hardening tests.

use crate::error::{MetaDbError, Result};
use crate::paged::format::{LEAF_BITMAP_BYTES, LEAF_ENTRY_COUNT, LEAF_VALUE_SIZE};

/// Format version stored at offset 17 of the compact payload. Future
/// schema changes bump this; Day 1 readers reject unknown versions and
/// surface zeros so a stray on-disk byte cannot pose as a valid value.
///
/// v1: pre-seq layout (28 B values, 11 B per-slot record, 100 unit cap).
/// v2: per-slot u64 seq (36 B values, 11 B per-slot record, 100 unit cap).
/// v3: per-leaf base_seq + u32 delta, drop on-disk lba_count, 128 unit cap.
pub const COMPACT_VERSION: u8 = 3;

/// Sentinel `seq_delta` value encoding "no seq guard" (full_seq=0).
pub const SEQ_DELTA_NO_GUARD: u32 = u32::MAX;

/// Onyx 4 KiB block size used to recover `unit_lba_count` from
/// `unit_original_size`. Must match
/// `onyx_storage::types::BLOCK_SIZE`.
pub const BLOCK_SIZE_4K: u32 = 4096;

/// Fixed-size fields at the head of a compact payload. Always present,
/// even for an empty leaf.
/// Layout: `[bitmap 16 | unit_count 1 | version 1 | base_seq 8]`.
pub const COMPACT_HEADER_BYTES: usize = LEAF_BITMAP_BYTES + 1 + 1 + 8; // 26

/// Byte offset of the leaf-local `base_seq` field within the header.
pub const COMPACT_BASE_SEQ_OFFSET: usize = LEAF_BITMAP_BYTES + 2;

/// Size of one per-entry slot-indexed record on disk.
/// Layout: `[unit_idx:u8 | offset_in_unit:u16 BE | seq_delta:u32 BE]`.
pub const COMPACT_ENTRY_BYTES: usize = 7;

/// Byte footprint of the slot-indexed entries region (always present).
pub const COMPACT_ENTRIES_REGION_BYTES: usize = LEAF_ENTRY_COUNT * COMPACT_ENTRY_BYTES;

/// Byte offset (within the leaf payload) at which the entries region
/// begins.
pub const COMPACT_ENTRIES_OFFSET: usize = COMPACT_HEADER_BYTES;

/// Byte offset at which the unit dictionary begins.
pub const COMPACT_UNIT_DICT_OFFSET: usize = COMPACT_ENTRIES_OFFSET + COMPACT_ENTRIES_REGION_BYTES;

/// Size of one unit-dict entry on disk.
/// Layout: `[base_pba 8 | comp_sz 4 | orig_sz 4 | slot_off 2 | crc32 4 | comp 1 | flags 1]`.
/// (`unit_lba_count` is derived from `unit_original_size` on decode.)
pub const COMPACT_UNIT_BYTES: usize = 24;

/// Footprint of the legacy dense format (16 B bitmap + 128 × 36 B
/// values). Kept as a reference baseline for benches and CR reporting;
/// the project has no in-service dense leaves, so encode never falls
/// back to it.
pub const DENSE_FOOTPRINT_BYTES: usize = LEAF_BITMAP_BYTES + LEAF_ENTRY_COUNT * LEAF_VALUE_SIZE;

/// Maximum number of distinct units a single leaf can reference. With
/// v3 schema (per-leaf base_seq + u32 delta, no on-disk lba_count) the
/// entries region is 896 B which leaves `(4032 - 26 - 896) / 24 = 129`
/// units of dict space — but a leaf can have at most `LEAF_ENTRY_COUNT
/// = 128` distinct units (one per slot), so we cap at 128. Onyx
/// typical packing keeps real leaves well under this; the cap exists
/// to absorb pathologically fragmented workloads
/// (`--refill_buffers` / 1-LBA-per-unit) without losing writes.
pub const MAX_UNITS_PER_LEAF: usize = 128;

/// Byte offset (within the leaf payload) of unit `i` in the dictionary.
#[inline]
pub const fn unit_offset(i: usize) -> usize {
    COMPACT_UNIT_DICT_OFFSET + i * COMPACT_UNIT_BYTES
}

/// Byte offset (within the leaf payload) of the entry record for slot `s`.
#[inline]
pub const fn entry_offset(s: usize) -> usize {
    COMPACT_ENTRIES_OFFSET + s * COMPACT_ENTRY_BYTES
}

/// Total compact-encoded size for a leaf with `unit_count` units. The
/// entries region is fixed at 1408 B regardless of population.
#[inline]
pub const fn compact_size(unit_count: usize) -> usize {
    COMPACT_UNIT_DICT_OFFSET + unit_count * COMPACT_UNIT_BYTES
}

/// Per-unit shared metadata extracted from a 36 B `L2pValue`. Two
/// values share a unit iff every field here is byte-identical. `crc32`
/// belongs here too — it's a unit-level checksum, not per-LBA.
///
/// `unit_lba_count` from the BlockmapValue is **not** stored on disk;
/// it's recovered on read as `unit_original_size / BLOCK_SIZE_4K`.
/// Onyx writers always set `unit_original_size = lba_count * 4096`, so
/// the round trip is exact (asserted in `from_value`).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct UnitMeta {
    pub(crate) base_pba: u64,
    pub(crate) unit_compressed_size: u32,
    pub(crate) unit_original_size: u32,
    pub(crate) slot_offset: u16,
    pub(crate) crc32: u32,
    pub(crate) compression: u8,
    pub(crate) flags: u8,
}

impl UnitMeta {
    /// Decompose a 36 B `L2pValue` into (unit-shared, per-entry-varying)
    /// parts. The first 28 B match
    /// `onyx_storage::meta::schema::encode_blockmap_value`; the last 8 B
    /// are a big-endian u64 commit-seq (v2 schema, see crate::paged::format::L2P_SEQ_OFFSET).
    #[inline]
    fn from_value(v: &[u8; LEAF_VALUE_SIZE]) -> (Self, EntryDelta) {
        let unit = UnitMeta {
            base_pba: u64::from_be_bytes(v[0..8].try_into().unwrap()),
            compression: v[8],
            unit_compressed_size: u32::from_be_bytes(v[9..13].try_into().unwrap()),
            unit_original_size: u32::from_be_bytes(v[13..17].try_into().unwrap()),
            crc32: u32::from_be_bytes(v[21..25].try_into().unwrap()),
            slot_offset: u16::from_be_bytes(v[25..27].try_into().unwrap()),
            flags: v[27],
        };
        // Onyx invariant: unit_original_size = unit_lba_count * 4096.
        // The on-disk dict drops lba_count, so we rely on this to
        // reconstruct it on decode. If a future writer breaks the
        // invariant, decode will return a wrong lba_count for the
        // unaffected callers; debug builds catch it here.
        debug_assert_eq!(
            u16::from_be_bytes(v[17..19].try_into().unwrap()) as u32 * BLOCK_SIZE_4K,
            unit.unit_original_size,
            "v3 compact encoding requires unit_original_size == lba_count * 4096"
        );
        let entry = EntryDelta {
            offset_in_unit: u16::from_be_bytes(v[19..21].try_into().unwrap()),
            seq: u64::from_be_bytes(v[28..36].try_into().unwrap()),
        };
        (unit, entry)
    }

    #[inline]
    fn write_to(&self, out: &mut [u8; COMPACT_UNIT_BYTES]) {
        out[0..8].copy_from_slice(&self.base_pba.to_be_bytes());
        out[8..12].copy_from_slice(&self.unit_compressed_size.to_be_bytes());
        out[12..16].copy_from_slice(&self.unit_original_size.to_be_bytes());
        out[16..18].copy_from_slice(&self.slot_offset.to_be_bytes());
        out[18..22].copy_from_slice(&self.crc32.to_be_bytes());
        out[22] = self.compression;
        out[23] = self.flags;
    }

    #[inline]
    fn read_from(buf: &[u8]) -> Self {
        debug_assert!(buf.len() >= COMPACT_UNIT_BYTES);
        UnitMeta {
            base_pba: u64::from_be_bytes(buf[0..8].try_into().unwrap()),
            unit_compressed_size: u32::from_be_bytes(buf[8..12].try_into().unwrap()),
            unit_original_size: u32::from_be_bytes(buf[12..16].try_into().unwrap()),
            slot_offset: u16::from_be_bytes(buf[16..18].try_into().unwrap()),
            crc32: u32::from_be_bytes(buf[18..22].try_into().unwrap()),
            compression: buf[22],
            flags: buf[23],
        }
    }

    /// Reconstruct `unit_lba_count` from `unit_original_size`. Used by
    /// `compose` to write back into the L2pValue bytes 17..19.
    #[inline]
    fn lba_count(&self) -> u16 {
        // Onyx writers always set original_size = lba_count * 4096;
        // round up just in case (a non-4K-aligned size shouldn't occur
        // in production but we don't want to truncate silently).
        self.unit_original_size.div_ceil(BLOCK_SIZE_4K).min(u16::MAX as u32) as u16
    }
}

/// Per-entry delta over its unit. `offset_in_unit` selects which 4 KB
/// LBA in the compressed unit this slot maps to; `seq` carries the
/// commit-time monotonic sequence number used by metadb's `seq_guard`
/// to reject stale concurrent commits (sentinel 0 = no guard).
///
/// In-memory representation always carries the **full** u64 seq. The
/// v3 on-disk encoding stores a u32 delta relative to the leaf's
/// `base_seq` (with `u32::MAX` reserved as the "no guard" sentinel for
/// full_seq=0). Conversion happens at the `read_entry` /
/// `write_entry` boundary.
#[derive(Clone, Copy, Debug)]
pub(crate) struct EntryDelta {
    pub(crate) offset_in_unit: u16,
    pub(crate) seq: u64,
}

/// Outcome of a (slot, base_seq, full_seq) encode attempt. Callers
/// use this to drive the rebase loop in [`compact_in_place`] /
/// [`paged::format::leaf_set`].
#[derive(Debug, Clone, Copy)]
pub(crate) enum WriteEntryOutcome {
    /// Entry written successfully under the existing `base_seq`.
    Written,
    /// `base_seq` was 0 and we just adopted the incoming `full_seq` as
    /// the new base. Callers must write the new base back to the
    /// payload header.
    AdoptedBase { new_base: u64 },
    /// The delta would overflow `u32::MAX` (or underflow because the
    /// incoming seq is below the current base). The caller must run
    /// [`compact_in_place`] (which rebases to `min(live seqs, new
    /// seq)`) and retry.
    NeedsRebase,
}

impl EntryDelta {
    /// Encode this entry into 7 bytes at `out`, using `base_seq` as the
    /// leaf's seq origin. Returns the outcome so the caller can rebase
    /// if needed.
    ///
    /// Storage:
    ///   - `self.seq == 0`            → seq_delta = `u32::MAX` (no-guard sentinel)
    ///   - `base_seq == 0` & `seq>0`  → adopt `base_seq = seq`, seq_delta = 0
    ///   - else                       → seq_delta = `seq - base_seq`
    ///                                  (rebase needed if `seq < base_seq`
    ///                                   or `seq - base_seq >= u32::MAX`).
    #[inline]
    fn try_write_to(
        &self,
        unit_idx: u8,
        base_seq: u64,
        out: &mut [u8; COMPACT_ENTRY_BYTES],
    ) -> WriteEntryOutcome {
        let (delta, outcome) = if self.seq == 0 {
            (SEQ_DELTA_NO_GUARD, WriteEntryOutcome::Written)
        } else if base_seq == 0 {
            (0u32, WriteEntryOutcome::AdoptedBase { new_base: self.seq })
        } else if self.seq < base_seq {
            return WriteEntryOutcome::NeedsRebase;
        } else {
            let diff = self.seq - base_seq;
            // Reserve u32::MAX as the no-guard sentinel; treat diff at
            // the sentinel value as overflow so we never collide.
            if diff >= SEQ_DELTA_NO_GUARD as u64 {
                return WriteEntryOutcome::NeedsRebase;
            }
            (diff as u32, WriteEntryOutcome::Written)
        };
        out[0] = unit_idx;
        out[1..3].copy_from_slice(&self.offset_in_unit.to_be_bytes());
        out[3..7].copy_from_slice(&delta.to_be_bytes());
        outcome
    }

    /// Decode a 7-byte entry record into `(unit_idx, EntryDelta)` using
    /// the leaf's `base_seq` to reconstruct the full per-entry seq.
    #[inline]
    fn read_from(buf: &[u8], base_seq: u64) -> (u8, Self) {
        debug_assert!(buf.len() >= COMPACT_ENTRY_BYTES);
        let unit_idx = buf[0];
        let offset_in_unit = u16::from_be_bytes(buf[1..3].try_into().unwrap());
        let delta = u32::from_be_bytes(buf[3..7].try_into().unwrap());
        let seq = if delta == SEQ_DELTA_NO_GUARD {
            0
        } else {
            base_seq.wrapping_add(delta as u64)
        };
        (
            unit_idx,
            EntryDelta {
                offset_in_unit,
                seq,
            },
        )
    }
}

/// Reassemble a 36 B `L2pValue` from its unit + entry parts.
/// Inverse of `UnitMeta::from_value`. `unit_lba_count` is recovered
/// from `unit_original_size / BLOCK_SIZE_4K`.
#[inline]
fn compose(unit: &UnitMeta, entry: &EntryDelta) -> [u8; LEAF_VALUE_SIZE] {
    let mut v = [0u8; LEAF_VALUE_SIZE];
    v[0..8].copy_from_slice(&unit.base_pba.to_be_bytes());
    v[8] = unit.compression;
    v[9..13].copy_from_slice(&unit.unit_compressed_size.to_be_bytes());
    v[13..17].copy_from_slice(&unit.unit_original_size.to_be_bytes());
    v[17..19].copy_from_slice(&unit.lba_count().to_be_bytes());
    v[19..21].copy_from_slice(&entry.offset_in_unit.to_be_bytes());
    v[21..25].copy_from_slice(&unit.crc32.to_be_bytes());
    v[25..27].copy_from_slice(&unit.slot_offset.to_be_bytes());
    v[27] = unit.flags;
    v[28..36].copy_from_slice(&entry.seq.to_be_bytes());
    v
}

// =====================================================================
// Page-level mutation primitives (used by paged::format)
//
// All primitives below operate on a `&mut [u8]` payload of length at
// least `COMPACT_UNIT_DICT_OFFSET`. The bitmap, version byte, entries
// region, and unit_count slots are at fixed offsets so reads/writes
// are O(1) regardless of population. The unit dictionary lives at
// [COMPACT_UNIT_DICT_OFFSET..) and grows from there.
//
// Convention: callers must seed a fresh leaf via `init_payload` before
// using these primitives. `init_payload` writes the version byte and
// zeroes everything else.
// =====================================================================

/// Stamp the version byte into a freshly-zeroed leaf payload. The
/// bitmap, unit_count, base_seq, entries region, and unit dictionary
/// are expected to already be zero (the caller's `init_leaf` zeroes the
/// page first).
#[inline]
pub fn init_payload(payload: &mut [u8]) {
    debug_assert!(payload.len() >= COMPACT_UNIT_DICT_OFFSET);
    payload[LEAF_BITMAP_BYTES + 1] = COMPACT_VERSION;
}

#[inline]
pub(crate) fn read_unit_count(payload: &[u8]) -> u8 {
    payload[LEAF_BITMAP_BYTES]
}

#[inline]
pub(crate) fn write_unit_count(payload: &mut [u8], n: u8) {
    payload[LEAF_BITMAP_BYTES] = n;
}

/// Read the leaf-local seq base from the header. Zero means "no
/// non-sentinel entries have been written yet".
#[inline]
pub(crate) fn read_base_seq(payload: &[u8]) -> u64 {
    u64::from_be_bytes(
        payload[COMPACT_BASE_SEQ_OFFSET..COMPACT_BASE_SEQ_OFFSET + 8]
            .try_into()
            .unwrap(),
    )
}

/// Write the leaf-local seq base into the header. Callers update this
/// when an entry adopts a new base or [`compact_in_place`] rebases.
#[inline]
pub(crate) fn write_base_seq(payload: &mut [u8], base_seq: u64) {
    payload[COMPACT_BASE_SEQ_OFFSET..COMPACT_BASE_SEQ_OFFSET + 8]
        .copy_from_slice(&base_seq.to_be_bytes());
}

/// Read the unit-dict entry at `idx`. Caller must ensure
/// `idx < read_unit_count(payload)`.
#[inline]
pub(crate) fn read_unit(payload: &[u8], idx: usize) -> UnitMeta {
    let off = unit_offset(idx);
    UnitMeta::read_from(&payload[off..off + COMPACT_UNIT_BYTES])
}

#[inline]
pub(crate) fn write_unit(payload: &mut [u8], idx: usize, u: &UnitMeta) {
    let off = unit_offset(idx);
    let dst: &mut [u8; COMPACT_UNIT_BYTES] = (&mut payload[off..off + COMPACT_UNIT_BYTES])
        .try_into()
        .unwrap();
    u.write_to(dst);
}

/// Read the entry record at `slot`. Returns `(unit_idx, EntryDelta)`.
/// Callers must check the bitmap first; an unset slot's bytes are zero
/// by invariant but `(0, EntryDelta { offset_in_unit: 0, seq: 0 })` is
/// indistinguishable from a real entry pointing at unit 0 with offset 0
/// and a legacy seq=0 sentinel.
#[inline]
pub(crate) fn read_entry(payload: &[u8], slot: usize) -> (u8, EntryDelta) {
    let off = entry_offset(slot);
    let base_seq = read_base_seq(payload);
    EntryDelta::read_from(&payload[off..off + COMPACT_ENTRY_BYTES], base_seq)
}

/// Try to write an entry record at `slot`. Returns the outcome so the
/// caller can adopt a new base / rebase as needed.
#[inline]
pub(crate) fn try_write_entry(
    payload: &mut [u8],
    slot: usize,
    unit_idx: u8,
    e: &EntryDelta,
) -> WriteEntryOutcome {
    let base_seq = read_base_seq(payload);
    let off = entry_offset(slot);
    let dst: &mut [u8; COMPACT_ENTRY_BYTES] = (&mut payload[off..off + COMPACT_ENTRY_BYTES])
        .try_into()
        .unwrap();
    e.try_write_to(unit_idx, base_seq, dst)
}

/// Write an entry. Returns an error if base_seq adoption or rebase
/// would be needed — callers that own the payload exclusively (page
/// rewrites, compact_in_place fallback) use [`try_write_entry`] +
/// rebase loop instead. This thin wrapper exists because most callers
/// in this module historically used `write_entry`; they all happen to
/// be paths where base_seq is already set up correctly.
#[inline]
pub(crate) fn write_entry(payload: &mut [u8], slot: usize, unit_idx: u8, e: &EntryDelta) {
    match try_write_entry(payload, slot, unit_idx, e) {
        WriteEntryOutcome::Written => {}
        WriteEntryOutcome::AdoptedBase { new_base } => {
            write_base_seq(payload, new_base);
        }
        WriteEntryOutcome::NeedsRebase => {
            panic!(
                "compact leaf write_entry slot={slot} unit={unit_idx} seq={} requires \
                 rebase (base_seq={}); caller must use try_write_entry + compact_in_place \
                 loop instead",
                e.seq,
                read_base_seq(payload)
            );
        }
    }
}

/// Zero the per-slot entry record. Does not touch the bitmap.
#[inline]
pub(crate) fn zero_entry(payload: &mut [u8], slot: usize) {
    let off = entry_offset(slot);
    payload[off..off + COMPACT_ENTRY_BYTES].fill(0);
}

/// Bitmap helpers operating on a payload slice. The bitmap region is
/// always at the head, so these never need to know the unit_dict size.
#[inline]
pub(crate) fn payload_bit_set(payload: &[u8], slot: usize) -> bool {
    (payload[slot / 8] >> (slot % 8)) & 1 == 1
}

#[inline]
pub(crate) fn payload_bit_set_true(payload: &mut [u8], slot: usize) {
    payload[slot / 8] |= 1u8 << (slot % 8);
}

#[inline]
pub(crate) fn payload_bit_clear(payload: &mut [u8], slot: usize) {
    payload[slot / 8] &= !(1u8 << (slot % 8));
}

/// Decompose a 36 B value into (UnitMeta, EntryDelta). Re-export of
/// the private helper for use by the leaf accessors.
#[inline]
pub(crate) fn decompose_value(v: &[u8; LEAF_VALUE_SIZE]) -> (UnitMeta, EntryDelta) {
    UnitMeta::from_value(v)
}

/// Find an existing unit in the dict whose bytes match `target`, OR
/// append a new dict entry if there's space. Returns the unit_idx, or
/// `None` if appending would push the dict past the leaf payload (in
/// which case the caller must run `compact_in_place` and retry).
///
/// "Find" walks the dict linearly. With 1..32 typical units this beats
/// a HashMap; with the v3 worst case of 128 units it's still ~3.1 KiB
/// of sequential reads, well within an L1 line.
pub(crate) fn find_or_append_unit(payload: &mut [u8], target: &UnitMeta) -> Option<u8> {
    let count = read_unit_count(payload) as usize;
    let cap = max_units_per_payload(payload.len()).min(MAX_UNITS_PER_LEAF);
    if count > cap {
        return None;
    }
    for i in 0..count {
        if read_unit(payload, i) == *target {
            return Some(i as u8);
        }
    }
    // No match — append. Check we can fit one more unit record before
    // running off the end of the payload.
    let new_off = unit_offset(count);
    if new_off + COMPACT_UNIT_BYTES > payload.len() {
        return None;
    }
    if count >= MAX_UNITS_PER_LEAF {
        return None;
    }
    write_unit(payload, count, target);
    write_unit_count(payload, (count + 1) as u8);
    Some(count as u8)
}

/// Maximum unit-dict capacity given the payload size. With v3's 4032 B
/// payload that's `(4032 - 922) / 24 = 129`, capped at
/// `MAX_UNITS_PER_LEAF = 128` by the caller (since a leaf can have at
/// most one unique unit per slot).
#[inline]
pub const fn max_units_per_payload(payload_len: usize) -> usize {
    if payload_len <= COMPACT_UNIT_DICT_OFFSET {
        0
    } else {
        (payload_len - COMPACT_UNIT_DICT_OFFSET) / COMPACT_UNIT_BYTES
    }
}

/// Rebuild the unit dictionary in place, dropping unreferenced units
/// and renumbering `unit_idx` references in the entries region. Also
/// rebases `base_seq` to `min(live full_seq)` so subsequent inserts
/// have the maximum possible delta headroom.
///
/// See [`compact_in_place_with_incoming`] for the variant that also
/// accounts for a not-yet-written entry's seq during rebase.
pub(crate) fn compact_in_place(payload: &mut [u8]) -> Result<()> {
    compact_in_place_with_incoming(payload, 0)
}

/// Like [`compact_in_place`] but also folds `incoming_seq` (the seq
/// of an entry about to be written by the caller) into the
/// `new_base = min(...)` computation. The caller still does the
/// follow-up `try_write_entry` itself; this just ensures the rebase
/// leaves enough delta headroom for the incoming write.
///
/// Pass `incoming_seq = 0` to skip incoming-seq consideration (the
/// behaviour [`compact_in_place`] gives — appropriate when compaction
/// is purely about reclaiming dead units, not about absorbing a new
/// out-of-range seq).
///
/// Called when:
/// - `find_or_append_unit` returns `None` because the dict is full.
/// - `try_write_entry` returns `NeedsRebase` because the seq delta
///   would overflow `u32::MAX` (or because the incoming seq is below
///   the current base).
/// - The caller wants to reclaim space from dead units (eg. a
///   periodic compaction pass).
///
/// Algorithm:
/// 1. Compute `new_base = min(live full_seq among non-sentinel
///    entries, incoming_seq if non-zero)`. If no candidate seqs
///    exist, `new_base = 0`.
/// 2. Scan all 128 entry slots. For each set slot, read its old
///    `unit_idx` and full seq. Build `old → new` index translation,
///    allocating a new index the first time we see each old index.
/// 3. Re-emit live units to a scratch Vec (preserving discovery order).
/// 4. Rewrite each live entry under the new base_seq (this re-encodes
///    seq_delta = full_seq - new_base, preserving sentinels).
/// 5. Overwrite the unit dict with the live units and zero out the
///    tail bytes between the new and old `unit_count`.
/// 6. Write the new base_seq into the header.
pub(crate) fn compact_in_place_with_incoming(
    payload: &mut [u8],
    incoming_seq: u64,
) -> Result<()> {
    let old_count = read_unit_count(payload) as usize;
    let cap = max_units_per_payload(payload.len()).min(MAX_UNITS_PER_LEAF);
    if old_count > cap {
        return Err(MetaDbError::Corruption(format!(
            "compact leaf unit_count {old_count} exceeds payload capacity {cap}"
        )));
    }
    if old_count == 0 {
        // No units yet — but base_seq may still be nonzero from a prior
        // population that's been entirely cleared. Reset it so the next
        // insert can adopt cleanly.
        write_base_seq(payload, 0);
        return Ok(());
    }

    // Pass 1: collect live entries' (slot, old_idx, full_seq) and the
    // new base_seq. We need to read entries with the current base_seq
    // BEFORE rewriting it.
    let mut live: Vec<(usize, usize, EntryDelta)> = Vec::with_capacity(LEAF_ENTRY_COUNT);
    let mut new_base: Option<u64> = None;
    // Fold the incoming-seq (if non-zero) into the min so the post-
    // compact base accommodates the caller's pending write without a
    // second rebase round-trip.
    if incoming_seq != 0 {
        new_base = Some(incoming_seq);
    }
    for slot in 0..LEAF_ENTRY_COUNT {
        if !payload_bit_set(payload, slot) {
            continue;
        }
        let (old_idx, entry) = read_entry(payload, slot);
        let old_idx = old_idx as usize;
        if old_idx >= old_count {
            return Err(MetaDbError::Corruption(format!(
                "compact leaf live slot {slot} references unit {old_idx} but dict has {old_count}"
            )));
        }
        if entry.seq != 0 {
            new_base = Some(match new_base {
                Some(b) => b.min(entry.seq),
                None => entry.seq,
            });
        }
        live.push((slot, old_idx, entry));
    }

    // Discovery-ordered remap from old unit_idx → new unit_idx. Width
    // matches the on-disk u8 unit_idx ceiling (256), generous vs cap.
    let mut remap: [Option<u8>; 256] = [None; 256];
    let mut live_units: Vec<UnitMeta> = Vec::with_capacity(old_count);

    let resolved_base = new_base.unwrap_or(0);
    // Write the new base before rewriting entries: try_write_entry
    // reads base_seq via the payload header.
    write_base_seq(payload, resolved_base);

    for (slot, old_idx, entry) in &live {
        let new_idx = match remap[*old_idx] {
            Some(i) => i,
            None => {
                // First time we see this old unit — copy it forward.
                let unit = read_unit(payload, *old_idx);
                let new_idx = live_units.len() as u8;
                live_units.push(unit);
                remap[*old_idx] = Some(new_idx);
                new_idx
            }
        };
        // Re-emit the entry under the new base. With new_base set to
        // the min of all live full_seqs, every delta is non-negative
        // and (since deltas were <= u32::MAX-1 at write time, and
        // shifting all by a constant preserves max-min) still fits.
        match try_write_entry(payload, *slot, new_idx, entry) {
            WriteEntryOutcome::Written => {}
            WriteEntryOutcome::AdoptedBase { new_base } => {
                // Should not happen: base_seq=0 only when no
                // non-sentinel entries exist, but we set base = min of
                // those. If it does happen, propagate the adoption.
                write_base_seq(payload, new_base);
            }
            WriteEntryOutcome::NeedsRebase => {
                return Err(MetaDbError::Corruption(format!(
                    "compact leaf seq_delta would overflow u32 even after rebase: \
                     slot={slot} seq={} base={}",
                    entry.seq, resolved_base
                )));
            }
        }
    }

    // Re-emit dict tightly.
    for (i, u) in live_units.iter().enumerate() {
        write_unit(payload, i, u);
    }
    // Zero the tail of the old dict region so a CRC over the page
    // doesn't capture stale unit bytes.
    let new_count = live_units.len();
    if new_count < old_count {
        let zero_start = unit_offset(new_count);
        let zero_end = unit_offset(old_count);
        debug_assert!(zero_end <= payload.len());
        payload[zero_start..zero_end].fill(0);
    }
    write_unit_count(payload, new_count as u8);
    Ok(())
}

/// Decode the value at slot `s` from a payload (no version check —
/// hot-path variant for in-tree readers that already trust the page).
/// Returns `[0u8; 28]` if the slot's bitmap bit is clear.
#[inline]
pub(crate) fn payload_decode_at(payload: &[u8], slot: usize) -> [u8; LEAF_VALUE_SIZE] {
    payload_decode_at_checked(payload, slot)
        .ok()
        .flatten()
        .unwrap_or([0u8; LEAF_VALUE_SIZE])
}

pub(crate) fn payload_decode_at_checked(
    payload: &[u8],
    slot: usize,
) -> Result<Option<[u8; LEAF_VALUE_SIZE]>> {
    if slot >= LEAF_ENTRY_COUNT {
        return Err(MetaDbError::Corruption(format!(
            "compact leaf slot {slot} out of range"
        )));
    }
    if payload.len() < COMPACT_UNIT_DICT_OFFSET {
        return Err(MetaDbError::Corruption(format!(
            "compact leaf payload too short: {}",
            payload.len()
        )));
    }
    let version = payload[LEAF_BITMAP_BYTES + 1];
    if version != COMPACT_VERSION {
        return Err(MetaDbError::Corruption(format!(
            "compact leaf version {version} != {COMPACT_VERSION}"
        )));
    }
    if !payload_bit_set(payload, slot) {
        return Ok(None);
    }
    let unit_count = read_unit_count(payload) as usize;
    let cap = max_units_per_payload(payload.len()).min(MAX_UNITS_PER_LEAF);
    if unit_count > cap {
        return Err(MetaDbError::Corruption(format!(
            "compact leaf unit_count {unit_count} exceeds payload capacity {cap}"
        )));
    }
    let (unit_idx, entry) = read_entry(payload, slot);
    if unit_idx as usize >= unit_count {
        return Err(MetaDbError::Corruption(format!(
            "compact leaf slot {slot} references unit {} but dict has {unit_count}",
            unit_idx
        )));
    }
    let unit = read_unit(payload, unit_idx as usize);
    Ok(Some(compose(&unit, &entry)))
}

// =====================================================================
// Vec-based encode/decode (used by full-leaf rebuilds, benches, tests)
// =====================================================================

/// Encode a dense `(bitmap, values)` leaf body into the compact form.
///
/// `bitmap` is the 16 B presence bitmap; `values` is the 128 × 36 B
/// value array (unset slots may be anything — they're skipped via the
/// bitmap). The output is always strictly smaller than the 4032 B leaf
/// payload, so encode never fails on size grounds and returns a
/// `Vec<u8>` directly.
pub fn encode(
    bitmap: &[u8; LEAF_BITMAP_BYTES],
    values: &[[u8; LEAF_VALUE_SIZE]; LEAF_ENTRY_COUNT],
) -> Vec<u8> {
    // Build the unit dict in a small scratch Vec; for the typical
    // 1..8 distinct units, `units.iter().position()` outperforms a
    // HashMap. We bound the dict to 128 (one per slot) which is well
    // below the u8 unit_idx ceiling.
    let mut units: Vec<UnitMeta> = Vec::with_capacity(8);
    // Pre-size the output buffer to the v3 worst case (128 distinct
    // units = 3994 B). We'll truncate to the actual dict size after.
    let mut out = vec![0u8; compact_size(MAX_UNITS_PER_LEAF)];
    out[..LEAF_BITMAP_BYTES].copy_from_slice(bitmap);
    out[LEAF_BITMAP_BYTES + 1] = COMPACT_VERSION;

    // Pass 1: decompose every set slot, compute base_seq = min(live
    // non-sentinel seqs) so encode produces the same byte pattern as
    // a leaf populated via the page-level primitives + final
    // compact_in_place.
    let mut entries: Vec<(usize, EntryDelta, usize /*unit_idx*/)> =
        Vec::with_capacity(LEAF_ENTRY_COUNT);
    let mut base_seq: Option<u64> = None;
    for slot in 0..LEAF_ENTRY_COUNT {
        if (bitmap[slot / 8] >> (slot % 8)) & 1 == 0 {
            continue;
        }
        let (unit, entry) = UnitMeta::from_value(&values[slot]);
        let unit_idx = match units.iter().position(|u| *u == unit) {
            Some(i) => i,
            None => {
                debug_assert!(units.len() < MAX_UNITS_PER_LEAF);
                units.push(unit);
                units.len() - 1
            }
        };
        if entry.seq != 0 {
            base_seq = Some(match base_seq {
                Some(b) => b.min(entry.seq),
                None => entry.seq,
            });
        }
        entries.push((slot, entry, unit_idx));
    }
    let resolved_base = base_seq.unwrap_or(0);
    out[COMPACT_BASE_SEQ_OFFSET..COMPACT_BASE_SEQ_OFFSET + 8]
        .copy_from_slice(&resolved_base.to_be_bytes());

    for (slot, entry, unit_idx) in &entries {
        let off = entry_offset(*slot);
        let dst: &mut [u8; COMPACT_ENTRY_BYTES] = (&mut out[off..off + COMPACT_ENTRY_BYTES])
            .try_into()
            .unwrap();
        let outcome = entry.try_write_to(*unit_idx as u8, resolved_base, dst);
        debug_assert!(
            !matches!(outcome, WriteEntryOutcome::NeedsRebase),
            "encode: seq_delta overflow at slot {slot} seq={} base={resolved_base}",
            entry.seq
        );
    }

    out[LEAF_BITMAP_BYTES] = units.len() as u8;
    for (i, u) in units.iter().enumerate() {
        let off = unit_offset(i);
        let dst: &mut [u8; COMPACT_UNIT_BYTES] = (&mut out[off..off + COMPACT_UNIT_BYTES])
            .try_into()
            .unwrap();
        u.write_to(dst);
    }
    // Truncate to the actual size (we sized for the worst case).
    out.truncate(compact_size(units.len()));
    out
}

/// Read the version byte from a compact-encoded blob. Returns `None` if
/// the blob is too short to be a compact payload at all.
#[inline]
pub fn version(encoded: &[u8]) -> Option<u8> {
    if encoded.len() < COMPACT_HEADER_BYTES {
        return None;
    }
    Some(encoded[LEAF_BITMAP_BYTES + 1])
}

/// True iff slot `i` is set in the compact-encoded blob's bitmap.
#[inline]
pub fn bit_set(encoded: &[u8], slot: usize) -> bool {
    debug_assert!(slot < LEAF_ENTRY_COUNT);
    debug_assert!(encoded.len() >= LEAF_BITMAP_BYTES);
    (encoded[slot / 8] >> (slot % 8)) & 1 == 1
}

/// Number of populated slots, as read from the bitmap. O(16) byte ops.
#[inline]
pub fn entry_count(encoded: &[u8]) -> usize {
    debug_assert!(encoded.len() >= LEAF_BITMAP_BYTES);
    encoded[..LEAF_BITMAP_BYTES]
        .iter()
        .map(|b| b.count_ones() as usize)
        .sum()
}

/// Decode the value at slot `slot`. Returns `None` if the slot is unset.
/// O(1): one fixed-offset read for the entry plus one fixed-offset read
/// for the unit dict.
pub fn decode_at(encoded: &[u8], slot: usize) -> Option<[u8; LEAF_VALUE_SIZE]> {
    debug_assert!(slot < LEAF_ENTRY_COUNT);
    if encoded.len() < COMPACT_UNIT_DICT_OFFSET {
        return None;
    }
    if encoded[LEAF_BITMAP_BYTES + 1] != COMPACT_VERSION {
        return None;
    }
    if !bit_set(encoded, slot) {
        return None;
    }
    let unit_count = encoded[LEAF_BITMAP_BYTES] as usize;
    let base_seq = u64::from_be_bytes(
        encoded[COMPACT_BASE_SEQ_OFFSET..COMPACT_BASE_SEQ_OFFSET + 8]
            .try_into()
            .unwrap(),
    );
    let entry_off = entry_offset(slot);
    let (unit_idx, entry) =
        EntryDelta::read_from(&encoded[entry_off..entry_off + COMPACT_ENTRY_BYTES], base_seq);
    if unit_idx as usize >= unit_count {
        return None;
    }
    let unit_off = unit_offset(unit_idx as usize);
    if encoded.len() < unit_off + COMPACT_UNIT_BYTES {
        return None;
    }
    let unit = UnitMeta::read_from(&encoded[unit_off..unit_off + COMPACT_UNIT_BYTES]);
    Some(compose(&unit, &entry))
}

/// Decode all populated entries. Returns an array indexed by slot;
/// `None` means the slot was unset.
///
/// Pre-decodes the unit dict once so per-slot work is two byte reads
/// plus a `compose`. Use this for full-leaf scans (`scan_prefix`,
/// invariant checks, proptest oracle).
pub fn decode_all(encoded: &[u8]) -> [Option<[u8; LEAF_VALUE_SIZE]>; LEAF_ENTRY_COUNT] {
    let mut out: [Option<[u8; LEAF_VALUE_SIZE]>; LEAF_ENTRY_COUNT] = [None; LEAF_ENTRY_COUNT];
    if encoded.len() < COMPACT_UNIT_DICT_OFFSET {
        return out;
    }
    if encoded[LEAF_BITMAP_BYTES + 1] != COMPACT_VERSION {
        return out;
    }
    let unit_count = encoded[LEAF_BITMAP_BYTES] as usize;
    let base_seq = u64::from_be_bytes(
        encoded[COMPACT_BASE_SEQ_OFFSET..COMPACT_BASE_SEQ_OFFSET + 8]
            .try_into()
            .unwrap(),
    );

    let mut units: Vec<UnitMeta> = Vec::with_capacity(unit_count);
    for i in 0..unit_count {
        let off = unit_offset(i);
        if encoded.len() < off + COMPACT_UNIT_BYTES {
            return out;
        }
        units.push(UnitMeta::read_from(&encoded[off..off + COMPACT_UNIT_BYTES]));
    }

    for slot in 0..LEAF_ENTRY_COUNT {
        if (encoded[slot / 8] >> (slot % 8)) & 1 == 0 {
            continue;
        }
        let off = entry_offset(slot);
        let (unit_idx, entry) =
            EntryDelta::read_from(&encoded[off..off + COMPACT_ENTRY_BYTES], base_seq);
        if (unit_idx as usize) < units.len() {
            out[slot] = Some(compose(&units[unit_idx as usize], &entry));
        }
    }
    out
}

#[cfg(test)]
mod tests {
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
        // 26 header + 896 entries (zero) + 0 unit dict = 922.
        assert_eq!(enc.len(), 922);
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
        // 26 + 896 + 24 = 946 (v3 with u32 delta + 24B unit)
        assert_eq!(enc.len(), 946);
        let cr = DENSE_FOOTPRINT_BYTES as f64 / enc.len() as f64;
        // 1 unit / 128 slots: dense=16+128*36=4624, compact=946 → ~4.9x.
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
        // 26 + 896 + 8*24 = 1114 (v3 with u32 delta + 24B unit)
        assert_eq!(enc.len(), 1114);
        let cr = DENSE_FOOTPRINT_BYTES as f64 / enc.len() as f64;
        // 8 units / 128 slots: dense=4624, compact=1114 → ~4.15x.
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
        // v3 caps at MAX_UNITS_PER_LEAF = 128 (= LEAF_ENTRY_COUNT).
        // The payload-bound cap is (4032 - 922) / 24 = 129; we cap at
        // 128 so every slot can have its own unique unit. 128 distinct
        // units => 26 + 896 + 24*128 = 3994 B, leaving 38 B headroom in
        // the 4032 B payload.
        let (mut bm, mut vals) = empty_leaf_input();
        for i in 0..MAX_UNITS_PER_LEAF {
            let v = bv(0x3000 + i as u64, 1, 500, 4096, 1, 0, i as u32, 0, 0);
            set(&mut bm, &mut vals, i, v);
        }
        let enc = encode(&bm, &vals);
        assert_eq!(enc.len(), 3994);
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
        // v3's 7 B per-slot record + 24 B unit + 26 B header the
        // payload-bound cap is 129, capped at MAX_UNITS_PER_LEAF=128
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
        // Sanity for v3 layout constants. Per-slot records shrink to
        // 7 B (unit_idx + offset + u32 seq_delta) and the unit-dict
        // entry shrinks to 24 B (lba_count dropped, recoverable from
        // unit_original_size / 4096). The 8-byte base_seq lives in
        // the header. The payload-bound cap on distinct units becomes
        // 129, capped at 128 = LEAF_ENTRY_COUNT.
        assert_eq!(COMPACT_HEADER_BYTES, 26);
        assert_eq!(COMPACT_BASE_SEQ_OFFSET, 18);
        assert_eq!(COMPACT_ENTRIES_OFFSET, 26);
        assert_eq!(COMPACT_UNIT_DICT_OFFSET, 922);
        assert_eq!(COMPACT_ENTRY_BYTES, 7);
        assert_eq!(COMPACT_UNIT_BYTES, 24);
        assert_eq!(MAX_UNITS_PER_LEAF, 128);
        assert_eq!(max_units_per_payload(crate::page::PAGE_PAYLOAD_SIZE), 129);
        assert_eq!(compact_size(0), 922);
        assert_eq!(compact_size(1), 946);
        assert_eq!(compact_size(8), 1114);
        assert_eq!(compact_size(MAX_UNITS_PER_LEAF), 3994);
        assert!(compact_size(MAX_UNITS_PER_LEAF) <= crate::page::PAGE_PAYLOAD_SIZE);
    }
}
