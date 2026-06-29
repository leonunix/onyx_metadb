//! Onyx-aware compact leaf encoding (v5 — adds per-unit `base_pba`
//! delta against a leaf-local `base_pba`, restoring the v3 worst-case
//! `MAX_UNITS_PER_LEAF = 128` after v4 tightened it to 110).
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
//! `unit_lba_count` is **not stored on disk** in v3+; it is reconstructed
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
//! Per-unit varying fields:
//!   pba_delta       (u32 BE; v5 NEW. The full per-unit `base_pba` is
//!                    `header.base_pba + pba_delta`. Sentinel `u32::MAX`
//!                    encodes the "not yet placed against a base"
//!                    transient state used by `write_to`; live on-disk
//!                    units always carry a real delta. Saves 4 B / unit
//!                    vs the v4 inline u64, which is what buys back the
//!                    v3 128-unit cap after birth_delta was added.)
//!   birth_delta     (u32 BE; v4. The full per-PBA `birth_lsn` is
//!                    `header.base_birth_lsn + birth_delta`. Sentinel
//!                    `u32::MAX` encodes the "no birth recorded" case
//!                    (full_birth_lsn = 0). Powers 's per-volume
//!                    dead-list emission — see
//!                    memory.)
//!
//! # On-disk layout (within the 4032 B leaf payload)
//!
//! ```text
//!   [ 0.. 16]  bitmap          128 bits, LE within each byte
//!   [16.. 17]  unit_count      u8 (number of live entries in unit dict)
//!   [17.. 18]  format_version  u8 (= COMPACT_VERSION)
//!   [18.. 26]  base_seq        u64 BE  (leaf-local seq base, set on
//!                                       first non-sentinel insert)
//!   [26.. 34]  base_birth_lsn  u64 BE  (leaf-local birth_lsn base, set on
//!                                       first non-sentinel insert, v4)
//!   [34.. 42]  base_pba        u64 BE  (leaf-local pba base, set on
//!                                       first unit insert, v5 NEW)
//!   [42..938]  entries         128 × 7 B (slot-indexed dense array)
//!     entry @ slot s lives at offset 42 + s*7
//!       [0..1]  unit_idx       u8 (index into unit dict)
//!       [1..3]  offset_in_unit u16 BE
//!       [3..7]  seq_delta      u32 BE
//!                — `u32::MAX` ⇒ full_seq = 0 ("no guard")
//!                — otherwise   ⇒ full_seq = base_seq + seq_delta
//!     Unset slots are zero (caller checks bitmap before reading).
//!   [938..938+24*N]  unit dict (N = unit_count entries × 24 B)
//!     per unit:
//!       [ 0.. 4]  pba_delta          u32 BE (v5 NEW; was 8 B inline base_pba)
//!       [ 4.. 8]  unit_compressed_sz u32 BE
//!       [ 8..12]  unit_original_sz   u32 BE
//!       [12..14]  slot_offset        u16 BE
//!       [14..18]  crc32              u32 BE
//!       [18..19]  compression        u8
//!       [19..20]  flags              u8
//!       [20..24]  birth_delta        u32 BE (v4; see above)
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
//! # Worst-case sizes for a 128-entry leaf (v5, 24 B/unit, 42 B header)
//!
//! - empty leaf:    42 +    0 +  896 =  938 B
//! - 1 unit:        42 +   24 +  896 =  962 B
//! - 8 units:       42 +  192 +  896 = 1130 B
//! - 32 units:      42 +  768 +  896 = 1706 B
//! - 64 units:      42 + 1536 +  896 = 2474 B
//! - 100 units:     42 + 2400 +  896 = 3338 B
//! - 128 units:     42 + 3072 +  896 = 4010 B  (≤ 4032 — payload cap; 22 B headroom)
//!
//! `MAX_UNITS_PER_LEAF = 128 = LEAF_ENTRY_COUNT`. The pathological
//! "every slot a unique unit" case fits exactly (same headroom v3 had
//! pre-). Workloads that legitimately span > 4 G PBA blocks
//! within a single 128-LBA leaf range will trip the v5 rebase fallback
//! in `compact_in_place_full` (mirrors birth_delta's > 4 G LSN edge);
//! the continuation-page overflow mechanism remains the long-term
//! mitigation for that case.

use crate::error::{MetaDbError, Result};
use crate::paged::format::{LEAF_BITMAP_BYTES, LEAF_ENTRY_COUNT, LEAF_VALUE_SIZE};

/// Format version stored at offset 17 of the compact payload. Future
/// schema changes bump this; Day 1 readers reject unknown versions and
/// surface zeros so a stray on-disk byte cannot pose as a valid value.
///
/// v1: pre-seq layout (28 B values, 11 B per-slot record, 100 unit cap).
/// v2: per-slot u64 seq (36 B values, 11 B per-slot record, 100 unit cap).
/// v3: per-leaf base_seq + u32 delta, drop on-disk lba_count, 128 unit cap.
/// v4: per-leaf base_birth_lsn + per-unit u32 birth_delta (28 B unit), 110 unit cap.
/// v5: per-leaf base_pba + per-unit u32 pba_delta (24 B unit), 128 unit cap.
pub const COMPACT_VERSION: u8 = 5;

/// Sentinel `seq_delta` value encoding "no seq guard" (full_seq=0).
pub const SEQ_DELTA_NO_GUARD: u32 = u32::MAX;

/// Sentinel `birth_delta` value encoding "no birth recorded"
/// (full_birth_lsn=0). Used by legacy / scanner direct-insert paths
/// that don't carry a birth_lsn. Mirrors `SEQ_DELTA_NO_GUARD`'s shape.
pub const BIRTH_DELTA_NO_RECORD: u32 = u32::MAX;

/// Sentinel `pba_delta` value for the transient "unit not yet placed
/// against a leaf base" state used by `UnitMeta::write_to` when it
/// runs without a base (e.g. during a `compact_in_place` rebase pass
/// before the new base is committed to the header). Live on-disk
/// units always carry a real delta. Mirrors `BIRTH_DELTA_NO_RECORD`'s
/// shape so the encode/decode patterns stay uniform.
pub const PBA_DELTA_NO_RECORD: u32 = u32::MAX;

/// Onyx 4 KiB block size used to recover `unit_lba_count` from
/// `unit_original_size`. Must match
/// `onyx_storage::types::BLOCK_SIZE`.
pub const BLOCK_SIZE_4K: u32 = 4096;

/// Fixed-size fields at the head of a compact payload. Always present,
/// even for an empty leaf.
/// Layout (v5): `[bitmap 16 | unit_count 1 | version 1 | base_seq 8 | base_birth_lsn 8 | base_pba 8]`.
pub const COMPACT_HEADER_BYTES: usize = LEAF_BITMAP_BYTES + 1 + 1 + 8 + 8 + 8; // 42

/// Byte offset of the leaf-local `base_seq` field within the header.
pub const COMPACT_BASE_SEQ_OFFSET: usize = LEAF_BITMAP_BYTES + 2;

/// Byte offset of the leaf-local `base_birth_lsn` field within the header.
pub const COMPACT_BASE_BIRTH_OFFSET: usize = COMPACT_BASE_SEQ_OFFSET + 8;

/// Byte offset of the leaf-local `base_pba` field within the header (v5).
pub const COMPACT_BASE_PBA_OFFSET: usize = COMPACT_BASE_BIRTH_OFFSET + 8;

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

/// Size of one unit-dict entry on disk (v5).
/// Layout:
///   `[pba_delta 4 | comp_sz 4 | orig_sz 4 | slot_off 2 | crc32 4 | comp 1 | flags 1 | birth_delta 4]`.
/// (`unit_lba_count` is derived from `unit_original_size` on decode.
/// `base_pba = header.base_pba + pba_delta` is the full per-unit PBA.)
pub const COMPACT_UNIT_BYTES: usize = 24;

/// Footprint of the legacy dense format (16 B bitmap + 128 × 44 B
/// values). Kept as a reference baseline for benches and CR reporting;
/// the project has no in-service dense leaves, so encode never falls
/// back to it.
pub const DENSE_FOOTPRINT_BYTES: usize = LEAF_BITMAP_BYTES + LEAF_ENTRY_COUNT * LEAF_VALUE_SIZE;

/// Maximum number of distinct units a single leaf can reference. With
/// v5 schema (header grew another 8 B for base_pba, unit dict shrank
/// 4 B because `base_pba` is now a u32 delta against the header base)
/// the worst-case footprint is `42 + 896 + N×24` which fits 4032 B up
/// to **N = 128 = LEAF_ENTRY_COUNT** (`42 + 896 + 128×24 = 4010 B`,
/// 22 B headroom — same headroom v4 had at N = 110). The pathological
/// "every slot a unique unit" case (synthetic `--refill_buffers` /
/// 1-LBA-per-unit) now fits exactly. Workloads that legitimately span
/// > 4 G PBA blocks within a single leaf trip the rebase fallback in
/// `compact_in_place_full` and surface Corruption (mirrors
/// birth_delta's > 4 G LSN edge case).
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

/// Per-unit shared metadata extracted from a 44 B `L2pValue`. Two
/// values share a unit iff every field here is byte-identical. `crc32`
/// belongs here too — it's a unit-level checksum, not per-LBA.
/// `birth_lsn` is also unit-scoped: it's a property of the underlying
/// PBA, and two writes that produce the same compressed unit at the
/// same PBA share the same first-write LSN.
///
/// `unit_lba_count` from the BlockmapValue is **not** stored on disk;
/// it's recovered on read as `unit_original_size / BLOCK_SIZE_4K`.
/// Onyx writers always set `unit_original_size = lba_count * 4096`, so
/// the round trip is exact (asserted in `from_value`).
///
/// `birth_lsn` is carried as the full u64 in memory but encoded on disk
/// as a u32 delta against the leaf-local `base_birth_lsn` (mirrors the
/// per-entry seq delta scheme). `0` is the "not yet stamped" sentinel
/// and is preserved across encode/decode (stored as
/// [`BIRTH_DELTA_NO_RECORD`]).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct UnitMeta {
    pub(crate) base_pba: u64,
    pub(crate) unit_compressed_size: u32,
    pub(crate) unit_original_size: u32,
    pub(crate) slot_offset: u16,
    pub(crate) crc32: u32,
    pub(crate) compression: u8,
    pub(crate) flags: u8,
    pub(crate) birth_lsn: u64,
}

impl UnitMeta {
    /// Decompose a 44 B `L2pValue` into (unit-shared, per-entry-varying)
    /// parts. The first 28 B match
    /// `onyx_storage::meta::schema::encode_blockmap_value`; the next
    /// 8 B are the big-endian u64 commit-seq, and the last 8 B are the
    /// big-endian u64 `birth_lsn`.
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
            birth_lsn: u64::from_be_bytes(v[36..44].try_into().unwrap()),
        };
        // Onyx invariant: unit_original_size = unit_lba_count * 4096
        // for ordinary mappings. The on-disk dict drops lba_count, so
        // we rely on this to reconstruct it on decode. If a future
        // writer breaks the invariant, decode will return a wrong
        // lba_count for the unaffected callers; debug builds catch it
        // here. Zero mappings (flags & 0x02 == FLAG_ZERO) are exempt:
        // they carry `unit_lba_count = 1` and `unit_original_size = 0`
        // by construction (BlockmapValue::zero()), since the LBA reads
        // as zero with no physical backing.
        debug_assert!(
            (unit.flags & 0x02) != 0
                || u16::from_be_bytes(v[17..19].try_into().unwrap()) as u32 * BLOCK_SIZE_4K
                    == unit.unit_original_size,
            "v5 compact encoding requires unit_original_size == lba_count * 4096 (unless FLAG_ZERO)"
        );
        let entry = EntryDelta {
            offset_in_unit: u16::from_be_bytes(v[19..21].try_into().unwrap()),
            seq: u64::from_be_bytes(v[28..36].try_into().unwrap()),
        };
        (unit, entry)
    }

    /// Encode unit fields (v5). The 4 B `pba_delta` head and 4 B
    /// `birth_delta` tail are filled by [`try_write_pba_delta`] /
    /// [`try_write_birth_delta`] which know the leaf-local bases; pure
    /// `write_to` writes both sentinels ("not yet placed against a
    /// base") so it's safe to call standalone (e.g. in the
    /// `compact_in_place` rebase loop before the new bases are
    /// updated).
    #[inline]
    fn write_to(&self, out: &mut [u8; COMPACT_UNIT_BYTES]) {
        out[0..4].copy_from_slice(&PBA_DELTA_NO_RECORD.to_be_bytes());
        out[4..8].copy_from_slice(&self.unit_compressed_size.to_be_bytes());
        out[8..12].copy_from_slice(&self.unit_original_size.to_be_bytes());
        out[12..14].copy_from_slice(&self.slot_offset.to_be_bytes());
        out[14..18].copy_from_slice(&self.crc32.to_be_bytes());
        out[18] = self.compression;
        out[19] = self.flags;
        out[20..24].copy_from_slice(&BIRTH_DELTA_NO_RECORD.to_be_bytes());
    }

    /// Encode the per-unit `pba_delta` against `base_pba` (v5). Returns
    /// `NeedsRebase` if the delta would overflow `u32::MAX` (or if
    /// `base_pba < header.base_pba`); callers must run
    /// [`compact_in_place_with_incoming_pba`] and retry. Mirrors
    /// [`try_write_birth_delta`] but signals "first unit, adopt
    /// incoming as the base" via the explicit `has_base` flag — unlike
    /// `birth_lsn`, PBA = 0 is a valid allocator output and cannot
    /// double as a sentinel.
    #[inline]
    fn try_write_pba_delta(
        &self,
        base_pba: u64,
        has_base: bool,
        out: &mut [u8; COMPACT_UNIT_BYTES],
    ) -> WritePbaOutcome {
        let (delta, outcome) = if !has_base {
            (
                0u32,
                WritePbaOutcome::AdoptedBase {
                    new_base: self.base_pba,
                },
            )
        } else if self.base_pba < base_pba {
            return WritePbaOutcome::NeedsRebase;
        } else {
            let diff = self.base_pba - base_pba;
            if diff >= PBA_DELTA_NO_RECORD as u64 {
                return WritePbaOutcome::NeedsRebase;
            }
            (diff as u32, WritePbaOutcome::Written)
        };
        out[0..4].copy_from_slice(&delta.to_be_bytes());
        outcome
    }

    /// Encode the per-unit `birth_delta` against `base_birth_lsn`.
    /// Returns `NeedsRebase` if the delta would overflow `u32::MAX`
    /// (or if `birth_lsn < base_birth_lsn`); callers must
    /// [`compact_in_place_with_incoming_birth`] and retry.
    #[inline]
    fn try_write_birth_delta(
        &self,
        base_birth_lsn: u64,
        out: &mut [u8; COMPACT_UNIT_BYTES],
    ) -> WriteBirthOutcome {
        let (delta, outcome) = if self.birth_lsn == 0 {
            (BIRTH_DELTA_NO_RECORD, WriteBirthOutcome::Written)
        } else if base_birth_lsn == 0 {
            (
                0u32,
                WriteBirthOutcome::AdoptedBase {
                    new_base: self.birth_lsn,
                },
            )
        } else if self.birth_lsn < base_birth_lsn {
            return WriteBirthOutcome::NeedsRebase;
        } else {
            let diff = self.birth_lsn - base_birth_lsn;
            if diff >= BIRTH_DELTA_NO_RECORD as u64 {
                return WriteBirthOutcome::NeedsRebase;
            }
            (diff as u32, WriteBirthOutcome::Written)
        };
        out[20..24].copy_from_slice(&delta.to_be_bytes());
        outcome
    }

    /// Decode the 24 B unit dict entry (v5), reconstructing the full
    /// `base_pba` from `header.base_pba` + `pba_delta` and the full
    /// `birth_lsn` from `header.base_birth_lsn` + `birth_delta`.
    #[inline]
    fn read_from(buf: &[u8], base_birth_lsn: u64, base_pba: u64) -> Self {
        debug_assert!(buf.len() >= COMPACT_UNIT_BYTES);
        let pba_delta = u32::from_be_bytes(buf[0..4].try_into().unwrap());
        // PBA_DELTA_NO_RECORD is a transient sentinel only ever set by
        // `write_to` while a unit is staged before
        // `try_write_pba_delta` commits the real delta. On a live page
        // it shouldn't appear; if it does (stale page after a crashed
        // rebase, fault injection) decode it as `base_pba` (delta 0)
        // so the value still round-trips through tests. The
        // `payload_decode_at_checked` path independently verifies the
        // unit_count / bitmap invariants.
        let resolved_pba = if pba_delta == PBA_DELTA_NO_RECORD {
            base_pba
        } else {
            base_pba.wrapping_add(pba_delta as u64)
        };
        let birth_delta = u32::from_be_bytes(buf[20..24].try_into().unwrap());
        let birth_lsn = if birth_delta == BIRTH_DELTA_NO_RECORD {
            0
        } else {
            base_birth_lsn.wrapping_add(birth_delta as u64)
        };
        UnitMeta {
            base_pba: resolved_pba,
            unit_compressed_size: u32::from_be_bytes(buf[4..8].try_into().unwrap()),
            unit_original_size: u32::from_be_bytes(buf[8..12].try_into().unwrap()),
            slot_offset: u16::from_be_bytes(buf[12..14].try_into().unwrap()),
            crc32: u32::from_be_bytes(buf[14..18].try_into().unwrap()),
            compression: buf[18],
            flags: buf[19],
            birth_lsn,
        }
    }

    /// Reconstruct `unit_lba_count` from `unit_original_size`. Used by
    /// `compose` to write back into the L2pValue bytes 17..19.
    #[inline]
    fn lba_count(&self) -> u16 {
        // Onyx writers always set original_size = lba_count * 4096;
        // round up just in case (a non-4K-aligned size shouldn't occur
        // in production but we don't want to truncate silently).
        self.unit_original_size
            .div_ceil(BLOCK_SIZE_4K)
            .min(u16::MAX as u32) as u16
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

/// Outcome of a unit-dict `birth_delta` encode attempt. Mirrors
/// [`WriteEntryOutcome`] but for the per-unit `birth_lsn` field.
#[derive(Debug, Clone, Copy)]
pub(crate) enum WriteBirthOutcome {
    /// Delta written successfully under the existing `base_birth_lsn`.
    Written,
    /// `base_birth_lsn` was 0; this unit's `birth_lsn` became the new
    /// base. Caller must persist `new_base` to the leaf header.
    AdoptedBase { new_base: u64 },
    /// Delta would overflow `u32::MAX` (or `birth_lsn < base_birth_lsn`).
    /// Caller must run [`compact_in_place_with_incoming_birth`] and
    /// retry.
    NeedsRebase,
}

/// Outcome of a unit-dict `pba_delta` encode attempt (v5). Mirrors
/// [`WriteBirthOutcome`] but for the per-unit `base_pba` field.
#[derive(Debug, Clone, Copy)]
pub(crate) enum WritePbaOutcome {
    /// Delta written successfully under the existing header `base_pba`.
    Written,
    /// Header `base_pba` was 0; this unit's `base_pba` became the new
    /// base. Caller must persist `new_base` to the leaf header.
    AdoptedBase { new_base: u64 },
    /// Delta would overflow `u32::MAX` (or `base_pba < header.base_pba`).
    /// Caller must run [`compact_in_place_with_incoming_pba`] and
    /// retry.
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

/// Reassemble a 44 B `L2pValue` from its unit + entry parts.
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
    v[36..44].copy_from_slice(&unit.birth_lsn.to_be_bytes());
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

/// Read the leaf-local `base_birth_lsn` from the header. Zero means
/// "no unit with a non-sentinel birth_lsn has been written yet".
#[inline]
pub(crate) fn read_base_birth(payload: &[u8]) -> u64 {
    u64::from_be_bytes(
        payload[COMPACT_BASE_BIRTH_OFFSET..COMPACT_BASE_BIRTH_OFFSET + 8]
            .try_into()
            .unwrap(),
    )
}

/// Write the leaf-local `base_birth_lsn` into the header. Callers
/// update this when a unit adopts a new base or
/// [`compact_in_place_with_incoming_birth`] rebases.
#[inline]
pub(crate) fn write_base_birth(payload: &mut [u8], base_birth_lsn: u64) {
    payload[COMPACT_BASE_BIRTH_OFFSET..COMPACT_BASE_BIRTH_OFFSET + 8]
        .copy_from_slice(&base_birth_lsn.to_be_bytes());
}

/// Read the leaf-local `base_pba` from the header (v5). Zero means
/// "no unit has been written yet"; the first unit insert adopts its
/// PBA as the new base.
#[inline]
pub(crate) fn read_base_pba(payload: &[u8]) -> u64 {
    u64::from_be_bytes(
        payload[COMPACT_BASE_PBA_OFFSET..COMPACT_BASE_PBA_OFFSET + 8]
            .try_into()
            .unwrap(),
    )
}

/// Write the leaf-local `base_pba` into the header (v5). Callers
/// update this when a unit adopts a new base or
/// [`compact_in_place_with_incoming_pba`] rebases.
#[inline]
pub(crate) fn write_base_pba(payload: &mut [u8], base_pba: u64) {
    payload[COMPACT_BASE_PBA_OFFSET..COMPACT_BASE_PBA_OFFSET + 8]
        .copy_from_slice(&base_pba.to_be_bytes());
}

/// Read the unit-dict entry at `idx` (v5). Caller must ensure
/// `idx < read_unit_count(payload)`. The `birth_lsn` field is
/// reconstructed from the leaf's `base_birth_lsn` + `birth_delta`;
/// the `base_pba` is reconstructed from the leaf's `base_pba` +
/// `pba_delta`.
#[inline]
pub(crate) fn read_unit(payload: &[u8], idx: usize) -> UnitMeta {
    let off = unit_offset(idx);
    let base_birth_lsn = read_base_birth(payload);
    let base_pba = read_base_pba(payload);
    UnitMeta::read_from(
        &payload[off..off + COMPACT_UNIT_BYTES],
        base_birth_lsn,
        base_pba,
    )
}

/// Outcome of `try_write_unit` (v5). Tracks which of the two delta
/// encodings (`pba_delta` or `birth_delta`) decided the operation's
/// fate. Callers driving the rebase loop in `find_or_append_unit`
/// dispatch the corresponding `compact_in_place_with_incoming_*` /
/// `compact_in_place_full` variant based on this discriminant.
#[derive(Debug, Clone, Copy)]
pub(crate) enum WriteUnitOutcome {
    /// Both deltas fit; the unit is fully written.
    Written,
    /// Header bases were updated as a side effect (one or both of
    /// `base_pba` / `base_birth_lsn` adopted the new unit's values).
    /// Caller has nothing more to do; the unit is fully written.
    AdoptedBases,
    /// `pba_delta` overflowed (or `base_pba < header.base_pba`). Caller
    /// must run [`compact_in_place_with_incoming_pba`] / pass the
    /// incoming pba through [`compact_in_place_full`] and retry.
    NeedsPbaRebase,
    /// `birth_delta` overflowed (or `birth_lsn < header.base_birth_lsn`).
    /// Caller must run [`compact_in_place_with_incoming_birth`] / pass
    /// the incoming birth through [`compact_in_place_full`] and retry.
    NeedsBirthRebase,
}

/// Write the unit-dict entry at `idx` (v5). Encodes `pba_delta`
/// against the current `base_pba` and `birth_delta` against the
/// current `base_birth_lsn`; the caller is responsible for adopting /
/// rebasing if the outcome is `NeedsPbaRebase` or `NeedsBirthRebase`.
/// Most callers should use [`write_unit`] which handles the base
/// adoption automatically.
///
/// On `NeedsPbaRebase` the unit's static bytes (compressed_size,
/// original_size, etc.) are written to disk with `PBA_DELTA_NO_RECORD`
/// as the sentinel head; on `NeedsBirthRebase` everything except
/// birth_delta is committed and the tail carries
/// `BIRTH_DELTA_NO_RECORD`. Callers that get a rebase outcome must
/// either retry-write after rebasing or treat the slot as transient.
#[inline]
pub(crate) fn try_write_unit(payload: &mut [u8], idx: usize, u: &UnitMeta) -> WriteUnitOutcome {
    let off = unit_offset(idx);
    let base_birth_lsn = read_base_birth(payload);
    let base_pba = read_base_pba(payload);
    // `has_base` distinguishes "header.base_pba is meaningful" from
    // "leaf is empty / first unit will adopt". We use unit_count > 0
    // as the signal since PBA = 0 is a valid allocator output and
    // can't double as an in-band sentinel.
    let has_base = read_unit_count(payload) > 0;
    let dst: &mut [u8; COMPACT_UNIT_BYTES] = (&mut payload[off..off + COMPACT_UNIT_BYTES])
        .try_into()
        .unwrap();
    u.write_to(dst);
    let pba_outcome = u.try_write_pba_delta(base_pba, has_base, dst);
    let birth_outcome = u.try_write_birth_delta(base_birth_lsn, dst);

    let pba_adopt = match pba_outcome {
        WritePbaOutcome::NeedsRebase => return WriteUnitOutcome::NeedsPbaRebase,
        WritePbaOutcome::AdoptedBase { new_base } => Some(new_base),
        WritePbaOutcome::Written => None,
    };
    let birth_adopt = match birth_outcome {
        WriteBirthOutcome::NeedsRebase => return WriteUnitOutcome::NeedsBirthRebase,
        WriteBirthOutcome::AdoptedBase { new_base } => Some(new_base),
        WriteBirthOutcome::Written => None,
    };

    let mut adopted = false;
    if let Some(b) = pba_adopt {
        write_base_pba(payload, b);
        adopted = true;
    }
    if let Some(b) = birth_adopt {
        write_base_birth(payload, b);
        adopted = true;
    }
    if adopted {
        WriteUnitOutcome::AdoptedBases
    } else {
        WriteUnitOutcome::Written
    }
}

/// Write a unit, transparently adopting bases if the header's
/// `base_pba` / `base_birth_lsn` were zero. Panics if a rebase is
/// required (caller should run
/// [`compact_in_place_with_incoming_birth`] /
/// [`compact_in_place_with_incoming_pba`] first when this can happen).
#[inline]
pub(crate) fn write_unit(payload: &mut [u8], idx: usize, u: &UnitMeta) {
    match try_write_unit(payload, idx, u) {
        WriteUnitOutcome::Written | WriteUnitOutcome::AdoptedBases => {}
        WriteUnitOutcome::NeedsPbaRebase => {
            panic!(
                "compact leaf write_unit idx={idx} base_pba={} requires rebase \
                 (header.base_pba={}); caller must use try_write_unit + \
                 compact_in_place_with_incoming_pba loop instead",
                u.base_pba,
                read_base_pba(payload)
            );
        }
        WriteUnitOutcome::NeedsBirthRebase => {
            panic!(
                "compact leaf write_unit idx={idx} birth_lsn={} requires rebase \
                 (base_birth_lsn={}); caller must use try_write_unit + \
                 compact_in_place_with_incoming_birth loop instead",
                u.birth_lsn,
                read_base_birth(payload)
            );
        }
    }
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

/// Decompose a 44 B value into (UnitMeta, EntryDelta). Re-export of
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
/// a HashMap; with the worst case of 128 units it's still ~3 KiB
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
    // Probe both delta encodings before mutating the dict; if either
    // would need a rebase, return `None` so the caller can run
    // `compact_in_place_full(payload, 0, target.birth_lsn,
    // target.base_pba)` (which folds both incoming bases into one
    // pass) and retry. Otherwise commit the write and bump unit_count.
    let base_birth = read_base_birth(payload);
    if needs_birth_rebase(base_birth, target.birth_lsn) {
        return None;
    }
    let base_pba = read_base_pba(payload);
    if needs_pba_rebase(base_pba, count > 0, target.base_pba) {
        return None;
    }
    write_unit(payload, count, target);
    write_unit_count(payload, (count + 1) as u8);
    Some(count as u8)
}

/// True iff encoding `incoming_birth` against `base_birth_lsn` would
/// overflow `u32::MAX` or underflow (incoming below base). Used by
/// `find_or_append_unit` to decide whether to bail out for a rebase
/// before mutating the dict.
#[inline]
pub(crate) fn needs_birth_rebase(base_birth_lsn: u64, incoming_birth: u64) -> bool {
    if incoming_birth == 0 {
        return false;
    }
    if base_birth_lsn == 0 {
        return false;
    }
    if incoming_birth < base_birth_lsn {
        return true;
    }
    let diff = incoming_birth - base_birth_lsn;
    diff >= BIRTH_DELTA_NO_RECORD as u64
}

/// True iff encoding `incoming_pba` against `base_pba` would overflow
/// `u32::MAX` or underflow (incoming below base). Used by
/// `find_or_append_unit` to decide whether to bail out for a rebase
/// before mutating the dict. Mirrors [`needs_birth_rebase`] but takes
/// an explicit `has_base` flag instead of treating `base_pba == 0` as
/// "no base set" — PBA = 0 is a valid allocator output and cannot
/// double as a sentinel. Callers should pass `has_base = unit_count
/// > 0`.
#[inline]
pub(crate) fn needs_pba_rebase(base_pba: u64, has_base: bool, incoming_pba: u64) -> bool {
    if !has_base {
        return false;
    }
    if incoming_pba < base_pba {
        return true;
    }
    let diff = incoming_pba - base_pba;
    diff >= PBA_DELTA_NO_RECORD as u64
}

/// Maximum unit-dict capacity given the payload size. With v4's 4032 B
/// payload that's `(4032 - 930) / 28 = 110`, matching the
/// `MAX_UNITS_PER_LEAF = 110` cap exactly.
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
/// rebases `base_seq` to `min(live full_seq)`, `base_birth_lsn` to
/// `min(live unit birth_lsn)`, and `base_pba` to `min(live unit
/// base_pba)` so subsequent inserts have the maximum possible delta
/// headroom.
///
/// See [`compact_in_place_with_incoming`] /
/// [`compact_in_place_with_incoming_birth`] /
/// [`compact_in_place_with_incoming_pba`] / [`compact_in_place_full`]
/// for variants that also account for a not-yet-written entry's or
/// unit's seq/birth/pba during rebase.
pub(crate) fn compact_in_place(payload: &mut [u8]) -> Result<()> {
    compact_in_place_full(payload, 0, 0, None)
}

/// Like [`compact_in_place`] but also folds `incoming_seq` (the seq
/// of an entry about to be written by the caller) into the
/// `new_base_seq = min(...)` computation. Equivalent to
/// `compact_in_place_full(payload, incoming_seq, 0, None)`.
pub(crate) fn compact_in_place_with_incoming(payload: &mut [u8], incoming_seq: u64) -> Result<()> {
    compact_in_place_full(payload, incoming_seq, 0, None)
}

/// Like [`compact_in_place`] but also folds `incoming_birth` (the
/// birth_lsn of a unit about to be appended by the caller) into the
/// `new_base_birth_lsn = min(...)` computation. Equivalent to
/// `compact_in_place_full(payload, 0, incoming_birth, None)`.
pub(crate) fn compact_in_place_with_incoming_birth(
    payload: &mut [u8],
    incoming_birth: u64,
) -> Result<()> {
    compact_in_place_full(payload, 0, incoming_birth, None)
}

/// Like [`compact_in_place`] but also folds `incoming_pba` (the
/// `base_pba` of a unit about to be appended by the caller) into the
/// `new_base_pba = min(...)` computation. Equivalent to
/// `compact_in_place_full(payload, 0, 0, Some(incoming_pba))`.
pub(crate) fn compact_in_place_with_incoming_pba(
    payload: &mut [u8],
    incoming_pba: u64,
) -> Result<()> {
    compact_in_place_full(payload, 0, 0, Some(incoming_pba))
}

/// Algorithm:
/// 1. Compute `new_base_seq = min(live full_seq among non-sentinel
///    entries, incoming_seq if non-zero)`, `new_base_birth_lsn =
///    min(live unit birth_lsn among non-sentinel units, incoming_birth
///    if non-zero)`, and `new_base_pba = min(live unit base_pba,
///    incoming_pba if non-zero)`. If no candidate values exist, the
///    corresponding base resets to 0.
/// 2. Scan all 128 entry slots. For each set slot, read its old
///    `unit_idx` and full seq. Build `old → new` index translation,
///    allocating a new index the first time we see each old index.
///    Capture each live unit's full birth_lsn and base_pba from the
///    OLD bases.
/// 3. Re-emit live units to a scratch Vec (preserving discovery order),
///    each with its full (already-decoded) `birth_lsn` and `base_pba`.
/// 4. Write the new `base_seq`, `base_birth_lsn`, and `base_pba` into
///    the header.
/// 5. Re-emit live entries under the new base_seq.
/// 6. Re-emit live units under the new bases.
/// 7. Zero out the tail bytes between the new and old `unit_count`.
/// 8. Write `new_unit_count`.
pub(crate) fn compact_in_place_full(
    payload: &mut [u8],
    incoming_seq: u64,
    incoming_birth: u64,
    incoming_pba: Option<u64>,
) -> Result<()> {
    let old_count = read_unit_count(payload) as usize;
    let cap = max_units_per_payload(payload.len()).min(MAX_UNITS_PER_LEAF);
    if old_count > cap {
        return Err(MetaDbError::Corruption(format!(
            "compact leaf unit_count {old_count} exceeds payload capacity {cap}"
        )));
    }
    if old_count == 0 {
        // No units yet — but bases may still be nonzero from a prior
        // population that's been entirely cleared. Reset all three so
        // the next insert can adopt cleanly.
        write_base_seq(payload, 0);
        write_base_birth(payload, 0);
        write_base_pba(payload, 0);
        return Ok(());
    }

    // Pass 1: collect live entries with their full seq under the
    // CURRENT base. We read entries BEFORE rewriting the base.
    let mut live: Vec<(usize, usize, EntryDelta)> = Vec::with_capacity(LEAF_ENTRY_COUNT);
    let mut new_base_seq: Option<u64> = None;
    if incoming_seq != 0 {
        new_base_seq = Some(incoming_seq);
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
            new_base_seq = Some(match new_base_seq {
                Some(b) => b.min(entry.seq),
                None => entry.seq,
            });
        }
        live.push((slot, old_idx, entry));
    }

    // Pass 2: discovery-ordered remap from old unit_idx → new unit_idx.
    // Captures each live unit with its full birth_lsn / base_pba under
    // the CURRENT header bases — must be read before rewriting them.
    let mut remap: [Option<u8>; 256] = [None; 256];
    let mut live_units: Vec<UnitMeta> = Vec::with_capacity(old_count);
    let mut new_base_birth: Option<u64> = None;
    if incoming_birth != 0 {
        new_base_birth = Some(incoming_birth);
    }
    // PBA fold: unlike birth/seq, 0 IS a valid PBA, so we fold every
    // live unit's `base_pba` into the min unconditionally. `Option`
    // discriminates "no candidate" (no live units AND no incoming)
    // from "min is 0".
    let mut new_base_pba: Option<u64> = incoming_pba;
    let live_clone = live.clone();
    for (_, old_idx, _) in &live_clone {
        if remap[*old_idx].is_some() {
            continue;
        }
        let unit = read_unit(payload, *old_idx);
        if unit.birth_lsn != 0 {
            new_base_birth = Some(match new_base_birth {
                Some(b) => b.min(unit.birth_lsn),
                None => unit.birth_lsn,
            });
        }
        new_base_pba = Some(match new_base_pba {
            Some(b) => b.min(unit.base_pba),
            None => unit.base_pba,
        });
        let new_idx = live_units.len() as u8;
        remap[*old_idx] = Some(new_idx);
        live_units.push(unit);
    }

    // Write the new bases BEFORE rewriting entries/units, since
    // try_write_entry / try_write_unit reads them via the payload
    // header.
    let resolved_base_seq = new_base_seq.unwrap_or(0);
    let resolved_base_birth = new_base_birth.unwrap_or(0);
    let resolved_base_pba = new_base_pba.unwrap_or(0);
    write_base_seq(payload, resolved_base_seq);
    write_base_birth(payload, resolved_base_birth);
    write_base_pba(payload, resolved_base_pba);

    // Pass 3: re-emit live entries under the new base_seq.
    for (slot, old_idx, entry) in &live {
        let new_idx = remap[*old_idx].expect("remap allocated in pass 2");
        match try_write_entry(payload, *slot, new_idx, entry) {
            WriteEntryOutcome::Written => {}
            WriteEntryOutcome::AdoptedBase { new_base } => {
                write_base_seq(payload, new_base);
            }
            WriteEntryOutcome::NeedsRebase => {
                return Err(MetaDbError::Corruption(format!(
                    "compact leaf seq_delta would overflow u32 even after rebase: \
                     slot={slot} seq={} base={}",
                    entry.seq, resolved_base_seq
                )));
            }
        }
    }

    // Pass 4: re-emit dict tightly, encoding each unit's pba_delta /
    // birth_delta against the new bases.
    for (i, u) in live_units.iter().enumerate() {
        match try_write_unit(payload, i, u) {
            WriteUnitOutcome::Written | WriteUnitOutcome::AdoptedBases => {}
            WriteUnitOutcome::NeedsPbaRebase => {
                return Err(MetaDbError::Corruption(format!(
                    "compact leaf pba_delta would overflow u32 even after rebase: \
                     unit_idx={i} base_pba={} base={}",
                    u.base_pba, resolved_base_pba
                )));
            }
            WriteUnitOutcome::NeedsBirthRebase => {
                return Err(MetaDbError::Corruption(format!(
                    "compact leaf birth_delta would overflow u32 even after rebase: \
                     unit_idx={i} birth_lsn={} base={}",
                    u.birth_lsn, resolved_base_birth
                )));
            }
        }
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
/// `bitmap` is the 16 B presence bitmap; `values` is the 128 × 44 B
/// value array (unset slots may be anything — they're skipped via the
/// bitmap). The output is always strictly smaller than the 4032 B leaf
/// payload for valid populations (≤ 128 distinct units), so encode
/// never fails on size grounds and returns a `Vec<u8>` directly.
pub fn encode(
    bitmap: &[u8; LEAF_BITMAP_BYTES],
    values: &[[u8; LEAF_VALUE_SIZE]; LEAF_ENTRY_COUNT],
) -> Vec<u8> {
    // Build the unit dict in a small scratch Vec; for the typical
    // 1..8 distinct units, `units.iter().position()` outperforms a
    // HashMap. We bound the dict to MAX_UNITS_PER_LEAF.
    let mut units: Vec<UnitMeta> = Vec::with_capacity(8);
    // Pre-size the output buffer to the v5 worst case (MAX_UNITS_PER_LEAF
    // distinct units = 4010 B). We'll truncate to the actual dict size.
    let mut out = vec![0u8; compact_size(MAX_UNITS_PER_LEAF)];
    out[..LEAF_BITMAP_BYTES].copy_from_slice(bitmap);
    out[LEAF_BITMAP_BYTES + 1] = COMPACT_VERSION;

    // Pass 1: decompose every set slot, compute base_seq = min(live
    // non-sentinel seqs), base_birth_lsn = min(live unit birth_lsns),
    // and base_pba = min(live unit base_pba) so encode produces the
    // same byte pattern as a leaf populated via the page-level
    // primitives + final compact_in_place.
    let mut entries: Vec<(usize, EntryDelta, usize /*unit_idx*/)> =
        Vec::with_capacity(LEAF_ENTRY_COUNT);
    let mut base_seq: Option<u64> = None;
    let mut base_birth: Option<u64> = None;
    let mut base_pba: Option<u64> = None;
    for slot in 0..LEAF_ENTRY_COUNT {
        if (bitmap[slot / 8] >> (slot % 8)) & 1 == 0 {
            continue;
        }
        let (unit, entry) = UnitMeta::from_value(&values[slot]);
        let unit_idx = match units.iter().position(|u| *u == unit) {
            Some(i) => i,
            None => {
                debug_assert!(units.len() < MAX_UNITS_PER_LEAF);
                if unit.birth_lsn != 0 {
                    base_birth = Some(match base_birth {
                        Some(b) => b.min(unit.birth_lsn),
                        None => unit.birth_lsn,
                    });
                }
                // PBA fold: 0 is a valid PBA, so fold every unit
                // unconditionally — `Option` discriminates "no live
                // units yet" from "min is 0".
                base_pba = Some(match base_pba {
                    Some(b) => b.min(unit.base_pba),
                    None => unit.base_pba,
                });
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
    let resolved_base_seq = base_seq.unwrap_or(0);
    let resolved_base_birth = base_birth.unwrap_or(0);
    let resolved_base_pba = base_pba.unwrap_or(0);
    out[COMPACT_BASE_SEQ_OFFSET..COMPACT_BASE_SEQ_OFFSET + 8]
        .copy_from_slice(&resolved_base_seq.to_be_bytes());
    out[COMPACT_BASE_BIRTH_OFFSET..COMPACT_BASE_BIRTH_OFFSET + 8]
        .copy_from_slice(&resolved_base_birth.to_be_bytes());
    out[COMPACT_BASE_PBA_OFFSET..COMPACT_BASE_PBA_OFFSET + 8]
        .copy_from_slice(&resolved_base_pba.to_be_bytes());

    for (slot, entry, unit_idx) in &entries {
        let off = entry_offset(*slot);
        let dst: &mut [u8; COMPACT_ENTRY_BYTES] = (&mut out[off..off + COMPACT_ENTRY_BYTES])
            .try_into()
            .unwrap();
        let outcome = entry.try_write_to(*unit_idx as u8, resolved_base_seq, dst);
        debug_assert!(
            !matches!(outcome, WriteEntryOutcome::NeedsRebase),
            "encode: seq_delta overflow at slot {slot} seq={} base={resolved_base_seq}",
            entry.seq
        );
    }

    out[LEAF_BITMAP_BYTES] = units.len() as u8;
    let has_pba_base = !units.is_empty();
    for (i, u) in units.iter().enumerate() {
        let off = unit_offset(i);
        let dst: &mut [u8; COMPACT_UNIT_BYTES] = (&mut out[off..off + COMPACT_UNIT_BYTES])
            .try_into()
            .unwrap();
        u.write_to(dst);
        // `has_pba_base` is true once any unit exists; resolved_base_pba
        // was just written to the header above. The first unit (i = 0)
        // sees has_base = true here because we already committed the
        // count + base, mirroring the post-compact state in
        // `compact_in_place_full`.
        let pba_outcome = u.try_write_pba_delta(resolved_base_pba, has_pba_base, dst);
        debug_assert!(
            !matches!(pba_outcome, WritePbaOutcome::NeedsRebase),
            "encode: pba_delta overflow at unit_idx {i} base_pba={} base={resolved_base_pba}",
            u.base_pba
        );
        let birth_outcome = u.try_write_birth_delta(resolved_base_birth, dst);
        debug_assert!(
            !matches!(birth_outcome, WriteBirthOutcome::NeedsRebase),
            "encode: birth_delta overflow at unit_idx {i} birth_lsn={} base={resolved_base_birth}",
            u.birth_lsn
        );
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
    let base_birth = u64::from_be_bytes(
        encoded[COMPACT_BASE_BIRTH_OFFSET..COMPACT_BASE_BIRTH_OFFSET + 8]
            .try_into()
            .unwrap(),
    );
    let base_pba = u64::from_be_bytes(
        encoded[COMPACT_BASE_PBA_OFFSET..COMPACT_BASE_PBA_OFFSET + 8]
            .try_into()
            .unwrap(),
    );
    let entry_off = entry_offset(slot);
    let (unit_idx, entry) = EntryDelta::read_from(
        &encoded[entry_off..entry_off + COMPACT_ENTRY_BYTES],
        base_seq,
    );
    if unit_idx as usize >= unit_count {
        return None;
    }
    let unit_off = unit_offset(unit_idx as usize);
    if encoded.len() < unit_off + COMPACT_UNIT_BYTES {
        return None;
    }
    let unit = UnitMeta::read_from(
        &encoded[unit_off..unit_off + COMPACT_UNIT_BYTES],
        base_birth,
        base_pba,
    );
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
    let base_birth = u64::from_be_bytes(
        encoded[COMPACT_BASE_BIRTH_OFFSET..COMPACT_BASE_BIRTH_OFFSET + 8]
            .try_into()
            .unwrap(),
    );
    let base_pba = u64::from_be_bytes(
        encoded[COMPACT_BASE_PBA_OFFSET..COMPACT_BASE_PBA_OFFSET + 8]
            .try_into()
            .unwrap(),
    );

    let mut units: Vec<UnitMeta> = Vec::with_capacity(unit_count);
    for i in 0..unit_count {
        let off = unit_offset(i);
        if encoded.len() < off + COMPACT_UNIT_BYTES {
            return out;
        }
        units.push(UnitMeta::read_from(
            &encoded[off..off + COMPACT_UNIT_BYTES],
            base_birth,
            base_pba,
        ));
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
mod tests;
