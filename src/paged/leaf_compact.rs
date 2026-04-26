//! Onyx-aware compact leaf encoding (v1).
//!
//! The legacy dense leaf format stored 128 × 28 B `BlockmapValue` records
//! back-to-back. Onyx's packer puts consecutive LBAs into the same
//! compression unit, so a single leaf typically references only 1..8
//! distinct units. That gives us a lot of redundancy to fold out.
//!
//! Per-unit shared fields (8 of 9 BlockmapValue fields — including
//! `crc32`, which is a unit-level checksum, see
//! `onyx_storage::buffer::flush::writer::passthrough` for the
//! construction site):
//!   pba, compression, unit_compressed_size, unit_original_size,
//!   unit_lba_count, slot_offset, crc32, flags
//!
//! Per-entry varying field (only 1 of 9):
//!   offset_in_unit
//!
//! # On-disk layout (within the 4032 B leaf payload)
//!
//! ```text
//!   [ 0.. 16]  bitmap          128 bits, LE within each byte
//!   [16.. 17]  unit_count      u8 (number of live entries in unit dict)
//!   [17.. 18]  format_version  u8 (= COMPACT_VERSION)
//!   [18..402]  entries         128 × 3 B (slot-indexed dense array)
//!     entry @ slot s lives at offset 18 + s*3
//!       [ 0.. 1]  unit_idx        u8 (index into unit dict)
//!       [ 1.. 3]  offset_in_unit  u16 BE
//!     Unset slots are zero (caller checks bitmap before reading).
//!   [402..402+26*N]  unit dict (N = unit_count entries × 26 B)
//!     per unit:
//!       [ 0.. 8]  base_pba           u64 BE
//!       [ 8..12]  unit_compressed_sz u32 BE
//!       [12..16]  unit_original_sz   u32 BE
//!       [16..18]  unit_lba_count     u16 BE
//!       [18..20]  slot_offset        u16 BE
//!       [20..24]  crc32              u32 BE
//!       [24..25]  compression        u8
//!       [25..26]  flags              u8
//! ```
//!
//! # Why slot-indexed (vs popcount-indexed) entries
//!
//! A popcount-indexed entries region is a few hundred B smaller for
//! sparse leaves but makes `leaf_set` / `leaf_clear` O(N) in the leaf
//! population because every mutation memmoves the entry array's tail.
//! With slot-indexed entries we trade 384 B of fixed-size headroom for
//! O(1) per-slot reads and writes that touch a fixed payload offset.
//! Onyx's L2P leaves are densely populated in steady state (sequential
//! writes; packer fills units), so the overhead only appears for
//! transient sparse states after random deletes. Steady-state size is
//! identical: 1-unit and 8-unit leaves both encode in 428 B and 610 B
//! respectively under either layout.
//!
//! # Worst-case sizes for a 128-entry leaf
//!
//! - empty leaf:    18 +   0 + 384 = 402 B   (vs popcount 18 B)
//! - 1 unit:        18 +  26 + 384 = 428 B   (8.41x vs dense 3600 B)
//! - 8 units:       18 + 208 + 384 = 610 B   (5.90x)
//! - 32 units:      18 + 832 + 384 = 1234 B  (2.92x)
//! - 64 units:      18 + 1664 + 384 = 2066 B (1.74x)
//! - 128 units:     18 + 3328 + 384 = 3730 B (0.97x — fits in 4032 B payload)
//!
//! Even the pathological all-distinct-units shape fits the payload, so
//! `encode` never fails on size grounds. Day 2 wires this directly into
//! `paged::format` (no dense fallback exists, the project has not shipped).

use crate::paged::format::{LEAF_BITMAP_BYTES, LEAF_ENTRY_COUNT, LEAF_VALUE_SIZE};

/// Format version stored at offset 17 of the compact payload. Future
/// schema changes bump this; Day 1 readers reject unknown versions and
/// surface zeros so a stray on-disk byte cannot pose as a valid value.
pub const COMPACT_VERSION: u8 = 1;

/// Fixed-size fields (bitmap + unit_count + version) at the head of a
/// compact payload. Always present, even for an empty leaf.
pub const COMPACT_HEADER_BYTES: usize = LEAF_BITMAP_BYTES + 1 + 1; // 18

/// Size of one per-entry slot-indexed record on disk.
pub const COMPACT_ENTRY_BYTES: usize = 3;

/// Byte footprint of the slot-indexed entries region (always present).
pub const COMPACT_ENTRIES_REGION_BYTES: usize = LEAF_ENTRY_COUNT * COMPACT_ENTRY_BYTES;

/// Byte offset (within the leaf payload) at which the entries region
/// begins.
pub const COMPACT_ENTRIES_OFFSET: usize = COMPACT_HEADER_BYTES;

/// Byte offset at which the unit dictionary begins.
pub const COMPACT_UNIT_DICT_OFFSET: usize = COMPACT_ENTRIES_OFFSET + COMPACT_ENTRIES_REGION_BYTES;

/// Size of one unit-dict entry on disk.
pub const COMPACT_UNIT_BYTES: usize = 26;

/// Footprint of the legacy dense format (16 B bitmap + 128 × 28 B
/// values). Kept as a reference baseline for benches and CR reporting;
/// the project has no in-service dense leaves, so encode never falls
/// back to it.
pub const DENSE_FOOTPRINT_BYTES: usize = LEAF_BITMAP_BYTES + LEAF_ENTRY_COUNT * LEAF_VALUE_SIZE;

/// Maximum number of distinct units a single leaf can reference. Limited
/// by the per-entry `unit_idx: u8` field; with at most 128 populated
/// slots, the cap is unreachable for the canonical encoder.
pub const MAX_UNITS_PER_LEAF: usize = 256;

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
/// entries region is fixed at 384 B regardless of population.
#[inline]
pub const fn compact_size(unit_count: usize) -> usize {
    COMPACT_UNIT_DICT_OFFSET + unit_count * COMPACT_UNIT_BYTES
}

/// Per-unit shared metadata extracted from a 28 B `BlockmapValue`. Two
/// values share a unit iff every field here is byte-identical. `crc32`
/// belongs here too — it's a unit-level checksum, not per-LBA.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct UnitMeta {
    pub(crate) base_pba: u64,
    pub(crate) unit_compressed_size: u32,
    pub(crate) unit_original_size: u32,
    pub(crate) unit_lba_count: u16,
    pub(crate) slot_offset: u16,
    pub(crate) crc32: u32,
    pub(crate) compression: u8,
    pub(crate) flags: u8,
}

impl UnitMeta {
    /// Decompose a 28 B `BlockmapValue` into (unit-shared, per-entry-varying)
    /// parts. Layout matches `onyx_storage::meta::schema::encode_blockmap_value`.
    #[inline]
    fn from_value(v: &[u8; LEAF_VALUE_SIZE]) -> (Self, EntryDelta) {
        let unit = UnitMeta {
            base_pba: u64::from_be_bytes(v[0..8].try_into().unwrap()),
            compression: v[8],
            unit_compressed_size: u32::from_be_bytes(v[9..13].try_into().unwrap()),
            unit_original_size: u32::from_be_bytes(v[13..17].try_into().unwrap()),
            unit_lba_count: u16::from_be_bytes(v[17..19].try_into().unwrap()),
            crc32: u32::from_be_bytes(v[21..25].try_into().unwrap()),
            slot_offset: u16::from_be_bytes(v[25..27].try_into().unwrap()),
            flags: v[27],
        };
        let entry = EntryDelta {
            offset_in_unit: u16::from_be_bytes(v[19..21].try_into().unwrap()),
        };
        (unit, entry)
    }

    #[inline]
    fn write_to(&self, out: &mut [u8; COMPACT_UNIT_BYTES]) {
        out[0..8].copy_from_slice(&self.base_pba.to_be_bytes());
        out[8..12].copy_from_slice(&self.unit_compressed_size.to_be_bytes());
        out[12..16].copy_from_slice(&self.unit_original_size.to_be_bytes());
        out[16..18].copy_from_slice(&self.unit_lba_count.to_be_bytes());
        out[18..20].copy_from_slice(&self.slot_offset.to_be_bytes());
        out[20..24].copy_from_slice(&self.crc32.to_be_bytes());
        out[24] = self.compression;
        out[25] = self.flags;
    }

    #[inline]
    fn read_from(buf: &[u8]) -> Self {
        debug_assert!(buf.len() >= COMPACT_UNIT_BYTES);
        UnitMeta {
            base_pba: u64::from_be_bytes(buf[0..8].try_into().unwrap()),
            unit_compressed_size: u32::from_be_bytes(buf[8..12].try_into().unwrap()),
            unit_original_size: u32::from_be_bytes(buf[12..16].try_into().unwrap()),
            unit_lba_count: u16::from_be_bytes(buf[16..18].try_into().unwrap()),
            slot_offset: u16::from_be_bytes(buf[18..20].try_into().unwrap()),
            crc32: u32::from_be_bytes(buf[20..24].try_into().unwrap()),
            compression: buf[24],
            flags: buf[25],
        }
    }
}

/// Per-entry delta over its unit. Only `offset_in_unit` differs across
/// LBAs in the same compression unit.
#[derive(Clone, Copy)]
pub(crate) struct EntryDelta {
    pub(crate) offset_in_unit: u16,
}

impl EntryDelta {
    #[inline]
    fn write_to(&self, unit_idx: u8, out: &mut [u8; COMPACT_ENTRY_BYTES]) {
        out[0] = unit_idx;
        out[1..3].copy_from_slice(&self.offset_in_unit.to_be_bytes());
    }

    #[inline]
    fn read_from(buf: &[u8]) -> (u8, Self) {
        debug_assert!(buf.len() >= COMPACT_ENTRY_BYTES);
        let unit_idx = buf[0];
        let offset_in_unit = u16::from_be_bytes(buf[1..3].try_into().unwrap());
        (unit_idx, EntryDelta { offset_in_unit })
    }
}

/// Reassemble a 28 B `BlockmapValue` from its unit + entry parts.
/// Inverse of `UnitMeta::from_value`.
#[inline]
fn compose(unit: &UnitMeta, entry: &EntryDelta) -> [u8; LEAF_VALUE_SIZE] {
    let mut v = [0u8; LEAF_VALUE_SIZE];
    v[0..8].copy_from_slice(&unit.base_pba.to_be_bytes());
    v[8] = unit.compression;
    v[9..13].copy_from_slice(&unit.unit_compressed_size.to_be_bytes());
    v[13..17].copy_from_slice(&unit.unit_original_size.to_be_bytes());
    v[17..19].copy_from_slice(&unit.unit_lba_count.to_be_bytes());
    v[19..21].copy_from_slice(&entry.offset_in_unit.to_be_bytes());
    v[21..25].copy_from_slice(&unit.crc32.to_be_bytes());
    v[25..27].copy_from_slice(&unit.slot_offset.to_be_bytes());
    v[27] = unit.flags;
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
/// bitmap, unit_count, entries region, and unit dictionary are
/// expected to already be zero (the caller's `init_leaf` zeroes the
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
/// by invariant but `(0, EntryDelta { offset_in_unit: 0 })` is
/// indistinguishable from a real entry pointing at unit 0 with offset 0.
#[inline]
pub(crate) fn read_entry(payload: &[u8], slot: usize) -> (u8, EntryDelta) {
    let off = entry_offset(slot);
    EntryDelta::read_from(&payload[off..off + COMPACT_ENTRY_BYTES])
}

#[inline]
pub(crate) fn write_entry(payload: &mut [u8], slot: usize, unit_idx: u8, e: &EntryDelta) {
    let off = entry_offset(slot);
    let dst: &mut [u8; COMPACT_ENTRY_BYTES] = (&mut payload[off..off + COMPACT_ENTRY_BYTES])
        .try_into()
        .unwrap();
    e.write_to(unit_idx, dst);
}

/// Zero the 3 B entry slot. Does not touch the bitmap.
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

/// Decompose a 28 B value into (UnitMeta, EntryDelta). Re-export of
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
/// a HashMap; with a worst-case 139 it's still ~3.6 KiB of sequential
/// reads, well within an L1 line.
pub(crate) fn find_or_append_unit(payload: &mut [u8], target: &UnitMeta) -> Option<u8> {
    let count = read_unit_count(payload) as usize;
    for i in 0..count {
        if read_unit(payload, i) == *target {
            return Some(i as u8);
        }
    }
    // No match — append. Check we can fit one more 26 B record before
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

/// Maximum unit-dict capacity given the payload size. With 4032 B
/// payload that's `(4032 - 402) / 26 = 139`.
#[inline]
pub const fn max_units_per_payload(payload_len: usize) -> usize {
    if payload_len <= COMPACT_UNIT_DICT_OFFSET {
        0
    } else {
        (payload_len - COMPACT_UNIT_DICT_OFFSET) / COMPACT_UNIT_BYTES
    }
}

/// Rebuild the unit dictionary in place, dropping unreferenced units
/// and renumbering `unit_idx` references in the entries region.
///
/// Called when:
/// - `find_or_append_unit` returns `None` because the dict is full.
/// - The caller wants to reclaim space from dead units (eg. a
///   periodic compaction pass).
///
/// Algorithm:
/// 1. Scan all 128 entry slots. For each set slot, read its old
///    `unit_idx`. Build `old → new` index translation, allocating a
///    new index the first time we see each old index.
/// 2. Re-emit live units to a scratch Vec (preserving discovery order).
/// 3. Rewrite live entries with their new `unit_idx` (offset_in_unit
///    is unchanged).
/// 4. Overwrite the unit dict with the live units and zero out the
///    tail bytes between the new and old `unit_count`.
pub(crate) fn compact_in_place(payload: &mut [u8]) {
    let old_count = read_unit_count(payload) as usize;
    if old_count == 0 {
        return;
    }
    // Discovery-ordered remap from old unit_idx → new unit_idx, plus
    // the cached UnitMeta we'll re-emit. `None` means "old unit was
    // unused / already remapped to nothing because no live entry
    // referenced it." 256-wide because the on-disk unit_idx is u8.
    let mut remap: [Option<u8>; MAX_UNITS_PER_LEAF] = [None; MAX_UNITS_PER_LEAF];
    let mut live_units: Vec<UnitMeta> = Vec::with_capacity(old_count);

    for slot in 0..LEAF_ENTRY_COUNT {
        if !payload_bit_set(payload, slot) {
            continue;
        }
        let (old_idx, entry) = read_entry(payload, slot);
        let old_idx = old_idx as usize;
        let new_idx = match remap[old_idx] {
            Some(i) => i,
            None => {
                // First time we see this old unit — copy it forward.
                debug_assert!(
                    old_idx < old_count,
                    "live entry references unit {old_idx} but dict has {old_count}"
                );
                let unit = read_unit(payload, old_idx);
                let new_idx = live_units.len() as u8;
                live_units.push(unit);
                remap[old_idx] = Some(new_idx);
                new_idx
            }
        };
        // Rewrite entry@slot with the new unit_idx (offset_in_unit unchanged).
        write_entry(payload, slot, new_idx, &entry);
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
}

/// Decode the value at slot `s` from a payload (no version check —
/// hot-path variant for in-tree readers that already trust the page).
/// Returns `[0u8; 28]` if the slot's bitmap bit is clear.
#[inline]
pub(crate) fn payload_decode_at(payload: &[u8], slot: usize) -> [u8; LEAF_VALUE_SIZE] {
    if !payload_bit_set(payload, slot) {
        return [0u8; LEAF_VALUE_SIZE];
    }
    let (unit_idx, entry) = read_entry(payload, slot);
    let unit = read_unit(payload, unit_idx as usize);
    compose(&unit, &entry)
}

// =====================================================================
// Vec-based encode/decode (used by full-leaf rebuilds, benches, tests)
// =====================================================================

/// Encode a dense `(bitmap, values)` leaf body into the compact form.
///
/// `bitmap` is the 16 B presence bitmap; `values` is the 128 × 28 B
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
    // Pre-size the output buffer; we'll patch the dict size after.
    // Worst case is 128 distinct units = 3730 B (still fits in payload).
    let mut out = vec![0u8; compact_size(LEAF_ENTRY_COUNT)];
    out[..LEAF_BITMAP_BYTES].copy_from_slice(bitmap);
    out[LEAF_BITMAP_BYTES + 1] = COMPACT_VERSION;

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
        let off = entry_offset(slot);
        let dst: &mut [u8; COMPACT_ENTRY_BYTES] = (&mut out[off..off + COMPACT_ENTRY_BYTES])
            .try_into()
            .unwrap();
        entry.write_to(unit_idx as u8, dst);
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
    let entry_off = entry_offset(slot);
    let (unit_idx, entry) =
        EntryDelta::read_from(&encoded[entry_off..entry_off + COMPACT_ENTRY_BYTES]);
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
        let (unit_idx, entry) = EntryDelta::read_from(&encoded[off..off + COMPACT_ENTRY_BYTES]);
        if (unit_idx as usize) < units.len() {
            out[slot] = Some(compose(&units[unit_idx as usize], &entry));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a 28 B BlockmapValue matching onyx-storage's
    /// `encode_blockmap_value` byte layout.
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
        // 18 header + 384 entries (zero) + 0 unit dict = 402.
        assert_eq!(enc.len(), 402);
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
        // 18 + 384 + 26 = 428
        assert_eq!(enc.len(), 428);
        let cr = DENSE_FOOTPRINT_BYTES as f64 / enc.len() as f64;
        assert!(cr > 8.0, "CR too low: {cr}");

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
        // 18 + 384 + 8*26 = 610
        assert_eq!(enc.len(), 610);
        let cr = DENSE_FOOTPRINT_BYTES as f64 / enc.len() as f64;
        assert!(cr >= 5.0, "8-unit CR {cr} below 5x");

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
        // 128 distinct units => 18 + 384 + 26*128 = 3730 B,
        // well within the 4032 B leaf payload.
        let (mut bm, mut vals) = empty_leaf_input();
        for i in 0..LEAF_ENTRY_COUNT {
            let v = bv(0x3000 + i as u64, 1, 500, 4096, 1, 0, i as u32, 0, 0);
            set(&mut bm, &mut vals, i, v);
        }
        let enc = encode(&bm, &vals);
        assert_eq!(enc.len(), 3730);
        for s in 0..LEAF_ENTRY_COUNT {
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
        let cap = max_units_per_payload(p.len());
        // Fill up the dict.
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
        compact_in_place(&mut p);
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
        compact_in_place(&mut p);
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
        compact_in_place(&mut p);
        let after_first = p.clone();
        compact_in_place(&mut p);
        assert_eq!(p, after_first);
    }

    #[test]
    fn full_overflow_then_compact_recovers() {
        // Drive the unit dict to its capacity, kill enough live entries
        // to free room, then verify a fresh `find_or_append_unit` works
        // again after `compact_in_place`.
        let mut p = fresh_payload();
        let cap = max_units_per_payload(p.len());

        // Fill: each leaf slot 0..min(cap,128) gets its own unit.
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
        // If cap > LEAF_ENTRY_COUNT, dict isn't truly full yet — pre-test
        // requires we filled it. With 4032-byte payload, cap=139 > 128
        // so we always have headroom; instead, force overflow by adding
        // a 129th *new* unit (must be done by re-using a slot we'll
        // first clear so we have a place to write). Skip the overflow
        // step if cap < LEAF_ENTRY_COUNT (impossible at PAGE_PAYLOAD_SIZE).
        assert!(
            cap >= LEAF_ENTRY_COUNT,
            "regression: dict capacity dropped below 128"
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
                    compact_in_place(&mut p);
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
        compact_in_place(&mut p);
        // Old dict spanned units [0..5); after compaction only unit 0
        // remains. Bytes for slots 1..5 must be zero so the page CRC
        // doesn't capture stale unit data.
        let tail_start = unit_offset(1);
        let tail_end = unit_offset(5);
        assert!(p[tail_start..tail_end].iter().all(|b| *b == 0));
    }

    #[test]
    fn fixed_offsets_compile_time_invariants() {
        // Sanity for layout constants and the fact that even the
        // worst-case shape fits inside the 4032 B leaf payload.
        assert_eq!(COMPACT_HEADER_BYTES, 18);
        assert_eq!(COMPACT_ENTRIES_OFFSET, 18);
        assert_eq!(COMPACT_UNIT_DICT_OFFSET, 402);
        assert_eq!(COMPACT_ENTRY_BYTES, 3);
        assert_eq!(COMPACT_UNIT_BYTES, 26);
        assert_eq!(compact_size(0), 402);
        assert_eq!(compact_size(1), 428);
        assert_eq!(compact_size(8), 610);
        assert_eq!(compact_size(128), 3730);
        assert!(compact_size(128) <= crate::page::PAGE_PAYLOAD_SIZE);
    }
}
