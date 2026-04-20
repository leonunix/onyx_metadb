//! Paged L2P page formats: leaf (level 0) + index (level 1..=4).
//!
//! Onyx's L2P maps u64 LBA → 28 B value. Keys are dense non-negative
//! integers with no lexicographic surprises, so we can skip the key-
//! storage and key-comparison overhead of a B+tree and use the LBA
//! itself as an array index through a 5-level radix tree:
//!
//! ```text
//! lba  ──┬──► bit_in_leaf = lba & 0x7F      (128 entries per leaf)
//!        └──► leaf_idx    = lba >> 7
//!
//! leaf_idx ──┬──► slot_in_level_0 = leaf_idx        & 0xFF
//!            ├──► slot_in_level_1 = (leaf_idx >> 8) & 0xFF
//!            ├──► slot_in_level_2 = (leaf_idx >> 16) & 0xFF
//!            ├──► slot_in_level_3 = (leaf_idx >> 24) & 0xFF
//!            └──► slot_in_level_4 = (leaf_idx >> 32) & 0xFF
//! ```
//!
//! An index page at level `L` has 256 child pointers, each pointing to
//! another index at level `L-1` (or, for `L=1`, to a leaf). The root is
//! an index page (or a leaf, if the tree only has one leaf's worth of
//! data); its level lives in [`type_header_level`].
//!
//! # Leaf page layout (4 KiB)
//!
//! ```text
//!   [ 0.. 64]  shared page header (64 B; type = PagedLeaf)
//!               - type_header[0]   level = 0
//!               - key_count        number of set bits in bitmap
//!   [64.. 80]  presence bitmap: 128 bits, little-endian within each byte
//!                bit `i` is set ↔ entry `i` is populated
//!   [80..3664] entries: 128 × 28 B = 3584 B
//!                entry `i` lives at payload offset 16 + i*28
//!   [3664..4032] padding (zeros; covered by CRC)
//! ```
//!
//! Unset entries have their bitmap bit clear. Their 28-byte slot is
//! zero-filled by invariant (we explicitly zero on delete and at
//! allocation), which keeps the CRC story simple.
//!
//! # Index page layout (4 KiB)
//!
//! ```text
//!   [ 0.. 64]  shared page header (64 B; type = PagedIndex)
//!               - type_header[0]   level 1..=MAX_INDEX_LEVEL
//!               - key_count        number of non-null child slots
//!   [64..2112] 256 child pointers × 8 B = 2048 B
//!                slot `i` lives at payload offset i*8 (little-endian u64)
//!                NULL_PAGE marks an empty slot (sparse subtree)
//!   [2112..4032] padding (zeros; covered by CRC)
//! ```
//!
//! # Addressable range
//!
//! - level 0 leaf: 128 LBAs  (= 512 KiB of 4 KiB LBAs)
//! - level 1 index: 256 × 128 = 32 K LBAs        (128 MiB)
//! - level 2 index: 256² × 128 = 8 M LBAs        (32 GiB)
//! - level 3 index: 256³ × 128 = 2 G LBAs        (8 TiB)
//! - level 4 index: 256⁴ × 128 = 512 G LBAs      (2 PiB)
//!
//! With 16 L2P shards, one shard's level-3 root already covers 128 TiB,
//! which we take as the practical ceiling for Onyx. Level 4 exists as
//! headroom — the tree only grows upward on demand.

use crate::config::PAGE_SIZE;
use crate::error::{MetaDbError, Result};
use crate::page::{PAGE_HEADER_SIZE, PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use crate::types::{Lsn, NULL_PAGE, PageId};

/// Bytes per entry in a leaf page. Matches `btree::L2P_VALUE_SIZE` on
/// purpose so we can share the 28 B `L2pValue` type.
pub const LEAF_VALUE_SIZE: usize = 28;

/// Entries per leaf. Chosen as a power of two so addressing is a pair
/// of bit ops (`lba & 0x7F`, `lba >> 7`).
pub const LEAF_ENTRY_COUNT: usize = 128;

/// Power-of-two shift: `lba >> LEAF_SHIFT` gives `leaf_idx`.
pub const LEAF_SHIFT: u32 = 7;

/// Mask selecting the bit-in-leaf from a raw LBA.
pub const LEAF_MASK: u64 = (LEAF_ENTRY_COUNT as u64) - 1;

/// Bitmap size in bytes (128 bits = 16 B).
pub const LEAF_BITMAP_BYTES: usize = LEAF_ENTRY_COUNT / 8;

/// Offset inside the payload where the bitmap starts.
pub const LEAF_BITMAP_OFFSET: usize = 0;

/// Offset inside the payload where the entries array starts.
pub const LEAF_ENTRIES_OFFSET: usize = LEAF_BITMAP_OFFSET + LEAF_BITMAP_BYTES;

/// Children per index page. Also a power of two; each level consumes
/// 8 LBA-bits.
pub const INDEX_FANOUT: usize = 256;

/// Bits of addressing consumed by one index level.
pub const INDEX_SHIFT: u32 = 8;

/// Mask selecting the slot-in-index from a `leaf_idx`-space value.
pub const INDEX_MASK: u64 = (INDEX_FANOUT as u64) - 1;

/// Bytes per child pointer (u64 page id, little-endian).
pub const INDEX_CHILD_SIZE: usize = 8;

/// Highest supported index level. Level 0 is a leaf; levels 1..=4 are
/// index pages. Level 4 covers 2 PiB of LBA space per shard — more than
/// any realistic Onyx volume fit.
pub const MAX_INDEX_LEVEL: u8 = 4;

// Compile-time invariants. If any of these fail the addressing code in
// `tree.rs` needs a fresh review.
const _: () = {
    assert!(PAGE_PAYLOAD_SIZE == 4032);
    assert!(LEAF_ENTRY_COUNT == 128);
    assert!(LEAF_BITMAP_BYTES == 16);
    assert!(LEAF_ENTRIES_OFFSET == 16);
    assert!(LEAF_ENTRIES_OFFSET + LEAF_ENTRY_COUNT * LEAF_VALUE_SIZE <= PAGE_PAYLOAD_SIZE);
    assert!(INDEX_FANOUT == 256);
    assert!(INDEX_FANOUT * INDEX_CHILD_SIZE <= PAGE_PAYLOAD_SIZE);
    assert!(1u64.wrapping_shl(LEAF_SHIFT) == LEAF_ENTRY_COUNT as u64);
    assert!(1u64.wrapping_shl(INDEX_SHIFT) == INDEX_FANOUT as u64);
    assert!(PAGE_SIZE == PAGE_HEADER_SIZE + PAGE_PAYLOAD_SIZE);
};

/// Level byte lives at type-header offset 0.
const TYPE_HDR_LEVEL: usize = 0;

/// 28-byte opaque value stored against each L2P key. The engine treats
/// this as opaque bytes — Onyx encodes its `BlockmapValue` into these
/// 28 bytes in the embedder layer.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct L2pValue(pub [u8; LEAF_VALUE_SIZE]);

impl L2pValue {
    /// All-zero value. Useful in tests and as a placeholder in unset
    /// leaf slots (leaves clear slots to zero on delete).
    pub const ZERO: Self = Self([0u8; LEAF_VALUE_SIZE]);

    /// Construct from a byte slice (padded with zeros if shorter than
    /// `LEAF_VALUE_SIZE`; panics if longer).
    pub fn from_slice(s: &[u8]) -> Self {
        assert!(s.len() <= LEAF_VALUE_SIZE, "value slice too long");
        let mut v = [0u8; LEAF_VALUE_SIZE];
        v[..s.len()].copy_from_slice(s);
        Self(v)
    }
}

/// Initialize a fresh empty leaf with `generation` stamped in the header.
/// Caller must `seal()` before persisting.
pub fn init_leaf(page: &mut Page, generation: Lsn) {
    page.bytes_mut().fill(0);
    page.write_header(&PageHeader::new(PageType::PagedLeaf, generation));
    // Level byte is already zero after the fill; no extra write needed.
}

/// Initialize a fresh empty index page at `level` with `generation`.
/// All child slots start as `NULL_PAGE`. Caller must `seal()` before
/// persisting.
pub fn init_index(page: &mut Page, generation: Lsn, level: u8) {
    assert!(
        (1..=MAX_INDEX_LEVEL).contains(&level),
        "init_index: level {level} out of range 1..={MAX_INDEX_LEVEL}"
    );
    page.bytes_mut().fill(0);
    page.write_header(&PageHeader::new(PageType::PagedIndex, generation));
    page.type_header_mut()[TYPE_HDR_LEVEL] = level;
    // Child slots sentinel to NULL_PAGE (u64::MAX = 0xFF bytes). The
    // post-header fill covers slots 0..INDEX_FANOUT; padding past the
    // slots region stays zero.
    let slots_end = INDEX_FANOUT * INDEX_CHILD_SIZE;
    page.payload_mut()[..slots_end].fill(0xFF);
}

/// Read the level byte from a paged page. Returns 0 for leaves, 1..=4
/// for index pages. Returns `Err` if the page is not a paged type.
pub fn page_level(page: &Page) -> Result<u8> {
    match page.header()?.page_type {
        PageType::PagedLeaf => Ok(0),
        PageType::PagedIndex => Ok(page.type_header()[TYPE_HDR_LEVEL]),
        other => Err(MetaDbError::Corruption(format!(
            "paged format: expected PagedLeaf/PagedIndex, got {other:?}"
        ))),
    }
}

// -------- leaf accessors ---------------------------------------------------

/// True iff the bit for entry `i` is set.
#[inline]
pub fn leaf_bit_set(page: &Page, i: usize) -> bool {
    debug_assert!(i < LEAF_ENTRY_COUNT);
    let byte = page.payload()[LEAF_BITMAP_OFFSET + i / 8];
    (byte >> (i % 8)) & 1 == 1
}

/// Set the bit for entry `i`.
#[inline]
pub fn leaf_bit_set_true(page: &mut Page, i: usize) {
    debug_assert!(i < LEAF_ENTRY_COUNT);
    let byte = &mut page.payload_mut()[LEAF_BITMAP_OFFSET + i / 8];
    *byte |= 1u8 << (i % 8);
}

/// Clear the bit for entry `i`.
#[inline]
pub fn leaf_bit_clear(page: &mut Page, i: usize) {
    debug_assert!(i < LEAF_ENTRY_COUNT);
    let byte = &mut page.payload_mut()[LEAF_BITMAP_OFFSET + i / 8];
    *byte &= !(1u8 << (i % 8));
}

/// Read the value at entry `i` without consulting the bitmap. Returns
/// `ZERO` for unset slots by invariant (delete zeroes the slot).
#[inline]
pub fn leaf_value_at(page: &Page, i: usize) -> L2pValue {
    debug_assert!(i < LEAF_ENTRY_COUNT);
    let off = LEAF_ENTRIES_OFFSET + i * LEAF_VALUE_SIZE;
    let mut v = [0u8; LEAF_VALUE_SIZE];
    v.copy_from_slice(&page.payload()[off..off + LEAF_VALUE_SIZE]);
    L2pValue(v)
}

/// Overwrite the value at entry `i`. Does not touch the bitmap.
#[inline]
pub fn leaf_set_value(page: &mut Page, i: usize, v: &L2pValue) {
    debug_assert!(i < LEAF_ENTRY_COUNT);
    let off = LEAF_ENTRIES_OFFSET + i * LEAF_VALUE_SIZE;
    page.payload_mut()[off..off + LEAF_VALUE_SIZE].copy_from_slice(&v.0);
}

/// Zero the value bytes at entry `i`. Called on delete so a CRC over the
/// page doesn't capture stale content.
#[inline]
pub fn leaf_zero_value(page: &mut Page, i: usize) {
    debug_assert!(i < LEAF_ENTRY_COUNT);
    let off = LEAF_ENTRIES_OFFSET + i * LEAF_VALUE_SIZE;
    page.payload_mut()[off..off + LEAF_VALUE_SIZE].fill(0);
}

/// Number of set bits, read from the page header. Maintained by
/// `leaf_set` / `leaf_clear` so reading is O(1).
#[inline]
pub fn leaf_entry_count(page: &Page) -> u16 {
    page.key_count()
}

/// Set entry `i` to `v`. Returns the previous value if the slot was
/// set, `None` otherwise. Updates the bitmap and the header counter.
pub fn leaf_set(page: &mut Page, i: usize, v: &L2pValue) -> Option<L2pValue> {
    debug_assert!(i < LEAF_ENTRY_COUNT);
    let was_set = leaf_bit_set(page, i);
    let old = if was_set { Some(leaf_value_at(page, i)) } else { None };
    leaf_set_value(page, i, v);
    if !was_set {
        leaf_bit_set_true(page, i);
        let n = page.key_count().wrapping_add(1);
        page.set_key_count(n);
    }
    old
}

/// Clear entry `i`. Returns the previous value if the slot was set,
/// `None` otherwise. Updates the bitmap and the header counter.
pub fn leaf_clear(page: &mut Page, i: usize) -> Option<L2pValue> {
    debug_assert!(i < LEAF_ENTRY_COUNT);
    if !leaf_bit_set(page, i) {
        return None;
    }
    let old = leaf_value_at(page, i);
    leaf_bit_clear(page, i);
    leaf_zero_value(page, i);
    let n = page.key_count().saturating_sub(1);
    page.set_key_count(n);
    Some(old)
}

// -------- index accessors --------------------------------------------------

/// Read child pointer at slot `i`. Returns `NULL_PAGE` for empty slots.
#[inline]
pub fn index_child_at(page: &Page, i: usize) -> PageId {
    debug_assert!(i < INDEX_FANOUT);
    let off = i * INDEX_CHILD_SIZE;
    u64::from_le_bytes(
        page.payload()[off..off + INDEX_CHILD_SIZE]
            .try_into()
            .unwrap(),
    )
}

/// Write child pointer at slot `i`. Updates the header counter if the
/// slot transitioned between null and non-null.
pub fn index_set_child(page: &mut Page, i: usize, child: PageId) {
    debug_assert!(i < INDEX_FANOUT);
    let was_null = index_child_at(page, i) == NULL_PAGE;
    let off = i * INDEX_CHILD_SIZE;
    page.payload_mut()[off..off + INDEX_CHILD_SIZE].copy_from_slice(&child.to_le_bytes());
    let becomes_null = child == NULL_PAGE;
    match (was_null, becomes_null) {
        (true, false) => {
            let n = page.key_count().wrapping_add(1);
            page.set_key_count(n);
        }
        (false, true) => {
            let n = page.key_count().saturating_sub(1);
            page.set_key_count(n);
        }
        _ => {}
    }
}

/// Number of non-null slots, read from the page header.
#[inline]
pub fn index_child_count(page: &Page) -> u16 {
    page.key_count()
}

/// Collect every non-null child id. Used by the recursive decref / drop
/// path to cascade refcount releases without recursing inside the
/// `PageBuf` lock.
pub fn index_collect_children(page: &Page) -> Vec<PageId> {
    let mut out = Vec::with_capacity(index_child_count(page) as usize);
    for i in 0..INDEX_FANOUT {
        let c = index_child_at(page, i);
        if c != NULL_PAGE {
            out.push(c);
        }
    }
    out
}

/// Max addressable `leaf_idx` for a tree rooted at `root_level`. Used by
/// the tree's growth logic and by out-of-range early exit in `get`.
///
/// - level 0 (root is a leaf): 1 leaf → max leaf_idx = 0
/// - level L (root is index):  `INDEX_FANOUT^L` leaves → max leaf_idx = fanout^L - 1
pub fn max_leaf_idx_at_level(level: u8) -> u64 {
    if level == 0 {
        return 0;
    }
    // 256^level - 1. Safe up to level 8 (256^8 = 2^64 overflows at
    // level 8). MAX_INDEX_LEVEL is 4 so no overflow concern here.
    1u64.wrapping_shl(INDEX_SHIFT * level as u32).wrapping_sub(1)
}

/// Given `leaf_idx` and the page's level, return the slot within this
/// index page. Meaningless for level 0 (leaves have no slots of index-
/// page form; they use `lba & LEAF_MASK`).
#[inline]
pub fn slot_in_index(leaf_idx: u64, level: u8) -> usize {
    debug_assert!((1..=MAX_INDEX_LEVEL).contains(&level));
    // One level consumes INDEX_SHIFT bits. Level 1 reads the lowest
    // 8 bits of leaf_idx; level 2 reads bits 8..16; etc.
    let shift = INDEX_SHIFT * (level as u32 - 1);
    ((leaf_idx >> shift) & INDEX_MASK) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_leaf() -> Page {
        let mut p = Page::zeroed();
        init_leaf(&mut p, 1);
        p
    }

    fn mk_index(level: u8) -> Page {
        let mut p = Page::zeroed();
        init_index(&mut p, 1, level);
        p
    }

    #[test]
    fn leaf_header_is_tagged_level_zero() {
        let p = mk_leaf();
        assert_eq!(p.header().unwrap().page_type, PageType::PagedLeaf);
        assert_eq!(page_level(&p).unwrap(), 0);
    }

    #[test]
    fn index_header_records_level() {
        for lv in 1..=MAX_INDEX_LEVEL {
            let p = mk_index(lv);
            assert_eq!(p.header().unwrap().page_type, PageType::PagedIndex);
            assert_eq!(page_level(&p).unwrap(), lv);
        }
    }

    #[test]
    fn leaf_set_and_get_roundtrip() {
        let mut p = mk_leaf();
        let v = L2pValue([0xABu8; LEAF_VALUE_SIZE]);
        assert_eq!(leaf_set(&mut p, 5, &v), None);
        assert_eq!(leaf_entry_count(&p), 1);
        assert!(leaf_bit_set(&p, 5));
        assert_eq!(leaf_value_at(&p, 5), v);
        // Overwrite returns the previous value.
        let v2 = L2pValue([0xCDu8; LEAF_VALUE_SIZE]);
        assert_eq!(leaf_set(&mut p, 5, &v2), Some(v));
        assert_eq!(leaf_entry_count(&p), 1); // still 1 entry
        assert_eq!(leaf_value_at(&p, 5), v2);
    }

    #[test]
    fn leaf_clear_zeros_slot_and_decrements_count() {
        let mut p = mk_leaf();
        let v = L2pValue([0x11u8; LEAF_VALUE_SIZE]);
        leaf_set(&mut p, 3, &v);
        leaf_set(&mut p, 100, &v);
        assert_eq!(leaf_entry_count(&p), 2);
        assert_eq!(leaf_clear(&mut p, 3), Some(v));
        assert_eq!(leaf_entry_count(&p), 1);
        assert!(!leaf_bit_set(&p, 3));
        // The cleared slot is zeroed.
        assert_eq!(leaf_value_at(&p, 3), L2pValue::ZERO);
        // Clearing an already-clear slot is a no-op.
        assert_eq!(leaf_clear(&mut p, 3), None);
        assert_eq!(leaf_entry_count(&p), 1);
    }

    #[test]
    fn leaf_bits_independent_per_slot() {
        let mut p = mk_leaf();
        for i in (0..LEAF_ENTRY_COUNT).step_by(7) {
            let v = L2pValue([i as u8; LEAF_VALUE_SIZE]);
            leaf_set(&mut p, i, &v);
        }
        for i in 0..LEAF_ENTRY_COUNT {
            if i % 7 == 0 {
                assert!(leaf_bit_set(&p, i), "slot {i} should be set");
                assert_eq!(leaf_value_at(&p, i).0[0], i as u8);
            } else {
                assert!(!leaf_bit_set(&p, i), "slot {i} should be clear");
                assert_eq!(leaf_value_at(&p, i), L2pValue::ZERO);
            }
        }
        assert_eq!(
            leaf_entry_count(&p),
            (LEAF_ENTRY_COUNT.div_ceil(7)) as u16
        );
    }

    #[test]
    fn index_child_slot_roundtrip() {
        let mut p = mk_index(2);
        assert_eq!(index_child_at(&p, 0), NULL_PAGE);
        assert_eq!(index_child_count(&p), 0);
        index_set_child(&mut p, 42, 9001);
        assert_eq!(index_child_at(&p, 42), 9001);
        assert_eq!(index_child_count(&p), 1);
        index_set_child(&mut p, 42, 9002); // overwrite (still non-null)
        assert_eq!(index_child_count(&p), 1);
        index_set_child(&mut p, 42, NULL_PAGE); // clear
        assert_eq!(index_child_at(&p, 42), NULL_PAGE);
        assert_eq!(index_child_count(&p), 0);
    }

    #[test]
    fn index_collect_children_skips_nulls() {
        let mut p = mk_index(1);
        index_set_child(&mut p, 0, 10);
        index_set_child(&mut p, 100, 11);
        index_set_child(&mut p, 255, 12);
        let children = index_collect_children(&p);
        assert_eq!(children, vec![10, 11, 12]);
    }

    #[test]
    fn seal_and_verify_roundtrip_for_both_types() {
        let mut leaf = mk_leaf();
        leaf_set(&mut leaf, 7, &L2pValue([0x5Au8; LEAF_VALUE_SIZE]));
        leaf.seal();
        leaf.verify(123).unwrap();

        let mut idx = mk_index(3);
        index_set_child(&mut idx, 9, 2001);
        idx.seal();
        idx.verify(123).unwrap();
    }

    #[test]
    fn max_leaf_idx_monotonic_by_level() {
        assert_eq!(max_leaf_idx_at_level(0), 0);
        assert_eq!(max_leaf_idx_at_level(1), 255);
        assert_eq!(max_leaf_idx_at_level(2), 65_535);
        assert_eq!(max_leaf_idx_at_level(3), 16_777_215);
        // 256^4 - 1 = 2^32 - 1.
        assert_eq!(max_leaf_idx_at_level(4), u32::MAX as u64);
    }

    #[test]
    fn slot_in_index_reads_expected_byte() {
        let idx = 0xAABB_CCDD_u64; // leaf_idx
        assert_eq!(slot_in_index(idx, 1), 0xDD);
        assert_eq!(slot_in_index(idx, 2), 0xCC);
        assert_eq!(slot_in_index(idx, 3), 0xBB);
        assert_eq!(slot_in_index(idx, 4), 0xAA);
    }
}
