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
//! # Leaf page layout (4 KiB) — Onyx-aware compact format
//!
//! Onyx's packer puts consecutive LBAs into the same compression unit,
//! so 8 of the 9 fields in `BlockmapValue` (everything except
//! `offset_in_unit`) repeat across all LBAs in a unit. We exploit that
//! by storing each unit's shared bytes once and a 3 B per-slot record
//! that names the unit and the per-slot offset. See
//! [`crate::paged::leaf_compact`] for the full scheme and rationale.
//!
//! ```text
//!   [  0.. 64]  shared page header (64 B; type = PagedLeaf)
//!                 - type_header[0]   level = 0
//!                 - key_count        number of set bits in bitmap
//!   [ 64.. 80]  presence bitmap: 128 bits, LE within each byte
//!                 bit `i` set ↔ slot `i` is populated
//!   [ 80.. 81]  unit_count        u8 (live entries in unit dict)
//!   [ 81.. 82]  format_version    u8 (= COMPACT_VERSION)
//!   [ 82..466]  entries           128 × 3 B (slot-indexed dense array)
//!                 entry @ slot s lives at payload offset 18 + s*3
//!                   [0..1] unit_idx        u8
//!                   [1..3] offset_in_unit  u16 BE
//!                 unset slots are zero (caller must check bitmap)
//!   [466..XXXX] unit dict         N × 26 B (variable, up to 139 units)
//!   [XXXX..4032] padding (zeros; covered by CRC)
//! ```
//!
//! Unset entry slots are zero by invariant: `leaf_clear` zeroes the 3 B
//! record. Dead unit-dict entries (units no longer referenced by any
//! live entry) accumulate until the dict fills up; at that point
//! `leaf_set` triggers an in-place compaction that rebuilds the dict
//! tightly. See [`leaf_compact::compact_in_place`].
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
use crate::paged::leaf_compact;
use crate::types::{Lsn, NULL_PAGE, PageId};

/// Bytes per logical L2P value. The first 28 bytes embed Onyx's
/// `BlockmapValue` (see `head_pba` contract below). The last 8 bytes
/// are a big-endian `u64` commit-seq used by metadb's `seq_guard` to
/// reject stale concurrent commits; see `L2P_SEQ_OFFSET` and
/// `apply::apply_l2p_remap` for the CAS semantics. In the compact leaf
/// format an entry on disk is **11 B** (slot record: unit_idx + offset
/// + seq) + a slice of the 26 B unit-dict entry — `LEAF_VALUE_SIZE` is
/// the *logical* size of the reconstituted value, not the per-slot
/// on-disk size.
pub const LEAF_VALUE_SIZE: usize = 36;

/// Byte offset within an `L2pValue` where the 8-byte big-endian commit
/// seq lives. Onyx fills this at the adapter boundary; metadb apply
/// reads it for the seq_guard CAS check.
pub const L2P_SEQ_OFFSET: usize = 28;

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
    // Compact format invariants (v3). With per-slot record at 7 B
    // (unit_idx + offset_in_unit + u32 seq_delta) and per-leaf
    // base_seq (8 B) in the header, the entries region is 896 B and
    // the unit-dict offset moves to 922. The pathological "128
    // distinct units" case fits at 3994 B with 38 B headroom.
    assert!(leaf_compact::COMPACT_HEADER_BYTES == 26);
    assert!(leaf_compact::COMPACT_UNIT_DICT_OFFSET == 922);
    assert!(leaf_compact::MAX_UNITS_PER_LEAF == LEAF_ENTRY_COUNT);
    assert!(leaf_compact::compact_size(leaf_compact::MAX_UNITS_PER_LEAF) <= PAGE_PAYLOAD_SIZE);
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

    /// Onyx encoding contract: the first 8 bytes of an `L2pValue` are
    /// the big-endian `Pba` that the mapping targets. This matches the
    /// layout of `BlockmapValue` in onyx-storage; changing the L2pValue
    /// header breaks the shared apply path in
    /// [`Db::commit_ops`](crate::db::Db::commit_ops) for `WalOp::L2pRemap`.
    ///
    /// SPEC §3.1 (ONYX_INTEGRATION_SPEC.md) formalises this contract:
    /// metadb is Onyx's only client and trades the "opaque L2pValue"
    /// abstraction for a 16-byte WAL saving per remap. Anyone storing
    /// a non-Onyx payload in `L2pValue` must avoid `L2pRemap`.
    pub fn head_pba(&self) -> crate::types::Pba {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.0[..8]);
        u64::from_be_bytes(buf)
    }

    /// Big-endian 8-byte commit-seq stored at `[L2P_SEQ_OFFSET..]`.
    /// Onyx stamps this from the buffer pool's monotonic per-LBA seq
    /// when building the value; metadb apply uses it for the seq_guard
    /// CAS check in `apply_l2p_remap` / `apply_l2p_put`.
    ///
    /// `0` is a sentinel meaning "no seq attached" — apply skips the
    /// CAS check in either direction (incoming or stored). Used by
    /// legacy callers like `DedupScanner` and direct `insert` that
    /// don't have a buffer seq to attach.
    pub fn seq(&self) -> u64 {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.0[L2P_SEQ_OFFSET..L2P_SEQ_OFFSET + 8]);
        u64::from_be_bytes(buf)
    }

    /// Return a copy with the seq field replaced. Onyx's adapter uses
    /// this to stamp the buffer seq into a freshly-built value before
    /// pushing the op into a `Transaction`.
    pub fn with_seq(mut self, seq: u64) -> Self {
        self.0[L2P_SEQ_OFFSET..L2P_SEQ_OFFSET + 8].copy_from_slice(&seq.to_be_bytes());
        self
    }
}

/// Initialize a fresh empty leaf with `generation` stamped in the header.
/// Caller must `seal()` before persisting. The compact-format version
/// byte is written into the payload so a freshly-allocated leaf
/// already passes `decode_at`'s version check.
pub fn init_leaf(page: &mut Page, generation: Lsn) {
    page.bytes_mut().fill(0);
    page.write_header(&PageHeader::new(PageType::PagedLeaf, generation));
    leaf_compact::init_payload(page.payload_mut());
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
/// `None` for unset slots by invariant (clear zeroes the slot record).
///
/// O(1): one fixed-offset entry read + one fixed-offset unit-dict read.
#[inline]
pub fn leaf_value_at(page: &Page, i: usize) -> Result<Option<L2pValue>> {
    debug_assert!(i < LEAF_ENTRY_COUNT);
    leaf_compact::payload_decode_at_checked(page.payload(), i).map(|v| v.map(L2pValue))
}

/// Zero the per-slot entry record at slot `i`. Does not touch the
/// bitmap or the unit dictionary. Called from `leaf_clear` so a CRC
/// over the page doesn't capture stale entry bytes.
#[inline]
pub fn leaf_zero_value(page: &mut Page, i: usize) {
    debug_assert!(i < LEAF_ENTRY_COUNT);
    leaf_compact::zero_entry(page.payload_mut(), i);
}

/// Number of set bits, read from the page header. Maintained by
/// `leaf_set` / `leaf_clear` so reading is O(1).
#[inline]
pub fn leaf_entry_count(page: &Page) -> u16 {
    page.key_count()
}

/// Set entry `i` to `v`. Returns the previous value if the slot was
/// set, `None` otherwise. Updates the bitmap and the page header
/// counter; finds or appends the unit-dict record matching `v` and
/// writes the 11 B per-slot record (v2 schema: unit_idx + offset + seq).
///
/// If the unit dict is full when a new unit would be appended, this
/// function runs `compact_in_place` to drop dead unit entries and
/// retries. In v2 the payload-bound cap is `MAX_UNITS_PER_LEAF = 100`;
/// `compact_in_place` after a typical clear cycle frees enough room for
/// one more unit. A retry that still fails surfaces as `Corruption`.
pub fn leaf_set(page: &mut Page, i: usize, v: &L2pValue) -> Result<Option<L2pValue>> {
    debug_assert!(i < LEAF_ENTRY_COUNT);
    let payload_head = |page: &Page| -> Vec<u8> { page.payload()[..32].to_vec() };
    let version = page.payload()[LEAF_BITMAP_BYTES + 1];
    if version != leaf_compact::COMPACT_VERSION {
        return Err(MetaDbError::Corruption(format!(
            "compact leaf version {version} != {} before set slot {i} (key_count={}, page_gen={}, rc={}, payload0={:02x?})",
            leaf_compact::COMPACT_VERSION,
            page.key_count(),
            page.generation(),
            page.refcount(),
            payload_head(page),
        )));
    }
    let key_count = page.key_count() as usize;
    if key_count > LEAF_ENTRY_COUNT {
        return Err(MetaDbError::Corruption(format!(
            "compact leaf key_count {key_count} exceeds leaf capacity {LEAF_ENTRY_COUNT} before set slot {i} (unit_count={}, page_gen={}, rc={}, payload0={:02x?})",
            page.payload()[LEAF_BITMAP_BYTES],
            page.generation(),
            page.refcount(),
            payload_head(page),
        )));
    }
    let unit_count = page.payload()[LEAF_BITMAP_BYTES] as usize;
    let cap = leaf_compact::max_units_per_payload(page.payload().len())
        .min(leaf_compact::MAX_UNITS_PER_LEAF);
    if unit_count > cap {
        return Err(MetaDbError::Corruption(format!(
            "compact leaf unit_count {unit_count} exceeds payload capacity {cap} before set slot {i} (key_count={}, page_gen={}, rc={}, payload0={:02x?})",
            page.key_count(),
            page.generation(),
            page.refcount(),
            payload_head(page),
        )));
    }
    let was_set = leaf_compact::payload_bit_set(page.payload(), i);
    let old = if was_set {
        leaf_value_at(page, i)?
    } else {
        None
    };

    let (unit, entry) = leaf_compact::decompose_value(&v.0);

    // Two ways to fail and need to compact:
    //   (a) unit dict is full → `find_or_append_unit` returns None
    //   (b) seq_delta would overflow u32 → `try_write_entry` returns
    //       NeedsRebase
    //
    // For (a), if we're overwriting a set slot, the old unit may be
    // the only thing keeping the dict full; clearing this slot's
    // bitmap bit before compact lets compact reclaim the orphan if no
    // other slot references it. We zero the entry record too so the
    // post-compact unit_idx renumbering doesn't see a dangling ref.
    // If compact still can't make room (legitimately 128 distinct
    // live units across other slots), we restore the bit and surface
    // Corruption.
    let unit_idx = match leaf_compact::find_or_append_unit(page.payload_mut(), &unit) {
        Some(idx) => idx,
        None => {
            if was_set {
                leaf_compact::payload_bit_clear(page.payload_mut(), i);
                leaf_compact::zero_entry(page.payload_mut(), i);
            }
            leaf_compact::compact_in_place(page.payload_mut())?;
            let result = leaf_compact::find_or_append_unit(page.payload_mut(), &unit);
            // Restore the bit so the rest of leaf_set sees the same
            // pre-state regardless of compact's success — the new
            // entry/unit will overwrite it; if compact failed,
            // surfacing Corruption with the slot still marked live is
            // the consistent state for the caller to retry from.
            if was_set {
                leaf_compact::payload_bit_set_true(page.payload_mut(), i);
            }
            match result {
                Some(idx) => idx,
                None => {
                    return Err(MetaDbError::Corruption(
                        "compact_in_place did not free enough room for one unit".into(),
                    ));
                }
            }
        }
    };
    match leaf_compact::try_write_entry(page.payload_mut(), i, unit_idx, &entry) {
        leaf_compact::WriteEntryOutcome::Written => {}
        leaf_compact::WriteEntryOutcome::AdoptedBase { new_base } => {
            leaf_compact::write_base_seq(page.payload_mut(), new_base);
        }
        leaf_compact::WriteEntryOutcome::NeedsRebase => {
            // Seq delta out of range — either the incoming seq is below
            // base_seq (caller wrote with a smaller LSN than already on
            // disk) or above base_seq + u32::MAX (leaf has been alive
            // long enough to span 4G LSNs). compact_in_place rebases
            // base_seq to min(live seq, incoming seq); afterwards the
            // new seq fits as long as the live-seq spread itself stays
            // under u32::MAX. If it doesn't, surface Corruption — that
            // means a single leaf's writes legitimately span >4G LSNs,
            // which shouldn't happen in onyx workloads but is worth
            // flagging loudly so we don't silently drop a write.
            leaf_compact::compact_in_place_with_incoming(page.payload_mut(), entry.seq)?;
            // The unit dict was rewritten by compact_in_place; the unit
            // we just appended (or matched) may have a new index, so
            // re-locate it.
            let unit_idx = leaf_compact::find_or_append_unit(page.payload_mut(), &unit)
                .ok_or_else(|| {
                    MetaDbError::Corruption(
                        "compact_in_place did not free enough room for one unit (post-rebase)"
                            .into(),
                    )
                })?;
            match leaf_compact::try_write_entry(page.payload_mut(), i, unit_idx, &entry) {
                leaf_compact::WriteEntryOutcome::Written => {}
                leaf_compact::WriteEntryOutcome::AdoptedBase { new_base } => {
                    leaf_compact::write_base_seq(page.payload_mut(), new_base);
                }
                leaf_compact::WriteEntryOutcome::NeedsRebase => {
                    return Err(MetaDbError::Corruption(format!(
                        "compact leaf seq range spans >4G LSN at slot {i}: cannot encode \
                         seq_delta even after rebase (base_seq={})",
                        leaf_compact::read_base_seq(page.payload())
                    )));
                }
            }
        }
    }

    if !was_set {
        if key_count >= LEAF_ENTRY_COUNT {
            return Err(MetaDbError::Corruption(format!(
                "compact leaf cannot add slot {i}: key_count {key_count} is already at capacity {LEAF_ENTRY_COUNT} (unit_count={}, page_gen={}, rc={}, payload0={:02x?})",
                page.payload()[LEAF_BITMAP_BYTES],
                page.generation(),
                page.refcount(),
                payload_head(page),
            )));
        }
        leaf_compact::payload_bit_set_true(page.payload_mut(), i);
        let n = page.key_count() + 1;
        page.set_key_count(n);
    }
    Ok(old)
}

/// Clear entry `i`. Returns the previous value if the slot was set,
/// `None` otherwise. Updates the bitmap and the page header counter.
///
/// The unit dict is intentionally **not** modified — orphaning the unit
/// would require renumbering every other entry that shares it. Dead
/// units accumulate until the dict fills up; `leaf_set` then runs
/// `compact_in_place` to reclaim them. This keeps the common-case
/// clear path O(1).
pub fn leaf_clear(page: &mut Page, i: usize) -> Result<Option<L2pValue>> {
    debug_assert!(i < LEAF_ENTRY_COUNT);
    let key_count = page.key_count() as usize;
    if key_count > LEAF_ENTRY_COUNT {
        return Err(MetaDbError::Corruption(format!(
            "compact leaf key_count {key_count} exceeds leaf capacity {LEAF_ENTRY_COUNT} before clear slot {i} (unit_count={}, version={}, page_gen={}, rc={}, payload0={:02x?})",
            page.payload()[LEAF_BITMAP_BYTES],
            page.payload()[LEAF_BITMAP_BYTES + 1],
            page.generation(),
            page.refcount(),
            &page.payload()[..32],
        )));
    }
    if !leaf_compact::payload_bit_set(page.payload(), i) {
        return Ok(None);
    }
    let old = leaf_value_at(page, i)?;
    leaf_compact::payload_bit_clear(page.payload_mut(), i);
    leaf_compact::zero_entry(page.payload_mut(), i);
    let n = page.key_count().saturating_sub(1);
    page.set_key_count(n);
    Ok(old)
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
    1u64.wrapping_shl(INDEX_SHIFT * level as u32)
        .wrapping_sub(1)
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

    /// Build an L2pValue with `byte` repeated across non-derived
    /// fields. The v3 compact encoder drops `unit_lba_count` and
    /// reconstructs it from `unit_original_size / 4096`, so the input
    /// must satisfy that invariant. The per-LBA seq trailer is set to
    /// `byte` as a small monotonic u64 (rather than `byte` repeated 8
    /// times) so the per-leaf u32 seq_delta encoding doesn't overflow
    /// when two different `byte` values land in the same leaf.
    fn v(byte: u8) -> L2pValue {
        let mut x = [byte; LEAF_VALUE_SIZE];
        x[13..17].copy_from_slice(&4096u32.to_be_bytes());
        x[17..19].copy_from_slice(&1u16.to_be_bytes());
        x[28..36].copy_from_slice(&(byte as u64).to_be_bytes());
        L2pValue(x)
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
        let v_ab = v(0xAB);
        assert_eq!(leaf_set(&mut p, 5, &v_ab).unwrap(), None);
        assert_eq!(leaf_entry_count(&p), 1);
        assert!(leaf_bit_set(&p, 5));
        assert_eq!(leaf_value_at(&p, 5).unwrap(), Some(v_ab));
        // Overwrite returns the previous value.
        let v_cd = v(0xCD);
        assert_eq!(leaf_set(&mut p, 5, &v_cd).unwrap(), Some(v_ab));
        assert_eq!(leaf_entry_count(&p), 1); // still 1 entry
        assert_eq!(leaf_value_at(&p, 5).unwrap(), Some(v_cd));
    }

    #[test]
    fn leaf_clear_zeros_slot_and_decrements_count() {
        let mut p = mk_leaf();
        let v11 = v(0x11);
        leaf_set(&mut p, 3, &v11).unwrap();
        leaf_set(&mut p, 100, &v11).unwrap();
        assert_eq!(leaf_entry_count(&p), 2);
        assert_eq!(leaf_clear(&mut p, 3).unwrap(), Some(v11));
        assert_eq!(leaf_entry_count(&p), 1);
        assert!(!leaf_bit_set(&p, 3));
        // The cleared slot is zeroed.
        assert_eq!(leaf_value_at(&p, 3).unwrap(), None);
        // Clearing an already-clear slot is a no-op.
        assert_eq!(leaf_clear(&mut p, 3).unwrap(), None);
        assert_eq!(leaf_entry_count(&p), 1);
    }

    #[test]
    fn leaf_bits_independent_per_slot() {
        let mut p = mk_leaf();
        for i in (0..LEAF_ENTRY_COUNT).step_by(7) {
            let val = v(i as u8);
            leaf_set(&mut p, i, &val).unwrap();
        }
        for i in 0..LEAF_ENTRY_COUNT {
            if i % 7 == 0 {
                assert!(leaf_bit_set(&p, i), "slot {i} should be set");
                assert_eq!(leaf_value_at(&p, i).unwrap().unwrap().0[0], i as u8);
            } else {
                assert!(!leaf_bit_set(&p, i), "slot {i} should be clear");
                assert_eq!(leaf_value_at(&p, i).unwrap(), None);
            }
        }
        assert_eq!(leaf_entry_count(&p), (LEAF_ENTRY_COUNT.div_ceil(7)) as u16);
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
        leaf_set(&mut leaf, 7, &v(0x5A)).unwrap();
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
