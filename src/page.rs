//! 4 KiB page format, CRC'd, version-stamped.
//!
//! Every persistent structure in metadb is built out of these 4 KiB pages:
//! B+tree leaves and internal nodes, LSM SST data pages, free-list nodes,
//! and manifest slots. Unifying them behind one header lets
//! [`metadb-verify`](../../docs/TESTING.md) check integrity without having
//! to know each type's internals.
//!
//! # Layout
//!
//! ```text
//! offset  size  field
//! ------  ----  ----------------------------------------------
//!   0     4     magic = "ONXP" (little-endian 0x504E584F)
//!   4     1     page_type (see [`PageType`])
//!   5     1     version = 1
//!   6     2     key_count (interpretation per page_type)
//!   8     4     crc32c (zeroed during computation)
//!  12     4     flags (reserved)
//!  16     8     generation (LSN at which the page was written)
//!  24     4     refcount (number of parents pointing to this page)
//!  28     4     reserved
//!  32    32     page-type-specific header
//!  64  4032     payload
//! ```
//!
//! The CRC is computed over the whole 4 KiB buffer with the four CRC bytes
//! zeroed during the hash. This way we can verify the header plus every
//! byte of payload with one pass and one field; we don't have to remember
//! "which bytes are covered" per page type.

use crate::config::PAGE_SIZE;
use crate::error::{MetaDbError, Result};
use crate::types::{Lsn, PageId};

/// Size of the shared page header, in bytes.
pub const PAGE_HEADER_SIZE: usize = 64;

/// Size of the payload area after the header, in bytes.
pub const PAGE_PAYLOAD_SIZE: usize = PAGE_SIZE - PAGE_HEADER_SIZE;

/// Size of the page-type-specific sub-header inside the shared header.
pub const TYPE_HEADER_SIZE: usize = 32;

/// `"ONXP"` ASCII, read little-endian as a `u32`.
const PAGE_MAGIC: u32 = 0x504E_584F;

/// Page-format version. Bumped on any breaking layout change.
pub const PAGE_VERSION: u8 = 1;

// Header field offsets. Kept `pub(crate)` so on-disk-format tests can poke
// specific bytes.
pub(crate) const OFF_MAGIC: usize = 0;
pub(crate) const OFF_PAGE_TYPE: usize = 4;
pub(crate) const OFF_VERSION: usize = 5;
pub(crate) const OFF_KEY_COUNT: usize = 6;
pub(crate) const OFF_CRC: usize = 8;
pub(crate) const OFF_FLAGS: usize = 12;
pub(crate) const OFF_GENERATION: usize = 16;
pub(crate) const OFF_REFCOUNT: usize = 24;
#[allow(dead_code)] // documents the 4B reserved span between refcount and type header
pub(crate) const OFF_RESERVED: usize = 28;
pub(crate) const OFF_TYPE_HEADER: usize = 32;

// Compile-time layout guards. If any of these fail, the page header has
// been reshuffled and the codec needs a fresh round of review.
const _: () = {
    assert!(PAGE_SIZE == 4096);
    assert!(PAGE_HEADER_SIZE == 64);
    assert!(PAGE_PAYLOAD_SIZE == PAGE_SIZE - PAGE_HEADER_SIZE);
    assert!(TYPE_HEADER_SIZE == 32);
    assert!(OFF_MAGIC < OFF_PAGE_TYPE);
    assert!(OFF_PAGE_TYPE < OFF_VERSION);
    assert!(OFF_VERSION < OFF_KEY_COUNT);
    assert!(OFF_KEY_COUNT < OFF_CRC);
    assert!(OFF_CRC < OFF_FLAGS);
    assert!(OFF_FLAGS < OFF_GENERATION);
    assert!(OFF_GENERATION < OFF_REFCOUNT);
    assert!(OFF_REFCOUNT < OFF_RESERVED);
    assert!(OFF_RESERVED < OFF_TYPE_HEADER);
    assert!(OFF_TYPE_HEADER + TYPE_HEADER_SIZE == PAGE_HEADER_SIZE);
};

/// Discriminator for every kind of page that can live in the page file.
#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PageType {
    /// Free page available for allocation.
    Free = 0,
    /// B+tree leaf node for an L2P partition.
    L2pLeaf = 1,
    /// B+tree internal node for an L2P partition.
    L2pInternal = 2,
    /// Sequence of sorted fixed-size records in an LSM SST, or the SST's
    /// header / bloom pages (all share the same page type; the content
    /// is disambiguated by position within the SST run).
    LsmData = 3,
    /// Chunk of the persisted free-list chain.
    FreeListNode = 4,
    /// One slot of the double-buffered manifest.
    Manifest = 5,
    /// Per-snapshot page holding the pinned shard-root vector.
    SnapshotRoots = 6,
    /// Page in the chained list of SST handles for one LSM level.
    LsmLevels = 7,
    /// Paged L2P leaf: 128 × 28 B entries plus a 128-bit presence bitmap.
    PagedLeaf = 8,
    /// Paged L2P index: 256 × 8 B child page pointers with a level byte in
    /// the type-header. Forms the upper 1..=4 levels of the paged radix tree.
    PagedIndex = 9,
}

impl PageType {
    /// Decode a raw byte into a `PageType`, failing on unknown values.
    pub fn from_u8(v: u8) -> Result<Self> {
        Ok(match v {
            0 => Self::Free,
            1 => Self::L2pLeaf,
            2 => Self::L2pInternal,
            3 => Self::LsmData,
            4 => Self::FreeListNode,
            5 => Self::Manifest,
            6 => Self::SnapshotRoots,
            7 => Self::LsmLevels,
            8 => Self::PagedLeaf,
            9 => Self::PagedIndex,
            _ => return Err(MetaDbError::UnknownPageType(v)),
        })
    }
}

/// Decoded header of a [`Page`].
#[derive(Copy, Clone, Debug)]
pub struct PageHeader {
    pub page_type: PageType,
    pub version: u8,
    pub key_count: u16,
    pub flags: u32,
    pub generation: Lsn,
    pub refcount: u32,
}

impl PageHeader {
    /// Header for a freshly-written page of `page_type`, stamped with the
    /// LSN at which it will become visible. `refcount` defaults to 1 (the
    /// single parent that's about to link to it).
    pub fn new(page_type: PageType, generation: Lsn) -> Self {
        Self {
            page_type,
            version: PAGE_VERSION,
            key_count: 0,
            flags: 0,
            generation,
            refcount: 1,
        }
    }
}

/// Owned 4 KiB page.
///
/// The buffer lives on the heap so `Page` values are cheap to move around.
/// Construction does not zero the buffer twice — we allocate a zeroed box
/// directly.
#[derive(Clone)]
pub struct Page {
    bytes: Box<[u8; PAGE_SIZE]>,
}

impl std::fmt::Debug for Page {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page")
            .field("header", &self.header().ok())
            .finish()
    }
}

impl Page {
    /// All-zero page. Not a valid on-disk page until [`seal`](Self::seal).
    pub fn zeroed() -> Self {
        Self {
            bytes: Box::new([0u8; PAGE_SIZE]),
        }
    }

    /// Create a page initialized with `header`. Not yet sealed — the caller
    /// is expected to populate the payload, then call `seal()` before
    /// writing to disk.
    pub fn new(header: PageHeader) -> Self {
        let mut page = Self::zeroed();
        page.write_header(&header);
        page
    }

    /// Raw underlying bytes.
    pub fn bytes(&self) -> &[u8; PAGE_SIZE] {
        &self.bytes
    }

    /// Raw underlying bytes, mutable.
    pub fn bytes_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.bytes
    }

    /// Decode the page header. Returns an error if the magic or version
    /// bytes are unexpected; does *not* verify the CRC. Use
    /// [`verify`](Self::verify) for full integrity checking when you have a
    /// `page_id` for error reporting.
    pub fn header(&self) -> Result<PageHeader> {
        let b = &*self.bytes;
        let magic = u32_le(b, OFF_MAGIC);
        if magic != PAGE_MAGIC {
            return Err(MetaDbError::PageMagicMismatch {
                page_id: 0,
                found: magic,
            });
        }
        let version = b[OFF_VERSION];
        if version != PAGE_VERSION {
            return Err(MetaDbError::PageVersionUnsupported {
                page_id: 0,
                version,
            });
        }
        Ok(PageHeader {
            page_type: PageType::from_u8(b[OFF_PAGE_TYPE])?,
            version,
            key_count: u16_le(b, OFF_KEY_COUNT),
            flags: u32_le(b, OFF_FLAGS),
            generation: u64_le(b, OFF_GENERATION),
            refcount: u32_le(b, OFF_REFCOUNT),
        })
    }

    /// Overwrite the page header fields. The CRC is not recomputed until
    /// [`seal`](Self::seal) is called.
    pub fn write_header(&mut self, h: &PageHeader) {
        let b = &mut *self.bytes;
        put_u32_le(b, OFF_MAGIC, PAGE_MAGIC);
        b[OFF_PAGE_TYPE] = h.page_type as u8;
        b[OFF_VERSION] = PAGE_VERSION;
        put_u16_le(b, OFF_KEY_COUNT, h.key_count);
        put_u32_le(b, OFF_FLAGS, h.flags);
        put_u64_le(b, OFF_GENERATION, h.generation);
        put_u32_le(b, OFF_REFCOUNT, h.refcount);
        // OFF_RESERVED zeroed by `zeroed()`; OFF_CRC handled by `seal`.
    }

    /// Read-only view of the 32-byte page-type-specific sub-header.
    pub fn type_header(&self) -> &[u8; TYPE_HEADER_SIZE] {
        (&self.bytes[OFF_TYPE_HEADER..OFF_TYPE_HEADER + TYPE_HEADER_SIZE])
            .try_into()
            .unwrap()
    }

    /// Mutable view of the 32-byte page-type-specific sub-header.
    pub fn type_header_mut(&mut self) -> &mut [u8; TYPE_HEADER_SIZE] {
        (&mut self.bytes[OFF_TYPE_HEADER..OFF_TYPE_HEADER + TYPE_HEADER_SIZE])
            .try_into()
            .unwrap()
    }

    /// Read-only view of the payload.
    pub fn payload(&self) -> &[u8; PAGE_PAYLOAD_SIZE] {
        (&self.bytes[PAGE_HEADER_SIZE..]).try_into().unwrap()
    }

    /// Mutable view of the payload.
    pub fn payload_mut(&mut self) -> &mut [u8; PAGE_PAYLOAD_SIZE] {
        (&mut self.bytes[PAGE_HEADER_SIZE..]).try_into().unwrap()
    }

    /// Current `key_count` from the header.
    pub fn key_count(&self) -> u16 {
        u16_le(&*self.bytes, OFF_KEY_COUNT)
    }

    /// Overwrite `key_count`.  Does not reseal.
    pub fn set_key_count(&mut self, n: u16) {
        put_u16_le(&mut *self.bytes, OFF_KEY_COUNT, n);
    }

    /// Current `generation` (LSN) from the header.
    pub fn generation(&self) -> Lsn {
        u64_le(&*self.bytes, OFF_GENERATION)
    }

    /// Overwrite `generation`. Does not reseal.
    pub fn set_generation(&mut self, lsn: Lsn) {
        put_u64_le(&mut *self.bytes, OFF_GENERATION, lsn);
    }

    /// Current `refcount` from the header.
    pub fn refcount(&self) -> u32 {
        u32_le(&*self.bytes, OFF_REFCOUNT)
    }

    /// Overwrite `refcount`. Does not reseal.
    pub fn set_refcount(&mut self, rc: u32) {
        put_u32_le(&mut *self.bytes, OFF_REFCOUNT, rc);
    }

    /// Compute the CRC32C over the page buffer with the CRC field zeroed.
    /// Pure; does not mutate the page.
    pub fn compute_crc(&self) -> u32 {
        let b = &*self.bytes;
        let mut c: u32 = 0;
        c = crc32c::crc32c_append(c, &b[..OFF_CRC]);
        c = crc32c::crc32c_append(c, &[0u8; 4]);
        c = crc32c::crc32c_append(c, &b[OFF_CRC + 4..]);
        c
    }

    /// Write the CRC into the header, making the page ready for on-disk
    /// storage. Must be the final mutation before a disk write.
    pub fn seal(&mut self) {
        let crc = self.compute_crc();
        put_u32_le(&mut *self.bytes, OFF_CRC, crc);
    }

    /// Full integrity check: magic, version, then CRC32C. `page_id` is used
    /// only to enrich the error; it is not otherwise consulted.
    pub fn verify(&self, page_id: PageId) -> Result<()> {
        let b = &*self.bytes;
        let magic = u32_le(b, OFF_MAGIC);
        if magic != PAGE_MAGIC {
            return Err(MetaDbError::PageMagicMismatch {
                page_id,
                found: magic,
            });
        }
        let version = b[OFF_VERSION];
        if version != PAGE_VERSION {
            return Err(MetaDbError::PageVersionUnsupported { page_id, version });
        }
        let stored = u32_le(b, OFF_CRC);
        let computed = self.compute_crc();
        if stored != computed {
            return Err(MetaDbError::PageChecksumMismatch {
                page_id,
                expected: stored,
                actual: computed,
            });
        }
        Ok(())
    }
}

// -------- little-endian byte helpers ---------------------------------------

#[inline]
fn u16_le(b: &[u8], off: usize) -> u16 {
    u16::from_le_bytes(b[off..off + 2].try_into().unwrap())
}

#[inline]
fn u32_le(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(b[off..off + 4].try_into().unwrap())
}

#[inline]
fn u64_le(b: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(b[off..off + 8].try_into().unwrap())
}

#[inline]
fn put_u16_le(b: &mut [u8], off: usize, v: u16) {
    b[off..off + 2].copy_from_slice(&v.to_le_bytes());
}

#[inline]
fn put_u32_le(b: &mut [u8], off: usize, v: u32) {
    b[off..off + 4].copy_from_slice(&v.to_le_bytes());
}

#[inline]
fn put_u64_le(b: &mut [u8], off: usize, v: u64) {
    b[off..off + 8].copy_from_slice(&v.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trip() {
        let h = PageHeader {
            page_type: PageType::L2pLeaf,
            version: PAGE_VERSION,
            key_count: 42,
            flags: 0xDEAD_BEEF,
            generation: 0x1234_5678_9ABC_DEF0,
            refcount: 7,
        };
        let mut p = Page::zeroed();
        p.write_header(&h);
        p.seal();
        let read = p.header().unwrap();
        assert_eq!(read.page_type, h.page_type);
        assert_eq!(read.version, h.version);
        assert_eq!(read.key_count, h.key_count);
        assert_eq!(read.flags, h.flags);
        assert_eq!(read.generation, h.generation);
        assert_eq!(read.refcount, h.refcount);
    }

    #[test]
    fn verify_succeeds_on_sealed_page() {
        let mut p = Page::new(PageHeader::new(PageType::L2pLeaf, 1));
        p.payload_mut()[0..4].copy_from_slice(b"test");
        p.seal();
        p.verify(7).unwrap();
    }

    #[test]
    fn verify_catches_payload_bit_flip() {
        let mut p = Page::new(PageHeader::new(PageType::L2pLeaf, 1));
        p.payload_mut()[0] = 0xAB;
        p.seal();
        assert!(p.verify(0).is_ok());

        p.bytes_mut()[100] ^= 0x01;
        match p.verify(42).unwrap_err() {
            MetaDbError::PageChecksumMismatch { page_id, .. } => {
                assert_eq!(page_id, 42);
            }
            e => panic!("wrong error: {e}"),
        }
    }

    #[test]
    fn verify_catches_header_bit_flip() {
        let mut p = Page::new(PageHeader::new(PageType::L2pLeaf, 1));
        p.seal();
        assert!(p.verify(0).is_ok());
        p.set_key_count(999); // not resealed
        assert!(matches!(
            p.verify(0).unwrap_err(),
            MetaDbError::PageChecksumMismatch { .. }
        ));
    }

    #[test]
    fn crc_is_independent_of_crc_field_contents() {
        let mut p = Page::new(PageHeader::new(PageType::L2pLeaf, 1));
        p.seal();
        let crc1 = p.compute_crc();
        put_u32_le(&mut *p.bytes, OFF_CRC, 0xFFFF_FFFF);
        let crc2 = p.compute_crc();
        assert_eq!(crc1, crc2, "CRC must be identical regardless of CRC field");
    }

    #[test]
    fn magic_mismatch_reports_page_id() {
        let mut p = Page::new(PageHeader::new(PageType::L2pLeaf, 1));
        p.seal();
        put_u32_le(&mut *p.bytes, OFF_MAGIC, 0xDEAD_BEEF);
        match p.verify(42).unwrap_err() {
            MetaDbError::PageMagicMismatch { page_id, found } => {
                assert_eq!(page_id, 42);
                assert_eq!(found, 0xDEAD_BEEF);
            }
            e => panic!("wrong error: {e}"),
        }
    }

    #[test]
    fn version_mismatch_reports_page_id() {
        let mut p = Page::new(PageHeader::new(PageType::L2pLeaf, 1));
        p.bytes_mut()[OFF_VERSION] = 99;
        p.seal();
        match p.verify(42).unwrap_err() {
            MetaDbError::PageVersionUnsupported { page_id, version } => {
                assert_eq!(page_id, 42);
                assert_eq!(version, 99);
            }
            e => panic!("wrong error: {e}"),
        }
    }

    #[test]
    fn page_type_round_trip() {
        for t in [
            PageType::Free,
            PageType::L2pLeaf,
            PageType::L2pInternal,
            PageType::LsmData,
            PageType::FreeListNode,
            PageType::Manifest,
        ] {
            assert_eq!(PageType::from_u8(t as u8).unwrap(), t);
        }
        assert!(PageType::from_u8(99).is_err());
    }

    #[test]
    fn crc_is_deterministic_and_sensitive() {
        let make = |seed: u8| {
            let mut p = Page::new(PageHeader::new(PageType::L2pLeaf, 100));
            p.payload_mut()[..8].copy_from_slice(&[seed; 8]);
            p.seal();
            p
        };
        let a1 = make(1);
        let a2 = make(1);
        let b = make(2);
        assert_eq!(a1.compute_crc(), a2.compute_crc());
        assert_ne!(a1.compute_crc(), b.compute_crc());
    }

    #[test]
    fn empty_page_verify_fails() {
        let p = Page::zeroed();
        assert!(matches!(
            p.verify(0).unwrap_err(),
            MetaDbError::PageMagicMismatch { .. }
        ));
    }

    #[test]
    fn type_header_access_is_disjoint_from_payload() {
        let mut p = Page::new(PageHeader::new(PageType::L2pLeaf, 1));
        p.type_header_mut()
            .copy_from_slice(&[0xAAu8; TYPE_HEADER_SIZE]);
        p.payload_mut().fill(0x55);
        p.seal();
        assert!(p.verify(0).is_ok());
        assert_eq!(p.type_header(), &[0xAAu8; TYPE_HEADER_SIZE]);
        assert_eq!(p.payload()[0], 0x55);
    }
}
