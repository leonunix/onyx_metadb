//! LSM record format.
//!
//! Each record is exactly 64 bytes so 63 records fit into a 4032-byte
//! page payload with zero slack:
//!
//! ```text
//! offset  size  field
//! ------  ----  ---------------------------
//!   0     32    hash (SHA-256 content hash; sort key)
//!  32      1    kind: 1 = Put, 2 = Delete (tombstone)
//!  33     28    value (opaque `DedupValue`; undefined for Delete)
//!  61      3    padding (must be zero)
//! ```
//!
//! Records are sorted by the 32-byte hash in lexicographic (ascending)
//! order. Because the hash is the first field, two records can be ordered
//! or binary-searched by straight `memcmp` across the first 32 bytes.

use crate::page::PAGE_PAYLOAD_SIZE;

/// Length of a content hash key, in bytes (SHA-256 → 32).
pub const HASH_SIZE: usize = 32;

/// Length of a dedup value payload, in bytes.
///
/// Matches `L2pValue` deliberately so the two fixed-size values can share
/// buffers where that turns out to be useful.
pub const DEDUP_VALUE_SIZE: usize = 28;

/// On-disk size of a single LSM record, in bytes. A multiple of 8 so the
/// record array starts 8-byte aligned and 63 records fill a 4 KiB page
/// payload exactly.
pub const LSM_RECORD_SIZE: usize = 64;

/// Number of records that fit in a single 4 KiB page payload.
pub const RECORDS_PER_PAGE: usize = PAGE_PAYLOAD_SIZE / LSM_RECORD_SIZE;

const OFF_HASH: usize = 0;
const OFF_KIND: usize = 32;
const OFF_VALUE: usize = 33;
const OFF_PADDING: usize = 61;
const PADDING_LEN: usize = 3;

/// Record kind tag: the record adds or updates `hash → value`.
pub const KIND_PUT: u8 = 1;

/// Record kind tag: the record tombstones `hash`. The value bytes are
/// undefined (written as zero).
pub const KIND_DELETE: u8 = 2;

const _: () = {
    assert!(OFF_HASH + HASH_SIZE == OFF_KIND);
    assert!(OFF_KIND + 1 == OFF_VALUE);
    assert!(OFF_VALUE + DEDUP_VALUE_SIZE == OFF_PADDING);
    assert!(OFF_PADDING + PADDING_LEN == LSM_RECORD_SIZE);
    assert!(LSM_RECORD_SIZE * RECORDS_PER_PAGE == PAGE_PAYLOAD_SIZE);
    assert!(RECORDS_PER_PAGE == 63);
};

/// 32-byte SHA-256 content hash. Plain array alias: fixed size, trivially
/// `Copy`, and byte-wise ordering matches the intended sort order.
pub type Hash32 = [u8; HASH_SIZE];

/// Opaque 28-byte dedup entry payload. Onyx stores a PBA plus per-entry
/// metadata here; metadb does not interpret the bytes.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct DedupValue(pub [u8; DEDUP_VALUE_SIZE]);

impl DedupValue {
    pub const fn new(bytes: [u8; DEDUP_VALUE_SIZE]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; DEDUP_VALUE_SIZE] {
        &self.0
    }

    pub const fn zero() -> Self {
        Self([0u8; DEDUP_VALUE_SIZE])
    }

    /// Onyx encoding contract: the first 8 bytes of a `DedupValue`
    /// encode the big-endian `Pba` the hash points at (head-8B PBA,
    /// mirroring [`L2pValue::head_pba`](crate::paged::L2pValue::head_pba)).
    ///
    /// `Db::cleanup_dedup_for_dead_pbas` (SPEC §2.2) uses this to check
    /// "did `hash` get re-registered against a different pba since the
    /// plan ran" before emitting a `DedupDelete` tombstone. Breaking
    /// the contract means losing dedup-cleanup race protection.
    pub fn head_pba(&self) -> crate::types::Pba {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.0[..8]);
        u64::from_be_bytes(buf)
    }
}

/// One serialized 64-byte record.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Record([u8; LSM_RECORD_SIZE]);

impl Record {
    /// Build a `PUT` record.
    pub fn put(hash: &Hash32, value: &DedupValue) -> Self {
        let mut r = [0u8; LSM_RECORD_SIZE];
        r[OFF_HASH..OFF_HASH + HASH_SIZE].copy_from_slice(hash);
        r[OFF_KIND] = KIND_PUT;
        r[OFF_VALUE..OFF_VALUE + DEDUP_VALUE_SIZE].copy_from_slice(&value.0);
        Self(r)
    }

    /// Build a `DELETE` (tombstone) record. Value bytes are left zero.
    pub fn tombstone(hash: &Hash32) -> Self {
        let mut r = [0u8; LSM_RECORD_SIZE];
        r[OFF_HASH..OFF_HASH + HASH_SIZE].copy_from_slice(hash);
        r[OFF_KIND] = KIND_DELETE;
        Self(r)
    }

    /// Parse a record from a 64-byte slice. Fails if the kind byte is
    /// unrecognized or the padding is nonzero.
    pub fn from_bytes(bytes: &[u8; LSM_RECORD_SIZE]) -> Result<Self, RecordDecodeError> {
        match bytes[OFF_KIND] {
            KIND_PUT | KIND_DELETE => {}
            other => return Err(RecordDecodeError::UnknownKind(other)),
        }
        for &p in &bytes[OFF_PADDING..OFF_PADDING + PADDING_LEN] {
            if p != 0 {
                return Err(RecordDecodeError::NonZeroPadding);
            }
        }
        Ok(Self(*bytes))
    }

    /// The raw 64 bytes of this record, suitable for writing to disk.
    pub fn as_bytes(&self) -> &[u8; LSM_RECORD_SIZE] {
        &self.0
    }

    /// Hash key (first 32 bytes).
    pub fn hash(&self) -> &Hash32 {
        (&self.0[OFF_HASH..OFF_HASH + HASH_SIZE])
            .try_into()
            .unwrap()
    }

    /// Kind tag (byte 32).
    pub fn kind(&self) -> u8 {
        self.0[OFF_KIND]
    }

    pub fn is_put(&self) -> bool {
        self.kind() == KIND_PUT
    }

    pub fn is_delete(&self) -> bool {
        self.kind() == KIND_DELETE
    }

    /// Copy the value payload out. For tombstones the bytes are zero; the
    /// caller is expected to check [`Record::kind`] first.
    pub fn value(&self) -> DedupValue {
        let mut v = [0u8; DEDUP_VALUE_SIZE];
        v.copy_from_slice(&self.0[OFF_VALUE..OFF_VALUE + DEDUP_VALUE_SIZE]);
        DedupValue(v)
    }
}

/// Errors that can arise decoding a record from on-disk bytes.
#[derive(Debug, PartialEq, Eq)]
pub enum RecordDecodeError {
    UnknownKind(u8),
    NonZeroPadding,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(n: u8) -> Hash32 {
        let mut x = [0u8; HASH_SIZE];
        x[0] = n;
        x
    }

    fn v(n: u8) -> DedupValue {
        let mut x = [0u8; DEDUP_VALUE_SIZE];
        x[0] = n;
        DedupValue(x)
    }

    #[test]
    fn put_round_trip() {
        let r = Record::put(&h(7), &v(9));
        assert!(r.is_put());
        assert!(!r.is_delete());
        assert_eq!(r.hash(), &h(7));
        assert_eq!(r.value(), v(9));
        let decoded = Record::from_bytes(r.as_bytes()).unwrap();
        assert_eq!(decoded, r);
    }

    #[test]
    fn tombstone_round_trip() {
        let r = Record::tombstone(&h(42));
        assert!(r.is_delete());
        assert!(!r.is_put());
        assert_eq!(r.hash(), &h(42));
        assert_eq!(r.value(), DedupValue::zero());
        let decoded = Record::from_bytes(r.as_bytes()).unwrap();
        assert_eq!(decoded, r);
    }

    #[test]
    fn decode_rejects_unknown_kind() {
        let mut bytes = [0u8; LSM_RECORD_SIZE];
        bytes[OFF_KIND] = 0xFF;
        match Record::from_bytes(&bytes) {
            Err(RecordDecodeError::UnknownKind(0xFF)) => {}
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn decode_rejects_nonzero_padding() {
        let mut r = Record::put(&h(1), &v(2));
        r.0[OFF_PADDING] = 0xAA;
        match Record::from_bytes(&r.0) {
            Err(RecordDecodeError::NonZeroPadding) => {}
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn records_are_hash_comparable_as_bytes() {
        let a = Record::put(&h(1), &v(0xFF));
        let b = Record::put(&h(2), &v(0x00));
        assert!(a.as_bytes() < b.as_bytes());
        assert!(a.hash() < b.hash());
    }

    #[test]
    fn constants_match_page_payload() {
        assert_eq!(LSM_RECORD_SIZE * RECORDS_PER_PAGE, PAGE_PAYLOAD_SIZE);
        assert_eq!(RECORDS_PER_PAGE, 63);
    }
}
