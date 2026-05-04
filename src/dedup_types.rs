//! Value types shared by the dedup index, dedup-reverse store, and WAL
//! op encoding.
//!
//! Lifted out of the legacy `lsm` module when the LSM dedup
//! implementation was retired; the on-the-wire shape is the same so
//! existing manifests and WAL records keep replaying.

use crate::types::Pba;

/// Backwards-compat shim for callers that used to read SST / record
/// counts from the LSM dedup index. Both indexes are now cuckoo /
/// paged-array, so every field reads zero; the shape stays so the
/// onyx-storage status surface keeps building during the migration.
/// New callers should query `Db::dedup_tier_sizes` instead.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct LsmStats {
    pub level_count: usize,
    pub total_ssts: usize,
    pub total_records: u64,
    pub memtable: MemtableStats,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct MemtableStats {
    pub active_entries: u64,
    pub frozen_entries: u64,
}

/// Length of a content hash key, in bytes.
///
/// Onyx hashes 4 KiB blocks with `xxh3_64`; collision tolerance comes
/// from a verify-on-hit step (read the original block from LV3 and
/// compare bytewise) rather than from cryptographic hash strength,
/// so an 8-byte fingerprint is enough. Birthday math at 1 PiB / 4 K
/// (≈ 2.7 × 10¹¹ unique blocks) puts the expected collision count at
/// ≈ 1900 across the entire dataset; verify makes those benign.
pub const HASH_SIZE: usize = 8;

/// Length of a dedup value payload, in bytes.
///
/// Matches `L2pValue` deliberately so the two fixed-size values can
/// share buffers where that turns out to be useful.
pub const DEDUP_VALUE_SIZE: usize = 28;

/// 8-byte content fingerprint (`xxh3_64` of the source 4 K block).
/// Plain array alias: fixed size, trivially `Copy`, and byte-wise
/// ordering matches the intended sort order.
pub type Hash8 = [u8; HASH_SIZE];

/// Opaque 28-byte dedup entry payload. Onyx stores a PBA plus
/// per-entry metadata here; metadb does not interpret the bytes.
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
    /// `Db::cleanup_dedup_for_dead_pbas` (SPEC §2.2) uses this to
    /// check "did `hash` get re-registered against a different pba
    /// since the plan ran" before emitting a `DedupDelete` tombstone.
    /// Breaking the contract means losing dedup-cleanup race
    /// protection.
    pub fn head_pba(&self) -> Pba {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&self.0[..8]);
        u64::from_be_bytes(buf)
    }
}

/// Routing for forward `dedup_index` keys (the content hash itself).
/// Apply lanes bucket WAL ops by the top bits of `hash[0]` so the
/// same hash always lands in the same lane, preserving per-key
/// ordering across concurrent dedup writes.
#[inline]
pub fn shard_for_hash(hash: &Hash8, shards: u32) -> u32 {
    debug_assert!(
        shards.is_power_of_two() && shards > 0,
        "dedup_shards must be a power of two; got {shards}"
    );
    let shift = 8u32 - shards.trailing_zeros();
    u32::from(hash[0]) >> shift
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_value_round_trip() {
        let mut bytes = [0u8; DEDUP_VALUE_SIZE];
        bytes[..8].copy_from_slice(&0xDEAD_BEEF_u64.to_be_bytes());
        let v = DedupValue::new(bytes);
        assert_eq!(v.as_bytes(), &bytes);
        assert_eq!(v.head_pba(), 0xDEAD_BEEF);
    }

    #[test]
    fn shard_for_hash_partitions_evenly() {
        let mut h = [0u8; HASH_SIZE];
        h[0] = 0;
        assert_eq!(shard_for_hash(&h, 8), 0);
        h[0] = 0xFF;
        assert_eq!(shard_for_hash(&h, 8), 7);
        // 32 shards still fit in 8 bits.
        h[0] = 0x80;
        assert_eq!(shard_for_hash(&h, 32), 16);
    }

    #[test]
    fn shard_for_hash_one_shard_routes_all_to_zero() {
        let mut h = [0u8; HASH_SIZE];
        h[0] = 0xAB;
        assert_eq!(shard_for_hash(&h, 1), 0);
    }
}
