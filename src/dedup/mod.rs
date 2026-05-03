//! Onyx-aware dedup index built around a four-tier cache:
//!
//! ```text
//! ┌───────────────────────────────────────────────────────┐
//! │ L0  in-memory 32-bit fingerprint set (full coverage)  │
//! │     fp ∉ L0  →  guaranteed MISS  (short-circuit)      │
//! │     fp ∈ L0  →  consult lower tiers                   │
//! ├───────────────────────────────────────────────────────┤
//! │ L1  in-memory hot cache, fp → (full hash, DedupValue) │
//! │     LRU bounded by `dedup_l1_cache_entries`           │
//! ├───────────────────────────────────────────────────────┤
//! │ L2  shared `PageCache` (already exists; cuckoo pages  │
//! │     read here on first touch, stay hot under LRU)     │
//! ├───────────────────────────────────────────────────────┤
//! │ L3  on-disk cuckoo hash table (stage 3.2)             │
//! └───────────────────────────────────────────────────────┘
//! ```
//!
//! L0/L1 are built on Day 3 of the metadb-restructure-v9 plan;
//! L2 reuses [`crate::cache::PageCache`] without changes; L3 lands
//! in stage 3.2 once the on-disk cuckoo layout is in place.

pub mod cuckoo;
pub mod index;
pub mod l1_cache;
pub mod sketch;

pub use cuckoo::CuckooHash;
pub use index::{DedupIndex, TierSizes};
pub use l1_cache::L1HotCache;
pub use sketch::FpSketch;

use crate::lsm::Hash32;

/// Take the first four bytes of a `Hash32` as the fingerprint.
/// `Hash32` is already uniformly distributed (SHA-256 / blake3
/// output), so any 32-bit slice is a fair fingerprint; using the
/// first bytes keeps the read cheap even without alignment.
#[inline]
pub fn fp_of(hash: &Hash32) -> u32 {
    u32::from_le_bytes([hash[0], hash[1], hash[2], hash[3]])
}

#[cfg(test)]
mod fp_tests {
    use super::*;

    #[test]
    fn fp_distinguishes_distinct_hashes() {
        let mut a = [0u8; 32];
        a[..4].copy_from_slice(&0xDEAD_BEEFu32.to_le_bytes());
        let mut b = [0u8; 32];
        b[..4].copy_from_slice(&0x1234_5678u32.to_le_bytes());
        assert_eq!(fp_of(&a), 0xDEAD_BEEF);
        assert_eq!(fp_of(&b), 0x1234_5678);
        assert_ne!(fp_of(&a), fp_of(&b));
    }

    #[test]
    fn fp_collides_when_first_bytes_match() {
        // Distinct hashes whose first 4 bytes are identical share an
        // fp. The L0 sketch must tolerate this (ref-counted entries).
        let mut a = [0u8; 32];
        let mut b = [0u8; 32];
        a[..4].copy_from_slice(&[1, 2, 3, 4]);
        b[..4].copy_from_slice(&[1, 2, 3, 4]);
        a[31] = 0xAA;
        b[31] = 0xBB;
        assert_eq!(fp_of(&a), fp_of(&b));
        assert_ne!(a, b);
    }
}
