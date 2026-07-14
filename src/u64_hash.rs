//! Fast deterministic hashing for internal `u64` identifiers.
//!
//! Page IDs and PBAs are often aligned or allocated in arithmetic sequences,
//! so using their value directly as the hash leaves the low bucket bits badly
//! distributed. SplitMix64's finalizer is a small, high-quality integer mixer
//! that avoids that pattern without the per-map random-key setup of
//! `RandomState`. These maps only contain trusted internal identifiers; this
//! is not intended as a hash-flooding defense for untrusted input.

use std::hash::{BuildHasherDefault, Hasher};

#[derive(Default)]
pub struct SplitMix64Hasher(u64);

impl Hasher for SplitMix64Hasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        // The specialized maps use `u64`, whose `Hash` implementation calls
        // `write_u64`. Keep a deterministic fallback for completeness.
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        for byte in bytes {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        self.0 = hash;
    }

    fn write_u64(&mut self, value: u64) {
        self.0 = mix_u64(value);
    }
}

pub(crate) type U64BuildHasher = BuildHasherDefault<SplitMix64Hasher>;

#[inline]
fn mix_u64(value: u64) -> u64 {
    let mut x = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn hash_u64(value: u64) -> u64 {
        let mut hasher = SplitMix64Hasher::default();
        hasher.write_u64(value);
        hasher.finish()
    }

    #[test]
    fn splitmix64_is_deterministic() {
        assert_eq!(hash_u64(0), 0xe220_a839_7b1d_cdaf);
        assert_eq!(hash_u64(1), 0x910a_2dec_8902_5cc1);
        assert_eq!(hash_u64(0), hash_u64(0));
    }

    #[test]
    fn splitmix64_spreads_aligned_identifier_low_bits() {
        let hashes: HashSet<_> = (0..4096u64).map(|id| hash_u64(id << 20)).collect();
        assert_eq!(hashes.len(), 4096, "the mixer must not collapse u64 keys");

        let low_buckets: HashSet<_> = hashes.iter().map(|hash| hash & 0xff).collect();
        assert!(
            low_buckets.len() >= 240,
            "aligned keys only reached {} of 256 low-bit buckets",
            low_buckets.len()
        );
    }
}
