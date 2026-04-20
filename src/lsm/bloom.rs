//! Bloom filter for the LSM read path.
//!
//! Standard double-hashing bloom: we compute two 64-bit hashes of the key
//! with xxh3 using two distinct seeds, then derive `hash_count` bit
//! positions as `h1 + i * h2` for `i` in `0..hash_count`. Equivalent to
//! running `hash_count` independent hash functions, without actually
//! evaluating a family of hashers. Kirsch–Mitzenmacher (2006) shows the
//! false-positive rate is indistinguishable from true independence when
//! `hash_count` is small.
//!
//! The filter owns its backing byte buffer but is otherwise cheap to
//! copy (just a pointer + length). It is used in two ways:
//! - `SstWriter` builds a fresh filter from the sorted record stream,
//!   then the filter bytes are laid out across one or more pages in the
//!   SST file.
//! - `SstReader` loads those page bytes back into a `BloomFilter` and
//!   serves `maybe_contains` checks before doing a binary search.

use xxhash_rust::xxh3::xxh3_64_with_seed;

use super::format::Hash32;

/// Seeds picked to be arbitrary and distinct. Changing them invalidates
/// all on-disk SSTs; treat as a format constant.
const SEED_A: u64 = 0x0123_4567_89AB_CDEF;
const SEED_B: u64 = 0xFEDC_BA98_7654_3210;

/// Fixed-size 10-bit-per-entry bloom filter with 7 hash functions. This
/// is the default configuration; it yields ≈ 1 % false-positive rate at
/// full occupancy, which is the RocksDB default.
pub const DEFAULT_BITS_PER_ENTRY: u32 = 10;

/// Number of hash functions for a given bits-per-entry value. Derived
/// from the classic `k = (bits/entry) * ln(2)` formula, rounded to the
/// nearest integer and clamped to [1, 16]. Evaluated at compile time so
/// the writer and reader cannot disagree.
pub const fn hash_count_for(bits_per_entry: u32) -> u32 {
    // k ≈ bits * ln(2) = bits * 0.6931…; use integer math with a
    // round-half-up adjustment (+500/1000) to avoid floating point in a
    // const context. Accurate enough within the range we care about
    // (1..=16 bits).
    let k = (bits_per_entry as u64 * 693 + 500) / 1000;
    let k = if k == 0 { 1 } else { k as u32 };
    if k > 16 { 16 } else { k }
}

/// In-memory bloom filter.
#[derive(Clone, Debug)]
pub struct BloomFilter {
    bits: Vec<u8>,
    bit_count: u32,
    hash_count: u32,
}

impl BloomFilter {
    /// Build an empty filter sized for `expected_entries` with the given
    /// bits-per-entry target. The bit count is rounded up to a multiple
    /// of 8 so the storage aligns on byte boundaries. An empty filter
    /// (expected_entries = 0) is allowed and always reports miss.
    pub fn with_capacity(expected_entries: usize, bits_per_entry: u32) -> Self {
        let bits = expected_entries
            .saturating_mul(bits_per_entry as usize)
            .max(8);
        // Round up to a multiple of 8 so the byte count is exact.
        let bit_count = (bits.div_ceil(8) * 8) as u32;
        let byte_count = (bit_count / 8) as usize;
        Self {
            bits: vec![0u8; byte_count],
            bit_count,
            hash_count: hash_count_for(bits_per_entry).max(1),
        }
    }

    /// Build a filter from existing bytes and parameters. Used by the
    /// reader. The byte buffer must equal `bit_count / 8` bytes exactly.
    pub fn from_parts(bits: Vec<u8>, bit_count: u32, hash_count: u32) -> Self {
        debug_assert_eq!(bits.len(), (bit_count / 8) as usize);
        Self {
            bits,
            bit_count,
            hash_count,
        }
    }

    /// Record `hash` as present.
    pub fn insert(&mut self, hash: &Hash32) {
        for bit in bit_positions(hash, self.bit_count, self.hash_count) {
            let byte = (bit / 8) as usize;
            let mask = 1u8 << (bit % 8);
            self.bits[byte] |= mask;
        }
    }

    /// `true` if `hash` might be in the set; `false` means definitely
    /// not. Never false-negative, always false-positive-tolerant.
    pub fn maybe_contains(&self, hash: &Hash32) -> bool {
        for bit in bit_positions(hash, self.bit_count, self.hash_count) {
            let byte = (bit / 8) as usize;
            let mask = 1u8 << (bit % 8);
            if self.bits[byte] & mask == 0 {
                return false;
            }
        }
        true
    }

    /// Raw bit buffer, suitable for writing to a page.
    pub fn bytes(&self) -> &[u8] {
        &self.bits
    }

    /// Byte count of the bit buffer.
    pub fn byte_len(&self) -> usize {
        self.bits.len()
    }

    /// Total bit count.
    pub fn bit_count(&self) -> u32 {
        self.bit_count
    }

    /// Number of hash functions used.
    pub fn hash_count(&self) -> u32 {
        self.hash_count
    }
}

fn bit_positions(hash: &Hash32, bit_count: u32, hash_count: u32) -> impl Iterator<Item = u32> {
    let h1 = xxh3_64_with_seed(hash, SEED_A);
    let h2 = xxh3_64_with_seed(hash, SEED_B);
    let bits = bit_count as u64;
    (0..hash_count).map(move |i| {
        let h = h1.wrapping_add((i as u64).wrapping_mul(h2));
        (h % bits) as u32
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(seed: u64) -> Hash32 {
        // Deterministic variety in all 32 bytes so the two seeds really
        // produce different h1/h2.
        let mut out = [0u8; 32];
        out[..8].copy_from_slice(&seed.to_le_bytes());
        out[8..16].copy_from_slice(&seed.wrapping_mul(0xDEAD).to_le_bytes());
        out[16..24].copy_from_slice(&seed.wrapping_mul(0xBEEF).to_le_bytes());
        out[24..].copy_from_slice(&seed.wrapping_add(0xCAFE).to_le_bytes());
        out
    }

    #[test]
    fn hash_count_for_defaults_to_seven() {
        // 10 * 0.6931… ≈ 6.93, rounds to 7.
        assert_eq!(hash_count_for(DEFAULT_BITS_PER_ENTRY), 7);
    }

    #[test]
    fn hash_count_clamps_to_one_minimum() {
        assert_eq!(hash_count_for(1), 1);
        assert_eq!(hash_count_for(0), 1);
    }

    #[test]
    fn empty_filter_always_misses() {
        let bf = BloomFilter::with_capacity(0, DEFAULT_BITS_PER_ENTRY);
        assert!(!bf.maybe_contains(&h(0)));
        assert!(!bf.maybe_contains(&h(1)));
    }

    #[test]
    fn inserted_keys_are_reported_as_present() {
        let mut bf = BloomFilter::with_capacity(1_000, DEFAULT_BITS_PER_ENTRY);
        for i in 0..500u64 {
            bf.insert(&h(i));
        }
        for i in 0..500u64 {
            assert!(bf.maybe_contains(&h(i)), "insert {i} went missing");
        }
    }

    #[test]
    fn false_positive_rate_is_under_three_percent() {
        let mut bf = BloomFilter::with_capacity(10_000, DEFAULT_BITS_PER_ENTRY);
        for i in 0..10_000u64 {
            bf.insert(&h(i));
        }
        let mut false_positives = 0;
        let trials = 10_000u64;
        for i in 10_000..10_000 + trials {
            if bf.maybe_contains(&h(i)) {
                false_positives += 1;
            }
        }
        let rate = false_positives as f64 / trials as f64;
        // 10 bits/entry → ~1 % theoretical; allow generous headroom.
        assert!(rate < 0.03, "false positive rate {rate:.4} exceeded 3%");
    }

    #[test]
    fn from_parts_round_trip() {
        let mut a = BloomFilter::with_capacity(100, DEFAULT_BITS_PER_ENTRY);
        for i in 0..50u64 {
            a.insert(&h(i));
        }
        let bytes = a.bytes().to_vec();
        let b = BloomFilter::from_parts(bytes, a.bit_count(), a.hash_count());
        for i in 0..50u64 {
            assert!(b.maybe_contains(&h(i)));
        }
    }

    #[test]
    fn bit_count_rounds_up_to_byte_multiple() {
        let bf = BloomFilter::with_capacity(3, 1);
        assert_eq!(bf.byte_len() * 8, bf.bit_count() as usize);
        assert_eq!(bf.bit_count() % 8, 0);
    }
}
