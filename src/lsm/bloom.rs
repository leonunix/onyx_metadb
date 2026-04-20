//! Bloom filter for the LSM read path.
//!
//! Double-hashing bloom: derive two 64-bit seeds `h1`, `h2` from the
//! key and compute `hash_count` bit positions as `h1 + i * h2` for `i`
//! in `0..hash_count`. Equivalent to running `hash_count` independent
//! hash functions under Kirsch–Mitzenmacher (2006) when the two seeds
//! are independent and uniformly distributed.
//!
//! # Where do `h1` and `h2` come from?
//!
//! metadb's LSM keys are **never raw user input**. They are always a
//! region of a SHA-256 content hash:
//!
//! - `dedup_index` key: the full 32-byte SHA-256.
//! - `dedup_reverse` key: `[pba: 8 B BE][hash[..24]]`. The first 8
//!   bytes are a PBA (non-uniform), but bytes 8..32 are still raw hash
//!   output.
//!
//! The bloom extracts `h1` and `h2` from bytes `[8..16]` and `[16..24]`
//! of the key. For `dedup_index`, those are uniform. For
//! `dedup_reverse`, they are also uniform (they come from the content
//! hash, not from the PBA prefix). This means we can skip the xxh3 we
//! used to run over the 32-byte key — it was redundant work over an
//! already-uniform input. Savings: ~one xxh3 call per bloom query.
//!
//! Caveat: this hardcodes "the last 24 bytes of a Hash32 are uniformly
//! random". If a future caller ever uses an LSM keyed on non-hash
//! bytes, the FP rate will be off. That would require redesigning
//! `BloomFilter` (and its `from_parts` / on-disk layout) anyway, so
//! this is a deliberate purpose-built choice for the Onyx workload.
//!
//! The filter owns its backing byte buffer but is otherwise cheap to
//! copy. `SstWriter` builds one from the sorted record stream;
//! `SstReader` reloads one via `from_parts` before doing point lookups.

use super::format::Hash32;

/// Byte offset of the 64-bit value used as `h1` in double hashing.
/// Points at `hash[8..16]`. See module docs for why we skip the first
/// 8 bytes (they may be a PBA prefix in `dedup_reverse`).
const H1_OFFSET: usize = 8;
/// Byte offset of `h2`. Points at `hash[16..24]`.
const H2_OFFSET: usize = 16;

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
    // Extract two 64-bit seeds directly from the key. See module docs:
    // metadb LSM keys always have 24 bytes of uniform content-hash
    // output starting at byte 8, so we can skip a round of xxh3.
    let h1 = u64::from_le_bytes(hash[H1_OFFSET..H1_OFFSET + 8].try_into().unwrap());
    let h2 = u64::from_le_bytes(hash[H2_OFFSET..H2_OFFSET + 8].try_into().unwrap());
    let bits = bit_count as u64;
    (0..hash_count).map(move |i| {
        let h = h1.wrapping_add((i as u64).wrapping_mul(h2));
        (h % bits) as u32
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use xxhash_rust::xxh3::xxh3_64_with_seed;

    /// Produce a SHA-256-shaped test key from a small integer seed.
    /// Bytes 8..24 are filled with diffused seed bits (via xxh3) so
    /// they have the same uniform distribution as real content-hash
    /// output. That matches what the bloom expects in production —
    /// see the module docs — so FP-rate tests here are meaningful.
    fn h(seed: u64) -> Hash32 {
        let mut out = [0u8; 32];
        out[..8].copy_from_slice(&seed.to_le_bytes());
        let h1 = xxh3_64_with_seed(&seed.to_le_bytes(), 0xA5A5_A5A5);
        let h2 = xxh3_64_with_seed(&seed.to_le_bytes(), 0x5A5A_5A5A);
        out[8..16].copy_from_slice(&h1.to_le_bytes());
        out[16..24].copy_from_slice(&h2.to_le_bytes());
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
