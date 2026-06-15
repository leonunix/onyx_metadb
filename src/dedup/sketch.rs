//! L0: cuckoo-filter approximate-membership store.
//!
//! Replaces the legacy `HashMap<u32, u32>` ref-counted sketch. The old
//! sketch had two scaling problems:
//!
//! 1. The `u32` fingerprint domain (~4.3 G distinct values) saturates
//!    near 4 G live entries, i.e. ~16 TiB of 4 KiB blocks at the cuckoo
//!    load-factor target. Past that point every fingerprint is shared
//!    by some live entry, `contains(fp) == true` for every probe, and
//!    the L0 short-circuit collapses to a constant `yes`.
//! 2. `HashMap<u32, u32>` costs ≈ 24 B per entry once load factor and
//!    bucket overhead are accounted for, so even at smaller scales it
//!    is a memory hog relative to its information content.
//!
//! The cuckoo filter:
//!
//! - stores **16-bit fingerprints** in **4-slot buckets** packed into
//!   one `u64` per bucket;
//! - has a false-positive rate ≈ `4 × 2^-16` ≈ 0.006 %;
//! - costs ≈ 2.5 B per inserted item at the 0.95 load-factor target;
//! - supports `remove` because each insertion claims a slot and each
//!   matching `remove` frees one slot — the same multiplicity
//!   semantics the old ref-counted `HashMap` provided.
//!
//! # Saturation
//!
//! A cuckoo filter cannot grow once allocated. If a kick chain
//! exhausts [`MAX_KICKS`] we mark the filter `saturated` and have
//! `contains` return `true` unconditionally. This is the *safe*
//! degradation: lookups still answer correctly because L1 / L3 sit
//! behind L0, the only loss is that we stop short-circuiting the
//! all-miss path. Operators detect the transition via
//! [`FpSketch::is_saturated`] and resize on the next open.

use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::RwLock;
use xxhash_rust::xxh3::xxh3_64_with_seed;

const SLOTS_PER_BUCKET: usize = 4;
const SLOT_BITS: u32 = 16;
const SLOT_MASK: u64 = (1u64 << SLOT_BITS) - 1;
/// Maximum number of relocations a single `insert` will attempt before
/// declaring the filter saturated.
const MAX_KICKS: u32 = 500;
/// Target load factor used when sizing the bucket array from a
/// requested capacity. Cuckoo filters become unstable above ≈ 0.95
/// because kick chains balloon.
const LOAD_TARGET: f64 = 0.95;
/// Fallback capacity for `new()` callers that did not pre-size. Most
/// of metadb's call sites use [`FpSketch::with_capacity`]; keep this
/// small so accidental `FpSketch::new()` does not allocate megabytes.
const DEFAULT_CAPACITY: usize = 4 * 1024;

const FP_SEED: u64 = 0xFB1E_AA0F_1A91_E700;
const BUCKET_SEED: u64 = 0xB0CC_EE5E_EDBA_BE00;
const ALT_SEED: u64 = 0xA175_EEDB_ABEF_AC00;

pub struct FpSketch {
    inner: RwLock<Inner>,
    saturated: AtomicBool,
}

struct Inner {
    /// One `u64` per bucket = 4 × 16-bit slots. Slot value `0` is the
    /// vacant sentinel; [`fingerprint_of`] never returns `0`.
    buckets: Vec<u64>,
    bucket_count: usize,
    bucket_mask: u64,
    occupancy: usize,
    /// xorshift64 state for tie-breaking which candidate bucket and
    /// which slot to evict during a kick. Inlined to avoid pulling
    /// `rand`'s `small_rng` feature just for this.
    rng_state: u64,
}

#[inline]
fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

impl FpSketch {
    /// Build a sketch with the default capacity. Prefer
    /// [`FpSketch::with_capacity`] in production code; this exists so
    /// the trait-style `Default` and the `Db::create` helpers remain
    /// terse in tests.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Build a sketch with enough buckets to fit `capacity` entries
    /// at the target load factor. Capacity is rounded up to the next
    /// power of two for fast modular arithmetic.
    pub fn with_capacity(capacity: usize) -> Self {
        let target_buckets =
            ((capacity.max(1) as f64) / (SLOTS_PER_BUCKET as f64) / LOAD_TARGET).ceil() as usize;
        let bucket_count = target_buckets.next_power_of_two().max(2);
        let bucket_mask = (bucket_count as u64) - 1;
        Self {
            inner: RwLock::new(Inner {
                buckets: vec![0u64; bucket_count],
                bucket_count,
                bucket_mask,
                occupancy: 0,
                rng_state: 0xC0FF_EEF1_17E7_0C00,
            }),
            saturated: AtomicBool::new(false),
        }
    }

    /// `true` iff at least one inserted item with this fingerprint is
    /// currently in the filter. False positives are bounded by
    /// ≈ `4 × 2^-16`; false negatives are impossible while not
    /// saturated.
    pub fn contains(&self, item: u32) -> bool {
        if self.saturated.load(Ordering::Relaxed) {
            return true;
        }
        let g = self.inner.read();
        let fp = fingerprint_of(item);
        let b1 = bucket_for(item, g.bucket_mask);
        if bucket_has_fp(&g.buckets, b1, fp) {
            return true;
        }
        let b2 = alt_bucket(b1, fp, g.bucket_mask);
        bucket_has_fp(&g.buckets, b2, fp)
    }

    /// Batched `contains`. Holds the read lock once across `items`.
    pub fn contains_batch(&self, items: &[u32]) -> Vec<bool> {
        if self.saturated.load(Ordering::Relaxed) {
            return vec![true; items.len()];
        }
        let g = self.inner.read();
        items
            .iter()
            .map(|&item| {
                let fp = fingerprint_of(item);
                let b1 = bucket_for(item, g.bucket_mask);
                if bucket_has_fp(&g.buckets, b1, fp) {
                    return true;
                }
                let b2 = alt_bucket(b1, fp, g.bucket_mask);
                bucket_has_fp(&g.buckets, b2, fp)
            })
            .collect()
    }

    /// Reserve a slot for this item. Two `insert` calls for the same
    /// item occupy two slots; this matches the legacy ref-counted
    /// behaviour and lets `remove` work correctly under fingerprint
    /// collisions (callers track multiplicity per item, not per
    /// fingerprint).
    pub fn insert(&self, item: u32) {
        if self.saturated.load(Ordering::Relaxed) {
            return;
        }
        let mut g = self.inner.write();
        let fp = fingerprint_of(item);
        let b1 = bucket_for(item, g.bucket_mask);
        if try_place(&mut g.buckets, b1, fp) {
            g.occupancy += 1;
            return;
        }
        let b2 = alt_bucket(b1, fp, g.bucket_mask);
        if try_place(&mut g.buckets, b2, fp) {
            g.occupancy += 1;
            return;
        }
        // Both candidate buckets full: kick.
        let mut current_b = if xorshift64(&mut g.rng_state) & 1 == 0 {
            b1
        } else {
            b2
        };
        let mut current_fp = fp;
        for _ in 0..MAX_KICKS {
            let slot = (xorshift64(&mut g.rng_state) as usize) % SLOTS_PER_BUCKET;
            let evicted_fp = read_slot(&g.buckets, current_b, slot);
            write_slot(&mut g.buckets, current_b, slot, current_fp);
            current_fp = evicted_fp;
            current_b = alt_bucket(current_b, current_fp, g.bucket_mask);
            if try_place(&mut g.buckets, current_b, current_fp) {
                g.occupancy += 1;
                return;
            }
        }
        // Filter saturated. The last evicted fingerprint has no home
        // in the array; flip the flag and stop. `contains` will return
        // `true` from now on, so we trade L0 short-circuit value for
        // correctness. The bucket array is left in a self-consistent
        // state (every slot still holds a real previously-inserted
        // fingerprint or 0).
        self.saturated.store(true, Ordering::Relaxed);
    }

    /// Free one slot matching `item`. No-op when `item` was never
    /// inserted (or already removed), matching the legacy semantics.
    /// Once the filter is saturated we cannot reliably locate the
    /// right slot, so removes are silently dropped — the saturated
    /// flag means `contains` already short-circuits to `true`.
    pub fn remove(&self, item: u32) {
        if self.saturated.load(Ordering::Relaxed) {
            return;
        }
        let mut g = self.inner.write();
        let fp = fingerprint_of(item);
        let b1 = bucket_for(item, g.bucket_mask);
        if try_clear(&mut g.buckets, b1, fp) {
            g.occupancy -= 1;
            return;
        }
        let b2 = alt_bucket(b1, fp, g.bucket_mask);
        if try_clear(&mut g.buckets, b2, fp) {
            g.occupancy -= 1;
        }
    }

    /// Number of slots currently occupied. Equal to total inserts
    /// minus total successful removes (so colliding inserts that share
    /// a fingerprint each count as one slot, matching the old ref
    /// count). Stops being accurate once the filter is saturated.
    pub fn len(&self) -> usize {
        self.inner.read().occupancy
    }

    pub fn try_len(&self) -> Option<usize> {
        self.inner.try_read().map(|inner| inner.occupancy)
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().occupancy == 0
    }

    /// Approximate in-memory cost. Bucket array dominates; per-bucket
    /// constants (8 B) are exact. Used by the operator-facing status
    /// report.
    pub fn approx_bytes(&self) -> usize {
        self.inner.read().bucket_count * std::mem::size_of::<u64>()
    }

    pub fn try_approx_bytes(&self) -> Option<usize> {
        self.inner
            .try_read()
            .map(|inner| inner.bucket_count * std::mem::size_of::<u64>())
    }

    /// Reset to empty. Used when a fresh database is created or when
    /// L0 is being rebuilt from L3.
    pub fn clear(&self) {
        let mut g = self.inner.write();
        for b in g.buckets.iter_mut() {
            *b = 0;
        }
        g.occupancy = 0;
        self.saturated.store(false, Ordering::Relaxed);
    }

    /// `true` once a kick chain has exceeded [`MAX_KICKS`]; from this
    /// point `contains` returns `true` unconditionally and `remove`
    /// is a no-op. Operators surface this in metrics.
    pub fn is_saturated(&self) -> bool {
        self.saturated.load(Ordering::Relaxed)
    }

    /// Bucket count (power of two). Exposed for tests and metrics.
    pub fn bucket_count(&self) -> usize {
        self.inner.read().bucket_count
    }
}

impl Default for FpSketch {
    fn default() -> Self {
        Self::new()
    }
}

#[inline]
fn fingerprint_of(item: u32) -> u16 {
    let h = xxh3_64_with_seed(&item.to_le_bytes(), FP_SEED);
    let fp = (h & SLOT_MASK) as u16;
    // Reserve `0` as the vacant sentinel.
    if fp == 0 { 1 } else { fp }
}

#[inline]
fn bucket_for(item: u32, mask: u64) -> usize {
    let h = xxh3_64_with_seed(&item.to_le_bytes(), BUCKET_SEED);
    (h & mask) as usize
}

#[inline]
fn alt_bucket(b1: usize, fp: u16, mask: u64) -> usize {
    let h = xxh3_64_with_seed(&fp.to_le_bytes(), ALT_SEED);
    let alt_offset = (h & mask) as usize;
    (b1 ^ alt_offset) & (mask as usize)
}

#[inline]
fn read_slot(buckets: &[u64], bucket: usize, slot: usize) -> u16 {
    let raw = buckets[bucket];
    ((raw >> (slot as u32 * SLOT_BITS)) & SLOT_MASK) as u16
}

#[inline]
fn write_slot(buckets: &mut [u64], bucket: usize, slot: usize, fp: u16) {
    let shift = slot as u32 * SLOT_BITS;
    let cleared = buckets[bucket] & !(SLOT_MASK << shift);
    buckets[bucket] = cleared | ((fp as u64) << shift);
}

#[inline]
fn bucket_has_fp(buckets: &[u64], bucket: usize, fp: u16) -> bool {
    let raw = buckets[bucket];
    for slot in 0..SLOTS_PER_BUCKET {
        if ((raw >> (slot as u32 * SLOT_BITS)) & SLOT_MASK) as u16 == fp {
            return true;
        }
    }
    false
}

#[inline]
fn try_place(buckets: &mut [u64], bucket: usize, fp: u16) -> bool {
    for slot in 0..SLOTS_PER_BUCKET {
        if read_slot(buckets, bucket, slot) == 0 {
            write_slot(buckets, bucket, slot, fp);
            return true;
        }
    }
    false
}

#[inline]
fn try_clear(buckets: &mut [u64], bucket: usize, fp: u16) -> bool {
    for slot in 0..SLOTS_PER_BUCKET {
        if read_slot(buckets, bucket, slot) == fp {
            write_slot(buckets, bucket, slot, 0);
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_sketch_contains_nothing() {
        let s = FpSketch::with_capacity(64);
        // 4 random fingerprints — at FPR ≈ 0.006 % the probability of
        // even one false positive on an empty filter is negligible.
        for fp in [0xDEAD_BEEFu32, 0xCAFE_F00D, 0x1234_5678, 0xABCD_1234] {
            assert!(!s.contains(fp));
        }
        assert_eq!(s.len(), 0);
        assert!(s.is_empty());
    }

    #[test]
    fn insert_then_contains() {
        let s = FpSketch::with_capacity(64);
        s.insert(0xDEAD_BEEF);
        assert!(s.contains(0xDEAD_BEEF));
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn distinct_keys_dont_falsely_collide_at_low_load() {
        // At 64 bucket-slots with 4 inserts, FPR is dominated by the
        // 16-bit fingerprint domain rather than the bucket map.
        let s = FpSketch::with_capacity(64);
        s.insert(0xDEAD_BEEF);
        // These four probes are all distinct from the inserted item;
        // none should collide on fingerprint with extremely high
        // probability.
        assert!(!s.contains(0xCAFE_F00D));
        assert!(!s.contains(0x1234_5678));
        assert!(!s.contains(0xABCD_1234));
    }

    #[test]
    fn remove_decrements_then_drops() {
        let s = FpSketch::with_capacity(64);
        s.insert(7);
        s.insert(7);
        assert!(s.contains(7));
        assert_eq!(s.len(), 2);
        s.remove(7);
        assert!(s.contains(7), "still one slot live");
        assert_eq!(s.len(), 1);
        s.remove(7);
        assert!(!s.contains(7), "all slots reclaimed");
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn remove_missing_is_noop() {
        let s = FpSketch::with_capacity(64);
        s.remove(42);
        assert_eq!(s.len(), 0);
        // And after a real insert / remove, we should still be at 0.
        s.insert(7);
        s.remove(7);
        s.remove(7); // double remove — already at 0
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn many_distinct_inserts_grow_len() {
        let s = FpSketch::with_capacity(8 * 1024);
        for i in 0..1000u32 {
            s.insert(i);
        }
        assert_eq!(s.len(), 1000);
        assert!(!s.is_saturated());
        for i in 0..500u32 {
            s.remove(i);
        }
        assert_eq!(s.len(), 500);
    }

    #[test]
    fn many_distinct_inserts_all_recallable() {
        let s = FpSketch::with_capacity(8 * 1024);
        for i in 0..1000u32 {
            s.insert(i);
        }
        for i in 0..1000u32 {
            assert!(s.contains(i), "lost insert for fp={i}");
        }
    }

    #[test]
    fn batch_contains_matches_per_item() {
        let s = FpSketch::with_capacity(1024);
        let inserted: Vec<u32> = (0..100u32).collect();
        for &i in &inserted {
            s.insert(i);
        }
        let probes: Vec<u32> = (0..200u32).collect();
        let batch = s.contains_batch(&probes);
        for (i, hit) in probes.iter().zip(batch.iter()) {
            // Inserted ones must hit; non-inserted ones may have a tiny
            // false-positive rate. Assert truth on the inserted side.
            if (*i as usize) < 100 {
                assert!(hit, "insert {i} missing from batch result");
            }
            assert_eq!(*hit, s.contains(*i));
        }
    }

    #[test]
    fn clear_drops_everything() {
        let s = FpSketch::with_capacity(128);
        s.insert(1);
        s.insert(2);
        s.clear();
        assert!(s.is_empty());
        assert!(!s.contains(1));
        assert!(!s.contains(2));
    }

    #[test]
    fn capacity_rounds_up_to_power_of_two() {
        let s = FpSketch::with_capacity(100);
        // 100 / 4 / 0.95 ≈ 27 → next pow2 = 32.
        assert_eq!(s.bucket_count(), 32);
    }

    #[test]
    fn approx_bytes_scales_with_bucket_count() {
        let small = FpSketch::with_capacity(64);
        let big = FpSketch::with_capacity(64 * 1024);
        assert!(big.approx_bytes() > small.approx_bytes());
        // Each bucket is exactly 8 B.
        assert_eq!(small.approx_bytes(), small.bucket_count() * 8);
    }

    #[test]
    fn fingerprint_never_zero() {
        // Run a wide sample to make sure fingerprint_of never collides
        // with the vacant sentinel.
        for i in 0..10_000u32 {
            assert_ne!(fingerprint_of(i), 0);
        }
    }

    #[test]
    fn saturation_starts_returning_true() {
        // Pick a tiny filter and load it past capacity to force the
        // kick chain to exhaust.
        let s = FpSketch::with_capacity(8);
        for i in 0..2048u32 {
            s.insert(i);
            if s.is_saturated() {
                break;
            }
        }
        assert!(s.is_saturated(), "expected saturation under load");
        // After saturation, contains returns true even for a value the
        // filter never saw.
        assert!(s.contains(0xFFFF_FF00));
    }

    #[test]
    fn fpr_is_low_at_target_load() {
        // Insert enough items to put the filter around target load,
        // then probe a disjoint range and check that the false-positive
        // rate stays well under 1 %.
        let capacity = 4096;
        let s = FpSketch::with_capacity(capacity);
        let inserts = ((capacity as f64) * LOAD_TARGET) as u32;
        for i in 0..inserts {
            s.insert(i);
        }
        let probe_start = 10 * inserts;
        let probe_count = 10_000u32;
        let mut fp_hits = 0u32;
        for i in probe_start..(probe_start + probe_count) {
            if s.contains(i) {
                fp_hits += 1;
            }
        }
        let rate = (fp_hits as f64) / (probe_count as f64);
        assert!(
            rate < 0.01,
            "false positive rate {rate:.4} above 1% target (hits={fp_hits} / {probe_count})"
        );
    }
}
