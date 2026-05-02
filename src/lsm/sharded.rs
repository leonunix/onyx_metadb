//! N-way sharded wrapper around [`Lsm`].
//!
//! # Responsibility
//!
//! Owns a fixed `Box<[Arc<Lsm>]>` whose length is a power of two. All
//! routed operations (`put`, `delete`, `get`, `multi_get`, batched
//! apply) pick a shard from the SHA-256 content hash; multi-shard
//! operations (`flush_memtable_all`, `persist_levels_all`,
//! `scan_prefix_all_shards`, ...) iterate every shard.
//!
//! # Concurrency
//!
//! Each shard is an independent `Lsm` with its own memtable lock,
//! reader-drain, and modify lock. Routed methods touch exactly one
//! shard, so writes to different shards do not contend.
//!
//! # Routing
//!
//! `Hash32` is SHA-256 output → already uniformly distributed; using
//! the codebase-wide `xxh3_64` shard router on top would just be cycles
//! wasted. The high bits of `hash[0]` fan out up to 256 shards
//! uniformly. For `dedup_reverse` keys (encoded `[pba: 8B][hash[..24]]`),
//! the same hash drives routing — we read byte 8 of the encoded key,
//! which is the first byte of the content hash. This guarantees a
//! single `(hash, pba)` pair lives in the same shard for both forward
//! and reverse indexes, so a single Onyx `put_dedup_entries` commit
//! hits exactly one shard.

use std::sync::Arc;

use crate::cache::PageCache;
use crate::error::Result;
use crate::page_store::PageStore;
use crate::types::{Lsn, PageId};

use super::format::{DedupValue, Hash32};
use super::lsm::{Lsm, LsmConfig, LsmStats};
use super::memtable::DedupOp;

/// Routing for forward `dedup_index` keys (the content hash itself).
#[inline]
pub fn shard_for_hash(hash: &Hash32, shards: u32) -> u32 {
    debug_assert!(
        shards.is_power_of_two() && shards > 0,
        "dedup_shards must be a power of two; got {shards}"
    );
    let shift = 8u32 - shards.trailing_zeros();
    u32::from(hash[0]) >> shift
}

/// Routing for `dedup_reverse` keys: encoded as
/// `[pba: 8B BE][hash_first_24B]`, so the content hash starts at byte
/// 8. Same hash → same shard as the forward index entry.
#[inline]
pub fn shard_for_reverse_key(key: &Hash32, shards: u32) -> u32 {
    debug_assert!(
        shards.is_power_of_two() && shards > 0,
        "dedup_shards must be a power of two; got {shards}"
    );
    let shift = 8u32 - shards.trailing_zeros();
    u32::from(key[8]) >> shift
}

/// Sharded `Lsm` wrapper. Length is fixed at construction.
pub struct ShardedLsm {
    shards: Box<[Arc<Lsm>]>,
}

impl std::fmt::Debug for ShardedLsm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardedLsm")
            .field("shards", &self.shards.len())
            .finish()
    }
}

impl ShardedLsm {
    /// Wrap an explicit shard list. Panics if empty or non-power-of-two.
    pub fn new(shards: Vec<Arc<Lsm>>) -> Self {
        assert!(!shards.is_empty(), "ShardedLsm requires at least one shard");
        assert!(
            shards.len().is_power_of_two(),
            "ShardedLsm shard count must be a power of two; got {}",
            shards.len()
        );
        Self {
            shards: shards.into_boxed_slice(),
        }
    }

    /// Build N empty shards sharing the supplied page cache.
    pub fn create_with_cache(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        config: LsmConfig,
        shard_count: usize,
    ) -> Self {
        let shards = (0..shard_count)
            .map(|_| {
                Arc::new(Lsm::create_with_cache(
                    page_store.clone(),
                    page_cache.clone(),
                    config.clone(),
                ))
            })
            .collect::<Vec<_>>();
        Self::new(shards)
    }

    /// Open one shard per entry in `per_shard_heads`, each from its own
    /// persisted level chain.
    pub fn open_with_cache(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        config: LsmConfig,
        per_shard_heads: &[Vec<PageId>],
    ) -> Result<Self> {
        let mut shards = Vec::with_capacity(per_shard_heads.len());
        for heads in per_shard_heads {
            shards.push(Arc::new(Lsm::open_with_cache(
                page_store.clone(),
                page_cache.clone(),
                config.clone(),
                heads,
            )?));
        }
        Ok(Self::new(shards))
    }

    /// Number of shards. Always a power of two ≥ 1.
    #[inline]
    pub fn shards(&self) -> usize {
        self.shards.len()
    }

    #[inline]
    fn shard_count_u32(&self) -> u32 {
        self.shards.len() as u32
    }

    #[inline]
    fn shard_for_forward(&self, hash: &Hash32) -> &Lsm {
        let sid = shard_for_hash(hash, self.shard_count_u32()) as usize;
        &self.shards[sid]
    }

    #[inline]
    fn shard_for_reverse(&self, key: &Hash32) -> &Lsm {
        let sid = shard_for_reverse_key(key, self.shard_count_u32()) as usize;
        &self.shards[sid]
    }

    // -------- forward index (key = content hash) ------------------------

    pub fn put(&self, hash: Hash32, value: DedupValue) {
        self.shard_for_forward(&hash).put(hash, value);
    }

    pub fn delete(&self, hash: Hash32) {
        self.shard_for_forward(&hash).delete(hash);
    }

    pub fn get(&self, hash: &Hash32) -> Result<Option<DedupValue>> {
        self.shard_for_forward(hash).get(hash)
    }

    /// Batched point lookup. Buckets by shard, then issues one
    /// `multi_get` per shard so each shard amortises its drain + levels
    /// snapshot. Output order matches input order.
    pub fn multi_get(&self, hashes: &[Hash32]) -> Result<Vec<Option<DedupValue>>> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        if self.shards.len() == 1 {
            return self.shards[0].multi_get(hashes);
        }
        let n = self.shard_count_u32();
        let mut buckets: Vec<Vec<usize>> = (0..self.shards.len()).map(|_| Vec::new()).collect();
        for (idx, hash) in hashes.iter().enumerate() {
            let sid = shard_for_hash(hash, n) as usize;
            buckets[sid].push(idx);
        }
        let mut out: Vec<Option<DedupValue>> = vec![None; hashes.len()];
        for (sid, idxs) in buckets.into_iter().enumerate() {
            if idxs.is_empty() {
                continue;
            }
            let bucket: Vec<Hash32> = idxs.iter().map(|&i| hashes[i]).collect();
            let results = self.shards[sid].multi_get(&bucket)?;
            for (i, result) in idxs.into_iter().zip(results) {
                out[i] = result;
            }
        }
        Ok(out)
    }

    /// Forward `apply_batch`: route each `(hash, op)` to its shard and
    /// invoke `Lsm::apply_batch` once per shard. Empty buckets are
    /// skipped.
    pub fn apply_batch_forward(&self, ops: &[(Hash32, DedupOp)]) {
        if ops.is_empty() {
            return;
        }
        if self.shards.len() == 1 {
            self.shards[0].apply_batch(ops);
            return;
        }
        let n = self.shard_count_u32();
        let mut buckets: Vec<Vec<(Hash32, DedupOp)>> =
            (0..self.shards.len()).map(|_| Vec::new()).collect();
        for op in ops {
            let sid = shard_for_hash(&op.0, n) as usize;
            buckets[sid].push((op.0, op.1));
        }
        for (sid, bucket) in buckets.into_iter().enumerate() {
            if !bucket.is_empty() {
                self.shards[sid].apply_batch(&bucket);
            }
        }
    }

    // -------- reverse index (key = `[pba | hash[..24]]`) ----------------

    pub fn put_reverse(&self, key: Hash32, value: DedupValue) {
        self.shard_for_reverse(&key).put(key, value);
    }

    pub fn delete_reverse(&self, key: Hash32) {
        self.shard_for_reverse(&key).delete(key);
    }

    /// Reverse `apply_batch`: same shape as [`apply_batch_forward`] but
    /// keys are reverse-encoded so routing reads `key[8]`.
    pub fn apply_batch_reverse(&self, ops: &[(Hash32, DedupOp)]) {
        if ops.is_empty() {
            return;
        }
        if self.shards.len() == 1 {
            self.shards[0].apply_batch(ops);
            return;
        }
        let n = self.shard_count_u32();
        let mut buckets: Vec<Vec<(Hash32, DedupOp)>> =
            (0..self.shards.len()).map(|_| Vec::new()).collect();
        for op in ops {
            let sid = shard_for_reverse_key(&op.0, n) as usize;
            buckets[sid].push((op.0, op.1));
        }
        for (sid, bucket) in buckets.into_iter().enumerate() {
            if !bucket.is_empty() {
                self.shards[sid].apply_batch(&bucket);
            }
        }
    }

    // -------- multi-shard fan-out ---------------------------------------

    /// Prefix scan that visits every shard and unions the results.
    ///
    /// Used for PBA-prefix scans on `dedup_reverse`: keys are
    /// `[pba | hash...]`, so the row for `(pba, hash)` lives in the
    /// shard chosen by hash. The scanner can't pre-pick a shard from
    /// the pba alone, so it has to ask every shard.
    pub fn scan_prefix_all_shards(&self, prefix: &[u8]) -> Result<Vec<(Hash32, DedupValue)>> {
        if self.shards.len() == 1 {
            return self.shards[0].scan_prefix(prefix);
        }
        let mut combined = Vec::new();
        for shard in self.shards.iter() {
            combined.extend(shard.scan_prefix(prefix)?);
        }
        Ok(combined)
    }

    /// Batched all-shards prefix scan. Each output `Vec<...>` is the
    /// union of rows matching the corresponding input prefix across
    /// every shard. Output order matches input order.
    pub fn multi_scan_prefix_all_shards(
        &self,
        prefixes: &[&[u8]],
    ) -> Result<Vec<Vec<(Hash32, DedupValue)>>> {
        if self.shards.len() == 1 {
            return self.shards[0].multi_scan_prefix(prefixes);
        }
        let mut out: Vec<Vec<(Hash32, DedupValue)>> =
            std::iter::repeat_with(Vec::new).take(prefixes.len()).collect();
        for shard in self.shards.iter() {
            let per_prefix = shard.multi_scan_prefix(prefixes)?;
            for (i, rows) in per_prefix.into_iter().enumerate() {
                out[i].extend(rows);
            }
        }
        Ok(out)
    }

    /// True if any shard's memtable has reached the freeze threshold.
    pub fn should_flush_any(&self) -> bool {
        self.shards.iter().any(|s| s.should_flush())
    }

    /// Flush every shard's memtable. Returns `true` if any shard wrote
    /// an SST.
    pub fn flush_memtable_all(&self, generation: Lsn) -> Result<bool> {
        let mut wrote = false;
        for shard in self.shards.iter() {
            if shard.flush_memtable(generation)?.is_some() {
                wrote = true;
            }
        }
        Ok(wrote)
    }

    /// Run one compaction round per shard. Returns `true` if any shard
    /// did work this call.
    pub fn compact_once_any(&self, generation: Lsn) -> Result<bool> {
        let mut did_work = false;
        for shard in self.shards.iter() {
            if shard.compact_once(generation)?.is_some() {
                did_work = true;
            }
        }
        Ok(did_work)
    }

    /// Persist every shard's level chain to fresh on-disk pages.
    /// Returns one `Vec<PageId>` per shard, in shard order. Caller is
    /// responsible for the manifest swap protocol (commit new heads
    /// then free old heads via [`free_old_level_heads_all`]).
    pub fn persist_levels_all(&self, generation: Lsn) -> Result<Vec<Vec<PageId>>> {
        let mut out = Vec::with_capacity(self.shards.len());
        for shard in self.shards.iter() {
            out.push(shard.persist_levels(generation)?);
        }
        Ok(out)
    }

    /// Free every shard's previously-persisted level heads. The outer
    /// length must equal `self.shards()`.
    pub fn free_old_level_heads_all(
        &self,
        old_heads_per_shard: &[Vec<PageId>],
        generation: Lsn,
    ) -> Result<()> {
        assert_eq!(
            old_heads_per_shard.len(),
            self.shards.len(),
            "free_old_level_heads_all: old_heads_per_shard length {} != shard count {}",
            old_heads_per_shard.len(),
            self.shards.len()
        );
        for (shard, heads) in self.shards.iter().zip(old_heads_per_shard) {
            shard.free_old_level_heads(heads, generation)?;
        }
        Ok(())
    }

    /// Per-shard stats. Length matches [`shards`](Self::shards).
    pub fn shard_stats(&self) -> Vec<LsmStats> {
        self.shards.iter().map(|s| s.stats()).collect()
    }

    /// Aggregate stats across all shards. Counts (`total_ssts`,
    /// `total_records`, memtable entries) are summed; `level_count` is
    /// taken as the maximum across shards (operational diagnostic —
    /// the per-shard depth is what matters, not the sum).
    pub fn aggregate_stats(&self) -> LsmStats {
        let mut agg = LsmStats::default();
        for shard in self.shards.iter() {
            let s = shard.stats();
            agg.memtable.active_entries += s.memtable.active_entries;
            agg.memtable.frozen_entries += s.memtable.frozen_entries;
            agg.level_count = agg.level_count.max(s.level_count);
            agg.total_ssts += s.total_ssts;
            agg.total_records += s.total_records;
        }
        agg
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash_with_byte0(b: u8) -> Hash32 {
        let mut h = [0u8; 32];
        h[0] = b;
        h
    }

    #[test]
    fn shard_for_hash_n_eq_one_always_zero() {
        for b in 0..=255u8 {
            assert_eq!(shard_for_hash(&hash_with_byte0(b), 1), 0);
        }
    }

    #[test]
    fn shard_for_hash_distributes_uniformly_for_power_of_two() {
        // For each power-of-two N up to 16, a sweep of all 256 byte-0
        // values must land each shard exactly 256/N times. SHA-256
        // output is uniform random over byte 0, so this is the right
        // model for real workload distribution.
        for log_n in 0..=4u32 {
            let n = 1u32 << log_n;
            let mut counts = vec![0u32; n as usize];
            for b in 0..=255u8 {
                let sid = shard_for_hash(&hash_with_byte0(b), n);
                counts[sid as usize] += 1;
            }
            let expected = 256 / n;
            for (sid, count) in counts.iter().enumerate() {
                assert_eq!(
                    *count, expected,
                    "n={n}: shard {sid} got {count} hashes, expected {expected}"
                );
            }
        }
    }

    #[test]
    fn shard_for_reverse_key_uses_byte_8() {
        // First 8 bytes of the reverse key are the PBA, ignored by
        // routing. Byte 8 onward is the content hash. Two reverse keys
        // with the same hash bytes must route identically regardless
        // of pba.
        let mut k1 = [0u8; 32];
        k1[0..8].copy_from_slice(&1u64.to_be_bytes());
        k1[8] = 0xC0;

        let mut k2 = [0u8; 32];
        k2[0..8].copy_from_slice(&999u64.to_be_bytes());
        k2[8] = 0xC0;

        for log_n in 0..=4u32 {
            let n = 1u32 << log_n;
            assert_eq!(shard_for_reverse_key(&k1, n), shard_for_reverse_key(&k2, n));
        }
    }

    #[test]
    fn forward_and_reverse_route_identically_for_matching_hash() {
        // The on-disk invariant: for any (hash, pba), the forward
        // entry (key=hash) and the reverse entry (key=encode(pba,hash))
        // share a shard. That makes a single put_dedup_entries commit
        // hit exactly one shard.
        let hash: Hash32 = {
            let mut h = [0u8; 32];
            for (i, b) in h.iter_mut().enumerate() {
                *b = (i * 13) as u8;
            }
            h
        };
        let mut reverse_key = [0u8; 32];
        reverse_key[0..8].copy_from_slice(&42u64.to_be_bytes());
        reverse_key[8..32].copy_from_slice(&hash[0..24]);

        for log_n in 0..=4u32 {
            let n = 1u32 << log_n;
            assert_eq!(
                shard_for_hash(&hash, n),
                shard_for_reverse_key(&reverse_key, n),
                "n={n}: forward routes to {} but reverse routes to {}",
                shard_for_hash(&hash, n),
                shard_for_reverse_key(&reverse_key, n),
            );
        }
    }
}
