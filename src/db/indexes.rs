use super::*;

impl Db {
    // -------- refcount + dedup ops --------------------------------------

    /// Return the current refcount for `pba`, or 0 if no entry exists.
    pub fn get_refcount(&self, pba: Pba) -> Result<u32> {
        let sid = self.refcount_shard_for(pba);
        self.refcount_shards[sid].rc.get(pba)
    }

    /// Batched refcount lookup. Groups `pbas` by shard so each shard's
    /// internal mutex is taken once. Output order matches input;
    /// duplicates produce repeated results. Unmapped PBAs read back
    /// as `0`, same as [`get_refcount`].
    pub fn multi_get_refcount(&self, pbas: &[Pba]) -> Result<Vec<u32>> {
        if pbas.is_empty() {
            return Ok(Vec::new());
        }
        let shard_count = self.refcount_shards.len();
        let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); shard_count];
        for (idx, pba) in pbas.iter().enumerate() {
            buckets[self.refcount_shard_for(*pba)].push(idx);
        }
        let mut out: Vec<u32> = vec![0; pbas.len()];
        for (sid, idxs) in buckets.into_iter().enumerate() {
            if idxs.is_empty() {
                continue;
            }
            let shard = &self.refcount_shards[sid];
            for idx in idxs {
                out[idx] = shard.rc.get(pbas[idx])?;
            }
        }
        Ok(out)
    }

    /// Test-only helper to seed PBA refcount state ahead of FreePbas /
    /// PromotionChunk apply scenarios. Phase 5 removed the per-write
    /// refcount path, so production code no longer needs an incref WAL
    /// op; the rc shard is still backed by the same paged-array. Tests
    /// drive `PromotionChunk` (the only remaining `rc.stage(+)` caller)
    /// so the seed lives on the same WAL replay path as production rc
    /// writes.
    ///
    /// `#[doc(hidden)]` and `pub` — not `cfg(test)` — so integration
    /// tests in `tests/` can call it. Production code must not.
    #[doc(hidden)]
    pub fn incref_pba(&self, pba: Pba, delta: u32) -> Result<u32> {
        if delta == 0 {
            return self.get_refcount(pba);
        }
        const BOOTSTRAP_VOL_ORD: crate::types::VolumeOrdinal = 0;
        // Pack `delta` copies of `pba` into PromotionChunks so one rc
        // bucket sees back-to-back stages — matches the pre-Phase-5
        // `tx.incref_pba(pba, delta)` shape. Each chunk is capped by
        // `MAX_PROMOTION_CHUNK_PBAS`; large deltas split across chunks.
        let mut remaining = delta as usize;
        while remaining > 0 {
            let take = remaining.min(crate::db::promotion::MAX_PROMOTION_CHUNK_PBAS);
            let mut pba_increfs = Vec::with_capacity(take);
            for _ in 0..take {
                pba_increfs.push(pba);
            }
            self.commit_promotion_chunk(BOOTSTRAP_VOL_ORD, pba_increfs, None)?;
            remaining -= take;
        }
        self.get_refcount(pba)
    }

    /// Test-only inverse of [`Self::incref_pba`]. Drives the rc down via
    /// `FreePbas` shared-branch semantics: each FreePbas op decrefs the
    /// PBA by 1 (rc>0) or surfaces it as exclusive (rc==0). The caller
    /// is responsible for the post-condition; with FreePbas the rc may
    /// reach 0 before `delta` is exhausted (further ops are no-ops on
    /// the rc side but still surface the PBA).
    #[doc(hidden)]
    pub fn decref_pba(&self, pba: Pba, delta: u32) -> Result<u32> {
        if delta == 0 {
            return self.get_refcount(pba);
        }
        const BOOTSTRAP_VOL_ORD: crate::types::VolumeOrdinal = 0;
        for _ in 0..delta {
            self.commit_free_pbas(BOOTSTRAP_VOL_ORD, &[pba])?;
        }
        self.get_refcount(pba)
    }

    /// Record a `hash → value` entry in the dedup index (WAL-logged).
    pub fn put_dedup(&self, hash: Hash8, value: DedupValue) -> Result<Lsn> {
        let mut tx = self.begin();
        tx.put_dedup(hash, value);
        tx.commit()
    }

    /// Tombstone `hash` in the dedup index (WAL-logged).
    pub fn delete_dedup(&self, hash: Hash8) -> Result<Lsn> {
        let mut tx = self.begin();
        tx.delete_dedup(hash);
        tx.commit()
    }

    /// Tombstone `hash` only when the current value still equals
    /// `old_value` at WAL apply time.
    pub fn compare_delete_dedup(&self, hash: Hash8, old_value: DedupValue) -> Result<bool> {
        let mut tx = self.begin();
        tx.compare_delete_dedup(hash, old_value);
        let (_, outcomes) = tx.commit_with_outcomes()?;
        match outcomes.into_iter().next().unwrap() {
            ApplyOutcome::DedupCompare { applied } => Ok(applied),
            _ => unreachable!("compare_delete_dedup produces DedupCompare"),
        }
    }

    /// Replace `hash` only when the current value still equals
    /// `old_value` at WAL apply time.
    pub fn compare_put_dedup(
        &self,
        hash: Hash8,
        old_value: DedupValue,
        new_value: DedupValue,
    ) -> Result<bool> {
        let mut tx = self.begin();
        tx.compare_put_dedup(hash, old_value, new_value);
        let (_, outcomes) = tx.commit_with_outcomes()?;
        match outcomes.into_iter().next().unwrap() {
            ApplyOutcome::DedupCompare { applied } => Ok(applied),
            _ => unreachable!("compare_put_dedup produces DedupCompare"),
        }
    }

    /// Point-lookup `hash` in the dedup index.
    pub fn get_dedup(&self, hash: &Hash8) -> Result<Option<DedupValue>> {
        self.dedup_index.get(hash)
    }

    /// [[no-refcount-hot-path-design]] Phase 5 helper: read the current
    /// `head_pba()` for `hash` from the dedup_index, used by
    /// `Transaction::resolve_dedup_old_pbas` to embed the prior PBA in
    /// `DedupPut` / `DedupPutGuarded` / `DedupDelete` WAL records so
    /// apply is deterministic from the WAL alone and replay doesn't
    /// observe the asymmetric on-disk dedup_index state.
    pub(crate) fn dedup_lookup_head_pba(&self, hash: &Hash8) -> Result<Option<Pba>> {
        Ok(self.dedup_index.get(hash)?.map(|v| v.head_pba()))
    }

    /// Batched dedup index lookup. Shares one LSM reader-drain and one
    /// `levels` snapshot across all hashes. Output order matches input
    /// order; duplicates produce repeated results.
    pub fn multi_get_dedup(&self, hashes: &[Hash8]) -> Result<Vec<Option<DedupValue>>> {
        self.dedup_index.multi_get(hashes)
    }

    /// Batched hot-path liveness check for dedup hits. The caller already
    /// looked up `(hash -> value)` once; this re-reads the current forward
    /// index and refcounts under shared snapshots/locks so stale rows and
    /// refcount-zero targets are rejected without one point query per hit.
    pub fn multi_dedup_entries_are_live(
        &self,
        entries: &[(Hash8, DedupValue)],
    ) -> Result<Vec<bool>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let hashes: Vec<Hash8> = entries.iter().map(|(hash, _)| *hash).collect();
        let forward = self.multi_get_dedup(&hashes)?;
        let pbas: Vec<Pba> = entries.iter().map(|(_, value)| value.head_pba()).collect();
        let refcounts = self.multi_get_refcount(&pbas)?;

        Ok(entries
            .iter()
            .zip(forward)
            .zip(refcounts)
            .map(|(((.., expected), current), rc)| rc > 0 && current.as_ref() == Some(expected))
            .collect())
    }

    // -------- dedup_reverse: retired in schema 0xB3 ---------------------
    //
    // The legacy `register_dedup_reverse` / `unregister_dedup_reverse` /
    // `scan_dedup_reverse_for_pba` / `multi_scan_dedup_reverse_for_pba` /
    // `cleanup_dedup_for_dead_pbas` API powered the pre-promote-on-verified-hit
    // dedup cleanup path (prefix-scan reverse → tombstone forward + reverse).
    // Onyx switched to old-mapping read-back: cleanup re-reads the freed PBA
    // payload, recomputes the xxh3 hash, and calls
    // `delete_dedup_index_if_matches`. The reverse index has been inert
    // since then; the API + the underlying `paged_reverse` module are gone
    // (see WAL schema 0xB3 / manifest v9 break).

    /// Iterate every `(Pba, refcount)` pair across all refcount shards,
    /// sorted by Pba. Refcount is a running tally (global), so there is
    /// no per-volume filtering — callers doing volume-scoped audits
    /// cross-reference with [`range`](Self::range) output themselves.
    ///
    /// Currently materialised upfront; the `impl Iterator` surface is
    /// exposed so future commits can swap the body for a lazy walker
    /// without touching call sites.
    pub fn iter_refcounts(&self) -> Result<DbRefcountIter> {
        // Keep the refcount roots and manifest checkpoint aligned.
        // Flushing shards independently can persist refcount pages
        // newer than the WAL checkpoint; recovery would then replay
        // older conditional L2P remaps against a future refcount base.
        self.flush()?;

        let mut all: Vec<(Pba, u32)> = Vec::new();
        for shard in &self.refcount_shards {
            for (pba, entry) in shard.rc.iter_live()? {
                all.push((pba, entry.rc));
            }
        }
        all.sort_unstable_by_key(|(pba, _)| *pba);
        Ok(DbRefcountIter {
            inner: all.into_iter(),
        })
    }

    /// Iterate every live `(Hash8, DedupValue)` entry in the dedup
    /// forward index. Order is the cuckoo data-page → bucket → slot
    /// order (deterministic but not lexicographic on hash); callers
    /// that need a sorted view should sort the returned vec.
    pub fn iter_dedup(&self) -> Result<DbDedupIter> {
        let all = self.dedup_index.iter()?;
        Ok(DbDedupIter {
            inner: all.into_iter(),
        })
    }
}
