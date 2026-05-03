use super::*;
use std::sync::atomic::Ordering;

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

    /// Increment `pba`'s refcount by `delta`. Returns the new value.
    /// `delta == 0` is a no-op that still performs a lookup.
    pub fn incref_pba(&self, pba: Pba, delta: u32) -> Result<u32> {
        let mut tx = self.begin();
        tx.incref_pba(pba, delta);
        let (_, outcomes) = tx.commit_with_outcomes()?;
        match outcomes.into_iter().next().unwrap() {
            ApplyOutcome::RefcountNew(v) => Ok(v),
            _ => unreachable!("incref produces RefcountNew"),
        }
    }

    /// Decrement `pba`'s refcount by `delta`. Returns the new value.
    /// Decrementing below zero is an error. When the new value hits
    /// zero the row is removed entirely, so the caller is responsible
    /// for cleaning up the corresponding dedup entry.
    pub fn decref_pba(&self, pba: Pba, delta: u32) -> Result<u32> {
        let mut tx = self.begin();
        tx.decref_pba(pba, delta);
        let (_, outcomes) = tx.commit_with_outcomes()?;
        match outcomes.into_iter().next().unwrap() {
            ApplyOutcome::RefcountNew(v) => Ok(v),
            _ => unreachable!("decref produces RefcountNew"),
        }
    }

    /// Record a `hash → value` entry in the dedup index (WAL-logged).
    pub fn put_dedup(&self, hash: Hash32, value: DedupValue) -> Result<Lsn> {
        let mut tx = self.begin();
        tx.put_dedup(hash, value);
        tx.commit()
    }

    /// Tombstone `hash` in the dedup index (WAL-logged).
    pub fn delete_dedup(&self, hash: Hash32) -> Result<Lsn> {
        let mut tx = self.begin();
        tx.delete_dedup(hash);
        tx.commit()
    }

    /// Point-lookup `hash` in the dedup index.
    pub fn get_dedup(&self, hash: &Hash32) -> Result<Option<DedupValue>> {
        self.dedup_index.get(hash)
    }

    /// Batched dedup index lookup. Shares one LSM reader-drain and one
    /// `levels` snapshot across all hashes. Output order matches input
    /// order; duplicates produce repeated results.
    pub fn multi_get_dedup(&self, hashes: &[Hash32]) -> Result<Vec<Option<DedupValue>>> {
        self.dedup_index.multi_get(hashes)
    }

    /// Batched hot-path liveness check for dedup hits. The caller already
    /// looked up `(hash -> value)` once; this re-reads the current forward
    /// index and refcounts under shared snapshots/locks so stale rows and
    /// refcount-zero targets are rejected without one point query per hit.
    pub fn multi_dedup_entries_are_live(
        &self,
        entries: &[(Hash32, DedupValue)],
    ) -> Result<Vec<bool>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let hashes: Vec<Hash32> = entries.iter().map(|(hash, _)| *hash).collect();
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

    // -------- dedup_reverse operations ----------------------------------

    /// Register `hash` as mapped to `pba` in the reverse index. This
    /// is an LSM put, not a modification of the forward dedup index.
    /// Callers typically pair it with `put_dedup(hash, value)` inside
    /// one `begin() / commit()` transaction so both land atomically.
    pub fn register_dedup_reverse(&self, pba: Pba, hash: Hash32) -> Result<Lsn> {
        let mut tx = self.begin();
        tx.register_dedup_reverse(pba, hash);
        tx.commit()
    }

    /// Remove the `(pba, hash)` entry from the reverse index.
    pub fn unregister_dedup_reverse(&self, pba: Pba, hash: Hash32) -> Result<Lsn> {
        let mut tx = self.begin();
        tx.unregister_dedup_reverse(pba, hash);
        tx.commit()
    }

    /// Every full 32-byte hash currently registered for `pba` in the
    /// reverse index. Does **not** include tombstoned entries.
    ///
    /// Cost: one paged-array point lookup per PBA (one shared
    /// `PageCache` hit on the warm path, one `read_page` on a cold
    /// data page). The legacy LSM prefix-scan was the bottleneck of
    /// `cleanup_dedup_for_pbas_batch`; this path is two orders of
    /// magnitude faster on a populated index.
    pub fn scan_dedup_reverse_for_pba(&self, pba: Pba) -> Result<Vec<Hash32>> {
        self.dedup_reverse.get_hashes(pba)
    }

    /// Batched [`scan_dedup_reverse_for_pba`]. Returns one
    /// `Vec<Hash32>` per input PBA, in input order. Each vec carries
    /// at most one entry under the v1 single-hash-per-PBA invariant
    /// (see `paged_reverse` module docs); the shape stays plural so
    /// the caller does not need to change when overflow lands.
    ///
    /// Intended caller: writer / dedup cleanup path sweeping dead PBAs
    /// in a single batch (see onyx-storage `cleanup_dedup_for_pbas_batch`).
    pub fn multi_scan_dedup_reverse_for_pba(&self, pbas: &[Pba]) -> Result<Vec<Vec<Hash32>>> {
        self.dedup_reverse.multi_get_hashes(pbas)
    }

    /// Clean up dedup state for a batch of pbas whose refcount has
    /// transitioned to zero (SPEC §2.2). Atomic: every `DedupDelete` +
    /// `DedupReverseDelete` goes into a single [`Transaction`] and
    /// commits as one WAL record.
    ///
    /// Semantics (1:1 with onyx's
    /// [`cleanup_dedup_for_pbas_batch`](../../../src/meta/store/dedup.rs#L410)):
    /// 1. [`multi_scan_dedup_reverse_for_pba`](Self::multi_scan_dedup_reverse_for_pba)
    ///    collects `(pba, hash)` pairs from the reverse index under one
    ///    LSM reader-drain.
    /// 2. For each `hash`, [`get_dedup`](Self::get_dedup) checks the
    ///    forward index: the tombstone is emitted only when the entry
    ///    still points to the target pba. This handles the race where a
    ///    concurrent writer re-registered `hash` against a different
    ///    pba between `drop_snapshot` finishing and this cleanup
    ///    running — deleting the forward entry there would lose the
    ///    live mapping.
    /// 3. A `DedupReverseDelete { pba, hash }` is unconditional: the
    ///    reverse entry is always stale once `pba` is freed.
    ///
    /// Idempotent under replay — both ops are tombstones, and both
    /// the `get_dedup` probe and the forward check re-read LSM state at
    /// commit apply time, so running twice is a no-op if the first run
    /// already landed.
    ///
    /// Empty `pbas` returns [`last_applied_lsn`](Self::last_applied_lsn)
    /// without touching the WAL (mirrors [`range_delete`](Self::range_delete)).
    pub fn cleanup_dedup_for_dead_pbas(&self, pbas: &[Pba]) -> Result<Lsn> {
        let total_started = std::time::Instant::now();
        self.metrics.record_cleanup_call(pbas.len());
        if pbas.is_empty() {
            self.metrics.record_cleanup_noop();
            self.metrics.record_cleanup_success(total_started.elapsed());
            return Ok(self.last_applied_lsn());
        }

        let scan_started = std::time::Instant::now();
        let hashes_per_pba = match self.multi_scan_dedup_reverse_for_pba(pbas) {
            Ok(hashes_per_pba) => {
                let hashes_found = hashes_per_pba.iter().map(Vec::len).sum();
                self.metrics
                    .record_cleanup_scan(scan_started.elapsed(), hashes_found);
                hashes_per_pba
            }
            Err(err) => {
                self.metrics.record_cleanup_scan(scan_started.elapsed(), 0);
                self.metrics.record_cleanup_error(total_started.elapsed());
                return Err(err);
            }
        };
        let pairs: Vec<(Pba, Hash32)> = pbas
            .iter()
            .copied()
            .zip(hashes_per_pba)
            .flat_map(|(pba, hashes)| hashes.into_iter().map(move |hash| (pba, hash)))
            .collect();
        let forward_entries = if pairs.is_empty() {
            Vec::new()
        } else {
            let hashes: Vec<Hash32> = pairs.iter().map(|(_, hash)| *hash).collect();
            let check_started = std::time::Instant::now();
            match self.multi_get_dedup(&hashes) {
                Ok(entries) => {
                    self.metrics
                        .record_cleanup_forward_checks(check_started.elapsed(), hashes.len());
                    entries
                }
                Err(err) => {
                    self.metrics
                        .record_cleanup_forward_checks(check_started.elapsed(), hashes.len());
                    self.metrics.record_cleanup_error(total_started.elapsed());
                    return Err(err);
                }
            }
        };

        let mut tx = self.begin();
        let mut forward_tombstones = 0usize;
        for ((pba, hash), entry) in pairs.into_iter().zip(forward_entries.into_iter()) {
            // Only drop the forward entry if it still points at
            // `pba`. Another writer may have re-registered `hash`
            // against a newer pba in the interval between the
            // plan-side scan and now — SPEC §4.5 race protection.
            if let Some(entry) = entry {
                if entry.head_pba() == pba {
                    tx.delete_dedup(hash);
                    forward_tombstones += 1;
                }
            }
            // The reverse entry itself is always stale — regardless
            // of the forward-index race outcome, the pba is freed.
            tx.unregister_dedup_reverse(pba, hash);
        }
        self.metrics
            .record_cleanup_tombstones(forward_tombstones, tx.len());
        if tx.is_empty() {
            self.metrics.record_cleanup_noop();
            self.metrics.record_cleanup_success(total_started.elapsed());
            return Ok(self.last_applied_lsn());
        }
        let commit_started = std::time::Instant::now();
        match tx.commit() {
            Ok(lsn) => {
                self.metrics.record_cleanup_commit(commit_started.elapsed());
                self.metrics.record_cleanup_success(total_started.elapsed());
                Ok(lsn)
            }
            Err(err) => {
                self.metrics.record_cleanup_commit(commit_started.elapsed());
                self.metrics.record_cleanup_error(total_started.elapsed());
                Err(err)
            }
        }
    }

    /// `true` if the dedup index has dirty in-memory state that
    /// would benefit from a flush. Cuckoo writes are synchronous so
    /// only the meta page can ever be dirty; the answer is always
    /// `false` outside of explicit `flush_meta` callers.
    pub fn dedup_should_flush(&self) -> bool {
        false
    }

    /// Persist the dedup index meta page if dirty. The cuckoo data
    /// pages are already on disk after each `put`/`delete`, so this
    /// only flushes the page-table meta page; manifest-neutral.
    pub fn flush_dedup_memtable(&self) -> Result<bool> {
        self.flush_dedup_memtables_at_generation(self.current_generation())
    }

    /// No-op: cuckoo dedup_index has no LSM levels to compact.
    /// Retained for API stability while operator tooling is updated.
    pub fn compact_dedup_once(&self) -> Result<bool> {
        Ok(false)
    }

    pub(super) fn flush_dedup_memtables_at_generation(&self, _generation: Lsn) -> Result<bool> {
        let dedup_dirty = self.dedup_index.flush_meta()?;
        // dedup_reverse: paged-array meta page is the only thing that
        // can be dirty here; data pages already wrote synchronously
        // under each `PagedReverse::put` / `delete`.
        self.dedup_reverse.flush_meta()?;
        Ok(dedup_dirty)
    }

    pub(super) fn maybe_schedule_dedup_maintenance(&self) {
        // Cuckoo dedup_index has no compaction or memtable freeze;
        // background maintenance is a no-op now. The lane wiring stays
        // in place so callers don't need to special-case the absence
        // of dedup work.
    }

    /// Iterate every `(Pba, refcount)` pair across all refcount shards,
    /// sorted by Pba. Refcount is a running tally (global), so there is
    /// no per-volume filtering — callers doing volume-scoped audits
    /// cross-reference with [`range`](Self::range) output themselves.
    ///
    /// Currently materialised upfront; the `impl Iterator` surface is
    /// exposed so future commits can swap the body for a lazy walker
    /// without touching call sites.
    pub fn iter_refcounts(&self) -> Result<DbRefcountIter> {
        let mut all: Vec<(Pba, u32)> = Vec::new();
        for shard in &self.refcount_shards {
            for (pba, entry) in shard.rc.iter_live_flushed()? {
                all.push((pba, entry.rc));
            }
        }
        all.sort_unstable_by_key(|(pba, _)| *pba);
        Ok(DbRefcountIter {
            inner: all.into_iter(),
        })
    }

    /// Iterate every live `(Hash32, DedupValue)` entry in the dedup
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
