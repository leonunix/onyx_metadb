use super::*;
use std::sync::atomic::Ordering;

impl Db {
    // -------- refcount + dedup ops --------------------------------------

    /// Return the current refcount for `pba`, or 0 if no entry exists.
    pub fn get_refcount(&self, pba: Pba) -> Result<u32> {
        let sid = self.refcount_shard_for(pba);
        let mut tree = self.refcount_shards[sid].tree.lock();
        Ok(tree.get(pba)?.map(|e| e.rc).unwrap_or(0))
    }

    /// Batched refcount lookup. Groups `pbas` by shard, locks each shard
    /// once, and reads every PBA that falls to it before moving on. Output
    /// order matches input order; duplicates produce repeated results.
    /// Unmapped PBAs read back as `0`, same as [`get_refcount`].
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
            let mut tree = self.refcount_shards[sid].tree.lock();
            for idx in idxs {
                out[idx] = tree.get(pbas[idx])?.map(|e| e.rc).unwrap_or(0);
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
    /// Cost: scans every `dedup_reverse` SST whose min/max range
    /// intersects the 8-byte PBA prefix, plus the memtable. Fine for
    /// the decref-to-zero cleanup path (rare, per-PBA); not suitable
    /// for hot-path queries.
    pub fn scan_dedup_reverse_for_pba(&self, pba: Pba) -> Result<Vec<Hash32>> {
        let prefix = pba.to_be_bytes();
        let rows = self.dedup_reverse.scan_prefix_all_shards(&prefix)?;
        Ok(rows
            .into_iter()
            .map(|(key, value)| decode_reverse_hash(&key, &value))
            .collect())
    }

    /// Batched `dedup_reverse` prefix scan: one call per PBA, one
    /// reader-drain acquisition and one `levels` snapshot shared across
    /// all PBAs. Returns one `Vec<Hash32>` per input PBA, in input order.
    ///
    /// Intended caller: writer / dedup cleanup path sweeping dead PBAs
    /// in a single batch (see onyx-storage `cleanup_dedup_for_pbas_batch`).
    pub fn multi_scan_dedup_reverse_for_pba(&self, pbas: &[Pba]) -> Result<Vec<Vec<Hash32>>> {
        if pbas.is_empty() {
            return Ok(Vec::new());
        }
        let prefixes: Vec<[u8; 8]> = pbas.iter().map(|pba| pba.to_be_bytes()).collect();
        let prefix_refs: Vec<&[u8]> = prefixes.iter().map(|p| p.as_slice()).collect();
        let rows_per_pba = self
            .dedup_reverse
            .multi_scan_prefix_all_shards(&prefix_refs)?;
        Ok(rows_per_pba
            .into_iter()
            .map(|rows| {
                rows.into_iter()
                    .map(|(key, value)| decode_reverse_hash(&key, &value))
                    .collect()
            })
            .collect())
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

    /// `true` if any dedup memtable shard has reached its freeze threshold.
    pub fn dedup_should_flush(&self) -> bool {
        self.dedup_index.should_flush_any() || self.dedup_reverse.should_flush_any()
    }

    /// Flush dedup memtables to fresh L0 SSTs. Returns true if either
    /// side wrote an SST. This is intentionally manifest-neutral: the
    /// next checkpoint persists the new level heads atomically with the
    /// database roots.
    pub fn flush_dedup_memtable(&self) -> Result<bool> {
        let generation = self.current_generation();
        self.flush_dedup_memtables_at_generation(generation)
    }

    /// Run one round of dedup compaction across every forward-index
    /// shard. Returns `true` if any shard performed work.
    pub fn compact_dedup_once(&self) -> Result<bool> {
        let generation = self.current_generation();
        self.dedup_index.compact_once_any(generation)
    }

    pub(super) fn flush_dedup_memtables_at_generation(&self, generation: Lsn) -> Result<bool> {
        let index = self.dedup_index.flush_memtable_all(generation)?;
        let reverse = self.dedup_reverse.flush_memtable_all(generation)?;
        Ok(index || reverse)
    }

    pub(super) fn maybe_schedule_dedup_maintenance(&self) {
        if !self.dedup_should_flush() {
            return;
        }
        if self
            .dedup_maintenance_queued
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }

        let index = self.dedup_index.clone();
        let reverse = self.dedup_reverse.clone();
        let queued = self.dedup_maintenance_queued.clone();
        let generation = self.last_applied_lsn().saturating_add(1).max(1);
        self.dedup_maintenance_lane
            .handle()
            .enqueue_maintenance(Box::new(move || {
                let result = (|| -> Result<()> {
                    index.flush_memtable_all(generation)?;
                    reverse.flush_memtable_all(generation)?;
                    Ok(())
                })();
                if let Err(err) = result {
                    tracing::warn!(error = %err, "metadb: background dedup memtable flush failed");
                }
                queued.store(false, Ordering::Release);
            }));
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
            let mut tree = shard.tree.lock();
            for rec in tree.iter_stream()? {
                let (pba, entry) = rec?;
                all.push((pba, entry.rc));
            }
        }
        all.sort_unstable_by_key(|(pba, _)| *pba);
        Ok(DbRefcountIter {
            inner: all.into_iter(),
        })
    }

    /// Iterate every live `(Hash32, DedupValue)` entry in the dedup
    /// forward index, sorted by hash. Tombstoned hashes are hidden.
    /// Materialised via the LSM's prefix-scan path with an empty prefix;
    /// shares one `reader_drain` and one `levels` snapshot with any
    /// concurrent readers.
    pub fn iter_dedup(&self) -> Result<DbDedupIter> {
        let all = self.dedup_index.scan_prefix_all_shards(&[])?;
        Ok(DbDedupIter {
            inner: all.into_iter(),
        })
    }
}
