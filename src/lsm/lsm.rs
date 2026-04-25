//! Top-level LSM facade.
//!
//! Owns:
//! - an active + at-most-one-frozen [`Memtable`]
//! - a `Vec<Vec<SstHandle>>` of on-disk SSTs per level
//!
//! Exposes:
//! - `put` / `delete` / `get`
//! - `flush_memtable` (freeze → write L0 SST → release)
//! - `load_levels` / `rewrite_levels` for manifest integration
//!
//! Compaction lands in phase 5d; this phase only handles memtable → L0
//! flushes. Until compaction runs, L0 grows unboundedly.
//!
//! # Concurrency
//!
//! - Memtable owns its own `RwLock` (see [`Memtable`](super::Memtable)).
//! - `levels` sits behind a `RwLock<Vec<Vec<SstHandle>>>`. Reads snapshot
//!   the vector (cheap; handles are 88-byte `Copy`) and release the lock
//!   before doing any page IO.
//! - `flush_lock` serializes concurrent flushers. A second flusher
//!   waiting on this lock will find the frozen slot already handled by
//!   the first and no-op out.

use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::cache::{DEFAULT_PAGE_CACHE_BYTES, PageCache};
use crate::error::{MetaDbError, Result};
use crate::page_store::PageStore;
use crate::types::{Lsn, PageId};

use super::format::{DedupValue, Hash32};
use super::memtable::{DedupOp, LookupResult, Memtable, MemtableStats};
use super::persist::{free_level, read_levels, rewrite_levels};
use super::sst::{SstHandle, SstReader, SstWriter};

/// Static tunables for one `Lsm` instance.
#[derive(Clone, Debug)]
pub struct LsmConfig {
    /// Soft limit at which the memtable is expected to be frozen.
    pub memtable_bytes: usize,
    /// Bloom filter budget, in bits per entry.
    pub bits_per_entry: u32,
    /// L0 SST count at which `L0 → L1` compaction should fire.
    pub l0_sst_count_trigger: usize,
    /// Records per output SST during compaction. Target only; the final
    /// SST in a compaction may hold fewer.
    pub target_sst_records: usize,
    /// Per-level record-count ratio used when computing `Ln` size
    /// budgets.
    pub level_ratio: u32,
}

impl Default for LsmConfig {
    fn default() -> Self {
        let memtable_bytes = 8 * 1024 * 1024; // 8 MiB
        Self {
            memtable_bytes,
            bits_per_entry: super::bloom::DEFAULT_BITS_PER_ENTRY,
            l0_sst_count_trigger: 4,
            target_sst_records: memtable_bytes / super::LSM_RECORD_SIZE,
            level_ratio: 10,
        }
    }
}

/// Counters for introspection / testing.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct LsmStats {
    pub memtable: MemtableStats,
    pub level_count: usize,
    pub total_ssts: usize,
    pub total_records: u64,
}

/// Top-level LSM instance.
#[derive(Debug)]
pub struct Lsm {
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    memtable: Memtable,
    levels: RwLock<Vec<Vec<SstHandle>>>,
    /// Serialises flush + compaction. Either path mutates `levels`, so
    /// they must not run concurrently.
    modify_lock: Mutex<()>,
    /// Read side held by `get` while it does SST IO; write side used by
    /// compaction as a drain barrier before freeing victim pages.
    reader_drain: RwLock<()>,
    config: LsmConfig,
}

impl Lsm {
    /// Fresh LSM with one empty level (L0) and no SSTs.
    pub fn create(page_store: Arc<PageStore>, config: LsmConfig) -> Self {
        let page_cache = Arc::new(PageCache::new(page_store.clone(), DEFAULT_PAGE_CACHE_BYTES));
        Self::create_with_cache(page_store, page_cache, config)
    }

    /// Fresh LSM using the shared `page_cache`.
    pub fn create_with_cache(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        config: LsmConfig,
    ) -> Self {
        Self {
            page_store,
            page_cache,
            memtable: Memtable::new(config.memtable_bytes),
            levels: RwLock::new(vec![Vec::new()]),
            modify_lock: Mutex::new(()),
            reader_drain: RwLock::new(()),
            config,
        }
    }

    /// Open an LSM whose level chain heads are already known. Each head
    /// points at the first page of that level's handle chain (or
    /// `NULL_PAGE` for an empty level).
    pub fn open(
        page_store: Arc<PageStore>,
        config: LsmConfig,
        level_heads: &[PageId],
    ) -> Result<Self> {
        let page_cache = Arc::new(PageCache::new(page_store.clone(), DEFAULT_PAGE_CACHE_BYTES));
        Self::open_with_cache(page_store, page_cache, config, level_heads)
    }

    /// Open an LSM using the shared `page_cache`.
    pub fn open_with_cache(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        config: LsmConfig,
        level_heads: &[PageId],
    ) -> Result<Self> {
        let mut levels = read_levels(&page_store, level_heads)?;
        // Always guarantee at least L0 exists so `put` / `flush_memtable`
        // can push into `levels[0]` unconditionally.
        if levels.is_empty() {
            levels.push(Vec::new());
        }
        Ok(Self {
            page_store,
            page_cache,
            memtable: Memtable::new(config.memtable_bytes),
            levels: RwLock::new(levels),
            modify_lock: Mutex::new(()),
            reader_drain: RwLock::new(()),
            config,
        })
    }

    // -------- public ops ------------------------------------------------

    pub fn put(&self, hash: Hash32, value: DedupValue) {
        self.memtable.put(hash, value);
    }

    pub fn delete(&self, hash: Hash32) {
        self.memtable.delete(hash);
    }

    pub fn get(&self, hash: &Hash32) -> Result<Option<DedupValue>> {
        // Hold the drain lock through the whole lookup so compaction
        // cannot free victim SST pages while we are still reading them.
        let _drain = self.reader_drain.read();
        self.lookup_locked(hash)
    }

    /// Batched point lookup. Shares one `reader_drain` acquisition and
    /// one `levels` snapshot across all input hashes, avoiding the
    /// re-snapshot cost the single-key `get` pays per call.
    ///
    /// Output order matches input order. Duplicate hashes produce
    /// repeated results (no de-dup on the caller's behalf).
    pub fn multi_get(&self, hashes: &[Hash32]) -> Result<Vec<Option<DedupValue>>> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let _drain = self.reader_drain.read();
        let snapshot = self.levels.read().clone();
        let mut out = Vec::with_capacity(hashes.len());
        for hash in hashes {
            out.push(self.lookup_with_snapshot(hash, &snapshot)?);
        }
        Ok(out)
    }

    fn lookup_locked(&self, hash: &Hash32) -> Result<Option<DedupValue>> {
        match self.memtable.get(hash) {
            LookupResult::Hit(v) => return Ok(Some(v)),
            LookupResult::Tombstone => return Ok(None),
            LookupResult::Miss => {}
        }
        // Snapshot the per-level handle vector. Handles are 88-byte
        // `Copy`, so cloning is cheap; the main cost is the underlying
        // Vec allocations.
        let snapshot = self.levels.read().clone();
        self.search_levels(hash, &snapshot)
    }

    fn lookup_with_snapshot(
        &self,
        hash: &Hash32,
        snapshot: &[Vec<SstHandle>],
    ) -> Result<Option<DedupValue>> {
        match self.memtable.get(hash) {
            LookupResult::Hit(v) => return Ok(Some(v)),
            LookupResult::Tombstone => return Ok(None),
            LookupResult::Miss => {}
        }
        self.search_levels(hash, snapshot)
    }

    fn search_levels(
        &self,
        hash: &Hash32,
        snapshot: &[Vec<SstHandle>],
    ) -> Result<Option<DedupValue>> {
        for (level_idx, handles) in snapshot.iter().enumerate() {
            let result = if level_idx == 0 {
                self.find_in_overlapping(handles.iter().rev(), hash)?
            } else {
                self.find_in_disjoint(handles, hash)?
            };
            match result {
                LookupResult::Hit(v) => return Ok(Some(v)),
                LookupResult::Tombstone => return Ok(None),
                LookupResult::Miss => {}
            }
        }
        Ok(None)
    }

    /// `true` if the memtable has reached the freeze threshold.
    pub fn should_flush(&self) -> bool {
        self.memtable.should_freeze() || self.memtable.has_frozen()
    }

    /// Freeze the active memtable, write it out as a new L0 SST, and
    /// release the frozen slot. Returns the new SST handle, or `None`
    /// if there was nothing to flush.
    ///
    /// The `generation` LSN is stamped on every new page. Supplying a
    /// strictly monotonic generation is the caller's responsibility;
    /// breaking monotonicity does not corrupt the tree but may confuse
    /// the verifier.
    pub fn flush_memtable(&self, generation: Lsn) -> Result<Option<SstHandle>> {
        let _guard = self.modify_lock.lock();

        // If a previous flush failed after freezing but before the write
        // completed, the frozen slot is still populated. Resume from
        // there instead of re-freezing an empty active map.
        let frozen = match self.memtable.frozen_snapshot() {
            Some(f) => f,
            None => {
                if self.memtable.stats().active_entries == 0 {
                    return Ok(None);
                }
                self.memtable.freeze()?
            }
        };

        let writer = SstWriter::new(&self.page_store, generation)
            .with_bits_per_entry(self.config.bits_per_entry);
        let handle = match writer.write_memtable(&frozen)? {
            Some(h) => h,
            None => {
                // Frozen was empty (all tombstones of never-existing
                // keys? only way to happen is via the empty-active path,
                // which we already bailed out of). Release and report
                // no flush.
                self.memtable.release_frozen()?;
                return Ok(None);
            }
        };

        {
            let mut levels = self.levels.write();
            levels[0].push(handle);
        }
        self.memtable.release_frozen()?;
        Ok(Some(handle))
    }

    // -------- compaction ------------------------------------------------

    /// Run at most one compaction round. Returns the report for the
    /// round that fired, or `None` if every level is within budget.
    ///
    /// The function returns after a single round; a caller that wants
    /// the LSM fully within budget should loop:
    ///
    /// ```ignore
    /// while lsm.compact_once(gen)?.is_some() {}
    /// ```
    pub fn compact_once(&self, generation: Lsn) -> Result<Option<super::CompactionReport>> {
        let _modify = self.modify_lock.lock();
        let snapshot = self.levels.read().clone();

        let plan_opt = self.choose_plan(&snapshot);
        let Some(plan) = plan_opt else {
            return Ok(None);
        };

        let outcome = super::compact::execute_plan(
            &self.page_store,
            &self.page_cache,
            generation,
            self.config.bits_per_entry,
            &plan,
        )?;

        // Swap handles under the levels lock.
        {
            let mut levels = self.levels.write();
            super::compact::apply_plan(&mut levels, &plan, &outcome.new_handles);
        }

        // Drain readers that started before the swap. Acquiring +
        // dropping the write side is a barrier; after this point no
        // reader holds a reference to any of the victim handles.
        drop(self.reader_drain.write());

        let victims_count = plan.from_victims.len() + plan.to_victims.len();
        let mut all_victims = plan.from_victims.clone();
        all_victims.extend(plan.to_victims.iter().copied());
        super::compact::free_victims(&self.page_store, &self.page_cache, generation, &all_victims)?;

        Ok(Some(super::CompactionReport {
            from_level: plan.from_level,
            to_level: plan.to_level,
            victims: victims_count,
            new_ssts: outcome.new_handles.len(),
            records_in: outcome.records_in,
            records_out: outcome.records_out,
        }))
    }

    fn choose_plan(&self, levels: &[Vec<SstHandle>]) -> Option<super::compact::Plan> {
        if let Some(p) = super::compact::plan_l0_to_l1(
            levels,
            self.config.l0_sst_count_trigger,
            self.config.target_sst_records,
        ) {
            return Some(p);
        }
        for level in 1..levels.len() {
            if let Some(p) = super::compact::plan_ln_to_next(
                levels,
                level,
                self.config.target_sst_records,
                self.config.l0_sst_count_trigger,
                self.config.level_ratio,
            ) {
                return Some(p);
            }
        }
        None
    }

    // -------- prefix scan ------------------------------------------------

    /// Collect every live (non-tombstoned) record whose key begins with
    /// `prefix`. Resolves shadowing correctly: a newer tombstone hides
    /// an older put, and a newer put overrides an older one.
    ///
    /// Used by `dedup_reverse` to find all (pba, hash) rows that a
    /// given PBA owns. Cost: one read + scan of every memtable plus
    /// every SST whose min/max hash range overlaps the prefix. Not
    /// optimal — a real range iterator lands in phase 8.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Hash32, DedupValue)>> {
        let _drain = self.reader_drain.read();
        let snapshot = self.levels.read().clone();
        self.scan_prefix_with_snapshot(prefix, &snapshot)
    }

    /// Batched prefix scan. Shares `reader_drain` and `levels` snapshot
    /// across all input prefixes. Output order matches input order; each
    /// output Vec is the result of the corresponding prefix scan.
    ///
    /// Single-prefix cost is unchanged; the saving comes from amortising
    /// snapshot allocation and the drain lock across many prefixes. For
    /// the intended caller (dedup cleanup of N dead PBAs) that is a
    /// measurable win when N grows.
    pub fn multi_scan_prefix(&self, prefixes: &[&[u8]]) -> Result<Vec<Vec<(Hash32, DedupValue)>>> {
        if prefixes.is_empty() {
            return Ok(Vec::new());
        }
        let _drain = self.reader_drain.read();
        let snapshot = self.levels.read().clone();
        let mut out = Vec::with_capacity(prefixes.len());
        for prefix in prefixes {
            out.push(self.scan_prefix_with_snapshot(prefix, &snapshot)?);
        }
        Ok(out)
    }

    fn scan_prefix_with_snapshot(
        &self,
        prefix: &[u8],
        snapshot: &[Vec<SstHandle>],
    ) -> Result<Vec<(Hash32, DedupValue)>> {
        use std::collections::BTreeMap;
        // `seen` maps key → Option<DedupValue>. Some = live put, None
        // = tombstone. First insertion wins (newest layer).
        let mut seen: BTreeMap<Hash32, Option<DedupValue>> = BTreeMap::new();

        // Memtable (newest) — shadows everything.
        for (k, op) in self.memtable.collect_prefix(prefix) {
            seen.entry(k).or_insert(match op {
                DedupOp::Put(v) => Some(v),
                DedupOp::Delete => None,
            });
        }

        // SSTs, newest-to-oldest.
        for (level_idx, handles) in snapshot.iter().enumerate() {
            let ordered: Vec<SstHandle> = if level_idx == 0 {
                handles.iter().rev().copied().collect()
            } else {
                handles.to_vec()
            };
            for handle in ordered {
                if !prefix_might_match_range(&handle.min_hash, &handle.max_hash, prefix) {
                    continue;
                }
                let reader = SstReader::open(&self.page_store, &self.page_cache, handle)?;
                for rec_result in reader.scan() {
                    let rec = rec_result?;
                    if !key_has_prefix(rec.hash(), prefix) {
                        continue;
                    }
                    seen.entry(*rec.hash()).or_insert(if rec.is_put() {
                        Some(rec.value())
                    } else {
                        None
                    });
                }
            }
        }

        Ok(seen
            .into_iter()
            .filter_map(|(k, v)| v.map(|val| (k, val)))
            .collect())
    }

    // -------- introspection ---------------------------------------------

    /// Snapshot of the current level → SST table.
    pub fn levels_snapshot(&self) -> Vec<Vec<SstHandle>> {
        self.levels.read().clone()
    }

    pub fn stats(&self) -> LsmStats {
        let levels = self.levels.read();
        let total_ssts = levels.iter().map(|l| l.len()).sum();
        let total_records = levels
            .iter()
            .flat_map(|l| l.iter())
            .map(|h| h.record_count)
            .sum();
        LsmStats {
            memtable: self.memtable.stats(),
            level_count: levels.len(),
            total_ssts,
            total_records,
        }
    }

    // -------- persistence -----------------------------------------------

    /// Persist the current level → SST table to fresh on-disk chains,
    /// returning the new per-level head page ids. Does NOT free any
    /// old chains; the caller is expected to hold the previous head
    /// list, commit the manifest with the new heads, then pass the old
    /// heads to [`free_old_level_heads`](Lsm::free_old_level_heads).
    ///
    /// Crash safety: if the process dies after new chains are written
    /// but before the manifest commit, the new pages are garbage.
    /// They will be reclaimed next time a full rewrite runs (since they
    /// aren't reachable from the committed manifest). The page file
    /// grows slightly but stays consistent.
    pub fn persist_levels(&self, generation: Lsn) -> Result<Vec<PageId>> {
        let levels = self.levels.read();
        rewrite_levels(&self.page_store, &levels, generation)
    }

    /// Free a previously-persisted set of level heads. Idempotent for
    /// empty-chain heads (`NULL_PAGE`).
    pub fn free_old_level_heads(&self, old_heads: &[PageId], generation: Lsn) -> Result<()> {
        for &head in old_heads {
            free_level(&self.page_store, head, generation)?;
        }
        Ok(())
    }

    // -------- test helpers ----------------------------------------------

    #[cfg(test)]
    pub(crate) fn debug_replace_levels(&self, new_levels: Vec<Vec<SstHandle>>) {
        *self.levels.write() = new_levels;
    }

    // -------- internal --------------------------------------------------

    fn find_in_overlapping<'a, I>(&self, handles: I, hash: &Hash32) -> Result<LookupResult>
    where
        I: IntoIterator<Item = &'a SstHandle>,
    {
        for handle in handles {
            if hash < &handle.min_hash || hash > &handle.max_hash {
                continue;
            }
            let reader = SstReader::open(&self.page_store, &self.page_cache, *handle)?;
            match reader.get(hash)? {
                LookupResult::Hit(v) => return Ok(LookupResult::Hit(v)),
                LookupResult::Tombstone => return Ok(LookupResult::Tombstone),
                LookupResult::Miss => {}
            }
        }
        Ok(LookupResult::Miss)
    }

    fn find_in_disjoint(&self, handles: &[SstHandle], hash: &Hash32) -> Result<LookupResult> {
        // Disjoint levels can be binary-searched by (min, max); for the
        // expected small per-level sizes a linear scan is fine and
        // cache-friendly. Revisit once levels grow past a few hundred
        // SSTs (phase 8).
        for handle in handles {
            if hash < &handle.min_hash || hash > &handle.max_hash {
                continue;
            }
            let reader = SstReader::open(&self.page_store, &self.page_cache, *handle)?;
            return reader.get(hash);
        }
        Ok(LookupResult::Miss)
    }
}

mod helpers;

use helpers::*;

#[cfg(test)]
mod tests;
