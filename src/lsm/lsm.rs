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
        Self {
            page_store,
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
        let mut levels = read_levels(&page_store, level_heads)?;
        // Always guarantee at least L0 exists so `put` / `flush_memtable`
        // can push into `levels[0]` unconditionally.
        if levels.is_empty() {
            levels.push(Vec::new());
        }
        Ok(Self {
            page_store,
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
        match self.memtable.get(hash) {
            LookupResult::Hit(v) => return Ok(Some(v)),
            LookupResult::Tombstone => return Ok(None),
            LookupResult::Miss => {}
        }
        // Snapshot the per-level handle vector. Handles are 88-byte
        // `Copy`, so cloning is cheap; the main cost is the underlying
        // Vec allocations.
        let snapshot = self.levels.read().clone();
        for (level_idx, handles) in snapshot.iter().enumerate() {
            let result = if level_idx == 0 {
                // L0 SSTs may overlap in hash range. Newest first.
                self.find_in_overlapping(handles.iter().rev(), hash)?
            } else {
                // L1+ SSTs are disjoint. At most one can match.
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
        super::compact::free_victims(&self.page_store, generation, &all_victims)?;

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
        let snapshot = self.levels.read().clone();
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
                let reader = SstReader::open(&self.page_store, handle)?;
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
            let reader = SstReader::open(&self.page_store, *handle)?;
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
            let reader = SstReader::open(&self.page_store, *handle)?;
            return reader.get(hash);
        }
        Ok(LookupResult::Miss)
    }
}

// Silence an otherwise-useful but currently-unused warning on the
// DedupOp import path; `use` is needed to keep the `_` pattern matching
// from turning into a dead-code warning if this file ever lands without
// `flush_memtable` being exercised.
#[allow(dead_code)]
fn key_has_prefix(key: &Hash32, prefix: &[u8]) -> bool {
    key.len() >= prefix.len() && &key[..prefix.len()] == prefix
}

/// Whether keys starting with `prefix` could fall within `[min, max]`.
/// Conservative: returns true unless we can prove no match exists.
fn prefix_might_match_range(min: &Hash32, max: &Hash32, prefix: &[u8]) -> bool {
    if prefix.is_empty() {
        return true;
    }
    // Any key starting with `prefix` lies in `[prefix || 0x00..,
    // prefix || 0xFF..]` inclusive. Overlap with `[min, max]` means
    // `prefix_upper >= min && prefix_lower <= max`.
    let mut lower = [0u8; 32];
    lower[..prefix.len()].copy_from_slice(prefix);
    let mut upper = [0xFFu8; 32];
    upper[..prefix.len()].copy_from_slice(prefix);
    upper.as_slice() >= min.as_slice() && lower.as_slice() <= max.as_slice()
}

#[allow(dead_code)]
fn _dedup_op_import_anchor(op: DedupOp) -> DedupOp {
    op
}

// Convert a private `MetaDbError::InvalidArgument` into a more specific
// message when a caller misuses `Lsm` APIs. Kept as a helper to centralise
// the wording.
#[allow(dead_code)]
fn invalid(msg: &str) -> MetaDbError {
    MetaDbError::InvalidArgument(msg.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsm::format::DedupValue;
    use tempfile::TempDir;

    fn mk_lsm() -> (TempDir, Arc<PageStore>, Lsm) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = Arc::new(PageStore::create(&path).unwrap());
        let lsm = Lsm::create(ps.clone(), LsmConfig::default());
        (dir, ps, lsm)
    }

    fn h(n: u64) -> Hash32 {
        let mut out = [0u8; 32];
        out[..8].copy_from_slice(&n.to_be_bytes());
        out
    }

    fn v(n: u8) -> DedupValue {
        let mut x = [0u8; 28];
        x[0] = n;
        DedupValue(x)
    }

    #[test]
    fn empty_lsm_misses() {
        let (_d, _ps, lsm) = mk_lsm();
        assert_eq!(lsm.get(&h(0)).unwrap(), None);
    }

    #[test]
    fn memtable_put_then_get() {
        let (_d, _ps, lsm) = mk_lsm();
        lsm.put(h(1), v(10));
        lsm.put(h(2), v(20));
        assert_eq!(lsm.get(&h(1)).unwrap(), Some(v(10)));
        assert_eq!(lsm.get(&h(2)).unwrap(), Some(v(20)));
        assert_eq!(lsm.get(&h(3)).unwrap(), None);
    }

    #[test]
    fn tombstone_masks_older_put() {
        let (_d, _ps, lsm) = mk_lsm();
        lsm.put(h(1), v(1));
        lsm.flush_memtable(1).unwrap().unwrap(); // L0 has put
        lsm.delete(h(1)); // tombstone in memtable
        assert_eq!(lsm.get(&h(1)).unwrap(), None);
        lsm.flush_memtable(2).unwrap().unwrap(); // L0 has [put, tombstone]
        assert_eq!(lsm.get(&h(1)).unwrap(), None);
    }

    #[test]
    fn flush_creates_l0_sst_and_clears_active() {
        let (_d, _ps, lsm) = mk_lsm();
        for i in 0..100u64 {
            lsm.put(h(i), v(i as u8));
        }
        let handle = lsm.flush_memtable(1).unwrap().unwrap();
        assert_eq!(handle.record_count, 100);
        let levels = lsm.levels_snapshot();
        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0].len(), 1);
        assert_eq!(lsm.stats().memtable.active_entries, 0);
        assert!(!lsm.memtable.has_frozen());

        // Get should now hit the SST, not the memtable.
        for i in 0..100u64 {
            assert_eq!(lsm.get(&h(i)).unwrap(), Some(v(i as u8)));
        }
    }

    #[test]
    fn newer_l0_sst_shadows_older() {
        let (_d, _ps, lsm) = mk_lsm();
        lsm.put(h(1), v(1));
        lsm.flush_memtable(1).unwrap().unwrap();
        lsm.put(h(1), v(2));
        lsm.flush_memtable(2).unwrap().unwrap();
        // L0 now has two SSTs; newest (v(2)) wins.
        assert_eq!(lsm.get(&h(1)).unwrap(), Some(v(2)));
    }

    #[test]
    fn flush_with_empty_memtable_returns_none() {
        let (_d, _ps, lsm) = mk_lsm();
        assert!(lsm.flush_memtable(1).unwrap().is_none());
    }

    #[test]
    fn flush_memtable_with_only_tombstones() {
        let (_d, _ps, lsm) = mk_lsm();
        lsm.delete(h(1));
        lsm.delete(h(2));
        let h_result = lsm.flush_memtable(1).unwrap().unwrap();
        assert_eq!(h_result.record_count, 2);
        assert_eq!(lsm.get(&h(1)).unwrap(), None);
    }

    #[test]
    fn persist_and_reopen_round_trip() {
        let (dir, ps, lsm) = mk_lsm();
        for i in 0..50u64 {
            lsm.put(h(i), v(i as u8));
        }
        lsm.flush_memtable(1).unwrap();
        for i in 50..100u64 {
            lsm.put(h(i), v(i as u8));
        }
        lsm.flush_memtable(2).unwrap();
        let heads = lsm.persist_levels(3).unwrap();
        // Drop the in-memory LSM; the SST pages remain on disk.
        drop(lsm);
        let ps2 = ps.clone();
        let lsm2 = Lsm::open(ps2, LsmConfig::default(), &heads).unwrap();
        let levels = lsm2.levels_snapshot();
        assert_eq!(levels.len(), 1);
        assert_eq!(levels[0].len(), 2);
        for i in 0..100u64 {
            assert_eq!(
                lsm2.get(&h(i)).unwrap(),
                Some(v(i as u8)),
                "mismatch at {i}"
            );
        }
        drop(dir);
    }

    #[test]
    fn put_after_flush_auto_resumes_frozen_if_needed() {
        // Ensure: if flush freezes but we put first, the new put goes
        // into a fresh active, and then flush sees frozen pre-existing
        // and still writes it correctly.
        let (_d, _ps, lsm) = mk_lsm();
        for i in 0..5u64 {
            lsm.put(h(i), v(1));
        }
        // Manually freeze to simulate mid-flight state.
        let frozen = lsm.memtable.freeze().unwrap();
        // Flusher would have consumed `frozen`, but assume it crashed.
        assert_eq!(frozen.len(), 5);
        // Meanwhile writes continue…
        for i in 5..10u64 {
            lsm.put(h(i), v(2));
        }
        // Retry flush: should consume the existing frozen, NOT re-freeze.
        let handle = lsm.flush_memtable(1).unwrap().unwrap();
        assert_eq!(handle.record_count, 5);
        // The 5 new writes are still in the active memtable.
        assert_eq!(lsm.stats().memtable.active_entries, 5);
        for i in 0..5u64 {
            assert_eq!(lsm.get(&h(i)).unwrap(), Some(v(1)));
        }
        for i in 5..10u64 {
            assert_eq!(lsm.get(&h(i)).unwrap(), Some(v(2)));
        }
    }

    #[test]
    fn l1_disjoint_lookup() {
        let (_d, _ps, lsm) = mk_lsm();
        // Bypass compaction: inject a synthetic L1.
        lsm.put(h(1), v(1));
        let h1 = lsm.flush_memtable(1).unwrap().unwrap();
        lsm.put(h(200), v(2));
        let h2 = lsm.flush_memtable(2).unwrap().unwrap();
        // Move both L0 SSTs into L1 (non-overlapping since they're
        // single-key SSTs at distinct hashes).
        lsm.debug_replace_levels(vec![Vec::new(), vec![h1, h2]]);
        assert_eq!(lsm.get(&h(1)).unwrap(), Some(v(1)));
        assert_eq!(lsm.get(&h(200)).unwrap(), Some(v(2)));
        assert_eq!(lsm.get(&h(50)).unwrap(), None);
    }

    fn small_cfg() -> LsmConfig {
        LsmConfig {
            memtable_bytes: 1024,
            bits_per_entry: 10,
            l0_sst_count_trigger: 3,
            target_sst_records: 50,
            level_ratio: 10,
        }
    }

    fn mk_lsm_small() -> (TempDir, Arc<PageStore>, Lsm) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = Arc::new(PageStore::create(&path).unwrap());
        let lsm = Lsm::create(ps.clone(), small_cfg());
        (dir, ps, lsm)
    }

    #[test]
    fn compact_noop_when_nothing_to_do() {
        let (_d, _ps, lsm) = mk_lsm();
        assert!(lsm.compact_once(1).unwrap().is_none());
    }

    #[test]
    fn compact_l0_to_l1_merges_disjoint_ssts() {
        let (_d, _ps, lsm) = mk_lsm_small();
        for round in 0..3u64 {
            for i in 0..20u64 {
                let key = round * 100 + i;
                lsm.put(h(key), v(key as u8));
            }
            lsm.flush_memtable(round as Lsn + 1).unwrap().unwrap();
        }
        // Under trigger (3 SSTs in L0).
        assert_eq!(lsm.levels_snapshot()[0].len(), 3);
        let report = lsm.compact_once(10).unwrap().unwrap();
        assert_eq!(report.from_level, 0);
        assert_eq!(report.to_level, 1);
        assert_eq!(report.victims, 3); // 3 L0 + 0 L1
        let levels = lsm.levels_snapshot();
        assert!(levels[0].is_empty());
        assert!(!levels[1].is_empty());
        // Every written key still reads correctly.
        for round in 0..3u64 {
            for i in 0..20u64 {
                let key = round * 100 + i;
                assert_eq!(lsm.get(&h(key)).unwrap(), Some(v(key as u8)));
            }
        }
    }

    #[test]
    fn compact_l0_to_l1_merges_overlapping_l1() {
        let (_d, _ps, lsm) = mk_lsm_small();
        // First, populate L1 by flushing + compacting a round that
        // targets hashes 0..50.
        for i in 0..50u64 {
            lsm.put(h(i), v(1));
        }
        lsm.flush_memtable(1).unwrap().unwrap();
        for i in 0..50u64 {
            lsm.put(h(i), v(1));
        }
        lsm.flush_memtable(2).unwrap().unwrap();
        for i in 0..50u64 {
            lsm.put(h(i), v(1));
        }
        lsm.flush_memtable(3).unwrap().unwrap();
        lsm.compact_once(10).unwrap().unwrap(); // pushes to L1
        assert_eq!(lsm.levels_snapshot()[0].len(), 0);
        assert!(!lsm.levels_snapshot()[1].is_empty());

        // Now overwrite the same keys so L0 overlaps L1.
        for i in 0..50u64 {
            lsm.put(h(i), v(2));
        }
        lsm.flush_memtable(11).unwrap().unwrap();
        for i in 0..50u64 {
            lsm.put(h(i), v(2));
        }
        lsm.flush_memtable(12).unwrap().unwrap();
        for i in 0..50u64 {
            lsm.put(h(i), v(2));
        }
        lsm.flush_memtable(13).unwrap().unwrap();
        // Fire compaction; L0 should absorb L1 with newer puts.
        let report = lsm.compact_once(20).unwrap().unwrap();
        assert!(report.victims >= 4, "expected ≥4 victims, got {report:?}");
        for i in 0..50u64 {
            assert_eq!(lsm.get(&h(i)).unwrap(), Some(v(2)));
        }
    }

    #[test]
    fn compact_drops_tombstones_at_deepest_level() {
        let (_d, _ps, lsm) = mk_lsm_small();
        // Put + tombstone for hash 1 across L0.
        for _ in 0..3 {
            lsm.put(h(1), v(1));
            lsm.flush_memtable(1).unwrap().unwrap();
        }
        lsm.delete(h(1));
        lsm.flush_memtable(2).unwrap().unwrap();
        // Four L0 SSTs; fire compaction — target L1 is deepest so
        // tombstones get dropped entirely.
        lsm.compact_once(10).unwrap().unwrap();
        let levels = lsm.levels_snapshot();
        let l1 = &levels[1];
        assert_eq!(
            l1.iter().map(|h| h.record_count).sum::<u64>(),
            0,
            "tombstone should have dropped to zero records in L1"
        );
        assert_eq!(lsm.get(&h(1)).unwrap(), None);
    }

    #[test]
    fn compact_preserves_tombstones_when_deeper_level_exists() {
        let (_d, _ps, lsm) = mk_lsm_small();
        // Seed L2 with put(h1) = v(1) by forcing multi-level.
        // Step 1: put + flush + compact to L1.
        for _ in 0..3 {
            lsm.put(h(1), v(1));
            lsm.flush_memtable(1).unwrap().unwrap();
        }
        lsm.compact_once(10).unwrap().unwrap(); // L1 now has put(h1,v1)
        // Step 2: fake-promote L1 content to L2 so L1 is empty and L2
        // holds put(h1,v1). We do this by injecting a synthetic level.
        let levels_now = lsm.levels_snapshot();
        assert_eq!(levels_now.len(), 2);
        let l1_content = levels_now[1].clone();
        lsm.debug_replace_levels(vec![Vec::new(), Vec::new(), l1_content]);
        // Step 3: new L0 with tombstone(h1) and pressure to push to L1.
        for _ in 0..3 {
            lsm.delete(h(1));
            lsm.flush_memtable(11).unwrap().unwrap();
        }
        lsm.compact_once(20).unwrap().unwrap(); // L0 → L1
        // Since L2 exists (deeper), tombstones should survive in L1.
        let levels = lsm.levels_snapshot();
        let l1_total: u64 = levels[1].iter().map(|h| h.record_count).sum();
        assert!(
            l1_total > 0,
            "tombstone must survive into L1 when L2 exists"
        );
        assert_eq!(lsm.get(&h(1)).unwrap(), None); // tombstone shadows L2
    }

    #[test]
    fn repeated_compaction_loop_converges() {
        let (_d, _ps, lsm) = mk_lsm_small();
        // Write a lot of data and keep flushing + compacting.
        for batch in 0..30u64 {
            for i in 0..25u64 {
                lsm.put(h(batch * 100 + i), v((batch + 1) as u8));
            }
            lsm.flush_memtable(batch as Lsn + 1).unwrap();
            while lsm
                .compact_once((batch as Lsn + 1) * 100)
                .unwrap()
                .is_some()
            {}
        }
        // Every key should still be present with its original batch
        // value. Check a subset.
        for batch in 0..30u64 {
            let key = batch * 100 + 13;
            assert_eq!(lsm.get(&h(key)).unwrap(), Some(v((batch + 1) as u8)));
        }
        // Levels should have stabilised (L0 below trigger).
        assert!(lsm.levels_snapshot()[0].len() < small_cfg().l0_sst_count_trigger);
    }
}
