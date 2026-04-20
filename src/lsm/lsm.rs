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
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            memtable_bytes: 8 * 1024 * 1024, // 8 MiB
            bits_per_entry: super::bloom::DEFAULT_BITS_PER_ENTRY,
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
    flush_lock: Mutex<()>,
    config: LsmConfig,
}

impl Lsm {
    /// Fresh LSM with one empty level (L0) and no SSTs.
    pub fn create(page_store: Arc<PageStore>, config: LsmConfig) -> Self {
        Self {
            page_store,
            memtable: Memtable::new(config.memtable_bytes),
            levels: RwLock::new(vec![Vec::new()]),
            flush_lock: Mutex::new(()),
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
            flush_lock: Mutex::new(()),
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
        let _guard = self.flush_lock.lock();

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
}
