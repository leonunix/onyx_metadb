//! Per-L2P-page refcount store: how many tree parents (across every volume
//! and snapshot) point at each paged-L2P `PageId`.
//!
//! This refcount used to live in the L2P page header (`PageHeader.refcount`).
//! The flush IO path writes whole pages and bypasses the per-pid `rc_locks`,
//! so an in-flight flush could clobber a concurrent incref's rc field — which
//! is exactly why `take_snapshot` / `clone_volume` / `drop_snapshot` had to
//! force a global TXG sync (drain in-flight flushes) before touching root rc.
//! Relocating the page rc OUT of the data page removes that coupling: the
//! flush no longer writes rc, so the lifecycle ops can incref/decref roots
//! inline, per-volume, with no global barrier.
//!
//! It reuses the soaked [`crate::refcount::RcShard`] verbatim: `PageId` and
//! `Pba` are both `u64`, and `RcShard` only needs a `u64` key + a `u32` count.
//! The `RcEntry.birth_lsn` field is unused for page rc (it is meaningful only
//! for the PBA dead-list suppression) and is left at its incidental value.
//!
//! Concurrency, durability, and the fold/consistent-read semantics are
//! identical to the PBA refcount store (see `refcount/shard.rs`): a 4-slot
//! TXG ring of delta maps folded on the Syncing-slot boundary, publish-before-
//! clear, `get_consistent` for any "rc==0 ⇒ free this page" decision.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use xxhash_rust::xxh3::xxh3_64;

use crate::cache::PageCache;
use crate::error::{MetaDbError, Result};
use crate::page_store::PageStore;
use crate::refcount::RcShard;
use crate::types::{Lsn, PageId, Txg};

/// Sharded per-page refcount store, keyed by `PageId`. Each shard is an
/// independent [`RcShard`]; routing mirrors `Db::refcount_shard_for`.
///
/// Each shard tracks its own `last_flushed_lsn` (the highest LSN whose
/// deltas are durable on disk), the exact analogue of the per-shard
/// atomic on the PBA-refcount `Shard` wrapper. The flush's TXG-sync
/// cycle folds the page-rc shards on the same boundary as the PBA
/// refcount shards and bumps these atomics in lock-step, so the
/// manifest's `l2p_page_rc_durable_seq` array joins the
/// `min(durable_seq[]) == checkpoint_lsn` invariant.
pub struct L2pPageRc {
    shards: Vec<Arc<RcShard>>,
    last_flushed_lsn: Vec<AtomicU64>,
}

impl L2pPageRc {
    /// Create `shard_count` fresh shards. Returns the per-shard meta-page ids
    /// (the manifest roots) in shard order.
    pub fn create(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        shard_count: usize,
    ) -> Result<(Self, Vec<PageId>)> {
        assert!(shard_count > 0, "l2p_page_rc shard_count must be > 0");
        let mut shards = Vec::with_capacity(shard_count);
        let mut roots = Vec::with_capacity(shard_count);
        let mut last_flushed_lsn = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            let shard = RcShard::create(page_store.clone(), page_cache.clone())?;
            roots.push(shard.meta_page_id());
            shards.push(Arc::new(shard));
            // Fresh shard: nothing folded yet, durable_seq == 0 (matches
            // the empty `checkpoint_lsn`).
            last_flushed_lsn.push(AtomicU64::new(0));
        }
        Ok((
            Self {
                shards,
                last_flushed_lsn,
            },
            roots,
        ))
    }

    /// Open existing shards from the manifest-recorded meta-page ids and
    /// per-shard durable_seq. `initial_last_flushed_lsn` mirrors
    /// `roots` (the manifest's `l2p_page_rc_durable_seq`); recovery
    /// restores each shard's atomic independently rather than collapsing
    /// through the global `checkpoint_lsn`.
    pub fn open(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        roots: &[PageId],
        initial_last_flushed_lsn: &[Lsn],
    ) -> Result<Self> {
        if initial_last_flushed_lsn.len() != roots.len() {
            return Err(MetaDbError::Corruption(format!(
                "l2p_page_rc open: durable_seq length {} != roots length {}",
                initial_last_flushed_lsn.len(),
                roots.len(),
            )));
        }
        let mut shards = Vec::with_capacity(roots.len());
        let mut last_flushed_lsn = Vec::with_capacity(roots.len());
        for (idx, &meta_page_id) in roots.iter().enumerate() {
            shards.push(Arc::new(RcShard::open(
                page_store.clone(),
                page_cache.clone(),
                meta_page_id,
            )?));
            last_flushed_lsn.push(AtomicU64::new(initial_last_flushed_lsn[idx]));
        }
        Ok(Self {
            shards,
            last_flushed_lsn,
        })
    }

    /// Shard index for `pid`. Same hash shape as the PBA refcount routing
    /// (`xxh3_64` of the big-endian key) so the two are independently sharded.
    #[inline]
    pub fn shard_for(&self, pid: PageId) -> usize {
        (xxh3_64(&pid.to_be_bytes()) as usize) % self.shards.len()
    }

    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub fn shards(&self) -> &[Arc<RcShard>] {
        &self.shards
    }

    pub fn shard(&self, idx: usize) -> &Arc<RcShard> {
        &self.shards[idx]
    }

    /// Current per-shard meta-page ids (manifest roots), in shard order. The
    /// head meta page is stable across opens, so these do not change across a
    /// fold; recorded once in the manifest at create time.
    pub fn roots(&self) -> Vec<PageId> {
        self.shards.iter().map(|s| s.meta_page_id()).collect()
    }

    /// Highest LSN whose deltas for shard `idx` are durable on disk.
    pub fn last_flushed_lsn(&self, idx: usize) -> Lsn {
        self.last_flushed_lsn[idx].load(Ordering::Acquire)
    }

    /// Bump shard `idx`'s durable watermark to `lsn` if higher. `fetch_max`
    /// (not `store`) keeps it monotonic when a Syncing slot had no commits
    /// (`wal_checkpoint == 0`) — matches the PBA-refcount post-manifest store.
    pub fn fetch_max_last_flushed(&self, idx: usize, lsn: Lsn) {
        self.last_flushed_lsn[idx].fetch_max(lsn, Ordering::Release);
    }

    /// Per-shard durable_seq snapshot, in shard order (for the manifest's
    /// `l2p_page_rc_durable_seq`).
    pub fn durable_seq(&self) -> Vec<Lsn> {
        self.last_flushed_lsn
            .iter()
            .map(|a| a.load(Ordering::Acquire))
            .collect()
    }

    /// Logical refcount for `pid` (ring-summed + array base). Hot read; may
    /// straddle a concurrent fold's publish/clear window (transient over-count
    /// only — never a spurious 0). Use [`Self::get_consistent`] for any free
    /// decision.
    pub fn get(&self, pid: PageId) -> Result<u32> {
        self.shards[self.shard_for(pid)].get(pid)
    }

    /// Fold-consistent refcount for `pid`. Required for any "rc==0 ⇒ free this
    /// page" decision (decref cascade / drop plan): the cheap `get` can floor a
    /// live rc to a spurious 0 across the fold's [publish, clear] window.
    pub fn get_consistent(&self, pid: PageId) -> Result<u32> {
        self.shards[self.shard_for(pid)].get_consistent(pid)
    }

    /// Stage a refcount delta for `pid` into `txg`'s ring slot. Returns the
    /// cumulative `(prev_rc, new_rc)`; callers surface a freed page on
    /// `new == 0 && prev > 0`.
    pub fn stage(&self, txg: Txg, pid: PageId, delta: i64, lsn: Lsn) -> Result<(u32, u32)> {
        self.shards[self.shard_for(pid)].stage(txg, pid, delta, lsn)
    }

    /// Cold-path flush: fold every shard's pending deltas to disk and rotate
    /// each meta chain. The hot path folds via the global TXG sync cycle
    /// (`begin_checkpoint` per shard, wired in Phase A2); this is for the
    /// non-checkpoint / recovery / test paths (analogue of `RcShard::flush`).
    pub fn flush(&self) -> Result<()> {
        for shard in &self.shards {
            shard.flush()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const T0: Txg = 0;

    fn make_store(shard_count: usize) -> (TempDir, Arc<PageStore>, Arc<PageCache>, L2pPageRc, Vec<PageId>) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let page_store = Arc::new(PageStore::create(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let (store, roots) =
            L2pPageRc::create(page_store.clone(), page_cache.clone(), shard_count).unwrap();
        (dir, page_store, page_cache, store, roots)
    }

    #[test]
    fn stage_then_get_sees_pending() {
        let (_d, _ps, _pc, store, _roots) = make_store(4);
        assert_eq!(store.stage(T0, 100, 1, 10).unwrap(), (0, 1));
        assert_eq!(store.get(100).unwrap(), 1);
        // A different PageId routed (possibly) to another shard is independent.
        assert_eq!(store.get(200).unwrap(), 0);
    }

    #[test]
    fn incref_decref_accumulate() {
        let (_d, _ps, _pc, store, _roots) = make_store(2);
        store.stage(T0, 7, 1, 1).unwrap();
        store.stage(T0, 7, 1, 2).unwrap();
        store.stage(T0, 7, -1, 3).unwrap();
        assert_eq!(store.get(7).unwrap(), 1);
        assert_eq!(store.get_consistent(7).unwrap(), 1);
    }

    #[test]
    fn flush_persists_across_reopen() {
        let (_d, ps, pc, store, roots) = make_store(4);
        // Several pages routed across shards.
        for pid in [1u64, 2, 3, 50, 99, 1000, 4096] {
            store.stage(T0, pid, (pid % 5 + 1) as i64, pid).unwrap();
        }
        store.flush().unwrap();
        // Roots are stable across the fold (head meta page is stable).
        assert_eq!(store.roots(), roots);
        drop(store);

        let reopened = L2pPageRc::open(ps, pc, &roots, &vec![0; roots.len()]).unwrap();
        for pid in [1u64, 2, 3, 50, 99, 1000, 4096] {
            assert_eq!(reopened.get(pid).unwrap(), (pid % 5 + 1) as u32, "pid {pid}");
        }
        // An untouched page reads 0.
        assert_eq!(reopened.get(123456).unwrap(), 0);
    }

    #[test]
    fn decref_to_zero_reports_freed() {
        let (_d, _ps, _pc, store, _roots) = make_store(2);
        store.stage(T0, 42, 1, 1).unwrap();
        let (prev, new) = store.stage(T0, 42, -1, 2).unwrap();
        assert_eq!((prev, new), (1, 0));
        assert_eq!(store.get(42).unwrap(), 0);
    }
}
