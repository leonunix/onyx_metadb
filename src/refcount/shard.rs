//! Refcount shard: composes [`PagedRefcountArray`] + [`DeltaMap`].
//!
//! Read path: lock delta (read), peek pending delta on top of the
//! on-disk entry, return the merged value. Apply path: lock delta
//! (write), accumulate ops; on `flush()`, drain delta and apply each
//! per-page batch to the array.
//!
//! Lock order: delta first, array internals second (via the array's
//! own internal lock). Never the other way around.

use std::sync::Arc;

use parking_lot::Mutex;

use super::RcEntry;
use crate::cache::PageCache;
use crate::error::Result;
use crate::page_store::PageStore;
use crate::types::{Lsn, PageId, Pba};

use super::apply_delta_pure;
use super::array::PagedRefcountArray;
use super::delta::DeltaMap;

pub struct RcShard {
    delta: Mutex<DeltaMap>,
    array: PagedRefcountArray,
}

impl RcShard {
    /// Create a fresh shard with a freshly allocated meta page.
    pub fn create(page_store: Arc<PageStore>, page_cache: Arc<PageCache>) -> Result<Self> {
        let array = PagedRefcountArray::create(page_store, page_cache)?;
        Ok(Self {
            delta: Mutex::new(DeltaMap::new()),
            array,
        })
    }

    /// Open an existing shard at `meta_page_id` (read from the manifest).
    pub fn open(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        meta_page_id: PageId,
    ) -> Result<Self> {
        let array = PagedRefcountArray::open(page_store, page_cache, meta_page_id)?;
        Ok(Self {
            delta: Mutex::new(DeltaMap::new()),
            array,
        })
    }

    pub fn meta_page_id(&self) -> PageId {
        self.array.meta_page_id()
    }

    /// Logical refcount = on-disk entry + pending delta. ~50 ns hot
    /// path: one mutex acquisition for delta + one disk-cache lookup
    /// (or zero if no data page is allocated for this PBA's region).
    pub fn get(&self, pba: Pba) -> Result<u32> {
        let pending = self.delta.lock().get(pba);
        let base = self.array.get(pba)?;
        let merged = match pending {
            Some(p) => apply_delta_pure(base, p.delta, p.last_lsn)?,
            None => base,
        };
        Ok(merged.rc)
    }

    /// Full entry (rc + birth_lsn). Internal use only — public callers
    /// usually want [`get`].
    pub fn get_entry(&self, pba: Pba) -> Result<RcEntry> {
        let pending = self.delta.lock().get(pba);
        let base = self.array.get(pba)?;
        match pending {
            Some(p) => apply_delta_pure(base, p.delta, p.last_lsn),
            None => Ok(base),
        }
    }

    /// Stage one op into the pending delta. Returns `(prev_rc, new_rc)`
    /// so callers can detect 0→N or N→0 transitions in one mutex
    /// acquisition. Errors on overflow / underflow without mutating
    /// delta state.
    ///
    /// Replay-skip: if there is no pending delta for this PBA yet and
    /// the on-disk data page's stamp `>= lsn`, the op already landed
    /// in a previous run that survived the crash; we return the
    /// current rc unchanged (`prev_rc == new_rc`). This is the only
    /// place the array's `last_applied_lsn` page stamp matters; the
    /// caller does not need to coordinate.
    ///
    /// Per-op apply hot path. ~50 ns in the common path (one mutex
    /// acquisition + one cached-page peek for the LSN compare).
    pub fn stage(&self, pba: Pba, delta: i64, lsn: Lsn) -> Result<(u32, u32)> {
        let mut d = self.delta.lock();
        let prev_pending = d.get(pba);
        let base = self.array.get(pba)?;
        let merged_prev = match prev_pending {
            Some(p) => apply_delta_pure(base, p.delta, p.last_lsn)?,
            None => base,
        };
        // Replay-skip: only safe when no pending delta covers the
        // PBA yet, since pending bytes haven't been persisted and
        // their LSN may exceed `lsn` from this op even though the
        // on-disk state hasn't moved.
        if prev_pending.is_none() && self.array.page_lsn(pba)? >= lsn {
            return Ok((base.rc, base.rc));
        }
        // Validate by computing the post-stage value first; if
        // arithmetic fails the delta map stays untouched.
        let post = apply_delta_pure(merged_prev, delta, lsn)?;
        d.merge(pba, delta, lsn);
        Ok((merged_prev.rc, post.rc))
    }

    /// Drain pending deltas into the on-disk array. Called at commit
    /// boundaries (or when delta map crosses a soft size cap).
    /// Persists the meta page if any new data pages were allocated.
    pub fn flush(&self) -> Result<()> {
        let drained: Vec<(Pba, super::delta::Pending)> = {
            let mut d = self.delta.lock();
            if d.is_empty() {
                return Ok(());
            }
            d.drain().collect()
        };
        self.array.apply_deltas(drained)?;
        self.array.flush_meta()?;
        Ok(())
    }

    /// Iterate every live entry. Forces a flush first so the iteration
    /// reflects committed state without pending overlay aliasing.
    pub fn iter_live_flushed(&self) -> Result<Vec<(Pba, RcEntry)>> {
        self.flush()?;
        self.array.iter_live()
    }

    /// Number of data pages currently on disk for this shard.
    pub fn allocated_data_pages(&self) -> usize {
        self.array.allocated_data_pages()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_shard() -> (TempDir, RcShard) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let page_store = Arc::new(PageStore::create(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let s = RcShard::create(page_store, page_cache).unwrap();
        (dir, s)
    }

    #[test]
    fn stage_then_get_sees_pending() {
        let (_d, s) = make_shard();
        assert_eq!(s.stage(10, 1, 100).unwrap(), (0, 1));
        assert_eq!(s.get(10).unwrap(), 1);
    }

    #[test]
    fn stage_accumulates_across_ops() {
        let (_d, s) = make_shard();
        s.stage(10, 1, 100).unwrap();
        s.stage(10, 2, 101).unwrap();
        s.stage(10, -1, 102).unwrap();
        assert_eq!(s.get(10).unwrap(), 2);
    }

    #[test]
    fn flush_moves_pending_to_array() {
        let (_d, s) = make_shard();
        s.stage(10, 5, 100).unwrap();
        s.flush().unwrap();
        assert_eq!(s.get(10).unwrap(), 5);
        assert_eq!(
            s.get_entry(10).unwrap(),
            RcEntry {
                rc: 5,
                birth_lsn: 100
            }
        );
    }

    #[test]
    fn stage_underflow_does_not_corrupt_delta() {
        let (_d, s) = make_shard();
        s.stage(10, 1, 1).unwrap();
        assert!(s.stage(10, -2, 2).is_err());
        assert_eq!(s.get(10).unwrap(), 1);
    }

    #[test]
    fn zero_to_one_to_zero_to_one_birth_lsn() {
        let (_d, s) = make_shard();
        s.stage(10, 1, 100).unwrap();
        s.flush().unwrap();
        s.stage(10, -1, 101).unwrap();
        s.flush().unwrap();
        assert_eq!(s.get_entry(10).unwrap(), RcEntry::ZERO);
        s.stage(10, 1, 200).unwrap();
        s.flush().unwrap();
        assert_eq!(
            s.get_entry(10).unwrap(),
            RcEntry {
                rc: 1,
                birth_lsn: 200
            }
        );
    }

    #[test]
    fn iter_live_flushed_skips_zero() {
        let (_d, s) = make_shard();
        s.stage(1, 1, 1).unwrap();
        s.stage(2, 1, 1).unwrap();
        s.stage(2, -1, 2).unwrap();
        s.stage(3, 3, 3).unwrap();
        let live = s.iter_live_flushed().unwrap();
        assert_eq!(live.len(), 2);
        assert_eq!(live[0].0, 1);
        assert_eq!(live[1].0, 3);
    }

    #[test]
    fn round_trip_via_open() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let meta_page_id;
        {
            let page_store = Arc::new(PageStore::create(&path).unwrap());
            let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
            let s = RcShard::create(page_store, page_cache).unwrap();
            meta_page_id = s.meta_page_id();
            s.stage(10, 5, 100).unwrap();
            s.stage(20, 2, 200).unwrap();
            s.flush().unwrap();
        }
        let page_store = Arc::new(PageStore::open(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let s = RcShard::open(page_store, page_cache, meta_page_id).unwrap();
        assert_eq!(s.get(10).unwrap(), 5);
        assert_eq!(s.get(20).unwrap(), 2);
        assert_eq!(s.get_entry(10).unwrap().birth_lsn, 100);
    }

    #[test]
    fn many_ops_one_shard_correctness() {
        // Stress: random PBAs across multiple pages, random +/-,
        // verified against a Reference HashMap at the end.
        use std::collections::HashMap;
        let (_d, s) = make_shard();

        let mut model: HashMap<Pba, i64> = HashMap::new();
        let ops: Vec<(Pba, i64, Lsn)> = (1u64..1000)
            .map(|i| {
                let pba = (i * 7) % (super::super::ENTRIES_PER_PAGE as u64 * 5);
                let delta = if i % 3 == 0 { -1i64 } else { 1i64 };
                (pba, delta, i)
            })
            .collect();

        for &(pba, delta, lsn) in &ops {
            let model_prev = *model.get(&pba).unwrap_or(&0);
            let model_new = model_prev + delta;
            if model_new < 0 {
                // skip ops that would underflow our model (and the shard)
                continue;
            }
            let (_prev, new) = s.stage(pba, delta, lsn).unwrap();
            assert_eq!(new, model_new as u32);
            model.insert(pba, model_new);
        }

        s.flush().unwrap();

        for (&pba, &expected) in &model {
            assert_eq!(s.get(pba).unwrap(), expected as u32, "pba {pba}");
        }
    }
}
