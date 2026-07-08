//! `DedupIndex`: four-tier dedup_index facade.
//!
//! Composes [L0 sketch](super::FpSketch), [L1 hot
//! cache](super::L1HotCache), and the [L3 cuckoo table](super::CuckooHash)
//! into a single `(hash → value)` store with the same shape as the
//! legacy `ShardedLsm` it replaces.
//!
//! # Read flow
//!
//! ```text
//! lookup(hash):
//!   fp = fp_of(hash)
//!   if !L0.contains(fp) → return None              // 99% all-miss path
//!   match L1.lookup(fp, hash):
//!     Hit(value) → return Some(value)
//!     Miss → consult L3
//!   match L3.get(hash):
//!     Some(value) → L1.put(fp, hash, value); return Some(value)
//!     None → return None
//! ```
//!
//! # Write flow
//!
//! ```text
//! put(hash, value):
//!   L3.put(hash, value, lsn)?
//!   L0.insert(fp(hash))
//!   L1.put(fp(hash), hash, value)
//!
//! delete(hash):
//!   L3.delete(hash, lsn)?
//!   L0.remove(fp(hash))
//!   L1.evict(fp(hash))
//! ```
//!
//! # Open
//!
//! `open()` walks L3 once with [`CuckooHash::iter`] to repopulate the
//! L0 sketch so `lookup` can short-circuit the all-miss path
//! immediately. L1 starts empty and warms up under traffic.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::cache::PageCache;
use crate::dedup_types::{DedupValue, Hash8};
use crate::error::Result;
use crate::metrics::DedupPutStageTimings;
use crate::page_store::PageStore;
use crate::types::{Lsn, PageId};

use super::cuckoo::{CuckooHash, CuckooPutEntry, ENTRIES_PER_BUCKET};
use super::fp_of;
use super::l1_cache::{L1HotCache, LookupResult};
use super::sketch::FpSketch;
use super::staging::{DedupStaging, StagedLookup, StagedMutation};
use crate::refcount::overlay::{DrainerHandle, DrainerState};

/// L0 capacity to use given the on-disk cuckoo bucket count. Mirrors
/// L3 max capacity (`bucket_count × ENTRIES_PER_BUCKET`) so the filter
/// can hold every fingerprint L3 can store without saturating.
fn l0_capacity_for(cuckoo_bucket_count: u64) -> usize {
    (cuckoo_bucket_count as usize).saturating_mul(ENTRIES_PER_BUCKET)
}

pub struct DedupIndex {
    sketch: FpSketch,
    l1: L1HotCache,
    cuckoo: CuckooHash,
    /// Per-shard pending cuckoo mutations awaiting the background
    /// drainer. Only populated when `drainer_enabled` (otherwise
    /// `stage_*` write the cuckoo eagerly and this stays empty).
    staging: DedupStaging,
    /// When true, `stage_put`/`stage_delete` defer the blocking cuckoo
    /// write into `staging` (drained off the commit critical path) and
    /// reads consult `staging` first. When false, `stage_*` are verbatim
    /// eager `put`/`delete` and reads skip the staging layer entirely —
    /// byte-identical to the pre-drainer behaviour.
    drainer_enabled: bool,
    /// Per-shard background drainer handles (one per staging shard),
    /// populated by [`attach_drainers`] after replay and joined by
    /// [`detach_drainers`] on `Db::drop`. Empty when the drainer is
    /// disabled. Each worker thread holds an `Arc<DedupIndex>`; the
    /// detach-before-drop discipline (mirroring the refcount drainer)
    /// breaks that cycle.
    drainers: Mutex<Vec<DrainerHandle>>,
}

impl DedupIndex {
    /// Build a fresh dedup index. `bucket_count` sizes the on-disk
    /// cuckoo table; pick `entries_target / (4 × load_factor_target)`
    /// where `load_factor_target` is typically 0.85. `l1_capacity`
    /// is the maximum number of `(fp → hash, value)` entries kept in
    /// the in-memory L1 LRU.
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        bucket_count: u64,
        l1_capacity: usize,
        seed1: u64,
        seed2: u64,
        staging_shards: usize,
        drainer_enabled: bool,
    ) -> Result<Self> {
        let cuckoo = CuckooHash::create(page_store, page_cache, bucket_count, seed1, seed2)?;
        let l0_capacity = l0_capacity_for(cuckoo.bucket_count());
        Ok(Self {
            sketch: FpSketch::with_capacity(l0_capacity),
            l1: L1HotCache::new(l1_capacity),
            cuckoo,
            staging: DedupStaging::new(staging_shards),
            drainer_enabled,
            drainers: Mutex::new(Vec::new()),
        })
    }

    /// Reopen at `meta_page_id` (recorded in the manifest). Walks L3
    /// once to repopulate L0; L1 starts empty.
    pub fn open(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        meta_page_id: PageId,
        l1_capacity: usize,
        staging_shards: usize,
        drainer_enabled: bool,
    ) -> Result<Self> {
        let cuckoo = CuckooHash::open(page_store, page_cache, meta_page_id)?;
        // Size L0 to mirror the on-disk cuckoo capacity rather than the
        // current load. The cuckoo filter cannot grow once allocated;
        // sizing it at 4 × bucket_count keeps load < 0.95 even after
        // L3 fills up, avoiding the saturation fallback.
        let sketch = FpSketch::with_capacity(l0_capacity_for(cuckoo.bucket_count()));
        let l1 = L1HotCache::new(l1_capacity);
        let me = Self {
            sketch,
            l1,
            cuckoo,
            staging: DedupStaging::new(staging_shards),
            drainer_enabled,
            drainers: Mutex::new(Vec::new()),
        };
        me.rebuild_l0_from_l3()?;
        Ok(me)
    }

    fn rebuild_l0_from_l3(&self) -> Result<()> {
        self.cuckoo.for_each(|hash, _value| {
            self.sketch.insert(fp_of(&hash));
            Ok(())
        })
    }

    pub fn meta_page_id(&self) -> PageId {
        self.cuckoo.meta_page_id()
    }

    pub fn bucket_count(&self) -> u64 {
        self.cuckoo.bucket_count()
    }

    /// Single-key lookup. Walks L0 → L1 → L3. Promotes L3 hits into
    /// L1 so the next lookup short-circuits to memory.
    pub fn get(&self, hash: &Hash8) -> Result<Option<DedupValue>> {
        let fp = fp_of(hash);
        if !self.sketch.contains(fp) {
            return Ok(None);
        }
        // Staging shadows the cuckoo for not-yet-drained mutations. It is
        // checked AFTER the L0 short-circuit (so the all-miss fast path
        // is untouched) and BEFORE L1/cuckoo. `stage_put` inserts the fp
        // into L0, so any staged Put passes the L0 gate above and is
        // observed here. Skipped entirely when the drainer is disabled
        // (staging is always empty then) → byte-identical fast path.
        if self.drainer_enabled {
            match self.staging.lookup(hash) {
                StagedLookup::Present(value) => return Ok(Some(value)),
                StagedLookup::Tombstone => return Ok(None),
                StagedLookup::Absent => {}
            }
        }
        if let LookupResult::Hit(value) = self.l1.lookup(fp, hash) {
            return Ok(Some(value));
        }
        match self.cuckoo.get(hash)? {
            Some(value) => {
                self.l1.put(fp, *hash, value);
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Batched lookup. Output order matches input order. Holds the L0
    /// read lock once across every fingerprint check, then the L1 mutex
    /// once across the surviving candidates, before falling through to
    /// L3 only for the residual misses. Most workloads see 90 %+ of
    /// hashes short-circuit in the L0 sketch, so this collapses N ×
    /// (L0 + L1) lock pairs to two.
    pub fn multi_get(&self, hashes: &[Hash8]) -> Result<Vec<Option<DedupValue>>> {
        if hashes.is_empty() {
            return Ok(Vec::new());
        }
        let fps: Vec<u32> = hashes.iter().map(fp_of).collect();
        let in_l0 = self.sketch.contains_batch(&fps);

        let mut out: Vec<Option<DedupValue>> = vec![None; hashes.len()];

        // Collect indices that survived L0 — these are the only ones
        // worth touching staging / L1 / L3 for. When the drainer is on,
        // a staged Put/Delete shadows the cuckoo: resolve it here and
        // drop the index from the L1/cuckoo residual. Survivors that are
        // `Absent` in staging fall through unchanged.
        let mut l1_pairs: Vec<(u32, Hash8)> = Vec::new();
        let mut l1_indices: Vec<usize> = Vec::new();
        for (i, &alive) in in_l0.iter().enumerate() {
            if !alive {
                continue;
            }
            if self.drainer_enabled {
                match self.staging.lookup(&hashes[i]) {
                    StagedLookup::Present(value) => {
                        out[i] = Some(value);
                        continue;
                    }
                    StagedLookup::Tombstone => {
                        out[i] = None;
                        continue;
                    }
                    StagedLookup::Absent => {}
                }
            }
            l1_pairs.push((fps[i], hashes[i]));
            l1_indices.push(i);
        }
        let l1_results = self.l1.lookup_batch(&l1_pairs);

        for ((idx, l1_result), pair) in l1_indices.iter().zip(l1_results).zip(l1_pairs.iter()) {
            match l1_result {
                LookupResult::Hit(value) => out[*idx] = Some(value),
                LookupResult::Miss => match self.cuckoo.get(&pair.1)? {
                    Some(value) => {
                        self.l1.put(pair.0, pair.1, value);
                        out[*idx] = Some(value);
                    }
                    None => out[*idx] = None,
                },
            }
        }
        Ok(out)
    }

    pub fn put(&self, hash: Hash8, value: DedupValue, lsn: Lsn) -> Result<()> {
        self.cuckoo.put(hash, value, lsn)?;
        let fp = fp_of(&hash);
        self.sketch.insert(fp);
        self.l1.put(fp, hash, value);
        Ok(())
    }

    pub(crate) fn put_with_metrics(
        &self,
        hash: Hash8,
        value: DedupValue,
        lsn: Lsn,
        timings: &mut DedupPutStageTimings,
    ) -> Result<()> {
        self.cuckoo.put_with_metrics(hash, value, lsn, timings)?;
        let fp = fp_of(&hash);
        let started = std::time::Instant::now();
        self.sketch.insert(fp);
        timings.l0_insert += started.elapsed();
        let started = std::time::Instant::now();
        self.l1.put(fp, hash, value);
        timings.l1_put += started.elapsed();
        Ok(())
    }

    pub(crate) fn put_many_with_metrics(
        &self,
        entries: &[(Hash8, DedupValue)],
        lsn: Lsn,
        timings: &mut DedupPutStageTimings,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let cuckoo_entries: Vec<CuckooPutEntry> = entries
            .iter()
            .map(|(hash, value)| CuckooPutEntry {
                hash: *hash,
                value: *value,
            })
            .collect();
        self.cuckoo
            .put_many_with_metrics(&cuckoo_entries, lsn, timings)?;
        for (hash, value) in entries {
            let fp = fp_of(hash);
            let started = std::time::Instant::now();
            self.sketch.insert(fp);
            timings.l0_insert += started.elapsed();
            let started = std::time::Instant::now();
            self.l1.put(fp, *hash, *value);
            timings.l1_put += started.elapsed();
        }
        Ok(())
    }

    pub fn delete(&self, hash: &Hash8, lsn: Lsn) -> Result<()> {
        // Order matters: clear L3 first so a concurrent reader that
        // sees fp ∈ L0 falls through to L3 and gets `None`. After
        // L3 returns clear, removing fp from L0 is safe.
        //
        // Only update L0 / L1 when L3 actually had this entry. The L0
        // sketch is reference-counted by fingerprint; multiple distinct
        // hashes sharing the low 32 bits land on the same fp slot, so
        // an unconditional `sketch.remove` for an absent hash would
        // evict L0 reservations belonging to live siblings.
        let removed = self.cuckoo.delete(hash, lsn)?;
        if removed {
            let fp = fp_of(hash);
            self.sketch.remove(fp);
            self.l1.evict(fp);
        }
        Ok(())
    }

    // ---- staging layer (async dedup-index drainer) ----
    //
    // `stage_*` are the apply-path entry points. With the drainer
    // disabled they are verbatim eager `put`/`delete` (byte-identical to
    // the pre-drainer behaviour). With it enabled they merge into the
    // in-RAM `staging` map and warm L0 (so `get`'s L0 short-circuit does
    // not hide the staged entry); the blocking cuckoo write is deferred
    // to `drain_shard_once`, run by the background drainer / checkpoint
    // barrier ().

    /// Stage a `(hash → value)` put.
    pub fn stage_put(&self, hash: Hash8, value: DedupValue, lsn: Lsn) -> Result<()> {
        if !self.drainer_enabled {
            return self.put(hash, value, lsn);
        }
        self.staging.merge_put(hash, value, lsn);
        // Reflect the staged entry in L0 so reads pass the short-circuit
        // and consult staging. L1 is intentionally NOT warmed here:
        // staging is authoritative for staged hashes, and the drainer
        // warms L1 with the value it writes to the cuckoo. (Matches the
        // eager path's unconditional `sketch.insert`; the rare orphaned
        // +1 from a put-then-delete that never reaches the cuckoo is
        // bounded and self-heals on reopen via `rebuild_l0_from_l3`.)
        self.sketch.insert(fp_of(&hash));
        Ok(())
    }

    pub(crate) fn stage_put_with_metrics(
        &self,
        hash: Hash8,
        value: DedupValue,
        lsn: Lsn,
        timings: &mut DedupPutStageTimings,
    ) -> Result<()> {
        if !self.drainer_enabled {
            return self.put_with_metrics(hash, value, lsn, timings);
        }
        self.staging.merge_put(hash, value, lsn);
        let started = std::time::Instant::now();
        self.sketch.insert(fp_of(&hash));
        timings.l0_insert += started.elapsed();
        Ok(())
    }

    pub(crate) fn stage_put_many_with_metrics(
        &self,
        entries: &[(Hash8, DedupValue)],
        lsn: Lsn,
        timings: &mut DedupPutStageTimings,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        if !self.drainer_enabled {
            return self.put_many_with_metrics(entries, lsn, timings);
        }
        for (hash, value) in entries {
            self.staging.merge_put(*hash, *value, lsn);
            let started = std::time::Instant::now();
            self.sketch.insert(fp_of(hash));
            timings.l0_insert += started.elapsed();
        }
        Ok(())
    }

    /// Stage a delete (tombstone).
    pub fn stage_delete(&self, hash: &Hash8, lsn: Lsn) -> Result<()> {
        if !self.drainer_enabled {
            return self.delete(hash, lsn);
        }
        // Do not touch L0/L1: the staging tombstone shadows reads (a hash
        // that passes the L0 gate consults staging first → `Tombstone` →
        // None). The drainer fixes up L0/L1 when it applies the delete to
        // the cuckoo (`self.delete` below).
        self.staging.merge_delete(*hash, lsn);
        Ok(())
    }

    /// Drain one shard's staged mutations into the on-disk cuckoo: swap
    /// `active`→`draining`, apply puts (page-grouped batch) + deletes,
    /// then clear `draining`. Draining entries stay visible to readers
    /// until the cuckoo write completes. Returns the entry count drained.
    /// Caller (background drainer / checkpoint barrier) serialises drains
    /// of a given shard.
    pub(crate) fn drain_shard_once(&self, shard: usize) -> Result<usize> {
        let snapshot = self.staging.swap_active_to_draining(shard);
        if snapshot.is_empty() {
            return Ok(0);
        }
        let n = snapshot.len();
        // Each hash appears exactly once (last-LSN-wins map) so puts and
        // deletes never collide and cross-hash order is irrelevant. The
        // cuckoo page `generation` is a monotonic high-water stamp (never
        // a skip gate), so batching puts at the max lsn is correct.
        let apply = || -> Result<()> {
            let mut cuckoo_entries: Vec<CuckooPutEntry> = Vec::new();
            let mut max_put_lsn: Lsn = 0;
            let mut deletes: Vec<(Hash8, Lsn)> = Vec::new();
            for (hash, m) in &snapshot {
                match m {
                    StagedMutation::Put { value, lsn } => {
                        cuckoo_entries.push(CuckooPutEntry {
                            hash: *hash,
                            value: *value,
                        });
                        max_put_lsn = max_put_lsn.max(*lsn);
                    }
                    StagedMutation::Delete { lsn } => deletes.push((*hash, *lsn)),
                }
            }
            if !cuckoo_entries.is_empty() {
                let mut timings = DedupPutStageTimings::default();
                // Cuckoo only — L0 already carries each fp from
                // `stage_put`; warm L1 with the drained value separately
                // so post-drain reads hit memory.
                self.cuckoo
                    .put_many_with_metrics(&cuckoo_entries, max_put_lsn, &mut timings)?;
                for entry in &cuckoo_entries {
                    self.l1.put(fp_of(&entry.hash), entry.hash, entry.value);
                }
            }
            for (hash, lsn) in &deletes {
                // `self.delete` clears the cuckoo and, only if it actually
                // removed an entry, decrements L0 + evicts L1 — matching
                // the `+1` a prior drained `stage_put` added to L0.
                self.delete(hash, *lsn)?;
            }
            Ok(())
        };
        match apply() {
            Ok(()) => {
                self.staging.clear_draining(shard);
                Ok(n)
            }
            Err(err) => {
                // Cuckoo put/delete are idempotent, so a partial apply +
                // full re-stage + retry is safe. Restore the snapshot
                // into `active` (last-LSN-wins preserves any newer
                // post-swap mutations) and clear `draining` so the next
                // swap's "draining is empty" invariant holds.
                for (hash, m) in &snapshot {
                    self.staging.merge_mutation(*hash, *m);
                }
                self.staging.clear_draining(shard);
                Err(err)
            }
        }
    }

    /// Synchronously drain every shard's staging into the cuckoo. Used by
    /// the checkpoint barrier before sampling `checkpoint_lsn`, and by
    /// tests. Caller must ensure no background drainer is concurrently
    /// draining the same shards (the barrier preempts them first).
    pub(crate) fn drain_all(&self) -> Result<usize> {
        let mut total = 0;
        for shard in 0..self.staging.shard_count() {
            total += self.drain_shard_once(shard)?;
        }
        Ok(total)
    }

    /// Spawn one background drainer thread per staging shard. Called by
    /// `Db::open`/`create` AFTER replay (so the drainer never races
    /// mid-replay state), mirroring `RcShard::attach_drainer`. No-op when
    /// the drainer is disabled or already attached.
    pub(crate) fn attach_drainers(
        self: &Arc<Self>,
        cfg: &crate::config::Config,
        metrics: Arc<crate::metrics::MetaMetrics>,
    ) {
        if !self.drainer_enabled {
            return;
        }
        let mut slot = self.drainers.lock();
        if !slot.is_empty() {
            return;
        }
        let shard_count = self.staging.shard_count();
        tracing::info!(
            shards = shard_count,
            interval_ms = cfg.dedup_drainer_interval_ms,
            threshold = cfg.dedup_drainer_threshold_entries,
            "dedup-drainer: attaching drainer threads"
        );
        for shard_idx in 0..shard_count {
            let state = Arc::new(DrainerState::new());
            let worker = super::drainer::DedupDrainerWorker {
                shard_idx,
                dedup_index: self.clone(),
                interval_ms: cfg.dedup_drainer_interval_ms,
                threshold_entries: cfg.dedup_drainer_threshold_entries,
                state: state.clone(),
                metrics: metrics.clone(),
            };
            let join = std::thread::Builder::new()
                .name(format!("dedup-drainer-{shard_idx}"))
                .spawn(move || {
                    crate::affinity::bind_current(
                        crate::affinity::ThreadRole::DedupDrainer,
                        shard_idx,
                    );
                    worker.run();
                })
                .expect("failed to spawn dedup drainer thread");
            slot.push(DrainerHandle::new(state, join));
        }
    }

    /// Signal shutdown + join every drainer thread. Idempotent. Called
    /// by `Db::drop` BEFORE the `Arc<DedupIndex>` last-drop so the
    /// worker→index Arc cycle is broken.
    pub(crate) fn detach_drainers(&self) {
        let mut slot = self.drainers.lock();
        for handle in slot.iter_mut() {
            handle.shutdown();
        }
        slot.clear();
    }

    /// Checkpoint barrier: preempt every drainer (wait for any in-flight
    /// cycle to wind down) then synchronously final-drain all staging
    /// into the cuckoo. Must run before the dedup manifest/`flush_meta`
    /// update and the buffer-seq sample so `checkpoint_lsn` never
    /// advances past an undrained staged entry. Caller re-arms the
    /// drainers via [`resume_drainers`] (RAII guard in flush). No-op when
    /// the drainer is disabled (staging is empty then).
    pub(crate) fn preempt_and_drain_for_checkpoint(
        &self,
        metrics: &crate::metrics::MetaMetrics,
    ) -> Result<()> {
        if !self.drainer_enabled {
            return Ok(());
        }
        let started = std::time::Instant::now();
        {
            let slot = self.drainers.lock();
            for handle in slot.iter() {
                handle.preempt_and_wait();
            }
        }
        metrics.record_dedup_drainer_checkpoint_wait(started.elapsed());
        // Drainers are paused; drain everything into the cuckoo.
        self.drain_all()?;
        Ok(())
    }

    /// Re-arm the drainers after the checkpoint barrier. Idempotent.
    pub(crate) fn resume_drainers(&self) {
        let slot = self.drainers.lock();
        for handle in slot.iter() {
            handle.resume();
        }
    }

    pub(crate) fn staging_shard_count(&self) -> usize {
        self.staging.shard_count()
    }

    pub(crate) fn staging_active_len(&self, shard: usize) -> usize {
        self.staging.active_len(shard)
    }

    pub(crate) fn staging_total_active_len(&self) -> usize {
        self.staging.total_active_len()
    }

    pub(crate) fn drainer_enabled(&self) -> bool {
        self.drainer_enabled
    }

    pub fn flush_meta(&self) -> Result<bool> {
        self.cuckoo.flush_meta()
    }

    pub fn iter(&self) -> Result<Vec<(Hash8, DedupValue)>> {
        let base = self.cuckoo.iter()?;
        if !self.drainer_enabled {
            return Ok(base);
        }
        // Overlay not-yet-drained staged mutations so callers (the soak
        // reference model, `iter_dedup`) see a complete logical snapshot.
        let staged = self.staging.snapshot_all();
        if staged.is_empty() {
            return Ok(base);
        }
        let mut map: HashMap<Hash8, DedupValue> = base.into_iter().collect();
        for (hash, m) in staged {
            match m {
                StagedMutation::Put { value, .. } => {
                    map.insert(hash, value);
                }
                StagedMutation::Delete { .. } => {
                    map.remove(&hash);
                }
            }
        }
        Ok(map.into_iter().collect())
    }

    /// Resumable, bounded scan over the on-disk cuckoo forward index — see
    /// [`crate::dedup::cuckoo::Cuckoo::scan_from`]. Returns up to `limit` live
    /// `(hash, value)` pairs from cursor `(page_idx, slot)`, the resume cursor,
    /// and a `wrapped` flag (full pass completed).
    ///
    /// Unlike [`Self::iter`] this does NOT overlay not-yet-drained staged
    /// mutations (a page-range scan cannot locate them): callers see the
    /// committed on-disk state, and a staged put becomes visible once drained.
    /// That is sufficient for the background orphan-reclaim sweep, whose
    /// correctness rests on the onyx Gate-2 confirm scan + guarded delete, not
    /// on scan completeness.
    pub fn scan_from(
        &self,
        page_idx: usize,
        slot: usize,
        limit: usize,
    ) -> Result<(Vec<(Hash8, DedupValue)>, usize, usize, bool)> {
        self.cuckoo.scan_from(page_idx, slot, limit)
    }

    /// Walk every allocated data page id (used by verifier).
    pub fn data_page_ids(&self) -> Vec<PageId> {
        self.cuckoo.data_page_ids()
    }

    /// Highest page id the dedup index physically references (meta chain + data
    /// pages). The device-path open MUST fold this into the bounded-scan
    /// ceiling — the cuckoo meta chain is generation-stable + in-place, so it
    /// can recover ahead of an older manifest's `page_high_water`. See
    /// [`crate::dedup::cuckoo::CuckooHash::max_referenced_page_id`].
    pub fn max_referenced_page_id(&self) -> PageId {
        self.cuckoo.max_referenced_page_id()
    }

    /// Every page id the dedup index physically references (meta head + meta
    /// chain + data pages). The device-path open removes these from the
    /// persisted free-list bitmap so `allocate()` never re-hands a live dedup
    /// page — see [`crate::dedup::cuckoo::CuckooHash::referenced_page_ids`].
    pub fn referenced_page_ids(&self) -> Vec<PageId> {
        self.cuckoo.referenced_page_ids()
    }

    /// Approximate live entry count. Tracks the cuckoo's running
    /// counter; for an exact figure call [`recount`].
    pub fn approx_len(&self) -> u64 {
        self.cuckoo.approx_len()
    }

    pub fn recount(&self) -> Result<u64> {
        self.cuckoo.recount()
    }

    /// In-memory tier sizes for status / soak metrics.
    pub fn tier_sizes(&self) -> TierSizes {
        TierSizes {
            l0_distinct_fps: self.sketch.len(),
            l0_approx_bytes: self.sketch.approx_bytes(),
            l1_entries: self.l1.len(),
        }
    }

    pub fn tier_sizes_best_effort(&self) -> TierSizes {
        TierSizes {
            l0_distinct_fps: self.sketch.try_len().unwrap_or(0),
            l0_approx_bytes: self.sketch.try_approx_bytes().unwrap_or(0),
            l1_entries: self.l1.try_len().unwrap_or(0),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct TierSizes {
    pub l0_distinct_fps: usize,
    pub l0_approx_bytes: usize,
    pub l1_entries: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_index() -> (TempDir, DedupIndex) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let page_store = Arc::new(PageStore::create(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let idx =
            DedupIndex::create(page_store, page_cache, 64, 16, 0xDEAD, 0xBEEF, 4, false).unwrap();
        (dir, idx)
    }

    fn make_index_staged() -> (TempDir, DedupIndex) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let page_store = Arc::new(PageStore::create(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        // drainer_enabled = true: stage_* defer into the staging layer
        // and reads consult it first.
        let idx =
            DedupIndex::create(page_store, page_cache, 64, 16, 0xDEAD, 0xBEEF, 4, true).unwrap();
        (dir, idx)
    }

    #[test]
    fn staged_put_visible_before_and_after_drain() {
        let (_d, idx) = make_index_staged();
        idx.stage_put(h(0xAA), dv(7), 100).unwrap();
        // Visible via staging before any drain.
        assert_eq!(idx.get(&h(0xAA)).unwrap(), Some(dv(7)));
        assert_eq!(idx.multi_get(&[h(0xAA)]).unwrap(), vec![Some(dv(7))]);
        assert_eq!(idx.staging_total_active_len(), 1);
        // Drain into the cuckoo: still visible, now from disk; staging empty.
        let drained = idx.drain_all().unwrap();
        assert_eq!(drained, 1);
        assert_eq!(idx.staging_total_active_len(), 0);
        assert_eq!(idx.get(&h(0xAA)).unwrap(), Some(dv(7)));
    }

    #[test]
    fn staged_delete_tombstones_a_drained_entry() {
        let (_d, idx) = make_index_staged();
        idx.stage_put(h(0xAA), dv(7), 100).unwrap();
        idx.drain_all().unwrap();
        assert_eq!(idx.get(&h(0xAA)).unwrap(), Some(dv(7)));
        // Tombstone shadows the on-disk entry immediately.
        idx.stage_delete(&h(0xAA), 101).unwrap();
        assert_eq!(idx.get(&h(0xAA)).unwrap(), None);
        assert_eq!(idx.multi_get(&[h(0xAA)]).unwrap(), vec![None]);
        // After draining the delete, the cuckoo entry is gone.
        idx.drain_all().unwrap();
        assert_eq!(idx.get(&h(0xAA)).unwrap(), None);
    }

    #[test]
    fn staged_put_overwrites_before_drain_last_lsn_wins() {
        let (_d, idx) = make_index_staged();
        idx.stage_put(h(0xAA), dv(7), 100).unwrap();
        idx.stage_put(h(0xAA), dv(9), 101).unwrap();
        assert_eq!(idx.get(&h(0xAA)).unwrap(), Some(dv(9)));
        idx.drain_all().unwrap();
        assert_eq!(idx.get(&h(0xAA)).unwrap(), Some(dv(9)));
    }

    #[test]
    fn staged_iter_includes_undrained_entries() {
        let (_d, idx) = make_index_staged();
        idx.stage_put(h(1), dv(1), 100).unwrap();
        idx.drain_all().unwrap();
        idx.stage_put(h(2), dv(2), 101).unwrap();
        idx.stage_delete(&h(1), 102).unwrap();
        // iter must reflect the drained h(2)... wait h(2) is staged, h(1) drained+tombstoned.
        let mut got = idx.iter().unwrap();
        got.sort_by_key(|(hh, _)| hh[0]);
        assert_eq!(got, vec![(h(2), dv(2))]);
    }

    fn h(byte: u8) -> Hash8 {
        let mut x = [0u8; 8];
        x.fill(byte);
        x
    }

    fn dv(byte: u8) -> DedupValue {
        let mut x = [0u8; 28];
        x[0] = byte;
        DedupValue(x)
    }

    #[test]
    fn empty_get_short_circuits_at_l0() {
        let (_d, idx) = make_index();
        assert_eq!(idx.get(&h(0xAA)).unwrap(), None);
        assert_eq!(idx.tier_sizes().l0_distinct_fps, 0);
    }

    #[test]
    fn put_then_get_round_trip() {
        let (_d, idx) = make_index();
        idx.put(h(0xAA), dv(7), 100).unwrap();
        assert_eq!(idx.get(&h(0xAA)).unwrap(), Some(dv(7)));
        // L0 + L1 populated.
        assert_eq!(idx.tier_sizes().l0_distinct_fps, 1);
        assert_eq!(idx.tier_sizes().l1_entries, 1);
    }

    #[test]
    fn put_many_round_trips_and_keeps_last_duplicate() {
        let (_d, idx) = make_index();
        let mut timings = DedupPutStageTimings::default();
        idx.put_many_with_metrics(
            &[(h(0xAA), dv(1)), (h(0xBB), dv(2)), (h(0xAA), dv(3))],
            100,
            &mut timings,
        )
        .unwrap();
        assert_eq!(idx.get(&h(0xAA)).unwrap(), Some(dv(3)));
        assert_eq!(idx.get(&h(0xBB)).unwrap(), Some(dv(2)));
    }

    #[test]
    fn delete_of_absent_hash_does_not_evict_fp_collision_sibling() {
        // Regression for db_proptest::db_vs_reference_with_reopens.
        // The L0 sketch is reference-counted by 32-bit fingerprint
        // (`fp_of(hash) = u32::from_le_bytes(hash[..4])`). Two distinct
        // hashes that share the same low 4 bytes also share an L0
        // counter slot. Deleting an *absent* hash must not decrement
        // that counter — otherwise the live sibling becomes invisible
        // because L0 short-circuits to `None` first.
        //
        // Pick `a` and `b` that differ only past byte 3 so they share
        // a fingerprint.
        let a = [0u8, 0, 0, 0, 0, 0, 0, 0];
        let b = [0u8, 0, 0, 0, 0, 0, 0, 1];
        assert_eq!(fp_of(&a), fp_of(&b), "test setup must collide");

        let (_d, idx) = make_index();
        idx.put(a, dv(0xAA), 100).unwrap();
        // `b` was never inserted; deleting it is a no-op at L3 and
        // must leave L0 / L1 entries for `a` intact.
        idx.delete(&b, 101).unwrap();
        assert_eq!(
            idx.get(&a).unwrap(),
            Some(dv(0xAA)),
            "fp-collision sibling must still be observable after no-op delete"
        );
    }

    #[test]
    fn put_then_get_all_zero_hash_round_trips() {
        // Regression: the all-zero hash (`Hash8([0; 8])`) maps to fp_of() = 0,
        // and an all-zero DedupValue is a valid payload. Both must round-trip
        // through L0 (sketch) + L1 (cache) + L3 (cuckoo) get.
        let (_d, idx) = make_index();
        let zero_hash = [0u8; 8];
        let zero_value = DedupValue([0u8; 28]);
        idx.put(zero_hash, zero_value, 100).unwrap();
        assert_eq!(
            idx.get(&zero_hash).unwrap(),
            Some(zero_value),
            "all-zero hash + all-zero value must be observable post-put"
        );
        // Also validate via multi_get (fast path used by lookup_dedup_hits).
        let multi = idx.multi_get(&[zero_hash]).unwrap();
        assert_eq!(multi, vec![Some(zero_value)]);
    }

    #[test]
    fn delete_removes_all_tiers() {
        let (_d, idx) = make_index();
        idx.put(h(0xAA), dv(7), 100).unwrap();
        idx.delete(&h(0xAA), 101).unwrap();
        assert_eq!(idx.get(&h(0xAA)).unwrap(), None);
        assert_eq!(idx.tier_sizes().l0_distinct_fps, 0);
        assert_eq!(idx.tier_sizes().l1_entries, 0);
    }

    #[test]
    fn miss_does_not_warm_l1() {
        let (_d, idx) = make_index();
        idx.put(h(0xAA), dv(7), 100).unwrap();
        // Lookup a hash that isn't there: L0 will reject (fp differs),
        // L1 stays empty for it.
        assert_eq!(idx.get(&h(0xBB)).unwrap(), None);
        assert_eq!(idx.tier_sizes().l1_entries, 1, "only the one put hit L1");
    }

    #[test]
    fn batched_multi_get_preserves_order() {
        let (_d, idx) = make_index();
        idx.put(h(1), dv(1), 100).unwrap();
        idx.put(h(2), dv(2), 101).unwrap();
        let got = idx.multi_get(&[h(2), h(99), h(1), h(2)]).unwrap();
        assert_eq!(got, vec![Some(dv(2)), None, Some(dv(1)), Some(dv(2))]);
    }

    #[test]
    fn put_overwrites_value() {
        let (_d, idx) = make_index();
        idx.put(h(0xAA), dv(7), 100).unwrap();
        idx.put(h(0xAA), dv(9), 101).unwrap();
        assert_eq!(idx.get(&h(0xAA)).unwrap(), Some(dv(9)));
        assert_eq!(idx.approx_len(), 1);
    }

    #[test]
    fn open_rebuilds_l0_but_l1_starts_cold() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let meta_page_id;
        {
            let page_store = Arc::new(PageStore::create(&path).unwrap());
            let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
            let idx = DedupIndex::create(page_store, page_cache, 64, 16, 0xDEAD, 0xBEEF, 4, false)
                .unwrap();
            meta_page_id = idx.meta_page_id();
            for i in 0..30u8 {
                idx.put(h(i), dv(i), (100 + i as u64) as Lsn).unwrap();
            }
            idx.flush_meta().unwrap();
        }
        let page_store = Arc::new(PageStore::open(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let idx = DedupIndex::open(page_store, page_cache, meta_page_id, 16, 4, false).unwrap();
        // L0 fully restored.
        assert!(idx.tier_sizes().l0_distinct_fps >= 30);
        // L1 starts empty.
        assert_eq!(idx.tier_sizes().l1_entries, 0);
        // Lookups still work; fill L1 along the way.
        for i in 0..30u8 {
            assert_eq!(idx.get(&h(i)).unwrap(), Some(dv(i)));
        }
        assert!(idx.tier_sizes().l1_entries > 0);
    }

    #[test]
    fn iter_yields_all_pairs_in_disk_order() {
        let (_d, idx) = make_index();
        for i in 0..20u8 {
            idx.put(h(i), dv(i), 100).unwrap();
        }
        let mut live = idx.iter().unwrap();
        live.sort_by_key(|(h, _)| h[0]);
        assert_eq!(live.len(), 20);
        for (i, (hash, value)) in live.iter().enumerate() {
            assert_eq!(*hash, h(i as u8));
            assert_eq!(*value, dv(i as u8));
        }
    }
}
