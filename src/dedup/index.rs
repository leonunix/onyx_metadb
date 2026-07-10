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

use parking_lot::{Mutex, RwLock};

use crate::cache::PageCache;
use crate::dedup_types::{DedupValue, Hash8};
use crate::error::{MetaDbError, Result};
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

/// The swappable core of the dedup index: the L0 fingerprint filter plus the
/// active cuckoo table (`new`) and, during an online modulus resize, the OLD
/// (frozen) table being drained into it. Guarded by a single `RwLock` so a
/// resize (`begin_grow` / `finish_swap`, under the checkpoint barrier) can
/// re-point all three atomically while readers see a consistent triple. Reads
/// clone the `Arc`s under a brief read guard and then operate lock-free on the
/// cuckoo's own internal per-page shard locks — so this lock is uncontended on
/// the hot path (writes to it happen only at grow/swap, which are rare).
struct DedupState {
    /// L0 fingerprint filter, sized for `new`'s bucket count. Replaced (seeded
    /// from OLD) when the modulus grows so the filter tracks the larger table.
    sketch: Arc<FpSketch>,
    /// The active (current / largest) cuckoo table. Every live write targets it.
    new: Arc<CuckooHash>,
    /// The OLD (smaller, frozen) table during a resize; `None` in steady state.
    /// Reads fall through to it after `new` misses; the migration walker drains
    /// it into `new`; dropped at the swap barrier once fully migrated.
    old: Option<Arc<CuckooHash>>,
}

pub struct DedupIndex {
    state: RwLock<DedupState>,
    l1: L1HotCache,
    /// Page store / cache, retained so [`DedupIndex::begin_grow`] can allocate
    /// the larger cuckoo table on demand.
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
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
        let cuckoo = CuckooHash::create(
            page_store.clone(),
            page_cache.clone(),
            bucket_count,
            seed1,
            seed2,
        )?;
        let l0_capacity = l0_capacity_for(cuckoo.bucket_count());
        Ok(Self {
            state: RwLock::new(DedupState {
                sketch: Arc::new(FpSketch::with_capacity(l0_capacity)),
                new: Arc::new(cuckoo),
                old: None,
            }),
            l1: L1HotCache::new(l1_capacity),
            page_store,
            page_cache,
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
        let cuckoo = CuckooHash::open(page_store.clone(), page_cache.clone(), meta_page_id)?;
        // Size L0 to mirror the on-disk cuckoo capacity rather than the
        // current load. The cuckoo filter cannot grow once allocated;
        // sizing it at 4 × bucket_count keeps load < 0.95 even after
        // L3 fills up, avoiding the saturation fallback.
        let sketch = Arc::new(FpSketch::with_capacity(l0_capacity_for(
            cuckoo.bucket_count(),
        )));
        let l1 = L1HotCache::new(l1_capacity);
        let me = Self {
            state: RwLock::new(DedupState {
                sketch,
                new: Arc::new(cuckoo),
                old: None,
            }),
            l1,
            page_store,
            page_cache,
            staging: DedupStaging::new(staging_shards),
            drainer_enabled,
            drainers: Mutex::new(Vec::new()),
        };
        me.rebuild_l0_from_l3()?;
        Ok(me)
    }

    /// Reopen mid-resize (crash recovery): open BOTH the NEW (active) table at
    /// `new_meta_pid` and the OLD (frozen) table at `old_meta_pid`, size L0 for
    /// the NEW capacity + reseed from the union, and resume in the Growing
    /// phase. The migration walker re-walks OLD from page 0 (idempotent
    /// `put_if_absent`), so a partially-migrated NEW is finished without loss.
    /// Called by `Db::open` when `manifest.dedup_migration_old_head` is set.
    #[allow(clippy::too_many_arguments)]
    pub fn open_growing(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        new_meta_pid: PageId,
        old_meta_pid: PageId,
        l1_capacity: usize,
        staging_shards: usize,
        drainer_enabled: bool,
    ) -> Result<Self> {
        let new_table = CuckooHash::open(page_store.clone(), page_cache.clone(), new_meta_pid)?;
        let old_table = CuckooHash::open(page_store.clone(), page_cache.clone(), old_meta_pid)?;
        let sketch = Arc::new(FpSketch::with_capacity(l0_capacity_for(
            new_table.bucket_count(),
        )));
        let me = Self {
            state: RwLock::new(DedupState {
                sketch,
                new: Arc::new(new_table),
                old: Some(Arc::new(old_table)),
            }),
            l1: L1HotCache::new(l1_capacity),
            page_store,
            page_cache,
            staging: DedupStaging::new(staging_shards),
            drainer_enabled,
            drainers: Mutex::new(Vec::new()),
        };
        me.rebuild_l0_from_l3()?; // unions NEW + OLD fingerprints
        Ok(me)
    }

    /// Snapshot the current `(sketch, new, old)` triple under a brief read
    /// guard, cloning the `Arc`s so the caller operates without holding the
    /// state lock (the cuckoo tables carry their own internal locks). This is
    /// the single choke point every routed read/write goes through, so a
    /// concurrent resize either happens fully before or fully after any given
    /// operation's snapshot.
    fn snapshot(&self) -> (Arc<FpSketch>, Arc<CuckooHash>, Option<Arc<CuckooHash>>) {
        let st = self.state.read();
        (st.sketch.clone(), st.new.clone(), st.old.clone())
    }

    fn rebuild_l0_from_l3(&self) -> Result<()> {
        let (sketch, new, old) = self.snapshot();
        new.for_each(|hash, _value| {
            sketch.insert(fp_of(&hash));
            Ok(())
        })?;
        if let Some(old) = &old {
            old.for_each(|hash, _value| {
                sketch.insert(fp_of(&hash));
                Ok(())
            })?;
        }
        Ok(())
    }

    /// Meta-page id of the ACTIVE (NEW) cuckoo table — recorded in the manifest
    /// as `dedup_index_shard_heads[0][0]`.
    pub fn meta_page_id(&self) -> PageId {
        self.state.read().new.meta_page_id()
    }

    /// Meta-page id of the OLD (frozen) cuckoo table during a resize, or `None`
    /// in steady state — recorded in the manifest as `dedup_migration_old_head`.
    pub fn old_meta_page_id(&self) -> Option<PageId> {
        self.state.read().old.as_ref().map(|o| o.meta_page_id())
    }

    /// Whether an online modulus resize is in progress (phase = Growing).
    pub fn is_growing(&self) -> bool {
        self.state.read().old.is_some()
    }

    /// Bucket count (modulus) of the ACTIVE (NEW) table — the current/target
    /// modulus.
    pub fn bucket_count(&self) -> u64 {
        self.state.read().new.bucket_count()
    }

    /// Hash seeds of the ACTIVE table, so a resize can build the larger table
    /// with the same seeds (the modulus differs, so the bucket mapping still
    /// changes — reusing seeds is standard and avoids threading them through
    /// the onyx trigger path).
    pub fn current_seeds(&self) -> (u64, u64) {
        self.state.read().new.seeds()
    }

    /// Single-key lookup. Walks L0 → L1 → L3. Promotes L3 hits into
    /// L1 so the next lookup short-circuits to memory.
    pub fn get(&self, hash: &Hash8) -> Result<Option<DedupValue>> {
        let fp = fp_of(hash);
        let (sketch, new, old) = self.snapshot();
        if !sketch.contains(fp) {
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
        // NEW then OLD: NEW is always ≥ as fresh (all live writes target it;
        // OLD is frozen during a resize), so the first hit wins.
        if let Some(value) = new.get(hash)? {
            self.l1.put(fp, *hash, value);
            return Ok(Some(value));
        }
        if let Some(old) = &old
            && let Some(value) = old.get(hash)?
        {
            self.l1.put(fp, *hash, value);
            return Ok(Some(value));
        }
        Ok(None)
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
        let (sketch, new, old) = self.snapshot();
        let in_l0 = sketch.contains_batch(&fps);

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
                // NEW then OLD for the residual L1 misses.
                LookupResult::Miss => {
                    let mut found = new.get(&pair.1)?;
                    if found.is_none()
                        && let Some(old) = &old
                    {
                        found = old.get(&pair.1)?;
                    }
                    match found {
                        Some(value) => {
                            self.l1.put(pair.0, pair.1, value);
                            out[*idx] = Some(value);
                        }
                        None => out[*idx] = None,
                    }
                }
            }
        }
        Ok(out)
    }

    pub fn put(&self, hash: Hash8, value: DedupValue, lsn: Lsn) -> Result<()> {
        let (sketch, new, old_present) = {
            let st = self.state.read();
            (st.sketch.clone(), st.new.clone(), st.old.is_some())
        };
        // Steady state keeps the evicting hard-error `put`; during a resize the
        // insert must be atomic against the migration walker (same ≤2-page
        // locks), so route through `put_overwrite_atomic` (soft-drop instead of
        // eviction — unreachable at NEW's provisioned load).
        let placed = if old_present {
            new.put_overwrite_atomic(hash, value, lsn)?
        } else {
            new.put(hash, value, lsn)?;
            true
        };
        if placed {
            let fp = fp_of(&hash);
            sketch.insert(fp);
            self.l1.put(fp, hash, value);
        }
        Ok(())
    }

    /// Returns `Ok(true)` if placed, `Ok(false)` if the cuckoo was
    /// saturated and the entry was dropped (see `CuckooHash::put_with_metrics`).
    /// L0/L1 are warmed only on a real placement — an L0 fp reservation
    /// with no backing L3 entry would shadow fingerprint siblings.
    pub(crate) fn put_with_metrics(
        &self,
        hash: Hash8,
        value: DedupValue,
        lsn: Lsn,
        timings: &mut DedupPutStageTimings,
    ) -> Result<bool> {
        let (sketch, new, old_present) = {
            let st = self.state.read();
            (st.sketch.clone(), st.new.clone(), st.old.is_some())
        };
        let placed = if old_present {
            new.put_overwrite_atomic(hash, value, lsn)?
        } else {
            new.put_with_metrics(hash, value, lsn, timings)?
        };
        if !placed {
            return Ok(false);
        }
        let fp = fp_of(&hash);
        let started = std::time::Instant::now();
        sketch.insert(fp);
        timings.l0_insert += started.elapsed();
        let started = std::time::Instant::now();
        self.l1.put(fp, hash, value);
        timings.l1_put += started.elapsed();
        Ok(true)
    }

    /// Batch put. Returns the set of hashes DROPPED because the cuckoo
    /// was saturated (empty = all placed). L0/L1 are warmed only for
    /// placed entries.
    pub(crate) fn put_many_with_metrics(
        &self,
        entries: &[(Hash8, DedupValue)],
        lsn: Lsn,
        timings: &mut DedupPutStageTimings,
    ) -> Result<Vec<Hash8>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let (sketch, new, old_present) = {
            let st = self.state.read();
            (st.sketch.clone(), st.new.clone(), st.old.is_some())
        };
        let dropped = if old_present {
            // Growing: per-entry atomic overwrite-or-insert into NEW so each
            // serialises with the migration walker (no page-batch fast path
            // during the transient resize window).
            let mut d = Vec::new();
            for (hash, value) in entries {
                if !new.put_overwrite_atomic(*hash, *value, lsn)? {
                    d.push(*hash);
                }
            }
            d
        } else {
            let cuckoo_entries: Vec<CuckooPutEntry> = entries
                .iter()
                .map(|(hash, value)| CuckooPutEntry {
                    hash: *hash,
                    value: *value,
                })
                .collect();
            new.put_many_with_metrics(&cuckoo_entries, lsn, timings)?
        };
        for (hash, value) in entries {
            if !dropped.is_empty() && dropped.contains(hash) {
                continue;
            }
            let fp = fp_of(hash);
            let started = std::time::Instant::now();
            sketch.insert(fp);
            timings.l0_insert += started.elapsed();
            let started = std::time::Instant::now();
            self.l1.put(fp, *hash, *value);
            timings.l1_put += started.elapsed();
        }
        Ok(dropped)
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
        let (sketch, new, old) = self.snapshot();
        // OLD-before-NEW lock ordering (matches `migrate_page_into`): a
        // concurrent copy of this hash takes the OLD page-shard lock before the
        // NEW one, so delete and copy serialise on OLD and the entry can't be
        // resurrected. Clearing OLD first also means a copy that lands AFTER our
        // OLD clear re-reads the now-absent OLD entry and skips.
        let removed_old = if let Some(old) = &old {
            old.delete(hash, lsn)?
        } else {
            false
        };
        let removed_new = new.delete(hash, lsn)?;
        if removed_old || removed_new {
            let fp = fp_of(hash);
            sketch.remove(fp);
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
    /// Eager (bare / replay) put. Returns `Ok(true)` if placed, `Ok(false)` if
    /// the cuckoo was saturated and the entry was dropped.
    ///
    /// Drainer-off it routes through `put_with_metrics` (the **drop-on-
    /// saturation** path), NOT the hard-erroring `put`: this is the apply path
    /// used by staged commits AND WAL replay, so a saturated cuckoo must degrade
    /// to a skipped promote (a future dedup miss) instead of failing the commit
    /// — and, critically, instead of re-erroring on every replay and wedging
    /// recovery. This mirrors the lane path (`put_many_with_metrics` in
    /// `apply_dedup_indices_to`); before this the bare path called `put`, which
    /// left saturation as a hard `Corruption` (the exact P0 the drop backstop
    /// was meant to close, on a path it had missed).
    pub fn stage_put(&self, hash: Hash8, value: DedupValue, lsn: Lsn) -> Result<bool> {
        if !self.drainer_enabled {
            let mut timings = DedupPutStageTimings::default();
            return self.put_with_metrics(hash, value, lsn, &mut timings);
        }
        self.staging.merge_put(hash, value, lsn);
        // Reflect the staged entry in L0 so reads pass the short-circuit
        // and consult staging. L1 is intentionally NOT warmed here:
        // staging is authoritative for staged hashes, and the drainer
        // warms L1 with the value it writes to the cuckoo. (Matches the
        // eager path's unconditional `sketch.insert`; the rare orphaned
        // +1 from a put-then-delete that never reaches the cuckoo is
        // bounded and self-heals on reopen via `rebuild_l0_from_l3`.)
        self.state.read().sketch.insert(fp_of(&hash));
        Ok(true)
    }

    /// Returns `Ok(true)` if placed/accepted, `Ok(false)` if dropped on
    /// cuckoo saturation. Drainer-off: mirrors `put_with_metrics`.
    /// Drainer-on: staging always accepts (`Ok(true)`); any saturation
    /// drop happens later in `drain_shard_once`.
    pub(crate) fn stage_put_with_metrics(
        &self,
        hash: Hash8,
        value: DedupValue,
        lsn: Lsn,
        timings: &mut DedupPutStageTimings,
    ) -> Result<bool> {
        if !self.drainer_enabled {
            return self.put_with_metrics(hash, value, lsn, timings);
        }
        self.staging.merge_put(hash, value, lsn);
        let started = std::time::Instant::now();
        self.state.read().sketch.insert(fp_of(&hash));
        timings.l0_insert += started.elapsed();
        Ok(true)
    }

    /// Batch stage. Returns hashes DROPPED on cuckoo saturation (empty =
    /// none). Drainer-off: mirrors `put_many_with_metrics`. Drainer-on:
    /// staging accepts everything (returns empty); saturation drops
    /// happen at drain time.
    pub(crate) fn stage_put_many_with_metrics(
        &self,
        entries: &[(Hash8, DedupValue)],
        lsn: Lsn,
        timings: &mut DedupPutStageTimings,
    ) -> Result<Vec<Hash8>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        if !self.drainer_enabled {
            return self.put_many_with_metrics(entries, lsn, timings);
        }
        let sketch = self.state.read().sketch.clone();
        for (hash, value) in entries {
            self.staging.merge_put(*hash, *value, lsn);
            let started = std::time::Instant::now();
            sketch.insert(fp_of(hash));
            timings.l0_insert += started.elapsed();
        }
        Ok(Vec::new())
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
            let (new, old_present) = {
                let st = self.state.read();
                (st.new.clone(), st.old.is_some())
            };
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
                // so post-drain reads hit memory. During a resize the drain
                // must use the same walker-serialising atomic insert into NEW.
                let dropped = if old_present {
                    let mut d = Vec::new();
                    for entry in &cuckoo_entries {
                        if !new.put_overwrite_atomic(entry.hash, entry.value, max_put_lsn)? {
                            d.push(entry.hash);
                        }
                    }
                    d
                } else {
                    new.put_many_with_metrics(&cuckoo_entries, max_put_lsn, &mut timings)?
                };
                if !dropped.is_empty() {
                    // Table saturated during drain: drop these promotes
                    // (a future dedup miss, not a failure). Their rc was
                    // already staged at apply time, so this is a bounded
                    // rc OVER-count (leak, the safe direction) until the
                    // entry is re-promoted or reclaimed by GC. Critically,
                    // we must NOT re-stage or error — that would wedge the
                    // flush/checkpoint barrier (`preempt_and_drain_for_checkpoint`).
                    tracing::warn!(
                        dropped = dropped.len(),
                        "dedup cuckoo saturated during drain; promotes dropped (bounded rc over-count)"
                    );
                }
                for entry in &cuckoo_entries {
                    if !dropped.is_empty() && dropped.contains(&entry.hash) {
                        continue;
                    }
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
        let (_, new, old) = self.snapshot();
        let mut wrote = new.flush_meta()?;
        if let Some(old) = &old {
            wrote |= old.flush_meta()?;
        }
        Ok(wrote)
    }

    pub fn iter(&self) -> Result<Vec<(Hash8, DedupValue)>> {
        let (_, new, old) = self.snapshot();
        // Fast path: steady state (no OLD table) + no drainer overlay → return
        // the single table's iter directly (byte-identical to the old path).
        if old.is_none() && !self.drainer_enabled {
            return new.iter();
        }
        // Union OLD then NEW (NEW is fresher, so it overwrites), then overlay
        // any not-yet-drained staged mutations so callers (the soak reference
        // model, `iter_dedup`) see a complete logical snapshot.
        let mut map: HashMap<Hash8, DedupValue> = HashMap::new();
        if let Some(old) = &old {
            for (hash, value) in old.iter()? {
                map.insert(hash, value);
            }
        }
        for (hash, value) in new.iter()? {
            map.insert(hash, value);
        }
        if self.drainer_enabled {
            for (hash, m) in self.staging.snapshot_all() {
                match m {
                    StagedMutation::Put { value, .. } => {
                        map.insert(hash, value);
                    }
                    StagedMutation::Delete { .. } => {
                        map.remove(&hash);
                    }
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
        // Scans the ACTIVE (NEW) table only. During a resize, OLD-only entries
        // are not yet visible to the orphan-reclaim sweep, which is correct: it
        // rests on the onyx Gate-2 confirm + guarded delete, not scan
        // completeness, and the walker migrates OLD into NEW shortly.
        self.state.read().new.scan_from(page_idx, slot, limit)
    }

    /// Walk every allocated data page id (used by verifier) — union of both
    /// tables during a resize.
    pub fn data_page_ids(&self) -> Vec<PageId> {
        let (_, new, old) = self.snapshot();
        let mut ids = new.data_page_ids();
        if let Some(old) = &old {
            ids.extend(old.data_page_ids());
        }
        ids
    }

    /// Highest page id the dedup index physically references (meta chain + data
    /// pages), across BOTH tables during a resize. The device-path open MUST
    /// fold this into the bounded-scan ceiling — the cuckoo meta chain is
    /// generation-stable + in-place, so it can recover ahead of an older
    /// manifest's `page_high_water`. See
    /// [`crate::dedup::cuckoo::CuckooHash::max_referenced_page_id`].
    pub fn max_referenced_page_id(&self) -> PageId {
        let (_, new, old) = self.snapshot();
        let mut max = new.max_referenced_page_id();
        if let Some(old) = &old {
            max = max.max(old.max_referenced_page_id());
        }
        max
    }

    /// Every page id the dedup index physically references (meta head + meta
    /// chain + data pages), across BOTH tables during a resize. The device-path
    /// open removes these from the persisted free-list bitmap so `allocate()`
    /// never re-hands a live dedup page — see
    /// [`crate::dedup::cuckoo::CuckooHash::referenced_page_ids`].
    pub fn referenced_page_ids(&self) -> Vec<PageId> {
        let (_, new, old) = self.snapshot();
        let mut ids = new.referenced_page_ids();
        if let Some(old) = &old {
            ids.extend(old.referenced_page_ids());
        }
        ids
    }

    /// Approximate live entry count. Tracks the cuckoo's running counter; for
    /// an exact figure call [`recount`]. During a resize this SUMS both tables,
    /// so it is an UPPER bound (a migrated-but-not-yet-swapped entry is counted
    /// in both) — fine for load-factor reporting; exact once Single again.
    pub fn approx_len(&self) -> u64 {
        let (_, new, old) = self.snapshot();
        new.approx_len() + old.as_ref().map(|o| o.approx_len()).unwrap_or(0)
    }

    pub fn recount(&self) -> Result<u64> {
        let (_, new, old) = self.snapshot();
        let mut n = new.recount()?;
        if let Some(old) = &old {
            n += old.recount()?;
        }
        Ok(n)
    }

    // ---- online modulus resize (incremental two-table migration) ----
    //
    // begin_grow / migrate_step / finish_swap implement the Elastic-Cuckoo
    // style resize. The Single→Growing and Growing→Single transitions
    // (`begin_grow` / `finish_swap`) MUST run under the checkpoint barrier
    // (`apply_gate.write()`) with the migration walker quiesced — no in-flight
    // put/delete/get/migrate_step — so re-pointing the state triple and freeing
    // OLD's pages can't race a live operation. `migrate_step` runs OUTSIDE the
    // barrier (concurrent with the front end); its safety rests on the
    // OLD-before-NEW lock ordering in `CuckooHash::migrate_page_into`.

    /// Enter the Growing phase: allocate a fresh table with `new_bucket_count`
    /// buckets (must exceed the current modulus), demote the current table to
    /// OLD (insert-frozen), and reseed L0 at the new capacity from OLD's
    /// fingerprints. The new (empty) table's meta is flushed so its head pid is
    /// durable before the caller records it in the manifest. Errors if already
    /// Growing. Caller holds the checkpoint barrier.
    pub(crate) fn begin_grow(&self, new_bucket_count: u64, seed1: u64, seed2: u64) -> Result<()> {
        let mut st = self.state.write();
        if st.old.is_some() {
            return Err(MetaDbError::InvalidArgument(
                "dedup online resize already in progress".into(),
            ));
        }
        if new_bucket_count <= st.new.bucket_count() {
            return Err(MetaDbError::InvalidArgument(format!(
                "dedup grow target {new_bucket_count} must exceed current modulus {}",
                st.new.bucket_count(),
            )));
        }
        let new_table = CuckooHash::create(
            self.page_store.clone(),
            self.page_cache.clone(),
            new_bucket_count,
            seed1,
            seed2,
        )?;
        // Durable head before the manifest references it (external-pages-first).
        new_table.flush_meta()?;
        let demoted = st.new.clone();
        st.old = Some(demoted);
        st.new = Arc::new(new_table);
        // Deliberately KEEP the existing sketch here. It already covers OLD's
        // fingerprints, and the front end keeps inserting NEW's fps into it, so
        // reads stay correct through the whole Growing phase: a cuckoo FILTER
        // never yields a false negative, and as the (OLD-capacity) sketch fills
        // toward NEW's larger working set it saturates and degrades to
        // contains()==true — still no missed dedup, only a rising false-positive
        // rate (extra table probes). `reseed_l0_after_grow`, called by
        // `dedup_resize_begin` immediately after it RELEASES `apply_gate.write`,
        // rebuilds a NEW-capacity sketch off the gate to restore the FPR.
        //
        // This is the fix for the box-observed multi-second commit stall / fio
        // 断流: the old code walked the entire soon-to-be-OLD table to reseed a
        // fresh sketch RIGHT HERE, under both `state.write()` AND (via the
        // caller) `apply_gate.write()`, blocking every commit for the duration
        // (seconds on a ~1M-entry table).
        Ok(())
    }

    /// Rebuild the L0 sketch at NEW's capacity from the OLD ∪ NEW fingerprint
    /// union, then install it. Called by [`crate::db::Db::dedup_resize_begin`]
    /// AFTER it releases `apply_gate.write` — so the (potentially multi-second)
    /// walk never stalls commits. Correctness during the walk is carried by the
    /// retained pre-grow sketch (see `begin_grow`); this only restores a
    /// low-false-positive filter. A crash before/during this is harmless: reopen
    /// resumes Growing and rebuilds L0 from the union in `open_growing`.
    ///
    /// A handful of front-end puts landing in the retained sketch during the
    /// walk (but after their table page was visited) can be missed by the fresh
    /// sketch → a bounded, transient dedup-ratio dip (never a correctness
    /// issue — a missed L0 hit is a dedup miss, not wrong data), self-healing on
    /// the next reopen L0 rebuild.
    pub(crate) fn reseed_l0_after_grow(&self) -> Result<()> {
        let (new, old) = {
            let st = self.state.read();
            (st.new.clone(), st.old.clone())
        };
        let Some(old) = old else {
            return Ok(()); // resize already finished (or never started)
        };
        let sketch = FpSketch::with_capacity(l0_capacity_for(new.bucket_count()));
        old.for_each(|hash, _| {
            sketch.insert(fp_of(&hash));
            Ok(())
        })?;
        new.for_each(|hash, _| {
            sketch.insert(fp_of(&hash));
            Ok(())
        })?;
        let mut st = self.state.write();
        // Only install if still Growing. If a concurrent finish_swap already
        // ended the resize, the retained sketch it kept still covers NEW (OLD's
        // originals + the front end's NEW inserts), so leaving it is correct —
        // we just skip swapping in a sketch built from a now-stale OLD snapshot.
        if st.old.is_some() {
            st.sketch = Arc::new(sketch);
        }
        Ok(())
    }

    /// Copy up to `max_pages` OLD pages (starting at `start_page`, wrapping) into
    /// NEW via [`CuckooHash::migrate_page_into`]. Returns the accumulated tally,
    /// the next page cursor, and `wrapped` (a full pass over OLD completed → the
    /// caller may request the swap). No-op (`growing = false`) when not Growing.
    /// Runs concurrently with the front end; the OLD-before-NEW lock ordering
    /// keeps it race-free.
    pub(crate) fn migrate_step(
        &self,
        start_page: usize,
        max_pages: usize,
        lsn: Lsn,
    ) -> Result<MigrateStepStats> {
        let (new, old) = {
            let st = self.state.read();
            (st.new.clone(), st.old.clone())
        };
        let Some(old) = old else {
            return Ok(MigrateStepStats {
                growing: false,
                next_page: 0,
                wrapped: true,
                ..MigrateStepStats::default()
            });
        };
        let page_count = old.page_count();
        if page_count == 0 || max_pages == 0 {
            return Ok(MigrateStepStats {
                growing: true,
                next_page: 0,
                wrapped: true,
                ..MigrateStepStats::default()
            });
        }
        let mut pi = if start_page >= page_count {
            0
        } else {
            start_page
        };
        let mut acc = MigrateStepStats {
            growing: true,
            ..MigrateStepStats::default()
        };
        let mut wrapped = false;
        for _ in 0..max_pages {
            let s = old.migrate_page_into(&new, pi, lsn)?;
            acc.inserted += s.inserted;
            acc.already_present += s.already_present;
            acc.dropped += s.dropped;
            pi += 1;
            if pi >= page_count {
                pi = 0;
                wrapped = true;
                break;
            }
        }
        acc.next_page = pi;
        acc.wrapped = wrapped;
        Ok(acc)
    }

    /// Complete the resize: drop the OLD table and return its physically
    /// referenced page ids for the caller to free AFTER the swap-to-Single
    /// manifest (`dedup_migration_old_head = NULL_PAGE`) is durable. Returns an
    /// empty vec if not Growing. Caller holds the checkpoint barrier with the
    /// walker quiesced (so no in-flight reader/copier holds an OLD `Arc` whose
    /// pages we are about to free).
    pub(crate) fn finish_swap(&self) -> Result<Vec<PageId>> {
        let mut st = self.state.write();
        let Some(old) = st.old.take() else {
            return Ok(Vec::new());
        };
        drop(st);
        // The RAM for `old` is released once the last in-flight `Arc` clone
        // drops; the caller frees the returned on-disk pages after durability.
        Ok(old.referenced_page_ids())
    }

    /// Snapshot of the resize state for `onyx status` and metrics.
    pub fn migration_status(&self) -> DedupMigrationStatus {
        let st = self.state.read();
        DedupMigrationStatus {
            growing: st.old.is_some(),
            new_bucket_count: st.new.bucket_count(),
            old_bucket_count: st.old.as_ref().map(|o| o.bucket_count()).unwrap_or(0),
            new_len: st.new.approx_len(),
            old_len: st.old.as_ref().map(|o| o.approx_len()).unwrap_or(0),
        }
    }

    /// In-memory tier sizes for status / soak metrics.
    pub fn tier_sizes(&self) -> TierSizes {
        let sketch = self.state.read().sketch.clone();
        TierSizes {
            l0_distinct_fps: sketch.len(),
            l0_approx_bytes: sketch.approx_bytes(),
            l1_entries: self.l1.len(),
        }
    }

    pub fn tier_sizes_best_effort(&self) -> TierSizes {
        let sketch = self.state.try_read().map(|st| st.sketch.clone());
        match sketch {
            Some(sketch) => TierSizes {
                l0_distinct_fps: sketch.try_len().unwrap_or(0),
                l0_approx_bytes: sketch.try_approx_bytes().unwrap_or(0),
                l1_entries: self.l1.try_len().unwrap_or(0),
            },
            None => TierSizes {
                l0_distinct_fps: 0,
                l0_approx_bytes: 0,
                l1_entries: self.l1.try_len().unwrap_or(0),
            },
        }
    }
}

/// Progress of one [`DedupIndex::migrate_step`] call.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MigrateStepStats {
    /// False when not Growing (nothing to migrate).
    pub growing: bool,
    /// Entries newly copied into NEW this step.
    pub inserted: u64,
    /// Entries already present in NEW (fresher front-end write, or re-walk).
    pub already_present: u64,
    /// Entries dropped on NEW saturation (unreachable at provisioned load).
    pub dropped: u64,
    /// OLD page cursor to resume from next call.
    pub next_page: usize,
    /// True when this step completed a full pass over OLD.
    pub wrapped: bool,
}

/// Online-resize status for `onyx status` / metrics.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DedupMigrationStatus {
    pub growing: bool,
    pub new_bucket_count: u64,
    pub old_bucket_count: u64,
    pub new_len: u64,
    pub old_len: u64,
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

    // ── online modulus resize (two-table migration) ──────────────

    fn migrate_to_completion(idx: &DedupIndex) {
        let mut cursor = 0;
        let mut guard = 0;
        loop {
            let s = idx.migrate_step(cursor, 4, 1_000_000).unwrap();
            assert!(s.growing, "migrate_step must report growing until swap");
            cursor = s.next_page;
            guard += 1;
            assert!(guard < 100_000, "migration must terminate");
            if s.wrapped {
                break;
            }
        }
    }

    #[test]
    fn grow_migrate_swap_preserves_all_entries() {
        let (_d, idx) = make_index(); // 64 buckets
        for i in 0..100u8 {
            idx.put(h(i), dv(i), 100 + i as u64).unwrap();
        }
        idx.begin_grow(256, 0x1111, 0x2222).unwrap();
        assert!(idx.is_growing());
        assert_eq!(idx.bucket_count(), 256);
        // Everything still visible, routed to OLD (NEW starts empty).
        for i in 0..100u8 {
            assert_eq!(idx.get(&h(i)).unwrap(), Some(dv(i)), "pre-migrate i={i}");
        }
        migrate_to_completion(&idx);
        for i in 0..100u8 {
            assert_eq!(idx.get(&h(i)).unwrap(), Some(dv(i)), "post-migrate i={i}");
        }
        // Swap to Single; OLD pages returned for the caller to free.
        let freed = idx.finish_swap().unwrap();
        assert!(!idx.is_growing());
        assert!(!freed.is_empty(), "OLD had allocated pages to reclaim");
        // All entries now served by NEW alone.
        for i in 0..100u8 {
            assert_eq!(idx.get(&h(i)).unwrap(), Some(dv(i)), "post-swap i={i}");
        }
        assert_eq!(idx.recount().unwrap(), 100);
    }

    #[test]
    fn growing_reads_route_new_then_old() {
        let (_d, idx) = make_index();
        idx.put(h(1), dv(11), 100).unwrap(); // A → soon-OLD table
        idx.begin_grow(256, 7, 9).unwrap();
        idx.put(h(2), dv(22), 200).unwrap(); // B → NEW
        assert_eq!(idx.get(&h(1)).unwrap(), Some(dv(11)), "A resolves from OLD");
        assert_eq!(idx.get(&h(2)).unwrap(), Some(dv(22)), "B resolves from NEW");
        assert_eq!(
            idx.multi_get(&[h(1), h(2), h(3)]).unwrap(),
            vec![Some(dv(11)), Some(dv(22)), None]
        );
    }

    #[test]
    fn growing_front_end_overwrite_beats_stale_old_copy() {
        let (_d, idx) = make_index();
        idx.put(h(1), dv(11), 100).unwrap();
        idx.begin_grow(256, 7, 9).unwrap();
        // Front-end writes a fresher value → NEW.
        idx.put(h(1), dv(99), 200).unwrap();
        assert_eq!(idx.get(&h(1)).unwrap(), Some(dv(99)));
        // Walker carries the stale OLD copy; put_if_absent must NOT clobber NEW.
        migrate_to_completion(&idx);
        assert_eq!(
            idx.get(&h(1)).unwrap(),
            Some(dv(99)),
            "fresher NEW value survives migration"
        );
        idx.finish_swap().unwrap();
        assert_eq!(idx.get(&h(1)).unwrap(), Some(dv(99)));
    }

    #[test]
    fn delete_during_growing_removes_from_both_and_is_not_resurrected() {
        let (_d, idx) = make_index();
        idx.put(h(1), dv(11), 100).unwrap();
        idx.begin_grow(256, 7, 9).unwrap();
        idx.delete(&h(1), 200).unwrap();
        assert_eq!(idx.get(&h(1)).unwrap(), None, "deleted while growing");
        // Migration re-reads OLD (entry already cleared) → skip; no resurrection.
        migrate_to_completion(&idx);
        assert_eq!(idx.get(&h(1)).unwrap(), None, "walker must not resurrect");
        idx.finish_swap().unwrap();
        assert_eq!(idx.get(&h(1)).unwrap(), None);
    }

    #[test]
    fn growing_reads_correct_before_and_after_off_gate_l0_reseed() {
        // Issue C fix: `begin_grow` no longer walks OLD to reseed L0 under the
        // apply gate (that walk was the multi-second commit stall / fio 断流).
        // It keeps the pre-grow sketch instead, and `reseed_l0_after_grow` runs
        // OFF the gate. Reads must stay correct across all three sub-states.
        let (_d, idx) = make_index(); // 64 buckets → L0 capacity 256
        for i in 0..200u8 {
            idx.put(h(i), dv(i), 100 + i as u64).unwrap();
        }
        // Big NEW so post-grow inserts never saturate the TABLE; the point is to
        // saturate the retained OLD-sized SKETCH, not the table.
        idx.begin_grow(4096, 7, 9).unwrap();
        // (1) Immediately after begin_grow — retained sketch still covers OLD.
        for i in 0..200u8 {
            assert_eq!(idx.get(&h(i)).unwrap(), Some(dv(i)), "post-begin i={i}");
        }
        // (2) Front-end inserts into NEW push the retained (256-cap) sketch past
        //     saturation → contains()==true. A cuckoo FILTER never false-
        //     negatives, so every entry (OLD + NEW) must still be found.
        for i in 200..255u8 {
            idx.put(h(i), dv(i), 300 + i as u64).unwrap();
        }
        for i in 0..255u8 {
            assert_eq!(idx.get(&h(i)).unwrap(), Some(dv(i)), "pre-reseed i={i}");
        }
        // (3) Off-gate reseed installs a NEW-capacity sketch; reads still exact.
        idx.reseed_l0_after_grow().unwrap();
        for i in 0..255u8 {
            assert_eq!(idx.get(&h(i)).unwrap(), Some(dv(i)), "post-reseed i={i}");
        }
        // And the migration still completes + swaps cleanly on top of it.
        migrate_to_completion(&idx);
        idx.finish_swap().unwrap();
        for i in 0..255u8 {
            assert_eq!(idx.get(&h(i)).unwrap(), Some(dv(i)), "post-swap i={i}");
        }
    }

    #[test]
    fn concurrent_front_end_and_migration_never_loses_entries() {
        // Stress the migration races under real threads: readers must ALWAYS
        // see every live entry (no drop / no error) while the walker migrates
        // and a writer overwrites concurrently; no duplicate/resurrection.
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
        let (_d, idx) = make_index(); // 64 buckets → 3 OLD pages
        let idx = Arc::new(idx);
        for i in 0..200u8 {
            idx.put(h(i), dv(i), 100 + i as u64).unwrap();
        }
        idx.begin_grow(1024, 7, 9).unwrap(); // 16× → ample headroom, no saturation

        let stop = Arc::new(AtomicBool::new(false));
        let read_errors = Arc::new(AtomicU64::new(0));
        let read_none = Arc::new(AtomicU64::new(0));

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let idx = idx.clone();
                let stop = stop.clone();
                let re = read_errors.clone();
                let rn = read_none.clone();
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        for i in 0..200u8 {
                            match idx.get(&h(i)) {
                                Ok(Some(_)) => {}
                                Ok(None) => {
                                    rn.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(_) => {
                                    re.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                })
            })
            .collect();

        let writer = {
            let idx = idx.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut round = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    for i in 0..200u8 {
                        // Overwrite with a fresh value (fresh-wins path).
                        idx.put(h(i), dv(i.wrapping_add(1)), 5000 + round).unwrap();
                    }
                    round += 1;
                }
            })
        };

        // Drive the migration to completion concurrently with reads + writes.
        let mut cursor = 0usize;
        let mut guard = 0;
        loop {
            let s = idx.migrate_step(cursor, 1, 9000).unwrap();
            cursor = s.next_page;
            guard += 1;
            assert!(guard < 1_000_000);
            if s.wrapped {
                break;
            }
        }
        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
        for r in readers {
            r.join().unwrap();
        }

        assert_eq!(
            read_errors.load(Ordering::Relaxed),
            0,
            "no get may error during migration"
        );
        assert_eq!(
            read_none.load(Ordering::Relaxed),
            0,
            "no live entry may momentarily read as None during migration (dropped entry)"
        );

        idx.finish_swap().unwrap();
        for i in 0..200u8 {
            assert!(idx.get(&h(i)).unwrap().is_some(), "post-swap i={i}");
        }
        assert_eq!(
            idx.recount().unwrap(),
            200,
            "no duplicate / no loss after swap"
        );
    }

    #[test]
    fn open_growing_resumes_and_completes_migration() {
        // Crash-recovery of a mid-resize database: persist BOTH cuckoo metas
        // (Growing), reopen via `open_growing`, and confirm the migration
        // resumes and completes without losing any entry.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let (new_pid, old_pid);
        {
            let page_store = Arc::new(PageStore::create(&path).unwrap());
            let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
            let idx =
                DedupIndex::create(page_store, page_cache, 64, 16, 0xAA, 0xBB, 4, false).unwrap();
            for i in 0..80u8 {
                idx.put(h(i), dv(i), 100 + i as u64).unwrap();
            }
            idx.begin_grow(256, 0xCC, 0xDD).unwrap();
            // Partially migrate, then persist both tables' metas (mid-resize).
            idx.migrate_step(0, 1, 1000).unwrap();
            idx.flush_meta().unwrap();
            new_pid = idx.meta_page_id();
            old_pid = idx.old_meta_page_id().expect("growing → OLD head present");
        }
        // Reopen mid-resize from the two persisted heads.
        let page_store = Arc::new(PageStore::open(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let idx = DedupIndex::open_growing(page_store, page_cache, new_pid, old_pid, 16, 4, false)
            .unwrap();
        assert!(idx.is_growing(), "reopen resumes Growing");
        assert_eq!(idx.bucket_count(), 256);
        for i in 0..80u8 {
            assert_eq!(idx.get(&h(i)).unwrap(), Some(dv(i)), "resume visible i={i}");
        }
        // Finish the migration and swap to Single.
        migrate_to_completion(&idx);
        for i in 0..80u8 {
            assert_eq!(idx.get(&h(i)).unwrap(), Some(dv(i)), "post-resume i={i}");
        }
        idx.finish_swap().unwrap();
        assert!(!idx.is_growing());
        for i in 0..80u8 {
            assert_eq!(idx.get(&h(i)).unwrap(), Some(dv(i)), "post-swap i={i}");
        }
        assert_eq!(idx.recount().unwrap(), 80);
    }

    #[test]
    fn begin_grow_rejects_smaller_or_concurrent() {
        let (_d, idx) = make_index(); // 64
        assert!(
            idx.begin_grow(64, 1, 2).is_err(),
            "must exceed current modulus"
        );
        assert!(idx.begin_grow(32, 1, 2).is_err());
        idx.begin_grow(256, 1, 2).unwrap();
        assert!(
            idx.begin_grow(1024, 1, 2).is_err(),
            "no second concurrent resize"
        );
    }
}
