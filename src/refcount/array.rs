//! Disk-backed paged refcount array.
//!
//! Layout: PBA space partitioned into fixed-size pages. Each *data*
//! page holds [`ENTRIES_PER_PAGE`] consecutive `(rc, birth_lsn)` pairs
//! starting at `pba = page_idx * ENTRIES_PER_PAGE`. Each shard owns
//! one *meta* page that records the data page id for every page_idx
//! the shard has ever populated. Holes (PBA ranges with rc=0
//! everywhere) are encoded as `PageId(0)` in the meta page and
//! consume neither a data page on disk nor an entry in the page
//! cache.
//!
//! # On-disk encoding
//!
//! ## Data page (PageType::RefcountArray)
//! - shared header (64 B) — `generation` doubles as `last_applied_lsn`
//!   so replay can skip ops whose LSN ≤ page generation. (Refcount
//!   apply is NOT idempotent — see metadb/CLAUDE.md recovery section.)
//! - payload (4032 B):
//!   for slot in 0..ENTRIES_PER_PAGE (336):
//!     rc[slot]:        u32 LE  (offset slot*12)
//!     birth_lsn[slot]: u64 LE  (offset slot*12 + 4)
//!   336 × 12 = 4032 B (no slack)
//!
//! ## Meta page chain (PageType::RefcountArray, `key_count = META_KEY_COUNT_MARKER`)
//! Each meta page carries a fixed 16 B chain header (`chunk_len: u32`,
//! reserved `u32`, `next_meta_pid: u64`) followed by a slice of the
//! page table (`[u64; chunk_len]`). The head meta page id is recorded
//! in the manifest; continuation pages are reachable via the chain
//! pointer. The chain grows / shrinks as the page table does — see
//! [`crate::paged_meta`].
//!
//! # Concurrency
//!
//! Disk I/O is gated by a single shard-level `Mutex`; readers contend
//! with the apply lane on it. Refcount reads are infrequent vs L2P
//! reads, so the mutex coarsen does not show up in metrics.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use parking_lot::Mutex;

use super::RcEntry;
use crate::cache::PageCache;
use crate::error::{MetaDbError, Result};
use crate::page::{PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use crate::page_store::{IoLaneClass, PageStore};
use crate::paged_meta;
use crate::types::{Lsn, PageId, Pba};

use super::delta::Pending;

/// Entries per data page. 336 × 12 B = 4032 B = PAGE_PAYLOAD_SIZE.
pub const ENTRIES_PER_PAGE: usize = 336;
pub(crate) const ENTRY_BYTES: usize = 12;

/// Marker stored in the shared header's `key_count` slot to distinguish
/// the meta page from data pages of the same `PageType`. Data pages
/// have `key_count = 0` (we don't track liveness per-slot since the
/// `rc == 0` check already disambiguates).
const META_KEY_COUNT_MARKER: u16 = 0xFFFF;
const DATA_KEY_COUNT_MARKER: u16 = 0;

// A checkpoint can touch millions of sparse refcount pages. Keep the base-page
// read parallelism bounded so the returned Arc<Page>s cannot pin an entire
// checkpoint above the configured cache capacity while sealed pages accumulate.
pub(super) const STAGE_BASE_READ_BATCH_PAGES: usize = 4096;

const _: () = {
    assert!(ENTRIES_PER_PAGE * ENTRY_BYTES == PAGE_PAYLOAD_SIZE);
};

pub struct PagedRefcountArray {
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    meta_page_id: PageId,
    inner: Mutex<Inner>,
    allocated_data_pages: AtomicUsize,
}

/// One sealed data page produced by a sample-phase stage. Tracks
/// whether the underlying page id was freshly allocated (page_idx was
/// a hole pre-stage) so abort can free it cleanly.
pub struct StagedPage {
    pub page_id: PageId,
    pub page_idx: usize,
    pub sealed: Arc<Page>,
    pub is_fresh: bool,
}

/// Compact checkpoint bookkeeping retained after a streaming data-page write
/// has completed. Unlike [`StagedPage`], this carries no page payload Arc.
#[derive(Clone, Copy, Debug)]
pub(super) struct StagedPageMeta {
    pub page_id: PageId,
    pub page_idx: usize,
    pub is_fresh: bool,
}

/// The full set of sealed pages from one shard's sample-phase work,
/// plus the highest LSN merged into them. Returned by
/// [`PagedRefcountArray::stage_deltas_in_memory`].
pub struct StagedDeltas {
    pub pages: Vec<StagedPage>,
    /// Highest LSN observed across the staged pages' headers. Equal
    /// to `max(pending.last_lsn)` over the drained delta batch when
    /// the on-disk page generations were lower; otherwise equal to
    /// the prior page generation. Used by tests / metrics; the page
    /// header generation is the durable record.
    pub max_lsn: Lsn,
}

impl StagedDeltas {
    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    /// Append `(page_id, sealed)` pairs to a shared write-out vec —
    /// matches L2P's `FlushedSnapshot::append_sealed_pages` pattern so
    /// refcount sealed pages share a single
    /// `page_store.write_sealed_page_runs` + `sync` with L2P during
    /// `flush_with_gate`.
    pub fn append_sealed_pages(&self, out: &mut Vec<(PageId, Arc<Page>)>) {
        out.extend(
            self.pages
                .iter()
                .map(|page| (page.page_id, page.sealed.clone())),
        );
    }

    pub(super) fn compact_pages(&self) -> Vec<StagedPageMeta> {
        self.pages
            .iter()
            .map(|page| StagedPageMeta {
                page_id: page.page_id,
                page_idx: page.page_idx,
                is_fresh: page.is_fresh,
            })
            .collect()
    }
}

struct Inner {
    /// Mirrors the on-disk page table. Length is the highest
    /// page_idx ever written + 1; interior holes hold `PageId(0)`.
    page_table: Vec<PageId>,
    /// Meta page chain in order, head first. The head pid is fixed
    /// (recorded in the manifest); continuation pages are allocated /
    /// freed by `flush_meta_locked` as the table grows or shrinks.
    meta_chain: Vec<PageId>,
    /// Set on every write that grows or mutates `page_table`. Cleared
    /// after the meta page is rewritten in `flush_meta_locked`.
    meta_dirty: bool,
    /// Dirty-staged page overlay: pid -> sealed (or placeholder) page
    /// content for every staged-but-not-yet-durable data page. The rc
    /// analogue of the paged tree's `PageBuf` dirty set.
    ///
    /// Why it must exist: the checkpoint fold (`stage_one_page`) runs in
    /// the GATELESS sample phase (BFG gate-shrink), so
    /// commit-side reads (`RcShard::stage` / `lookup_entry` under
    /// `rc_authoritative_reclaim`) are concurrent with it. The staged
    /// page only enters the *evictable* shared LRU; its disk write is
    /// the flush's later `write_sealed_page_runs` batch. If the LRU
    /// evicts the staged page inside that window, a reader falls
    /// through to disk and sees, for a fresh page, unwritten zeros
    /// (`PageMagicMismatch 0x0` — the 2026-06-11 nvme-box commit-error
    /// case), or, for a pre-existing page, the PRE-fold content while the
    /// delta slot was already cleared (silent rc under-count → spurious
    /// `freed_pba` → premature free). Every data-page read therefore
    /// consults this overlay (under `inner`) before the cache/disk.
    ///
    /// Entries are inserted under the SAME `inner` lock that publishes
    /// the pid in `page_table` (fresh pages get an all-zero placeholder
    /// at publish time, replaced by the sealed page once built), and
    /// removed by `clear_staged` once the page bytes are durable, or by
    /// `abort_staged_deltas` (always BEFORE the fresh pid is freed, so a
    /// recycled pid can never be shadowed by a stale overlay entry).
    staged_overlay: HashMap<PageId, Arc<Page>>,
}

struct CleanPageRun {
    page_id: PageId,
    start: usize,
    end: usize,
}

#[derive(Clone)]
struct StagePageBase {
    page_idx: usize,
    page_id: PageId,
    is_fresh: bool,
    base: Option<Arc<Page>>,
}

impl PagedRefcountArray {
    /// Create a fresh shard backing store. Allocates the meta page
    /// from `page_store`. The returned `meta_page_id()` must be
    /// recorded in the manifest.
    pub fn create(page_store: Arc<PageStore>, page_cache: Arc<PageCache>) -> Result<Self> {
        let meta_page_id = page_store.allocate()?;
        let inner = Inner {
            page_table: Vec::new(),
            meta_chain: vec![meta_page_id],
            meta_dirty: false,
            staged_overlay: HashMap::new(),
        };
        let me = Self {
            page_store,
            page_cache,
            meta_page_id,
            inner: Mutex::new(inner),
            allocated_data_pages: AtomicUsize::new(0),
        };
        // Persist an empty meta page so `open()` after a clean restart
        // sees a valid header rather than uninitialised bytes.
        let mut guard = me.inner.lock();
        me.flush_meta_locked(&mut guard)?;
        drop(guard);
        Ok(me)
    }

    /// Open an existing shard. Walks the meta chain rooted at
    /// `meta_page_id` and rebuilds the in-memory page table.
    pub fn open(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        meta_page_id: PageId,
    ) -> Result<Self> {
        let read = paged_meta::read_chain(
            &page_store,
            meta_page_id,
            PageType::RefcountArray,
            META_KEY_COUNT_MARKER,
            0,
        )?;
        let allocated_data_pages = read.page_table.iter().filter(|&&pid| pid != 0).count();
        Ok(Self {
            page_store,
            page_cache,
            meta_page_id,
            inner: Mutex::new(Inner {
                page_table: read.page_table,
                meta_chain: read.chain_pids,
                meta_dirty: false,
                staged_overlay: HashMap::new(),
            }),
            allocated_data_pages: AtomicUsize::new(allocated_data_pages),
        })
    }

    pub fn meta_page_id(&self) -> PageId {
        self.meta_page_id
    }

    /// Populate the shared cache with every allocated refcount data page.
    /// The page table itself is already resident in `inner` after open.
    pub(crate) fn warmup_data_pages(&self) -> Result<u64> {
        const READ_BATCH_PAGES: usize = 4096;
        let page_ids: Vec<PageId> = self
            .inner
            .lock()
            .page_table
            .iter()
            .copied()
            .filter(|pid| *pid != 0)
            .collect();
        for chunk in page_ids.chunks(READ_BATCH_PAGES) {
            self.page_cache.get_many(chunk)?;
        }
        Ok(page_ids.len() as u64)
    }

    /// Resolve `page_idx` to its pid and, if the page is currently
    /// staged (dirty, not yet durable), the overlay copy — both under
    /// ONE `inner` acquisition so a reader can never observe a
    /// published pid without its overlay entry. Returns `(0, None)`
    /// for a hole.
    fn resolve_data_page(&self, page_idx: usize) -> (PageId, Option<Arc<Page>>) {
        let inner = self.inner.lock();
        let pid = inner.page_table.get(page_idx).copied().unwrap_or(0);
        if pid == 0 {
            return (0, None);
        }
        let staged = inner.staged_overlay.get(&pid).cloned();
        if staged.is_some() {
            super::note_staged_overlay_hit();
        }
        (pid, staged)
    }

    /// Look up one entry. Returns [`RcEntry::ZERO`] if no data page
    /// is allocated for the PBA's page_idx.
    pub fn get(&self, pba: Pba) -> Result<RcEntry> {
        self.get_with_page_lsn(pba).map(|(entry, _)| entry)
    }

    /// Look up one entry and the generation of the exact backing page used
    /// for that entry. Sampling both from one resolved page is required by
    /// commit-side staging: a separate `get` + `page_lsn` could straddle a
    /// checkpoint publish and make replay-skip decisions from a torn view.
    pub(crate) fn get_with_page_lsn(&self, pba: Pba) -> Result<(RcEntry, Lsn)> {
        let (page_idx, slot) = page_offset(pba);
        let (page_id, staged) = self.resolve_data_page(page_idx);
        if page_id == 0 {
            return Ok((RcEntry::ZERO, 0));
        }
        let page = match staged {
            Some(page) => page,
            None => self.page_cache.get(page_id)?,
        };
        Ok((read_entry(&page, slot), page.header()?.generation))
    }

    /// Resolve a batch of entries and their backing-page generations with one
    /// page-table lock and one cache multi-get. The page generation is the
    /// replay-skip operand used by [`crate::refcount::RcShard::stage_batch`].
    pub(crate) fn get_many_with_page_lsn(&self, pbas: &[Pba]) -> Result<Vec<(RcEntry, Lsn)>> {
        if pbas.is_empty() {
            return Ok(Vec::new());
        }
        if pbas.is_sorted() {
            return self.get_many_sorted_with_page_lsn(pbas);
        }

        let resolved: Vec<(PageId, Option<Arc<Page>>, usize)> = {
            let inner = self.inner.lock();
            pbas.iter()
                .map(|&pba| {
                    let (page_idx, slot) = page_offset(pba);
                    let pid = inner.page_table.get(page_idx).copied().unwrap_or(0);
                    let staged = if pid == 0 {
                        None
                    } else {
                        inner.staged_overlay.get(&pid).cloned()
                    };
                    if staged.is_some() {
                        super::note_staged_overlay_hit();
                    }
                    (pid, staged, slot)
                })
                .collect()
        };

        let disk_pids: Vec<PageId> = resolved
            .iter()
            .filter_map(|(pid, staged, _)| (*pid != 0 && staged.is_none()).then_some(*pid))
            .collect();
        let disk_pages = self.page_cache.get_many(&disk_pids)?;
        let mut disk_pages = disk_pages.into_iter();
        let mut out = Vec::with_capacity(pbas.len());
        for (pid, staged, slot) in resolved {
            if pid == 0 {
                out.push((RcEntry::ZERO, 0));
                continue;
            }
            let page = match staged {
                Some(page) => page,
                None => disk_pages.next().ok_or_else(|| {
                    crate::error::MetaDbError::Corruption(
                        "refcount batch page iterator underflow".into(),
                    )
                })?,
            };
            out.push((read_entry(&page, slot), page.header()?.generation));
        }
        debug_assert!(disk_pages.next().is_none());
        Ok(out)
    }

    /// Sorted hot path used by commit-side refcount staging. Resolve each
    /// logical refcount page once, borrow cache hits in place, and decode all
    /// covered PBA slots without expanding one `Arc<Page>` per PBA.
    pub(crate) fn get_many_sorted_with_page_lsn(
        &self,
        pbas: &[Pba],
    ) -> Result<Vec<(RcEntry, Lsn)>> {
        if pbas.is_empty() {
            return Ok(Vec::new());
        }
        debug_assert!(
            pbas.is_sorted(),
            "sorted refcount lookup requires nondecreasing PBAs"
        );

        let mut out = vec![(RcEntry::ZERO, 0); pbas.len()];
        let mut clean_runs = Vec::new();
        {
            let inner = self.inner.lock();
            let overlay_empty = inner.staged_overlay.is_empty();
            let mut start = 0;
            while start < pbas.len() {
                let (page_idx, _) = page_offset(pbas[start]);
                let mut end = start + 1;
                while end < pbas.len() && page_offset(pbas[end]).0 == page_idx {
                    end += 1;
                }

                let page_id = inner.page_table.get(page_idx).copied().unwrap_or(0);
                if page_id == 0 {
                    start = end;
                    continue;
                }
                if !overlay_empty && let Some(page) = inner.staged_overlay.get(&page_id) {
                    decode_page_run(page, pbas, start, end, &mut out)?;
                    for _ in start..end {
                        super::note_staged_overlay_hit();
                    }
                } else {
                    clean_runs.push(CleanPageRun {
                        page_id,
                        start,
                        end,
                    });
                }
                start = end;
            }
        }

        let requests: Vec<(PageId, usize)> = clean_runs
            .iter()
            .map(|run| (run.page_id, run.end - run.start))
            .collect();
        self.page_cache
            .visit_many_weighted(&requests, |request_idx, page| {
                let run = &clean_runs[request_idx];
                decode_page_run(page, pbas, run.start, run.end, &mut out)
            })?;
        Ok(out)
    }

    /// `last_applied_lsn` recorded on the data page covering `pba`.
    /// Returns 0 if no page is allocated yet (replay must apply
    /// unconditionally for a fresh page).
    pub fn page_lsn(&self, pba: Pba) -> Result<Lsn> {
        let (page_idx, _) = page_offset(pba);
        let (page_id, staged) = self.resolve_data_page(page_idx);
        if page_id == 0 {
            return Ok(0);
        }
        let page = match staged {
            Some(page) => page,
            None => self.page_cache.get(page_id)?,
        };
        Ok(page.header()?.generation)
    }

    /// Apply a batch of pending deltas to the on-disk array. Groups
    /// by data page so each page is read+written exactly once. Marks
    /// the meta page dirty if any new data pages were allocated.
    /// The caller must subsequently call [`flush_meta`] to persist
    /// the meta page.
    ///
    /// Implemented as `stage_deltas_in_memory` followed by
    /// `write_staged_pages`. Cold callers (`RcShard::flush` wrapper,
    /// non-checkpoint paths) use this; the checkpoint hot path drives
    /// the two halves separately so the disk write happens outside
    /// `apply_gate.write()`.
    pub fn apply_deltas(&self, deltas: Vec<(Pba, Pending)>) -> Result<()> {
        let staged = self.stage_deltas_in_memory(deltas, false)?;
        self.write_staged_pages(&staged)
    }

    /// Sample-phase work for the checkpoint snapshot model: drain
    /// in-memory state only, no disk IO. Builds sealed pages from the
    /// drained delta batch, allocates fresh page ids for previously-hole
    /// `page_idx` slots, extends `inner.page_table` to point at them,
    /// and inserts each sealed page into the shared `page_cache` so
    /// subsequent `RcShard::stage` reads observe the staged content.
    /// Returns a [`StagedDeltas`] the caller can later persist via
    /// [`Self::write_staged_pages`] (or hand to the global flush
    /// `sealed_pages` runner) and roll back via
    /// [`Self::abort_staged_deltas`].
    ///
    /// `force = true` (the cold-path lifecycle fold,
    /// [`crate::refcount::RcShard::flush`]) folds every drained delta even
    /// when `page_generation >= last_lsn` would normally skip it: the
    /// per-fold replay-skip exists for in-place checkpoint *retries*, but
    /// the cold path is driven by serialized lifecycle ops that never
    /// retry in place, and the non-WAL snapshot incref legitimately
    /// carries `last_lsn == created_lsn ==` the array page's current
    /// generation (the last pre-snapshot op), so it MUST apply despite the
    /// `>=`. The hot run_sync_cycle fold passes `false`. See
    /// [`stage_one_page`].
    pub fn stage_deltas_in_memory(
        &self,
        mut deltas: Vec<(Pba, Pending)>,
        force: bool,
    ) -> Result<StagedDeltas> {
        self.stage_deltas_in_memory_preserving(&mut deltas, force, &[])
    }

    /// Like [`Self::stage_deltas_in_memory`] but additionally applies a set
    /// of **force-increfs** — an unconditional `+1` to each `force_increfs`
    /// pba that (a) always applies (never gated by the per-page replay-skip)
    /// and (b) contributes nothing to the page generation. This is how a
    /// snapshot-root incref folds inside the BFG sync cycle: it must land even
    /// when its natural lsn `<=` the page generation (the snapshot-take group
    /// often has no commits of its own), and it must not
    /// raise the generation — bumping it to a value above a future COW
    /// delta on a SIBLING pid sharing the data page would mis-skip that
    /// sibling's fold → child premature-free (the "bone"). A force-incref
    /// is +1 only, so it never underflows; its idempotency across the
    /// cycle's abort-retry rides the same publish/clear + abort_staged
    /// machinery as the normal slot deltas (the caller recomputes the
    /// incref set from the queued snapshot task each cycle attempt).
    pub fn stage_deltas_in_memory_with_force_increfs(
        &self,
        mut deltas: Vec<(Pba, Pending)>,
        force: bool,
        force_increfs: &[Pba],
    ) -> Result<StagedDeltas> {
        self.stage_deltas_in_memory_preserving(&mut deltas, force, force_increfs)
    }

    /// Stage a fold while retaining the caller's delta vector for checkpoint
    /// rollback. Sorting the slice in place groups logical data pages without
    /// cloning the full delta set or allocating one small `Vec` per page.
    pub(crate) fn stage_deltas_in_memory_preserving(
        &self,
        deltas: &mut [(Pba, Pending)],
        force: bool,
        force_increfs: &[Pba],
    ) -> Result<StagedDeltas> {
        if deltas.is_empty() && force_increfs.is_empty() {
            return Ok(StagedDeltas {
                pages: Vec::new(),
                max_lsn: 0,
            });
        }

        // Stable ordering preserves the former slot-by-slot insertion order
        // when the all-slot recovery path contributes the same PBA more than
        // once. Production's one-slot fold has distinct PBAs.
        deltas.sort_by_key(|(pba, _)| page_offset(*pba).0);

        // Force-increfs grouped by data page. A page may appear in both maps
        // (a snapshot root that also took a COW delta this cycle) or only
        // here (an unmodified shared root) — the union is walked below.
        let mut force_by_page: HashMap<usize, Vec<usize>> = HashMap::new();
        for &pba in force_increfs {
            let (page_idx, slot) = page_offset(pba);
            force_by_page.entry(page_idx).or_default().push(slot);
        }

        let mut max_lsn: Lsn = 0;
        let mut page_idxs = Vec::new();
        let mut cursor = 0;
        while cursor < deltas.len() {
            let page_idx = page_offset(deltas[cursor].0).0;
            page_idxs.push(page_idx);
            cursor += 1;
            while cursor < deltas.len() && page_offset(deltas[cursor].0).0 == page_idx {
                cursor += 1;
            }
        }
        page_idxs.extend(force_by_page.keys().copied());
        page_idxs.sort_unstable();
        page_idxs.dedup();

        let mut pages = Vec::with_capacity(page_idxs.len());
        let mut delta_cursor = 0;
        for page_idx_batch in page_idxs.chunks(STAGE_BASE_READ_BATCH_PAGES) {
            // Consume each bounded base batch before fetching the next one.
            // The staged output remains live for checkpoint writeout, but the
            // replaced clean bases become evictable as this Vec is dropped.
            let page_bases = match self.resolve_stage_page_bases(page_idx_batch) {
                Ok(page_bases) => page_bases,
                Err(err) => {
                    self.abort_staged_deltas(&StagedDeltas { pages, max_lsn }, 0);
                    return Err(err);
                }
            };
            let mut page_bases = page_bases.into_iter().peekable();
            while let Some(page_base) = page_bases.next() {
                let failed_base = page_base.is_fresh.then(|| page_base.clone());
                let delta_start = delta_cursor;
                while delta_cursor < deltas.len()
                    && page_offset(deltas[delta_cursor].0).0 == page_base.page_idx
                {
                    delta_cursor += 1;
                }
                let force_slots = force_by_page
                    .remove(&page_base.page_idx)
                    .unwrap_or_default();
                let staged = match self.stage_one_page(
                    page_base,
                    &deltas[delta_start..delta_cursor],
                    force,
                    &force_slots,
                ) {
                    Ok(staged) => staged,
                    Err(err) => {
                        self.abort_unstaged_page_bases(failed_base);
                        self.abort_unstaged_page_bases(page_bases);
                        self.abort_staged_deltas(&StagedDeltas { pages, max_lsn }, 0);
                        return Err(err);
                    }
                };
                let page_gen = match staged.sealed.header() {
                    Ok(header) => header.generation,
                    Err(err) => {
                        pages.push(staged);
                        self.abort_unstaged_page_bases(page_bases);
                        self.abort_staged_deltas(&StagedDeltas { pages, max_lsn }, 0);
                        return Err(err);
                    }
                };
                if page_gen > max_lsn {
                    max_lsn = page_gen;
                }
                pages.push(staged);
            }
        }
        debug_assert_eq!(delta_cursor, deltas.len());
        Ok(StagedDeltas { pages, max_lsn })
    }

    /// Resolve every page touched by one refcount fold before applying deltas.
    /// Existing clean pages are fetched through one cache multi-get, so cold
    /// checkpoint work reaches the page device as a queued batch instead of a
    /// serial `read_page` loop. Fresh and staged-overlay pages retain the same
    /// publish-before-clear visibility guarantees as the former per-page path.
    fn resolve_stage_page_bases(&self, page_idxs: &[usize]) -> Result<Vec<StagePageBase>> {
        let mut page_bases = Vec::with_capacity(page_idxs.len());
        let mut clean_positions = Vec::new();
        {
            let mut inner = self.inner.lock();
            for &page_idx in page_idxs {
                if page_idx >= inner.page_table.len() {
                    inner.page_table.resize(page_idx + 1, 0);
                    inner.meta_dirty = true;
                }
                if inner.page_table[page_idx] == 0 {
                    let page_id = match self.page_store.allocate() {
                        Ok(page_id) => page_id,
                        Err(err) => {
                            drop(inner);
                            self.abort_unstaged_page_bases(page_bases);
                            return Err(err);
                        }
                    };
                    inner.page_table[page_idx] = page_id;
                    inner.meta_dirty = true;
                    self.allocated_data_pages.fetch_add(1, Ordering::Relaxed);

                    // Publish a valid zero base atomically with the new pid.
                    // Concurrent readers must never fall through to the fresh
                    // page's unwritten disk backing while this fold is built.
                    let mut placeholder = new_empty_data_page();
                    placeholder.seal();
                    inner.staged_overlay.insert(page_id, Arc::new(placeholder));
                    page_bases.push(StagePageBase {
                        page_idx,
                        page_id,
                        is_fresh: true,
                        base: None,
                    });
                    continue;
                }

                let page_id = inner.page_table[page_idx];
                let base = inner.staged_overlay.get(&page_id).cloned();
                let position = page_bases.len();
                page_bases.push(StagePageBase {
                    page_idx,
                    page_id,
                    is_fresh: false,
                    base,
                });
                if page_bases[position].base.is_none() {
                    clean_positions.push(position);
                }
            }
        }

        let clean_page_ids: Vec<PageId> = clean_positions
            .iter()
            .map(|&position| page_bases[position].page_id)
            .collect();
        let clean_pages = match self.page_cache.get_many(&clean_page_ids) {
            Ok(clean_pages) => clean_pages,
            Err(err) => {
                self.abort_unstaged_page_bases(page_bases);
                return Err(err);
            }
        };
        debug_assert_eq!(clean_pages.len(), clean_positions.len());
        for (position, page) in clean_positions.into_iter().zip(clean_pages) {
            page_bases[position].base = Some(page);
        }
        Ok(page_bases)
    }

    /// Undo fresh page reservations that were published with zero placeholders
    /// but never reached [`Self::stage_one_page`]. Existing-page bases have no
    /// state to undo and are simply dropped.
    fn abort_unstaged_page_bases(&self, page_bases: impl IntoIterator<Item = StagePageBase>) {
        for page_base in page_bases {
            if !page_base.is_fresh {
                continue;
            }
            {
                let mut inner = self.inner.lock();
                if page_base.page_idx < inner.page_table.len()
                    && inner.page_table[page_base.page_idx] == page_base.page_id
                {
                    inner.page_table[page_base.page_idx] = 0;
                    inner.meta_dirty = true;
                    self.allocated_data_pages.fetch_sub(1, Ordering::Relaxed);
                }
                inner.staged_overlay.remove(&page_base.page_id);
            }
            self.page_cache.invalidate(page_base.page_id);
            if let Err(err) = self.page_store.free(page_base.page_id, 0) {
                tracing::warn!(
                    page_id = page_base.page_id,
                    error = %err,
                    "abort_unstaged_page_bases: failed to free fresh page id"
                );
            }
        }
    }

    fn stage_one_page(
        &self,
        page_base: StagePageBase,
        pba_pendings: &[(Pba, Pending)],
        force: bool,
        force_incref_slots: &[usize],
    ) -> Result<StagedPage> {
        let StagePageBase {
            page_idx,
            page_id,
            is_fresh,
            base,
        } = page_base;

        let mut page = if is_fresh {
            new_empty_data_page()
        } else {
            // The base must be the staged content when this pid is
            // still dirty from a previous fold (overlay hit) — the LRU
            // copy can be evicted and the disk copy is pre-fold, which
            // would silently drop that fold's deltas (their slot was
            // already cleared).
            match base {
                Some(staged) => (*staged).clone(),
                None => {
                    return Err(MetaDbError::Corruption(format!(
                        "resolved refcount page {page_id} has no base"
                    )));
                }
            }
        };

        let page_generation = page.header()?.generation;
        let mut max_lsn = page_generation;
        for &(pba, pending) in pba_pendings {
            let (pending_page_idx, slot) = page_offset(pba);
            debug_assert_eq!(pending_page_idx, page_idx);
            // Replay-skip: a previous checkpoint attempt may have
            // written this page's deltas to disk before failing
            // (write_meta_chain_external / manifest commit / etc.).
            // The abort path invalidates the cache and restores the
            // drained deltas; on retry, the page is re-read from disk
            // with `generation >= pending.last_lsn` for already-applied
            // slots. Skip them — re-applying would double-count.
            // Mirrors the per-op replay-skip in `RcShard::stage`. The cold
            // lifecycle fold (`force`) bypasses it — it never retries in
            // place, and the snapshot incref's `last_lsn` may legitimately
            // equal the page generation (see `stage_deltas_in_memory_force`).
            if !force && page_generation >= pending.last_lsn {
                continue;
            }
            let prev = read_entry(&page, slot);
            // A decref whose accumulated delta lands past zero is a benign
            // double-decref split across drainer cycles (the on-disk base
            // was already taken to 0 by an earlier cycle). Skip it (leave
            // the entry at its floor) instead of failing the checkpoint.
            let (new, skipped) = super::apply_delta_or_skip(prev, pending.delta, pending.last_lsn)?;
            if skipped {
                super::note_decref_underflow_skip(
                    pending.delta,
                    pending.last_lsn,
                    prev.rc,
                    "array",
                );
            }
            write_entry(&mut page, slot, new);
            if pending.last_lsn > max_lsn {
                max_lsn = pending.last_lsn;
            }
        }

        // Force-increfs: unconditional `+1`, applied AFTER the normal slot
        // deltas (so a root that also took a COW delta this cycle sees both)
        // and deliberately NOT folded into `max_lsn` — the snapshot-root
        // incref must not raise this data page's generation (siblings sharing
        // the page would then mis-skip their own folds; see
        // `stage_deltas_in_memory_with_force_increfs`). No replay-skip (the
        // incref always lands) and no underflow possible.
        for &slot in force_incref_slots {
            let prev = read_entry(&page, slot);
            let (new, _) = super::apply_delta_or_skip(prev, 1, prev.birth_lsn)?;
            write_entry(&mut page, slot, new);
        }

        let mut header = page.header()?;
        header.generation = max_lsn;
        page.write_header(&header);
        page.seal();

        let sealed = Arc::new(page);
        // Publish the sealed content in the overlay FIRST (replacing
        // the fresh placeholder, if any): concurrent `RcShard::stage` /
        // `lookup_entry` reads must observe the post-stage entries
        // (replay-skip correctness), and the overlay — unlike the LRU
        // insert below — cannot be evicted before the page bytes are
        // durable. `clear_staged` drops the entry once
        // `write_sealed_page_runs` + sync have landed.
        self.inner
            .lock()
            .staged_overlay
            .insert(page_id, sealed.clone());
        // Also seed the shared LRU so post-clear reads hit cache
        // instead of re-reading the just-written page from disk.
        self.page_cache.replace_or_insert(page_id, sealed.clone());
        Ok(StagedPage {
            page_id,
            page_idx,
            sealed,
            is_fresh,
        })
    }

    /// Persist sealed pages from a [`StagedDeltas`] to disk. Inline,
    /// non-batched form used by `RcShard::flush()` for cold callers
    /// (snapshot / drop_volume / iter_live_flushed). The checkpoint hot
    /// path appends sealed pages to a shared global vec and writes them
    /// via `page_store.write_sealed_page_runs` instead (and must call
    /// [`Self::clear_staged`] itself once that batch is durable).
    pub fn write_staged_pages(&self, staged: &StagedDeltas) -> Result<()> {
        for staged_page in &staged.pages {
            self.page_store
                .write_page(staged_page.page_id, &staged_page.sealed)?;
        }
        self.clear_staged(staged);
        Ok(())
    }

    /// Write one bounded streaming-checkpoint chunk through the refcount IO
    /// lane. The caller clears the overlay only after this returns: an Ok means
    /// every write CQE has completed, so cache eviction can safely fall through
    /// to the newly-written disk bytes even though the checkpoint-wide sync is
    /// intentionally deferred.
    pub(super) fn write_staged_page_runs(&self, staged: &StagedDeltas) -> Result<()> {
        let pages = staged
            .pages
            .iter()
            .map(|page| (page.page_id, page.sealed.clone()))
            .collect();
        self.page_store
            .write_sealed_page_runs_for_class(pages, IoLaneClass::Refcount)
    }

    /// Drop the dirty-staged overlay entries for `staged` — call ONLY
    /// once the staged page bytes are durable on disk (or from the
    /// abort path, which restores disk truth first). Removal is gated
    /// on `Arc::ptr_eq` so a newer fold's re-staging of the same pid is
    /// never clobbered by a stale clear.
    pub fn clear_staged(&self, staged: &StagedDeltas) {
        if staged.pages.is_empty() {
            return;
        }
        let mut inner = self.inner.lock();
        for staged_page in &staged.pages {
            if inner
                .staged_overlay
                .get(&staged_page.page_id)
                .is_some_and(|cur| Arc::ptr_eq(cur, &staged_page.sealed))
            {
                inner.staged_overlay.remove(&staged_page.page_id);
            }
        }
    }

    /// Snapshot of the current page table; fresh clone, takes the inner
    /// lock briefly. Caller drives `paged_meta::write_chain` outside
    /// the apply gate using this snapshot.
    pub fn page_table_snapshot(&self) -> Vec<PageId> {
        self.inner.lock().page_table.clone()
    }

    /// Snapshot of the current meta chain; fresh clone, takes the inner
    /// lock briefly. Caller passes this as `existing_chain` to
    /// `paged_meta::write_chain` outside the apply gate.
    pub fn meta_chain_snapshot(&self) -> Vec<PageId> {
        self.inner.lock().meta_chain.clone()
    }

    /// Read every page id owned by the durable array rooted at `meta_page_id`:
    /// the stable meta head, its continuations, and all non-hole data pages.
    /// Device open uses this to reconcile a persisted free bitmap after a crash
    /// in which the stable head became durable before the next manifest slot.
    pub(crate) fn referenced_page_ids(
        page_store: &PageStore,
        meta_page_id: PageId,
    ) -> Result<Vec<PageId>> {
        let read = paged_meta::read_chain(
            page_store,
            meta_page_id,
            PageType::RefcountArray,
            META_KEY_COUNT_MARKER,
            0,
        )?;
        let mut pids = read.chain_pids;
        pids.extend(read.page_table.into_iter().filter(|pid| *pid != 0));
        Ok(pids)
    }

    /// Outside-gate IO: write a fresh meta chain whose entries are
    /// `snapshot_page_table` and whose existing chain is
    /// `snapshot_meta_chain`. Returns the new chain (head first).
    /// Does NOT install the new chain into `inner` — that is
    /// [`Self::install_meta_chain`]'s job. The head pid is stable
    /// (`existing_chain[0]`), so the manifest needs no per-flush
    /// update — it always references the head pid recorded at create.
    ///
    /// Cold-path shim (snapshot / drop_volume / `RcShard::flush`); the
    /// flush hot path uses [`Self::build_meta_chain_external`] +
    /// folds the sealed pages into the global checkpoint batch.
    pub fn write_meta_chain_external(
        &self,
        snapshot_page_table: &[PageId],
        snapshot_meta_chain: &[PageId],
        free_lsn: Lsn,
    ) -> Result<Vec<PageId>> {
        let (new_chain, sealed_pages, to_free) =
            self.build_meta_chain_external(snapshot_page_table, snapshot_meta_chain)?;
        self.write_built_meta_chain_external(sealed_pages, to_free, free_lsn)?;
        Ok(new_chain)
    }

    /// Outside-gate, **no-IO** companion of
    /// [`Self::write_meta_chain_external`]: builds + seals every page in
    /// the new chain entirely in memory, returning the chain layout for
    /// the caller to drive a single batched
    /// [`PageStore::write_sealed_page_runs`] across many shards' meta
    /// chains. The caller is then responsible for the
    /// `invalidate + page_store.free` of the trailing pids and the
    /// `page_cache.replace_or_insert` of the sealed pages.
    ///
    /// Used by `flush_with_gate` to fold every refcount shard's
    /// meta-chain pages into the same global submission as the L2P /
    /// refcount data pages — replacing N synchronous per-page
    /// `write_page` round-trips with one io_uring batch.
    pub fn build_meta_chain_external(
        &self,
        snapshot_page_table: &[PageId],
        snapshot_meta_chain: &[PageId],
    ) -> Result<(Vec<PageId>, Vec<(PageId, Arc<Page>)>, Vec<PageId>)> {
        paged_meta::build_chain_pages(
            &self.page_store,
            PageType::RefcountArray,
            META_KEY_COUNT_MARKER,
            &[],
            snapshot_page_table,
            snapshot_meta_chain,
        )
    }

    /// Persist a meta chain that was built separately by
    /// [`Self::build_meta_chain_external`]. Once write submission starts the
    /// stable head may reference this chain's fresh data pages, so callers must
    /// not roll those pages back on an error.
    ///
    /// Reclaim of trailing old-chain pages is best-effort after the new bytes
    /// land. Leaking one old continuation is recoverable; returning an error
    /// that tempts a caller to free newly reachable data pages is not.
    pub(super) fn write_built_meta_chain_external(
        &self,
        sealed_pages: Vec<(PageId, Arc<Page>)>,
        to_free: Vec<PageId>,
        free_lsn: Lsn,
    ) -> Result<()> {
        self.page_store
            .write_sealed_page_runs(sealed_pages.clone())?;

        for (pid, page) in sealed_pages {
            self.page_cache.replace_or_insert(pid, page);
        }
        for pid in to_free {
            self.page_cache.invalidate(pid);
            if let Err(err) = self.page_store.free(pid, free_lsn) {
                tracing::warn!(
                    page_id = pid,
                    error = %err,
                    "write_built_meta_chain_external: failed to free old continuation page"
                );
            }
        }
        Ok(())
    }

    /// Install a freshly-written meta chain. Briefly takes
    /// `inner.lock()`. Clears `meta_dirty` because the on-disk chain
    /// now matches `inner.page_table` (sample-phase mutated the table
    /// under the gate; no concurrent path mutates page_table outside
    /// of sample).
    pub fn install_meta_chain(&self, new_chain: Vec<PageId>) {
        let mut inner = self.inner.lock();
        inner.meta_chain = new_chain;
        inner.meta_dirty = false;
    }

    /// Roll back state mutated by [`Self::stage_deltas_in_memory`].
    /// Best-effort; failures are logged via tracing and otherwise
    /// ignored — abort runs on an error path and a subsequent
    /// `RcShard::stage` retry must converge regardless.
    pub fn abort_staged_deltas(&self, staged: &StagedDeltas, free_lsn: Lsn) {
        for staged_page in &staged.pages {
            if staged_page.is_fresh {
                {
                    let mut inner = self.inner.lock();
                    if staged_page.page_idx < inner.page_table.len()
                        && inner.page_table[staged_page.page_idx] == staged_page.page_id
                    {
                        inner.page_table[staged_page.page_idx] = 0;
                        inner.meta_dirty = true;
                        self.allocated_data_pages.fetch_sub(1, Ordering::Relaxed);
                    }
                    // Must precede `page_store.free`: once the pid can
                    // be recycled, a lingering overlay entry would
                    // shadow whatever the new owner writes there.
                    // Unconditional (no ptr_eq): the page_table entry
                    // is gone, so no read can resolve this pid anyway.
                    inner.staged_overlay.remove(&staged_page.page_id);
                }
                self.page_cache.invalidate(staged_page.page_id);
                if let Err(err) = self.page_store.free(staged_page.page_id, free_lsn) {
                    tracing::warn!(
                        page_id = staged_page.page_id,
                        error = %err,
                        "abort_staged_deltas: failed to free fresh page id"
                    );
                }
            } else {
                // Pre-existing page: drop the overlay entry (ptr_eq
                // gated — never clobber a newer fold's re-staging) and
                // invalidate the LRU so subsequent reads fall through
                // to disk truth (which still reflects the pre-stage
                // content, since we never wrote this page).
                {
                    let mut inner = self.inner.lock();
                    if inner
                        .staged_overlay
                        .get(&staged_page.page_id)
                        .is_some_and(|cur| Arc::ptr_eq(cur, &staged_page.sealed))
                    {
                        inner.staged_overlay.remove(&staged_page.page_id);
                    }
                }
                self.page_cache.invalidate(staged_page.page_id);
            }
        }
    }

    /// Streaming checkpoints no longer retain page payload Arcs after their
    /// write CQEs complete. Before the global meta-chain write starts, existing
    /// pages stay installed (their bytes already include the folded deltas) and
    /// unreachable fresh allocations can be rolled back from compact metadata.
    /// Once the stable meta head may have been overwritten, this is forbidden.
    pub(super) fn abort_streamed_fresh_pages(&self, pages: &[StagedPageMeta], free_lsn: Lsn) {
        for page in pages.iter().filter(|page| page.is_fresh) {
            {
                let mut inner = self.inner.lock();
                if page.page_idx < inner.page_table.len()
                    && inner.page_table[page.page_idx] == page.page_id
                {
                    inner.page_table[page.page_idx] = 0;
                    inner.meta_dirty = true;
                    self.allocated_data_pages.fetch_sub(1, Ordering::Relaxed);
                }
                inner.staged_overlay.remove(&page.page_id);
            }
            self.page_cache.invalidate(page.page_id);
            if let Err(err) = self.page_store.free(page.page_id, free_lsn) {
                tracing::warn!(
                    page_id = page.page_id,
                    error = %err,
                    "abort_streamed_fresh_pages: failed to free fresh page id"
                );
            }
        }
    }

    #[cfg(test)]
    pub(super) fn staged_overlay_len(&self) -> usize {
        self.inner.lock().staged_overlay.len()
    }

    /// Persist the meta chain if it has been mutated since the last
    /// flush. Returns `true` when a write actually happened.
    pub fn flush_meta(&self) -> Result<bool> {
        let mut inner = self.inner.lock();
        if !inner.meta_dirty {
            return Ok(false);
        }
        self.flush_meta_locked(&mut inner)?;
        inner.meta_dirty = false;
        Ok(true)
    }

    fn flush_meta_locked(&self, inner: &mut parking_lot::MutexGuard<'_, Inner>) -> Result<()> {
        // The free LSN passed to `paged_meta::write_chain` is used to
        // stamp the deferred-free entry on shrink. Use the highest
        // generation observed across the page table so the deferred
        // entry sorts after the last committed write.
        let free_lsn: Lsn = 0;
        let new_chain = paged_meta::write_chain(
            &self.page_store,
            &self.page_cache,
            PageType::RefcountArray,
            META_KEY_COUNT_MARKER,
            &[],
            &inner.page_table,
            &inner.meta_chain,
            free_lsn,
        )?;
        inner.meta_chain = new_chain;
        Ok(())
    }

    /// Iterate every (pba, RcEntry) where `rc > 0`. Order is
    /// PBA-ascending. Forces no flush — caller must ensure deltas are
    /// drained beforehand if they want a consistent view.
    pub fn iter_live(&self) -> Result<Vec<(Pba, RcEntry)>> {
        let inner = self.inner.lock();
        let page_ids: Vec<(usize, PageId, Option<Arc<Page>>)> = inner
            .page_table
            .iter()
            .enumerate()
            .filter_map(|(idx, &pid)| {
                if pid != 0 {
                    Some((idx, pid, inner.staged_overlay.get(&pid).cloned()))
                } else {
                    None
                }
            })
            .collect();
        drop(inner);
        let mut out = Vec::new();
        for (page_idx, page_id, staged) in page_ids {
            let page = match staged {
                Some(page) => page,
                None => self.page_cache.get(page_id)?,
            };
            for slot in 0..ENTRIES_PER_PAGE {
                let entry = read_entry(&page, slot);
                if entry.rc > 0 {
                    let pba = (page_idx * ENTRIES_PER_PAGE + slot) as Pba;
                    out.push((pba, entry));
                }
            }
        }
        Ok(out)
    }

    /// Number of data pages currently allocated (excludes the meta
    /// page and unwritten holes).
    pub fn allocated_data_pages(&self) -> usize {
        self.allocated_data_pages.load(Ordering::Relaxed)
    }
}

#[inline]
fn page_offset(pba: Pba) -> (usize, usize) {
    let pba = pba as usize;
    (pba / ENTRIES_PER_PAGE, pba % ENTRIES_PER_PAGE)
}

#[inline]
fn read_entry(page: &Page, slot: usize) -> RcEntry {
    let payload = page.payload();
    let off = slot * ENTRY_BYTES;
    let rc = u32::from_le_bytes(payload[off..off + 4].try_into().unwrap());
    let birth_lsn = u64::from_le_bytes(payload[off + 4..off + 12].try_into().unwrap());
    RcEntry { rc, birth_lsn }
}

fn decode_page_run(
    page: &Page,
    pbas: &[Pba],
    start: usize,
    end: usize,
    out: &mut [(RcEntry, Lsn)],
) -> Result<()> {
    let generation = page.header()?.generation;
    for idx in start..end {
        let (_, slot) = page_offset(pbas[idx]);
        out[idx] = (read_entry(page, slot), generation);
    }
    Ok(())
}

#[inline]
fn write_entry(page: &mut Page, slot: usize, entry: RcEntry) {
    let payload = page.payload_mut();
    let off = slot * ENTRY_BYTES;
    payload[off..off + 4].copy_from_slice(&entry.rc.to_le_bytes());
    payload[off + 4..off + 12].copy_from_slice(&entry.birth_lsn.to_le_bytes());
}

fn new_empty_data_page() -> Page {
    Page::new(PageHeader {
        page_type: PageType::RefcountArray,
        version: crate::page::PAGE_VERSION,
        key_count: DATA_KEY_COUNT_MARKER,
        flags: 0,
        generation: 0,
        // Non-L2P page: birth is meaningless here. The old header rc field is
        // gone; birth_lsn is carried only as an incidental value, never read for
        // this type.
        birth_lsn: 0,
    })
}

#[cfg(test)]
mod tests;
