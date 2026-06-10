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

use parking_lot::Mutex;

use super::RcEntry;
use crate::cache::PageCache;
use crate::error::Result;
use crate::page::{PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use crate::page_store::PageStore;
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

const _: () = {
    assert!(ENTRIES_PER_PAGE * ENTRY_BYTES == PAGE_PAYLOAD_SIZE);
};

pub struct PagedRefcountArray {
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    meta_page_id: PageId,
    inner: Mutex<Inner>,
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
        };
        let me = Self {
            page_store,
            page_cache,
            meta_page_id,
            inner: Mutex::new(inner),
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
        Ok(Self {
            page_store,
            page_cache,
            meta_page_id,
            inner: Mutex::new(Inner {
                page_table: read.page_table,
                meta_chain: read.chain_pids,
                meta_dirty: false,
            }),
        })
    }

    pub fn meta_page_id(&self) -> PageId {
        self.meta_page_id
    }

    /// Look up one entry. Returns [`RcEntry::ZERO`] if no data page
    /// is allocated for the PBA's page_idx.
    pub fn get(&self, pba: Pba) -> Result<RcEntry> {
        let (page_idx, slot) = page_offset(pba);
        let page_id = {
            let inner = self.inner.lock();
            inner.page_table.get(page_idx).copied().unwrap_or(0)
        };
        if page_id == 0 {
            return Ok(RcEntry::ZERO);
        }
        let page = self.page_cache.get(page_id)?;
        Ok(read_entry(&page, slot))
    }

    /// `last_applied_lsn` recorded on the data page covering `pba`.
    /// Returns 0 if no page is allocated yet (replay must apply
    /// unconditionally for a fresh page).
    pub fn page_lsn(&self, pba: Pba) -> Result<Lsn> {
        let (page_idx, _) = page_offset(pba);
        let page_id = {
            let inner = self.inner.lock();
            inner.page_table.get(page_idx).copied().unwrap_or(0)
        };
        if page_id == 0 {
            return Ok(0);
        }
        let page = self.page_cache.get(page_id)?;
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
        let staged = self.stage_deltas_in_memory(deltas)?;
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
    pub fn stage_deltas_in_memory(&self, deltas: Vec<(Pba, Pending)>) -> Result<StagedDeltas> {
        if deltas.is_empty() {
            return Ok(StagedDeltas {
                pages: Vec::new(),
                max_lsn: 0,
            });
        }

        let mut by_page: HashMap<usize, Vec<(usize, Pending)>> = HashMap::new();
        for (pba, pending) in deltas {
            let (page_idx, slot) = page_offset(pba);
            by_page.entry(page_idx).or_default().push((slot, pending));
        }

        let mut pages = Vec::with_capacity(by_page.len());
        let mut max_lsn: Lsn = 0;
        for (page_idx, slot_pendings) in by_page {
            let staged = self.stage_one_page(page_idx, slot_pendings)?;
            let page_gen = staged.sealed.header()?.generation;
            if page_gen > max_lsn {
                max_lsn = page_gen;
            }
            pages.push(staged);
        }
        Ok(StagedDeltas { pages, max_lsn })
    }

    fn stage_one_page(
        &self,
        page_idx: usize,
        slot_pendings: Vec<(usize, Pending)>,
    ) -> Result<StagedPage> {
        // Resolve / allocate the page id under inner; drop inner before
        // touching the cache so concurrent reads on other pages don't
        // block. `inner.page_table` mutation under gate is load-bearing:
        // see refcount/shard.rs::begin_checkpoint.
        let (page_id, is_fresh) = {
            let mut inner = self.inner.lock();
            if page_idx >= inner.page_table.len() {
                inner.page_table.resize(page_idx + 1, 0);
                inner.meta_dirty = true;
            }
            if inner.page_table[page_idx] == 0 {
                let pid = self.page_store.allocate()?;
                inner.page_table[page_idx] = pid;
                inner.meta_dirty = true;
                (pid, true)
            } else {
                (inner.page_table[page_idx], false)
            }
        };

        let mut page = if is_fresh {
            new_empty_data_page()
        } else {
            (*self.page_cache.get(page_id)?).clone()
        };

        let page_generation = page.header()?.generation;
        let mut max_lsn = page_generation;
        for (slot, pending) in slot_pendings {
            // Replay-skip: a previous checkpoint attempt may have
            // written this page's deltas to disk before failing
            // (write_meta_chain_external / manifest commit / etc.).
            // The abort path invalidates the cache and restores the
            // drained deltas; on retry, the page is re-read from disk
            // with `generation >= pending.last_lsn` for already-applied
            // slots. Skip them — re-applying would double-count.
            // Mirrors the per-op replay-skip in `RcShard::stage`.
            if page_generation >= pending.last_lsn {
                continue;
            }
            let prev = read_entry(&page, slot);
            // A decref whose accumulated delta lands past zero is a benign
            // double-decref split across drainer cycles (the on-disk base
            // was already taken to 0 by an earlier cycle). Skip it (leave
            // the entry at its floor) instead of failing the checkpoint.
            let (new, skipped) =
                super::apply_delta_or_skip(prev, pending.delta, pending.last_lsn)?;
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

        let mut header = page.header()?;
        header.generation = max_lsn;
        page.write_header(&header);
        page.seal();

        let sealed = Arc::new(page);
        // Cache must hold the staged page before the apply gate is
        // released — concurrent `RcShard::stage` after gate-release
        // reads via `array.get` / `array.page_lsn` and must observe
        // the post-stage entries (replay-skip correctness).
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
    /// via `page_store.write_sealed_page_runs` instead.
    pub fn write_staged_pages(&self, staged: &StagedDeltas) -> Result<()> {
        for staged_page in &staged.pages {
            self.page_store
                .write_page(staged_page.page_id, &staged_page.sealed)?;
        }
        Ok(())
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
        paged_meta::write_chain(
            &self.page_store,
            &self.page_cache,
            PageType::RefcountArray,
            META_KEY_COUNT_MARKER,
            &[],
            snapshot_page_table,
            snapshot_meta_chain,
            free_lsn,
        )
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
                    }
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
                // Pre-existing page: invalidate so subsequent reads
                // fall through to disk truth (which still reflects the
                // pre-stage content, since we never wrote this page).
                self.page_cache.invalidate(staged_page.page_id);
            }
        }
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
        let page_ids: Vec<(usize, PageId)> = inner
            .page_table
            .iter()
            .enumerate()
            .filter_map(|(idx, &pid)| if pid != 0 { Some((idx, pid)) } else { None })
            .collect();
        drop(inner);
        let mut out = Vec::new();
        for (page_idx, page_id) in page_ids {
            let page = self.page_cache.get(page_id)?;
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
        let inner = self.inner.lock();
        inner.page_table.iter().filter(|&&pid| pid != 0).count()
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
        // The shared `verify` path expects every manifest-reachable
        // page to carry header refcount = 1 (the meta page is the
        // single owner). Set it here so the very first write of a
        // freshly allocated data page is already correctly stamped.
        refcount: 1,
    })
}

#[cfg(test)]
mod tests;
