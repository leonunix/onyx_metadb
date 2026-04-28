//! File-backed page store.
//!
//! The page store is a flat file of 4 KiB pages, indexed by page id. The
//! offset of page `p` is simply `p * PAGE_SIZE` — no indirection, no
//! extent map.
//!
//! # Free list (v0)
//!
//! The in-memory free list is a `Vec<PageId>`, popped LIFO for cache
//! locality. On `create`, it starts empty. On `open`, we scan pages from
//! [`FIRST_DATA_PAGE`] to the file's high-water mark and collect every page
//! whose header decodes as [`PageType::Free`] or whose bytes are all zero.
//! The all-zero case lets filesystems represent freed pages as sparse holes.
//! A later phase will persist the free list in its own page chain to avoid
//! the scan for large databases; v0 keeps it simple and correct.
//!
//! # File extension (batched)
//!
//! The on-disk invariant is `file_size == committed_file_pages * PAGE_SIZE`
//! and `committed_file_pages >= high_water`. The single-page `allocate`
//! path no longer calls `set_len` once per page; instead it bumps an
//! in-memory `high_water`, and when that crosses the current committed
//! file size it rounds up to the next `grow_chunk_pages` boundary and
//! issues one `set_len`. The tail pages between `high_water` and
//! `committed_file_pages` are zero-init and carry no headers, so they
//! are recoverable as growth tail on crash (see `open`).
//!
//! # Concurrency
//!
//! `read_page` / `write_page` take a shared `&File` and issue positional IO
//! (`pread` / `pwrite`) — safe under concurrent callers since each call
//! is atomic at the kernel level. The mutex only protects metadata (free
//! list, high-water mark, committed file size).

use parking_lot::Mutex;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[cfg(target_os = "linux")]
use io_uring::{IoUring, opcode, types};

use crate::config::PAGE_SIZE;
use crate::epoch::EpochManager;
use crate::error::{MetaDbError, Result};
use crate::page::{Page, PageHeader, PageType};
use crate::types::{FIRST_DATA_PAGE, Lsn, PageId};

const RC_LOCK_SHARDS: usize = 64;
const MAX_RECLAIM_RUN_PAGES: usize = 256;
#[cfg(target_os = "linux")]
const DEFAULT_READ_URING_ENTRIES: u32 = 128;
#[cfg(target_os = "linux")]
const DEFAULT_WRITE_URING_ENTRIES: u32 = 128;

/// Default pre-extension chunk if the caller uses [`PageStore::create`]
/// / [`PageStore::open`] without threading a `Config`. Must stay in
/// sync with `Config::page_grow_chunk_pages`'s documented default.
pub const DEFAULT_GROW_CHUNK_PAGES: u64 = 512;

/// Flat page file.
pub struct PageStore {
    path: PathBuf,
    file: File,
    #[cfg(target_os = "linux")]
    read_uring: Mutex<Option<IoUring>>,
    #[cfg(target_os = "linux")]
    write_uring: Mutex<Option<IoUring>>,
    inner: Mutex<Inner>,
    /// Per-pid sharded mutexes serialising [`atomic_rc_delta`]. Needed
    /// because a page can be shared across multiple [`PagedL2p`]
    /// instances after `clone_volume` — two trees each holding their
    /// own `PageBuf` would otherwise each read the same pre-decrement
    /// rc and race their writes back to disk (last-writer-wins losing
    /// one decrement).
    rc_locks: Box<[Mutex<()>]>,
    /// Batch size for pre-extending the backing file in `allocate` /
    /// `allocate_run`. Frozen at construction; never mutated.
    grow_chunk: u64,
    /// Epoch coordinator shared with lock-free L2P readers. Reader
    /// `pin()` records its starting epoch in a slot; [`free`] /
    /// [`free_idempotent`] tag deferred work with the pre-bump epoch
    /// and `try_reclaim` only physically frees pids whose tag is below
    /// every active pin. See [`crate::epoch`] for the safety proof.
    epoch: Arc<EpochManager>,
    /// Pending physical frees, keyed by pid (HashMap dedups so the
    /// idempotent / replay path cannot push the same pid twice).
    deferred_free: Mutex<HashMap<PageId, DeferredFree>>,
}

#[derive(Clone, Copy, Debug)]
struct DeferredFree {
    epoch: u64,
    generation: Lsn,
    /// `true` if the entry came from [`free_idempotent`] — i.e. WAL
    /// replay path. Reclaim re-checks the on-disk page type before
    /// pushing onto the free list so a crash mid-reclaim cannot leave
    /// a duplicate free-list entry on the next replay.
    idempotent: bool,
}

fn new_rc_locks() -> Box<[Mutex<()>]> {
    (0..RC_LOCK_SHARDS)
        .map(|_| Mutex::new(()))
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

impl std::fmt::Debug for PageStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock();
        f.debug_struct("PageStore")
            .field("path", &self.path)
            .field("high_water", &inner.high_water)
            .field("free_list_len", &inner.free_list.len())
            .finish()
    }
}

struct Inner {
    /// Smallest page id that has *not* yet been allocated. Always
    /// `<= committed_file_pages`; the gap between them is pre-extended
    /// zero-init growth tail.
    high_water: u64,
    /// File length in pages. `file.metadata().len() == committed_file_pages
    /// * PAGE_SIZE` at all times outside `allocate` / `allocate_run` /
    /// `open`, and `committed_file_pages >= high_water` always.
    committed_file_pages: u64,
    /// Explicitly-freed pages available for reuse. LIFO.
    free_list: Vec<PageId>,
}

impl PageStore {
    /// Create a brand-new page store at `path` with the default batch
    /// grow chunk ([`DEFAULT_GROW_CHUNK_PAGES`]). Fails if the file
    /// already exists.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Self::create_with_grow_chunk(path, DEFAULT_GROW_CHUNK_PAGES)
    }

    /// Open an existing page store with the default batch grow chunk
    /// ([`DEFAULT_GROW_CHUNK_PAGES`]).
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_grow_chunk(path, DEFAULT_GROW_CHUNK_PAGES)
    }

    /// Create a brand-new page store at `path`. `grow_chunk` sets how
    /// many pages are pre-reserved on each file extension; see module
    /// docs. Must be `>= 1`. Fails if the file already exists.
    pub fn create_with_grow_chunk(path: impl AsRef<Path>, grow_chunk: u64) -> Result<Self> {
        if grow_chunk == 0 {
            return Err(MetaDbError::InvalidArgument(
                "page store grow_chunk must be >= 1".into(),
            ));
        }
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        // Pre-size to FIRST_DATA_PAGE so the manifest slot offsets are
        // immediately addressable, even though we leave those pages zeroed
        // (the manifest layer will populate them). First data allocation
        // will pre-extend to FIRST_DATA_PAGE + grow_chunk.
        file.set_len(FIRST_DATA_PAGE * PAGE_SIZE as u64)?;
        Ok(Self {
            path,
            file,
            #[cfg(target_os = "linux")]
            read_uring: Mutex::new(new_read_uring()),
            #[cfg(target_os = "linux")]
            write_uring: Mutex::new(new_write_uring()),
            inner: Mutex::new(Inner {
                high_water: FIRST_DATA_PAGE,
                committed_file_pages: FIRST_DATA_PAGE,
                free_list: Vec::new(),
            }),
            rc_locks: new_rc_locks(),
            grow_chunk,
            epoch: Arc::new(EpochManager::new()),
            deferred_free: Mutex::new(HashMap::new()),
        })
    }

    /// Open an existing page store. `grow_chunk` is the batch size used
    /// for subsequent file extensions (does not affect the scan). The
    /// scan rebuilds the in-memory free list by walking pages from
    /// [`FIRST_DATA_PAGE`] to EOF; any contiguous zero-init tail left
    /// over from a crashed pre-extend is truncated back before the
    /// store is returned.
    pub fn open_with_grow_chunk(path: impl AsRef<Path>, grow_chunk: u64) -> Result<Self> {
        if grow_chunk == 0 {
            return Err(MetaDbError::InvalidArgument(
                "page store grow_chunk must be >= 1".into(),
            ));
        }
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        let size = file.metadata()?.len();
        if size % PAGE_SIZE as u64 != 0 {
            return Err(MetaDbError::Corruption(format!(
                "page file size {size} is not a multiple of page size {PAGE_SIZE}",
            )));
        }
        if size < FIRST_DATA_PAGE * PAGE_SIZE as u64 {
            return Err(MetaDbError::Corruption(format!(
                "page file size {size} is shorter than the reserved manifest region",
            )));
        }
        let file_end_pages = size / PAGE_SIZE as u64;
        // Walk every page in [FIRST_DATA_PAGE, file_end_pages). Typed pages
        // extend the recovered `high_water`; Free pages and all-zero punched
        // holes are reusable. A zero suffix past the last typed page is
        // growth tail and is truncated below.
        let mut high_water = FIRST_DATA_PAGE;
        let mut free_list = Vec::new();
        for page_id in FIRST_DATA_PAGE..file_end_pages {
            let page = read_page_raw(&file, page_id)?;
            if let Ok(h) = page.header() {
                high_water = page_id + 1;
                if h.page_type == PageType::Free {
                    free_list.push(page_id);
                }
            } else if is_zero_page(&page) {
                free_list.push(page_id);
            }
            // Torn pages are left in place below high_water; the verifier
            // flags them later.
        }
        if high_water < file_end_pages {
            file.set_len(high_water * PAGE_SIZE as u64)?;
        }
        free_list.retain(|pid| *pid < high_water);
        Ok(Self {
            path,
            file,
            #[cfg(target_os = "linux")]
            read_uring: Mutex::new(new_read_uring()),
            #[cfg(target_os = "linux")]
            write_uring: Mutex::new(new_write_uring()),
            inner: Mutex::new(Inner {
                high_water,
                committed_file_pages: high_water,
                free_list,
            }),
            rc_locks: new_rc_locks(),
            grow_chunk,
            epoch: Arc::new(EpochManager::new()),
            deferred_free: Mutex::new(HashMap::new()),
        })
    }

    /// Path the store was opened from.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Shared epoch coordinator. Lock-free L2P readers `pin()` here
    /// before walking; deferred-free reclaim respects every active pin.
    pub fn epoch(&self) -> &Arc<EpochManager> {
        &self.epoch
    }

    /// Number of pages currently waiting for an epoch-safe reclaim.
    /// Useful for tests and the metrics layer; do not gate behaviour on
    /// this — production callers use [`try_reclaim`] which atomically
    /// drains.
    pub fn deferred_free_len(&self) -> usize {
        self.deferred_free.lock().len()
    }

    /// Next page id that will be handed out by `allocate` if the free list
    /// is empty. Also equals the file's length in pages.
    pub fn high_water(&self) -> u64 {
        self.inner.lock().high_water
    }

    /// Number of pages currently on the free list.
    pub fn free_list_len(&self) -> usize {
        self.inner.lock().free_list.len()
    }

    /// Read page `page_id`. Performs full integrity verification before
    /// returning.
    pub fn read_page(&self, page_id: PageId) -> Result<Page> {
        self.check_in_range(page_id)?;
        let page = read_page_raw(&self.file, page_id)?;
        page.verify(page_id)?;
        Ok(page)
    }

    /// Read and verify several pages. On Linux this uses one io_uring submit
    /// per chunk, so callers with many cache misses can raise device queue
    /// depth instead of serialising `pread` calls.
    pub(crate) fn read_pages(&self, page_ids: &[PageId]) -> Result<Vec<Page>> {
        if page_ids.is_empty() {
            return Ok(Vec::new());
        }
        for &page_id in page_ids {
            self.check_in_range(page_id)?;
        }
        let pages = read_pages_raw(&self.file, page_ids, self.read_uring())?;
        for (page_id, page) in page_ids.iter().copied().zip(&pages) {
            page.verify(page_id)?;
        }
        Ok(pages)
    }

    /// Read page `page_id` without running `verify`. Used by recovery and
    /// verifier tooling that want to inspect potentially-bad pages without
    /// erroring out.
    pub fn read_page_unchecked(&self, page_id: PageId) -> Result<Page> {
        self.check_in_range(page_id)?;
        read_page_raw(&self.file, page_id)
    }

    /// Write `page` at `page_id`. The caller is responsible for having
    /// called [`Page::seal`] first; `write_page` does not reseal.
    pub fn write_page(&self, page_id: PageId, page: &Page) -> Result<()> {
        self.check_in_range(page_id)?;
        self.file
            .write_all_at(page.bytes(), page_id * PAGE_SIZE as u64)?;
        Ok(())
    }

    /// Write a contiguous run of already-sealed page bytes starting at
    /// `start_page`. `bytes.len()` must be a non-zero multiple of
    /// [`PAGE_SIZE`].
    pub fn write_page_run_bytes(&self, start_page: PageId, bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() || bytes.len() % PAGE_SIZE != 0 {
            return Err(MetaDbError::InvalidArgument(format!(
                "page run write requires a non-empty multiple of {PAGE_SIZE} bytes, got {}",
                bytes.len()
            )));
        }
        let pages = (bytes.len() / PAGE_SIZE) as u64;
        let last = start_page
            .checked_add(pages - 1)
            .ok_or(MetaDbError::OutOfSpace)?;
        self.check_in_range(last)?;
        self.file
            .write_all_at(bytes, start_page * PAGE_SIZE as u64)?;
        Ok(())
    }

    /// Write multiple already-sealed page runs, keeping the final
    /// durability boundary at the caller's later [`sync`](Self::sync).
    pub fn write_page_runs_parallel(&self, runs: Vec<(PageId, Vec<u8>)>) -> Result<()> {
        write_page_runs_raw(&self.file, runs, self.write_uring())
    }

    pub fn write_sealed_page_runs(&self, mut pages: Vec<(PageId, Arc<Page>)>) -> Result<()> {
        if pages.is_empty() {
            return Ok(());
        }
        pages.sort_unstable_by_key(|(pid, _)| *pid);
        let mut runs = Vec::new();
        let mut run_start: Option<PageId> = None;
        let mut run_next = 0;
        let mut run_bytes = Vec::with_capacity(MAX_RECLAIM_RUN_PAGES * PAGE_SIZE);
        for (pid, page) in pages {
            let run_pages = run_bytes.len() / PAGE_SIZE;
            if run_start.is_some() && (pid != run_next || run_pages >= MAX_RECLAIM_RUN_PAGES) {
                runs.push((run_start.unwrap(), std::mem::take(&mut run_bytes)));
                run_bytes.reserve(MAX_RECLAIM_RUN_PAGES * PAGE_SIZE);
                run_start = None;
            }
            if run_start.is_none() {
                run_start = Some(pid);
            }
            run_bytes.extend_from_slice(page.bytes());
            run_next = pid + 1;
        }
        if let Some(start) = run_start {
            runs.push((start, run_bytes));
        }
        self.write_page_runs_parallel(runs)
    }

    /// Atomically mutate the refcount of `page_id` by `delta` (positive
    /// for incref, negative for decref). Returns the post-delta rc.
    ///
    /// Bypasses [`PageCache`] and [`PageBuf`]: reads the authoritative
    /// on-disk version inside a per-pid sharded mutex, mutates, writes
    /// back. Used by [`crate::paged::PageBuf::cow_for_write`] when the
    /// page is shared across multiple tree instances (post-`clone_volume`).
    /// Without this, two trees each holding a Clean copy would race:
    /// both read the same pre-decrement rc, both write rc-1 via flush,
    /// losing one decrement.
    ///
    /// Leaves `page.generation` unchanged — that field is reserved for
    /// WAL-apply idempotency markers and must not regress.
    ///
    /// The caller is responsible for invalidating any cached copies of
    /// `page_id` in `PageCache` / `PageBuf` after this call so a
    /// subsequent read observes the new rc.
    pub fn atomic_rc_delta(&self, page_id: PageId, delta: i32) -> Result<u32> {
        self.check_in_range(page_id)?;
        let shard = (page_id as usize) % RC_LOCK_SHARDS;
        let _guard = self.rc_locks[shard].lock();
        let mut page = read_page_raw(&self.file, page_id)?;
        page.verify(page_id)?;
        let cur = page.refcount();
        let new_rc = if delta >= 0 {
            cur.checked_add(delta as u32)
        } else {
            cur.checked_sub((-delta) as u32)
        }
        .ok_or_else(|| {
            MetaDbError::Corruption(format!(
                "atomic_rc_delta: page {page_id} refcount {cur} + {delta} out of range"
            ))
        })?;
        page.set_refcount(new_rc);
        page.seal();
        self.file
            .write_all_at(page.bytes(), page_id * PAGE_SIZE as u64)?;
        Ok(new_rc)
    }

    /// Same as [`atomic_rc_delta`] but with WAL-replay idempotency. The
    /// `(lsn, ordinal)` pair identifies one rc-delta application within
    /// a WAL record. If the page already carries a later marker, this
    /// delta is treated as already applied and skipped. On successful
    /// apply the page is stamped with `(lsn, ordinal)`.
    ///
    /// Used by [`crate::paged::PageBuf::cow_for_write`] so that a WAL
    /// op replayed after crash does not double-apply an already landed
    /// delta. A single WAL record can contain multiple L2P ops with the
    /// same LSN, so comparing only `generation >= lsn` is insufficient:
    /// distinct same-LSN deltas on the same page must not swallow each
    /// other. `ordinal` disambiguates those same-record applications.
    ///
    /// `lsn` must be strictly greater than zero — tree pages
    /// carry `generation = 0` for their entire unsnapped lifetime, so
    /// `lsn = 0` would spuriously skip on every call. The function
    /// rejects that case.
    ///
    /// The caller is responsible for invalidating any cached copies
    /// of `page_id` in `PageCache` / `PageBuf` after this call so a
    /// subsequent read observes the new rc + generation.
    pub fn atomic_rc_delta_with_gen(
        &self,
        page_id: PageId,
        delta: i32,
        lsn: Lsn,
        ordinal: u32,
    ) -> Result<u32> {
        if lsn == 0 {
            return Err(MetaDbError::InvalidArgument(
                "atomic_rc_delta_with_gen: lsn must be > 0".into(),
            ));
        }
        self.check_in_range(page_id)?;
        let shard = (page_id as usize) % RC_LOCK_SHARDS;
        let _guard = self.rc_locks[shard].lock();
        let mut page = read_page_raw(&self.file, page_id)?;
        page.verify(page_id)?;
        let cur_gen = page.generation();
        let cur_ordinal = page.flags();
        let cur_rc = page.refcount();
        if cur_gen > lsn || (cur_gen == lsn && cur_ordinal >= ordinal) {
            return Ok(cur_rc);
        }
        let new_rc = if delta >= 0 {
            cur_rc.checked_add(delta as u32)
        } else {
            cur_rc.checked_sub((-delta) as u32)
        }
        .ok_or_else(|| {
            MetaDbError::Corruption(format!(
                "atomic_rc_delta_with_gen: page {page_id} refcount {cur_rc} + {delta} out of range"
            ))
        })?;
        page.set_refcount(new_rc);
        page.set_generation(lsn);
        page.set_flags(ordinal);
        page.seal();
        self.file
            .write_all_at(page.bytes(), page_id * PAGE_SIZE as u64)?;
        Ok(new_rc)
    }

    /// Allocate a fresh page id. If the free list has entries, one is
    /// popped and returned; otherwise `high_water` advances by one and
    /// the file is pre-extended in `grow_chunk` units so most calls
    /// avoid a `set_len` syscall. The on-disk content is not
    /// initialized — the caller is expected to write a sealed page at
    /// the returned id.
    pub fn allocate(&self) -> Result<PageId> {
        let mut inner = self.inner.lock();
        if let Some(page_id) = inner.free_list.pop() {
            return Ok(page_id);
        }
        let page_id = inner.high_water;
        let new_high = inner
            .high_water
            .checked_add(1)
            .ok_or(MetaDbError::OutOfSpace)?;
        self.ensure_file_covers(&mut inner, new_high)?;
        inner.high_water = new_high;
        Ok(page_id)
    }

    /// Allocate a contiguous run of `count` fresh page ids.
    ///
    /// Always extends the high-water mark; the free list is never
    /// consulted, because a LIFO pool of individual pages does not
    /// efficiently produce contiguous runs. The returned page id is the
    /// first page of the run. On-disk content is uninitialized; the
    /// caller is expected to write a sealed page at each id.
    pub fn allocate_run(&self, count: usize) -> Result<PageId> {
        if count == 0 {
            return Err(MetaDbError::InvalidArgument(
                "allocate_run requires count > 0".into(),
            ));
        }
        let count = u64::try_from(count)
            .map_err(|_| MetaDbError::InvalidArgument("page run too large".into()))?;
        let mut inner = self.inner.lock();
        let start = inner.high_water;
        let new_high = inner
            .high_water
            .checked_add(count)
            .ok_or(MetaDbError::OutOfSpace)?;
        self.ensure_file_covers(&mut inner, new_high)?;
        inner.high_water = new_high;
        Ok(start)
    }

    /// Ensure the backing file covers at least `target` pages. Rounds
    /// up to the next `grow_chunk` boundary so subsequent allocations
    /// within the chunk avoid `set_len`. Called with `inner` already
    /// locked.
    fn ensure_file_covers(&self, inner: &mut Inner, target: u64) -> Result<()> {
        if target <= inner.committed_file_pages {
            return Ok(());
        }
        // Round target up to the next grow_chunk boundary.
        let chunk = self.grow_chunk;
        let span = target
            .checked_sub(inner.committed_file_pages)
            .expect("target > committed by the early return above");
        let chunks_needed = span.div_ceil(chunk);
        let add = chunks_needed
            .checked_mul(chunk)
            .ok_or(MetaDbError::OutOfSpace)?;
        let new_committed = inner
            .committed_file_pages
            .checked_add(add)
            .ok_or(MetaDbError::OutOfSpace)?;
        self.file.set_len(
            new_committed
                .checked_mul(PAGE_SIZE as u64)
                .ok_or(MetaDbError::OutOfSpace)?,
        )?;
        inner.committed_file_pages = new_committed;
        Ok(())
    }

    /// Free `count` pages starting at `start`, stamping each with
    /// `generation`. Pages rejoin the single-page free list individually.
    /// Convenience wrapper over [`free`]; fails as soon as any page id
    /// falls outside the allowed range.
    pub fn free_run(&self, start: PageId, count: u32, generation: Lsn) -> Result<()> {
        let page_ids: Vec<PageId> = (start..start + count as u64).collect();
        self.free_many(&page_ids, generation)
    }

    /// Mark `page_id` as free. The physical Free-stamp + hole-punch +
    /// free-list push is **deferred** until [`try_reclaim`] runs and
    /// observes that no live reader could still walk the page (see
    /// [`crate::epoch`] for the safety proof). The on-disk bytes stay
    /// the page's old (still-valid) content during the deferred window,
    /// so a stale L2P reader that falls through page-cache to disk
    /// keeps decoding correctly. `generation` is recorded with the
    /// deferred entry and stamped onto the Free page at reclaim time.
    ///
    /// Refuses to free reserved pages (manifest slots) or pages outside
    /// the current high-water range.
    pub fn free(&self, page_id: PageId, generation: Lsn) -> Result<()> {
        self.free_many(&[page_id], generation)
    }

    /// Batch form of [`free`]. All pages are tagged with one epoch and
    /// inserted under one deferred-free lock acquisition; this keeps
    /// checkpoint install from paying per-page lock/epoch overhead when a
    /// dirty shard retires tens of thousands of pages at once.
    pub fn free_many(&self, page_ids: &[PageId], generation: Lsn) -> Result<()> {
        if page_ids.is_empty() {
            return Ok(());
        }
        {
            let inner = self.inner.lock();
            for &page_id in page_ids {
                if page_id < FIRST_DATA_PAGE {
                    return Err(MetaDbError::InvalidArgument(format!(
                        "page {page_id} is reserved (manifest slot)",
                    )));
                }
                if page_id >= inner.high_water {
                    return Err(MetaDbError::PageOutOfRange(page_id));
                }
            }
        }

        let mut sorted = page_ids.to_vec();
        sorted.sort_unstable();
        if let Some(duplicate) = sorted.windows(2).find_map(|pair| {
            if pair[0] == pair[1] {
                Some(pair[0])
            } else {
                None
            }
        }) {
            return Err(MetaDbError::Corruption(format!(
                "page_store: duplicate free of page {duplicate} in one batch",
            )));
        }

        // Tag with the pre-bump epoch and bump global so any reader
        // pinning after this call observes G_pin > tag.
        let tag = self.epoch.advance();
        let mut deferred = self.deferred_free.lock();
        for &page_id in &sorted {
            if deferred.contains_key(&page_id) {
                return Err(MetaDbError::Corruption(format!(
                    "page_store: double free of page {page_id} (already pending reclaim)",
                )));
            }
        }
        for page_id in sorted {
            deferred.insert(
                page_id,
                DeferredFree {
                    epoch: tag,
                    generation,
                    idempotent: false,
                },
            );
        }
        Ok(())
    }

    /// Idempotent version of [`free`]. If `page_id` is already pending
    /// reclaim, or is already on disk as a `Free` / zero page, no work
    /// is queued and `Ok(false)` is returned. Otherwise the deferred
    /// entry is recorded and `Ok(true)` is returned.
    ///
    /// Used by WAL-replay paths (e.g. `DropSnapshot`) that may re-run
    /// against pages a crashed predecessor already freed. Cross-process
    /// correctness comes from [`open`] rebuilding `free_list` by scanning
    /// Free-typed pages, so each Free page ends up on the list exactly
    /// once regardless of how many times this was called before the
    /// crash.
    pub fn free_idempotent(&self, page_id: PageId, generation: Lsn) -> Result<bool> {
        if page_id < FIRST_DATA_PAGE {
            return Err(MetaDbError::InvalidArgument(format!(
                "page {page_id} is reserved (manifest slot)",
            )));
        }
        self.check_in_range(page_id)?;
        // Disk Free / zero check: a crash + replay path may already have
        // physically freed this pid in an earlier attempt.
        if let Ok(existing) = read_page_raw(&self.file, page_id) {
            if is_zero_page(&existing) {
                return Ok(false);
            }
            if let Ok(h) = existing.header() {
                if h.page_type == PageType::Free {
                    return Ok(false);
                }
            }
        }
        let tag = self.epoch.advance();
        let mut deferred = self.deferred_free.lock();
        if deferred.contains_key(&page_id) {
            return Ok(false);
        }
        deferred.insert(
            page_id,
            DeferredFree {
                epoch: tag,
                generation,
                idempotent: true,
            },
        );
        Ok(true)
    }

    /// Drain every deferred-free entry whose tag is below the smallest
    /// active reader pin, physically free those pids (Free-stamp +
    /// hole-punch + free-list push), and return the list of reclaimed
    /// pids so the caller can invalidate any stale page-cache entries.
    ///
    /// Idempotent and lock-free relative to readers: callers that hold
    /// no apply-side guard may invoke this from a background sweeper.
    pub fn try_reclaim(&self) -> Result<Vec<PageId>> {
        self.try_reclaim_limit(usize::MAX)
    }

    /// Budgeted variant of [`try_reclaim`]. Reclaims at most `max_pages`
    /// safe entries so latency-sensitive callers can make progress
    /// without turning one checkpoint into an unbounded free storm.
    pub fn try_reclaim_limit(&self, max_pages: usize) -> Result<Vec<PageId>> {
        if max_pages == 0 {
            return Ok(Vec::new());
        }
        let safe_below = self.epoch.min_active_pin();
        let mut deferred = self.deferred_free.lock();
        if deferred.is_empty() {
            return Ok(Vec::new());
        }
        let mut to_reclaim: Vec<(PageId, DeferredFree)> = Vec::new();
        deferred.retain(|pid, entry| {
            if entry.epoch < safe_below && to_reclaim.len() < max_pages {
                to_reclaim.push((*pid, *entry));
                false
            } else {
                true
            }
        });
        drop(deferred);

        to_reclaim.sort_unstable_by_key(|(pid, _)| *pid);
        let mut reclaimable = Vec::with_capacity(to_reclaim.len());
        for (pid, entry) in to_reclaim {
            if entry.idempotent && self.is_already_free_on_disk(pid)? {
                continue;
            }
            reclaimable.push((pid, entry));
        }
        let reclaimed = self.reclaim_sorted_runs(&reclaimable)?;
        Ok(reclaimed)
    }

    fn is_already_free_on_disk(&self, page_id: PageId) -> Result<bool> {
        let existing = read_page_raw(&self.file, page_id)?;
        if is_zero_page(&existing) {
            return Ok(true);
        }
        if let Ok(h) = existing.header() {
            if h.page_type == PageType::Free {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn reclaim_sorted_runs(&self, pages: &[(PageId, DeferredFree)]) -> Result<Vec<PageId>> {
        let mut reclaimed = Vec::with_capacity(pages.len());
        let mut idx = 0;
        while idx < pages.len() {
            let start = pages[idx].0;
            let mut end = idx + 1;
            while end < pages.len()
                && pages[end].0 == pages[end - 1].0 + 1
                && end - idx < MAX_RECLAIM_RUN_PAGES
            {
                end += 1;
            }

            let mut bytes = Vec::with_capacity((end - idx) * PAGE_SIZE);
            for (_, entry) in &pages[idx..end] {
                let mut page = Page::new(PageHeader::new(PageType::Free, entry.generation));
                page.set_refcount(0);
                page.seal();
                bytes.extend_from_slice(page.bytes());
            }
            self.write_page_run_bytes(start, &bytes)?;
            self.punch_free_run(start, end - idx)?;
            reclaimed.extend(pages[idx..end].iter().map(|(pid, _)| *pid));
            idx = end;
        }

        let mut inner = self.inner.lock();
        inner.free_list.extend(reclaimed.iter().copied());
        Ok(reclaimed)
    }

    /// `fdatasync` the page file (content only).
    pub fn sync(&self) -> Result<()> {
        sync_data_raw(&self.file, self.write_uring())
    }

    /// `fsync` the page file (content + metadata).
    pub fn sync_all(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    fn check_in_range(&self, page_id: PageId) -> Result<()> {
        let inner = self.inner.lock();
        if page_id >= inner.high_water {
            return Err(MetaDbError::PageOutOfRange(page_id));
        }
        Ok(())
    }

    fn punch_free_run(&self, start_page: PageId, page_count: usize) -> Result<()> {
        punch_hole(
            &self.file,
            start_page * PAGE_SIZE as u64,
            (page_count * PAGE_SIZE) as u64,
        )
    }

    #[cfg(target_os = "linux")]
    fn read_uring(&self) -> Option<&Mutex<Option<IoUring>>> {
        Some(&self.read_uring)
    }

    #[cfg(target_os = "linux")]
    fn write_uring(&self) -> Option<&Mutex<Option<IoUring>>> {
        Some(&self.write_uring)
    }

    #[cfg(not(target_os = "linux"))]
    fn read_uring(&self) -> Option<&()> {
        None
    }

    #[cfg(not(target_os = "linux"))]
    fn write_uring(&self) -> Option<&()> {
        None
    }
}

fn read_page_raw(file: &File, page_id: PageId) -> Result<Page> {
    let mut page = Page::zeroed();
    file.read_exact_at(page.bytes_mut(), page_id * PAGE_SIZE as u64)?;
    Ok(page)
}

#[cfg(target_os = "linux")]
fn new_read_uring() -> Option<IoUring> {
    match IoUring::new(DEFAULT_READ_URING_ENTRIES) {
        Ok(ring) => Some(ring),
        Err(err) => {
            tracing::debug!(error = %err, "page_store io_uring unavailable; falling back to pread");
            None
        }
    }
}

#[cfg(target_os = "linux")]
fn new_write_uring() -> Option<IoUring> {
    match IoUring::new(DEFAULT_WRITE_URING_ENTRIES) {
        Ok(ring) => Some(ring),
        Err(err) => {
            tracing::debug!(error = %err, "page_store write io_uring unavailable; falling back to pwrite");
            None
        }
    }
}

#[cfg(target_os = "linux")]
fn read_pages_raw(
    file: &File,
    page_ids: &[PageId],
    read_uring: Option<&Mutex<Option<IoUring>>>,
) -> Result<Vec<Page>> {
    if page_ids.len() == 1 {
        return Ok(vec![read_page_raw(file, page_ids[0])?]);
    }
    let Some(read_uring) = read_uring else {
        return read_pages_raw_pread(file, page_ids);
    };
    let mut guard = read_uring.lock();
    let Some(ring) = guard.as_mut() else {
        return read_pages_raw_pread(file, page_ids);
    };
    match read_pages_raw_uring(file, page_ids, ring) {
        Ok(pages) => Ok(pages),
        Err(err) if is_uring_setup_error(&err) => {
            tracing::debug!(error = %err, "page_store io_uring read failed; falling back to pread");
            *guard = None;
            read_pages_raw_pread(file, page_ids)
        }
        Err(err) => Err(err),
    }
}

#[cfg(not(target_os = "linux"))]
fn read_pages_raw(file: &File, page_ids: &[PageId], _read_uring: Option<&()>) -> Result<Vec<Page>> {
    read_pages_raw_pread(file, page_ids)
}

fn read_pages_raw_pread(file: &File, page_ids: &[PageId]) -> Result<Vec<Page>> {
    let mut pages = Vec::with_capacity(page_ids.len());
    for &page_id in page_ids {
        pages.push(read_page_raw(file, page_id)?);
    }
    Ok(pages)
}

#[cfg(target_os = "linux")]
fn write_page_runs_raw(
    file: &File,
    runs: Vec<(PageId, Vec<u8>)>,
    write_uring: Option<&Mutex<Option<IoUring>>>,
) -> Result<()> {
    if runs.is_empty() {
        return Ok(());
    }
    let Some(write_uring) = write_uring else {
        return write_page_runs_raw_pwrite(file, runs);
    };
    let mut guard = write_uring.lock();
    let Some(ring) = guard.as_mut() else {
        return write_page_runs_raw_pwrite(file, runs);
    };
    match write_page_runs_raw_uring(file, &runs, ring) {
        Ok(()) => Ok(()),
        Err(err) if is_uring_setup_error(&err) => {
            tracing::debug!(error = %err, "page_store io_uring write failed; falling back to pwrite");
            *guard = None;
            write_page_runs_raw_pwrite(file, runs)
        }
        Err(err) => Err(err),
    }
}

#[cfg(not(target_os = "linux"))]
fn write_page_runs_raw(
    file: &File,
    runs: Vec<(PageId, Vec<u8>)>,
    _write_uring: Option<&()>,
) -> Result<()> {
    write_page_runs_raw_pwrite(file, runs)
}

fn write_page_runs_raw_pwrite(file: &File, runs: Vec<(PageId, Vec<u8>)>) -> Result<()> {
    for (start_page, bytes) in runs {
        file.write_all_at(&bytes, start_page * PAGE_SIZE as u64)?;
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn sync_data_raw(file: &File, write_uring: Option<&Mutex<Option<IoUring>>>) -> Result<()> {
    let Some(write_uring) = write_uring else {
        file.sync_data()?;
        return Ok(());
    };
    let mut guard = write_uring.lock();
    let Some(ring) = guard.as_mut() else {
        file.sync_data()?;
        return Ok(());
    };
    match sync_data_raw_uring(file, ring) {
        Ok(()) => Ok(()),
        Err(err) if is_uring_setup_error(&err) => {
            tracing::debug!(error = %err, "page_store io_uring fsync failed; falling back to fdatasync");
            *guard = None;
            file.sync_data()?;
            Ok(())
        }
        Err(err) => Err(err),
    }
}

#[cfg(not(target_os = "linux"))]
fn sync_data_raw(file: &File, _write_uring: Option<&()>) -> Result<()> {
    file.sync_data()?;
    Ok(())
}

#[cfg(target_os = "linux")]
fn sync_data_raw_uring(file: &File, ring: &mut IoUring) -> Result<()> {
    let fd = file.as_raw_fd();
    let entry = opcode::Fsync::new(types::Fd(fd))
        .flags(types::FsyncFlags::DATASYNC)
        .build()
        .user_data(0);
    {
        let mut sq = ring.submission();
        // SAFETY: the SQE carries only the file descriptor and no borrowed
        // userspace buffer. The descriptor stays open for the whole call.
        unsafe {
            sq.push(&entry).map_err(|_| {
                MetaDbError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    "page_store fsync io_uring submission queue full",
                ))
            })?;
        }
    }
    ring.submit_and_wait(1).map_err(MetaDbError::Io)?;
    let mut cq = ring.completion();
    cq.sync();
    let Some(cqe) = cq.next() else {
        return Err(MetaDbError::Io(io::Error::new(
            io::ErrorKind::Other,
            "page_store fsync io_uring missing CQE",
        )));
    };
    let result = cqe.result();
    if result < 0 {
        return Err(MetaDbError::Io(io::Error::from_raw_os_error(-result)));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn write_page_runs_raw_uring(
    file: &File,
    runs: &[(PageId, Vec<u8>)],
    ring: &mut IoUring,
) -> Result<()> {
    let fd = file.as_raw_fd();
    for (base, chunk) in runs
        .chunks(DEFAULT_WRITE_URING_ENTRIES as usize)
        .enumerate()
    {
        for (idx, (start_page, bytes)) in chunk.iter().enumerate() {
            if bytes.is_empty() || bytes.len() % PAGE_SIZE != 0 {
                return Err(MetaDbError::InvalidArgument(format!(
                    "page run write requires a non-empty multiple of {PAGE_SIZE} bytes, got {}",
                    bytes.len()
                )));
            }
            let len = u32::try_from(bytes.len()).map_err(|_| {
                MetaDbError::InvalidArgument(format!(
                    "page run write too large for io_uring: {} bytes",
                    bytes.len()
                ))
            })?;
            let entry = opcode::Write::new(types::Fd(fd), bytes.as_ptr(), len)
                .offset(start_page * PAGE_SIZE as u64)
                .build()
                .user_data(idx as u64);
            let mut sq = ring.submission();
            // SAFETY: SQEs borrow byte buffers owned by `runs`; `runs` is
            // kept alive and not mutated until all CQEs for this chunk have
            // been harvested below.
            unsafe {
                sq.push(&entry).map_err(|_| {
                    MetaDbError::Io(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "page_store write io_uring submission queue full at chunk {base} op {idx}/{}",
                            chunk.len()
                        ),
                    ))
                })?;
            }
        }

        ring.submit_and_wait(chunk.len()).map_err(MetaDbError::Io)?;

        let mut results = vec![None; chunk.len()];
        let mut harvested = 0usize;
        let mut cq = ring.completion();
        cq.sync();
        for cqe in &mut cq {
            let idx = cqe.user_data() as usize;
            if idx >= results.len() {
                return Err(MetaDbError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "page_store write io_uring CQE user_data {idx} out of range (batch size {})",
                        chunk.len()
                    ),
                )));
            }
            results[idx] = Some(cqe.result());
            harvested += 1;
            if harvested == chunk.len() {
                break;
            }
        }
        if harvested != chunk.len() {
            return Err(MetaDbError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "page_store write io_uring harvested {harvested} CQEs, expected {}",
                    chunk.len()
                ),
            )));
        }
        drop(cq);

        for (idx, result) in results.into_iter().enumerate() {
            let result = result.ok_or_else(|| {
                MetaDbError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("page_store write io_uring missing CQE for op {idx}"),
                ))
            })?;
            if result < 0 {
                return Err(MetaDbError::Io(io::Error::from_raw_os_error(-result)));
            }
            if result as usize != chunk[idx].1.len() {
                return Err(MetaDbError::Io(io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!(
                        "page_store write io_uring short write at page {}: got {result} of {}",
                        chunk[idx].0,
                        chunk[idx].1.len()
                    ),
                )));
            }
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn read_pages_raw_uring(file: &File, page_ids: &[PageId], ring: &mut IoUring) -> Result<Vec<Page>> {
    let fd = file.as_raw_fd();
    let mut out = Vec::with_capacity(page_ids.len());
    for chunk in page_ids.chunks(DEFAULT_READ_URING_ENTRIES as usize) {
        let mut pages: Vec<Page> = (0..chunk.len()).map(|_| Page::zeroed()).collect();
        for (idx, (&page_id, page)) in chunk.iter().zip(pages.iter_mut()).enumerate() {
            let entry = opcode::Read::new(
                types::Fd(fd),
                page.bytes_mut().as_mut_ptr(),
                PAGE_SIZE as u32,
            )
            .offset(page_id * PAGE_SIZE as u64)
            .build()
            .user_data(idx as u64);
            let mut sq = ring.submission();
            // SAFETY: each SQE points at a distinct Page buffer in `pages`.
            // The vector is kept alive and not reallocated until every CQE for
            // this chunk has been harvested below.
            unsafe {
                sq.push(&entry).map_err(|_| {
                    MetaDbError::Io(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "page_store io_uring submission queue full at op {idx}/{}",
                            chunk.len()
                        ),
                    ))
                })?;
            }
        }

        ring.submit_and_wait(chunk.len()).map_err(MetaDbError::Io)?;

        let mut results = vec![None; chunk.len()];
        let mut harvested = 0usize;
        let mut cq = ring.completion();
        cq.sync();
        for cqe in &mut cq {
            let idx = cqe.user_data() as usize;
            if idx >= results.len() {
                return Err(MetaDbError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "page_store io_uring CQE user_data {idx} out of range (batch size {})",
                        chunk.len()
                    ),
                )));
            }
            results[idx] = Some(cqe.result());
            harvested += 1;
            if harvested == chunk.len() {
                break;
            }
        }
        if harvested != chunk.len() {
            return Err(MetaDbError::Io(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "page_store io_uring harvested {harvested} CQEs, expected {}",
                    chunk.len()
                ),
            )));
        }
        drop(cq);

        for (idx, result) in results.into_iter().enumerate() {
            let result = result.ok_or_else(|| {
                MetaDbError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("page_store io_uring missing CQE for op {idx}"),
                ))
            })?;
            if result < 0 {
                return Err(MetaDbError::Io(io::Error::from_raw_os_error(-result)));
            }
            if result as usize != PAGE_SIZE {
                return Err(MetaDbError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "page_store io_uring short read at page {}: got {result} of {PAGE_SIZE}",
                        chunk[idx]
                    ),
                )));
            }
        }
        out.extend(pages);
    }
    Ok(out)
}

#[cfg(target_os = "linux")]
fn is_uring_setup_error(err: &MetaDbError) -> bool {
    match err {
        MetaDbError::Io(io) => matches!(
            io.raw_os_error(),
            Some(libc::ENOSYS) | Some(libc::EINVAL) | Some(libc::EPERM)
        ),
        _ => false,
    }
}

fn is_zero_page(page: &Page) -> bool {
    page.bytes().iter().all(|b| *b == 0)
}

#[cfg(target_os = "linux")]
fn punch_hole(file: &File, offset: u64, len: u64) -> Result<()> {
    let rc = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            offset as libc::off_t,
            len as libc::off_t,
        )
    };
    if rc == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EOPNOTSUPP) | Some(libc::ENOSYS) | Some(libc::EINVAL) => Ok(()),
        _ => Err(err.into()),
    }
}

#[cfg(not(target_os = "linux"))]
fn punch_hole(_file: &File, _offset: u64, _len: u64) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn mk_page(lsn: Lsn, first_byte: u8) -> Page {
        let mut p = Page::new(PageHeader::new(PageType::L2pLeaf, lsn));
        p.payload_mut()[0] = first_byte;
        p.seal();
        p
    }

    #[test]
    fn create_sizes_file_to_manifest_region() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        assert_eq!(ps.high_water(), FIRST_DATA_PAGE);
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            FIRST_DATA_PAGE * PAGE_SIZE as u64,
        );
    }

    #[test]
    fn allocate_write_read_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();

        let pid = ps.allocate().unwrap();
        assert_eq!(pid, FIRST_DATA_PAGE);

        ps.write_page(pid, &mk_page(42, 0xAB)).unwrap();
        ps.sync().unwrap();

        let r = ps.read_page(pid).unwrap();
        let h = r.header().unwrap();
        assert_eq!(h.generation, 42);
        assert_eq!(r.payload()[0], 0xAB);
    }

    #[test]
    fn many_pages_round_trip() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        let mut ids = Vec::new();
        for i in 0..16u64 {
            let pid = ps.allocate().unwrap();
            ids.push(pid);
            ps.write_page(pid, &mk_page(i, i as u8)).unwrap();
        }
        ps.sync().unwrap();
        assert_eq!(ps.high_water(), FIRST_DATA_PAGE + 16);
        for (i, pid) in ids.iter().enumerate() {
            let r = ps.read_page(*pid).unwrap();
            assert_eq!(r.header().unwrap().generation, i as u64);
            assert_eq!(r.payload()[0], i as u8);
        }
    }

    #[test]
    fn reopen_preserves_pages_and_rebuilds_free_list() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        {
            let ps = PageStore::create(&path).unwrap();
            for i in 0..4u64 {
                let pid = ps.allocate().unwrap();
                ps.write_page(pid, &mk_page(i + 1, i as u8)).unwrap();
            }
            ps.free(FIRST_DATA_PAGE + 1, 100).unwrap();
            ps.try_reclaim().unwrap();
            ps.sync_all().unwrap();
        }
        let ps = PageStore::open(&path).unwrap();
        assert_eq!(ps.high_water(), FIRST_DATA_PAGE + 4);
        assert_eq!(ps.free_list_len(), 1);
        // Reallocating must recycle the freed page.
        let pid = ps.allocate().unwrap();
        assert_eq!(pid, FIRST_DATA_PAGE + 1);
    }

    #[test]
    fn try_reclaim_recycles_freed_pids() {
        // Deferred-free means three free calls + one try_reclaim batch
        // hands every pid back to the free list in some order. We assert
        // the SET of recycled pids and that no allocation bumped past
        // the original high-water — order within the batch is unspecified
        // because reclaim drains a HashMap.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        let a = ps.allocate().unwrap();
        let b = ps.allocate().unwrap();
        let c = ps.allocate().unwrap();
        for pid in [a, b, c] {
            ps.write_page(pid, &mk_page(1, 0)).unwrap();
        }
        ps.free(a, 10).unwrap();
        ps.free(b, 11).unwrap();
        ps.free(c, 12).unwrap();
        // Frees are deferred; the free list is empty until reclaim runs.
        assert_eq!(ps.free_list_len(), 0);
        assert_eq!(ps.deferred_free_len(), 3);
        let reclaimed = ps.try_reclaim().unwrap();
        assert_eq!(reclaimed.len(), 3);
        let mut got = vec![
            ps.allocate().unwrap(),
            ps.allocate().unwrap(),
            ps.allocate().unwrap(),
        ];
        got.sort();
        assert_eq!(got, vec![a, b, c]);
        assert_eq!(ps.high_water(), c + 1);
    }

    #[test]
    fn read_beyond_high_water_is_error() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        match ps.read_page(999).unwrap_err() {
            MetaDbError::PageOutOfRange(999) => {}
            e => panic!("{e}"),
        }
    }

    #[test]
    fn cannot_free_manifest_slots() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        assert!(matches!(
            ps.free(0, 1).unwrap_err(),
            MetaDbError::InvalidArgument(_)
        ));
        assert!(matches!(
            ps.free(1, 1).unwrap_err(),
            MetaDbError::InvalidArgument(_)
        ));
    }

    #[test]
    fn cannot_free_out_of_range() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        assert!(matches!(
            ps.free(999, 1).unwrap_err(),
            MetaDbError::PageOutOfRange(999)
        ));
    }

    #[test]
    fn corrupt_page_read_fails_verify_with_page_id() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let pid = {
            let ps = PageStore::create(&path).unwrap();
            let pid = ps.allocate().unwrap();
            ps.write_page(pid, &mk_page(1, 0)).unwrap();
            ps.sync_all().unwrap();
            pid
        };
        // Flip a byte directly on disk inside the payload area.
        {
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            let off = pid * PAGE_SIZE as u64 + 100;
            f.write_all_at(&[0xFF], off).unwrap();
            f.sync_all().unwrap();
        }
        let ps = PageStore::open(&path).unwrap();
        match ps.read_page(pid).unwrap_err() {
            MetaDbError::PageChecksumMismatch { page_id, .. } => {
                assert_eq!(page_id, pid);
            }
            e => panic!("{e}"),
        }
        // But read_page_unchecked must succeed (returns the corrupt bytes).
        let corrupt = ps.read_page_unchecked(pid).unwrap();
        assert!(corrupt.verify(pid).is_err());
    }

    #[test]
    fn open_rejects_non_page_multiple_size() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        // Write 5000 bytes (not a multiple of 4096)
        std::fs::write(&path, vec![0u8; 5000]).unwrap();
        match PageStore::open(&path).unwrap_err() {
            MetaDbError::Corruption(_) => {}
            e => panic!("{e}"),
        }
    }

    #[test]
    fn open_rejects_shorter_than_manifest_region() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        std::fs::write(&path, vec![0u8; PAGE_SIZE]).unwrap(); // only 1 page
        match PageStore::open(&path).unwrap_err() {
            MetaDbError::Corruption(_) => {}
            e => panic!("{e}"),
        }
    }

    #[test]
    fn create_fails_if_file_exists() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        std::fs::write(&path, b"").unwrap();
        assert!(PageStore::create(&path).is_err());
    }

    #[test]
    fn path_is_retained() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        assert_eq!(ps.path(), path);
    }

    #[test]
    fn allocate_run_returns_contiguous_range() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        let start = ps.allocate_run(8).unwrap();
        assert_eq!(start, FIRST_DATA_PAGE);
        assert_eq!(ps.high_water(), FIRST_DATA_PAGE + 8);
        // A subsequent single allocate extends further; no overlap.
        let next = ps.allocate().unwrap();
        assert_eq!(next, FIRST_DATA_PAGE + 8);
    }

    #[test]
    fn allocate_run_rejects_zero_count() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        assert!(matches!(
            ps.allocate_run(0).unwrap_err(),
            MetaDbError::InvalidArgument(_)
        ));
    }

    #[test]
    fn allocate_run_skips_free_list() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        // Seed the free list with one individual page.
        let pid = ps.allocate().unwrap();
        ps.write_page(pid, &mk_page(1, 0)).unwrap();
        ps.free(pid, 1).unwrap();
        ps.try_reclaim().unwrap();
        assert_eq!(ps.free_list_len(), 1);
        // Run allocation bypasses the free list entirely.
        let start = ps.allocate_run(4).unwrap();
        assert_eq!(start, FIRST_DATA_PAGE + 1);
        assert_eq!(ps.free_list_len(), 1);
    }

    #[test]
    fn free_run_returns_pages_to_free_list() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        let start = ps.allocate_run(3).unwrap();
        for i in 0..3 {
            ps.write_page(start + i, &mk_page(1, 0)).unwrap();
        }
        ps.free_run(start, 3, 99).unwrap();
        ps.try_reclaim().unwrap();
        assert_eq!(ps.free_list_len(), 3);
    }

    #[test]
    fn batched_allocate_extends_file_by_chunk_boundary() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let chunk: u64 = 8;
        let ps = PageStore::create_with_grow_chunk(&path, chunk).unwrap();
        // One allocate should pre-extend the file by the whole chunk.
        let _ = ps.allocate().unwrap();
        let expected_pages = FIRST_DATA_PAGE + chunk;
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            expected_pages * PAGE_SIZE as u64,
            "first allocate should pre-extend to the next chunk boundary",
        );
        // Fill the rest of the chunk; file size must not change.
        for _ in 1..chunk {
            let _ = ps.allocate().unwrap();
        }
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            expected_pages * PAGE_SIZE as u64,
            "allocations within the committed chunk must not extend the file",
        );
        assert_eq!(ps.high_water(), FIRST_DATA_PAGE + chunk);
        // One more allocate should roll into the next chunk.
        let _ = ps.allocate().unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            (FIRST_DATA_PAGE + 2 * chunk) * PAGE_SIZE as u64,
            "crossing a chunk boundary extends the file by exactly one more chunk",
        );
    }

    #[test]
    fn allocate_run_respects_grow_chunk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let chunk: u64 = 4;
        let ps = PageStore::create_with_grow_chunk(&path, chunk).unwrap();
        // Run of 6 with chunk 4 → file must cover >= 6 pages, rounded up
        // to the next chunk boundary (8).
        let start = ps.allocate_run(6).unwrap();
        assert_eq!(start, FIRST_DATA_PAGE);
        assert_eq!(ps.high_water(), FIRST_DATA_PAGE + 6);
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            (FIRST_DATA_PAGE + 2 * chunk) * PAGE_SIZE as u64,
        );
    }

    #[test]
    fn reject_zero_grow_chunk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        assert!(matches!(
            PageStore::create_with_grow_chunk(&path, 0).unwrap_err(),
            MetaDbError::InvalidArgument(_)
        ));
    }

    #[test]
    fn open_truncates_growth_tail() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let chunk: u64 = 64;
        let last_valid_pid;
        {
            let ps = PageStore::create_with_grow_chunk(&path, chunk).unwrap();
            // Allocate + write 3 pages. Pre-extend reserves `chunk`
            // pages worth of growth tail on disk (pages 5..=66 zero-init).
            for i in 0..3 {
                let pid = ps.allocate().unwrap();
                ps.write_page(pid, &mk_page(1, i as u8)).unwrap();
            }
            last_valid_pid = FIRST_DATA_PAGE + 2;
            ps.sync_all().unwrap();
            assert_eq!(
                std::fs::metadata(&path).unwrap().len(),
                (FIRST_DATA_PAGE + chunk) * PAGE_SIZE as u64,
                "pre-extend must have reserved the whole chunk",
            );
        }
        // Reopen: the growth tail (zero pages past the last valid one)
        // should be truncated back.
        let ps = PageStore::open_with_grow_chunk(&path, chunk).unwrap();
        assert_eq!(ps.high_water(), last_valid_pid + 1);
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            (last_valid_pid + 1) * PAGE_SIZE as u64,
            "open must truncate zero-init growth tail back to last valid page",
        );
    }

    #[test]
    fn open_truncates_punched_tail_free_page() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let chunk: u64 = 32;
        let last_valid_pid;
        {
            let ps = PageStore::create_with_grow_chunk(&path, chunk).unwrap();
            // Allocate 3, free the last: hole punching turns that page into
            // zero tail, so reopen can truncate it and hand the id out again
            // from high_water.
            for _ in 0..3 {
                let pid = ps.allocate().unwrap();
                ps.write_page(pid, &mk_page(1, 0)).unwrap();
            }
            ps.free(FIRST_DATA_PAGE + 2, 42).unwrap();
            ps.try_reclaim().unwrap();
            last_valid_pid = FIRST_DATA_PAGE + 1;
            ps.sync_all().unwrap();
        }
        let ps = PageStore::open_with_grow_chunk(&path, chunk).unwrap();
        assert_eq!(ps.high_water(), last_valid_pid + 1);
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            (last_valid_pid + 1) * PAGE_SIZE as u64,
        );
        assert_eq!(ps.free_list_len(), 0);
        assert_eq!(ps.allocate().unwrap(), FIRST_DATA_PAGE + 2);
    }

    #[test]
    fn open_on_all_zero_growth_tail_recovers_as_empty_data_region() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        // Fabricate a file with a manifest region and pure zero growth tail
        // past it, as if a crash happened after pre-extend but before any
        // data page was written.
        let pages_on_disk = FIRST_DATA_PAGE + 16;
        std::fs::write(
            &path,
            vec![0u8; (pages_on_disk * PAGE_SIZE as u64) as usize],
        )
        .unwrap();
        let ps = PageStore::open_with_grow_chunk(&path, 16).unwrap();
        // No page past the manifest region decoded as valid → high_water
        // sits at FIRST_DATA_PAGE, and the growth tail is truncated.
        assert_eq!(ps.high_water(), FIRST_DATA_PAGE);
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            FIRST_DATA_PAGE * PAGE_SIZE as u64,
        );
    }

    #[test]
    fn crash_safety_allocate_without_write_is_not_leaked_after_reopen() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let chunk: u64 = 16;
        {
            let ps = PageStore::create_with_grow_chunk(&path, chunk).unwrap();
            // Write 2 pages then leak an allocation (simulating a crash
            // between allocate and write_page, with WAL un-committed).
            for i in 0..2 {
                let pid = ps.allocate().unwrap();
                ps.write_page(pid, &mk_page(1, i as u8)).unwrap();
            }
            let _leaked = ps.allocate().unwrap();
            ps.sync_all().unwrap();
        }
        // Reopen: the leaked allocation becomes part of growth tail (the
        // page is still zero on disk, so its header fails to decode).
        let ps = PageStore::open_with_grow_chunk(&path, chunk).unwrap();
        assert_eq!(ps.high_water(), FIRST_DATA_PAGE + 2);
        // New allocations reuse page ids from where the recovered high
        // water points, overwriting the zeroed leak in place.
        let pid = ps.allocate().unwrap();
        assert_eq!(pid, FIRST_DATA_PAGE + 2);
    }
}
