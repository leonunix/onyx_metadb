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

use crossbeam_channel::{Receiver, Sender, bounded};
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
use io_uring::{IoUring, opcode, types};

use crate::config::PAGE_SIZE;
use crate::epoch::EpochManager;
use crate::error::{MetaDbError, Result};
use crate::io_submitter::IoSubmitter;
use crate::metrics::MetaMetrics;
use crate::page::{Page, PageHeader, PageType};
use crate::types::{FIRST_DATA_PAGE, Lsn, PageId};

const RC_LOCK_SHARDS: usize = 64;
const MAX_RECLAIM_RUN_PAGES: usize = 1024;
const MIN_PUNCH_HOLE_RUN_PAGES: usize = 16;
// Keep checkpoint writev runs large enough to amortise syscall overhead
// without letting one checkpoint submit hundreds of MiB and starve WAL fsync.
const MAX_SEALED_WRITE_RUN_PAGES: usize = 256;
#[cfg(target_os = "linux")]
const MAX_SEALED_WRITE_URING_RUNS: usize = 16;
const PAGE_READ_POOL_WORKERS_MAX: usize = 8;
const PAGE_READ_BATCH_MAX: usize = 64;
const PAGE_READ_BATCH_WINDOW: Duration = Duration::from_micros(8);
const PAGE_READ_CHANNEL_CAP: usize = 512;
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
    read_pool: PageReadPool,
    /// Pool of io_uring submitters. Each entry owns its own ring +
    /// worker thread, so concurrent writers from different shards /
    /// background workers don't share an SQ — a writeback burst on
    /// one submitter cannot stall commit-apply writes routed to a
    /// different submitter.
    ///
    /// Routing: every write API hashes its `PageId` into the pool
    /// (`io_submitter_for(pid) = io_submitters[pid as usize % len]`).
    /// Allocation is sequential, so adjacent pids land on the same
    /// submitter and coalesce into the same `IORING_OP_WRITEV` SQE;
    /// non-adjacent runs from disparate shards naturally spread across
    /// rings. `sync()` fans out `IORING_OP_FSYNC` to every submitter
    /// in parallel because io_uring fsync only orders writes on the
    /// same ring.
    ///
    /// Empty when io_uring is unavailable (old kernel, sandboxed
    /// environment); callers fall back to direct pwrite + fdatasync.
    io_submitters: Box<[IoSubmitter]>,
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
    /// Pending physical frees, keyed by pid. BTreeMap keeps reclaim
    /// selection ordered by page id so a budgeted pass still coalesces
    /// into large hole-punch extents instead of random single pages.
    /// idempotent / replay path cannot push the same pid twice).
    deferred_free: Mutex<BTreeMap<PageId, DeferredFree>>,
    /// Set once during `Db` construction (after `MetaMetrics::new`),
    /// then read-only for the lifetime of the store. Use `Option::ok()`
    /// in IO paths so unit tests that build a bare `PageStore` still
    /// work without a metrics handle.
    metrics: OnceLock<Arc<MetaMetrics>>,
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

#[derive(Debug, Default)]
pub struct ReclaimOutcome {
    pub safe_below: u64,
    pub selected: usize,
    pub reclaimed: Vec<PageId>,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RcDeltaWithGen {
    pub page_id: PageId,
    pub delta: i32,
    pub lsn: Lsn,
    pub ordinal: u32,
}

fn new_rc_locks() -> Box<[Mutex<()>]> {
    (0..RC_LOCK_SHARDS)
        .map(|_| Mutex::new(()))
        .collect::<Vec<_>>()
        .into_boxed_slice()
}

/// Default pool size. Pool>1 requires
/// [`AffinityConfig::io_submitter_cpus`] to pin each submitter to a
/// distinct CPU — without pinning, multiple submitter threads can
/// land on the same core and net-regress vs pool=1 (j8 d4 with
/// pool=3 unbound: 32k LBA/s vs 59k for pool=1). Once a CPU set is
/// available, callers can flip this up to one submitter per
/// [`IoLaneClass`] (currently three) for per-class SQ isolation;
/// SQ=16384 per ring stays the throughput knob.
pub const DEFAULT_IO_SUBMITTER_POOL_SIZE: usize = 1;

/// Per-IO routing class. The pool keeps one [`IoSubmitter`] per
/// variant so independent write streams cannot saturate one another's
/// SQ. The hot lane mapping:
///
/// * `L2p` — paged radix tree writes from `PagedL2p` (single most
///   numerous foreground writer; gates commit ack).
/// * `Refcount` — `paged_meta` writes from refcount delta apply and
///   the legacy dedup_reverse path (lower volume, also commit-sync).
/// * `Dedup` — cuckoo page writes from the dedup_index apply lane.
///   These run async w.r.t. commit ack, so bursts here must not stall
///   L2p / Refcount writes that commit threads still wait on.
///
/// Variant ordinals must be `0..N` and contiguous so the
/// [`PageStore::io_submitter_for_class`] lookup is a slice index. Add
/// new variants only when the pool size grows in lockstep.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoLaneClass {
    L2p = 0,
    Refcount = 1,
    Dedup = 2,
}

impl IoLaneClass {
    fn index(self) -> usize {
        self as usize
    }
}

fn make_io_submitters(file: &File, count: usize) -> Box<[IoSubmitter]> {
    #[cfg(target_os = "linux")]
    {
        let count = count.max(1);
        let mut subs: Vec<IoSubmitter> = Vec::with_capacity(count);
        for ordinal in 0..count {
            match IoSubmitter::start_with_ordinal(file.as_raw_fd(), ordinal) {
                Some(sub) => subs.push(sub),
                None => {
                    // First submitter failed — io_uring unavailable.
                    // Bail out empty so callers fall back to pwrite.
                    return Box::new([]);
                }
            }
        }
        subs.into_boxed_slice()
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = file;
        let _ = count;
        Box::new([])
    }
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

struct PageReadRequest {
    page_id: PageId,
    reply: Sender<Result<Page>>,
}

struct PageReadPool {
    sender: Option<Sender<PageReadRequest>>,
    workers: Vec<JoinHandle<()>>,
}

impl PageReadPool {
    fn start(file: &File) -> Result<Self> {
        let workers = std::thread::available_parallelism()
            .map(|n| n.get().div_ceil(16))
            .unwrap_or(1)
            .clamp(1, PAGE_READ_POOL_WORKERS_MAX);
        let (tx, rx) = bounded::<PageReadRequest>(PAGE_READ_CHANNEL_CAP);
        let mut handles = Vec::with_capacity(workers);
        for worker_idx in 0..workers {
            let worker_file = file.try_clone()?;
            let rx = rx.clone();
            let join = thread::Builder::new()
                .name(format!("metadb-page-read-{worker_idx}"))
                .spawn(move || page_read_worker_loop(worker_file, rx))
                .map_err(MetaDbError::Io)?;
            handles.push(join);
        }
        Ok(Self {
            sender: Some(tx),
            workers: handles,
        })
    }

    fn read_page(&self, page_id: PageId) -> Result<Page> {
        let sender = self
            .sender
            .as_ref()
            .ok_or_else(|| MetaDbError::Io(io::Error::other("page read pool already shut down")))?;
        let (reply_tx, reply_rx) = bounded(1);
        sender
            .send(PageReadRequest {
                page_id,
                reply: reply_tx,
            })
            .map_err(|_| MetaDbError::Io(io::Error::other("page read pool closed")))?;
        reply_rx
            .recv()
            .map_err(|_| MetaDbError::Io(io::Error::other("page read worker dropped reply")))?
    }
}

impl Drop for PageReadPool {
    fn drop(&mut self) {
        self.sender.take();
        for join in self.workers.drain(..) {
            let _ = join.join();
        }
    }
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

    /// Fast-open an existing page store without rebuilding the free list.
    ///
    /// This trusts the file length as the high-water mark and starts with an
    /// empty in-memory free list. It is correctness-preserving for normal
    /// reads/replay because every reachable page still lies below EOF, but
    /// previously-free interior pages will not be reused until a later online
    /// reclaim/checkpoint or an explicit verifier/repair pass makes them
    /// visible again. Intended for large embedded databases where scanning
    /// every historical page at service startup is too expensive.
    pub fn open_fast(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_fast_with_grow_chunk(path, DEFAULT_GROW_CHUNK_PAGES)
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
        let read_pool = PageReadPool::start(&file)?;
        let io_submitters = make_io_submitters(&file, DEFAULT_IO_SUBMITTER_POOL_SIZE);
        Ok(Self {
            path,
            file,
            read_pool,
            io_submitters,
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
            deferred_free: Mutex::new(BTreeMap::new()),
            metrics: OnceLock::new(),
        })
    }

    /// Fast-open an existing page store with the caller's grow chunk. See
    /// [`open_fast`](Self::open_fast) for the tradeoff.
    pub fn open_fast_with_grow_chunk(path: impl AsRef<Path>, grow_chunk: u64) -> Result<Self> {
        if grow_chunk == 0 {
            return Err(MetaDbError::InvalidArgument(
                "page store grow_chunk must be >= 1".into(),
            ));
        }
        let open_started = std::time::Instant::now();
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
        let read_pool = PageReadPool::start(&file)?;
        let io_submitters = make_io_submitters(&file, DEFAULT_IO_SUBMITTER_POOL_SIZE);
        tracing::info!(
            path = %path.display(),
            high_water_pages = file_end_pages,
            elapsed_ms = open_started.elapsed().as_millis(),
            "metadb page store fast-open complete"
        );
        Ok(Self {
            path,
            file,
            read_pool,
            io_submitters,
            #[cfg(target_os = "linux")]
            read_uring: Mutex::new(new_read_uring()),
            #[cfg(target_os = "linux")]
            write_uring: Mutex::new(new_write_uring()),
            inner: Mutex::new(Inner {
                high_water: file_end_pages,
                committed_file_pages: file_end_pages,
                free_list: Vec::new(),
            }),
            rc_locks: new_rc_locks(),
            grow_chunk,
            epoch: Arc::new(EpochManager::new()),
            deferred_free: Mutex::new(BTreeMap::new()),
            metrics: OnceLock::new(),
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
        let open_started = std::time::Instant::now();
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
        let read_pool = PageReadPool::start(&file)?;
        let io_submitters = make_io_submitters(&file, DEFAULT_IO_SUBMITTER_POOL_SIZE);
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
        tracing::info!(
            path = %path.display(),
            scanned_pages = file_end_pages.saturating_sub(FIRST_DATA_PAGE),
            high_water_pages = high_water,
            free_list_pages = free_list.len(),
            elapsed_ms = open_started.elapsed().as_millis(),
            "metadb page store open scan complete"
        );
        Ok(Self {
            path,
            file,
            read_pool,
            io_submitters,
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
            deferred_free: Mutex::new(BTreeMap::new()),
            metrics: OnceLock::new(),
        })
    }

    /// Path the store was opened from.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Attach the parent `Db`'s metrics handle so page-store IO paths
    /// can record `meta_io_*` counters. Idempotent: calling twice with
    /// the same handle is fine; calling with different handles is a
    /// no-op after the first.
    pub fn attach_metrics(&self, metrics: Arc<MetaMetrics>) {
        for submitter in self.io_submitters.iter() {
            submitter.attach_metrics(Arc::clone(&metrics));
        }
        let _ = self.metrics.set(metrics);
    }

    fn metrics(&self) -> Option<&Arc<MetaMetrics>> {
        self.metrics.get()
    }

    /// Pick an `IoSubmitter` for a write of `pid`. Returns `None` when
    /// io_uring is unavailable (callers fall back to pwrite).
    ///
    /// Legacy hash routing kept for callers that have no natural lane
    /// class (recovery, verifier tooling, single-writer test
    /// fixtures). Production hot writers should go through
    /// [`Self::io_submitter_for_class`] so they pin to their own
    /// submitter and cannot saturate the SQ of a sibling lane.
    fn io_submitter_for(&self, pid: PageId) -> Option<&IoSubmitter> {
        if self.io_submitters.is_empty() {
            None
        } else {
            let idx = (pid as usize) % self.io_submitters.len();
            Some(&self.io_submitters[idx])
        }
    }

    /// Pick the [`IoSubmitter`] reserved for `class`. Returns `None`
    /// when io_uring is unavailable, or when the pool is smaller than
    /// expected (pre-upgrade installs / test fixtures that build
    /// `PageStore` with a forced pool size). In both fallbacks the
    /// caller drops to `pwrite`, preserving the on-disk contract.
    fn io_submitter_for_class(&self, class: IoLaneClass) -> Option<&IoSubmitter> {
        self.io_submitters.get(class.index())
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
        let started = Instant::now();
        let page = self.read_pool.read_page(page_id)?;
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_read_batch(1, PAGE_SIZE, started.elapsed());
        }
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
        let started = Instant::now();
        let pages = read_pages_raw(&self.file, page_ids, self.read_uring())?;
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_read_batch(
                page_ids.len(),
                page_ids.len() * PAGE_SIZE,
                started.elapsed(),
            );
        }
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
        self.write_page_for_class(page_id, page, IoLaneClass::L2p)
    }

    /// Write `page` at `page_id`, routed through the [`IoLaneClass`]
    /// submitter so dedup / refcount bursts cannot saturate the L2p
    /// SQ. Caller must have sealed the page; this method does not
    /// reseal.
    pub fn write_page_for_class(
        &self,
        page_id: PageId,
        page: &Page,
        class: IoLaneClass,
    ) -> Result<()> {
        self.check_in_range(page_id)?;
        let started = Instant::now();
        if let Some(submitter) = self.io_submitter_for_class(class) {
            submitter.submit_write(page_id, Arc::new(page.clone()))?;
        } else if let Some(submitter) = self.io_submitter_for(page_id) {
            submitter.submit_write(page_id, Arc::new(page.clone()))?;
        } else {
            self.file
                .write_all_at(page.bytes(), page_id * PAGE_SIZE as u64)?;
        }
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_write_batch(1, PAGE_SIZE, started.elapsed());
        }
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
        let started = Instant::now();
        self.file
            .write_all_at(bytes, start_page * PAGE_SIZE as u64)?;
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_write_batch(pages as usize, bytes.len(), started.elapsed());
        }
        Ok(())
    }

    /// Write multiple already-sealed page runs, keeping the final
    /// durability boundary at the caller's later [`sync`](Self::sync).
    pub fn write_page_runs_parallel(&self, runs: Vec<(PageId, Vec<u8>)>) -> Result<()> {
        let (ops, bytes) = runs.iter().fold((0usize, 0usize), |(o, b), (_, run)| {
            (o + run.len() / PAGE_SIZE, b + run.len())
        });
        let started = Instant::now();
        write_page_runs_raw(&self.file, runs, self.write_uring())?;
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_write_batch(ops, bytes, started.elapsed());
        }
        Ok(())
    }

    pub fn write_sealed_page_runs(&self, pages: Vec<(PageId, Arc<Page>)>) -> Result<()> {
        self.write_sealed_page_runs_for_class(pages, IoLaneClass::L2p)
    }

    pub fn write_sealed_page_runs_for_class(
        &self,
        mut pages: Vec<(PageId, Arc<Page>)>,
        class: IoLaneClass,
    ) -> Result<()> {
        if pages.is_empty() {
            return Ok(());
        }
        let ops = pages.len();
        let bytes = ops * PAGE_SIZE;
        pages.sort_unstable_by_key(|(pid, _)| *pid);
        let started = Instant::now();
        // Route through the lane-class submitter so dedup / refcount /
        // L2p write streams cannot saturate one another's SQ. Falls
        // back to hash routing (legacy behaviour) when the pool is
        // smaller than expected, and to pwrite when io_uring is
        // unavailable.
        let class_submitter = self.io_submitter_for_class(class);
        if class_submitter.is_some() || !self.io_submitters.is_empty() {
            let runs = coalesce_sealed_runs(pages, MAX_SEALED_WRITE_RUN_PAGES);
            let receivers: Vec<_> = runs
                .into_iter()
                .map(|(start, run_pages)| {
                    let submitter = class_submitter
                        .or_else(|| self.io_submitter_for(start))
                        .expect("io_submitters non-empty above");
                    submitter.submit_write_run_async(start, run_pages)
                })
                .collect::<Result<Vec<_>>>()?;
            let mut first_err: Option<MetaDbError> = None;
            for rx in receivers {
                match rx.recv() {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        if first_err.is_none() {
                            first_err = Some(err);
                        }
                    }
                    Err(_) => {
                        if first_err.is_none() {
                            first_err = Some(MetaDbError::Io(io::Error::other(
                                "io submitter dropped reply for write run",
                            )));
                        }
                    }
                }
            }
            if let Some(err) = first_err {
                return Err(err);
            }
        } else {
            write_sealed_pages_raw(&self.file, pages, self.write_uring())?;
        }
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_write_batch(ops, bytes, started.elapsed());
        }
        Ok(())
    }

    /// Fallback writer for sealed pages. It keeps only one coalesced
    /// byte run in memory at a time, rather than materialising every
    /// dirty checkpoint page into a second full-size buffer.
    fn write_sealed_page_runs_pwrite(file: &File, pages: Vec<(PageId, Arc<Page>)>) -> Result<()> {
        let mut run_start: Option<PageId> = None;
        let mut run_next = 0;
        let mut run_bytes = Vec::with_capacity(MAX_SEALED_WRITE_RUN_PAGES * PAGE_SIZE);

        fn flush_run(
            file: &File,
            run_start: &mut Option<PageId>,
            run_bytes: &mut Vec<u8>,
        ) -> Result<()> {
            if let Some(start) = run_start.take() {
                file.write_all_at(run_bytes, start * PAGE_SIZE as u64)?;
                run_bytes.clear();
            }
            Ok(())
        }

        for (pid, page) in pages {
            let run_pages = run_bytes.len() / PAGE_SIZE;
            if run_start.is_some() && (pid != run_next || run_pages >= MAX_SEALED_WRITE_RUN_PAGES) {
                flush_run(file, &mut run_start, &mut run_bytes)?;
            }
            if run_start.is_none() {
                run_start = Some(pid);
            }
            run_bytes.extend_from_slice(page.bytes());
            run_next = pid + 1;
        }
        flush_run(file, &mut run_start, &mut run_bytes)?;
        Ok(())
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

    /// Batch form of [`atomic_rc_delta_with_gen`]. All target pages are
    /// locked in shard order, read through the page-store batch path,
    /// mutated in memory, and written back through the sealed-page batch
    /// writer. This preserves the same per-page WAL idempotency markers
    /// while giving NVMe real queue depth for COW refcount commits.
    pub(crate) fn atomic_rc_delta_batch_with_gen(
        &self,
        deltas: &[RcDeltaWithGen],
    ) -> Result<Vec<u32>> {
        if deltas.is_empty() {
            return Ok(Vec::new());
        }
        {
            let inner = self.inner.lock();
            for delta in deltas {
                if delta.lsn == 0 {
                    return Err(MetaDbError::InvalidArgument(
                        "atomic_rc_delta_batch_with_gen: lsn must be > 0".into(),
                    ));
                }
                if delta.page_id >= inner.high_water {
                    return Err(MetaDbError::PageOutOfRange(delta.page_id));
                }
            }
        }

        let mut indexed: Vec<(usize, RcDeltaWithGen)> =
            deltas.iter().copied().enumerate().collect();
        indexed.sort_unstable_by_key(|(_, delta)| delta.page_id);
        if let Some(duplicate) = indexed.windows(2).find_map(|pair| {
            if pair[0].1.page_id == pair[1].1.page_id {
                Some(pair[0].1.page_id)
            } else {
                None
            }
        }) {
            return Err(MetaDbError::Corruption(format!(
                "atomic_rc_delta_batch_with_gen: duplicate page {duplicate} in one batch"
            )));
        }

        let mut shards: Vec<usize> = indexed
            .iter()
            .map(|(_, delta)| (delta.page_id as usize) % RC_LOCK_SHARDS)
            .collect();
        shards.sort_unstable();
        shards.dedup();
        let _guards: Vec<_> = shards
            .iter()
            .map(|&shard| self.rc_locks[shard].lock())
            .collect();

        let page_ids: Vec<PageId> = indexed.iter().map(|(_, delta)| delta.page_id).collect();
        let pages = self.read_pages(&page_ids)?;
        let mut results = vec![0u32; deltas.len()];
        let mut sealed_pages = Vec::new();

        for (((original_idx, delta), mut page), page_id) in
            indexed.into_iter().zip(pages).zip(page_ids)
        {
            let cur_gen = page.generation();
            let cur_ordinal = page.flags();
            let cur_rc = page.refcount();
            if cur_gen > delta.lsn || (cur_gen == delta.lsn && cur_ordinal >= delta.ordinal) {
                results[original_idx] = cur_rc;
                continue;
            }
            let new_rc = if delta.delta >= 0 {
                cur_rc.checked_add(delta.delta as u32)
            } else {
                cur_rc.checked_sub((-delta.delta) as u32)
            }
            .ok_or_else(|| {
                MetaDbError::Corruption(format!(
                    "atomic_rc_delta_batch_with_gen: page {} refcount {} + {} out of range",
                    delta.page_id, cur_rc, delta.delta
                ))
            })?;
            page.set_refcount(new_rc);
            page.set_generation(delta.lsn);
            page.set_flags(delta.ordinal);
            page.seal();
            results[original_idx] = new_rc;
            sealed_pages.push((page_id, Arc::new(page)));
        }

        self.write_sealed_page_runs(sealed_pages)?;
        Ok(results)
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

    /// Allocate a contiguous run of `count` page ids.
    ///
    /// Fast path reuses a contiguous run from the free list. Reclaim
    /// appends sorted page ids, so checkpoint-local allocation runs can
    /// recycle recently freed runs instead of monotonically pushing
    /// `high_water` forward. The common case is a contiguous LIFO
    /// suffix; if that suffix is fragmented, scan backward for an older
    /// contiguous run before extending the file.
    pub fn allocate_run(&self, count: usize) -> Result<PageId> {
        if count == 0 {
            return Err(MetaDbError::InvalidArgument(
                "allocate_run requires count > 0".into(),
            ));
        }
        let count_usize = count;
        let count = u64::try_from(count)
            .map_err(|_| MetaDbError::InvalidArgument("page run too large".into()))?;
        let mut inner = self.inner.lock();
        if let Some(start) = take_contiguous_free_run(&mut inner.free_list, count_usize) {
            return Ok(start);
        }
        let start = inner.high_water;
        let new_high = inner
            .high_water
            .checked_add(count)
            .ok_or(MetaDbError::OutOfSpace)?;
        self.ensure_file_covers(&mut inner, new_high)?;
        inner.high_water = new_high;
        Ok(start)
    }

    /// Allocate up to `count` page ids as a local scratch batch.
    ///
    /// Unlike [`allocate_run`](Self::allocate_run), this does not require
    /// reused pages to be contiguous. Hot COW writers only need a small pool
    /// of fresh page ids; forcing that pool to come from a 256-page contiguous
    /// free-list span strands fragmented reclaimed pages and keeps pushing the
    /// high-water mark forward under random-write workloads.
    ///
    /// The returned vector is ordered as a stack for callers that consume it
    /// with `pop()`: reclaimed free-list pages are returned before newly
    /// extended tail pages.
    pub fn allocate_batch(&self, count: usize) -> Result<Vec<PageId>> {
        if count == 0 {
            return Err(MetaDbError::InvalidArgument(
                "allocate_batch requires count > 0".into(),
            ));
        }
        let mut inner = self.inner.lock();
        let reuse = count.min(inner.free_list.len());
        let mut reused = Vec::with_capacity(reuse);
        for _ in 0..reuse {
            // LIFO keeps the free-list's existing cache-locality behaviour.
            if let Some(page_id) = inner.free_list.pop() {
                reused.push(page_id);
            }
        }

        let missing = count - reused.len();
        let mut pages = Vec::with_capacity(count);
        if missing > 0 {
            let missing_u64 = u64::try_from(missing)
                .map_err(|_| MetaDbError::InvalidArgument("page batch too large".into()))?;
            let start = inner.high_water;
            let new_high = inner
                .high_water
                .checked_add(missing_u64)
                .ok_or(MetaDbError::OutOfSpace)?;
            self.ensure_file_covers(&mut inner, new_high)?;
            inner.high_water = new_high;
            // Store new tail pages in reverse so `pop()` yields ascending ids.
            pages.extend((start..new_high).rev());
        }
        // Appended last so `pop()` consumes reclaimed pages before growing
        // into the newly extended tail.
        pages.extend(reused);
        Ok(pages)
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
    pub fn try_reclaim(&self) -> Result<ReclaimOutcome> {
        self.try_reclaim_limit(usize::MAX)
    }

    /// Budgeted variant of [`try_reclaim`]. Reclaims at most `max_pages`
    /// safe entries so latency-sensitive callers can make progress
    /// without turning one checkpoint into an unbounded free storm.
    pub fn try_reclaim_limit(&self, max_pages: usize) -> Result<ReclaimOutcome> {
        if max_pages == 0 {
            return Ok(ReclaimOutcome::default());
        }
        let safe_below = self.epoch.min_active_pin();
        let mut deferred = self.deferred_free.lock();
        if deferred.is_empty() {
            return Ok(ReclaimOutcome {
                safe_below,
                ..ReclaimOutcome::default()
            });
        }
        let selected: Vec<(PageId, DeferredFree)> = deferred
            .iter()
            .filter_map(|(pid, entry)| {
                if entry.epoch < safe_below {
                    Some((*pid, *entry))
                } else {
                    None
                }
            })
            .take(max_pages)
            .collect();
        for (pid, _) in &selected {
            deferred.remove(pid);
        }
        drop(deferred);

        let mut reclaimable = Vec::with_capacity(selected.len());
        for (pid, entry) in selected {
            if entry.idempotent && self.is_already_free_on_disk(pid)? {
                continue;
            }
            reclaimable.push((pid, entry));
        }
        let reclaimed = self.reclaim_sorted_runs(&reclaimable)?;
        Ok(ReclaimOutcome {
            safe_below,
            selected: reclaimable.len(),
            reclaimed,
        })
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
            if end - idx >= MIN_PUNCH_HOLE_RUN_PAGES {
                self.punch_free_run(start, end - idx)?;
            }
            reclaimed.extend(pages[idx..end].iter().map(|(pid, _)| *pid));
            idx = end;
        }

        let mut inner = self.inner.lock();
        inner.free_list.extend(reclaimed.iter().copied());
        self.truncate_free_tail_locked(&mut inner)?;
        Ok(reclaimed)
    }

    fn truncate_free_tail_locked(&self, inner: &mut Inner) -> Result<()> {
        if inner.high_water <= FIRST_DATA_PAGE || inner.free_list.is_empty() {
            return Ok(());
        }
        let tail_page = inner.high_water - 1;
        if !inner.free_list.iter().any(|pid| *pid == tail_page) {
            return Ok(());
        }

        inner.free_list.sort_unstable();
        inner.free_list.dedup();
        let original_high_water = inner.high_water;
        while inner.high_water > FIRST_DATA_PAGE
            && inner
                .free_list
                .last()
                .is_some_and(|pid| *pid == inner.high_water - 1)
        {
            inner.free_list.pop();
            inner.high_water -= 1;
        }

        if inner.high_water < original_high_water {
            self.file.set_len(
                inner
                    .high_water
                    .checked_mul(PAGE_SIZE as u64)
                    .ok_or(MetaDbError::OutOfSpace)?,
            )?;
            inner.committed_file_pages = inner.high_water;
        }
        Ok(())
    }

    /// `fdatasync` the page file (content only).
    ///
    /// Routed through the central [`IoSubmitter`] so the fsync
    /// SQE serialises naturally behind every previously Ok'd
    /// `write_page` / `write_sealed_page_runs` op (each of those
    /// blocked on its own CQE before returning, so by the time the
    /// fsync op is dequeued the kernel has already received the
    /// bytes). When the submitter is unavailable, fall back to the
    /// `fdatasync(2)` syscall — which is per-fd, so it sees the same
    /// kernel state regardless of the path the writes took.
    pub fn sync(&self) -> Result<()> {
        let started = Instant::now();
        if !self.io_submitters.is_empty() {
            // Fan out `IORING_OP_FSYNC` to every submitter in parallel.
            // io_uring fsync only orders writes on the same ring, so a
            // single fsync via submitter[0] would NOT cover the writes
            // routed to submitter[1..]. Issuing in parallel + waiting
            // for all replies gives the same "every Ok'd write is
            // durable" guarantee the previous single-submitter path
            // gave, just spread across N rings.
            let receivers: Vec<_> = self
                .io_submitters
                .iter()
                .map(|sub| sub.submit_fsync_async())
                .collect::<Result<Vec<_>>>()?;
            let mut first_err: Option<MetaDbError> = None;
            for rx in receivers {
                match rx.recv() {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        if first_err.is_none() {
                            first_err = Some(err);
                        }
                    }
                    Err(_) => {
                        if first_err.is_none() {
                            first_err = Some(MetaDbError::Io(io::Error::other(
                                "io submitter dropped fsync reply",
                            )));
                        }
                    }
                }
            }
            if let Some(err) = first_err {
                return Err(err);
            }
        } else {
            self.file.sync_data()?;
        }
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_fsync(started.elapsed());
        }
        Ok(())
    }

    /// `fsync` the page file (content + metadata).
    pub fn sync_all(&self) -> Result<()> {
        let started = Instant::now();
        self.file.sync_all()?;
        if let Some(metrics) = self.metrics() {
            metrics.record_meta_io_fsync(started.elapsed());
        }
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

/// Group pid-sorted sealed pages into contiguous runs, capped at
/// `max_pages_per_run` so a single `IORING_OP_WRITEV` SQE doesn't
/// exceed IOV_MAX. Each run becomes one writev submission through the
/// centralised submitter.
fn coalesce_sealed_runs(
    pages: Vec<(PageId, Arc<Page>)>,
    max_pages_per_run: usize,
) -> Vec<(PageId, Vec<Arc<Page>>)> {
    let mut runs: Vec<(PageId, Vec<Arc<Page>>)> = Vec::new();
    for (pid, page) in pages {
        let extend = runs
            .last()
            .map(|(start, run_pages)| {
                let next_pid = start + run_pages.len() as PageId;
                pid == next_pid && run_pages.len() < max_pages_per_run
            })
            .unwrap_or(false);
        if extend {
            runs.last_mut().expect("checked extend").1.push(page);
        } else {
            runs.push((pid, vec![page]));
        }
    }
    runs
}

fn take_contiguous_free_run(free_list: &mut Vec<PageId>, count: usize) -> Option<PageId> {
    if count == 0 || free_list.len() < count {
        return None;
    }

    let mut run_end = free_list.len();
    while run_end >= count {
        let mut run_start = run_end - 1;
        while run_start > 0 && free_list[run_start - 1].checked_add(1) == Some(free_list[run_start])
        {
            run_start -= 1;
        }

        if run_end - run_start >= count {
            let take_start = run_end - count;
            let start = free_list[take_start];
            if take_start == free_list.len() - count {
                free_list.truncate(take_start);
            } else {
                free_list.drain(take_start..run_end);
            }
            return Some(start);
        }

        if run_start == 0 {
            break;
        }
        run_end = run_start;
    }

    None
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

fn page_read_worker_loop(file: File, rx: Receiver<PageReadRequest>) {
    #[cfg(target_os = "linux")]
    let read_uring = Mutex::new(new_read_uring());
    let mut batch = Vec::with_capacity(PAGE_READ_BATCH_MAX);
    loop {
        let first = match rx.recv() {
            Ok(req) => req,
            Err(_) => return,
        };
        batch.clear();
        batch.push(first);
        let deadline = Instant::now() + PAGE_READ_BATCH_WINDOW;
        loop {
            while batch.len() < PAGE_READ_BATCH_MAX {
                match rx.try_recv() {
                    Ok(req) => batch.push(req),
                    Err(_) => break,
                }
            }
            if batch.len() >= PAGE_READ_BATCH_MAX {
                break;
            }
            let now = Instant::now();
            if now >= deadline {
                break;
            }
            match rx.recv_timeout(deadline.saturating_duration_since(now)) {
                Ok(req) => batch.push(req),
                Err(_) => break,
            }
        }

        let page_ids: Vec<PageId> = batch.iter().map(|req| req.page_id).collect();
        #[cfg(target_os = "linux")]
        let pages_result = read_pages_raw(&file, &page_ids, Some(&read_uring));
        #[cfg(not(target_os = "linux"))]
        let pages_result = read_pages_raw(&file, &page_ids, None);

        match pages_result {
            Ok(pages) => {
                for (req, page) in batch.drain(..).zip(pages.into_iter()) {
                    let _ = req.reply.send(page.verify(req.page_id).map(|()| page));
                }
            }
            Err(_) => {
                for req in batch.drain(..) {
                    let result = read_page_raw(&file, req.page_id).and_then(|page| {
                        page.verify(req.page_id)?;
                        Ok(page)
                    });
                    let _ = req.reply.send(result);
                }
            }
        }
    }
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
fn write_sealed_pages_raw(
    file: &File,
    pages: Vec<(PageId, Arc<Page>)>,
    write_uring: Option<&Mutex<Option<IoUring>>>,
) -> Result<()> {
    if pages.is_empty() {
        return Ok(());
    }
    let Some(write_uring) = write_uring else {
        return PageStore::write_sealed_page_runs_pwrite(file, pages);
    };
    let mut guard = write_uring.lock();
    let Some(ring) = guard.as_mut() else {
        return PageStore::write_sealed_page_runs_pwrite(file, pages);
    };
    match write_sealed_pages_raw_uring(file, &pages, ring) {
        Ok(()) => Ok(()),
        Err(err) if is_uring_setup_error(&err) => {
            tracing::debug!(error = %err, "page_store sealed write io_uring failed; falling back to pwrite");
            *guard = None;
            PageStore::write_sealed_page_runs_pwrite(file, pages)
        }
        Err(err) => Err(err),
    }
}

#[cfg(not(target_os = "linux"))]
fn write_sealed_pages_raw(
    file: &File,
    pages: Vec<(PageId, Arc<Page>)>,
    _write_uring: Option<&()>,
) -> Result<()> {
    PageStore::write_sealed_page_runs_pwrite(file, pages)
}

#[cfg(target_os = "linux")]
struct SealedWriteRun {
    start_page: PageId,
    pages: Vec<Arc<Page>>,
    iovecs: Vec<libc::iovec>,
    total_len: usize,
}

#[cfg(target_os = "linux")]
impl SealedWriteRun {
    fn new(start_page: PageId) -> Self {
        Self {
            start_page,
            pages: Vec::with_capacity(MAX_SEALED_WRITE_RUN_PAGES),
            iovecs: Vec::with_capacity(MAX_SEALED_WRITE_RUN_PAGES),
            total_len: 0,
        }
    }

    fn push(&mut self, page: Arc<Page>) {
        self.iovecs.push(libc::iovec {
            iov_base: page.bytes().as_ptr() as *mut libc::c_void,
            iov_len: PAGE_SIZE,
        });
        self.pages.push(page);
        self.total_len += PAGE_SIZE;
    }
}

#[cfg(target_os = "linux")]
fn sealed_writev_runs(pages: &[(PageId, Arc<Page>)]) -> Vec<SealedWriteRun> {
    let mut runs = Vec::new();
    let mut current: Option<SealedWriteRun> = None;
    let mut run_next = 0;

    for (pid, page) in pages {
        let run_pages = current.as_ref().map(|run| run.pages.len()).unwrap_or(0);
        if current.is_some() && (*pid != run_next || run_pages >= MAX_SEALED_WRITE_RUN_PAGES) {
            runs.push(current.take().expect("current run checked Some"));
        }
        if current.is_none() {
            current = Some(SealedWriteRun::new(*pid));
        }
        current
            .as_mut()
            .expect("current run created above")
            .push(page.clone());
        run_next = *pid + 1;
    }

    if let Some(run) = current {
        runs.push(run);
    }
    runs
}

#[cfg(target_os = "linux")]
fn write_sealed_pages_raw_uring(
    file: &File,
    pages: &[(PageId, Arc<Page>)],
    ring: &mut IoUring,
) -> Result<()> {
    let runs = sealed_writev_runs(pages);
    if runs.is_empty() {
        return Ok(());
    }
    let fd = file.as_raw_fd();
    let chunk_count = runs.len().div_ceil(MAX_SEALED_WRITE_URING_RUNS);
    for (base, chunk) in runs.chunks(MAX_SEALED_WRITE_URING_RUNS).enumerate() {
        for (idx, run) in chunk.iter().enumerate() {
            if run.iovecs.is_empty() {
                return Err(MetaDbError::InvalidArgument(
                    "page sealed writev run cannot be empty".into(),
                ));
            }
            let iovcnt = u32::try_from(run.iovecs.len()).map_err(|_| {
                MetaDbError::InvalidArgument(format!(
                    "page sealed writev run too large: {} iovecs",
                    run.iovecs.len()
                ))
            })?;
            let entry = opcode::Writev::new(types::Fd(fd), run.iovecs.as_ptr(), iovcnt)
                .offset(run.start_page * PAGE_SIZE as u64)
                .build()
                .user_data(idx as u64);
            let mut sq = ring.submission();
            // SAFETY: SQEs borrow iovec arrays and page byte buffers owned
            // by `runs`; both stay alive and immutable until every CQE for
            // this chunk has been harvested below.
            unsafe {
                sq.push(&entry).map_err(|_| {
                    MetaDbError::Io(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "page_store sealed writev io_uring submission queue full at chunk {base} op {idx}/{}",
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
                        "page_store sealed writev CQE user_data {idx} out of range (batch size {})",
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
                    "page_store sealed writev harvested {harvested} CQEs, expected {}",
                    chunk.len()
                ),
            )));
        }
        drop(cq);

        for (idx, result) in results.into_iter().enumerate() {
            let result = result.ok_or_else(|| {
                MetaDbError::Io(io::Error::new(
                    io::ErrorKind::Other,
                    format!("page_store sealed writev missing CQE for op {idx}"),
                ))
            })?;
            if result < 0 {
                return Err(MetaDbError::Io(io::Error::from_raw_os_error(-result)));
            }
            if result as usize != chunk[idx].total_len {
                return Err(MetaDbError::Io(io::Error::new(
                    io::ErrorKind::WriteZero,
                    format!(
                        "page_store sealed writev short write at page {}: got {result} of {}",
                        chunk[idx].start_page, chunk[idx].total_len
                    ),
                )));
            }
        }
        request_writeback_for_sealed_runs(file, chunk);
        if base + 1 < chunk_count {
            std::thread::yield_now();
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn request_writeback_for_sealed_runs(file: &File, runs: &[SealedWriteRun]) {
    let Some(first) = runs.first() else {
        return;
    };
    let start = first.start_page.saturating_mul(PAGE_SIZE as u64);
    let end = runs
        .iter()
        .map(|run| {
            run.start_page
                .saturating_mul(PAGE_SIZE as u64)
                .saturating_add(run.total_len as u64)
        })
        .max()
        .unwrap_or(start);
    let len = end.saturating_sub(start);
    if len == 0 {
        return;
    }
    let rc = unsafe {
        libc::sync_file_range(
            file.as_raw_fd(),
            start as libc::off64_t,
            len as libc::off64_t,
            libc::SYNC_FILE_RANGE_WRITE,
        )
    };
    if rc != 0 {
        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::ENOSYS) | Some(libc::EINVAL) | Some(libc::EOPNOTSUPP) => {}
            _ => tracing::debug!(error = %err, "page_store sync_file_range writeback hint failed"),
        }
    }
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
    fn fast_open_preserves_reads_without_rebuilding_free_list() {
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
        let ps = PageStore::open_fast(&path).unwrap();
        let file_pages = std::fs::metadata(&path).unwrap().len() / PAGE_SIZE as u64;
        assert_eq!(ps.high_water(), file_pages);
        assert_eq!(ps.free_list_len(), 0);
        let r = ps.read_page(FIRST_DATA_PAGE + 2).unwrap();
        assert_eq!(r.payload()[0], 2);
        // Fast open does not spend startup time discovering interior free
        // pages; fresh allocations move forward from EOF.
        let pid = ps.allocate().unwrap();
        assert_eq!(pid, file_pages);
    }

    #[test]
    fn try_reclaim_recycles_freed_pids() {
        // Deferred-free means three free calls + one try_reclaim batch
        // hands every pid back to the free list. We assert the SET of
        // recycled pids and that no allocation bumped past the original
        // high-water.
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
        assert_eq!(reclaimed.reclaimed.len(), 3);
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
    fn allocate_does_not_reclaim_deferred_pages_by_itself() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        let a = ps.allocate().unwrap();
        let b = ps.allocate().unwrap();
        ps.write_page(a, &mk_page(1, 0)).unwrap();
        ps.free(a, 10).unwrap();

        assert_eq!(ps.free_list_len(), 0);
        assert_eq!(ps.deferred_free_len(), 1);

        let next = ps.allocate().unwrap();
        assert_eq!(
            next,
            b + 1,
            "PageStore allocation must not reclaim pages behind Db's cache invalidation"
        );
        assert_eq!(ps.deferred_free_len(), 1);
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
    fn allocate_run_leaves_fragmented_free_list_entries() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        // Seed the free list with one interior individual page. Tail
        // free pages are truncated now, so keep a live page after it.
        let pid = ps.allocate().unwrap();
        let live_tail = ps.allocate().unwrap();
        ps.write_page(pid, &mk_page(1, 0)).unwrap();
        ps.write_page(live_tail, &mk_page(1, 1)).unwrap();
        ps.free(pid, 1).unwrap();
        ps.try_reclaim().unwrap();
        assert_eq!(ps.free_list_len(), 1);
        // A single free page cannot satisfy a larger contiguous run.
        let start = ps.allocate_run(4).unwrap();
        assert_eq!(start, live_tail + 1);
        assert_eq!(ps.free_list_len(), 1);
    }

    #[test]
    fn allocate_batch_reuses_fragmented_free_list_entries() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        let start = ps.allocate_run(8).unwrap();
        for i in 0..8 {
            ps.write_page(start + i, &mk_page(1, i as u8)).unwrap();
        }
        ps.free_many(&[start, start + 2, start + 5], 99).unwrap();
        ps.try_reclaim().unwrap();
        assert_eq!(ps.free_list_len(), 3);
        let high_water = ps.high_water();

        let mut batch = ps.allocate_batch(3).unwrap();
        batch.sort_unstable();
        assert_eq!(batch, vec![start, start + 2, start + 5]);
        assert_eq!(ps.high_water(), high_water);
        assert_eq!(ps.free_list_len(), 0);
    }

    #[test]
    fn allocate_batch_extends_only_for_missing_pages() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        let start = ps.allocate_run(4).unwrap();
        let live_tail = ps.allocate().unwrap();
        for i in 0..4 {
            ps.write_page(start + i, &mk_page(1, i as u8)).unwrap();
        }
        ps.write_page(live_tail, &mk_page(1, 9)).unwrap();
        ps.free_many(&[start + 1, start + 3], 99).unwrap();
        ps.try_reclaim().unwrap();
        let high_water = ps.high_water();

        let batch = ps.allocate_batch(5).unwrap();
        assert_eq!(batch.len(), 5);
        assert_eq!(ps.high_water(), high_water + 3);
        assert_eq!(ps.free_list_len(), 0);
    }

    #[test]
    fn allocate_run_reuses_contiguous_free_list_suffix() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        let start = ps.allocate_run(4).unwrap();
        let live_tail = ps.allocate().unwrap();
        for i in 0..4 {
            ps.write_page(start + i, &mk_page(1, i as u8)).unwrap();
        }
        ps.write_page(live_tail, &mk_page(1, 9)).unwrap();
        ps.free_run(start, 4, 99).unwrap();
        ps.try_reclaim().unwrap();
        assert_eq!(ps.free_list_len(), 4);

        let reused = ps.allocate_run(4).unwrap();
        assert_eq!(reused, start);
        assert_eq!(ps.free_list_len(), 0);
        assert_eq!(ps.high_water(), live_tail + 1);
    }

    #[test]
    fn allocate_run_reuses_contiguous_run_before_fragmented_tail() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        let run_start = ps.allocate_run(3).unwrap();
        let gap = ps.allocate().unwrap();
        let tail = ps.allocate().unwrap();
        for (idx, pid) in [run_start, run_start + 1, run_start + 2, gap, tail]
            .into_iter()
            .enumerate()
        {
            ps.write_page(pid, &mk_page(1, idx as u8)).unwrap();
        }
        ps.free_run(run_start, 3, 99).unwrap();
        ps.try_reclaim().unwrap();
        ps.free(tail, 100).unwrap();
        ps.try_reclaim().unwrap();
        assert_eq!(ps.free_list_len(), 3);

        let reused = ps.allocate_run(3).unwrap();
        assert_eq!(reused, run_start);
        assert_eq!(ps.free_list_len(), 0);
        assert_eq!(ps.high_water(), tail);
    }

    #[test]
    fn free_run_returns_pages_to_free_list() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        let start = ps.allocate_run(3).unwrap();
        let live_tail = ps.allocate().unwrap();
        for i in 0..3 {
            ps.write_page(start + i, &mk_page(1, 0)).unwrap();
        }
        ps.write_page(live_tail, &mk_page(1, 9)).unwrap();
        ps.free_run(start, 3, 99).unwrap();
        ps.try_reclaim().unwrap();
        assert_eq!(ps.free_list_len(), 3);
    }

    #[test]
    fn reclaim_truncates_contiguous_free_tail() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        let start = ps.allocate_run(5).unwrap();
        for i in 0..5 {
            ps.write_page(start + i, &mk_page(1, i as u8)).unwrap();
        }
        assert_eq!(ps.high_water(), start + 5);

        ps.free_run(start + 2, 3, 99).unwrap();
        let reclaimed = ps.try_reclaim().unwrap();
        assert_eq!(reclaimed.reclaimed, vec![start + 2, start + 3, start + 4]);
        assert_eq!(ps.high_water(), start + 2);
        assert_eq!(ps.free_list_len(), 0);
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            (start + 2) * PAGE_SIZE as u64
        );
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
