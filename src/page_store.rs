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
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use crate::config::PAGE_SIZE;
use crate::epoch::EpochManager;
use crate::error::{MetaDbError, Result};
use crate::metrics::MetaMetrics;
use crate::page::{Page, PageHeader, PageType};
use crate::types::{FIRST_DATA_PAGE, Lsn, PageId};

mod accessors;
mod allocation;
mod device;
mod open;
mod page_io;
mod raw_io;
mod read_pool;
mod reclaim;
mod submitter;
mod sync;

pub use device::{BlockPageDevice, FileDevice, MemDevice, PageBlockIo, PageDevice};
use raw_io::{is_zero_page, take_contiguous_free_run};
pub use submitter::{DEFAULT_IO_SUBMITTER_POOL_SIZE, IoLaneClass};

const MAX_RECLAIM_RUN_PAGES: usize = 1024;
const MIN_PUNCH_HOLE_RUN_PAGES: usize = 16;

/// Default pre-extension chunk if the caller uses [`PageStore::create`]
/// / [`PageStore::open`] without threading a `Config`. Must stay in
/// sync with `Config::page_grow_chunk_pages`'s documented default.
pub const DEFAULT_GROW_CHUNK_PAGES: u64 = 512;

/// Flat page store over a pluggable [`PageDevice`].
///
/// The store owns only device-independent bookkeeping — free list,
/// high-water mark, deferred-free epoch reclaim, metrics. The physical
/// bytes (and the fd / io_uring / hole-punch / `set_len` machinery for the
/// file path) live behind [`Self::device`].
pub struct PageStore {
    /// Label for diagnostics (the file path, or a device identifier).
    path: PathBuf,
    /// Physical backing store. [`FileDevice`] by default; a fixed-capacity
    /// block window (onyx over chunklet) on the device path.
    device: Arc<dyn PageDevice>,
    inner: Mutex<Inner>,
    high_water_pages: AtomicU64,
    free_list_pages: AtomicUsize,
    deferred_free_pages: AtomicUsize,
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
    /// replay path. Kept as a provenance flag; reclaim does not branch
    /// on it. Crash-replay safety is provided upstream: every caller of
    /// `free_idempotent` (`apply_drop_snapshot_pages`) bypasses already-
    /// freed pages via the zero-page check and the `header.generation
    /// >= lsn` short-circuit before reaching this queue, and cross-
    /// process correctness comes from `open()` rebuilding `free_list`
    /// by scanning Free-typed pages on disk.
    #[allow(dead_code)]
    idempotent: bool,
}

#[derive(Debug, Default)]
pub struct ReclaimOutcome {
    pub safe_below: u64,
    pub selected: usize,
    pub reclaimed: Vec<PageId>,
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
    /// `<= device.len_pages()`; the gap is pre-extended zero-init growth
    /// tail (file) or unused capacity (device).
    high_water: u64,
    /// Explicitly-freed pages available for reuse. LIFO.
    free_list: Vec<PageId>,
}

impl PageStore {}

#[cfg(test)]
mod tests;
