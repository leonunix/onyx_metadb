//! Physical backing device for the page store.
//!
//! The page store's bookkeeping (free list, high-water mark, deferred-free
//! epoch reclaim) is device-independent; the *bytes* live behind a
//! [`PageDevice`]. Two shapes exist:
//!
//! * [`FileDevice`] — a flat file plus the full fd / io_uring submitter /
//!   read-pool / hole-punch / `set_len` machinery. This is the default path
//!   and is byte-for-byte identical to the pre-refactor page store (the
//!   only intended semantic change lands later, when the grow path moves
//!   from `set_len` to `fallocate`).
//! * A fixed-capacity block window (implemented by the onyx side over a
//!   chunklet LogicalDisk). It reports `capacity_pages() = Some(n)`, so the
//!   file's variable-length semantics (`set_len` growth/shrink, hole punch,
//!   "file length is the recovery bound") degrade to bounds-checks and
//!   no-ops. See the Phase 3 plan.
//!
//! # Concurrency
//!
//! Read / write / sync methods take `&self` and issue positional IO; they
//! are safe under concurrent callers (each op is atomic at the kernel /
//! device level). Only the physical length (`committed_pages`) is guarded
//! by an internal mutex, taken as a leaf under the page store's `inner`
//! lock — never the reverse.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use parking_lot::Mutex;

#[cfg(target_os = "linux")]
use io_uring::IoUring;

use crate::config::PAGE_SIZE;
use crate::error::{MetaDbError, Result};
use crate::io_submitter::{IoPriority, IoSubmitter};
use crate::metrics::MetaMetrics;
use crate::page::Page;
use crate::types::{FIRST_DATA_PAGE, PageId};

use super::raw_io::{
    MAX_SEALED_WRITE_RUN_PAGES, coalesce_sealed_runs, new_read_uring, new_write_uring, punch_hole,
    read_page_raw, read_pages_raw, write_page_runs_raw, write_sealed_pages_raw,
};
use super::read_pool::PageReadPool;
use super::submitter::{DEFAULT_IO_SUBMITTER_POOL_SIZE, IoLaneClass, make_io_submitters};

/// The bytes behind a page store.
///
/// Offsets are always page-aligned (`page_id * PAGE_SIZE`); implementers may
/// assume 4 KiB granularity. Read verification is split to mirror the
/// existing file path exactly: [`read_page`](PageDevice::read_page) returns a
/// verified page (it may go through a read pool), while
/// [`read_pages`](PageDevice::read_pages) and
/// [`read_page_unchecked`](PageDevice::read_page_unchecked) return raw bytes
/// that the caller verifies.
pub trait PageDevice: Send + Sync {
    /// Verified single-page read.
    fn read_page(&self, page_id: PageId) -> Result<Page>;

    /// Batched read; pages are returned **unverified** (the caller runs
    /// [`Page::verify`]). On Linux the file path uses one io_uring submit
    /// per chunk to raise device queue depth.
    fn read_pages(&self, page_ids: &[PageId]) -> Result<Vec<Page>>;

    /// Raw single-page read, no verification. Used by recovery / the open
    /// scan / `free_idempotent`, which must inspect possibly-bad pages.
    fn read_page_unchecked(&self, page_id: PageId) -> Result<Page>;

    /// Write one already-sealed page, routed by lane class.
    fn write_page(&self, page_id: PageId, page: &Page, class: IoLaneClass) -> Result<()>;

    /// Write a contiguous run of already-sealed page bytes at `start_page`.
    /// `bytes.len()` is a non-zero multiple of [`PAGE_SIZE`].
    fn write_page_run_bytes(&self, start_page: PageId, bytes: &[u8]) -> Result<()>;

    /// Write multiple already-sealed page runs.
    fn write_page_runs_parallel(&self, runs: Vec<(PageId, Vec<u8>)>) -> Result<()>;

    /// Write already-sealed pages, coalescing contiguous ids into runs,
    /// routed by lane class + priority.
    fn write_sealed_page_runs(
        &self,
        pages: Vec<(PageId, Arc<Page>)>,
        class: IoLaneClass,
        priority: IoPriority,
    ) -> Result<()>;

    /// Content-only durability fence (`fdatasync` / device flush).
    fn sync(&self) -> Result<()>;

    /// Content + metadata fence (`fsync`). On a fixed-capacity device this
    /// is equivalent to [`sync`](PageDevice::sync).
    fn sync_all(&self) -> Result<()>;

    /// Current physical length in pages. For a file this is the on-disk
    /// length (the open-scan upper bound); for a fixed device it is the
    /// capacity.
    fn len_pages(&self) -> Result<u64>;

    /// Ensure the backing store physically covers at least `target_pages`.
    /// A file extends (rounded to the grow chunk); a fixed device
    /// bounds-checks and returns [`MetaDbError::CapacityExhausted`] past
    /// capacity.
    fn ensure_covers(&self, target_pages: u64) -> Result<()>;

    /// Shrink the physical backing to exactly `pages`. A file `set_len`s;
    /// a fixed device is a no-op (capacity is immutable).
    fn truncate_to(&self, pages: u64) -> Result<()>;

    /// Punch a hole over the page run `[start_page, start_page + count)`.
    /// A file uses `FALLOC_FL_PUNCH_HOLE`; a fixed device is a no-op (the
    /// `Free`-typed page stamp is the reuse marker there).
    fn punch_run(&self, start_page: PageId, count: usize) -> Result<()>;

    /// `Some(n)` for a fixed-capacity device, `None` for a growable file.
    fn capacity_pages(&self) -> Option<u64>;

    /// Attach the parent `Db`'s metrics handle (fans out to any io_uring
    /// submitters so their loop counters record).
    fn attach_metrics(&self, metrics: Arc<MetaMetrics>);
}

/// File-backed [`PageDevice`]: a flat file plus the fd / io_uring submitter
/// pool / read pool / hole-punch / batched-`set_len` machinery.
pub struct FileDevice {
    file: File,
    read_pool: PageReadPool,
    /// Pool of io_uring submitters. Each entry owns its own ring + worker
    /// thread; concurrent writers from different shards / background
    /// workers don't share an SQ. Empty when io_uring is unavailable
    /// (old kernel, sandbox) → callers fall back to pwrite + fdatasync.
    io_submitters: Box<[IoSubmitter]>,
    #[cfg(target_os = "linux")]
    read_uring: Mutex<Option<IoUring>>,
    #[cfg(target_os = "linux")]
    write_uring: Mutex<Option<IoUring>>,
    /// Batch size for pre-extending the backing file in `ensure_covers`.
    /// Frozen at construction.
    grow_chunk: u64,
    /// File length in pages. `file.metadata().len() == committed_pages *
    /// PAGE_SIZE` at all times outside `ensure_covers` / `truncate_to`.
    committed_pages: Mutex<u64>,
}

impl FileDevice {
    /// Create a brand-new backing file. Fails if it already exists.
    pub(super) fn create(path: &Path, grow_chunk: u64, bg_inflight_cap: usize) -> Result<Self> {
        if grow_chunk == 0 {
            return Err(MetaDbError::InvalidArgument(
                "page store grow_chunk must be >= 1".into(),
            ));
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        // Pre-size to FIRST_DATA_PAGE so the manifest slot offsets are
        // immediately addressable, even though those pages start zeroed
        // (the manifest layer populates them). The first data allocation
        // pre-extends to FIRST_DATA_PAGE + grow_chunk.
        file.set_len(FIRST_DATA_PAGE * PAGE_SIZE as u64)?;
        let read_pool = PageReadPool::start(&file)?;
        let io_submitters =
            make_io_submitters(&file, DEFAULT_IO_SUBMITTER_POOL_SIZE, bg_inflight_cap);
        Ok(Self {
            file,
            read_pool,
            io_submitters,
            #[cfg(target_os = "linux")]
            read_uring: Mutex::new(new_read_uring()),
            #[cfg(target_os = "linux")]
            write_uring: Mutex::new(new_write_uring()),
            grow_chunk,
            committed_pages: Mutex::new(FIRST_DATA_PAGE),
        })
    }

    /// Open an existing backing file. Validates the length is page-aligned
    /// and covers the reserved manifest region, then reports the current
    /// length in pages so the page store can scan / trust it.
    pub(super) fn open_existing(
        path: &Path,
        grow_chunk: u64,
        bg_inflight_cap: usize,
    ) -> Result<(Self, u64)> {
        if grow_chunk == 0 {
            return Err(MetaDbError::InvalidArgument(
                "page store grow_chunk must be >= 1".into(),
            ));
        }
        let file = OpenOptions::new().read(true).write(true).open(path)?;
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
        let io_submitters =
            make_io_submitters(&file, DEFAULT_IO_SUBMITTER_POOL_SIZE, bg_inflight_cap);
        let dev = Self {
            file,
            read_pool,
            io_submitters,
            #[cfg(target_os = "linux")]
            read_uring: Mutex::new(new_read_uring()),
            #[cfg(target_os = "linux")]
            write_uring: Mutex::new(new_write_uring()),
            grow_chunk,
            committed_pages: Mutex::new(file_end_pages),
        };
        Ok((dev, file_end_pages))
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

    /// Pick an [`IoSubmitter`] for a write of `pid` by hash routing.
    /// Returns `None` when io_uring is unavailable.
    fn io_submitter_for(&self, pid: PageId) -> Option<&IoSubmitter> {
        if self.io_submitters.is_empty() {
            None
        } else {
            let idx = (pid as usize) % self.io_submitters.len();
            Some(&self.io_submitters[idx])
        }
    }

    /// Pick the [`IoSubmitter`] reserved for `class`. Returns `None` when
    /// io_uring is unavailable or the pool is smaller than expected; the
    /// caller drops to pwrite.
    fn io_submitter_for_class(&self, class: IoLaneClass) -> Option<&IoSubmitter> {
        self.io_submitters.get(class.index())
    }
}

impl PageDevice for FileDevice {
    fn read_page(&self, page_id: PageId) -> Result<Page> {
        self.read_pool.read_page(page_id)
    }

    fn read_pages(&self, page_ids: &[PageId]) -> Result<Vec<Page>> {
        read_pages_raw(&self.file, page_ids, self.read_uring())
    }

    fn read_page_unchecked(&self, page_id: PageId) -> Result<Page> {
        read_page_raw(&self.file, page_id)
    }

    fn write_page(&self, page_id: PageId, page: &Page, class: IoLaneClass) -> Result<()> {
        if let Some(submitter) = self.io_submitter_for_class(class) {
            submitter.submit_write(page_id, Arc::new(page.clone()))
        } else if let Some(submitter) = self.io_submitter_for(page_id) {
            submitter.submit_write(page_id, Arc::new(page.clone()))
        } else {
            self.file
                .write_all_at(page.bytes(), page_id * PAGE_SIZE as u64)?;
            Ok(())
        }
    }

    fn write_page_run_bytes(&self, start_page: PageId, bytes: &[u8]) -> Result<()> {
        self.file
            .write_all_at(bytes, start_page * PAGE_SIZE as u64)?;
        Ok(())
    }

    fn write_page_runs_parallel(&self, runs: Vec<(PageId, Vec<u8>)>) -> Result<()> {
        write_page_runs_raw(&self.file, runs, self.write_uring())
    }

    fn write_sealed_page_runs(
        &self,
        mut pages: Vec<(PageId, Arc<Page>)>,
        class: IoLaneClass,
        priority: IoPriority,
    ) -> Result<()> {
        if pages.is_empty() {
            return Ok(());
        }
        pages.sort_unstable_by_key(|(pid, _)| *pid);
        // Route through the lane-class submitter so dedup / refcount / L2p
        // write streams cannot saturate one another's SQ. Falls back to
        // hash routing when the pool is smaller than expected, and to
        // pwrite when io_uring is unavailable.
        let class_submitter = self.io_submitter_for_class(class);
        if class_submitter.is_some() || !self.io_submitters.is_empty() {
            let runs = coalesce_sealed_runs(pages, MAX_SEALED_WRITE_RUN_PAGES);
            let receivers: Vec<_> = runs
                .into_iter()
                .map(|(start, run_pages)| {
                    let submitter = class_submitter
                        .or_else(|| self.io_submitter_for(start))
                        .expect("io_submitters non-empty above");
                    submitter.submit_write_run_async_with_priority(start, run_pages, priority)
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
        Ok(())
    }

    fn sync(&self) -> Result<()> {
        if !self.io_submitters.is_empty() {
            // Fan out `IORING_OP_FSYNC` to every submitter in parallel:
            // io_uring fsync only orders writes on the same ring, so a
            // single fsync via submitter[0] would not cover writes routed
            // to submitter[1..].
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
        Ok(())
    }

    fn sync_all(&self) -> Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    fn len_pages(&self) -> Result<u64> {
        Ok(*self.committed_pages.lock())
    }

    fn ensure_covers(&self, target_pages: u64) -> Result<()> {
        let mut committed = self.committed_pages.lock();
        if target_pages <= *committed {
            return Ok(());
        }
        // Round target up to the next grow_chunk boundary.
        let chunk = self.grow_chunk;
        let span = target_pages
            .checked_sub(*committed)
            .expect("target > committed by the early return above");
        let chunks_needed = span.div_ceil(chunk);
        let add = chunks_needed
            .checked_mul(chunk)
            .ok_or(MetaDbError::OutOfSpace)?;
        let new_committed = committed.checked_add(add).ok_or(MetaDbError::OutOfSpace)?;
        self.file.set_len(
            new_committed
                .checked_mul(PAGE_SIZE as u64)
                .ok_or(MetaDbError::OutOfSpace)?,
        )?;
        *committed = new_committed;
        Ok(())
    }

    fn truncate_to(&self, pages: u64) -> Result<()> {
        let mut committed = self.committed_pages.lock();
        if pages >= *committed {
            return Ok(());
        }
        self.file.set_len(
            pages
                .checked_mul(PAGE_SIZE as u64)
                .ok_or(MetaDbError::OutOfSpace)?,
        )?;
        *committed = pages;
        Ok(())
    }

    fn punch_run(&self, start_page: PageId, count: usize) -> Result<()> {
        punch_hole(
            &self.file,
            start_page * PAGE_SIZE as u64,
            (count * PAGE_SIZE) as u64,
        )
    }

    fn capacity_pages(&self) -> Option<u64> {
        None
    }

    fn attach_metrics(&self, metrics: Arc<MetaMetrics>) {
        for submitter in self.io_submitters.iter() {
            submitter.attach_metrics(Arc::clone(&metrics));
        }
    }
}
