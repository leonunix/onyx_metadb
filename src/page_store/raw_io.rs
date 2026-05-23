#[cfg(target_os = "linux")]
use parking_lot::Mutex;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;
use std::sync::Arc;

#[cfg(target_os = "linux")]
use io_uring::{IoUring, opcode, types};

use crate::config::PAGE_SIZE;
use crate::error::{MetaDbError, Result};
use crate::page::Page;
use crate::types::PageId;

pub(super) const MAX_SEALED_WRITE_RUN_PAGES: usize = 256;

#[cfg(target_os = "linux")]
const DEFAULT_READ_URING_ENTRIES: u32 = 128;
#[cfg(target_os = "linux")]
const DEFAULT_WRITE_URING_ENTRIES: u32 = 128;
#[cfg(target_os = "linux")]
const MAX_SEALED_WRITE_URING_RUNS: usize = 16;

/// Group pid-sorted sealed pages into contiguous runs, capped at
/// `max_pages_per_run` so a single `IORING_OP_WRITEV` SQE doesn't
/// exceed IOV_MAX. Each run becomes one writev submission through the
/// centralised submitter.
pub(super) fn coalesce_sealed_runs(
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

pub(super) fn take_contiguous_free_run(
    free_list: &mut Vec<PageId>,
    count: usize,
) -> Option<PageId> {
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

pub(super) fn read_page_raw(file: &File, page_id: PageId) -> Result<Page> {
    let mut page = Page::zeroed();
    file.read_exact_at(page.bytes_mut(), page_id * PAGE_SIZE as u64)?;
    Ok(page)
}

#[cfg(target_os = "linux")]
pub(super) fn new_read_uring() -> Option<IoUring> {
    match IoUring::new(DEFAULT_READ_URING_ENTRIES) {
        Ok(ring) => Some(ring),
        Err(err) => {
            tracing::debug!(error = %err, "page_store io_uring unavailable; falling back to pread");
            None
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub(super) fn new_read_uring() {}

#[cfg(target_os = "linux")]
pub(super) fn new_write_uring() -> Option<IoUring> {
    match IoUring::new(DEFAULT_WRITE_URING_ENTRIES) {
        Ok(ring) => Some(ring),
        Err(err) => {
            tracing::debug!(error = %err, "page_store write io_uring unavailable; falling back to pwrite");
            None
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub(super) fn new_write_uring() {}

#[cfg(target_os = "linux")]
pub(super) fn read_pages_raw(
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
pub(super) fn read_pages_raw(
    file: &File,
    page_ids: &[PageId],
    _read_uring: Option<&()>,
) -> Result<Vec<Page>> {
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
pub(super) fn write_page_runs_raw(
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
pub(super) fn write_page_runs_raw(
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
pub(super) fn write_sealed_pages_raw(
    file: &File,
    pages: Vec<(PageId, Arc<Page>)>,
    write_uring: Option<&Mutex<Option<IoUring>>>,
) -> Result<()> {
    if pages.is_empty() {
        return Ok(());
    }
    let Some(write_uring) = write_uring else {
        return super::PageStore::write_sealed_page_runs_pwrite(file, pages);
    };
    let mut guard = write_uring.lock();
    let Some(ring) = guard.as_mut() else {
        return super::PageStore::write_sealed_page_runs_pwrite(file, pages);
    };
    match write_sealed_pages_raw_uring(file, &pages, ring) {
        Ok(()) => Ok(()),
        Err(err) if is_uring_setup_error(&err) => {
            tracing::debug!(error = %err, "page_store sealed write io_uring failed; falling back to pwrite");
            *guard = None;
            super::PageStore::write_sealed_page_runs_pwrite(file, pages)
        }
        Err(err) => Err(err),
    }
}

#[cfg(not(target_os = "linux"))]
pub(super) fn write_sealed_pages_raw(
    file: &File,
    pages: Vec<(PageId, Arc<Page>)>,
    _write_uring: Option<&()>,
) -> Result<()> {
    super::PageStore::write_sealed_page_runs_pwrite(file, pages)
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

pub(super) fn is_zero_page(page: &Page) -> bool {
    page.bytes().iter().all(|b| *b == 0)
}

#[cfg(target_os = "linux")]
pub(super) fn punch_hole(file: &File, offset: u64, len: u64) -> Result<()> {
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
pub(super) fn punch_hole(_file: &File, _offset: u64, _len: u64) -> Result<()> {
    Ok(())
}
