use super::*;
use device::FileDevice;
use std::sync::atomic::{AtomicU64, AtomicUsize};

impl PageStore {
    /// Assemble a store from an already-constructed device plus recovered
    /// bookkeeping. `path` is a diagnostic label only.
    pub(super) fn from_parts(
        path: PathBuf,
        device: Arc<dyn PageDevice>,
        high_water: u64,
        free_list: Vec<PageId>,
    ) -> Self {
        let free_list_pages = free_list.len();
        Self {
            path,
            device,
            inner: Mutex::new(Inner {
                high_water,
                free_list,
            }),
            high_water_pages: AtomicU64::new(high_water),
            free_list_pages: AtomicUsize::new(free_list_pages),
            deferred_free_pages: AtomicUsize::new(0),
            epoch: Arc::new(EpochManager::new()),
            deferred_free: Mutex::new(BTreeMap::new()),
            metrics: OnceLock::new(),
        }
    }

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
        Self::create_with_grow_chunk_and_bg_cap(
            path,
            grow_chunk,
            crate::io_submitter::DEFAULT_BG_INFLIGHT_CAP,
        )
    }

    /// As [`Self::create_with_grow_chunk`] but with an explicit cap on
    /// background-priority ops in flight at the centralised submitter.
    /// 0 disables the cap (admits bg ops freely).
    pub fn create_with_grow_chunk_and_bg_cap(
        path: impl AsRef<Path>,
        grow_chunk: u64,
        bg_inflight_cap: usize,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let device = FileDevice::create(&path, grow_chunk, bg_inflight_cap)?;
        Ok(Self::from_parts(
            path,
            Arc::new(device),
            FIRST_DATA_PAGE,
            Vec::new(),
        ))
    }

    /// Fast-open an existing page store with the caller's grow chunk. See
    /// [`open_fast`](Self::open_fast) for the tradeoff.
    pub fn open_fast_with_grow_chunk(path: impl AsRef<Path>, grow_chunk: u64) -> Result<Self> {
        Self::open_fast_with_grow_chunk_and_bg_cap(
            path,
            grow_chunk,
            crate::io_submitter::DEFAULT_BG_INFLIGHT_CAP,
        )
    }

    /// As [`Self::open_fast_with_grow_chunk`] but with an explicit cap
    /// on background-priority ops in flight at the centralised submitter.
    /// 0 disables the cap (admits bg ops freely).
    pub fn open_fast_with_grow_chunk_and_bg_cap(
        path: impl AsRef<Path>,
        grow_chunk: u64,
        bg_inflight_cap: usize,
    ) -> Result<Self> {
        let open_started = std::time::Instant::now();
        let path = path.as_ref().to_path_buf();
        let (device, file_end_pages) =
            FileDevice::open_existing(&path, grow_chunk, bg_inflight_cap)?;
        tracing::info!(
            path = %path.display(),
            high_water_pages = file_end_pages,
            elapsed_ms = open_started.elapsed().as_millis(),
            "metadb page store fast-open complete"
        );
        Ok(Self::from_parts(
            path,
            Arc::new(device),
            file_end_pages,
            Vec::new(),
        ))
    }

    /// Open an existing page store. `grow_chunk` is the batch size used
    /// for subsequent file extensions (does not affect the scan). The
    /// scan rebuilds the in-memory free list by walking pages from
    /// [`FIRST_DATA_PAGE`] to EOF; any contiguous zero-init tail left
    /// over from a crashed pre-extend is truncated back before the
    /// store is returned.
    pub fn open_with_grow_chunk(path: impl AsRef<Path>, grow_chunk: u64) -> Result<Self> {
        Self::open_with_grow_chunk_and_bg_cap(
            path,
            grow_chunk,
            crate::io_submitter::DEFAULT_BG_INFLIGHT_CAP,
        )
    }

    /// As [`Self::open_with_grow_chunk`] but with an explicit cap on
    /// background-priority ops in flight at the centralised submitter.
    /// 0 disables the cap (admits bg ops freely).
    pub fn open_with_grow_chunk_and_bg_cap(
        path: impl AsRef<Path>,
        grow_chunk: u64,
        bg_inflight_cap: usize,
    ) -> Result<Self> {
        let open_started = std::time::Instant::now();
        let path = path.as_ref().to_path_buf();
        let (device, file_end_pages) =
            FileDevice::open_existing(&path, grow_chunk, bg_inflight_cap)?;
        // Walk every page in [FIRST_DATA_PAGE, file_end_pages). Typed pages
        // extend the recovered `high_water`; Free pages and all-zero punched
        // holes are reusable. A zero suffix past the last typed page is
        // growth tail and is truncated below.
        let (high_water, free_list) = scan_free_list(&device, FIRST_DATA_PAGE, file_end_pages)?;
        if high_water < file_end_pages {
            device.truncate_to(high_water)?;
        }
        tracing::info!(
            path = %path.display(),
            scanned_pages = file_end_pages.saturating_sub(FIRST_DATA_PAGE),
            high_water_pages = high_water,
            free_list_pages = free_list.len(),
            elapsed_ms = open_started.elapsed().as_millis(),
            "metadb page store open scan complete"
        );
        Ok(Self::from_parts(path, Arc::new(device), high_water, free_list))
    }
}

/// Scan pages `[scan_from, scan_to)` on `device` to recover the high-water
/// mark and the reusable free list. Typed pages push `high_water` past
/// them; `Free`-stamped pages and all-zero (punched / never-written) pages
/// join the free list. Torn pages are left in place below high_water; the
/// verifier flags them later.
pub(super) fn scan_free_list(
    device: &dyn PageDevice,
    scan_from: PageId,
    scan_to: PageId,
) -> Result<(u64, Vec<PageId>)> {
    let mut high_water = scan_from;
    let mut free_list = Vec::new();
    for page_id in scan_from..scan_to {
        let page = device.read_page_unchecked(page_id)?;
        if let Ok(h) = page.header() {
            high_water = page_id + 1;
            if h.page_type == PageType::Free {
                free_list.push(page_id);
            }
        } else if is_zero_page(&page) {
            free_list.push(page_id);
        }
    }
    free_list.retain(|pid| *pid < high_water);
    Ok((high_water, free_list))
}
