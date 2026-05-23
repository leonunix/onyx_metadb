use super::*;

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
        let io_submitters =
            make_io_submitters(&file, DEFAULT_IO_SUBMITTER_POOL_SIZE, bg_inflight_cap);
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
        let io_submitters =
            make_io_submitters(&file, DEFAULT_IO_SUBMITTER_POOL_SIZE, bg_inflight_cap);
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
        let io_submitters =
            make_io_submitters(&file, DEFAULT_IO_SUBMITTER_POOL_SIZE, bg_inflight_cap);
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
}
