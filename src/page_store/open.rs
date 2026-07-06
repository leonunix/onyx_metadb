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

    /// Create a fresh page store over an already-constructed [`PageDevice`]
    /// (the fixed-capacity device path — onyx over a chunklet LogicalDisk).
    /// The device must cover at least the reserved manifest region.
    pub fn create_on_device(device: Arc<dyn PageDevice>) -> Result<Self> {
        device.ensure_covers(FIRST_DATA_PAGE)?;
        Ok(Self::from_parts(
            PathBuf::from("<device>"),
            device,
            FIRST_DATA_PAGE,
            Vec::new(),
        ))
    }

    /// Open a page store over an existing [`PageDevice`] **without** scanning:
    /// the high-water mark is set to the device capacity so the manifest and
    /// its catalog chains (which live anywhere below capacity) are addressable.
    /// The caller must load the manifest and then call
    /// [`rebuild_free_list_bounded`](Self::rebuild_free_list_bounded) with the
    /// manifest's `page_high_water` to recover the true frontier + free list.
    pub fn open_on_device(device: Arc<dyn PageDevice>) -> Result<Self> {
        let capacity = device.len_pages()?.max(FIRST_DATA_PAGE);
        Ok(Self::from_parts(
            PathBuf::from("<device>"),
            device,
            capacity,
            Vec::new(),
        ))
    }

    /// Rebuild the free list + high-water mark by a **bounded** scan of
    /// `[FIRST_DATA_PAGE, frontier)`. This is the device-path counterpart of
    /// the file open scan: a fixed device has no EOF to bound the walk and no
    /// hole-punch to reclaim leaked pages, so a recovered upper bound bounds the
    /// scan and recovers freed pages that a trust-capacity open would have
    /// leaked. Garbage from a prior tenant above the frontier is never touched.
    ///
    /// ⚠ **Caller contract:** `frontier` must be a strict upper bound on every
    /// page id ANY live durable structure references — NOT merely
    /// `manifest.page_high_water`. Every root recorded per-manifest-generation
    /// (refcount / L2P / catalog / deadlists) is already `< page_high_water` by
    /// construction, BUT the cuckoo dedup meta chain is generation-stable +
    /// mutated in place, so on a crash-to-older-generation it can reference
    /// pages a newer flush made durable above `page_high_water`. The device
    /// open MUST pass `max(manifest.page_high_water,
    /// dedup_index.max_referenced_page_id() + 1)` or the allocator will
    /// double-allocate a page the live dedup index still points at (the file
    /// path is immune because EOF covers those pages).
    pub fn rebuild_free_list_bounded(&self, frontier: u64) -> Result<()> {
        use std::sync::atomic::Ordering;
        let capacity = self.device.len_pages()?;
        let scan_to = frontier.min(capacity);
        let (high_water, free_list) =
            scan_free_list(self.device.as_ref(), FIRST_DATA_PAGE, scan_to)?;
        let free_list_len = free_list.len();
        let mut inner = self.inner.lock();
        inner.high_water = high_water;
        inner.free_list = free_list;
        self.high_water_pages.store(high_water, Ordering::Relaxed);
        self.free_list_pages.store(free_list_len, Ordering::Relaxed);
        Ok(())
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

/// Pages per batched `read_pages` call during the open scan. 1024 * 4 KiB =
/// 4 MiB, matching the meta-LD device batch cap (`MAX_DEVICE_IO_BYTES`) so one
/// submit fans across the RAID's PDs at depth instead of issuing QD1 single
/// reads — the difference between a ~75 s and a few-second reopen on a large
/// meta LD.
const SCAN_CHUNK_PAGES: usize = 1024;

/// Scan pages `[scan_from, scan_to)` on `device` to recover the high-water
/// mark and the reusable free list. Typed pages push `high_water` past
/// them; `Free`-stamped pages and all-zero (punched / never-written) pages
/// join the free list. Torn pages are left in place below high_water; the
/// verifier flags them later.
///
/// Reads are issued in [`SCAN_CHUNK_PAGES`]-page batches via
/// [`PageDevice::read_pages`] (returned **unverified**, exactly like the
/// per-page `read_page_unchecked` this replaced), so the classification below
/// is byte-for-byte identical while the device sees batched, high-QD IO.
pub(super) fn scan_free_list(
    device: &dyn PageDevice,
    scan_from: PageId,
    scan_to: PageId,
) -> Result<(u64, Vec<PageId>)> {
    let mut high_water = scan_from;
    let mut free_list = Vec::new();
    let mut ids: Vec<PageId> = Vec::with_capacity(SCAN_CHUNK_PAGES);
    let mut chunk_start = scan_from;
    while chunk_start < scan_to {
        let chunk_end = (chunk_start + SCAN_CHUNK_PAGES as u64).min(scan_to);
        ids.clear();
        ids.extend(chunk_start..chunk_end);
        let pages = device.read_pages(&ids)?;
        for (page_id, page) in ids.iter().copied().zip(pages.iter()) {
            if let Ok(h) = page.header() {
                high_water = page_id + 1;
                if h.page_type == PageType::Free {
                    free_list.push(page_id);
                }
            } else if is_zero_page(page) {
                free_list.push(page_id);
            }
        }
        chunk_start = chunk_end;
    }
    free_list.retain(|pid| *pid < high_water);
    Ok((high_water, free_list))
}
