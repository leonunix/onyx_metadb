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
//! whose header decodes as [`PageType::Free`]. A later phase will persist
//! the free list in its own page chain to avoid the scan for large
//! databases; v0 keeps it simple and correct.
//!
//! # Concurrency
//!
//! `read_page` / `write_page` take a shared `&File` and issue positional IO
//! (`pread` / `pwrite`) — safe under concurrent callers since each call
//! is atomic at the kernel level. The mutex only protects metadata (free
//! list, high-water mark).

use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use crate::config::PAGE_SIZE;
use crate::error::{MetaDbError, Result};
use crate::page::{Page, PageHeader, PageType};
use crate::types::{FIRST_DATA_PAGE, Lsn, PageId};

const RC_LOCK_SHARDS: usize = 64;

/// Flat page file.
pub struct PageStore {
    path: PathBuf,
    file: File,
    inner: Mutex<Inner>,
    /// Per-pid sharded mutexes serialising [`atomic_rc_delta`]. Needed
    /// because a page can be shared across multiple [`PagedL2p`]
    /// instances after `clone_volume` — two trees each holding their
    /// own `PageBuf` would otherwise each read the same pre-decrement
    /// rc and race their writes back to disk (last-writer-wins losing
    /// one decrement).
    rc_locks: Box<[Mutex<()>]>,
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
    /// Smallest page id that has *not* yet been allocated. File length is
    /// always `high_water * PAGE_SIZE`.
    high_water: u64,
    /// Explicitly-freed pages available for reuse. LIFO.
    free_list: Vec<PageId>,
}

impl PageStore {
    /// Create a brand-new page store at `path`. Fails if the file already
    /// exists.
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        // Pre-size to FIRST_DATA_PAGE so the manifest slot offsets are
        // immediately addressable, even though we leave those pages zeroed
        // (the manifest layer will populate them).
        file.set_len(FIRST_DATA_PAGE * PAGE_SIZE as u64)?;
        Ok(Self {
            path,
            file,
            inner: Mutex::new(Inner {
                high_water: FIRST_DATA_PAGE,
                free_list: Vec::new(),
            }),
            rc_locks: new_rc_locks(),
        })
    }

    /// Open an existing page store.  Rebuilds the in-memory free list by
    /// scanning pages from [`FIRST_DATA_PAGE`] to EOF.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
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
        let high_water = size / PAGE_SIZE as u64;
        let mut free_list = Vec::new();
        for page_id in FIRST_DATA_PAGE..high_water {
            let page = read_page_raw(&file, page_id)?;
            // Lenient scan: only pages whose header decodes AND declares
            // itself `Free` go on the list. Bad / unknown pages are left
            // alone for the verifier to flag.
            if let Ok(h) = page.header() {
                if h.page_type == PageType::Free {
                    free_list.push(page_id);
                }
            }
        }
        Ok(Self {
            path,
            file,
            inner: Mutex::new(Inner {
                high_water,
                free_list,
            }),
            rc_locks: new_rc_locks(),
        })
    }

    /// Path the store was opened from.
    pub fn path(&self) -> &Path {
        &self.path
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

    /// Same as [`atomic_rc_delta`] but with WAL-replay idempotency: if
    /// `page.generation >= lsn` the delta is treated as already-applied
    /// by a prior attempt of the same op and the function returns the
    /// current rc without mutating the page. On successful apply the
    /// page's `generation` is stamped with `lsn`.
    ///
    /// Used by [`crate::paged::PageBuf::cow_for_write`] so that a WAL
    /// op replayed after crash does not double-apply the rc delta on a
    /// cross-tree shared page. Same `page.generation >= lsn` skip
    /// pattern as [`crate::db::apply_drop_snapshot_pages`] and
    /// [`crate::db::apply_clone_volume_incref`].
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
        let cur_rc = page.refcount();
        if cur_gen >= lsn {
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
        page.seal();
        self.file
            .write_all_at(page.bytes(), page_id * PAGE_SIZE as u64)?;
        Ok(new_rc)
    }

    /// Allocate a fresh page id. If the free list has entries, one is
    /// popped and returned; otherwise the file is extended by one page.
    /// The on-disk content is not initialized — the caller is expected to
    /// write a sealed page at the returned id.
    pub fn allocate(&self) -> Result<PageId> {
        let mut inner = self.inner.lock();
        if let Some(page_id) = inner.free_list.pop() {
            return Ok(page_id);
        }
        let page_id = inner.high_water;
        inner.high_water = inner
            .high_water
            .checked_add(1)
            .ok_or(MetaDbError::OutOfSpace)?;
        self.file.set_len(inner.high_water * PAGE_SIZE as u64)?;
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
        self.file.set_len(new_high * PAGE_SIZE as u64)?;
        inner.high_water = new_high;
        Ok(start)
    }

    /// Free `count` pages starting at `start`, stamping each with
    /// `generation`. Pages rejoin the single-page free list individually.
    /// Convenience wrapper over [`free`]; fails as soon as any page id
    /// falls outside the allowed range.
    pub fn free_run(&self, start: PageId, count: u32, generation: Lsn) -> Result<()> {
        for i in 0..count as u64 {
            self.free(start + i, generation)?;
        }
        Ok(())
    }

    /// Mark `page_id` as free. Writes a sealed `Free`-typed page stamped
    /// with `generation` and pushes onto the free list. `generation`
    /// should be the current LSN so verifier tooling can tell when the
    /// page was released.
    ///
    /// Refuses to free reserved pages (manifest slots) or pages outside
    /// the current high-water range.
    pub fn free(&self, page_id: PageId, generation: Lsn) -> Result<()> {
        if page_id < FIRST_DATA_PAGE {
            return Err(MetaDbError::InvalidArgument(format!(
                "page {page_id} is reserved (manifest slot)",
            )));
        }
        self.check_in_range(page_id)?;
        let mut page = Page::new(PageHeader::new(PageType::Free, generation));
        page.set_refcount(0);
        page.seal();
        self.file
            .write_all_at(page.bytes(), page_id * PAGE_SIZE as u64)?;
        let mut inner = self.inner.lock();
        inner.free_list.push(page_id);
        Ok(())
    }

    /// Idempotent version of [`free`]. If `page_id` already decodes as a
    /// `Free` page, no write happens and the free list is untouched —
    /// returns `Ok(false)`. Otherwise behaves exactly like `free` and
    /// returns `Ok(true)`.
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
        if let Ok(existing) = read_page_raw(&self.file, page_id) {
            if let Ok(h) = existing.header() {
                if h.page_type == PageType::Free {
                    return Ok(false);
                }
            }
        }
        let mut page = Page::new(PageHeader::new(PageType::Free, generation));
        page.set_refcount(0);
        page.seal();
        self.file
            .write_all_at(page.bytes(), page_id * PAGE_SIZE as u64)?;
        let mut inner = self.inner.lock();
        inner.free_list.push(page_id);
        Ok(true)
    }

    /// `fdatasync` the page file (content only).
    pub fn sync(&self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
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
}

fn read_page_raw(file: &File, page_id: PageId) -> Result<Page> {
    let mut page = Page::zeroed();
    file.read_exact_at(page.bytes_mut(), page_id * PAGE_SIZE as u64)?;
    Ok(page)
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
    fn free_list_is_lifo() {
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
        // LIFO: the last freed (c) comes back first.
        assert_eq!(ps.allocate().unwrap(), c);
        assert_eq!(ps.allocate().unwrap(), b);
        assert_eq!(ps.allocate().unwrap(), a);
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
        assert_eq!(ps.free_list_len(), 3);
    }
}
