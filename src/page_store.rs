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

/// Flat page file.
pub struct PageStore {
    path: PathBuf,
    file: File,
    inner: Mutex<Inner>,
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
}
