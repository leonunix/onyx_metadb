//! Persistent storage of an [`Lsm`](super::Lsm)'s level → SST-handle
//! table.
//!
//! One chained list of [`PageType::LsmLevels`] pages per level. Each
//! page stores up to [`HANDLES_PER_PAGE`] serialized `SstHandle`s and a
//! pointer to the next page in the chain (or `NULL_PAGE` for the tail).
//! A level with no SSTs has no pages and its head in the manifest is
//! `NULL_PAGE`.
//!
//! # Page payload layout
//!
//! ```text
//! offset  size  field
//! ------  ----  ---------------------------
//!   0      4    layout_version = 1
//!   4      8    next_page (PageId, NULL_PAGE for last)
//!  12      4    handle_count (u32)
//!  16     ...   SstHandle × handle_count (88 bytes each)
//! ```
//!
//! Pages are allocated individually via `PageStore::allocate`; chains
//! are built tail-first so only the head id needs to be remembered by
//! the manifest.

use crate::error::{MetaDbError, Result};
use crate::page::{PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use crate::page_store::PageStore;
use crate::types::{Lsn, NULL_PAGE, PageId};

use super::format::HASH_SIZE;
use super::sst::SstHandle;

/// Layout version written into each level page. Bump on any breaking
/// on-disk change.
pub const LSM_LEVELS_LAYOUT_VERSION: u32 = 1;

/// Serialized size of one SST handle.
pub const HANDLE_ENCODED_SIZE: usize = 8 /* head_page */
    + 8 /* record_count */
    + 4 /* bloom_page_count */
    + 4 /* body_page_count */
    + HASH_SIZE /* min_hash */
    + HASH_SIZE /* max_hash */;

const OFF_LAYOUT_VERSION: usize = 0;
const OFF_NEXT_PAGE: usize = 4;
const OFF_HANDLE_COUNT: usize = 12;
const OFF_HANDLES_START: usize = 16;

/// Number of handles that fit in one level page.
pub const HANDLES_PER_PAGE: usize = (PAGE_PAYLOAD_SIZE - OFF_HANDLES_START) / HANDLE_ENCODED_SIZE;

const _: () = {
    assert!(HANDLE_ENCODED_SIZE == 88);
    assert!(HANDLES_PER_PAGE >= 45);
    assert!(OFF_HANDLES_START + HANDLE_ENCODED_SIZE * HANDLES_PER_PAGE <= PAGE_PAYLOAD_SIZE);
};

/// Write one level's handle list as a chain of `LsmLevels` pages. Head
/// returned; `NULL_PAGE` if `handles` is empty.
///
/// Each page is allocated individually, so freed pages can be reused by
/// the B+tree free list later.
pub fn write_level(
    page_store: &PageStore,
    handles: &[SstHandle],
    generation: Lsn,
) -> Result<PageId> {
    if handles.is_empty() {
        return Ok(NULL_PAGE);
    }
    // Build the chain tail-first so each page knows its `next_page` at
    // write time.
    let chunks: Vec<&[SstHandle]> = handles.chunks(HANDLES_PER_PAGE).collect();
    let mut next: PageId = NULL_PAGE;
    for chunk in chunks.iter().rev() {
        next = write_one_page(page_store, chunk, next, generation)?;
    }
    Ok(next)
}

/// Read a level's handle list by walking the chain from `head`. Returns
/// an empty vector if `head == NULL_PAGE`.
pub fn read_level(page_store: &PageStore, head: PageId) -> Result<Vec<SstHandle>> {
    let mut out = Vec::new();
    let mut cursor = head;
    while cursor != NULL_PAGE {
        let page = page_store.read_page(cursor)?;
        let (next, handles) = decode_level_page(cursor, &page)?;
        out.extend(handles);
        cursor = next;
    }
    Ok(out)
}

/// Free every page in a level chain back to the page store. No-op if
/// `head == NULL_PAGE`.
pub fn free_level(page_store: &PageStore, head: PageId, generation: Lsn) -> Result<()> {
    let mut cursor = head;
    while cursor != NULL_PAGE {
        let page = page_store.read_page(cursor)?;
        let p = page.payload();
        let next = u64::from_le_bytes(p[OFF_NEXT_PAGE..OFF_NEXT_PAGE + 8].try_into().unwrap());
        page_store.free(cursor, generation)?;
        cursor = next;
    }
    Ok(())
}

/// Rewrite every level head's chain using the provided handle tables,
/// returning the new head list. Old chains are freed afterwards.
///
/// Typical use at `Db::flush`: hold the previous `Vec<PageId>` of
/// level heads, compute the new list with this function, commit the
/// manifest with the new heads, then free the old chains.
pub fn rewrite_levels(
    page_store: &PageStore,
    levels: &[Vec<SstHandle>],
    generation: Lsn,
) -> Result<Vec<PageId>> {
    let mut heads = Vec::with_capacity(levels.len());
    for handles in levels {
        heads.push(write_level(page_store, handles, generation)?);
    }
    Ok(heads)
}

/// Convenience wrapper for opening all levels at once.
pub fn read_levels(page_store: &PageStore, heads: &[PageId]) -> Result<Vec<Vec<SstHandle>>> {
    heads
        .iter()
        .map(|&h| read_level(page_store, h))
        .collect::<Result<Vec<_>>>()
}

pub(crate) fn decode_level_page(page_id: PageId, page: &Page) -> Result<(PageId, Vec<SstHandle>)> {
    let h = page.header()?;
    if h.page_type != PageType::LsmLevels {
        return Err(MetaDbError::Corruption(format!(
            "page {page_id} expected to be LsmLevels, found {:?}",
            h.page_type
        )));
    }
    let p = page.payload();
    let layout = u32::from_le_bytes(
        p[OFF_LAYOUT_VERSION..OFF_LAYOUT_VERSION + 4]
            .try_into()
            .unwrap(),
    );
    if layout != LSM_LEVELS_LAYOUT_VERSION {
        return Err(MetaDbError::Corruption(format!(
            "LsmLevels page {page_id}: unknown layout version {layout}",
        )));
    }
    let next = u64::from_le_bytes(p[OFF_NEXT_PAGE..OFF_NEXT_PAGE + 8].try_into().unwrap());
    let count = u32::from_le_bytes(
        p[OFF_HANDLE_COUNT..OFF_HANDLE_COUNT + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    if count > HANDLES_PER_PAGE {
        return Err(MetaDbError::Corruption(format!(
            "LsmLevels page {page_id}: handle_count {count} exceeds capacity {HANDLES_PER_PAGE}",
        )));
    }
    let mut handles = Vec::with_capacity(count);
    let mut off = OFF_HANDLES_START;
    for _ in 0..count {
        handles.push(decode_handle(&p[off..off + HANDLE_ENCODED_SIZE]));
        off += HANDLE_ENCODED_SIZE;
    }
    Ok((next, handles))
}

fn write_one_page(
    page_store: &PageStore,
    handles: &[SstHandle],
    next_page: PageId,
    generation: Lsn,
) -> Result<PageId> {
    assert!(handles.len() <= HANDLES_PER_PAGE);
    let page_id = page_store.allocate()?;
    let mut page = Page::new(PageHeader::new(PageType::LsmLevels, generation));
    let p = page.payload_mut();
    p[OFF_LAYOUT_VERSION..OFF_LAYOUT_VERSION + 4]
        .copy_from_slice(&LSM_LEVELS_LAYOUT_VERSION.to_le_bytes());
    p[OFF_NEXT_PAGE..OFF_NEXT_PAGE + 8].copy_from_slice(&next_page.to_le_bytes());
    p[OFF_HANDLE_COUNT..OFF_HANDLE_COUNT + 4]
        .copy_from_slice(&(handles.len() as u32).to_le_bytes());
    let mut off = OFF_HANDLES_START;
    for handle in handles {
        encode_handle(handle, &mut p[off..off + HANDLE_ENCODED_SIZE]);
        off += HANDLE_ENCODED_SIZE;
    }
    page.seal();
    page_store.write_page(page_id, &page)?;
    Ok(page_id)
}

fn encode_handle(handle: &SstHandle, out: &mut [u8]) {
    debug_assert_eq!(out.len(), HANDLE_ENCODED_SIZE);
    out[0..8].copy_from_slice(&handle.head_page.to_le_bytes());
    out[8..16].copy_from_slice(&handle.record_count.to_le_bytes());
    out[16..20].copy_from_slice(&handle.bloom_page_count.to_le_bytes());
    out[20..24].copy_from_slice(&handle.body_page_count.to_le_bytes());
    out[24..24 + HASH_SIZE].copy_from_slice(&handle.min_hash);
    out[24 + HASH_SIZE..24 + 2 * HASH_SIZE].copy_from_slice(&handle.max_hash);
}

fn decode_handle(bytes: &[u8]) -> SstHandle {
    debug_assert_eq!(bytes.len(), HANDLE_ENCODED_SIZE);
    let head_page = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let record_count = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let bloom_page_count = u32::from_le_bytes(bytes[16..20].try_into().unwrap());
    let body_page_count = u32::from_le_bytes(bytes[20..24].try_into().unwrap());
    let mut min_hash = [0u8; HASH_SIZE];
    min_hash.copy_from_slice(&bytes[24..24 + HASH_SIZE]);
    let mut max_hash = [0u8; HASH_SIZE];
    max_hash.copy_from_slice(&bytes[24 + HASH_SIZE..24 + 2 * HASH_SIZE]);
    SstHandle {
        head_page,
        record_count,
        bloom_page_count,
        body_page_count,
        min_hash,
        max_hash,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn mk_ps() -> (TempDir, PageStore) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = PageStore::create(&path).unwrap();
        (dir, ps)
    }

    fn mk_handle(seed: u64) -> SstHandle {
        let mut min = [0u8; HASH_SIZE];
        let mut max = [0u8; HASH_SIZE];
        min[..8].copy_from_slice(&seed.to_be_bytes());
        max[..8].copy_from_slice(&(seed + 1).to_be_bytes());
        SstHandle {
            head_page: 100 + seed,
            record_count: 1000 + seed,
            bloom_page_count: 1,
            body_page_count: 2,
            min_hash: min,
            max_hash: max,
        }
    }

    #[test]
    fn empty_level_returns_null() {
        let (_d, ps) = mk_ps();
        let head = write_level(&ps, &[], 1).unwrap();
        assert_eq!(head, NULL_PAGE);
        let back = read_level(&ps, head).unwrap();
        assert!(back.is_empty());
    }

    #[test]
    fn single_page_round_trip() {
        let (_d, ps) = mk_ps();
        let handles: Vec<SstHandle> = (0..5).map(mk_handle).collect();
        let head = write_level(&ps, &handles, 10).unwrap();
        let back = read_level(&ps, head).unwrap();
        assert_eq!(back, handles);
    }

    #[test]
    fn multi_page_chain_round_trip() {
        let (_d, ps) = mk_ps();
        // Force 3 pages: each page holds ≤ 45, so 100 is enough.
        let handles: Vec<SstHandle> = (0..100).map(mk_handle).collect();
        let head = write_level(&ps, &handles, 7).unwrap();
        let back = read_level(&ps, head).unwrap();
        assert_eq!(back.len(), 100);
        assert_eq!(back, handles);
    }

    #[test]
    fn free_level_returns_pages_to_free_list() {
        let (_d, ps) = mk_ps();
        let handles: Vec<SstHandle> = (0..HANDLES_PER_PAGE as u64 * 2 + 3)
            .map(mk_handle)
            .collect();
        let head = write_level(&ps, &handles, 1).unwrap();
        let before = ps.free_list_len();
        free_level(&ps, head, 2).unwrap();
        ps.try_reclaim().unwrap();
        assert_eq!(ps.free_list_len(), before + 3); // 3 chained pages freed
    }

    #[test]
    fn rewrite_and_read_multiple_levels() {
        let (_d, ps) = mk_ps();
        let level0: Vec<SstHandle> = (0..3).map(mk_handle).collect();
        let level1: Vec<SstHandle> = (3..60).map(mk_handle).collect(); // > 1 page
        let level2: Vec<SstHandle> = Vec::new();
        let levels = vec![level0.clone(), level1.clone(), level2.clone()];
        let heads = rewrite_levels(&ps, &levels, 1).unwrap();
        assert_eq!(heads.len(), 3);
        assert_ne!(heads[0], NULL_PAGE);
        assert_ne!(heads[1], NULL_PAGE);
        assert_eq!(heads[2], NULL_PAGE);
        let back = read_levels(&ps, &heads).unwrap();
        assert_eq!(back, vec![level0, level1, level2]);
    }

    #[test]
    fn head_pointing_at_non_levels_page_errors() {
        let (_d, ps) = mk_ps();
        // Allocate a wrong-type page (empty Page::new fills with L2pLeaf).
        let pid = ps.allocate().unwrap();
        let mut page = Page::new(PageHeader::new(PageType::L2pLeaf, 1));
        page.seal();
        ps.write_page(pid, &page).unwrap();
        match read_level(&ps, pid).unwrap_err() {
            MetaDbError::Corruption(_) => {}
            e => panic!("{e}"),
        }
    }
}
