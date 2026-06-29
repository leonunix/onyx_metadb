//! COW byte-stream chains for the manifest volume catalog + snapshot table.
//!
//! The manifest used to inline both tables in its single 4 KiB page, capping a
//! database at ~10-30 volumes (the `{volumes + snapshots}` budget had to share
//! one [`PAGE_PAYLOAD_SIZE`] payload). This module spills each table into its
//! own page chain so the catalog is O(pages): the manifest stores only a head
//! [`PageId`] per chain plus the entry count.
//!
//! # Per-slot, in place — chains are part of their manifest slot
//!
//! The catalog chains are double-buffered ALONGSIDE the manifest slots: slot A's
//! manifest body anchors slot A's chains, slot B's anchors slot B's. A commit
//! rewrites only the TARGET slot's chains in place (reusing their head +
//! continuation pids, growing/shrinking continuations as the byte stream
//! changes) and then the target slot's body — the OTHER slot's body and chains
//! are never touched, so the previous generation stays fully intact (a torn new
//! slot falls back to it cleanly). This is the same in-place reuse `paged_meta`
//! uses for refcount/cuckoo, but applied per-slot so there is NO shared head
//! between the two manifest generations (which is what would have made in-place
//! unsafe). `paged_meta` stores a table of `PageId`s; the catalog stores an
//! opaque byte stream (variable-length `VolumeEntry` rows / fixed
//! `SnapshotEntry` rows), so it needs its own codec.
//!
//! # On-disk page layout (head and continuation pages are identical)
//!
//! ```text
//!   bytes 0..4    chunk_len: u32 LE  (catalog bytes used in this page)
//!   bytes 4..8    reserved   u32 LE  (0)
//!   bytes 8..16   next_pid   u64 LE  (0 = end of chain)
//!   bytes 16..    catalog payload bytes
//! ```
//!
//! `next_pid == 0` marks the end of the chain: `0` is [`MANIFEST_PAGE_A`], never
//! a data-page allocation, so it is an unambiguous sentinel (same convention as
//! `paged_meta`). The chain kind (volumes vs snapshots) is stamped into the
//! page header's `key_count` so a misrouted page is rejected on read.

use std::sync::Arc;

use crate::error::{MetaDbError, Result};
use crate::page::{PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use crate::page_store::PageStore;
use crate::types::{Lsn, PageId};

/// Fixed per-page chain header (chunk_len + reserved + next_pid).
const CHAIN_HEADER_BYTES: usize = 16;

/// Catalog payload bytes that fit on one chain page.
pub const CATALOG_PAGE_CAPACITY: usize = PAGE_PAYLOAD_SIZE - CHAIN_HEADER_BYTES;

/// Runaway/cycle guard: a corrupt `next_pid` that forms a loop must surface as
/// an error rather than spin forever. Far above any real chain length (65 534
/// volumes × ~356 B ÷ 4016 ≈ 5 800 pages for the volume chain).
const MAX_CHAIN_PAGES: usize = 1 << 24;

/// Which logical table a catalog chain carries. Stamped into the page header
/// `key_count` so a chain page read with the wrong head pid is rejected.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum CatalogKind {
    Volumes,
    Snapshots,
}

impl CatalogKind {
    /// Header `key_count` marker for this kind.
    fn marker(self) -> u16 {
        match self {
            CatalogKind::Volumes => 0xCA01,
            CatalogKind::Snapshots => 0xCA02,
        }
    }
}

/// Lay out `bytes` over a chain rooted at `existing_chain`, reusing its pids in
/// place. Allocates one page per [`CATALOG_PAGE_CAPACITY`]-byte chunk (always at
/// least one page, so the head pid is never the `0` sentinel): the head + as
/// many continuations as still fit are reused from `existing_chain`, extra
/// continuations are freshly allocated, and trailing pages the shrunken stream
/// no longer needs are returned in `to_free`. Pure CPU + allocator work — no
/// `write_page` / `sync` / `free`.
///
/// Returns `(new_chain, sealed_pages, to_free)`:
/// - `new_chain` — chain order, head first; `new_chain[0]` is the anchor the
///   manifest records (== `existing_chain[0]` when `existing_chain` is
///   non-empty, so a slot's head is stable across commits).
/// - `sealed_pages` — `(pid, Arc<Page>)` for every page in `new_chain`, sealed.
/// - `to_free` — trailing pids from `existing_chain` the new stream dropped;
///   the caller frees them AFTER the manifest slot is durable.
///
/// `existing_chain` empty (first build for a slot) allocates the whole chain.
pub fn build_catalog_chain(
    page_store: &PageStore,
    kind: CatalogKind,
    bytes: &[u8],
    existing_chain: &[PageId],
    generation: Lsn,
) -> Result<(Vec<PageId>, Vec<(PageId, Arc<Page>)>, Vec<PageId>)> {
    let chunk_count = bytes.len().div_ceil(CATALOG_PAGE_CAPACITY).max(1);

    let mut new_chain = Vec::with_capacity(chunk_count);
    for i in 0..chunk_count {
        let pid = match existing_chain.get(i) {
            Some(&pid) => pid,
            None => page_store.allocate()?,
        };
        new_chain.push(pid);
    }
    let to_free: Vec<PageId> = existing_chain.iter().skip(chunk_count).copied().collect();

    let mut sealed = Vec::with_capacity(chunk_count);
    for (i, &pid) in new_chain.iter().enumerate() {
        let start = i * CATALOG_PAGE_CAPACITY;
        let end = ((i + 1) * CATALOG_PAGE_CAPACITY).min(bytes.len());
        let chunk = &bytes[start..end];
        let next_pid = new_chain.get(i + 1).copied().unwrap_or(0);

        let mut page = Page::new(PageHeader::new(PageType::ManifestCatalog, generation));
        page.set_key_count(kind.marker());
        {
            let p = page.payload_mut();
            p[0..4].copy_from_slice(&(chunk.len() as u32).to_le_bytes());
            p[4..8].fill(0);
            p[8..16].copy_from_slice(&(next_pid).to_le_bytes());
            p[CHAIN_HEADER_BYTES..CHAIN_HEADER_BYTES + chunk.len()].copy_from_slice(chunk);
        }
        page.seal();
        sealed.push((pid, Arc::new(page)));
    }

    Ok((new_chain, sealed, to_free))
}

/// Walk the chain rooted at `head_pid`, validating + concatenating every page's
/// catalog bytes, and invoke `on_page` once per page id (head first). Any
/// unreadable / mistyped / mis-kinded / over-long page returns `Err` so the
/// manifest decode fails cleanly and `load_latest` falls back to the other
/// slot. `head_pid == 0` yields an empty stream with no pages walked.
fn walk(
    page_store: &PageStore,
    head_pid: PageId,
    kind: CatalogKind,
    on_page: &mut dyn FnMut(PageId),
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut next = head_pid;
    let mut pages = 0usize;
    while next != 0 {
        if pages >= MAX_CHAIN_PAGES {
            return Err(MetaDbError::Corruption(format!(
                "manifest catalog chain at head exceeds {MAX_CHAIN_PAGES} pages (cycle?)",
            )));
        }
        on_page(next);
        let page = page_store.read_page(next)?;
        let header = page.header()?;
        if header.page_type != PageType::ManifestCatalog || header.key_count != kind.marker() {
            return Err(MetaDbError::Corruption(format!(
                "manifest catalog page {next} has wrong header (type={:?}, key_count={:#06x}, \
                 expected kind {:#06x})",
                header.page_type,
                header.key_count,
                kind.marker(),
            )));
        }
        let p = page.payload();
        let chunk_len = u32::from_le_bytes(p[0..4].try_into().unwrap()) as usize;
        let next_pid = u64::from_le_bytes(p[8..16].try_into().unwrap()) as PageId;
        if chunk_len > CATALOG_PAGE_CAPACITY {
            return Err(MetaDbError::Corruption(format!(
                "manifest catalog page {next} chunk_len {chunk_len} exceeds {CATALOG_PAGE_CAPACITY}",
            )));
        }
        out.extend_from_slice(&p[CHAIN_HEADER_BYTES..CHAIN_HEADER_BYTES + chunk_len]);
        next = next_pid;
        pages += 1;
    }
    Ok(out)
}

/// Read the whole chain rooted at `head_pid` back into one byte stream.
pub fn read_catalog_chain(
    page_store: &PageStore,
    head_pid: PageId,
    kind: CatalogKind,
) -> Result<Vec<u8>> {
    walk(page_store, head_pid, kind, &mut |_| {})
}

/// Collect every page id in the chain (head first). Used at open to seed
/// [`super::store::ManifestStore`]'s per-slot chain bookkeeping (so a later
/// commit reuses pids in place) and by the verifier to mark catalog pages live.
pub fn chain_pids(page_store: &PageStore, head_pid: PageId, kind: CatalogKind) -> Result<Vec<PageId>> {
    let mut pids = Vec::new();
    walk(page_store, head_pid, kind, &mut |pid| pids.push(pid))?;
    Ok(pids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn store() -> (TempDir, Arc<PageStore>) {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(PageStore::create(&dir.path().join("pages")).unwrap());
        (dir, store)
    }

    fn round_trip(kind: CatalogKind, bytes: &[u8]) {
        let (_d, store) = store();
        let (chain, sealed, to_free) = build_catalog_chain(&store, kind, bytes, &[], 7).unwrap();
        assert!(to_free.is_empty());
        store.write_sealed_page_runs(sealed).unwrap();
        store.sync().unwrap();
        let head = chain[0];
        let read = read_catalog_chain(&store, head, kind).unwrap();
        assert_eq!(read, bytes);
        let walked = chain_pids(&store, head, kind).unwrap();
        assert_eq!(walked, chain);
    }

    #[test]
    fn empty_stream_is_one_head_page() {
        let (_d, store) = store();
        let (chain, sealed, _free) =
            build_catalog_chain(&store, CatalogKind::Volumes, &[], &[], 1).unwrap();
        assert_eq!(chain.len(), 1);
        assert_ne!(chain[0], 0);
        store.write_sealed_page_runs(sealed).unwrap();
        store.sync().unwrap();
        assert!(
            read_catalog_chain(&store, chain[0], CatalogKind::Volumes)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn reuse_existing_chain_head_stable_and_shrink_frees_trailing() {
        let (_d, store) = store();
        // Grow to 3 pages.
        let big: Vec<u8> = (0..(CATALOG_PAGE_CAPACITY * 2 + 10))
            .map(|i| (i % 251) as u8)
            .collect();
        let (chain, sealed, free0) =
            build_catalog_chain(&store, CatalogKind::Volumes, &big, &[], 1).unwrap();
        assert!(free0.is_empty());
        assert_eq!(chain.len(), 3);
        store.write_sealed_page_runs(sealed).unwrap();
        store.sync().unwrap();

        // Rewrite a smaller stream reusing the chain: head reused in place,
        // the two trailing continuation pages returned to free.
        let small: Vec<u8> = (0..50).collect();
        let (chain2, sealed2, free1) =
            build_catalog_chain(&store, CatalogKind::Volumes, &small, &chain, 2).unwrap();
        assert_eq!(chain2.len(), 1);
        assert_eq!(chain2[0], chain[0], "head pid must be stable across commits");
        assert_eq!(free1, chain[1..].to_vec(), "trailing pages must be freed");
        store.write_sealed_page_runs(sealed2).unwrap();
        store.sync().unwrap();
        assert_eq!(
            read_catalog_chain(&store, chain2[0], CatalogKind::Volumes).unwrap(),
            small
        );
    }

    #[test]
    fn round_trips_within_one_page() {
        round_trip(CatalogKind::Volumes, &(0u8..200).collect::<Vec<_>>());
    }

    #[test]
    fn round_trips_across_many_pages() {
        // ~5 pages of volume catalog bytes.
        let bytes: Vec<u8> = (0..(CATALOG_PAGE_CAPACITY * 4 + 37))
            .map(|i| (i % 251) as u8)
            .collect();
        round_trip(CatalogKind::Snapshots, &bytes);
    }

    #[test]
    fn exact_page_boundary_round_trips() {
        let bytes: Vec<u8> = (0..(CATALOG_PAGE_CAPACITY * 2)).map(|i| (i % 97) as u8).collect();
        round_trip(CatalogKind::Volumes, &bytes);
    }

    #[test]
    fn wrong_kind_marker_is_rejected() {
        let (_d, store) = store();
        let bytes: Vec<u8> = (0..50).collect();
        let (chain, sealed, _free) =
            build_catalog_chain(&store, CatalogKind::Volumes, &bytes, &[], 3).unwrap();
        store.write_sealed_page_runs(sealed).unwrap();
        store.sync().unwrap();
        // Reading the volume chain as a snapshot chain must be rejected.
        assert!(read_catalog_chain(&store, chain[0], CatalogKind::Snapshots).is_err());
    }
}
