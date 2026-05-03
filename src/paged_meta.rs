//! Chained meta-page reader/writer for paged-array indexes.
//!
//! `refcount`, `paged_reverse`, and `dedup::cuckoo` all map a sparse
//! page table (`page_idx -> on-disk PageId`) onto a single meta page.
//! That capped each shard at ~503 entries — fine for unit tests, far
//! too small for production sizing (50 M dedup entries, millions of
//! refcount PBAs). This module factors the chain semantics out so all
//! three indexes share one implementation.
//!
//! # On-disk layout
//!
//! Every meta page in a chain carries a fixed 16 B chain header at a
//! known offset:
//! ```text
//!   bytes 0..4    chunk_len:    u32 LE  (entries in this page's chunk)
//!   bytes 4..8    reserved      u32 LE  (must be 0)
//!   bytes 8..16   next_meta_pid u64 LE  (0 = end of chain)
//! ```
//! The page-table chunk follows immediately after.
//!
//! The **head** meta page may also reserve `head_extra` bytes at the
//! front for module-specific fields (e.g. cuckoo's `bucket_count` and
//! the two hash seeds). The chain header on the head therefore starts
//! at offset `head_extra`. Continuation pages always have the chain
//! header at offset 0.
//!
//! # Concurrency
//!
//! No internal locking — each module holds its own mutex around the
//! in-memory `chain_pids` and `page_table` while calling [`read_chain`]
//! / [`write_chain`]. Page IO is delegated to `PageStore` / `PageCache`,
//! which have their own concurrency model.

use std::sync::Arc;

use crate::cache::PageCache;
use crate::error::{MetaDbError, Result};
use crate::page::{PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use crate::page_store::PageStore;
use crate::types::{Lsn, PageId};

const CHAIN_HEADER_BYTES: usize = 16;

/// Page-table entries that fit on a continuation meta page (no
/// module-specific head_extra).
pub const CONTINUATION_CAPACITY: usize =
    (PAGE_PAYLOAD_SIZE - CHAIN_HEADER_BYTES) / std::mem::size_of::<u64>();

/// Page-table entries that fit on the **head** meta page given that
/// it reserves `head_extra` bytes at the front for module-specific
/// data.
#[inline]
pub const fn head_capacity(head_extra: usize) -> usize {
    let usable = PAGE_PAYLOAD_SIZE - CHAIN_HEADER_BYTES - head_extra;
    usable / std::mem::size_of::<u64>()
}

pub struct ReadResult {
    /// Module-specific bytes copied from the head page (length =
    /// `head_extra` requested in [`read_chain`]).
    pub head_extra: Vec<u8>,
    /// Concatenated page-table from every chunk in the chain.
    pub page_table: Vec<PageId>,
    /// Meta page ids in chain order, head first.
    pub chain_pids: Vec<PageId>,
}

/// Read the entire meta chain rooted at `head_pid`. Validates each
/// page's `page_type` and `key_count` markers.
pub fn read_chain(
    page_store: &PageStore,
    head_pid: PageId,
    expected_type: PageType,
    expected_key_count: u16,
    head_extra: usize,
) -> Result<ReadResult> {
    let mut chain_pids = Vec::new();
    let mut page_table = Vec::new();
    let mut head_extra_bytes = vec![0u8; head_extra];
    let mut next_pid = head_pid;
    let mut is_head = true;
    while next_pid != 0 {
        let page = page_store.read_page(next_pid)?;
        let header = page.header()?;
        if header.page_type != expected_type || header.key_count != expected_key_count {
            return Err(MetaDbError::Corruption(format!(
                "meta chain page {next_pid} has wrong header (type={:?}, key_count={})",
                header.page_type, header.key_count,
            )));
        }
        chain_pids.push(next_pid);
        let payload = page.payload();
        let header_off = if is_head { head_extra } else { 0 };
        if is_head && head_extra > 0 {
            head_extra_bytes.copy_from_slice(&payload[..head_extra]);
        }
        let chunk_len =
            u32::from_le_bytes(payload[header_off..header_off + 4].try_into().unwrap()) as usize;
        let next = u64::from_le_bytes(payload[header_off + 8..header_off + 16].try_into().unwrap())
            as PageId;
        let chunk_cap = if is_head {
            head_capacity(head_extra)
        } else {
            CONTINUATION_CAPACITY
        };
        if chunk_len > chunk_cap {
            return Err(MetaDbError::Corruption(format!(
                "meta chain page {next_pid} chunk_len {chunk_len} exceeds capacity {chunk_cap}",
            )));
        }
        let entries_off = header_off + CHAIN_HEADER_BYTES;
        for i in 0..chunk_len {
            let off = entries_off + i * 8;
            let pid = u64::from_le_bytes(payload[off..off + 8].try_into().unwrap()) as PageId;
            page_table.push(pid);
        }
        next_pid = next;
        is_head = false;
    }
    Ok(ReadResult {
        head_extra: head_extra_bytes,
        page_table,
        chain_pids,
    })
}

/// Persist `page_table` across a chain rooted at `existing_chain[0]`.
///
/// Reuses the existing head + as many continuation pages as needed.
/// Allocates new continuation pages if the table grew; frees old ones
/// if it shrank. Returns the new chain (head first).
///
/// `head_extra` are the module-specific bytes written at the front of
/// the head page (length must equal whatever `head_extra` was passed
/// to [`read_chain`] when this chain was last opened).
pub fn write_chain(
    page_store: &PageStore,
    page_cache: &PageCache,
    page_type: PageType,
    expected_key_count: u16,
    head_extra: &[u8],
    page_table: &[PageId],
    existing_chain: &[PageId],
    free_lsn: Lsn,
) -> Result<Vec<PageId>> {
    assert!(
        !existing_chain.is_empty(),
        "write_chain requires the head meta page to already exist",
    );
    let head_cap = head_capacity(head_extra.len());

    // Compute chunks: the head always writes one chunk (possibly empty
    // if the table is empty); continuations fill `CONTINUATION_CAPACITY`
    // entries each.
    let head_chunk_size = head_cap.min(page_table.len());
    let mut chunks: Vec<&[PageId]> = vec![&page_table[..head_chunk_size]];
    let mut i = head_chunk_size;
    while i < page_table.len() {
        let end = (i + CONTINUATION_CAPACITY).min(page_table.len());
        chunks.push(&page_table[i..end]);
        i = end;
    }

    // Reuse meta page ids; allocate new ones for additional chunks.
    let mut new_chain = Vec::with_capacity(chunks.len());
    new_chain.push(existing_chain[0]);
    for idx in 1..chunks.len() {
        let pid = if idx < existing_chain.len() {
            existing_chain[idx]
        } else {
            page_store.allocate()?
        };
        new_chain.push(pid);
    }

    // Trailing pages from the previous chain go on the free list once
    // the new chain has been written.
    let to_free: Vec<PageId> = existing_chain
        .iter()
        .skip(new_chain.len())
        .copied()
        .collect();

    for (chunk_idx, chunk) in chunks.iter().enumerate() {
        let pid = new_chain[chunk_idx];
        let next_pid = new_chain.get(chunk_idx + 1).copied().unwrap_or(0);
        let mut page = Page::new(PageHeader {
            page_type,
            version: crate::page::PAGE_VERSION,
            key_count: expected_key_count,
            flags: 0,
            generation: 0,
            refcount: 1,
        });
        let is_head = chunk_idx == 0;
        {
            let payload = page.payload_mut();
            let header_off = if is_head {
                if !head_extra.is_empty() {
                    payload[..head_extra.len()].copy_from_slice(head_extra);
                }
                head_extra.len()
            } else {
                0
            };
            payload[header_off..header_off + 4]
                .copy_from_slice(&(chunk.len() as u32).to_le_bytes());
            payload[header_off + 4..header_off + 8].fill(0);
            payload[header_off + 8..header_off + 16]
                .copy_from_slice(&(next_pid as u64).to_le_bytes());
            let entries_off = header_off + CHAIN_HEADER_BYTES;
            for (i, &entry) in chunk.iter().enumerate() {
                let off = entries_off + i * 8;
                payload[off..off + 8].copy_from_slice(&(entry as u64).to_le_bytes());
            }
        }
        page.seal();
        page_store.write_page(pid, &page)?;
        page_cache.replace_or_insert(pid, Arc::new(page));
    }

    for pid in to_free {
        page_cache.invalidate(pid);
        page_store.free(pid, free_lsn)?;
    }

    Ok(new_chain)
}

/// Walk every meta page id in the chain rooted at `head_pid` and
/// return the concatenated page-table. Used by the verifier.
///
/// `on_meta` is invoked once per meta page id (including the head) so
/// the caller can mark them live without re-reading the page.
pub fn walk_chain<F>(
    page_store: &PageStore,
    head_pid: PageId,
    expected_type: PageType,
    expected_key_count: u16,
    head_extra: usize,
    mut on_meta: F,
) -> Result<Vec<PageId>>
where
    F: FnMut(PageId),
{
    let mut next_pid = head_pid;
    let mut page_table = Vec::new();
    let mut is_head = true;
    while next_pid != 0 {
        on_meta(next_pid);
        let page = page_store.read_page(next_pid)?;
        let header = page.header()?;
        if header.page_type != expected_type || header.key_count != expected_key_count {
            return Err(MetaDbError::Corruption(format!(
                "meta chain page {next_pid} has wrong header (type={:?}, key_count={})",
                header.page_type, header.key_count,
            )));
        }
        let payload = page.payload();
        let header_off = if is_head { head_extra } else { 0 };
        let chunk_len =
            u32::from_le_bytes(payload[header_off..header_off + 4].try_into().unwrap()) as usize;
        let next = u64::from_le_bytes(payload[header_off + 8..header_off + 16].try_into().unwrap())
            as PageId;
        let entries_off = header_off + CHAIN_HEADER_BYTES;
        for i in 0..chunk_len {
            let off = entries_off + i * 8;
            let pid = u64::from_le_bytes(payload[off..off + 8].try_into().unwrap()) as PageId;
            page_table.push(pid);
        }
        next_pid = next;
        is_head = false;
    }
    Ok(page_table)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn fixture() -> (TempDir, Arc<PageStore>, Arc<PageCache>) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let store = Arc::new(PageStore::create(&path).unwrap());
        let cache = Arc::new(PageCache::new(store.clone(), 8 * 1024 * 1024));
        (dir, store, cache)
    }

    #[test]
    fn capacities_are_consistent() {
        // Continuation must be > head when head_extra > 0.
        assert!(CONTINUATION_CAPACITY >= head_capacity(0));
        assert!(head_capacity(0) > head_capacity(64));
        // Sanity: head with no extra bytes must fit at least 500 entries
        // (the previous single-page cap was 503; we lose 1 to next_pid).
        assert!(head_capacity(0) >= 500);
    }

    #[test]
    fn round_trip_within_one_page() {
        let (_d, store, cache) = fixture();
        let head = store.allocate().unwrap();
        let table: Vec<PageId> = (10..40).collect();
        let chain = write_chain(
            &store,
            &cache,
            PageType::RefcountArray,
            0xFFFF,
            &[],
            &table,
            &[head],
            42,
        )
        .unwrap();
        assert_eq!(chain, vec![head]);
        let read = read_chain(&store, head, PageType::RefcountArray, 0xFFFF, 0).unwrap();
        assert_eq!(read.page_table, table);
        assert_eq!(read.chain_pids, vec![head]);
    }

    #[test]
    fn round_trip_spans_multiple_pages() {
        let (_d, store, cache) = fixture();
        let head = store.allocate().unwrap();
        // 3 chunks: head + 2 continuation pages.
        let n = head_capacity(0) + CONTINUATION_CAPACITY + 5;
        let table: Vec<PageId> = (1000..(1000 + n as PageId)).collect();
        let chain = write_chain(
            &store,
            &cache,
            PageType::RefcountArray,
            0xFFFF,
            &[],
            &table,
            &[head],
            42,
        )
        .unwrap();
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0], head);
        let read = read_chain(&store, head, PageType::RefcountArray, 0xFFFF, 0).unwrap();
        assert_eq!(read.page_table, table);
        assert_eq!(read.chain_pids, chain);
    }

    #[test]
    fn shrink_frees_trailing_meta_pages() {
        let (_d, store, cache) = fixture();
        let head = store.allocate().unwrap();
        // Grow to 3 pages.
        let n = head_capacity(0) + CONTINUATION_CAPACITY + 5;
        let table: Vec<PageId> = (1000..(1000 + n as PageId)).collect();
        let chain = write_chain(
            &store,
            &cache,
            PageType::RefcountArray,
            0xFFFF,
            &[],
            &table,
            &[head],
            42,
        )
        .unwrap();
        assert_eq!(chain.len(), 3);
        let original_deferred = store.deferred_free_len();

        // Shrink back to fit on head only.
        let small_table: Vec<PageId> = (1..50).collect();
        let chain2 = write_chain(
            &store,
            &cache,
            PageType::RefcountArray,
            0xFFFF,
            &[],
            &small_table,
            &chain,
            100,
        )
        .unwrap();
        assert_eq!(chain2, vec![head]);
        // Two continuation pages were queued for reclaim.
        assert_eq!(store.deferred_free_len(), original_deferred + 2);

        // Re-read confirms the shorter chain.
        let read = read_chain(&store, head, PageType::RefcountArray, 0xFFFF, 0).unwrap();
        assert_eq!(read.page_table, small_table);
        assert_eq!(read.chain_pids, vec![head]);
    }

    #[test]
    fn head_extra_round_trips() {
        let (_d, store, cache) = fixture();
        let head = store.allocate().unwrap();
        let extra: Vec<u8> = (0..40u8).collect();
        let n = head_capacity(extra.len()) + 3; // forces continuation
        let table: Vec<PageId> = (500..(500 + n as PageId)).collect();
        let chain = write_chain(
            &store,
            &cache,
            PageType::CuckooData,
            0xFFFF,
            &extra,
            &table,
            &[head],
            42,
        )
        .unwrap();
        assert_eq!(chain.len(), 2);
        let read = read_chain(&store, head, PageType::CuckooData, 0xFFFF, extra.len()).unwrap();
        assert_eq!(read.head_extra, extra);
        assert_eq!(read.page_table, table);
    }

    #[test]
    fn empty_table_writes_just_the_head() {
        let (_d, store, cache) = fixture();
        let head = store.allocate().unwrap();
        let chain = write_chain(
            &store,
            &cache,
            PageType::RefcountArray,
            0xFFFF,
            &[],
            &[],
            &[head],
            7,
        )
        .unwrap();
        assert_eq!(chain, vec![head]);
        let read = read_chain(&store, head, PageType::RefcountArray, 0xFFFF, 0).unwrap();
        assert!(read.page_table.is_empty());
    }
}
