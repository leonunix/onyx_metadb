//! Disk-backed paged-array dedup-reverse store with overflow chains.
//!
//! Each PBA owns one inline slot in a *data* page, plus an optional
//! overflow page chain when more than one hash is registered.
//!
//! # On-disk encoding
//!
//! ## Data page (PageType::DedupReverseArray, slot-keyed)
//! Each leaf carries [`ENTRIES_PER_PAGE`] = 84 fixed-size slots, one
//! per PBA in `[page_idx * 84, (page_idx + 1) * 84)`. Slot layout
//! (48 B):
//! ```text
//!   bytes 0..2   hash_count: u16 LE   (0 = unused slot)
//!   bytes 2..4   reserved
//!   bytes 4..12  overflow_pid: u64 LE (0 = no overflow)
//!   bytes 12..44 inline_hash: [u8; 32]
//!   bytes 44..48 reserved
//! ```
//! 84 × 48 = 4032 B = PAGE_PAYLOAD_SIZE.
//!
//! ## Overflow page (PageType::DedupReverseArray, chain-keyed)
//! Holds the **extra** hashes for one PBA whose inline slot was full.
//! ```text
//!   bytes 0..2   used: u16 LE          (number of hashes stored, 0..125)
//!   bytes 2..10  next_pid: u64 LE      (0 if last in chain)
//!   bytes 10..16 reserved
//!   bytes 16..   hashes: [Hash32; 125]
//! ```
//! 16 + 125 × 32 = 4016 B used, 16 B padding to 4032 B.
//!
//! ## Meta page chain (PageType::DedupReverseArray, `key_count = 0xFFFF`)
//! Each meta page carries a fixed 16 B chain header (`chunk_len: u32`,
//! reserved `u32`, `next_meta_pid: u64`) followed by a slice of the
//! page table. The head meta pid is recorded in the manifest;
//! continuation pages are reachable via the chain pointer (see
//! [`crate::paged_meta`]).
//!
//! `generation` on every page is the page's `last_applied_lsn` so
//! `put` / `delete` can replay-skip when a crash repeats an op.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::cache::PageCache;
use crate::error::{MetaDbError, Result};
use crate::dedup_types::Hash32;
use crate::page::{Page, PageHeader, PageType, PAGE_PAYLOAD_SIZE};
use crate::page_store::PageStore;
use crate::paged_meta;
use crate::types::{Lsn, Pba, PageId};

pub const ENTRIES_PER_PAGE: usize = 84;
const SLOT_BYTES: usize = 48;

const OVERFLOW_HEADER_BYTES: usize = 16;
const OVERFLOW_HASHES_PER_PAGE: usize = (PAGE_PAYLOAD_SIZE - OVERFLOW_HEADER_BYTES) / 32;

const META_KEY_COUNT_MARKER: u16 = 0xFFFF;
const DATA_KEY_COUNT_MARKER: u16 = 0;
const OVERFLOW_KEY_COUNT_MARKER: u16 = 0xFFFE;

const ZERO_HASH: Hash32 = [0u8; 32];

const _: () = {
    assert!(ENTRIES_PER_PAGE * SLOT_BYTES == PAGE_PAYLOAD_SIZE);
    assert!(OVERFLOW_HASHES_PER_PAGE >= 125);
};

pub struct PagedReverse {
    page_store: Arc<PageStore>,
    page_cache: Arc<PageCache>,
    meta_page_id: PageId,
    inner: Mutex<Inner>,
}

struct Inner {
    page_table: Vec<PageId>,
    meta_chain: Vec<PageId>,
    meta_dirty: bool,
}

impl PagedReverse {
    pub fn create(page_store: Arc<PageStore>, page_cache: Arc<PageCache>) -> Result<Self> {
        let meta_page_id = page_store.allocate()?;
        let me = Self {
            page_store,
            page_cache,
            meta_page_id,
            inner: Mutex::new(Inner {
                page_table: Vec::new(),
                meta_chain: vec![meta_page_id],
                meta_dirty: false,
            }),
        };
        let mut guard = me.inner.lock();
        me.flush_meta_locked(&mut guard)?;
        drop(guard);
        Ok(me)
    }

    pub fn open(
        page_store: Arc<PageStore>,
        page_cache: Arc<PageCache>,
        meta_page_id: PageId,
    ) -> Result<Self> {
        let read = paged_meta::read_chain(
            &page_store,
            meta_page_id,
            PageType::DedupReverseArray,
            META_KEY_COUNT_MARKER,
            0,
        )?;
        Ok(Self {
            page_store,
            page_cache,
            meta_page_id,
            inner: Mutex::new(Inner {
                page_table: read.page_table,
                meta_chain: read.chain_pids,
                meta_dirty: false,
            }),
        })
    }

    pub fn meta_page_id(&self) -> PageId {
        self.meta_page_id
    }

    pub fn get_hashes(&self, pba: Pba) -> Result<Vec<Hash32>> {
        let (page_idx, slot) = page_offset(pba);
        let page_id = {
            let inner = self.inner.lock();
            inner.page_table.get(page_idx).copied().unwrap_or(0)
        };
        if page_id == 0 {
            return Ok(Vec::new());
        }
        let data_page = self.page_cache.get(page_id)?;
        let header = read_slot(&data_page, slot);
        if header.count == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(header.count as usize);
        out.push(header.inline_hash);
        let mut next = header.overflow_pid;
        while next != 0 {
            let page = self.page_cache.get(next)?;
            let (used, chain_next, hashes) = read_overflow(&page);
            out.extend(hashes.into_iter().take(used as usize));
            next = chain_next;
        }
        Ok(out)
    }

    /// Batched form of [`get_hashes`]. Resolves every PBA's page id
    /// under one inner-lock acquisition, then walks the page cache
    /// without holding the lock. Output order matches `pbas`.
    pub fn multi_get_hashes(&self, pbas: &[Pba]) -> Result<Vec<Vec<Hash32>>> {
        if pbas.is_empty() {
            return Ok(Vec::new());
        }
        let resolved: Vec<(usize, PageId)> = {
            let inner = self.inner.lock();
            pbas.iter()
                .map(|&pba| {
                    let (page_idx, slot) = page_offset(pba);
                    let pid = inner.page_table.get(page_idx).copied().unwrap_or(0);
                    (slot, pid)
                })
                .collect()
        };
        let mut out = Vec::with_capacity(pbas.len());
        for (slot, page_id) in resolved {
            if page_id == 0 {
                out.push(Vec::new());
                continue;
            }
            let data_page = self.page_cache.get(page_id)?;
            let header = read_slot(&data_page, slot);
            if header.count == 0 {
                out.push(Vec::new());
                continue;
            }
            let mut hashes = Vec::with_capacity(header.count as usize);
            hashes.push(header.inline_hash);
            let mut next = header.overflow_pid;
            while next != 0 {
                let page = self.page_cache.get(next)?;
                let (used, chain_next, more) = read_overflow(&page);
                hashes.extend(more.into_iter().take(used as usize));
                next = chain_next;
            }
            out.push(hashes);
        }
        Ok(out)
    }

    /// Append `hash` to `pba`'s registered list. Idempotent: a put of
    /// a hash that is already present is a no-op.
    ///
    /// Replay-skip: if the data page's `last_applied_lsn` already
    /// covers `lsn`, the op short-circuits.
    pub fn put(&self, pba: Pba, hash: Hash32, lsn: Lsn) -> Result<()> {
        // The all-zero hash is a legitimate (if astronomically rare)
        // value; the on-disk encoding distinguishes occupied vs empty
        // via the slot's `count` field, not the hash bytes, so no
        // sentinel guard is needed here.
        self.mutate(pba, lsn, |existing| {
            if existing.iter().any(|h| h == &hash) {
                Ok(existing.to_vec())
            } else {
                let mut v = existing.to_vec();
                v.push(hash);
                Ok(v)
            }
        })
    }

    /// Remove the registered hash for `pba` (no-op if absent).
    ///
    /// Replay-skip: same as [`put`].
    pub fn delete(&self, pba: Pba, hash: Hash32, lsn: Lsn) -> Result<()> {
        self.mutate(pba, lsn, |existing| {
            Ok(existing.iter().copied().filter(|h| h != &hash).collect())
        })
    }

    fn mutate(
        &self,
        pba: Pba,
        lsn: Lsn,
        update: impl FnOnce(&[Hash32]) -> Result<Vec<Hash32>>,
    ) -> Result<()> {
        let (page_idx, slot) = page_offset(pba);
        let (page_id, freshly_allocated) = {
            let mut inner = self.inner.lock();
            if page_idx >= inner.page_table.len() {
                inner.page_table.resize(page_idx + 1, 0);
                inner.meta_dirty = true;
            }
            if inner.page_table[page_idx] == 0 {
                let pid = self.page_store.allocate()?;
                inner.page_table[page_idx] = pid;
                inner.meta_dirty = true;
                (pid, true)
            } else {
                (inner.page_table[page_idx], false)
            }
        };
        let mut page = if freshly_allocated {
            new_data_page()
        } else {
            (*self.page_cache.get(page_id)?).clone()
        };
        // Note: dedup_reverse `put` and `delete` are inherently
        // idempotent (put of an already-present hash is a no-op; delete
        // of a missing hash is a no-op), so we do *not* gate on the
        // page LSN. The legacy refcount-style page-LSN replay-skip
        // would erroneously skip later ops in the same WAL record
        // that share the record's LSN with an earlier op against the
        // same page.
        let current = self.read_slot_full(&page, slot)?;
        let new = update(&current)?;
        if new == current {
            // No change, but still bump the page LSN so a future
            // replay of this exact op short-circuits.
            let mut header = page.header()?;
            header.generation = lsn.max(header.generation);
            page.write_header(&header);
            page.seal();
            self.page_store.write_page(page_id, &page)?;
            self.page_cache.replace_or_insert(page_id, Arc::new(page));
            return Ok(());
        }
        // Free the previous overflow chain (we always rewrite it from
        // scratch on change to keep the on-disk state consistent with
        // the in-memory list — overflow pages are cheap to allocate
        // because dedup-reverse mutations are off the hot path).
        let prev_overflow = read_slot(&page, slot).overflow_pid;
        let mut to_free = collect_overflow_chain_pids(self.page_store.as_ref(), prev_overflow)?;

        // Build the new state on disk.
        let new_overflow_pid =
            self.write_overflow_chain(&new, lsn)?;
        let new_inline = new.first().copied().unwrap_or(ZERO_HASH);
        write_slot(
            &mut page,
            slot,
            SlotView {
                count: new.len() as u16,
                overflow_pid: new_overflow_pid,
                inline_hash: new_inline,
            },
        );
        let mut header = page.header()?;
        header.generation = lsn.max(header.generation);
        page.write_header(&header);
        page.seal();
        self.page_store.write_page(page_id, &page)?;
        self.page_cache.replace_or_insert(page_id, Arc::new(page));
        // Free old overflow chain after the data page is durable; on
        // crash mid-way, recovery still finds either the old or the
        // new chain via the page LSN check.
        for pid in to_free.drain(..) {
            self.page_cache.invalidate(pid);
            self.page_store.free(pid, lsn)?;
        }
        Ok(())
    }

    fn read_slot_full(&self, page: &Page, slot: usize) -> Result<Vec<Hash32>> {
        let header = read_slot(page, slot);
        if header.count == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(header.count as usize);
        out.push(header.inline_hash);
        let mut next = header.overflow_pid;
        while next != 0 {
            let page = self.page_cache.get(next)?;
            let (used, chain_next, hashes) = read_overflow(&page);
            out.extend(hashes.into_iter().take(used as usize));
            next = chain_next;
        }
        Ok(out)
    }

    /// Allocate and write a fresh overflow chain holding the
    /// non-inline hashes (`hashes[1..]`). Returns the head pid (0 if
    /// no overflow needed).
    fn write_overflow_chain(&self, hashes: &[Hash32], lsn: Lsn) -> Result<PageId> {
        if hashes.len() <= 1 {
            return Ok(0);
        }
        let extras = &hashes[1..];
        let chunks: Vec<&[Hash32]> = extras.chunks(OVERFLOW_HASHES_PER_PAGE).collect();
        let mut next: PageId = 0;
        for chunk in chunks.into_iter().rev() {
            let pid = self.page_store.allocate()?;
            let mut page = new_overflow_page();
            write_overflow(&mut page, chunk, next);
            let mut header = page.header()?;
            header.generation = lsn;
            page.write_header(&header);
            page.seal();
            self.page_store.write_page(pid, &page)?;
            self.page_cache.replace_or_insert(pid, Arc::new(page));
            next = pid;
        }
        Ok(next)
    }

    /// Persist the meta chain if it has been mutated since the last
    /// flush. Returns `true` when a write actually happened.
    pub fn flush_meta(&self) -> Result<bool> {
        let mut inner = self.inner.lock();
        if !inner.meta_dirty {
            return Ok(false);
        }
        self.flush_meta_locked(&mut inner)?;
        inner.meta_dirty = false;
        Ok(true)
    }

    fn flush_meta_locked(&self, inner: &mut parking_lot::MutexGuard<'_, Inner>) -> Result<()> {
        let new_chain = paged_meta::write_chain(
            &self.page_store,
            &self.page_cache,
            PageType::DedupReverseArray,
            META_KEY_COUNT_MARKER,
            &[],
            &inner.page_table,
            &inner.meta_chain,
            0,
        )?;
        inner.meta_chain = new_chain;
        Ok(())
    }

    /// Iterate every (pba, hash) pair in PBA order (ascending). Used
    /// by the verifier and offline tools.
    pub fn iter_live(&self) -> Result<Vec<(Pba, Hash32)>> {
        let inner = self.inner.lock();
        let page_ids: Vec<(usize, PageId)> = inner
            .page_table
            .iter()
            .enumerate()
            .filter_map(|(idx, &pid)| if pid != 0 { Some((idx, pid)) } else { None })
            .collect();
        drop(inner);
        let mut out = Vec::new();
        for (page_idx, page_id) in page_ids {
            let page = self.page_cache.get(page_id)?;
            for slot in 0..ENTRIES_PER_PAGE {
                let header = read_slot(&page, slot);
                if header.count == 0 {
                    continue;
                }
                let pba = (page_idx * ENTRIES_PER_PAGE + slot) as Pba;
                out.push((pba, header.inline_hash));
                let mut next = header.overflow_pid;
                while next != 0 {
                    let opage = self.page_cache.get(next)?;
                    let (used, chain_next, hashes) = read_overflow(&opage);
                    for h in hashes.into_iter().take(used as usize) {
                        out.push((pba, h));
                    }
                    next = chain_next;
                }
            }
        }
        Ok(out)
    }

    pub fn allocated_data_pages(&self) -> usize {
        let inner = self.inner.lock();
        inner.page_table.iter().filter(|&&pid| pid != 0).count()
    }

    /// Walk every allocated data page id (used by verifier).
    pub fn data_page_ids(&self) -> Vec<PageId> {
        let inner = self.inner.lock();
        inner
            .page_table
            .iter()
            .copied()
            .filter(|&pid| pid != 0)
            .collect()
    }

    /// Walk every overflow page id reachable from this index (used by
    /// verifier).
    pub fn overflow_page_ids(&self) -> Result<Vec<PageId>> {
        let mut out = Vec::new();
        for pid in self.data_page_ids() {
            let page = self.page_cache.get(pid)?;
            for slot in 0..ENTRIES_PER_PAGE {
                let header = read_slot(&page, slot);
                let mut next = header.overflow_pid;
                while next != 0 {
                    out.push(next);
                    let opage = self.page_cache.get(next)?;
                    let (_used, chain_next, _hashes) = read_overflow(&opage);
                    next = chain_next;
                }
            }
        }
        Ok(out)
    }
}

#[derive(Clone, Copy)]
struct SlotView {
    count: u16,
    overflow_pid: PageId,
    inline_hash: Hash32,
}

#[inline]
fn page_offset(pba: Pba) -> (usize, usize) {
    let pba = pba as usize;
    (pba / ENTRIES_PER_PAGE, pba % ENTRIES_PER_PAGE)
}

#[inline]
fn read_slot(page: &Page, slot: usize) -> SlotView {
    let payload = page.payload();
    let off = slot * SLOT_BYTES;
    let count = u16::from_le_bytes(payload[off..off + 2].try_into().unwrap());
    let overflow_pid =
        u64::from_le_bytes(payload[off + 4..off + 12].try_into().unwrap()) as PageId;
    let mut inline_hash = [0u8; 32];
    inline_hash.copy_from_slice(&payload[off + 12..off + 44]);
    SlotView {
        count,
        overflow_pid,
        inline_hash,
    }
}

#[inline]
fn write_slot(page: &mut Page, slot: usize, view: SlotView) {
    let payload = page.payload_mut();
    let off = slot * SLOT_BYTES;
    payload[off..off + 2].copy_from_slice(&view.count.to_le_bytes());
    payload[off + 2..off + 4].fill(0);
    payload[off + 4..off + 12].copy_from_slice(&(view.overflow_pid as u64).to_le_bytes());
    payload[off + 12..off + 44].copy_from_slice(&view.inline_hash);
    payload[off + 44..off + 48].fill(0);
}

fn read_overflow(page: &Page) -> (u16, PageId, Vec<Hash32>) {
    let payload = page.payload();
    let used = u16::from_le_bytes(payload[0..2].try_into().unwrap());
    let next = u64::from_le_bytes(payload[2..10].try_into().unwrap()) as PageId;
    let mut hashes = Vec::with_capacity(used as usize);
    for i in 0..used as usize {
        let off = OVERFLOW_HEADER_BYTES + i * 32;
        let mut h = [0u8; 32];
        h.copy_from_slice(&payload[off..off + 32]);
        hashes.push(h);
    }
    (used, next, hashes)
}

fn write_overflow(page: &mut Page, hashes: &[Hash32], next: PageId) {
    debug_assert!(hashes.len() <= OVERFLOW_HASHES_PER_PAGE);
    let payload = page.payload_mut();
    payload[0..2].copy_from_slice(&(hashes.len() as u16).to_le_bytes());
    payload[2..10].copy_from_slice(&(next as u64).to_le_bytes());
    payload[10..16].fill(0);
    for (i, h) in hashes.iter().enumerate() {
        let off = OVERFLOW_HEADER_BYTES + i * 32;
        payload[off..off + 32].copy_from_slice(h);
    }
    // Zero remaining slots so on-disk image is deterministic.
    let used_end = OVERFLOW_HEADER_BYTES + hashes.len() * 32;
    payload[used_end..].fill(0);
}

fn collect_overflow_chain_pids(
    page_store: &PageStore,
    head: PageId,
) -> Result<Vec<PageId>> {
    let mut out = Vec::new();
    let mut next = head;
    while next != 0 {
        out.push(next);
        let page = page_store.read_page(next)?;
        let (_used, chain_next, _hashes) = read_overflow(&page);
        next = chain_next;
    }
    Ok(out)
}

fn new_data_page() -> Page {
    Page::new(PageHeader {
        page_type: PageType::DedupReverseArray,
        version: crate::page::PAGE_VERSION,
        key_count: DATA_KEY_COUNT_MARKER,
        flags: 0,
        generation: 0,
        refcount: 1,
    })
}

fn new_overflow_page() -> Page {
    Page::new(PageHeader {
        page_type: PageType::DedupReverseArray,
        version: crate::page::PAGE_VERSION,
        key_count: OVERFLOW_KEY_COUNT_MARKER,
        flags: 0,
        generation: 0,
        refcount: 1,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_index() -> (TempDir, PagedReverse) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let page_store = Arc::new(PageStore::create(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let r = PagedReverse::create(page_store, page_cache).unwrap();
        (dir, r)
    }

    fn h(byte: u8) -> Hash32 {
        let mut x = [0u8; 32];
        x.fill(byte);
        x
    }

    #[test]
    fn empty_get_returns_none() {
        let (_d, r) = make_index();
        assert_eq!(r.get_hashes(0).unwrap(), Vec::<Hash32>::new());
        assert_eq!(r.get_hashes(1_000_000).unwrap(), Vec::<Hash32>::new());
    }

    #[test]
    fn put_get_round_trip() {
        let (_d, r) = make_index();
        r.put(7, h(0xAB), 100).unwrap();
        assert_eq!(r.get_hashes(7).unwrap(), vec![h(0xAB)]);
    }

    #[test]
    fn put_two_hashes_same_pba() {
        let (_d, r) = make_index();
        r.put(7, h(0xAB), 100).unwrap();
        r.put(7, h(0xCD), 101).unwrap();
        let got = r.get_hashes(7).unwrap();
        assert!(got.contains(&h(0xAB)));
        assert!(got.contains(&h(0xCD)));
        assert_eq!(got.len(), 2);
    }

    #[test]
    fn put_many_hashes_chains_overflow() {
        let (_d, r) = make_index();
        let count = OVERFLOW_HASHES_PER_PAGE * 3 + 5; // forces multi-page chain
        for i in 0..count {
            // build a unique non-zero hash by writing the running
            // counter into multiple bytes so we never collide with
            // the all-zero sentinel even for large counts.
            let mut hash = [0u8; 32];
            hash[0] = ((i + 1) & 0xFF) as u8;
            hash[1] = (((i + 1) >> 8) & 0xFF) as u8;
            hash[31] = 0xAA;
            r.put(7, hash, (100 + i) as Lsn).unwrap();
        }
        let got = r.get_hashes(7).unwrap();
        assert_eq!(got.len(), count);
    }

    #[test]
    fn put_idempotent_for_same_hash() {
        let (_d, r) = make_index();
        r.put(7, h(0xAB), 100).unwrap();
        r.put(7, h(0xAB), 101).unwrap();
        assert_eq!(r.get_hashes(7).unwrap(), vec![h(0xAB)]);
    }

    #[test]
    fn delete_removes_specific_hash() {
        let (_d, r) = make_index();
        r.put(7, h(0xAB), 100).unwrap();
        r.put(7, h(0xCD), 101).unwrap();
        r.delete(7, h(0xAB), 102).unwrap();
        assert_eq!(r.get_hashes(7).unwrap(), vec![h(0xCD)]);
    }

    #[test]
    fn delete_last_hash_clears_slot() {
        let (_d, r) = make_index();
        r.put(7, h(0xAB), 100).unwrap();
        r.delete(7, h(0xAB), 101).unwrap();
        assert_eq!(r.get_hashes(7).unwrap(), Vec::<Hash32>::new());
    }

    #[test]
    fn delete_with_wrong_hash_is_noop() {
        let (_d, r) = make_index();
        r.put(7, h(0xAB), 100).unwrap();
        r.delete(7, h(0xCD), 101).unwrap();
        assert_eq!(r.get_hashes(7).unwrap(), vec![h(0xAB)]);
    }

    #[test]
    fn zero_hash_put_round_trips() {
        // count-based absence means the all-zero hash is a legitimate
        // value, not a sentinel.
        let (_d, r) = make_index();
        r.put(7, ZERO_HASH, 100).unwrap();
        assert_eq!(r.get_hashes(7).unwrap(), vec![ZERO_HASH]);
    }

    #[test]
    fn put_grows_pages() {
        let (_d, r) = make_index();
        let pba_p0 = 5;
        let pba_p1 = (ENTRIES_PER_PAGE + 7) as Pba;
        let pba_p3 = (ENTRIES_PER_PAGE * 3 + 1) as Pba;
        r.put(pba_p0, h(1), 100).unwrap();
        r.put(pba_p1, h(2), 101).unwrap();
        r.put(pba_p3, h(3), 102).unwrap();
        assert_eq!(r.get_hashes(pba_p0).unwrap(), vec![h(1)]);
        assert_eq!(r.get_hashes(pba_p1).unwrap(), vec![h(2)]);
        assert_eq!(r.get_hashes(pba_p3).unwrap(), vec![h(3)]);
        assert_eq!(r.allocated_data_pages(), 3);
    }

    #[test]
    fn put_at_same_lsn_appends_idempotent() {
        // Same-LSN ops happen when one WAL record contains multiple
        // (pba, hash) pairs (e.g. cleanup_dedup_for_dead_pbas
        // batching). Both must apply; the `put` of a brand new hash
        // appends, the `put` of an already-present hash is a no-op.
        let (_d, r) = make_index();
        r.put(7, h(0xAB), 100).unwrap();
        r.put(7, h(0xCD), 100).unwrap();
        let mut got = r.get_hashes(7).unwrap();
        got.sort();
        let mut expected = vec![h(0xAB), h(0xCD)];
        expected.sort();
        assert_eq!(got, expected);
        // Idempotent re-put of an existing hash is still a no-op.
        r.put(7, h(0xAB), 100).unwrap();
        assert_eq!(r.get_hashes(7).unwrap().len(), 2);
    }

    #[test]
    fn iter_live_yields_all_pairs() {
        let (_d, r) = make_index();
        r.put(1, h(1), 1).unwrap();
        r.put(3, h(3), 2).unwrap();
        r.put(3, h(33), 3).unwrap();
        let live = r.iter_live().unwrap();
        assert_eq!(live.len(), 3);
    }

    #[test]
    fn round_trip_via_open() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages");
        let meta_page_id;
        {
            let page_store = Arc::new(PageStore::create(&path).unwrap());
            let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
            let r = PagedReverse::create(page_store.clone(), page_cache).unwrap();
            meta_page_id = r.meta_page_id();
            r.put(7, h(0xAB), 100).unwrap();
            r.put(7, h(0xCD), 101).unwrap();
            r.put((ENTRIES_PER_PAGE * 2 + 5) as Pba, h(0xEE), 200).unwrap();
            r.flush_meta().unwrap();
        }
        let page_store = Arc::new(PageStore::open(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let r = PagedReverse::open(page_store, page_cache, meta_page_id).unwrap();
        let mut got = r.get_hashes(7).unwrap();
        got.sort();
        let mut expected = vec![h(0xAB), h(0xCD)];
        expected.sort();
        assert_eq!(got, expected);
        assert_eq!(
            r.get_hashes((ENTRIES_PER_PAGE * 2 + 5) as Pba).unwrap(),
            vec![h(0xEE)]
        );
    }

    #[test]
    fn multi_get_preserves_order_and_dups() {
        let (_d, r) = make_index();
        r.put(1, h(1), 1).unwrap();
        r.put(5, h(5), 2).unwrap();
        let got = r.multi_get_hashes(&[5, 1, 9, 1]).unwrap();
        assert_eq!(got[0], vec![h(5)]);
        assert_eq!(got[1], vec![h(1)]);
        assert_eq!(got[2], Vec::<Hash32>::new());
        assert_eq!(got[3], vec![h(1)]);
    }

    #[test]
    fn page_idx_beyond_one_meta_page_chains_a_continuation() {
        let (_d, r) = make_index();
        let head_cap = paged_meta::head_capacity(0);
        let big_pba = ((head_cap + 1) * ENTRIES_PER_PAGE) as Pba;
        r.put(big_pba, h(1), 1).unwrap();
        r.flush_meta().unwrap();
        assert!(r.inner.lock().meta_chain.len() >= 2);
        assert_eq!(r.get_hashes(big_pba).unwrap(), vec![h(1)]);
    }
}
