//! Sorted-string-table format, writer, and reader.
//!
//! Each SST is a contiguous run of 4 KiB pages laid out as:
//!
//! ```text
//! page 0         — header (LsmData): SST-wide metadata in payload
//! page 1..=B     — bloom filter bits (LsmData): raw bytes in payload
//! page B+1..end  — body (LsmData): 63 sorted records per page
//! ```
//!
//! Fixed record size (64 B) means there is no per-record length prefix
//! and no per-block index — an SST-wide binary search by record index
//! maps onto `(body_page, record_in_page)` with integer arithmetic.
//!
//! # Why contiguous pages
//!
//! Keeping all of an SST's pages as a consecutive id range lets us
//! describe it in the manifest with `(head_page, page_count)` rather
//! than an extent map. `PageStore::allocate_run` bypasses the free list
//! so runs don't fragment; freed pages rejoin the list one-by-one and
//! are reused by the B+tree.
//!
//! # Header page payload
//!
//! ```text
//! offset  size  field
//! ------  ----  ---------------------------
//!   0      4    layout_version = 1
//!   4      8    record_count (u64)
//!  12      4    bloom_bit_count (u32)
//!  16      4    bloom_hash_count (u32)
//!  20      4    bloom_page_count (u32)
//!  24      4    body_page_count (u32)
//!  28     32    min_hash
//!  60     32    max_hash
//!  92     ...   reserved (zero)
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::cache::PageCache;
use crate::error::{MetaDbError, Result};
use crate::page::{PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use crate::page_store::PageStore;
use crate::types::{Lsn, PageId};

use super::bloom::{BloomFilter, DEFAULT_BITS_PER_ENTRY, hash_count_for};
use super::format::{HASH_SIZE, Hash32, LSM_RECORD_SIZE, RECORDS_PER_PAGE, Record};
use super::memtable::{DedupOp, LookupResult};

const SST_SCAN_PREFETCH_PAGES: u32 = 64;

/// Layout version written into each SST header. Bump on any breaking
/// on-disk change.
pub const SST_LAYOUT_VERSION: u32 = 1;

// Header payload offsets.
const OFF_LAYOUT_VERSION: usize = 0;
const OFF_RECORD_COUNT: usize = 4;
const OFF_BLOOM_BIT_COUNT: usize = 12;
const OFF_BLOOM_HASH_COUNT: usize = 16;
const OFF_BLOOM_PAGE_COUNT: usize = 20;
const OFF_BODY_PAGE_COUNT: usize = 24;
const OFF_MIN_HASH: usize = 28;
const OFF_MAX_HASH: usize = 60;
const HEADER_USED_BYTES: usize = 92;

const _: () = {
    assert!(OFF_LAYOUT_VERSION + 4 == OFF_RECORD_COUNT);
    assert!(OFF_RECORD_COUNT + 8 == OFF_BLOOM_BIT_COUNT);
    assert!(OFF_BLOOM_BIT_COUNT + 4 == OFF_BLOOM_HASH_COUNT);
    assert!(OFF_BLOOM_HASH_COUNT + 4 == OFF_BLOOM_PAGE_COUNT);
    assert!(OFF_BLOOM_PAGE_COUNT + 4 == OFF_BODY_PAGE_COUNT);
    assert!(OFF_BODY_PAGE_COUNT + 4 == OFF_MIN_HASH);
    assert!(OFF_MIN_HASH + 32 == OFF_MAX_HASH);
    assert!(OFF_MAX_HASH + 32 == HEADER_USED_BYTES);
    assert!(HEADER_USED_BYTES <= PAGE_PAYLOAD_SIZE);
};

/// Handle to a finalized SST. Small, `Copy`, suitable for the manifest.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SstHandle {
    pub head_page: PageId,
    pub record_count: u64,
    pub bloom_page_count: u32,
    pub body_page_count: u32,
    pub min_hash: Hash32,
    pub max_hash: Hash32,
}

impl SstHandle {
    /// Total number of pages owned by this SST.
    pub fn page_count(&self) -> u32 {
        1 + self.bloom_page_count + self.body_page_count
    }

    /// First page id of the body region (the sorted records start here).
    pub fn body_start_page(&self) -> PageId {
        self.head_page + 1 + self.bloom_page_count as u64
    }
}

/// Writes a new SST from a sorted stream of records.
pub struct SstWriter<'a> {
    page_store: &'a PageStore,
    bits_per_entry: u32,
    generation: Lsn,
}

impl<'a> SstWriter<'a> {
    /// New writer using the default bloom sizing (10 bits / entry).
    pub fn new(page_store: &'a PageStore, generation: Lsn) -> Self {
        Self {
            page_store,
            bits_per_entry: DEFAULT_BITS_PER_ENTRY,
            generation,
        }
    }

    /// Override the bloom filter's bits-per-entry budget. Clamped to
    /// `1..=32` to prevent runaway bloom pages.
    pub fn with_bits_per_entry(mut self, bits_per_entry: u32) -> Self {
        self.bits_per_entry = bits_per_entry.clamp(1, 32);
        self
    }

    /// Write an SST from a frozen memtable. Tombstones produce tombstone
    /// records in the output. Returns `None` if the memtable is empty.
    pub fn write_memtable(
        &self,
        frozen: &Arc<BTreeMap<Hash32, DedupOp>>,
    ) -> Result<Option<SstHandle>> {
        if frozen.is_empty() {
            return Ok(None);
        }
        let records: Vec<Record> = frozen
            .iter()
            .map(|(hash, op)| match *op {
                DedupOp::Put(v) => Record::put(hash, &v),
                DedupOp::Delete => Record::tombstone(hash),
            })
            .collect();
        Ok(Some(self.write_sorted(&records)?))
    }

    /// Write an SST from a sorted slice of records. The caller is
    /// responsible for ensuring `records` is sorted by hash and holds no
    /// duplicates (the memtable path satisfies this by construction).
    pub fn write_sorted(&self, records: &[Record]) -> Result<SstHandle> {
        if records.is_empty() {
            return Err(MetaDbError::InvalidArgument(
                "SstWriter::write_sorted called with no records".into(),
            ));
        }

        // Defensive sorted-check in debug; a broken writer would corrupt
        // every subsequent reader.
        debug_assert!(
            records.windows(2).all(|w| w[0].hash() <= w[1].hash()),
            "SstWriter input is not sorted"
        );

        let record_count = records.len();
        let mut bloom = BloomFilter::with_capacity(record_count, self.bits_per_entry);
        for r in records {
            bloom.insert(r.hash());
        }
        let bloom_bytes = bloom.bytes();
        let bloom_page_count = ceil_div(bloom_bytes.len(), PAGE_PAYLOAD_SIZE);
        let body_page_count = ceil_div(record_count, RECORDS_PER_PAGE);

        // 1 header + bloom + body
        let total_pages = 1 + bloom_page_count + body_page_count;

        let head_page = self.page_store.allocate_run(total_pages)?;

        // Build in id order and hand the whole run to PageStore. The SST
        // already lives in a contiguous allocation, so this becomes one
        // batched writev path instead of thousands of tiny pwrite calls.
        let mut sealed_pages = Vec::with_capacity(total_pages);
        sealed_pages.push((
            head_page,
            Arc::new(self.build_header_page(HeaderMetadata {
                record_count: record_count as u64,
                bloom: &bloom,
                bloom_page_count: bloom_page_count as u32,
                body_page_count: body_page_count as u32,
                min_hash: records[0].hash(),
                max_hash: records[record_count - 1].hash(),
            })),
        ));

        let mut cursor = head_page + 1;
        for chunk in bloom_bytes.chunks(PAGE_PAYLOAD_SIZE) {
            sealed_pages.push((cursor, Arc::new(self.build_bloom_page(chunk))));
            cursor += 1;
        }

        for page_idx in 0..body_page_count {
            let start = page_idx * RECORDS_PER_PAGE;
            let end = ((page_idx + 1) * RECORDS_PER_PAGE).min(record_count);
            sealed_pages.push((cursor, Arc::new(self.build_body_page(&records[start..end]))));
            cursor += 1;
        }
        self.page_store.write_sealed_page_runs(sealed_pages)?;

        Ok(SstHandle {
            head_page,
            record_count: record_count as u64,
            bloom_page_count: bloom_page_count as u32,
            body_page_count: body_page_count as u32,
            min_hash: *records[0].hash(),
            max_hash: *records[record_count - 1].hash(),
        })
    }

    fn build_header_page(&self, meta: HeaderMetadata<'_>) -> Page {
        let mut page = Page::new(PageHeader::new(PageType::LsmData, self.generation));
        let p = page.payload_mut();
        p[OFF_LAYOUT_VERSION..OFF_LAYOUT_VERSION + 4]
            .copy_from_slice(&SST_LAYOUT_VERSION.to_le_bytes());
        p[OFF_RECORD_COUNT..OFF_RECORD_COUNT + 8].copy_from_slice(&meta.record_count.to_le_bytes());
        p[OFF_BLOOM_BIT_COUNT..OFF_BLOOM_BIT_COUNT + 4]
            .copy_from_slice(&meta.bloom.bit_count().to_le_bytes());
        p[OFF_BLOOM_HASH_COUNT..OFF_BLOOM_HASH_COUNT + 4]
            .copy_from_slice(&meta.bloom.hash_count().to_le_bytes());
        p[OFF_BLOOM_PAGE_COUNT..OFF_BLOOM_PAGE_COUNT + 4]
            .copy_from_slice(&meta.bloom_page_count.to_le_bytes());
        p[OFF_BODY_PAGE_COUNT..OFF_BODY_PAGE_COUNT + 4]
            .copy_from_slice(&meta.body_page_count.to_le_bytes());
        p[OFF_MIN_HASH..OFF_MIN_HASH + 32].copy_from_slice(meta.min_hash);
        p[OFF_MAX_HASH..OFF_MAX_HASH + 32].copy_from_slice(meta.max_hash);
        page.seal();
        page
    }

    fn build_bloom_page(&self, chunk: &[u8]) -> Page {
        let mut page = Page::new(PageHeader::new(PageType::LsmData, self.generation));
        page.payload_mut()[..chunk.len()].copy_from_slice(chunk);
        page.seal();
        page
    }

    fn build_body_page(&self, records: &[Record]) -> Page {
        debug_assert!(records.len() <= RECORDS_PER_PAGE);
        let mut page = Page::new(PageHeader::new(PageType::LsmData, self.generation));
        page.set_key_count(records.len() as u16);
        let p = page.payload_mut();
        for (i, r) in records.iter().enumerate() {
            let off = i * LSM_RECORD_SIZE;
            p[off..off + LSM_RECORD_SIZE].copy_from_slice(r.as_bytes());
        }
        page.seal();
        page
    }
}

struct HeaderMetadata<'a> {
    record_count: u64,
    bloom: &'a BloomFilter,
    bloom_page_count: u32,
    body_page_count: u32,
    min_hash: &'a Hash32,
    max_hash: &'a Hash32,
}

/// Decoded SST header.
#[derive(Clone, Debug)]
struct SstHeader {
    record_count: u64,
    bloom_bit_count: u32,
    bloom_hash_count: u32,
    bloom_page_count: u32,
    body_page_count: u32,
    min_hash: Hash32,
    max_hash: Hash32,
}

/// Read-side accessor for a finalized SST.
#[derive(Debug)]
pub struct SstReader<'a> {
    page_cache: &'a PageCache,
    handle: SstHandle,
    header: SstHeader,
    bloom: BloomFilter,
}

impl<'a> SstReader<'a> {
    /// Open an SST given its handle. Reads the header page, loads the
    /// bloom filter bytes into memory, and validates that the on-disk
    /// metadata matches the handle.
    pub fn open(
        _page_store: &'a PageStore,
        page_cache: &'a PageCache,
        handle: SstHandle,
    ) -> Result<Self> {
        let (header, bloom) = load_header_and_bloom(page_cache, handle)?;

        Ok(Self {
            page_cache,
            handle,
            header,
            bloom,
        })
    }

    /// Handle that was used to open this reader.
    pub fn handle(&self) -> SstHandle {
        self.handle
    }

    /// Point lookup. The return value mirrors the memtable so callers
    /// that walk levels have a uniform shape:
    /// - `Hit(value)` — the SST contains a live put for this hash.
    /// - `Tombstone`  — the SST contains a tombstone for this hash.
    ///   Callers MUST NOT consult older SSTs.
    /// - `Miss`       — the SST does not contain this hash. Older SSTs
    ///   may.
    pub fn get(&self, hash: &Hash32) -> Result<LookupResult> {
        get_from_sst(
            self.page_cache,
            self.handle,
            &self.header,
            &self.bloom,
            hash,
        )
    }

    /// Batched point lookup against one immutable SST.
    ///
    /// Results follow input order. Compared with calling [`Self::get`]
    /// repeatedly, this shares body-page probes across the batch: every
    /// binary-search round groups keys by the page they would inspect and
    /// fetches each page at most once.
    pub fn multi_get(&self, hashes: &[Hash32]) -> Result<Vec<LookupResult>> {
        multi_get_from_sst(
            self.page_cache,
            self.handle,
            &self.header,
            &self.bloom,
            hashes,
        )
    }

    /// Iterator over every record in hash order. Used by compaction.
    pub fn scan(&self) -> SstScan<'_> {
        SstScan {
            page_cache: self.page_cache,
            body_start: self.handle.body_start_page(),
            body_pages: self.handle.body_page_count,
            next_page_idx: 0,
            buffered: Vec::new(),
            buffered_off: 0,
            page_batch: Vec::new(),
            page_batch_next: 0,
        }
    }

    /// Collect records whose hash starts with `prefix`.
    ///
    /// Unlike [`Self::scan`], this binary-searches into the sorted body and
    /// only reads pages that can contain the requested prefix. This is the hot
    /// path for `dedup_reverse` cleanup, where the prefix is the 8-byte PBA.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<Record>> {
        scan_prefix_from_sst(self.page_cache, self.handle, &self.header, prefix)
    }
}

/// Owning read-side accessor intended for LSM point-lookup caching.
#[derive(Debug)]
pub struct CachedSstReader {
    page_cache: Arc<PageCache>,
    handle: SstHandle,
    header: SstHeader,
    bloom: BloomFilter,
}

impl CachedSstReader {
    /// Open an SST and keep the immutable header + bloom filter in this
    /// reader. Body pages still flow through the shared `PageCache`.
    pub fn open(page_cache: Arc<PageCache>, handle: SstHandle) -> Result<Self> {
        let (header, bloom) = load_header_and_bloom(&page_cache, handle)?;
        Ok(Self {
            page_cache,
            handle,
            header,
            bloom,
        })
    }

    /// Handle that was used to open this reader.
    pub fn handle(&self) -> SstHandle {
        self.handle
    }

    /// Point lookup using the cached bloom filter.
    pub fn get(&self, hash: &Hash32) -> Result<LookupResult> {
        get_from_sst(
            &self.page_cache,
            self.handle,
            &self.header,
            &self.bloom,
            hash,
        )
    }

    /// Batched point lookup using the cached bloom filter/header.
    pub fn multi_get(&self, hashes: &[Hash32]) -> Result<Vec<LookupResult>> {
        multi_get_from_sst(
            &self.page_cache,
            self.handle,
            &self.header,
            &self.bloom,
            hashes,
        )
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<Record>> {
        scan_prefix_from_sst(&self.page_cache, self.handle, &self.header, prefix)
    }
}

fn load_header_and_bloom(
    page_cache: &PageCache,
    handle: SstHandle,
) -> Result<(SstHeader, BloomFilter)> {
    let header_page = page_cache.get(handle.head_page)?;
    let header = decode_header(&header_page, handle.head_page)?;

    // Cross-check: the handle and header must agree.
    if header.record_count != handle.record_count
        || header.bloom_page_count != handle.bloom_page_count
        || header.body_page_count != handle.body_page_count
        || header.min_hash != handle.min_hash
        || header.max_hash != handle.max_hash
    {
        return Err(MetaDbError::Corruption(format!(
            "SST header at page {} disagrees with handle",
            handle.head_page,
        )));
    }

    // Read bloom filter bytes once per cached reader.
    let mut bloom_bytes = Vec::with_capacity((header.bloom_bit_count / 8) as usize);
    for i in 0..header.bloom_page_count as u64 {
        let page = page_cache.get(handle.head_page + 1 + i)?;
        let remaining = (header.bloom_bit_count as usize / 8) - bloom_bytes.len();
        let take = remaining.min(PAGE_PAYLOAD_SIZE);
        bloom_bytes.extend_from_slice(&page.payload()[..take]);
    }
    let bloom =
        BloomFilter::from_parts(bloom_bytes, header.bloom_bit_count, header.bloom_hash_count);
    Ok((header, bloom))
}

fn get_from_sst(
    page_cache: &PageCache,
    handle: SstHandle,
    header: &SstHeader,
    bloom: &BloomFilter,
    hash: &Hash32,
) -> Result<LookupResult> {
    if hash < &handle.min_hash || hash > &handle.max_hash {
        return Ok(LookupResult::Miss);
    }
    if !bloom.maybe_contains(hash) {
        return Ok(LookupResult::Miss);
    }
    // Binary search across the body: treat records as one flat
    // sorted array indexed 0..record_count. Each read fetches one
    // page and does an intra-page binary search. With ~log2(N/63)
    // outer steps and constant inner work, a 1 M-record SST costs
    // ~14 page reads at the absolute worst case.
    let mut lo = 0u64;
    let mut hi = header.record_count; // exclusive
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let page_idx = (mid as usize) / RECORDS_PER_PAGE;
        let page = page_cache.get(handle.body_start_page() + page_idx as u64)?;
        let page_records = page.key_count() as usize;
        match search_page(&page, page_records, hash)? {
            IntraPageResult::Found(kind, value) => {
                return Ok(match kind {
                    super::format::KIND_PUT => LookupResult::Hit(value),
                    super::format::KIND_DELETE => LookupResult::Tombstone,
                    other => {
                        return Err(MetaDbError::Corruption(format!(
                            "unknown SST record kind byte {other}",
                        )));
                    }
                });
            }
            IntraPageResult::TooLow => lo = (page_idx + 1) as u64 * RECORDS_PER_PAGE as u64,
            IntraPageResult::TooHigh => hi = page_idx as u64 * RECORDS_PER_PAGE as u64,
        }
    }
    Ok(LookupResult::Miss)
}

#[derive(Clone, Copy)]
struct BatchProbe {
    input_idx: usize,
    hash: Hash32,
    lo_page: u32,
    hi_page: u32,
    next_page: Option<u32>,
}

fn multi_get_from_sst(
    page_cache: &PageCache,
    handle: SstHandle,
    header: &SstHeader,
    bloom: &BloomFilter,
    hashes: &[Hash32],
) -> Result<Vec<LookupResult>> {
    if hashes.is_empty() {
        return Ok(Vec::new());
    }
    if header.record_count > 0 && header.body_page_count == 0 {
        return Err(MetaDbError::Corruption(format!(
            "SST head page {} has records but no body pages",
            handle.head_page
        )));
    }

    let mut out = vec![LookupResult::Miss; hashes.len()];
    let mut pending = Vec::with_capacity(hashes.len());
    for (input_idx, hash) in hashes.iter().enumerate() {
        if hash < &handle.min_hash || hash > &handle.max_hash {
            continue;
        }
        if !bloom.maybe_contains(hash) {
            continue;
        }
        pending.push(BatchProbe {
            input_idx,
            hash: *hash,
            lo_page: 0,
            hi_page: header.body_page_count,
            next_page: Some(estimate_probe_page(handle, header.body_page_count, hash)),
        });
    }

    while !pending.is_empty() {
        let mut page_probes = Vec::with_capacity(pending.len());
        for mut probe in pending.drain(..) {
            if probe.lo_page >= probe.hi_page {
                continue;
            }
            let page_idx = probe
                .next_page
                .take()
                .filter(|page_idx| *page_idx >= probe.lo_page && *page_idx < probe.hi_page)
                .unwrap_or_else(|| probe.lo_page + (probe.hi_page - probe.lo_page) / 2);
            page_probes.push((page_idx, probe));
        }
        if page_probes.is_empty() {
            break;
        }
        page_probes.sort_unstable_by_key(|(page_idx, _)| *page_idx);

        let mut groups = Vec::new();
        let mut page_ids = Vec::new();
        let mut pos = 0;
        while pos < page_probes.len() {
            let page_idx = page_probes[pos].0;
            let start = pos;
            pos += 1;
            while pos < page_probes.len() && page_probes[pos].0 == page_idx {
                pos += 1;
            }
            page_ids.push(handle.body_start_page() + page_idx as u64);
            groups.push((page_idx, start..pos));
        }
        let pages = page_cache.get_many(&page_ids)?;

        for ((page_idx, range), page) in groups.into_iter().zip(pages.into_iter()) {
            let page_records = page.key_count() as usize;
            for (_, probe) in page_probes[range].iter().copied() {
                match search_page_exact(&page, page_records, &probe.hash)? {
                    PageSearchResult::Found(kind, value) => {
                        out[probe.input_idx] = lookup_from_record_kind(kind, value)?;
                    }
                    PageSearchResult::BelowPage => {
                        if probe.lo_page < page_idx {
                            pending.push(BatchProbe {
                                hi_page: page_idx,
                                next_page: None,
                                ..probe
                            });
                        }
                    }
                    PageSearchResult::AbovePage => {
                        let next_page = page_idx + 1;
                        if next_page < probe.hi_page {
                            pending.push(BatchProbe {
                                lo_page: next_page,
                                next_page: None,
                                ..probe
                            });
                        }
                    }
                    PageSearchResult::InPageMiss => {}
                }
            }
        }
    }

    Ok(out)
}

fn estimate_probe_page(handle: SstHandle, body_page_count: u32, hash: &Hash32) -> u32 {
    if body_page_count <= 1 {
        return 0;
    }

    // Dedup keys are SHA-256 bytes sorted lexicographically. The first
    // 64 bits are enough to pick a near-target page for uniformly
    // distributed keys, and correctness does not depend on the estimate:
    // misses fall back to normal page-level binary search.
    let min = hash_prefix_u64(&handle.min_hash);
    let max = hash_prefix_u64(&handle.max_hash);
    let key = hash_prefix_u64(hash);
    if max <= min {
        return body_page_count / 2;
    }

    let span = (max - min) as u128;
    let offset = key.saturating_sub(min).min(max - min) as u128;
    let page = (offset * body_page_count as u128 / (span + 1)) as u32;
    page.min(body_page_count - 1)
}

fn hash_prefix_u64(hash: &Hash32) -> u64 {
    u64::from_be_bytes(hash[..8].try_into().unwrap())
}

fn scan_prefix_from_sst(
    page_cache: &PageCache,
    handle: SstHandle,
    header: &SstHeader,
    prefix: &[u8],
) -> Result<Vec<Record>> {
    if prefix.len() > HASH_SIZE {
        return Ok(Vec::new());
    }
    if prefix.is_empty() {
        return SstScan {
            page_cache,
            body_start: handle.body_start_page(),
            body_pages: handle.body_page_count,
            next_page_idx: 0,
            buffered: Vec::new(),
            buffered_off: 0,
            page_batch: Vec::new(),
            page_batch_next: 0,
        }
        .collect();
    }
    if header.record_count == 0 {
        return Ok(Vec::new());
    }

    let (lo, hi) = prefix_bounds_exclusive(prefix);
    if lo > handle.max_hash || hi.as_ref().is_some_and(|upper| upper <= &handle.min_hash) {
        return Ok(Vec::new());
    }

    let mut idx = lower_bound_record_index(page_cache, handle, header, &lo)?;
    let mut out = Vec::new();
    while idx < header.record_count {
        let page_idx = (idx as usize) / RECORDS_PER_PAGE;
        let page = page_cache.get(handle.body_start_page() + page_idx as u64)?;
        let page_records = page.key_count() as usize;
        if page_records == 0 {
            return Err(MetaDbError::Corruption(format!(
                "SST body page {} has zero records",
                handle.body_start_page() + page_idx as u64
            )));
        }

        let mut in_page = (idx as usize) % RECORDS_PER_PAGE;
        while in_page < page_records {
            let off = in_page * LSM_RECORD_SIZE;
            let p = page.payload();
            let hash: &Hash32 = (&p[off..off + HASH_SIZE]).try_into().unwrap();
            if hi.as_ref().is_some_and(|upper| hash >= upper) {
                return Ok(out);
            }
            if !hash.starts_with(prefix) {
                return Ok(out);
            }
            let bytes: &[u8; LSM_RECORD_SIZE] =
                (&p[off..off + LSM_RECORD_SIZE]).try_into().unwrap();
            let rec = Record::from_bytes(bytes).map_err(|e| {
                MetaDbError::Corruption(format!(
                    "SST body page {} has malformed prefix record {}: {e:?}",
                    handle.body_start_page() + page_idx as u64,
                    in_page
                ))
            })?;
            out.push(rec);
            in_page += 1;
        }
        idx = ((page_idx + 1) * RECORDS_PER_PAGE) as u64;
    }
    Ok(out)
}

fn lower_bound_record_index(
    page_cache: &PageCache,
    handle: SstHandle,
    header: &SstHeader,
    target: &Hash32,
) -> Result<u64> {
    let mut lo = 0u64;
    let mut hi = header.record_count;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let page_idx = (mid as usize) / RECORDS_PER_PAGE;
        let in_page = (mid as usize) % RECORDS_PER_PAGE;
        let page = page_cache.get(handle.body_start_page() + page_idx as u64)?;
        let page_records = page.key_count() as usize;
        if page_records == 0 || in_page >= page_records {
            return Err(MetaDbError::Corruption(format!(
                "SST body page {} cannot serve record index {}",
                handle.body_start_page() + page_idx as u64,
                mid
            )));
        }
        let off = in_page * LSM_RECORD_SIZE;
        let hash: &Hash32 = (&page.payload()[off..off + HASH_SIZE]).try_into().unwrap();
        if hash < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

fn prefix_bounds_exclusive(prefix: &[u8]) -> (Hash32, Option<Hash32>) {
    debug_assert!(prefix.len() <= HASH_SIZE);
    let mut lo = [0u8; HASH_SIZE];
    lo[..prefix.len()].copy_from_slice(prefix);

    let mut hi = [0u8; HASH_SIZE];
    for i in (0..prefix.len()).rev() {
        if prefix[i] < 0xFF {
            hi[..i].copy_from_slice(&prefix[..i]);
            hi[i] = prefix[i] + 1;
            return (lo, Some(hi));
        }
    }
    (lo, None)
}

fn search_page(page: &Page, page_records: usize, hash: &Hash32) -> Result<IntraPageResult> {
    Ok(match search_page_exact(page, page_records, hash)? {
        PageSearchResult::Found(kind, value) => IntraPageResult::Found(kind, value),
        PageSearchResult::BelowPage => IntraPageResult::TooHigh,
        PageSearchResult::AbovePage => IntraPageResult::TooLow,
        PageSearchResult::InPageMiss => IntraPageResult::TooHigh,
    })
}

fn search_page_exact(page: &Page, page_records: usize, hash: &Hash32) -> Result<PageSearchResult> {
    let p = page.payload();
    if page_records == 0 {
        return Err(MetaDbError::Corruption(
            "SST body page has zero records".into(),
        ));
    }
    // Quick rejection: if page's range does not cover the hash, we
    // can narrow the outer search in one side or the other.
    let first_hash: &Hash32 = (&p[0..32]).try_into().unwrap();
    let last_off = (page_records - 1) * LSM_RECORD_SIZE;
    let last_hash: &Hash32 = (&p[last_off..last_off + 32]).try_into().unwrap();
    if hash < first_hash {
        return Ok(PageSearchResult::BelowPage);
    }
    if hash > last_hash {
        return Ok(PageSearchResult::AbovePage);
    }
    // In range: binary-search inside the page.
    let mut lo = 0usize;
    let mut hi = page_records;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let off = mid * LSM_RECORD_SIZE;
        let mid_hash: &Hash32 = (&p[off..off + 32]).try_into().unwrap();
        match hash.cmp(mid_hash) {
            std::cmp::Ordering::Equal => {
                let kind = p[off + 32];
                let mut value_bytes = [0u8; super::format::DEDUP_VALUE_SIZE];
                value_bytes
                    .copy_from_slice(&p[off + 33..off + 33 + super::format::DEDUP_VALUE_SIZE]);
                return Ok(PageSearchResult::Found(
                    kind,
                    super::format::DedupValue(value_bytes),
                ));
            }
            std::cmp::Ordering::Less => hi = mid,
            std::cmp::Ordering::Greater => lo = mid + 1,
        }
    }
    // Key falls between this page's bounds but no record matches →
    // miss entirely.
    Ok(PageSearchResult::InPageMiss)
}

fn lookup_from_record_kind(kind: u8, value: super::format::DedupValue) -> Result<LookupResult> {
    Ok(match kind {
        super::format::KIND_PUT => LookupResult::Hit(value),
        super::format::KIND_DELETE => LookupResult::Tombstone,
        other => {
            return Err(MetaDbError::Corruption(format!(
                "unknown SST record kind byte {other}",
            )));
        }
    })
}

enum PageSearchResult {
    Found(u8, super::format::DedupValue),
    BelowPage,
    AbovePage,
    InPageMiss,
}

enum IntraPageResult {
    Found(u8, super::format::DedupValue),
    TooLow,
    TooHigh,
}

/// Iterator over the full record stream of an SST. Full scans bypass the LRU
/// and read small page batches so compaction and diagnostics do not evict the
/// point-lookup working set one page at a time.
pub struct SstScan<'a> {
    page_cache: &'a PageCache,
    body_start: PageId,
    body_pages: u32,
    next_page_idx: u32,
    buffered: Vec<Record>,
    buffered_off: usize,
    page_batch: Vec<Page>,
    page_batch_next: usize,
}

impl<'a> Iterator for SstScan<'a> {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Result<Record>> {
        loop {
            if self.buffered_off < self.buffered.len() {
                let r = self.buffered[self.buffered_off];
                self.buffered_off += 1;
                return Some(Ok(r));
            }
            if self.page_batch_next >= self.page_batch.len() {
                if self.next_page_idx >= self.body_pages {
                    return None;
                }
                let remaining = self.body_pages - self.next_page_idx;
                let batch_pages = remaining.min(SST_SCAN_PREFETCH_PAGES);
                let page_ids: Vec<PageId> = (0..batch_pages)
                    .map(|idx| self.body_start + (self.next_page_idx + idx) as u64)
                    .collect();
                self.next_page_idx += batch_pages;
                self.page_batch = match self.page_cache.get_many_bypass(&page_ids) {
                    Ok(pages) => pages,
                    Err(e) => return Some(Err(e)),
                };
                self.page_batch_next = 0;
            }
            let page = &self.page_batch[self.page_batch_next];
            self.page_batch_next += 1;
            let batch_start_idx = self.next_page_idx - self.page_batch.len() as u32;
            let page_id =
                self.body_start + batch_start_idx as u64 + self.page_batch_next as u64 - 1;
            let page_records = page.key_count() as usize;
            if page_records == 0 {
                return Some(Err(MetaDbError::Corruption(format!(
                    "SST body page {page_id} has zero records"
                ))));
            };
            self.buffered.clear();
            self.buffered_off = 0;
            let p = page.payload();
            for i in 0..page_records {
                let off = i * LSM_RECORD_SIZE;
                let bytes: &[u8; LSM_RECORD_SIZE] =
                    (&p[off..off + LSM_RECORD_SIZE]).try_into().unwrap();
                match Record::from_bytes(bytes) {
                    Ok(r) => self.buffered.push(r),
                    Err(e) => {
                        return Some(Err(MetaDbError::Corruption(format!(
                            "SST body page {page_id} has malformed record {i}: {e:?}",
                        ))));
                    }
                }
            }
        }
    }
}

fn decode_header(page: &Page, page_id: PageId) -> Result<SstHeader> {
    let h = page.header()?;
    if h.page_type != PageType::LsmData {
        return Err(MetaDbError::Corruption(format!(
            "SST head page {page_id} has wrong page type {:?}",
            h.page_type
        )));
    }
    let p = page.payload();
    let layout_version = u32::from_le_bytes(
        p[OFF_LAYOUT_VERSION..OFF_LAYOUT_VERSION + 4]
            .try_into()
            .unwrap(),
    );
    if layout_version != SST_LAYOUT_VERSION {
        return Err(MetaDbError::Corruption(format!(
            "SST head page {page_id} has unknown layout version {layout_version}",
        )));
    }
    let record_count = u64::from_le_bytes(
        p[OFF_RECORD_COUNT..OFF_RECORD_COUNT + 8]
            .try_into()
            .unwrap(),
    );
    let bloom_bit_count = u32::from_le_bytes(
        p[OFF_BLOOM_BIT_COUNT..OFF_BLOOM_BIT_COUNT + 4]
            .try_into()
            .unwrap(),
    );
    let bloom_hash_count = u32::from_le_bytes(
        p[OFF_BLOOM_HASH_COUNT..OFF_BLOOM_HASH_COUNT + 4]
            .try_into()
            .unwrap(),
    );
    let bloom_page_count = u32::from_le_bytes(
        p[OFF_BLOOM_PAGE_COUNT..OFF_BLOOM_PAGE_COUNT + 4]
            .try_into()
            .unwrap(),
    );
    let body_page_count = u32::from_le_bytes(
        p[OFF_BODY_PAGE_COUNT..OFF_BODY_PAGE_COUNT + 4]
            .try_into()
            .unwrap(),
    );
    let mut min_hash = [0u8; 32];
    min_hash.copy_from_slice(&p[OFF_MIN_HASH..OFF_MIN_HASH + 32]);
    let mut max_hash = [0u8; 32];
    max_hash.copy_from_slice(&p[OFF_MAX_HASH..OFF_MAX_HASH + 32]);

    // Expected hash count for the declared bit budget; reject mismatch.
    let expected_hash_count = hash_count_for(bloom_bit_count / record_count.max(1) as u32);
    if bloom_hash_count == 0 || bloom_hash_count > 64 {
        return Err(MetaDbError::Corruption(format!(
            "SST head page {page_id}: suspicious bloom_hash_count {bloom_hash_count}",
        )));
    }
    if bloom_bit_count == 0 && record_count > 0 {
        return Err(MetaDbError::Corruption(format!(
            "SST head page {page_id}: zero bloom_bit_count with {record_count} records",
        )));
    }
    let _ = expected_hash_count; // informational: we don't require exact match.

    Ok(SstHeader {
        record_count,
        bloom_bit_count,
        bloom_hash_count,
        bloom_page_count,
        body_page_count,
        min_hash,
        max_hash,
    })
}

fn ceil_div(num: usize, denom: usize) -> usize {
    num.div_ceil(denom)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::PageCache;
    use crate::config::PAGE_SIZE;
    use crate::lsm::format::DedupValue;
    use tempfile::TempDir;

    fn mk_ps() -> (TempDir, Arc<PageStore>, Arc<PageCache>) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("pages.onyx_meta");
        let ps = Arc::new(PageStore::create(&path).unwrap());
        let cache = Arc::new(PageCache::new(ps.clone(), 256 * PAGE_SIZE as u64));
        (dir, ps, cache)
    }

    fn h(n: u64) -> Hash32 {
        // Put the seed at the front so ordering by byte == ordering by
        // n-big-endian. Tests rely on this.
        let mut out = [0u8; 32];
        out[..8].copy_from_slice(&n.to_be_bytes());
        out
    }

    fn v(n: u8) -> DedupValue {
        let mut x = [0u8; 28];
        x[0] = n;
        DedupValue(x)
    }

    fn sorted_puts(count: u64) -> Vec<Record> {
        (0..count)
            .map(|i| Record::put(&h(i), &v(i as u8)))
            .collect()
    }

    #[test]
    fn empty_records_rejected() {
        let (_d, ps, _cache) = mk_ps();
        let w = SstWriter::new(&ps, 1);
        assert!(matches!(
            w.write_sorted(&[]).unwrap_err(),
            MetaDbError::InvalidArgument(_),
        ));
    }

    #[test]
    fn single_record_round_trip() {
        let (_d, ps, cache) = mk_ps();
        let records = vec![Record::put(&h(7), &v(42))];
        let handle = SstWriter::new(&ps, 1).write_sorted(&records).unwrap();
        assert_eq!(handle.record_count, 1);
        assert_eq!(handle.min_hash, h(7));
        assert_eq!(handle.max_hash, h(7));

        let reader = SstReader::open(&ps, &cache, handle).unwrap();
        match reader.get(&h(7)).unwrap() {
            LookupResult::Hit(got) => assert_eq!(got, v(42)),
            other => panic!("{other:?}"),
        }
        assert_eq!(reader.get(&h(8)).unwrap(), LookupResult::Miss);
    }

    #[test]
    fn tombstones_round_trip() {
        let (_d, ps, cache) = mk_ps();
        let records = vec![
            Record::put(&h(1), &v(10)),
            Record::tombstone(&h(2)),
            Record::put(&h(3), &v(30)),
        ];
        let handle = SstWriter::new(&ps, 1).write_sorted(&records).unwrap();
        let reader = SstReader::open(&ps, &cache, handle).unwrap();
        assert_eq!(reader.get(&h(1)).unwrap(), LookupResult::Hit(v(10)));
        assert_eq!(reader.get(&h(2)).unwrap(), LookupResult::Tombstone);
        assert_eq!(reader.get(&h(3)).unwrap(), LookupResult::Hit(v(30)));
    }

    #[test]
    fn multi_get_preserves_order_hits_misses_and_tombstones() {
        let (_d, ps, cache) = mk_ps();
        let mut records = sorted_puts(400);
        records[123] = Record::tombstone(&h(123));
        let handle = SstWriter::new(&ps, 1).write_sorted(&records).unwrap();
        let reader = SstReader::open(&ps, &cache, handle).unwrap();

        let hashes = vec![h(399), h(7), h(123), h(500), h(7), h(0)];
        let got = reader.multi_get(&hashes).unwrap();
        assert_eq!(
            got,
            vec![
                LookupResult::Hit(v(399u64 as u8)),
                LookupResult::Hit(v(7)),
                LookupResult::Tombstone,
                LookupResult::Miss,
                LookupResult::Hit(v(7)),
                LookupResult::Hit(v(0)),
            ]
        );
        for (hash, result) in hashes.iter().zip(got) {
            assert_eq!(result, reader.get(hash).unwrap());
        }
    }

    #[test]
    fn many_records_binary_search() {
        let (_d, ps, cache) = mk_ps();
        // Enough to span multiple body pages.
        let count = (RECORDS_PER_PAGE as u64 * 5) + 13; // 328
        let records = sorted_puts(count);
        let handle = SstWriter::new(&ps, 1).write_sorted(&records).unwrap();
        assert!(handle.body_page_count >= 6);

        let reader = SstReader::open(&ps, &cache, handle).unwrap();
        for i in 0..count {
            match reader.get(&h(i)).unwrap() {
                LookupResult::Hit(got) => assert_eq!(got, v(i as u8)),
                other => panic!("miss for {i}: {other:?}"),
            }
        }
        // Miss on key before the minimum.
        assert_eq!(reader.get(&h(count + 1)).unwrap(), LookupResult::Miss);
    }

    #[test]
    fn scan_returns_sorted_records() {
        let (_d, ps, cache) = mk_ps();
        let count = 200u64;
        let records = sorted_puts(count);
        let handle = SstWriter::new(&ps, 1).write_sorted(&records).unwrap();
        let reader = SstReader::open(&ps, &cache, handle).unwrap();

        let got: Vec<Record> = reader.scan().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(got.len(), count as usize);
        for (i, r) in got.iter().enumerate() {
            assert_eq!(*r.hash(), h(i as u64));
        }
    }

    #[test]
    fn scan_prefix_returns_only_matching_range_across_pages() {
        let (_d, ps, cache) = mk_ps();
        let mut records = Vec::new();
        for group in 0..4u64 {
            for n in 0..80u64 {
                let mut hash = [0u8; 32];
                hash[..8].copy_from_slice(&group.to_be_bytes());
                hash[8..16].copy_from_slice(&n.to_be_bytes());
                let rec = if group == 2 && n == 17 {
                    Record::tombstone(&hash)
                } else {
                    Record::put(&hash, &v(n as u8))
                };
                records.push(rec);
            }
        }
        records.sort_unstable_by(|a, b| a.hash().cmp(b.hash()));
        let handle = SstWriter::new(&ps, 1).write_sorted(&records).unwrap();
        let reader = SstReader::open(&ps, &cache, handle).unwrap();

        let prefix = 2u64.to_be_bytes();
        let got = reader.scan_prefix(&prefix).unwrap();
        assert_eq!(got.len(), 80);
        assert!(got.iter().all(|r| r.hash().starts_with(&prefix)));
        assert!(got.iter().any(|r| r.is_delete()));
        assert_eq!(reader.scan_prefix(&9u64.to_be_bytes()).unwrap(), Vec::new());
    }

    #[test]
    fn handle_mismatch_flagged() {
        let (_d, ps, cache) = mk_ps();
        let handle = SstWriter::new(&ps, 1)
            .write_sorted(&sorted_puts(10))
            .unwrap();
        let bogus = SstHandle {
            record_count: 9999, // wrong!
            ..handle
        };
        match SstReader::open(&ps, &cache, bogus).unwrap_err() {
            MetaDbError::Corruption(_) => {}
            e => panic!("{e}"),
        }
    }

    #[test]
    fn memtable_flush_produces_sst() {
        use crate::lsm::memtable::Memtable;
        let (_d, ps, cache) = mk_ps();
        let m = Memtable::new(1_024);
        for i in 0..100u64 {
            m.put(h(i), v(i as u8));
        }
        m.delete(h(50));
        let frozen = m.freeze().unwrap();
        let handle = SstWriter::new(&ps, 1)
            .write_memtable(&frozen)
            .unwrap()
            .unwrap();
        let reader = SstReader::open(&ps, &cache, handle).unwrap();
        for i in 0..100u64 {
            if i == 50 {
                assert_eq!(reader.get(&h(i)).unwrap(), LookupResult::Tombstone);
            } else {
                assert_eq!(reader.get(&h(i)).unwrap(), LookupResult::Hit(v(i as u8)));
            }
        }
    }

    #[test]
    fn memtable_flush_empty_returns_none() {
        use crate::lsm::memtable::Memtable;
        let (_d, ps, _cache) = mk_ps();
        let m = Memtable::new(1_024);
        let frozen = m.freeze().unwrap();
        assert!(
            SstWriter::new(&ps, 1)
                .write_memtable(&frozen)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn multi_page_bloom_survives_round_trip() {
        // Enough records that the bloom spans several 4 KiB pages.
        // With 10 bits/entry, 8 KB / 1.25 B per entry ≈ 6400 entries
        // per bloom page, so aim for ~20 000 to force ≥ 4 bloom pages.
        let (_d, ps, cache) = mk_ps();
        let records = sorted_puts(20_000);
        let handle = SstWriter::new(&ps, 1).write_sorted(&records).unwrap();
        assert!(handle.bloom_page_count >= 4);
        let reader = SstReader::open(&ps, &cache, handle).unwrap();
        for i in (0..20_000u64).step_by(137) {
            assert_eq!(reader.get(&h(i)).unwrap(), LookupResult::Hit(v(i as u8)));
        }
    }
}
