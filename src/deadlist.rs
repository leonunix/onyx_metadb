//! Per-volume dead-list: append-only log of `(pba, birth_lsn, death_lsn)`
//! triples emitted on every L2P overwrite. Feeds Phase 3 lineage GC of
//! the [[no-refcount-hot-path-design]] plan.
//!
//! # Concurrency
//!
//! Apply path pushes into [`DeadListState::push`] under `apply_gate.read()`.
//! Checkpoint flush drains via [`DeadListState::drain`] under
//! `apply_gate.write()`, so the buffer cannot race new apply ops at drain
//! time. The internal `Mutex` only guards the `Vec` itself.
//!
//! # On-disk layout
//!
//! Each segment is a run of `page_store::allocate_run(n)` pages, all
//! tagged `PageType::DeadListSegment`. The first page's payload begins
//! with a 40 B header, then packs 24 B records; continuation pages are
//! pure records. Page CRC32C (from the shared page header) covers the
//! whole 4096 B, so no segment-level CRC is needed.

use parking_lot::Mutex;

use crate::error::{MetaDbError, Result};
use crate::page::{PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use crate::types::{Lsn, NULL_PAGE, PageId, Pba};

/// One death event captured at apply time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeadRecord {
    pub pba: Pba,
    pub birth_lsn: Lsn,
    pub death_lsn: Lsn,
}

/// On-disk size of one [`DeadRecord`].
pub const DEAD_RECORD_BYTES: usize = 24;

/// Bytes consumed by the segment header at the start of the first page.
pub const SEGMENT_HEADER_BYTES: usize = 40;

/// Magic bytes `b"DEDS"` interpreted little-endian.
pub const SEGMENT_MAGIC: u32 = u32::from_le_bytes(*b"DEDS");

const _: () = {
    assert!(SEGMENT_HEADER_BYTES + DEAD_RECORD_BYTES <= PAGE_PAYLOAD_SIZE);
    assert!(DEAD_RECORD_BYTES == 24);
};

/// Per-volume runtime state. The chain's persistent anchor (`head_pid`
/// / `tail_pid`) lives on `VolumeEntry`; this struct only holds the
/// in-memory append buffer that the checkpoint path drains and writes
/// to a new segment.
pub struct DeadListState {
    records: Mutex<Vec<DeadRecord>>,
}

impl DeadListState {
    pub fn new() -> Self {
        Self {
            records: Mutex::new(Vec::new()),
        }
    }

    pub fn push(&self, rec: DeadRecord) {
        self.records.lock().push(rec);
    }

    pub fn drain(&self) -> Vec<DeadRecord> {
        std::mem::take(&mut *self.records.lock())
    }

    /// Return drained records to the head of the buffer in their original
    /// order, in case the checkpoint write fails part-way through.
    pub fn restore_front(&self, mut records: Vec<DeadRecord>) {
        if records.is_empty() {
            return;
        }
        let mut guard = self.records.lock();
        records.append(&mut guard);
        *guard = records;
    }

    pub fn len(&self) -> usize {
        self.records.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.lock().is_empty()
    }
}

impl Default for DeadListState {
    fn default() -> Self {
        Self::new()
    }
}

/// Decoded segment header.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentHeader {
    pub record_count: u32,
    pub min_lsn: Lsn,
    pub max_lsn: Lsn,
    pub prev_seg_pid: PageId,
    pub seg_page_count: u32,
}

impl SegmentHeader {
    fn encode_into(&self, payload: &mut [u8]) {
        payload[0..4].copy_from_slice(&SEGMENT_MAGIC.to_le_bytes());
        payload[4..8].copy_from_slice(&self.record_count.to_le_bytes());
        payload[8..16].copy_from_slice(&self.min_lsn.to_le_bytes());
        payload[16..24].copy_from_slice(&self.max_lsn.to_le_bytes());
        payload[24..32].copy_from_slice(&self.prev_seg_pid.to_le_bytes());
        payload[32..36].copy_from_slice(&self.seg_page_count.to_le_bytes());
        // [36..40] reserved, left zero
    }

    pub fn decode(payload: &[u8]) -> Result<Self> {
        if payload.len() < SEGMENT_HEADER_BYTES {
            return Err(MetaDbError::Corruption(
                "dead-list segment header truncated".to_string(),
            ));
        }
        let magic = u32::from_le_bytes(payload[0..4].try_into().unwrap());
        if magic != SEGMENT_MAGIC {
            return Err(MetaDbError::Corruption(format!(
                "dead-list segment magic mismatch: expected {SEGMENT_MAGIC:#x}, got {magic:#x}"
            )));
        }
        let record_count = u32::from_le_bytes(payload[4..8].try_into().unwrap());
        let min_lsn = u64::from_le_bytes(payload[8..16].try_into().unwrap());
        let max_lsn = u64::from_le_bytes(payload[16..24].try_into().unwrap());
        let prev_seg_pid = u64::from_le_bytes(payload[24..32].try_into().unwrap());
        let seg_page_count = u32::from_le_bytes(payload[32..36].try_into().unwrap());
        if record_count == 0 {
            return Err(MetaDbError::Corruption(
                "dead-list segment record_count is 0".to_string(),
            ));
        }
        if seg_page_count == 0 {
            return Err(MetaDbError::Corruption(
                "dead-list segment seg_page_count is 0".to_string(),
            ));
        }
        if min_lsn > max_lsn {
            return Err(MetaDbError::Corruption(format!(
                "dead-list segment min_lsn {min_lsn} > max_lsn {max_lsn}"
            )));
        }
        Ok(Self {
            record_count,
            min_lsn,
            max_lsn,
            prev_seg_pid,
            seg_page_count,
        })
    }
}

/// Page count needed to fit `record_count` records in one segment.
pub fn segment_pages_for(record_count: usize) -> usize {
    if record_count == 0 {
        return 0;
    }
    let first_capacity = first_page_record_capacity();
    if record_count <= first_capacity {
        return 1;
    }
    let remaining = record_count - first_capacity;
    let cont = continuation_record_capacity();
    1 + remaining.div_ceil(cont)
}

#[inline]
fn first_page_record_capacity() -> usize {
    (PAGE_PAYLOAD_SIZE - SEGMENT_HEADER_BYTES) / DEAD_RECORD_BYTES
}

#[inline]
fn continuation_record_capacity() -> usize {
    PAGE_PAYLOAD_SIZE / DEAD_RECORD_BYTES
}

/// Build the sealed pages of a new segment. Caller has already
/// `allocate_run(n)`-ed `start_pid` for `pages.len()` contiguous pages
/// and is responsible for the subsequent `write_sealed_page_runs`.
pub fn build_segment_pages(
    start_pid: PageId,
    records: &[DeadRecord],
    prev_seg_pid: PageId,
    flush_lsn: Lsn,
) -> Vec<(PageId, Page)> {
    if records.is_empty() {
        return Vec::new();
    }
    let pages_needed = segment_pages_for(records.len());
    let mut pages = Vec::with_capacity(pages_needed);

    let min_lsn = records.iter().map(|r| r.death_lsn).min().unwrap_or(0);
    let max_lsn = records.iter().map(|r| r.death_lsn).max().unwrap_or(0);

    let header = SegmentHeader {
        record_count: records.len() as u32,
        min_lsn,
        max_lsn,
        prev_seg_pid,
        seg_page_count: pages_needed as u32,
    };

    let first_capacity = first_page_record_capacity();
    let cont_capacity = continuation_record_capacity();
    let mut cursor = 0usize;

    for page_ix in 0..pages_needed {
        let mut page = Page::new(PageHeader::new(PageType::DeadListSegment, flush_lsn));
        let payload = page.payload_mut();
        let take = if page_ix == 0 {
            header.encode_into(payload);
            let body = &mut payload[SEGMENT_HEADER_BYTES..];
            let n = first_capacity.min(records.len() - cursor);
            write_records(body, &records[cursor..cursor + n]);
            n
        } else {
            let n = cont_capacity.min(records.len() - cursor);
            write_records(payload, &records[cursor..cursor + n]);
            n
        };
        cursor += take;
        page.seal();
        pages.push((start_pid + page_ix as u64, page));
    }
    debug_assert_eq!(cursor, records.len());
    pages
}

fn write_records(buf: &mut [u8], records: &[DeadRecord]) {
    let mut off = 0;
    for r in records {
        buf[off..off + 8].copy_from_slice(&r.pba.to_le_bytes());
        buf[off + 8..off + 16].copy_from_slice(&r.birth_lsn.to_le_bytes());
        buf[off + 16..off + 24].copy_from_slice(&r.death_lsn.to_le_bytes());
        off += DEAD_RECORD_BYTES;
    }
}

/// Decode all records from a segment given its head page payload and
/// the continuation pages' payloads in chain order. Verifies that the
/// supplied continuation count matches the header.
pub fn read_segment_records(
    head_payload: &[u8],
    cont_payloads: &[&[u8]],
) -> Result<Vec<DeadRecord>> {
    let header = SegmentHeader::decode(head_payload)?;
    if header.seg_page_count as usize != cont_payloads.len() + 1 {
        return Err(MetaDbError::Corruption(format!(
            "dead-list segment: header says {} pages, got {} continuations",
            header.seg_page_count,
            cont_payloads.len()
        )));
    }
    let total = header.record_count as usize;
    let mut out = Vec::with_capacity(total);

    let first_capacity = first_page_record_capacity();
    let cont_capacity = continuation_record_capacity();

    let body = &head_payload[SEGMENT_HEADER_BYTES..];
    let want_first = first_capacity.min(total);
    decode_records(body, want_first, &mut out);
    let mut taken = want_first;

    for payload in cont_payloads {
        let want = cont_capacity.min(total - taken);
        decode_records(payload, want, &mut out);
        taken += want;
    }
    debug_assert_eq!(taken, total);
    Ok(out)
}

fn decode_records(buf: &[u8], count: usize, out: &mut Vec<DeadRecord>) {
    let mut off = 0;
    for _ in 0..count {
        let pba = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
        let birth = u64::from_le_bytes(buf[off + 8..off + 16].try_into().unwrap());
        let death = u64::from_le_bytes(buf[off + 16..off + 24].try_into().unwrap());
        out.push(DeadRecord {
            pba,
            birth_lsn: birth,
            death_lsn: death,
        });
        off += DEAD_RECORD_BYTES;
    }
}

/// Walk a volume's segment chain from `tail_pid` backward through
/// `prev_seg_pid` links and return every page id owned by the chain
/// (each segment contributes its full `seg_page_count` consecutive
/// pids starting at the segment's first page). Returns empty if the
/// chain is empty. Used by `drop_volume` to feed `WalOp::DropVolume.pages`
/// and by `metadb-verify` for chain auditing.
pub fn walk_chain_pages<F>(tail_pid: PageId, read_page: F) -> Result<Vec<PageId>>
where
    F: Fn(PageId) -> Result<Page>,
{
    let mut pids = Vec::new();
    if tail_pid == NULL_PAGE {
        return Ok(pids);
    }
    let mut cur = tail_pid;
    loop {
        let page = read_page(cur)?;
        let h = page.header()?;
        if h.page_type != PageType::DeadListSegment {
            return Err(MetaDbError::Corruption(format!(
                "dead-list chain page {cur} has wrong page_type {:?}",
                h.page_type
            )));
        }
        let header = SegmentHeader::decode(page.payload())?;
        for i in 0..header.seg_page_count as u64 {
            pids.push(cur + i);
        }
        if header.prev_seg_pid == NULL_PAGE {
            break;
        }
        cur = header.prev_seg_pid;
    }
    Ok(pids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::page::PageType;
    use crate::types::FIRST_DATA_PAGE;

    fn rec(pba: Pba, birth: Lsn, death: Lsn) -> DeadRecord {
        DeadRecord {
            pba,
            birth_lsn: birth,
            death_lsn: death,
        }
    }

    #[test]
    fn capacity_math() {
        let first = first_page_record_capacity();
        let cont = continuation_record_capacity();
        assert_eq!(first, (PAGE_PAYLOAD_SIZE - SEGMENT_HEADER_BYTES) / DEAD_RECORD_BYTES);
        assert_eq!(cont, PAGE_PAYLOAD_SIZE / DEAD_RECORD_BYTES);
        assert_eq!(segment_pages_for(0), 0);
        assert_eq!(segment_pages_for(1), 1);
        assert_eq!(segment_pages_for(first), 1);
        assert_eq!(segment_pages_for(first + 1), 2);
        assert_eq!(segment_pages_for(first + cont), 2);
        assert_eq!(segment_pages_for(first + cont + 1), 3);
    }

    #[test]
    fn buffer_push_drain_restore() {
        let s = DeadListState::new();
        s.push(rec(1, 10, 20));
        s.push(rec(2, 11, 21));
        assert_eq!(s.len(), 2);
        let drained = s.drain();
        assert!(s.is_empty());
        assert_eq!(drained, vec![rec(1, 10, 20), rec(2, 11, 21)]);
        s.push(rec(3, 12, 22));
        s.restore_front(drained);
        let final_view = s.drain();
        assert_eq!(
            final_view,
            vec![rec(1, 10, 20), rec(2, 11, 21), rec(3, 12, 22)]
        );
    }

    #[test]
    fn single_page_segment_round_trip() {
        let records = vec![rec(10, 1, 100), rec(11, 2, 101), rec(12, 3, 102)];
        let pages = build_segment_pages(FIRST_DATA_PAGE, &records, NULL_PAGE, 999);
        assert_eq!(pages.len(), 1);
        let (pid, page) = &pages[0];
        assert_eq!(*pid, FIRST_DATA_PAGE);
        assert_eq!(page.header().unwrap().page_type, PageType::DeadListSegment);
        page.verify(*pid).unwrap();

        let header = SegmentHeader::decode(page.payload()).unwrap();
        assert_eq!(header.record_count, 3);
        assert_eq!(header.min_lsn, 100);
        assert_eq!(header.max_lsn, 102);
        assert_eq!(header.prev_seg_pid, NULL_PAGE);
        assert_eq!(header.seg_page_count, 1);

        let decoded = read_segment_records(page.payload(), &[]).unwrap();
        assert_eq!(decoded, records);
    }

    #[test]
    fn multi_page_segment_round_trip() {
        let first = first_page_record_capacity();
        let total = first + 200;
        let records: Vec<DeadRecord> = (0..total as u64)
            .map(|i| rec(i, i * 2, i * 2 + 1))
            .collect();
        let pages = build_segment_pages(FIRST_DATA_PAGE, &records, 42, 1000);
        let header = SegmentHeader::decode(pages[0].1.payload()).unwrap();
        assert_eq!(header.seg_page_count as usize, pages.len());
        assert_eq!(header.prev_seg_pid, 42);
        assert!(pages.len() >= 2);
        for (i, (pid, page)) in pages.iter().enumerate() {
            assert_eq!(*pid, FIRST_DATA_PAGE + i as u64);
            page.verify(*pid).unwrap();
        }
        let cont_payloads: Vec<&[u8]> = pages[1..].iter().map(|(_, p)| &p.payload()[..]).collect();
        let decoded = read_segment_records(pages[0].1.payload(), &cont_payloads).unwrap();
        assert_eq!(decoded, records);
    }

    #[test]
    fn segment_magic_mismatch_is_corruption() {
        let mut payload = [0u8; PAGE_PAYLOAD_SIZE];
        payload[0..4].copy_from_slice(b"WRNG");
        assert!(matches!(
            SegmentHeader::decode(&payload),
            Err(MetaDbError::Corruption(_))
        ));
    }
}
