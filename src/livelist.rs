//! Per-clone page-livelist: append-only log of
//! `(pid, birth_lsn, event_lsn, kind: ALLOC|FREE)` records emitted as a
//! clone allocates / frees its clone-private L2P metadata pages
//! (`birth_lsn > branched_at_lsn`). At `drop_volume` the live-ALLOC set
//! (ALLOC not cancelled by a matching `(pid, birth)` FREE) is the clone's
//! privately-allocated, still-live subtree.
//!
//! Background condense cancels ALLOC/FREE pairs to bound chain growth.
//! `live_allocs` also cancels pairs at read time, and dropped clones' segments
//! are reclaimed idempotently with the rest of the page store.
//!
//! # Concurrency
//!
//! Mirrors [`crate::deadlist`]. Apply pushes into [`LiveListState::push`]
//! under `apply_gate.read()`; the checkpoint flush drains via
//! [`LiveListState::drain_up_to_lsn`] from the gate-free sample phase, so
//! the `event_lsn <= wal_checkpoint` filter is what bounds drained records
//! and keeps the segment chain's "max_lsn strictly older going backward"
//! invariant true.
//!
//! # On-disk layout
//!
//! Identical segment shape to [`crate::deadlist`] (40 B header on the first
//! page, packed records after, page CRC covers the whole page) but a
//! distinct magic ("LIVS"), a distinct [`PageType::LiveListSegment`], and a
//! 32 B record that carries the ALLOC/FREE kind byte.

use parking_lot::Mutex;

use crate::error::{MetaDbError, Result};
use crate::page::{PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use crate::types::{Lsn, NULL_PAGE, PageId};

/// ALLOC = a clone-private page was created; FREE = it left the live tree.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LiveKind {
    Alloc,
    Free,
}

impl LiveKind {
    fn to_byte(self) -> u8 {
        match self {
            LiveKind::Alloc => 0,
            LiveKind::Free => 1,
        }
    }

    fn from_byte(b: u8) -> Result<Self> {
        match b {
            0 => Ok(LiveKind::Alloc),
            1 => Ok(LiveKind::Free),
            other => Err(MetaDbError::Corruption(format!(
                "live-list record has unknown kind byte {other}"
            ))),
        }
    }
}

/// One ALLOC/FREE event captured at apply time. `pid` + `birth_lsn` identify
/// the exact page version; `event_lsn` orders the record in the chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LiveRecord {
    pub pid: PageId,
    pub birth_lsn: Lsn,
    pub event_lsn: Lsn,
    pub kind: LiveKind,
}

/// On-disk size of one [`LiveRecord`]: pid(8) + birth(8) + event(8) +
/// kind(1) + pad(7).
pub const LIVE_RECORD_BYTES: usize = 32;

/// Bytes consumed by the segment header at the start of the first page.
/// Same shape/size as [`crate::deadlist::SEGMENT_HEADER_BYTES`].
pub const SEGMENT_HEADER_BYTES: usize = 40;

/// Magic bytes `b"LIVS"` interpreted little-endian.
pub const SEGMENT_MAGIC: u32 = u32::from_le_bytes(*b"LIVS");

const _: () = {
    assert!(SEGMENT_HEADER_BYTES + LIVE_RECORD_BYTES <= PAGE_PAYLOAD_SIZE);
    assert!(LIVE_RECORD_BYTES == 32);
};

/// Per-clone runtime state — the in-memory append buffer the checkpoint
/// path drains and seals into a new segment. The persistent anchor
/// (`page_live_list_{head,tail}_pid`) lives on `VolumeEntry`.
pub struct LiveListState {
    records: Mutex<Vec<LiveRecord>>,
}

impl LiveListState {
    pub fn new() -> Self {
        Self {
            records: Mutex::new(Vec::new()),
        }
    }

    pub fn push(&self, rec: LiveRecord) {
        self.records.lock().push(rec);
    }

    pub fn drain(&self) -> Vec<LiveRecord> {
        std::mem::take(&mut *self.records.lock())
    }

    /// Drain only records with `event_lsn <= max_lsn`; keep the rest for a
    /// later flush. Same rationale as
    /// [`crate::deadlist::DeadListState::drain_up_to_lsn`] — a concurrent
    /// apply may keep pushing `event_lsn > wal_checkpoint` records during
    /// the gate-free drain, and folding those in would violate the chain's
    /// "max_lsn strictly older going backward" invariant.
    pub fn drain_up_to_lsn(&self, max_lsn: Lsn) -> Vec<LiveRecord> {
        let mut guard = self.records.lock();
        let buffered = std::mem::take(&mut *guard);
        let mut taken: Vec<LiveRecord> = Vec::with_capacity(buffered.len());
        let mut keep: Vec<LiveRecord> = Vec::new();
        for rec in buffered {
            if rec.event_lsn <= max_lsn {
                taken.push(rec);
            } else {
                keep.push(rec);
            }
        }
        *guard = keep;
        taken
    }

    /// Return drained records to the head of the buffer in their original
    /// order, in case the checkpoint write fails part-way through.
    pub fn restore_front(&self, mut records: Vec<LiveRecord>) {
        if records.is_empty() {
            return;
        }
        let mut guard = self.records.lock();
        records.append(&mut guard);
        *guard = records;
    }

    /// Non-destructive snapshot of the buffered records.
    pub fn peek(&self) -> Vec<LiveRecord> {
        self.records.lock().clone()
    }

    pub fn len(&self) -> usize {
        self.records.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.lock().is_empty()
    }
}

impl Default for LiveListState {
    fn default() -> Self {
        Self::new()
    }
}

/// Decoded segment header. Same layout as
/// [`crate::deadlist::SegmentHeader`] (only the magic differs).
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
                "live-list segment header truncated".to_string(),
            ));
        }
        let magic = u32::from_le_bytes(payload[0..4].try_into().unwrap());
        if magic != SEGMENT_MAGIC {
            return Err(MetaDbError::Corruption(format!(
                "live-list segment magic mismatch: expected {SEGMENT_MAGIC:#x}, got {magic:#x}"
            )));
        }
        let record_count = u32::from_le_bytes(payload[4..8].try_into().unwrap());
        let min_lsn = u64::from_le_bytes(payload[8..16].try_into().unwrap());
        let max_lsn = u64::from_le_bytes(payload[16..24].try_into().unwrap());
        let prev_seg_pid = u64::from_le_bytes(payload[24..32].try_into().unwrap());
        let seg_page_count = u32::from_le_bytes(payload[32..36].try_into().unwrap());
        if record_count == 0 {
            return Err(MetaDbError::Corruption(
                "live-list segment record_count is 0".to_string(),
            ));
        }
        if seg_page_count == 0 {
            return Err(MetaDbError::Corruption(
                "live-list segment seg_page_count is 0".to_string(),
            ));
        }
        if min_lsn > max_lsn {
            return Err(MetaDbError::Corruption(format!(
                "live-list segment min_lsn {min_lsn} > max_lsn {max_lsn}"
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
    (PAGE_PAYLOAD_SIZE - SEGMENT_HEADER_BYTES) / LIVE_RECORD_BYTES
}

#[inline]
fn continuation_record_capacity() -> usize {
    PAGE_PAYLOAD_SIZE / LIVE_RECORD_BYTES
}

/// Build the sealed pages of a new segment. Caller has already
/// `allocate_run(n)`-ed `start_pid` for `pages.len()` contiguous pages.
pub fn build_segment_pages(
    start_pid: PageId,
    records: &[LiveRecord],
    prev_seg_pid: PageId,
    flush_lsn: Lsn,
) -> Vec<(PageId, Page)> {
    if records.is_empty() {
        return Vec::new();
    }
    let pages_needed = segment_pages_for(records.len());
    let mut pages = Vec::with_capacity(pages_needed);

    let min_lsn = records.iter().map(|r| r.event_lsn).min().unwrap_or(0);
    let max_lsn = records.iter().map(|r| r.event_lsn).max().unwrap_or(0);

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
        let mut page = Page::new(PageHeader::new(PageType::LiveListSegment, flush_lsn));
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

fn write_records(buf: &mut [u8], records: &[LiveRecord]) {
    let mut off = 0;
    for r in records {
        buf[off..off + 8].copy_from_slice(&r.pid.to_le_bytes());
        buf[off + 8..off + 16].copy_from_slice(&r.birth_lsn.to_le_bytes());
        buf[off + 16..off + 24].copy_from_slice(&r.event_lsn.to_le_bytes());
        buf[off + 24] = r.kind.to_byte();
        // [off+25 .. off+32] reserved, left zero
        off += LIVE_RECORD_BYTES;
    }
}

/// Decode all records from a segment given its head page payload and the
/// continuation pages' payloads in chain order.
pub fn read_segment_records(
    head_payload: &[u8],
    cont_payloads: &[&[u8]],
) -> Result<Vec<LiveRecord>> {
    let header = SegmentHeader::decode(head_payload)?;
    if header.seg_page_count as usize != cont_payloads.len() + 1 {
        return Err(MetaDbError::Corruption(format!(
            "live-list segment: header says {} pages, got {} continuations",
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
    decode_records(body, want_first, &mut out)?;
    let mut taken = want_first;

    for payload in cont_payloads {
        let want = cont_capacity.min(total - taken);
        decode_records(payload, want, &mut out)?;
        taken += want;
    }
    debug_assert_eq!(taken, total);
    Ok(out)
}

fn decode_records(buf: &[u8], count: usize, out: &mut Vec<LiveRecord>) -> Result<()> {
    let mut off = 0;
    for _ in 0..count {
        let pid = u64::from_le_bytes(buf[off..off + 8].try_into().unwrap());
        let birth = u64::from_le_bytes(buf[off + 8..off + 16].try_into().unwrap());
        let event = u64::from_le_bytes(buf[off + 16..off + 24].try_into().unwrap());
        let kind = LiveKind::from_byte(buf[off + 24])?;
        out.push(LiveRecord {
            pid,
            birth_lsn: birth,
            event_lsn: event,
            kind,
        });
        off += LIVE_RECORD_BYTES;
    }
    Ok(())
}

/// Read every [`LiveRecord`] from a chain, walking `tail_pid` backward
/// through `prev_seg_pid` to `NULL_PAGE`. Order is tail-segment-first.
pub fn read_chain_records<F>(tail_pid: PageId, read_page: F) -> Result<Vec<LiveRecord>>
where
    F: Fn(PageId) -> Result<Page>,
{
    let mut out = Vec::new();
    if tail_pid == NULL_PAGE {
        return Ok(out);
    }
    let mut cur = tail_pid;
    loop {
        let head_page = read_page(cur)?;
        let h = head_page.header()?;
        if h.page_type != PageType::LiveListSegment {
            return Err(MetaDbError::Corruption(format!(
                "page-livelist chain page {cur} has wrong page_type {:?}",
                h.page_type
            )));
        }
        let header = SegmentHeader::decode(head_page.payload())?;
        let cont_pages: Vec<Page> = (1..header.seg_page_count as u64)
            .map(|i| read_page(cur + i))
            .collect::<Result<_>>()?;
        let cont_payloads: Vec<&[u8]> = cont_pages.iter().map(|p| &p.payload()[..]).collect();
        let mut recs = read_segment_records(head_page.payload(), &cont_payloads)?;
        out.append(&mut recs);
        if header.prev_seg_pid == NULL_PAGE {
            break;
        }
        cur = header.prev_seg_pid;
    }
    Ok(out)
}

/// Walk a clone's livelist segment chain from `tail_pid` backward and
/// return every page id owned by the chain. Used by `verify`'s
/// `collect_live_pages` to mark the chain's segment pages live (so
/// orphan-reclaim does not free them), and (follow-up) by condense
/// to reclaim the old chain.
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
        if h.page_type != PageType::LiveListSegment {
            return Err(MetaDbError::Corruption(format!(
                "page-livelist chain page {cur} has wrong page_type {:?}",
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

/// Walk the chain backward from `tail_pid` reading only each segment's
/// head page (for `prev_seg_pid` + the type check) and return whether it
/// holds at least `threshold` segments. Stops the moment the count reaches
/// `threshold`, so it reads at most `threshold` head pages — the cheap gate
/// the background condense worker uses to skip chains not worth rewriting
/// without decoding every record. `threshold == 0` is trivially true.
pub fn chain_has_at_least_segments<F>(
    tail_pid: PageId,
    threshold: usize,
    read_page: F,
) -> Result<bool>
where
    F: Fn(PageId) -> Result<Page>,
{
    if threshold == 0 {
        return Ok(true);
    }
    let mut cur = tail_pid;
    let mut count = 0usize;
    while cur != NULL_PAGE {
        let page = read_page(cur)?;
        let h = page.header()?;
        if h.page_type != PageType::LiveListSegment {
            return Err(MetaDbError::Corruption(format!(
                "page-livelist chain page {cur} has wrong page_type {:?}",
                h.page_type
            )));
        }
        let header = SegmentHeader::decode(page.payload())?;
        count += 1;
        if count >= threshold {
            return Ok(true);
        }
        cur = header.prev_seg_pid;
    }
    Ok(false)
}

/// Cancel matched `(pid, birth)` ALLOC/FREE pairs from a chain's records,
/// returning the surviving live-ALLOC records. A FREE cancels the most
/// recent un-cancelled ALLOC of the same `(pid, birth)`. Records arrive
/// tail-first from [`read_chain_records`], so they are sorted by
/// `event_lsn` first to apply the events in birth order. Used by both
/// condense and the offline verify equality check.
pub fn live_allocs(mut records: Vec<LiveRecord>) -> Result<Vec<LiveRecord>> {
    use std::collections::HashMap;
    // Sort ALLOC strictly before FREE at an otherwise-equal key so a pair
    // born and freed in the SAME op (`event_lsn == birth_lsn`, e.g. a delete
    // that COWs then prunes a leaf in one op) always cancels — independent of
    // the order they were captured / drained / segmented in. `kind == Free`
    // sorts as 1 (after `Alloc`'s 0).
    records.sort_by_key(|r| (r.event_lsn, r.pid, r.birth_lsn, r.kind == LiveKind::Free));
    // (pid, birth) -> stack of outstanding ALLOC records.
    let mut outstanding: HashMap<(PageId, Lsn), Vec<LiveRecord>> = HashMap::new();
    for r in records {
        let key = (r.pid, r.birth_lsn);
        match r.kind {
            LiveKind::Alloc => outstanding.entry(key).or_default().push(r),
            LiveKind::Free => {
                let stack = outstanding.get_mut(&key);
                match stack.and_then(|s| s.pop()) {
                    Some(_) => {}
                    None => {
                        return Err(MetaDbError::Corruption(format!(
                            "page-livelist FREE for pid={} birth={} has no matching ALLOC",
                            r.pid, r.birth_lsn
                        )));
                    }
                }
            }
        }
    }
    let mut out: Vec<LiveRecord> = outstanding.into_values().flatten().collect();
    out.sort_by_key(|r| (r.event_lsn, r.pid, r.birth_lsn));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::FIRST_DATA_PAGE;

    fn alloc(pid: PageId, birth: Lsn, event: Lsn) -> LiveRecord {
        LiveRecord {
            pid,
            birth_lsn: birth,
            event_lsn: event,
            kind: LiveKind::Alloc,
        }
    }

    fn free(pid: PageId, birth: Lsn, event: Lsn) -> LiveRecord {
        LiveRecord {
            pid,
            birth_lsn: birth,
            event_lsn: event,
            kind: LiveKind::Free,
        }
    }

    #[test]
    fn capacity_math() {
        let first = first_page_record_capacity();
        let cont = continuation_record_capacity();
        assert_eq!(
            first,
            (PAGE_PAYLOAD_SIZE - SEGMENT_HEADER_BYTES) / LIVE_RECORD_BYTES
        );
        assert_eq!(cont, PAGE_PAYLOAD_SIZE / LIVE_RECORD_BYTES);
        assert_eq!(segment_pages_for(0), 0);
        assert_eq!(segment_pages_for(1), 1);
        assert_eq!(segment_pages_for(first), 1);
        assert_eq!(segment_pages_for(first + 1), 2);
        assert_eq!(segment_pages_for(first + cont), 2);
        assert_eq!(segment_pages_for(first + cont + 1), 3);
    }

    #[test]
    fn buffer_push_drain_restore() {
        let s = LiveListState::new();
        s.push(alloc(1, 10, 20));
        s.push(free(1, 10, 30));
        assert_eq!(s.len(), 2);
        let drained = s.drain();
        assert!(s.is_empty());
        assert_eq!(drained, vec![alloc(1, 10, 20), free(1, 10, 30)]);
        s.push(alloc(2, 12, 40));
        s.restore_front(drained);
        let final_view = s.drain();
        assert_eq!(
            final_view,
            vec![alloc(1, 10, 20), free(1, 10, 30), alloc(2, 12, 40)]
        );
    }

    #[test]
    fn drain_up_to_lsn_keeps_newer() {
        let s = LiveListState::new();
        s.push(alloc(1, 5, 10));
        s.push(alloc(2, 6, 25));
        let taken = s.drain_up_to_lsn(20);
        assert_eq!(taken, vec![alloc(1, 5, 10)]);
        assert_eq!(s.drain(), vec![alloc(2, 6, 25)]);
    }

    #[test]
    fn single_page_segment_round_trip() {
        let records = vec![alloc(10, 1, 100), free(10, 1, 101), alloc(12, 3, 102)];
        let pages = build_segment_pages(FIRST_DATA_PAGE, &records, NULL_PAGE, 999);
        assert_eq!(pages.len(), 1);
        let (pid, page) = &pages[0];
        assert_eq!(*pid, FIRST_DATA_PAGE);
        assert_eq!(page.header().unwrap().page_type, PageType::LiveListSegment);
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
        let records: Vec<LiveRecord> = (0..total as u64)
            .map(|i| alloc(i, i * 2, i * 2 + 1))
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
    fn live_allocs_cancels_pairs() {
        // pid 10 born@1: ALLOC then FREE -> cancelled.
        // pid 11 born@2: ALLOC, never freed -> survives.
        // pid 12 born@3: ALLOC, FREE, ALLOC (reborn at same birth via two
        // separate alloc events) -> one survives.
        let records = vec![
            alloc(10, 1, 100),
            free(10, 1, 105),
            alloc(11, 2, 101),
            alloc(12, 3, 102),
            free(12, 3, 106),
            alloc(12, 3, 110),
        ];
        let live = live_allocs(records).unwrap();
        let surviving: Vec<(PageId, Lsn)> = live.iter().map(|r| (r.pid, r.birth_lsn)).collect();
        assert!(surviving.contains(&(11, 2)));
        assert!(surviving.contains(&(12, 3)));
        assert!(!surviving.contains(&(10, 1)));
        assert_eq!(live.len(), 2);
    }

    #[test]
    fn live_allocs_idempotent_on_own_output() {
        // Condense writes `live_allocs(chain)` as the new segment; verify then
        // recomputes `live_allocs` over that segment and must get the same set.
        // An all-ALLOC input (every FREE already cancelled) must pass through
        // unchanged — otherwise condense would drift the live-ALLOC set.
        let records = vec![
            alloc(10, 1, 100),
            free(10, 1, 105),
            alloc(11, 2, 101),
            alloc(12, 3, 102),
        ];
        let once = live_allocs(records).unwrap();
        let key = |recs: &[LiveRecord]| {
            recs.iter()
                .map(|r| (r.pid, r.birth_lsn))
                .collect::<std::collections::BTreeSet<_>>()
        };
        let twice = live_allocs(once.clone()).unwrap();
        assert_eq!(key(&once), key(&twice));
        assert_eq!(once.len(), 2); // pid 11 + pid 12 survive; pid 10 cancelled.
    }

    #[test]
    fn live_allocs_free_without_alloc_is_corruption() {
        let records = vec![free(10, 1, 100)];
        assert!(matches!(
            live_allocs(records),
            Err(MetaDbError::Corruption(_))
        ));
    }

    #[test]
    fn segment_magic_mismatch_is_corruption() {
        let mut payload = [0u8; PAGE_PAYLOAD_SIZE];
        payload[0..4].copy_from_slice(b"DEDS"); // deadlist magic, not LIVS
        assert!(matches!(
            SegmentHeader::decode(&payload),
            Err(MetaDbError::Corruption(_))
        ));
    }
}
