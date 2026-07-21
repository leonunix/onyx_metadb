//! COW segment-directory chain for durable refcount delta-run segments (v27).
//!
//! # Responsibility
//!
//! Each rc shard that carries un-condensed delta-run segments anchors a chain of
//! [`PageType::RefcountSegmentDir`] pages in the manifest
//! (`refcount_delta_run_heads[shard]`). The chain lists one
//! [`SegmentDescriptor`] per segment: the BFG that froze it, its covered LSN
//! range, its record count, and the data-page ids
//! ([`PageType::RefcountDeltaRun`]) that hold its encoded records. Open replays
//! these segments oldest→newest; condense folds them into the base array and
//! empties the chain.
//!
//! # Crash model — stable-head, in-place, recover-ahead (like the array/cuckoo)
//!
//! The directory head pid is **stable**: it is allocated once (reserved empty in
//! the manifest before any segment is appended — see `Db::open`) and rewritten
//! **in place** on every append/condense, exactly like the refcount ARRAY meta
//! chain and the dedup cuckoo meta chain ([`crate::paged_meta`]). A stable head
//! recorded in the manifest ONCE means the segment data pages + directory content
//! — both made durable at the flush's `page_store.sync()` BEFORE the manifest
//! commit — are reachable on reopen even if that commit is lost. `Db::open` then
//! folds any recovered segments into the base array (`condense_on_open`), which
//! restores rc for standalone metadb AND stamps the base pages' `generation` so
//! the existing `page_lsn >= lsn` replay-skip drops any onyx LV2 re-drive of the
//! same commits (no double count). This is why an earlier fresh-pid-COW design
//! was WRONG for the WAL-free model: a COW head recorded only in the not-yet-
//! committed manifest orphans the just-written segment on crash → the rc delta is
//! lost while its dedup entry survives (a premature-free). Segment DATA pages are
//! write-once and carry `generation = covered_lsn_max`; only the directory chain
//! framing is rewritten in place, reusing the head + continuation pids and
//! freeing trailing pids after the new content is durable.
//!
//! # Concurrency
//!
//! Pure codec + page IO; no internal locking. The caller (`RcShard` /
//! `flush_with_gate`) serialises directory rewrites through the checkpoint /
//! condense path under `fold_lock` + the flush gate.

use std::sync::Arc;

use crate::error::{MetaDbError, Result};
use crate::page::{PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use crate::page_store::PageStore;
use crate::types::{Bfg, Lsn, NULL_PAGE, PageId};

/// Fixed per-page chain header: `chunk_len(4) | reserved(4) | next_pid(8)`.
/// `next_pid == 0` (== `MANIFEST_PAGE_A`, never a data page) marks end-of-chain,
/// the same sentinel convention as [`crate::manifest::catalog`].
const CHAIN_HEADER_BYTES: usize = 16;

/// Directory-stream bytes that fit on one chain page.
const DIR_PAGE_CAPACITY: usize = PAGE_PAYLOAD_SIZE - CHAIN_HEADER_BYTES;

/// Header `key_count` marker stamped into every directory page so a chain page
/// read with the wrong head pid is rejected.
const DIR_KEY_COUNT_MARKER: u16 = 0xD1E0;

/// Runaway/cycle guard: a corrupt `next_pid` forming a loop must surface as an
/// error rather than spin forever. Far above any real directory length.
const MAX_CHAIN_PAGES: usize = 1 << 20;

/// Sanity caps on decoded counts so a corrupt byte stream can't drive an
/// unbounded allocation. Both are far above any real workload: a shard's BFG
/// admission cohort bounds a segment to a few hundred data pages, and K bounds
/// the segment count per condense interval.
const MAX_SEGMENTS: usize = 1 << 20;
const MAX_SEGMENT_DATA_PAGES: u64 = 1 << 32;

/// One un-condensed delta-run segment recorded in a shard's directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SegmentDescriptor {
    /// BFG that froze this segment's slot.
    pub bfg: Bfg,
    /// Lowest `last_lsn` over the segment's records (== the codec's
    /// `covered_lsn_min`).
    pub covered_lsn_min: Lsn,
    /// Highest `last_lsn` over the segment's records (== the pages' generation).
    pub covered_lsn_max: Lsn,
    /// Number of `(pba, delta, last_lsn)` records the segment encodes.
    pub records: u64,
    /// Data pages ([`PageType::RefcountDeltaRun`]) in page order (oldest record
    /// first), as produced by [`crate::refcount::delta_run`]'s `encode_pages`.
    pub data_pids: Vec<PageId>,
}

// ─────────────────────────────── byte codec ────────────────────────────────

/// Serialise the descriptors into the directory byte stream. Layout:
/// `segment_count(u32) then per-segment [bfg,min,max,records (u64×4),
/// pid_run_count(u32), runs of (start u64, len u32)]`. Data-page ids are
/// run-length coalesced (consecutive ascending pids → one run) so a segment
/// whose pages were frontier-allocated costs a handful of bytes.
pub(crate) fn encode_directory(segments: &[SegmentDescriptor]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(segments.len() as u32).to_le_bytes());
    for seg in segments {
        out.extend_from_slice(&seg.bfg.to_le_bytes());
        out.extend_from_slice(&seg.covered_lsn_min.to_le_bytes());
        out.extend_from_slice(&seg.covered_lsn_max.to_le_bytes());
        out.extend_from_slice(&seg.records.to_le_bytes());
        encode_pid_runs(&seg.data_pids, &mut out);
    }
    out
}

fn encode_pid_runs(pids: &[PageId], out: &mut Vec<u8>) {
    // Coalesce consecutive ascending pids into (start, len) runs, preserving
    // page order. Non-monotone gaps just start a new length-1 run.
    let mut runs: Vec<(PageId, u32)> = Vec::new();
    for &pid in pids {
        if let Some(last) = runs.last_mut()
            && last.1 < u32::MAX
            && last.0 + last.1 as u64 == pid
        {
            last.1 += 1;
            continue;
        }
        runs.push((pid, 1));
    }
    out.extend_from_slice(&(runs.len() as u32).to_le_bytes());
    for (start, len) in runs {
        out.extend_from_slice(&start.to_le_bytes());
        out.extend_from_slice(&len.to_le_bytes());
    }
}

/// Inverse of [`encode_directory`]. Rejects a truncated / over-long / trailing-
/// byte stream so a corrupt chain surfaces as `Err`.
pub(crate) fn decode_directory(bytes: &[u8]) -> Result<Vec<SegmentDescriptor>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let mut cursor = 0usize;
    let segment_count = read_u32(bytes, &mut cursor)? as usize;
    if segment_count > MAX_SEGMENTS {
        return Err(MetaDbError::Corruption(format!(
            "refcount segment directory segment_count {segment_count} exceeds {MAX_SEGMENTS}"
        )));
    }
    let mut segments = Vec::with_capacity(segment_count.min(1024));
    for _ in 0..segment_count {
        let bfg = read_u64(bytes, &mut cursor)?;
        let covered_lsn_min = read_u64(bytes, &mut cursor)?;
        let covered_lsn_max = read_u64(bytes, &mut cursor)?;
        let records = read_u64(bytes, &mut cursor)?;
        if covered_lsn_min > covered_lsn_max {
            return Err(MetaDbError::Corruption(format!(
                "refcount segment directory covered range {covered_lsn_min}..={covered_lsn_max} inverted"
            )));
        }
        let data_pids = decode_pid_runs(bytes, &mut cursor)?;
        segments.push(SegmentDescriptor {
            bfg,
            covered_lsn_min,
            covered_lsn_max,
            records,
            data_pids,
        });
    }
    if cursor != bytes.len() {
        return Err(MetaDbError::Corruption(format!(
            "refcount segment directory has {} trailing bytes",
            bytes.len() - cursor
        )));
    }
    Ok(segments)
}

fn decode_pid_runs(bytes: &[u8], cursor: &mut usize) -> Result<Vec<PageId>> {
    let run_count = read_u32(bytes, cursor)? as usize;
    let mut pids = Vec::new();
    let mut total: u64 = 0;
    for _ in 0..run_count {
        let start = read_u64(bytes, cursor)?;
        let len = read_u32(bytes, cursor)? as u64;
        if len == 0 {
            return Err(MetaDbError::Corruption(
                "refcount segment directory has zero-length pid run".into(),
            ));
        }
        total = total.saturating_add(len);
        if total > MAX_SEGMENT_DATA_PAGES {
            return Err(MetaDbError::Corruption(format!(
                "refcount segment directory pid count {total} exceeds {MAX_SEGMENT_DATA_PAGES}"
            )));
        }
        let end = start.checked_add(len).ok_or_else(|| {
            MetaDbError::Corruption("refcount segment directory pid run overflows u64".into())
        })?;
        pids.extend(start..end);
    }
    Ok(pids)
}

fn read_u32(bytes: &[u8], cursor: &mut usize) -> Result<u32> {
    let end = cursor.checked_add(4).filter(|&e| e <= bytes.len()).ok_or_else(|| {
        MetaDbError::Corruption("refcount segment directory truncated u32".into())
    })?;
    let v = u32::from_le_bytes(bytes[*cursor..end].try_into().unwrap());
    *cursor = end;
    Ok(v)
}

fn read_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64> {
    let end = cursor.checked_add(8).filter(|&e| e <= bytes.len()).ok_or_else(|| {
        MetaDbError::Corruption("refcount segment directory truncated u64".into())
    })?;
    let v = u64::from_le_bytes(bytes[*cursor..end].try_into().unwrap());
    *cursor = end;
    Ok(v)
}

// ────────────────────────────── COW chain IO ───────────────────────────────

/// Rewrite the **stable-head, in-place** directory chain for `segments`, reusing
/// `existing_chain[0]` as the head + as many continuation pids as needed,
/// allocating fresh pids only when the directory grew, and returning trailing
/// pids `to_free` when it shrank. Returns `(new_chain, sealed_pages, to_free)`
/// (chain head first). The caller writes `sealed_pages` (folded into the flush's
/// global batch, durable at the flush `sync()` BEFORE the manifest commit) and —
/// after the manifest commit lands — frees `to_free`.
///
/// `existing_chain` must be non-empty: the head is reserved once (empty) in the
/// manifest before any append (see `Db::open`), so every append/condense rewrites
/// an already-durable head in place. An empty `segments` still writes one head
/// page (the reserved/emptied directory), mirroring `paged_meta::build_chain_pages`.
pub(crate) fn build_directory_chain(
    page_store: &PageStore,
    existing_chain: &[PageId],
    segments: &[SegmentDescriptor],
    generation: Lsn,
) -> Result<(Vec<PageId>, Vec<(PageId, Arc<Page>)>, Vec<PageId>)> {
    assert!(
        !existing_chain.is_empty(),
        "build_directory_chain requires the reserved head pid to already exist",
    );
    let bytes = encode_directory(segments);
    let chunk_count = bytes.len().div_ceil(DIR_PAGE_CAPACITY).max(1);

    // Reuse the stable head + existing continuations; allocate fresh only for
    // net-new chunks (mirrors `paged_meta::build_chain_pages`). On an alloc
    // failure free ONLY the freshly-grabbed pids (indices >= existing_chain.len()).
    let mut new_chain = Vec::with_capacity(chunk_count);
    new_chain.push(existing_chain[0]);
    for idx in 1..chunk_count {
        let pid = if idx < existing_chain.len() {
            existing_chain[idx]
        } else {
            match page_store.allocate() {
                Ok(pid) => pid,
                Err(err) => {
                    for &fresh in new_chain.iter().skip(existing_chain.len()) {
                        let _ = page_store.free(fresh, generation);
                    }
                    return Err(err);
                }
            }
        };
        new_chain.push(pid);
    }
    // Trailing pages from the previous chain go on the free list once the new
    // chain is durable + the manifest committed.
    let to_free: Vec<PageId> = existing_chain.iter().skip(new_chain.len()).copied().collect();

    let mut sealed = Vec::with_capacity(chunk_count);
    for (i, &pid) in new_chain.iter().enumerate() {
        let start = i * DIR_PAGE_CAPACITY;
        let end = ((i + 1) * DIR_PAGE_CAPACITY).min(bytes.len());
        let chunk = &bytes[start..end];
        let next_pid = new_chain.get(i + 1).copied().unwrap_or(0);

        let mut page = Page::new(PageHeader::new(PageType::RefcountSegmentDir, generation));
        page.set_key_count(DIR_KEY_COUNT_MARKER);
        {
            let p = page.payload_mut();
            p[0..4].copy_from_slice(&(chunk.len() as u32).to_le_bytes());
            p[4..8].fill(0);
            p[8..16].copy_from_slice(&next_pid.to_le_bytes());
            p[CHAIN_HEADER_BYTES..CHAIN_HEADER_BYTES + chunk.len()].copy_from_slice(chunk);
        }
        page.seal();
        sealed.push((pid, Arc::new(page)));
    }
    Ok((new_chain, sealed, to_free))
}

/// Reserve a fresh, empty directory head: allocate one page id and seal it as an
/// empty directory. Used once per shard (at `Db::open` when persist is enabled and
/// the shard has no durable head yet) so every later append rewrites this
/// already-durable head in place. Returns `(head_pid, sealed_head_page)`.
pub(crate) fn reserve_empty_directory(
    page_store: &PageStore,
    generation: Lsn,
) -> Result<(PageId, (PageId, Arc<Page>))> {
    let head = page_store.allocate()?;
    let (chain, mut sealed, _to_free) = build_directory_chain(page_store, &[head], &[], generation)?;
    debug_assert_eq!(chain.as_slice(), &[head]);
    Ok((head, sealed.pop().expect("reserve writes exactly one head page")))
}

/// Walk the chain rooted at `head_pid`, validating + concatenating every page's
/// directory bytes and invoking `on_page` once per page id (head first). A
/// [`NULL_PAGE`] head yields an empty stream with no pages walked; an
/// unreadable / mistyped / over-long page returns `Err`.
fn walk(
    page_store: &PageStore,
    head_pid: PageId,
    on_page: &mut dyn FnMut(PageId),
) -> Result<Vec<u8>> {
    if head_pid == NULL_PAGE {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let mut next = head_pid;
    let mut pages = 0usize;
    while next != 0 {
        if pages >= MAX_CHAIN_PAGES {
            return Err(MetaDbError::Corruption(format!(
                "refcount segment directory chain at head {head_pid} exceeds {MAX_CHAIN_PAGES} pages (cycle?)"
            )));
        }
        on_page(next);
        let page = page_store.read_page(next)?;
        let header = page.header()?;
        if header.page_type != PageType::RefcountSegmentDir
            || header.key_count != DIR_KEY_COUNT_MARKER
        {
            return Err(MetaDbError::Corruption(format!(
                "refcount segment directory page {next} has wrong header (type={:?}, key_count={:#06x})",
                header.page_type, header.key_count,
            )));
        }
        let p = page.payload();
        let chunk_len = u32::from_le_bytes(p[0..4].try_into().unwrap()) as usize;
        let next_pid = u64::from_le_bytes(p[8..16].try_into().unwrap()) as PageId;
        if chunk_len > DIR_PAGE_CAPACITY {
            return Err(MetaDbError::Corruption(format!(
                "refcount segment directory page {next} chunk_len {chunk_len} exceeds {DIR_PAGE_CAPACITY}"
            )));
        }
        out.extend_from_slice(&p[CHAIN_HEADER_BYTES..CHAIN_HEADER_BYTES + chunk_len]);
        next = next_pid;
        pages += 1;
    }
    Ok(out)
}

/// Read the whole chain rooted at `head_pid` back into its segment descriptors.
/// [`NULL_PAGE`] head → empty.
pub(crate) fn read_directory_chain(
    page_store: &PageStore,
    head_pid: PageId,
) -> Result<Vec<SegmentDescriptor>> {
    let bytes = walk(page_store, head_pid, &mut |_| {})?;
    decode_directory(&bytes)
}

/// Only the chain FRAMING pages of the directory at `head_pid` (head first),
/// NOT the segment data pages. Used to seed the shard's cached chain at open (so
/// later in-place rewrites reuse the continuation pids) — the data pages it lists
/// stay live because the directory re-lists them. Empty for a [`NULL_PAGE`] head.
pub(crate) fn directory_chain_pids(
    page_store: &PageStore,
    head_pid: PageId,
) -> Result<Vec<PageId>> {
    let mut pids = Vec::new();
    walk(page_store, head_pid, &mut |pid| pids.push(pid))?;
    Ok(pids)
}

/// Offline-verify check: decode the directory at `head_pid` and assert every
/// segment's covered LSN range is well-formed and the sequence of
/// `covered_lsn_max` is non-decreasing oldest→newest (segments are appended in
/// increasing BFG/checkpoint order). A [`NULL_PAGE`] head passes trivially.
pub(crate) fn verify_directory(page_store: &PageStore, head_pid: PageId) -> Result<()> {
    let segments = read_directory_chain(page_store, head_pid)?;
    let mut prev_max: Option<Lsn> = None;
    for (i, seg) in segments.iter().enumerate() {
        if seg.covered_lsn_min > seg.covered_lsn_max {
            return Err(MetaDbError::Corruption(format!(
                "refcount segment directory {head_pid} segment {i} covered range \
                 {}..={} inverted",
                seg.covered_lsn_min, seg.covered_lsn_max
            )));
        }
        if let Some(prev) = prev_max
            && seg.covered_lsn_max < prev
        {
            return Err(MetaDbError::Corruption(format!(
                "refcount segment directory {head_pid} segment {i} covered_lsn_max {} \
                 regresses below the previous segment's {prev}",
                seg.covered_lsn_max
            )));
        }
        prev_max = Some(seg.covered_lsn_max);
    }
    Ok(())
}

/// Every page id the directory at `head_pid` occupies AND references: the chain
/// framing pages plus every segment's data pages. Used by the verifier /
/// orphan-reclaim / device-open protected set so live segment pages are never
/// reclaimed. Empty for a [`NULL_PAGE`] head.
pub(crate) fn collect_live_pages(page_store: &PageStore, head_pid: PageId) -> Result<Vec<PageId>> {
    let mut pids = Vec::new();
    let bytes = walk(page_store, head_pid, &mut |pid| pids.push(pid))?;
    for seg in decode_directory(&bytes)? {
        pids.extend_from_slice(&seg.data_pids);
    }
    Ok(pids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn store() -> (TempDir, Arc<PageStore>) {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(PageStore::create(dir.path().join("pages")).unwrap());
        (dir, store)
    }

    fn seg(bfg: Bfg, min: Lsn, max: Lsn, records: u64, pids: &[PageId]) -> SegmentDescriptor {
        SegmentDescriptor {
            bfg,
            covered_lsn_min: min,
            covered_lsn_max: max,
            records,
            data_pids: pids.to_vec(),
        }
    }

    #[test]
    fn byte_codec_round_trips_including_pid_runs() {
        let segments = vec![
            // Contiguous ascending pids → coalesced to one run.
            seg(3, 10, 40, 700, &[100, 101, 102, 103]),
            // Non-monotone + empty pid list edge cases.
            seg(7, 41, 55, 12, &[500, 502, 503, 999]),
            seg(9, 60, 60, 0, &[]),
        ];
        let bytes = encode_directory(&segments);
        assert_eq!(decode_directory(&bytes).unwrap(), segments);
    }

    #[test]
    fn empty_directory_round_trips() {
        assert!(decode_directory(&encode_directory(&[])).unwrap().is_empty());
        // A zero-length stream (NULL head walk) is also empty.
        assert!(decode_directory(&[]).unwrap().is_empty());
    }

    #[test]
    fn decode_rejects_truncated_and_trailing() {
        let bytes = encode_directory(&[seg(1, 2, 3, 4, &[9, 10])]);
        // Truncated.
        assert!(matches!(
            decode_directory(&bytes[..bytes.len() - 1]),
            Err(MetaDbError::Corruption(_))
        ));
        // Trailing junk.
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(matches!(
            decode_directory(&extra),
            Err(MetaDbError::Corruption(_))
        ));
    }

    #[test]
    fn decode_rejects_zero_length_pid_run_and_inverted_range() {
        // Inverted covered range.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_le_bytes()); // 1 segment
        bytes.extend_from_slice(&1u64.to_le_bytes()); // bfg
        bytes.extend_from_slice(&5u64.to_le_bytes()); // min
        bytes.extend_from_slice(&4u64.to_le_bytes()); // max < min
        bytes.extend_from_slice(&0u64.to_le_bytes()); // records
        bytes.extend_from_slice(&0u32.to_le_bytes()); // 0 runs
        assert!(matches!(
            decode_directory(&bytes),
            Err(MetaDbError::Corruption(_))
        ));
    }

    #[test]
    fn chain_round_trips_within_and_across_pages() {
        let (_d, store) = store();
        // A directory big enough to span multiple chain pages: many segments,
        // each with a non-coalescible (strided) pid list so the stream is large.
        let mut segments = Vec::new();
        for i in 0..400u64 {
            let pids: Vec<PageId> = (0..8).map(|k| 1_000 + i * 100 + k * 2).collect();
            segments.push(seg(i % 4, i, i + 3, 8, &pids));
        }
        let head = store.allocate().unwrap();
        let (chain, sealed, _free) = build_directory_chain(&store, &[head], &segments, 42).unwrap();
        assert_eq!(chain[0], head, "head pid is stable/reused");
        assert!(chain.len() > 1, "should span multiple chain pages");
        store.write_sealed_page_runs(sealed).unwrap();
        store.sync().unwrap();

        assert_eq!(read_directory_chain(&store, chain[0]).unwrap(), segments);

        // Live pages = framing pages + every segment's data pages.
        let live = collect_live_pages(&store, chain[0]).unwrap();
        let mut expected = chain.clone();
        for s in &segments {
            expected.extend_from_slice(&s.data_pids);
        }
        assert_eq!(live, expected);
    }

    #[test]
    fn stable_head_reused_across_rewrites_with_grow_and_shrink() {
        let (_d, store) = store();
        // Reserve an empty directory head.
        let (head, sealed) = reserve_empty_directory(&store, 1).unwrap();
        store.write_sealed_page_runs(vec![sealed]).unwrap();
        store.sync().unwrap();
        assert!(read_directory_chain(&store, head).unwrap().is_empty());
        let mut chain = vec![head];

        // Append one small segment: head is REUSED, single page, nothing freed.
        let small = vec![seg(0, 1, 10, 2, &[7])];
        let (c1, s1, free1) = build_directory_chain(&store, &chain, &small, 2).unwrap();
        assert_eq!(c1[0], head, "head pid stable");
        assert_eq!(c1.len(), 1);
        assert!(free1.is_empty());
        store.write_sealed_page_runs(s1).unwrap();
        store.sync().unwrap();
        chain = c1;

        // Grow to a multi-page directory: head reused, a fresh continuation
        // allocated, nothing freed yet.
        let mut big = Vec::new();
        for i in 0..400u64 {
            let pids: Vec<PageId> = (0..8).map(|k| 1_000 + i * 100 + k * 2).collect();
            big.push(seg(i % 4, i, i + 3, 8, &pids));
        }
        let (c2, s2, free2) = build_directory_chain(&store, &chain, &big, 3).unwrap();
        assert_eq!(c2[0], head, "head pid still stable across grow");
        assert!(c2.len() > 1, "grew to multiple pages");
        assert!(free2.is_empty(), "growth frees nothing");
        store.write_sealed_page_runs(s2).unwrap();
        store.sync().unwrap();
        assert_eq!(read_directory_chain(&store, head).unwrap(), big);
        let grown = c2;

        // Shrink back to one segment: head reused, the continuation pids are
        // returned as `to_free`.
        let (c3, s3, free3) = build_directory_chain(&store, &grown, &small, 4).unwrap();
        assert_eq!(c3[0], head);
        assert_eq!(c3.len(), 1);
        assert_eq!(free3, grown[1..].to_vec(), "trailing continuations freed");
        store.write_sealed_page_runs(s3).unwrap();
        store.sync().unwrap();
        assert_eq!(read_directory_chain(&store, head).unwrap(), small);
    }

    #[test]
    fn null_head_is_empty() {
        let (_d, store) = store();
        assert!(read_directory_chain(&store, NULL_PAGE).unwrap().is_empty());
        assert!(collect_live_pages(&store, NULL_PAGE).unwrap().is_empty());
        verify_directory(&store, NULL_PAGE).unwrap();
    }

    #[test]
    fn verify_directory_accepts_monotone_rejects_regression() {
        let (_d, store) = store();
        // Monotone covered_lsn_max: accepted.
        let ok = vec![seg(0, 1, 10, 2, &[7]), seg(1, 11, 20, 2, &[8])];
        let head1 = store.allocate().unwrap();
        let (chain, sealed, _f) = build_directory_chain(&store, &[head1], &ok, 1).unwrap();
        store.write_sealed_page_runs(sealed).unwrap();
        store.sync().unwrap();
        verify_directory(&store, chain[0]).unwrap();

        // Regressing covered_lsn_max (segment 1's max < segment 0's): rejected.
        let bad = vec![seg(0, 1, 30, 2, &[7]), seg(1, 5, 20, 2, &[8])];
        let head2 = store.allocate().unwrap();
        let (chain2, sealed2, _f2) = build_directory_chain(&store, &[head2], &bad, 2).unwrap();
        store.write_sealed_page_runs(sealed2).unwrap();
        store.sync().unwrap();
        assert!(matches!(
            verify_directory(&store, chain2[0]),
            Err(MetaDbError::Corruption(_))
        ));
    }

    #[test]
    fn wrong_page_type_is_rejected() {
        let (_d, store) = store();
        let head = store.allocate().unwrap();
        let (chain, sealed, _f) =
            build_directory_chain(&store, &[head], &[seg(1, 1, 1, 1, &[7])], 1).unwrap();
        store.write_sealed_page_runs(sealed).unwrap();
        store.sync().unwrap();
        // Overwrite the head with a plain Free page — read must reject it.
        let mut bogus = Page::new(PageHeader::new(PageType::Free, 1));
        bogus.seal();
        store.write_page(chain[0], &bogus).unwrap();
        store.sync().unwrap();
        assert!(read_directory_chain(&store, chain[0]).is_err());
    }
}
