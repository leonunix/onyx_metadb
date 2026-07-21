//! In-memory codec for bounded immutable refcount delta runs.
//!
//! The checkpoint shadow path uses this module to measure the exact page and
//! byte cost of persisting a frozen BFG slot without allocating page IDs or
//! issuing IO. Each page is sealed and handed to a callback immediately, so a
//! production measurement retains at most one 4 KiB [`Page`] at a time.
//!
//! A run is an ordered sequence. Future durable readers must apply runs oldest
//! to newest; combining entries across runs would lose zero-crossing and
//! `birth_lsn` semantics.

use crate::error::{MetaDbError, Result};
use crate::page::{PAGE_PAYLOAD_SIZE, Page, PageHeader, PageType};
use crate::refcount::delta::Pending;
use crate::types::{Bfg, Lsn, PageId, Pba};
use std::time::Instant;

const CODEC_MAGIC: [u8; 4] = *b"RCDR";
pub(crate) const DELTA_RUN_CODEC_VERSION: u8 = 1;
#[cfg(test)]
const PAGE_AFFINE_ROUTING_VERSION: u32 = 26;
#[cfg(test)]
const UNKNOWN_SHARD_ID: u32 = u32::MAX;

const TH_MAGIC: usize = 0;
const TH_CODEC_VERSION: usize = 4;
const TH_ROUTING_VERSION: usize = 5;
const TH_RESERVED: usize = 6;
const TH_SHARD_ID: usize = 8;
const TH_PAYLOAD_BYTES: usize = 12;
const TH_RESERVED_2: usize = 14;
const TH_FIRST_PBA: usize = 16;
const TH_COVERED_LSN_MIN: usize = 24;

const MAX_VARINT_BYTES: usize = 10;
const MAX_RECORD_BYTES: usize = MAX_VARINT_BYTES * 3;

const _: () = {
    assert!(TH_COVERED_LSN_MIN + 8 == crate::page::TYPE_HEADER_SIZE);
    assert!(MAX_RECORD_BYTES <= PAGE_PAYLOAD_SIZE);
};

/// Durable identity and replay boundary carried by every page in one run.
///
/// The checkpoint shadow path uses [`measure_shadow_run_with_context`] so the
/// measured bytes carry the real shard/BFG identity. [`measure_shadow_run`] is
/// a context-inferred convenience used by codec tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DeltaRunContext {
    pub routing_version: u32,
    pub shard_id: u32,
    pub bfg: Bfg,
    pub covered_lsn_min: Lsn,
    pub covered_lsn_max: Lsn,
}

/// Exact encoded data-page size of one shadow run, plus the descriptor echoed
/// into its pages. `pages` and `payload_bytes` are a lower bound for a future
/// durable layout: they exclude its manifest anchor, page locator/index, and
/// compaction metadata. `payload_bytes` also excludes headers and zero padding.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DeltaRunStats {
    pub records: u64,
    pub pages: u64,
    pub payload_bytes: u64,
    /// CPU spent decoding sealed pages, verifying CRC/context, and comparing
    /// every decoded record with the source slice. Excludes encoding itself.
    pub verify_us: u64,
    pub codec_version: u8,
    pub routing_version: u32,
    pub shard_id: u32,
    pub bfg: Bfg,
    pub covered_lsn_min: Lsn,
    pub covered_lsn_max: Lsn,
}

impl DeltaRunStats {
    fn new(context: DeltaRunContext) -> Self {
        Self {
            codec_version: DELTA_RUN_CODEC_VERSION,
            routing_version: context.routing_version,
            shard_id: context.shard_id,
            bfg: context.bfg,
            covered_lsn_min: context.covered_lsn_min,
            covered_lsn_max: context.covered_lsn_max,
            ..Self::default()
        }
    }
}

impl DeltaRunContext {
    pub(crate) fn for_manifest_routing(
        routing_version: u32,
        shard_id: u32,
        bfg: Bfg,
        covered_lsn_min: Lsn,
        covered_lsn_max: Lsn,
    ) -> Self {
        Self {
            routing_version,
            shard_id,
            bfg,
            covered_lsn_min,
            covered_lsn_max,
        }
    }
}

/// Encode a sorted frozen slot in memory, seal each page, account it, and drop
/// it immediately. No [`crate::page_store::PageStore`] operation is performed.
#[cfg(test)]
pub(crate) fn measure_shadow_run(records: &[(Pba, Pending)]) -> Result<DeltaRunStats> {
    let (covered_lsn_min, covered_lsn_max) = lsn_bounds(records);
    let context = DeltaRunContext {
        routing_version: PAGE_AFFINE_ROUTING_VERSION,
        shard_id: UNKNOWN_SHARD_ID,
        bfg: 0,
        covered_lsn_min,
        covered_lsn_max,
    };
    measure_shadow_run_with_context(context, records)
}

/// Context-aware shadow encoder for the checkpoint integration. Pages are
/// still pure in-memory values and are dropped after sealing.
pub(crate) fn measure_shadow_run_with_context(
    context: DeltaRunContext,
    records: &[(Pba, Pending)],
) -> Result<DeltaRunStats> {
    let mut verify_us = 0u64;
    let mut page_id = 0u64;
    let mut stats = encode_pages(context, records, |page, expected| {
        let started = Instant::now();
        let decoded = decode_page(&page, page_id)?;
        if decoded.context != context || decoded.records.as_slice() != expected {
            return Err(MetaDbError::Corruption(format!(
                "refcount delta-run shadow verification mismatch on page {page_id}"
            )));
        }
        verify_us = verify_us
            .saturating_add(started.elapsed().as_micros().min(u128::from(u64::MAX)) as u64);
        page_id = page_id.saturating_add(1);
        Ok(())
    })?;
    stats.verify_us = verify_us;
    Ok(stats)
}

/// Encode a sorted frozen slot into sealed [`PageType::RefcountDeltaRun`] pages
/// for the DURABLE persist path (v27). The caller assigns a [`PageId`] to each
/// returned page and writes them via `write_sealed_page_runs_for_class`; the
/// page CRC is pid-independent (`Page::verify(pid)` uses the pid only for error
/// reporting), so sealing before allocation is sound. Returns the run stats
/// (records / pages / payload_bytes; timing fields stay zero) beside the pages.
pub(crate) fn encode_run_pages(
    context: DeltaRunContext,
    records: &[(Pba, Pending)],
) -> Result<(DeltaRunStats, Vec<Page>)> {
    let mut pages = Vec::new();
    let stats = encode_pages(context, records, |page, _expected| {
        pages.push(page);
        Ok(())
    })?;
    Ok((stats, pages))
}

/// Decode one sealed segment page back into its records + descriptor context.
/// Used by condense / open-replay to reconstruct the delta stream oldest→newest.
pub(crate) fn decode_run_page(page: &Page, page_id: PageId) -> Result<DecodedRun> {
    let decoded = decode_page(page, page_id)?;
    Ok(DecodedRun {
        context: decoded.context,
        records: decoded.records,
    })
}

/// Public shape returned by [`decode_run_page`].
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct DecodedRun {
    pub context: DeltaRunContext,
    pub records: Vec<(Pba, Pending)>,
}

#[cfg(test)]
fn lsn_bounds(records: &[(Pba, Pending)]) -> (Lsn, Lsn) {
    let mut min = Lsn::MAX;
    let mut max = 0;
    for (_, pending) in records {
        min = min.min(pending.last_lsn);
        max = max.max(pending.last_lsn);
    }
    if records.is_empty() {
        (0, 0)
    } else {
        (min, max)
    }
}

fn validate_context(context: DeltaRunContext, records: &[(Pba, Pending)]) -> Result<()> {
    if super::RefcountRouting::from_manifest_version(context.routing_version).is_none() {
        return Err(MetaDbError::InvalidArgument(format!(
            "refcount delta-run routing version {} is unsupported",
            context.routing_version
        )));
    }
    if context.covered_lsn_min > context.covered_lsn_max {
        return Err(MetaDbError::InvalidArgument(format!(
            "refcount delta-run covered LSN range {}..={} is inverted",
            context.covered_lsn_min, context.covered_lsn_max
        )));
    }
    for window in records.windows(2) {
        if window[0].0 >= window[1].0 {
            return Err(MetaDbError::InvalidArgument(format!(
                "refcount delta-run PBAs must be strictly increasing: {} then {}",
                window[0].0, window[1].0
            )));
        }
    }
    for &(pba, pending) in records {
        if pending.last_lsn < context.covered_lsn_min || pending.last_lsn > context.covered_lsn_max
        {
            return Err(MetaDbError::InvalidArgument(format!(
                "refcount delta-run PBA {pba} last_lsn {} outside covered range {}..={} ",
                pending.last_lsn, context.covered_lsn_min, context.covered_lsn_max
            )));
        }
    }
    Ok(())
}

fn encode_pages<F>(
    context: DeltaRunContext,
    records: &[(Pba, Pending)],
    mut consume: F,
) -> Result<DeltaRunStats>
where
    F: FnMut(Page, &[(Pba, Pending)]) -> Result<()>,
{
    validate_context(context, records)?;
    let mut stats = DeltaRunStats::new(context);
    if records.is_empty() {
        return Ok(stats);
    }

    let mut cursor = 0;
    while cursor < records.len() {
        let page_start = cursor;
        let first_pba = records[cursor].0;
        let mut previous_pba = first_pba;
        let mut payload_bytes = 0usize;
        let mut page_records = 0u16;

        let mut page = Page::new(PageHeader::new(
            PageType::RefcountDeltaRun,
            context.covered_lsn_max,
        ));
        page.set_birth_lsn(context.bfg);

        while cursor < records.len() {
            let (pba, pending) = records[cursor];
            let pba_delta = if cursor == page_start {
                0
            } else {
                pba.checked_sub(previous_pba).ok_or_else(|| {
                    MetaDbError::InvalidArgument("refcount delta-run PBA order regressed".into())
                })?
            };
            let mut encoded = [0u8; MAX_RECORD_BYTES];
            let encoded_len = encode_record(pba_delta, pending, &mut encoded);
            if page_records > 0 && payload_bytes + encoded_len > PAGE_PAYLOAD_SIZE {
                break;
            }
            debug_assert!(payload_bytes + encoded_len <= PAGE_PAYLOAD_SIZE);
            page.payload_mut()[payload_bytes..payload_bytes + encoded_len]
                .copy_from_slice(&encoded[..encoded_len]);
            payload_bytes += encoded_len;
            page_records += 1;
            previous_pba = pba;
            cursor += 1;
        }

        page.set_key_count(page_records);
        write_type_header(&mut page, context, first_pba, payload_bytes)?;
        page.seal();

        stats.records = stats.records.saturating_add(u64::from(page_records));
        stats.pages = stats.pages.saturating_add(1);
        stats.payload_bytes = stats.payload_bytes.saturating_add(payload_bytes as u64);
        consume(page, &records[page_start..cursor])?;
    }
    Ok(stats)
}

fn write_type_header(
    page: &mut Page,
    context: DeltaRunContext,
    first_pba: Pba,
    payload_bytes: usize,
) -> Result<()> {
    let payload_bytes = u16::try_from(payload_bytes).map_err(|_| {
        MetaDbError::InvalidArgument("refcount delta-run page payload exceeds u16".into())
    })?;
    let h = page.type_header_mut();
    h[TH_MAGIC..TH_MAGIC + 4].copy_from_slice(&CODEC_MAGIC);
    h[TH_CODEC_VERSION] = DELTA_RUN_CODEC_VERSION;
    h[TH_ROUTING_VERSION] = context.routing_version as u8;
    h[TH_RESERVED..TH_RESERVED + 2].fill(0);
    h[TH_SHARD_ID..TH_SHARD_ID + 4].copy_from_slice(&context.shard_id.to_le_bytes());
    h[TH_PAYLOAD_BYTES..TH_PAYLOAD_BYTES + 2].copy_from_slice(&payload_bytes.to_le_bytes());
    h[TH_RESERVED_2..TH_RESERVED_2 + 2].fill(0);
    h[TH_FIRST_PBA..TH_FIRST_PBA + 8].copy_from_slice(&first_pba.to_le_bytes());
    h[TH_COVERED_LSN_MIN..TH_COVERED_LSN_MIN + 8]
        .copy_from_slice(&context.covered_lsn_min.to_le_bytes());
    Ok(())
}

fn encode_record(pba_delta: u64, pending: Pending, out: &mut [u8; MAX_RECORD_BYTES]) -> usize {
    let mut cursor = 0;
    cursor += encode_uvarint(pba_delta, &mut out[cursor..]);
    cursor += encode_uvarint(zigzag_encode(pending.delta), &mut out[cursor..]);
    cursor += encode_uvarint(pending.last_lsn, &mut out[cursor..]);
    cursor
}

fn encode_uvarint(mut value: u64, out: &mut [u8]) -> usize {
    let mut cursor = 0;
    while value >= 0x80 {
        out[cursor] = (value as u8 & 0x7f) | 0x80;
        value >>= 7;
        cursor += 1;
    }
    out[cursor] = value as u8;
    cursor + 1
}

#[inline]
fn zigzag_encode(value: i64) -> u64 {
    ((value as u64) << 1) ^ ((value >> 63) as u64)
}

#[inline]
fn zigzag_decode(value: u64) -> i64 {
    ((value >> 1) as i64) ^ (-((value & 1) as i64))
}

#[derive(Debug, PartialEq, Eq)]
struct DecodedPage {
    context: DeltaRunContext,
    records: Vec<(Pba, Pending)>,
    payload_bytes: u16,
}

#[allow(dead_code)]
fn decode_page(page: &Page, page_id: PageId) -> Result<DecodedPage> {
    page.verify(page_id)?;
    let header = page.header()?;
    if header.page_type != PageType::RefcountDeltaRun {
        return Err(MetaDbError::Corruption(format!(
            "page {page_id} is {:?}, expected RefcountDeltaRun",
            header.page_type
        )));
    }
    if header.key_count == 0 {
        return Err(MetaDbError::Corruption(format!(
            "refcount delta-run page {page_id} has no records"
        )));
    }

    let th = page.type_header();
    if th[TH_MAGIC..TH_MAGIC + 4] != CODEC_MAGIC {
        return Err(MetaDbError::Corruption(format!(
            "refcount delta-run page {page_id} has invalid codec magic"
        )));
    }
    if th[TH_CODEC_VERSION] != DELTA_RUN_CODEC_VERSION {
        return Err(MetaDbError::Corruption(format!(
            "refcount delta-run page {page_id} has unsupported codec version {}",
            th[TH_CODEC_VERSION]
        )));
    }
    if th[TH_RESERVED..TH_RESERVED + 2] != [0, 0] || th[TH_RESERVED_2..TH_RESERVED_2 + 2] != [0, 0]
    {
        return Err(MetaDbError::Corruption(format!(
            "refcount delta-run page {page_id} has non-zero reserved bytes"
        )));
    }

    let routing_version = u32::from(th[TH_ROUTING_VERSION]);
    if super::RefcountRouting::from_manifest_version(routing_version).is_none() {
        return Err(MetaDbError::Corruption(format!(
            "refcount delta-run page {page_id} has unsupported routing version {routing_version}"
        )));
    }
    let shard_id = u32::from_le_bytes(th[TH_SHARD_ID..TH_SHARD_ID + 4].try_into().unwrap());
    let payload_bytes = u16::from_le_bytes(
        th[TH_PAYLOAD_BYTES..TH_PAYLOAD_BYTES + 2]
            .try_into()
            .unwrap(),
    );
    if usize::from(payload_bytes) > PAGE_PAYLOAD_SIZE {
        return Err(MetaDbError::Corruption(format!(
            "refcount delta-run page {page_id} payload length {payload_bytes} exceeds page"
        )));
    }
    let first_pba = u64::from_le_bytes(th[TH_FIRST_PBA..TH_FIRST_PBA + 8].try_into().unwrap());
    let covered_lsn_min = u64::from_le_bytes(
        th[TH_COVERED_LSN_MIN..TH_COVERED_LSN_MIN + 8]
            .try_into()
            .unwrap(),
    );
    let context = DeltaRunContext {
        routing_version,
        shard_id,
        bfg: header.birth_lsn,
        covered_lsn_min,
        covered_lsn_max: header.generation,
    };
    if context.covered_lsn_min > context.covered_lsn_max {
        return Err(MetaDbError::Corruption(format!(
            "refcount delta-run page {page_id} has inverted covered LSN range"
        )));
    }

    let payload = &page.payload()[..usize::from(payload_bytes)];
    let mut cursor = 0;
    let mut previous_pba = first_pba;
    let mut records = Vec::with_capacity(usize::from(header.key_count));
    for index in 0..header.key_count {
        let pba_delta = decode_uvarint(payload, &mut cursor, "PBA delta", page_id)?;
        if index == 0 && pba_delta != 0 {
            return Err(MetaDbError::Corruption(format!(
                "refcount delta-run page {page_id} first PBA delta is not zero"
            )));
        }
        if index > 0 && pba_delta == 0 {
            return Err(MetaDbError::Corruption(format!(
                "refcount delta-run page {page_id} contains duplicate PBA"
            )));
        }
        let pba = if index == 0 {
            first_pba
        } else {
            previous_pba.checked_add(pba_delta).ok_or_else(|| {
                MetaDbError::Corruption(format!(
                    "refcount delta-run page {page_id} PBA delta overflows"
                ))
            })?
        };
        let delta = zigzag_decode(decode_uvarint(payload, &mut cursor, "delta", page_id)?);
        let last_lsn = decode_uvarint(payload, &mut cursor, "last_lsn", page_id)?;
        if last_lsn < context.covered_lsn_min || last_lsn > context.covered_lsn_max {
            return Err(MetaDbError::Corruption(format!(
                "refcount delta-run page {page_id} PBA {pba} last_lsn {last_lsn} outside covered range"
            )));
        }
        records.push((pba, Pending { delta, last_lsn }));
        previous_pba = pba;
    }
    if cursor != payload.len() {
        return Err(MetaDbError::Corruption(format!(
            "refcount delta-run page {page_id} has {} trailing payload bytes",
            payload.len() - cursor
        )));
    }
    Ok(DecodedPage {
        context,
        records,
        payload_bytes,
    })
}

fn decode_uvarint(bytes: &[u8], cursor: &mut usize, field: &str, page_id: PageId) -> Result<u64> {
    let start = *cursor;
    let mut value = 0u64;
    for index in 0..MAX_VARINT_BYTES {
        let byte = *bytes.get(*cursor).ok_or_else(|| {
            MetaDbError::Corruption(format!(
                "refcount delta-run page {page_id} truncated {field} varint"
            ))
        })?;
        *cursor += 1;
        if index == MAX_VARINT_BYTES - 1 && byte > 1 {
            return Err(MetaDbError::Corruption(format!(
                "refcount delta-run page {page_id} {field} varint overflows u64"
            )));
        }
        value |= u64::from(byte & 0x7f) << (index * 7);
        if byte & 0x80 == 0 {
            if *cursor - start > 1 && byte == 0 {
                return Err(MetaDbError::Corruption(format!(
                    "refcount delta-run page {page_id} {field} varint is non-canonical"
                )));
            }
            return Ok(value);
        }
    }
    Err(MetaDbError::Corruption(format!(
        "refcount delta-run page {page_id} {field} varint exceeds ten bytes"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::refcount::{RcEntry, apply_delta_pure};

    fn context(records: &[(Pba, Pending)]) -> DeltaRunContext {
        let (covered_lsn_min, covered_lsn_max) = lsn_bounds(records);
        DeltaRunContext {
            routing_version: PAGE_AFFINE_ROUTING_VERSION,
            shard_id: 7,
            bfg: 1234,
            covered_lsn_min,
            covered_lsn_max,
        }
    }

    fn collect(records: &[(Pba, Pending)]) -> (DeltaRunStats, Vec<Page>) {
        let mut pages = Vec::new();
        let stats = encode_pages(context(records), records, |page, _expected| {
            pages.push(page);
            Ok(())
        })
        .unwrap();
        (stats, pages)
    }

    fn decode_all(pages: &[Page]) -> Vec<(Pba, Pending)> {
        let mut decoded = Vec::new();
        for (index, page) in pages.iter().enumerate() {
            decoded.extend(decode_page(page, index as PageId).unwrap().records);
        }
        decoded
    }

    #[test]
    fn empty_run_has_zero_size_and_allocates_no_page() {
        let (stats, pages) = collect(&[]);
        assert_eq!(stats.records, 0);
        assert_eq!(stats.pages, 0);
        assert_eq!(stats.payload_bytes, 0);
        assert!(pages.is_empty());
    }

    #[test]
    fn page_type_16_is_registered() {
        assert_eq!(
            PageType::from_u8(PageType::RefcountDeltaRun as u8).unwrap(),
            PageType::RefcountDeltaRun
        );
    }

    #[test]
    fn reference_roundtrip_spans_pages_and_preserves_zero_net_pending() {
        let mut records = Vec::new();
        for index in 0..5000u64 {
            records.push((
                index.saturating_mul(1021),
                Pending {
                    delta: match index % 5 {
                        0 => 0,
                        1 => 1,
                        2 => -1,
                        3 => i64::MAX,
                        _ => i64::MIN,
                    },
                    last_lsn: 10_000 + index.saturating_mul(3),
                },
            ));
        }
        let (stats, pages) = collect(&records);
        assert_eq!(stats.records, records.len() as u64);
        assert_eq!(stats.pages, pages.len() as u64);
        assert!(stats.pages > 1);
        assert!(stats.payload_bytes < stats.records * 28);
        assert_eq!(decode_all(&pages), records);
        for page in &pages {
            page.verify(99).unwrap();
            let decoded = decode_page(page, 99).unwrap();
            assert_eq!(decoded.context, context(&records));
        }
        assert!(
            decode_all(&pages)
                .iter()
                .any(|(_, pending)| pending.delta == 0)
        );
    }

    #[test]
    fn rejects_unsorted_and_duplicate_pbas() {
        for records in [
            vec![
                (
                    2,
                    Pending {
                        delta: 1,
                        last_lsn: 1,
                    },
                ),
                (
                    1,
                    Pending {
                        delta: 1,
                        last_lsn: 2,
                    },
                ),
            ],
            vec![
                (
                    2,
                    Pending {
                        delta: 1,
                        last_lsn: 1,
                    },
                ),
                (
                    2,
                    Pending {
                        delta: -1,
                        last_lsn: 2,
                    },
                ),
            ],
        ] {
            assert!(matches!(
                measure_shadow_run(&records),
                Err(MetaDbError::InvalidArgument(_))
            ));
        }
    }

    #[test]
    fn truncated_payload_is_rejected_after_valid_crc() {
        let records = vec![
            (
                4,
                Pending {
                    delta: 1,
                    last_lsn: 300,
                },
            ),
            (
                9,
                Pending {
                    delta: -2,
                    last_lsn: 700,
                },
            ),
        ];
        let (_, mut pages) = collect(&records);
        let page = &mut pages[0];
        let payload_bytes = u16::from_le_bytes(
            page.type_header()[TH_PAYLOAD_BYTES..TH_PAYLOAD_BYTES + 2]
                .try_into()
                .unwrap(),
        );
        page.type_header_mut()[TH_PAYLOAD_BYTES..TH_PAYLOAD_BYTES + 2]
            .copy_from_slice(&(payload_bytes - 1).to_le_bytes());
        page.seal();
        assert!(matches!(
            decode_page(page, 17),
            Err(MetaDbError::Corruption(_))
        ));
    }

    #[test]
    fn crc_detects_payload_corruption() {
        let records = vec![(
            4,
            Pending {
                delta: 1,
                last_lsn: 300,
            },
        )];
        let (_, mut pages) = collect(&records);
        pages[0].payload_mut()[0] ^= 0x40;
        assert!(matches!(
            decode_page(&pages[0], 23),
            Err(MetaDbError::PageChecksumMismatch { page_id: 23, .. })
        ));
    }

    #[test]
    fn decoded_runs_must_be_applied_oldest_to_newest() {
        let pba = 77;
        let oldest = vec![(
            pba,
            Pending {
                delta: -1,
                last_lsn: 10,
            },
        )];
        let newest = vec![(
            pba,
            Pending {
                delta: 1,
                last_lsn: 20,
            },
        )];
        let (_, oldest_pages) = collect(&oldest);
        let (_, newest_pages) = collect(&newest);

        let mut eager = RcEntry {
            rc: 1,
            birth_lsn: 5,
        };
        for (_, pending) in oldest.iter().chain(newest.iter()) {
            eager = apply_delta_pure(eager, pending.delta, pending.last_lsn).unwrap();
        }
        let mut decoded = RcEntry {
            rc: 1,
            birth_lsn: 5,
        };
        for pages in [&oldest_pages, &newest_pages] {
            for (_, pending) in decode_all(pages) {
                decoded = apply_delta_pure(decoded, pending.delta, pending.last_lsn).unwrap();
            }
        }
        assert_eq!(decoded, eager);
        assert_eq!(
            decoded,
            RcEntry {
                rc: 1,
                birth_lsn: 20
            }
        );

        let incorrectly_combined = apply_delta_pure(
            RcEntry {
                rc: 1,
                birth_lsn: 5,
            },
            oldest[0].1.delta + newest[0].1.delta,
            newest[0].1.last_lsn,
        )
        .unwrap();
        assert_ne!(incorrectly_combined, decoded);
    }

    #[test]
    fn rejects_invalid_context_and_record_lsn_range() {
        let records = vec![(
            9,
            Pending {
                delta: 0,
                last_lsn: 20,
            },
        )];
        for bad_context in [
            DeltaRunContext {
                routing_version: 99,
                ..context(&records)
            },
            DeltaRunContext {
                covered_lsn_min: 21,
                covered_lsn_max: 20,
                ..context(&records)
            },
            DeltaRunContext {
                covered_lsn_min: 1,
                covered_lsn_max: 19,
                ..context(&records)
            },
        ] {
            assert!(matches!(
                measure_shadow_run_with_context(bad_context, &records),
                Err(MetaDbError::InvalidArgument(_))
            ));
        }
    }

    #[test]
    fn rejects_corrupt_codec_header_and_key_count() {
        let records = vec![(
            4,
            Pending {
                delta: 1,
                last_lsn: 300,
            },
        )];
        for case in 0..6 {
            let (_, mut pages) = collect(&records);
            let page = &mut pages[0];
            match case {
                0 => page.type_header_mut()[TH_MAGIC] ^= 1,
                1 => page.type_header_mut()[TH_CODEC_VERSION] += 1,
                2 => page.type_header_mut()[TH_ROUTING_VERSION] = 99,
                3 => page.type_header_mut()[TH_RESERVED] = 1,
                4 => page.set_key_count(0),
                5 => page.set_generation(299),
                _ => unreachable!(),
            }
            page.seal();
            assert!(matches!(
                decode_page(page, 31 + case),
                Err(MetaDbError::Corruption(_))
            ));
        }
    }

    #[test]
    fn rejects_noncanonical_overflow_trailing_and_duplicate_payloads() {
        let records = vec![
            (
                10,
                Pending {
                    delta: 1,
                    last_lsn: 1,
                },
            ),
            (
                20,
                Pending {
                    delta: 1,
                    last_lsn: 2,
                },
            ),
        ];

        for case in 0..4 {
            let (_, mut pages) = collect(&records);
            let page = &mut pages[0];
            let payload_bytes = u16::from_le_bytes(
                page.type_header()[TH_PAYLOAD_BYTES..TH_PAYLOAD_BYTES + 2]
                    .try_into()
                    .unwrap(),
            ) as usize;
            match case {
                0 => {
                    page.payload_mut().copy_within(1..payload_bytes, 2);
                    page.payload_mut()[0] = 0x80;
                    page.payload_mut()[1] = 0;
                    set_payload_bytes(page, payload_bytes + 1);
                }
                1 => {
                    page.payload_mut()[..9].fill(0x80);
                    page.payload_mut()[9] = 2;
                    set_payload_bytes(page, 10);
                }
                2 => {
                    page.payload_mut()[payload_bytes] = 0;
                    set_payload_bytes(page, payload_bytes + 1);
                }
                3 => {
                    // First record is [pba_gap=0, delta=2, lsn=1], so byte 3
                    // is the second record's PBA gap.
                    page.payload_mut()[3] = 0;
                }
                _ => unreachable!(),
            }
            page.seal();
            assert!(matches!(
                decode_page(page, 41 + case),
                Err(MetaDbError::Corruption(_))
            ));
        }
    }

    fn set_payload_bytes(page: &mut Page, payload_bytes: usize) {
        page.type_header_mut()[TH_PAYLOAD_BYTES..TH_PAYLOAD_BYTES + 2]
            .copy_from_slice(&(payload_bytes as u16).to_le_bytes());
    }
}
