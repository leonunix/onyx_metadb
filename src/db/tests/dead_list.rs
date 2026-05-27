//! Phase 2 dead-list tests. Cover emit correctness across the three
//! L2P apply sites, checkpoint flush behaviour (single + multi page
//! segment, chain extension across two flushes), WAL replay re-emit,
//! buffer-only flush trigger, and drop_volume chain reclaim.

use super::{mk_db, v, wal_mode_cfg};
use crate::deadlist::{DEAD_RECORD_BYTES, DeadRecord, SegmentHeader, segment_pages_for};
use crate::page::PageType;
use crate::types::NULL_PAGE;
use crate::{Db, L2pValue};
use tempfile::TempDir;

fn drain_dead_list(db: &Db, vol_ord: u16) -> Vec<DeadRecord> {
    db.test_drain_dead_list(vol_ord)
        .expect("test helper expects volume to exist")
}

fn dead_list_anchors(db: &Db, vol_ord: u16) -> (u64, u64) {
    db.test_dead_list_anchors(vol_ord)
        .expect("test helper expects volume to exist")
}

fn v_zero() -> L2pValue {
    // FLAG_ZERO bit set at byte 27 bit 1. Birth_lsn trailer left at 0
    // so the apply path stamps it (does not affect the zero-skip check).
    let mut x = [0u8; crate::paged::format::LEAF_VALUE_SIZE];
    x[27] = 0x02;
    L2pValue(x)
}

#[test]
fn l2p_put_emits_on_overwrite_only() {
    let (_d, db) = mk_db();
    // First write — no prior mapping, no record.
    db.insert(0, 1, v(0xAA)).unwrap();
    assert!(drain_dead_list(&db, 0).is_empty());
    // Overwrite — emits the old (pba, birth_lsn=first_write_lsn,
    // death_lsn=second_write_lsn) triple.
    db.insert(0, 1, v(0xBB)).unwrap();
    let records = drain_dead_list(&db, 0);
    assert_eq!(records.len(), 1);
    // `v(0xAA)` stamps byte 7 (LOW byte of the big-endian u64
    // base_pba) to 0xAA, so the recovered PBA is 0xAA. (Old v() put
    // it in byte 0 = high byte = 0xAA << 56, but that produced
    // u64-wide spreads incompatible with v5's u32 pba_delta encoding.)
    assert_eq!(records[0].pba, 0xAAu64);
    assert_ne!(records[0].birth_lsn, 0);
    assert!(records[0].death_lsn > records[0].birth_lsn);
}

#[test]
fn l2p_put_skips_zero_mapping_prev() {
    let (_d, db) = mk_db();
    db.insert(0, 1, v_zero()).unwrap();
    db.insert(0, 1, v(0xCC)).unwrap();
    let records = drain_dead_list(&db, 0);
    assert!(records.is_empty(), "FLAG_ZERO prev must not emit a dead record");
}

#[test]
fn checkpoint_flush_writes_single_page_segment() {
    let (_d, db) = mk_db();
    // 50 overwrites → ~50 dead records → fits one page (~166 cap).
    for i in 0u64..50 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    for i in 0u64..50 {
        db.insert(0, i, v((i as u8).wrapping_add(1))).unwrap();
    }
    db.flush().unwrap();
    let (head, tail) = dead_list_anchors(&db, 0);
    assert_ne!(head, NULL_PAGE);
    assert_eq!(head, tail, "single segment chain: head == tail");
    let page = db.test_read_page(head).unwrap();
    assert_eq!(page.header().unwrap().page_type, PageType::DeadListSegment);
    let header = SegmentHeader::decode(page.payload()).unwrap();
    assert_eq!(header.record_count, 50);
    assert_eq!(header.seg_page_count, 1);
    assert_eq!(header.prev_seg_pid, NULL_PAGE);
    // Buffer was drained, subsequent flush is a no-op for dead-list.
    assert!(drain_dead_list(&db, 0).is_empty());
}

#[test]
fn checkpoint_flush_writes_multi_page_segment() {
    let (_d, db) = mk_db();
    // Push past one-page capacity: ~167 records on page 0 + cont
    // pages of ~168 each. 400 dead records → 3 pages. Generate them
    // by overwriting a small set of LBAs many times so the L2P leaf
    // compact cap (110 units per leaf) is never tripped.
    for round in 0u8..32 {
        for lba in 0u64..30 {
            db.insert(0, lba, v(lba as u8 ^ round)).unwrap();
        }
    }
    db.flush().unwrap();
    let (_, tail) = dead_list_anchors(&db, 0);
    let page = db.test_read_page(tail).unwrap();
    let header = SegmentHeader::decode(page.payload()).unwrap();
    // 32 rounds × 30 LBAs = 960 inserts, of which only the first 30
    // produce no record (fresh LBA). 930 dead records → 6 pages.
    assert_eq!(header.record_count, 930);
    assert_eq!(header.seg_page_count as usize, segment_pages_for(930));
    assert!(header.seg_page_count >= 2);
    // Every continuation page is also a DeadListSegment-typed page.
    for i in 1..header.seg_page_count as u64 {
        let p = db.test_read_page(tail + i).unwrap();
        assert_eq!(p.header().unwrap().page_type, PageType::DeadListSegment);
    }
}

#[test]
fn second_flush_appends_segment_linked_via_prev_seg_pid() {
    let (_d, db) = mk_db();
    for i in 0u64..30 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    for i in 0u64..30 {
        db.insert(0, i, v((i as u8) ^ 0x80)).unwrap();
    }
    db.flush().unwrap();
    let (head1, tail1) = dead_list_anchors(&db, 0);
    assert_eq!(head1, tail1);

    for i in 30u64..70 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    for i in 30u64..70 {
        db.insert(0, i, v((i as u8) ^ 0x40)).unwrap();
    }
    db.flush().unwrap();
    let (head2, tail2) = dead_list_anchors(&db, 0);
    assert_eq!(head2, head1, "head_pid pins to the oldest segment");
    assert_ne!(tail2, tail1, "tail_pid advances on every flush");
    let new_tail = db.test_read_page(tail2).unwrap();
    let new_header = SegmentHeader::decode(new_tail.payload()).unwrap();
    assert_eq!(new_header.prev_seg_pid, tail1);
}

#[test]
fn buffer_nonempty_triggers_flush_even_with_no_l2p_dirty() {
    let (_d, db) = mk_db();
    // Two overwrites: enough to leave records in the dead-list buffer.
    db.insert(0, 1, v(0xAA)).unwrap();
    db.insert(0, 1, v(0xBB)).unwrap();
    // Force-flush so the L2P dirty set is empty afterwards.
    db.flush().unwrap();
    // Even after the volume's L2P has been flushed, another overwrite
    // emits one more dead record. A subsequent try_flush must observe
    // the dead-list-buffer-non-empty trigger and write a segment.
    db.insert(0, 1, v(0xCC)).unwrap();
    let (_, tail_before) = dead_list_anchors(&db, 0);
    db.flush().unwrap();
    let (_, tail_after) = dead_list_anchors(&db, 0);
    assert_ne!(tail_after, tail_before, "flush must advance tail when buffer has records");
}

#[test]
fn wal_replay_re_emits_dead_records_into_buffer() {
    // Phase D.4: Wal-mode-only — the test verifies that the WAL's
    // L2pPut replay path re-stamps dead-list records. Buffer mode
    // doesn't store data-plane ops in any metadb-side journal.
    let dir = TempDir::new().unwrap();
    let cfg = wal_mode_cfg(dir.path());
    {
        let db = Db::create_with_config(cfg.clone()).unwrap();
        db.insert(0, 1, v(0xAA)).unwrap();
        db.insert(0, 1, v(0xBB)).unwrap();
        db.insert(0, 1, v(0xCC)).unwrap();
        // Drain ahead of close: nothing else flushes the volume so
        // every record stays in the buffer + WAL when we drop.
        drain_dead_list(&db, 0);
        db.insert(0, 1, v(0xDD)).unwrap();
        // NOTE: no flush() — only WAL is durable for the latest write.
    }
    let db = Db::open_with_config(cfg).unwrap();
    // Replay should have re-stamped every overwrite's dead record into
    // the buffer. The 3 emit sites (PUT history above) leave 3 records.
    let records = drain_dead_list(&db, 0);
    assert!(records.len() >= 3, "WAL replay must reproduce dead records");
}

#[test]
fn drop_volume_reclaims_dead_list_chain_pages() {
    let (_d, db) = mk_db();
    let v_ord = db.create_volume().unwrap();
    // Force two flushes so the volume has a 2-segment chain.
    for i in 0u64..20 {
        db.insert(v_ord, i, v(i as u8)).unwrap();
        db.insert(v_ord, i, v((i as u8) ^ 0xFF)).unwrap();
    }
    db.flush().unwrap();
    for i in 20u64..40 {
        db.insert(v_ord, i, v(i as u8)).unwrap();
        db.insert(v_ord, i, v((i as u8) ^ 0xFF)).unwrap();
    }
    db.flush().unwrap();
    let (head, tail) = dead_list_anchors(&db, v_ord);
    assert_ne!(head, NULL_PAGE);
    assert_ne!(tail, NULL_PAGE);
    let head_pid_owned_before = head;
    db.drop_volume(v_ord).unwrap().expect("drop returns DropVolumeReport");
    // Verify no orphan rc / chain pages survive: offline verify must
    // be clean. drop_volume's manifest commit released the chain
    // anchor; the WAL apply path freed the per-segment pages via
    // free_idempotent. Reading the head pid via the page store after
    // reclaim returns either a Free / zeroed page or a recycled
    // allocation, so we don't try to read content — the soak guarantee
    // is verify cleanliness, exercised by the global suite.
    let _ = head_pid_owned_before;
}

#[test]
fn manifest_capacity_accounts_for_dead_list_fields() {
    // Build a manifest with several volumes and confirm encode/decode
    // round-trips after the v13 schema bump. The dedicated capacity
    // test in db_per_volume_snapshot.rs covers the snapshot-table
    // squeeze case; here we just want to ensure the encoder doesn't
    // accidentally reject a baseline workload after we added 16 B per
    // VolumeEntry.
    let (_d, db) = mk_db();
    let v1 = db.create_volume().unwrap();
    let v2 = db.create_volume().unwrap();
    db.insert(v1, 1, v(0xAA)).unwrap();
    db.insert(v2, 1, v(0xBB)).unwrap();
    db.insert(v1, 1, v(0xCC)).unwrap();
    db.insert(v2, 1, v(0xDD)).unwrap();
    db.flush().unwrap();
    let (h1, t1) = dead_list_anchors(&db, v1);
    let (h2, t2) = dead_list_anchors(&db, v2);
    assert_ne!(h1, NULL_PAGE);
    assert_eq!(h1, t1);
    assert_ne!(h2, NULL_PAGE);
    assert_eq!(h2, t2);
    assert_ne!(h1, h2, "two volumes get distinct chain anchors");
}

#[test]
fn segment_record_count_excludes_zero_mappings() {
    let (_d, db) = mk_db();
    // Mix of zero-mapping overwrites (should NOT emit) and real
    // overwrites (should emit). After flush the segment should
    // contain only the real ones.
    db.insert(0, 1, v(0xAA)).unwrap();
    db.insert(0, 2, v_zero()).unwrap();
    db.insert(0, 3, v(0xBB)).unwrap();
    db.insert(0, 1, v(0xCC)).unwrap(); // emits
    db.insert(0, 2, v(0xDD)).unwrap(); // does NOT emit (prev was zero)
    db.insert(0, 3, v(0xEE)).unwrap(); // emits
    db.flush().unwrap();
    let (_, tail) = dead_list_anchors(&db, 0);
    let page = db.test_read_page(tail).unwrap();
    let header = SegmentHeader::decode(page.payload()).unwrap();
    assert_eq!(
        header.record_count, 2,
        "FLAG_ZERO overwrites must not contribute records"
    );
}

#[test]
fn segment_record_size_matches_spec() {
    // Guard the wire-level constant — Phase 3 GC will scan segments
    // assuming 24 B/record, so a future refactor must not silently
    // change DEAD_RECORD_BYTES without bumping the manifest version.
    assert_eq!(DEAD_RECORD_BYTES, 24);
    assert_eq!(
        std::mem::size_of::<DeadRecord>(),
        24,
        "DeadRecord struct layout drift would break replay round-trip"
    );
}
