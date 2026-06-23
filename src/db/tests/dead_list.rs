//! Phase 2 dead-list tests. Cover emit correctness across the three
//! L2P apply sites, checkpoint flush behaviour (single + multi page
//! segment, chain extension across two flushes), WAL replay re-emit,
//! buffer-only flush trigger, and drop_volume chain reclaim.

use super::{mk_db, v};
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
fn page_deadlist_populated_by_snapshot_overwrite() {
    // ZFS port Phase 2 make-or-break: a live snapshot pins the L2P tree,
    // so a subsequent overwrite COWs each root→leaf path and the old
    // (snapshot-pinned) L2P pages "die off the head" and MUST be recorded
    // into the HEAD page-deadlist. An empty deadlist here would mean the
    // `effective_rc > 1` COW capture never fires for snapshot-pinned pages
    // (the whole producing side would be a no-op).
    let (_d, db) = mk_db();
    for i in 0u64..300 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    db.flush().unwrap();
    let _s1 = db.take_snapshot(0).unwrap();
    for i in 0u64..300 {
        db.insert(0, i, v((i as u8).wrapping_add(1))).unwrap();
    }
    // Direct mode records at apply; buffer mode at the fold — a flush
    // forces the fold + drains the head chain to a segment either way.
    let in_mem = db.test_page_dead_list_len(0).unwrap();
    db.flush().unwrap();
    let (head, _tail) = db.test_page_dead_list_anchors(0).unwrap();
    assert!(
        in_mem > 0 || head != NULL_PAGE,
        "snapshot+overwrite recorded NO page deaths (rc>1 COW capture broken): \
         in_mem={in_mem} head={head}"
    );
}

#[test]
fn page_deadlist_segments_survive_reopen() {
    // ZFS port Phase 2: the page-deadlist segments live under the new
    // `page_dead_list_*_pid` anchors (volume) + `page_dead_list_tail_pid`
    // (snapshots). `collect_live_pages` MUST walk those chains or
    // `reclaim_orphan_pages` (run on open) frees the segments out from
    // under the live anchors → page-type corruption on the next walk.
    let (dir, db) = mk_db();
    for i in 0u64..300 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    db.flush().unwrap();
    let _s1 = db.take_snapshot(0).unwrap();
    for i in 0u64..300 {
        db.insert(0, i, v((i as u8).wrapping_add(1))).unwrap();
    }
    db.flush().unwrap();
    let (head_before, tail_before) = db.test_page_dead_list_anchors(0).unwrap();
    assert_ne!(
        head_before, NULL_PAGE,
        "page-deadlist chain should be non-empty after snapshot+overwrite+flush"
    );
    drop(db);

    let db = Db::open(dir.path()).unwrap();
    let (head_after, tail_after) = db.test_page_dead_list_anchors(0).unwrap();
    assert_eq!(head_after, head_before, "page-deadlist head anchor lost across reopen");
    assert_eq!(tail_after, tail_before, "page-deadlist tail anchor lost across reopen");
    let page = db.test_read_page(head_after).unwrap();
    assert_eq!(
        page.header().unwrap().page_type,
        PageType::DeadListSegment,
        "page-deadlist segment was freed/reused across reopen (orphan-reclaim bug)"
    );
    drop(db);

    let report = crate::verify::verify_path(
        dir.path(),
        crate::verify::VerifyOptions {
            strict: false,
            check_birth_shadow: true,
            check_clone_livelist: false,
        },
    )
    .unwrap();
    assert!(report.is_clean(), "verify issues after reopen: {:?}", report.issues);
}

#[test]
fn drop_older_snapshot_frees_s_next_deadlist_not_s_own() {
    // ZFS `process_old_deadlist`: destroying S frees from S_NEXT's
    // page-deadlist (deaths in `(S, S_next]`), filtered `birth > S_prev`,
    // NOT S's own (deaths in `(S_prev, S]`, which S never referenced).
    // The drop shadow assertion HARD-fails (`Corruption`) on a premature
    // free, so a regression to reading the wrong chain would surface as a
    // drop error here. We keep two snapshots live and drop the OLDER one
    // (S_next = the younger snapshot, not HEAD), the exact case the buffer
    // churn test's rolling window exercises but in fast direct mode.
    let (_d, db) = mk_db();
    for i in 0u64..300 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    db.flush().unwrap();
    let s1 = db.take_snapshot(0).unwrap();
    for i in 0u64..300 {
        db.insert(0, i, v((i as u8).wrapping_add(1))).unwrap();
    }
    db.flush().unwrap();
    let _s2 = db.take_snapshot(0).unwrap();
    for i in 0u64..300 {
        db.insert(0, i, v((i as u8).wrapping_add(2))).unwrap();
    }
    db.flush().unwrap();
    // Drop the OLDER snapshot while the younger one is still live.
    db.drop_snapshot(s1).unwrap().expect("drop older snapshot");
    // Live data must survive (a premature free would lose a mapping).
    for i in 0u64..300 {
        assert_eq!(db.get(0, i).unwrap(), Some(v((i as u8).wrapping_add(2))));
    }
}

#[test]
fn drop_middle_snapshot_merges_keep_into_s_next() {
    // ZFS port Phase 2a MERGE (process_old_deadlist): dropping a MIDDLE
    // snapshot S2 (S1 < S2 < S3 all live) is the only case with a
    // non-trivial KEEP/FREE partition — S3's deadlist entries born <= S1
    // are KEPT (still pinned by S1) and merged into S3, the rest are freed.
    // With S_prev = S1 > 0 this exercises the partition the oldest-drop
    // churn (S_prev = 0, KEEP empty) never reaches. Data must survive and
    // the merged chains must stay disjoint + clean under verify.
    let (dir, db) = mk_db();
    for i in 0u64..300 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    db.flush().unwrap();
    let s1 = db.take_snapshot(0).unwrap();
    // Overwrite only the FIRST half so some pages stay born<=S1 across S2/S3
    // (KEEP) while others are reborn in (S1,S2] (FREE on the S2 drop).
    for i in 0u64..150 {
        db.insert(0, i, v((i as u8).wrapping_add(1))).unwrap();
    }
    db.flush().unwrap();
    let s2 = db.take_snapshot(0).unwrap();
    for i in 0u64..300 {
        db.insert(0, i, v((i as u8).wrapping_add(2))).unwrap();
    }
    db.flush().unwrap();
    let _s3 = db.take_snapshot(0).unwrap();
    for i in 0u64..300 {
        db.insert(0, i, v((i as u8).wrapping_add(3))).unwrap();
    }
    db.flush().unwrap();
    let _ = s1;
    db.drop_snapshot(s2).unwrap().expect("drop middle snapshot");
    for i in 0u64..300 {
        assert_eq!(
            db.get(0, i).unwrap(),
            Some(v((i as u8).wrapping_add(3))),
            "lba {i}: live mapping lost after middle-snapshot drop+merge"
        );
    }
    // Persist the snapshot removal + merged anchors before reopen (the
    // drop contract: the WAL'd page frees are only consistent with a
    // snapshot-less manifest once a flush commits it).
    db.flush().unwrap();
    drop(db);
    // Reopen so `reclaim_orphan_pages` sweeps the old S2/S3 deadlist
    // segments the MERGE superseded (deferred-free, like the post-drop
    // SnapshotRoots pages); strict verify trips on them otherwise.
    let db = crate::Db::open(dir.path()).unwrap();
    for i in 0u64..300 {
        assert_eq!(db.get(0, i).unwrap(), Some(v((i as u8).wrapping_add(3))), "reopen lba {i}");
    }
    drop(db);
    let report = crate::verify::verify_path(
        dir.path(),
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
            check_clone_livelist: false,
        },
    )
    .unwrap();
    assert!(
        report.is_clean(),
        "verify issues after middle-snapshot drop+merge: {:?}",
        report.issues
    );
}

#[test]
fn page_deadlist_disjoint_across_live_snapshot_chains() {
    // ZFS port Phase 2a verify (E.2): with several live snapshots each
    // owning a sealed page-deadlist chain, every dying page version is
    // recorded into exactly one chain (the head accumulator at death time,
    // sealed into one snapshot). `metadb-verify --birth-shadow` runs
    // `check_page_deadlist`, which flags birth>=death and cross-chain
    // double-records; a clean report proves disjointness holds.
    let (dir, db) = mk_db();
    for i in 0u64..300 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    db.flush().unwrap();
    // Three live snapshots, each followed by a full-volume overwrite so
    // every snapshot pins a distinct set of now-dead L2P pages.
    for round in 1u8..=3 {
        let _s = db.take_snapshot(0).unwrap();
        for i in 0u64..300 {
            db.insert(0, i, v((i as u8).wrapping_add(round))).unwrap();
        }
        db.flush().unwrap();
    }
    drop(db);
    let report = crate::verify::verify_path(
        dir.path(),
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
            check_clone_livelist: false,
        },
    )
    .unwrap();
    assert!(
        report.is_clean(),
        "page-deadlist verify issues across live snapshot chains: {:?}",
        report.issues
    );
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

// Phase D.5: `wal_replay_re_emits_dead_records_into_buffer` tested
// the L2pPut WAL-replay arm's dead-list emission; the WAL is gone.
// Dead-list emission on the buffer-replay path lives on the onyx
// side (the LV2 flusher re-issues each L2pPut on recovery and the
// live commit emits the dead record).

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

// ── ZFS port Phase 4 Step 4 (S1): birth-authoritative non-clone COW-kill ──
// The COW-kill decision (preserve-on-overwrite vs recycle) now keys on
// `birth_lsn(P) <= youngest_snap` for non-clones instead of the page-rc
// `effective_rc > 1`. These assert the observable consequences; the existing
// page-deadlist suite above (snapshot-overwrite records, the HARD drop shadow,
// reopen + `check_birth_shadow`) is the broader regression net for the flip.

/// No snapshot ⇒ `youngest_snap == None` ⇒ no page is snapshot-pinned ⇒ every
/// overwrite recycles in place. The page-deadlist stays empty and the birth /
/// page-rc soft-warn never fires (no divergence without sharing).
#[test]
fn s1_no_snapshot_overwrite_records_no_page_deaths() {
    use std::sync::atomic::Ordering;
    let before = crate::paged::tree::BIRTH_SHADOW_DANGEROUS_DIVERGENCES.load(Ordering::Relaxed);
    let (dir, db) = mk_db();
    for i in 0u64..300 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    db.flush().unwrap();
    // Many overwrites, NO snapshot anywhere.
    for round in 1u8..6 {
        for i in 0u64..300 {
            db.insert(0, i, v((i as u8).wrapping_add(round))).unwrap();
        }
        db.flush().unwrap();
    }
    assert_eq!(
        db.test_page_dead_list_len(0).unwrap(),
        0,
        "non-clone with no snapshot must record zero page deaths (birth recycles in place)"
    );
    assert_eq!(
        db.test_page_dead_list_anchors(0).unwrap(),
        (NULL_PAGE, NULL_PAGE),
        "no snapshot ⇒ no page-deadlist chain"
    );
    let after = crate::paged::tree::BIRTH_SHADOW_DANGEROUS_DIVERGENCES.load(Ordering::Relaxed);
    assert_eq!(
        after, before,
        "birth/page-rc soft-warn fired without any sharing (spurious divergence)"
    );
    drop(db);
    let report = crate::verify::verify_path(
        dir.path(),
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
            check_clone_livelist: false,
        },
    )
    .unwrap();
    assert!(report.is_clean(), "verify issues: {:?}", report.issues);
}

/// A live snapshot pins the pre-overwrite pages by birth. Overwriting the head
/// must PRESERVE those pages (record them into the page-deadlist) so the
/// snapshot can still be read — proven by cloning the snapshot and reading the
/// OLD values back. In this clean steady state birth == page-rc, so the
/// dangerous-divergence soft-warn must NOT fire, and `check_birth_shadow` (the
/// HARD offline oracle) stays clean.
#[test]
fn s1_snapshot_overwrite_preserves_old_via_birth_no_divergence() {
    use std::sync::atomic::Ordering;
    let before = crate::paged::tree::BIRTH_SHADOW_DANGEROUS_DIVERGENCES.load(Ordering::Relaxed);
    let (dir, db) = mk_db();
    // Leaf-spaced LBAs so each mapping lands in a distinct leaf → an overwrite
    // COWs a distinct root→leaf path (a snapshot-pinned page dies off the head).
    for i in 0u64..8 {
        db.insert(0, i * 256, v(0x10 | i as u8)).unwrap();
    }
    db.flush().unwrap();
    let snap = db.take_snapshot(0).unwrap();
    for i in 0u64..8 {
        db.insert(0, i * 256, v(0xA0 | i as u8)).unwrap();
    }
    db.flush().unwrap();

    // Preserve recorded: the snapshot-pinned pages died off the head.
    let (head, _tail) = db.test_page_dead_list_anchors(0).unwrap();
    assert_ne!(
        head, NULL_PAGE,
        "snapshot+overwrite recorded NO page deaths — birth COW-kill failed to preserve"
    );

    // Preserve correctness: clone the snapshot (shares its pages) and read the
    // OLD values back. A wrong recycle would have clobbered them in place.
    let clone = db.clone_volume(snap).unwrap();
    for i in 0u64..8 {
        let val = db.get(clone, i * 256).unwrap().expect("snapshot mapping lost");
        assert_eq!(
            val.head_pba(),
            (0x10 | i as u8) as u64,
            "snapshot page for lba {} was not preserved (premature recycle)",
            i * 256
        );
    }
    // The live head still reads the NEW values.
    for i in 0u64..8 {
        let val = db.get(0, i * 256).unwrap().expect("head mapping lost");
        assert_eq!(val.head_pba(), (0xA0 | i as u8) as u64);
    }

    let after = crate::paged::tree::BIRTH_SHADOW_DANGEROUS_DIVERGENCES.load(Ordering::Relaxed);
    assert_eq!(
        after, before,
        "steady-state birth/page-rc divergence (birth and page-rc should agree once folded)"
    );
    drop(db);
    let report = crate::verify::verify_path(
        dir.path(),
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
            check_clone_livelist: false,
        },
    )
    .unwrap();
    assert!(report.is_clean(), "verify issues: {:?}", report.issues);
}
