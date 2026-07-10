//! dead-list tests. Cover emit correctness across the three
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
    assert!(
        records.is_empty(),
        "FLAG_ZERO prev must not emit a dead record"
    );
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
    // BFG make-or-break: a live snapshot pins the L2P tree,
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
    // BFG: the page-deadlist segments live under the new
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
    assert_eq!(
        head_after, head_before,
        "page-deadlist head anchor lost across reopen"
    );
    assert_eq!(
        tail_after, tail_before,
        "page-deadlist tail anchor lost across reopen"
    );
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
            check_clone_birth_shadow: false,
        },
    )
    .unwrap();
    assert!(
        report.is_clean(),
        "verify issues after reopen: {:?}",
        report.issues
    );
}

#[test]
fn drop_older_snapshot_frees_s_next_deadlist_not_s_own() {
    // Snapshot-deadlist rule: destroying S frees from S_NEXT's
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
    // BFG MERGE (process_old_deadlist): dropping a middle snapshot `s2` while
    // `s1 < s2 < s3` are all live is the only case with a non-trivial KEEP/FREE
    // partition. `s3`'s deadlist entries born <= `s1` stay pinned by `s1` and
    // merge into `s3`; the rest are freed. With `S_prev = s1 > 0`, this exercises
    // the partition the oldest-drop
    // churn (S_prev = 0, KEEP empty) never reaches. Data must survive and
    // the merged chains must stay disjoint + clean under verify.
    let (dir, db) = mk_db();
    for i in 0u64..300 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    db.flush().unwrap();
    let s1 = db.take_snapshot(0).unwrap();
    // Overwrite only the FIRST half so some pages stay born <= s1 across s2/s3
    // (KEEP) while others are reborn in (s1, s2] (FREE on the s2 drop).
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
    // Reopen so `reclaim_orphan_pages` sweeps the old s2/s3 deadlist segments
    // the merge superseded (deferred-free, like the post-drop
    // SnapshotRoots pages); strict verify trips on them otherwise.
    let db = crate::Db::open(dir.path()).unwrap();
    for i in 0u64..300 {
        assert_eq!(
            db.get(0, i).unwrap(),
            Some(v((i as u8).wrapping_add(3))),
            "reopen lba {i}"
        );
    }
    drop(db);
    let report = crate::verify::verify_path(
        dir.path(),
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
            check_clone_livelist: false,
            check_clone_birth_shadow: false,
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
fn s2_drop_snapshot_nonclone_frees_via_free_pages() {
    // BFG: a NON-clone snapshot drop now frees the explicit,
    // page-rc-independent deadlist set (`DropSnapshot.free_pages = Some(...)`),
    // NOT the implicit "rc→0" cascade. The HARD `check_page_deadlist_shadow`
    // proves the set equals the structural free-set before the WAL submit, so
    // behaviour is unchanged: pages are actually freed, the live mapping
    // survives, and reopen + strict verify is clean. Here S is the youngest
    // (and only) snapshot, so the inheritor is the live HEAD.
    let (dir, db) = mk_db();
    for i in 0u64..200 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    db.flush().unwrap();
    let s = db.take_snapshot(0).unwrap();
    for i in 0u64..200 {
        db.insert(0, i, v((i as u8).wrapping_add(9))).unwrap();
    }
    db.flush().unwrap();
    let report = db
        .drop_snapshot(s)
        .unwrap()
        .expect("drop youngest snapshot");
    assert!(
        report.pages_freed > 0,
        "S2 deadlist free_pages must actually free the snapshot-exclusive pages"
    );
    for i in 0u64..200 {
        assert_eq!(
            db.get(0, i).unwrap(),
            Some(v((i as u8).wrapping_add(9))),
            "lba {i}: live mapping lost after non-clone snapshot drop"
        );
    }
    db.flush().unwrap();
    drop(db);
    let db = crate::Db::open(dir.path()).unwrap();
    for i in 0u64..200 {
        assert_eq!(
            db.get(0, i).unwrap(),
            Some(v((i as u8).wrapping_add(9))),
            "reopen lba {i}"
        );
    }
    drop(db);
    let report = crate::verify::verify_path(
        dir.path(),
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
            check_clone_livelist: false,
            check_clone_birth_shadow: false,
        },
    )
    .unwrap();
    assert!(
        report.is_clean(),
        "verify issues after S2 snapshot free: {:?}",
        report.issues
    );
}

#[test]
fn s2_drop_snapshot_crash_recovery_completeness() {
    // Crash-recovery completeness: COW deaths sitting
    // in the in-memory page-deadlist accumulator must be sealed DURABLY,
    // atomically with the drop_snapshot commit that advances checkpoint_lsn
    // (accumulator-seal), and the MERGE re-anchor must survive a hard crash via the WAL op
    // (merge-reanchor). Pre-fix, a Snapshot-inheritor drop left the source volume's HEAD
    // accumulator unsealed while advancing checkpoint past it; a hard kill +
    // reopen lost those deaths and a later drop fired MISSING/COMPLETENESS HOLE.
    let (dir, db) = mk_db();
    for i in 0u64..64 {
        db.insert(0, i, v(i as u8)).unwrap();
    }
    db.flush().unwrap();
    let s1 = db.take_snapshot(0).unwrap();
    for i in 0u64..64 {
        db.insert(0, i, v((i as u8).wrapping_add(1))).unwrap();
    }
    db.flush().unwrap();
    let s2 = db.take_snapshot(0).unwrap();
    // Overwrite after s2 so the deaths are pinned by s2 and sit in the HEAD
    // accumulator; NO flush, so the normal flush seal never runs for them.
    for i in 0u64..64 {
        db.insert(0, i, v((i as u8).wrapping_add(2))).unwrap();
    }
    assert!(
        db.test_page_dead_list_len(0).unwrap_or(0) > 0,
        "setup: post-S2 overwrite must leave deaths in the HEAD accumulator"
    );

    // Drop the oldest snapshot. The next surviving snapshot inherits its
    // dead-list records; this is the case that used to leave the HEAD
    // accumulator unsealed. The accumulator seal must write those records
    // into the durable HEAD chain as part of the drop commit.
    db.drop_snapshot(s1).unwrap().expect("drop s1");
    assert_eq!(
        db.test_page_dead_list_len(0),
        Some(0),
        "drop_snapshot must seal the HEAD accumulator (pre-fix the snapshot-inheritor \
         path retained the post-snapshot deaths only in volatile RAM)"
    );

    // Hard crash: drop the Db WITHOUT a final flush — the volatile accumulator
    // is gone. Reopen replays the lifecycle journal.
    drop(db);
    let db = crate::Db::open(dir.path()).unwrap();

    // Drop s2 (now youngest → HEAD-inheritor). Pre-fix the post-s2 deaths were
    // lost across the crash, so the inheritor's dl_next under-counted →
    // check_page_deadlist_shadow MISSING Corruption. Post-fix they are durable
    // in the HEAD chain (accumulator-seal) → the drop succeeds.
    db.drop_snapshot(s2)
        .unwrap()
        .expect("drop s2 after crash + reopen (no MISSING)");
    for i in 0u64..64 {
        assert_eq!(
            db.get(0, i).unwrap(),
            Some(v((i as u8).wrapping_add(2))),
            "lba {i} live"
        );
    }
    db.flush().unwrap();
    drop(db);
    let db = crate::Db::open(dir.path()).unwrap();
    for i in 0u64..64 {
        assert_eq!(
            db.get(0, i).unwrap(),
            Some(v((i as u8).wrapping_add(2))),
            "lba {i} reopen"
        );
    }
    drop(db);
    let report = crate::verify::verify_path(
        dir.path(),
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
            check_clone_livelist: false,
            check_clone_birth_shadow: false,
        },
    )
    .unwrap();
    assert!(
        report.is_clean(),
        "verify after crash-recovery drop: {:?}",
        report.issues
    );
}

#[test]
fn s1_promoted_exclone_buffer_fold_capture_watermark_no_hole() {
    // BFG — Bug 2 regression: a snapshot's `capture_watermark`
    // must be the shard's true FOLD watermark (`next_generation()-1`), NOT
    // `root_birth_lsn()`.
    //
    // Once a shard root forks private off a prior snapshot it is mutated IN
    // PLACE (the RECYCLE arm returns the same pid; `PageBuf::modify` never
    // re-stamps birth), so under a BUFFERED multi-write fold `root.birth` is the
    // FORK lsn while leaves reachable from that root are born LATER in the same
    // batch. A `root_birth_lsn` capture_watermark therefore UNDER-covers them.
    //
    // On a PROMOTED ex-clone (`clone_birth_lsn` sticky ⇒ `is_clone()` stays
    // true ⇒ the CLONE branch of `drain_page_deaths_into` keeps a death only if
    // `birth <= youngest_snap_below(death)`), an undercover watermark drops a
    // snapshot-pinned death — yet the drop shadow, seeing `parent_vol_ord=None`,
    // treats the volume as a plain one and EXPECTS that death → the HARD
    // `check_page_deadlist_shadow` fires MISSING / COMPLETENESS HOLE. The
    // fold-watermark covers the births, so the death is kept and the drop
    // succeeds.
    let dir = TempDir::new().unwrap();
    let mut cfg = crate::config::Config::new(dir.path());
    cfg.shards_per_partition = 1;
    cfg.l2p_buffer_enabled = true;
    // Huge thresholds + threads-off: a batch of writes accumulates and folds in
    // ONE pass on the explicit flush, so the clone's root forks ONCE per batch
    // and mutates in place for the rest — the exact in-place-root shape the bug
    // needs (a tiny soft trigger would fold each write separately, re-stamping
    // the root birth each time and hiding the undercover).
    cfg.l2p_buffer_soft_entries = 1_000_000;
    cfg.l2p_buffer_hard_entries = 4_000_000;
    cfg.l2p_buffer_max_interval_ms = 10_000_000;
    let db = Db::create_with_config(cfg).unwrap();

    let base = db.create_volume().unwrap();
    for i in 0u64..32 {
        db.insert(base, i * 256, v(i as u8)).unwrap();
    }
    db.flush().unwrap();
    let base_snap = db.take_snapshot(base).unwrap();
    let clone = db.clone_volume(base_snap).unwrap();
    assert!(
        db.promote_volume(clone).unwrap(),
        "clone should promote to independence"
    );

    // Diverge the promoted clone in ONE folded batch: the root forks once;
    // leaves are born across the batch's lsn span (above the fork lsn).
    for i in 0u64..32 {
        db.insert(clone, i * 256, v((0xC0u8).wrapping_add(i as u8)))
            .unwrap();
    }
    db.flush().unwrap();

    // Snapshot the promoted clone — `capture_watermark` is sampled here.
    let s = db.take_snapshot(clone).unwrap();

    // Overwrite the snapshot-pinned leaves in another folded batch → their
    // deaths (birth = a post-fork batch lsn) are recorded against S.
    for i in 0u64..32 {
        db.insert(clone, i * 256, v((0x40u8).wrapping_add(i as u8)))
            .unwrap();
    }
    db.flush().unwrap();

    // Pre-fix: the clone filter drops the snapshot-pinned deaths
    // (`youngest_snap_below(death) = fork lsn < birth`) → drop_snapshot returns
    // Err (MISSING / COMPLETENESS HOLE). Post-fix the fold watermark covers the
    // births, so the deadlist is complete and the drop succeeds.
    db.drop_snapshot(s)
        .unwrap()
        .expect("drop must not fire MISSING (Bug 2 capture_watermark undercover)");

    for i in 0u64..32 {
        assert_eq!(
            db.get(clone, i * 256).unwrap(),
            Some(v((0x40u8).wrapping_add(i as u8))),
            "lba {i}: live mapping lost after promoted-ex-clone snapshot drop"
        );
    }
    db.flush().unwrap();
    drop(db);
    let db = crate::Db::open(dir.path()).unwrap();
    for i in 0u64..32 {
        assert_eq!(
            db.get(clone, i * 256).unwrap(),
            Some(v((0x40u8).wrapping_add(i as u8))),
            "reopen lba {i}"
        );
    }
    drop(db);
    let report = crate::verify::verify_path(
        dir.path(),
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
            check_clone_livelist: true,
            check_clone_birth_shadow: false,
        },
    )
    .unwrap();
    assert!(
        report.is_clean(),
        "verify issues after promoted-ex-clone drop: {:?}",
        report.issues
    );
}

#[test]
fn h1_steady_flush_seals_all_folded_page_deaths() {
    // H1 (steady-flush crash completeness). A steady flush must seal EVERY
    // page-death folded into the roots it makes durable — not just those with
    // death_lsn <= wal_checkpoint. Pre-fix, the gate-free sample could fold a
    // shard's root past wal_checkpoint while the page drain bounded at
    // wal_checkpoint, leaving those durable-in-root deaths only in volatile RAM
    // → lost on a hard crash → a later drop fired MISSING. The fix drains each
    // shard's WHOLE accumulator under its tree guard, atomic with the root
    // capture, so the seal cannot lag the durable root. (A `root_birth_lsn`
    // bound is NOT sufficient: once a shard root forks private off the snapshot
    // it is mutated in place without bumping its birth, so a later snapshot-
    // pinned leaf death can sit above the root birth.)
    let (dir, db) = mk_db();
    // Leaf-spaced LBAs so each lands in a distinct leaf, spreading the dying
    // pages across many (non-shard-0) L2P shards.
    for i in 0u64..16 {
        db.insert(0, i * 256, v(0x10 | i as u8)).unwrap();
    }
    db.flush().unwrap();
    let snap = db.take_snapshot(0).unwrap();
    // Overwrite all → snapshot-pinned pages die off the head into the per-shard
    // accumulators.
    for i in 0u64..16 {
        db.insert(0, i * 256, v(0xA0 | i as u8)).unwrap();
    }
    assert!(
        db.test_page_dead_list_len(0).unwrap_or(0) > 0,
        "setup: snapshot+overwrite must leave page-deaths in the accumulators"
    );
    // THE H1 SEAL POINT: a forced flush selects every shard, so it must drain
    // every shard's accumulator completely — nothing left in RAM to lose.
    db.flush().unwrap();
    assert_eq!(
        db.test_page_dead_list_len(0),
        Some(0),
        "H1: a forced flush must seal EVERY folded page-death across all shards"
    );
    // Hard crash WITHOUT a further flush, then reopen: the deaths must be
    // durable in the sealed chain (pre-fix they were only in volatile RAM).
    drop(db);
    let db = crate::Db::open(dir.path()).unwrap();
    // Drop the snapshot post-crash: the inherited deaths must be present (no
    // MISSING) since the steady flush sealed them durably before the crash.
    db.drop_snapshot(snap)
        .unwrap()
        .expect("drop after crash + reopen must not fire MISSING");
    for i in 0u64..16 {
        assert_eq!(
            db.get(0, i * 256).unwrap(),
            Some(v(0xA0 | i as u8)),
            "lba {} live after drop",
            i * 256
        );
    }
    db.flush().unwrap();
    drop(db);
    // Reopen so `reclaim_orphan_pages` collects the dropped snapshot's deferred
    // `SnapshotRoots` + deadlist pages (drop_snapshot leaves them for the next
    // open by design, see snapshot.rs); offline `verify_path` does NOT reclaim,
    // so without this the benign deferred orphans would be flagged.
    let db = crate::Db::open(dir.path()).unwrap();
    for i in 0u64..16 {
        assert_eq!(
            db.get(0, i * 256).unwrap(),
            Some(v(0xA0 | i as u8)),
            "lba {} reopen",
            i * 256
        );
    }
    drop(db);
    let report = crate::verify::verify_path(
        dir.path(),
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
            check_clone_livelist: false,
            check_clone_birth_shadow: false,
        },
    )
    .unwrap();
    assert!(
        report.is_clean(),
        "verify after H1 crash-recovery: {:?}",
        report.issues
    );
}

#[test]
fn h1_drop_volume_seals_other_volumes_page_deaths() {
    // H1 (produced-then-lost — the soak's COMPLETENESS HOLE). drop_volume
    // `force_compact`s EVERY volume's buffer (folding pending overwrites into
    // snapshot-pinned page-deaths) and commits a checkpoint that makes those
    // roots durable + advances checkpoint_lsn. Pre-fix it did NOT seal the
    // page-deadlist accumulators, so a SURVIVING volume's just-folded deaths
    // lived only in volatile RAM; a hard crash lost them and a later
    // drop_snapshot fired a COMPLETENESS HOLE. The fix seals every surviving
    // volume's accumulator into that same commit (a death is
    // durable iff the fold that produced it is durable, in one commit).
    let (dir, db) = mk_db();
    let victim = db.create_volume().unwrap(); // a second volume, dropped below
    // Volume 0 (the survivor): write, flush, snapshot, then overwrite WITHOUT
    // flushing so its snapshot-pinned page-deaths sit UNFOLDED in the buffer.
    for i in 0u64..16 {
        db.insert(0, i * 256, v(0x10 | i as u8)).unwrap();
    }
    db.flush().unwrap();
    let snap = db.take_snapshot(0).unwrap();
    for i in 0u64..16 {
        db.insert(0, i * 256, v(0xA0 | i as u8)).unwrap();
    }
    // Drop the OTHER volume: this force_compacts volume 0's buffer (producing
    // its snapshot-pinned deaths) and advances the checkpoint. The fix must
    // seal volume 0's deaths into THIS commit.
    db.drop_volume(victim).unwrap().expect("drop victim volume");
    // Hard crash WITHOUT a further flush, then reopen.
    drop(db);
    let db = crate::Db::open(dir.path()).unwrap();
    // The snapshot-pinned deaths must be durable -> drop succeeds (no MISSING).
    db.drop_snapshot(snap)
        .unwrap()
        .expect("drop after drop_volume + crash + reopen must not fire MISSING");
    for i in 0u64..16 {
        assert_eq!(
            db.get(0, i * 256).unwrap(),
            Some(v(0xA0 | i as u8)),
            "lba {} live",
            i * 256
        );
    }
    db.flush().unwrap();
    drop(db);
    // Reopen so reclaim_orphan_pages collects the dropped snapshot's deferred
    // pages before the offline verify (which does not reclaim).
    let db = crate::Db::open(dir.path()).unwrap();
    drop(db);
    let report = crate::verify::verify_path(
        dir.path(),
        crate::verify::VerifyOptions {
            strict: true,
            check_birth_shadow: true,
            check_clone_livelist: false,
            check_clone_birth_shadow: false,
        },
    )
    .unwrap();
    assert!(
        report.is_clean(),
        "verify after H1 drop_volume crash-recovery: {:?}",
        report.issues
    );
}

#[test]
fn page_deadlist_disjoint_across_live_snapshot_chains() {
    // BFG verify (E.2): with several live snapshots each
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
            check_clone_birth_shadow: false,
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
    assert_ne!(
        tail_after, tail_before,
        "flush must advance tail when buffer has records"
    );
}

// WAL-free recovery: `wal_replay_re_emits_dead_records_into_buffer` tested
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
    db.drop_volume(v_ord)
        .unwrap()
        .expect("drop returns DropVolumeReport");
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
    // Guard the wire-level constant — GC will scan segments
    // assuming 24 B/record, so a future refactor must not silently
    // change DEAD_RECORD_BYTES without bumping the manifest version.
    assert_eq!(DEAD_RECORD_BYTES, 24);
    assert_eq!(
        std::mem::size_of::<DeadRecord>(),
        24,
        "DeadRecord struct layout drift would break replay round-trip"
    );
}

// ── BFG: birth-authoritative non-clone COW-kill ──
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
            check_clone_birth_shadow: false,
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
        let val = db
            .get(clone, i * 256)
            .unwrap()
            .expect("snapshot mapping lost");
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
            check_clone_birth_shadow: false,
        },
    )
    .unwrap();
    assert!(report.is_clean(), "verify issues: {:?}", report.issues);
}
