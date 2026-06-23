use super::op::{decode, encode};
use super::{LifecycleJournal, LifecycleOp};
use tempfile::TempDir;

fn sample_ops() -> Vec<LifecycleOp> {
    vec![
        LifecycleOp::CreateVolume {
            ord: 7,
            shard_count: 16,
        },
        LifecycleOp::TakeSnapshot {
            id: 100,
            vol_ord: 7,
            l2p_shard_roots: vec![10, 20, 30],
        },
        LifecycleOp::CloneVolume {
            src_ord: 7,
            new_ord: 8,
            src_snap_id: 100,
            src_shard_roots: vec![10, 20, 30],
        },
        LifecycleOp::PromotionChunk {
            vol_ord: 8,
            pba_increfs: vec![1000, 2000, 3000],
            next_cursor: Some(4096),
        },
        LifecycleOp::PromotionComplete { vol_ord: 8 },
        LifecycleOp::Discard {
            vol_ord: 8,
            start_lba: 512,
            count: 64,
        },
        // S2: cover both `free_pages` presence-byte arms across the two ops
        // (Some = the flip's frozen authoritative set; None = legacy page-rc
        // cascade). Kept at exactly two ops so the replay-seq tests that
        // hardcode `sample_ops().len()` stay valid.
        LifecycleOp::DropSnapshot {
            id: 100,
            pages: vec![10, 11, 12, 13],
            pba_decrefs: vec![1000, 2000],
            free_pages: Some(vec![11, 13]),
            // Snapshot-inheritor merge re-anchor (crash-recovery completeness).
            merge: Some((crate::lifecycle_log::DropMergeTarget::Snapshot { id: 99 }, 4242)),
        },
        LifecycleOp::DropVolume {
            ord: 8,
            pages: vec![40, 41, 42],
            free_pages: None,
        },
    ]
}

#[test]
fn encode_decode_roundtrip_every_variant() {
    for op in sample_ops() {
        let bytes = encode(&op);
        let decoded = decode(&bytes).expect("decode");
        assert_eq!(decoded, op, "roundtrip differs for {op:?}");
    }
}

#[test]
fn decode_rejects_unknown_tag() {
    let err = decode(&[0xff]).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("unknown tag"), "got: {msg}");
}

#[test]
fn decode_rejects_truncated_body() {
    // CreateVolume needs 2 (ord) + 4 (shard_count) bytes after the tag.
    let err = decode(&[super::op::TAG_CREATE_VOLUME, 0x07, 0x00, 0x10]).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("short read"), "got: {msg}");
}

#[test]
fn decode_rejects_trailing_bytes() {
    let mut bytes = encode(&LifecycleOp::PromotionComplete { vol_ord: 5 });
    bytes.push(0xaa);
    let err = decode(&bytes).unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("trailing bytes"), "got: {msg}");
}

#[test]
fn append_assigns_monotonic_seqs() {
    let dir = TempDir::new().unwrap();
    let mut j = LifecycleJournal::open(dir.path(), 1, 1 << 20).unwrap();
    let s1 = j.append(&encode(&sample_ops()[0])).unwrap();
    let s2 = j.append(&encode(&sample_ops()[1])).unwrap();
    let s3 = j.append(&encode(&sample_ops()[2])).unwrap();
    assert_eq!(s1, 1);
    assert_eq!(s2, 2);
    assert_eq!(s3, 3);
    assert_eq!(j.next_seq(), 4);
}

#[test]
fn replay_returns_every_record_in_seq_order() {
    let dir = TempDir::new().unwrap();
    let ops = sample_ops();
    let mut j = LifecycleJournal::open(dir.path(), 1, 1 << 20).unwrap();
    for op in &ops {
        j.append(&encode(op)).unwrap();
    }
    drop(j);

    let mut seen: Vec<(u64, LifecycleOp)> = Vec::new();
    let last_seq = LifecycleJournal::replay(dir.path(), 0, |rec| {
        let op = decode(&rec.body).unwrap();
        seen.push((rec.seq, op));
        Ok(())
    })
    .unwrap();

    assert_eq!(seen.len(), ops.len());
    assert_eq!(last_seq, ops.len() as u64);
    for (i, (seq, op)) in seen.iter().enumerate() {
        assert_eq!(*seq, (i + 1) as u64);
        assert_eq!(op, &ops[i]);
    }
}

#[test]
fn replay_skips_records_at_or_below_from_seq() {
    let dir = TempDir::new().unwrap();
    let ops = sample_ops();
    let mut j = LifecycleJournal::open(dir.path(), 1, 1 << 20).unwrap();
    for op in &ops {
        j.append(&encode(op)).unwrap();
    }
    drop(j);

    // The manifest's lifecycle_replay_seq says we've already covered
    // records 1..=3; replay should hand back only 4 onward.
    let mut seen_seqs = Vec::new();
    LifecycleJournal::replay(dir.path(), 3, |rec| {
        seen_seqs.push(rec.seq);
        Ok(())
    })
    .unwrap();
    assert_eq!(seen_seqs, vec![4, 5, 6, 7, 8]);
}

#[test]
fn replay_truncates_torn_tail_in_last_segment() {
    let dir = TempDir::new().unwrap();
    let ops = sample_ops();
    let mut j = LifecycleJournal::open(dir.path(), 1, 1 << 20).unwrap();
    for op in ops.iter().take(3) {
        j.append(&encode(op)).unwrap();
    }
    drop(j);

    // Find the single segment file and corrupt the last record by
    // appending half a frame header — simulates an interrupted append.
    let mut paths: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    paths.sort();
    let path = &paths[0];
    let valid_len = std::fs::metadata(path).unwrap().len();
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
        f.write_all(&[0u8; 8]).unwrap(); // less than WAL_HEADER_SIZE
        f.sync_all().unwrap();
    }

    let mut seen = 0usize;
    LifecycleJournal::replay(dir.path(), 0, |_| {
        seen += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(seen, 3, "all three pre-tear records replayed");
    let after = std::fs::metadata(path).unwrap().len();
    assert_eq!(after, valid_len, "torn tail truncated back to last good frame");
}

#[test]
fn prune_drops_segments_wholly_below_checkpoint() {
    let dir = TempDir::new().unwrap();
    // Two segments by rotating on a tight byte budget.
    let mut j = LifecycleJournal::open(dir.path(), 1, 64).unwrap();
    j.append(&encode(&sample_ops()[0])).unwrap();
    j.append(&encode(&sample_ops()[1])).unwrap();
    j.append(&encode(&sample_ops()[2])).unwrap();
    drop(j);

    let before: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert!(before.len() >= 2, "expected segment rotation, saw {before:?}");

    // checkpoint covers seq 1 → first segment may go (next segment
    // starts past replay_start). The newest segment is always kept.
    LifecycleJournal::prune(dir.path(), 1).unwrap();
    let after: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert!(after.len() < before.len());
    assert!(!after.is_empty(), "newest segment must survive prune");
}

#[test]
fn frame_shares_layout() {
    // Belt-and-suspenders: confirm the journal's append actually
    // produces the [`super::record`] framing the iterator expects.
    let body = encode(&LifecycleOp::PromotionComplete { vol_ord: 3 });
    let framed = super::journal::frame_one(42, &body);
    let iter = crate::lifecycle_log::record::WalRecordIter::new(&framed);
    let recs: Vec<_> = iter.collect();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0].lsn, 42);
    assert_eq!(recs[0].body, body.as_slice());
}
