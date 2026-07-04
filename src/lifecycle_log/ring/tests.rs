use super::*;
use crate::lifecycle_log::LifecycleOp;

fn dev(blocks: u64) -> Arc<dyn JournalDevice> {
    Arc::new(MemJournalDevice::new(blocks))
}

fn collect(device: &Arc<dyn JournalDevice>, head: u64, from_seq: u64) -> Vec<(u64, Vec<u8>)> {
    let mut out = Vec::new();
    RingJournal::replay(device, head, from_seq, |rec| {
        out.push((rec.seq, rec.body));
        Ok(())
    })
    .unwrap();
    out
}

#[test]
fn append_then_replay_round_trip() {
    let device = dev(16);
    let mut j = RingJournal::open(device.clone(), 0, 1).unwrap();
    assert_eq!(j.append(b"alpha").unwrap(), 1);
    assert_eq!(j.append(b"beta").unwrap(), 2);
    assert_eq!(j.append(b"gamma").unwrap(), 3);

    let got = collect(&device, 0, 0);
    assert_eq!(
        got,
        vec![
            (1, b"alpha".to_vec()),
            (2, b"beta".to_vec()),
            (3, b"gamma".to_vec()),
        ]
    );
}

#[test]
fn replay_skips_records_at_or_below_from_seq() {
    let device = dev(16);
    let mut j = RingJournal::open(device.clone(), 0, 1).unwrap();
    for i in 0..5 {
        j.append(format!("rec{i}").as_bytes()).unwrap();
    }
    // from_seq = 3 → only seqs 4,5 replay.
    let got = collect(&device, 0, 3);
    assert_eq!(got, vec![(4, b"rec3".to_vec()), (5, b"rec4".to_vec())]);
}

#[test]
fn empty_ring_replays_nothing() {
    let device = dev(8);
    assert!(collect(&device, 0, 0).is_empty());
    // Open on a pristine ring: next_seq honoured, no phantom records.
    let j = RingJournal::open(device, 0, 1).unwrap();
    assert_eq!(j.next_seq(), 1);
    assert_eq!(j.used_blocks(), 0);
}

#[test]
fn large_body_spans_multiple_blocks() {
    let device = dev(64);
    let mut j = RingJournal::open(device.clone(), 0, 1).unwrap();
    // ~10 KiB body → 3 blocks (16B header + 10240B = 10256B → 3 blocks).
    let big = vec![0xABu8; 10 * 1024];
    j.append(b"small").unwrap();
    let seq = j.append(&big).unwrap();
    j.append(b"after").unwrap();
    assert_eq!(seq, 2);
    assert!(j.used_blocks() >= 5); // 1 + 3 + 1

    let got = collect(&device, 0, 0);
    assert_eq!(got.len(), 3);
    assert_eq!(got[1].0, 2);
    assert_eq!(got[1].1, big);
    assert_eq!(got[2], (3, b"after".to_vec()));
}

#[test]
fn reopen_rediscovers_tail_and_continues() {
    let device = dev(16);
    {
        let mut j = RingJournal::open(device.clone(), 0, 1).unwrap();
        j.append(b"one").unwrap();
        j.append(b"two").unwrap();
    }
    // Reopen: next_seq passed by caller must land after the scanned max.
    let mut j = RingJournal::open(device.clone(), 0, 3).unwrap();
    assert_eq!(j.next_seq(), 3);
    assert_eq!(j.used_blocks(), 2);
    assert_eq!(j.append(b"three").unwrap(), 3);
    let got = collect(&device, 0, 0);
    assert_eq!(got.len(), 3);
    assert_eq!(got[2], (3, b"three".to_vec()));
}

#[test]
fn open_recovers_next_seq_when_hint_is_stale() {
    // If the caller's next_seq hint is below what's physically on the ring
    // (should not happen, but be defensive), open bumps to scanned_max + 1.
    let device = dev(16);
    {
        let mut j = RingJournal::open(device.clone(), 0, 1).unwrap();
        for _ in 0..4 {
            j.append(b"x").unwrap();
        }
    }
    let j = RingJournal::open(device, 0, 1).unwrap();
    assert_eq!(j.next_seq(), 5);
}

#[test]
fn torn_tail_stops_replay_cleanly() {
    let device = dev(16);
    {
        let mut j = RingJournal::open(device.clone(), 0, 1).unwrap();
        j.append(b"good1").unwrap();
        j.append(b"good2").unwrap();
    }
    // Corrupt the third block (would-be next record's header block) with
    // garbage that is neither zero nor a valid contiguous frame.
    let mut garbage = vec![0u8; BLOCK_SIZE];
    garbage[0..8].copy_from_slice(&99u64.to_le_bytes()); // seq 99, discontiguous
    garbage[8..12].copy_from_slice(&4u32.to_le_bytes());
    garbage[12..16].copy_from_slice(&0xdead_beefu32.to_le_bytes());
    device.write_block(2, &garbage).unwrap();

    // Replay stops at the discontiguity: only the two good records survive.
    let got = collect(&device, 0, 0);
    assert_eq!(got, vec![(1, b"good1".to_vec()), (2, b"good2".to_vec())]);
    // Open rediscovers the tail right after the two good records.
    let j = RingJournal::open(device, 0, 3).unwrap();
    assert_eq!(j.used_blocks(), 2);
    assert_eq!(j.next_seq(), 3);
}

#[test]
fn ring_full_returns_out_of_space() {
    // 4 blocks, each record 1 block; 4 fit, 5th overflows.
    let device = dev(4);
    let mut j = RingJournal::open(device, 0, 1).unwrap();
    for _ in 0..4 {
        j.append(b"z").unwrap();
    }
    assert_eq!(j.used_blocks(), 4);
    match j.append(b"overflow") {
        Err(MetaDbError::OutOfSpace) => {}
        other => panic!("expected OutOfSpace when ring full, got {other:?}"),
    }
}

#[test]
fn prune_frees_blocks_and_advances_head() {
    let device = dev(8);
    let mut j = RingJournal::open(device.clone(), 0, 1).unwrap();
    for i in 0..5 {
        j.append(format!("r{i}").as_bytes()).unwrap(); // seqs 1..=5
    }
    assert_eq!(j.used_blocks(), 5);
    // Prune everything through seq 3 → head advances past 3 records.
    let new_head = j.prune(3).unwrap();
    assert_eq!(new_head, 3);
    assert_eq!(j.ring_head(), 3);
    assert_eq!(j.used_blocks(), 2);
    // Replay from the new head sees only seqs 4,5.
    let got = collect(&device, new_head, 0);
    assert_eq!(got, vec![(4, b"r3".to_vec()), (5, b"r4".to_vec())]);
}

#[test]
fn wrap_around_reuses_pruned_blocks() {
    // 4 blocks. Fill, prune 2, then append 2 more that must wrap into the
    // freed head blocks. Replay from the (advanced) head must see them in order.
    let device = dev(4);
    let mut j = RingJournal::open(device.clone(), 0, 1).unwrap();
    j.append(b"a").unwrap(); // seq1 @ blk0
    j.append(b"b").unwrap(); // seq2 @ blk1
    j.append(b"c").unwrap(); // seq3 @ blk2
    j.append(b"d").unwrap(); // seq4 @ blk3
    let head = j.prune(2).unwrap(); // free blk0,blk1; head=2
    assert_eq!(head, 2);
    assert_eq!(j.used_blocks(), 2);
    // Now append two more: seq5 wraps to blk0, seq6 to blk1.
    assert_eq!(j.append(b"e").unwrap(), 5);
    assert_eq!(j.append(b"f").unwrap(), 6);
    assert_eq!(j.used_blocks(), 4);

    // Replay from the current head (2) must yield seqs 3..=6 in order,
    // straddling the ring wrap.
    let got = collect(&device, j.ring_head(), 0);
    assert_eq!(
        got,
        vec![
            (3, b"c".to_vec()),
            (4, b"d".to_vec()),
            (5, b"e".to_vec()),
            (6, b"f".to_vec()),
        ]
    );
    // And a fresh open rediscovers the wrapped tail correctly.
    let j2 = RingJournal::open(device, j.ring_head(), 7).unwrap();
    assert_eq!(j2.used_blocks(), 4);
    assert_eq!(j2.next_seq(), 7);
}

#[test]
fn frame_is_wire_compatible_with_file_journal() {
    // The ring writes the same seq|len|crc|body frame the file journal + WAL
    // use, so a body round-trips byte-identically.
    let device = dev(8);
    let mut j = RingJournal::open(device.clone(), 0, 1).unwrap();
    let body = crate::lifecycle_log::op::encode(&LifecycleOp::PromotionComplete { vol_ord: 7 });
    j.append(&body).unwrap();
    let got = collect(&device, 0, 0);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].1, body);
    let decoded = crate::lifecycle_log::op::decode(&got[0].1).unwrap();
    assert!(matches!(
        decoded,
        LifecycleOp::PromotionComplete { vol_ord: 7 }
    ));
}
