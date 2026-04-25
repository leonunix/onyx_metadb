use super::*;
use crate::lsm::format::DedupValue;
use tempfile::TempDir;

fn mk_lsm() -> (TempDir, Arc<PageStore>, Lsm) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = Arc::new(PageStore::create(&path).unwrap());
    let lsm = Lsm::create(ps.clone(), LsmConfig::default());
    (dir, ps, lsm)
}

fn h(n: u64) -> Hash32 {
    let mut out = [0u8; 32];
    out[..8].copy_from_slice(&n.to_be_bytes());
    out
}

fn v(n: u8) -> DedupValue {
    let mut x = [0u8; 28];
    x[0] = n;
    DedupValue(x)
}

#[test]
fn empty_lsm_misses() {
    let (_d, _ps, lsm) = mk_lsm();
    assert_eq!(lsm.get(&h(0)).unwrap(), None);
}

#[test]
fn memtable_put_then_get() {
    let (_d, _ps, lsm) = mk_lsm();
    lsm.put(h(1), v(10));
    lsm.put(h(2), v(20));
    assert_eq!(lsm.get(&h(1)).unwrap(), Some(v(10)));
    assert_eq!(lsm.get(&h(2)).unwrap(), Some(v(20)));
    assert_eq!(lsm.get(&h(3)).unwrap(), None);
}

#[test]
fn tombstone_masks_older_put() {
    let (_d, _ps, lsm) = mk_lsm();
    lsm.put(h(1), v(1));
    lsm.flush_memtable(1).unwrap().unwrap(); // L0 has put
    lsm.delete(h(1)); // tombstone in memtable
    assert_eq!(lsm.get(&h(1)).unwrap(), None);
    lsm.flush_memtable(2).unwrap().unwrap(); // L0 has [put, tombstone]
    assert_eq!(lsm.get(&h(1)).unwrap(), None);
}

#[test]
fn flush_creates_l0_sst_and_clears_active() {
    let (_d, _ps, lsm) = mk_lsm();
    for i in 0..100u64 {
        lsm.put(h(i), v(i as u8));
    }
    let handle = lsm.flush_memtable(1).unwrap().unwrap();
    assert_eq!(handle.record_count, 100);
    let levels = lsm.levels_snapshot();
    assert_eq!(levels.len(), 1);
    assert_eq!(levels[0].len(), 1);
    assert_eq!(lsm.stats().memtable.active_entries, 0);
    assert!(!lsm.memtable.has_frozen());

    // Get should now hit the SST, not the memtable.
    for i in 0..100u64 {
        assert_eq!(lsm.get(&h(i)).unwrap(), Some(v(i as u8)));
    }
}

#[test]
fn newer_l0_sst_shadows_older() {
    let (_d, _ps, lsm) = mk_lsm();
    lsm.put(h(1), v(1));
    lsm.flush_memtable(1).unwrap().unwrap();
    lsm.put(h(1), v(2));
    lsm.flush_memtable(2).unwrap().unwrap();
    // L0 now has two SSTs; newest (v(2)) wins.
    assert_eq!(lsm.get(&h(1)).unwrap(), Some(v(2)));
}

#[test]
fn flush_with_empty_memtable_returns_none() {
    let (_d, _ps, lsm) = mk_lsm();
    assert!(lsm.flush_memtable(1).unwrap().is_none());
}

#[test]
fn flush_memtable_with_only_tombstones() {
    let (_d, _ps, lsm) = mk_lsm();
    lsm.delete(h(1));
    lsm.delete(h(2));
    let h_result = lsm.flush_memtable(1).unwrap().unwrap();
    assert_eq!(h_result.record_count, 2);
    assert_eq!(lsm.get(&h(1)).unwrap(), None);
}

#[test]
fn persist_and_reopen_round_trip() {
    let (dir, ps, lsm) = mk_lsm();
    for i in 0..50u64 {
        lsm.put(h(i), v(i as u8));
    }
    lsm.flush_memtable(1).unwrap();
    for i in 50..100u64 {
        lsm.put(h(i), v(i as u8));
    }
    lsm.flush_memtable(2).unwrap();
    let heads = lsm.persist_levels(3).unwrap();
    // Drop the in-memory LSM; the SST pages remain on disk.
    drop(lsm);
    let ps2 = ps.clone();
    let lsm2 = Lsm::open(ps2, LsmConfig::default(), &heads).unwrap();
    let levels = lsm2.levels_snapshot();
    assert_eq!(levels.len(), 1);
    assert_eq!(levels[0].len(), 2);
    for i in 0..100u64 {
        assert_eq!(
            lsm2.get(&h(i)).unwrap(),
            Some(v(i as u8)),
            "mismatch at {i}"
        );
    }
    drop(dir);
}

#[test]
fn put_after_flush_auto_resumes_frozen_if_needed() {
    // Ensure: if flush freezes but we put first, the new put goes
    // into a fresh active, and then flush sees frozen pre-existing
    // and still writes it correctly.
    let (_d, _ps, lsm) = mk_lsm();
    for i in 0..5u64 {
        lsm.put(h(i), v(1));
    }
    // Manually freeze to simulate mid-flight state.
    let frozen = lsm.memtable.freeze().unwrap();
    // Flusher would have consumed `frozen`, but assume it crashed.
    assert_eq!(frozen.len(), 5);
    // Meanwhile writes continue…
    for i in 5..10u64 {
        lsm.put(h(i), v(2));
    }
    // Retry flush: should consume the existing frozen, NOT re-freeze.
    let handle = lsm.flush_memtable(1).unwrap().unwrap();
    assert_eq!(handle.record_count, 5);
    // The 5 new writes are still in the active memtable.
    assert_eq!(lsm.stats().memtable.active_entries, 5);
    for i in 0..5u64 {
        assert_eq!(lsm.get(&h(i)).unwrap(), Some(v(1)));
    }
    for i in 5..10u64 {
        assert_eq!(lsm.get(&h(i)).unwrap(), Some(v(2)));
    }
}

#[test]
fn l1_disjoint_lookup() {
    let (_d, _ps, lsm) = mk_lsm();
    // Bypass compaction: inject a synthetic L1.
    lsm.put(h(1), v(1));
    let h1 = lsm.flush_memtable(1).unwrap().unwrap();
    lsm.put(h(200), v(2));
    let h2 = lsm.flush_memtable(2).unwrap().unwrap();
    // Move both L0 SSTs into L1 (non-overlapping since they're
    // single-key SSTs at distinct hashes).
    lsm.debug_replace_levels(vec![Vec::new(), vec![h1, h2]]);
    assert_eq!(lsm.get(&h(1)).unwrap(), Some(v(1)));
    assert_eq!(lsm.get(&h(200)).unwrap(), Some(v(2)));
    assert_eq!(lsm.get(&h(50)).unwrap(), None);
}

fn small_cfg() -> LsmConfig {
    LsmConfig {
        memtable_bytes: 1024,
        bits_per_entry: 10,
        l0_sst_count_trigger: 3,
        target_sst_records: 50,
        level_ratio: 10,
    }
}

fn mk_lsm_small() -> (TempDir, Arc<PageStore>, Lsm) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = Arc::new(PageStore::create(&path).unwrap());
    let lsm = Lsm::create(ps.clone(), small_cfg());
    (dir, ps, lsm)
}

#[test]
fn compact_noop_when_nothing_to_do() {
    let (_d, _ps, lsm) = mk_lsm();
    assert!(lsm.compact_once(1).unwrap().is_none());
}

#[test]
fn compact_l0_to_l1_merges_disjoint_ssts() {
    let (_d, _ps, lsm) = mk_lsm_small();
    for round in 0..3u64 {
        for i in 0..20u64 {
            let key = round * 100 + i;
            lsm.put(h(key), v(key as u8));
        }
        lsm.flush_memtable(round as Lsn + 1).unwrap().unwrap();
    }
    // Under trigger (3 SSTs in L0).
    assert_eq!(lsm.levels_snapshot()[0].len(), 3);
    let report = lsm.compact_once(10).unwrap().unwrap();
    assert_eq!(report.from_level, 0);
    assert_eq!(report.to_level, 1);
    assert_eq!(report.victims, 3); // 3 L0 + 0 L1
    let levels = lsm.levels_snapshot();
    assert!(levels[0].is_empty());
    assert!(!levels[1].is_empty());
    // Every written key still reads correctly.
    for round in 0..3u64 {
        for i in 0..20u64 {
            let key = round * 100 + i;
            assert_eq!(lsm.get(&h(key)).unwrap(), Some(v(key as u8)));
        }
    }
}

#[test]
fn compact_l0_to_l1_merges_overlapping_l1() {
    let (_d, _ps, lsm) = mk_lsm_small();
    // First, populate L1 by flushing + compacting a round that
    // targets hashes 0..50.
    for i in 0..50u64 {
        lsm.put(h(i), v(1));
    }
    lsm.flush_memtable(1).unwrap().unwrap();
    for i in 0..50u64 {
        lsm.put(h(i), v(1));
    }
    lsm.flush_memtable(2).unwrap().unwrap();
    for i in 0..50u64 {
        lsm.put(h(i), v(1));
    }
    lsm.flush_memtable(3).unwrap().unwrap();
    lsm.compact_once(10).unwrap().unwrap(); // pushes to L1
    assert_eq!(lsm.levels_snapshot()[0].len(), 0);
    assert!(!lsm.levels_snapshot()[1].is_empty());

    // Now overwrite the same keys so L0 overlaps L1.
    for i in 0..50u64 {
        lsm.put(h(i), v(2));
    }
    lsm.flush_memtable(11).unwrap().unwrap();
    for i in 0..50u64 {
        lsm.put(h(i), v(2));
    }
    lsm.flush_memtable(12).unwrap().unwrap();
    for i in 0..50u64 {
        lsm.put(h(i), v(2));
    }
    lsm.flush_memtable(13).unwrap().unwrap();
    // Fire compaction; L0 should absorb L1 with newer puts.
    let report = lsm.compact_once(20).unwrap().unwrap();
    assert!(report.victims >= 4, "expected ≥4 victims, got {report:?}");
    for i in 0..50u64 {
        assert_eq!(lsm.get(&h(i)).unwrap(), Some(v(2)));
    }
}

#[test]
fn compact_drops_tombstones_at_deepest_level() {
    let (_d, _ps, lsm) = mk_lsm_small();
    // Put + tombstone for hash 1 across L0.
    for _ in 0..3 {
        lsm.put(h(1), v(1));
        lsm.flush_memtable(1).unwrap().unwrap();
    }
    lsm.delete(h(1));
    lsm.flush_memtable(2).unwrap().unwrap();
    // Four L0 SSTs; fire compaction — target L1 is deepest so
    // tombstones get dropped entirely.
    lsm.compact_once(10).unwrap().unwrap();
    let levels = lsm.levels_snapshot();
    let l1 = &levels[1];
    assert_eq!(
        l1.iter().map(|h| h.record_count).sum::<u64>(),
        0,
        "tombstone should have dropped to zero records in L1"
    );
    assert_eq!(lsm.get(&h(1)).unwrap(), None);
}

#[test]
fn compact_preserves_tombstones_when_deeper_level_exists() {
    let (_d, _ps, lsm) = mk_lsm_small();
    // Seed L2 with put(h1) = v(1) by forcing multi-level.
    // Step 1: put + flush + compact to L1.
    for _ in 0..3 {
        lsm.put(h(1), v(1));
        lsm.flush_memtable(1).unwrap().unwrap();
    }
    lsm.compact_once(10).unwrap().unwrap(); // L1 now has put(h1,v1)
    // Step 2: fake-promote L1 content to L2 so L1 is empty and L2
    // holds put(h1,v1). We do this by injecting a synthetic level.
    let levels_now = lsm.levels_snapshot();
    assert_eq!(levels_now.len(), 2);
    let l1_content = levels_now[1].clone();
    lsm.debug_replace_levels(vec![Vec::new(), Vec::new(), l1_content]);
    // Step 3: new L0 with tombstone(h1) and pressure to push to L1.
    for _ in 0..3 {
        lsm.delete(h(1));
        lsm.flush_memtable(11).unwrap().unwrap();
    }
    lsm.compact_once(20).unwrap().unwrap(); // L0 → L1
    // Since L2 exists (deeper), tombstones should survive in L1.
    let levels = lsm.levels_snapshot();
    let l1_total: u64 = levels[1].iter().map(|h| h.record_count).sum();
    assert!(
        l1_total > 0,
        "tombstone must survive into L1 when L2 exists"
    );
    assert_eq!(lsm.get(&h(1)).unwrap(), None); // tombstone shadows L2
}

#[test]
fn repeated_compaction_loop_converges() {
    let (_d, _ps, lsm) = mk_lsm_small();
    // Write a lot of data and keep flushing + compacting.
    for batch in 0..30u64 {
        for i in 0..25u64 {
            lsm.put(h(batch * 100 + i), v((batch + 1) as u8));
        }
        lsm.flush_memtable(batch as Lsn + 1).unwrap();
        while lsm
            .compact_once((batch as Lsn + 1) * 100)
            .unwrap()
            .is_some()
        {}
    }
    // Every key should still be present with its original batch
    // value. Check a subset.
    for batch in 0..30u64 {
        let key = batch * 100 + 13;
        assert_eq!(lsm.get(&h(key)).unwrap(), Some(v((batch + 1) as u8)));
    }
    // Levels should have stabilised (L0 below trigger).
    assert!(lsm.levels_snapshot()[0].len() < small_cfg().l0_sst_count_trigger);
}

// -------- batch read API --------------------------------------------

#[test]
fn multi_get_matches_single_gets_mixed_layers() {
    let (_d, _ps, lsm) = mk_lsm();
    // First batch flushes to L0; second batch stays in the memtable.
    for i in 0u64..32 {
        lsm.put(h(i), v(i as u8));
    }
    lsm.flush_memtable(1).unwrap();
    for i in 32u64..64 {
        lsm.put(h(i), v(i as u8));
    }
    // Tombstone one of each layer's keys.
    lsm.delete(h(5));
    lsm.delete(h(40));

    let hashes = vec![h(0), h(5), h(31), h(32), h(40), h(63), h(999), h(0)];
    let got = lsm.multi_get(&hashes).unwrap();
    assert_eq!(got.len(), hashes.len());
    for (i, hash) in hashes.iter().enumerate() {
        assert_eq!(got[i], lsm.get(hash).unwrap(), "hash {i} mismatch");
    }
}

#[test]
fn multi_get_empty_input_returns_empty() {
    let (_d, _ps, lsm) = mk_lsm();
    lsm.put(h(1), v(1));
    assert!(lsm.multi_get(&[]).unwrap().is_empty());
    assert!(lsm.multi_scan_prefix(&[]).unwrap().is_empty());
}

#[test]
fn multi_scan_prefix_matches_single_prefix_scans() {
    let (_d, _ps, lsm) = mk_lsm();
    // Group A (prefix 0x00..00..01) — 3 entries, partially flushed.
    let prefix_a = [0u8; 1];
    let key_a = |n: u64| {
        let mut h = [0u8; 32];
        h[0] = prefix_a[0];
        h[1..9].copy_from_slice(&n.to_be_bytes());
        h
    };
    lsm.put(key_a(1), v(1));
    lsm.put(key_a(2), v(2));
    lsm.flush_memtable(1).unwrap();
    lsm.put(key_a(3), v(3));

    // Group B (prefix 0x42) — 2 entries, entirely in memtable.
    let key_b = |n: u64| {
        let mut h = [0u8; 32];
        h[0] = 0x42;
        h[1..9].copy_from_slice(&n.to_be_bytes());
        h
    };
    lsm.put(key_b(10), v(10));
    lsm.put(key_b(20), v(20));

    // Group C (prefix 0xFF) — empty.
    let prefix_c = [0xFFu8; 1];

    let prefix_b = [0x42u8; 1];
    let prefixes: Vec<&[u8]> = vec![&prefix_a, &prefix_b, &prefix_c];
    let batched = lsm.multi_scan_prefix(&prefixes).unwrap();
    assert_eq!(batched.len(), 3);
    for (i, prefix) in prefixes.iter().enumerate() {
        let mut expected = lsm.scan_prefix(prefix).unwrap();
        let mut got = batched[i].clone();
        expected.sort_by_key(|(k, _)| *k);
        got.sort_by_key(|(k, _)| *k);
        assert_eq!(got, expected, "prefix {i} mismatch");
    }
}

#[test]
fn scan_prefix_explicitly_resolves_shadowing_and_tombstones() {
    let (_d, _ps, lsm) = mk_lsm();

    fn prefixed(prefix: u8, n: u64) -> Hash32 {
        let mut h = [0u8; 32];
        h[0] = prefix;
        h[1..9].copy_from_slice(&n.to_be_bytes());
        h
    }

    let prefix_a = [0x11u8; 1];
    let prefix_b = [0x22u8; 1];

    let a1 = prefixed(prefix_a[0], 1);
    let a2 = prefixed(prefix_a[0], 2);
    let a3 = prefixed(prefix_a[0], 3);
    let a4 = prefixed(prefix_a[0], 4);
    let a5 = prefixed(prefix_a[0], 5);
    let b1 = prefixed(prefix_b[0], 1);

    // Oldest layer.
    lsm.put(a1, v(1));
    lsm.put(a2, v(2));
    lsm.put(a3, v(3));
    lsm.put(b1, v(9));
    lsm.flush_memtable(1).unwrap();

    // Newer flushed layer.
    lsm.put(a1, v(11)); // overwrite older put
    lsm.delete(a2); // tombstone older put
    lsm.put(a4, v(4)); // brand-new key
    lsm.flush_memtable(2).unwrap();

    // Newest memtable layer.
    lsm.delete(a3); // tombstone only lives in memtable
    lsm.put(a5, v(5));

    let mut got_a = lsm.scan_prefix(&prefix_a).unwrap();
    got_a.sort_by_key(|(k, _)| *k);
    let mut expected_a = vec![(a1, v(11)), (a4, v(4)), (a5, v(5))];
    expected_a.sort_by_key(|(k, _)| *k);
    assert_eq!(got_a, expected_a);

    let mut got_b = lsm.scan_prefix(&prefix_b).unwrap();
    got_b.sort_by_key(|(k, _)| *k);
    assert_eq!(got_b, vec![(b1, v(9))]);

    let mut batched = lsm
        .multi_scan_prefix(&[&prefix_a[..], &prefix_b[..]])
        .unwrap();
    batched[0].sort_by_key(|(k, _)| *k);
    batched[1].sort_by_key(|(k, _)| *k);
    assert_eq!(batched[0], expected_a);
    assert_eq!(batched[1], vec![(b1, v(9))]);
}
