use super::*;
use tempfile::TempDir;

fn mk_tree() -> (TempDir, BTree) {
    let dir = TempDir::new().unwrap();
    let ps = Arc::new(PageStore::create(dir.path().join("p.onyx_meta")).unwrap());
    let tree = BTree::create(ps).unwrap();
    (dir, tree)
}

fn rc(rc: u32) -> RcEntry {
    RcEntry { rc, birth_lsn: 1 }
}

#[test]
fn insert_then_get() {
    let (_d, mut t) = mk_tree();
    assert_eq!(t.insert(42, rc(100)).unwrap(), None);
    assert_eq!(t.get(42).unwrap(), Some(rc(100)));
}

#[test]
fn overwrite_returns_old_value() {
    let (_d, mut t) = mk_tree();
    t.insert(1, rc(10)).unwrap();
    assert_eq!(t.insert(1, rc(20)).unwrap(), Some(rc(10)));
    assert_eq!(t.get(1).unwrap(), Some(rc(20)));
}

#[test]
fn delete_returns_value_and_removes() {
    let (_d, mut t) = mk_tree();
    t.insert(7, rc(77)).unwrap();
    assert_eq!(t.delete(7).unwrap(), Some(rc(77)));
    assert_eq!(t.get(7).unwrap(), None);
    assert_eq!(t.delete(7).unwrap(), None);
}

#[test]
fn many_inserts_cause_splits() {
    let (_d, mut t) = mk_tree();
    // Enough to overflow one leaf (MAX_LEAF_ENTRIES = 201).
    for i in 0..1_000u64 {
        t.insert(i, rc(i as u32 + 1)).unwrap();
    }
    for i in 0..1_000u64 {
        assert_eq!(t.get(i).unwrap(), Some(rc(i as u32 + 1)));
    }
}

#[test]
fn many_deletes_cause_merges_without_losing_keys() {
    let (_d, mut t) = mk_tree();
    for i in 0..1_000u64 {
        t.insert(i, rc(1)).unwrap();
    }
    // Delete half.
    for i in 0..1_000u64 {
        if i % 2 == 0 {
            t.delete(i).unwrap();
        }
    }
    for i in 0..1_000u64 {
        let expect = if i % 2 == 0 { None } else { Some(rc(1)) };
        assert_eq!(t.get(i).unwrap(), expect, "key {i}");
    }
}

#[test]
fn range_returns_sorted_hits() {
    let (_d, mut t) = mk_tree();
    for i in 0..100u64 {
        t.insert(i, rc(i as u32)).unwrap();
    }
    let got: Vec<(u64, RcEntry)> = t.range(10..=20).unwrap().map(|r| r.unwrap()).collect();
    let expected: Vec<(u64, RcEntry)> = (10..=20).map(|i| (i, rc(i as u32))).collect();
    assert_eq!(got, expected);
}

#[test]
fn flush_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let ps = Arc::new(PageStore::create(dir.path().join("p.onyx_meta")).unwrap());
    let root;
    {
        let mut t = BTree::create(ps.clone()).unwrap();
        t.insert(100, rc(500)).unwrap();
        t.insert(200, rc(600)).unwrap();
        t.flush().unwrap();
        root = t.root();
    }
    let mut t = BTree::open(ps, root, 100).unwrap();
    assert_eq!(t.get(100).unwrap(), Some(rc(500)));
    assert_eq!(t.get(200).unwrap(), Some(rc(600)));
}

#[test]
fn flush_and_reads_do_not_leave_private_clean_pages_resident() {
    let (_d, mut t) = mk_tree();
    for i in 0..2048u64 {
        t.insert(i, rc(i as u32)).unwrap();
    }
    assert!(t.cached_pages_for_test() > 0);
    t.flush().unwrap();
    assert_eq!(t.cached_pages_for_test(), 0);

    for i in 0..256u64 {
        let _ = t.get(i).unwrap();
        assert_eq!(t.cached_pages_for_test(), 0);
    }
}

#[test]
fn iter_stream_visits_every_key_in_order() {
    let (_d, mut t) = mk_tree();
    let keys: Vec<u64> = (0..512u64).map(|i| i * 13).collect();
    for k in &keys {
        t.insert(*k, rc(*k as u32)).unwrap();
    }
    let got: Vec<(u64, RcEntry)> = t.iter_stream().unwrap().map(|r| r.unwrap()).collect();
    let mut expected: Vec<(u64, RcEntry)> = keys.iter().map(|k| (*k, rc(*k as u32))).collect();
    expected.sort_unstable_by_key(|(k, _)| *k);
    assert_eq!(got, expected);
}

#[test]
fn iter_stream_matches_full_range() {
    let (_d, mut t) = mk_tree();
    for i in 0..128u64 {
        t.insert(i * 7, rc(i as u32)).unwrap();
    }
    let via_range: Vec<_> = t.range(..).unwrap().map(|r| r.unwrap()).collect();
    let via_iter: Vec<_> = t.iter_stream().unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(via_range, via_iter);
}
