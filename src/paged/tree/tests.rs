use super::*;
use crate::config::PAGE_SIZE;
use tempfile::TempDir;

fn mk_store() -> (TempDir, Arc<PageStore>) {
    let dir = TempDir::new().unwrap();
    let ps = Arc::new(PageStore::create(dir.path().join("p.onyx_meta")).unwrap());
    (dir, ps)
}

fn v(n: u8) -> L2pValue {
    L2pValue([n; 28])
}

#[test]
fn empty_tree_starts_as_leaf() {
    let (_d, ps) = mk_store();
    let t = PagedL2p::create(ps).unwrap();
    assert_eq!(t.root_level(), 0);
}

#[test]
fn insert_single_lba_stays_at_level_zero() {
    let (_d, ps) = mk_store();
    let mut t = PagedL2p::create(ps).unwrap();
    assert_eq!(t.insert(5, v(1)).unwrap(), None);
    assert_eq!(t.get(5).unwrap(), Some(v(1)));
    assert_eq!(t.root_level(), 0);
}

#[test]
fn insert_beyond_leaf_grows_to_index() {
    let (_d, ps) = mk_store();
    let mut t = PagedL2p::create(ps).unwrap();
    assert_eq!(t.insert(200, v(42)).unwrap(), None);
    // 200 >> 7 = 1, which exceeds level-0 capacity (max_leaf_idx=0).
    assert_eq!(t.root_level(), 1);
    assert_eq!(t.get(200).unwrap(), Some(v(42)));
    // Entries in the original leaf slot survive the promotion.
    t.insert(5, v(5)).unwrap();
    assert_eq!(t.get(5).unwrap(), Some(v(5)));
}

#[test]
fn insert_far_lba_grows_multiple_levels() {
    let (_d, ps) = mk_store();
    let mut t = PagedL2p::create(ps).unwrap();
    // leaf_idx needs ~17 bits → level 3 (covers 24 bits of leaf_idx).
    let far = 200_000u64 << LEAF_SHIFT;
    t.insert(far, v(9)).unwrap();
    assert!(t.root_level() >= 3);
    assert_eq!(t.get(far).unwrap(), Some(v(9)));
}

#[test]
fn overwrite_returns_previous_value() {
    let (_d, ps) = mk_store();
    let mut t = PagedL2p::create(ps).unwrap();
    assert_eq!(t.insert(17, v(1)).unwrap(), None);
    assert_eq!(t.insert(17, v(2)).unwrap(), Some(v(1)));
    assert_eq!(t.get(17).unwrap(), Some(v(2)));
}

#[test]
fn delete_missing_key_is_noop() {
    let (_d, ps) = mk_store();
    let mut t = PagedL2p::create(ps).unwrap();
    assert_eq!(t.delete(42).unwrap(), None);
    t.insert(10, v(1)).unwrap();
    // 10 is in a different leaf bit than 42 but same leaf pre-growth.
    assert_eq!(t.delete(42).unwrap(), None);
    assert_eq!(t.get(10).unwrap(), Some(v(1)));
}

#[test]
fn delete_returns_and_removes_value() {
    let (_d, ps) = mk_store();
    let mut t = PagedL2p::create(ps).unwrap();
    t.insert(3, v(7)).unwrap();
    t.insert(4, v(8)).unwrap();
    assert_eq!(t.delete(3).unwrap(), Some(v(7)));
    assert_eq!(t.get(3).unwrap(), None);
    assert_eq!(t.get(4).unwrap(), Some(v(8)));
}

#[test]
fn delete_empties_leaf_then_parent() {
    let (_d, ps) = mk_store();
    let mut t = PagedL2p::create(ps.clone()).unwrap();
    // Force level 2+: two distant LBAs.
    let a = 0u64;
    let b = 1_000_000u64;
    t.insert(a, v(1)).unwrap();
    t.insert(b, v(2)).unwrap();
    let lvl_before = t.root_level();
    assert!(lvl_before >= 2);

    t.flush().unwrap();
    let free_before = ps.free_list_len();
    // Deleting `b` should empty its sub-tree and free its leaf +
    // ancestors up to (but not including) the root.
    assert_eq!(t.delete(b).unwrap(), Some(v(2)));
    t.flush().unwrap();
    // PagedL2p::flush ran tree.checkpoint_committed which deferred the
    // physical free; drain reclaim before asserting.
    ps.try_reclaim().unwrap();
    // At least some pages should have been freed (leaf plus any
    // dedicated index pages on b's path).
    assert!(ps.free_list_len() > free_before);

    // `a` remains.
    assert_eq!(t.get(a).unwrap(), Some(v(1)));
    // `b` is gone.
    assert_eq!(t.get(b).unwrap(), None);
}

#[test]
fn flush_persists_state_across_reopen() {
    let (_d, ps) = mk_store();
    let root_pid;
    let next_gen;
    {
        let mut t = PagedL2p::create(ps.clone()).unwrap();
        t.insert(1, v(11)).unwrap();
        t.insert(500_000, v(22)).unwrap();
        t.flush().unwrap();
        root_pid = t.root();
        next_gen = 100;
    }
    let mut t = PagedL2p::open(ps, root_pid, next_gen).unwrap();
    assert_eq!(t.get(1).unwrap(), Some(v(11)));
    assert_eq!(t.get(500_000).unwrap(), Some(v(22)));
    assert_eq!(t.get(999).unwrap(), None);
}

#[test]
fn flush_and_reads_do_not_leave_private_clean_pages_resident() {
    let (_d, ps) = mk_store();
    let mut t = PagedL2p::create(ps).unwrap();
    for i in 0..1024u64 {
        t.insert(i * 1024, v((i % 255) as u8)).unwrap();
    }
    assert!(t.cached_pages_for_test() > 0);
    t.flush().unwrap();
    assert_eq!(t.cached_pages_for_test(), 0);

    for i in 0..256u64 {
        let _ = t.get(i * 1024).unwrap();
        assert_eq!(t.cached_pages_for_test(), 0);
    }
}

#[test]
fn snapshot_incref_preserves_old_view_under_writes() {
    let (_d, ps) = mk_store();
    let mut t = PagedL2p::create(ps).unwrap();
    t.insert(3, v(1)).unwrap();
    t.insert(300, v(2)).unwrap(); // forces level 1+
    t.flush().unwrap();

    // "Take a snapshot" — bump root rc and remember the root id.
    t.incref_root_for_snapshot().unwrap();
    let snap_root = t.root();

    // Mutate the live tree.
    t.insert(3, v(99)).unwrap();
    t.insert(300, v(88)).unwrap();
    assert_eq!(t.get(3).unwrap(), Some(v(99)));

    // The snapshot root still sees the old values.
    assert_eq!(t.get_at(snap_root, 3).unwrap(), Some(v(1)));
    assert_eq!(t.get_at(snap_root, 300).unwrap(), Some(v(2)));
}

#[test]
fn range_scan_returns_sorted_hits_within_bounds() {
    let (_d, ps) = mk_store();
    let mut t = PagedL2p::create(ps).unwrap();
    for k in [1u64, 5, 200, 500, 10_000, 1_000_000] {
        t.insert(k, v((k % 255) as u8)).unwrap();
    }
    let got: Vec<u64> = t.range(5..=500).unwrap().map(|r| r.unwrap().0).collect();
    assert_eq!(got, vec![5, 200, 500]);

    let got_all: Vec<u64> = t.range(..).unwrap().map(|r| r.unwrap().0).collect();
    assert_eq!(got_all, vec![1, 5, 200, 500, 10_000, 1_000_000]);

    let got_hi: Vec<u64> = t.range(600..).unwrap().map(|r| r.unwrap().0).collect();
    assert_eq!(got_hi, vec![10_000, 1_000_000]);
}

#[test]
fn growth_caps_at_max_level() {
    let (_d, ps) = mk_store();
    let mut t = PagedL2p::create(ps).unwrap();
    // The largest LBA we can insert sits at level 4. Just below the
    // overflow boundary: leaf_idx = 2^32 - 1 → lba = (2^32 - 1) << 7.
    let max_leaf_idx_at_level4: u64 = max_leaf_idx_at_level(4);
    let max_lba = (max_leaf_idx_at_level4 << LEAF_SHIFT) | LEAF_MASK;
    t.insert(max_lba, v(1)).unwrap();
    assert_eq!(t.root_level(), 4);
    assert_eq!(t.get(max_lba).unwrap(), Some(v(1)));

    // One past that (requires level 5) must refuse.
    let out_of_range = max_lba.wrapping_add(1);
    if out_of_range > max_lba {
        // Only test if it didn't wrap to 0.
        assert!(t.insert(out_of_range, v(2)).is_err());
    }
}

#[test]
fn many_inserts_deletes_preserve_invariants() {
    let (_d, ps) = mk_store();
    let mut t = PagedL2p::create(ps).unwrap();
    // Interleave hot leaves across levels.
    let keys: Vec<u64> = (0..512u64).map(|i| i * 2000).collect();
    for (i, k) in keys.iter().enumerate() {
        t.insert(*k, v(i as u8)).unwrap();
    }
    for (i, k) in keys.iter().enumerate() {
        assert_eq!(t.get(*k).unwrap(), Some(v(i as u8)), "key {k}");
    }
    for k in &keys {
        t.delete(*k).unwrap();
    }
    for k in &keys {
        assert_eq!(t.get(*k).unwrap(), None, "key {k} not deleted");
    }
}

#[test]
fn range_stream_matches_range() {
    let (_d, ps) = mk_store();
    let mut t = PagedL2p::create(ps).unwrap();
    for (i, k) in [3u64, 10, 57, 200, 2048, 50_000].iter().enumerate() {
        t.insert(*k, v(i as u8)).unwrap();
    }
    let via_range: Vec<_> = t.range(..).unwrap().collect::<Result<Vec<_>>>().unwrap();
    let via_stream: Vec<_> = t
        .range_stream(..)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(via_range, via_stream);
    // Non-empty sanity.
    assert_eq!(via_stream.len(), 6);
}

#[test]
fn attach_subtree_root_adopts_foreign_root_for_reads() {
    // Populate a source tree, snapshot its root (incref), then attach
    // that root into a freshly-created empty tree. Reads via the
    // attached tree must see the source's data.
    let (_d, ps) = mk_store();
    let mut src = PagedL2p::create_with_cache(
        ps.clone(),
        Arc::new(PageCache::new(ps.clone(), DEFAULT_PAGE_CACHE_BYTES)),
    )
    .unwrap();
    for (i, k) in [1u64, 200, 4096].iter().enumerate() {
        src.insert(*k, v((i + 1) as u8)).unwrap();
    }
    let src_root = src.root();
    let src_level = src.root_level();
    // Bump the root's on-disk refcount so the clone share survives.
    src.incref_root_for_snapshot().unwrap();
    src.flush().unwrap();

    let mut dst = PagedL2p::create_with_cache(
        ps.clone(),
        Arc::new(PageCache::new(ps.clone(), DEFAULT_PAGE_CACHE_BYTES)),
    )
    .unwrap();
    dst.attach_subtree_root(src_root, src_level).unwrap();
    assert_eq!(dst.root(), src_root);
    assert_eq!(dst.root_level(), src_level);
    assert_eq!(dst.get(1).unwrap(), Some(v(1)));
    assert_eq!(dst.get(200).unwrap(), Some(v(2)));
    assert_eq!(dst.get(4096).unwrap(), Some(v(3)));
    assert_eq!(dst.get(9999).unwrap(), None);
}

#[test]
fn attach_subtree_root_rejects_invalid_level() {
    let (_d, ps) = mk_store();
    let mut t = PagedL2p::create(ps).unwrap();
    let bad_level = MAX_INDEX_LEVEL + 1;
    assert!(matches!(
        t.attach_subtree_root(42, bad_level),
        Err(MetaDbError::InvalidArgument(_))
    ));
}

/// Regression for the sibling-concurrent cross-tree race:
/// `incref_root_for_snapshot` must go through the per-pid-locked
/// disk-direct RMW (not the legacy PageBuf dirty-flush path). Shape
/// mirrors `tests/repro_clone_verify.rs::
/// clone_volume_cross_write_keeps_refcounts_clean` — two trees
/// share a root page; a snapshot-incref on one must survive a
/// cow_for_write on the other without the two paths racing on
/// their local PageBuf rc views.
#[test]
fn incref_root_for_snapshot_uses_disk_direct_rc() {
    let (_d, ps) = mk_store();
    let page_cache = Arc::new(PageCache::new(ps.clone(), DEFAULT_PAGE_CACHE_BYTES));
    // Build the source tree and take a "snapshot" by bumping its
    // root rc. Fresh root page → disk rc starts at 1, post-incref
    // = 2.
    let mut src = PagedL2p::create_with_cache(ps.clone(), page_cache.clone()).unwrap();
    src.insert(1, v(10)).unwrap();
    src.insert(300, v(20)).unwrap();
    src.flush().unwrap();
    let root_pid = src.root();
    src.incref_root_for_snapshot().unwrap();

    // Re-read disk refcount directly (atomic_rc_delta(_, 0) would
    // work too but requires exposing arithmetic; read_page_unchecked
    // is the simplest inspector).
    let page = ps.read_page_unchecked(root_pid).unwrap();
    let header = page.header().unwrap();
    assert_eq!(header.refcount, 2, "snapshot incref must reach disk");

    // Dst attaches the same root page — simulating a post-
    // `clone_volume` sibling that shares the root.
    let mut dst = PagedL2p::create_with_cache(ps.clone(), page_cache.clone()).unwrap();
    dst.attach_subtree_root(root_pid, src.root_level()).unwrap();
    assert_eq!(dst.get(1).unwrap(), Some(v(10)));

    // A write on dst triggers cow_for_write(root, lsn) which
    // queues pending_rc[root] -= 1; commit routes through
    // atomic_rc_delta_with_gen. Disk rc must go 2 → 1 — not
    // 1 → 0 — proving the incref never raced with the cow.
    dst.insert_at_lsn(42, v(99), 100).unwrap();
    let page = ps.read_page_unchecked(root_pid).unwrap();
    let header = page.header().unwrap();
    assert_eq!(
        header.refcount, 1,
        "cross-tree cow must see disk-direct incref, not a stale PageBuf view"
    );

    // Snapshot root still readable; its stored mapping is preserved.
    assert_eq!(src.get_at(root_pid, 1).unwrap(), Some(v(10)));
    assert_eq!(src.get_at(root_pid, 300).unwrap(), Some(v(20)));
}

#[test]
fn same_lsn_parent_then_child_cow_does_not_skip_child_decref() {
    let (_d, ps) = mk_store();
    let page_cache = Arc::new(PageCache::new(ps.clone(), DEFAULT_PAGE_CACHE_BYTES));

    let mut src = PagedL2p::create_with_cache(ps.clone(), page_cache.clone()).unwrap();
    src.insert(1, v(1)).unwrap();
    src.insert(300, v(2)).unwrap();
    src.flush().unwrap();
    let shared_root = src.root();
    let root_level = src.root_level();
    assert_eq!(root_level, 1);

    let root_page = ps.read_page_unchecked(shared_root).unwrap();
    let child_pid = crate::paged::format::index_child_at(&root_page, 0);
    assert_ne!(child_pid, crate::types::NULL_PAGE);
    assert_eq!(ps.read_page_unchecked(child_pid).unwrap().refcount(), 1);

    src.incref_root_for_snapshot().unwrap();

    let mut writer = PagedL2p::create_with_cache(ps.clone(), page_cache.clone()).unwrap();
    writer.attach_subtree_root(shared_root, root_level).unwrap();

    writer
        .insert_at_lsn(LEAF_ENTRY_COUNT as u64, v(10), 100)
        .unwrap();
    assert_eq!(ps.read_page_unchecked(child_pid).unwrap().refcount(), 2);

    writer.insert_at_lsn(2, v(20), 100).unwrap();
    assert_eq!(
        ps.read_page_unchecked(child_pid).unwrap().refcount(),
        1,
        "same-LSN child COW must apply -1 after parent COW stamped the child"
    );
}

// -------- warmup_index_pages ---------------------------------------

fn mk_tree_with_pin(pin_pages: u64) -> (TempDir, PagedL2p) {
    let dir = TempDir::new().unwrap();
    let ps = Arc::new(PageStore::create(dir.path().join("p.onyx_meta")).unwrap());
    let cache = Arc::new(PageCache::new_with_pin_budget(
        ps.clone(),
        64 * PAGE_SIZE as u64,
        pin_pages * PAGE_SIZE as u64,
    ));
    let tree = PagedL2p::create_with_cache(ps, cache).unwrap();
    (dir, tree)
}

#[test]
fn warmup_on_empty_leaf_root_pins_nothing() {
    let (_d, mut t) = mk_tree_with_pin(16);
    let stats = t.warmup_index_pages().unwrap();
    assert_eq!(stats.pages_pinned, 0);
    assert!(!stats.skipped_budget);
    assert_eq!(t.buf.page_cache().pinned_pages(), 0);
}

#[test]
fn warmup_pins_all_index_pages_in_small_tree() {
    let (_d, mut t) = mk_tree_with_pin(64);
    // Force growth to level 2 by spreading inserts across >256 leaves.
    for i in 0..300u64 {
        t.insert(i << LEAF_SHIFT, v((i & 0xff) as u8)).unwrap();
    }
    assert!(t.root_level() >= 2);
    // Warmup reads through the shared page cache, which only sees
    // content that has been flushed to disk. Real callers (Db::open)
    // run warmup after WAL replay + flush, so this mirrors the
    // production sequence.
    t.flush().unwrap();
    let stats = t.warmup_index_pages().unwrap();
    assert!(!stats.skipped_budget);
    // At least the root must be pinned. An exact count depends on
    // how sparsely the radix spreads the 300 inserts across level-1
    // subtrees, so just assert non-zero and that cache agrees.
    assert!(stats.pages_pinned > 0);
    assert_eq!(t.buf.page_cache().pinned_pages(), stats.pages_pinned,);
}

#[test]
fn warmup_respects_pin_budget() {
    let (_d, mut t) = mk_tree_with_pin(1);
    // Level-2 tree with multiple level-1 index pages under the
    // root (> 256 leaves = 2+ level-1 pages + root).
    for i in 0..400u64 {
        t.insert(i << LEAF_SHIFT, v((i & 0xff) as u8)).unwrap();
    }
    assert!(t.root_level() >= 2);
    t.flush().unwrap();
    let stats = t.warmup_index_pages().unwrap();
    assert!(stats.skipped_budget);
    assert_eq!(t.buf.page_cache().pinned_pages(), 1);
}

#[test]
fn warmup_does_not_pin_leaves() {
    let (_d, mut t) = mk_tree_with_pin(64);
    // Single-leaf tree (root is a leaf). Warmup must not pin it.
    t.insert(3, v(3)).unwrap();
    assert_eq!(t.root_level(), 0);
    t.flush().unwrap();
    let stats = t.warmup_index_pages().unwrap();
    assert_eq!(stats.pages_pinned, 0);
    assert_eq!(t.buf.page_cache().pinned_pages(), 0);
}

#[test]
fn warmup_is_idempotent() {
    let (_d, mut t) = mk_tree_with_pin(64);
    for i in 0..300u64 {
        t.insert(i << LEAF_SHIFT, v((i & 0xff) as u8)).unwrap();
    }
    t.flush().unwrap();
    let a = t.warmup_index_pages().unwrap();
    let b = t.warmup_index_pages().unwrap();
    assert_eq!(a.pages_pinned, b.pages_pinned);
    assert_eq!(t.buf.page_cache().pinned_pages(), a.pages_pinned);
}
