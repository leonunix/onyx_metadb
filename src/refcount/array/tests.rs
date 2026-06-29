use super::*;
use tempfile::TempDir;

fn make_array() -> (TempDir, PagedRefcountArray) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages");
    let page_store = Arc::new(PageStore::create(&path).unwrap());
    let page_cache = Arc::new(PageCache::new(page_store.clone(), 64 * 1024 * 1024));
    let array = PagedRefcountArray::create(page_store, page_cache).unwrap();
    (dir, array)
}

fn pending(delta: i64, lsn: Lsn) -> Pending {
    Pending {
        delta,
        last_lsn: lsn,
    }
}

#[test]
fn empty_array_returns_zero_for_any_pba() {
    let (_dir, a) = make_array();
    assert_eq!(a.get(0).unwrap(), RcEntry::ZERO);
    assert_eq!(a.get(99_999).unwrap(), RcEntry::ZERO);
    assert_eq!(a.allocated_data_pages(), 0);
}

#[test]
fn apply_deltas_persists_one_entry() {
    let (_dir, a) = make_array();
    a.apply_deltas(vec![(7, pending(1, 100))]).unwrap();
    let e = a.get(7).unwrap();
    assert_eq!(
        e,
        RcEntry {
            rc: 1,
            birth_lsn: 100
        }
    );
    assert_eq!(a.allocated_data_pages(), 1);
    assert_eq!(a.page_lsn(7).unwrap(), 100);
}

#[test]
fn apply_deltas_groups_same_page_into_one_io() {
    let (_dir, a) = make_array();
    // All 3 PBAs land in page_idx=0
    a.apply_deltas(vec![
        (1, pending(1, 100)),
        (2, pending(2, 101)),
        (3, pending(5, 102)),
    ])
    .unwrap();
    assert_eq!(a.get(1).unwrap().rc, 1);
    assert_eq!(a.get(2).unwrap().rc, 2);
    assert_eq!(a.get(3).unwrap().rc, 5);
    assert_eq!(a.allocated_data_pages(), 1);
}

#[test]
fn apply_deltas_spans_multiple_pages() {
    let (_dir, a) = make_array();
    let pba_p0 = 5;
    let pba_p1 = (ENTRIES_PER_PAGE + 7) as Pba;
    let pba_p3 = (ENTRIES_PER_PAGE * 3 + 1) as Pba;
    a.apply_deltas(vec![
        (pba_p0, pending(1, 100)),
        (pba_p1, pending(1, 101)),
        (pba_p3, pending(1, 102)),
    ])
    .unwrap();
    assert_eq!(a.get(pba_p0).unwrap().rc, 1);
    assert_eq!(a.get(pba_p1).unwrap().rc, 1);
    assert_eq!(a.get(pba_p3).unwrap().rc, 1);
    // page_idx 2 is a hole; page_table grows to 4 but only 3 data pages are allocated.
    assert_eq!(a.allocated_data_pages(), 3);
}

#[test]
fn round_trip_via_open() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages");
    let meta_page_id;
    {
        let page_store = Arc::new(PageStore::create(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let a = PagedRefcountArray::create(page_store.clone(), page_cache).unwrap();
        meta_page_id = a.meta_page_id();
        a.apply_deltas(vec![
            (10, pending(3, 100)),
            ((ENTRIES_PER_PAGE * 2 + 5) as Pba, pending(7, 200)),
        ])
        .unwrap();
        a.flush_meta().unwrap();
    }
    // Reopen
    let page_store = Arc::new(PageStore::open(&path).unwrap());
    let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
    let a = PagedRefcountArray::open(page_store, page_cache, meta_page_id).unwrap();
    assert_eq!(
        a.get(10).unwrap(),
        RcEntry {
            rc: 3,
            birth_lsn: 100
        }
    );
    assert_eq!(
        a.get((ENTRIES_PER_PAGE * 2 + 5) as Pba).unwrap(),
        RcEntry {
            rc: 7,
            birth_lsn: 200
        }
    );
    assert_eq!(a.get(0).unwrap(), RcEntry::ZERO);
    assert_eq!(a.allocated_data_pages(), 2);
}

#[test]
fn iter_live_skips_zero_slots() {
    let (_dir, a) = make_array();
    a.apply_deltas(vec![
        (1, pending(5, 1)),
        (3, pending(0, 0)), // no-op, stays zero
        (7, pending(9, 2)),
    ])
    .unwrap();
    let live = a.iter_live().unwrap();
    assert_eq!(live.len(), 2);
    assert_eq!(
        live[0],
        (
            1,
            RcEntry {
                rc: 5,
                birth_lsn: 1
            }
        )
    );
    assert_eq!(
        live[1],
        (
            7,
            RcEntry {
                rc: 9,
                birth_lsn: 2
            }
        )
    );
}

#[test]
fn page_idx_beyond_one_meta_page_chains_a_continuation() {
    // Forcing a page_idx past the head meta capacity must extend
    // the chain instead of failing.
    let (_dir, a) = make_array();
    let head_cap = paged_meta::head_capacity(0);
    let big_pba = ((head_cap + 1) * ENTRIES_PER_PAGE) as Pba;
    a.apply_deltas(vec![(big_pba, pending(1, 1))]).unwrap();
    a.flush_meta().unwrap();
    // Chain should now have at least 2 meta pages.
    assert!(a.inner.lock().meta_chain.len() >= 2);
    assert_eq!(a.get(big_pba).unwrap().rc, 1);
}

#[test]
fn reapply_idempotency_via_page_lsn_skip_is_callers_job() {
    // The array itself does not skip — replay-skip is enforced
    // by the caller (RcShard::stage / commit apply path) reading
    // page_lsn() and comparing against op LSN. Here we just
    // confirm the page_lsn machinery works.
    let (_dir, a) = make_array();
    a.apply_deltas(vec![(0, pending(1, 100))]).unwrap();
    assert_eq!(a.page_lsn(0).unwrap(), 100);
    a.apply_deltas(vec![(0, pending(1, 200))]).unwrap();
    assert_eq!(a.page_lsn(0).unwrap(), 200);
    assert_eq!(a.get(0).unwrap().rc, 2);
}

// ---- dirty-staged overlay (the 2026-06-11 rc_authoritative case) ----
//
// `stage_deltas_in_memory` publishes the pid in `page_table` and seeds
// the shared LRU, but the disk write happens later (the flush's
// `write_sealed_page_runs` batch). These tests simulate the window's
// LRU eviction with `page_cache.invalidate` and assert reads are served
// from the staged overlay instead of falling through to disk (which,
// for a fresh page, holds unwritten zeros -> PageMagicMismatch).

fn make_array_with_cache() -> (TempDir, Arc<PageCache>, PagedRefcountArray) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages");
    let page_store = Arc::new(PageStore::create(&path).unwrap());
    let page_cache = Arc::new(PageCache::new(page_store.clone(), 64 * 1024 * 1024));
    let array = PagedRefcountArray::create(page_store, page_cache.clone()).unwrap();
    (dir, page_cache, array)
}

#[test]
fn staged_fresh_page_survives_cache_eviction() {
    let (_dir, cache, a) = make_array_with_cache();
    let staged = a
        .stage_deltas_in_memory(vec![(7, pending(1, 100))], false)
        .unwrap();
    assert_eq!(staged.pages.len(), 1);
    assert!(staged.pages[0].is_fresh);
    // Simulate LRU eviction before the page bytes are durable.
    cache.invalidate(staged.pages[0].page_id);
    // Pre-overlay this read fell through to disk and failed with
    // PageMagicMismatch(0x0): the fresh pid's backing was never written.
    let e = a.get(7).unwrap();
    assert_eq!(e.rc, 1);
    assert_eq!(e.birth_lsn, 100);
    assert_eq!(a.page_lsn(7).unwrap(), 100);
}

#[test]
fn staged_existing_page_eviction_reads_post_fold_not_disk_stale() {
    let (_dir, cache, a) = make_array_with_cache();
    // Durable base: rc=1 on disk.
    a.apply_deltas(vec![(7, pending(1, 100))]).unwrap();
    // Second fold staged but NOT yet written.
    let staged = a
        .stage_deltas_in_memory(vec![(7, pending(1, 200))], false)
        .unwrap();
    assert!(!staged.pages[0].is_fresh);
    cache.invalidate(staged.pages[0].page_id);
    // Pre-overlay this read returned the PRE-fold disk content (rc=1):
    // a silent under-count once the caller's delta slot was cleared.
    assert_eq!(a.get(7).unwrap().rc, 2);
    assert_eq!(a.page_lsn(7).unwrap(), 200);
}

#[test]
fn clear_staged_after_write_falls_back_to_disk_truth() {
    let (_dir, cache, a) = make_array_with_cache();
    let staged = a
        .stage_deltas_in_memory(vec![(7, pending(3, 100))], false)
        .unwrap();
    a.write_staged_pages(&staged).unwrap(); // clears the overlay
    cache.invalidate(staged.pages[0].page_id);
    // Overlay is gone; this read must come from disk and still be the
    // staged content (the write made it durable).
    assert_eq!(a.get(7).unwrap().rc, 3);
    assert!(a.inner.lock().staged_overlay.is_empty());
}

#[test]
fn clear_staged_is_ptr_eq_gated_against_newer_restage() {
    let (_dir, _cache, a) = make_array_with_cache();
    let first = a
        .stage_deltas_in_memory(vec![(7, pending(1, 100))], false)
        .unwrap();
    // Re-stage the same page (newer fold) before the first clear runs.
    let second = a
        .stage_deltas_in_memory(vec![(8, pending(1, 200))], false)
        .unwrap();
    assert_eq!(first.pages[0].page_id, second.pages[0].page_id);
    // A stale clear for the FIRST fold must not drop the SECOND fold's
    // overlay entry.
    a.clear_staged(&first);
    assert!(!a.inner.lock().staged_overlay.is_empty());
    a.clear_staged(&second);
    assert!(a.inner.lock().staged_overlay.is_empty());
}

#[test]
fn abort_removes_overlay_before_freeing_fresh_pid() {
    let (_dir, cache, a) = make_array_with_cache();
    let staged = a
        .stage_deltas_in_memory(vec![(7, pending(1, 100))], false)
        .unwrap();
    let pid = staged.pages[0].page_id;
    a.abort_staged_deltas(&staged, 100);
    assert!(a.inner.lock().staged_overlay.is_empty());
    // page_table entry reset: reads resolve to ZERO, no overlay shadow.
    assert_eq!(a.get(7).unwrap(), RcEntry::ZERO);
    cache.invalidate(pid);
    assert_eq!(a.get(7).unwrap(), RcEntry::ZERO);
}

#[test]
fn restage_after_eviction_uses_staged_base_not_disk() {
    let (_dir, cache, a) = make_array_with_cache();
    // Fold 1 staged (slot 7), never written, then evicted from the LRU.
    let first = a
        .stage_deltas_in_memory(vec![(7, pending(1, 100))], false)
        .unwrap();
    cache.invalidate(first.pages[0].page_id);
    // Fold 2 on the same page (slot 8) must base itself on fold 1's
    // staged content (overlay), not the unwritten disk page.
    let second = a
        .stage_deltas_in_memory(vec![(8, pending(1, 200))], false)
        .unwrap();
    assert_eq!(first.pages[0].page_id, second.pages[0].page_id);
    assert_eq!(a.get(7).unwrap().rc, 1);
    assert_eq!(a.get(8).unwrap().rc, 1);
    // iter_live also serves from the overlay.
    let live = a.iter_live().unwrap();
    assert_eq!(live.len(), 2);
}
