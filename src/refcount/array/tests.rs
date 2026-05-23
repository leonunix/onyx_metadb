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

/// Regression for the 2026-05-07 drainer-mode lost-incref P0
/// (`nvme-box:.dev/fio-dedupe-compress-soak/20260507T-bug-repro2/`).
///
/// `build_overlay_pages` is called by the drainer per cycle. Within
/// one tx at lsn=N, `apply_refcount_bucket_to_tree` calls
/// `rc.stage` per pba group, releasing `delta_active.lock` between
/// calls. The drainer's transition-1 swap can fire between two
/// stages of the same tx, splitting that tx's contributions across
/// two drainer cycles:
///
///   cycle K   : applies slot X at lsn=N → page_gen=N
///   cycle K+1 : has slot Y still pending at lsn=N (same page)
///
/// With the previous `page_generation >= pending.last_lsn` check,
/// cycle K+1 silently dropped slot Y's pending because its
/// `page_generation` (the prior cycle K's output, gen=N) was equal
/// to the pending lsn — a same-tx split caused a permanent
/// undercounting of refcounts. The fix is `>` not `>=`. This test
/// emulates the split by invoking `build_overlay_pages` twice in
/// succession with two pendings at the same lsn for two different
/// slots in the same page_idx, feeding the first cycle's output
/// as the second cycle's `prior_overlay`.
#[test]
fn build_overlay_pages_does_not_drop_same_lsn_pending_split_across_cycles() {
    use crate::refcount::overlay::{OverlayEntry, PagePool};
    use std::collections::HashMap;
    let (_dir, a) = make_array();
    let metrics = Arc::new(crate::metrics::MetaMetrics::default());
    let mut pool = PagePool::new(a.page_store.clone(), 4, metrics);
    // Cycle 1: stage slot 0 (pba=0) at lsn=N. Empty prior.
    let prior: HashMap<usize, OverlayEntry> = HashMap::new();
    let lsn_n: Lsn = 11_239;
    let entries1 = a
        .build_overlay_pages(vec![(0u64, pending(7, lsn_n))], &mut pool, &prior)
        .unwrap();
    assert_eq!(entries1.len(), 1);
    let cycle1_entry = entries1.into_iter().next().unwrap();
    assert_eq!(cycle1_entry.page_idx, 0);
    let cycle1_gen = cycle1_entry.sealed.header().unwrap().generation;
    assert_eq!(cycle1_gen, lsn_n);
    // Slot 0 has rc=7 from the +7 incref.
    assert_eq!(read_entry(&cycle1_entry.sealed, 0).rc, 7);
    // Slot 1 stayed at zero (cycle 1 didn't touch it).
    assert_eq!(read_entry(&cycle1_entry.sealed, 1).rc, 0);
    // Cycle 2: stage slot 1 (pba=1) at the SAME lsn=N. Prior is
    // cycle 1's entry — its generation already equals lsn=N.
    // With the `>=` bug this pending would be silently dropped.
    let mut prior2: HashMap<usize, OverlayEntry> = HashMap::new();
    prior2.insert(0, cycle1_entry);
    let entries2 = a
        .build_overlay_pages(vec![(1u64, pending(8, lsn_n))], &mut pool, &prior2)
        .unwrap();
    assert_eq!(entries2.len(), 1);
    let cycle2_entry = entries2.into_iter().next().unwrap();
    // Slot 1 must reflect the +8 incref.
    assert_eq!(
        read_entry(&cycle2_entry.sealed, 1).rc,
        8,
        "same-lsn pending split across cycles must apply, not skip"
    );
    // Slot 0 must preserve cycle 1's contribution.
    assert_eq!(read_entry(&cycle2_entry.sealed, 0).rc, 7);
    // Page generation stays at lsn_n (max of cycle 1 and cycle 2).
    assert_eq!(cycle2_entry.sealed.header().unwrap().generation, lsn_n);
}
