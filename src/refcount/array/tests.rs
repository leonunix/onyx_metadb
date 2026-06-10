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
