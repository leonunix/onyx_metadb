use super::*;
use tempfile::TempDir;

fn make_index(bucket_count: u64) -> (TempDir, CuckooHash) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages");
    let page_store = Arc::new(PageStore::create(&path).unwrap());
    let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
    let cuckoo = CuckooHash::create(page_store, page_cache, bucket_count, 0xDEAD, 0xBEEF).unwrap();
    (dir, cuckoo)
}

fn h(byte: u8) -> Hash8 {
    let mut x = [0u8; 8];
    x.fill(byte);
    x
}

fn dv(byte: u8) -> DedupValue {
    let mut x = [0u8; 28];
    x[0] = byte;
    DedupValue(x)
}

#[test]
fn empty_get_returns_none() {
    let (_d, c) = make_index(64);
    assert_eq!(c.get(&h(0xAA)).unwrap(), None);
}

#[test]
fn put_get_round_trip() {
    let (_d, c) = make_index(64);
    c.put(h(0xAA), dv(7), 100).unwrap();
    assert_eq!(c.get(&h(0xAA)).unwrap(), Some(dv(7)));
    assert_eq!(c.get(&h(0xBB)).unwrap(), None);
}

#[test]
fn put_overwrites_same_key() {
    let (_d, c) = make_index(64);
    c.put(h(0xAA), dv(7), 100).unwrap();
    c.put(h(0xAA), dv(9), 101).unwrap();
    assert_eq!(c.get(&h(0xAA)).unwrap(), Some(dv(9)));
    assert_eq!(c.approx_len(), 1);
}

#[test]
fn delete_clears_entry() {
    let (_d, c) = make_index(64);
    c.put(h(0xAA), dv(7), 100).unwrap();
    c.delete(&h(0xAA), 101).unwrap();
    assert_eq!(c.get(&h(0xAA)).unwrap(), None);
}

#[test]
fn delete_missing_is_noop() {
    let (_d, c) = make_index(64);
    c.delete(&h(0xAA), 100).unwrap();
    assert_eq!(c.approx_len(), 0);
}

#[test]
fn many_inserts_below_load_factor() {
    // 64 buckets × 4 = 256 capacity; 200 inserts ≈ 0.78 load
    // factor — well within reach without an eviction-chain
    // overflow.
    let (_d, c) = make_index(64);
    for i in 0..200u8 {
        c.put(h(i), dv(i), (100 + i as u64) as Lsn).unwrap();
    }
    for i in 0..200u8 {
        assert_eq!(c.get(&h(i)).unwrap(), Some(dv(i)), "i={i}");
    }
    assert_eq!(c.approx_len(), 200);
}

#[test]
fn iter_returns_all_live_pairs() {
    let (_d, c) = make_index(64);
    for i in 0..50u8 {
        c.put(h(i), dv(i), 100).unwrap();
    }
    let live = c.iter().unwrap();
    assert_eq!(live.len(), 50);
}

#[test]
fn scan_from_covers_iter_in_bounded_steps() {
    let (_d, c) = make_index(64);
    for i in 0..150u8 {
        c.put(h(i), dv(i), 100).unwrap();
    }
    let want: std::collections::HashSet<Hash8> =
        c.iter().unwrap().into_iter().map(|(hash, _)| hash).collect();
    assert_eq!(want.len(), 150);

    // Walk the whole index in small bounded batches via the resume cursor.
    let mut got: Vec<Hash8> = Vec::new();
    let (mut pi, mut sl) = (0usize, 0usize);
    let limit = 7;
    let mut batches = 0;
    loop {
        let (batch, npi, nsl, wrapped) = c.scan_from(pi, sl, limit).unwrap();
        assert!(batch.len() <= limit, "batch must be bounded by limit");
        got.extend(batch.into_iter().map(|(hash, _)| hash));
        pi = npi;
        sl = nsl;
        batches += 1;
        assert!(batches < 1000, "scan must terminate");
        if wrapped {
            break;
        }
    }
    // One full pass visits every live entry EXACTLY once (no dup, no miss).
    assert_eq!(got.len(), 150, "exactly-once coverage in one pass");
    let got_set: std::collections::HashSet<Hash8> = got.into_iter().collect();
    assert_eq!(got_set, want, "scan_from one pass == iter()");
}

#[test]
fn scan_from_empty_index_wraps_immediately() {
    let (_d, c) = make_index(64);
    let (batch, pi, sl, wrapped) = c.scan_from(0, 0, 10).unwrap();
    assert!(batch.is_empty());
    assert!(wrapped);
    assert_eq!((pi, sl), (0, 0));
}

#[test]
fn scan_from_out_of_range_cursor_restarts() {
    let (_d, c) = make_index(64);
    for i in 0..20u8 {
        c.put(h(i), dv(i), 100).unwrap();
    }
    // A stale/out-of-range cursor (table shrank) restarts at the beginning
    // rather than returning nothing.
    let (batch, _, _, _) = c.scan_from(usize::MAX, 999, 100).unwrap();
    assert_eq!(batch.len(), 20);
}

#[test]
fn scan_from_zero_limit_is_empty() {
    let (_d, c) = make_index(64);
    c.put(h(1), dv(1), 100).unwrap();
    let (batch, _, _, wrapped) = c.scan_from(0, 0, 0).unwrap();
    assert!(batch.is_empty());
    assert!(wrapped);
}

#[test]
fn recount_matches_iter_length() {
    let (_d, c) = make_index(64);
    for i in 0..30u8 {
        c.put(h(i), dv(i), 100).unwrap();
    }
    for i in 0..10u8 {
        c.delete(&h(i), 200).unwrap();
    }
    let exact = c.recount().unwrap();
    assert_eq!(exact, 20);
    assert_eq!(c.approx_len(), 20);
}

#[test]
fn round_trip_via_open() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages");
    let meta_page_id;
    {
        let page_store = Arc::new(PageStore::create(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let c = CuckooHash::create(page_store.clone(), page_cache, 32, 0xAAAA, 0xBBBB).unwrap();
        meta_page_id = c.meta_page_id();
        for i in 0..40u8 {
            c.put(h(i), dv(i), (100 + i as u64) as Lsn).unwrap();
        }
        c.flush_meta().unwrap();
    }
    let page_store = Arc::new(PageStore::open(&path).unwrap());
    let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
    let c = CuckooHash::open(page_store, page_cache, meta_page_id).unwrap();
    assert_eq!(c.bucket_count(), 32);
    assert_eq!(c.seeds(), (0xAAAA, 0xBBBB));
    for i in 0..40u8 {
        assert_eq!(c.get(&h(i)).unwrap(), Some(dv(i)), "i={i}");
    }
    assert_eq!(c.recount().unwrap(), 40);
}

#[test]
fn bucket_count_zero_rejected() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages");
    let page_store = Arc::new(PageStore::create(&path).unwrap());
    let page_cache = Arc::new(PageCache::new(page_store.clone(), 4 * 1024 * 1024));
    let err = CuckooHash::create(page_store, page_cache, 0, 1, 2)
        .err()
        .unwrap();
    assert!(matches!(err, MetaDbError::InvalidArgument(_)));
}

#[test]
fn bucket_count_past_one_meta_page_chains_a_continuation() {
    // Pick a bucket count whose page_table needs > one head meta
    // page. The chain must extend instead of erroring out.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages");
    let page_store = Arc::new(PageStore::create(&path).unwrap());
    let page_cache = Arc::new(PageCache::new(page_store.clone(), 8 * 1024 * 1024));
    let head_cap = paged_meta::head_capacity(HEAD_EXTRA_BYTES);
    let big_buckets = ((head_cap + 5) * BUCKETS_PER_PAGE) as u64;
    let cuckoo = CuckooHash::create(page_store, page_cache, big_buckets, 1, 2).unwrap();
    // Force a meta flush so the chain is materialised on disk.
    cuckoo.flush_meta().unwrap();
    assert!(cuckoo.inner.lock().meta_chain.len() >= 2);
}

#[test]
fn concurrent_writers_across_pages() {
    // Drive ~1000 puts from 8 threads. Buckets are sized so that
    // each thread's hashes spread across many data pages, so the
    // per-page-shard locking sees genuine concurrency. Verify
    // every key round-trips after the storm.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages");
    let page_store = Arc::new(PageStore::create(&path).unwrap());
    let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
    // 4096 buckets * 4 entries = 16384 slots, plenty of headroom
    // for 8 * 1000 = 8000 inserts.
    let cuckoo =
        Arc::new(CuckooHash::create(page_store, page_cache, 4096, 0xDEAD, 0xBEEF).unwrap());
    let threads: Vec<_> = (0..8u8)
        .map(|tid| {
            let c = cuckoo.clone();
            std::thread::spawn(move || {
                for i in 0..1000u32 {
                    let mut hash = [0u8; 8];
                    hash[0] = tid;
                    hash[1..5].copy_from_slice(&i.to_le_bytes());
                    c.put(hash, dv(tid), 100 + i as Lsn).unwrap();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    for tid in 0..8u8 {
        for i in 0..1000u32 {
            let mut hash = [0u8; 8];
            hash[0] = tid;
            hash[1..5].copy_from_slice(&i.to_le_bytes());
            assert_eq!(cuckoo.get(&hash).unwrap(), Some(dv(tid)));
        }
    }
    assert_eq!(cuckoo.recount().unwrap(), 8000);
}

#[test]
fn page_associative_insert_avoids_sub_bucket_false_full() {
    let (_d, c) = make_index(32);
    for i in 0..64u64 {
        let mut hash = [0u8; 8];
        hash[..8].copy_from_slice(&i.to_be_bytes());
        c.put(hash, dv((i & 0xff) as u8), 100 + i).unwrap();
    }
    assert_eq!(c.recount().unwrap(), 64);
    for i in 0..64u64 {
        let mut hash = [0u8; 8];
        hash[..8].copy_from_slice(&i.to_be_bytes());
        assert_eq!(c.get(&hash).unwrap(), Some(dv((i & 0xff) as u8)));
    }
}
