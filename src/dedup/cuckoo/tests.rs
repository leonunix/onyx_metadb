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
fn saturation_drops_instead_of_erroring() {
    // `bucket_count=1` collapses every hash onto a single page. Cuckoo
    // eviction can't reach 100% fill, so at some point a fresh distinct
    // hash has nowhere to go and the eviction chain is exceeded. That is
    // the saturation case — and it must DROP, not error.
    let (_d, c) = make_index(1);
    let mut timings = crate::metrics::DedupPutStageTimings::default();
    let mut placed = 0u64;
    let mut dropped = 0u64;
    let mut placed_hashes = Vec::new();
    // Push well past a single page's capacity so saturation is certain.
    for i in 0..400u64 {
        // `put_with_metrics` NEVER errors — it reports a drop as Ok(false).
        if c.put_with_metrics(h64(i), dv64(1), (100 + i) as Lsn, &mut timings)
            .unwrap()
        {
            placed += 1;
            placed_hashes.push(h64(i));
        } else {
            dropped += 1;
        }
    }
    assert!(dropped > 0, "the page must saturate and drop some inserts");
    assert!(placed > 0, "some inserts must be placed before saturation");
    // A dropped insert must not bump the live count.
    assert_eq!(c.approx_len(), placed);
    // Everything reported as placed is still readable.
    for hash in &placed_hashes {
        assert_eq!(c.get(hash).unwrap(), Some(dv64(1)));
    }
    // The public `put` preserves the hard-error contract for the
    // verifier / direct callers once the table is saturated.
    assert!(
        c.put(h64(1_000_000), dv64(1), 999).is_err(),
        "CuckooHash::put must still hard-error on saturation"
    );
}

#[test]
fn batch_put_reports_dropped_hashes_on_saturation() {
    // Saturate the single page, then batch-put a mix of an overwrite of
    // a known-present hash (placed) and fresh distinct hashes (dropped),
    // and confirm the returned dropped set is exactly the fresh ones.
    let (_d, c) = make_index(1);
    let mut timings = crate::metrics::DedupPutStageTimings::default();
    // h64(0) is guaranteed placed (first insert into an empty page).
    assert!(
        c.put_with_metrics(h64(0), dv64(7), 100, &mut timings)
            .unwrap()
    );
    // Saturate the rest of the page.
    for i in 1..400u64 {
        c.put_with_metrics(h64(i), dv64(1), (100 + i) as Lsn, &mut timings)
            .unwrap();
    }
    // Fresh hashes far outside the placed set — certain to be dropped.
    let fresh_a = h64(9_000_001);
    let fresh_b = h64(9_000_002);
    let batch = vec![
        CuckooPutEntry {
            hash: h64(0),
            value: dv64(200),
        }, // overwrite of a present hash → placed
        CuckooPutEntry {
            hash: fresh_a,
            value: dv64(201),
        }, // fresh → dropped (page full)
        CuckooPutEntry {
            hash: fresh_b,
            value: dv64(202),
        }, // fresh → dropped
    ];
    let dropped = c.put_many_with_metrics(&batch, 500, &mut timings).unwrap();
    let dropped_set: std::collections::HashSet<Hash8> = dropped.into_iter().collect();
    assert_eq!(
        dropped_set,
        [fresh_a, fresh_b]
            .into_iter()
            .collect::<std::collections::HashSet<_>>(),
    );
    // The overwrite landed; the fresh drops did not.
    assert_eq!(c.get(&h64(0)).unwrap(), Some(dv64(200)));
    assert_eq!(c.get(&fresh_a).unwrap(), None);
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
    let want: std::collections::HashSet<Hash8> = c
        .iter()
        .unwrap()
        .into_iter()
        .map(|(hash, _)| hash)
        .collect();
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

fn h64(x: u64) -> Hash8 {
    x.to_be_bytes()
}

fn dv64(x: u64) -> DedupValue {
    let mut v = [0u8; 28];
    v[..8].copy_from_slice(&x.to_be_bytes());
    DedupValue(v)
}

#[test]
fn concurrent_reads_during_grow_never_error() {
    // Regression for the page_table publish/read race: a writer sets
    // `page_table[page_idx] = pid` (visible to lockless readers) BEFORE
    // it finishes `write_page` + `replace_or_insert`. A reader that
    // resolved the pid in that window used to read the still-unwritten
    // on-disk slot — all-zeros for a freshly extended page (surfacing as
    // `PageMagicMismatch(0x0)`) or stale-but-CRC-valid bytes for a reused
    // free-list page (a silently mis-parsed bucket). The fix routes every
    // read through the per-page shard lock the writer already holds, so a
    // reader either sees `page_table[page_idx] == 0` (clean miss) or the
    // fully written page. This test grows the index from several writers
    // while several readers hammer the same key space; every `get` must
    // return `Ok` (never the magic-mismatch error), and every committed
    // key must round-trip afterwards.
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages");
    let page_store = Arc::new(PageStore::create(&path).unwrap());
    let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
    // 4096 buckets / 28 buckets-per-page ≈ 147 data pages; 4000 unique
    // keys spread across them keeps growth continuous throughout the run
    // while staying under a 0.7 load factor (4096 * 4 = 16384 slots).
    let cuckoo =
        Arc::new(CuckooHash::create(page_store, page_cache, 4096, 0xC0DE, 0xF00D).unwrap());

    const WRITERS: u64 = 4;
    const PER_WRITER: u64 = 1000;
    const READERS: usize = 4;
    let total = WRITERS * PER_WRITER;

    let done = Arc::new(AtomicBool::new(false));
    let read_errors = Arc::new(AtomicU64::new(0));
    let reads_ok = Arc::new(AtomicU64::new(0));

    let readers: Vec<_> = (0..READERS)
        .map(|_| {
            let c = cuckoo.clone();
            let done = done.clone();
            let read_errors = read_errors.clone();
            let reads_ok = reads_ok.clone();
            std::thread::spawn(move || {
                // Sweep the whole key space repeatedly while writers grow
                // the index, so reads land on pages mid-publish.
                while !done.load(Ordering::Acquire) {
                    for x in 0..total {
                        match c.get(&h64(x)) {
                            // None is fine: the key may not be inserted yet.
                            Ok(_) => {
                                reads_ok.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                read_errors.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            })
        })
        .collect();

    let writers: Vec<_> = (0..WRITERS)
        .map(|w| {
            let c = cuckoo.clone();
            std::thread::spawn(move || {
                // Disjoint, interleaved key stripes so every writer keeps
                // allocating fresh pages across the whole table.
                let mut x = w;
                while x < total {
                    c.put(h64(x), dv64(x), 100 + x as Lsn).unwrap();
                    x += WRITERS;
                }
            })
        })
        .collect();

    for t in writers {
        t.join().unwrap();
    }
    done.store(true, Ordering::Release);
    for t in readers {
        t.join().unwrap();
    }

    assert_eq!(
        read_errors.load(Ordering::Relaxed),
        0,
        "no concurrent read may surface an error (page_table publish race)"
    );
    assert!(
        reads_ok.load(Ordering::Relaxed) > 0,
        "readers must have actually run"
    );
    // Every committed key round-trips with its exact value.
    for x in 0..total {
        assert_eq!(cuckoo.get(&h64(x)).unwrap(), Some(dv64(x)), "x={x}");
    }
    assert_eq!(cuckoo.recount().unwrap(), total);
}

#[test]
fn dangling_unwritten_page_without_writer_still_errors() {
    // The fix relies on a concurrent writer HOLDING the shard lock across
    // its page write; it deliberately does NOT add zero-page tolerance to
    // the read path. So a `page_table` entry pointing at a genuinely
    // unwritten (all-zero) page with no writer in flight is a real fault
    // and must still surface as a verify error, not a silent empty
    // bucket. This guards against anyone "fixing" the race by swallowing
    // `PageMagicMismatch` (which would also mask true corruption).
    let (_d, c) = make_index(64);
    // Allocate a fresh page id but never write a sealed page to it, then
    // publish it into the page table — mimicking a corrupt/torn state.
    let dangling = c.page_store.allocate().unwrap();
    c.inner.lock().page_table[0] = dangling;
    let err = c
        .recount()
        .expect_err("unwritten page must not read as empty");
    assert!(
        matches!(err, MetaDbError::PageMagicMismatch { .. }),
        "expected magic mismatch on an unwritten page, got {err:?}"
    );
}

// ── online-resize migration primitives (put_if_absent / migrate_page_into) ──

#[test]
fn put_if_absent_does_not_overwrite_or_duplicate() {
    let (_d, c) = make_index(64);
    // Absent → inserted.
    assert_eq!(
        c.put_if_absent(h64(1), dv64(10), 100).unwrap(),
        PutIfAbsentOutcome::Inserted
    );
    assert_eq!(c.get(&h64(1)).unwrap(), Some(dv64(10)));
    // Present → left untouched (walker never clobbers a fresher value),
    // reported AlreadyPresent.
    assert_eq!(
        c.put_if_absent(h64(1), dv64(999), 101).unwrap(),
        PutIfAbsentOutcome::AlreadyPresent
    );
    assert_eq!(c.get(&h64(1)).unwrap(), Some(dv64(10)));
    // A subsequent front-end overwrite is also respected as "present".
    c.put(h64(1), dv64(20), 102).unwrap();
    assert_eq!(
        c.put_if_absent(h64(1), dv64(7), 103).unwrap(),
        PutIfAbsentOutcome::AlreadyPresent
    );
    assert_eq!(c.get(&h64(1)).unwrap(), Some(dv64(20)));
    // Never duplicated: still exactly one live entry.
    assert_eq!(c.approx_len(), 1);
}

#[test]
fn put_if_absent_drops_when_candidate_pages_full() {
    // bucket_count=1 collapses every hash onto a single page. Fill it via the
    // normal (evicting) put path, then a fresh put_if_absent has nowhere to go
    // — it does NOT evict, so it drops (never errors).
    let (_d, c) = make_index(1);
    let mut timings = crate::metrics::DedupPutStageTimings::default();
    let mut placed = Vec::new();
    for i in 0..400u64 {
        if c.put_with_metrics(h64(i), dv64(i), 100 + i, &mut timings)
            .unwrap()
        {
            placed.push(i);
        }
    }
    assert!(!placed.is_empty());
    // A fresh distinct hash on the saturated page must drop.
    assert_eq!(
        c.put_if_absent(h64(9_999_999), dv64(1), 1000).unwrap(),
        PutIfAbsentOutcome::Dropped
    );
    // A hash already present is reported present, never re-inserted or evicted.
    assert_eq!(
        c.put_if_absent(h64(placed[0]), dv64(2), 1001).unwrap(),
        PutIfAbsentOutcome::AlreadyPresent
    );
}

#[test]
fn migrate_page_into_copies_all_live_entries_idempotently() {
    // OLD: 60 buckets → 3 data pages. NEW: 200 buckets → 8 data pages, so the
    // bucket geometry (and thus each hash's candidate pages) genuinely differs
    // between the two tables — exercising the remap, not a trivial page copy.
    let (_do, old) = make_index(60);
    let (_dn, new) = make_index(200);
    for i in 0..100u64 {
        old.put(h64(i), dv64(i), 100 + i).unwrap();
    }
    assert_eq!(old.recount().unwrap(), 100);

    let old_pages = old.inner.lock().page_table.len();
    for pi in 0..old_pages {
        old.migrate_page_into(&new, pi, 1000).unwrap();
    }
    // Every OLD entry now lives in NEW with its exact value.
    for i in 0..100u64 {
        assert_eq!(new.get(&h64(i)).unwrap(), Some(dv64(i)), "i={i}");
    }
    assert_eq!(new.recount().unwrap(), 100);

    // Re-walking OLD is idempotent: nothing inserted, everything already present.
    let mut already = 0u64;
    for pi in 0..old_pages {
        let s = old.migrate_page_into(&new, pi, 1001).unwrap();
        assert_eq!(s.inserted, 0, "no fresh inserts on re-walk (page {pi})");
        already += s.already_present;
    }
    assert_eq!(already, 100);
    assert_eq!(new.recount().unwrap(), 100);
}

#[test]
fn migrate_preserves_fresher_new_value_over_stale_old() {
    // The delete/overwrite-during-migration invariant in miniature: a value
    // already in NEW is never clobbered by the OLD copy the walker carries.
    let (_do, old) = make_index(32);
    let (_dn, new) = make_index(64);
    old.put(h64(5), dv64(500), 100).unwrap(); // stale value in OLD
    new.put(h64(5), dv64(999), 200).unwrap(); // fresher value already in NEW
    let old_pages = old.inner.lock().page_table.len();
    for pi in 0..old_pages {
        old.migrate_page_into(&new, pi, 300).unwrap();
    }
    // NEW keeps its fresher value; the stale OLD copy did not win.
    assert_eq!(new.get(&h64(5)).unwrap(), Some(dv64(999)));
    assert_eq!(new.recount().unwrap(), 1);
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

#[test]
fn referenced_page_ids_returns_distinct_pages() {
    // Regression for the box-found OLD-page leak. `referenced_page_ids` used to
    // push `meta_page_id` AND extend with `meta_chain`, whose element 0 IS
    // `meta_page_id` (the chain is rooted at the stable head) — so the head was
    // returned TWICE. `finish_swap` feeds the OLD table's referenced pages to
    // `page_store.free_many`, which rejects a batch containing a duplicate page
    // id ("duplicate free of page N in one batch") and then leaks the entire
    // OLD table on every resize swap. Every physical page must appear once.
    //
    // A modulus large enough to need a multi-page meta chain, so the chain
    // pages beyond the head are exercised too.
    let (_d, c) = make_index(20_000);
    for i in 0..400u64 {
        let mut hash = [0u8; 8];
        hash.copy_from_slice(&i.to_be_bytes());
        c.put(hash, dv((i & 0xff) as u8), 100 + i).unwrap();
    }
    c.flush_meta().unwrap();

    let ids = c.referenced_page_ids();
    let mut deduped = ids.clone();
    deduped.sort_unstable();
    deduped.dedup();
    assert_eq!(
        deduped.len(),
        ids.len(),
        "referenced_page_ids must contain no duplicate page ids (got {ids:?})",
    );
    assert_eq!(
        ids.iter().filter(|&&p| p == c.meta_page_id()).count(),
        1,
        "the stable meta head page id must appear exactly once",
    );
}

#[test]
fn put_if_absent_many_grouped_skips_present_inserts_absent_no_dup() {
    // The batched migration primitive (Issue D fix: one coalesced page write
    // per OLD page instead of per entry) must match single-entry put-if-absent
    // semantics: skip hashes already present (fresher NEW value wins), insert
    // the absent ones exactly once, count each, and never duplicate a hash.
    let (_d, c) = make_index(600);
    c.put_if_absent(h(1), dv(1), 100).unwrap();
    c.put_if_absent(h(2), dv(2), 101).unwrap();

    let entries = vec![
        // present — a fresher value is offered but must be IGNORED (put-if-absent)
        CuckooPutEntry {
            hash: h(1),
            value: dv(99),
        },
        CuckooPutEntry {
            hash: h(2),
            value: dv(99),
        },
        // absent — must be inserted
        CuckooPutEntry {
            hash: h(3),
            value: dv(3),
        },
        CuckooPutEntry {
            hash: h(4),
            value: dv(4),
        },
        CuckooPutEntry {
            hash: h(5),
            value: dv(5),
        },
    ];
    let stats = c.put_if_absent_many_grouped(&entries, 200).unwrap();
    assert_eq!(stats.already_present, 2, "h1,h2 present → skipped");
    assert_eq!(stats.inserted, 3, "h3,h4,h5 inserted");
    assert_eq!(stats.dropped, 0);

    assert_eq!(
        c.get(&h(1)).unwrap(),
        Some(dv(1)),
        "present entry NOT overwritten"
    );
    assert_eq!(
        c.get(&h(2)).unwrap(),
        Some(dv(2)),
        "present entry NOT overwritten"
    );
    assert_eq!(c.get(&h(3)).unwrap(), Some(dv(3)));
    assert_eq!(c.get(&h(5)).unwrap(), Some(dv(5)));
    assert_eq!(c.recount().unwrap(), 5, "exactly 5 entries, no duplicates");

    // Idempotent re-run: everything present now, nothing inserted.
    let again = c.put_if_absent_many_grouped(&entries, 201).unwrap();
    assert_eq!(again.inserted, 0);
    assert_eq!(again.already_present, 5);
    assert_eq!(c.recount().unwrap(), 5);
}
