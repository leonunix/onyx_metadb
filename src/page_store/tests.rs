use super::*;
use std::fs::OpenOptions;
use std::os::unix::fs::FileExt;
use tempfile::TempDir;

fn mk_page(lsn: Lsn, first_byte: u8) -> Page {
    let mut p = Page::new(PageHeader::new(PageType::L2pLeaf, lsn));
    p.payload_mut()[0] = first_byte;
    p.seal();
    p
}

#[test]
fn create_sizes_file_to_manifest_region() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();
    assert_eq!(ps.high_water(), FIRST_DATA_PAGE);
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        FIRST_DATA_PAGE * PAGE_SIZE as u64,
    );
}

#[test]
fn allocate_write_read_round_trip() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();

    let pid = ps.allocate().unwrap();
    assert_eq!(pid, FIRST_DATA_PAGE);

    ps.write_page(pid, &mk_page(42, 0xAB)).unwrap();
    ps.sync().unwrap();

    let r = ps.read_page(pid).unwrap();
    let h = r.header().unwrap();
    assert_eq!(h.generation, 42);
    assert_eq!(r.payload()[0], 0xAB);
}

#[test]
fn many_pages_round_trip() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();
    let mut ids = Vec::new();
    for i in 0..16u64 {
        let pid = ps.allocate().unwrap();
        ids.push(pid);
        ps.write_page(pid, &mk_page(i, i as u8)).unwrap();
    }
    ps.sync().unwrap();
    assert_eq!(ps.high_water(), FIRST_DATA_PAGE + 16);
    for (i, pid) in ids.iter().enumerate() {
        let r = ps.read_page(*pid).unwrap();
        assert_eq!(r.header().unwrap().generation, i as u64);
        assert_eq!(r.payload()[0], i as u8);
    }
}

#[test]
fn reopen_preserves_pages_and_rebuilds_free_list() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    {
        let ps = PageStore::create(&path).unwrap();
        for i in 0..4u64 {
            let pid = ps.allocate().unwrap();
            ps.write_page(pid, &mk_page(i + 1, i as u8)).unwrap();
        }
        ps.free(FIRST_DATA_PAGE + 1, 100).unwrap();
        ps.try_reclaim().unwrap();
        ps.sync_all().unwrap();
    }
    let ps = PageStore::open(&path).unwrap();
    assert_eq!(ps.high_water(), FIRST_DATA_PAGE + 4);
    assert_eq!(ps.free_list_len(), 1);
    // Reallocating must recycle the freed page.
    let pid = ps.allocate().unwrap();
    assert_eq!(pid, FIRST_DATA_PAGE + 1);
}

#[test]
fn fast_open_preserves_reads_without_rebuilding_free_list() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    {
        let ps = PageStore::create(&path).unwrap();
        for i in 0..4u64 {
            let pid = ps.allocate().unwrap();
            ps.write_page(pid, &mk_page(i + 1, i as u8)).unwrap();
        }
        ps.free(FIRST_DATA_PAGE + 1, 100).unwrap();
        ps.try_reclaim().unwrap();
        ps.sync_all().unwrap();
    }
    let ps = PageStore::open_fast(&path).unwrap();
    let file_pages = std::fs::metadata(&path).unwrap().len() / PAGE_SIZE as u64;
    assert_eq!(ps.high_water(), file_pages);
    assert_eq!(ps.free_list_len(), 0);
    let r = ps.read_page(FIRST_DATA_PAGE + 2).unwrap();
    assert_eq!(r.payload()[0], 2);
    // Fast open does not spend startup time discovering interior free
    // pages; fresh allocations move forward from EOF.
    let pid = ps.allocate().unwrap();
    assert_eq!(pid, file_pages);
}

#[test]
fn try_reclaim_recycles_freed_pids() {
    // Deferred-free means three free calls + one try_reclaim batch
    // hands every pid back to the free list. We assert the SET of
    // recycled pids and that no allocation bumped past the original
    // high-water.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();
    let a = ps.allocate().unwrap();
    let b = ps.allocate().unwrap();
    let c = ps.allocate().unwrap();
    for pid in [a, b, c] {
        ps.write_page(pid, &mk_page(1, 0)).unwrap();
    }
    ps.free(a, 10).unwrap();
    ps.free(b, 11).unwrap();
    ps.free(c, 12).unwrap();
    // Frees are deferred; the free list is empty until reclaim runs.
    assert_eq!(ps.free_list_len(), 0);
    assert_eq!(ps.deferred_free_len(), 3);
    let reclaimed = ps.try_reclaim().unwrap();
    assert_eq!(reclaimed.reclaimed.len(), 3);
    let mut got = vec![
        ps.allocate().unwrap(),
        ps.allocate().unwrap(),
        ps.allocate().unwrap(),
    ];
    got.sort();
    assert_eq!(got, vec![a, b, c]);
    assert_eq!(ps.high_water(), c + 1);
}

#[test]
fn allocate_does_not_reclaim_deferred_pages_by_itself() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();
    let a = ps.allocate().unwrap();
    let b = ps.allocate().unwrap();
    ps.write_page(a, &mk_page(1, 0)).unwrap();
    ps.free(a, 10).unwrap();

    assert_eq!(ps.free_list_len(), 0);
    assert_eq!(ps.deferred_free_len(), 1);

    let next = ps.allocate().unwrap();
    assert_eq!(
        next,
        b + 1,
        "PageStore allocation must not reclaim pages behind Db's cache invalidation"
    );
    assert_eq!(ps.deferred_free_len(), 1);
}

#[test]
fn read_beyond_high_water_is_error() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();
    match ps.read_page(999).unwrap_err() {
        MetaDbError::PageOutOfRange(999) => {}
        e => panic!("{e}"),
    }
}

#[test]
fn cannot_free_manifest_slots() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();
    assert!(matches!(
        ps.free(0, 1).unwrap_err(),
        MetaDbError::InvalidArgument(_)
    ));
    assert!(matches!(
        ps.free(1, 1).unwrap_err(),
        MetaDbError::InvalidArgument(_)
    ));
}

#[test]
fn cannot_free_out_of_range() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();
    assert!(matches!(
        ps.free(999, 1).unwrap_err(),
        MetaDbError::PageOutOfRange(999)
    ));
}

#[test]
fn corrupt_page_read_fails_verify_with_page_id() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let pid = {
        let ps = PageStore::create(&path).unwrap();
        let pid = ps.allocate().unwrap();
        ps.write_page(pid, &mk_page(1, 0)).unwrap();
        ps.sync_all().unwrap();
        pid
    };
    // Flip a byte directly on disk inside the payload area.
    {
        let f = OpenOptions::new().write(true).open(&path).unwrap();
        let off = pid * PAGE_SIZE as u64 + 100;
        f.write_all_at(&[0xFF], off).unwrap();
        f.sync_all().unwrap();
    }
    let ps = PageStore::open(&path).unwrap();
    match ps.read_page(pid).unwrap_err() {
        MetaDbError::PageChecksumMismatch { page_id, .. } => {
            assert_eq!(page_id, pid);
        }
        e => panic!("{e}"),
    }
    // But read_page_unchecked must succeed (returns the corrupt bytes).
    let corrupt = ps.read_page_unchecked(pid).unwrap();
    assert!(corrupt.verify(pid).is_err());
}

#[test]
fn open_rejects_non_page_multiple_size() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    // Write 5000 bytes (not a multiple of 4096)
    std::fs::write(&path, vec![0u8; 5000]).unwrap();
    match PageStore::open(&path).unwrap_err() {
        MetaDbError::Corruption(_) => {}
        e => panic!("{e}"),
    }
}

#[test]
fn open_rejects_shorter_than_manifest_region() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    std::fs::write(&path, vec![0u8; PAGE_SIZE]).unwrap(); // only 1 page
    match PageStore::open(&path).unwrap_err() {
        MetaDbError::Corruption(_) => {}
        e => panic!("{e}"),
    }
}

#[test]
fn create_fails_if_file_exists() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    std::fs::write(&path, b"").unwrap();
    assert!(PageStore::create(&path).is_err());
}

#[test]
fn path_is_retained() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();
    assert_eq!(ps.path(), path);
}

#[test]
fn allocate_run_returns_contiguous_range() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();
    let start = ps.allocate_run(8).unwrap();
    assert_eq!(start, FIRST_DATA_PAGE);
    assert_eq!(ps.high_water(), FIRST_DATA_PAGE + 8);
    // A subsequent single allocate extends further; no overlap.
    let next = ps.allocate().unwrap();
    assert_eq!(next, FIRST_DATA_PAGE + 8);
}

#[test]
fn allocate_run_rejects_zero_count() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();
    assert!(matches!(
        ps.allocate_run(0).unwrap_err(),
        MetaDbError::InvalidArgument(_)
    ));
}

#[test]
fn allocate_run_leaves_fragmented_free_list_entries() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();
    // Seed the free list with one interior individual page. Tail
    // free pages are truncated now, so keep a live page after it.
    let pid = ps.allocate().unwrap();
    let live_tail = ps.allocate().unwrap();
    ps.write_page(pid, &mk_page(1, 0)).unwrap();
    ps.write_page(live_tail, &mk_page(1, 1)).unwrap();
    ps.free(pid, 1).unwrap();
    ps.try_reclaim().unwrap();
    assert_eq!(ps.free_list_len(), 1);
    // A single free page cannot satisfy a larger contiguous run.
    let start = ps.allocate_run(4).unwrap();
    assert_eq!(start, live_tail + 1);
    assert_eq!(ps.free_list_len(), 1);
}

#[test]
fn allocate_batch_reuses_fragmented_free_list_entries() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();
    let start = ps.allocate_run(8).unwrap();
    for i in 0..8 {
        ps.write_page(start + i, &mk_page(1, i as u8)).unwrap();
    }
    ps.free_many(&[start, start + 2, start + 5], 99).unwrap();
    ps.try_reclaim().unwrap();
    assert_eq!(ps.free_list_len(), 3);
    let high_water = ps.high_water();

    let mut batch = ps.allocate_batch(3).unwrap();
    batch.sort_unstable();
    assert_eq!(batch, vec![start, start + 2, start + 5]);
    assert_eq!(ps.high_water(), high_water);
    assert_eq!(ps.free_list_len(), 0);
}

#[test]
fn allocate_batch_extends_only_for_missing_pages() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();
    let start = ps.allocate_run(4).unwrap();
    let live_tail = ps.allocate().unwrap();
    for i in 0..4 {
        ps.write_page(start + i, &mk_page(1, i as u8)).unwrap();
    }
    ps.write_page(live_tail, &mk_page(1, 9)).unwrap();
    ps.free_many(&[start + 1, start + 3], 99).unwrap();
    ps.try_reclaim().unwrap();
    let high_water = ps.high_water();

    let batch = ps.allocate_batch(5).unwrap();
    assert_eq!(batch.len(), 5);
    assert_eq!(ps.high_water(), high_water + 3);
    assert_eq!(ps.free_list_len(), 0);
}

#[test]
fn allocate_run_reuses_contiguous_free_list_suffix() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();
    let start = ps.allocate_run(4).unwrap();
    let live_tail = ps.allocate().unwrap();
    for i in 0..4 {
        ps.write_page(start + i, &mk_page(1, i as u8)).unwrap();
    }
    ps.write_page(live_tail, &mk_page(1, 9)).unwrap();
    ps.free_run(start, 4, 99).unwrap();
    ps.try_reclaim().unwrap();
    assert_eq!(ps.free_list_len(), 4);

    let reused = ps.allocate_run(4).unwrap();
    assert_eq!(reused, start);
    assert_eq!(ps.free_list_len(), 0);
    assert_eq!(ps.high_water(), live_tail + 1);
}

#[test]
fn allocate_run_reuses_contiguous_run_before_fragmented_tail() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();
    let run_start = ps.allocate_run(3).unwrap();
    let gap = ps.allocate().unwrap();
    let tail = ps.allocate().unwrap();
    for (idx, pid) in [run_start, run_start + 1, run_start + 2, gap, tail]
        .into_iter()
        .enumerate()
    {
        ps.write_page(pid, &mk_page(1, idx as u8)).unwrap();
    }
    ps.free_run(run_start, 3, 99).unwrap();
    ps.try_reclaim().unwrap();
    ps.free(tail, 100).unwrap();
    ps.try_reclaim().unwrap();
    assert_eq!(ps.free_list_len(), 3);

    let reused = ps.allocate_run(3).unwrap();
    assert_eq!(reused, run_start);
    assert_eq!(ps.free_list_len(), 0);
    assert_eq!(ps.high_water(), tail);
}

#[test]
fn free_run_returns_pages_to_free_list() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();
    let start = ps.allocate_run(3).unwrap();
    let live_tail = ps.allocate().unwrap();
    for i in 0..3 {
        ps.write_page(start + i, &mk_page(1, 0)).unwrap();
    }
    ps.write_page(live_tail, &mk_page(1, 9)).unwrap();
    ps.free_run(start, 3, 99).unwrap();
    ps.try_reclaim().unwrap();
    assert_eq!(ps.free_list_len(), 3);
}

#[test]
fn reclaim_truncates_contiguous_free_tail() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();
    let start = ps.allocate_run(5).unwrap();
    for i in 0..5 {
        ps.write_page(start + i, &mk_page(1, i as u8)).unwrap();
    }
    assert_eq!(ps.high_water(), start + 5);

    ps.free_run(start + 2, 3, 99).unwrap();
    let reclaimed = ps.try_reclaim().unwrap();
    assert_eq!(reclaimed.reclaimed, vec![start + 2, start + 3, start + 4]);
    assert_eq!(ps.high_water(), start + 2);
    assert_eq!(ps.free_list_len(), 0);
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        (start + 2) * PAGE_SIZE as u64
    );
}

#[test]
fn batched_allocate_extends_file_by_chunk_boundary() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let chunk: u64 = 8;
    let ps = PageStore::create_with_grow_chunk(&path, chunk).unwrap();
    // One allocate should pre-extend the file by the whole chunk.
    let _ = ps.allocate().unwrap();
    let expected_pages = FIRST_DATA_PAGE + chunk;
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        expected_pages * PAGE_SIZE as u64,
        "first allocate should pre-extend to the next chunk boundary",
    );
    // Fill the rest of the chunk; file size must not change.
    for _ in 1..chunk {
        let _ = ps.allocate().unwrap();
    }
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        expected_pages * PAGE_SIZE as u64,
        "allocations within the committed chunk must not extend the file",
    );
    assert_eq!(ps.high_water(), FIRST_DATA_PAGE + chunk);
    // One more allocate should roll into the next chunk.
    let _ = ps.allocate().unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        (FIRST_DATA_PAGE + 2 * chunk) * PAGE_SIZE as u64,
        "crossing a chunk boundary extends the file by exactly one more chunk",
    );
}

#[test]
fn allocate_run_respects_grow_chunk() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let chunk: u64 = 4;
    let ps = PageStore::create_with_grow_chunk(&path, chunk).unwrap();
    // Run of 6 with chunk 4 → file must cover >= 6 pages, rounded up
    // to the next chunk boundary (8).
    let start = ps.allocate_run(6).unwrap();
    assert_eq!(start, FIRST_DATA_PAGE);
    assert_eq!(ps.high_water(), FIRST_DATA_PAGE + 6);
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        (FIRST_DATA_PAGE + 2 * chunk) * PAGE_SIZE as u64,
    );
}

#[test]
fn reject_zero_grow_chunk() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    assert!(matches!(
        PageStore::create_with_grow_chunk(&path, 0).unwrap_err(),
        MetaDbError::InvalidArgument(_)
    ));
}

#[test]
fn open_truncates_growth_tail() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let chunk: u64 = 64;
    let last_valid_pid;
    {
        let ps = PageStore::create_with_grow_chunk(&path, chunk).unwrap();
        // Allocate + write 3 pages. Pre-extend reserves `chunk`
        // pages worth of growth tail on disk (pages 5..=66 zero-init).
        for i in 0..3 {
            let pid = ps.allocate().unwrap();
            ps.write_page(pid, &mk_page(1, i as u8)).unwrap();
        }
        last_valid_pid = FIRST_DATA_PAGE + 2;
        ps.sync_all().unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            (FIRST_DATA_PAGE + chunk) * PAGE_SIZE as u64,
            "pre-extend must have reserved the whole chunk",
        );
    }
    // Reopen: the growth tail (zero pages past the last valid one)
    // should be truncated back.
    let ps = PageStore::open_with_grow_chunk(&path, chunk).unwrap();
    assert_eq!(ps.high_water(), last_valid_pid + 1);
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        (last_valid_pid + 1) * PAGE_SIZE as u64,
        "open must truncate zero-init growth tail back to last valid page",
    );
}

#[test]
fn open_truncates_punched_tail_free_page() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let chunk: u64 = 32;
    let last_valid_pid;
    {
        let ps = PageStore::create_with_grow_chunk(&path, chunk).unwrap();
        // Allocate 3, free the last: hole punching turns that page into
        // zero tail, so reopen can truncate it and hand the id out again
        // from high_water.
        for _ in 0..3 {
            let pid = ps.allocate().unwrap();
            ps.write_page(pid, &mk_page(1, 0)).unwrap();
        }
        ps.free(FIRST_DATA_PAGE + 2, 42).unwrap();
        ps.try_reclaim().unwrap();
        last_valid_pid = FIRST_DATA_PAGE + 1;
        ps.sync_all().unwrap();
    }
    let ps = PageStore::open_with_grow_chunk(&path, chunk).unwrap();
    assert_eq!(ps.high_water(), last_valid_pid + 1);
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        (last_valid_pid + 1) * PAGE_SIZE as u64,
    );
    assert_eq!(ps.free_list_len(), 0);
    assert_eq!(ps.allocate().unwrap(), FIRST_DATA_PAGE + 2);
}

#[test]
fn open_on_all_zero_growth_tail_recovers_as_empty_data_region() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    // Fabricate a file with a manifest region and pure zero growth tail
    // past it, as if a crash happened after pre-extend but before any
    // data page was written.
    let pages_on_disk = FIRST_DATA_PAGE + 16;
    std::fs::write(
        &path,
        vec![0u8; (pages_on_disk * PAGE_SIZE as u64) as usize],
    )
    .unwrap();
    let ps = PageStore::open_with_grow_chunk(&path, 16).unwrap();
    // No page past the manifest region decoded as valid → high_water
    // sits at FIRST_DATA_PAGE, and the growth tail is truncated.
    assert_eq!(ps.high_water(), FIRST_DATA_PAGE);
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        FIRST_DATA_PAGE * PAGE_SIZE as u64,
    );
}

#[test]
fn crash_safety_allocate_without_write_is_not_leaked_after_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let chunk: u64 = 16;
    {
        let ps = PageStore::create_with_grow_chunk(&path, chunk).unwrap();
        // Write 2 pages then leak an allocation (simulating a crash
        // between allocate and write_page, with WAL un-committed).
        for i in 0..2 {
            let pid = ps.allocate().unwrap();
            ps.write_page(pid, &mk_page(1, i as u8)).unwrap();
        }
        let _leaked = ps.allocate().unwrap();
        ps.sync_all().unwrap();
    }
    // Reopen: the leaked allocation becomes part of growth tail (the
    // page is still zero on disk, so its header fails to decode).
    let ps = PageStore::open_with_grow_chunk(&path, chunk).unwrap();
    assert_eq!(ps.high_water(), FIRST_DATA_PAGE + 2);
    // New allocations reuse page ids from where the recovered high
    // water points, overwriting the zeroed leak in place.
    let pid = ps.allocate().unwrap();
    assert_eq!(pid, FIRST_DATA_PAGE + 2);
}

/// buffer-backed journal parallel-submit path: a multi-run reclaim must still
/// (a) stamp every selected pid as `Free` on disk, (b) extend
/// `free_list` with the full set, and (c) re-allocate them in
/// pid-sorted order. A multi-run batch is built by interleaving
/// contiguous and gap-separated pids so coalescing produces > 1
/// run, forcing the IoSubmitter fan-out path.
#[test]
fn reclaim_handles_multi_run_batch_via_io_submitter() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages.onyx_meta");
    let ps = PageStore::create(&path).unwrap();
    // Allocate 8 contiguous, write payload, free 4 non-adjacent
    // pairs so reclaim sees four 1-page "runs" that must each
    // become its own SQE.
    let mut all = Vec::new();
    for _ in 0..8 {
        all.push(ps.allocate().unwrap());
    }
    for pid in &all {
        ps.write_page(*pid, &mk_page(7, 0xaa)).unwrap();
    }
    // Free every other page (creates 4 runs of length 1).
    let to_free = [all[0], all[2], all[4], all[6]];
    for pid in to_free {
        ps.free(pid, 99).unwrap();
    }
    assert_eq!(ps.deferred_free_len(), 4);

    let outcome = ps.try_reclaim().unwrap();
    let mut reclaimed = outcome.reclaimed.clone();
    reclaimed.sort();
    assert_eq!(reclaimed, to_free.to_vec());

    // Every freed pid now reads back as Free-headered.
    for pid in to_free {
        let page = ps.read_page_unchecked(pid).unwrap();
        // Either zero (punch took effect) or Free header.
        if page.bytes().iter().all(|b| *b == 0) {
            continue;
        }
        let h = page.header().unwrap();
        assert_eq!(h.page_type, PageType::Free);
        assert_eq!(h.generation, 99);
    }
    // free_list must contain all four; an allocate cycle hands
    // them back in pid-sorted order.
    let mut got = Vec::new();
    for _ in 0..4 {
        got.push(ps.allocate().unwrap());
    }
    got.sort();
    assert_eq!(got, to_free.to_vec());
}

// ---- fixed-capacity device path (MemDevice) --------------------------------

/// A fixed-capacity device returns `CapacityExhausted` (not a panic, not a
/// generic IO error) once allocation would cross `capacity_pages`, so the
/// caller can abort the in-flight checkpoint cleanly.
#[test]
fn mem_device_capacity_exhaustion_is_clean() {
    // capacity 6 pages: 2 reserved for the manifest slots, 4 allocatable.
    let device: Arc<dyn PageDevice> = Arc::new(MemDevice::new(6));
    let ps = PageStore::create_on_device(device).unwrap();
    for _ in 0..4 {
        ps.allocate().expect("first 4 allocations fit");
    }
    match ps.allocate() {
        Err(MetaDbError::CapacityExhausted {
            requested_pages,
            capacity_pages,
        }) => {
            assert_eq!(requested_pages, 7);
            assert_eq!(capacity_pages, 6);
        }
        other => panic!("expected CapacityExhausted past capacity, got {other:?}"),
    }
    // The store's high-water did not advance past the failed allocation.
    assert_eq!(ps.high_water(), 6);
}

/// Bounded-scan open over a device rebuilds the free list from `Free`-stamped
/// interior pages, exactly like the file open scan but bounded by
/// `page_high_water` instead of EOF.
#[test]
fn mem_device_bounded_scan_recovers_free_list() {
    let device: Arc<dyn PageDevice> = Arc::new(MemDevice::new(64));
    let ps = PageStore::create_on_device(device.clone()).unwrap();
    let mut all = Vec::new();
    for _ in 0..10 {
        all.push(ps.allocate().unwrap());
    }
    for pid in &all {
        ps.write_page(*pid, &mk_page(5, 0xcd)).unwrap();
    }
    // Free three interior pages (never the tail) so the frontier stays put.
    let to_free = [all[1], all[3], all[5]];
    for pid in to_free {
        ps.free(pid, 42).unwrap();
    }
    ps.try_reclaim().unwrap();
    let frontier = ps.high_water();
    drop(ps);

    // Reopen over the SAME device and rebuild the free list bounded by the
    // frontier — the three Free pages must reappear.
    let ps2 = PageStore::open_on_device(device).unwrap();
    ps2.rebuild_free_list_bounded(frontier).unwrap();
    assert_eq!(ps2.high_water(), frontier);
    assert_eq!(ps2.free_list_len(), 3);
    let mut got = Vec::new();
    for _ in 0..3 {
        got.push(ps2.allocate().unwrap());
    }
    got.sort();
    assert_eq!(got, to_free.to_vec());
}

/// The bounded scan must never touch a prior tenant's garbage sitting above
/// `page_high_water`: a device is not zeroed on (re)allocation, so stale
/// but structurally-valid pages can live past the frontier.
#[test]
fn mem_device_bounded_scan_ignores_garbage_above_frontier() {
    let device: Arc<dyn PageDevice> = Arc::new(MemDevice::new(64));
    let ps = PageStore::create_on_device(device.clone()).unwrap();
    for _ in 0..5 {
        let pid = ps.allocate().unwrap();
        ps.write_page(pid, &mk_page(9, 0x11)).unwrap();
    }
    let frontier = ps.high_water(); // == 7 (FIRST_DATA_PAGE + 5)
    // Plant a structurally-valid but out-of-frontier page (a prior tenant's
    // leftover) at pid 20, far above the frontier.
    let garbage = mk_page(999, 0xff);
    device.write_page_run_bytes(20, garbage.bytes()).unwrap();
    drop(ps);

    let ps2 = PageStore::open_on_device(device).unwrap();
    ps2.rebuild_free_list_bounded(frontier).unwrap();
    // The scan stopped at the frontier: garbage at pid 20 is invisible, the
    // frontier is unchanged, and the next allocation resumes at the frontier
    // (NOT at 21).
    assert_eq!(ps2.high_water(), frontier);
    assert_eq!(ps2.free_list_len(), 0);
    assert_eq!(ps2.allocate().unwrap(), frontier);
}
