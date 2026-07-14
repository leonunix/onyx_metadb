//! Device-path (`create_on_device` / `open_on_device`) tests over the
//! in-memory `MemDevice` + `MemJournalDevice`. These exercise the fixed-capacity
//! semantics onyx gets over a chunklet meta LogicalDisk without a real LD:
//! bounded-scan open, `CapacityExhausted`, the ring lifecycle journal, and the
//! crash-to-older-generation dedup-ceiling frontier (the 3d MUST-FIX).

use super::*;
use crate::lifecycle_log::MemJournalDevice;
use crate::page_store::MemDevice;
use tempfile::TempDir;

/// Config for device-backed tests: background workers off (deterministic
/// high-water / free-list assertions, no racing page allocation) and a small
/// cuckoo table (bounds page growth on the fixed device). `path` is a throwaway
/// TempDir — the device path never touches it for persistence.
fn device_cfg(dir: &TempDir) -> Config {
    let mut cfg = Config::new(dir.path());
    cfg.dedup_cuckoo_buckets = 256;
    cfg.async_reclaim_enabled = false;
    cfg.lineage_gc_enabled = false;
    cfg.dedup_drainer_enabled = false;
    cfg.l2p_writeback_enabled = false;
    cfg.bfg_threads_enabled = false;
    cfg.livelist_condense_min_segments = 0;
    cfg
}

fn mk_db_on_device(
    dir: &TempDir,
    page_cap_pages: u64,
    ring_blocks: u64,
) -> (Arc<MemDevice>, Arc<MemJournalDevice>, Arc<Db>) {
    let page_dev = Arc::new(MemDevice::new(page_cap_pages));
    let journal_dev = Arc::new(MemJournalDevice::new(ring_blocks));
    let db = Db::create_on_device_with_faults(
        device_cfg(dir),
        FaultController::disabled(),
        page_dev.clone(),
        journal_dev.clone(),
    )
    .unwrap();
    (page_dev, journal_dev, db)
}

/// L2P + dedup entries survive a close/reopen over the same device bytes, and
/// the bounded-scan open recovers a sane high-water mark.
#[test]
fn device_roundtrip_l2p_and_dedup() {
    let dir = TempDir::new().unwrap();
    let (page_dev, journal_dev, db) = mk_db_on_device(&dir, 8192, 512);

    for i in 0u64..300 {
        db.insert(0, i, v((i / 32) as u8)).unwrap();
    }
    for n in 1u64..40 {
        db.put_dedup(h(n), dv(n as u8)).unwrap();
    }
    db.flush().unwrap();
    let high_water_before = db.page_store.high_water();
    assert!(high_water_before > FIRST_DATA_PAGE);
    drop(db);

    // Reopen over the SAME device bytes (Arcs kept alive across the drop).
    let db = Db::open_on_device_with_faults(
        device_cfg(&dir),
        FaultController::disabled(),
        page_dev.clone(),
        journal_dev.clone(),
    )
    .unwrap();
    for i in 0u64..300 {
        assert_eq!(
            db.get(0, i).unwrap(),
            Some(v((i / 32) as u8)),
            "reopen lba {i}"
        );
    }
    for n in 1u64..40 {
        assert_eq!(
            db.get_dedup(&h(n)).unwrap(),
            Some(dv(n as u8)),
            "reopen dedup {n}"
        );
    }
    // Bounded scan must recover a high-water mark that covers the live pages.
    assert!(
        db.page_store.high_water() >= high_water_before,
        "reopened high_water {} regressed below pre-close {}",
        db.page_store.high_water(),
        high_water_before
    );
}

/// A fixed device returns a clean `CapacityExhausted` (not a panic / generic Io)
/// once the page window is full, and the DB reopens intact afterwards — the
/// unit-level regression for the "meta region fills → don't brick" contract.
#[test]
fn device_capacity_exhausted_is_clean_and_reopens() {
    let dir = TempDir::new().unwrap();
    // Enough to create a fresh DB (bare create ~4119 pages), small enough to hit
    // the wall quickly with raw allocations afterward.
    let cap = 4400u64;
    let (page_dev, journal_dev, db) = mk_db_on_device(&dir, cap, 512);

    // Drive raw page allocation until the fixed window is exhausted.
    let mut hit_wall = false;
    for _ in 0..cap * 2 {
        match db.page_store.allocate() {
            Ok(_) => {}
            Err(MetaDbError::CapacityExhausted {
                requested_pages,
                capacity_pages,
            }) => {
                assert_eq!(capacity_pages, cap);
                assert!(requested_pages > capacity_pages);
                hit_wall = true;
                break;
            }
            Err(other) => panic!("expected CapacityExhausted, got {other:?}"),
        }
    }
    assert!(hit_wall, "device never reported CapacityExhausted");
    drop(db);

    // The failed allocation left no durable manifest referencing the phantom
    // pages, so reopen recovers the pre-exhaustion state cleanly (no brick).
    let db = Db::open_on_device_with_faults(
        device_cfg(&dir),
        FaultController::disabled(),
        page_dev,
        journal_dev,
    )
    .unwrap();
    assert!(
        db.manifest()
            .volumes
            .iter()
            .any(|e| e.ord == BOOTSTRAP_VOLUME_ORD)
    );
}

/// 3d MUST-FIX regression: a crash back to an older manifest generation must not
/// let the allocator re-hand-out a page the (generation-stable, in-place) cuckoo
/// dedup meta chain still references above that generation's `page_high_water`.
///
/// Setup: flush → gen N (`page_high_water = H_N`). Then put more dedup entries
/// (allocating cuckoo pages `P > H_N`) and durably sync ONLY the dedup meta +
/// page content (`flush_meta` + `page_store.sync`) — NOT a fresh manifest. Drop
/// without a flush (crash). On reopen the manifest is still gen N (`H_N`), but
/// the dedup index loads the synced-in-place chain referencing `P`. The device
/// open MUST lift the bounded-scan frontier to `max(H_N, dedup_max+1)` so `P`
/// is marked live and never re-allocated.
#[test]
fn device_open_lifts_frontier_past_dedup_pages() {
    let dir = TempDir::new().unwrap();
    let (page_dev, journal_dev, db) = mk_db_on_device(&dir, 8192, 512);

    // Gen N: a first cohort of dedup entries, made fully durable + covered by
    // the manifest.
    for n in 1u64..30 {
        db.put_dedup(h(n), dv(n as u8)).unwrap();
    }
    db.flush().unwrap();
    let high_water_n = db.manifest().page_high_water;
    assert!(high_water_n > FIRST_DATA_PAGE);

    // Between-flush cohort: allocate more cuckoo pages ABOVE H_N. Distinct
    // fingerprints force fresh bucket/data pages.
    for n in 200u64..400 {
        db.put_dedup(hash_full(n, n.wrapping_mul(7)), dv((n % 250) as u8))
            .unwrap();
    }
    // Make the dedup meta chain (referencing the new pages) + page content
    // durable, but do NOT commit a new manifest — this is exactly the window a
    // crash between `page_store.sync()` and the gen N+1 manifest slot exposes.
    db.dedup_index.flush_meta().unwrap();
    db.page_store.sync().unwrap();

    // The bug condition: the durable (gen N) manifest does NOT cover the
    // between-flush dedup pages — the dedup index references a page id ABOVE
    // `page_high_water`. (The dedup meta chain head is generation-stable + was
    // rewritten in place + synced, so gen N's manifest reaches it.)
    let dedup_max = db.dedup_index.max_referenced_page_id();
    assert_eq!(
        db.manifest().page_high_water,
        high_water_n,
        "manifest advanced unexpectedly"
    );
    assert!(
        dedup_max > high_water_n,
        "test precondition not met: dedup_max {dedup_max} must exceed gen-N page_high_water {high_water_n}"
    );

    // Crash: drop without a flush. Manifest stays at gen N.
    drop(db);

    let db = Db::open_on_device_with_faults(
        device_cfg(&dir),
        FaultController::disabled(),
        page_dev,
        journal_dev,
    )
    .unwrap();

    // THE FIX: the bounded scan lifted its frontier to
    // `max(page_high_water, dedup.max_referenced_page_id()+1)`, so every
    // dedup-referenced page is marked live and the allocator grows ABOVE them.
    // Without the ceiling the scan would stop at H_N (<= dedup_max) and the next
    // allocation would re-hand-out a page the live dedup index still points at.
    // (The reopen then re-commits the manifest at this lifted high_water, which
    // is exactly the self-heal we want — so we assert on the recovered
    // high_water, not the manifest value.)
    let recovered_dedup_max = db.dedup_index.max_referenced_page_id();
    assert!(
        db.page_store.high_water() > recovered_dedup_max,
        "device open failed to lift frontier past dedup pages: high_water {} <= dedup_max {}",
        db.page_store.high_water(),
        recovered_dedup_max
    );
    // Dedup-referenced pages are typed, so the bounded scan marks them live (not
    // free) and no allocation can alias one. Drain the reusable free list and
    // confirm it holds none of the dedup data pages. (Free pages BELOW
    // `dedup_max` that are not dedup-referenced — e.g. superseded COW pages —
    // are legitimately reusable, so the bound is per-page, not `> dedup_max`.)
    let dedup_pages: std::collections::HashSet<PageId> =
        db.dedup_index.data_page_ids().into_iter().collect();
    for _ in 0..db.page_store.free_list_len() + 8 {
        let p = db.page_store.allocate().unwrap();
        assert!(
            !dedup_pages.contains(&p),
            "allocator handed out live dedup data page {p}"
        );
    }
}

/// A lifecycle op (Discard) journaled through the ring but NOT yet covered by a
/// manifest commit survives a crash + reopen: the ring replay re-applies the
/// range delete, the same way segment-file replay does on the file path.
#[test]
fn device_ring_lifecycle_discard_replays() {
    let dir = TempDir::new().unwrap();
    let (page_dev, journal_dev, db) = mk_db_on_device(&dir, 8192, 512);

    // Durable baseline: map [0, 64) on the bootstrap volume, flush.
    for i in 0u64..64 {
        db.insert(0, i, v(1)).unwrap();
    }
    db.flush().unwrap();

    // Range-delete [10, 20). This appends a Discard record to the ring but does
    // NOT force a flush — so the deletion is durable only via the ring.
    db.range_delete(0, 10, 20).unwrap();
    assert_eq!(db.get(0, 15).unwrap(), None, "range not deleted pre-crash");
    assert_eq!(db.get(0, 5).unwrap(), Some(v(1)), "unrelated lba disturbed");
    drop(db);

    // Reopen: the manifest still maps [10, 20) (no flush covered the discard),
    // so recovery MUST replay the ring's Discard record to re-delete them.
    let db = Db::open_on_device_with_faults(
        device_cfg(&dir),
        FaultController::disabled(),
        page_dev,
        journal_dev,
    )
    .unwrap();
    assert_eq!(db.get(0, 15).unwrap(), None, "ring Discard did not replay");
    assert_eq!(db.get(0, 5).unwrap(), Some(v(1)), "replay deleted too much");
    assert_eq!(
        db.get(0, 25).unwrap(),
        Some(v(1)),
        "replay deleted too much (tail)"
    );
}

/// Vec-backed [`PageBlockIo`] — the byte-level seam onyx implements over a
/// chunklet meta LD window. Exercises the `BlockPageDevice` page-framing path
/// (the onyx-facing production wiring) without a real LD.
struct MemBlockIo {
    bytes: parking_lot::Mutex<Vec<u8>>,
}

impl MemBlockIo {
    fn new(capacity_bytes: usize) -> Self {
        Self {
            bytes: parking_lot::Mutex::new(vec![0u8; capacity_bytes]),
        }
    }
}

impl crate::page_store::PageBlockIo for MemBlockIo {
    fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let off = offset as usize;
        let bytes = self.bytes.lock();
        buf.copy_from_slice(&bytes[off..off + buf.len()]);
        Ok(())
    }
    fn write_at(&self, offset: u64, buf: &[u8]) -> Result<()> {
        let off = offset as usize;
        let mut bytes = self.bytes.lock();
        bytes[off..off + buf.len()].copy_from_slice(buf);
        Ok(())
    }
    fn flush(&self) -> Result<()> {
        Ok(())
    }
    fn capacity_bytes(&self) -> u64 {
        self.bytes.lock().len() as u64
    }
}

/// The full onyx-facing production path: a byte-level `PageBlockIo` wrapped by
/// `BlockPageDevice` (which owns Page framing + verification) drives a `Db`
/// through create → write → flush → reopen. This is exactly what onyx's
/// `MetaWindow` → `BlockPageDevice` wiring does over a chunklet LD.
#[test]
fn block_page_device_roundtrip_through_db() {
    use crate::page_store::BlockPageDevice;
    let dir = TempDir::new().unwrap();
    let page_io = Arc::new(MemBlockIo::new(8192 * crate::config::PAGE_SIZE));
    let journal_dev = Arc::new(MemJournalDevice::new(512));
    let page_dev: Arc<dyn crate::page_store::PageDevice> =
        Arc::new(BlockPageDevice::new(page_io.clone()).unwrap());

    let db = Db::create_on_device(device_cfg(&dir), page_dev.clone(), journal_dev.clone()).unwrap();
    for i in 0u64..200 {
        db.insert(0, i, v((i / 16) as u8)).unwrap();
    }
    for n in 1u64..25 {
        db.put_dedup(h(n), dv(n as u8)).unwrap();
    }
    db.flush().unwrap();
    drop(db);

    // Reopen over the same byte window (Arc kept alive).
    let page_dev: Arc<dyn crate::page_store::PageDevice> =
        Arc::new(BlockPageDevice::new(page_io).unwrap());
    let db = Db::open_on_device(device_cfg(&dir), page_dev, journal_dev).unwrap();
    for i in 0u64..200 {
        assert_eq!(
            db.get(0, i).unwrap(),
            Some(v((i / 16) as u8)),
            "reopen lba {i}"
        );
    }
    for n in 1u64..25 {
        assert_eq!(
            db.get_dedup(&h(n)).unwrap(),
            Some(dv(n as u8)),
            "reopen dedup {n}"
        );
    }
}

/// Reclaim `n` interior free pages into the page-store free list without letting
/// `truncate_free_tail` pull them back off `high_water`: allocate `n + 1`
/// scratch pages, keep the topmost one allocated (so the tail is not free), then
/// free + reclaim the rest. Returns the freed pids.
fn make_interior_free_pages(db: &Db, n: usize) -> Vec<PageId> {
    let mut scratch = Vec::with_capacity(n + 1);
    for _ in 0..n + 1 {
        scratch.push(db.page_store.allocate().unwrap());
    }
    let _keep_tail = scratch.pop().unwrap(); // topmost stays allocated
    for &pid in &scratch {
        db.page_store.free(pid, 1).unwrap();
    }
    db.page_store.try_reclaim().unwrap();
    scratch
}

/// The persisted free-list bitmap replaces the open-time page scan: a device
/// flush writes `free_list_head`, and reopen recovers `high_water` + the free
/// list from it (no scan), byte-faithfully round-tripping the store's own free
/// list.
#[test]
fn device_persisted_free_list_roundtrips_without_scan() {
    let dir = TempDir::new().unwrap();
    let (page_dev, journal_dev, db) = mk_db_on_device(&dir, 8192, 512);

    for i in 0u64..300 {
        db.insert(0, i, v((i / 32) as u8)).unwrap();
    }
    for n in 1u64..40 {
        db.put_dedup(h(n), dv(n as u8)).unwrap();
    }
    // Interior free pages so the persisted bitmap carries a non-empty free list.
    make_interior_free_pages(&db, 200);
    db.flush().unwrap();

    let hw = db.page_store.high_water();
    let fll = db.page_store.free_list_len();
    assert!(fll > 0, "test setup produced no persisted free pages");
    assert_ne!(
        db.manifest().free_list_head,
        crate::types::NULL_PAGE,
        "device flush must persist a free-list bitmap head"
    );
    drop(db);

    let db = Db::open_on_device_with_faults(
        device_cfg(&dir),
        FaultController::disabled(),
        page_dev,
        journal_dev,
    )
    .unwrap();

    // Fast path (no scan) recovers high_water exactly, and a free list that is a
    // safe snapshot: non-empty and never larger than the store held at close
    // (the bitmap never invents free pages; it may omit the few pages the flush
    // reclaimed after its commit snapshot — see `ManifestStore::commit`).
    assert_eq!(
        db.page_store.high_water(),
        hw,
        "high_water regressed on bitmap reopen"
    );
    let reopened_fll = db.page_store.free_list_len();
    assert!(
        reopened_fll > 0 && reopened_fll <= fll,
        "bitmap-recovered free_list_len {reopened_fll} not in (0, {fll}]"
    );
    for i in 0u64..300 {
        assert_eq!(
            db.get(0, i).unwrap(),
            Some(v((i / 32) as u8)),
            "reopen lba {i}"
        );
    }
    for n in 1u64..40 {
        assert_eq!(
            db.get_dedup(&h(n)).unwrap(),
            Some(dv(n as u8)),
            "reopen dedup {n}"
        );
    }

    // Every recovered free page is genuinely reusable: draining the free list
    // hands out only pages below high_water and no live dedup page.
    let dedup_pages: std::collections::HashSet<PageId> =
        db.dedup_index.referenced_page_ids().into_iter().collect();
    let base_hw = db.page_store.high_water();
    for _ in 0..reopened_fll {
        let p = db.page_store.allocate().unwrap();
        assert!(
            p >= FIRST_DATA_PAGE && p < base_hw,
            "handed out out-of-range page {p}"
        );
        assert!(!dedup_pages.contains(&p), "handed out live dedup page {p}");
    }
}

/// The bitmap decode is a deterministic fixed point: two consecutive reopens of
/// the same on-disk bitmap (no flush between, so the bytes never change) recover
/// an identical free list — i.e. the bitmap round-trips its own content exactly.
#[test]
fn device_persisted_free_list_reopen_is_a_fixed_point() {
    let dir = TempDir::new().unwrap();
    let (page_dev, journal_dev, db) = mk_db_on_device(&dir, 8192, 512);
    for i in 0u64..120 {
        db.insert(0, i, v((i / 16) as u8)).unwrap();
    }
    make_interior_free_pages(&db, 128);
    db.flush().unwrap();
    drop(db);

    let db = Db::open_on_device_with_faults(
        device_cfg(&dir),
        FaultController::disabled(),
        page_dev.clone(),
        journal_dev.clone(),
    )
    .unwrap();
    let (hw1, fll1) = (db.page_store.high_water(), db.page_store.free_list_len());
    drop(db); // no flush → on-disk bitmap unchanged

    let db = Db::open_on_device_with_faults(
        device_cfg(&dir),
        FaultController::disabled(),
        page_dev,
        journal_dev,
    )
    .unwrap();
    assert_eq!(
        db.page_store.high_water(),
        hw1,
        "high_water not a fixed point"
    );
    assert_eq!(
        db.page_store.free_list_len(),
        fll1,
        "free list not a fixed point"
    );
}

/// The bitmap run relocates (geometric grow) when the free list outgrows its
/// reserve, and the batched reopen still recovers it. Push high_water past a
/// data-page boundary (every ~32 128 pages) between two flushes so the second
/// commit takes the relocation branch, then reopen and confirm integrity.
#[test]
fn device_persisted_free_list_run_relocates_on_growth() {
    let dir = TempDir::new().unwrap();
    // ~70 000 pages ≈ 287 MiB MemDevice — room to push high_water past a
    // data-page boundary (~32 128 pages) with headroom for the run + reserve.
    let page_dev = Arc::new(MemDevice::new(70_000));
    let journal_dev = Arc::new(MemJournalDevice::new(512));
    let db = Db::create_on_device_with_faults(
        device_cfg(&dir),
        FaultController::disabled(),
        page_dev.clone(),
        journal_dev.clone(),
    )
    .unwrap();

    for i in 0u64..64 {
        db.insert(0, i, v(1)).unwrap();
    }
    db.flush().unwrap(); // commit 1: small run capacity

    // Grow high_water past a data-page boundary → forces the relocation branch.
    make_interior_free_pages(&db, 62_000);
    db.flush().unwrap(); // commit 2: run relocates + grows

    let hw = db.page_store.high_water();
    let fll = db.page_store.free_list_len();
    assert!(fll > 30_000, "expected a large free list, got {fll}");
    drop(db);

    let db = Db::open_on_device_with_faults(
        device_cfg(&dir),
        FaultController::disabled(),
        page_dev,
        journal_dev,
    )
    .unwrap();
    assert_eq!(
        db.page_store.high_water(),
        hw,
        "high_water regressed after relocation reopen"
    );
    assert!(db.page_store.free_list_len() <= fll && db.page_store.free_list_len() > 30_000);
    for i in 0u64..64 {
        assert_eq!(
            db.get(0, i).unwrap(),
            Some(v(1)),
            "reopen lba {i} after relocation"
        );
    }
}

/// The persisted-free-list fast path must preserve the dedup-frontier invariant
/// through the reconciliation (not the bounded scan). Setup: gen N persists a
/// bitmap that includes interior free pages, THEN an uncommitted newer
/// generation's dedup growth pops those interior pages (`P < page_high_water`)
/// and makes them durable without a manifest commit. On reopen the gen-N bitmap
/// still marks `P` free — the fix removes every live-dedup-referenced page from
/// the loaded free list, so the allocator can never re-hand-out `P`.
#[test]
fn device_persisted_free_list_preserves_dedup_frontier() {
    let dir = TempDir::new().unwrap();
    let (page_dev, journal_dev, db) = mk_db_on_device(&dir, 8192, 512);

    // Gen N cohort + a pool of interior free pages, all persisted in the bitmap.
    for n in 1u64..30 {
        db.put_dedup(h(n), dv(n as u8)).unwrap();
    }
    make_interior_free_pages(&db, 200);
    db.flush().unwrap();
    let high_water_n = db.manifest().page_high_water;
    assert_ne!(db.manifest().free_list_head, crate::types::NULL_PAGE);
    let free_before = db.page_store.free_list_len();
    assert!(free_before > 0);

    // Between-flush cohort: fresh fingerprints allocate cuckoo pages, popping the
    // interior free pages (all `< high_water_n`) the gen-N bitmap marks free.
    for n in 200u64..400 {
        db.put_dedup(hash_full(n, n.wrapping_mul(7)), dv((n % 250) as u8))
            .unwrap();
    }
    db.dedup_index.flush_meta().unwrap();
    db.page_store.sync().unwrap();

    // Precondition: dedup now references at least one page BELOW gen-N's
    // high-water — i.e. an interior page the persisted bitmap still lists free.
    let interior_dedup = db
        .dedup_index
        .referenced_page_ids()
        .into_iter()
        .any(|p| p < high_water_n);
    assert!(
        interior_dedup,
        "test precondition not met: no dedup page landed below gen-N high_water {high_water_n} \
         (free pool exhausted?)"
    );
    assert_eq!(
        db.manifest().page_high_water,
        high_water_n,
        "manifest advanced unexpectedly"
    );

    // Crash: manifest stays at gen N (with the gen-N bitmap).
    drop(db);

    let db = Db::open_on_device_with_faults(
        device_cfg(&dir),
        FaultController::disabled(),
        page_dev,
        journal_dev,
    )
    .unwrap();

    // The reconciliation dropped every live dedup page from the loaded free list,
    // so no allocation aliases a page the dedup index still points at — including
    // the interior pages the gen-N bitmap wrongly marked free.
    let dedup_pages: std::collections::HashSet<PageId> =
        db.dedup_index.referenced_page_ids().into_iter().collect();
    assert!(
        dedup_pages.iter().any(|&p| p < high_water_n),
        "recovered dedup index lost its interior references"
    );
    for _ in 0..db.page_store.free_list_len() + 8 {
        let p = db.page_store.allocate().unwrap();
        assert!(
            !dedup_pages.contains(&p),
            "allocator handed out live dedup page {p} after bitmap reopen"
        );
    }
}

/// A free-list run owns its unwritten reserve in both manifest slots. An older
/// buggy open could nevertheless install a stale bitmap bit for one reserve
/// page and Free-stamp it. The verifier may tolerate that exact harmless form,
/// but reopen must remove the owned pid before installing the allocator list.
#[test]
fn device_reconciles_and_verifies_free_list_reserve() {
    let dir = TempDir::new().unwrap();
    let (page_dev, journal_dev, db) = mk_db_on_device(&dir, 8192, 512);
    db.flush().unwrap();

    let loaded = crate::manifest::ManifestStore::load_latest(&db.page_store)
        .unwrap()
        .unwrap();
    let run = crate::manifest::catalog::free_list_run_pids(
        &db.page_store,
        loaded.manifest.free_list_head,
    )
    .unwrap();
    let reserve = crate::manifest::catalog::free_list_reserve_pids(
        &db.page_store,
        loaded.manifest.free_list_head,
    )
    .unwrap();
    assert!(!reserve.is_empty(), "test needs free-list growth reserve");
    let reserve_pid = reserve[0];

    // Fabricate the stale persisted bit without changing runtime ownership.
    let mut bitmap = crate::manifest::catalog::read_free_list_run(
        &db.page_store,
        loaded.manifest.free_list_head,
    )
    .unwrap();
    let bit = (reserve_pid - FIRST_DATA_PAGE) as usize;
    bitmap[bit / 8] |= 1 << (bit % 8);
    let sealed =
        crate::manifest::catalog::seal_free_list_run(&run, &bitmap, loaded.manifest.checkpoint_lsn)
            .unwrap();
    db.page_store.write_sealed_page_runs(sealed).unwrap();
    db.page_store.sync().unwrap();

    // Reproduce the on-disk legacy symptom directly. Keep the current runtime
    // allocator untouched so only reopen observes the stale persisted bit.
    let mut free_page = crate::page::Page::new(crate::page::PageHeader::new(
        PageType::Free,
        loaded.manifest.checkpoint_lsn,
    ));
    free_page.seal();
    db.page_store.write_page(reserve_pid, &free_page).unwrap();
    db.page_store.sync().unwrap();
    assert_eq!(
        db.page_store
            .read_page_unchecked(reserve_pid)
            .unwrap()
            .header()
            .unwrap()
            .page_type,
        PageType::Free
    );
    let report = db
        .verify(crate::verify::VerifyOptions {
            strict: true,
            ..crate::verify::VerifyOptions::default()
        })
        .unwrap();
    assert!(
        report.is_clean(),
        "free-list reserve Free page should be tolerated: {:?}",
        report.issues
    );
    drop(db);

    let db = Db::open_on_device_with_faults(
        device_cfg(&dir),
        FaultController::disabled(),
        page_dev,
        journal_dev,
    )
    .unwrap();
    let protected: std::collections::HashSet<PageId> =
        crate::manifest::catalog_chain_pids_all_slots(&db.page_store)
            .into_iter()
            .collect();
    assert!(protected.contains(&reserve_pid));
    for _ in 0..db.page_store.free_list_len() + 8 {
        let pid = db.page_store.allocate().unwrap();
        assert!(
            !protected.contains(&pid),
            "allocator handed out manifest-owned page {pid}"
        );
    }

    // The exception is scoped to reserve pids: a Free-stamped RC root remains
    // a verifier failure rather than inheriting generic zero/Free tolerance.
    let rc_root = db.manifest().refcount_shard_roots[0];
    db.page_store.free(rc_root, 1).unwrap();
    db.page_store.try_reclaim().unwrap();
    let report = db.verify(crate::verify::VerifyOptions::default()).unwrap();
    assert!(
        !report.is_clean(),
        "non-reserve live Free page was incorrectly tolerated"
    );
}

/// Refcount's meta head is generation-stable and updated in place. New RC data
/// or continuation pages can therefore become durable before the manifest that
/// advances `page_high_water`: reopen must protect both pages an older bitmap
/// still marks free and pages above that manifest frontier.
#[test]
fn device_persisted_free_list_reconciles_refcount_stable_head() {
    let dir = TempDir::new().unwrap();
    let (page_dev, journal_dev, db) = mk_db_on_device(&dir, 8192, 512);
    make_interior_free_pages(&db, 200);
    db.refcount_shards[0]
        .rc
        .stage_unskippable(0, 0, 1, 1)
        .unwrap();
    db.flush().unwrap();

    let manifest_n = crate::manifest::ManifestStore::load_latest(&db.page_store)
        .unwrap()
        .unwrap()
        .manifest;
    let high_water_n = manifest_n.page_high_water;
    // Persist the current runtime free set as generation N. Normal flush takes
    // its snapshot before post-publish reclaim, so construct the same older-
    // bitmap crash window explicitly and deterministically for this test.
    let (bitmap_high_water, bitmap) = db.page_store.snapshot_free_bitmap_and_high_water();
    assert_eq!(bitmap_high_water, high_water_n);
    let run =
        crate::manifest::catalog::free_list_run_pids(&db.page_store, manifest_n.free_list_head)
            .unwrap();
    let sealed =
        crate::manifest::catalog::seal_free_list_run(&run, &bitmap, manifest_n.checkpoint_lsn)
            .unwrap();
    db.page_store.write_sealed_page_runs(sealed).unwrap();
    db.page_store.sync().unwrap();
    let persisted_free: std::collections::HashSet<PageId> =
        crate::manifest::catalog::decode_free_list_bitmap(&bitmap, high_water_n)
            .into_iter()
            .collect();
    assert!(!persisted_free.is_empty());

    let rc = &db.refcount_shards[0].rc;
    let root = manifest_n.refcount_shard_roots[0];
    let before: std::collections::HashSet<PageId> =
        crate::refcount::PagedRefcountArray::referenced_page_ids(&db.page_store, root)
            .unwrap()
            .into_iter()
            .collect();

    // First stable-head rewrite consumes an interior pid the gen-N bitmap still
    // marks free.
    let pba_interior = crate::refcount::ENTRIES_PER_PAGE as Pba;
    rc.stage_unskippable(0, pba_interior, 1, 2).unwrap();
    rc.flush().unwrap();
    let after_interior: std::collections::HashSet<PageId> =
        crate::refcount::PagedRefcountArray::referenced_page_ids(&db.page_store, root)
            .unwrap()
            .into_iter()
            .collect();
    assert!(
        after_interior
            .difference(&before)
            .any(|pid| persisted_free.contains(pid)),
        "RC update did not consume an interior page from the persisted bitmap"
    );

    // Exhaust runtime reusable pages, then force a continuation + data page at
    // the frontier without publishing a newer manifest.
    while db.page_store.free_list_len() > 0 {
        let _ = db.page_store.allocate().unwrap();
    }
    let page_idx = crate::paged_meta::head_capacity(0) + 1;
    let pba_frontier = (page_idx * crate::refcount::ENTRIES_PER_PAGE) as Pba;
    rc.stage_unskippable(0, pba_frontier, 1, 3).unwrap();
    rc.flush().unwrap();
    db.page_store.sync().unwrap();
    let durable_refs =
        crate::refcount::PagedRefcountArray::referenced_page_ids(&db.page_store, root).unwrap();
    assert!(
        durable_refs.iter().any(|pid| *pid >= high_water_n),
        "RC update did not grow past gen-N high-water {high_water_n}"
    );
    assert_eq!(
        crate::manifest::ManifestStore::load_latest(&db.page_store)
            .unwrap()
            .unwrap()
            .manifest
            .page_high_water,
        high_water_n
    );
    drop(db);

    let db = Db::open_on_device_with_faults(
        device_cfg(&dir),
        FaultController::disabled(),
        page_dev,
        journal_dev,
    )
    .unwrap();
    assert_eq!(db.refcount_shards[0].rc.get(pba_interior).unwrap(), 1);
    assert_eq!(db.refcount_shards[0].rc.get(pba_frontier).unwrap(), 1);
    let reopened_refs: std::collections::HashSet<PageId> =
        crate::refcount::PagedRefcountArray::referenced_page_ids(
            &db.page_store,
            db.manifest().refcount_shard_roots[0],
        )
        .unwrap()
        .into_iter()
        .collect();
    let max_ref = reopened_refs.iter().copied().max().unwrap();
    assert!(db.page_store.high_water() > max_ref);
    for _ in 0..db.page_store.free_list_len() + 8 {
        let pid = db.page_store.allocate().unwrap();
        assert!(
            !reopened_refs.contains(&pid),
            "allocator handed out live refcount page {pid}"
        );
    }
}

/// Zero regression on the file backend: it keeps its EOF-bounded open scan and
/// never persists a free-list bitmap, so `free_list_head` stays `NULL_PAGE`.
#[test]
fn file_backend_never_persists_free_list_head() {
    let dir = TempDir::new().unwrap();
    let db = Db::create_with_config(device_cfg(&dir)).unwrap();
    for i in 0u64..128 {
        db.insert(0, i, v(1)).unwrap();
    }
    db.flush().unwrap();
    assert_eq!(
        db.manifest().free_list_head,
        crate::types::NULL_PAGE,
        "file backend must not persist a free-list bitmap"
    );
    drop(db);

    let db = Db::open_with_config(device_cfg(&dir)).unwrap();
    assert_eq!(db.manifest().free_list_head, crate::types::NULL_PAGE);
    for i in 0u64..128 {
        assert_eq!(db.get(0, i).unwrap(), Some(v(1)), "file reopen lba {i}");
    }
}

/// `--ignored` fault-injection: a free-list-bitmap commit that fails at the
/// manifest fsync (after the bitmap chain is durable) must not corrupt free-list
/// recovery — the DB reopens and the dedup-frontier invariant still holds.
#[test]
#[ignore]
fn device_free_list_commit_fault_reopens_consistent() {
    let dir = TempDir::new().unwrap();
    let page_dev = Arc::new(MemDevice::new(8192));
    let journal_dev = Arc::new(MemJournalDevice::new(512));
    let faults = FaultController::new();
    let db = Db::create_on_device_with_faults(
        device_cfg(&dir),
        faults.clone(),
        page_dev.clone(),
        journal_dev.clone(),
    )
    .unwrap();

    for n in 1u64..30 {
        db.put_dedup(h(n), dv(n as u8)).unwrap();
    }
    make_interior_free_pages(&db, 200);
    db.flush().unwrap();

    // Arm a failure at the manifest fsync of the NEXT commit (after the bitmap
    // chain has been written + synced).
    faults.install(
        crate::testing::faults::FaultPoint::ManifestFsyncBefore,
        1,
        crate::testing::faults::FaultAction::Error,
    );
    for n in 200u64..260 {
        db.put_dedup(hash_full(n, n.wrapping_mul(7)), dv((n % 250) as u8))
            .ok();
    }
    let _ = db.flush(); // expected to error at the injected fault
    faults.clear();
    drop(db);

    let db = Db::open_on_device_with_faults(
        device_cfg(&dir),
        FaultController::disabled(),
        page_dev,
        journal_dev,
    )
    .unwrap();
    // Whichever generation won, no allocation may alias a live dedup page.
    let dedup_pages: std::collections::HashSet<PageId> =
        db.dedup_index.referenced_page_ids().into_iter().collect();
    for _ in 0..db.page_store.free_list_len() + 8 {
        let p = db.page_store.allocate().unwrap();
        assert!(
            !dedup_pages.contains(&p),
            "handed out live dedup page {p} after fault reopen"
        );
    }
    for n in 1u64..30 {
        assert_eq!(
            db.get_dedup(&h(n)).unwrap(),
            Some(dv(n as u8)),
            "gen-N dedup {n} lost"
        );
    }
}

/// `verify::verify_page_store` (the device-generic counterpart of
/// `verify_path`, added so a chunklet-backed metadb instance can be audited
/// offline the same way a plain-file one can via `metadb-verify`) must work
/// against a `MemDevice`-backed store exactly like it does against a file —
/// `PageStore` only ever sees the device through the `PageDevice` trait, so
/// there is nothing file-specific left for this check to exercise.
#[test]
fn device_verify_page_store_reports_clean() {
    let dir = TempDir::new().unwrap();
    let (page_dev, journal_dev, db) = mk_db_on_device(&dir, 8192, 512);

    for i in 0u64..300 {
        db.insert(0, i, v((i / 32) as u8)).unwrap();
    }
    for n in 1u64..40 {
        db.put_dedup(h(n), dv(n as u8)).unwrap();
    }
    db.flush().unwrap();
    drop(db);

    // Reopen over the SAME device bytes through the normal recovery path
    // (WAL replay + frontier reconciliation) — an offline audit must see
    // exactly the state a live engine would recover to, not a hand-rolled
    // reopen that skips generation/frontier bookkeeping. `Db::verify` then
    // runs the same reachability scan `verify_path` runs for the file
    // backend, just against the already-open `Db`.
    let db = Db::open_on_device_with_faults(
        device_cfg(&dir),
        FaultController::disabled(),
        page_dev,
        journal_dev,
    )
    .unwrap();
    let report = db.verify(crate::verify::VerifyOptions::default()).unwrap();
    assert!(report.is_clean(), "unexpected issues: {:?}", report.issues);
    assert!(
        report.orphan_pages.is_empty(),
        "unexpected orphans: {:?}",
        report.orphan_pages
    );
    assert!(report.high_water > FIRST_DATA_PAGE);
}
