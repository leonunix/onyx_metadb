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
        assert_eq!(db.get(0, i).unwrap(), Some(v((i / 32) as u8)), "reopen lba {i}");
    }
    for n in 1u64..40 {
        assert_eq!(db.get_dedup(&h(n)).unwrap(), Some(dv(n as u8)), "reopen dedup {n}");
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
    assert!(db.manifest().volumes.iter().any(|e| e.ord == BOOTSTRAP_VOLUME_ORD));
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
        db.put_dedup(hash_full(n, n.wrapping_mul(7)), dv((n % 250) as u8)).unwrap();
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
    assert_eq!(db.manifest().page_high_water, high_water_n, "manifest advanced unexpectedly");
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
    assert_eq!(db.get(0, 25).unwrap(), Some(v(1)), "replay deleted too much (tail)");
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
        assert_eq!(db.get(0, i).unwrap(), Some(v((i / 16) as u8)), "reopen lba {i}");
    }
    for n in 1u64..25 {
        assert_eq!(db.get_dedup(&h(n)).unwrap(), Some(dv(n as u8)), "reopen dedup {n}");
    }
}
