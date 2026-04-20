//! Phase 1 end-to-end: page store + manifest + WAL + replay woven
//! together. No indexes yet, but every piece of durable infrastructure
//! is exercised in one flow that survives a process boundary.

use std::sync::Arc;

use onyx_metadb::config::Config;
use onyx_metadb::manifest::{Manifest, ManifestStore};
use onyx_metadb::page_store::PageStore;
use onyx_metadb::recovery::replay;
use onyx_metadb::testing::faults::{FaultAction, FaultController, FaultPoint};
use onyx_metadb::types::NULL_PAGE;
use onyx_metadb::wal::Wal;
use tempfile::TempDir;

fn cfg() -> Config {
    let mut c = Config::new("unused");
    c.group_commit_timeout_us = 50;
    c.wal_segment_bytes = 4096;
    c
}

struct Db {
    #[allow(dead_code)]
    page_store: Arc<PageStore>,
    manifest_store: ManifestStore,
    wal: Wal,
    faults: Arc<FaultController>,
}

impl Db {
    fn create(dir: &std::path::Path) -> Self {
        std::fs::create_dir_all(dir.join("wal")).unwrap();
        let page_store = Arc::new(PageStore::create(dir.join("pages.onyx_meta")).unwrap());
        let faults = FaultController::new();
        let (manifest_store, _) =
            ManifestStore::open_or_create(page_store.clone(), faults.clone()).unwrap();
        let start_lsn = 1; // fresh DB always starts at 1
        let wal = Wal::create(&dir.join("wal"), &cfg(), start_lsn, faults.clone()).unwrap();
        Self {
            page_store,
            manifest_store,
            wal,
            faults,
        }
    }

    fn open(dir: &std::path::Path) -> (Self, onyx_metadb::recovery::ReplayOutcome, Manifest) {
        let page_store = Arc::new(PageStore::open(dir.join("pages.onyx_meta")).unwrap());
        let faults = FaultController::new();
        let (manifest_store, manifest) =
            ManifestStore::open_or_create(page_store.clone(), faults.clone()).unwrap();
        let outcome = replay(&dir.join("wal"), manifest.checkpoint_lsn + 1).unwrap();
        // Continue writing at last_lsn+1 (or from_lsn if nothing
        // replayed).
        let start_lsn = outcome
            .last_lsn
            .map(|l| l + 1)
            .unwrap_or(manifest.checkpoint_lsn + 1);
        let wal = Wal::create(&dir.join("wal"), &cfg(), start_lsn, faults.clone()).unwrap();
        (
            Self {
                page_store,
                manifest_store,
                wal,
                faults,
            },
            outcome,
            manifest,
        )
    }

    fn shutdown(self) {
        self.wal.shutdown().unwrap();
    }
}

#[test]
fn fresh_to_replay_round_trip() {
    let dir = TempDir::new().unwrap();

    // Phase A: fresh DB. Write 20 records, checkpoint at LSN 10,
    // write 10 more records, shutdown cleanly.
    {
        let mut db = Db::create(dir.path());
        for i in 0..20u64 {
            let lsn = db.wal.submit(format!("{i}").into_bytes()).unwrap();
            assert_eq!(lsn, i + 1);
        }
        db.manifest_store
            .commit(&Manifest {
                body_version: onyx_metadb::manifest::MANIFEST_BODY_VERSION,
                checkpoint_lsn: 10,
                free_list_head: NULL_PAGE,
            })
            .unwrap();
        for i in 20..30u64 {
            let lsn = db.wal.submit(format!("{i}").into_bytes()).unwrap();
            assert_eq!(lsn, i + 1);
        }
        db.shutdown();
    }

    // Phase B: reopen. Manifest restored, replay covers LSNs 11..=30.
    let (db, outcome, manifest) = Db::open(dir.path());
    assert_eq!(manifest.checkpoint_lsn, 10);
    assert_eq!(outcome.first_lsn, Some(11));
    assert_eq!(outcome.last_lsn, Some(30));
    assert_eq!(outcome.record_count, 20);
    assert!(outcome.torn_tail.is_none());

    // New writes continue from LSN 31.
    let lsn = db.wal.submit(b"after-reopen".to_vec()).unwrap();
    assert_eq!(lsn, 31);
    db.shutdown();
}

#[test]
fn fresh_then_crash_mid_write_still_recovers() {
    let dir = TempDir::new().unwrap();

    // Phase A: simulate a crash by making the next fsync panic.
    let _ = std::panic::catch_unwind(|| {
        let db = Db::create(dir.path());
        // Ack a batch so prior content is durable.
        db.wal.submit(b"pre-crash-1".to_vec()).unwrap();
        db.wal.submit(b"pre-crash-2".to_vec()).unwrap();

        // Arm a panic at the next fsync.
        db.faults
            .install(FaultPoint::WalFsyncAfter, 1, FaultAction::Panic);
        let _ = db.wal.submit(b"doomed".to_vec());
        // Unreachable; panic unwinds the writer and possibly this
        // closure depending on thread scheduling.
    });

    // Phase B: reopen. We expect to see pre-crash-1 and pre-crash-2
    // (acked, therefore durable). The "doomed" record may or may not
    // have made it depending on exactly when the panic fired relative
    // to fsync; either way, replay must succeed (possibly with torn
    // tail).
    let (db, outcome, manifest) = Db::open(dir.path());
    assert_eq!(manifest.checkpoint_lsn, 0, "no checkpoint taken yet");
    assert!(
        outcome.record_count >= 2,
        "at least the two acked records must survive, got {}",
        outcome.record_count,
    );
    assert_eq!(outcome.first_lsn, Some(1));

    // WAL is healthy enough to keep writing.
    let lsn = db.wal.submit(b"post-crash".to_vec()).unwrap();
    assert!(
        lsn > outcome.last_lsn.unwrap(),
        "new lsn {} must exceed previous last {}",
        lsn,
        outcome.last_lsn.unwrap(),
    );
    db.shutdown();
}

#[test]
fn manifest_survives_page_store_reopen() {
    let dir = TempDir::new().unwrap();
    let pages = dir.path().join("pages.onyx_meta");

    // Commit a couple of manifest versions.
    {
        let ps = Arc::new(PageStore::create(&pages).unwrap());
        let faults = FaultController::new();
        let (mut mstore, _) = ManifestStore::open_or_create(ps, faults).unwrap();
        for lsn in [42u64, 77, 123] {
            mstore
                .commit(&Manifest {
                    body_version: onyx_metadb::manifest::MANIFEST_BODY_VERSION,
                    checkpoint_lsn: lsn,
                    free_list_head: NULL_PAGE,
                })
                .unwrap();
        }
    }

    // Reopen and verify the freshest manifest is loaded.
    let ps = Arc::new(PageStore::open(&pages).unwrap());
    let faults = FaultController::new();
    let (mstore, manifest) = ManifestStore::open_or_create(ps, faults).unwrap();
    assert_eq!(manifest.checkpoint_lsn, 123);
    // We wrote 4 times total (1 fresh + 3 commits), so sequence=4.
    assert_eq!(mstore.sequence(), 4);
}
