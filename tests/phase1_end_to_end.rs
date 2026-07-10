//! end-to-end: page store + manifest reopen round-trip.
//!
//! The original fixture also wove the WAL into the flow, but
//! the buffer-as-sole-journal refactor (WAL-free recovery) retired the metadb
//! WAL — durability for data-plane ops lives in the LV2 buffer and
//! durability for lifecycle ops lives in `lifecycle_log/`. What's
//! retained here is the persistence layer that survives both refactors:
//! page-store-backed manifest commit + reload.

use std::sync::Arc;

use onyx_metadb::cache::PageCache;
use onyx_metadb::manifest::{Manifest, ManifestStore};
use onyx_metadb::page_store::PageStore;
use onyx_metadb::testing::faults::FaultController;
use tempfile::TempDir;

fn cache(ps: &Arc<PageStore>) -> Arc<PageCache> {
    Arc::new(PageCache::new(ps.clone(), 8 * 1024 * 1024))
}

#[test]
fn manifest_survives_page_store_reopen() {
    let dir = TempDir::new().unwrap();
    let pages = dir.path().join("pages.onyx_meta");

    // Commit a couple of manifest versions.
    {
        let ps = Arc::new(PageStore::create(&pages).unwrap());
        let faults = FaultController::new();
        let (mut mstore, _) =
            ManifestStore::open_or_create(ps.clone(), cache(&ps), faults).unwrap();
        for lsn in [42u64, 77, 123] {
            let mut m = Manifest::empty();
            m.checkpoint_lsn = lsn;
            mstore.commit(&mut m).unwrap();
        }
    }

    // Reopen and verify the freshest manifest is loaded.
    let ps = Arc::new(PageStore::open(&pages).unwrap());
    let faults = FaultController::new();
    let (mstore, manifest) = ManifestStore::open_or_create(ps.clone(), cache(&ps), faults).unwrap();
    assert_eq!(manifest.checkpoint_lsn, 123);
    // We wrote 4 times total (1 fresh + 3 commits), so sequence=4.
    assert_eq!(mstore.sequence(), 4);
}
