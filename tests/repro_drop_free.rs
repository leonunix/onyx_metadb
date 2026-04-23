//! Regression: successive `clone_volume` calls on the same snapshot used
//! to leave the first clone's `PagedL2p::PageBuf` with a stale Clean copy
//! of the shared root, because `apply_clone_volume_incref` only
//! invalidated the source volume's PageBuf. A later
//! `take_snapshot`/`cow_for_write` on the first clone would then flush
//! the stale rc back over the disk-direct bump, losing one refcount per
//! subsequent clone. Eventually `drop_volume` on one of the clones
//! decrements rc to 0 and frees a page the surviving clone still points
//! at — reopen then fails with
//! `paged format: expected PagedLeaf/PagedIndex, got Free`.

use onyx_metadb::{Db, L2pValue};
use tempfile::TempDir;

fn v(n: u8) -> L2pValue {
    let mut x = [0u8; 28];
    x[0] = n;
    L2pValue(x)
}

#[test]
fn clone_snapshot_take_drop_then_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();

    {
        let db = Db::create(path).unwrap();
        let s1 = db.take_snapshot(0).unwrap();
        let c1 = db.clone_volume(s1).unwrap();
        let _c2 = db.clone_volume(s1).unwrap();
        let s2 = db.take_snapshot(c1).unwrap();
        db.drop_snapshot(s1).unwrap().unwrap();
        db.drop_snapshot(s2).unwrap().unwrap();
        db.insert(0, 0, v(0)).unwrap();
        db.insert(0, 0, v(0)).unwrap();
        db.drop_volume(c1).unwrap().unwrap();
        db.flush().unwrap();
    }

    Db::open(path).expect("reopen should succeed after drop_volume on a clone");
}
