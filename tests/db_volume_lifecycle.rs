//! Phase 7 commit 8 integration: per-volume `create_volume` /
//! `drop_volume` lifecycle. Covers create/drop/read isolation, flush +
//! manifest persistence, and WAL-replay recovery without a prior flush.

use onyx_metadb::{Db, L2pValue, VolumeOrdinal};
use tempfile::TempDir;

fn v(n: u8) -> L2pValue {
    let mut x = [0u8; 28];
    x[0] = n;
    L2pValue(x)
}

#[test]
fn create_insert_flush_reopen_preserves_value() {
    let dir = TempDir::new().unwrap();
    let ord;
    {
        let db = Db::create(dir.path()).unwrap();
        ord = db.create_volume().unwrap();
        for i in 0u64..8 {
            db.insert(ord, i, v(i as u8)).unwrap();
        }
        db.flush().unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.volumes(), vec![0, ord]);
    for i in 0u64..8 {
        assert_eq!(db.get(ord, i).unwrap(), Some(v(i as u8)));
    }
}

#[test]
fn create_drop_flush_reopen_sees_only_bootstrap() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        let ord = db.create_volume().unwrap();
        db.insert(ord, 0, v(1)).unwrap();
        db.drop_volume(ord).unwrap().unwrap();
        db.flush().unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.volumes(), vec![0]);
}

#[test]
fn create_insert_reopen_wal_replay() {
    let dir = TempDir::new().unwrap();
    let ord;
    {
        let db = Db::create(dir.path()).unwrap();
        ord = db.create_volume().unwrap();
        db.insert(ord, 42, v(9)).unwrap();
        // No flush — force WAL replay on reopen.
    }
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.volumes(), vec![0, ord]);
    assert_eq!(db.get(ord, 42).unwrap(), Some(v(9)));
    assert_eq!(db.manifest().next_volume_ord, ord + 1);
}

#[test]
fn create_drop_reopen_wal_replay() {
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        let ord = db.create_volume().unwrap();
        db.insert(ord, 1, v(2)).unwrap();
        db.drop_volume(ord).unwrap().unwrap();
        // No flush.
    }
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.volumes(), vec![0]);
    assert!(db.manifest().next_volume_ord >= 2);
}

#[test]
fn dropping_one_of_two_volumes_leaves_the_other_intact() {
    let dir = TempDir::new().unwrap();
    let a = {
        let db = Db::create(dir.path()).unwrap();
        let a = db.create_volume().unwrap();
        let b = db.create_volume().unwrap();
        db.insert(a, 10, v(1)).unwrap();
        db.insert(b, 10, v(2)).unwrap();
        db.drop_volume(a).unwrap().unwrap();
        // ord is monotonic; `b` must still be 2.
        assert_eq!(db.volumes(), vec![0, b]);
        assert_eq!(db.get(b, 10).unwrap(), Some(v(2)));
        db.flush().unwrap();
        b
    };
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.volumes(), vec![0, a]);
    assert_eq!(db.get(a, 10).unwrap(), Some(v(2)));
}

#[test]
fn ord_is_not_reused_after_drop() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let a = db.create_volume().unwrap();
    db.drop_volume(a).unwrap().unwrap();
    let b = db.create_volume().unwrap();
    assert!(b > a, "new ord {b} must exceed dropped ord {a}");
}

#[test]
fn multi_volume_crash_recovery_without_flush() {
    let dir = TempDir::new().unwrap();
    let (a, b) = {
        let db = Db::create(dir.path()).unwrap();
        let a = db.create_volume().unwrap();
        let b = db.create_volume().unwrap();
        for i in 0u64..16 {
            db.insert(a, i, v(0xA0 | i as u8)).unwrap();
            db.insert(b, i, v(0xB0 | i as u8)).unwrap();
        }
        (a, b)
    };
    let db = Db::open(dir.path()).unwrap();
    assert_eq!(db.volumes(), vec![0, a, b]);
    for i in 0u64..16 {
        assert_eq!(db.get(a, i).unwrap(), Some(v(0xA0 | i as u8)));
        assert_eq!(db.get(b, i).unwrap(), Some(v(0xB0 | i as u8)));
    }
}

#[test]
fn bootstrap_volume_is_untouchable() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    assert!(db.drop_volume(0 as VolumeOrdinal).is_err());
    assert_eq!(db.volumes(), vec![0]);
}
