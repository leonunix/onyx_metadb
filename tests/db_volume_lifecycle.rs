//! Phase 7 commit 8 integration: per-volume `create_volume` /
//! `drop_volume` lifecycle. Covers create/drop/read isolation, flush +
//! manifest persistence, and WAL-replay recovery without a prior flush.

use onyx_metadb::{Db, L2pValue, VolumeOrdinal};
use tempfile::TempDir;

fn v(n: u8) -> L2pValue {
    let mut x = [0u8; 36];
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

// -------- Phase 2 dedup-shard lifecycle ---------------------------------

#[test]
fn create_with_dedup_shards_n4_round_trips_through_reopen() {
    let dir = TempDir::new().unwrap();
    let mut cfg = onyx_metadb::Config::new(dir.path());
    cfg.dedup_shards = 4;

    {
        let db = Db::create_with_config(cfg.clone()).unwrap();
        // Write a few entries that route to different shards (route by
        // hash[0]) so every shard ends up with at least one row after
        // a flush.
        for byte0 in [0u8, 64, 128, 192] {
            let mut hash = [0u8; 8];
            hash[0] = byte0;
            let mut value = [0u8; 28];
            value[0] = byte0;
            db.put_dedup(hash, onyx_metadb::DedupValue(value)).unwrap();
        }
        db.flush().unwrap();

        let manifest = db.manifest();
        // `dedup_shards` is still a runtime tunable (it drives apply
        // lanes and dedup routing fan-out), but the dedup_index is now
        // a single cuckoo table (`a07bd50`) and dedup_reverse is a
        // single paged-array (`6bc4d92`). Both stamp their stable head
        // page id under the legacy `*_shard_heads` slot, so the outer
        // length is always 1 regardless of `dedup_shards`.
        assert_eq!(manifest.dedup_shards, 4);
        assert_eq!(manifest.dedup_index_shard_heads.len(), 1);
        assert_eq!(manifest.dedup_index_shard_heads[0].len(), 1);
        assert_eq!(manifest.dedup_reverse_shard_heads.len(), 1);
        assert_eq!(manifest.dedup_reverse_shard_heads[0].len(), 1);
    }

    // Reopen with the same N=4 succeeds and reads back every entry.
    let db = Db::open_with_config(cfg).unwrap();
    for byte0 in [0u8, 64, 128, 192] {
        let mut hash = [0u8; 8];
        hash[0] = byte0;
        let got = db.get_dedup(&hash).unwrap();
        assert!(got.is_some(), "missing entry for byte0={byte0}");
    }
}

fn err_msg<T>(r: onyx_metadb::Result<T>) -> String {
    match r {
        Ok(_) => panic!("expected error"),
        Err(e) => format!("{e}"),
    }
}

#[test]
fn open_rejects_dedup_shards_mismatch_against_manifest() {
    let dir = TempDir::new().unwrap();
    let mut cfg_n1 = onyx_metadb::Config::new(dir.path());
    cfg_n1.dedup_shards = 1;
    {
        let _db = Db::create_with_config(cfg_n1.clone()).unwrap();
    }

    let mut cfg_n4 = onyx_metadb::Config::new(dir.path());
    cfg_n4.dedup_shards = 4;
    let msg = err_msg(Db::open_with_config(cfg_n4));
    assert!(
        msg.contains("dedup_shards") && msg.contains("recreate"),
        "expected layout-mismatch error, got: {msg}",
    );
}

#[test]
fn create_rejects_non_power_of_two_dedup_shards() {
    let dir = TempDir::new().unwrap();
    let mut cfg = onyx_metadb::Config::new(dir.path());
    cfg.dedup_shards = 3;
    let msg = err_msg(Db::create_with_config(cfg));
    assert!(msg.contains("power of two"), "got: {msg}");
}

#[test]
fn create_rejects_zero_dedup_shards() {
    let dir = TempDir::new().unwrap();
    let mut cfg = onyx_metadb::Config::new(dir.path());
    cfg.dedup_shards = 0;
    let msg = err_msg(Db::create_with_config(cfg));
    assert!(msg.contains("greater than zero"), "got: {msg}");
}

#[test]
fn create_rejects_dedup_shards_above_cap() {
    let dir = TempDir::new().unwrap();
    let mut cfg = onyx_metadb::Config::new(dir.path());
    cfg.dedup_shards = onyx_metadb::MAX_DEDUP_SHARDS * 2;
    let msg = err_msg(Db::create_with_config(cfg));
    assert!(msg.contains("MAX_DEDUP_SHARDS"), "got: {msg}");
}
