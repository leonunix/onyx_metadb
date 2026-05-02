//! Phase 3 of dedup-lane sharding: verify that the per-shard
//! `dedup_lanes` and `DispatchLaneKey::Dedup(u32)` actually produce
//! the parallelism + ordering guarantees the design promises.
//!
//! Each test creates a database with `cfg.dedup_shards = 4` and uses
//! hashes whose first byte deliberately routes to different shards
//! (high two bits of `hash[0]` pick the shard for N=4).

use onyx_metadb::lsm::Hash32;
use onyx_metadb::{Db, DedupValue};
use tempfile::TempDir;

fn cfg_for(dir: &TempDir, dedup_shards: u32) -> onyx_metadb::Config {
    let mut cfg = onyx_metadb::Config::new(dir.path());
    cfg.dedup_shards = dedup_shards;
    cfg
}

/// Construct a 32-byte hash where `hash[0]` routes to the requested
/// shard under `shard_for_hash(hash, 4)`. With N=4, `shard_for_hash`
/// reads bits 7..6 of `hash[0]`, so `shard 0 = 0x00..0x3F`,
/// `shard 1 = 0x40..0x7F`, etc.
fn hash_for_shard(shard: u8, salt: u8) -> Hash32 {
    assert!(shard < 4, "this helper only covers N=4");
    let mut h = [0u8; 32];
    h[0] = (shard << 6) | (salt & 0x3F);
    h[31] = salt; // make hashes distinct even within a shard
    h
}

fn dval(n: u8) -> DedupValue {
    let mut v = [0u8; 28];
    v[0] = n;
    DedupValue(v)
}

/// Build a `DedupValue` whose `head_pba()` decodes to `pba`. The
/// cleanup race-protection check (`entry.head_pba() == pba`) only
/// emits a forward tombstone when this is true, so cleanup tests
/// that want to assert deletion need values that actually point at
/// the target PBA.
fn dval_for_pba(pba: u64) -> DedupValue {
    let mut v = [0u8; 28];
    v[..8].copy_from_slice(&pba.to_be_bytes());
    DedupValue(v)
}

#[test]
fn forward_and_reverse_for_same_pair_route_to_same_shard() {
    let dir = TempDir::new().unwrap();
    let db = Db::create_with_config(cfg_for(&dir, 4)).unwrap();
    // Pick four hashes, one per shard. For each, write the forward
    // entry + reverse entry in one transaction; verify both readbacks
    // see them.
    for shard in 0u8..4 {
        let hash = hash_for_shard(shard, shard);
        let pba: u64 = (1000 + shard as u64) * 1000;
        let mut tx = db.begin();
        tx.put_dedup(hash, dval(shard));
        tx.register_dedup_reverse(pba, hash);
        tx.commit().unwrap();

        assert_eq!(db.get_dedup(&hash).unwrap(), Some(dval(shard)));
        let reverse = db.scan_dedup_reverse_for_pba(pba).unwrap();
        assert_eq!(reverse, vec![hash]);
    }
}

#[test]
fn cleanup_dedup_for_dead_pbas_unions_across_shards() {
    // Register one PBA against four hashes that route to four
    // different shards. cleanup must find every reverse row across
    // shards and tombstone every forward entry.
    let dir = TempDir::new().unwrap();
    let db = Db::create_with_config(cfg_for(&dir, 4)).unwrap();
    let pba: u64 = 0xDEAD_BEEF;

    let hashes: Vec<Hash32> = (0u8..4).map(|s| hash_for_shard(s, 0xAA)).collect();
    let value = dval_for_pba(pba);
    for hash in &hashes {
        let mut tx = db.begin();
        tx.put_dedup(*hash, value);
        tx.register_dedup_reverse(pba, *hash);
        tx.incref_pba(pba, 1);
        tx.commit().unwrap();
    }
    // Force memtable -> SST so the cleanup must hit every shard's
    // SST chain, not just memtable.
    db.flush_dedup_memtable().unwrap();

    // Drop refcount to zero, then run the dedup cleanup.
    db.decref_pba(pba, 4).unwrap();
    db.cleanup_dedup_for_dead_pbas(&[pba]).unwrap();

    for hash in &hashes {
        assert!(
            db.get_dedup(hash).unwrap().is_none(),
            "forward entry for hash {hash:?} should have been tombstoned"
        );
    }
    let reverse = db.scan_dedup_reverse_for_pba(pba).unwrap();
    assert!(
        reverse.is_empty(),
        "reverse entries for dead pba should have been tombstoned, got {reverse:?}"
    );
}

#[test]
fn dedup_index_per_shard_stats_reflect_routing() {
    // Write distinct hashes that each go to a different shard, then
    // read back per-shard stats. Each shard should see exactly the
    // ops we routed to it.
    let dir = TempDir::new().unwrap();
    let db = Db::create_with_config(cfg_for(&dir, 4)).unwrap();
    // 8 hashes per shard.
    for shard in 0u8..4 {
        for salt in 0u8..8 {
            let hash = hash_for_shard(shard, salt);
            db.put_dedup(hash, dval(salt)).unwrap();
        }
    }

    let (index_per_shard, _reverse_per_shard) = db.dedup_lsm_stats_per_shard();
    assert_eq!(index_per_shard.len(), 4);
    for (sid, stats) in index_per_shard.iter().enumerate() {
        let entries = stats.memtable.active_entries + stats.memtable.frozen_entries;
        assert_eq!(
            entries, 8,
            "shard {sid} got {entries} active+frozen forward entries; expected 8",
        );
    }
}

#[test]
fn dedup_writes_survive_reopen_with_n_eq_four() {
    // Sanity check that the per-shard apply lanes don't lose ops
    // across an open / close / reopen cycle. Forward + reverse are
    // both verified.
    let dir = TempDir::new().unwrap();
    let cfg = cfg_for(&dir, 4);
    let mut entries = Vec::new();
    {
        let db = Db::create_with_config(cfg.clone()).unwrap();
        for shard in 0u8..4 {
            for salt in 0u8..3 {
                let hash = hash_for_shard(shard, salt);
                let pba: u64 = ((shard as u64) << 32) | (salt as u64 + 1);
                let mut tx = db.begin();
                tx.put_dedup(hash, dval(salt));
                tx.register_dedup_reverse(pba, hash);
                tx.commit().unwrap();
                entries.push((hash, pba, salt));
            }
        }
        db.flush().unwrap();
    }
    let db = Db::open_with_config(cfg).unwrap();
    for (hash, pba, salt) in &entries {
        assert_eq!(db.get_dedup(hash).unwrap(), Some(dval(*salt)));
        let reverse = db.scan_dedup_reverse_for_pba(*pba).unwrap();
        assert!(reverse.contains(hash));
    }
}
