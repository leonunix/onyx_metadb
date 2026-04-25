use super::*;
use tempfile::TempDir;

pub(super) fn v(n: u8) -> L2pValue {
    let mut x = [0u8; 28];
    x[0] = n;
    L2pValue(x)
}

pub(super) fn mk_db() -> (TempDir, Db) {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    (dir, db)
}

pub(super) fn mk_db_with_shards(shards: u32) -> (TempDir, Db) {
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.shards_per_partition = shards;
    let db = Db::create_with_config(cfg).unwrap();
    (dir, db)
}

pub(super) fn mk_db_with_cache_bytes(page_cache_bytes: u64) -> (TempDir, Db) {
    let dir = TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.page_cache_bytes = page_cache_bytes;
    let db = Db::create_with_config(cfg).unwrap();
    (dir, db)
}

mod batch;
mod cleanup;

pub(super) fn h(n: u64) -> Hash32 {
    let mut out = [0u8; 32];
    out[..8].copy_from_slice(&n.to_be_bytes());
    out
}

pub(super) fn dv(n: u8) -> DedupValue {
    let mut x = [0u8; 28];
    x[0] = n;
    DedupValue(x)
}

pub(super) fn hash_full(high: u64, low: u64) -> Hash32 {
    let mut h = [0u8; 32];
    h[..8].copy_from_slice(&high.to_be_bytes());
    h[8..16].copy_from_slice(&(high.wrapping_mul(7)).to_be_bytes());
    h[16..24].copy_from_slice(&(low.wrapping_mul(11)).to_be_bytes());
    h[24..].copy_from_slice(&low.to_be_bytes());
    h
}

pub(super) fn hash_bytes(high: u64, low: u64) -> Hash32 {
    let mut h = [0u8; 32];
    h[..8].copy_from_slice(&high.to_be_bytes());
    h[24..].copy_from_slice(&low.to_be_bytes());
    h
}

pub(super) fn dedup_val(n: u8) -> DedupValue {
    let mut v = [0u8; 28];
    v[0] = n;
    DedupValue(v)
}

mod core;
mod indexes;
mod remap_range;
mod volume;
