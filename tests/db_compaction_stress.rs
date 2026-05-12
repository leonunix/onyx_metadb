use onyx_metadb::{Config, Db, DedupValue, Hash32, NULL_PAGE};
use tempfile::TempDir;

const RECORD_SIZE_BYTES: u64 = 64;

fn h(seed: u64) -> Hash32 {
    let mut out = [0u8; 32];
    out[..8].copy_from_slice(&seed.to_be_bytes());
    out[8..16].copy_from_slice(&seed.rotate_left(7).to_be_bytes());
    out[16..24].copy_from_slice(&seed.rotate_left(19).to_be_bytes());
    out[24..32].copy_from_slice(&seed.wrapping_mul(11).to_be_bytes());
    out
}

fn dval(seed: u64) -> DedupValue {
    let mut out = [0u8; 28];
    out[..8].copy_from_slice(&seed.to_be_bytes());
    out[8..16].copy_from_slice(&seed.rotate_left(9).to_be_bytes());
    out[16..24].copy_from_slice(&seed.rotate_left(21).to_be_bytes());
    out[24..28].copy_from_slice(&(seed as u32).to_be_bytes());
    DedupValue(out)
}

fn stress_config(dir: &TempDir) -> Config {
    let mut cfg = Config::new(dir.path());
    cfg.lsm_memtable_bytes = 256 * 1024;
    cfg.lsm_l0_sst_count_trigger = 4;
    cfg.lsm_level_ratio = 4;
    cfg.page_cache_bytes = 64 * 1024 * 1024;
    cfg.wal_segment_bytes = 4 * 1024 * 1024;
    cfg
}

#[test]
fn dedup_compaction_smoke_reopen_clean() {
    let dir = TempDir::new().unwrap();
    let db = Db::create_with_config(stress_config(&dir)).unwrap();
    run_compaction_storm(&db, 2 * 1024 * 1024, 4_096);
    db.flush().unwrap();
    drop(db);

    let reopened = Db::open_with_config(stress_config(&dir)).unwrap();
    verify_prefix(&reopened, (2u64 * 1024 * 1024).div_ceil(RECORD_SIZE_BYTES));
}

#[test]
#[ignore = "Phase 8a 10 GiB compaction storm; override with METADB_COMPACTION_TARGET_BYTES"]
fn dedup_compaction_10gib_reaches_l3_and_stays_readable() {
    let target_bytes = std::env::var("METADB_COMPACTION_TARGET_BYTES")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(10 * 1024 * 1024 * 1024);
    let checkpoint_records = std::env::var("METADB_COMPACTION_VERIFY_EVERY")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(262_144);

    let dir = TempDir::new().unwrap();
    let cfg = stress_config(&dir);
    let db = Db::create_with_config(cfg.clone()).unwrap();
    run_compaction_storm(&db, target_bytes, checkpoint_records);
    db.flush().unwrap();
    let manifest = db.manifest();
    assert!(
        manifest.dedup_level_heads.len() >= 4 && manifest.dedup_level_heads[3] != NULL_PAGE,
        "expected L3+ after compaction storm, got {:?}",
        manifest.dedup_level_heads
    );
    drop(db);

    let reopened = Db::open_with_config(cfg).unwrap();
    verify_prefix(&reopened, target_bytes.div_ceil(RECORD_SIZE_BYTES));
}

fn run_compaction_storm(db: &Db, target_bytes: u64, checkpoint_records: u64) {
    let total_records = target_bytes.div_ceil(RECORD_SIZE_BYTES);
    let mut inserted = 0u64;
    while inserted < total_records {
        let end = (inserted + checkpoint_records).min(total_records);
        for i in inserted..end {
            db.put_dedup(h(i), dval(i)).unwrap();
            assert_eq!(db.get_dedup(&h(i)).unwrap(), Some(dval(i)));
            if db.dedup_should_flush() {
                let _ = db.flush_dedup_memtable().unwrap();
            }
        }
        inserted = end;
        while db.compact_dedup_once().unwrap() {}
        verify_prefix(db, inserted);
    }
}

fn verify_prefix(db: &Db, count: u64) {
    for i in 0..count {
        assert_eq!(db.get_dedup(&h(i)).unwrap(), Some(dval(i)), "record {i}");
    }
}
