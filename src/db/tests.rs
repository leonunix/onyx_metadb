use super::*;
use tempfile::TempDir;

pub(super) fn v(n: u8) -> L2pValue {
    let mut x = [0u8; crate::paged::format::LEAF_VALUE_SIZE];
    x[0] = n;
    // Pin the birth_lsn trailer to a small non-zero u64 so the apply
    // path's birth_lsn stamping (replaces 0 sentinel with apply lsn)
    // doesn't perturb the value bytes underneath round-trip assertions
    // throughout the test suite. The exact value doesn't matter — only
    // that it's non-zero so the stamp pass preserves it.
    x[crate::paged::format::LEAF_VALUE_SIZE - 1] = 1;
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

#[test]
fn apply_lane_maintenance_bypasses_queued_wal_work() {
    let lane = ApplyLane::new(0, ApplyLaneKind::L2p, 0, Arc::new(MetaMetrics::new()));
    let order = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();

    let order_for_lsn1 = order.clone();
    lane.enqueue_ready(
        1,
        Box::new(move || {
            order_for_lsn1.lock().unwrap().push(1);
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        }),
    );
    started_rx.recv().unwrap();

    let pending = lane.enqueue_pending(2);
    let order_for_lsn2 = order.clone();
    let order_for_maintenance = order.clone();
    lane.enqueue_maintenance(Box::new(move || {
        order_for_maintenance.lock().unwrap().push(99);
    }));

    release_tx.send(()).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(10));
    pending.set(Box::new(move || {
        order_for_lsn2.lock().unwrap().push(2);
        done_tx.send(()).unwrap();
    }));
    done_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();
    assert_eq!(*order.lock().unwrap(), vec![1, 99, 2]);
    assert_eq!(lane.last_applied_lsn(), 2);
}

#[test]
fn apply_lane_prioritizes_ready_wal_work_with_bounded_maintenance() {
    let lane = ApplyLane::new(0, ApplyLaneKind::L2p, 0, Arc::new(MetaMetrics::new()));
    let order = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();

    let order_for_lsn1 = order.clone();
    lane.enqueue_ready(
        1,
        Box::new(move || {
            order_for_lsn1.lock().unwrap().push(1);
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        }),
    );
    started_rx.recv().unwrap();

    let order_for_maintenance = order.clone();
    lane.enqueue_maintenance(Box::new(move || {
        order_for_maintenance.lock().unwrap().push(90);
    }));

    for lsn in 2..=66 {
        let order_for_lsn = order.clone();
        let done_tx = done_tx.clone();
        lane.enqueue_ready(
            lsn,
            Box::new(move || {
                order_for_lsn.lock().unwrap().push(lsn);
                if lsn == 66 {
                    done_tx.send(()).unwrap();
                }
            }),
        );
    }
    drop(done_tx);

    release_tx.send(()).unwrap();
    done_rx
        .recv_timeout(std::time::Duration::from_secs(1))
        .unwrap();
    let mut expected: Vec<u64> = (1..=64).collect();
    expected.push(90);
    expected.extend(65..=66);
    assert_eq!(*order.lock().unwrap(), expected);
    assert_eq!(lane.last_applied_lsn(), 66);
}

mod batch;

pub(super) fn h(n: u64) -> Hash8 {
    let mut out = [0u8; 8];
    out[..8].copy_from_slice(&n.to_be_bytes());
    out
}

pub(super) fn dv(n: u8) -> DedupValue {
    let mut x = [0u8; 28];
    x[0] = n;
    DedupValue(x)
}

pub(super) fn hash_full(high: u64, low: u64) -> Hash8 {
    // Mix `high` and `low` into a single 8-byte fingerprint so the
    // (high, low) pair survives the schema swap. xorshift-style mix
    // avoids accidental collisions when `low == 0` (a common case in
    // tests).
    let mixed = high.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ low;
    mixed.to_be_bytes()
}

pub(super) fn hash_bytes(high: u64, low: u64) -> Hash8 {
    // Lighter mix: top half is `high`, bottom half folds `low` in.
    let mixed = (high & 0xFFFF_FFFF_0000_0000) | (low & 0x0000_0000_FFFF_FFFF);
    mixed.to_be_bytes()
}

pub(super) fn dedup_val(n: u8) -> DedupValue {
    let mut v = [0u8; 28];
    v[0] = n;
    DedupValue(v)
}

mod core;
mod dead_list;
mod indexes;
mod remap_range;
mod volume;
