//! Regression: `clone_volume` followed by writes into both the source
//! and the clone must leave refcounts coherent. Before the atomic_rc_delta
//! fix in `paged::cache`, each tree's PageBuf held an independent Clean
//! slot for the shared root; both cow'd from the same pre-decrement rc
//! and wrote `rc-1` via flush, losing one decrement (header would end up
//! one higher than the true reference count).

use onyx_metadb::{Config, Db, L2pValue, VerifyOptions, verify_path};
use tempfile::TempDir;

fn v(n: u8) -> L2pValue {
    let mut x = [0u8; 28];
    x[0] = n;
    L2pValue(x)
}

#[test]
fn clone_volume_cross_write_keeps_refcounts_clean() {
    let dir = TempDir::new().unwrap();
    let path = dir.path();

    {
        let mut cfg = Config::new(path);
        cfg.shards_per_partition = 1;
        let db = Db::create_with_config(cfg).unwrap();
        for i in 0u64..20 {
            db.insert(0, i, v(i as u8)).unwrap();
        }
        db.flush().unwrap();
        let _s1 = db.take_snapshot(0).unwrap();
    }

    {
        let db = Db::open(path).unwrap();
        for i in 20u64..40 {
            db.insert(0, i, v(i as u8)).unwrap();
        }
        db.flush().unwrap();
        let _s2 = db.take_snapshot(0).unwrap();
    }

    {
        let db = Db::open(path).unwrap();
        let v1 = db.clone_volume(2).unwrap();
        // Write into the clone, then into the source: both cow the
        // shared root. Pre-fix this lost a decref and verify_path
        // reported rc mismatches.
        for i in 0u64..10 {
            db.insert(v1, i, v((i + 100) as u8)).unwrap();
        }
        for i in 40u64..50 {
            db.insert(0, i, v(i as u8)).unwrap();
        }
        db.flush().unwrap();
    }

    let report = verify_path(path, VerifyOptions { strict: true }).unwrap();
    assert!(
        report.is_clean(),
        "verify_path reported issues: {:?}",
        report.issues
    );
}
