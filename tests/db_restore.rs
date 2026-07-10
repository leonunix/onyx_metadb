//! A4 integration: in-place `restore_volume_to_snapshot`. Restore rolls a
//! volume's live L2P back to a snapshot by replaying the snapshot→current diff
//! (overwrites reverted, post-snapshot adds deleted, post-snapshot deletes
//! re-added) through the normal commit_ops remap path. Covers the reference
//! model, persistence across reopen, the no-op case, and the error path.

use onyx_metadb::{Config, Db, L2pValue};
use tempfile::TempDir;

fn v(n: u8) -> L2pValue {
    let mut x = [0u8; onyx_metadb::paged::LEAF_VALUE_SIZE];
    x[7] = n;
    x[onyx_metadb::paged::LEAF_VALUE_SIZE - 1] = 1;
    L2pValue(x)
}

#[test]
fn restore_reverts_overwrites_adds_and_deletes() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let vol = db.create_volume().unwrap();

    // Snapshot state: LBAs 0..64 = v(i).
    for i in 0u64..64 {
        db.insert(vol, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(vol).unwrap();

    // Diverge: overwrite 0..16, delete 16..32, add new 64..96.
    for i in 0u64..16 {
        db.insert(vol, i, v(0xA0 | (i as u8))).unwrap();
    }
    for i in 16u64..32 {
        db.delete(vol, i).unwrap();
    }
    for i in 64u64..96 {
        db.insert(vol, i, v(i as u8)).unwrap();
    }

    let report = db.restore_volume_to_snapshot(snap).unwrap();
    assert_eq!(report.snapshot_id, snap);
    assert_eq!(report.vol_ord, vol);
    // 16 overwrites (Changed) + 16 deletes-to-revert (RemovedInB) = 32 remaps.
    assert_eq!(report.lbas_remapped, 32);
    // 32 post-snapshot adds (AddedInB) deleted.
    assert_eq!(report.lbas_deleted, 32);

    // Volume is now byte-identical to the snapshot.
    for i in 0u64..64 {
        assert_eq!(db.get(vol, i).unwrap(), Some(v(i as u8)), "lba {i}");
    }
    for i in 64u64..96 {
        assert_eq!(db.get(vol, i).unwrap(), None, "post-snapshot lba {i} gone");
    }

    // The snapshot itself is untouched and still readable.
    let view = db.snapshot_view(snap).unwrap();
    for i in 0u64..64 {
        assert_eq!(view.get(i).unwrap(), Some(v(i as u8)));
    }
}

#[test]
fn restore_is_a_noop_with_no_changes() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let vol = db.create_volume().unwrap();
    for i in 0u64..8 {
        db.insert(vol, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(vol).unwrap();

    let report = db.restore_volume_to_snapshot(snap).unwrap();
    assert_eq!(report.lbas_remapped, 0);
    assert_eq!(report.lbas_deleted, 0);
    for i in 0u64..8 {
        assert_eq!(db.get(vol, i).unwrap(), Some(v(i as u8)));
    }
}

#[test]
fn restore_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let (vol, snap) = {
        let db = Db::create(dir.path()).unwrap();
        let vol = db.create_volume().unwrap();
        for i in 0u64..32 {
            db.insert(vol, i, v(i as u8)).unwrap();
        }
        let snap = db.take_snapshot(vol).unwrap();
        for i in 0u64..32 {
            db.insert(vol, i, v(0x80 | (i as u8))).unwrap();
        }
        db.restore_volume_to_snapshot(snap).unwrap();
        db.flush().unwrap();
        (vol, snap)
    };

    let db = Db::open(dir.path()).unwrap();
    for i in 0u64..32 {
        assert_eq!(
            db.get(vol, i).unwrap(),
            Some(v(i as u8)),
            "lba {i} after reopen"
        );
    }
    // Snapshot entry survived too.
    assert!(db.snapshots().iter().any(|s| s.id == snap));
}

/// Onyx runs with the L2P write buffer enabled, where recent writes live in
/// the buffer overlay rather than the committed tree root. Restore must fold
/// the buffer before diffing or it silently no-ops.
#[test]
fn restore_reverts_with_l2p_buffer_enabled() {
    let dir = TempDir::new().unwrap();
    let db = {
        let mut cfg = Config::new(dir.path());
        cfg.l2p_buffer_enabled = true;
        cfg.commit_direct_apply_enabled = true;
        Db::create_with_config(cfg).unwrap()
    };
    let vol = db.create_volume().unwrap();
    for i in 0u64..32 {
        db.insert(vol, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(vol).unwrap();
    // Diverge with writes that stay in the buffer overlay.
    for i in 0u64..16 {
        db.insert(vol, i, v(0xF0 | (i as u8))).unwrap();
    }
    for i in 32u64..48 {
        db.insert(vol, i, v(i as u8)).unwrap();
    }

    let report = db.restore_volume_to_snapshot(snap).unwrap();
    assert_eq!(report.lbas_remapped, 16, "16 overwrites reverted");
    assert_eq!(report.lbas_deleted, 16, "16 post-snapshot adds removed");

    for i in 0u64..32 {
        assert_eq!(db.get(vol, i).unwrap(), Some(v(i as u8)), "lba {i}");
    }
    for i in 32u64..48 {
        assert_eq!(db.get(vol, i).unwrap(), None, "post-snapshot lba {i}");
    }
}

/// Reproduces the onyx path where every write carries a monotonic commit seq.
/// The snapshot value's (low) seq must not let `seq_guard_rejects` drop the
/// rollback remap over a higher-seq overwrite.
#[test]
fn restore_reverts_writes_with_increasing_seqs() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let vol = db.create_volume().unwrap();

    let put = |lba: u64, val: L2pValue, seq: u64| {
        let mut tx = db.begin();
        tx.l2p_remap(vol, lba, val.with_seq(seq), None);
        tx.commit().unwrap();
    };

    let mut seq = 1u64;
    for i in 0u64..16 {
        put(i, v(i as u8), seq);
        seq += 1;
    }
    let snap = db.take_snapshot(vol).unwrap();
    // Overwrite 0..8 with strictly higher seqs.
    for i in 0u64..8 {
        put(i, v(0xE0 | (i as u8)), seq);
        seq += 1;
    }

    let report = db.restore_volume_to_snapshot(snap).unwrap();
    assert_eq!(report.lbas_remapped, 8);

    // Compare by head PBA — restore rewrites the value with seq=0, so the
    // stored seq differs from the original but the mapping target must match.
    for i in 0u64..16 {
        assert_eq!(
            db.get(vol, i).unwrap().map(|x| x.head_pba()),
            Some(v(i as u8).head_pba()),
            "lba {i} restored to snapshot target"
        );
    }
}

#[test]
fn restore_unknown_snapshot_errors() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let _vol = db.create_volume().unwrap();
    assert!(db.restore_volume_to_snapshot(999_999).is_err());
}

#[test]
fn restore_can_be_rerun_idempotently() {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    let vol = db.create_volume().unwrap();
    for i in 0u64..24 {
        db.insert(vol, i, v(i as u8)).unwrap();
    }
    let snap = db.take_snapshot(vol).unwrap();
    for i in 0u64..24 {
        db.insert(vol, i, v(0x40 | (i as u8))).unwrap();
    }

    db.restore_volume_to_snapshot(snap).unwrap();
    // Second restore sees no diff -> no-op, state still equals the snapshot.
    let again = db.restore_volume_to_snapshot(snap).unwrap();
    assert_eq!(again.lbas_remapped, 0);
    assert_eq!(again.lbas_deleted, 0);
    for i in 0u64..24 {
        assert_eq!(db.get(vol, i).unwrap(), Some(v(i as u8)));
    }
}
