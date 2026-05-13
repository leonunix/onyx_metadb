//! Apply-time per-LBA seq_guard CAS — see L2pValue::seq for the wire layout.
//!
//! Onyx stamps a monotonic per-LBA buffer seq into bytes [28..36] of
//! `L2pValue`. The metadb apply lane rejects `L2pRemap` / `L2pPut`
//! when both the incoming and the stored value carry a non-zero seq
//! and the incoming seq is not strictly greater. `seq == 0` either
//! side is the legacy / no-guard sentinel and always accepts.

use onyx_metadb::tx::ApplyOutcome;
use onyx_metadb::{Db, L2pValue, paged::format::LEAF_VALUE_SIZE};
use tempfile::TempDir;

const VOL: u16 = 0; // bootstrap volume

fn mk_db() -> (TempDir, Db) {
    let dir = TempDir::new().unwrap();
    let db = Db::create(dir.path()).unwrap();
    (dir, db)
}

/// Build a 36-byte L2pValue stamped with `(pba, seq)`. The first 8
/// bytes are PBA (Onyx contract), bytes [28..36] are the per-LBA seq.
fn v(pba: u64, seq: u64) -> L2pValue {
    let mut bytes = [0u8; LEAF_VALUE_SIZE];
    bytes[..8].copy_from_slice(&pba.to_be_bytes());
    bytes[28..36].copy_from_slice(&seq.to_be_bytes());
    L2pValue(bytes)
}

#[test]
fn seq_guard_zero_sentinel_always_applies() {
    // Both sides seq=0 → no guard, second write wins unconditionally.
    let (_d, db) = mk_db();
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(100, 0), None);
    tx.commit().unwrap();
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(200, 0), None);
    tx.commit().unwrap();
    let cur = db.get(VOL, 5).unwrap().expect("mapped");
    assert_eq!(cur.head_pba(), 200, "seq=0 second write wins");
}

#[test]
fn seq_guard_lower_seq_loses_to_higher() {
    // First commit seq=10, second commit seq=20 → second wins.
    let (_d, db) = mk_db();
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(100, 10), None);
    let (_, outcomes) = tx.commit_with_outcomes().unwrap();
    assert!(matches!(
        outcomes[0],
        ApplyOutcome::L2pRemap { applied: true, .. }
    ));

    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(200, 20), None);
    let (_, outcomes) = tx.commit_with_outcomes().unwrap();
    assert!(matches!(
        outcomes[0],
        ApplyOutcome::L2pRemap { applied: true, .. }
    ));
    assert_eq!(db.get(VOL, 5).unwrap().unwrap().head_pba(), 200);
}

#[test]
fn seq_guard_higher_first_then_lower_rejects() {
    // Commit seq=20 first, then attempt seq=10 → second rejected,
    // L2P unchanged, refcount unchanged.
    let (_d, db) = mk_db();
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(200, 20), None);
    tx.commit().unwrap();
    let rc_before = db.get_refcount(100).unwrap();
    let rc200_before = db.get_refcount(200).unwrap();

    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(100, 10), None);
    let (_, outcomes) = tx.commit_with_outcomes().unwrap();
    assert!(
        matches!(outcomes[0], ApplyOutcome::L2pRemap { applied: false, .. }),
        "stale seq=10 must be rejected; got {:?}",
        outcomes[0]
    );
    // L2P still points at 200.
    assert_eq!(db.get(VOL, 5).unwrap().unwrap().head_pba(), 200);
    // Refcounts unchanged: rejected op must not incref 100 or decref 200.
    assert_eq!(db.get_refcount(100).unwrap(), rc_before);
    assert_eq!(db.get_refcount(200).unwrap(), rc200_before);
}

#[test]
fn seq_guard_equal_seq_accepts() {
    // Equality is the recovery-replay case (mark_flushed is memory-only,
    // so a recovered buffer entry re-commits its own write with the same
    // seq already in L2P). The guard accepts on equality so the retry
    // lands instead of leaking the freshly-allocated PBA.
    let (_d, db) = mk_db();
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(100, 42), None);
    tx.commit().unwrap();

    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(200, 42), None);
    let (_, outcomes) = tx.commit_with_outcomes().unwrap();
    assert!(matches!(
        outcomes[0],
        ApplyOutcome::L2pRemap { applied: true, .. }
    ));
    assert_eq!(db.get(VOL, 5).unwrap().unwrap().head_pba(), 200);
}

#[test]
fn seq_guard_zero_incoming_overrides_existing_seq() {
    // Existing entry has seq=20; incoming has seq=0 (legacy caller).
    // Sentinel rule: incoming seq=0 → skip check → apply.
    let (_d, db) = mk_db();
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(100, 20), None);
    tx.commit().unwrap();

    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(200, 0), None);
    let (_, outcomes) = tx.commit_with_outcomes().unwrap();
    assert!(matches!(
        outcomes[0],
        ApplyOutcome::L2pRemap { applied: true, .. }
    ));
    assert_eq!(db.get(VOL, 5).unwrap().unwrap().head_pba(), 200);
}

#[test]
fn seq_guard_zero_existing_accepts_new_seq() {
    // Existing entry seq=0 (legacy), incoming seq>0. Sentinel rule:
    // cur seq=0 → skip check → apply.
    let (_d, db) = mk_db();
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(100, 0), None);
    tx.commit().unwrap();

    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(200, 5), None);
    let (_, outcomes) = tx.commit_with_outcomes().unwrap();
    assert!(matches!(
        outcomes[0],
        ApplyOutcome::L2pRemap { applied: true, .. }
    ));
    assert_eq!(db.get(VOL, 5).unwrap().unwrap().head_pba(), 200);
}

#[test]
fn seq_guard_with_pba_guard_pba_rejection_takes_precedence() {
    // PBA guard fails (refcount < min_rc) → reject with applied=false
    // regardless of seq. Both guards must pass to apply.
    let (_d, db) = mk_db();
    // Set up: LBA 5 → PBA 100 with seq=10.
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(100, 10), None);
    tx.commit().unwrap();
    // Try remap with seq=20 (would pass seq guard) but PBA guard
    // requires refcount(999) >= 1 — refcount(999) = 0, so reject.
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(200, 20), Some((999, 1)));
    let (_, outcomes) = tx.commit_with_outcomes().unwrap();
    assert!(
        matches!(outcomes[0], ApplyOutcome::L2pRemap { applied: false, .. }),
        "PBA guard failure must reject before seq is checked"
    );
    assert_eq!(db.get(VOL, 5).unwrap().unwrap().head_pba(), 100);
}

#[test]
fn seq_guard_survives_reopen() {
    // The seq is persisted inside the L2pValue (bytes [28..36]).
    // After flush + reopen, the seq guard still rejects stale writes.
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create(dir.path()).unwrap();
        let mut tx = db.begin();
        tx.l2p_remap(VOL, 5, v(100, 50), None);
        tx.commit().unwrap();
        db.flush().unwrap();
    }
    let db = Db::open(dir.path()).unwrap();
    // Stored seq=50; try a stale seq=30 → reject.
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(200, 30), None);
    let (_, outcomes) = tx.commit_with_outcomes().unwrap();
    assert!(matches!(
        outcomes[0],
        ApplyOutcome::L2pRemap { applied: false, .. }
    ));
    assert_eq!(db.get(VOL, 5).unwrap().unwrap().head_pba(), 100);
    // A fresh seq=60 → accept.
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(300, 60), None);
    let (_, outcomes) = tx.commit_with_outcomes().unwrap();
    assert!(matches!(
        outcomes[0],
        ApplyOutcome::L2pRemap { applied: true, .. }
    ));
    assert_eq!(db.get(VOL, 5).unwrap().unwrap().head_pba(), 300);
}

