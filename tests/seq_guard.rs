//! Apply-time per-LBA seq_guard CAS — see L2pValue::seq for the wire layout.
//!
//! Onyx stamps a monotonic per-LBA buffer seq into bytes [28..36] of
//! `L2pValue`. The metadb apply lane rejects `L2pRemap` / `L2pPut`
//! when both the incoming and the stored value carry a non-zero seq
//! and the incoming seq is not strictly greater. `seq == 0` either
//! side is the legacy / no-guard sentinel and always accepts.
//!
//! Every scenario runs three times: once via `tx.commit_with_outcomes()`
//! (sync delivery), once via `tx.commit_deferred_with_outcomes()`
//! + a forced compactor pass (ZFS-TXG-clone Phase 2 deferred
//! delivery), and once via the same deferred path with
//! `wal_async_commits_enabled=true` (Phase 3 async WAL — submit acked
//! pre-fsync, durability via the next `flush()` barrier). All three
//! must produce identical seq_guard outcomes because the apply itself
//! is the same code in all paths; only outcome delivery timing and
//! WAL fsync timing shift. Async-WAL is intentionally tested only in
//! combination with deferred outcomes — async WAL on the inline
//! outcome path is an untested combination that `commit_ops_deferred`
//! rejects.

use onyx_metadb::tx::{ApplyOutcome, Transaction};
use onyx_metadb::types::Lsn;
use onyx_metadb::{Config, Db, L2pValue, paged::format::LEAF_VALUE_SIZE};
use tempfile::TempDir;

const VOL: u16 = 0; // bootstrap volume

/// ZFS-TXG-clone Phase 2/3 axis: which commit-with-outcomes shape the
/// test should drive. All three modes share the same apply pipeline
/// (only outcome delivery and WAL fsync timing shift), so identical
/// behaviour is the load-bearing invariant.
///
/// `AsyncWal` implies `Deferred` — async WAL on the inline outcome
/// path is rejected by `commit_ops_deferred`. Never enable
/// `wal_async_commits_enabled` without
/// `commit_deferred_outcomes_enabled`.
#[derive(Copy, Clone, Debug)]
enum Mode {
    Sync,
    Deferred,
    AsyncWal,
}

fn mk_db_for_mode(mode: Mode) -> (TempDir, Db) {
    let dir = TempDir::new().unwrap();
    let cfg = mode_config(dir.path(), mode);
    let db = Db::create_with_config(cfg).unwrap();
    (dir, db)
}

fn mode_config(path: &std::path::Path, mode: Mode) -> Config {
    let mut cfg = Config::new(path);
    // Deferred outcome delivery rides on the L2P compactor's
    // per-pass step-7 drain (see src/db/commit/outcomes.rs). The
    // compactor only spawns when the L2pBuffer is enabled, so both
    // dependencies must flip together — otherwise
    // `tx.commit_deferred_with_outcomes().recv()` would hang
    // forever waiting on a sender no compactor will ever fire.
    cfg.l2p_buffer_enabled = true;
    cfg.commit_direct_apply_enabled = true;
    cfg.commit_deferred_outcomes_enabled =
        matches!(mode, Mode::Deferred | Mode::AsyncWal);
    // Phase 3: async WAL only flips on in AsyncWal mode. The
    // deferred-outcome guard in `commit_ops_deferred` requires both
    // flags to be on before it threads `SubmitOptions::async_` into
    // the WAL submit.
    cfg.wal_async_commits_enabled = matches!(mode, Mode::AsyncWal);
    cfg.l2p_buffer_soft_entries = 1;
    cfg.l2p_buffer_max_interval_ms = 25;
    cfg
}

/// Resolve a transaction into `(lsn, outcomes)` using the path that
/// matches `mode`. Deferred and AsyncWal handles need a compactor
/// pass before `recv()` resolves; the sync path is unchanged. The
/// dispatcher itself doesn't branch on AsyncWal because the recv
/// path is the same — only the underlying WAL fsync timing differs.
fn commit_outcomes(tx: Transaction, db: &Db, mode: Mode) -> (Lsn, Vec<ApplyOutcome>) {
    match mode {
        Mode::Sync => tx.commit_with_outcomes().unwrap(),
        Mode::Deferred | Mode::AsyncWal => {
            let (lsn, handle) = tx.commit_deferred_with_outcomes().unwrap();
            db.test_force_compact_pass();
            (lsn, handle.recv().unwrap())
        }
    }
}

/// Mode-agnostic commit for setup steps that don't observe outcomes.
fn commit(tx: Transaction, db: &Db, mode: Mode) {
    let _ = commit_outcomes(tx, db, mode);
}

/// Build a 36-byte L2pValue stamped with `(pba, seq)`. The first 8
/// bytes are PBA (Onyx contract), bytes [28..36] are the per-LBA seq.
fn v(pba: u64, seq: u64) -> L2pValue {
    let mut bytes = [0u8; LEAF_VALUE_SIZE];
    bytes[..8].copy_from_slice(&pba.to_be_bytes());
    bytes[28..36].copy_from_slice(&seq.to_be_bytes());
    L2pValue(bytes)
}

fn zero_sentinel_always_applies(mode: Mode) {
    // Both sides seq=0 → no guard, second write wins unconditionally.
    let (_d, db) = mk_db_for_mode(mode);
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(100, 0), None);
    commit(tx, &db, mode);
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(200, 0), None);
    commit(tx, &db, mode);
    let cur = db.get(VOL, 5).unwrap().expect("mapped");
    assert_eq!(cur.head_pba(), 200, "seq=0 second write wins");
}

#[test]
fn seq_guard_zero_sentinel_always_applies_sync() {
    zero_sentinel_always_applies(Mode::Sync);
}

#[test]
fn seq_guard_zero_sentinel_always_applies_deferred() {
    zero_sentinel_always_applies(Mode::Deferred);
}

#[test]
fn seq_guard_zero_sentinel_always_applies_async_wal() {
    zero_sentinel_always_applies(Mode::AsyncWal);
}

fn lower_seq_loses_to_higher(mode: Mode) {
    // First commit seq=10, second commit seq=20 → second wins.
    let (_d, db) = mk_db_for_mode(mode);
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(100, 10), None);
    let (_, outcomes) = commit_outcomes(tx, &db, mode);
    assert!(matches!(
        outcomes[0],
        ApplyOutcome::L2pRemap { applied: true, .. }
    ));

    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(200, 20), None);
    let (_, outcomes) = commit_outcomes(tx, &db, mode);
    assert!(matches!(
        outcomes[0],
        ApplyOutcome::L2pRemap { applied: true, .. }
    ));
    assert_eq!(db.get(VOL, 5).unwrap().unwrap().head_pba(), 200);
}

#[test]
fn seq_guard_lower_seq_loses_to_higher_sync() {
    lower_seq_loses_to_higher(Mode::Sync);
}

#[test]
fn seq_guard_lower_seq_loses_to_higher_deferred() {
    lower_seq_loses_to_higher(Mode::Deferred);
}

#[test]
fn seq_guard_lower_seq_loses_to_higher_async_wal() {
    lower_seq_loses_to_higher(Mode::AsyncWal);
}

fn higher_first_then_lower_rejects(mode: Mode) {
    // Commit seq=20 first, then attempt seq=10 → second rejected,
    // L2P unchanged, refcount unchanged.
    let (_d, db) = mk_db_for_mode(mode);
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(200, 20), None);
    commit(tx, &db, mode);
    let rc_before = db.get_refcount(100).unwrap();
    let rc200_before = db.get_refcount(200).unwrap();

    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(100, 10), None);
    let (_, outcomes) = commit_outcomes(tx, &db, mode);
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
fn seq_guard_higher_first_then_lower_rejects_sync() {
    higher_first_then_lower_rejects(Mode::Sync);
}

#[test]
fn seq_guard_higher_first_then_lower_rejects_deferred() {
    higher_first_then_lower_rejects(Mode::Deferred);
}

#[test]
fn seq_guard_higher_first_then_lower_rejects_async_wal() {
    higher_first_then_lower_rejects(Mode::AsyncWal);
}

fn equal_seq_accepts(mode: Mode) {
    // Equality is the recovery-replay case (mark_flushed is memory-only,
    // so a recovered buffer entry re-commits its own write with the same
    // seq already in L2P). The guard accepts on equality so the retry
    // lands instead of leaking the freshly-allocated PBA.
    let (_d, db) = mk_db_for_mode(mode);
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(100, 42), None);
    commit(tx, &db, mode);

    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(200, 42), None);
    let (_, outcomes) = commit_outcomes(tx, &db, mode);
    assert!(matches!(
        outcomes[0],
        ApplyOutcome::L2pRemap { applied: true, .. }
    ));
    assert_eq!(db.get(VOL, 5).unwrap().unwrap().head_pba(), 200);
}

#[test]
fn seq_guard_equal_seq_accepts_sync() {
    equal_seq_accepts(Mode::Sync);
}

#[test]
fn seq_guard_equal_seq_accepts_deferred() {
    equal_seq_accepts(Mode::Deferred);
}

#[test]
fn seq_guard_equal_seq_accepts_async_wal() {
    equal_seq_accepts(Mode::AsyncWal);
}

fn zero_incoming_overrides_existing_seq(mode: Mode) {
    // Existing entry has seq=20; incoming has seq=0 (legacy caller).
    // Sentinel rule: incoming seq=0 → skip check → apply.
    let (_d, db) = mk_db_for_mode(mode);
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(100, 20), None);
    commit(tx, &db, mode);

    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(200, 0), None);
    let (_, outcomes) = commit_outcomes(tx, &db, mode);
    assert!(matches!(
        outcomes[0],
        ApplyOutcome::L2pRemap { applied: true, .. }
    ));
    assert_eq!(db.get(VOL, 5).unwrap().unwrap().head_pba(), 200);
}

#[test]
fn seq_guard_zero_incoming_overrides_existing_seq_sync() {
    zero_incoming_overrides_existing_seq(Mode::Sync);
}

#[test]
fn seq_guard_zero_incoming_overrides_existing_seq_deferred() {
    zero_incoming_overrides_existing_seq(Mode::Deferred);
}

#[test]
fn seq_guard_zero_incoming_overrides_existing_seq_async_wal() {
    zero_incoming_overrides_existing_seq(Mode::AsyncWal);
}

fn zero_existing_accepts_new_seq(mode: Mode) {
    // Existing entry seq=0 (legacy), incoming seq>0. Sentinel rule:
    // cur seq=0 → skip check → apply.
    let (_d, db) = mk_db_for_mode(mode);
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(100, 0), None);
    commit(tx, &db, mode);

    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(200, 5), None);
    let (_, outcomes) = commit_outcomes(tx, &db, mode);
    assert!(matches!(
        outcomes[0],
        ApplyOutcome::L2pRemap { applied: true, .. }
    ));
    assert_eq!(db.get(VOL, 5).unwrap().unwrap().head_pba(), 200);
}

#[test]
fn seq_guard_zero_existing_accepts_new_seq_sync() {
    zero_existing_accepts_new_seq(Mode::Sync);
}

#[test]
fn seq_guard_zero_existing_accepts_new_seq_deferred() {
    zero_existing_accepts_new_seq(Mode::Deferred);
}

#[test]
fn seq_guard_zero_existing_accepts_new_seq_async_wal() {
    zero_existing_accepts_new_seq(Mode::AsyncWal);
}

fn with_pba_guard_pba_rejection_takes_precedence(mode: Mode) {
    // PBA guard fails (refcount < min_rc) → reject with applied=false
    // regardless of seq. Both guards must pass to apply.
    let (_d, db) = mk_db_for_mode(mode);
    // Set up: LBA 5 → PBA 100 with seq=10.
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(100, 10), None);
    commit(tx, &db, mode);
    // Try remap with seq=20 (would pass seq guard) but PBA guard
    // requires refcount(999) >= 1 — refcount(999) = 0, so reject.
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(200, 20), Some((999, 1)));
    let (_, outcomes) = commit_outcomes(tx, &db, mode);
    assert!(
        matches!(outcomes[0], ApplyOutcome::L2pRemap { applied: false, .. }),
        "PBA guard failure must reject before seq is checked"
    );
    assert_eq!(db.get(VOL, 5).unwrap().unwrap().head_pba(), 100);
}

#[test]
fn seq_guard_with_pba_guard_pba_rejection_takes_precedence_sync() {
    with_pba_guard_pba_rejection_takes_precedence(Mode::Sync);
}

#[test]
fn seq_guard_with_pba_guard_pba_rejection_takes_precedence_deferred() {
    with_pba_guard_pba_rejection_takes_precedence(Mode::Deferred);
}

#[test]
fn seq_guard_with_pba_guard_pba_rejection_takes_precedence_async_wal() {
    with_pba_guard_pba_rejection_takes_precedence(Mode::AsyncWal);
}

fn survives_reopen(mode: Mode) {
    // The seq is persisted inside the L2pValue (bytes [28..36]).
    // After flush + reopen, the seq guard still rejects stale writes.
    // The reopen drops the in-memory aggregator, so the reopened
    // Db's `commit_deferred_outcomes_enabled` is what `mode_config`
    // requested at open time.
    let dir = TempDir::new().unwrap();
    {
        let db = Db::create_with_config(mode_config(dir.path(), mode)).unwrap();
        let mut tx = db.begin();
        tx.l2p_remap(VOL, 5, v(100, 50), None);
        commit(tx, &db, mode);
        db.flush().unwrap();
    }
    let db = Db::open_with_config(mode_config(dir.path(), mode)).unwrap();
    // Stored seq=50; try a stale seq=30 → reject.
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(200, 30), None);
    let (_, outcomes) = commit_outcomes(tx, &db, mode);
    assert!(matches!(
        outcomes[0],
        ApplyOutcome::L2pRemap { applied: false, .. }
    ));
    assert_eq!(db.get(VOL, 5).unwrap().unwrap().head_pba(), 100);
    // A fresh seq=60 → accept.
    let mut tx = db.begin();
    tx.l2p_remap(VOL, 5, v(300, 60), None);
    let (_, outcomes) = commit_outcomes(tx, &db, mode);
    assert!(matches!(
        outcomes[0],
        ApplyOutcome::L2pRemap { applied: true, .. }
    ));
    assert_eq!(db.get(VOL, 5).unwrap().unwrap().head_pba(), 300);
}

#[test]
fn seq_guard_survives_reopen_sync() {
    survives_reopen(Mode::Sync);
}

#[test]
fn seq_guard_survives_reopen_deferred() {
    survives_reopen(Mode::Deferred);
}

#[test]
fn seq_guard_survives_reopen_async_wal() {
    // Phase 3: the pre-reopen `db.flush()` call inside `survives_reopen`
    // exercises the new `fsync_all_lanes` TXG-sync barrier — without
    // it, async-submitted WAL bytes would still sit in OS page cache
    // and the reopen would see an OLD checkpoint.
    survives_reopen(Mode::AsyncWal);
}

