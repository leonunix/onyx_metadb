use super::*;
use tempfile::TempDir;

fn make_shard() -> (TempDir, RcShard) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages");
    let page_store = Arc::new(PageStore::create(&path).unwrap());
    let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
    let s = RcShard::create(page_store, page_cache).unwrap();
    (dir, s)
}

#[test]
fn stage_then_get_sees_pending() {
    let (_d, s) = make_shard();
    assert_eq!(s.stage(10, 1, 100).unwrap(), (0, 1));
    assert_eq!(s.get(10).unwrap(), 1);
}

#[test]
fn stage_accumulates_across_ops() {
    let (_d, s) = make_shard();
    s.stage(10, 1, 100).unwrap();
    s.stage(10, 2, 101).unwrap();
    s.stage(10, -1, 102).unwrap();
    assert_eq!(s.get(10).unwrap(), 2);
}

#[test]
fn flush_moves_pending_to_array() {
    let (_d, s) = make_shard();
    s.stage(10, 5, 100).unwrap();
    s.flush().unwrap();
    assert_eq!(s.get(10).unwrap(), 5);
    assert_eq!(
        s.get_entry(10).unwrap(),
        RcEntry {
            rc: 5,
            birth_lsn: 100
        }
    );
}

fn clamped() -> u64 {
    crate::refcount::underflow_clamped_total()
}

#[test]
fn stage_decref_past_zero_is_skipped_not_errored() {
    let (_d, s) = make_shard();
    let c0 = clamped();
    s.stage(10, 1, 1).unwrap(); // rc 0 -> 1
    // Decref by 2 when only 1 ref exists would underflow. Instead of
    // erroring (which `poison_commit_waiters` fans out into a cascade of
    // unrelated commit failures) the redundant decref is skipped and the
    // count is left unchanged.
    assert_eq!(s.stage(10, -2, 2).unwrap(), (1, 1));
    assert_eq!(s.get(10).unwrap(), 1);
    assert_eq!(clamped() - c0, 1);
    // The delta map is not corrupted: a subsequent legitimate decref
    // still works.
    assert_eq!(s.stage(10, -1, 3).unwrap(), (1, 0));
    assert_eq!(s.get(10).unwrap(), 0);
}

#[test]
fn stage_double_decref_of_freed_pba_is_benign() {
    // The production race: two ops remove the same last reference. The
    // first takes the count to 0; the second finds it already 0 and is
    // skipped — not an error, never negative.
    let (_d, s) = make_shard();
    let c0 = clamped();
    s.stage(20, 1, 1).unwrap(); // rc 0 -> 1
    assert_eq!(s.stage(20, -1, 2).unwrap(), (1, 0)); // legit last decref
    assert_eq!(s.stage(20, -1, 3).unwrap(), (0, 0)); // redundant -> skipped
    assert_eq!(s.get(20).unwrap(), 0);
    assert_eq!(clamped() - c0, 1);
}

#[test]
fn stage_overflow_still_errors() {
    // An increment past u32::MAX is genuinely catastrophic and must still
    // surface as an Err — it is never silently absorbed, and it is not a
    // clamp.
    let (_d, s) = make_shard();
    let c0 = clamped();
    s.stage(30, i64::from(u32::MAX), 1).unwrap(); // rc -> u32::MAX
    assert!(s.stage(30, 1, 2).is_err());
    assert_eq!(clamped() - c0, 0);
}

#[test]
fn zero_to_one_to_zero_to_one_birth_lsn() {
    let (_d, s) = make_shard();
    s.stage(10, 1, 100).unwrap();
    s.flush().unwrap();
    s.stage(10, -1, 101).unwrap();
    s.flush().unwrap();
    assert_eq!(s.get_entry(10).unwrap(), RcEntry::ZERO);
    s.stage(10, 1, 200).unwrap();
    s.flush().unwrap();
    assert_eq!(
        s.get_entry(10).unwrap(),
        RcEntry {
            rc: 1,
            birth_lsn: 200
        }
    );
}

#[test]
fn iter_live_flushed_skips_zero() {
    let (_d, s) = make_shard();
    s.stage(1, 1, 1).unwrap();
    s.stage(2, 1, 1).unwrap();
    s.stage(2, -1, 2).unwrap();
    s.stage(3, 3, 3).unwrap();
    let live = s.iter_live_flushed().unwrap();
    assert_eq!(live.len(), 2);
    assert_eq!(live[0].0, 1);
    assert_eq!(live[1].0, 3);
}

#[test]
fn round_trip_via_open() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages");
    let meta_page_id;
    {
        let page_store = Arc::new(PageStore::create(&path).unwrap());
        let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
        let s = RcShard::create(page_store, page_cache).unwrap();
        meta_page_id = s.meta_page_id();
        s.stage(10, 5, 100).unwrap();
        s.stage(20, 2, 200).unwrap();
        s.flush().unwrap();
    }
    let page_store = Arc::new(PageStore::open(&path).unwrap());
    let page_cache = Arc::new(PageCache::new(page_store.clone(), 16 * 1024 * 1024));
    let s = RcShard::open(page_store, page_cache, meta_page_id).unwrap();
    assert_eq!(s.get(10).unwrap(), 5);
    assert_eq!(s.get(20).unwrap(), 2);
    assert_eq!(s.get_entry(10).unwrap().birth_lsn, 100);
}

#[test]
fn begin_checkpoint_drains_delta_and_stages_without_overwriting_disk() {
    // Drainer-disabled path (priority-1) — verifies the original
    // semantics still hold.
    let (_d, s) = make_shard();
    s.stage(10, 5, 100).unwrap();
    s.stage(20, 3, 100).unwrap();

    let ckpt = s.begin_checkpoint().unwrap();
    assert_eq!(s.get(10).unwrap(), 5, "stage value visible via cache");
    assert_eq!(s.get(20).unwrap(), 3);
    assert!(s.allocated_data_pages() >= 1);
    assert!(!ckpt.is_empty());
    s.abort_checkpoint(ckpt, 0);
    assert_eq!(s.get(10).unwrap(), 5);
    assert_eq!(s.get(20).unwrap(), 3);
}

#[test]
fn checkpoint_pipeline_round_trips_through_disk() {
    let (_d, s) = make_shard();
    s.stage(7, 2, 50).unwrap();
    s.stage(800, 4, 60).unwrap();

    let ckpt = s.begin_checkpoint().unwrap();
    assert!(!ckpt.is_empty());
    assert_eq!(ckpt.fresh_page_ids().len(), 2);

    s.array.write_staged_pages(&ckpt.staged).unwrap();
    let new_chain = s.write_meta_chain(&ckpt, 0).unwrap();
    s.install_meta_chain(new_chain);
    assert_eq!(s.get(7).unwrap(), 2);
    assert_eq!(s.get(800).unwrap(), 4);
}

#[test]
fn abort_then_retry_does_not_double_apply_via_replay_skip() {
    let (_d, s) = make_shard();
    s.stage(10, 5, 100).unwrap();

    let ckpt = s.begin_checkpoint().unwrap();
    s.array.write_staged_pages(&ckpt.staged).unwrap();
    let _ = s.write_meta_chain(&ckpt, 0).unwrap();
    s.abort_checkpoint(ckpt, 0);
    assert_eq!(s.get(10).unwrap(), 5, "value still observable post-abort");
    s.flush().unwrap();
    assert_eq!(s.get(10).unwrap(), 5, "no double-apply on retry");
    assert_eq!(s.get_entry(10).unwrap().birth_lsn, 100);
}

#[test]
fn empty_checkpoint_is_no_op() {
    let (_d, s) = make_shard();
    let ckpt = s.begin_checkpoint().unwrap();
    assert!(ckpt.is_empty());
    let new_chain = s.write_meta_chain(&ckpt, 0).unwrap();
    s.install_meta_chain(new_chain);
    let ckpt2 = s.begin_checkpoint().unwrap();
    s.abort_checkpoint(ckpt2, 0);
}

#[test]
fn many_ops_one_shard_correctness() {
    use std::collections::HashMap;
    let (_d, s) = make_shard();
    let mut model: HashMap<Pba, i64> = HashMap::new();
    let ops: Vec<(Pba, i64, Lsn)> = (1u64..1000)
        .map(|i| {
            let pba = (i * 7) % (super::super::ENTRIES_PER_PAGE as u64 * 5);
            let delta = if i % 3 == 0 { -1i64 } else { 1i64 };
            (pba, delta, i)
        })
        .collect();
    for &(pba, delta, lsn) in &ops {
        let model_prev = *model.get(&pba).unwrap_or(&0);
        let model_new = model_prev + delta;
        if model_new < 0 {
            continue;
        }
        let (_prev, new) = s.stage(pba, delta, lsn).unwrap();
        assert_eq!(new, model_new as u32);
        model.insert(pba, model_new);
    }
    s.flush().unwrap();
    for (&pba, &expected) in &model {
        assert_eq!(s.get(pba).unwrap(), expected as u32, "pba {pba}");
    }
}
