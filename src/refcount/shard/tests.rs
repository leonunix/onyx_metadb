use super::*;
use tempfile::TempDir;

fn make_shard() -> (TempDir, RcShard) {
    make_shard_with_cache_bytes(16 * 1024 * 1024)
}

fn make_shard_with_cache_bytes(cache_bytes: u64) -> (TempDir, RcShard) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("pages");
    let page_store = Arc::new(PageStore::create(&path).unwrap());
    let page_cache = Arc::new(PageCache::new(page_store.clone(), cache_bytes));
    let s = RcShard::create(page_store, page_cache).unwrap();
    (dir, s)
}

// Most tests don't care which BFG a stage lands in — use slot 0 (bfg=0).
const T0: crate::types::Bfg = 0;

#[test]
fn stage_then_get_sees_pending() {
    let (_d, s) = make_shard();
    assert_eq!(s.stage(T0, 10, 1, 100).unwrap(), (0, 1));
    assert_eq!(s.get(10).unwrap(), 1);
}

#[test]
fn stage_accumulates_across_ops() {
    let (_d, s) = make_shard();
    s.stage(T0, 10, 1, 100).unwrap();
    s.stage(T0, 10, 2, 101).unwrap();
    s.stage(T0, 10, -1, 102).unwrap();
    assert_eq!(s.get(10).unwrap(), 2);
}

#[test]
fn active_slot_mask_keeps_open_and_nonempty_maps() {
    let (_d, s) = make_shard();
    s.delta_slots[1].lock().merge(10, 2, 20);
    {
        let mut zero_net = s.delta_slots[3].lock();
        zero_net.merge(10, -1, 25);
        zero_net.merge(20, 1, 30);
        zero_net.merge(20, -1, 31);
    }

    let slots: Vec<_> = s.delta_slots.iter().map(|slot| slot.lock()).collect();
    let active = active_slot_mask(&slots, 0);
    assert_eq!(active, 0b1011);
    assert_eq!(
        scan_active_locked_pending(&slots, active, 10),
        (1, 25, true)
    );
    // A zero-net entry is still semantically active: its presence and LSN
    // participate in replay-skip and merged-entry generation decisions.
    assert_eq!(
        scan_active_locked_pending(&slots, active, 20),
        (0, 31, true)
    );
}

#[test]
fn stage_batch_probes_zero_net_pending_before_replay_skip() {
    let (_d, s) = make_shard();
    // Raise the shared page generation while leaving the target PBA at rc=0.
    s.array
        .apply_deltas(vec![(
            11,
            Pending {
                delta: 1,
                last_lsn: 100,
            },
        )])
        .unwrap();
    assert_eq!(s.array.page_lsn(10).unwrap(), 100);

    // DeltaMap deliberately retains zero-net entries. Its presence means the
    // target has pending history, so page_lsn >= replay_lsn must not trigger
    // the !any replay-skip fast path.
    {
        let mut pending = s.delta_slots[0].lock();
        pending.merge(10, 1, 40);
        pending.merge(10, -1, 41);
    }
    let (staged, _) = s.stage_batch(1, &[(10, 1)], 50).unwrap();
    assert_eq!(staged, vec![(0, 1)]);
    assert_eq!(s.get(10).unwrap(), 1);
}

#[test]
fn stage_batch_probes_and_updates_zero_net_open_pending() {
    let (_d, s) = make_shard();
    s.array
        .apply_deltas(vec![(
            11,
            Pending {
                delta: 1,
                last_lsn: 100,
            },
        )])
        .unwrap();

    {
        let mut open = s.delta_slots[1].lock();
        open.merge(10, 1, 40);
        open.merge(10, -1, 41);
    }
    let (staged, _) = s.stage_batch(1, &[(10, 1)], 50).unwrap();

    assert_eq!(staged, vec![(0, 1)]);
    assert_eq!(
        s.delta_slots[1].lock().get(10),
        Some(Pending {
            delta: 1,
            last_lsn: 50,
        })
    );
}

#[test]
fn stage_batch_handles_an_initially_empty_open_slot() {
    let (_d, s) = make_shard();
    s.stage(T0, 10, 2, 10).unwrap();
    assert!(s.delta_slots[1].lock().is_empty());

    let lsn = (20..)
        .find(|&candidate| !sample_refcount_breakdown(candidate))
        .unwrap();
    let (staged, _) = s
        .stage_batch(1, &[(10, -1), (20, 3), (30, 4)], lsn)
        .unwrap();

    // The open slot is included even though it was initially empty, then
    // receives all three distinct PBAs without changing the snapshot mask.
    assert_eq!(staged, vec![(2, 1), (0, 3), (0, 4)]);
    assert_eq!(s.get_many(&[10, 20, 30]).unwrap(), vec![1, 3, 4]);
}

#[test]
fn batch_get_and_stage_preserve_per_pba_results() {
    let (_d, s) = make_shard();
    s.stage(T0, 10, 2, 10).unwrap();
    s.stage(1, 20, 3, 11).unwrap();

    let sampled_lsn = (1..).find(|&lsn| sample_refcount_breakdown(lsn)).unwrap();
    let (staged, timings) = s
        .stage_batch(2, &[(10, -1), (20, 2), (30, 4)], sampled_lsn)
        .unwrap();
    assert_eq!(staged, vec![(2, 1), (3, 5), (0, 4)]);
    assert_eq!(timings.sampled_pbas, 3);
    assert_eq!(timings.base_lookup_attempts, 1);
    assert_eq!(timings.epoch_retries, 0);
    assert_eq!(s.get_many(&[30, 10, 99, 20]).unwrap(), vec![4, 1, 0, 5]);
}

#[test]
fn stage_batch_keeps_replay_skip_semantics() {
    let (_d, s) = make_shard();
    let unsampled_lsn = (1..).find(|&lsn| !sample_refcount_breakdown(lsn)).unwrap();
    s.stage(T0, 10, 1, unsampled_lsn).unwrap();
    s.flush().unwrap();
    let (staged, timings) = s.stage_batch(1, &[(10, 1)], unsampled_lsn).unwrap();
    assert_eq!(staged, vec![(1, 1)]);
    assert_eq!(timings.sampled_pbas, 0);
    assert_eq!(timings.base_lookup_attempts, 1);
    assert_eq!(timings.epoch_retries, 0);
    assert_eq!(s.get(10).unwrap(), 1);
    assert!(s.delta_slots[1].lock().get(10).is_none());
}

#[test]
fn stage_batch_skip_does_not_mutate_or_insert_open_entries() {
    let (_d, s) = make_shard();
    s.delta_slots[1].lock().merge(10, 1, 10);

    let (staged, _) = s.stage_batch(1, &[(10, -2), (20, -1)], 20).unwrap();

    assert_eq!(staged, vec![(1, 1), (0, 0)]);
    assert_eq!(
        s.delta_slots[1].lock().get(10),
        Some(Pending {
            delta: 1,
            last_lsn: 10,
        })
    );
    assert!(s.delta_slots[1].lock().get(20).is_none());
}

#[test]
fn stage_batch_error_leaves_open_entry_unchanged() {
    let (_d, s) = make_shard();
    let pending = Pending {
        delta: i64::from(u32::MAX) + 1,
        last_lsn: 10,
    };
    s.delta_slots[1].lock().merge_pending(10, pending);

    let err = s.stage_batch(1, &[(10, 1)], 20).err().unwrap();

    assert!(matches!(err, MetaDbError::InvalidArgument(_)));
    assert_eq!(s.delta_slots[1].lock().get(10), Some(pending));
}

#[test]
fn stage_batch_retries_when_checkpoint_moves_state_after_base_lookup() {
    let (_d, s) = make_shard();
    let pba = 10;
    s.array
        .apply_deltas(vec![(
            pba,
            Pending {
                delta: 1,
                last_lsn: 100,
            },
        )])
        .unwrap();
    // Logical rc=2: durable base 1 plus the Syncing slot's pending +1.
    s.delta_slots[0].lock().merge(pba, 1, 200);

    let s = Arc::new(s);
    let hook = Arc::new(StageBatchTestHook::new());
    *s.stage_batch_test_hook.lock() = Some(hook.clone());
    let stage = {
        let s = s.clone();
        std::thread::spawn(move || s.stage_batch(1, &[(pba, -2)], 300))
    };

    // The first lookup has captured base=1, but stage_batch has not acquired
    // any slot lock. A checkpoint can therefore publish base=2 and clear slot
    // 0 without waiting behind the page lookup.
    hook.after_lookup.wait();
    for slot in &s.delta_slots {
        assert!(
            slot.try_lock().is_some(),
            "base lookup must not hold slot locks"
        );
    }
    let _checkpoint = s.begin_checkpoint(0).unwrap();
    hook.resume.wait();

    let (staged, timings) = stage.join().unwrap().unwrap();
    assert_eq!(hook.lookup_count.load(Ordering::SeqCst), 2);
    assert_eq!(timings.base_lookup_attempts, 2);
    assert_eq!(timings.epoch_retries, 1);
    assert_eq!(staged, vec![(2, 0)]);
    assert_eq!(s.get(pba).unwrap(), 0);
}

#[test]
fn stage_retries_when_checkpoint_moves_state_after_base_lookup() {
    let (_d, s) = make_shard();
    let pba = 10;
    s.array
        .apply_deltas(vec![(
            pba,
            Pending {
                delta: 1,
                last_lsn: 100,
            },
        )])
        .unwrap();
    // Logical rc=2: durable base 1 plus the Syncing slot's pending +1.
    s.delta_slots[0].lock().merge(pba, 1, 200);

    let s = Arc::new(s);
    let hook = Arc::new(StageBatchTestHook::new());
    *s.stage_batch_test_hook.lock() = Some(hook.clone());
    let stage = {
        let s = s.clone();
        std::thread::spawn(move || s.stage(1, pba, -2, 300))
    };

    // The first lookup captured base=1. Move the pending +1 into the array
    // before stage validates; accepting the old base with the now-empty slot
    // would miss the real 2 -> 0 freed transition.
    hook.after_lookup.wait();
    let _checkpoint = s.begin_checkpoint(0).unwrap();
    hook.resume.wait();

    let transition = stage.join().unwrap().unwrap();
    assert_eq!(hook.lookup_count.load(Ordering::SeqCst), 2);
    assert_eq!(transition, (2, 0));
    assert_eq!(s.get(pba).unwrap(), 0);
}

#[test]
fn streaming_checkpoint_bounds_overlay_and_releases_chunk_page_arcs() {
    const PAGE_COUNT: usize = 5;
    const CHUNK_PAGES: usize = 2;

    // A zero-capacity shared cache makes the payload lifetime observable: once
    // the streaming checkpoint clears the overlay and drops its local chunk,
    // no cache Arc can keep the page alive.
    let (_d, s) = make_shard_with_cache_bytes(0);
    for page_idx in 0..PAGE_COUNT {
        let pba = (page_idx * ENTRIES_PER_PAGE + 7) as Pba;
        s.stage(T0, pba, 1, 100 + page_idx as u64).unwrap();
    }

    let hook = Arc::new(StreamingCheckpointTestHook::new(false));
    *s.streaming_checkpoint_test_hook.lock() = Some(hook.clone());
    let ckpt = s
        .begin_checkpoint_streaming_capped(T0, CHUNK_PAGES)
        .unwrap();

    assert_eq!(ckpt.data_pages_count(), PAGE_COUNT);
    assert!(ckpt.staged.pages.is_empty());
    let stream = ckpt.streaming_write_stats();
    assert_eq!(stream.calls, 3);
    assert_eq!(stream.pages, PAGE_COUNT as u64);
    assert_eq!(stream.max_chunk_pages, CHUNK_PAGES as u64);
    assert!(stream.max_chunk_us <= stream.service_us);
    assert_eq!(hook.chunks.load(Ordering::SeqCst), 3);
    assert!(hook.max_chunk_pages.load(Ordering::Relaxed) <= CHUNK_PAGES);
    assert!(hook.max_overlay_pages.load(Ordering::Relaxed) <= CHUNK_PAGES);
    assert_eq!(s.array.staged_overlay_len(), 0);
    assert!(
        hook.page_weaks
            .lock()
            .iter()
            .all(|page| page.upgrade().is_none()),
        "streaming RcCheckpoint retained a sealed page Arc"
    );
}

#[test]
fn stage_batch_retries_when_streaming_checkpoint_moves_a_later_chunk() {
    let (_d, s) = make_shard();
    let p0 = 7;
    let p1 = (ENTRIES_PER_PAGE + 7) as Pba;
    s.array
        .apply_deltas(vec![
            (
                p0,
                Pending {
                    delta: 1,
                    last_lsn: 100,
                },
            ),
            (
                p1,
                Pending {
                    delta: 1,
                    last_lsn: 100,
                },
            ),
        ])
        .unwrap();
    s.stage(0, p0, 1, 200).unwrap();
    s.stage(0, p1, 1, 201).unwrap();

    let s = Arc::new(s);
    let checkpoint_hook = Arc::new(StreamingCheckpointTestHook::new(true));
    *s.streaming_checkpoint_test_hook.lock() = Some(checkpoint_hook.clone());
    let checkpoint = {
        let s = s.clone();
        std::thread::spawn(move || s.begin_checkpoint_streaming_capped(0, 1))
    };
    checkpoint_hook.after_first_chunk.wait();

    let stage_hook = Arc::new(StageBatchTestHook::new());
    *s.stage_batch_test_hook.lock() = Some(stage_hook.clone());
    let stage = {
        let s = s.clone();
        std::thread::spawn(move || s.stage_batch(1, &[(p0, -2), (p1, -2)], 300))
    };
    // stage_batch captured the mixed physical representation (first page
    // folded, second still pending) at one logically coherent instant.
    stage_hook.after_lookup.wait();

    // Let the checkpoint publish+clear the second chunk before stage_batch
    // validates its epoch. The batch must discard both old bases and retry.
    checkpoint_hook.resume.wait();
    let ckpt = checkpoint.join().unwrap().unwrap();
    assert_eq!(ckpt.data_pages_count(), 2);
    stage_hook.resume.wait();

    let (staged, timings) = stage.join().unwrap().unwrap();
    assert_eq!(stage_hook.lookup_count.load(Ordering::SeqCst), 2);
    assert_eq!(timings.base_lookup_attempts, 2);
    assert_eq!(timings.epoch_retries, 1);
    assert_eq!(staged, vec![(2, 0), (2, 0)]);
    assert_eq!(s.get(p0).unwrap(), 0);
    assert_eq!(s.get(p1).unwrap(), 0);
}

#[test]
fn streaming_chunk_stage_error_cleans_published_pages_and_fresh_reservations() {
    let (_d, s) = make_shard();
    let p0 = 7;
    let p1 = (ENTRIES_PER_PAGE + 7) as Pba;
    s.delta_slots[0].lock().merge(p0, 1, 100);
    s.delta_slots[0]
        .lock()
        .merge(p1, i64::from(u32::MAX) + 1, 101);

    let err = s.begin_checkpoint_streaming_capped(0, 2).err().unwrap();
    assert!(matches!(err, MetaDbError::InvalidArgument(_)));
    assert_eq!(s.array.staged_overlay_len(), 0);
    assert_eq!(s.allocated_data_pages(), 0);
    assert_eq!(s.delta_slots[0].lock().get(p0).unwrap().delta, 1);
    assert_eq!(
        s.delta_slots[0].lock().get(p1).unwrap().delta,
        i64::from(u32::MAX) + 1
    );
}

#[test]
fn streaming_later_chunk_stage_error_reclaims_prior_fresh_page() {
    let (_d, s) = make_shard();
    let first = 7;
    let second = (ENTRIES_PER_PAGE + 7) as Pba;
    s.delta_slots[0].lock().merge(first, 1, 100);
    s.delta_slots[0]
        .lock()
        .merge(second, i64::from(u32::MAX) + 1, 101);

    let err = s.begin_checkpoint_streaming_capped(0, 1).err().unwrap();
    assert!(matches!(err, MetaDbError::InvalidArgument(_)));

    // Chunk 1 completed its write, but no meta-chain submission started. Its
    // fresh page is therefore unreachable and must be detached and restored
    // alongside chunk 2's still-pending delta.
    assert_eq!(s.array.get(first).unwrap().rc, 0);
    assert_eq!(s.get(first).unwrap(), 1);
    assert_eq!(s.delta_slots[0].lock().get(first).unwrap().delta, 1);
    assert_eq!(
        s.delta_slots[0].lock().get(second).unwrap().delta,
        i64::from(u32::MAX) + 1
    );
    let page_table = s.array.page_table_snapshot();
    assert_eq!(page_table[0], 0);
    assert_eq!(page_table.get(1).copied().unwrap_or(0), 0);
    assert_eq!(s.allocated_data_pages(), 0);
    assert_eq!(s.array.staged_overlay_len(), 0);
}

#[test]
fn streaming_later_chunk_write_error_reclaims_all_fresh_pages() {
    let (_d, s) = make_shard();
    let first = 7;
    let second = (ENTRIES_PER_PAGE + 7) as Pba;
    s.stage(T0, first, 1, 100).unwrap();
    s.stage(T0, second, 1, 101).unwrap();

    let hook = Arc::new(StreamingCheckpointTestHook::new(false));
    hook.fail_before_write_chunk(1);
    *s.streaming_checkpoint_test_hook.lock() = Some(hook);

    let err = s.begin_checkpoint_streaming_capped(T0, 1).err().unwrap();
    assert!(matches!(err, MetaDbError::InjectedFault(_)));
    assert_eq!(s.array.page_table_snapshot(), vec![0, 0]);
    assert_eq!(s.allocated_data_pages(), 0);
    assert_eq!(s.array.staged_overlay_len(), 0);
    assert_eq!(s.delta_slots[0].lock().get(first).unwrap().delta, 1);
    assert_eq!(s.delta_slots[0].lock().get(second).unwrap().delta, 1);
    assert_eq!(s.get(first).unwrap(), 1);
    assert_eq!(s.get(second).unwrap(), 1);
}

#[test]
fn streaming_abort_keeps_written_existing_pages_and_restores_only_fresh_deltas() {
    let (_d, s) = make_shard();
    let existing = 7;
    let fresh = (ENTRIES_PER_PAGE + 7) as Pba;
    s.array
        .apply_deltas(vec![(
            existing,
            Pending {
                delta: 1,
                last_lsn: 100,
            },
        )])
        .unwrap();
    s.stage(0, existing, 1, 200).unwrap();
    s.stage(0, fresh, 1, 201).unwrap();

    let ckpt = s.begin_checkpoint_streaming_capped(0, 1).unwrap();
    assert_eq!(s.get(existing).unwrap(), 2);
    assert_eq!(s.get(fresh).unwrap(), 1);
    s.abort_checkpoint(ckpt, 0);

    // Existing bytes already completed their write and remain authoritative;
    // restoring their delta would transiently double-count before restart.
    assert!(s.delta_slots[0].lock().get(existing).is_none());
    assert_eq!(s.get(existing).unwrap(), 2);
    // A fresh pid can be detached and freed exactly, so its pending delta is
    // restored for the fatal cycle's diagnostic in-memory view.
    assert_eq!(s.delta_slots[0].lock().get(fresh).unwrap().delta, 1);
    assert_eq!(s.get(fresh).unwrap(), 1);
    assert_eq!(s.allocated_data_pages(), 1);
}

#[test]
fn flush_moves_pending_to_array() {
    let (_d, s) = make_shard();
    s.stage(T0, 10, 5, 100).unwrap();
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
    s.stage(T0, 10, 1, 1).unwrap(); // rc 0 -> 1
    // Decref by 2 when only 1 ref exists would underflow. Instead of
    // erroring the redundant decref is skipped and the count is left
    // unchanged.
    assert_eq!(s.stage(T0, 10, -2, 2).unwrap(), (1, 1));
    assert_eq!(s.get(10).unwrap(), 1);
    assert_eq!(clamped() - c0, 1);
    // The delta map is not corrupted: a subsequent legitimate decref
    // still works.
    assert_eq!(s.stage(T0, 10, -1, 3).unwrap(), (1, 0));
    assert_eq!(s.get(10).unwrap(), 0);
}

#[test]
fn stage_double_decref_of_freed_pba_is_benign() {
    // The production race: two ops remove the same last reference. The
    // first takes the count to 0; the second finds it already 0 and is
    // skipped — not an error, never negative.
    let (_d, s) = make_shard();
    let c0 = clamped();
    s.stage(T0, 20, 1, 1).unwrap(); // rc 0 -> 1
    assert_eq!(s.stage(T0, 20, -1, 2).unwrap(), (1, 0)); // legit last decref
    assert_eq!(s.stage(T0, 20, -1, 3).unwrap(), (0, 0)); // redundant -> skipped
    assert_eq!(s.get(20).unwrap(), 0);
    assert_eq!(clamped() - c0, 1);
}

#[test]
fn stage_decref_if_positive_is_coherent_and_zero_is_not_an_underflow() {
    let (_d, s) = make_shard();
    let c0 = clamped();

    assert_eq!(s.stage_decref_if_positive(T0, 21, 1).unwrap(), (0, 0));
    assert_eq!(clamped(), c0, "expected zero must be a quiet no-op");

    s.stage(T0, 21, 1, 2).unwrap();
    assert_eq!(s.stage_decref_if_positive(T0, 21, 3).unwrap(), (1, 0));
    assert_eq!(s.get(21).unwrap(), 0);
    assert_eq!(clamped(), c0);
}

fn read_floored() -> u64 {
    crate::refcount::read_underflow_floored_total()
}

#[test]
fn get_floors_torn_read_but_stage_rejects_incoherent_state() {
    // Reproduce the torn `(pending, array)` state a concurrent rc fold
    // can leave: the array base for P has already folded a pending decref
    // (rc=0), but a stale `-1` pending still sits in a slot. The logical
    // rc floor is 0; `merge_read_or_floor` floors instead of erroring.
    let (_d, s) = make_shard();
    let p = 4242;
    let floored0 = read_floored();
    // array(p) defaults to rc=0; inject the stale -1 pending into slot 0.
    s.delta_slots[0].lock().merge(p, -1, 500);

    // `get` must floor (base 0 + pending -1 -> 0), NOT return Err.
    assert_eq!(s.get(p).unwrap(), 0);
    assert!(read_floored() > floored0, "get should record a read floor");

    // `stage` takes a fold-coherent base+slots sample. Seeing this impossible
    // negative state there is a model error, not a read tear to hide.
    let floored1 = read_floored();
    assert!(s.stage(T0, p, -1, 501).is_err());
    assert_eq!(read_floored(), floored1, "stage must not use read floor");
    assert_eq!(s.get(p).unwrap(), 0);
}

#[test]
fn stage_overflow_still_errors() {
    let (_d, s) = make_shard();
    let c0 = clamped();
    s.stage(T0, 30, i64::from(u32::MAX), 1).unwrap(); // rc -> u32::MAX
    assert!(s.stage(T0, 30, 1, 2).is_err());
    assert_eq!(clamped() - c0, 0);
}

#[test]
fn zero_to_one_to_zero_to_one_birth_lsn() {
    let (_d, s) = make_shard();
    s.stage(T0, 10, 1, 100).unwrap();
    s.flush().unwrap();
    s.stage(T0, 10, -1, 101).unwrap();
    s.flush().unwrap();
    assert_eq!(s.get_entry(10).unwrap(), RcEntry::ZERO);
    s.stage(T0, 10, 1, 200).unwrap();
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
    s.stage(T0, 1, 1, 1).unwrap();
    s.stage(T0, 2, 1, 1).unwrap();
    s.stage(T0, 2, -1, 2).unwrap();
    s.stage(T0, 3, 3, 3).unwrap();
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
        s.stage(T0, 10, 5, 100).unwrap();
        s.stage(T0, 20, 2, 200).unwrap();
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
fn begin_checkpoint_all_slots_stages_without_overwriting_disk() {
    let (_d, s) = make_shard();
    s.stage(T0, 10, 5, 100).unwrap();
    s.stage(T0, 20, 3, 100).unwrap();

    let ckpt = s.begin_checkpoint_all_slots(false).unwrap();
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
    s.stage(T0, 7, 2, 50).unwrap();
    s.stage(T0, 800, 4, 60).unwrap();

    let ckpt = s.begin_checkpoint_all_slots(false).unwrap();
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
    s.stage(T0, 10, 5, 100).unwrap();

    let ckpt = s.begin_checkpoint_all_slots(false).unwrap();
    s.array.write_staged_pages(&ckpt.staged).unwrap();
    // Abort remains legal after data-page IO while the stable meta head still
    // names the old page table. Once meta-chain write submission starts, fresh
    // pages may be reachable and must be retained instead.
    s.abort_checkpoint(ckpt, 0);
    assert_eq!(s.get(10).unwrap(), 5, "value still observable post-abort");
    s.flush().unwrap();
    assert_eq!(s.get(10).unwrap(), 5, "no double-apply on retry");
    assert_eq!(s.get_entry(10).unwrap().birth_lsn, 100);
}

#[test]
fn empty_checkpoint_is_no_op() {
    let (_d, s) = make_shard();
    let ckpt = s.begin_checkpoint_all_slots(false).unwrap();
    assert!(ckpt.is_empty());
    let new_chain = s.write_meta_chain(&ckpt, 0).unwrap();
    s.install_meta_chain(new_chain);
    let ckpt2 = s.begin_checkpoint_all_slots(false).unwrap();
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
        let (_prev, new) = s.stage(T0, pba, delta, lsn).unwrap();
        assert_eq!(new, model_new as u32);
        model.insert(pba, model_new);
    }
    s.flush().unwrap();
    for (&pba, &expected) in &model {
        assert_eq!(s.get(pba).unwrap(), expected as u32, "pba {pba}");
    }
}

// ── BFG-slot ring tests ──────────────────────────────────────────────

#[test]
fn stage_routes_to_bfg_slot_and_reads_sum_across_slots() {
    let (_d, s) = make_shard();
    // Two BFGs touch the same PBA, landing in different ring slots.
    s.stage(0, 10, 1, 100).unwrap(); // slot 0
    s.stage(1, 10, 1, 200).unwrap(); // slot 1
    // The deltas live in distinct slots…
    assert_eq!(s.delta_slots[0].lock().get(10).unwrap().delta, 1);
    assert_eq!(s.delta_slots[1].lock().get(10).unwrap().delta, 1);
    assert!(s.delta_slots[2].lock().get(10).is_none());
    // …and the cumulative read sums them.
    assert_eq!(s.get(10).unwrap(), 2);
}

#[test]
fn begin_checkpoint_folds_only_the_syncing_slot() {
    let (_d, s) = make_shard();
    s.stage(0, 10, 3, 100).unwrap(); // slot 0
    s.stage(1, 20, 5, 200).unwrap(); // slot 1

    // Fold ONLY slot 0 (bfg=0).
    let ckpt0 = s.begin_checkpoint(0).unwrap();
    assert!(!ckpt0.is_empty());
    // Slot 0 is now empty; slot 1 still pending.
    assert!(s.delta_slots[0].lock().get(10).is_none());
    assert_eq!(s.delta_slots[1].lock().get(20).unwrap().delta, 5);
    // Both PBAs still read correctly (10 from array, 20 from slot 1).
    assert_eq!(s.get(10).unwrap(), 3);
    assert_eq!(s.get(20).unwrap(), 5);

    // Fold slot 1 (bfg=1).
    let ckpt1 = s.begin_checkpoint(1).unwrap();
    assert!(!ckpt1.is_empty());
    assert_eq!(s.get(20).unwrap(), 5);
}

#[test]
fn begin_checkpoint_empty_slot_is_no_op() {
    let (_d, s) = make_shard();
    // No deltas for bfg=2's slot → empty checkpoint.
    let ckpt = s.begin_checkpoint(2).unwrap();
    assert!(ckpt.is_empty());
}

#[test]
fn cross_bfg_incref_then_decref_same_pba_folds_to_correct_count() {
    let (_d, s) = make_shard();
    // BFG 0 increfs P to 1; BFG 1 decrefs it back to 0.
    s.stage(0, 42, 1, 100).unwrap();
    s.stage(1, 42, -1, 200).unwrap();
    // Cumulative read across both slots = 0.
    assert_eq!(s.get(42).unwrap(), 0);

    // Fold bfg 0 → array has rc=1 for P, slot 1 still holds the -1.
    let _c0 = s.begin_checkpoint(0).unwrap();
    assert_eq!(s.get(42).unwrap(), 0, "array(1) + slot1(-1) = 0");

    // Fold bfg 1 → the decref lands, rc back to 0.
    let _c1 = s.begin_checkpoint(1).unwrap();
    assert_eq!(s.get(42).unwrap(), 0);
    // Persist + reopen-style check via flush: still 0.
    s.flush().unwrap();
    assert_eq!(s.get(42).unwrap(), 0);
}

#[test]
fn freed_pba_surfaces_on_cumulative_zero_not_per_slot() {
    // P has a durable rc=1 (folded). A decref in a later BFG slot takes
    // the cumulative to 0 → stage returns (1, 0) so the caller surfaces
    // freed_pba. The decref before fold reads the array base (1).
    let (_d, s) = make_shard();
    s.stage(0, 77, 1, 100).unwrap();
    s.begin_checkpoint(0).unwrap(); // P durable at rc=1
    assert_eq!(s.get(77).unwrap(), 1);
    // Decref in BFG 1's slot.
    assert_eq!(s.stage(1, 77, -1, 200).unwrap(), (1, 0));
    assert_eq!(s.get(77).unwrap(), 0);
}

#[test]
fn pending_delta_count_sums_all_slots() {
    let (_d, s) = make_shard();
    s.stage(0, 1, 1, 1).unwrap();
    s.stage(1, 2, 1, 1).unwrap();
    s.stage(2, 3, 1, 1).unwrap();
    assert_eq!(s.pending_delta_count(), 3);
}

/// REPRO: the cold-path force-fold (`RcShard::flush()` →
/// `begin_checkpoint_all_slots(force=true)`) RE-APPLIES a decref the array
/// already reflects, driving a still-live rc below its true floor.
///
/// PBA-rc monotonicity guard (BFG — replaces the page-rc
/// force-fold premature-free repro `c8a6bfc`, which was deliberately RED until
/// the page-rc array was deleted).
///
/// The premature-free case lived in the now-DELETED per-L2P-page refcount array,
/// whose `stage_unskippable` + buffer-drain radix-key ordering produced
/// NON-MONOTONE deltas: a decref the array had already folded could reappear in
/// a slot with `last_lsn <= page_generation`, and a force fold (`flush()` →
/// `begin_checkpoint_all_slots(force=true)`, which BYPASSES the per-page
/// replay-skip) would re-apply it, flooring a live rc to 0 = premature free.
///
/// The KEPT PBA refcount (`RcShard`) is force-fold-SAFE by a different, stronger
/// invariant: its ONLY staging entry point is [`stage`](RcShard::stage) (WAL
/// lsn), whose replay-skip early-return REFUSES to stage a delta whose lsn is at
/// or below the array page's already-folded generation. So a stale / non-monotone
/// delta never reaches a slot in the first place, and the force fold has nothing
/// to re-apply. (Page-rc's vulnerability was precisely that it bypassed `stage`
/// via `stage_unskippable` + direct drain.)
///
/// This guards that invariant at the production entry point: re-stage an
/// already-folded decref with a stale lsn and assert `stage` does NOT lower the
/// reported live rc (it skips/clamps), so the subsequent force fold leaves the
/// live rc intact. Under the old page-rc path the same shape floored rc to 0.
#[test]
fn pba_rc_stage_skips_stale_decref_so_force_fold_cannot_reapply() {
    let (_d, s) = make_shard();
    let p: Pba = 4242;

    // incref P by 2 @ lsn 100, fold → array rc=2, page gen=100.
    s.stage(0, p, 2, 100).unwrap();
    s.begin_checkpoint(0).unwrap();
    assert_eq!(s.get(p).unwrap(), 2);

    // legitimate decref -1 @ lsn 200, fold → array rc=1, page gen=200.
    s.stage(1, p, -1, 200).unwrap();
    s.begin_checkpoint(1).unwrap();
    assert_eq!(s.get(p).unwrap(), 1, "live rc after the legitimate decref");

    // The SAME -1 reappears with a NON-MONOTONE lsn (150 <= page gen 200) — a
    // recovery / abort-retry residue. Routed through the PRODUCTION entry point
    // `stage` (NOT a direct `delta_slots[..].merge`, which is exactly the
    // bypass the deleted page-rc path used). The replay-skip early-return must
    // refuse to stage it (page gen 200 >= lsn 150, slots empty after the folds
    // above), so the reported live rc stays 1 — no stale delta reaches a slot.
    let (_prev, new) = s.stage(2, p, -1, 150).unwrap();
    assert_eq!(
        new, 1,
        "stage must not lower the live rc for a stale decref (page gen 200 >= \
         lsn 150) — no non-monotone delta may reach a PBA-rc slot"
    );

    // Because `stage` staged nothing, the cold-path force fold (`flush()` →
    // begin_checkpoint_all_slots(true)) has no stale delta to re-apply: the
    // live rc stays 1. (Under the deleted page-rc array this same shape floored
    // rc to 0 = the premature-free CRC this port exists to kill.)
    let clamps_before = crate::refcount::underflow_clamped_total();
    s.flush().unwrap();
    let clamps_after = crate::refcount::underflow_clamped_total();
    assert_eq!(
        s.get(p).unwrap(),
        1,
        "force fold must leave the live rc intact (no stale delta was staged); \
         underflow clamps fired: {}",
        clamps_after - clamps_before,
    );
}

/// Regression for the rc_authoritative premature-free CRC (2026-06-12 r2
/// soak, pba 661307): a fold's publish-before-clear window lets a cumulative
/// read straddle it and double-count a NET-DECREF slot, flooring a still-live
/// rc to a spurious 0. Under `rc_authoritative_reclaim` the GC reclaim Gate-1
/// treats that 0 as proof to irreversibly free the PBA (Gate-2 reverify is
/// skipped) → reuse → read CRC. `get_consistent` holds `fold_lock` so it can
/// never observe the torn state.
///
/// The writer keeps the TRUE cumulative rc oscillating in {4, 8} — NEVER 0 —
/// while folding a net `-4` every iteration (the exact tear-prone shape). The
/// reader asserts `get_consistent` never dips below the true floor of 4.
/// Flip `get_consistent` → `get` and this fails (the plain read tears to 0).
#[test]
fn get_consistent_never_reads_spurious_zero_under_concurrent_fold() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

    let (_d, s) = make_shard();
    let s = Arc::new(s);
    let pba: Pba = 4242;

    // Durable base rc = 8 (folded). True cumulative rc stays in {4, 8}.
    s.stage(0, pba, 8, 1).unwrap();
    s.begin_checkpoint(0).unwrap();
    assert_eq!(s.get(pba).unwrap(), 8);

    let stop = Arc::new(AtomicBool::new(false));
    // Count how often the PLAIN (racy) read tears below the floor — proves
    // the window is actually exercised on this run (observational, not an
    // assertion, so it can't be flaky).
    let plain_torn = Arc::new(AtomicU32::new(0));

    let writer = {
        let s = s.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let mut lsn = 100u64;
            let mut bfg = 1u64;
            while !stop.load(Ordering::Relaxed) {
                // decref 4 then fold (array 8 -> 4): net-decref publish.
                s.stage(bfg, pba, -4, lsn).unwrap();
                lsn += 1;
                s.begin_checkpoint(bfg).unwrap();
                bfg += 1;
                // incref 4 back then fold (array 4 -> 8).
                s.stage(bfg, pba, 4, lsn).unwrap();
                lsn += 1;
                s.begin_checkpoint(bfg).unwrap();
                bfg += 1;
            }
        })
    };

    let mut min_consistent = u32::MAX;
    for _ in 0..200_000 {
        let rc = s.get_consistent(pba).unwrap();
        min_consistent = min_consistent.min(rc);
        assert!(
            rc >= 4,
            "get_consistent observed spurious rc={rc} (true rc always in {{4,8}})"
        );
        // Observe the plain read's tearing without gating the test on it.
        if s.get(pba).unwrap() < 4 {
            plain_torn.fetch_add(1, Ordering::Relaxed);
        }
    }

    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    assert!(
        min_consistent == 4 || min_consistent == 8,
        "expected to observe both arms; min_consistent={min_consistent}"
    );
    eprintln!(
        "plain (racy) get torn below floor {} / 200000 times",
        plain_torn.load(Ordering::Relaxed)
    );
}
