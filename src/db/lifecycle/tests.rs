use super::*;

#[test]
fn flush_reclaim_budget_scales_with_writes_and_backlog() {
    assert_eq!(flush_reclaim_budget(0, 0), FLUSH_RECLAIM_MIN_BUDGET_PAGES);
    assert_eq!(
        flush_reclaim_budget(0, FLUSH_RECLAIM_MIN_BUDGET_PAGES),
        FLUSH_RECLAIM_MIN_BUDGET_PAGES * 8
    );
    assert_eq!(
        flush_reclaim_budget(8 * 1_048_576, 1),
        FLUSH_RECLAIM_MAX_BUDGET_PAGES / 2
    );
    assert_eq!(
        flush_reclaim_budget(FLUSH_RECLAIM_BACKLOG_HARD_CAP_PAGES, 1),
        FLUSH_RECLAIM_MAX_BUDGET_PAGES
    );
}

#[test]
fn l2p_drain_worker_count_zero_preserves_legacy_fanout() {
    assert_eq!(l2p_drain_worker_count(0, 0), 0);
    assert_eq!(l2p_drain_worker_count(16, 0), 16);
    assert_eq!(l2p_drain_worker_count(16, 4), 4);
    assert_eq!(l2p_drain_worker_count(2, 4), 2);
}

/// Both fan-out paths under one set of assertions.
///
/// The persistent pool ([`crate::ckpt_pool`]) is an ARM, not a rewrite: it has
/// to execute each job exactly once, preserve input order, honour
/// `configured_workers` as a concurrency bound, and keep draining after a job
/// returns an error. Running the same assertions over both paths is what makes
/// `metadb-ckpt-pool off` a valid A/B baseline rather than a different
/// workload.
fn both_fanout_paths(mut check: impl FnMut(Option<&crate::ckpt_pool::CheckpointPool>, &str)) {
    check(None, "scoped");
    let pool = crate::ckpt_pool::CheckpointPool::new(4).expect("pool builds in tests");
    check(Some(&pool), "pooled");
}

#[test]
fn bounded_scoped_jobs_execute_each_job_once_and_preserve_order() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    both_fanout_paths(|pool, path| {
        let calls: Arc<Vec<AtomicUsize>> = Arc::new((0..32).map(|_| AtomicUsize::new(0)).collect());
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let results = run_scoped_jobs_bounded((0..32).collect(), 4, pool, {
            let calls = Arc::clone(&calls);
            let active = Arc::clone(&active);
            let max_active = Arc::clone(&max_active);
            move |job: usize| {
                calls[job].fetch_add(1, Ordering::Relaxed);
                let now = active.fetch_add(1, Ordering::AcqRel) + 1;
                max_active.fetch_max(now, Ordering::AcqRel);
                std::thread::sleep(std::time::Duration::from_millis(2));
                active.fetch_sub(1, Ordering::AcqRel);
                job
            }
        });

        assert_eq!(results, (0..32).collect::<Vec<_>>(), "{path}");
        assert!(
            calls.iter().all(|calls| calls.load(Ordering::Relaxed) == 1),
            "{path}: every job exactly once"
        );
        assert_eq!(active.load(Ordering::Acquire), 0, "{path}");
        assert!(
            max_active.load(Ordering::Acquire) <= 4,
            "{path}: configured_workers must still bound concurrency"
        );
        assert!(max_active.load(Ordering::Acquire) > 1, "{path}: ran in parallel");
    });
}

#[test]
fn bounded_scoped_jobs_collect_all_errors_before_ordered_return() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    both_fanout_paths(|pool, path| {
        let executed = Arc::new(AtomicUsize::new(0));
        let results: Vec<std::result::Result<usize, usize>> =
            run_scoped_jobs_bounded((0..8).collect(), 3, pool, {
                let executed = Arc::clone(&executed);
                move |job: usize| {
                    executed.fetch_add(1, Ordering::Relaxed);
                    if job == 2 || job == 6 { Err(job) } else { Ok(job) }
                }
            });

        assert_eq!(executed.load(Ordering::Relaxed), 8, "{path}: keeps draining after an error");
        assert_eq!(results[2], Err(2), "{path}");
        assert_eq!(results[6], Err(6), "{path}");
        let first_error = results.into_iter().find_map(std::result::Result::err);
        assert_eq!(first_error, Some(2), "{path}: first error by INPUT order");
    });
}

/// `configured_workers == 0` means "one worker per job", i.e. unbounded, which a
/// fixed-width pool cannot honour — so that configuration must keep using
/// scoped threads even when a pool is available.
#[test]
fn unbounded_worker_count_does_not_use_the_pool() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let pool = crate::ckpt_pool::CheckpointPool::new(2).expect("pool builds in tests");
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let results = run_scoped_jobs_bounded((0..8).collect(), 0, Some(&pool), {
        let active = Arc::clone(&active);
        let max_active = Arc::clone(&max_active);
        move |job: usize| {
            let now = active.fetch_add(1, Ordering::AcqRel) + 1;
            max_active.fetch_max(now, Ordering::AcqRel);
            std::thread::sleep(std::time::Duration::from_millis(20));
            active.fetch_sub(1, Ordering::AcqRel);
            job
        }
    });

    assert_eq!(results, (0..8).collect::<Vec<_>>());
    assert!(
        max_active.load(Ordering::Acquire) > 2,
        "a 2-thread pool would have capped this at 2; it must have used scoped threads"
    );
}
