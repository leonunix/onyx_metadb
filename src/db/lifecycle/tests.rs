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

#[test]
fn bounded_scoped_jobs_execute_each_job_once_and_preserve_order() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let calls: Arc<Vec<AtomicUsize>> = Arc::new((0..32).map(|_| AtomicUsize::new(0)).collect());
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let results = run_scoped_jobs_bounded((0..32).collect(), 4, {
        let calls = Arc::clone(&calls);
        let active = Arc::clone(&active);
        let max_active = Arc::clone(&max_active);
        move |job| {
            calls[job].fetch_add(1, Ordering::Relaxed);
            let now = active.fetch_add(1, Ordering::AcqRel) + 1;
            max_active.fetch_max(now, Ordering::AcqRel);
            std::thread::sleep(std::time::Duration::from_millis(2));
            active.fetch_sub(1, Ordering::AcqRel);
            job
        }
    });

    assert_eq!(results, (0..32).collect::<Vec<_>>());
    assert!(calls.iter().all(|calls| calls.load(Ordering::Relaxed) == 1));
    assert_eq!(active.load(Ordering::Acquire), 0);
    assert!(max_active.load(Ordering::Acquire) <= 4);
    assert!(max_active.load(Ordering::Acquire) > 1);
}

#[test]
fn bounded_scoped_jobs_collect_all_errors_before_ordered_return() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let executed = Arc::new(AtomicUsize::new(0));
    let results: Vec<std::result::Result<usize, usize>> =
        run_scoped_jobs_bounded((0..8).collect(), 3, {
            let executed = Arc::clone(&executed);
            move |job| {
                executed.fetch_add(1, Ordering::Relaxed);
                if job == 2 || job == 6 {
                    Err(job)
                } else {
                    Ok(job)
                }
            }
        });

    assert_eq!(executed.load(Ordering::Relaxed), 8);
    assert_eq!(results[2], Err(2));
    assert_eq!(results[6], Err(6));
    let first_error = results.into_iter().find_map(std::result::Result::err);
    assert_eq!(first_error, Some(2));
}
