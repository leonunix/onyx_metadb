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
