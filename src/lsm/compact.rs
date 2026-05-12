//! LSM compaction: merge SSTs between levels.
//!
//! Two flavors:
//! - **L0 → L1**: when L0 has ≥ `l0_sst_count_trigger` SSTs. All L0
//!   SSTs + overlapping L1 SSTs merge into new L1 SSTs.
//! - **Ln → Ln+1** (n ≥ 1): when `Ln` exceeds its size budget. Pick one
//!   `Ln` victim SST and every overlapping `Ln+1` SST; merge into the
//!   target level.
//!
//! In both cases the merge does a k-way hash-ordered scan and picks the
//! newest record for each hash. Tombstones propagate unless the target
//! level has no level deeper than it; then they drop.
//!
//! Size budgets (in records):
//! - `target_records(L1) = l0_sst_count_trigger * target_sst_records`
//! - `target_records(Ln) = target_records(L1) * level_ratio^(n-1)`
//! - Compaction of `Ln → Ln+1` fires when `sum(records in Ln) >
//!   target_records(Ln)`.
//!
//! # Concurrency
//!
//! Compaction holds the caller-provided modify lock (exclusive with
//! flush) while reading / writing SSTs, then swaps handles under
//! `levels.write()`, then forces a read-drain barrier before freeing
//! old pages. Readers that snapshotted the old handle set finish
//! reading the old SST pages before those pages are reused by future
//! allocations.

use std::iter::Peekable;

use crate::cache::PageCache;
use crate::error::{MetaDbError, Result};
use crate::page_store::PageStore;
use crate::types::Lsn;

use super::format::{Hash32, KIND_DELETE, Record};
use super::sst::{SstHandle, SstReader, SstScan, SstWriter};

/// Reported result of a successful compaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionReport {
    pub from_level: usize,
    pub to_level: usize,
    pub victims: usize,
    pub new_ssts: usize,
    pub records_in: u64,
    pub records_out: u64,
}

/// Plan of what a compaction is going to touch.
#[derive(Clone, Debug)]
pub(super) struct Plan {
    pub from_level: usize,
    pub to_level: usize,
    /// L0 source SSTs ordered newest-first (index 0 = newest) OR a
    /// single Ln source SST.
    pub from_victims: Vec<SstHandle>,
    /// Target-level SSTs that overlap the source range. Ordered by
    /// min_hash ascending.
    pub to_victims: Vec<SstHandle>,
    pub drop_tombstones: bool,
    pub target_records_per_sst: usize,
}

/// Compute the target record budget for a given level.
///
/// `n` is 0-indexed. `target_records(L0)` is meaningless (L0 is tiered)
/// and returns `0` — the L0 trigger is driven by sst count, not
/// record count.
pub(super) fn target_records_for_level(
    n: usize,
    l0_sst_count_trigger: usize,
    target_sst_records: usize,
    level_ratio: u32,
) -> u64 {
    if n == 0 {
        return 0;
    }
    let base = (l0_sst_count_trigger as u64).saturating_mul(target_sst_records as u64);
    let mut budget = base;
    for _ in 1..n {
        budget = budget.saturating_mul(level_ratio as u64);
    }
    budget
}

/// Plan an L0 → L1 compaction, or return `None` if L0 is under the
/// count trigger.
pub(super) fn plan_l0_to_l1(
    levels: &[Vec<SstHandle>],
    l0_sst_count_trigger: usize,
    target_records_per_sst: usize,
) -> Option<Plan> {
    let l0 = levels.first()?;
    if l0.len() < l0_sst_count_trigger {
        return None;
    }
    // Source = all L0 SSTs, newest first (last inserted = last in vec).
    let from_victims: Vec<SstHandle> = l0.iter().rev().copied().collect();
    if from_victims.is_empty() {
        return None;
    }

    // Compute the source hash range (min/max across all L0 victims).
    let (source_min, source_max) = hash_range(&from_victims);
    // If L0 has duplicate hashes across SSTs, newer SST's record wins.
    // Source ordering (newest first) handles that automatically.
    // Determine overlapping L1 SSTs.
    let to_victims: Vec<SstHandle> = levels
        .get(1)
        .map(|l1| {
            l1.iter()
                .filter(|h| !is_disjoint(h, &source_min, &source_max))
                .copied()
                .collect()
        })
        .unwrap_or_default();
    // Sort newer-first doesn't apply to L1; they're disjoint. But we
    // need to make sure L1 victims are in hash order so the merge input
    // stays monotonic within each source. They already come sorted by
    // min_hash in the level; keep that ordering.
    let _ = source_min;
    let _ = source_max;

    // Also sort from_victims by min_hash for merge convenience; the
    // newest-first ordering is re-imposed by their placement at the
    // start of the source list (indices 0..=l0_count-1). We'll encode
    // ordering explicitly in the merge by putting sources in newest-
    // first order. Keep the order as established.
    Some(Plan {
        from_level: 0,
        to_level: 1,
        from_victims,
        to_victims,
        drop_tombstones: levels.len() <= 2,
        target_records_per_sst,
    })
}

/// Plan a single Ln → Ln+1 compaction for n ≥ 1, or return `None` if
/// Ln is within its budget.
pub(super) fn plan_ln_to_next(
    levels: &[Vec<SstHandle>],
    from_level: usize,
    target_sst_records: usize,
    l0_sst_count_trigger: usize,
    level_ratio: u32,
) -> Option<Plan> {
    if from_level == 0 {
        return None;
    }
    let level = levels.get(from_level)?;
    if level.is_empty() {
        return None;
    }
    let total_records: u64 = level.iter().map(|h| h.record_count).sum();
    let budget = target_records_for_level(
        from_level,
        l0_sst_count_trigger,
        target_sst_records,
        level_ratio,
    );
    if total_records <= budget {
        return None;
    }
    // Pick the SST with the most records as victim (simple heuristic;
    // RocksDB uses round-robin for leveled, but for MVP this keeps
    // levels sorted in record density).
    let (victim_idx, _) = level
        .iter()
        .enumerate()
        .max_by_key(|(_, h)| h.record_count)?;
    let victim = level[victim_idx];
    let target_level = from_level + 1;
    let to_victims: Vec<SstHandle> = levels
        .get(target_level)
        .map(|next_level| {
            next_level
                .iter()
                .filter(|h| !is_disjoint(h, &victim.min_hash, &victim.max_hash))
                .copied()
                .collect()
        })
        .unwrap_or_default();
    Some(Plan {
        from_level,
        to_level: target_level,
        from_victims: vec![victim],
        to_victims,
        drop_tombstones: levels.len() <= target_level + 1,
        target_records_per_sst: target_sst_records,
    })
}

/// Apply a compaction plan to the in-memory levels vector. The plan's
/// `from_victims` are removed from `levels[from_level]`; `to_victims`
/// are removed from `levels[to_level]`; `new_handles` are inserted into
/// `levels[to_level]` in hash order.
pub(super) fn apply_plan(levels: &mut Vec<Vec<SstHandle>>, plan: &Plan, new_handles: &[SstHandle]) {
    // Ensure target level exists.
    while levels.len() <= plan.to_level {
        levels.push(Vec::new());
    }
    // Remove source victims from from_level by head_page id.
    levels[plan.from_level]
        .retain(|h| !plan.from_victims.iter().any(|v| v.head_page == h.head_page));
    // Remove target victims from to_level by head_page id.
    levels[plan.to_level].retain(|h| !plan.to_victims.iter().any(|v| v.head_page == h.head_page));
    // Insert new handles, then re-sort target level by min_hash
    // (invariant: L1+ SSTs disjoint and sorted).
    levels[plan.to_level].extend_from_slice(new_handles);
    if plan.to_level > 0 {
        levels[plan.to_level].sort_by(|a, b| a.min_hash.cmp(&b.min_hash));
    }
}

/// Execute a plan: open scan iterators over the victims, merge them,
/// and write new SSTs to disk. Returns the list of new handles.
pub(super) fn execute_plan<'a>(
    page_store: &'a PageStore,
    page_cache: &'a PageCache,
    generation: Lsn,
    bits_per_entry: u32,
    plan: &Plan,
) -> Result<ExecuteOutcome> {
    // Build sources: source indices are assigned newer-first.
    //
    // For L0 → L1 compaction:
    //   - plan.from_victims is ordered newest-first. Those go first.
    //   - plan.to_victims are older than L0; they come after.
    //
    // For Ln → Ln+1 compaction (n ≥ 1):
    //   - plan.from_victims is the single Ln SST (newer).
    //   - plan.to_victims are the Ln+1 SSTs (older).
    let ordered: Vec<SstHandle> = plan
        .from_victims
        .iter()
        .copied()
        .chain(plan.to_victims.iter().copied())
        .collect();

    let mut readers: Vec<SstReader<'a>> = ordered
        .iter()
        .map(|h| SstReader::open(page_store, page_cache, *h))
        .collect::<Result<Vec<_>>>()?;

    // Build peekable scan iterators for each reader. The scan must
    // outlive `readers`, so take `&reader` references.
    // NB: SstScan borrows from SstReader via its internal lifetime.
    // We move readers into a vec and build scans referencing them.
    //
    // Safe because `readers` outlives `sources`: both are owned by
    // this function and `sources` is dropped before the function
    // returns (before `readers`).
    //
    // Rust can't express that pattern without self-referential
    // structs, so we use the natural scope: readers first, then scans.
    let mut sources: Vec<Peekable<SstScan<'_>>> =
        readers.iter_mut().map(|r| r.scan().peekable()).collect();

    let mut records_in = 0u64;
    let mut records_out = 0u64;
    let mut pending: Vec<Record> = Vec::with_capacity(plan.target_records_per_sst);
    let mut new_handles: Vec<SstHandle> = Vec::new();

    loop {
        // Find the smallest hash across all peekable sources and the
        // newest (lowest index) record at that hash.
        let mut min_hash: Option<Hash32> = None;
        let mut owning_idx: Option<usize> = None;
        let mut errored = false;
        for (i, src) in sources.iter_mut().enumerate() {
            match src.peek() {
                None => {}
                Some(Err(_)) => {
                    errored = true;
                    owning_idx = Some(i);
                    break;
                }
                Some(Ok(r)) => {
                    let rh = *r.hash();
                    match min_hash {
                        None => {
                            min_hash = Some(rh);
                            owning_idx = Some(i);
                        }
                        Some(mh) if rh < mh => {
                            min_hash = Some(rh);
                            owning_idx = Some(i);
                        }
                        _ => {}
                    }
                }
            }
        }
        if errored {
            // Propagate the first error we can find.
            let idx = owning_idx.unwrap();
            return Err(match sources[idx].next() {
                Some(Err(e)) => e,
                _ => MetaDbError::Corruption("disappearing error in merge".into()),
            });
        }
        let Some(chosen_idx) = owning_idx else { break };
        let chosen_hash = min_hash.unwrap();

        // Pop the winner and advance every source at this hash.
        let winner = next_record(&mut sources[chosen_idx])?;
        records_in += 1;
        for (i, src) in sources.iter_mut().enumerate() {
            if i == chosen_idx {
                continue;
            }
            while matches!(src.peek(), Some(Ok(r)) if *r.hash() == chosen_hash) {
                next_record(src)?;
                records_in += 1;
            }
        }

        // Emit winner unless we can drop it.
        if plan.drop_tombstones && winner.kind() == KIND_DELETE {
            continue;
        }
        pending.push(winner);
        records_out += 1;
        if pending.len() >= plan.target_records_per_sst {
            let handle = SstWriter::new(page_store, generation)
                .with_bits_per_entry(bits_per_entry)
                .write_sorted(&pending)?;
            new_handles.push(handle);
            pending.clear();
        }
    }

    if !pending.is_empty() {
        let handle = SstWriter::new(page_store, generation)
            .with_bits_per_entry(bits_per_entry)
            .write_sorted(&pending)?;
        new_handles.push(handle);
    }

    Ok(ExecuteOutcome {
        new_handles,
        records_in,
        records_out,
    })
}

/// Outcome of [`execute_plan`]; the caller still has to swap handles
/// under `levels.write()` and free the victims.
pub(super) struct ExecuteOutcome {
    pub new_handles: Vec<SstHandle>,
    pub records_in: u64,
    pub records_out: u64,
}

/// Free every page of every victim SST. Used after the manifest-side
/// swap has been observed by `reader_drain`.
pub(super) fn free_victims(
    page_store: &PageStore,
    page_cache: &PageCache,
    generation: Lsn,
    victims: &[SstHandle],
) -> Result<()> {
    for handle in victims {
        page_cache.invalidate_run(handle.head_page, handle.page_count());
        page_store.free_run(handle.head_page, handle.page_count(), generation)?;
    }
    Ok(())
}

fn next_record(src: &mut Peekable<SstScan<'_>>) -> Result<Record> {
    match src.next() {
        Some(Ok(r)) => Ok(r),
        Some(Err(e)) => Err(e),
        None => Err(MetaDbError::Corruption("merge ran past end".into())),
    }
}

fn hash_range(handles: &[SstHandle]) -> (Hash32, Hash32) {
    let mut min = handles[0].min_hash;
    let mut max = handles[0].max_hash;
    for h in handles.iter().skip(1) {
        if h.min_hash < min {
            min = h.min_hash;
        }
        if h.max_hash > max {
            max = h.max_hash;
        }
    }
    (min, max)
}

fn is_disjoint(handle: &SstHandle, src_min: &Hash32, src_max: &Hash32) -> bool {
    handle.max_hash < *src_min || handle.min_hash > *src_max
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_handle(head_page: u64, min: u64, max: u64, records: u64) -> SstHandle {
        let mut min_hash = [0u8; 32];
        min_hash[..8].copy_from_slice(&min.to_be_bytes());
        let mut max_hash = [0u8; 32];
        max_hash[..8].copy_from_slice(&max.to_be_bytes());
        SstHandle {
            head_page,
            record_count: records,
            bloom_page_count: 1,
            body_page_count: 1,
            min_hash,
            max_hash,
        }
    }

    #[test]
    fn target_records_monotonic_across_levels() {
        let l1 = target_records_for_level(1, 4, 1000, 10);
        let l2 = target_records_for_level(2, 4, 1000, 10);
        let l3 = target_records_for_level(3, 4, 1000, 10);
        assert_eq!(l1, 4_000);
        assert_eq!(l2, 40_000);
        assert_eq!(l3, 400_000);
    }

    #[test]
    fn l0_plan_skipped_when_under_trigger() {
        let levels = vec![vec![mk_handle(10, 0, 99, 100)]];
        assert!(plan_l0_to_l1(&levels, 4, 1000).is_none());
    }

    #[test]
    fn l0_plan_fires_at_trigger() {
        let levels = vec![vec![
            mk_handle(10, 0, 99, 100),
            mk_handle(11, 50, 149, 100),
            mk_handle(12, 100, 199, 100),
            mk_handle(13, 150, 249, 100),
        ]];
        let plan = plan_l0_to_l1(&levels, 4, 1000).expect("plan");
        assert_eq!(plan.from_level, 0);
        assert_eq!(plan.to_level, 1);
        assert_eq!(plan.from_victims.len(), 4);
        assert!(plan.to_victims.is_empty());
        assert!(plan.drop_tombstones); // no L2 yet
    }

    #[test]
    fn l0_plan_includes_overlapping_l1() {
        let levels = vec![
            vec![
                mk_handle(10, 0, 100, 100),
                mk_handle(11, 200, 300, 100),
                mk_handle(12, 400, 500, 100),
                mk_handle(13, 600, 700, 100),
            ],
            vec![
                mk_handle(100, 50, 250, 500),  // overlaps
                mk_handle(101, 400, 550, 500), // overlaps
                mk_handle(102, 800, 900, 500), // disjoint
            ],
        ];
        let plan = plan_l0_to_l1(&levels, 4, 1000).unwrap();
        assert_eq!(plan.to_victims.len(), 2);
        assert_eq!(plan.to_victims[0].head_page, 100);
        assert_eq!(plan.to_victims[1].head_page, 101);
        // L1 is the deepest level (levels.len() == 2), so tombstones
        // can drop in the target level.
        assert!(plan.drop_tombstones);
    }

    #[test]
    fn ln_plan_picks_largest_when_over_budget() {
        let levels = vec![
            Vec::new(),
            vec![
                mk_handle(100, 0, 100, 1_000),
                mk_handle(101, 200, 300, 5_000), // largest
                mk_handle(102, 400, 500, 2_000),
            ],
            vec![
                mk_handle(200, 200, 280, 3_000), // overlaps with the largest
                mk_handle(201, 400, 500, 3_000),
            ],
        ];
        // budget = 4 * 1000 = 4000, total = 8000 > budget.
        let plan = plan_ln_to_next(&levels, 1, 1000, 4, 10).unwrap();
        assert_eq!(plan.from_level, 1);
        assert_eq!(plan.to_level, 2);
        assert_eq!(plan.from_victims.len(), 1);
        assert_eq!(plan.from_victims[0].head_page, 101);
        assert_eq!(plan.to_victims.len(), 1);
        assert_eq!(plan.to_victims[0].head_page, 200);
    }

    #[test]
    fn ln_plan_none_when_under_budget() {
        let levels = vec![
            Vec::new(),
            vec![mk_handle(100, 0, 100, 500), mk_handle(101, 200, 300, 500)],
        ];
        // budget = 4000, total = 1000 <= budget.
        assert!(plan_ln_to_next(&levels, 1, 1000, 4, 10).is_none());
    }

    #[test]
    fn apply_plan_swaps_victims_for_new_handles_and_sorts() {
        let mut levels = vec![
            vec![mk_handle(10, 0, 100, 100), mk_handle(11, 200, 300, 100)],
            vec![mk_handle(100, 50, 250, 300)],
        ];
        let plan = Plan {
            from_level: 0,
            to_level: 1,
            from_victims: vec![mk_handle(10, 0, 100, 100), mk_handle(11, 200, 300, 100)],
            to_victims: vec![mk_handle(100, 50, 250, 300)],
            drop_tombstones: true,
            target_records_per_sst: 1000,
        };
        let new = vec![mk_handle(999, 200, 300, 50), mk_handle(998, 0, 100, 50)];
        apply_plan(&mut levels, &plan, &new);
        assert!(levels[0].is_empty());
        assert_eq!(levels[1].len(), 2);
        // Sorted by min_hash.
        assert_eq!(levels[1][0].head_page, 998);
        assert_eq!(levels[1][1].head_page, 999);
    }
}
