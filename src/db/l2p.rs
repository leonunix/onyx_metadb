use super::*;
use std::ops::Bound;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

const MULTI_GET_STACK_INDICES: usize = 64;
const RANGE_SHARD_ENUM_LEAF_CAP: u64 = 4096;

struct ActiveShardRead<'a> {
    shard: &'a L2pShard,
}

impl Drop for ActiveShardRead<'_> {
    fn drop(&mut self) {
        self.shard.active_readers.fetch_sub(1, Ordering::Release);
    }
}

fn acquire_l2p_read_view(shard: &L2pShard) -> (Arc<crate::paged::ReadView>, ActiveShardRead<'_>) {
    let guard = shard.read_view.read();
    shard.active_readers.fetch_add(1, Ordering::AcqRel);
    let view = guard.clone();
    drop(guard);
    (view, ActiveShardRead { shard })
}

fn range_start_inclusive(range: &OwnedRange) -> u64 {
    match range.start_bound() {
        Bound::Included(&v) => v,
        Bound::Excluded(&v) => v.saturating_add(1),
        Bound::Unbounded => 0,
    }
}

fn range_end_exclusive(range: &OwnedRange) -> Option<u64> {
    match range.end_bound() {
        Bound::Included(&v) => Some(v.saturating_add(1)),
        Bound::Excluded(&v) => Some(v),
        Bound::Unbounded => None,
    }
}

fn l2p_shards_for_range(shards: &[L2pShard], range: &OwnedRange) -> Vec<usize> {
    if shards.len() <= 1 {
        return (0..shards.len()).collect();
    }

    let Some(end_exclusive) = range_end_exclusive(range) else {
        return (0..shards.len()).collect();
    };
    let start = range_start_inclusive(range);
    if start >= end_exclusive {
        return Vec::new();
    }

    let first_leaf = start >> crate::paged::format::LEAF_SHIFT;
    let last_leaf = (end_exclusive - 1) >> crate::paged::format::LEAF_SHIFT;
    let leaf_count = last_leaf - first_leaf + 1;
    if leaf_count > RANGE_SHARD_ENUM_LEAF_CAP {
        return (0..shards.len()).collect();
    }

    let mut seen = vec![false; shards.len()];
    let mut out = Vec::with_capacity(shards.len().min(leaf_count as usize));
    for leaf_idx in first_leaf..=last_leaf {
        let lba = leaf_idx << crate::paged::format::LEAF_SHIFT;
        let sid = shard_for_key_l2p(shards, lba);
        if !seen[sid] {
            seen[sid] = true;
            out.push(sid);
        }
    }
    out.sort_unstable();
    out
}

impl Db {
    // -------- tree operations --------------------------------------------

    fn multi_get_ordered(
        &self,
        volume: &Volume,
        lbas: &[Lba],
        order: &[usize],
        out: &mut [Option<L2pValue>],
        pin_wait: Duration,
    ) -> Result<()> {
        let mut start = 0;
        while start < order.len() {
            let sid = shard_for_key_l2p(&volume.shards, lbas[order[start]]);
            let mut end = start + 1;
            while end < order.len() && shard_for_key_l2p(&volume.shards, lbas[order[end]]) == sid {
                end += 1;
            }

            let view_started = std::time::Instant::now();
            let (view, _active_read) = acquire_l2p_read_view(&volume.shards[sid]);
            self.metrics
                .record_l2p_multi_get_view(view_started.elapsed());
            let walk_started = std::time::Instant::now();
            view.multi_get_into(lbas, &order[start..end], out)?;
            let tree_walk = walk_started.elapsed();
            self.metrics.record_l2p_multi_get_tree(tree_walk);
            // `pin_wait` is recorded once per shard so the metric's
            // existing `lock_wait_us / calls` ratio still represents
            // per-shard barrier-acquire latency.
            self.metrics.record_l2p_get(pin_wait, tree_walk);
            start = end;
        }
        Ok(())
    }

    /// Point lookup in volume `vol_ord`'s L2P tree. Lock-free: pin the
    /// epoch barrier (one atomic load + one atomic store), clone the
    /// shard's published `ReadView`, walk it without touching the apply
    /// path. Flush no longer blocks reads — it physically frees pages
    /// only via the deferred-reclaim queue, which respects every
    /// active pin (see [`crate::epoch`]).
    pub fn get(&self, vol_ord: VolumeOrdinal, lba: Lba) -> Result<Option<L2pValue>> {
        let pin_started = std::time::Instant::now();
        let _pin = self.page_store.epoch().pin();
        let pin_wait = pin_started.elapsed();
        let volume = self.volume(vol_ord)?;
        let sid = shard_for_key_l2p(&volume.shards, lba);
        let (view, _active_read) = acquire_l2p_read_view(&volume.shards[sid]);
        let walk_started = std::time::Instant::now();
        let result = view.get(lba);
        let tree_walk = walk_started.elapsed();
        self.metrics.record_l2p_get(pin_wait, tree_walk);
        result
    }

    /// Batched L2P lookup inside volume `vol_ord`. Groups `lbas` by shard,
    /// loads each shard's published `ReadView` once, and walks every
    /// lba that falls to it. Output order matches input order;
    /// duplicates produce repeated results. Same epoch-pin model as
    /// [`get`](Self::get) — a single pin guards the whole batch.
    pub fn multi_get(&self, vol_ord: VolumeOrdinal, lbas: &[Lba]) -> Result<Vec<Option<L2pValue>>> {
        if lbas.is_empty() {
            return Ok(Vec::new());
        }
        let pin_started = std::time::Instant::now();
        let _pin = self.page_store.epoch().pin();
        let pin_wait = pin_started.elapsed();
        self.metrics.record_l2p_multi_get_call(lbas.len());
        self.metrics.record_l2p_multi_get_pin(pin_wait);
        let volume_started = std::time::Instant::now();
        let volume = self.volume(vol_ord)?;
        self.metrics
            .record_l2p_multi_get_volume(volume_started.elapsed());
        let mut out: Vec<Option<L2pValue>> = vec![None; lbas.len()];

        if lbas.len() <= MULTI_GET_STACK_INDICES {
            let mut order_buf = [0usize; MULTI_GET_STACK_INDICES];
            for (idx, slot) in order_buf.iter_mut().take(lbas.len()).enumerate() {
                *slot = idx;
            }
            let order = &mut order_buf[..lbas.len()];
            let sort_started = std::time::Instant::now();
            order.sort_unstable_by_key(|&idx| shard_for_key_l2p(&volume.shards, lbas[idx]));
            self.metrics
                .record_l2p_multi_get_sort(sort_started.elapsed());
            self.multi_get_ordered(&volume, lbas, order, &mut out, pin_wait)?;
        } else {
            let mut order: Vec<usize> = (0..lbas.len()).collect();
            let sort_started = std::time::Instant::now();
            order.sort_unstable_by_key(|&idx| shard_for_key_l2p(&volume.shards, lbas[idx]));
            self.metrics
                .record_l2p_multi_get_sort(sort_started.elapsed());
            self.multi_get_ordered(&volume, lbas, &order, &mut out, pin_wait)?;
        }
        Ok(out)
    }

    /// Insert `lba → value` in volume `vol_ord`, returning the previous
    /// value if any. Auto-commits as a one-op transaction.
    pub fn insert(
        &self,
        vol_ord: VolumeOrdinal,
        lba: Lba,
        value: L2pValue,
    ) -> Result<Option<L2pValue>> {
        let mut tx = self.begin();
        tx.insert(vol_ord, lba, value);
        let (_, outcomes) = tx.commit_with_outcomes()?;
        match outcomes.into_iter().next().unwrap() {
            ApplyOutcome::L2pPrev(prev) => Ok(prev),
            _ => unreachable!("insert produces L2pPrev"),
        }
    }

    /// Delete `lba` from volume `vol_ord`, returning the previous value
    /// if any. Auto-commits as a one-op transaction.
    pub fn delete(&self, vol_ord: VolumeOrdinal, lba: Lba) -> Result<Option<L2pValue>> {
        let mut tx = self.begin();
        tx.delete(vol_ord, lba);
        let (_, outcomes) = tx.commit_with_outcomes()?;
        match outcomes.into_iter().next().unwrap() {
            ApplyOutcome::L2pPrev(prev) => Ok(prev),
            _ => unreachable!("delete produces L2pPrev"),
        }
    }

    /// Range scan within volume `vol_ord`. Returns globally-key-ordered
    /// `(lba, value)` pairs by locking every shard of the volume and
    /// merging their individual range scans.
    pub fn range<R: RangeBounds<Lba>>(
        &self,
        vol_ord: VolumeOrdinal,
        range: R,
    ) -> Result<DbRangeIter> {
        let range = OwnedRange::new(range);
        let _pin = self.page_store.epoch().pin();
        let volume = self.volume(vol_ord)?;
        let mut items = Vec::new();
        for sid in l2p_shards_for_range(&volume.shards, &range) {
            let shard = &volume.shards[sid];
            let (view, _active_read) = acquire_l2p_read_view(shard);
            items.extend(view.range(range.clone())?.collect::<Result<Vec<_>>>()?);
        }
        items.sort_unstable_by_key(|(k, _)| *k);
        Ok(DbRangeIter::new(items))
    }

    /// Streaming variant of [`range`](Self::range). Currently an alias —
    /// the body delegates to `range`'s eager materialisation so every
    /// caller already gets a stable iterator surface, and a future commit
    /// can swap the body for a lazy frame-stack walker without touching
    /// call sites.
    pub fn range_stream<R: RangeBounds<Lba>>(
        &self,
        vol_ord: VolumeOrdinal,
        range: R,
    ) -> Result<DbRangeIter> {
        self.range(vol_ord, range)
    }

    /// Unordered, streaming scan over L2P mappings in `range`.
    ///
    /// This is intentionally not a replacement for [`range`](Self::range):
    /// callers that need globally sorted output should keep using `range`.
    /// Background maintenance paths such as GC only need to visit every live
    /// mapping once, so this avoids materialising and sorting millions of
    /// entries just to immediately iterate them.
    pub fn scan_range_unordered<R, F>(
        &self,
        vol_ord: VolumeOrdinal,
        range: R,
        mut f: F,
    ) -> Result<()>
    where
        R: RangeBounds<Lba>,
        F: FnMut(Lba, L2pValue) -> Result<()>,
    {
        let range = OwnedRange::new(range);
        let volume = self.volume(vol_ord)?;
        for sid in l2p_shards_for_range(&volume.shards, &range) {
            let shard = &volume.shards[sid];
            let _pin = self.page_store.epoch().pin();
            let (view, _active_read) = acquire_l2p_read_view(shard);
            view.for_each_range(range.clone(), |lba, value| f(lba, value))?;
        }
        Ok(())
    }

    /// Chunked variant of [`scan_range_unordered`](Self::scan_range_unordered)
    /// for background maintenance that may scan an entire volume. Each
    /// `(chunk, shard)` walk gets its own short epoch pin, so deferred page
    /// reclaim is not held hostage by a multi-second full-tree scan.
    ///
    /// This is best-effort maintenance visibility, not a transactionally
    /// consistent volume snapshot: concurrent writes may be observed in later
    /// chunks. Callers such as GC tolerate that and revalidate before rewrite.
    pub fn scan_range_unordered_chunked<F>(
        &self,
        vol_ord: VolumeOrdinal,
        start: Lba,
        end: Lba,
        chunk_lbas: u64,
        mut f: F,
    ) -> Result<()>
    where
        F: FnMut(Lba, L2pValue) -> Result<()>,
    {
        if start >= end {
            return Ok(());
        }
        let chunk_lbas = chunk_lbas.max(1);
        let volume = self.volume(vol_ord)?;
        let mut chunk_start = start;
        while chunk_start < end {
            let chunk_end = chunk_start.saturating_add(chunk_lbas).min(end);
            let chunk_range = OwnedRange::new(chunk_start..chunk_end);
            for sid in l2p_shards_for_range(&volume.shards, &chunk_range) {
                let shard = &volume.shards[sid];
                let _pin = self.page_store.epoch().pin();
                let (view, _active_read) = acquire_l2p_read_view(shard);
                view.for_each_range(chunk_start..chunk_end, |lba, value| f(lba, value))?;
            }
            chunk_start = chunk_end;
        }
        Ok(())
    }

    // -------- range delete (SPEC §3.2) ----------------------------------

    /// Bulk L2P delete over `[start, end)` for one volume. The
    /// plan-apply path mirrors [`drop_snapshot`](Self::drop_snapshot):
    /// take `drop_gate.write()` + `apply_gate.write()`, scan the range
    /// to build the `(lba, head_pba(value))` `captured` list, submit a
    /// `WalOp::L2pRangeDelete` (auto-split when the scan exceeds
    /// [`MAX_RANGE_DELETE_CAPTURED`]), and apply inline under the held
    /// apply gate. Each apply emits one decref per captured entry
    /// under SPEC §4.4 leaf-rc-suppress.
    ///
    /// Returns the LSN of the last submitted record. An empty range
    /// (`start >= end`) or a range with no live mappings returns
    /// [`last_applied_lsn`](Self::last_applied_lsn) without touching
    /// the WAL — replay has nothing to do, and callers get the current
    /// high-water LSN the same way [`commit`](Self::begin) does.
    ///
    /// Freed pba lists (for onyx's `SpaceAllocator` callback) are not
    /// exposed on this return; callers that need them can route a
    /// single-chunk range through a `Transaction::commit_with_outcomes`-
    /// style helper in a later session. S3 keeps the entry-point
    /// signature minimal; freed_pba observability is S6 / S3 follow-up.
    pub fn range_delete(&self, vol_ord: VolumeOrdinal, start: Lba, end: Lba) -> Result<Lsn> {
        let total_started = std::time::Instant::now();
        self.metrics.record_range_delete_call();
        if start >= end {
            self.metrics.record_range_delete_noop();
            self.metrics
                .record_range_delete_success(total_started.elapsed());
            return Ok(self.last_applied_lsn());
        }

        let drop_gate_started = std::time::Instant::now();
        let _drop_guard = self.drop_gate.write();
        self.metrics
            .record_range_delete_drop_gate_wait(drop_gate_started.elapsed());

        let apply_gate_started = std::time::Instant::now();
        let _apply_guard = self.apply_gate.write();
        self.metrics
            .record_range_delete_apply_gate_wait(apply_gate_started.elapsed());

        let volume = match self.volume(vol_ord) {
            Ok(volume) => volume,
            Err(err) => {
                self.metrics
                    .record_range_delete_error(total_started.elapsed());
                return Err(err);
            }
        };
        // Clone volume map up front — apply_op_bare needs it, and we
        // want to avoid holding `volumes.read()` across the WAL
        // submit + cvar wait pair (mirrors `commit_ops`).
        let volumes_map = self.volumes.read().clone();

        // Phase 1: scan each shard under its own mutex, collect
        // (lba, full_value) for every live mapping in the range. Full
        // value is needed so the apply-time snap-pin check can match
        // audit semantics (distinct (V, lba, value_28B) tuples). Locks
        // are released before WAL submit so the submit path can rotate
        // segments / fsync without the shard mutex held.
        let scan_started = std::time::Instant::now();
        let captured_result: Result<Vec<(Lba, L2pValue)>> = {
            let mut acc: Vec<(Lba, L2pValue)> = Vec::new();
            for shard in &volume.shards {
                let mut tree = shard.tree.write();
                let iter = tree.range(start..end)?;
                for item in iter {
                    let (lba, value) = item?;
                    acc.push((lba, value));
                }
            }
            acc.sort_unstable_by_key(|(lba, _)| *lba);
            Ok(acc)
        };
        let captured_len = captured_result.as_ref().map_or(0, Vec::len);
        self.metrics
            .record_range_delete_scan(scan_started.elapsed(), captured_len);
        let captured = match captured_result {
            Ok(captured) => captured,
            Err(err) => {
                self.metrics
                    .record_range_delete_error(total_started.elapsed());
                return Err(err);
            }
        };

        if captured.is_empty() {
            self.metrics.record_range_delete_noop();
            self.metrics
                .record_range_delete_success(total_started.elapsed());
            return Ok(self.last_applied_lsn());
        }
        self.metrics.record_range_delete_chunks(
            captured
                .chunks(crate::wal::op::MAX_RANGE_DELETE_CAPTURED)
                .len(),
        );

        // Phase 2: split into WAL records of at most
        // MAX_RANGE_DELETE_CAPTURED entries. Each chunk gets its own
        // WAL submit + apply, so a 100k-entry range becomes two
        // consecutive records and replay sees them as two separate
        // ops — both atomic on their own. Apply order is identical
        // to submit order under the held apply gate.
        let mut last_lsn = self.last_applied_lsn();
        for chunk in captured.chunks(crate::wal::op::MAX_RANGE_DELETE_CAPTURED) {
            let op = WalOp::L2pRangeDelete {
                vol_ord,
                start,
                end,
                captured: chunk.to_vec(),
            };
            let body = try_encode_body(std::slice::from_ref(&op))?;
            let wal_started = std::time::Instant::now();
            let lsn = match self.submit_wal_ops(std::slice::from_ref(&op), body, None) {
                Ok(lsn) => {
                    self.metrics.record_range_delete_wal(wal_started.elapsed());
                    lsn
                }
                Err(err) => {
                    self.metrics.record_range_delete_wal(wal_started.elapsed());
                    self.metrics
                        .record_range_delete_error(total_started.elapsed());
                    self.poison_commit_waiters(&err);
                    return Err(err);
                }
            };
            if let Err(err) = self.faults.inject(FaultPoint::CommitPostWalBeforeApply) {
                self.metrics
                    .record_range_delete_error(total_started.elapsed());
                self.poison_commit_waiters(&err);
                return Err(err);
            }

            // Under apply_gate.write no one else can apply, so the
            // cvar wait is defensive and usually passes immediately.
            let wait_started = std::time::Instant::now();
            self.wait_for_global_apply_turn(lsn)?;
            self.metrics
                .record_range_delete_apply_wait(wait_started.elapsed());

            let snap_lookup = |vol: VolumeOrdinal| -> Vec<SnapInfo> { self.snap_info_for_vol(vol) };
            let apply_started = std::time::Instant::now();
            let apply_result = apply_op_bare(
                &volumes_map,
                &self.refcount_shards,
                &self.dedup_index,
                &self.page_store,
                lsn,
                &op,
                &snap_lookup,
            );
            match apply_result {
                Ok(_outcome) => self
                    .metrics
                    .record_range_delete_apply(apply_started.elapsed()),
                Err(err) => {
                    self.metrics
                        .record_range_delete_apply(apply_started.elapsed());
                    self.metrics
                        .record_range_delete_error(total_started.elapsed());
                    self.poison_commit_waiters(&err);
                    return Err(err);
                }
            }
            if let Err(err) = self.faults.inject(FaultPoint::CommitPostApplyBeforeLsnBump) {
                self.metrics
                    .record_range_delete_error(total_started.elapsed());
                self.poison_commit_waiters(&err);
                return Err(err);
            }

            self.finish_global_apply(lsn)?;
            self.advance_dispatch_lsn(lsn);
            last_lsn = lsn;
        }
        self.metrics
            .record_range_delete_success(total_started.elapsed());
        Ok(last_lsn)
    }
}
