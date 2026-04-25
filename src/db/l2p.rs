use super::*;

impl Db {
    // -------- tree operations --------------------------------------------

    /// Point lookup in volume `vol_ord`'s L2P tree.
    pub fn get(&self, vol_ord: VolumeOrdinal, lba: Lba) -> Result<Option<L2pValue>> {
        let volume = self.volume(vol_ord)?;
        let sid = shard_for_key_l2p(&volume.shards, lba);
        let mut tree = volume.shards[sid].tree.lock();
        tree.get(lba)
    }

    /// Batched L2P lookup inside volume `vol_ord`. Groups `lbas` by shard,
    /// locks each shard once, and reads every lba that falls to it before
    /// moving on. Output order matches input order; duplicates produce
    /// repeated results.
    pub fn multi_get(&self, vol_ord: VolumeOrdinal, lbas: &[Lba]) -> Result<Vec<Option<L2pValue>>> {
        if lbas.is_empty() {
            return Ok(Vec::new());
        }
        let volume = self.volume(vol_ord)?;
        let shard_count = volume.shards.len();
        let mut buckets: Vec<Vec<usize>> = vec![Vec::new(); shard_count];
        for (idx, lba) in lbas.iter().enumerate() {
            buckets[shard_for_key_l2p(&volume.shards, *lba)].push(idx);
        }
        let mut out: Vec<Option<L2pValue>> = vec![None; lbas.len()];
        for (sid, idxs) in buckets.into_iter().enumerate() {
            if idxs.is_empty() {
                continue;
            }
            let mut tree = volume.shards[sid].tree.lock();
            for idx in idxs {
                out[idx] = tree.get(lbas[idx])?;
            }
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
        let volume = self.volume(vol_ord)?;
        let mut guards: Vec<MutexGuard<'_, PagedL2p>> =
            volume.shards.iter().map(|s| s.tree.lock()).collect();
        let mut items = Vec::new();
        for tree in &mut guards {
            items.extend(tree.range(range.clone())?.collect::<Result<Vec<_>>>()?);
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
        if start >= end {
            return Ok(self.last_applied_lsn());
        }
        let _drop_guard = self.drop_gate.write();
        let _apply_guard = self.apply_gate.write();

        let volume = self.volume(vol_ord)?;
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
        let captured: Vec<(Lba, L2pValue)> = {
            let mut acc: Vec<(Lba, L2pValue)> = Vec::new();
            for shard in &volume.shards {
                let mut tree = shard.tree.lock();
                let iter = tree.range(start..end)?;
                for item in iter {
                    let (lba, value) = item?;
                    acc.push((lba, value));
                }
            }
            acc.sort_unstable_by_key(|(lba, _)| *lba);
            acc
        };

        if captured.is_empty() {
            return Ok(self.last_applied_lsn());
        }

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
            let body = encode_body(std::slice::from_ref(&op));
            let lsn = self.wal.submit(body)?;
            self.faults.inject(FaultPoint::CommitPostWalBeforeApply)?;

            // Under apply_gate.write no one else can apply, so the
            // cvar wait is defensive and usually passes immediately.
            {
                let mut applied = self.last_applied_lsn.lock();
                while *applied + 1 < lsn {
                    self.commit_cvar.wait(&mut applied);
                }
            }

            let snap_lookup = |vol: VolumeOrdinal| -> Vec<SnapInfo> { self.snap_info_for_vol(vol) };
            let _outcome = apply_op_bare(
                &volumes_map,
                &self.refcount_shards,
                &self.dedup_index,
                &self.dedup_reverse,
                &self.page_store,
                lsn,
                &op,
                &snap_lookup,
            )?;
            self.faults
                .inject(FaultPoint::CommitPostApplyBeforeLsnBump)?;

            {
                let mut applied = self.last_applied_lsn.lock();
                *applied = lsn;
                self.commit_cvar.notify_all();
            }
            last_lsn = lsn;
        }
        Ok(last_lsn)
    }
}
