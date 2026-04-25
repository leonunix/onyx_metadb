use super::*;

impl Db {
    // -------- transaction / commit --------------------------------------

    /// Start a new transaction that buffers ops until `commit()`.
    pub fn begin(&self) -> Transaction<'_> {
        Transaction::new(self)
    }

    /// LSN of the most recent successful commit.
    pub fn last_applied_lsn(&self) -> Lsn {
        *self.last_applied_lsn.lock()
    }

    /// Internal: submit a set of ops to the WAL, apply them to indexes,
    /// and return the assigned LSN plus any per-op outcomes.
    ///
    /// Concurrency: WAL submission runs **outside** any Db-level lock so
    /// multiple submitters coalesce into one group-commit batch at the
    /// WAL writer. Apply order is re-serialised after submit via the
    /// `last_applied_lsn` + `commit_cvar` queue — each commit waits
    /// until `*last_applied_lsn + 1 == lsn`, applies under
    /// `apply_gate.read()`, then bumps `last_applied_lsn` **before**
    /// dropping the gate so flush / snapshot never observe trees whose
    /// state is ahead of `last_applied_lsn`.
    pub(crate) fn commit_ops(&self, ops: &[WalOp]) -> Result<(Lsn, Vec<ApplyOutcome>)> {
        if ops.is_empty() {
            return Ok((self.last_applied_lsn(), Vec::new()));
        }
        // `drop_gate.read()` pairs with lifecycle paths' write acquire.
        // Hold it across submit + apply so `drop_snapshot` /
        // `range_delete` cannot wedge themselves between our LSN
        // assignment and apply. Using the read side is important:
        // ordinary commits must still submit concurrently so the WAL
        // writer can coalesce them into group commits.
        let _drop_guard = self.drop_gate.read();
        let body = encode_body(ops);
        let lsn = self.wal.submit(body)?;
        self.faults.inject(FaultPoint::CommitPostWalBeforeApply)?;

        // Wait until every lower LSN has applied. LSNs are unique and
        // assigned in submit order by the WAL writer, so exactly one
        // waiter is unblocked by each bump.
        {
            let mut applied = self.last_applied_lsn.lock();
            while *applied + 1 < lsn {
                self.commit_cvar.wait(&mut applied);
            }
        }

        let apply_guard = self.apply_gate.read();
        // Clone out the volume map once per commit — HashMap + `Arc`
        // clones are cheap, and keeping the Arcs live for the whole
        // apply loop avoids holding `volumes.read()` across apply.
        // That matters because commit 8+ will acquire `volumes.write()`
        // on the lifecycle path, and a long-held reader would stall it.
        let volumes = self.volumes.read().clone();
        let outcomes = self.apply_commit_batch(&volumes, lsn, ops)?;
        self.faults
            .inject(FaultPoint::CommitPostApplyBeforeLsnBump)?;

        // Bump BEFORE dropping the gate: if we released the gate first
        // a concurrent flush could observe `last_applied_lsn = lsn - 1`
        // while trees already contain op `lsn`, causing recovery to
        // double-apply on restart (refcount incref is not idempotent).
        {
            let mut applied = self.last_applied_lsn.lock();
            *applied = lsn;
            self.commit_cvar.notify_all();
        }
        drop(apply_guard);
        Ok((lsn, outcomes))
    }

    fn apply_op(
        &self,
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        lsn: Lsn,
        op: &WalOp,
    ) -> Result<ApplyOutcome> {
        let snap_lookup = |vol: VolumeOrdinal| -> Vec<SnapInfo> { self.snap_info_for_vol(vol) };
        let outcome = apply_op_bare(
            volumes,
            &self.refcount_shards,
            &self.dedup_index,
            &self.dedup_reverse,
            &self.page_store,
            lsn,
            op,
            &snap_lookup,
        )?;
        // DropSnapshot also mutates the in-memory manifest's snapshot
        // list; the page work lives in apply_op_bare so it can be
        // shared with the replay path. Lock order (apply_gate.read →
        // manifest_state) matches every other call site.
        if let WalOp::DropSnapshot { id, .. } = op {
            let dropped_vol = {
                let mut mstate = self.manifest_state.lock();
                let dropped = mstate
                    .manifest
                    .snapshots
                    .iter()
                    .find(|s| s.id == *id)
                    .map(|s| s.vol_ord);
                mstate.manifest.snapshots.retain(|s| s.id != *id);
                dropped
            };
            if let Some(vol) = dropped_vol {
                self.recompute_snap_info(vol);
            }
        }
        Ok(outcome)
    }

    /// Apply a commit batch under `apply_gate.read()`. Large batches
    /// group ops by shard so each shard lock is taken once per (vol,
    /// shard) rather than once per op. Small batches fall through to
    /// the serial path — the bucketing overhead dominates below a
    /// handful of ops.
    ///
    /// Correctness:
    /// - Intra-bucket order is preserved (`Vec<usize>` of original op
    ///   indices), so multiple ops to the same (vol, lba) or same pba
    ///   apply in caller order.
    /// - Cross-bucket order is relaxed: L2P shards and refcount shards
    ///   live on disjoint trees, and same-LSN `cow_for_write` is
    ///   idempotent via `page.generation >= lsn`, so reordering between
    ///   two shards does not change the committed state.
    /// - Each shard lock is held for the span of its bucket only;
    ///   locks are taken one at a time in (vol_ord, shard_id) sorted
    ///   order, so the CLAUDE-documented "cross-shard ops take locks
    ///   in shard index order" invariant still holds.
    ///
    /// Defensive fallback: if the batch contains a lifecycle op
    /// (DropSnapshot, CreateVolume, DropVolume, CloneVolume), we fall
    /// back to serial apply. Those ops do not currently reach
    /// `commit_ops` from any caller — they have their own entry points
    /// with stronger locking — but the fallback keeps the bucketed
    /// path safe if a future caller routes them through here.
    fn apply_commit_batch(
        &self,
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        lsn: Lsn,
        ops: &[WalOp],
    ) -> Result<Vec<ApplyOutcome>> {
        const BUCKET_THRESHOLD: usize = 8;
        if ops.len() < BUCKET_THRESHOLD || batch_contains_lifecycle_op(ops) {
            let mut outcomes = Vec::with_capacity(ops.len());
            for op in ops {
                outcomes.push(self.apply_op(volumes, lsn, op)?);
            }
            return Ok(outcomes);
        }
        self.apply_ops_grouped(volumes, lsn, ops)
    }

    /// Bucketed batch-apply. Only invoked for sufficiently large
    /// batches composed entirely of bucketable ops (L2P / refcount /
    /// dedup). See [`apply_commit_batch`] for the dispatch rule.
    fn apply_ops_grouped(
        &self,
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        lsn: Lsn,
        ops: &[WalOp],
    ) -> Result<Vec<ApplyOutcome>> {
        let mut outcomes: Vec<Option<ApplyOutcome>> = (0..ops.len()).map(|_| None).collect();
        let mut l2p_buckets: HashMap<(VolumeOrdinal, usize), Vec<usize>> = HashMap::new();
        let mut rc_buckets: Vec<Vec<usize>> = vec![Vec::new(); self.refcount_shards.len()];
        let mut dedup_idxs: Vec<usize> = Vec::new();

        for (idx, op) in ops.iter().enumerate() {
            match op {
                WalOp::L2pPut { vol_ord, lba, .. } | WalOp::L2pDelete { vol_ord, lba } => {
                    let volume = volumes.get(vol_ord).ok_or_else(|| {
                        MetaDbError::Corruption(format!("L2P op for unknown volume ord {vol_ord}"))
                    })?;
                    let sid = shard_for_key_l2p(&volume.shards, *lba);
                    l2p_buckets.entry((*vol_ord, sid)).or_default().push(idx);
                }
                WalOp::Incref { pba, .. } | WalOp::Decref { pba, .. } => {
                    let sid = shard_for_key(&self.refcount_shards, *pba);
                    rc_buckets[sid].push(idx);
                }
                WalOp::DedupPut { .. }
                | WalOp::DedupDelete { .. }
                | WalOp::DedupReversePut { .. }
                | WalOp::DedupReverseDelete { .. } => {
                    dedup_idxs.push(idx);
                }
                WalOp::DropSnapshot { .. }
                | WalOp::CreateVolume { .. }
                | WalOp::DropVolume { .. }
                | WalOp::CloneVolume { .. }
                | WalOp::L2pRemap { .. }
                | WalOp::L2pRangeDelete { .. } => {
                    // Already filtered out by `batch_contains_lifecycle_op`.
                    unreachable!("lifecycle ops must not reach apply_ops_grouped");
                }
            }
        }

        // Apply L2P buckets in a deterministic (vol_ord, shard_id)
        // order. One shard lock held at a time; no cross-shard
        // simultaneous locking, so no deadlock risk.
        let mut l2p_sorted: Vec<_> = l2p_buckets.into_iter().collect();
        l2p_sorted.sort_by_key(|((vol, sid), _)| (*vol, *sid));
        for ((vol_ord, sid), indices) in l2p_sorted {
            let volume = volumes
                .get(&vol_ord)
                .expect("volume presence checked during bucketing");
            let mut tree = volume.shards[sid].tree.lock();
            for idx in indices {
                let outcome = match &ops[idx] {
                    WalOp::L2pPut { lba, value, .. } => {
                        let prev = tree.insert_at_lsn(*lba, *value, lsn)?;
                        ApplyOutcome::L2pPrev(prev)
                    }
                    WalOp::L2pDelete { lba, .. } => {
                        let prev = tree.delete_at_lsn(*lba, lsn)?;
                        ApplyOutcome::L2pPrev(prev)
                    }
                    other => unreachable!("L2P bucket holds only L2P ops; saw {other:?}"),
                };
                outcomes[idx] = Some(outcome);
            }
        }

        // Apply refcount buckets in shard_id order.
        for (sid, indices) in rc_buckets.into_iter().enumerate() {
            if indices.is_empty() {
                continue;
            }
            let mut tree = self.refcount_shards[sid].tree.lock();
            for idx in indices {
                let outcome = match &ops[idx] {
                    WalOp::Incref { pba, delta } => {
                        let new = refcount_apply_delta(&mut tree, *pba, i64::from(*delta), lsn)?;
                        ApplyOutcome::RefcountNew(new)
                    }
                    WalOp::Decref { pba, delta } => {
                        let new = refcount_apply_delta(&mut tree, *pba, -i64::from(*delta), lsn)?;
                        ApplyOutcome::RefcountNew(new)
                    }
                    other => {
                        unreachable!("refcount bucket holds only rc ops; saw {other:?}")
                    }
                };
                outcomes[idx] = Some(outcome);
            }
        }

        // Dedup ops route through the LSM's own synchronisation; no
        // shard lock needed. Apply in original order — LSM puts on the
        // same key are last-write-wins, matching the serial path.
        for idx in dedup_idxs {
            let outcome = match &ops[idx] {
                WalOp::DedupPut { hash, value } => {
                    self.dedup_index.put(*hash, *value);
                    ApplyOutcome::Dedup
                }
                WalOp::DedupDelete { hash } => {
                    self.dedup_index.delete(*hash);
                    ApplyOutcome::Dedup
                }
                WalOp::DedupReversePut { pba, hash } => {
                    let (key, value) = encode_reverse_entry(*pba, hash);
                    self.dedup_reverse.put(key, value);
                    ApplyOutcome::Dedup
                }
                WalOp::DedupReverseDelete { pba, hash } => {
                    let (key, _) = encode_reverse_entry(*pba, hash);
                    self.dedup_reverse.delete(key);
                    ApplyOutcome::Dedup
                }
                other => unreachable!("dedup bucket holds only dedup ops; saw {other:?}"),
            };
            outcomes[idx] = Some(outcome);
        }

        Ok(outcomes
            .into_iter()
            .map(|o| o.expect("every op index filled by exactly one bucket"))
            .collect())
    }
}
