use super::*;

#[derive(Clone, Copy, Debug)]
struct RcApplyAction {
    op_idx: usize,
    pba: Pba,
    delta: i64,
    standalone_refcount: bool,
    remap_freed_candidate: bool,
}

struct L2pBucketApplyResult {
    outcomes: Vec<(usize, ApplyOutcome)>,
    rc_actions: Vec<RcApplyAction>,
}

#[derive(Default)]
struct RcBucketApplyResult {
    refcount_outcomes: Vec<(usize, u32)>,
    remap_freed: Vec<(usize, Pba)>,
}

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
            self.metrics.record_commit_empty();
            return Ok((self.last_applied_lsn(), Vec::new()));
        }
        let commit_started = std::time::Instant::now();
        self.metrics.record_commit_attempt(ops.len());
        // `drop_gate.read()` pairs with lifecycle paths' write acquire.
        // Hold it across submit + apply so `drop_snapshot` /
        // `range_delete` cannot wedge themselves between our LSN
        // assignment and apply. Using the read side is important:
        // ordinary commits must still submit concurrently so the WAL
        // writer can coalesce them into group commits.
        let drop_gate_started = std::time::Instant::now();
        let _drop_guard = self.drop_gate.read();
        self.metrics
            .record_commit_drop_gate_wait(drop_gate_started.elapsed());
        let body = encode_body(ops);
        let wal_started = std::time::Instant::now();
        let lsn = match self.wal.submit(body) {
            Ok(lsn) => {
                self.metrics.record_commit_wal_submit(wal_started.elapsed());
                lsn
            }
            Err(err) => {
                self.metrics.record_commit_wal_submit(wal_started.elapsed());
                self.metrics.record_commit_error(commit_started.elapsed());
                return Err(err);
            }
        };
        if let Err(err) = self.faults.inject(FaultPoint::CommitPostWalBeforeApply) {
            self.metrics.record_commit_error(commit_started.elapsed());
            return Err(err);
        }

        // Wait until every lower LSN has applied. LSNs are unique and
        // assigned in submit order by the WAL writer, so exactly one
        // waiter is unblocked by each bump.
        let wait_started = std::time::Instant::now();
        {
            let mut applied = self.last_applied_lsn.lock();
            while *applied + 1 < lsn {
                self.commit_cvar.wait(&mut applied);
            }
        }
        self.metrics
            .record_commit_apply_wait(wait_started.elapsed());

        let apply_gate_started = std::time::Instant::now();
        let apply_guard = self.apply_gate.read();
        self.metrics
            .record_commit_apply_gate_wait(apply_gate_started.elapsed());
        // Clone out the volume map once per commit — HashMap + `Arc`
        // clones are cheap, and keeping the Arcs live for the whole
        // apply loop avoids holding `volumes.read()` across apply.
        // That matters because commit 8+ will acquire `volumes.write()`
        // on the lifecycle path, and a long-held reader would stall it.
        let volumes = self.volumes.read().clone();
        let apply_started = std::time::Instant::now();
        let outcomes = match self.apply_commit_batch(&volumes, lsn, ops) {
            Ok(outcomes) => {
                self.metrics.record_commit_apply(apply_started.elapsed());
                outcomes
            }
            Err(err) => {
                self.metrics.record_commit_apply(apply_started.elapsed());
                self.metrics.record_commit_error(commit_started.elapsed());
                return Err(err);
            }
        };
        if let Err(err) = self.faults.inject(FaultPoint::CommitPostApplyBeforeLsnBump) {
            self.metrics.record_commit_error(commit_started.elapsed());
            return Err(err);
        }

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
        self.metrics.record_commit_success(commit_started.elapsed());
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
    /// (DropSnapshot, CreateVolume, DropVolume, CloneVolume), a guarded
    /// remap, or a remap against a volume with live snapshots, we fall
    /// back to serial apply. Lifecycle ops do not currently reach
    /// `commit_ops` from any caller — they have their own entry points
    /// with stronger locking — but the fallback keeps the bucketed path
    /// safe if a future caller routes them through here.
    fn apply_commit_batch(
        &self,
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        lsn: Lsn,
        ops: &[WalOp],
    ) -> Result<Vec<ApplyOutcome>> {
        const BUCKET_THRESHOLD: usize = 8;
        if ops.len() < BUCKET_THRESHOLD || self.batch_requires_serial_apply(ops) {
            let mut outcomes = Vec::with_capacity(ops.len());
            for op in ops {
                outcomes.push(self.apply_op(volumes, lsn, op)?);
            }
            return Ok(outcomes);
        }
        self.apply_ops_grouped(volumes, lsn, ops)
    }

    fn batch_requires_serial_apply(&self, ops: &[WalOp]) -> bool {
        if batch_contains_lifecycle_op(ops) {
            return true;
        }

        let mut remap_vols = HashSet::new();
        for op in ops {
            if let WalOp::L2pRemap { vol_ord, guard, .. } = op {
                if guard.is_some() {
                    return true;
                }
                remap_vols.insert(*vol_ord);
            }
        }

        remap_vols
            .into_iter()
            .any(|vol_ord| !self.snap_info_for_vol(vol_ord).is_empty())
    }

    fn apply_l2p_bucket(
        volume: Arc<Volume>,
        sid: usize,
        indices: Vec<usize>,
        lsn: Lsn,
        ops: &[WalOp],
    ) -> Result<L2pBucketApplyResult> {
        let mut outcomes = Vec::with_capacity(indices.len());
        let mut rc_actions = Vec::new();
        let mut tree = volume.shards[sid].tree.write();
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
                WalOp::L2pRemap { lba, new_value, .. } => {
                    let prev = tree.insert_at_lsn(*lba, *new_value, lsn)?;
                    let value_changed = prev != Some(*new_value);
                    if value_changed {
                        let new_pba = new_value.head_pba();
                        match prev {
                            Some(old_value) => {
                                let old_pba = old_value.head_pba();
                                if old_pba != new_pba {
                                    rc_actions.push(RcApplyAction {
                                        op_idx: idx,
                                        pba: old_pba,
                                        delta: -1,
                                        standalone_refcount: false,
                                        remap_freed_candidate: true,
                                    });
                                    rc_actions.push(RcApplyAction {
                                        op_idx: idx,
                                        pba: new_pba,
                                        delta: 1,
                                        standalone_refcount: false,
                                        remap_freed_candidate: false,
                                    });
                                }
                            }
                            None => {
                                rc_actions.push(RcApplyAction {
                                    op_idx: idx,
                                    pba: new_pba,
                                    delta: 1,
                                    standalone_refcount: false,
                                    remap_freed_candidate: false,
                                });
                            }
                        }
                    }
                    ApplyOutcome::L2pRemap {
                        applied: true,
                        prev,
                        freed_pba: None,
                    }
                }
                other => unreachable!("L2P bucket holds only L2P ops; saw {other:?}"),
            };
            outcomes.push((idx, outcome));
        }
        Ok(L2pBucketApplyResult {
            outcomes,
            rc_actions,
        })
    }

    fn apply_l2p_buckets(
        &self,
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        lsn: Lsn,
        ops: &[WalOp],
        l2p_sorted: Vec<((VolumeOrdinal, usize), Vec<usize>)>,
    ) -> Result<Vec<L2pBucketApplyResult>> {
        const PARALLEL_BUCKET_THRESHOLD: usize = 64;
        if l2p_sorted.len() <= 1 || ops.len() < PARALLEL_BUCKET_THRESHOLD {
            let mut results = Vec::with_capacity(l2p_sorted.len());
            for ((vol_ord, sid), indices) in l2p_sorted {
                let volume = volumes
                    .get(&vol_ord)
                    .expect("volume presence checked during bucketing")
                    .clone();
                results.push(Self::apply_l2p_bucket(volume, sid, indices, lsn, ops)?);
            }
            return Ok(results);
        }

        std::thread::scope(|scope| -> Result<Vec<L2pBucketApplyResult>> {
            let mut handles = Vec::with_capacity(l2p_sorted.len());
            for ((vol_ord, sid), indices) in l2p_sorted {
                let volume = volumes
                    .get(&vol_ord)
                    .expect("volume presence checked during bucketing")
                    .clone();
                handles.push(
                    scope.spawn(move || Self::apply_l2p_bucket(volume, sid, indices, lsn, ops)),
                );
            }

            let mut results = Vec::with_capacity(handles.len());
            for handle in handles {
                let result = handle.join().map_err(|_| {
                    MetaDbError::Corruption("parallel L2P apply worker panicked".into())
                })??;
                results.push(result);
            }
            Ok(results)
        })
    }

    fn apply_refcount_bucket(
        &self,
        sid: usize,
        mut actions: Vec<RcApplyAction>,
        lsn: Lsn,
    ) -> Result<RcBucketApplyResult> {
        let mut result = RcBucketApplyResult::default();
        if actions.is_empty() {
            return Ok(result);
        }
        actions.sort_by_key(|action| action.op_idx);
        let mut tree = self.refcount_shards[sid].tree.lock();
        let mut by_pba: HashMap<Pba, Vec<RcApplyAction>> = HashMap::new();
        for action in actions {
            by_pba.entry(action.pba).or_default().push(action);
        }
        let mut by_pba: Vec<_> = by_pba.into_iter().collect();
        by_pba.sort_by_key(|(pba, _)| *pba);

        for (pba, group) in by_pba {
            let can_coalesce_remap = group.iter().all(|action| !action.standalone_refcount)
                && (group.iter().all(|action| action.delta > 0)
                    || group.iter().all(|action| action.delta < 0));

            if can_coalesce_remap {
                let delta: i64 = group.iter().map(|action| action.delta).sum();
                let pre = if group.iter().any(|action| action.remap_freed_candidate) {
                    tree.get(pba)?.map(|e| e.rc).unwrap_or(0)
                } else {
                    0
                };
                let new = refcount_apply_delta(&mut tree, pba, delta, lsn)?;
                if new == 0 && pre > 0 {
                    if let Some(action) = group
                        .iter()
                        .rev()
                        .find(|action| action.remap_freed_candidate)
                    {
                        result.remap_freed.push((action.op_idx, action.pba));
                    }
                }
                continue;
            }

            for action in group {
                let pre = if action.remap_freed_candidate {
                    tree.get(action.pba)?.map(|e| e.rc).unwrap_or(0)
                } else {
                    0
                };
                let new = refcount_apply_delta(&mut tree, action.pba, action.delta, lsn)?;
                if action.remap_freed_candidate {
                    if new == 0 && pre > 0 {
                        result.remap_freed.push((action.op_idx, action.pba));
                    }
                } else if action.standalone_refcount {
                    result.refcount_outcomes.push((action.op_idx, new));
                }
            }
        }
        Ok(result)
    }

    fn apply_refcount_buckets(
        &self,
        rc_buckets: Vec<Vec<RcApplyAction>>,
        lsn: Lsn,
        op_count: usize,
    ) -> Result<Vec<RcBucketApplyResult>> {
        const PARALLEL_BUCKET_THRESHOLD: usize = 64;
        let non_empty = rc_buckets
            .iter()
            .filter(|actions| !actions.is_empty())
            .count();
        if non_empty <= 1 || op_count < PARALLEL_BUCKET_THRESHOLD {
            let mut results = Vec::with_capacity(non_empty);
            for (sid, actions) in rc_buckets.into_iter().enumerate() {
                if actions.is_empty() {
                    continue;
                }
                results.push(self.apply_refcount_bucket(sid, actions, lsn)?);
            }
            return Ok(results);
        }

        std::thread::scope(|scope| -> Result<Vec<RcBucketApplyResult>> {
            let mut handles = Vec::with_capacity(non_empty);
            for (sid, actions) in rc_buckets.into_iter().enumerate() {
                if actions.is_empty() {
                    continue;
                }
                handles.push(scope.spawn(move || self.apply_refcount_bucket(sid, actions, lsn)));
            }

            let mut results = Vec::with_capacity(handles.len());
            for handle in handles {
                let result = handle.join().map_err(|_| {
                    MetaDbError::Corruption("parallel refcount apply worker panicked".into())
                })??;
                results.push(result);
            }
            Ok(results)
        })
    }

    /// Bucketed batch-apply. Only invoked for sufficiently large
    /// batches composed entirely of bucketable ops (plain L2P,
    /// unguarded/no-snapshot remap, refcount, dedup). See
    /// [`apply_commit_batch`] for the dispatch rule.
    fn apply_ops_grouped(
        &self,
        volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
        lsn: Lsn,
        ops: &[WalOp],
    ) -> Result<Vec<ApplyOutcome>> {
        let mut outcomes: Vec<Option<ApplyOutcome>> = (0..ops.len()).map(|_| None).collect();
        let mut l2p_buckets: HashMap<(VolumeOrdinal, usize), Vec<usize>> = HashMap::new();
        let mut rc_buckets: Vec<Vec<RcApplyAction>> = vec![Vec::new(); self.refcount_shards.len()];
        let mut dedup_idxs: Vec<usize> = Vec::new();

        for (idx, op) in ops.iter().enumerate() {
            match op {
                WalOp::L2pPut { vol_ord, lba, .. }
                | WalOp::L2pDelete { vol_ord, lba }
                | WalOp::L2pRemap { vol_ord, lba, .. } => {
                    let volume = volumes.get(vol_ord).ok_or_else(|| {
                        MetaDbError::Corruption(format!("L2P op for unknown volume ord {vol_ord}"))
                    })?;
                    let sid = shard_for_key_l2p(&volume.shards, *lba);
                    l2p_buckets.entry((*vol_ord, sid)).or_default().push(idx);
                }
                WalOp::Incref { pba, .. } | WalOp::Decref { pba, .. } => {
                    let sid = shard_for_key(&self.refcount_shards, *pba);
                    let delta = match op {
                        WalOp::Incref { delta, .. } => i64::from(*delta),
                        WalOp::Decref { delta, .. } => -i64::from(*delta),
                        _ => unreachable!(),
                    };
                    rc_buckets[sid].push(RcApplyAction {
                        op_idx: idx,
                        pba: *pba,
                        delta,
                        standalone_refcount: true,
                        remap_freed_candidate: false,
                    });
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
                | WalOp::L2pRangeDelete { .. } => {
                    // Already filtered out by `batch_contains_lifecycle_op`.
                    unreachable!("lifecycle ops must not reach apply_ops_grouped");
                }
            }
        }

        // Apply L2P buckets in deterministic bucket order for small
        // commits, and in parallel for large commits. Buckets target
        // disjoint per-volume shards; any refcount side effects are
        // collected and applied after all L2P buckets finish.
        let mut l2p_sorted: Vec<_> = l2p_buckets.into_iter().collect();
        l2p_sorted.sort_by_key(|((vol, sid), _)| (*vol, *sid));
        for result in self.apply_l2p_buckets(volumes, lsn, ops, l2p_sorted)? {
            for (idx, outcome) in result.outcomes {
                outcomes[idx] = Some(outcome);
            }
            for action in result.rc_actions {
                let sid = shard_for_key(&self.refcount_shards, action.pba);
                rc_buckets[sid].push(action);
            }
        }

        // Apply refcount buckets. Each bucket owns one refcount shard,
        // so large remap batches can parallelise this phase too.
        for result in self.apply_refcount_buckets(rc_buckets, lsn, ops.len())? {
            for (idx, new) in result.refcount_outcomes {
                outcomes[idx] = Some(ApplyOutcome::RefcountNew(new));
            }
            for (idx, pba) in result.remap_freed {
                match outcomes[idx].as_mut() {
                    Some(ApplyOutcome::L2pRemap { freed_pba, .. }) => {
                        *freed_pba = Some(pba);
                    }
                    other => {
                        unreachable!("remap rc action missing L2pRemap outcome: {other:?}")
                    }
                }
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
