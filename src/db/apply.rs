use super::*;

/// Must be invoked **before** the caller drops the tree write guard:
/// the guard's lifetime is what makes "tree.root() at publish time" a
/// well-defined moment that includes every just-mutated dirty page.
pub(super) fn publish_l2p_read_view(shard: &L2pShard, tree: &PagedL2p) {
    let view = Arc::new(tree.snapshot_read_view());
    *shard.read_view.write() = view;
}

/// Apply one [`WalOp`] to raw `Db` state. Used by both the live commit
/// path (through `self.apply_op`) and the WAL-replay path (before
/// `Self` exists). Takes individual references so it can run against
/// locally-constructed state during `open`. Private to this module
/// because `Shard` is.
///
/// `lsn` is the WAL record LSN the op is applying at; used only by
/// `DropSnapshot` to stamp page generations for idempotent replay.
/// Callers that don't have an exact LSN (e.g. `replay_into`, which
/// passes the enclosing record's LSN) just pass the best available
/// value — the only correctness requirement is that `lsn` strictly
/// increases across apply invocations, which is already guaranteed by
/// WAL LSN monotonicity.
///
/// `DropSnapshot` mutates the in-memory manifest; callers that need
/// that side effect must handle the `manifest.snapshots.retain(...)`
/// themselves after calling this function. This split keeps
/// `apply_op_bare` usable in the replay path (which owns a bare
/// `Manifest`) and the live path (which owns a `Mutex<ManifestState>`).
///
/// `snap_info_for_vol` is a per-volume callback returning the live
/// snapshot view info for that volume (empty `Vec` for no-snap case).
/// [`apply_l2p_remap`] / [`apply_l2p_range_delete`] consult it to gate
/// decref decisions:
///
/// 1. Fast filter: if `old_pba.birth_lsn > min(snap.created_lsn)`,
///    no live snap can pin this content → decref.
/// 2. Otherwise read each snap's L2P at `(V, lba)`; suppress decref
///    iff any snap has that lba mapping to `old_pba`.
///
/// Replaces the legacy `leaf_was_shared` proxy. Callers that don't
/// have a snap-aware state (unit tests) can pass `&|_| Vec::new()`.

pub(super) fn apply_op_bare(
    volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
    refcount_shards: &[Shard],
    dedup_index: &crate::dedup::DedupIndex,
    page_store: &Arc<PageStore>,
    lsn: Lsn,
    op: &WalOp,
    snap_info_for_vol: &dyn Fn(VolumeOrdinal) -> Vec<SnapInfo>,
) -> Result<ApplyOutcome> {
    match op {
        WalOp::L2pPut {
            vol_ord,
            lba,
            value,
        } => {
            let volume = volumes.get(vol_ord).ok_or_else(|| {
                MetaDbError::Corruption(format!("L2pPut for unknown volume ord {vol_ord}"))
            })?;
            let sid = shard_for_key_l2p(&volume.shards, *lba);
            let use_buffer = volume.shards[sid].use_buffer;
            let mut tree = volume.shards[sid].tree.write();
            // L2pPut returns the rejecting `cur` as `L2pPrev(Some(cur))`;
            // caller distinguishes accept vs reject by comparing
            // `value.seq()` against `cur.seq()`.
            let cur = if use_buffer {
                match volume.shards[sid].l2p_buffer.lookup(*lba) {
                    crate::db::l2p_buffer::BufferLookup::Present(v) => Some(v),
                    crate::db::l2p_buffer::BufferLookup::Tombstone => None,
                    crate::db::l2p_buffer::BufferLookup::Absent => tree.get(*lba)?,
                }
            } else {
                tree.get(*lba)?
            };
            if seq_guard_rejects(value.seq(), cur.as_ref()) {
                return Ok(ApplyOutcome::L2pPrev(cur));
            }
            let prev = if use_buffer {
                volume.shards[sid].l2p_buffer.insert(*lba, *value, lsn);
                cur
            } else {
                let p = tree.insert_at_lsn(*lba, *value, lsn)?;
                publish_l2p_read_view(&volume.shards[sid], &tree);
                p
            };
            Ok(ApplyOutcome::L2pPrev(prev))
        }
        WalOp::L2pDelete { vol_ord, lba } => {
            let volume = volumes.get(vol_ord).ok_or_else(|| {
                MetaDbError::Corruption(format!("L2pDelete for unknown volume ord {vol_ord}"))
            })?;
            let sid = shard_for_key_l2p(&volume.shards, *lba);
            let use_buffer = volume.shards[sid].use_buffer;
            let mut tree = volume.shards[sid].tree.write();
            let prev = if use_buffer {
                let cur = match volume.shards[sid].l2p_buffer.lookup(*lba) {
                    crate::db::l2p_buffer::BufferLookup::Present(v) => Some(v),
                    crate::db::l2p_buffer::BufferLookup::Tombstone => None,
                    crate::db::l2p_buffer::BufferLookup::Absent => tree.get(*lba)?,
                };
                volume.shards[sid].l2p_buffer.insert_tombstone(*lba, lsn);
                cur
            } else {
                let p = tree.delete_at_lsn(*lba, lsn)?;
                publish_l2p_read_view(&volume.shards[sid], &tree);
                p
            };
            Ok(ApplyOutcome::L2pPrev(prev))
        }
        WalOp::DedupPut { hash, value } => {
            dedup_index.put(*hash, *value, lsn)?;
            Ok(ApplyOutcome::Dedup)
        }
        WalOp::DedupPutGuarded {
            hash,
            value,
            pba_guard,
            min_rc,
        } => {
            let sid = shard_for_key(refcount_shards, *pba_guard);
            if refcount_shards[sid].rc.get(*pba_guard)? >= *min_rc {
                dedup_index.put(*hash, *value, lsn)?;
            }
            Ok(ApplyOutcome::Dedup)
        }
        WalOp::DedupDelete { hash } => {
            dedup_index.delete(hash, lsn)?;
            Ok(ApplyOutcome::Dedup)
        }
        WalOp::DedupCompareDelete { hash, old_value } => {
            let applied = dedup_index.get(hash)?.as_ref() == Some(old_value);
            if applied {
                dedup_index.delete(hash, lsn)?;
            }
            Ok(ApplyOutcome::DedupCompare { applied })
        }
        WalOp::DedupComparePut {
            hash,
            old_value,
            new_value,
        } => {
            let applied = dedup_index.get(hash)?.as_ref() == Some(old_value);
            if applied {
                dedup_index.put(*hash, *new_value, lsn)?;
            }
            Ok(ApplyOutcome::DedupCompare { applied })
        }
        WalOp::Incref { pba, delta } => {
            let sid = shard_for_key(refcount_shards, *pba);
            let (_, new) = refcount_shards[sid]
                .rc
                .stage(*pba, i64::from(*delta), lsn)?;
            Ok(ApplyOutcome::RefcountNew(new))
        }
        WalOp::Decref { pba, delta } => {
            let sid = shard_for_key(refcount_shards, *pba);
            let (_, new) = refcount_shards[sid]
                .rc
                .stage(*pba, -i64::from(*delta), lsn)?;
            Ok(ApplyOutcome::RefcountNew(new))
        }
        WalOp::L2pRemap {
            vol_ord,
            lba,
            new_value,
            guard,
        } => apply_l2p_remap(
            volumes,
            refcount_shards,
            lsn,
            *vol_ord,
            *lba,
            *new_value,
            *guard,
            &snap_info_for_vol(*vol_ord),
        ),
        WalOp::L2pRangeDelete {
            vol_ord,
            start: _,
            end: _,
            captured,
        } => apply_l2p_range_delete(
            volumes,
            refcount_shards,
            lsn,
            *vol_ord,
            captured,
            &snap_info_for_vol(*vol_ord),
        ),
        WalOp::DropSnapshot {
            id: _,
            pages,
            pba_decrefs,
        } => apply_drop_snapshot_pages_and_decrefs(
            page_store,
            refcount_shards,
            lsn,
            pages,
            pba_decrefs,
        ),
        // Phase 7 per-volume lifecycle ops: decodable since Phase A, but
        // their apply semantics land with commit 8/9. Commit 6 still
        // expects to see `vol_ord = 0` on L2P ops only; any of these
        // three tags in the WAL means either a mixed-binary recovery
        // attempt or a logic bug in the caller.
        WalOp::CreateVolume { ord, .. }
        | WalOp::DropVolume { ord, .. }
        | WalOp::CloneVolume { new_ord: ord, .. } => Err(MetaDbError::Corruption(format!(
            "Phase 7 volume-lifecycle WAL op for ord {ord} hit the commit-6 apply path; \
             commit 8/9 implements these — this binary is too old to replay it"
        ))),
    }
}

/// Apply-time CAS gate. Returns true iff `new_seq` is stale relative
/// to `cur` and the op must be skipped. `seq == 0` on either side is
/// the no-guard sentinel (legacy callers like `DedupScanner` and
/// direct `insert`) — the check is bypassed and the op applies. The
/// guard is strict less-than: `new_seq == cur_seq` accepts. Onyx's
/// buffer seq is globally monotonic per append, so equality only
/// happens when a recovered buffer entry replays its own previously
/// committed write (mark_flushed is memory-only — after a clean
/// shutdown + reopen, the recovered entry is re-flushed even though
/// L2P already carries its seq). Accepting on equality lets the
/// retry land instead of leaking the freshly-allocated PBA.
/// See `L2pValue::seq` for the wire layout.
#[inline]
pub(super) fn seq_guard_rejects(new_seq: u64, cur: Option<&L2pValue>) -> bool {
    if new_seq == 0 {
        return false;
    }
    match cur {
        Some(c) => {
            let cs = c.seq();
            cs != 0 && new_seq < cs
        }
        None => false,
    }
}

/// Apply one [`WalOp::L2pRemap`]. Fuses L2P put + refcount decref(old)
/// + refcount incref(new) + (optionally) a liveness guard read into one
/// atomic step — the onyx adapter hot path.
///
/// Decision table (post birth/death LSN + precise snap-pin check):
///
/// | prev        | new vs old | snap pins (V,lba,old)? | decref(old) | incref(new) |
/// |-------------|------------|------------------------|-------------|-------------|
/// | `None`      | —          | —                      | no          | **yes**     |
/// | `Some(old)` | same       | —                      | no          | no          |
/// | `Some(old)` | different  | no                     | **yes**     | **yes**     |
/// | `Some(old)` | different  | yes (suppress)         | no          | **yes**     |
///
/// "Snap pins (V, lba, old)" = some live snap of V has lba mapped to
/// old_pba in its L2P. Decided in two stages:
///   1. Fast filter: `old_pba.birth_lsn > min(snap.created_lsn)` →
///      content is younger than every snap → no snap can pin it → no.
///   2. Otherwise read each snap's L2P at lba (cached after first
///      access) → match against old_pba.
///
/// Replaces the legacy `leaf_was_shared` proxy, which only fired on
/// the *first* COW of a snapshot-shared leaf and missed subsequent
/// overwrites in the same (now-private) leaf — see
/// [`docs/ONYX_INTEGRATION_SPEC.md`](../../docs/ONYX_INTEGRATION_SPEC.md)
/// §4.4.
///
/// Locking: the L2P shard mutex stays held across snap reads + refcount
/// RMWs so the suppress decision and the incref/decref land atomically
/// against any concurrent op on the same (vol, lba). Snap reads use
/// the same `PagedL2p` instance (different root) so no extra locks.
pub(super) fn apply_l2p_remap(
    volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
    refcount_shards: &[Shard],
    lsn: Lsn,
    vol_ord: VolumeOrdinal,
    lba: Lba,
    new_value: L2pValue,
    guard: Option<(Pba, u32)>,
    snap_infos: &[SnapInfo],
) -> Result<ApplyOutcome> {
    let volume = volumes.get(&vol_ord).ok_or_else(|| {
        MetaDbError::Corruption(format!("L2pRemap for unknown volume ord {vol_ord}"))
    })?;
    let l2p_sid = shard_for_key_l2p(&volume.shards, lba);
    let new_pba = new_value.head_pba();
    let new_is_zero = new_value.0[27] & 0x02 != 0;
    let use_buffer = volume.shards[l2p_sid].use_buffer;

    // Guard check is done after the L2P shard mutex is taken but
    // before any mutation, so the "guard passed" decision and the
    // subsequent put/incref/decref sit inside the same critical
    // section (SPEC §4.3). The guard reads the target-pba's refcount
    // shard; that shard may differ from `new_pba` / `old_pba`'s
    // shards — we acquire all needed refcount shards up front in
    // sorted order to avoid cross-shard deadlock.
    //
    // In the B2 buffer path the tree write lock STILL protects against
    // concurrent compactor cycles on this shard. Per-shard apply
    // serialisation comes from the apply lane (one worker per shard),
    // not from this lock. The lock prevents commits from racing the
    // compactor's `tree.write()` mid-cycle.
    let mut tree = volume.shards[l2p_sid].tree.write();

    if let Some((gp, min_rc)) = guard {
        let gp_sid = shard_for_key(refcount_shards, gp);
        let cur = refcount_shards[gp_sid].rc.get(gp)?;
        if cur < min_rc {
            return Ok(ApplyOutcome::L2pRemap {
                applied: false,
                prev: None,
                freed_pba: None,
            });
        }
    }

    // Read current value: buffer-first when B2 is active, else tree.
    let cur = if use_buffer {
        match volume.shards[l2p_sid].l2p_buffer.lookup(lba) {
            crate::db::l2p_buffer::BufferLookup::Present(v) => Some(v),
            crate::db::l2p_buffer::BufferLookup::Tombstone => None,
            crate::db::l2p_buffer::BufferLookup::Absent => tree.get(lba)?,
        }
    } else {
        tree.get(lba)?
    };
    if seq_guard_rejects(new_value.seq(), cur.as_ref()) {
        return Ok(ApplyOutcome::L2pRemap {
            applied: false,
            prev: cur,
            freed_pba: None,
        });
    }

    // Drive the mutation. In B2 the prev value we just read IS the
    // pre-write state — buffer.insert is a swap, so `cur` (read above)
    // equals what tree.insert_at_lsn would have returned.
    let prev = if use_buffer {
        volume.shards[l2p_sid]
            .l2p_buffer
            .insert(lba, new_value, lsn);
        cur
    } else {
        tree.insert_at_lsn(lba, new_value, lsn)?
    };
    let old_pba = prev.map(|p| p.head_pba());

    // Snapshot-pin check: does any live snap of `vol_ord` map `lba`
    // to the FULL `L2pValue` we're about to lose / gain? Audit
    // semantics count distinct `(V, lba, value_28B)` tuples — so two
    // values that share head_pba but differ on later bytes (e.g.
    // `salt`) are independently counted, and a snap pinning
    // `(V, lba, old_value)` blocks decref of `head_pba(old_value)`
    // only when the FULL value matches.
    //
    // Two-stage detection:
    //   1. Per-snap fast filter: skip any snap whose `created_lsn` is
    //      earlier than `target.head_pba`'s `birth_lsn` — that snap was
    //      taken before the content existed and cannot reference it.
    //      Saves 1 paged-tree walk per skippable snap.
    //   2. For surviving candidates, walk `L2P[V][lba]` from each
    //      snap's root (single paged-tree get, hot pages cached) and
    //      compare full value.
    // Reads use the same shard's `tree` instance via `get_at(snap_root,
    // lba)` so no extra locking is needed.
    let snap_sid = shard_for_key_l2p(&volume.shards, lba);
    let any_snap_pins =
        |target: L2pValue, target_birth: Option<Lsn>, tree: &mut PagedL2p| -> Result<bool> {
            for s in snap_infos {
                if let Some(b) = target_birth {
                    if b > s.created_lsn {
                        // pba content was born after this snap — snap
                        // can't reference it via any lba.
                        continue;
                    }
                }
                let snap_root = s.l2p_shard_roots[snap_sid];
                if let Some(snap_val) = tree.get_at(snap_root, lba)? {
                    if snap_val == target {
                        return Ok(true);
                    }
                }
            }
            Ok(false)
        };

    let snap_pins_old = if snap_infos.is_empty() {
        false
    } else {
        match prev {
            Some(old_value) => {
                let birth = lookup_birth_lsn(refcount_shards, old_value.head_pba())?;
                any_snap_pins(old_value, birth, &mut tree)?
            }
            None => false,
        }
    };
    let snap_pins_new = if new_is_zero || snap_infos.is_empty() {
        false
    } else {
        let birth = lookup_birth_lsn(refcount_shards, new_pba)?;
        any_snap_pins(new_value, birth, &mut tree)?
    };

    // Publish here — snap_pins above needed `&mut tree`; below this
    // line the function only touches refcount shards.
    //
    // B2 path: the compactor will publish on its next cycle, so commit
    // here only needs to make the buffer entry observable, which
    // `buffer.insert` did atomically above.
    if !use_buffer {
        publish_l2p_read_view(&volume.shards[l2p_sid], &tree);
    }

    // Decision: rc[head_pba] changes only when the tuple is gained /
    // lost from the union of (live ∪ all snaps). `prev != Some(new)`
    // captures "live's mapping at this lba changes value byte-for-byte".
    let value_changed = prev != Some(new_value);
    let live_loses_old = prev.is_some() && value_changed;
    let live_gains_new = value_changed;
    let old_is_zero = prev.is_some_and(|old_value| old_value.0[27] & 0x02 != 0);
    let do_decref = live_loses_old && !old_is_zero && !snap_pins_old;
    let do_incref = live_gains_new && !new_is_zero && !snap_pins_new;

    // Collapse per-pba net delta before taking shard locks. Same-head-pba
    // overwrites can legitimately add and remove one logical version in the
    // same op; applying the decref and incref independently would transiently
    // hit rc=0 and incorrectly surface `freed_pba` even though the net effect
    // is zero.
    let mut net_delta: HashMap<Pba, i32> = HashMap::new();
    if do_decref {
        let pba = old_pba.expect("do_decref implies prev.is_some()");
        *net_delta.entry(pba).or_insert(0) -= 1;
    }
    if do_incref {
        *net_delta.entry(new_pba).or_insert(0) += 1;
    }
    let mut touched: Vec<(usize, Pba, i32)> = net_delta
        .into_iter()
        .filter(|(_, delta)| *delta != 0)
        .map(|(pba, delta)| (shard_for_key(refcount_shards, pba), pba, delta))
        .collect();
    touched.sort_by_key(|(sid, _, _)| *sid);

    let mut freed_pba: Option<Pba> = None;
    for (sid, pba, delta) in touched {
        let (pre, new) = refcount_shards[sid].rc.stage(pba, i64::from(delta), lsn)?;
        if new == 0 && pre > 0 {
            freed_pba = Some(pba);
        }
    }

    Ok(ApplyOutcome::L2pRemap {
        applied: true,
        prev,
        freed_pba,
    })
}

/// Apply one [`WalOp::L2pRangeDelete`]. Walks the `captured` list,
/// deleting each lba from its volume's L2P shard and emitting a
/// decref against `old_pba` — modulo birth/death LSN suppression: if
/// `min_alive_snap_lsn` is `Some(snap_lsn)` and `pba.birth_lsn <=
/// snap_lsn`, the snapshot may still reference this pba and the
/// decref is suppressed. `drop_snapshot` later compensates via the
/// deadlist (next session) or via `diff_subtrees`-derived
/// `pba_decrefs` (current).
///
/// Replay safety: the captured list is authoritative — both live
/// apply and replay consume the same (lba, pba) pairs, and birth_lsn
/// is part of the rc tree's persistent state, so suppress decisions
/// reproduce across restarts.
/// Range-delete's `Db::range_delete` caller uses apply_gate.write
/// (same pattern as `drop_snapshot`) to exclude concurrent commits
/// while plan + submit + apply run, so captured is consistent with
/// the tree state at apply time.
pub(super) fn apply_l2p_range_delete(
    volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
    refcount_shards: &[Shard],
    lsn: Lsn,
    vol_ord: VolumeOrdinal,
    captured: &[(Lba, L2pValue)],
    snap_infos: &[SnapInfo],
) -> Result<ApplyOutcome> {
    let volume = volumes.get(&vol_ord).ok_or_else(|| {
        MetaDbError::Corruption(format!("L2pRangeDelete for unknown volume ord {vol_ord}"))
    })?;

    // Bucket captured entries by L2P shard so each tree mutex is
    // taken once.
    let shard_count = volume.shards.len();
    let mut shard_buckets: Vec<Vec<usize>> = vec![Vec::new(); shard_count];
    for (idx, (lba, _)) in captured.iter().enumerate() {
        shard_buckets[shard_for_key_l2p(&volume.shards, *lba)].push(idx);
    }

    // For each captured (lba, value): did it have a live mapping? And
    // if so, does any live snap of V still hold the *exact same* full
    // value at that lba? Full-value compare matches audit semantics
    // (distinct `(V, lba, value_28B)` tuples). Both determined while
    // holding the L2P shard mutex; snap reads use `get_at` on the same
    // tree instance, different root.
    let mut was_mapped: Vec<bool> = vec![false; captured.len()];
    let mut snap_pinned: Vec<bool> = vec![false; captured.len()];
    for (sid, indices) in shard_buckets.iter().enumerate() {
        if indices.is_empty() {
            continue;
        }
        let mut tree = volume.shards[sid].tree.write();
        for &idx in indices {
            let (lba, captured_value) = captured[idx];
            let prev = tree.delete_at_lsn(lba, lsn)?;
            was_mapped[idx] = prev.is_some();
            if !snap_infos.is_empty() && was_mapped[idx] {
                // Per-snap fast filter via birth_lsn, same shape as
                // apply_l2p_remap. Look up birth_lsn first under the
                // rc shard mutex, then walk only candidate snaps.
                let captured_birth = lookup_birth_lsn(refcount_shards, captured_value.head_pba())?;
                for s in snap_infos {
                    if let Some(b) = captured_birth {
                        if b > s.created_lsn {
                            continue;
                        }
                    }
                    let snap_root = s.l2p_shard_roots[sid];
                    if let Some(snap_val) = tree.get_at(snap_root, lba)? {
                        if snap_val == captured_value {
                            snap_pinned[idx] = true;
                            break;
                        }
                    }
                }
            }
        }
        publish_l2p_read_view(&volume.shards[sid], &tree);
    }

    // Second pass: precise snap-pin suppress + decref. Bucket by
    // refcount shard so each shard mutex is taken at most once.
    let mut rc_bucket: Vec<Vec<usize>> = vec![Vec::new(); refcount_shards.len()];
    for (idx, (_, value)) in captured.iter().enumerate() {
        if !was_mapped[idx] || snap_pinned[idx] || value.0[27] & 0x02 != 0 {
            continue;
        }
        rc_bucket[shard_for_key(refcount_shards, value.head_pba())].push(idx);
    }

    let mut freed_pbas: Vec<Pba> = Vec::new();
    for (sid, indices) in rc_bucket.iter().enumerate() {
        if indices.is_empty() {
            continue;
        }
        let shard = &refcount_shards[sid];
        for &idx in indices {
            let (_, value) = captured[idx];
            let pba = value.head_pba();
            let (pre, new) = shard.rc.stage(pba, -1, lsn)?;
            if new == 0 && pre > 0 {
                freed_pbas.push(pba);
            }
        }
    }

    Ok(ApplyOutcome::RangeDelete { freed_pbas })
}

pub(super) fn collect_paged_refcounts_for_roots(
    page_store: &Arc<PageStore>,
    roots: &[PageId],
) -> Result<BTreeMap<PageId, u32>> {
    fn walk(
        page_store: &PageStore,
        pid: PageId,
        refs: &mut BTreeMap<PageId, u32>,
        seen: &mut HashSet<PageId>,
    ) -> Result<()> {
        if !seen.insert(pid) {
            return Ok(());
        }
        let page = page_store.read_page(pid)?;
        match page.header()?.page_type {
            PageType::PagedLeaf => Ok(()),
            PageType::PagedIndex => {
                for slot in 0..crate::paged::format::INDEX_FANOUT {
                    let child = crate::paged::format::index_child_at(&page, slot);
                    if child == crate::types::NULL_PAGE {
                        continue;
                    }
                    *refs.entry(child).or_insert(0) += 1;
                    walk(page_store, child, refs, seen)?;
                }
                Ok(())
            }
            other => Err(MetaDbError::Corruption(format!(
                "page {pid} has unexpected type {other:?} in paged refcount walk"
            ))),
        }
    }

    let mut refs = BTreeMap::new();
    let mut seen = HashSet::new();
    for &root in roots {
        if root == crate::types::NULL_PAGE {
            continue;
        }
        *refs.entry(root).or_insert(0) += 1;
        walk(page_store, root, &mut refs, &mut seen)?;
    }
    Ok(refs)
}

/// Allocate a fresh shard group for a `CreateVolume` apply. Delegates
/// to [`create_l2p_shards`]; kept separate so the Db public API and
/// the recovery replay closure share one call site.
pub(super) fn apply_create_volume(
    page_store: &Arc<PageStore>,
    page_cache: &Arc<PageCache>,
    shard_count: u32,
    metrics: Arc<MetaMetrics>,
    use_buffer: bool,
) -> Result<(Vec<L2pShard>, Box<[PageId]>)> {
    let n = validate_shard_count(shard_count)?;
    create_l2p_shards(page_store.clone(), page_cache.clone(), n, metrics, use_buffer)
}

/// Apply a `DropVolume` op's page-decref cascade. Reuses
/// [`apply_drop_snapshot_pages`]; `DropVolume` has the same
/// per-page semantics (decref, free at rc=0, idempotent via
/// `page.generation >= lsn`) and just doesn't need the freed-leaf-values
/// vec the snapshot path surfaces in its report.
pub(super) fn apply_drop_volume(
    page_store: &Arc<PageStore>,
    lsn: Lsn,
    pages: &[PageId],
) -> Result<usize> {
    let (_leaf_values, pages_freed) = apply_drop_snapshot_pages(page_store, lsn, pages)?;
    Ok(pages_freed)
}

/// Increment the on-disk refcount of each shard root that a cloned
/// volume pins. Idempotent across replays: pages already stamped with
/// `page.generation >= lsn` are skipped (same guard pattern
/// [`apply_drop_snapshot_pages`] uses). `NULL_PAGE` roots — empty
/// source shards — are ignored because the clone materialises fresh
/// empty trees for those shards (see [`build_clone_volume_shards`]).
pub(super) fn apply_clone_volume_incref(
    page_store: &Arc<PageStore>,
    faults: &FaultController,
    lsn: Lsn,
    src_shard_roots: &[PageId],
) -> Result<()> {
    for (idx, &pid) in src_shard_roots.iter().enumerate() {
        if pid == crate::types::NULL_PAGE {
            continue;
        }
        let mut page = page_store.read_page_unchecked(pid)?;
        page.verify(pid)?;
        let header = page.header()?;
        if header.generation >= lsn {
            // Already incref'd by a prior apply of this same CloneVolume
            // op (replay-after-crash case); skip.
            continue;
        }
        let new_rc = header.refcount.checked_add(1).ok_or_else(|| {
            MetaDbError::Corruption(format!(
                "clone_volume: refcount overflow on source root page {pid}"
            ))
        })?;
        page.set_refcount(new_rc);
        page.set_generation(lsn);
        page.seal();
        page_store.write_page(pid, &page)?;
        // Fault injection window: fires after the first root is durably
        // incref'd but before subsequent ones. Recovery's generation-stamp
        // guard skips the pre-fault root and completes the rest.
        if idx == 0 {
            faults.inject(FaultPoint::CloneVolumeMidIncref)?;
        }
    }
    page_store.sync()?;
    Ok(())
}

/// Build the new volume's shard group for a clone. Each source root
/// becomes the initial root of a fresh [`PagedL2p`]; empty source
/// shards (`NULL_PAGE` root) get a freshly-allocated empty leaf so the
/// tree is always operable. Caller must have already incref'd the
/// non-null roots via [`apply_clone_volume_incref`].
pub(super) fn build_clone_volume_shards(
    src_shard_roots: &[PageId],
    page_store: &Arc<PageStore>,
    page_cache: &Arc<PageCache>,
    created_lsn: Lsn,
    metrics: Arc<MetaMetrics>,
    use_buffer: bool,
) -> Result<(Vec<L2pShard>, Box<[PageId]>)> {
    let mut shards = Vec::with_capacity(src_shard_roots.len());
    let mut actual_roots = Vec::with_capacity(src_shard_roots.len());
    for (shard_idx, &root) in src_shard_roots.iter().enumerate() {
        let tree = if root == crate::types::NULL_PAGE {
            PagedL2p::create_with_cache(page_store.clone(), page_cache.clone())?
        } else {
            PagedL2p::open_with_cache(
                page_store.clone(),
                page_cache.clone(),
                root,
                created_lsn + 1,
            )?
        };
        actual_roots.push(tree.root());
        shards.push(super::helpers::make_l2p_shard(
            tree,
            page_cache,
            shard_idx,
            metrics.clone(),
            // Cloned volume has no on-disk dirty pages of its own
            // yet; its shards' content is durable for every LSN at
            // or before `created_lsn` (because the volume didn't
            // exist before). Setting `last_flushed_lsn = created_lsn`
            // keeps `Db::compute_min_last_flushed_lsn` from being
            // dragged down to 0 by a fresh volume that hasn't been
            // flushed yet. Subsequent partial flushes bump it as
            // usual.
            created_lsn,
            use_buffer,
        ));
    }
    Ok((shards, actual_roots.into_boxed_slice()))
}

/// Core of the `DropSnapshot` / `DropVolume` page-refcount cascade.
/// Iterates `pages`, decrements each page's refcount by 1, stamps
/// `generation = lsn`, and frees any page that hits rc=0. Idempotent
/// on replay via the generation check.
///
/// Returns `(freed_leaf_values, pages_freed)` rather than a full
/// `ApplyOutcome` so `DropVolume` (which doesn't care about leaf values)
/// can reuse the function without unpacking a misnamed variant.
pub(super) fn apply_drop_snapshot_pages(
    page_store: &Arc<PageStore>,
    lsn: Lsn,
    pages: &[PageId],
) -> Result<(Vec<L2pValue>, usize)> {
    use crate::page::PageType;

    let mut freed_leaf_values: Vec<L2pValue> = Vec::new();
    let mut pages_freed: usize = 0;

    for &pid in pages {
        let mut page = page_store.read_page_unchecked(pid)?;
        if page.bytes().iter().all(|b| *b == 0) {
            // `free_idempotent` punches freed pages after writing the
            // Free header. On reopen, a page already processed by a
            // crashed prior apply can therefore read back as all zeroes
            // rather than as a decodable Free page.
            pages_freed += 1;
            continue;
        }
        // verify before we do anything; `read_page_unchecked` skipped it
        page.verify(pid)?;
        let header = page.header()?;
        if header.generation >= lsn {
            // already processed by a prior apply of this same DropSnapshot
            // (replay-after-crash case); skip to keep the overall apply
            // idempotent.
            if header.page_type == PageType::Free {
                // count it so the outcome is stable across replays — but
                // skip the write.
                pages_freed += 1;
            }
            continue;
        }
        let rc = header.refcount;
        if rc == 0 {
            return Err(MetaDbError::Corruption(format!(
                "DropSnapshot apply: page {pid} already at refcount 0"
            )));
        }
        let new_rc = rc - 1;
        if new_rc == 0 {
            if matches!(header.page_type, PageType::PagedLeaf) {
                for i in 0..crate::paged::format::LEAF_ENTRY_COUNT {
                    if crate::paged::format::leaf_bit_set(&page, i) {
                        if let Some(value) = crate::paged::format::leaf_value_at(&page, i)? {
                            freed_leaf_values.push(value);
                        }
                    }
                }
            }
            // free_idempotent stamps generation=lsn via the Free header.
            let pushed = page_store.free_idempotent(pid, lsn)?;
            if pushed {
                pages_freed += 1;
            }
        } else {
            page.set_refcount(new_rc);
            page.set_generation(lsn);
            page.seal();
            page_store.write_page(pid, &page)?;
        }
    }

    page_store.sync()?;

    Ok((freed_leaf_values, pages_freed))
}

/// Apply a full `WalOp::DropSnapshot`: the page-refcount cascade for
/// `pages` followed by the SPEC §3.3 leaf-rc-suppress compensation —
/// one `decref(pba, 1)` per entry in `pba_decrefs`, collected at plan
/// time via `diff_with_current`.
///
/// `pba_decrefs` is walked in shard-sorted order; each shard mutex is
/// taken once, mirroring `apply_l2p_range_delete`'s pattern. The
/// returned `freed_pbas` lists every pba whose refcount transitioned
/// from `>0` to `0` during this apply. Duplicates in `pba_decrefs`
/// are intentional (packed-slot many-LBA-share-one-pba case) and each
/// produces one decref; only the one that drives rc to 0 adds the pba
/// to `freed_pbas`.
///
/// No leaf-rc-suppress logic here: `drop_snapshot` holds
/// `drop_gate.write()` + `apply_gate.write()`, so there is no
/// concurrent mutation, and "suppress because a live snapshot still
/// pins it" cannot apply — we *are* dropping the snapshot.
pub(super) fn apply_drop_snapshot_pages_and_decrefs(
    page_store: &Arc<PageStore>,
    refcount_shards: &[Shard],
    lsn: Lsn,
    pages: &[PageId],
    pba_decrefs: &[Pba],
) -> Result<ApplyOutcome> {
    let (freed_leaf_values, pages_freed) = apply_drop_snapshot_pages(page_store, lsn, pages)?;

    let mut rc_bucket: Vec<Vec<usize>> = vec![Vec::new(); refcount_shards.len()];
    for (idx, &pba) in pba_decrefs.iter().enumerate() {
        rc_bucket[shard_for_key(refcount_shards, pba)].push(idx);
    }

    let mut freed_pbas: Vec<Pba> = Vec::new();
    for (sid, indices) in rc_bucket.iter().enumerate() {
        if indices.is_empty() {
            continue;
        }
        let shard = &refcount_shards[sid];
        for &idx in indices {
            let pba = pba_decrefs[idx];
            let (pre, new) = shard.rc.stage(pba, -1, lsn)?;
            if new == 0 && pre > 0 {
                freed_pbas.push(pba);
            }
        }
    }

    Ok(ApplyOutcome::DropSnapshot {
        freed_leaf_values,
        pages_freed,
        freed_pbas,
    })
}

pub(super) fn shard_for_key(shards: &[Shard], key: u64) -> usize {
    (xxh3_64(&key.to_be_bytes()) as usize) % shards.len()
}

/// Read `pba`'s `birth_lsn` from the refcount shard. Returns `Some` if
/// the entry is live (rc > 0); `None` if rc == 0. Used by snap-pin
/// fast filters in [`apply_l2p_remap`] / [`apply_l2p_range_delete`].
///
/// The 0→1 birth_lsn stamp is what powers the birth/death LSN
/// suppression: a pba's birth_lsn equals the lsn of the op that
/// revived it from rc=0, so snapshots whose `created_lsn >= birth_lsn`
/// can be ruled out as having pinned this pba's content.
pub(super) fn lookup_birth_lsn(refcount_shards: &[Shard], pba: Pba) -> Result<Option<Lsn>> {
    let sid = shard_for_key(refcount_shards, pba);
    let entry = refcount_shards[sid].rc.get_entry(pba)?;
    Ok(if entry.rc > 0 {
        Some(entry.birth_lsn)
    } else {
        None
    })
}

pub(super) fn shard_for_key_l2p(shards: &[L2pShard], key: u64) -> usize {
    // Keep one compact L2P leaf (128 consecutive LBAs) in one shard. The old
    // hash(lba) router scattered a single 128 KiB/256 KiB user read across
    // almost every shard, so read-side multi_get had to walk 16 independent
    // trees before issuing the LV3 reads. Hashing leaf_idx preserves random
    // write distribution while making spatially-local reads and remap batches
    // shard-local.
    let leaf_idx = key >> crate::paged::format::LEAF_SHIFT;
    (xxh3_64(&leaf_idx.to_be_bytes()) as usize) % shards.len()
}

/// Returns true if the batch contains any op whose apply has manifest
/// or volume-lifecycle side effects. Used to fall back to serial apply.
pub(super) fn batch_contains_lifecycle_op(ops: &[WalOp]) -> bool {
    ops.iter().any(|op| {
        matches!(
            op,
            WalOp::DropSnapshot { .. }
                | WalOp::CreateVolume { .. }
                | WalOp::DropVolume { .. }
                | WalOp::CloneVolume { .. }
                // L2pRangeDelete has its own Db entry point
                // (`Db::range_delete`) that submits + applies inline
                // under apply_gate.write, mirroring drop_snapshot.
                // It never reaches commit_ops; this arm just keeps
                // the bucketed path safe if a future caller routes
                // it through here by mistake.
                | WalOp::L2pRangeDelete { .. }
        )
    })
}
