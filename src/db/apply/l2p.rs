use super::*;

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
pub(in crate::db) fn seq_guard_rejects(new_seq: u64, cur: Option<&L2pValue>) -> bool {
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

/// Stamp the incoming value with the current apply `lsn` as its
/// `birth_lsn` if the caller did not already attach one (sentinel 0).
/// Promote / dedup-hit / scanner-remap callers carry the source PBA's
/// original birth_lsn in the value and want it preserved; fresh writes
/// arrive with birth_lsn=0 and get stamped here so Phase 2's per-volume
/// dead-list emitter ([[no-refcount-hot-path-design]]) can read it
/// directly off `ApplyOutcome::L2pRemap.prev` without an extra
/// refcount-shard lookup.
#[inline]
pub(in crate::db) fn stamp_birth_lsn(value: L2pValue, lsn: Lsn) -> L2pValue {
    if value.birth_lsn() == 0 {
        value.with_birth_lsn(lsn)
    } else {
        value
    }
}

/// Emit `(prev.head_pba, prev.birth_lsn, death_lsn=lsn)` into the
/// volume's in-memory dead-list buffer if `prev` represents a real
/// mapping (i.e. not a `FLAG_ZERO` placeholder). The buffer is drained
/// at the next checkpoint flush; WAL replay re-emits whatever hasn't
/// been written to a segment yet, so this hook is safe to fire from
/// both live commit and recovery paths.
#[inline]
pub(in crate::db) fn record_dead(volume: &Volume, prev: Option<L2pValue>, death_lsn: Lsn) {
    if let Some(old) = prev {
        if old.0[27] & 0x02 != 0 {
            return;
        }
        volume.dead_list.push(crate::deadlist::DeadRecord {
            pba: old.head_pba(),
            birth_lsn: old.birth_lsn(),
            death_lsn,
        });
    }
}

/// The live snapshots' `capture_watermark`s across `snap_infos`, the operand of
/// the birth COW-kill decision (fed to `tree.set_snapshot_wms`).
///
/// v21 (Phase 4 S1): MUST be the fold-watermarks (`max(root.birth_lsn)`), NOT
/// `created_lsn`. `created_lsn` (= last_applied) races ahead of the fold under
/// concurrent writers + background TXG threads, so a page with
/// `birth <= created_lsn` need NOT be in any snapshot's roots — using it
/// over-pins HEAD-only transients into the page-deadlist, surfacing as
/// premature-frees the drop shadow rejects. `capture_watermark` is exactly what
/// the roots contain. The COW-kill further filters by the dying page's lsn
/// (`youngest_snap_below`), so the full list (not just the max) is needed.
#[inline]
pub(in crate::db) fn snapshot_wms_of(snap_infos: &[SnapInfo]) -> Vec<Lsn> {
    snap_infos.iter().map(|s| s.capture_watermark).collect()
}

/// ZFS port Phase 2: drain the tree's page-deadlist witness (L2P pages
/// displaced off the head by a shared COW this op) and append the
/// snapshot-pinned survivors to the volume's HEAD page-deadlist. ALWAYS drains
/// the witness (so it never leaks into the next op even when no snapshot is
/// live). Fires from BOTH the direct-apply tree COW and the buffer-fold COW (the
/// only two `cow_for_write` entry points). The records carry the dying `PageId`
/// in `DeadRecord.pba`.
#[inline]
pub(in crate::db) fn drain_page_deaths(
    volume: &Volume,
    tree: &mut PagedL2p,
    _snap_infos: &[SnapInfo],
) {
    drain_page_deaths_into(&volume.page_dead_list, tree);
}

/// Lower-level page-death drainer. ALWAYS drains the witness (clears it so it
/// can't leak into the next fold).
///
/// NON-CLONE: records EVERY `cow_displaced` record. `cow_for_write` only pushes
/// in its SHARED (snapshot-pinned) arm (`rc>1 OR birth`), so every record is
/// already a gated snapshot death — re-filtering by birth here would DROP the
/// `rc>1`-pinned ones (whose `birth > youngest_snap_below`, the snapshot-cache
/// lag case), leaving the drop-time page-deadlist MISSING a page the structural
/// graph frees (a completeness hole).
///
/// CLONE: the SHARED arm fires on `rc>1` (clone-private sharing OR snapshot), so
/// filter by `birth <= youngest_snap_below(death_lsn)` to keep only the
/// snapshot-captured deaths for the page-deadlist; the clone-private ones
/// (`birth` past every clone-snapshot watermark) are owned by the livelist.
#[inline]
pub(in crate::db) fn drain_page_deaths_into(
    page_dead_list: &crate::deadlist::DeadListState,
    tree: &mut PagedL2p,
) {
    let is_clone = tree.is_clone();
    let displaced = tree.take_cow_displaced();
    for rec in displaced {
        let keep = if is_clone {
            tree.youngest_snap_below(rec.death_lsn)
                .is_some_and(|y| rec.birth_lsn <= y)
        } else {
            true
        };
        if keep {
            page_dead_list.push(rec);
        }
    }
}

/// ZFS port Phase 3b: drain the tree's page-livelist witness (ALLOC/FREE
/// events for this clone's clone-private L2P pages this op) into the clone's
/// `page_live_list`. Peer of [`drain_page_deaths`]; called at every same
/// site. ALWAYS drains the witness (clears it so it can't leak into the next
/// op — the witness is empty for non-clones, whose trees never capture). No
/// `youngest`-style gate and no replay suppression: the tree already gates
/// capture by `clone_birth_lsn` + `birth > B`, and the `checkpoint_lsn`
/// cutoff makes persisted events (`event_lsn <= checkpoint_lsn`) disjoint
/// from any replayed op's events (`event_lsn > checkpoint_lsn`), so
/// re-recording on replay/buffer-fold cannot double up the durable chain.
#[inline]
pub(in crate::db) fn drain_live_events(volume: &Volume, tree: &mut PagedL2p) {
    drain_live_events_into(&volume.page_live_list, tree);
}

/// Lower-level peer of [`drain_live_events`] for the buffer-fold path, which
/// holds the volume's `page_live_list` but not the full `Volume`.
#[inline]
pub(in crate::db) fn drain_live_events_into(
    page_live_list: &crate::livelist::LiveListState,
    tree: &mut PagedL2p,
) {
    for rec in tree.take_live_events() {
        page_live_list.push(rec);
    }
}

/// Does any live snapshot in `snap_infos` pin `target` at `lba`? A snapshot
/// pins it iff its point-in-time L2P maps `lba` to the *exact same* 28-byte
/// value (audit semantics count distinct `(V, lba, value)` tuples, so two
/// values sharing a head_pba but differing on later bytes are independent).
///
/// rc-authoritative decref suppression: when a live overwrite/delete drops a
/// reference to `old.head_pba`, the decref is suppressed if some snapshot
/// still pins it — otherwise rc could fall to 0 while a snapshot references it
/// and reclaim (rc==0) would free a still-live PBA. Snapshots do NOT incref
/// PBA rc (they share via L2P page-COW), so this suppression is what keeps
/// snapshot-referenced PBAs alive in the authoritative count.
///
/// Per-snap fast filter: skip any snapshot whose `created_lsn < target_birth`
/// — it was taken before this content existed and cannot reference it.
/// `target_birth == 0` (unstamped) disables the filter for that probe (walk
/// all). `snap_sid` indexes both the live shard layout and each snapshot's
/// `l2p_shard_roots` (the shard router is volume-agnostic). The probe reads
/// the snapshot's immutable root via `get_at_read_only`; the caller holds the
/// shard `tree.write()` and `apply_gate.read()` excludes `drop_snapshot`, so
/// the snapshot's pages are stable during the walk.
fn any_snap_pins(
    snap_infos: &[SnapInfo],
    snap_sid: usize,
    tree: &PagedL2p,
    lba: Lba,
    target: L2pValue,
    target_birth: Lsn,
) -> Result<bool> {
    for s in snap_infos {
        if target_birth != 0 && target_birth > s.created_lsn {
            continue;
        }
        let snap_root = s.l2p_shard_roots[snap_sid];
        if let Some(snap_val) = tree.get_at_read_only(snap_root, lba)? {
            if snap_val == target {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Apply one [`WalOp::L2pRemap`]. Mutates the L2P shard
/// (buffer or tree, depending on the B2 toggle).
///
/// Two rc regimes, selected by `rc_authoritative`:
/// * **Off (Phase 5)** — hot-path L2P remaps are **rc-neutral**; only
///   `PromotionChunk` / `FreePbas` / the `DedupPut`/`Dedup*` family move rc.
///   `snap_infos` is unused; the prev mapping rides the dead-list via
///   `record_dead` and lineage GC drives reclaim.
/// * **On (rc-authoritative)** — refcount counts live L2P references, so this
///   remap does the traditional inline pair: **incref(new) + decref(old)**,
///   net-collapsed per pba (a same-pba overwrite cancels to 0 and never
///   transiently surfaces freed_pba). The decref is **suppressed when a live
///   snapshot still pins `old`** (`any_snap_pins`) — snapshots don't incref
///   PBA rc, so suppression keeps a snapshot-referenced PBA at rc > 0 and out
///   of reclaim's `rc==0` Gate. `record_dead` is NOT called (lineage stays
///   dormant — inline decref is the sole rc driver, balanced 1:1 with the
///   incref). A net `rc==0` surfaces `freed_pba` for onyx's writer cleanup.
///
/// `guard` still applies — onyx-side dedup-hit promote relies on a
/// liveness floor read of a target PBA's rc to gate the remap.
#[allow(clippy::too_many_arguments)]
pub(in crate::db) fn apply_l2p_remap(
    volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
    refcount_shards: &[Shard],
    lsn: Lsn,
    txg: crate::types::Txg,
    vol_ord: VolumeOrdinal,
    lba: Lba,
    new_value: L2pValue,
    guard: Option<(Pba, u32)>,
    snap_infos: &[SnapInfo],
    rc_authoritative: bool,
) -> Result<ApplyOutcome> {
    let volume = volumes.get(&vol_ord).ok_or_else(|| {
        MetaDbError::Corruption(format!("L2pRemap for unknown volume ord {vol_ord}"))
    })?;
    let l2p_sid = shard_for_key_l2p(&volume.shards, lba);
    let use_buffer = volume.shards[l2p_sid].use_buffer;

    // The L2P shard write lock (or, on the B2 path, its serialising
    // role against compactor cycles) brackets the guard read + L2P
    // write so the "guard passed" decision and the put land atomically
    // against concurrent ops on this (vol, lba). Per-shard apply
    // serialisation comes from the apply lane (one worker per shard);
    // this lock prevents commits from racing the compactor's
    // `tree.write()` mid-cycle.
    let mut tree = volume.shards[l2p_sid].tree.write();
    // A3 cutover: tree-mode COW stages page-rc deltas into this commit's
    // TXG slot. (Buffer mode COWs later in the drain, which sets its own
    // sync txg; this set is then a harmless no-op for the buffered path.)
    tree.set_current_txg(txg);
    // ZFS port Phase 4 Step 4 (S1): arm the birth-authoritative non-clone
    // COW-kill decision with this op's youngest-snapshot lsn (same source the
    // `drain_page_deaths` below uses; identical live and on replay).
    tree.set_snapshot_wms(snapshot_wms_of(snap_infos));

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
        match volume.shards[l2p_sid]
            .l2p_buffer
            .lookup_for_open_txg(txg, lba)
        {
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
    let new_value = stamp_birth_lsn(new_value, lsn);

    // Drive the mutation. In B2 the prev value we just read IS the
    // pre-write state — buffer.insert is a swap, so `cur` (read above)
    // equals what tree.insert_at_lsn would have returned.
    let prev = if use_buffer {
        volume.shards[l2p_sid]
            .l2p_buffer
            .insert_at_txg(txg, lba, new_value, lsn);
        cur
    } else {
        tree.insert_at_lsn(lba, new_value, lsn)?
    };

    let mut freed_pba: Option<Pba> = None;
    if rc_authoritative {
        freed_pba = stage_remap_rc(
            refcount_shards,
            txg,
            &tree,
            l2p_sid,
            lba,
            new_value,
            prev,
            snap_infos,
            lsn,
        )?;
    } else {
        record_dead(volume, prev, lsn);
    }

    // B2 path: the compactor will publish on its next cycle, so commit
    // here only needs to make the buffer entry observable, which
    // `buffer.insert` did atomically above. (Published after the snap-pin
    // probe so the probe saw a stable `&tree`.)
    if !use_buffer {
        publish_l2p_read_view(&volume.shards[l2p_sid], &tree);
    }
    // ZFS port Phase 2: record any L2P page this remap's COW displaced
    // off the head into the HEAD page-deadlist. Direct mode COWs at
    // `tree.insert_at_lsn` above; buffer mode COWs later in the fold (the
    // witness is empty here, drained by the fold instead).
    drain_page_deaths(volume, &mut tree, snap_infos);
    drain_live_events(volume, &mut tree);

    Ok(ApplyOutcome::L2pRemap {
        applied: true,
        prev,
        freed_pba,
    })
}

/// rc-authoritative net-delta core: stage incref(new)+decref(old) for one
/// applied install, collapsing the per-pba net so a same-pba overwrite never
/// transiently crosses `rc==0` (which would falsely surface `freed_pba`).
/// `snap_pins_old`/`snap_pins_new` are the snapshot-pin suppression decisions
/// (true → that side's ±1 is dropped). Zero-flag (`FLAG_ZERO`) values and a
/// byte-identical `prev == new` re-land contribute no delta. Returns
/// `Some(pba)` iff a pba's net rc reached 0.
fn stage_rc_net(
    refcount_shards: &[Shard],
    txg: crate::types::Txg,
    new_value: L2pValue,
    prev: Option<L2pValue>,
    snap_pins_old: bool,
    snap_pins_new: bool,
    lsn: Lsn,
) -> Result<Option<Pba>> {
    let new_is_zero = new_value.0[27] & 0x02 != 0;
    if prev == Some(new_value) {
        return Ok(None);
    }
    let mut net_delta: std::collections::HashMap<Pba, i64> = std::collections::HashMap::new();
    if let Some(old) = prev {
        if old.0[27] & 0x02 == 0 && !snap_pins_old {
            *net_delta.entry(old.head_pba()).or_insert(0) -= 1;
        }
    }
    if !new_is_zero && !snap_pins_new {
        *net_delta.entry(new_value.head_pba()).or_insert(0) += 1;
    }

    // Stage in shard-sorted order (cross-shard deadlock avoidance convention).
    let mut touched: Vec<(usize, Pba, i64)> = net_delta
        .into_iter()
        .filter(|(_, d)| *d != 0)
        .map(|(pba, d)| (shard_for_key(refcount_shards, pba), pba, d))
        .collect();
    touched.sort_by_key(|(sid, _, _)| *sid);
    let mut freed_pba = None;
    for (sid, pba, delta) in touched {
        let (pre, new) = refcount_shards[sid].rc.stage(txg, pba, delta, lsn)?;
        if new == 0 && pre > 0 {
            freed_pba = Some(pba);
        }
    }
    Ok(freed_pba)
}

/// [`stage_rc_net`] plus the snapshot-pin probe. `tree` is the live shard tree
/// (write-locked by the caller, read-only here via `get_at_read_only` on
/// snapshot roots); `snap_sid` is the L2P shard index. When `snap_infos` is
/// empty the probe is skipped and both sides decref/incref unconditionally —
/// callers with no snapshots can bypass this and call [`stage_rc_net`] with
/// `false, false` directly (no tree needed). See [`any_snap_pins`].
#[allow(clippy::too_many_arguments)]
pub(in crate::db) fn stage_remap_rc(
    refcount_shards: &[Shard],
    txg: crate::types::Txg,
    tree: &PagedL2p,
    snap_sid: usize,
    lba: Lba,
    new_value: L2pValue,
    prev: Option<L2pValue>,
    snap_infos: &[SnapInfo],
    lsn: Lsn,
) -> Result<Option<Pba>> {
    if prev == Some(new_value) {
        return Ok(None);
    }
    let new_is_zero = new_value.0[27] & 0x02 != 0;
    let snap_pins_old = match prev {
        Some(old) if !snap_infos.is_empty() && old.0[27] & 0x02 == 0 => {
            any_snap_pins(snap_infos, snap_sid, tree, lba, old, old.birth_lsn())?
        }
        _ => false,
    };
    let snap_pins_new = if new_is_zero || snap_infos.is_empty() {
        false
    } else {
        any_snap_pins(snap_infos, snap_sid, tree, lba, new_value, new_value.birth_lsn())?
    };
    stage_rc_net(
        refcount_shards,
        txg,
        new_value,
        prev,
        snap_pins_old,
        snap_pins_new,
        lsn,
    )
}

/// rc-authoritative inline decref for a deleted reference (`L2pDelete` /
/// `L2pRangeDelete`): decref(old.head_pba), suppressed when a live snapshot
/// still pins `old`. No incref (a delete installs nothing). Returns
/// `Some(pba)` iff rc reached 0. `tree` is the write-locked live shard tree
/// (read-only here); `snap_sid` is the L2P shard index. FLAG_ZERO `prev` and
/// `None` contribute nothing.
pub(in crate::db) fn stage_delete_rc(
    refcount_shards: &[Shard],
    txg: crate::types::Txg,
    tree: &PagedL2p,
    snap_sid: usize,
    lba: Lba,
    prev: Option<L2pValue>,
    snap_infos: &[SnapInfo],
    lsn: Lsn,
) -> Result<Option<Pba>> {
    let Some(old) = prev.filter(|p| p.0[27] & 0x02 == 0) else {
        return Ok(None);
    };
    let snap_pins = if snap_infos.is_empty() {
        false
    } else {
        any_snap_pins(snap_infos, snap_sid, tree, lba, old, old.birth_lsn())?
    };
    if snap_pins {
        return Ok(None);
    }
    let rsid = shard_for_key(refcount_shards, old.head_pba());
    let (pre, new) = refcount_shards[rsid]
        .rc
        .stage(txg, old.head_pba(), -1, lsn)?;
    Ok(if new == 0 && pre > 0 {
        Some(old.head_pba())
    } else {
        None
    })
}

/// Apply one [`WalOp::L2pRemapRange`]: per-LBA L2P remap semantics over
/// `[start_lba .. start_lba + values.len())` of one volume, all under
/// one apply call. Equivalent in net effect to N calls of
/// [`apply_l2p_remap`] with `guard = None` on each LBA, with three
/// amortizations:
///
/// 1. **Tree write lock per shard, not per LBA**: LBAs are bucketed by
///    L2P shard once, then each shard's tree is locked once for the
///    whole bucket. Onyx's passthrough caller produces contiguous LBAs
///    that usually land in one shard (and often one leaf, since
///    `shard_for_key_l2p` hashes `lba >> LEAF_SHIFT`).
/// 2. **Refcount net delta across the range**: incref/decref pairs that
///    cancel within the same range never touch the refcount shard.
///    Same per-PBA net-delta collapse that the per-LBA path uses, just
///    aggregated over more LBAs.
/// 3. **WAL / op-dispatch / bucket-assembly cost**: the entire range is
///    one record, one outcome slot, one apply-lane dispatch.
///
/// Range ops are always unguarded; the dedup-hit path that needs a
/// guard keeps emitting per-LBA `L2pRemap`. The snap-pin check stays
/// per-LBA inside the range — a range-aware snap-pin walk is the
/// Stage 2 amortization tracked as `metadb_leaf_pin_todo`.
///
/// rc-authoritative: each applied LBA does the inline incref(new)+decref(old)
/// pair (`stage_rc_net` / `stage_remap_rc`); net `rc==0` pbas surface in
/// `freed_pbas`. The lock-light buffer path is kept for the common
/// snapshot-free case; when a snapshot is present the per-LBA snap-pin probe
/// needs the tree, so the bucket takes `tree.write()` (matching
/// `apply_l2p_remap`). Phase-5 (flag off): rc-neutral, `freed_pbas` empty.
#[allow(clippy::too_many_arguments)]
pub(in crate::db) fn apply_l2p_remap_range(
    volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
    refcount_shards: &[Shard],
    lsn: Lsn,
    txg: crate::types::Txg,
    vol_ord: VolumeOrdinal,
    start_lba: Lba,
    values: &[L2pValue],
    snap_infos: &[SnapInfo],
    rc_authoritative: bool,
) -> Result<ApplyOutcome> {
    let volume = volumes.get(&vol_ord).ok_or_else(|| {
        MetaDbError::Corruption(format!("L2pRemapRange for unknown volume ord {vol_ord}"))
    })?;

    let n = values.len();
    debug_assert!(n > 0, "L2pRemapRange with empty values reached apply");

    // Bucket LBAs by L2P shard so each shard's tree mutex is taken once
    // for the whole range. Mirrors apply_l2p_range_delete's shape.
    let shard_count = volume.shards.len();
    let mut shard_buckets: Vec<Vec<usize>> = vec![Vec::new(); shard_count];
    for i in 0..n {
        let lba = start_lba + i as u64;
        shard_buckets[shard_for_key_l2p(&volume.shards, lba)].push(i);
    }

    let mut prevs: Vec<Option<L2pValue>> = vec![None; n];
    let mut applied: Vec<bool> = vec![false; n];
    let mut freed_pbas: Vec<Pba> = Vec::new();

    for (l2p_sid, indices) in shard_buckets.iter().enumerate() {
        if indices.is_empty() {
            continue;
        }
        let shard = &volume.shards[l2p_sid];
        // rc-authoritative with a live snapshot → the per-LBA snap-pin probe
        // reads snapshot roots, which needs the shard tree. Take it (even in
        // buffer mode, as `apply_l2p_remap` does). Otherwise the lock-light
        // buffer path stands.
        let need_tree = rc_authoritative && !snap_infos.is_empty();

        if shard.use_buffer && !need_tree {
            // Lock-light buffer path — mirrors `apply_l2p_bucket_buffer`
            // (the grouped/≥8-op path), which deliberately does NOT take
            // `tree.write()`. The mutation lands in `l2p_buffer`
            // (its own per-slot mutex), and fallthrough reads consult the
            // published `read_view`, re-fetched per LBA: the TxgSync
            // compactor publishes the read view *before* clearing the
            // synced slot (publish-before-clear), so a `read_view.read()`
            // snapshot is always consistent and a commit never observes a
            // half-folded tree. Range ops are always unguarded, so there is
            // no guard-read+put atomicity to protect (unlike guarded
            // `apply_l2p_remap`). No snapshot present, so the decref needs no
            // snap-pin probe → `stage_rc_net` directly (tree-free).
            for &i in indices {
                let lba = start_lba + i as u64;
                let new_value = values[i];

                let cur = match shard.l2p_buffer.lookup_for_open_txg(txg, lba) {
                    crate::db::l2p_buffer::BufferLookup::Present(v) => Some(v),
                    crate::db::l2p_buffer::BufferLookup::Tombstone => None,
                    crate::db::l2p_buffer::BufferLookup::Absent => {
                        shard.read_view.read().clone().get(lba)?
                    }
                };

                if seq_guard_rejects(new_value.seq(), cur.as_ref()) {
                    prevs[i] = cur;
                    continue;
                }
                let new_value = stamp_birth_lsn(new_value, lsn);
                shard.l2p_buffer.insert_at_txg(txg, lba, new_value, lsn);
                if rc_authoritative {
                    if let Some(f) =
                        stage_rc_net(refcount_shards, txg, new_value, cur, false, false, lsn)?
                    {
                        freed_pbas.push(f);
                    }
                } else {
                    record_dead(volume, cur, lsn);
                }
                prevs[i] = cur;
                applied[i] = true;
            }
            // No publish: the compactor republishes the read view on its
            // next fold cycle (same contract as apply_l2p_bucket_buffer).
            continue;
        }

        // Tree-locked path: tree mode, OR buffer mode with a snapshot present
        // (rc-authoritative needs the tree for the snap-pin probe). One tree
        // write lock per shard bucket.
        let mut tree = shard.tree.write();
        // A3 cutover: tree-mode COW stages page-rc deltas into this TXG slot.
        tree.set_current_txg(txg);
        // ZFS port Phase 4 Step 4 (S1): birth-authoritative non-clone COW-kill.
        tree.set_snapshot_wms(snapshot_wms_of(snap_infos));
        for &i in indices {
            let lba = start_lba + i as u64;
            let new_value = values[i];

            let cur = if shard.use_buffer {
                match shard.l2p_buffer.lookup_for_open_txg(txg, lba) {
                    crate::db::l2p_buffer::BufferLookup::Present(v) => Some(v),
                    crate::db::l2p_buffer::BufferLookup::Tombstone => None,
                    crate::db::l2p_buffer::BufferLookup::Absent => tree.get(lba)?,
                }
            } else {
                tree.get(lba)?
            };
            if seq_guard_rejects(new_value.seq(), cur.as_ref()) {
                prevs[i] = cur;
                continue;
            }
            let new_value = stamp_birth_lsn(new_value, lsn);
            let prev = if shard.use_buffer {
                shard.l2p_buffer.insert_at_txg(txg, lba, new_value, lsn);
                cur
            } else {
                tree.insert_at_lsn(lba, new_value, lsn)?
            };
            if rc_authoritative {
                if let Some(f) = stage_remap_rc(
                    refcount_shards,
                    txg,
                    &tree,
                    l2p_sid,
                    lba,
                    new_value,
                    prev,
                    snap_infos,
                    lsn,
                )? {
                    freed_pbas.push(f);
                }
            } else {
                record_dead(volume, prev, lsn);
            }
            prevs[i] = prev;
            applied[i] = true;
        }
        if !shard.use_buffer {
            publish_l2p_read_view(shard, &tree);
        }
        // ZFS port Phase 2: drain this bucket's COW page-deaths. Only the
        // tree-locked path COWs (the lock-light buffer path `continue`d
        // above without touching the tree); buffer-mode COW happens in
        // the fold.
        drain_page_deaths(volume, &mut tree, snap_infos);
        drain_live_events(volume, &mut tree);
    }

    Ok(ApplyOutcome::L2pRemapRange {
        applied: applied.into_boxed_slice(),
        prevs: prevs.into_boxed_slice(),
        freed_pbas,
    })
}

/// Scan one volume's L2P over `[start, end)` and return every live
/// `(lba, value)` pair, sorted by lba. Used by `Db::range_delete`
/// (live path) and by the Phase C.4 lifecycle replay for
/// [`crate::lifecycle_log::LifecycleOp::Discard`] — both need the
/// same captured list before calling
/// [`apply_l2p_range_delete`]. Takes a `write` lock on each shard's
/// tree so the caller (which holds `apply_gate.write`) gets a
/// consistent view; the lock is released as soon as the iterator
/// is drained.
pub(in crate::db) fn scan_l2p_range(
    volume: &Volume,
    start: Lba,
    end: Lba,
) -> Result<Vec<(Lba, L2pValue)>> {
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
}

/// Apply one [`LifecycleOp::Discard`]. Walks the `captured` list and
/// deletes each lba from its volume's L2P shard.
///
/// * **Off (Phase 5)** — rc-neutral; a discard does not touch PBA rc.
///   Physical reuse is driven by the onyx-side retired-extent path /
///   lineage GC.
/// * **On (rc-authoritative)** — each deleted live reference does an inline
///   **decref(old)** (no incref — a delete installs nothing), suppressed when
///   a live snapshot still pins it (`any_snap_pins`). A net `rc==0` surfaces
///   in `freed_pbas`. `record_dead` is NOT called (lineage stays dormant).
///   Two captured LBAs sharing a packed base pba are two genuine references →
///   two decrefs (same sign, no net-collapse needed).
///
/// Replay safety: the captured list is authoritative — both live
/// apply and replay consume the same (lba, value) pairs; already-deleted
/// LBAs simply remain absent. `Db::range_delete` uses apply_gate.write
/// (same pattern as `drop_snapshot`) to exclude concurrent commits while
/// plan + submit + apply run, so captured is consistent with the tree state
/// at apply time, and the snapshot list is stable during the probe.
#[allow(clippy::too_many_arguments)]
pub(in crate::db) fn apply_l2p_range_delete(
    volumes: &HashMap<VolumeOrdinal, Arc<Volume>>,
    refcount_shards: &[Shard],
    lsn: Lsn,
    txg: crate::types::Txg,
    vol_ord: VolumeOrdinal,
    captured: &[(Lba, L2pValue)],
    snap_infos: &[SnapInfo],
    rc_authoritative: bool,
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

    let mut freed_pbas: Vec<Pba> = Vec::new();
    for (sid, indices) in shard_buckets.iter().enumerate() {
        if indices.is_empty() {
            continue;
        }
        let mut tree = volume.shards[sid].tree.write();
        // A3 cutover: tree-mode COW (delete) stages page-rc deltas here.
        tree.set_current_txg(txg);
        // ZFS port Phase 4 Step 4 (S1): birth-authoritative non-clone COW-kill.
        // This is also the Discard REPLAY COW site; `snap_infos` is rebuilt from
        // the in-memory manifest at the replay point, so youngest is consistent.
        tree.set_snapshot_wms(snapshot_wms_of(snap_infos));
        for &idx in indices {
            let (lba, _) = captured[idx];
            // The snap-pin probe inside `stage_delete_rc` reads the snapshot
            // root (untouched by the live delete above); `tree` is still `&`.
            let prev = tree.delete_at_lsn(lba, lsn)?;
            if rc_authoritative {
                if let Some(f) =
                    stage_delete_rc(refcount_shards, txg, &tree, sid, lba, prev, snap_infos, lsn)?
                {
                    freed_pbas.push(f);
                }
            }
        }
        publish_l2p_read_view(&volume.shards[sid], &tree);
        // ZFS port Phase 2: a range delete COWs the path to each deleted
        // leaf; record any page displaced off the head.
        drain_page_deaths(volume, &mut tree, snap_infos);
        drain_live_events(volume, &mut tree);
    }

    Ok(ApplyOutcome::RangeDelete { freed_pbas })
}
