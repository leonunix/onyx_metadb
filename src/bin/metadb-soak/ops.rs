fn generate_worker_op(tid: usize, rng: &mut ChaCha8Rng, model: &Model) -> WorkerOp {
    let slot = rng.gen_range(0..KEY_SLOTS_PER_THREAD);
    let pba = refcount_pba(tid, slot);
    let live = model.live_volumes();
    let vol_ord = live[rng.gen_range(0..live.len())];
    let kind = match rng.gen_range(0..100) {
        0..=29 => WorkerOpKind::Insert(rng.r#gen()),
        30..=39 => WorkerOpKind::Delete,
        40..=59 => WorkerOpKind::PutDedup(rng.r#gen()),
        60..=69 => WorkerOpKind::DeleteDedup,
        70..=84 => WorkerOpKind::Incref,
        85..=94 if model.refcount.get(&pba).copied().unwrap_or(0) > 0 => WorkerOpKind::Decref,
        85..=94 => WorkerOpKind::Get,
        _ => WorkerOpKind::Get,
    };
    WorkerOp {
        tid,
        vol_ord,
        slot,
        kind,
    }
}

fn generate_onyx_worker_op(tid: usize, rng: &mut ChaCha8Rng, model: &OnyxRefModel) -> WorkerOp {
    let slot = rng.gen_range(0..ONYX_MAX_LBA);
    let pba = rng.gen_range(1..=ONYX_MAX_PBA);
    let live = model.live_volumes();
    let vol_ord = live[rng.gen_range(0..live.len())];
    let salt = ((tid as u64) << 48) | (slot << 16) | rng.gen_range(0..=u16::MAX) as u64;
    let kind = match rng.gen_range(0..100) {
        0..=39 => WorkerOpKind::OnyxRemap {
            pba,
            salt,
            guard: 0,
        },
        40..=59 => WorkerOpKind::OnyxRemap {
            pba,
            salt,
            guard: 1,
        },
        60..=64 => WorkerOpKind::OnyxRangeDelete {
            len: rng.gen_range(1..32),
        },
        65..=94 => WorkerOpKind::OnyxDedupHit { pba, salt },
        _ => {
            let pending = model.pending_dead_pbas();
            let pba = pending.first().copied().unwrap_or(pba);
            WorkerOpKind::OnyxCleanup { pba }
        }
    };
    WorkerOp {
        tid,
        vol_ord,
        slot,
        kind,
    }
}

fn apply_worker_op(model: &mut Model, op: &WorkerOp) -> Result<(), String> {
    match op.kind {
        WorkerOpKind::Insert(byte) => {
            if let Some(vol) = model.l2p.get_mut(&op.vol_ord) {
                vol.insert(l2p_key(op.tid, op.slot), l2p_value(byte));
            }
        }
        WorkerOpKind::Delete => {
            if let Some(vol) = model.l2p.get_mut(&op.vol_ord) {
                vol.remove(&l2p_key(op.tid, op.slot));
            }
        }
        WorkerOpKind::PutDedup(byte) => {
            model
                .dedup
                .insert(dedup_hash(op.tid, op.slot), dedup_value(byte));
        }
        WorkerOpKind::DeleteDedup => {
            model.dedup.remove(&dedup_hash(op.tid, op.slot));
        }
        WorkerOpKind::Incref => {
            *model
                .refcount
                .entry(refcount_pba(op.tid, op.slot))
                .or_insert(0) += 1;
        }
        WorkerOpKind::Decref => {
            let pba = refcount_pba(op.tid, op.slot);
            if let Some(value) = model.refcount.get_mut(&pba) {
                if *value == 0 {
                    return Err(format!("model underflow on pba {pba}"));
                }
                *value -= 1;
                if *value == 0 {
                    model.refcount.remove(&pba);
                }
            }
        }
        WorkerOpKind::Get => {}
        WorkerOpKind::OnyxRemap { .. }
        | WorkerOpKind::OnyxRangeDelete { .. }
        | WorkerOpKind::OnyxDedupHit { .. }
        | WorkerOpKind::OnyxCleanup { .. } => {}
    }
    Ok(())
}

fn apply_onyx_ack(
    model: &mut OnyxRefModel,
    stats: &mut OnyxStats,
    op: &WorkerOp,
    detail: Option<&str>,
) -> Result<(), String> {
    if let Some(detail) = detail {
        if let Some(delta) = parse_delta(detail)? {
            stats.refcount_sum += delta;
        }
    }
    match op.kind {
        WorkerOpKind::OnyxRemap { pba, salt, guard } => {
            let outcome = if detail.is_some_and(|value| value.starts_with("rejected")) {
                onyx_metadb::testing::onyx_model::ModelRemapOutcome {
                    applied: false,
                    prev: None,
                    freed_pba: None,
                    invalid: false,
                }
            } else {
                model.apply_l2p_remap(op.vol_ord, op.slot, onyx_l2p_value(pba, salt), None)
            };
            if outcome.invalid {
                return Err("onyx remap targeted invalid volume".into());
            }
            if guard == 1 {
                if outcome.applied {
                    stats.guard_hit += 1;
                } else {
                    stats.guard_miss += 1;
                }
            }
            if let Some(freed) = outcome.freed_pba {
                stats.freed_pbas += 1;
                stats.cleanup_deleted += model.cleanup_dedup_for_dead_pbas(&[freed]) as u64;
            }
            stats.onyx_ops += 1;
        }
        WorkerOpKind::OnyxRangeDelete { len } => {
            let freed = model.apply_range_delete(op.vol_ord, op.slot, op.slot.saturating_add(len));
            stats.freed_pbas += freed.len() as u64;
            if !freed.is_empty() {
                stats.cleanup_deleted += model.cleanup_dedup_for_dead_pbas(&freed) as u64;
            }
            stats.onyx_ops += 1;
        }
        WorkerOpKind::OnyxDedupHit { pba, salt } => {
            let outcome = if detail.is_some_and(|value| value.starts_with("rejected")) {
                onyx_metadb::testing::onyx_model::ModelRemapOutcome {
                    applied: false,
                    prev: None,
                    freed_pba: None,
                    invalid: false,
                }
            } else {
                model.apply_l2p_remap(op.vol_ord, op.slot, onyx_l2p_value(pba, salt), None)
            };
            if outcome.applied {
                stats.guard_hit += 1;
            } else {
                stats.guard_miss += 1;
            }
            model.put_dedup_raw(onyx_hash(salt), onyx_dedup_value(pba, salt));
            stats.onyx_ops += 1;
        }
        WorkerOpKind::OnyxCleanup { pba } => {
            stats.cleanup_deleted += model.cleanup_dedup_for_dead_pbas(&[pba]) as u64;
            stats.onyx_ops += 1;
        }
        _ => return Err(format!("non-onyx op in onyx model: {:?}", op.kind)),
    }
    Ok(())
}

fn verify_reopened_db(
    path: &Path,
    model: &Model,
    onyx_model: &OnyxRefModel,
    snapshots: &[ModelSnapshot],
    threads: usize,
    workload: Workload,
) -> onyx_metadb::Result<onyx_metadb::VerifyReport> {
    let db = Db::open(path)?;
    match workload {
        Workload::Legacy => verify_live_db(&db, model, threads)?,
        Workload::Onyx | Workload::OnyxConcurrent => {
            let _ = onyx_model;
        }
    }
    for snap in snapshots {
        if workload == Workload::Legacy {
            verify_snapshot(&db, snap.id, &snap.l2p)?;
        }
    }
    drop(db);
    verify_path(path, VerifyOptions { strict: true })
}

fn join_pbas(pbas: &[Pba]) -> String {
    pbas.iter()
        .map(|pba| pba.to_string())
        .collect::<Vec<_>>()
        .join(" ")
}

fn verify_live_db(db: &Db, model: &Model, threads: usize) -> onyx_metadb::Result<()> {
    // Volume set match: db.volumes() vs model.l2p keys.
    let got_vols = db.volumes();
    let want_vols: Vec<VolumeOrdinal> = model.l2p.keys().copied().collect();
    if got_vols != want_vols {
        return Err(onyx_metadb::MetaDbError::Corruption(format!(
            "volume set diverged: got={got_vols:?} want={want_vols:?}"
        )));
    }

    for (vol_ord, vol_model) in &model.l2p {
        let got_l2p: Vec<(u64, L2pValue)> = db
            .range(*vol_ord, ..)?
            .collect::<onyx_metadb::Result<Vec<_>>>()?;
        let want_l2p: Vec<(u64, L2pValue)> = vol_model.iter().map(|(k, v)| (*k, *v)).collect();
        if got_l2p != want_l2p {
            return Err(onyx_metadb::MetaDbError::Corruption(format!(
                "L2P model diverged for vol {vol_ord} (got {} entries, want {} entries)",
                got_l2p.len(),
                want_l2p.len(),
            )));
        }
    }

    for tid in 0..threads {
        for slot in 0..KEY_SLOTS_PER_THREAD {
            let hash = dedup_hash(tid, slot);
            let got = db.get_dedup(&hash)?;
            let want = model.dedup.get(&hash).copied();
            if got != want {
                return Err(onyx_metadb::MetaDbError::Corruption(format!(
                    "dedup model diverged for tid={tid} slot={slot}: got={got:?} want={want:?}"
                )));
            }

            let pba = refcount_pba(tid, slot);
            let got = db.get_refcount(pba)?;
            let want = model.refcount.get(&pba).copied().unwrap_or(0);
            if got != want {
                return Err(onyx_metadb::MetaDbError::Corruption(format!(
                    "refcount model diverged for tid={tid} slot={slot}: got={got} want={want}"
                )));
            }
        }
    }

    Ok(())
}

fn verify_snapshot(
    db: &Db,
    snapshot_id: SnapshotId,
    expected: &BTreeMap<u64, L2pValue>,
) -> onyx_metadb::Result<()> {
    let Some(view) = db.snapshot_view(snapshot_id) else {
        return Err(onyx_metadb::MetaDbError::Corruption(format!(
            "snapshot {snapshot_id} vanished during soak"
        )));
    };
    let got: Vec<(u64, L2pValue)> = view.range(..)?.collect::<onyx_metadb::Result<Vec<_>>>()?;
    let want: Vec<(u64, L2pValue)> = expected.iter().map(|(k, v)| (*k, *v)).collect();
    if got != want {
        return Err(onyx_metadb::MetaDbError::Corruption(format!(
            "snapshot {snapshot_id} diverged from captured model"
        )));
    }
    Ok(())
}

fn spawn_deadlock_monitor(
    stop: Arc<AtomicBool>,
    deadlock_seen: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while !stop.load(Ordering::Acquire) {
            let deadlocks = parking_lot::deadlock::check_deadlock();
            if !deadlocks.is_empty() {
                deadlock_seen.store(true, Ordering::Release);
                return;
            }
            thread::sleep(Duration::from_secs(2));
        }
    })
}

struct ChildHandle {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
}
