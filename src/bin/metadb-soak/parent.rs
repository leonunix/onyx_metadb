fn run_parent(cfg: ParentConfig) -> Result<ExitCode, String> {
    std::fs::create_dir_all(&cfg.path).map_err(|e| e.to_string())?;
    let mut events = EventLog::open(&cfg.events_path).map_err(|e| e.to_string())?;
    events.write(
        "start",
        &format!(
            "seed={} threads={} pipeline_depth={} cleanup_batch_size={}",
            cfg.seed, cfg.threads, cfg.pipeline_depth, cfg.cleanup_batch_size
        ),
    )?;

    let started = Instant::now();
    let mut model = Model::default();
    let mut onyx_model = OnyxRefModel::default();
    let mut rngs: Vec<ChaCha8Rng> = (0..cfg.threads)
        .map(|tid| ChaCha8Rng::seed_from_u64(cfg.seed ^ ((tid as u64 + 1) << 20)))
        .collect();
    let mut cycle_rng = ChaCha8Rng::seed_from_u64(cfg.seed ^ 0xA11C_E001);
    let mut snapshots: Vec<ModelSnapshot> = Vec::new();
    let mut total_ops = 0u64;
    let mut cycles = 0u64;
    let mut restarts = 0u64;
    let mut verifies = 0u64;
    let mut fault_cycles = 0u64;
    let mut deadlock_detected = false;
    let mut last_error = None;
    let mut onyx_stats = OnyxStats::default();
    let mut child = None;
    let mut child_fault = None;
    let mut last_restart = Instant::now();

    while started.elapsed() < Duration::from_secs(cfg.duration_secs) {
        cycles += 1;
        if child.is_none() {
            child_fault = choose_fault(&mut cycle_rng, cfg.fault_density_pct);
            if child_fault.is_some() {
                fault_cycles += 1;
            }
            events.write(
                "child_spawn",
                &format!("cycle={} fault={}", cycles, fault_label(child_fault)),
            )?;
            child = Some(spawn_child(&cfg, child_fault).map_err(|e| e.to_string())?);
            last_restart = Instant::now();
        }
        events.write(
            "cycle_start",
            &format!("cycle={} fault={}", cycles, fault_label(child_fault)),
        )?;
        let running_child = child.as_mut().expect("child spawned before cycle");

        match run_cycle(
            &cfg,
            cycles,
            running_child,
            &mut model,
            &mut onyx_model,
            &mut rngs,
            &mut cycle_rng,
            &mut snapshots,
            &mut total_ops,
            &mut onyx_stats,
            &mut events,
        ) {
            Ok(()) => {}
            Err(err) => {
                last_error = Some(err);
                if let Some(mut running_child) = child.take() {
                    let _ = kill_child(&mut running_child);
                }
                break;
            }
        }

        let should_restart = cfg.restart_interval_secs.is_none_or(|secs| {
            last_restart.elapsed() >= Duration::from_secs(secs)
                || started.elapsed() >= Duration::from_secs(cfg.duration_secs)
        });
        if !should_restart {
            events.write("child_kept", &format!("cycle={cycles}"))?;
            continue;
        }

        let mut running_child = child.take().expect("child exists when restart is due");
        if let Err(err) = kill_and_verify_child(
            &cfg,
            &mut running_child,
            cycles,
            &model,
            &onyx_model,
            &snapshots,
            &mut restarts,
            &mut verifies,
            &mut events,
        ) {
            if err.contains("deadlock") {
                deadlock_detected = true;
            }
            last_error = Some(err);
            break;
        }
        child_fault = None;
    }

    if last_error.is_none() {
        if let Some(mut running_child) = child.take() {
            if let Err(err) = kill_and_verify_child(
                &cfg,
                &mut running_child,
                cycles,
                &model,
                &onyx_model,
                &snapshots,
                &mut restarts,
                &mut verifies,
                &mut events,
            ) {
                if err.contains("deadlock") {
                    deadlock_detected = true;
                }
                last_error = Some(err);
            }
        }
    }

    let summary = Summary {
        path: cfg.path.clone(),
        duration_secs: started.elapsed().as_secs(),
        cycles,
        ops: total_ops,
        restarts,
        verifies,
        fault_cycles,
        onyx_ops: onyx_stats.onyx_ops,
        guard_hit: onyx_stats.guard_hit,
        guard_miss: onyx_stats.guard_miss,
        freed_pbas: onyx_stats.freed_pbas,
        cleanup_deleted: onyx_stats.cleanup_deleted,
        refcount_sum_mismatches: onyx_stats.refcount_sum_mismatches,
        deadlock_detected,
        success: last_error.is_none(),
        last_error,
    };
    write_summary(&cfg.summary_path, &summary).map_err(|e| e.to_string())?;
    events.write("finish", &format!("success={}", summary.success))?;

    Ok(if summary.success {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    })
}

#[allow(clippy::too_many_arguments)]
fn kill_and_verify_child(
    cfg: &ParentConfig,
    child: &mut ChildHandle,
    cycle: u64,
    model: &Model,
    onyx_model: &OnyxRefModel,
    snapshots: &[ModelSnapshot],
    restarts: &mut u64,
    verifies: &mut u64,
    events: &mut EventLog,
) -> Result<(), String> {
    if let Err(err) = kill_child(child) {
        return Err(err.to_string());
    }
    *restarts += 1;
    events.write("child_killed", &format!("cycle={cycle}"))?;

    match verify_reopened_db(
        &cfg.path,
        model,
        onyx_model,
        snapshots,
        cfg.threads,
        cfg.workload,
    ) {
        Ok(report) => {
            if !report.is_clean() {
                return Err(format!("metadb-verify failed: {:?}", report.issues));
            }
            *verifies += 1;
            events.write(
                "verify_ok",
                &format!(
                    "cycle={} live_pages={} free_pages={}",
                    cycle, report.live_pages, report.free_pages
                ),
            )?;
            Ok(())
        }
        Err(err) => Err(err.to_string()),
    }
}

#[allow(clippy::too_many_arguments)]
fn run_cycle(
    cfg: &ParentConfig,
    cycle: u64,
    child: &mut ChildHandle,
    model: &mut Model,
    onyx_model: &mut OnyxRefModel,
    rngs: &mut [ChaCha8Rng],
    admin_rng: &mut ChaCha8Rng,
    snapshots: &mut Vec<ModelSnapshot>,
    total_ops: &mut u64,
    onyx_stats: &mut OnyxStats,
    events: &mut EventLog,
) -> Result<(), String> {
    let mut next_id = 1u64;
    let cycle_start_refcount_sum = if cfg.workload == Workload::Onyx {
        match Db::open(&cfg.path) {
            Ok(db) => Some(db_refcount_sum(&db).map_err(|e| e.to_string())?),
            Err(_) => Some(0),
        }
    } else {
        None
    };
    let cycle_start_recorded_sum = onyx_stats.refcount_sum;

    // Phase 1: pre-worker volume admin (create / drop / clone).
    if cfg.workload == Workload::Legacy {
        next_id = run_volume_admin(child, cycle, model, admin_rng, snapshots, events, next_id)?;
    }

    // Phase 2: workers pounding across the live volume set.
    let mut sent_ops = 0usize;
    let effective_pipeline_depth = match cfg.workload {
        // Onyx-mix is reference-model checked. Keep exactly one op in
        // flight so the parent applies the model in the same order the
        // child commits WAL records. Legacy and concurrent modes can
        // pipeline per-worker jobs to keep the child saturated.
        Workload::Onyx => 1,
        Workload::Legacy | Workload::OnyxConcurrent => cfg.pipeline_depth,
    };
    let mut free_tids: VecDeque<usize> = (0..cfg.threads)
        .flat_map(|tid| (0..effective_pipeline_depth).map(move |_| tid))
        .collect();
    let mut inflight: HashMap<u64, WorkerOp> = HashMap::new();

    let max_inflight = match cfg.workload {
        Workload::Legacy | Workload::OnyxConcurrent => cfg.threads * effective_pipeline_depth,
        Workload::Onyx => 1,
    };

    while sent_ops < cfg.ops_per_cycle || !inflight.is_empty() {
        while sent_ops < cfg.ops_per_cycle && !free_tids.is_empty() && inflight.len() < max_inflight
        {
            let tid = free_tids.pop_front().unwrap();
            let op = match cfg.workload {
                Workload::Legacy => generate_worker_op(tid, &mut rngs[tid], model),
                Workload::Onyx | Workload::OnyxConcurrent => {
                    generate_onyx_worker_op(tid, &mut rngs[tid], onyx_model)
                }
            };
            send_worker_op(child, next_id, &op)?;
            inflight.insert(next_id, op);
            next_id += 1;
            sent_ops += 1;
        }

        let ack = recv_ack(child)?;
        match ack {
            Ack::Ok(id) => {
                let op = inflight
                    .remove(&id)
                    .ok_or_else(|| format!("unknown ack id {id}"))?;
                match cfg.workload {
                    Workload::Legacy => apply_worker_op(model, &op)?,
                    Workload::Onyx | Workload::OnyxConcurrent => {
                        apply_onyx_ack(onyx_model, onyx_stats, &op, None)?
                    }
                }
                *total_ops += 1;
                free_tids.push_back(op.tid);
            }
            Ack::Onyx(id, detail) => {
                let op = inflight
                    .remove(&id)
                    .ok_or_else(|| format!("unknown onyx ack id {id}"))?;
                apply_onyx_ack(onyx_model, onyx_stats, &op, Some(&detail))?;
                if cfg.event_verbosity == EventVerbosity::Ops {
                    events.write(
                        "onyx_ack",
                        &format!(
                            "cycle={cycle} id={id} kind={} vol={} slot={} detail={}",
                            worker_kind_name(&op.kind),
                            op.vol_ord,
                            op.slot,
                            escape_json(&detail)
                        ),
                    )?;
                }
                *total_ops += 1;
                free_tids.push_back(op.tid);
            }
            Ack::Error(id, err) => {
                let op = inflight
                    .remove(&id)
                    .ok_or_else(|| format!("unknown error id {id}"))?;
                events.write(
                    "worker_error",
                    &format!(
                        "cycle={} tid={} vol={} err={}",
                        cycle,
                        op.tid,
                        op.vol_ord,
                        escape_json(&err)
                    ),
                )?;
                free_tids.push_back(op.tid);
            }
            Ack::Snapshot(id, _) => {
                return Err(format!("unexpected snapshot ack {id} in worker phase"));
            }
            Ack::Volume(id, _) => {
                return Err(format!("unexpected volume ack {id} in worker phase"));
            }
        }
    }

    if matches!(cfg.workload, Workload::Onyx | Workload::OnyxConcurrent) {
        send_admin(child, next_id, "REFCOUNT_SUM")?;
        let got = match recv_ack(child)? {
            Ack::Onyx(id, detail) if id == next_id => parse_sum(&detail)?,
            other => return Err(format!("unexpected refcount-sum ack: {other:?}")),
        };
        next_id += 1;
        let expected = cycle_start_refcount_sum.unwrap_or(0) as i64
            + (onyx_stats.refcount_sum - cycle_start_recorded_sum);
        if cfg.workload == Workload::Onyx && got as i64 != expected {
            onyx_stats.refcount_sum_mismatches += 1;
            return Err(format!(
                "onyx refcount sum mismatch: db={got} model={}",
                expected
            ));
        }
        onyx_stats.refcount_sum = got as i64;

        send_admin(child, next_id, "AUDIT_PBA_REFCOUNTS")?;
        match recv_ack(child)? {
            Ack::Ok(id) if id == next_id => {}
            Ack::Error(id, err) if id == next_id => return Err(err),
            other => return Err(format!("unexpected pba-audit ack: {other:?}")),
        }
        next_id += 1;
    }

    if matches!(cfg.workload, Workload::Onyx | Workload::OnyxConcurrent) {
        events.write(
            "onyx_cycle",
            &format!(
                "cycle={} ops={} guard_hit={} guard_miss={} freed_pbas={} cleanup_deleted={}",
                cycle,
                sent_ops,
                onyx_stats.guard_hit,
                onyx_stats.guard_miss,
                onyx_stats.freed_pbas,
                onyx_stats.cleanup_deleted,
            ),
        )?;
    }

    // Phase 3: flush + optional snapshot churn.
    send_admin(child, next_id, "FLUSH")?;
    match recv_ack(child)? {
        Ack::Ok(id) if id == next_id => {
            events.write("flush_ok", &format!("cycle={cycle}"))?;
        }
        Ack::Error(id, err) if id == next_id => {
            events.write(
                "flush_err",
                &format!("cycle={} err={}", cycle, escape_json(&err)),
            )?;
        }
        other => return Err(format!("unexpected flush ack: {other:?}")),
    }
    next_id += 1;

    if !cfg.snapshots_enabled {
        return Ok(());
    }

    let live = match cfg.workload {
        Workload::Legacy => model.live_volumes(),
        Workload::Onyx | Workload::OnyxConcurrent => onyx_model.live_volumes(),
    };
    let snap_vol = live[admin_rng.gen_range(0..live.len())];
    let snapshot_model = match cfg.workload {
        Workload::Legacy => model.l2p.get(&snap_vol).cloned().unwrap_or_default(),
        Workload::Onyx | Workload::OnyxConcurrent => onyx_model.volume_l2p(snap_vol),
    };
    send_admin(child, next_id, &format!("SNAPSHOT {snap_vol}"))?;
    match recv_ack(child)? {
        Ack::Snapshot(id, snapshot_id) if id == next_id => {
            snapshots.push(ModelSnapshot {
                id: snapshot_id,
                vol_ord: snap_vol,
                l2p: snapshot_model,
            });
            if cfg.workload == Workload::Onyx {
                onyx_model.take_snapshot_with_id(snap_vol, snapshot_id);
            }
            events.write(
                "snapshot_ok",
                &format!("cycle={} vol={} snapshot={}", cycle, snap_vol, snapshot_id),
            )?;
        }
        Ack::Error(id, err) if id == next_id => {
            events.write(
                "snapshot_err",
                &format!("cycle={} vol={} err={}", cycle, snap_vol, escape_json(&err)),
            )?;
        }
        other => return Err(format!("unexpected snapshot ack: {other:?}")),
    }
    next_id += 1;

    if snapshots.len() > 4 {
        let entry = snapshots.remove(0);
        let expected_freed = if cfg.workload == Workload::Onyx {
            onyx_model.drop_snapshot(entry.id)
        } else {
            Vec::new()
        };
        send_admin(child, next_id, &format!("DROP {}", entry.id))?;
        match recv_ack(child)? {
            Ack::Ok(id) if id == next_id => {
                if cfg.workload == Workload::Onyx && !expected_freed.is_empty() {
                    send_admin(
                        child,
                        next_id + 10_000_000,
                        &format!("CLEANUP {}", join_pbas(&expected_freed)),
                    )?;
                    match recv_ack(child)? {
                        Ack::Ok(_) => {
                            onyx_model.cleanup_dedup_for_dead_pbas(&expected_freed);
                        }
                        other => return Err(format!("unexpected cleanup ack: {other:?}")),
                    }
                }
                events.write(
                    "drop_snapshot_ok",
                    &format!(
                        "cycle={} vol={} snapshot={}",
                        cycle, entry.vol_ord, entry.id
                    ),
                )?;
            }
            Ack::Error(id, err) if id == next_id => {
                events.write(
                    "drop_snapshot_err",
                    &format!(
                        "cycle={} vol={} snapshot={} err={}",
                        cycle,
                        entry.vol_ord,
                        entry.id,
                        escape_json(&err)
                    ),
                )?;
            }
            other => return Err(format!("unexpected drop-snapshot ack: {other:?}")),
        }
    }

    Ok(())
}

/// Opportunistic create / drop / clone_volume run at the head of each
/// cycle. Each branch fires with probability ~1/3 so cycles typically
/// mutate the volume set exactly once and occasionally more.
fn run_volume_admin(
    child: &mut ChildHandle,
    cycle: u64,
    model: &mut Model,
    rng: &mut ChaCha8Rng,
    snapshots: &[ModelSnapshot],
    events: &mut EventLog,
    mut next_id: u64,
) -> Result<u64, String> {
    // Snapshots pin their source volume — we can't drop a vol if we
    // still hold a snapshot entry over it.
    let pinned: BTreeSet<VolumeOrdinal> = snapshots.iter().map(|s| s.vol_ord).collect();

    // CREATE: cap total live volumes so ords don't explode.
    if model.l2p.len() < MAX_LIVE_VOLUMES && rng.gen_bool(0.5) {
        send_admin(child, next_id, "CREATE_VOLUME")?;
        match recv_ack(child)? {
            Ack::Volume(id, ord) if id == next_id => {
                model.l2p.insert(ord, BTreeMap::new());
                events.write("create_volume_ok", &format!("cycle={cycle} ord={ord}"))?;
            }
            Ack::Error(id, err) if id == next_id => {
                events.write(
                    "create_volume_err",
                    &format!("cycle={} err={}", cycle, escape_json(&err)),
                )?;
            }
            other => return Err(format!("unexpected create-volume ack: {other:?}")),
        }
        next_id += 1;
    }

    // CLONE: roll a snapshot if we have any.
    if model.l2p.len() < MAX_LIVE_VOLUMES && !snapshots.is_empty() && rng.gen_bool(0.4) {
        let pick = rng.gen_range(0..snapshots.len());
        let src = snapshots[pick].clone();
        send_admin(child, next_id, &format!("CLONE_VOLUME {}", src.id))?;
        match recv_ack(child)? {
            Ack::Volume(id, ord) if id == next_id => {
                model.l2p.insert(ord, src.l2p.clone());
                events.write(
                    "clone_volume_ok",
                    &format!("cycle={} src_snap={} ord={}", cycle, src.id, ord),
                )?;
            }
            Ack::Error(id, err) if id == next_id => {
                events.write(
                    "clone_volume_err",
                    &format!(
                        "cycle={} src_snap={} err={}",
                        cycle,
                        src.id,
                        escape_json(&err)
                    ),
                )?;
            }
            other => return Err(format!("unexpected clone-volume ack: {other:?}")),
        }
        next_id += 1;
    }

    // DROP: only non-bootstrap, non-pinned volumes.
    let candidates = model.drop_candidates(&pinned);
    if !candidates.is_empty() && rng.gen_bool(0.35) {
        let pick = candidates[rng.gen_range(0..candidates.len())];
        send_admin(child, next_id, &format!("DROP_VOLUME {pick}"))?;
        match recv_ack(child)? {
            Ack::Ok(id) if id == next_id => {
                model.l2p.remove(&pick);
                events.write("drop_volume_ok", &format!("cycle={} ord={}", cycle, pick))?;
            }
            Ack::Error(id, err) if id == next_id => {
                events.write(
                    "drop_volume_err",
                    &format!("cycle={} ord={} err={}", cycle, pick, escape_json(&err)),
                )?;
            }
            other => return Err(format!("unexpected drop-volume ack: {other:?}")),
        }
        next_id += 1;
    }

    Ok(next_id)
}
