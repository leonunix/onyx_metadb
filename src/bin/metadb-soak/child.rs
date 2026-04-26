fn run_child(cfg: ChildConfig) -> Result<ExitCode, String> {
    let faults = FaultController::new();
    if let Some(fault) = cfg.fault {
        faults.install(fault.point, fault.hit, fault.action);
    }
    let db = Arc::new(open_or_create_with_faults(&cfg.path, faults).map_err(|e| e.to_string())?);

    let deadlock_seen = Arc::new(AtomicBool::new(false));
    let deadlock_stop = Arc::new(AtomicBool::new(false));
    let monitor = spawn_deadlock_monitor(deadlock_stop.clone(), deadlock_seen.clone());
    let metrics_stop = Arc::new(AtomicBool::new(false));
    let metrics_reporter = cfg.metrics_path.as_ref().and_then(|path| {
        (cfg.metrics_interval_secs > 0).then(|| {
            spawn_metrics_reporter(
                db.clone(),
                path.clone(),
                Duration::from_secs(cfg.metrics_interval_secs),
                metrics_stop.clone(),
            )
        })
    });
    let cleanup_batcher = Arc::new(CleanupBatcher::new(cfg.cleanup_batch_size));

    let (ack_tx, ack_rx) = crossbeam_channel::unbounded::<Ack>();
    let admin_ack_tx = ack_tx.clone();
    let mut worker_txs = Vec::with_capacity(cfg.threads);
    let mut worker_handles = Vec::with_capacity(cfg.threads);
    for tid in 0..cfg.threads {
        let (job_tx, job_rx) = crossbeam_channel::unbounded::<WorkerJob>();
        worker_txs.push(job_tx);
        let db = db.clone();
        let ack_tx = ack_tx.clone();
        let workload = cfg.workload;
        let cleanup_batcher = cleanup_batcher.clone();
        worker_handles.push(thread::spawn(move || {
            worker_main(tid, db, workload, cleanup_batcher, job_rx, ack_tx)
        }));
    }
    drop(ack_tx);

    let writer = thread::spawn(move || writer_main(ack_rx));
    let stdin = std::io::stdin();
    let mut reader = BufReader::new(stdin.lock());
    let mut line = String::new();
    while !deadlock_seen.load(Ordering::Acquire) {
        line.clear();
        let read = reader.read_line(&mut line).map_err(|e| e.to_string())?;
        if read == 0 {
            break;
        }
        let cmd = line.trim_end();
        if cmd.is_empty() {
            continue;
        }
        let mut parts = cmd.split_whitespace();
        match parts.next() {
            Some("W") => {
                let id = parse_part_u64(parts.next(), "worker id")?;
                let op = parse_worker_op(parts)?;
                worker_txs[op.tid]
                    .send(WorkerJob::Exec { id, op })
                    .map_err(|e| e.to_string())?;
            }
            Some("A") => {
                let id = parse_part_u64(parts.next(), "admin id")?;
                let verb = parts
                    .next()
                    .ok_or_else(|| "missing admin verb".to_string())?;
                if verb != "METRICS" {
                    if let Err(err) = cleanup_batcher.flush(&db) {
                        admin_ack_tx
                            .send(Ack::Error(id, err.to_string()))
                            .map_err(|e| e.to_string())?;
                        continue;
                    }
                }
                let ack = match verb {
                    "FLUSH" => db.flush().map(|_| Ack::Ok(id)),
                    "REFCOUNT_SUM" => {
                        db_refcount_sum(&db).map(|sum| Ack::Onyx(id, format!("sum={sum}")))
                    }
                    "METRICS" => Ok(Ack::Onyx(id, db.metrics_json())),
                    "AUDIT_PBA_REFCOUNTS" => audit_pba_refcounts(&db).map(|_| Ack::Ok(id)),
                    "CLEANUP" => {
                        let pbas = parts
                            .map(|part| part.parse::<Pba>().map_err(|e| e.to_string()))
                            .collect::<Result<Vec<_>, _>>()?;
                        db.cleanup_dedup_for_dead_pbas(&pbas).map(|_| Ack::Ok(id))
                    }
                    "SNAPSHOT" => {
                        let vol_ord =
                            parse_part_u64(parts.next(), "snapshot vol_ord")? as VolumeOrdinal;
                        db.take_snapshot(vol_ord)
                            .map(|snapshot| Ack::Snapshot(id, snapshot))
                    }
                    "DROP" => {
                        let snap_id = parse_part_u64(parts.next(), "snapshot id")?;
                        db.drop_snapshot(snap_id).map(|_| Ack::Ok(id))
                    }
                    "CREATE_VOLUME" => db.create_volume().map(|ord| Ack::Volume(id, ord)),
                    "DROP_VOLUME" => {
                        let vol_ord =
                            parse_part_u64(parts.next(), "drop vol_ord")? as VolumeOrdinal;
                        db.drop_volume(vol_ord).map(|_| Ack::Ok(id))
                    }
                    "CLONE_VOLUME" => {
                        let src_snap_id = parse_part_u64(parts.next(), "clone src_snap_id")?;
                        db.clone_volume(src_snap_id).map(|ord| Ack::Volume(id, ord))
                    }
                    "QUIT" => {
                        for tx in &worker_txs {
                            let _ = tx.send(WorkerJob::Stop);
                        }
                        break;
                    }
                    other => return Err(format!("unknown admin verb `{other}`")),
                };
                match ack {
                    Ok(ack) => admin_ack_tx.send(ack).map_err(|e| e.to_string())?,
                    Err(err) => admin_ack_tx
                        .send(Ack::Error(id, err.to_string()))
                        .map_err(|e| e.to_string())?,
                }
            }
            Some(other) => return Err(format!("unknown command kind `{other}`")),
            None => {}
        }
    }

    deadlock_stop.store(true, Ordering::Release);
    metrics_stop.store(true, Ordering::Release);
    let _ = monitor.join();
    if let Some(handle) = metrics_reporter {
        let _ = handle.join();
    }
    if deadlock_seen.load(Ordering::Acquire) {
        return Err("deadlock detected in child monitor".into());
    }
    for tx in &worker_txs {
        let _ = tx.send(WorkerJob::Stop);
    }
    for handle in worker_handles {
        let _ = handle.join();
    }
    drop(admin_ack_tx);
    let _ = writer.join();
    Ok(ExitCode::SUCCESS)
}

fn spawn_metrics_reporter(
    db: Arc<Db>,
    path: PathBuf,
    interval: Duration,
    stop: Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path);
        let Ok(file) = file else {
            return;
        };
        let mut writer = BufWriter::new(file);
        while !stop.load(Ordering::Acquire) {
            let ts_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis())
                .unwrap_or(0);
            let _ = writeln!(
                writer,
                "{{\"ts_ms\":{},\"metrics\":{}}}",
                ts_ms,
                db.metrics_json()
            );
            let _ = writer.flush();

            let mut slept = Duration::ZERO;
            while slept < interval && !stop.load(Ordering::Acquire) {
                let step = (interval - slept).min(Duration::from_millis(200));
                thread::sleep(step);
                slept += step;
            }
        }
        let ts_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        let _ = writeln!(
            writer,
            "{{\"ts_ms\":{},\"metrics\":{}}}",
            ts_ms,
            db.metrics_json()
        );
        let _ = writer.flush();
    })
}

fn worker_main(
    _tid: usize,
    db: Arc<Db>,
    workload: Workload,
    cleanup_batcher: Arc<CleanupBatcher>,
    rx: crossbeam_channel::Receiver<WorkerJob>,
    ack_tx: crossbeam_channel::Sender<Ack>,
) {
    while let Ok(job) = rx.recv() {
        match job {
            WorkerJob::Exec { id, op } => {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    execute_worker_op_ack(&db, &cleanup_batcher, &op, workload)
                }));
                match result {
                    Ok(Ok(Some(detail))) => {
                        let _ = ack_tx.send(Ack::Onyx(id, detail));
                    }
                    Ok(Ok(None)) => {
                        let _ = ack_tx.send(Ack::Ok(id));
                    }
                    Ok(Err(err)) => {
                        let _ = ack_tx.send(Ack::Error(id, err.to_string()));
                    }
                    Err(_) => process::abort(),
                }
            }
            WorkerJob::Stop => break,
        }
    }
}

fn writer_main(rx: crossbeam_channel::Receiver<Ack>) {
    let stdout = std::io::stdout();
    let mut writer = BufWriter::new(stdout.lock());
    while let Ok(ack) = rx.recv() {
        let _ = write_ack_line(&mut writer, &ack);
    }
    let _ = writer.flush();
}

fn write_ack_line(writer: &mut impl Write, ack: &Ack) -> std::io::Result<()> {
    match ack {
        Ack::Ok(id) => writeln!(writer, "OK {id}")?,
        Ack::Snapshot(id, snapshot_id) => writeln!(writer, "SNAP {id} {snapshot_id}")?,
        Ack::Volume(id, ord) => writeln!(writer, "VOL {id} {ord}")?,
        Ack::Onyx(id, detail) => writeln!(writer, "ONYX {id} {}", escape_json(detail))?,
        Ack::Error(id, err) => writeln!(writer, "ERR {id} {}", escape_json(err))?,
    }
    writer.flush()
}

struct CleanupBatcher {
    pending: Mutex<Vec<Pba>>,
    batch_size: usize,
}

impl CleanupBatcher {
    fn new(batch_size: usize) -> Self {
        Self {
            pending: Mutex::new(Vec::with_capacity(batch_size.min(4096))),
            batch_size: batch_size.max(1),
        }
    }

    fn push(&self, db: &Db, pba: Pba) -> onyx_metadb::Result<()> {
        let batch = {
            let mut pending = self.pending.lock();
            pending.push(pba);
            if pending.len() < self.batch_size {
                return Ok(());
            }
            std::mem::take(&mut *pending)
        };
        self.cleanup(db, batch)
    }

    fn flush(&self, db: &Db) -> onyx_metadb::Result<()> {
        let batch = {
            let mut pending = self.pending.lock();
            if pending.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *pending)
        };
        self.cleanup(db, batch)
    }

    fn cleanup(&self, db: &Db, mut batch: Vec<Pba>) -> onyx_metadb::Result<()> {
        batch.sort_unstable();
        batch.dedup();
        db.cleanup_dedup_for_dead_pbas(&batch)?;
        Ok(())
    }
}

fn execute_worker_op(
    db: &Db,
    cleanup_batcher: &CleanupBatcher,
    op: &WorkerOp,
) -> onyx_metadb::Result<()> {
    match op.kind {
        WorkerOpKind::Insert(byte) => {
            db.insert(op.vol_ord, l2p_key(op.tid, op.slot), l2p_value(byte))?;
        }
        WorkerOpKind::Delete => {
            db.delete(op.vol_ord, l2p_key(op.tid, op.slot))?;
        }
        WorkerOpKind::PutDedup(byte) => {
            db.put_dedup(dedup_hash(op.tid, op.slot), dedup_value(byte))?;
        }
        WorkerOpKind::DeleteDedup => {
            db.delete_dedup(dedup_hash(op.tid, op.slot))?;
        }
        WorkerOpKind::Incref => {
            db.incref_pba(refcount_pba(op.tid, op.slot), 1)?;
        }
        WorkerOpKind::Decref => {
            let _ = db.decref_pba(refcount_pba(op.tid, op.slot), 1);
        }
        WorkerOpKind::Get => {
            let _ = db.get(op.vol_ord, l2p_key(op.tid, op.slot))?;
            let _ = db.get_dedup(&dedup_hash(op.tid, op.slot))?;
            let _ = db.get_refcount(refcount_pba(op.tid, op.slot))?;
        }
        WorkerOpKind::OnyxRemap { pba, salt, guard } => {
            let guard = match guard {
                0 => None,
                1 => Some((pba, 1)),
                _ => Some((pba, u32::MAX)),
            };
            let mut tx = db.begin();
            tx.l2p_remap(op.vol_ord, op.slot, onyx_l2p_value(pba, salt), guard);
            let (_, outcomes) = tx.commit_with_outcomes()?;
            let detail = encode_onyx_outcome(&outcomes[0]);
            if let Some(freed) = detail
                .strip_prefix("applied freed=")
                .and_then(|s| s.parse::<Pba>().ok())
            {
                cleanup_batcher.push(db, freed)?;
            }
        }
        WorkerOpKind::OnyxRangeDelete { len } => {
            db.range_delete(op.vol_ord, op.slot, op.slot.saturating_add(len))?;
        }
        WorkerOpKind::OnyxDedupHit { pba, salt } => {
            let value = onyx_l2p_value(pba, salt);
            let mut tx = db.begin();
            tx.l2p_remap(op.vol_ord, op.slot, value, Some((pba, 1)));
            tx.put_dedup(onyx_hash(salt), onyx_dedup_value(pba, salt));
            tx.register_dedup_reverse(pba, onyx_hash(salt));
            tx.commit()?;
        }
        WorkerOpKind::OnyxCleanup { pba } => {
            cleanup_batcher.push(db, pba)?;
        }
    }
    Ok(())
}

fn execute_worker_op_ack(
    db: &Db,
    cleanup_batcher: &CleanupBatcher,
    op: &WorkerOp,
    workload: Workload,
) -> onyx_metadb::Result<Option<String>> {
    match op.kind {
        WorkerOpKind::OnyxRemap { pba, salt, guard } => {
            let before = (workload == Workload::Onyx)
                .then(|| db_refcount_sum(db))
                .transpose()?;
            let guard = match guard {
                0 => None,
                1 => Some((pba, 1)),
                _ => Some((pba, u32::MAX)),
            };
            let mut tx = db.begin();
            tx.l2p_remap(op.vol_ord, op.slot, onyx_l2p_value(pba, salt), guard);
            let (_, outcomes) = tx.commit_with_outcomes()?;
            let detail = encode_onyx_outcome(&outcomes[0]);
            if let ApplyOutcome::L2pRemap {
                freed_pba: Some(freed),
                ..
            } = outcomes[0]
            {
                cleanup_batcher.push(db, freed)?;
            }
            if let Some(before) = before {
                let after = db_refcount_sum(db)?;
                Ok(Some(format!(
                    "{detail} delta={}",
                    after as i64 - before as i64
                )))
            } else {
                Ok(Some(detail))
            }
        }
        WorkerOpKind::OnyxDedupHit { pba, salt } => {
            let before = (workload == Workload::Onyx)
                .then(|| db_refcount_sum(db))
                .transpose()?;
            let value = onyx_l2p_value(pba, salt);
            let mut tx = db.begin();
            tx.l2p_remap(op.vol_ord, op.slot, value, Some((pba, 1)));
            tx.put_dedup(onyx_hash(salt), onyx_dedup_value(pba, salt));
            tx.register_dedup_reverse(pba, onyx_hash(salt));
            let (_, outcomes) = tx.commit_with_outcomes()?;
            let detail = encode_onyx_outcome(&outcomes[0]);
            if let ApplyOutcome::L2pRemap {
                freed_pba: Some(freed),
                ..
            } = outcomes[0]
            {
                cleanup_batcher.push(db, freed)?;
            }
            if let Some(before) = before {
                let after = db_refcount_sum(db)?;
                Ok(Some(format!(
                    "{detail} delta={}",
                    after as i64 - before as i64
                )))
            } else {
                Ok(Some(detail))
            }
        }
        WorkerOpKind::OnyxRangeDelete { .. } | WorkerOpKind::OnyxCleanup { .. } => {
            let before = (workload == Workload::Onyx)
                .then(|| db_refcount_sum(db))
                .transpose()?;
            execute_worker_op(db, cleanup_batcher, op)?;
            if let Some(before) = before {
                let after = db_refcount_sum(db)?;
                Ok(Some(format!("ok delta={}", after as i64 - before as i64)))
            } else {
                Ok(Some("ok".into()))
            }
        }
        _ => {
            execute_worker_op(db, cleanup_batcher, op)?;
            Ok(None)
        }
    }
}

fn encode_onyx_outcome(outcome: &ApplyOutcome) -> String {
    match outcome {
        ApplyOutcome::L2pRemap { applied: false, .. } => "rejected".into(),
        ApplyOutcome::L2pRemap {
            applied: true,
            freed_pba: Some(pba),
            ..
        } => format!("applied freed={pba}"),
        ApplyOutcome::L2pRemap {
            applied: true,
            freed_pba: None,
            ..
        } => "applied".into(),
        _ => "ok".into(),
    }
}

fn db_refcount_sum(db: &Db) -> onyx_metadb::Result<u64> {
    db.iter_refcounts()?.try_fold(0u64, |acc, item| {
        let (_, rc) = item?;
        Ok(acc + u64::from(rc))
    })
}

fn audit_pba_refcounts(db: &Db) -> onyx_metadb::Result<()> {
    let mut versions: BTreeMap<(VolumeOrdinal, u64), BTreeSet<[u8; 28]>> = BTreeMap::new();
    let mut evidence: BTreeMap<Pba, Vec<String>> = BTreeMap::new();
    for vol in db.volumes() {
        for item in db.range(vol, ..)? {
            let (lba, value) = item?;
            if versions.entry((vol, lba)).or_default().insert(value.0) {
                evidence
                    .entry(value.head_pba())
                    .or_default()
                    .push(format!("live vol={vol} lba={lba}"));
            }
        }
    }
    for snapshot in db.snapshots() {
        let Some(view) = db.snapshot_view(snapshot.id) else {
            return Err(onyx_metadb::MetaDbError::Corruption(format!(
                "snapshot {} disappeared during audit",
                snapshot.id
            )));
        };
        for item in view.range(..)? {
            let (lba, value) = item?;
            if versions
                .entry((snapshot.vol_ord, lba))
                .or_default()
                .insert(value.0)
            {
                evidence.entry(value.head_pba()).or_default().push(format!(
                    "snapshot={} vol={} lba={lba}",
                    snapshot.id, snapshot.vol_ord
                ));
            }
        }
    }

    let mut expected: BTreeMap<Pba, u32> = BTreeMap::new();
    for values in versions.values() {
        for value in values {
            *expected.entry(L2pValue(*value).head_pba()).or_insert(0) += 1;
        }
    }
    let actual: BTreeMap<Pba, u32> = db
        .iter_refcounts()?
        .collect::<onyx_metadb::Result<Vec<_>>>()?
        .into_iter()
        .collect();
    if actual != expected {
        let mut diff = Vec::new();
        let mut keys: BTreeSet<Pba> = actual.keys().copied().collect();
        keys.extend(expected.keys().copied());
        for pba in keys {
            let actual_rc = actual.get(&pba).copied().unwrap_or(0);
            let expected_rc = expected.get(&pba).copied().unwrap_or(0);
            if actual_rc != expected_rc {
                let why = evidence
                    .get(&pba)
                    .map(|items| items.join("; "))
                    .unwrap_or_else(|| "<no live/snapshot witness>".into());
                diff.push(format!(
                    "pba={pba} actual={actual_rc} expected={expected_rc} witnesses=[{why}]"
                ));
                if diff.len() >= 8 {
                    break;
                }
            }
        }
        return Err(onyx_metadb::MetaDbError::Corruption(format!(
            "PBA refcount audit mismatch: actual={actual:?} expected={expected:?} diffs={diff:?}"
        )));
    }
    Ok(())
}

fn parse_delta(detail: &str) -> Result<Option<i64>, String> {
    for part in detail.split_whitespace() {
        if let Some(raw) = part.strip_prefix("delta=") {
            return raw.parse::<i64>().map(Some).map_err(|e| e.to_string());
        }
    }
    Ok(None)
}

fn parse_sum(detail: &str) -> Result<u64, String> {
    for part in detail.split_whitespace() {
        if let Some(raw) = part.strip_prefix("sum=") {
            return raw.parse::<u64>().map_err(|e| e.to_string());
        }
    }
    Err(format!("missing sum= in `{detail}`"))
}
