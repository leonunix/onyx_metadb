use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::env;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{self, Child, ChildStdin, ChildStdout, Command, ExitCode, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use onyx_metadb::testing::faults::{FaultAction, FaultController, FaultPoint};
use onyx_metadb::{
    Db, DedupValue, Hash32, L2pValue, SnapshotId, VerifyOptions, VolumeOrdinal, verify_path,
};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;

const KEY_SLOTS_PER_THREAD: u64 = 256;
const MAX_LIVE_VOLUMES: usize = 4;
const BOOTSTRAP_VOL: VolumeOrdinal = 0;

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(err) => {
            if !err.is_empty() {
                eprintln!("metadb-soak: {err}");
            }
            ExitCode::from(2)
        }
    }
}

fn run() -> Result<ExitCode, String> {
    let mode = Mode::parse(env::args().skip(1))?;
    match mode {
        Mode::Parent(cfg) => run_parent(cfg),
        Mode::Child(cfg) => run_child(cfg),
    }
}

enum Mode {
    Parent(ParentConfig),
    Child(ChildConfig),
}

impl Mode {
    fn parse<I>(mut args: I) -> Result<Self, String>
    where
        I: Iterator<Item = String>,
    {
        let mut child = false;
        let mut raw = Vec::new();
        while let Some(arg) = args.next() {
            if arg == "--child" {
                child = true;
            } else {
                raw.push(arg);
            }
        }
        if child {
            Ok(Self::Child(ChildConfig::parse(raw.into_iter())?))
        } else {
            Ok(Self::Parent(ParentConfig::parse(raw.into_iter())?))
        }
    }
}

#[derive(Clone, Debug)]
struct ParentConfig {
    path: PathBuf,
    duration_secs: u64,
    ops_per_cycle: usize,
    threads: usize,
    seed: u64,
    summary_path: PathBuf,
    events_path: PathBuf,
    fault_density_pct: u8,
}

impl ParentConfig {
    fn parse<I>(mut args: I) -> Result<Self, String>
    where
        I: Iterator<Item = String>,
    {
        let mut path = None;
        let mut duration_secs = 300u64;
        let mut ops_per_cycle = 2_000usize;
        let mut threads = 4usize;
        let mut seed = 0x5EED_8A5Eu64;
        let mut summary_path = None;
        let mut events_path = None;
        let mut fault_density_pct = 0u8;

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--duration-secs" => {
                    duration_secs = parse_u64(args.next(), "--duration-secs")?;
                }
                "--ops-per-cycle" => {
                    ops_per_cycle = parse_u64(args.next(), "--ops-per-cycle")? as usize;
                }
                "--threads" => {
                    threads = parse_u64(args.next(), "--threads")? as usize;
                }
                "--seed" => {
                    seed = parse_u64(args.next(), "--seed")?;
                }
                "--summary" => {
                    summary_path = Some(PathBuf::from(
                        args.next()
                            .ok_or_else(|| "--summary needs a path".to_string())?,
                    ));
                }
                "--events" => {
                    events_path = Some(PathBuf::from(
                        args.next()
                            .ok_or_else(|| "--events needs a path".to_string())?,
                    ));
                }
                "--fault-density-pct" => {
                    let value = parse_u64(args.next(), "--fault-density-pct")?;
                    if value > 100 {
                        return Err("--fault-density-pct must be in 0..=100".into());
                    }
                    fault_density_pct = value as u8;
                }
                "-h" | "--help" => {
                    print_parent_usage();
                    return Err(String::new());
                }
                _ if arg.starts_with('-') => return Err(format!("unknown flag `{arg}`")),
                _ if path.is_none() => path = Some(PathBuf::from(arg)),
                _ => return Err("expected exactly one database path".into()),
            }
        }

        let Some(path) = path else {
            print_parent_usage();
            return Err(String::new());
        };
        Ok(Self {
            summary_path: summary_path.unwrap_or_else(|| path.join("summary.json")),
            events_path: events_path.unwrap_or_else(|| path.join("events.jsonl")),
            path,
            duration_secs,
            ops_per_cycle: ops_per_cycle.max(1),
            threads: threads.max(1),
            seed,
            fault_density_pct,
        })
    }
}

#[derive(Clone, Debug, Default)]
struct ChildConfig {
    path: PathBuf,
    threads: usize,
    fault: Option<FaultSpec>,
}

impl ChildConfig {
    fn parse<I>(mut args: I) -> Result<Self, String>
    where
        I: Iterator<Item = String>,
    {
        let mut path = None;
        let mut threads = 1usize;
        let mut fault_point = None;
        let mut fault_hit = None;
        let mut fault_action = None;

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--path" => {
                    path = Some(PathBuf::from(
                        args.next()
                            .ok_or_else(|| "--path needs a value".to_string())?,
                    ));
                }
                "--threads" => {
                    threads = parse_u64(args.next(), "--threads")? as usize;
                }
                "--fault-point" => {
                    fault_point = Some(parse_fault_point(
                        &args
                            .next()
                            .ok_or_else(|| "--fault-point needs a value".to_string())?,
                    )?);
                }
                "--fault-hit" => {
                    fault_hit = Some(parse_u64(args.next(), "--fault-hit")?);
                }
                "--fault-action" => {
                    fault_action =
                        Some(parse_fault_action(&args.next().ok_or_else(|| {
                            "--fault-action needs a value".to_string()
                        })?)?);
                }
                _ => return Err(format!("unknown child flag `{arg}`")),
            }
        }

        let Some(path) = path else {
            return Err("child mode requires --path".into());
        };
        let fault = match (fault_point, fault_hit, fault_action) {
            (Some(point), Some(hit), Some(action)) => Some(FaultSpec { point, hit, action }),
            (None, None, None) => None,
            _ => {
                return Err(
                    "fault config requires --fault-point, --fault-hit, and --fault-action".into(),
                );
            }
        };
        Ok(Self {
            path,
            threads: threads.max(1),
            fault,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct FaultSpec {
    point: FaultPoint,
    hit: u64,
    action: FaultAction,
}

#[derive(Clone, Debug)]
struct Summary {
    path: PathBuf,
    duration_secs: u64,
    cycles: u64,
    ops: u64,
    restarts: u64,
    verifies: u64,
    fault_cycles: u64,
    deadlock_detected: bool,
    success: bool,
    last_error: Option<String>,
}

struct Model {
    l2p: BTreeMap<VolumeOrdinal, BTreeMap<u64, L2pValue>>,
    dedup: BTreeMap<Hash32, DedupValue>,
    refcount: BTreeMap<u64, u32>,
}

impl Default for Model {
    fn default() -> Self {
        let mut l2p = BTreeMap::new();
        l2p.insert(BOOTSTRAP_VOL, BTreeMap::new());
        Self {
            l2p,
            dedup: BTreeMap::new(),
            refcount: BTreeMap::new(),
        }
    }
}

impl Model {
    fn live_volumes(&self) -> Vec<VolumeOrdinal> {
        self.l2p.keys().copied().collect()
    }

    fn drop_candidates(&self, pinned: &BTreeSet<VolumeOrdinal>) -> Vec<VolumeOrdinal> {
        self.l2p
            .keys()
            .copied()
            .filter(|ord| *ord != BOOTSTRAP_VOL && !pinned.contains(ord))
            .collect()
    }
}

#[derive(Clone, Debug)]
enum WorkerOpKind {
    Insert(u8),
    Delete,
    PutDedup(u8),
    DeleteDedup,
    Incref,
    Decref,
    Get,
}

#[derive(Clone, Debug)]
struct WorkerOp {
    tid: usize,
    vol_ord: VolumeOrdinal,
    slot: u64,
    kind: WorkerOpKind,
}

#[derive(Clone, Debug)]
struct ModelSnapshot {
    id: SnapshotId,
    vol_ord: VolumeOrdinal,
    l2p: BTreeMap<u64, L2pValue>,
}

#[derive(Clone, Debug)]
enum Ack {
    Ok(u64),
    Snapshot(u64, SnapshotId),
    Volume(u64, VolumeOrdinal),
    Error(u64, String),
}

#[derive(Clone, Debug)]
enum WorkerJob {
    Exec { id: u64, op: WorkerOp },
    Stop,
}

fn run_parent(cfg: ParentConfig) -> Result<ExitCode, String> {
    std::fs::create_dir_all(&cfg.path).map_err(|e| e.to_string())?;
    let mut events = EventLog::open(&cfg.events_path).map_err(|e| e.to_string())?;
    events.write(
        "start",
        &format!("seed={} threads={}", cfg.seed, cfg.threads),
    )?;

    let started = Instant::now();
    let mut model = Model::default();
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

    while started.elapsed() < Duration::from_secs(cfg.duration_secs) {
        cycles += 1;
        let fault = choose_fault(&mut cycle_rng, cfg.fault_density_pct);
        if fault.is_some() {
            fault_cycles += 1;
        }
        events.write(
            "cycle_start",
            &format!("cycle={} fault={}", cycles, fault_label(fault)),
        )?;
        let mut child = spawn_child(&cfg, fault).map_err(|e| e.to_string())?;

        match run_cycle(
            &cfg,
            cycles,
            &mut child,
            &mut model,
            &mut rngs,
            &mut cycle_rng,
            &mut snapshots,
            &mut total_ops,
            &mut events,
        ) {
            Ok(()) => {}
            Err(err) => {
                last_error = Some(err);
                let _ = kill_child(&mut child);
                break;
            }
        }

        if let Err(err) = kill_child(&mut child) {
            last_error = Some(err.to_string());
            break;
        }
        restarts += 1;
        events.write("child_killed", &format!("cycle={cycles}"))?;

        match verify_reopened_db(&cfg.path, &model, &snapshots, cfg.threads) {
            Ok(report) => {
                if !report.is_clean() {
                    last_error = Some(format!("metadb-verify failed: {:?}", report.issues));
                    break;
                }
                verifies += 1;
                events.write(
                    "verify_ok",
                    &format!(
                        "cycle={} live_pages={} free_pages={}",
                        cycles, report.live_pages, report.free_pages
                    ),
                )?;
            }
            Err(err) => {
                if err.to_string().contains("deadlock") {
                    deadlock_detected = true;
                }
                last_error = Some(err.to_string());
                break;
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
fn run_cycle(
    cfg: &ParentConfig,
    cycle: u64,
    child: &mut ChildHandle,
    model: &mut Model,
    rngs: &mut [ChaCha8Rng],
    admin_rng: &mut ChaCha8Rng,
    snapshots: &mut Vec<ModelSnapshot>,
    total_ops: &mut u64,
    events: &mut EventLog,
) -> Result<(), String> {
    let mut next_id = 1u64;

    // Phase 1: pre-worker volume admin (create / drop / clone).
    next_id = run_volume_admin(child, cycle, model, admin_rng, snapshots, events, next_id)?;

    // Phase 2: workers pounding across the live volume set.
    let mut sent_ops = 0usize;
    let mut free_tids: VecDeque<usize> = (0..cfg.threads).collect();
    let mut inflight: HashMap<u64, WorkerOp> = HashMap::new();

    while sent_ops < cfg.ops_per_cycle || !inflight.is_empty() {
        while sent_ops < cfg.ops_per_cycle && !free_tids.is_empty() {
            let tid = free_tids.pop_front().unwrap();
            let op = generate_worker_op(tid, &mut rngs[tid], model);
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
                apply_worker_op(model, &op)?;
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

    // Phase 3: flush + per-volume snapshot on a random live vol.
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

    let live = model.live_volumes();
    let snap_vol = live[admin_rng.gen_range(0..live.len())];
    let snapshot_model = model
        .l2p
        .get(&snap_vol)
        .cloned()
        .unwrap_or_default();
    send_admin(child, next_id, &format!("SNAPSHOT {snap_vol}"))?;
    match recv_ack(child)? {
        Ack::Snapshot(id, snapshot_id) if id == next_id => {
            snapshots.push(ModelSnapshot {
                id: snapshot_id,
                vol_ord: snap_vol,
                l2p: snapshot_model,
            });
            events.write(
                "snapshot_ok",
                &format!("cycle={} vol={} snapshot={}", cycle, snap_vol, snapshot_id),
            )?;
        }
        Ack::Error(id, err) if id == next_id => {
            events.write(
                "snapshot_err",
                &format!(
                    "cycle={} vol={} err={}",
                    cycle,
                    snap_vol,
                    escape_json(&err)
                ),
            )?;
        }
        other => return Err(format!("unexpected snapshot ack: {other:?}")),
    }
    next_id += 1;

    if snapshots.len() > 4 {
        let entry = snapshots.remove(0);
        send_admin(child, next_id, &format!("DROP {}", entry.id))?;
        match recv_ack(child)? {
            Ack::Ok(id) if id == next_id => {
                events.write(
                    "drop_snapshot_ok",
                    &format!("cycle={} vol={} snapshot={}", cycle, entry.vol_ord, entry.id),
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
                events.write(
                    "create_volume_ok",
                    &format!("cycle={cycle} ord={ord}"),
                )?;
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
                events.write(
                    "drop_volume_ok",
                    &format!("cycle={} ord={}", cycle, pick),
                )?;
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

fn run_child(cfg: ChildConfig) -> Result<ExitCode, String> {
    let faults = FaultController::new();
    if let Some(fault) = cfg.fault {
        faults.install(fault.point, fault.hit, fault.action);
    }
    let db = Arc::new(open_or_create_with_faults(&cfg.path, faults).map_err(|e| e.to_string())?);

    let deadlock_seen = Arc::new(AtomicBool::new(false));
    let deadlock_stop = Arc::new(AtomicBool::new(false));
    let monitor = spawn_deadlock_monitor(deadlock_stop.clone(), deadlock_seen.clone());

    let (ack_tx, ack_rx) = crossbeam_channel::unbounded::<Ack>();
    let admin_ack_tx = ack_tx.clone();
    let mut worker_txs = Vec::with_capacity(cfg.threads);
    let mut worker_handles = Vec::with_capacity(cfg.threads);
    for tid in 0..cfg.threads {
        let (job_tx, job_rx) = crossbeam_channel::unbounded::<WorkerJob>();
        worker_txs.push(job_tx);
        let db = db.clone();
        let ack_tx = ack_tx.clone();
        worker_handles.push(thread::spawn(move || worker_main(tid, db, job_rx, ack_tx)));
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
                let ack = match verb {
                    "FLUSH" => db.flush().map(|_| Ack::Ok(id)),
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
                        let vol_ord = parse_part_u64(parts.next(), "drop vol_ord")? as VolumeOrdinal;
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
    let _ = monitor.join();
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

fn worker_main(
    _tid: usize,
    db: Arc<Db>,
    rx: crossbeam_channel::Receiver<WorkerJob>,
    ack_tx: crossbeam_channel::Sender<Ack>,
) {
    while let Ok(job) = rx.recv() {
        match job {
            WorkerJob::Exec { id, op } => {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    execute_worker_op(&db, &op)
                }));
                match result {
                    Ok(Ok(())) => {
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
        Ack::Error(id, err) => writeln!(writer, "ERR {id} {}", escape_json(err))?,
    }
    writer.flush()
}

fn execute_worker_op(db: &Db, op: &WorkerOp) -> onyx_metadb::Result<()> {
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
    }
    Ok(())
}

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
    }
    Ok(())
}

fn verify_reopened_db(
    path: &Path,
    model: &Model,
    snapshots: &[ModelSnapshot],
    threads: usize,
) -> onyx_metadb::Result<onyx_metadb::VerifyReport> {
    let db = Db::open(path)?;
    verify_live_db(&db, model, threads)?;
    for snap in snapshots {
        verify_snapshot(&db, snap.id, &snap.l2p)?;
    }
    drop(db);
    verify_path(path, VerifyOptions { strict: true })
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

fn spawn_child(cfg: &ParentConfig, fault: Option<FaultSpec>) -> std::io::Result<ChildHandle> {
    let mut cmd = Command::new(env::current_exe()?);
    cmd.arg("--child")
        .arg("--path")
        .arg(&cfg.path)
        .arg("--threads")
        .arg(cfg.threads.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    if let Some(fault) = fault {
        cmd.arg("--fault-point")
            .arg(fault.point.name())
            .arg("--fault-hit")
            .arg(fault.hit.to_string())
            .arg("--fault-action")
            .arg(match fault.action {
                FaultAction::Error => "error",
                FaultAction::Panic => "panic",
            });
    }
    let mut child = cmd.spawn()?;
    let stdin = child.stdin.take().expect("child stdin");
    let stdout = child.stdout.take().expect("child stdout");
    Ok(ChildHandle {
        child,
        stdin,
        stdout: BufReader::new(stdout),
    })
}

fn send_worker_op(child: &mut ChildHandle, id: u64, op: &WorkerOp) -> Result<(), String> {
    writeln!(
        child.stdin,
        "W {} {} {} {} {} {}",
        id,
        op.tid,
        worker_kind_name(&op.kind),
        op.vol_ord,
        op.slot,
        worker_kind_arg(&op.kind)
    )
    .map_err(|e| e.to_string())?;
    child.stdin.flush().map_err(|e| e.to_string())
}

fn send_admin(child: &mut ChildHandle, id: u64, command: &str) -> Result<(), String> {
    writeln!(child.stdin, "A {id} {command}").map_err(|e| e.to_string())?;
    child.stdin.flush().map_err(|e| e.to_string())
}

fn recv_ack(child: &mut ChildHandle) -> Result<Ack, String> {
    let mut line = String::new();
    let read = child
        .stdout
        .read_line(&mut line)
        .map_err(|e| e.to_string())?;
    if read == 0 {
        let status = child.child.try_wait().ok().flatten();
        return Err(format!("child exited unexpectedly: {status:?}"));
    }
    parse_ack(line.trim_end())
}

fn parse_ack(line: &str) -> Result<Ack, String> {
    let mut parts = line.split_whitespace();
    match parts.next() {
        Some("OK") => Ok(Ack::Ok(parse_part_u64(parts.next(), "ack id")?)),
        Some("SNAP") => Ok(Ack::Snapshot(
            parse_part_u64(parts.next(), "ack id")?,
            parse_part_u64(parts.next(), "snapshot id")?,
        )),
        Some("VOL") => Ok(Ack::Volume(
            parse_part_u64(parts.next(), "ack id")?,
            parse_part_u64(parts.next(), "volume ord")? as VolumeOrdinal,
        )),
        Some("ERR") => {
            let id = parse_part_u64(parts.next(), "ack id")?;
            let rest = parts.collect::<Vec<_>>().join(" ");
            Ok(Ack::Error(id, rest))
        }
        Some(other) => Err(format!("unknown ack kind `{other}`")),
        None => Err("empty ack line".into()),
    }
}

fn kill_child(child: &mut ChildHandle) -> std::io::Result<()> {
    let _ = child.child.kill();
    let _ = child.child.wait();
    Ok(())
}

fn choose_fault(rng: &mut ChaCha8Rng, density_pct: u8) -> Option<FaultSpec> {
    if density_pct == 0 || rng.gen_range(0..100u8) >= density_pct {
        return None;
    }
    let point = match rng.gen_range(0..2) {
        0 => FaultPoint::ManifestFsyncBefore,
        _ => FaultPoint::FlushPostLevelRewriteBeforeManifest,
    };
    Some(FaultSpec {
        point,
        hit: rng.gen_range(1..=8),
        action: FaultAction::Error,
    })
}

fn fault_label(fault: Option<FaultSpec>) -> String {
    match fault {
        Some(fault) => format!("{}@{}", fault.point.name(), fault.hit),
        None => "none".into(),
    }
}

fn open_or_create_with_faults(
    path: &Path,
    faults: Arc<FaultController>,
) -> onyx_metadb::Result<Db> {
    match Db::open_with_faults(path, faults.clone()) {
        Ok(db) => Ok(db),
        Err(_) => Db::create_with_faults(path, faults),
    }
}

fn parse_fault_point(raw: &str) -> Result<FaultPoint, String> {
    match raw {
        "wal.fsync.before" => Ok(FaultPoint::WalFsyncBefore),
        "manifest.fsync.before" => Ok(FaultPoint::ManifestFsyncBefore),
        "flush.level_rewrite.before_manifest" => {
            Ok(FaultPoint::FlushPostLevelRewriteBeforeManifest)
        }
        other => Err(format!("unsupported fault point `{other}`")),
    }
}

fn parse_fault_action(raw: &str) -> Result<FaultAction, String> {
    match raw {
        "error" => Ok(FaultAction::Error),
        "panic" => Ok(FaultAction::Panic),
        other => Err(format!("unsupported fault action `{other}`")),
    }
}

fn parse_worker_op<'a, I>(mut parts: I) -> Result<WorkerOp, String>
where
    I: Iterator<Item = &'a str>,
{
    let tid = parse_part_u64(parts.next(), "worker tid")? as usize;
    let kind = parts
        .next()
        .ok_or_else(|| "missing worker kind".to_string())?;
    let vol_ord = parse_part_u64(parts.next(), "worker vol_ord")? as VolumeOrdinal;
    let slot = parse_part_u64(parts.next(), "worker slot")?;
    let arg = parse_part_u64(parts.next(), "worker arg")? as u8;
    let kind = match kind {
        "insert" => WorkerOpKind::Insert(arg),
        "delete" => WorkerOpKind::Delete,
        "put_dedup" => WorkerOpKind::PutDedup(arg),
        "delete_dedup" => WorkerOpKind::DeleteDedup,
        "incref" => WorkerOpKind::Incref,
        "decref" => WorkerOpKind::Decref,
        "get" => WorkerOpKind::Get,
        other => return Err(format!("unknown worker kind `{other}`")),
    };
    Ok(WorkerOp {
        tid,
        vol_ord,
        slot,
        kind,
    })
}

fn worker_kind_name(kind: &WorkerOpKind) -> &'static str {
    match kind {
        WorkerOpKind::Insert(_) => "insert",
        WorkerOpKind::Delete => "delete",
        WorkerOpKind::PutDedup(_) => "put_dedup",
        WorkerOpKind::DeleteDedup => "delete_dedup",
        WorkerOpKind::Incref => "incref",
        WorkerOpKind::Decref => "decref",
        WorkerOpKind::Get => "get",
    }
}

fn worker_kind_arg(kind: &WorkerOpKind) -> u8 {
    match kind {
        WorkerOpKind::Insert(byte) | WorkerOpKind::PutDedup(byte) => *byte,
        _ => 0,
    }
}

fn parse_part_u64(part: Option<&str>, label: &str) -> Result<u64, String> {
    part.ok_or_else(|| format!("missing {label}"))?
        .parse::<u64>()
        .map_err(|e| format!("{label}: {e}"))
}

fn parse_u64(value: Option<String>, flag: &str) -> Result<u64, String> {
    value
        .ok_or_else(|| format!("{flag} needs a value"))?
        .parse::<u64>()
        .map_err(|e| format!("{flag}: {e}"))
}

fn l2p_key(tid: usize, slot: u64) -> u64 {
    ((tid as u64) << 32) | slot
}

fn dedup_hash(tid: usize, slot: u64) -> Hash32 {
    let mut hash = [0u8; 32];
    hash[..8].copy_from_slice(&(tid as u64).to_be_bytes());
    hash[8..16].copy_from_slice(&slot.to_be_bytes());
    hash
}

fn refcount_pba(tid: usize, slot: u64) -> u64 {
    ((tid as u64) << 24) | slot
}

fn l2p_value(byte: u8) -> L2pValue {
    let mut value = [0u8; 28];
    value[0] = byte;
    value[1] = byte.wrapping_mul(3);
    L2pValue(value)
}

fn dedup_value(byte: u8) -> DedupValue {
    let mut value = [0u8; 28];
    value[0] = byte;
    value[1] = byte.wrapping_mul(7);
    DedupValue(value)
}

struct EventLog {
    writer: BufWriter<std::fs::File>,
}

impl EventLog {
    fn open(path: &Path) -> std::io::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    fn write(&mut self, kind: &str, detail: &str) -> Result<(), String> {
        writeln!(
            self.writer,
            "{{\"kind\":\"{}\",\"detail\":\"{}\"}}",
            escape_json(kind),
            escape_json(detail)
        )
        .map_err(|e| e.to_string())?;
        self.writer.flush().map_err(|e| e.to_string())
    }
}

fn write_summary(path: &Path, summary: &Summary) -> std::io::Result<()> {
    let json = format!(
        concat!(
            "{{\n",
            "  \"path\": \"{}\",\n",
            "  \"duration_secs\": {},\n",
            "  \"cycles\": {},\n",
            "  \"ops\": {},\n",
            "  \"restarts\": {},\n",
            "  \"verifies\": {},\n",
            "  \"fault_cycles\": {},\n",
            "  \"deadlock_detected\": {},\n",
            "  \"success\": {},\n",
            "  \"last_error\": {}\n",
            "}}\n"
        ),
        escape_json(&summary.path.display().to_string()),
        summary.duration_secs,
        summary.cycles,
        summary.ops,
        summary.restarts,
        summary.verifies,
        summary.fault_cycles,
        summary.deadlock_detected,
        summary.success,
        match &summary.last_error {
            Some(err) => format!("\"{}\"", escape_json(err)),
            None => "null".into(),
        }
    );
    std::fs::write(path, json)
}

fn print_parent_usage() {
    eprintln!(
        "usage: metadb-soak <path> [--duration-secs N] [--ops-per-cycle N] [--threads N] [--seed N] [--fault-density-pct N] [--summary path] [--events path]"
    );
}

fn escape_json(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if ch.is_control() => out.push_str(&format!("\\u{:04x}", ch as u32)),
            ch => out.push(ch),
        }
    }
    out
}
