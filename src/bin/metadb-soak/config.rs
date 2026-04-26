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
    pipeline_depth: usize,
    threads: usize,
    seed: u64,
    summary_path: PathBuf,
    events_path: PathBuf,
    metrics_path: PathBuf,
    metrics_interval_secs: u64,
    event_verbosity: EventVerbosity,
    fault_density_pct: u8,
    workload: Workload,
    snapshots_enabled: bool,
    restart_interval_secs: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Workload {
    Legacy,
    Onyx,
    OnyxConcurrent,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EventVerbosity {
    Summary,
    Ops,
}

fn parse_event_verbosity(raw: &str) -> Result<EventVerbosity, String> {
    match raw {
        "summary" => Ok(EventVerbosity::Summary),
        "ops" => Ok(EventVerbosity::Ops),
        other => Err(format!("unknown event verbosity `{other}`")),
    }
}

fn workload_name(workload: Workload) -> &'static str {
    match workload {
        Workload::Legacy => "legacy",
        Workload::Onyx => "onyx",
        Workload::OnyxConcurrent => "onyx-concurrent",
    }
}

fn parse_workload(raw: &str) -> Result<Workload, String> {
    match raw {
        "legacy" => Ok(Workload::Legacy),
        "onyx" => Ok(Workload::Onyx),
        "onyx-concurrent" => Ok(Workload::OnyxConcurrent),
        other => Err(format!("unknown workload `{other}`")),
    }
}

impl ParentConfig {
    fn parse<I>(mut args: I) -> Result<Self, String>
    where
        I: Iterator<Item = String>,
    {
        let mut path = None;
        let mut duration_secs = 300u64;
        let mut ops_per_cycle = 2_000usize;
        let mut pipeline_depth = 1usize;
        let mut threads = 4usize;
        let mut seed = 0x5EED_8A5Eu64;
        let mut summary_path = None;
        let mut events_path = None;
        let mut metrics_path = None;
        let mut metrics_interval_secs = 5u64;
        let mut event_verbosity = EventVerbosity::Summary;
        let mut fault_density_pct = 0u8;
        let mut workload = Workload::Onyx;
        let mut snapshots_enabled = true;
        let mut restart_interval_secs = None;

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--duration-secs" => {
                    duration_secs = parse_u64(args.next(), "--duration-secs")?;
                }
                "--minutes" => {
                    duration_secs = parse_u64(args.next(), "--minutes")?.saturating_mul(60);
                }
                "--hours" => {
                    duration_secs = parse_u64(args.next(), "--hours")?.saturating_mul(3600);
                }
                "--restart-interval-secs" => {
                    restart_interval_secs =
                        Some(parse_u64(args.next(), "--restart-interval-secs")?);
                }
                "--restart-interval" => {
                    restart_interval_secs =
                        Some(parse_duration_arg(args.next().ok_or_else(|| {
                            "--restart-interval needs a value".to_string()
                        })?)?);
                }
                "--ops-per-cycle" => {
                    ops_per_cycle = parse_u64(args.next(), "--ops-per-cycle")? as usize;
                }
                "--pipeline-depth" => {
                    pipeline_depth = parse_u64(args.next(), "--pipeline-depth")? as usize;
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
                "--metrics" => {
                    metrics_path = Some(PathBuf::from(
                        args.next()
                            .ok_or_else(|| "--metrics needs a path".to_string())?,
                    ));
                }
                "--metrics-interval-secs" => {
                    metrics_interval_secs = parse_u64(args.next(), "--metrics-interval-secs")?;
                }
                "--event-verbosity" => {
                    event_verbosity = parse_event_verbosity(
                        &args
                            .next()
                            .ok_or_else(|| "--event-verbosity needs a value".to_string())?,
                    )?;
                }
                "--events-summary" => event_verbosity = EventVerbosity::Summary,
                "--events-ops" => event_verbosity = EventVerbosity::Ops,
                "--fault-density-pct" => {
                    let value = parse_u64(args.next(), "--fault-density-pct")?;
                    if value > 100 {
                        return Err("--fault-density-pct must be in 0..=100".into());
                    }
                    fault_density_pct = value as u8;
                }
                "--no-snapshots" => snapshots_enabled = false,
                "--onyx-mix" => workload = Workload::Onyx,
                "--onyx-concurrent-mix" => workload = Workload::OnyxConcurrent,
                "--legacy-mix" => workload = Workload::Legacy,
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
            metrics_path: metrics_path.unwrap_or_else(|| path.join("metrics.jsonl")),
            metrics_interval_secs,
            event_verbosity,
            path,
            duration_secs,
            ops_per_cycle: ops_per_cycle.max(1),
            pipeline_depth: pipeline_depth.max(1),
            threads: threads.max(1),
            seed,
            fault_density_pct,
            workload,
            snapshots_enabled,
            restart_interval_secs,
        })
    }
}

#[derive(Clone, Debug)]
struct ChildConfig {
    path: PathBuf,
    threads: usize,
    fault: Option<FaultSpec>,
    workload: Workload,
    metrics_path: Option<PathBuf>,
    metrics_interval_secs: u64,
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
        let mut workload = Workload::Legacy;
        let mut metrics_path = None;
        let mut metrics_interval_secs = 5u64;

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
                "--workload" => {
                    workload = parse_workload(
                        &args
                            .next()
                            .ok_or_else(|| "--workload needs a value".to_string())?,
                    )?;
                }
                "--metrics" => {
                    metrics_path = Some(PathBuf::from(
                        args.next()
                            .ok_or_else(|| "--metrics needs a path".to_string())?,
                    ));
                }
                "--metrics-interval-secs" => {
                    metrics_interval_secs = parse_u64(args.next(), "--metrics-interval-secs")?;
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
            workload,
            metrics_path,
            metrics_interval_secs,
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
    onyx_ops: u64,
    guard_hit: u64,
    guard_miss: u64,
    freed_pbas: u64,
    cleanup_deleted: u64,
    refcount_sum_mismatches: u64,
    deadlock_detected: bool,
    success: bool,
    last_error: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct OnyxStats {
    onyx_ops: u64,
    guard_hit: u64,
    guard_miss: u64,
    freed_pbas: u64,
    cleanup_deleted: u64,
    refcount_sum_mismatches: u64,
    refcount_sum: i64,
}
