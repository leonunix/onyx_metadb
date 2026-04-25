fn spawn_child(cfg: &ParentConfig, fault: Option<FaultSpec>) -> std::io::Result<ChildHandle> {
    let mut cmd = Command::new(env::current_exe()?);
    cmd.arg("--child")
        .arg("--path")
        .arg(&cfg.path)
        .arg("--threads")
        .arg(cfg.threads.to_string())
        .arg("--workload")
        .arg(workload_name(cfg.workload))
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
        Some("ONYX") => {
            let id = parse_part_u64(parts.next(), "ack id")?;
            let rest = parts.collect::<Vec<_>>().join(" ");
            Ok(Ack::Onyx(id, rest))
        }
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
