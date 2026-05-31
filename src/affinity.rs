use std::sync::OnceLock;

#[derive(Clone, Debug, Default)]
pub struct AffinityConfig {
    pub wal_cpus: String,
    pub l2p_apply_cpus: String,
    pub refcount_apply_cpus: String,
    pub dedup_apply_cpus: String,
    /// CPU set for the per-shard refcount drainer threads (priority 3).
    /// Same syntax as the other knobs ("0-3,8,12-15"). Leave empty to
    /// inherit the OS default.
    pub refcount_drainer_cpus: String,
    /// CPU set for the ZFS-TXG-clone Phase 4 background workers — the
    /// `TxgSyncThread` (single serial thread that drains the syncing
    /// TXG slot per shard, writes RC + L2P checkpoint pages, fsyncs,
    /// commits the manifest) plus the `TxgQuiesceThread` (also a
    /// single thread, on the same role).
    ///
    /// Field name kept for backward compatibility with operator
    /// config (`metadb_l2p_compactor_cpus`) — the retired
    /// `L2pCompactor` was the historical occupant of this CPU set
    /// and the placement reasoning carries over verbatim: pinning to
    /// 1–2 CPUs on the same NUMA node as `l2p_apply_cpus` stops the
    /// scheduler from co-locating the sync worker on an apply-lane
    /// CPU during the flush window.
    pub l2p_compactor_cpus: String,
    /// CPU set for the io_uring submitter threads. With pool>1, each
    /// submitter thread picks `cpus[ordinal % len]` so distinct
    /// submitters land on distinct CPUs and Linux mq-block routes
    /// their writes to distinct NVMe hardware queues. Leave empty to
    /// inherit the OS default — which makes pool>1 net-regress (the
    /// scheduler can stack multiple submitters on the same core).
    pub io_submitter_cpus: String,
}

#[derive(Clone, Copy, Debug)]
pub enum ThreadRole {
    Wal,
    L2pApply,
    RefcountApply,
    DedupApply,
    RefcountDrainer,
    /// ZFS-TXG-clone Phase 4 sync + quiesce workers. Bound to a small
    /// dedicated CPU set so the kernel scheduler cannot co-locate
    /// them on an apply-lane CPU during a flush window. Replaces the
    /// retired `L2pCompactor` role; the affinity config field
    /// (`l2p_compactor_cpus`) keeps its legacy name for backward
    /// compatibility with operator configs.
    TxgSync,
    /// io_uring submitter thread. With pool>1 each ordinal binds to a
    /// distinct CPU so the kernel mq-block layer routes its IO to a
    /// different NVMe hardware queue. Without pinning, multiple
    /// submitter threads can land on the same CPU and serialise via
    /// runqueue contention — net-worse than pool=1.
    IoSubmitter,
}

#[derive(Clone, Debug, Default)]
struct AffinityLayout {
    wal: CpuSet,
    l2p_apply: CpuSet,
    refcount_apply: CpuSet,
    dedup_apply: CpuSet,
    refcount_drainer: CpuSet,
    /// Backs `ThreadRole::TxgSync`. Field name kept for backward
    /// compatibility with the `l2p_compactor_cpus` config knob the
    /// retired `L2pCompactor` used.
    l2p_compactor: CpuSet,
    io_submitter: CpuSet,
}

#[derive(Clone, Debug, Default)]
struct CpuSet {
    cpus: Vec<usize>,
}

static LAYOUT: OnceLock<Option<AffinityLayout>> = OnceLock::new();

pub fn configure(config: AffinityConfig) {
    let _ = LAYOUT.set(AffinityLayout::from_config(config));
}

pub(crate) fn bind_current(role: ThreadRole, ordinal: usize) {
    let Some(Some(layout)) = LAYOUT.get() else {
        return;
    };
    let Some(cpu) = layout.cpus_for(role).pick(ordinal) else {
        return;
    };
    if let Err(err) = set_current_cpu(cpu) {
        tracing::warn!(
            ?role,
            cpu,
            error = %err,
            "failed to set metadb thread CPU affinity"
        );
    }
}

impl AffinityLayout {
    fn from_config(config: AffinityConfig) -> Option<Self> {
        Some(Self {
            wal: CpuSet::parse(&config.wal_cpus),
            l2p_apply: CpuSet::parse(&config.l2p_apply_cpus),
            refcount_apply: CpuSet::parse(&config.refcount_apply_cpus),
            dedup_apply: CpuSet::parse(&config.dedup_apply_cpus),
            refcount_drainer: CpuSet::parse(&config.refcount_drainer_cpus),
            l2p_compactor: CpuSet::parse(&config.l2p_compactor_cpus),
            io_submitter: CpuSet::parse(&config.io_submitter_cpus),
        })
        .filter(|layout| {
            !layout.wal.cpus.is_empty()
                || !layout.l2p_apply.cpus.is_empty()
                || !layout.refcount_apply.cpus.is_empty()
                || !layout.dedup_apply.cpus.is_empty()
                || !layout.refcount_drainer.cpus.is_empty()
                || !layout.l2p_compactor.cpus.is_empty()
                || !layout.io_submitter.cpus.is_empty()
        })
    }

    fn cpus_for(&self, role: ThreadRole) -> &CpuSet {
        match role {
            ThreadRole::Wal => &self.wal,
            ThreadRole::L2pApply => &self.l2p_apply,
            ThreadRole::RefcountApply => &self.refcount_apply,
            ThreadRole::DedupApply => &self.dedup_apply,
            ThreadRole::RefcountDrainer => &self.refcount_drainer,
            ThreadRole::TxgSync => &self.l2p_compactor,
            ThreadRole::IoSubmitter => &self.io_submitter,
        }
    }
}

impl CpuSet {
    fn parse(spec: &str) -> Self {
        let mut cpus = Vec::new();
        for part in spec.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            if let Some((start, end)) = part.split_once('-') {
                let Ok(start) = start.trim().parse::<usize>() else {
                    tracing::warn!(spec, part, "ignoring invalid metadb CPU range start");
                    continue;
                };
                let Ok(end) = end.trim().parse::<usize>() else {
                    tracing::warn!(spec, part, "ignoring invalid metadb CPU range end");
                    continue;
                };
                if start > end {
                    tracing::warn!(spec, part, "ignoring descending metadb CPU range");
                    continue;
                }
                cpus.extend(start..=end);
            } else if let Ok(cpu) = part.parse::<usize>() {
                cpus.push(cpu);
            } else {
                tracing::warn!(spec, part, "ignoring invalid metadb CPU entry");
            }
        }
        cpus.sort_unstable();
        cpus.dedup();
        Self { cpus }
    }

    fn pick(&self, ordinal: usize) -> Option<usize> {
        if self.cpus.is_empty() {
            None
        } else {
            Some(self.cpus[ordinal % self.cpus.len()])
        }
    }
}

#[cfg(target_os = "linux")]
fn set_current_cpu(cpu: usize) -> std::io::Result<()> {
    const CPU_SETSIZE: usize = 1024;
    const BITS_PER_WORD: usize = 8 * std::mem::size_of::<libc::c_ulong>();
    let mut set = [0 as libc::c_ulong; CPU_SETSIZE / BITS_PER_WORD];
    if cpu >= CPU_SETSIZE {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("cpu {cpu} >= CPU_SETSIZE {CPU_SETSIZE}"),
        ));
    }
    set[cpu / BITS_PER_WORD] |= (1 as libc::c_ulong) << (cpu % BITS_PER_WORD);
    let rc = unsafe {
        libc::sched_setaffinity(
            0,
            std::mem::size_of_val(&set),
            set.as_ptr().cast::<libc::cpu_set_t>(),
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn set_current_cpu(_cpu: usize) -> std::io::Result<()> {
    Ok(())
}

/// Widen the calling thread's CPU affinity to ALL CPUs, clearing any
/// inherited single-CPU pin. Worker threads spawned from a `bind_current`-
/// pinned parent (e.g. the parallel L2P drain fanned out from the pinned
/// `metadb-txg-sync` thread) inherit that one-CPU mask and would otherwise
/// pile onto a single core instead of spreading — the ZFS `dp_sync_taskq`
/// runs its sync threads at normal priority across all CPUs, not pinned.
pub(crate) fn unbind_current() {
    if let Err(err) = set_current_all_cpus() {
        tracing::warn!(error = %err, "failed to widen metadb thread CPU affinity");
    }
}

#[cfg(target_os = "linux")]
fn set_current_all_cpus() -> std::io::Result<()> {
    const CPU_SETSIZE: usize = 1024;
    const BITS_PER_WORD: usize = 8 * std::mem::size_of::<libc::c_ulong>();
    // All-ones mask = run on any CPU (kernel ignores bits past online CPUs).
    let set = [!(0 as libc::c_ulong); CPU_SETSIZE / BITS_PER_WORD];
    let rc = unsafe {
        libc::sched_setaffinity(
            0,
            std::mem::size_of_val(&set),
            set.as_ptr().cast::<libc::cpu_set_t>(),
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn set_current_all_cpus() -> std::io::Result<()> {
    Ok(())
}
