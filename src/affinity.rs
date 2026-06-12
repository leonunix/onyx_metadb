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
    /// Per-dedup-shard background drainer threads. Bound to the same
    /// CPU set as `DedupApply` (`dedup_apply_cpus`): the dedup apply
    /// lanes are idle on the `stage_ops` hot path, so their NUMA-local
    /// CPU set is free for the drainer, and reusing it co-locates the
    /// drainer with dedup work on the right NUMA node without a new
    /// affinity config knob.
    DedupDrainer,
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
static NODE_LAYOUT: OnceLock<NodeAffinityConfig> = OnceLock::new();

/// One NUMA "pod": a data node's engine CPU pool.
#[derive(Clone, Debug)]
pub struct NodePod {
    pub node: usize,
    pub cpus: Vec<usize>,
}

/// NUMA-partitioned placement (set by the embedding engine, e.g. onyx
/// `[numa] mode = "partition"`). Takes precedence over the string-based
/// `AffinityConfig`: shard-indexed roles bind to their shard's pod CPU SET
/// (not a single CPU) and set their own memory policy to prefer the pod's
/// node so per-shard structures first-touch locally; singletons (WAL,
/// TxgSync, IoSubmitter) bind to the home pod.
#[derive(Clone, Debug)]
pub struct NodeAffinityConfig {
    pub pods: Vec<NodePod>,
    pub home_pod: usize,
    /// L2P/refcount shard index → pod index (len = shards_per_partition).
    pub shard_pods: Vec<usize>,
    /// Dedup shard index → pod index (len = dedup_shards).
    pub dedup_shard_pods: Vec<usize>,
}

pub fn configure(config: AffinityConfig) {
    let _ = LAYOUT.set(AffinityLayout::from_config(config));
}

pub fn configure_nodes(config: NodeAffinityConfig) {
    let _ = NODE_LAYOUT.set(config);
}

impl NodeAffinityConfig {
    fn pod_for(&self, role: ThreadRole, ordinal: usize) -> &NodePod {
        let idx = match role {
            ThreadRole::L2pApply | ThreadRole::RefcountApply | ThreadRole::RefcountDrainer => {
                self.shard_pods[ordinal % self.shard_pods.len().max(1)]
            }
            ThreadRole::DedupApply | ThreadRole::DedupDrainer => {
                self.dedup_shard_pods[ordinal % self.dedup_shard_pods.len().max(1)]
            }
            ThreadRole::Wal | ThreadRole::TxgSync | ThreadRole::IoSubmitter => self.home_pod,
        };
        &self.pods[idx.min(self.pods.len() - 1)]
    }
}

pub(crate) fn bind_current(role: ThreadRole, ordinal: usize) {
    if let Some(nodes) = NODE_LAYOUT.get() {
        if !nodes.pods.is_empty() {
            let pod = nodes.pod_for(role, ordinal);
            if let Err(err) = set_thread_preferred_node(pod.node) {
                tracing::warn!(?role, ordinal, node = pod.node, error = %err,
                    "failed to set metadb thread memory policy");
            }
            if let Err(err) = set_current_cpus(&pod.cpus) {
                tracing::warn!(?role, ordinal, error = %err,
                    "failed to set metadb thread CPU affinity");
            }
            return;
        }
    }
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

/// Placement for the parallel L2P TXG-drain workers (one per shard,
/// scope-spawned from the pinned `metadb-txg-sync` thread). Under NUMA
/// partition each worker binds to its shard's pod so the COW fold touches
/// node-local pages; otherwise fall back to the legacy "widen to all CPUs"
/// (the ZFS `dp_sync_taskq` analog) to escape the inherited single-CPU pin.
pub(crate) fn bind_for_l2p_drain(shard_idx: usize) {
    if let Some(nodes) = NODE_LAYOUT.get() {
        if !nodes.pods.is_empty() {
            let pod = nodes.pod_for(ThreadRole::L2pApply, shard_idx);
            let _ = set_thread_preferred_node(pod.node);
            if set_current_cpus(&pod.cpus).is_ok() {
                return;
            }
        }
    }
    unbind_current();
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
            ThreadRole::DedupDrainer => &self.dedup_apply,
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

#[cfg(target_os = "linux")]
fn set_current_cpus(cpus: &[usize]) -> std::io::Result<()> {
    const CPU_SETSIZE: usize = 1024;
    const BITS_PER_WORD: usize = 8 * std::mem::size_of::<libc::c_ulong>();
    if cpus.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "empty cpu set",
        ));
    }
    let mut set = [0 as libc::c_ulong; CPU_SETSIZE / BITS_PER_WORD];
    for &cpu in cpus {
        if cpu >= CPU_SETSIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("cpu {cpu} >= CPU_SETSIZE {CPU_SETSIZE}"),
            ));
        }
        set[cpu / BITS_PER_WORD] |= (1 as libc::c_ulong) << (cpu % BITS_PER_WORD);
    }
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
fn set_current_cpus(_cpus: &[usize]) -> std::io::Result<()> {
    Ok(())
}

/// Set the calling thread's memory policy to prefer `node` (children
/// inherit). PREFERRED, not BIND: when the node fills the kernel spills
/// instead of stalling in local direct reclaim.
#[cfg(target_os = "linux")]
fn set_thread_preferred_node(node: usize) -> std::io::Result<()> {
    const MPOL_PREFERRED: i32 = 1;
    if node >= 64 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("node {node} >= 64 unsupported"),
        ));
    }
    let mask: u64 = 1 << node;
    let rc = unsafe {
        libc::syscall(
            libc::SYS_set_mempolicy,
            MPOL_PREFERRED,
            &mask as *const u64,
            65usize,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn set_thread_preferred_node(_node: usize) -> std::io::Result<()> {
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
