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
    /// CPU set for the BFG background workers — the
    /// `BfgSyncThread` (single serial thread that drains the syncing
    /// BFG slot per shard, writes RC + L2P checkpoint pages, fsyncs,
    /// commits the manifest) plus the `BfgQuiesceThread` (also a
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
    /// BFG sync + quiesce workers. Bound to a small
    /// dedicated CPU set so the kernel scheduler cannot co-locate
    /// them on an apply-lane CPU during a flush window. Replaces the
    /// retired `L2pCompactor` role; the affinity config field
    /// (`l2p_compactor_cpus`) keeps its legacy name for backward
    /// compatibility with operator configs.
    BfgSync,
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
    /// Backs `ThreadRole::BfgSync`. Field name kept for backward
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
/// BfgSync, IoSubmitter) bind to the home pod.
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
            ThreadRole::Wal | ThreadRole::BfgSync | ThreadRole::IoSubmitter => self.home_pod,
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

/// Placement for the parallel L2P BFG-drain workers (one per shard,
/// scope-spawned from `metadb-bfg-sync`). These workers are checkpoint
/// background work: partition mode keeps them on the BFG home pod, an explicit
/// per-role layout uses the complete compactor CPU set, and confine/unconfigured
/// mode preserves the exact mask inherited from the parent.
pub(crate) fn bind_for_l2p_drain(shard_idx: usize) {
    match select_l2p_drain_placement(
        NODE_LAYOUT.get(),
        LAYOUT.get().and_then(Option::as_ref),
        shard_idx,
    ) {
        L2pDrainPlacement::CpuSet {
            cpus,
            preferred_node,
        } => {
            if let Some(node) = preferred_node {
                if let Err(err) = set_thread_preferred_node(node) {
                    tracing::warn!(
                        shard_idx,
                        node,
                        error = %err,
                        "failed to set L2P drain worker memory policy"
                    );
                }
            }
            if let Err(err) = set_current_cpus(cpus) {
                tracing::warn!(
                    shard_idx,
                    cpus = ?cpus,
                    error = %err,
                    "failed to bind L2P drain worker; preserving inherited affinity"
                );
            }
        }
        L2pDrainPlacement::Inherit => {}
    }
}

#[derive(Debug, PartialEq, Eq)]
enum L2pDrainPlacement<'a> {
    Inherit,
    CpuSet {
        cpus: &'a [usize],
        preferred_node: Option<usize>,
    },
}

fn select_l2p_drain_placement<'a>(
    nodes: Option<&'a NodeAffinityConfig>,
    layout: Option<&'a AffinityLayout>,
    shard_idx: usize,
) -> L2pDrainPlacement<'a> {
    if let Some(nodes) = nodes {
        if !nodes.pods.is_empty() {
            let pod = nodes.pod_for(ThreadRole::BfgSync, shard_idx);
            return L2pDrainPlacement::CpuSet {
                cpus: &pod.cpus,
                preferred_node: Some(pod.node),
            };
        }
    }
    if let Some(layout) = layout {
        if !layout.l2p_compactor.cpus.is_empty() {
            return L2pDrainPlacement::CpuSet {
                cpus: &layout.l2p_compactor.cpus,
                preferred_node: None,
            };
        }
    }
    L2pDrainPlacement::Inherit
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
            ThreadRole::BfgSync => &self.l2p_compactor,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn per_role_l2p_drain_uses_the_complete_compactor_cpu_set() {
        let layout = AffinityLayout::from_config(AffinityConfig {
            l2p_compactor_cpus: "3-5,9".into(),
            ..AffinityConfig::default()
        })
        .unwrap();

        assert_eq!(
            select_l2p_drain_placement(None, Some(&layout), 7),
            L2pDrainPlacement::CpuSet {
                cpus: &[3, 4, 5, 9],
                preferred_node: None,
            }
        );
    }

    #[test]
    fn node_layout_routes_l2p_drain_to_the_bfg_home_pod() {
        let layout = NodeAffinityConfig {
            pods: vec![
                NodePod {
                    node: 0,
                    cpus: vec![0, 2],
                },
                NodePod {
                    node: 1,
                    cpus: vec![4, 6],
                },
            ],
            home_pod: 1,
            shard_pods: vec![0],
            dedup_shard_pods: vec![0],
        };

        assert_eq!(
            select_l2p_drain_placement(Some(&layout), None, 0),
            L2pDrainPlacement::CpuSet {
                cpus: &[4, 6],
                preferred_node: Some(1),
            }
        );
    }

    #[test]
    fn unconfigured_l2p_drain_selects_inherited_affinity() {
        assert_eq!(
            select_l2p_drain_placement(None, None, 0),
            L2pDrainPlacement::Inherit
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn unconfigured_l2p_drain_preserves_the_inherited_cpu_mask() {
        assert!(NODE_LAYOUT.get().is_none());
        assert!(LAYOUT.get().is_none());
        let available = current_cpu_set().unwrap();
        if available.len() < 3 {
            return;
        }
        let inherited = available[..2].to_vec();
        let expected = inherited.clone();

        let observed = std::thread::spawn(move || {
            set_current_cpus(&inherited).unwrap();
            bind_for_l2p_drain(0);
            current_cpu_set().unwrap()
        })
        .join()
        .unwrap();

        assert_eq!(observed, expected);
    }

    #[cfg(target_os = "linux")]
    fn current_cpu_set() -> std::io::Result<Vec<usize>> {
        const CPU_SETSIZE: usize = 1024;
        const BITS_PER_WORD: usize = 8 * std::mem::size_of::<libc::c_ulong>();
        let mut set = [0 as libc::c_ulong; CPU_SETSIZE / BITS_PER_WORD];
        let rc = unsafe {
            libc::sched_getaffinity(
                0,
                std::mem::size_of_val(&set),
                set.as_mut_ptr().cast::<libc::cpu_set_t>(),
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok((0..CPU_SETSIZE)
            .filter(|cpu| set[cpu / BITS_PER_WORD] & (1 << (cpu % BITS_PER_WORD)) != 0)
            .collect())
    }
}
