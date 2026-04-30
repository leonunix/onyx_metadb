use std::sync::OnceLock;

#[derive(Clone, Debug, Default)]
pub struct AffinityConfig {
    pub wal_cpus: String,
    pub l2p_apply_cpus: String,
    pub refcount_apply_cpus: String,
    pub dedup_apply_cpus: String,
}

#[derive(Clone, Copy, Debug)]
pub enum ThreadRole {
    Wal,
    L2pApply,
    RefcountApply,
    DedupApply,
}

#[derive(Clone, Debug, Default)]
struct AffinityLayout {
    wal: CpuSet,
    l2p_apply: CpuSet,
    refcount_apply: CpuSet,
    dedup_apply: CpuSet,
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
        })
        .filter(|layout| {
            !layout.wal.cpus.is_empty()
                || !layout.l2p_apply.cpus.is_empty()
                || !layout.refcount_apply.cpus.is_empty()
                || !layout.dedup_apply.cpus.is_empty()
        })
    }

    fn cpus_for(&self, role: ThreadRole) -> &CpuSet {
        match role {
            ThreadRole::Wal => &self.wal,
            ThreadRole::L2pApply => &self.l2p_apply,
            ThreadRole::RefcountApply => &self.refcount_apply,
            ThreadRole::DedupApply => &self.dedup_apply,
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

