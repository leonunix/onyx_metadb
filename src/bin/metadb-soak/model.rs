struct Model {
    l2p: BTreeMap<VolumeOrdinal, BTreeMap<u64, L2pValue>>,
    dedup: BTreeMap<Hash8, DedupValue>,
    refcount: BTreeMap<u64, u32>,
    /// BFG: ords of clones not yet promoted to independence.
    /// A clone enters on `CLONE_VOLUME`, leaves on `PROMOTE` (now
    /// independent) or `DROP_VOLUME`. Promotion is metadb-internal lite
    /// bookkeeping (bumps global PBA rc + clears `parent_vol_ord`) and does
    /// NOT change L2P content, so it never touches the `l2p` reference model.
    clones: BTreeSet<VolumeOrdinal>,
}

impl Default for Model {
    fn default() -> Self {
        let mut l2p = BTreeMap::new();
        l2p.insert(BOOTSTRAP_VOL, BTreeMap::new());
        Self {
            l2p,
            dedup: BTreeMap::new(),
            refcount: BTreeMap::new(),
            clones: BTreeSet::new(),
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

    /// Un-promoted clones still present in the model — promotion candidates.
    fn promote_candidates(&self) -> Vec<VolumeOrdinal> {
        self.clones
            .iter()
            .copied()
            .filter(|ord| self.l2p.contains_key(ord))
            .collect()
    }
}

#[derive(Clone, Debug)]
enum WorkerOpKind {
    Insert(u8),
    Delete,
    PutDedup(u8),
    DeleteDedup,
    Get,
    OnyxRemap { pba: Pba, salt: u64, guard: u8 },
    OnyxRangeDelete { len: u64 },
    OnyxDedupHit { pba: Pba, salt: u64 },
    OnyxCleanup { pba: Pba },
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
    Onyx(u64, String),
    Error(u64, String),
}

#[derive(Clone, Debug)]
enum WorkerJob {
    Exec { id: u64, op: WorkerOp },
    Stop,
}
