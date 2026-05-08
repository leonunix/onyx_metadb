//! In-memory reference model for the Onyx integration workload.
//!
//! The model intentionally mirrors the public metadb/Onyx semantics at
//! the operation boundary, not the internal page layout. It is used by
//! the mixed proptest and by `metadb-soak` to catch cross-op refcount,
//! snapshot, and dedup-cleanup drift.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    ApplyOutcome, Db, DedupValue, Hash8, L2pValue, Pba, Result, SnapshotId, VolumeOrdinal,
};

pub const BOOTSTRAP_VOL: VolumeOrdinal = 0;
const LEAF_SHIFT: u32 = 7;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FrozenSnapshot {
    pub id: SnapshotId,
    pub base_vol: VolumeOrdinal,
    pub l2p: BTreeMap<u64, L2pValue>,
}

#[derive(Clone, Debug)]
pub struct OnyxRefModel {
    live_volumes: BTreeSet<VolumeOrdinal>,
    l2p: BTreeMap<(VolumeOrdinal, u64), L2pValue>,
    refcount: BTreeMap<Pba, u32>,
    dedup: BTreeMap<Hash8, DedupValue>,
    dedup_reverse: BTreeSet<(Pba, Hash8)>,
    snapshots: BTreeMap<SnapshotId, FrozenSnapshot>,
    shared_leaves: BTreeSet<(VolumeOrdinal, u64)>,
    pending_dead_pbas: BTreeSet<Pba>,
    next_snapshot_id: SnapshotId,
}

impl Default for OnyxRefModel {
    fn default() -> Self {
        let mut live_volumes = BTreeSet::new();
        live_volumes.insert(BOOTSTRAP_VOL);
        Self {
            live_volumes,
            l2p: BTreeMap::new(),
            refcount: BTreeMap::new(),
            dedup: BTreeMap::new(),
            dedup_reverse: BTreeSet::new(),
            snapshots: BTreeMap::new(),
            shared_leaves: BTreeSet::new(),
            pending_dead_pbas: BTreeSet::new(),
            next_snapshot_id: 1,
        }
    }
}

impl OnyxRefModel {
    pub fn live_volumes(&self) -> Vec<VolumeOrdinal> {
        self.live_volumes.iter().copied().collect()
    }

    pub fn snapshots(&self) -> impl Iterator<Item = &FrozenSnapshot> {
        self.snapshots.values()
    }

    pub fn l2p(&self) -> &BTreeMap<(VolumeOrdinal, u64), L2pValue> {
        &self.l2p
    }

    pub fn refcount(&self) -> &BTreeMap<Pba, u32> {
        &self.refcount
    }

    pub fn dedup(&self) -> &BTreeMap<Hash8, DedupValue> {
        &self.dedup
    }

    pub fn pending_dead_pbas(&self) -> Vec<Pba> {
        self.pending_dead_pbas.iter().copied().collect()
    }

    pub fn refcount_sum(&self) -> u64 {
        self.refcount.values().map(|v| u64::from(*v)).sum()
    }

    pub fn contains_volume(&self, vol: VolumeOrdinal) -> bool {
        self.live_volumes.contains(&vol)
    }

    pub fn current_refcount(&self, pba: Pba) -> u32 {
        self.refcount.get(&pba).copied().unwrap_or(0)
    }

    pub fn get(&self, vol: VolumeOrdinal, lba: u64) -> Option<L2pValue> {
        self.l2p.get(&(vol, lba)).copied()
    }

    pub fn insert_raw(&mut self, vol: VolumeOrdinal, lba: u64, value: L2pValue) {
        self.live_volumes.insert(vol);
        self.l2p.insert((vol, lba), value);
    }

    pub fn set_refcount_raw(&mut self, pba: Pba, count: u32) {
        if count == 0 {
            self.refcount.remove(&pba);
        } else {
            self.refcount.insert(pba, count);
        }
    }

    pub fn put_dedup_raw(&mut self, hash: Hash8, value: DedupValue) {
        let pba = value.head_pba();
        if let Some(old) = self.dedup.insert(hash, value) {
            self.dedup_reverse.remove(&(old.head_pba(), hash));
        }
        self.dedup_reverse.insert((pba, hash));
    }

    pub fn register_dedup_raw(&mut self, pba: Pba, hash: Hash8) {
        self.dedup_reverse.insert((pba, hash));
    }

    pub fn create_volume(&mut self, ord: VolumeOrdinal) -> bool {
        self.live_volumes.insert(ord)
    }

    pub fn drop_volume(&mut self, ord: VolumeOrdinal) -> bool {
        if ord == BOOTSTRAP_VOL || !self.live_volumes.remove(&ord) {
            return false;
        }
        let keys: Vec<_> = self
            .l2p
            .keys()
            .filter(|(vol, _)| *vol == ord)
            .copied()
            .collect();
        for key in keys {
            if let Some(value) = self.l2p.remove(&key) {
                self.decref_one(value.head_pba());
            }
        }
        true
    }

    pub fn apply_l2p_remap(
        &mut self,
        vol: VolumeOrdinal,
        lba: u64,
        new_value: L2pValue,
        guard: Option<(Pba, u32)>,
    ) -> ModelRemapOutcome {
        if !self.live_volumes.contains(&vol) {
            return ModelRemapOutcome::invalid();
        }
        if let Some((pba, min_rc)) = guard {
            if self.current_refcount(pba) < min_rc {
                return ModelRemapOutcome {
                    applied: false,
                    prev: None,
                    freed_pba: None,
                    invalid: false,
                };
            }
        }

        let key = (vol, lba);
        let prev = self.l2p.insert(key, new_value);
        let new_pba = new_value.head_pba();
        let old_pba = prev.map(|value| value.head_pba());
        let leaf_idx = lba >> LEAF_SHIFT;
        let shared = prev.is_some() && self.leaf_covered_by_snapshot(vol, lba);
        let do_decref = prev.is_some() && !shared && old_pba != Some(new_pba);
        let do_incref = !(prev.is_some() && !shared && old_pba == Some(new_pba));
        let freed_pba = if do_decref {
            self.decref_one(old_pba.unwrap())
        } else {
            None
        };
        if do_incref {
            self.incref_one(new_pba);
        }
        self.shared_leaves.remove(&(vol, leaf_idx));
        ModelRemapOutcome {
            applied: true,
            prev,
            freed_pba,
            invalid: false,
        }
    }

    pub fn apply_range_delete(&mut self, vol: VolumeOrdinal, start: u64, end: u64) -> Vec<Pba> {
        if start >= end || !self.live_volumes.contains(&vol) {
            return Vec::new();
        }
        let keys: Vec<_> = self
            .l2p
            .range((vol, start)..(vol, end))
            .map(|(key, _)| *key)
            .collect();
        let mut freed = Vec::new();
        let mut touched_leaves = BTreeSet::new();
        for (key_vol, lba) in keys {
            if let Some(value) = self.l2p.remove(&(key_vol, lba)) {
                if !self.leaf_covered_by_snapshot(key_vol, lba) {
                    if let Some(pba) = self.decref_one(value.head_pba()) {
                        freed.push(pba);
                    }
                }
                touched_leaves.insert((key_vol, lba >> LEAF_SHIFT));
            }
        }
        for leaf in touched_leaves {
            self.shared_leaves.remove(&leaf);
        }
        freed
    }

    pub fn take_snapshot_with_id(
        &mut self,
        vol: VolumeOrdinal,
        id: SnapshotId,
    ) -> Option<FrozenSnapshot> {
        if !self.live_volumes.contains(&vol) {
            return None;
        }
        self.next_snapshot_id = self.next_snapshot_id.max(id.saturating_add(1));
        let l2p = self.volume_l2p(vol);
        for lba in l2p.keys() {
            self.shared_leaves.insert((vol, *lba >> LEAF_SHIFT));
        }
        let snapshot = FrozenSnapshot {
            id,
            base_vol: vol,
            l2p,
        };
        self.snapshots.insert(id, snapshot.clone());
        Some(snapshot)
    }

    pub fn drop_snapshot(&mut self, id: SnapshotId) -> Vec<Pba> {
        self.snapshots.remove(&id);
        Vec::new()
    }

    pub fn clone_volume_from_snapshot(
        &mut self,
        snapshot_id: SnapshotId,
        new_ord: VolumeOrdinal,
    ) -> bool {
        let Some(snapshot) = self.snapshots.get(&snapshot_id).cloned() else {
            return false;
        };
        self.drop_volume(new_ord);
        self.live_volumes.insert(new_ord);
        for (lba, value) in snapshot.l2p {
            self.l2p.insert((new_ord, lba), value);
            self.shared_leaves.insert((new_ord, lba >> LEAF_SHIFT));
        }
        true
    }

    pub fn cleanup_dedup_for_dead_pbas(&mut self, pbas: &[Pba]) -> usize {
        let mut removed_forward = 0usize;
        for &pba in pbas {
            let hashes: Vec<Hash8> = self
                .dedup_reverse
                .iter()
                .filter_map(|(entry_pba, hash)| (*entry_pba == pba).then_some(*hash))
                .collect();
            for hash in hashes {
                if self
                    .dedup
                    .get(&hash)
                    .is_some_and(|value| value.head_pba() == pba)
                {
                    self.dedup.remove(&hash);
                    removed_forward += 1;
                }
                self.dedup_reverse.remove(&(pba, hash));
            }
            self.pending_dead_pbas.remove(&pba);
        }
        removed_forward
    }

    pub fn volume_l2p(&self, vol: VolumeOrdinal) -> BTreeMap<u64, L2pValue> {
        self.l2p
            .range((vol, 0)..=(vol, u64::MAX))
            .map(|((_, lba), value)| (*lba, *value))
            .collect()
    }

    pub fn assert_db_matches(&self, db: &Db) -> Result<()> {
        let db_refcounts: BTreeMap<Pba, u32> = db
            .iter_refcounts()?
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect();
        if db_refcounts != self.refcount {
            return Err(crate::MetaDbError::Corruption(format!(
                "refcount diverged: db={db_refcounts:?} model={:?}",
                self.refcount
            )));
        }

        let db_dedup: BTreeMap<Hash8, DedupValue> = db
            .iter_dedup()?
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .collect();
        if db_dedup != self.dedup {
            return Err(crate::MetaDbError::Corruption(
                "dedup forward diverged".into(),
            ));
        }

        for &vol in &self.live_volumes {
            let got: Vec<(u64, L2pValue)> = db.range(vol, ..)?.collect::<Result<Vec<_>>>()?;
            let want: Vec<(u64, L2pValue)> = self.volume_l2p(vol).into_iter().collect();
            if got != want {
                let first_diff = got
                    .iter()
                    .zip(want.iter())
                    .enumerate()
                    .find(|(_, (got, want))| got != want)
                    .map(|(idx, (got, want))| format!("idx={idx} got={got:?} want={want:?}"))
                    .unwrap_or_else(|| {
                        format!(
                            "common prefix matched; got_len={} want_len={}",
                            got.len(),
                            want.len()
                        )
                    });
                return Err(crate::MetaDbError::Corruption(format!(
                    "volume {vol} l2p diverged: got_len={} want_len={} first_diff={first_diff}",
                    got.len(),
                    want.len()
                )));
            }
        }

        for snapshot in self.snapshots.values() {
            let Some(view) = db.snapshot_view(snapshot.id) else {
                return Err(crate::MetaDbError::Corruption(format!(
                    "snapshot {} missing",
                    snapshot.id
                )));
            };
            let got: Vec<(u64, L2pValue)> = view.range(..)?.collect::<Result<Vec<_>>>()?;
            let want: Vec<(u64, L2pValue)> = snapshot.l2p.iter().map(|(k, v)| (*k, *v)).collect();
            if got != want {
                return Err(crate::MetaDbError::Corruption(format!(
                    "snapshot {} diverged",
                    snapshot.id
                )));
            }
        }
        Ok(())
    }

    fn leaf_covered_by_snapshot(&self, vol: VolumeOrdinal, lba: u64) -> bool {
        let leaf_idx = lba >> LEAF_SHIFT;
        self.shared_leaves.contains(&(vol, leaf_idx))
    }

    fn incref_one(&mut self, pba: Pba) {
        *self.refcount.entry(pba).or_insert(0) += 1;
    }

    fn decref_one(&mut self, pba: Pba) -> Option<Pba> {
        let Some(cur) = self.refcount.get_mut(&pba) else {
            return None;
        };
        if *cur == 0 {
            return None;
        }
        *cur -= 1;
        if *cur == 0 {
            self.refcount.remove(&pba);
            self.pending_dead_pbas.insert(pba);
            Some(pba)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModelRemapOutcome {
    pub applied: bool,
    pub prev: Option<L2pValue>,
    pub freed_pba: Option<Pba>,
    pub invalid: bool,
}

impl ModelRemapOutcome {
    fn invalid() -> Self {
        Self {
            applied: false,
            prev: None,
            freed_pba: None,
            invalid: true,
        }
    }

    pub fn matches_apply(&self, outcome: &ApplyOutcome) -> bool {
        matches!(
            (self, outcome),
            (
                ModelRemapOutcome { applied, prev, freed_pba, invalid: false },
                ApplyOutcome::L2pRemap { applied: got_applied, prev: got_prev, freed_pba: got_freed }
            ) if applied == got_applied && prev == got_prev && freed_pba == got_freed
        )
    }
}

pub fn onyx_l2p_value(pba: Pba, salt: u64) -> L2pValue {
    let mut bytes = [0u8; 28];
    bytes[..8].copy_from_slice(&pba.to_be_bytes());
    bytes[8..16].copy_from_slice(&salt.to_be_bytes());
    bytes[16..20].copy_from_slice(&(salt as u32).wrapping_mul(0x9E37_79B9).to_be_bytes());
    bytes[20..22].copy_from_slice(&((salt as u16) & 0x0fff).to_be_bytes());
    bytes[22..24].copy_from_slice(&(((salt >> 16) as u16) & 0x0fff).to_be_bytes());
    bytes[24..28].copy_from_slice(&(salt as u32 ^ 0xA5A5_5A5A).to_be_bytes());
    // Bit 1 of byte 27 is the Onyx-side "is_zero" flag — when set, the
    // L2P entry represents a zero-fill marker and `apply_l2p_remap`
    // skips incref/decref. The reference model in this file doesn't
    // model that flag (these proptests exercise non-zero remaps), so
    // mask it off so refcount semantics match the model.
    bytes[27] &= !0x02;
    L2pValue(bytes)
}

pub fn onyx_hash(seed: u64) -> Hash8 {
    // `Hash8` is 8 bytes (xxh3_64) since commit e7cfeab. The seed is
    // already u64-wide, so a single mix is enough to spread the
    // proptest's small seed values across the fingerprint space.
    let mixed = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ seed.rotate_left(17) ^ !seed;
    mixed.to_be_bytes()
}

pub fn onyx_dedup_value(pba: Pba, salt: u64) -> DedupValue {
    let mut bytes = [0u8; 28];
    bytes[..8].copy_from_slice(&pba.to_be_bytes());
    bytes[8..16].copy_from_slice(&salt.to_be_bytes());
    bytes[16..24].copy_from_slice(&salt.rotate_right(7).to_be_bytes());
    bytes[24..28].copy_from_slice(&(salt as u32).to_be_bytes());
    DedupValue(bytes)
}
