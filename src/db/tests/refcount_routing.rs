use super::*;

use crate::manifest::{LEGACY_MANIFEST_BODY_VERSION, MANIFEST_BODY_VERSION};
use crate::refcount::RefcountRouting;

const V26_PAGE_ENTRIES: u64 = crate::refcount::REFCOUNT_V26_ROUTING_ENTRIES_PER_PAGE as u64;

fn dedup_value_for_pba(pba: Pba) -> DedupValue {
    let mut bytes = [0u8; 28];
    bytes[..8].copy_from_slice(&pba.to_be_bytes());
    DedupValue(bytes)
}

fn assert_free_outcome(outcome: ApplyOutcome, expected: Pba) {
    match outcome {
        ApplyOutcome::FreePbas { freed_pbas } => {
            assert_eq!(freed_pbas.as_ref(), &[expected]);
        }
        other => panic!("expected FreePbas outcome, got {other:?}"),
    }
}

#[test]
fn page_affine_routes_every_entry_in_one_refcount_page_together() {
    const SHARDS: usize = 16;
    let first = 17 * V26_PAGE_ENTRIES;
    let expected = RefcountRouting::PageAffine.shard_for_pba(first, SHARDS);

    for pba in first..first + V26_PAGE_ENTRIES {
        assert_eq!(
            RefcountRouting::PageAffine.shard_for_pba(pba, SHARDS),
            expected,
            "pba {pba} escaped its refcount-page owner"
        );
    }
}

#[test]
fn v26_durable_routing_divisor_matches_refcount_array_layout() {
    assert_eq!(
        crate::refcount::REFCOUNT_V26_ROUTING_ENTRIES_PER_PAGE,
        crate::refcount::ENTRIES_PER_PAGE
    );
}

#[test]
fn planner_and_live_dedup_apply_use_the_same_page_affine_shard() {
    let (_dir, db) = mk_db_with_shards(16);
    let pba = 41 * V26_PAGE_ENTRIES + 73;
    let value = dedup_value_for_pba(pba);
    let op = WalOp::DedupPut {
        hash: h(0xabc),
        value,
        old_pba: None,
    };
    let volumes = db.volumes.read().clone();
    let plan = db
        .build_lane_dispatch_plan(&volumes, std::slice::from_ref(&op))
        .unwrap();
    let expected = db.refcount_shard_for(pba);

    for (sid, enqueued) in plan.rc_enqueued.iter().copied().enumerate() {
        assert_eq!(enqueued, sid == expected, "planner RC footprint mismatch");
    }
    db.commit_ops(&[op]).unwrap();
    db.wait_apply_idle();
    assert_eq!(db.get_refcount(pba).unwrap(), 1);
    for (sid, shard) in db.refcount_shards.iter().enumerate() {
        assert_eq!(
            shard.rc.get(pba).unwrap(),
            u32::from(sid == expected),
            "live apply routed PBA to shard {sid}, expected {expected}"
        );
    }
}

#[test]
fn page_affine_allocates_one_array_page_for_one_complete_pba_page() {
    let (_dir, db) = mk_db_with_shards(16);
    let first = 23 * V26_PAGE_ENTRIES;
    let pbas: Vec<Pba> = (first..first + V26_PAGE_ENTRIES).collect();

    db.commit_promotion_chunk(0, pbas, None).unwrap();
    db.flush().unwrap();

    let allocated: usize = db
        .refcount_shards
        .iter()
        .map(|shard| shard.rc.allocated_data_pages())
        .sum();
    assert_eq!(allocated, 1);
    assert_eq!(
        db.manifest_state.lock().manifest.body_version,
        MANIFEST_BODY_VERSION
    );
}

#[test]
fn page_affine_v26_reopens_refcount_birth_and_free_outcome() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.shards_per_partition = 16;
    let page_first = 31 * V26_PAGE_ENTRIES;
    let pba0 = page_first + 7;
    let pba1 = page_first + 209;
    let birth0;
    let birth1;

    {
        let db = Db::create_with_config(cfg.clone()).unwrap();
        assert_eq!(db.refcount_routing(), RefcountRouting::PageAffine);
        assert_eq!(db.refcount_shard_for(pba0), db.refcount_shard_for(pba1));
        db.incref_pba(pba0, 1).unwrap();
        db.incref_pba(pba1, 2).unwrap();
        birth0 = db.refcount_shards[db.refcount_shard_for(pba0)]
            .rc
            .get_entry(pba0)
            .unwrap()
            .birth_lsn;
        birth1 = db.refcount_shards[db.refcount_shard_for(pba1)]
            .rc
            .get_entry(pba1)
            .unwrap()
            .birth_lsn;
        db.flush().unwrap();
        assert_eq!(
            db.manifest_state.lock().manifest.body_version,
            MANIFEST_BODY_VERSION
        );
    }

    {
        let db = Db::open_with_config(cfg.clone()).unwrap();
        assert_eq!(db.refcount_routing(), RefcountRouting::PageAffine);
        assert_eq!(db.get_refcount(pba0).unwrap(), 1);
        assert_eq!(db.get_refcount(pba1).unwrap(), 2);
        assert_eq!(
            db.refcount_shards[db.refcount_shard_for(pba0)]
                .rc
                .get_entry(pba0)
                .unwrap()
                .birth_lsn,
            birth0
        );
        assert_eq!(
            db.refcount_shards[db.refcount_shard_for(pba1)]
                .rc
                .get_entry(pba1)
                .unwrap()
                .birth_lsn,
            birth1
        );
        assert_free_outcome(db.commit_free_pbas(0, &[pba0]).unwrap(), pba0);
        db.flush().unwrap();
    }

    let reopened = Db::open_with_config(cfg).unwrap();
    assert_eq!(reopened.refcount_routing(), RefcountRouting::PageAffine);
    assert_eq!(reopened.get_refcount(pba0).unwrap(), 0);
    assert_eq!(reopened.get_refcount(pba1).unwrap(), 2);
    assert_eq!(
        reopened.refcount_shards[reopened.refcount_shard_for(pba1)]
            .rc
            .get_entry(pba1)
            .unwrap()
            .birth_lsn,
        birth1
    );
    assert_eq!(
        reopened.manifest_state.lock().manifest.body_version,
        MANIFEST_BODY_VERSION
    );
}

#[test]
fn legacy_v25_reopens_without_changing_routing_or_manifest_version() {
    let dir = tempfile::TempDir::new().unwrap();
    let mut cfg = Config::new(dir.path());
    cfg.shards_per_partition = 16;
    let page_first = 29 * V26_PAGE_ENTRIES;
    let pba0 = page_first;
    let legacy_sid0 = RefcountRouting::LegacyPbaHash.shard_for_pba(pba0, 16);
    let pba1 = (page_first + 1..page_first + V26_PAGE_ENTRIES)
        .find(|pba| RefcountRouting::LegacyPbaHash.shard_for_pba(*pba, 16) != legacy_sid0)
        .expect("one full refcount page must span legacy hash shards");
    let birth0;
    let birth1;

    {
        let db = Db::create_legacy_v25_with_config(cfg.clone()).unwrap();
        assert_eq!(db.refcount_routing(), RefcountRouting::LegacyPbaHash);
        db.incref_pba(pba0, 1).unwrap();
        db.incref_pba(pba1, 2).unwrap();
        birth0 = db.refcount_shards[db.refcount_shard_for(pba0)]
            .rc
            .get_entry(pba0)
            .unwrap()
            .birth_lsn;
        birth1 = db.refcount_shards[db.refcount_shard_for(pba1)]
            .rc
            .get_entry(pba1)
            .unwrap()
            .birth_lsn;
        assert_eq!(
            db.manifest_state.lock().manifest.body_version,
            LEGACY_MANIFEST_BODY_VERSION
        );
    }

    {
        let db = Db::open_with_config(cfg).unwrap();
        assert_eq!(db.refcount_routing(), RefcountRouting::LegacyPbaHash);
        assert_eq!(db.get_refcount(pba0).unwrap(), 1);
        assert_eq!(db.get_refcount(pba1).unwrap(), 2);
        assert_eq!(
            db.refcount_shards[db.refcount_shard_for(pba0)]
                .rc
                .get_entry(pba0)
                .unwrap()
                .birth_lsn,
            birth0
        );
        assert_eq!(
            db.refcount_shards[db.refcount_shard_for(pba1)]
                .rc
                .get_entry(pba1)
                .unwrap()
                .birth_lsn,
            birth1
        );
        assert_free_outcome(db.commit_free_pbas(0, &[pba0]).unwrap(), pba0);
        assert_eq!(db.get_refcount(pba0).unwrap(), 0);
        assert_eq!(db.get_refcount(pba1).unwrap(), 2);
        db.flush().unwrap();
        assert_eq!(
            db.manifest_state.lock().manifest.body_version,
            LEGACY_MANIFEST_BODY_VERSION
        );
    }

    let reopened = Db::open(dir.path()).unwrap();
    assert_eq!(reopened.refcount_routing(), RefcountRouting::LegacyPbaHash);
    assert_eq!(reopened.get_refcount(pba0).unwrap(), 0);
    assert_eq!(reopened.get_refcount(pba1).unwrap(), 2);
    assert_eq!(
        reopened.manifest_state.lock().manifest.body_version,
        LEGACY_MANIFEST_BODY_VERSION
    );
}
