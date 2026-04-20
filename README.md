# onyx-metadb

Embedded metadata engine for [Onyx Storage](https://github.com/leonunix/onyx_storage).

Purpose-built to replace RocksDB for Onyx's metadata plane, which has two
distinct workloads that a general-purpose LSM cannot serve well at the same
time:

| Workload | Access pattern | Best fit |
|----------|---------------|----------|
| L2P (LBA → BlockmapValue) | per-volume, int keys, point + range, snapshot-heavy, must support multi-writer | COW paged B+tree |
| Dedup (SHA-256 → entry) | global, uniform-random keys, append-heavy, point lookups only | fixed-record LSM |

Both indexes share one WAL so cross-index updates commit atomically in one
fsync. Snapshots are per-partition, O(1) to take, and diffable — Onyx can
compute the exact set of PBAs that become reclaimable when a snapshot is
dropped.

## Why not RocksDB / redb / fjall

- **RocksDB**: LSM read/write amplification kills dedup; single DB with both
  workloads fights itself; varlen/CF abstractions cost dearly for fixed-size
  records.
- **redb**: single-writer per DB. Onyx has N concurrent flush lanes — a
  serialized root-COW is a hard throughput ceiling.
- **fjall**: pure LSM. Bolting a B+tree partition onto its shared-WAL protocol
  is equivalent to a rewrite.

See [`docs/DESIGN.md`](docs/DESIGN.md) for the full rationale and architecture.

## Status

Phase 0 (scaffolding). No working code yet. See [`docs/ROADMAP.md`](docs/ROADMAP.md).

## Layout

```
src/                       engine code (phase 1+)
docs/
  DESIGN.md               architecture, on-disk formats, recovery semantics
  ROADMAP.md              phased implementation plan
  TESTING.md              test strategy + invariant checks + fault injection
```

## License

Apache-2.0
