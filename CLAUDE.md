# onyx-metadb

Onyx 的定制元数据引擎（crate）。独立 git 仓库，但通过 path dep 被 onyx-storage 主
crate 引用——onyx **唯一**的 meta backend，已经替掉 RocksDB（onyx 主仓 commit
`b512c44 meta: switch onyx to metadb`）。所有针对 onyx 写路径需求做的 API 形状让步
（fused WalOp、`atomic_batch_*`、L2pValue 头 8B 即 PBA 的契约、snapshot-aware refcount
规则等）保留为长期不变量；不再为"抽象通用 KV"留中立接口。

## 构建与测试

```bash
cargo build
cargo test
cargo build --release
cargo test -- --ignored  # 长跑 proptest + 故障注入，发布前必跑 ，测试需要限定1个线程。不然会相互锁死
```

测试覆盖率目标 90%+。`--ignored` 的用例不是可选，是发布门控。仓库当前 `#[test]` 标
记 ~570 个（src/ + tests/，proptest 共享 harness）。

## 模块地图

| 模块 | 路径 | 职责 |
|------|------|------|
| db | `src/db.rs` + `src/db/*.rs` | `Db` facade：shard 路由、apply gate、commit_ops、L2P / refcount / dedup / snapshot / volume lifecycle 入口 |
| tx | `src/tx.rs` | `Transaction`：累积 `WalOp`，一次 `commit()` = 一条 WAL 记录、一次 group fsync |
| wal | `src/wal/` | Append-only segment 文件 + group commit writer + recovery |
| page_store | `src/page_store.rs` | 4 KiB page 分配/释放/读/写（Linux O_DIRECT），free list 打开时重建，writeback worker（in-flight pid 保护） |
| manifest | `src/manifest.rs` + `src/manifest/` | 双缓冲 manifest：volume entries（含 ordinal + drop pending）、shard roots、dedup level heads / shard heads、checkpoint_lsn、WAL tail |
| paged | `src/paged/` | Paged COW radix tree（用于 L2P）；leaf compact codec、read view、index pin |
| paged_meta | `src/paged_meta.rs` | 把 paged radix 复用给非-L2P 用途的薄壳（refcount array 复用 page IO + COW；legacy paged_reverse 也走这里） |
| paged_reverse | `src/paged_reverse/` | legacy dedup_reverse 的 paged-array + overflow 链；当前 onyx 主路径已退役持久 reverse，保留给旧实验/测试直至清理 |
| refcount | `src/refcount/` | PBA refcount：per-shard paged array + DeltaMap，apply lane 在 commit 边界排干 delta |
| dedup | `src/dedup/` | dedup_index：4 tier（L0 cuckoo filter → L1 hot cache → page cache → on-disk cuckoo）+ apply lanes；`cuckoo.rs` / `index.rs` / `l1_cache.rs` / `sketch.rs`（L0 早期是 ref-counted fp set，已换成 16-bit cuckoo filter，饱和后 lossless 降级） |
| dedup_types | `src/dedup_types.rs` | `Hash8`（xxh3_64，8 字节）/ `DedupValue`（27 字节，cuckoo 槽位 32 字节零 padding）/ 大小常量。短哈希 schema：pair 碰撞率 ~1.5e-8，需要 client（onyx）做字节验证；旧的 32-byte SHA-256 布局已退役 |
| apply_gate | `src/apply_gate.rs` | commit apply 与 flush / snapshot 之间的 RwLock；commit 持 read，flush / snapshot / drop 持 write |
| epoch | `src/epoch.rs` | per-shard epoch 槽位（NVMe worker 容量已扩） |
| affinity | `src/affinity.rs` | apply lane / WAL writer 的 CPU pinning |
| cache | `src/cache.rs` | 16 内部 shard page cache（LRU，scan-resistant，invalidate-on-modify dirty pin，`get_bypass` 给 cuckoo / refcount array 的批量 scan 用；legacy paged_reverse 如被触碰也用 bypass） |
| metrics | `src/metrics.rs` | 运行时计数器/延迟累计；`metadb_metrics_summary.py` 解析这个 |
| recovery | `src/recovery.rs` | 打开时 WAL replay；apply 必须幂等（refcount delta + paged COW + cuckoo put；legacy dedup_reverse op 若存在也必须幂等） |
| verify | `src/verify.rs` | 结构校验 + offline audit（`metadb-verify` 入口） |
| testing | `src/testing/` | `FaultController` / `FaultPoint`，proptest 共享 harness |
| bin | `src/bin/` | CLI：verify / soak / bench / dump / replay |
| scripts | `scripts/` | 本地诊断脚本（`metadb_metrics_summary.py` 等） |

> 历史：`src/lsm/` 和 `src/btree/` 已 retire（commit `0e1c69e`），不要再
> 引用。dedup_index / refcount 都走专用 paged/cuckoo 系列；dedup_reverse 是已退役的 legacy 路径。
> L2P 之外**没有 LSM、没有 B+tree**。看到任何 PR 想"引一个 LSM 来做 X"先停下
> 来读 `docs/DESIGN.md` 里的 workload 拆分理由。

## 关键不变式（非显而易见的，动之前先读）

### 锁序

- commit path（`Db::commit_ops`）不再有全局 commit_lock。`wal.submit` 在无锁下
  并发，WAL writer 负责 group commit；submit 返回拿到 LSN 后，通过
  `last_applied_lsn` + `commit_cvar` 排队等 "我的 LSN - 1 已经 apply 完"，然后持
  `apply_gate.read()` apply ops、bump `last_applied_lsn`、notify_all，**最后**才
  drop read guard。LSN 顺序 ≡ apply 顺序依旧成立，但序列化的是 apply 阶段而不是
  WAL fsync，所以 batch 重新形成。`flush` / `take_snapshot` / `drop_snapshot` 取
  `apply_gate.write()` 排干所有 apply，保证采样的 `last_applied_lsn` 和 tree 状态
  对齐。**bump 必须在 drop read gate 之前**，否则 flush 可能采到"tree 含 op N + 
  checkpoint_lsn = N-1"的组合，recovery double-apply（refcount incref 非幂等）。
- **dedup_index 的 page-shard 锁**（`dedup/cuckoo.rs`）：cuckoo 数据页改写按 page-shard
  分锁（commit `de2501f`），foreground put / background reader 的等价物变成 read 侧拿
  `PageCache` 一致视图、write 侧拿 page-shard 锁后回写并 invalidate。**先 read view、
  后 page IO、释放 read view**；reader 不持锁穿越多页 hop。
- **refcount 的 shard 锁**（`refcount/shard.rs`；legacy `paged_reverse/` 遵守同样规则）：每
  shard 一组锁（delta lock + array lock）。apply lane worker 持 delta lock 简短地 merge
  ops、再持 array lock flush；read 路径只读 delta，miss 落 array。**改 lock 顺序 = 死
  锁风险**，新增跨 shard 聚合接口（multi_get / scan）必须按 shard index 顺序拿。
- L2P / refcount / dedup shard 的并发都建立在"shard 是 mutation 单位"上。跨 shard 操
  作（`take_snapshot`、`diff_with_current`、`drop_volume`）必须按 shard index 顺序取
  锁，避免和写路径死锁。新增聚合接口（比如 `multi_get_*` 按 shard bucket）也要遵守这条。

### 分片

- L2P / refcount: `shard_for(key) = xxh3_64(key.to_be_bytes()) as usize % shard_count`。
  分片数写死在 `Config::shards_per_partition`（默认 16），落在 manifest 里。
- dedup: `Config::dedup_shards`（默认 8）。**当前最优值**——见
  [memory: phase4_dedup_shards_results](dev:phase4)。`MAX_DEDUP_SHARDS = 64`。N=4
  是不稳定平衡点，不要默认。manifest v8 持久化 `dedup_shards` + per-shard heads。
- **改 shard 数或哈希函数 = 数据迁移**，不要当普通 refactor 处理。

### Page cache

- 一个 `Db` 只持有一个 `Arc<PageCache>`，clone 给所有 L2P shard / refcount shard /
  dedup_index；legacy paged_reverse 若被启用也共享这份 cache。预算在 `cfg.page_cache_bytes`。
- 16 内部 shard，对齐 L2P shard fanout。
- "dirty pin" 是 **invalidate-on-modify + re-insert-on-flush**（不是 refcount pin）。
  脏页不会被驱逐——因为它根本不在 cache 里。维护这条语义的是写路径和 flush 路径，
  别在读路径上加绕过。
- 保持 **`get_bypass`** 给 cuckoo / refcount array（以及 legacy paged_reverse）的批量 scan 用，避免
  热页被全表扫刷掉。
- 当前只 pin L2P **index pages**（`cfg.index_pin_bytes`），leaf 仍走普通 LRU。不要把
  leaf-pin 当作既定优化路线：除非 metrics 显示 leaf miss / leaf read latency 已经成为
  主瓶颈，否则 leaf-pin 的生命周期复杂度通常大于收益。
- dedup_index 多了一个 `cfg.dedup_l1_cache_entries` 控制 L1 hot LRU；`cfg.dedup_cuckoo_buckets`
  按 unique-4K 工作集 sizing（每 bucket 4 slot，目标 load factor 0.5–0.7）。**桶数偏小
  会触发 `MAX_CUCKOO_CHAIN`**，writer 进 packed-slot retry，污染读 p99。

### WAL / recovery

- WAL 记录体 = `encode_body(&[WalOp])`，CRC32C 覆盖 body。
- recovery = 找到最新有效 manifest → 从 `checkpoint_lsn + 1` 回放 WAL → 截断撕裂的
  尾部。apply 必须**幂等**（apply_op 对同一 op 重放结果一致）。
- WAL 有 `group_commit_max_batch_bytes` / 超时合批，Phase 8b 把 commit path
  的全局锁去掉后真正形成 batch：多 writer 场景下 fsync 数接近 `ceil(ops / batch)`，
  不再是 1:1。单线程提交仍是 1 fsync/op，因为 writer 端没有并发 submit 可合。
- **DropSnapshot 走 WAL**：`TakeSnapshot` 仍然只靠 manifest commit（没落盘的
  snapshot 等同于没存在过），但 `DropSnapshot` 会同时改页 refcount 和 snapshot
  列表，单次 manifest commit 无法把两者原子化。所以 drop 记录进 WAL，带 rc
  级联算出来的 page 列表；apply 对每页做 `rc--`，靠 `page.generation >= lsn`
  做重放幂等。`drop_snapshot` 取 `drop_gate.write()` 排他，确保 plan 期间无
  并发 `cow_for_write` 改动共享页；commit 路径取 `drop_gate.read()` 共享。

### Manifest swap

- 任何要替换 root 集合的结构都遵循 **写新页链 → manifest commit → 释放旧页链** 三步：
  L2P shard roots、refcount shard array roots、dedup_index per-shard cuckoo / level heads
  （以及 legacy paged_reverse heads，如果重新启用）。三步之间断电恢复出来的状态要么是 pre-commit、要么是
  post-commit，不能是中间态。fault injection 覆盖这些切换。
- 'chained meta pages'（commit `38a19ca`）：refcount / cuckoo（以及 legacy paged_reverse）的 manifest
  meta 不再受单页大小限制。改这块布局前读 commit `38a19ca` + `055174a` 的回归说明，
  老 cuckoo 默认值（commit `d9b16cd` 抬升）不能往回退。

### Snapshot 范围

- **只有 L2P 支持 snapshot**。Refcount 是累计量、dedup 是 global，都不做 point-in-time。
  看到 "shard_roots.is_empty()" 类的 `debug_assert` 就是在挡这条。
- Snapshot 读走 `SnapshotView`，持 `snapshot_views.read()` 共享 guard；`drop_snapshot`
  拿写侧，保证不会释放还在被读的页。
- `drop_snapshot` 取 `drop_gate.write()` 排他，确保 plan 期间无并发 `cow_for_write`
  改动共享页；commit 路径取 `drop_gate.read()` 共享。`take_snapshot` 只靠 manifest
  commit（没落盘的 snapshot 等同于没存在过），但 `DropSnapshot` 会同时改页 refcount
  和 snapshot 列表，单次 manifest commit 无法把两者原子化，所以 drop 走 WAL，apply 对
  每页做 `rc--`、靠 `page.generation >= lsn` 做重放幂等。

## Soak 门控（持续生效，不只是 Phase 8a）

Phase 7（onyx 接入）已 landed，但 standalone soak 仍是任何深层改动的发布门控。`metadb-soak`
二进制不经过 onyx，目标：billions of ops + 进程重启 + fault injection + `metadb-verify`
无报错 + reference-model 无偏差。

所以：

- 任何改 commit path / apply gate / page cache / snapshot / cuckoo /
  refcount delta apply / chained meta pages / manifest swap 的 PR，**本地 soak 至少
  过几个小时**再 merge。怀疑 flaky 就停下来查根因，不要重跑看是否复现。
- 新功能优先配一条 proptest 或 fault-injection 用例；没对应的测试，默认不接受。
- 禁止为了让 soak 过去绕过校验（关 assert、放宽 invariant check）。
- onyx 主仓里 NVMe 测试机也是这条门控的一部分：metadb 单干跑通的改动，可能在 onyx
  侧 4-shard concurrent commit 路径上暴露 22-94s stall（见 onyx 上 buffer head stuck
  的诊断记录）。phase4 的 perf 数据（READ ~96k IOPS、p99 174ms、p99.9 460ms，
  N=8 / 1024 packed_meta_batch_max_lbas）当前是已验证基线。

### Soak / metrics 快速入口

- `./dev.sh start 24h concurrent --restart-interval 2h --pipeline-depth 128`：
  低频重启 + 高 pipeline 压力；`concurrent` 映射到 `--onyx-concurrent-mix`。
- `METADB_SOAK_OPS_PER_CYCLE=1000000` 控制每个 cycle 的 op 数；`METADB_SOAK_THREADS`
  控制 child worker 数；`METADB_SOAK_PIPELINE_DEPTH` 控制每 worker 预排队深度。
- `METADB_SOAK_ONYX_MAX_PBA=100000000` 控制 Onyx-mix 的随机 PBA 空间；小值（如
  256）只用于 pathological dedup / refcount 热点压力。
- `./dev.sh metrics` tail 当前 run 的 `metrics.jsonl`。
- `./dev.sh metrics-summary [run-dir|metrics.jsonl] [samples]` 把累计 counter 转成窗口内
  `tx/s`、`logical ops/s`、WAL batch size、fsync、gate wait、cache hit/miss 和瓶颈提示。
- 当前 small-tx soak 的主要用途是 crash / mutex / WAL 串行点暴露，不代表 Onyx flusher
  批量 metadata commit 的最终吞吐上限。评估 30w IOPS 需要 batch metadata workload。
- 2026-04-26 soak 显示小 `range_delete` 的 scan/apply 只有几十微秒，主要成本是
  `drop_gate` 等待 + WAL/fsync；discard / reclaim 后续按
  [`docs/ASYNC_RECLAIM_PLAN.md`](docs/ASYNC_RECLAIM_PLAN.md) 分阶段异步化。

## 代码风格

- 私有 helper 不加 doc comment，除非 WHY 不显然（锁序、fault-injection hook、不变式）。
- 模块顶部的 `//!` 说明**责任 + 并发模型**，别写"这个模块做 X"（代码已经告诉你了）。
- 新增公共 API 要在 `README.md` 的 "Public API at a glance" 里顺手加一行。
- 不要轻易引入新 crate。当前依赖：`parking_lot` / `xxhash-rust` / `lru` / `crc32c` /
  `rand` / `rand_chacha` / `tempfile`（test）/ `proptest`（test）。
- `unsafe` 需要写原因注释。目前只有 page_store / AlignedBuf 几处，边界明确。

## 和 onyx-storage 的关系

- onyx-storage 在 `/root/onyx_storage`，是 metadb 的**唯一 client**，已经把 RocksDB
  切到 metadb（onyx commit `b512c44 meta: switch onyx to metadb`）。metadb 就是 onyx 的
  定制元数据引擎，**接受为 onyx 语义下沉做 API 形状让步**（fused WalOp、头 8B 即 pba
  的 L2pValue 布局契约、`atomic_batch_*`、snapshot-aware refcount 规则等），不再为
  "抽象的通用 db" 保留中立接口。
- onyx 的 meta 适配层在 [`/root/onyx_storage/src/meta/`](../src/meta/)：`backend::metadb`
  是唯一 backend，`store` 是给 flusher / writer / cleaner 用的高层封装。onyx 用字符串
  `VolumeId`、metadb 用 `VolumeOrdinal`，由 `MetadbBackend` 维护双向映射。
- **施工规格**：[`docs/ONYX_INTEGRATION_SPEC.md`](docs/ONYX_INTEGRATION_SPEC.md)。
  涵盖 WalOp / Db API、必须守住的不变量、测试矩阵、性能目标。API 演进继续遵照此文档，
  避免"先做最干净的接口"再返工。
- **整合实施进度**：[`docs/ONYX_INTEGRATION_PLAN.md`](docs/ONYX_INTEGRATION_PLAN.md)
  自维护 session 拆解 / 退出判据。
- 父项目的 CLAUDE.md（`/root/onyx_storage/CLAUDE.md`）讲 ublk / buffer / packer / GC /
  dedup pipeline 等存储层面的东西，和 metadb 内部约束不重叠。切 `cd /root/onyx_storage`
  工作时读那边的 CLAUDE.md。

## 当前不要再做的方向（实测撤回，避免重复踩）

- **L2P read view 改 ArcSwap 发布**：失去"apply 前空 overlay + 独占 mutation"后 COW
  克隆暴涨，`l2p_remap` apply 累计时间膨胀 4×，READ p99 从 ~338ms 飙到 1.15s。源码已
  撤回，仅留实验数据。
- **multi_get 解码进读路径输出 buffer**：`read_submit.meta_get` 反而暴涨到 9.44s，
  READ 跌到 42k。源码已撤回。
- **N=4 dedup_shards 当默认值**：`apply_wait_max` 曾到 3.55s，是不稳定平衡点。默认走
  `dedup_shards=8`。
- **leaf-pin（pin L2P leaf 页）**：metrics 没显示是主瓶颈前不要做，复杂度大于收益。
  保留为 deferred TODO（参见 user memory `metadb_leaf_pin_todo`）。
