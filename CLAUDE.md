# onyx-metadb

独立的元数据引擎（crate），用来替换 onyx-storage 里的 RocksDB。自己一个 git 仓库，
独立构建、独立测试。

## 构建与测试

```bash
cargo build
cargo test               # 290 unit + 25 integration tests
cargo build --release
cargo test -- --ignored  # 长跑 proptest + 故障注入，发布前再跑
```

测试覆盖率目标 90%+。`--ignored` 的用例不是可选，是发布门控。

## 模块地图

| 模块 | 路径 | 职责 |
|------|------|------|
| db | `src/db.rs` | `Db` facade：shard 路由、事务入口、snapshot、commit_ops |
| tx | `src/tx.rs` | `Transaction`：累积 `WalOp`，一次 `commit()` = 一条 WAL 记录 |
| wal | `src/wal/` | Append-only segment 文件 + group commit + recovery |
| page_store | `src/page_store.rs` | 4 KiB page 分配/释放/读/写（Linux O_DIRECT），free list 打开时重建 |
| manifest | `src/manifest.rs` | 双缓冲 manifest：tree roots / checkpoint_lsn / WAL tail |
| paged | `src/paged/` | Paged COW radix tree，用于 L2P（LBA → L2pValue） |
| btree | `src/btree/` | COW B+tree，特化到 refcount（value = u32） |
| lsm | `src/lsm/` | 固定长度记录 LSM：dedup_index + dedup_reverse |
| cache | `src/cache.rs` | 统一 16 shard page cache（LRU，scan-resistant） |
| recovery | `src/recovery.rs` | 打开时 WAL replay |
| verify | `src/verify.rs` | 结构校验器（`metadb-verify` CLI 的基础） |
| testing | `src/testing/` | 故障注入点 + 共享 test harness |
| bin | `src/bin/` | CLI 二进制（8a 还要加 `metadb-verify` / `metadb-soak`） |

## 关键不变式（非显而易见的，动之前先读）

### 锁序

- `commit_lock`（`Db::commit_ops`）串行化 **WAL submit + apply**。LSN 顺序 ≡ apply
  顺序，所以 recovery 重放必然和正常运行得到同一内存状态。未来 Phase 8b 要换成
  LSN-ordered condvar，现在不要绕开。
- `Lsm::reader_drain`：read 侧给 `get` / `scan_prefix` 读 SST 期间用，write 侧是
  compaction 回收 victim page 前的 drain barrier。**先 read 侧、后 page IO、释放 read
  侧**；compaction 改完 levels 后必须 `drop(reader_drain.write())` 再 `free_victims`。
- L2P / refcount shard 的 `Mutex<tree>`：粒度是 shard，不是单 key。跨 shard 操作要
  取所有锁时（比如 `take_snapshot`、`diff_with_current`）**必须按 shard index 顺序**
  取锁，避免和写路径死锁。新增聚合接口（比如 `multi_get` 按 shard bucket）也要遵
  守这条。

### 分片

- `shard_for(key) = xxh3_64(key.to_be_bytes()) as usize % shard_count`。
- 分片数写死在 `Config::shards_per_partition`，落在 manifest 里。**改 shard 数或哈希
  函数 = 数据迁移**，不要当普通 refactor 处理。

### Page cache

- 一个 `Db` 只持有一个 `Arc<PageCache>`，clone 给所有 L2P shard / refcount shard /
  dedup_index / dedup_reverse。预算在 `cfg.page_cache_bytes`，默认 512 MiB。
- 16 内部 shard，对齐 L2P shard fanout。
- "dirty pin" 是 **invalidate-on-modify + re-insert-on-flush**（不是 refcount pin）。
  脏页不会被驱逐——因为它根本不在 cache 里。维护这条语义的是写路径和 flush 路径，
  别在读路径上加绕过。
- 保持 **`get_bypass`** 给 LSM scan / compaction 用，避免热页被全表扫刷掉。

### WAL / recovery

- WAL 记录体 = `encode_body(&[WalOp])`，CRC32C 覆盖 body。
- recovery = 找到最新有效 manifest → 从 `checkpoint_lsn + 1` 回放 WAL → 截断撕裂的
  尾部。apply 必须**幂等**（apply_op 对同一 op 重放结果一致）。
- 目前 WAL 有 `group_commit_max_batch_bytes` / 超时合批，但 commit path 被
  `commit_lock` 串行了，所以 batch 形不起来。Phase 8b 才解开。

### Manifest swap

- `dedup_level_heads` 写新页链 → manifest commit → 释放旧页链。三步之间断电恢复出来
  的状态要么是 pre-commit、要么是 post-commit，不能是中间态。Phase 8a 要加 fault
  injection 覆盖这个切换。

### Snapshot 范围

- **只有 L2P 支持 snapshot**。Refcount 是累计量，不做 point-in-time；dedup 同理。
  看到 `entry.refcount_shard_roots.is_empty()` 的 `debug_assert` 就是在挡这条。
- Snapshot 读走 `SnapshotView`，持 `snapshot_views.read()` 共享 guard；`drop_snapshot`
  拿写侧，保证不会释放还在被读的页。

## Phase 8a 门控

8a 是 standalone soak（`metadb-soak` 二进制，不经过 onyx）。目标：billions of ops +
进程重启 + fault injection + `metadb-verify` 无报错 + reference-model 无偏差，周级别
跑干净。8a 不过，Phase 7（接入 onyx）不开工。

所以：

- 任何改 commit path / page cache / snapshot / compaction 的 PR，**本地 soak 至少
  过几个小时**再 merge。怀疑 flaky 就停下来查根因，不要重跑看是否复现。
- 新功能优先配一条 proptest 或 fault-injection 用例；没对应的测试，默认不接受。
- 禁止为了让 soak 过去绕过校验（关 assert、放宽 invariant check）。

## 代码风格

- 私有 helper 不加 doc comment，除非 WHY 不显然（锁序、fault-injection hook、不变式）。
- 模块顶部的 `//!` 说明**责任 + 并发模型**，别写"这个模块做 X"（代码已经告诉你了）。
- 新增公共 API 要在 `README.md` 的 "Public API at a glance" 里顺手加一行。
- 不要轻易引入新 crate。当前依赖：`parking_lot` / `xxhash-rust` / `lru` / `crc32c` /
  `tempfile`（test）/ `proptest`（test）/ `rand`（test）。
- `unsafe` 需要写原因注释。目前只有 page_store / AlignedBuf 几处，边界明确。

## 和 onyx-storage 的关系

- onyx-storage 在 `/root/onyx_storage`，作为上层调用方。Phase 7 adapter 之前，不要
  为 onyx 的具体使用方式改 metadb API 形状——先在 metadb 这边定最干净的接口，adapter
  层承担语义映射。
- 父项目的 CLAUDE.md 讲 ublk / buffer / packer / GC / dedup pipeline 等存储层面的东
  西，和 metadb 内部约束不重叠。切 `cd /root/onyx_storage` 工作时读那边的 CLAUDE.md。
