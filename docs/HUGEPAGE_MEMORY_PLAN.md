# metadb HugeTLB Memory Plan

**版本**：v1  2026-04-26
**范围**：metadb 的 L2P 与 dedup metadata working set。
**目标**：把 metadb 最重、最长驻留、最关键的内存预算迁入可控的 hugepage-backed arena，
降低普通匿名内存 OOM 风险，并减少大规模随机 metadata 访问的 TLB 压力。

本文只讨论 metadb。onyx 主引擎的 LV3 IO buffer、write buffer、packer 等不在本计划内。

---

## 1. 背景

metadb 是 onyx-storage 里最重要、也最重的 metadata 子系统。生产规模下，真正值得保护的
不是临时 IO buffer，而是：

1. **L2P**：LBA -> BlockmapValue 的地址翻译层。读写路径都绕不开。
2. **Dedup**：content hash -> DedupEntry，以及 reverse index / bloom / memtable / SST block cache。

这两块共同决定：

- 读路径是否稳定。
- 写路径是否能持续查重、提交、回收。
- snapshot / GC / drop volume 这类后台任务是否会把 metadata working set 顶爆。

当前 metadb 的 4 KiB page 结构非常适合 hugepage slab 化：一个 2 MiB hugepage 可以容纳
512 个 4 KiB metadata page。L2P index pages 和 leaf pages 都天然落在这个模型里。

---

## 2. 目标与非目标

### 目标

- 提供一套统一的 metadb memory backend，而不是维护两套 metadb 实现。
- 支持用户按部署环境选择：
  - 普通内存。
  - THP best-effort。
  - 严格 HugeTLB 预留池。
- L2P `PageCache` 与 pinned index pages 优先进入 hugepage arena。
- dedup 的 cache / memtable / bloom working set 后续接入同一套 arena。
- HugeTLB 模式下，默认 **不静默 fallback**，避免用户以为 metadata 已经离开普通 OOM 域。
- 暴露 metrics，让用户知道配置的 hugepage 是否真的生效。

### 非目标

- 不保证进程绝对不会被 OOM kill。HugeTLB 只能保护迁入 hugepage pool 的内存；普通 heap、
  临时 `Vec`、线程栈、压缩 buffer 等仍可能触发普通 OOM。
- 不在第一阶段改 on-disk format。Page layout 仍然是 4 KiB。
- 不把 WAL durability、direct IO、io_uring 一起重做。它们可以共享 aligned buffer 工具，
  但不是本计划的主线。
- 不为了 hugepage 牺牲 crash consistency 或 snapshot COW 语义。

---

## 3. 内存模式

配置建议：

```rust
pub enum PageMemoryMode {
    Normal,
    Thp,
    HugeTlb,
}

pub struct Config {
    pub page_cache_bytes: u64,
    pub index_pin_bytes: u64,

    pub page_memory_mode: PageMemoryMode,
    pub page_memory_fallback: bool,
}
```

TOML / adapter 层可映射为：

```toml
[metadb.memory]
page_memory_mode = "hugetlb"   # normal / thp / hugetlb
page_memory_fallback = false
```

### Normal

使用当前普通 allocator 路径。适合测试、开发环境、小规模部署。

### THP

使用 `mmap` 分配大 slab，并对 slab 调用 `madvise(MADV_HUGEPAGE)`。这是 best-effort 模式：

- 部署简单。
- 不需要预留 hugepage。
- 仍属于普通匿名内存 / RSS。
- 内存压力下仍可能被普通 OOM 影响。

THP 模式可以允许 fallback，因为它本来就是性能优化，不是隔离承诺。

### HugeTLB

使用 `mmap(MAP_HUGETLB)` 从 HugeTLB pool 分配 slab。语义：

- 需要系统预留 hugepage。
- 申请失败应默认启动失败。
- 不应该默认 fallback 到普通内存。
- 受 hugetlb pool / hugetlb cgroup 限制，不等同于普通 anonymous RSS。

生产推荐：

```toml
[metadb.memory]
page_memory_mode = "hugetlb"
page_memory_fallback = false
```

---

## 4. 设计方向

### 4.1 统一 PageArena

新增统一 arena：

```text
PageArena
  -> Normal backend
  -> Thp backend
  -> HugeTlb backend
```

arena 以 slab 为单位向 OS 申请内存：

```text
2 MiB slab
  ├── 4 KiB slot 0
  ├── 4 KiB slot 1
  ├── ...
  └── 4 KiB slot 511
```

每个 slot 存一个 metadb page。arena 负责：

- 分配 / 回收 4 KiB slot。
- 维护 slab 使用计数。
- 统计 normal / thp / hugetlb 已分配字节。
- 在 HugeTLB 模式下严格报告分配失败。

### 4.2 PageCache 接入

当前 `PageCache` 存 `Arc<Page>`，`Page` 内部是 `Box<[u8; 4096]>`。这会导致大量独立
4 KiB heap allocation，不适合 HugeTLB。

目标形态：

```text
PageCache
  LRU shard
    PageId -> Arc<CachedPage>

CachedPage
  arena slot handle
  immutable 4 KiB bytes view
```

读路径：

1. `PageStore::read_page` 读出 4 KiB。
2. `PageCache` 从 `PageArena` 分配 slot。
3. 把 page bytes 拷入 slot。
4. LRU / pinned table 保存 `Arc<CachedPage>`。

evict 路径：

1. LRU 删除 `Arc<CachedPage>`。
2. 最后一个 `Arc` drop 时归还 arena slot。

### 4.3 Dirty PageBuf

第一阶段可以保留 dirty `Page` 走普通 heap，降低改动面。原因：

- dirty working set 通常小于 shared `PageCache`。
- COW / mutation 路径对可变引用要求更高。
- 先把长期驻留的 clean cache 和 pinned index pages 搬走，收益最大。

第二阶段再考虑：

- `DirtyPage` 也从 arena 分配。
- 或者为 dirty pages 做单独 transient arena。
- flush 后 dirty page 直接转成 cached page，减少一次 heap 分配。

### 4.4 Pinned L2P Index Pages

pinned index pages 是最高价值对象：

- 生命周期长。
- 访问频率高。
- 不参与 LRU 竞争。
- 是随机 L2P get 的上层路径。

因此 HugeTLB 模式下，`index_pin_bytes` 对应的 pinned pages 应优先从 hugepage arena 分配。
如果 `page_memory_fallback = false`，pinned index 分配失败应使 warmup / open 失败，而不是
悄悄退回普通内存。

---

## 5. L2P 优先级

L2P 是第一优先级，拆分如下：

1. **L2P pinned index pages**
   - 接入 `PageArena`。
   - metrics 区分 pinned normal / hugepage bytes。
   - HugeTLB strict 模式下禁止 fallback。

2. **L2P shared PageCache**
   - LRU clean pages 接入 `PageArena`。
   - 保持现有 shard + LRU 语义。
   - eviction 归还 arena slot。

3. **L2P dirty PageBuf**
   - 后续阶段再迁。
   - 不影响第一阶段收益。

L2P 接入后的关键不变量：

- page bytes 仍然是 4096 B。
- CRC / header / generation / refcount 语义不变。
- snapshot COW 不因为 cache slot 复用而暴露可变别名。
- `Arc<CachedPage>` 必须只暴露 immutable bytes。

---

## 6. Dedup 优先级

dedup 是第二条主线，但要和当前实现状态对齐。

如果 dedup 仍主要在 onyx RocksDB 路径中，metadb 的 hugepage arena 覆盖不到 RocksDB
内部 block cache。此时只能通过 RocksDB cache / allocator 侧做间接调优。

当 dedup 迁入 metadb 原生结构后，接入顺序建议：

1. **dedup index block cache**
   - hash -> DedupEntry 的 SST / block cache。
   - 随机点查多，收益明显。

2. **dedup bloom / filter cache**
   - 长期驻留，结构紧凑。
   - 可单独预算，避免挤占 L2P。

3. **dedup memtable arena**
   - 写路径热点。
   - 需要关注 flush / freeze 后的所有权转移。

4. **dedup reverse index cache**
   - PBA cleanup / refcount-zero 路径使用。
   - 优先级低于正向 dedup index。

建议配置拆成独立预算，避免 dedup 把 L2P 顶出 hugepage pool：

```toml
[metadb.l2p]
page_cache_bytes = 64_000_000_000
index_pin_bytes = 8_000_000_000

[metadb.dedup]
cache_bytes = 64_000_000_000
memtable_bytes = 8_000_000_000
bloom_cache_bytes = 4_000_000_000
```

---

## 7. Metrics

必须暴露以下指标：

```text
metadb_page_memory_mode
metadb_page_memory_fallback_enabled
metadb_page_arena_slabs_total
metadb_page_arena_slots_total
metadb_page_arena_slots_used
metadb_page_arena_bytes_normal
metadb_page_arena_bytes_thp
metadb_page_arena_bytes_hugetlb
metadb_page_arena_alloc_failures_total
metadb_l2p_cache_hugepage_bytes
metadb_l2p_pinned_hugepage_bytes
metadb_dedup_cache_hugepage_bytes
```

启动日志必须打印：

- requested memory mode。
- actual backend。
- fallback 是否发生。
- hugepage slab size。
- 成功分配的 hugepage bytes。

HugeTLB strict 模式下，如果实际 backend 不是 HugeTLB，必须返回错误。

---

## 8. 测试计划

### 单元测试

- arena slot allocate / free / reuse。
- slab 使用计数。
- HugeTLB 分配失败时的 strict / fallback 行为。
- `CachedPage` drop 后 slot 归还。
- LRU eviction 后 arena usage 下降。
- pinned page invalidate 后 arena usage 下降。

### 集成测试

- `PageCache::get` / `get_for_modify` / `pin` / `invalidate` 行为与现有测试一致。
- `PagedL2p` random get / put / snapshot / drop snapshot 与 reference model 一致。
- warmup pinned index pages 后 metrics 与 pinned page 数一致。

### 故障测试

- HugeTLB strict 模式下 pool 不足，open 失败且不创建半初始化状态。
- fallback 模式下 pool 不足，启动成功但 metrics / log 明确记录 fallback。
- LRU 高压 eviction + snapshot read 并发下不出现 use-after-free。

### 性能测试

- 随机 L2P get：比较 normal / THP / HugeTLB。
- 大 cache warmup 时间。
- TLB miss / page walk 指标：
  - `perf stat -e dTLB-load-misses,dTLB-store-misses,page-faults`
- soak 中记录 arena usage 是否稳定，无 slot 泄漏。

---

## 9. 实施拆分

### H0 — 配置与模式枚举

交付：

- `PageMemoryMode`。
- `Config` 字段。
- 默认 `Normal`。
- 配置解析 / debug print。

退出：

- 所有现有测试不变。
- 新增配置 round-trip 测试。

### H1 — PageArena

交付：

- normal backend。
- THP backend。
- HugeTLB backend。
- strict / fallback 行为。
- metrics 基础计数。

退出：

- arena 单元测试覆盖分配、释放、失败、fallback。

### H2 — L2P PageCache clean pages 接入

交付：

- `CachedPage` / arena slot handle。
- `PageCache` LRU 存 arena-backed pages。
- eviction 归还 slot。

退出：

- `metadb` 现有 `PageCache`、`PagedL2p`、snapshot 测试全绿。
- normal mode 性能不出现明显回退。

### H3 — L2P pinned index pages 接入

交付：

- pinned table 使用 arena-backed page。
- `index_pin_bytes` 与 hugepage metrics 对齐。
- HugeTLB strict 模式下 pinned warmup 失败即返回错误。

退出：

- pinned warmup 测试全绿。
- L2P random read benchmark 产出 normal / THP / HugeTLB 对比。

### H4 — Dedup cache 接入

交付：

- dedup block cache 使用 arena budget。
- bloom / filter cache 独立预算。
- dedup metrics。

退出：

- dedup point lookup benchmark。
- dedup cleanup / reverse index 测试全绿。

### H5 — Dedup memtable 接入

交付：

- dedup memtable arena allocator。
- freeze / flush 所有权转移测试。

退出：

- dedup write-heavy soak 稳定。
- arena slot / byte usage 无泄漏。

### H6 — 生产化门控

交付：

- 运维文档：如何预留 HugeTLB pool。
- cgroup / systemd 配置说明。
- dashboard / metrics 接入。
- 24h soak 对比报告。

退出：

- HugeTLB strict 模式生产签收。
- fallback / non-fallback 行为文档化。
- 失败路径可观测。

---

## 10. 运维注意事项

HugeTLB 需要机器预留 hugepage，例如：

```bash
sysctl -w vm.nr_hugepages=32768
```

2 MiB hugepage 下，`32768` 页约等于 64 GiB。生产环境应通过启动脚本或系统配置固定，
不要依赖运行时临时申请。

systemd / cgroup 环境还需要确认 hugetlb limit。否则即使系统有空闲 hugepage，服务也可能
因为 cgroup 限制而 `mmap(MAP_HUGETLB)` 失败。

建议生产启动策略：

```text
page_memory_mode = "hugetlb"
page_memory_fallback = false
```

如果启动失败，说明 metadata 预算没有真正进入预留 hugepage pool，应该修部署，而不是让
服务悄悄退回普通内存。

---

## 11. 风险与决策

### 风险：API 改动面

`Page` 当前是 owned heap object。`PageCache` 改为 arena-backed page 后，需要谨慎处理：

- immutable cached page。
- mutable dirty page。
- `Arc` 生命周期。
- eviction 与 invalidate。

决策：第一阶段只迁 clean cache / pinned pages，dirty path 保持普通 `Page`。

### 风险：HugeTLB 资源不足

HugeTLB pool 不足会导致分配失败。

决策：HugeTLB strict 模式下失败即返回错误；fallback 必须显式开启，并通过 metrics / log
报告。

### 风险：dedup 与 L2P 争抢预算

dedup 和 L2P 都可能很大。

决策：配置拆成独立预算。L2P 优先级高于 dedup。

### 风险：误解 OOM 语义

HugeTLB 只能保护进入 hugepage pool 的内存。

决策：文档、日志、metrics 都明确 actual backend 与普通内存使用量。禁止宣传为“绝对不会
OOM”。

---

## 12. 推荐落地顺序

短期只做三件事：

1. H0：配置与模式枚举。
2. H1：PageArena。
3. H2 + H3：L2P PageCache / pinned index 接入。

这三步完成后，metadb 最大、最稳定、最关键的 L2P working set 就可以进入 HugeTLB。
dedup 接入应等原生 dedup index 结构稳定后再做，避免为了内存后端反复改接口。
