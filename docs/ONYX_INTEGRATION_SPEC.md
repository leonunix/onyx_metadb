# Onyx 对 metadb 的需求对接书

**版本**：v1  2026-04-23
**关联文档**：[onyx_metadb_integration.md](onyx_metadb_integration.md)（Phase B 切换计划）

## 0. 背景

onyx-storage 准备一次性移除 rocksdb，改由 onyx-metadb 承载全部元数据语义。
本文是 Phase A（metadb 侧改造）的验收清单：以下需求全部 landed、soak 过、门控项
全部 green，才进 Phase B（onyx 侧切换）。

**基本态度**：metadb 是 onyx 定制 db，不做通用性让步。refcount / L2P / dedup /
snapshot 的交互不变量在 metadb 内部关死，onyx 只负责发意图和管物理块。

---

## 1. 职责划分（这是最重要的一条，先定死）

### metadb 拥有（逻辑 / 决策 / 原子性）

- L2P 表（per-volume paged COW tree）
- 全局 PBA refcount 表（shard B+tree）
- dedup_index + dedup_reverse（LSM）
- snapshot / clone 的 L2P 子树生命周期
- **所有 refcount ± 的决策权**：给定一个 remap / range-delete / drop_snapshot 意图，
  metadb 内部算出哪些 pba 该 incref、哪些该 decref、哪些要抑制（leaf-rc-suppress）、
  哪些到零需要联动清 dedup_index
- 跨表原子性（WAL + apply_gate）

### onyx 拥有（物理 / 策略）

- SpaceAllocator（BTreeSet 自由链表，strip 对齐，extent 合并）
- LV3 物理块读写、O_DIRECT、RAID-aware strip 聚合
- WriteBuffer 层、flusher 流水线、GC scan/rewrite 触发策略
- 内容哈希算法（SHA-256）、dedup 开关 / 背压策略 / DEDUP_SKIPPED flag 语义
- ublk 前端、zone 路由、metric 采集、dashboard IPC
- **被 metadb 告知 "pba X 归零" 后，还给 SpaceAllocator 自由池**

### 明确的边界

- onyx **不再持有**任何 refcount 条件判断逻辑、不再拼 RocksDB WriteBatch、
  不再维护自己的 striped lock 保护跨表原子性
- metadb **不感知**物理设备、不感知 4KB 内压缩 fragment 打包、不感知 SHA-256

---

## 2. 新增 Db 层 API

### 2.1 公开 `Transaction::commit_with_outcomes`

```rust
// metadb/src/tx.rs
impl<'db> Transaction<'db> {
    /// 原 pub(crate)，升级为 pub。
    /// 返回每个 op 对应的 ApplyOutcome（顺序 = ops 输入顺序）。
    pub fn commit_with_outcomes(self) -> Result<(Lsn, Vec<ApplyOutcome>)>;
}
```

**不变量**：`outcomes.len() == ops.len()` 严格成立，即便 op 在 bucketed apply 路径。

### 2.2 `Db::cleanup_dedup_for_dead_pbas`

```rust
impl Db {
    /// 对一批已 decref → 0 的 pba，一次事务清掉它们在
    /// dedup_index + dedup_reverse 的残留。
    ///
    /// 内部步骤（原子提交）：
    ///   1. multi_scan_dedup_reverse_for_pba(&pbas) 拿到每个 pba 的 hash 列表
    ///   2. 对每个 hash 做 get_dedup；若 entry.pba == 目标 pba 才发 DedupDelete
    ///      （防止 hash 已被另一 pba 重新注册的误删）
    ///   3. 无条件发 DedupReverseDelete((pba, hash))
    ///   4. 全部 ops 打到一条 WAL 记录，单次 commit
    ///
    /// 调用方：writer / dedup hit / drop_snapshot 返回 freed_pbas 后的 cleanup 异步线程。
    pub fn cleanup_dedup_for_dead_pbas(&self, pbas: &[Pba]) -> Result<Lsn>;
}
```

**幂等性**：DedupDelete / DedupReverseDelete 本身是 tombstone，replay 安全。

**现有 API `multi_scan_dedup_reverse_for_pba` 保留**，作为此函数的子调用暴露给 verify / 诊断工具。

---

## 3. 新增 WalOp

### 3.1 `WalOp::L2pRemap`（替代 L2pPut + Incref + Decref 的核心热路径原语）

```rust
pub enum WalOp {
    // ...
    L2pRemap {
        vol_ord: VolumeOrdinal,
        lba: Lba,
        new_value: L2pValue,
        /// hit-path 用的 target-pba liveness guard。
        /// Some((pba, min_rc)) = apply 时读 refcount(pba)，< min_rc 整条 op 跳过（Rejected）。
        /// None = 无条件 apply（miss-path 或内部调用）。
        guard: Option<(Pba, u32)>,
    },
}
```

**new_pba 从哪来**：Onyx `BlockmapValue` 编码的头 8 字节（BE）就是 pba，apply 直接
`Pba(u64::from_be_bytes(new_value.0[0..8].try_into().unwrap()))`，零拷贝零成本。
old_pba 类似从 `L2pPrev` 的头 8 字节取。**metadb 此处放弃 "L2pValue 不透明" 的抽象**
—— Onyx 是 metadb 唯一 client，抽象换 16B WAL 膨胀（每个 L2pRemap × 亿级 ops）不划算。

**编码**：`tag(1) + vol_ord(2) + lba(8) + new_value(28) + guard_tag(1) + [guard_pba(8) + min_rc(4)]`
  有 guard 时总长 60B，无 guard 时 48B。
  （`VolumeOrdinal` 在 metadb 内部是 `u16`，见 `src/types.rs`；现有 `L2P_PUT` / `L2P_DELETE`
  也用 2B vol_ord。本规格跟齐，不单开一个 4B 变体。）

**Apply 语义（有序，在 apply_gate.read + 对应 shard mutex 内）**：

1. 若 `guard = Some((gp, min))`：读 `refcount(gp)`，若 `< min` → **整条 op 视为 no-op**，
   ApplyOutcome = `L2pRemap { applied: false, .. }`，L2P 和 refcount **都不动**
2. 执行 `tree.insert_at_lsn(lba, new_value, lsn)`，记下 `prev: Option<L2pValue>`。
3. 决定 `decref(old.pba)` / `incref(new.pba)`，**按 audit 不变式**："rc(pba) = 集合
   `{(V, lba, value_28B) | live ∪ all live snaps}` 中 `head_pba(value)==pba` 的条目数"
   是否变化：

   | `prev` | new vs old | snap pins (V,lba,old)? | snap pins (V,lba,new)? | decref(old.pba) | incref(new.pba) |
   |---|---|---|---|---|---|
   | None | —          | —    | yes | 否 | 否 |
   | None | —          | —    | no  | 否 | **是** |
   | Some | same value | —    | —   | 否 | 否（net 0）|
   | Some | diff       | yes  | yes | 否（pin）| 否（pin）|
   | Some | diff       | yes  | no  | 否（pin）| **是** |
   | Some | diff       | no   | yes | **是** | 否（pin）|
   | Some | diff       | no   | no  | **是** | **是** |

   "snap pins (V, lba, X)" = 任意活快照 `S of V` 满足 `S.l2p[V][lba] == X`（**全 28B
   value 比较**，head_pba 一致但 salt/其他字段不同算两条 distinct tuple）。

   实现两段式：
   - **快筛**：rc 树拿 `pba.birth_lsn`；若 `birth_lsn > min(snap.created_lsn for snap of V)`
     → 该 pba 的内容在所有活快照之后才出生，必然不在任何快照 L2P 里 → 不 pin。
   - **精确比对**：通过 `paged_tree.get_at(snap_root, lba)` 走每个活快照的 L2P 取值，与
     `target_value` 比对（`==` 全 28B）。snap 读用同一 shard 的 PagedL2p 实例，无额外锁。

   **替代关系**：本节是旧的 `leaf_was_shared` 规则（只在 leaf 第一次 COW 时正确，后续
   覆写就漏判）的修复版。详见 §4.4。

4. 收集 `freed_pba`：仅当 step 3 执行了 decref **且** 该 pba 的 refcount 因此从 >0 降为 0 时，
   ApplyOutcome 带 `freed_pba = Some(old.pba)`；否则 None。

**ApplyOutcome**：

```rust
pub enum ApplyOutcome {
    L2pRemap {
        applied: bool,              // false 仅当 guard 拦截
        prev: Option<L2pValue>,     // 旧 L2P 值
        freed_pba: Option<Pba>,     // decref → 0 的 pba（给 onyx 释放物理块）
    },
    // ...
}
```

**WAL body 幂等性**：WAL 记录里存 `guard` 原值 + 原 `new_value`；replay 时 apply 对同一 op
产生相同结果的前提是 refcount/L2P/leaf-rc 状态一致，由 `page.generation >= lsn` 守护
（现有机制）。guard 拦截的结果在回放时也一致（同一个 refcount 值）。

#### 3.1.1 onyx 如何从 ApplyOutcome 反推"释放多少物理块"

metadb **不感知** packer / 压缩单元 / 4KB slot 打包。它只告诉 onyx "这个 pba 没人引用了"
（`freed_pba = Some(pba)`）。**释放规模由 onyx 解码对应 `prev: L2pValue` 自己算**。

`BlockmapValue` 布局里 onyx 要读的两个字段（从 `L2pPrev.0` 里按 Onyx 编码取）：

- `slot_offset`（byte 25..27，`u16` BE）
- `unit_compressed_size`（byte 9..13，`u32` BE）

决定释放多少 block（每 block = 4096 字节）：

| `slot_offset` | `unit_compressed_size` | 释放规模 |
|---|---|---|
| `> 0` | 任意 | 打包槽非首 fragment → **1 block**（整个 4KB 槽）|
| `= 0` | `< 4096` | 打包槽首 fragment → **1 block** |
| `= 0` | `≥ 4096` | passthrough 压缩单元 → `ceil(unit_compressed_size / 4096)` blocks，起始 = `pba` |

onyx 侧实现骨架（伪码）：

```rust
for outcome in outcomes {
    if let ApplyOutcome::L2pRemap { freed_pba: Some(pba), prev: Some(prev_value), .. } = outcome {
        let bv = BlockmapValue::decode(&prev_value.0);
        let nblocks = if bv.slot_offset > 0 || bv.unit_compressed_size < BLOCK_SIZE {
            1                                                   // packed slot
        } else {
            (bv.unit_compressed_size as usize).div_ceil(BLOCK_SIZE)  // passthrough extent
        };
        space_allocator.free(pba, nblocks);
    }
}
```

**为什么这样行**：
- 打包槽里 N 个 fragment 共享 pba，refcount(pba) = N。逐个 decref 直到 N→0，最后一次
  decref 的 `prev` 里 `slot_offset` 和 `unit_compressed_size` 指向最后一个 fragment，
  但判据 (`slot_offset > 0` 或 `size < 4KB`) 只用来**区分打包 vs passthrough**，不用于
  计算释放大小 —— 打包槽永远释放 1 block（整个 4KB 物理槽）。
- passthrough 单元的 N 个 LBA 条目共享 pba、共享同一个 `unit_compressed_size`（整个单元的
  压缩长度），每条 LBA refcount 贡献 1。最后一次 decref 的 `prev.unit_compressed_size`
  就是完整单元大小，直接算 extent。
- metadb 保证 `freed_pba` 和 `prev` 在同一个 `ApplyOutcome` 里原子对应（同一次 remap 的前
  后状态），onyx 不需要二次查询。

### 3.2 `WalOp::L2pRangeDelete`

```rust
pub enum WalOp {
    // ...
    L2pRangeDelete {
        vol_ord: VolumeOrdinal,
        start: Lba,
        end: Lba,          // 半开区间 [start, end)
        /// Plan 阶段扫出的 (lba, pba) 列表 —— 确定性塞进 WAL body。
        /// 为每条发 decref（同样走 leaf-rc-suppress 规则）。
        captured: Vec<(Lba, Pba)>,
    },
}
```

**Plan 阶段**（Db 层 API 入口）：持 `drop_gate.read()` + 对应 L2P shard mutex 读，
扫 `[start, end)`，把每个 `(lba, L2pValue)` 的 pba 抽出来塞进 `captured`。
扫完释放锁，提交 WAL（group commit）。

**Apply 语义**：

1. 对每个 `(lba, old_pba)`：
   - 执行 `tree.delete_at_lsn(lba, lsn)`，拿到 `leaf_was_shared`
   - leaf shared → 抑制 decref；否则 `decref(old_pba, 1)`
2. 收集 `freed_pbas`

**ApplyOutcome**：

```rust
ApplyOutcome::RangeDelete {
    freed_pbas: Vec<Pba>,
}
```

**边界**：单次 range delete 的 `captured.len()` 上限 65536（超过拆多次），避免单条 WAL body 失控。

### 3.3 `WalOp::DropSnapshot` 扩展

现有 `DropSnapshot { id, pages: Vec<PageId> }` 扩展为：

```rust
WalOp::DropSnapshot {
    id: SnapshotId,
    pages: Vec<PageId>,
    /// Plan 阶段用 diff_with_current 算出来：snapshot 有但 current 已不同的
    /// (lba, pba) 对应的 pba。每个发一次 decref。
    pba_decrefs: Vec<Pba>,
}
```

**Plan 阶段**：现有的持 `drop_gate.write()` + `apply_gate.write()` 逻辑不变；
额外调 `diff_with_current(snap) → Vec<DiffEntry>`，筛 "snap 有值但 current 不同" 的项，
把 snap 侧 pba 装进 `pba_decrefs`。

**Apply 语义**：现有 pages 释放逻辑不变；每条 pba_decrefs 发 `decref(pba, 1)`。
（drop_snapshot 阶段不需要 leaf-rc-suppress —— 我们正是在释放这些 leaf，pba 该走就走）

**ApplyOutcome**：

```rust
ApplyOutcome::DropSnapshot {
    freed_leaf_values: Vec<L2pValue>,   // 现有
    pages_freed: usize,                 // 现有
    freed_pbas: Vec<Pba>,               // 新增：decref → 0 的 pba
}
```

Adapter 拿到 `freed_pbas` 后：一发 `cleanup_dedup_for_dead_pbas` + 一次
`SpaceAllocator.free_many`（异步线程 OK，不阻塞 drop）。

---

## 4. 关键不变量（porting from onyx war stories）

以下不变量来自 onyx 当前代码 / memory bank / soak 事故复盘，必须在 metadb 内部守住，
并有对应 proptest / fault-injection 覆盖。

### 4.1 `newly_zeroed` 严格性

ApplyOutcome 里报告的 `freed_pba` / `freed_pbas` 集合，必须严格等于
"本次 commit 导致 refcount 从 >0 变 0 的 pba 集合"。不多不少。

反例（必须测到拒绝）：
- 某 pba 本次只被 decref，但未到 0 → 不在 freed 集合
- 某 pba 本次被 decref 到 0 又 incref 回 1（同 batch 内）→ 不在 freed 集合
- 某 pba 本次被 decref 且之前就是 0 → 不在 freed 集合

### 4.2 packed slot decref 聚合

一条 WAL 里包含 N 个 `L2pRemap` 且它们的 `L2pPrev.pba` 相同（打包槽同 pba 多 LBA 场景）时：
refcount 最终 delta = -N + 0（如果全部 leaf shared）或 -N + (new_pba incref 总数)。

metadb 的 apply 不做 dedup / 合并这些 decref —— 逐条 op 独立决策，但结果一致
（refcount 是可加的）。

### 4.3 guard 的原子性

`L2pRemap { guard: Some((gp, min)) }` 的 guard 判断和后续的 put/incref/decref
**必须在同一把锁内完成**。允许的唯一中断点是：guard 不满足时 early-return，整条 op 不动。

反例（必须测到拒绝）：
- apply 线程 A 读到 refcount(gp) = 1，切出去
- apply 线程 B 走 decref 把 gp 干到 0 + 触发 cleanup
- apply 线程 A 回来继续 apply —— 禁止

### 4.4 birth/death LSN snapshot suppression

历史上这条规则叫 "leaf-rc-suppress"——抑制条件是 "leaf page rc > 1 时 decref"。
错的：leaf 第一次 COW 后变成 private（rc=1），后续在该 leaf 上的覆写就漏掉
"snap 仍通过同 (V, lba) 持有 old_pba" 这个事实，造成 actual=0 而 snapshot
还引用着的 audit 失败。

现行规则（精确，audit 一致性"任何时刻"成立，不限于 drop 之后）：

- 写路径在 `apply_gate.read` + L2P shard mutex 下：
  - **快筛**：`pba.birth_lsn > min(snap.created_lsn for snap of V)` → 不 pin。
  - **精确**：每个活快照 `S of V`，读 `S.l2p[V][lba]`，与 target value 全 28B 比较。
- decref 仅当 (V, lba, old_value) 不被任何活快照持有；incref 仅当 (V, lba, new_value)
  不被任何活快照持有。
- 不需要任何 deadlist、不需要 drop_snapshot 补发；drop 走 `diff_subtrees` 计算的
  `pba_decrefs` 仍然存在但**只覆盖**"snap 有、current 已不同"的 lba 的本地 decref。
- 等价表达式（audit "distinct (V, lba, value_28B) tuples" 计数）：

  ```
  live_loses_old = prev.is_some() && prev != Some(new_value)
  live_gains_new = prev != Some(new_value)
  do_decref = live_loses_old && !snap_pins_old
  do_incref = live_gains_new && !snap_pins_new
  ```

`pba.birth_lsn` 在 rc 0→1 转换时记 op 的 `lsn`，rc≥1 改动时保持不变；rc→0 时连同
entry 一起从 rc 树删除（下一次 0→1 重新打 birth_lsn）。replay 幂等：每条 WAL op 的
`lsn` 一致，birth_lsn 决策可重现。

`SnapshotEntry.created_lsn = take_snapshot 时刻的 last_applied_lsn`（WAL lsn 空间），
和 `pba.birth_lsn` 同空间，可以直接 `<=` 比较。Manifest body version 7 起生效。

### 4.5 drop_snapshot 和 dedup hit 的序列化

`drop_snapshot` 期间持 `drop_gate.write() + apply_gate.write()`，所有 writer / dedup hit
的 commit 都被阻塞。drop_snapshot 结束后，adapter 异步发 `cleanup_dedup_for_dead_pbas`，
期间可能有新的 dedup hit 对 "刚被 cleanup 的 pba" 做 guard 检查 —— guard 正确拒绝。

### 4.6 hole-fill 物理顺序（这个是 metadata 层面的对应保证）

onyx 的 hole fill 要求 "metadata 验证通过" 先于 "物理覆盖"。metadb 对应的责任：
`L2pRemap { guard }` 在 guard 不过时**绝对不能改 L2P**（哪怕部分改）。由 "guard 检查在
整条 op 开头、状态未变" 保证。

### 4.7 删卷 / range delete 按 pba 聚合

`L2pRangeDelete` 扫出的 `captured` 可能有多条 `(lba, pba)` 指向同一 pba（dedup 场景）。
apply 为每条发 decref，refcount 正确累减 —— 单 pba 的 refcount 从 N 到 0 要发 N 次
decref，不能聚合成 1 次（否则 freed 报告漂）。

---

## 5. 测试矩阵（Phase 8a 门控）

### 5.1 单元 / 属性

- `L2pRemap` guard 的四象限：(hit / miss) × (leaf shared / exclusive)
- `L2pRangeDelete` 跨 shard + 含 dedup 重复 pba
- `DropSnapshot` 在 dedup 场景下的 freed_pbas 正确性
- `cleanup_dedup_for_dead_pbas` 的竞态：hash 被重新注册到另一 pba 不误删

### 5.2 Proptest（reference model 对照）

Reference model：`HashMap<(VolumeOrdinal, Lba), L2pValue>` + `HashMap<Pba, u32>` +
`HashMap<Hash32, (Pba, live)>`。

随机 op 序列：`L2pRemap / L2pRangeDelete / take_snapshot / drop_snapshot /
clone_volume / cleanup_dedup_for_dead_pbas`，长度 ≥ 10k，种子 ≥ 256。

每轮收尾：
- `iter_refcounts()` vs reference model 的 refcount 表 —— 严格相等
- 每个 live volume 的 `range(..)` vs reference L2P —— 严格相等
- 所有 `snapshot_view(id).range(..)` vs reference snapshot 快照 —— 严格相等

### 5.3 故障注入（用现有 `FaultController`）

注入点组合测试：
- `CommitPostWalBeforeApply` × 上述所有 op 类型
- `CommitPostApplyBeforeLsnBump` × 上述所有 op 类型
- `DropSnapshot` 的 pages 释放中途 crash → replay 后 freed_pbas / refcount 一致

### 5.4 Soak（metadb-soak）

新 op 混入 workload，24h 跑过：
- op mix 模拟 onyx 真实比例（remap 60% / range_delete 5% / snapshot 1% / dedup hit via guard 30% / cleanup 4%）
- 进程重启 × ≥ 10 次，每次重启后 `metadb-verify` 无 warn/err
- `iter_refcounts` 总和守恒（相邻 snapshot 间 delta = incref 总 - decref 总）

---

## 6. 显式非目标

- **不做 point-in-time refcount snapshot**。refcount 仍是 running tally，
  snapshot 只冻结 L2P。
- **不做 dedup_index 的 snapshot**。dedup 是全局 LSM，snapshot 内的 dedup hit 依赖
  "refcount > 0" 的动态判断。
- **不为泛用性保留抽象**：L2pRemap 就是 Onyx 的意图，不为 "其他 db client" 留空间。
- **不加 rollback / savepoint**：Transaction 单次提交，失败全丢。

---

## 7. 删除 / 简化（Phase A 顺手做的）

- `Db::incref_pba` / `Db::decref_pba` / `Db::put_dedup` / `Db::delete_dedup` 等独立入口
  **保留**，但标注 "normal write path 不要直接用，用 L2pRemap"。给诊断工具 / 特殊
  维护路径留接口。
- `Transaction::insert` / `delete` **保留**，给非 refcount 相关场景用（比如纯测试）。
- 旧版 `DropSnapshot` op 的 WAL 兼容性：**不做兼容**。metadb 自身版本号 bump，
  旧 WAL 不读（和 onyx "rocksdb 一次进历史" 同步）。

---

## 8. 性能目标（不倒退）

以 metadb 现在 benchmark（`metadb_bench_wins` memory 条目）为基线：

- 单 remap 吞吐（10k ops，guard=None）：**不低于现有 insert + incref + decref 三 op 串行的 90%**（理想情况应持平或更快，因为一条 WAL 记录 vs 三条）
- 批 remap（1k ops per Transaction）：**达到或超过** 现有纯 insert 批提交吞吐
- guard=Some 的 remap 相对 guard=None 开销 < 5%（一次 refcount 点 get）

不达标是设计问题，不是优化问题 —— merge 前必须解决。

---

## 9. 验收清单（Phase A → Phase B 门控）

- [ ] §2 所有 API 签名 landed、文档齐全
- [ ] §3 所有 WalOp 编解码 + apply + replay 实现、WAL schema 版本 bump
- [ ] §4 所有不变量有对应 proptest 或 integration test，全绿
- [ ] §5.1 / 5.2 / 5.3 测试全绿
- [ ] §5.4 soak 24h 过（`metadb-verify` 无报错）
- [ ] §8 性能目标达成
- [ ] metadb CLAUDE.md 的 "和 onyx-storage 的关系" 段更新，反映新 API
- [ ] 标注本 v1 文档完成 —— Phase B 开工

---

## 10. 变更记录

- **v1** 2026-04-23：初稿。snapshot 相关的 leaf-rc-suppress + drop_snapshot
  pba_decrefs、guard 语义、proptest 要求一次纳入。
  2026-04-23 修正：§3.1 `L2pRemap` 编码 `vol_ord` 宽度从 4B 改为 2B，跟齐
  `VolumeOrdinal = u16` 及现有 `L2P_PUT` / `L2P_DELETE` 编码；相应总长无 guard
  从 50B 改为 48B、有 guard 从 62B 改为 60B。属 schema 误记的字面修正，语义
  不变。
