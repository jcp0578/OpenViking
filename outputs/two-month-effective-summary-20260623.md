# 两个月有效信息总汇总（本地 + 远端 outputs，2026-06-23）

## 1. 范围与原则

这份文档基于以下信息源整理：

- 本地：`/home/jcp/Agent/code/OpenViking/outputs`
- 远端：`/home/jcp/agent/code/OpenViking/outputs`

整理原则：

- 只保留当前仍有效、可交接、可作为后续决策依据的信息
- 已被更新结论覆盖的旧实验细节不再展开
- 中间态、重复诊断、无结论价值的 run 噪声不再呈现

## 2. 产出全貌

截至当前：

- 本地 `outputs` 文件数：`284`
- 远端 `outputs` 文件数：`148`
- 远端独有文件：`61`
- 本地独有文件：`197`

可按工作线收敛为三大类：

1. **LoCoMo 优化与评测主线**
2. **Preprocessor / benchmark / 远端环境工程化**
3. **非 LoCoMo 的贡献统计、review、工作总结类材料**

其中：

- 本地更偏高层总结、分析、交接文档
- 远端更偏 LoCoMo 原始运行产物、gate、health、CSV/meta/txt 证据

远端独有的 `61` 个文件主要是 `health`、`covcontract`、`travelyear`、`shrink58`、`after48`、`fallback` 等阶段性 LoCoMo run 产物。它们在本汇总中只作为“运行路径、环境健康、阶段性 gate 取证”的支撑材料，不展开为最终 accuracy baseline；最终口径仍以已写入本地高层文档并经过有效性判定的结果为准。

## 3. 当前最重要的主线：LoCoMo

### 3.1 任务目标已经收敛

LoCoMo 后续任务的统一验收口径已经明确：

- 相比 `off` 基线，准确率不能明显下降；若下降，最多容忍约 `3%`
- `token per success` 至少下降 `10%`
- 只有有效 accuracy run 才能作为验收证据
- `timeout`、`HTTP 5xx`、空答案、`usage.total_tokens=0`、judge/provider 污染都不能当作代码效果证据

这已经成为当前最核心的执行约束。

### 3.2 当前稳定基线

当前最常用的对照口径如下：

| 口径 | version | correct | tokens | token/success |
|---|---|---:|---:|---:|
| all samples 0-9 | best off | `805/987` | `10,690,745` | `13,280` |
| all samples 0-9 | best on/recalltrim | `814/987` | `10,499,507` | `12,899` |
| sample5/6/9 | best off | `196/230` | `2,632,865` | `13,432.98` |
| sample5/6/9 | cheapest on reference | `167/230` | `1,904,834` | `11,406.19` |
| sample5/6/9 | current accuracy-positive on | `188/230` | `2,325,689` | `12,370.69` |

解释：

- recalltrim 在 all samples `0-9` 大盘上略有收益
- 但 `sample5/6/9` 是明确的退化敏感集，不能被大盘平均值掩盖

### 3.3 当前不可接受的基线

当前 latest-code Gold baseline 在 `sample5/6/9` 上不可接受：

| sample | accuracy | tokens/success |
|---|---:|---:|
| `sample5` | `43/66 = 65.15%` | `13024.0` |
| `sample6` | `69/86 = 80.23%` | `10315.2` |
| `sample9` | `55/78 = 70.51%` | `11510.1` |
| overall | `167/230 = 72.61%` | `11406.2` |

结论：

- 成本低，但准确率损失过大
- 可以作为“低成本坏基线”参考
- 不能作为接受版本

### 3.4 LoCoMo 已经沉淀出的稳定结论

当前可确认的稳定结论有四条：

1. `memories=0` 不是可靠的 durable memory 覆盖率指标
   - direct-OV full run 路径里，`memory_count` 有统计失真
   - 不能据此直接断言 extraction 失败

2. 失败层级不能再简单归因于 query-side ranking
   - 多轮实验已基本证伪“继续加 `memory-ranking.ts` 强规则”这条主线

3. 更可信的优化层面是：
   - extraction coverage / durable memory surface form
   - retrieval / injection selection
   - time anchoring 与事件粒度

4. 严格区分有效 run、invalid run、judge/provider 污染是必须的
   - 否则会把环境和模型层问题误判成代码收益或退化

### 3.5 已确认有价值、但应谨慎继承的候选方向

目前只有两类内容值得作为新一轮任务的候选继承项：

1. **通用 recall 集成修复思路**
   - 例如 `client.ts` 中 canonical URI retry 这类真实修复过 recall 集成 bug 的思路
   - 继承方式应是在干净基线上重新 cherry-pick 或重实现，并重新验证；不应直接继承当前脏工作区文件状态

2. **运行与验收方法论**
   - health gate
   - invalid run 判定
   - judge 分离意识
   - 小 gate -> 子集 gate -> 完整 sample gate 的分层流程

### 3.6 已被大量证伪的方向

以下方向不应作为新一轮主线继续投入：

- `memory-ranking.ts` 式 query-side 强规则
- answer normalization
- 修改 benchmark / judge / 测试框架来“修结果”
- 围绕单题做局部 overfit
- 把 extractor-only / retrieval-only / smoke 诊断冒充 accuracy evidence

### 3.7 远端运行层面的稳定教训

远端环境已经暴露出几个稳定问题：

1. benchmark 环境会被共享状态污染
   - 共享 container
   - 共享 gateway
   - 共享 `OPENCLAW_STATE_DIR`
   - 共享 `openclaw.json`
   - 共享锁文件 `/tmp/locomo-openclaw-benchmark.lock`

2. provider 健康必须前置验证
   - 仅 `OpenViking /health`、`gateway /health` 正常还不够
   - 必须再验证最小 OpenClaw QA 请求返回真实答案且 `usage.total_tokens > 0`

3. 若不满足健康闸门，应直接停止 LoCoMo gate
   - 否则只会继续制造 invalid run

### 3.8 LoCoMo 后续最合理的重启方式

已经明确不建议在当前脏工作区上继续堆实验，而应：

1. 基于干净代码新开分支
2. 建立独占远端 runtime
3. 复刻 `off` 基线与 clean `on` 基线
4. 先跑一个至少 `30` 题的中等规模 gate
5. 再只引入一个小改动做对照

优先主线应是：

- durable memory 更短、更 answerable、更可检索
- conservative injection selection / recall budget 降成本

当前本地代码状态也支持这个判断：工作区里混有 `examples/openclaw-plugin/*`、`benchmark/locomo/openclaw/*`、`openviking/session/*`、prompt、tests、outputs 等多条实验线的未提交改动，其中仍包含 `memory-ranking.ts` 这类已被判定不应作为主线继承的候选改动。因此后续若继续实验，应先从干净分支建立因果边界，再逐项搬运已确认有价值的候选。

## 4. Preprocessor / benchmark / 环境工程化

### 4.1 已收敛的 Preprocessor 结论

Preprocessor 阶段已经有清晰结论：

不应保留的方向：

- `created_at -> chat date anchor`
- 低信息句删除
- 激进包装去重 / 纯格式瘦身
- 短 session 强制 fallback

历史阶段中相对低风险、但仍需在新基线上重验的候选路径：

- `[] tail` 清理
- 相对时间句保真
- 句级时间窗口抽取
- `Last Fri` 缩写支持
- 相邻同主题时间去重
- exact duplicate 才做 `fact -> source ref`
- 短 session facts floor `4 -> 6`

这些结论来自较早的 Preprocessor 阶段与局部样本验证。当前本地代码已经包含更大范围的 extraction、prompt、session、memory 相关未提交改动，因此这些路径不应被直接升级为长期默认策略，只能作为后续干净基线上的候选项重新验证。

### 4.2 已识别的 benchmark 口径问题

这条线最重要的工程结论不是某个 patch，而是运行口径问题：

1. QA autoCapture 会把同轮答案写回 memory，污染后续题召回
2. 不能只看 `relevant_memories_count=0` 判断 recall 没发生
3. 远端容器里后台 `nohup ... &` 跑 benchmark 不稳定，前台 / 长连接更可靠

### 4.3 已沉淀的环境与交接价值

这两个月已经形成了一批有复用价值的工程文档：

- benchmark 隔离思路
- 容器内 GW / OV 启停与配置路径
- handoff 操作文档
- preprocessor 阶段失效方向与低风险默认路径

它们的价值主要在于：

- 降低重复排障成本
- 防止 benchmark 口径继续漂移
- 为后续新一轮 LoCoMo 验证提供稳定运行框架

## 5. 非 LoCoMo 的有效产出

除 LoCoMo 外，outputs 中还沉淀出一组稳定的工作总结材料。

### 5.1 跨仓代码与工程贡献

当前可保留的高层事实：

- 作者 PR/MR：`16`
- 已合入 PR/MR：`15`
- 代码/配置类新增行：`6706`
- 代码/配置类文件：`77`
- 检视相关记录：约 `195+`
- 作者 Issue：`5`

涉及的主要方向：

- OpenViking / OpenClaw 插件能力建设
- oGMemory 多租能力
- KunpengRAG / OpenFuyao 部署工程化
- 测试与质量防护
- 代码检视与工程收敛

### 5.2 这些材料的定位

这部分文档已经足够支持：

- 工作总结
- 贡献统计
- review 评价
- 评优材料

不需要再重复回到原始 PR/MR 明细做二次汇总，除非目标变成正式对外材料。该部分采用既有统计文档口径，本次审视没有重新联网复核 GitHub / GitCode / Gitee 的实时状态。

## 6. 基于本地 + 远端信息后的总判断

如果只保留当前真正有效的信息，这两个月的工作可以收敛为下面这段话：

1. **LoCoMo 方向**
   已经完成了从大盘效果、Gold 验收规则、失败层级、远端 root cause、health gate 到重启建议的一轮完整收敛；当前最清晰的结论不是“已有可接受 patch”，而是“已经明确哪些方向值得继续、哪些方向不值得继续，以及新一轮应如何在干净基线上重新开始”。

2. **工程化方向**
   已经识别并沉淀了 preprocessor、benchmark 口径、远端容器环境、gateway / OV 运行方式等一批工程问题与操作经验，这些结论对后续验证工作有直接复用价值。

3. **团队与项目贡献方向**
   已经形成了较完整的跨仓贡献、代码检视、部署工程化、工作总结与评优材料，不需要再重复做低层统计。

## 7. 当前最值得保留的一页结论

如果后续只允许保留一页“最关键结论”，应保留这五点：

1. LoCoMo 的目标已经从“继续堆 patch”收敛为“在干净代码与独占环境上重跑，追求 accuracy 不明显下降且 token/success 降低 10%”。
2. `sample5/6/9` 是真实敏感集，不能用 all samples `0-9` 的平均收益掩盖它们的退化。
3. `memory-ranking.ts` 这类 query-side 强规则路线已经基本证伪，不应继续作为主线。
4. 远端 provider / runtime / judge 污染是过去大量无效结论的根源，health gate 必须前置。
5. 下一轮真正值得做的是：在干净基线上验证 extraction coverage / durable memory atomization，以及 conservative injection selection / recall budget，而不是继续围绕单题补丁打转。

## 8. 建议的下一步

如果这份总文档是为了后续交接或重新开题，最合理的下一步不是继续补总结，而是：

1. 基于这份文档做一个新的精简 Gold brief
2. 在干净代码上建立新分支
3. 搭独占远端环境
4. 先拿 clean baseline，再开始新一轮 LoCoMo 改动验证
