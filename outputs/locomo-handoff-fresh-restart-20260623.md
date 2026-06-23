# LoCoMo 工作交接与重启建议（2026-06-23）

## 1. 文档目的

这份交接文档用于在 **全新代码基线** 上重新启动 LoCoMo 优化任务。目标不是延续当前工作区里的所有实验状态，而是沉淀已经被 `outputs/` 证据支持的稳定结论，明确哪些方向值得继承，哪些方向应直接丢弃。

本交接主要基于以下文档：

- `outputs/locomo-effective-results-summary-20260608.md`
- `outputs/locomo-gold-regression-v1.md`
- `outputs/locomo-gold-regression-v1-results-20260610.md`
- `outputs/locomo-gold-regression-v1-analysis-20260610.md`
- `outputs/locomo-gold-regression-v1-followup-20260610.md`
- `outputs/locomo-remote-root-cause-20260609.md`
- `outputs/token_comparison.md`

## 2. 任务目标口径

后续任务应统一按这个口径执行：

- 相比 `off` 基线，准确率不能明显下降；若有下降，最多容忍约 `3%`
- `token per success` 至少下降 `10%`
- 只有 **有效 accuracy run** 才能作为验收证据
- `timeout`、`HTTP 5xx`、空答案、`usage.total_tokens=0`、judge/provider 污染都不能直接当成代码效果证据

当前最常用的对照口径：

| 口径 | version | correct | tokens | token/success |
|---|---|---:|---:|---:|
| all samples 0-9 | best off | `805/987` | `10,690,745` | `13,280` |
| all samples 0-9 | best on/recalltrim | `814/987` | `10,499,507` | `12,899` |
| sample5/6/9 | best off | `196/230` | `2,632,865` | `13,432.98` |
| sample5/6/9 | cheapest on reference | `167/230` | `1,904,834` | `11,406.19` |
| sample5/6/9 | current accuracy-positive on | `188/230` | `2,325,689` | `12,370.69` |

## 3. 已确认的稳定结论

### 3.1 大盘上 recalltrim 有收益，但收益不均匀

`outputs/locomo-effective-results-summary-20260608.md` 的稳定结论是：

- 全量 `sample0-9` 上，recalltrim 相比 best off 略有精度提升，也略省 `token/success`
- 但 `sample5/6/9` 是明确退化集，尤其 `sample9` 风险最大
- 因此不能用大盘平均收益掩盖 `sample5/6/9` 的局部退化

这意味着后续任何新方案都必须把 `sample5/6/9` 当成主验收面，而不是只看 all-sample aggregate。

### 3.2 `memories=0` 不是可靠的 durable memory 覆盖率指标

`outputs/locomo-gold-regression-v1-analysis-20260610.md` 已确认：

- `phase_a_off.py` 里的 `memory_count` 统计依赖 `memories_extracted`
- 在 direct-OV full run 路径里，这个字段可能缺失或没有回填
- 因此 `memories=0` 不等于“没有写入 durable memories”

这条结论很重要，因为它排除了一个常见误判：不能仅凭 benchmark summary 里的 `memories=0` 就断定 extraction 整体失效。

### 3.3 当前 latest-code Gold baseline 在 sample5/6/9 上不可接受

`outputs/locomo-gold-regression-v1-results-20260610.md` 给出的完整 Gold baseline：

| sample | accuracy | tokens/success |
|---|---:|---:|
| `sample5` | `43/66 = 65.15%` | `13024.0` |
| `sample6` | `69/86 = 80.23%` | `10315.2` |
| `sample9` | `55/78 = 70.51%` | `11510.1` |
| overall | `167/230 = 72.61%` | `11406.2` |

它的意义是：

- token/success 很低，但准确率损失过大
- 这可以作为“低成本但不可接受”的参考版本
- 它不能作为最终接受版本

### 3.4 失败层级不能简单归因于 ranking

`outputs/locomo-gold-regression-v1-analysis-20260610.md` 和 follow-up 文档的共同结论是：

- 已经多次证伪“再加一层 query-side ranking 强规则就能解决问题”
- 更强的怀疑对象是：
  - extraction coverage / durable memory surface form
  - retrieval / injection selection
  - time anchoring 与事件粒度

换句话说，后续任务不应该再把 `memory-ranking.ts` 作为主要突破口。

## 4. 已证明有价值的工作

### 4.1 retrieval / recall 集成层确实存在过真实问题

`outputs/locomo-remote-root-cause-20260609.md` 已确认过一个真实且通用的 recall 集成问题：

- 早期 remote `on` 子集结果无效，部分原因是 harness/runtime 没真正切到目标代码
- 随后又确认 `client.ts` 的 namespace / canonical URI retry 能修复 auto-recall 搜索目标 URI 形状不兼容的问题
- 该问题修复后，代表性子集结果出现大幅恢复

这条结论可继承，但要注意：

- 它证明的是“retrieval 集成层曾有真实 bug”
- 不等于当前所有退化都还能靠同类补丁解决

### 4.2 严格区分有效 run、invalid run、judge 污染是必要的

follow-up 文档后半段已经反复证明：

- provider timeout
- provider quota
- judge 429
- shared lock / runtime contamination

都会制造“看起来像准确率变化”的假象。

这部分经验本身就是后续任务的必备约束：如果不先做健康闸门，任何对比都可能失真。

### 4.3 sample6 q68-q78 的窄门 rejudge 结果说明极小候选可以有局部收益

follow-up 文档末尾的修正结论里：

- `u` 在原始 judge 口径下被误看成 `0/11`
- 分离 generation / token / judge 后，重判得到 `9/11`
- 与 `t` 的同范围比较里，准确率相同，但 token 下降约 `11.88%`

这条证据说明：

- 极小 injection-selection 候选不是完全没有希望
- 但它只证明了一个 **窄门样本**，还不具备全局接受资格

## 5. 已证伪或不值得继续继承的方向

以下方向在已有文档里已经被大量证伪，不建议在新一轮任务中继续投入：

### 5.1 query-side 强规则

包括但不限于：

- `memory-ranking.ts` 强语义规则
- 围绕单题模板加硬编码筛选
- 为某类问题单独加 query classifier 再改检索逻辑

原因：

- 很容易造成 sample 局部 overfit
- 已多次出现 `sample5` 小升、`sample6/9` 受伤，或相反
- 与“泛化、可证伪、可扩展到完整 sample”的目标不一致

### 5.2 answer normalization

已明确不应通过：

- 改 benchmark / judge / 测试框架
- 改 answer normalization
- 用后处理把答案修正成更像 gold

来冒充模型或记忆系统本身的提升。

### 5.3 把 diagnostic run 当 accuracy run

以下都只能作为诊断，不应再作为 acceptance evidence：

- extractor-only probe
- retrieval-only probe
- 单题 smoke
- timeout 污染 run
- `total_tokens=0` run
- judge 侧报错但未分离重判的 run

## 6. 当前阻塞与环境教训

follow-up 文档后半段给出的关键环境结论如下：

### 6.1 远端 benchmark 环境会被共享状态污染

典型污染源：

- `/tmp/locomo-openclaw-benchmark.lock`
- 其他 benchmark 调用方复用同一 container / gateway / state dir
- `openclaw.json` 被并发改写
- gateway 被外部终止

所以后续必须保证：

- 独立 gateway 端口
- 独立 `OPENCLAW_STATE_DIR`
- 独立 config 路径
- 最好独立 container；至少也要有 benchmark 锁

### 6.2 provider 健康必须先于 LoCoMo 运行

已有文档已经验证过多种 provider 层污染：

- timeout
- quota exceeded
- 生成侧可回答但 judge 侧失败
- smoke 可运行但正式 gate 被污染

后续的硬性前置条件应是：

1. `OpenViking /health` 正常
2. `gateway /health` 正常
3. 最小 OpenClaw QA 请求返回真实答案
4. `usage.total_tokens > 0`

只要第 3 或第 4 条不满足，当次 LoCoMo gate 就应直接判为 invalid，不继续扩大测试。

## 7. 为什么建议基于全新代码重新开始

建议重新开始，而不是在当前工作区继续堆实验，原因很直接：

1. 当前工作区已经积累了大量未提交修改，其中混合了不同阶段、不同假设、不同目标的实验代码。
2. `outputs/` 里已经足够明确地说明了哪些方向有效、哪些方向无效，没有必要把旧实验状态继续当作“半成品基线”。
3. 后续目标是“精度不降太多且 token/success 明显下降”，这要求对每个改动的因果边界更清楚；脏工作区会模糊边界。
4. 环境污染、judge 污染、provider 污染都很多，新的任务应从更严格、更干净的 execution contract 开始。

因此，推荐的重启方式不是“续修当前状态”，而是：

- 选一个干净代码基线
- 只显式搬运那些已经有稳定证据支持的低风险改动
- 重新建立运行与验收口径

## 8. 基于全新代码的重启建议

### 8.1 代码基线建议

推荐从干净分支重新开始：

- 基于远端/主分支最新稳定代码 checkout 新分支
- 不直接继承当前工作区里的混合实验状态
- 不默认带入所有 `memory-ranking.ts`、prompt、extraction、diagnostic 脚本改动

### 8.2 默认保留项与默认排除项

**默认可保留候选：**

- 已被证明属于通用 recall 集成修复的 `client.ts` canonical URI retry 思路
- benchmark 运行流程中的健康闸门、invalid run 判定、judge 分离意识

**默认排除：**

- query-side 强规则
- answer normalization
- 针对 sample5/6/9 的局部 hardcode
- 没有完整 gate 证据支撑的 injection-selection 规则

### 8.3 新一轮任务的建议顺序

1. 先建立干净环境与独占 runtime。
2. 先跑最小健康闸门。
3. 复刻 `off` 参考基线与当前 clean `on` 基线，确认口径一致。
4. 选一个 **至少 30 题** 的 gate 做首轮比较，不再过度依赖 1-2 题小门判断方向。
5. 优先从两类方向选候选：
   - extraction coverage / durable memory atomization
   - conservative injection selection / recall budget
6. 每次只引入一个小改动，并记录：
   - accuracy delta
   - token delta
   - token/success delta
   - `CORRECT -> WRONG`
   - `WRONG -> CORRECT`
7. 只有当中等规模 gate 通过后，再扩大到 `sample5/6/9` 全集，最后再决定是否扩到 all samples `0-9`。

## 9. 新一轮任务的推荐主线

结合现有输出证据，新的优化主线应优先考虑：

### 主线 A：durable memory 更短、更 answerable、更可检索

关注点：

- 小事件是否被写成 standalone event memory
- 相对时间是否被规范化为 durable calendar range
- 图片/文本混合事实是否被统一落成可检索事件
- 不要只落到大 person/entity card

### 主线 B：更保守的 injection selection / recall budget

关注点：

- 减少中等长度 event bundle 的过宽注入
- 优先保留直接 answer-bearing event/fact
- 不靠 query-side 强判断做 aggressive trim

### 不建议作为主线的方向

- 继续强化 `memory-ranking.ts`
- 再做单题对齐
- 再围绕 judge/output 后处理做修补

## 10. 交接后的第一步动作建议

如果由新一轮执行者接手，第一步建议不是立刻改代码，而是：

1. 新建干净分支与独占远端运行环境。
2. 用当前确定的 provider key 和 base URL 重新做最小 QA 健康闸门。
3. 在干净代码上重放一个 30+ 题 gate，得到新的 clean on baseline。
4. 只有 clean baseline 稳定后，才开始引入第一条小改动。

## 11. 一句话结论

过去这轮工作的真正产出，不是某个已经可直接上线的 LoCoMo 优化 patch，而是：**已经把可接受目标、主要风险、无效方向、环境污染源、以及下一轮应如何在干净基线上重新开始，基本厘清了。**
