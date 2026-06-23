# outputs 目录跨环境汇总（本地 + 远端，2026-06-23）

## 1. 结论先行

不是“2 个月只有这些总结”。

基于当前本地工作区 `/home/jcp/Agent/code/OpenViking/outputs` 和远端服务器 `/home/jcp/agent/code/OpenViking/outputs` 的实际盘点，过去这段时间沉淀下来的产物可以分成两类：

1. **本地为主的总结/分析/交接文档**
2. **远端为主的 LoCoMo 原始运行产物与阶段性 gate 结果**

两边合起来，信息量明显大于当前少数几份 handoff 文档所体现的内容。

## 2. 文件量概览

### 本地

- 文件总数：`282`
- 其中：
  - `253` 个是 `outputs/locomo-gold-regression-v1/` 下的 LoCoMo 运行产物、诊断文件、CSV、meta、rejudge 结果
  - `11` 个是 LoCoMo 汇总/分析/基线文档
  - `8` 个是 preprocessor / 环境隔离 / handoff 文档
  - `7` 个是工作总结 / 贡献统计 / 评优材料
  - `3` 个是 agent-memory 相关 HTML 说明页

### 远端

- 文件总数：`148`
- 当前全部都在 `outputs/locomo-gold-regression-v1/` 体系下
- 没有看到本地这些高层总结文档；远端更像“运行现场”与“实验产物仓”

### 差异

- 远端独有文件：`61`
- 本地独有文件：`195`

这说明：

- **远端保留了不少原始 run 证据，但没有同步成高层总结**
- **本地汇总文档更多，但并不等于已经覆盖了远端所有原始实验产物**

## 3. 本地 outputs 反映出来的主要工作线

### 3.1 LoCoMo 主线

可见的本地 LoCoMo 高层文档包括：

- `locomo-effective-results-summary-20260608.md`
- `locomo-gold-regression-v1.md`
- `locomo-gold-regression-v1-results-20260610.md`
- `locomo-gold-regression-v1-analysis-20260610.md`
- `locomo-gold-regression-v1-followup-20260610.md`
- `locomo-remote-root-cause-20260609.md`
- `locomo-handoff-fresh-restart-20260623.md`
- `recalltrim-validation-log-20260605.md`
- `sample0-accuracy-regression-analysis-20260605.md`
- `off-small-volcengine-20260604.md`
- `token_comparison.md`

这条线本质上不是“只有一份总结”，而是至少覆盖了：

- 大盘有效结果汇总
- gold 设计与验收规则
- 完整 sample5/6/9 Gold baseline
- 失败层级拆分
- 远端 root cause
- follow-up 连续实验记录
- 最新交接与重启建议

### 3.2 预处理 / 环境隔离 / 运行机制

这类文档包括：

- `preprocessor-optimization-summary-20260530.md`
- `preprocessor-optimization-analysis.md`
- `preprocessor-optimization-goal-20260602.md`
- `preprocessor-optimization-goal-short-20260603.md`
- `preprocessor-optimization-blocker-20260531.md`
- `benchmark-settle-isolation-20260601.md`
- `handoff.md`
- `handoff-20260512.md`

这说明过去两个月并不只有“LoCoMo 准确率结果”，还做了大量：

- preprocessor 优化分析
- benchmark 隔离与运行机制整理
- 远端容器 / gateway / OV 的操作交接

### 3.3 非 LoCoMo 的工作沉淀

本地还有一组明显不属于 LoCoMo 的产物：

- `jcp0578-cross-repo-contribution-stats-20260614.md`
- `jcp0578-code-review-evaluation-20260614.md`
- `jcp0578-code-review-evaluation-20260614.xlsx`
- `jcp0578-work-summary-20260621.md`
- `jcp0578-work-summary-numbered-20260621.md`
- `jcp0578-award-summary-20260621.md`
- `openviking-merged-prs-by-author-20260608.md`

这表明这两个月输出里还有：

- 贡献统计
- 代码评审汇总
- 工作总结
- 评优材料

## 4. 远端 outputs 反映出来的主要工作线

远端 `/home/jcp/agent/code/OpenViking/outputs` 目前几乎完全是 LoCoMo 原始产物树，重点是：

- `outputs/locomo-gold-regression-v1/runs/...`
- `outputs/locomo-gold-regression-v1/sample6_*`
- `outputs/locomo-gold-regression-v1/sample5_*`
- `outputs/locomo-gold-regression-v1/sample9_*`
- `outputs/locomo-gold-regression-v1/health/...`

远端最近的活跃时间集中在：

- `2026-06-11` 到 `2026-06-15`

典型文件类型是三件套：

- `phaseA_...csv`
- `phaseA_...txt`
- `phaseA_..._meta.json`

这说明远端更像：

- 小 gate / 子集 gate / full sample gate 的运行现场
- health gate 与 smoke 的一手证据
- rejudge 前后的中间结果存放地

## 5. 远端独有内容说明了什么

远端独有的 `61` 个文件里，能看到一批本地没有同步回来的 run 结果，例如：

- `outputs/locomo-gold-regression-v1/health/...`
- `outputs/locomo-gold-regression-v1/runs/phaseA_on_19sessions_s5q6q9_*`
- `outputs/locomo-gold-regression-v1/runs/phaseA_on_19sessions_s6full_covcontract_*`
- `outputs/locomo-gold-regression-v1/runs/phaseA_on_19sessions_s5full_covcontract_*`

这说明当前本地的高层总结，**并没有完全覆盖远端原始证据**。
换句话说，若要做真正完整的两个月工作回顾，不能只看本地那几份 markdown，还要把远端这些 gate 产物纳入视野。

## 6. 从 outputs 目录看，过去两个月实际做了什么

如果只基于 `outputs` 目录，而不看代码或对话历史，可以把过去两个月的工作概括为五条主线：

1. **LoCoMo 准确率与 token 成本对比**
   - 包括 off/on、sample0-9、sample5/6/9、subset gate、full sample gate

2. **LoCoMo Gold 评测体系建设**
   - 包括 Gold 集、acceptance rules、invalid run 规则、health gate、Extraction/Retrieval/QA 分层

3. **LoCoMo 失败层级与 root cause 分析**
   - 包括 recall 注入、time anchoring、durable memory coverage、judge/provider 污染、runtime mismatch

4. **Preprocessor 与 benchmark 环境工程化**
   - 包括 preprocessor 优化、benchmark 隔离、远端 handoff、容器运行方式沉淀

5. **非 LoCoMo 的工作总结类产出**
   - 包括贡献统计、review 评价、工作总结、评优材料

## 7. 为什么会产生“只有这些总结”的感觉

主要有三个原因：

1. **高层总结和原始 run 分散在不同环境**
   - 本地偏总结
   - 远端偏运行产物

2. **LoCoMo 主线文档过于集中在少数几个大文件**
   - 尤其 `locomo-gold-regression-v1-followup-20260610.md`
   - 它吸收了大量阶段性结论，导致表面上“文件数不多”，实际上内容极长

3. **远端不少实验结果没有被回收成本地统一汇总**
   - 导致本地看上去像“只有结论”
   - 远端看上去像“只有碎片 run”

## 8. 当前最合理的整理结论

基于 outputs 目录，比较准确的判断不是“2 个月只有这些总结”，而是：

- **本地已经有一批较强的总结性文档，但它们主要覆盖了 LoCoMo 主线的高层结论**
- **远端还保留着一批未完全回收的原始实验产物，尤其是 2026-06-11 到 2026-06-15 的 gate / health / full-sample 运行结果**
- **如果要做完整工作交接，还应再补一层“远端原始 run -> 本地结构化索引”的整理**

## 9. 建议的下一步整理动作

如果目标是把这两个月工作整理成真正可交接的材料，建议下一步做这三件事：

1. 以当前这份跨环境汇总为入口，补一份 **LoCoMo runs 索引表**
   - 列出每个 run 的时间、范围、有效性、是否已写入结论文档

2. 把远端独有的 `61` 个文件再按主题分组
   - health gate
   - sample5 gate
   - sample6 gate
   - sample9 gate
   - covcontract / travelyear / fallback / after48 / shrink58 等实验线

3. 输出一份“过去两个月工作总表”
   - LoCoMo
   - preprocessor / benchmark 工程
   - 非 LoCoMo 总结类产出

这样才会把“少数总结文档”和“大量远端实验产物”真正合并成一套完整交接视图。
