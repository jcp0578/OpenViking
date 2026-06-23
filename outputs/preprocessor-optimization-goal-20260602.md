# PREPROCESSOR / Extraction 优化目标（2026-06-02 版）

## 背景

当前阶段已经出现有效的最小主候选，但 full-sample QA 验证通路不稳定，出现了恢复后 QA 路径稳定 `http_400` 的问题。
因此，目标需要从“直接追 full-sample 分数”调整为“先恢复稳定验证通路，再验证更大范围效果”。

## 总体目标

本阶段目标分两步推进：

1. 先恢复并稳定 full-sample QA 验证通路，确保远端 current `1933` + gateway + benchmark QA 路径可复现。
2. 在稳定验证通路上，继续围绕 PREPROCESSOR / extraction 侧最小候选，验证更大范围效果，目标是降低 token 开销和/或提升准确率。

## 具体目标

### 1. 优化范围

优化范围继续以 PREPROCESSOR / extraction 为主，但不再把 PREPROCESSOR 设为唯一约束。

允许修改：

- 会话信息预处理
- `memory_extraction / events` 的 extraction 级改动
- benchmark ingest / benchmark settle 的最小必要改动

原则：

- 非必要不改召回主流程
- 非必要不扩大到无关 runtime 面
- 每次改动都要说明为什么对当前实测问题是必要的

### 2. 验证原则

以远端容器实测为准，但要先区分：

- 候选效果问题
- 验证通路问题

要求：

- 算法/候选结论必须基于远端 clean 实测
- 如果 benchmark QA 通路异常（如 `http_400`、quota、index visibility），应先修复验证通路，再继续评估候选
- 不把通路异常误判成候选失效

### 3. 当前默认主候选

当前主候选默认按“最小改动集”维护，优先级明确为：

- baseline runtime
- `benchmark/locomo/openclaw/phase_a_off.py`
- `benchmark/locomo/openclaw/judge.py`
- `openviking/prompts/templates/compression/memory_extraction.yaml`
- `benchmark/locomo/openclaw/import_to_ov.py`

后续优化默认先在这个最小集合上做，不轻易混入更大 runtime 改动。

### 4. 历史信息与证据获取

优先级如下：

1. 优先从 `outputs/`、已有 CSV / meta / log / account memory 读取历史结果
2. 必要时读取远端持久化 memory / archive / `messages.jsonl`
3. 先判断是“已有数据足够回答”，还是必须新开跑
4. 避免把环境恢复问题和算法问题混在一起猜

### 5. 代码调整规则

“基于实测调整实现代码”细化为：

- 如果实测表明是候选问题，允许继续改实现
- 如果实测表明是验证链路问题，优先修验证链路
- 所有代码调整都要能对应到具体实测现象

## 当前阶段性任务

### A. 验证通路恢复

优先恢复以下链路的稳定性：

- current `1933`
- current gateway
- `phase_a_off.py` QA-only
- full-sample QA 输出 CSV / meta

目标不是先拿更高分，而是先保证：

- benchmark QA 请求可以稳定完成
- 不再从第 2 题开始稳定 `http_400`
- full-sample 结果可以完整落盘

### B. 更大范围效果验证

在验证通路稳定后，继续推进：

- full `sample0`
- clean fresh account
- 当前最小主候选

验证输出包括：

- 总正确数
- 平均 `input_tokens`
- 错题分布
- 与上一阶段最优候选的对比

## 停止条件

停止条件分两类：

### 1. 候选优化阻塞

如果 8 轮或 6 小时内没有实质候选改进：

- 停止
- 输出候选阻塞总结

### 2. 验证通路阻塞

如果连续命中同一验证通路阻塞，例如：

- QA 路径稳定 `http_400`
- gateway / `1933` 契约不兼容
- 配额或环境问题反复阻断 full-sample 验证

则：

- 停止
- 输出“验证通路阻塞总结”

不再把“候选阻塞”和“验证通路阻塞”混成一个结论。

## 需要先出 plan 的情况

以下情况视为大改，必须先出 plan：

- 修改召回主流程
- 修改服务启动/部署结构
- 扩大到新的多模态 / OCR 子系统
- 引入新的依赖安装或运行环境切换
- full-sample 评测口径或 benchmark 主脚本结构性变化

## 当前一句话目标

先恢复稳定的 full-sample QA 验证通路，再在最小主候选上验证更大范围效果。
