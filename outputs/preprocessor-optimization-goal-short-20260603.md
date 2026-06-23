# /goal 短版

背景

full-sample QA 验证通路已经恢复并验证完成。当前远端 clean fresh account + 最小主候选 full-sample 基线为：

- `96 / 150 correct`
- `accuracy = 64.00%`
- `avg_input_tokens = 950.6`
- `avg_input_plus_cacheRead = 6538.0`

当前目标

在已恢复的稳定验证链路上，围绕当前最小主候选继续优化效果，目标是：

1. 提升 clean full-sample 准确率，优先超过 `96/150`
2. 在不明显伤害准确率的前提下，尽量降低 token 开销

当前测试设计主 LLM 固定配置

- `OPENAI_BASE_URL=https://api.minimaxi.com/v1`
- `APIKEY=sk-cp-ibbtAu6TLyFdvtgNwuMeSMV8aIYNew3I7Q9vLuQv-pD1sjEoeV_0cJbLmHdpVN6t2HNgIwmO6Ckm2C0koL0Vz_hXJM3FJhuz2vQZjentoNaJmQa4D0JMNjA`
- `model="MiniMax-M3"`

适用范围

- `openclaw`
- `openviking` 主 VLM 配置
- `judge`

优化范围

允许修改仅限于：

- 会话信息预处理
- `memory_extraction / events` 的 extraction 级改动
- benchmark ingest / benchmark settle 的最小必要改动

原则：

- 非必要不改召回主流程
- 非必要不扩大到无关 runtime / 部署 / 启动结构
- 每次改动都必须对应具体实测问题

当前默认比较基线

固定对比基线为 run `20260602_1102nsfix`：

- `accuracy = 96 / 150 = 64.00%`
- `avg_input_tokens = 950.6`
- `avg_input_plus_cacheRead = 6538.0`

当前主要问题

当前问题已经不是验证链路问题，而是候选效果问题，主要表现为：

- 事件簇选择错误
- 相邻事实混入答案
- 集合题漏项
- 抽象题 / 象征意义题 / 弱线索题退化成“无信息”
- 时间归一化或答案收束不稳

验证原则

- 以远端 clean 实测为准
- 先做已有结果差异分析，再决定是否新开跑
- 优先 small diff / 代表性样本验证，再决定 full-sample
- 如果验证链路再次异常，立即切回链路修复优先

阶段性任务

1. 先按错题模式整理当前 `96/150` 的失败类型
2. 在最小主候选集合上做小步修改，每次只打一类问题
3. 代表性样本确认改善后，再重跑 clean full-sample
4. 对比：
   - 总正确数
   - 准确率
   - `avg_input_tokens`
   - `avg_input_plus_cacheRead`
   - 错题类型变化

停止条件

1. 候选优化阻塞
   如果 `8` 轮或 `6` 小时内无法超过当前基线 `96/150`，则停止并输出候选阻塞总结。

2. 验证通路阻塞
   如果再次连续出现同类链路阻塞（QA 报错、judge 无法回填、gateway/1933 契约失配等），则停止候选优化并输出验证通路阻塞总结。

一句话目标

在已恢复的稳定 full-sample 验证链路上，围绕当前最小主候选继续优化，目标是超过 `96/150`，并尽量降低 token 开销。
