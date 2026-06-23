# jcp0578 代码评价规则评分与举证

生成时间：2026-06-14
统计对象：`jcp0578`
基础数据来源：[jcp0578-cross-repo-contribution-stats-20260614.md](/home/jcp/Agent/code/OpenViking/outputs/jcp0578-cross-repo-contribution-stats-20260614.md)

## 口径说明

- 本文按当前已统计到的 GitHub / GitCode / Gitee 数据填写。原模板写的是“2025年输出”，但当前可用统计快照覆盖的是 2026-03 至 2026-06 为主的公开/已认证仓库数据；如用于 2025 年度评价，需要替换为 2025 全年数据后重算。
- “代码量”不采用提交次数，而采用作者 PR/MR 文件 diff 中的代码/配置类新增、删除、文件新增/修改/删除数据。
- 代码/配置类文件包括 `.py`、`.ts`、`.js`、`.sh`、`.yaml/.yml`、`.json`、`.toml`、`.sql`、`Dockerfile/Makefile` 等；排除 `.md/.rst/.txt`、图片、表格等文档/资产类文件。
- 检视意见按可复核数据合并统计：GitHub reviewed PR / review comment / conversation comment、GitCode `lgtm-jcp0578` 标签和 MR 评论、Gitee PR 评论。
- 对没有证据的数据项，按 0 分或保守低分处理，并在“举证”中标明“未提供直接证据”。
- 原模板权重合计为 140%（50% + 20% + 15% + 10% + 25% + 20%），本文保留原权重，同时给出归一化总分。

## 关键统计摘要

| 指标 | 数量 | 举证 |
|---|---:|---|
| 作者 PR/MR | 16 | OpenViking：[#1832](https://github.com/volcengine/OpenViking/pull/1832)、[#1606](https://github.com/volcengine/OpenViking/pull/1606)、[#1369](https://github.com/volcengine/OpenViking/pull/1369)、[#1058](https://github.com/volcengine/OpenViking/pull/1058)、[#1037](https://github.com/volcengine/OpenViking/pull/1037)、[#1000](https://github.com/volcengine/OpenViking/pull/1000)；oGMemory：[MR #34](https://gitcode.com/opengauss/oGMemory/merge_requests/34)、[MR #9](https://gitcode.com/opengauss/oGMemory/merge_requests/9)；KunpengRAG：[PR #67](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/67)、[#66](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66)、[#65](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/65)、[#64](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/64)、[#63](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/63)、[#61](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/61)、[#58](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/58)、[#57](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/57) |
| 已合入 PR/MR | 15 | 已合入证据同上；其中 oGMemory [MR #9](https://gitcode.com/opengauss/oGMemory/merge_requests/9) 为 closed 未合入，其余 15 个为 merged |
| 代码/配置类文件 | 77 | 详见统计文档中的[总表](/home/jcp/Agent/code/OpenViking/outputs/jcp0578-cross-repo-contribution-stats-20260614.md)：OpenViking 23、oGMemory 26、KunpengRAG 28；对应 PR/MR 链接见上一行 |
| 代码/配置类新增行 | 6706 | OpenViking 2996（[#1832](https://github.com/volcengine/OpenViking/pull/1832)、[#1606](https://github.com/volcengine/OpenViking/pull/1606)、[#1369](https://github.com/volcengine/OpenViking/pull/1369)、[#1058](https://github.com/volcengine/OpenViking/pull/1058)、[#1037](https://github.com/volcengine/OpenViking/pull/1037)、[#1000](https://github.com/volcengine/OpenViking/pull/1000)）；oGMemory 1854（[MR #34](https://gitcode.com/opengauss/oGMemory/merge_requests/34)）；KunpengRAG 1856（[#67](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/67)、[#66](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66)、[#65](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/65)、[#64](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/64)、[#63](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/63)、[#61](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/61)） |
| 代码/配置类删除行 | 556 | OpenViking 289、oGMemory 50、KunpengRAG 217；明细见统计文档[代码/配置类变更量](/home/jcp/Agent/code/OpenViking/outputs/jcp0578-cross-repo-contribution-stats-20260614.md)和各 PR/MR 链接 |
| 代码/配置类新增文件 | 23 | oGMemory [MR #34](https://gitcode.com/opengauss/oGMemory/merge_requests/34) 新增 13 个；KunpengRAG [PR #61](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/61)、[#65](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/65)、[#66](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66) 合计新增 10 个 |
| 代码/配置类修改文件 | 53 | OpenViking 23（[#1832](https://github.com/volcengine/OpenViking/pull/1832)、[#1606](https://github.com/volcengine/OpenViking/pull/1606)、[#1369](https://github.com/volcengine/OpenViking/pull/1369)、[#1058](https://github.com/volcengine/OpenViking/pull/1058)、[#1037](https://github.com/volcengine/OpenViking/pull/1037)、[#1000](https://github.com/volcengine/OpenViking/pull/1000)）；oGMemory [MR #34](https://gitcode.com/opengauss/oGMemory/merge_requests/34) 13；KunpengRAG 17（[#67](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/67)、[#66](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66)、[#65](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/65)、[#64](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/64)、[#63](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/63)） |
| 代码/配置类删除文件 | 1 | KunpengRAG [PR #66](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66) 删除 1 个代码/配置类文件 |
| 检视相关记录 | 约 195+ | GitCode MR 评论 143 条和 `lgtm-jcp0578` 27 个 MR，代表 MR 包括 [#48](https://gitcode.com/opengauss/oGMemory/merge_requests/48)、[#65](https://gitcode.com/opengauss/oGMemory/merge_requests/65)、[#43](https://gitcode.com/opengauss/oGMemory/merge_requests/43)、[#72](https://gitcode.com/opengauss/oGMemory/merge_requests/72)、[#85](https://gitcode.com/opengauss/oGMemory/merge_requests/85)；GitHub review/comment 涉及 [#1606](https://github.com/volcengine/OpenViking/pull/1606)、[#1378](https://github.com/volcengine/OpenViking/pull/1378)、[#957](https://github.com/volcengine/OpenViking/pull/957)；Gitee PR 评论涉及 [#68](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/68)、[#60](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/60) |
| 作者 Issue | 5 | OpenViking [#2100](https://github.com/volcengine/OpenViking/issues/2100)、[#1831](https://github.com/volcengine/OpenViking/issues/1831)、[#1479](https://github.com/volcengine/OpenViking/issues/1479)、[#1381](https://github.com/volcengine/OpenViking/issues/1381)、[#1368](https://github.com/volcengine/OpenViking/issues/1368) |

## 评分总览

| 维度 | 原模板权重 | 维度建议分 | 加权分（未归一） | 评分结论 |
|---|---:|---:|---:|---|
| 1. 软件开发 | 50% | 90 | 45.00 | 代码量和检视量均超过吃水线，复杂特性和测试补充较充分；社区实践发表类证据缺失 |
| 2. 软件设计 | 20% | 80 | 16.00 | 有多租户设计、部署设计、接口/插件方案和社区文档证据；缺少完整 UML/时序图等详细设计举证 |
| 3. 开发者测试 | 15% | 72 | 10.80 | 多个 PR 包含 UT/测试脚本；缺少覆盖率、转测缺陷率和线上质量数据 |
| 7. 开源社区影响力 | 10% | 18.75 | 1.88 | 已补充 committer 身份、技术直播、峰会活动；具体链接/证明材料待补充 |
| 4. 软件工程 | 25% | 85 | 21.25 | 有工具链、部署自动化、工程效率和质量防护建设证据 |
| 6. 专利 | 20% | 0 | 0.00 | 未提供专利评审通过材料 |
| 合计 | 140% | - | 94.93 / 140 | 原模板权重合计口径 |
| 归一化总分 | 100% | - | 67.81 / 100 | `94.93 / 1.4` |

## 大类优点与不足

| 大类 | 优点 | 不足/后续补充 |
|---|---|---|
| 软件开发 | 在 OpenViking、oGMemory、KunpengRAG 三个仓库完成了较多代码/配置类交付，累计新增 6706 行，超过活跃编码人员 2000 行参考吃水线；任务覆盖 OpenClaw 插件导入查询、auth/namespace/routing、多租认证与共享检索管理面、OpenFuyao 一键部署/持久化/绑核等复杂特性。代表证据：[OpenViking #1369](https://github.com/volcengine/OpenViking/pull/1369)、[#1606](https://github.com/volcengine/OpenViking/pull/1606)、[#1832](https://github.com/volcengine/OpenViking/pull/1832)、[oGMemory MR #34](https://gitcode.com/opengauss/oGMemory/merge_requests/34)、[KunpengRAG #61](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/61)、[#64](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/64)、[#66](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66)。同时参与了较多代码检视和质量把关，检视相关记录约 195+，覆盖结构、权限、安全、性能、逻辑和工具链等维度，代表证据：[GitCode #48](https://gitcode.com/opengauss/oGMemory/merge_requests/48)、[#65](https://gitcode.com/opengauss/oGMemory/merge_requests/65)、[#82](https://gitcode.com/opengauss/oGMemory/merge_requests/82)、[#86](https://gitcode.com/opengauss/oGMemory/merge_requests/86)、[GitHub #1606](https://github.com/volcengine/OpenViking/pull/1606)。 | 当前统计主要覆盖 2026-03 至 2026-06；如用于 2025 年度考核，需要继续补齐 2025 全年同口径代码量和检视明细。后续可补充 1-2 个代表 MR 的逐行代码自评、复杂特性性能/效率收益数据、典型检视评论原文和问题分类表，让 CleanCode、性能收益和检视质量的举证更充分。 |
| 软件设计 | 具备明确的设计类交付和“设计 -> 实现”闭环：多租户设计通过 [oGMemory MR #9](https://gitcode.com/opengauss/oGMemory/merge_requests/9) 提出，并在 [MR #34](https://gitcode.com/opengauss/oGMemory/merge_requests/34) 落地到认证、角色控制、共享检索、审计、管理面和单元测试；OpenClaw Plugin RFC 通过 [OpenViking Issue #1368](https://github.com/volcengine/OpenViking/issues/1368) 提出，并在 [OpenViking #1369](https://github.com/volcengine/OpenViking/pull/1369)、[#1606](https://github.com/volcengine/OpenViking/pull/1606)、[#1832](https://github.com/volcengine/OpenViking/pull/1832) 中落地导入查询、鉴权命名空间和路由一致性；部署设计通过 [KunpengRAG #57](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/57)、[#58](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/58) 输出，并在 [#61](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/61)、[#64](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/64)、[#66](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66) 落地为一键部署、持久化和绑核脚本。整体设计涉及服务端、检索、审计、插件、部署、宿主机资源绑定等多模块联动。 | 当前主要用 PR/Issue 作为设计交付件举证，后续可把关键设计内容整理成独立归档文档，并补充方案取舍、风险分析、接口清单、异常场景、安全考虑、架构图/时序图/模块交互图等材料，增强文档完整性和可读性。 |
| 开发者测试 | 在关键功能交付中同步补充了开发者测试和冒烟验证：OpenViking 插件改造包含 compact、tools、client、config、afterTurn 等 UT（[OpenViking #1369](https://github.com/volcengine/OpenViking/pull/1369)、[#1606](https://github.com/volcengine/OpenViking/pull/1606)、[#1832](https://github.com/volcengine/OpenViking/pull/1832)）；oGMemory 多租能力包含 auth、admin、audit、retrieval、write API 等单元测试（[oGMemory MR #34](https://gitcode.com/opengauss/oGMemory/merge_requests/34)）；KunpengRAG cpuset 绑核能力包含 smoke test（[KunpengRAG #66](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66)）。这些测试覆盖了核心路径、配置解析、工具调用和部署脚本基本可用性。 | 当前尚未整理覆盖率报告、函数覆盖率趋势、转测缺陷率、线上质量稳定性数据；后续可补充 CI 覆盖率、本地覆盖率结果、典型测试用例自评和缺陷闭环记录，使开发者测试效果更可量化。 |
| 开源社区影响力 | 当前已有一定开源仓库贡献和 Issue/PR 参与基础，例如在 OpenViking 提出和跟进插件 RFC、Bug/Feature Issue（[OpenViking #1368](https://github.com/volcengine/OpenViking/issues/1368)、[#1381](https://github.com/volcengine/OpenViking/issues/1381)、[#1479](https://github.com/volcengine/OpenViking/issues/1479)、[#1831](https://github.com/volcengine/OpenViking/issues/1831)、[#2100](https://github.com/volcengine/OpenViking/issues/2100)），并在 oGMemory、KunpengRAG 等开源平台持续参与 MR/PR 评审和协作。同时具备 committer 社区角色，并参与过技术直播和峰会活动，按当前规则可分别计入社区关键成员和社区活动得分。 | 后续需要补充 committer 身份证明、技术直播链接/议程/回放、峰会活动议程/参会记录/发言材料等证据，便于评审直接核验。Issue 解决闭环、博客/培训、考试/沙箱、外部伙伴评审等材料如有也可继续补充。 |
| 软件工程 | 在工程效率和交付自动化方面有较明确的任务沉淀：OpenViking 统一导入/查询工具提升插件使用效率（[OpenViking #1369](https://github.com/volcengine/OpenViking/pull/1369)）；KunpengRAG 建设 OpenFuyao quick start、持久化、绑核脚本和验证流程（[KunpengRAG #61](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/61)、[#64](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/64)、[#66](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66)）；同时通过检视推动 oGMemory CLI 统一、脚本清理和部署文档完善（[oGMemory #62](https://gitcode.com/opengauss/oGMemory/merge_requests/62)、[#54](https://gitcode.com/opengauss/oGMemory/merge_requests/54)、[#68](https://gitcode.com/opengauss/oGMemory/merge_requests/68)）。这些工作降低了部署和使用门槛，也改善了工程可维护性。 | 当前主要以 PR/MR 任务作为工程成效举证，缺少效率提升量化数据，如部署耗时、人工步骤减少、失败率变化、专项立项/验收报告、收益复盘或知识货架发布记录；后续可补充这些材料增强说服力。 |
| 专利 | 当前统计范围内没有发现专利相关记录，因此本项未纳入有效得分。 | 当前未整理专利材料；如有 2025/2026 年评审通过专利，可补充专利编号、贡献者排序、潜高/普通分类、评审通过材料后重新评分。 |

## 1. 软件开发 - 100 分（权重 50%）

维度建议分：90 / 100。

| 评价维度 | 评分项 | 打分 | 举证 | 不足 |
|---|---|---:|---|---|
| 【2.1】自己提交代码的质量 | 1）年度提交代码达成吃水线，活跃编码人员参考值 >=2000 行，超过吃水线记满分 | 100 | 完成 OpenClaw 插件、oGMemory 多租能力、OpenFuyao 部署脚本等代码/配置类开发，累计新增 6706 行，超过 2000 行吃水线；证据：[OpenViking #1832](https://github.com/volcengine/OpenViking/pull/1832)、[#1606](https://github.com/volcengine/OpenViking/pull/1606)、[#1369](https://github.com/volcengine/OpenViking/pull/1369)、[#1058](https://github.com/volcengine/OpenViking/pull/1058)、[#1037](https://github.com/volcengine/OpenViking/pull/1037)、[#1000](https://github.com/volcengine/OpenViking/pull/1000)、[oGMemory MR #34](https://gitcode.com/opengauss/oGMemory/merge_requests/34)、[KunpengRAG #61](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/61)、[#64](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/64)、[#66](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66) | 当前已整理 2026-03 至 2026-06 的跨仓贡献数据；如用于 2025 年度考核，可继续补齐 2025 全年同口径数据 |
| 【2.1】自己提交代码的质量 | 2）抽查 MR，从安全、可读性、可维护性等方面主观评价 | 85 | 任务包含鉴权/命名空间/角色处理（[OpenViking #1606](https://github.com/volcengine/OpenViking/pull/1606)）、多租认证与共享检索管理面（[oGMemory MR #34](https://gitcode.com/opengauss/oGMemory/merge_requests/34)）、OpenFuyao 一键部署/持久化/绑核（[KunpengRAG #61](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/61)、[#64](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/64)、[#66](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66)），覆盖安全、配置、脚本化和测试可维护性 | 后续可补充 1-2 个代表 MR 的逐行自评，说明安全性、可读性、可维护性和复杂度控制情况 |
| 【2.1】自己提交代码的质量 | 3）承担复杂特性开发，攻克技术难点，提升可复用性，避免霰弹式修改，提升性能 | 90 | 承担多模块复杂特性：统一导入/查询工具（[OpenViking #1369](https://github.com/volcengine/OpenViking/pull/1369)）、统一 session/routing（[#1832](https://github.com/volcengine/OpenViking/pull/1832)）、多租基础能力（[oGMemory MR #34](https://gitcode.com/opengauss/oGMemory/merge_requests/34)）、OpenFuyao 部署自动化与绑核（[KunpengRAG #61](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/61)、[#66](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66)） | 后续可补充性能数据、复杂度变化或部署效率前后对比，让复杂特性收益更可量化 |
| 【2.2】帮助团队提升代码质量 | 1）年度代码检视次数（含提出 ISSUE 数量）达成 60 次，不足按比例记分 | 100 | 参与 MR 检视和质量把关，检视相关记录约 195+；代表 MR：[GitCode #48](https://gitcode.com/opengauss/oGMemory/merge_requests/48)、[#65](https://gitcode.com/opengauss/oGMemory/merge_requests/65)、[#43](https://gitcode.com/opengauss/oGMemory/merge_requests/43)、[#72](https://gitcode.com/opengauss/oGMemory/merge_requests/72)、[#85](https://gitcode.com/opengauss/oGMemory/merge_requests/85)；GitHub review 涉及 [#1606](https://github.com/volcengine/OpenViking/pull/1606)、[#1378](https://github.com/volcengine/OpenViking/pull/1378)、[#957](https://github.com/volcengine/OpenViking/pull/957)；Issue：[OpenViking #2100](https://github.com/volcengine/OpenViking/issues/2100)、[#1831](https://github.com/volcengine/OpenViking/issues/1831)、[#1479](https://github.com/volcengine/OpenViking/issues/1479)、[#1381](https://github.com/volcengine/OpenViking/issues/1381)、[#1368](https://github.com/volcengine/OpenViking/issues/1368) | 当前按各平台可复核字段合并统计；如评审要求单一口径，可进一步导出评论明细并按同一规则去重 |
| 【2.2】帮助团队提升代码质量 | 1）检视意见总数平均每月 5 条以上 | 100 | 在 GitCode oGMemory 集中参与 MR 评审，评论 143 条；代表任务：[MR #48](https://gitcode.com/opengauss/oGMemory/merge_requests/48) React extraction、[#65](https://gitcode.com/opengauss/oGMemory/merge_requests/65) extraction 去重、[#43](https://gitcode.com/opengauss/oGMemory/merge_requests/43) CI/CD 方案、[#72](https://gitcode.com/opengauss/oGMemory/merge_requests/72) 性能记录模块、[#85](https://gitcode.com/opengauss/oGMemory/merge_requests/85) SQLRelationStore fallback | 当前按已完成的 2026-03 至 2026-06 快照折算月均；如用于年度材料，可补齐全年逐月检视统计 |
| 【2.2】帮助团队提升代码质量 | 2）检视意见明确指出问题点、修改原因，并给出修改样例代码 | 75 | 评论分布覆盖多个实质任务：[GitCode #48](https://gitcode.com/opengauss/oGMemory/merge_requests/48)、[#65](https://gitcode.com/opengauss/oGMemory/merge_requests/65)、[#43](https://gitcode.com/opengauss/oGMemory/merge_requests/43)、[#72](https://gitcode.com/opengauss/oGMemory/merge_requests/72)、[#85](https://gitcode.com/opengauss/oGMemory/merge_requests/85)、Gitee [#68](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/68)、[#60](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/60)，说明参与了代码、CI、性能、部署等问题检视 | 后续可导出典型评论原文，挑选包含问题点、修改原因和示例建议的评论作为强举证 |
| 【2.2】帮助团队提升代码质量 | 3）检视维度全面，如代码结构、安全、性能、逻辑功能等 | 85 | 检视任务覆盖结构、权限、性能、逻辑和工具链：[MR #48](https://gitcode.com/opengauss/oGMemory/merge_requests/48) ReAct loop、[#88](https://gitcode.com/opengauss/oGMemory/merge_requests/88) tool 调用整合、[#86](https://gitcode.com/opengauss/oGMemory/merge_requests/86) owner_space 访问控制、[#82](https://gitcode.com/opengauss/oGMemory/merge_requests/82) BM25 hybrid retrieval、[#63](https://gitcode.com/opengauss/oGMemory/merge_requests/63) archive/session、[#62](https://gitcode.com/opengauss/oGMemory/merge_requests/62) CLI 统一 | 后续可把典型检视意见按结构、安全、性能、逻辑、工具链等维度归类，形成更完整的检视案例表 |
| 【2.2】帮助团队提升代码质量 | 4）总结优秀编码实践、负向编码案例、负向安全案例并在鲲鹏软件能力提升社区发表 | 0 | 当前没有可用的社区发表任务链接 | 当前尚未整理社区发表材料；如有文章或社区帖子，可补充链接作为该项举证 |
| 【2.3】消除历史债务 | 1）主动重构腐化代码，通过良好设计减少代码元素和坏味道 | 80 | 推动/参与 legacy 与脚本债务治理：OpenViking compaction/routing 收敛（[#1832](https://github.com/volcengine/OpenViking/pull/1832)、[#1037](https://github.com/volcengine/OpenViking/pull/1037)），GitCode legacy bridge 清理和 CLI/脚本统一（[#67](https://gitcode.com/opengauss/oGMemory/merge_requests/67)、[#62](https://gitcode.com/opengauss/oGMemory/merge_requests/62)、[#54](https://gitcode.com/opengauss/oGMemory/merge_requests/54)） | 后续可区分“直接提交”和“检视推动”的债务治理案例，并补充重构前后对比说明 |
| 【2.3】消除历史债务 | 2）代码分支收编，落地 OneTrack | 0 | 当前没有分支收编或 OneTrack 落地任务链接 | 当前没有可提交的 OneTrack/分支收编材料；如有内部记录，可补充为该项举证 |

## 2. 软件设计 - 100 分（权重 20%）

维度建议分：80 / 100。

| 评价维度 | 评分项 | 打分 | 举证 | 不足 |
|---|---|---:|---|---|
| 【1.1】设计质量 | 1）有设计交付件 | 85 | 输出多租户、插件导入查询、部署和镜像制作类设计交付件：多租户设计（[oGMemory MR #9](https://gitcode.com/opengauss/oGMemory/merge_requests/9)）、OpenViking + openGauss 部署说明（[KunpengRAG #57](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/57)）、镜像制作说明（[#58](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/58)）、OpenClaw Plugin RFC（[OpenViking Issue #1368](https://github.com/volcengine/OpenViking/issues/1368)） | 当前以 PR/Issue 作为设计交付件举证；后续可把关键设计内容整理成独立归档文档，方便评审查阅 |
| 【1.1】设计质量 | 2）设计文档能够有效指导编码；评估报告能支撑技术决策 | 80 | 设计落地为代码和脚本：多租户设计后续落地认证、角色控制和共享检索管理面（[oGMemory MR #34](https://gitcode.com/opengauss/oGMemory/merge_requests/34)）；部署设计落地 quick start、持久化和绑核脚本（[KunpengRAG #61](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/61)、[#64](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/64)、[#66](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66)） | 后续可补充方案取舍、风险分析和技术决策记录，增强设计决策举证 |
| 【1.1】设计质量 | 3）逻辑自恰 | 80 | 形成“设计 -> 实现”的闭环：多租户设计（[MR #9](https://gitcode.com/opengauss/oGMemory/merge_requests/9)）到多租实现（[MR #34](https://gitcode.com/opengauss/oGMemory/merge_requests/34)）；插件 RFC（[Issue #1368](https://github.com/volcengine/OpenViking/issues/1368)）到 `ov_import`/`ov_search` 与 auth/routing 改造（[OpenViking #1369](https://github.com/volcengine/OpenViking/pull/1369)、[#1606](https://github.com/volcengine/OpenViking/pull/1606)、[#1832](https://github.com/volcengine/OpenViking/pull/1832)）；部署文档（[KunpengRAG #57](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/57)）到脚本（[#61](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/61)、[#64](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/64)） | 后续可补充设计到实现的闭环说明，逐项对应需求、设计、代码和测试 |
| 【1.2】文档质量 | 1）要素完整，包含接口定义、模块交互、异常场景、安全等 | 75 | 设计任务覆盖安全、管理面、服务接口和部署：多租管理面代码涉及 `auth`、`tenant_admin`、`control_plane_store`、`memory_service`（[oGMemory MR #34](https://gitcode.com/opengauss/oGMemory/merge_requests/34)）；部署文档覆盖 OpenViking + openGauss 和镜像制作（[KunpengRAG #57](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/57)、[#58](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/58)） | 后续可补充接口清单、异常场景和安全考虑说明，完善详细设计要素 |
| 【1.2】文档质量 | 2）通过 UML、思维导图等设计元素与文字结合，包含关键上下文 | 45 | 当前能证明存在设计/部署类交付件（[MR #9](https://gitcode.com/opengauss/oGMemory/merge_requests/9)、[KunpengRAG #57](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/57)、[#58](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/58)） | 后续可为关键模块补充架构图、时序图或模块交互图，提升设计文档可读性 |
| 【1.3】设计难度 | 1）复杂模块设计、多模块联动设计、或子系统以上设计 | 90 | 设计和实现跨多个模块/系统：oGMemory 多租认证、RBAC、共享检索、审计、测试（[MR #34](https://gitcode.com/opengauss/oGMemory/merge_requests/34)）；OpenViking 插件跨 context engine、client、config、tool、tests（[#1369](https://github.com/volcengine/OpenViking/pull/1369)、[#1606](https://github.com/volcengine/OpenViking/pull/1606)、[#1832](https://github.com/volcengine/OpenViking/pull/1832)）；KunpengRAG 跨 OpenFuyao/OpenClaw/OpenViking/openGauss 和宿主机资源绑定（[#61](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/61)、[#64](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/64)、[#66](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66)） | 后续可补充架构评审纪要或复杂度说明，明确系统边界、风险和模块协作关系 |

## 3. 开发者测试 - 100 分（权重 15%）

维度建议分：72 / 100。

| 评价维度 | 评分项 | 打分 | 举证 | 不足 |
|---|---|---:|---|---|
| 【3.1】开发者测试效果 | 1）新增代码语句覆盖率达成团队目标，参考 60% | 50 | 在功能开发中补充单元/冒烟测试：OpenViking 插件 UT（[#1606](https://github.com/volcengine/OpenViking/pull/1606)、[#1369](https://github.com/volcengine/OpenViking/pull/1369)、[#1832](https://github.com/volcengine/OpenViking/pull/1832)）、oGMemory 多租 auth/admin/audit/retrieval/write API 单测（[MR #34](https://gitcode.com/opengauss/oGMemory/merge_requests/34)）、KunpengRAG cpuset smoke test（[#66](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66)） | 后续可补充 CI 覆盖率报告或本地覆盖率结果，证明新增代码覆盖率达标情况 |
| 【3.1】开发者测试效果 | 2）转测试后代码缺陷率持续改进或低于 1.5 个/K，上网后质量稳定 | 0 | 当前没有可链接的转测缺陷率或线上质量任务材料 | 当前没有整理转测/线上缺陷率数据；后续可补充版本质量报表或缺陷闭环记录 |
| 【3.2】测试代码质量 | 1）抽查 MR 或用例代码，基于坏味道扣分；鼓励表驱动等方法减少重复 | 80 | 测试任务覆盖 compact、tools、client、config、afterTurn、多租 auth/admin/audit/retrieval/write API 等场景；证据：[OpenViking #1832](https://github.com/volcengine/OpenViking/pull/1832)、[#1606](https://github.com/volcengine/OpenViking/pull/1606)、[oGMemory MR #34](https://gitcode.com/opengauss/oGMemory/merge_requests/34) | 后续可补充典型测试用例自评，说明用例组织、重复控制和可维护性 |
| 【3.3】开发者测试持续改进 | 1）负责测试防护网建设，通过用例专项整改提升测试有效性，存量代码函数覆盖率达成目标 | 78 | 为关键模块补充测试防护网：oGMemory 多租单测（[MR #34](https://gitcode.com/opengauss/oGMemory/merge_requests/34)）、OpenViking 插件 UT（[#1369](https://github.com/volcengine/OpenViking/pull/1369)、[#1606](https://github.com/volcengine/OpenViking/pull/1606)、[#1832](https://github.com/volcengine/OpenViking/pull/1832)）、KunpengRAG cpuset smoke test（[#66](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66)） | 后续可补充测试防护网建设前后对比、覆盖率趋势和专项整改闭环 |

## 7. 开源社区影响力 - 100 分（权重 10%）

维度建议分：18.75 / 100。

| 评价维度 | 评分项 | 打分 | 举证 | 不足 |
|---|---|---:|---|---|
| 【7.1】在 openGauss 社区发布博客、做社区公开培训 | 每篇博客/每次培训记 6.25 分 | 0 | 当前统计中没有博客发布或公开培训任务链接 | 当前未整理博客或公开培训材料；如有发布记录，可补充链接计入社区影响力 |
| 【7.2】参与 openGauss 社区活动 | 技术直播、技术沙龙、峰会、meetup、布道活动等，每次 6.25 分 | 12.5 | 已参与技术直播和峰会活动，按 2 次社区活动计分，每次 6.25 分 | 后续需要补充技术直播链接/议程/回放、峰会活动议程/参会记录/发言材料等可核验证据 |
| 【7.3】参与社区相关考试出题、设计沙箱实验等 | 每参与一次记 6.25 分 | 0 | 当前统计中没有考试出题或沙箱实验设计任务链接 | 当前未整理考试出题或沙箱实验材料；如有内部/社区记录，可补充为举证 |
| 【7.4】参与外部伙伴的方案设计讨论和评审 | 提供会议纪要，每参与一次记 6.25 分 | 0 | 当前统计中没有外部伙伴方案评审任务链接 | 当前未整理外部伙伴评审材料；如有会议纪要或评审意见，可补充为举证 |
| 【7.5】社区担任关键成员 | TC 委员/maintainer 12.5 分，committer 6.25 分 | 6.25 | 具备 committer 社区角色，按 committer 6.25 分计入 | 后续需要补充社区角色页面、任命公告、SIG 贡献记录或 committer 身份证明 |
| 【7.3】社区 issue 解决 | 超过 40 个记满分，半年按 20 个算，不足按比例计分 | 0 | 当前只统计到作者提出 Issue：[OpenViking #2100](https://github.com/volcengine/OpenViking/issues/2100)、[#1831](https://github.com/volcengine/OpenViking/issues/1831)、[#1479](https://github.com/volcengine/OpenViking/issues/1479)、[#1381](https://github.com/volcengine/OpenViking/issues/1381)、[#1368](https://github.com/volcengine/OpenViking/issues/1368) | 当前只整理了提出的 Issue；后续可继续统计由 jcp0578 修复/关闭的 issue 与 PR/MR 闭环 |

## 4. 软件工程 - 100 分（权重 25%）

维度建议分：85 / 100。

| 评价维度 | 评分项 | 打分 | 举证 | 不足 |
|---|---|---:|---|---|
| 【4.1】开发或引入工具、测试框架、工程方法等提升效率 | 1）引入新工具或新方法提升软件开发效率 | 88 | 建设统一工具和自动化脚本：OpenViking 统一导入/查询工具（[#1369](https://github.com/volcengine/OpenViking/pull/1369)）、OpenFuyao quick start（[KunpengRAG #61](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/61)）、cpuset 绑定脚本与 smoke test（[#66](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66)）；检视推动 CLI 统一和脚本清理（[oGMemory #62](https://gitcode.com/opengauss/oGMemory/merge_requests/62)、[#54](https://gitcode.com/opengauss/oGMemory/merge_requests/54)、[#68](https://gitcode.com/opengauss/oGMemory/merge_requests/68)） | 后续可补充效率收益量化数据，例如部署耗时、人工步骤减少和失败率变化 |
| 【4.1】开发或引入工具、测试框架、工程方法等提升效率 | 2）承担软件工程能力提升专项并达成目标 | 80 | 围绕部署自动化、持久化、绑核和 OpenClaw/OpenViking 集成开展工程化任务：[KunpengRAG #61](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/61)、[#64](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/64)、[#66](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66)、[OpenViking #1369](https://github.com/volcengine/OpenViking/pull/1369)、[#1606](https://github.com/volcengine/OpenViking/pull/1606) | 后续可补充专项目标、达成情况、验收结论或收益复盘材料 |
| 【4.2】围绕软件主战场输出专业能力建设实战案例 | 3）承担专业能力建设专项工作，输出实战案例固化到知识货架 | 70 | 形成 OpenViking + openGauss / OpenFuyao 部署、镜像制作和验证实践材料：[KunpengRAG #57](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/57)、[#58](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/58)、[#61](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/61)、[#64](https://gitee.com/kunpeng_compute/KunpengRAG/pulls/64) | 后续可补充知识货架、培训复用或案例归档链接，证明材料已被复用沉淀 |

## 6. 专利 - 100 分（权重 20%）

维度建议分：0 / 100。

| 评价维度 | 评分项 | 打分 | 举证 | 不足 |
|---|---|---:|---|---|
| 【6.1】专利价值，潜高专利和其他专利 | 高潜专利第一贡献者满分，其他贡献者 60 分；普通专利第一贡献者 50 分，其他贡献者 30 分；多篇可叠加 | 0 | 当前统计中没有专利相关任务链接 | 当前未整理专利材料；如有评审通过专利，可补充专利编号、贡献者排序和价值等级 |

## 可补充材料清单

为提高分数可信度，建议补充以下证据：

| 补充材料 | 影响的评分项 |
|---|---|
| 逐条检视意见原文截图或导出，尤其是包含问题点、修改原因、样例代码的评论 | 软件开发 【2.2】第 2 项 |
| 代码覆盖率报告、CI 记录、转测缺陷率、线上缺陷率 | 开发者测试 【3.1】、【3.3】 |
| 设计文档全文、UML/时序图/模块交互图/API 文档截图 | 软件设计 【1.1】、【1.2】 |
| 社区博客、培训、活动、meetup、外部伙伴评审会议纪要 | 开源社区影响力 【7.1】至【7.4】 |
| TC/maintainer/committer 身份证明和 SIG 实际贡献记录 | 开源社区影响力 【7.5】 |
| issue 解决记录，即 issue -> PR/MR 修复闭环 | 开源社区影响力 issue 解决项 |
| 专利评审通过材料、贡献者排序、潜高/普通分类 | 专利 【6.1】 |
