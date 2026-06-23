# jcp0578 跨仓贡献统计

生成时间：2026-06-14
统计对象：`jcp0578`

## 统计口径

- GitHub `volcengine/OpenViking`：使用 `gh search`、GitHub REST API / GraphQL 可公开/已认证访问数据。
- GitCode `opengauss/oGMemory`：使用 `https://gitcode.com/api/v5/repos/opengauss/oGMemory/...` API；评论明细使用访问令牌复核。
- Gitee `kunpeng_compute/KunpengRAG`：使用 `https://gitee.com/api/v5/repos/kunpeng_compute/KunpengRAG/...` API；评论明细使用访问令牌复核。
- `PR 数量`：按作者为 `jcp0578` 的 PR/MR 计数，并单独标注已合入数量。
- `代码/配置类变更量`：按作者 PR/MR 的文件 diff 统计，代码/配置类包括 `.py`、`.ts`、`.js`、`.sh`、`.yaml/.yml`、`.json`、`.toml`、`.sql`、`Dockerfile/Makefile` 等；排除 `.md/.rst/.txt`、图片、表格等文档/资产类文件。
- `新增/修改文件`：按 diff 文件状态统计代码/配置类文件。平台未显式返回状态时，根据 patch 中 `/dev/null` 推断新增，否则按修改计。
- `新增/删除行`：使用平台文件 diff 返回的 additions/deletions。注意 diff API 不直接提供“修改行”指标，替换通常体现为删除行 + 新增行。
- `提交次数`：保留为补充指标，不再作为代码量口径；GitCode 另补充“jcp0578 发起 MR 内的 commit 数”。
- `检视意见数量`：不同平台数据模型不一致，分平台列出可复核指标：GitHub 为 reviewed PR、review comment、issue/PR conversation comment；GitCode 为 `lgtm-jcp0578` 标签数量和 MR 评论数；Gitee 为 PR 评论数。
- `Issue 数量`：按 issue 作者为 `jcp0578` 计数。

## 总表

| 仓库 | 作者 PR/MR | 已合入 PR/MR | 代码/配置类文件 | 新增行 | 删除行 | 新增文件 | 修改文件 | 删除文件 | 提交次数补充 | 检视意见/检视记录 | 作者 Issue |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---:|
| `volcengine/OpenViking` | 6 | 6 | 23 | 2996 | 289 | 0 | 23 | 0 | 5 | reviewed PR 3；review comments 4；conversation comments 17 | 5 |
| `opengauss/oGMemory` | 2 | 1 | 26 | 1854 | 50 | 13 | 13 | 0 | 0（仓库 commits author 口径）；11（作者 MR 内 commits 补充口径） | `lgtm-jcp0578` 27 个 MR；作者评论 143 条 | 0 |
| `kunpeng_compute/KunpengRAG` | 8 | 8 | 28 | 1856 | 217 | 10 | 17 | 1 | 35 | 作者 PR 评论 4 条 | 0 |

## volcengine/OpenViking

数据源：

- PR：`gh search prs --repo volcengine/OpenViking --author jcp0578 --merged --limit 200`
- Commit：`gh search commits --repo volcengine/OpenViking --author jcp0578 --limit 500`
- Issue：`gh search issues --repo volcengine/OpenViking --author jcp0578 --limit 200`
- Review：`gh search prs --repo volcengine/OpenViking --reviewed-by jcp0578 --limit 200`
- Review comment：`gh api /repos/volcengine/OpenViking/pulls/comments`
- Conversation comment：`gh api /repos/volcengine/OpenViking/issues/comments`

### 作者 PR

| PR | 标题 | 链接 |
|---:|---|---|
| 1832 | fix(openclaw-plugin): unify session and agent-prefix routing across compact, commit, and tools | https://github.com/volcengine/OpenViking/pull/1832 |
| 1606 | feat(openclaw-plugin): align auth, namespace, and role id handling | https://github.com/volcengine/OpenViking/pull/1606 |
| 1369 | feat(openclaw-plugin): add unified ov_import and ov_search | https://github.com/volcengine/OpenViking/pull/1369 |
| 1058 | fix(openclaw-plugin): fix compact result mapping | https://github.com/volcengine/OpenViking/pull/1058 |
| 1037 | fix(openclaw-plugin): Use OpenViking-owned compaction instead of legacy delegation | https://github.com/volcengine/OpenViking/pull/1037 |
| 1000 | fix(openclaw-plugin): use plugin-sdk exports for compaction delegation (fixes #833) openclaw >= v2026.3.22 | https://github.com/volcengine/OpenViking/pull/1000 |

### 作者提交

| Commit | 标题 | 链接 |
|---|---|---|
| `f5dd276` | fix(openclaw-plugin): use plugin-sdk exports for compaction delegation (fixes #833) openclaw >= v2026.3.22 (#1000) | https://github.com/volcengine/OpenViking/commit/f5dd2767140602141f6f2ce22fcdd7c86f8f5859 |
| `21bb8e9` | fix(openclaw-plugin): fix compact result mapping (#1058) | https://github.com/volcengine/OpenViking/commit/21bb8e9bcbca4cd86b867aa3d1a86348178d2d3a |
| `23f0775` | feat(openclaw-plugin): add unified ov_import and ov_search (#1369) | https://github.com/volcengine/OpenViking/commit/23f0775ab5b9e9184cda290bb49b9a0ffb4f5fae |
| `ef2513c` | feat(openclaw-plugin): align auth, namespace, and role id handling (#1606) | https://github.com/volcengine/OpenViking/commit/ef2513c3e73be47c640c15c4dcd2b0a71a0c5f5d |
| `17cfaac` | fix(openclaw-plugin): unify session and agent-prefix routing across compact, commit, and tools (#1832) | https://github.com/volcengine/OpenViking/commit/17cfaac87cbbb179cbd3b506391abc1d66632de6 |

### 代码/配置类变更量

合计：代码/配置类文件 23 个，新增 2996 行，删除 289 行；新增文件 0 个，修改文件 23 个，删除文件 0 个。另有非代码/文档类文件 7 个。

| PR | 代码/配置类文件 | 新增行 | 删除行 | 新增文件 | 修改文件 | 删除文件 | 非代码文件 | 链接 |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1832 | 4 | 278 | 74 | 0 | 4 | 0 | 0 | https://github.com/volcengine/OpenViking/pull/1832 |
| 1606 | 10 | 959 | 117 | 0 | 10 | 0 | 4 | https://github.com/volcengine/OpenViking/pull/1606 |
| 1369 | 6 | 1410 | 16 | 0 | 6 | 0 | 3 | https://github.com/volcengine/OpenViking/pull/1369 |
| 1058 | 1 | 126 | 10 | 0 | 1 | 0 | 0 | https://github.com/volcengine/OpenViking/pull/1058 |
| 1037 | 1 | 114 | 49 | 0 | 1 | 0 | 0 | https://github.com/volcengine/OpenViking/pull/1037 |
| 1000 | 1 | 109 | 23 | 0 | 1 | 0 | 0 | https://github.com/volcengine/OpenViking/pull/1000 |

### 作者 Issue

| Issue | 标题 | 状态 | 链接 |
|---:|---|---|---|
| 2100 | [Bug]:官网机器人,提示记忆保存错误 | closed | https://github.com/volcengine/OpenViking/issues/2100 |
| 1831 | [Bug]: OpenClaw 插件 compact 路径与 memory session/routing 状态不一致 | closed | https://github.com/volcengine/OpenViking/issues/1831 |
| 1479 | [Feature]: 提取openclaw的sender_id，增加userMode字符配置，以支持群聊场景 | closed | https://github.com/volcengine/OpenViking/issues/1479 |
| 1381 | [Bug]: `vectordb/context` 脏状态下，resource 已成功导入和索引，但 `search/find` 返回空结果 | open | https://github.com/volcengine/OpenViking/issues/1381 |
| 1368 | # [RFC] OpenClaw Plugin 支持通过 OpenViking 导入/查询 Resource 与 Skill | closed | https://github.com/volcengine/OpenViking/issues/1368 |

### 检视记录

| 指标 | 数量 | 明细 |
|---|---:|---|
| reviewed PR | 3 | #1606, #1378, #957 |
| review comments | 4 | #957 1 条；#1606 3 条 |
| issue/PR conversation comments | 17 | 分布在 #1000、#1014、#1351、#1381、#1369、#1378、#1388、#1606、#1617、#1832、#2131 |

## opengauss/oGMemory

数据源：

- MR：`https://gitcode.com/api/v5/repos/opengauss/oGMemory/pulls?per_page=100&page=N`
- Issue：`https://gitcode.com/api/v5/repos/opengauss/oGMemory/issues?per_page=100&page=N`
- Commit：`https://gitcode.com/api/v5/repos/opengauss/oGMemory/commits?per_page=100&page=N`
- 检视：MR labels 中的 `lgtm-jcp0578`
- 评论：`https://gitcode.com/api/v5/repos/opengauss/oGMemory/pulls/{number}/comments`

### 作者 MR

| MR | 标题 | 状态 | 合入时间 | MR 内提交数 | 链接 |
|---:|---|---|---|---:|---|
| 34 | 多租基础能力-认证、角色控制、共享检索与管理面 | merged | 2026-04-13 10:50:23 +08:00 | 10 | https://gitcode.com/opengauss/oGMemory/merge_requests/34 |
| 9 | 多租户设计 | closed | - | 1 | https://gitcode.com/opengauss/oGMemory/merge_requests/9 |

### 代码/配置类变更量

合计：代码/配置类文件 26 个，新增 1854 行，删除 50 行；新增文件 13 个，修改文件 13 个，删除文件 0 个。另有非代码/文档类文件 2 个。

| MR | 代码/配置类文件 | 新增行 | 删除行 | 新增文件 | 修改文件 | 删除文件 | 非代码文件 | 链接 |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 34 | 26 | 1854 | 50 | 13 | 13 | 0 | 1 | https://gitcode.com/opengauss/oGMemory/merge_requests/34 |
| 9 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | https://gitcode.com/opengauss/oGMemory/merge_requests/9 |

### 检视记录

`lgtm-jcp0578` 标签出现在 27 个 MR 上：

| MR | 标题 | 链接 |
|---:|---|---|
| 89 | React Loop泛化 | https://gitcode.com/opengauss/oGMemory/merge_requests/89 |
| 88 | tool调用整合，token统计优化 | https://gitcode.com/opengauss/oGMemory/merge_requests/88 |
| 86 | feat(fs): unify owner_space access control with visible_owner_spaces support | https://gitcode.com/opengauss/oGMemory/merge_requests/86 |
| 85 | fix(providers): SQLRelationStore fallback to context_nodes.relations | https://gitcode.com/opengauss/oGMemory/merge_requests/85 |
| 84 | 实现 Provenance ID 溯源 | https://gitcode.com/opengauss/oGMemory/merge_requests/84 |
| 83 | fix(cli): ogmem onboard参数传递错误修复 & service port配置统一 | https://gitcode.com/opengauss/oGMemory/merge_requests/83 |
| 82 | feat: BM25 hybrid retrieval, L0 structured summary, session improvements, and cleanup | https://gitcode.com/opengauss/oGMemory/merge_requests/82 |
| 81 | 统一双套 Tool 定义系统 | https://gitcode.com/opengauss/oGMemory/merge_requests/81 |
| 79 | Fix OpenClaw takeover compaction boundary | https://gitcode.com/opengauss/oGMemory/merge_requests/79 |
| 78 | fix(session): use archive store for latest archive fusion context | https://gitcode.com/opengauss/oGMemory/merge_requests/78 |
| 75 | DBFirst实现：后端存储新增sql直连选项 | https://gitcode.com/opengauss/oGMemory/merge_requests/75 |
| 72 | feat(module) Performance Recording Module - Local & Docker Support (Re-uploaded) | https://gitcode.com/opengauss/oGMemory/merge_requests/72 |
| 68 | add deploy/ogmem_opengauss_deploy.md | https://gitcode.com/opengauss/oGMemory/merge_requests/68 |
| 67 | refactor(plugin): remove legacy bridge and dead code | https://gitcode.com/opengauss/oGMemory/merge_requests/67 |
| 66 | fix(session): archive after turn snapshots safely | https://gitcode.com/opengauss/oGMemory/merge_requests/66 |
| 65 | perf(extraction): dedupe phase two prompts | https://gitcode.com/opengauss/oGMemory/merge_requests/65 |
| 64 | fix(llm): retry empty required tool calls | https://gitcode.com/opengauss/oGMemory/merge_requests/64 |
| 63 | feat: archive fusion, structured overview, compose trim, rolling compress gate | https://gitcode.com/opengauss/oGMemory/merge_requests/63 |
| 62 | feat(cli): unified ogmem CLI to replace scattered shell scripts (closes #35) | https://gitcode.com/opengauss/oGMemory/merge_requests/62 |
| 60 | feat(extraction): add speaker attribution for profile disambiguation (closes #32) | https://gitcode.com/opengauss/oGMemory/merge_requests/60 |
| 59 | feat(retrieval): improve working set search with dynamic score gap truncation (closes #31) | https://gitcode.com/opengauss/oGMemory/merge_requests/59 |
| 58 | feat(e2e): add LoCoMo benchmark comparison report generator (closes #33) | https://gitcode.com/opengauss/oGMemory/merge_requests/58 |
| 54 | fix(scripts): cleanup outdated scripts and fix one-click startup | https://gitcode.com/opengauss/oGMemory/merge_requests/54 |
| 53 | feat(claude-plugin): add post_tool_use hook and http header for claude code plugin | https://gitcode.com/opengauss/oGMemory/merge_requests/53 |
| 51 | fix(plugin): remove stale scripts and add skills field to plugin.json | https://gitcode.com/opengauss/oGMemory/merge_requests/51 |
| 49 | feat: openclaw压缩功能接管 | https://gitcode.com/opengauss/oGMemory/merge_requests/49 |
| 43 | 增加CICD测试方案设计 | https://gitcode.com/opengauss/oGMemory/merge_requests/43 |

作者为 `jcp0578` 的 MR 评论共 143 条，分布在 50 个 MR 上。评论数最多的 MR：

| MR | 评论数 | 标题 | 链接 |
|---:|---:|---|---|
| 48 | 10 | feat: ReAct extraction loop + schema-driven policy routing | https://gitcode.com/opengauss/oGMemory/merge_requests/48 |
| 65 | 7 | perf(extraction): dedupe phase two prompts | https://gitcode.com/opengauss/oGMemory/merge_requests/65 |
| 43 | 6 | 增加CICD测试方案设计 | https://gitcode.com/opengauss/oGMemory/merge_requests/43 |
| 72 | 5 | feat(module) Performance Recording Module - Local & Docker Support (Re-uploaded) | https://gitcode.com/opengauss/oGMemory/merge_requests/72 |
| 85 | 4 | fix(providers): SQLRelationStore fallback to context_nodes.relations | https://gitcode.com/opengauss/oGMemory/merge_requests/85 |
| 66 | 4 | fix(session): archive after turn snapshots safely | https://gitcode.com/opengauss/oGMemory/merge_requests/66 |
| 64 | 4 | fix(llm): retry empty required tool calls | https://gitcode.com/opengauss/oGMemory/merge_requests/64 |
| 62 | 4 | feat(cli): unified ogmem CLI to replace scattered shell scripts (closes #35) | https://gitcode.com/opengauss/oGMemory/merge_requests/62 |
| 54 | 4 | fix(scripts): cleanup outdated scripts and fix one-click startup | https://gitcode.com/opengauss/oGMemory/merge_requests/54 |
| 53 | 4 | feat(claude-plugin): add post_tool_use hook and http header for claude code plugin | https://gitcode.com/opengauss/oGMemory/merge_requests/53 |

### Issue 与提交

| 指标 | 数量 | 说明 |
|---|---:|---|
| 作者 Issue | 0 | GitCode issues API 中未发现 `user.login == jcp0578` |
| 仓库 commits author 为 `jcp0578` | 0 | GitCode commits API 中未发现 author name 为 `jcp0578` |
| 作者 MR 内提交数 | 11 | MR #34 有 10 个；MR #9 有 1 个 |

## kunpeng_compute/KunpengRAG

数据源：

- PR：`https://gitee.com/api/v5/repos/kunpeng_compute/KunpengRAG/pulls?per_page=100&page=1&state=all`
- Issue：`https://gitee.com/api/v5/repos/kunpeng_compute/KunpengRAG/issues?per_page=100&page=1&state=all`
- Commit：`https://gitee.com/api/v5/repos/kunpeng_compute/KunpengRAG/commits?per_page=100&page=1`
- 评论：`https://gitee.com/api/v5/repos/kunpeng_compute/KunpengRAG/pulls/{number}/comments`

注意：Gitee API 返回的 PR 总量当前不足 100，第一页已覆盖全量 PR；评论明细已通过访问令牌复核。

### 作者 PR

| PR | 标题 | 合入时间 | 链接 |
|---:|---|---|---|
| 67 | Update OpenFuyao image tags to 0422 | 2026-06-05 17:48:57 +08:00 | https://gitee.com/kunpeng_compute/KunpengRAG/pulls/67 |
| 66 | 修复绑核问题 | 2026-05-27 23:22:23 +08:00 | https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66 |
| 65 | Update OpenFuyao image tags to 0526 | 2026-05-26 17:11:28 +08:00 | https://gitee.com/kunpeng_compute/KunpengRAG/pulls/65 |
| 64 | Refine OpenClaw persistence for OpenFuyao deployment and the OpenViking verification flow | 2026-05-07 20:52:04 +08:00 | https://gitee.com/kunpeng_compute/KunpengRAG/pulls/64 |
| 63 | Fix OpenFuyao memory deployment docs | 2026-05-01 00:07:02 +08:00 | https://gitee.com/kunpeng_compute/KunpengRAG/pulls/63 |
| 61 | Add OpenFuyao quick start script | 2026-04-25 18:23:15 +08:00 | https://gitee.com/kunpeng_compute/KunpengRAG/pulls/61 |
| 58 | 优化文档内容，增加镜像制作说明 | 2026-03-26 09:20:29 +08:00 | https://gitee.com/kunpeng_compute/KunpengRAG/pulls/58 |
| 57 | OpenViking+OpenGauss部署说明文档 | 2026-03-18 13:33:54 +08:00 | https://gitee.com/kunpeng_compute/KunpengRAG/pulls/57 |

### 代码/配置类变更量

合计：代码/配置类文件 28 个，新增 1856 行，删除 217 行；新增文件 10 个，修改文件 17 个，删除文件 1 个。另有非代码/文档类文件 14 个。

| PR | 代码/配置类文件 | 新增行 | 删除行 | 新增文件 | 修改文件 | 删除文件 | 非代码文件 | 链接 |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 67 | 3 | 3 | 3 | 0 | 3 | 0 | 0 | https://gitee.com/kunpeng_compute/KunpengRAG/pulls/67 |
| 66 | 4 | 649 | 186 | 2 | 1 | 1 | 2 | https://gitee.com/kunpeng_compute/KunpengRAG/pulls/66 |
| 65 | 5 | 199 | 5 | 1 | 4 | 0 | 1 | https://gitee.com/kunpeng_compute/KunpengRAG/pulls/65 |
| 64 | 5 | 81 | 10 | 0 | 5 | 0 | 2 | https://gitee.com/kunpeng_compute/KunpengRAG/pulls/64 |
| 63 | 4 | 11 | 13 | 0 | 4 | 0 | 1 | https://gitee.com/kunpeng_compute/KunpengRAG/pulls/63 |
| 61 | 7 | 913 | 0 | 7 | 0 | 0 | 4 | https://gitee.com/kunpeng_compute/KunpengRAG/pulls/61 |
| 58 | 0 | 0 | 0 | 0 | 0 | 0 | 3 | https://gitee.com/kunpeng_compute/KunpengRAG/pulls/58 |
| 57 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | https://gitee.com/kunpeng_compute/KunpengRAG/pulls/57 |

### 作者提交

Gitee commits API 中 author name 为 `jcp0578` 的提交共 35 个。前 20 个如下：

| Commit | 标题 | 链接 |
|---|---|---|
| `e45f14f` | Update OpenFuyao image tags | https://gitee.com/kunpeng_compute/KunpengRAG/commit/e45f14f1488f560bae61b13e6d6d3207c363c345 |
| `2b9ec2f` | Clarify OpenFuyao cpuset usage guidance | https://gitee.com/kunpeng_compute/KunpengRAG/commit/2b9ec2f27301869c2755c1f5c4f5188ef509d561 |
| `66804ce` | Fix OpenFuyao cpuset component scoping | https://gitee.com/kunpeng_compute/KunpengRAG/commit/66804ce92018653d8fd5b362c32f5aae3d0abd3b |
| `056ace3` | Align OpenFuyao openGauss YAML naming | https://gitee.com/kunpeng_compute/KunpengRAG/commit/056ace3f1c9a20dd40ac0c02ed228173323b1dc5 |
| `b9f92c7` | Fix OpenFuyao component name typo | https://gitee.com/kunpeng_compute/KunpengRAG/commit/b9f92c7a6fc97b1c624356f1ea1a4b71bc968ec4 |
| `a34002b` | Remove NRI cpuset annotations and refresh OpenFuyao docs | https://gitee.com/kunpeng_compute/KunpengRAG/commit/a34002b80de74ca3240ad94a1a4a884db1922013 |
| `63b2407` | Refactor OpenFuyao cpuset flow into single host-bind entry | https://gitee.com/kunpeng_compute/KunpengRAG/commit/63b2407755e9dfd571f8bc0bca82304de05d6740 |
| `619e22c` | Unify OpenFuyao cpuset flow around resource policy | https://gitee.com/kunpeng_compute/KunpengRAG/commit/619e22c5870e9dde6b02f68e5e9acdeb0292f987 |
| `c9953d3` | Document host dependency checks for OpenFuyao cpuset binding | https://gitee.com/kunpeng_compute/KunpengRAG/commit/c9953d307bdb041655287518f39b8a77a41a0d46 |
| `6079353` | Update OpenFuyao image tags to 0526 | https://gitee.com/kunpeng_compute/KunpengRAG/commit/6079353676950db75fda26e9fb3fb4615cb85931 |
| `b420a1c` | Harden OpenFuyao cpuset script invocation | https://gitee.com/kunpeng_compute/KunpengRAG/commit/b420a1c7e05055b0b7fddfd64dfa01bba7c92073 |
| `ab09c19` | Add reusable cpuset binding script for OpenFuyao | https://gitee.com/kunpeng_compute/KunpengRAG/commit/ab09c19e93c69a2661c602fe628087edd756ca5f |
| `2a83e5a` | Add cpuset binding support to OpenFuyao quick start | https://gitee.com/kunpeng_compute/KunpengRAG/commit/2a83e5a62947acf0744bd682445f34102e4e9729 |
| `cd9738d` | Update OpenViking Docker verification commands | https://gitee.com/kunpeng_compute/KunpengRAG/commit/cd9738d255bfdf19b58564ebf09ab72bc2472d80 |
| `60c48ea` | Align OpenFuyao deployment guide with current scripts | https://gitee.com/kunpeng_compute/KunpengRAG/commit/60c48ead7859e48cd99fe9914a58a2fd88971582 |
| `95e3b78` | Fix OpenViking default tenant values | https://gitee.com/kunpeng_compute/KunpengRAG/commit/95e3b78d18baaf53a09349fa6c508fd64d49ee0f |
| `17acdca` | Document OpenClaw data root override timing | https://gitee.com/kunpeng_compute/KunpengRAG/commit/17acdcaed5dcf1fef8e4375bbd4d11846eb2a2c2 |
| `de6594f` | Persist OpenClaw state on host storage | https://gitee.com/kunpeng_compute/KunpengRAG/commit/de6594f1ac56d9deb59b032afb40cff79d5f4446 |
| `a71f06a` | Update OpenFuyao download source and persistence docs | https://gitee.com/kunpeng_compute/KunpengRAG/commit/a71f06a566a89f58b220e8e0bc264b1e13bcd876 |
| `6de2530` | Fix OpenFuyao memory deployment docs | https://gitee.com/kunpeng_compute/KunpengRAG/commit/6de25309b78209f2dbd395bf89e225a19357c8bb |

### Issue 与检视

| 指标 | 数量 | 说明 |
|---|---:|---|
| 作者 Issue | 0 | Gitee issues API 中未发现 `user.login == jcp0578` |
| PR 评论 | 4 | PR #68 有 2 条；PR #60 有 2 条 |
| reviewer/assignee/tester 记录 | 0 | PR 列表中未发现 `jcp0578` 作为 assignee/tester |

PR 评论明细：

| PR | 评论数 | 标题 | 链接 |
|---:|---:|---|---|
| 68 | 2 | Update OpenViking RaBitQ patch | https://gitee.com/kunpeng_compute/KunpengRAG/pulls/68 |
| 60 | 2 | feat: add one-click deployment script for OpenViking + openGauss | https://gitee.com/kunpeng_compute/KunpengRAG/pulls/60 |
