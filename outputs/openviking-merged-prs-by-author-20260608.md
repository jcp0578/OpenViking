# volcengine/OpenViking 指定作者已合入 PR 链接汇总

生成时间：2026-06-08
仓库：`volcengine/OpenViking`
筛选口径：`gh search prs --repo volcengine/OpenViking --author <login> --merged --limit 100`

## 汇总

| 作者 | 已合入 PR 数 |
|---|---:|
| `Mijamind719` | 22 |
| `jcp0578` | 6 |
| `huangxun375-stack` | 9 |
| `wlff123` | 36 |
| `LinQiang391` | 19 |
| `HaotianChen616` | 2 |
| 合计 | 94 |

## Mijamind719

- PR #2448: [Make OpenClaw auto-recall timeout configurable](https://github.com/volcengine/OpenViking/pull/2448)
- PR #2333: [fix(cli): probe embedding provider in doctor](https://github.com/volcengine/OpenViking/pull/2333)
- PR #2324: [fix(server): proxy namespace policy in tool context](https://github.com/volcengine/OpenViking/pull/2324)
- PR #2323: [docs(openclaw): document explicit memory writes](https://github.com/volcengine/OpenViking/pull/2323)
- PR #2307: [fix(openclaw-plugin): report memory_store zero extraction](https://github.com/volcengine/OpenViking/pull/2307)
- PR #1946: [fix(openclaw): split OpenViking import tools](https://github.com/volcengine/OpenViking/pull/1946)
- PR #1913: [fix(openclaw): add plugin load path before enable](https://github.com/volcengine/OpenViking/pull/1913)
- PR #1866: [fix(openclaw): support 5.2 contracts and shared remember writes](https://github.com/volcengine/OpenViking/pull/1866)
- PR #1851: [chore(agent-tools): converge MCP tool names](https://github.com/volcengine/OpenViking/pull/1851)
- PR #1540: [fix(openclaw-plugin): clean up ov-healthcheck artifacts](https://github.com/volcengine/OpenViking/pull/1540)
- PR #1511: [fix(openclaw-plugin): enforce assemble token budgets](https://github.com/volcengine/OpenViking/pull/1511)
- PR #1388: [feat: add local llama-cpp embedding support](https://github.com/volcengine/OpenViking/pull/1388)
- PR #1194: [feat(openclaw-plugin): add bypass session patterns](https://github.com/volcengine/OpenViking/pull/1194)
- PR #1136: [feat(openclaw-plugin): add session-pattern guard for ingest reply assist](https://github.com/volcengine/OpenViking/pull/1136)
- PR #938: [feat(openclaw-plugin): add archive-aware context assembly and async session commit](https://github.com/volcengine/OpenViking/pull/938)
- PR #891: [feat(openclaw-plugin):context engine refactor design & enforce token budget and reduce context bloat](https://github.com/volcengine/OpenViking/pull/891)
- PR #662: [feat(openclaw-plugin 2.0): from memory plugin to context engine](https://github.com/volcengine/OpenViking/pull/662)
- PR #268: [fix: claude code memory-plugin example:add_message改为写入TextPart列表，避免session解析异常](https://github.com/volcengine/OpenViking/pull/268)
- PR #256: [feat(vectordb): integrate KRL for ARM Kunpeng vector search optimization](https://github.com/volcengine/OpenViking/pull/256)
- PR #246: [feat(examples): add Claude memory plugin example for OpenViking](https://github.com/volcengine/OpenViking/pull/246)
- PR #137: [\[WIP\]adapt for openclaw: add memory output language pipeline](https://github.com/volcengine/OpenViking/pull/137)
- PR #131: [add_message改为写入TextPart列表，避免session消息解析异常; extract_session增加json转换方法](https://github.com/volcengine/OpenViking/pull/131)

## jcp0578

- PR #1832: [fix(openclaw-plugin): unify session and agent-prefix routing across compact, commit, and tools](https://github.com/volcengine/OpenViking/pull/1832)
- PR #1606: [feat(openclaw-plugin): align auth, namespace, and role id handling](https://github.com/volcengine/OpenViking/pull/1606)
- PR #1369: [feat(openclaw-plugin): add unified ov_import and ov_search](https://github.com/volcengine/OpenViking/pull/1369)
- PR #1058: [fix(openclaw-plugin):  fix compact result mapping](https://github.com/volcengine/OpenViking/pull/1058)
- PR #1037: [fix(openclaw-plugin): Use OpenViking-owned compaction instead of legacy delegation](https://github.com/volcengine/OpenViking/pull/1037)
- PR #1000: [fix(openclaw-plugin): use plugin-sdk exports for compaction delegation (fixes #833) openclaw ≥ v2026.3.22](https://github.com/volcengine/OpenViking/pull/1000)

## huangxun375-stack

- PR #2348: [fix: add cjk-aware token estimation](https://github.com/volcengine/OpenViking/pull/2348)
- PR #2248: [feat(openclaw): add typed synopsis stubs for externalized tool results](https://github.com/volcengine/OpenViking/pull/2248)
- PR #1858: [fix(openclaw-plugin): merge consecutive user turns (#1724)](https://github.com/volcengine/OpenViking/pull/1858)
- PR #1782: [Introduce Working Memory v2: 7-section session memory, anti-bloat guards, sliding-window tokens, and OpenClaw integration](https://github.com/volcengine/OpenViking/pull/1782)
- PR #1740: [fix(openclaw-plugin): use character budget for auto recall](https://github.com/volcengine/OpenViking/pull/1740)
- PR #1617: [adjust default value for recallTokenBudget and recallMaxContentChars.](https://github.com/volcengine/OpenViking/pull/1617)
- PR #1154: [add e2e test under tests/ut/e2e](https://github.com/volcengine/OpenViking/pull/1154)
- PR #1144: [test(openclaw-plugin): add comprehensive UT suite under tests/ut/ (27…](https://github.com/volcengine/OpenViking/pull/1144)
- PR #1053: [fix(openclaw-plugin): add defensive re-spawn for OpenViking subproces…](https://github.com/volcengine/OpenViking/pull/1053)

## wlff123

- PR #2321: [Add URI to autorecall memories](https://github.com/volcengine/OpenViking/pull/2321)
- PR #2235: [Rename search tool to ov_search to avoid conflict with existing OpenClaw tool name](https://github.com/volcengine/OpenViking/pull/2235)
- PR #2146: [feat(mcp): add code navigation tools (code_outline / code_search / code_expand)](https://github.com/volcengine/OpenViking/pull/2146)
- PR #2050: [chore(openclaw-plugin): remove unused code and redundant guards](https://github.com/volcengine/OpenViking/pull/2050)
- PR #2043: [docs(design): consolidate openclaw plugin docs into single design doc](https://github.com/volcengine/OpenViking/pull/2043)
- PR #2002: [fix(openclaw): read L2 content in memory recall](https://github.com/volcengine/OpenViking/pull/2002)
- PR #1835: [fix(openclaw): route auto recall through assemble](https://github.com/volcengine/OpenViking/pull/1835)
- PR #1675: [fix(session): count tool parts in pending tokens](https://github.com/volcengine/OpenViking/pull/1675)
- PR #1641: [Align context-engine assemble test with toolCall output](https://github.com/volcengine/OpenViking/pull/1641)
- PR #1564: [Remove ingestReplyAssist feature and all related config, logic, and t…](https://github.com/volcengine/OpenViking/pull/1564)
- PR #1482: [fix(plugin): propagate toolCallId and handle user-role tool parts in …](https://github.com/volcengine/OpenViking/pull/1482)
- PR #1472: [fix(plugin): sanitize prompt fallback in before_prompt_build to preve…](https://github.com/volcengine/OpenViking/pull/1472)
- PR #1446: [openclaw refactor: assemble context partitioning (Instruction/Archive/Session/…](https://github.com/volcengine/OpenViking/pull/1446)
- PR #1340: [afterTurn: store messages with actual roles and skip heartbeat messages](https://github.com/volcengine/OpenViking/pull/1340)
- PR #1288: [add create time in add_message](https://github.com/volcengine/OpenViking/pull/1288)
- PR #1206: [fix(openclaw-plugin): default ingestReplyAssist to false](https://github.com/volcengine/OpenViking/pull/1206)
- PR #1204: [fix(openclaw-plugin): default recallPreferAbstract to false](https://github.com/volcengine/OpenViking/pull/1204)
- PR #1180: [feat(openclaw-plugin): add end-to-end healthcheck tool for OpenViking…](https://github.com/volcengine/OpenViking/pull/1180)
- PR #1158: [Handle OpenViking outages without blocking OpenClaw](https://github.com/volcengine/OpenViking/pull/1158)
- PR #1128: [Unify test directory in openclaw-plugin](https://github.com/volcengine/OpenViking/pull/1128)
- PR #1118: [Fix HTTPX recognition issue with SOCKS5 proxy causing OpenViking crash](https://github.com/volcengine/OpenViking/pull/1118)
- PR #1052: [set DEFAULT_COMMIT_TOKEN_THRESHOLD to 20000 for openclaw-plugin](https://github.com/volcengine/OpenViking/pull/1052)
- PR #1040: [refactor(openclaw-plugin): Unified session APIs and refactored the OpenClaw context pipeline for more consistent behavior, better maintainability, and stronger test coverage.](https://github.com/volcengine/OpenViking/pull/1040)
- PR #1010: [add param for commitTokenThreshold](https://github.com/volcengine/OpenViking/pull/1010)
- PR #1009: [Feat/session context api](https://github.com/volcengine/OpenViking/pull/1009)
- PR #985: [feat(openclaw-plugin): enhance assemble and afterTurn diag output wit…](https://github.com/volcengine/OpenViking/pull/985)
- PR #976: [openclaw-plugin refactor](https://github.com/volcengine/OpenViking/pull/976)
- PR #933: [add script to uninstall openclaw-plugin](https://github.com/volcengine/OpenViking/pull/933)
- PR #902: [support commit for openclaw-plugin](https://github.com/volcengine/OpenViking/pull/902)
- PR #843: [Add instructions for cleaning up old version plugins.](https://github.com/volcengine/OpenViking/pull/843)
- PR #832: [add script to clean up memory-openviking plugin](https://github.com/volcengine/OpenViking/pull/832)
- PR #766: [update docs for openclaw-plugin](https://github.com/volcengine/OpenViking/pull/766)
- PR #758: [add openclaw-plugin upgrade description](https://github.com/volcengine/OpenViking/pull/758)
- PR #453: [\[wip\]Enhance OpenViking Status Checks in the OpenClaw Plugin](https://github.com/volcengine/OpenViking/pull/453)
- PR #449: [OpenViking Plugin Exception Handling & Fixing](https://github.com/volcengine/OpenViking/pull/449)
- PR #373: [fix: normalize OpenViking memory target paths](https://github.com/volcengine/OpenViking/pull/373)

## LinQiang391

- PR #2150: [docs(openclaw): align plugin docs with ClawHub standard install exper…](https://github.com/volcengine/OpenViking/pull/2150)
- PR #2099: [docs(openclaw): use canonical OpenViking plugin package](https://github.com/volcengine/OpenViking/pull/2099)
- PR #2072: [fix(openclaw): support npm plugin installs in setup helper](https://github.com/volcengine/OpenViking/pull/2072)
- PR #1941: [chore: release setup helper 0.3.0](https://github.com/volcengine/OpenViking/pull/1941)
- PR #1904: [feat(openclaw-plugin): 支持 OpenViking OpenClaw 插件的自动化安装与 ClawHub 发布流程，适配OpenClaw >5.4安装](https://github.com/volcengine/OpenViking/pull/1904)
- PR #1783: [refactor(openclaw-plugin): 移除 local 模式，配置项 agentId 改名为 agent_prefix](https://github.com/volcengine/OpenViking/pull/1783)
- PR #1620: [fix: Windows .bat env read/write, shell escaping, ov.conf validation,…](https://github.com/volcengine/OpenViking/pull/1620)
- PR #1587: [feat: add ClawHub publishing](https://github.com/volcengine/OpenViking/pull/1587)
- PR #1477: [fix: downgrade embedding metadata check from fatal error to warning f…](https://github.com/volcengine/OpenViking/pull/1477)
- PR #1183: [fix(embedder): pass dimensions in OpenAIDenseEmbedder.embed()](https://github.com/volcengine/OpenViking/pull/1183)
- PR #1149: [fix(openclaw-plugin): stop writing gateway.mode from installers](https://github.com/volcengine/OpenViking/pull/1149)
- PR #1054: [feat(openclaw-plugin): align agent routing and reduce default log noi…](https://github.com/volcengine/OpenViking/pull/1054)
- PR #1050: [feat(openclaw-plugin): default plugin install to latest Git tag](https://github.com/volcengine/OpenViking/pull/1050)
- PR #1020: [feat(installer, openclaw-plugin): unified installer upgrade](https://github.com/volcengine/OpenViking/pull/1020)
- PR #524: [feat(setup-helper): 用 install.js 替换 cli.js，发布为 npm 包](https://github.com/volcengine/OpenViking/pull/524)
- PR #460: [openclaw-memory-plugin: ov.conf backend/agfs, default embedding 25121…](https://github.com/volcengine/OpenViking/pull/460)
- PR #426: [\[fix\]: 修复通过curl命令安装场下下ubuntu/debian等系统触发系统保护无法安装的openviking的问题](https://github.com/volcengine/OpenViking/pull/426)
- PR #415: [支持通过curl方式安装部署openclaw+openviking插件](https://github.com/volcengine/OpenViking/pull/415)
- PR #307: [添加openclaw-openviking-plugin插件安装方式](https://github.com/volcengine/OpenViking/pull/307)

## HaotianChen616

- PR #2058: [Feat(OpenclawToolResultCompression): Add OpenViking Tool Result Externalization for OpenClaw](https://github.com/volcengine/OpenViking/pull/2058)
- PR #147: [feat(parser): support repo branch and commit refs](https://github.com/volcengine/OpenViking/pull/147)
