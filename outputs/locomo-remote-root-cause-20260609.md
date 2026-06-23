# LoCoMo Remote Root Cause Notes - 2026-06-09

## Confirmed harness issues

1. Earlier remote `on` subset evidence was invalid.
   - Cause: helper only restarted `openclaw-gateway`, not `openviking.server.bootstrap`.
   - Result: `ov.conf.memory.wm_v2_preprocess_enabled` changed on disk, but the live OpenViking process still used the previous `off` runtime.
   - Evidence:
     - `/home/jcp/agent/debug-code/openclaw_gateway_stop_check.sh` only stops gateway.
     - remote `on` meta previously showed `wm_preprocess.status=disabled`.

2. Remote service was importing site-packages instead of the repo checkout.
   - Cause: helper started OpenViking before exporting `PYTHONPATH=/home/jcp/agent/code/OpenViking`.
   - Result: startup log showed `Unknown config field 'wm_v2_preprocess_enabled'`.
   - Fix in local helper:
     - stop OpenViking process explicitly
     - export `PYTHONPATH` before starting OpenViking
     - then start `python3 -m openviking.server.bootstrap`

3. After the helper fix, `on` mode is genuinely active.
   - Evidence: `/tmp/on_sample5_subset_q1_1_smoke_20260609c/..._meta.json`
   - First ingest session shows:
     - `wm_preprocess.enabled=true`
     - `wm_preprocess.status=active`
     - non-empty `selected_spans`
     - non-empty `structured_facts`

## Confirmed failure layer

The current first stable failure is **after selected_spans / structured_facts, before persisted memories**.

Evidence from `/tmp/on_sample5_subset_q1_1_trace_20260609a/..._meta.json`:

- `wm_preprocess.status = active`
- `selected_spans` populated
- `structured_facts_count = 4`
- `compact_status.commit_status = completed`
- `ov_commit_count = 1`
- `memories_extracted.total = 0`
- `memory_count = 0`

So:

- not a `selected_spans` generation failure
- not a session commit failure
- not primarily a retrieval failure

The first failing layer is the **memory extraction output / operation generation / persist path**.

## What is ruled out

1. Not a simple `neighbor_window` / `neighbor_budget_tokens` regression.
   - Because earlier `on` evidence was partially invalid due harness/runtime mismatch.
   - After fixing the harness, the live `on` path still shows zero extracted memories even when preprocessor is active.

2. Not a blanket updater/persistence failure.
   - Isolated apply-path probe on sample5 session1 with aligned IDs produced:
     - `OPS_UPSERTS = 8`
     - `APPLY_RESULT = 8 written / 0 edited / 0 deleted / 0 errors`
     - returned `contexts = 8`
   - It also wrote real files under:
     - `.../user/user-s5-extractor-debug-20260609b/memories/entities/...`
     - `.../user/user-s5-extractor-debug-20260609b/memories/events/...`

So updater + file persistence can work correctly in isolation.

3. Not a deterministic extractor success either.
   - After fixing stale output-path confusion, `remote_extractor_only_probe.py`
     on the same session and same ID set can also return:
     - `upsert_operations = []`

This means the extraction/generation layer is currently unstable or sensitive
to context differences, rather than consistently failing in one fixed step.

## Still open

1. Why live session-commit extraction for LoCoMo often ends with:
   - `wm_preprocess active`
   - `memories_extracted.total = 0`
   even though an isolated apply-path probe for the same session can write 8 memories.
2. Why `remote_extractor_only_probe.py` is itself unstable for the same session/ID set
   (sometimes empty, sometimes non-empty).
3. Why `locomo-eval` QA sessions still do not emit `*.trajectory.jsonl`, so `relevant_memories` cannot yet be recovered from the standard trajectory path.

## Immediate next debugging targets

1. Compare live commit-path extraction vs isolated apply-path extraction at the
   `ExtractLoop.run()` boundary:
   - message formatting
   - request role / identity
   - latest archive overview
   - search/read prefetch context
2. Capture raw extractor payload for one empty case and one non-empty case from
   the same session.
3. Re-run representative live subsets only after the extraction instability is
   reduced; otherwise retrieval conclusions remain confounded.

## Update: recall layer root cause confirmed

The later remote evidence shows the main QA failure was not the extraction
pipeline itself. The direct cause for the wrong LoCoMo answers in the tested
QA sessions was **auto-recall failing before any memories were injected**.

### Evidence

1. The new remote run `on_sample5_subset_q1_1_fixuri_20260609d` improved from:
   - previous answer: `2018` (`WRONG`)
   - new answer: `2020` (`CORRECT`)

2. The matching OpenClaw QA session log now contains injected memories:
   - session file:
     `/root/.openclaw/agents/locomo-eval/sessions/9a485353-1394-4ce4-a43d-075b2e7d8210.jsonl`
   - user message includes `<relevant-memories> ... </relevant-memories>`
   - one injected snippet explicitly says Audrey had the first three dogs for
     3 years as of `2023-03-27`, which supports `2020`

3. The gateway log for the fixed run shows recall is now active:
   - `hook before_prompt_build ... on_sample5_subset_q1_1_fixuri_20260609d`
   - `openviking: injecting 5 memories (~421 tokens, budget=4000)`
   - `inject-detail ... new_dog_collars ... has had for 3 years as of 2023-03-27`

4. Older failing runs showed a deterministic namespace error instead:
   - `User URI must include /agent/{agent_id} under current policy`
   - `Agent URI must include /user/{user_id} under current policy`
   - So the plugin was searching `viking://user/<user>/memories` and
     `viking://agent/<agent>/memories`, while the live account policy required
     nested user+agent canonical roots.

### Minimal fix implemented

Local code change:
- `examples/openclaw-plugin/client.ts`

Behavior:
- when `find()` receives the namespace-shape `INVALID_ARGUMENT` error,
  it retries once with the agent-qualified or user-qualified canonical target URI

Added tests:
- `examples/openclaw-plugin/tests/ut/client.test.ts`
  - retries user memory alias with agent-qualified canonical URI
  - retries agent memory alias with user-qualified canonical URI

Verification:
- `npx vitest run tests/ut/client.test.ts -t "retries .* canonical URI when server requires it"`
- `npx vitest run tests/ut/client.test.ts tests/ut/context-engine-assemble.test.ts`

### Revised conclusion

For the tested `sample5 q1` case, the first blocking failure layer is:

`selected_spans / extracted memories exist`
-> `auto-recall search target_uri shape is rejected by OpenViking`
-> `no relevant memories injected`
-> `final answer regresses`

So this specific regression belongs primarily to the **retrieval / recall
integration layer**, not to prompt extraction.

## Regression results after fix

Remote environment:
- host: `123.60.114.206:10008`
- container: `jcp-dev`
- framework/code: unchanged benchmark harness, unchanged test code
- fix applied only in plugin runtime/client code

### sample5 subset

- old baseline: `off_sample5_subset_q1_10_20260608s2`
  - `1/7 = 14.29%`
- fixed run: `on_sample5_subset_q1_10_fixuri_20260609e`
  - `6/7 = 85.71%`

### sample6 subset

- old baseline: `off_sample6_subset_q1_20_20260608s1`
  - `1/19 = 5.26%`
- fixed run: `on_sample6_subset_q1_20_fixuri_20260609f`
  - `10/19 = 52.63%`

### sample9 subset

- old baseline: `off_sample9_subset_q75_90_20260609s4`
  - `2/16 = 12.50%`
- fixed run: `on_sample9_subset_q75_90_fixuri_20260609g`
  - `12/16 = 75.00%`

### Interpretation

The same fix improved all three representative degraded subsets:

- sample5: `+71.42` points
- sample6: `+47.37` points
- sample9: `+62.50` points

This is strong evidence that the main regression for the tested 5/6/9 subsets
was the failed recall injection path, not the extraction preprocessor itself.

Remaining errors now should be analyzed as second-order issues:
- recall ranking quality
- answer synthesis / normalization
- question-specific reasoning gaps
