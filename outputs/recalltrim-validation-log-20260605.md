# recalltrim Validation Log 2026-06-05

## Purpose

- record the 2026-06-04 to 2026-06-05 LoCoMo `sample0` validation process
- record the strongest available `recalltrim` evidence
- separate what is already validated from what is still blocked

## Background

The work split the accuracy regression into two groups:

1. `8b310ad4` relative to `off`
   - main issue: temporal anchoring and compressed-memory disambiguation loss
2. `current latest` relative to `8b310ad4`
   - main issue: benchmark ingest polluted memory extraction with image-side metadata

The current best narrow fix is `recalltrim`:

- do not change recall logic
- do not refactor the main memory-extraction path
- trim recalled event memory before injection so QA sees normalized summary instead of long `ChatLog` tails

Core code change:

- `/home/jcp/Agent/code/OpenViking/examples/openclaw-plugin/auto-recall.ts`

Related supporting changes:

- `/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/import_to_ov.py`
- `/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/phase_a_off.py`
- `/home/jcp/Agent/code/OpenViking/openviking/prompts/templates/memory/events.yaml`

## Yesterday's Validation Process

### Phase 1: isolate regression causes

- compare `off baseline`, `8b310ad4`, and `current latest`
- confirm:
  - `8b310ad4` loses temporal anchoring
  - `current latest` adds image-context contamination

Reference analysis:

- `/home/jcp/Agent/code/OpenViking/outputs/sample0-accuracy-regression-analysis-20260605.md`

### Phase 2: benchmark-ingest cleanup

- make benchmark ingest text-first
- stop image caption/query metadata from polluting extraction input by default
- add local tests for ingest shaping

Representative recovery:

- `Q5 What did Caroline research?`
  - broken latest behavior: flower symbolism
  - recovered behavior: `Adoption agencies`

### Phase 3: temporal and entity narrow fixes

- preserve `profile` facts more aggressively
- make event names and person facts more explicit
- recover the main `Q10-Q14` chain in focused probes

### Phase 4: identify the remaining hard failure

- `Q10` still failed in full run even when memory content was already correct
- direct inspection showed:
  - recall was working
  - `school_speech.md` contained the correct fact
  - QA still answered as if the fact was missing

Conclusion:

- the remaining issue was not recall itself
- the injection format was burying normalized event summaries behind long recalled `ChatLog` content

### Phase 5: apply `recalltrim`

- add `normalizeInjectedMemoryContent()` in the OpenClaw plugin
- if recalled memory contains `ChatLog:`, keep only the normalized summary prefix
- retain the earlier benchmark ingest cleanup and narrower QA prompt guidance

Local verification:

- `/home/jcp/Agent/code/OpenViking/examples/openclaw-plugin/tests/ut/build-memory-lines.test.ts`
- result: `17 passed`

## Important Runs

### Focused probe proving `recalltrim`

- run id: `on_probe_q10shortevent_s1s4_20260605_0535`
- interpretation:
  - after trimming injected event content, the full `Q10-Q14` chain recovered together

Observed direction:

- `Q10` recovered
- `Q11` recovered
- `Q12` recovered
- `Q13` recovered to `Sweden`
- `Q14` recovered

### Full `sample0` validation run

- run id: `on_sample_recalltrim_r1_20260605_0605`
- scope:
  - `sample0`
  - `19 sessions`
  - `mode=on`

## Full-run Data Snapshot

Pulled from container CSV on 2026-06-05 09:57 CST.

- rows: `150`
- max_qi: `153`
- missing qi: `32, 48`
- cumulative `input_tokens`: `1,004,126`
- cumulative `total_tokens`: `1,023,845`
- average `input_tokens`: `6694.17`
- average `total_tokens`: `6825.63`

Interpretation:

- the run is effectively near-complete from a QA coverage perspective
- token usage stayed below older comparable runs on key recovered questions

## Strong Evidence from recalltrim

### Key recovered chain

These are the most important regressions that recovered in the full `sample0` run:

- `Q5`: `Adoption agencies`
- `Q6`: `Transgender woman`
- `Q10`: `in the week before 2023-06-09`
- `Q11`: `in the week before 2023-06-09`
- `Q12`: `4 years`
- `Q13`: `Sweden`
- `Q14`: `about 10 years ago`
- `Q15`: counseling / mental health path
- `Q18`: `2023-07-02`
- `Q19`: `July 2023`
- `Q22`: `2023-07-05`
- `Q23`: `the week before 2023-07-06`
- `Q27`: `2023-07-10`
- `Q28`: `2022`
- `Q29`: no writing career path
- `Q30`: `2023-07-14`
- `Q31`: `2023-07-14`
- `Q33`: week before `2023-06-27`
- `Q118-Q125`: largely aligned
- `Q131-Q138`: largely aligned
- `Q141-Q142`: aligned

### Token evidence

Representative token reduction:

- `Q10` older full run
  - `input_tokens=6819`
  - `total_tokens=7006`
- `Q10` recalltrim full run
  - `input_tokens=6715`
  - `total_tokens=6741`

This is the clearest single-question token win because it also corresponds to a previously broken answer that is now correct.

## Known Remaining Weak Areas

The remaining loss pattern is now concentrated in QA output boundary behavior, not recall or memory extraction.

### Enumeration/list answers expand too much

- `Q16`
- `Q17`
- `Q20`
- `Q21`
- `Q24`
- `Q25`
- `Q26`
- `Q34`
- `Q35`
- `Q36`
- `Q40`
- `Q41`

Representative behavior:

- answer includes related but not asked-for items
- answer includes unnamed placeholders
- answer includes inferred broader categories

### Fine-grained detail/object questions become generic

- `Q112`: expected `a cup with a dog face on it`, answered as generic `pots`
- `Q114`: expected `a sunset with a palm tree`, answered as generic later painting
- `Q135`: wording of the sign remained too vague
- `Q139`: expected `pink sky sunset`, answered with the blue-streak abstract details from the other painting
- `Q140`: expected `abstract painting with blue streaks on a wall`, answered too generically

### Explicit detail still missing on some facts

- `Q116`: sunflower meaning not recovered
- `Q122`: whose birthday not recovered
- `Q126`: pets set expanded too far
- `Q128`: horseback-riding fact not recovered

## Continued Validation on 2026-06-05

### What was checked today

1. confirm the main `recalltrim` full-run CSV is still available
2. pull the latest CSV snapshot out of the remote container
3. verify recovered key questions and still-weak questions directly from the CSV
4. check whether a usable formal judge environment exists

### Judge environment status

Checked both local and remote/container environments:

- local:
  - no `~/.openviking_benchmark_env`
  - `ARK_API_KEY`: absent
  - `OPENAI_API_KEY`: present
- remote container:
  - no `~/.openviking_benchmark_env`
  - `ARK_API_KEY`: absent
  - `OPENAI_API_KEY`: absent

Implication:

- the official judge path should use:
  - base URL: `https://ark.cn-beijing.volces.com/api/coding/v3`
  - model: `doubao-seed-2.0-pro`
- but there is currently no usable ARK judge credential in either local or remote validation environment

## Official Judge Result

On 2026-06-05, `judge.py` was run successfully with the official ARK path:

- base URL: `https://ark.cn-beijing.volces.com/api/coding/v3`
- model: `doubao-seed-2.0-pro`

Judged file:

- `/tmp/recalltrim-local/recalltrim.csv`

Result:

- valid rows: `150`
- correct: `120`
- wrong: `30`
- judged accuracy: `80.00%`

This converts the earlier engineering evidence into a formal judged result.

## Current Conclusion

What is already supported by evidence:

- `recalltrim` is the strongest current fix
- it repairs the most important full-run regression chain
- it lowers token usage on important recovered questions
- it does so without changing recall logic or refactoring the main extraction pipeline
- it now also has a formal judged accuracy result of `80.00%` on the validated `sample0` CSV

Comparison against earlier reference runs:

- `off baseline`: `114 / 150 = 76.00%`
- `8b310ad4` best observed reference: `107 / 150 = 71.33%`
- `current latest r1`: `99 / 150 = 66.00%`
- `current latest r2`: `92 / 150 = 61.33%`
- `recalltrim`: `120 / 150 = 80.00%`

## sample0 Comparison Table

Token scope in this table:

- `ingest token = ov_direct_ingest_total_tokens`
- `QA token = gateway_qa_tokens`
- `total token = ingest + QA`
- judge token is excluded

| scheme | run_id | accuracy | ingest token | QA token | total token | vs recalltrim |
|---|---|---:|---:|---:|---:|---|
| `off baseline` | `off_sample_cfg_20260604_0112` | `76.00%` | `401,822` | `1,258,566` | `1,660,388` | accuracy `-4.00%`, total token `+249,896` |
| `8b310ad4` | `on_sample_8b310ad4_r3_20260604_1702` | `71.33%` | `421,511` | `1,273,676` | `1,695,187` | accuracy `-8.67%`, total token `+284,695` |
| `current latest r1` | `on_sample_latest_r1_20260604_1247` | `66.00%` | `334,304` | `1,054,220` | `1,388,524` | accuracy `-14.00%`, total token `-21,968` |
| `current latest r2` | `on_sample_latest_r2_20260604_1336` | `61.33%` | `338,812` | `1,053,219` | `1,392,031` | accuracy `-18.67%`, total token `-18,461` |
| `recalltrim` | `on_sample_recalltrim_r1_20260605_0605` | `80.00%` | `386,647` | `1,023,845` | `1,410,492` | baseline |

### QA-token only comparison

| scheme | QA token | delta vs recalltrim |
|---|---:|---:|
| `off baseline` | `1,258,566` | `+234,721` |
| `8b310ad4` | `1,273,676` | `+249,831` |
| `current latest r1` | `1,054,220` | `+30,375` |
| `current latest r2` | `1,053,219` | `+29,374` |
| `recalltrim` | `1,023,845` | `0` |

### Comparison takeaway

- against `off baseline`, `recalltrim` is both more accurate and lower in total token cost
- against `8b310ad4`, `recalltrim` is both more accurate and lower in total token cost
- against `current latest`, `recalltrim` is much more accurate; total token is slightly higher, but QA token is lower

## Recommended Next Step

If validation continues, the highest-value next action is:

1. stabilize the remaining QA-boundary misses
2. focus only on:
   - list/enumeration over-expansion
   - object/detail over-generalization
3. avoid spending effort on recall logic or large extraction refactors

The current bottleneck is no longer missing judge infrastructure. It is the residual QA answer-style error pattern.

## recalltrim Re-test Round 2

Re-test run:

- run id: `on_sample_recalltrim_r2_20260605_1053`
- scope:
  - `sample0`
  - `19 sessions`
  - `mode=on`
  - `ingest-mode=direct-ov`

Official judge result:

- valid rows: `150`
- correct: `118`
- wrong: `32`
- judged accuracy: `78.67%`

Token summary:

- `gateway_qa_tokens = 1,038,402`
- `ov_direct_ingest_total_tokens = 391,811`
- `total token = 1,430,213`
- CSV summed `input_tokens = 1,020,021`
- CSV summed `total_tokens = 1,038,402`

Interpretation:

- the `recalltrim` scheme remained above `off baseline`
- but this re-test regressed slightly versus `recalltrim r1`
- judged accuracy dropped from `80.00%` to `78.67%`
- total token rose from `1,410,492` to `1,430,213`

### recalltrim r2 vs baseline

| scheme | run_id | accuracy | ingest token | QA token | total token | vs `off baseline` |
|---|---|---:|---:|---:|---:|---|
| `off baseline` | `off_sample_cfg_20260604_0112` | `76.00%` | `401,822` | `1,258,566` | `1,660,388` | baseline |
| `recalltrim r2` | `on_sample_recalltrim_r2_20260605_1053` | `78.67%` | `391,811` | `1,038,402` | `1,430,213` | accuracy `+2.67%`, total token `-230,175` |

### recalltrim r1 vs r2

| scheme | run_id | accuracy | ingest token | QA token | total token |
|---|---|---:|---:|---:|---:|
| `recalltrim r1` | `on_sample_recalltrim_r1_20260605_0605` | `80.00%` | `386,647` | `1,023,845` | `1,410,492` |
| `recalltrim r2` | `on_sample_recalltrim_r2_20260605_1053` | `78.67%` | `391,811` | `1,038,402` | `1,430,213` |

Delta `r2 - r1`:

- accuracy: `-1.33%`
- ingest token: `+5,164`
- QA token: `+14,557`
- total token: `+19,721`
