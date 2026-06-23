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

## memory_updater Probe: events abstract trims ChatLog

Experiment scope:

- code change only in:
  - `openviking/session/memory/memory_updater.py`
- rule:
  - for `memory_type == events`, if the stored event body contains `ChatLog:`,
    trim the `abstract` and `embedding_text` prefix at the `ChatLog:` line
- benchmark/test code unchanged
- prompt/template unchanged

Local verification:

- `tests/unit/session/memory/test_searchable.py`
- `tests/benchmark/locomo/openclaw/test_import_to_ov.py`
- `tests/benchmark/locomo/openclaw/test_phase_a_off.py`
- `tests/unit/session/test_extraction_preprocessor.py`
- plugin recalltrim unit still passed

Result summary:

- minimal probe run id: `on_probe_memoryupdater_s1s4_q1015_20260606_0010`
- scope:
  - `sample0`
  - `sessions 1-4`
  - `Q10-Q15`
  - `skip-judge` in remote run, then judged locally with Doubao

Judge result:

- correct: `6/6`
- accuracy: `100.00%`

### Same-scope comparison vs current recalltrim baseline

| metric | recalltrim baseline | memory_updater probe | delta |
|---|---:|---:|---:|
| correctness over `Q10-Q15` | `6/6` | `6/6` | `0` |
| `input_tokens` sum | `40,293` | `41,504` | `+1,211` |
| `total_tokens` sum | `40,827` | `42,311` | `+1,484` |

Per-question token comparison:

| qi | baseline input | probe input | delta | baseline total | probe total | delta |
|---|---:|---:|---:|---:|---:|---:|
| `10` | `6,715` | `7,036` | `+321` | `6,741` | `7,062` | `+321` |
| `11` | `6,776` | `7,078` | `+302` | `6,792` | `7,424` | `+632` |
| `12` | `6,701` | `7,023` | `+322` | `6,760` | `7,047` | `+287` |
| `13` | `6,635` | `6,281` | `-354` | `6,797` | `6,451` | `-346` |
| `14` | `6,570` | `6,953` | `+383` | `6,744` | `7,128` | `+384` |
| `15` | `6,896` | `7,133` | `+237` | `6,993` | `7,199` | `+206` |

Interpretation:

- the code-path change preserved correctness on `Q10-Q15`
- but it did **not** reduce token cost
- token got worse on `5/6` questions
- the largest regression was `Q11`, where the answer expanded and total token rose by `+632`

Decision:

- do **not** push this `memory_updater.py` change to full `sample0`
- do **not** use it as the new recalltrim candidate for broader sample validation
- keep current recalltrim baseline unchanged

Next-step implication:

- stop this optimization line
- if recalltrim validation is expanded to other LoCoMo samples, use the existing recalltrim baseline rather than this `memory_updater` variant

## Expanded validation kickoff: sample 1 off baseline

New target sample:

- `sample 1`
- LoCoMo id: `conv-30`

Driver status notes:

- first attempt with `phase_a_off.py` hit a repeatable bootstrap blocker:
  - `phase_a_off.py` auto-called `restart_local_gateway_for_base_url(...)`
  - it did this whenever plugin namespace config changed
  - the internal gateway restart did not come back healthy on the container
- confirmed from code:
  - the restart is triggered by `sync_plugin_config` changes in `phase_a_off.py`
- workaround used without modifying benchmark code:
  - pre-sync `/root/.openclaw/openclaw.json` externally
  - restart gateway externally
  - then run `phase_a_off.py` with `--no-sync-plugin-config`

Current active off run:

- run id: `off_sample1_cfg_20260606d`
- mode: `off`
- ingest mode: `direct-ov`

Current progress snapshot:

- services healthy:
  - OpenViking: healthy
  - gateway: healthy
- ingest has started:
  - `session 1/19` memories=`7`
  - `session 2/19` memories=`0`

Pending:

- wait for full `off_sample1_cfg_20260606d` ingest / QA / judge
- then run the corresponding `recalltrim` sample-1 run under the same external namespace-sync workaround

### sample 1 off progress update

Current confirmed progress for `off_sample1_cfg_20260606d`:

- `phase_a_off.py` process remains alive
- ingest has advanced through at least `session 14/19`
- observed ingest snapshots:
  - `session 1` memories=`7`
  - `session 2-14` memories=`0`
- QA has not started yet at this snapshot

Operational note:

- the external namespace-sync workaround is required for this sample line too
- without pre-writing `openclaw.json` and passing `--no-sync-plugin-config`,
  `phase_a_off.py` re-enters its internal gateway restart path and fails before ingest

### sample 1 off QA start confirmed

Updated status for `off_sample1_cfg_20260606d`:

- ingest has completed through `session 19/19`
- QA has started
- current CSV snapshot contains:
  - `Q1`
  - `Q2`

Early QA observations:

- `Q1` current answer:
  - `Jon lost his job as a banker when he moved from Germany to pursue acting.`
- `Q2` current answer:
  - `Gina lost her job at DoorDash in the week before 2023-07-23.`

Interpretation:

- the sample-1 `off` path is now confirmed end-to-end viable under the external namespace-sync workaround
- next step remains:
  - wait for full `off_sample1_cfg_20260606d` judge result
  - then launch the prepared sample-1 `recalltrim` run under the same workaround

### sample 1 off final result

Final off run:

- run id: `off_sample1_cfg_20260606d`
- sample: `1` (`conv-30`)
- mode: `off`
- scope:
  - `19 sessions`
  - `81` judged QA rows

Judge result:

- correct: `13/81`
- accuracy: `16.05%`

Observed failure pattern:

- late-session questions around `Q70-Q82` were especially weak
- clear misses included:
  - `Q72`: answered `lovely`, gold `amazing`
  - `Q75`: answered concrete opening activities, gold `savor all the good vibes`
  - `Q76`: answered a restaurant opening anecdote instead of Gina's encouragement
  - `Q78`: answered `jewelry`, gold `Hoodies`
  - `Q79`: answered `professional experience and empathy`, gold `positivity and determination`
  - `Q81`: answer stayed generic and missed the three explicit business plans
- token spikes also appeared in some wrong answers:
  - `Q72 input_tokens = 19,967`
  - `Q76 input_tokens = 20,212`

Interpretation:

- the `off` baseline for sample 1 is much weaker than sample0's `off baseline`
- this makes sample 1 a useful discriminator for whether recalltrim's gains generalize or whether they were sample0-specific

### sample 1 recalltrim launch

Prepared and launched the matching recalltrim run under the same external namespace-sync workaround:

- run id: `on_sample1_recalltrim_20260606a`
- mode: `on`
- ingest mode: `direct-ov`
- current confirmed start:
  - `session 1/19` memories=`7`

### sample 1 recalltrim mid-run snapshot

Current snapshot:

- CSV has reached `Q47`
- partial comparison window:
  - `Q1-Q47`

Partial judged comparison:

| scheme | window | correct | total | accuracy |
|---|---|---:|---:|---:|
| `off_sample1_cfg_20260606d` | `Q1-Q47` | `6` | `47` | `12.77%` |
| `on_sample1_recalltrim_20260606a` | `Q1-Q47` | `8` | `47` | `17.02%` |

Partial token comparison:

| scheme | `Q1-Q47 input_tokens` | `Q1-Q47 total_tokens` |
|---|---:|---:|
| `off_sample1_cfg_20260606d` | `364,612` | `388,988` |
| `on_sample1_recalltrim_20260606a` | `398,520` | `422,545` |

Interpretation:

- by the midpoint, recalltrim is still noisy and clearly wrong on many questions
- but it is not strictly worse than off on the judged prefix
- partial accuracy is higher than off (`17.02%` vs `12.77%`)
- token cost is also higher on the same prefix

Important caution:

- this does **not** prove recalltrim wins overall on sample 1
- it only shows that the early/mid-run evidence is mixed:
  - accuracy slightly better so far
  - token clearly worse so far
  - answer quality still shows strong time-anchor and fact-substitution pathologies

### sample 1 recalltrim deeper partial snapshot

Updated partial comparison window:

- `Q1-Q65`

| scheme | window | correct | total | accuracy |
|---|---|---:|---:|---:|
| `off_sample1_cfg_20260606d` | `Q1-Q65` | `10` | `65` | `15.38%` |
| `on_sample1_recalltrim_20260606a` | `Q1-Q65` | `16` | `65` | `24.62%` |

Token comparison on the same prefix:

| scheme | `Q1-Q65 input_tokens` | `Q1-Q65 total_tokens` |
|---|---:|---:|
| `off_sample1_cfg_20260606d` | `540,837` | `575,741` |
| `on_sample1_recalltrim_20260606a` | `539,866` | `572,912` |

Interpretation:

- by `Q1-Q65`, recalltrim is ahead of off on both metrics:
  - higher partial accuracy
  - slightly lower partial token cost
- this overturns the earlier impression that recalltrim was simply worse on sample 1
- however, the failure mode remains structurally bad:
  - many time questions still collapse toward the `2023-07-23` anchor
  - multiple facts still substitute unrelated cities, careers, or life stages
  - some answers leak workspace/meta language

### sample 1 final comparison

Final off vs recalltrim comparison for sample 1 (`conv-30`):

| scheme | run_id | valid | correct | accuracy | input_tokens | total_tokens |
|---|---|---:|---:|---:|---:|---:|
| `off` | `off_sample1_cfg_20260606d` | `81` | `13` | `16.05%` | `689,441` | `733,126` |
| `recalltrim` | `on_sample1_recalltrim_20260606a` | `81` | `19` | `23.46%` | `688,554` | `731,974` |

Delta `recalltrim - off`:

- accuracy: `+7.41%`
- input_tokens: `-887`
- total_tokens: `-1,152`

Conclusion:

- recalltrim is better than off on sample 1
- the gain is not marginal: recalltrim wins on both accuracy and total token cost
- even though many individual answers still look poor, the judged aggregate result is clearly better than off

Next action:

- continue to the next sample with the same external namespace-sync workaround

### sample 2 off final result

Final off result for sample 2 (`conv-41`):

| scheme | run_id | valid | correct | accuracy | input_tokens | total_tokens |
|---|---|---:|---:|---:|---:|---:|
| `off` | `off_sample2_cfg_20260606a` | `85` | `15` | `17.65%` | `706,793` | `757,524` |

Observed answer-pathology pattern:

- early and middle questions again showed time-anchor collapse toward August 2023
- repeated entity substitution across people, places, and activities
- multiple late questions drifted into generic or unrelated narratives
- no benchmark code changes were made; this used the same external namespace-sync workaround as sample 1

Next action:

- launch `sample 2 / recalltrim` with the same external namespace-sync workaround for direct comparison

### sample 2 final comparison

Final off vs recalltrim comparison for sample 2 (`conv-41`):

| scheme | run_id | valid | correct | accuracy | input_tokens | total_tokens |
|---|---|---:|---:|---:|---:|---:|
| `off` | `off_sample2_cfg_20260606a` | `85` | `15` | `17.65%` | `706,793` | `757,524` |
| `recalltrim` | `on_sample2_recalltrim_20260606a` | `85` | `75` | `88.24%` | `581,474` | `593,781` |

Delta `recalltrim - off`:

- accuracy: `+70.59%`
- input_tokens: `-125,319`
- total_tokens: `-163,743`

Conclusion:

- recalltrim is dramatically better than off on sample 2
- unlike sample 1, sample 2 recalltrim is not merely a marginal gain; it restores the run to a high-accuracy regime

Representative remaining recalltrim sample 2 error modes:

- partial list answers: e.g. `kickboxing` without `Taekwondo`
- event-detail substitution: e.g. fundraiser sub-event vs parent event
- occasional unrelated retrieval jump: e.g. `Q87` answering road trip instead of `car broke down`
- one late severe miss: `Q108` answering gym instead of dog-shelter volunteering

### low-accuracy analysis for sample 1 / sample 2

Reference sanity check from historical report:

- historical OV `conv-30` accuracy: `77.8%`
- historical OV `conv-41` accuracy: `75.7%`

Current results against that reference:

| sample | current off | current recalltrim | historical OV |
|---|---:|---:|---:|
| `conv-30` | `16.05%` | `23.46%` | `77.8%` |
| `conv-41` | `17.65%` | `88.24%` | `75.7%` |

Initial diagnosis:

1. The very low accuracies are **not** explained by dataset hardness alone.
   - `conv-41` under current recalltrim reaches `88.24%`
   - so the framework can still produce healthy results on at least one sample

2. The current `off` path appears systematically unhealthy on both samples.
   - both `conv-30` and `conv-41` off runs are stuck around `16-18%`
   - both show large-scale time-anchor drift, entity substitution, and generic “no recalled memory” failures

3. `conv-30` has a sample-specific severe failure even under recalltrim.
   - recalltrim improves `conv-30` over off, but only to `23.46%`
   - this is still far below the historical `77.8%`
   - observed failure pattern includes:
     - many temporal questions collapsing toward `2023-07-23`
     - fact substitution across unrelated people / cities / activities
     - occasional workspace/meta leakage

4. `conv-41` recalltrim suggests the current recalltrim path itself is not globally broken.
   - most remaining misses are narrower:
     - missing one element from a set
     - over-specific or under-specific event naming
     - a few isolated retrieval jumps

Implication for next step:

- before expanding to the next sample, the most important analysis target is now `conv-30 recalltrim`
- because `conv-41 recalltrim` already demonstrates that high accuracy is achievable in the current framework

Refined diagnosis details:

- `conv-30` (`sample 1`) low accuracy is driven by a broad early collapse:
  - first `10` judged questions are almost all wrong in both `off` and `recalltrim`
  - common failure shapes:
    - date answers repeatedly collapse to the `2023-07-23` anchor
    - “I don’t have recalled memory” style misses on facts that should be local and recoverable
    - cross-entity substitution such as unrelated cities / people / businesses
    - occasional workspace/meta leakage

- `conv-41` (`sample 2`) shows a different picture:
  - `off` is broadly unhealthy from the first ten questions onward
  - but `recalltrim` recovers to `88.24%`, proving the current framework can still hit a healthy regime on this sample
  - remaining `recalltrim` misses are mostly narrow:
    - partial list answers
    - event parent/child substitution
    - isolated retrieval jumps
    - one severe late miss (`Q108`)

Operational interpretation:

- the current `off` path is likely not a reliable absolute baseline for these expanded-sample runs
- `conv-30 recalltrim` is now the highest-value anomaly to explain, because its failure pattern is much broader than `conv-41 recalltrim`

Abnormality threshold:

- from this point onward, `accuracy < 50%` is treated as **abnormal**

Applying that threshold to the current expanded-sample runs:

| sample | scheme | accuracy | abnormal |
|---|---|---:|---:|
| `conv-30` | `off` | `16.05%` | yes |
| `conv-30` | `recalltrim` | `23.46%` | yes |
| `conv-41` | `off` | `17.65%` | yes |
| `conv-41` | `recalltrim` | `88.24%` | no |

Implication:

- `conv-30` is a **dual-abnormal** sample in the current framework
- `conv-41` is **off-abnormal but recalltrim-healthy**

Concrete abnormal failure signatures:

- `conv-30 recalltrim`
  - temporal collapse to `2023-07-23` on many questions:
    - `Q2/Q8/Q11/Q13/Q14/Q15/Q20/Q21/Q22/Q23/Q27/Q33/Q34`
  - no-memory / cannot-answer failures on answerable local facts:
    - `Q6/Q7/Q9/Q12/Q16/Q25/Q30/Q42/Q44/Q63`
  - broad fact substitution:
    - `Q3/Q4/Q10/Q18/Q29/Q31/Q35/Q40/Q66/Q71/Q72/Q75/Q76`
  - meta leakage:
    - `Q52/Q53`

- `conv-41 off`
  - time-anchor collapse toward August 2023:
    - `Q2/Q5/Q6/Q21/Q23/Q25/Q28/Q35`
  - no-memory / insufficient-evidence style misses:
    - `Q3/Q7/Q8/Q15/Q17/Q18/Q22/Q24/Q31/Q33/Q36/Q77/Q83/Q89/Q110`
  - entity substitution:
    - `Q1/Q4/Q10/Q13/Q19/Q20/Q26/Q29/Q32/Q36/Q66`
  - meta leakage:
    - `Q30`

- `conv-41 recalltrim` remaining errors are not abnormal-scale
  - mostly partial-list answers:
    - `Q3/Q7/Q24/Q30/Q31/Q103`
  - isolated wrong-fact jumps:
    - `Q9/Q87`
  - one severe late miss:
    - `Q108`

### sample 3 off launched

Started next baseline run because `sample 2 recalltrim > off`:

| item | status |
|---|---|
| run_id | `off_sample3_cfg_20260606a` |
| mode | `off` |
| current progress | ingest reached `session 4/19` |
| early shape | `session 1 memories=5`, `session 2-4 memories=0` |

### sample 3 off final result

Final off result for sample 3 (`conv-42`):

| scheme | run_id | valid | correct | accuracy | input_tokens | total_tokens |
|---|---|---:|---:|---:|---:|---:|
| `off` | `off_sample3_cfg_20260606a` | `103` | `78` | `75.73%` | `732,504` | `748,463` |

Abnormality check (`<50%` = abnormal):

- `sample 3 / off` is **not abnormal**

Interpretation:

- unlike `conv-30 off` and `conv-41 off`, `conv-42 off` stays in a healthy accuracy range
- this further supports the current diagnosis that the very low accuracies are not caused by a global model-service collapse
- the abnormality is sample/path-specific, not universal across all expanded-sample off runs

### sample 3 final comparison

Final off vs recalltrim comparison for sample 3 (`conv-42`):

| scheme | run_id | valid | correct | accuracy | input_tokens | total_tokens |
|---|---|---:|---:|---:|---:|---:|
| `off` | `off_sample3_cfg_20260606a` | `103` | `78` | `75.73%` | `732,504` | `748,463` |
| `recalltrim` | `on_sample3_recalltrim_20260606a` | `103` | `87` | `84.47%` | `716,221` | `729,079` |

Delta `recalltrim - off`:

- accuracy: `+8.74%`
- input_tokens: `-16,283`
- total_tokens: `-19,384`

Conclusion:

- recalltrim is better than off on sample 3
- unlike sample 2, this is not a dramatic recovery from an abnormal baseline
- instead, sample 3 shows a healthier baseline plus a meaningful-but-moderate recalltrim gain

Interpretation:

- sample 3 strengthens the current hypothesis:
  - the framework and model service are capable of healthy runs
  - sample/path-specific behavior explains the abnormal low-accuracy runs better than a universal service failure

Next action:

- continue to the next sample because `sample 3 recalltrim > off`

### sample 4 off final result

Final off result for sample 4 (`conv-43`):

| scheme | run_id | valid | correct | accuracy | input_tokens | total_tokens |
|---|---|---:|---:|---:|---:|---:|
| `off` | `off_sample4_cfg_20260606a` | `114` | `100` | `87.72%` | `778,902` | `794,584` |

Abnormality check (`<50%` = abnormal):

- `sample 4 / off` is **not abnormal**

Interpretation:

- like `conv-42 off`, `conv-43 off` stays in a healthy range
- this further weakens the hypothesis that the low scores on `conv-30` and `conv-41 off` came from a global model-service problem
- the current evidence continues to point toward sample/path-specific behavior

### sample 4 final comparison

Final off vs recalltrim comparison for sample 4 (`conv-43`):

| scheme | run_id | valid | correct | accuracy | input_tokens | total_tokens |
|---|---|---:|---:|---:|---:|---:|
| `off` | `off_sample4_cfg_20260606a` | `114` | `100` | `87.72%` | `778,902` | `794,584` |
| `recalltrim` | `on_sample4_recalltrim_20260606a` | `114` | `97` | `85.09%` | `784,056` | `796,065` |

Delta `recalltrim - off`:

- accuracy: `-2.63%`
- input_tokens: `+5,154`
- total_tokens: `+1,481`

Conclusion:

- recalltrim is worse than off on sample 4
- this is not an abnormal-run collapse; both runs stay in a healthy range
- the regression is a small but real net loss

Wrong-set comparison:

- off wrong qids:
  - `7, 8, 12, 20, 21, 26, 36, 43, 47, 49, 50, 114, 136, 137`
- recalltrim wrong qids:
  - `7, 8, 12, 17, 20, 26, 30, 36, 39, 41, 43, 47, 50, 100, 107, 114, 136`
- fixed by recalltrim:
  - `21, 49, 137`
- regressed under recalltrim:
  - `17, 30, 39, 41, 100, 107`

Interpretation:

- the net regression is not driven by broad retrieval failure
- recalltrim preserved many off errors (`7, 8, 12, 20, 26, 36, 43, 47, 50, 114, 136`)
- the additional loss comes mainly from six new regressions:
  - temporal over-specificity:
    - `Q17`: `three weeks` -> `about four weeks`
  - missing required set members:
    - `Q30`: dropped `college` and `4 years`
  - overly cautious yes/location handling:
    - `Q39`: did not recover `UK`
    - `Q41`: did not convert `Smoky Mountains` into `Yes`
  - wrong topic substitution:
    - `Q100`: answered Thanksgiving movies instead of feast/thankfulness tradition
  - over-committing a specific city where gold expects a generic answer:
    - `Q107`: `a new city` -> `Edinburgh, Scotland`

Small-scale validation plan:

1. Recheck the six regression qids (`17, 30, 39, 41, 100, 107`) against retrieved memory snippets and local session traces, to separate retrieval miss from answer-selection drift.
2. Compare those six qids with their off counterparts, focusing on whether recalltrim changed:
   - the recalled memory set,
   - the prompt framing,
   - or only the final answer selection.
3. Run a micro-validation on these regression qids plus the three fixes (`21, 49, 137`) using the current framework only, to test whether the regression is concentrated in:
   - temporal normalization,
   - set completeness,
   - and generic-vs-specific answer selection.

### sample 4 micro-validation findings

Focused on the six recalltrim regressions:

- `Q17`
- `Q30`
- `Q39`
- `Q41`
- `Q100`
- `Q107`

Validated against:

- `off` CSV answer + judge reasoning
- `recalltrim` CSV answer + judge reasoning
- local QA session prompt/answer traces for `recalltrim`

Finding:

- the main failure mode is **answer-selection drift under recalltrim**, not broad retrieval collapse

Evidence by question:

1. `Q17` (`three weeks`)
- recalltrim prompt already contained both anchors:
  - `week before 2023-07-16`
  - `by 2023-08-09`
- off answered the relative interval directly: `About three weeks later.`
- recalltrim over-computed the calendar distance and answered `about four weeks later`
- diagnosis:
  - temporal over-normalization / over-specificity

2. `Q30` (`middle school, high school, college, 4 years`)
- recalltrim prompt already contained:
  - `middle school`
  - `high school`
  - `earned a college scholarship`
- but the answer dropped `college` and denied the `4 years`
- diagnosis:
  - set/member omission despite available evidence
  - likely answer compression / conservative selection, not missing retrieval

3. `Q39` (`UK`)
- recalltrim prompt did not surface the strongest UK-frequency evidence; it mostly showed Europe / NYC / Edinburgh / coastline snippets
- answer became `unspecified`
- off answered `The UK`
- diagnosis:
  - retrieval-quality drop on a country-frequency question

4. `Q41` (`Has Tim been to North Carolina and/or Tennessee? -> Yes`)
- recalltrim prompt did include `Smoky Mountains in summer 2022`
- but answer stayed at `doesn't specify whether North Carolina, Tennessee, or both`
- off converted the same evidence into `Yes`
- diagnosis:
  - conservative entailment failure
  - evidence was sufficient, but recalltrim answer did not bridge from location to yes/no conclusion

5. `Q100` (`Thanksgiving tradition`)
- recalltrim prompt explicitly contained:
  - `prepare a feast`
  - `talk about what they are thankful for`
  - `watch movies afterward`
- but answer selected only the movie tradition
- off kept the feast + thankful core and therefore passed
- diagnosis:
  - salient-but-secondary detail hijacked the answer
  - answer-selection drift, not retrieval miss

6. `Q107` (`a new city`)
- recalltrim prompt explicitly contained both:
  - gold-level generic fact: `planning ... to explore a new city`
  - suggestion detail: `Tim suggested Edinburgh, Scotland`
- off preserved both and centered the generic answer
- recalltrim over-committed to `Edinburgh, Scotland`
- diagnosis:
  - generic-vs-specific selection error

Micro-validation conclusion:

- among the six regressions:
  - retrieval drop is clear on `Q39`
  - the other five are primarily answer-selection / normalization failures with evidence already present in the prompt
- this means `sample4 recalltrim < off` is **not** mainly caused by recall failure alone
- the dominant regression pattern is:
  - over-specific temporal normalization
  - incomplete set extraction
  - failure to convert evidence into a yes/no conclusion
  - choosing a vivid secondary detail instead of the gold-level core answer

Recommended next minimal validation:

1. Add a tiny targeted evaluation slice containing only:
   - regressions: `17, 30, 39, 41, 100, 107`
   - fixes: `21, 49, 137`
2. For each item, diff:
   - retrieved memory block
   - final answer
   - judge result
3. Use that slice to decide whether the next change should target:
   - recall quality (`Q39`-like),
   - or answer-selection constraints (`Q17/Q30/Q41/Q100/Q107`-like).

### sample 4 fix-side findings

Validated the three qids that recalltrim fixed relative to off:

- `Q21`
- `Q49`
- `Q137`

Evidence:

1. `Q21` (`Which city was John in before traveling to Chicago? -> Seattle`)
- recalltrim prompt explicitly surfaced:
  - `Chicago by 2023-08-11`
  - `Seattle on 2023-07-16`
  - plus a distractor `New York City`
- off answered `New York City` and failed
- recalltrim selected the temporally correct predecessor city: `Seattle`
- benefit pattern:
  - better temporal anchoring across nearby city memories

2. `Q49` (`When did John get an ankle injury in 2023? -> around 2023-11-16`)
- recalltrim prompt surfaced both:
  - `last season before 2023-11-21` (coarser anchor)
  - `during the week of 2023-11-16` (finer anchor)
- off stayed at `last season`
- recalltrim selected the sharper date band around `2023-11-16`
- benefit pattern:
  - choosing the more specific temporal anchor when two relevant memories are present

3. `Q137` (`What did Tim say about his injury on 16 November, 2023?`)
- recalltrim prompt put the key sentence first:
  - `doctor said it was not too serious`
- off drifted into surrounding sympathy / encouragement context
- recalltrim kept the doctor-assessment core and passed
- benefit pattern:
  - better prioritization of the gold-bearing clause over nearby conversational context

Benefit-side conclusion:

- recalltrim helps when it:
  - surfaces the more precise temporal anchor among multiple nearby facts
  - keeps the answer tied to the most diagnostic clause instead of broader surrounding context

Combined interpretation from regressions + fixes:

- recalltrim is useful for:
  - sharper temporal disambiguation (`Q21`, `Q49`)
  - focusing on the highest-signal clause (`Q137`)
- recalltrim regresses when it:
  - over-normalizes time (`Q17`)
  - drops required set members (`Q30`)
  - refuses or fails to convert evidence into the needed yes/no or country answer (`Q39`, `Q41`)
  - over-selects vivid secondary details (`Q100`, `Q107`)

Refined next-step recommendation:

1. Keep the targeted 9-qid slice (`17, 21, 30, 39, 41, 49, 100, 107, 137`) as the minimal decision set.
2. For future prompt or recalltrim adjustments, optimize specifically for:
   - preserving set completeness,
   - preventing over-specific temporal arithmetic,
   - preferring gold-level core propositions over secondary colorful details,
   - while retaining the current gains in temporal anchor selection and clause prioritization.

### sample 1 / sample 2 log-based anomaly diagnosis

Log-level evidence first:

- no infrastructure-failure signals were found in the four main run logs:
  - no `http_500`
  - no `Traceback`
  - no gateway timeout / startup failure during the successful runs
- all four runs completed the expected ingest stage:
  - `sample1 off`: `sessions_ingested=19`, `qa_questions=81`
  - `sample1 recalltrim`: `sessions_ingested=19`, `qa_questions=81`
  - `sample2 off`: `sessions_ingested=19`, `qa_questions=85`
  - `sample2 recalltrim`: `sessions_ingested=19`, `qa_questions=85`
- token usage stayed in a normal benchmark range; there is no sign of early truncation or judge-path failure

This strongly suggests the anomalies are **not** caused by obvious framework crashes or service unavailability.

#### sample 1 (`conv-30`) anomaly shape

- `off = 16.05%`
- `recalltrim = 23.46%`
- both abnormal

Quantitative signatures:

- `sample1 off` response/reasoning mentions `2023-07-23` anchor `13` times
- `sample1 recalltrim` response/reasoning mentions `2023-07-23` anchor `15` times
- `sample1 off` contains `I don't have ... recalled memory` style responses on `15` qids
- `sample1 recalltrim` still contains that style on `10` qids

Observed failure pattern:

- many early questions collapse into one late-July time cluster
- multiple answers select clearly unrelated facts:
  - `Q1`: date question answered with `moved from Germany to pursue acting`
  - `Q4`: commonality question answered with `both moved from Sweden`
  - `Q3`: shared destress method answered as `cooking` instead of `dancing`
- recalltrim reduces some explicit `don't have` failures, but still preserves the same July-anchor collapse

Current best diagnosis for sample1:

- this does **not** look like a transient gateway/server failure
- it looks more like a **sample-specific retrieval/selection collapse**:
  - the recalled context is repeatedly dominated by a wrong late-July cluster
  - and the final answer selection frequently chooses unrelated but salient normalized facts

#### sample 2 (`conv-41`) anomaly shape

- `off = 17.65%`
- `recalltrim = 88.24%`

Quantitative signatures:

- `sample2 off` mentions the `2023-08-16` anchor `4` times
- `sample2 off` has `16` explicit `don't have`-style responses
- `sample2 recalltrim` has:
  - `0` `2023-08-16` anchor-collapse hits
  - `0` `don't have`-style responses

Observed failure pattern:

- `sample2 off` repeatedly drifts to an August 2023 cluster for earlier events:
  - car donation date
  - support-group join date
  - beach visit date
- it also frequently fails open with no recalled memory on fact/list questions
- but `sample2 recalltrim` removes both behaviors and returns to a healthy regime

Current best diagnosis for sample2:

- the `off` anomaly is unlikely to be caused by the dataset, judge, or global model-service state
- it is more consistent with an **off-path retrieval/context-construction failure**
- because once recalltrim changes the recalled context shape, the anomaly disappears almost completely

#### combined diagnosis

- `sample1` and `sample2 off` share one high-level symptom:
  - wrong temporal cluster dominates many answers
- but they are not identical:
  - `sample2` is largely recoverable by recalltrim
  - `sample1` remains abnormal even with recalltrim

Most likely explanation at this stage:

1. there is **no evidence of a broad infrastructure failure**
2. the dominant problem is **context selection / retrieval concentration around the wrong event cluster**
3. `sample1` additionally shows stronger **entity/fact substitution**, so it is a harder anomaly than `sample2`

### answer-selection optimization options

Based on the regression/fix analysis from `sample4` and the anomaly signatures from `sample1/2`, there are at least four optimization directions.

#### option A: tighten the QA answer-selection prompt

Add question-type-specific constraints before the final answer is produced:

- time questions:
  - prefer the stated relative/date expression from memory
  - do not derive a more specific calendar interval unless explicitly stated
- set/list questions:
  - include all named items that match the asked category
  - do not stop after the first plausible item if multiple supported items are present
- yes/no questions:
  - answer `Yes` / `No` first when the evidence supports entailment
  - put qualification after the decision, not instead of it
- generic-vs-specific questions:
  - if the gold-level memory says `a new city`, do not replace it with a suggested specific city unless the destination is finalized
- core-vs-secondary-detail questions:
  - prefer the proposition that directly answers the question over nearby colorful details

Why this is attractive:

- cheapest to try
- directly targets the observed drift classes
- unlikely to harm retrieval itself

#### option B: add a lightweight pre-answer fact extraction step

Before free-form answering, extract a tiny structured frame such as:

- `time_anchor`
- `named_items`
- `yes_no_entailment`
- `core_proposition`
- `secondary_details`

Then generate the final answer only from that frame.

Why this is attractive:

- separates retrieval from answer realization
- makes `Q17/Q30/Q41/Q100/Q107`-style failures easier to control

Risk:

- extra latency / token cost

#### option C: add post-answer sanity checks on a narrow slice

After the draft answer is produced, run a cheap rule check:

- if list question and prompt contains multiple same-category items but answer returns one item -> regenerate
- if yes/no question and answer lacks `yes/no` while evidence implies one -> regenerate
- if answer says `unspecified` but prompt contains a direct named candidate -> regenerate
- if generic question is answered with a suggestion detail instead of the generic target -> regenerate
- if temporal answer is derived by arithmetic from two dates rather than a stated expression -> prefer the stated expression

Why this is attractive:

- very targeted
- can be applied only to known risky question types

#### option D: improve retrieval diversity / anti-anchor concentration

Especially for `sample1/2` anomalies:

- penalize over-concentration on one date cluster
- require at least one cross-check memory when the top recalled snippets all come from the same narrow date window
- down-rank unrelated but salient normalized facts when they do not match the asked entity/relation

Why this is attractive:

- targets `sample1/2` directly

Risk:

- more intrusive than answer-selection fixes
- should be validated only after the 9-qid targeted slice

#### recommended order

1. try `option A + option C` first
2. if `Q39`-like failures remain, add `option D`
3. if prompt-only changes are unstable, consider `option B`

### sample 1 off retest launched

To check whether the `sample1 off` abnormal run is stable or state-dependent, a clean retest was started with a fresh isolated account/user and the same current benchmark framework:

| item | status |
|---|---|
| run_id | `off_sample1_cfg_20260606f` |
| mode | `off` |
| sample | `1` / `conv-30` |
| preprocess | `disabled` |
| isolation | fresh OpenClaw/OpenViking data + fresh account/user |

Early retest status:

- ingest progressed normally to `session 8/19`
- no `http_500`
- no `Traceback`
- no gateway startup failure

Interpretation so far:

- the retest has not yet proven whether `sample1 off` can recover to a non-abnormal result
- but it already weakens the theory that the original anomaly was caused by an immediate startup/runtime infrastructure fault

### sample 1 off retest final result

The clean retest for `sample1 off` finished with a non-abnormal result:

| scheme | run_id | valid | correct | accuracy | abnormal |
|---|---|---:|---:|---:|---|
| `off` | `off_sample1_cfg_20260606d` | `81` | `13` | `16.05%` | yes |
| `off` retest | `off_sample1_cfg_20260606f` | `81` | `62` | `76.54%` | no |

Retest token usage:

- input_tokens: not separately summarized in the quick retest check yet
- total_tokens: not separately summarized in the quick retest check yet

Key change in run shape:

- original abnormal run:
  - `2023-07-23` anchor collapse hits: `13`
  - `don't have` style responses: `15`
- retest:
  - `2023-07-23` anchor collapse hits: `2`
  - `don't have` style responses: `0`

Interpretation:

- `sample1 off` abnormality is **not stable**
- the original `16.05%` result should be treated as a **state-dependent abnormal run**, not the representative off baseline for `conv-30`
- the clean retest strongly weakens any claim that `sample1` is inherently off-abnormal under the current framework

### sample 2 off retest launched

Because `sample1 off` retest recovered to a healthy range without changing the benchmark core code, a matching clean retest was started for `sample2 off`:

| item | status |
|---|---|
| run_id | `off_sample2_cfg_20260606b2` |
| mode | `off` |
| sample | `2` / `conv-41` |
| preprocess | `disabled` |
| isolation | fresh OpenClaw/OpenViking data + fresh account/user |

Early retest status:

- process started normally
- ingest progressed to `session 5/19`
- no immediate infrastructure failure signal

Retest objective:

- check whether the original `sample2 off = 17.65%` abnormal run is also state-dependent
- specifically watch for:
  - `2023-08-16` anchor collapse
  - `don't have`-style QA answers

### sample 2 off retest final result

The clean retest for `sample2 off` also returned to a non-abnormal range:

| scheme | run_id | valid | correct | accuracy | abnormal |
|---|---|---:|---:|---:|---|
| `off` | `off_sample2_cfg_20260606a` | `85` | `15` | `17.65%` | yes |
| `off` retest | `off_sample2_cfg_20260606b2` | `85` | `70` | `82.35%` | no |

Retest signatures:

- original abnormal run:
  - `2023-08-16` anchor collapse hits: `4`
  - `don't have` style responses: `16`
- retest:
  - `2023-08-16` anchor collapse hits: `0`
  - `don't have` style responses: `0`

Representative remaining retest wrong qids:

- `3, 4, 6, 7, 9, 10, 18, 20, 24, 31, 67, 87, 96, 106, 108`

Interpretation:

- `sample2 off` abnormality is also **not stable**
- the original `17.65%` result should be treated as a **state-dependent abnormal run**, not the representative off baseline for `conv-41`
- the clean retest strongly supports the same conclusion already seen on `sample1 off`:
  - the low-accuracy abnormal runs were real,
  - but they are reproducibly recoverable without changing the benchmark core code

### anomaly retest conclusion for sample1 / sample2

After clean retests:

| sample | original off | retest off | abnormal after retest? |
|---|---:|---:|---|
| `sample1 / conv-30` | `16.05%` | `76.54%` | no |
| `sample2 / conv-41` | `17.65%` | `82.35%` | no |

Combined conclusion:

- both `sample1 off` and `sample2 off` abnormal runs are best interpreted as **state-dependent anomalies**
- there is no need to treat those original low accuracies as the stable representative baselines for the current framework
- this further reduces the likelihood of a persistent framework-core bug and strengthens the hypothesis that the original failures came from unstable run state / context construction state

## sample1-4 consolidated process and result record

This section consolidates the latest test process and final conclusions for
`sample1` through `sample4` under the current benchmark framework.

### process summary

Common execution method:

- all runs used the current `phase_a_off.py` benchmark path
- no benchmark core code changes were made for these comparisons
- `sample1` and `sample2` needed clean isolated retests because the original
  `off` runs were abnormal
- the clean retests used:
  - fresh OpenClaw/OpenViking data
  - fresh account/user
  - the same current framework
  - the existing external namespace-sync workaround with
    `--no-sync-plugin-config`
- `sample3` and `sample4` original `off` runs were already in a healthy range,
  so no extra `off` retest was required
- `sample1 recalltrim` was also re-run after its old abnormal result, and the
  clean retest recovered to a healthy range

Per-sample process notes:

| sample | process note |
|---|---|
| `sample1 / conv-30` | original `off` abnormal, clean `off` retest run; original `recalltrim` also abnormal, then clean `recalltrim` retest run |
| `sample2 / conv-41` | original `off` abnormal, clean `off` retest run; original `recalltrim` already healthy and kept as latest normal reference |
| `sample3 / conv-42` | original `off` healthy; compared directly against existing healthy `recalltrim` run |
| `sample4 / conv-43` | original `off` healthy; compared directly against existing healthy `recalltrim` run |

### latest normal result table

Latest normal `off` vs `recalltrim` comparison:

| sample | off run_id | off accuracy | recalltrim run_id | recalltrim accuracy | delta |
|---|---|---:|---|---:|---:|
| `sample1 / conv-30` | `off_sample1_cfg_20260606f` | `76.54%` | `on_sample1_recalltrim_20260607b` | `85.19%` | `+8.65%` |
| `sample2 / conv-41` | `off_sample2_cfg_20260606b2` | `82.35%` | `on_sample2_recalltrim_20260606a` | `88.24%` | `+5.89%` |
| `sample3 / conv-42` | `off_sample3_cfg_20260606a` | `75.73%` | `on_sample3_recalltrim_20260606a` | `84.47%` | `+8.74%` |
| `sample4 / conv-43` | `off_sample4_cfg_20260606a` | `87.72%` | `on_sample4_recalltrim_20260606a` | `85.09%` | `-2.63%` |

Token comparison for the same latest normal runs:

| sample | off QA token | recalltrim QA token | off full token | recalltrim full token |
|---|---:|---:|---:|---:|
| `sample1 / conv-30` | `571,581` | `567,950` | `911,175` | `887,254` |
| `sample2 / conv-41` | `585,750` | `593,781` | `984,372` | `948,819` |
| `sample3 / conv-42` | `748,463` | `729,079` | `1,073,516` | `1,133,947` |
| `sample4 / conv-43` | `794,584` | `796,065` | `1,211,811` | `1,211,463` |

Where:

- `QA token` = summed CSV `total_tokens`
- `full token` = `ingest token + QA token`

### sample conclusions

`sample1 / conv-30`

- the old `off = 16.05%` and old `recalltrim = 23.46%` were both abnormal
- after clean retests:
  - `off` recovered to `76.54%`
  - `recalltrim` recovered to `85.19%`
- conclusion:
  - both old low scores were state-dependent anomalies
  - under normal state, `recalltrim` is better than `off`

`sample2 / conv-41`

- the old `off = 17.65%` was abnormal
- clean `off` retest recovered to `82.35%`
- existing `recalltrim = 88.24%` stayed healthy
- conclusion:
  - the old `off` low score was a state-dependent anomaly
  - under normal state, `recalltrim` is better than `off`

`sample3 / conv-42`

- `off = 75.73%` was already healthy
- `recalltrim = 84.47%` improved accuracy further
- conclusion:
  - no abnormal baseline issue was observed
  - `recalltrim` provides a clear positive gain on this sample

`sample4 / conv-43`

- `off = 87.72%` was already healthy
- `recalltrim = 85.09%` regressed slightly
- conclusion:
  - this sample remains the main negative case for current `recalltrim`
  - the dominant issue is answer-selection drift rather than broad retrieval failure

### overall conclusion for sample1-4

- the original low `off` scores on `sample1` and `sample2` were not stable
- after isolating run state and re-running, all four samples now have healthy
  `off` baselines
- under the latest normal data:
  - `recalltrim` wins on `sample1`
  - `recalltrim` wins on `sample2`
  - `recalltrim` wins on `sample3`
  - `recalltrim` loses on `sample4`
- therefore, the current practical conclusion is:
  - `recalltrim` is generally beneficial on the expanded sample set
  - but it still needs targeted control for answer-selection drift, especially
    on `sample4`

## remaining sample kickoff after sample0-4

Objective extension:

- continue from the completed `sample0-4` coverage
- run the remaining LoCoMo samples under the same current benchmark code
- keep the process clean and traceable
- record `model` explicitly in addition to the earlier fields

Remaining sample scope:

| sample index | sample id |
|---:|---|
| `5` | `conv-44` |
| `6` | `conv-47` |
| `7` | `conv-48` |
| `8` | `conv-49` |
| `9` | `conv-50` |

Execution contract for the remaining samples:

- benchmark path: current `phase_a_off.py`
- clean run requirement:
  - backup OpenViking original data
  - clear OpenViking/OpenClaw run state
  - rebuild through the existing clean run script flow
  - isolate account/user state between runs
- comparison modes:
  - `off`
  - `recalltrim`
- abnormal rule:
  - accuracy `< 50%` is treated as abnormal
- model:
  - `volcengine/doubao-seed-2.0-pro`

### sample 5 off launch

Started the first remaining-sample clean baseline run:

| item | value |
|---|---|
| sample | `5` / `conv-44` |
| mode | `off` |
| model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |
| run method | existing clean container script + isolated account/user + current benchmark code |

Early status:

- run_id: `off_sample5_cfg_20260607a`
- `wm_v2_preprocess_enabled = false`
- OpenViking health recovered normally
- gateway health recovered normally
- ingest progressed to `session 3/19`
- no immediate `http_500`
- no `Traceback`

Mid-run status:

- ingest progressed further to `session 15/19`
- no CSV output yet, so QA had not started at that snapshot
- no infrastructure-level failure signal was observed during the ingest phase

QA start snapshot:

- ingest reached `19/19`
- QA CSV started writing and reached `Q9`
- early answer shape is mixed:
  - some direct temporal answers are present
  - at least one early answer already used an `I don't see any recalled memory...`
    style response
- judge had not started at that snapshot, so abnormality could not yet be
  concluded

Deeper QA snapshot:

- QA progressed to `Q44`
- judge still had not started at that snapshot
- `don't have` / `not specified` style answers had started to appear, but they
  were not yet dense enough to conclude an abnormal run without the final score

Later QA snapshot:

- QA progressed further to `Q91`
- judge still had not started at that snapshot
- `don't have` / `not specified` style answers did not explode in frequency by
  that point, but they were still present and worth tracking
- final abnormal vs normal judgment still depended on the eventual score

### sample 5 off final result

Final clean `off` result for `sample5 / conv-44`:

| item | value |
|---|---|
| sample | `5` / `conv-44` |
| mode | `off` |
| run_id | `off_sample5_cfg_20260607a` |
| model label | `openclaw` |
| provider model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |
| valid | `66` |
| correct | `51` |
| accuracy | `77.27%` |
| abnormal | no |
| QA token | `475,505` |
| full token | `799,788` |
| re-run | no |

Observed shape:

- `don't have` / `not specified` style hits: about `7`
- despite those misses, the final score stayed in a healthy range

Interpretation:

- `sample5 off` is not abnormal
- this sample currently behaves more like `sample3/4 off` than the earlier
  abnormal `sample1/2 off` runs

### sample 5 recalltrim launch

Started the matching clean `recalltrim` run for direct comparison:

| item | value |
|---|---|
| sample | `5` / `conv-44` |
| mode | `recalltrim` |
| model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |

Early status:

- run_id: `on_sample5_recalltrim_20260607a`
- `wm_v2_preprocess_enabled = true`
- OpenViking health recovered normally
- gateway health recovered normally
- ingest progressed to `session 3/19`
- no immediate `http_500`
- no `Traceback`

Mid-run status:

- ingest later progressed to at least `session 15/19`
- QA then started and reached `Q7`
- early answer shape looked mostly normal, with only a small number of
  `don’t explicitly state` style responses at that point

Deeper QA snapshot:

- QA later progressed to `Q28`, then to `Q71`
- judge still had not started at those snapshots
- `don't have` / `not specified` style answers had increased to roughly the
  same order as `sample5 off` by that stage, so the final score remained the
  decisive criterion

### sample 5 recalltrim final result

Final clean `recalltrim` result for `sample5 / conv-44`:

| item | value |
|---|---|
| sample | `5` / `conv-44` |
| mode | `recalltrim` |
| run_id | `on_sample5_recalltrim_20260607a` |
| model label | `openclaw` |
| provider model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |
| valid | `66` |
| correct | `49` |
| accuracy | `74.24%` |
| abnormal | no |
| QA token | `472,056` |
| full token | `792,604` |
| re-run | no |

Observed shape:

- `don't have` / `not specified` style hits: about `9`
- answer style was slightly more conservative than the paired `off` run

### sample 5 final comparison

| mode | run_id | accuracy | QA token | full token | abnormal |
|---|---|---:|---:|---:|---|
| `off` | `off_sample5_cfg_20260607a` | `77.27%` | `475,505` | `799,788` | no |
| `recalltrim` | `on_sample5_recalltrim_20260607a` | `74.24%` | `472,056` | `792,604` | no |

Interpretation:

- `sample5` is healthy in both modes
- `recalltrim` is slightly worse than `off` on accuracy for this sample
- `recalltrim` is slightly cheaper than `off` on both QA token and full token

### sample 6 off launch

Started the next remaining-sample clean baseline run:

| item | value |
|---|---|
| sample | `6` / `conv-47` |
| mode | `off` |
| model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |

Early status:

- run_id: `off_sample6_cfg_20260607a`
- `wm_v2_preprocess_enabled = false`
- OpenViking health recovered normally
- gateway health recovered normally
- ingest started normally and reached `session 1/19`
- no immediate `http_500`
- no `Traceback`

Mid-run status:

- ingest later progressed to `session 13/19`
- no CSV output yet at that snapshot, so QA had not started
- no infrastructure-level failure signal was observed during ingest

## Completion note 2026-06-08

This log file contains earlier out-of-order append history. The authoritative
final sections for the completed goal are:

- `## Final remaining-sample comparison`
- `## Abnormal / rerun summary`
- `## Overall conclusion`
- `## Recalltrim optimization opportunities`
- `## oc ingest retest`

Final state:

- remaining samples `5-9` all completed in both `off` and `recalltrim`
- no counted benchmark run was below the `<50%` abnormal threshold
- `off_sample8_cfg_20260608a` was a non-counted launcher failure before
  benchmark entry; replacement `off_sample8_cfg_20260608b` completed normally
- temporary SSH/network observation issue recovered; completed artifacts do not
  show benchmark corruption from that issue
- `oc ingest` retests for `small`, `sample0`, and `sample1` all passed cleanly
- final conclusion: recalltrim is mixed and net negative over remaining samples
  `5-9`, so this evidence supports recalltrim algorithm tuning rather than
  benchmark core-code changes

### remote connectivity recovery check

After a temporary SSH observation failure, rechecked the remote environment before
continuing the benchmark queue:

| item | value |
|---|---|
| host | `123.60.114.206:10008` |
| container | `jcp-dev` |
| host status | reachable |
| container status | up |
| gateway health | `{"ok":true,"status":"live"}` |
| OpenViking health | `{"status":"ok","healthy":true,"version":"0.3.18.dev76","auth_mode":"api_key"}` |

Interpretation:

- the observed network problem affected the SSH observation path
- the benchmark service stack was healthy at recheck time
- `sample7 recalltrim` had completed and produced normal artifacts before the
  recheck, so there is no evidence from this run that the temporary SSH issue
  corrupted benchmark output

### sample 7 recalltrim final result

Final clean `recalltrim` result for `sample7 / conv-48`:

| item | value |
|---|---|
| sample | `7` / `conv-48` |
| mode | `recalltrim` |
| run_id | `on_sample7_recalltrim_20260607a` |
| model label | `openclaw` |
| provider model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |
| valid | `117` |
| correct | `98` |
| accuracy | `83.76%` |
| abnormal | no |
| QA token | `815,091` |
| full token | `1,092,317` |
| re-run | no |

Observed shape:

- `don't have` / `not specified` style hits: `0` by exact phrase scan
- final score is above the `< 50%` abnormal threshold
- answer shape did not show the broad refusal/unknown collapse seen in earlier
  abnormal runs

### sample 7 off vs recalltrim comparison

| sample | mode | run_id | accuracy | QA token | full token | abnormal | re-run |
|---|---|---|---:|---:|---:|---|---|
| `7 / conv-48` | `off` | `off_sample7_cfg_20260607a` | `82.05%` | `816,670` | `1,159,245` | no | no |
| `7 / conv-48` | `recalltrim` | `on_sample7_recalltrim_20260607a` | `83.76%` | `815,091` | `1,092,317` | no | no |

Interpretation:

- both runs are healthy
- recalltrim is slightly better on accuracy for this sample
- recalltrim is also cheaper on full token usage in this run
- this contrasts with `sample5` and `sample6`, so recalltrim impact is not
  uniformly positive or negative across samples

### sample 8 off launch

Started clean `off` run:

| item | value |
|---|---|
| sample | `8` / `conv-49` |
| mode | `off` |
| run_id | `off_sample8_cfg_20260608a` |
| model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |
| sessions | `1-19` |
| skip judge | `false` |
| isolate user scope by agent | `false` |
| isolate agent scope by user | `false` |

Early status:

- backup/reset completed
- `wm_v2_preprocess_enabled = true`
- OpenViking health recovered normally
- gateway health recovered normally
- ingest started and reached `session 7/19`
- no immediate `http_500`
- no benchmark traceback observed

Mid status:

- ingest completed `19/19`
- no CSV yet at that snapshot, so QA had not started or had not written rows
- no infrastructure-level failure signal observed during ingest

QA start status:

- QA CSV started and reached `Q18`
- judge had not started writing `result` values at that snapshot
- `don't have` / `not specified` style hits: about `2`
- early rows include some "no relevant information" answers, so this run should
  be watched for possible conservative-answer drift

Later QA / judge status:

- QA progressed through `Q75` and `Q93`, then completed all `78` rows
- unknown / not-enough-information style hits increased to about `6`
- judge started after QA completion and then produced final meta

### sample 9 recalltrim final result

Final clean `recalltrim` result for `sample9 / conv-50`:

| item | value |
|---|---|
| sample | `9` / `conv-50` |
| mode | `recalltrim` |
| run_id | `on_sample9_recalltrim_20260608a` |
| model label | `openclaw` |
| provider model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |
| valid | `78` |
| correct | `58` |
| accuracy | `74.36%` |
| abnormal | no |
| QA token | `650,778` |
| full token | `979,606` |
| re-run | no |

Observed shape:

- `don't have` / `not specified` / no-relevant-information style hits: about `6`
- final score is above the `< 50%` abnormal threshold
- this is a normal but degraded recalltrim run relative to off

### sample 9 off vs recalltrim comparison

| sample | mode | run_id | accuracy | QA token | full token | abnormal | re-run |
|---|---|---|---:|---:|---:|---|---|
| `9 / conv-50` | `off` | `off_sample9_cfg_20260608a` | `85.90%` | `549,797` | `813,112` | no | no |
| `9 / conv-50` | `recalltrim` | `on_sample9_recalltrim_20260608a` | `74.36%` | `650,778` | `979,606` | no | no |

Interpretation:

- both runs are above the abnormal threshold
- recalltrim is clearly worse on this sample, both in accuracy and token usage
- the increased unknown/no-relevant-information answers are a concrete signal
  that recalltrim can under-supply or over-filter useful context for some
  questions

## `oc ingest` retest

After finishing the remaining sample benchmark runs, started `oc ingest`
retests to check whether the earlier conclusion changed under the current code
and recovered network conditions.

Run id:

- `oc_ingest_retest_20260608a`

Retest definition:

- path: `benchmark/locomo/openclaw/eval.py ingest`
- gateway: `http://127.0.0.1:18789`
- data: `/home/jcp/agent/code/locomo-test-kit/data/locomo10.json`
- mode: gateway / OpenClaw ingest path
- preprocessor: `wm_v2_preprocess_enabled = false`
- clean per case: yes; backup, clear OpenViking data, clear OpenClaw
  `locomo-eval` sessions/state, restart OpenViking and gateway

Cases:

| case | sample | sessions | purpose |
|---|---:|---|---|
| `small` | `0 / conv-26` | `1-4` | small gateway ingest smoke |
| `sample0` | `0 / conv-26` | `1-19` | full sample0 gateway ingest |
| `sample1` | `1 / conv-30` | `1-19` | full sample1 gateway ingest |

### `oc ingest` small result

Clean gateway ingest result:

| item | value |
|---|---|
| case | `small` |
| sample | `0 / conv-26` |
| sessions | `1-4` |
| exit | `0` |
| completed sessions | `4/4` |
| skipped | `0` |
| output | `/tmp/oc_ingest_retest_20260608a_small.out` |
| json output | `/tmp/oc_ingest_retest_20260608a_small.out.json` |

Observed shape:

- all four sessions returned a completed gateway response
- each compact call returned `status=ok`
- one session returned `safe\n\n！` instead of the usual `stored`, but the compact
  still completed and the ingest summary counted the session as completed

Interpretation:

- current code/network can complete the small `oc ingest` smoke case

### `oc ingest` sample0 result

Clean gateway ingest result:

| item | value |
|---|---|
| case | `sample0` |
| sample | `0 / conv-26` |
| sessions | `1-19` |
| exit | `0` |
| completed sessions | `19/19` |
| skipped | `0` |
| output | `/tmp/oc_ingest_retest_20260608a_sample0.out` |
| json output | `/tmp/oc_ingest_retest_20260608a_sample0.out.json` |

Observed shape:

- all nineteen sessions returned completed gateway responses
- every compact call returned `status=ok`
- response text varied between `stored` and `stored.`, but no ingest failure
  occurred

Interpretation:

- current code/network can complete full `sample0` through the OpenClaw gateway
  ingest path
- this is stronger than the earlier cautious guidance that gateway ingest was
  only suitable for small `<10` session cases

### `oc ingest` sample1 result

Clean gateway ingest result:

| item | value |
|---|---|
| case | `sample1` |
| sample | `1 / conv-30` |
| sessions | `1-19` |
| exit | `0` |
| completed sessions | `19/19` |
| skipped | `0` |
| output | `/tmp/oc_ingest_retest_20260608a_sample1.out` |
| json output | `/tmp/oc_ingest_retest_20260608a_sample1.out.json` |

Observed shape:

- all nineteen sessions returned completed gateway responses
- every compact call returned `status=ok`
- response text varied between `stored`, `stored.`, and one longer
  "I'll store this conversation..." response, but no ingest failure occurred

Interpretation:

- current code/network can complete full `sample1` through the OpenClaw gateway
  ingest path
- together with `sample0`, this changes the earlier cautious conclusion: gateway
  ingest is not currently limited to the small `<10` session case in this
  environment

### `oc ingest` retest summary

| case | sample | sessions | exit | completed | compact status | conclusion |
|---|---:|---|---:|---:|---|---|
| `small` | `0 / conv-26` | `1-4` | `0` | `4/4` | all `ok` | pass |
| `sample0` | `0 / conv-26` | `1-19` | `0` | `19/19` | all `ok` | pass |
| `sample1` | `1 / conv-30` | `1-19` | `0` | `19/19` | all `ok` | pass |

Updated `oc ingest` conclusion:

- the previous network issue does not reproduce in these clean retests
- the current gateway ingest path successfully handles `small`, full `sample0`,
  and full `sample1`
- the earlier guidance that gateway ingest should be kept to small cases should
  be softened: it was a cautious workaround under earlier instability, not a
  current proven limitation

## Final remaining-sample comparison

### Accuracy

| sample | off run_id | off accuracy | recalltrim run_id | recalltrim accuracy | delta | abnormal? |
|---|---|---:|---|---:|---:|---|
| `5 / conv-44` | `off_sample5_cfg_20260607a` | `77.27%` | `on_sample5_recalltrim_20260607a` | `74.24%` | `-3.03%` | no |
| `6 / conv-47` | `off_sample6_cfg_20260607a` | `90.70%` | `on_sample6_recalltrim_20260607a` | `82.56%` | `-8.14%` | no |
| `7 / conv-48` | `off_sample7_cfg_20260607a` | `82.05%` | `on_sample7_recalltrim_20260607a` | `83.76%` | `+1.71%` | no |
| `8 / conv-49` | `off_sample8_cfg_20260608b` | `83.18%` | `on_sample8_recalltrim_20260608a` | `84.11%` | `+0.93%` | no |
| `9 / conv-50` | `off_sample9_cfg_20260608a` | `85.90%` | `on_sample9_recalltrim_20260608a` | `74.36%` | `-11.54%` | no |

Aggregate over remaining samples `5-9`:

| metric | off | recalltrim | delta |
|---|---:|---:|---:|
| mean accuracy | `83.82%` | `79.81%` | `-4.01%` |
| wins | `3` | `2` | - |
| abnormal runs `<50%` | `0` | `0` | - |

### Token Usage

| sample | off QA token | recalltrim QA token | off full token | recalltrim full token |
|---|---:|---:|---:|---:|
| `5 / conv-44` | `475,505` | `472,056` | `799,788` | `792,604` |
| `6 / conv-47` | `609,140` | `605,704` | `1,019,965` | `990,218` |
| `7 / conv-48` | `816,670` | `815,091` | `1,159,245` | `1,092,317` |
| `8 / conv-49` | `764,813` | `766,748` | `1,057,373` | `1,052,787` |
| `9 / conv-50` | `549,797` | `650,778` | `813,112` | `979,606` |
| **total** | `3,215,925` | `3,310,377` | `4,849,483` | `4,907,532` |

Token conclusion:

- recalltrim saved full tokens on samples `5-8`
- sample `9` reversed that trend sharply and made the remaining-sample total
  slightly more expensive for recalltrim
- token savings are therefore not stable enough to claim as a general result
  from samples `5-9`

## Abnormal / rerun summary

| item | result |
|---|---|
| benchmark runs below `<50%` | none in samples `5-9` |
| clean reruns needed for low accuracy | none in samples `5-9` |
| non-counted failed launcher | `off_sample8_cfg_20260608a`, failed before benchmark entry due `bash -lc` / gvm `GVM_DEBUG` shell init issue |
| temporary SSH observation issue | recovered; no evidence that it corrupted completed benchmark artifacts |

## Overall conclusion

For remaining samples `5-9`, recalltrim is mixed and net negative on mean
accuracy:

- positive on `sample7` and `sample8`
- negative on `sample5`, `sample6`, and especially `sample9`
- no run crossed the `<50%` abnormal threshold
- the most important negative signal is not a broad infrastructure failure, but
  recalltrim answer-selection/context-selection drift on some samples

This does not justify changing benchmark core code. The evidence supports
further recalltrim algorithm work rather than benchmark-framework changes.

## Recalltrim optimization opportunities

1. Add an uncertainty fallback: when the answer generator starts producing
   repeated "not specified" / "no relevant information" answers, widen context
   or retry with less aggressive trimming before finalizing the answer.

2. Improve span selection for partial-evidence questions: several degraded cases
   look like recalltrim had some relevant context but not enough adjacent turns
   to answer causal, planning, or relationship questions.

3. Add per-question token guardrails: `sample9` shows recalltrim can spend more
   tokens while answering worse, so the system should detect expensive low-signal
   context expansion and either rerank or fallback.

4. Strengthen anchor preservation: keep date/person/activity anchors together
   with neighboring explanation turns, instead of selecting isolated snippets
   that lead to under-specified or conservative answers.

5. Track unknown-answer rate as a first-class run metric: the phrase-count signal
   correlated with degraded recalltrim runs on `sample6` and `sample9`, and is
   useful as an early warning even when accuracy remains above abnormal threshold.

Early status:

- backup/reset completed
- `wm_v2_preprocess_enabled = false`
- OpenViking health recovered normally
- gateway health recovered normally
- ingest started and reached `session 7/19`
- no immediate `http_500`
- no benchmark traceback observed

QA start status:

- QA CSV started and reached `Q8`
- judge had not started writing `result` values at that snapshot
- `don't have` / `not specified` style hits: `0` by phrase scan, with one
  nearby "don't say whether" answer observed manually
- no broad refusal/unknown collapse observed in early rows

Later QA / judge status:

- QA progressed to `Q73` and `Q99`
- judge then started and reached `45/78` judged rows with interim accuracy
  `84.44%`
- `don't have` / `not specified` style hits stayed low, about `2`

### sample 9 off final result

Final clean `off` result for `sample9 / conv-50`:

| item | value |
|---|---|
| sample | `9` / `conv-50` |
| mode | `off` |
| run_id | `off_sample9_cfg_20260608a` |
| model label | `openclaw` |
| provider model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |
| valid | `78` |
| correct | `67` |
| accuracy | `85.90%` |
| abnormal | no |
| QA token | `549,797` |
| full token | `813,112` |
| re-run | no |

Observed shape:

- `don't have` / `not specified` style hits: about `2`
- final score is above the `< 50%` abnormal threshold
- no network or service error was observed in the final artifact path

### sample 9 recalltrim launch

Started matching clean `recalltrim` run:

| item | value |
|---|---|
| sample | `9` / `conv-50` |
| mode | `recalltrim` |
| run_id | `on_sample9_recalltrim_20260608a` |
| model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |
| sessions | `1-19` |
| skip judge | `false` |
| isolate user scope by agent | `false` |
| isolate agent scope by user | `false` |

Early status:

- backup/reset completed
- `wm_v2_preprocess_enabled = true`
- OpenViking health recovered normally
- gateway health recovered normally
- ingest started and reached `session 6/19`
- no immediate `http_500`
- no benchmark traceback observed

Mid status:

- ingest completed `19/19`
- no CSV yet at that snapshot, so QA had not started or had not written rows
- no infrastructure-level failure signal observed during ingest

QA start status:

- QA CSV started and reached `Q28`
- judge had not started writing `result` values at that snapshot
- `don't have` / `not specified` style hits: `0` by phrase scan, with one
  nearby "don't explicitly mention" answer observed manually
- no broad refusal/unknown collapse observed in early rows

Later QA status:

- QA progressed through `Q64`, `Q102`, and `Q128`
- judge still had not started during those snapshots
- `don't have` / `not specified` style hits stayed low, about `2`
- no fixed-question stall or infrastructure error was observed

### sample 8 recalltrim final result

Final clean `recalltrim` result for `sample8 / conv-49`:

| item | value |
|---|---|
| sample | `8` / `conv-49` |
| mode | `recalltrim` |
| run_id | `on_sample8_recalltrim_20260608a` |
| model label | `openclaw` |
| provider model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |
| valid | `107` |
| correct | `90` |
| accuracy | `84.11%` |
| abnormal | no |
| QA token | `766,748` |
| full token | `1,052,787` |
| re-run | no |

Observed shape:

- `don't have` / `not specified` style hits: about `2`
- final score is above the `< 50%` abnormal threshold
- answer shape stayed normal through the run

### sample 8 off vs recalltrim comparison

| sample | mode | run_id | accuracy | QA token | full token | abnormal | re-run |
|---|---|---|---:|---:|---:|---|---|
| `8 / conv-49` | `off` | `off_sample8_cfg_20260608b` | `83.18%` | `764,813` | `1,057,373` | no | no |
| `8 / conv-49` | `recalltrim` | `on_sample8_recalltrim_20260608a` | `84.11%` | `766,748` | `1,052,787` | no | no |

Interpretation:

- both runs are healthy
- recalltrim is slightly better on accuracy for this sample
- recalltrim is slightly cheaper on full token usage, while QA token is slightly
  higher

### sample 9 off launch

Started clean `off` run:

| item | value |
|---|---|
| sample | `9` / `conv-50` |
| mode | `off` |
| run_id | `off_sample9_cfg_20260608a` |
| model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |
| sessions | `1-19` |
| skip judge | `false` |
| isolate user scope by agent | `false` |
| isolate agent scope by user | `false` |

Purpose:

- continue remaining-sample clean comparison after confirming the temporary SSH
  observation problem had recovered

Launch note:

- `off_sample8_cfg_20260608a` did not enter the benchmark script because the
  launcher used `bash -lc` and hit a container gvm shell initialization error:
  `GVM_DEBUG: unbound variable`
- no benchmark result is counted for `off_sample8_cfg_20260608a`
- relaunched as `off_sample8_cfg_20260608b` with non-login bash

Replacement run:

| item | value |
|---|---|
| sample | `8` / `conv-49` |
| mode | `off` |
| run_id | `off_sample8_cfg_20260608b` |
| clean | yes |
| status | started |

Early status:

- backup/reset completed
- `wm_v2_preprocess_enabled = false`
- OpenViking health recovered normally
- gateway health recovered normally
- ingest started and reached `session 6/19`
- no immediate `http_500`
- no benchmark traceback observed

Mid status:

- ingest completed `19/19`
- QA CSV started and reached `Q21`
- judge had not started writing `result` values at that snapshot
- `don't have` / `not specified` style hits: `0`
- answer shape looked normal in the early QA rows

Later QA status:

- QA progressed through `Q52`, `Q86`, `Q105`, and `Q130`
- judge still had not started during those snapshots
- `don't have` / `not specified` style hits stayed low, about `2`
- no fixed-question stall or infrastructure error was observed

### sample 8 off final result

Final clean `off` result for `sample8 / conv-49`:

| item | value |
|---|---|
| sample | `8` / `conv-49` |
| mode | `off` |
| run_id | `off_sample8_cfg_20260608b` |
| model label | `openclaw` |
| provider model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |
| valid | `107` |
| correct | `89` |
| accuracy | `83.18%` |
| abnormal | no |
| QA token | `764,813` |
| full token | `1,057,373` |
| re-run | no |

Observed shape:

- `don't have` / `not specified` style hits: about `2`
- final score is above the `< 50%` abnormal threshold
- no evidence that the earlier SSH observation issue affected this run

### sample 8 recalltrim launch

Started matching clean `recalltrim` run:

| item | value |
|---|---|
| sample | `8` / `conv-49` |
| mode | `recalltrim` |
| run_id | `on_sample8_recalltrim_20260608a` |
| model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |
| sessions | `1-19` |
| skip judge | `false` |
| isolate user scope by agent | `false` |
| isolate agent scope by user | `false` |

Later ingest snapshot:

- ingest progressed further to at least `session 18/19`
- no infrastructure-level failure signal was observed before QA start

QA start snapshot:

- QA CSV started writing and reached `Q8`
- early answers already showed multiple `don’t explicitly` / `not explicitly`
  style responses
- judge had not started at that snapshot

Deeper QA snapshot:

- QA later progressed to `Q31`
- `don't have` / `not explicitly` / `not specified` style responses had
  already accumulated to a visibly higher level than the paired `sample6 off`
  run
- this is a concrete candidate optimization direction for `recalltrim`:
  it is sometimes too conservative and declines to commit even when adjacent
  evidence appears sufficient

Later QA snapshot:

- QA later progressed to `Q85`
- the conservative answer pattern remained present, but did not appear to
  explode further by that point
- final comparison against `sample6 off` still depends on the judge score

### sample 6 recalltrim final result

Final clean `recalltrim` result for `sample6 / conv-47`:

| item | value |
|---|---|
| sample | `6` / `conv-47` |
| mode | `recalltrim` |
| run_id | `on_sample6_recalltrim_20260607a` |
| model label | `openclaw` |
| provider model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |
| valid | `86` |
| correct | `71` |
| accuracy | `82.56%` |
| abnormal | no |
| QA token | `605,704` |
| full token | `990,218` |
| re-run | no |

Observed shape:

- `don't have` / `not explicitly` / `not specified` style hits: about `7`
- answer style remained noticeably more conservative than the paired `off` run

### sample 6 final comparison

| mode | run_id | accuracy | QA token | full token | abnormal |
|---|---|---:|---:|---:|---|
| `off` | `off_sample6_cfg_20260607a` | `90.70%` | `609,140` | `1,019,965` | no |
| `recalltrim` | `on_sample6_recalltrim_20260607a` | `82.56%` | `605,704` | `990,218` | no |

Interpretation:

- `sample6` is healthy in both modes
- `recalltrim` is clearly worse than `off` on accuracy for this sample
- `recalltrim` is slightly cheaper on both QA token and full token
- this sample reinforces a recurring optimization direction:
  current `recalltrim` is sometimes too conservative and over-prefers
  `not explicitly mentioned` style answers

### sample 7 off launch

Started the next remaining-sample clean baseline run:

| item | value |
|---|---|
| sample | `7` / `conv-48` |
| mode | `off` |
| model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |

Early status:

- run_id: `off_sample7_cfg_20260607a`
- `wm_v2_preprocess_enabled = false`
- OpenViking health recovered normally
- gateway health recovered normally
- ingest started normally and reached `session 1/19`
- no immediate `http_500`
- no `Traceback`

Mid-run status:

- ingest later completed `19/19`
- QA then started and reached `Q17`
- early answers were direct and, at that snapshot, did not show obvious
  `don't have` / `not explicitly` style collapse

Deeper QA snapshot:

- QA later progressed to `Q49`
- `don't have` / `not specified` style answers had appeared a few times by
  then, but were still sparse
- judge had not started at that snapshot

Late QA snapshot:

- QA later progressed to `Q143`, effectively near completion
- `don't have` / `not specified` style answers still remained sparse
- the run shape stayed healthy through the late QA stage; only the formal
  judge result remained pending

### sample 7 off final result

Final clean `off` result for `sample7 / conv-48`:

| item | value |
|---|---|
| sample | `7` / `conv-48` |
| mode | `off` |
| run_id | `off_sample7_cfg_20260607a` |
| model label | `openclaw` |
| provider model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |
| valid | `117` |
| correct | `96` |
| accuracy | `82.05%` |
| abnormal | no |
| QA token | `816,670` |
| full token | `1,159,245` |
| re-run | no |

Observed shape:

- `don't have` / `not specified` style hits: about `4`
- no visible abnormal collapse signal during QA

Interpretation:

- `sample7 off` is healthy
- this sample also does not show the earlier low-accuracy anomaly pattern

### sample 7 recalltrim launch

Started the matching clean `recalltrim` run for direct comparison:

| item | value |
|---|---|
| sample | `7` / `conv-48` |
| mode | `recalltrim` |
| model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |

Early status:

- run_id: `on_sample7_recalltrim_20260607a`
- `wm_v2_preprocess_enabled = true`
- OpenViking health recovered normally
- gateway health recovered normally
- ingest started normally and reached `session 1/19`
- no immediate `http_500`
- no `Traceback`

Mid-run status:

- ingest later progressed to at least `session 14/19`
- QA then started and reached `Q2`
- early answers at that point looked normal and had not yet shown a clear
  conservative `don't have` pattern

Deeper QA snapshot:

- QA later progressed to `Q35`
- only a small number of `don't have` / `lack evidence` style responses had
  appeared by that stage
- relative to `sample6 recalltrim`, the early-to-mid recalltrim answer shape
  on `sample7` looked healthier

QA start snapshot:

- ingest completed `19/19`
- QA CSV started writing and reached `Q14`
- early answers were mostly direct, with a small amount of `don’t have`
  phrasing
- judge had not started at that snapshot

Deeper QA snapshots:

- QA later progressed to `Q37`, then to `Q79`
- judge still had not started at those snapshots
- `don't have` / `not specified` style answers remained sparse by then, so the
  run shape looked much healthier than the earlier clearly abnormal cases

### sample 6 off final result

Final clean `off` result for `sample6 / conv-47`:

| item | value |
|---|---|
| sample | `6` / `conv-47` |
| mode | `off` |
| run_id | `off_sample6_cfg_20260607a` |
| model label | `openclaw` |
| provider model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |
| valid | `86` |
| correct | `78` |
| accuracy | `90.70%` |
| abnormal | no |
| QA token | `609,140` |
| full token | `1,019,965` |
| re-run | no |

Observed shape:

- `don't have` / `not specified` style hits: about `3`
- no visible abnormal collapse signal during QA

Interpretation:

- `sample6 off` is clearly healthy
- this sample is not showing the earlier low-accuracy anomaly pattern

### sample 6 recalltrim launch

Started the matching clean `recalltrim` run for direct comparison:

| item | value |
|---|---|
| sample | `6` / `conv-47` |
| mode | `recalltrim` |
| model | `volcengine/doubao-seed-2.0-pro` |
| clean | yes |

Early status:

- run_id: `on_sample6_recalltrim_20260607a`
- `wm_v2_preprocess_enabled = true`
- OpenViking health recovered normally
- gateway health recovered normally
- ingest started normally and reached `session 1/19`
- no immediate `http_500`
- no `Traceback`

Mid-run status:

- ingest later progressed to `session 13/19`
- no CSV output yet at that snapshot, so QA had not started
- no infrastructure-level failure signal was observed during ingest
## Final EOF status 2026-06-08

- Remaining samples `5-9` completed for both `off` and `recalltrim`.
- `oc ingest` retests for `small`, `sample0`, and `sample1` completed successfully.
- See `## Final remaining-sample comparison` and `## Recalltrim optimization opportunities` above for the authoritative tables and conclusions.
