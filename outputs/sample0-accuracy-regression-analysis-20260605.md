# LoCoMo sample0 Accuracy Regression Analysis 2026-06-05

## Scope

- objective:
  - analyze why two `on` implementations regress on LoCoMo `sample0`
  - keep focus on memory content and QA content
  - do not change recall behavior
  - avoid large changes to the memory-extraction core

## Compared runs

### Off baseline

- `off_sample_cfg_20260604_0112`
- `114 / 36`
- `76.00%`

### 8b310ad4

- best observed reference:
  - `on_sample_8b310ad4_r3_20260604_1702`
  - `107 / 43`
  - `71.33%`

### Current latest

- `on_sample_latest_r1_20260604_1247`
  - `99 / 51`
  - `66.00%`
- `on_sample_latest_r2_20260604_1336`
  - `92 / 58`
  - `61.33%`

## Root Cause Split

### Group 1: `8b310ad4` relative to `off`

Primary issue:

- temporal facts regress under WM-preprocessed ingest

Evidence:

- `off -> 8b` regressions are dominated by `when` questions
  - count: `11`
- representative failures:
  - `Q2` support-group date: `7 May 2023` drifted to `11 July 2023`
  - `Q30` adoption meeting date: `Friday before 15 July 2023` drifted one week early
  - `Q31` pottery workshop date: same one-week-early drift
  - `Q37` July camping question: answer drifted to `2022`
  - `Q81/Q84/Q85/Q86`: several temporal/self-care facts became `no record`

Interpretation:

- `8b310ad4` already pays an accuracy cost when converting full session text into WM-preprocessed compact memory
- the cost is concentrated in relative-time anchoring and multi-event disambiguation
- this explains why `8b310ad4` stays below `off` on full `sample0` even though it can do well on `small`

### Group 2: `current latest` relative to `8b310ad4`

Primary issue:

- benchmark ingest pollutes memory extraction input with image metadata

Evidence from code diff:

- `8b310ad4` ingest was text-only:
  - `build_session_messages()` wrote only `[speaker]: text`
  - `viking_ingest()` sent only text parts
- `current latest` added:
  - `[image_caption]: ...`
  - `[image_query]: ...`
  - `image_url` multimodal parts
  - optional `[image_title_hint]`

Evidence from actual ingest meta:

- `current latest` `wm_preprocess.selected_spans` contained:
  - `image_caption`
  - `image_query`
  - serialized `{'type': 'image_url', ...}`
- affected sessions included `1, 2, 4, 7, 8, 14, 17, 19` and others

Representative QA regressions unique to `current latest`:

- `Q5 What did Caroline research?`
  - expected: `Adoption agencies`
  - `current latest`: answered with flower symbolism
- `Q57 What subject have Caroline and Melanie both painted?`
  - expected: `Sunsets`
  - `current latest`: answered `sunrise`
- `Q93/Q94/Q95`
  - necklace / Sweden / grandma gift facts swapped or lost
- `Q106`
  - recommended book fact lost

Interpretation:

- these errors are not mainly recall misses
- they are consistent with polluted memory construction: image-side descriptors become strong topical signals and push extracted memories toward visual surface themes instead of the chat fact that the QA needs

## Code Change

Changed file:

- [benchmark/locomo/openclaw/import_to_ov.py](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/import_to_ov.py)

Behavioral changes:

1. `build_session_messages(..., include_image_context=False)`
   - benchmark default is now text-only
   - image caption/query injection becomes explicit opt-in

2. `viking_ingest(..., attach_images=False, add_visual_hints=False)`
   - benchmark default sends text-only parts
   - image parts and visual hints become explicit opt-in

3. placeholder message cleanup
   - literal `[]` messages are skipped during benchmark session assembly

Why this change fits the objective:

- no recall logic changed
- no large memory-extraction refactor
- fix is limited to benchmark ingest shaping
- expected effect:
  - remove image-noise contamination
  - keep token usage flat or lower
  - recover text-fact accuracy on image-heavy LoCoMo sessions

## Test Verification

Local tests:

- [tests/benchmark/locomo/openclaw/test_import_to_ov.py](/home/jcp/Agent/code/OpenViking/tests/benchmark/locomo/openclaw/test_import_to_ov.py)
- result:
  - `10 passed`

Newly locked behaviors:

- default benchmark session assembly is text-only
- image context is opt-in
- image parts are opt-in
- visual hints are opt-in
- placeholder `[]` messages are skipped

## Runtime Validation

Focused remote validation run:

- `run_id = validation_imgclean_s1s2_20260605_0010`
- scope:
  - `sample0`
  - `sessions 1-2`
  - `mode=on`
  - `skip_judge=true`

Observed ingest result:

- `session_1 memories=4`
- `session_2 memories=0`

Key validation evidence:

- before fix, `current latest` session spans included image noise such as:
  - `image_caption`
  - `image_query`
  - serialized `image_url`
- after fix, the same validation run's `resume.json` showed clean trailing spans:
  - `session_1`
    - `[Caroline]: Thanks, Melanie! That's really sweet. Is this your own painting?`
    - `[Melanie]: Yeah, I painted that lake sunrise last year! It's special to me.`
  - `session_2`
    - adoption-agency text
    - follow-up question text
- note:
  - this focused remote validation was executed before the final local `[]` placeholder-skip patch was re-synced and re-run remotely
  - the `[]` cleanup itself is covered by local test verification in `test_build_session_messages_skips_placeholder_bracket_messages`

Targeted QA recovery from the same validation run:

- `Q5 What did Caroline research?`
  - old `current latest`: flower symbolism
  - validation run: `Caroline researched adoption agencies, especially ones that support LGBTQ+ people with adoption.`

Token spot-check from validation ingest:

- `session_1 ov llm total = 9597`
- `session_2 ov llm total = 11132`

Compared with previous `current latest r1` ingest meta:

- `session_1 ov llm total = 9790`
- `session_2 ov llm total = 10819`

Interpretation:

- no material token blow-up was introduced by the fix
- one session decreased and one slightly increased
- the net change is small enough that this fix remains compatible with the "token should be saved, not expanded materially" direction

## Current Status

What is proven:

- two regression groups have different causes
- `8b310ad4` regresses mainly on temporal compression/disambiguation
- `current latest` adds image-noise contamination on top of that
- the implemented benchmark-ingest fix removes the image contamination path
- local tests are green
- a focused remote validation run recovered at least one representative regression (`Q5`) without a meaningful token increase

What is not yet proven:

- full `sample0` accuracy for the new scheme
- whether this ingest-only fix is enough by itself to get back to at least `8b310ad4` best run
- whether an additional narrow temporal fix is needed for the older `8b`-style temporal regression cluster

## Next Recommended Step

Run one controlled `sample0` validation with the new ingest shaping and compare:

- accuracy against `current latest r1/r2`
- token against `current latest r1/r2`
- whether image-heavy regressions shrink while temporal regressions remain
