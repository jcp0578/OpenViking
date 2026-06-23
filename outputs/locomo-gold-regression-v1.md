# LoCoMoGoldRegressionv1

Date: 2026-06-09

## Purpose

This gold regression set is for evaluating overall LoCoMo behavior, not for proving a single narrow rule. Small subsets are useful as kill gates, but accepted changes must eventually pass full-sample regression.

## Test Sets

### Primary Gold Set

Run the following complete samples in the same remote container environment:

| sample | role |
|---|---|
| `sample5` | Known recall/injection sensitivity; includes fragile temporal and entity-specific questions |
| `sample6` | Multi-fact, multi-person, date and list coverage |
| `sample9` | Dense person/event memory coverage; good for detecting broad injection and ranking side effects |

### Sentinel Set

Keep a fixed 10-20 question slice from each `sample0-9`.

Coverage should include:

- yes/no questions
- relative and absolute time questions
- list/set questions
- multi-entity questions
- preference questions
- location questions
- causal or indirect inference questions

### Focus Set

Keep known fragile questions for diagnosis, not final acceptance:

| sample | questions |
|---|---|
| `sample5` | `q6`, `q9` |
| `sample6` | `q7`, `q8`, `q14`, `q17`, `q19` |
| `sample9` | `q75`, `q76`, `q78`, `q86`, `q88` |

## Acceptance Rules

Freeze the current acceptable baseline before new optimization:

- keep `client.ts` namespace retry
- keep `auto-recall.ts` benchmark `Question:` extraction
- do not include new `memory-ranking.ts` query-side strong rules unless they pass this gold flow
- do not accept answer-normalization, judge, or test-harness changes as LoCoMo accuracy fixes
- do not count diagnostic runs as accuracy runs

### Current Execution Goal Override (2026-06-13)

Until this override is replaced by a newer dated section, LoCoMoGoldRegressionv1 execution is scoped to effective LoCoMo accuracy improvement only.

Every stage must first answer whether the current action directly serves a valid accuracy improvement. If it does not, stop the experiment, write the reason to `outputs/locomo-gold-regression-v1-followup-20260610.md`, and do not expand the run matrix.

Current hard exclusions:

- do not add new `memory-ranking.ts` query-side strong rules
- do not implement answer normalization as an accuracy fix
- do not modify benchmark code, judge code, or the test framework to make a result pass
- do not overfit a single LoCoMo question

Current preferred investigation layer:

- prioritize extraction coverage and durable memory write quality
- focus on whether small events with relative time and image/text mixed evidence are written as retrievable, injectable durable events
- use diagnostics to locate the failure layer, but never count diagnostics as accuracy evidence

Current health gate before any LoCoMo accuracy run:

- gateway `/health` passes
- OpenViking `/health` passes
- a minimal OpenClaw QA request returns a real answer
- the same minimal QA response has `usage.total_tokens > 0`
- timeout, HTTP 5xx, empty answer, or `total_tokens=0` makes the run invalid

Current staged execution order:

1. Run the health gate.
2. If the health gate fails repeatedly, stop LoCoMo and record model-layer blocking.
3. If the health gate passes, run `sample9 q8-13` shared auto-recall small regression.
4. If `sample9 q8-13` is below cleanbase `3/6`, stop expansion and analyze the evidence path.
5. If `sample9 q8-13` is at least cleanbase `3/6`, run the `sample5/6/9` subset gate.
6. Accept the subset gate only if `sample5` improves and `sample6/9` do not regress.
7. Expand to the three complete primary gold samples only after the subset gate passes.

For any failed gate, return to evidence-path analysis:

`extraction input -> durable memories -> selected_spans -> relevant_memories -> final answer`

Any code change proposed after that analysis must explain why it is generic and why it is not a local overfit to `sample5/6/9`.

### Current Running Gold Target (2026-06-14)

Status: `running`

This section supersedes the blocked `sample5 q6` immediate gate as the active gold execution target. The q6 multimodal item remains valid as an extractor-only diagnostic, but it is not the current accuracy gate because the accepted benchmark path is text-only and does not expose caption/query evidence to extraction.

Current selected gold item:

| field | value |
|---|---|
| active gold item | `sample6 session_13 Durable Fact Extraction Gold` |
| target QA questions | `sample6 q95`, `sample6 q96`, `sample6 q97` |
| evidence visibility | text-visible in the accepted accuracy input |
| target failure class | compact durable facts are buried in oversized person cards or skipped by injection budget |

Current required durable facts:

| fact | required durable shape |
|---|---|
| James game-design/course project | standalone answerable memory linking James, football simulator, course/project context, and player databases |
| James football support | standalone answerable memory linking James to Liverpool FC support/fandom |
| John football support | standalone answerable memory linking John to Manchester City support/fandom |

Current validation order:

1. Run the model health gate before any accuracy run.
2. Run fresh ingest diagnostic for `sample6 sessions 1-19` under a new account/user; do not count this as accuracy.
3. Inspect durable files and require focused small memories for `football_simulator_project`, `liverpool_fc`, and `manchester_city_fc`.
4. If durable files pass, run retrieval/injection diagnostics for `q95/q96/q97` and prove the focused memories are injected rather than skipped over budget.
5. Only after durable survival and injection visibility pass, run a valid accuracy gate on `sample6 q95-q97`.
6. Continue to broader gates only if `sample6 q95-q97` improves without invalid runs or material token-cost regression.

Current guardrails:

- do not add `memory-ranking.ts` query-side strong rules
- do not do answer normalization
- do not edit benchmark, judge, or test framework code
- do not count diagnostics as accuracy
- every behavioral change must be generic to durable support/fandom, project/course, or similar compact fact atomization, not hardcoded to sample6 names or expected answers

A change can move past the small-gate stage only if:

- `sample5` improves on the target slice or full sample
- `sample6` does not regress materially
- `sample9` does not regress materially
- token cost per successful task does not worsen materially
- known fragile questions do not show a systematic `CORRECT -> WRONG` pattern

For full gold acceptance:

- total accuracy across the primary gold set improves
- no primary sample has a significant accuracy drop
- per-success token cost is stable or lower
- wrong-to-right gains are not only offset by right-to-wrong swaps
- if service randomness is suspected, repeat key full runs at least twice and compare both mean accuracy and per-question stability

## Gold Layers

LoCoMoGoldRegressionv1 now distinguishes three layers. A run or fix must be labeled with the highest layer it actually proves.

| layer | purpose | valid evidence | not valid evidence |
|---|---|---|---|
| Extraction Input Gold | Whether source evidence is actually visible to the extractor | assembled extraction prompt or session messages containing the relevant text/image/caption fields | raw dataset fields that were dropped before extraction |
| Extraction Gold | Durable memory write quality | extractor-only output or fresh ingest durable memory files | final QA answer alone |
| Retrieval Gold | Whether the right durable memory is recalled and injected | selected spans, relevant memories, final prompt injection evidence | a correct durable memory that was never retrieved |
| QA Gold | End-to-end benchmark answer quality | judged QA CSV with non-zero usage from a valid run | timeout, `total_tokens=0`, or diagnostic probe |

Rules:

- Extraction diagnostics can explain failure layers, but cannot be counted as accuracy improvement.
- Extraction Gold is meaningful only after Extraction Input Gold proves the required evidence was visible to the extractor.
- Retrieval diagnostics are meaningful only after the required durable memory exists.
- QA accuracy runs are meaningful only after the model health gate passes and `usage.total_tokens > 0`.
- A code candidate must not skip from a weak extraction diagnostic directly to broad full-sample regression.

## Required Output Fields

Each gold run should write a Markdown summary and machine-readable artifacts.

Record run metadata:

- `run_id`
- `commit_sha`
- plugin file checksums
- remote host and container
- model provider and model
- test mode
- sample range
- QA range
- timestamp

Record per-sample metrics:

- `accuracy`
- `correct`
- `judged`
- `avg_input_tokens`
- `avg_total_tokens`
- `tokens_per_successful_task`

Record per-question details:

- `sample`
- `qi`
- `question`
- `expected`
- `response`
- `result`
- `reasoning`
- `input_tokens`
- `total_tokens`

Record diff against baseline:

- `CORRECT -> WRONG`
- `WRONG -> CORRECT`
- answer-shape changes
- token changes
- suspected failure layer: retrieval, injection selection, answer synthesis, normalization, or judge variance

## Time Anchoring Requirement

The mem0 time anchoring design is worth adopting as a gold evaluation principle. Its useful property is that it does not leave "time understanding" to unconstrained model inference. Instead, it turns time grounding into an explicit prompt interface and output contract.

In the additive extraction prompt, the implementation explicitly injects both `ObservationDate` and `CurrentDate`. It then states that all relative time expressions must be resolved only against `ObservationDate`. The model must not use `CurrentDate` to infer historical dialogue references such as `yesterday`, `next month`, `recently`, `today`, or `last week`.

The output side should also avoid preserving short-lived vague expressions in durable memory. A memory should not keep unstable phrases such as `last week` when the observation date allows a concrete week or date range. The durable form should be something like `the week of 2023-05-15` or an explicit date interval, so the memory remains searchable, alignable, and reusable months later.

The engineering principle is:

- define which time anchors are present in the input
- define which anchor is allowed for relative-time resolution
- define what temporal granularity is allowed in memory output

This turns time grounding from a vague model capability problem into a controllable design. It should reduce time-anchor session pollution and later retrieval ambiguity.

Source reference:

- `/home/jcp/Agent/code/mem0/outputs/mem0-2026-update-summary.html`, section `1.1 时间锚定的具体实现`

## Gold Coverage For Time Anchoring

The gold set should include questions that verify the full time-grounding contract:

| category | expected behavior |
|---|---|
| historical `yesterday` | resolve relative to the dialogue observation date, not the run date |
| historical `today` | resolve to the observation date |
| `last week` / `next month` | convert to a durable week or month range when possible |
| `recently` | avoid keeping the vague word as the only time expression |
| multiple sessions | prevent one session's current date from contaminating another session's historical references |
| retrieval | make durable time expressions retrievable months later |

## ExtractionCoverageGold

ExtractionCoverageGold evaluates whether the memory system writes durable, reusable memories from raw conversation evidence. This is separate from final QA correctness.

### Generic Coverage Contract

For small but durable personal events, extraction should preserve the event as an answerable memory when the raw evidence contains:

- a concrete actor or participant
- an activity or event context
- an observed object, subject, place, or outcome
- an observation/session time anchor
- relative time that can be grounded from that anchor
- image/photo/caption evidence when the fact is multimodal

The durable memory should prefer stable calendar expressions over short-lived relative wording when an observation date is available.

Reject an extraction candidate when:

- the event is reduced to a generic preference or habit
- the actor, activity, observed subject, or time anchor is dropped
- visual evidence is present in raw multimodal fields but absent from the durable event
- the primary time expression remains only `last week`, `previous week`, or `week before <date>` when a concrete calendar range can be derived
- the fact is only recoverable through query-side ranking or answer-side prompt hints

### sample5 q6 Extraction Gold

This gold item captures the current failure class: small event + relative time + image/text mixed fact.

Source evidence:

| field | value |
|---|---|
| sample | `sample5` |
| session | `session_4` |
| dialogue id | `D4:1` |
| session time | `2023-05-03 17:41` |
| speaker | `Audrey` |
| text evidence | Audrey said that last week she went on a hike and had an amazing experience with a hummingbird. |
| image evidence | `https://images.pexels.com/photos/7875455/pexels-photo-7875455.jpeg` |
| caption evidence | `a photography of a hummingbird sitting on a branch with its wings spread` |
| query evidence | `cute little bird perched branch hummingbird hike nectar flowers` |

Extraction input requirement:

| requirement | reason |
|---|---|
| The assembled extraction input must include the text evidence. | Without the dialogue text, actor/event/subject cannot be grounded. |
| The assembled extraction input must include image/photo/caption/query evidence when visual coverage is being evaluated. | The extractor cannot be required to preserve visual evidence that was dropped before extraction. |
| If `img_url`, `blip_caption`, and `query` are absent from the extraction input, classify the failure as `Extraction Input Gold` failure, not retrieval/ranking failure. | This prevents query-side or prompt-side fixes from masking an upstream evidence-loss problem. |

Required durable memory coverage:

| required field | expected coverage |
|---|---|
| actor | `Audrey` |
| event context | hike / hiking |
| observed subject | hummingbird |
| grounded time | `last week` anchored to `2023-05-03 17:41` |
| durable time form | `late April to early May 2023`, `the previous seven days before 2023-05-03`, or an equivalent concrete calendar range |
| visual evidence | image/photo/caption evidence that the hummingbird fact is visually supported |
| memory shape | must include at least one standalone durable event memory; entity/person memories may supplement it but must not be the only durable record |

Acceptable durable event examples:

- `Around late April to early May 2023, Audrey went hiking and had an amazing hummingbird encounter, supported by the shared hummingbird photo/caption.`
- `During the previous seven days before 2023-05-03, Audrey saw a hummingbird while hiking; the conversation included image evidence of a hummingbird.`

Reject examples:

- `Audrey likes hiking.`
- `Audrey saw a hummingbird.`
- `Audrey went hiking the previous week.`
- `A hummingbird was observed by Audrey during a hike the week before 2023-05-03.`
- Any memory that preserves the event but omits both the image/photo evidence and durable calendar expression.
- Entity/person-only coverage, even if it mentions Audrey, hike, hummingbird, time, and photo evidence.

### sample5 q6 Gate

Before any fresh QA run for sample5 q6, the extractor-only gate must pass:

| gate | requirement |
|---|---|
| extractor-only input | `sample5 session_4` with the original text and multimodal `D4:1` evidence visible to extraction |
| extractor-only output | a durable event containing Audrey, hike, hummingbird, grounded durable time, and visual evidence |
| invalid output | entity-only coverage, relative-only time, or missing visual evidence |
| next step if pass | fresh ingest `sample5 sessions 1-19` with a new account/user, then QA `sample5 q6/q9` |
| next step if fail | do not run fresh QA; improve extraction coverage or reject the candidate |

### sample6 session_13 Durable Fact Extraction Gold

This gold item captures a second extraction coverage class found after the full sample6 regression: compact durable facts can be present in long person cards but still fail downstream retrieval/injection because the decisive evidence is not available as smaller answerable memories.

Source evidence:

| field | value |
|---|---|
| sample | `sample6` |
| session | `session_13` |
| session time | `2022-06-13 16:30` |
| participants | John, James |
| project evidence | James is working on a football simulator project for a gaming/programming course and completed collecting player databases. |
| fandom evidence | James is a Liverpool FC fan and does not miss Liverpool matches. |
| fandom evidence | John is a Manchester City fan. |

Required durable memory coverage:

| required field | expected coverage |
|---|---|
| project/course fact | a standalone answerable memory, not only a broad James person profile, links James, the course/project context, football simulator, and player databases |
| James fandom fact | a standalone answerable memory, not only a broad person profile, links James to Liverpool FC support/fandom |
| John fandom fact | a standalone answerable memory, not only a broad person profile, links John to Manchester City support/fandom |
| observation time | facts should carry the 2022-06-13 observation/session date when available |

Acceptable memory shapes include focused `entities/project/...`, `entities/football_club/...`, or event memories that keep actor, relation, object, and date together.

Reject examples:

- Only `entities/person/james.md` contains the football simulator and Liverpool facts.
- Only `entities/person/john.md` contains the Manchester City fact.
- A broad catch-up event mentions football generally but cannot directly answer which club each person supports or what project James is working on.
- The facts are only recoverable through query-side ranking, answer normalization, or oversized person-card snippets.

### sample5 q6 Retrieval Gold

After fresh ingest `sample5 sessions 1-19`, retrieval/injection must prove the durable event is usable, not just written.

| layer | pass condition |
|---|---|
| durable files | at least one `events/...` memory contains Audrey, hike/hiking, hummingbird, durable calendar range, and image/photo/caption evidence |
| search/retrieval | query `When did Audrey see a hummingbird?` retrieves the standalone hummingbird/hike event or injects an equivalent event memory |
| injection | final QA context includes event-level evidence, not only broad `entities/person/audrey.md` |
| invalid pass | query-side ranking tricks, answer-side hints, or entity-only recall do not satisfy retrieval gold |

### sample5 q6/q9 QA Gold

This gate is valid only after the model health gate passes and fresh ingest uses a new account/user.

| question | requirement |
|---|---|
| q6 | answer must correctly state that Audrey saw the hummingbird around late April to early May 2023, the week before 2023-05-03, or an equivalent calendar range |
| q9 | must remain correct; no regression allowed |
| sample5 focus gate | must improve from old baseline `1/2` to `2/2` |
| invalid run | timeout, HTTP 5xx, empty answer, or `total_tokens=0` |

## Recommended Flow

1. Verify the model health gate: OpenViking/gateway health plus a minimal OpenClaw QA response with a real answer and `usage.total_tokens > 0`.
2. For extraction-coverage candidates, verify Extraction Input Gold before judging extractor output.
3. Run the relevant ExtractionCoverageGold gate only after the required evidence is visible to extraction.
4. For sample5 q6, require extractor-only success before any fresh ingest QA.
5. If extractor-only passes, run fresh ingest `sample5 sessions 1-19` and QA `sample5 q6/q9`.
6. Reject changes that do not improve `sample5 q6/q9` over the old `1/2` focus baseline or that regress q9.
7. Only after sample5 passes, run `sample9 q8-13` shared auto-recall regression and require it to stay at or above the cleanbase `3/6`.
8. Only after sample9 passes, run the `sample5/6/9` subset gate and reject changes that hurt `sample6/9`.
9. For changes that pass, run the three complete primary gold samples.
10. Compare accuracy, token cost, and per-question diffs against the frozen gold baseline.
11. Only after that, expand to a broader `sample0-9` sentinel or full matrix.

## Current Position

Recent narrow experiments established the current staged gate:

- query-side entity anchor boost did not improve `sample5` and severely hurt `sample9`
- injection-side single-person filtering fixed `sample5 q9` but caused a `sample5 q6` regression, so net `sample5` did not improve
- answer-side time hints did not fix `sample5 q6` and should not be accepted
- `sample5 q6` extractor-only can pass when gateway-style multimodal evidence is visible to extraction
- the remaining unproven point is full fresh ingest stability: whether the same fact is written as a standalone event memory under `sample5 sessions 1-19`, retrieved/injected for q6, and improves `sample5 q6/q9` from `1/2` to `2/2`
- current LoCoMo import/probe defaults are text-only for image context, so visual evidence must be proven visible in extraction input before holding the extractor responsible for preserving it

Therefore the current gold baseline should keep only the proven integration fixes and avoid accepting new ranking, injection, answer-normalization, or test-harness rules until they pass the staged flow above.
