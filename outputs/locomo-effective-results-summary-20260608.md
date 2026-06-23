# LoCoMo Effective Results Summary - 2026-06-08

Source log:

- `outputs/recalltrim-validation-log-20260605.md`

Scope:

- latest accepted clean effective runs for `sample0-9`
- early abnormal state-dependent runs for `sample1/2` are excluded
- token scope follows the main validation log: `full token = ingest token + QA token`, judge token excluded
- `token / success = full token / correct answers`

## Effective Result Table

| sample | off accuracy | off QA/full token | off token/success | recalltrim accuracy | recalltrim QA/full token | recalltrim token/success | accuracy delta |
|---|---:|---:|---:|---:|---:|---:|---:|
| `sample0 / conv-26` | `76.00%` | `1,258,566 / 1,660,388` | `14,565` | `80.00%` | `1,023,845 / 1,410,492` | `11,754` | `+4.00%` |
| `sample1 / conv-30` | `76.54%` | `571,581 / 911,175` | `14,696` | `85.19%` | `567,950 / 887,254` | `12,859` | `+8.65%` |
| `sample2 / conv-41` | `82.35%` | `585,750 / 984,372` | `14,062` | `88.24%` | `593,781 / 948,819` | `12,651` | `+5.89%` |
| `sample3 / conv-42` | `75.73%` | `748,463 / 1,073,516` | `13,763` | `84.47%` | `729,079 / 1,133,947` | `13,034` | `+8.74%` |
| `sample4 / conv-43` | `87.72%` | `794,584 / 1,211,811` | `12,118` | `85.09%` | `796,065 / 1,211,463` | `12,489` | `-2.63%` |
| `sample5 / conv-44` | `77.27%` | `475,505 / 799,788` | `15,682` | `74.24%` | `472,056 / 792,604` | `16,176` | `-3.03%` |
| `sample6 / conv-47` | `90.70%` | `609,140 / 1,019,965` | `13,076` | `82.56%` | `605,704 / 990,218` | `13,947` | `-8.14%` |
| `sample7 / conv-48` | `82.05%` | `816,670 / 1,159,245` | `12,075` | `83.76%` | `815,091 / 1,092,317` | `11,146` | `+1.71%` |
| `sample8 / conv-49` | `83.18%` | `764,813 / 1,057,373` | `11,881` | `84.11%` | `766,748 / 1,052,787` | `11,698` | `+0.93%` |
| `sample9 / conv-50` | `85.90%` | `549,797 / 813,112` | `12,136` | `74.36%` | `650,778 / 979,606` | `16,890` | `-11.54%` |

## Aggregate

| metric | off | recalltrim | delta |
|---|---:|---:|---:|
| total correct / total QA | `805 / 987` | `814 / 987` | `+9 correct` |
| total accuracy | `81.56%` | `82.47%` | `+0.91%` |
| QA token | `7,174,869` | `7,021,097` | `-153,772` |
| full token | `10,690,745` | `10,499,507` | `-191,238` |
| token / success | `13,280` | `12,899` | `-381` |

Overall:

- On all effective samples `0-9`, recalltrim is slightly better in total accuracy and slightly cheaper in token/success.
- The gain is uneven. `sample5/6/9` regress, and `sample9` is severe enough to dominate the risk profile.

## Degradation Analysis: sample5 / sample6 / sample9

### Evidence Table

| sample | accuracy delta | off->recalltrim regressions | recalltrim recoveries | unknown-style Q count off -> recalltrim | no response | strict service error hits |
|---|---:|---:|---:|---:|---:|---:|
| `sample5 / conv-44` | `-3.03%` | `8` | `6` | `4 -> 5` | `0` | `0` |
| `sample6 / conv-47` | `-8.14%` | `9` | `2` | `2 -> 3` | `0` | `1` warning |
| `sample9 / conv-50` | `-11.54%` | `12` | `3` | `2 -> 4` | `1` | `0` |

Strict service-error check searched paired launcher/master/OV/GW logs for:

- `Traceback`
- `http_500`
- `network connection error`
- `timed out`
- `TimeoutError`
- `ReadTimeout`
- `ConnectTimeout`
- `rate limit`
- `HTTP 429`
- `HTTP 500/502/503/504`
- `connection reset`
- `ConnectionError`

Result:

- `sample5`: no strict service-error hits in either mode.
- `sample6`: one LiteLLM warning in recalltrim OV log: failed to fetch remote model cost map due read timeout. This is a cost-map metadata warning, not a QA model response failure, and the run completed normally.
- `sample9`: no strict service-error hits in either mode.

### sample5 Pattern

Regression examples:

| qi | expected | off answer shape | recalltrim answer shape | diagnosis |
|---:|---|---|---|---|
| `8` | `three years` | direct: about 3 years | says adoption timing not stated | context under-supplied |
| `21` | `chicken` | direct: chicken | says preference not found | useful fact omitted |
| `23` | `camping with girlfriend` | direct: camping | drifts to hiking trip / location unspecified | nearby activity confusion |
| `24` | pet-friendly apartments / open spaces | includes housing, landlords, park/woods | narrows to work stress / city living | adjacent constraints lost |
| `77` | about an hour | direct: about an hour | says duration not specified | conservative answer caused by missing span |
| `88` | two hours | direct: two hours | says duration not specified | conservative answer caused by missing span |

Counter-signal:

- recalltrim also recovers some facts, e.g. Pixie from breeder, dog names, grooming.

Interpretation:

- This does not look like model-service instability. The run has normal completion, no no-response rows, and only a small accuracy drop.
- The pattern is a recalltrim selection tradeoff: it recovers some compact facts but loses timing/duration/location details in other questions.

### sample6 Pattern

Regression examples:

| qi | expected | off answer shape | recalltrim answer shape | diagnosis |
|---:|---|---|---|---|
| `6` | John CS:GO, James Apex Legends | exact paired favorites | lists James games, says John's favorite not explicit | one entity's key fact omitted |
| `7` | likely yes, Connecticut | infers yes from Stamford | refuses direct inference | over-conservative inference |
| `10` | Ned, Daisy, Max | all three dogs | misses Max | incomplete entity set |
| `15` | 2022-04-26 | exact date | says no recalled memory | missing date span |
| `17` | UNO | infers Uno from colored cards | says not mentioned | loss of indirect evidence |
| `19` | Name of the Wind, Stormlight, Kingkiller, Expanse | all four | misses Name of the Wind | incomplete list |
| `37` | Jill likely John's partner | gives relationship inference | says exact relationship unspecified | inference weakened |

Counter-signal:

- recalltrim recovers a few facts, e.g. Max catching frisbees, and includes Nuuk for countries visited.

Interpretation:

- The one service warning is a LiteLLM cost-map fetch timeout, not an answer-generation failure.
- The regression is concentrated in missing entity-list members, exact dates, and indirect inference. This points to recalltrim implementation behavior: trimming/ranking is not preserving enough supporting neighboring evidence for multi-fact and inferential questions.

### sample9 Pattern

Regression examples:

| qi | expected | off answer shape | recalltrim answer shape | diagnosis |
|---:|---|---|---|---|
| `1` | between Mar 26 and Apr 20 2023 | around Apr 2023 | before 2023-04-20 | answer too broad / partial |
| `9` | May 1, 2023 | exact date | `No response from OpenClaw` | isolated response failure |
| `13` | last week of May 2023 | week before 2023-05-31 | lists multiple Tokyo concerts | event disambiguation failure |
| `14` | yes, likes large crowds / stage rush | strong inference yes | says Hollywood Bowl not mentioned | over-literal retrieval/inference |
| `80` | immerse himself in something he loves | uses concert/favorite albums advice | says no information | missing advice span |
| `81` | explore other things and have fun | correct advice | drifts to studio repair/collaboration/Boston | wrong anchor selected |
| `89` | regular walks together | exact park walk arrangement | says only Dave arrangement, not Calvin | entity/role split too rigid |
| `110` | explore and grow brand | exact-ish | Boston music scene | anchor drift |
| `111` | catch up, food, show city spots/music spots | partly correct but judged wrong | expands venues but misses expected shape | answer-shape drift |

Counter-signal:

- recalltrim recovers some off misses, e.g. two insurance paperwork cases, San Francisco location, and Sep 1 return date.

Interpretation:

- There is one `No response from OpenClaw` row in recalltrim, but no corresponding strict service-error hits in logs. A single no-response can explain one lost question, not the full `-11.54%` drop.
- The larger pattern is wrong-context or over-literal-context selection: recalltrim often chooses a nearby but different anchor, or refuses an inference because the exact named item is not in the selected snippets.
- `sample9` also reverses token savings: recalltrim spends more QA and full tokens while getting fewer correct answers. That is inconsistent with a simple external service outage and more consistent with ineffective context selection/reranking.

## Root-Cause Judgment

Primary cause:

- recalltrim implementation logic / retrieval-trimming behavior is the dominant cause of the `sample5/6/9` degradation.

Why not primarily large-model service instability:

- The paired runs completed with normal artifacts.
- Strict log scan found no broad model/network/HTTP failure pattern.
- `sample5` and `sample6` have zero no-response rows; `sample9` has only one.
- Degraded answers are semantically structured, not random failures: they repeatedly show missing dates, incomplete entity lists, conservative "not specified" answers, and wrong neighboring anchors.
- The same recalltrim runs also recover some specific facts, which argues against a global service collapse.

What recalltrim likely needs:

- preserve neighboring turns around selected evidence, especially for dates, durations, entity lists, and causal/inferential questions
- add a fallback when the generated answer contains "not specified" / "no relevant information" despite high-confidence off-mode answers
- improve multi-entity list completeness checks before final answer
- add anchor disambiguation for repeated events, trips, concerts, parks, books, pets, and dates
- track `unknown-style Q count`, `off-correct/on-wrong count`, and `token/success` as first-class validation metrics
