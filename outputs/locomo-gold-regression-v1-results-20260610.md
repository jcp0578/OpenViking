# LoCoMoGoldRegressionv1 Results - 2026-06-10

## Scope

This run executed the primary Gold set from `LoCoMoGoldRegressionv1` in the remote container environment.

Tested complete samples:

- `sample5`
- `sample6`
- `sample9`

Execution mode:

- `mode`: `on`
- `sessions`: `1-19`
- `qa range`: `1-999`, with the benchmark driver filtering to valid questions
- `qa-disable-autocapture`: enabled
- `ingest-mode`: `direct-ov`
- `model`: `volcengine/doubao-seed-2.0-pro`
- remote host: `123.60.114.206:10008`
- container: `jcp-dev`
- remote repo HEAD: `89d3f621ba08fe02d26c74271b9be90afde46623`

Runtime plugin checksums:

| file | sha256 |
|---|---|
| `client.ts` | `b8aa3db4fb631827d66fcdedfdaa78e25bd41fb9b38d35cdcdc4107634f54279` |
| `auto-recall.ts` | `bd5c71b9d1a07f8d5afc6def6c15f00a0676f99e668f18cf8534ae8d3d5c3424` |
| `index.ts` | `49943c45701997e07e64d84616256afe0944de50712692bf9a790efd51431317` |
| `memory-ranking.ts` | `9b9f3255d6d1cee09fd75e769634701855b9d83374e6ece6d07e656ed723d206` |

Note: `memory-ranking.ts` was deployed from local `HEAD` for this Gold run, so the run does not include the later query-side ranking experiments.

## Results

| sample | run_id | correct | judged | accuracy | avg input tokens | avg total tokens | tokens per success |
|---|---|---:|---:|---:|---:|---:|---:|
| `sample5` | `on_sample5_full_gold_20260610a` | 43 | 66 | 65.15% | 2268.8 | 8485.3 | 13024.0 |
| `sample6` | `on_sample6_full_gold_20260610b` | 69 | 86 | 80.23% | 2057.3 | 8276.2 | 10315.2 |
| `sample9` | `on_sample9_full_gold_20260610c` | 55 | 78 | 70.51% | 2092.4 | 8116.1 | 11510.1 |
| **overall** | - | **167** | **230** | **72.61%** | - | - | **11406.2** |

Token basis:

- `sample5` gateway QA tokens: `560030`
- `sample6` gateway QA tokens: `711750`
- `sample9` gateway QA tokens: `633054`
- total gateway QA tokens: `1904834`

## Wrong Question Sets

| sample | wrong qi |
|---|---|
| `sample5` | `5, 6, 13, 16, 19, 20, 22, 24, 25, 27, 29, 44, 45, 54, 65, 66, 69, 71, 72, 80, 81, 82, 85` |
| `sample6` | `1, 6, 12, 14, 17, 18, 20, 32, 34, 38, 68, 76, 83, 89, 102, 103, 109` |
| `sample9` | `2, 8, 9, 12, 13, 24, 28, 29, 32, 61, 63, 73, 75, 80, 89, 90, 91, 92, 94, 95, 96, 97, 105` |

## Local Artifacts

| sample | CSV | meta | summary |
|---|---|---|---|
| `sample5` | `outputs/locomo-gold-regression-v1/on_sample5_full_gold_20260610a/phaseA_on_19sessions_on_sample5_full_gold_20260610a.csv` | `outputs/locomo-gold-regression-v1/on_sample5_full_gold_20260610a/phaseA_on_19sessions_on_sample5_full_gold_20260610a_meta.json` | `outputs/locomo-gold-regression-v1/on_sample5_full_gold_20260610a/phaseA_on_19sessions_on_sample5_full_gold_20260610a.txt` |
| `sample6` | `outputs/locomo-gold-regression-v1/on_sample6_full_gold_20260610b/phaseA_on_19sessions_on_sample6_full_gold_20260610b.csv` | `outputs/locomo-gold-regression-v1/on_sample6_full_gold_20260610b/phaseA_on_19sessions_on_sample6_full_gold_20260610b_meta.json` | `outputs/locomo-gold-regression-v1/on_sample6_full_gold_20260610b/phaseA_on_19sessions_on_sample6_full_gold_20260610b.txt` |
| `sample9` | `outputs/locomo-gold-regression-v1/on_sample9_full_gold_20260610c/phaseA_on_19sessions_on_sample9_full_gold_20260610c.csv` | `outputs/locomo-gold-regression-v1/on_sample9_full_gold_20260610c/phaseA_on_19sessions_on_sample9_full_gold_20260610c_meta.json` | `outputs/locomo-gold-regression-v1/on_sample9_full_gold_20260610c/phaseA_on_19sessions_on_sample9_full_gold_20260610c.txt` |

## Interpretation

This establishes the current `LoCoMoGoldRegressionv1` primary Gold baseline for the frozen runtime configuration.

Important boundary:

- These results are a baseline, not an optimization acceptance result.
- Future optimization runs should compare against these CSV/meta artifacts and report `WRONG -> CORRECT`, `CORRECT -> WRONG`, token movement, and suspected failure layer.
- Earlier historical runs may not be directly comparable if they used different runtime `memory-ranking.ts`, different QA filtering, or a different judged question count.

## Next Use

For any candidate change:

1. Run the small gate first on `sample5/6/9` slices.
2. If it passes, rerun this primary Gold set.
3. Accept only if overall accuracy improves, no primary sample materially regresses, and tokens per successful task remains stable or improves.
