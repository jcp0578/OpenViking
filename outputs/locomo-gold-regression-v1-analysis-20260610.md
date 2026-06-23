# LoCoMoGoldRegressionv1 Analysis - 2026-06-10

## Scope

This note analyzes two questions after the `LoCoMoGoldRegressionv1` full runs:

1. Is `memories=0` in the benchmark summary a real extraction/persistence failure or a misleading statistic?
2. How does the current latest-code Gold baseline compare with the earlier clean `off` baseline for `sample5/6/9`?

## Finding 1: `memories=0` is not a reliable proxy for durable memory coverage

### What the benchmark actually counts

In [phase_a_off.py](/home/jcp/Agent/code/OpenViking/benchmark/locomo/openclaw/phase_a_off.py:2174), `total_memories()` only sums:

- `session_detail["memories_extracted"]`

If that dict is missing, zeroed, or not backfilled for the current commit, `memory_count` is reported as `0` even if durable memory files exist under the account/agent tree.

### Evidence from the full Gold runs

Gold ingest summary totals:

| sample | sum of `memory_count` across 19 sessions | sessions with non-zero `memory_count` |
|---|---:|---|
| `sample5` | `11` | `session_1` only |
| `sample6` | `17` | `session_1` only |
| `sample9` | `6` | `session_1` only |

At the same time, preprocessing remained active almost everywhere:

| sample | active sessions | fallback sessions | total selected spans | total structured facts |
|---|---:|---:|---:|---:|
| `sample5` | `18` | `1` | `307` | `94` |
| `sample6` | `17` | `2` | `303` | `93` |
| `sample9` | `18` | `1` | `238` | `86` |

This rules out a simple "preprocessor did not run" explanation.

### Strongest proof from `sample9`

For `sample9`, the persisted durable memory tree after the run contains many files even though the benchmark summary reports:

- `sum_memory_count = 6`
- only `session_1` is non-zero

But the actual durable memory tree under the account/agent path contains:

- `94` memory files
- substantial `events/...` coverage
- substantial `entities/...` coverage

That means:

- `memories_extracted.total = 0` is **not** equivalent to "no durable memory was written"
- `memory_count = 0` in the current direct-OV summary is at least partly a **statistics/telemetry problem**

### Narrow conclusion

`memories=0` is not a trustworthy top-level coverage metric in the current full-run workflow.

It should be treated as:

- a weak signal for "newly counted extracted memories"
- not a direct signal for "persisted durable memories available for retrieval"

## Finding 2: this is not only a stats issue; durable memory quality is still weak

The statistics are misleading, but the latest-code baseline is still materially worse than the older clean `off` baseline on `sample5/6/9`.

### Off baseline vs current latest-code Gold baseline

| sample | off accuracy | latest Gold accuracy | accuracy delta | off QA token | latest QA token | off full token | latest full token | token/success delta |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `sample5` | `77.27%` | `65.15%` | `-12.12%` | `475,505` | `560,030` | `799,788` | `995,519` | `-2658.0` |
| `sample6` | `90.70%` | `80.23%` | `-10.47%` | `609,140` | `711,750` | `1,019,965` | `1,191,108` | `-2760.8` |
| `sample9` | `85.90%` | `70.51%` | `-15.39%` | `549,797` | `633,054` | `813,112` | `1,017,181` | `-625.9` |

Interpretation:

- Accuracy is materially worse on all three primary Gold samples.
- QA token cost is materially higher on all three.
- Full token cost is also materially higher on all three.
- Token per success is numerically lower in the latest Gold runs, but that does not compensate for the large accuracy loss.

So the current latest-code baseline is not merely "mis-measured"; it is also underperforming on the actual QA outcome.

## Finding 3: time anchoring exists, but normalization is only partial

Durable event memory content shows that time grounding is present, but not fully normalized into the most reusable form.

### Good signs

Examples already normalized to durable anchors:

- `On May 1, 2023, Dave told Calvin ...`
- `Calvin performed at a music festival in Tokyo.`
- `On 2023-06-08 (the day before 2023-06-09), Calvin met with his creative team ...`

This indicates that extraction is already using session dates and converting some relative references into anchored forms.

### Remaining weaknesses

Examples still carrying semi-relative wording:

- `before 2023-08-14`
- `the previous week`
- `recently joined`
- `the day before 2023-09-02`
- `end of next month relative to 2023-08-14`

These are better than raw `yesterday` / `last week`, but they are still not the most durable final form for long-range retrieval and QA copying.

### Narrow conclusion

Time anchoring is present, but relative-time normalization is still incomplete.

The next extraction-side optimization should target:

- converting more relative expressions into explicit dates or explicit date ranges
- reducing mixed phrasing where one sentence is absolute and the next still uses relative shorthand
- improving durable summary text so the answer model copies the exact gold-bearing date statement more often

## Finding 4: the failure layer is not primarily ranking

The earlier ranking and injection-rule experiments failed the small gate.

Current evidence points somewhere else:

- preprocessing is active
- selected spans exist
- structured facts exist
- durable memory files exist
- but QA accuracy is still worse than `off`

This suggests the stronger next hypothesis is:

- the main gap is in extraction quality and durable memory wording
- especially temporal normalization and memory surface form
- not in another round of ranking tweaks

## Immediate engineering direction

### Do next

1. Treat `memory_count` / `memories_extracted.total` as unreliable telemetry in direct-OV full runs.
2. Add a better coverage metric for evaluation:
   - durable file count delta
   - event/entity file count delta
   - direct API retrieval probe hit rate for representative question terms
3. Optimize extraction/durable memory writing before more ranking work.
4. Focus first on:
   - time anchoring
   - relative-time normalization
   - exact gold-bearing temporal statements in event summaries

### Do not do next

- do not spend another round on `memory-ranking.ts` query-side strong rules
- do not accept `memory_count=0` as proof that extraction failed
- do not compare future optimizations only against the misleading `memory_count` field

## Practical next experiment

The next minimal-but-meaningful experiment should be extraction-side:

1. tighten event summary prompt/output so relative times are rewritten into explicit date/date-range form more aggressively
2. validate on `sample5/6/9` small gate
3. if `sample5` improves and `sample6/9` do not regress, rerun the full Gold set

That path is more consistent with the current evidence than another retrieval-layer tweak.
