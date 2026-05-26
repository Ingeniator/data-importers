# Sampling Feature Design

## Overview

Sampling is configured in the import modal (when user presses "Import Dataset").
Default: no sampling. User expands the block via "Configure Sampling" button.
Sampling executes on the backend based on the config sent with the import request.

Sampling controls the **annotation budget** — which traces from the available pool
are forwarded for human review. It is not about reducing evaluation coverage;
automated evaluation (checkr) runs on all traces. Sampling only decides what humans
see.

---

## Mental Model

| Concept | Definition |
|---|---|
| **Evaluation coverage** | How many traces checkr scores — default 100%, cost-knob only |
| **Annotation budget** | How many traces humans can review per day/week — the real constraint |
| **Sampling strategy** | How the annotation budget is allocated across trace categories |
| **Sampling rate** | A capture rule applied *within* a category, not globally added |

> **Key insight:** `20% random + 10% high-cost ≠ 30% total`.
> The same trace can match both criteria.
> Total = `|union(random_sample, high_cost_sample, ...)|` — typically less than the sum.
> If the union reaches 100%, disable all sampling and take everything.

---

## Observable Signals

Sampling criteria must come from **observable signals in the trace**, not manual intuition.
A mature trace contains:

```
trace_id          session_id        user_id           model
prompt_version    tool_calls        latency           cost
token_usage       retrieval_chunks  judge_scores      feedback
errors            span_tree         retry_count       fallback_used
```

Sampling = querying this dataset intelligently.

---

## Trace Taxonomy

Before choosing sampling percentages, classify traces into a taxonomy.
Sampling applied per bucket prevents dominant categories from consuming the entire budget.

```yaml
trace_type:
  - simple_answer          # single-turn, no tools
  - rag_answer             # retrieval-augmented
  - tool_execution         # one or more tool calls
  - multi_step_agent       # multi-turn with planning
  - recovery_flow          # failed + retried + succeeded
  - human_escalation       # transferred to human

intent:
  - support_question
  - data_analysis
  - code_generation
  - workflow_automation

risk:
  - low / medium / high

failure_mode:               # populated only when applicable
  - retrieval_miss
  - bad_tool_choice
  - loop
  - hallucination
  - schema_error
  - timeout
```

---

## New backend endpoint: schema discovery

```
GET /api/public/datasource/sample?datasource=xxx
  &start=...&end=...&session_id=...&trace_id=...&trace_type=...&input_hash=...
  &keys[]=a&keys[]=b&...     ← S3 only; omit for Langfuse/ClickHouse/Trino
```

Takes first 5 records **from within the user's active filter context**
(time range, session, trace type, etc. — same params as the search endpoint).
Returns a flat field→type map and example values. Drives which strategies the UI
renders as available.

**Datasource-type behaviour:**
- **S3** — reads first 5 keys via DuckDB (`LIMIT 1` per file for JSON; Parquet footer only).
  `keys[]` param required.
- **Langfuse** — calls `/api/public/traces` with active filters, `limit=5`. No keys needed.
- **ClickHouse / Trino / CHYT** — runs `SELECT * FROM table WHERE <filters> LIMIT 5`.
  No keys needed.

**Important:** the sample must be drawn from the filtered subset, not from all data.
If the user has already narrowed to a specific trace type or session, the discovered
schema should reflect that subset — not the full datasource schema.

---

## Sampling config data model

Each rule is **independent** — its `rate` is a percentage of the traces that match
its own condition, not a slice of a shared budget.

```python
class SamplingRule(BaseModel):
    strategy: str       # see strategy list below
    rate: float         # 0-100 — percentage of qualifying traces to include
    field: str | None   # field to evaluate (user picks from discovered schema)
    params: dict        # strategy-specific: threshold, percentile, tag_value, seed, etc.
                        # Random supports optional: seed (int) for reproducible sampling
```

`ExportRequest` gets these new optional fields:
- `sampling: list[SamplingRule] | None` — if `None` or empty, import all selected keys.
- `strict_schema: bool` — if `true`, traces whose field structure doesn't match the
  schema discovered during the sample step are silently dropped before sampling.
- `schema_snapshot: dict[str, str] | None` — field→type map captured at schema discovery
  time; required when `strict_schema=true` so the backend knows what to compare against.
- `max_traces: int | None` — optional hard cap on total output after union; applied last.

### Rate semantics

- `rate` applies to the pool of traces that satisfy the rule's condition.
- Random has no condition — `rate=20` means take 20% of ALL traces.
- High-Cost with `rate=10` means take 10% of traces where `cost > threshold`.
- Rates are independent: there is no constraint that they sum to any value.
- Expected yield ≈ sum of (rate × pool_fraction) for each rule, minus overlaps.

### The 100% warning

- If any single rule has `rate=100` → warn inline: "equivalent to no sampling".
- If the estimated union yield approaches 100% of input → warn: "your config will
  likely include all traces".

---

## Backend sampling execution (independent pools)

1. Load all candidate traces/records from source.
2. If `strict_schema=true`: drop any trace whose top-level fields don't match
   `schema_snapshot` (missing required fields or unexpected-only structure).
3. For each rule: filter the remaining set by its condition → pool.
4. Sample `rate%` of that pool independently (`random.sample`, using `seed` from
   `params` if provided for reproducibility).
5. Union all sampled IDs (dedup by `trace_id` / key).
6. If `max_traces` is set: truncate the union to that count (random subset of the union).
7. Proceed with upload using only the sampled set.

**Zero-result handling:** if the union after all steps is empty, the backend returns
a completed job with `files_uploaded=0` and a warning:
`"Sampling config produced 0 results — no traces matched the configured strategies."`
No error is raised; the dataset is created empty.

---

## UI flow in import modal

```
[ Dataset Name ]  [ Access ]  [ Type ]

[ Configure Sampling ▼ ]           ← button, expands block below

┌─ Sampling ───────────────────────────────────────────────────┐
│ Schema (from 5 samples):                                      │
│   latency: float   total_cost: float   tags: list   name: str │
│                                                               │
│ [x] Ignore traces with mismatched structure                   │
│  Max traces: [ no limit ]                                     │
│                                                               │
│ + Add Strategy  [ Random ▾ ]                                  │
│                                                               │
│  Random           rate: [20%]                          [✕]   │
│  High-Cost        rate: [10%]   field: [total_cost ▾]  [✕]   │
│  Latency Spike    rate: [ 5%]   field: [latency ▾]     [✕]   │
│                                                               │
│  Est. yield: ~30–35% of input traces                          │
└───────────────────────────────────────────────────────────────┘

[ Cancel ]  [ Import ]
```

- Each rate field is edited independently — no auto-balancing.
- Est. yield is a rough estimate shown for guidance, not enforced.
- Strategies whose required fields are absent in the schema are shown greyed out
  with tooltip "field not found in this datasource".

---

## Strategies available vs. datasource fields

### v1 (implemented)

| Strategy              | Enabled when field exists                             |
|-----------------------|-------------------------------------------------------|
| Random                | always                                                |
| High-Cost             | total_cost, totalCost, token_count, usage.totalTokens |
| Latency Spike         | latency, latency_ms, duration_ms                      |
| Long Trace            | span_count, observation_count, depth                  |
| Failure               | level, error, status_code, is_error                   |
| User Dissatisfaction  | score, tags (with thumbsdown/negative values)         |
| Business-Critical     | tags, metadata (user defines a tag/key value)         |
| Prompt/Version Change | version, model, prompt_hash, prompt_version           |
| Low Confidence        | confidence, logprob, score (as model output field)    |
| Weird Tool Sequences  | tool_calls, tools, tool_use, observations             |

### v2 (follow-on — see section below)

| Strategy             | Required signal |
|----------------------|-----------------|
| Retrieval Failure    | retrieval_similarity, chunk_score, retrieved_chunks |
| Recovery Behavior    | retry_count, fallback_used, final_success |
| Judge Disagreement   | human_score + llm_score (needs annotation history) |
| Drift Detection      | embedding distance from baseline distribution |
| Active Learning      | composite priority score (composite of all signals) |

---

## Strategy descriptions

### Random Sampling
**Purpose:** Baseline health monitoring.
**How it works:** Select random traces uniformly.
**Best for:** Overall quality estimation, regression monitoring, discovering unknown unknowns.
Without stratification by taxonomy bucket, high-traffic categories dominate — consider
filtering by `trace_type` or `model` before applying random sampling.
**Advantages:** Unbiased, simple, statistically meaningful.
**Weaknesses:** Misses rare failures, inefficient use of annotation budget.
**Typical usage:** 1-5% of production traffic; stratify by `model × workflow × tenant`.

### High-Cost Sampling
**Purpose:** Detect inefficient reasoning/tool usage.
**Signals:** `total_tokens > p95`, `cost_usd > threshold`, `tool_call_count > p95`,
`context_window_utilization > 80%`.
**Best for:** Agent optimization, cost reduction, loop detection.
**Advantages:** Catches pathological agent behavior.
**Weaknesses:** Expensive traces are not always bad.
**Params:** `field`, `percentile` (p75/p90/p95/p99).

### Latency Spike Sampling
**Purpose:** Diagnose slow reasoning.
**Signals:** `trace.latency > p99`, `slowest_span > threshold`,
`tool_wait_time / total_time > threshold`.
**Best for:** Real-time agents, interactive assistants.
**Advantages:** Connects quality with UX.
**Params:** `field`, `percentile`.

### Long Trace Sampling
**Purpose:** Detect loops and reasoning degradation.
**Signals:** `span_count > threshold`, `max_depth > threshold`,
repeated tool patterns, same prompt called repeatedly.
**Best for:** Autonomous agents, planning systems, tool-using agents.
**Advantages:** Exposes agent instability.
**Weaknesses:** Some legitimate workflows are naturally long.
**Params:** `field`, `threshold`.

### Failure Sampling
**Purpose:** Catch regressions before they compound.
**Signals:** `trace.status == "error"`, `tool.error_count > 0`,
`response.valid_json == false`, `retry_count > N`,
first occurrence of a new error pattern, failure rate spike.
**Best for:** Reliability monitoring, root cause analysis.
**Advantages:** Directly surfaces broken behavior.
**Weaknesses:** May over-index on known error patterns.
**Params:** `field`, `values` (comma-separated failure indicator values).

### User Dissatisfaction Sampling
**Purpose:** Capture explicit and implicit negative signals.
**Signals:** `thumbs_down == true`, `user_clicked_regenerate == true`,
same user repeating question within 5m, conversation abandoned after response,
transferred to human.
**Best for:** User-facing assistants, feedback-driven improvement.
**Advantages:** Aligns annotation with user impact.
**Params:** `field`, `threshold` (score below), `thumbsdown_tags`.

### Business-Critical Sampling
**Purpose:** Ensure coverage of high-stakes interactions regardless of other signals.
**Signals:** Workflow in critical list, enterprise tenant, contains payment/approval action.
**Best for:** Regulated domains, SLA-bound workflows, VIP users.
**Advantages:** Guarantees representation of important cases.
**Params:** `field`, `match_type` (contains/equals), `value`.

### Prompt/Version Change Sampling
**Purpose:** Regression detection across prompt or model versions.
**Signals:** `prompt_version != baseline_version`, `model_version changed`,
`retrieval_index_version changed`, `tool_hash changed`.
**Best for:** A/B prompt testing, model upgrades, prompt iteration.
**Advantages:** Directly targets changed behaviour, focuses annotation where regressions are likely.
Best practice: replay the **same inputs** through old and new system; sample the diffs.
**Weaknesses:** Only meaningful when version metadata is present.
**Params:** `field` (e.g. `model`), `baseline` (e.g. `gpt-4o`).

### Low Confidence Sampling
**Purpose:** Surface cases where the system was uncertain.
**Signals:** `judge.score_variance > threshold` (LLM judge fluctuates on re-runs),
`agent.confidence < threshold`, `retrieval_similarity < threshold`,
`retrieved_docs_conflict == true`.
**Best for:** Classification tasks, RAG pipelines, uncertainty-aware systems.
**Weaknesses:** Not all systems expose confidence; logprob is not always calibrated.
**Params:** `field`, `threshold`.

### Weird Tool Sequences Sampling
**Purpose:** Detect abnormal or pathological tool usage patterns.
**Signals:** Same tool called repeatedly, excessive total calls, policy-violating
tool sequence, required tool not called, parallel branches exceed threshold.
**Best for:** Tool-using agents, ReAct loops, multi-step planners.
**Weaknesses:** Requires structured tool call data; thresholds are system-specific.
**Params:** `field`, `max_repeat`, `min_total_calls`, `unexpected_tools`.

---

## Priority Scoring Pipeline (v2)

> Do not build one sampling strategy. Build a **scoring pipeline**.

Instead of applying rules independently and unioning results naively, assign each
trace a composite priority score and select the top N per budget window:

```
sampling_priority =
    failure_weight          * is_failure           +
    cost_weight             * is_high_cost         +
    novelty_weight          * novelty_score        +
    dissatisfaction_weight  * has_negative_signal  +
    risk_weight             * risk_level           +
    business_impact_weight  * is_critical_workflow +
    judge_disagreement_w    * judge_variance       +
    recovery_weight         * is_recovery_flow
```

Example weights for an agentic system:

| Signal | Score |
|---|---|
| High-risk workflow (payment, auth) | +50 |
| New / unseen trace category | +30 |
| Tool error | +25 |
| Judge score variance > 0.3 | +20 |
| P95 token cost | +15 |
| Recovery flow (failed → succeeded) | +15 |
| Retrieval failure | +15 |
| Random baseline (always eligible) | +5 |

Select top N traces per day sorted by score descending.

---

## Three-Layer Design (v2)

```
┌─────────────────────────────────────────────────────────────┐
│  Layer 1 — Always Include (no budget limit)                 │
│  · Critical failures (auth down, payment error)             │
│  · Policy / compliance events                               │
│  · Security-flagged traces                                  │
├─────────────────────────────────────────────────────────────┤
│  Layer 2 — Stratified Sampling (budget: ~60% of daily N)    │
│  · One sample per taxonomy bucket (trace_type × risk)       │
│  · Prevents dominant categories from eating the budget      │
│  · Baseline health coverage across all system behaviors     │
├─────────────────────────────────────────────────────────────┤
│  Layer 3 — Exploration (budget: ~40% of daily N)            │
│  · Novelty / outliers                                       │
│  · New tool sequences                                       │
│  · Drift-detected categories                                │
│  · Active learning top-N by composite priority score        │
└─────────────────────────────────────────────────────────────┘
```

---

## Example Annotation Budget Allocation (1,000 traces/day)

| Category | Count | Signal |
|---|---|---|
| Random stratified baseline | 300 | Health across all categories |
| Failures / suspicious | 250 | Regression detection |
| High-risk business flows | 150 | Business-critical coverage |
| High-cost / long traces | 100 | Agent degradation |
| Novelty / rare taxonomy | 100 | Blind-spot discovery |
| Recovery flows | 100 | Model improvement seeds |
| **Total** | **1,000** | |

This is **not** "X% of all traces". It is an **evaluation budget allocation** across
trace categories defined by the taxonomy.

---

## Practical Starting Configuration

For a new agentic system without annotation history yet:

| Signal | Initial Filter |
|---|---|
| Errors | any tool error |
| Cost | tokens > p95 |
| Length | span count > p95 |
| Recovery | retry_count > 0 |
| Novelty | unseen tool chain |
| Feedback | thumbs down |
| Retrieval | similarity < threshold |
| Regression | changed prompt/model version |

Start with these eight. Add taxonomy classification once enough traces are collected
to identify meaningful clusters. Migrate to full priority scoring in Phase 2.

---

## Maturity Phases

| Phase | Sampling maturity |
|---|---|
| Phase 1 (MVP) | Failure + cost + random; independent pools; manual budget cap |
| Phase 2 | Taxonomy classification; per-bucket quotas; retrieval failure + recovery signals |
| Phase 3 | Full priority scoring pipeline; active learning; drift-triggered resampling; judge disagreement |

---

## Resolved design decisions

1. **Percentile thresholds** (p95, p99) — computed over the full selected set at
   import time (not over the 5-sample preview).

2. **Business-Critical** — user selects a tag name + filter type (`contains` | `equals`) +
   value to match (e.g. `tags contains "critical"`, `tags equals "vip"`).

3. **S3 datasource** — resolved by DuckDB. Schema discovery uses
   `DESCRIBE SELECT * FROM read_parquet(...) LIMIT 0` (reads only the Parquet footer,
   no data scan). For JSON Lines, `SELECT * FROM read_json_auto(...) LIMIT 1`.
   Field-based sampling conditions and `USING SAMPLE n PERCENT (bernoulli)` are pushed
   into DuckDB at import time — no full file reads in Python. All strategies are
   available for S3, same as other datasource types.

4. **Random seed** — Random strategy supports an optional `seed` in `params` for
   reproducible imports (re-running with the same seed produces the same sample).

5. **Max traces cap** — `max_traces` is an optional hard ceiling applied after the union,
   useful when annotation budget is fixed regardless of input size.

6. **Zero results** — treated as a successful import of an empty dataset, not an error.
   A warning is surfaced in the job result.

---

## Follow-on feature: Span Slicing

Slicing is a separate dimension from sampling, applied after trace selection:

- **Sampling** = which traces to import (trace-level)
- **Slicing** = which spans within each selected trace to import (span-level)

They compose sequentially: sample traces → slice spans from each → import result.

### Why

A trace with 40 spans imported whole is expensive to annotate. Importing only
retrieval spans (2-3 per trace) gives the same diagnostic signal at a fraction
of the cost.

### Slice strategies

| Slice | Filter |
|---|---|
| Retrieval spans | `type=retrieval` or name matches `search/fetch/retrieve` |
| Tool decisions | `type=tool` or `type=tool-call` |
| Recovery attempts | `level=ERROR` or name matches `retry/fallback/recover` |
| Final synthesis | `type=generation` + last span, or name matches `synthesize/respond/answer` |

User picks one or more slice types; all matching spans across selected traces are
included in the imported dataset item.

### Data model (future)

```python
class SliceRule(BaseModel):
    slice_type: str      # "retrieval" | "tool_decision" | "recovery" | "final_synthesis" | "custom"
    field: str           # span field to match on (e.g. "type", "name")
    match: str           # value or pattern to match
```

`ExportRequest` would gain: `slices: list[SliceRule] | None`

### Datasource considerations

- **Langfuse** — observations fetched via separate `/api/public/observations` call per
  trace; feasible but N+1 at scale, needs batching.
- **ClickHouse/Trino** — depends on schema; spans may be rows in the same table
  (directly filterable) or nested JSON (requires unnesting).
- **S3** — spans typically nested inside trace JSON; DuckDB can unnest and filter.

### Deferred because

Span extraction requires per-datasource implementation work and a separate API call
pattern. Import modal UI also needs a second configuration block. Scoped out of v1
to avoid blocking sampling delivery.

---

## Follow-on: v2 signals

These signals are architecturally sound but require data that is not yet available
in the import pipeline (human annotation history, embedding index, multi-judge setup):

**Retrieval Failure** — `avg_chunk_similarity < threshold`, `response_claims_without_sources`,
`retrieved_chunks_not_referenced`. Needs retrieval metadata in trace.

**Recovery Behavior** — `retry_count > 0 AND final_success == true`. Excellent training
material — the system recovered from a failure. Needs `retry_count` and `final_success` fields.

**Judge Disagreement** — `abs(human_score - llm_score) > threshold`. Highest annotation
value: cases where the automated judge is least reliable. Requires prior annotation history.

**Drift Detection** — `current_intent_distribution != baseline_distribution`,
embedding centroid shift, new languages or seasonal behavior. Needs embedding index.

**Active Learning (Composite)** — `priority = uncertainty × w1 + novelty × w2 + business_impact × w3`.
Select top-N traces per time window by composite score. The full priority scoring
pipeline described above; target for Phase 3.

---

## Open questions

1. **Novelty strategy** — requires embedding/vector field (cosine distance to cluster
   centroids or similar). Needs a separate embedding index or pre-computed field.
   Decision needed: out of scope for v1, or add as a greyed-out placeholder in the UI?
