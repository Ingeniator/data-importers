# Sampling Feature Design

## Overview

Sampling is configured in the import modal (when user presses "Import Dataset").
Default: no sampling. User expands the block via "Configure Sampling" button.
Sampling executes on the backend based on the config sent with the import request.

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
    strategy: str       # "random" | "high_cost" | "latency_spike" | "long_trace"
                        # | "failure" | "user_dissatisfaction" | "business_critical"
                        # | "prompt_version_change" | "low_confidence" | "weird_tool_sequences"
                        # "novelty" — deferred, requires vector/embedding field (see open questions)
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

---

## Strategy descriptions

### Random Sampling
**Purpose:** Baseline health monitoring.
**How it works:** Select random traces uniformly.
**Best for:** overall quality estimation, regression monitoring, discovering unknown unknowns
**Advantages:** unbiased, simple, statistically meaningful
**Weaknesses:** misses rare failures, inefficient use of annotation budget
**Typical usage:** 1-5% of production traffic

### High-Cost Sampling
**Purpose:** Detect inefficient reasoning/tool usage.
**Signals:** high token count, many tool calls, long chains, excessive retries
**Best for:** agent optimization, cost reduction, loop detection
**Advantages:** catches pathological agent behavior
**Weaknesses:** expensive traces are not always bad
**Example:** sample if tokens > p95

### Long Trace Sampling
**Purpose:** Detect loops and reasoning degradation.
**Signals:** many spans, deep recursion, repeated actions, repeated prompts
**Best for:** autonomous agents, planning systems, tool-using agents
**Advantages:** exposes agent instability
**Weaknesses:** some legitimate workflows are naturally long

### Latency Spike Sampling
**Purpose:** Diagnose slow reasoning.
**Signals:** p95/p99 latency, stalled spans, slow retrieval, excessive planning
**Best for:** real-time agents, interactive assistants
**Advantages:** connects quality with UX

### Failure Sampling
**Purpose:** Capture error cases for debugging.
**Signals:** error flags, non-200 status codes, exception traces
**Best for:** reliability monitoring, root cause analysis
**Advantages:** directly surfaces broken behavior
**Weaknesses:** may over-index on known error patterns

### User Dissatisfaction Sampling
**Purpose:** Capture poor user experiences.
**Signals:** negative scores, thumbsdown tags, low ratings
**Best for:** user-facing assistants, feedback-driven improvement
**Advantages:** aligns annotation with user impact

### Business-Critical Sampling
**Purpose:** Ensure coverage of high-stakes interactions.
**Signals:** user-defined tag or metadata field/value
**Best for:** regulated domains, SLA-bound workflows, VIP users
**Advantages:** guarantees representation of important cases

### Prompt/Version Change Sampling
**Purpose:** Regression detection across prompt or model versions.
**Signals:** version, model, prompt_hash, or prompt_version field differs from a user-specified baseline value
**How it works:** user picks a field and a baseline value; traces where field ≠ baseline are the pool
**Best for:** A/B prompt testing, model upgrades, prompt iteration cycles
**Advantages:** directly targets changed behaviour, focuses annotation where regressions are likely
**Weaknesses:** only meaningful when version metadata is present in traces
**Params:** `field` (e.g. `model`), `baseline` (e.g. `gpt-4o`) — samples traces where field ≠ baseline

### Low Confidence Sampling
**Purpose:** Capture uncertain model outputs for review.
**Signals:** confidence, logprob, or model-output score below a threshold
**Best for:** classification tasks, generation with self-assessed uncertainty, RAG pipelines
**Advantages:** surfaces cases where the model itself signals doubt
**Weaknesses:** not all systems expose confidence; logprob is not always calibrated
**Params:** `field` (e.g. `confidence`), `threshold` (e.g. `0.5`) — samples traces where field < threshold

### Weird Tool Sequences Sampling
**Purpose:** Detect abnormal or pathological tool usage patterns.
**Signals:**
- same tool called more than N times in one trace (repetition/loop)
- total tool call count exceeds a threshold (excessive tool use)
- specific tool names present that are considered unexpected or dangerous
**Best for:** tool-using agents, ReAct loops, multi-step planners
**Advantages:** catches stuck agents, runaway loops, misrouted tool calls
**Weaknesses:** requires structured tool call data; what counts as "weird" depends on the system
**Params (user-defined):**
- `field` — field containing the tool calls list (e.g. `tool_calls`, `tools`, `observations`)
- `max_repeat` — flag traces where any single tool appears more than N times (e.g. `3`)
- `min_total_calls` — flag traces with more than N tool calls total (e.g. `10`)
- `unexpected_tools` — comma-separated list of tool names to flag if present (e.g. `delete_file,drop_table`)
Any one signal firing is sufficient to include the trace in the pool.

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

## Open questions

1. **Novelty strategy** — requires embedding/vector field (cosine distance to cluster
   centroids or similar). Needs a separate embedding index or pre-computed field.
   Decision needed: out of scope for v1, or add as a greyed-out placeholder in the UI?
