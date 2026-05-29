# Feature: Sampling Engine v1

## One-liner
Apply one or more independent sampling strategies to a trace set before import, selecting a budget-controlled annotation subset via union of strategy pools.

## Problem
Importing all traces for human annotation is cost-prohibitive. A configurable sampling layer lets users direct the annotation budget toward high-value traces (failures, high-cost, version changes) rather than random noise.

## Implementation
- `src/dataimporter/sampling.py` — `SamplingRule`, `apply_sampling()` (events path), `apply_sampling_s3()` (S3 key path).
- `src/dataimporter/importer.py` — both `run_import_dataset` and `run_import_dataset_events` call sampling before upload.
- 9 strategies: `random`, `high_cost`, `latency_spike`, `long_trace`, `failure`, `user_dissatisfaction`, `business_critical`, `prompt_version_change`, `low_confidence`, `weird_tool_sequences`.
- Each rule independently filters its pool and samples `rate%` via `random.sample` (optional `seed` in `params`).
- Union of all sampled trace IDs; optional `max_traces` hard cap after union.
- `strict_schema=True` + `schema_snapshot` drops traces whose top-level fields don't match the captured schema.
- Zero-result union → empty dataset + `sampling_warning` in job result (not an error).

```python
class SamplingRule(BaseModel):
    strategy: str       # one of 9 strategies
    rate: float         # 0–100 % of qualifying pool
    field: str | None
    params: dict        # percentile, threshold, baseline, seed, etc.
```

## Scope
- **In**: All 9 v1 strategies; S3 path (DuckDB field reads); events path (in-memory); strict schema; max_traces cap; reproducible seed.
- **Out**: v2 strategies (retrieval failure, judge disagreement, drift, active learning — see `docs/sampling.md`); span slicing (planned `docs/features/06-span-slicing.md`).

## Known gaps
- Percentile thresholds computed over the full selected set at import time — not over the 5-sample preview, which may differ significantly.
- S3 path reads the first record of each file for field values; files with the relevant field only in later records may be misclassified.
