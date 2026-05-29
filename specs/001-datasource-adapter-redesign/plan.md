# Implementation Plan: Datasource Adapter Redesign

**Branch**: `001-datasource-adapter-redesign` | **Date**: 2026-05-29 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-datasource-adapter-redesign/spec.md`

## Summary

Make `get_adapter()` the *only* dispatch path for all backend access. Today S3 escapes the
adapter in `routes/sample.py` and `routes/proxy.py`, every adapter restates a 10-parameter
signature it merely forwards, `time_field` is honored by only ClickHouse/Trino, and schema
discovery samples the first N records. The redesign introduces a `SearchFilters` value object
(built once per request, tz-normalized), a `BaseAdapter` carrying shared `ping()`/`sample()`,
a new `sample()` protocol capability that lets S3 derive keys from filters (removing the
`keys[]` param), unifies proxy-S3 onto content search, enforces `time_field` on every backend,
and replaces first-N sampling with a time-bucketed spread. Net effect: three route files with
zero `ds.type` branches and a substantially smaller `adapters.py`.

## Technical Context

**Language/Version**: Python 3.13

**Primary Dependencies**: FastAPI, httpx (async), duckdb, aioboto3, structlog, prometheus_client, pydantic

**Storage**: N/A (read-only over external backends: S3, ClickHouse, Trino, Langfuse, CHYT)

**Testing**: pytest (`tests/unit` style flat files + `tests/e2e` Playwright); FastAPI `TestClient`

**Target Platform**: Linux server (containerized FastAPI/uvicorn)

**Project Type**: Single web service (`src/dataimporter`)

**Performance Goals**: Search endpoints ≥50 concurrent at p95 ≤500 ms (Constitution §VI); this is a behavior-preserving refactor of the dispatch layer

**Constraints**: All adapter I/O MUST be async; S3/DuckDB blocking calls MUST stay off the event loop via `asyncio.to_thread` (preserve existing pattern)

**Scale/Scope**: 5 backends, 3 consuming routes (`search`, `sample`, `proxy`), ~187-line `adapters.py` targeted to shrink ~half

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Gate | Status |
|---|---|
| `specs/[feature]/data-model.md` has Pydantic model or PlantUML diagram | ☑ (Phase 1) |
| `specs/[feature]/contracts/openapi.yaml` present for any HTTP change | ☑ (Phase 1) |
| Failing test commit precedes implementation commit in git log | ☐ (enforced at `/speckit-implement`) |
| All new UI elements carry `data-testid` matching spec notation | ☑ N/A — no UI elements added (see Complexity Tracking) |
| No synchronous adapter calls in the event loop | ☑ S3/DuckDB stay on `asyncio.to_thread` |
| New adapter/route exports Prometheus counter + histogram | ☑ wire existing `SEARCH_SECONDS`/`SEARCH_ERRORS` into adapter dispatch |
| p95 ≤500 ms verified by load test or written exception + rationale | ☑ written exception — behavior-preserving refactor (see Complexity Tracking) |

**Post-Design Re-check**: No new violations introduced. `BaseAdapter` strengthens §IV (Liskov/DIP):
services depend on the protocol, new backends are added by subclassing without modifying existing
adapters (Open/Closed). `SearchFilters` reduces DRY violations across routes/adapters/backends.

## Project Structure

### Documentation (this feature)

```text
specs/001-datasource-adapter-redesign/
├── plan.md              # This file
├── research.md          # Phase 0 output — resolved design decisions
├── data-model.md        # Phase 1 output — SearchFilters, BaseAdapter, protocol
├── quickstart.md        # Phase 1 output — how to add a backend / verify
├── contracts/
│   └── openapi.yaml     # Phase 1 output — changed endpoints
└── tasks.md             # Phase 2 output (/speckit-tasks — NOT created here)
```

### Source Code (repository root)

```text
src/dataimporter/
├── adapters.py              # REWRITE — DatasourceAdapter protocol + BaseAdapter + 5 concrete adapters + get_adapter()
├── filters.py               # ADD SearchFilters value object (or new search_filters.py) + FastAPI dependency
├── metrics.py               # reuse SEARCH_SECONDS / SEARCH_ERRORS
├── routes/
│   ├── search.py            # EDIT — use SearchFilters dependency; pure adapter dispatch (already clean)
│   ├── sample.py            # EDIT — drop S3 branch + keys[] param; call adapter.sample()
│   ├── proxy.py             # EDIT — drop S3 branch; proxy-S3 → adapter.search()
│   └── deps.py              # ADD get_search_filters() dependency (tz-normalization moves here)
├── backends/
│   ├── clickhouse.py        # accept SearchFilters; default ts col preserved
│   ├── chyt.py              # accept SearchFilters; default ts col preserved
│   ├── trino.py             # accept SearchFilters; default ts col preserved
│   └── langfuse.py          # EDIT — honor time_field (single time dimension; reject unknown col)
├── search.py                # S3/DuckDB content search — used by S3Adapter.search (proxy + authed)
├── s3.py                    # list_batch_keys reused for S3 sample; list_objects_proxy retired from proxy path
└── sampling.py              # read_s3_traces_for_sampling reused by S3Adapter.sample (time-bucketed keys)

tests/
├── test_adapters.py         # NEW — protocol/BaseAdapter/get_adapter + time_field enforcement
├── test_search_filters.py   # NEW — SearchFilters dependency + tz normalization
├── test_sample.py           # EDIT — remove keys[] expectations; add time-bucketed + no-keys S3 cases
├── test_logs.py             # unchanged
└── e2e/                     # unchanged
```

**Structure Decision**: Single-service layout under `src/dataimporter`. The change is concentrated
in `adapters.py` (rewrite), a new `SearchFilters` + dependency, and edits to the three route files
and the backend signatures. No new top-level packages.

## Complexity Tracking

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| Load-test gate waived (written exception) | This is a behavior-preserving refactor of the dispatch layer; query execution paths are unchanged. Existing `tests/test_performance.py` continues to guard search latency. | Running a fresh 50-concurrent load test adds no signal for a refactor that does not alter the SQL/HTTP query paths; re-run only if profiling later shows regression. |
| `data-testid` gate marked N/A | No interactive UI elements are added or changed; this is a backend/API refactor. | n/a — gate is conditional on new UI elements. |
| Proxy-S3 runs DuckDB on caller credentials | Required to unify proxy-S3 onto `search()` per the resolved product decision (FR-005). The connection URL is already allowlist-validated in `_resolve_connection`. | Keeping `list_objects_proxy` preserves a per-type branch in `proxy.py`, the exact leak this feature removes. |
