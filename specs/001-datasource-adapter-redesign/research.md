# Phase 0 Research: Datasource Adapter Redesign

Resolves the implementation decisions deferred from the spec. The three product decisions (proxy-S3
→ content search, time_field per-request + enforced, time-bucketed sampling) were already settled
with the user during specification; this document records the *technical* decisions those imply.

## Decision 1 — `BaseAdapter` shape: abstract method vs. function-pointer registry

**Decision**: `BaseAdapter` is an abstract base. Concrete adapters override one backend-specific
method, `search(query, filters, *, auth, limit) -> list[dict]`, and declare a class attribute
`_health_path` for the default `ping()`. `sample()` and (HTTP-GET) `ping()` are inherited.

**Rationale**:
- Aligns with Constitution §IV: "all adapters MUST implement `BaseAdapter`", "add variants via new
  adapter; never modify existing adapters" (Open/Closed), "services depend on `BaseAdapter`".
- An abstract method is more discoverable and type-checkable than a `_search_fn` callable stored on
  the instance, and matches the existing class-per-backend idiom.
- `runtime_checkable` `DatasourceAdapter` Protocol is retained for structural typing at the route
  boundary; `BaseAdapter` provides the concrete shared implementation.

**Alternatives considered**:
- *Function-pointer registry* (`type → (search_fn, health_path)`): fewer lines, but scatters
  backend behavior between a dict and free functions, weakening §IV substitutability and making
  per-backend overrides (Langfuse ping, S3 sample) awkward.
- *Keep Protocol-only, no base class*: leaves the `ping()` duplication and forces every adapter to
  re-implement `sample()`.

**ping() handling**: `ClickhouseAdapter` and `ChytAdapter` share the inherited `ping()` with
`_health_path = "/ping"`; `TrinoAdapter` sets `_health_path = "/v1/info"`. `LangfuseAdapter` and
`S3Adapter` override `ping()` (they need auth / `ping_s3`), so `_health_path` stays `None` for them.

## Decision 2 — `SearchFilters` value object and where tz-normalization lives

**Decision**: A frozen dataclass `SearchFilters` carries `start, end, session_id, trace_id,
trace_type, input_hash, time_field`. A FastAPI dependency `get_search_filters()` (in
`routes/deps.py`) builds it from query params and performs the naive→UTC normalization currently
copy-pasted in `search.py`, `sample.py`, and `logs.py`. The proxy path constructs `SearchFilters`
directly from its request body (it already normalizes in `proxy_search`).

**Rationale**:
- Collapses the repeated 7-field signature across 3 routes, 5 adapters, and 5 backend functions to
  a single object (DRY, §IV).
- Centralizing tz-normalization removes four duplicated `if x.tzinfo is None` blocks.
- `frozen=True` matches the existing `Datasource`/`Settings` dataclass style and keeps filters
  immutable through the call chain.

**Alternatives considered**:
- *Pydantic model*: heavier; filters are internal plumbing, not a validated API body. A dataclass +
  the existing per-param `Query()` declarations preserve FastAPI's automatic OpenAPI doc generation
  for the query string. (Kept individual `Query()` params in the route signature; the dependency
  assembles them into `SearchFilters`.)

## Decision 3 — Enforcing `time_field` across all five backends

**Decision**: `time_field` selects the timestamp column on backends that have a real column
(ClickHouse, CHYT, Trino — already implemented, default `"timestamp"`, validated by `_SAFE_COL_RE`).
For single-time-dimension backends:
- **Langfuse**: the only time dimension is the trace timestamp (`fromTimestamp`/`toTimestamp`).
  `time_field` defaults to the sentinel `"timestamp"`; any *other* value raises a clear 400 from the
  adapter ("time_field 'X' not supported for langfuse; only 'timestamp'") rather than being silently
  ignored.
- **S3**: the only time dimension is the key-embedded timestamp parsed by `parse_key_meta`. Same
  rule — default `"timestamp"`, non-default value → 400.

**Rationale**: "Honor or reject, never silently ignore" makes behavior consistent and testable
(FR-008, SC-004). Backends with a single fixed time dimension cannot meaningfully apply an arbitrary
column, so a loud rejection is the honest contract.

**Default timestamp columns (documented)**:

| Backend | Default time column | Override supported? |
|---|---|---|
| ClickHouse | `timestamp` | yes (any safe column name) |
| CHYT | `timestamp` | yes |
| Trino | `timestamp` | yes |
| Langfuse | trace `timestamp` | no (400 on other values) |
| S3 | key-embedded `timestamp` | no (400 on other values) |

**Alternatives considered**:
- *Silently apply the canonical column* for Langfuse/S3: rejected — reproduces the current "passed
  through but ignored" confusion the feature exists to fix.

## Decision 4 — Time-bucketed sampling for schema discovery

**Decision**: `BaseAdapter.sample(filters, *, auth, limit)` splits the `[start, end]` window into
`limit` equal sub-windows and runs `search("*", filters_i, limit=1)` per sub-window concurrently
(`asyncio.gather`), then merges de-duplicated records. If `start`/`end` are absent, it degrades to a
single `search("*", filters, limit=limit)` (current behavior). `S3Adapter.sample()` overrides:
derive keys from `list_batch_keys(filters)`, bucket them by `parse_key_meta` timestamp across the
range, pick one key per bucket, and read first-row-per-key via `read_s3_traces_for_sampling`.

**Rationale**:
- Spreads the sample across the filter range so fields appearing only in later records surface
  (FR-009, SC-005), while keeping cost bounded: `limit` is small (default 5), so at most 5 light
  `limit=1` queries run, concurrently.
- Reuses existing machinery (`list_batch_keys`, `read_s3_traces_for_sampling`) so S3 sampling now
  derives keys from filters exactly like S3 *search* does — eliminating the `keys[]` param.

**Bucket count**: equal to `limit` (so each bucket contributes ~1 record). With `limit=5` this is 5
buckets. No new tuning knob is exposed; bucket count is an internal function of `limit`.

**Edge cases**:
- Range smaller than `limit` buckets, or single-bucket range → buckets collapse; duplicates removed
  by record identity (`trace_id`/`id`/`_key`), so the result is simply the available records.
- Empty match → empty record list → empty field map (unchanged contract).

**Alternatives considered**:
- *`ORDER BY rand() LIMIT n`* on SQL backends: most statistically representative but a full random
  sort can be expensive on large tables; rejected for cost.
- *Keep first-N*: rejected by the user during specification.

## Decision 5 — Proxy-S3 unification

**Decision**: `proxy.py` drops its `if ds.type == "s3"` branch. Proxy search dispatches through
`get_adapter(ds).search(...)` for every type, including S3, which runs DuckDB content search via the
existing `S3Adapter.search` path (keys derived from `list_batch_keys`, content scanned by
`search_logs`). `list_objects_proxy` is no longer used by the proxy route.

**Rationale**: FR-005 / the user's "Unify to search()" decision. The connection URL is already
allowlist-validated in `_resolve_connection`, so running DuckDB with caller credentials carries the
same trust boundary as the existing proxy ping. Removes the last per-type branch from `proxy.py`.

**Note**: `list_objects_proxy` remains in `s3.py` for now (not deleted) in case other callers exist;
a follow-up cleanup can remove it once confirmed unreferenced. (Verified: only `proxy.py` imports it
today, so it can be deleted in the same change — captured as a task.)

## Metrics (Constitution §VI)

`SEARCH_SECONDS` (histogram) and `SEARCH_ERRORS` (counter) already exist in `metrics.py` but are not
currently wired into the search route. The redesign wires adapter `search()`/`sample()` dispatch
with `SEARCH_SECONDS.time()` and `SEARCH_ERRORS.inc()` on failure, satisfying the "new adapter/route
exports counter + histogram" gate without inventing new metric names.
