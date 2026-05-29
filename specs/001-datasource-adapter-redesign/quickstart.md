# Quickstart: Datasource Adapter Redesign

## What changes for API consumers

- **Schema discovery** (`GET /api/public/datasource/sample`): stop sending `keys[]`. S3 now derives
  its sample from the same filters as search. The response shape is unchanged
  (`{ "fields": {...}, "sample_count": N }`).
- **Proxy search over S3** (`POST /api/public/proxy/search`): now returns content-search results
  (matched records), not an object listing. Same `{ "results": [...], "backend": "s3" }` envelope.
- **`time_field`**: still a per-request param. ClickHouse/CHYT/Trino accept any safe column name;
  Langfuse and S3 accept only their canonical `timestamp` and return 400 for any other value.

## Adding a new backend (the payoff)

1. Add a `search_logs_<backend>()` function in `src/dataimporter/backends/<backend>.py`.
2. In `adapters.py`, subclass `BaseAdapter`:
   ```python
   class FooAdapter(BaseAdapter):
       _health_path = "/health"            # or override ping() if it needs auth
       async def search(self, query, filters, *, auth, limit=50):
           from dataimporter.backends.foo import search_logs_foo
           return await search_logs_foo(query, filters, auth=auth, limit=limit)
   ```
3. Register it: `_REGISTRY["foo"] = FooAdapter`.
4. Done. `sample()` and `ping()` are inherited; **no route file changes**. Search, schema discovery,
   and proxy all work through `get_adapter()`.

## Verifying the refactor

```bash
# 1. No per-type branches remain in the consuming routes
! grep -rn "ds.type ==" src/dataimporter/routes/{search,sample,proxy}.py

# 2. No backend-specific imports in routes
! grep -rn "from dataimporter.backends" src/dataimporter/routes/

# 3. Run the suite (TDD: failing tests committed first, then implementation)
uv run pytest tests/test_adapters.py tests/test_search_filters.py tests/test_sample.py -q

# 4. Full suite + performance guard
uv run pytest -q
```

## Key acceptance checks (from spec.md)

- **US1** — `grep` finds zero `ds.type ==` in the three routes; a throwaway registered backend is
  reachable from all three endpoints without route edits.
- **US2** — `ClickhouseAdapter`/`ChytAdapter` share one inherited `ping()`; no adapter restates the
  filter list.
- **US3** — same `time_field` request behaves consistently per the resolution table in
  `data-model.md`.
- **US4** — schema discovery over a range where late records add fields surfaces those fields.

## TDD order (Constitution §VII)

1. `data-model.md` + `contracts/openapi.yaml` (this plan) ✅
2. Write failing tests: `tests/test_search_filters.py`, `tests/test_adapters.py`, updated
   `tests/test_sample.py` — **commit them red**.
3. Implement `SearchFilters`, `BaseAdapter`, rewritten adapters, route edits — make tests green.
4. Refactor under green.
