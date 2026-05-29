---
description: "Task list for Datasource Adapter Redesign"
---

# Tasks: Datasource Adapter Redesign

**Input**: Design documents from `/specs/001-datasource-adapter-redesign/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/openapi.yaml, quickstart.md

**Tests**: REQUIRED — Constitution §III (TDD) is non-negotiable. Test tasks precede implementation
and MUST be committed failing first (§VII Architecture Sequence). Following the existing suite's
style, backend functions are mocked at the route boundary (TestClient + dependency overrides).

**Organization**: Tasks grouped by user story (US1–US4 from spec.md) for independent delivery.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies on incomplete tasks)
- **[Story]**: US1–US4; Setup/Foundational/Polish carry no story label

## Path Conventions

Single service: `src/dataimporter/`, tests at `tests/` (flat files, per existing layout).

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Establish a known-good baseline before refactoring.

- [ ] T001 Run `uv run pytest -q` to confirm a green baseline and record `wc -l src/dataimporter/adapters.py` (currently ~187) for SC-003 comparison

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: The shared building blocks every user story depends on — the value object, the request
dependency, and the protocol/base class skeleton.

**⚠️ CRITICAL**: No user story work can begin until this phase is complete.

- [ ] T002 [P] Create frozen `SearchFilters` dataclass (start, end, session_id, trace_id, trace_type, input_hash, time_field) in src/dataimporter/search_filters.py
- [ ] T003 Add `get_search_filters()` FastAPI dependency with naive→UTC normalization (the `_utc` helper) in src/dataimporter/routes/deps.py (depends on T002)
- [ ] T004 Define `DatasourceAdapter` Protocol (`search`/`sample`/`ping`) and abstract `BaseAdapter` (abstract `search`, default `ping()` via `_health_path`, minimal `sample()` = `search("*", filters, limit=limit)`) in src/dataimporter/adapters.py (depends on T002)
- [ ] T005 [P] Write FAILING tests for `SearchFilters` + `get_search_filters` tz-normalization (naive datetimes become UTC; all fields pass through) in tests/test_search_filters.py — commit red

**Checkpoint**: `SearchFilters`, the dependency, and the protocol/base class exist; foundation ready.

---

## Phase 3: User Story 1 - Uniform dispatch with no per-type branches (Priority: P1) 🎯 MVP

**Goal**: All three routes dispatch exclusively through `get_adapter()`; S3 leaks in `sample.py`
and `proxy.py` are removed; the `keys[]` param is gone.

**Independent Test**: `grep` finds zero `ds.type ==` and zero `from dataimporter.backends` in the
three route files; a throwaway registered backend is reachable from search/sample/proxy with no
route edits.

### Tests for User Story 1 (write FIRST, commit failing) ⚠️

- [ ] T006 [P] [US1] FAILING test: no `ds.type ==` and no `from dataimporter.backends` imports in src/dataimporter/routes/{search,sample,proxy}.py (source-scan assertion) in tests/test_adapters.py
- [ ] T007 [P] [US1] FAILING test: a fake adapter registered in `_REGISTRY` is reachable via `/api/public/logs/search`, `/api/public/datasource/sample`, and `/api/public/proxy/search` without route edits, in tests/test_adapters.py
- [ ] T008 [P] [US1] Update tests/test_sample.py to drop `keys[]` expectations: remove `test_sample_s3_missing_keys_returns_400` / `test_sample_s3_limits_to_5_keys`; add a FAILING test that S3 sample derives keys from filters via `S3Adapter.sample` (mock `list_batch_keys` + `read_s3_traces_for_sampling`)
- [ ] T009 [P] [US1] FAILING test: `/api/public/proxy/search` for an S3 connection returns content-search results with `backend == "s3"` (mock `S3Adapter.search`) in tests/test_proxy.py

### Implementation for User Story 1

- [ ] T010 [US1] Rewrite the 5 concrete adapters to the new signature `search(query, filters, *, auth, limit)`, unpacking `SearchFilters` into the existing `search_logs_*` kwargs; set `_health_path` ("/ping" for ClickHouse+CHYT, "/v1/info" for Trino) and override `ping()` for Langfuse (`ping_langfuse`) and S3 (`ping_s3`), in src/dataimporter/adapters.py (depends on T004)
- [ ] T011 [US1] Add `S3Adapter.sample(filters, *, auth, limit)` that derives keys via `list_batch_keys(filters)` then reads first-row-per-key via `read_s3_traces_for_sampling` (non-bucketed for now; US4 upgrades), in src/dataimporter/adapters.py (depends on T010)
- [ ] T012 [US1] Edit src/dataimporter/routes/search.py to consume `get_search_filters` and dispatch purely via `get_adapter().search(...)` (remove inline tz-normalization) (depends on T003, T010)
- [ ] T013 [US1] Edit src/dataimporter/routes/sample.py: remove the `if ds.type == "s3"` branch and the `keys[]` Query param; call `get_adapter().sample(filters, auth=auth, limit=5)`; keep `_flatten_fields`/`_collect_fields`/`_infer_type` unchanged (depends on T003, T010, T011)
- [ ] T014 [US1] Edit src/dataimporter/routes/proxy.py: remove the `if ds.type == "s3"` branch; build `SearchFilters` from the request body and dispatch `get_adapter(ds).search(...)` for all types incl. S3 (depends on T010)
- [ ] T015 [US1] Remove the now-unused `list_objects_proxy` from src/dataimporter/s3.py and its import in proxy.py (verified only proxy.py referenced it) (depends on T014)

**Checkpoint**: MVP — every route dispatches through `get_adapter()` with no per-type branches; T006–T009 green.

---

## Phase 4: User Story 2 - Reduced boilerplate when adding/maintaining backends (Priority: P2)

**Goal**: Concrete adapters declare only backend-specific behavior; identical health checks are
inherited; adapter dispatch exports the required Prometheus metrics.

**Independent Test**: `ClickhouseAdapter` and `ChytAdapter` resolve to the same inherited `ping()`;
no adapter restates the filter list; `SEARCH_SECONDS`/`SEARCH_ERRORS` move on dispatch.

### Tests for User Story 2 (write FIRST, commit failing) ⚠️

- [ ] T016 [P] [US2] FAILING test: `ClickhouseAdapter.ping` and `ChytAdapter.ping` are the inherited `BaseAdapter.ping` (same function object), and no concrete adapter defines its own `search` filter-param list beyond `(query, filters, *, auth, limit)`, in tests/test_adapters.py
- [ ] T017 [P] [US2] FAILING test: a successful `adapter.search` increments `SEARCH_SECONDS` count and a raising backend increments `SEARCH_ERRORS`, in tests/test_adapters.py

### Implementation for User Story 2

- [ ] T018 [US2] Ensure ClickHouse/CHYT/Trino adapters carry no own `ping()` (inherited via `_health_path`) and contain only `search()`; confirm DRY in src/dataimporter/adapters.py (depends on T010)
- [ ] T019 [US2] Wire `SEARCH_SECONDS.time()` and `SEARCH_ERRORS.inc()` around `search()`/`sample()` dispatch (shared wrapper in `BaseAdapter` or at the route boundary), importing from src/dataimporter/metrics.py, in src/dataimporter/adapters.py (depends on T010)

**Checkpoint**: Adapters are minimal and metric-instrumented; T016–T017 green.

---

## Phase 5: User Story 3 - Consistent time_field behavior across backends (Priority: P2)

**Goal**: Every backend honors an explicit `time_field` or rejects it loudly; documented default
column applies when omitted (see data-model.md resolution table).

**Independent Test**: Same `time_field` request behaves per the resolution table; Langfuse/S3 return
400 for any non-`timestamp` value; CH/CHYT/Trino apply any safe column.

### Tests for User Story 3 (write FIRST, commit failing) ⚠️

- [ ] T020 [P] [US3] FAILING tests: CH/CHYT/Trino use `timestamp` when `time_field` omitted and the supplied safe column when provided (assert on generated SQL / forwarded kwarg), in tests/test_adapters.py
- [ ] T021 [P] [US3] FAILING tests: Langfuse and S3 return 400 when `time_field` is set to anything other than `timestamp`, and behave normally when omitted or `timestamp`, in tests/test_adapters.py

### Implementation for User Story 3

- [ ] T022 [US3] Edit src/dataimporter/backends/langfuse.py to accept `time_field` and honor only the canonical trace timestamp; signal unsupported values back to the adapter (depends on T010)
- [ ] T023 [US3] Add `time_field` validation in `LangfuseAdapter.search` and `S3Adapter.search`/`sample` — raise `HTTPException(400, ...)` for unsupported column names, in src/dataimporter/adapters.py (depends on T010, T022)
- [ ] T024 [US3] Confirm CH/CHYT/Trino default-column + safe-column override flows correctly through the `SearchFilters` unpack (no behavior change beyond passthrough); add regression assertions (depends on T010)

**Checkpoint**: `time_field` is honored-or-rejected on all five backends; T020–T021 green (SC-004).

---

## Phase 6: User Story 4 - Representative schema discovery (Priority: P3)

**Goal**: Schema discovery draws a time-bucketed spread instead of the first N records, so
late-appearing fields surface.

**Independent Test**: Over a range whose later records add fields, schema discovery returns those
fields; a range smaller than the bucket count degrades gracefully.

### Tests for User Story 4 (write FIRST, commit failing) ⚠️

- [ ] T025 [P] [US4] FAILING test: `BaseAdapter.sample` over a `[start,end]` range issues per-bucket `search(limit=1)` calls and merges/dedupes, surfacing fields present only in later buckets (mock `search`), in tests/test_sample.py
- [ ] T026 [P] [US4] FAILING test: range smaller than `limit` buckets (or missing start/end) degrades to a single `search` with no error, in tests/test_sample.py

### Implementation for User Story 4

- [ ] T027 [US4] Replace the minimal `BaseAdapter.sample()` with the time-bucketed implementation: split `[start,end]` into `limit` sub-windows, run `search("*", window_i, limit=1)` concurrently via `asyncio.gather`, merge de-duplicated records (fallback to single search when start/end absent), in src/dataimporter/adapters.py (depends on T004, T010)
- [ ] T028 [US4] Upgrade `S3Adapter.sample()` to bucket keys by `parse_key_meta` timestamp across the range and read one key per bucket via `read_s3_traces_for_sampling`, in src/dataimporter/adapters.py (depends on T011, T027)

**Checkpoint**: Schema discovery is representative; T025–T026 green (SC-005).

---

## Phase 7: Polish & Cross-Cutting Concerns

- [ ] T029 [P] Update docs/implemented/features/02-multi-backend-search.md and 04-schema-discovery.md to reflect the new protocol and the closed "Known gaps" (S3 bypasses, time_field, first-N sampling)
- [ ] T030 Run quickstart.md verification greps: `! grep -rn "ds.type ==" src/dataimporter/routes/{search,sample,proxy}.py` and `! grep -rn "from dataimporter.backends" src/dataimporter/routes/`
- [ ] T031 Verify SC-003 (`wc -l src/dataimporter/adapters.py` roughly halved vs. T001 baseline) and run full `uv run pytest -q` green incl. tests/test_performance.py
- [ ] T032 [P] Final refactor/dead-code removal under green tests (confirm `list_objects_proxy` fully gone, no stale imports)

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: none — start immediately
- **Foundational (Phase 2)**: depends on Setup — BLOCKS all user stories
- **User Stories (Phase 3–6)**: all depend on Foundational
  - US1 (P1) is the MVP and the structural core; US2/US3/US4 build on the rewritten adapters from US1
  - US2, US3, US4 are independently testable but all touch `adapters.py`, so coordinate edits if run in parallel
- **Polish (Phase 7)**: depends on all targeted stories

### User Story Dependencies

- **US1 (P1)**: after Foundational. Delivers the MVP.
- **US2 (P2)**: after US1 (operates on the rewritten adapters)
- **US3 (P2)**: after US1 (validates time_field on the new signatures)
- **US4 (P3)**: after US1 (replaces the minimal `sample()` with bucketing)

### Within Each User Story

- Tests written and FAILING before implementation (§III, §VII)
- Model/dependency (Foundational) → adapters → routes → backend edits → refactor

### Parallel Opportunities

- T002 and T005 in Foundational
- All `[P]` test tasks within a story (T006–T009; T016–T017; T020–T021; T025–T026) run together
- T029 and T032 in Polish
- **Caution**: T010, T011, T018, T019, T023, T024, T027, T028 all edit `src/dataimporter/adapters.py` — these are NOT parallel with each other.

---

## Parallel Example: User Story 1

```bash
# Launch all US1 tests together (commit them failing first):
Task: "FAILING source-scan test for no ds.type branches in tests/test_adapters.py"        # T006
Task: "FAILING fake-backend reachability test in tests/test_adapters.py"                   # T007
Task: "Update S3 sample tests (drop keys[], derive from filters) in tests/test_sample.py"  # T008
Task: "FAILING proxy-S3 content-search test in tests/test_proxy.py"                         # T009
```

---

## Implementation Strategy

### MVP First (User Story 1 only)

1. Phase 1 Setup → 2. Phase 2 Foundational → 3. Phase 3 US1 → **STOP & VALIDATE** (grep for zero
   branches; suite green) → demo. This alone delivers the core promise (uniform dispatch, S3 leaks
   closed, `keys[]` removed, proxy-S3 unified).

### Incremental Delivery

1. Foundational ready
2. US1 → test → demo (MVP)
3. US2 (boilerplate + metrics) → test → demo
4. US3 (time_field) → test → demo
5. US4 (representative sampling) → test → demo

---

## Notes

- `[P]` = different files, no dependencies. Most `adapters.py` tasks are serial by necessity.
- Adapters translate `SearchFilters` → existing `search_logs_*` kwargs, so backend signatures stay
  mostly unchanged (only Langfuse changes, for `time_field`). This minimizes churn/risk.
- Verify each test fails before implementing; commit failing tests before the implementation commit
  (§VII / Quality Gate "Tests first").
- Commit after each task or logical group.

## Architecture Sequence (Constitution §VII)

Model → API Contract → Failing Tests → Implementation → Refactor:
1. `data-model.md` (`SearchFilters`, `BaseAdapter`) — done in /speckit-plan ✅
2. `contracts/openapi.yaml` — done in /speckit-plan ✅
3. Failing tests — T005, T006–T009, T016–T017, T020–T021, T025–T026
4. Implementation — T010–T015, T018–T019, T022–T024, T027–T028
5. Refactor — T031–T032

No UI elements are added, so the §V `data-testid` sub-task requirement does not apply.
