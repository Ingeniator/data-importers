# Feature: Timestamp Filter + Search

## One-liner
Combine a free-text search query with a timestamp range filter in a single UI interaction and API call, across all backends.

## Problem
Today the UI exposes either a time-range browse (S3 file listing) or a full-text search, but not both together in a coherent way. Users who want "all errors in the last 24 hours" have to either post-filter results or use backend-specific workarounds.

## Design decisions already made
- `GET /api/public/logs/search` already accepts `start` and `end` params alongside `query`.
- All adapters receive `start`/`end` — ClickHouse, Trino, Langfuse pass them as SQL/API filters; S3 uses them for key-prefix scoping.
- The UI date-range picker exists for S3 browse mode — reuse it for the search path.

## Scope
- **In**: UI date-range picker visible and active on the search tab; `start`/`end` always sent with search requests; adapter-level timestamp push-down verified for all 5 backends.
- **Out**: Per-field date filters (e.g. `span.end_time`); relative time expressions ("last 7 days" shortcut — deferred).

## Open questions
- Should the time range default to "last 24 hours" or "no filter" when switching to search mode?
- ClickHouse/Trino: which column is used for the timestamp filter — `timestamp`, `created_at`, or configurable per-datasource?
