# Feature: Server-side Field Filtering

## One-liner
Post-query field filter applied to search results in Python before the response is returned, using a declarative rule set.

## Problem
Backend query languages (DuckDB SQL, Langfuse REST, Trino) have different filter capabilities. A unified post-query filter layer lets the UI apply consistent field-level predicates regardless of backend.

## Implementation
- `src/dataimporter/filters.py` — `FilterRule(field, op, value)` Pydantic model + `apply_filters()` + `parse_filters()`.
- Wired in `GET /api/public/logs/search` via `filters` query param (JSON-encoded array of rules).
- Supported ops: `eq`, `neq`, `contains`, `not_contains`, `starts_with`, `gt`, `lt`, `gte`, `lte`, `is_null`, `not_null`, .
- Dot-notation field traversal: `body.input`, `metadata.cost_usd`.
- Numeric ops fall back gracefully if the field value isn't numeric.

```python
class FilterRule(BaseModel):
    field: str   # dot-notation path
    op: str      # eq | neq | contains | not_contains | starts_with | gt | lt | gte | lte | is_null | not_null
    value: str | None = None
```

## Scope
- **In**: All ops above; dot-notation nested access; graceful no-op on type mismatch.
- **Out**: OR-combining multiple rules (currently AND-only); push-down to backend SQL.

## Known gaps
- Filters are applied in Python after the full result set is fetched — no push-down to ClickHouse/Trino/DuckDB. For large result sets this is inefficient.
- `op: last` / `op: between` from `JobConfig.FiltersConfig` are UI-side shorthands, not handled by this module.
- add `in`, `not in` ops
