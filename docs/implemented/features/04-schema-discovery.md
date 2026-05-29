# Feature: Schema Discovery

## One-liner
Fetch 5 sample records from any datasource within the active filter context and return a flat field→type map with example values.

## Problem
The sampling UI needs to know which fields exist in the user's current filtered dataset before showing available strategies. A generic schema-discovery endpoint eliminates per-datasource hardcoding in the UI.

## Implementation
- `src/dataimporter/routes/sample.py` — `GET /api/public/datasource/sample`.
- Same filter params as search (`start`, `end`, `session_id`, `trace_id`, `trace_type`, `input_hash`).
- S3: `keys[]` param required; reads via `sampling.read_s3_traces_for_sampling` (first row per file, DuckDB).
- Other backends: dispatches via `get_adapter().search(limit=5)`.
- `_collect_fields()` recursively traverses nested dicts up to 3 levels, dot-joining keys.
- Returns: `{ "fields": { "body.cost": { "type": "float", "example": 0.012 }, ... } }`.

```python
class SampleResponse(BaseModel):
    fields: dict[str, dict]   # { "field.path": { "type": str, "example": Any } }
    backend: str
    sample_count: int
```

## Scope
- **In**: All 5 backends; recursive field traversal (max depth 3); private fields (leading `_`) excluded.
- **Out**: Array element schema inspection; type inference beyond 6 primitive types.

## Known gaps
- Sample is drawn from the first `limit=5` search results, not a random sample — for large datasets the schema may not represent rare fields.
