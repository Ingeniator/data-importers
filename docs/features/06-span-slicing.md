# Feature: Span Slicing

## One-liner
After trace sampling, filter which spans within each selected trace to import — reducing annotation cost by importing only the relevant span types (retrieval, tool decisions, recovery, final synthesis).

## Problem (from docs/sampling.md — Follow-on: Span Slicing)
Importing a full trace with 40 spans is expensive to annotate. In most evaluation scenarios only 2–3 specific span types carry the diagnostic signal. Span slicing composes with sampling: sample which traces → slice which spans from each.

## Design decisions already made
- Ordering: `sample traces → slice spans → import result` (sequential, not interleaved).
- Slice types (v1): `retrieval`, `tool_decision`, `recovery`, `final_synthesis`, `custom`.
- Data model:
  ```python
  class SliceRule(BaseModel):
      slice_type: str   # "retrieval" | "tool_decision" | "recovery" | "final_synthesis" | "custom"
      field: str        # span field to match on (e.g. "type", "name")
      match: str        # value or regex pattern
  ```
- `ExportRequest` gains `slices: list[SliceRule] | None` — `None` = import full traces.
- UI: second collapsible config block in import modal, below sampling block (`#span-slicing-section`).

## Datasource considerations
- **Langfuse** — observations fetched via `/api/public/observations` per trace (N+1; needs batching).
- **ClickHouse/Trino** — depends on schema; spans may be rows or nested JSON (unnesting required).
- **S3** — spans nested in trace JSON; DuckDB unnests and filters inline.

## Scope
- **In**: `SliceRule` model + `slices` field on `ExportRequest`; worker applies slicing after sampling; S3 + Langfuse implementations; UI slice config block with test-IDs.
- **Out**: ClickHouse/Trino slicing (nested JSON unnesting deferred); per-span annotation UI changes.

## Open questions
- For Langfuse, should batching use `/api/public/observations?traceId[]=...` bulk param or sequential per-trace calls?
- Should a sliced import preserve the original trace structure with non-matching spans removed, or export matching spans as flat records?
