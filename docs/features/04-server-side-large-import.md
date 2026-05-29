# Feature: Server-Side Large Import (Re-query on Server)

## One-liner
Remove the 500-row import cap by re-running the search query server-side during import instead of relying on the events already displayed in the UI.

## Problem
The events-path import (`POST /export/dataset/events`) is limited to what the UI has fetched — capped at 500 rows by the search endpoint's `limit` param (default 100 in the UI). Importing more requires re-querying on the server with a higher limit or no limit, rather than sending the `currentEvents` array from the browser.

## Design decisions already made
- The server already has all the context needed for a re-query: datasource name, auth, query params (start/end, session_id, trace_id, query string).
- A new export variant should accept `query_params` instead of (or in addition to) `events`, run the search with a configurable `limit`, and stream results into the dataset service.
- Worker process is the right execution context (same arq queue, progress tracking already exists).
- Sampling rules should apply to the full re-queried set, not just the UI-visible subset.

## Scope
- **In**: New `POST /api/public/export/dataset/query` endpoint accepting search params + sampling config; server-side re-query with configurable `limit` (up to a reasonable server-set max); progress reporting via existing job status poll; apply sampling to full result set.
- **Out**: Streaming pagination for unlimited result sets (cap at a server-configured max, e.g. 50k rows); ClickHouse-specific "ignore user selection" path until the query model is unified.

## Open questions
- What is the safe server-side row cap? (Memory: 50k × ~2 KB avg = ~100 MB per job — fits within worker limit.)
- Should this replace the events-path endpoint or exist alongside it?
