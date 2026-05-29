# Feature: Request ID Tracing

## One-liner
Bind an incoming `x-request-id` header value to structlog's context-var store so every log line within that request carries `request_id` without explicit passing.

## Problem
In a concurrent async server, log lines from different requests interleave. Correlating all lines from one request requires a stable ID field present on every log line, injected once at the request boundary.

## Implementation
- `src/dataimporter/main.py` — `RequestIDMiddleware(BaseHTTPMiddleware)`.
- On each request: clears previous context-vars (`clear_contextvars`), then binds `request_id` if `x-request-id` header is present.
- Uses `structlog.contextvars` — context is async-local (task-scoped), safe for concurrent requests in the same event loop.
- Downstream code calls `structlog.get_logger(__name__)` normally — `request_id` is merged automatically by the `merge_contextvars` processor (see [Structured Logging](10-structured-logging.md)).

## Scope
- **In**: `x-request-id` header binding; async-safe context-var propagation; automatic merge into all log lines.
- **Out**: Request ID generation (server does not mint IDs — nginx/caller must supply them); trace propagation to upstream services (e.g. dataset service calls do not forward the ID).

## Known gaps
- Requests without an `x-request-id` header produce log lines with no `request_id` field — no fallback UUID is generated.
- Worker jobs have no request context — arq task logs carry no request ID.
