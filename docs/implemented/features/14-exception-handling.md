# Feature: Exception Handling

## One-liner
Layered exception handling: FastAPI HTTP exceptions for client errors, per-handler try/except with structlog for operational errors, and uvicorn's default 500 for unhandled exceptions.

## Problem
The service communicates with five external backends and one dataset service — all can fail independently. Errors must be surfaced to the client with appropriate HTTP status codes while being logged with enough context to diagnose root cause.

## Implementation

Three layers:

**1. FastAPI `HTTPException`** — raised explicitly in route handlers and dependency resolvers for expected client errors (404 datasource not found, 400 unsupported type, 401 missing auth). FastAPI serialises these as `{"detail": "..."}` with the given status code.

**2. Per-handler try/except** — import and export paths catch backend exceptions per-item (e.g. per S3 key in `importer.py`) and accumulate them in a `failed: list[dict]` result rather than aborting the whole job. Failed items are logged via `logger.warning("file_export_failed", key=key, error=str(e))`.

**3. FastAPI/Starlette default handler** — any unhandled exception becomes a 500 with `{"detail": "Internal Server Error"}`. The traceback is logged by uvicorn (plain text, not structlog JSON).

Pydantic `ValidationError` on request bodies is handled by FastAPI's built-in 422 handler — returns field-level error details.

## Scope
- **In**: HTTP 4xx for client errors; per-item error accumulation in import jobs; Pydantic 422 validation errors; structlog `format_exc_info` in ad-hoc warning/error calls.
- **Out**: Global structured exception middleware; error classification (transient vs. permanent); client-visible retry hints; Sentry/error-tracker integration.

## Known gaps
- No global `@app.exception_handler(Exception)` — unhandled exceptions are logged by uvicorn in plain text, not as structured JSON by structlog.
- Backend connection errors during search are not caught at the route level — a ClickHouse timeout results in a 500 with no datasource context in the response.
- Worker task failures set the arq job status to `failed` but the error detail is only in logs; the `/export/status/{job_id}` response returns minimal context.
