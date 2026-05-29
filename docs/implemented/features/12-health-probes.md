# Feature: Health & Readiness Probes

## One-liner
Three probe endpoints for k8s lifecycle management: liveness (always 200), readiness (pings all datasources), and detailed health (per-datasource status + error messages).

## Problem
k8s needs separate signals for "is the process alive" (liveness) and "is it ready to serve traffic" (readiness). Ops teams need a richer endpoint that names which backend is degraded without exposing credentials.

## Implementation
- `src/dataimporter/main.py` — all three endpoints.
- `GET /livez` — always `{"status": "ok"}` 200. Never fails. Used by k8s liveness probe.
- `GET /ready` — calls `get_adapter(ds).ping()` for every configured datasource concurrently. Returns 200 if all OK, 503 if any degraded. Used by k8s readiness probe; traffic is withheld until 200.
- `GET /health` — same ping sweep; returns per-datasource status dict + `details` map with error strings on failure. `"disabled"` components excluded from the overall status calculation.
- Probe paths are silenced from uvicorn access logs when `silence_probes: true` (see [Structured Logging](10-structured-logging.md)).

```
GET /health →
{
  "status": "degraded",
  "components": { "ClickHouse": "ok", "S3": "degraded" },
  "details":    { "S3": "Connection refused" }
}
```

## Scope
- **In**: Liveness, readiness, health; per-datasource ping via adapter; probe log silencing.
- **Out**: Dependency version checks; Redis connectivity probe; dataset service target probe.

## Known gaps
- `/ready` and `/health` ping datasources sequentially in a `for` loop — slow when multiple datasources are configured. Should be `asyncio.gather()`.
- Redis availability is not checked by any probe endpoint.
