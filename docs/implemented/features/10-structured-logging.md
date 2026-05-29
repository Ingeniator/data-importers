# Feature: Structured Logging

## One-liner
JSON structured logging via structlog with per-request context binding, configurable debug mode, and optional probe-path silencing.

## Problem
Plain text logs are hard to query in production log aggregators (Loki, Datadog, CloudWatch). Correlation across requests requires a stable request ID field. Health-check noise from k8s probes pollutes log streams.

## Implementation
- `src/dataimporter/logging_config.py` — `setup_logging(debug, silence_probes)` called at process startup.
- Processor chain: `merge_contextvars` → `filter_by_level` → `add_logger_name` → `add_log_level` → `PositionalArgumentsFormatter` → `TimeStamper(iso)` → `StackInfoRenderer` → `format_exc_info` → `UnicodeDecoder` → `JSONRenderer` (prod) / `ConsoleRenderer` (debug).
- `format_exc_info` serialises exception tracebacks into the `exception` JSON field — no manual `exc_info=True` needed in call sites.
- `silence_probes=True` (default): `SilenceProbesFilter` suppresses uvicorn access log lines for `/livez`, `/ready`, `/health`, `/metrics`.
- `debug=True`: switches renderer to `ConsoleRenderer` (coloured, human-readable) and sets log level to `DEBUG`.
- `structlog.contextvars` carries `request_id` across async boundaries within a request (see [Request ID Tracing](13-request-id-tracing.md)).

## Scope
- **In**: Structlog JSON output; ISO timestamps; exception serialisation; probe silencing; debug mode toggle.
- **Out**: Log sampling / rate limiting; per-logger level overrides at runtime; log shipping configuration.

## Known gaps
- No global exception handler — unhandled exceptions bubble up to FastAPI's default handler and are logged by uvicorn, not structlog. Structured `error` fields are only present where code explicitly calls `logger.exception()` or `logger.warning(..., exc_info=True)`.
- Worker process (`worker_entrypoint.py`) calls `setup_logging()` independently — log config is not shared with the API server process.
