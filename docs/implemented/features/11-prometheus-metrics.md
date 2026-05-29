# Feature: Prometheus Metrics

## One-liner
Multiprocess-safe Prometheus metrics for HTTP traffic, import operations, and search/S3 latency; scraped at `/metrics`.

## Problem
uvicorn runs multiple worker processes. Standard `prometheus_client` uses per-process registries — a single `/metrics` endpoint must aggregate across all workers. Import jobs run in a separate arq worker process that also needs metric exposure.

## Implementation
- `src/dataimporter/metrics.py` — custom metric definitions.
- `src/dataimporter/main.py` — multiprocess setup + `/metrics` endpoint + HTTP instrumentator.
- Multiprocess mode: `PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc`; `MultiProcessCollector` aggregates all worker files on each scrape.
- `prometheus_fastapi_instrumentator` adds HTTP metrics: `latency`, `request_size`, `response_size`, `requests` (labelled by route template, not raw path).
- Worker exposes its own metrics endpoint on `:9101` (separate `prometheus_client` HTTP server in `worker_entrypoint.py`).

### Custom metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `dataimporter_s3_list_seconds` | Histogram | — | S3 listing latency |
| `dataimporter_s3_list_errors_total` | Counter | — | S3 listing failures |
| `dataimporter_search_seconds` | Histogram | — | Search endpoint latency |
| `dataimporter_search_errors_total` | Counter | — | Search failures |
| `dataimporter_import_files_total` | Counter | `datasource`, `target`, `status` | Files processed |
| `dataimporter_import_bytes_total` | Counter | `datasource`, `target` | Bytes uploaded |
| `dataimporter_import_seconds` | Histogram | `datasource`, `target` | Import duration |

Import histogram buckets: `[1, 5, 15, 30, 60, 120, 300, 600]` seconds.

## Scope
- **In**: All metrics above; multiprocess aggregation; HTTP instrumentator; worker sidecar metrics on `:9101`.
- **Out**: Per-adapter search latency labels; alerting rules; Grafana dashboard definition (provisioned separately in the Docker Compose stack).

## Known gaps
- Search and S3 list metrics have no `datasource` label — cannot distinguish per-backend latency without adding a label.
- Worker metrics port `:9101` is hardcoded in `worker_entrypoint.py`.
