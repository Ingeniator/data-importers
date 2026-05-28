# dataimporter — Architecture

## Overview

dataimporter is a FastAPI middleware service that bridges multiple LLM trace data stores (S3, ClickHouse, Trino, Langfuse, CHYT) to a downstream "dataset service." Its two main jobs are:

1. **Browse / search** — expose a unified API over heterogeneous backends so a UI can list and full-text-search LLM traces.
2. **Import / export** — copy a user-selected subset of traces (with optional sampling) into the dataset service for evaluation.

---

## Process topology

```
  ┌────────────┐   HTTP   ┌─────────────────┐
  │  nginx /   │ ───────► │  API server      │  :5001
  │  UI / SDK  │          │  (FastAPI/uvicorn)│
  └────────────┘          └────────┬────────┘
                                   │ enqueue_job (arq)
                              ┌────▼─────┐
                              │  Redis   │
                              └────┬─────┘
                                   │ dequeue
                          ┌────────▼────────┐
                          │  Worker process  │  metrics :9101
                          │  (arq, max=1)    │
                          └─────────────────┘
```

- **API server** (`entrypoint.py` → `main.py`): uvicorn, multiprocess Prometheus via a shared `/tmp/prometheus_multiproc` dir.
- **Worker** (`worker_entrypoint.py` → `worker.py`): single arq worker, `max_jobs=1` (one import at a time to prevent OOM on large S3 uploads).
- **Redis** is optional — if `redis_url` is not configured, exports run synchronously in the API process with a warning in the response.

---

## Configuration (`config.py`)

Three top-level config entities defined in `config.yaml`:

| Entity | Purpose |
|---|---|
| `datasources` | Named backends (S3 bucket, ClickHouse, Trino, Langfuse, CHYT) the service reads from |
| `connections` | Allowlisted URLs that users may provide credentials for (proxy/browser-initiated searches) |
| `targets` | Dataset service endpoints the service writes to (OAuth2 client_credentials) |

Secrets are injected via two mechanisms: `vault:KEY` references replaced from a Vault sidecar file, and `$ENV_VAR` shell expansion. Settings are cached as a frozen dataclass singleton via `@lru_cache`.

---

## Data sources

Each datasource has a `type` field that selects its adapter (see [Datasource adapters](#datasource-adapters-adapterspy)):

| Type | Search backend | Sampling | Notes |
|---|---|---|---|
| `s3` | DuckDB over S3 (httpfs) | reads first row per file | Key-prefix based listing; presigned URL generation |
| `clickhouse` | HTTP SQL API | in-memory (events path) | table `llogr_events`, project-scoped |
| `trino` | Trino REST API | in-memory | catalog + schema configurable |
| `langfuse` | Langfuse REST API | in-memory | proxy path also supported |
| `chyt` | ClickHouse-over-YT HTTP | in-memory | Yandex-specific |

---

## API routes

### Observability / infra
| Method | Path | Description |
|---|---|---|
| GET | `/livez` | Liveness probe (always 200) |
| GET | `/ready` | Readiness probe — checks each datasource; 503 on any failure |
| GET | `/health` | Detailed health — returns per-datasource status + error details |
| GET | `/metrics` | Multiprocess-safe Prometheus metrics |

### Datasource browsing (S3-specific)
| Method | Path | Description |
|---|---|---|
| GET | `/api/public/logs` | List S3 file URLs for a time range + filter params |
| GET | `/api/public/logs/list` | List S3 keys (metadata only, no URLs) |
| POST | `/api/public/logs/urls` | Generate presigned URLs for a given list of keys |

### Search (multi-backend)
| Method | Path | Description |
|---|---|---|
| GET | `/api/public/logs/search` | Full-text search; dispatches via adapter to DuckDB, ClickHouse, Trino, Langfuse, or CHYT |

### Import / export
| Method | Path | Description |
|---|---|---|
| POST | `/api/public/export/dataset` | Queue S3-key batch → dataset service |
| POST | `/api/public/export/dataset/events` | Queue in-memory events payload → dataset service (jsonl / individual / catalog formats) |
| GET | `/api/public/export/status/{job_id}` | Poll arq job status + progress (files_done / files_total / bytes_done) |

### Schema discovery
| Method | Path | Description |
|---|---|---|
| GET | `/api/public/datasource/sample` | Return field names, inferred types, and example values from a sample of records |

### Proxy (user-provided credentials)
| Method | Path | Description |
|---|---|---|
| POST | `/api/public/proxy/search` | Search using user-supplied credentials; URL must be in the `connections` allowlist |
| POST | `/api/public/proxy/ping` | Test a user-supplied connection |

### UI meta
| Method | Path | Description |
|---|---|---|
| GET | `/api/public/datasources` | List datasource names + types (no secrets) |
| GET | `/api/public/ui-config` | Full UI bootstrap config (datasources, connections, targets) |

---

## Datasource adapters (`adapters.py`)

All datasource-specific dispatch is centralised in `adapters.py`. The `DatasourceAdapter` Protocol defines two methods every backend must implement:

```
DatasourceAdapter
  search(query, *, auth, start, end, session_id, trace_id,
         trace_type, input_hash, limit, time_field) -> list[dict]
  ping() -> None
```

A concrete adapter class wraps each backend and delegates to the existing backend module functions:

| Adapter | Delegates to |
|---|---|
| `ClickhouseAdapter` | `clickhouse.search_logs_ch` |
| `ChytAdapter` | `chyt.search_logs_chyt` |
| `TrinoAdapter` | `trino.search_logs_trino` |
| `LangfuseAdapter` | `langfuse.search_logs_langfuse` / `langfuse.ping_langfuse` |
| `S3Adapter` | `s3.list_batch_keys` + `search.search_logs` (DuckDB) / `s3.ping_s3` |

`get_adapter(ds: Datasource) -> DatasourceAdapter` is the single factory used by all routes. Adding a new datasource type requires only a new adapter class and one `_REGISTRY` entry — no route files change.

**S3 special cases** — two S3 operations bypass the adapter because they have no equivalent on other backends:
- Schema discovery (`/datasource/sample`) uses `sampling.read_s3_traces_for_sampling` (first row per file via DuckDB for schema inference, not content search).
- Proxy search (`/proxy/search`) uses `s3.list_objects_proxy` (object listing, no DuckDB) because the caller supplies credentials directly with no server-side tenant prefix.

Both are handled as explicit S3 branches in their respective routes before calling `get_adapter()`.

---

## Auth (`auth.py`)

1. **Primary**: `X-Group-ID` header forwarded by nginx — value becomes the `public_key` used for tenant scoping in S3 key lookups.
2. **Fallback**: HTTP Basic auth — `public_key:secret_key` base64-encoded. Credentials are sanitized against path traversal before use.

The proxy routes do not require auth but enforce the URL allowlist.

---

## Import pipeline

```
POST /export/dataset
       |
       +-- Redis available? --YES--> enqueue arq job --> return {job_id, status:"queued"}
       |
       +-- NO ----------------> run inline -----------> return {status:"complete", result}

Worker / inline path:
  1. Resolve target + datasource from config
  2. Apply sampling rules (optional, runs in thread via asyncio.to_thread)
  3. dataset_service.create_dataset()  ->  POST /api/v0/datasets
  4. For each S3 key (or event):
       s3.get_object() -> dataset_service.upload_file()
       update Redis progress hash  (TTL 1h)
  5. Emit Prometheus counters / histogram
```

---

## Sampling engine (`sampling.py`)

Rules are applied as a union: each rule filters a qualifying pool from all traces, then randomly samples `rate`% of that pool. Deduplication by trace ID prevents the same trace from appearing twice.

Supported strategies:

| Strategy | Description |
|---|---|
| `random` | Uniform random sample |
| `high_cost` / `latency_spike` | Traces above Nth percentile on a numeric field |
| `long_trace` | Traces exceeding a numeric threshold |
| `failure` | Traces where a field matches error values or HTTP >= 400 |
| `user_dissatisfaction` | Traces with negative score/tag |
| `business_critical` | Traces where a field contains a keyword |
| `prompt_version_change` | Traces where a field differs from a baseline value |
| `low_confidence` | Traces where a numeric field is below a threshold |
| `weird_tool_sequences` | Traces with repeated, unexpected, or excess tool calls |

For the S3 path, DuckDB reads the first record of each file to get field values for non-random strategies.

---

## Dataset service integration (`dataset_service.py`)

- OAuth2 `client_credentials` token exchange; tokens cached in-process with a 30-second safety margin before expiry.
- `create_dataset` -> `POST /api/v0/datasets`
- `upload_file` -> `POST /api/v0/datasets/{id}/files` (multipart)
- `upload_timeout` defaults to 300 s (sized for ~100 MB at ~3 MB/s).

---

## Observability

### Prometheus metrics

| Metric | Type | Description |
|---|---|---|
| `dataimporter_s3_list_seconds` | Histogram | S3 listing latency |
| `dataimporter_s3_list_errors_total` | Counter | S3 listing failures |
| `dataimporter_search_seconds` | Histogram | Search endpoint latency |
| `dataimporter_search_errors_total` | Counter | Search failures |
| `dataimporter_import_files_total` | Counter | Files processed, labelled datasource/target/status |
| `dataimporter_import_bytes_total` | Counter | Bytes uploaded, labelled datasource/target |
| `dataimporter_import_seconds` | Histogram | Total import duration, labelled datasource/target |

Plus standard `prometheus_fastapi_instrumentator` HTTP metrics (latency, request/response size, request count).

The worker exposes its own metrics endpoint on `:9101` (hardcoded in `worker_entrypoint.py`).

### Logging

structlog structured JSON, with `request_id` bound per-request via `RequestIDMiddleware`. `/livez`, `/ready`, and `/metrics` are silenced from logs when `silence_probes: true`.

---

## Suggested improvements


### 1. No job cancellation or deduplication

The queue has no deduplication — submitting the same export twice creates two sequential jobs. Consider using arq's `job_id` parameter with a deterministic key (hash of target + datasource + sorted keys) to deduplicate, and expose a `DELETE /api/public/export/{job_id}` cancel endpoint.


### 2. S3 file-level import loads entire object into memory

`worker.import_dataset` does `content = await obj["Body"].read()` — the full file is buffered in Python `bytes` before upload. For large files this can OOM the worker. Stream the S3 body directly into the multipart upload using `httpx`'s streaming upload support.
