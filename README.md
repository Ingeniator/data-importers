# dataimporter

Browse and search LLM logs stored in external backends. Import selected data into a dataset service.

Read-only companion to [llogr](../llogr) (which handles data ingestion).

## Supported backends

| Type | Mode | Description |
|---|---|---|
| **S3 / MinIO** | server-configured | List and download JSONL batch files, full-text search via DuckDB |
| **ClickHouse** | server-configured | Full-text search over the `llogr_events` table |
| **Trino** | server-configured | SQL search with polling-based query execution |
| **Langfuse** | server-configured or user-connected | Fetch traces via the Langfuse REST API |
| **CHYT** | server-configured | Full-text search over a YTsaurus table via ClickHouse over YT |

## Configuration

All configuration is in a YAML file (default `config.yaml`, override with `DATAIMPORTER_CONFIG` env var).

### Datasources (server-configured)

Fully server-side — credentials live in config, users don't need to provide anything.

```yaml
datasources:
  - name: "S3 / MinIO"
    type: s3
    bucket: "llogr-raw-events"
    region: "us-east-1"
    endpoint: "http://minio:9000"
    access_key_id: "minioadmin"
    secret_access_key: "minioadmin"

  - name: "ClickHouse"
    type: clickhouse
    url: "http://clickhouse:8123"
    database: "default"
    table: "llogr_events"
    user: "default"
    password: "clickhouse"

  - name: "Langfuse Prod"
    type: langfuse
    url: "https://cloud.langfuse.com"
    access_key_id: "pk-lf-..."
    secret_access_key: "sk-lf-..."

  - name: "CHYT"
    type: chyt
    url: "http://chyt-proxy:8123"
    database: "*ch_public"              # CHYT clique alias
    table: "//home/user/llogr_events"  # full YT table path
    user: "yt"
    password: "vault:YT_TOKEN"         # YT OAuth token
```

CHYT uses the ClickHouse HTTP protocol. `database` is the clique alias (e.g. `*ch_public`), `table` is the full YTsaurus path. The `password` field holds the YT OAuth token.

### Connections (user-connected)

Admin allowlists a host URL; users provide their own credentials from the UI.
Credentials are stored in the browser (`localStorage`), not on the server.

The server only proxies requests to URLs listed here — arbitrary URLs are rejected (prevents SSRF).

```yaml
connections:
  # Users provide their own Langfuse keys
  - type: langfuse
    url: "https://cloud.langfuse.com"
    label: "Langfuse Cloud"

  # Shared credentials — users don't need to enter keys
  - type: langfuse
    url: "http://langfuse:3000"
    label: "Langfuse (local)"
    public_key: "lf-pk-ai-suite"
    secret_key: "lf-sk-ai-suite"
```

When `public_key` and `secret_key` are set on a connection, users can add it without entering credentials (the server-side keys are used as defaults). Users can still override with their own keys.

### Dataset targets (import destination)

Defines external dataset services that users can import S3 files into. Tokens are obtained automatically via OAuth2 client credentials flow (Keycloak-compatible).

```yaml
targets:
  - name: "Production Dataset Service"
    base_url: "https://ds.example.com"
    token_url: "https://keycloak.example.com/realms/prod/protocol/openid-connect/token"
    client_id: "vault:DS_CLIENT_ID"
    client_secret: "vault:DS_CLIENT_SECRET"
    default_access: "organization"       # pre-selected access level in UI
    default_dataset_type: "DATASET"      # pre-selected dataset type in UI
    upload_timeout: 300                  # seconds; default 300, increase for slow links
```

If no targets are configured the import button does not appear in the UI.

### Server settings

```yaml
server:
  root_path: "/dataimporter"   # for reverse proxy
  host: "0.0.0.0"
  port: 5001
  workers: 2
  hide_auth_inputs: true       # hide pk/sk fields in the UI
  silence_probes: true         # suppress health check logs
  redis_url: "redis://redis:6379/1"  # optional — enables import queue
```

`redis_url` is optional. Without it imports run synchronously (one at a time is not enforced — concurrent imports from different users each consume memory for the full file set). With it, imports are serialised through a single arq worker (`max_jobs=1`), preventing simultaneous OOM from large files.

### Vault secrets

Credentials can reference a vault sidecar file instead of being hardcoded:

```yaml
datasources:
  - name: "ClickHouse"
    type: clickhouse
    password: "vault:CH_PASSWORD"
```

Set `VAULT_SECRETS_PATH` (default `/vault/secrets/env`) to the file containing `CH_PASSWORD=...`.
Supported formats: `KEY=value`, `export KEY=value`, `KEY: value`.

## Running

**API server:**
```bash
uv run python entrypoint.py
```

**Import worker** (required for queue-based import, needs `redis_url` configured):
```bash
uv run python worker_entrypoint.py
```

The web UI is at `/`, API at `/api/public/`.

## Import flow

Users can import selected S3 files into a configured dataset service directly from the browser UI:

1. Browse or search files in an S3 datasource
2. Select files (or use all results)
3. Click **Import to dataset service**
4. Set a dataset name (pre-filled from source name + time range), access level, and dataset type
5. Click **Import**

**With Redis configured:** the job is enqueued and the UI shows a live progress bar (polling every 2 s). The worker processes one import at a time, preventing concurrent memory spikes from large files.

**Without Redis:** the import runs synchronously in the API server and completes before the response is returned. A warning is shown in the UI. Concurrent imports from different users are possible — each reads files into memory, so avoid large concurrent imports on memory-constrained deployments.

Files are streamed from S3 into memory one at a time and uploaded to the dataset service via multipart form upload. The `upload_timeout` setting controls the per-file upload deadline (default 300 s, enough for 100 MB at ~3 MB/s).

## Mock dataset service

A local mock of the dataset service is included for testing the import UX:

```bash
uv run python tests/mock_dataset_service.py --port 9100
```

The mock prints a `config.yaml` snippet on startup. It stores uploaded files in `/tmp/mock-dataset-service` (override with `--upload-dir`).

Inspect what was imported:
```
GET http://localhost:9100/_mock/datasets          — list datasets
GET http://localhost:9100/_mock/datasets/{id}     — dataset detail and file list
GET http://localhost:9100/_mock/files/{id}        — download an uploaded file
```

In the Docker Compose stack, the mock runs as the `dataset-mock` service and is pre-wired as a target in `config.gateway.yaml`.

## Endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Web UI |
| `GET /api/public/logs/search` | Search across any server-configured datasource |
| `GET /api/public/logs/list` | List S3 batch files |
| `POST /api/public/logs/urls` | Get presigned URLs for S3 files |
| `GET /api/public/media/{id}` | Fetch media metadata + presigned URL |
| `POST /api/public/proxy/search` | Search via user-connected datasource |
| `POST /api/public/proxy/ping` | Test a user connection |
| `POST /api/public/export/dataset` | Enqueue (or run) a dataset import |
| `GET /api/public/export/status/{job_id}` | Poll import job status and progress |
| `GET /api/public/datasources` | List configured datasources |
| `GET /api/public/ui-config` | UI configuration (datasources, connections, targets) |
| `GET /livez` | Liveness probe |
| `GET /ready` | Readiness probe |
| `GET /health` | Detailed health status |
| `GET /metrics` | Prometheus metrics |

## Observability

Prometheus metrics are exposed at `/metrics`. The `dataimporter` Grafana dashboard (provisioned automatically in the Docker Compose stack) covers:

- **Import intensity** — files/s and bytes/s per datasource, import duration P50/P95, failure rate
- **Memory** — process RSS over time with threshold markers (yellow at 60 %, red at 90 % of the 512 MB container limit), memory % of limit stat, CPU usage
- **GC** — Python garbage collection rate per generation
