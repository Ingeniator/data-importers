# dataimporter

Browse and search LLM logs stored in external backends.

Read-only companion to [llogr](../llogr) (which handles data ingestion).

## Supported backends

| Type | Mode | Description |
|---|---|---|
| **S3 / MinIO** | server-configured | List and download JSONL batch files, full-text search via DuckDB |
| **ClickHouse** | server-configured | Full-text search over the `llogr_events` table |
| **Trino** | server-configured | SQL search with polling-based query execution |
| **Langfuse** | server-configured or user-connected | Fetch traces via the Langfuse REST API |

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
```

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

### Server settings

```yaml
server:
  root_path: "/dataimporter"   # for reverse proxy
  host: "0.0.0.0"
  port: 5001
  workers: 2
  hide_auth_inputs: true       # hide pk/sk fields in the UI
  silence_probes: true         # suppress health check logs
```

### Vault secrets

Credentials can reference a vault sidecar file instead of being hardcoded:

```yaml
datasources:
  - name: "ClickHouse"
    type: clickhouse
    password: "vault:CH_PASSWORD"
```

Set `VAULT_SECRETS_PATH` (default `/vault/secrets/env`) to the file containing `CH_PASSWORD=...`.

## Running

```bash
uv run python entrypoint.py
```

The web UI is at `/`, API at `/api/public/`.

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
| `GET /api/public/datasources` | List configured datasources |
| `GET /api/public/ui-config` | UI configuration (datasources + connections) |
| `GET /livez` | Liveness probe |
| `GET /ready` | Readiness probe |
| `GET /health` | Detailed health status |
| `GET /metrics` | Prometheus metrics |
