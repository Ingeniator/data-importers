# dataimporter — Key Outputs for New Service

> **Purpose of this document**: distilled knowledge from the current dataimporter codebase and its
> redesign spec (`001-datasource-adapter-redesign`) — enough to rebuild the service from scratch
> with the right shape, without inheriting the current implementation's mistakes.

---

## 1. What the Service Does

dataimporter is a FastAPI service that provides a **uniform search and export API** over multiple
observability/data backends. It has two modes of operation:

**Read path** — consumers (search UI, schema-discovery UI) query named datasources:

| Operation | Endpoint | What it does |
|---|---|---|
| Full-text search | `GET /api/public/logs/search` | Query a named datasource by keyword + filters |
| Schema discovery | `GET /api/public/datasource/sample` | Return a field map inferred from a representative sample |
| Proxy search | `POST /api/public/proxy/search` | Search using user-supplied credentials (no server auth) |
| Proxy ping | `POST /api/public/proxy/ping` | Health-check a user-supplied connection |

**Persistent path** — users save connections and schedule recurring export jobs:

| Operation | Endpoint | What it does |
|---|---|---|
| Manage connections | `CRUD /api/public/connections` | Save backend credentials encrypted at rest |
| Manage export jobs | `CRUD /api/public/jobs` | Define cron export jobs referencing a saved connection |
| Job history | `GET /api/public/jobs/{id}/runs` | Execution log with status and record counts |
| Manual trigger | `POST /api/public/jobs/{id}/run` | Run a job immediately outside its schedule |

**Storage**: PostgreSQL for saved connections and job state. Credentials are encrypted with a
server-side Fernet key (`ENCRYPTION_KEY` env var) before writing to the database.

---

## 2. Domain Model

### 2.1 Core entities

```python
# Frozen dataclasses — immutable value objects throughout

@dataclass(frozen=True)
class Datasource:
    """Named, server-configured backend connection."""
    name: str
    type: str          # "s3" | "clickhouse" | "chyt" | "trino" | "langfuse"
    # S3 fields
    bucket: str = ""
    region: str = "us-east-1"
    endpoint: str | None = None
    access_key_id: str = ""
    secret_access_key: str = ""
    public_endpoint: str | None = None
    key_prefix: str = ""
    addressing_style: str = "virtual"
    presign_expiry: int = 3600
    # SQL backend fields (ClickHouse / CHYT / Trino)
    url: str = ""
    database: str = "default"
    table: str = "llogr_events"
    user: str = "default"
    password: str = ""
    catalog: str = ""       # Trino
    schema_name: str = ""   # Trino
    duckdb_temp_dir: str = "/tmp/duckdb_temp"


@dataclass(frozen=True)
class Connection:
    """Allowlisted connection template — users provide only credentials."""
    type: str
    url: str       # base URL or "*" for S3 wildcard
    label: str = ""
    public_key: str = ""
    secret_key: str = ""
    bucket: str = ""
    region: str = "us-east-1"
    addressing_style: str = "path"
    key_prefix: str = ""


@dataclass(frozen=True)
class SearchFilters:
    """Request-scoped filter context — built once per request, all datetimes UTC.
    
    This is the TARGET design (001-datasource-adapter-redesign).
    Replaces the repeated 7-parameter filter list on every route/adapter/backend.
    """
    start: datetime | None = None
    end: datetime | None = None
    session_id: str | None = None
    trace_id: str | None = None
    trace_type: str | None = None
    input_hash: str | None = None
    time_field: str | None = None   # None → backend's documented default column


class AuthContext(NamedTuple):
    public_key: str
    secret_key: str
    is_org_admin: bool = False
```

### 2.2 Persistence entities

```python
# PostgreSQL tables — async access via asyncpg

@dataclass
class SavedConnection:
    """User-owned backend connection stored encrypted in the database."""
    id: UUID                   # primary key
    owner_key: str             # public_key from AuthContext — tenant scope
    label: str                 # user-visible name
    type: str                  # "s3" | "clickhouse" | "chyt" | "trino" | "langfuse"
    connection_url: str        # stored in plaintext — used for display and allowlist check
    credentials_enc: bytes     # Fernet-encrypted JSON of sensitive fields
    created_at: datetime
    updated_at: datetime

    # credentials_enc decrypts to a dict like:
    # { "access_key_id": "...", "secret_access_key": "...", "bucket": "...",
    #   "password": "...", "public_key": "...", "secret_key": "..." }
    # Only fields relevant to the connection type are populated.


@dataclass
class ExportJob:
    """Recurring export job — runs on a cron schedule, reads from a saved connection."""
    id: UUID
    owner_key: str
    label: str
    connection_id: UUID        # FK → SavedConnection
    target_name: str           # references a server-configured DatasetTarget in YAML
    schedule: str              # cron expression, e.g. "0 * * * *"
    filters: dict              # serialized filter config (relative time offsets, type filters, etc.)
    enabled: bool = True
    created_at: datetime
    updated_at: datetime
    last_run_at: datetime | None = None
    next_run_at: datetime | None = None


@dataclass
class JobRun:
    """Single execution record for an ExportJob."""
    id: UUID
    job_id: UUID               # FK → ExportJob
    started_at: datetime
    finished_at: datetime | None = None
    status: str = "running"    # "running" | "success" | "failed"
    records_exported: int | None = None
    bytes_exported: int | None = None
    error_message: str | None = None
```

### 2.3 Encryption

All credential encryption uses `cryptography.fernet.Fernet` (AES-128-CBC + HMAC-SHA256):

```python
import json
from cryptography.fernet import Fernet

# At startup — read from ENCRYPTION_KEY env var (base64-urlsafe 32-byte key)
fernet = Fernet(os.environ["ENCRYPTION_KEY"])

def encrypt_credentials(creds: dict) -> bytes:
    return fernet.encrypt(json.dumps(creds).encode())

def decrypt_credentials(ciphertext: bytes) -> dict:
    return json.loads(fernet.decrypt(ciphertext))
```

`connection_url` is stored **in plaintext** — it is non-sensitive (it's an endpoint URL, not a
secret) and needs to be queryable for display and allowlist validation. Only the credential
fields (`access_key_id`, `secret_access_key`, `password`, `public_key`, `secret_key`) go into
`credentials_enc`.

Key rotation: re-encrypt all `credentials_enc` rows with the new key during a migration.

### 2.4 Schema-discovery output shape (field map)

```json
{
  "fields": {
    "body.cost":    { "type": "float",  "example": 0.012 },
    "body.input":   { "type": "string", "example": "hello" },
    "session_id":   { "type": "string", "example": "sess-1" }
  },
  "sample_count": 5
}
```

Rules:
- `type` ∈ `{bool, int, float, list, object, string}`
- Nested dicts flattened with dot-joined keys to depth 3
- Keys starting with `_` excluded
- Object fields: `example: null`; list fields: `example: first 2 elements`

---

## 3. Adapter Architecture (Target Design)

The core insight: **every route must dispatch through `get_adapter()` with zero `ds.type` branches**.
The current code leaks S3 in two routes; the redesign closes all leaks.

```python
@runtime_checkable
class DatasourceAdapter(Protocol):
    async def search(self, query: str, filters: SearchFilters, *, auth: AuthContext, limit: int = 50) -> list[dict]: ...
    async def sample(self, filters: SearchFilters, *, auth: AuthContext, limit: int = 5) -> list[dict]: ...
    async def ping(self) -> None: ...


class BaseAdapter:
    _health_path: str | None = None   # set by HTTP-GET-health backends

    def __init__(self, ds: Datasource) -> None:
        self.ds = ds

    async def search(self, query, filters, *, auth, limit=50) -> list[dict]:
        raise NotImplementedError   # concrete adapters must override

    async def sample(self, filters, *, auth, limit=5) -> list[dict]:
        # Default: time-bucketed — split [start,end] into `limit` sub-windows,
        # run search("*", window_i, limit=1) concurrently, merge+dedupe.
        # Falls back to single search when start/end absent.
        ...

    async def ping(self) -> None:
        # Default: GET {ds.url}{_health_path} with 3s timeout
        ...


# Concrete adapters — only search() and optionally ping() are backend-specific
class ClickhouseAdapter(BaseAdapter):
    _health_path = "/ping"
    async def search(self, query, filters, *, auth, limit=50): ...

class ChytAdapter(BaseAdapter):
    _health_path = "/ping"
    async def search(self, query, filters, *, auth, limit=50): ...

class TrinoAdapter(BaseAdapter):
    _health_path = "/v1/info"
    async def search(self, query, filters, *, auth, limit=50): ...

class LangfuseAdapter(BaseAdapter):
    async def search(self, query, filters, *, auth, limit=50): ...
    async def ping(self) -> None: ...   # needs auth — overrides base

class S3Adapter(BaseAdapter):
    async def search(self, query, filters, *, auth, limit=50): ...  # list_batch_keys + DuckDB
    async def sample(self, filters, *, auth, limit=5): ...          # bucketed keys
    async def ping(self) -> None: ...                                # ping_s3

_REGISTRY: dict[str, type[BaseAdapter]] = {
    "clickhouse": ClickhouseAdapter,
    "chyt": ChytAdapter,
    "trino": TrinoAdapter,
    "langfuse": LangfuseAdapter,
    "s3": S3Adapter,
}

def get_adapter(ds: Datasource) -> DatasourceAdapter:
    cls = _REGISTRY.get(ds.type)
    if cls is None:
        raise ValueError(f"Unknown datasource type: {ds.type!r}")
    return cls(ds)
```

### Adding a new backend (the full recipe)

1. Add `search_logs_<backend>()` in `backends/<backend>.py`
2. Subclass `BaseAdapter`, set `_health_path`, implement `search()`
3. Register `_REGISTRY["name"] = NewAdapter`
4. Done — `sample()`, `ping()`, and all three routes work automatically

---

## 4. API Contracts

### 4.1 Full-text search

```
GET /api/public/logs/search

Query params:
  datasource    string  required
  q             string  required, min 1 char
  start         datetime (ISO 8601)
  end           datetime (ISO 8601)
  session_id    string
  trace_id      string
  trace_type    string
  input_hash    string
  time_field    string  — see §5 for per-backend rules
  filters       string  — JSON array of FilterRule
  limit         integer default 50, max 500

Auth: Basic auth OR X-Group-ID header (nginx upstream)

Response 200:
  { "results": [ { ...record } ], "backend": "clickhouse" }

Response 400: unknown datasource type or unsupported time_field
Response 404: datasource not found
```

### 4.2 Schema discovery

```
GET /api/public/datasource/sample

Query params:
  datasource    string  required
  start / end / session_id / trace_id / trace_type / input_hash / time_field
  (NO keys[] — that was S3-only and is removed in the redesign)

Auth: Basic auth OR X-Group-ID header

Response 200:
  { "fields": { "<dot.path>": { "type": str, "example": any } }, "sample_count": int }

Response 400: unsupported type or time_field
Response 404: datasource not found
```

### 4.3 Proxy search

```
POST /api/public/proxy/search
Content-Type: application/json

Body:
{
  "credentials": {
    "connection_url": "https://...",   // must be in allowlist
    "access_key_id": "",
    "secret_access_key": "",
    "bucket": "",       // S3 only
    "key_prefix": "",   // S3 only
    "region": ""        // S3 only
  },
  "q": "error",
  "start": null, "end": null,
  "session_id": null, "trace_id": null,
  "trace_type": null, "input_hash": null,
  "limit": 50
}

No auth header — proxy is public, credentials come from the request body.

Response 200:  { "results": [...], "backend": "s3" }
Response 400:  missing bucket, unsupported type
Response 403:  connection URL not in allowlist
Response 502:  backend unreachable
```

### 4.4 Proxy ping

```
POST /api/public/proxy/ping
Body: { "credentials": { ... } }

Response 200: { "status": "ok" }
Response 400/403/502: same as proxy search
```

### 4.5 Saved connections CRUD

```
POST /api/public/connections
Body: {
  "label": "My ClickHouse",
  "type": "clickhouse",
  "connection_url": "http://ch.example.com:8123",
  "credentials": { "user": "default", "password": "s3cr3t", "database": "logs" }
}
Response 201: { "id": "<uuid>", "label": "...", "type": "...", "connection_url": "...", "created_at": "..." }
# credentials are never returned

GET /api/public/connections
Response 200: { "items": [ { "id", "label", "type", "connection_url", "created_at" } ] }
# returns only connections owned by the authenticated user (owner_key == public_key)

GET /api/public/connections/{id}
Response 200: same shape as single item above (no credentials)
Response 404: not found or not owned by caller

DELETE /api/public/connections/{id}
Response 204: deleted
Response 409: connection is referenced by one or more jobs

POST /api/public/connections/{id}/ping
Response 200: { "status": "ok" }
Response 502: backend unreachable
# decrypts credentials, builds a transient Datasource, calls adapter.ping()
```

### 4.6 Export jobs CRUD

```
POST /api/public/jobs
Body: {
  "label": "Hourly CH export",
  "connection_id": "<uuid>",
  "target_name": "prod-dataset",
  "schedule": "0 * * * *",
  "filters": { "trace_type": "generation", "lookback_hours": 1 },
  "enabled": true
}
Response 201: { "id", "label", "connection_id", "target_name", "schedule",
                "filters", "enabled", "next_run_at", "created_at" }

GET /api/public/jobs
Response 200: { "items": [ ... ] }

GET /api/public/jobs/{id}
Response 200: full job object
Response 404: not found or not owned by caller

PATCH /api/public/jobs/{id}
Body: partial — any of { "label", "schedule", "filters", "enabled" }
Response 200: updated job object
# changing schedule recomputes next_run_at

DELETE /api/public/jobs/{id}
Response 204

GET /api/public/jobs/{id}/runs?limit=20&offset=0
Response 200: { "items": [ { "id", "started_at", "finished_at", "status",
                              "records_exported", "bytes_exported", "error_message" } ] }

POST /api/public/jobs/{id}/run
Response 202: { "run_id": "<uuid>" }
# enqueues an immediate execution; does not wait for completion
```

---

## 5. Backend Behavior Matrix

### time_field resolution

| Backend | Default column | Custom column | Unsupported value |
|---|---|---|---|
| ClickHouse | `timestamp` | any safe col name | silently falls back (current) → reject with 400 (redesign) |
| CHYT | `timestamp` | any safe col name | same |
| Trino | `timestamp` | any safe col name | same |
| Langfuse | trace `timestamp` | 400 | 400 |
| S3 | key-embedded `timestamp` | 400 | 400 |

**Rule**: honor or reject loudly — never silently ignore.

### Backend I/O constraints

| Backend | I/O model | Notes |
|---|---|---|
| ClickHouse | async HTTP (httpx) | SQL via HTTP interface |
| CHYT | async HTTP (httpx) | ClickHouse-over-YT, same HTTP interface |
| Trino | async HTTP (httpx) | REST API |
| Langfuse | async HTTP (httpx) | REST API, needs auth |
| S3 + DuckDB | **blocking** — run via `asyncio.to_thread` | aioboto3 for listing, DuckDB for content scan |

**Critical constraint**: all S3/DuckDB blocking calls MUST stay off the event loop via
`asyncio.to_thread`. Never await a DuckDB call directly.

---

## 6. Auth Model

Two modes, resolved in this order:

1. **nginx upstream** — `X-Group-ID` header contains `tenant/user`. No credential check needed —
   nginx is the trust boundary. `is_org_admin` comes from `X-Role: ORG_ADMIN`.
2. **Basic auth fallback** — `Authorization: Basic <base64(public_key:secret_key)>`. Used by SDK
   integrations (e.g. Langfuse calling directly).

The proxy path uses a synthetic `AuthContext(public_key="", secret_key="")` — proxy calls carry
the user's own credentials in the request body, not the server's tenant credentials.

Key sanitization: `public_key` strips `..` path traversal and unsafe characters. An empty key
after sanitization is a 401.

---

## 7. Configuration

YAML file, path from `DATAIMPORTER_CONFIG` env var (falls back to `config.yaml` next to `src/`).

Supports vault secret injection: `vault:KEY` references are replaced from a sidecar file at
`VAULT_SECRETS_PATH` (default `/vault/secrets/env`). Also expands `$ENV_VARS`.

```yaml
server:
  host: 0.0.0.0
  port: 5001
  workers: 1
  redis_url: ""          # empty = no background worker queue
  debug: false

datasources:
  - name: prod-ch
    type: clickhouse
    url: http://clickhouse:8123
    database: default
    table: llogr_events
    user: default
    password: vault:CH_PASSWORD

  - name: prod-s3
    type: s3
    endpoint: https://s3.amazonaws.com
    bucket: my-logs
    region: us-east-1
    access_key_id: vault:S3_KEY
    secret_access_key: vault:S3_SECRET

connections:
  - type: langfuse
    url: https://cloud.langfuse.com
    label: Langfuse Cloud
  - type: s3
    url: "*"    # wildcard — any S3 endpoint is allowed
```

---

## 8. Observability

### Prometheus metrics (in `metrics.py`)

| Metric | Type | Labels | What it measures |
|---|---|---|---|
| `dataimporter_search_seconds` | Histogram | — | Search dispatch latency |
| `dataimporter_search_errors_total` | Counter | — | Search failures |
| `dataimporter_s3_list_seconds` | Histogram | — | S3 key listing latency |
| `dataimporter_s3_list_errors_total` | Counter | — | S3 list failures |
| `dataimporter_import_files_total` | Counter | datasource, target, status | Files processed |
| `dataimporter_import_bytes_total` | Counter | datasource, target | Bytes uploaded |
| `dataimporter_import_seconds` | Histogram | datasource, target | Import duration |

### Logging

Structured JSON via `structlog`. Every request logs `authenticated`, `search_scope` (backend + file
count for S3), adapter dispatch, and errors. No PII in logs — keys are sanitized.

### Health

`GET /api/public/health` — returns 200 when the service is up. Per-datasource ping is separate
(via proxy ping endpoint or internal health routes).

---

## 9. Tech Stack

| Layer | Choice | Notes |
|---|---|---|
| Language | Python 3.13 | |
| Web framework | FastAPI + uvicorn | Async, OpenAPI generation |
| HTTP client | httpx (async) | Used for CH/CHYT/Trino/Langfuse |
| S3 client | aioboto3 | Async wrapper around boto3 |
| S3 content search | DuckDB | Read Parquet/JSON from S3; run in thread |
| Config | pydantic + frozen dataclasses | YAML → dataclasses (no Pydantic for internal models) |
| Database | PostgreSQL | Saved connections + job definitions + job runs |
| DB driver | asyncpg | Async PostgreSQL driver; no ORM |
| DB migrations | Alembic | Schema versioning |
| Encryption | cryptography (Fernet) | AES-128-CBC + HMAC for credential storage |
| Job scheduling | APScheduler (AsyncIOScheduler) | Loads job definitions from DB on startup; Redis-backed job store if `redis_url` is set |
| Logging | structlog | JSON output |
| Metrics | prometheus_client | Histogram + Counter |
| Testing | pytest + FastAPI TestClient | Unit: mock at route boundary; E2E: Playwright |
| Package manager | uv | `uv run pytest` |
| Container | Linux/uvicorn | Standard FastAPI container pattern |

---

## 10. Known Design Mistakes to Avoid

These are the exact problems the `001-datasource-adapter-redesign` spec was written to fix.
Build the new service without them from day one.

### Mistake 1 — Adapter abstraction that leaks

**What happened**: `DatasourceAdapter` protocol was defined but routes bypassed it with
`if ds.type == "s3"` branches, importing backend modules directly.

**How to avoid**: The adapter is the *only* dispatch point. Routes call `get_adapter(ds).method()`
with no type inspection. The protocol + `_REGISTRY` dict is the extension point.

### Mistake 2 — Repeated parameter lists

**What happened**: Every adapter and backend function restated the same 10 filter parameters
(`start, end, session_id, trace_id, trace_type, input_hash, time_field, ...`). Changed once in a
route, easy to miss in an adapter.

**How to avoid**: Use a single frozen value object (`SearchFilters`) built once per request in a
FastAPI dependency. Pass the object, not individual params.

### Mistake 3 — Timezone normalization copy-pasted

**What happened**: The `if x.tzinfo is None: x = x.replace(tzinfo=UTC)` pattern appeared in
`search.py`, `sample.py`, `logs.py`, and `proxy.py`.

**How to avoid**: Normalize once in the `get_search_filters()` dependency. By the time
`SearchFilters` reaches an adapter, all datetimes are UTC-aware.

### Mistake 4 — `time_field` honored by only some backends

**What happened**: ClickHouse and Trino respected `time_field`; Langfuse silently ignored it.
Impossible to test consistently.

**How to avoid**: Every backend either applies `time_field` or raises a clear 400. "Honor or
reject, never ignore" is a unit-testable invariant.

### Mistake 5 — Schema discovery takes only the first N records

**What happened**: S3 sample read the first 5 files; other backends did `search("*", limit=5)`.
Fields appearing only in later records were invisible to schema discovery.

**How to avoid**: Time-bucketed sampling — split `[start, end]` into `limit` equal sub-windows,
run `search("*", window_i, limit=1)` concurrently, merge. Cost is bounded (`limit` is small).

### Mistake 6 — S3 proxy returned object listing, not content search

**What happened**: `proxy.py` had a separate `list_objects_proxy` code path for S3, diverging from
the authenticated search path (which does DuckDB content scan).

**How to avoid**: Unify — proxy S3 runs the same `S3Adapter.search()` as the authenticated route.
One code path, one set of tests.

---

## 11. Testing Conventions

- **Location**: `tests/` flat directory (`test_adapters.py`, `test_search_filters.py`,
  `test_sample.py`, `test_proxy.py`, `test_logs.py`)
- **Style**: `FastAPI TestClient` + dependency overrides to mock at the route boundary (not at
  `unittest.mock.patch` level inside adapters)
- **TDD order**: failing test committed → implementation → green
- **E2E**: `tests/e2e/` with Playwright, for browser-driven flows
- **Performance guard**: `tests/test_performance.py` enforces p95 ≤ 500 ms at 50 concurrent

Key test patterns:
```python
# Mock adapter at route boundary
app.dependency_overrides[get_adapter] = lambda ds: FakeAdapter()

# Source-scan test (structural assertion)
def test_no_ds_type_branches():
    for route_file in ["search.py", "sample.py", "proxy.py"]:
        src = (ROUTES_DIR / route_file).read_text()
        assert "ds.type ==" not in src
        assert "from dataimporter.backends" not in src
```

---

## 12. File Map (Target State)

```
src/dataimporter/
├── main.py                  # FastAPI app factory, router registration, scheduler startup
├── config.py                # Datasource, DatasetTarget, Settings dataclasses + YAML loader
├── auth.py                  # AuthContext, get_auth() dependency
├── metrics.py               # Prometheus counters + histograms
├── adapters.py              # DatasourceAdapter protocol + BaseAdapter + 5 adapters + get_adapter()
├── search_filters.py        # SearchFilters frozen dataclass
├── filters.py               # FilterRule parsing + apply_filters (post-search filtering)
├── search.py                # S3/DuckDB content search (used by S3Adapter)
├── s3.py                    # list_batch_keys, ping_s3
├── sampling.py              # read_s3_traces_for_sampling
├── duckdb.py                # DuckDB connection helpers
│
├── db/
│   ├── pool.py              # asyncpg connection pool (get_pool() dependency)
│   ├── migrations/          # Alembic migration scripts
│   ├── connections.py       # SavedConnection CRUD (insert/select/delete queries)
│   └── jobs.py              # ExportJob + JobRun CRUD
│
├── crypto.py                # encrypt_credentials() / decrypt_credentials() (Fernet)
│
├── scheduler/
│   ├── setup.py             # APScheduler setup, load jobs from DB on startup
│   └── runner.py            # execute_export_job() — decrypt creds, build Datasource, run export
│
├── routes/
│   ├── deps.py              # get_datasource(), get_search_filters(), get_pool() dependencies
│   ├── search.py            # GET /api/public/logs/search
│   ├── sample.py            # GET /api/public/datasource/sample
│   ├── proxy.py             # POST /api/public/proxy/{search,ping}
│   ├── connections.py       # CRUD /api/public/connections + /ping
│   ├── jobs.py              # CRUD /api/public/jobs + /runs + /run
│   ├── health.py            # GET /api/public/health
│   └── export.py            # server-configured export pipeline (existing)
│
└── backends/
    ├── clickhouse.py
    ├── chyt.py
    ├── trino.py
    └── langfuse.py

tests/
├── test_adapters.py
├── test_search_filters.py
├── test_sample.py
├── test_proxy.py
├── test_connections.py      # NEW — save/retrieve/delete, encryption round-trip, ping
├── test_jobs.py             # NEW — CRUD, schedule parsing, run trigger, history
├── test_crypto.py           # NEW — encrypt/decrypt round-trip, key rotation helper
├── test_logs.py
├── test_performance.py
└── e2e/
```

---

## 13. Database Schema

```sql
-- Alembic-managed. All timestamps stored as TIMESTAMPTZ.

CREATE TABLE saved_connections (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_key       TEXT NOT NULL,               -- public_key from AuthContext
    label           TEXT NOT NULL,
    type            TEXT NOT NULL,               -- "s3" | "clickhouse" | "chyt" | "trino" | "langfuse"
    connection_url  TEXT NOT NULL,               -- plaintext — display and allowlist check only
    credentials_enc BYTEA NOT NULL,              -- Fernet-encrypted JSON
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ON saved_connections (owner_key);

CREATE TABLE export_jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_key       TEXT NOT NULL,
    label           TEXT NOT NULL,
    connection_id   UUID NOT NULL REFERENCES saved_connections(id),
    target_name     TEXT NOT NULL,               -- server-configured DatasetTarget name
    schedule        TEXT NOT NULL,               -- cron expression
    filters         JSONB NOT NULL DEFAULT '{}',
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_run_at     TIMESTAMPTZ,
    next_run_at     TIMESTAMPTZ
);
CREATE INDEX ON export_jobs (owner_key);
CREATE INDEX ON export_jobs (next_run_at) WHERE enabled = TRUE;

CREATE TABLE job_runs (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id            UUID NOT NULL REFERENCES export_jobs(id) ON DELETE CASCADE,
    started_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at       TIMESTAMPTZ,
    status            TEXT NOT NULL DEFAULT 'running',  -- "running" | "success" | "failed"
    records_exported  INTEGER,
    bytes_exported    BIGINT,
    error_message     TEXT
);
CREATE INDEX ON job_runs (job_id, started_at DESC);
```

**Ownership enforcement**: all queries for connections and jobs include `AND owner_key = $1` using
the caller's `public_key`. A row not owned by the caller is treated as not found (404), not
forbidden — avoids leaking existence.

**Connection deletion guard**: before deleting a `saved_connection`, check for referencing
`export_jobs`. If any exist, return 409 with the count of blocking jobs.

---

## 14. Scheduler Design

APScheduler `AsyncIOScheduler` runs inside the FastAPI process:

1. **On startup** (`lifespan`): load all `enabled = TRUE` jobs from `export_jobs`, register each
   with APScheduler using its `schedule` (cron expression) and `id` as the job key.
2. **On job CRUD**: add/remove/modify the APScheduler job alongside the DB write so the in-memory
   schedule stays in sync.
3. **On execution**: `runner.execute_export_job(job_id)` —
   1. Load `ExportJob` + `SavedConnection` from DB
   2. `decrypt_credentials()` → build a transient `Datasource`
   3. Run the export (existing pipeline in `importer.py`)
   4. Write a `JobRun` row with `status = "success"` or `"failed"` + `error_message`
   5. Update `last_run_at` + `next_run_at` on the job row
4. **Manual trigger** (`POST /jobs/{id}/run`): calls `scheduler.add_job(execute_export_job, 'date',
   run_date=now(), args=[job_id])` — fires once immediately without affecting the cron schedule.

If `redis_url` is configured, APScheduler uses a Redis job store for persistence across restarts.
Otherwise the in-memory store is rebuilt from DB on each startup (safe — DB is the source of truth).

---

## 15. Performance Targets

- Search endpoints: **≥ 50 concurrent at p95 ≤ 500 ms**
- S3/DuckDB blocking calls: always on `asyncio.to_thread` (never block the event loop)
- Schema discovery: at most `limit` (default 5) light `search(limit=1)` calls, run concurrently
- Health checks: 3-second timeout per backend ping
