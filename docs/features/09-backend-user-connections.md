# Feature: Backend User Connection Storage

## One-liner
Store user-defined connections server-side (per user, encrypted secrets) instead of in browser `localStorage`, with a CRUD API and connection list page.

## Problem
Current proxy connections keep credentials in `localStorage` — they are lost on browser clear, can't be shared across devices, and are sent in plaintext in every request body. Users need persistent, secure, per-account connection management.

## Design decisions already made
- Scoped by `auth.public_key` — each user sees only their own connections.
- Secrets encrypted at rest using Fernet (symmetric key from `server.secret_key` config field, or `SECRET_KEY` env var). Never returned in GET responses — write-only after save.
- The server allowlist (`connections` in `config.yaml`) still acts as a URL safelist — users can only save connections whose URL matches an allowlist entry (SSRF prevention unchanged).
- Storage: SQLite file (`user_connections.db`) in a configurable path; `aiosqlite` for async access. Chosen over PostgreSQL to avoid new infra dependencies.
- Connection ID: server-generated UUID returned on create; used for update/delete.
- `PATCH /api/public/user-connections/{id}` updates label or credentials; omitted fields are unchanged.

## API

```yaml
GET    /api/public/user-connections          → list (no secrets in response)
POST   /api/public/user-connections          → create → { id, label, type, url }
PATCH  /api/public/user-connections/{id}     → update label or credentials
DELETE /api/public/user-connections/{id}     → delete
POST   /api/public/user-connections/{id}/ping → test saved connection
```

## Model

```python
class UserConnection(BaseModel):
    id: UUID
    owner: str          # auth.public_key — never exposed in API response
    label: str
    type: str           # langfuse | clickhouse | trino | s3
    url: str
    # credentials stored encrypted; omitted from GET responses
    access_key_id: str = ""
    secret_key: str = ""
    # S3-specific
    bucket: str = ""
    region: str = ""
    key_prefix: str = ""
```

## Scope
- **In**: CRUD API; Fernet encryption; SQLite storage; allowlist enforcement on create/update; ping endpoint; migration from localStorage (UI reads saved connections on load and offers one-time import).
- **Out**: Cross-user connection sharing; admin view of all users' connections; connection usage analytics.

## Open questions
- Where is the Fernet key sourced? (`SECRET_KEY` env var, vault reference, or derived from existing `server` config?)
- Should `DELETE` cascade to scheduled tasks that reference the connection?
