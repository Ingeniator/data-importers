# Feature: Proxy Connections (User-provided Credentials)

## One-liner
Users supply their own Langfuse or S3 credentials from the browser; the server proxies requests only to admin-allowlisted URLs.

## Problem
Server-configured datasources require admin access to `config.yaml`. Users with their own Langfuse accounts or S3 buckets need a self-service path without exposing server credentials or enabling SSRF.

## Implementation
- `src/dataimporter/routes/proxy.py` — `POST /api/public/proxy/search` and `POST /api/public/proxy/ping`.
- Credentials in `UserCredentials` payload (never persisted server-side); users store them in `localStorage`.
- `_resolve_connection()` validates `connection_url` against the `connections` allowlist in `config.yaml` — arbitrary URLs are rejected (SSRF prevention).
- Server-configured `public_key`/`secret_key` on a connection act as defaults; user-supplied keys take priority.
- S3 wildcard connection (`url: "*"`) allows any S3 endpoint — used for fully user-controlled S3 access.
- Dispatches via `get_adapter()` using a synthetic `AuthContext(public_key="", secret_key="")`.

## Scope
- **In**: Langfuse user connections; S3 user connections (with wildcard support); URL allowlist enforcement; server-side credential defaults.
- **Out**: ClickHouse/Trino user connections (planned in `docs/features/05-user-connections-extension.md`); credential storage on server.

## Known gaps
- No rate limiting on proxy endpoints — a user can proxy-search at full backend throughput.
