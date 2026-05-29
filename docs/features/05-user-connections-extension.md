# Feature: Extendable User Connections

## One-liner
Let users add their own ClickHouse (and Trino) connections from the UI, the same way Langfuse connections are added today.

## Problem
Currently only Langfuse supports user-provided credentials via the `connections` allowlist. ClickHouse and Trino require server-side `datasources` config — users can't connect to their own ClickHouse instance without admin access to the config file.

## Design decisions already made
- The SSRF-prevention model (server allowlists host URLs, user supplies credentials) already exists for Langfuse — reuse it.
- Credentials stored in `localStorage`, never sent to server except proxied per-request.
- `POST /api/public/proxy/search` and `POST /api/public/proxy/ping` already handle proxy logic — extend for ClickHouse/Trino.
- UI credential form: host URL + user + password (ClickHouse); host + catalog + schema + user + password (Trino).

## Scope
- **In**: ClickHouse user connections; Trino user connections; extend connection config schema; UI add-connection form; proxy search + ping for new types.
- **Out**: S3 user connections (credential model doesn't fit); per-user connection persistence on server.

## Open questions
- Should connection config support HTTPS/TLS options (e.g. `verify_ssl`)?
- Trino connection: should the user configure catalog + schema at connection time or at search time?
