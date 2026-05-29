# Feature: S3 Log Browsing

## One-liner
List S3 batch files by time range and filter params, and generate presigned download URLs — S3-specific routes that bypass the adapter pattern.

## Problem
Users browse S3 files before deciding what to import or search. They need to list available files (with metadata), get direct download URLs, and request presigned URLs for a known set of keys — operations with no equivalent on non-S3 backends.

## Implementation
- `src/dataimporter/routes/logs.py` — three endpoints, all S3-only.
- `GET /api/public/logs` — `list_batch_urls()`: lists keys matching `auth.public_key` + time range + optional `session_id`, `trace_id`, `input_hash`, `trace_type`; returns each key with a presigned URL.
- `GET /api/public/logs/list` — `list_batch_keys()`: same filtering, returns metadata only (no presigned URLs). Time range is optional.
- `POST /api/public/logs/urls` — `generate_presigned_urls()`: accepts a `keys: list[str]` body, returns presigned URLs for exactly those keys. Used when the UI already knows which keys it wants.
- All three depend on `auth.public_key` for tenant-scoped S3 key prefix construction.
- S3 operations in `src/dataimporter/s3.py` — key-prefix listing via `aiobotocore`, presigned URL generation via `generate_presigned_url("get_object")`.

## Scope
- **In**: Time-range S3 key listing; metadata-only listing; batch presigned URL generation; tenant-scoped key prefix; `session_id`, `trace_id`, `input_hash`, `trace_type` filters.
- **Out**: Non-S3 backends (no equivalent); directory-style browsing (key prefix only); pagination (all matching keys returned in one response).

## Known gaps
- No pagination — a wide time range can return thousands of keys in a single response.
- S3 columns (key, size, last_modified) are hardcoded in the UI table; the schema is not driven by schema discovery.
