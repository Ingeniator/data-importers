# Feature: Streaming S3-to-Dataset Upload

## One-liner
Stream S3 object bodies directly into the dataset service multipart upload without buffering the full file in Python memory.

## Problem (from architecture.md §2)
`worker.import_dataset` reads `content = await obj["Body"].read()` — the entire file is loaded into `bytes` before upload. For large files (e.g. 500 MB S3 batch) this OOMs the worker. The `max_jobs=1` constraint limits blast radius but doesn't prevent the OOM itself.

## Design decisions already made
- Use `httpx` async streaming upload: pass the S3 `StreamingBody` as an async generator into the `httpx` multipart request — no intermediate `bytes` buffer.
- `aiobotocore` `StreamingBody` exposes an `async for chunk` interface compatible with `httpx` streaming.
- Progress tracking: update `bytes_done` in Redis incrementally per chunk rather than post-upload.
- `upload_timeout` still applies per-file — streaming doesn't change the timeout semantics.

## Scope
- **In**: Replace `await body.read()` with streaming generator in `worker.py`; update `dataset_service.upload_file()` to accept an async iterable; incremental `bytes_done` Redis progress updates; integration test with a mock dataset service that validates chunked delivery.
- **Out**: Streaming for the events-path import (events are already in-memory JSON, not S3 objects); S3 multipart download (aiobotocore handles this transparently).

## Open questions
- `httpx` streaming upload with multipart: does the dataset service support chunked transfer encoding (`Transfer-Encoding: chunked`) or does it require `Content-Length`? (If the latter, we must buffer or pre-stat the S3 object for its size.)
