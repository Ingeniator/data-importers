# Feature: Queue-based Import Pipeline

## One-liner
Import selected traces into the dataset service via an arq job queue (Redis) with live progress tracking; falls back to synchronous execution when Redis is not configured.

## Problem
Import jobs are long-running and memory-intensive. Without a queue, concurrent imports from multiple users each buffer full file sets into memory, risking OOM. A single-worker queue serialises imports and provides progress visibility.

## Implementation
- `src/dataimporter/importer.py` — `run_import_dataset()` (S3 keys path) and `run_import_dataset_events()` (in-memory events path).
- `src/dataimporter/worker.py` — arq worker task; `max_jobs=1` (one import at a time).
- `src/dataimporter/queue.py` — `get_pool()`, `is_queue_available()`, `PROGRESS_KEY` Redis hash helpers.
- `src/dataimporter/dataset_service.py` — OAuth2 `client_credentials` token exchange + `create_dataset()` + `upload_file()`.
- `POST /api/public/export/dataset` (S3 keys) and `POST /api/public/export/dataset/events` — enqueue or run inline.
- `GET /api/public/export/status/{job_id}` — polls arq job status + Redis progress hash (`files_done`, `files_total`, `bytes_done`).
- Prometheus metrics: `dataimporter_import_files_total`, `dataimporter_import_bytes_total`, `dataimporter_import_seconds`.
- Upload formats (events path): `jsonl`, `individual` (one file per event), `catalog`.
- Tokens cached in-process with 30s safety margin before expiry.

## Scope
- **In**: S3 keys import; in-memory events import; arq queue + sync fallback; progress polling; sampling integration; OAuth2 token cache; Prometheus metrics.
- **Out**: Job cancellation (planned `docs/features/01-job-cancellation.md`); streaming upload (planned `docs/features/03-streaming-s3-upload.md`); multi-worker parallelism.

## Known gaps
- Full S3 file buffered in memory before upload (`await body.read()`) — OOM risk for large files (see `docs/features/03-streaming-s3-upload.md`).
- No job deduplication — identical exports create sequential duplicate jobs.
