# Feature: Import Job Cancellation & Deduplication

## One-liner
Let users cancel a running import job, and prevent duplicate jobs when the same export is submitted twice.

## Problem (from architecture.md §1)
1. No cancel — once an import is queued there is no way to stop it, even if the user submitted it by mistake.
2. No deduplication — submitting the same export twice silently creates two sequential jobs, both burning worker time and dataset service quota.

## Design decisions already made
- arq supports job cancellation via `await job.abort()` and deduplication via a stable `job_id`.
- Deterministic job key: `sha256(target_name + datasource_name + sorted(keys))[:16]` — resubmitting the identical set returns the existing job ID without queueing a new one.
- Cancel endpoint: `DELETE /api/public/export/{job_id}` — calls `arq.Job(job_id).abort()`, sets status to `"cancelled"` in the Redis hash, returns 200.
- UI: add a **Cancel** button next to the progress bar, visible while status is `queued` or `running`.

## Scope
- **In**: `DELETE /api/public/export/{job_id}`; deterministic job ID dedup on enqueue; `"cancelled"` status in progress poll response; Cancel button in import modal (`#cancel-import-btn`).
- **Out**: Job retry UI; job history list; cancellation of synchronous (no-Redis) imports.

## Open questions
- Should aborting a mid-run job attempt to roll back the partially created dataset in the dataset service, or leave it as-is with a note?
