# Feature: Asset Resolution (Artifact Ownership)

## One-liner
When importing traces, fetch and store any referenced media artifacts (images, audio, files) into the dataset service instead of keeping external Langfuse/S3 presigned-URL links that expire.

## Problem (from todo.md)
Imported datasets reference artifacts via Langfuse signed URLs or S3 presigned URLs. These expire (Langfuse retention, S3 TTL) or break on migration, making traces non-reproducible after the fact. The dataset service must own a copy of the artifact data.

## Design decisions already made
- `asset_resolve: true` is already sent in the export payload as a flag — backend implementation is the missing piece.
- Worker resolves assets during import: for each trace in the import set, detect referenced media URLs, fetch them, and upload to the dataset service as associated files.
- `GET /api/public/media/{id}` already fetches Langfuse media metadata + presigned URL — reuse for resolution.
- Asset resolution is opt-in (flag in import modal: "Resolve and copy media artifacts") to avoid unexpected storage costs.

## Scope
- **In**: Worker-side asset fetch + re-upload for Langfuse media references; `asset_resolve` flag wired end-to-end; progress tracking counts asset bytes separately; UI checkbox `#asset-resolve-checkbox` in import modal.
- **Out**: S3 native path asset resolution (separate from events path); video/large binary streaming (cap at configurable size, e.g. 50 MB per artifact); de-duplication of identical assets across datasets.

## Open questions
- Which URL patterns should be treated as resolvable assets? (Langfuse `/api/public/media/{id}`, S3 presigned, or any `http(s)://` reference in trace JSON?)
- What happens when an asset fetch fails — skip silently, warn, or abort the import?
