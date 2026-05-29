# Feature: Media Endpoint

## One-liner
Fetch metadata and a presigned download URL for an S3-stored media artifact by its media ID and datasource.

## Problem
LLM traces can reference binary media (images, audio) stored as S3 objects alongside the trace JSONL files. Clients need a way to resolve a media ID to a temporary download URL without exposing S3 credentials.

## Implementation
- `src/dataimporter/routes/media.py` — `GET /api/public/media/{media_id}`.
- Resolves two S3 keys from the media ID: `{prefix}/media/{id}.meta.json` (metadata) and `{prefix}/media/{id}` (blob).
- Returns `GetMediaResponse`: `mediaId`, `contentType`, `contentLength`, `uploadedAt`, `url` (presigned, 1h TTL), `urlExpiry`.
- Auth-scoped: `public_key` from `AuthContext` scopes the S3 key prefix (tenant isolation).
- `datasource` query param selects which S3 datasource to resolve against.

```python
class GetMediaResponse(BaseModel):
    mediaId: str
    contentType: str
    contentLength: int
    uploadedAt: datetime | None
    url: str          # presigned S3 URL, 1-hour TTL
    urlExpiry: str    # ISO-8601
```

## Scope
- **In**: S3 media resolution; presigned URL generation; tenant-scoped key prefix; metadata + blob key convention.
- **Out**: Langfuse media resolution (handled by Langfuse's own `/api/public/media/{id}` — used in asset resolution feature); media upload.

## Known gaps
- Presigned URL TTL (1h) is hardcoded — no config knob.
- No caching of presigned URLs — repeated calls for the same media ID generate new URLs each time.
