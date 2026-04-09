"""Full-text search endpoint — supports duckdb (over S3) and clickhouse backends."""

from __future__ import annotations

from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from dataimporter.auth import AuthContext, get_auth
from dataimporter.config import Datasource, Settings, get_settings

logger = structlog.get_logger(__name__)

router = APIRouter()


def _resolve_datasource(
    datasource: str = Query(),
    settings: Settings = Depends(get_settings),
) -> Datasource:
    ds = settings.get_datasource(datasource)
    if ds is None:
        raise HTTPException(status_code=404, detail=f"Datasource '{datasource}' not found")
    return ds


@router.get("/api/public/logs/search")
async def search(
    q: str = Query(min_length=1),
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    session_id: str | None = Query(default=None),
    trace_id: str | None = Query(default=None),
    trace_type: str | None = Query(default=None),
    input_hash: str | None = Query(default=None),
    limit: int = Query(default=50, le=500),
    ds: Datasource = Depends(_resolve_datasource),
    auth: AuthContext = Depends(get_auth),
) -> dict:
    if start and start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end and end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    if ds.type == "clickhouse":
        from dataimporter.clickhouse import search_logs_ch

        results = await search_logs_ch(
            query=q,
            project_id=auth.public_key,
            is_org_admin=auth.is_org_admin,
            ds=ds,
            start=start, end=end,
            session_id=session_id, trace_id=trace_id,
            trace_type=trace_type, input_hash=input_hash,
            limit=limit,
        )
        return {"results": results, "backend": "clickhouse"}

    if ds.type == "trino":
        from dataimporter.trino import search_logs_trino

        results = await search_logs_trino(
            query=q,
            project_id=auth.public_key,
            is_org_admin=auth.is_org_admin,
            ds=ds,
            start=start, end=end,
            session_id=session_id, trace_id=trace_id,
            trace_type=trace_type, input_hash=input_hash,
            limit=limit,
        )
        return {"results": results, "backend": "trino"}

    if ds.type == "s3":
        from dataimporter.s3 import list_batch_keys
        from dataimporter.search import search_logs

        keys_meta = await list_batch_keys(
            auth, ds, start=start, end=end,
            session_id=session_id, trace_id=trace_id,
            trace_type=trace_type, input_hash=input_hash,
        )
        keys = [f["key"] for f in keys_meta]

        logger.info("search_scope", query=q, backend="duckdb", files=len(keys))

        if not keys:
            return {"results": [], "files_scanned": 0, "backend": "duckdb"}

        import asyncio
        results = await asyncio.to_thread(search_logs, keys, q, ds, limit)

        return {"results": results, "files_scanned": len(keys), "keys": keys, "backend": "duckdb"}

    raise HTTPException(status_code=400, detail=f"Search not supported for datasource type '{ds.type}'")
