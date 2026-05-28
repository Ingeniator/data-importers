"""Full-text search endpoint — dispatches to the datasource adapter."""

from __future__ import annotations

from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from dataimporter.adapters import get_adapter
from dataimporter.auth import AuthContext, get_auth
from dataimporter.config import Datasource, Settings, get_settings
from dataimporter.filters import apply_filters, parse_filters

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
    filters: str | None = Query(default=None),
    time_field: str | None = Query(default=None),
    ds: Datasource = Depends(_resolve_datasource),
    auth: AuthContext = Depends(get_auth),
) -> dict:
    if start and start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end and end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    filter_rules = parse_filters(filters)

    try:
        adapter = get_adapter(ds)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Search not supported for datasource type '{ds.type}'")

    results = await adapter.search(
        q, auth=auth,
        start=start, end=end,
        session_id=session_id, trace_id=trace_id,
        trace_type=trace_type, input_hash=input_hash,
        limit=limit, time_field=time_field,
    )

    if filter_rules:
        results = apply_filters(results, filter_rules)

    return {"results": results, "backend": ds.type}
