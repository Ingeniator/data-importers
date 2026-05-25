"""Schema discovery endpoint — returns field types from a sample of datasource records."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from dataimporter.auth import AuthContext, get_auth
from dataimporter.config import Datasource, Settings, get_settings

logger = structlog.get_logger(__name__)
router = APIRouter()


def _infer_type(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, list):
        return "list"
    if isinstance(value, dict):
        return "object"
    return "string"


def _flatten_fields(records: list[dict]) -> dict[str, dict]:
    """Collect field names, inferred types, and first non-None example from records."""
    fields: dict[str, dict] = {}
    for rec in records:
        for k, v in rec.items():
            if k.startswith("_"):
                continue
            if k not in fields:
                fields[k] = {"type": _infer_type(v), "example": None}
            if fields[k]["example"] is None and v is not None and not isinstance(v, dict):
                fields[k]["example"] = v if not isinstance(v, list) else (v[:2] if v else None)
    return fields


def _resolve_datasource(
    datasource: str = Query(),
    settings: Settings = Depends(get_settings),
) -> Datasource:
    ds = settings.get_datasource(datasource)
    if ds is None:
        raise HTTPException(status_code=404, detail=f"Datasource '{datasource}' not found")
    return ds


@router.get("/api/public/datasource/sample")
async def datasource_sample(
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    session_id: str | None = Query(default=None),
    trace_id: str | None = Query(default=None),
    trace_type: str | None = Query(default=None),
    input_hash: str | None = Query(default=None),
    keys: list[str] = Query(default=[]),
    ds: Datasource = Depends(_resolve_datasource),
    auth: AuthContext = Depends(get_auth),
) -> dict:
    if start and start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end and end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    records: list[dict] = []

    if ds.type == "s3":
        if not keys:
            raise HTTPException(status_code=400, detail="keys[] required for S3 datasource")
        import asyncio
        from dataimporter.sampling import read_s3_traces_for_sampling
        records = await asyncio.to_thread(read_s3_traces_for_sampling, keys[:5], ds)

    elif ds.type == "langfuse":
        from dataimporter.langfuse import search_logs_langfuse
        records = await search_logs_langfuse(
            query="*", ds=ds,
            start=start, end=end,
            session_id=session_id, trace_id=trace_id,
            trace_type=trace_type, input_hash=input_hash,
            limit=5,
        )

    elif ds.type == "clickhouse":
        from dataimporter.clickhouse import search_logs_ch
        records = await search_logs_ch(
            query="*", ds=ds,
            project_id=auth.public_key,
            is_org_admin=auth.is_org_admin,
            start=start, end=end,
            session_id=session_id, trace_id=trace_id,
            trace_type=trace_type, input_hash=input_hash,
            limit=5,
        )

    elif ds.type == "trino":
        from dataimporter.trino import search_logs_trino
        records = await search_logs_trino(
            query="*", ds=ds,
            project_id=auth.public_key,
            is_org_admin=auth.is_org_admin,
            start=start, end=end,
            session_id=session_id, trace_id=trace_id,
            trace_type=trace_type, input_hash=input_hash,
            limit=5,
        )

    elif ds.type == "chyt":
        from dataimporter.chyt import search_logs_chyt
        records = await search_logs_chyt(
            query="*", ds=ds,
            project_id=auth.public_key,
            is_org_admin=auth.is_org_admin,
            start=start, end=end,
            session_id=session_id, trace_id=trace_id,
            trace_type=trace_type, input_hash=input_hash,
            limit=5,
        )

    else:
        raise HTTPException(status_code=400, detail=f"Schema discovery not supported for '{ds.type}'")

    return {"fields": _flatten_fields(records), "sample_count": len(records)}
