from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from dataimporter.auth import AuthContext, get_auth
from dataimporter.config import Datasource
from dataimporter.routes.deps import get_s3_datasource
from dataimporter.s3 import generate_presigned_urls, list_batch_keys, list_batch_urls

router = APIRouter()


@router.get("/api/public/logs")
async def get_logs(
    start: datetime = Query(),
    end: datetime = Query(),
    session_id: str | None = Query(default=None),
    trace_id: str | None = Query(default=None),
    input_hash: str | None = Query(default=None),
    trace_type: str | None = Query(default=None),
    ds: Datasource = Depends(get_s3_datasource),
    auth: AuthContext = Depends(get_auth),
) -> dict:
    if start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    files = await list_batch_urls(
        auth, ds, start, end,
        session_id=session_id, trace_id=trace_id, input_hash=input_hash,
        trace_type=trace_type,
    )
    return {"files": files}


@router.get("/api/public/logs/list")
async def list_logs(
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    session_id: str | None = Query(default=None),
    trace_id: str | None = Query(default=None),
    input_hash: str | None = Query(default=None),
    trace_type: str | None = Query(default=None),
    ds: Datasource = Depends(get_s3_datasource),
    auth: AuthContext = Depends(get_auth),
) -> dict:
    if start and start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end and end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    files = await list_batch_keys(
        auth, ds, start=start, end=end,
        session_id=session_id, trace_id=trace_id, input_hash=input_hash,
        trace_type=trace_type,
    )
    return {"files": files}


class PresignRequest(BaseModel):
    keys: list[str]


@router.post("/api/public/logs/urls")
async def get_urls(
    body: PresignRequest,
    ds: Datasource = Depends(get_s3_datasource),
    auth: AuthContext = Depends(get_auth),
) -> dict:
    files = await generate_presigned_urls(body.keys, auth, ds)
    return {"files": files}
