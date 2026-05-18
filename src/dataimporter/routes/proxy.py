"""Proxy endpoint — user provides credentials, server validates URL against allowlist."""

from __future__ import annotations

from datetime import datetime, timezone

import structlog
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from dataimporter.config import Datasource, Settings, get_settings

logger = structlog.get_logger(__name__)

router = APIRouter()


class UserCredentials(BaseModel):
    """Credentials the user provides from the browser."""
    connection_url: str  # must match an allowlisted connection URL
    access_key_id: str = ""
    secret_access_key: str = ""
    # S3-specific (ignored for non-S3 connections)
    bucket: str = ""
    key_prefix: str = ""
    region: str = ""


class ProxySearchRequest(BaseModel):
    credentials: UserCredentials
    q: str
    start: datetime | None = None
    end: datetime | None = None
    session_id: str | None = None
    trace_id: str | None = None
    trace_type: str | None = None
    input_hash: str | None = None
    limit: int = 50


class ProxyPingRequest(BaseModel):
    credentials: UserCredentials


def _resolve_connection(creds: UserCredentials, settings: Settings) -> Datasource:
    """Build a Datasource from allowlisted connection + user/server credentials.

    User-supplied credentials take priority; falls back to server-configured
    public_key/secret_key on the connection template.
    Raises HTTPException if the URL is not in the allowlist.
    """
    url = creds.connection_url.rstrip("/")
    for conn in settings.connections:
        s3_wildcard = conn.type == "s3" and conn.url == "*"
        url_match = conn.url.rstrip("/") == url
        if not (s3_wildcard or url_match):
            continue
        if conn.type == "s3":
            endpoint = creds.connection_url if s3_wildcard else conn.url
            bucket = creds.bucket or conn.bucket
            if not bucket:
                raise HTTPException(status_code=400, detail="S3 connection requires a bucket name")
            return Datasource(
                name="__user__",
                type="s3",
                endpoint=endpoint,
                bucket=bucket,
                key_prefix=creds.key_prefix or conn.key_prefix,
                region=creds.region or conn.region,
                addressing_style=conn.addressing_style,
                access_key_id=creds.access_key_id or conn.public_key,
                secret_access_key=creds.secret_access_key or conn.secret_key,
            )
        return Datasource(
            name="__user__",
            type=conn.type,
            url=conn.url,
            access_key_id=creds.access_key_id or conn.public_key,
            secret_access_key=creds.secret_access_key or conn.secret_key,
        )
    raise HTTPException(status_code=403, detail="Connection URL not in allowlist")


@router.post("/api/public/proxy/search")
async def proxy_search(
    req: ProxySearchRequest,
    settings: Settings = Depends(get_settings),
) -> dict:
    """Search using user-provided credentials against an allowlisted host."""
    ds = _resolve_connection(req.credentials, settings)

    start = req.start
    end = req.end
    if start and start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end and end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    limit = min(req.limit, 500)

    if ds.type == "langfuse":
        from dataimporter.langfuse import search_logs_langfuse

        results = await search_logs_langfuse(
            query=req.q, ds=ds,
            start=start, end=end,
            session_id=req.session_id, trace_id=req.trace_id,
            trace_type=req.trace_type, input_hash=req.input_hash,
            limit=limit,
        )
        return {"results": results, "backend": "langfuse"}

    if ds.type == "s3":
        from dataimporter.s3 import list_objects_proxy

        results = await list_objects_proxy(
            ds=ds,
            start=start, end=end,
            session_id=req.session_id, trace_id=req.trace_id,
            trace_type=req.trace_type, input_hash=req.input_hash,
            limit=limit,
        )
        return {"results": results, "backend": "s3"}

    raise HTTPException(status_code=400, detail=f"Proxy search not supported for type '{ds.type}'")


@router.post("/api/public/proxy/ping")
async def proxy_ping(
    req: ProxyPingRequest,
    settings: Settings = Depends(get_settings),
) -> dict:
    """Test a user-provided connection against an allowlisted host."""
    ds = _resolve_connection(req.credentials, settings)

    try:
        if ds.type == "langfuse":
            from dataimporter.langfuse import ping_langfuse
            await ping_langfuse(ds)
        elif ds.type == "s3":
            from dataimporter.s3 import ping_s3
            await ping_s3(ds)
        else:
            raise HTTPException(status_code=400, detail=f"Ping not supported for type '{ds.type}'")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=str(e))

    return {"status": "ok"}
