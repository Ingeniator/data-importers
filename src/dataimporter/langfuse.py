"""Langfuse read adapter — fetches traces and observations via the Langfuse REST API."""

from __future__ import annotations

import logging
from datetime import datetime

import httpx
import structlog

from dataimporter.config import Datasource

logger = structlog.get_logger(__name__)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def _base_url(ds: Datasource) -> str:
    return ds.url.rstrip("/")


def _auth(ds: Datasource) -> httpx.BasicAuth:
    return httpx.BasicAuth(username=ds.access_key_id, password=ds.secret_access_key)


async def search_logs_langfuse(
    query: str,
    ds: Datasource,
    start: datetime | None = None,
    end: datetime | None = None,
    session_id: str | None = None,
    trace_id: str | None = None,
    trace_type: str | None = None,
    input_hash: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Search traces in Langfuse, then fetch their observations."""
    if not ds.url:
        return []

    base = _base_url(ds)
    auth = _auth(ds)

    params: dict[str, str | int] = {"limit": min(limit, 500)}
    if start:
        params["fromTimestamp"] = start.isoformat()
    if end:
        params["toTimestamp"] = end.isoformat()
    if session_id:
        params["sessionId"] = session_id

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            if trace_id:
                resp = await client.get(f"{base}/api/public/traces/{trace_id}", auth=auth)
                resp.raise_for_status()
                trace = resp.json()
                traces = [trace]
            else:
                resp = await client.get(f"{base}/api/public/traces", params=params, auth=auth)
                resp.raise_for_status()
                data = resp.json()
                traces = data.get("data", [])

            results = []
            for t in traces:
                body = {
                    "input": t.get("input"),
                    "output": t.get("output"),
                    "metadata": t.get("metadata"),
                }
                name = t.get("name", "")

                if trace_type and name != trace_type:
                    continue

                if query and query != "*":
                    q = query.lower()
                    searchable = str(body).lower() + name.lower()
                    if q not in searchable:
                        continue

                results.append({
                    "id": t.get("id", ""),
                    "type": "trace",
                    "timestamp": t.get("timestamp", ""),
                    "trace_id": t.get("id", ""),
                    "session_id": t.get("sessionId", ""),
                    "name": name,
                    "body": body,
                    "tags": t.get("tags", []),
                    "latency": t.get("latency"),
                    "total_cost": t.get("totalCost"),
                    "usage": t.get("usage"),
                })

            return results[:limit]

    except Exception as e:
        logger.error("langfuse_search_failed", error=str(e))
        return []


async def ping_langfuse(ds: Datasource) -> None:
    """Health check — verify Langfuse API is reachable."""
    async with httpx.AsyncClient(timeout=3) as client:
        resp = await client.get(
            f"{_base_url(ds)}/api/public/traces",
            params={"limit": 1},
            auth=_auth(ds),
        )
        resp.raise_for_status()
