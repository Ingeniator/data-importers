"""ClickHouse read adapter — search queries only (no inserts)."""

from __future__ import annotations

import json
import logging
from datetime import datetime

import httpx
import structlog

from dataimporter.config import Datasource

logger = structlog.get_logger(__name__)

# Suppress httpx request logging — it leaks ClickHouse password in query params
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)


def _ch_url(ds: Datasource) -> str:
    return f"{ds.url.rstrip('/')}/"


def _ch_params(ds: Datasource) -> dict:
    params = {"database": ds.database}
    if ds.user:
        params["user"] = ds.user
    if ds.password:
        params["password"] = ds.password
    return params


async def search_logs_ch(
    query: str,
    project_id: str,
    ds: Datasource,
    is_org_admin: bool = False,
    start: datetime | None = None,
    end: datetime | None = None,
    session_id: str | None = None,
    trace_id: str | None = None,
    trace_type: str | None = None,
    input_hash: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Full-text search in ClickHouse."""
    if not ds.url:
        return []

    if is_org_admin and "/" in project_id:
        org = project_id.split("/", 1)[0]
        conditions = ["project_id LIKE {project_id:String}"]
        params = {"project_id": f"{org}/%"}
    else:
        conditions = ["project_id = {project_id:String}"]
        params = {"project_id": project_id}

    if start:
        conditions.append("timestamp >= {start:String}")
        params["start"] = start.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    if end:
        conditions.append("timestamp <= {end:String}")
        params["end"] = end.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    if session_id:
        conditions.append("session_id = {session_id:String}")
        params["session_id"] = session_id
    if trace_id:
        conditions.append("trace_id = {trace_id:String}")
        params["trace_id"] = trace_id
    if trace_type:
        conditions.append("name = {trace_type:String}")
        params["trace_type"] = trace_type
    if input_hash:
        conditions.append("input_hash = {input_hash:String}")
        params["input_hash"] = input_hash

    if query and query != "*":
        conditions.append("body ILIKE {query:String}")
        params["query"] = f"%{query}%"

    where = " AND ".join(conditions)
    sql = f"""
        SELECT event_id, event_type, timestamp, project_id, model, name, trace_id, session_id, body
        FROM {ds.database}.{ds.table}
        WHERE {where}
        ORDER BY timestamp DESC
        LIMIT {min(limit, 500)}
        FORMAT JSON
    """

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                _ch_url(ds),
                params={
                    **_ch_params(ds),
                    "query": sql,
                    **{f"param_{k}": v for k, v in params.items()},
                },
            )
            resp.raise_for_status()
            data = resp.json()

        results = []
        for row in data.get("data", []):
            try:
                body = json.loads(row.get("body", "{}"))
            except (json.JSONDecodeError, TypeError):
                body = {}
            results.append({
                "id": row.get("event_id", ""),
                "type": row.get("event_type", ""),
                "timestamp": row.get("timestamp", ""),
                "body": body,
            })
        return results
    except Exception as e:
        logger.error("clickhouse_search_failed", error=str(e))
        return []
