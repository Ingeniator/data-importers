"""Trino read adapter — search queries only (no inserts)."""

from __future__ import annotations

import json
import logging
from datetime import datetime

import httpx
import structlog

from dataimporter.config import Datasource

logger = structlog.get_logger(__name__)

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

_POLL_INTERVAL = 0.1  # seconds between polling Trino for results


def _statement_url(ds: Datasource) -> str:
    return f"{ds.url.rstrip('/')}/v1/statement"


def _headers(ds: Datasource) -> dict[str, str]:
    h: dict[str, str] = {}
    if ds.user:
        h["X-Trino-User"] = ds.user
    if ds.catalog:
        h["X-Trino-Catalog"] = ds.catalog
    if ds.schema_name:
        h["X-Trino-Schema"] = ds.schema_name
    return h


async def _execute(ds: Datasource, sql: str, timeout: float = 30) -> list[dict]:
    """Submit SQL to Trino and poll until results are ready."""
    import asyncio

    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            _statement_url(ds),
            content=sql,
            headers={**_headers(ds), "Content-Type": "text/plain"},
        )
        resp.raise_for_status()
        body = resp.json()

        columns: list[str] = []
        rows: list[dict] = []

        while True:
            if "columns" in body and not columns:
                columns = [c["name"] for c in body["columns"]]
            if "data" in body:
                for row in body["data"]:
                    rows.append(dict(zip(columns, row)))

            next_uri = body.get("nextUri")
            if not next_uri:
                break

            await asyncio.sleep(_POLL_INTERVAL)
            resp = await client.get(next_uri, headers=_headers(ds))
            resp.raise_for_status()
            body = resp.json()

        if error := body.get("error"):
            raise RuntimeError(f"Trino query failed: {error.get('message', error)}")

        return rows


async def search_logs_trino(
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
    """Full-text search in Trino."""
    if not ds.url:
        return []

    table = f"{ds.catalog}.{ds.schema_name}.{ds.table}" if ds.catalog and ds.schema_name else ds.table

    if is_org_admin and "/" in project_id:
        org = project_id.split("/", 1)[0]
        conditions = [f"project_id LIKE '{_esc(org)}/%'"]
    else:
        conditions = [f"project_id = '{_esc(project_id)}'"]

    if start:
        conditions.append(f"timestamp >= TIMESTAMP '{start.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'")
    if end:
        conditions.append(f"timestamp <= TIMESTAMP '{end.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'")
    if session_id:
        conditions.append(f"session_id = '{_esc(session_id)}'")
    if trace_id:
        conditions.append(f"trace_id = '{_esc(trace_id)}'")
    if trace_type:
        conditions.append(f"name = '{_esc(trace_type)}'")
    if input_hash:
        conditions.append(f"input_hash = '{_esc(input_hash)}'")

    if query and query != "*":
        conditions.append(f"body LIKE '%{_esc(query)}%'")

    where = " AND ".join(conditions)
    sql = f"""
        SELECT event_id, event_type, timestamp, project_id, model, name, trace_id, session_id, body
        FROM {table}
        WHERE {where}
        ORDER BY timestamp DESC
        LIMIT {min(limit, 500)}
    """

    try:
        rows = await _execute(ds, sql)
        results = []
        for row in rows:
            try:
                body = json.loads(row.get("body", "{}"))
            except (json.JSONDecodeError, TypeError):
                body = {}
            results.append({
                "id": row.get("event_id", ""),
                "type": row.get("event_type", ""),
                "timestamp": str(row.get("timestamp", "")),
                "body": body,
            })
        return results
    except Exception as e:
        logger.error("trino_search_failed", error=str(e))
        return []


def _esc(value: str) -> str:
    """Escape single quotes for Trino SQL string literals."""
    return value.replace("'", "''")
