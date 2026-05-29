"""Datasource adapter protocol + concrete implementations + factory.

Each adapter wraps one datasource type and exposes a uniform interface so
routes don't need to dispatch on ds.type. Add a new datasource type by
subclassing, implementing search() + ping(), and registering in _REGISTRY.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Protocol, runtime_checkable

import structlog

from dataimporter.auth import AuthContext
from dataimporter.config import Datasource

logger = structlog.get_logger(__name__)


@runtime_checkable
class DatasourceAdapter(Protocol):
    async def search(
        self,
        query: str,
        *,
        auth: AuthContext,
        start: datetime | None = None,
        end: datetime | None = None,
        session_id: str | None = None,
        trace_id: str | None = None,
        trace_type: str | None = None,
        input_hash: str | None = None,
        limit: int = 50,
        time_field: str | None = None,
    ) -> list[dict]: ...

    async def ping(self) -> None: ...


class ClickhouseAdapter:
    def __init__(self, ds: Datasource) -> None:
        self.ds = ds

    async def search(
        self, query: str, *, auth: AuthContext,
        start: datetime | None = None, end: datetime | None = None,
        session_id: str | None = None, trace_id: str | None = None,
        trace_type: str | None = None, input_hash: str | None = None,
        limit: int = 50, time_field: str | None = None,
    ) -> list[dict]:
        from dataimporter.backends.clickhouse import search_logs_ch
        return await search_logs_ch(
            query=query, project_id=auth.public_key, is_org_admin=auth.is_org_admin,
            ds=self.ds, start=start, end=end, session_id=session_id,
            trace_id=trace_id, trace_type=trace_type, input_hash=input_hash,
            limit=limit, time_field=time_field,
        )

    async def ping(self) -> None:
        import httpx
        async with httpx.AsyncClient(timeout=3) as client:
            resp = await client.get(f"{self.ds.url.rstrip('/')}/ping")
            resp.raise_for_status()


class ChytAdapter:
    def __init__(self, ds: Datasource) -> None:
        self.ds = ds

    async def search(
        self, query: str, *, auth: AuthContext,
        start: datetime | None = None, end: datetime | None = None,
        session_id: str | None = None, trace_id: str | None = None,
        trace_type: str | None = None, input_hash: str | None = None,
        limit: int = 50, time_field: str | None = None,
    ) -> list[dict]:
        from dataimporter.backends.chyt import search_logs_chyt
        return await search_logs_chyt(
            query=query, project_id=auth.public_key, is_org_admin=auth.is_org_admin,
            ds=self.ds, start=start, end=end, session_id=session_id,
            trace_id=trace_id, trace_type=trace_type, input_hash=input_hash,
            limit=limit, time_field=time_field,
        )

    async def ping(self) -> None:
        import httpx
        async with httpx.AsyncClient(timeout=3) as client:
            resp = await client.get(f"{self.ds.url.rstrip('/')}/ping")
            resp.raise_for_status()


class TrinoAdapter:
    def __init__(self, ds: Datasource) -> None:
        self.ds = ds

    async def search(
        self, query: str, *, auth: AuthContext,
        start: datetime | None = None, end: datetime | None = None,
        session_id: str | None = None, trace_id: str | None = None,
        trace_type: str | None = None, input_hash: str | None = None,
        limit: int = 50, time_field: str | None = None,
    ) -> list[dict]:
        from dataimporter.backends.trino import search_logs_trino
        return await search_logs_trino(
            query=query, project_id=auth.public_key, is_org_admin=auth.is_org_admin,
            ds=self.ds, start=start, end=end, session_id=session_id,
            trace_id=trace_id, trace_type=trace_type, input_hash=input_hash,
            limit=limit, time_field=time_field,
        )

    async def ping(self) -> None:
        import httpx
        async with httpx.AsyncClient(timeout=3) as client:
            resp = await client.get(f"{self.ds.url.rstrip('/')}/v1/info")
            resp.raise_for_status()


class LangfuseAdapter:
    def __init__(self, ds: Datasource) -> None:
        self.ds = ds

    async def search(
        self, query: str, *, auth: AuthContext,
        start: datetime | None = None, end: datetime | None = None,
        session_id: str | None = None, trace_id: str | None = None,
        trace_type: str | None = None, input_hash: str | None = None,
        limit: int = 50, time_field: str | None = None,
    ) -> list[dict]:
        from dataimporter.backends.langfuse import search_logs_langfuse
        return await search_logs_langfuse(
            query=query, ds=self.ds, start=start, end=end,
            session_id=session_id, trace_id=trace_id,
            trace_type=trace_type, input_hash=input_hash, limit=limit,
        )

    async def ping(self) -> None:
        from dataimporter.backends.langfuse import ping_langfuse
        await ping_langfuse(self.ds)


class S3Adapter:
    def __init__(self, ds: Datasource) -> None:
        self.ds = ds

    async def search(
        self, query: str, *, auth: AuthContext,
        start: datetime | None = None, end: datetime | None = None,
        session_id: str | None = None, trace_id: str | None = None,
        trace_type: str | None = None, input_hash: str | None = None,
        limit: int = 50, time_field: str | None = None,
    ) -> list[dict]:
        from dataimporter.s3 import list_batch_keys
        from dataimporter.search import search_logs

        keys_meta = await list_batch_keys(
            auth, self.ds, start=start, end=end,
            session_id=session_id, trace_id=trace_id,
            trace_type=trace_type, input_hash=input_hash,
        )
        keys = [f["key"] for f in keys_meta]
        logger.info("search_scope", query=query, backend="duckdb", files=len(keys))
        if not keys:
            return []
        return await asyncio.to_thread(search_logs, keys, query, self.ds, limit)

    async def ping(self) -> None:
        from dataimporter.s3 import ping_s3
        await asyncio.wait_for(ping_s3(self.ds), timeout=3)


_REGISTRY: dict[str, type] = {
    "clickhouse": ClickhouseAdapter,
    "chyt": ChytAdapter,
    "trino": TrinoAdapter,
    "langfuse": LangfuseAdapter,
    "s3": S3Adapter,
}


def get_adapter(ds: Datasource) -> DatasourceAdapter:
    """Return the adapter for ds, or raise ValueError if the type is unknown."""
    cls = _REGISTRY.get(ds.type)
    if cls is None:
        raise ValueError(f"Unknown datasource type: {ds.type!r}")
    return cls(ds)
