"""Integration test — exercises dataimporter.langfuse against a real Langfuse.

Activated only when LANGFUSE_URL / LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY are
set. Intended to run inside docker compose (see the `dataimporter-tests`
service), where it points at `http://langfuse-web:3000`.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timezone

import httpx
import pytest

import aioboto3
from botocore.config import Config as BotoConfig

from dataimporter.config import Datasource
from dataimporter.backends.langfuse import ping_langfuse, search_logs_langfuse

LANGFUSE_URL = os.environ.get("LANGFUSE_URL")
LANGFUSE_PUBLIC_KEY = os.environ.get("LANGFUSE_PUBLIC_KEY")
LANGFUSE_SECRET_KEY = os.environ.get("LANGFUSE_SECRET_KEY")
INGEST_TIMEOUT = float(os.environ.get("LANGFUSE_INGEST_TIMEOUT", "60"))

S3_ENDPOINT = os.environ.get("S3_ENDPOINT")
S3_ACCESS_KEY_ID = os.environ.get("S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY = os.environ.get("S3_SECRET_ACCESS_KEY")
S3_BUCKET = os.environ.get("S3_BUCKET", "trace-assets")

pytestmark = pytest.mark.skipif(
    not (LANGFUSE_URL and LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY),
    reason="Set LANGFUSE_URL/LANGFUSE_PUBLIC_KEY/LANGFUSE_SECRET_KEY to run",
)


def _s3_client_kwargs(ds: Datasource) -> dict:
    return dict(
        endpoint_url=ds.endpoint,
        aws_access_key_id=ds.access_key_id,
        aws_secret_access_key=ds.secret_access_key,
        config=BotoConfig(
            s3={"addressing_style": ds.addressing_style},
            signature_version="s3v4",
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


async def _upload_asset(key: str, data: bytes, content_type: str, ds: Datasource) -> None:
    session = aioboto3.Session()
    async with session.client("s3", **_s3_client_kwargs(ds)) as client:
        await client.put_object(Bucket=ds.bucket, Key=key, Body=data, ContentType=content_type)


async def _presign_asset(key: str, ds: Datasource) -> str:
    session = aioboto3.Session()
    async with session.client("s3", **_s3_client_kwargs(ds)) as client:
        return await client.generate_presigned_url(
            "get_object",
            Params={"Bucket": ds.bucket, "Key": key},
            ExpiresIn=3600,
        )


@pytest.fixture
def s3_ds() -> Datasource:
    if not (S3_ENDPOINT and S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY):
        pytest.skip("Set S3_ENDPOINT/S3_ACCESS_KEY_ID/S3_SECRET_ACCESS_KEY to run")
    return Datasource(
        name="minio-it",
        type="s3",
        bucket=S3_BUCKET,
        endpoint=S3_ENDPOINT,
        access_key_id=S3_ACCESS_KEY_ID or "",
        secret_access_key=S3_SECRET_ACCESS_KEY or "",
        addressing_style="path",
    )


@pytest.fixture
def langfuse_ds() -> Datasource:
    return Datasource(
        name="langfuse-it",
        type="langfuse",
        url=LANGFUSE_URL or "",
        access_key_id=LANGFUSE_PUBLIC_KEY or "",
        secret_access_key=LANGFUSE_SECRET_KEY or "",
    )


async def _ingest_trace(ds: Datasource, trace_id: str, name: str, body: dict) -> None:
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "batch": [
            {
                "id": str(uuid.uuid4()),
                "type": "trace-create",
                "timestamp": now,
                "body": {
                    "id": trace_id,
                    "timestamp": now,
                    "name": name,
                    "input": body.get("input"),
                    "output": body.get("output"),
                    "metadata": body.get("metadata"),
                    "sessionId": body.get("sessionId"),
                },
            }
        ],
    }
    auth = httpx.BasicAuth(ds.access_key_id, ds.secret_access_key)
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(f"{ds.url.rstrip('/')}/api/public/ingestion", json=payload, auth=auth)
        resp.raise_for_status()


async def _wait_for(predicate, timeout: float) -> bool:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if await predicate():
            return True
        await asyncio.sleep(1.0)
    return False


@pytest.mark.asyncio
async def test_ping_langfuse(langfuse_ds: Datasource):
    await ping_langfuse(langfuse_ds)


@pytest.mark.asyncio
async def test_search_finds_seeded_trace_by_keyword(langfuse_ds: Datasource):
    marker = uuid.uuid4().hex[:12]
    trace_id = f"trace-{marker}"
    await _ingest_trace(
        langfuse_ds,
        trace_id=trace_id,
        name=f"it-{marker}",
        body={
            "input": {"prompt": f"hello {marker}"},
            "output": {"text": "world"},
            "metadata": {"test_marker": marker},
        },
    )

    async def found() -> bool:
        results = await search_logs_langfuse(query=marker, ds=langfuse_ds, limit=50)
        return any(r.get("trace_id") == trace_id for r in results)

    assert await _wait_for(found, INGEST_TIMEOUT), (
        f"trace {trace_id} not searchable within {INGEST_TIMEOUT}s"
    )


@pytest.mark.asyncio
async def test_search_by_trace_id(langfuse_ds: Datasource):
    marker = uuid.uuid4().hex[:12]
    trace_id = f"trace-{marker}"
    await _ingest_trace(
        langfuse_ds,
        trace_id=trace_id,
        name=f"it-{marker}",
        body={"input": {"q": marker}, "output": {"a": "ok"}},
    )

    async def found() -> bool:
        results = await search_logs_langfuse(
            query="*", ds=langfuse_ds, trace_id=trace_id, limit=1,
        )
        return any(r.get("trace_id") == trace_id for r in results)

    assert await _wait_for(found, INGEST_TIMEOUT), (
        f"trace {trace_id} not fetchable by id within {INGEST_TIMEOUT}s"
    )


@pytest.mark.asyncio
async def test_search_filters_by_session_id(langfuse_ds: Datasource):
    marker = uuid.uuid4().hex[:12]
    session_id = f"sess-{marker}"
    trace_id = f"trace-{marker}"
    await _ingest_trace(
        langfuse_ds,
        trace_id=trace_id,
        name=f"it-{marker}",
        body={
            "input": {"prompt": marker},
            "output": {"text": "ok"},
            "sessionId": session_id,
        },
    )

    async def found() -> bool:
        results = await search_logs_langfuse(
            query="*", ds=langfuse_ds, session_id=session_id, limit=10,
        )
        return any(r.get("session_id") == session_id for r in results)

    assert await _wait_for(found, INGEST_TIMEOUT), (
        f"session {session_id} not visible within {INGEST_TIMEOUT}s"
    )


@pytest.mark.asyncio
async def test_ingest_trace_with_s3_asset(langfuse_ds: Datasource, s3_ds: Datasource):
    marker = uuid.uuid4().hex[:12]
    trace_id = f"trace-asset-{marker}"
    asset_key = f"test-assets/{trace_id}.txt"
    asset_data = f"asset content for trace {marker}".encode()

    await _upload_asset(asset_key, asset_data, "text/plain", s3_ds)
    asset_url = await _presign_asset(asset_key, s3_ds)

    await _ingest_trace(
        langfuse_ds,
        trace_id=trace_id,
        name=f"it-asset-{marker}",
        body={
            "input": {"prompt": f"describe asset {marker}"},
            "output": {"text": "asset described"},
            "metadata": {"asset_url": asset_url, "asset_key": asset_key},
        },
    )

    async def found() -> bool:
        results = await search_logs_langfuse(query=marker, ds=langfuse_ds, limit=50)
        return any(r.get("trace_id") == trace_id for r in results)

    assert await _wait_for(found, INGEST_TIMEOUT), (
        f"trace {trace_id} with asset not searchable within {INGEST_TIMEOUT}s"
    )

    results = await search_logs_langfuse(query="*", ds=langfuse_ds, trace_id=trace_id, limit=1)
    assert results, f"trace {trace_id} not fetchable by id"
    assert results[0]["body"]["metadata"]["asset_key"] == asset_key
