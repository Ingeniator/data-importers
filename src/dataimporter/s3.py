"""S3 read adapter — list, presign, and metadata operations (no writes)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

import aioboto3
import structlog
from botocore.config import Config as BotoConfig

from dataimporter.auth import AuthContext
from dataimporter.config import Datasource

logger = structlog.get_logger(__name__)


def _s3_client_config(ds: Datasource) -> BotoConfig:
    return BotoConfig(
        s3={"addressing_style": ds.addressing_style, "payload_signing_enabled": False},
        signature_version="s3v4",
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )


def _s3_session(ds: Datasource) -> aioboto3.Session:
    return aioboto3.Session(
        aws_access_key_id=ds.access_key_id,
        aws_secret_access_key=ds.secret_access_key,
        region_name=ds.region,
    )


S3_KEY_TS_FORMAT = "%Y%m%dT%H%M%SZ"


@dataclass
class KeyMeta:
    session_id: str
    trace_id: str
    trace_type: str
    input_hash: str
    timestamp: datetime


def parse_key_meta(key: str) -> KeyMeta | None:
    """Parse S3 key: {public_key}/{session_id}_{trace_id}_{trace_type}_{input_hash}_{ts}_{uuid}.jsonl"""
    try:
        filename = key.rsplit("/", 1)[-1]
        name = filename.removesuffix(".jsonl").removesuffix(".json")
        parts = name.split("_")
        ts_str = parts[-2]
        input_hash = parts[-3]
        trace_type = parts[-4]
        prefix_parts = parts[:-4]
        session_id = prefix_parts[0]
        trace_id = "_".join(prefix_parts[1:]) if len(prefix_parts) > 1 else "unknown"
        ts = datetime.strptime(ts_str, S3_KEY_TS_FORMAT).replace(tzinfo=timezone.utc)
        return KeyMeta(
            session_id=session_id,
            trace_id=trace_id,
            trace_type=trace_type,
            input_hash=input_hash,
            timestamp=ts,
        )
    except (ValueError, IndexError):
        return None


def _list_prefix(auth: AuthContext, ds: Datasource) -> str:
    """Build S3 prefix for listing. Org admins see the whole org."""
    if auth.is_org_admin and "/" in auth.public_key:
        org = auth.public_key.split("/", 1)[0]
        prefix = f"{org}/"
    else:
        prefix = f"{auth.public_key}/"
    if ds.key_prefix:
        prefix = f"{ds.key_prefix.strip('/')}/{prefix}"
    return prefix


async def list_batch_keys(
    auth: AuthContext,
    ds: Datasource,
    start: datetime | None = None,
    end: datetime | None = None,
    session_id: str | None = None,
    trace_id: str | None = None,
    input_hash: str | None = None,
    trace_type: str | None = None,
) -> list[dict]:
    """List batch keys (metadata only, no presigned URLs)."""
    prefix = _list_prefix(auth, ds)
    session = _s3_session(ds)
    results: list[dict] = []
    async with session.client("s3", endpoint_url=ds.endpoint, config=_s3_client_config(ds)) as client:
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=ds.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                meta = parse_key_meta(key)
                if meta is None:
                    continue
                if start and meta.timestamp < start:
                    continue
                if end and meta.timestamp > end:
                    continue
                if session_id and meta.session_id != session_id:
                    continue
                if trace_id and meta.trace_id != trace_id:
                    continue
                if input_hash and meta.input_hash != input_hash:
                    continue
                if trace_type and meta.trace_type != trace_type:
                    continue
                results.append({
                    "key": key,
                    "session_id": meta.session_id,
                    "trace_id": meta.trace_id,
                    "trace_type": meta.trace_type,
                    "input_hash": meta.input_hash,
                    "timestamp": meta.timestamp.isoformat(),
                })
    return results


async def generate_presigned_urls(
    keys: list[str],
    auth: AuthContext,
    ds: Datasource,
) -> list[dict]:
    """Generate presigned URLs for given keys, validating ownership."""
    prefix = _list_prefix(auth, ds)
    results: list[dict] = []
    session = _s3_session(ds)
    async with session.client("s3", endpoint_url=ds.endpoint, config=_s3_client_config(ds)) as client:
        for key in keys:
            if not key.startswith(prefix):
                continue
            url = await client.generate_presigned_url(
                "get_object",
                Params={"Bucket": ds.bucket, "Key": key},
                ExpiresIn=ds.presign_expiry,
            )
            if ds.public_endpoint and ds.endpoint:
                url = url.replace(ds.endpoint, ds.public_endpoint, 1)
            results.append({"key": key, "url": url})
    return results


async def list_batch_urls(
    auth: AuthContext,
    ds: Datasource,
    start: datetime,
    end: datetime,
    session_id: str | None = None,
    trace_id: str | None = None,
    input_hash: str | None = None,
    trace_type: str | None = None,
) -> list[dict]:
    prefix = _list_prefix(auth, ds)
    session = _s3_session(ds)
    results: list[dict] = []
    async with session.client("s3", endpoint_url=ds.endpoint, config=_s3_client_config(ds)) as client:
        paginator = client.get_paginator("list_objects_v2")
        async for page in paginator.paginate(Bucket=ds.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                meta = parse_key_meta(key)
                if meta is None or not (start <= meta.timestamp <= end):
                    continue
                if session_id and meta.session_id != session_id:
                    continue
                if trace_id and meta.trace_id != trace_id:
                    continue
                if input_hash and meta.input_hash != input_hash:
                    continue
                if trace_type and meta.trace_type != trace_type:
                    continue
                url = await client.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": ds.bucket, "Key": key},
                    ExpiresIn=ds.presign_expiry,
                )
                if ds.public_endpoint and ds.endpoint:
                    url = url.replace(ds.endpoint, ds.public_endpoint, 1)
                results.append({
                    "key": key,
                    "session_id": meta.session_id,
                    "trace_id": meta.trace_id,
                    "trace_type": meta.trace_type,
                    "input_hash": meta.input_hash,
                    "timestamp": meta.timestamp.isoformat(),
                    "url": url,
                })
    return results
