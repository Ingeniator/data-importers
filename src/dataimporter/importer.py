"""Core import logic shared between the arq worker and the inline (no-Redis) path."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

import structlog

from dataimporter import dataset_service
from dataimporter.metrics import IMPORT_BYTES, IMPORT_FILES, IMPORT_SECONDS
from dataimporter.s3 import _s3_client_config, _s3_session

if TYPE_CHECKING:
    from dataimporter.config import DatasetTarget, Datasource

logger = structlog.get_logger(__name__)

OnProgress = Callable[[int, int, int], Awaitable[None]]


async def run_import_dataset(
    target_cfg: DatasetTarget,
    ds: Datasource,
    *,
    keys: list[str],
    dataset_name: str,
    access: str,
    dataset_type: str,
    datasource: str,
    target: str,
    sampling: list[dict] | None = None,
    strict_schema: bool = False,
    schema_snapshot: dict[str, str] | None = None,
    max_traces: int | None = None,
    on_progress: OnProgress | None = None,
) -> dict:
    sampling_warning: str | None = None
    if sampling:
        from dataimporter.sampling import SamplingRule, apply_sampling_s3
        rules = [SamplingRule(**r) for r in sampling]
        keys, sampling_warning = await asyncio.to_thread(
            apply_sampling_s3, keys, rules, ds, strict_schema, schema_snapshot, max_traces,
        )
        if sampling_warning:
            logger.warning("sampling_warning", warning=sampling_warning)

    dataset_id = await dataset_service.create_dataset(
        target_cfg, dataset_name, access, dataset_type,
    )

    if on_progress:
        await on_progress(0, len(keys), 0)

    uploaded = 0
    failed: list[dict] = []
    bytes_done = 0
    labels = {"datasource": datasource, "target": target}
    t0 = time.monotonic()

    s3 = _s3_session(ds)
    async with s3.client("s3", endpoint_url=ds.endpoint, config=_s3_client_config(ds)) as client:
        for i, key in enumerate(keys):
            try:
                obj = await client.get_object(Bucket=ds.bucket, Key=key)
                content: bytes = await obj["Body"].read()
                filename = key.rsplit("/", 1)[-1]
                await dataset_service.upload_file(target_cfg, dataset_id, filename, content)
                uploaded += 1
                bytes_done += len(content)
                IMPORT_FILES.labels(**labels, status="success").inc()
                IMPORT_BYTES.labels(**labels).inc(len(content))
                logger.info("file_exported", key=key, dataset_id=dataset_id, bytes=len(content))
            except Exception as e:
                IMPORT_FILES.labels(**labels, status="failed").inc()
                logger.warning("file_export_failed", key=key, error=str(e))
                failed.append({"key": key, "error": str(e)})
            finally:
                if on_progress:
                    await on_progress(i + 1, len(keys), bytes_done)

    IMPORT_SECONDS.labels(**labels).observe(time.monotonic() - t0)

    return {
        "dataset_id": dataset_id,
        "files_uploaded": uploaded,
        "files_failed": len(failed),
        "failed": failed,
        "bytes_total": bytes_done,
        "sampling_warning": sampling_warning,
    }


async def run_import_dataset_events(
    target_cfg: DatasetTarget,
    *,
    events: list[dict],
    dataset_name: str,
    access: str,
    dataset_type: str,
    datasource: str,
    target: str,
    format: str = "jsonl",
    sampling: list[dict] | None = None,
    strict_schema: bool = False,
    schema_snapshot: dict[str, str] | None = None,
    max_traces: int | None = None,
    on_progress: OnProgress | None = None,
) -> dict:
    events = list(events)
    sampling_warning: str | None = None
    if sampling:
        from dataimporter.sampling import SamplingRule, apply_sampling
        rules = [SamplingRule(**r) for r in sampling]
        events, sampling_warning = await asyncio.to_thread(
            apply_sampling, events, rules, strict_schema, schema_snapshot, max_traces,
        )
        if sampling_warning:
            logger.warning("sampling_warning", warning=sampling_warning)

    dataset_id = await dataset_service.create_dataset(
        target_cfg, dataset_name, access, dataset_type,
    )

    if on_progress:
        await on_progress(0, len(events), 0)

    labels = {"datasource": datasource, "target": target}
    t0 = time.monotonic()
    bytes_total = 0
    files_uploaded = 0

    if format == "individual":
        for i, ev in enumerate(events):
            ev_content = json.dumps(ev, ensure_ascii=False, indent=2).encode()
            ev_filename = f"event-{i:04d}.json"
            await dataset_service.upload_file(target_cfg, dataset_id, ev_filename, ev_content)
            bytes_total += len(ev_content)
            files_uploaded += 1
            IMPORT_FILES.labels(**labels, status="success").inc()
            IMPORT_BYTES.labels(**labels).inc(len(ev_content))
            if on_progress:
                await on_progress(files_uploaded, len(events), bytes_total)
    elif format == "catalog":
        content = json.dumps(events, ensure_ascii=False, indent=2).encode()
        filename = f"{dataset_name}-catalog.json"
        await dataset_service.upload_file(target_cfg, dataset_id, filename, content)
        bytes_total = len(content)
        files_uploaded = 1
        IMPORT_FILES.labels(**labels, status="success").inc()
        IMPORT_BYTES.labels(**labels).inc(len(content))
        if on_progress:
            await on_progress(len(events), len(events), bytes_total)
    else:  # "jsonl" (default)
        content = "\n".join(json.dumps(e, ensure_ascii=False) for e in events).encode()
        filename = f"{dataset_name}.jsonl"
        await dataset_service.upload_file(target_cfg, dataset_id, filename, content)
        bytes_total = len(content)
        files_uploaded = len(events)
        IMPORT_FILES.labels(**labels, status="success").inc(len(events))
        IMPORT_BYTES.labels(**labels).inc(len(content))
        if on_progress:
            await on_progress(len(events), len(events), bytes_total)

    IMPORT_SECONDS.labels(**labels).observe(time.monotonic() - t0)

    logger.info(
        "events_exported",
        dataset_id=dataset_id,
        format=format,
        records=len(events),
        bytes=bytes_total,
        datasource=datasource,
    )

    return {
        "dataset_id": dataset_id,
        "files_uploaded": files_uploaded,
        "files_failed": 0,
        "records_uploaded": len(events),
        "bytes_total": bytes_total,
        "unit": "record",
        "sampling_warning": sampling_warning,
    }
