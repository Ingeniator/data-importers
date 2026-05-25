"""Export route — queue-based import when Redis is available, sync fallback otherwise."""

from __future__ import annotations

import asyncio
import time

import httpx
from arq.jobs import Job, JobStatus
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from dataimporter import dataset_service
from dataimporter.auth import AuthContext, get_auth
from dataimporter.config import DatasetTarget, Datasource, Settings, get_settings
from dataimporter.metrics import IMPORT_BYTES, IMPORT_FILES, IMPORT_SECONDS
from dataimporter.queue import get_pool, is_queue_available
from dataimporter.s3 import _s3_client_config, _s3_session

import structlog

logger = structlog.get_logger(__name__)

router = APIRouter()

_PROGRESS_KEY = "dataimporter:progress:{job_id}"
_NO_REDIS_WARNING = (
    "Import ran synchronously — redis_url is not configured. "
    "Configure redis_url to enable the queue and prevent concurrent OOM."
)


class ExportRequest(BaseModel):
    target: str
    datasource: str
    keys: list[str]
    dataset_name: str
    access: str = "organization"
    dataset_type: str = "DATASET"
    sampling: list[dict] | None = None
    strict_schema: bool = False
    schema_snapshot: dict[str, str] | None = None
    max_traces: int | None = None


def _resolve_target(name: str, settings: Settings) -> DatasetTarget:
    t = settings.get_target(name)
    if t is None:
        raise HTTPException(status_code=404, detail=f"Target '{name}' not found")
    return t


def _resolve_s3_datasource(name: str, settings: Settings) -> Datasource:
    ds = settings.get_datasource(name)
    if ds is None:
        raise HTTPException(status_code=404, detail=f"Datasource '{name}' not found")
    if ds.type != "s3":
        raise HTTPException(status_code=400, detail=f"Datasource '{name}' is not an S3 datasource")
    return ds


async def _run_inline(
    target: DatasetTarget,
    ds: Datasource,
    req: ExportRequest,
) -> dict:
    """Synchronous import path used when Redis is not configured."""
    keys = req.keys
    sampling_warning: str | None = None

    if req.sampling:
        from dataimporter.sampling import SamplingRule, apply_sampling_s3
        rules = [SamplingRule(**r) for r in req.sampling]
        keys, sampling_warning = await asyncio.to_thread(
            apply_sampling_s3, keys, rules, ds,
            req.strict_schema, req.schema_snapshot, req.max_traces,
        )
        if sampling_warning:
            logger.warning("sampling_warning", warning=sampling_warning)

    try:
        dataset_id = await dataset_service.create_dataset(
            target, req.dataset_name, req.access, req.dataset_type,
        )
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Failed to create dataset: {e}")

    uploaded = 0
    failed: list[dict] = []
    bytes_total = 0
    labels = {"datasource": req.datasource, "target": req.target}
    t0 = time.monotonic()

    s3 = _s3_session(ds)
    async with s3.client("s3", endpoint_url=ds.endpoint, config=_s3_client_config(ds)) as client:
        for key in keys:
            try:
                obj = await client.get_object(Bucket=ds.bucket, Key=key)
                content: bytes = await obj["Body"].read()
                filename = key.rsplit("/", 1)[-1]
                await dataset_service.upload_file(target, dataset_id, filename, content)
                uploaded += 1
                bytes_total += len(content)
                IMPORT_FILES.labels(**labels, status="success").inc()
                IMPORT_BYTES.labels(**labels).inc(len(content))
                logger.info("file_exported", key=key, dataset_id=dataset_id, bytes=len(content))
            except Exception as e:
                IMPORT_FILES.labels(**labels, status="failed").inc()
                logger.warning("file_export_failed", key=key, error=str(e))
                failed.append({"key": key, "error": str(e)})

    IMPORT_SECONDS.labels(**labels).observe(time.monotonic() - t0)

    return {
        "dataset_id": dataset_id,
        "files_uploaded": uploaded,
        "files_failed": len(failed),
        "failed": failed,
        "bytes_total": bytes_total,
        "sampling_warning": sampling_warning,
    }


@router.post("/api/public/export/dataset")
async def enqueue_export(
    req: ExportRequest,
    settings: Settings = Depends(get_settings),
    auth: AuthContext = Depends(get_auth),
) -> dict:
    if not settings.targets:
        raise HTTPException(status_code=404, detail="No dataset targets configured")
    if not req.keys:
        raise HTTPException(status_code=400, detail="No keys provided")

    target = _resolve_target(req.target, settings)
    ds = _resolve_s3_datasource(req.datasource, settings)

    if is_queue_available():
        pool = await get_pool()
        job = await pool.enqueue_job(
            "import_dataset",
            target=req.target,
            datasource=req.datasource,
            keys=req.keys,
            dataset_name=req.dataset_name,
            access=req.access,
            dataset_type=req.dataset_type,
            sampling=req.sampling,
            strict_schema=req.strict_schema,
            schema_snapshot=req.schema_snapshot,
            max_traces=req.max_traces,
        )
        return {"job_id": job.job_id, "status": "queued", "warning": None}

    # no Redis — run synchronously and return a complete result immediately
    result = await _run_inline(target, ds, req)
    return {
        "job_id": None,
        "status": "complete",
        "warning": _NO_REDIS_WARNING,
        "progress": {
            "files_done": len(req.keys),
            "files_total": len(req.keys),
            "bytes_done": result["bytes_total"],
        },
        "result": result,
    }


@router.get("/api/public/export/status/{job_id}")
async def get_export_status(
    job_id: str,
    auth: AuthContext = Depends(get_auth),
) -> dict:
    pool = await get_pool()
    job = Job(job_id, pool)

    arq_status = await job.status()

    if arq_status == JobStatus.not_found:
        raise HTTPException(status_code=404, detail="Job not found")

    raw = await pool.hgetall(_PROGRESS_KEY.format(job_id=job_id))
    progress = (
        {
            "files_done": int(raw[b"files_done"]),
            "files_total": int(raw[b"files_total"]),
            "bytes_done": int(raw[b"bytes_done"]),
        }
        if raw
        else None
    )

    status = arq_status.value
    result = None
    error = None

    if arq_status == JobStatus.complete:
        try:
            result = await job.result(timeout=0)
        except asyncio.TimeoutError:
            pass
        except Exception as e:
            status = "failed"
            error = str(e)

    return {
        "job_id": job_id,
        "status": status,
        "warning": None,
        "progress": progress,
        "result": result,
        "error": error,
    }
