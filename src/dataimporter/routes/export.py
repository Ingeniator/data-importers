"""Export route — queue-based import when Redis is available, sync fallback otherwise."""

from __future__ import annotations

import asyncio

import httpx
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from dataimporter.auth import AuthContext, get_auth
from dataimporter.config import Settings, get_settings
from dataimporter.importer import run_import_dataset, run_import_dataset_events
from dataimporter.queue import PROGRESS_KEY, get_pool, is_queue_available

import structlog

logger = structlog.get_logger(__name__)

router = APIRouter()

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


class EventsExportRequest(BaseModel):
    target: str
    datasource: str
    events: list[dict]
    dataset_name: str
    access: str = "organization"
    dataset_type: str = "DATASET"
    format: str = "jsonl"  # "jsonl" | "individual" | "catalog"
    sampling: list[dict] | None = None
    strict_schema: bool = False
    schema_snapshot: dict[str, str] | None = None
    max_traces: int | None = None


def _resolve_target(name: str, settings: Settings):
    t = settings.get_target(name)
    if t is None:
        raise HTTPException(status_code=404, detail=f"Target '{name}' not found")
    return t


def _resolve_s3_datasource(name: str, settings: Settings):
    ds = settings.get_datasource(name)
    if ds is None:
        raise HTTPException(status_code=404, detail=f"Datasource '{name}' not found")
    if ds.type != "s3":
        raise HTTPException(status_code=400, detail=f"Datasource '{name}' is not an S3 datasource")
    return ds


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
    try:
        result = await run_import_dataset(
            target,
            ds,
            keys=req.keys,
            dataset_name=req.dataset_name,
            access=req.access,
            dataset_type=req.dataset_type,
            datasource=req.datasource,
            target=req.target,
            sampling=req.sampling,
            strict_schema=req.strict_schema,
            schema_snapshot=req.schema_snapshot,
            max_traces=req.max_traces,
        )
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Failed to create dataset: {e}")
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


@router.post("/api/public/export/dataset/events")
async def enqueue_events_export(
    req: EventsExportRequest,
    settings: Settings = Depends(get_settings),
    auth: AuthContext = Depends(get_auth),
) -> dict:
    if not settings.targets:
        raise HTTPException(status_code=404, detail="No dataset targets configured")
    if not req.events:
        raise HTTPException(status_code=400, detail="No events provided")

    target = _resolve_target(req.target, settings)

    if settings.get_datasource(req.datasource) is None:
        raise HTTPException(status_code=404, detail=f"Datasource '{req.datasource}' not found")

    if is_queue_available():
        pool = await get_pool()
        job = await pool.enqueue_job(
            "import_dataset_events",
            target=req.target,
            datasource=req.datasource,
            events=req.events,
            dataset_name=req.dataset_name,
            access=req.access,
            dataset_type=req.dataset_type,
            format=req.format,
            sampling=req.sampling,
            strict_schema=req.strict_schema,
            schema_snapshot=req.schema_snapshot,
            max_traces=req.max_traces,
        )
        return {"job_id": job.job_id, "status": "queued", "warning": None}

    # no Redis — run synchronously
    try:
        result = await run_import_dataset_events(
            target,
            events=req.events,
            dataset_name=req.dataset_name,
            access=req.access,
            dataset_type=req.dataset_type,
            datasource=req.datasource,
            target=req.target,
            format=req.format,
            sampling=req.sampling,
            strict_schema=req.strict_schema,
            schema_snapshot=req.schema_snapshot,
            max_traces=req.max_traces,
        )
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Failed to create dataset: {e}")
    n = result["records_uploaded"]
    b = result["bytes_total"]
    return {
        "job_id": None,
        "status": "complete",
        "warning": _NO_REDIS_WARNING,
        "progress": {"files_done": n, "files_total": n, "bytes_done": b},
        "result": result,
    }


@router.get("/api/public/export/status/{job_id}")
async def get_export_status(
    job_id: str,
    auth: AuthContext = Depends(get_auth),
) -> dict:
    from arq.jobs import Job, JobStatus
    pool = await get_pool()
    job = Job(job_id, pool)

    arq_status = await job.status()

    if arq_status == JobStatus.not_found:
        raise HTTPException(status_code=404, detail="Job not found")

    raw = await pool.hgetall(PROGRESS_KEY.format(job_id=job_id))
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
