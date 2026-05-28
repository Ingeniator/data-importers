"""arq worker — processes import jobs one at a time (max_jobs=1)."""

from __future__ import annotations

import structlog
from arq.connections import RedisSettings

from dataimporter.config import get_settings
from dataimporter.queue import PROGRESS_KEY

logger = structlog.get_logger(__name__)


async def import_dataset(
    ctx: dict,
    *,
    target: str,
    datasource: str,
    keys: list[str],
    dataset_name: str,
    access: str,
    dataset_type: str,
    sampling: list[dict] | None = None,
    strict_schema: bool = False,
    schema_snapshot: dict[str, str] | None = None,
    max_traces: int | None = None,
) -> dict:
    from dataimporter.importer import run_import_dataset

    job_id: str = ctx["job_id"]
    redis = ctx["redis"]
    settings = get_settings()

    target_cfg = settings.get_target(target)
    if not target_cfg:
        raise ValueError(f"Target '{target}' not found in config")
    ds = settings.get_datasource(datasource)
    if not ds:
        raise ValueError(f"Datasource '{datasource}' not found in config")

    async def _set_progress(done: int, total: int, bytes_done: int) -> None:
        key = PROGRESS_KEY.format(job_id=job_id)
        await redis.hset(key, mapping={"files_done": done, "files_total": total, "bytes_done": bytes_done})
        await redis.expire(key, 3600)

    return await run_import_dataset(
        target_cfg,
        ds,
        keys=keys,
        dataset_name=dataset_name,
        access=access,
        dataset_type=dataset_type,
        datasource=datasource,
        target=target,
        sampling=sampling,
        strict_schema=strict_schema,
        schema_snapshot=schema_snapshot,
        max_traces=max_traces,
        on_progress=_set_progress,
    )


async def import_dataset_events(
    ctx: dict,
    *,
    target: str,
    datasource: str,
    events: list[dict],
    dataset_name: str,
    access: str,
    dataset_type: str,
    format: str = "jsonl",
    sampling: list[dict] | None = None,
    strict_schema: bool = False,
    schema_snapshot: dict[str, str] | None = None,
    max_traces: int | None = None,
) -> dict:
    from dataimporter.importer import run_import_dataset_events

    job_id: str = ctx["job_id"]
    redis = ctx["redis"]
    settings = get_settings()

    target_cfg = settings.get_target(target)
    if not target_cfg:
        raise ValueError(f"Target '{target}' not found in config")
    if settings.get_datasource(datasource) is None:
        raise ValueError(f"Datasource '{datasource}' not found in config")

    async def _set_progress(done: int, total: int, bytes_done: int) -> None:
        key = PROGRESS_KEY.format(job_id=job_id)
        await redis.hset(key, mapping={"files_done": done, "files_total": total, "bytes_done": bytes_done})
        await redis.expire(key, 3600)

    return await run_import_dataset_events(
        target_cfg,
        events=events,
        dataset_name=dataset_name,
        access=access,
        dataset_type=dataset_type,
        datasource=datasource,
        target=target,
        format=format,
        sampling=sampling,
        strict_schema=strict_schema,
        schema_snapshot=schema_snapshot,
        max_traces=max_traces,
        on_progress=_set_progress,
    )


class WorkerSettings:
    functions = [import_dataset, import_dataset_events]
    max_jobs = 1
    job_timeout = 3600
    redis_settings = RedisSettings.from_dsn(get_settings().server.redis_url or "redis://localhost:6379/1")
