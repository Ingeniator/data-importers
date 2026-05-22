"""arq worker — processes import jobs one at a time (max_jobs=1)."""

from __future__ import annotations

import time

import structlog
from arq.connections import RedisSettings

from dataimporter.config import get_settings

logger = structlog.get_logger(__name__)

_PROGRESS_KEY = "dataimporter:progress:{job_id}"


async def import_dataset(
    ctx: dict,
    *,
    target: str,
    datasource: str,
    keys: list[str],
    dataset_name: str,
    access: str,
    dataset_type: str,
) -> dict:
    from dataimporter import dataset_service
    from dataimporter.metrics import IMPORT_BYTES, IMPORT_FILES, IMPORT_SECONDS
    from dataimporter.s3 import _s3_client_config, _s3_session

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
        await redis.hset(
            _PROGRESS_KEY.format(job_id=job_id),
            mapping={"files_done": done, "files_total": total, "bytes_done": bytes_done},
        )
        await redis.expire(_PROGRESS_KEY.format(job_id=job_id), 3600)

    dataset_id = await dataset_service.create_dataset(
        target_cfg, dataset_name, access, dataset_type,
    )
    await _set_progress(0, len(keys), 0)

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
                await _set_progress(i + 1, len(keys), bytes_done)

    IMPORT_SECONDS.labels(**labels).observe(time.monotonic() - t0)

    return {
        "dataset_id": dataset_id,
        "files_uploaded": uploaded,
        "files_failed": len(failed),
        "failed": failed,
    }


class WorkerSettings:
    functions = [import_dataset]
    max_jobs = 1
    job_timeout = 3600
    redis_settings = RedisSettings.from_dsn(get_settings().server.redis_url or "redis://localhost:6379/1")
