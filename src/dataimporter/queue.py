"""Redis/arq pool — lazy singleton, one pool per process."""

from __future__ import annotations

from arq import ArqRedis, create_pool
from arq.connections import RedisSettings

from dataimporter.config import get_settings

_pool: ArqRedis | None = None

PROGRESS_KEY = "dataimporter:progress:{job_id}"


def is_queue_available() -> bool:
    return bool(get_settings().server.redis_url)


async def get_pool() -> ArqRedis:
    global _pool
    if _pool is None:
        url = get_settings().server.redis_url
        _pool = await create_pool(RedisSettings.from_dsn(url))
    return _pool
