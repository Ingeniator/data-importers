"""Redis/arq pool — lazy singleton, one pool per process."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dataimporter.config import get_settings

if TYPE_CHECKING:
    from arq import ArqRedis

_pool: ArqRedis | None = None

PROGRESS_KEY = "dataimporter:progress:{job_id}"


def is_queue_available() -> bool:
    return bool(get_settings().server.redis_url)


async def get_pool() -> ArqRedis:
    global _pool
    if _pool is None:
        from arq import create_pool
        from arq.connections import RedisSettings
        url = get_settings().server.redis_url
        _pool = await create_pool(RedisSettings.from_dsn(url))
    return _pool
