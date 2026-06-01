"""Dataset service integration — OAuth2 token exchange and file upload."""

from __future__ import annotations

import asyncio
import time

import httpx
import structlog

from dataimporter.config import DatasetTarget

logger = structlog.get_logger(__name__)

# target name → (token, expires_at_monotonic)
_token_cache: dict[str, tuple[str, float]] = {}
# one lock per target name prevents concurrent refresh storms
_token_locks: dict[str, asyncio.Lock] = {}


def _auth_headers(token: str) -> dict[str, str]:
    """Authorization header for a token, or no header for auth-less targets."""
    return {"Authorization": f"Bearer {token}"} if token else {}


async def get_token(target: DatasetTarget) -> str:
    """Return a valid Bearer token, refreshing via client_credentials if needed.

    Returns "" when the target has no token_url configured, in which case
    requests are sent without an Authorization header (auth-less mock targets).
    """
    if not target.token_url:
        return ""

    lock = _token_locks.setdefault(target.name, asyncio.Lock())
    async with lock:
        now = time.monotonic()
        cached = _token_cache.get(target.name)
        if cached and cached[1] > now + 30:
            return cached[0]

        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                target.token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": target.client_id,
                    "client_secret": target.client_secret,
                },
            )
            resp.raise_for_status()
            body = resp.json()

        token: str = body["access_token"]
        expires_in: int = body.get("expires_in", 300)
        _token_cache[target.name] = (token, now + expires_in)
        logger.debug("token_refreshed", target=target.name, expires_in=expires_in)
        return token


async def create_dataset(
    target: DatasetTarget,
    name: str,
    access: str,
    dataset_type: str,
) -> str:
    """Create a dataset and return its ID."""
    token = await get_token(target)
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{target.base_url.rstrip('/')}/api/v0/datasets",
            json={"name": name, "access": access, "dataset_type": dataset_type},
            headers=_auth_headers(token),
        )
        resp.raise_for_status()
        return resp.json()["id"]


async def upload_file(
    target: DatasetTarget,
    dataset_id: str,
    filename: str,
    content: bytes,
) -> dict:
    """Upload a single file to the dataset. Returns the service response."""
    token = await get_token(target)
    timeout = httpx.Timeout(connect=10, read=target.upload_timeout, write=target.upload_timeout, pool=5)
    async with httpx.AsyncClient(timeout=timeout) as client:
        resp = await client.post(
            f"{target.base_url.rstrip('/')}/api/v0/datasets/{dataset_id}/files",
            files={"file": (filename, content, "application/octet-stream")},
            headers=_auth_headers(token),
        )
        resp.raise_for_status()
        return resp.json()
