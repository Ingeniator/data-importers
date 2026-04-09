from __future__ import annotations

import os

import structlog
from fastapi import Depends, FastAPI, Request
from prometheus_client import CollectorRegistry, generate_latest, multiprocess, CONTENT_TYPE_LATEST
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_fastapi_instrumentator.metrics import latency, request_size, requests, response_size
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse

from dataimporter.config import Settings, get_settings
from dataimporter.routes.logs import router as logs_router
from dataimporter.routes.media import router as media_router
from dataimporter.routes.search import router as search_router
from dataimporter.routes.ui import router as ui_router

settings = get_settings()

# Prometheus multiprocess setup — must happen before any metrics are created
_METRICS_DIR = os.environ.get("PROMETHEUS_MULTIPROC_DIR", "/tmp/prometheus_multiproc")
os.environ["PROMETHEUS_MULTIPROC_DIR"] = _METRICS_DIR
os.makedirs(_METRICS_DIR, exist_ok=True)

app = FastAPI(title="dataimporter", version="0.1.0", root_path=settings.server.root_path)


class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("x-request-id", "")
        structlog.contextvars.clear_contextvars()
        if request_id:
            structlog.contextvars.bind_contextvars(request_id=request_id)
        return await call_next(request)


app.add_middleware(RequestIDMiddleware)

app.include_router(logs_router)
app.include_router(media_router)
app.include_router(search_router)
app.include_router(ui_router)


Instrumentator(
    should_group_status_codes=False,
    should_group_untemplated=True,
).add(
    latency(),
).add(
    request_size(),
).add(
    response_size(),
).add(
    requests(),
).instrument(app)


@app.get("/metrics")
async def metrics():
    """Multiprocess-safe metrics endpoint."""
    if os.path.isdir(_METRICS_DIR):
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
    else:
        from prometheus_client import REGISTRY
        registry = REGISTRY
    return StarletteResponse(content=generate_latest(registry), media_type=CONTENT_TYPE_LATEST)


@app.get("/livez")
async def livez() -> dict:
    return {"status": "ok"}


@app.get("/ready")
async def ready(settings: Settings = Depends(get_settings)):
    import asyncio
    import aioboto3
    import httpx as _httpx

    for ds in settings.datasources:
        try:
            if ds.type == "s3":
                from dataimporter.s3 import _s3_client_config, _s3_session
                session = _s3_session(ds)
                async with session.client("s3", endpoint_url=ds.endpoint, config=_s3_client_config(ds)) as client:
                    await asyncio.wait_for(client.head_bucket(Bucket=ds.bucket), timeout=3)
            elif ds.type == "clickhouse" and ds.url:
                async with _httpx.AsyncClient(timeout=3) as client:
                    resp = await client.get(f"{ds.url.rstrip('/')}/ping")
                    resp.raise_for_status()
            elif ds.type == "trino" and ds.url:
                async with _httpx.AsyncClient(timeout=3) as client:
                    resp = await client.get(f"{ds.url.rstrip('/')}/v1/info")
                    resp.raise_for_status()
            elif ds.type == "langfuse" and ds.url:
                from dataimporter.langfuse import ping_langfuse
                await ping_langfuse(ds)
        except Exception:
            return StarletteResponse(status_code=503)

    return StarletteResponse(status_code=200)


@app.get("/health")
async def health(settings: Settings = Depends(get_settings)) -> dict:
    import asyncio
    import aioboto3
    import httpx as _httpx

    components: dict[str, str] = {}
    details: dict[str, str] = {}

    for ds in settings.datasources:
        try:
            if ds.type == "s3":
                from dataimporter.s3 import _s3_client_config, _s3_session
                session = _s3_session(ds)
                async with session.client("s3", endpoint_url=ds.endpoint, config=_s3_client_config(ds)) as client:
                    await asyncio.wait_for(client.head_bucket(Bucket=ds.bucket), timeout=3)
                components[ds.name] = "ok"
            elif ds.type == "clickhouse" and ds.url:
                async with _httpx.AsyncClient(timeout=3) as client:
                    resp = await client.get(f"{ds.url.rstrip('/')}/ping")
                    resp.raise_for_status()
                components[ds.name] = "ok"
            elif ds.type == "trino" and ds.url:
                async with _httpx.AsyncClient(timeout=3) as client:
                    resp = await client.get(f"{ds.url.rstrip('/')}/v1/info")
                    resp.raise_for_status()
                components[ds.name] = "ok"
            elif ds.type == "langfuse" and ds.url:
                from dataimporter.langfuse import ping_langfuse
                await ping_langfuse(ds)
                components[ds.name] = "ok"
        except Exception as exc:
            components[ds.name] = "degraded"
            details[ds.name] = str(exc)

    enabled = {k: v for k, v in components.items() if v != "disabled"}
    status = "ok" if all(v == "ok" for v in enabled.values()) else "degraded"

    result: dict = {"status": status, "components": components}
    if details:
        result["details"] = details
    return result


@app.get("/api/public/datasources")
def list_datasources(settings: Settings = Depends(get_settings)) -> dict:
    """List configured datasources (name + type only, no secrets)."""
    return {
        "datasources": [
            {"name": ds.name, "type": ds.type}
            for ds in settings.datasources
        ]
    }


@app.get("/api/public/ui-config")
def ui_config(settings: Settings = Depends(get_settings)) -> dict:
    return {
        "hide_auth_inputs": settings.server.hide_auth_inputs,
        "datasources": [
            {"name": ds.name, "type": ds.type}
            for ds in settings.datasources
        ],
    }
