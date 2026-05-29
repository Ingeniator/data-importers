from __future__ import annotations

import os
from pathlib import Path

import structlog
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from prometheus_client import CollectorRegistry, generate_latest, multiprocess, CONTENT_TYPE_LATEST
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_fastapi_instrumentator.metrics import latency, request_size, requests, response_size
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse

from dataimporter.config import get_settings
from dataimporter.routes.config import router as config_router
from dataimporter.routes.export import router as export_router
from dataimporter.routes.health import router as health_router
from dataimporter.routes.logs import router as logs_router
from dataimporter.routes.media import router as media_router
from dataimporter.routes.proxy import router as proxy_router
from dataimporter.routes.sample import router as sample_router
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

app.include_router(config_router)
app.include_router(export_router)
app.include_router(health_router)
app.include_router(logs_router)
app.include_router(media_router)
app.include_router(proxy_router)
app.include_router(sample_router)
app.include_router(search_router)
app.include_router(ui_router)

_STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


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
