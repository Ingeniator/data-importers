"""Health, liveness, and readiness endpoints."""
from __future__ import annotations

from fastapi import APIRouter, Depends
from starlette.responses import Response as StarletteResponse

from dataimporter.config import Settings, get_settings

router = APIRouter()


async def _check_all_datasources(
    settings: Settings,
) -> tuple[dict[str, str], dict[str, str]]:
    from dataimporter.adapters import get_adapter

    components: dict[str, str] = {}
    details: dict[str, str] = {}
    for ds in settings.datasources:
        try:
            await get_adapter(ds).ping()
            components[ds.name] = "ok"
        except Exception as exc:
            components[ds.name] = "degraded"
            details[ds.name] = str(exc)
    return components, details


@router.get("/livez")
async def livez() -> dict:
    return {"status": "ok"}


@router.get("/ready")
async def ready(settings: Settings = Depends(get_settings)):
    components, _ = await _check_all_datasources(settings)
    if any(v != "ok" for v in components.values()):
        return StarletteResponse(status_code=503)
    return StarletteResponse(status_code=200)


@router.get("/health")
async def health(settings: Settings = Depends(get_settings)) -> dict:
    components, details = await _check_all_datasources(settings)
    enabled = {k: v for k, v in components.items() if v != "disabled"}
    status = "ok" if all(v == "ok" for v in enabled.values()) else "degraded"
    result: dict = {"status": status, "components": components}
    if details:
        result["details"] = details
    return result
