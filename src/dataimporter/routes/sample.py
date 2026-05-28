"""Schema discovery endpoint — returns field types from a sample of datasource records."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query

from dataimporter.adapters import get_adapter
from dataimporter.auth import AuthContext, get_auth
from dataimporter.config import Datasource, Settings, get_settings

logger = structlog.get_logger(__name__)
router = APIRouter()


def _infer_type(value: Any) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, list):
        return "list"
    if isinstance(value, dict):
        return "object"
    return "string"


def _collect_fields(
    obj: dict,
    fields: dict[str, dict],
    prefix: str = "",
    depth: int = 0,
    max_depth: int = 3,
) -> None:
    """Recursively collect field names, types, and examples into *fields* (mutates it).

    Nested object keys are joined with a dot: ``parent.child.leaf``.
    """
    for k, v in obj.items():
        if k.startswith("_"):
            continue
        full_key = f"{prefix}.{k}" if prefix else k
        inferred = _infer_type(v)

        if full_key not in fields:
            fields[full_key] = {"type": inferred, "example": None}

        # Record first non-None, non-object example value
        if fields[full_key]["example"] is None and v is not None:
            if isinstance(v, dict):
                pass  # example stays None for objects; sub-fields carry the values
            elif isinstance(v, list):
                fields[full_key]["example"] = v[:2] if v else None
            else:
                fields[full_key]["example"] = v

        # Recurse into nested dicts up to max_depth
        if isinstance(v, dict) and depth < max_depth:
            _collect_fields(v, fields, prefix=full_key, depth=depth + 1, max_depth=max_depth)


def _flatten_fields(records: list[dict]) -> dict[str, dict]:
    """Collect field names, inferred types, and first non-None example from records.

    Object (dict) fields are expanded recursively using dot-notation keys so that
    nested structure is visible to callers (e.g. ``"metadata.user_id"``).
    """
    fields: dict[str, dict] = {}
    for rec in records:
        _collect_fields(rec, fields)
    return fields


def _resolve_datasource(
    datasource: str = Query(),
    settings: Settings = Depends(get_settings),
) -> Datasource:
    ds = settings.get_datasource(datasource)
    if ds is None:
        raise HTTPException(status_code=404, detail=f"Datasource '{datasource}' not found")
    return ds


@router.get("/api/public/datasource/sample")
async def datasource_sample(
    start: datetime | None = Query(default=None),
    end: datetime | None = Query(default=None),
    session_id: str | None = Query(default=None),
    trace_id: str | None = Query(default=None),
    trace_type: str | None = Query(default=None),
    input_hash: str | None = Query(default=None),
    keys: list[str] = Query(default=[]),
    ds: Datasource = Depends(_resolve_datasource),
    auth: AuthContext = Depends(get_auth),
) -> dict:
    if start and start.tzinfo is None:
        start = start.replace(tzinfo=timezone.utc)
    if end and end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)

    records: list[dict]

    if ds.type == "s3":
        # S3 uses DuckDB to read the first row per file for schema inference —
        # a different code path from content search.
        if not keys:
            raise HTTPException(status_code=400, detail="keys[] required for S3 datasource")
        import asyncio
        from dataimporter.sampling import read_s3_traces_for_sampling
        records = await asyncio.to_thread(read_s3_traces_for_sampling, keys[:5], ds)
    else:
        try:
            adapter = get_adapter(ds)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Schema discovery not supported for '{ds.type}'")
        records = await adapter.search(
            "*", auth=auth,
            start=start, end=end,
            session_id=session_id, trace_id=trace_id,
            trace_type=trace_type, input_hash=input_hash,
            limit=5,
        )

    return {"fields": _flatten_fields(records), "sample_count": len(records)}
