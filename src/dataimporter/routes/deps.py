"""Shared FastAPI dependencies for datasource and target resolution."""
from __future__ import annotations

from fastapi import Depends, HTTPException, Query

from dataimporter.config import Datasource, Settings, get_settings


def resolve_datasource(name: str, settings: Settings) -> Datasource:
    ds = settings.get_datasource(name)
    if ds is None:
        raise HTTPException(status_code=404, detail=f"Datasource '{name}' not found")
    return ds


def resolve_s3_datasource(name: str, settings: Settings) -> Datasource:
    ds = resolve_datasource(name, settings)
    if ds.type != "s3":
        raise HTTPException(status_code=400, detail=f"Datasource '{name}' is not an S3 datasource")
    return ds


def get_datasource(
    datasource: str = Query(),
    settings: Settings = Depends(get_settings),
) -> Datasource:
    return resolve_datasource(datasource, settings)


def get_s3_datasource(
    datasource: str = Query(),
    settings: Settings = Depends(get_settings),
) -> Datasource:
    return resolve_s3_datasource(datasource, settings)
