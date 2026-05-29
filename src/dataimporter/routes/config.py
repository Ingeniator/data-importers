"""Config schema endpoints — JSON Schema introspection, YAML validation, and datasource/UI config."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ValidationError

from dataimporter.config import Settings, get_settings
from dataimporter.job_config import JobConfig, parse_job_config

router = APIRouter()


class ValidateRequest(BaseModel):
    yaml_text: str


class ValidateResponse(BaseModel):
    valid: bool
    errors: list[str] = []
    parsed: dict | None = None


@router.get("/api/public/config/schema")
def config_schema() -> dict:
    """Return the JSON Schema for a job configuration YAML.

    Consumers (editors, CI validators) can use this to provide autocompletion
    and inline error highlighting.
    """
    return JobConfig.model_json_schema()


@router.post("/api/public/config/validate", response_model=ValidateResponse)
def validate_config(req: ValidateRequest) -> ValidateResponse:
    """Validate a YAML config string against the ``JobConfig`` schema.

    Returns ``valid: true`` and the parsed object on success, or a list of
    human-readable error messages on failure.
    """
    try:
        cfg = parse_job_config(req.yaml_text)
        return ValidateResponse(
            valid=True,
            parsed=cfg.model_dump(exclude_none=True, by_alias=True),
        )
    except ValueError as exc:
        return ValidateResponse(valid=False, errors=[str(exc)])
    except ValidationError as exc:
        errors = [
            ".".join(str(loc) for loc in e["loc"]) + ": " + e["msg"]
            for e in exc.errors()
        ]
        return ValidateResponse(valid=False, errors=errors)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.get("/api/public/datasources")
def list_datasources(settings: Settings = Depends(get_settings)) -> dict:
    """List configured datasources (name + type only, no secrets)."""
    return {
        "datasources": [
            {"name": ds.name, "type": ds.type}
            for ds in settings.datasources
        ]
    }


@router.get("/api/public/ui-config")
def ui_config(settings: Settings = Depends(get_settings)) -> dict:
    return {
        "hide_auth_inputs": settings.server.hide_auth_inputs,
        "datasources": [
            {"name": ds.name, "type": ds.type}
            for ds in settings.datasources
        ],
        "connections": [
            {
                "type": c.type, "url": c.url,
                "label": c.label or f"{c.type} ({c.url})",
                "has_credentials": bool(c.public_key and c.secret_key),
            }
            for c in settings.connections
        ],
        "targets": [
            {
                "name": t.name,
                "default_access": t.default_access,
                "default_dataset_type": t.default_dataset_type,
            }
            for t in settings.targets
        ],
    }
