"""Config schema endpoints — JSON Schema introspection and YAML validation."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, ValidationError

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
