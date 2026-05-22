"""
Standalone mock of the dataset service + Keycloak token endpoint.

Usage:
    uv run python tests/mock_dataset_service.py [--port 9100] [--upload-dir /tmp/ds-uploads]

Add to config.yaml:
    targets:
      - name: "Mock Dataset Service"
        base_url: "http://localhost:9100"
        token_url: "http://localhost:9100/realms/test/protocol/openid-connect/token"
        client_id: "any"
        client_secret: "any"
        default_access: "organization"
        default_dataset_type: "DATASET"

Inspect uploaded files:
    GET http://localhost:9100/_mock/datasets          — list datasets
    GET http://localhost:9100/_mock/datasets/{id}     — dataset detail + file list
    GET http://localhost:9100/_mock/files/{id}        — download uploaded file
"""

from __future__ import annotations

import argparse
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path

import uvicorn
from fastapi import FastAPI, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel

app = FastAPI(title="mock-dataset-service")

# ── In-memory store ──────────────────────────────────────────────────────────

_datasets: dict[str, dict] = {}  # id → {name, access, dataset_type, files: [...]}
_files: dict[str, dict] = {}     # file_id → {name, dataset_id, path, size}

_UPLOAD_DIR = Path(os.environ.get("MOCK_UPLOAD_DIR", "/tmp/mock-dataset-service"))


# ── Token endpoint (Keycloak-compatible) ─────────────────────────────────────

@app.post("/realms/{realm}/protocol/openid-connect/token")
async def token(realm: str):
    """Accept any client_credentials request and return a mock token."""
    return {
        "access_token": f"mock-token-{uuid.uuid4().hex[:8]}",
        "expires_in": 3600,
        "token_type": "Bearer",
        "scope": "profile email",
    }


# ── Dataset endpoints ─────────────────────────────────────────────────────────

class CreateDatasetBody(BaseModel):
    name: str
    access: str = "organization"
    dataset_type: str = "DATASET"
    data_source: str | None = None
    data_classification_level: str | None = None


def _require_bearer(authorization: str | None):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")


@app.post("/api/v0/datasets", status_code=201)
async def create_dataset(
    body: CreateDatasetBody,
    authorization: str | None = Header(default=None),
):
    _require_bearer(authorization)
    dataset_id = str(uuid.uuid4())
    _datasets[dataset_id] = {
        "id": dataset_id,
        "name": body.name,
        "access": body.access,
        "dataset_type": body.dataset_type,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "files": [],
    }
    return _datasets[dataset_id]


@app.get("/api/v0/datasets/{dataset_id}")
async def get_dataset(
    dataset_id: str,
    authorization: str | None = Header(default=None),
):
    _require_bearer(authorization)
    if dataset_id not in _datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")
    return _datasets[dataset_id]


@app.post("/api/v0/datasets/{dataset_id}/files", status_code=201)
async def upload_file(
    dataset_id: str,
    file: UploadFile = File(...),
    authorization: str | None = Header(default=None),
):
    _require_bearer(authorization)
    if dataset_id not in _datasets:
        raise HTTPException(status_code=404, detail="Dataset not found")

    _UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    file_id = str(uuid.uuid4())
    dest = _UPLOAD_DIR / file_id
    content = await file.read()
    dest.write_bytes(content)

    file_meta = {
        "id": file_id,
        "dataset_id": dataset_id,
        "name": file.filename,
        "size": len(content),
        "uploaded_at": datetime.now(timezone.utc).isoformat(),
    }
    _files[file_id] = {**file_meta, "path": str(dest)}
    _datasets[dataset_id]["files"].append(file_meta)

    print(f"  [upload] dataset={dataset_id} file={file.filename} size={len(content):,}B id={file_id}")
    return file_meta


# ── Debug / inspection endpoints ──────────────────────────────────────────────

@app.get("/_mock/datasets")
async def list_datasets():
    return {"datasets": list(_datasets.values()), "count": len(_datasets)}


@app.get("/_mock/datasets/{dataset_id}")
async def inspect_dataset(dataset_id: str):
    if dataset_id not in _datasets:
        raise HTTPException(status_code=404)
    return _datasets[dataset_id]


@app.get("/_mock/files/{file_id}")
async def download_file(file_id: str):
    if file_id not in _files:
        raise HTTPException(status_code=404)
    meta = _files[file_id]
    return FileResponse(meta["path"], filename=meta["name"])


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=9100)
    parser.add_argument("--upload-dir", default=str(_UPLOAD_DIR))
    args = parser.parse_args()

    _UPLOAD_DIR = Path(args.upload_dir)

    print(f"\nMock dataset service running on http://localhost:{args.port}")
    print(f"Uploads stored in: {_UPLOAD_DIR}")
    print("\nAdd to dataimporter config.yaml:")
    print("  targets:")
    print("    - name: \"Mock Dataset Service\"")
    print(f"      base_url: \"http://localhost:{args.port}\"")
    print(f"      token_url: \"http://localhost:{args.port}/realms/test/protocol/openid-connect/token\"")
    print("      client_id: \"any\"")
    print("      client_secret: \"any\"")
    print()

    uvicorn.run(app, host="0.0.0.0", port=args.port, log_level="warning")
