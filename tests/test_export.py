"""Tests for the export routes and run_import_dataset_events format variants."""
from __future__ import annotations

import base64
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from dataimporter.config import DatasetTarget, Datasource, Settings, get_settings
from dataimporter.importer import run_import_dataset_events
from dataimporter.main import app
from dataimporter.routes.export import EventsExportRequest

# ── Fixtures / shared objects ─────────────────────────────────────────────────

S3_DS = Datasource(
    name="test-s3",
    type="s3",
    bucket="test-bucket",
    region="us-east-1",
    access_key_id="testing",
    secret_access_key="testing",
)

CH_DS = Datasource(
    name="ch-ds",
    type="clickhouse",
    url="http://localhost:8123",
)

TARGET = DatasetTarget(
    name="test-target",
    base_url="http://localhost:9100",
    token_url="http://localhost:9100/token",
    client_id="client",
    client_secret="secret",
)

EXPORT_SETTINGS = Settings(datasources=(S3_DS, CH_DS), targets=(TARGET,))
NO_TARGET_SETTINGS = Settings(datasources=(S3_DS,))
NO_DS_SETTINGS = Settings(targets=(TARGET,))

EVENTS = [
    {"input": "hello", "output": "world"},
    {"input": "ping", "output": "pong"},
]


def _auth() -> dict[str, str]:
    return {"Authorization": "Basic " + base64.b64encode(b"pk:sk").decode()}


@pytest.fixture
def export_client():
    app.dependency_overrides[get_settings] = lambda: EXPORT_SETTINGS
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def no_target_client():
    app.dependency_overrides[get_settings] = lambda: NO_TARGET_SETTINGS
    yield TestClient(app)
    app.dependency_overrides.clear()


# ── /api/public/export/dataset — validation errors ────────────────────────────


def test_export_no_targets_returns_404(no_target_client):
    resp = no_target_client.post(
        "/api/public/export/dataset",
        json={"target": "t", "datasource": "test-s3", "keys": ["a.jsonl"], "dataset_name": "ds"},
        headers=_auth(),
    )
    assert resp.status_code == 404


def test_export_no_keys_returns_400(export_client):
    resp = export_client.post(
        "/api/public/export/dataset",
        json={"target": "test-target", "datasource": "test-s3", "keys": [], "dataset_name": "ds"},
        headers=_auth(),
    )
    assert resp.status_code == 400


def test_export_unknown_target_returns_404(export_client):
    resp = export_client.post(
        "/api/public/export/dataset",
        json={"target": "nonexistent", "datasource": "test-s3", "keys": ["a.jsonl"], "dataset_name": "ds"},
        headers=_auth(),
    )
    assert resp.status_code == 404


def test_export_unknown_datasource_returns_404(export_client):
    resp = export_client.post(
        "/api/public/export/dataset",
        json={"target": "test-target", "datasource": "nope", "keys": ["a.jsonl"], "dataset_name": "ds"},
        headers=_auth(),
    )
    assert resp.status_code == 404


def test_export_non_s3_datasource_returns_400(export_client):
    resp = export_client.post(
        "/api/public/export/dataset",
        json={"target": "test-target", "datasource": "ch-ds", "keys": ["a.jsonl"], "dataset_name": "ds"},
        headers=_auth(),
    )
    assert resp.status_code == 400


# ── /api/public/export/dataset — sync (no-Redis) path ────────────────────────


def test_export_sync_path_returns_complete_status(export_client):
    fake_result = {
        "dataset_id": "ds-123",
        "files_uploaded": 2,
        "files_failed": 0,
        "failed": [],
        "bytes_total": 256,
        "sampling_warning": None,
    }

    with patch("dataimporter.routes.export.is_queue_available", return_value=False):
        with patch("dataimporter.routes.export.run_import_dataset", new_callable=AsyncMock, return_value=fake_result):
            resp = export_client.post(
                "/api/public/export/dataset",
                json={
                    "target": "test-target",
                    "datasource": "test-s3",
                    "keys": ["k1.jsonl", "k2.jsonl"],
                    "dataset_name": "my-dataset",
                },
                headers=_auth(),
            )

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "complete"
    assert body["job_id"] is None
    assert "warning" in body
    assert body["result"]["dataset_id"] == "ds-123"
    assert body["result"]["files_uploaded"] == 2
    assert body["progress"]["files_done"] == 2


# ── /api/public/export/dataset/events — validation errors ────────────────────


def test_events_export_no_targets_returns_404(no_target_client):
    resp = no_target_client.post(
        "/api/public/export/dataset/events",
        json={"target": "t", "datasource": "test-s3", "events": EVENTS, "dataset_name": "ds"},
        headers=_auth(),
    )
    assert resp.status_code == 404


def test_events_export_no_events_returns_400(export_client):
    resp = export_client.post(
        "/api/public/export/dataset/events",
        json={"target": "test-target", "datasource": "test-s3", "events": [], "dataset_name": "ds"},
        headers=_auth(),
    )
    assert resp.status_code == 400


def test_events_export_unknown_datasource_returns_404(export_client):
    resp = export_client.post(
        "/api/public/export/dataset/events",
        json={"target": "test-target", "datasource": "nope", "events": EVENTS, "dataset_name": "ds"},
        headers=_auth(),
    )
    assert resp.status_code == 404


def test_events_export_unknown_target_returns_404(export_client):
    resp = export_client.post(
        "/api/public/export/dataset/events",
        json={"target": "nope", "datasource": "test-s3", "events": EVENTS, "dataset_name": "ds"},
        headers=_auth(),
    )
    assert resp.status_code == 404


# ── /api/public/export/dataset/events — sync path ────────────────────────────


def test_events_export_sync_path_returns_complete_status(export_client):
    fake_result = {
        "dataset_id": "ds-ev-1",
        "files_uploaded": 1,
        "files_failed": 0,
        "records_uploaded": 2,
        "bytes_total": 42,
        "unit": "record",
        "sampling_warning": None,
    }

    with patch("dataimporter.routes.export.is_queue_available", return_value=False):
        with patch("dataimporter.routes.export.run_import_dataset_events", new_callable=AsyncMock, return_value=fake_result):
            resp = export_client.post(
                "/api/public/export/dataset/events",
                json={
                    "target": "test-target",
                    "datasource": "test-s3",
                    "events": EVENTS,
                    "dataset_name": "ev-dataset",
                    "format": "jsonl",
                },
                headers=_auth(),
            )

    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "complete"
    assert body["job_id"] is None
    assert body["result"]["records_uploaded"] == 2
    assert body["progress"]["files_done"] == 2


# ── run_import_dataset_events — format variants (unit tests) ─────────────────


@pytest.mark.asyncio
async def test_run_inline_events_jsonl_single_upload():
    with (
        patch("dataimporter.dataset_service.create_dataset", new_callable=AsyncMock, return_value="ds-1"),
        patch("dataimporter.dataset_service.upload_file", new_callable=AsyncMock, return_value={}) as mock_upload,
    ):
        result = await run_import_dataset_events(
            TARGET,
            events=EVENTS,
            dataset_name="my-ds",
            access="organization",
            dataset_type="DATASET",
            datasource="test-s3",
            target="test-target",
            format="jsonl",
        )

    assert result["records_uploaded"] == 2
    assert result["files_uploaded"] == 2
    assert result["files_failed"] == 0
    # JSONL: one upload call for all events in a single file
    assert mock_upload.call_count == 1
    filename = mock_upload.call_args.args[2]
    assert filename.endswith(".jsonl")
    content: bytes = mock_upload.call_args.args[3]
    lines = content.decode().strip().split("\n")
    assert len(lines) == 2


@pytest.mark.asyncio
async def test_run_inline_events_individual_one_file_per_event():
    with (
        patch("dataimporter.dataset_service.create_dataset", new_callable=AsyncMock, return_value="ds-1"),
        patch("dataimporter.dataset_service.upload_file", new_callable=AsyncMock, return_value={}) as mock_upload,
    ):
        result = await run_import_dataset_events(
            TARGET,
            events=EVENTS,
            dataset_name="my-ds",
            access="organization",
            dataset_type="DATASET",
            datasource="test-s3",
            target="test-target",
            format="individual",
        )

    assert result["files_uploaded"] == len(EVENTS)
    assert mock_upload.call_count == len(EVENTS)
    # Each file is named event-NNNN.json
    filenames = [call.args[2] for call in mock_upload.call_args_list]
    assert filenames[0] == "event-0000.json"
    assert filenames[1] == "event-0001.json"


@pytest.mark.asyncio
async def test_run_inline_events_catalog_single_json_array():
    with (
        patch("dataimporter.dataset_service.create_dataset", new_callable=AsyncMock, return_value="ds-1"),
        patch("dataimporter.dataset_service.upload_file", new_callable=AsyncMock, return_value={}) as mock_upload,
    ):
        result = await run_import_dataset_events(
            TARGET,
            events=EVENTS,
            dataset_name="my-ds",
            access="organization",
            dataset_type="DATASET",
            datasource="test-s3",
            target="test-target",
            format="catalog",
        )

    assert result["files_uploaded"] == 1
    assert mock_upload.call_count == 1
    filename = mock_upload.call_args.args[2]
    assert filename == "my-ds-catalog.json"
    import json
    content = json.loads(mock_upload.call_args.args[3])
    assert isinstance(content, list)
    assert len(content) == 2


@pytest.mark.asyncio
async def test_run_inline_events_dataset_service_error_propagates():
    import httpx

    with patch(
        "dataimporter.dataset_service.create_dataset",
        new_callable=AsyncMock,
        side_effect=httpx.HTTPError("service unavailable"),
    ):
        with pytest.raises(httpx.HTTPError):
            await run_import_dataset_events(
                TARGET,
                events=EVENTS,
                dataset_name="my-ds",
                access="organization",
                dataset_type="DATASET",
                datasource="test-s3",
                target="test-target",
                format="jsonl",
            )


@pytest.mark.asyncio
async def test_enqueue_events_export_returns_502_on_dataset_service_error(export_client):
    """Route handler converts httpx.HTTPError to 502 for the inline path."""
    import httpx

    with patch("dataimporter.routes.export.is_queue_available", return_value=False):
        with patch(
            "dataimporter.routes.export.run_import_dataset_events",
            new_callable=AsyncMock,
            side_effect=httpx.HTTPError("service unavailable"),
        ):
            resp = export_client.post(
                "/api/public/export/dataset/events",
                json={
                    "target": "test-target",
                    "datasource": "test-s3",
                    "events": EVENTS,
                    "dataset_name": "my-ds",
                },
                headers=_auth(),
            )

    assert resp.status_code == 502
