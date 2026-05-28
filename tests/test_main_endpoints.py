"""Tests for main.py endpoints not covered elsewhere: /ready and /api/public/ui-config."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from dataimporter.config import (
    Connection,
    DatasetTarget,
    Datasource,
    ServerConfig,
    Settings,
    get_settings,
)
from dataimporter.main import app

S3_DS = Datasource(
    name="my-s3",
    type="s3",
    bucket="b",
    access_key_id="k",
    secret_access_key="s",
)

LANGFUSE_CONN = Connection(
    type="langfuse",
    url="http://lf.example.com",
    label="My Langfuse",
    public_key="pk-conn",
    secret_key="sk-conn",
)

CONN_NO_CREDS = Connection(
    type="langfuse",
    url="http://other.example.com",
)

TARGET = DatasetTarget(
    name="my-target",
    base_url="http://ds",
    token_url="http://ds/token",
    client_id="c",
    client_secret="s",
    default_access="organization",
    default_dataset_type="DATASET",
)

FULL_SETTINGS = Settings(
    datasources=(S3_DS,),
    connections=(LANGFUSE_CONN, CONN_NO_CREDS),
    targets=(TARGET,),
    server=ServerConfig(hide_auth_inputs=True),
)


@pytest.fixture
def full_client():
    app.dependency_overrides[get_settings] = lambda: FULL_SETTINGS
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def empty_client():
    app.dependency_overrides[get_settings] = lambda: Settings()
    yield TestClient(app)
    app.dependency_overrides.clear()


# ── /ready ─────────────────────────────────────────────────────────────────────


def test_ready_no_datasources_returns_200(empty_client):
    resp = empty_client.get("/ready")
    assert resp.status_code == 200


def test_ready_skips_unavailable_datasource_and_returns_503():
    """A datasource that can't be reached should make /ready return 503."""
    import asyncio
    from unittest.mock import patch, AsyncMock

    ds = Datasource(
        name="broken-s3",
        type="s3",
        bucket="x",
        access_key_id="k",
        secret_access_key="s",
    )
    settings = Settings(datasources=(ds,))
    app.dependency_overrides[get_settings] = lambda: settings
    client = TestClient(app)

    try:
        # Make the S3 head_bucket call raise so the datasource is unreachable
        async def raise_error(*args, **kwargs):
            raise ConnectionError("no route to host")

        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.head_bucket = raise_error

        mock_session = AsyncMock()
        mock_session.client.return_value = mock_client

        with patch("dataimporter.s3._s3_session", return_value=mock_session):
            resp = client.get("/ready")

        assert resp.status_code == 503
    finally:
        app.dependency_overrides.clear()


# ── /api/public/ui-config ──────────────────────────────────────────────────────


def test_ui_config_empty_settings(empty_client):
    resp = empty_client.get("/api/public/ui-config")
    assert resp.status_code == 200
    body = resp.json()
    assert body["datasources"] == []
    assert body["connections"] == []
    assert body["targets"] == []
    assert body["hide_auth_inputs"] is False


def test_ui_config_datasources_shape(full_client):
    resp = full_client.get("/api/public/ui-config")
    assert resp.status_code == 200
    body = resp.json()
    datasources = body["datasources"]
    assert len(datasources) == 1
    assert datasources[0]["name"] == "my-s3"
    assert datasources[0]["type"] == "s3"


def test_ui_config_hide_auth_inputs(full_client):
    resp = full_client.get("/api/public/ui-config")
    assert resp.json()["hide_auth_inputs"] is True


def test_ui_config_connections_shape(full_client):
    resp = full_client.get("/api/public/ui-config")
    body = resp.json()
    connections = body["connections"]
    assert len(connections) == 2

    # First connection has credentials
    first = next(c for c in connections if c["url"] == "http://lf.example.com")
    assert first["type"] == "langfuse"
    assert first["label"] == "My Langfuse"
    assert first["has_credentials"] is True

    # Second connection has no credentials
    second = next(c for c in connections if c["url"] == "http://other.example.com")
    assert second["has_credentials"] is False


def test_ui_config_connections_no_secrets_exposed(full_client):
    resp = full_client.get("/api/public/ui-config")
    body = resp.json()
    for conn in body["connections"]:
        assert "public_key" not in conn
        assert "secret_key" not in conn


def test_ui_config_connections_auto_label(full_client):
    resp = full_client.get("/api/public/ui-config")
    body = resp.json()
    # Connection with no label gets a generated one
    second = next(c for c in body["connections"] if c["url"] == "http://other.example.com")
    assert second["label"]  # non-empty auto-generated label


def test_ui_config_targets_shape(full_client):
    resp = full_client.get("/api/public/ui-config")
    body = resp.json()
    targets = body["targets"]
    assert len(targets) == 1
    assert targets[0]["name"] == "my-target"
    assert targets[0]["default_access"] == "organization"
    assert targets[0]["default_dataset_type"] == "DATASET"
