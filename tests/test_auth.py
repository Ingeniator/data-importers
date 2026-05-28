"""Tests for dataimporter.auth — _sanitize_key and the get_auth dependency."""
from __future__ import annotations

import base64
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from dataimporter.auth import _sanitize_key
from dataimporter.config import Datasource, Settings, get_settings
from dataimporter.main import app

S3_DS = Datasource(
    name="test-s3",
    type="s3",
    bucket="test-bucket",
    region="us-east-1",
    access_key_id="testing",
    secret_access_key="testing",
)

AUTH_SETTINGS = Settings(datasources=(S3_DS,))


@pytest.fixture
def auth_client():
    app.dependency_overrides[get_settings] = lambda: AUTH_SETTINGS
    yield TestClient(app)
    app.dependency_overrides.clear()


def _basic(pk: str, sk: str) -> dict[str, str]:
    creds = base64.b64encode(f"{pk}:{sk}".encode()).decode()
    return {"Authorization": f"Basic {creds}"}


# ── _sanitize_key unit tests ──────────────────────────────────────────────────


def test_sanitize_key_plain():
    assert _sanitize_key("pk-test") == "pk-test"


def test_sanitize_key_allows_slash_for_nesting():
    assert _sanitize_key("org/user") == "org/user"


def test_sanitize_key_removes_path_traversal():
    result = _sanitize_key("../../etc/passwd")
    assert ".." not in result


def test_sanitize_key_path_traversal_result_is_safe():
    # ../../etc/passwd → removes .. → //etc/passwd → collapse slashes → /etc/passwd
    # → strip leading slashes → etc/passwd
    result = _sanitize_key("../../etc/passwd")
    assert result == "etc/passwd"


def test_sanitize_key_replaces_space():
    result = _sanitize_key("pk test")
    assert " " not in result


def test_sanitize_key_replaces_at_sign():
    result = _sanitize_key("user@domain")
    assert "@" not in result


def test_sanitize_key_replaces_exclamation():
    result = _sanitize_key("key!")
    assert "!" not in result


def test_sanitize_key_collapses_multiple_slashes():
    result = _sanitize_key("org//user")
    assert "//" not in result


def test_sanitize_key_strips_leading_slash():
    result = _sanitize_key("/foo/bar")
    assert not result.startswith("/")


def test_sanitize_key_strips_trailing_slash():
    result = _sanitize_key("foo/bar/")
    assert not result.endswith("/")


def test_sanitize_key_all_unsafe_chars_becomes_empty():
    # "@@@" → "___" → strip("_") → ""
    result = _sanitize_key("@@@")
    assert result == ""


def test_sanitize_key_empty_input():
    assert _sanitize_key("") == ""


# ── get_auth via HTTP — Basic auth ────────────────────────────────────────────


def test_auth_missing_header_returns_401(auth_client):
    with patch("dataimporter.routes.logs.list_batch_keys", new_callable=AsyncMock, return_value=[]):
        resp = auth_client.get("/api/public/logs/list?datasource=test-s3")
    assert resp.status_code == 401


def test_auth_non_basic_scheme_returns_401(auth_client):
    with patch("dataimporter.routes.logs.list_batch_keys", new_callable=AsyncMock, return_value=[]):
        resp = auth_client.get(
            "/api/public/logs/list?datasource=test-s3",
            headers={"Authorization": "Bearer some-token"},
        )
    assert resp.status_code == 401


def test_auth_no_colon_in_credentials_returns_401(auth_client):
    # base64("nocolon") is valid base64 but split(":", 1) won't produce 2 parts
    no_colon = base64.b64encode(b"nocolon").decode()
    with patch("dataimporter.routes.logs.list_batch_keys", new_callable=AsyncMock, return_value=[]):
        resp = auth_client.get(
            "/api/public/logs/list?datasource=test-s3",
            headers={"Authorization": f"Basic {no_colon}"},
        )
    assert resp.status_code == 401


def test_auth_valid_basic_returns_200(auth_client):
    with patch("dataimporter.routes.logs.list_batch_keys", new_callable=AsyncMock, return_value=[]):
        resp = auth_client.get(
            "/api/public/logs/list?datasource=test-s3",
            headers=_basic("pk-test", "sk-test"),
        )
    assert resp.status_code == 200


# ── get_auth via HTTP — X-Group-ID header (nginx forwarding) ─────────────────


def test_auth_x_group_id_accepted(auth_client):
    with patch("dataimporter.routes.logs.list_batch_keys", new_callable=AsyncMock, return_value=[]):
        resp = auth_client.get(
            "/api/public/logs/list?datasource=test-s3",
            headers={"X-Group-ID": "org/project"},
        )
    assert resp.status_code == 200


def test_auth_x_group_id_path_traversal_sanitized(auth_client):
    # ../../etc/passwd sanitizes to "etc/passwd" — non-empty, so auth passes
    with patch("dataimporter.routes.logs.list_batch_keys", new_callable=AsyncMock, return_value=[]):
        resp = auth_client.get(
            "/api/public/logs/list?datasource=test-s3",
            headers={"X-Group-ID": "../../etc/passwd"},
        )
    assert resp.status_code == 200  # sanitized to "etc/passwd", auth passes


def test_auth_x_group_id_all_unsafe_returns_401(auth_client):
    # "@@@" sanitizes to "" → rejected
    with patch("dataimporter.routes.logs.list_batch_keys", new_callable=AsyncMock, return_value=[]):
        resp = auth_client.get(
            "/api/public/logs/list?datasource=test-s3",
            headers={"X-Group-ID": "@@@"},
        )
    assert resp.status_code == 401


# ── X-Role: ORG_ADMIN ─────────────────────────────────────────────────────────


def test_auth_org_admin_role_succeeds(auth_client):
    with patch("dataimporter.routes.logs.list_batch_keys", new_callable=AsyncMock, return_value=[]):
        resp = auth_client.get(
            "/api/public/logs/list?datasource=test-s3",
            headers={"X-Group-ID": "myorg", "X-Role": "ORG_ADMIN"},
        )
    assert resp.status_code == 200


def test_auth_x_group_id_preferred_over_basic(auth_client):
    # Both headers present — X-Group-ID takes precedence (no Basic auth parse needed)
    with patch("dataimporter.routes.logs.list_batch_keys", new_callable=AsyncMock, return_value=[]):
        resp = auth_client.get(
            "/api/public/logs/list?datasource=test-s3",
            headers={
                "X-Group-ID": "myorg",
                "Authorization": "Basic bad-creds",  # would fail if parsed
            },
        )
    assert resp.status_code == 200
