"""Tests for dataset_service auth behaviour, including auth-less (mock) targets."""
from __future__ import annotations

import httpx
import pytest
import respx

from dataimporter import dataset_service
from dataimporter.config import DatasetTarget

AUTH_TARGET = DatasetTarget(
    name="auth-target",
    base_url="http://ds:9100",
    token_url="http://ds:9100/token",
    client_id="client",
    client_secret="secret",
)

NOAUTH_TARGET = DatasetTarget(
    name="noauth-target",
    base_url="http://annotator-mock:8010",
)


@pytest.fixture(autouse=True)
def _clear_token_cache():
    dataset_service._token_cache.clear()
    yield
    dataset_service._token_cache.clear()


@pytest.mark.asyncio
async def test_get_token_empty_when_no_token_url():
    """A target without token_url skips the OAuth exchange entirely."""
    assert await dataset_service.get_token(NOAUTH_TARGET) == ""


@pytest.mark.asyncio
@respx.mock
async def test_create_dataset_sends_no_auth_header_for_noauth_target():
    route = respx.post("http://annotator-mock:8010/api/v0/datasets").mock(
        return_value=httpx.Response(201, json={"id": "ds-1"})
    )
    ds_id = await dataset_service.create_dataset(
        NOAUTH_TARGET, name="n", access="organization", dataset_type="DATASET"
    )
    assert ds_id == "ds-1"
    assert "authorization" not in route.calls.last.request.headers


@pytest.mark.asyncio
@respx.mock
async def test_upload_file_sends_no_auth_header_for_noauth_target():
    route = respx.post("http://annotator-mock:8010/api/v0/datasets/ds-1/files").mock(
        return_value=httpx.Response(201, json={"id": "f-1"})
    )
    await dataset_service.upload_file(NOAUTH_TARGET, "ds-1", "a.jsonl", b"{}")
    assert "authorization" not in route.calls.last.request.headers


@pytest.mark.asyncio
@respx.mock
async def test_create_dataset_sends_bearer_for_auth_target():
    respx.post("http://ds:9100/token").mock(
        return_value=httpx.Response(200, json={"access_token": "tok", "expires_in": 300})
    )
    route = respx.post("http://ds:9100/api/v0/datasets").mock(
        return_value=httpx.Response(201, json={"id": "ds-1"})
    )
    await dataset_service.create_dataset(
        AUTH_TARGET, name="n", access="organization", dataset_type="DATASET"
    )
    assert route.calls.last.request.headers["authorization"] == "Bearer tok"
