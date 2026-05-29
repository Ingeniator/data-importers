import json
import os

import pytest
from playwright.sync_api import Page, Route

# Override with DATAIMPORTER_URL env var when running against a non-default server.
# The Makefile `make dev` starts on port 5001; production proxy is at :8888/dataimporter.
BASE_URL = os.environ.get("DATAIMPORTER_URL", "http://127.0.0.1:5001")

MOCK_UI_CONFIG = {
    "hide_auth_inputs": False,
    "datasources": [
        {"name": "s3-prod", "type": "s3"},
        {"name": "clickhouse-prod", "type": "clickhouse"},
    ],
    "connections": [
        {
            "type": "clickhouse",
            "url": "http://ch.example.com:8123",
            "label": "ClickHouse Staging",
            "has_credentials": True,
        }
    ],
    "targets": [
        {
            "name": "langfuse-prod",
            "default_access": "organization",
            "default_dataset_type": "DATASET",
        }
    ],
}

MOCK_SEARCH_RESPONSE = {
    "results": [
        {"id": "trace-001", "timestamp": "2024-11-20T09:42:11Z", "input": "hello", "output": "world"},
        {"id": "trace-002", "timestamp": "2024-11-20T09:43:00Z", "input": "foo", "output": "bar"},
    ],
    "total": 2,
    "truncated": False,
}


@pytest.fixture
def base_url() -> str:
    return BASE_URL


@pytest.fixture
def ui_page(page: Page, base_url: str) -> Page:
    """Navigate to the UI root and return the page."""
    page.goto(base_url)
    return page


@pytest.fixture
def ui_page_with_config(page: Page, base_url: str) -> Page:
    """Navigate to the UI with a mocked ui-config response."""

    def _handle_ui_config(route: Route) -> None:
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(MOCK_UI_CONFIG),
        )

    page.route("**/api/public/ui-config", _handle_ui_config)
    page.goto(base_url)
    # Wait for JS boot to fire the ui-config fetch and render tabs
    page.wait_for_timeout(300)
    return page


@pytest.fixture
def ui_page_with_search(page: Page, base_url: str) -> Page:
    """Navigate to the UI with ui-config and a mocked search response, then select the first datasource."""

    def _handle_ui_config(route: Route) -> None:
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(MOCK_UI_CONFIG),
        )

    def _handle_search(route: Route) -> None:
        route.fulfill(
            status=200,
            content_type="application/json",
            body=json.dumps(MOCK_SEARCH_RESPONSE),
        )

    page.route("**/api/public/ui-config", _handle_ui_config)
    page.route("**/logs/search**", _handle_search)
    page.route("**/proxy/search**", _handle_search)
    page.goto(base_url)
    page.wait_for_timeout(300)
    return page
