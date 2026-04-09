"""Performance tests for dataimporter.

Measures application-layer overhead: key parsing, filtering, serialization,
route handling, and search adapter response assembly. Backends are mocked
so these tests run without external services.
"""

from __future__ import annotations

import base64
import json
import time
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from dataimporter.auth import AuthContext
from dataimporter.config import Datasource, Settings, get_settings
from dataimporter.main import app
from dataimporter.s3 import list_batch_keys, list_batch_urls, parse_key_meta

# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

S3_DS = Datasource(
    name="perf-s3",
    type="s3",
    bucket="perf-bucket",
    region="us-east-1",
    endpoint="http://localhost:9000",
    access_key_id="testing",
    secret_access_key="testing",
    addressing_style="path",
)

CH_DS = Datasource(
    name="perf-ch",
    type="clickhouse",
    url="http://localhost:8123",
    database="default",
    table="llogr_events",
    user="default",
    password="test",
)

TRINO_DS = Datasource(
    name="perf-trino",
    type="trino",
    url="http://localhost:8080",
    catalog="hive",
    schema_name="llm_logs",
    table="llogr_events",
    user="dataimporter",
)

PERF_SETTINGS = Settings(datasources=(S3_DS, CH_DS, TRINO_DS))

TRACE_TYPES = ["default", "chatbot", "qa-bot", "summarizer", "embedder"]


def _generate_s3_keys(n: int, prefix: str = "pk-test") -> list[dict]:
    """Generate n realistic S3 object keys."""
    keys = []
    base_ts = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    for i in range(n):
        ts = base_ts.replace(hour=i % 24, minute=i % 60, second=i % 60)
        ts_str = ts.strftime("%Y%m%dT%H%M%SZ")
        sid = f"sess{i % 50:04d}"
        tid = f"trace{i % 200:04d}"
        tt = TRACE_TYPES[i % len(TRACE_TYPES)]
        ih = f"{i:08x}"
        uid = uuid.uuid4().hex[:8]
        keys.append({"Key": f"{prefix}/{sid}_{tid}_{tt}_{ih}_{ts_str}_{uid}.jsonl"})
    return keys


def _make_mock_session(keys: list[dict]):
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)
    mock_client.generate_presigned_url = AsyncMock(return_value="https://presigned-url")

    mock_paginator = MagicMock()

    async def async_pages(**kwargs):
        yield {"Contents": keys}

    mock_paginator.paginate = async_pages
    mock_client.get_paginator = MagicMock(return_value=mock_paginator)

    mock_session = MagicMock()
    mock_session.client.return_value = mock_client
    return mock_session


def _generate_ch_rows(n: int) -> list[dict]:
    """Generate n ClickHouse-style result rows."""
    rows = []
    base_ts = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
    for i in range(n):
        ts = base_ts.replace(hour=i % 24, minute=i % 60, second=i % 60)
        body = json.dumps({"messages": [{"role": "user", "content": f"message {i}"}]})
        rows.append({
            "event_id": f"evt-{i:06d}",
            "event_type": "llm.completion",
            "timestamp": ts.isoformat(),
            "project_id": "pk-test",
            "model": "gpt-4",
            "name": TRACE_TYPES[i % len(TRACE_TYPES)],
            "trace_id": f"trace{i % 200:04d}",
            "session_id": f"sess{i % 50:04d}",
            "body": body,
        })
    return rows


@pytest.fixture
def auth() -> AuthContext:
    return AuthContext(public_key="pk-test", secret_key="sk-test")


@pytest.fixture
def perf_client():
    app.dependency_overrides[get_settings] = lambda: PERF_SETTINGS
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def auth_headers() -> dict[str, str]:
    creds = base64.b64encode(b"pk-test:sk-test").decode()
    return {"Authorization": f"Basic {creds}"}


# ---------------------------------------------------------------------------
# 1. Key parsing throughput
# ---------------------------------------------------------------------------


class TestKeyParsingPerformance:
    """parse_key_meta is called per S3 object — must stay fast."""

    def test_parse_1000_keys(self):
        keys = _generate_s3_keys(1000)
        start = time.perf_counter()
        for k in keys:
            parse_key_meta(k["Key"])
        elapsed = time.perf_counter() - start
        assert elapsed < 0.1, f"Parsing 1000 keys took {elapsed:.3f}s (limit 0.1s)"

    def test_parse_10000_keys(self):
        keys = _generate_s3_keys(10_000)
        start = time.perf_counter()
        for k in keys:
            parse_key_meta(k["Key"])
        elapsed = time.perf_counter() - start
        assert elapsed < 0.5, f"Parsing 10000 keys took {elapsed:.3f}s (limit 0.5s)"

    def test_parse_invalid_keys_fast(self):
        """Invalid keys should fail quickly, not hang on backtracking."""
        bad_keys = [f"pk-test/bad_key_{i}.jsonl" for i in range(1000)]
        start = time.perf_counter()
        for k in bad_keys:
            parse_key_meta(k)
        elapsed = time.perf_counter() - start
        assert elapsed < 0.1, f"Parsing 1000 invalid keys took {elapsed:.3f}s (limit 0.1s)"


# ---------------------------------------------------------------------------
# 2. S3 listing & filtering throughput
# ---------------------------------------------------------------------------


class TestS3ListingPerformance:
    """Measure filtering overhead when listing large sets of S3 keys."""

    @pytest.mark.asyncio
    async def test_list_batch_keys_1000_no_filter(self, auth: AuthContext):
        keys = _generate_s3_keys(1000)
        mock_session = _make_mock_session(keys)
        with patch("dataimporter.s3.aioboto3.Session", return_value=mock_session):
            start = time.perf_counter()
            results = await list_batch_keys(auth, S3_DS)
            elapsed = time.perf_counter() - start
        assert len(results) == 1000
        assert elapsed < 0.5, f"Listing 1000 keys took {elapsed:.3f}s (limit 0.5s)"

    @pytest.mark.asyncio
    async def test_list_batch_keys_5000_with_filters(self, auth: AuthContext):
        keys = _generate_s3_keys(5000)
        mock_session = _make_mock_session(keys)
        start_ts = datetime(2026, 3, 1, 6, 0, 0, tzinfo=timezone.utc)
        end_ts = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
        with patch("dataimporter.s3.aioboto3.Session", return_value=mock_session):
            start = time.perf_counter()
            results = await list_batch_keys(
                auth, S3_DS, start=start_ts, end=end_ts, session_id="sess0010",
            )
            elapsed = time.perf_counter() - start
        assert elapsed < 1.0, f"Filtered listing of 5000 keys took {elapsed:.3f}s (limit 1.0s)"

    @pytest.mark.asyncio
    async def test_list_batch_urls_1000(self, auth: AuthContext):
        keys = _generate_s3_keys(1000)
        mock_session = _make_mock_session(keys)
        start_ts = datetime(2026, 3, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_ts = datetime(2026, 3, 2, 0, 0, 0, tzinfo=timezone.utc)
        with patch("dataimporter.s3.aioboto3.Session", return_value=mock_session):
            start = time.perf_counter()
            results = await list_batch_urls(auth, S3_DS, start_ts, end_ts)
            elapsed = time.perf_counter() - start
        assert len(results) == 1000
        assert elapsed < 2.0, f"URL generation for 1000 keys took {elapsed:.3f}s (limit 2.0s)"


# ---------------------------------------------------------------------------
# 3. Search route — ClickHouse backend
# ---------------------------------------------------------------------------


class TestClickHouseSearchPerformance:
    """Measure route + response serialization overhead for ClickHouse search."""

    def test_search_200_results(self, perf_client: TestClient, auth_headers: dict):
        ch_rows = _generate_ch_rows(200)
        ch_response = {"data": ch_rows, "rows": 200}

        with patch("dataimporter.clickhouse.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_resp = MagicMock()
            mock_resp.json.return_value = ch_response
            mock_resp.raise_for_status = MagicMock()
            mock_client.post = AsyncMock(return_value=mock_resp)
            mock_cls.return_value = mock_client

            start = time.perf_counter()
            resp = perf_client.get(
                "/api/public/logs/search?datasource=perf-ch&q=message",
                headers=auth_headers,
            )
            elapsed = time.perf_counter() - start

        assert resp.status_code == 200
        assert len(resp.json()["results"]) == 200
        assert elapsed < 1.0, f"CH search (200 rows) took {elapsed:.3f}s (limit 1.0s)"

    def test_search_500_results(self, perf_client: TestClient, auth_headers: dict):
        ch_rows = _generate_ch_rows(500)
        ch_response = {"data": ch_rows, "rows": 500}

        with patch("dataimporter.clickhouse.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_resp = MagicMock()
            mock_resp.json.return_value = ch_response
            mock_resp.raise_for_status = MagicMock()
            mock_client.post = AsyncMock(return_value=mock_resp)
            mock_cls.return_value = mock_client

            start = time.perf_counter()
            resp = perf_client.get(
                "/api/public/logs/search?datasource=perf-ch&q=message&limit=500",
                headers=auth_headers,
            )
            elapsed = time.perf_counter() - start

        assert resp.status_code == 200
        assert len(resp.json()["results"]) == 500
        assert elapsed < 2.0, f"CH search (500 rows) took {elapsed:.3f}s (limit 2.0s)"


# ---------------------------------------------------------------------------
# 4. Search route — Trino backend
# ---------------------------------------------------------------------------


class TestTrinoSearchPerformance:
    """Measure route + polling + response serialization for Trino search."""

    def test_search_200_results(self, perf_client: TestClient, auth_headers: dict):
        rows = _generate_ch_rows(200)
        columns = [
            {"name": "event_id"}, {"name": "event_type"}, {"name": "timestamp"},
            {"name": "project_id"}, {"name": "model"}, {"name": "name"},
            {"name": "trace_id"}, {"name": "session_id"}, {"name": "body"},
        ]
        col_names = [c["name"] for c in columns]
        trino_data = [[row[c] for c in col_names] for row in rows]

        # Single-page response (no nextUri — results ready immediately)
        trino_response = {"columns": columns, "data": trino_data, "stats": {}}

        with patch("dataimporter.trino.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)
            mock_resp = MagicMock()
            mock_resp.json.return_value = trino_response
            mock_resp.raise_for_status = MagicMock()
            mock_client.post = AsyncMock(return_value=mock_resp)
            mock_cls.return_value = mock_client

            start = time.perf_counter()
            resp = perf_client.get(
                "/api/public/logs/search?datasource=perf-trino&q=message",
                headers=auth_headers,
            )
            elapsed = time.perf_counter() - start

        assert resp.status_code == 200
        assert len(resp.json()["results"]) == 200
        assert elapsed < 1.0, f"Trino search (200 rows) took {elapsed:.3f}s (limit 1.0s)"

    def test_search_with_pagination(self, perf_client: TestClient, auth_headers: dict):
        """Simulate Trino returning results across multiple pages."""
        rows = _generate_ch_rows(300)
        columns = [
            {"name": "event_id"}, {"name": "event_type"}, {"name": "timestamp"},
            {"name": "project_id"}, {"name": "model"}, {"name": "name"},
            {"name": "trace_id"}, {"name": "session_id"}, {"name": "body"},
        ]
        col_names = [c["name"] for c in columns]
        trino_data = [[row[c] for c in col_names] for row in rows]

        # Split into 3 pages of 100 rows
        page1 = {
            "columns": columns,
            "data": trino_data[:100],
            "nextUri": "http://localhost:8080/v1/statement/query1/1",
        }
        page2 = {
            "columns": columns,
            "data": trino_data[100:200],
            "nextUri": "http://localhost:8080/v1/statement/query1/2",
        }
        page3 = {
            "columns": columns,
            "data": trino_data[200:],
            "stats": {},
        }

        with patch("dataimporter.trino.httpx.AsyncClient") as mock_cls:
            mock_client = AsyncMock()
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=False)

            post_resp = MagicMock()
            post_resp.json.return_value = page1
            post_resp.raise_for_status = MagicMock()
            mock_client.post = AsyncMock(return_value=post_resp)

            get_resp_2 = MagicMock()
            get_resp_2.json.return_value = page2
            get_resp_2.raise_for_status = MagicMock()

            get_resp_3 = MagicMock()
            get_resp_3.json.return_value = page3
            get_resp_3.raise_for_status = MagicMock()

            mock_client.get = AsyncMock(side_effect=[get_resp_2, get_resp_3])
            mock_cls.return_value = mock_client

            with patch("dataimporter.trino._POLL_INTERVAL", 0):
                start = time.perf_counter()
                resp = perf_client.get(
                    "/api/public/logs/search?datasource=perf-trino&q=message&limit=500",
                    headers=auth_headers,
                )
                elapsed = time.perf_counter() - start

        assert resp.status_code == 200
        assert len(resp.json()["results"]) == 300
        assert elapsed < 2.0, f"Trino paginated search (3 pages) took {elapsed:.3f}s (limit 2.0s)"


# ---------------------------------------------------------------------------
# 5. Route response time — lightweight endpoints
# ---------------------------------------------------------------------------


class TestEndpointLatency:
    """Health/metadata endpoints should respond near-instantly."""

    def test_livez_latency(self, perf_client: TestClient):
        # Warm up
        perf_client.get("/livez")

        times = []
        for _ in range(100):
            start = time.perf_counter()
            resp = perf_client.get("/livez")
            times.append(time.perf_counter() - start)
            assert resp.status_code == 200

        avg = sum(times) / len(times)
        p99 = sorted(times)[98]
        assert avg < 0.01, f"livez avg {avg:.4f}s (limit 0.01s)"
        assert p99 < 0.05, f"livez p99 {p99:.4f}s (limit 0.05s)"

    def test_datasources_latency(self, perf_client: TestClient):
        # Warm up
        perf_client.get("/api/public/datasources")

        times = []
        for _ in range(100):
            start = time.perf_counter()
            resp = perf_client.get("/api/public/datasources")
            times.append(time.perf_counter() - start)
            assert resp.status_code == 200

        avg = sum(times) / len(times)
        p99 = sorted(times)[98]
        assert avg < 0.01, f"datasources avg {avg:.4f}s (limit 0.01s)"
        assert p99 < 0.05, f"datasources p99 {p99:.4f}s (limit 0.05s)"

    def test_ui_config_latency(self, perf_client: TestClient):
        perf_client.get("/api/public/ui-config")

        times = []
        for _ in range(100):
            start = time.perf_counter()
            resp = perf_client.get("/api/public/ui-config")
            times.append(time.perf_counter() - start)
            assert resp.status_code == 200

        avg = sum(times) / len(times)
        assert avg < 0.01, f"ui-config avg {avg:.4f}s (limit 0.01s)"


# ---------------------------------------------------------------------------
# 6. Concurrent requests
# ---------------------------------------------------------------------------


class TestConcurrentRequests:
    """Verify the app handles concurrent requests without degradation."""

    def test_concurrent_log_listing(self, perf_client: TestClient, auth_headers: dict):
        import concurrent.futures

        keys = _generate_s3_keys(500)
        mock_session = _make_mock_session(keys)

        def make_request(_):
            resp = perf_client.get(
                "/api/public/logs/list?datasource=perf-s3",
                headers=auth_headers,
            )
            return resp.status_code

        start = time.perf_counter()
        with patch("dataimporter.s3.aioboto3.Session", return_value=mock_session):
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
                statuses = list(pool.map(make_request, range(10)))
        elapsed = time.perf_counter() - start

        assert all(s == 200 for s in statuses)
        assert elapsed < 5.0, f"10 concurrent listings took {elapsed:.3f}s (limit 5.0s)"

    def test_concurrent_search(self, perf_client: TestClient, auth_headers: dict):
        import concurrent.futures

        ch_rows = _generate_ch_rows(100)
        ch_response = {"data": ch_rows, "rows": 100}

        def make_request(_):
            with patch("dataimporter.clickhouse.httpx.AsyncClient") as mock_cls:
                mock_client = AsyncMock()
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=False)
                mock_resp = MagicMock()
                mock_resp.json.return_value = ch_response
                mock_resp.raise_for_status = MagicMock()
                mock_client.post = AsyncMock(return_value=mock_resp)
                mock_cls.return_value = mock_client

                resp = perf_client.get(
                    "/api/public/logs/search?datasource=perf-ch&q=test",
                    headers=auth_headers,
                )
            return resp.status_code

        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
            statuses = list(pool.map(make_request, range(10)))
        elapsed = time.perf_counter() - start

        assert all(s == 200 for s in statuses)
        assert elapsed < 5.0, f"10 concurrent searches took {elapsed:.3f}s (limit 5.0s)"


# ---------------------------------------------------------------------------
# 7. JSON serialization overhead
# ---------------------------------------------------------------------------


class TestSerializationPerformance:
    """Measure JSON body parsing overhead in adapters."""

    def test_json_body_parsing_500_rows(self):
        """Simulate the body JSON parsing loop from clickhouse/trino adapters."""
        rows = _generate_ch_rows(500)
        start = time.perf_counter()
        results = []
        for row in rows:
            try:
                body = json.loads(row.get("body", "{}"))
            except (json.JSONDecodeError, TypeError):
                body = {}
            results.append({
                "id": row.get("event_id", ""),
                "type": row.get("event_type", ""),
                "timestamp": str(row.get("timestamp", "")),
                "body": body,
            })
        elapsed = time.perf_counter() - start
        assert len(results) == 500
        assert elapsed < 0.1, f"Parsing 500 JSON bodies took {elapsed:.3f}s (limit 0.1s)"

    def test_json_body_parsing_with_large_bodies(self):
        """Bodies with nested structures should still parse quickly."""
        large_body = json.dumps({
            "messages": [
                {"role": "user", "content": f"message content {'x' * 1000}"}
                for _ in range(20)
            ],
            "metadata": {f"key_{i}": f"value_{i}" for i in range(50)},
        })
        rows = [{"body": large_body, "event_id": f"e{i}", "event_type": "llm", "timestamp": "2026-01-01"} for i in range(200)]

        start = time.perf_counter()
        results = []
        for row in rows:
            body = json.loads(row["body"])
            results.append({"id": row["event_id"], "body": body})
        elapsed = time.perf_counter() - start
        assert len(results) == 200
        assert elapsed < 0.5, f"Parsing 200 large JSON bodies took {elapsed:.3f}s (limit 0.5s)"
