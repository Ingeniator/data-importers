"""Tests for dataimporter.routes.sample — helpers and the datasource/sample route."""
from __future__ import annotations

import base64
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from dataimporter.config import Datasource, Settings, get_settings
from dataimporter.main import app
from dataimporter.routes.sample import _collect_fields, _flatten_fields, _infer_type

S3_DS = Datasource(
    name="test-s3",
    type="s3",
    bucket="test-bucket",
    region="us-east-1",
    access_key_id="testing",
    secret_access_key="testing",
)

LANGFUSE_DS = Datasource(
    name="test-lf",
    type="langfuse",
    url="http://langfuse",
    access_key_id="pk",
    secret_access_key="sk",
)

# A datasource type with no supported sample path
UNSUPPORTED_DS = Datasource(name="unsupported", type="other")

SAMPLE_SETTINGS = Settings(datasources=(S3_DS, LANGFUSE_DS, UNSUPPORTED_DS))


def _auth() -> dict[str, str]:
    creds = base64.b64encode(b"pk-test:sk-test").decode()
    return {"Authorization": f"Basic {creds}"}


@pytest.fixture
def sample_client():
    app.dependency_overrides[get_settings] = lambda: SAMPLE_SETTINGS
    yield TestClient(app)
    app.dependency_overrides.clear()


# ── _infer_type ───────────────────────────────────────────────────────────────


def test_infer_type_bool_true():
    assert _infer_type(True) == "bool"


def test_infer_type_bool_false():
    assert _infer_type(False) == "bool"


def test_infer_type_int():
    assert _infer_type(42) == "int"


def test_infer_type_float():
    assert _infer_type(3.14) == "float"


def test_infer_type_list():
    assert _infer_type([1, 2, 3]) == "list"


def test_infer_type_dict():
    assert _infer_type({"key": "val"}) == "object"


def test_infer_type_string():
    assert _infer_type("hello") == "string"


def test_infer_type_none_falls_to_string():
    # None doesn't match bool/int/float/list/dict → falls through to "string"
    assert _infer_type(None) == "string"


def test_infer_type_bool_before_int():
    # bool is a subclass of int — must be detected as bool, not int
    assert _infer_type(True) == "bool"
    assert _infer_type(1) == "int"


# ── _collect_fields ───────────────────────────────────────────────────────────


def test_collect_fields_simple_values():
    fields: dict = {}
    _collect_fields({"name": "alice", "score": 7, "ratio": 0.5, "active": True}, fields)
    assert fields["name"]["type"] == "string"
    assert fields["name"]["example"] == "alice"
    assert fields["score"]["type"] == "int"
    assert fields["ratio"]["type"] == "float"
    assert fields["active"]["type"] == "bool"


def test_collect_fields_nested_dot_notation():
    fields: dict = {}
    _collect_fields({"meta": {"user": "bob", "count": 3}}, fields)
    assert "meta" in fields
    assert fields["meta"]["type"] == "object"
    assert "meta.user" in fields
    assert fields["meta.user"]["example"] == "bob"
    assert "meta.count" in fields


def test_collect_fields_skips_underscore_prefixed_keys():
    fields: dict = {}
    _collect_fields({"_internal": "secret", "public": "value"}, fields)
    assert "_internal" not in fields
    assert "public" in fields


def test_collect_fields_list_example_truncated_to_two():
    fields: dict = {}
    _collect_fields({"items": [10, 20, 30, 40, 50]}, fields)
    assert fields["items"]["type"] == "list"
    assert fields["items"]["example"] == [10, 20]


def test_collect_fields_empty_list_example_is_none():
    fields: dict = {}
    _collect_fields({"items": []}, fields)
    assert fields["items"]["example"] is None


def test_collect_fields_dict_value_example_stays_none():
    fields: dict = {}
    _collect_fields({"obj": {"child": 1}}, fields)
    # The dict key itself gets no example (sub-fields carry the values)
    assert fields["obj"]["example"] is None
    assert fields["obj.child"]["example"] == 1


def test_collect_fields_max_depth_stops_recursion():
    fields: dict = {}
    deep = {"l1": {"l2": {"l3": {"l4": "leaf"}}}}
    # With max_depth=2: recurse at depth 0 (into l1), depth 1 (into l2), stop at depth 2
    _collect_fields(deep, fields, max_depth=2)
    assert "l1" in fields
    assert "l1.l2" in fields
    assert "l1.l2.l3" in fields      # added at depth=2, but not recursed further
    assert "l1.l2.l3.l4" not in fields


def test_collect_fields_first_call_sets_type_and_example():
    fields: dict = {}
    _collect_fields({"x": 1}, fields)
    assert fields["x"]["type"] == "int"
    assert fields["x"]["example"] == 1


def test_collect_fields_subsequent_calls_dont_overwrite_type():
    fields: dict = {}
    _collect_fields({"x": 1}, fields)
    _collect_fields({"x": 2}, fields)  # same key, different value
    assert fields["x"]["type"] == "int"
    assert fields["x"]["example"] == 1  # first non-None value is kept


def test_collect_fields_none_value_example_filled_by_later_record():
    fields: dict = {}
    _collect_fields({"x": None}, fields)
    assert fields["x"]["example"] is None
    _collect_fields({"x": "hello"}, fields)
    assert fields["x"]["example"] == "hello"  # filled in by second record


# ── _flatten_fields ───────────────────────────────────────────────────────────


def test_flatten_fields_empty_records():
    assert _flatten_fields([]) == {}


def test_flatten_fields_single_record():
    result = _flatten_fields([{"a": 1, "b": "hello"}])
    assert "a" in result
    assert "b" in result


def test_flatten_fields_multiple_records_union_of_keys():
    result = _flatten_fields([{"a": 1}, {"b": "x"}])
    assert "a" in result
    assert "b" in result


def test_flatten_fields_preserves_first_example():
    result = _flatten_fields([{"x": "first"}, {"x": "second"}])
    assert result["x"]["example"] == "first"


def test_flatten_fields_nested_keys_present():
    result = _flatten_fields([{"meta": {"user": "alice", "role": "admin"}}])
    assert "meta" in result
    assert "meta.user" in result
    assert "meta.role" in result


# ── /api/public/datasource/sample — route tests ───────────────────────────────


def test_sample_unknown_datasource_returns_404(sample_client):
    resp = sample_client.get(
        "/api/public/datasource/sample?datasource=nonexistent",
        headers=_auth(),
    )
    assert resp.status_code == 404


def test_sample_s3_missing_keys_returns_400(sample_client):
    resp = sample_client.get(
        "/api/public/datasource/sample?datasource=test-s3",
        headers=_auth(),
    )
    assert resp.status_code == 400
    assert "keys" in resp.json()["detail"].lower()


def test_sample_unsupported_datasource_type_returns_400(sample_client):
    resp = sample_client.get(
        "/api/public/datasource/sample?datasource=unsupported",
        headers=_auth(),
    )
    assert resp.status_code == 400


def test_sample_s3_with_keys_returns_fields(sample_client):
    records = [{"input": "hello", "output": "world", "score": 0.9}]
    with patch("dataimporter.sampling.read_s3_traces_for_sampling", return_value=records):
        resp = sample_client.get(
            "/api/public/datasource/sample?datasource=test-s3&keys=pk/a.jsonl",
            headers=_auth(),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert "fields" in body
    assert body["sample_count"] == 1
    assert "input" in body["fields"]
    assert "output" in body["fields"]
    assert "score" in body["fields"]


def test_sample_s3_limits_to_5_keys(sample_client):
    """Route should pass at most 5 keys to read_s3_traces_for_sampling."""
    records = [{"x": 1}]
    captured_keys: list = []

    def capture(keys, ds):
        captured_keys.extend(keys)
        return records

    with patch("dataimporter.sampling.read_s3_traces_for_sampling", side_effect=capture):
        resp = sample_client.get(
            "/api/public/datasource/sample?datasource=test-s3"
            "&keys=a.jsonl&keys=b.jsonl&keys=c.jsonl&keys=d.jsonl&keys=e.jsonl&keys=f.jsonl",
            headers=_auth(),
        )

    assert resp.status_code == 200
    assert len(captured_keys) == 5  # capped at 5


def test_sample_langfuse_returns_fields(sample_client):
    records = [{"trace_id": "t1", "input": "hi", "score": 1}]
    with patch(
        "dataimporter.langfuse.search_logs_langfuse",
        new_callable=AsyncMock,
        return_value=records,
    ):
        resp = sample_client.get(
            "/api/public/datasource/sample?datasource=test-lf",
            headers=_auth(),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["sample_count"] == 1
    assert "trace_id" in body["fields"]


def test_sample_no_records_returns_empty_fields(sample_client):
    with patch("dataimporter.sampling.read_s3_traces_for_sampling", return_value=[]):
        resp = sample_client.get(
            "/api/public/datasource/sample?datasource=test-s3&keys=pk/a.jsonl",
            headers=_auth(),
        )
    assert resp.status_code == 200
    body = resp.json()
    assert body["fields"] == {}
    assert body["sample_count"] == 0


def test_sample_requires_auth(sample_client):
    resp = sample_client.get("/api/public/datasource/sample?datasource=test-s3")
    assert resp.status_code == 401
