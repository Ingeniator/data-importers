"""Tests for dataimporter.sampling — apply_sampling, apply_sampling_s3,
and the sampling path in _run_inline / _run_inline_events."""
from __future__ import annotations

import random
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dataimporter.config import DatasetTarget, Datasource
from dataimporter.importer import run_import_dataset, run_import_dataset_events
from dataimporter.routes.export import EventsExportRequest, ExportRequest
from dataimporter.sampling import (
    SamplingRule,
    _filter_pool,
    _percentile,
    _sample,
    apply_sampling,
    apply_sampling_s3,
)

# ── helpers ───────────────────────────────────────────────────────────────────

TARGET = DatasetTarget(
    name="t",
    base_url="http://ds",
    token_url="http://ds/token",
    client_id="c",
    client_secret="s",
)

S3_DS = Datasource(
    name="s3",
    type="s3",
    bucket="b",
    region="us-east-1",
    access_key_id="k",
    secret_access_key="s",
)


def _rule(strategy: str, rate: float = 100, field: str | None = None, **params) -> SamplingRule:
    return SamplingRule(strategy=strategy, rate=rate, field=field, params=params)


def _traces(n: int, **extra) -> list[dict]:
    return [{"trace_id": f"t{i}", **extra} for i in range(n)]


# ── _percentile ───────────────────────────────────────────────────────────────


def test_percentile_empty():
    assert _percentile([], 95) == 0.0


def test_percentile_single():
    assert _percentile([10.0], 50) == 10.0


def test_percentile_known_values():
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert _percentile(values, 0) == 1.0
    assert _percentile(values, 100) == 5.0
    p50 = _percentile(values, 50)
    assert 2.5 < p50 < 3.5


def test_percentile_skips_none():
    values = [1.0, None, 3.0]  # type: ignore[list-item]
    result = _percentile(values, 50)
    assert result == 2.0


# ── _sample ───────────────────────────────────────────────────────────────────


def test_sample_empty_pool():
    assert _sample([], 50, None) == []


def test_sample_rate_zero():
    assert _sample([{"trace_id": "x"}], 0, None) == []


def test_sample_rate_100_returns_all():
    pool = _traces(10)
    assert len(_sample(pool, 100, None)) == 10


def test_sample_rate_partial():
    pool = _traces(100)
    result = _sample(pool, 50, seed=42)
    assert 1 <= len(result) <= 100


def test_sample_seeded_is_reproducible():
    pool = _traces(50)
    a = _sample(pool, 40, seed=7)
    b = _sample(pool, 40, seed=7)
    assert [t["trace_id"] for t in a] == [t["trace_id"] for t in b]


def test_sample_different_seeds_differ():
    pool = _traces(50)
    a = _sample(pool, 40, seed=1)
    b = _sample(pool, 40, seed=2)
    # Statistically near-certain to differ for large pools
    assert [t["trace_id"] for t in a] != [t["trace_id"] for t in b]


# ── _filter_pool ──────────────────────────────────────────────────────────────


def test_filter_pool_random_returns_all():
    traces = _traces(5)
    result = _filter_pool(traces, _rule("random"))
    assert len(result) == 5


def test_filter_pool_no_field_returns_empty_for_non_random():
    traces = _traces(5, cost=1.0)
    result = _filter_pool(traces, SamplingRule(strategy="high_cost", rate=100, field=None))
    assert result == []


def test_filter_pool_high_cost():
    traces = [
        {"trace_id": "cheap", "cost": 0.01},
        {"trace_id": "mid",   "cost": 0.50},
        {"trace_id": "pricey","cost": 1.00},
        {"trace_id": "costly","cost": 5.00},
    ]
    rule = _rule("high_cost", field="cost", percentile="50")
    result = _filter_pool(traces, rule)
    # Should include only traces above the 50th percentile
    result_ids = {t["trace_id"] for t in result}
    assert "cheap" not in result_ids
    assert "costly" in result_ids


def test_filter_pool_latency_spike():
    traces = [
        {"trace_id": "fast", "latency": 0.1},
        {"trace_id": "slow", "latency": 9.9},
    ]
    rule = _rule("latency_spike", field="latency", percentile="50")
    result = _filter_pool(traces, rule)
    assert any(t["trace_id"] == "slow" for t in result)
    assert all(t["trace_id"] != "fast" for t in result)


def test_filter_pool_long_trace():
    traces = [
        {"trace_id": "short", "duration": 3},
        {"trace_id": "long",  "duration": 25},
    ]
    rule = _rule("long_trace", field="duration", threshold="10")
    result = _filter_pool(traces, rule)
    assert len(result) == 1
    assert result[0]["trace_id"] == "long"


def test_filter_pool_failure_string():
    traces = [
        {"trace_id": "ok",    "status": "success"},
        {"trace_id": "fail",  "status": "error"},
        {"trace_id": "crash", "status": "failed"},
    ]
    rule = _rule("failure", field="status")
    result = _filter_pool(traces, rule)
    ids = {t["trace_id"] for t in result}
    assert "fail" in ids
    assert "crash" in ids
    assert "ok" not in ids


def test_filter_pool_failure_http_status():
    traces = [
        {"trace_id": "ok",  "code": 200},
        {"trace_id": "err", "code": 500},
    ]
    rule = _rule("failure", field="code")
    result = _filter_pool(traces, rule)
    assert result[0]["trace_id"] == "err"


def test_filter_pool_failure_bool():
    traces = [
        {"trace_id": "fine",   "is_error": False},
        {"trace_id": "broken", "is_error": True},
    ]
    rule = _rule("failure", field="is_error")
    result = _filter_pool(traces, rule)
    assert len(result) == 1
    assert result[0]["trace_id"] == "broken"


def test_filter_pool_user_dissatisfaction_numeric():
    traces = [
        {"trace_id": "happy", "score": 0.9},
        {"trace_id": "meh",   "score": -0.5},
    ]
    rule = _rule("user_dissatisfaction", field="score", threshold="0")
    result = _filter_pool(traces, rule)
    assert len(result) == 1
    assert result[0]["trace_id"] == "meh"


def test_filter_pool_user_dissatisfaction_tag_list():
    traces = [
        {"trace_id": "good", "tags": ["helpful"]},
        {"trace_id": "bad",  "tags": ["thumbsdown", "irrelevant"]},
    ]
    rule = _rule("user_dissatisfaction", field="tags")
    result = _filter_pool(traces, rule)
    assert result[0]["trace_id"] == "bad"


def test_filter_pool_business_critical_contains():
    traces = [
        {"trace_id": "generic", "category": "general"},
        {"trace_id": "vip",     "category": "enterprise-billing"},
    ]
    rule = _rule("business_critical", field="category", value="billing")
    result = _filter_pool(traces, rule)
    assert len(result) == 1
    assert result[0]["trace_id"] == "vip"


def test_filter_pool_business_critical_exact():
    traces = [
        {"trace_id": "x", "tag": "billing"},
        {"trace_id": "y", "tag": "non-billing"},
    ]
    rule = _rule("business_critical", field="tag", value="billing", match_type="exact")
    result = _filter_pool(traces, rule)
    assert len(result) == 1
    assert result[0]["trace_id"] == "x"


def test_filter_pool_prompt_version_change():
    traces = [
        {"trace_id": "old", "version": "v1"},
        {"trace_id": "new", "version": "v2"},
    ]
    rule = _rule("prompt_version_change", field="version", baseline="v1")
    result = _filter_pool(traces, rule)
    assert len(result) == 1
    assert result[0]["trace_id"] == "new"


def test_filter_pool_low_confidence():
    traces = [
        {"trace_id": "sure",    "confidence": 0.95},
        {"trace_id": "unsure",  "confidence": 0.30},
    ]
    rule = _rule("low_confidence", field="confidence", threshold="0.5")
    result = _filter_pool(traces, rule)
    assert result[0]["trace_id"] == "unsure"


def test_filter_pool_weird_tool_sequences_unexpected():
    traces = [
        {"trace_id": "normal", "tools": [{"name": "search"}, {"name": "calc"}]},
        {"trace_id": "weird",  "tools": [{"name": "rm_rf"}, {"name": "exec"}]},
    ]
    rule = _rule("weird_tool_sequences", field="tools", unexpected_tools="rm_rf,exec")
    result = _filter_pool(traces, rule)
    assert result[0]["trace_id"] == "weird"


def test_filter_pool_weird_tool_sequences_repeat():
    traces = [
        {"trace_id": "fine",   "tools": [{"name": "search"}, {"name": "calc"}]},
        {"trace_id": "looped", "tools": [{"name": "search"}] * 5},
    ]
    rule = _rule("weird_tool_sequences", field="tools", max_repeat="3")
    result = _filter_pool(traces, rule)
    assert result[0]["trace_id"] == "looped"


def test_filter_pool_weird_tool_sequences_min_total():
    traces = [
        {"trace_id": "short", "tools": [{"name": "a"}, {"name": "b"}]},
        {"trace_id": "long",  "tools": [{"name": f"t{i}"} for i in range(20)]},
    ]
    rule = _rule("weird_tool_sequences", field="tools", min_total_calls="10")
    result = _filter_pool(traces, rule)
    assert result[0]["trace_id"] == "long"


def test_filter_pool_weird_tool_sequences_non_list_field():
    traces = [{"trace_id": "x", "tools": "not-a-list"}]
    rule = _rule("weird_tool_sequences", field="tools")
    assert _filter_pool(traces, rule) == []


def test_filter_pool_unknown_strategy_returns_empty():
    traces = _traces(5)
    rule = SamplingRule(strategy="does_not_exist", rate=100)
    assert _filter_pool(traces, rule) == []


# ── apply_sampling ────────────────────────────────────────────────────────────


def test_apply_sampling_no_rules_passthrough():
    traces = _traces(10)
    result, warning = apply_sampling(traces, [])
    assert result == traces
    assert warning is None


def test_apply_sampling_random_all():
    traces = _traces(20)
    rules = [_rule("random", rate=100)]
    result, warning = apply_sampling(traces, rules)
    assert len(result) == 20
    assert warning is None


def test_apply_sampling_random_partial():
    traces = _traces(100)
    rules = [SamplingRule(strategy="random", rate=50, params={"seed": 42})]
    result, _ = apply_sampling(traces, rules)
    assert 1 <= len(result) <= 100


def test_apply_sampling_max_traces_cap():
    traces = _traces(100)
    rules = [_rule("random", rate=100)]
    result, _ = apply_sampling(traces, rules, max_traces=10)
    assert len(result) <= 10


def test_apply_sampling_deduplication():
    """Two rules that both select the same trace must not produce duplicates."""
    traces = [{"trace_id": "shared", "x": 1}]
    # Both random rules at rate=100 will each try to include "shared"
    rules = [_rule("random", rate=100), _rule("random", rate=100)]
    result, _ = apply_sampling(traces, rules)
    assert len(result) == 1


def test_apply_sampling_strict_schema_filters_missing_fields():
    traces = [
        {"trace_id": "complete", "input": "hi", "output": "bye"},
        {"trace_id": "partial",  "input": "hi"},  # missing 'output'
    ]
    snapshot = {"input": "string", "output": "string"}
    rules = [_rule("random", rate=100)]
    result, _ = apply_sampling(traces, rules, strict_schema=True, schema_snapshot=snapshot)
    ids = {t["trace_id"] for t in result}
    assert "complete" in ids
    assert "partial" not in ids


def test_apply_sampling_strict_schema_no_snapshot_no_filter():
    """strict_schema=True without snapshot should not filter anything."""
    traces = _traces(5)
    rules = [_rule("random", rate=100)]
    result, _ = apply_sampling(traces, rules, strict_schema=True, schema_snapshot=None)
    assert len(result) == 5


def test_apply_sampling_zero_results_emits_warning():
    traces = _traces(10)
    # rate=0 → nothing sampled
    rules = [SamplingRule(strategy="random", rate=0)]
    result, warning = apply_sampling(traces, rules)
    assert result == []
    assert warning is not None
    assert "0 results" in warning


def test_apply_sampling_multiple_rules_union():
    cheap = [{"trace_id": "cheap", "cost": 0.01}]
    pricey = [{"trace_id": "pricey", "cost": 5.00}]
    traces = cheap + pricey
    rules = [
        _rule("low_confidence", rate=100, field="cost", threshold="0.1"),  # matches cheap
        _rule("high_cost",      rate=100, field="cost", percentile="0"),   # matches pricey
    ]
    result, _ = apply_sampling(traces, rules)
    ids = {t["trace_id"] for t in result}
    assert "cheap" in ids
    assert "pricey" in ids


# ── apply_sampling_s3 ─────────────────────────────────────────────────────────


def test_apply_sampling_s3_no_rules_passthrough():
    keys = ["a.jsonl", "b.jsonl", "c.jsonl"]
    result, warning = apply_sampling_s3(keys, [], S3_DS)
    assert result == keys
    assert warning is None


def test_apply_sampling_s3_random_uses_stubs_not_read():
    """random strategy doesn't need file contents — should not call read_s3_traces_for_sampling."""
    keys = [f"key{i}.jsonl" for i in range(10)]
    rules = [_rule("random", rate=50, seed=1)]
    with patch("dataimporter.sampling.read_s3_traces_for_sampling") as mock_read:
        result, _ = apply_sampling_s3(keys, rules, S3_DS)
        mock_read.assert_not_called()
    assert len(result) <= len(keys)


def test_apply_sampling_s3_field_strategy_reads_files():
    """A non-random strategy must call read_s3_traces_for_sampling to get field values."""
    keys = ["a.jsonl", "b.jsonl"]
    mock_traces = [
        {"_key": "a.jsonl", "cost": 0.01},
        {"_key": "b.jsonl", "cost": 5.00},
    ]
    rules = [_rule("high_cost", rate=100, field="cost", percentile="50")]
    with patch("dataimporter.sampling.read_s3_traces_for_sampling", return_value=mock_traces) as mock_read:
        result, _ = apply_sampling_s3(keys, rules, S3_DS)
        mock_read.assert_called_once()
    # Only the high-cost key should survive
    assert "b.jsonl" in result
    assert "a.jsonl" not in result


def test_apply_sampling_s3_max_traces_caps_keys():
    keys = [f"key{i}.jsonl" for i in range(20)]
    rules = [_rule("random", rate=100)]
    result, _ = apply_sampling_s3(keys, rules, S3_DS, max_traces=5)
    assert len(result) <= 5


def test_apply_sampling_s3_zero_results_emits_warning():
    keys = ["a.jsonl"]
    rules = [SamplingRule(strategy="random", rate=0)]
    _, warning = apply_sampling_s3(keys, rules, S3_DS)
    assert warning is not None


# ── _run_inline_events with sampling ─────────────────────────────────────────


EVENTS_10 = [{"trace_id": f"t{i}", "cost": float(i)} for i in range(10)]


@pytest.mark.asyncio
async def test_run_inline_events_sampling_reduces_events():
    """Sampling should run before upload; fewer events should be uploaded."""
    with (
        patch("dataimporter.dataset_service.create_dataset", new_callable=AsyncMock, return_value="ds-1"),
        patch("dataimporter.dataset_service.upload_file", new_callable=AsyncMock, return_value={}) as mock_upload,
        patch("dataimporter.sampling._DUCKDB_AVAILABLE", False),  # keep sampling pure
    ):
        result = await run_import_dataset_events(
            TARGET,
            events=EVENTS_10,
            dataset_name="ds",
            access="organization",
            dataset_type="DATASET",
            datasource="s3",
            target="t",
            format="jsonl",
            sampling=[{"strategy": "random", "rate": 50, "params": {"seed": 42}}],
        )

    assert result["records_uploaded"] < len(EVENTS_10)
    content: bytes = mock_upload.call_args.args[3]
    lines = [l for l in content.decode().strip().split("\n") if l]
    assert len(lines) == result["records_uploaded"]


@pytest.mark.asyncio
async def test_run_inline_events_sampling_zero_results_warning():
    with (
        patch("dataimporter.dataset_service.create_dataset", new_callable=AsyncMock, return_value="ds-1"),
        patch("dataimporter.dataset_service.upload_file", new_callable=AsyncMock, return_value={}),
    ):
        result = await run_import_dataset_events(
            TARGET,
            events=EVENTS_10,
            dataset_name="ds",
            access="organization",
            dataset_type="DATASET",
            datasource="s3",
            target="t",
            format="jsonl",
            sampling=[{"strategy": "random", "rate": 0}],
        )

    assert result["records_uploaded"] == 0
    assert result["sampling_warning"] is not None


@pytest.mark.asyncio
async def test_run_inline_events_sampling_strict_schema():
    """strict_schema filters out events missing required fields."""
    events = [
        {"trace_id": "full", "input": "hi", "output": "bye"},
        {"trace_id": "partial", "input": "hi"},  # missing 'output'
    ]
    with (
        patch("dataimporter.dataset_service.create_dataset", new_callable=AsyncMock, return_value="ds-1"),
        patch("dataimporter.dataset_service.upload_file", new_callable=AsyncMock, return_value={}) as mock_upload,
    ):
        result = await run_import_dataset_events(
            TARGET,
            events=events,
            dataset_name="ds",
            access="organization",
            dataset_type="DATASET",
            datasource="s3",
            target="t",
            format="jsonl",
            sampling=[{"strategy": "random", "rate": 100}],
            strict_schema=True,
            schema_snapshot={"input": "string", "output": "string"},
        )

    assert result["records_uploaded"] == 1
    import json
    content = json.loads(mock_upload.call_args.args[3])
    assert content["trace_id"] == "full"


@pytest.mark.asyncio
async def test_run_inline_events_sampling_max_traces():
    with (
        patch("dataimporter.dataset_service.create_dataset", new_callable=AsyncMock, return_value="ds-1"),
        patch("dataimporter.dataset_service.upload_file", new_callable=AsyncMock, return_value={}),
    ):
        result = await run_import_dataset_events(
            TARGET,
            events=EVENTS_10,
            dataset_name="ds",
            access="organization",
            dataset_type="DATASET",
            datasource="s3",
            target="t",
            format="jsonl",
            sampling=[{"strategy": "random", "rate": 100}],
            max_traces=3,
        )

    assert result["records_uploaded"] <= 3


# ── _run_inline (S3 keys) with sampling ──────────────────────────────────────


def _make_s3_mock_session(content: bytes = b'{"trace_id": "x"}'):
    """Build a properly structured S3 session mock for _run_inline tests.

    The session itself must be a MagicMock (not AsyncMock) because s3.client(...)
    is called synchronously and the result used as an async context manager.
    """
    mock_body = AsyncMock()
    mock_body.read = AsyncMock(return_value=content)

    mock_s3_client = AsyncMock()
    mock_s3_client.__aenter__ = AsyncMock(return_value=mock_s3_client)
    mock_s3_client.__aexit__ = AsyncMock(return_value=False)
    mock_s3_client.get_object = AsyncMock(return_value={"Body": mock_body})

    mock_session = MagicMock()  # MagicMock so .client() returns sync, not a coroutine
    mock_session.client.return_value = mock_s3_client
    return mock_session


@pytest.mark.asyncio
async def test_run_inline_sampling_reduces_keys():
    """apply_sampling_s3 should filter keys before S3 downloads."""
    all_keys = [f"key{i}.jsonl" for i in range(10)]
    sampled_keys = all_keys[:3]  # pretend sampling picked 3

    with (
        patch("dataimporter.sampling.apply_sampling_s3", return_value=(sampled_keys, None)) as mock_sample,
        patch("dataimporter.importer._s3_session", return_value=_make_s3_mock_session()),
        patch("dataimporter.dataset_service.create_dataset", new_callable=AsyncMock, return_value="ds-1"),
        patch("dataimporter.dataset_service.upload_file", new_callable=AsyncMock, return_value={}),
    ):
        result = await run_import_dataset(
            TARGET,
            S3_DS,
            keys=all_keys,
            dataset_name="ds",
            access="organization",
            dataset_type="DATASET",
            datasource="s3",
            target="t",
            sampling=[{"strategy": "random", "rate": 30, "params": {"seed": 1}}],
        )

    mock_sample.assert_called_once()
    assert result["files_uploaded"] == len(sampled_keys)


@pytest.mark.asyncio
async def test_run_inline_sampling_warning_propagated():
    """sampling_warning returned by apply_sampling_s3 must appear in result."""
    with (
        patch("dataimporter.sampling.apply_sampling_s3",
              return_value=([], "Sampling config produced 0 results — no traces matched.")),
        patch("dataimporter.importer._s3_session", return_value=_make_s3_mock_session()),
        patch("dataimporter.dataset_service.create_dataset", new_callable=AsyncMock, return_value="ds-1"),
        patch("dataimporter.dataset_service.upload_file", new_callable=AsyncMock, return_value={}),
    ):
        result = await run_import_dataset(
            TARGET,
            S3_DS,
            keys=["a.jsonl"],
            dataset_name="ds",
            access="organization",
            dataset_type="DATASET",
            datasource="s3",
            target="t",
            sampling=[{"strategy": "random", "rate": 0}],
        )

    assert result["files_uploaded"] == 0
    assert result["sampling_warning"] is not None
    assert "0 results" in result["sampling_warning"]
