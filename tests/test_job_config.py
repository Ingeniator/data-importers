"""Tests for the JobConfig schema and its API endpoints."""
from __future__ import annotations

import textwrap

import pytest
import yaml
from pydantic import ValidationError

from dataimporter.job_config import (
    JobConfig,
    JobConfig,
    dump_job_config,
    parse_job_config,
)


# ── parse_job_config ──────────────────────────────────────────────────────────

FULL_YAML = textwrap.dedent("""\
    ingestion:
      datasource: my-clickhouse
      type: clickhouse

    filters:
      mode: and
      rules:
        - field: created_at
          op: last
          value: "7d"
        - field: cost_usd
          op: greater_than
          value: 0.05

    masking:
      default_policy: deny
      allow_fields:
        - trace.id
        - trace.timestamp
      rules:
        - field: trace.input.password
          action: remove

    sampling:
      strategy: hybrid
      strict_schema: false
      max_traces: 1000
      rules:
        - type: random
          rate: 0.2
        - type: high_cost
          rate: 0.1
          field: cost_usd
          params:
            percentile: "95"

    asset_resolution:
      enabled: true
      sources:
        - field: trace.attachments
      fetch_mode: metadata_only
      deduplicate_by:
        - asset.sha256
""")


def test_parse_full_yaml():
    cfg = parse_job_config(FULL_YAML)

    assert cfg.ingestion is not None
    assert cfg.ingestion.datasource == "my-clickhouse"
    assert cfg.ingestion.type == "clickhouse"

    assert cfg.filters is not None
    assert cfg.filters.mode == "and"
    assert len(cfg.filters.rules) == 2
    assert cfg.filters.rules[0].op == "last"
    assert cfg.filters.rules[0].value == "7d"
    assert cfg.filters.rules[1].op == "greater_than"

    assert cfg.masking is not None
    assert cfg.masking.default_policy == "deny"
    assert "trace.id" in cfg.masking.allow_fields
    assert cfg.masking.rules[0].field == "trace.input.password"
    assert cfg.masking.rules[0].action == "remove"

    assert cfg.sampling is not None
    assert cfg.sampling.strategy == "hybrid"
    assert cfg.sampling.max_traces == 1000
    assert len(cfg.sampling.rules) == 2
    assert cfg.sampling.rules[0].rate == pytest.approx(0.2)
    assert cfg.sampling.rules[1].type == "high_cost"
    assert cfg.sampling.rules[1].params == {"percentile": "95"}

    assert cfg.asset_resolution is not None
    assert cfg.asset_resolution.enabled is True
    assert cfg.asset_resolution.sources[0].field == "trace.attachments"
    assert cfg.asset_resolution.fetch_mode == "metadata_only"
    assert cfg.asset_resolution.deduplicate_by == ["asset.sha256"]


def test_parse_minimal_yaml():
    cfg = parse_job_config("ingestion:\n  datasource: ds1\n")
    assert cfg.ingestion.datasource == "ds1"
    assert cfg.filters is None
    assert cfg.sampling is None


def test_parse_empty_yaml():
    cfg = parse_job_config("")
    assert cfg.ingestion is None


def test_invalid_op_rejected():
    bad = "filters:\n  rules:\n    - field: x\n      op: nonexistent_op\n"
    with pytest.raises((ValidationError, ValueError)):
        parse_job_config(bad)


def test_between_requires_from_to():
    bad = textwrap.dedent("""\
        filters:
          rules:
            - field: created_at
              op: between
    """)
    with pytest.raises((ValidationError, ValueError)):
        parse_job_config(bad)


def test_between_valid():
    cfg = parse_job_config(textwrap.dedent("""\
        filters:
          rules:
            - field: created_at
              op: between
              from: "2024-01-01T00:00:00Z"
              to: "2024-01-31T23:59:59Z"
    """))
    rule = cfg.filters.rules[0]
    assert rule.op == "between"
    assert rule.from_ == "2024-01-01T00:00:00Z"
    assert rule.to == "2024-01-31T23:59:59Z"


def test_sampling_rate_bounds():
    with pytest.raises((ValidationError, ValueError)):
        parse_job_config(
            "sampling:\n  rules:\n    - type: random\n      rate: 1.5\n"
        )

    with pytest.raises((ValidationError, ValueError)):
        parse_job_config(
            "sampling:\n  rules:\n    - type: random\n      rate: -0.1\n"
        )


def test_invalid_yaml_raises():
    with pytest.raises(Exception):
        parse_job_config("}{invalid yaml{{")


# ── dump_job_config ───────────────────────────────────────────────────────────

def test_roundtrip():
    cfg = parse_job_config(FULL_YAML)
    dumped = dump_job_config(cfg)
    cfg2 = parse_job_config(dumped)

    assert cfg2.ingestion.datasource == cfg.ingestion.datasource
    assert cfg2.sampling.max_traces == cfg.sampling.max_traces
    assert len(cfg2.sampling.rules) == len(cfg.sampling.rules)
    assert cfg2.masking.allow_fields == cfg.masking.allow_fields


def test_dump_excludes_none_sections():
    cfg = JobConfig(ingestion=None, filters=None)
    dumped = dump_job_config(cfg)
    data = yaml.safe_load(dumped)
    assert data == {} or data is None


# ── JSON Schema ───────────────────────────────────────────────────────────────

def test_json_schema_has_expected_keys():
    schema = JobConfig.model_json_schema()
    props = schema.get("properties", {})
    assert "ingestion" in props
    assert "filters" in props
    assert "masking" in props
    assert "sampling" in props
    assert "asset_resolution" in props


# ── API endpoints ─────────────────────────────────────────────────────────────

def test_schema_endpoint(client):
    resp = client.get("/api/public/config/schema")
    assert resp.status_code == 200
    data = resp.json()
    assert "properties" in data
    assert "ingestion" in data["properties"]


def test_validate_endpoint_valid(client):
    resp = client.post(
        "/api/public/config/validate",
        json={"yaml_text": FULL_YAML},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is True
    assert body["errors"] == []
    assert body["parsed"]["ingestion"]["datasource"] == "my-clickhouse"


def test_validate_endpoint_invalid(client):
    resp = client.post(
        "/api/public/config/validate",
        json={"yaml_text": "sampling:\n  rules:\n    - type: random\n      rate: 99\n"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is False
    assert body["errors"]


def test_validate_endpoint_bad_yaml(client):
    resp = client.post(
        "/api/public/config/validate",
        json={"yaml_text": "}{[invalid"},
    )
    assert resp.status_code in (200, 400)
    if resp.status_code == 200:
        assert resp.json()["valid"] is False
