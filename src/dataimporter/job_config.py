"""Pydantic schema for a dataimporter export-job configuration YAML.

The YAML format mirrors what the browser UI downloads via ⬇ Config YAML.
Use ``JobConfig.model_validate(yaml.safe_load(text))`` to parse and validate.
Use ``JobConfig.model_json_schema()`` to get a JSON Schema for editor tooling.

Example
-------
::

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
        - body.input
      rules:
        - field: trace.input.password
          action: remove

    sampling:
      strategy: hybrid
      strict_schema: false
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
"""
from __future__ import annotations

import io
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, model_validator


# ── Ingestion ─────────────────────────────────────────────────────────────────

class IngestionConfig(BaseModel):
    """Which datasource to read from."""

    datasource: str = Field(..., description="Name of a configured datasource.")
    type: str | None = Field(
        None,
        description="Datasource type (informational; actual type is taken from server config).",
    )


# ── Filters ───────────────────────────────────────────────────────────────────

FilterOp = Literal[
    # equality
    "equals", "not_equals",
    # string
    "contains", "not_contains", "starts_with",
    # numeric comparisons
    "greater_than", "less_than", "greater_than_or_equal", "less_than_or_equal",
    # null checks
    "is_empty", "not_empty",
    # time-range shorthands (resolved by the UI; not sent to the backend as-is)
    "last",      # value = "7d" / "24h" / "1h" / …
    "between",   # from / to = ISO-8601 datetimes
]


class FilterRule(BaseModel):
    """A single filter predicate applied to a field."""

    field: str = Field(..., description="Dot-notation field path, e.g. ``body.model``.")
    op: FilterOp = Field(..., description="Comparison operator.")
    value: str | int | float | None = Field(
        None,
        description=(
            "Comparison value. "
            "For ``last``: a duration string such as ``7d``, ``24h``, ``1h``. "
            "For ``is_empty`` / ``not_empty``: omit."
        ),
    )
    # Used only when op == "between"
    from_: str | None = Field(None, alias="from", description="ISO-8601 start datetime (op=between).")
    to: str | None = Field(None, description="ISO-8601 end datetime (op=between).")

    model_config = {"populate_by_name": True}

    @model_validator(mode="after")
    def _check_between(self) -> "FilterRule":
        if self.op == "between" and not (self.from_ and self.to):
            raise ValueError("op='between' requires both 'from' and 'to' fields")
        return self


class FiltersConfig(BaseModel):
    """Collection of filter rules combined with a logical mode."""

    mode: Literal["and", "or"] = Field("and", description="How rules are combined.")
    rules: list[FilterRule] = Field(default_factory=list)


# ── Masking ───────────────────────────────────────────────────────────────────

MaskingAction = Literal["remove", "hash", "redact", "keep"]


class MaskingRule(BaseModel):
    """Field-level masking override."""

    field: str = Field(..., description="Dot-notation field path.")
    action: MaskingAction = Field("remove", description="What to do with the field.")


class MaskingConfig(BaseModel):
    """Controls which fields are included in the export.

    When ``default_policy`` is ``deny``, only fields listed in ``allow_fields``
    are kept; individual ``rules`` can further override per-field behaviour.
    """

    default_policy: Literal["allow", "deny"] = Field(
        "allow",
        description=(
            "``allow`` — export all fields unless explicitly blocked. "
            "``deny``  — export only fields listed in ``allow_fields``."
        ),
    )
    allow_fields: list[str] = Field(
        default_factory=list,
        description="Dot-notation paths to always include (relevant when default_policy=deny).",
    )
    rules: list[MaskingRule] = Field(default_factory=list)


# ── Sampling ──────────────────────────────────────────────────────────────────

SamplingStrategy = Literal[
    "random",
    "high_cost",
    "latency_spike",
    "long_trace",
    "failure",
    "user_dissatisfaction",
    "business_critical",
    "prompt_version_change",
    "low_confidence",
    "weird_tool_sequences",
    "hybrid",
]


class SamplingRule(BaseModel):
    """One sampling strategy with its inclusion rate."""

    type: SamplingStrategy = Field(..., description="Which sampling strategy to apply.")
    rate: float = Field(
        ...,
        ge=0.0,
        le=1.0,
        description="Fraction of qualifying traces to include (0.0 – 1.0).",
    )
    field: str | None = Field(
        None,
        description="Source field used by the strategy (e.g. ``cost_usd`` for high_cost).",
    )
    params: dict[str, Any] = Field(
        default_factory=dict,
        description="Strategy-specific parameters (e.g. ``percentile``, ``threshold``).",
    )


class SamplingConfig(BaseModel):
    """Sampling configuration applied before the export."""

    strategy: SamplingStrategy = Field(
        "random",
        description=(
            "Top-level strategy label. Use ``hybrid`` when multiple rules are defined."
        ),
    )
    strict_schema: bool = Field(
        False,
        description="Drop traces whose schema does not match ``schema_snapshot``.",
    )
    max_traces: int | None = Field(
        None,
        ge=1,
        description="Hard cap on the total number of traces exported.",
    )
    rules: list[SamplingRule] = Field(default_factory=list)


# ── Asset Resolution ──────────────────────────────────────────────────────────

class AssetSource(BaseModel):
    """A field that may contain asset references."""

    field: str = Field(..., description="Dot-notation field path that holds asset references.")


class AssetFilter(BaseModel):
    """A rejection predicate evaluated against each discovered asset."""

    reject_if: str = Field(
        ...,
        description=(
            "Expression string, e.g. ``asset.size_mb > 20`` or "
            "``asset.mime_type not_in [image/png, image/jpeg]``."
        ),
    )


class AssetResolutionConfig(BaseModel):
    """Configuration for finding and exporting referenced binary assets."""

    enabled: bool = Field(False)
    sources: list[AssetSource] = Field(
        default_factory=list,
        description="Fields to scan for asset references.",
    )
    check_availability: bool = Field(
        False,
        description="HEAD-check each asset URL before including it.",
    )
    fetch_mode: Literal["metadata_only", "full"] = Field(
        "metadata_only",
        description=(
            "``metadata_only`` — record URL/size/mime, do not download bytes. "
            "``full`` — download asset content into the export dataset."
        ),
    )
    filters: list[AssetFilter] = Field(default_factory=list)
    deduplicate_by: list[str] = Field(
        default_factory=list,
        description="Asset attributes used to deduplicate (e.g. ``asset.sha256``).",
    )


# ── Top-level ─────────────────────────────────────────────────────────────────

class JobConfig(BaseModel):
    """Top-level schema for a dataimporter export-job configuration YAML.

    All sections are optional — omit those not relevant to your export.
    """

    ingestion: IngestionConfig | None = None
    filters: FiltersConfig | None = None
    masking: MaskingConfig | None = None
    sampling: SamplingConfig | None = None
    asset_resolution: AssetResolutionConfig | None = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def parse_job_config(yaml_text: str) -> JobConfig:
    """Parse and validate a YAML string; raises ``ValueError`` on invalid input."""
    raw = yaml.safe_load(io.StringIO(yaml_text))
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ValueError("Config must be a YAML mapping")
    return JobConfig.model_validate(raw)


def dump_job_config(cfg: JobConfig) -> str:
    """Serialise a ``JobConfig`` to a canonical YAML string."""
    raw = cfg.model_dump(exclude_none=True, by_alias=True)
    return yaml.dump(raw, sort_keys=False, allow_unicode=True, default_flow_style=False)
