"""Sampling engine — filter traces by strategy before import."""
from __future__ import annotations

import glob as _glob
import math
import os
import random as _random
from typing import Any

import structlog
from pydantic import BaseModel

logger = structlog.get_logger(__name__)


class SamplingRule(BaseModel):
    strategy: str  # random | high_cost | latency_spike | long_trace | failure |
                   # user_dissatisfaction | business_critical | prompt_version_change |
                   # low_confidence | weird_tool_sequences
    rate: float    # 0-100 — % of qualifying traces to include
    field: str | None = None
    params: dict = {}


# ── field helpers ────────────────────────────────────────────────────────────

def _get(obj: dict, path: str) -> Any:
    """Dot-notation nested field access."""
    parts = path.split(".", 1)
    val = obj.get(parts[0])
    if len(parts) == 2 and isinstance(val, dict):
        return _get(val, parts[1])
    return val


def _percentile(values: list[float], p: float) -> float:
    sv = sorted(v for v in values if v is not None)
    if not sv:
        return 0.0
    idx = (p / 100) * (len(sv) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sv) - 1)
    return sv[lo] + (sv[hi] - sv[lo]) * (idx - lo)


# ── per-strategy pool filters ────────────────────────────────────────────────

def _filter_pool(traces: list[dict], rule: SamplingRule) -> list[dict]:
    s = rule.strategy
    f = rule.field
    p = rule.params

    if s == "random":
        return list(traces)

    if not f:
        return []

    if s in ("high_cost", "latency_spike"):
        pct = float(p.get("percentile", 95))
        nums = [float(_get(t, f)) for t in traces if _get(t, f) is not None]
        threshold = _percentile(nums, pct)
        return [t for t in traces if float(_get(t, f) or 0) > threshold]

    if s == "long_trace":
        threshold = float(p.get("threshold", 10))
        return [t for t in traces if float(_get(t, f) or 0) > threshold]

    if s == "failure":
        fail_vals = {v.strip().lower() for v in str(p.get("values", "error,failed,failure")).split(",")}
        def _is_fail(t: dict) -> bool:
            v = _get(t, f)
            if v is None:
                return False
            if isinstance(v, bool):
                return v
            if isinstance(v, (int, float)):
                return v >= 400
            return str(v).lower() in fail_vals
        return [t for t in traces if _is_fail(t)]

    if s == "user_dissatisfaction":
        threshold = float(p.get("threshold", 0))
        neg = {v.strip().lower() for v in str(p.get("thumbsdown_tags", "thumbsdown,dislike,negative")).split(",")}
        def _is_dissat(t: dict) -> bool:
            v = _get(t, f)
            if v is None:
                return False
            if isinstance(v, (int, float)):
                return float(v) < threshold
            if isinstance(v, list):
                return any(str(i).lower() in neg for i in v)
            return str(v).lower() in neg
        return [t for t in traces if _is_dissat(t)]

    if s == "business_critical":
        match_type = p.get("match_type", "contains")
        value = str(p.get("value", "")).lower()
        def _is_critical(t: dict) -> bool:
            v = _get(t, f)
            if v is None:
                return False
            if isinstance(v, list):
                items = [str(i).lower() for i in v]
                return any((value in i) if match_type == "contains" else (value == i) for i in items)
            sv = str(v).lower()
            return (value in sv) if match_type == "contains" else (value == sv)
        return [t for t in traces if _is_critical(t)]

    if s == "prompt_version_change":
        baseline = str(p.get("baseline", ""))
        return [t for t in traces if str(_get(t, f) or "") != baseline]

    if s == "low_confidence":
        threshold = float(p.get("threshold", 0.5))
        return [t for t in traces if float(_get(t, f) or 1.0) < threshold]

    if s == "weird_tool_sequences":
        max_repeat = int(p.get("max_repeat", 999_999))
        min_total = int(p.get("min_total_calls", 999_999))
        unexpected = {v.strip().lower() for v in str(p.get("unexpected_tools", "")).split(",") if v.strip()}
        def _is_weird(t: dict) -> bool:
            v = _get(t, f)
            if not isinstance(v, list):
                return False
            names = [str(i.get("name", i) if isinstance(i, dict) else i).lower() for i in v]
            if len(names) >= min_total:
                return True
            if unexpected and any(n in unexpected for n in names):
                return True
            counts: dict[str, int] = {}
            for n in names:
                counts[n] = counts.get(n, 0) + 1
            return any(c > max_repeat for c in counts.values())
        return [t for t in traces if _is_weird(t)]

    logger.warning("unknown_sampling_strategy", strategy=s)
    return []


def _sample(pool: list[dict], rate: float, seed: int | None) -> list[dict]:
    if not pool or rate <= 0:
        return []
    if rate >= 100:
        return list(pool)
    k = max(1, math.ceil(len(pool) * rate / 100))
    rng = _random.Random(seed)
    return rng.sample(pool, min(k, len(pool)))


def _trace_id(t: dict) -> str:
    return str(t.get("trace_id") or t.get("id") or t.get("_key") or id(t))


# ── main entry point ─────────────────────────────────────────────────────────

def apply_sampling(
    traces: list[dict],
    rules: list[SamplingRule],
    strict_schema: bool = False,
    schema_snapshot: dict[str, str] | None = None,
    max_traces: int | None = None,
) -> tuple[list[dict], str | None]:
    """Apply sampling rules to a list of traces. Returns (sampled, warning_or_None)."""
    if not rules:
        return traces, None

    if strict_schema and schema_snapshot:
        required = set(schema_snapshot.keys())
        traces = [t for t in traces if required.issubset(t.keys())]

    seen: set[str] = set()
    result: list[dict] = []
    for rule in rules:
        pool = _filter_pool(traces, rule)
        seed = rule.params.get("seed")
        for t in _sample(pool, rule.rate, seed):
            tid = _trace_id(t)
            if tid not in seen:
                seen.add(tid)
                result.append(t)

    if max_traces is not None and len(result) > max_traces:
        result = _random.sample(result, max_traces)

    warning: str | None = None
    if not result:
        warning = "Sampling config produced 0 results — no traces matched the configured strategies."

    return result, warning


# ── S3 / DuckDB integration ──────────────────────────────────────────────────

try:
    import duckdb
    import duckdb_extension_httpfs
    _HTTPFS_EXT = _glob.glob(
        str(duckdb_extension_httpfs.__path__[0]) + "/**/httpfs.duckdb_extension",
        recursive=True,
    )[0]
    _DUCKDB_AVAILABLE = True
except Exception:
    _DUCKDB_AVAILABLE = False

_httpfs_installed = False


def _duckdb_s3_conn(ds: Any) -> Any:
    global _httpfs_installed
    endpoint = (ds.endpoint or "").replace("https://", "").replace("http://", "")
    use_ssl = (ds.endpoint or "").startswith("https://")
    os.makedirs(ds.duckdb_temp_dir, exist_ok=True)
    os.environ.setdefault("HOME", ds.duckdb_temp_dir)
    conn = duckdb.connect(":memory:", config={
        "temp_directory": ds.duckdb_temp_dir,
        "home_directory": ds.duckdb_temp_dir,
    })
    if not _httpfs_installed:
        conn.install_extension(_HTTPFS_EXT, force_install=True)
        _httpfs_installed = True
    conn.load_extension("httpfs")
    conn.execute(f"SET s3_endpoint = '{endpoint}';")
    conn.execute(f"SET s3_access_key_id = '{ds.access_key_id}';")
    conn.execute(f"SET s3_secret_access_key = '{ds.secret_access_key}';")
    conn.execute(f"SET s3_region = '{ds.region}';")
    conn.execute(f"SET s3_use_ssl = {'true' if use_ssl else 'false'};")
    url_style = "vhost" if ds.addressing_style == "virtual" else "path"
    conn.execute(f"SET s3_url_style = '{url_style}';")
    return conn


def read_s3_traces_for_sampling(keys: list[str], ds: Any) -> list[dict]:
    """Read one representative row per S3 key via DuckDB. Each row gains a _key field."""
    if not keys or not _DUCKDB_AVAILABLE:
        return [{"_key": k} for k in keys]

    urls = [f"s3://{ds.bucket}/{k}" for k in keys]
    url_to_key = {u: k for u, k in zip(urls, keys)}
    files_list = ", ".join(f"'{u}'" for u in urls)

    conn = _duckdb_s3_conn(ds)
    try:
        sql = f"""
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT *, filename,
                       ROW_NUMBER() OVER (PARTITION BY filename) AS rn
                FROM read_json_auto(
                    [{files_list}],
                    format='newline_delimited',
                    ignore_errors=true,
                    union_by_name=true,
                    filename=true
                )
            )
            WHERE rn = 1
        """
        result = conn.execute(sql)
        columns = [d[0] for d in result.description]
        rows = result.fetchall()
        traces = []
        for row in rows:
            d = dict(zip(columns, row))
            filename = d.pop("filename", "")
            d["_key"] = url_to_key.get(filename, filename.split(f"{ds.bucket}/", 1)[-1])
            traces.append(d)
        return traces
    except Exception as e:
        logger.error("s3_sampling_read_failed", error=str(e))
        return [{"_key": k} for k in keys]
    finally:
        conn.close()


def apply_sampling_s3(
    keys: list[str],
    rules: list[SamplingRule],
    ds: Any,
    strict_schema: bool = False,
    schema_snapshot: dict[str, str] | None = None,
    max_traces: int | None = None,
) -> tuple[list[str], str | None]:
    """Apply sampling rules to S3 keys. Returns (sampled_keys, warning)."""
    if not rules:
        return keys, None

    needs_fields = any(r.strategy != "random" for r in rules)
    if needs_fields:
        traces = read_s3_traces_for_sampling(keys, ds)
    else:
        traces = [{"_key": k} for k in keys]

    sampled, warning = apply_sampling(traces, rules, strict_schema, schema_snapshot, max_traces)
    sampled_keys = [t["_key"] for t in sampled if "_key" in t]
    return sampled_keys, warning
