"""Shared DuckDB connection utilities for S3 JSONL access."""
from __future__ import annotations

import glob as _glob
import os
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

try:
    import duckdb_extension_httpfs
    _HTTPFS_EXT: str = _glob.glob(
        str(duckdb_extension_httpfs.__path__[0]) + "/**/httpfs.duckdb_extension",
        recursive=True,
    )[0]
    _DUCKDB_AVAILABLE = True
except Exception:
    _DUCKDB_AVAILABLE = False
    _HTTPFS_EXT = ""

_httpfs_installed = False


def make_connection(ds: Any) -> Any:
    """Create a fully initialised DuckDB in-memory connection with httpfs loaded."""
    global _httpfs_installed
    import duckdb
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
    return conn


def apply_s3_settings(conn: Any, ds: Any) -> None:
    """(Re-)apply S3 credentials and endpoint on an existing connection."""
    endpoint = (ds.endpoint or "").replace("https://", "").replace("http://", "")
    use_ssl = (ds.endpoint or "").startswith("https://")
    url_style = "vhost" if ds.addressing_style == "virtual" else "path"
    conn.execute(f"SET s3_endpoint = '{endpoint}';")
    conn.execute(f"SET s3_access_key_id = '{ds.access_key_id}';")
    conn.execute(f"SET s3_secret_access_key = '{ds.secret_access_key}';")
    conn.execute(f"SET s3_region = '{ds.region}';")
    conn.execute(f"SET s3_use_ssl = {'true' if use_ssl else 'false'};")
    conn.execute(f"SET s3_url_style = '{url_style}';")
