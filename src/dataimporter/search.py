"""DuckDB-based full-text search over S3 JSONL log files."""

from __future__ import annotations

import glob as _glob

import duckdb
import duckdb_extension_httpfs
import structlog

from dataimporter.config import Datasource

logger = structlog.get_logger(__name__)

# Pre-locate the httpfs extension binary from the pip package
_HTTPFS_EXT = _glob.glob(str(duckdb_extension_httpfs.__path__[0]) + "/**/httpfs.duckdb_extension", recursive=True)[0]
_httpfs_installed = False


def search_logs(
    keys: list[str],
    query: str,
    ds: Datasource,
    limit: int = 100,
) -> list[dict]:
    """Search inside JSONL files on S3 using DuckDB."""
    if not keys:
        return []

    endpoint = ds.endpoint or ""
    use_ssl = endpoint.startswith("https://")
    endpoint_host = endpoint.replace("https://", "").replace("http://", "")

    urls = [f"s3://{ds.bucket}/{k}" for k in keys]

    import os
    global _httpfs_installed
    os.makedirs(ds.duckdb_temp_dir, exist_ok=True)
    os.environ.setdefault("HOME", ds.duckdb_temp_dir)
    conn = duckdb.connect(":memory:", config={
        "temp_directory": ds.duckdb_temp_dir,
        "home_directory": ds.duckdb_temp_dir,
    })
    try:
        if not _httpfs_installed:
            conn.install_extension(_HTTPFS_EXT, force_install=True)
            _httpfs_installed = True
        conn.load_extension("httpfs")
        conn.execute(f"SET s3_endpoint = '{endpoint_host}';")
        conn.execute(f"SET s3_access_key_id = '{ds.access_key_id}';")
        conn.execute(f"SET s3_secret_access_key = '{ds.secret_access_key}';")
        conn.execute(f"SET s3_region = '{ds.region}';")
        conn.execute(f"SET s3_use_ssl = {'true' if use_ssl else 'false'};")
        duckdb_url_style = "vhost" if ds.addressing_style == "virtual" else "path"
        conn.execute(f"SET s3_url_style = '{duckdb_url_style}';")

        files_list = ", ".join(f"'{u}'" for u in urls)

        if query and query != "*":
            sql = f"""
                SELECT *
                FROM read_json_auto([{files_list}],
                     format='newline_delimited',
                     ignore_errors=true,
                     union_by_name=true)
                WHERE to_json(body) ILIKE $1
                   OR CAST(id AS VARCHAR) ILIKE $1
                   OR CAST(type AS VARCHAR) ILIKE $1
                LIMIT {min(limit, 500)}
            """
            result = conn.execute(sql, [f"%{query}%"])
        else:
            sql = f"""
                SELECT *
                FROM read_json_auto([{files_list}],
                     format='newline_delimited',
                     ignore_errors=true,
                     union_by_name=true)
                LIMIT {min(limit, 500)}
            """
            result = conn.execute(sql)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        return [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        logger.error("duckdb_search_failed", error=str(e))
        return []
    finally:
        conn.close()
