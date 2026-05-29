"""DuckDB-based full-text search over S3 JSONL log files."""

from __future__ import annotations

import structlog

from dataimporter.config import Datasource
from dataimporter.duckdb import apply_s3_settings, make_connection

logger = structlog.get_logger(__name__)


def search_logs(
    keys: list[str],
    query: str,
    ds: Datasource,
    limit: int = 100,
) -> list[dict]:
    """Search inside JSONL files on S3 using DuckDB."""
    if not keys:
        return []

    urls = [f"s3://{ds.bucket}/{k}" for k in keys]
    conn = make_connection(ds)
    try:
        apply_s3_settings(conn, ds)
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
