from __future__ import annotations

from prometheus_client import Counter, Histogram

S3_LIST_SECONDS = Histogram(
    "dataimporter_s3_list_seconds",
    "S3 list latency in seconds",
)

S3_LIST_ERRORS = Counter(
    "dataimporter_s3_list_errors_total",
    "S3 list failures",
)

SEARCH_SECONDS = Histogram(
    "dataimporter_search_seconds",
    "Search latency in seconds",
)

SEARCH_ERRORS = Counter(
    "dataimporter_search_errors_total",
    "Search failures",
)

IMPORT_FILES = Counter(
    "dataimporter_import_files_total",
    "Files processed per import operation",
    ["datasource", "target", "status"],  # status: success | failed
)

IMPORT_BYTES = Counter(
    "dataimporter_import_bytes_total",
    "Bytes successfully uploaded to dataset service",
    ["datasource", "target"],
)

IMPORT_SECONDS = Histogram(
    "dataimporter_import_seconds",
    "Total import operation duration in seconds",
    ["datasource", "target"],
    buckets=[1, 5, 15, 30, 60, 120, 300, 600],
)
