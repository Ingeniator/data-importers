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
