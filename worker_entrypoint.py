from arq import run_worker
from prometheus_client import start_http_server

from dataimporter.config import get_settings
from dataimporter.worker import WorkerSettings

start_http_server(get_settings().server.worker_metrics_port)
run_worker(WorkerSettings)
