from arq import run_worker
from prometheus_client import start_http_server

from dataimporter.worker import WorkerSettings

start_http_server(9101)
run_worker(WorkerSettings)
