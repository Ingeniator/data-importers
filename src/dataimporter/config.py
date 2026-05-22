from __future__ import annotations

import os
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path

import structlog
import yaml

logger = structlog.get_logger(__name__)


def _find_config() -> Path:
    if env := os.environ.get("DATAIMPORTER_CONFIG"):
        return Path(env)
    src_relative = Path(__file__).resolve().parents[2] / "config.yaml"
    if src_relative.exists():
        return src_relative
    return Path("config.yaml")


CONFIG_PATH = _find_config()
VAULT_SECRETS_PATH = os.environ.get("VAULT_SECRETS_PATH", "/vault/secrets/env")


def _load_vault_secrets(path: str | Path) -> dict[str, str]:
    """Load secrets from a vault sidecar file.

    Supported formats (auto-detected):
        KEY=value
        export KEY=value
        KEY: value
    """
    p = Path(path)
    if not p.exists():
        return {}
    secrets = {}
    for line in p.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:]
        if ": " in line:
            key, _, value = line.partition(": ")
        elif "=" in line:
            key, _, value = line.partition("=")
        else:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        secrets[key.strip()] = value
    return secrets


def _resolve_vault_refs(text: str, secrets: dict[str, str]) -> str:
    """Replace vault:KEY references with values from the vault secrets file."""
    for key, value in secrets.items():
        text = text.replace(f"vault:{key}", value)
    return text


@dataclass(frozen=True)
class Datasource:
    """A named datasource — analogous to a Grafana datasource."""
    name: str
    type: str  # "s3", "clickhouse", "trino", "langfuse"
    # S3 fields (used when type == "s3")
    bucket: str = ""
    region: str = "us-east-1"
    endpoint: str | None = None
    access_key_id: str = ""
    secret_access_key: str = ""
    public_endpoint: str | None = None
    key_prefix: str = ""
    addressing_style: str = "virtual"
    presign_expiry: int = 3600
    # ClickHouse fields (used when type == "clickhouse")
    url: str = ""
    database: str = "default"
    table: str = "llogr_events"
    user: str = "default"
    password: str = ""
    # Trino fields (used when type == "trino")
    catalog: str = ""
    schema_name: str = ""
    # DuckDB search over S3 files
    duckdb_temp_dir: str = "/tmp/duckdb_temp"


@dataclass(frozen=True)
class Connection:
    """An allowlisted connection template — users provide only credentials."""
    type: str  # "langfuse", "s3", "clickhouse", "trino"  
    url: str   # langfuse base URL or S3 endpoint URL
    label: str = ""
    public_key: str = ""   # langfuse public key or S3 access key id
    secret_key: str = ""   # langfuse secret key or S3 secret access key
    # S3-specific defaults (users may override bucket and key_prefix)
    bucket: str = ""
    region: str = "us-east-1"
    addressing_style: str = "path"
    key_prefix: str = ""


@dataclass(frozen=True)
class ServerConfig:
    root_path: str = ""
    host: str = "0.0.0.0"
    port: int = 5001
    workers: int = 1
    timeout_keep_alive: int = 65
    debug: bool = False
    silence_probes: bool = True
    hide_auth_inputs: bool = False
    redis_url: str = ""


@dataclass(frozen=True)
class DatasetTarget:
    """A remote dataset service to import files into."""
    name: str
    base_url: str
    token_url: str
    client_id: str
    client_secret: str
    default_access: str = "organization"
    default_dataset_type: str = "DATASET"
    # seconds; 300 comfortably covers 100 MB at ~3 MB/s
    upload_timeout: int = 300


@dataclass(frozen=True)
class Settings:
    datasources: tuple[Datasource, ...] = ()
    connections: tuple[Connection, ...] = ()
    targets: tuple[DatasetTarget, ...] = ()
    server: ServerConfig = ServerConfig()

    def get_datasource(self, name: str) -> Datasource | None:
        for ds in self.datasources:
            if ds.name == name:
                return ds
        return None

    def get_target(self, name: str) -> DatasetTarget | None:
        for t in self.targets:
            if t.name == name:
                return t
        return None

    @property
    def default_datasource(self) -> Datasource | None:
        return self.datasources[0] if self.datasources else None


def _parse_datasource(raw: dict) -> Datasource:
    known_fields = {f.name for f in Datasource.__dataclass_fields__.values()}
    filtered = {k: v for k, v in raw.items() if k in known_fields}
    return Datasource(**filtered)


def _parse_connection(raw: dict) -> Connection:
    known_fields = {f.name for f in Connection.__dataclass_fields__.values()}
    filtered = {k: v for k, v in raw.items() if k in known_fields}
    return Connection(**filtered)


def _parse_target(raw: dict) -> DatasetTarget:
    known_fields = {f.name for f in DatasetTarget.__dataclass_fields__.values()}
    filtered = {k: v for k, v in raw.items() if k in known_fields}
    return DatasetTarget(**filtered)


def load_config(path: str | Path) -> Settings:
    text = Path(path).read_text()
    text = _resolve_vault_refs(text, _load_vault_secrets(VAULT_SECRETS_PATH))
    text = os.path.expandvars(text)
    raw = yaml.safe_load(text)

    datasources_raw = raw.get("datasources", [])
    datasources = tuple(_parse_datasource(ds) for ds in datasources_raw)

    connections_raw = raw.get("connections", [])
    connections = tuple(_parse_connection(c) for c in connections_raw)

    targets_raw = raw.get("targets", [])
    targets = tuple(_parse_target(t) for t in targets_raw)

    return Settings(
        datasources=datasources,
        connections=connections,
        targets=targets,
        server=ServerConfig(**raw.get("server", {})),
    )


@lru_cache
def get_settings() -> Settings:
    return load_config(CONFIG_PATH)
