# Feature: Configuration System

## One-liner
YAML-based config loaded once at startup into an immutable frozen-dataclass singleton, with vault sidecar secret injection and `$ENV_VAR` expansion.

## Problem
Credentials must not live in environment variables alone (no multiline support, hard to rotate) or in code. A YAML file is human-readable and version-controllable, but secrets still need to be injected at runtime from a vault sidecar without rewriting the file.

## Implementation
- `src/dataimporter/config.py` — `load_config()` + `@lru_cache get_settings()`.
- Config file location: `DATAIMPORTER_CONFIG` env var → `../../config.yaml` (relative to package) → `./config.yaml`.
- Secret injection pipeline (applied to raw YAML text before parsing):
  1. `_load_vault_secrets(VAULT_SECRETS_PATH)` — reads `KEY=value` / `export KEY=value` / `KEY: value` from vault sidecar file (default `/vault/secrets/env`).
  2. `_resolve_vault_refs(text, secrets)` — replaces `vault:KEY` tokens with sidecar values.
  3. `os.path.expandvars(text)` — expands `$ENV_VAR` references.
- Parsed into frozen dataclasses: `Datasource`, `Connection`, `DatasetTarget`, `ServerConfig`, `Settings`.
- `_check_unknown_keys()` validates each entity against known dataclass fields — raises `ValueError` with a helpful message listing valid keys.
- `get_settings()` is `@lru_cache` — config is loaded once per process; routes call it via `Depends(get_settings)`.

```python
@dataclass(frozen=True)
class Settings:
    datasources: tuple[Datasource, ...]
    connections: tuple[Connection, ...]
    targets:     tuple[DatasetTarget, ...]
    server:      ServerConfig
```

## Scope
- **In**: YAML loading; vault:KEY injection; $ENV_VAR expansion; frozen dataclass models; unknown-key validation; `@lru_cache` singleton; `DATAIMPORTER_CONFIG` override.
- **Out**: Hot-reload without restart; per-datasource secret rotation at runtime; config encryption at rest.

## Known gaps
- `get_settings()` is cached for the process lifetime — config changes require a restart.
- No config schema validation at startup beyond unknown-key checks (e.g. `type: "clickhouse"` with missing `url` fails only at first use, not at boot).
- Vault sidecar path is a global constant; no per-datasource vault path support.
