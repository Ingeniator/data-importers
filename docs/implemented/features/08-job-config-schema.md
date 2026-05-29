# Feature: Job Config YAML Schema + Validation

## One-liner
Pydantic schema for a self-contained export-job configuration YAML, with JSON Schema introspection and server-side validation endpoints.

## Problem
Users need a machine-readable, editor-friendly format to define export jobs (datasource, filters, masking, sampling, asset resolution) that can be version-controlled, validated in CI, and downloaded from the UI.

## Implementation
- `src/dataimporter/job_config.py` — full `JobConfig` Pydantic model with sub-models for each section.
- `src/dataimporter/routes/config.py` — two endpoints:
  - `GET /api/public/config/schema` — returns `JobConfig.model_json_schema()` for editor autocompletion.
  - `POST /api/public/config/validate` — parses + validates a YAML string, returns errors or the parsed object.
- `parse_job_config(yaml_text)` and `dump_job_config(cfg)` helpers.

```python
class JobConfig(BaseModel):
    ingestion:        IngestionConfig | None        # datasource + type
    filters:          FiltersConfig | None          # field rules, and/or mode
    masking:          MaskingConfig | None          # allow/deny field policy
    sampling:         SamplingConfig | None         # strategy + rules + max_traces
    asset_resolution: AssetResolutionConfig | None  # fetch mode, filters, dedup
```

- `FiltersConfig` supports ops: `equals`, `not_equals`, `contains`, `not_contains`, `starts_with`, `greater_than`, `less_than`, `greater_than_or_equal`, `less_than_or_equal`, `is_empty`, `not_empty`, `last`, `between`.
- `MaskingConfig`: `default_policy` (allow/deny) + `allow_fields` list + per-field `MaskingRule` overrides.
- `AssetResolutionConfig`: `enabled`, `sources`, `fetch_mode` (metadata_only/full), `filters`, `deduplicate_by`.

## Scope
- **In**: Schema definition; JSON Schema export; YAML parse + validate; `configure.example.yaml` + `configure.schema.json` generated artifacts.
- **Out**: Masking and asset resolution **execution** in the import pipeline (schema exists, wiring is planned).

## Known gaps
- `MaskingConfig` and `AssetResolutionConfig` are fully modelled but not yet wired into `importer.py` — they validate but have no runtime effect on imports.
