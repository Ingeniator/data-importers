extendable connections - add custom clickhouse connection

look on process in chatgpt conversation about mask/filter/sampling/asset resolution

+filter by tymestamp and search

?import from ch ignore user selection

Current limit

  For the events path the import is bounded by what's displayed — the search endpoints cap at 500 rows (limit param, default 100 in the UI). If you need to import
  more than that in one shot, it would require a re-query-on-server approach rather than sending currentEvents.

asset resoulution
система должна владеть копией artifacts, а не зависеть от ссылок Langfuse. Иначе через retention, signed URL expiry или миграцию ты потеряешь воспроизводимость trace.

allow sampling for download on pc?

Column mask (_expColMask):
  - PC downloads — _applyColMask(events) is called in _dlZip and _dlJsonl before writing, picking only the fields currently visible in the column picker
  (_schemaCache[ds].visibleColumns). Uses dot-notation path traversal so nested fields like body.input work correctly.
  - Dataset service — events path — mask applied client-side before the events array is sent in the POST body.
  - Dataset service — S3 native path — col_mask: [...] field is included in the payload for future backend implementation.

  Asset resolution (_expAssetResolve):
  - asset_resolve: true is sent in the payload for both service export paths — ready for backend implementation.
  



S3 ingestion talbe columts hardcodes in table with results


add trino as datasource and test it 

Proxy (user-provided credentials)
