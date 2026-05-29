# Feature: Browser UI (SPA)

## One-liner
Vanilla-JS SPA served at `/` — browse S3 files, search across all backends, configure connections, run imports with sampling, and track job progress.

## Problem
All backend capabilities (multi-backend search, proxy connections, sampling, import) need a usable interface without a separate frontend build pipeline or external dependencies.

## Implementation

### Source layout
| File | Role |
|---|---|
| `src/dataimporter/templates/browser.html` | Page shell: nav, layout, all functional HTML with IDs. ~550 lines. |
| `src/dataimporter/static/browser.css` | Styles for JS-generated dynamic elements (tabs, modals, filter rules, progress bars, schema bar, column picker). ~300 lines. |
| `src/dataimporter/static/browser.js` | All application logic. ~2 700 lines. |

Static files are mounted at `/static` via `fastapi.staticfiles.StaticFiles` in `main.py`.

`browser.html` is served by `src/dataimporter/routes/ui.py` via Jinja2. `base_path` is injected as `data-base` on `<html>` for reverse-proxy `root_path` compatibility; `browser.js` reads it via `document.documentElement.dataset.base`.

### Design system
The UI uses Tailwind CSS (CDN, `forms` + `container-queries` plugins) with a Material Design 3–inspired token palette and Google Fonts (Inter / Noto Serif / Public Sans). Icons are Material Symbols Outlined. `browser.css` handles only components whose HTML is generated at runtime by `browser.js`; static layout elements use Tailwind utility classes directly in the HTML.

### Page structure
```
Top nav bar          (Data Import / Jobs / Billing)
Auth credentials bar (#authCard — pk/sk inputs)
Datasource tabs      (#dsTabs — JS-populated)
─────────────────────────────────────────────────
Refine Dataset card  (#filtersCard)
  ├─ Schema bar      (#schemaBar)
  ├─ Column picker   (#colPicker)
  ├─ Time range      (#timePicker / #timeDropdown)
  ├─ Search input    (#f_query)
  └─ Filter builder  (#filter_panel)
Status line          (#status)
Data Preview table   (#results / #thead / #tbody)
─────────────────────────────────────────────────
Export Card          (#exportCard)
  ├─ Sampling        (#samp_panel)
  ├─ Data Masking    (#mask_rules_section)
  ├─ Export & Dest   (#dest_section, #yamlFileInput)
  └─ Scheduling &
     Automation      (#schedule_cron, #webhook_url)
─────────────────────────────────────────────────
Modal overlay        (#modal — JS-rendered)
```

- UI bootstrap: `GET /api/public/ui-config` fetches datasources, connections, targets, and `hide_auth_inputs` flag on load.
- Credentials for user connections stored in `localStorage` — never sent to the server except per-request in the proxy payload.

### Key UI flows
| Flow | Relevant endpoints |
|---|---|
| S3 browse | `GET /logs`, `GET /logs/list`, `POST /logs/urls` |
| Multi-backend search | `GET /logs/search` |
| Proxy search | `POST /proxy/search`, `POST /proxy/ping` |
| Schema discovery | `GET /datasource/sample` |
| Import with sampling | `POST /export/dataset`, `POST /export/dataset/events` |
| Job progress | `GET /export/status/{job_id}` (polls every 2 s) |
| Column picker | client-side, drives `col_mask` sent in export payload |
| Config YAML download | client-side generation + `POST /config/validate` |

## Scope
- **In**: All flows above; column picker; sampling config UI; import modal; connection manager; progress bar; scheduling & webhook config.
- **Out**: Server-side rendering beyond `base_path` injection; mobile layout; accessibility audit; dark mode.

## Known gaps
- `data-testid` attributes cover all **static** HTML elements (41 attributes — nav, time picker, search, filter builder, results table, sampling, masking, destination, yaml upload). Elements **generated at runtime by `browser.js`** (result rows `#result-row-{index}`, filter rule chips `#filter-rule-{index}`, sampling rule rows `#sampling-rule-{index}`, datasource cards `#ds-card-{name}`, job cards `#job-card-{id}`, preview modal, export modal, connection form) still lack testids — they must be added in the JS HTML-string templates in `browser.js`.
- `col_mask` and `asset_resolve: true` are sent in the export payload but not yet executed server-side (see [Job Config Schema known gaps](08-job-config-schema.md)).
- Tailwind is loaded from CDN — adds ~300 ms cold-load latency; acceptable for an internal tool, but a build step would eliminate it.
