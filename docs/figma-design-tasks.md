# Figma Design Tasks — dataimporter UI

> Component IDs use `#test-id` notation — must be added as `data-testid="test-id"` in the final HTML for Playwright tests (Constitution §V).

---

## App Structure

Three pages with a persistent top-level navigation:

```
┌─────────────────────────────────────────────┐
│  [ Datasources ]  [ Connections ]  [ Jobs ] │  ← top nav
├─────────────────────────────────────────────┤
│                                             │
│               Page content                 │
│                                             │
└─────────────────────────────────────────────┘
```

| Component | `#test-id` |
|---|---|
| Top nav bar | `#top-nav` |
| Datasources nav link | `#nav-datasources` |
| Connections nav link | `#nav-connections` |
| Jobs nav link | `#nav-jobs` |
| Active job count badge | `#nav-jobs-badge` |

---

## Page 1 — Datasources

User browses and searches traces across server-configured backends.

### P1-S1: Datasource List

**Goal**: User sees which backends are available and their health status.

| State | Description |
|---|---|
| Loading | Skeleton cards while `GET /api/public/ui-config` loads |
| Loaded | Grid of datasource cards: name, type badge, ping status |
| Datasource degraded | Card shows warning icon + error tooltip |
| No datasources | Empty state — "No datasources configured by your admin" |

| Component | `#test-id` |
|---|---|
| Datasource card | `#ds-card-{name}` |
| Type badge | `#ds-card-{name}-type` |
| Health indicator | `#ds-card-{name}-health` |
| Open datasource button | `#ds-card-{name}-open-btn` |

---

### P1-S2: Browse & Search View

**Goal**: User explores files or searches traces within a selected datasource.

| State | Description |
|---|---|
| S3 datasource | Time range picker + file list table (no search input) |
| Search datasource | Search input prominent; time range optional |
| Loading results | Skeleton rows / spinner in table area |
| Results loaded | Table with rows, column picker active |
| Empty results | Inline empty state with query and range echoed |
| Error | Backend error banner above table with retry button |

| Component | `#test-id` |
|---|---|
| Datasource page header | `#ds-header` |
| Back to list link | `#ds-back-btn` |
| Search input | `#search-input` |
| Search button | `#search-btn` |
| Time range button | `#time-range-btn` |
| Time range dropdown | `#time-range-dropdown` |
| Preset range option | `#time-preset-{label}` |
| Date from input | `#time-from` |
| Date to input | `#time-to` |
| Apply range button | `#apply-time-range-btn` |
| Results table | `#results-table` |
| Result row | `#result-row-{index}` |
| Row select checkbox | `#result-row-{index}-checkbox` |
| Row preview button | `#result-row-{index}-preview-btn` |
| Select all checkbox | `#select-all-checkbox` |
| Selected count badge | `#selected-count` |
| Result count label | `#result-count` |
| Backend badge | `#backend-badge` |

---

### P1-S3: Field Filters Panel

**Goal**: User narrows results with field-level predicates.

| State | Description |
|---|---|
| Collapsed | "⚗ Add filters" button only |
| Expanded | AND/OR toggle + rule list + add row |
| Rule added | Field / op / value chip with remove button |

| Component | `#test-id` |
|---|---|
| Toggle button | `#filter-toggle-btn` |
| Filter panel | `#filter-panel` |
| AND mode button | `#filter-mode-and` |
| OR mode button | `#filter-mode-or` |
| Field selector | `#filter-field-sel` |
| Operator selector | `#filter-op-sel` |
| Value input | `#filter-val-input` |
| Add rule button | `#filter-add-btn` |
| Rule chip | `#filter-rule-{index}` |
| Remove rule button | `#filter-rule-{index}-remove-btn` |

> **Note**: Operator list should change based on field type — show unavailable ops greyed out, not hidden.

---

### P1-S4: Column Picker

**Goal**: User controls which fields appear in the table and optionally restricts export to those fields.

| State | Description |
|---|---|
| Collapsed | "⚙ Columns" button with current field count |
| Open, schema loaded | Scrollable field list, each with visibility toggle |
| Schema loading | Spinner — "detecting schema…" |
| Schema error | "Could not detect" + retry button |

| Component | `#test-id` |
|---|---|
| Column picker toggle | `#column-picker-btn` |
| Column picker panel | `#column-picker-panel` |
| Column row | `#col-row-{field-name}` |
| Column visibility toggle | `#col-row-{field-name}-toggle` |
| Reset defaults button | `#col-reset-defaults-btn` |
| Export col mask checkbox | `#export-col-mask-checkbox` |

---

### P1-S5: Event Preview

**Goal**: User inspects the full JSON of a single trace.

| Component | `#test-id` |
|---|---|
| Preview modal | `#preview-modal` |
| Preview content | `#preview-content` |
| Copy JSON button | `#preview-copy-btn` |
| Close button | `#preview-close-btn` |

---

### P1-S6: Export Modal

**Goal**: User submits selected results to the dataset service.

| State | Description |
|---|---|
| Open | Dataset name pre-filled, target / access / type selectors |
| Validation error | Inline errors under empty required fields |
| Submitting | Import button disabled + spinner |
| Submitted | Modal closes → user redirected to Jobs page (P3) |

| Component | `#test-id` |
|---|---|
| Export modal | `#export-modal` |
| Dataset name input | `#dest-dataset-name` |
| Target selector | `#dest-target` |
| Access selector | `#dest-access` |
| Dataset type selector | `#dest-dataset-type` |
| Import button | `#import-btn` |
| Cancel button | `#export-modal-cancel-btn` |

---

### P1-S7: Sampling Panel (inside Export Modal)

**Goal**: User configures trace sampling strategies before import.

| State | Description |
|---|---|
| Collapsed | "Configure Sampling" button |
| Loading schema | Spinner — schema discovery in progress |
| Schema loaded | Discovered field chips; unavailable strategies greyed out |
| Strategy added | Rate + field selector + remove button per rule |
| 100% rate warning | Inline "equivalent to no sampling" |

| Component | `#test-id` |
|---|---|
| Toggle button | `#sampling-toggle-btn` |
| Sampling panel | `#sampling-panel` |
| Schema field chip | `#schema-field-{name}` |
| Strategy selector | `#sampling-strategy-sel` |
| Add strategy button | `#sampling-add-strategy-btn` |
| Strategy rule row | `#sampling-rule-{index}` |
| Rate input | `#sampling-rule-{index}-rate` |
| Field selector | `#sampling-rule-{index}-field` |
| Remove button | `#sampling-rule-{index}-remove-btn` |
| Strict schema checkbox | `#sampling-strict-checkbox` |
| Max traces input | `#sampling-max-traces` |
| Estimated yield label | `#sampling-yield-estimate` |

---

## Page 2 — Connections

User manages their own saved connections to external backends.

### P2-S1: Connection List

**Goal**: User sees all saved connections and their health.

| State | Description |
|---|---|
| Empty | "No connections yet" + "Add connection" CTA |
| List loaded | Cards: label, type badge, URL, last tested status |
| Connection OK | Green dot, last tested timestamp |
| Connection failing | Red dot, error tooltip |
| Migrating | One-time banner: "You have connections saved in your browser — import them?" |

| Component | `#test-id` |
|---|---|
| Connection list | `#connection-list` |
| Connection card | `#conn-card-{id}` |
| Connection type badge | `#conn-card-{id}-type` |
| Connection status dot | `#conn-card-{id}-status` |
| Edit button | `#conn-card-{id}-edit-btn` |
| Delete button | `#conn-card-{id}-delete-btn` |
| Test button | `#conn-card-{id}-test-btn` |
| Add connection button | `#add-connection-btn` |
| LocalStorage import banner | `#localstorage-import-banner` |
| Import connections button | `#import-localstorage-btn` |

---

### P2-S2: Add / Edit Connection Form

**Goal**: User creates or updates a connection with credentials.

| State | Description |
|---|---|
| Add — blank | Type selector, label, URL, credential fields |
| Edit — loaded | Fields pre-filled; credentials shown as `••••••` placeholders |
| URL not allowlisted | Inline error: "This URL is not permitted by your admin" |
| Ping in progress | Spinner next to "Test" button |
| Ping success | Green "Connected" with timestamp |
| Ping failed | Red error message from backend |
| Saving | Save button disabled + spinner |
| Saved | Form closes → connection appears in list |

| Component | `#test-id` |
|---|---|
| Connection form | `#conn-form` |
| Type selector | `#conn-type-sel` |
| Label input | `#conn-label-input` |
| URL input | `#conn-url-input` |
| Access key input | `#conn-pk-input` |
| Secret key input | `#conn-sk-input` |
| Bucket input (S3) | `#conn-bucket-input` |
| Region input (S3) | `#conn-region-input` |
| Test connection button | `#conn-test-btn` |
| Connection status | `#conn-status` |
| Save button | `#conn-save-btn` |
| Cancel button | `#conn-cancel-btn` |

> **Note**: Credentials must visually indicate "stored securely on server" — not just "remember me in browser".
> Secret fields: show value on edit only via explicit "reveal" toggle, never pre-filled in plaintext.

---

## Page 3 — Jobs

User monitors running and past export jobs and can create new ones from a config file.

### P3-S1: Job List

**Goal**: User sees all their export jobs sorted by creation time.

| State | Description |
|---|---|
| Empty | "No jobs yet" + "Create job" CTA |
| List loaded | Job cards with status, datasource, target, created at |
| Running job | Progress bar within card, auto-refreshes |
| All terminal | Static list, no polling |

| Component | `#test-id` |
|---|---|
| Job list | `#job-list` |
| Job card | `#job-card-{id}` |
| Job status badge | `#job-card-{id}-status` |
| Job progress bar | `#job-card-{id}-progress` |
| Job files counter | `#job-card-{id}-files` |
| Job bytes counter | `#job-card-{id}-bytes` |
| Cancel job button | `#job-card-{id}-cancel-btn` |
| Open detail button | `#job-card-{id}-detail-btn` |
| Create job button | `#create-job-btn` |
| Active jobs badge (nav) | `#nav-jobs-badge` |

> **Note**: Cancel is a planned feature (`docs/features/01-job-cancellation.md`) — design it now, wire later.

---

### P3-S2: Job Detail

**Goal**: User inspects the full result of a completed or failed job.

| State | Description |
|---|---|
| Running | Progress bar, files done/total, bytes, cancel button |
| Complete | Summary: N files, X MB, dataset link |
| Failed | Error message + expandable list of failed files |
| Warning | Yellow banner — e.g. "Sampling produced 0 results" |

| Component | `#test-id` |
|---|---|
| Job detail panel | `#job-detail` |
| Status label | `#job-status` |
| Progress bar | `#job-progress-bar` |
| Files counter | `#job-files-count` |
| Bytes counter | `#job-bytes-count` |
| Dataset link | `#job-dataset-link` |
| Cancel button | `#job-cancel-btn` |
| Failed files list | `#job-failed-files` |
| Warning banner | `#job-warning` |

---

### P3-S3: Create Job from Config YAML

**Goal**: User uploads a YAML config file to define and submit a new export job without using the browse UI.

| State | Description |
|---|---|
| Default | Drop zone + "Browse" button + "⬇ Download example" link |
| File selected | Filename shown, "Validate" triggered automatically |
| Validating | Spinner |
| Valid | Green "Config valid" + parsed summary (datasource, sampling rules count, masking policy) |
| Invalid | Red error list with field paths (from `POST /config/validate`) |
| Submitting | Submit button disabled + spinner |
| Submitted | Redirect to new job card in the job list |

| Component | `#test-id` |
|---|---|
| YAML upload zone | `#yaml-upload-zone` |
| YAML file input | `#yaml-file-input` |
| Browse button | `#yaml-browse-btn` |
| Download example button | `#yaml-example-btn` |
| Validation status | `#yaml-validation-status` |
| Config summary | `#yaml-config-summary` |
| Submit job button | `#submit-job-btn` |
| Cancel button | `#create-job-cancel-btn` |

---

## Cross-page Notes

- **Redirect after export**: submitting the export modal on Page 1 redirects to the new job on Page 3 (not an inline progress bar).
- **Active jobs badge**: `#nav-jobs-badge` shows count of queued/running jobs; disappears when all jobs are terminal.
- **Connections on Page 1**: the datasource view should allow selecting a saved connection (Page 2) as the search source, not just server-configured datasources.
- **Responsive**: define behaviour at ≥1280 px (primary), 768–1279 px (tablet), and flag if mobile is out of scope.

---

## Design Checklist

- [ ] All interactive elements carry `data-testid` matching the IDs above
- [ ] Empty state for every list (datasource list, connection list, job list)
- [ ] Error state for every async operation
- [ ] Loading skeleton for every data-fetching step
- [ ] Credential fields never show secrets in plaintext after save
- [ ] `hide_auth_inputs=true` server flag variant of connection form
- [ ] Cancel button designed on job cards (wired later per feature 01)
- [ ] One-time localStorage migration banner on Page 2
- [ ] Navigation badge auto-updates without full page reload
