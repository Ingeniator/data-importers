const BASE = document.documentElement.dataset.base || '';
let datasources = [];      // server-configured datasources
let connections = [];       // allowlisted connection templates from server
let userConnections = [];   // user-created connections (localStorage)
let targets = [];           // dataset service targets from server config
let activeDs = null;        // current tab: {name, type, _user?: true, _connUrl?, _pk?, _sk?}
let currentEvents = [];
let currentSearchKeys = [];
let timePreset = '24h';
let customFrom = null, customTo = null;

// ── Schema / column state ──
let _schemaCache = {};      // dsName → { fields, defaultColumns, visibleColumns }
let _schemaDetecting = {};  // dsName → true (dedup guard)
let _colPickerOpen = false;
const LS_COLS_KEY = 'dataimporter_columns_v1';

// ── Client-side sort state ──
let _sortCol = null;
let _sortDir = 'asc';   // 'asc' | 'desc'

// ── Backend filter rules ──
let _filterRules = [];  // [{ field, op, value }]
let _filtersOpen = false;

// ── Time field ──
// null = not yet determined (schema pending); '' = off; 'fieldname' = use this column
let _timeField = null;

const PRESETS = {
  '15m': 15*60*1000, '1h': 60*60*1000, '4h': 4*60*60*1000,
  '24h': 24*60*60*1000, '7d': 7*24*60*60*1000, '30d': 30*24*60*60*1000,
  'all': null,
};

const LS_KEY = 'dataimporter_user_connections';

function loadUserConnections() {
  try { userConnections = JSON.parse(localStorage.getItem(LS_KEY) || '[]'); } catch { userConnections = []; }
}
function saveUserConnections() {
  localStorage.setItem(LS_KEY, JSON.stringify(userConnections));
}

// ── Time picker ──
function toggleTimeDropdown() { document.getElementById('timeDropdown').classList.toggle('open'); }
function setPreset(key, label) {
  timePreset = key; customFrom = null; customTo = null;
  document.getElementById('timeLabel').textContent = label;
  document.querySelectorAll('#timePresets div').forEach(d => d.classList.remove('active'));
  event.target.classList.add('active');
  document.getElementById('timeDropdown').classList.remove('open');
}
function applyCustomRange() {
  const from = document.getElementById('time_from').value;
  const to = document.getElementById('time_to').value;
  if (!from || !to) return;
  customFrom = new Date(from).toISOString(); customTo = new Date(to).toISOString();
  timePreset = null;
  document.getElementById('timeLabel').textContent = from.replace('T',' ') + '  \u2192  ' + to.replace('T',' ');
  document.querySelectorAll('#timePresets div').forEach(d => d.classList.remove('active'));
  document.getElementById('timeDropdown').classList.remove('open');
}
function getTimeRange() {
  if (_timeField === '') return {};  // time field disabled → no time filtering
  if (customFrom && customTo) return { start: customFrom, end: customTo };
  const ms = PRESETS[timePreset]; if (!ms) return {};
  const now = new Date();
  return { start: new Date(now - ms).toISOString(), end: now.toISOString() };
}
document.addEventListener('click', e => {
  const timePicker = document.getElementById('timePicker');
  if (!timePicker.contains(e.target)) document.getElementById('timeDropdown').classList.remove('open');

  // Close column picker when clicking outside the schema bar + picker area
  if (_colPickerOpen) {
    const bar = document.getElementById('schemaBar');
    const cp  = document.getElementById('colPicker');
    if (bar && cp && !bar.contains(e.target) && !cp.contains(e.target))
      closeColumnPicker();
  }
});

// ══════════════════════════════════════════
//  Schema persistence (localStorage)
// ══════════════════════════════════════════

function _saveColumns(dsName, cols) {
  try {
    const stored = JSON.parse(localStorage.getItem(LS_COLS_KEY) || '{}');
    stored[dsName] = cols;
    localStorage.setItem(LS_COLS_KEY, JSON.stringify(stored));
  } catch {}
}

function _loadColumns(dsName) {
  try { return JSON.parse(localStorage.getItem(LS_COLS_KEY) || '{}')[dsName] || null; }
  catch { return null; }
}

// ══════════════════════════════════════════
//  Schema helpers
// ══════════════════════════════════════════

/** Return only leaf fields — fields that have no children (dot-path descendants). */
function _leafFields(fields) {
  const keys = Object.keys(fields);
  return keys.filter(k => !keys.some(o => o !== k && o.startsWith(k + '.')));
}

/** Pick up to 6 default columns: timestamp/time first, then *_id fields, then rest.
 *  Skip fields whose example is null (proxy for "mostly empty in sample").
 */
function _computeDefaultColumns(fields) {
  const leaves = _leafFields(fields);
  const nonNull = leaves.filter(k => fields[k].example !== null);
  const pool = nonNull.length >= 2 ? nonNull : leaves;

  const tsRe = /^(timestamp|time|ts|created_at|created|date|datetime|at)$/i;
  const idRe = /^(id|trace_id|session_id|request_id|span_id|run_id|conversation_id)$/i;
  const getName = k => k.split('.').pop();

  const p1 = pool.filter(k => tsRe.test(getName(k)));
  const p2 = pool.filter(k => idRe.test(getName(k)) && !p1.includes(k));
  const rest = pool.filter(k => !p1.includes(k) && !p2.includes(k));

  return [...p1, ...p2, ...rest].slice(0, 6);
}

/** Store schema into cache, preserving any previously-saved column selection. */
function _storeSchema(dsName, fields) {
  const defaults = _computeDefaultColumns(fields);
  const saved = _loadColumns(dsName);
  const inMem = _schemaCache[dsName] ? _schemaCache[dsName].visibleColumns : null;

  // Prefer saved (localStorage), fall back to in-memory, fall back to computed defaults.
  // Validate: keep only columns that actually exist in the new fields.
  const validate = cols => cols ? cols.filter(c => fields[c] !== undefined) : null;
  const validSaved  = validate(saved);
  const validInMem  = validate(inMem);
  const toUse = (validSaved  && validSaved.length)  ? validSaved  :
                (validInMem  && validInMem.length)   ? validInMem  :
                [...defaults];

  _schemaCache[dsName] = { fields, defaultColumns: defaults, visibleColumns: toUse };
}

/** Build a schema object from records entirely on the client (used as fallback). */
function _deriveSchemaFromRecords(records) {
  const fields = {};
  function collect(obj, prefix, depth) {
    if (!obj || typeof obj !== 'object' || Array.isArray(obj) || depth > 3) return;
    for (const [k, v] of Object.entries(obj)) {
      if (k.startsWith('_')) continue;
      const key = prefix ? prefix + '.' + k : k;
      let type = 'string';
      if      (v === null || v === undefined)          type = 'string';
      else if (typeof v === 'boolean')                 type = 'bool';
      else if (Array.isArray(v))                       type = 'list';
      else if (typeof v === 'object')                  type = 'object';
      else if (typeof v === 'number' && Number.isInteger(v)) type = 'int';
      else if (typeof v === 'number')                  type = 'float';

      if (!fields[key]) fields[key] = { type, example: null };
      if (fields[key].example === null && v != null) {
        if (typeof v !== 'object')      fields[key].example = v;
        else if (Array.isArray(v))      fields[key].example = v.slice(0, 2);
      }
      if (typeof v === 'object' && v !== null && !Array.isArray(v))
        collect(v, key, depth + 1);
    }
  }
  records.forEach(r => { if (r && typeof r === 'object') collect(r, '', 0); });
  return fields;
}

// ══════════════════════════════════════════
//  Time field detection + UI
// ══════════════════════════════════════════

const _TS_RE = /^(timestamp|time|ts|created_at|created|date|datetime|at|event_time|start_time|end_time|updated_at|logged_at)$/i;

/** Return the best timestamp-looking leaf field, or null if none found. */
function _findTimeField(fields) {
  const leaves = _leafFields(fields);
  // Prefer top-level (no dot) fields whose name matches the timestamp regex
  const top = leaves.filter(k => !k.includes('.') && _TS_RE.test(k));
  if (top.length) return top[0];
  // Fall back to dotted paths whose last segment looks like a timestamp
  const nested = leaves.filter(k => _TS_RE.test(k.split('.').pop()));
  if (nested.length) return nested[0];
  return null;
}

/** Populate the time field selector and enable/disable the time range picker. */
function _updateTimeFieldUI() {
  const sel  = document.getElementById('timeFieldSel');
  const wrap = document.getElementById('timePickerWrap');
  if (!sel || !wrap) return;

  const schema = activeDs && _schemaCache[activeDs.name];
  if (!schema) {
    sel.style.display = 'none';
    wrap.classList.remove('time-off');
    return;
  }

  const leaves = _leafFields(schema.fields);
  const tsFields    = leaves.filter(k => _TS_RE.test(k.split('.').pop()));
  const otherFields = leaves.filter(k => !_TS_RE.test(k.split('.').pop()));

  let html = '<option value="">— off —</option>';
  if (tsFields.length) {
    html += '<optgroup label="Timestamp fields">' +
      tsFields.map(f => '<option value="' + esc(f) + '">' + esc(f) + '</option>').join('') +
      '</optgroup>';
  }
  if (otherFields.length) {
    html += '<optgroup label="Other fields">' +
      otherFields.map(f => '<option value="' + esc(f) + '">' + esc(f) + '</option>').join('') +
      '</optgroup>';
  }
  sel.innerHTML = html;

  // Auto-select on first schema load for this datasource (_timeField === null)
  if (_timeField === null) {
    const detected = _findTimeField(schema.fields);
    _timeField = (detected !== null) ? detected : '';
  }

  sel.value = _timeField;
  sel.style.display = '';
  wrap.classList.toggle('time-off', _timeField === '');
}

/** Called when user changes the time field selector. */
function setTimeField(value) {
  _timeField = value;
  const wrap = document.getElementById('timePickerWrap');
  if (wrap) wrap.classList.toggle('time-off', value === '');
}

// ══════════════════════════════════════════
//  Schema detection (hybrid: fires on tab select)
// ══════════════════════════════════════════

async function _detectSchema(ds, force = false) {
  const name = ds.name;
  if (!force && _schemaCache[name]) { _setSchemaBarStatus('ready'); return; }
  if (_schemaDetecting[name] && !force) return;

  _schemaDetecting[name] = true;
  _setSchemaBarStatus('detecting');

  try {
    let fields;
    const auth = authHeader();

    if (ds._user) {
      // User-defined connection: proxy search for a few records, derive client-side
      const data = await proxySearch('*', 5);
      fields = _deriveSchemaFromRecords(data.results || []);

    } else if (ds.type === 's3') {
      // S3 needs keys first: list recent files, then sample their contents
      const lp = new URLSearchParams({ datasource: name });
      const now = new Date();
      lp.set('start', new Date(now - 7 * 24 * 60 * 60 * 1000).toISOString());
      lp.set('end', now.toISOString());
      const lr = await fetch(BASE + '/api/public/logs/list?' + lp, { headers: { Authorization: auth } });
      if (!lr.ok) throw new Error('list failed: ' + lr.status);
      const ld = await lr.json();
      const keys = (ld.files || []).slice(0, 5).map(f => f.key);
      if (!keys.length) { _setSchemaBarStatus('nodata'); return; }

      const sp = new URLSearchParams({ datasource: name });
      keys.forEach(k => sp.append('keys', k));
      const sr = await fetch(BASE + '/api/public/datasource/sample?' + sp, { headers: { Authorization: auth } });
      if (!sr.ok) throw new Error('sample failed: ' + sr.status);
      fields = (await sr.json()).fields;

    } else {
      // ClickHouse / Trino / Langfuse / CHYT: direct sample call
      const sp = new URLSearchParams({ datasource: name });
      const now = new Date();
      sp.set('start', new Date(now - 24 * 60 * 60 * 1000).toISOString());
      sp.set('end', now.toISOString());
      const r = await fetch(BASE + '/api/public/datasource/sample?' + sp, { headers: { Authorization: auth } });
      if (!r.ok) throw new Error('sample failed: ' + r.status);
      fields = (await r.json()).fields;
    }

    // Bail out if the user switched away before we finished
    if (!activeDs || activeDs.name !== name) return;

    _storeSchema(name, fields);
    _setSchemaBarStatus('ready');
    _updateTimeFieldUI();

    // If search results are already on screen, re-render with proper schema
    // keepState=true so sort/filter the user already applied is preserved
    if (currentEvents.length) renderDynamicTable(currentEvents, true);

  } catch (e) {
    if (!activeDs || activeDs.name !== name) return;
    _setSchemaBarStatus('error', e.message);
  } finally {
    delete _schemaDetecting[name];
  }
}

// ══════════════════════════════════════════
//  Schema bar UI
// ══════════════════════════════════════════

function _setSchemaBarStatus(status, msg) {
  const bar = document.getElementById('schemaBar');
  if (!bar) return;
  const dsName = activeDs ? activeDs.name : '';
  bar.style.display = '';

  if (status === 'detecting') {
    bar.className = 'schema-bar detecting';
    bar.innerHTML =
      '<span class="schema-dot detecting"></span>' +
      'Detecting schema from <b>' + esc(dsName) + '</b>…';

  } else if (status === 'ready') {
    const schema = _schemaCache[dsName];
    const nLeaf = schema ? _leafFields(schema.fields).length : 0;
    bar.className = 'schema-bar ready';
    bar.innerHTML =
      '<span class="schema-dot ready"></span>' +
      '<span><b>' + nLeaf + '</b> fields detected from <b>' + esc(dsName) + '</b></span>' +
      '<span class="schema-actions">' +
        '<button class="cp-btn" id="cpToggleBtn" onclick="toggleColumnPicker()">⚙ Columns ▾</button>' +
        '<button class="cp-btn" onclick="_detectSchema(activeDs, true)" title="Re-sample schema">↺</button>' +
      '</span>';

  } else if (status === 'error') {
    bar.className = 'schema-bar error';
    bar.innerHTML =
      '<span class="schema-dot error"></span>' +
      '<span>Schema detection failed' + (msg ? ': ' + esc(msg) : '') + '</span>' +
      '<span class="schema-actions">' +
        '<button class="cp-btn" onclick="_detectSchema(activeDs, true)">Retry</button>' +
      '</span>';

  } else if (status === 'nodata') {
    bar.className = 'schema-bar nodata';
    bar.innerHTML =
      '<span class="schema-dot nodata"></span>' +
      'No data found in last 7 days — schema will be inferred after first search';
  }
}

// ══════════════════════════════════════════
//  Column picker
// ══════════════════════════════════════════

function toggleColumnPicker() {
  _colPickerOpen ? closeColumnPicker() : openColumnPicker();
}

function openColumnPicker() {
  _colPickerOpen = true;
  _renderColumnPicker();
  const el = document.getElementById('colPicker');
  if (el) el.style.display = '';
  const btn = document.getElementById('cpToggleBtn');
  if (btn) { btn.textContent = '⚙ Columns ▲'; btn.classList.add('active'); }
}

function closeColumnPicker() {
  _colPickerOpen = false;
  const el = document.getElementById('colPicker');
  if (el) el.style.display = 'none';
  const btn = document.getElementById('cpToggleBtn');
  if (btn) { btn.textContent = '⚙ Columns ▾'; btn.classList.remove('active'); }
}

function _renderColumnPicker() {
  const schema = activeDs && _schemaCache[activeDs.name];
  if (!schema) return;
  const leaves = _leafFields(schema.fields);
  const visible = new Set(schema.visibleColumns);

  const items = leaves.map(f => {
    const info = schema.fields[f];
    const ex = info.example !== null ? String(info.example).substring(0, 28) : '';
    const safeF = f.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
    return '<label class="col-picker-item">' +
      '<input type="checkbox"' + (visible.has(f) ? ' checked' : '') +
        ' onchange="toggleColumn(\'' + safeF + '\', this.checked)">' +
      '<span class="col-picker-name">' + esc(f) + '</span>' +
      '<span class="col-picker-type">' + esc(info.type) + '</span>' +
      (ex ? '<span class="col-picker-ex" title="' + esc(String(info.example)) + '">' + esc(ex) + '</span>' : '') +
    '</label>';
  }).join('');

  document.getElementById('colPicker').innerHTML =
    '<div class="col-picker-header">' +
      '<span>Visible columns <span style="font-weight:400;color:#9ca3af;font-size:.78rem">' +
        visible.size + ' / ' + leaves.length + ' fields</span></span>' +
      '<button class="btn-secondary btn-sm" onclick="resetToDefaultColumns()">Reset defaults</button>' +
    '</div>' +
    '<div class="col-picker-list">' + items + '</div>';
}

function toggleColumn(field, checked) {
  const schema = activeDs && _schemaCache[activeDs.name];
  if (!schema) return;
  if (checked) {
    if (!schema.visibleColumns.includes(field)) schema.visibleColumns.push(field);
  } else {
    schema.visibleColumns = schema.visibleColumns.filter(f => f !== field);
  }
  // Clear sort for a column being hidden
  if (!checked && _sortCol === field) { _sortCol = null; _sortDir = 'asc'; }
  _saveColumns(activeDs.name, schema.visibleColumns);
  _renderColumnPicker();  // refresh checkbox counts
  if (currentEvents.length) renderDynamicTable(currentEvents, true);
}

function resetToDefaultColumns() {
  const schema = activeDs && _schemaCache[activeDs.name];
  if (!schema) return;
  schema.visibleColumns = [...schema.defaultColumns];
  if (_sortCol && !schema.visibleColumns.includes(_sortCol)) { _sortCol = null; _sortDir = 'asc'; }
  _saveColumns(activeDs.name, schema.visibleColumns);
  _renderColumnPicker();
  if (currentEvents.length) renderDynamicTable(currentEvents, true);
}

// ══════════════════════════════════════════
//  Dynamic table renderer
// ══════════════════════════════════════════

function _getNestedValue(obj, path) {
  let curr = obj;
  for (const key of path.split('.')) {
    if (curr == null || typeof curr !== 'object') return undefined;
    curr = curr[key];
  }
  return curr;
}

function _renderCellValue(v) {
  if (v === undefined || v === null) return '<span style="color:#d1d5db">—</span>';
  if (typeof v === 'boolean') return String(v);
  if (typeof v === 'object') {
    const s = JSON.stringify(v);
    return esc(s.length > 60 ? s.substring(0, 58) + '…' : s);
  }
  const s = String(v);
  return esc(s.length > 80 ? s.substring(0, 78) + '…' : s);
}

// ══════════════════════════════════════════
//  Client-side sort (backend handles filtering)
// ══════════════════════════════════════════

function setSortCol(col) {
  _sortDir = (_sortCol === col && _sortDir === 'asc') ? 'desc' : 'asc';
  _sortCol = col;
  _updateSortIndicators();
  _renderTableBody(_sortedEvents());
}

function _sortedEvents() {
  if (!_sortCol) return [...currentEvents];
  return [...currentEvents].sort((a, b) => {
    const av = _getNestedValue(a, _sortCol);
    const bv = _getNestedValue(b, _sortCol);
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    const as = typeof av === 'object' ? JSON.stringify(av) : String(av);
    const bs = typeof bv === 'object' ? JSON.stringify(bv) : String(bv);
    const cmp = as < bs ? -1 : as > bs ? 1 : 0;
    return _sortDir === 'asc' ? cmp : -cmp;
  });
}

function _updateSortIndicators() {
  document.querySelectorAll('.sort-icon').forEach(el => {
    const btn = el.closest('[data-col]');
    const col = btn && btn.dataset.col;
    if (col === _sortCol) {
      el.textContent = _sortDir === 'asc' ? '↑' : '↓';
      el.className = 'sort-icon ' + _sortDir;
    } else {
      el.textContent = '⇅';
      el.className = 'sort-icon';
    }
  });
}

/** Re-render only tbody — preserves thead and sort state. */
function _renderTableBody(rows) {
  const schema = activeDs && _schemaCache[activeDs.name];
  const cols = schema && schema.visibleColumns && schema.visibleColumns.length
    ? schema.visibleColumns : [];
  const tbody = document.getElementById('tbody');
  if (!tbody || !cols.length) return;

  tbody.innerHTML = '';
  document.getElementById('results').style.display = rows.length ? '' : 'none';

  rows.forEach((ev, i) => {
    const cells = cols.map(c => {
      const v = _getNestedValue(ev, c);
      const raw = v != null ? (typeof v === 'object' ? JSON.stringify(v) : String(v)) : '';
      return '<td class="truncate" title="' + esc(raw.substring(0, 300)) + '">' + _renderCellValue(v) + '</td>';
    }).join('');
    const origIdx = currentEvents.indexOf(ev);
    const idx = origIdx >= 0 ? origIdx : i;
    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td><input type="checkbox" class="sel" data-idx="' + idx + '"></td>' +
      cells +
      '<td><button class="btn-secondary btn-sm" onclick="previewEvent(' + idx + ')">Preview</button></td>';
    tbody.appendChild(tr);
  });
}

// ══════════════════════════════════════════
//  Backend filter rule builder
// ══════════════════════════════════════════

const FILTER_OP_LABELS = {
  contains: 'contains', not_contains: 'not contains', eq: 'equals', neq: 'not equals',
  starts_with: 'starts with', in: 'in', not_in: 'not in',
  gt: '>', lt: '<', gte: '>=', lte: '<=',
  is_null: 'is empty', not_null: 'not empty',
};

function toggleFilters() {
  _filtersOpen = !_filtersOpen;
  document.getElementById('filter_panel').style.display = _filtersOpen ? '' : 'none';
  _updateFilterToggleBtn();
  _updateFilterModeUI();
  if (_filtersOpen) {
    _refreshFilterFieldSelect();
    _renderFilterRules();
  }
}

function _updateFilterToggleBtn() {
  const btn = document.getElementById('filter_toggle_btn');
  if (!btn) return;
  const n = _filterRules.length;
  const badge = n ? '<span class="filter-badge">' + n + '</span>' : '';
  btn.innerHTML = '⚗ Filters' + badge + (_filtersOpen ? ' ▲' : ' ▼');
  btn.classList.toggle('active', _filtersOpen || n > 0);
}

function _setFilterMode(mode) {
  _filterMode = mode;
  _updateFilterModeUI();
}

function _updateFilterModeUI() {
  const toggle = document.getElementById('filter_mode_toggle');
  if (toggle) toggle.style.display = (_filtersOpen || _filterRules.length > 1) ? '' : 'none';
  const andBtn = document.getElementById('fm_and');
  const orBtn  = document.getElementById('fm_or');
  if (andBtn) andBtn.classList.toggle('active', _filterMode === 'and');
  if (orBtn)  orBtn.classList.toggle('active', _filterMode === 'or');
}

function _onFilterOpChange(sel) {
  const inp = document.getElementById('filter_val_inp');
  if (!inp) return;
  if (sel.value === 'in' || sel.value === 'not_in') {
    inp.placeholder = 'val1, val2, …';
  } else if (sel.value === 'is_null' || sel.value === 'not_null') {
    inp.placeholder = '';
  } else {
    inp.placeholder = 'value…';
  }
}

// ── Masking rules ────────────────────────────────────────────────────────────
function _onMaskActionChange(sel) {
  const ml = document.getElementById('mask_maxlen_inp');
  if (ml) ml.style.display = sel.value === 'truncate' ? '' : 'none';
}

function addMaskRule() {
  const field  = (document.getElementById('mask_field_inp').value || '').trim();
  const action = document.getElementById('mask_action_sel').value;
  const maxLen = action === 'truncate' ? parseInt(document.getElementById('mask_maxlen_inp').value) || null : null;
  if (!field) return;
  _maskRules.push({ field, action, max_length: maxLen });
  document.getElementById('mask_field_inp').value = '';
  _renderMaskRules();
}

function removeMaskRule(i) {
  _maskRules.splice(i, 1);
  _renderMaskRules();
}

function _renderMaskRules() {
  const el = document.getElementById('mask_rules');
  if (!el) return;
  if (!_maskRules.length) {
    el.innerHTML = '<div style="font-size:.78rem;color:#aaa;padding:2px 0 6px">No masking rules.</div>';
    return;
  }
  el.innerHTML = _maskRules.map((r, i) =>
    '<div class="mask-rule">' +
      '<span class="mask-rule-field">' + esc(r.field) + '</span>' +
      '<span class="mask-rule-action">' + esc(r.action) + (r.max_length ? ' (' + r.max_length + ')' : '') + '</span>' +
      '<button class="mask-rule-remove" onclick="removeMaskRule(' + i + ')" title="Remove">✕</button>' +
    '</div>'
  ).join('');
}

// ── Asset resolution panel ───────────────────────────────────────────────────
function _toggleAssetPanel(show) {
  const p = document.getElementById('asset_panel');
  if (p) p.style.display = show ? '' : 'none';
  if (show) _renderAssetSources();
}

function _addAssetSource() {
  const inp = document.getElementById('asset_src_inp');
  const val = (inp && inp.value || '').trim();
  if (!val || _assetSources.includes(val)) return;
  _assetSources.push(val);
  inp.value = '';
  _renderAssetSources();
}

function _removeAssetSource(i) {
  _assetSources.splice(i, 1);
  _renderAssetSources();
}

function _renderAssetSources() {
  const el = document.getElementById('asset_sources_list');
  if (!el) return;
  el.innerHTML = _assetSources.map((s, i) =>
    '<span class="asset-tag">' + esc(s) +
      '<button onclick="_removeAssetSource(' + i + ')" title="Remove">✕</button>' +
    '</span>'
  ).join('');
}

function _refreshFilterFieldSelect() {
  const sel = document.getElementById('filter_field_sel');
  if (!sel) return;
  const schema = activeDs && _schemaCache[activeDs.name];
  if (!schema) return;
  const leaves = _leafFields(schema.fields);
  sel.innerHTML = '<option value="">— field —</option>' +
    leaves.map(f => '<option value="' + esc(f) + '">' + esc(f) + '</option>').join('');
}

function addFilterRule() {
  const field = document.getElementById('filter_field_sel').value;
  const op    = document.getElementById('filter_op_sel').value;
  const value = document.getElementById('filter_val_inp').value.trim();
  if (!field) return;
  const needsValue = op !== 'is_null' && op !== 'not_null';
  if (needsValue && !value) return;
  _filterRules.push({ field, op, value: needsValue ? value : null });
  document.getElementById('filter_val_inp').value = '';
  _renderFilterRules();
  _updateFilterToggleBtn();
}

function removeFilterRule(i) {
  _filterRules.splice(i, 1);
  _renderFilterRules();
  _updateFilterToggleBtn();
}

function _renderFilterRules() {
  const el = document.getElementById('filter_rules');
  if (!el) return;
  if (!_filterRules.length) {
    el.innerHTML = '<div style="font-size:.8rem;color:#aaa;padding:4px 0 8px">No filters — all results shown.</div>';
    return;
  }
  el.innerHTML = _filterRules.map((r, i) =>
    '<div class="filter-rule">' +
      '<span class="filter-rule-field">' + esc(r.field) + '</span>' +
      '<span class="filter-rule-op">' + esc(FILTER_OP_LABELS[r.op] || r.op) + '</span>' +
      (r.value != null ? '<span class="filter-rule-value">' + esc(r.value) + '</span>' : '') +
      '<button class="filter-rule-remove" onclick="removeFilterRule(' + i + ')" title="Remove">✕</button>' +
    '</div>'
  ).join('');
}

/** Serialize active filter rules for the backend (JSON query param). */
function _filtersParam() {
  return _filterRules.length ? JSON.stringify(_filterRules) : null;
}

// ══════════════════════════════════════════
//  Dynamic table renderer
// ══════════════════════════════════════════

/**
 * Full table render — call for new search results.
 * @param {object[]} records  Raw results from backend.
 * @param {boolean}  keepState  When true, preserves current sort/filter (used by schema re-render).
 */
function renderDynamicTable(records, keepState = false) {
  const schema = activeDs && _schemaCache[activeDs.name];
  let cols;

  if (schema && schema.visibleColumns && schema.visibleColumns.length) {
    cols = schema.visibleColumns;
  } else {
    // Schema not ready — derive client-side
    const derived = _deriveSchemaFromRecords(records);
    if (Object.keys(derived).length && activeDs) {
      _storeSchema(activeDs.name, derived);
      cols = _schemaCache[activeDs.name].visibleColumns;
      _setSchemaBarStatus('ready');
    } else {
      cols = Object.keys(records[0] || {}).filter(k => !k.startsWith('_')).slice(0, 6);
    }
  }

  // Reset sort on fresh search; keep it on schema-triggered re-render
  if (!keepState) {
    _sortCol = null;
    _sortDir = 'asc';
  }

  // Disambiguate labels: show full path only when last segment is duplicated
  const lastSegs = cols.map(c => c.split('.').pop());
  const colHeaders = cols.map((c, i) => {
    const label = lastSegs.filter((s, j) => s === lastSegs[i] && j !== i).length > 0 ? c : lastSegs[i];
    const safeC = c.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
    const sortIcon = (c === _sortCol)
      ? '<span class="sort-icon ' + _sortDir + '">' + (_sortDir === 'asc' ? '↑' : '↓') + '</span>'
      : '<span class="sort-icon">⇅</span>';
    return '<th>' +
      '<button class="col-sort" data-col="' + esc(c) + '" onclick="setSortCol(\'' + safeC + '\')" title="Sort by ' + esc(c) + '">' +
        esc(label) + sortIcon +
      '</button>' +
    '</th>';
  }).join('');

  document.getElementById('thead').innerHTML =
    '<tr>' +
      '<th><input type="checkbox" id="selectAll" onchange="toggleAll(this)"></th>' +
      colHeaders +
      '<th></th>' +
    '</tr>';

  document.getElementById('results').style.display = '';
  document.getElementById('resultActions').innerHTML = '';

  _renderTableBody(_sortedEvents());
}

// ── Auth ──
function authHeader() {
  const pk = document.getElementById('pk').value;
  const sk = document.getElementById('sk').value;
  if (pk) return 'Basic ' + btoa(pk + ':' + sk);
  return '';
}
function esc(s) { const d = document.createElement('div'); d.textContent = s; return d.innerHTML; }

// ══════════════════════════════════════════
//  Datasource tabs
// ══════════════════════════════════════════

async function init() {
  loadUserConnections();
  try {
    const resp = await fetch(BASE + '/api/public/ui-config');
    if (resp.ok) {
      const cfg = await resp.json();
      if (cfg.hide_auth_inputs) document.getElementById('authCard').style.display = 'none';
      datasources = cfg.datasources || [];
      connections = cfg.connections || [];
      targets = cfg.targets || [];
    }
  } catch {}

  // Populate destination target dropdown
  const destSel = document.getElementById('dest_target');
  if (destSel) {
    destSel.innerHTML = '<option value="">— select target —</option>' +
      targets.map(t => '<option value="' + esc(t.name) + '">' + esc(t.name) + '</option>').join('');
  }

  renderTabs();
  const allTabs = getAllTabs();
  if (allTabs.length) selectDatasource(allTabs[0]);
}

function getAllTabs() {
  const tabs = datasources.map(ds => ({...ds}));
  for (const uc of userConnections) {
    tabs.push({
      name: uc.name, type: uc.type, _user: true,
      _connUrl: uc.url, _pk: uc.pk, _sk: uc.sk,
      _bucket: uc.bucket, _key_prefix: uc.key_prefix, _region: uc.region,
    });
  }
  return tabs;
}

function renderTabs() {
  const container = document.getElementById('dsTabs');
  const allTabs = getAllTabs();
  const hasConnections = connections.length > 0;

  if (!allTabs.length && !hasConnections) {
    container.innerHTML = '';
    document.getElementById('filtersCard').style.display = 'none';
    document.getElementById('results').style.display = 'none';
    document.getElementById('status').innerHTML =
      '<div class="empty-state"><p>No datasources configured</p><small>Add datasources to config.yaml</small></div>';
    return;
  }

  let html = allTabs.map(ds => {
    const removeBtn = ds._user
      ? '<span class="ds-remove" onclick="event.stopPropagation();removeUserConnection(\'' + esc(ds.name) + '\')">&times;</span>'
      : '';
    return '<div class="ds-tab" data-name="' + esc(ds.name) + '" onclick="selectDatasource(findTab(\'' + esc(ds.name) + '\'))">' +
      esc(ds.name) + removeBtn +
      '<span class="ds-type-badge ds-type-' + esc(ds.type) + '">' + esc(ds.type) + '</span>' +
    '</div>';
  }).join('');

  if (hasConnections) {
    html += '<div class="ds-tab ds-tab-add" onclick="showAddConnectionModal()">+ Add connection</div>';
  }

  container.innerHTML = html;
}

function findTab(name) { return getAllTabs().find(d => d.name === name); }

function removeUserConnection(name) {
  userConnections = userConnections.filter(c => c.name !== name);
  saveUserConnections();
  renderTabs();
  if (activeDs && activeDs.name === name) {
    const allTabs = getAllTabs();
    if (allTabs.length) selectDatasource(allTabs[0]);
    else {
      activeDs = null;
      document.getElementById('filtersCard').style.display = 'none';
      document.getElementById('results').style.display = 'none';
    }
  }
}

// ── Add connection modal ──
function showAddConnectionModal() {
  const options = connections.map(c =>
    '<option value="' + esc(c.url) + '" data-type="' + esc(c.type) + '" data-has-creds="' + (c.has_credentials ? '1' : '') + '">' + esc(c.label) + '</option>'
  ).join('');

  showModal('Add connection', '');
  document.querySelector('.modal-body').innerHTML =
    '<div class="conn-form">' +
      '<label>Host</label>' +
      '<select id="conn_url" onchange="onConnHostChange()">' + options + '</select>' +
      '<label>Name</label>' +
      '<input type="text" id="conn_name" placeholder="My connection">' +
      '<div id="conn_endpoint_field" style="display:none">' +
        '<label>Endpoint URL</label>' +
        '<input type="text" id="conn_endpoint" placeholder="https://s3.amazonaws.com or http://minio:9000">' +
      '</div>' +
      '<div id="conn_creds_fields">' +
        '<label id="conn_pk_label">Public Key</label>' +
        '<input type="text" id="conn_pk" placeholder="...">' +
        '<label id="conn_sk_label">Secret Key</label>' +
        '<input type="text" id="conn_sk" placeholder="...">' +
      '</div>' +
      '<div id="conn_s3_fields" style="display:none">' +
        '<label>Bucket</label>' +
        '<input type="text" id="conn_bucket" placeholder="my-bucket">' +
        '<label>Path prefix <small style="font-weight:400;color:#999">(optional)</small></label>' +
        '<input type="text" id="conn_key_prefix" placeholder="logs/prod">' +
        '<label>Region <small style="font-weight:400;color:#999">(optional)</small></label>' +
        '<input type="text" id="conn_region" placeholder="us-east-1">' +
      '</div>' +
      '<div id="conn_status"></div>' +
      '<div class="actions">' +
        '<button class="btn-secondary" onclick="testConnection()">Test</button>' +
        '<button class="btn-primary" onclick="saveConnection()">Save</button>' +
      '</div>' +
    '</div>';
  onConnHostChange();
}

function onConnHostChange() {
  const sel = document.getElementById('conn_url');
  const opt = sel.options[sel.selectedIndex];
  const hasCreds = opt && opt.dataset.hasCreds === '1';
  const type = opt && opt.dataset.type;
  const isS3 = type === 's3';
  const isWildcard = isS3 && sel.value === '*';

  document.getElementById('conn_endpoint_field').style.display = isWildcard ? '' : 'none';
  document.getElementById('conn_s3_fields').style.display = isS3 ? '' : 'none';
  document.getElementById('conn_creds_fields').style.display = (!isS3 && hasCreds) ? 'none' : '';

  if (isS3) {
    document.getElementById('conn_pk_label').textContent = 'Access Key ID';
    document.getElementById('conn_sk_label').textContent = 'Secret Access Key';
    document.getElementById('conn_pk').placeholder = 'AKIA...';
    document.getElementById('conn_sk').placeholder = 'wJalrXUtnFEMI...';
  } else {
    document.getElementById('conn_pk_label').textContent = 'Public Key';
    document.getElementById('conn_sk_label').textContent = 'Secret Key';
    document.getElementById('conn_pk').placeholder = 'pk-lf-...';
    document.getElementById('conn_sk').placeholder = 'sk-lf-...';
  }
}

async function testConnection() {
  const statusDiv = document.getElementById('conn_status');
  statusDiv.innerHTML = '<div class="conn-status">Testing...</div>';

  const sel = document.getElementById('conn_url');
  const opt = sel.options[sel.selectedIndex];
  const type = opt && opt.dataset.type;
  const isS3 = type === 's3';
  const isWildcard = isS3 && sel.value === '*';

  let connUrl = sel.value;
  if (isWildcard) {
    connUrl = document.getElementById('conn_endpoint').value.trim();
    if (!connUrl) { statusDiv.innerHTML = '<div class="conn-status err">Endpoint URL is required</div>'; return; }
  }

  const pk = document.getElementById('conn_pk').value;
  const sk = document.getElementById('conn_sk').value;
  const credentials = { connection_url: connUrl, access_key_id: pk, secret_access_key: sk };

  if (isS3) {
    const bucket = document.getElementById('conn_bucket').value.trim();
    if (!bucket) { statusDiv.innerHTML = '<div class="conn-status err">Bucket is required</div>'; return; }
    credentials.bucket = bucket;
    const keyPrefix = document.getElementById('conn_key_prefix').value.trim();
    const region = document.getElementById('conn_region').value.trim();
    if (keyPrefix) credentials.key_prefix = keyPrefix;
    if (region) credentials.region = region;
  }

  try {
    const resp = await fetch(BASE + '/api/public/proxy/ping', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ credentials }),
    });
    if (resp.ok) {
      statusDiv.innerHTML = '<div class="conn-status ok">Connected successfully</div>';
    } else {
      const data = await resp.json().catch(() => ({}));
      statusDiv.innerHTML = '<div class="conn-status err">Failed: ' + esc(data.detail || resp.statusText) + '</div>';
    }
  } catch (e) {
    statusDiv.innerHTML = '<div class="conn-status err">Error: ' + esc(e.message) + '</div>';
  }
}

function saveConnection() {
  const sel = document.getElementById('conn_url');
  const opt = sel.options[sel.selectedIndex];
  const type = opt && opt.dataset.type;
  const isS3 = type === 's3';
  const isWildcard = isS3 && sel.value === '*';

  const conn = connections.find(c => c.url === sel.value);
  if (!conn) return;

  let connUrl = sel.value;
  if (isWildcard) {
    connUrl = document.getElementById('conn_endpoint').value.trim();
    if (!connUrl) { alert('Endpoint URL is required'); return; }
  }

  const name = document.getElementById('conn_name').value.trim();
  if (!name) { alert('Name is required'); return; }

  const pk = document.getElementById('conn_pk').value;
  const sk = document.getElementById('conn_sk').value;
  if (!isS3 && !conn.has_credentials && !pk) { alert('Public Key is required'); return; }

  const entry = { name, type: conn.type, url: connUrl, pk, sk };

  if (isS3) {
    const bucket = document.getElementById('conn_bucket').value.trim();
    if (!bucket) { alert('Bucket is required'); return; }
    entry.bucket = bucket;
    const keyPrefix = document.getElementById('conn_key_prefix').value.trim();
    const region = document.getElementById('conn_region').value.trim();
    if (keyPrefix) entry.key_prefix = keyPrefix;
    if (region) entry.region = region;
  }

  userConnections = userConnections.filter(c => c.name !== name);
  userConnections.push(entry);
  saveUserConnections();
  closeModal();
  renderTabs();
  selectDatasource(findTab(name));
}

function selectDatasource(ds) {
  if (!ds) return;
  activeDs = ds;
  // Update tab highlights
  document.querySelectorAll('.ds-tab').forEach(tab => {
    tab.classList.toggle('active', tab.dataset.name === ds.name);
  });
  // Clear results
  document.getElementById('results').style.display = 'none';
  document.getElementById('status').textContent = '';
  document.getElementById('urls').innerHTML = '';
  currentEvents = [];
  currentSearchKeys = [];
  // Close pickers, reset sort, filters, sampling and time field when switching tabs
  closeColumnPicker();
  _sortCol = null; _sortDir = 'asc';
  _filterRules = []; _filtersOpen = false;
  _timeField = null;  // will be auto-detected when schema loads
  const fp = document.getElementById('filter_panel');
  if (fp) fp.style.display = 'none';
  // Reset sampling state for the new datasource
  _sampRules = []; _sampOpen = false; _sampSchema = {};
  const sp = document.getElementById('samp_panel');
  if (sp) sp.style.display = 'none';
  const sb = document.getElementById('samp_btn');
  if (sb) { sb.classList.remove('active'); sb.textContent = '⚗ Sampling ▼'; }
  const so = document.getElementById('samp_options');
  if (so) so.style.display = 'none';
  const ssa = document.getElementById('samp_schema_area');
  if (ssa) ssa.innerHTML = '<span style="color:#aaa;font-size:.78rem">Click to load schema</span>';
  // Reset export options
  _expColMask = false; _expAssetResolve = false;
  _filterMode = 'and'; _maskRules = [];
  _assetSources = []; _assetFetchMode = 'metadata_only'; _assetCheckAvail = false;
  const ecm = document.getElementById('exp_col_mask'); if (ecm) ecm.checked = false;
  const ear = document.getElementById('exp_asset_resolve'); if (ear) ear.checked = false;
  _toggleAssetPanel(false);
  _renderMaskRules();
  _updateFilterModeUI();
  // Export card visibility is controlled by renderFilters() below
  document.getElementById('exportCard').style.display = 'none';
  // Show filters
  document.getElementById('filtersCard').style.display = '';
  renderFilters();
  _updateTimeFieldUI();  // hides selector until schema arrives
  // Fire schema detection immediately (hybrid: runs in background while user sets filters)
  _detectSchema(ds);
}

function renderFilters() {
  const isS3 = activeDs.type === 's3';
  const actions = document.getElementById('actionButtons');

  // Filter builder is shown for record-returning modes only
  const showFilters = !isS3 || activeDs._user;
  document.getElementById('filterToggleArea').style.display = showFilters ? '' : 'none';
  if (showFilters) _refreshFilterFieldSelect();

  // Export panel is always visible when a datasource is active
  document.getElementById('exportCard').style.display = '';
  _sampMode = (isS3 && !activeDs._user) ? 's3' : 'events';

  // Search info badge: tell the user what field(s) the query text searches
  const badge = document.getElementById('searchInfoBadge');
  if (badge) {
    let badgeText = '';
    if (activeDs._user) {
      badgeText = 'via proxy';
    } else {
      switch (activeDs.type) {
        case 's3':         badgeText = 'full-text'; break;
        case 'clickhouse': badgeText = 'body column'; break;
        case 'trino':      badgeText = 'body column'; break;
        case 'chyt':       badgeText = 'body column'; break;
        case 'langfuse':   badgeText = 'all content'; break;
      }
    }
    badge.textContent = badgeText;
    badge.style.display = badgeText ? '' : 'none';
  }

  if (isS3 && !activeDs._user) {
    // Server-configured S3: DuckDB full-text search
    actions.innerHTML =
      '<button class="btn-primary" onclick="fullTextSearch()">Search</button>' +
      '<button class="btn-secondary" onclick="loadLogs()">List recent</button>';
  } else {
    // Langfuse, ClickHouse, Trino, or user-defined S3 via proxy
    actions.innerHTML =
      '<button class="btn-primary" onclick="searchEvents()">Search</button>' +
      '<button class="btn-secondary" onclick="listEvents()">List recent</button>';
  }
}

// ── Query params ──
function searchParams() {
  const params = new URLSearchParams();
  params.set('datasource', activeDs.name);
  const range = getTimeRange();
  if (range.start) params.set('start', range.start);
  if (range.end) params.set('end', range.end);
  // Pass time_field to CH/Trino/CHYT backends when a non-default field is selected
  if (_timeField) params.set('time_field', _timeField);
  const fp = _filtersParam();
  if (fp) params.set('filters', fp);
  return params;
}

function commonFilters() {
  const range = getTimeRange();
  const obj = {};
  if (range.start) obj.start = range.start;
  if (range.end) obj.end = range.end;
  return obj;
}

// Proxy search for user connections
async function proxySearch(q, limit) {
  const credentials = {
    connection_url: activeDs._connUrl,
    access_key_id: activeDs._pk,
    secret_access_key: activeDs._sk,
  };
  if (activeDs.type === 's3') {
    if (activeDs._bucket) credentials.bucket = activeDs._bucket;
    if (activeDs._key_prefix) credentials.key_prefix = activeDs._key_prefix;
    if (activeDs._region) credentials.region = activeDs._region;
  }
  const body = {
    credentials,
    q: q,
    limit: limit,
    ...commonFilters(),
  };
  const resp = await fetch(BASE + '/api/public/proxy/search', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!resp.ok) {
    const data = await resp.json().catch(() => ({}));
    throw new Error(data.detail || 'HTTP ' + resp.status);
  }
  const data = await resp.json();
  // Apply backend filters client-side for proxy connections
  if (_filterRules.length && data.results) {
    data.results = _applyFiltersClientSide(data.results);
  }
  return data;
}

/** Mirror of Python apply_filters — used only for proxy (user) connections. */
function _applyFiltersClientSide(records) {
  return records.filter(rec =>
    _filterRules.every(r => {
      const v = _getNestedValue(rec, r.field);
      if (r.op === 'is_null')  return v == null || v === '';
      if (r.op === 'not_null') return v != null && v !== '';
      if (v == null) return false;
      const sv = String(typeof v === 'object' ? JSON.stringify(v) : v).toLowerCase();
      const fv = (r.value || '').toLowerCase();
      if (r.op === 'contains')     return sv.includes(fv);
      if (r.op === 'not_contains') return !sv.includes(fv);
      if (r.op === 'eq')           return sv === fv;
      if (r.op === 'neq')          return sv !== fv;
      if (r.op === 'starts_with')  return sv.startsWith(fv);
      try {
        const nv = parseFloat(sv), nfv = parseFloat(r.value || '0');
        if (r.op === 'gt')  return nv > nfv;
        if (r.op === 'lt')  return nv < nfv;
        if (r.op === 'gte') return nv >= nfv;
        if (r.op === 'lte') return nv <= nfv;
      } catch {}
      return true;
    })
  );
}

// ══════════════════════════════════════════
//  SEARCH MODE (ClickHouse, Trino, Langfuse)
// ══════════════════════════════════════════

async function searchEvents() {
  const q = document.getElementById('f_query').value.trim();
  if (!q) return;
  const status = document.getElementById('status');
  status.textContent = 'Searching...';
  try {
    let data;
    if (activeDs._user) {
      data = await proxySearch(q, 100);
    } else {
      const params = searchParams();
      params.set('q', q);
      params.set('limit', '100');
      const resp = await fetch(BASE + '/api/public/logs/search?' + params, {
        headers: { 'Authorization': authHeader() }
      });
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      data = await resp.json();
    }
    currentEvents = data.results;
    currentSearchKeys = [];
    if (data.backend === 's3') {
      renderFilesTable(currentEvents);
      status.textContent = currentEvents.length + ' batch(es) found [' + activeDs.name + ' / s3]';
    } else {
      renderDynamicTable(currentEvents);
      status.textContent = currentEvents.length + ' event(s) found [' + activeDs.name + ' / ' + (data.backend || activeDs.type) + ']';
    }
  } catch (e) { status.textContent = 'Error: ' + e.message; }
}

async function listEvents() {
  const status = document.getElementById('status');
  status.textContent = 'Loading...';
  try {
    let data;
    if (activeDs._user) {
      data = await proxySearch('*', 100);
    } else {
      const params = searchParams();
      params.set('q', '*');
      params.set('limit', '100');
      const resp = await fetch(BASE + '/api/public/logs/search?' + params, {
        headers: { 'Authorization': authHeader() }
      });
      if (!resp.ok) throw new Error('HTTP ' + resp.status);
      data = await resp.json();
    }
    currentEvents = data.results;
    if (data.backend === 's3') {
      renderFilesTable(currentEvents);
      status.textContent = currentEvents.length + ' batch(es) [' + activeDs.name + ' / s3]';
    } else {
      renderDynamicTable(currentEvents);
      status.textContent = currentEvents.length + ' event(s) [' + activeDs.name + ' / ' + (data.backend || activeDs.type) + ']';
    }
  } catch (e) { status.textContent = 'Error: ' + e.message; }
}

// ══════════════════════════════════════════
//  S3 MODE
// ══════════════════════════════════════════

async function loadLogs() {
  const status = document.getElementById('status');
  status.textContent = 'Loading...';
  document.getElementById('urls').innerHTML = '';
  const params = searchParams();
  try {
    const resp = await fetch(BASE + '/api/public/logs/list?' + params, {
      headers: { 'Authorization': authHeader() }
    });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    renderFilesTable(data.files);
    status.textContent = data.files.length + ' batch(es) found [' + activeDs.name + ' / s3]';
  } catch (e) { status.textContent = 'Error: ' + e.message; }
}

async function fullTextSearch() {
  const q = document.getElementById('f_query').value.trim();
  if (!q) return;
  const status = document.getElementById('status');
  status.textContent = 'Searching...';
  const params = searchParams();
  params.set('q', q);
  try {
    const resp = await fetch(BASE + '/api/public/logs/search?' + params, {
      headers: { 'Authorization': authHeader() }
    });
    if (!resp.ok) throw new Error('HTTP ' + resp.status);
    const data = await resp.json();
    currentEvents = data.results;
    currentSearchKeys = data.keys || [];
    renderDynamicTable(currentEvents);
    status.textContent = data.results.length + ' result(s) across ' + (data.files_scanned || '?') + ' file(s) [' + activeDs.name + ' / duckdb]';
  } catch (e) { status.textContent = 'Error: ' + e.message; }
}

// ══════════════════════════════════════════
//  Renderers
// ══════════════════════════════════════════

function toStr(v) {
  if (typeof v === 'string') return v;
  if (Array.isArray(v)) return v.map(p => p.text || p.content || JSON.stringify(p)).join(' ');
  return v != null ? JSON.stringify(v) : '';
}

function parseBody(body) {
  if (!body) return {};
  if (typeof body === 'string') { try { return JSON.parse(body); } catch(e) { return {}; } }
  return body;
}

function _extractMessages(inp) {
  if (Array.isArray(inp)) return inp;
  if (inp.messages && Array.isArray(inp.messages)) return inp.messages;
  return null;
}

function inputPreview(body) {
  const b = parseBody(body);
  if (!b.input) return '-';
  const inp = typeof b.input === 'string' ? (() => { try { return JSON.parse(b.input); } catch(e) { return b.input; } })() : b.input;
  if (typeof inp === 'string') return inp.substring(0, 80);
  const msgs = _extractMessages(inp);
  if (msgs && msgs.length) {
    const last = msgs[msgs.length - 1];
    return toStr(last.content).substring(0, 80);
  }
  return JSON.stringify(inp).substring(0, 80);
}

function outputPreview(body) {
  const b = parseBody(body);
  if (!b.output) return '-';
  const out = typeof b.output === 'string' ? (() => { try { return JSON.parse(b.output); } catch(e) { return b.output; } })() : b.output;
  if (typeof out === 'string') return out.substring(0, 80);
  if (out.choices && out.choices.length) {
    const msg = out.choices[0].message;
    return msg ? toStr(msg.content).substring(0, 80) : '-';
  }
  if (out.content) return toStr(out.content).substring(0, 80);
  return '-';
}

function renderEventsTable(events) {
  const thead = document.getElementById('thead');
  thead.innerHTML = '<tr><th><input type="checkbox" id="selectAll" onchange="toggleAll(this)"></th>' +
    '<th>Timestamp</th><th>Model</th><th>Input</th><th>Output</th><th>Type</th><th></th></tr>';

  const tbody = document.getElementById('tbody');
  tbody.innerHTML = '';
  document.getElementById('results').style.display = events.length ? '' : 'none';
  document.getElementById('resultActions').innerHTML = '';

  events.forEach((ev, i) => {
    const b = parseBody(ev.body);
    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td><input type="checkbox" class="sel" data-idx="' + i + '"></td>' +
      '<td>' + esc(ev.timestamp || '') + '</td>' +
      '<td>' + esc(b.model || '-') + '</td>' +
      '<td class="truncate" title="' + esc(inputPreview(b)) + '">' + esc(inputPreview(b)) + '</td>' +
      '<td class="truncate" title="' + esc(outputPreview(b)) + '">' + esc(outputPreview(b)) + '</td>' +
      '<td>' + esc(ev.type || '-') + '</td>' +
      '<td><button class="btn-secondary btn-sm" onclick="previewEvent(' + i + ')">Preview</button></td>';
    tbody.appendChild(tr);
  });
}

function renderFilesTable(files) {
  const thead = document.getElementById('thead');
  thead.innerHTML = '<tr><th><input type="checkbox" id="selectAll" onchange="toggleAll(this)"></th>' +
    '<th>Timestamp</th><th>Session ID</th><th>Trace ID</th><th>Trace Type</th><th>Input Hash</th><th></th></tr>';

  const tbody = document.getElementById('tbody');
  tbody.innerHTML = '';
  document.getElementById('results').style.display = files.length ? '' : 'none';
  document.getElementById('resultActions').innerHTML = '';

  files.forEach(f => {
    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td><input type="checkbox" class="sel" value="' + f.key.replace(/"/g, '&quot;') + '"></td>' +
      '<td>' + esc(f.timestamp) + '</td>' +
      '<td>' + esc(f.session_id) + '</td>' +
      '<td>' + esc(f.trace_id) + '</td>' +
      '<td>' + esc(f.trace_type) + '</td>' +
      '<td>' + esc(f.input_hash) + '</td>' +
      '<td><button class="btn-secondary btn-sm" onclick="previewFile(this)" data-key="' + f.key.replace(/"/g, '&quot;') + '">Preview</button></td>';
    tbody.appendChild(tr);
  });
}

// ══════════════════════════════════════════
//  Actions
// ══════════════════════════════════════════

function toggleAll(master) { document.querySelectorAll('.sel').forEach(c => c.checked = master.checked); }

function previewEvent(idx) {
  const ev = currentEvents[idx];
  showModal(ev.id || 'Event', JSON.stringify(ev, null, 2));
}

function selectedEventIndices() {
  return [...document.querySelectorAll('.sel:checked')].map(c => parseInt(c.dataset.idx));
}

function getSelectedEvents() {
  const indices = selectedEventIndices();
  if (indices.length) return indices.map(i => currentEvents[i]);
  return currentEvents;
}

// ══════════════════════════════════════════
//  S3 file fetch helpers
// ══════════════════════════════════════════

async function fetchFileEvents(keys) {
  const resp = await fetch(BASE + '/api/public/logs/urls?datasource=' + encodeURIComponent(activeDs.name), {
    method: 'POST',
    headers: { 'Authorization': authHeader(), 'Content-Type': 'application/json' },
    body: JSON.stringify({ keys })
  });
  if (!resp.ok) throw new Error('HTTP ' + resp.status);
  const data = await resp.json();
  const all = [];
  for (const f of data.files) {
    const r = await fetch(f.url);
    if (!r.ok) throw new Error('Failed to fetch ' + f.key);
    const text = await r.text();
    all.push(...text.trim().split('\n').filter(Boolean).map(line => JSON.parse(line)));
  }
  return all;
}

// Returns [{filename, events}] — preserves per-file grouping for ZIP downloads
async function _fetchFileEventsGrouped(keys) {
  const resp = await fetch(BASE + '/api/public/logs/urls?datasource=' + encodeURIComponent(activeDs.name), {
    method: 'POST',
    headers: { 'Authorization': authHeader(), 'Content-Type': 'application/json' },
    body: JSON.stringify({ keys })
  });
  if (!resp.ok) throw new Error('HTTP ' + resp.status);
  const data = await resp.json();
  const groups = [];
  for (const f of data.files) {
    const r = await fetch(f.url);
    if (!r.ok) throw new Error('Failed to fetch ' + f.key);
    const text = await r.text();
    const evs = text.trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
    groups.push({ filename: f.key.split('/').pop() || f.key, events: evs });
  }
  return groups;
}

async function _fetchPresignedUrls(keys) {
  const resp = await fetch(BASE + '/api/public/logs/urls?datasource=' + encodeURIComponent(activeDs.name), {
    method: 'POST',
    headers: { 'Authorization': authHeader(), 'Content-Type': 'application/json' },
    body: JSON.stringify({ keys })
  });
  if (!resp.ok) throw new Error('HTTP ' + resp.status);
  return (await resp.json()).files; // [{key, url}]
}

async function previewFile(btn) {
  const key = btn.dataset.key;
  showModal(key, 'Loading...');
  try {
    const events = await fetchFileEvents([key]);
    document.querySelector('.modal-body pre').textContent = JSON.stringify(events, null, 2);
  } catch (e) {
    document.querySelector('.modal-body pre').textContent = 'Error: ' + e.message;
  }
}

// ══════════════════════════════════════════
//  Shared helpers
// ══════════════════════════════════════════

function eventsToDataset(events) {
  const dataset = [];
  for (const ev of events) {
    const b = ev.body || ev;
    const msgs = [];
    const inp = b.input;
    if (inp && inp.messages && Array.isArray(inp.messages)) {
      for (const m of inp.messages) { if (m.role && m.content) msgs.push({ role: m.role, content: m.content }); }
    } else if (Array.isArray(inp)) {
      for (const m of inp) { if (m.role && m.content) msgs.push({ role: m.role, content: m.content }); }
    }
    const out = b.output;
    if (out && out.choices && out.choices.length) {
      const msg = out.choices[0].message;
      if (msg && msg.role && msg.content) msgs.push({ role: msg.role, content: msg.content });
    } else if (out && out.role && out.content) {
      msgs.push({ role: out.role, content: out.content });
    }
    if (msgs.length >= 2) dataset.push({ messages: msgs });
  }
  return dataset;
}

/** Apply column-mask filter: keep only visibleColumns fields (dot-notation paths). */
function _applyColMask(events) {
  if (!_expColMask) return events;
  const cache = activeDs && _schemaCache[activeDs.name];
  const cols = cache && cache.visibleColumns;
  if (!cols || !cols.length) return events;

  function pickPaths(obj, paths) {
    const result = {};
    for (const path of paths) {
      const parts = path.split('.');
      let src = obj, dst = result;
      let valid = true;
      for (let i = 0; i < parts.length - 1; i++) {
        if (src == null || src[parts[i]] == null || typeof src[parts[i]] !== 'object') { valid = false; break; }
        src = src[parts[i]];
        if (dst[parts[i]] == null) dst[parts[i]] = {};
        dst = dst[parts[i]];
      }
      if (valid && src != null) {
        const last = parts[parts.length - 1];
        if (src[last] !== undefined) dst[last] = src[last];
      }
    }
    return result;
  }

  return events.map(ev => pickPaths(ev, cols));
}

function downloadFile(content, filename, type) {
  const blob = new Blob([content], { type });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a'); a.href = url; a.download = filename; a.click();
  URL.revokeObjectURL(url);
}

// ══════════════════════════════════════════
//  Export modal
// ══════════════════════════════════════════

function suggestDatasetName() {
  const parts = [activeDs.name.replace(/\s+/g, '-').toLowerCase()];
  const range = getTimeRange();
  if (range.start) parts.push(range.start.substring(0, 10));
  if (range.end && range.end.substring(0, 10) !== (range.start || '').substring(0, 10))
    parts.push(range.end.substring(0, 10));
  const q = document.getElementById('f_query') && document.getElementById('f_query').value.trim();
  if (q && q !== '*') parts.push(q.substring(0, 30).replace(/\s+/g, '-').replace(/[^a-zA-Z0-9\-_]/g, ''));
  return parts.filter(Boolean).join('_');
}

function showExportModal(mode) {
  _exportMode = mode;
  _exportDest = 'pc';
  _exportFormat = 'jsonl';
  // Sampling state (_sampRules, _sampSchema, etc.) is global — set via the Sampling panel

  const selectedCount = document.querySelectorAll('.sel:checked').length;
  const totalCount = mode === 's3'
    ? (selectedCount || document.querySelectorAll('.sel').length)
    : (selectedCount || currentEvents.length);
  const itemLabel = mode === 's3' ? 'file' : 'event';
  const countLabel = selectedCount
    ? selectedCount + ' ' + itemLabel + '(s) selected'
    : totalCount + ' ' + itemLabel + '(s)';

  showModal('Export', '');
  document.querySelector('.modal-body').innerHTML = _buildExportStep1Html(countLabel, mode);
  // pre-select pc card
  document.querySelector('.export-dest-card[data-dest="pc"]').classList.add('selected');
  document.querySelector('.export-fmt-opt[data-fmt="jsonl"]').classList.add('selected');
  document.querySelector('.export-fmt-opt[data-fmt="jsonl"] input').checked = true;
}

function _buildExportStep1Html(countLabel, mode) {
  const svcDisabled = targets.length === 0 ? ' disabled' : '';
  return '<div id="exp_step1">' +
    '<div style="font-size:.82rem;color:#6b7280;margin-bottom:2px">Exporting <b>' + esc(countLabel) + '</b></div>' +
    '<div class="export-section-label">Destination</div>' +
    '<div class="export-dest-grid">' +
      '<div class="export-dest-card" data-dest="pc" onclick="_selDest(\'pc\')">' +
        '<div class="dest-icon">💻</div>' +
        '<div class="dest-label">Download to PC</div>' +
        '<div class="dest-desc">Save file(s) directly to your computer</div>' +
      '</div>' +
      '<div class="export-dest-card' + svcDisabled + '" data-dest="service" onclick="_selDest(\'service\')">' +
        '<div class="dest-icon">☁️</div>' +
        '<div class="dest-label">Dataset service</div>' +
        '<div class="dest-desc">' + (targets.length ? 'Upload to ' + esc(targets[0].name) : 'No targets configured') + '</div>' +
      '</div>' +
    '</div>' +
    '<div class="export-section-label">Format</div>' +
    '<div class="export-fmt-list">' +
      _fmtOpt('individual', '📁 Individual files',
        mode === 's3' ? 'One JSON file per S3 batch — bundled as ZIP' : 'One JSON file per event — bundled as ZIP') +
      _fmtOpt('jsonl', '📄 JSONL — raw',
        'All records as newline-delimited JSON (.jsonl)') +
      _fmtOpt('jsonl_conv', '💬 JSONL — conversations',
        'Transform to OpenAI <code>{messages:[{role,content}]}</code> format') +
      _fmtOpt('catalog', '🔗 Catalog',
        mode === 's3' ? 'Presigned URL index — links back to S3 source files' : 'Pretty-printed JSON array of all records') +
    '</div>' +
    '<div id="exp_status"></div>' +
    '<div class="actions">' +
      '<button class="btn-secondary" onclick="closeModal()">Cancel</button>' +
      '<button class="btn-primary" id="exp_btn" onclick="_advanceExport()">Export</button>' +
    '</div>' +
  '</div>';
}

function _fmtOpt(fmt, label, desc) {
  return '<label class="export-fmt-opt" data-fmt="' + fmt + '" onclick="_selFmt(\'' + fmt + '\')">' +
    '<input type="radio" name="exp_fmt" value="' + fmt + '">' +
    '<div><div class="fo-label">' + label + '</div><div class="fo-desc">' + desc + '</div></div>' +
  '</label>';
}

function _selDest(dest) {
  _exportDest = dest;
  document.querySelectorAll('.export-dest-card').forEach(c => c.classList.toggle('selected', c.dataset.dest === dest));
  const btn = document.getElementById('exp_btn');
  if (btn) btn.textContent = dest === 'service' ? 'Next →' : 'Export';
}

function _selFmt(fmt) {
  _exportFormat = fmt;
  document.querySelectorAll('.export-fmt-opt').forEach(o => o.classList.toggle('selected', o.dataset.fmt === fmt));
  const radio = document.querySelector('.export-fmt-opt[data-fmt="' + fmt + '"] input');
  if (radio) radio.checked = true;
}

async function _advanceExport() {
  if (_exportDest === 'pc') {
    const statusEl = document.getElementById('exp_status');
    if (statusEl) statusEl.innerHTML = '<div class="import-status info">Preparing download…</div>';
    const btn = document.getElementById('exp_btn');
    if (btn) btn.disabled = true;
    try {
      await _execPcDownload(_exportMode);
      closeModal();
    } catch (e) {
      if (statusEl) statusEl.innerHTML = '<div class="import-status err">Error: ' + esc(e.message) + '</div>';
      if (btn) btn.disabled = false;
    }
  } else {
    // advance to step 2 — dataset service settings
    const suggested = suggestDatasetName();
    const t = targets[0];
    const targetOptions = targets.map(t => '<option value="' + esc(t.name) + '">' + esc(t.name) + '</option>').join('');
    const accessOptions = ['public', 'organization', 'task', 'private'].map(a =>
      '<option value="' + a + '"' + (a === t.default_access ? ' selected' : '') + '>' + a + '</option>'
    ).join('');
    const typeOptions = ['DATASET', 'BUCKET', 'TEMPLATE', 'BUNDLE', 'LORA_LEARNING'].map(v =>
      '<option value="' + v + '"' + (v === t.default_dataset_type ? ' selected' : '') + '>' + v + '</option>'
    ).join('');

    const selectedCount = document.querySelectorAll('.sel:checked').length;
    const totalCount = _exportMode === 's3'
      ? (selectedCount || document.querySelectorAll('.sel').length)
      : (selectedCount || currentEvents.length);
    const itemLabel = _exportMode === 's3' ? 'file' : 'event';
    const fmtLabel = { individual: 'individual files', jsonl: 'JSONL raw', jsonl_conv: 'JSONL conversations', catalog: 'catalog' };

    const optBadges = [
      _sampRules.length ? '⚗ ' + _sampRules.length + ' sampling rule(s)' : '',
      _expColMask       ? '⬛ column mask' : '',
      _expAssetResolve  ? '📎 asset resolution' : '',
    ].filter(Boolean);
    const optSummary = optBadges.length
      ? '<div class="hint" style="margin-top:6px;color:#1e40af;background:#eff6ff;padding:5px 8px;border-radius:4px">' + optBadges.map(esc).join(' · ') + '</div>'
      : '';
    document.querySelector('.modal-body').innerHTML =
      '<div class="import-form" id="imp_form">' +
        '<div class="hint" style="margin-top:0">' +
          esc(totalCount + ' ' + itemLabel + '(s) · ' + (fmtLabel[_exportFormat] || _exportFormat)) + ' → ' + esc(t.name) +
        '</div>' +
        optSummary +
        '<label>Target</label><select id="imp_target">' + targetOptions + '</select>' +
        '<label>Dataset name</label><input type="text" id="imp_name" value="' + esc(suggested) + '">' +
        '<label>Access</label><select id="imp_access">' + accessOptions + '</select>' +
        '<label>Dataset type</label><select id="imp_type">' + typeOptions + '</select>' +
        '<div id="imp_status"></div>' +
        '<div class="actions">' +
          '<button class="btn-secondary" onclick="showExportModal(_exportMode)">← Back</button>' +
          '<button class="btn-primary" id="imp_submit" onclick="_execServiceExport()">Export</button>' +
        '</div>' +
      '</div>';
  }
}

// ── PC download implementations ─────────────────────────────────────────────

async function _execPcDownload(mode) {
  switch (_exportFormat) {
    case 'individual':   return _dlZip(mode);
    case 'jsonl':        return _dlJsonl(mode);
    case 'jsonl_conv':   return _dlConversations(mode);
    case 'catalog':      return _dlCatalog(mode);
    default: throw new Error('Unknown format: ' + _exportFormat);
  }
}

async function _dlZip(mode) {
  if (typeof JSZip === 'undefined') throw new Error('JSZip library not loaded');
  const zip = new JSZip();
  if (mode === 's3') {
    const keys = _collectKeys('s3');
    if (!keys || !keys.length) throw new Error('No files selected');
    const groups = await _fetchFileEventsGrouped(keys);
    for (const g of groups) {
      const masked = _applyColMask(g.events);
      zip.file(g.filename, JSON.stringify(masked, null, 2));
    }
  } else {
    const events = _applyColMask(getSelectedEvents());
    if (!events.length) throw new Error('No results to export');
    events.forEach((ev, i) => {
      const id = ev.id || ev.trace_id || String(i);
      zip.file('event-' + String(i).padStart(4, '0') + '-' + String(id).slice(0, 20).replace(/[^a-zA-Z0-9_-]/g, '_') + '.json',
        JSON.stringify(ev, null, 2));
    });
  }
  const blob = await zip.generateAsync({ type: 'blob', compression: 'DEFLATE' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a'); a.href = url; a.download = 'export.zip'; a.click();
  URL.revokeObjectURL(url);
}

async function _dlJsonl(mode) {
  let events;
  if (mode === 's3') {
    const keys = _collectKeys('s3');
    if (!keys || !keys.length) throw new Error('No files selected');
    events = await fetchFileEvents(keys);
  } else {
    events = getSelectedEvents();
    if (!events.length) throw new Error('No results to export');
  }
  events = _applyColMask(events);
  const lines = events.map(e => JSON.stringify(e)).join('\n') + '\n';
  downloadFile(lines, 'events.jsonl', 'application/x-ndjson');
}

async function _dlConversations(mode) {
  let events;
  if (mode === 's3') {
    const keys = _collectKeys('s3');
    if (!keys || !keys.length) throw new Error('No files selected');
    events = await fetchFileEvents(keys);
  } else {
    events = getSelectedEvents();
    if (!events.length) throw new Error('No results to export');
  }
  const conv = eventsToDataset(events);
  if (!conv.length) throw new Error('No valid conversations found (need input + output messages)');
  downloadFile(conv.map(c => JSON.stringify(c)).join('\n') + '\n', 'conversations.jsonl', 'application/x-ndjson');
}

async function _dlCatalog(mode) {
  if (mode === 's3') {
    const keys = _collectKeys('s3');
    if (!keys || !keys.length) throw new Error('No files selected');
    const files = await _fetchPresignedUrls(keys);
    const lines = files.map(f => JSON.stringify({ key: f.key, url: f.url })).join('\n') + '\n';
    downloadFile(lines, 'catalog.jsonl', 'application/x-ndjson');
  } else {
    const events = getSelectedEvents();
    if (!events.length) throw new Error('No results to export');
    downloadFile(JSON.stringify(events, null, 2), 'catalog.json', 'application/json');
  }
}

// ── Dataset service upload ───────────────────────────────────────────────────

async function _execServiceExport() {
  const name = document.getElementById('imp_name').value.trim();
  const statusDiv = document.getElementById('imp_status');
  if (!name) {
    statusDiv.innerHTML = '<div class="import-status err">Dataset name is required</div>';
    return;
  }

  const mode = _exportMode;
  const fmt = _exportFormat;

  // Validate we have data to export
  if (mode === 's3') {
    const keys = _collectKeys('s3');
    if (!keys || !keys.length) {
      statusDiv.innerHTML = '<div class="import-status err">No files selected</div>';
      return;
    }
  } else {
    if (!currentEvents.length) {
      statusDiv.innerHTML = '<div class="import-status err">No results to import</div>';
      return;
    }
  }

  const target = document.getElementById('imp_target').value;
  const access = document.getElementById('imp_access').value;
  const dataset_type = document.getElementById('imp_type').value;
  document.getElementById('imp_submit').disabled = true;
  statusDiv.innerHTML = '<div class="import-status info">Preparing export…</div>';

  const samplingPayload = _sampRules.length ? _sampRules.map(r => ({
    strategy: r.strategy, rate: parseFloat(r.rate) || 1,
    field: r.field || null, params: r.params || {},
  })) : null;
  // Read sampling config from the global Sampling panel (not injected in this modal)
  const strictSchema = document.getElementById('samp_strict') ? document.getElementById('samp_strict').checked : false;
  const maxTracesEl = document.getElementById('samp_max_traces');
  const maxTraces = maxTracesEl && maxTracesEl.value ? parseInt(maxTracesEl.value) : null;
  const schemaSnapshot = Object.keys(_sampSchema).length
    ? Object.fromEntries(Object.entries(_sampSchema).map(([k, v]) => [k, v.type]))
    : null;
  // Export options from the export panel
  const colMaskFields = _expColMask
    ? ((_schemaCache[activeDs.name] || {}).visibleColumns || null)
    : null;

  try {
    let resp, totalCount;

    if (mode === 's3' && fmt === 'individual') {
      // Native S3 path — worker downloads files from S3 directly (most efficient)
      const keys = _collectKeys('s3');
      totalCount = keys.length;
      resp = await fetch(BASE + '/api/public/export/dataset', {
        method: 'POST',
        headers: { 'Authorization': authHeader(), 'Content-Type': 'application/json' },
        body: JSON.stringify({
          target, datasource: activeDs.name, keys, dataset_name: name, access, dataset_type,
          sampling: samplingPayload, strict_schema: strictSchema,
          schema_snapshot: schemaSnapshot, max_traces: maxTraces,
          col_mask: colMaskFields, asset_resolve: _expAssetResolve || null,
        }),
      });
    } else {
      // Events path — browser fetches S3 content if needed, then sends events array
      let events;
      if (mode === 's3') {
        statusDiv.innerHTML = '<div class="import-status info">Fetching S3 files…</div>';
        const keys = _collectKeys('s3');
        if (fmt === 'catalog') {
          // Catalog for S3: upload presigned URL list
          const files = await _fetchPresignedUrls(keys);
          events = files.map(f => ({ key: f.key, url: f.url }));
        } else {
          events = await fetchFileEvents(keys);
          if (fmt === 'jsonl_conv') events = eventsToDataset(events);
        }
      } else {
        events = [...currentEvents];
        if (fmt === 'jsonl_conv') events = eventsToDataset(events);
      }
      // Apply column mask client-side before sending
      events = _applyColMask(events);
      totalCount = events.length;
      // Determine backend format: jsonl_conv already transformed → send as jsonl
      const backendFmt = (fmt === 'jsonl_conv') ? 'jsonl' : (fmt === 'catalog' && mode === 's3' ? 'catalog' : fmt);
      statusDiv.innerHTML = '<div class="import-status info">Queuing import…</div>';
      resp = await fetch(BASE + '/api/public/export/dataset/events', {
        method: 'POST',
        headers: { 'Authorization': authHeader(), 'Content-Type': 'application/json' },
        body: JSON.stringify({
          target, datasource: activeDs.name, events, dataset_name: name, access, dataset_type,
          format: backendFmt,
          sampling: samplingPayload, strict_schema: strictSchema,
          schema_snapshot: schemaSnapshot, max_traces: maxTraces,
          asset_resolve: _expAssetResolve || null,
        }),
      });
    }

    const data = await resp.json();
    if (!resp.ok) {
      statusDiv.innerHTML = '<div class="import-status err">Error: ' + esc(data.detail || resp.statusText) + '</div>';
      document.getElementById('imp_submit').disabled = false;
      return;
    }
    _showImportProgress(data.job_id, totalCount, data.warning);
    if (data.job_id === null) {
      _renderImportDone(data.progress, data.result);
    } else {
      _pollImportStatus(data.job_id);
    }
  } catch (e) {
    statusDiv.innerHTML = '<div class="import-status err">Error: ' + esc(e.message) + '</div>';
    document.getElementById('imp_submit').disabled = false;
  }
}

// ── Sampling state ──────────────────────────────────────────────────────────
let _sampSchema = {};      // { fieldName: {type, example} }
let _sampRules = [];       // array of rule objects
let _sampOpen = false;
let _sampMode = 's3';      // 's3' | 'events' — set by renderFilters based on active datasource
let _exportDest = 'pc';    // 'pc' | 'service'
let _exportFormat = 'jsonl'; // 'jsonl' | 'individual' | 'jsonl_conv' | 'catalog'
let _exportMode = 's3';    // 's3' | 'events' — set by showExportModal
let _expColMask = false;
let _expAssetResolve = false;
let _filterMode = 'and';
let _maskRules = [];       // [{ field, action, max_length }]
let _assetSources = [];    // ['field.path', ...]
let _assetFetchMode = 'metadata_only';
let _assetCheckAvail = false;
let _destTarget = '';
let _destDatasetName = '';
let _destAccess = 'organization';
let _destDatasetType = 'DATASET';
let _webhookUrl = '';
let _webhookHeadersRaw = '';
let _webhookSecret = '';
let _webhookTimeout = 30;
let _scheduleCron = '';
let _scheduleTz = 'UTC';
let _scheduleEnabled = true;

const STRATEGY_CATALOG = {
  random: {
    label: 'Random',
    requiredFields: [],
    extraParams: [
      { key: 'seed', label: 'Seed (optional)', type: 'number', placeholder: 'e.g. 42' },
    ],
    desc: {
      purpose: 'Baseline health monitoring.',
      best_for: 'Overall quality estimation, regression monitoring, discovering unknown unknowns.',
      advantages: 'Unbiased, simple, statistically meaningful.',
      weaknesses: 'Misses rare failures, inefficient use of annotation budget.',
    },
  },
  high_cost: {
    label: 'High-Cost',
    requiredFields: ['total_cost', 'totalCost', 'token_count', 'usage'],
    extraParams: [
      { key: 'percentile', label: 'Percentile', type: 'select', options: ['75','90','95','99'], default: '95' },
    ],
    desc: {
      purpose: 'Detect inefficient reasoning or tool usage.',
      signals: 'High token count, many tool calls, long chains, excessive retries.',
      best_for: 'Agent optimization, cost reduction, loop detection.',
      weaknesses: 'Expensive traces are not always bad.',
    },
  },
  latency_spike: {
    label: 'Latency Spike',
    requiredFields: ['latency', 'latency_ms', 'duration_ms'],
    extraParams: [
      { key: 'percentile', label: 'Percentile', type: 'select', options: ['75','90','95','99'], default: '95' },
    ],
    desc: {
      purpose: 'Diagnose slow reasoning.',
      signals: 'p95/p99 latency, stalled spans, slow retrieval, excessive planning.',
      best_for: 'Real-time agents, interactive assistants.',
      advantages: 'Connects quality with UX.',
    },
  },
  long_trace: {
    label: 'Long Trace',
    requiredFields: ['span_count', 'observation_count', 'depth'],
    extraParams: [
      { key: 'threshold', label: 'Min spans', type: 'number', placeholder: 'e.g. 10' },
    ],
    desc: {
      purpose: 'Detect loops and reasoning degradation.',
      signals: 'Many spans, deep recursion, repeated actions.',
      best_for: 'Autonomous agents, planning systems, tool-using agents.',
      weaknesses: 'Some legitimate workflows are naturally long.',
    },
  },
  failure: {
    label: 'Failure',
    requiredFields: ['level', 'error', 'status_code', 'is_error'],
    extraParams: [
      { key: 'values', label: 'Failure values', type: 'text', placeholder: 'error,failed,failure' },
    ],
    desc: {
      purpose: 'Capture error cases for debugging.',
      signals: 'Error flags, non-200 status codes, exception traces.',
      best_for: 'Reliability monitoring, root cause analysis.',
      weaknesses: 'May over-index on known error patterns.',
    },
  },
  user_dissatisfaction: {
    label: 'User Dissatisfaction',
    requiredFields: ['score', 'tags'],
    extraParams: [
      { key: 'threshold', label: 'Score threshold (below)', type: 'number', placeholder: 'e.g. 0' },
      { key: 'thumbsdown_tags', label: 'Negative tags', type: 'text', placeholder: 'thumbsdown,dislike' },
    ],
    desc: {
      purpose: 'Capture poor user experiences.',
      signals: 'Negative scores, thumbsdown tags, low ratings.',
      best_for: 'User-facing assistants, feedback-driven improvement.',
      advantages: 'Aligns annotation with user impact.',
    },
  },
  business_critical: {
    label: 'Business-Critical',
    requiredFields: ['tags', 'metadata'],
    extraParams: [
      { key: 'match_type', label: 'Match type', type: 'select', options: ['contains','equals'], default: 'contains' },
      { key: 'value', label: 'Value', type: 'text', placeholder: 'e.g. critical' },
    ],
    desc: {
      purpose: 'Ensure coverage of high-stakes interactions.',
      signals: 'User-defined tag or metadata field/value.',
      best_for: 'Regulated domains, SLA-bound workflows, VIP users.',
      advantages: 'Guarantees representation of important cases.',
    },
  },
  prompt_version_change: {
    label: 'Prompt/Version Change',
    requiredFields: ['version', 'model', 'prompt_hash', 'prompt_version'],
    extraParams: [
      { key: 'baseline', label: 'Baseline value', type: 'text', placeholder: 'e.g. gpt-4o' },
    ],
    desc: {
      purpose: 'Regression detection across prompt or model versions.',
      signals: 'Field differs from a specified baseline value.',
      best_for: 'A/B prompt testing, model upgrades, prompt iteration.',
      weaknesses: 'Only meaningful when version metadata is present.',
    },
  },
  low_confidence: {
    label: 'Low Confidence',
    requiredFields: ['confidence', 'logprob', 'score'],
    extraParams: [
      { key: 'threshold', label: 'Threshold (below)', type: 'number', placeholder: 'e.g. 0.5' },
    ],
    desc: {
      purpose: 'Capture uncertain model outputs for review.',
      signals: 'Confidence, logprob, or model-output score below threshold.',
      best_for: 'Classification tasks, RAG pipelines, uncertainty-aware systems.',
      weaknesses: 'Not all systems expose confidence; logprob is not always calibrated.',
    },
  },
  weird_tool_sequences: {
    label: 'Weird Tool Sequences',
    requiredFields: ['tool_calls', 'tools', 'tool_use', 'observations'],
    extraParams: [
      { key: 'max_repeat', label: 'Max same-tool repeats', type: 'number', placeholder: 'e.g. 3' },
      { key: 'min_total_calls', label: 'Min total tool calls', type: 'number', placeholder: 'e.g. 10' },
      { key: 'unexpected_tools', label: 'Unexpected tool names', type: 'text', placeholder: 'tool_a,tool_b' },
    ],
    desc: {
      purpose: 'Detect abnormal or pathological tool usage patterns.',
      signals: 'Same tool called repeatedly, excessive total calls, unexpected tool names.',
      best_for: 'Tool-using agents, ReAct loops, multi-step planners.',
      weaknesses: 'Requires structured tool call data; thresholds are system-specific.',
    },
  },
};

function _strategyAvailable(stratKey) {
  const def = STRATEGY_CATALOG[stratKey];
  if (!def || !def.requiredFields || def.requiredFields.length === 0) return true;
  return def.requiredFields.some(f => _sampSchema[f] !== undefined);
}

function _availableFieldsForStrategy(stratKey) {
  const def = STRATEGY_CATALOG[stratKey];
  if (!def || !def.requiredFields || def.requiredFields.length === 0) return Object.keys(_sampSchema);
  const matching = def.requiredFields.filter(f => _sampSchema[f] !== undefined);
  return matching.length ? matching : Object.keys(_sampSchema);
}

function _renderStrategyDesc(def) {
  const d = def.desc;
  const rows = [
    d.purpose   ? '<li><b>Purpose:</b> ' + esc(d.purpose) + '</li>' : '',
    d.signals   ? '<li><b>Signals:</b> ' + esc(d.signals) + '</li>' : '',
    d.best_for  ? '<li><b>Best for:</b> ' + esc(d.best_for) + '</li>' : '',
    d.advantages? '<li><b>Advantages:</b> ' + esc(d.advantages) + '</li>' : '',
    d.weaknesses? '<li><b>Weaknesses:</b> ' + esc(d.weaknesses) + '</li>' : '',
  ].filter(Boolean).join('');
  return '<div class="samp-rule-desc"><details><summary>What is this?</summary><ul>' + rows + '</ul></details></div>';
}

function _renderFieldSelect(ruleIdx, selectedField, stratKey) {
  const fields = _availableFieldsForStrategy(stratKey);
  if (!fields.length) return '';
  const opts = fields.map(f =>
    '<option value="' + esc(f) + '"' + (f === selectedField ? ' selected' : '') + '>' + esc(f) + '</option>'
  ).join('');
  return '<div class="fp"><label>Field</label><select onchange="_sampRules[' + ruleIdx + '].field=this.value">' + opts + '</select></div>';
}

function _renderExtraParams(ruleIdx, rule, def) {
  if (!def.extraParams || !def.extraParams.length) return '';
  return def.extraParams.map(p => {
    const val = rule.params[p.key] !== undefined ? rule.params[p.key] : (p.default || '');
    const onChange = '_sampRules[' + ruleIdx + '].params["' + p.key + '"]=this.value';
    if (p.type === 'select') {
      const opts = p.options.map(o => '<option value="' + o + '"' + (String(val) === o ? ' selected' : '') + '>' + o + '</option>').join('');
      return '<div class="fp"><label>' + esc(p.label) + '</label><select onchange="' + onChange + '">' + opts + '</select></div>';
    }
    return '<div class="fp"><label>' + esc(p.label) + '</label><input type="' + p.type + '" value="' + esc(String(val)) + '" placeholder="' + esc(p.placeholder || '') + '" onchange="' + onChange + '"></div>';
  }).join('');
}

function _renderRules() {
  if (!_sampRules.length) {
    document.getElementById('samp_rules').innerHTML = '<div style="font-size:.8rem;color:#aaa;padding:4px 0">No strategies added — all traces will be exported.</div>';
    _renderYield();
    _updateSampBadge();
    return;
  }
  const html = _sampRules.map((rule, i) => {
    const def = STRATEGY_CATALOG[rule.strategy];
    if (!def) return '';
    const avail = _strategyAvailable(rule.strategy);
    const needsField = def.requiredFields && def.requiredFields.length > 0;
    return '<div class="samp-rule' + (avail ? '' : ' unavail') + '">' +
      '<div class="samp-rule-header">' +
        '<span class="samp-rule-name">' + esc(def.label) + (avail ? '' : ' <span style="font-weight:400;color:#f59e0b;font-size:.73rem">⚠ field not in schema</span>') + '</span>' +
        '<div class="samp-rule-rate">' +
          '<input type="number" min="1" max="100" value="' + rule.rate + '" onchange="_sampRules[' + i + '].rate=parseFloat(this.value)||1;_renderYield()">' +
          '<span>%</span>' +
        '</div>' +
        '<button class="samp-rule-remove" onclick="removeSampRule(' + i + ')" title="Remove">✕</button>' +
      '</div>' +
      '<div class="samp-rule-params">' +
        (needsField ? _renderFieldSelect(i, rule.field, rule.strategy) : '') +
        _renderExtraParams(i, rule, def) +
      '</div>' +
      _renderStrategyDesc(def) +
    '</div>';
  }).join('');
  document.getElementById('samp_rules').innerHTML = html;
  _renderYield();
  _updateSampBadge();
}

function _renderYield() {
  const el = document.getElementById('samp_yield');
  if (!el) return;
  if (!_sampRules.length) { el.style.display = 'none'; return; }
  const totalRate = _sampRules.reduce((s, r) => s + (parseFloat(r.rate) || 0), 0);
  const est = Math.min(100, Math.round(totalRate * 0.85));
  const isHigh = est >= 95;
  el.style.display = 'block';
  el.className = 'samp-yield' + (isHigh ? ' warn' : '');
  el.textContent = isHigh
    ? '⚠ Est. yield ≈ ' + est + '% — config may include nearly all traces'
    : 'Est. yield ≈ ' + est + '% of input traces (rough estimate; deduped)';
}

function addSampRule() {
  const sel = document.getElementById('samp_strat_select');
  const stratKey = sel.value;
  const def = STRATEGY_CATALOG[stratKey];
  if (!def) return;
  const fields = _availableFieldsForStrategy(stratKey);
  _sampRules.push({ strategy: stratKey, rate: 10, field: fields[0] || null, params: {} });
  _renderRules();
}

function removeSampRule(i) {
  _sampRules.splice(i, 1);
  _renderRules();
}

function toggleSampling() {
  _sampOpen = !_sampOpen;
  const btn = document.getElementById('samp_btn');
  const panel = document.getElementById('samp_panel');
  if (_sampOpen) {
    panel.style.display = 'block';
    // Populate strategy select on first open
    const sel = document.getElementById('samp_strat_select');
    if (sel && !sel.options.length) _initSampStratSelect();
    if (!Object.keys(_sampSchema).length) _loadSampSchema();
  } else {
    panel.style.display = 'none';
  }
  _updateSampBadge();
}

function _initSampStratSelect() {
  const sel = document.getElementById('samp_strat_select');
  if (!sel) return;
  sel.innerHTML = Object.entries(STRATEGY_CATALOG).map(([k, v]) => {
    const avail = _strategyAvailable(k);
    return '<option value="' + k + '">' + esc(v.label) + (avail ? '' : ' (field missing)') + '</option>';
  }).join('');
}

function _updateSampBadge() {
  const btn = document.getElementById('samp_btn');
  if (!btn) return;
  const count = _sampRules.length;
  const arrow = _sampOpen ? '▲' : '▼';
  if (count) {
    btn.classList.add('active');
    btn.innerHTML = '⚗ Sampling <span class="filter-badge">' + count + '</span> ' + arrow;
  } else {
    btn.classList.remove('active');
    btn.textContent = '⚗ Sampling ' + arrow;
  }
}

async function _loadSampSchema() {
  const schemaArea = document.getElementById('samp_schema_area');
  schemaArea.innerHTML = '<span style="color:#aaa;font-size:.78rem">Loading schema…</span>';
  try {
    const params = new URLSearchParams({ datasource: activeDs.name });
    if (_sampMode === 's3') {
      // Use currently visible S3 keys (from result table) for schema sampling
      const keys = _collectKeys('s3') || [];
      keys.slice(0, 5).forEach(k => params.append('keys', k));
    }
    // pass active time range if available
    const fromEl = document.getElementById('time_from');
    const toEl   = document.getElementById('time_to');
    if (fromEl && fromEl.value) params.set('start', new Date(fromEl.value).toISOString());
    if (toEl   && toEl.value)   params.set('end',   new Date(toEl.value).toISOString());

    const resp = await fetch(BASE + '/api/public/datasource/sample?' + params, {
      headers: { 'Authorization': authHeader() },
    });
    if (!resp.ok) throw new Error((await resp.json().catch(() => ({}))).detail || resp.statusText);
    const data = await resp.json();
    _sampSchema = data.fields || {};
    _renderSchemaChips();
    document.getElementById('samp_options').style.display = 'block';
    // Re-populate strategy select now that we have schema info
    _initSampStratSelect();
  } catch (e) {
    schemaArea.innerHTML = '<span style="color:#dc2626;font-size:.78rem">Schema load failed: ' + esc(e.message) + '</span>';
  }
}

function _renderSchemaChips() {
  const schemaArea = document.getElementById('samp_schema_area');
  const keys = Object.keys(_sampSchema);
  if (!keys.length) {
    schemaArea.innerHTML = '<div class="samp-schema"><span class="samp-schema-empty">No fields discovered</span></div>';
    return;
  }
  const chips = keys.map(f => {
    const info = _sampSchema[f];
    return '<span class="samp-field-chip" title="' + esc(info.type) + (info.example !== null ? ': ' + esc(String(info.example)) : '') + '">' + esc(f) + ' <span style="opacity:.6">' + esc(info.type) + '</span></span>';
  }).join('');
  schemaArea.innerHTML = '<div class="samp-schema"><b>Schema</b> (' + keys.length + ' fields from ' + (activeDs ? esc(activeDs.name) : '') + '):<div class="samp-schema-fields">' + chips + '</div></div>';
}


function _collectKeys(mode) {
  if (mode === 's3') {
    const checked = [...document.querySelectorAll('.sel:checked')].map(c => c.value);
    return checked.length ? checked : [...document.querySelectorAll('.sel')].map(c => c.value);
  }
  if (!currentSearchKeys.length) return null;
  const checked = [...document.querySelectorAll('.sel:checked')];
  return checked.length
    ? checked.map(c => currentSearchKeys[parseInt(c.dataset.idx)]).filter(Boolean)
    : [...currentSearchKeys];
}

function _showImportProgress(jobId, totalFiles, warning) {
  const warningHtml = warning
    ? '<div class="import-status" style="background:#fef9c3;color:#854d0e;margin-bottom:10px;font-size:.8rem">' +
        '&#9888; ' + esc(warning) + '</div>'
    : '';
  document.querySelector('.modal-body').innerHTML =
    '<div class="import-progress" id="imp_progress">' +
      warningHtml +
      '<div id="imp_badge" class="status-badge status-queued">Queued</div>' +
      '<div class="progress-track"><div class="progress-fill" id="imp_bar" style="width:0%"></div></div>' +
      '<div class="progress-label">' +
        '<span id="imp_files">0 / ' + totalFiles + ' files</span>' +
        '<span id="imp_bytes">0 B</span>' +
      '</div>' +
      '<div class="progress-dataset" id="imp_result"></div>' +
      '<div class="actions" style="margin-top:16px">' +
        '<button class="btn-secondary" onclick="closeModal()">Close</button>' +
      '</div>' +
    '</div>';
}

function _renderImportDone(progress, result) {
  const badge = document.getElementById('imp_badge');
  if (!badge) return;
  badge.className = 'status-badge status-complete';
  badge.textContent = 'Complete';
  if (progress) {
    const pct = progress.files_total > 0 ? Math.round(progress.files_done / progress.files_total * 100) : 100;
    document.getElementById('imp_bar').style.width = pct + '%';
    const unitLabel = result && result.unit ? result.unit + '(s)' : 'files';
    document.getElementById('imp_files').textContent = progress.files_done + ' / ' + progress.files_total + ' ' + unitLabel;
    document.getElementById('imp_bytes').textContent = _fmtBytes(progress.bytes_done);
  }
  if (result) {
    const unitLabel = result.unit ? result.unit + '(s)' : 'file(s)';
    let msg = 'Dataset <b>' + esc(result.dataset_id) + '</b> — ' + result.files_uploaded + ' ' + unitLabel + ' uploaded';
    if (result.files_failed) msg += ', <span style="color:#dc2626">' + result.files_failed + ' failed</span>';
    document.getElementById('imp_result').innerHTML = msg;
  }
}

function _fmtBytes(b) {
  if (b < 1024) return b + ' B';
  if (b < 1048576) return (b / 1024).toFixed(1) + ' KB';
  return (b / 1048576).toFixed(1) + ' MB';
}

async function _pollImportStatus(jobId) {
  try {
    const resp = await fetch(BASE + '/api/public/export/status/' + jobId, {
      headers: { 'Authorization': authHeader() },
    });
    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      _setImportFailed('Poll error: ' + (err.detail || resp.statusText));
      return;
    }
    const data = await resp.json();

    const badge = document.getElementById('imp_badge');
    if (!badge) return; // modal was closed

    const statusLabels = { queued: 'Queued', in_progress: 'Uploading…', complete: 'Complete', failed: 'Failed', deferred: 'Deferred' };
    badge.className = 'status-badge status-' + data.status;
    badge.textContent = statusLabels[data.status] || data.status;

    if (data.progress) {
      const p = data.progress;
      const pct = p.files_total > 0 ? Math.round(p.files_done / p.files_total * 100) : 0;
      const unitLabel = data.result && data.result.unit ? data.result.unit + '(s)' : 'files';
      document.getElementById('imp_bar').style.width = pct + '%';
      document.getElementById('imp_files').textContent = p.files_done + ' / ' + p.files_total + ' ' + unitLabel;
      document.getElementById('imp_bytes').textContent = _fmtBytes(p.bytes_done);
    }

    if (data.status === 'complete') {
      _renderImportDone(data.progress, data.result);
      return;
    }

    if (data.status === 'failed') {
      _setImportFailed(data.error || 'Unknown error');
      return;
    }

    setTimeout(() => _pollImportStatus(jobId), 2000);
  } catch (e) {
    const badge = document.getElementById('imp_badge');
    if (badge) setTimeout(() => _pollImportStatus(jobId), 3000); // retry on network blip
  }
}

function _setImportFailed(msg) {
  const badge = document.getElementById('imp_badge');
  if (!badge) return;
  badge.className = 'status-badge status-failed';
  badge.textContent = 'Failed';
  document.getElementById('imp_result').innerHTML = '<span style="color:#dc2626">' + esc(msg) + '</span>';
}

function showModal(title, content) {
  const modal = document.getElementById('modal');
  modal.innerHTML =
    '<div class="modal-overlay" onclick="if(event.target===this)closeModal()">' +
    '<div class="modal"><div class="modal-header"><h2>' + esc(title) + '</h2>' +
    '<button class="modal-close" onclick="closeModal()">&times;</button></div>' +
    '<div class="modal-body"><pre>' + esc(content) + '</pre></div></div></div>';
}

function closeModal() { document.getElementById('modal').innerHTML = ''; }

// ══════════════════════════════════════════
//  YAML config — download / upload
// ══════════════════════════════════════════

// Map internal filter op codes ↔ human-readable YAML names
const OP_TO_YAML = {
  gt: 'greater_than', lt: 'less_than', gte: 'greater_than_or_equal', lte: 'less_than_or_equal',
  eq: 'equals', neq: 'not_equals',
  contains: 'contains', not_contains: 'not_contains', starts_with: 'starts_with',
  in: 'in', not_in: 'not_in',
  is_null: 'is_empty', not_null: 'not_empty',
};
const OP_FROM_YAML = Object.fromEntries(Object.entries(OP_TO_YAML).map(([k, v]) => [v, k]));

const TIME_PRESET_LABELS = {
  '15m': 'Last 15 minutes', '1h': 'Last 1 hour', '4h': 'Last 4 hours',
  '24h': 'Last 24 hours', '7d': 'Last 7 days', '30d': 'Last 30 days', 'all': 'All time',
};

function _buildExportConfig() {
  const cfg = {};

  // ── ingestion ──────────────────────────────────────────────────────────────
  if (activeDs) {
    cfg.ingestion = { datasource: activeDs.name, type: activeDs.type };
  }

  // ── filters ────────────────────────────────────────────────────────────────
  const filterRules = [];
  // Time range → first rule with op: last / between
  const timeRange = getTimeRange();
  if (timePreset && timePreset !== 'all' && PRESETS[timePreset]) {
    filterRules.push({ field: _timeField || 'created_at', op: 'last', value: timePreset });
  } else if (customFrom && customTo) {
    filterRules.push({ field: _timeField || 'created_at', op: 'between', from: customFrom, to: customTo });
  }
  // Backend filter rules
  for (const r of _filterRules) {
    const op = OP_TO_YAML[r.op] || r.op;
    const isListOp = op === 'in' || op === 'not_in';
    const value = isListOp
      ? String(r.value || '').split(',').map(s => s.trim()).filter(Boolean)
      : r.value;
    filterRules.push({ field: r.field, op, value });
  }
  if (filterRules.length) {
    cfg.filters = { mode: _filterMode, rules: filterRules };
  }

  // ── masking ────────────────────────────────────────────────────────────────
  if (_expColMask || _maskRules.length) {
    const cache = activeDs && _schemaCache[activeDs.name];
    const cols = (cache && cache.visibleColumns) || [];
    cfg.masking = {
      default_policy: _expColMask ? 'deny' : 'allow',
      allow_fields: _expColMask ? cols : [],
      rules: _maskRules.map(r => {
        const entry = { field: r.field, action: r.action };
        if (r.action === 'truncate' && r.max_length) entry.max_length = r.max_length;
        return entry;
      }),
    };
  }

  // ── asset_resolution ───────────────────────────────────────────────────────
  if (_expAssetResolve) {
    const ar = { enabled: true };
    if (_assetSources.length) ar.sources = _assetSources.map(f => ({ field: f }));
    ar.fetch_mode = _assetFetchMode;
    if (_assetCheckAvail) ar.check_availability = true;
    cfg.asset_resolution = ar;
  }

  // ── destination ────────────────────────────────────────────────────────────
  if (_destTarget || _destDatasetName) {
    const dest = {};
    if (_destTarget) dest.datasource = _destTarget;
    if (_destDatasetName) dest.dataset_name = _destDatasetName;
    if (_destAccess !== 'organization') dest.access = _destAccess;
    if (_destDatasetType !== 'DATASET') dest.dataset_type = _destDatasetType;
    cfg.destination = dest;
  }

  // ── webhook ────────────────────────────────────────────────────────────────
  if (_webhookUrl.trim()) {
    const wh = { url: _webhookUrl.trim() };
    if (_webhookHeadersRaw.trim()) {
      const headers = {};
      for (const line of _webhookHeadersRaw.split('\n')) {
        const idx = line.indexOf(':');
        if (idx > 0) headers[line.slice(0, idx).trim()] = line.slice(idx + 1).trim();
      }
      if (Object.keys(headers).length) wh.headers = headers;
    }
    if (_webhookSecret.trim()) wh.secret = _webhookSecret.trim();
    if (_webhookTimeout !== 30) wh.timeout_seconds = _webhookTimeout;
    cfg.webhook = wh;
  }

  // ── schedule ───────────────────────────────────────────────────────────────
  if (_scheduleCron.trim()) {
    cfg.schedule = { cron: _scheduleCron.trim() };
    if (_scheduleTz && _scheduleTz !== 'UTC') cfg.schedule.timezone = _scheduleTz;
    if (!_scheduleEnabled) cfg.schedule.enabled = false;
  }

  // ── sampling ───────────────────────────────────────────────────────────────
  if (_sampRules.length) {
    const strictEl = document.getElementById('samp_strict');
    const maxEl = document.getElementById('samp_max_traces');
    const strictSchema = strictEl ? strictEl.checked : false;
    const maxTraces = maxEl && maxEl.value ? parseInt(maxEl.value) : null;

    const strategyName = _sampRules.length === 1 ? _sampRules[0].strategy : 'hybrid';
    const sampEntry = {
      strategy: strategyName,
      rules: _sampRules.map(r => {
        const entry = { type: r.strategy, rate: Math.round((parseFloat(r.rate) || 1) / 100 * 1000) / 1000 };
        if (r.field) entry.field = r.field;
        if (r.params && Object.keys(r.params).length) entry.params = r.params;
        return entry;
      }),
    };
    if (strictSchema) sampEntry.strict_schema = true;
    if (maxTraces) sampEntry.max_traces = maxTraces;
    cfg.sampling = sampEntry;
  }

  return cfg;
}

function downloadConfigYaml() {
  if (typeof jsyaml === 'undefined') { alert('js-yaml library not loaded'); return; }
  const cfg = _buildExportConfig();
  const yaml = jsyaml.dump(cfg, { indent: 2, lineWidth: 100, quotingType: '"' });
  downloadFile(yaml, 'dataimporter-config.yaml', 'text/yaml');
}

function _onYamlFileSelected(input) {
  const file = input.files[0];
  if (!file) return;
  input.value = '';   // reset so same file can be re-uploaded
  const reader = new FileReader();
  reader.onload = e => _applyConfigYaml(e.target.result);
  reader.readAsText(file);
}

async function _applyConfigYaml(yamlText) {
  if (typeof jsyaml === 'undefined') { alert('js-yaml library not loaded'); return; }
  let cfg;
  try { cfg = jsyaml.load(yamlText); }
  catch (e) { alert('Invalid YAML: ' + e.message); return; }
  if (!cfg || typeof cfg !== 'object') { alert('Config must be a YAML object'); return; }

  // ── ingestion — switch datasource if specified ─────────────────────────────
  if (cfg.ingestion && cfg.ingestion.datasource) {
    const tab = findTab(cfg.ingestion.datasource);
    if (tab) {
      selectDatasource(tab);
      await new Promise(r => setTimeout(r, 80));  // let DOM settle
    } else {
      console.warn('[config] datasource not found:', cfg.ingestion.datasource);
    }
  }

  // ── filters ────────────────────────────────────────────────────────────────
  if (cfg.filters) {
    _filterMode = cfg.filters.mode === 'or' ? 'or' : 'and';
    _updateFilterModeUI();
    if (Array.isArray(cfg.filters.rules)) {
      _filterRules = [];
      for (const r of cfg.filters.rules) {
        if (r.op === 'last') {
          const preset = String(r.value || '').toLowerCase();
          if (PRESETS[preset] !== undefined) {
            timePreset = preset; customFrom = null; customTo = null;
            const lbl = document.getElementById('timeLabel');
            if (lbl) lbl.textContent = TIME_PRESET_LABELS[preset] || preset;
            document.querySelectorAll('#timePresets div').forEach(d => {
              d.classList.toggle('active', d.textContent.trim() === (TIME_PRESET_LABELS[preset] || preset));
            });
          }
        } else if (r.op === 'between') {
          customFrom = r.from || null; customTo = r.to || null; timePreset = null;
          const lbl = document.getElementById('timeLabel');
          if (lbl && customFrom && customTo)
            lbl.textContent = customFrom.substring(0, 10) + ' → ' + customTo.substring(0, 10);
        } else {
          const uiOp = OP_FROM_YAML[r.op] || r.op;
          const value = Array.isArray(r.value) ? r.value.join(', ') : (r.value || '');
          _filterRules.push({ field: r.field, op: uiOp, value });
        }
      }
      if (_filterRules.length) {
        const fp = document.getElementById('filter_panel');
        if (fp) { fp.style.display = ''; _filtersOpen = true; }
        _renderFilterRules();
        _updateFilterToggleBtn();
        _updateFilterModeUI();
      }
    }
  }

  // ── masking ────────────────────────────────────────────────────────────────
  if (cfg.masking) {
    const deny = cfg.masking.default_policy === 'deny';
    _expColMask = deny;
    const ecm = document.getElementById('exp_col_mask');
    if (ecm) ecm.checked = deny;
    if (deny && Array.isArray(cfg.masking.allow_fields) && cfg.masking.allow_fields.length) {
      const dsName = activeDs ? activeDs.name : null;
      if (dsName) {
        if (!_schemaCache[dsName]) _schemaCache[dsName] = { fields: {}, defaultColumns: [], visibleColumns: [] };
        _schemaCache[dsName].visibleColumns = cfg.masking.allow_fields;
        _saveColumns(dsName, cfg.masking.allow_fields);
      }
    }
    if (Array.isArray(cfg.masking.rules)) {
      _maskRules = cfg.masking.rules.map(r => ({ field: r.field, action: r.action || 'remove', max_length: r.max_length || null }));
      _renderMaskRules();
    }
  }

  // ── sampling ───────────────────────────────────────────────────────────────
  if (cfg.sampling && Array.isArray(cfg.sampling.rules) && cfg.sampling.rules.length) {
    _sampRules = cfg.sampling.rules.map(r => ({
      strategy: r.type,
      rate: Math.round((parseFloat(r.rate) || 0.1) * 100 * 10) / 10,
      field: r.field || null,
      params: r.params || {},
    }));
    const strictEl = document.getElementById('samp_strict');
    if (strictEl) strictEl.checked = !!cfg.sampling.strict_schema;
    const maxEl = document.getElementById('samp_max_traces');
    if (maxEl) maxEl.value = cfg.sampling.max_traces || '';
    if (!_sampOpen) toggleSampling(); else _renderRules();
  }

  // ── asset_resolution ───────────────────────────────────────────────────────
  if (cfg.asset_resolution) {
    _expAssetResolve = !!cfg.asset_resolution.enabled;
    const ear = document.getElementById('exp_asset_resolve');
    if (ear) ear.checked = _expAssetResolve;
    if (_expAssetResolve) {
      if (Array.isArray(cfg.asset_resolution.sources)) {
        _assetSources = cfg.asset_resolution.sources.map(s => s.field || s).filter(Boolean);
      }
      _assetFetchMode = cfg.asset_resolution.fetch_mode || 'metadata_only';
      _assetCheckAvail = !!cfg.asset_resolution.check_availability;
      _toggleAssetPanel(true);
      const fmSel = document.getElementById('asset_fetch_mode');
      if (fmSel) fmSel.value = _assetFetchMode;
      const cavEl = document.getElementById('asset_check_avail');
      if (cavEl) cavEl.checked = _assetCheckAvail;
    }
  }

  // ── destination ────────────────────────────────────────────────────────────
  if (cfg.destination) {
    _destTarget = cfg.destination.datasource || '';
    _destDatasetName = cfg.destination.dataset_name || '';
    _destAccess = cfg.destination.access || 'organization';
    _destDatasetType = cfg.destination.dataset_type || 'DATASET';
    const dt = document.getElementById('dest_target'); if (dt) dt.value = _destTarget;
    const dn = document.getElementById('dest_dataset_name'); if (dn) dn.value = _destDatasetName;
    const da = document.getElementById('dest_access'); if (da) da.value = _destAccess;
    const ddt = document.getElementById('dest_dataset_type'); if (ddt) ddt.value = _destDatasetType;
  }

  // ── webhook ────────────────────────────────────────────────────────────────
  if (cfg.webhook) {
    _webhookUrl = cfg.webhook.url || '';
    _webhookSecret = cfg.webhook.secret || '';
    _webhookTimeout = cfg.webhook.timeout_seconds || 30;
    if (cfg.webhook.headers && typeof cfg.webhook.headers === 'object') {
      _webhookHeadersRaw = Object.entries(cfg.webhook.headers).map(([k, v]) => k + ': ' + v).join('\n');
    }
    const wu = document.getElementById('webhook_url'); if (wu) wu.value = _webhookUrl;
    const wh = document.getElementById('webhook_headers'); if (wh) wh.value = _webhookHeadersRaw;
    const ws = document.getElementById('webhook_secret'); if (ws) ws.value = _webhookSecret;
    const wt = document.getElementById('webhook_timeout'); if (wt) wt.value = _webhookTimeout;
  }

  // ── schedule ───────────────────────────────────────────────────────────────
  if (cfg.schedule) {
    _scheduleCron = cfg.schedule.cron || '';
    _scheduleTz = cfg.schedule.timezone || 'UTC';
    _scheduleEnabled = cfg.schedule.enabled !== false;
    const sc = document.getElementById('schedule_cron'); if (sc) sc.value = _scheduleCron;
    const st = document.getElementById('schedule_tz'); if (st) st.value = _scheduleTz;
    const se = document.getElementById('schedule_enabled'); if (se) se.checked = _scheduleEnabled;
  }
}

// Boot
init();
</script>
