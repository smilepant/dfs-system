'use strict';

/* DFS Console: talks only to the metadata server API; every number shown comes from it. */

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

const PAGES = {
  overview: 'Overview',
  files: 'Files',
  integrity: 'Integrity',
  nodes: 'Storage nodes',
  activity: 'Activity',
};
const POLL_MS = 15000;
const VERIFY_TIMEOUT_MS = 10 * 60 * 1000;
const CLIENT_HASH_LIMIT = 512 * 1024 * 1024; // skip in-browser checksum above this size

const state = {
  page: 'overview',
  loaded: false,
  connected: null,
  stats: null,
  nodes: [],
  files: [],
  checks: new Map(),   // file id -> latest integrity check
  ops: [],
  activityOps: null,   // filtered activity list (null = use state.ops)
  busy: new Map(),      // file id -> 'verify' | 'repair' while a request is running
  repairs: new Map(),   // file id -> latest repair result from this session
  verifyAll: null,     // { cancelled: bool }
  drawer: null,        // { kind: 'file' | 'report', id }
  uploads: [],
  uploading: false,
  filters: { search: '', status: '', integrity: '', sort: 'created', dir: 'desc' },
};

/* ---------- Server URL ---------- */

const SERVER_KEY = 'dfs.server';
function defaultServer() {
  return /^https?:$/.test(location.protocol) ? location.origin : 'http://localhost:8000';
}
function readStoredServer() {
  try { return localStorage.getItem(SERVER_KEY); } catch { return null; }
}
let server = readStoredServer() || defaultServer();

function setServer(url) {
  server = url.replace(/\/+$/, '');
  try {
    if (server === defaultServer()) localStorage.removeItem(SERVER_KEY);
    else localStorage.setItem(SERVER_KEY, server);
  } catch { /* storage unavailable: keep for this session only */ }
  $('#server-label').textContent = server.replace(/^https?:\/\//, '');
  $('#server-label').title = server;
}

/* ---------- API ---------- */

class ApiError extends Error {
  constructor(message, status) { super(message); this.status = status; }
}

async function api(path, { method = 'GET', timeout = 15000 } = {}) {
  let res;
  try {
    res = await fetch(server + path, { method, signal: AbortSignal.timeout(timeout) });
  } catch (e) {
    throw new ApiError(e.name === 'TimeoutError' ? 'The request timed out.' : 'Cannot reach the metadata server.', 0);
  }
  if (!res.ok) {
    let msg = '';
    try { msg = (await res.json()).error; } catch { /* non-JSON error body */ }
    throw new ApiError(msg || `Request failed with HTTP ${res.status}.`, res.status);
  }
  return res.json();
}

/* ---------- Formatting ---------- */

function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function fmtBytes(n) {
  if (n == null || isNaN(n)) return '—';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  let v = Number(n);
  while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
  return (i === 0 ? v : v.toFixed(v < 10 ? 2 : 1)) + ' ' + units[i];
}

const fmtNum = n => Number(n ?? 0).toLocaleString();

function fmtTime(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  return isNaN(d) ? '—' : d.toLocaleString();
}

function relTime(iso) {
  if (!iso) return '—';
  const d = new Date(iso);
  if (isNaN(d)) return '—';
  const s = Math.round((Date.now() - d.getTime()) / 1000);
  if (s < 45) return 'just now';
  if (s < 3600) return `${Math.round(s / 60)} min ago`;
  if (s < 86400) return `${Math.round(s / 3600)} h ago`;
  if (s < 7 * 86400) return `${Math.round(s / 86400)} d ago`;
  return d.toLocaleDateString();
}

function timeCell(iso) {
  return `<time datetime="${esc(iso)}" title="${esc(fmtTime(iso))}">${esc(relTime(iso))}</time>`;
}

const shortHash = h => (h ? `${h.slice(0, 10)}…${h.slice(-6)}` : '');

const TYPE_GROUPS = {
  image: ['png', 'jpg', 'jpeg', 'gif', 'svg', 'webp', 'bmp', 'ico', 'tif', 'tiff'],
  doc: ['pdf', 'doc', 'docx', 'txt', 'md', 'rtf', 'odt', 'ppt', 'pptx'],
  sheet: ['xls', 'xlsx', 'csv', 'ods', 'tsv'],
  archive: ['zip', 'tar', 'gz', 'tgz', 'rar', '7z', 'bz2', 'xz', 'iso'],
  media: ['mp4', 'mkv', 'avi', 'mov', 'webm', 'mp3', 'wav', 'flac', 'ogg', 'm4a'],
  code: ['js', 'ts', 'go', 'py', 'java', 'c', 'cpp', 'h', 'rs', 'sh', 'json', 'yaml', 'yml', 'xml', 'html', 'css', 'sql', 'exe', 'bin', 'dll'],
};
function fileType(name) {
  const dot = (name || '').lastIndexOf('.');
  const ext = dot > 0 ? name.slice(dot + 1).toLowerCase() : '';
  const group = Object.keys(TYPE_GROUPS).find(g => TYPE_GROUPS[g].includes(ext)) || 'other';
  return { label: ext ? ext.slice(0, 4) : 'file', cls: 't-' + group };
}

function fileCell(f, { clickable = true } = {}) {
  const t = fileType(f.filename);
  const name = clickable
    ? `<button type="button" class="file-name" data-action="details" data-id="${f.id}" title="${esc(f.filename)}">${esc(f.filename)}</button>`
    : `<span class="file-name" title="${esc(f.filename)}">${esc(f.filename)}</span>`;
  return `<div class="file-cell"><span class="ftype ${t.cls}" aria-hidden="true">${esc(t.label)}</span>
    <div class="name-wrap">${name}<span class="file-id">#${f.id}<span class="show-sm"> · ${fmtBytes(f.size_bytes)}</span></span></div></div>`;
}

/* ---------- Status vocabulary ---------- */

const FILE_STATUS = {
  ACTIVE: ['ok', 'Active'],
  FAILED: ['err', 'Failed'],
  PENDING: ['warn', 'Uploading'],
};
const CHECK_STATUS = {
  VERIFIED: ['ok', 'Verified'],
  DEGRADED: ['warn', 'Degraded'],
  CORRUPT: ['err', 'Corrupt'],
  UNAVAILABLE: ['err', 'Unavailable'],
  FAILED: ['err', 'Check failed'],
};
const REPLICA_STATUS = {
  OK: ['ok', 'Intact'],
  CORRUPT: ['err', 'Corrupt'],
  MISSING: ['err', 'Missing'],
  OFFLINE: ['warn', 'Node offline'],
  ERROR: ['warn', 'Read error'],
};
const OP_RESULT = {
  SUCCESS: ['ok', 'Success'],
  FAILED: ['err', 'Failed'],
};
// VERIFY entries carry the check status as their result; other operations use SUCCESS/FAILED.
const opResultMap = op => (op.type === 'VERIFY' || op.type === 'REPAIR' ? CHECK_STATUS : OP_RESULT);
const OP_LABEL = { UPLOAD: 'Upload', DOWNLOAD: 'Download', DELETE: 'Delete', VERIFY: 'Verify', REPAIR: 'Repair' };
const REPAIR_ACTION = {
  REWRITTEN: ['ok', 'Rewritten'],
  ADDED: ['ok', 'Added'],
  FAILED: ['err', 'Failed'],
  SKIPPED: ['warn', 'Skipped'],
};

function badge(map, key, fallback) {
  const [cls, label] = map[key] || ['', fallback ?? key ?? '—'];
  return `<span class="badge ${cls}">${esc(label)}</span>`;
}

function checkBadge(fileId) {
  if (state.busy.has(fileId)) return `<span class="badge info">${state.busy.get(fileId) === 'repair' ? 'Repairing…' : 'Checking…'}</span>`;
  const c = state.checks.get(fileId);
  return c ? badge(CHECK_STATUS, c.status) : '<span class="badge">Not checked</span>';
}

const isIssue = c => c && c.status !== 'VERIFIED';
// Repair can help whenever the last check found damaged, missing or too few replicas.
const isRepairable = c => !!c && ['DEGRADED', 'CORRUPT', 'UNAVAILABLE'].includes(c.status);

function repairButton(id, cls = 'btn btn-sm') {
  const busy = state.busy.has(id);
  return `<button class="${cls}" type="button" data-action="repair" data-id="${id}" ${busy ? 'disabled' : ''}>` +
    `<svg class="icon"><use href="#i-repair"/></svg>${state.busy.get(id) === 'repair' ? 'Repairing…' : 'Repair replicas'}</button>`;
}

/* ---------- Replication (derived from real node + replica data) ---------- */

function liveNodeIds() {
  const known = state.nodes.some(n => n.in_cluster != null);
  return new Set(state.nodes.filter(n => (known ? n.in_cluster : n.online)).map(n => n.node_id));
}

function replicationTarget(live) {
  const rf = state.stats?.replication_factor || 1;
  return Math.max(1, Math.min(rf, live.size));
}

function fileReplication(f, live = liveNodeIds()) {
  const chunks = f.chunks || [];
  if (f.status !== 'ACTIVE' || chunks.length === 0) return '<span class="faint">—</span>';
  const target = replicationTarget(live);
  let minLive = Infinity;
  for (const c of chunks) {
    const n = (c.replicas || []).filter(r => live.has(r.node_id)).length;
    minLive = Math.min(minLive, n);
  }
  const cls = minLive === 0 ? 'err' : minLive < target ? 'warn' : '';
  const title = minLive === 0
    ? 'At least one chunk has no replica on a live node'
    : `Every chunk has at least ${minLive} replica(s) on live nodes; target is ${target}`;
  return `<span class="repl ${cls}" title="${esc(title)}">${minLive} / ${target}</span>`;
}

/* ---------- Toasts ---------- */

function toast(message, type = 'info', duration = 4500) {
  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.setAttribute('role', type === 'err' ? 'alert' : 'status');
  el.innerHTML = `<div class="msg"></div><button class="icon-btn" type="button" aria-label="Dismiss"><svg class="icon"><use href="#i-x"/></svg></button>`;
  $('.msg', el).textContent = message;
  let timer;
  const close = () => { clearTimeout(timer); el.remove(); };
  const arm = ms => { clearTimeout(timer); if (ms) timer = setTimeout(close, ms); };
  $('button', el).addEventListener('click', close);
  $('#toasts').appendChild(el);
  arm(duration);
  return {
    update(msg, t = type, ms = 4500) { $('.msg', el).textContent = msg; el.className = `toast ${t}`; arm(ms); },
    close,
  };
}

/* ---------- Dialog helpers ---------- */

function confirmDialog({ title, message, ok = 'Confirm', danger = false }) {
  const dlg = $('#confirm-dialog');
  $('#confirm-title').textContent = title;
  $('#confirm-message').textContent = message;
  const okBtn = $('#confirm-ok');
  okBtn.textContent = ok;
  okBtn.className = 'btn ' + (danger ? 'btn-danger' : 'btn-primary');
  dlg.returnValue = '';
  dlg.showModal();
  $('button[value="cancel"]', dlg).focus();
  return new Promise(resolve => dlg.addEventListener('close', () => resolve(dlg.returnValue === 'ok'), { once: true }));
}

async function copyText(text, what = 'Value') {
  try {
    await navigator.clipboard.writeText(text);
    toast(`${what} copied to clipboard.`, 'ok', 2500);
  } catch {
    toast('Copy failed: clipboard access is not available here.', 'err');
  }
}

/* ---------- Data loading ---------- */

async function loadAll() {
  const [stats, nodes, files, integ, ops] = await Promise.allSettled([
    api('/stats'),
    api('/nodes/health'),
    api('/files'),
    api('/integrity'),
    api('/operations?limit=200'),
  ]);

  const unreachable = [stats, nodes, files, integ, ops].every(r => r.status === 'rejected' && r.reason.status === 0);
  setConnection(!unreachable, unreachable ? null : [stats, nodes, files, integ, ops].find(r => r.status === 'rejected')?.reason);

  if (stats.status === 'fulfilled') state.stats = stats.value;
  if (nodes.status === 'fulfilled') state.nodes = nodes.value || [];
  if (files.status === 'fulfilled') state.files = files.value || [];
  if (integ.status === 'fulfilled') {
    state.checks = new Map((integ.value || []).filter(x => x.last_check).map(x => [x.file_id, x.last_check]));
  }
  if (ops.status === 'fulfilled') state.ops = ops.value || [];
  if (state.page === 'activity' && $('#activity-type').value) await loadActivity();

  state.loaded = true;
  render();
}

async function loadActivity() {
  const type = $('#activity-type').value;
  if (!type) { state.activityOps = null; return; }
  try {
    state.activityOps = await api(`/operations?limit=200&type=${encodeURIComponent(type)}`);
  } catch (e) {
    toast(`Could not load activity: ${e.message}`, 'err');
  }
}

function setConnection(ok, partialError) {
  state.connected = ok;
  const conn = $('#conn');
  conn.className = 'conn ' + (ok ? 'ok' : 'err');
  $('#conn-text').textContent = ok ? 'Connected' : 'Disconnected';
  conn.title = ok ? `Connected to ${server}` : `Cannot reach ${server}`;
  const banner = $('#conn-banner');
  if (!ok) {
    banner.textContent = `Cannot reach the metadata server at ${server}. Check that it is running, or change the server address in the sidebar. Showing the last data received.`;
    banner.hidden = false;
  } else if (partialError) {
    banner.textContent = `Some data could not be loaded: ${partialError.message}`;
    banner.hidden = false;
  } else {
    banner.hidden = true;
  }
}

/* ---------- Rendering ---------- */

function render() {
  renderNavCounts();
  const fn = { overview: renderOverview, files: renderFiles, integrity: renderIntegrity, nodes: renderNodes, activity: renderActivity }[state.page];
  fn();
}

function loadingRow(cols) {
  return `<tr class="loading-row"><td colspan="${cols}">Loading…</td></tr>`;
}

function renderNavCounts() {
  const active = state.files.filter(f => f.status === 'ACTIVE').length;
  $('#nav-files-count').textContent = state.loaded ? active : '';
  const issues = state.files.filter(f => f.status === 'ACTIVE' && isIssue(state.checks.get(f.id))).length;
  $('#nav-integrity-count').textContent = issues || '';
  $('#nav-integrity-count').title = issues ? `${issues} file(s) with integrity issues` : '';
  const offline = state.nodes.filter(n => !n.online).length;
  $('#nav-nodes-count').textContent = offline || '';
  $('#nav-nodes-count').title = offline ? `${offline} node(s) not responding` : '';
}

function renderOverview() {
  const s = state.stats;
  const online = state.nodes.filter(n => n.online);
  const capacity = online.reduce((a, n) => a + n.total_space, 0);
  const used = online.reduce((a, n) => a + (n.total_space - n.free_space), 0);
  const pct = capacity ? (used / capacity) * 100 : 0;
  const ib = s?.integrity_by_status || {};
  const issues = (ib.DEGRADED || 0) + (ib.CORRUPT || 0) + (ib.UNAVAILABLE || 0) + (ib.FAILED || 0);

  const metric = (label, value, sub, cls = '', extra = '') =>
    `<div class="metric"><div class="metric-label">${label}</div><div class="metric-value ${cls}">${value}</div><div class="metric-sub">${sub}</div>${extra}</div>`;

  if (!s) {
    $('#summary').innerHTML = metric('Files', '—', state.loaded ? 'Unavailable' : 'Loading…');
  } else {
    const fileSub = [s.files_failed && `${s.files_failed} failed`, s.files_pending && `${s.files_pending} uploading`].filter(Boolean).join(' · ') || 'No failed uploads';
    let replValue = fmtNum(s.under_replicated);
    let replSub = `Under-replicated chunks (target ${s.replication_factor})`;
    let replCls = s.unavailable_chunks ? 'err' : s.under_replicated ? 'warn' : '';
    if (s.discovery_error) { replValue = '—'; replSub = 'etcd is unreachable'; replCls = 'err'; }
    else if (s.unavailable_chunks) replSub = `${fmtNum(s.unavailable_chunks)} chunk(s) have no live replica`;

    $('#summary').innerHTML = [
      metric('Files', fmtNum(s.files_active), fileSub),
      metric('Stored data', fmtBytes(s.logical_bytes), `${fmtNum(s.chunks)} chunks · ${fmtNum(s.replicas)} replicas`),
      metric('Node disk usage', fmtBytes(used), capacity ? `of ${fmtBytes(capacity)} on ${online.length} online node(s)` : 'No online nodes', '',
        `<div class="meter ${pct > 90 ? 'err' : pct > 75 ? 'warn' : ''}" role="img" aria-label="${pct.toFixed(1)}% used"><span style="width:${Math.min(100, pct).toFixed(1)}%"></span></div>`),
      metric('Storage nodes', `${online.length} / ${state.nodes.length}`, s.discovery_error ? 'etcd membership unknown' : `${s.live_nodes} in cluster (etcd lease)`,
        online.length < state.nodes.length ? 'warn' : ''),
      metric('Replication', replValue, replSub, replCls),
      metric('Integrity', `${fmtNum(ib.VERIFIED || 0)} / ${fmtNum(s.files_active)}`,
        `verified · ${issues} with issues · ${ib.UNCHECKED || 0} not checked`, issues ? 'warn' : ''),
    ].join('');
  }

  // Recent files
  const recent = state.files.slice(0, 6);
  const live = liveNodeIds();
  $('#recent-files').innerHTML = !state.loaded ? loadingRow(5) : recent.length === 0
    ? `<tr><td colspan="5"><div class="empty"><strong>No files stored yet</strong>
        <p>Uploaded files are split into chunks and replicated across the storage nodes.</p>
        <button class="btn btn-primary" type="button" data-action="open-upload">Upload a file</button></div></td></tr>`
    : recent.map(f => `<tr>
        <td>${fileCell(f)}</td>
        <td class="num nowrap col-opt">${fmtBytes(f.size_bytes)}</td>
        <td class="col-opt">${fileReplication(f, live)}</td>
        <td>${f.status === 'ACTIVE' ? checkBadge(f.id) : badge(FILE_STATUS, f.status)}</td>
        <td class="col-opt nowrap">${timeCell(f.created_at)}</td>
      </tr>`).join('');

  // Nodes
  $('#overview-nodes').innerHTML = !state.loaded ? '<li class="muted">Loading…</li>' : state.nodes.length === 0
    ? `<li><div class="empty compact"><p>No storage nodes have registered. Start a storage node with <code>-metadata-url</code> pointing at this server.</p></div></li>`
    : state.nodes.map(n => {
      const u = n.total_space ? ((n.total_space - n.free_space) / n.total_space) * 100 : 0;
      return `<li>
        <div class="node-line"><strong>${esc(n.node_id)}</strong>${nodeHealthBadge(n)}</div>
        ${n.online
          ? `<div class="usage"><div class="meter ${u > 90 ? 'err' : u > 75 ? 'warn' : ''}"><span style="width:${u.toFixed(1)}%"></span></div><span class="pct">${u.toFixed(1)}%</span></div>
             <div class="metric-sub">${fmtBytes(n.total_space - n.free_space)} of ${fmtBytes(n.total_space)} · ${fmtNum(n.replica_count)} replicas</div>`
          : `<div class="metric-sub">${esc(n.address)} · ${esc(n.error || 'not responding')} · ${fmtNum(n.replica_count)} replicas recorded</div>`}
      </li>`;
    }).join('');

  // Activity
  const ops = state.ops.slice(0, 8);
  $('#overview-activity').innerHTML = !state.loaded ? '<li class="muted">Loading…</li>' : ops.length === 0
    ? '<li><div class="empty compact"><p>No file operations recorded yet.</p></div></li>'
    : ops.map(op => {
      const [cls, label] = opResultMap(op)[op.result] || ['', op.result];
      return `<li>
        <span class="op">${esc(OP_LABEL[op.type] || op.type)}</span>
        <span class="what"><span class="name" title="${esc(op.filename)}">${esc(op.filename || '—')}</span>
          <span class="small res-${cls}">${esc(label)}</span></span>
        ${timeCell(op.created_at)}
      </li>`;
    }).join('');
}

function nodeHealthBadge(n) {
  if (!n.online) return '<span class="badge err">Offline</span>';
  if (n.in_cluster === false) return '<span class="badge warn" title="Responds to /health but has no live etcd lease">No lease</span>';
  return '<span class="badge ok">Online</span>';
}

function filteredFiles() {
  const { search, status, integrity, sort, dir } = state.filters;
  const q = search.trim().toLowerCase();
  let list = state.files.filter(f => {
    if (q && !f.filename.toLowerCase().includes(q) && String(f.id) !== q.replace(/^#/, '')) return false;
    if (status && f.status !== status) return false;
    if (integrity) {
      const c = state.checks.get(f.id);
      if (integrity === 'unchecked' && (c || f.status !== 'ACTIVE')) return false;
      if (integrity === 'issues' && !(f.status === 'ACTIVE' && isIssue(c))) return false;
      if (integrity === 'VERIFIED' && c?.status !== 'VERIFIED') return false;
    }
    return true;
  });
  const key = { name: f => f.filename.toLowerCase(), size: f => f.size_bytes, created: f => f.id }[sort];
  list = list.slice().sort((a, b) => {
    const x = key(a), y = key(b);
    return (x < y ? -1 : x > y ? 1 : 0) * (dir === 'asc' ? 1 : -1);
  });
  return list;
}

function renderFiles() {
  $$('#files-table .sort').forEach(btn => {
    if (btn.dataset.sort === state.filters.sort) btn.setAttribute('aria-sort', state.filters.dir === 'asc' ? 'ascending' : 'descending');
    else btn.removeAttribute('aria-sort');
  });
  const body = $('#files-body');
  if (!state.loaded) { body.innerHTML = loadingRow(7); return; }

  const list = filteredFiles();
  const filtering = state.filters.search || state.filters.status || state.filters.integrity;
  $('#files-count').textContent = filtering
    ? `${list.length} of ${state.files.length} file(s)`
    : `${state.files.length} file(s)`;

  if (state.files.length === 0) {
    body.innerHTML = `<tr><td colspan="7"><div class="empty"><strong>No files stored yet</strong>
      <p>Upload a file to split it into 4 MB chunks and replicate each chunk across the storage nodes. You can also drop files onto this table.</p>
      <button class="btn btn-primary" type="button" data-action="open-upload">Upload files</button></div></td></tr>`;
    return;
  }
  if (list.length === 0) {
    body.innerHTML = `<tr><td colspan="7"><div class="empty"><strong>No files match the current filters</strong>
      <p></p><button class="btn" type="button" data-action="clear-filters">Clear filters</button></div></td></tr>`;
    return;
  }
  const live = liveNodeIds();
  body.innerHTML = list.map(f => {
    const active = f.status === 'ACTIVE';
    const busy = state.busy.has(f.id);
    return `<tr>
      <td>${fileCell(f)}</td>
      <td class="num nowrap col-opt">${fmtBytes(f.size_bytes)}</td>
      <td class="num col-opt">${(f.chunks || []).length}</td>
      <td class="col-opt">${fileReplication(f, live)}</td>
      <td>${active ? checkBadge(f.id) : badge(FILE_STATUS, f.status)}</td>
      <td class="col-opt nowrap">${timeCell(f.created_at)}</td>
      <td class="actions">
        <button class="icon-btn" type="button" data-action="download" data-id="${f.id}" ${active ? '' : 'disabled'} aria-label="Download ${esc(f.filename)}" title="Download"><svg class="icon"><use href="#i-download"/></svg></button>
        <button class="icon-btn hide-sm" type="button" data-action="verify" data-id="${f.id}" ${active && !busy ? '' : 'disabled'} aria-label="Verify integrity of ${esc(f.filename)}" title="Verify integrity"><svg class="icon"><use href="#i-shield"/></svg></button>
        <button class="icon-btn" type="button" data-action="details" data-id="${f.id}" aria-label="Details for ${esc(f.filename)}" title="Details"><svg class="icon"><use href="#i-info"/></svg></button>
        <button class="icon-btn danger hide-sm" type="button" data-action="delete" data-id="${f.id}" aria-label="Delete ${esc(f.filename)}" title="Delete"><svg class="icon"><use href="#i-trash"/></svg></button>
      </td>
    </tr>`;
  }).join('');
}

function renderIntegrity() {
  const body = $('#integrity-body');
  const btn = $('#verify-all-btn');
  const active = state.files.filter(f => f.status === 'ACTIVE');
  btn.disabled = !state.verifyAll && active.length === 0;
  if (!state.loaded) { body.innerHTML = loadingRow(6); return; }

  const filter = $('#integrity-filter').value;
  const list = active.filter(f => {
    const c = state.checks.get(f.id);
    if (filter === 'issues') return isIssue(c);
    if (filter === 'unchecked') return !c;
    if (filter === 'VERIFIED') return c?.status === 'VERIFIED';
    return true;
  });
  $('#integrity-count').textContent = `${list.length} of ${active.length} active file(s)`;

  if (active.length === 0) {
    body.innerHTML = `<tr><td colspan="6"><div class="empty"><strong>Nothing to verify yet</strong>
      <p>Integrity checks run against active files. Upload a file first.</p>
      <button class="btn btn-primary" type="button" data-action="open-upload">Upload files</button></div></td></tr>`;
    return;
  }
  if (list.length === 0) {
    const msg = filter === 'issues' ? 'No file has failed its most recent check.' : 'No files match this filter.';
    body.innerHTML = `<tr><td colspan="6"><div class="empty"><strong>${msg}</strong></div></td></tr>`;
    return;
  }
  body.innerHTML = list.map(f => {
    const c = state.checks.get(f.id);
    const replicas = (f.chunks || []).reduce((a, ch) => a + (ch.replicas || []).length, 0);
    const busy = state.busy.has(f.id);
    return `<tr>
      <td>${fileCell(f)}</td>
      <td class="col-opt">${f.checksum
        ? `<span class="copy-row"><span class="hash" title="${esc(f.checksum)}">${esc(shortHash(f.checksum))}</span>
           <button class="icon-btn" type="button" data-action="copy" data-value="${esc(f.checksum)}" aria-label="Copy SHA-256"><svg class="icon"><use href="#i-copy"/></svg></button></span>`
        : '<span class="faint" title="Uploaded before file checksums were recorded; the next check will compute it">Not recorded</span>'}</td>
      <td class="num col-opt">${c ? `${c.replicas_ok} / ${c.replicas_total}` : replicas}</td>
      <td>${checkBadge(f.id)}</td>
      <td class="col-opt nowrap">${c ? timeCell(c.checked_at) : '<span class="faint">Never</span>'}</td>
      <td class="actions">
        ${isRepairable(c) ? repairButton(f.id) : ''}
        <button class="btn btn-sm" type="button" data-action="verify" data-id="${f.id}" ${busy ? 'disabled' : ''}>Verify</button>
        <button class="btn btn-sm" type="button" data-action="report" data-id="${f.id}" ${c ? '' : 'disabled'}>Report</button>
      </td>
    </tr>`;
  }).join('');
}

function renderNodes() {
  const body = $('#nodes-body');
  if (!state.loaded) { body.innerHTML = loadingRow(8); return; }
  if (state.nodes.length === 0) {
    body.innerHTML = `<tr><td colspan="8"><div class="empty"><strong>No storage nodes registered</strong>
      <p>Storage nodes register themselves on startup. Start one with <code>go run ./cmd/storage-node -metadata-url ${esc(server)}</code>.</p></div></td></tr>`;
    return;
  }
  body.innerHTML = state.nodes.map(n => {
    const used = n.total_space - n.free_space;
    const u = n.total_space ? (used / n.total_space) * 100 : 0;
    const cluster = n.in_cluster == null ? '<span class="badge">Unknown</span>'
      : n.in_cluster ? '<span class="badge ok">Active lease</span>' : '<span class="badge warn">No lease</span>';
    return `<tr>
      <td class="mono">${esc(n.node_id)}</td>
      <td class="col-md mono muted">${esc(n.address)}</td>
      <td>${n.online ? '<span class="badge ok">Responding</span>' : `<span class="badge err" title="${esc(n.error || '')}">${esc(n.error === 'unreachable' ? 'Unreachable' : n.error || 'Down')}</span>`}</td>
      <td>${cluster}</td>
      <td class="num">${fmtNum(n.replica_count)}</td>
      <td class="num col-opt nowrap">${n.online ? fmtBytes(used) : '—'}</td>
      <td class="num col-opt nowrap">${n.online ? fmtBytes(n.total_space) : '—'}</td>
      <td>${n.online
        ? `<div class="usage"><div class="meter ${u > 90 ? 'err' : u > 75 ? 'warn' : ''}"><span style="width:${u.toFixed(1)}%"></span></div><span class="pct">${u.toFixed(1)}%</span></div>`
        : '<span class="faint">—</span>'}</td>
    </tr>`;
  }).join('');
}

function renderActivity() {
  const body = $('#activity-body');
  if (!state.loaded) { body.innerHTML = loadingRow(6); return; }
  const ops = state.activityOps ?? state.ops;
  if (ops.length === 0) {
    body.innerHTML = `<tr><td colspan="6"><div class="empty"><strong>No operations recorded</strong>
      <p>Uploads, downloads, deletions and integrity checks appear here as they happen.</p></div></td></tr>`;
    return;
  }
  const existing = new Set(state.files.map(f => f.id));
  body.innerHTML = ops.map(op => `<tr>
    <td class="nowrap">${timeCell(op.created_at)}</td>
    <td>${esc(OP_LABEL[op.type] || op.type)}</td>
    <td>${op.file_id && existing.has(op.file_id)
      ? `<button type="button" class="file-name" data-action="details" data-id="${op.file_id}" title="${esc(op.filename)}">${esc(op.filename)}</button>`
      : `<span class="file-name" title="${esc(op.filename)}">${esc(op.filename || '—')}</span>`}</td>
    <td>${badge(opResultMap(op), op.result)}</td>
    <td class="num col-opt nowrap">${op.size_bytes ? fmtBytes(op.size_bytes) : '—'}</td>
    <td class="col-opt small muted">${esc(op.detail || '')}</td>
  </tr>`).join('');
}

/* ---------- Drawer: file details & integrity report ---------- */

const drawer = $('#drawer');

function openDrawer(title) {
  $('#drawer-title').textContent = title;
  $('#drawer-body').innerHTML = '<p class="muted">Loading…</p>';
  if (!drawer.open) drawer.showModal();
}

async function showFileDetails(id) {
  state.drawer = { kind: 'file', id };
  const f0 = state.files.find(f => f.id === id);
  openDrawer(f0 ? f0.filename : `File #${id}`);
  try {
    const [file, report] = await Promise.all([api(`/files/${id}`), api(`/files/${id}/integrity`)]);
    if (state.drawer?.kind !== 'file' || state.drawer.id !== id) return;
    renderFileDetails(file, report);
  } catch (e) {
    $('#drawer-body').innerHTML = `<div class="callout err">${esc(e.message)}</div>`;
  }
}

function renderFileDetails(f, report) {
  $('#drawer-title').textContent = f.filename;
  const active = f.status === 'ACTIVE';
  const live = liveNodeIds();
  const c = report.file.last_check;
  const chunks = f.chunks || [];
  $('#drawer-body').innerHTML = `
    <div class="drawer-actions">
      <button class="btn btn-primary" type="button" data-action="download" data-id="${f.id}" ${active ? '' : 'disabled'}><svg class="icon"><use href="#i-download"/></svg>Download</button>
      <button class="btn" type="button" data-action="verify" data-id="${f.id}" ${active && !state.busy.has(f.id) ? '' : 'disabled'}><svg class="icon"><use href="#i-shield"/></svg>${state.busy.get(f.id) === 'verify' ? 'Checking…' : 'Verify integrity'}</button>
      ${active && isRepairable(c) ? repairButton(f.id, 'btn') : ''}
      <span class="spacer"></span>
      <button class="btn" type="button" data-action="delete" data-id="${f.id}"><svg class="icon"><use href="#i-trash"/></svg>Delete</button>
    </div>
    <dl class="kv">
      <dt>File ID</dt><dd>#${f.id}</dd>
      <dt>Status</dt><dd>${badge(FILE_STATUS, f.status)}</dd>
      <dt>Size</dt><dd>${fmtBytes(f.size_bytes)} <span class="muted">(${fmtNum(f.size_bytes)} bytes)</span></dd>
      <dt>Uploaded</dt><dd>${esc(fmtTime(f.created_at))}</dd>
      <dt>SHA-256</dt><dd>${f.checksum
        ? `<span class="copy-row"><span class="hash">${esc(f.checksum)}</span><button class="icon-btn" type="button" data-action="copy" data-value="${esc(f.checksum)}" aria-label="Copy SHA-256"><svg class="icon"><use href="#i-copy"/></svg></button></span>`
        : '<span class="muted">Not recorded (uploaded before checksums were added; a verification will compute it)</span>'}</dd>
      <dt>Replication</dt><dd>${fileReplication(f, live)} <span class="muted small">min. live replicas per chunk / target</span></dd>
      <dt>Last integrity check</dt><dd>${active
        ? (c ? `${badge(CHECK_STATUS, c.status)} <span class="muted small">${esc(fmtTime(c.checked_at))}</span> · <button class="link-btn" type="button" data-action="report" data-id="${f.id}">View report</button>`
             : '<span class="muted">Never checked</span>')
        : '<span class="muted">Only active files can be verified</span>'}</dd>
    </dl>

    <h3>Chunk map <span class="muted small">(${chunks.length} chunk${chunks.length === 1 ? '' : 's'})</span></h3>
    ${chunks.length === 0
      ? '<p class="muted">This file has no chunks (it is empty, or the upload failed before any chunk was stored).</p>'
      : `<div class="panel"><div class="table-scroll"><table class="table">
          <thead><tr><th class="num">#</th><th>Chunk SHA-256</th><th class="num">Size</th><th>Replicas</th></tr></thead>
          <tbody>${chunks.map(ch => `<tr>
            <td class="num">${ch.sequence}</td>
            <td><span class="hash" title="${esc(ch.hash)}">${esc(shortHash(ch.hash))}</span></td>
            <td class="num nowrap">${fmtBytes(ch.size_bytes)}</td>
            <td>${(ch.replicas || []).map(r => `<span class="chip ${live.has(r.node_id) ? '' : 'off'}" title="${live.has(r.node_id) ? 'Node is live' : 'Node is not live'}">${esc(r.node_id)}</span>`).join('') || '<span class="faint">none</span>'}</td>
          </tr>`).join('')}</tbody></table></div></div>`}
  `;
}

async function showReport(id) {
  state.drawer = { kind: 'report', id };
  const f0 = state.files.find(f => f.id === id);
  openDrawer(`Integrity report: ${f0 ? f0.filename : '#' + id}`);
  try {
    const report = await api(`/files/${id}/integrity`);
    if (state.drawer?.kind !== 'report' || state.drawer.id !== id) return;
    renderReport(report);
  } catch (e) {
    $('#drawer-body').innerHTML = `<div class="callout err">${esc(e.message)}</div>`;
  }
}

function renderReport(report) {
  const f = report.file;
  $('#drawer-title').textContent = `Integrity report: ${f.filename}`;
  const c = report.history[0];
  const busy = state.busy.has(f.file_id);
  const actions = `<div class="drawer-actions">
      ${f.status === 'ACTIVE' && isRepairable(report.history[0]) ? repairButton(f.file_id, 'btn btn-primary') : ''}
      <button class="btn ${isRepairable(report.history[0]) ? '' : 'btn-primary'}" type="button" data-action="verify" data-id="${f.file_id}" ${busy || f.status !== 'ACTIVE' ? 'disabled' : ''}><svg class="icon"><use href="#i-shield"/></svg>${state.busy.get(f.file_id) === 'verify' ? 'Checking…' : 'Run check again'}</button>
      <button class="btn" type="button" data-action="details" data-id="${f.file_id}">File details</button>
    </div>`;
  if (!c) {
    $('#drawer-body').innerHTML = actions + '<p class="muted">This file has not been checked yet.</p>';
    return;
  }
  const [cls] = CHECK_STATUS[c.status] || [''];
  const match = c.computed_checksum && c.expected_checksum && c.computed_checksum === c.expected_checksum;
  $('#drawer-body').innerHTML = actions + `
    <div class="callout ${cls}">${badge(CHECK_STATUS, c.status)}<div>${esc(c.message)}</div></div>
    ${renderRepairSummary(state.repairs.get(f.file_id))}
    <dl class="kv">
      <dt>Checked</dt><dd>${esc(fmtTime(c.checked_at))} <span class="muted small">in ${fmtNum(c.duration_ms)} ms</span></dd>
      <dt>Replicas intact</dt><dd>${c.replicas_ok} of ${c.replicas_total} across ${c.chunks_total} chunk(s)</dd>
      <dt>Expected SHA-256</dt><dd><span class="hash">${esc(c.expected_checksum || '—')}</span></dd>
      <dt>Computed SHA-256</dt><dd><span class="hash">${esc(c.computed_checksum || 'Not computed (file could not be reassembled)')}</span>
        ${c.computed_checksum && c.expected_checksum ? `<div class="small ${match ? 'res-ok' : 'res-err'}">${match ? 'Matches the recorded checksum' : 'Does not match the recorded checksum'}</div>` : ''}</dd>
    </dl>

    <h3>Replica results</h3>
    ${(c.replicas || []).length === 0 ? '<p class="muted">No replicas were checked (the file is empty, or the check could not start).</p>' : `
    <div class="panel"><div class="table-scroll"><table class="table">
      <thead><tr><th class="num">Chunk</th><th>Node</th><th>Result</th><th>Detail</th></tr></thead>
      <tbody>${c.replicas.map(r => `<tr>
        <td class="num" title="${esc(r.chunk_hash)}">${r.chunk_sequence}</td>
        <td class="mono">${esc(r.node_id)}</td>
        <td>${badge(REPLICA_STATUS, r.status)}</td>
        <td class="small muted">${esc(r.detail || '')}</td>
      </tr>`).join('')}</tbody></table></div></div>`}

    <h3>Recent checks</h3>
    <div class="panel"><div class="table-scroll"><table class="table">
      <thead><tr><th>Checked</th><th>Result</th><th class="num">Intact</th><th class="num">Duration</th></tr></thead>
      <tbody>${report.history.map(h => `<tr>
        <td class="nowrap">${esc(fmtTime(h.checked_at))}</td>
        <td>${badge(CHECK_STATUS, h.status)}</td>
        <td class="num">${h.replicas_ok} / ${h.replicas_total}</td>
        <td class="num">${fmtNum(h.duration_ms)} ms</td>
      </tr>`).join('')}</tbody></table></div></div>
  `;
}

function renderRepairSummary(r) {
  if (!r) return '';
  return `<h3>Last repair</h3>
    <p class="small repair-summary">${esc(r.summary)}</p>
    ${r.actions.length === 0 ? '' : `<div class="panel repair-actions"><div class="table-scroll"><table class="table">
      <thead><tr><th class="num">Chunk</th><th>Node</th><th>Action</th><th>Detail</th></tr></thead>
      <tbody>${r.actions.map(a => `<tr>
        <td class="num">${a.chunk_sequence}</td>
        <td class="mono">${esc(a.node_id || '—')}</td>
        <td>${badge(REPAIR_ACTION, a.action)}</td>
        <td class="small muted">${esc(a.detail)}</td>
      </tr>`).join('')}</tbody></table></div></div>`}`;
}

function refreshDrawer(id) {
  if (!drawer.open || !state.drawer || state.drawer.id !== id) return;
  if (state.drawer.kind === 'file') showFileDetails(id);
  else showReport(id);
}

/* ---------- Actions ---------- */

async function verifyFile(id, { quiet = false } = {}) {
  if (state.busy.has(id)) return null;
  const f = state.files.find(x => x.id === id);
  const name = f ? f.filename : `#${id}`;
  state.busy.set(id, "verify");
  render();
  if (drawer.open && state.drawer?.id === id) refreshDrawer(id);
  try {
    const check = await api(`/files/${id}/verify`, { method: 'POST', timeout: VERIFY_TIMEOUT_MS });
    state.checks.set(id, check);
    if (!quiet) {
      const [cls, label] = CHECK_STATUS[check.status] || ['', check.status];
      toast(`${name}: ${label}. ${check.message}`, cls === 'ok' ? 'ok' : cls === 'warn' ? 'warn' : 'err', cls === 'ok' ? 4500 : 9000);
    }
    return check;
  } catch (e) {
    if (!quiet) toast(`Could not verify ${name}: ${e.message}`, 'err', 7000);
    return { status: 'ERROR', error: e.message };
  } finally {
    state.busy.delete(id);
    render();
    refreshDrawer(id);
  }
}

async function repairFile(id) {
  if (state.busy.has(id)) return;
  const f = state.files.find(x => x.id === id);
  const name = f ? f.filename : `#${id}`;
  state.busy.set(id, 'repair');
  render();
  refreshDrawer(id);
  try {
    const res = await api(`/files/${id}/repair`, { method: 'POST', timeout: VERIFY_TIMEOUT_MS });
    state.repairs.set(id, res);
    state.checks.set(id, res.check);
    const kind = res.check.status === 'VERIFIED' ? 'ok' : res.chunks_unrecoverable ? 'err' : 'warn';
    toast(`${name}: ${res.summary}`, kind, kind === 'ok' ? 6000 : 10000);
  } catch (e) {
    toast(`Could not repair ${name}: ${e.message}`, 'err', 8000);
  } finally {
    state.busy.delete(id);
    render();
    loadAll();
    // Show what was done: refresh the open panel, or open the integrity report.
    if (drawer.open && state.drawer?.id === id) refreshDrawer(id);
    else if (state.repairs.has(id)) showReport(id);
  }
}

async function verifyAll() {
  if (state.verifyAll) { state.verifyAll.cancelled = true; return; }
  const targets = state.files.filter(f => f.status === 'ACTIVE').map(f => f.id);
  if (targets.length === 0) return;
  const run = state.verifyAll = { cancelled: false };
  const btn = $('#verify-all-btn');
  const status = $('#verify-all-status');
  btn.innerHTML = '<svg class="icon"><use href="#i-x"/></svg>Stop';
  const tally = {};
  let done = 0;
  for (const id of targets) {
    if (run.cancelled) break;
    status.textContent = `Checking ${done + 1} of ${targets.length}…`;
    const res = await verifyFile(id, { quiet: true });
    if (res) tally[res.status] = (tally[res.status] || 0) + 1;
    done++;
  }
  state.verifyAll = null;
  btn.innerHTML = '<svg class="icon"><use href="#i-shield"/></svg>Verify all active files';
  status.textContent = '';
  const parts = Object.entries(tally).map(([k, v]) => `${v} ${(CHECK_STATUS[k] || [, k === 'ERROR' ? 'not checked (error)' : k])[1].toLowerCase()}`);
  const bad = Object.keys(tally).some(k => k !== 'VERIFIED');
  toast(`${run.cancelled ? 'Stopped after' : 'Checked'} ${done} of ${targets.length} file(s): ${parts.join(', ') || 'none'}.`, bad ? 'warn' : 'ok', 9000);
  loadAll();
}

async function deleteFile(id) {
  const f = state.files.find(x => x.id === id);
  const name = f ? f.filename : `file #${id}`;
  const ok = await confirmDialog({
    title: 'Delete file?',
    message: `"${name}" will be removed from the metadata server, and its chunks will be deleted from storage nodes unless another file shares them. This cannot be undone.`,
    ok: 'Delete',
    danger: true,
  });
  if (!ok) return;
  try {
    await api(`/files/${id}`, { method: 'DELETE' });
    toast(`Deleted ${name}.`, 'ok');
    if (drawer.open && state.drawer?.id === id) drawer.close();
    loadAll();
  } catch (e) {
    toast(`Could not delete ${name}: ${e.message}`, 'err', 7000);
    loadAll();
  }
}

async function sha256Hex(buffer) {
  const digest = await crypto.subtle.digest('SHA-256', buffer);
  return Array.from(new Uint8Array(digest), b => b.toString(16).padStart(2, '0')).join('');
}

async function downloadFile(id) {
  const f = state.files.find(x => x.id === id);
  const name = f ? f.filename : `dfs-file-${id}`;
  const t = toast(`Downloading ${name}…`, 'info', 0);
  try {
    let res;
    try {
      res = await fetch(`${server}/download/${id}`);
    } catch {
      throw new Error('Cannot reach the metadata server.');
    }
    if (!res.ok) {
      const body = await res.json().catch(() => ({}));
      throw new Error(body.error || `HTTP ${res.status}`);
    }
    const expected = res.headers.get('X-Checksum-SHA256');
    let blob;
    try {
      blob = await res.blob();
    } catch {
      throw new Error('The transfer was interrupted (a chunk could not be read from any replica).');
    }

    let note = '';
    let kind = 'ok';
    if (expected && window.crypto?.subtle && blob.size <= CLIENT_HASH_LIMIT) {
      t.update(`Checking SHA-256 of ${name}…`, 'info', 0);
      const actual = await sha256Hex(await blob.arrayBuffer());
      if (actual === expected) note = ' SHA-256 matches the checksum recorded at upload.';
      else { note = ' Warning: its SHA-256 does not match the checksum recorded at upload.'; kind = 'warn'; }
    }

    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 10000);
    t.update(`Downloaded ${name} (${fmtBytes(blob.size)}).${note}`, kind, kind === 'ok' ? 5000 : 10000);
  } catch (e) {
    t.update(`Download of ${name} failed: ${e.message}`, 'err', 8000);
  } finally {
    loadAll();
  }
}

/* ---------- Upload ---------- */

const uploadDialog = $('#upload-dialog');

function openUpload(files) {
  if (!state.uploading) state.uploads = state.uploads.filter(u => u.status === 'queued');
  if (files?.length) addUploads(files);
  renderUploads();
  if (!uploadDialog.open) uploadDialog.showModal();
}

function addUploads(fileList) {
  for (const file of fileList) state.uploads.push({ file, status: 'queued', progress: 0, message: '' });
  renderUploads();
}

function renderUploads() {
  const list = $('#upload-queue');
  list.innerHTML = state.uploads.map((u, i) => {
    let status = '';
    switch (u.status) {
      case 'queued': status = '<span class="uq-status">Ready to upload</span>'; break;
      case 'uploading': status = `<span class="uq-status"><span class="meter"><span style="width:${(u.progress * 100).toFixed(1)}%"></span></span>${Math.round(u.progress * 100)}% sent</span>`; break;
      case 'processing': status = '<span class="uq-status"><span class="meter indeterminate"><span></span></span>Chunking and replicating…</span>'; break;
      case 'done': status = `<span class="uq-status ok">${esc(u.message)}</span>`; break;
      case 'error': status = `<span class="uq-status err">${esc(u.message)}</span>`; break;
    }
    return `<li>
      <div class="uq-line"><span class="uq-name" title="${esc(u.file.name)}">${esc(u.file.name)}</span><span class="uq-size">${fmtBytes(u.file.size)}</span>
        ${u.status === 'queued' && !state.uploading ? `<button class="icon-btn" type="button" data-action="remove-upload" data-index="${i}" aria-label="Remove ${esc(u.file.name)}"><svg class="icon"><use href="#i-x"/></svg></button>` : ''}
      </div>${status}</li>`;
  }).join('');
  const queued = state.uploads.filter(u => u.status === 'queued').length;
  const startBtn = $('#upload-start');
  startBtn.disabled = state.uploading || queued === 0;
  startBtn.textContent = state.uploading ? 'Uploading…' : queued > 1 ? `Upload ${queued} files` : 'Upload';
  const done = state.uploads.filter(u => u.status === 'done').length;
  const failed = state.uploads.filter(u => u.status === 'error').length;
  $('#upload-summary').textContent = state.uploads.length === 0 ? '' :
    [done && `${done} stored`, failed && `${failed} failed`, queued && `${queued} waiting`].filter(Boolean).join(' · ');
}

function uploadOne(item) {
  return new Promise(resolve => {
    const xhr = new XMLHttpRequest();
    xhr.open('POST', server + '/upload');
    item.status = 'uploading';
    renderUploads();
    xhr.upload.onprogress = e => {
      if (!e.lengthComputable) return;
      item.progress = e.loaded / e.total;
      item.status = e.loaded >= e.total ? 'processing' : 'uploading';
      renderUploads();
    };
    xhr.upload.onload = () => { item.status = 'processing'; renderUploads(); };
    xhr.onload = () => {
      let body = null;
      try { body = JSON.parse(xhr.responseText); } catch { /* not JSON */ }
      if (xhr.status === 200 && body) {
        item.status = 'done';
        const n = (body.chunks || []).length;
        item.message = `Stored as #${body.id} · ${n} chunk${n === 1 ? '' : 's'}`;
      } else {
        item.status = 'error';
        item.message = (body && body.error) || `Upload failed with HTTP ${xhr.status}.`;
      }
      resolve();
    };
    xhr.onerror = () => {
      item.status = 'error';
      item.message = 'Network error: the metadata server could not be reached.';
      resolve();
    };
    const form = new FormData();
    form.append('file', item.file);
    xhr.send(form);
  });
}

async function startUploads() {
  if (state.uploading) return;
  state.uploading = true;
  renderUploads();
  let ok = 0;
  let failed = 0;
  for (const item of state.uploads) {
    if (item.status !== 'queued') continue;
    await uploadOne(item);
    renderUploads();
    if (item.status === 'done') ok++; else failed++;
    loadAll();
  }
  state.uploading = false;
  renderUploads();
  if (!uploadDialog.open) {
    toast(`Upload finished: ${ok} stored${failed ? `, ${failed} failed` : ''}.`, failed ? 'err' : 'ok', 7000);
  }
}

/* ---------- Navigation ---------- */

function route() {
  const name = location.hash.replace(/^#\/?/, '').split('/')[0];
  const page = PAGES[name] ? name : 'overview';
  state.page = page;
  $$('section[data-page]').forEach(s => { s.hidden = s.dataset.page !== page; });
  $$('.nav a').forEach(a => {
    if (a.dataset.nav === page) a.setAttribute('aria-current', 'page');
    else a.removeAttribute('aria-current');
  });
  $('#page-title').textContent = PAGES[page];
  document.title = `${PAGES[page]} · DFS Console`;
  setNav(false);
  if (page === 'activity') loadActivity().then(render);
  else render();
}

function setNav(open) {
  $('#sidebar').classList.toggle('open', open);
  $('#scrim').hidden = !open;
  $('.menu-btn').setAttribute('aria-expanded', String(open));
}

/* ---------- Event wiring ---------- */

document.addEventListener('click', e => {
  const el = e.target.closest('[data-action]');
  if (!el || el.disabled) return;
  const id = Number(el.dataset.id);
  switch (el.dataset.action) {
    case 'refresh': loadAll(); break;
    case 'open-upload': openUpload(); break;
    case 'close-upload': uploadDialog.close(); break;
    case 'start-upload': startUploads(); break;
    case 'remove-upload': state.uploads.splice(Number(el.dataset.index), 1); renderUploads(); break;
    case 'details': showFileDetails(id); break;
    case 'report': showReport(id); break;
    case 'close-drawer': drawer.close(); break;
    case 'download': downloadFile(id); break;
    case 'verify': verifyFile(id); break;
    case 'repair': repairFile(id); break;
    case 'verify-all': verifyAll(); break;
    case 'delete': deleteFile(id); break;
    case 'copy': copyText(el.dataset.value, 'Checksum'); break;
    case 'toggle-nav': setNav(!$('#sidebar').classList.contains('open')); break;
    case 'clear-filters':
      Object.assign(state.filters, { search: '', status: '', integrity: '' });
      $('#file-search').value = ''; $('#file-status').value = ''; $('#file-integrity').value = '';
      renderFiles();
      break;
    case 'change-server': {
      const dlg = $('#server-dialog');
      $('#server-input').value = server;
      dlg.returnValue = '';
      dlg.showModal();
      break;
    }
  }
});

$('#scrim').addEventListener('click', () => setNav(false));
document.addEventListener('keydown', e => { if (e.key === 'Escape') setNav(false); });

// Close the drawer when its backdrop is clicked.
drawer.addEventListener('click', e => { if (e.target === drawer) drawer.close(); });
drawer.addEventListener('close', () => { state.drawer = null; });

$('#server-dialog').addEventListener('close', () => {
  const dlg = $('#server-dialog');
  if (dlg.returnValue !== 'ok') return;
  const raw = $('#server-input').value.trim();
  try {
    const u = new URL(raw);
    if (!/^https?:$/.test(u.protocol)) throw new Error();
    setServer(u.origin + u.pathname);
    toast(`Using metadata server ${server}.`, 'info');
    loadAll();
  } catch {
    toast('Enter a valid http:// or https:// URL.', 'err');
  }
});

$('#file-search').addEventListener('input', e => { state.filters.search = e.target.value; renderFiles(); });
$('#file-status').addEventListener('change', e => { state.filters.status = e.target.value; renderFiles(); });
$('#file-integrity').addEventListener('change', e => { state.filters.integrity = e.target.value; renderFiles(); });
$('#integrity-filter').addEventListener('change', renderIntegrity);
$('#activity-type').addEventListener('change', async () => { await loadActivity(); renderActivity(); });
$$('#files-table .sort').forEach(btn => btn.addEventListener('click', () => {
  const key = btn.dataset.sort;
  if (state.filters.sort === key) state.filters.dir = state.filters.dir === 'asc' ? 'desc' : 'asc';
  else { state.filters.sort = key; state.filters.dir = key === 'name' ? 'asc' : 'desc'; }
  renderFiles();
}));

$('#file-input').addEventListener('change', e => { addUploads(e.target.files); e.target.value = ''; });

// Drag and drop: onto the upload dialog's drop zone, or onto the files table.
function wireDrop(el, onFiles) {
  let depth = 0;
  el.addEventListener('dragenter', e => { if (e.dataTransfer?.types.includes('Files')) { depth++; el.classList.add('dragging'); } });
  el.addEventListener('dragleave', () => { if (--depth <= 0) { depth = 0; el.classList.remove('dragging'); } });
  el.addEventListener('dragover', e => { if (e.dataTransfer?.types.includes('Files')) e.preventDefault(); });
  el.addEventListener('drop', e => {
    e.preventDefault();
    depth = 0;
    el.classList.remove('dragging');
    if (e.dataTransfer.files.length) onFiles(e.dataTransfer.files);
  });
}
wireDrop($('#dropzone'), files => addUploads(files));
wireDrop($('#files-panel'), files => openUpload(files));
// Stop the browser from opening files dropped anywhere else.
window.addEventListener('dragover', e => e.preventDefault());
window.addEventListener('drop', e => e.preventDefault());

window.addEventListener('hashchange', route);

/* ---------- Start ---------- */

setServer(server);
route();
loadAll();
setInterval(() => { if (document.visibilityState === 'visible') loadAll(); }, POLL_MS);
