// 资产视图（KVWO-84）最小 DOM 冒烟测试：mock 回退、渲染、筛选/排序、下钻、360°、跳转
const fs = require('fs'), vm = require('vm');
const js = fs.readFileSync('/tmp/panel_test.js', 'utf8');

class El {
  constructor(id) { this.id = id; this._html = ''; this.hidden = false; this.value = ''; this.textContent = '';
    this.dataset = {}; this.style = {}; this.listeners = {}; this.onclick = null; this.checked = false;
    this._cls = new Set(); }
  set innerHTML(v) { this._html = String(v); } get innerHTML() { return this._html; }
  get classList() { const s = this._cls; return { add: c => s.add(c), remove: c => s.delete(c),
    toggle: (c, on) => { (on === undefined ? !s.has(c) : on) ? s.add(c) : s.delete(c); }, contains: c => s.has(c) }; }
  addEventListener(t, fn) { (this.listeners[t] = this.listeners[t] || []).push(fn); }
  fire(t, ev) { for (const fn of this.listeners[t] || []) fn(ev); }
  querySelectorAll() { return []; } querySelector() { return null; }
  closest() { return null; } appendChild() {} remove() {} scrollIntoView() {}
}
const byId = {};
const el = id => byId[id] || (byId[id] = new El(id));
// 预置导航按钮（setView 会遍历）
for (const [i, v] of ['overview', 'assets', 'upgrade'].entries()) el('viewTabs');
const navBtns = ['overview', 'assets', 'upgrade'].map(v => { const b = new El('btn-' + v); b.dataset.view = v; return b; });
const tabBtns = ['dashboard', 'hardware', 'repairs', 'versions', 'units'].map(v => { const b = new El('abtn-' + v); b.dataset.atab = v; return b; });

const document = {
  getElementById: id => {
    if (id === 'viewTabs') { const t = el(id); t.querySelectorAll = () => navBtns; return t; }
    if (id === 'assetTabs') { const t = el(id); t.querySelectorAll = () => tabBtns; return t; }
    return el(id);
  },
  querySelector: sel => el('sel:' + sel),
  querySelectorAll: sel => sel.includes('viewTabs') ? navBtns : (sel.includes('assetTabs') ? tabBtns : []),
  createElement: () => new El('tmp'), addEventListener: () => {}, body: new El('body'),
};
let HASH = '#view=assets';
const location = { get hash() { return HASH; }, set hash(v) { HASH = '#' + v; for (const fn of (winL.hashchange || [])) fn(); }, pathname: '/', search: '' };
const winL = {};
const window = { addEventListener: (t, fn) => { (winL[t] = winL[t] || []).push(fn); }, Assets: null };
const history = { replaceState: (a, b, url) => { const h = String(url).split('#')[1]; HASH = h ? '#' + h : ''; } };
const intervals = [];
const fetchLog = [];
const fetchMock = async (path) => {
  fetchLog.push(path);
  if (path.startsWith('/api/assets')) return realApi(path);
  if (path.startsWith('/api/hosts')) return { status: 502, ok: false, json: async () => ({ error: 'frps 不可用' }) };
  return { status: 404, ok: false, json: async () => null };
};
const ctx = { document, window, location, history, fetch: fetchMock, setInterval: (fn) => { intervals.push(fn); return 0; },
  clearInterval: () => {}, setTimeout: (fn) => 0, clearTimeout: () => {}, console, URLSearchParams, Date, JSON,
  Object, Array, String, Number, Math, Error, isNaN, parseInt, encodeURIComponent, decodeURIComponent, alert: () => {},
  navigator: { clipboard: { writeText: async () => true } } };
ctx.globalThis = ctx;
vm.createContext(ctx);

const statsFix = { registered_total: 100, archived_total: 5,
  inventory: { total: 20, by_unit: { '上海办公室': 12, '潍坊办公室': 8 }, units: ['上海办公室', '潍坊办公室'], unused_note: 8 },
  deployed: 80, by_status: { '正常': 90, '维修中': 6, '待维修': 4 }, by_type: { 'EgoPortal-Stereo-V0': 70, 'EgoPortal-Lite': 30 },
  by_unit: { '上海办公室': 12, '潍坊办公室': 8, '苏州项目组': 80 }, repair_rate: 0.1, damage_rate: 0.1, last_sync_at: '2026-10-20T09:00:00+08:00' };
let mode = 'real';
const realApi = async (path) => {
  if (mode !== 'real') return { status: 500, ok: false, json: async () => ({ error: '数据库不可用' }) };
  const J = o => ({ status: 200, ok: true, json: async () => o });
  if (path.includes('stats')) return J(statsFix);
  if (path.includes('units')) return J({ units: [{ unit: '上海办公室', total: 12, normal_rate: 0.9, repairing: 1, abnormal: 1, last_updated: '2026-10-20T08:00:00+08:00' }, { unit: '潍坊办公室', total: 8, normal_rate: 1, repairing: 0, abnormal: 0, last_updated: '2026-10-20T08:00:00+08:00' }, { unit: '苏州项目组', total: 80, normal_rate: 0.95, repairing: 5, abnormal: 3, last_updated: '2026-10-19T18:00:00+08:00' }] });
  if (path.includes('versions')) return J({ detected: [{ version: 'v0.5.0', count: 40 }], ledger: [], ledger_empty: 100, matched: 40, detected_only: ['ep-888888'], ledger_missing: [] });
  if (path.includes('?')) return J({ devices: [] });
  if (path.startsWith('/api/assets/')) return J({ ledger: { device_code: path.split('/').pop(), device_type: 'EgoPortal-Stereo-V0', status: '正常', unit: '苏州项目组' }, repairs: [], flows: [], parts: [] });
  return J({ devices: [{ device_code: 'EP-200001', device_type: 'EgoPortal-Stereo-V0', status: '维修中', unit: '苏州项目组', accessories: '头盔、相机', remark: '一线部署', detected_version: 'v0.5.0', device_online: true, open_repairs: 2, updated_at: '2026-10-19T10:00:00+08:00' }] });
};
vm.runInContext(js + '\n;globalThis.__T={Assets:Assets};', ctx);
const A = ctx.__T.Assets;
const sleep = ms => new Promise(r => setTimeout(r, ms));
const ok = (n, c) => { if (!c) { console.error('FAIL: ' + n); process.exitCode = 1; } else console.log('ok - ' + n); };
(async () => {
  await sleep(120);
  await A.loadAll();
  ok('真实数据：mock 未启用', A.mock === false);
  ok('真实数据：在册 100 来自 /api/assets/stats', el('asMetrics').innerHTML.includes('>100<'));
  ok('真实数据：台账 1 台 EP-200001', el('asLedgerBody').innerHTML.includes('EP-200001'));
  ok('真实数据：同步横幅为正常态', el('asSyncBar').className.includes('ok') && el('asSyncBar').innerHTML.includes('飞书同步于'));
  await A.open360('EP-200001');
  ok('真实数据：360° 详情', el('modalBody').innerHTML.includes('台账信息') && el('modalBody').innerHTML.includes('EP-200001'));
  mode = 'down';
  await A.loadAll();
  ok('断供：保留最后成功数据', el('asLedgerBody').innerHTML.includes('EP-200001'));
  ok('断供：错误横幅含最后成功时间', el('asSyncBar').className.includes('err') && el('asSyncBar').innerHTML.includes('最后成功数据'));
  if (!process.exitCode) console.log('REAL-API PATH ALL PASS');
})();
