// 筛选模型（KVWO-115 F9）自动化断言：设备总览 / 升级两处接入点 + URL 序列化 + 窄屏规则。
// 交 KVWO-116 复用：node verify_filters_ui.js
// 覆盖验收条目：S2（多选=或 / 字段间=且）、S5（URL 往返）、S6（内置快捷视图）、
//               S7（空结果态写明条件）、S11（升级默认 0 选中 / 选中当前筛选只勾在线 / 确认清单完整）、
//               U10（窄屏筛选折叠与表格转卡片的 CSS 规则存在）
const fs = require('fs'), vm = require('vm'), path = require('path');
const { execFileSync } = require('child_process');
execFileSync('python3', ['-c',
  'import html, server; open("/tmp/panel_filters.js","w").write(server.JS); ' +
  'open("/tmp/panel_filters_index.html","w").write(server.INDEX.format(css=server.CSS, js=server.JS, ' +
  'title=html.escape(server.panel_title(), quote=False)))'],
  { cwd: __dirname, stdio: 'pipe' });
const js = fs.readFileSync('/tmp/panel_filters.js', 'utf8');
const IDX = fs.readFileSync('/tmp/panel_filters_index.html', 'utf8');

class El {
  constructor(id) { this.id = id; this._html = ''; this.hidden = false; this.value = ''; this.textContent = '';
    this.dataset = {}; this.style = {}; this.listeners = {}; this.onclick = null; this.checked = false;
    this.title = ''; this._cls = new Set(); }
  set innerHTML(v) { this._html = String(v); } get innerHTML() { return this._html; }
  get classList() { const s = this._cls; return { add: c => s.add(c), remove: c => s.delete(c),
    toggle: (c, on) => { (on === undefined ? !s.has(c) : on) ? s.add(c) : s.delete(c); }, contains: c => s.has(c) }; }
  addEventListener(t, fn) { (this.listeners[t] = this.listeners[t] || []).push(fn); }
  fire(t, ev) { for (const fn of (this.listeners[t] || [])) fn(ev); }
  querySelectorAll() { return []; } querySelector() { return null; }
  closest() { return null; } appendChild() {} remove() {} scrollIntoView() {} focus() {}
}
const byId = {};
const el = id => byId[id] || (byId[id] = new El(id));
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
  activeElement: null,
};
let HASH = '#view=upgrade';
const location = { get hash() { return HASH; }, set hash(v) { HASH = '#' + v; for (const fn of (winL.hashchange || [])) fn(); }, pathname: '/', search: '' };
const winL = {};
const window = { addEventListener: (t, fn) => { (winL[t] = winL[t] || []).push(fn); } };
const history = { replaceState: (a, b, url) => { const h = String(url).split('#')[1]; HASH = h ? '#' + h : ''; } };
const intervals = [], fetchLog = [];

/* ---- mock：20 台设备 / 3 站点 / 3 版本，在线 13 台；便于手算 OR/AND ---- */
const SITES = [['上海办公室', 8], ['潍坊办公室', 7], ['苏州项目组', 5]];
const fleet = [], sites = [];
let n = 0;
for (const [site, cnt] of SITES) {
  let on = 0;
  for (let i = 0; i < cnt; i++) {
    n++;
    const online = n % 3 !== 0;                    // 每 3 台 1 台离线
    if (online) on++;
    fleet.push({ hostname: 'ep-1000' + String(n).padStart(2, '0'), device_id: 'EP-1000' + String(n).padStart(2, '0'),
      device_name: '采集-' + n, site, ip: '10.0.0.' + n, online, version: ['v0.4.6', 'v0.4.5', 'v0.5.0'][n % 3],
      hw_model: ['OrangePi-5', 'N100'][n % 2], hardware: 'v1', ssh_port: 21000 + n, web_port: 6000 + n,
      connections: online ? 1 : null });
  }
  sites.push({ site, expected: cnt, online: on });
}
const HOSTS = { fleet, sites, versions: [], ssh_hint: {}, refresh_interval: 15, timestamp: Math.floor(Date.now() / 1000), online: fleet.filter(f => f.online).length, registered: fleet.length, total: fleet.length };
const jr = (b, st) => ({ ok: (st || 200) < 400, status: st || 200, json: async () => b });
const fetchMock = async (p) => {
  fetchLog.push(p);
  if (p.startsWith('/api/hosts')) return jr(HOSTS);
  if (p.startsWith('/api/events')) return jr({ enabled: false, events: [] });
  if (p.startsWith('/api/collect-duration')) return jr({ enabled: false, items: [] });
  if (p.startsWith('/api/webfix/tasks')) return jr({ enabled: false, tasks: [], candidates: [] });
  if (p.startsWith('/api/assets')) return jr(null, 404);
  if (p.startsWith('/api/repairs/stats')) return jr({ total: 0, active_count: 0, by_stage: {} });
  if (p.startsWith('/api/repairs')) return jr({ repairs: [] });
  if (p.startsWith('/api/upgrade/jobs')) return jr({ jobs: [] });
  if (p.startsWith('/api/upgrade/packages')) return jr({ packages: [] });
  return jr(null, 404);
};
const ctx = { document, window, location, history, fetch: fetchMock, setInterval: (fn) => { intervals.push(fn); return 0; },
  clearInterval: () => {}, setTimeout: (fn) => { try { fn(); } catch (e) {} return 0; }, clearTimeout: () => {},
  console, URLSearchParams, Date, JSON, Object, Array, String, Number, Math, Error, Promise, isNaN, parseInt,
  encodeURIComponent, decodeURIComponent, alert: () => {}, navigator: { clipboard: { writeText: async () => true } } };
ctx.globalThis = ctx;
vm.createContext(ctx);
vm.runInContext(js + '\n;globalThis.__T={Assets:Assets,Upg:Upg,Rep:Rep,FS:FS,URLState:URLState,FilterBar:FilterBar,ovFs:()=>ovFs,filtered:()=>filtered(),fleet:()=>fleet};', ctx);
const T = ctx.__T, FSx = T.FS, U = T.Upg;
const sleep = ms => new Promise(r => setTimeout(r, ms));
const ok = (name, cond) => { if (!cond) { console.error('FAIL: ' + name); process.exitCode = 1; } else console.log('ok - ' + name); };

(async () => {
  await sleep(300);
  const ov = () => T.ovFs();

  // ---- S2：同字段多选=或、字段之间=且 ----
  FSx.clear(ov()); FSx.set(ov(), 'site', ['上海办公室', '潍坊办公室']);
  const orN = T.filtered().length;
  const expOr = T.fleet().filter(f => f.site === '上海办公室' || f.site === '潍坊办公室').length;
  ok('S2 总览：两个站点=或（结果=两站点台数之和 15）', orN === expOr && expOr === 15);
  FSx.set(ov(), 'online', ['1']);
  const andN = T.filtered().length;
  const expAnd = T.fleet().filter(f => (f.site === '上海办公室' || f.site === '潍坊办公室') && f.online).length;
  ok('S2 总览：站点 2 个 ∧ 在线 → 结果同时满足两个字段', andN === expAnd && andN < orN);
  ok('S2 总览：结果逐条满足条件（无越界）', T.filtered().every(f => ['上海办公室', '潍坊办公室'].includes(f.site) && f.online));
  FSx.clear(ov()); FSx.set(ov(), 'online', ['0']);
  ok('S2 总览：字段内多选值互换后结果互补（离线 = 总数 - 在线）', T.filtered().length === T.fleet().length - HOSTS.online);

  // ---- S5：URL 序列化往返 ----
  FSx.clear(ov()); FSx.set(ov(), 'site', ['上海办公室', '苏州项目组']); FSx.set(ov(), 'online', ['1']); ov().q = 'ep-1';
  const enc = FSx.encode(ov());
  const dec = FSx.decode(enc); dec.q = ov().q;
  ok('S5 URL：f 段可读（字段:值,值;字段:值）且往返一致', /^site:.+;online:1$/.test(enc) && JSON.stringify(dec) === JSON.stringify(ov()) && decodeURIComponent(enc).includes('上海办公室'));
  ok('S5 URL：f 段值走 encodeURIComponent（可直接手改）', enc.includes('%E4%B8%8A%E6%B5%B7%E5%8A%9E%E5%85%AC%E5%AE%A4'));
  T.URLState.write({ q: ov().q, f: enc });
  const back = T.URLState.readFs();
  ok('S5 URL：写入 hash 后重新读取得到同一组条件', JSON.stringify(back) === JSON.stringify(ov()));

  // ---- S6：内置快捷视图 ----
  const sc = el('ovFilter').innerHTML;
  ok('S6 总览：快捷视图按钮（待升级 / 未上线 / 在线）都在筛选条上', sc.includes('待升级') && sc.includes('未上线') && sc.includes('在线') && sc.includes('data-sc'));
  FSx.clear(ov()); FSx.set(ov(), 'pending', ['1']);
  ok('S6 总览：待升级 = 版本 ≠ 升级目标', T.filtered().every(f => (f.version || '未获取') !== el('targetSel').value));

  // ---- S7：空结果态 ----
  FSx.clear(ov()); FSx.set(ov(), 'site', ['不存在的站点']);
  vm.runInContext('renderGrid()', ctx);
  const grid = el('sel:#grid').innerHTML;   // 总览用 $() = document.querySelector
  ok('S7 总览：空结果写明当前条件 + 引导清空', grid.includes('没有匹配的设备') && grid.includes('清空') && grid.includes('站点'));

  // ---- S11：升级页 ----
  ok('S11 升级：进入页面默认 0 选中（不再预选全部在线）', U.selected.size === 0);
  ok('S11 升级：可选中集合只含在线设备', U.selectable().length === HOSTS.online && U.selectable().every(h => fleet.find(f => f.hostname === h).online));
  FSx.clear(U.upgFs); FSx.set(U.upgFs, 'site', ['上海办公室']);
  ok('S11 升级：选中当前筛选只勾当前筛选内的在线设备', U.selectable().length === fleet.filter(f => f.site === '上海办公室' && f.online).length);
  const picked = U.selectable().slice(0, 15);
  U.selected = new Set(picked.concat([picked[0]]));
  U.param = Object.assign({}, U.param, { ok: true, version: 'v0.4.6', arch: 'rk3588-arm64', filename: 'ego-portal-v0.4.6-rk3588-arm64.tar.gz', sha256: 'a'.repeat(64) });
  U.showConfirm();
  const conf = el('upgModalBody').innerHTML;
  ok('S11 升级：二次确认清单完整列出全部选中设备（不再截断前 12 台）', picked.every(h => conf.includes(h)) && conf.includes('全部列出') && !/等 \d+ 台/.test(conf));

  // ---- U10：窄屏规则存在 ----
  ok('U10 窄屏：筛选折叠为「搜索 + 筛选(N)」（<=768 隐藏字段按钮、显示 sheet 按钮）', /@media \(max-width:768px\)[\s\S]*\.fbar \.fbtn[\s\S]*display:none/.test(IDX) && /\.fsheetbtn \{\s*display:inline-flex/.test(IDX));
  ok('U10 窄屏：表格转卡片列表（thead 隐藏 + td::before 用 data-label）', /\.astable thead \{\s*display:none/.test(IDX) && /\.astable td::before[\s\S]*content:attr\(data-label\)/.test(IDX));

  console.log('\nfetch 序列（前 6）：', fetchLog.slice(0, 6).join(', '));
  if (!process.exitCode) console.log('\nALL PASS');
})();
