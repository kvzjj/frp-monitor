// 资产视图真实契约回归（KVWO-111）：起真实 server.py + SQLite 夹具，把 /api/assets
// 系列的【真实响应】原样喂给前端渲染——不再用自造载荷（旧 verify_assets_ui.js 走
// 404 mock、verify_assets_real_api.js 喂 device_code，均未覆盖真实契约字段名，
// 导致 normDev 缺 device_no 时台账恒为空而套件全绿）。
// 断言：列表/归档开关/单位下钻/360°/搜索筛选全部基于真实 items 形状（device_no）。
const fs = require('fs'), vm = require('vm'), path = require('path'), os = require('os'), http = require('http');
const { execFileSync, spawn } = require('child_process');
const HERE = __dirname;
const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'assets-contract-'));

// 1) 夹具：3 台在册（苏州青柠服务商，含小写 ep-a002 与维修中样例）+ 2 台归档（含小写 ep-a901）
const baseEnv = {
  FRP_MONITOR_ASSET_DB: path.join(tmp, 'a.sqlite'),
  FRP_MONITOR_REPAIR_DB: path.join(tmp, 'r.sqlite'),
  FRP_MONITOR_REPAIR_ATTACH_DIR: path.join(tmp, 'att'),
  FRP_MONITOR_ASSET_SYNC: '0', FRP_MONITOR_REPAIR_SYNC: '0',
  PATH: process.env.PATH || '/usr/bin:/bin',
};
const prep = path.join(tmp, 'prep.py');
const prepSrc = [
  "import sys; sys.path.insert(0, '.')",
  "import server as s",
  "def D(no, st, unit='苏州青柠服务商', note=None):",
  "    return {'device_no': no, 'device_type': 'EgoPortal-Stereo-V1', 'status': st,",
  "            'unit': unit, 'accessories': '头盔、相机', 'note': note,",
  "            'software_version': None, 'modified_by': '李娜',",
  "            'updated_at': '2026-10-18T00:00:00.000+08:00'}",
  "devs = {'r1': D('EP-A001', '正常', note='一线部署'),",
  "        'r2': D('ep-a002', '维修中'),",
  "        'r3': D('EP-A003', '正常')}",
  "arch = {'ra1': D('EP-A900', '已拆解'), 'ra2': D('ep-a901', '已拆解', unit='上海办公室')}",
  "s.asset_store.replace_all('devices', devs, synced_at=1761100000.0)",
  "s.asset_store.replace_all('archived', arch, synced_at=1761100000.0)",
  "rep = {'rp1': {'record_id': 'rp1', 'device_no': 'ep-a002', 'device_type': 'EgoPortal-Stereo-V1',",
  "               'parts': None, 'unit': '苏州青柠服务商', 'stage': '维修中',",
  "               'problem': '相机模块无画面', 'owner_name': '刘希源',",
  "               'last_followup_at': '2026-10-17 14:00', 'last_followup_note': '已联系现场寄回',",
  "               'created_at': '2026-10-15 09:00'}}",
  "s.repair_store.replace_all(rep, synced_at=1761100000.0)",
  "acts = {'ac1': {'record_id': 'ac1', 'device_no': 'EP-A001', 'action': '维修跟进',",
  "                'operator': '刘希源', 'operated_at': '2026-10-15 10:00',",
  "                'note': '更换相机模组后复测', 'summary': None,",
  "                'attachments': None, 'linked_records': None}}",
  "parts_ = {'pc1': {'record_id': 'pc1', 'device_no': 'EP-A001', 'part': '相机', 'item': '相机模组',",
  "                  'arrival_status': '完好', 'status_note': '到货已核对', 'checker': '李娜',",
  "                  'checked_at': '2026-10-16 09:00', 'photos': None, 'linked_records': None}}",
  "trans = {'tr1': {'record_id': 'tr1', 'unit': '苏州青柠服务商', 'item': '整机', 'quantity': 1,",
  "                 'note': '项目发放', 'occurred_at': '2026-10-10 12:00', 'proof': None}}",
  "s.asset_store.replace_all('actions', acts, synced_at=1761100000.0)",
  "s.asset_store.replace_all('parts', parts_, synced_at=1761100000.0)",
  "s.asset_store.replace_all('transfers', trans, synced_at=1761100000.0)",
  "print('PREP_OK')",
].join('\n');
fs.writeFileSync(prep, prepSrc);
execFileSync('python3', [prep], { cwd: HERE, env: baseEnv, stdio: 'pipe' });

// 2) 起真实 server（默认 FRPS_API 127.0.0.1:7400 立即拒连 → device_online=null，亦属真实形状）
const freePort = () => new Promise((resolve, reject) => {
  const s = http.createServer();
  s.listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => resolve(p)); });
  s.on('error', reject);
});
const sleep = ms => new Promise(r => setTimeout(r, ms));
const ok = (n, c) => { if (!c) { console.error('FAIL: ' + n); process.exitCode = 1; } else console.log('ok - ' + n); };

(async () => {
  const port = await freePort();
  const srv = spawn('python3', ['server.py'], { cwd: HERE, env: { ...baseEnv, HTTP_PORT: String(port) }, stdio: 'ignore' });
  const getJson = (p) => new Promise((resolve, reject) => {
    http.get({ host: '127.0.0.1', port, path: p }, r => {
      let b = ''; r.on('data', c => b += c); r.on('end', () => r.statusCode === 200 ? resolve(JSON.parse(b)) : reject(Error('HTTP ' + r.statusCode)));
    }).on('error', reject);
  });

  // 3) 导出前端 JS + 拉取真实响应（loadAll 五接口 + 360° 详情两路：大写/小写编号）
  execFileSync('python3', ['-c',
    "import html, server; open(" + JSON.stringify(path.join(tmp, 'panel.js')) + ",'w').write(server.JS)"],
    { cwd: HERE, env: { ...baseEnv, HTTP_PORT: String(port) }, stdio: 'pipe' });
  const js = fs.readFileSync(path.join(tmp, 'panel.js'), 'utf8');

  let up = false;
  for (let i = 0; i < 60 && !up; i++) { await sleep(500); try { await getJson('/api/assets/stats'); up = true; } catch (e) { /* 启动中 */ } }
  if (!up) { console.error('FAIL: server 未在 30s 内就绪'); srv.kill(); process.exit(1); }

  const listP = await getJson('/api/assets');                                   // 真实：{items:[…],total,…}
  const allP = await getJson('/api/assets?include_archived=1');                 // 真实：在册+归档全量
  const statsP = await getJson('/api/assets/stats');
  const unitsP = await getJson('/api/assets/units');
  const versP = await getJson('/api/assets/versions');
  const det1 = await getJson('/api/assets/EP-A001');
  const det2 = await getJson('/api/assets/ep-a002');                            // 小写编号（真实台账存在形态）
  ok('真实契约：/api/assets 为 {items,total} 形状且条目含 device_no 字段',
     Array.isArray(listP.items) && listP.items.length === 3 && 'device_no' in listP.items[0]);
  ok('真实契约：include_archived=1 返回 5 条且条目自带 archived 标记（2 条 true）',
     allP.items.length === 5 && allP.items.filter(i => i.archived).length === 2);
  ok('真实契约：条目含 device_online/open_repairs（fleet 未启用时为 null 属实）',
     'device_online' in listP.items[0] && 'active_repairs' in listP.items[0]);

  // 4) 前端 VM：真实响应原样喂给 Assets.loadAll（fetch 按路径回放，绝不自造字段名）
  class El {
    constructor(id) { this.id = id; this._html = ''; this.hidden = false; this.value = ''; this.textContent = '';
      this.dataset = {}; this.style = {}; this.listeners = {}; this.onclick = null; this.checked = false;
      this.title = ''; this._cls = new Set(); }
    set innerHTML(v) { this._html = String(v); } get innerHTML() { return this._html; }
    get classList() { const s = this._cls; return { add: c => s.add(c), remove: c => s.delete(c),
      toggle: (c, on) => { (on === undefined ? !s.has(c) : on) ? s.add(c) : s.delete(c); }, contains: c => s.has(c) }; }
    addEventListener(t, fn) { (this.listeners[t] = this.listeners[t] || []).push(fn); }
    querySelectorAll() { return []; } querySelector() { return null; }
    closest() { return null; } appendChild() {} remove() {} scrollIntoView() {}
  }
  const byId = {}; const el = id => byId[id] || (byId[id] = new El(id));
  const navBtns = ['overview', 'assets', 'upgrade'].map(v => { const b = new El('btn-' + v); b.dataset.view = v; return b; });
  const tabBtns = ['dashboard', 'hardware', 'repairs', 'versions', 'units'].map(v => { const b = new El('abtn-' + v); b.dataset.atab = v; return b; });
  const document = {
    getElementById: id => (id === 'viewTabs' ? Object.assign(el(id), { querySelectorAll: () => navBtns })
      : id === 'assetTabs' ? Object.assign(el(id), { querySelectorAll: () => tabBtns }) : el(id)),
    querySelector: sel => el('sel:' + sel),
    querySelectorAll: sel => sel.includes('viewTabs') ? navBtns : (sel.includes('assetTabs') ? tabBtns : []),
    createElement: () => new El('tmp'), addEventListener: () => {}, body: new El('body'),
  };
  let HASH = '#view=assets';
  const location = { get hash() { return HASH; }, set hash(v) { HASH = '#' + v; }, pathname: '/', search: '' };
  const winL = {};
  const window = { addEventListener: (t, fn) => { (winL[t] = winL[t] || []).push(fn); }, Assets: null };
  const history = { replaceState: (a, b, url) => { const h = String(url).split('#')[1]; HASH = h ? '#' + h : ''; } };
  const FEED = {
    '/api/assets/stats': statsP, '/api/assets/units': unitsP, '/api/assets/versions': versP,
    '/api/assets': listP, '/api/assets?include_archived=1': allP,
    '/api/assets/EP-A001': det1, '/api/assets/ep-a002': det2,
  };
  const BOOT_POLL = ['/api/hosts', '/api/events?hours=24&limit=60', '/api/collect-duration?days=7', '/api/webfix/tasks'];   // 总览/升级视图初始化拉取，与资产契约无关
  const fetchMock = async (p) => {
    const hit = FEED[p];
    if (hit !== undefined) return { status: 200, ok: true, json: async () => hit };
    if (!BOOT_POLL.includes(p)) console.error('unexpected fetch: ' + p);
    return { status: 404, ok: false, json: async () => null };
  };
  const ctx = { document, window, location, history, fetch: fetchMock, setInterval: () => 0,
    clearInterval: () => {}, setTimeout: fn => 0, clearTimeout: () => {}, console, URLSearchParams, Date, JSON,
    Object, Array, String, Number, Math, Error, isNaN, parseInt, encodeURIComponent, decodeURIComponent, alert: () => {},
    navigator: { clipboard: { writeText: async () => true } } };
  ctx.globalThis = ctx;
  vm.createContext(ctx);
  vm.runInContext(js + '\n;globalThis.__T={Assets:Assets};', ctx);
  const A = ctx.__T.Assets;
  const rows = () => el('asLedgerBody').innerHTML.split('<tr data-code=').length - 1;

  await sleep(150);
  await A.loadAll();
  ok('真实数据：mock 未启用', A.mock === false);
  ok('normDev 解析 device_no：Assets.devices = 3 台（KVWO-111 修复前为 0）', A.devices.length === 3);
  ok('台账默认渲染 3 台在册', rows() === 3);
  ok('台账行含真实编号（小写 ep-a002 显示为大写）', el('asLedgerBody').innerHTML.includes('>EP-A002<'));
  ok('归档列表 = 2 台（include_archived 按标记拆分，非在册重复）', A.archived.length === 2);
  A.hw.archived = true; el('asArchived').checked = true; A.renderLedger();
  ok('勾选归档 → 3+2=5 台（248+69 形态的小样）', rows() === 5);
  ok('归档行淡色标记 + 小写 ep-a901 在列', el('asLedgerBody').innerHTML.includes('class=aarch') && el('asLedgerBody').innerHTML.includes('data-code="ep-a901"'));
  A.hw.archived = false; A.renderLedger();
  // 搜索 / 筛选（真实数据）
  A.hw.q = 'ep-a002'; A.renderLedger();
  ok('关键词 ep-a002（大小写不敏感）→ 1 台', rows() === 1);
  A.hw.q = ''; A.hw.status = '维修中'; A.renderLedger();
  ok('筛选状态=维修中 → 1 台', rows() === 1);
  A.hw.status = ''; A.renderLedger();
  // 单位下钻
  A.go('units'); A.drill('苏州青柠服务商');
  ok('单位下钻显示 3 台在册', el('asUnitDetail').innerHTML.includes('3 台在册'));
  ok('下钻设备表含真实编号', el('asUnitDetail').innerHTML.includes('EP-A003'));
  // 360°（真实 detail 载荷，大写/小写编号两路 + 新对齐的 actions/parts_checks/transfers 段）
  await A.open360('EP-A001');
  const m1 = el('modalBody').innerHTML;
  ok('360°：EP-A001 台账信息（真实 detail 响应）', m1.includes('台账信息') && m1.includes('EP-A001'));
  ok('360°：处理流水段渲染真实 actions（动作/操作人/说明）', m1.includes('处理流水') && m1.includes('维修跟进') && m1.includes('刘希源') && m1.includes('更换相机模组后复测'));
  ok('360°：配件核对段渲染真实 parts_checks（配件/到货状态）', m1.includes('配件核对') && m1.includes('相机模组') && m1.includes('完好'));
  ok('360°：流转记录段渲染真实 transfers（occurred_at/quantity）', m1.includes('流转记录') && m1.includes('项目发放'));
  await A.open360('ep-a002');
  const m2 = el('modalBody').innerHTML;
  ok('360°：小写编号路由同样命中并大写展示', m2.includes('EP-A002'));
  ok('360°：维修记录段渲染真实字段（problem/owner_name/last_followup_note）', m2.includes('相机模块无画面') && m2.includes('已联系现场寄回'));
  ok('列表 open_repairs 联动：ep-a002 未完结维修 1 条', (A.devices.find(d => d.code === 'ep-a002') || {}).openRepairs === 1);
  ok('同步横幅为正常态（非 mock 提示）', el('asSyncBar').className.includes('ok') || !el('asSyncBar').innerHTML.includes('mock'));

  srv.kill();
  if (!process.exitCode) console.log('\nREAL-CONTRACT ALL PASS（真实 server 响应原样喂前端）');
  process.exit(process.exitCode || 0);
})().catch(e => { console.error('FAIL:', e.message); srv.kill(); process.exit(1); });
