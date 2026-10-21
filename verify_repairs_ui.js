// 维修看板（KVWO-80，并入资产区子页签——KVWO-88）最小 DOM 冒烟测试：
// 统计卡/7 阶段看板/列表折叠/筛选传参/下拉计数/详情 17 字段/契约容错/降级横幅/入口整合与深链兼容。
// 自包含：先从 server.py 提取浏览器实际收到的 JS 与页面 HTML（经 Python 转义求值，非源码文本）。
const fs = require('fs'), vm = require('vm'), path = require('path');
const { execFileSync } = require('child_process');
execFileSync('python3', ['-c',
  'import html, server; open("/tmp/panel_test.js","w").write(server.JS); ' +
  'open("/tmp/panel_index.html","w").write(server.INDEX.format(css=server.CSS, js=server.JS, ' +
  'title=html.escape(server.panel_title(), quote=False)))'],   // INDEX 含 {title}（KVWO-97），与 server.py 渲染路径一致
  { cwd: path.join(__dirname), stdio: 'pipe' });
const js = fs.readFileSync('/tmp/panel_test.js', 'utf8');
const IDX = fs.readFileSync('/tmp/panel_index.html', 'utf8');

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
  closest() { return null; } appendChild() {} remove() {} scrollIntoView() {}
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
};
let HASH = '#view=repairs';   // 初始深链：旧维修页签入口（兼容路由验证）
const location = { get hash() { return HASH; }, set hash(v) { HASH = '#' + v; for (const fn of (winL.hashchange || [])) fn(); }, pathname: '/', search: '' };
const winL = {};
const window = { addEventListener: (t, fn) => { (winL[t] = winL[t] || []).push(fn); } };
const history = { replaceState: (a, b, url) => { const h = String(url).split('#')[1]; HASH = h ? '#' + h : ''; } };
const intervals = [];
const fetchLog = [];

/* ---- mock 数据：27 条，对齐飞书异常设备表现状（已完结 16 / 维修中 5 / 待接收 5 / 待发出 1） ---- */
const T0 = Date.now();
const iso = ms => new Date(ms).toISOString();
const mk = (i, stage, extra) => Object.assign({
  record_id: 'rec' + String(i).padStart(3, '0'),
  device_id: 'EP-' + String(100 + i).padStart(6, '0'),
  device_type: i % 3 === 0 ? '双目' : (i % 3 === 1 ? '单目' : ''),
  unit: ['上海办公室', '潍坊办公室', '构元服务商'][i % 3],
  stage,
  problem: '设备 ' + i + ' 相机模块无画面，待检测',
  owner: i % 2 ? '刘希源' : '张伟',
  last_followup_at: iso(T0 - (i % 7 + 1) * 3600e3),
  last_followup_note: '跟进说明 ' + i,
  created_at: iso(T0 - (20 + i) * 86400e3),
  feishu_url: 'https://feishu.example/base/rec' + i,
}, extra || {});
const RECORDS = [];
for (let i = 1; i <= 5; i++) RECORDS.push(mk(i, '待接收'));
RECORDS[0].device_online = true;
RECORDS[0].attachments = [{ name: '正面.jpg', url: '/repair-attachments/tok1/a.jpg' }];
RECORDS[1].device_online = false;                       // rec002 离线
// rec003：device_online 未匹配（null）+ 缺失字段
delete RECORDS[2].device_online; delete RECORDS[2].last_followup_note; delete RECORDS[2].unit;
// rec004：候选键容错（device_no/description）+ device_online 字符串形态
RECORDS[3] = Object.assign({}, RECORDS[3], { device_id: undefined, problem: undefined, device_online: 'true', device_no: 'EP-000104', description: '候选键：电源故障' });
RECORDS[4].device_online = 'false';
for (let i = 6; i <= 10; i++) RECORDS.push(mk(i, '维修中'));
RECORDS.push(mk(11, '待发出', { problem: '相机<x>模块异常' }));   // XSS 转义样例
for (let i = 12; i <= 27; i++) RECORDS.push(mk(i, '已完结'));
const STATS_OK = { total: 27, active_count: 11,
  by_stage: { '待接收': 5, '损坏原因确认中': 0, '维修中': 5, '待发出确认': 0, '待发出': 1, '已发出': 0, '已完结': 16 },
  last_sync_at: iso(T0 - 600e3) };
const DETAIL = { record_id: 'rec001', device_id: 'EP-000101', device_type: '双目', unit: '上海办公室',
  stage: '待接收', problem: '相机模块无画面，待检测', owner: '刘希源',
  last_followup_at: iso(T0 - 3600e3), created_at: iso(T0 - 30 * 86400e3),
  device_online: true, feishu_url: 'https://feishu.example/base/rec001', accessories: ['相机'],
  attachments: [{ name: '正面.jpg', url: '/repair-attachments/tok1/a.jpg' }, { name: '检测报告.pdf', url: '/repair-attachments/tok1/b.pdf' }] };

let mode = 'ok';
const jresp = (status, body) => ({ status, ok: status < 400, json: async () => body });
const fetchMock = async (p) => {
  fetchLog.push(p);
  if (p.startsWith('/api/repairs/stats')) {
    if (mode === 'stats404') return jresp(404, { error: 'not found' });
    if (mode === 'net') throw new TypeError('fetch failed');
    if (mode === 'empty') return jresp(200, { total: 0, active_count: 0, by_stage: {}, last_sync_at: iso(T0 - 600e3) });
    if (mode === 'syncerr') return jresp(200, Object.assign({}, STATS_OK, { sync_error: 'lark-cli 认证过期' }));
    return jresp(200, STATS_OK);
  }
  if (p.startsWith('/api/repairs/')) {
    if (mode === 'detail404') return jresp(404, { error: 'not found' });
    return jresp(200, DETAIL);
  }
  if (p.startsWith('/api/repairs')) {
    if (mode === 'list404') return jresp(404, { error: 'not found' });
    if (mode === 'net') throw new TypeError('fetch failed');
    if (mode === 'empty') return jresp(200, { repairs: [] });
    return jresp(200, { repairs: RECORDS });
  }
  if (p.startsWith('/api/assets')) return jresp(404, null);
  if (p.startsWith('/api/hosts')) return jresp(502, { error: 'frps 不可用' });
  return jresp(404, null);
};
const ctx = { document, window, location, history, fetch: fetchMock, setInterval: (fn) => { intervals.push(fn); return 0; },
  clearInterval: () => {}, setTimeout: (fn) => 0, clearTimeout: () => {}, console, URLSearchParams, Date, JSON,
  Object, Array, String, Number, Math, Error, Promise, isNaN, parseInt, encodeURIComponent, decodeURIComponent,
  alert: () => {}, navigator: { clipboard: { writeText: async () => true } } };
ctx.globalThis = ctx;
vm.createContext(ctx);
vm.runInContext(js + "\n;globalThis.__T={Assets:Assets,Upg:Upg,Rep:Rep};", ctx);

const A = ctx.__T.Assets, UpgT = ctx.__T.Upg, R = ctx.__T.Rep;
const sleep = ms => new Promise(r => setTimeout(r, ms));
const ok = (name, cond) => { if (!cond) { console.error('FAIL: ' + name); process.exitCode = 1; } else console.log('ok - ' + name); };
const lastFetch = () => fetchLog[fetchLog.length - 1];

(async () => {
  await sleep(300); // 等 Upg.init → route → Assets.enter → loadAll → renderRepairs → Rep.enter → load
  // ---- 入口整合：深链 #view=repairs 兼容（全站唯一维修入口 = 资产区「维修看板」子页签） ----
  ok('深链 #view=repairs → 资产视图 + 维修看板子页签', !el('viewAssets').hidden && !el('atabRepairs').hidden && A.tab === 'repairs');
  ok('深链进入自动拉取维修统计与列表', fetchLog.some(p => p.startsWith('/api/repairs/stats')) && (fetchLog.includes('/api/repairs') || fetchLog.some(p => p.startsWith('/api/repairs?'))));
  ok('顶部导航无独立「维修」页签（唯一入口=子页签）', !IDX.includes('data-view=repairs') && IDX.includes('data-atab=repairs>') && IDX.includes('id=atabRepairs') && !IDX.includes('viewRepairs') && !IDX.includes('asRepairsSlot'));
  // ---- 统计卡 ----
  ok('统计卡：未完结 11 / 总数 27 / 已完结 16', String(el('repActive').textContent) === '11' && String(el('repTotal').textContent) === '27' && String(el('repDoneN').textContent) === '16');
  ok('统计卡：最后同步相对时间 + 悬停绝对时间', el('repSync').textContent !== '—' && el('repSync').title.includes('最后成功同步'));
  ok('阶段 chips：7 阶段齐全且计数对齐 by_stage', (el('repStageStats').innerHTML.match(/class="chip rchip"/g) || []).length === 7 && el('repStageStats').innerHTML.includes('待接收 <b>5</b>') && el('repStageStats').innerHTML.includes('损坏原因确认中 <b>0</b>') && el('repStageStats').innerHTML.includes('已完结 <b>16</b>'));
  // ---- 看板视图 ----
  const kan = el('repKanban').innerHTML;
  ok('看板：7 列固定顺序（空阶段保留列头）', (kan.match(/class="rkcol"/g) || []).length === 7 && kan.includes('data-col="损坏原因确认中"'));
  ok('看板：列计数正确（待接收 5 / 已完结 16，看板不折叠已完结）', kan.includes('待接收</span><span class="rkn">5</span>') && kan.includes('已完结</span><span class="rkn">16</span>'));
  ok('看板：27 张卡片全量展示', (kan.match(/class="rcard"/g) || []).length === 27);
  ok('看板：设备编号 + 单目/双目标记', kan.includes('EP-000101') && kan.includes('rt2') && kan.includes('rt1'));
  ok('看板：问题摘要 + title 完整文本', kan.includes('rprob') && kan.includes('title="设备 1 相机模块无画面，待检测"'));
  ok('看板：台账在线三态徽章各就位', kan.includes('ron on link') && kan.includes('ron off link') && kan.includes('ron na'));
  ok('看板：负责人渲染', kan.includes('刘希源') && kan.includes('张伟'));
  ok('看板：XSS 转义（< > 实体化，无裸标签注入）', kan.includes('相机&lt;x&gt;模块异常') && !kan.includes('相机<x>'));
  ok('看板：最后跟进相对时间', kan.includes(' 小时前</span>') || kan.includes(' 分钟前</span>') || kan.includes(' 天前</span>'));
  // ---- 列表视图 ----
  R.setView('list');
  const tb = el('repTbody').innerHTML;
  ok('列表：未完结 11 行（已完结折叠不混排）', (tb.split('<tr data-rid=').length - 1) === 11);
  ok('列表：排序阶段优先（待接收在维修中之前）', tb.indexOf('EP-000101') < tb.indexOf('EP-000106'));
  ok('列表：已完结折叠区 16 行默认收起', !el('repDoneWrap').hidden && el('repDoneTable').hidden && (el('repDoneTbody').innerHTML.split('<tr data-rid=').length - 1) === 16);
  R.doneOpen = true; R.renderList();
  ok('列表：展开已完结（文案带计数）', !el('repDoneTable').hidden && el('repDoneToggle').textContent.includes('已完结记录（16）'));
  R.doneOpen = false; R.renderList();
  ok('列表：行内阶段标签配色 + 编号等宽样式', tb.includes('style="--c:#dc2626"') && tb.includes('class="rdev"'));
  // ---- 筛选与搜索传参（查询参数走 API，与后端筛选一致） ----
  R.q = 'EP-000101'; await R.loadList();
  ok('搜索：?q= 精确传参', lastFetch() === '/api/repairs?q=EP-000101');
  R.q = ''; R.stageF = '待接收'; await R.loadList();
  ok('阶段筛选：?stage= 传参（仅列表视图）', lastFetch() === '/api/repairs?stage=' + encodeURIComponent('待接收'));
  R.typeF = '单目'; R.unitF = '上海办公室'; R.ownerF = '刘希源'; await R.loadList();
  ok('类型/单位/负责人筛选同时传参', lastFetch() === '/api/repairs?stage=' + encodeURIComponent('待接收') + '&type=' + encodeURIComponent('单目') + '&unit=' + encodeURIComponent('上海办公室') + '&owner=' + encodeURIComponent('刘希源'));
  R.stageF = ''; R.typeF = ''; R.unitF = ''; R.ownerF = ''; R.setView('kanban'); await R.loadList();
  ok('看板视图不携带 stage 参数', lastFetch() === '/api/repairs');
  ok('下拉选项：单位/负责人带计数（上海 8（1 条无单位）/ 刘希源 14）', el('repUnitF').innerHTML.includes('上海办公室（8）') && el('repOwnerF').innerHTML.includes('刘希源（14）'));
  ok('下拉选项：类型 单目/双目/其他', el('repTypeF').innerHTML.includes('单目（9）') && el('repTypeF').innerHTML.includes('双目（9）') && el('repTypeF').innerHTML.includes('其他（0）'));
  // ---- 详情弹层 ----
  await R.openDetail('rec001');
  const mb = el('repModalBody').innerHTML;
  ok('详情：弹层打开 + 17 字段标签齐全', el('repModal').classList.contains('show') && ['设备编号', '设备所属类型', '所含配件', '所属单位', '当前阶段', '问题描述', '姓名', '联系方式', '收件地址', '期望寄回地址和联系方式', '快递单号', '发出快递单号', '维修负责人', '最后跟进时间', '最后跟进说明', '创建时间', '台账在线状态'].every(s => mb.includes(s)));
  ok('详情：附件图片缩略 + 非图片文件名外链', mb.includes('<img src="/repair-attachments/tok1/a.jpg"') && mb.includes('检测报告.pdf'));
  ok('详情：飞书原记录外链', mb.includes('class="rfeishu" href="https://feishu.example/base/rec001"'));
  ok('详情：缺失字段如实显示 —（姓名/联系方式/地址未填）', mb.includes('姓名') && mb.includes('>—<'));
  ok('详情：台账在线徽章（在线态）', mb.includes('台账在线'));
  mode = 'detail404'; await R.openDetail('rec002');
  ok('详情接口 404 → 列表缓存降级渲染并明示', el('repModalBody').innerHTML.includes('详情接口不可用') && el('repModalBody').innerHTML.includes('EP-000102'));
  mode = 'ok';
  // ---- 数据契约容错 ----
  ok('容错：候选键 device_no/description 解析', R.norm({ device_no: 'EP-9', description: '电源故障' }).device_id === 'EP-9' && R.norm({ device_no: 'EP-9', description: '电源故障' }).problem === '电源故障');
  ok('容错：后端定稿字段名（owner_name/contact_phone/tracking_no_in/out）全接住', R.norm({ owner_name: '刘希源', contact_phone: '138', tracking_no_in: 'SF1', tracking_no_out: 'SF2' }).owner === '刘希源' && R.norm({ contact_phone: '138' }).contact === '138' && R.norm({ tracking_no_in: 'SF1' }).tracking_no === 'SF1' && R.norm({ tracking_no_out: 'SF2' }).outbound_tracking_no === 'SF2');
  ok('容错：列表响应兼容裸数组与 {repairs:[…]}', R.normList([1, 2]).length === 2 && R.normList({ repairs: [1] }).length === 1 && R.normList({}).length === 0);
  ok('容错：device_online 布尔/字符串/缺失三态', R.norm({ device_online: 'true' }).online === true && R.norm({ device_online: 'false' }).online === false && R.norm({}).online === null);
  ok('容错：attachments 字符串数组与对象数组', R.norm({ attachments: ['/x.png'] }).attachments[0].url === '/x.png' && R.norm({ attachments: [{ filename: 'a.jpg', link: '/a.jpg' }] }).attachments[0].name === 'a.jpg');
  ok('容错：accessories 顿号拆分', R.norm({ accessories: '相机、电源' }).accessories.join(',') === '相机,电源');
  ok('容错：未知阶段归入附加列不丢失', (() => { const n = R.norm({ stage: '异常退回' }); return n.stage === '异常退回' && n.stageIdx === -1; })());
  // ---- 降级与数据诚实 ----
  mode = 'list404'; await R.loadList();
  ok('列表 404 → 红色横幅 + 看板空态明示不可用', !el('repBanner').hidden && el('repBanner').innerHTML.includes('维修数据加载失败') && el('repKanban').innerHTML.includes('维修数据不可用'));
  mode = 'stats404'; await R.loadStats();
  ok('统计 404 → 保留最后成功统计与同步时间', R.statsErr.includes('最后成功同步') && String(el('repActive').textContent) === '11');
  mode = 'syncerr'; R.listErr = ''; await R.loadStats();
  ok('后端同步失败（sync_error）→ 横幅明示', el('repBanner').innerHTML.includes('后端最近一次飞书同步失败') && el('repBanner').innerHTML.includes('lark-cli 认证过期'));
  mode = 'empty'; await R.loadList(); await R.loadStats();
  ok('空数据 → 引导文案 + 空阶段列头保留', el('repKanban').innerHTML.includes('暂无维修记录') && (el('repKanban').innerHTML.match(/class="rkcol"/g) || []).length === 7);
  mode = 'ok';
  // ---- 入口整合：路由别名 / 台账徽章联动 / 定时刷新可见性 ----
  HASH = ''; UpgT.setView('repairs');
  ok('setView("repairs") 别名 → hash 规范化为资产区子页签', HASH === '#view=assets&atab=repairs' && !el('viewAssets').hidden && A.tab === 'repairs');
  HASH = '#view=assets&atab=repairs&q=EP-000102'; A.applyHash();
  ok('深链 atab=repairs&q= → Rep 搜索词应用', A.tab === 'repairs' && R.q === 'EP-000102' && el('repQ').value === 'EP-000102');
  R.q = ''; el('repQ').value = '';
  A.go('repairs', { q: 'EP-000205' });
  ok('台账未完结徽章跳转 → 子页签 + 搜索联动 + hash 记录', A.tab === 'repairs' && R.q === 'EP-000205' && HASH.includes('atab=repairs') && HASH.includes('q=EP-000205') && el('repQ').value === 'EP-000205');
  R.enter();
  ok('无搜索词进入不清除既有筛选', R.q === 'EP-000205');
  R.q = ''; await R.loadList();
  // 台账在线/离线徽章点击 → 跳设备总览定位（重置状态筛选为全部）
  el('statusFilter').value = 'online';
  R.jumpDevice('EP-000101');
  ok('在线徽章跳设备总览：搜索词代入 + 状态筛选重置 + 视图切换', UpgT.view === 'overview' && !el('viewOverview').hidden && el('q').value === 'EP-000101' && el('statusFilter').value === 'all');
  const cnt0 = fetchLog.filter(p => p.startsWith('/api/repairs/stats')).length;
  A.setTab('hardware'); for (const fn of intervals) fn(); await sleep(80);
  const cnt1 = fetchLog.filter(p => p.startsWith('/api/repairs/stats')).length;
  A.setTab('repairs'); for (const fn of intervals) fn(); await sleep(80);
  const cnt2 = fetchLog.filter(p => p.startsWith('/api/repairs/stats')).length;
  ok('60s 自动刷新仅子页签可见时拉数（隐藏不拉/可见拉）', cnt1 === cnt0 && cnt2 === cnt0 + 1);
  console.log('\nfetch 序列（前 8）：', fetchLog.slice(0, 8).join(', '));
  if (!process.exitCode) console.log('\nALL PASS');
})();
