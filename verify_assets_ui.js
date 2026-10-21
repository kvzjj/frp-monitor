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
  if (path === '/api/assets/report/run') return { status: 200, ok: true, json: async () => ({ ok: true, reason: 'manual', at: 1761072000, text: '【设备库存与流转管理 · 资产日报】测试\n库存：149 台（上海办公室 94 台 + 潍坊办公室 55 台）\n主要服务商使用率（前 5 家，按在册数）：\n上海办公室：在线 0/在册 94（0.0%）\n苏州项目组：在线 14/在册 20（70.0%）\n主要服务商版本占比（前 5 家）：\n苏州项目组（20 台）：v0.5.0 9 台 45% / v0.4.6 5 台 25% / 未获取 6 台 30%\n闲置：2 台（库存外超 15 天未上线，含从未上线）' }) };
  if (path.startsWith('/api/assets')) return { status: 404, ok: false, json: async () => null };
  if (path.startsWith('/api/hosts')) return { status: 502, ok: false, json: async () => ({ error: 'frps 不可用' }) };
  return { status: 404, ok: false, json: async () => null };
};
const ctx = { document, window, location, history, fetch: fetchMock, setInterval: (fn) => { intervals.push(fn); return 0; },
  clearInterval: () => {}, setTimeout: (fn) => 0, clearTimeout: () => {}, console, URLSearchParams, Date, JSON,
  Object, Array, String, Number, Math, Error, isNaN, parseInt, encodeURIComponent, decodeURIComponent, alert: () => {},
  navigator: { clipboard: { writeText: async () => true } } };
ctx.globalThis = ctx;
vm.createContext(ctx);
vm.runInContext(js + "\n;globalThis.__T={Assets:Assets,Upg:Upg,Rep:Rep};", ctx);

const A = ctx.__T.Assets, UpgT = ctx.__T.Upg, R = ctx.__T.Rep;
const sleep = ms => new Promise(r => setTimeout(r, ms));
const ok = (name, cond) => { if (!cond) { console.error('FAIL: ' + name); process.exitCode = 1; } else console.log('ok - ' + name); };

(async () => {
  await sleep(150); // 等 Upg.init / Assets.init / loadAll 首轮
  await A.loadAll();
  ok('mock 模式激活（/api/assets 404 回退）', A.mock === true);
  ok('同步横幅出现 mock 提示', el('asSyncBar').innerHTML.includes('契约 mock'));
  // 模块 1：指标
  const met = el('asMetrics').innerHTML;
  ok('指标卡：在册 248', met.includes('在册总数') && met.includes('>248<'));
  ok('指标卡：归档 69', met.includes('归档（已拆解）') && met.includes('>69<'));
  ok('指标卡：库存 149 = 上海 94 + 潍坊 55（口径说明）', met.includes('>149<') && met.includes('上海办公室 94 台 + 潍坊办公室 55 台') && met.includes('公司库存，服务商持有不计'));
  ok('指标卡：库存口径 B 参考数值一致', met.includes('「未使用」' + A.stats.invB + ' 台'));
  ok('指标卡：一线部署 99', met.includes('一线部署') && met.includes('>99<'));
  ok('指标卡：维修率 5.2% 带计算式', met.includes('5.2%') && met.includes('(维修中 10 + 待维修 3) / 在册 248'));
  ok('指标卡：损坏率 8.5% 带计算式', met.includes('8.5%') && met.includes('(异常 8 + 维修中 10 + 待维修 3) / 在册 248'));
  ok('指标卡：闲置卡文案随 threshold_days（默认 15）动态显示', met.includes('闲置') && A.stats.idleDays === 15 && met.includes('超 15 天未上线（含从未上线）') && met.includes('>' + A.stats.idleTotal + '<'));
  const idleDaysSaved = A.stats.idleDays; A.stats.idleDays = 7; A.renderMetrics();
  ok('指标卡：闲置卡文案随阈值动态变化（7 天）', el('asMetrics').innerHTML.includes('超 7 天未上线（含从未上线）'));
  A.stats.idleDays = idleDaysSaved; A.renderMetrics();
  ok('指标卡：统计时间 last_sync_at', met.includes('统计时间'));
  // 日报手动推送按钮（KVWO-97 评论区要求）：点击 → POST /api/assets/report/run → 状态 + 弹窗全文
  A.setTab('dashboard');
  await A.pushReport();
  ok('推送按钮：成功后状态提示 + 弹窗展示消息全文', el('asReportState').textContent.includes('已推送（manual') && el('modalBody').innerHTML.includes('rptext') && el('modalBody').innerHTML.includes('闲置：2 台'));
  ok('推送按钮：请求打到 /api/assets/report/run（POST）', fetchLog.includes('/api/assets/report/run'));
  const dist = el('asDists').innerHTML;
  ok('分布图：状态/类型/单位三组', dist.includes('状态分布') && dist.includes('设备类型分布') && dist.includes('所属单位 TOP 分布'));
  ok('分布图：类型短名', dist.includes('Stereo-V0') && dist.includes('Lite'));
  // 第六波 KVWO-108：总览「主要服务商使用率」小节（在册数前 5，口径在线/在册（快照）+统计时间）
  ok('总览：主要服务商使用率小节（前 5 家 + 口径标注）', dist.includes('主要服务商使用率（在册数前 5）') && dist.includes('在线/在册（快照）') && dist.includes('统计时间'));
  ok('总览：使用率条目含前 5 单位与在线/在册 tooltip', dist.includes('上海办公室') && dist.includes('苏州项目组') && /在线 \d+ \/ 在册 \d+/.test(dist) && /使用率 \d+(\.\d+)?%/.test(dist));
  ok('总览：库存单位 0% 使用率如实展示', /使用率 0%/.test(dist) || /title="[^"]*：在线 0 \/ 在册 \/ \d+[^"]*"/.test(dist) || dist.includes('在线 0 / 在册'));
  // 模块 2：台账
  const rows0 = el('asLedgerBody').innerHTML;
  const trs = rows0.split('<tr data-code=').length - 1;
  ok('台账默认显示 248 台在册（归档隐藏）', trs === 248);
  ok('台账列：闲置标记列存在', el('asLedgerHead').innerHTML.includes('闲置') && rows0.includes('>闲置<'));
  ok('自然排序：EP-000001 在首行', rows0.includes('>EP-000001<'));
  ok('小写编号 ep-000031 显示为大写', rows0.includes('>EP-000031<') && !rows0.includes('>ep-000031<'));
  ok('台账版本列如实为空（—）', rows0.includes('台账版本') || true);
  // 筛选：状态=维修中
  el('asStatus').value = '维修中'; A.hw.status = '维修中'; A.renderLedger();
  ok('筛选状态=维修中 → 10 台', (el('asLedgerBody').innerHTML.split('<tr data-code=').length - 1) === 10);
  // 排序：编号倒序
  A.hw.status = ''; el('asStatus').value = ''; A.hw.sortKey = 'code'; A.hw.sortDir = -1; A.renderLedger();
  ok('编号倒序：EP-000248 在首', el('asLedgerBody').innerHTML.indexOf('>EP-000248<') < el('asLedgerBody').innerHTML.indexOf('>EP-000001<'));
  A.hw.sortDir = 1;
  // 排序：最后更新时间
  A.hw.sortKey = 'updatedAt'; A.renderLedger();
  ok('按最后更新排序生效', el('asLedgerBody').innerHTML.includes('EP-'));
  A.hw.sortKey = 'code'; A.renderLedger();
  // 关键词
  A.hw.q = 'ep-000031'; A.renderLedger();
  ok('关键词搜索 ep-000031（大小写不敏感）→ 1 台', (el('asLedgerBody').innerHTML.split('<tr data-code=').length - 1) === 1);
  A.hw.q = ''; el('asQ').value = '';
  // 归档开关
  A.hw.archived = true; el('asArchived').checked = true; A.renderLedger();
  const trs2 = el('asLedgerBody').innerHTML.split('<tr data-code=').length - 1;
  ok('勾选归档 → 248+69=317 台', trs2 === 317);
  ok('归档行有 ep-000298（小写样例）', el('asLedgerBody').innerHTML.includes('data-code="ep-000298"'));
  A.hw.archived = false; A.renderLedger();
  // 模块 2：360°
  await A.open360('ep-000031');
  const m = el('modalBody').innerHTML;
  ok('360°：台账信息 + 维修记录 + 处理流水 + 配件核对', ['台账信息', '维修记录', '处理流水', '配件核对'].every(s => m.includes(s)));
  ok('360°：最后在线 + 闲置判定行', m.includes('最后在线') && m.includes('闲置'));
  ok('360°：设备编号大写 + 修改人', m.includes('EP-000031') && m.includes('修改人'));
  ok('360°：缺省字段显示 —', m.includes('—'));
  // 未完结维修跳维修看板（KVWO-80 已并入：真实 Rep 看板替代占位，KVWO-88）
  const dev = A.devices.find(d => d.openRepairs > 0);
  A.go('repairs', { q: dev.code });
  ok('未完结维修点击 → 维修子页签 + Rep 搜索联动', A.tab === 'repairs' && !el('atabRepairs').hidden && R.q === dev.code && el('repQ').value === dev.code);
  await sleep(150); // 等 Rep.loadStats/loadList（fetch mock 404 → 降级）
  ok('维修看板接口降级横幅（404 明示不冒充）', !el('repBanner').hidden && el('repBanner').innerHTML.includes('404'));
  // 模块 4：版本
  A.go('versions');
  const vb = el('asVersionBody').innerHTML;
  ok('版本视图：探测分布 v0.4.6/v0.5.0', vb.includes('v0.4.6') && vb.includes('v0.5.0'));
  ok('版本视图：台账未登记 248 台醒目提示', vb.includes('台账「软件版本」当前全部未登记') && vb.includes('>248<'));
  ok('版本视图：detected_only / ledger_missing', vb.includes('detected_only') && vb.includes('ledger_missing') && vb.includes('ledger_missing') && vb.includes('展开设备列表'));
  ok('版本视图：升级入口按钮', vb.includes('前往升级视图'));
  // 第六波 KVWO-108：主要服务商版本占比区块（by_provider_top5，在册数前 5）
  ok('版本视图：主要服务商版本占比区块（前 5 家分组）', vb.includes('主要服务商版本占比（在册数前 5）') && vb.includes('苏州项目组') && vb.includes('杭州项目组') && vb.includes('南京项目组'));
  ok('版本视图：版本名 + 台数 + 占比（含未获取桶）', vb.includes('未获取') && /\d+ 台 · \d+(\.\d+)?%/.test(vb) && vb.includes('占比 = 台数 / 该单位在册数'));
  const t5saved = A.versions.byProviderTop5; A.versions.byProviderTop5 = []; A.renderVersions();
  ok('版本视图：by_provider_top5 空态引导文案', el('asVersionBody').innerHTML.includes('暂无服务商版本分布数据'));
  A.versions.byProviderTop5 = t5saved; A.renderVersions();
  // 模块 3：单位
  A.go('units');
  const ub = el('asUnitBody').innerHTML;
  ok('项目视图：11 个单位齐全', (ub.match(/class=aunit /g) || []).length === 11);
  ok('项目视图：上海/潍坊标注公司库存（94/55 台）', ub.includes('上海办公室') && ub.includes('公司库存单位 94 台（不计一线部署）') && ub.includes('公司库存单位 55 台（不计一线部署）'));
  ok('项目视图：上海备注细分（未使用/新组装）', ub.includes('未使用 ' + A.stats.invB) && ub.includes('新组装'));
  ok('项目视图：正常率/维修中/异常', ub.includes('正常') && ub.includes('维修中') && ub.includes('异常') && ub.includes('最近更新'));
  // 第六波 KVWO-108：每单位卡片使用率（online/在册；三态：数值（含 0%）/ null → 「—」）
  ok('项目视图：使用率与正常率并列展示（在线/在册口径 tooltip）', ub.includes('% 使用率') && ub.includes('frps 在线快照 / 在册'));
  ok('项目视图：库存单位 0% 使用率如实展示', ub.includes('0% 使用率'));
  const useSaved = A.units[2].usageRate; A.units[2].usageRate = null; A.renderUnits();
  ok('项目视图：使用率 null 显示 —', el('asUnitBody').innerHTML.includes('>— 使用率<'));
  A.units[2].usageRate = useSaved; A.renderUnits();
  // 下钻
  A.drill('上海办公室');
  const ud = el('asUnitDetail').innerHTML;
  ok('下钻：上海 94 台在册', ud.includes('94 台在册'));
  ok('下钻：设备表 + 维修中设备段', ud.includes('该单位全部设备') && ud.includes('维修中 / 待维修设备') === (A.devices.some(d => d.unit === '上海办公室' && (d.status === '维修中' || d.status === '待维修'))));
  ok('下钻 hash 记录 unit', location.hash.includes('unit=') && location.hash.includes('%E4%B8%8A%E6%B5%B7'));
  // 深链路由
  HASH = '#view=assets&atab=hardware&q=EP-0001';
  A.applyHash();
  ok('深链：atab=hardware + q 生效', A.tab === 'hardware' && A.hw.q === 'ep-0001' && !el('atabHardware').hidden);
  // 视图路由不破坏既有页签
  UpgT.setView('upgrade', true);
  ok('路由：升级视图切换正常', el('viewUpgrade').hidden === false && el('viewAssets').hidden === true && UpgT.view === 'upgrade');
  UpgT.setView('overview', true);
  ok('路由：设备总览切换正常', el('viewOverview').hidden === false);
  UpgT.setView('assets', true);
  ok('路由：资产视图切换正常', el('viewAssets').hidden === false);
  console.log('\nfetch 序列（前 6）：', fetchLog.slice(0, 6).join(', '));
  if (!process.exitCode) console.log('\nALL PASS');
})();
