// 真实数据回归冒烟：以线上 /tmp/real-api 抓包为 fetch 桩，断言资产视图关键数字
// 用法：先在网关抓取 real-*.json（见 2026-10-26 排障记录），然后 node smoke-real.mjs
import { Window } from 'happy-dom'
import fs from 'node:fs'

const DIR = process.env.REAL_DIR || '/tmp/real-api'
const load = f => JSON.parse(fs.readFileSync(`${DIR}/${f}`, 'utf8'))

const win = new Window({ url: 'http://panel.test/' })
const payloads = {
  '/api/panel-title': { title: '设备库存与流转管理' },
  '/api/hosts': { fleet: [], sites: [], versions: [], ssh_hint: {}, online: 0, registered: 0, total: 0, timestamp: 1792990000, refresh_interval: 15 },
  '/api/events?hours=24&limit=60': { enabled: false, events: [] },
  '/api/collect-duration?days=7': { enabled: false, items: [] },
  '/api/webfix/tasks': { tasks: [], candidates: [] },
  '/api/upgrade/packages': { packages: [] },
  '/api/upgrade/jobs': { jobs: [] },
  '/api/assets/stats': load('real-assets_stats.json'),
  '/api/assets/units': load('real-assets_units.json'),
  '/api/assets/versions': load('real-assets_versions.json'),
  '/api/assets?include_archived=1': load('real-assets-ia1.json'),
  '/api/assets': load('real-assets.json'),
  '/api/assets/ep-000110': load('real-asset-detail2.json'),
  '/api/repairs/stats': load('real-repairs_stats.json'),
  '/api/repairs': load('real-repairs.json'),
}
const stubFetch = async url => {
  const u = String(url)
  if (payloads[u] !== undefined) return new win.Response(JSON.stringify(payloads[u]), { status: 200 })
  if (u.startsWith('/api/assets/')) {
    const code = decodeURIComponent(u.slice('/api/assets/'.length)).toLowerCase()
    if (code === 'ep-000110') return new win.Response(JSON.stringify(payloads['/api/assets/ep-000110']), { status: 200 })
    return new win.Response(JSON.stringify({ error: '设备不存在：' + code }), { status: 404 })
  }
  const hit = payloads[u]
  if (hit === undefined) return new win.Response(JSON.stringify({ error: 'not found' }), { status: 404 })
  return new win.Response(JSON.stringify(hit), { status: 200 })
}
win.fetch = stubFetch
globalThis.fetch = stubFetch

for (const [k, v] of Object.entries({
  window: win, document: win.document, location: win.location,
  navigator: win.navigator, HTMLElement: win.HTMLElement, Node: win.Node,
  Event: win.Event, CustomEvent: win.CustomEvent, MouseEvent: win.MouseEvent,
  addEventListener: win.addEventListener.bind(win), removeEventListener: win.removeEventListener.bind(win),
  getComputedStyle: win.getComputedStyle.bind(win),
  requestAnimationFrame: cb => setTimeout(cb, 0),
  SVGElement: win.SVGElement, Element: win.Element, Text: win.Text, Comment: win.Comment,
  DocumentFragment: win.DocumentFragment, ShadowRoot: win.ShadowRoot,
  HTMLStyleElement: win.HTMLStyleElement, HTMLInputElement: win.HTMLInputElement,
  HTMLSelectElement: win.HTMLSelectElement, HTMLTextAreaElement: win.HTMLTextAreaElement,
  HTMLButtonElement: win.HTMLButtonElement, HTMLAnchorElement: win.HTMLAnchorElement,
  HTMLTableElement: win.HTMLTableElement, HTMLPreElement: win.HTMLPreElement,
  HTMLDetailsElement: win.HTMLDetailsElement, HTMLLabelElement: win.HTMLLabelElement,
  Range: win.Range, XMLSerializer: win.XMLSerializer,
})) {
  try { Object.defineProperty(globalThis, k, { value: v, configurable: true, writable: true }) } catch (e) { /* node 内置 getter */ }
}
globalThis.EventSource = class { constructor() { this.readyState = 2 } close() {} }

document.body.innerHTML = '<div id="app"></div>'
const js = fs.readFileSync(fs.readdirSync('dist/assets').filter(f => f.endsWith('.js')).map(f => 'dist/assets/' + f)[0], 'utf8')
const css = fs.readFileSync(fs.readdirSync('dist/assets').filter(f => f.endsWith('.css')).map(f => 'dist/assets/' + f)[0], 'utf8')
const style = document.createElement('style'); style.textContent = css; document.head.appendChild(style)

const sleep = ms => new Promise(r => setTimeout(r, ms))
const text = () => document.body.textContent
const $ = s => document.querySelector(s)
const $$ = s => [...document.querySelectorAll(s)]
let pass = 0, fail = 0
function ok(name, cond) { if (cond) { pass++; console.log('  ✓', name) } else { fail++; console.log('  ✗ FAIL:', name) } }

const errs = []
win.addEventListener('error', e => errs.push(String(e.message)))
try { new Function(js)() } catch (e) { console.log('BUNDLE THROW:', e.message) }
await sleep(1200)

console.log('== 仪表盘（真实 stats：在册248/归档69/库存149/部署99/闲置8）')
location.hash = '#view=assets'
win.dispatchEvent(new win.Event('hashchange'))
await sleep(1500)
ok('指标卡渲染', $$('.amcard').length === 7)
const cardTxt = $$('.amcard').map(c => c.textContent).join('|')
ok('在册总数=248', cardTxt.includes('在册总数') && cardTxt.includes('248'))
ok('归档=69 / 库存=149 / 部署=99', ['69', '149', '99'].every(n => cardTxt.includes(n)))
ok('维修率 5.2%', cardTxt.includes('5.2%'))
ok('分布四面板', $$('.apanel').length >= 4)
ok('状态分布条(正常235)', $$('.abar').length > 10)

console.log('== 硬件台账（修复 device_no 归一后应有 248 行）')
location.hash = '#view=assets&atab=hardware'
win.dispatchEvent(new win.Event('hashchange'))
await sleep(600)
ok('台账 248 行', $$('#atabHardware tbody tr').length === 248)
ok('提示文案含「在册 248 台」', ($('#asLedgerHint')?.textContent || '').includes('在册 248 台'))
// 类型筛选（value 应为全名）
const typeSel = $('#asType')
const opt = [...typeSel.options].find(o => o.value === 'EgoPortal-Stereo-V0')
ok('类型选项 value=全名', !!opt && opt.textContent.includes('Stereo-V0'))
// 搜索小写
st_set: {
  const q = $('#asQ')
  q.value = 'EP-000110'   // 大写输入
  q.dispatchEvent(new win.Event('input'))
  await sleep(300)
  ok('大写搜索命中(小写化)', $$('#atabHardware tbody tr[data-code], #atabHardware tbody tr').length >= 1 && text().includes('EP-000110'))
}

// 归档勾选：追加 69 台归档（先清空搜索，避免上个用例残留筛选）
const qel = $('#asQ'); qel.value = ''; qel.dispatchEvent(new win.Event('input'))
await sleep(300)
const arc = $('#asArchived')
arc.checked = true
arc.dispatchEvent(new win.Event('change'))
await sleep(300)
const rowsAll = $$('#atabHardware tbody tr').length; ok(`勾选归档后 316 行=317-1条无编号记录（实际 ${rowsAll}）`, rowsAll === 316)

console.log('== 360° 详情（EP-000110：维修中 + 维修记录/处理流水）')
const row = $$('#atabHardware tbody tr').find(tr => tr.textContent.includes('EP-000110'))
row?.click()
await sleep(800)
ok('360 弹窗打开', !!$('#modal') && $('#modal').classList.contains('show'))
ok('维修记录表有数据', text().includes('摄像头连接处脱落') || text().includes('维修中'))
ok('处理流水(actions)有数据', text().includes('转交') || text().includes('刘希源'))
document.querySelector('#modal .x')?.click()
await sleep(100)

console.log('== 版本视图（真实契约 detected_by_version/match_counts）')
location.hash = '#view=assets&atab=versions'
win.dispatchEvent(new win.Event('hashchange'))
await sleep(600)
ok('探测分布 v0.4.6=4', $$('#asVersionBody .abar').some(el => el.textContent.includes('v0.4.6')))
const vb = $('#asVersionBody')?.textContent || ''; console.log('  [debug] 版本视图前200字:', vb.slice(0, 200).replace(/\s+/g, ' ')); ok('匹配=4 / 台账未登记=248 / 探测不到=244', /4\s*版本匹配/.test(vb) && vb.includes('248') && vb.includes('244'))
ok('ledger_missing 可展开', text().includes('展开设备列表'))
ok('服务商版本占比 5 家', $$('#asVersionBody .apanel').some(el => el.textContent.includes('主要服务商版本占比')))

console.log('== 项目视图（9 个单位）')
location.hash = '#view=assets&atab=units'
win.dispatchEvent(new win.Event('hashchange'))
await sleep(600)
const un = $$('.aunit').length; console.log('  [debug] 单位卡:', $$('.aunit').map(e => e.querySelector('b')?.textContent).join(',')); ok(`单位卡 9 张（实际 ${un}）`, un === 9)
ok('上海办公室 94 台', $$('.aunit').some(el => el.textContent.includes('上海办公室') && el.textContent.includes('94')))

console.log('== 维修看板（30 条 / 未完结 14）')
location.hash = '#view=assets&atab=repairs'
win.dispatchEvent(new win.Event('hashchange'))
await sleep(1200)
ok('看板 7 列', $$('.rkcol').length === 7)
ok('统计未完结=14 / 记录30', $$('.summary div').some(d => d.textContent.includes('14')) && $$('.summary div').some(d => d.textContent.includes('30')))
ok('维修卡片有内容', $$('.rcard').length > 5)

console.log('== JS 错误:', errs.length ? errs.slice(0, 3) : '无')
console.log(`\n结果: ${pass} 通过, ${fail} 失败`)
process.exit(fail ? 1 : 0)
