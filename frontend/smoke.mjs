// 无头冒烟：加载构建产物，验证 Vue 应用挂载与三视图渲染
import { Window } from 'happy-dom'
import fs from 'node:fs'

const win = new Window({ url: 'http://panel.test/' })
const origFetch = globalThis.fetch
// fetch 打桩：回放关键 API 的最小真实形状（来自线上契约）
const stub = {
  '/api/panel-title': { title: '设备库存与流转管理' },
  '/api/hosts': { fleet: [{ hostname: 'ep-000001', site: '上海办公室', online: true, version: 'v0.4.6', ssh_port: 20001, web_port: 30001, device_id: 'D1', hardware: 'V0', hw_model: 'N100', ip: '10.0.0.1', connections: 2, started: '2026-10-26 10:00' },
                    { hostname: 'ep-000002', site: '潍坊办公室', online: false, version: 'v0.4.5', last_seen: '2026-10-20' }],
                   sites: [{ site: '上海办公室', online: 1, expected: 1 }, { site: '潍坊办公室', online: 0, expected: 1 }],
                   versions: [{ version: 'v0.4.6', count: 1, online: 1 }, { version: 'v0.4.5', count: 1, online: 0 }],
                   ssh_hint: { jump: 'ubuntu@124.222.147.63', user: 'orangepi' }, online: 1, registered: 2, total: 2, timestamp: 1792986000, refresh_interval: 15 },
  '/api/events?hours=24&limit=60': { enabled: true, events: [{ event_type: 'online', hostname: 'ep-000001', ts: '2026-10-26 11:00' }] },
  '/api/collect-duration?days=7': { enabled: false, items: [] },
  '/api/webfix/tasks': { tasks: [], candidates: [] },
  '/api/upgrade/packages': { packages: [{ version: 'v0.4.6', arch: 'amd64', filename: 'ego-portal-v0.4.6-amd64.tar.gz', sha256: 'a'.repeat(64), size: 1e9 }] },
  '/api/upgrade/jobs': { jobs: [] },
  '/api/assets/stats': 404, '/api/assets/units': 404, '/api/assets/versions': 404,
  '/api/assets': 404, '/api/assets?archived=1': 404,
  '/api/repairs/stats': { total: 3, active_count: 2, by_stage: { '维修中': 1, '已完结': 1 } },
}
const stubFetch = async (url) => {
  const u = String(url).split(location.origin)[1] || String(url)
  const hit = stub[u]
  if (hit === 404) return new win.Response(JSON.stringify({ error: 'not found' }), { status: 404 })
  return new win.Response(JSON.stringify(hit ?? {}), { status: hit ? 200 : 404 })
}
win.fetch = stubFetch

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
  try { Object.defineProperty(globalThis, k, { value: v, configurable: true, writable: true }) }
  catch (e) { /* node 内置 getter（navigator 等）跳过 */ }
}
win.ResizeObserver = class { observe(){} unobserve(){} disconnect(){} }
globalThis.EventSource = class { constructor(){ this.readyState = 2 } close(){} }

document.body.innerHTML = '<div id="app"></div>'
const html = fs.readFileSync('dist/index.html', 'utf8')
const js = fs.readFileSync(fs.readdirSync('dist/assets').filter(f => f.endsWith('.js')).map(f => 'dist/assets/' + f)[0], 'utf8')
const css = fs.readFileSync(fs.readdirSync('dist/assets').filter(f => f.endsWith('.css')).map(f => 'dist/assets/' + f)[0], 'utf8')
// 注入 CSS 文本避免 fetch
const style = document.createElement('style'); style.textContent = css; document.head.appendChild(style)

const sleep = ms => new Promise(r => setTimeout(r, ms))
const errs = []
win.addEventListener('error', e => errs.push(String(e.message)))

try { new Function(js)() } catch (e) { console.log('BUNDLE THROW:', e.message) }
await sleep(1500)

const app = document.querySelector('#app')
const text = () => document.body.textContent
console.log('挂载:', !!app && app.children.length > 0)
console.log('总览渲染: 设备卡=', document.querySelectorAll('.card').length, ' 站点chip=', document.querySelectorAll('#sites .chip').length)
console.log('事件面板:', text().includes('最近事件'))

// 切到升级视图
location.hash = '#view=upgrade'
win.dispatchEvent(new win.Event('hashchange'))
await sleep(800)
console.log('升级视图: 参数区=', text().includes('升级参数'), ' 设备分组=', document.querySelectorAll('.vgroup').length, ' 全组升级btn=', text().includes('全组升级'))

// 切到资产视图（404 → mock 兜底）
location.hash = '#view=assets'
win.dispatchEvent(new win.Event('hashchange'))
await sleep(1500)
console.log('资产视图: 仪表盘=', document.querySelectorAll('.amcard').length > 0, ' mock横幅=', text().includes('mock'), ' 子页签=', document.querySelectorAll('#assetTabs button').length)

// 维修看板子页签
location.hash = '#view=assets&atab=repairs'
win.dispatchEvent(new win.Event('hashchange'))
await sleep(1200)
console.log('维修看板: 看板列=', document.querySelectorAll('.rkcol').length, ' 统计卡=', text().includes('未完结维修'))

console.log('JS错误:', errs.length ? errs.slice(0, 3) : '无')
globalThis.fetch = origFetch
process.exit(0)
