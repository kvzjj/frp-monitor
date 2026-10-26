<script setup>
// 资产管理视图：总览仪表盘 / 硬件台账（360°）/ 维修看板（RepairsBoard）/ 版本视图 / 项目视图
// 数据契约同后端 KVWO-83；接口 404 时按契约 mock（横幅明示）。逻辑自原 Assets.* 忠实平移。
import { ref, reactive, computed, onMounted, onUnmounted, onActivated, onDeactivated, watch } from 'vue'
import { api } from '../api'
import { fmtClockOf, parseTs, natCmp } from '../format'
import RepairsBoard from '../components/RepairsBoard.vue'

const STATUS_ORDER = ['正常', '异常', '维修中', '待维修', '待标定', '测试中', '已拆解']
const ST_COLOR = { '正常': '#16a34a', '异常': '#dc2626', '维修中': '#ea580c', '待维修': '#d97706', '待标定': '#2563eb', '测试中': '#0891b2', '已拆解': '#64748b' }
const TYPE_COLOR = { 'EgoPortal-Stereo-V0': '#2563eb', 'EgoPortal-Stereo-V1': '#7c3aed', 'EgoPortal-Lite': '#0891b2' }

const st = reactive({
  mock: false, err: '', lastGoodAt: 0, loaded: false,
  tab: 'dashboard', drillUnit: '',
  stats: null, units: null, versions: null, devices: [], archived: [],
  hw: { q: '', status: '', type: '', unit: '', archived: false, sortKey: 'code', sortDir: 1 },
  reportState: ''
})
const modal360 = ref(null)   // 设备 360° 详情
const detail360 = ref(null)
let refreshTimer = null

const repBoard = ref(null)

/* ---------- 路由（#view=assets&atab=&q=&unit=） ---------- */
function applyHash() {
  const p = new URLSearchParams(location.hash.slice(1))
  const vh = p.get('view')
  if (vh !== 'assets' && vh !== 'repairs') return
  const t = p.get('atab') || (vh === 'repairs' ? 'repairs' : 'dashboard')
  setTab(['dashboard', 'hardware', 'repairs', 'versions', 'units'].includes(t) ? t : 'dashboard')
  if (p.get('q') != null) st.hw.q = String(p.get('q')).toLowerCase()
  const u = p.get('unit')
  if (u && st.units) drill(u)
}

function writeHash(extra) {
  const p = new URLSearchParams(location.hash.slice(1))
  p.set('view', 'assets')
  for (const k of ['atab', 'q', 'unit']) p.delete(k)
  if (extra) for (const [k, v] of Object.entries(extra)) p.set(k, v)
  history.replaceState(null, '', location.pathname + location.search + '#' + p.toString())
}

function setTab(t) {
  st.tab = t
  if (t !== 'units') st.drillUnit = ''
}

function go(tab, extra) {
  setTab(tab)
  writeHash(Object.assign({ atab: tab }, extra || {}))
}

/* ---------- 数据层 ---------- */
function nByKey(o, ks) { for (const k of ks) if (o && o[k] != null) return o[k]; return null }

async function apiAssets(path) {
  const r = await api(path)
  if (r.status === 404) { st.mock = true; return mockFor(path) }
  const d = await r.json().catch(() => null)
  if (!r.ok || d == null) throw new Error((d && d.error) || ('HTTP ' + r.status))
  st.mock = false
  return d
}

function normDev(r, archived) {
  const on = r ? (r.device_online !== undefined ? r.device_online : r.online) : null
  return {
    // 真实契约（KVWO-107 后）：device_no / note / modified_by / active_repairs；
    // 候选键保留旧契约与 mock 形态兼容
    code: String(nByKey(r, ['device_no', 'device_code', 'device_id', 'code']) || '').trim(),
    type: nByKey(r, ['device_type', 'type']), status: nByKey(r, ['status']),
    unit: nByKey(r, ['unit', 'organization']), parts: nByKey(r, ['accessories', 'parts']),
    remark: nByKey(r, ['note', 'remark']), ledgerVer: nByKey(r, ['software_version', 'ledger_version']),
    detVer: nByKey(r, ['detected_version']),
    online: on === true ? true : (on === false ? false : null),
    openRepairs: Number(nByKey(r, ['active_repairs', 'open_repairs', 'repair_count']) || 0),
    updatedAt: nByKey(r, ['updated_at', 'last_updated']), modifier: nByKey(r, ['modified_by', 'modifier']),
    lastSeen: nByKey(r, ['last_seen', 'lastSeen']), idle: nByKey(r, ['idle']),
    archived: !!(archived || (r && r.archived))
  }
}

function normStats(s) {
  return {
    total: nByKey(s, ['registered_total', 'total', 'registered']), archived: nByKey(s, ['archived_total', 'archived']),
    invTotal: nByKey(s && s.inventory, ['total']), invB: nByKey(s && s.inventory, ['unused_note']),
    invUnits: (s && s.inventory && Array.isArray(s.inventory.units)) ? s.inventory.units : [],
    invByUnit: Object.assign({}, (s && s.inventory && s.inventory.by_unit) || {}),
    idleTotal: nByKey(s && s.idle, ['total']), idleDays: nByKey(s && s.idle, ['threshold_days']),
    idleByUnit: Object.assign({}, (s && s.idle && s.idle.by_unit) || {}),
    deployed: nByKey(s, ['deployed']),
    byStatus: Object.assign({}, s && (s.by_status || s.status) || {}), byType: Object.assign({}, s && (s.by_type || s.type) || {}),
    byUnit: Object.assign({}, s && (s.by_unit || s.unit) || {}),
    repairRate: nByKey(s, ['repair_rate']), damageRate: nByKey(s, ['damage_rate']),
    lastSync: nByKey(s, ['last_sync_at', 'last_sync', 'synced_at'])
  }
}

function normUnits(u) {
  const arr = (u && (u.units || u.items)) || (Array.isArray(u) ? u : [])
  return arr.map(x => ({
    unit: String(nByKey(x, ['unit', 'name']) || ''), total: nByKey(x, ['total', 'count']),
    normalRate: nByKey(x, ['normal_rate']), repairing: nByKey(x, ['repairing']), abnormal: nByKey(x, ['abnormal']),
    online: nByKey(x, ['online', 'online_count']), usageRate: nByKey(x, ['usage_rate']),
    lastUpdated: nByKey(x, ['last_updated', 'updated_at'])
  }))
}

function normVers(v) {
  const arr = a => Array.isArray(a) ? a : []
  // 真实契约（KVWO-107 后）：detected_by_version/ledger_by_version 字典、
  // ledger_no_version、match_counts.{matched,…}、match.{detected_only,ledger_missing}；
  // 旧契约（detected/ledger 数组、顶层 matched 等）保留兼容
  const dictBars = d => (d && typeof d === 'object' && !Array.isArray(d))
    ? Object.entries(d).map(([version, count]) => ({ version, count })) : []
  const detected = arr(v && v.detected).length ? arr(v && v.detected) : dictBars(v && v.detected_by_version)
  const ledger = arr(v && v.ledger).length ? arr(v && v.ledger) : dictBars(v && v.ledger_by_version)
  const mc = (v && v.match_counts) || {}
  const mm = (v && v.match) || {}
  return {
    detected, ledger,
    ledgerEmpty: nByKey(v, ['ledger_no_version', 'ledger_empty']),
    matched: mc.matched != null ? mc.matched : nByKey(v, ['matched']),
    detectedOnly: Array.isArray(mm.detected_only) ? mm.detected_only : arr(v && v.detected_only),
    ledgerMissing: Array.isArray(mm.ledger_missing) ? mm.ledger_missing : arr(v && v.ledger_missing),
    byProviderTop5: arr(v && v.by_provider_top5).map(p => ({
      unit: String(nByKey(p, ['unit', 'name']) || ''), total: nByKey(p, ['total', 'count']),
      versions: arr(p && p.versions).map(x => ({ version: nByKey(x, ['version']), count: nByKey(x, ['count']), share: nByKey(x, ['share']) }))
    }))
  }
}

async function loadAll() {
  try {
    // 归档：后端参数为 include_archived=1（原前端误发 archived=1，导致归档列表与在册重复）；
    // 单次拉全量后按 archived 标志拆分（旧后端忽略该参数时退化为无归档，可接受）
    const res = await Promise.all([
      apiAssets('/api/assets/stats'), apiAssets('/api/assets/units'), apiAssets('/api/assets/versions'),
      apiAssets('/api/assets?include_archived=1')
    ])
    st.stats = normStats(res[0]); st.units = normUnits(res[1]); st.versions = normVers(res[2])
    const all = (res[3] && (res[3].items || res[3].devices) || [])
    st.devices = all.filter(r => !r.archived).map(r => normDev(r, false)).filter(x => x.code)
    st.archived = all.filter(r => !!r.archived).map(r => normDev(r, true)).filter(x => x.code)
    st.err = ''; st.loaded = true; st.lastGoodAt = Date.now()
  } catch (e) { st.err = String(e.message || e) }
}

function idleDays() { const d = st.stats && st.stats.idleDays; return d != null ? d : 15 }

const syncBar = computed(() => {
  const when = st.stats && st.stats.lastSync ? ('飞书同步于 ' + (fmtClockOf(st.stats.lastSync) || '')) : ''
  const good = st.lastGoodAt ? ('拉取于 ' + new Date(st.lastGoodAt).toLocaleString('zh-CN', { hour12: false })) : ''
  if (st.mock) return { cls: 'asyncbar warn', html: '⚠ /api/assets 接口未就绪（后端 KVWO-83 未合并）：当前展示<b>契约 mock 演示数据</b>，仅供开发预览，不代表真实资产；后端合入后自动切换真实数据，前端无需改动' }
  if (st.err && !st.loaded) return { cls: 'asyncbar err', html: '资产数据加载失败：' + st.err + '（每 60 秒自动重试）' }
  if (st.err) return { cls: 'asyncbar err', html: '最近一次刷新失败：' + st.err + '，展示最后成功数据' + (good ? '（' + good + '）' : '') + (when ? ' · ' + when : '') }
  return { cls: 'asyncbar ok', html: '资产数据' + (when || '') + (good ? ' · ' + good : '') + ' · 来源：飞书多维表（只读同步）' }
})

/* ---------- 仪表盘 ---------- */
const metricCards = computed(() => {
  const s = st.stats
  if (!s) return []
  const n = k => Number((s.byStatus || {})[k] || 0)
  const pctv = v => v == null ? '—' : (Math.round(v * 1000) / 10) + '%'
  const stime = '统计时间：' + (fmtClockOf(s.lastSync) || '—')
  return [
    { v: s.total, l: '在册总数', sub: '口径：设备明细表（不含归档）' },
    { v: s.archived, l: '归档（已拆解）', sub: '口径：设备归档数据表，不参与在册指标' },
    { v: s.invTotal, l: '库存', sub: '口径：' + invLabel() + (s.invB != null ? '；参考：备注含「未使用」' + s.invB + ' 台' : ''), c: '#0f766e' },
    { v: s.deployed, l: '一线部署', sub: '口径：在册 − 库存（库存单位持有不计部署）' },
    { v: pctv(s.repairRate), l: '维修率', rate: 1, c: '#ea580c', sub: '计算式：(维修中 ' + n('维修中') + ' + 待维修 ' + n('待维修') + ') / 在册 ' + s.total },
    { v: pctv(s.damageRate), l: '损坏率', rate: 1, c: '#dc2626', sub: '计算式：(异常 ' + n('异常') + ' + 维修中 ' + n('维修中') + ' + 待维修 ' + n('待维修') + ') / 在册 ' + s.total },
    { v: s.idleTotal, l: '闲置', sub: '口径：库存外·超 ' + idleDays() + ' 天未上线（含从未上线）；fleet 库未启用时不可计算', c: '#7c3aed' }
  ].map(m => ({ ...m, stime }))
})

function invLabel() {
  const s = st.stats || {}, bu = s.invByUnit || {}
  const parts = (s.invUnits && s.invUnits.length ? s.invUnits : ['上海办公室', '潍坊办公室']).map(u => u + ' ' + (bu[u] != null ? bu[u] : 0) + ' 台')
  return (parts.length ? parts.join(' + ') : '—') + '（公司库存，服务商持有不计）'
}

function barEntries(obj, order) {
  let keys = Object.keys(obj || {})
  if (order) keys = order.filter(k => obj[k] != null).concat(keys.filter(k => order.indexOf(k) < 0))
  return keys.map(k => [k, obj[k]])
}

function distPanels() {
  const s = st.stats
  if (!s) return []
  const uTop = (st.units || []).slice().sort((a, b) => (b.total || 0) - (a.total || 0) || String(a.unit).localeCompare(String(b.unit), 'zh')).slice(0, 5)
  return [
    { title: '状态分布（在册 ' + (s.total != null ? s.total : '—') + ' 台）', bars: barEntries(s.byStatus, STATUS_ORDER).map(e => ({ k: e[0], n: e[1], c: ST_COLOR[e[0]] || '#64748b' })) },
    { title: '设备类型分布', bars: barEntries(s.byType).sort((a, b) => b.n - a.n).map(e => ({ k: shortType(e.k), n: e.n, c: TYPE_COLOR[e.k] || '#2563eb' })) },
    { title: '所属单位 TOP 分布', bars: barEntries(s.byUnit).sort((a, b) => b.n - a.n).map(e => ({ k: e.k, n: e.n, c: '#2563eb' })) },
    { title: '主要服务商使用率（在册数前 5）', hint: '口径：在线/在册（快照） · 统计时间：' + (fmtClockOf(s.lastSync) || '—'), units: uTop }
  ]
}

function shortType(t) { return String(t || '').replace('EgoPortal-', '') }

async function pushReport() {
  if (st.reportBusy) return
  st.reportBusy = true
  st.reportState = '推送中…'
  try {
    const r = await api('/api/assets/report/run', { method: 'POST' })
    const d = await r.json().catch(() => null)
    if (!r.ok || !d || !d.ok) { st.reportState = '推送失败：' + ((d && d.error) || ('HTTP ' + r.status)); return }
    st.reportState = '已推送（' + (d.reason || 'manual') + ' · ' + (fmtClockOf((d.at || 0) * 1000) || '') + '），消息全文见弹窗'
    st.reportText = d.text || ''
  } catch (e) { st.reportState = '推送失败：' + String(e.message || e) }
  finally { st.reportBusy = false }
}

/* ---------- 硬件台账 ---------- */
const filterOptions = computed(() => {
  const s = st.stats
  if (!s) return { status: [], type: [], unit: [] }
  const um = {}
  for (const u of st.units || []) um[u.unit] = u.total
  return {
    status: barEntries(s.byStatus, STATUS_ORDER),
    type: barEntries(s.byType),
    unit: Object.entries(um)
  }
})

function sortDevs(list) {
  const hw = st.hw
  return list.slice().sort((x, y) => {
    if (hw.sortKey === 'code') return natCmp(x.code, y.code) * hw.sortDir
    return (parseTs(x.updatedAt) - parseTs(y.updatedAt)) * hw.sortDir || natCmp(x.code, y.code)
  })
}

const ledgerRows = computed(() => {
  const hw = st.hw, q = hw.q.trim().toLowerCase()
  const match = d => {
    if (hw.status && d.status !== hw.status) return false
    if (hw.type && d.type !== hw.type) return false
    if (hw.unit && d.unit !== hw.unit) return false
    if (q) { const hay = (d.code + ' ' + (d.remark || '') + ' ' + (d.unit || '')).toLowerCase(); if (!hay.includes(q)) return false }
    return true
  }
  let list = sortDevs(st.devices.filter(match))
  if (hw.archived) list = list.concat(sortDevs(st.archived.filter(match)))
  return list
})

const ledgerHint = computed(() => '在册 ' + st.devices.length + ' 台 · 当前显示 ' + ledgerRows.value.filter(d => !d.archived).length + ' 台' +
  (st.hw.archived ? ' · 归档 ' + ledgerRows.value.filter(d => d.archived).length + ' 台（已追加在下方，淡色显示）' : ' · 归档 ' + st.archived.length + ' 台默认隐藏（勾选「显示归档设备」展开）'))

const LEDGER_COLS = [['code', '设备编号'], ['type', '类型'], ['status', '状态'], ['unit', '所属单位'], ['parts', '配件情况'], ['remark', '去向 / 备注'], ['ledgerVer', '台账版本'], ['detVer', '探测版本'], ['online', '在线'], ['idle', '闲置'], ['openRepairs', '维修'], ['updatedAt', '最后更新']]

function sortCol(k) {
  if (st.hw.sortKey === k) st.hw.sortDir *= -1
  else { st.hw.sortKey = k; st.hw.sortDir = 1 }
}

async function open360(code) {
  modal360.value = code
  detail360.value = null
  try {
    const d = await apiAssets('/api/assets/' + encodeURIComponent(code))
    detail360.value = d
  } catch (e) { detail360.value = { error: String(e.message || e) } }
}

function norm360() {
  const d = detail360.value
  if (!d || d.error) return null
  const led = (d && (d.ledger || d.device)) || d || {}
  return { dev: normDev(led, !!(d && d.archived)), raw: d }
}

/* ---------- 项目视图 ---------- */
function shanghaiInv() {
  if (!st.stats) return null
  const sh = st.devices.filter(d => d.unit === '上海办公室')
  const unused = st.stats.invB != null ? st.stats.invB : sh.filter(d => String(d.remark || '').includes('未使用')).length
  return { unused, newBuild: sh.filter(d => String(d.remark || '').includes('新组装')).length }
}

function drill(unit) {
  st.drillUnit = unit
  setTab('units')
  writeHash({ atab: 'units', unit })
}

function drillData(unit) {
  const devs = sortDevs(st.devices.filter(d => d.unit === unit))
  const rep = st.devices.filter(d => d.unit === unit && (d.status === '维修中' || d.status === '待维修'))
  return { devs, rep }
}

/* ---------- 版本视图 ---------- */
const versData = computed(() => {
  const v = st.versions
  if (!v) return null
  const det = (v.detected || []).slice().sort((a, b) => (b.count || 0) - (a.count || 0))
  const led = (v.ledger || []).slice().sort((a, b) => (b.count || 0) - (a.count || 0))
  const dmax = Math.max(...det.map(x => x.count || 0), 1)
  const lmax = Math.max(...led.map(x => x.count || 0), 1)
  const codeOf = x => typeof x === 'string' ? x : String((x && (x.device_code || x.code || x.hostname)) || '')
  return {
    det, led, dmax, lmax,
    matched: v.matched, ledgerEmpty: v.ledgerEmpty,
    only: v.detectedOnly || [], miss: v.ledgerMissing || [],
    t5: v.byProviderTop5 || []
  }
})

/* ---------- 契约 mock（仅 404 时启用；确定性种子，数字对齐 KVWO-97 基线） ---------- */
let _mockCache = null
function mockFor(path) {
  const M = mockData()
  if (path.indexOf('/api/assets/stats') === 0) return M.stats
  if (path.indexOf('/api/assets/units') === 0) return { units: M.units }
  if (path.indexOf('/api/assets/versions') === 0) return M.versions
  if (path.indexOf('/api/assets?') === 0) return { devices: M.devices.concat(M.archived), last_sync_at: M.lastSync }
  if (path.indexOf('/api/assets/') === 0) {
    const code = decodeURIComponent(path.slice('/api/assets/'.length)).toLowerCase()
    const dev = M.devices.concat(M.archived).filter(x => x.device_code.toLowerCase() === code)[0]
    if (dev) return mockDetail(dev)
    throw new Error('设备不存在：' + code)
  }
  return { devices: M.devices, last_sync_at: M.lastSync }
}
function mockData() {
  if (_mockCache) return _mockCache
  let seed = 20261020
  const rnd = () => { seed |= 0; seed = seed + 0x6D2B79F5 | 0; let t = Math.imul(seed ^ seed >>> 15, 1 | seed); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296 }
  const pickR = a => a[Math.floor(rnd() * a.length)]
  const UNITS = [['上海办公室', 94], ['潍坊办公室', 55], ['苏州项目组', 20], ['杭州项目组', 18], ['南京项目组', 15], ['成都项目组', 12], ['武汉项目组', 10], ['西安项目组', 8], ['北京项目组', 6], ['广州项目组', 6], ['长沙项目组', 4]]
  const pool = pairs => { const a = []; for (const [s, n] of pairs) for (let i = 0; i < n; i++) a.push(s); for (let i = a.length - 1; i > 0; i--) { const j = Math.floor(rnd() * (i + 1)); const t = a[i]; a[i] = a[j]; a[j] = t } return a }
  const stPool = pool([['正常', 217], ['异常', 8], ['维修中', 10], ['待维修', 3], ['待标定', 6], ['测试中', 4]])
  const tyPool = pool([['EgoPortal-Stereo-V0', 160], ['EgoPortal-Stereo-V1', 55], ['EgoPortal-Lite', 33]])
  const PARTS = ['头盔', '相机', '主机', '数据线', '腰带', '充电宝']
  const base = Date.UTC(2026, 9, 20, 0, 0, 0)
  const devs = []; let k = 0
  for (const [unit, count] of UNITS) for (let i = 0; i < count; i++) {
    const status = stPool[k], type = tyPool[k]; k++
    let code = 'EP-' + String(k).padStart(6, '0'); if (k === 31) code = 'ep-000031'
    const isInv = unit === '上海办公室' || unit === '潍坊办公室', r = rnd()
    const remark = isInv ? (r < 0.38 ? '库存·未使用' : r < 0.5 ? '新组装备用' : r < 0.6 ? '新组装·待发放' : (unit === '上海办公室' ? '上海备机' : '潍坊备机')) : (pickR(['一线部署', '现场备用', '项目交付', '演示用机', '']))
    const online = isInv ? (r < 0.1 ? null : false) : (rnd() < 0.72 ? true : false)
    const detVer = online ? pickR(['v0.4.6', 'v0.4.6', 'v0.5.0', 'v0.4.5']) : null
    const openRep = status === '维修中' ? (rnd() < 0.3 ? 2 : 1) : status === '待维修' ? 1 : (status === '异常' && rnd() < 0.4 ? 1 : 0)
    devs.push({ device_code: code, device_type: type, status, unit, accessories: pickR(PARTS) + (rnd() < 0.5 ? '、' + pickR(PARTS) : ''), remark, ledger_version: null, detected_version: detVer, device_online: online, open_repairs: openRep, updated_at: new Date(base - k * 37 * 60000).toISOString(), modifier: pickR(['张伟', '李娜', '王强', '刘敏', '陈晨']) })
  }
  const arch = []
  for (let i = 0; i < 69; i++) arch.push({ device_code: i === 49 ? 'ep-000298' : 'EP-' + String(249 + i).padStart(6, '0'), device_type: pickR(['EgoPortal-Stereo-V0', 'EgoPortal-Stereo-V1', 'EgoPortal-Lite']), status: '已拆解', unit: pickR(UNITS.map(u => u[0])), accessories: '', remark: '已拆解归档', ledger_version: null, detected_version: null, device_online: false, open_repairs: 0, updated_at: new Date(base - 400 * 3600000 - i * 3600000).toISOString(), modifier: '李娜', archived: true })
  const byStatus = {}, byType = {}, byUnit = {}
  for (const s of STATUS_ORDER) byStatus[s] = devs.filter(d => d.status === s).length
  for (const d of devs) { byType[d.device_type] = (byType[d.device_type] || 0) + 1; byUnit[d.unit] = (byUnit[d.unit] || 0) + 1 }
  const idleByUnit = {}; let idleTotal = 0
  for (const d of devs) {
    const isInv = d.unit === '上海办公室' || d.unit === '潍坊办公室'
    const idle = !isInv && d.device_online === false && rnd() < 0.35
    d.idle = idle
    d.last_seen = d.device_online === true ? String(d.updated_at).slice(0, 16).replace('T', ' ') : (idle ? null : new Date(Date.parse(d.updated_at) - 5 * 86400000).toISOString().slice(0, 16).replace('T', ' '))
    if (idle) { idleTotal++; idleByUnit[d.unit] = (idleByUnit[d.unit] || 0) + 1 }
  }
  const invA = (byUnit['上海办公室'] || 0) + (byUnit['潍坊办公室'] || 0)
  const invB = devs.filter(d => (d.unit === '上海办公室' || d.unit === '潍坊办公室') && String(d.remark || '').includes('未使用')).length
  const det = {}; for (const d of devs) if (d.detected_version) det[d.detected_version] = (det[d.detected_version] || 0) + 1
  const units = UNITS.map(u => {
    const us = devs.filter(d => d.unit === u[0])
    const mx = us.reduce((m, d) => Math.max(m, Date.parse(d.updated_at) || 0), 0)
    const onc = us.filter(d => d.device_online === true).length
    return { unit: u[0], total: us.length, online: onc, usage_rate: us.length ? onc / us.length : null, normal_rate: us.length ? us.filter(d => d.status === '正常').length / us.length : null, repairing: us.filter(d => d.status === '维修中').length, abnormal: us.filter(d => d.status === '异常').length, last_updated: mx ? new Date(mx).toISOString() : null }
  })
  const provTop5 = UNITS.slice().sort((a, b) => b[1] - a[1] || String(a[0]).localeCompare(String(b[0]), 'zh')).slice(0, 5).map(u => {
    const us = devs.filter(d => d.unit === u[0]), cnt = {}
    for (const d of us) { const kk = d.detected_version || '未获取'; cnt[kk] = (cnt[kk] || 0) + 1 }
    return { unit: u[0], total: us.length, versions: Object.entries(cnt).sort((x, y) => y[1] - x[1]).map(e => ({ version: e[0], count: e[1], share: Math.round(e[1] / us.length * 100) / 100 })) }
  })
  const lastSync = new Date(base + 5 * 60000).toISOString()
  _mockCache = {
    devices: devs, archived: arch, lastSync,
    stats: { registered_total: devs.length, archived_total: arch.length, inventory: { total: invA, by_unit: { '上海办公室': byUnit['上海办公室'] || 0, '潍坊办公室': byUnit['潍坊办公室'] || 0 }, units: ['上海办公室', '潍坊办公室'], unused_note: invB }, deployed: devs.length - invA, idle: { total: idleTotal, threshold_days: 15, by_unit: idleByUnit }, by_status: byStatus, by_type: byType, by_unit: byUnit, repair_rate: (byStatus['维修中'] + byStatus['待维修']) / devs.length, damage_rate: (byStatus['异常'] + byStatus['维修中'] + byStatus['待维修']) / devs.length, last_sync_at: lastSync },
    units,
    versions: { detected: Object.entries(det).map(e => ({ version: e[0], count: e[1] })), ledger: [], ledger_empty: devs.length, matched: devs.filter(d => d.detected_version).length, detected_only: [{ device_code: 'ep-900201', version: 'v0.5.0' }, { device_code: 'EP-900202', version: 'v0.4.6' }, { device_code: 'EP-900203', version: 'v0.5.0' }], ledger_missing: devs.filter(d => !d.detected_version).map(d => d.device_code), by_provider_top5: provTop5 }
  }
  return _mockCache
}
function mockDetail(dev) {
  let s = String(dev.device_code).length * 7919 + 42
  const rnd = () => { s |= 0; s = s + 0x6D2B79F5 | 0; let t = Math.imul(s ^ s >>> 15, 1 | s); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296 }
  const needRep = dev.status === '维修中' || dev.status === '待维修' || dev.status === '异常' || dev.open_repairs > 0
  const base = (Date.parse(dev.updated_at) || Date.now()) - 86400000
  const repairs = needRep ? [{ stage: dev.status === '待维修' ? '待接收' : (dev.status === '异常' ? '损坏原因确认中' : '维修中'), issue_description: dev.device_type + ' 相机模块无画面，待进一步检测', repair_owners: ['刘希源'], last_followed_at: new Date(base - 86400000).toISOString(), last_follow_note: '已联系现场确认寄回地址' }] : []
  const flows = [{ action: '设备登记', operator: '李娜', ts: new Date(base - 19 * 86400000).toISOString(), note: '台账初始登记' }, { action: '接收确认', operator: '刘希源', ts: new Date(base - 5 * 86400000).toISOString(), note: '收到设备，外观完好' }, { action: '维修跟进', operator: '刘希源', ts: new Date(base - 86400000).toISOString(), note: '更换相机模组后复测' }]
  const parts = needRep ? [{ part: '相机', arrival: rnd() < 0.7 ? '完好' : '缺失', item: '相机模组', note: '到货待核对', checked_at: new Date(base - 2 * 86400000).toISOString() }] : []
  return { ledger: dev, repairs, flows, parts, transfers: [{ ts: new Date(base - 14 * 86400000).toISOString(), unit: dev.unit, item: '整机', qty: 1, note: '项目发放' }] }
}

/* ---------- 生命周期 ---------- */
onActivated(() => {
  applyHash()
  if (!st.loaded) loadAll()
  if (!refreshTimer) refreshTimer = setInterval(() => { if (st.loaded && !st.mock) loadAll() }, 60000)
})
onDeactivated(() => { if (refreshTimer) { clearInterval(refreshTimer); refreshTimer = null } })

// hash 变化时同步子页签/筛选/下钻（App.vue 只切顶级视图；此处处理 #atab=/q=/unit= 深链）
function onHashChange() {
  const p = new URLSearchParams(location.hash.slice(1))
  if (p.get('view') !== 'assets' && p.get('view') !== 'repairs') return
  applyHash()
}
if (typeof window !== 'undefined') {
  onMounted(() => window.addEventListener('hashchange', onHashChange))
  onUnmounted(() => {
    window.removeEventListener('hashchange', onHashChange)
    if (refreshTimer) { clearInterval(refreshTimer); refreshTimer = null }
  })
}

// 维修看板子页签携带 q（台账徽章跳转/深链）
const repQuery = ref('')
watch(() => st.tab, t => {
  if (t === 'repairs') {
    const p = new URLSearchParams(location.hash.slice(1))
    const q = (p.get('view') === 'assets' && p.get('atab') === 'repairs') ? p.get('q') : ''
    if (q) repQuery.value = q
  }
})
</script>

<template>
  <section id="viewAssets">
    <nav id="assetTabs" class="vtabs asub">
      <button v-for="t in [['dashboard', '资产总览'], ['hardware', '硬件台账'], ['repairs', '维修看板'], ['versions', '版本视图'], ['units', '项目视图']]" :key="t[0]"
              type="button" :class="{ on: st.tab === t[0] }" @click="go(t[0])">{{ t[1] }}</button>
    </nav>

    <div v-if="st.loaded || st.err || st.mock" id="asSyncBar" :class="syncBar.cls" v-html="syncBar.html"></div>

    <!-- 资产总览 -->
    <section v-show="st.tab === 'dashboard'" id="atabDashboard">
      <div class="toolbar">
        <button type="button" id="asReportPush" class="listbtn" title="立即向飞书群推送一次资产日报（测试用；需服务器已配置 FRP_MONITOR_FEISHU_WEBHOOK，未配置会如实报错）" @click="pushReport">📨 立即推送日报（测试）</button>
        <span id="asReportState" class="uhint">{{ st.reportState }}</span>
      </div>
      <section v-if="st.stats" class="amcards" id="asMetrics">
        <div v-for="(m, i) in metricCards" :key="i" class="amcard" :class="{ rate: m.rate }" :style="{ '--c': m.c || '' }" :title="m.sub + '\n' + m.stime">
          <b>{{ m.v == null ? '—' : m.v }}</b>
          <span class="albl">{{ m.l }}</span>
          <div class="asub2">{{ m.sub }}<br>{{ m.stime }}</div>
        </div>
      </section>
      <div v-if="st.stats" class="adists" id="asDists">
        <div v-for="(p, i) in distPanels()" :key="i" class="apanel">
          <h3>{{ p.title }}</h3>
          <p v-if="p.hint" class="uhint" style="margin: 0 0 10px">{{ p.hint }}</p>
          <template v-if="p.bars">
            <div v-for="(b, j) in p.bars" :key="j" class="abar" :title="b.k + '：' + b.n">
              <span class="albl2">{{ b.k }}</span>
              <div class="atrack"><i :style="{ width: (b.n / Math.max(...p.bars.map(x => x.n), 1) * 100).toFixed(1) + '%', '--c': b.c }"></i></div>
              <span class="an">{{ b.n }}</span>
            </div>
            <p v-if="!p.bars.length" class="eempty">暂无数据</p>
          </template>
          <template v-if="p.units">
            <div v-for="u in p.units" :key="u.unit" class="abar"
                 :title="u.unit + '：在线 ' + (u.online != null ? u.online : '—') + ' / 在册 ' + (u.total != null ? u.total : '—') + ' · 使用率 ' + (u.usageRate == null ? '—（不可计算）' : (Math.round(u.usageRate * 1000) / 10) + '%') + '（在线/在册（快照））'">
              <span class="albl2">{{ u.unit }}</span>
              <div class="atrack"><i :style="{ width: (u.usageRate == null ? 0 : Math.min(Math.round(u.usageRate * 1000) / 10, 100)) + '%', '--c': '#0891b2' }"></i></div>
              <span class="an">{{ u.usageRate == null ? '—' : (Math.round(u.usageRate * 1000) / 10) + '%' }}</span>
            </div>
            <p v-if="!p.units.length" class="eempty">暂无单位数据</p>
          </template>
        </div>
      </div>
      <p v-if="st.reportText" style="display:none"></p>
    </section>

    <!-- 硬件台账 -->
    <section v-show="st.tab === 'hardware'" id="atabHardware">
      <div class="toolbar">
        <input id="asQ" v-model="st.hw.q" placeholder="搜索设备编号 / 去向备注 / 单位">
        <select id="asStatus" v-model="st.hw.status">
          <option value="">全部状态</option>
          <option v-for="[k, n] in filterOptions.status" :key="k" :value="k">{{ k }}（{{ n }}）</option>
        </select>
        <select id="asType" v-model="st.hw.type">
          <option value="">全部类型</option>
          <option v-for="[k, n] in filterOptions.type" :key="k" :value="k">{{ shortType(k) }}（{{ n }}）</option>
        </select>
        <select id="asUnit" v-model="st.hw.unit">
          <option value="">全部单位</option>
          <option v-for="[k, n] in filterOptions.unit" :key="k" :value="k">{{ k }}（{{ n }}）</option>
        </select>
        <label class="aswitch" title="归档（已拆解）设备默认隐藏，勾选后追加在表格底部">
          <input type="checkbox" id="asArchived" v-model="st.hw.archived">显示归档设备
        </label>
      </div>
      <div class="twrap">
        <table class="astable">
          <thead>
            <tr>
              <th v-for="c in LEDGER_COLS" :key="c[0]" class="sort" @click="sortCol(c[0])">
                {{ c[1] }} <span v-if="st.hw.sortKey === c[0]" class="sarr">{{ st.hw.sortDir > 0 ? '▲' : '▼' }}</span>
              </th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="d in ledgerRows" :key="d.code + (d.archived ? '-arc' : '')" :class="{ aarch: d.archived }" @click="open360(d.code)">
              <td class="mono">{{ d.code.toUpperCase() }}</td>
              <td>{{ shortType(d.type) || '—' }}</td>
              <td><span class="stag" :style="{ '--c': ST_COLOR[d.status] || '#64748b' }">{{ d.status || '—' }}</span></td>
              <td>{{ d.unit || '—' }}</td>
              <td class="wrap" :title="d.parts || ''">{{ d.parts || '—' }}</td>
              <td class="wrap" :title="d.remark || ''">{{ d.remark || '—' }}</td>
              <td>{{ d.ledgerVer || '—' }}</td>
              <td>{{ d.detVer || '—' }}</td>
              <td>
                <span v-if="d.online === true" class="aon" title="在线">●</span>
                <span v-else-if="d.online === false" class="aoff" title="离线">○</span>
                <span v-else class="anull" title="未匹配">—</span>
              </td>
              <td><span v-if="d.idle === true" class="stag" style="--c: #7c3aed" :title="'库存外·超 ' + idleDays() + ' 天未上线'">闲置</span><span v-else class="anull" title="不在闲置口径或不可判定">—</span></td>
              <td>
                <button v-if="d.openRepairs > 0" class="abadge" :title="'点击查看维修看板'" @click.stop="st.hw.q = d.code.toLowerCase(); go('repairs', { q: d.code })">{{ d.openRepairs }}</button>
                <span v-else class="anull">0</span>
              </td>
              <td class="mut">{{ fmtClockOf(d.updatedAt) || '—' }}</td>
            </tr>
            <tr v-if="!ledgerRows.length"><td colspan="12" class="eempty">没有匹配的设备（可调整筛选或关键词）</td></tr>
          </tbody>
        </table>
      </div>
      <p class="uhint" id="asLedgerHint">{{ ledgerHint }}</p>
    </section>

    <!-- 维修看板 -->
    <section v-show="st.tab === 'repairs'" id="atabRepairs">
      <RepairsBoard ref="repBoard" :initial-q="repQuery" />
    </section>

    <!-- 版本视图 -->
    <section v-show="st.tab === 'versions'" id="atabVersions">
      <div id="asVersionBody">
        <p v-if="!versData" class="eempty">暂无版本数据</p>
        <template v-else>
          <div class="adists">
            <div class="apanel">
              <h3>探测版本分布（在线设备）</h3>
              <div v-for="(x, i) in versData.det" :key="i" class="abar">
                <span class="albl2 mono">{{ x.version || '—' }}</span>
                <div class="atrack"><i :style="{ width: ((x.count || 0) / versData.dmax * 100).toFixed(1) + '%' }"></i></div>
                <span class="an">{{ x.count || 0 }}</span>
              </div>
              <p v-if="!versData.det.length" class="eempty">暂无探测数据</p>
            </div>
            <div class="apanel">
              <h3>台账登记版本</h3>
              <div v-if="versData.ledgerEmpty" class="uwarn" style="margin: 0 0 12px">台账「软件版本」当前全部未登记：<b>{{ versData.ledgerEmpty }}</b> 台为空（如实展示，待台账补录后此处显示分布）</div>
              <div v-for="(x, i) in versData.led" :key="i" class="abar">
                <span class="albl2 mono">{{ x.version || '—' }}</span>
                <div class="atrack"><i :style="{ width: ((x.count || 0) / versData.lmax * 100).toFixed(1) + '%', '--c': '#0f766e' }"></i></div>
                <span class="an">{{ x.count || 0 }}</span>
              </div>
              <p v-if="!versData.led.length" class="eempty">无已登记版本</p>
            </div>
            <div class="apanel">
              <h3>台账 × 探测对照</h3>
              <div class="acards3">
                <div><b>{{ versData.matched != null ? versData.matched : '—' }}</b>版本匹配</div>
                <div><b>{{ versData.only.length }}</b>在线但台账未登记</div>
                <div><b>{{ versData.miss.length }}</b>台账有但探测不到</div>
              </div>
              <h4>detected_only · 在线但台账未登记</h4>
              <span v-for="(x, i) in versData.only" :key="i" class="chip2" :title="typeof x === 'string' ? x : (x.device_code || '')">{{ (typeof x === 'string' ? x : String(x.device_code || '')).toUpperCase() }}</span>
              <p v-if="!versData.only.length" class="eempty">无</p>
              <h4>ledger_missing · 台账有但探测不到（离线 / 未部署）</h4>
              <template v-if="versData.miss.length">
                <p class="uhint">共 {{ versData.miss.length }} 台</p>
                <details>
                  <summary style="cursor: pointer; font-size: 12px; color: #2563eb">展开设备列表</summary>
                  <div style="margin-top: 8px">
                    <span v-for="(x, i) in versData.miss" :key="i" class="chip2" :title="typeof x === 'string' ? x : (x.device_code || '')">{{ (typeof x === 'string' ? x : String(x.device_code || '')).toUpperCase() }}</span>
                  </div>
                </details>
              </template>
              <p v-else class="eempty">无</p>
              <p style="margin-top: 14px"><button class="listbtn" @click="location.hash = 'view=upgrade'">▶ 前往升级视图（版本批量升级入口）</button></p>
            </div>
            <div v-if="versData.t5.length" class="apanel">
              <h3>主要服务商版本占比（在册数前 5）</h3>
              <p class="uhint" style="margin: 0 0 4px">口径：探测版本（未探测计入「未获取」桶） · 占比 = 台数 / 该单位在册数 · 统计时间：{{ fmtClockOf(st.stats && st.stats.lastSync) || '—' }}</p>
              <template v-for="p in versData.t5" :key="p.unit">
                <h4>{{ p.unit }}（在册 {{ p.total != null ? p.total : '—' }} 台）</h4>
                <div v-for="x in p.versions" :key="x.version" class="abar vbar" :title="p.unit + ' · ' + x.version + '：' + x.count + ' 台'">
                  <span class="albl2 mono" :class="{ mut: x.version === '未获取' }">{{ x.version }}</span>
                  <div class="atrack"><i :style="{ width: (x.count / Math.max(...p.versions.map(v => v.count || 0), 1) * 100).toFixed(1) + '%', '--c': x.version === '未获取' ? '#94a3b8' : '#2563eb' }"></i></div>
                  <span class="an">{{ x.count }} 台 · {{ x.share != null ? (Math.round(x.share * 1000) / 10) + '%' : '—' }}</span>
                </div>
              </template>
            </div>
            <div v-else class="apanel">
              <h3>主要服务商版本占比（在册数前 5）</h3>
              <p class="eempty">暂无服务商版本分布数据——版本来自 fleet 在线探测（未探测计入「未获取」桶）；后端 by_provider_top5 就绪后此处自动展示，前端无需改动。</p>
            </div>
          </div>
        </template>
      </div>
    </section>

    <!-- 项目视图 -->
    <section v-show="st.tab === 'units'" id="atabUnits">
      <div v-show="!st.drillUnit" id="asUnitBody">
        <p v-if="!st.units || !st.units.length" class="eempty">暂无单位数据</p>
        <div v-else class="aunitgrid">
          <div v-for="u in st.units" :key="u.unit" class="aunit" title="点击下钻该单位设备列表" @click="drill(u.unit)">
            <div class="auhead"><b>{{ u.unit }}</b><span>{{ u.total != null ? u.total : '—' }} 台</span></div>
            <div class="abarate">
              <div class="atrack"><i :style="{ width: (u.normalRate == null ? 0 : Math.min(Math.round(u.normalRate * 1000) / 10, 100)) + '%', '--c': '#16a34a' }"></i></div>
              <span>{{ u.normalRate == null ? '—' : (Math.round(u.normalRate * 1000) / 10) + '%' }} 正常</span>
            </div>
            <div class="abarate aurate" :title="'使用率 = frps 在线快照 / 在册（在线 ' + (u.online != null ? u.online : '—') + ' / ' + (u.total != null ? u.total : '—') + '，快照口径；fleet 未启用或无法计算时为 —）'">
              <div class="atrack"><i :style="{ width: (u.usageRate == null ? 0 : Math.min(Math.round(u.usageRate * 1000) / 10, 100)) + '%', '--c': '#0891b2' }"></i></div>
              <span>{{ u.usageRate == null ? '—' : (Math.round(u.usageRate * 1000) / 10) + '%' }} 使用率</span>
            </div>
            <div class="aurow">
              <span class="stag" :style="{ '--c': ST_COLOR['维修中'] }">维修中 {{ u.repairing != null ? u.repairing : '—' }}</span>
              <span class="stag" :style="{ '--c': ST_COLOR['异常'] }">异常 {{ u.abnormal != null ? u.abnormal : '—' }}</span>
            </div>
            <div v-if="st.stats && (st.stats.invUnits || []).includes(u.unit)" class="auinv">
              公司库存单位{{ (st.stats.invByUnit || {})[u.unit] != null ? ' ' + st.stats.invByUnit[u.unit] + ' 台' : '' }}（不计一线部署）{{ u.unit === '上海办公室' && shanghaiInv() ? ' · 备注细分：未使用 ' + shanghaiInv().unused + ' · 新组装 ' + shanghaiInv().newBuild : '' }}
            </div>
            <div class="aumut">最近更新：{{ fmtClockOf(u.lastUpdated) || '—' }}</div>
          </div>
        </div>
      </div>
      <div v-if="st.drillUnit" id="asUnitDetail" class="apanel">
        <div class="auhead2">
          <button class="listbtn" @click="st.drillUnit = ''; writeHash({ atab: 'units' })">← 返回单位列表</button>
          <b>{{ st.drillUnit }}</b>
          <span>{{ drillData(st.drillUnit).devs.length }} 台在册{{ drillData(st.drillUnit).rep.length ? ' · 维修中/待维修 ' + drillData(st.drillUnit).rep.length + ' 台' : '' }}</span>
        </div>
        <h3 v-if="drillData(st.drillUnit).rep.length">维修中 / 待维修设备</h3>
        <div v-if="drillData(st.drillUnit).rep.length" class="twrap">
          <table class="astable">
            <thead><tr><th v-for="c in LEDGER_COLS" :key="c[0]">{{ c[1] }}</th></tr></thead>
            <tbody>
              <tr v-for="d in drillData(st.drillUnit).rep" :key="d.code" @click="open360(d.code)">
                <td class="mono">{{ d.code.toUpperCase() }}</td><td>{{ shortType(d.type) || '—' }}</td>
                <td><span class="stag" :style="{ '--c': ST_COLOR[d.status] || '#64748b' }">{{ d.status || '—' }}</span></td>
                <td>{{ d.unit || '—' }}</td><td class="wrap">{{ d.parts || '—' }}</td><td class="wrap">{{ d.remark || '—' }}</td>
                <td>{{ d.ledgerVer || '—' }}</td><td>{{ d.detVer || '—' }}</td>
                <td><span v-if="d.online === true" class="aon">●</span><span v-else-if="d.online === false" class="aoff">○</span><span v-else class="anull">—</span></td>
                <td><span v-if="d.idle === true" class="stag" style="--c: #7c3aed">闲置</span><span v-else class="anull">—</span></td>
                <td><span class="anull">{{ d.openRepairs || 0 }}</span></td>
                <td class="mut">{{ fmtClockOf(d.updatedAt) || '—' }}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <h3>该单位全部设备（点击行查看 360° 详情）</h3>
        <div class="twrap">
          <table class="astable">
            <thead><tr><th v-for="c in LEDGER_COLS" :key="c[0]">{{ c[1] }}</th></tr></thead>
            <tbody>
              <tr v-for="d in drillData(st.drillUnit).devs" :key="d.code" @click="open360(d.code)">
                <td class="mono">{{ d.code.toUpperCase() }}</td><td>{{ shortType(d.type) || '—' }}</td>
                <td><span class="stag" :style="{ '--c': ST_COLOR[d.status] || '#64748b' }">{{ d.status || '—' }}</span></td>
                <td>{{ d.unit || '—' }}</td><td class="wrap">{{ d.parts || '—' }}</td><td class="wrap">{{ d.remark || '—' }}</td>
                <td>{{ d.ledgerVer || '—' }}</td><td>{{ d.detVer || '—' }}</td>
                <td><span v-if="d.online === true" class="aon">●</span><span v-else-if="d.online === false" class="aoff">○</span><span v-else class="anull">—</span></td>
                <td><span v-if="d.idle === true" class="stag" style="--c: #7c3aed">闲置</span><span v-else class="anull">—</span></td>
                <td><span class="anull">{{ d.openRepairs || 0 }}</span></td>
                <td class="mut">{{ fmtClockOf(d.updatedAt) || '—' }}</td>
              </tr>
              <tr v-if="!drillData(st.drillUnit).devs.length"><td colspan="12" class="eempty">该单位暂无在册设备</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </section>

    <!-- 设备 360° 详情弹窗 -->
    <div v-if="modal360" id="modal" class="modal show" @click.self="modal360 = null">
      <div id="modalBody" class="mbody">
        <p v-if="!detail360" class="eempty">加载中…</p>
        <p v-else-if="detail360.error" class="eempty">读取设备详情失败：{{ detail360.error }}</p>
        <p v-else-if="st.reportText" style="display:none"></p>
        <template v-else-if="norm360()">
          <header>
            <div>
              <span class="name">{{ norm360().dev.code.toUpperCase() }}</span>
              <span class="stag" :style="{ '--c': ST_COLOR[norm360().dev.status] || '#64748b' }">{{ norm360().dev.status || '—' }}</span>
              <span v-if="norm360().dev.archived" class="stag" style="--c: #64748b">归档</span>
            </div>
            <button class="x" @click="modal360 = null">✕</button>
          </header>
          <h3>台账信息</h3>
          <div class="mgrid">
            <span>设备编号</span><b>{{ norm360().dev.code.toUpperCase() }}</b>
            <span>设备类型</span><b>{{ norm360().dev.type || '—' }}</b>
            <span>所属单位</span><b>{{ norm360().dev.unit || '—' }}</b>
            <span>配件情况</span><b>{{ norm360().dev.parts || '—' }}</b>
            <span>去向 / 备注</span><b>{{ norm360().dev.remark || '—' }}</b>
            <span>台账软件版本</span><b>{{ norm360().dev.ledgerVer || '—' }}</b>
            <span>探测版本</span><b>{{ norm360().dev.detVer || '—' }}</b>
            <span>在线状态</span>
            <b>
              <span v-if="norm360().dev.online === true" style="color: #16a34a">● 在线</span>
              <span v-else-if="norm360().dev.online === false" style="color: #dc2626">○ 离线</span>
              <span v-else>—（未匹配）</span>
            </b>
            <span>最后在线</span><b>{{ norm360().dev.lastSeen ? fmtClockOf(norm360().dev.lastSeen) : '—（从未上线或 fleet 未启用）' }}</b>
            <span>闲置</span>
            <b>
              <span v-if="norm360().dev.idle === true" style="color: #7c3aed">是（超 {{ idleDays() }} 天未上线）</span>
              <span v-else-if="norm360().dev.idle === false">否</span>
              <span v-else>—（不可判定）</span>
            </b>
            <span>未完结维修</span><b>{{ norm360().dev.openRepairs || 0 }} 条</b>
            <span>最后更新</span><b>{{ fmtClockOf(norm360().dev.updatedAt) || '—' }}</b>
            <span>修改人</span><b>{{ norm360().dev.modifier || '—' }}</b>
          </div>
          <h3>维修记录（异常设备表）</h3>
          <div class="twrap">
            <table class="astable">
              <thead><tr><th>阶段</th><th>问题描述</th><th>负责人</th><th>最后跟进</th><th>跟进说明</th></tr></thead>
              <tbody>
                <tr v-for="(r, i) in (norm360().raw.repairs || [])" :key="i">
                  <td>{{ r.stage || '—' }}</td>
                  <td class="wrap">{{ r.issue_description || r.issue || r.problem || '—' }}</td>
                  <td>{{ Array.isArray(r.repair_owners) ? r.repair_owners.join('、') : (r.repair_owners || r.owner || r.owner_name || '—') }}</td>
                  <td>{{ fmtClockOf(r.last_followed_at) || '—' }}</td>
                  <td class="wrap">{{ r.last_follow_note || r.last_followup_note || '—' }}</td>
                </tr>
                <tr v-if="!(norm360().raw.repairs || []).length"><td colspan="5" class="eempty">暂无数据</td></tr>
              </tbody>
            </table>
          </div>
          <h3>处理流水（异常处理流转）</h3>
          <div class="twrap">
            <table class="astable">
              <thead><tr><th>动作</th><th>操作人</th><th>时间</th><th>说明</th></tr></thead>
              <tbody>
                <tr v-for="(r, i) in (norm360().raw.actions || norm360().raw.flows || [])" :key="i">
                  <td>{{ r.action || '—' }}</td>
                  <td>{{ r.operator || r.created_by || '—' }}</td>
                  <td>{{ fmtClockOf(r.ts || r.time || r.operated_at) || '—' }}</td>
                  <td class="wrap">{{ r.note || r.summary || '—' }}</td>
                </tr>
                <tr v-if="!((norm360().raw.actions || norm360().raw.flows || [])).length"><td colspan="4" class="eempty">暂无数据</td></tr>
              </tbody>
            </table>
          </div>
          <h3>配件核对</h3>
          <div class="twrap">
            <table class="astable">
              <thead><tr><th>配件</th><th>到货状态</th><th>核对项</th><th>状态说明</th><th>核对时间</th></tr></thead>
              <tbody>
                <tr v-for="(r, i) in (norm360().raw.parts_checks || norm360().raw.parts || [])" :key="i">
                  <td>{{ r.part || r.accessory || '—' }}</td>
                  <td>{{ r.arrival || r.arrival_status || '—' }}</td>
                  <td>{{ r.item || r.check_item || '—' }}</td>
                  <td class="wrap">{{ r.note || r.status_note || '—' }}</td>
                  <td>{{ fmtClockOf(r.checked_at || r.time) || '—' }}</td>
                </tr>
                <tr v-if="!((norm360().raw.parts_checks || norm360().raw.parts || [])).length"><td colspan="5" class="eempty">暂无数据</td></tr>
              </tbody>
            </table>
          </div>
          <template v-if="(norm360().raw.transfers || []).length">
            <h3>流转记录</h3>
            <div class="twrap">
              <table class="astable">
                <thead><tr><th>时间</th><th>所属单位</th><th>物品</th><th>数量</th><th>说明</th></tr></thead>
                <tbody>
                  <tr v-for="(r, i) in norm360().raw.transfers" :key="i">
                    <td>{{ fmtClockOf(r.ts || r.time || r.occurred_at) || '—' }}</td>
                    <td>{{ r.unit || '—' }}</td>
                    <td>{{ r.item || '—' }}</td>
                    <td>{{ r.qty != null ? r.qty : (r.quantity != null ? r.quantity : '—') }}</td>
                    <td class="wrap">{{ r.note || '—' }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </template>
        </template>
      </div>
    </div>

    <!-- 日报推送结果弹窗 -->
    <div v-if="st.reportText" id="modal" class="modal show" @click.self="st.reportText = ''">
      <div id="modalBody" class="mbody">
        <header>
          <div><span class="name">资产日报 · 推送成功（飞书群应已收到）</span></div>
          <button class="x" @click="st.reportText = ''">✕</button>
        </header>
        <pre class="rptext">{{ st.reportText }}</pre>
      </div>
    </div>
  </section>
</template>
