// 设备总览共享状态（模块级单例）：fleet 数据、筛选、采集时长、webfix 补配状态
// 轮询由 App.vue 挂载时启动、卸载时停止；总览与升级视图共用同一份数据
import { reactive, computed } from 'vue'
import { api } from '../api'

export const hosts = reactive({
  fleet: [], sites: [], versions: [], sshHint: {},
  online: 0, registered: 0, timestamp: 0, refreshInterval: 15,
  sub: '正在加载…', loaded: false, err: ''
})

export const filters = reactive({
  q: '', site: '', ver: '', pendingOnly: false, status: 'online', targetVer: ''
})

export const collectDur = reactive({ enabled: false, map: {} })
export const webfix = reactive({ map: {}, cands: {} })

export function matches(f) {
  const { site, ver, pendingOnly, targetVer, q, status } = filters
  if (site && (f.site || '未登记') !== site) return false
  if (ver && (f.version || '未获取') !== ver) return false
  if (pendingOnly && (f.version || '未获取') === targetVer) return false
  if (q) {
    const n = q.toLowerCase()
    if (!f.hostname.includes(n) &&
        !String(f.device_id || '').toLowerCase().includes(n) &&
        !String(f.device_name || '').toLowerCase().includes(n) &&
        !String(f.ip || '').includes(n) &&
        !String(f.hw_model || '').toLowerCase().includes(n)) return false
  }
  if (status === 'online' && !f.online) return false
  if (status === 'offline' && f.online) return false
  return true
}

export const filtered = computed(() => hosts.fleet.filter(matches))

export const pendingCount = computed(() =>
  hosts.fleet.filter(f => !f.retired && (f.version || '未获取') !== filters.targetVer).length)

export const missingCount = computed(() => hosts.fleet.filter(f => !f.online).length)
export const unregisteredCount = computed(() => hosts.fleet.filter(f => !f.site).length)

export const pendingVerCount = computed(() =>
  hosts.fleet.filter(f => !f.retired && (f.version || '未获取') !== filters.targetVer).length)

export async function loadHosts(force = false) {
  try {
    const r = await api('/api/hosts' + (force ? '?refresh=1' : ''))
    const d = await r.json()
    if (!r.ok) throw new Error(d.error || '请求失败')
    hosts.fleet = d.fleet || []
    hosts.sites = d.sites || []
    hosts.versions = d.versions || []
    hosts.sshHint = d.ssh_hint || {}
    if (d.refresh_interval) hosts.refreshInterval = d.refresh_interval
    hosts.online = d.online
    hosts.registered = d.registered != null ? d.registered : d.total
    hosts.timestamp = d.timestamp
    hosts.err = ''
    hosts.loaded = true
    hosts.sub = '最后更新：' + new Date(d.timestamp * 1000).toLocaleString('zh-CN', { hour12: false }) +
      ' · ' + d.online + ' 在线 / ' + hosts.registered + ' 台登记 · 每 ' + hosts.refreshInterval + 's 自动刷新'
    // 目标版本兜底（版本列表变化后保持有效选择）
    const opts = hosts.versions.map(v => v.version)
    if (!filters.targetVer || !opts.includes(filters.targetVer)) {
      filters.targetVer = opts.find(v => v !== '未获取') || opts[0] || ''
    }
  } catch (e) {
    hosts.sub = '获取 frps 状态失败：' + e
    hosts.err = String(e.message || e)
  }
}

export async function loadCollectDuration() {
  try {
    const r = await api('/api/collect-duration?days=7')
    const d = await r.json()
    if (!r.ok) throw new Error(d.error || '请求失败')
    collectDur.enabled = !!d.enabled
    collectDur.map = {}
    for (const it of d.items || []) collectDur.map[String(it.hostname || '').toLowerCase()] = it
  } catch (e) { /* 接口未上线或拉取失败：沿用已有数据 */ }
}

export async function loadWebfix() {
  try {
    const r = await api('/api/webfix/tasks')
    const d = await r.json()
    if (!r.ok) throw new Error(d.error || ('HTTP ' + r.status))
    let arr = Array.isArray(d) ? d : (d.tasks || d.items || d.devices || d.list || [])
    if (!Array.isArray(arr) && arr && typeof arr === 'object') {
      arr = Object.entries(arr).map(([h, t]) => Object.assign({ hostname: h }, (t && typeof t === 'object') ? t : { status: t }))
    }
    const tsOf = v => {
      if (typeof v === 'number') return v > 1e12 ? v : Math.round(v * 1000)
      const p = Date.parse(String(v == null ? '' : v).replace(' ', 'T'))
      return isNaN(p) ? 0 : p
    }
    const map = {}
    for (const t of (arr || [])) {
      const h = String((t && (t.hostname || t.host)) || '').toLowerCase()
      if (!h) continue
      const ts = tsOf(t && (t.created_at || t.created))
      const old = map[h]
      if (old && ts < old._ts) continue
      map[h] = {
        hostname: h, status: t.status, error: t.error, attempts: t.attempts,
        trigger_type: t.trigger_type, web_port: t.web_port,
        created_at: t.created_at, finished_at: t.finished_at, _ts: ts
      }
    }
    const cands = {}
    for (const c of (d.candidates || [])) if (c && c.hostname) cands[String(c.hostname).toLowerCase()] = c
    webfix.map = map
    webfix.cands = cands
  } catch (e) { /* webfix 接口未上线/拉取失败：沿用已有数据，退化为仅「web 缺配」灰色标记 */ }
}

export function webfixTask(f) {
  return (webfix.map || {})[String((f && (f.hostname || f.host)) || '').toLowerCase()] || null
}

export function webfixState(f) {
  const t = webfixTask(f)
  if (f && f.web_port) return { code: 'normal', task: t }
  if (!(f && f.online && f.ssh_port)) return { code: '', task: t }
  const s = t ? String(t.status || '').toLowerCase() : ''
  if (s === 'success' || s === 'done' || s === 'ok') return { code: '', task: t }
  if (s === 'queued' || s === 'running' || s === 'pending') return { code: 'running', task: t }
  if (s === 'failed' || s === 'fail' || s === 'error') return { code: 'failed', title: (t && t.error) || '补配失败（未返回原因）', task: t }
  return { code: 'missing', task: t }
}

let timers = []

export function startPolling() {
  stopPolling()
  loadHosts()
  loadCollectDuration()
  loadWebfix()
  timers.push(setInterval(() => loadHosts(false), 15000))
  timers.push(setInterval(loadCollectDuration, 300000))
  timers.push(setInterval(loadWebfix, 15000))
}

export function stopPolling() {
  for (const t of timers) clearInterval(t)
  timers = []
}
