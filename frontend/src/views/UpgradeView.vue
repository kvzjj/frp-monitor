<script setup>
// 升级管理视图：OSS 参数（URL 解析/注册表匹配/服务端算哈希）→ 设备分组选择 → 发起（全流程/单阶段）
// → 任务与进度（SSE 实时 + 轮询降级 + 逐台日志跟随）。逻辑自原 Upg.* 忠实平移。
import { ref, reactive, computed, watch, nextTick, onActivated, onUnmounted } from 'vue'
import { api, copyText } from '../api'
import { siteColor, verCmp, fmtBytes, parseTs, fmtClock, fmtDur2 } from '../format'
import { hosts } from '../composables/useHosts'

const STAGES = ['precheck', 'download', 'prepare', 'install', 'verify', 'cleanup']
const STAGE_LBL = { precheck: '预检', download: '下载', prepare: '准备', install: '安装', verify: '验证', cleanup: '清理' }
const KIND_LBL = { upgrade: '全流程升级', precheck: '仅预检', download: '仅下载', prepare: '仅准备', install: '仅安装', verify: '仅验证', cleanup: '仅清理' }
const STAT_LBL = { running: '进行中', success: '成功', failed: '失败', blocked: '已阻断', pending: '等待中' }

const reg = ref([])
const regFail = ref(false)
const selected = ref(new Set())
const selTouched = ref(false)
const targetVersion = ref('')
const urlInput = ref('')
const shaInput = ref('')
const param = reactive({ url: '', ok: false, filename: '', version: '', arch: '', expiresAt: null, sha256: '', shaSrc: '', size: null })
const shaComputing = ref(false)
const job = ref(null)
const tasks = ref([])
const logs = reactive({})
const logOff = reactive({})
const follow = reactive({})
const expanded = ref(null)
const fromSnap = reactive({})
const connState = ref('off')
const stickyNote = ref(null)
const confirmOpen = ref(false)
const hint = ref({ text: '', warn: false })
const logRefs = {}

let es = null, pollT = null, shaPollT = null, shaPollN = 0, urlTimer = null, fleetTimer = null

function normStatus(s) {
  s = String(s == null ? '' : s).toLowerCase()
  if (['success', 'done', 'ok', 'complete', 'completed', 'finished'].includes(s)) return 'success'
  if (['failed', 'fail', 'error', 'err'].includes(s)) return 'failed'
  if (['blocked', 'block', 'skipped', 'skip'].includes(s)) return 'blocked'
  if (['running', 'run', 'active', 'in_progress', 'progress', 'started'].includes(s)) return 'running'
  return 'pending'
}
const jobStatus = computed(() => job.value ? normStatus(job.value.status) : null)
const active = computed(() => !!job.value && ['running', 'pending'].includes(jobStatus.value))

function archOf(f) {
  const m = String((f && (f.hw_model || f.hardware)) || '').toLowerCase()
  if (/rk3588|rk3576|rk3568|arm64|aarch64|orange\s?pi|v1/.test(m)) return 'rk3588-arm64'
  if (/amd64|x86|intel|celeron|n100|v0/.test(m)) return 'amd64'
  return ''
}

/* ---------- ① 参数区 ---------- */
function parseOssUrl(u) {
  u = String(u || '').trim()
  const out = { url: u, ok: false, filename: '', version: '', arch: '', expiresAt: null, err: '', sha256: '', shaSrc: '', size: null }
  if (!u) return out
  let p = null
  try { p = new URL(u) } catch (e) { return out }
  out.ok = true
  const segs = p.pathname.split('/').filter(Boolean)
  try { out.filename = decodeURIComponent(segs[segs.length - 1] || '') } catch (e) { out.filename = segs[segs.length - 1] || '' }
  const vm = out.filename.match(/v\d+\.\d+(\.\d+)*/i)
  out.version = vm ? vm[0].toLowerCase() : ''
  if (/rk3588|arm64|aarch64/i.test(out.filename)) out.arch = 'rk3588-arm64'
  else if (/amd64|x86/i.test(out.filename)) out.arch = 'amd64'
  const v1 = parseInt(p.searchParams.get('Expires') || '', 10)
  if (!isNaN(v1) && v1 > 1000000) out.expiresAt = v1 * 1000
  else {
    const d = p.searchParams.get('x-oss-date') || '', dur = parseInt(p.searchParams.get('x-oss-expires') || '', 10)
    const m = d.match(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/)
    if (m && !isNaN(dur)) {
      const t = Date.parse(m[1] + '-' + m[2] + '-' + m[3] + 'T' + m[4] + ':' + m[5] + ':' + m[6] + 'Z')
      if (!isNaN(t)) out.expiresAt = t + dur * 1000
    }
  }
  return out
}

function fmtRemain(ms) {
  if (ms == null || isNaN(ms)) return '—'
  if (ms <= 0) return '已过期'
  const h = ms / 3600000
  if (h >= 48) return Math.floor(h / 24) + ' 天'
  if (h >= 1) return h.toFixed(1) + ' 小时'
  return Math.max(1, Math.round(ms / 60000)) + ' 分钟'
}
function estMs() { return Math.max(1, Math.ceil((selected.value.size || 1) / 6)) * 12 * 60000 }

function onUrlInput() {
  const prev = param.filename
  Object.assign(param, parseOssUrl(urlInput.value))
  targetVersion.value = param.version
  shaComputing.value = false
  stopShaPoll()
  if (param.ok && param.filename && prev !== param.filename) {
    if (reg.value.length) matchRegistry()
    else loadRegistry()
  }
}

watch(urlInput, () => {
  clearTimeout(urlTimer)
  urlTimer = setTimeout(onUrlInput, 400)
})

watch(shaInput, v => {
  const s = v.trim().toLowerCase()
  param.sha256 = /^[0-9a-f]{64}$/.test(s) ? s : ''
  param.shaSrc = param.sha256 ? 'manual' : ''
})

watch(() => param.sha256, v => { if (param.shaSrc !== 'manual' && shaInput.value !== (v || '')) shaInput.value = v || '' })

async function loadRegistry() {
  try {
    const r = await api('/api/upgrade/packages')
    const d = await r.json()
    if (!r.ok) throw new Error(d.error || ('HTTP ' + r.status))
    reg.value = d.packages || d.items || (Array.isArray(d) ? d : [])
    regFail.value = false
    if (param.filename && param.shaSrc !== 'server') matchRegistry()
  } catch (e) { reg.value = []; regFail.value = true }
}

function regEntry() {
  return reg.value.find(x => x.version === param.version && x.arch === param.arch) ||
         reg.value.find(x => String(x.filename || '') === param.filename && x.filename)
}

function matchRegistry() {
  const fn = param.filename
  let hit = reg.value.find(x => String(x.filename || x.name || '') === fn)
  if (!hit && param.version) hit = reg.value.find(x => x.version === param.version && (!param.arch || x.arch === param.arch))
  if (hit && hit.sha256) {
    param.sha256 = String(hit.sha256).toLowerCase()
    param.shaSrc = 'registry'
    param.size = hit.size != null ? hit.size : null
    return true
  }
  return false
}

const regStateText = computed(() => {
  if (regFail.value) return '包注册表接口不可用（后端未上线？），sha256 可手填'
  const computing = reg.value.filter(x => x.hash_status === 'computing').length
  return reg.value.length ? '注册表已加载 ' + reg.value.length + ' 条' + (computing ? ' · ' + computing + ' 个计算中' : '') : ''
})

const expiry = computed(() => {
  if (!param.ok || !param.url) return { text: '', cls: 'pmeta' }
  if (param.expiresAt == null) return { text: '⚠ 未识别到有效期参数（Expires / x-oss-expires），请自行确认链接时效', cls: 'pmeta warn' }
  const rem = param.expiresAt - Date.now()
  const left = fmtRemain(rem)
  const until = '至 ' + new Date(param.expiresAt).toLocaleString('zh-CN', { hour12: false })
  if (rem <= 0) return { text: '链接已过期（' + until + '），请重新生成签名 URL 后粘贴', cls: 'pmeta bad' }
  if (rem < estMs()) return { text: '有效期约剩 ' + left + '（' + until + '）——低于预估批量时长 ' + fmtRemain(estMs()) + '（' + selected.value.size + ' 台 · 6 并行），批量中途可能过期', cls: 'pmeta warn' }
  return { text: '有效期约剩 ' + left + '（' + until + '）· 预估批量时长 ' + fmtRemain(estMs()), cls: 'pmeta' }
})

const shaMeta = computed(() => {
  if (shaComputing.value) return { text: '服务端计算中：正在从 OSS 直连拉取整包（约 1G，耗时视出口带宽），完成后自动填入并缓存进注册表…', cls: 'pmeta' }
  if (shaErr.value) return { text: shaErr.value, cls: 'pmeta bad' }
  if (param.sha256 || param.shaSrc === 'manual') return { text: '', cls: 'pmeta' }
  if (param.ok && param.filename) return { text: regFail.value ? '注册表不可用且未填 sha256，可手填' : '注册表未命中，可手填或点「服务端计算」', cls: 'pmeta' }
  return { text: '', cls: 'pmeta' }
})
const shaErr = ref('')

async function computeSha() {
  if (shaComputing.value) return
  if (!param.ok || !param.url) return setHint('请先粘贴有效的 OSS 签名 URL', true)
  shaComputing.value = true
  shaErr.value = ''
  try {
    const r = await api('/api/upgrade/packages/hash', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ oss_url: param.url })
    })
    const d = await r.json().catch(() => ({}))
    if (!r.ok) {
      if (d.error_code === 'oss_expired') {
        shaComputing.value = false
        shaErr.value = '服务端计算失败：' + (d.error || '链接已过期') + '，请重新生成签名 URL'
        return
      }
      throw new Error(d.error || ('HTTP ' + r.status))
    }
    if (d.status === 'done' && (d.entry || {}).sha256) {
      param.sha256 = String(d.entry.sha256).toLowerCase()
      param.shaSrc = 'server'
      if (d.entry.size != null) param.size = d.entry.size
      shaComputing.value = false
      loadRegistry()
    } else startShaPoll()
  } catch (e) {
    shaComputing.value = false
    shaErr.value = '服务端计算失败：' + e.message + '（可手填）'
  }
}

async function pollShaTick() {
  try {
    const r = await api('/api/upgrade/packages')
    const d = await r.json()
    if (!r.ok) return
    reg.value = d.packages || d.items || (Array.isArray(d) ? d : [])
    regFail.value = false
  } catch (e) { return }
  const e = regEntry()
  if (e && e.sha256) {
    stopShaPoll(); shaComputing.value = false
    param.sha256 = String(e.sha256).toLowerCase(); param.shaSrc = 'server'
    if (e.size != null) param.size = e.size
    return
  }
  if (e && e.hash_status === 'failed') {
    stopShaPoll(); shaComputing.value = false
    shaErr.value = '服务端计算失败：' + (e.error || e.error_code || '未知原因') + '（可手填）'
    return
  }
}

function startShaPoll() {
  stopShaPoll(); shaPollN = 0
  pollShaTick()
  shaPollT = setInterval(() => {
    shaPollN++
    if (shaPollN > 150) {
      stopShaPoll(); shaComputing.value = false
      shaErr.value = '服务端计算超时（>10 分钟仍未完成），可稍后重试或手填'
      return
    }
    pollShaTick()
  }, 4000)
}
function stopShaPoll() { if (shaPollT) { clearInterval(shaPollT); shaPollT = null } }

function isOssErr(t, code) {
  if (String(code || '').toLowerCase() === 'oss_expired') return true
  t = String(t || '')
  if (!t) return false
  return /(^|[^0-9])403([^0-9]|$)/i.test(t) || /access ?denied|oss_expired|forbidden/i.test(t) || /链接?过期|重新生成/.test(t)
}

/* ---------- ② 设备选择 ---------- */
const devRows = computed(() => hosts.fleet.filter(f => !f.retired).map(f => ({
  h: f.hostname, site: f.site || '', online: !!f.online,
  ver: (f.version || '').trim() || '未获取', arch: archOf(f), hw: f.hardware || f.hw_model || ''
})))

const devGroups = computed(() => {
  const groups = {}
  for (const r of devRows.value) (groups[r.ver] = groups[r.ver] || []).push(r)
  const keys = Object.keys(groups).sort((a, b) => a === '未获取' ? 1 : b === '未获取' ? -1 : verCmp(b, a))
  return keys.map(k => {
    const g = groups[k].slice().sort((a, b) => (b.online - a.online) || a.h.localeCompare(b.h, undefined, { numeric: true }))
    const on = g.filter(r => r.online)
    return { key: k, rows: g, on, selN: on.filter(r => selected.value.has(r.h)).length }
  })
})

function syncSelection() {
  const online = hosts.fleet.filter(f => !f.retired && f.online).map(f => f.hostname)
  if (!selTouched.value) selected.value = new Set(online)
  else {
    const live = new Set(online)
    for (const h of [...selected.value]) if (!live.has(h)) selected.value.delete(h)
  }
}

watch(() => hosts.fleet, syncSelection)

function toggleDev(h, checked) {
  selTouched.value = true
  if (checked) selected.value.add(h)
  else selected.value.delete(h)
}
function toggleGroup(k, checked) {
  selTouched.value = true
  const rows = devRows.value.filter(r => r.ver === k && r.online)
  for (const r of rows) {
    if (checked) selected.value.add(r.h)
    else selected.value.delete(r.h)
  }
}
function pickGroup(k) {
  selTouched.value = true
  const rows = devRows.value.filter(r => r.ver === k && r.online)
  selected.value = new Set(rows.map(r => r.h))
  setHint('已选择 ' + k + ' 全组 ' + rows.length + ' 台，确认无误后在下方发起', false)
  document.getElementById('upgActPanel')?.scrollIntoView({ behavior: 'smooth', block: 'center' })
}
function clearSel() { selected.value.clear(); selTouched.value = true }

const selBarVer = computed(() => {
  const vers = new Set()
  for (const h of selected.value) {
    const f = hosts.fleet.find(x => x.hostname === h)
    vers.add(((f && f.version) || '').trim() || '未获取')
  }
  return selected.value.size ? (vers.size === 1 ? [...vers][0] : '多版本') : '—'
})

/* ---------- ③ 发起 ---------- */
function setHint(text, warn) { hint.value = { text: text || '', warn: !!warn } }

const actionState = computed(() => {
  const busy = active.value
  const noUrl = !param.ok || !param.url
  const noVer = !param.version
  const noSel = !selected.value.size
  const expired = param.expiresAt != null && param.expiresAt - Date.now() <= 0
  const lowTime = param.expiresAt != null && !expired && param.expiresAt - Date.now() < estMs()
  const noSha = !param.sha256
  let mism = 0
  if (param.arch) for (const h of selected.value) {
    const f = hosts.fleet.find(x => x.hostname === h)
    const a = f && archOf(f)
    if (a && a !== param.arch) mism++
  }
  let text = '', warn = false
  if (busy) text = '有升级任务进行中（#' + job.value.id + '），任务结束后才能发起新任务（后端并发护栏兜底）'
  else if (expired) text = 'OSS 链接已过期，请重新生成签名 URL 后粘贴'
  else if (noUrl) text = '请先在「① 升级参数」粘贴 OSS 签名 URL（自动识别版本/机型）'
  else if (noVer) { text = 'URL 中未识别出版本号（文件名需形如 ego-portal-v0.4.6-rk3588-arm64.tar.gz）'; warn = true }
  else if (noSel) text = '请至少勾选一台在线设备（默认已勾选全部在线设备，离线设备不可选）'
  else if (mism) { text = '⚠ 所选设备中 ' + mism + ' 台机型与目标包（' + param.arch + '）不符，下载/安装类操作不可发起；请改选相符机型设备，或更换对应机型的包链接'; warn = true }
  else if (noSha) { text = '⚠ 尚无 sha256 基准：下载/安装类操作不可发起（注册表未命中时请手填或点「服务端计算」）；仅预检/验证/清理可发起'; warn = true }
  else if (lowTime) { text = '⚠ 链接剩余有效期低于预估批量时长（' + fmtRemain(estMs()) + '），批量中途可能过期，建议重新生成更长时效链接'; warn = true }
  if (busy && stickyNote.value && Date.now() < stickyNote.value.until) { text = stickyNote.value.text; warn = stickyNote.value.warn }
  const heavy = busy || noUrl || noVer || noSel || expired || mism > 0 || noSha
  const light = busy || noUrl || noVer || noSel || expired
  return { text, warn, heavy, light, canLaunch: !busy && param.ok && !!param.version && selected.value.size > 0 }
})

const confirmInfo = computed(() => {
  const n = selected.value.size
  const archSplit = {}
  let mism = 0
  for (const h of selected.value) {
    const f = hosts.fleet.find(x => x.hostname === h)
    const a = (f && archOf(f)) || '未知机型'
    if (param.arch && a !== '未知机型' && a !== param.arch) { mism++; archSplit['机型不符'] = (archSplit['机型不符'] || 0) + 1 }
    else archSplit[a] = (archSplit[a] || 0) + 1
  }
  const list = [...selected.value].sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))
  return {
    n, mism,
    archTxt: Object.entries(archSplit).map(([a, c]) => a + ' × ' + c).join('、'),
    preview: list.length > 12 ? list.slice(0, 12).join('、') + ' 等 ' + n + ' 台' : list.join('、'),
    remTxt: param.expiresAt == null ? '未识别（请自行确认链接时效）'
      : (param.expiresAt - Date.now() > 0 ? '约剩 ' : '') + fmtRemain(param.expiresAt - Date.now()) +
        (param.expiresAt - Date.now() > 0 ? '（至 ' + new Date(param.expiresAt).toLocaleString('zh-CN', { hour12: false }) + '）' : '')
  }
})

async function launch(kind) {
  const devices = [...selected.value]
  setHint('正在创建任务…', false)
  try {
    const r = await api('/api/upgrade/jobs', {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ kind, devices, params: { version: targetVersion.value, oss_url: param.url, sha256: param.sha256 || undefined } })
    })
    const d = await r.json().catch(() => ({}))
    if (!r.ok) {
      if (d.error_code === 'oss_expired') { param.expiresAt = Date.now() - 1000; throw new Error((d.error || 'OSS 链接已过期') + '（后端校验拒绝）') }
      if (d.error_code === 'sha_missing') throw new Error(d.error || '缺少 sha256 基准')
      throw new Error(d.error || ('HTTP ' + r.status))
    }
    const j = d.job || d
    const id = j.id != null ? j.id : j.job_id
    if (id == null) throw new Error('响应缺少任务 id（job_id）')
    if (d.expires_at != null) { const t = parseTs(d.expires_at); if (t) param.expiresAt = t }
    for (const h in fromSnap) delete fromSnap[h]
    for (const h of devices) {
      const f = hosts.fleet.find(x => x.hostname === h)
      if (f) fromSnap[h] = f.version || ''
    }
    const effective = (Array.isArray(d.devices) && d.devices.length) ? d.devices : devices
    adoptJob({ id, kind: j.kind || kind, version: j.version || targetVersion.value, filename: j.filename || param.filename || '', status: j.status || 'running', created_at: j.created_at || new Date().toISOString(), finished_at: j.finished_at || null, devices: effective })
    let note = '任务 #' + id + ' 已创建'
    if (Array.isArray(d.skipped) && d.skipped.length) note += '；' + d.skipped.length + ' 台被跳过：' + d.skipped.map(s => s.hostname + '（' + (s.reason || s.error || '原因未说明') + '）').join('、')
    if (Array.isArray(d.warnings) && d.warnings.length) note += '；⚠ ' + d.warnings.map(w => (typeof w === 'string' ? w : (w.message || w.msg || ''))).join('；')
    stickyNote.value = { text: note, warn: !!(d.warnings && d.warnings.length), until: Date.now() + 20000 }
    setHint(stickyNote.value.text, stickyNote.value.warn)
    loadJobDetail(id)
    subscribe()
  } catch (e) { setHint('发起失败：' + e.message, true) }
}

/* ---------- ④ 任务与进度 ---------- */
function normJob(j) {
  return {
    id: j.id != null ? j.id : j.job_id, kind: j.kind || '',
    version: j.version || (j.params && j.params.version) || '', filename: j.filename || '',
    oss_url_expires: j.oss_url_expires != null ? j.oss_url_expires : null,
    error: j.error || j.error_code || '', error_code: j.error_code || '',
    status: j.status || 'running',
    created_at: j.created_at || j.createdAt || null, finished_at: j.finished_at || j.finishedAt || null,
    devices: j.devices || [], summary: j.summary || null
  }
}
function normTask(t) {
  return {
    hostname: t.hostname || t.host || '', stage: String(t.stage || '').toLowerCase(),
    status: normStatus(t.status), error: t.error || t.reason || '', error_code: t.error_code || '',
    started_at: t.started_at || t.startedAt || null, finished_at: t.finished_at || t.finishedAt || null,
    from_version: t.from_version || t.version_before || ''
  }
}
function adoptJob(j) {
  job.value = j
  tasks.value = (j.devices || []).map(h => ({ hostname: h, stage: '', status: 'pending', error: '', from_version: fromSnap[h] || '' }))
  for (const k in logs) delete logs[k]
  for (const k in logOff) delete logOff[k]
  expanded.value = null
}

async function loadJobs() {
  try {
    const r = await api('/api/upgrade/jobs')
    const d = await r.json()
    if (!r.ok) throw new Error(d.error || ('HTTP ' + r.status))
    const list = d.jobs || d.items || (Array.isArray(d) ? d : [])
    if (!list.length) return
    const j = normJob(list[0])
    if (j.id == null) return
    if (!job.value || job.value.id !== j.id) { adoptJob(j); loadJobDetail(j.id) }
    else job.value = j
    if (active.value) subscribe()
  } catch (e) { /* 接口未上线：保留空态 */ }
}

async function loadJobDetail(id) {
  try {
    const r = await api('/api/upgrade/jobs/' + encodeURIComponent(id))
    const d = await r.json()
    if (!r.ok) throw new Error(d.error || ('HTTP ' + r.status))
    const j = normJob(d.job || d)
    if (job.value && job.value.id !== j.id) return
    job.value = j
    const ts = (d.tasks || j.tasks || []).map(normTask)
    if (ts.length) tasks.value = ts
    else if ((j.devices || []).length && !tasks.value.length) {
      tasks.value = j.devices.map(h => ({ hostname: h, stage: '', status: 'pending', error: '', from_version: '' }))
    }
    if (!active.value) teardown()
  } catch (e) { /* 保留已有状态 */ }
}

const jobCounts = computed(() => {
  const cnt = { success: 0, failed: 0, blocked: 0, running: 0, pending: 0 }
  for (const t of tasks.value) cnt[t.status] = (cnt[t.status] || 0) + 1
  return cnt
})
const jobTotal = computed(() => tasks.value.length || ((job.value && job.value.devices) || []).length || ((job.value && job.value.summary && job.value.summary.total) || 0))
const jobElapsed = computed(() => {
  const j = job.value
  if (!j) return null
  const created = parseTs(j.created_at), fin = parseTs(j.finished_at)
  const end = fin || (jobStatus.value === 'running' ? Date.now() : null)
  return (created && end) ? (end - created) / 1000 : null
})
const jobOssExpired = computed(() => {
  const j = job.value
  if (!j) return false
  if (isOssErr(j.error, j.error_code)) return true
  return tasks.value.some(t => t.status === 'failed' && isOssErr(t.error, t.error_code))
})

function taskSteps(t) {
  const scopeIdx = job.value && job.value.kind === 'upgrade' ? null : STAGES.indexOf(job.value.kind)
  const curIdx = STAGES.indexOf(t.stage)
  return STAGES.map((s, i) => {
    let c = ''
    if (scopeIdx != null && i !== scopeIdx) c = 'na'
    else if (t.status === 'success') c = 'done'
    else if (t.status === 'failed' || t.status === 'blocked') c = i === curIdx ? t.status : (curIdx >= 0 && i < curIdx ? 'done' : '')
    else if (t.status === 'running') c = i === curIdx ? 'cur' : (curIdx >= 0 && i < curIdx ? 'done' : '')
    return { label: STAGE_LBL[s], cls: c }
  })
}

function toggleLog(h) {
  expanded.value = expanded.value === h ? null : h
  if (expanded.value === h && job.value) fetchLog(h)
}

async function appendLog(h, txt) {
  if (h == null || txt == null) return
  let s = String(txt)
  if (!s) return
  if (!s.endsWith('\n')) s += '\n'
  logs[h] = (logs[h] || '') + s
  if (expanded.value === h) {
    await nextTick()
    const pre = logRefs[h]
    if (pre && follow[h] !== false) pre.scrollTop = pre.scrollHeight
  }
}

async function fetchLog(h) {
  if (!job.value || job.value.id == null) return
  const off = logOff[h] || 0
  try {
    const r = await api('/api/upgrade/jobs/' + encodeURIComponent(job.value.id) + '/log?offset=' + off + '&hostname=' + encodeURIComponent(h))
    const d = await r.json()
    if (!r.ok) return
    let txt = d.text || d.chunk || d.data || (Array.isArray(d.lines) ? d.lines.join('\n') : '')
    if (!txt) return
    if (d.offset != null) logOff[h] = d.offset
    else logOff[h] = off + String(txt).length
    appendLog(h, txt)
  } catch (e) { /* 忽略 */ }
}

function setConn(s) { connState.value = s }

function subscribe() {
  if (!active.value) return
  if (es && es.readyState <= 1) return
  try { es = new EventSource('/api/upgrade/events') } catch (e) { degrade(); return }
  es.onopen = () => { setConn('live'); if (pollT) { clearInterval(pollT); pollT = null } }
  es.onmessage = e => onEvent('message', e.data)
  for (const n of ['job', 'task', 'log', 'stage', 'progress']) es.addEventListener(n, e => onEvent(n, e.data))
  es.onerror = () => degrade()
}

function degrade() {
  setConn('poll')
  if (!pollT && (active.value || expanded.value != null)) pollT = setInterval(pollTick, 5000)
}
function pollTick() {
  if (!job.value) return
  loadJobDetail(job.value.id)
  if (expanded.value != null) fetchLog(expanded.value)
}
function teardown() {
  if (es) { es.close(); es = null }
  if (pollT) { clearInterval(pollT); pollT = null }
  setConn('off')
}

function onEvent(name, raw) {
  let m = null
  const s = String(raw == null ? '' : raw)
  if (s.trim().startsWith('{')) { try { m = JSON.parse(s) } catch (e) { m = null } }
  if (!m) return
  const type = String(m.type || name || '').toLowerCase()
  if (type === 'webfix' || type === 'webfix_log') return  // 补配事件只驱动总览（useHosts 轮询覆盖）
  if (type === 'job' || m.job) {
    const j = normJob(m.job || m)
    if (j.id == null) return
    if (!job.value || job.value.id !== j.id) adoptJob(j)
    else job.value = j
    if (!active.value) { teardown(); loadJobDetail(j.id) }
    return
  }
  if (type === 'pkg') {
    if (shaComputing.value && m.version != null && String(m.version) === String(param.version) && (!m.arch || String(m.arch) === String(param.arch))) {
      if (m.status === 'ok' && m.sha256) {
        stopShaPoll(); shaComputing.value = false
        param.sha256 = String(m.sha256).toLowerCase(); param.shaSrc = 'server'
        if (m.size != null) param.size = m.size
      } else pollShaTick()
    } else if (m.status === 'ok') loadRegistry()
    return
  }
  if (type === 'task' || m.task) { applyTask(normTask(m.task || m)); return }
  if (type === 'log' || m.text || m.chunk || m.lines) {
    const h = m.hostname
    if (h != null) {
      let txt = m.text || m.chunk || (Array.isArray(m.lines) ? m.lines.join('\n') : '')
      if (m.offset != null) logOff[h] = m.offset
      appendLog(h, txt)
    }
    return
  }
  if (m.hostname && (m.stage != null || m.status != null)) applyTask(normTask(m))
}

function applyTask(t) {
  if (!t.hostname) return
  if (!job.value || !tasks.value.length) tasks.value.push(t)
  const i = tasks.value.findIndex(x => x.hostname === t.hostname)
  if (i >= 0) tasks.value[i] = Object.assign({}, tasks.value[i], t)
  else tasks.value.push(t)
}

async function copySha(ev) {
  const btn = ev.target
  const ok = await copyText(btn.dataset.sha)
  btn.textContent = ok ? '已复制' : '失败'
  setTimeout(() => { btn.textContent = '复制' }, 1500)
}

function onKey(e) {
  if (e.key === 'Escape') confirmOpen.value = false
}

onActivated(() => {
  loadRegistry(); syncSelection(); loadJobs()
})
onUnmounted(() => {
  teardown(); stopShaPoll(); clearTimeout(urlTimer); clearInterval(fleetTimer)
  document.removeEventListener('keydown', onKey)
})
if (typeof document !== 'undefined') document.addEventListener('keydown', onKey)
</script>

<template>
  <section id="viewUpgrade">
    <!-- ① 升级参数 -->
    <section class="upanel" id="upgParamPanel">
      <div class="uhead"><h2>① 升级参数</h2><span class="uhint">{{ regStateText }}</span></div>
      <div class="uparam">
        <div class="prow">
          <label for="upgUrl">OSS 签名 URL</label>
          <input id="upgUrl" v-model="urlInput" type="text" spellcheck="false" autocomplete="off"
                 placeholder="https://<bucket>.oss-cn-hangzhou.aliyuncs.com/.../ego-portal-v0.4.6-rk3588-arm64.tar.gz?Expires=…&OSSAccessKeyId=…&Signature=…">
        </div>
        <div class="pmeta" v-if="!param.url">粘贴后自动识别文件名、版本与机型；签名 URL 属敏感凭据，请勿外传</div>
        <div class="pmeta bad" v-else-if="!param.ok">URL 格式无法解析，请检查后重新粘贴</div>
        <div class="pmeta" :class="{ warn: param.version && param.arch }" v-else>文件名 {{ param.filename || '未识别' }} · 版本 {{ param.version || '未识别' }} · 机型 {{ param.arch || '未识别' }}</div>

        <div class="prow">
          <label for="upgSha">sha256 基准</label>
          <input id="upgSha" v-model="shaInput" type="text" spellcheck="false" autocomplete="off"
                 placeholder="留空自动匹配包注册表，未命中可手填或点「服务端计算」">
          <button type="button" class="stagebtn" title="服务端从 OSS 直连拉取整包计算（约 1G 下载，耗时视服务器出口带宽），结果入注册表缓存" @click="computeSha">服务端计算</button>
          <span class="shaorigin" :class="param.shaSrc">
            {{ param.shaSrc === 'registry' ? '注册表命中' : param.shaSrc === 'manual' ? '手动填写' : param.shaSrc === 'server' ? '服务端计算' : '' }}
          </span>
        </div>
        <div class="pmeta" :class="shaMeta.cls">{{ shaMeta.text }}</div>
        <div class="pmeta" :class="expiry.cls">{{ expiry.text }}</div>
        <div id="upgParamSummary" class="psummary" v-if="param.ok && param.filename">
          <span>目标版本 <b>{{ param.version || '—' }}</b></span>
          <span>机型 <b>{{ param.arch || '—' }}</b></span>
          <span v-if="param.size != null">大小 <b>{{ fmtBytes(param.size) }}</b></span>
          <span v-if="param.sha256" class="shalink">sha256 <span class="sha" :title="param.sha256">{{ param.sha256.slice(0, 16) }}…</span><button type="button" class="copymini" :data-sha="param.sha256" @click="copySha">复制</button></span>
        </div>
      </div>
    </section>

    <!-- ② 选择目标设备 -->
    <section class="upanel">
      <div class="uhead">
        <h2>② 选择目标设备</h2>
        <div class="uselbar" id="upgSelBar">
          已选 <b>{{ selected.size }}</b> 台 · 当前版本 {{ selBarVer }} → 目标版本 <b>{{ targetVersion || '—' }}</b>
          <button v-if="selected.size" type="button" class="minibtn" @click="clearSel">清空选择</button>
        </div>
      </div>
      <div id="upgDevGroups">
        <p v-if="!devGroups.length" class="uempty">暂无设备数据——设备总览接口不可用或未登记设备</p>
        <div v-for="g in devGroups" :key="g.key" class="vgroup">
          <div class="vghead">
            <input type="checkbox" :checked="g.on.length && g.selN === g.on.length" :disabled="!g.on.length" @change="toggleGroup(g.key, $event.target.checked)">
            <span class="vgt">{{ g.key }}</span>
            <span class="vgn">{{ g.on.length }}/{{ g.rows.length }} 在线{{ g.selN ? ' · 已选 ' + g.selN : '' }}</span>
            <button type="button" class="grpbtn" @click="pickGroup(g.key)">全组升级</button>
          </div>
          <div class="vrows">
            <label v-for="r in g.rows" :key="r.h" class="vrow" :class="{ off: !r.online }" :title="r.online ? '' : '离线设备不可选'">
              <input type="checkbox" :checked="selected.has(r.h)" :disabled="!r.online" @change="toggleDev(r.h, $event.target.checked)">
              <span class="bh">{{ r.h }}</span>
              <small>{{ r.arch || r.hw || '' }}</small>
              <span v-if="param.arch && r.arch && r.arch !== param.arch" class="mism" :title="'机型与目标包（' + param.arch + '）不符'">机型不符</span>
              <span v-if="r.site" class="stag vsite" :style="{ '--c': siteColor(r.site) }">{{ r.site }}</span>
            </label>
          </div>
        </div>
      </div>
    </section>

    <!-- ③ 发起升级 -->
    <section class="upanel" id="upgActPanel">
      <div class="uhead"><h2>③ 发起升级</h2></div>
      <div class="ubtns">
        <button type="button" id="upgRunAll" :disabled="actionState.heavy" @click="actionState.canLaunch && (confirmOpen = true)">▶ 全流程升级所选</button>
        <button v-for="k in ['precheck', 'download', 'install', 'verify', 'cleanup']" :key="k" type="button" class="stagebtn"
                :data-kind="k" :disabled="(k === 'download' || k === 'install') ? actionState.heavy : actionState.light"
                @click="actionState.canLaunch && launch(k)">{{ { precheck: '仅预检', download: '仅下载', install: '仅安装', verify: '仅验证', cleanup: '仅清理' }[k] }}</button>
      </div>
      <p class="uhint" :class="{ warn: actionState.warn }">{{ actionState.text }}</p>
    </section>

    <!-- ④ 任务与进度 -->
    <section class="upanel">
      <div class="uhead">
        <h2>④ 任务与进度</h2>
        <span class="connchip" :class="connState">{{ connState === 'live' ? '● 实时连接正常' : connState === 'poll' ? '○ 实时连接断开 · 轮询中' : '未连接' }}</span>
      </div>
      <div id="upgJobSummary">
        <p v-if="!job" class="uempty">暂无升级任务——在上方勾选设备发起后，这里展示逐台进度与日志</p>
        <template v-else>
          <div v-if="jobOssExpired" class="ossban">⚠ OSS 链接已过期（403），设备下载被拒——请重新生成签名 URL 并新建任务</div>
          <b>#{{ job.id }}</b>
          <span>{{ KIND_LBL[job.kind] || job.kind || '任务' }} → <b>{{ job.version || '—' }}</b></span>
          <span class="tstat" :class="jobStatus">{{ STAT_LBL[jobStatus] || jobStatus }}</span>
          <span>共 <b>{{ jobTotal }}</b> 台</span>
          <span>成功 <b style="color: #15803d">{{ jobCounts.success }}</b></span>
          <span>失败 <b style="color: #b91c1c">{{ jobCounts.failed }}</b></span>
          <span>阻断 <b style="color: #b45309">{{ jobCounts.blocked }}</b></span>
          <span>进行中 <b style="color: #1d4ed8">{{ jobCounts.running }}</b></span>
          <span>等待 <b>{{ jobCounts.pending }}</b></span>
          <span v-if="jobElapsed != null">耗时 <b>{{ fmtDur2(jobElapsed) }}</b></span>
          <span v-if="job.created_at">发起于 {{ fmtClock(parseTs(job.created_at)) }}</span>
          <span v-if="job.oss_url_expires">链接有效期 {{ fmtRemain(job.oss_url_expires * 1000 - Date.now()) }}</span>
        </template>
      </div>
      <div id="upgTaskGrid">
        <p v-if="job && !tasks.length" class="uempty">{{ active ? '任务已创建，等待设备状态上报…' : '' }}</p>
        <article v-for="t in tasks" :key="t.hostname" class="ucard"
                 :class="[t.status === 'success' ? 'done' : t.status === 'failed' ? 'failed' : t.status === 'blocked' ? 'blocked' : '', { expanded: expanded === t.hostname }]"
                 @click="toggleLog(t.hostname)">
          <div class="uh">
            <span class="name">{{ t.hostname }}</span>
            <span v-if="(hosts.fleet.find(x => x.hostname === t.hostname) || {}).site" class="stag"
                  :style="{ '--c': siteColor((hosts.fleet.find(x => x.hostname === t.hostname) || {}).site) }">
              {{ (hosts.fleet.find(x => x.hostname === t.hostname) || {}).site }}
            </span>
            <span class="tstat" :class="t.status">{{ STAT_LBL[t.status] || t.status }}</span>
          </div>
          <div class="uver">版本：{{ t.from_version || fromSnap[t.hostname] || ((hosts.fleet.find(x => x.hostname === t.hostname) || {}).version) || '—' }} → <b>{{ (job && job.version) || targetVersion || '—' }}</b></div>
          <div class="stepper">
            <span v-for="(s, i) in taskSteps(t)" :key="i" class="step" :class="s.cls">{{ s.label }}</span>
          </div>
          <div v-if="(t.status === 'failed' || t.status === 'blocked') && t.error" class="terr" :class="t.status">{{ t.status === 'blocked' ? '阻断：' : '失败：' }}{{ t.error }}</div>
          <div class="ulogwrap">
            <div class="ulogbar">
              <span>实时日志（SSE 增量，点击卡片收起）</span>
              <button type="button" @click.stop="follow[t.hostname] = follow[t.hostname] === false ? true : false; $forceUpdate()">
                {{ follow[t.hostname] === false ? '恢复跟随' : '暂停跟随' }}
              </button>
            </div>
            <pre :ref="el => { if (el) logRefs[t.hostname] = el }" class="ulog">{{ logs[t.hostname] || '' }}</pre>
          </div>
        </article>
      </div>
    </section>

    <!-- 确认弹窗 -->
    <div v-if="confirmOpen" id="upgModal" class="modal show" @click.self="confirmOpen = false">
      <div id="upgModalBody" class="mbody">
        <header>
          <div><span class="name">确认发起全流程升级</span></div>
          <button type="button" class="x" @click="confirmOpen = false">✕</button>
        </header>
        <div class="mgrid">
          <span>目标版本</span><b>{{ targetVersion }}（{{ param.arch || '机型未识别' }} · {{ param.filename || '文件名未识别' }}）</b>
          <span>设备数量</span><b>{{ confirmInfo.n }} 台（{{ confirmInfo.archTxt }}{{ confirmInfo.mism ? '，其中 ' + confirmInfo.mism + ' 台机型不符' : '' }}）</b>
          <span>设备清单</span><b>{{ confirmInfo.preview }}</b>
          <span>链接有效期</span><b>{{ confirmInfo.remTxt }}</b>
          <span>执行流程</span><b>预检 → 下载 → 准备 → 安装 → 验证 → 清理（设备直连 OSS 下载，sha256 基准校验）</b>
        </div>
        <p class="uwarn">⚠ 升级将覆盖设备上的程序，过程不可逆；单台失败不影响其他设备，安装器失败时会自动回滚兜底。请确认目标版本、机型与设备清单无误后再发起。</p>
        <div class="ubtns" style="justify-content: flex-end; margin-top: 16px">
          <button type="button" class="stagebtn" @click="confirmOpen = false">取消</button>
          <button type="button" @click="confirmOpen = false; launch('upgrade')">确认发起升级</button>
        </div>
      </div>
    </div>
  </section>
</template>
