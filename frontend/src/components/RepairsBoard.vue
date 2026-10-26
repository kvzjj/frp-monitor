<script setup>
// 维修流转看板（KVWO-80）：看板/列表双视图 + 阶段统计 + 筛选 + 详情弹窗（附件预览）+ 设备总览定位
// 数据契约同后端 KVWO-79；404/拉取失败时明确提示，不渲染陈旧数据冒充实时。自原 Rep.* 忠实平移。
import { ref, reactive, computed, onMounted, onUnmounted, watch } from 'vue'
import { api } from '../api'
import { fmtClock, fmtRel, parseTs } from '../format'
import { setView } from '../router'
import { filters } from '../composables/useHosts'

const STAGES = ['待接收', '损坏原因确认中', '维修中', '待发出确认', '待发出', '已发出', '已完结']
const SC = { '待接收': '#dc2626', '损坏原因确认中': '#d97706', '维修中': '#ea580c', '待发出确认': '#7c3aed', '待发出': '#4f46e5', '已发出': '#0891b2', '已完结': '#64748b' }

const KEYS = {
  device_id: ['device_id', 'device_no', 'device_code'],
  device_type: ['device_type', 'type'],
  accessories: ['accessories', 'parts'],
  unit: ['unit', 'unit_name', 'org'],
  stage: ['stage', 'phase'],
  problem: ['problem', 'description', 'issue'],
  name: ['name', 'contact_name'],
  contact: ['contact', 'phone', 'mobile', 'contact_info', 'contact_phone'],
  receive_address: ['receive_address', 'address', 'recv_address'],
  return_address: ['return_address', 'return_info', 'expected_return'],
  tracking_no: ['tracking_no', 'express_no', 'tracking_number', 'tracking_no_in'],
  outbound_tracking_no: ['outbound_tracking_no', 'send_tracking_no', 'outbound_express_no', 'tracking_no_out'],
  owner: ['owner', 'repair_owner', 'maintainer', 'assignee', 'owner_name'],
  last_followup_at: ['last_followup_at', 'last_follow_time', 'followup_at', 'last_followup'],
  last_followup_note: ['last_followup_note', 'last_follow_desc', 'followup_note', 'last_note'],
  attachments: ['attachments', 'files'],
  created_at: ['created_at', 'create_time', 'created'],
  record_id: ['record_id', 'id'],
  feishu_url: ['feishu_url', 'feishu_link', 'record_url'],
  device_online: ['device_online', 'online']
}

const st = reactive({
  view: 'kanban', loading: false, rows: [], all: [],
  stats: null, statsErr: '', listErr: '',
  q: '', stageF: '', typeF: '', unitF: '', ownerF: '', doneOpen: false
})
const detail = ref(null)   // { o, degraded }
let timer = null, qT = null

const props = defineProps({ initialQ: { type: String, default: '' } })

function pick(r, ks) { for (const k of ks) { const v = r ? r[k] : undefined; if (v !== undefined && v !== null && v !== '') return v } return null }

function apiErr(r, d) {
  if (r && r.status === 404) return '接口不可用（404）——请确认服务为含 /api/repairs（KVWO-79）的版本'
  return (d && d.error) ? d.error : ('HTTP ' + (r ? r.status : '?'))
}

function norm(r) {
  const o = { stage: '' }
  for (const k in KEYS) o[k] = pick(r, KEYS[k])
  o.stage = o.stage == null ? '' : String(o.stage)
  o.stageIdx = STAGES.indexOf(o.stage)
  o.done = o.stage === '已完结'
  o.accessories = Array.isArray(o.accessories) ? o.accessories.map(String) : (o.accessories ? String(o.accessories).split(/[,、]/).map(s => s.trim()).filter(Boolean) : [])
  const atts = Array.isArray(o.attachments) ? o.attachments.slice() : (o.attachments ? [o.attachments] : [])
  o.attachments = atts.map(a => {
    if (a == null) return null
    if (typeof a === 'string') return { name: '', url: a }
    const u = String((a.url || a.link || a.src || a.path) || '')
    const nm = String((a.name || a.filename || a.title) || (u ? u.split('?')[0].split('/').pop() : '') || '附件')
    return { name: nm, url: u }
  }).filter(a => a && a.url)
  const dv = o.device_online
  o.online = (dv === true || dv === 1 || dv === 'true' || dv === 'True') ? true : ((dv === false || dv === 0 || dv === 'false' || dv === 'False') ? false : null)
  o.followMs = parseTs(o.last_followup_at)
  o.createdMs = parseTs(o.created_at)
  return o
}

function normList(d) {
  const l = Array.isArray(d) ? d : (d && (d.repairs || d.items || d.records || d.list))
  return Array.isArray(l) ? l : []
}

function hasFilter() { return !!(st.q || st.stageF || st.typeF || st.unitF || st.ownerF) }

function params() {
  const p = new URLSearchParams()
  if (st.view === 'list' && st.stageF) p.set('stage', st.stageF)
  if (st.typeF) p.set('type', st.typeF)
  if (st.unitF) p.set('unit', st.unitF)
  if (st.ownerF) p.set('owner', st.ownerF)
  if (st.q) p.set('q', st.q)
  const s = p.toString()
  return s ? '?' + s : ''
}

async function loadStats() {
  try {
    const r = await api('/api/repairs/stats')
    const d = await r.json().catch(() => ({}))
    if (!r.ok) throw new Error(apiErr(r, d))
    st.stats = { total: d.total, active_count: d.active_count, byStage: d.by_stage || d.byStage || {}, syncMs: parseTs(d.last_sync_at || d.lastSyncAt), syncError: d.sync_error || d.last_error || d.last_sync_error || '' }
    st.statsErr = ''
  } catch (e) {
    st.statsErr = '维修统计不可用：' + ((e && e.message) ? e.message : String(e)) + (st.stats && st.stats.syncMs ? '（最后成功同步 ' + fmtClock(st.stats.syncMs) + '，以下统计为当时数据）' : '')
  }
}

async function loadList() {
  st.loading = true
  try {
    const r = await api('/api/repairs' + params())
    const d = await r.json().catch(() => ({}))
    if (!r.ok) throw new Error(apiErr(r, d))
    st.rows = normList(d).map(norm)
    if (!params()) { st.all = st.rows; buildOptions() }
    st.listErr = ''
  } catch (e) { st.listErr = '维修数据加载失败：' + ((e && e.message) ? e.message : String(e)); st.rows = [] }
  st.loading = false
}

function load() { loadStats(); loadList() }

/* 统计卡 */
const num = v => (v == null || v === '' || isNaN(Number(v))) ? null : Number(v)
const statCards = computed(() => ({
  active: st.stats ? num(st.stats.active_count) : null,
  total: st.stats ? num(st.stats.total) : null,
  done: st.stats ? num((st.stats.byStage || {})['已完结']) : null
}))

const bannerErrs = computed(() => {
  const errs = []
  if (st.statsErr) errs.push(st.statsErr)
  if (st.listErr) errs.push(st.listErr)
  if (!errs.length && st.stats && st.stats.syncError) {
    errs.push('后端最近一次飞书同步失败：' + st.stats.syncError + (st.stats.syncMs ? '（最后成功同步 ' + fmtClock(st.stats.syncMs) + '）' : '（尚无成功同步）'))
  }
  return errs
})

/* 看板分组（未知阶段追加在尾部） */
const kanbanCols = computed(() => {
  const cols = STAGES.slice()
  const seen = new Set(STAGES)
  for (const r of st.rows) {
    const s = r.stage || '未填写阶段'
    if (!seen.has(s)) { seen.add(s); cols.push(s) }
  }
  const groups = new Map()
  for (const s of cols) groups.set(s, [])
  for (const r of st.rows) groups.get(r.stage || '未填写阶段').push(r)
  return cols.map(s => ({
    stage: s, color: SC[s] || '#64748b',
    list: (groups.get(s) || []).slice().sort((a, b) => (b.followMs || 0) - (a.followMs || 0))
  }))
})

const listActive = computed(() => {
  const act = st.rows.filter(r => !r.done).sort((a, b) => (a.stageIdx - b.stageIdx) || ((b.followMs || 0) - (a.followMs || 0)))
  return act
})
const listDone = computed(() => st.rows.filter(r => r.done).sort((a, b) => (b.followMs || 0) - (a.followMs || 0)))
const listMsg = computed(() => {
  if (st.listErr) return '维修数据不可用，详见上方提示'
  if (!st.rows.length) return hasFilter() ? '没有匹配的维修记录——调整筛选或搜索词试试' : '暂无维修记录——飞书维修表同步到数据后此处展示'
  if (!listActive.value.length) return '匹配记录均为已完结，见下方折叠区'
  return ''
})

/* 筛选选项（由全量列表构建） */
const optUnits = ref([])
const optOwners = ref([])
const optTypes = ref([])

function buildOptions() {
  const cnt = key => {
    const m = new Map()
    for (const r of st.all) { const v = r[key]; if (v) m.set(v, (m.get(v) || 0) + 1) }
    return [...m.entries()].sort((a, b) => a[0].localeCompare(b[0], 'zh'))
  }
  optUnits.value = cnt('unit')
  optOwners.value = cnt('owner')
  const tmap = new Map([['单目', 0], ['双目', 0], ['其他', 0]])
  for (const r of st.all) { if (r.device_type) tmap.set(r.device_type, (tmap.get(r.device_type) || 0) + 1) }
  optTypes.value = [...tmap.entries()]
}

/* 详情弹窗 */
const IMG_EXT = ['.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp']
function isImg(url) { return IMG_EXT.some(e => String(url).toLowerCase().split('?')[0].endsWith(e)) }

async function openDetail(rid) {
  rid = rid == null ? '' : String(rid)
  const cached = st.rows.find(r => String(r.record_id) === rid) || st.all.find(r => String(r.record_id) === rid) || null
  if (!cached && !rid) { detail.value = { error: '记录缺少 record_id，无法加载详情' }; return }
  if (!rid) { detail.value = { o: cached, degraded: true }; return }
  detail.value = { loading: true }
  try {
    const r = await api('/api/repairs/' + encodeURIComponent(rid))
    const d = await r.json().catch(() => ({}))
    if (!r.ok) throw new Error(apiErr(r, d))
    const raw = Array.isArray(d) ? d[0] : (d && (d.record || d.repair || d.item) || d)
    detail.value = { o: norm((raw && typeof raw === 'object') ? raw : {}), degraded: false }
  } catch (e) {
    if (cached) detail.value = { o: cached, degraded: true }
    else detail.value = { error: '详情加载失败：' + ((e && e.message) ? e.message : String(e)) }
  }
}

/* 设备总览定位 */
function jumpDevice(did) {
  if (!did) return
  setView('overview')
  filters.q = String(did).toLowerCase()
  filters.site = ''; filters.ver = ''; filters.pendingOnly = false; filters.status = 'all'
  detail.value = null
  setTimeout(() => document.getElementById('q')?.scrollIntoView({ behavior: 'smooth', block: 'center' }), 50)
}

function stageChipClick(stg) {
  if (st.view === 'list') { st.stageF = st.stageF === stg ? '' : stg; loadList() }
}

watch(() => props.initialQ, q => { if (q) { st.q = q; loadList() } }, { immediate: true })

let searchBox = ref('')
watch(searchBox, v => {
  clearTimeout(qT)
  qT = setTimeout(() => { st.q = v.trim(); loadList() }, 350)
})

onMounted(() => {
  load()
  timer = setInterval(load, 60000)
})
onUnmounted(() => clearInterval(timer))
</script>

<template>
  <div>
    <section class="summary">
      <div><b>{{ statCards.active != null ? statCards.active : '—' }}</b><span>未完结维修</span></div>
      <div><b>{{ statCards.total != null ? statCards.total : '—' }}</b><span>维修记录</span></div>
      <div><b>{{ statCards.done != null ? statCards.done : '—' }}</b><span>已完结</span></div>
      <div><b :title="st.stats && st.stats.syncMs ? '最后成功同步：' + fmtClock(st.stats.syncMs) : (st.stats ? '后端尚未完成过一次成功同步' : '')">
        {{ st.stats && st.stats.syncMs ? fmtRel(st.stats.syncMs) : (st.stats ? '未同步' : '—') }}</b><span>最后同步</span></div>
    </section>

    <div v-if="bannerErrs.length" id="repBanner" class="rbanner">
      <div v-for="(e, i) in bannerErrs" :key="i">{{ e }}</div>
      <div>当前未展示实时数据；后端同步与接口恢复后，点「重新加载」获取最新。</div>
    </div>

    <nav id="repStageStats" class="sitesbar">
      <span v-for="s in STAGES" :key="s" class="chip rchip" :style="{ '--c': SC[s] }" :title="st.view === 'list' ? '点击按阶段筛选列表' : ''" @click="stageChipClick(s)">
        {{ s }} <b>{{ st.stats && st.stats.byStage ? (num(st.stats.byStage[s]) != null ? num(st.stats.byStage[s]) : '—') : '—' }}</b>
      </span>
    </nav>

    <div class="toolbar">
      <input v-model="searchBox" placeholder="搜索设备编号 / 问题描述" spellcheck="false" autocomplete="off">
      <select v-show="st.view === 'list'" v-model="st.stageF" title="按流转阶段筛选（仅列表视图）" @change="loadList">
        <option value="">全部阶段</option>
        <option v-for="s in STAGES" :key="s" :value="s">{{ s }}</option>
      </select>
      <select v-model="st.typeF" @change="loadList">
        <option value="">全部类型</option>
        <option v-for="[k, n] in optTypes" :key="k" :value="k">{{ k }}（{{ n }}）</option>
      </select>
      <select v-model="st.unitF" @change="loadList">
        <option value="">全部单位</option>
        <option v-for="[k, n] in optUnits" :key="k" :value="k">{{ k }}（{{ n }}）</option>
      </select>
      <select v-model="st.ownerF" @change="loadList">
        <option value="">全部负责人</option>
        <option v-for="[k, n] in optOwners" :key="k" :value="k">{{ k }}（{{ n }}）</option>
      </select>
      <button class="listbtn" title="重新拉取维修列表与统计（后端默认每 5 分钟从飞书表同步一次）" @click="load">重新加载</button>
      <span class="rvswitch">
        <button type="button" :class="{ on: st.view === 'kanban' }" @click="st.view = 'kanban'">看板</button>
        <button type="button" :class="{ on: st.view === 'list' }" @click="st.view = 'list'; loadList()">列表</button>
      </span>
    </div>

    <!-- 看板视图 -->
    <div v-show="st.view === 'kanban'" id="repKanban" class="rkan">
      <p v-if="st.listErr" class="uempty">维修数据不可用，详见上方提示</p>
      <p v-else-if="st.loading && !st.rows.length" class="uempty rkanhint">加载中…</p>
      <p v-else-if="!st.rows.length && hasFilter()" class="uempty rkanhint">没有匹配的维修记录——调整筛选或搜索词，或点「重新加载」</p>
      <p v-else-if="!st.rows.length" class="uempty rkanhint">暂无维修记录——飞书维修表同步到数据后此处展示</p>
      <div v-for="col in kanbanCols" :key="col.stage" class="rkcol" :data-col="col.stage">
        <h3><span class="rkt" :style="{ '--c': col.color }">{{ col.stage }}</span><span class="rkn">{{ col.list.length }}</span></h3>
        <div v-for="r in col.list" :key="r.record_id" class="rcard" @click="openDetail(r.record_id)">
          <div class="rd">{{ r.device_id || '（无编号）' }}<span v-if="r.device_type" class="rtyp" :class="r.device_type === '双目' ? 'rt2' : (r.device_type === '单目' ? 'rt1' : '')">{{ r.device_type }}</span></div>
          <div v-if="r.problem" class="rprob" :title="r.problem">{{ r.problem }}</div>
          <div class="rmeta">
            <span>{{ r.owner || '负责人 —' }}</span>
            <span :title="r.followMs ? fmtClock(r.followMs) : ''">{{ r.followMs ? fmtRel(r.followMs) : '跟进 —' }}</span>
          </div>
          <div class="ronrow">
            <span v-if="r.online === true" class="ron on link" title="设备台账当前在线，点击跳到设备总览定位" @click.stop="jumpDevice(r.device_id)">台账在线</span>
            <span v-else-if="r.online === false" class="ron off link" title="设备台账当前离线，点击跳到设备总览定位" @click.stop="jumpDevice(r.device_id)">台账离线</span>
            <span v-else class="ron na" title="设备编号未匹配到设备台账">台账 —</span>
          </div>
        </div>
        <div v-if="!col.list.length" class="rkempty">暂无</div>
      </div>
    </div>

    <!-- 列表视图 -->
    <div v-show="st.view === 'list'" id="repListWrap">
      <div class="twrap">
        <table class="rtable">
          <thead><tr><th>设备编号</th><th>类型</th><th>阶段</th><th>所属单位</th><th>问题描述</th><th>负责人</th><th>最后跟进</th><th>台账</th></tr></thead>
          <tbody>
            <tr v-for="r in listActive" :key="r.record_id" @click="openDetail(r.record_id)">
              <td class="rdev">{{ r.device_id || '—' }}</td>
              <td>{{ r.device_type || '—' }}</td>
              <td><span class="stag" :style="{ '--c': SC[r.stage] || '#64748b' }">{{ r.stage || '—' }}</span></td>
              <td>{{ r.unit || '—' }}</td>
              <td class="rprob-cell" :title="r.problem || ''">{{ r.problem || '—' }}</td>
              <td>{{ r.owner || '—' }}</td>
              <td :title="r.followMs ? fmtClock(r.followMs) : ''">{{ r.followMs ? fmtRel(r.followMs) : '—' }}</td>
              <td>
                <span v-if="r.online === true" class="ron on link" title="设备台账当前在线，点击跳到设备总览定位" @click.stop="jumpDevice(r.device_id)">在线</span>
                <span v-else-if="r.online === false" class="ron off link" title="设备台账当前离线，点击跳到设备总览定位" @click.stop="jumpDevice(r.device_id)">离线</span>
                <span v-else class="ron na" title="设备编号未匹配到设备台账">—</span>
              </td>
            </tr>
            <tr v-if="listMsg"><td colspan="8" class="rempty">{{ listMsg }}</td></tr>
          </tbody>
        </table>
      </div>
      <div v-if="listDone.length" id="repDoneWrap" class="rdonewrap">
        <button type="button" id="repDoneToggle" class="listbtn" @click="st.doneOpen = !st.doneOpen">已完结记录（{{ listDone.length }}）{{ st.doneOpen ? ' 收起 ▴' : ' 展开 ▾' }}</button>
        <div v-show="st.doneOpen" class="twrap">
          <table class="rtable">
            <thead><tr><th>设备编号</th><th>类型</th><th>阶段</th><th>所属单位</th><th>问题描述</th><th>负责人</th><th>最后跟进</th><th>台账</th></tr></thead>
            <tbody>
              <tr v-for="r in listDone" :key="r.record_id" @click="openDetail(r.record_id)">
                <td class="rdev">{{ r.device_id || '—' }}</td>
                <td>{{ r.device_type || '—' }}</td>
                <td><span class="stag" :style="{ '--c': SC[r.stage] || '#64748b' }">{{ r.stage || '—' }}</span></td>
                <td>{{ r.unit || '—' }}</td>
                <td class="rprob-cell">{{ r.problem || '—' }}</td>
                <td>{{ r.owner || '—' }}</td>
                <td>{{ r.followMs ? fmtRel(r.followMs) : '—' }}</td>
                <td><span class="ron na">—</span></td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- 详情弹窗 -->
    <div v-if="detail" id="repModal" class="modal show" @click.self="detail = null">
      <div id="repModalBody" class="mbody">
        <p v-if="detail.loading" class="eempty">加载中…</p>
        <p v-else-if="detail.error" class="eempty">{{ detail.error }}</p>
        <template v-else-if="detail.o">
          <header>
            <div>
              <span class="name">{{ detail.o.device_id || '（无设备编号）' }}</span>
              <span class="stag" :style="{ '--c': SC[detail.o.stage] || '#64748b' }">{{ detail.o.stage || '阶段 —' }}</span>
            </div>
            <button class="x" @click="detail = null">✕</button>
          </header>
          <p v-if="detail.degraded" class="uhint warn">详情接口不可用，以下为列表缓存数据（附件可能缺失）</p>
          <div class="mgrid">
            <span>设备编号</span><b>{{ detail.o.device_id || '—' }}</b>
            <span>设备所属类型</span><b>{{ detail.o.device_type || '—' }}</b>
            <span>所含配件</span><b>{{ detail.o.accessories && detail.o.accessories.length ? detail.o.accessories.join('、') : '—' }}</b>
            <span>所属单位</span><b>{{ detail.o.unit || '—' }}</b>
            <span>当前阶段</span><b>{{ detail.o.stage || '—' }}</b>
            <span>问题描述</span><b style="white-space: pre-wrap">{{ detail.o.problem || '—' }}</b>
            <span>姓名</span><b>{{ detail.o.name || '—' }}</b>
            <span>联系方式</span><b>{{ detail.o.contact || '—' }}</b>
            <span>收件地址</span><b>{{ detail.o.receive_address || '—' }}</b>
            <span>期望寄回地址和联系方式</span><b>{{ detail.o.return_address || '—' }}</b>
            <span>快递单号</span><b>{{ detail.o.tracking_no || '—' }}</b>
            <span>发出快递单号</span><b>{{ detail.o.outbound_tracking_no || '—' }}</b>
            <span>维修负责人</span><b>{{ detail.o.owner || '—' }}</b>
            <span>最后跟进时间</span><b>{{ detail.o.followMs ? fmtClock(detail.o.followMs) + '（' + fmtRel(detail.o.followMs) + '）' : '—' }}</b>
            <span>最后跟进说明</span><b style="white-space: pre-wrap">{{ detail.o.last_followup_note || '—' }}</b>
            <span>创建时间</span><b>{{ detail.o.createdMs ? fmtClock(detail.o.createdMs) : '—' }}</b>
            <span>台账在线状态</span>
            <b>
              <span v-if="detail.o.online === true" class="ron on">在线</span>
              <span v-else-if="detail.o.online === false" class="ron off">离线</span>
              <span v-else class="ron na">—</span>
            </b>
          </div>
          <h3>附件（{{ detail.o.attachments.length }}）</h3>
          <div v-if="detail.o.attachments.length" class="rattach">
            <a v-for="(a, i) in detail.o.attachments" :key="i" :href="a.url" target="_blank" rel="noopener" :title="a.name || a.url">
              <img v-if="isImg(a.url)" :src="a.url" loading="lazy" :alt="a.name" @error="$event.target.style.display = 'none'">
              <span class="rfname">{{ a.name || '附件' }}</span>
            </a>
          </div>
          <p v-else class="eempty">无附件</p>
          <div class="rfootbar">
            <a v-if="detail.o.feishu_url" class="rfeishu" :href="detail.o.feishu_url" target="_blank" rel="noopener">查看飞书原记录 ↗</a>
            <span v-else class="eempty">飞书原记录链接：—</span>
            <button v-if="detail.o.online !== null && detail.o.device_id" type="button" @click="jumpDevice(detail.o.device_id)">在设备总览中定位 {{ detail.o.device_id }}</button>
          </div>
        </template>
      </div>
    </div>
  </div>
</template>
