<script setup>
// 设备总览：汇总卡 / 工具栏（搜索·状态·目标版本·复制） / 站点·版本 chips / 设备网格 / 事件
import { ref, computed } from 'vue'
import { copyText } from '../api'
import { siteColor, fmtHours } from '../format'
import {
  hosts, filters, filtered, pendingCount, missingCount, unregisteredCount,
  webfixState, collectDur
} from '../composables/useHosts'
import DeviceModal from '../components/DeviceModal.vue'
import EventsPanel from '../components/EventsPanel.vue'

const modalHost = ref('')

const pendingLabel = computed(() => filters.targetVer ? `待升级（≠ ${filters.targetVer}）` : '待升级')

function toggleSite(s) {
  filters.site = filters.site === s ? '' : s
  if (filters.site && filters.status === 'online') filters.status = 'all'
}
function toggleVer(v) {
  if (v === '__pending__') { filters.pendingOnly = !filters.pendingOnly; if (filters.pendingOnly) filters.ver = '' }
  else { filters.ver = filters.ver === v ? '' : v; filters.pendingOnly = false }
}

function flashBtn(ev, label, okLabel, ms = 1800) {
  const btn = ev && ev.target
  if (!btn) return
  btn.textContent = okLabel
  setTimeout(() => { btn.textContent = label }, ms)
}

async function copyPending(ev) {
  const rows = filtered.value.filter(f => !f.retired)
  if (!rows.length) return flashBtn(ev, '复制待升级清单', '无匹配设备', 1200)
  const head = ['设备号', '站点', 'IP', 'SSH端口', 'device_id', '当前版本', '硬件']
  const text = [head.join('\t')]
    .concat(rows.map(f => [f.hostname.toUpperCase(), f.site || '未登记', f.ip || '', f.ssh_port || '', f.device_id || '', f.version || '未获取', [f.hardware, f.hw_model].filter(Boolean).join(' ') || ''].join('\t')))
    .join('\n')
  if (await copyText(text)) flashBtn(ev, '复制待升级清单', `已复制 ${rows.length} 台 ✓`)
  else alert(text)
}

async function copyCmds(ev) {
  const hint = hosts.sshHint || {}, jump = hint.jump || 'ubuntu@124.222.147.63', user = hint.user || 'orangepi'
  const rows = filtered.value.filter(f => !f.retired && f.ssh_port)
  if (!rows.length) return flashBtn(ev, '复制跳板命令', '无匹配设备', 1200)
  const text = rows.map(f => `ssh -J ${jump} -p ${f.ssh_port} ${f.ssh_user || user}@127.0.0.1  # ${f.hostname.toUpperCase()}${f.site ? ' (' + f.site + ')' : ''}`).join('\n')
  if (await copyText(text)) flashBtn(ev, '复制跳板命令', `已复制 ${rows.length} 台 ✓`)
  else alert(text)
}

async function copyList(ev) {
  const list = filtered.value.map(f => f.hostname.toUpperCase())
  if (!list.length) return flashBtn(ev, '复制编号', '无匹配设备', 1200)
  if (await copyText(list.join('\n'))) flashBtn(ev, '复制编号', `已复制 ${list.length} 台 ✓`)
  else alert(list.join('\n'))
}
</script>

<template>
  <section id="viewOverview">
    <section class="summary">
      <div><b>{{ hosts.online }}</b><span>在线设备</span></div>
      <div><b>{{ hosts.registered }}</b><span>登记设备</span></div>
      <div><b>{{ pendingCount }}</b><span>{{ pendingLabel }}</span></div>
      <div><b>{{ hosts.sites.length }}</b><span>站点</span></div>
      <div><b>{{ missingCount }}</b><span>未上线</span></div>
    </section>

    <div class="toolbar">
      <input id="q" v-model="filters.q" placeholder="搜索 hostname / device_id / 设备名 / IP">
      <select id="statusFilter" v-model="filters.status">
        <option value="all">全部状态</option>
        <option value="online">仅在线</option>
        <option value="offline">仅未上线</option>
      </select>
      <select id="targetSel" v-model="filters.targetVer" title="升级目标版本">
        <option v-for="v in hosts.versions" :key="v.version" :value="v.version">{{ v.version }}</option>
      </select>
      <button id="copyPending" class="listbtn" title="按当前筛选（站点/状态/版本/搜索）复制：设备号 站点 IP SSH端口 device_id 当前版本" @click="copyPending">复制待升级清单</button>
      <button id="copyCmds" class="listbtn" title="按当前筛选复制 SSH 跳板登录命令" @click="copyCmds">复制跳板命令</button>
      <button id="copyList" class="listbtn" title="按当前筛选复制设备编号" @click="copyList">复制编号</button>
    </div>

    <nav id="sites" class="sitesbar">
      <button class="chip" :class="{ on: filters.site === '' }" @click="toggleSite('')">全部 {{ hosts.fleet.length }}</button>
      <button v-for="s in hosts.sites" :key="s.site" class="chip" :class="{ on: filters.site === s.site }"
              :style="{ '--c': siteColor(s.site) }" @click="toggleSite(s.site)">
        {{ s.site }} <b>{{ s.online }}/{{ s.expected }}</b>
      </button>
      <button v-if="unregisteredCount" class="chip" :class="{ on: filters.site === '未登记' }"
              style="--c: #64748b" @click="toggleSite('未登记')">未登记 <b>{{ unregisteredCount }}</b></button>
    </nav>

    <nav id="vers" class="sitesbar">
      <button class="chip" :class="{ on: filters.ver === '' && !filters.pendingOnly }" @click="toggleVer('')">全部 {{ hosts.fleet.length }}</button>
      <button v-for="v in hosts.versions" :key="v.version" class="chip" :class="{ on: filters.ver === v.version }"
              :style="{ '--c': v.version === '未获取' ? '#64748b' : '#0f766e' }" @click="toggleVer(v.version)">
        {{ v.version }} <b>{{ v.online !== v.count ? v.online + '/' + v.count : String(v.count) }}</b>
      </button>
      <button v-if="filters.targetVer" class="chip" :class="{ on: filters.pendingOnly }" style="--c: #b45309"
              title="只看待升级设备（≠ 目标版本）" @click="toggleVer('__pending__')">≠ {{ filters.targetVer }} <b>{{ pendingCount }}</b></button>
    </nav>

    <div id="grid" class="grid">
      <template v-if="filtered.length">
        <article v-for="f in filtered" :key="f.hostname"
                 class="card" :class="f.online ? 'online' : 'offline'" @click="modalHost = f.hostname">
          <div class="top">
            <span class="name"><i class="dot" :class="{ off: !f.online }"></i>{{ f.hostname }}</span>
            <span v-if="f.site" class="stag" :style="{ '--c': siteColor(f.site) }">{{ f.site }}</span>
            <span v-if="webfixState(f).code === 'running'" class="wfix running" title="web 代理补配进行中…"><i class="wspin"></i>补配中…</span>
            <span v-else-if="webfixState(f).code === 'failed'" class="wfix failed" :title="webfixState(f).title">补配失败</span>
            <span v-else-if="webfixState(f).code === 'missing'" class="wfix" title="未检测到 web 代理（ep-编号-web），等待自动补配">web 缺配</span>
          </div>
          <template v-if="f.online">
            <div class="row"><span>版本</span><span class="value">{{ f.version || '未获取' }}<small v-if="f.version_error" :title="f.version_error"> (读取失败)</small></span></div>
            <div class="row"><span>硬件</span><span class="value">{{ f.hardware || '—' }}{{ f.hw_model ? ' · ' + f.hw_model : '' }}</span></div>
            <div class="row"><span>IP</span><span class="value">{{ f.ip || '—' }}</span></div>
            <div class="row"><span>SSH / Web</span><span class="value">{{ f.ssh_port || '—' }} / {{ f.web_port || '—' }}</span></div>
            <div class="row"><span>device_id</span><span class="value">{{ f.device_id || '—' }}</span></div>
            <div class="row"><span>连接数</span><span class="value">{{ f.connections != null ? f.connections : '—' }}</span></div>
            <div v-if="collectDur.enabled" class="row"><span>今日采集</span><span class="value">{{ fmtHours((collectDur.map[(f.hostname || '').toLowerCase()] || {}).today_minutes) }}</span></div>
            <div class="row"><small>frpc 上线：{{ f.started || '—' }}</small></div>
          </template>
          <template v-else>
            <div class="row"><span>状态</span><span class="value bad">未上线</span></div>
            <div class="row"><span>最后在线</span><span class="value">{{ f.last_seen ? f.last_seen : '从未连接' }}</span></div>
            <div class="row"><span>硬件</span><span class="value">{{ f.hardware || '—' }}{{ f.hw_model ? ' · ' + f.hw_model : '' }}</span></div>
            <div class="row"><span>IP</span><span class="value">{{ f.ip || '—' }}</span></div>
            <div class="row"><span>device_id</span><span class="value">{{ f.device_id || '—' }}</span></div>
            <div class="row"><span>历史版本</span><span class="value">{{ f.version || '—' }}</span></div>
          </template>
        </article>
      </template>
      <div v-else class="empty">没有匹配的设备</div>
    </div>

    <section id="events">
      <h2>最近事件（24 小时）</h2>
      <EventsPanel />
    </section>

    <DeviceModal v-if="modalHost" :hostname="modalHost" @close="modalHost = ''" />
  </section>
</template>
