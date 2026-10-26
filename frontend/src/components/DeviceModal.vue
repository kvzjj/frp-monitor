<script setup>
// 设备详情弹窗：360° 信息 + SSH 跳板命令 + webfix 重试 + 采集/快照/事件
import { ref, computed, onMounted, onUnmounted } from 'vue'
import { api, copyText } from '../api'
import { siteColor, pct, fmtDuration, ETAGS } from '../format'
import { hosts, collectDur, webfixState, webfix, loadWebfix } from '../composables/useHosts'

const props = defineProps({ hostname: { type: String, required: true } })
const emit = defineEmits(['close'])

const d = ref(null)
const err = ref('')
const copied = ref(false)
const retry = ref({ arm: false, busy: false, text: '重试补配', cls: '' })

const hint = computed(() => (d.value && d.value.ssh_hint) || hosts.sshHint || {})
const cd = computed(() => (collectDur.map || {})[String((d.value && d.value.hostname) || '').toLowerCase()] || null)
const cdHistory = computed(() => cd.value && Array.isArray(cd.value.history)
  ? cd.value.history.slice().sort((a, b) => String(b.date || '').localeCompare(String(a.date || ''))) : [])
const cmd = computed(() => {
  const dv = d.value
  return dv && dv.ssh_port
    ? `ssh -J ${hint.value.jump || 'ubuntu@124.222.147.63'} -p ${dv.ssh_port} ${hint.value.user || 'orangepi'}@127.0.0.1`
    : ''
})
const wf = computed(() => webfixState(d.value || {}))
const wfExpected = computed(() => {
  const dv = d.value || {}
  const wfc = (webfix.cands || {})[String(dv.hostname || dv.host || '').toLowerCase()] || null
  return (wfc && wfc.web_port) || (wf.value.task && wf.value.task.web_port) || null
})

async function load() {
  err.value = ''
  d.value = null
  try {
    const r = await api('/api/device/' + encodeURIComponent(props.hostname))
    const j = await r.json()
    if (!r.ok) throw new Error(j.error || '读取失败')
    d.value = j
  } catch (e) { err.value = String(e) }
}

async function copyCmd() {
  if (await copyText(cmd.value)) {
    copied.value = true
    setTimeout(() => copied.value = false, 1500)
  } else alert(cmd.value)
}

function resetRetry(ms) {
  setTimeout(() => {
    if (!retry.value.busy) { retry.value = { arm: false, busy: false, text: '重试补配', cls: '' } }
  }, ms)
}

async function onRetry() {
  if (!retry.value.arm) {
    retry.value.arm = true
    retry.value.cls = 'arm'
    retry.value.text = '确认重试？'
    resetRetry(4000)
    return
  }
  const rb = { arm: false, busy: true, text: '发起中…', cls: '' }
  retry.value = rb
  const fail = msg => {
    retry.value = { arm: false, busy: false, text: '未发起：' + msg, cls: 'err' }
    resetRetry(8000)
  }
  try {
    const r = await api('/api/webfix/retry', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ devices: [d.value.hostname] })
    })
    const j = await r.json().catch(() => ({}))
    if (!r.ok || j.ok === false) throw new Error(j.error || ('HTTP ' + r.status))
    const wnorm = x => String((x && (x.hostname || x.host)) || '').toLowerCase()
    const me = String(d.value.hostname || '').toLowerCase()
    const started = (j.started || []).some(x => wnorm(x) === me)
    const skip = (j.skipped || []).find(x => wnorm(x) === me)
    if (!started) throw new Error(String((skip && (skip.reason || skip.error)) || '服务端未确认该设备（响应无 started 记录）'))
    retry.value = { arm: false, busy: false, text: '已发起重试 ✓', cls: '' }
    setTimeout(() => { loadWebfix(); load() }, 900)
  } catch (e) { fail(String(e.message || e)) }
}

function onKey(e) { if (e.key === 'Escape') emit('close') }

onMounted(() => {
  document.addEventListener('keydown', onKey)
  load()
})
onUnmounted(() => document.removeEventListener('keydown', onKey))
</script>

<template>
  <div id="modal" class="modal show" @click.self="emit('close')">
    <div id="modalBody" class="mbody">
      <p v-if="err" class="eempty">{{ err }}</p>
      <p v-else-if="!d" class="eempty">加载中…</p>
      <template v-else>
        <header>
          <div>
            <span class="name"><i class="dot" :class="{ off: !d.online }"></i>{{ d.hostname }}</span>
            <span v-if="d.site" class="stag" :style="{ '--c': siteColor(d.site) }">{{ d.site }}</span>
          </div>
          <button class="x" @click="emit('close')">✕</button>
        </header>

        <div class="mgrid">
          <span>设备名 / device_id</span><b>{{ d.device_name || '—' }} / {{ d.device_id || '—' }}</b>
          <span>归属</span><b>{{ d.owner || d.site || '未登记' }}</b>
          <span>硬件版本</span><b>{{ d.hardware || '—' }}</b>
          <span>型号 / IP</span><b>{{ d.hw_model || '—' }}{{ d.ip ? ' / ' + d.ip : '' }}</b>
          <span>当前状态</span><b>{{ d.online ? '在线' : '未上线（最后在线 ' + (d.last_seen ? d.last_seen : '从未') + '）' }}</b>
          <span>版本</span><b>{{ d.release_version || '未获取' }}</b>
          <span>SSH / Web 端口</span><b>{{ d.ssh_port || '—' }} / {{ d.web_port || '—' }}</b>
          <span>web 代理</span>
          <b>
            {{ d.web_port || '—' }} /
            <span v-if="wf.code === 'normal'" style="color: #16a34a">正常</span>
            <span v-else-if="wf.code === 'running'" style="color: #1d4ed8"><i class="wspin"></i> 补配中</span>
            <span v-else-if="wf.code === 'failed'" style="color: #dc2626" :title="wf.title">失败：{{ wf.title }}</span>
            <span v-else-if="wf.code === 'missing'" style="color: #667085" title="未检测到 web 代理（ep-编号-web），等待自动补配">{{ wfExpected ? '缺配（补配后 ' + wfExpected + '）' : '缺配' }}</span>
            <span v-else style="color: #98a2b3" title="设备未在线或无 SSH 隧道时不判定缺配">—</span>
          </b>
          <span>首次 / 最后在线</span><b>{{ d.first_seen || '—' }} / {{ d.last_seen || '从未' }}</b>
          <span>在线率 24h / 7d</span><b>{{ pct(d.uptime_24h) }} / {{ pct(d.uptime_7d) }}</b>
          <span>采集时长</span><b>{{ fmtDuration(cd ? cd.today_minutes : null) }}</b>
        </div>

        <div v-if="wf.code === 'failed' && d.online && d.ssh_port" class="wfixbar">
          <small>自动补配仅触发一次；重试将重新推送 frpc 配置并重启该机 frpc（隧道秒级闪断）</small>
          <button type="button" :class="retry.cls" :disabled="retry.busy" @click="onRetry">{{ retry.text }}</button>
        </div>

        <div v-if="cmd" class="cmd">
          <code>{{ cmd }}</code>
          <button @click="copyCmd">{{ copied ? '已复制 ✓' : '复制跳板命令' }}</button>
        </div>

        <h3>近 7 天采集时长</h3>
        <div class="twrap">
          <table>
            <tr><th>日期</th><th>时长</th></tr>
            <tr v-for="(r, i) in cdHistory" :key="i"><td>{{ r.date }}</td><td>{{ fmtDuration(r.minutes) }}</td></tr>
            <tr v-if="!cdHistory.length"><td colspan="2" class="eempty">{{ collectDur.enabled ? '暂无采集记录' : '采集时长统计未启用' }}</td></tr>
          </table>
        </div>

        <h3>最近快照（5 分钟一条）</h3>
        <div class="twrap">
          <table>
            <tr><th>时间</th><th>状态</th><th>版本</th><th>连接</th></tr>
            <tr v-for="(s, i) in (d.status || [])" :key="i">
              <td>{{ s.ts }}</td>
              <td><span :style="{ color: s.online ? '#16a34a' : '#dc2626' }">{{ s.online ? '在线' : '离线' }}</span></td>
              <td>{{ s.release_version || '—' }}</td>
              <td>{{ s.connections != null ? s.connections : '—' }}</td>
            </tr>
            <tr v-if="!(d.status || []).length"><td colspan="4" class="eempty">暂无快照</td></tr>
          </table>
        </div>

        <h3>最近事件</h3>
        <div class="elist">
          <div v-for="(e, i) in (d.events || [])" :key="i" class="erow">
            <span class="etag" :class="e.event_type">{{ ETAGS[e.event_type] || e.event_type }}</span>
            <b>{{ e.ts || '' }}</b>
            <span class="edetail">{{ e.new_value || e.detail || '' }}{{ e.old_value && e.new_value ? ' ← ' + e.old_value : '' }}</span>
          </div>
          <p v-if="!(d.events || []).length" class="eempty">暂无事件</p>
        </div>
      </template>
    </div>
  </div>
</template>
