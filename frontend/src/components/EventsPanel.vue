<script setup>
// 最近事件面板（24h，60s 轮询）
import { ref, onMounted, onUnmounted } from 'vue'
import { api } from '../api'
import { ETAGS } from '../format'

const events = ref(null)
const state = ref('加载中…')

async function load() {
  try {
    const r = await api('/api/events?hours=24&limit=60')
    const d = await r.json()
    if (!d.enabled) { state.value = '历史持久化未启用（数据库不可用）'; return }
    events.value = d.events || []
    state.value = ''
  } catch (e) { state.value = '事件读取失败' }
}

let timer = null
onMounted(() => { load(); timer = setInterval(load, 60000) })
onUnmounted(() => clearInterval(timer))
</script>

<template>
  <div id="eventsBody" class="elist">
    <p v-if="state" class="eempty">{{ state }}</p>
    <p v-else-if="!events.length" class="eempty">24 小时内暂无事件</p>
    <div v-for="(e, i) in events" :key="i" v-else class="erow">
      <span class="etag" :class="e.event_type">{{ ETAGS[e.event_type] || e.event_type }}</span>
      <b>{{ e.hostname }}</b>
      <span class="edetail">{{ e.new_value || e.detail || '' }}{{ e.old_value && e.new_value ? ' ← ' + e.old_value : '' }}</span>
      <time>{{ e.ts }}</time>
    </div>
  </div>
</template>
