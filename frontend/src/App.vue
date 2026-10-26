<script setup>
import { ref, onMounted, onUnmounted, watch } from 'vue'
import { api } from './api'
import { startPolling, stopPolling, loadHosts, hosts } from './composables/useHosts'
import { view, setView, readHashView } from './router'
import OverviewView from './views/OverviewView.vue'
import AssetsView from './views/AssetsView.vue'
import UpgradeView from './views/UpgradeView.vue'

const title = ref('设备面板')
const sub = ref('正在加载…')

const VIEWS = { overview: OverviewView, assets: AssetsView, upgrade: UpgradeView }

function onHashChange() { setView(readHashView(), true) }

onMounted(async () => {
  window.addEventListener('hashchange', onHashChange)
  setView(readHashView(), true)
  startPolling()
  // 面板标题（后端 lark 解析；登录页静态注入之外 SPA 动态跟随）
  try {
    const r = await api('/api/panel-title')
    if (r.ok) {
      const d = await r.json()
      if (d.title) {
        title.value = d.title
        document.title = d.title
      }
    }
  } catch (e) { /* 标题接口不可用则保持默认 */ }
})

onUnmounted(() => {
  window.removeEventListener('hashchange', onHashChange)
  stopPolling()
})

watch(() => hosts.sub, v => { sub.value = v }, { immediate: true })
</script>

<template>
  <main>
    <header>
      <div>
        <h1>{{ title }}</h1>
        <p id="sub">{{ sub }}</p>
      </div>
      <div class="hdrbtns">
        <button id="refresh" @click="loadHosts(true)">立即刷新</button>
        <a class="logout" href="/logout" title="退出登录并清除会话">退出</a>
      </div>
    </header>
    <nav id="viewTabs" class="vtabs">
      <button type="button" :class="{ on: view === 'overview' }" @click="setView('overview')">设备总览</button>
      <button type="button" :class="{ on: view === 'assets' }" @click="setView('assets')">资产</button>
      <button type="button" :class="{ on: view === 'upgrade' }" @click="setView('upgrade')">升级</button>
    </nav>
    <KeepAlive>
      <component :is="VIEWS[view]" />
    </KeepAlive>
  </main>
</template>
