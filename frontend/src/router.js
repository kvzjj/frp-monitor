// 极简 hash 路由（#view=overview|assets|upgrade，#atab=/q=/unit= 由资产视图自解析）
import { ref } from 'vue'

export const view = ref('overview')

export function setView(v, skipHash = false) {
  if (v !== 'upgrade' && v !== 'assets') v = 'overview'
  view.value = v
  if (!skipHash) {
    const p = new URLSearchParams(location.hash.slice(1))
    if (v !== 'overview') {
      p.set('view', v)
    } else {
      p.delete('view'); p.delete('atab'); p.delete('unit')
    }
    const s = p.toString()
    history.replaceState(null, '', location.pathname + location.search + (s ? '#' + s : ''))
  }
}

export function readHashView() {
  const p = new URLSearchParams(location.hash.slice(1))
  const h = p.get('view')
  return h === 'upgrade' ? 'upgrade' : (h === 'assets' || h === 'repairs') ? 'assets' : 'overview'
}
