// fetch 封装：401（会话过期/未登录）→ 整页跳登录页；其余透传
const rawFetch = window.fetch.bind(window)

export async function api(path, opts = {}) {
  const r = await rawFetch(path, { cache: 'no-store', ...opts })
  if (r.status === 401 && !location.pathname.startsWith('/login')) {
    location.href = '/login'
  }
  return r
}

export async function apiJson(path, opts = {}) {
  const r = await api(path, opts)
  const d = await r.json().catch(() => null)
  if (!r.ok || d == null) throw new Error((d && d.error) || `HTTP ${r.status}`)
  return d
}

export async function copyText(text) {
  try { await navigator.clipboard.writeText(text); return true } catch (e) { /* fallback below */ }
  try {
    const ta = document.createElement('textarea')
    ta.value = text
    ta.style.position = 'fixed'
    ta.style.opacity = '0'
    document.body.appendChild(ta)
    ta.select()
    const ok = document.execCommand('copy')
    ta.remove()
    return ok
  } catch (e) { return false }
}
