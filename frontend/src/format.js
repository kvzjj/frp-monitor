// 纯格式化/工具函数（自原 vanilla 前端平移，语义不变）

export const SITE_COLORS = ['#2563eb', '#059669', '#d97706', '#7c3aed', '#db2777', '#0891b2']

export function siteColor(s) {
  let i = 0
  for (const c of String(s)) i = (i * 31 + c.charCodeAt(0)) % 997
  return SITE_COLORS[i % SITE_COLORS.length]
}

export function fmtTime(u) {
  return new Date(u * 1000).toLocaleString('zh-CN', { hour12: false })
}

export function pct(v) {
  return v == null ? '—' : (Math.round(v * 1000) / 10) + '%'
}

export function fmtDuration(m) {
  if (m == null || isNaN(m)) return '—'
  m = Math.round(m)
  if (m <= 0) return '0分钟'
  const h = Math.floor(m / 60), r = m % 60
  if (h && r) return h + '小时' + r + '分'
  if (h) return h + '小时'
  return r + '分钟'
}

export function fmtHours(m) {
  return m == null || isNaN(m) ? '—' : (m / 60).toFixed(1) + 'h'
}

export function natCmp(a, b) {
  return String(a).localeCompare(String(b), undefined, { numeric: true, sensitivity: 'base' })
}

export function verCmp(a, b) {
  const pa = String(a).split(/[.-]/), pb = String(b).split(/[.-]/)
  for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
    const x = parseInt(pa[i]), y = parseInt(pb[i])
    if (!isNaN(x) && !isNaN(y)) {
      if (x !== y) return x < y ? -1 : 1
    } else {
      const c = String(pa[i] || '').localeCompare(String(pb[i] || ''))
      if (c) return c < 0 ? -1 : 1
    }
  }
  return 0
}

export function fmtBytes(n) {
  if (n == null || isNaN(n)) return '—'
  const u = ['B', 'KB', 'MB', 'GB', 'TB']
  let i = 0
  n = Number(n)
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++ }
  return (i ? n.toFixed(1) : Math.round(n)) + ' ' + u[i]
}

export function parseTs(v) {
  if (v == null) return null
  if (typeof v === 'number') return v < 1e12 ? v * 1000 : v
  const t = Date.parse(String(v).replace(' ', 'T'))
  return isNaN(t) ? null : t
}

export function fmtClock(ms) {
  const d = new Date(ms)
  return isNaN(d.getTime()) ? String(ms == null ? '—' : ms) : d.toLocaleString('zh-CN', { hour12: false })
}

export function fmtClockOf(v) {
  const ms = parseTs(v)
  return ms == null ? null : fmtClock(ms)
}

export function fmtDur2(sec) {
  if (sec == null || isNaN(sec) || sec < 0) return '—'
  sec = Math.round(sec)
  const h = Math.floor(sec / 3600), m = Math.floor(sec % 3600 / 60), s = sec % 60
  if (h) return h + '时' + m + '分'
  if (m) return m + '分' + s + '秒'
  return s + '秒'
}

export function fmtRel(ms) {
  if (ms == null) return '—'
  let s = Math.max(0, (Date.now() - ms) / 1000)
  if (s < 60) return Math.floor(s) + ' 秒前'
  const m = s / 60
  if (m < 60) return Math.floor(m) + ' 分钟前'
  const h = m / 60
  if (h < 24) return Math.floor(h) + ' 小时前'
  const d = h / 24
  if (d < 30) return Math.floor(d) + ' 天前'
  if (d < 365) return Math.floor(d / 30) + ' 个月前'
  return Math.floor(d / 365) + ' 年前'
}

export const ETAGS = { online: '上线', offline: '掉线', version_change: '版本变更', first_seen: '新设备' }
