import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

// 开发：BACKEND 指向后端（默认本机 9100；连网关可配 http://127.0.0.1:9100 经 ssh -L 转发）
const backend = process.env.BACKEND || 'http://127.0.0.1:9100'

export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5173,
    proxy: {
      '/api': { target: backend, ws: false },
      '/attach': { target: backend }
    }
  },
  build: {
    outDir: 'dist',
    assetsInlineLimit: 8192
  }
})
