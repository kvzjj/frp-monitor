#!/usr/bin/env bash
# 构建 + 部署到网关 web/（静态下发，无需重启 frp-monitor）
set -euo pipefail
cd "$(dirname "$0")"
GATEWAY="${GATEWAY:-frp-gateway}"
npm run build
echo "==> 上传 dist → ${GATEWAY}:/opt/frp-monitor/web/"
ssh "$GATEWAY" 'mkdir -p /opt/frp-monitor/web/assets'
scp -q dist/index.html "$GATEWAY:/opt/frp-monitor/web/index.html"
scp -q dist/assets/* "$GATEWAY:/opt/frp-monitor/web/assets/"
echo "==> 完成：面板首页为 Vue 构建（/classic 为原版兜底）"
