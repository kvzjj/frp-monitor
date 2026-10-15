#!/usr/bin/env python3
"""Live FRP dashboard proxy monitor.

The server asks frps for its current proxy list, so adding/removing an frpc
requires no change to this program. Only proxies currently reported by frps
are returned. Results are naturally sorted by hostname/proxy name.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, unquote, urlsplit
from urllib.request import Request, urlopen
from datetime import datetime, timedelta
import base64
import hmac
import json
import os
import re
import threading
import time

FRPS_API = os.getenv("FRPS_API", "http://127.0.0.1:7400/api/proxy/tcp")
FRPS_USER = os.getenv("FRPS_USER", "frpmonitor")
FRPS_PASSWORD = os.getenv("FRPS_PASSWORD", "")
HTTP_PORT = int(os.getenv("HTTP_PORT", "9100"))
REFRESH_INTERVAL = float(os.getenv("FRP_MONITOR_REFRESH_INTERVAL", "15"))
VERSION_TTL = float(os.getenv("FRP_MONITOR_VERSION_TTL", "600"))
VERSION_ERROR_TTL = float(os.getenv("FRP_MONITOR_VERSION_ERROR_TTL", "60"))
# 硬件型号/IP 变化很慢:探测到后缓存这么久(默认 12h)再刷新一次
HW_TTL = float(os.getenv("FRP_MONITOR_HW_TTL", "43200"))

# --- optional MySQL history persistence -------------------------------------
# All FRP_MONITOR_DB_* settings are optional; without a password (or without
# python3-pymysql) the monitor keeps running memory-only, exactly as before.
DB_HOST = os.getenv("FRP_MONITOR_DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("FRP_MONITOR_DB_PORT", "3307"))
DB_USER = os.getenv("FRP_MONITOR_DB_USER", "frp_monitor")
DB_NAME = os.getenv("FRP_MONITOR_DB_NAME", "frp_monitor")
DB_PASSWORD = os.getenv("FRP_MONITOR_DB_PASSWORD", "")
SNAPSHOT_EVERY = float(os.getenv("FRP_MONITOR_SNAPSHOT_EVERY", "300"))
STATUS_RETENTION_DAYS = int(os.getenv("FRP_MONITOR_STATUS_RETENTION_DAYS", "90"))
BATCH_RETENTION_DAYS = int(os.getenv("FRP_MONITOR_BATCH_RETENTION_DAYS", "14"))

# --- optional HTTP Basic Auth for the whole panel (page + APIs) ---------------
# Both must be set to enable; empty = open access (legacy behavior).
PANEL_USER = os.getenv("FRP_MONITOR_PANEL_USER", "")
PANEL_PASSWORD = os.getenv("FRP_MONITOR_PANEL_PASSWORD", "")
SSH_JUMP = os.getenv("FRP_MONITOR_SSH_JUMP", "ubuntu@124.222.147.63")
SSH_DEVICE_USER = os.getenv("FRP_MONITOR_SSH_DEVICE_USER", "orangepi")

# --- 设备采集时长（collect duration）-----------------------------------------
# 每天这些服务器本地时间点各拉一次在线设备的当日采集时长；参数化便于自测，
# 例如 FRP_MONITOR_COLLECT_TIMES=19:00,21:00（逗号分隔 HH:MM，非法项忽略）。
COLLECT_TIMES = os.getenv("FRP_MONITOR_COLLECT_TIMES", "19:00,21:00")
COLLECT_DURATION_TIMEOUT = float(os.getenv("FRP_MONITOR_COLLECT_DURATION_TIMEOUT", "8"))

V0_HOSTS = {
    "ep-000101", "ep-000102", "ep-000103", "ep-000104", "ep-000105",
    "ep-000107", "ep-000110", "ep-000111", "ep-000112", "ep-000113",
    "ep-000114", "ep-000116",
}
V1_HOSTS = {"ep-000214", "ep-000270", "ep-000271", "ep-000280"}


def _hardware_category(model):
    """把探测到的 CPU/型号归一化成站点习惯的硬件分类(V0=amd64 / V1=rk3588)。"""
    m = (model or "").casefold()
    if not m:
        return ""
    if "rk3588" in m or "rk3576" in m or "rk3568" in m or "orange pi" in m or "orangepi" in m:
        return "V1"
    if "amd64" in m or "x86" in m or "intel" in m or "celeron" in m or "n100" in m:
        return "V0"
    return ""


def device_type(hostname):
    name = (hostname or "").casefold().split(".", 1)[0]
    if name in V0_HOSTS:
        return "EGO-Stereo-V0"
    if name in V1_HOSTS:
        return "EGO-Stereo-V1"
    return "未知"

INDEX = """<!doctype html><html lang=zh-CN><head><meta charset=utf-8><meta name=viewport content="width=device-width,initial-scale=1"><title>EGO 设备面板</title><style>{css}</style></head><body><main>
<header><div><h1>EGO 设备面板</h1><p id=sub>正在加载…</p></div><button id=refresh>立即刷新</button></header>
<nav id=viewTabs class=vtabs><button type=button data-view=overview class=on>设备总览</button><button type=button data-view=upgrade>升级</button></nav>
<section id=viewOverview>
<section class=summary><div><b id=online>0</b><span>在线设备</span></div><div><b id=registered>0</b><span>登记设备</span></div><div><b id=pendingN>0</b><span id=targetLbl>待升级</span></div><div><b id=sitesN>0</b><span>站点</span></div><div><b id=missingN>0</b><span>未上线</span></div></section>
<div class=toolbar><input id=q placeholder="搜索 hostname / device_id / 设备名 / IP"><select id=statusFilter><option value=all>全部状态</option><option value=online selected>仅在线</option><option value=offline>仅未上线</option></select><select id=targetSel title="升级目标版本"></select><button id=copyPending class=listbtn title="按当前筛选（站点/状态/版本/搜索）复制：设备号 站点 IP SSH端口 device_id 当前版本">复制待升级清单</button><button id=copyCmds class=listbtn title="按当前筛选复制 SSH 跳板登录命令">复制跳板命令</button><button id=copyList class=listbtn title="按当前筛选复制设备编号">复制编号</button></div>
<nav id=sites class=sitesbar></nav>
<nav id=vers class=sitesbar></nav>
<div id=grid class=grid></div>
<section id=events><h2>最近事件（24 小时）</h2><div id=eventsBody class=elist><p class=eempty>加载中…</p></div></section>
</section>
<section id=viewUpgrade hidden>
<section class=upanel><div class=uhead><h2>① 安装包</h2><div class=uactions><input type=file id=upgFile hidden accept=".tar.gz,.gz"><button type=button id=upgUploadBtn class=listbtn>上传安装包</button><button type=button id=upgPkgRefresh class=listbtn>刷新</button><label class=utarget>目标版本 <select id=upgVerSel></select></label></div></div><div id=upgUpBar class=pbarwrap hidden><div class=pbar><i id=upgUpFill></i></div><span id=upgUpPct></span></div><div class=twrap><table><thead><tr><th>版本</th><th>机型</th><th>大小</th><th>sha256</th><th>状态</th></tr></thead><tbody id=upgPkgBody></tbody></table></div><p class=uhint id=upgPkgHint></p><p class=uhint id=upgShaInfo></p></section>
<section class=upanel><div class=uhead><h2>② 选择目标设备</h2><div class=uselbar id=upgSelBar>已选 0 台</div></div><div id=upgDevGroups><p class=uempty>加载中…</p></div></section>
<section class=upanel id=upgActPanel><div class=uhead><h2>③ 发起升级</h2></div><div class=ubtns><button type=button id=upgRunAll>▶ 全流程升级所选</button><button type=button class=stagebtn data-kind=precheck>仅预检</button><button type=button class=stagebtn data-kind=download>仅下载</button><button type=button class=stagebtn data-kind=install>仅安装</button><button type=button class=stagebtn data-kind=verify>仅验证</button><button type=button class=stagebtn data-kind=cleanup>仅清理</button></div><p class=uhint id=upgActHint></p></section>
<section class=upanel><div class=uhead><h2>④ 任务与进度</h2><span id=upgConn class=connchip>未连接</span></div><div id=upgJobSummary><p class=uempty>暂无升级任务</p></div><div id=upgTaskGrid></div></section>
</section>
</main>
<div id=modal class=modal><div id=modalBody class=mbody></div></div>
<div id=upgModal class=modal><div id=upgModalBody class=mbody></div></div>
<script>{js}</script></body></html>"""
CSS = """*{box-sizing:border-box}body{margin:0;background:#f4f7fb;color:#172033;font:14px system-ui,-apple-system,BlinkMacSystemFont,'Segoe UI','Microsoft YaHei',sans-serif}main{max-width:1100px;margin:0 auto;padding:32px 20px}header{display:flex;justify-content:space-between;align-items:center;margin-bottom:22px}h1{margin:0 0 7px;font-size:28px}p{margin:0;color:#718096}button{border:0;border-radius:8px;padding:10px 16px;background:#2563eb;color:white;cursor:pointer;font-size:14px}input,select{font:inherit;padding:9px 12px;border:1px solid #cbd5e1;border-radius:8px;background:white;color:#172033;outline:none}input:focus,select:focus{border-color:#2563eb}.summary{display:flex;gap:14px;margin-bottom:18px;flex-wrap:wrap}.summary div{background:white;border:1px solid #e5eaf1;border-radius:12px;padding:14px 22px;min-width:120px;box-shadow:0 2px 8px #1720330a}.summary b{display:block;font-size:26px}.summary span{color:#718096;font-size:12px}.toolbar{display:flex;gap:10px;margin-bottom:12px;flex-wrap:wrap}.toolbar input{flex:1;min-width:180px}.listbtn{background:#f1f5f9;color:#334155;border:1px solid #cbd5e1;white-space:nowrap}.listbtn:hover{background:#e2e8f0}.sitesbar{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:18px}.chip{border:1px solid #e5eaf1;background:white;color:#334155;border-radius:999px;padding:6px 14px;font-size:13px;cursor:pointer}.chip b{color:var(--c,#2563eb)}.chip.on{background:var(--c,#2563eb);border-color:var(--c,#2563eb);color:white}.chip.on b{color:white}.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:14px}.card{background:white;border:1px solid #e5eaf1;border-radius:12px;padding:16px;box-shadow:0 2px 8px #1720330a;cursor:pointer;transition:transform .08s}.card:hover{transform:translateY(-1px);border-color:#b9cbdf}.card.offline{opacity:.72;background:#fafbfc}.top{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;gap:6px}.name{font-weight:700;font-size:16px;word-break:break-all}.dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:7px;background:#16a34a}.dot.off{background:#dc2626}.stag{font-size:11px;padding:2px 9px;border-radius:999px;background:color-mix(in srgb,var(--c,#64748b) 12%,white);color:var(--c,#64748b);border:1px solid color-mix(in srgb,var(--c,#64748b) 35%,white);white-space:nowrap}.row{display:flex;justify-content:space-between;color:#667085;padding-top:7px;font-size:13px;gap:8px}.value{font-family:monospace;color:#344054;text-align:right;word-break:break-all}.value.bad{color:#dc2626}small{color:#98a2b3}.empty{grid-column:1/-1;background:white;border:1px dashed #cbd5e1;border-radius:12px;padding:30px;text-align:center;color:#718096}#events{margin-top:26px;background:white;border:1px solid #e5eaf1;border-radius:12px;padding:18px 20px}#events h2{margin:0 0 10px;font-size:16px}#events .elist{max-height:320px;overflow:auto}.erow{display:flex;gap:10px;align-items:baseline;padding:7px 0;border-bottom:1px dashed #eef2f7;font-size:13px}.erow:last-child{border-bottom:0}.erow b{font-family:monospace}.etag{display:inline-block;min-width:64px;text-align:center;padding:2px 8px;border-radius:999px;font-size:12px;white-space:nowrap}.etag.online,.etag.first_seen{background:#dcfce7;color:#15803d}.etag.offline{background:#fee2e2;color:#b91c1c}.etag.version_change{background:#dbeafe;color:#1d4ed8}.edetail{color:#475569;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.erow time{color:#98a2b3;font-size:12px;white-space:nowrap}.eempty{color:#718096;margin:6px 0}.modal{display:none;position:fixed;inset:0;background:#0f172ac9;z-index:50;padding:24px;overflow:auto}.modal.show{display:flex;align-items:flex-start;justify-content:center}.mbody{width:min(760px,100%);background:white;border-radius:14px;padding:22px 24px;box-shadow:0 30px 90px #0006}.mbody header{margin-bottom:6px}.mbody .name{font-size:19px}.mbody .x{background:#f1f5f9;color:#475569;padding:6px 12px;font-size:15px}.mbody h3{margin:18px 0 8px;font-size:14px;color:#334155}.mgrid{display:grid;grid-template-columns:auto 1fr;gap:6px 16px;margin:12px 0;font-size:13px}.mgrid span{color:#718096}.mgrid b{font-family:ui-monospace,monospace;font-weight:600;word-break:break-all}.cmd{display:flex;gap:8px;align-items:center;background:#0f172a;border-radius:8px;padding:10px 12px;margin:10px 0}.cmd code{flex:1;color:#7dd3fc;font-size:12px;font-family:ui-monospace,monospace;word-break:break-all}.cmd button{padding:7px 12px;font-size:12px;background:#0369a1;white-space:nowrap}.twrap{overflow:auto;border:1px solid #e5eaf1;border-radius:8px}.twrap table{width:100%;border-collapse:collapse;font-size:12px;min-width:420px}.twrap th,.twrap td{padding:7px 10px;border-bottom:1px solid #eef2f7;text-align:left}.twrap th{color:#718096;font-weight:500;background:#f8fafc}.mbody .elist{max-height:240px;overflow:auto}@media(max-width:640px){.toolbar{flex-direction:column}.summary div{min-width:100px;flex:1}}
/* ---- 升级管理视图（Upg） ---- */
.vtabs{display:flex;gap:8px;margin:-6px 0 20px}.vtabs button{background:white;border:1px solid #e5eaf1;color:#475569;border-radius:999px;padding:8px 20px;font-size:14px}.vtabs button:hover{border-color:#b9cbdf}.vtabs button.on{background:#2563eb;border-color:#2563eb;color:white}
.upanel{background:white;border:1px solid #e5eaf1;border-radius:12px;padding:18px 20px;margin-bottom:18px;box-shadow:0 2px 8px #1720330a}.upanel button:disabled{opacity:.5;cursor:not-allowed}
.uhead{display:flex;justify-content:space-between;align-items:center;gap:12px;margin-bottom:12px;flex-wrap:wrap}.uhead h2{margin:0;font-size:16px}
.uactions{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.utarget{color:#667085;font-size:13px;display:inline-flex;gap:6px;align-items:center}.utarget select{padding:6px 10px}
.uhint{color:#98a2b3;font-size:12px;margin:10px 0 0}.uhint.warn{color:#b45309}.uhint:empty{display:none}
.shalink{display:inline-flex;align-items:center;gap:6px}.sha{font-family:ui-monospace,monospace;font-size:12px;color:#475569}
.copymini{padding:2px 8px;font-size:11px;background:#f1f5f9;border:1px solid #cbd5e1;color:#334155}.copymini:hover{background:#e2e8f0}
.pill{display:inline-block;padding:2px 10px;border-radius:999px;font-size:12px;white-space:nowrap}.pill.ok{background:#dcfce7;color:#15803d}.pill.wait{background:#fef3c7;color:#b45309}
.pbarwrap{display:flex;align-items:center;gap:10px;margin:10px 0}.pbar{flex:1;height:8px;background:#eef2f7;border-radius:999px;overflow:hidden}.pbar i{display:block;height:100%;width:0;background:#2563eb;border-radius:999px;transition:width .2s}.pbarwrap span{font-size:12px;color:#667085;white-space:nowrap}
.uselbar{color:#475569;font-size:13px;display:flex;gap:10px;align-items:center;flex-wrap:wrap}.uselbar b{color:#2563eb}
.minibtn{padding:3px 10px;font-size:11px;background:#f1f5f9;border:1px solid #cbd5e1;color:#667085;border-radius:999px}.minibtn:hover{background:#e2e8f0}
.vgroup{border:1px solid #e5eaf1;border-radius:10px;margin-bottom:12px;overflow:hidden}.vghead{display:flex;align-items:center;gap:10px;padding:9px 14px;background:#f8fafc;border-bottom:1px solid #eef2f7;font-size:13px;flex-wrap:wrap}.vghead .vgt{font-weight:600}.vghead .vgn{color:#98a2b3}.vghead .grpbtn{margin-left:auto;padding:4px 12px;font-size:12px;background:#f1f5f9;border:1px solid #cbd5e1;color:#334155}.vghead .grpbtn:hover{background:#e2e8f0}.vghead input,.vrow input[type=checkbox]{width:15px;height:15px;accent-color:#2563eb;flex:none}
.vrows{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr))}.vrow{display:flex;align-items:center;gap:9px;padding:8px 14px;border-bottom:1px solid #f1f5f9;font-size:13px;min-width:0;cursor:pointer}.vrow:last-child{border-bottom:0}.vrow.off{opacity:.45;background:#fafbfc;cursor:not-allowed}.vrow .bh{font-family:ui-monospace,monospace;word-break:break-all}.vrow small{color:#98a2b3;white-space:nowrap}.vrow .vsite{margin-left:auto}
.ubtns{display:flex;gap:10px;flex-wrap:wrap;align-items:center}.ubtns #upgRunAll{background:#16a34a;font-size:15px;padding:11px 22px}.ubtns #upgRunAll:hover:not(:disabled){background:#15803d}.stagebtn{background:#f1f5f9;color:#334155;border:1px solid #cbd5e1;font-size:13px;padding:9px 14px}.stagebtn:hover:not(:disabled){background:#e2e8f0}
.connchip{font-size:12px;padding:3px 12px;border-radius:999px;background:#f1f5f9;color:#98a2b3;white-space:nowrap}.connchip.live{background:#dcfce7;color:#15803d}.connchip.poll{background:#fef3c7;color:#b45309}
.ujsum{background:#f8fafc;border:1px solid #e5eaf1;border-radius:10px;padding:10px 14px;font-size:13px;color:#475569;margin-bottom:14px;display:flex;gap:14px;flex-wrap:wrap;align-items:center}.ujsum b{color:#172033}
.ucardgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:14px}
.ucard{background:white;border:1px solid #e5eaf1;border-radius:12px;padding:14px 16px;box-shadow:0 2px 8px #1720330a;cursor:pointer}.ucard.done{border-color:#bbf7d0}.ucard.failed{border-color:#fecaca}.ucard.blocked{border-color:#fde68a}
.ucard .uh{display:flex;align-items:center;gap:8px;margin-bottom:8px;flex-wrap:wrap}.ucard .uh .name{font-weight:700;font-size:15px;word-break:break-all}.ucard .uh .tstat{margin-left:auto}
.tstat{font-size:12px;padding:2px 10px;border-radius:999px;white-space:nowrap}.tstat.running{background:#dbeafe;color:#1d4ed8}.tstat.success{background:#dcfce7;color:#15803d}.tstat.failed{background:#fee2e2;color:#b91c1c}.tstat.blocked{background:#fef3c7;color:#b45309}.tstat.pending{background:#f1f5f9;color:#667085}
.uver{color:#667085;font-size:12px;margin-bottom:10px}
.stepper{display:flex;gap:4px;margin-bottom:8px}.step{flex:1;text-align:center;font-size:11px;padding:5px 2px;border-radius:6px;background:#f1f5f9;color:#98a2b3;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}.step.done{background:#dcfce7;color:#15803d}.step.cur{background:#dbeafe;color:#1d4ed8;animation:upulse 1.2s ease-in-out infinite}.step.failed{background:#fee2e2;color:#b91c1c}.step.blocked{background:#fef3c7;color:#b45309}.step.na{opacity:.35}
@keyframes upulse{0%,100%{opacity:1}50%{opacity:.5}}
.terr{font-size:12px;color:#b91c1c;background:#fef2f2;border:1px solid #fee2e2;border-radius:6px;padding:5px 8px;margin-bottom:8px;word-break:break-all}.terr.blocked{color:#b45309;background:#fffbeb;border-color:#fde68a}
.ulogwrap{display:none;border-top:1px dashed #eef2f7;margin-top:8px;padding-top:8px}.ucard.expanded .ulogwrap{display:block}.ucard.expanded{cursor:default}
.ulogbar{display:flex;gap:8px;align-items:center;margin-bottom:6px}.ulogbar span{font-size:11px;color:#98a2b3}.ulogbar button{padding:3px 10px;font-size:11px;background:#f1f5f9;border:1px solid #cbd5e1;color:#334155}.ulogbar button:hover{background:#e2e8f0}
.ulog{background:#0f172a;color:#e2e8f0;border-radius:8px;padding:10px 12px;font:11px/1.55 ui-monospace,monospace;max-height:260px;overflow:auto;white-space:pre-wrap;word-break:break-all;margin:0}
.uempty{border:1px dashed #cbd5e1;border-radius:10px;padding:26px;text-align:center;color:#718096;font-size:13px;margin:0}
.uwarn{background:#fffbeb;border:1px solid #fde68a;color:#92400e;border-radius:8px;padding:10px 12px;font-size:13px;margin:12px 0 0}
@media(max-width:640px){.vrows{grid-template-columns:1fr}.ucardgrid{grid-template-columns:1fr}.stepper{flex-wrap:wrap}.step{flex:1 1 30%}}"""
JS = """const $=s=>document.querySelector(s);
const esc=s=>String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'})[c]);
const ETAGS={online:'上线',offline:'掉线',version_change:'版本变更',first_seen:'新设备'};
const SITE_COLORS=['#2563eb','#059669','#d97706','#7c3aed','#db2777','#0891b2'];
function siteColor(s){let i=0;for(const c of String(s))i=(i*31+c.charCodeAt(0))%997;return SITE_COLORS[i%SITE_COLORS.length]}
function fmtTime(u){return new Date(u*1000).toLocaleString('zh-CN',{hour12:false})}
async function copyText(text){
  try{await navigator.clipboard.writeText(text);return true}catch(e){}
  try{const ta=document.createElement('textarea');ta.value=text;ta.style.position='fixed';ta.style.opacity='0';document.body.appendChild(ta);ta.select();const ok=document.execCommand('copy');ta.remove();return ok}catch(e){return false}
}
function pct(v){return v==null?'—':(Math.round(v*1000)/10)+'%'}
function fmtDuration(m){if(m==null||isNaN(m))return '—';m=Math.round(m);if(m<=0)return '0分钟';const h=Math.floor(m/60),r=m%60;if(h&&r)return h+'小时'+r+'分';if(h)return h+'小时';return r+'分钟'}
function fmtHours(m){return m==null||isNaN(m)?'—':(m/60).toFixed(1)+'h'}
let fleet=[],sites=[],versions=[],sshHint={},q='',siteFilter='',verFilter='',targetVer='',pendingOnly=false,statusFilter='online',refreshInterval=15;
let collectDur={enabled:false,map:{}};
function matches(f){
  if(siteFilter&&(f.site||'未登记')!==siteFilter)return false;
  if(verFilter&&(f.version||'未获取')!==verFilter)return false;
  if(pendingOnly&&(f.version||'未获取')===targetVer)return false;
  if(q){const n=q.toLowerCase();if(!f.hostname.includes(n)&&!String(f.device_id||'').toLowerCase().includes(n)&&!String(f.device_name||'').toLowerCase().includes(n)&&!String(f.ip||'').includes(n)&&!String(f.hw_model||'').toLowerCase().includes(n))return false}
  if(statusFilter==='online'&&!f.online)return false;
  if(statusFilter==='offline'&&f.online)return false;
  return true;
}
function filtered(){return fleet.filter(matches)}
function renderSites(){
  const el=$('#sites');let h=`<button class="chip${siteFilter===''?' on':''}" data-site="">全部 ${fleet.length}</button>`;
  for(const s of sites){h+=`<button class="chip${siteFilter===s.site?' on':''}" data-site="${esc(s.site)}" style="--c:${siteColor(s.site)}">${esc(s.site)} <b>${s.online}/${s.expected}</b></button>`}
  const un=fleet.filter(f=>!f.site).length;
  if(un)h+=`<button class="chip${siteFilter==='未登记'?' on':''}" data-site="未登记" style="--c:#64748b">未登记 <b>${un}</b></button>`;
  el.innerHTML=h;
  el.querySelectorAll('.chip').forEach(b=>b.onclick=()=>{siteFilter=b.dataset.site===siteFilter?'':b.dataset.site;if(siteFilter&&statusFilter==='online'){statusFilter='all';$('#statusFilter').value='all'}renderSites();renderGrid()});
  $('#sitesN').textContent=sites.length;
  $('#missingN').textContent=fleet.filter(f=>!f.online).length;
}
function updateDrift(){
  const pend=fleet.filter(f=>!f.retired&&(f.version||'未获取')!==targetVer);
  $('#pendingN').textContent=pend.length;
  $('#targetLbl').textContent=targetVer?`待升级（≠ ${targetVer}）`:'待升级';
}
function renderVers(){
  const el=$('#vers');if(!el)return;
  let h=`<button class="chip${verFilter===''&&!pendingOnly?' on':''}" data-v="">全部 ${fleet.length}</button>`;
  for(const v of versions){
    const c=v.version==='未获取'?'#64748b':'#0f766e';
    const t=v.online!==v.count?`${v.online}/${v.count}`:String(v.count);
    h+=`<button class="chip${verFilter===v.version?' on':''}" data-v="${esc(v.version)}" style="--c:${c}">${esc(v.version)} <b>${t}</b></button>`;
  }
  if(targetVer){
    const pend=fleet.filter(f=>!f.retired&&(f.version||'未获取')!==targetVer).length;
    h+=`<button class="chip${pendingOnly?' on':''}" data-pending=1 style="--c:#b45309" title="只看待升级设备（≠ 目标版本）">≠ ${esc(targetVer)} <b>${pend}</b></button>`;
  }
  el.innerHTML=h;
  el.querySelectorAll('.chip').forEach(b=>b.onclick=()=>{
    if(b.dataset.pending){pendingOnly=!pendingOnly;if(pendingOnly)verFilter=''}
    else{verFilter=b.dataset.v===verFilter?'':b.dataset.v;pendingOnly=false}
    renderVers();renderGrid();
  });
  updateDrift();
}
function renderTargetSel(){
  const sel=$('#targetSel');if(!sel)return;
  const opts=versions.map(v=>v.version);
  sel.innerHTML=opts.map(v=>`<option value="${esc(v)}">${esc(v)}</option>`).join('');
  if(!targetVer||!opts.includes(targetVer))targetVer=opts.find(v=>v!=='未获取')||opts[0]||'';
  sel.value=targetVer;
}
function card(f){
  const tag=f.site?`<span class="stag" style="--c:${siteColor(f.site)}">${esc(f.site)}</span>`:'';
  const cd=collectDur.enabled?collectDur.map[(f.hostname||'').toLowerCase()]:null;
  if(f.online){
    return `<article class="card online" data-h="${esc(f.hostname)}"><div class="top"><span class="name"><i class="dot"></i>${esc(f.hostname)}</span>${tag}</div>
    <div class="row"><span>版本</span><span class="value">${esc(f.version||'未获取')}${f.version_error?` <small title="${esc(f.version_error)}">(读取失败)</small>`:''}</span></div>
    <div class="row"><span>硬件</span><span class="value">${esc(f.hardware||'—')}${f.hw_model?` · ${esc(f.hw_model)}`:''}</span></div>
    <div class="row"><span>IP</span><span class="value">${esc(f.ip||'—')}</span></div>
    <div class="row"><span>SSH / Web</span><span class="value">${f.ssh_port||'—'} / ${f.web_port||'—'}</span></div>
    <div class="row"><span>device_id</span><span class="value">${esc(f.device_id||'—')}</span></div>
    <div class="row"><span>连接数</span><span class="value">${f.connections!=null?f.connections:'—'}</span></div>
    ${collectDur.enabled?`<div class="row"><span>今日采集</span><span class="value">${cd&&cd.today_minutes!=null?fmtHours(cd.today_minutes):'—'}</span></div>`:''}
    <div class="row"><small>frpc 上线：${esc(f.started||'—')}</small></div></article>`;
  }
  return `<article class="card offline" data-h="${esc(f.hostname)}"><div class="top"><span class="name"><i class="dot off"></i>${esc(f.hostname)}</span>${tag}</div>
  <div class="row"><span>状态</span><span class="value bad">未上线</span></div>
  <div class="row"><span>最后在线</span><span class="value">${f.last_seen?esc(f.last_seen):'从未连接'}</span></div>
  <div class="row"><span>硬件</span><span class="value">${esc(f.hardware||'—')}${f.hw_model?` · ${esc(f.hw_model)}`:''}</span></div>
  <div class="row"><span>IP</span><span class="value">${esc(f.ip||'—')}</span></div>
  <div class="row"><span>device_id</span><span class="value">${esc(f.device_id||'—')}</span></div>
  <div class="row"><span>历史版本</span><span class="value">${esc(f.version||'—')}</span></div></article>`;
}
function renderGrid(){
  const list=fleet.filter(matches);
  $('#grid').innerHTML=list.length?list.map(card).join(''):'<div class="empty">没有匹配的设备</div>';
  document.querySelectorAll('.card').forEach(c=>c.onclick=()=>openDevice(c.dataset.h));
}
async function load(force){
  try{
    const r=await fetch('/api/hosts'+(force?'?refresh=1':''),{cache:'no-store'});
    const d=await r.json();
    if(!r.ok)throw Error(d.error||'请求失败');
    fleet=d.fleet||[];sites=d.sites||[];versions=d.versions||[];sshHint=d.ssh_hint||{};
    if(d.refresh_interval)refreshInterval=d.refresh_interval;
    $('#online').textContent=d.online;
    $('#registered').textContent=d.registered!=null?d.registered:d.total;
    $('#sub').textContent='最后更新：'+fmtTime(d.timestamp)+' · '+d.online+' 在线 / '+(d.registered!=null?d.registered:d.total)+' 台登记 · 每 '+refreshInterval+'s 自动刷新';
    renderSites();renderTargetSel();renderVers();renderGrid();
  }catch(e){$('#sub').textContent='获取 frps 状态失败：'+e}
}
function closeModal(){$('#modal').classList.remove('show')}
async function openDevice(h){
  const m=$('#modal');m.classList.add('show');$('#modalBody').innerHTML='<p class="eempty">加载中…</p>';
  try{
    const r=await fetch('/api/device/'+encodeURIComponent(h));const d=await r.json();
    if(!r.ok)throw Error(d.error||'读取失败');
    const hint=d.ssh_hint||sshHint||{};
    const cd=(collectDur.map||{})[String(d.hostname||'').toLowerCase()]||null;
    const cdRows=cd&&Array.isArray(cd.history)?cd.history.slice().sort((a,b)=>String(b.date||'').localeCompare(String(a.date||''))).map(r=>`<tr><td>${esc(r.date)}</td><td>${fmtDuration(r.minutes)}</td></tr>`).join(''):'';
    const evRows=(d.events||[]).map(e=>`<div class="erow"><span class="etag ${esc(e.event_type)}">${ETAGS[e.event_type]||esc(e.event_type)}</span><b>${esc(e.ts||'')}</b><span class="edetail">${esc(e.new_value||e.detail||'')}${e.old_value&&e.new_value?' ← '+esc(e.old_value):''}</span></div>`).join('')||'<p class="eempty">暂无事件</p>';
    const stRows=(d.status||[]).map(s=>`<tr><td>${esc(s.ts)}</td><td>${s.online?'<span style="color:#16a34a">在线</span>':'<span style="color:#dc2626">离线</span>'}</td><td>${esc(s.release_version||'—')}</td><td>${s.connections!=null?s.connections:'—'}</td></tr>`).join('');
    const cmd=d.ssh_port?`ssh -J ${hint.jump||'ubuntu@124.222.147.63'} -p ${d.ssh_port} ${hint.user||'orangepi'}@127.0.0.1`:'';
    $('#modalBody').innerHTML=`
      <header><div><span class="name"><i class="dot${d.online?'':' off'}"></i>${esc(d.hostname)}</span>${d.site?` <span class="stag" style="--c:${siteColor(d.site)}">${esc(d.site)}</span>`:''}</div><button class="x" id="mClose">✕</button></header>
      <div class="mgrid">
        <span>设备名 / device_id</span><b>${esc(d.device_name||'—')} / ${esc(d.device_id||'—')}</b>
        <span>归属</span><b>${esc(d.owner||d.site||'未登记')}</b>
        <span>硬件版本</span><b>${esc(d.hardware||'—')}</b>
        <span>型号 / IP</span><b>${esc(d.hw_model||'—')}${d.ip?' / '+esc(d.ip):''}</b>
        <span>当前状态</span><b>${d.online?'在线':'未上线（最后在线 '+(d.last_seen?esc(d.last_seen):'从未')+'）'}</b>
        <span>版本</span><b>${esc(d.release_version||'未获取')}</b>
        <span>SSH / Web 端口</span><b>${d.ssh_port||'—'} / ${d.web_port||'—'}</b>
        <span>首次 / 最后在线</span><b>${esc(d.first_seen||'—')} / ${esc(d.last_seen||'从未')}</b>
        <span>在线率 24h / 7d</span><b>${pct(d.uptime_24h)} / ${pct(d.uptime_7d)}</b>
        <span>采集时长</span><b>${fmtDuration(cd?cd.today_minutes:null)}</b>
      </div>
      ${cmd?`<div class="cmd"><code id="sshCmd">${esc(cmd)}</code><button id="copyBtn">复制跳板命令</button></div>`:''}
      <h3>近 7 天采集时长</h3><div class="twrap"><table><tr><th>日期</th><th>时长</th></tr>${cdRows||`<tr><td colspan=2 class="eempty">${collectDur.enabled?'暂无采集记录':'采集时长统计未启用'}</td></tr>`}</table></div>
      <h3>最近快照（5 分钟一条）</h3><div class="twrap"><table><tr><th>时间</th><th>状态</th><th>版本</th><th>连接</th></tr>${stRows||'<tr><td colspan=4 class="eempty">暂无快照</td></tr>'}</table></div>
      <h3>最近事件</h3><div class="elist">${evRows}</div>`;
    $('#mClose').onclick=closeModal;
    const cb=$('#copyBtn');if(cb)cb.onclick=async()=>{if(await copyText(cmd)){cb.textContent='已复制 ✓';setTimeout(()=>cb.textContent='复制跳板命令',1500)}else alert(cmd)};
  }catch(e){$('#modalBody').innerHTML='<p class="eempty">'+esc(String(e))+'</p>'}
}
$('#refresh').onclick=()=>load(true);
$('#q').oninput=e=>{q=e.target.value.trim().toLowerCase();renderGrid()};
$('#statusFilter').onchange=e=>{statusFilter=e.target.value;renderGrid()};
$('#targetSel').onchange=e=>{targetVer=e.target.value;renderVers();renderGrid()};
async function copyBtn(btn,text,label,okLabel){
  if(!text){btn.textContent='无匹配设备';setTimeout(()=>btn.textContent=label,1200);return}
  if(await copyText(text)){btn.textContent=okLabel}else{alert(text)}
  setTimeout(()=>btn.textContent=label,1800);
}
$('#copyPending').onclick=async()=>{
  const rows=filtered().filter(f=>!f.retired);
  const head=['设备号','站点','IP','SSH端口','device_id','当前版本','硬件'];
  const text=[head.join('\\t')].concat(rows.map(f=>[f.hostname.toUpperCase(),f.site||'未登记',f.ip||'',f.ssh_port||'',f.device_id||'',f.version||'未获取',[f.hardware,f.hw_model].filter(Boolean).join(' ')||''].join('\\t'))).join('\\n');
  await copyBtn($('#copyPending'),rows.length?text:'','复制待升级清单',`已复制 ${rows.length} 台 ✓`);
};
$('#copyCmds').onclick=async()=>{
  const hint=sshHint||{},jump=hint.jump||'ubuntu@124.222.147.63',user=hint.user||'orangepi';
  const rows=filtered().filter(f=>!f.retired&&f.ssh_port);
  const text=rows.map(f=>`ssh -J ${jump} -p ${f.ssh_port} ${user}@127.0.0.1  # ${f.hostname.toUpperCase()}${f.site?' ('+f.site+')':''}`).join('\\n');
  await copyBtn($('#copyCmds'),rows.length?text:'','复制跳板命令',`已复制 ${rows.length} 台 ✓`);
};
$('#copyList').onclick=async()=>{
  const list=filtered().map(f=>f.hostname.toUpperCase());
  const btn=$('#copyList');
  if(!list.length){btn.textContent='无匹配设备';setTimeout(()=>btn.textContent='复制编号',1200);return}
  if(await copyText(list.join('\\n'))){btn.textContent=`已复制 ${list.length} 台 ✓`}else{alert(list.join('\\n'))}
  setTimeout(()=>btn.textContent='复制编号',1800);
};
$('#modal').onclick=e=>{if(e.target.id==='modal')closeModal()};
document.addEventListener('keydown',e=>{if(e.key==='Escape')closeModal()});
load();setInterval(()=>load(false),15000);
async function loadEvents(){const el=document.querySelector('#eventsBody');if(!el)return;try{const r=await fetch('/api/events?hours=24&limit=60',{cache:'no-store'});const d=await r.json();if(!d.enabled){el.innerHTML='<p class=eempty>历史持久化未启用（数据库不可用）</p>';return}el.innerHTML=d.events.length?d.events.map(e=>`<div class=erow><span class="etag ${esc(e.event_type)}">${ETAGS[e.event_type]||esc(e.event_type)}</span><b>${esc(e.hostname)}</b><span class=edetail>${esc(e.new_value||e.detail||'')}${e.old_value&&e.new_value?' ← '+esc(e.old_value):''}</span><time>${esc(e.ts)}</time></div>`).join(''):'<p class=eempty>24 小时内暂无事件</p>'}catch(err){el.innerHTML='<p class=eempty>事件读取失败</p>'}}
loadEvents();setInterval(loadEvents,60000);
async function loadCollectDuration(){
  try{
    const r=await fetch('/api/collect-duration?days=7',{cache:'no-store'});
    const d=await r.json();
    if(!r.ok)throw Error(d.error||'请求失败');
    collectDur={enabled:!!d.enabled,map:{}};
    for(const it of d.items||[])collectDur.map[String(it.hostname||'').toLowerCase()]=it;
  }catch(e){/* 接口未上线或拉取失败:沿用已有数据,无数据时展示 — / 空态 */}
  if(fleet.length)renderGrid();
}
loadCollectDuration();setInterval(loadCollectDuration,300000);

/* ================= 升级管理视图（Upg.* 独立命名空间，不改动设备总览逻辑） =================
 数据契约（后端联调基准）：GET /api/packages、POST /api/upgrade/jobs、GET /api/upgrade/jobs(/<id>)、
 GET /api/upgrade/events（SSE：{type:'job'|'task'|'log',...}）、GET /api/upgrade/jobs/<id>/log?offset=N[&hostname=]。
 字段解析全部做了兼容容错，后端未上线时各区城优雅空态。 */
const Upg={
STAGES:['precheck','download','prepare','install','verify','cleanup'],
STAGE_LBL:{precheck:'预检',download:'下载',prepare:'准备',install:'安装',verify:'验证',cleanup:'清理'},
KIND_LBL:{upgrade:'全流程升级',precheck:'仅预检',download:'仅下载',prepare:'仅准备',install:'仅安装',verify:'仅验证',cleanup:'仅清理'},
STAT_LBL:{running:'进行中',success:'成功',failed:'失败',blocked:'已阻断',pending:'等待中'},
packages:[],fleet:[],selected:new Set(),selTouched:false,targetVersion:'',pkgFail:0,
job:null,tasks:[],logs:{},logOff:{},follow:{},expanded:null,fromSnap:{},
es:null,pollT:null,connState:'off',view:'overview',upXhr:null,
gid(i){return document.getElementById(i)},
normStatus(s){s=String(s==null?'':s).toLowerCase();
 if(['success','done','ok','complete','completed','finished'].includes(s))return 'success';
 if(['failed','fail','error','err'].includes(s))return 'failed';
 if(['blocked','block','skipped','skip'].includes(s))return 'blocked';
 if(['running','run','active','in_progress','progress','started'].includes(s))return 'running';
 return 'pending'},
active(){return !!this.job&&['running','pending'].includes(this.normStatus(this.job.status))},
fmtB(n){if(n==null||isNaN(n))return '—';const u=['B','KB','MB','GB','TB'];let i=0;n=Number(n);while(n>=1024&&i<u.length-1){n/=1024;i++}return(i?n.toFixed(1):Math.round(n))+' '+u[i]},
parseTs(v){if(v==null)return null;if(typeof v==='number')return v<1e12?v*1000:v;const t=Date.parse(String(v).replace(' ','T'));return isNaN(t)?null:t},
fmtClock(ms){const d=new Date(ms);return isNaN(d.getTime())?String(ms==null?'—':ms):d.toLocaleString('zh-CN',{hour12:false})},
fmtDur2(sec){if(sec==null||isNaN(sec)||sec<0)return '—';sec=Math.round(sec);const h=Math.floor(sec/3600),m=Math.floor(sec%3600/60),s=sec%60;if(h)return h+'时'+m+'分';if(m)return m+'分'+s+'秒';return s+'秒'},
verCmp(a,b){const pa=String(a).split(/[.-]/),pb=String(b).split(/[.-]/);for(let i=0;i<Math.max(pa.length,pb.length);i++){const x=parseInt(pa[i]),y=parseInt(pb[i]);if(!isNaN(x)&&!isNaN(y)){if(x!==y)return x<y?-1:1}else{const c=String(pa[i]||'').localeCompare(String(pb[i]||''));if(c)return c<0?-1:1}}return 0},
archOf(f){const m=String((f&&(f.hw_model||f.hardware))||'').toLowerCase();if(/rk3588|rk3576|rk3568|arm64|aarch64|orange\\s?pi|v1/.test(m))return 'rk3588-arm64';if(/amd64|x86|intel|celeron|n100|v0/.test(m))return 'amd64';return ''},
init(){
 const tabs=this.gid('viewTabs');
 tabs.addEventListener('click',e=>{const b=e.target.closest('button[data-view]');if(b)this.setView(b.dataset.view)});
 window.addEventListener('hashchange',()=>{const v=new URLSearchParams(location.hash.slice(1)).get('view')==='upgrade'?'upgrade':'overview';if(v!==this.view)this.setView(v,true)});
 this.gid('upgUploadBtn').onclick=()=>this.gid('upgFile').click();
 this.gid('upgFile').onchange=e=>{const f=e.target.files&&e.target.files[0];if(f)this.upload(f);e.target.value=''};
 this.gid('upgPkgRefresh').onclick=()=>this.loadPkgs();
 this.gid('upgVerSel').onchange=e=>{this.targetVersion=e.target.value;this.renderPkgs();this.updateSelBar();this.updateActions()};
 this.gid('upgPkgBody').addEventListener('click',async e=>{const b=e.target.closest('button[data-sha]');if(!b)return;b.textContent='…';const ok=await copyText(b.dataset.sha);b.textContent=ok?'已复制':'失败';setTimeout(()=>b.textContent='复制',1500)});
 const dg=this.gid('upgDevGroups');
 dg.addEventListener('change',e=>{const el=e.target;
  if(el.dataset.h!==undefined&&el.type==='checkbox'){this.selTouched=true;if(el.checked)this.selected.add(el.dataset.h);else this.selected.delete(el.dataset.h);this.renderDevs();this.updateSelBar();this.updateActions()}
  else if(el.dataset.grp!==undefined){this.selTouched=true;const rows=this.devRows().filter(r=>r.ver===el.dataset.grp&&r.online);for(const r of rows){if(el.checked)this.selected.add(r.h);else this.selected.delete(r.h)}this.renderDevs();this.updateSelBar();this.updateActions()}});
 dg.addEventListener('click',e=>{const b=e.target.closest('button[data-grpup]');if(!b)return;const k=b.dataset.grpup;this.selTouched=true;const rows=this.devRows().filter(r=>r.ver===k&&r.online);this.selected=new Set(rows.map(r=>r.h));this.renderDevs();this.updateSelBar();this.updateActions();this.setActHint('已选择 '+k+' 全组 '+rows.length+' 台，确认无误后在下方发起',false);this.gid('upgActPanel').scrollIntoView({behavior:'smooth',block:'center'})});
 this.gid('upgSelBar').addEventListener('click',e=>{if(e.target.closest('button[data-clr]')){this.selected.clear();this.selTouched=true;this.renderDevs();this.updateSelBar();this.updateActions()}});
 this.gid('upgRunAll').onclick=()=>{if(this.canLaunch())this.showConfirm()};
 document.querySelectorAll('.stagebtn[data-kind]').forEach(b=>b.onclick=()=>{if(this.canLaunch())this.launch(b.dataset.kind)});
 const grid=this.gid('upgTaskGrid');
 grid.addEventListener('click',e=>{
  const fb=e.target.closest('button[data-fl]');if(fb){const h=fb.dataset.fl;const cur=this.follow[h]!==false;this.follow[h]=!cur;fb.textContent=this.follow[h]===false?'恢复跟随':'暂停跟随';if(this.follow[h]!==false){const pre=this.logDom(h);if(pre)pre.scrollTop=pre.scrollHeight}return}
  const c=e.target.closest('.ucard');if(c)this.toggleLog(c.dataset.h)});
 const um=this.gid('upgModal');um.addEventListener('click',e=>{if(e.target===um)this.closeConfirm()});
 document.addEventListener('keydown',e=>{if(e.key==='Escape')this.closeConfirm()});
 this.route();
 setInterval(()=>{if(this.view==='upgrade')this.loadFleet()},15000);
 setInterval(()=>{if(this.view==='upgrade'&&this.packages.some(p=>!p.ready))this.loadPkgs()},15000);
},
setView(v,skipHash){
 if(v!=='upgrade')v='overview';
 this.view=v;
 this.gid('viewOverview').hidden=v!=='overview';
 this.gid('viewUpgrade').hidden=v!=='upgrade';
 document.querySelectorAll('#viewTabs button').forEach(b=>b.classList.toggle('on',b.dataset.view===v));
 if(!skipHash){const p=new URLSearchParams(location.hash.slice(1));if(v==='upgrade')p.set('view','upgrade');else p.delete('view');const s=p.toString();history.replaceState(null,'',location.pathname+location.search+(s?'#'+s:''))}
 if(v==='upgrade')this.enter();
},
route(){const v=new URLSearchParams(location.hash.slice(1)).get('view')==='upgrade'?'upgrade':'overview';this.setView(v,true)},
enter(){this.loadPkgs();this.loadFleet();this.loadJobs();this.updateSelBar();this.updateActions()},
setPkgHint(msg,warn){const el=this.gid('upgPkgHint');el.textContent=msg||'';el.className='uhint'+(warn?' warn':'')},
setActHint(msg,warn){const el=this.gid('upgActHint');el.textContent=msg||'';el.className='uhint'+(warn?' warn':'')},
normPkg(p){
 const fn=String(p.filename||p.name||'');
 let ver=p.version||p.ver||'';if(!ver){const m=fn.match(/v\\d+\\.\\d+(\\.\\d+)*/i);ver=m?m[0].toLowerCase():(fn||'未知版本')}
 let arch=p.arch||p.platform||p.machine||'';
 if(!arch){if(/rk3588|arm64|aarch64/i.test(fn))arch='rk3588-arm64';else if(/amd64|x86/i.test(fn))arch='amd64';else arch=''}
 const sha=String(p.sha256||p.hash||'');
 return{filename:fn,version:ver,arch:arch||'未知机型',size:p.size!=null?p.size:p.bytes,sha256:sha,ready:!!sha&&p.ready!==false}
},
async loadPkgs(){
 try{
  const r=await fetch('/api/packages',{cache:'no-store'});const d=await r.json();
  if(!r.ok)throw Error(d.error||('HTTP '+r.status));
  const list=d.packages||d.items||(Array.isArray(d)?d:[]);
  this.packages=list.map(p=>this.normPkg(p));this.pkgFail=0;this.renderPkgs();
 }catch(e){this.pkgFail++;this.renderPkgs()}
},
renderPkgs(){
 const tb=this.gid('upgPkgBody');
 if(this.pkgFail){tb.innerHTML='<tr><td colspan=5 class=eempty>安装包接口不可用（后端未上线或服务异常），可点「刷新」重试</td></tr>';this.setPkgHint('GET /api/packages 请求失败：稍后自动重试或手动刷新',true)}
 else if(!this.packages.length){tb.innerHTML='<tr><td colspan=5 class=eempty>暂无安装包：可点「上传安装包」，或由 ops 将包 scp 到服务器包目录后点「刷新」</td></tr>';this.setPkgHint('')}
 else{tb.innerHTML=this.packages.map(p=>{
  const shaCell=p.sha256?`<span class=shalink><span class=sha title="${esc(p.sha256)}">${esc(p.sha256.slice(0,14))}…</span><button type=button class=copymini data-sha="${esc(p.sha256)}">复制</button></span>`:'<span class=sha>—</span>';
  const st=p.ready?'<span class="pill ok">就绪</span>':'<span class="pill wait">哈希计算中…</span>';
  return `<tr><td>${esc(p.version)}</td><td>${esc(p.arch)}</td><td>${this.fmtB(p.size)}</td><td>${shaCell}</td><td>${st}</td></tr>`}).join('');this.setPkgHint('')}
 const sel=this.gid('upgVerSel');
 const vers=[...new Set(this.packages.map(p=>p.version))].sort((a,b)=>this.verCmp(b,a));
 sel.innerHTML=vers.length?vers.map(v=>`<option value="${esc(v)}">${esc(v)}</option>`).join(''):'<option value="">（暂无版本）</option>';
 if(!vers.includes(this.targetVersion)){
  const ready=[...new Set(this.packages.filter(p=>p.ready).map(p=>p.version))].sort((a,b)=>this.verCmp(b,a));
  this.targetVersion=ready[0]||vers[0]||''}
 if(this.targetVersion)sel.value=this.targetVersion;
 this.renderShaInfo();
 this.updateSelBar();this.updateActions();
},
renderShaInfo(){
 const el=this.gid('upgShaInfo');
 if(!this.targetVersion){el.textContent='';return}
 const pkgs=this.packages.filter(p=>p.version===this.targetVersion);
 if(!pkgs.length){el.textContent='';return}
 const parts=pkgs.map(p=>`${p.ready?p.arch+'：'+this.fmtB(p.size)+' · sha256 '+p.sha256.slice(0,16)+'…（就绪）':p.arch+'：'+this.fmtB(p.size)+' · 哈希计算中…'}`);
 el.textContent='目标版本 '+this.targetVersion+' 已关联 '+parts.join('；')+'，发起时自动校验设备端下载哈希';
 el.className='uhint'+(pkgs.some(p=>!p.ready)?' warn':'');
},
upload(file){
 if(this.upXhr)return;
 const fd=new FormData();fd.append('file',file);
 const x=new XMLHttpRequest();this.upXhr=x;
 const bar=this.gid('upgUpBar'),fill=this.gid('upgUpFill'),pct=this.gid('upgUpPct');
 bar.hidden=false;fill.style.width='0%';pct.textContent='上传中 0%';
 x.open('POST','/api/packages/upload');
 x.upload.onprogress=e=>{if(e.lengthComputable){const p=Math.round(e.loaded/e.total*100);fill.style.width=p+'%';pct.textContent='上传中 '+p+'%（'+this.fmtB(e.loaded)+' / '+this.fmtB(e.total)+'）'}};
 x.onload=()=>{this.upXhr=null;bar.hidden=true;let d={};try{d=JSON.parse(x.responseText)}catch(e){}
  if(x.status>=200&&x.status<300){this.setPkgHint('已上传 '+file.name+'，正在刷新包列表…');this.loadPkgs()}
  else this.setPkgHint('上传失败：'+(d.error||('HTTP '+x.status)),true)};
 x.onerror=()=>{this.upXhr=null;bar.hidden=true;this.setPkgHint('上传失败：网络错误',true)};
 x.send(fd);
},
devRows(){return this.fleet.filter(f=>!f.retired).map(f=>({h:f.hostname,site:f.site||'',online:!!f.online,ver:(f.version||'').trim()||'未获取',arch:this.archOf(f),hw:f.hardware||f.hw_model||''}))},
async loadFleet(){
 try{
  const r=await fetch('/api/hosts',{cache:'no-store'});const d=await r.json();
  if(!r.ok)throw Error('HTTP '+r.status);
  this.fleet=d.fleet||[];
  const online=this.fleet.filter(f=>!f.retired&&f.online).map(f=>f.hostname);
  if(!this.selTouched)this.selected=new Set(online);
  else{const live=new Set(online);for(const h of [...this.selected])if(!live.has(h))this.selected.delete(h)}
  this.renderDevs();this.updateSelBar();this.updateActions();
 }catch(e){if(!this.fleet.length){const el=this.gid('upgDevGroups');if(el)el.innerHTML='<p class=uempty>设备数据加载失败（/api/hosts 不可用），稍后自动重试</p>'};this.updateSelBar();this.updateActions()}
},
renderDevs(){
 const wrap=this.gid('upgDevGroups');
 const rows=this.devRows();
 if(!rows.length){wrap.innerHTML='<p class=uempty>暂无设备数据——设备总览接口不可用或未登记设备</p>';return}
 const groups={};
 for(const r of rows)(groups[r.ver]=groups[r.ver]||[]).push(r);
 const keys=Object.keys(groups).sort((a,b)=>a==='未获取'?1:b==='未获取'?-1:this.verCmp(b,a));
 let h='';
 for(const k of keys){
  const g=groups[k].sort((a,b)=>(b.online-a.online)||a.h.localeCompare(b.h,undefined,{numeric:true}));
  const on=g.filter(r=>r.online);const selN=on.filter(r=>this.selected.has(r.h)).length;
  h+=`<div class=vgroup><div class=vghead><input type=checkbox data-grp="${esc(k)}" ${on.length&&selN===on.length?'checked':''} ${on.length?'':'disabled'}><span class=vgt>${esc(k)}</span><span class=vgn>${on.length}/${g.length} 在线${selN?' · 已选 '+selN:''}</span><button type=button class=grpbtn data-grpup="${esc(k)}">全组升级</button></div><div class=vrows>`;
  for(const r of g){
   const tag=r.site?`<span class="stag vsite" style="--c:${siteColor(r.site)}">${esc(r.site)}</span>`:'';
   h+=`<label class="vrow${r.online?'':' off'}" title="${r.online?'':'离线设备不可选'}"><input type=checkbox data-h="${esc(r.h)}" ${this.selected.has(r.h)?'checked':''} ${r.online?'':'disabled'}><span class=bh>${esc(r.h)}</span><small>${esc(r.arch||r.hw||'')}</small>${tag}</label>`}
  h+='</div></div>'
 }
 wrap.innerHTML=h;
},
updateSelBar(){
 const n=this.selected.size;const el=this.gid('upgSelBar');
 el.innerHTML=`已选 <b>${n}</b> 台 · 预计目标版本 <b>${esc(this.targetVersion||'—')}</b>${n?' <button type=button class=minibtn data-clr>清空选择</button>':''}`;
},
canLaunch(){return !this.active()&&!!this.targetVersion&&this.selected.size>0},
updateActions(){
 const busy=this.active();const noVer=!this.targetVersion;const noSel=!this.selected.size;
 let warnArches=[];
 if(this.targetVersion&&this.selected.size){
  const ready=new Set(this.packages.filter(p=>p.ready&&p.version===this.targetVersion).map(p=>p.arch));
  const need=new Set();
  for(const h of this.selected){const f=this.fleet.find(x=>x.hostname===h);const a=f&&this.archOf(f);if(a&&!ready.has(a))need.add(a)}
  warnArches=[...need]
 }
 let hint='',warn=false;
 if(busy)hint='有升级任务进行中（#'+this.job.id+'），任务结束后才能发起新任务（后端并发护栏兜底）';
 else if(warnArches.length){hint='⚠ 目标版本 '+this.targetVersion+' 缺少 '+warnArches.join('、')+' 机型的就绪安装包（未上传或哈希未完成），下载/安装类操作暂不可发起';warn=true}
 else if(noVer)hint='请先在「① 安装包」区选择目标版本（选中后自动关联 sha256 基准）';
 else if(noSel)hint='请至少勾选一台在线设备（默认已勾选全部在线设备，离线设备不可选）';
 const heavy=busy||noVer||noSel||warnArches.length>0;
 this.gid('upgRunAll').disabled=heavy;
 document.querySelectorAll('.stagebtn[data-kind]').forEach(b=>{const k=b.dataset.kind;b.disabled=(k==='download'||k==='install')?heavy:(busy||noVer||noSel)});
 this.setActHint(hint,warn);
},
closeConfirm(){const m=this.gid('upgModal');if(m)m.classList.remove('show')},
showConfirm(){
 const n=this.selected.size;const vs=this.targetVersion;
 const archSplit={};for(const h of this.selected){const f=this.fleet.find(x=>x.hostname===h);const a=(f&&this.archOf(f))||'未知机型';archSplit[a]=(archSplit[a]||0)+1}
 const archTxt=Object.entries(archSplit).map(([a,c])=>esc(a)+' × '+c).join('、');
 const list=[...this.selected].sort((a,b)=>a.localeCompare(b,undefined,{numeric:true}));
 const preview=esc(list.length>12?list.slice(0,12).join('、')+' 等 '+n+' 台':list.join('、'));
 const m=this.gid('upgModalBody');
 m.innerHTML=`<header><div><span class=name>确认发起全流程升级</span></div><button type=button class="x" id=upgConfirmClose>✕</button></header>
 <div class=mgrid>
  <span>目标版本</span><b>${esc(vs)}</b>
  <span>设备数量</span><b>${n} 台（${archTxt}）</b>
  <span>设备清单</span><b>${preview}</b>
  <span>执行流程</span><b>预检 → 下载 → 准备 → 安装 → 验证 → 清理</b>
 </div>
 <p class=uwarn>⚠ 升级将覆盖设备上的程序，过程不可逆；单台失败不影响其他设备，安装器失败时会自动回滚兜底。请确认目标版本与设备清单无误后再发起。</p>
 <div class=ubtns style="justify-content:flex-end;margin-top:16px"><button type=button class=stagebtn id=upgConfirmNo>取消</button><button type=button id=upgConfirmYes>确认发起升级</button></div>`;
 this.gid('upgModal').classList.add('show');
 this.gid('upgConfirmClose').onclick=this.gid('upgConfirmNo').onclick=()=>this.closeConfirm();
 this.gid('upgConfirmYes').onclick=()=>{this.closeConfirm();this.launch('upgrade')};
},
async launch(kind){
 const devices=[...this.selected];
 this.setActHint('正在创建任务…');
 try{
  const r=await fetch('/api/upgrade/jobs',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({kind,devices,params:{version:this.targetVersion}})});
  const d=await r.json().catch(()=>({}));
  if(!r.ok)throw Error(d.error||('HTTP '+r.status));
  const j=d.job||d;const id=j.id!=null?j.id:j.job_id;
  if(id==null)throw Error('响应缺少任务 id');
  this.fromSnap={};for(const h of devices){const f=this.fleet.find(x=>x.hostname===h);if(f)this.fromSnap[h]=f.version||''}
  this.adoptJob({id:id,kind:j.kind||kind,version:j.version||this.targetVersion,status:j.status||'running',created_at:j.created_at||new Date().toISOString(),finished_at:j.finished_at||null,devices:devices});
  this.setActHint('任务 #'+id+' 已创建');
  this.loadJobDetail(id);this.subscribe();
 }catch(e){this.setActHint('发起失败：'+e.message,true)}
},
normJob(j){return{id:j.id!=null?j.id:j.job_id,kind:j.kind||'',version:j.version||(j.params&&j.params.version)||'',status:j.status||'running',created_at:j.created_at||j.createdAt||null,finished_at:j.finished_at||j.finishedAt||null,devices:j.devices||[],summary:j.summary||null}},
normTask(t){return{hostname:t.hostname||t.host||'',stage:String(t.stage||'').toLowerCase(),status:this.normStatus(t.status),error:t.error||t.reason||'',started_at:t.started_at||t.startedAt||null,finished_at:t.finished_at||t.finishedAt||null,from_version:t.from_version||t.version_before||''}},
adoptJob(j){this.job=j;this.tasks=(j.devices||[]).map(h=>({hostname:h,stage:'',status:'pending',error:'',from_version:this.fromSnap[h]||''}));this.logs={};this.logOff={};this.expanded=null;this.renderJob()},
async loadJobs(){
 try{
  const r=await fetch('/api/upgrade/jobs',{cache:'no-store'});const d=await r.json();
  if(!r.ok)throw Error(d.error||('HTTP '+r.status));
  const list=d.jobs||d.items||(Array.isArray(d)?d:[]);
  if(!list.length){if(!this.job)this.renderJob();return}
  const j=this.normJob(list[0]);
  if(j.id==null)return;
  if(!this.job||this.job.id!==j.id){this.adoptJob(j);this.loadJobDetail(j.id)}
  else{this.job=j;this.renderJob()}
  if(this.active())this.subscribe();
 }catch(e){if(!this.job){const el=this.gid('upgJobSummary');if(el)el.innerHTML='<p class=uempty>升级任务接口暂不可用（后端未上线？），发起任务后此处展示逐台进度</p>';this.gid('upgTaskGrid').innerHTML=''}}
},
async loadJobDetail(id){
 try{
  const r=await fetch('/api/upgrade/jobs/'+encodeURIComponent(id),{cache:'no-store'});const d=await r.json();
  if(!r.ok)throw Error(d.error||('HTTP '+r.status));
  const j=this.normJob(d.job||d);
  if(this.job&&this.job.id!==j.id)return;
  this.job=j;
  const ts=(d.tasks||j.tasks||[]).map(t=>this.normTask(t));
  if(ts.length)this.tasks=ts;
  else if((j.devices||[]).length&&!this.tasks.length)this.tasks=j.devices.map(h=>({hostname:h,stage:'',status:'pending',error:'',from_version:''}));
  this.renderJob();
  if(!this.active())this.teardown();
 }catch(e){/*保留已有状态，等待下次刷新/SSE*/}
},
renderJob(){
 this.updateActions();
 const sumEl=this.gid('upgJobSummary'),grid=this.gid('upgTaskGrid');
 if(!this.job){sumEl.innerHTML='<p class=uempty>暂无升级任务——在上方勾选设备发起后，这里展示逐台进度与日志</p>';grid.innerHTML='';return}
 const j=this.job;const st=this.normStatus(j.status);
 const cnt={success:0,failed:0,blocked:0,running:0,pending:0};
 for(const t of this.tasks)cnt[t.status]=(cnt[t.status]||0)+1;
 const total=this.tasks.length||((j.devices||[]).length)||((j.summary&&j.summary.total)||0);
 const created=this.parseTs(j.created_at);const fin=this.parseTs(j.finished_at);
 const end=fin||(st==='running'?Date.now():null);
 const el=(created&&end)?(end-created)/1000:null;
 sumEl.innerHTML=`<b>#${esc(j.id)}</b><span>${esc(this.KIND_LBL[j.kind]||j.kind||'任务')} → <b>${esc(j.version||'—')}</b></span><span class="tstat ${st}">${this.STAT_LBL[st]||esc(st)}</span><span>共 <b>${total}</b> 台</span><span>成功 <b style="color:#15803d">${cnt.success}</b></span><span>失败 <b style="color:#b91c1c">${cnt.failed}</b></span><span>阻断 <b style="color:#b45309">${cnt.blocked}</b></span><span>进行中 <b style="color:#1d4ed8">${cnt.running}</b></span><span>等待 <b>${cnt.pending}</b></span>${el!=null?`<span>耗时 <b>${this.fmtDur2(el)}</b></span>`:''}${created?`<span>发起于 ${esc(this.fmtClock(created))}</span>`:''}`;
 grid.innerHTML=this.tasks.length?this.tasks.map(t=>this.cardHtml(t)).join(''):(this.active()?'<p class=uempty>任务已创建，等待设备状态上报…</p>':'');
},
cardHtml(t){
 const h=t.hostname;const f=this.fleet.find(x=>x.hostname===h);
 const site=(f&&f.site)||'';const tag=site?`<span class="stag" style="--c:${siteColor(site)}">${esc(site)}</span>`:'';
 const from=t.from_version||this.fromSnap[h]||((f&&f.version)||'');
 const to=this.job.version||this.targetVersion||'';
 const st=t.status;
 const cls=st==='success'?'done':st==='failed'?'failed':st==='blocked'?'blocked':'';
 const scopeIdx=this.job.kind==='upgrade'?null:this.STAGES.indexOf(this.job.kind);
 const curIdx=this.STAGES.indexOf(t.stage);
 let steps='';
 for(let i=0;i<this.STAGES.length;i++){
  let c='';
  if(scopeIdx!=null&&i!==scopeIdx)c='na';
  else if(st==='success')c='done';
  else if(st==='failed'||st==='blocked')c=i===curIdx?st:(curIdx>=0&&i<curIdx?'done':'');
  else if(st==='running')c=i===curIdx?'cur':(curIdx>=0&&i<curIdx?'done':'');
  steps+=`<span class="step ${c}">${this.STAGE_LBL[this.STAGES[i]]}</span>`
 }
 const errBox=(st==='failed'||st==='blocked')&&t.error?`<div class="terr ${st}">${esc(st==='blocked'?'阻断：':'失败：')}${esc(t.error)}</div>`:'';
 return `<article class="ucard ${cls}${this.expanded===h?' expanded':''}" data-h="${esc(h)}">
 <div class=uh><span class=name>${esc(h)}</span>${tag}<span class="tstat ${st}">${this.STAT_LBL[st]||esc(st)}</span></div>
 <div class=uver>版本：${esc(from||'—')} → <b>${esc(to||'—')}</b></div>
 <div class=stepper>${steps}</div>
 ${errBox}
 <div class=ulogwrap><div class=ulogbar><span>实时日志（SSE 增量，点击卡片收起）</span><button type=button data-fl="${esc(h)}">${this.follow[h]===false?'恢复跟随':'暂停跟随'}</button></div><pre class=ulog>${esc(this.logs[h]||'')}</pre></div>
 </article>`;
},
logDom(h){try{return document.querySelector('.ucard[data-h="'+(window.CSS&&CSS.escape?CSS.escape(h):h)+'"] .ulog')}catch(e){return null}},
toggleLog(h){
 this.expanded=this.expanded===h?null:h;
 this.renderJob();
 if(this.expanded===h&&this.job)this.fetchLog(h);
},
appendLog(h,txt){
 if(h==null||txt==null)return;let s=String(txt);if(!s)return;
 if(!this.logs[h])this.logs[h]='';
 if(!s.endsWith('\\n'))s+='\\n';
 this.logs[h]+=s;
 if(this.expanded===h){const pre=this.logDom(h);if(pre){pre.textContent=this.logs[h];if(this.follow[h]!==false)pre.scrollTop=pre.scrollHeight}}
},
async fetchLog(h){
 if(!this.job||this.job.id==null)return;
 const off=this.logOff[h]||0;
 try{
  const r=await fetch('/api/upgrade/jobs/'+encodeURIComponent(this.job.id)+'/log?offset='+off+'&hostname='+encodeURIComponent(h),{cache:'no-store'});
  const d=await r.json();
  if(!r.ok)return;
  let txt=d.text||d.chunk||d.data||(Array.isArray(d.lines)?d.lines.join('\\n'):'');
  if(!txt)return;
  if(d.offset!=null)this.logOff[h]=d.offset;else this.logOff[h]=off+String(txt).length;
  this.appendLog(h,txt);
 }catch(e){}
},
setConn(s){this.connState=s;const el=this.gid('upgConn');if(!el)return;el.className='connchip '+s;el.textContent=s==='live'?'● 实时连接正常':s==='poll'?'○ 实时连接断开 · 轮询中':'未连接'},
subscribe(){
 if(!this.active())return;
 if(this.es&&this.es.readyState<=1)return;
 try{this.es=new EventSource('/api/upgrade/events')}catch(e){this.degrade();return}
 this.es.onopen=()=>{this.setConn('live');if(this.pollT){clearInterval(this.pollT);this.pollT=null}};
 this.es.onmessage=e=>this.onEvent('message',e.data);
 for(const n of['job','task','log','stage','progress'])this.es.addEventListener(n,e=>this.onEvent(n,e.data));
 this.es.onerror=()=>this.degrade();
},
degrade(){this.setConn('poll');if(!this.pollT&&(this.active()||this.expanded!=null))this.pollT=setInterval(()=>this.pollTick(),5000)},
pollTick(){if(!this.job)return;this.loadJobDetail(this.job.id);if(this.expanded!=null)this.fetchLog(this.expanded)},
teardown(){if(this.es){this.es.close();this.es=null}if(this.pollT){clearInterval(this.pollT);this.pollT=null}this.setConn('off')},
onEvent(name,raw){
 let m=null;const s=String(raw==null?'':raw);
 if(s.trim().startsWith('{')){try{m=JSON.parse(s)}catch(e){m=null}}
 if(!m)return;
 const type=String(m.type||name||'').toLowerCase();
 if(type==='job'||m.job){
  const j=this.normJob(m.job||m);
  if(j.id==null)return;
  if(!this.job||this.job.id!==j.id)this.adoptJob(j);else{this.job=j}
  this.renderJob();
  if(!this.active()){this.teardown();this.loadJobDetail(j.id)}
  return}
 if(type==='task'||m.task){this.applyTask(this.normTask(m.task||m));return}
 if(type==='log'||m.text||m.chunk||m.lines){
  const h=m.hostname;if(h!=null){let txt=m.text||m.chunk||(Array.isArray(m.lines)?m.lines.join('\\n'):'');if(m.offset!=null)this.logOff[h]=m.offset;this.appendLog(h,txt)}
  return}
 if(m.hostname&&(m.stage!=null||m.status!=null))this.applyTask(this.normTask(m));
},
applyTask(t){
 if(!t.hostname)return;
 if(!this.job||!this.tasks.length)this.tasks.push(t);
 const i=this.tasks.findIndex(x=>x.hostname===t.hostname);
 if(i>=0)this.tasks[i]=Object.assign({},this.tasks[i],t);else this.tasks.push(t);
 this.renderJob();
}
};
Upg.init();"""


def _json_request(url, *, method="GET", data=None, headers=None, timeout=4):
    request = Request(url, method=method, data=data, headers=headers or {})
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _setup_status_probe(base):
    """最轻的探测:/api/setup/status 无需登录,给出版本+身份(无硬件/网络信息)。"""
    setup = _json_request(base + "/api/setup/status", timeout=4)
    if not setup.get("release_version"):
        raise ValueError("setup/status has no release_version")
    return {
        "release_version": str(setup.get("release_version") or ""),
        "device_id": str(setup.get("device_id") or ""),
        "device_name": str(setup.get("device_name") or ""),
        "hostname": str(setup.get("hostname") or setup.get("device_hostname") or ""),
        "source": "setup/status",
    }


def _primary_ipv4(network):
    """取有线网卡的第一个 IPv4;优先 carrier/up 的网卡,否则第一个有地址的。"""
    fallback = ("", "")
    for nic in (network or {}).get("ethernet_interfaces") or []:
        if not isinstance(nic, dict):
            continue
        addrs = [a for a in (nic.get("addresses") or []) if isinstance(a, str) and ":" not in a]
        if not addrs:
            continue
        cand = (addrs[0], str(nic.get("name") or ""))
        if nic.get("carrier") or str(nic.get("operstate") or "").casefold() == "up":
            return cand
        if not fallback[0]:
            fallback = cand
    return fallback


def _about_probe(base):
    """/api/system/about:需要 guest token,但一次给齐版本/身份/CPU 型号/网卡 IP。"""
    access = _json_request(
        base + "/api/auth/guest",
        method="POST",
        data=b"{}",
        headers={"Content-Type": "application/json"},
        timeout=4,
    )
    access_token = str(access.get("access_token") or "")
    if not access_token:
        raise ValueError("device did not return a guest access token")
    about = _json_request(
        base + "/api/system/about",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=4,
    )
    identity = about.get("identity") if isinstance(about.get("identity"), dict) else {}
    cpu = about.get("cpu") if isinstance(about.get("cpu"), dict) else {}
    info = {
        "release_version": str(about.get("release_version") or ""),
        "device_id": str(identity.get("device_id") or ""),
        "device_name": str(identity.get("device_name") or ""),
        "hostname": str(identity.get("hostname") or ""),
        "hw_model": str(about.get("cpu_model") or cpu.get("model") or about.get("device_model") or ""),
        "source": "system/about",
    }
    ip, iface = _primary_ipv4(about.get("network"))
    if ip:
        info["ip"], info["ip_iface"] = ip, iface
    return info


def _duration_summary_probe(base, from_ms, to_ms):
    """采集时长数据源：/api/auth/guest 换 token 后查 /api/tasks/duration-summary。

    与设备门户「今日总计」用的是同一接口；窗口边界为 epoch 毫秒，
    返回 total_duration_ms（毫秒）/ total_session_count（段数）。
    """
    access = _json_request(
        base + "/api/auth/guest",
        method="POST",
        data=b"{}",
        headers={"Content-Type": "application/json"},
        timeout=4,
    )
    access_token = str(access.get("access_token") or "")
    if not access_token:
        raise ValueError("device did not return a guest access token")
    summary = _json_request(
        base + f"/api/tasks/duration-summary?from={from_ms}&to={to_ms}",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=COLLECT_DURATION_TIMEOUT,
    )
    ms = summary.get("total_duration_ms")
    if not isinstance(ms, (int, float)) or ms < 0:
        raise ValueError("duration-summary has no total_duration_ms")
    return int(ms), int(summary.get("total_session_count") or 0)


def get_device_version(port, want_hw=False):
    """Read device information through the FRP web proxy.

    The SSH FRP proxy cannot be used for HTTP: it terminates at port 22 on the
    device.  Each device therefore needs a second ``<name>-web`` proxy to its
    local portal port (normally 80).  ``/api/setup/status`` is intentionally
    used first because it is available before login and is the same endpoint
    used by the device login page; it is cheap but carries no hardware
    information, so ``want_hw`` switches to ``/api/system/about`` (one guest
    token + one request) which also reports the CPU model and NIC addresses.
    """
    base = f"http://127.0.0.1:{int(port)}"
    errors = []
    if want_hw:
        try:
            info = _about_probe(base)
            if info.get("release_version"):
                return info
            errors.append("system/about has no release_version")
        except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
            errors.append(str(exc))
    try:
        return _setup_status_probe(base)
    except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
        errors.append(str(exc))
    if not want_hw:
        try:
            return _about_probe(base)
        except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
            errors.append(str(exc))
    raise ValueError("; ".join(e for e in errors if e) or "cannot read device information")


def natural_key(value):
    return [int(x) if x.isdigit() else x.casefold() for x in re.split(r"(\d+)", value or "")]


# Snapshot + per-device version cache.  Probing every device through the FRP
# tunnel takes seconds, so it runs on a background thread; ``/api/hosts`` just
# returns the latest snapshot.  Version results are cached per device (keyed by
# hostname + web port + frps start time, so a device restart invalidates it).
_version_cache = {}
_hw_probed_at = {}  # hostname -> 上次硬件/IP 探测时间(内存态;持久值在 devices.hw_model)
_snapshot = None
_snapshot_ts = 0.0
_snapshot_lock = threading.Lock()


def _cache_key(item):
    return (item["hostname"], item.get("web_port"), item.get("started") or "")


def _cached_result(item):
    hit = _version_cache.get(_cache_key(item))
    if hit and hit[0] > time.time():
        return hit[1]
    return None


def _needs_hw_probe(hostname):
    """是否需要用 about 接口补型号/IP。

    已探测过的按 HW_TTL 刷新;进程重启后,DB 里已有型号的机器不重复探测,
    避免每次重启都对所有设备发一轮 about 请求。
    """
    last = _hw_probed_at.get(hostname)
    if last is not None:
        return (time.time() - last) >= HW_TTL
    known = store.roster.get(hostname) or {}
    if known.get("hw_model") or known.get("ip"):
        _hw_probed_at[hostname] = time.time()
        return False
    return True


def _store_result(item, result):
    ttl = VERSION_TTL if result and result[0] else VERSION_ERROR_TTL
    _version_cache[_cache_key(item)] = (time.time() + ttl, result)
    if len(_version_cache) > 2000:  # 键含 frpc 上线时刻,长期运行会累积,定期清过期项
        now = time.time()
        for key in [k for k, v in _version_cache.items() if v[0] < now]:
            _version_cache.pop(key, None)


def enrich_device_info(item):
    cached = _cached_result(item)
    if cached is not None:
        item["device"], item["version_error"] = cached
        item["version"] = item["device"].get("release_version", "") if item["device"] else ""
        return item
    want_hw = _needs_hw_probe(item["hostname"])
    try:
        item["device"] = get_device_version(item["web_port"], want_hw=want_hw)
        item["version"] = item["device"].get("release_version", "")
        item["version_error"] = ""
        if want_hw:
            _hw_probed_at[item["hostname"]] = time.time()
        _store_result(item, (item["device"], ""))
    except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
        item["device"] = None
        item["version"] = ""
        item["version_error"] = str(exc)
        _store_result(item, (None, str(exc)))
    return item


def enrich_versions(proxies):
    with ThreadPoolExecutor(max_workers=min(32, max(1, len(proxies)))) as pool:
        jobs = {pool.submit(enrich_device_info, item): item for item in proxies if item.get("web_port")}
        for job in as_completed(jobs):
            job.result()
    return proxies


def get_proxies():
    request = Request(FRPS_API)
    token = base64.b64encode(f"{FRPS_USER}:{FRPS_PASSWORD}".encode()).decode()
    request.add_header("Authorization", f"Basic {token}")
    with urlopen(request, timeout=2) as response:
        payload = json.loads(response.read().decode("utf-8"))
    proxies = {}
    for item in payload.get("proxies", []):
        conf = item.get("conf") or {}
        proxy = str(item.get("name") or conf.get("name") or "")
        if not proxy or item.get("status") != "online":
            continue
        is_web = proxy.endswith("-web")
        hostname = proxy.removesuffix("-web").casefold()
        # frpc with a `user` set reports proxies as "<USER>.<hostname>"; when
        # both parts are the same device name, collapse to the canonical
        # hostname so the device merges with its existing entry instead of
        # appearing twice (e.g. "ep-000102.ep-000102").
        head, sep, tail = hostname.partition(".")
        if sep and tail and head == tail:
            hostname = tail
        entry = proxies.setdefault(hostname, {"hostname": hostname, "proxy": hostname, "type": device_type(hostname), "port": None, "web_port": None, "connections": 0, "started": "", "status": "online", "version": "", "version_error": "", "device": None})
        if is_web:
            entry["web_port"] = conf.get("remotePort")
        else:
            entry.update({"port": conf.get("remotePort"), "connections": item.get("curConns", 0), "started": item.get("lastStartTime", "")})
    proxies = list(proxies.values())
    proxies.sort(key=lambda x: natural_key(x["hostname"]))
    return enrich_versions(proxies)


def refresh_snapshot():
    global _snapshot, _snapshot_ts
    started = time.time()
    try:
        data = get_proxies()
    except Exception as exc:
        store.record([], int((time.time() - started) * 1000), str(exc))
        raise
    with _snapshot_lock:
        _snapshot, _snapshot_ts = data, time.time()
    store.record(data, int((time.time() - started) * 1000), "")


def get_snapshot():
    global _snapshot, _snapshot_ts
    with _snapshot_lock:
        if _snapshot is None:
            data = get_proxies()
            _snapshot, _snapshot_ts = data, time.time()
        return _snapshot, _snapshot_ts


def _parse_frp_started(value):
    """frps lastStartTime is "MM-DD HH:MM:SS" (no year)."""
    if not value:
        return None
    try:
        return datetime.strptime(str(value), "%m-%d %H:%M:%S").replace(year=datetime.now().year)
    except ValueError:
        return None


class Store:
    """MySQL history store (devices / host_status / host_events / refresh_batches).

    Degrades gracefully: any db problem only disables persistence, never the
    live dashboard.  A single RLock serializes writes; reads use the same lock
    so record()/events()/uptime() never interleave on one connection.
    """

    def __init__(self):
        self.pymysql = None
        self.conn = None
        self.lock = threading.RLock()
        self.enabled = False
        self.state = {}  # hostname -> {"online": bool, "version": str}
        self.roster = {}  # hostname -> {"site", "owner", "hardware", "retired"}
        self._last_snapshot_at = 0.0
        self._last_retention_at = 0.0
        self._last_roster_at = 0.0
        try:
            import pymysql
            self.pymysql = pymysql
        except ImportError:
            print("frp-monitor: python3-pymysql not installed, history persistence disabled", flush=True)
            return
        if not DB_PASSWORD:
            print("frp-monitor: FRP_MONITOR_DB_PASSWORD not set, history persistence disabled", flush=True)
            return
        if self._connect():
            self.enabled = True
            self._migrate()
            self._load_state()
            self._load_roster()
            print(f"frp-monitor: history persistence enabled "
                  f"({DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}, {len(self.state)} known hosts)", flush=True)

    def _connect(self):
        try:
            self.conn = self.pymysql.connect(
                host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD,
                database=DB_NAME, autocommit=True, charset="utf8mb4")
            return True
        except Exception as exc:
            print(f"frp-monitor: db connect failed: {exc}", flush=True)
            self.conn = None
            return False

    def _execute(self, sql, args=(), fetch=None):
        if not self.enabled:
            return None
        with self.lock:
            for attempt in (1, 2):
                try:
                    if self.conn is None and not self._connect():
                        return None
                    with self.conn.cursor() as cur:
                        cur.execute(sql, args or None)
                        if fetch == "all":
                            return cur.fetchall()
                        if fetch == "one":
                            return cur.fetchone()
                        return cur.rowcount
                except Exception as exc:
                    try:
                        if self.conn is not None:
                            self.conn.close()
                    except Exception:
                        pass
                    self.conn = None
                    if attempt == 2:
                        print(f"frp-monitor: db error: {exc}", flush=True)
                        return None

    def _migrate(self):
        """给老库补上自动探测用的列与新表（幂等）。"""
        # 设备采集时长：每天 19:00/21:00 拉在线设备当日累计采集时长入库
        self._execute(
            "CREATE TABLE IF NOT EXISTS collect_duration ("
            "hostname VARCHAR(64) NOT NULL, "
            "stat_date DATE NOT NULL, "
            "duration_sec INT NULL, "
            "sessions INT NOT NULL DEFAULT 0, "
            "run_tag VARCHAR(16) NOT NULL DEFAULT '', "
            "error VARCHAR(255) NOT NULL DEFAULT '', "
            "pulled_at DATETIME NULL, "
            "UNIQUE KEY uq_host_date (hostname, stat_date)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
        rows = self._execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=DATABASE() AND table_name='devices'", fetch="all")
        cols = {r[0] for r in rows or []}
        if "hw_model" not in cols:
            self._execute("ALTER TABLE devices ADD COLUMN hw_model VARCHAR(64) NOT NULL DEFAULT ''")
        if "ip" not in cols:
            self._execute("ALTER TABLE devices ADD COLUMN ip VARCHAR(64) NOT NULL DEFAULT ''")

    def _load_roster(self):
        rows = self._execute(
            "SELECT hostname, IFNULL(site,''), IFNULL(owner,''), IFNULL(hardware,''), retired, "
            "IFNULL(hw_model,''), IFNULL(ip,'') FROM devices",
            fetch="all")
        self.roster = {r[0]: {"site": r[1], "owner": r[2], "hardware": r[3], "retired": bool(r[4]),
                              "hw_model": r[5], "ip": r[6]}
                       for r in rows or []}

    def _load_state(self):
        """Baseline online/version state so a monitor restart does not emit
        bogus online/offline events."""
        rows = self._execute(
            "SELECT s.hostname, s.online, s.release_version FROM host_status s "
            "JOIN (SELECT hostname, MAX(ts) AS mts FROM host_status GROUP BY hostname) t "
            "ON s.hostname = t.hostname AND s.ts = t.mts",
            fetch="all")
        for hostname, online, version in rows or []:
            self.state[hostname] = {"online": bool(online), "version": version or ""}

    def _event(self, ts, hostname, event_type, old="", new="", detail=""):
        self._execute(
            "INSERT INTO host_events (hostname, ts, event_type, old_value, new_value, detail) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (hostname, ts, event_type, str(old)[:250], str(new)[:250], str(detail)[:500]))

    def _upsert_device(self, ts, item):
        device = item.get("device") or {}
        hw_model = str(device.get("hw_model") or "")[:64]
        ip = str(device.get("ip") or "")[:64]
        rowcount = self._execute(
            "INSERT INTO devices (hostname, device_id, device_name, release_version, "
            "ssh_port, web_port, hw_model, ip, hardware, first_seen_at, last_seen_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE device_id=VALUES(device_id), device_name=VALUES(device_name), "
            "release_version=VALUES(release_version), ssh_port=VALUES(ssh_port), "
            "web_port=VALUES(web_port), "
            "hw_model=IF(VALUES(hw_model)<>'', VALUES(hw_model), hw_model), "
            "ip=IF(VALUES(ip)<>'', VALUES(ip), ip), "
            "hardware=IF(hardware='' AND VALUES(hardware)<>'', VALUES(hardware), hardware), "
            "first_seen_at=COALESCE(first_seen_at, VALUES(first_seen_at)), "
            "last_seen_at=VALUES(last_seen_at)",
            (item["hostname"], str(device.get("device_id") or "")[:32],
             str(device.get("device_name") or "")[:64], item.get("version") or "",
             item.get("port"), item.get("web_port"), hw_model, ip,
             _hardware_category(hw_model), ts, ts))
        return rowcount == 1  # 1 = inserted (first seen), 2 = updated

    def record(self, hosts, duration_ms, error=""):
        """Persist one refresh cycle: heartbeat + devices + events + snapshot."""
        if not self.enabled:
            return
        with self.lock:
            now = datetime.now()
            online_hosts = {item["hostname"]: item for item in hosts}
            self._execute(
                "INSERT INTO refresh_batches (ts, total, online, duration_ms, error) "
                "VALUES (%s, %s, %s, %s, %s)",
                (now, len(hosts), len(online_hosts), duration_ms, error[:250]))
            for hostname, item in online_hosts.items():
                version = item.get("version") or ""
                prev = self.state.get(hostname)
                try:
                    is_new = self._upsert_device(now, item)
                except Exception:
                    is_new = False
                if is_new:
                    self._event(now, hostname, "first_seen", "", version,
                                "auto-registered from frps proxy list")
                elif prev is None or not prev.get("online"):
                    self._event(now, hostname, "online", "", version)
                if prev and version and prev.get("version") and version != prev.get("version"):
                    self._event(now, hostname, "version_change", prev.get("version"), version)
                self.state[hostname] = {"online": True, "version": version}
            for hostname, prev in list(self.state.items()):
                if hostname not in online_hosts and prev.get("online"):
                    self._event(now, hostname, "offline", prev.get("version"), "")
                    self.state[hostname] = {"online": False, "version": prev.get("version", "")}
            if time.time() - self._last_snapshot_at >= SNAPSHOT_EVERY:
                self._last_snapshot_at = time.time()
                for hostname in set(online_hosts) | set(self.state):
                    item = online_hosts.get(hostname)
                    if item:
                        device = item.get("device") or {}
                        row = (hostname, now, 1, item.get("port"), item.get("web_port"),
                               item.get("connections") or 0, _parse_frp_started(item.get("started")),
                               item.get("version") or "", (item.get("version_error") or "")[:250],
                               str(device.get("device_id") or "")[:32], str(device.get("device_name") or "")[:64])
                    else:
                        prev = self.state.get(hostname) or {}
                        row = (hostname, now, 0, None, None, 0, None,
                               prev.get("version", ""), "", "", "")
                    self._execute(
                        "INSERT INTO host_status (hostname, ts, online, ssh_port, web_port, "
                        "connections, frpc_started_at, release_version, version_error, device_id, device_name) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", row)
            if time.time() - self._last_retention_at >= 3600:
                self._last_retention_at = time.time()
                self._execute("DELETE FROM host_status WHERE ts < NOW() - INTERVAL %s DAY",
                              (STATUS_RETENTION_DAYS,))
                self._execute("DELETE FROM refresh_batches WHERE ts < NOW() - INTERVAL %s DAY",
                              (BATCH_RETENTION_DAYS,))
                self._load_roster()

    def _uptime(self, hostname, hours):
        row = self._execute(
            "SELECT SUM(online), COUNT(*) FROM host_status WHERE hostname=%s AND ts >= %s",
            (hostname, datetime.now() - timedelta(hours=hours)), fetch="one")
        if not row or not row[1]:
            return None
        return round(float(row[0] or 0) / int(row[1]), 4)

    def devices_view(self):
        rows = self._execute(
            "SELECT hostname, IFNULL(site,''), IFNULL(owner,''), IFNULL(hardware,''), retired, "
            "IFNULL(hw_model,''), IFNULL(ip,''), "
            "IFNULL(device_id,''), IFNULL(device_name,''), IFNULL(release_version,''), ssh_port, web_port, "
            "IFNULL(DATE_FORMAT(first_seen_at,'%Y-%m-%d %H:%i'),''), IFNULL(DATE_FORMAT(last_seen_at,'%Y-%m-%d %H:%i'),'') "
            "FROM devices", fetch="all")
        keys = ("hostname", "site", "owner", "hardware", "retired", "hw_model", "ip",
                "device_id", "device_name", "release_version", "ssh_port", "web_port",
                "first_seen", "last_seen")
        return [dict(zip(keys, r)) for r in rows or []]

    def device_detail(self, hostname, online, snapshot=None):
        device = self.devices_view_one(hostname)
        if device is None:
            return None
        device["online"] = online
        if snapshot:
            device["release_version"] = snapshot.get("version") or device["release_version"]
            device["version_error"] = snapshot.get("version_error") or ""
            device["connections"] = snapshot.get("connections")
            device["started"] = snapshot.get("started") or ""
        device["uptime_24h"] = self._uptime(hostname, 24)
        device["uptime_7d"] = self._uptime(hostname, 168)
        rows = self._execute(
            "SELECT event_type, old_value, new_value, detail, DATE_FORMAT(ts,'%%Y-%%m-%%d %%H:%%i:%%s') "
            "FROM host_events WHERE hostname=%s ORDER BY ts DESC, id DESC LIMIT 50",
            (hostname,), fetch="all")
        device["events"] = [{"event_type": r[0], "old_value": r[1], "new_value": r[2],
                             "detail": r[3], "ts": r[4]} for r in rows or []]
        rows = self._execute(
            "SELECT DATE_FORMAT(ts,'%%Y-%%m-%%d %%H:%%i'), online, release_version, connections "
            "FROM host_status WHERE hostname=%s ORDER BY ts DESC LIMIT 12", (hostname,), fetch="all")
        device["status"] = [{"ts": r[0], "online": bool(r[1]), "release_version": r[2] or "",
                             "connections": r[3]} for r in rows or []]
        return device

    def devices_view_one(self, hostname):
        rows = self._execute(
            "SELECT hostname, IFNULL(site,''), IFNULL(owner,''), IFNULL(hardware,''), retired, "
            "IFNULL(hw_model,''), IFNULL(ip,''), "
            "IFNULL(device_id,''), IFNULL(device_name,''), IFNULL(release_version,''), ssh_port, web_port, "
            "IFNULL(DATE_FORMAT(first_seen_at,'%%Y-%%m-%%d %%H:%%i'),''), IFNULL(DATE_FORMAT(last_seen_at,'%%Y-%%m-%%d %%H:%%i'),'') "
            "FROM devices WHERE hostname=%s", (hostname,), fetch="all")
        keys = ("hostname", "site", "owner", "hardware", "retired", "hw_model", "ip",
                "device_id", "device_name", "release_version", "ssh_port", "web_port",
                "first_seen", "last_seen")
        return dict(zip(keys, rows[0])) if rows else None

    def record_collect_duration(self, hostname, stat_date, duration_sec, sessions, run_tag, error, pulled_at):
        """采集时长覆盖语义：成功则覆盖同日早前值（21:00 覆盖 19:00）；
        失败只更新 error，不动已有成功值（duration_sec/run_tag/pulled_at 保留）。"""
        if not self.enabled:
            return
        if error:
            self._execute(
                "INSERT INTO collect_duration "
                "(hostname, stat_date, duration_sec, sessions, run_tag, error, pulled_at) "
                "VALUES (%s, %s, NULL, 0, '', %s, NULL) "
                "ON DUPLICATE KEY UPDATE error=VALUES(error)",
                (hostname, stat_date, error[:250]))
        else:
            self._execute(
                "INSERT INTO collect_duration "
                "(hostname, stat_date, duration_sec, sessions, run_tag, error, pulled_at) "
                "VALUES (%s, %s, %s, %s, %s, '', %s) "
                "ON DUPLICATE KEY UPDATE duration_sec=VALUES(duration_sec), "
                "sessions=VALUES(sessions), run_tag=VALUES(run_tag), "
                "error='', pulled_at=VALUES(pulled_at)",
                (hostname, stat_date, duration_sec, sessions, run_tag, pulled_at))

    def collect_duration_view(self, days):
        """查询视图：items[{hostname, today_minutes, history[{date, minutes}]}]。

        minutes = round(duration_sec/60)；无记录的日期/设备为 None（前端显示 —）。
        history 按日期从近到远，不含今天；设备集合 = devices 登记表 ∪ 已有记录。"""
        today = datetime.now().date()
        dates = [today - timedelta(days=i) for i in range(days)]  # dates[0] = 今天
        rows = self._execute(
            "SELECT hostname, stat_date, duration_sec FROM collect_duration "
            "WHERE stat_date BETWEEN %s AND %s", (dates[-1], today), fetch="all")
        by_host = {}
        for hostname, stat_date, duration_sec in rows or []:
            by_host.setdefault(hostname, {})[str(stat_date)] = duration_sec
        hostnames = {d["hostname"] for d in self.devices_view()} | set(by_host)
        items = []
        for hostname in sorted(hostnames, key=natural_key):
            rec = by_host.get(hostname) or {}
            today_sec = rec.get(str(today))
            items.append({
                "hostname": hostname,
                "today_minutes": None if today_sec is None else round(today_sec / 60),
                "history": [{"date": str(day),
                             "minutes": None if rec.get(str(day)) is None else round(rec[str(day)] / 60)}
                            for day in dates[1:]],
            })
        return items

    def events(self, hours, limit):
        rows = self._execute(
            "SELECT hostname, DATE_FORMAT(ts, '%%Y-%%m-%%d %%H:%%i:%%s'), event_type, "
            "old_value, new_value, detail FROM host_events WHERE ts >= %s "
            "ORDER BY ts DESC, id DESC LIMIT %s",
            (datetime.now() - timedelta(hours=hours), limit), fetch="all")
        return [{"hostname": r[0], "ts": r[1], "event_type": r[2], "old_value": r[3],
                 "new_value": r[4], "detail": r[5]} for r in rows or []]

    def uptime(self, hours):
        rows = self._execute(
            "SELECT hostname, SUM(online), COUNT(*), MAX(release_version), "
            "DATE_FORMAT(MIN(ts), '%%Y-%%m-%%d %%H:%%i:%%s'), DATE_FORMAT(MAX(ts), '%%Y-%%m-%%d %%H:%%i:%%s') "
            "FROM host_status WHERE ts >= %s GROUP BY hostname ORDER BY hostname",
            (datetime.now() - timedelta(hours=hours),), fetch="all")
        out = []
        for hostname, online_sum, samples, version, first, last in rows or []:
            out.append({"hostname": hostname,
                        "uptime": round(float(online_sum or 0) / samples, 4) if samples else None,
                        "samples": int(samples), "release_version": version or "",
                        "first_ts": first, "last_ts": last})
        return out


store = Store()


def _refresh_loop():
    while True:
        try:
            refresh_snapshot()
        except Exception:
            pass
        time.sleep(REFRESH_INTERVAL)


# --- 设备采集时长：拉取与定时调度 ---------------------------------------------
def _parse_collect_times(spec):
    """解析 FRP_MONITOR_COLLECT_TIMES（"19:00,21:00"）为排序去重的时间点。"""
    slots = set()
    for part in (spec or "").split(","):
        hh, sep, mm = part.strip().partition(":")
        if not sep:
            continue
        try:
            slot = (int(hh), int(mm))
        except ValueError:
            continue
        if 0 <= slot[0] <= 23 and 0 <= slot[1] <= 59:
            slots.add(slot)
    return sorted(slots) or [(19, 0), (21, 0)]


COLLECT_SLOTS = _parse_collect_times(COLLECT_TIMES)
_collect_run_lock = threading.Lock()  # 手动触发与定时调度串行，避免同日并发写


def _next_collect_slot(now=None):
    """下一个调度时刻（服务器本地时间）及其 run_tag。

    每轮都从墙上时钟重新推算，进程重启后自然对齐下一次 19:00/21:00，
    不依赖内存计数器，因此重启不会丢失或重复调度。"""
    now = now or datetime.now()
    for hh, mm in COLLECT_SLOTS:
        cand = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if cand > now:
            return cand, f"{hh:02d}:{mm:02d}"
    hh, mm = COLLECT_SLOTS[0]
    return (now + timedelta(days=1)).replace(hour=hh, minute=mm, second=0, microsecond=0), f"{hh:02d}:{mm:02d}"


def run_collect_duration(tag):
    """拉取当时在线设备的当日采集时长并入库。

    窗口为服务器本地时间的当日 0 点到次日 0 点（epoch 毫秒）；
    单台失败只记 error 并继续，返回汇总结果。"""
    with _collect_run_lock:
        hosts, _ = get_snapshot()
        now = datetime.now()
        day0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
        from_ms = int(day0.timestamp() * 1000)
        to_ms = from_ms + 86400000
        stat_date, pulled_at = now.date(), now
        targets = [h for h in hosts if h.get("web_port")]

        def pull(item):
            hostname = item["hostname"]
            try:
                ms, sessions = _duration_summary_probe(
                    f"http://127.0.0.1:{int(item['web_port'])}", from_ms, to_ms)
            except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
                store.record_collect_duration(hostname, stat_date, None, 0, "", str(exc), None)
                return {"hostname": hostname, "ok": False, "error": str(exc)}
            store.record_collect_duration(hostname, stat_date, ms // 1000, sessions, tag, "", pulled_at)
            return {"hostname": hostname, "ok": True, "duration_sec": ms // 1000, "sessions": sessions}

        results = []
        if targets:
            with ThreadPoolExecutor(max_workers=min(16, len(targets))) as pool:
                for job in [pool.submit(pull, item) for item in targets]:
                    results.append(job.result())
        ok = [r for r in results if r["ok"]]
        return {"enabled": store.enabled, "tag": tag, "stat_date": str(stat_date),
                "online": len(hosts), "pulled": len(targets), "ok": len(ok),
                "failed": len(results) - len(ok),
                "errors": [r for r in results if not r["ok"]][:10]}


def _collect_duration_loop():
    while True:
        deadline, tag = _next_collect_slot()
        while True:  # 分段睡眠到点，兼顾长时间休眠与时钟漂移
            remaining = (deadline - datetime.now()).total_seconds()
            if remaining <= 0:
                break
            time.sleep(min(30.0, remaining))
        try:
            summary = run_collect_duration(tag)
            print(f"frp-monitor: collect-duration {tag} run: "
                  f"{summary['ok']}/{summary['pulled']} ok", flush=True)
        except Exception as exc:
            print(f"frp-monitor: collect-duration {tag} run failed: {exc}", flush=True)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def send_json(self, status, data):
        body = json.dumps(data, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _authorized(self):
        if not PANEL_USER or not PANEL_PASSWORD:
            return True
        header = self.headers.get("Authorization") or ""
        if not header.startswith("Basic "):
            return False
        try:
            supplied = base64.b64decode(header[6:].strip()).decode("utf-8")
        except Exception:
            return False
        expected = f"{PANEL_USER}:{PANEL_PASSWORD}"
        return hmac.compare_digest(supplied.encode("utf-8"), expected.encode("utf-8"))

    def send_unauthorized(self):
        self.send_response(401)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("WWW-Authenticate", 'Basic realm="frp-monitor"')
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(b'{"error": "unauthorized"}')

    def do_GET(self):
        if not self._authorized():
            self.send_unauthorized()
            return
        path = urlsplit(self.path).path
        if path == "/api/hosts":
            force = "refresh" in parse_qs(urlsplit(self.path).query)
            if force:
                try:
                    refresh_snapshot()
                except (HTTPError, URLError, TimeoutError, ValueError, OSError):
                    pass
            try:
                hosts, ts = get_snapshot()
                by_name = {h["hostname"]: h for h in hosts}
                fleet, sites = [], {}
                if store.enabled:
                    for d in store.devices_view():
                        snap = by_name.get(d["hostname"])
                        online = snap is not None
                        fleet.append({
                            "hostname": d["hostname"], "site": d["site"], "owner": d["owner"],
                            "hardware": d["hardware"] or _hardware_category(d["hw_model"] or ""), "retired": d["retired"],
                            "hw_model": ((snap.get("device") or {}).get("hw_model") if snap else "") or d["hw_model"] or "",
                            "ip": ((snap.get("device") or {}).get("ip") if snap else "") or d["ip"] or "",
                            "device_id": d["device_id"], "device_name": d["device_name"],
                            "online": online, "first_seen": d["first_seen"], "last_seen": d["last_seen"],
                            "version": (snap.get("version") if snap else "") or d["release_version"] or "",
                            "version_error": (snap.get("version_error") if snap else "") or "",
                            "ssh_port": (snap.get("port") if snap else None) or d["ssh_port"],
                            "web_port": (snap.get("web_port") if snap else None) or d["web_port"],
                            "connections": snap.get("connections") if snap else None,
                            "started": (snap.get("started") if snap else "") or "",
                        })
                        if d["site"] and not d["retired"]:
                            s = sites.setdefault(d["site"], {"site": d["site"], "owner": d["owner"],
                                                             "expected": 0, "online": 0, "missing": []})
                            s["expected"] += 1
                            if online:
                                s["online"] += 1
                            else:
                                s["missing"].append({"hostname": d["hostname"], "last_seen": d["last_seen"]})
                else:
                    fleet = [{"hostname": h["hostname"], "site": "", "owner": "", "hardware": "",
                              "retired": False, "device_id": (h.get("device") or {}).get("device_id", ""),
                              "device_name": (h.get("device") or {}).get("device_name", ""), "online": True,
                              "hw_model": (h.get("device") or {}).get("hw_model", ""),
                              "ip": (h.get("device") or {}).get("ip", ""),
                              "first_seen": "", "last_seen": "", "version": h.get("version", ""),
                              "version_error": h.get("version_error", ""), "ssh_port": h.get("port"),
                              "web_port": h.get("web_port"), "connections": h.get("connections"),
                              "started": h.get("started", "")} for h in hosts]
                for h in hosts:
                    r = store.roster.get(h["hostname"]) or {}
                    dev = h.get("device") or {}
                    h["site"] = r.get("site") or ""
                    h["owner"] = r.get("owner") or ""
                    h["hw_model"] = r.get("hw_model") or dev.get("hw_model") or ""
                    h["ip"] = r.get("ip") or dev.get("ip") or ""
                    # 型号自动识别优先,推不出 V0/V1 时再回退到硬编码白名单
                    hw = r.get("hardware") or _hardware_category(h["hw_model"]) or ""
                    h["hardware"] = hw
                    h["type"] = f"EGO-Stereo-{hw}" if hw else device_type(h["hostname"])
                versions = {}
                for f in fleet:
                    key = (f.get("version") or "").strip() or "未获取"
                    entry = versions.setdefault(key, {"version": key, "count": 0, "online": 0})
                    entry["count"] += 1
                    if f.get("online"):
                        entry["online"] += 1
                ordered = sorted((v for v in versions.values() if v["version"] != "未获取"),
                                 key=lambda v: natural_key(v["version"]), reverse=True)
                if "未获取" in versions:
                    ordered.append(versions["未获取"])
                self.send_json(200, {
                    "timestamp": ts, "age": round(time.time() - ts, 1),
                    "refresh_interval": int(REFRESH_INTERVAL),
                    "total": len(hosts), "online": len(hosts), "hosts": hosts,
                    "fleet": fleet, "registered": len(fleet), "roster_enabled": store.enabled,
                    "versions": ordered,
                    "sites": sorted(sites.values(), key=lambda s: -s["expected"]),
                    "ssh_hint": {"jump": SSH_JUMP, "user": SSH_DEVICE_USER},
                })
            except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
                self.send_json(502, {"timestamp": time.time(), "error": str(exc), "total": 0, "online": 0, "hosts": []})
            return
        if path.startswith("/api/device/"):
            hostname = unquote(path[len("/api/device/"):]).strip("/")
            if not hostname or "/" in hostname:
                self.send_json(400, {"error": "invalid hostname"})
                return
            hosts, _ = get_snapshot()
            snap = next((h for h in hosts if h["hostname"] == hostname), None)
            detail = store.device_detail(hostname, snap is not None, snap) if store.enabled else None
            if detail is None and snap is None:
                self.send_json(404, {"error": "unknown device", "hostname": hostname})
            elif detail is None:
                snap_device = snap.get("device") or {}
                self.send_json(200, {"hostname": hostname, "online": True, "site": "", "owner": "",
                                     "hardware": _hardware_category(snap_device.get("hw_model") or ""),
                                     "hw_model": snap_device.get("hw_model", ""),
                                     "ip": snap_device.get("ip", ""),
                                     "release_version": snap.get("version", ""), "events": [], "status": [],
                                     "ssh_port": snap.get("port"), "web_port": snap.get("web_port"),
                                     "first_seen": "", "last_seen": ""})
            else:
                self.send_json(200, {**detail, "ssh_hint": {"jump": SSH_JUMP, "user": SSH_DEVICE_USER}})
            return
        if path.startswith("/api/hosts/") and path.endswith("/version"):
            hostname = unquote(path[len("/api/hosts/"):-len("/version")]).strip("/")
            if not hostname or "/" in hostname:
                self.send_json(400, {"error": "invalid hostname"})
                return
            try:
                hosts, _ = get_snapshot()
                host = next((item for item in hosts if item["hostname"] == hostname), None)
                if host is None:
                    try:
                        refresh_snapshot()
                        hosts, _ = get_snapshot()
                        host = next((item for item in hosts if item["hostname"] == hostname), None)
                    except (HTTPError, URLError, TimeoutError, ValueError, OSError):
                        pass
                if host is None:
                    self.send_json(404, {"error": "host is offline or not found", "hostname": hostname})
                elif not host.get("web_port"):
                    self.send_json(503, {"error": "web FRP proxy is not configured", "hostname": hostname, "host": host})
                elif not host.get("device"):
                    self.send_json(502, {"error": host.get("version_error") or "cannot read device information", "hostname": hostname, "host": host})
                else:
                    self.send_json(200, {"proxy_hostname": hostname, **host["device"]})
            except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
                self.send_json(502, {"error": str(exc), "hostname": hostname})
            return
        if path == "/api/collect-duration":
            qs = parse_qs(urlsplit(self.path).query)
            try:
                days = min(max(int(qs.get("days", ["7"])[0]), 1), 90)
            except (TypeError, ValueError):
                days = 7
            self.send_json(200, {"enabled": store.enabled, "days": days,
                                 "items": store.collect_duration_view(days) if store.enabled else []})
            return
        if path == "/api/events":
            qs = parse_qs(urlsplit(self.path).query)
            try:
                hours = min(max(float(qs.get("hours", ["24"])[0]), 1.0), 720.0)
            except (TypeError, ValueError):
                hours = 24.0
            try:
                limit = min(max(int(qs.get("limit", ["100"])[0]), 1), 1000)
            except (TypeError, ValueError):
                limit = 100
            self.send_json(200, {"enabled": store.enabled, "hours": hours,
                                 "events": store.events(hours, limit)})
            return
        if path == "/api/uptime":
            qs = parse_qs(urlsplit(self.path).query)
            try:
                hours = min(max(float(qs.get("hours", ["24"])[0]), 1.0), 720.0)
            except (TypeError, ValueError):
                hours = 24.0
            self.send_json(200, {"enabled": store.enabled, "hours": hours,
                                 "hosts": store.uptime(hours)})
            return
        if path in ("/", "/index.html"):
            body = INDEX.format(css=CSS, js=JS).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)

    def do_POST(self):
        if not self._authorized():
            self.send_unauthorized()
            return
        path = urlsplit(self.path).path
        if path == "/api/collect-duration/run":
            # 手动触发一次拉取（自测/验证用，不必等到 19:00）
            try:
                result = run_collect_duration("manual")
            except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
                self.send_json(502, {"enabled": store.enabled, "error": str(exc)})
                return
            self.send_json(200, result)
            return
        self.send_error(404)


if __name__ == "__main__":
    threading.Thread(target=_refresh_loop, name="snapshot-refresher", daemon=True).start()
    threading.Thread(target=_collect_duration_loop, name="collect-duration-puller", daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), Handler).serve_forever()
