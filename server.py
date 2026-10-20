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
from datetime import datetime, timedelta, timezone
import base64
import hashlib
import hmac
import json
import os
import queue
import re
import shlex
import shutil
import subprocess
import tempfile
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
# 历史数据保留统一由 RETENTION_DAYS（默认 30 天，每日维护线程清理）控制：
# host_status / refresh_batches / host_events / upgrade_jobs+upgrade_tasks；
# devices 与 collect_duration 永不清理。

# --- optional HTTP Basic Auth for the whole panel (page + APIs) ---------------
# Both must be set to enable; empty = open access (legacy behavior).
PANEL_USER = os.getenv("FRP_MONITOR_PANEL_USER", "")
PANEL_PASSWORD = os.getenv("FRP_MONITOR_PANEL_PASSWORD", "")
SSH_JUMP = os.getenv("FRP_MONITOR_SSH_JUMP", "ubuntu@124.222.147.63")
# 设备 SSH 用户名按机型区分（2026-10-16）：arm（V1/rk3588 等）→ orangepi，
# amd64（V0/Intel）→ archebase。FRP_MONITOR_SSH_DEVICE_USER 非空时作为全局
# 覆盖保留（兼容旧配置），优先级：全局覆盖 > 按机型解析。
SSH_DEVICE_USER = os.getenv("FRP_MONITOR_SSH_DEVICE_USER", "")
SSH_USER_ARM = os.getenv("FRP_MONITOR_SSH_USER_ARM", "orangepi")
SSH_USER_AMD = os.getenv("FRP_MONITOR_SSH_USER_AMD", "archebase")

# --- 设备采集时长（collect duration）-----------------------------------------
# 每天这些服务器本地时间点各拉一次在线设备的当日采集时长；参数化便于自测，
# 例如 FRP_MONITOR_COLLECT_TIMES=19:00,21:00（逗号分隔 HH:MM，非法项忽略）。
COLLECT_TIMES = os.getenv("FRP_MONITOR_COLLECT_TIMES", "19:00,21:00")
COLLECT_DURATION_TIMEOUT = float(os.getenv("FRP_MONITOR_COLLECT_DURATION_TIMEOUT", "8"))

# --- 批量升级：OSS 签名链接分发 / 升级编排引擎 / 数据保留与每日备份 ---------
# 分发通道（设计变更 2026-10-14）：不做服务端包托管/下载代理。安装包经 OSS
# 签名 URL 由设备本地直连下载；服务器仅在计算 sha256 基准时从 OSS 直连拉
# 一次包（拉完即删临时文件），哈希入 pkg_registry，不做存储与分发。
# 设备登录/sudo 密码：来自 /etc/frp-monitor.env（systemd EnvironmentFile 注入），
# 日志与 API 输出全程脱敏，严禁落盘/入库。
DEPLOY_PW = os.getenv("DEPLOY_PW", "")
# 并行升级设备数（SOP：预检/安装可更高，默认 6 为保守值）
UPGRADE_MAX_PARALLEL = max(1, int(os.getenv("UPGRADE_MAX_PARALLEL", "6")))
# SSH 目标：设备代理绑定本机 127.0.0.1:<ssh_port>（端口以面板设备记录为准，不按公式推算）
SSH_KNOWN_HOSTS = os.getenv("FRP_MONITOR_SSH_KNOWN_HOSTS", "/opt/frp-monitor/.known-hosts")
INSTALL_TIMEOUT = int(os.getenv("FRP_MONITOR_INSTALL_TIMEOUT", "2400"))  # 单台安装跟踪上限（秒）
# 服务端一次性拉包算哈希的临时目录与超时（拉完即删，不落盘存包）
OSS_HASH_TMP_DIR = os.getenv("FRP_MONITOR_OSS_HASH_TMP_DIR", "/var/tmp")
OSS_HASH_TIMEOUT = int(os.getenv("FRP_MONITOR_OSS_HASH_TIMEOUT", "1800"))
# 设备端下载停滞阈值（秒）：size 持续不变则杀掉 curl 并上设备验尸（SOP 坑 #13）
DOWNLOAD_STALL_TIMEOUT = int(os.getenv("FRP_MONITOR_DOWNLOAD_STALL_TIMEOUT", "180"))
# 数据保留：host_status / refresh_batches / host_events / upgrade_* 统一保留天数
# （devices、collect_duration、pkg_registry 永不清理）
RETENTION_DAYS = max(1, int(os.getenv("FRP_MONITOR_RETENTION_DAYS", "30")))
# 每日备份：mysqldump frp_monitor → BACKUP_DIR，保留最近 BACKUP_KEEP 份
BACKUP_DIR = os.getenv("FRP_MONITOR_BACKUP_DIR", "/opt/frp-monitor/backups")
BACKUP_KEEP = max(1, int(os.getenv("FRP_MONITOR_BACKUP_KEEP", "7")))
MAINTENANCE_TIME = os.getenv("FRP_MONITOR_MAINTENANCE_TIME", "03:30")

# --- webfix：缺失 web 代理设备的自动检测与一次性补配（KVWO-17/19/44）--------
# 触发时机（KVWO-44，需求方定稿 2026-10-16）：上线事件 + 稳定窗口，不再周期
# 扫描命中即触发——设备从离线转在线（且 ssh 代理在、web 代理缺、无 auto 台账
# 记录）→ 启动稳定计时；连续在线满 WEBFIX_DELAY 秒且期间未掉线（掉线计时
# 作废、重新上线重计）→ 仍缺 web 代理 → 触发；面板启动时已在线的存量设备
# 从首次观测起算同样窗口。
# 检测判据：在线 + 有 SSH 代理端口 + 无 web 代理端口（仅 version_error 属探测
# 瞬时失败，不触发）；每台设备自动触发严格一次（webfix_tasks 台账防重，永不
# 清理），失败不自动重试，面板 POST /api/webfix/retry 手动重试。补配只动设备侧
# frpc.toml（网关 allowPorts 30000-30999 已由 DevOps 预放宽，见兄弟任务），单机
# 隧道秒级闪断为预期行为；不触碰 frps / 网关任何配置。
WEBFIX_AUTO = os.getenv("FRP_MONITOR_WEBFIX_AUTO", "1").strip().lower() not in ("0", "false", "no", "off")
WEBFIX_PORT_BASE = int(os.getenv("FRP_MONITOR_WEBFIX_PORT_BASE", "30000"))  # web 端口 = 基数 + 设备编号
WEBFIX_PORT_MAX = int(os.getenv("FRP_MONITOR_WEBFIX_PORT_MAX", "30999"))     # 网关预放宽段上界
WEBFIX_VERIFY_TIMEOUT = float(os.getenv("FRP_MONITOR_WEBFIX_VERIFY_TIMEOUT", "180"))  # 重启回连+闭环校验上限(秒)
WEBFIX_MAX_PARALLEL = max(1, int(os.getenv("FRP_MONITOR_WEBFIX_MAX_PARALLEL", "2")))
# 上线后稳定窗口（秒，默认 60）：连续在线满窗且期间未掉线才自动触发，线上
# 可经环境变量调整（KVWO-44）；0 = 满窗判定退化为下一轮刷新即触发
WEBFIX_DELAY = max(0.0, float(os.getenv("FRP_MONITOR_WEBFIX_DELAY", "60")))

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


def _ssh_user_for_arch(arch):
    """设备 SSH 用户名按机型解析：arm（rk3588-arm64）→ orangepi，amd64/其他 → archebase。

    FRP_MONITOR_SSH_DEVICE_USER 非空时全局覆盖（兼容旧配置），优先级最高。"""
    if SSH_DEVICE_USER:
        return SSH_DEVICE_USER
    return SSH_USER_ARM if arch == "rk3588-arm64" else SSH_USER_AMD


def ssh_user_for_device(hardware="", hw_model=""):
    """按设备解析 SSH 用户名：hardware 字段（V0/V1）优先，回退 hw_model 型号识别。"""
    hw = (hardware or "").strip() or _hardware_category(hw_model or "")
    arch = ARCH_BY_HW.get(hw) or ARCH_BY_HW.get(hw.casefold() if hw else "")
    return _ssh_user_for_arch(arch)

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
<section class=upanel id=upgParamPanel><div class=uhead><h2>① 升级参数</h2><span class=uhint id=upgRegState></span></div>
<div class=uparam>
<div class=prow><label for=upgUrl>OSS 签名 URL</label><input id=upgUrl type=text spellcheck=false autocomplete=off placeholder="https://<bucket>.oss-cn-hangzhou.aliyuncs.com/.../ego-portal-v0.4.6-rk3588-arm64.tar.gz?Expires=…&OSSAccessKeyId=…&Signature=…"></div>
<div class=pmeta id=upgUrlMeta>粘贴后自动识别文件名、版本与机型；签名 URL 属敏感凭据，请勿外传</div>
<div class=prow><label for=upgSha>sha256 基准</label><input id=upgSha type=text spellcheck=false autocomplete=off placeholder="留空自动匹配包注册表，未命中可手填或点「服务端计算」"><button type=button id=upgShaCompute class=stagebtn title="服务端从 OSS 直连拉取整包计算（约 1G 下载，耗时视服务器出口带宽），结果入注册表缓存">服务端计算</button><span id=upgShaSrc class=shaorigin></span></div>
<div class=pmeta id=upgShaMeta></div>
<div class=pmeta id=upgExpiry></div>
<div id=upgParamSummary class=psummary></div>
</div></section>
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
.uhint{color:#98a2b3;font-size:12px;margin:10px 0 0}.uhint.warn{color:#b45309}.uhint:empty{display:none}
.shalink{display:inline-flex;align-items:center;gap:6px}.sha{font-family:ui-monospace,monospace;font-size:12px;color:#475569}
.copymini{padding:2px 8px;font-size:11px;background:#f1f5f9;border:1px solid #cbd5e1;color:#334155}.copymini:hover{background:#e2e8f0}
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
.ossban{background:#fef2f2;border:1px solid #fecaca;color:#b91c1c;border-radius:8px;padding:10px 14px;font-size:13px;font-weight:600;margin-bottom:10px}
.uparam .prow{display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin-bottom:8px}.prow label{width:96px;flex:none;text-align:right;color:#667085;font-size:13px}.prow input[type=text]{flex:1;min-width:280px;font-family:ui-monospace,monospace;font-size:12px;padding:8px 10px}.prow .stagebtn{flex:none}
.pmeta{font-size:12px;color:#718096;margin:0 0 8px 106px;min-height:0}.pmeta:empty{display:none;margin:0}.pmeta.bad{color:#b91c1c}.pmeta.warn{color:#b45309}
.shaorigin{font-size:12px;padding:2px 10px;border-radius:999px;background:#f1f5f9;color:#667085;white-space:nowrap}.shaorigin.registry{background:#dcfce7;color:#15803d}.shaorigin.manual{background:#dbeafe;color:#1d4ed8}.shaorigin.server{background:#ccfbf1;color:#0f766e}
.psummary{display:flex;gap:14px;flex-wrap:wrap;align-items:center;background:#f8fafc;border:1px solid #e5eaf1;border-radius:8px;padding:10px 12px;margin-top:10px;font-size:13px;color:#475569}.psummary:empty{display:none}.psummary b{color:#172033}.psummary .shalink{gap:4px}
.mism{font-size:11px;padding:1px 8px;border-radius:999px;background:#fef3c7;color:#b45309;border:1px solid #fde68a;white-space:nowrap}
/* ---- web 代理补配标记（KVWO-18）：卡片小标 + 详情重试 ---- */
.wfix{display:inline-flex;align-items:center;gap:5px;font-size:11px;padding:2px 9px;border-radius:999px;background:#f1f5f9;color:#667085;border:1px solid #cbd5e1;white-space:nowrap}
.wfix.running{background:#dbeafe;color:#1d4ed8;border-color:#bfdbfe}
.wfix.failed{background:#fee2e2;color:#b91c1c;border-color:#fecaca;cursor:help}
.wspin{display:inline-block;width:9px;height:9px;border:2px solid #93c5fd;border-top-color:transparent;border-radius:50%;animation:wspin .8s linear infinite;vertical-align:-1px}
@keyframes wspin{to{transform:rotate(360deg)}}
.wfixbar{display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap;margin:-4px 0 10px}
.wfixbar small{color:#98a2b3}
.wfixbar button{padding:6px 16px;font-size:12px;background:#dc2626}
.wfixbar button:hover:not(:disabled){background:#b91c1c}
.wfixbar button.arm{background:#b45309}
.wfixbar button.arm:hover:not(:disabled){background:#92400e}
.wfixbar button.err{background:#475569;color:#fff;cursor:default;overflow:hidden;text-overflow:ellipsis;max-width:100%}
@media(max-width:640px){.vrows{grid-template-columns:1fr}.ucardgrid{grid-template-columns:1fr}.stepper{flex-wrap:wrap}.step{flex:1 1 30%}.prow label{width:auto;text-align:left}.pmeta{margin-left:0}}"""
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
let webfix={map:{},cands:{}};
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
/* ---- web 代理补配状态（KVWO-18，契约按 KVWO-19 feat/auto-webfix 定稿对齐）----
 GET /api/webfix/tasks → {enabled,auto_enabled,port_base,port_max,verify_timeout,
 candidates:[{hostname,web_port}],counts,tasks:[{task_id,hostname,site,trigger_type,
 status queued|running|success|failed,error,attempts,web_port,created_at(Unix 秒),
 finished_at,chars}]}（tasks 为每设备最新一行；candidates 为当前缺配候选）。
 POST /api/webfix/retry {devices:[...]} → {ok,started:[{hostname,task_id,attempts}],
 skipped:[{hostname,reason}]}（skipped 必须向用户呈现原因）；SSE 复用
 /api/upgrade/events：type=webfix（task 同上字段）/ webfix_log（日志增量，总览不消费）。
 与 /api/hosts 的 web_port 组合判定：有 web_port → 正常（无标记）；在线 + 有 ssh_port + 无
 web_port → 缺配候选，再叠加最新任务状态：queued/running → 补配中，failed → 失败（悬停看
 error），success → 标记消失（版本随下次 /api/hosts 刷新恢复显示）。
 接口未上线（404）或拉取失败时优雅降级：沿用上次数据，从无数据时仅按 web_port 显示灰色
「web 缺配」标记，不显示进行中/失败态。字段解析保留容错（数组/字典形态、ISO/Unix 时间）。 */
function webfixTask(f){return (webfix.map||{})[String((f&&(f.hostname||f.host))||'').toLowerCase()]||null}
function webfixState(f){
  const t=webfixTask(f);
  if(f&&f.web_port)return{code:'normal',task:t};
  if(!(f&&f.online&&f.ssh_port))return{code:'',task:t};
  const s=t?String(t.status||'').toLowerCase():'';
  if(s==='success'||s==='done'||s==='ok')return{code:'',task:t};
  if(s==='queued'||s==='running'||s==='pending')return{code:'running',task:t};
  if(s==='failed'||s==='fail'||s==='error')return{code:'failed',title:(t&&t.error)||'补配失败（未返回原因）',task:t};
  return{code:'missing',task:t};
}
function webfixBadge(f){
  const st=webfixState(f);
  if(st.code==='running')return '<span class="wfix running" title="web 代理补配进行中…"><i class="wspin"></i>补配中…</span>';
  if(st.code==='failed')return `<span class="wfix failed" title="${esc(st.title)}">补配失败</span>`;
  if(st.code==='missing')return '<span class="wfix" title="未检测到 web 代理（ep-编号-web），等待自动补配">web 缺配</span>';
  return '';
}
function card(f){
  const tag=f.site?`<span class="stag" style="--c:${siteColor(f.site)}">${esc(f.site)}</span>`:'';
  const wtag=webfixBadge(f);
  const cd=collectDur.enabled?collectDur.map[(f.hostname||'').toLowerCase()]:null;
  if(f.online){
    return `<article class="card online" data-h="${esc(f.hostname)}"><div class="top"><span class="name"><i class="dot"></i>${esc(f.hostname)}</span>${tag}${wtag}</div>
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
    const wf=webfixState(d);
    const wfc=(webfix.cands||{})[String(d.hostname||d.host||'').toLowerCase()]||null;
    const wfExp=(wfc&&wfc.web_port)||(wf.task&&wf.task.web_port)||null;
    const wfRow=wf.code==='normal'?`${d.web_port||'—'} / <span style="color:#16a34a">正常</span>`
      :wf.code==='running'?`${d.web_port||'—'} / <span style="color:#1d4ed8"><i class="wspin"></i> 补配中</span>`
      :wf.code==='failed'?`${d.web_port||'—'} / <span style="color:#dc2626" title="${esc(wf.title)}">失败：${esc(wf.title)}</span>`
      :wf.code==='missing'?`${d.web_port||'—'} / <span style="color:#667085" title="未检测到 web 代理（ep-编号-web），等待自动补配">${wfExp?`缺配（补配后 ${esc(String(wfExp))}）`:'缺配'}</span>`
      :`${d.web_port||'—'} / <span style="color:#98a2b3" title="设备未在线或无 SSH 隧道时不判定缺配">—</span>`;
    const wfBar=wf.code==='failed'&&d.online&&d.ssh_port?`<div class="wfixbar"><small>自动补配仅触发一次；重试将重新推送 frpc 配置并重启该机 frpc（隧道秒级闪断）</small><button type="button" id="webfixRetry">重试补配</button></div>`:'';
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
        <span>web 代理</span><b>${wfRow}</b>
        <span>首次 / 最后在线</span><b>${esc(d.first_seen||'—')} / ${esc(d.last_seen||'从未')}</b>
        <span>在线率 24h / 7d</span><b>${pct(d.uptime_24h)} / ${pct(d.uptime_7d)}</b>
        <span>采集时长</span><b>${fmtDuration(cd?cd.today_minutes:null)}</b>
      </div>
      ${wfBar}
      ${cmd?`<div class="cmd"><code id="sshCmd">${esc(cmd)}</code><button id="copyBtn">复制跳板命令</button></div>`:''}
      <h3>近 7 天采集时长</h3><div class="twrap"><table><tr><th>日期</th><th>时长</th></tr>${cdRows||`<tr><td colspan=2 class="eempty">${collectDur.enabled?'暂无采集记录':'采集时长统计未启用'}</td></tr>`}</table></div>
      <h3>最近快照（5 分钟一条）</h3><div class="twrap"><table><tr><th>时间</th><th>状态</th><th>版本</th><th>连接</th></tr>${stRows||'<tr><td colspan=4 class="eempty">暂无快照</td></tr>'}</table></div>
      <h3>最近事件</h3><div class="elist">${evRows}</div>`;
    $('#mClose').onclick=closeModal;
    const cb=$('#copyBtn');if(cb)cb.onclick=async()=>{if(await copyText(cmd)){cb.textContent='已复制 ✓';setTimeout(()=>cb.textContent='复制跳板命令',1500)}else alert(cmd)};
    const rb=$('#webfixRetry');
    if(rb)rb.onclick=async()=>{
      if(rb.dataset.arm!=='1'){rb.dataset.arm='1';rb.classList.add('arm');rb.textContent='确认重试？';setTimeout(()=>{if(rb.isConnected&&rb.dataset.arm==='1'){rb.dataset.arm='';rb.classList.remove('arm');rb.textContent='重试补配'}},4000);return}
      rb.dataset.arm='';rb.classList.remove('arm');
      rb.disabled=true;rb.textContent='发起中…';
      const wfail=msg=>{rb.disabled=false;rb.classList.remove('arm');rb.classList.add('err');rb.textContent='未发起：'+msg;setTimeout(()=>{if(rb.isConnected&&!rb.disabled){rb.classList.remove('err');rb.textContent='重试补配'}},8000)};
      try{
        const r=await fetch('/api/webfix/retry',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({devices:[d.hostname]})});
        const j=await r.json().catch(()=>({}));
        if(!r.ok||j.ok===false)throw Error(j.error||('HTTP '+r.status));
        const wnorm=x=>String((x&&(x.hostname||x.host))||'').toLowerCase();
        const me=String(d.hostname||'').toLowerCase();
        const wstarted=(j.started||[]).some(x=>wnorm(x)===me);
        const wskip=(j.skipped||[]).find(x=>wnorm(x)===me);
        if(!wstarted)throw Error(String((wskip&&(wskip.reason||wskip.error))||'服务端未确认该设备（响应无 started 记录）'));
        rb.classList.remove('arm');rb.textContent='已发起重试 ✓';
        setTimeout(()=>{loadWebfix();if($('#modal').classList.contains('show'))openDevice(d.hostname)},900);
      }catch(e){wfail(String(e.message||e))}
    };
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
  const text=rows.map(f=>`ssh -J ${jump} -p ${f.ssh_port} ${f.ssh_user||user}@127.0.0.1  # ${f.hostname.toUpperCase()}${f.site?' ('+f.site+')':''}`).join('\\n');
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
async function loadWebfix(){
  try{
    const r=await fetch('/api/webfix/tasks',{cache:'no-store'});
    const d=await r.json();
    if(!r.ok)throw Error(d.error||('HTTP '+r.status));
    let arr=Array.isArray(d)?d:(d.tasks||d.items||d.devices||d.list||[]);
    if(!Array.isArray(arr)&&arr&&typeof arr==='object')arr=Object.entries(arr).map(([h,t])=>Object.assign({hostname:h},(t&&typeof t==='object')?t:{status:t}));
    const tsOf=v=>{if(typeof v==='number')return v>1e12?v:Math.round(v*1000);const p=Date.parse(String(v==null?'':v).replace(' ','T'));return isNaN(p)?0:p};
    const map={};
    for(const t of (arr||[])){
      const h=String((t&&(t.hostname||t.host))||'').toLowerCase();if(!h)continue;
      const ts=tsOf(t&&(t.created_at||t.created));
      const old=map[h];if(old&&ts<old._ts)continue;
      map[h]={hostname:h,status:t.status,error:t.error,attempts:t.attempts,trigger_type:t.trigger_type,web_port:t.web_port,created_at:t.created_at,finished_at:t.finished_at,_ts:ts};
    }
    const cands={};for(const c of (d.candidates||[]))if(c&&c.hostname)cands[String(c.hostname).toLowerCase()]=c;
    webfix={map:map,cands:cands};
  }catch(e){/* webfix 接口未上线/拉取失败：沿用已有数据；从无数据时退化为仅「web 缺配」灰色标记 */}
  if(fleet.length)renderGrid();
}
loadWebfix();setInterval(loadWebfix,15000);

/* ================= 升级管理视图（Upg.* 独立命名空间，不改动设备总览逻辑） =================
 数据契约（后端返工后联调基准）：POST /api/upgrade/jobs {kind,devices,params{version,oss_url,sha256}}、
 GET /api/upgrade/jobs(/<id>)、GET /api/upgrade/events（SSE：{type:'job'|'task'|'log'|'webfix'|'webfix_log',...}，
 webfix 类事件只驱动设备总览刷新、不进升级视图）、
 GET /api/upgrade/jobs/<id>/log?offset=N[&hostname=]、GET /api/upgrade/packages（包注册表）、
 POST/GET /api/upgrade/sha256（服务端一次性拉包算哈希，异步轮询）。旧 /api/packages 整套已不再依赖。
 字段解析全部做了兼容容错，后端未上线时各区城优雅空态；最终契约以胡磊涛完成评论为准。 */
const Upg={
STAGES:['precheck','download','prepare','install','verify','cleanup'],
STAGE_LBL:{precheck:'预检',download:'下载',prepare:'准备',install:'安装',verify:'验证',cleanup:'清理'},
KIND_LBL:{upgrade:'全流程升级',precheck:'仅预检',download:'仅下载',prepare:'仅准备',install:'仅安装',verify:'仅验证',cleanup:'仅清理'},
STAT_LBL:{running:'进行中',success:'成功',failed:'失败',blocked:'已阻断',pending:'等待中'},
fleet:[],selected:new Set(),selTouched:false,targetVersion:'',reg:[],regFail:false,
param:{url:'',ok:false,filename:'',version:'',arch:'',expiresAt:null,sha256:'',shaSrc:'',size:null},
shaComputing:false,shaPollT:null,shaPollN:0,urlTimer:null,
job:null,tasks:[],logs:{},logOff:{},follow:{},expanded:null,fromSnap:{},
es:null,pollT:null,connState:'off',view:'overview',stickyNote:null,
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
 const urlIn=this.gid('upgUrl'),shaIn=this.gid('upgSha');
 urlIn.addEventListener('input',()=>{clearTimeout(this.urlTimer);this.urlTimer=setTimeout(()=>this.onUrlInput(),400)});
 urlIn.addEventListener('change',()=>{clearTimeout(this.urlTimer);this.onUrlInput()});
 shaIn.addEventListener('input',()=>{const v=shaIn.value.trim().toLowerCase();this.param.sha256=/^[0-9a-f]{64}$/.test(v)?v:'';this.param.shaSrc=this.param.sha256?'manual':'';this.renderParam()});
 this.gid('upgShaCompute').onclick=()=>this.computeSha();
 this.gid('upgParamSummary').addEventListener('click',async e=>{const b=e.target.closest('button[data-sha]');if(!b)return;b.textContent='…';const ok=await copyText(b.dataset.sha);b.textContent=ok?'已复制':'失败';setTimeout(()=>b.textContent='复制',1500)});
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
enter(){this.loadRegistry();this.loadFleet();this.loadJobs();this.renderParam();this.updateSelBar();this.updateActions()},
setActHint(msg,warn){const el=this.gid('upgActHint');el.textContent=msg||'';el.className='uhint'+(warn?' warn':'')},
parseOssUrl(u){
 u=String(u||'').trim();
 const out={url:u,ok:false,filename:'',version:'',arch:'',expiresAt:null,err:'',sha256:'',shaSrc:'',size:null};
 if(!u)return out;
 let p=null;try{p=new URL(u)}catch(e){out.err='URL 格式无法解析';return out}
 out.ok=true;
 const segs=p.pathname.split('/').filter(Boolean);
 try{out.filename=decodeURIComponent(segs[segs.length-1]||'')}catch(e){out.filename=segs[segs.length-1]||''}
 const vm=out.filename.match(/v\\d+\\.\\d+(\\.\\d+)*/i);out.version=vm?vm[0].toLowerCase():'';
 if(/rk3588|arm64|aarch64/i.test(out.filename))out.arch='rk3588-arm64';
 else if(/amd64|x86/i.test(out.filename))out.arch='amd64';
 const v1=parseInt(p.searchParams.get('Expires')||'',10);
 if(!isNaN(v1)&&v1>1000000)out.expiresAt=v1*1000;
 else{
  const d=p.searchParams.get('x-oss-date')||'',dur=parseInt(p.searchParams.get('x-oss-expires')||'',10);
  const m=d.match(/^(\\d{4})(\\d{2})(\\d{2})T(\\d{2})(\\d{2})(\\d{2})Z$/);
  if(m&&!isNaN(dur)){const t=Date.parse(m[1]+'-'+m[2]+'-'+m[3]+'T'+m[4]+':'+m[5]+':'+m[6]+'Z');if(!isNaN(t))out.expiresAt=t+dur*1000}
 }
 return out;
},
fmtRemain(ms){
 if(ms==null||isNaN(ms))return '—';
 if(ms<=0)return '已过期';
 const h=ms/3600000;
 if(h>=48)return Math.floor(h/24)+' 天';
 if(h>=1)return h.toFixed(1)+' 小时';
 return Math.max(1,Math.round(ms/60000))+' 分钟';
},
estMs(){return Math.max(1,Math.ceil((this.selected.size||1)/6))*12*60000},
onUrlInput(){
 const url=this.gid('upgUrl').value;
 const prev=this.param.filename;
 this.param=this.parseOssUrl(url);
 this.targetVersion=this.param.version;
 this.shaComputing=false;this.stopShaPoll();
 if(this.param.ok&&this.param.filename&&prev!==this.param.filename){
  if(this.reg.length)this.matchRegistry();
  else this.loadRegistry();
 }
 this.renderParam();
},
async loadRegistry(){
 try{
  const r=await fetch('/api/upgrade/packages',{cache:'no-store'});
  const d=await r.json();
  if(!r.ok)throw Error(d.error||('HTTP '+r.status));
  this.reg=d.packages||d.items||(Array.isArray(d)?d:[]);
  this.regFail=false;
  if(this.param.filename&&this.param.shaSrc!=='server')this.matchRegistry();
 }catch(e){this.reg=[];this.regFail=true}
 this.renderParam();
},
matchRegistry(){
 const fn=this.param.filename;
 let hit=this.reg.find(x=>String(x.filename||x.name||'')===fn);
 if(!hit&&this.param.version)hit=this.reg.find(x=>x.version===this.param.version&&(!this.param.arch||x.arch===this.param.arch));
 if(hit&&hit.sha256){
  this.param.sha256=String(hit.sha256).toLowerCase();
  this.param.shaSrc='registry';
  this.param.size=hit.size!=null?hit.size:null;
  return true;
 }
 return false;
},
renderExpiry(){
 const el=this.gid('upgExpiry');if(!el)return;
 if(!this.param.ok||!this.param.url){el.textContent='';el.className='pmeta';return}
 if(this.param.expiresAt==null){el.textContent='⚠ 未识别到有效期参数（Expires / x-oss-expires），请自行确认链接时效';el.className='pmeta warn';return}
 const rem=this.param.expiresAt-Date.now();
 const left=this.fmtRemain(rem);
 const until='至 '+new Date(this.param.expiresAt).toLocaleString('zh-CN',{hour12:false});
 if(rem<=0){el.textContent='链接已过期（'+until+'），请重新生成签名 URL 后粘贴';el.className='pmeta bad'}
 else if(rem<this.estMs()){el.textContent='有效期约剩 '+left+'（'+until+'）——低于预估批量时长 '+this.fmtRemain(this.estMs())+'（'+this.selected.size+' 台 · 6 并行），批量中途可能过期';el.className='pmeta warn'}
 else{el.textContent='有效期约剩 '+left+'（'+until+'）· 预估批量时长 '+this.fmtRemain(this.estMs());el.className='pmeta'}
},
renderParam(){
 const urlMeta=this.gid('upgUrlMeta'),shaMeta=this.gid('upgShaMeta'),srcEl=this.gid('upgShaSrc'),sumEl=this.gid('upgParamSummary'),regEl=this.gid('upgRegState');
 if(!urlMeta)return;
 if(this.regFail)regEl.textContent='包注册表接口不可用（后端未上线？），sha256 可手填';
 else regEl.textContent=this.reg.length?'注册表已加载 '+this.reg.length+' 条':'';
 if(!this.param.url){urlMeta.textContent='粘贴后自动识别文件名、版本与机型；签名 URL 属敏感凭据，请勿外传';urlMeta.className='pmeta'}
 else if(!this.param.ok){urlMeta.textContent='URL 格式无法解析，请检查后重新粘贴';urlMeta.className='pmeta bad'}
 else{
  urlMeta.textContent='文件名 '+(this.param.filename||'未识别')+' · 版本 '+(this.param.version||'未识别')+' · 机型 '+(this.param.arch||'未识别');
  urlMeta.className='pmeta'+(this.param.version&&this.param.arch?'':' warn');
 }
 const shaIn=this.gid('upgSha');
 if(this.param.shaSrc!=='manual'&&shaIn.value!==(this.param.sha256||''))shaIn.value=this.param.sha256||'';
 srcEl.textContent=this.param.shaSrc==='registry'?'注册表命中':this.param.shaSrc==='manual'?'手动填写':this.param.shaSrc==='server'?'服务端计算':'';
 srcEl.className='shaorigin '+(this.param.shaSrc||'');
 if(this.shaComputing){shaMeta.textContent='服务端计算中：正在从 OSS 直连拉取整包（约 1G，耗时视出口带宽），完成后自动填入并缓存进注册表…';shaMeta.className='pmeta'}
 else if(this.param.sha256||this.param.shaSrc==='manual'){shaMeta.textContent='';shaMeta.className='pmeta'}
 else if(this.param.ok&&this.param.filename){shaMeta.textContent=this.regFail?'注册表不可用且未填 sha256，可手填':'注册表未命中，可手填或点「服务端计算」';shaMeta.className='pmeta'}
 else{shaMeta.textContent='';shaMeta.className='pmeta'}
 this.renderExpiry();
 if(!this.param.ok||!this.param.filename)sumEl.innerHTML='';
 else sumEl.innerHTML='<span>目标版本 <b>'+esc(this.param.version||'—')+'</b></span><span>机型 <b>'+esc(this.param.arch||'—')+'</b></span>'+(this.param.size!=null?'<span>大小 <b>'+this.fmtB(this.param.size)+'</b></span>':'')+(this.param.sha256?'<span class=shalink>sha256 <span class=sha title="'+esc(this.param.sha256)+'">'+esc(this.param.sha256.slice(0,16))+'…</span><button type=button class=copymini data-sha="'+esc(this.param.sha256)+'">复制</button></span>':'');
 this.renderDevs();this.updateSelBar();this.updateActions();
},
async computeSha(){
 if(this.shaComputing)return;
 if(!this.param.ok||!this.param.url){this.setActHint('请先粘贴有效的 OSS 签名 URL',true);return}
 this.shaComputing=true;this.renderParam();
 try{
  const r=await fetch('/api/upgrade/packages/hash',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({oss_url:this.param.url})});
  const d=await r.json().catch(()=>({}));
  if(!r.ok){
   if(d.error_code==='oss_expired'){this.shaComputing=false;const m=this.gid('upgShaMeta');if(m){m.textContent='服务端计算失败：'+(d.error||'链接已过期')+'，请重新生成签名 URL';m.className='pmeta bad'}return}
   throw Error(d.error||('HTTP '+r.status));
  }
  if(d.status==='done'&&(d.entry||{}).sha256){
   this.param.sha256=String(d.entry.sha256).toLowerCase();this.param.shaSrc='server';
   if(d.entry.size!=null)this.param.size=d.entry.size;
   this.shaComputing=false;this.loadRegistry();
  }else this.startShaPoll();
 }catch(e){
  this.shaComputing=false;
  const m=this.gid('upgShaMeta');if(m){m.textContent='服务端计算失败：'+e.message+'（可手填）';m.className='pmeta bad'}
 }
},
regEntry(){return this.reg.find(x=>x.version===this.param.version&&x.arch===this.param.arch)||this.reg.find(x=>String(x.filename||'')===this.param.filename&&x.filename)},
async pollShaTick(){
 try{
  const r=await fetch('/api/upgrade/packages',{cache:'no-store'});
  const d=await r.json();
  if(!r.ok)return;
  this.reg=d.packages||d.items||(Array.isArray(d)?d:[]);
  this.regFail=false;
 }catch(e){return}
 const e=this.regEntry();
 if(e&&e.sha256){this.stopShaPoll();this.shaComputing=false;this.param.sha256=String(e.sha256).toLowerCase();this.param.shaSrc='server';if(e.size!=null)this.param.size=e.size;this.renderParam();return}
 if(e&&e.hash_status==='failed'){this.stopShaPoll();this.shaComputing=false;const m=this.gid('upgShaMeta');if(m){m.textContent='服务端计算失败：'+(e.error||e.error_code||'未知原因')+'（可手填）';m.className='pmeta bad'}return}
 if(e&&e.hash_status==='computing'){
  const m=this.gid('upgShaMeta');
  if(m){m.className='pmeta';m.textContent=e.total>0?('服务端计算中：已从 OSS 拉取 '+Math.min(100,Math.round((e.downloaded||0)/e.total*100))+'%（'+this.fmtB(e.downloaded||0)+' / '+this.fmtB(e.total)+'），完成后自动填入并缓存进注册表…'):'服务端计算中：正在从 OSS 直连拉取整包（约 1G，耗时视出口带宽），完成后自动填入并缓存进注册表…'}
 }
 const rs=this.gid('upgRegState');
 if(rs&&!this.regFail)rs.textContent=this.reg.length?('注册表已加载 '+this.reg.length+' 条'+(this.reg.some(x=>x.hash_status==='computing')?' · '+this.reg.filter(x=>x.hash_status==='computing').length+' 个计算中':'')):'';
},
startShaPoll(){
 this.stopShaPoll();this.shaPollN=0;
 this.pollShaTick();
 this.shaPollT=setInterval(()=>{
  this.shaPollN++;
  if(this.shaPollN>150){this.stopShaPoll();this.shaComputing=false;const m=this.gid('upgShaMeta');if(m){m.textContent='服务端计算超时（>10 分钟仍未完成），可稍后重试或手填';m.className='pmeta bad'}return}
  this.pollShaTick()},4000);
},
stopShaPoll(){if(this.shaPollT){clearInterval(this.shaPollT);this.shaPollT=null}},
isOssErr(t,code){
 if(String(code||'').toLowerCase()==='oss_expired')return true;
 t=String(t||'');
 if(!t)return false;
 return /(^|[^0-9])403([^0-9]|$)/i.test(t)||/access ?denied|oss_expired|forbidden/i.test(t)||/链接?过期|重新生成/.test(t);
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
   const mism=(this.param.arch&&r.arch&&r.arch!==this.param.arch)?'<span class=mism title="机型与目标包（'+esc(this.param.arch)+'）不符">机型不符</span>':'';
   h+=`<label class="vrow${r.online?'':' off'}" title="${r.online?'':'离线设备不可选'}"><input type=checkbox data-h="${esc(r.h)}" ${this.selected.has(r.h)?'checked':''} ${r.online?'':'disabled'}><span class=bh>${esc(r.h)}</span><small>${esc(r.arch||r.hw||'')}</small>${mism}${tag}</label>`}
  h+='</div></div>'
 }
 wrap.innerHTML=h;
},
updateSelBar(){
 const n=this.selected.size;const el=this.gid('upgSelBar');
 const vers=new Set();
 for(const h of this.selected){const f=this.fleet.find(x=>x.hostname===h);vers.add(((f&&f.version)||'').trim()||'未获取')}
 const cur=n?(vers.size===1?[...vers][0]:'多版本'):'—';
 el.innerHTML=`已选 <b>${n}</b> 台 · 当前版本 ${esc(cur)} → 目标版本 <b>${esc(this.targetVersion||'—')}</b>${n?' <button type=button class=minibtn data-clr>清空选择</button>':''}`;
 this.renderExpiry();
},
canLaunch(){return !this.active()&&this.param.ok&&!!this.param.version&&this.selected.size>0},
updateActions(){
 const busy=this.active();
 const noUrl=!this.param.ok||!this.param.url;
 const noVer=!this.param.version;
 const noSel=!this.selected.size;
 const expired=this.param.expiresAt!=null&&this.param.expiresAt-Date.now()<=0;
 const lowTime=this.param.expiresAt!=null&&!expired&&this.param.expiresAt-Date.now()<this.estMs();
 const noSha=!this.param.sha256;
 let mism=0;
 if(this.param.arch)for(const h of this.selected){const f=this.fleet.find(x=>x.hostname===h);const a=f&&this.archOf(f);if(a&&a!==this.param.arch)mism++}
 let hint='',warn=false;
 if(busy)hint='有升级任务进行中（#'+this.job.id+'），任务结束后才能发起新任务（后端并发护栏兜底）';
 else if(expired)hint='OSS 链接已过期，请重新生成签名 URL 后粘贴';
 else if(noUrl)hint='请先在「① 升级参数」粘贴 OSS 签名 URL（自动识别版本/机型）';
 else if(noVer)hint='URL 中未识别出版本号（文件名需形如 ego-portal-v0.4.6-rk3588-arm64.tar.gz）',warn=true;
 else if(noSel)hint='请至少勾选一台在线设备（默认已勾选全部在线设备，离线设备不可选）';
 else if(mism)hint='⚠ 所选设备中 '+mism+' 台机型与目标包（'+this.param.arch+'）不符，下载/安装类操作不可发起；请改选相符机型设备，或更换对应机型的包链接',warn=true;
 else if(noSha)hint='⚠ 尚无 sha256 基准：下载/安装类操作不可发起（注册表未命中时请手填或点「服务端计算」）；仅预检/验证/清理可发起',warn=true;
 else if(lowTime)hint='⚠ 链接剩余有效期低于预估批量时长（'+this.fmtRemain(this.estMs())+'），批量中途可能过期，建议重新生成更长时效链接',warn=true;
 if(busy&&this.stickyNote&&Date.now()<this.stickyNote.until){hint=this.stickyNote.text;warn=this.stickyNote.warn}
 else if(!busy)this.stickyNote=null;
 const heavy=busy||noUrl||noVer||noSel||expired||mism>0||noSha;
 const light=busy||noUrl||noVer||noSel||expired;
 this.gid('upgRunAll').disabled=heavy;
 document.querySelectorAll('.stagebtn[data-kind]').forEach(b=>{const k=b.dataset.kind;b.disabled=(k==='download'||k==='install')?heavy:light});
 this.setActHint(hint,warn);
},
closeConfirm(){const m=this.gid('upgModal');if(m)m.classList.remove('show')},
showConfirm(){
 const n=this.selected.size;const vs=this.targetVersion;
 const archSplit={};let mism=0;
 for(const h of this.selected){const f=this.fleet.find(x=>x.hostname===h);const a=(f&&this.archOf(f))||'未知机型';if(this.param.arch&&a!=='未知机型'&&a!==this.param.arch){mism++;archSplit['机型不符']=(archSplit['机型不符']||0)+1}else archSplit[a]=(archSplit[a]||0)+1}
 const archTxt=Object.entries(archSplit).map(([a,c])=>esc(a)+' × '+c).join('、');
 const list=[...this.selected].sort((a,b)=>a.localeCompare(b,undefined,{numeric:true}));
 const preview=esc(list.length>12?list.slice(0,12).join('、')+' 等 '+n+' 台':list.join('、'));
 const remTxt=this.param.expiresAt==null?'未识别（请自行确认链接时效）':((this.param.expiresAt-Date.now()>0?'约剩 ':'')+this.fmtRemain(this.param.expiresAt-Date.now())+(this.param.expiresAt-Date.now()>0?'（至 '+new Date(this.param.expiresAt).toLocaleString('zh-CN',{hour12:false})+'）':''));
 const m=this.gid('upgModalBody');
 m.innerHTML=`<header><div><span class=name>确认发起全流程升级</span></div><button type=button class="x" id=upgConfirmClose>✕</button></header>
 <div class=mgrid>
  <span>目标版本</span><b>${esc(vs)}（${esc(this.param.arch||'机型未识别')} · ${esc(this.param.filename||'文件名未识别')}）</b>
  <span>设备数量</span><b>${n} 台（${archTxt}${mism?'，其中 '+mism+' 台机型不符':''}）</b>
  <span>设备清单</span><b>${preview}</b>
  <span>链接有效期</span><b>${esc(remTxt)}</b>
  <span>执行流程</span><b>预检 → 下载 → 准备 → 安装 → 验证 → 清理（设备直连 OSS 下载，sha256 基准校验）</b>
 </div>
 <p class=uwarn>⚠ 升级将覆盖设备上的程序，过程不可逆；单台失败不影响其他设备，安装器失败时会自动回滚兜底。请确认目标版本、机型与设备清单无误后再发起。</p>
 <div class=ubtns style="justify-content:flex-end;margin-top:16px"><button type=button class=stagebtn id=upgConfirmNo>取消</button><button type=button id=upgConfirmYes>确认发起升级</button></div>`;
 this.gid('upgModal').classList.add('show');
 this.gid('upgConfirmClose').onclick=this.gid('upgConfirmNo').onclick=()=>this.closeConfirm();
 this.gid('upgConfirmYes').onclick=()=>{this.closeConfirm();this.launch('upgrade')};
},
async launch(kind){
 const devices=[...this.selected];
 this.setActHint('正在创建任务…');
 try{
  const r=await fetch('/api/upgrade/jobs',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({kind,devices,params:{version:this.targetVersion,oss_url:this.param.url,sha256:this.param.sha256||undefined}})});
  const d=await r.json().catch(()=>({}));
  if(!r.ok){
   if(d.error_code==='oss_expired'){this.param.expiresAt=Date.now()-1000;this.renderParam();throw Error((d.error||'OSS 链接已过期')+'（后端校验拒绝）')}
   if(d.error_code==='sha_missing')throw Error(d.error||'缺少 sha256 基准');
   throw Error(d.error||('HTTP '+r.status));
  }
  const j=d.job||d;const id=j.id!=null?j.id:j.job_id;
  if(id==null)throw Error('响应缺少任务 id（job_id）');
  if(d.expires_at!=null){const t=this.parseTs(d.expires_at);if(t)this.param.expiresAt=t}
  this.fromSnap={};for(const h of devices){const f=this.fleet.find(x=>x.hostname===h);if(f)this.fromSnap[h]=f.version||''}
  const effective=(Array.isArray(d.devices)&&d.devices.length)?d.devices:devices;
  this.adoptJob({id:id,kind:j.kind||kind,version:j.version||this.targetVersion,filename:j.filename||this.param.filename||'',status:j.status||'running',created_at:j.created_at||new Date().toISOString(),finished_at:j.finished_at||null,devices:effective});
  let note='任务 #'+id+' 已创建';
  if(Array.isArray(d.skipped)&&d.skipped.length)note+='；'+d.skipped.length+' 台被跳过：'+d.skipped.map(s=>esc(s.hostname)+'（'+esc(s.reason||s.error||'原因未说明')+'）').join('、');
  if(Array.isArray(d.warnings)&&d.warnings.length)note+='；⚠ '+d.warnings.map(w=>esc(typeof w==='string'?w:(w.message||w.msg||''))).join('；');
  this.stickyNote={text:note,warn:!!(d.warnings&&d.warnings.length),until:Date.now()+20000};
  this.setActHint(this.stickyNote.text,this.stickyNote.warn);
  this.loadJobDetail(id);this.subscribe();
 }catch(e){this.setActHint('发起失败：'+e.message,true)}
},
normJob(j){return{id:j.id!=null?j.id:j.job_id,kind:j.kind||'',version:j.version||(j.params&&j.params.version)||'',filename:j.filename||'',oss_url_expires:j.oss_url_expires!=null?j.oss_url_expires:null,error:j.error||j.error_code||'',error_code:j.error_code||'',status:j.status||'running',created_at:j.created_at||j.createdAt||null,finished_at:j.finished_at||j.finishedAt||null,devices:j.devices||[],summary:j.summary||null}},
normTask(t){return{hostname:t.hostname||t.host||'',stage:String(t.stage||'').toLowerCase(),status:this.normStatus(t.status),error:t.error||t.reason||'',error_code:t.error_code||'',started_at:t.started_at||t.startedAt||null,finished_at:t.finished_at||t.finishedAt||null,from_version:t.from_version||t.version_before||''}},
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
 let oss=false;
 if(this.isOssErr(j.error,j.error_code))oss=true;
 if(!oss)for(const t of this.tasks){if(t.status==='failed'&&this.isOssErr(t.error,t.error_code)){oss=true;break}}
 sumEl.innerHTML=(oss?'<div class=ossban>⚠ OSS 链接已过期（403），设备下载被拒——请重新生成签名 URL 并新建任务</div>':'')+`<b>#${esc(j.id)}</b><span>${esc(this.KIND_LBL[j.kind]||j.kind||'任务')} → <b>${esc(j.version||'—')}</b></span><span class="tstat ${st}">${this.STAT_LBL[st]||esc(st)}</span><span>共 <b>${total}</b> 台</span><span>成功 <b style="color:#15803d">${cnt.success}</b></span><span>失败 <b style="color:#b91c1c">${cnt.failed}</b></span><span>阻断 <b style="color:#b45309">${cnt.blocked}</b></span><span>进行中 <b style="color:#1d4ed8">${cnt.running}</b></span><span>等待 <b>${cnt.pending}</b></span>${el!=null?`<span>耗时 <b>${this.fmtDur2(el)}</b></span>`:''}${created?`<span>发起于 ${esc(this.fmtClock(created))}</span>`:''}${j.oss_url_expires?`<span>链接有效期 ${esc(this.fmtRemain(j.oss_url_expires*1000-Date.now()))}</span>`:''}`;
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
 if(type==='webfix'){loadWebfix();return}   // 补配任务事件（KVWO-19 共流）：只驱动总览刷新，不进升级视图
 if(type==='webfix_log')return;             // 补配日志增量：升级日志面板不消费
 if(type==='job'||m.job){
  const j=this.normJob(m.job||m);
  if(j.id==null)return;
  if(!this.job||this.job.id!==j.id)this.adoptJob(j);else{this.job=j}
  this.renderJob();
  if(!this.active()){this.teardown();this.loadJobDetail(j.id)}
  return}
 if(type==='pkg'){
  if(this.shaComputing&&m.version!=null&&String(m.version)===String(this.param.version)&&(!m.arch||String(m.arch)===String(this.param.arch))){
   if(m.status==='ok'&&m.sha256){this.stopShaPoll();this.shaComputing=false;this.param.sha256=String(m.sha256).toLowerCase();this.param.shaSrc='server';if(m.size!=null)this.param.size=m.size;this.renderParam()}
   else this.pollShaTick();
  }else if(m.status==='ok')this.loadRegistry();
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
        # 批量升级：任务（一次批量操作）与逐台子任务；日志随子任务入库，
        # 重启后仍可回看；每日维护线程按保留窗口清理。签名 URL 只存脱敏摘要
        # （文件名 + 过期时间），完整 URL 仅存在于运行期内存与设备端命令。
        self._execute(
            "CREATE TABLE IF NOT EXISTS upgrade_jobs ("
            "id INT NOT NULL PRIMARY KEY, "
            "kind VARCHAR(16) NOT NULL, "               # precheck/download/prepare/install/verify/cleanup/upgrade
            "version VARCHAR(32) NOT NULL DEFAULT '', "
            "filename VARCHAR(255) NOT NULL DEFAULT '', "  # OSS 包文件名（脱敏摘要，不含签名）
            "oss_url_expires DATETIME NULL, "            # 签名链接过期时间（无则 NULL）
            "params TEXT NULL, "                        # 创建参数快照（sha256/old_tag 等，不含 URL）
            "status VARCHAR(16) NOT NULL DEFAULT 'running', "  # running/completed
            "total INT NOT NULL DEFAULT 0, "
            "created_at DATETIME NOT NULL, "
            "finished_at DATETIME NULL, "
            "KEY idx_created (created_at)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
        self._execute(
            "CREATE TABLE IF NOT EXISTS upgrade_tasks ("
            "id INT NOT NULL PRIMARY KEY, "
            "job_id INT NOT NULL, "
            "hostname VARCHAR(64) NOT NULL, "
            "ssh_port INT NULL, "
            "stage VARCHAR(16) NOT NULL DEFAULT '', "   # 当前/最后阶段
            "status VARCHAR(16) NOT NULL DEFAULT 'queued', "  # queued/running/success/failed/blocked
            "error VARCHAR(500) NOT NULL DEFAULT '', "
            "error_code VARCHAR(32) NOT NULL DEFAULT '', "  # 结构化错误码（如 oss_expired）
            "log LONGTEXT NULL, "
            "started_at DATETIME NULL, "
            "finished_at DATETIME NULL, "
            "KEY idx_job (job_id), "
            "KEY idx_host (hostname)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
        # 包注册表：sha256 基准缓存（服务端一次性从 OSS 拉包计算），永不清理
        self._execute(
            "CREATE TABLE IF NOT EXISTS pkg_registry ("
            "version VARCHAR(32) NOT NULL, "
            "arch VARCHAR(16) NOT NULL, "              # rk3588-arm64 / amd64
            "filename VARCHAR(255) NOT NULL DEFAULT '', "
            "sha256 CHAR(64) NOT NULL, "
            "size BIGINT NOT NULL DEFAULT 0, "
            "first_seen DATETIME NOT NULL, "
            "UNIQUE KEY uq_ver_arch (version, arch)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
        # webfix 台账（KVWO-17/19/44）：每次补配一行（auto 一次性防重靠 trigger_type='auto'
        # 的历史行；attempts 为累计次数）；与 devices 同为永不清理的台账，
        # run_maintenance 的 30 天保留不涉及此表。log 随子任务入库，重启后仍可回看。
        # status 三态收口（KVWO-44，对齐父需求）：success / failed（环境性失败，
        # 排障后重试可成功）/ blocked（阻断类：前置不满足或确定性错配，重试同样
        # 结果，需人工介入）；error_code 为细分码（如 arch_unknown / proxy_conflict）。
        self._execute(
            "CREATE TABLE IF NOT EXISTS webfix_tasks ("
            "id INT NOT NULL PRIMARY KEY, "
            "hostname VARCHAR(64) NOT NULL, "
            "trigger_type VARCHAR(8) NOT NULL, "         # auto / manual
            "status VARCHAR(16) NOT NULL DEFAULT 'queued', "  # queued/running/success/failed/blocked
            "error VARCHAR(500) NOT NULL DEFAULT '', "
            "error_code VARCHAR(32) NOT NULL DEFAULT '', "   # 细分码（KVWO-44；空 = 未分类）
            "attempts INT NOT NULL DEFAULT 1, "           # 该设备累计补配次数
            "log LONGTEXT NULL, "
            "created_at DATETIME NOT NULL, "
            "finished_at DATETIME NULL, "
            "KEY idx_host (hostname), "
            "KEY idx_created (created_at)"
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
        rows = self._execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=DATABASE() AND table_name='devices'", fetch="all")
        cols = {r[0] for r in rows or []}
        if "hw_model" not in cols:
            self._execute("ALTER TABLE devices ADD COLUMN hw_model VARCHAR(64) NOT NULL DEFAULT ''")
        if "ip" not in cols:
            self._execute("ALTER TABLE devices ADD COLUMN ip VARCHAR(64) NOT NULL DEFAULT ''")
        # 老库补列（返工前版本建的 upgrade_* 表）
        rows = self._execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=DATABASE() AND table_name='upgrade_jobs'", fetch="all")
        jcols = {r[0] for r in rows or []}
        if jcols and "filename" not in jcols:
            self._execute("ALTER TABLE upgrade_jobs ADD COLUMN filename VARCHAR(255) NOT NULL DEFAULT ''")
        if jcols and "oss_url_expires" not in jcols:
            self._execute("ALTER TABLE upgrade_jobs ADD COLUMN oss_url_expires DATETIME NULL")
        rows = self._execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=DATABASE() AND table_name='upgrade_tasks'", fetch="all")
        tcols = {r[0] for r in rows or []}
        if tcols and "error_code" not in tcols:
            self._execute("ALTER TABLE upgrade_tasks ADD COLUMN error_code VARCHAR(32) NOT NULL DEFAULT ''")
        # 老库补列（KVWO-44 前的 webfix_tasks 无 error_code）
        rows = self._execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema=DATABASE() AND table_name='webfix_tasks'", fetch="all")
        wcols = {r[0] for r in rows or []}
        if wcols and "error_code" not in wcols:
            self._execute("ALTER TABLE webfix_tasks ADD COLUMN error_code VARCHAR(32) NOT NULL DEFAULT ''")

    def _load_roster(self):
        rows = self._execute(
            "SELECT hostname, IFNULL(site,''), IFNULL(owner,''), IFNULL(hardware,''), retired, "
            "IFNULL(hw_model,''), IFNULL(ip,''), ssh_port, IFNULL(device_id,'') FROM devices",
            fetch="all")
        self.roster = {r[0]: {"site": r[1], "owner": r[2], "hardware": r[3], "retired": bool(r[4]),
                              "hw_model": r[5], "ip": r[6], "ssh_port": r[7], "device_id": r[8]}
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
                # 每小时只刷新一次登记表快照（拾取人工在库里改的 site/owner 等）；
                # 历史数据的删除统一交给每日维护线程（run_maintenance）。
                self._last_retention_at = time.time()
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

    # --- 批量升级任务持久化 -------------------------------------------------
    def create_upgrade_job(self, job_id, kind, version, params_json, total, created_at,
                           filename="", oss_url_expires=None):
        self._execute(
            "INSERT INTO upgrade_jobs (id, kind, version, filename, oss_url_expires, "
            "params, status, total, created_at) VALUES (%s, %s, %s, %s, %s, %s, 'running', %s, %s)",
            (job_id, kind, version, filename[:255], oss_url_expires, params_json, total, created_at))

    def create_upgrade_task(self, task_id, job_id, hostname, ssh_port):
        self._execute(
            "INSERT INTO upgrade_tasks (id, job_id, hostname, ssh_port, stage, status) "
            "VALUES (%s, %s, %s, %s, '', 'queued')",
            (task_id, job_id, hostname, ssh_port))

    def create_upgrade_task_blocked(self, task_id, job_id, hostname, ssh_port, error, now):
        """创建即 blocked（无 SSH 端口/机型未知/已有任务在跑等）。"""
        self._execute(
            "INSERT INTO upgrade_tasks (id, job_id, hostname, ssh_port, stage, status, error, started_at, finished_at) "
            "VALUES (%s, %s, %s, %s, '', 'blocked', %s, %s, %s)",
            (task_id, job_id, hostname, ssh_port, error[:500], now, now))

    def update_upgrade_task(self, task_id, stage=None, status=None, error=None,
                            error_code=None, log=None, started_at=None, finished_at=None):
        sets, args = [], []
        if stage is not None:
            sets.append("stage=%s"); args.append(stage)
        if status is not None:
            sets.append("status=%s"); args.append(status)
        if error is not None:
            sets.append("error=%s"); args.append(error[:500])
        if error_code is not None:
            sets.append("error_code=%s"); args.append(error_code[:32])
        if log is not None:
            sets.append("log=%s"); args.append(log)
        if started_at is not None:
            sets.append("started_at=%s"); args.append(started_at)
        if finished_at is not None:
            sets.append("finished_at=%s"); args.append(finished_at)
        if not sets:
            return
        args.append(task_id)
        self._execute(f"UPDATE upgrade_tasks SET {', '.join(sets)} WHERE id=%s", args)

    def finish_upgrade_job(self, job_id, finished_at):
        self._execute("UPDATE upgrade_jobs SET status='completed', finished_at=%s WHERE id=%s",
                      (finished_at, job_id))

    def upgrade_job_view(self, job_id):
        """单任务视图（含逐台状态）；不存在返回 None。签名 URL 只回显脱敏摘要。"""
        row = self._execute(
            "SELECT id, kind, version, filename, oss_url_expires, status, total, params, "
            "UNIX_TIMESTAMP(created_at), IFNULL(UNIX_TIMESTAMP(finished_at), 0) "
            "FROM upgrade_jobs WHERE id=%s", (job_id,), fetch="one")
        if not row:
            return None
        job = {"id": row[0], "kind": row[1], "version": row[2] or "", "filename": row[3] or "",
               "oss_url_expires": int(row[4].timestamp()) if row[4] else None,
               "status": row[5], "total": row[6],
               "params": json.loads(row[7]) if row[7] else {},
               "created_at": row[8], "finished_at": row[9] or None, "tasks": self.upgrade_tasks_view(job_id)}
        job["counts"] = _task_counts(job["tasks"])
        return job

    def upgrade_tasks_view(self, job_id):
        rows = self._execute(
            "SELECT t.id, t.hostname, t.stage, t.status, t.error, t.error_code, "
            "IFNULL(UNIX_TIMESTAMP(t.started_at), 0), IFNULL(UNIX_TIMESTAMP(t.finished_at), 0), "
            "IFNULL(LENGTH(t.log), 0) "
            "FROM upgrade_tasks t WHERE t.job_id=%s ORDER BY t.id", (job_id,), fetch="all")
        out = []
        for tid, hostname, stage, status, error, error_code, started, finished, chars in rows or []:
            out.append({"id": tid, "hostname": hostname, "stage": stage or "",
                        "stage_cn": STAGE_CN.get(stage or "", stage or ""), "status": status,
                        "error": error or "", "error_code": error_code or "",
                        "started_at": started or None,
                        "finished_at": finished or None, "chars": int(chars or 0)})
        return out

    def upgrade_jobs_list(self, limit=50):
        rows = self._execute(
            "SELECT j.id, j.kind, j.version, j.filename, IFNULL(UNIX_TIMESTAMP(j.oss_url_expires), 0), "
            "j.status, j.total, "
            "UNIX_TIMESTAMP(j.created_at), IFNULL(UNIX_TIMESTAMP(j.finished_at), 0), "
            "IFNULL(SUM(t.status='success'), 0), IFNULL(SUM(t.status='failed'), 0), "
            "IFNULL(SUM(t.status='blocked'), 0) "
            "FROM upgrade_jobs j LEFT JOIN upgrade_tasks t ON t.job_id=j.id "
            "GROUP BY j.id ORDER BY j.id DESC LIMIT %s", (limit,), fetch="all")
        return [{"id": r[0], "kind": r[1], "version": r[2] or "", "filename": r[3] or "",
                 "oss_url_expires": r[4] or None,
                 "status": r[5], "total": r[6],
                 "created_at": r[7], "finished_at": r[8] or None,
                 "counts": {"success": int(r[9]), "failed": int(r[10]), "blocked": int(r[11])}}
                for r in rows or []]

    # --- 包注册表（sha256 基准缓存） ------------------------------------------
    def register_pkg(self, version, arch, filename, sha256, size):
        """注册/刷新 (version, arch) 基准；重复升级直接命中。"""
        self._execute(
            "INSERT INTO pkg_registry (version, arch, filename, sha256, size, first_seen) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE filename=VALUES(filename), sha256=VALUES(sha256), "
            "size=VALUES(size), first_seen=VALUES(first_seen)",
            (version, arch, filename[:255], sha256, size, datetime.now()))

    def pkg_registry_get(self, version, arch):
        row = self._execute(
            "SELECT version, arch, filename, sha256, size, "
            "UNIX_TIMESTAMP(first_seen) FROM pkg_registry WHERE version=%s AND arch=%s",
            (version, arch), fetch="one")
        if not row:
            return None
        return {"version": row[0], "arch": row[1], "filename": row[2], "sha256": row[3],
                "size": int(row[4] or 0), "first_seen": row[5]}

    def pkg_registry_list(self):
        rows = self._execute(
            "SELECT version, arch, filename, sha256, size, UNIX_TIMESTAMP(first_seen) "
            "FROM pkg_registry", fetch="all")
        return [{"version": r[0], "arch": r[1], "filename": r[2], "sha256": r[3],
                 "size": int(r[4] or 0), "first_seen": r[5]} for r in rows or []]

    def upgrade_task_log(self, job_id, task_id=None, hostname=None):
        """取库内日志（进程重启后的回看亮底）；返回 (log, status) 或 None。"""
        if task_id is not None:
            row = self._execute(
                "SELECT log, status FROM upgrade_tasks WHERE id=%s AND job_id=%s",
                (task_id, job_id), fetch="one")
        else:
            row = self._execute(
                "SELECT log, status FROM upgrade_tasks WHERE job_id=%s AND hostname=%s "
                "ORDER BY id DESC LIMIT 1", (job_id, hostname or ""), fetch="one")
        if not row:
            return None
        return row[0] or "", row[1] or ""

    def max_upgrade_id(self):
        """upgrade_jobs/upgrade_tasks 共用的最大 id（新进程续号，防重启后主键冲突）。"""
        row = self._execute(
            "SELECT GREATEST(IFNULL((SELECT MAX(id) FROM upgrade_jobs), 0), "
            "IFNULL((SELECT MAX(id) FROM upgrade_tasks), 0))", fetch="one")
        return int(row[0] or 0) if row else 0

    def reconcile_upgrade_jobs(self):
        """进程重启后把遗留 queued/running 的子任务标记为 failed（设备端安装
        可能仍在后台进行，日志里说明用「验证」补查），并结束对应任务。"""
        rows = self._execute(
            "SELECT id, job_id, hostname FROM upgrade_tasks "
            "WHERE status IN ('queued', 'running')", fetch="all")
        if not rows:
            return 0
        now = datetime.now()
        for tid, job_id, hostname in rows:
            self.update_upgrade_task(
                tid, status="failed", error="面板进程重启，任务中断（安装可能仍在设备后台进行，可用「验证」补查）",
                finished_at=now)
        jobs = {r[1] for r in rows}
        for job_id in jobs:
            self.finish_upgrade_job(job_id, now)
        return len(rows)

    # --- webfix 台账（KVWO-17/19/44）----------------------------------------------
    def create_webfix_task(self, task_id, hostname, trigger_type, attempts, created_at,
                           status="queued", error="", error_code="", finished_at=None):
        """落一行台账；KVWO-44 起支持直接以终态落行（auto 阻断类前置不满足：
        不跑设备端，状态直接 blocked 收口，占一次性名额、可手动重试）。"""
        self._execute(
            "INSERT INTO webfix_tasks (id, hostname, trigger_type, status, error, "
            "error_code, attempts, created_at, finished_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (task_id, hostname, trigger_type, status, (error or "")[:500],
             (error_code or "")[:32], attempts, created_at, finished_at))

    def update_webfix_task(self, task_id, status=None, error=None, error_code=None,
                           log=None, finished_at=None):
        sets, args = [], []
        if status is not None:
            sets.append("status=%s"); args.append(status)
        if error is not None:
            sets.append("error=%s"); args.append(error[:500])
        if error_code is not None:
            sets.append("error_code=%s"); args.append(error_code[:32])
        if log is not None:
            sets.append("log=%s"); args.append(log)
        if finished_at is not None:
            sets.append("finished_at=%s"); args.append(finished_at)
        if not sets:
            return
        args.append(task_id)
        self._execute(f"UPDATE webfix_tasks SET {', '.join(sets)} WHERE id=%s", args)

    def webfix_has_auto(self, hostname):
        """该设备是否已有自动触发记录（无论成败）——自动补配严格一次的判据。"""
        row = self._execute(
            "SELECT 1 FROM webfix_tasks WHERE hostname=%s AND trigger_type='auto' LIMIT 1",
            (hostname,), fetch="one")
        return bool(row)

    def webfix_attempts(self, hostname):
        row = self._execute(
            "SELECT MAX(attempts) FROM webfix_tasks WHERE hostname=%s", (hostname,), fetch="one")
        return int(row[0] or 0) if row else 0

    def webfix_tasks_view(self):
        """各设备补配状态：每设备取最新一行（含 error / error_code / 累计 attempts / 日志长度）。"""
        rows = self._execute(
            "SELECT w.id, w.hostname, w.trigger_type, w.status, w.error, w.error_code, "
            "w.attempts, "
            "UNIX_TIMESTAMP(w.created_at), IFNULL(UNIX_TIMESTAMP(w.finished_at), 0), "
            "IFNULL(LENGTH(w.log), 0), IFNULL(d.site, '') "
            "FROM webfix_tasks w "
            "JOIN (SELECT hostname, MAX(id) AS mid FROM webfix_tasks GROUP BY hostname) t "
            "ON w.id = t.mid "
            "LEFT JOIN devices d ON d.hostname = w.hostname "
            "ORDER BY w.id DESC", fetch="all")
        return [{"task_id": r[0], "hostname": r[1], "trigger_type": r[2], "status": r[3],
                 "error": r[4] or "", "error_code": r[5] or "", "attempts": int(r[6] or 1),
                 "created_at": r[7],
                 "finished_at": r[8] or None, "chars": int(r[9] or 0), "site": r[10] or ""}
                for r in rows or []]

    def webfix_task_log(self, task_id):
        """取库内日志（进程重启后回看）；返回 (log, status, hostname) 或 None。"""
        row = self._execute(
            "SELECT log, status, hostname FROM webfix_tasks WHERE id=%s", (task_id,), fetch="one")
        if not row:
            return None
        return row[0] or "", row[1] or "", row[2] or ""

    def max_webfix_id(self):
        row = self._execute("SELECT MAX(id) FROM webfix_tasks", fetch="one")
        return int(row[0] or 0) if row else 0

    def reconcile_webfix_tasks(self):
        """进程重启后把遗留 queued/running 的补配任务标记为 failed（自动触发的
        一次性语义不固中断而重置：台账里 auto 记录仍在，不再自动触发；可手动重试）。"""
        rows = self._execute(
            "SELECT id, hostname FROM webfix_tasks WHERE status IN ('queued', 'running')",
            fetch="all")
        if not rows:
            return 0
        now = datetime.now()
        for tid, hostname in rows:
            self.update_webfix_task(
                tid, status="failed",
                error="面板进程重启，补配任务中断（若配置已写入则保留现场；可手动重试）",
                finished_at=now)
        return len(rows)

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
        else:
            # webfix 自动检测（KVWO-17/19/44）：挂入现有刷新循环，每轮快照后推进
            # 「上线事件 + 稳定窗口」状态机——新候选记窗口起点，掉线作废重计，
            # 连续在线满 WEBFIX_DELAY（默认 60s）且仍缺 web 代理才触发（判据只看
            # 代理列表；version_error 单独出现不触发，探测瞬时失败 ≠ 缺配置）。
            try:
                webfix_auto_detect()
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


# ============================================================================
# 批量升级：设备端阶段脚本（以字符串常量内嵌，随服务分发）
# 来源：update/scripts/*.sh（同步日期 2026-10-15，含 SOP 15 条坑位护栏）。
# 分发通道（设计变更 2026-10-14）：设备直连 OSS 签名 URL 下载，不经服务器。
# 与源文件的差异：
#   precheck.sh  —— 增加 NOW_EPOCH 时钟偏差检查（SOP 坑 #10：>5min 预检告警）；
#                   +PKG_HOME/PKG_ARCH 按机型适配家目录兜底路径（amd64→/home/archebase）；
#                   其余与源文件一致（OSS_URL 连通性探测保留）
#   download.sh  —— 设备直连 OSS 下载，在源文件基础上增加：403 预探（链接过期
#                   明确报 OSS_HTTP=403）+ 下载停滞看门狗（size 持续不变杀 curl
#                   并 pgrep 验尸，SOP 坑 #13）+ 周期性 size 心跳输出
#   prepare.sh   —— PKG_SRC 默认改为 $BASE/pkg.tar.gz（下载阶段落点）；其余不变
#   run-install.sh / verify.sh / cleanup.sh —— 原样内嵌，无改动
# 所有脚本仍以 stdin 推送 + bash -s 执行，参数走远程命令行环境变量。
# ============================================================================
SCRIPT_PRECHECK = r'''#!/usr/bin/env bash
# [内嵌自 update/scripts/precheck.sh · 2026-10-15；差异：+NOW_EPOCH 时钟检查]
# 设备端升级前预检（经 FRP SSH 推送执行）。可选环境变量：
#   OSS_URL（OSS 签名链接连通性探测）、NOW_EPOCH（面板 epoch，时钟偏差告警）、
#   PKG_HOME/PKG_ARCH（家目录兜底路径按机型适配，默认 /home/orangepi + rk3588-arm64）
set -u
echo "### host=$(hostname) arch=$(uname -m) date=$(date -u +%FT%TZ)"
pw=${DEPLOY_PW:-}
s() { printf '%s\n' "$pw" | sudo -S -p '' "$@" 2>/dev/null; }
echo "### version: $(s sed -n 's/^EGO_PORTAL_RELEASE_VERSION=//p' /etc/ego-portal/device.env)"
s grep -E '^(DEVICE_ID|DEVICE_NAME|DEVICE_HOSTNAME|PUBLIC_URL)=' /etc/ego-portal/device.env
echo "### service: $(systemctl is-active ego-portal.service 2>/dev/null) | frpc=$(systemctl is-active frpc 2>/dev/null)"
echo "### disk: $(df -BM / | awk 'NR==2{print $4"M free"}')"
echo "### camera-containers: $(s docker ps --format '{{.Names}}' 2>/dev/null | grep -c decxin || true)"
echo "### active sessions (非空=禁止升级):"
s python3 -c "import sqlite3;c=sqlite3.connect('file:/data/collector/db/collector.sqlite?mode=ro',uri=True);print(c.execute(\"select id,status from collection_sessions where status in ('RECORDING','STOPPING','VALIDATING')\").fetchall())" || echo "check-skipped"
if [ -n "${NOW_EPOCH:-}" ]; then
  drift=$(( $(date +%s) - NOW_EPOCH ))
  echo "### clock-drift: ${drift}s"
fi
echo "### pkg:"
for p in "${PKG_SRC:-}" "${PKG_HOME:-/home/orangepi}"/ego-portal-*-"${PKG_ARCH:-rk3588-arm64}".tar.gz /var/tmp/ego-*/pkg.tar.gz; do
  [ -f "$p" ] && echo "found: $p size=$(stat -c %s "$p")"
done 2>/dev/null | sort -u
[ -n "${OSS_URL:-}" ] && { echo -n "### OSS: "; curl -s -o /dev/null -w 'http=%{http_code}\n' --max-time 20 -r 0-0 "$OSS_URL" || echo FAIL; }
echo "### done"
'''

SCRIPT_DOWNLOAD = r'''#!/usr/bin/env bash
# [内嵌自 update/scripts/download.sh · 2026-10-15；差异：+403 预探 + 停滞看门狗]
# 设备端直连 OSS 签名 URL 下载安装包（断点续传，/tmp 是 tmpfs 一律放 /var/tmp）。
# 必需环境变量：OSS_URL、PKG_VERSION；可选：DEST、STALL_TIMEOUT（默认 180s）
set -u
: "${OSS_URL:?need OSS_URL}"
PKG_VERSION="${PKG_VERSION:?need PKG_VERSION, e.g. v0.4.6}"
DEST="${DEST:-/var/tmp/ego-${PKG_VERSION}}"
STALL_TIMEOUT="${STALL_TIMEOUT:-180}"
mkdir -p "$DEST" && cd "$DEST"
probe=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 -r 0-0 "$OSS_URL" || echo 000)
echo "probe_http=$probe"
case "$probe" in
  200|206) ;;
  403) echo "OSS_HTTP=403"; echo "ERROR_LINK_EXPIRED"; exit 10 ;;
  *)    echo "OSS_HTTP=$probe"; echo "ERROR_LINK_UNREACHABLE"; exit 11 ;;
esac
echo "==> downloading to $DEST/pkg.tar.gz"
curl -fL --retry 4 --retry-delay 2 --connect-timeout 15 -C - -o pkg.tar.gz "$OSS_URL" &
cpid=$!
last=0; stall=0
while kill -0 "$cpid" 2>/dev/null; do
  sleep 15
  sz=$(stat -c %s pkg.tar.gz 2>/dev/null || echo 0)
  echo "progress size=$sz"
  if [ "$sz" = "$last" ]; then stall=$((stall+15)); else stall=0; last=$sz; fi
  if [ "$stall" -ge "$STALL_TIMEOUT" ]; then
    echo "DOWNLOAD_STALLED size_unchanged=${stall}s"
    kill "$cpid" 2>/dev/null
    break
  fi
done
wait "$cpid"; rc=$?
echo "curl_exit=$rc"
echo "curl_procs_after=$(pgrep -c -x curl || true)"   # 验尸（坑 #13：无声死亡）
if [ "$rc" -eq 22 ]; then  # HTTP 错误（含 403）：再探一次区分链接过期
  probe2=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 -r 0-0 "$OSS_URL" || echo 000)
  if [ "$probe2" = "403" ]; then echo "OSS_HTTP=403"; echo "ERROR_LINK_EXPIRED"; exit 10; fi
fi
if [ "$rc" -eq 0 ]; then
  echo "size=$(stat -c %s pkg.tar.gz)"
  echo "sha256=$(sha256sum pkg.tar.gz | awk '{print $1}')"
  echo "DOWNLOAD_OK   # 与面板基准 sha256 比对，一致才可安装"
fi
exit "$rc"
'''

SCRIPT_PREPARE = r'''#!/usr/bin/env bash
# [内嵌自 update/scripts/prepare.sh · 2026-10-15；PKG_SRC 默认改为 $BASE/pkg.tar.gz]
# 设备端：sha256 校验 → 解压 → --prepare-only 生成安装 bundle（继承设备身份）。
# 必需：PKG_VERSION、SHA256；可选：PKG_SRC、BASE、PKG_ARCH（默认 rk3588-arm64，amd64 机型传 amd64）
set -euo pipefail
PKG_VERSION="${PKG_VERSION:?need PKG_VERSION, e.g. v0.4.6}"
SHA256="${SHA256:?need SHA256 of the official tarball}"
BASE="${BASE:-/var/tmp/ego-${PKG_VERSION}}"
PKG_ARCH="${PKG_ARCH:-rk3588-arm64}"
PKG_TGZ="${PKG_SRC:-${BASE}/pkg.tar.gz}"
PKGDIR="${BASE}/ego-portal-${PKG_VERSION}-${PKG_ARCH}"
BUNDLE="${BASE}/bundle"
pw="${DEPLOY_PW:?need DEPLOY_PW}"
[ -f "$PKG_TGZ" ] || { echo "pkg missing: $PKG_TGZ"; exit 1; }
echo "==> verify sha256"
printf '%s  %s\n' "$SHA256" "$PKG_TGZ" | sha256sum -c - || exit 1
if [ ! -f "$PKGDIR/install-ego-portal.sh" ]; then
  echo "==> extracting package"
  mkdir -p "$BASE"
  tar -xzf "$PKG_TGZ" -C "$BASE"
fi
chmod +x "$PKGDIR/install-ego-portal.sh" "$PKGDIR/lib/"*.sh
printf '%s\n' "$pw" | sudo -S -p '' -v
sudo -n cat /etc/ego-portal/device.env > "$BASE/device.env.work"
chmod 600 "$BASE/device.env.work"
rm -rf "$BUNDLE"
cd "$PKGDIR"
echo "==> prepare-only"
./install-ego-portal.sh --prepare-only "$BUNDLE" --device-env "$BASE/device.env.work" --reuse-device-env >/dev/null
grep -E '^(DEVICE_ID|DEVICE_NAME|RELEASE_VERSION)=' "$BUNDLE/deployment.conf"
echo "PREPARE_OK"
'''

SCRIPT_RUN_INSTALL = r'''#!/usr/bin/env bash
# [内嵌自 update/scripts/run-install.sh · 2026-10-15，无改动]
# 设备端安装执行器：写入 $BASE/run-install.sh 后以
#   nohup setsid bash $BASE/run-install.sh > $BASE/install.log 2>&1 < /dev/null &
# 启动（nohup+setsid 保证 SSH/FRP 断连不影响安装）。
set -u
pw="${DEPLOY_PW:?need DEPLOY_PW}"
BASE="${BASE:-/var/tmp/ego-${PKG_VERSION:-}}"
[ -n "$BASE" ] || { echo "need BASE or PKG_VERSION"; exit 1; }
exec 9>"$BASE/install.lock"
flock -n 9 || { echo "another install is running"; exit 42; }   # 防双安装器
printf '%s\n' "$pw" | sudo -S -p '' -v || exit 1
( while sudo -n true 2>/dev/null; do sleep 45; done ) &           # sudo 时间戳保活
keeper=$!
trap 'kill $keeper 2>/dev/null || true' EXIT INT TERM
sudo -n bash "$BASE/bundle/install-remote.sh"
rc=$?
echo "INSTALL_EXIT=$rc"
exit $rc
'''

SCRIPT_VERIFY = r'''#!/usr/bin/env bash
# [内嵌自 update/scripts/verify.sh · 2026-10-15]
# 设备端升级后验证。成功标准：version=新版本 / service=active / http=200 /
# api 含新版本 / 常驻容器新镜像 / frpc=active（由面板侧比对判分）。
# 容器判分按机型（面板侧，2026-10-16 KVWO-14）：V1(rk3588)=4 常驻容器全为新版镜像；
# V0(amd64)=运行中 portal 容器全为新版且 ≥ 核心 2 个（backend+相机模块）。
set -u
pw=${DEPLOY_PW:-}
s() { printf '%s\n' "$pw" | sudo -S -p '' "$@" 2>/dev/null; }
echo "host=$(hostname)"
echo "version=$(s sed -n 's/^EGO_PORTAL_RELEASE_VERSION=//p' /etc/ego-portal/device.env)"
echo "device=$(s grep -E '^DEVICE_NAME=' /etc/ego-portal/device.env)"
echo "service=$(systemctl is-active ego-portal.service 2>/dev/null)"
echo "http=$(curl -s -o /dev/null -w '%{http_code}' --max-time 8 http://127.0.0.1/)"
echo "api=$(curl -s --max-time 8 http://127.0.0.1/api/setup/status | head -c 200)"
echo "frpc=$(systemctl is-active frpc 2>/dev/null)"
echo "containers:"
s docker ps --format '{{.Names}} | {{.Image}} | {{.Status}}'
echo "current=$(readlink -f /opt/ego-portal/current 2>/dev/null)"
echo "disk: $(df -h / | awk 'NR==2{print $4" free"}')"
'''

SCRIPT_CLEANUP = r'''#!/usr/bin/env bash
# [内嵌自 update/scripts/cleanup.sh · 2026-10-15，无改动]
# 设备端清理：staging 目录 + （可选）被终止部署遗留的 Created 状态旧容器。
# 可选：BASE、OLD_TAG（仅删 Created 且镜像含该 tag 的容器）
set -u
BASE="${BASE:-/var/tmp/ego-${PKG_VERSION:-}}"
pw=${DEPLOY_PW:-}
s() { printf '%s\n' "$pw" | sudo -S -p '' "$@" 2>/dev/null; }
echo "### $(hostname)"
if [ -n "${OLD_TAG:-}" ]; then
  ids=$(s docker ps -a --format '{{.ID}} {{.Image}} {{.Status}}' | awk -v t="$OLD_TAG" '$3=="Created" && $2 ~ t {print $1}')
  if [ -n "$ids" ]; then
    # shellcheck disable=SC2086
    s docker rm $ids >/dev/null && echo "removed created-old containers: $ids"
  else
    echo "no created-old containers"
  fi
fi
[ -n "$BASE" ] && [ -d "$BASE" ] && { du -sh "$BASE" 2>/dev/null; rm -rf "$BASE"; echo "staging cleaned"; }
echo "health: portal=$(systemctl is-active ego-portal.service 2>/dev/null) frpc=$(systemctl is-active frpc 2>/dev/null) api=$(curl -s --max-time 5 http://127.0.0.1/api/setup/status | grep -oP '"release_version":"[^"]*"' | head -1)"
echo "disk: $(df -h / | awk 'NR==2{print $4" free"}')"
echo "### done"
'''


SCRIPT_WEBFIX = r'''#!/usr/bin/env bash
# 设备端 web 代理补配（KVWO-17/19 · 2026-10-16）。经 FRP SSH stdin 推送执行，
# 参数走远程命令行环境变量：WEBFIX_HOSTNAME（如 ep-000160）、WEBFIX_PORT
# （如 30160）、DEPLOY_PW（sudo 凭据，经 stdin；严禁密码管道接 tee -a —— SOP #4）。
# 流程：flock 防并发 → 备份 frpc.toml → 幂等检查（同名代理已存在：端口一致 =
# 无事可做；不一致 = 报错人工介入，不覆盖）→ 临时文件追加 [[proxies]] 块 →
# frpc verify → sudo cp 覆盖（保留原文件属主/权限）→ 延迟重启 frpc。重启会闪断
# 自身 SSH 隧道（预期内），故以 setsid 后台延迟执行后立即退出，回连与版本闭环
# 校验（/api/setup/status 比对 device_id）由面板侧完成。
# 结果标记：WEBFIX_APPLIED / WEBFIX_ALREADY / ERROR_*（失败不回滚，保留现场）。
set -u
pw="${DEPLOY_PW:?need DEPLOY_PW}"
s() { printf '%s\n' "$pw" | sudo -S -p '' "$@" 2>/dev/null; }
HOST="${WEBFIX_HOSTNAME:?need WEBFIX_HOSTNAME, e.g. ep-000160}"
PORT="${WEBFIX_PORT:?need WEBFIX_PORT, e.g. 30160}"
printf '%s' "$HOST" | grep -qE '^ep-[0-9]+$' || { echo "ERROR_BAD_HOSTNAME: $HOST 不符合 ep-<编号> 约定"; exit 18; }
case "$PORT" in ''|*[!0-9]*) echo "ERROR_BAD_PORT: $PORT"; exit 19;; esac
CONF=/etc/frp/frpc.toml
exec 9>/var/tmp/webfix.lock
flock -n 9 || { echo "ERROR_BUSY: 另一个 webfix 正在此设备上运行"; exit 20; }
echo "webfix: host=$HOST web=$PORT conf=$CONF frpc=$(systemctl is-active frpc 2>/dev/null || true)"
s test -f "$CONF" || { echo "ERROR_NO_CONF: $CONF 不存在（frpc 配置路径与预期不符，需人工确认）"; exit 11; }
# 1) 幂等检查：同名代理块已存在 → remotePort 一致 = 无事可做；不一致 = 不覆盖，人工介入
line=$(s grep -nE "^[[:space:]]*name[[:space:]]*=[[:space:]]*\"${HOST}-web\"" "$CONF" | head -1 || true)
if [ -n "$line" ]; then
  ln=${line%%:*}
  existing=$(s tail -n +"$((ln + 1))" "$CONF" | sed -n '/^[[:space:]]*\[\[/q; /remotePort[[:space:]]*=/p' | grep -oE '[0-9]+' | head -1)
  if [ "$existing" = "$PORT" ]; then
    echo "WEBFIX_ALREADY: 代理 ${HOST}-web 已存在且 remotePort=$PORT 一致，未做任何改动"
    exit 0
  fi
  echo "ERROR_PROXY_CONFLICT: 已存在同名代理 ${HOST}-web（remotePort=${existing:-?} ≠ $PORT），不覆盖，需人工介入"
  exit 12
fi
# 2) 备份（时间戳留档，不覆盖历史备份，供人工排查/回滚参考）
BAK="${CONF}.bak-webfix-$(date +%Y%m%d-%H%M%S)"
s cp -p "$CONF" "$BAK" || { echo "ERROR_BACKUP: 备份失败，未改动配置"; exit 13; }
echo "backup: $BAK"
# 3) 临时文件组装（用户态临时文件；sudo cat 读原配置，不依赖其权限）→
#    frpc verify → sudo cp 覆盖（对已存在目标为截断写，保留原属主/权限）
TMP=$(mktemp /var/tmp/frpc-webfix.XXXXXX)
trap 'rm -f "$TMP"' EXIT
s cat "$CONF" > "$TMP" || { echo "ERROR_READ_CONF: 读取 $CONF 失败"; exit 14; }
[ "$(tail -c1 "$TMP" | wc -l)" -gt 0 ] || printf '\n' >> "$TMP"   # 原文件无结尾换行时补一行，防 TOML 粘连
printf '[[proxies]]\nname = "%s-web"\ntype = "tcp"\nlocalIP = "127.0.0.1"\nlocalPort = 80\nremotePort = %s\n' "$HOST" "$PORT" >> "$TMP"
echo "append: ${HOST}-web remotePort=$PORT -> 127.0.0.1:80"
FRPC=$(command -v frpc 2>/dev/null || true)
[ -n "$FRPC" ] || for c in /usr/local/bin/frpc /usr/bin/frpc /opt/frp/frpc; do [ -x "$c" ] && FRPC="$c" && break; done
[ -n "$FRPC" ] || { echo "ERROR_NO_FRPC: 找不到 frpc 可执行文件，需人工确认安装路径"; exit 17; }
printf '%s\n' "$pw" | sudo -S -p '' "$FRPC" verify -c "$TMP" || { echo "ERROR_VERIFY: frpc verify 未通过（见上方输出），未改动 $CONF"; exit 15; }
echo "verify: frpc syntax check pass"
s cp "$TMP" "$CONF" || { echo "ERROR_INSTALL: sudo cp 覆盖 $CONF 失败（备份在 $BAK）"; exit 16; }
s ls -l "$CONF" | sed 's/^/installed: /'
# 4) 延迟重启（setsid 脱离会话：frpc 重启会闪断本 SSH 隧道，本进程须先安全退出；
#    9>&- 不把 flock 句柄遗传给后台进程，锁随本脚本退出即释放）
s sh -c "nohup setsid sh -c 'sleep 2; systemctl restart frpc; sleep 3; systemctl is-active frpc' >/var/tmp/webfix-restart.log 2>&1 9>&- &"
echo "WEBFIX_APPLIED: 已写入 ${HOST}-web（remotePort=$PORT -> 127.0.0.1:80），2s 后重启 frpc（本机隧道秒级闪断为预期）"
exit 0
'''


# ----------------------------------------------------------------------------
# 事件总线（SSE）：任务状态变更 / 逐台阶段流转 / 日志块增量
# ----------------------------------------------------------------------------
class Bus:
    def __init__(self):
        self.subs = []
        self.lock = threading.Lock()

    def subscribe(self):
        q = queue.Queue(maxsize=4000)
        with self.lock:
            self.subs.append(q)
        return q

    def unsubscribe(self, q):
        with self.lock:
            if q in self.subs:
                self.subs.remove(q)

    def publish(self, event):
        with self.lock:
            for q in list(self.subs):
                try:
                    q.put_nowait(event)
                except queue.Full:
                    pass


BUS = Bus()


# ----------------------------------------------------------------------------
# 升级参数与包注册（pkg_registry）：OSS 签名 URL 解析 + sha256 基准缓存。
# 签名 URL 属敏感凭据：只存在于运行期内存与设备端命令行，不进日志/DB/API 回显。
# ----------------------------------------------------------------------------
PKG_NAME_RE = re.compile(r"ego-portal-(v[0-9][0-9A-Za-z.\-]+?)-(rk3588-arm64|amd64)\.tar\.gz")
SAFE_FILENAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*\.tar\.gz$")


def _vkey(version):
    m = re.match(r"v?(\d+)\.(\d+)\.(\d+)(?:[-.](.*))?$", version or "")
    if not m:
        return [0, 0, 0, version or ""]
    return [int(m.group(1)), int(m.group(2)), int(m.group(3)), m.group(4) or ""]


def _sanitize_oss_url(url):
    """签名 URL 的展示形态：仅保留 host + 文件名（凭据全隐）。"""
    try:
        parts = urlsplit(url)
        return f"{parts.scheme}://{parts.netloc}/…/{os.path.basename(parts.path)}"
    except Exception:  # noqa: BLE001
        return "<oss-url>"


def parse_oss_url(url):
    """解析 OSS 签名 URL：文件名/版本/机型识别 + 有效期。

    返回 {ok, error, error_code, filename, version, arch, expires_epoch}。
    有效期来源：Expires（V1 签名，绝对 epoch）或 x-oss-date + x-oss-expires
    （V4 签名，日期 + 时长秒）；均缺失时 expires_epoch=None（无法预警）。
    """
    url = (url or "").strip()
    out = {"ok": False, "error": "", "error_code": "", "filename": "",
           "version": "", "arch": "", "expires_epoch": None}
    if not url.startswith(("http://", "https://")):
        out["error"] = "oss_url 必须是 http(s) 链接"
        return out
    try:
        parts = urlsplit(url)
        qs = parse_qs(parts.query)
    except ValueError:
        out["error"] = "oss_url 无法解析"
        return out
    filename = os.path.basename(unquote(parts.path))
    m = PKG_NAME_RE.search(filename)
    if not m or not SAFE_FILENAME_RE.match(filename):
        out["error"] = (f"无法从 URL 识别安装包（期望文件名形如 "
                        f"ego-portal-<版本>-rk3588-arm64|amd64.tar.gz）：{_sanitize_oss_url(url)}")
        return out
    out["filename"] = filename
    out["version"], out["arch"] = m.group(1), m.group(2)
    # 有效期
    try:
        if qs.get("Expires"):
            out["expires_epoch"] = int(qs["Expires"][0])
        elif qs.get("x-oss-expires") and qs.get("x-oss-date"):
            date = qs["x-oss-date"][0]
            base = datetime.strptime(date, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
            out["expires_epoch"] = int(base.timestamp()) + int(qs["x-oss-expires"][0])
    except (ValueError, OverflowError):
        out["expires_epoch"] = None
    out["ok"] = True
    return out


# 服务端一次性拉包算哈希：同 (version,arch) 只算一次，拉完即删临时文件
_PKG_HASH_JOBS = {}        # (version, arch) -> {status, filename, downloaded, total, error, error_code}
_PKG_HASH_LOCK = threading.Lock()


def start_pkg_hash(oss_url):
    """触发服务端从 OSS 直连拉包计算 sha256（异步；不经 frp，拉完即删）。

    已在注册表命中则直接返回；已在计算中则返回进行态。"""
    meta = parse_oss_url(oss_url)
    if not meta["ok"]:
        return {"ok": False, "error": meta["error"], "error_code": meta["error_code"] or "bad_url"}
    key = (meta["version"], meta["arch"])
    hit = store.pkg_registry_get(meta["version"], meta["arch"])
    if hit and hit.get("sha256"):
        return {"ok": True, "status": "done", "entry": hit}
    if meta["expires_epoch"] is not None and meta["expires_epoch"] <= time.time():
        return {"ok": False, "error": "链接已过期，请重新生成", "error_code": "oss_expired"}
    with _PKG_HASH_LOCK:
        job = _PKG_HASH_JOBS.get(key)
        if job and job["status"] == "computing":
            return {"ok": True, "status": "computing",
                    "version": meta["version"], "arch": meta["arch"],
                    "filename": meta["filename"],
                    "downloaded": job["downloaded"], "total": job["total"]}
        _PKG_HASH_JOBS[key] = {"status": "computing", "filename": meta["filename"],
                               "downloaded": 0, "total": 0, "error": "", "error_code": ""}
    threading.Thread(target=_hash_pkg_from_oss, args=(oss_url, meta), daemon=True).start()
    return {"ok": True, "status": "computing", "version": meta["version"],
            "arch": meta["arch"], "filename": meta["filename"],
            "downloaded": 0, "total": 0}


def _hash_pkg_from_oss(oss_url, meta):
    """直连 OSS 流式拉包 → 边拉边算 sha256 → 入 pkg_registry → 立即删临时文件。

    服务器不存储、不分发安装包；此路径只为建立基准哈希。"""
    key = (meta["version"], meta["arch"])
    job = _PKG_HASH_JOBS[key]
    digest = hashlib.sha256()
    size = 0
    fd, tmp_path = tempfile.mkstemp(prefix="frpmon-pkg-", suffix=".tar.gz",
                                    dir=OSS_HASH_TMP_DIR)
    tmp = os.fdopen(fd, "wb")
    try:
        request = Request(oss_url, method="GET", headers={"User-Agent": "frp-monitor-hash/1.0"})
        with urlopen(request, timeout=60) as resp, tmp:
            total = int(resp.headers.get("Content-Length") or 0)
            job["total"] = total
            deadline = time.time() + OSS_HASH_TIMEOUT
            while True:
                chunk = resp.read(1 << 22)
                if not chunk:
                    break
                if time.time() > deadline:
                    raise TimeoutError(f"拉包超时（>{OSS_HASH_TIMEOUT}s）")
                tmp.write(chunk)
                digest.update(chunk)
                size += len(chunk)
                job["downloaded"] = size
        store.register_pkg(meta["version"], meta["arch"], meta["filename"],
                           digest.hexdigest(), size)
        job.update(status="ok", sha256=digest.hexdigest(), size=size, error="")
        print(f"frp-monitor: pkg_registry +{meta['version']}/{meta['arch']} "
              f"sha256={digest.hexdigest()[:16]}… size={size}（临时文件已删）", flush=True)
        BUS.publish({"type": "pkg", "status": "ok", "version": meta["version"],
                     "arch": meta["arch"], "filename": meta["filename"],
                     "sha256": digest.hexdigest(), "size": size})
    except HTTPError as exc:
        code = "oss_expired" if exc.code == 403 else "oss_unreachable"
        msg = "链接已过期（403），请重新生成" if exc.code == 403 else f"OSS 拉取失败: HTTP {exc.code}"
        job.update(status="failed", error=msg, error_code=code)
        BUS.publish({"type": "pkg", "status": "failed", "version": meta["version"],
                     "arch": meta["arch"], "filename": meta["filename"],
                     "error": msg, "error_code": code})
    except (URLError, TimeoutError, OSError) as exc:
        job.update(status="failed", error=f"OSS 拉取失败: {exc}", error_code="oss_unreachable")
        BUS.publish({"type": "pkg", "status": "failed", "version": meta["version"],
                     "arch": meta["arch"], "filename": meta["filename"],
                     "error": str(exc), "error_code": "oss_unreachable"})
    finally:
        try:
            os.remove(tmp_path)
        except OSError:
            pass


def packages_registry_view():
    """GET /api/upgrade/packages 视图：注册表命中项 + 计算中项（含进度）。"""
    out = []
    computing = []
    with _PKG_HASH_LOCK:
        for (version, arch), job in _PKG_HASH_JOBS.items():
            if job["status"] == "ok":
                continue  # 已入注册表：以 DB 行为准（带 first_seen）
            entry = {"version": version, "arch": arch, "filename": job["filename"],
                     "sha256": "", "size": None, "first_seen": None,
                     "hash_status": job["status"],
                     "downloaded": job.get("downloaded", 0), "total": job.get("total", 0)}
            if job["status"] == "failed":
                entry["error"] = job["error"]
                entry["error_code"] = job["error_code"]
                out.append(entry)
            else:  # computing
                computing.append(entry)
    for row in store.pkg_registry_list():
        if any(e["version"] == row["version"] and e["arch"] == row["arch"] for e in out):
            continue
        out.append({"version": row["version"], "arch": row["arch"],
                    "filename": row["filename"], "sha256": row["sha256"],
                    "size": row["size"], "first_seen": row["first_seen"],
                    "hash_status": "ok"})
    out.sort(key=lambda p: (_vkey(p["version"]), p["arch"]), reverse=True)
    return out + computing


# ----------------------------------------------------------------------------
# 升级编排引擎：逐台阶段机 precheck→download→prepare→install→verify→cleanup
# ----------------------------------------------------------------------------
UPGRADE_STAGES = ["precheck", "download", "prepare", "install", "verify", "cleanup"]
STAGE_CN = {"precheck": "预检", "download": "下载", "prepare": "准备", "install": "安装",
            "verify": "验证", "cleanup": "清理", "": "—"}
UPGRADE_KINDS = set(UPGRADE_STAGES) | {"upgrade"}
ARCH_BY_HW = {"V1": "rk3588-arm64", "rk3588": "rk3588-arm64", "rk3588-arm64": "rk3588-arm64",
              "V0": "amd64", "amd64": "amd64"}
TASK_LOG_CAP = 1_000_000        # 单台日志内存/入库上限（超出截头保留尾部）


def _sanitize(text):
    """凭据脱敏：DEPLOY_PW 出现在任何日志/错误文本中一律替换。"""
    if DEPLOY_PW and DEPLOY_PW in text:
        return text.replace(DEPLOY_PW, "***")
    return text


def _task_counts(tasks):
    counts = {"queued": 0, "running": 0, "success": 0, "failed": 0, "blocked": 0}
    for t in tasks:
        counts[t.get("status") or "queued"] = counts.get(t.get("status") or "queued", 0) + 1
    return counts


class UpgradeTask:
    """一台设备在一个升级任务中的运行态（内存态权威，阶段流转/日志增量落库）。"""

    def __init__(self, task_id, job, hostname, ssh_port, ctx):
        self.id = task_id
        self.job = job
        self.hostname = hostname
        self.ssh_port = ssh_port
        self.ctx = ctx
        # SSH 用户名按机型解析（amd64→archebase / arm→orangepi，环境变量可覆盖）
        self.ssh_user = _ssh_user_for_arch((ctx or {}).get("arch") or "")
        self.status = "queued"
        self.stage = ""
        self.error = ""
        self.error_code = ""   # 结构化错误码（如 oss_expired），前端醒目提示用
        self.started_at = 0.0
        self.finished_at = 0.0
        self.text = ""
        self._flushed = 0
        self.lock = threading.Lock()

    def log(self, line):
        chunk = _sanitize(str(line)) + "\n"
        with self.lock:
            self.text += chunk
            if len(self.text) > TASK_LOG_CAP:  # 截头保留尾部，防内存/库无限增长
                cut = self.text.find("\n", len(self.text) - int(TASK_LOG_CAP * 0.8))
                self.text = "…（前文截断）…\n" + self.text[cut + 1:]
            total = len(self.text)
            flush = total - self._flushed >= 65536
        # 契约兼容：chunk/total + hostname/offset 别名（前端按 hostname 路由日志块）
        BUS.publish({"type": "log", "job_id": self.job.id, "task_id": self.id,
                     "hostname": self.hostname, "chunk": chunk, "total": total,
                     "text": chunk, "offset": total})
        if flush:
            self.persist(log=True)

    def set_stage(self, stage):
        with self.lock:
            self.stage = stage
        self.persist(stage=stage, log=True)
        BUS.publish({"type": "task", "job_id": self.job.id, "task": self.to_dict()})

    def set_status(self, status, error=None, error_code=None):
        with self.lock:
            self.status = status
            if error is not None:
                self.error = _sanitize(str(error))[:500]
            if error_code is not None:
                self.error_code = str(error_code)[:32]
        BUS.publish({"type": "task", "job_id": self.job.id, "task": self.to_dict()})

    def persist(self, stage=None, status=None, error=None, error_code=None, log=False,
                started=False, finished=False):
        kwargs = {}
        if stage is not None:
            kwargs["stage"] = stage
        if status is not None:
            kwargs["status"] = status
        if error is not None:
            kwargs["error"] = error
        if error_code is not None:
            kwargs["error_code"] = error_code
        if log:
            with self.lock:
                kwargs["log"] = self.text
                self._flushed = len(self.text)
        if started:
            kwargs["started_at"] = datetime.now()
        if finished:
            kwargs["finished_at"] = datetime.now()
        if kwargs:
            store.update_upgrade_task(self.id, **kwargs)

    def to_dict(self):
        with self.lock:
            return {"id": self.id, "hostname": self.hostname,
                    "site": (store.roster.get(self.hostname) or {}).get("site", ""),
                    "stage": self.stage, "stage_cn": STAGE_CN.get(self.stage, self.stage),
                    "status": self.status, "error": self.error, "error_code": self.error_code,
                    "started_at": self.started_at or None,
                    "finished_at": self.finished_at or None,
                    "chars": len(self.text)}


class UpgradeJob:
    def __init__(self, job_id, kind, version, params):
        self.id = job_id
        self.kind = kind
        self.version = version
        self.params = params
        self.filename = ""          # OSS 包文件名（脱敏摘要，签名 URL 不入库）
        self.oss_url_expires = None  # 签名链接过期时间（epoch 秒；无则 None）
        self.status = "running"
        self.created_at = time.time()
        self.finished_at = None
        self.tasks = []
        self.lock = threading.Lock()

    def to_dict(self, with_tasks=True):
        with self.lock:
            tasks = [t.to_dict() for t in self.tasks]
            out = {"id": self.id, "kind": self.kind, "version": self.version,
                   "filename": self.filename,
                   "oss_url_expires": self.oss_url_expires,
                   "status": self.status, "total": len(tasks),
                   "params": self.params, "created_at": self.created_at,
                   "finished_at": self.finished_at, "counts": _task_counts(tasks)}
            if with_tasks:
                out["tasks"] = tasks
            return out

    def check_done(self):
        """全部子任务终态 → 任务收口（内存 + DB + SSE）。"""
        with self.lock:
            if self.status != "running":
                return
            if any(t.status in ("queued", "running") for t in self.tasks):
                return
            self.status = "completed"
            self.finished_at = time.time()
        store.finish_upgrade_job(self.id, datetime.now())
        BUS.publish({"type": "job", "job": self.to_dict()})


UPGRADES = {}            # job_id -> UpgradeJob（内存态；历史任务从 DB 回看）
UPGRADES_LOCK = threading.Lock()
_UPGRADE_SEQ = store.max_upgrade_id() if store.enabled else 0  # 重启后从 DB 续号，防主键冲突
UPGRADE_POOL = ThreadPoolExecutor(max_workers=UPGRADE_MAX_PARALLEL,
                                  thread_name_prefix="upgrade")


def _next_upgrade_ids(n):
    global _UPGRADE_SEQ
    with UPGRADES_LOCK:
        base = _UPGRADE_SEQ
        _UPGRADE_SEQ += n
    return base + 1, base + n


def device_busy(hostname):
    """该设备是否已有 queued/running 子任务（跨任务查重，并发护栏）。

    含 webfix 补配任务（frpc 重启会闪断 SSH 隧道，与升级互斥）。"""
    with UPGRADES_LOCK:
        for job in UPGRADES.values():
            with job.lock:
                for t in job.tasks:
                    if t.hostname == hostname and t.status in ("queued", "running"):
                        return job.id
    webfix = webfix_active(hostname)
    if webfix is not None:
        return f"webfix#{webfix.id}"
    return None


def _ssh_argv(port, user, remote_cmd):
    """frp-monitor 与 frps 同机：设备代理绑定本机 127.0.0.1:<port>，直连不走跳板。
    user 按设备机型解析（amd64→archebase / arm→orangepi）。"""
    return ["sshpass", "-e", "ssh",
            "-o", "StrictHostKeyChecking=accept-new",
            "-o", f"UserKnownHostsFile={SSH_KNOWN_HOSTS}",
            "-o", "ConnectTimeout=15",
            "-o", "ServerAliveInterval=15",
            "-o", "NumberOfPasswordPrompts=1",
            "-p", str(int(port)), f"{user}@127.0.0.1", remote_cmd]


def run_remote(task, remote_cmd, timeout=60, stdin_data=None, retries=2, log=True):
    """在设备上执行命令；stdout 逐行进入任务日志并推 SSE。返回 (rc, 全部输出)。

    对 FRP 瞬断（Connection refused/reset、banner 超时）自动重试；设备端
    后台任务有 flock 护栏，重试安全。remote_cmd 不落日志（可能含凭据）。"""
    env = dict(os.environ, SSHPASS=DEPLOY_PW)
    last = ""
    for attempt in range(retries + 1):
        try:
            proc = subprocess.Popen(
                _ssh_argv(task.ssh_port, task.ssh_user, remote_cmd), env=env,
                stdin=subprocess.PIPE if stdin_data is not None else subprocess.DEVNULL,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                errors="replace")
        except OSError as exc:
            task.log(f"[panel] 无法启动 ssh: {exc}")
            return 127, ""
        if stdin_data is not None:
            try:
                proc.stdin.write(stdin_data)
                proc.stdin.close()
            except (BrokenPipeError, OSError):
                pass
        out = []

        def pump():
            for line in proc.stdout:
                out.append(line)
                if log:
                    task.log(line.rstrip("\n"))

        t = threading.Thread(target=pump, daemon=True)
        t.start()
        try:
            rc = proc.wait(timeout=timeout)
            t.join(5)
        except subprocess.TimeoutExpired:
            proc.kill()
            t.join(5)
            task.log(f"[panel] 命令超时({timeout}s)，已终止")
            return 124, "".join(out)
        last = "".join(out)
        conn_err = (rc == 255 and any(s in last for s in (
            "Connection refused", "Connection reset", "Connection timed out",
            "kex_exchange_identification", "closed by remote host", "No route to host")))
        if rc == 0 or not conn_err or attempt >= retries:
            return rc, last
        task.log(f"[panel] FRP 隧道瞬断，10s 后重试({attempt + 1}/{retries})…")
        time.sleep(10)
    return 255, last


def push_script(task, script, envs, timeout):
    """阶段脚本经 stdin 推送到设备 bash -s 执行（不上传、不留痕）。"""
    all_envs = {"DEPLOY_PW": DEPLOY_PW, **envs}
    prefix = " ".join(f"{k}={shlex.quote(str(v))}" for k, v in all_envs.items() if v != "")
    return run_remote(task, f"{prefix} bash -s", timeout=timeout, stdin_data=script)


def _base_dir(version):
    return f"/var/tmp/ego-{version}" if version else ""


def stage_precheck(task, ctx):
    envs = {"NOW_EPOCH": str(int(time.time())),
            "PKG_HOME": f"/home/{task.ssh_user}",
            "PKG_ARCH": ctx.get("arch") or "rk3588-arm64"}
    if ctx.get("oss_url"):
        envs["OSS_URL"] = ctx["oss_url"]
    rc, out = push_script(task, SCRIPT_PRECHECK, envs, timeout=180)
    if rc != 0:
        task.set_status("failed", "预检 SSH 失败（设备不可达或凭据失效）")
        return False
    clock_warn = None
    for line in out.splitlines():
        if line.startswith("### clock-drift:"):
            try:
                drift = int(line.rsplit(":", 1)[1].rstrip("s").strip())
            except ValueError:
                continue
            if abs(drift) > 300:
                clock_warn = drift
                task.log(f"[panel] 预检告警：设备时钟偏差 {drift}s（>5min），"
                         f"apt 可能失败，建议先修复时间同步再升级（SOP 坑 #10）")
        elif line.startswith("### OSS:"):
            if "http=403" in line:
                task.set_status("failed", "OSS 链接已过期（403），请重新生成签名 URL 后新建任务",
                                error_code="oss_expired")
                return False
            if "http=206" not in line and "http=200" not in line:
                task.log(f"[panel] 预检告警：设备无法连通 OSS（{line.strip()}），下载阶段可能失败")
    if clock_warn is None:
        task.log("[panel] 时钟偏差正常（≤5min）")
    # 活跃采集会话（非 [] = 禁止升级）；check-skipped = 采集库不可读，放行
    sess = [l for l in out.splitlines()
            if any(x in l for x in ("('", "[(", "RECORDING")) and "check-skipped" not in l and "[]" not in l]
    if sess:
        task.set_status("blocked", "存在活跃采集会话，先停止采集再升级")
        return False
    for line in out.splitlines():
        if line.startswith("### camera-containers:"):
            try:
                n = int(line.rsplit(":", 1)[1].strip() or 0)
            except ValueError:
                n = 0
            if n > 0:
                task.set_status("blocked", f"检测到 {n} 个运行中的 decxin 相机容器，需先移除（确认无活跃会话后）")
                return False
    return True


def stage_download(task, ctx):
    """设备直连 OSS 下载；403（链接过期）与停滞看门狗为结构化错误。"""
    if not ctx.get("oss_url") or not ctx.get("sha256"):
        task.set_status("failed", "缺少 OSS 链接或 sha256 基准，无法下载")
        return False
    envs = {"OSS_URL": ctx["oss_url"], "PKG_VERSION": ctx["version"],
            "STALL_TIMEOUT": str(ctx.get("stall_timeout") or DOWNLOAD_STALL_TIMEOUT)}
    rc, out = push_script(task, SCRIPT_DOWNLOAD, envs, timeout=3600)
    if "ERROR_LINK_EXPIRED" in out or "OSS_HTTP=403" in out:
        task.set_status("failed", "OSS 链接已过期（403），请重新生成签名 URL 后新建任务",
                        error_code="oss_expired")
        return False
    if "DOWNLOAD_STALLED" in out:
        procs = next((l for l in out.splitlines() if l.startswith("curl_procs_after=")), "")
        task.set_status("failed", f"下载停滞（size 长时间不变，已终止；{procs}），"
                                  f"可重试续传或检查设备网络（SOP 坑 #13）",
                        error_code="download_stalled")
        return False
    if rc != 0 or "DOWNLOAD_OK" not in out:
        task.set_status("failed", "下载失败（curl 未完成，详见日志）")
        return False
    got = next((l.split("=", 1)[1].strip() for l in out.splitlines()
                if l.startswith("sha256=")), "")
    if got != ctx["sha256"]:
        task.set_status("failed", f"sha256 不一致：设备端 {got[:16]}… ≠ 基准 {ctx['sha256'][:16]}…，已终止",
                        error_code="sha_mismatch")
        return False
    task.log("[panel] 下载完成且 sha256 与基准一致")
    return True


def stage_prepare(task, ctx):
    if not ctx.get("sha256"):
        task.set_status("failed", "缺少 sha256 基准，无法校验")
        return False
    envs = {"PKG_VERSION": ctx["version"], "SHA256": ctx["sha256"],
            "PKG_ARCH": ctx.get("arch") or "rk3588-arm64"}
    if ctx.get("pkg_src"):
        envs["PKG_SRC"] = ctx["pkg_src"]
    rc, out = push_script(task, SCRIPT_PREPARE, envs, timeout=1200)
    if rc != 0 or "PREPARE_OK" not in out:
        task.set_status("failed", "prepare 失败（sha 校验/解压/prepare-only，详见日志）")
        return False
    for line in out.splitlines():
        if line.startswith(("DEVICE_ID=", "DEVICE_NAME=", "RELEASE_VERSION=")):
            task.log(f"[panel] bundle 自检 {line}")
    return True


def stage_install(task, ctx):
    base = _base_dir(ctx["version"])
    if not base:
        task.set_status("failed", "缺少目标版本，无法安装")
        return False
    # 1) 防双安装器：检查与启动分两条 SSH 命令，pattern 用 [i] 写法防 pgrep 自匹配
    rc, out = run_remote(task, 'pgrep -c -f "[i]nstall-remote.sh" || true', timeout=40, log=False)
    n = next((l.strip() for l in out.splitlines() if l.strip().isdigit()), "1")
    if n != "0":
        task.set_status("blocked", f"设备上已有安装进程在运行 (n={n})，禁止重复启动")
        return False
    # 2) 上传安装执行器（flock 防重入 + sudo 保活 + INSTALL_EXIT 判据）
    rc, _ = run_remote(
        task,
        f"mkdir -p {shlex.quote(base)} && cat > {shlex.quote(base + '/run-install.sh')} "
        f"&& chmod 700 {shlex.quote(base + '/run-install.sh')}",
        timeout=60, stdin_data=SCRIPT_RUN_INSTALL)
    if rc != 0:
        task.set_status("failed", "上传 run-install.sh 失败")
        return False
    # 3) nohup+setsid 后台启动：SSH/面板断连不影响安装（凭据只出现在命令行瞬间，不落日志）
    launch = (f"DEPLOY_PW={shlex.quote(DEPLOY_PW)} PKG_VERSION={shlex.quote(ctx['version'])} "
              f"nohup setsid bash {shlex.quote(base + '/run-install.sh')} "
              f"> {shlex.quote(base + '/install.log')} 2>&1 < /dev/null & sleep 2; echo LAUNCHED")
    rc, out = run_remote(task, launch, timeout=40, log=False)
    if rc != 0 or "LAUNCHED" not in out:
        task.set_status("failed", "启动安装器失败")
        return False
    task.log("[panel] 安装器已在设备后台启动（nohup+setsid），开始跟踪 install.log …")
    # 4) 增量跟踪 install.log；成功判据 = 成功横幅 + INSTALL_EXIT=0
    offset, collected = 0, ""
    deadline = time.time() + int(ctx.get("install_timeout") or INSTALL_TIMEOUT)
    drop = 0
    while time.time() < deadline:
        time.sleep(8)
        rc, out = run_remote(
            task,
            f"wc -c < {shlex.quote(base + '/install.log')} 2>/dev/null; "
            f"tail -c +{offset + 1} {shlex.quote(base + '/install.log')} 2>/dev/null",
            timeout=40, log=False)
        if rc != 0:
            drop += 1
            task.log(f"[panel] 轮询 SSH 失败({drop}/5)，设备端安装不受影响，继续…")
            if drop >= 5:
                task.set_status("failed", "连续多次无法连接设备；安装可能仍在后台进行，稍后用「验证」补查")
                return False
            continue
        drop = 0
        lines = out.splitlines()
        try:
            size = int(lines[0].strip() or 0)
        except (ValueError, IndexError):
            size = 0
        if size > offset:
            delta = "\n".join(lines[1:])
            if delta:
                for l in delta.splitlines():
                    task.log(l)
                collected += delta + "\n"
            offset = size
        if "INSTALL_EXIT=" in collected:
            ok = "INSTALL_EXIT=0" in collected and "is running." in collected
            exit_val = collected.rsplit("INSTALL_EXIT=", 1)[-1].splitlines()[0][:8]
            task.log(f"[panel] 安装器退出 INSTALL_EXIT={exit_val}；"
                     f"成功判据（横幅 + INSTALL_EXIT=0）{'满足' if ok else '未满足'}")
            if not ok:
                task.set_status("failed", f"安装未同时满足成功判据（横幅 + INSTALL_EXIT=0），安装器已自动回滚")
            return ok
    task.set_status("failed", f"安装跟踪超时（>{int(ctx.get('install_timeout') or INSTALL_TIMEOUT)}s，"
                              f"安装可能仍在设备后台进行，稍后用「验证」补查）")
    return False


# 验证容器判据按机型校准（2026-10-16 KVWO-14）：V1（rk3588）常驻栈 4 容器全为新版
# 镜像；V0（amd64）常驻核心集 = backend + 相机模块（2 个），判据为「运行中的 portal
# 容器全部为新版镜像，且数量 ≥ 机型核心数」——有旧容器还在跑即判真失败。
VERIFY_CORE_CONTAINERS = {"rk3588-arm64": 4, "amd64": 2}
VERIFY_MODEL_BY_ARCH = {"amd64": "V0", "rk3588-arm64": "V1"}


def _verify_containers(out, want, arch):
    """按机型校准容器判分。返回 (ok, 新版数, portal 运行数, 核心数, 机型)。"""
    core = VERIFY_CORE_CONTAINERS.get(arch, 4)
    model = VERIFY_MODEL_BY_ARCH.get(arch, arch or "未知机型")
    # docker ps 输出行形如：ego_portal-backend-1 | registry/…:v0.4.6 | Up 2 minutes
    portal = [l for l in out.splitlines()
              if "|" in l and l.split("|", 1)[0].strip().startswith("ego_portal-")]
    new = sum(1 for l in portal if want and want in l)
    if arch == "amd64":
        ok = bool(portal) and new == len(portal) and new >= core
    else:
        ok = new >= core
    return ok, new, len(portal), core, model


def stage_verify(task, ctx):
    rc, out = push_script(task, SCRIPT_VERIFY, {}, timeout=180)
    if rc != 0:
        task.set_status("failed", "验证 SSH 失败")
        return False
    s = {}
    for line in out.splitlines():
        for key in ("version", "service", "http", "frpc", "api"):
            if line.startswith(key + "="):
                s[key] = line.split("=", 1)[1][:160]
    want = ctx.get("version") or ""
    arch = ctx.get("arch") or "rk3588-arm64"
    cont_ok, new_containers, portal_total, core, model = _verify_containers(out, want, arch)
    ok = (s.get("service") == "active" and s.get("http") == "200"
          and s.get("frpc") == "active"
          and (not want or s.get("version") == want)
          and (not want or cont_ok))
    task.log(f"[panel] 验证结果 version={s.get('version')} service={s.get('service')} "
             f"http={s.get('http')} frpc={s.get('frpc')} "
             f"新版容器={new_containers}/{portal_total}（{model} 核心={core}）")
    if not ok:
        task.set_status("failed", f"验证未全部通过（版本/service/HTTP/frpc/容器镜像："
                                  f"新版容器 {new_containers}/{portal_total}（{model} 核心={core}））")
    return ok


def stage_cleanup(task, ctx):
    envs = {}
    if ctx.get("version"):
        envs["PKG_VERSION"] = ctx["version"]
    if ctx.get("old_tag"):
        envs["OLD_TAG"] = ctx["old_tag"]
    rc, _ = push_script(task, SCRIPT_CLEANUP, envs, timeout=180)
    if rc != 0:
        task.set_status("failed", "清理失败（详见日志）")
        return False
    return True


STAGE_FN = {"precheck": stage_precheck, "download": stage_download, "prepare": stage_prepare,
            "install": stage_install, "verify": stage_verify, "cleanup": stage_cleanup}


def _run_upgrade_task(task):
    stages = UPGRADE_STAGES if task.job.kind == "upgrade" else [task.job.kind]
    task.set_status("running")
    task.started_at = time.time()
    task.persist(status="running", started=True)
    try:
        for stage in stages:
            task.set_stage(stage)
            ok = STAGE_FN[stage](task, task.ctx)
            if not ok:
                if task.status != "blocked":
                    task.set_status("failed", task.error or f"{stage} 阶段失败")
                break
        else:
            task.set_status("success")
    except Exception as exc:  # noqa: BLE001
        task.log(f"[panel] 任务异常: {exc!r}")
        task.set_status("failed", f"任务异常: {exc!r}")
    finally:
        task.finished_at = time.time()
        task.persist(status=task.status, error=task.error or None,
                     error_code=task.error_code or None, log=True, finished=True)
        BUS.publish({"type": "task", "job_id": task.job.id, "task": task.to_dict()})
        task.job.check_done()


def _resolve_device(hostname):
    """以面板设备记录为准解析 ssh_port 与机型；返回 (ssh_port, arch, error, error_code)。

    error_code（KVWO-44 阻断类细分码）：no_ssh_port / arch_unknown，供 webfix
    阻断落库与 API 回显；升级编排调用方忽略该值。"""
    snap = {}
    try:
        hosts, _ = get_snapshot()
        snap = next((h for h in hosts if h["hostname"] == hostname), {}) or {}
    except Exception:  # noqa: BLE001
        pass
    roster = store.roster.get(hostname) or {}
    ssh_port = snap.get("port") or roster.get("ssh_port")
    if isinstance(ssh_port, str) and ssh_port.isdigit():
        ssh_port = int(ssh_port)
    hw = (roster.get("hardware") or "").strip() or _hardware_category(roster.get("hw_model") or "")
    arch = ARCH_BY_HW.get((hw or "").strip()) or ARCH_BY_HW.get(hw.casefold() if hw else "")
    error, code = None, ""
    if not ssh_port:
        error = "无 SSH 端口记录（以面板设备记录为准，请等设备上线后再试）"
        code = "no_ssh_port"
    elif not arch:
        error = "无法确定机型（V0/amd64 或 V1/rk3588），请先在设备登记表维护 hardware"
        code = "arch_unknown"
    return ssh_port, arch, error, code


def create_upgrade_job(kind, devices, params):
    """创建升级任务（OSS 签名链接分发）。

    返回 {ok, job_id, total, skipped, expires_at, warnings} 或
    {ok:False, error, error_code?}；链接过期返回 error_code=oss_expired。
    完整 oss_url 只进运行期内存（ctx），DB 仅存文件名 + 过期时间。"""
    if kind not in UPGRADE_KINDS:
        return {"ok": False, "error": f"未知操作类型: {kind}（可选 {sorted(UPGRADE_KINDS)}）"}
    if not DEPLOY_PW:
        return {"ok": False, "error": "服务端未设置 DEPLOY_PW 环境变量（/etc/frp-monitor.env）"}
    devices = [str(h).strip().casefold() for h in devices or [] if str(h).strip()]
    seen = set()
    devices = [h for h in devices if not (h in seen or seen.add(h))]
    if not devices:
        return {"ok": False, "error": "未指定设备"}
    params = params or {}
    version = str(params.get("version") or "").strip()
    oss_url = str(params.get("oss_url") or "").strip()
    manual_sha = str(params.get("sha256") or "").strip().lower()
    needs_pkg = kind in ("download", "prepare", "install", "upgrade")
    warnings = []
    meta = None
    sha256 = ""
    arch_from_url = ""
    expires_at = None
    if oss_url:
        meta = parse_oss_url(oss_url)
        if not meta["ok"]:
            return {"ok": False, "error": meta["error"], "error_code": "bad_url"}
        if needs_pkg:
            # URL 识别出的版本/机型是权威：与手填 version 冲突时以 URL 为准并提示
            if version and version != meta["version"]:
                warnings.append(f"手填版本 {version} 与 URL 识别 {meta['version']} 不一致，已按 URL 为准")
            version = meta["version"]
            arch_from_url = meta["arch"]
        expires_at = meta["expires_epoch"]
    if needs_pkg:
        if not version:
            return {"ok": False, "error": f"{kind} 需要目标版本（从 oss_url 自动识别或手填 version）"}
        if not oss_url:
            return {"ok": False, "error": f"{kind} 需要 oss_url（OSS 签名链接，设备直连下载）"}
        # sha256 基准：手填 > 注册表命中；都没有则拒绝并提示先触发计算
        hit = store.pkg_registry_get(version, arch_from_url)
        sha256 = manual_sha or (hit or {}).get("sha256", "")
        if manual_sha and hit and manual_sha != hit["sha256"]:
            warnings.append("手填 sha256 与注册表不一致，已按手填值执行")
        if not sha256:
            return {"ok": False, "error": f"版本 {version}/{arch_from_url} 无 sha256 基准："
                                            f"请先触发服务端计算（POST /api/upgrade/packages/hash）"
                                            f"或手填 params.sha256",
                    "error_code": "sha_missing"}
        # 链接有效期校验（解析不到有效期时只提示不阻断）
        if expires_at is None:
            warnings.append("OSS 链接未解析出有效期（Expires/x-oss-expires），无法预警过期")
        else:
            remaining = expires_at - time.time()
            if remaining <= 0:
                return {"ok": False, "error": "OSS 链接已过期，请重新生成签名 URL",
                        "error_code": "oss_expired"}
    # 解析每台设备（端口/机型/占用），逐台给出跳过原因
    try:
        hosts, _ = get_snapshot()
        snapshot_names = {h["hostname"] for h in hosts}
    except Exception:  # noqa: BLE001  # frps 不可达时仅按登记表判定
        snapshot_names = set()
    resolved, skipped = [], []
    for hostname in devices:
        if hostname not in store.roster and hostname not in snapshot_names:
            skipped.append({"hostname": hostname, "reason": "未知设备（不在面板记录中）"})
            continue
        busy = device_busy(hostname)
        if busy:
            skipped.append({"hostname": hostname, "reason": f"已有任务 {busy} 在进行中"})
            continue
        ssh_port, arch, error, _ = _resolve_device(hostname)
        if error:
            skipped.append({"hostname": hostname, "reason": error})
            continue
        if needs_pkg and arch_from_url and arch != arch_from_url:
            skipped.append({"hostname": hostname,
                            "reason": f"设备机型 {arch} 与链接机型 {arch_from_url} 不符，请使用对应机型的签名 URL"})
            continue
        resolved.append((hostname, ssh_port, arch))
    if not resolved:
        return {"ok": False, "error": "没有可执行的设备", "skipped": skipped}
    # 批量时长预警：单台全流程 6~12 分钟，按并行度估算总窗口（SOP §3）
    if expires_at is not None:
        batches = max(1, -(-len(resolved) // UPGRADE_MAX_PARALLEL))
        estimate = batches * 12 * 60  # 单台按 12 分钟上限估算
        remaining = expires_at - time.time()
        if remaining < estimate:
            warnings.append(f"链接剩余 {int(remaining // 60)} 分钟，低于预估批量时长 "
                            f"{int(estimate // 60)} 分钟（{len(resolved)} 台 ÷ {UPGRADE_MAX_PARALLEL} 并行 × ≤12min/台），"
                            f"建议重新生成链接")
    # 建任务（job + 每台一个子任务）；params 入库快照不含 oss_url
    first_id, _ = _next_upgrade_ids(1 + len(resolved))
    job_id = first_id
    snap_params = {k: v for k, v in params.items()
                   if k in ("version", "sha256", "old_tag", "install_timeout", "pkg_src")}
    snap_params["version"] = version
    job = UpgradeJob(job_id, kind, version, snap_params)
    job.filename = meta["filename"] if meta else ""
    job.oss_url_expires = expires_at
    with UPGRADES_LOCK:
        UPGRADES[job_id] = job
    store.create_upgrade_job(job_id, kind, version,
                             json.dumps(snap_params, ensure_ascii=False), len(resolved), datetime.now(),
                             filename=job.filename,
                             oss_url_expires=(datetime.fromtimestamp(expires_at)
                                              if expires_at else None))
    for i, (hostname, ssh_port, arch) in enumerate(resolved):
        task_id = first_id + 1 + i
        ctx = {"version": version, "arch": arch,
               "filename": meta["filename"] if meta else "",
               "sha256": sha256, "oss_url": oss_url,
               "old_tag": str(params.get("old_tag") or ""),
               "install_timeout": params.get("install_timeout") or INSTALL_TIMEOUT,
               "pkg_src": str(params.get("pkg_src") or ""),
               "stall_timeout": params.get("stall_timeout") or DOWNLOAD_STALL_TIMEOUT}
        task = UpgradeTask(task_id, job, hostname, ssh_port, ctx)
        with job.lock:
            job.tasks.append(task)
        store.create_upgrade_task(task_id, job_id, hostname, ssh_port)
        UPGRADE_POOL.submit(_run_upgrade_task, task)
    BUS.publish({"type": "job", "job": job.to_dict()})
    return {"ok": True, "job_id": job_id, "total": len(resolved), "skipped": skipped,
            "expires_at": expires_at, "warnings": warnings}


def upgrade_job_view(job_id):
    with UPGRADES_LOCK:
        job = UPGRADES.get(job_id)
    if job is not None:
        return job.to_dict()
    if store.enabled:
        return store.upgrade_job_view(job_id)
    return None


def upgrade_jobs_list_view(limit=50):
    out = []
    with UPGRADES_LOCK:
        jobs = sorted(UPGRADES.values(), key=lambda j: -j.id)[:limit]
        out = [j.to_dict(with_tasks=False) for j in jobs]
    if store.enabled:
        known = {j["id"] for j in out}
        for row in store.upgrade_jobs_list(limit):
            if row["id"] not in known:
                out.append(row)
        out.sort(key=lambda j: -j["id"])
    return out[:limit]


def upgrade_task_log_view(job_id, task_id=None, hostname=None, offset=0):
    """日志增量读取（内存态优先，重启后回落 DB）；返回 dict 或 None。

    字段契约兼容：log/text 同义，next_offset/offset 同义。"""
    task = None
    with UPGRADES_LOCK:
        job = UPGRADES.get(job_id)
    if job is not None:
        with job.lock:
            candidates = job.tasks
        if task_id is not None:
            task = next((t for t in candidates if t.id == task_id), None)
        elif hostname:
            task = next((t for t in candidates if t.hostname == hostname), None)
    if task is not None:
        with task.lock:
            text, total = task.text[offset:], len(task.text)
        return {"ok": True, "task_id": task.id, "hostname": task.hostname,
                "status": task.status, "log": text, "text": text,
                "next_offset": total, "offset": total}
    if store.enabled:
        row = store.upgrade_task_log(job_id, task_id=task_id, hostname=hostname)
        if row is not None:
            text, status = row
            text = text or ""
            return {"ok": True, "hostname": hostname, "status": status,
                    "log": text[offset:], "text": text[offset:],
                    "next_offset": len(text), "offset": len(text)}
    return None


def _prune_upgrade_memory():
    """清理 24h 前已结束的内存态任务（历史回看走 DB）。"""
    with UPGRADES_LOCK:
        stale = [jid for jid, j in UPGRADES.items()
                 if j.status != "running" and time.time() - (j.finished_at or j.created_at) > 86400]
        for jid in stale:
            UPGRADES.pop(jid, None)


# ----------------------------------------------------------------------------
# webfix 引擎（KVWO-17/19/44）：缺失 web 代理设备的自动检测与一次性补配。
# 检测挂入 _refresh_loop（上线事件 + 稳定窗口，KVWO-44）；补配复用升级编排的
# SSH 基建（run_remote/push_script，SSH 用户按机型解析，KVWO-13）与 SSE/日志
# 体系；台账入库 webfix_tasks。
# 只动设备侧 frpc.toml，不触碰 frps / 网关（allowPorts 30000-30999 已预放宽）。
# ----------------------------------------------------------------------------
# 阻断类细分码（KVWO-44，对齐父需求「成功/失败/阻断」三态与 KVWO-21 的
# blocked / error_code 设计）：阻断 = 前置条件不满足或确定性错配，重试同样
# 结果，需人工介入（补登记表 / 放宽网关段 / 处理配置冲突）；普通失败 =
# 环境性/瞬时性失败（SSH 不可达、闭环校验超时等），排障后重试可成功。
#   面板侧前置：bad_hostname / port_out_of_range / no_ssh_port / arch_unknown
#   设备端脚本：no_conf（配置缺失）/ proxy_conflict（同名代理端口冲突）/ no_frpc
#               / bad_hostname / bad_port
#   闭环校验：  device_id_mismatch（端口「借住」错配，不会自愈，等人工）
WEBFIX_BLOCKED_CODES = {"bad_hostname", "bad_port", "port_out_of_range", "no_ssh_port",
                        "arch_unknown", "no_conf", "proxy_conflict", "no_frpc",
                        "device_id_mismatch"}
_SCRIPT_ERROR_CODES = {"ERROR_BAD_HOSTNAME": "bad_hostname", "ERROR_BAD_PORT": "bad_port",
                       "ERROR_NO_CONF": "no_conf", "ERROR_PROXY_CONFLICT": "proxy_conflict",
                       "ERROR_NO_FRPC": "no_frpc"}


def _webfix_device_number(hostname):
    """从设备名提取编号（ep-000160 → 160）；不符合约定返回 None。"""
    m = re.fullmatch(r"ep-(\d+)", (hostname or "").strip().casefold())
    return int(m.group(1)) if m else None


def webfix_port_for(hostname):
    """该设备应使用的 web 远端端口（WEBFIX_PORT_BASE + 编号）；超段返回 None。"""
    number = _webfix_device_number(hostname)
    if number is None:
        return None
    port = WEBFIX_PORT_BASE + number
    return port if WEBFIX_PORT_BASE <= port <= WEBFIX_PORT_MAX else None


def webfix_candidates(hosts):
    """缺 web 代理候选：在线 + 有 SSH 代理端口 + 无 web 代理端口。

    判据只看 frps 代理列表；web 代理已存在但探测失败（version_error）不属于
    缺配置，不触发——那是探测瞬时失败，改设备配置解决不了。"""
    out = []
    for h in hosts or []:
        if not h.get("port"):        # 无 SSH 代理：无法推送补配脚本
            continue
        if h.get("web_port"):        # web 代理已存在（即使 version_error 也不是候选）
            continue
        out.append(h["hostname"])
    return out


def _webfix_id_matches(got, expected):
    """闭环校验的 device_id 比对：数字串按值比（"0160"="160"），其余按字面比。"""
    g, e = str(got or "").strip(), str(expected or "").strip()
    if not g or not e:
        return False
    if g.isdigit() and e.isdigit():
        return int(g) == int(e)
    return g.casefold() == e.casefold()


class WebfixTask:
    """一台设备一次补配的运行态（内存态权威，状态/日志增量落 webfix_tasks）。

    兼容 run_remote/push_script 的任务接口（ssh_port/ssh_user/log）。status 三态
    收口（KVWO-44）：success / failed（环境性）/ blocked（阻断类，重试同样
    结果，需人工介入）；error_code 为细分码。"""

    def __init__(self, task_id, hostname, trigger_type, attempts, ssh_port, ssh_user,
                 port, expected_id):
        self.id = task_id
        self.hostname = hostname
        self.trigger_type = trigger_type      # auto / manual
        self.attempts = attempts              # 该设备累计补配次数
        self.ssh_port = ssh_port
        self.ssh_user = ssh_user
        self.port = port                      # 目标 web 远端端口（30000+编号）
        self.expected_id = expected_id        # 闭环校验期望 device_id
        self.status = "queued"
        self.error = ""
        self.error_code = ""
        self.created_at = time.time()
        self.finished_at = 0.0
        self.text = ""
        self._flushed = 0
        self.lock = threading.Lock()

    def log(self, line):
        chunk = _sanitize(str(line)) + "\n"
        with self.lock:
            self.text += chunk
            if len(self.text) > TASK_LOG_CAP:
                cut = self.text.find("\n", len(self.text) - int(TASK_LOG_CAP * 0.8))
                self.text = "…（前文截断）…\n" + self.text[cut + 1:]
            total = len(self.text)
            flush = total - self._flushed >= 65536
        BUS.publish({"type": "webfix_log", "task_id": self.id, "hostname": self.hostname,
                     "chunk": chunk, "total": total, "text": chunk, "offset": total})
        if flush:
            self.persist(log=True)

    def set_status(self, status, error=None, error_code=None):
        with self.lock:
            self.status = status
            if error is not None:
                self.error = _sanitize(str(error))[:500]
            if error_code is not None:
                self.error_code = _sanitize(str(error_code))[:32]
        BUS.publish({"type": "webfix", "task": self.to_dict()})

    def persist(self, status=None, error=None, error_code=None, log=False, finished=False):
        kwargs = {}
        if status is not None:
            kwargs["status"] = status
        if error is not None:
            kwargs["error"] = error
        if error_code is not None:
            kwargs["error_code"] = error_code
        if log:
            with self.lock:
                kwargs["log"] = self.text
                self._flushed = len(self.text)
        if finished:
            kwargs["finished_at"] = datetime.now()
        if kwargs:
            store.update_webfix_task(self.id, **kwargs)

    def to_dict(self):
        with self.lock:
            return {"task_id": self.id, "hostname": self.hostname,
                    "site": (store.roster.get(self.hostname) or {}).get("site", ""),
                    "trigger_type": self.trigger_type, "status": self.status,
                    "error": self.error, "error_code": self.error_code,
                    "attempts": self.attempts,
                    "web_port": self.port,
                    "created_at": self.created_at or None,
                    "finished_at": self.finished_at or None,
                    "chars": len(self.text)}


WEBFIX = {}              # task_id -> WebfixTask（内存态；历史从 DB 回看）
WEBFIX_LOCK = threading.RLock()  # 可重入：_start_webfix_checked 持锁时内查 webfix_active
_WEBFIX_SEQ = store.max_webfix_id() if store.enabled else 0  # 重启后从 DB 续号，防主键冲突
WEBFIX_POOL = ThreadPoolExecutor(max_workers=WEBFIX_MAX_PARALLEL,
                                 thread_name_prefix="webfix")


def webfix_active(hostname):
    """该设备是否有 queued/running 的补配任务（返回任务或 None）。"""
    with WEBFIX_LOCK:
        for t in WEBFIX.values():
            if t.hostname == hostname and t.status in ("queued", "running"):
                return t
    return None


def _webfix_reject_auto(hostname, reason, error_code):
    """auto 触发遇阻断类前置不满足：不跑设备端，直接落一条 blocked 台账行。

    阻断也占一次性名额（对齐 KVWO-21 语义）：结果落库可查、面板可见，人工
    补齐登记表 / 放宽网关段后可手动重试；避免每轮刷新对永久性缺口反复探测。"""
    with WEBFIX_LOCK:
        global _WEBFIX_SEQ
        _WEBFIX_SEQ += 1
        task_id = _WEBFIX_SEQ
        attempts = (store.webfix_attempts(hostname) or 0) + 1
        task = WebfixTask(task_id, hostname, "auto", attempts, None, "", None, "")
        task.status = "blocked"
        task.error = _sanitize(str(reason))[:500]
        task.error_code = error_code
        task.finished_at = time.time()
        WEBFIX[task_id] = task
    store.create_webfix_task(task_id, hostname, "auto", attempts, datetime.now(),
                             status="blocked", error=task.error,
                             error_code=error_code, finished_at=datetime.now())
    BUS.publish({"type": "webfix", "task": task.to_dict()})
    try:
        store._event(datetime.now(), hostname, "webfix", "auto", "blocked", task.error)
    except Exception:  # noqa: BLE001  # 事件失败不影响阻断收口
        pass
    return {"ok": False, "reason": reason, "error_code": error_code,
            "blocked": True, "task_id": task_id}


def _start_webfix_checked(hostname, trigger):
    """校验后创建并异步启动一次补配；返回 {ok, task_id?, attempts?, reason?,
    error_code?, blocked?}。

    trigger: auto（检测引擎，严格一次）/ manual（面板重试，每次新建台账行、
    attempts 累加、trigger_type=manual）。阻断类前置不满足（机型未知、端口
    超段等）时：auto 落 blocked 台账行（KVWO-44），manual 仅跳过并回明原因。"""
    hostname = (hostname or "").strip().casefold()
    if not hostname:
        return {"ok": False, "reason": "空设备名"}

    def reject(reason, code=""):
        if trigger == "auto" and code in WEBFIX_BLOCKED_CODES:
            return _webfix_reject_auto(hostname, reason, code)
        return {"ok": False, "reason": reason, "error_code": code}

    port = webfix_port_for(hostname)
    if port is None:
        number = _webfix_device_number(hostname)
        if number is None:
            return reject(f"设备名 {hostname} 不符合 ep-<编号> 约定，无法推导 web 端口",
                          "bad_hostname")
        return reject(f"推导端口 {WEBFIX_PORT_BASE + number} 超出网关预放宽段 "
                      f"{WEBFIX_PORT_BASE}-{WEBFIX_PORT_MAX}，需先放宽 allowPorts",
                      "port_out_of_range")
    if not store.enabled:
        return {"ok": False, "reason": "历史库未启用（webfix 依赖台账防重，需 MySQL）"}
    if not DEPLOY_PW:
        return {"ok": False, "reason": "服务端未设置 DEPLOY_PW（/etc/frp-monitor.env）"}
    try:
        hosts, _ = get_snapshot()
        snap = next((h for h in hosts if h["hostname"] == hostname), None)
    except Exception:  # noqa: BLE001  # frps 不可达时仅按登记表判定
        hosts, snap = [], None
    roster = store.roster.get(hostname) or {}
    if hostname not in store.roster and snap is None:
        return {"ok": False, "reason": "未知设备（不在面板记录中）"}
    if trigger == "auto" and roster.get("retired"):
        return {"ok": False, "reason": "设备已标记退役，自动补配跳过（可手动重试）"}
    if snap is not None and snap.get("web_port"):
        return {"ok": False, "reason": f"web 代理已存在（web_port={snap['web_port']}），无需补配"}
    busy = device_busy(hostname)
    if busy:
        return {"ok": False, "reason": f"已有升级任务 {busy} 在进行中，稍后再试"}
    ssh_port, arch, error, code = _resolve_device(hostname)
    if error:
        return reject(error, code)
    # 期望 device_id：登记表值优先（探测过的设备），否则按设备编号
    # （闭环校验防端口「借住」错配的比对基准，SOP #11）
    expected_id = str(roster.get("device_id") or "").strip() or str(_webfix_device_number(hostname))
    with WEBFIX_LOCK:
        active = webfix_active(hostname)
        if active is not None:
            return {"ok": False, "reason": f"已有补配任务 #{active.id} 在进行中"}
        global _WEBFIX_SEQ
        _WEBFIX_SEQ += 1
        task_id = _WEBFIX_SEQ
        attempts = (store.webfix_attempts(hostname) or 0) + 1
        task = WebfixTask(task_id, hostname, trigger, attempts, ssh_port,
                          _ssh_user_for_arch(arch), port, expected_id)
        WEBFIX[task_id] = task
    store.create_webfix_task(task_id, hostname, trigger, attempts, datetime.now())
    WEBFIX_POOL.submit(_run_webfix_task, task)
    return {"ok": True, "task_id": task_id, "attempts": attempts}


def _webfix_error_code(out):
    """从设备端脚本输出提取结构化错误码（末尾 ERROR_* 行优先：
    ERROR_PROXY_CONFLICT → proxy_conflict）；无标记返回 ''（环境性/无输出失败）。"""
    for line in reversed([l.strip() for l in (out or "").splitlines() if l.strip()]):
        if line.startswith("ERROR_"):
            marker = line.split(":", 1)[0].strip()
            return _SCRIPT_ERROR_CODES.get(marker,
                                           marker.removeprefix("ERROR_").strip().lower())
    return ""


def _webfix_script_error(out, rc):
    """从设备端脚本输出提取可读错误（ERROR_* 行优先，否则末行/退出码）。"""
    lines = [l.strip() for l in (out or "").splitlines() if l.strip()]
    for line in reversed(lines):
        if line.startswith("ERROR_"):
            body = line.split(":", 1)[1].strip() if ":" in line else line
            return f"{body}（rc={rc}）" if rc else body
    if lines:
        return f"rc={rc}: {lines[-1][:300]}" if rc else lines[-1][:300]
    return f"rc={rc}（无输出；设备不可达或 SSH 凭据失效）"


def _webfix_verify(task):
    """闭环校验：经 127.0.0.1:<30000+编号> 探 /api/setup/status，要求
    release_version 非空且 device_id 与设备编号一致（防端口「借住」错配）。
    返回 (ok, error, error_code)；device_id 不一致立即失败并标阻断类
    （错配不会自愈，等人工），超时归普通失败（环境性，排障后可重试成功）。"""
    base = f"http://127.0.0.1:{task.port}"
    deadline = time.time() + WEBFIX_VERIFY_TIMEOUT
    last_err = ""
    while True:
        try:
            info = _setup_status_probe(base)
            got = str(info.get("device_id") or "").strip()
            if not _webfix_id_matches(got, task.expected_id):
                err = (f"端口 {task.port} 探到 device_id={got or '<空>'}，与期望 "
                       f"{task.expected_id} 不一致（疑似端口被其他设备占用/错配，SOP #11）；"
                       f"设备配置已保留现场，需人工排查")
                task.log(f"[panel] 闭环校验失败：{err}")
                return False, err, "device_id_mismatch"
            task.log(f"[panel] 闭环校验通过：device_id={got} "
                     f"release_version={info.get('release_version')} — 补配成功")
            return True, "", ""
        except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
            last_err = str(exc)
        if time.time() >= deadline:
            break
        time.sleep(5)
    err = (f"闭环校验超时（>{int(WEBFIX_VERIFY_TIMEOUT)}s，frpc 重启后 web 代理未就绪"
           f"或版本不可读）：{last_err}")
    task.log(f"[panel] {err}")
    return False, err, "verify_timeout"


def _run_webfix_task(task):
    """一次补配：推送设备端脚本 → 等待重启回连 → 闭环校验。

    失败不回滚设备配置（保留现场供排查），错误信息可读；台账与 SSE 同步收口。"""
    task.set_status("running")
    task.persist(status="running")
    try:
        task.log(f"[panel] webfix #{task.id} 开始：{task.hostname} 追加代理 "
                 f"{task.hostname}-web（remotePort={task.port} → 127.0.0.1:80，"
                 f"SSH {task.ssh_user}@127.0.0.1:{task.ssh_port}）")
        rc, out = push_script(task, SCRIPT_WEBFIX,
                              {"WEBFIX_HOSTNAME": task.hostname,
                               "WEBFIX_PORT": str(task.port)}, timeout=180)
        if "ERROR_" in out:               # 设备端结构化错误：阻断类（端口冲突/配置缺失等，
            code = _webfix_error_code(out)  # 重试同样结果需人工）与普通失败分开收口
            task.set_status("blocked" if code in WEBFIX_BLOCKED_CODES else "failed",
                            _webfix_script_error(out, rc), error_code=code)
        elif "WEBFIX_APPLIED" not in out and "WEBFIX_ALREADY" not in out:
            task.set_status("failed", _webfix_script_error(out, rc))
        else:
            if rc != 0:
                task.log(f"[panel] 注意：SSH 会话在收尾时断开（rc={rc}），"
                         f"以结果标记与闭环校验为准")
            if "WEBFIX_ALREADY" in out:
                task.log("[panel] 设备端代理已存在且端口一致，未改动配置；进入闭环校验")
            else:
                task.log("[panel] 设备端配置已写入并重启 frpc（本机隧道秒级闪断为预期），"
                         "等待 web 代理回连…")
            ok, err, code = _webfix_verify(task)
            if ok:
                task.set_status("success")
            else:
                task.set_status("blocked" if code in WEBFIX_BLOCKED_CODES else "failed",
                                err, error_code=code or None)
    except Exception as exc:  # noqa: BLE001
        task.log(f"[panel] 任务异常: {exc!r}")
        task.set_status("failed", f"任务异常: {exc!r}")
    finally:
        task.finished_at = time.time()
        task.persist(status=task.status, error=task.error or None,
                     error_code=task.error_code or None, log=True, finished=True)
        BUS.publish({"type": "webfix", "task": task.to_dict()})
        try:
            store._event(datetime.now(), task.hostname, "webfix", task.trigger_type,
                         task.status, task.error or f"attempts={task.attempts}")
        except Exception:  # noqa: BLE001  # 事件失败不影响任务收口
            pass


_webfix_windows = {}                 # hostname → 稳定窗口起点（内存态，KVWO-44）
_webfix_windows_lock = threading.Lock()


def webfix_advance_windows(candidates, now=None):
    """推进「上线事件 + 稳定窗口」状态机；返回窗口已满、应触发自动补配的设备名。

    - 新进入候选集（离线转在线；或面板启动后首次观测到的存量设备）→ 记窗口
      起点（存量设备从首次观测起算同样窗口）；
    - 离开候选集（掉线 / web 代理已出现 / ssh 代理消失）→ 计时作废，移除跟踪，
      重新上线后从新起点重计；
    - 连续在候选集满 WEBFIX_DELAY 秒 → 进入返回列表。跟踪条目保留：若触发被
      瞬时原因拒绝（如升级任务占用），下轮窗口仍满可继续尝试；触发成功或落
      auto 台账（含 blocked）后，调用方的候选集预过滤会将其移出，条目随之清除。
    """
    now = time.time() if now is None else now
    due = []
    with _webfix_windows_lock:
        for hostname in set(_webfix_windows) - set(candidates):
            _webfix_windows.pop(hostname, None)      # 掉线/不再是候选：计时作废
        for hostname in candidates:
            since = _webfix_windows.get(hostname)
            if since is None:
                _webfix_windows[hostname] = now      # 上线事件：启动稳定计时
            elif now - since >= WEBFIX_DELAY:
                due.append(hostname)
    return due


def webfix_auto_detect():
    """每轮快照后：上线事件 + 稳定窗口模型的一次性自动补配（KVWO-44）。

    触发时机（需求方定稿 2026-10-16）：不再周期扫描命中即触发——设备从离线
    转在线（且 ssh 代理在、web 代理缺、无 auto 台账记录）→ 启动稳定计时，
    连续在线满 WEBFIX_DELAY（默认 60s，环境变量可调）且期间未掉线（掉线计时
    作废、重新上线重计）→ 仍缺 web 代理 → 触发一次。跳过：退役设备、升级/
    补配任务进行中、已有 auto 台账记录（无论成败；阻断类前置不满足也占名额
    落 blocked 行，不反复探测）。"""
    if not WEBFIX_AUTO or not store.enabled or not DEPLOY_PW:
        return
    try:
        hosts, _ = get_snapshot()
    except Exception:  # noqa: BLE001
        return
    candidates = []
    for hostname in webfix_candidates(hosts):
        if store.webfix_has_auto(hostname):
            continue                      # 一次为限：有 auto 记录不再跟踪/触发
        if (store.roster.get(hostname) or {}).get("retired"):
            continue
        candidates.append(hostname)
    for hostname in webfix_advance_windows(candidates):
        result = _start_webfix_checked(hostname, "auto")
        if result.get("ok"):
            print(f"frp-monitor: webfix auto-trigger {hostname} "
                  f"(task #{result['task_id']}, attempts={result['attempts']}, "
                  f"stable>={int(WEBFIX_DELAY)}s)", flush=True)
        elif result.get("blocked"):
            print(f"frp-monitor: webfix auto-trigger {hostname} blocked "
                  f"(task #{result.get('task_id')}, code={result.get('error_code')}): "
                  f"{result.get('reason', '')}", flush=True)


def webfix_retry(devices):
    """POST /api/webfix/retry：手动重试（面板认证）；每次新建台账行，
    attempts 累加，trigger_type=manual。自动触发的一次性语义不受影响。"""
    if not store.enabled:
        return {"ok": False, "error": "历史库未启用，webfix 不可用（需要 MySQL 台账防重）"}
    if not DEPLOY_PW:
        return {"ok": False, "error": "服务端未设置 DEPLOY_PW 环境变量（/etc/frp-monitor.env）"}
    names, seen = [], set()
    for h in devices or []:
        name = str(h).strip().casefold()
        if name and name not in seen:
            seen.add(name)
            names.append(name)
    if not names:
        return {"ok": False, "error": "未指定设备"}
    started, skipped = [], []
    for hostname in names:
        result = _start_webfix_checked(hostname, "manual")
        if result.get("ok"):
            started.append({"hostname": hostname, "task_id": result["task_id"],
                            "attempts": result["attempts"]})
        else:
            skipped.append({"hostname": hostname, "reason": result.get("reason", "未知原因")})
    return {"ok": True, "started": started, "skipped": skipped}


def webfix_candidates_view():
    """当前快照中的候选（在线 + 有 SSH 代理 + 缺 web 代理），供面板标记。"""
    try:
        hosts, _ = get_snapshot()
    except Exception:  # noqa: BLE001
        return []
    out = []
    for hostname in webfix_candidates(hosts):
        port = webfix_port_for(hostname)
        out.append({"hostname": hostname, "web_port": port})
    return out


def webfix_view():
    """GET /api/webfix/tasks：各设备补配状态（最新一行/设备，含 error）。"""
    rows = store.webfix_tasks_view() if store.enabled else []
    merged = {row["hostname"]: row for row in rows}
    with WEBFIX_LOCK:
        memory = [(t.hostname, t.to_dict()) for t in WEBFIX.values()]
    for hostname, d in memory:  # 内存态为权威（运行中/刚结束的任务）
        if hostname not in merged or d["task_id"] >= merged[hostname]["task_id"]:
            merged[hostname] = d
    tasks = sorted(merged.values(), key=lambda t: -t["task_id"])
    return {"enabled": store.enabled,
            "auto_enabled": WEBFIX_AUTO and store.enabled and bool(DEPLOY_PW),
            "port_base": WEBFIX_PORT_BASE, "port_max": WEBFIX_PORT_MAX,
            "delay": WEBFIX_DELAY,          # 上线后稳定窗口秒数（KVWO-44，增补字段）
            "verify_timeout": int(WEBFIX_VERIFY_TIMEOUT),
            "candidates": webfix_candidates_view(),
            "counts": _task_counts(tasks),
            "tasks": tasks}


def webfix_task_log_view(task_id, offset=0):
    """补配日志增量读取（内存态优先，重启后回落 DB）。"""
    with WEBFIX_LOCK:
        task = WEBFIX.get(task_id)
    if task is not None:
        with task.lock:
            text, total = task.text[offset:], len(task.text)
        return {"ok": True, "task_id": task.id, "hostname": task.hostname,
                "status": task.status, "log": text, "text": text,
                "next_offset": total, "offset": total}
    if store.enabled:
        row = store.webfix_task_log(task_id)
        if row is not None:
            text, status, hostname = row
            text = text or ""
            return {"ok": True, "task_id": task_id, "hostname": hostname, "status": status,
                    "log": text[offset:], "text": text[offset:],
                    "next_offset": len(text), "offset": len(text)}
    return None


def _prune_webfix_memory():
    """清理 24h 前已结束的内存态补配任务（历史回看走 DB 台账）。"""
    with WEBFIX_LOCK:
        stale = [tid for tid, t in WEBFIX.items()
                 if t.status not in ("queued", "running")
                 and time.time() - (t.finished_at or t.created_at) > 86400]
        for tid in stale:
            WEBFIX.pop(tid, None)


# ----------------------------------------------------------------------------
# 数据保留与每日备份（随服务运行的后台线程）
# ----------------------------------------------------------------------------
def run_maintenance(retention=True, backup=True, reason="daily"):
    """30 天数据保留清理 + mysqldump 每日备份；devices / collect_duration 永不清理。"""
    result = {"reason": reason, "retention": {}, "backup": None, "enabled": store.enabled}
    if store.enabled and retention:
        for label, sql, args in (
            ("host_status", "DELETE FROM host_status WHERE ts < NOW() - INTERVAL %s DAY", (RETENTION_DAYS,)),
            ("refresh_batches", "DELETE FROM refresh_batches WHERE ts < NOW() - INTERVAL %s DAY", (RETENTION_DAYS,)),
            ("host_events", "DELETE FROM host_events WHERE ts < NOW() - INTERVAL %s DAY", (RETENTION_DAYS,)),
            ("upgrade_tasks", "DELETE t FROM upgrade_tasks t JOIN upgrade_jobs j "
                              "ON t.job_id = j.id WHERE j.created_at < NOW() - INTERVAL %s DAY", (RETENTION_DAYS,)),
            ("upgrade_jobs", "DELETE FROM upgrade_jobs WHERE created_at < NOW() - INTERVAL %s DAY", (RETENTION_DAYS,)),
        ):
            result["retention"][label] = store._execute(sql, args)
    if store.enabled and backup:
        result["backup"] = _run_db_backup()
    _prune_upgrade_memory()
    _prune_webfix_memory()
    return result


def _run_db_backup():
    """mysqldump frp_monitor → BACKUP_DIR/frp_monitor-<ts>.sql.gz，保留最近 BACKUP_KEEP 份。

    凭据经临时 defaults-extra-file（0600）传入，不出现在命令行。"""
    if not shutil.which("mysqldump"):
        return {"ok": False, "error": "mysqldump 不可用（请安装 mysql-client）"}
    try:
        os.makedirs(BACKUP_DIR, exist_ok=True)
    except OSError as exc:
        return {"ok": False, "error": f"无法创建备份目录 {BACKUP_DIR}: {exc}"}
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    dest = os.path.join(BACKUP_DIR, f"frp_monitor-{stamp}.sql.gz")
    defaults = os.path.join(BACKUP_DIR, f".my-{stamp}.cnf")
    try:
        with open(defaults, "w", encoding="utf-8") as fh:
            fh.write("[client]\n")
            fh.write(f"user={DB_USER}\n")
            fh.write(f"password={DB_PASSWORD}\n")
            fh.write(f"host={DB_HOST}\n")
            fh.write(f"port={DB_PORT}\n")
        os.chmod(defaults, 0o600)
        with open(dest, "wb") as out:
            dumper = subprocess.Popen(
                ["mysqldump", f"--defaults-extra-file={defaults}", "--single-transaction",
                 "--routines", "--triggers", DB_NAME],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            zipper = subprocess.Popen(["gzip", "-6"], stdin=dumper.stdout, stdout=out)
            dumper.stdout.close()  # gzip 先退出时让 mysqldump 收到 SIGPIPE
            dump_rc = dumper.wait(timeout=1800)
            zip_rc = zipper.wait(timeout=600)
            dump_err = dumper.stderr.read().decode("utf-8", "replace")[:300]
        if dump_rc != 0 or zip_rc != 0:
            try:
                os.remove(dest)
            except OSError:
                pass
            return {"ok": False, "error": _sanitize(dump_err or f"mysqldump rc={dump_rc}, gzip rc={zip_rc}")}
    except OSError as exc:
        return {"ok": False, "error": str(exc)}
    finally:
        try:
            os.remove(defaults)
        except OSError:
            pass
    # 只保留最近 BACKUP_KEEP 份
    try:
        backups = sorted(
            (f for f in os.listdir(BACKUP_DIR)
             if f.startswith("frp_monitor-") and f.endswith(".sql.gz")),
            key=lambda f: os.path.getmtime(os.path.join(BACKUP_DIR, f)))
        for old in backups[:-BACKUP_KEEP]:
            os.remove(os.path.join(BACKUP_DIR, old))
    except OSError:
        pass
    return {"ok": True, "file": os.path.basename(dest), "size": os.path.getsize(dest),
            "kept": BACKUP_KEEP, "dir": BACKUP_DIR}


def _parse_maintenance_time(spec):
    hh, sep, mm = (spec or "").strip().partition(":")
    try:
        hh, mm = int(hh), int(mm)
    except ValueError:
        return 3, 30
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return 3, 30
    return hh, mm


def _next_maintenance_slot(now=None):
    hh, mm = _parse_maintenance_time(MAINTENANCE_TIME)
    now = now or datetime.now()
    cand = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if cand <= now:
        cand += timedelta(days=1)
    return cand


def _maintenance_loop():
    while True:
        deadline = _next_maintenance_slot()
        while True:
            remaining = (deadline - datetime.now()).total_seconds()
            if remaining <= 0:
                break
            time.sleep(min(60.0, remaining))
        try:
            result = run_maintenance(reason="daily")
            retention = result.get("retention") or {}
            backup = result.get("backup") or {}
            print(f"frp-monitor: daily maintenance done: retention="
                  f"{ {k: v for k, v in retention.items()} } backup={backup.get('file') or backup.get('error')}",
                  flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"frp-monitor: daily maintenance failed: {exc}", flush=True)



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

    def _json_body(self):
        n = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(n) or b"{}")

    def _api_refresh(self):
        """强刷快照缓存（SOP/旧 fleet-panel 的 GET/POST /api/refresh 兼容入口，
        等价于面板「立即刷新」的 /api/hosts?refresh=1；此前 404 是端点路径不同）。"""
        try:
            refresh_snapshot()
        except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
            self.send_json(502, {"ok": False, "error": str(exc)})
            return
        hosts, ts = get_snapshot()
        self.send_json(200, {"ok": True, "timestamp": ts,
                             "total": len(hosts), "online": len(hosts)})

    def do_GET(self):
        if not self._authorized():
            self.send_unauthorized()
            return
        path = urlsplit(self.path).path
        if path == "/api/upgrade/packages":
            # 包注册表（sha256 基准缓存）+ 计算中项（含进度）；不含签名 URL
            self.send_json(200, {"packages": packages_registry_view()})
            return
        if path == "/api/upgrade/jobs":
            qs = parse_qs(urlsplit(self.path).query)
            try:
                limit = min(max(int(qs.get("limit", ["50"])[0]), 1), 200)
            except (TypeError, ValueError):
                limit = 50
            self.send_json(200, {"jobs": upgrade_jobs_list_view(limit)})
            return
        m = re.match(r"^/api/upgrade/jobs/(\d+)$", path)
        if m:
            job = upgrade_job_view(int(m.group(1)))
            if job is None:
                self.send_json(404, {"error": "unknown job", "job_id": int(m.group(1))})
            else:
                self.send_json(200, job)
            return
        m = re.match(r"^/api/upgrade/jobs/(\d+)/log$", path)
        if m:
            qs = parse_qs(urlsplit(self.path).query)
            job_id = int(m.group(1))
            task_id = hostname = None
            if qs.get("task"):
                try:
                    task_id = int(qs["task"][0])
                except (TypeError, ValueError):
                    return self.send_json(400, {"error": "bad task id"})
            elif qs.get("hostname"):
                hostname = qs["hostname"][0].strip().casefold()
            if task_id is None and not hostname:
                return self.send_json(400, {"error": "需要 task=<id> 或 hostname=<设备名> 指定子任务"})
            try:
                offset = max(0, int(qs.get("offset", ["0"])[0]))
            except (TypeError, ValueError):
                offset = 0
            view = upgrade_task_log_view(job_id, task_id=task_id, hostname=hostname, offset=offset)
            if view is None:
                self.send_json(404, {"error": "unknown job/task", "job_id": job_id})
            else:
                self.send_json(200, view)
            return
        if path == "/api/upgrade/events":
            return self._sse()
        if path == "/api/webfix/tasks":
            # 各设备补配状态（最新一行/设备，含 error）+ 当前候选列表（KVWO-17/19）
            return self.send_json(200, webfix_view())
        m = re.match(r"^/api/webfix/tasks/(\d+)/log$", path)
        if m:
            qs = parse_qs(urlsplit(self.path).query)
            try:
                offset = max(0, int(qs.get("offset", ["0"])[0]))
            except (TypeError, ValueError):
                offset = 0
            view = webfix_task_log_view(int(m.group(1)), offset=offset)
            if view is None:
                return self.send_json(404, {"error": "unknown webfix task",
                                            "task_id": int(m.group(1))})
            return self.send_json(200, view)
        if path == "/api/refresh":
            return self._api_refresh()
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
                        hw = d["hardware"] or _hardware_category(d["hw_model"] or "")
                        fleet.append({
                            "hostname": d["hostname"], "site": d["site"], "owner": d["owner"],
                            "hardware": hw, "retired": d["retired"],
                            "hw_model": ((snap.get("device") or {}).get("hw_model") if snap else "") or d["hw_model"] or "",
                            "ssh_user": ssh_user_for_device(hw, d["hw_model"] or ""),
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
                              "ssh_user": ssh_user_for_device("", (h.get("device") or {}).get("hw_model") or ""),
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
                    "ssh_hint": {"jump": SSH_JUMP, "user": SSH_DEVICE_USER or SSH_USER_ARM},
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
                                     "first_seen": "", "last_seen": "",
                                     "ssh_hint": {"jump": SSH_JUMP,
                                                  "user": ssh_user_for_device("", snap_device.get("hw_model") or "")}})
            else:
                self.send_json(200, {**detail, "ssh_hint": {
                    "jump": SSH_JUMP,
                    "user": ssh_user_for_device(detail.get("hardware") or "",
                                                detail.get("hw_model") or "")}})
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

    def _sse(self):
        """SSE：任务状态变更 / 逐台阶段流转 / 日志块增量。"""
        q = BUS.subscribe()
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            self.wfile.write(b": connected\n\n")
            self.wfile.flush()
            while True:
                try:
                    ev = q.get(timeout=15)
                except queue.Empty:
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                    continue
                data = json.dumps(ev, ensure_ascii=False)
                self.wfile.write(f"data: {data}\n\n".encode("utf-8"))
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            pass
        finally:
            BUS.unsubscribe(q)

    def do_POST(self):
        path = urlsplit(self.path).path
        if not self._authorized():
            self.send_unauthorized()
            return
        if path == "/api/upgrade/packages/hash":
            # 触发服务端从 OSS 直连拉包计算 sha256（异步，拉完即删临时文件）
            try:
                req = self._json_body()
            except json.JSONDecodeError:
                return self.send_json(400, {"ok": False, "error": "bad json"})
            out = start_pkg_hash(str(req.get("oss_url") or ""))
            code = 200 if out.get("ok") else 400
            return self.send_json(code, out)
        if path == "/api/upgrade/jobs":
            try:
                req = self._json_body()
            except json.JSONDecodeError:
                return self.send_json(400, {"ok": False, "error": "bad json"})
            params = req.get("params") or {}
            if not isinstance(params, dict):
                return self.send_json(400, {"ok": False, "error": "params 必须是对象"})
            out = create_upgrade_job(str(req.get("kind") or ""),
                                     req.get("devices") or [], params)
            return self.send_json(200 if out.get("ok") else 400, out)
        if path == "/api/webfix/retry":
            # 手动重试补配（面板认证）；每次新建台账行，attempts 累加，
            # trigger_type=manual；自动触发的一次性语义不受影响（KVWO-17/19）
            try:
                req = self._json_body()
            except json.JSONDecodeError:
                return self.send_json(400, {"ok": False, "error": "bad json"})
            devices = req.get("devices")
            if not isinstance(devices, list):
                return self.send_json(400, {"ok": False,
                                            "error": "devices 必须是设备名数组"})
            out = webfix_retry(devices)
            return self.send_json(200 if out.get("ok") else 400, out)
        if path == "/api/refresh":
            # 与 GET /api/refresh 等价（SOP 工具既有 GET 也有 POST 调法）
            return self._api_refresh()
        if path == "/api/maintenance/run-cleanup":
            result = run_maintenance(retention=True, backup=False, reason="manual")
            return self.send_json(200, result)
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
    # 目录就绪（已存在则无副作用；无权限时不阻塞启动）
    for _d in (BACKUP_DIR, os.path.dirname(SSH_KNOWN_HOSTS), OSS_HASH_TMP_DIR):
        try:
            os.makedirs(_d, exist_ok=True)
        except OSError:
            pass
    if store.enabled:
        try:
            _restarted = store.reconcile_upgrade_jobs()
            if _restarted:
                print(f"frp-monitor: reconciled {_restarted} interrupted upgrade task(s) "
                      f"left by a previous process", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"frp-monitor: upgrade job reconcile failed: {exc}", flush=True)
        try:
            _interrupted = store.reconcile_webfix_tasks()
            if _interrupted:
                print(f"frp-monitor: reconciled {_interrupted} interrupted webfix task(s) "
                      f"left by a previous process", flush=True)
        except Exception as exc:  # noqa: BLE001
            print(f"frp-monitor: webfix reconcile failed: {exc}", flush=True)
    if not DEPLOY_PW:
        print("frp-monitor: DEPLOY_PW not set —— 升级编排不可用（浏览可用），"
              "请在 /etc/frp-monitor.env 配置后重启", flush=True)
    if not (WEBFIX_AUTO and store.enabled and DEPLOY_PW):
        why = "FRP_MONITOR_WEBFIX_AUTO=0" if not WEBFIX_AUTO else (
            "历史库未启用" if not store.enabled else "DEPLOY_PW 未设置")
        print(f"frp-monitor: webfix 自动补配未启用（{why}；POST /api/webfix/retry 亦不可用）",
              flush=True)
    threading.Thread(target=_refresh_loop, name="snapshot-refresher", daemon=True).start()
    threading.Thread(target=_collect_duration_loop, name="collect-duration-puller", daemon=True).start()
    threading.Thread(target=_maintenance_loop, name="daily-maintenance", daemon=True).start()
    print(f"frp-monitor: serving on :{HTTP_PORT} (oss-signed-url distribution, "
          f"upgrade_parallel={UPGRADE_MAX_PARALLEL}, retention={RETENTION_DAYS}d, "
          f"backup={BACKUP_DIR} keep {BACKUP_KEEP}, "
          f"webfix={'auto' if (WEBFIX_AUTO and store.enabled and DEPLOY_PW) else 'off'} "
          f"ports {WEBFIX_PORT_BASE}-{WEBFIX_PORT_MAX}, stable-window {WEBFIX_DELAY:g}s)",
          flush=True)
    httpd = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), Handler)
    httpd.daemon_threads = True  # SSE 长连接随主进程退出，不阻塞重启
    httpd.serve_forever()
