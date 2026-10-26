#!/usr/bin/env python3
"""Live FRP dashboard proxy monitor.

The server asks frps for its current proxy list, so adding/removing an frpc
requires no change to this program. Only proxies currently reported by frps
are returned. Results are naturally sorted by hostname/proxy name.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, unquote, urlsplit
from urllib.request import Request, urlopen
from datetime import datetime, timedelta, timezone
import base64
import hashlib
import hmac
import html
import json
import mimetypes
import os
import queue
import re
import shlex
import shutil
import sqlite3
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

# --- 维修面板（KVWO-78/79）：飞书维修表 lark-cli 定时同步 → 本地 SQLite + 只读 API
# 数据源为飞书多维表（唯一维护入口，面板只读不写回）。同步命令 / base / table /
# 本地库路径全部走环境变量，不硬编码凭证（lark-cli 自身凭证由其配置目录管理，
# 与本服务解耦）。同步失败只记录错误状态、保留上次成功数据继续服务，不影响
# 面板其他页签。配置说明见仓库 REPAIRS.md。
REPAIR_SYNC_ENABLED = os.getenv("FRP_MONITOR_REPAIR_SYNC", "1").strip().lower() not in ("0", "false", "no", "off")
REPAIR_SYNC_INTERVAL = max(60.0, float(os.getenv("FRP_MONITOR_REPAIR_INTERVAL", "300")))  # 默认 5 分钟
REPAIR_LARK_CMD = os.getenv("FRP_MONITOR_REPAIR_LARK_CMD", "lark-cli")
REPAIR_BASE_TOKEN = os.getenv("FRP_MONITOR_REPAIR_BASE_TOKEN", "V0YwbaOzya0ADBsO2wSc8M9Cntf")
REPAIR_TABLE_ID = os.getenv("FRP_MONITOR_REPAIR_TABLE_ID", "tblf1nlwkHhMX1V9")
REPAIR_VIEW_ID = os.getenv("FRP_MONITOR_REPAIR_VIEW", "vewVBaWePF")  # 仅用于拼飞书原记录链接
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPAIR_DB_PATH = os.getenv("FRP_MONITOR_REPAIR_DB", os.path.join(_SCRIPT_DIR, "repairs.sqlite"))

# --- 登录页（2026-10-26）：前端独立文件 + Cookie 会话，Basic Auth 保留 ----------
# 登录页等前端文件放 web/ 目录（不再内嵌 server.py）；会话为 HMAC 签名自包含
# Cookie（exp.签名），密钥未配置时由面板口令派生——重启不失效，改口令即全员下线。
# HTTP Basic（curl -u / 运维脚本）继续可用，与 Cookie 会话并行。
WEB_DIR = os.path.join(_SCRIPT_DIR, "web")
SESSION_COOKIE = "frpmonitor_session"
SESSION_TTL = float(os.getenv("FRP_MONITOR_PANEL_SESSION_TTL", str(7 * 86400)))
SESSION_SECRET = (os.getenv("FRP_MONITOR_PANEL_SESSION_SECRET", "").strip() or
                  hashlib.sha256(b"frp-monitor-session-v1:" + PANEL_PASSWORD.encode("utf-8")
                                 ).hexdigest())
REPAIR_ATTACH_DIR = os.getenv("FRP_MONITOR_REPAIR_ATTACH_DIR", os.path.join(_SCRIPT_DIR, "repair-attachments"))

# 资产多表同步（KVWO-83）：复用维修面板（KVWO-79）的 lark-cli 同步机制与调度模式，
# 在同一 Base 上增量接入 5 张资产表（设备明细/归档/异常处理流转/配件核对/流转记录）。
# 同一 Base 与 lark-cli 命令直接沿用维修面板配置，仅同步开关/间隔/库路径独立，
# 便于资产侧故障隔离。配置说明见 ASSETS.md。
ASSET_SYNC_ENABLED = os.getenv("FRP_MONITOR_ASSET_SYNC", "1").strip().lower() not in ("0", "false", "no", "off")
ASSET_SYNC_INTERVAL = max(60.0, float(os.getenv("FRP_MONITOR_ASSET_INTERVAL", "300")))  # 默认 5 分钟
ASSET_BASE_TOKEN = os.getenv("FRP_MONITOR_ASSET_BASE_TOKEN", REPAIR_BASE_TOKEN)  # 与维修表同一 Base
ASSET_DB_PATH = os.getenv("FRP_MONITOR_ASSET_DB", os.path.join(_SCRIPT_DIR, "assets.sqlite"))

# 资产报告推送与面板标题（KVWO-97）：
# - 库存口径 = 所属单位 ∈ INVENTORY_UNITS（提出人 2026-10-20 确认上海办公室+
#   潍坊办公室均为公司库存未使用；服务商等其他单位持有不计库存）。
#   单位清单走 env 可扩展，逗号分隔。
# - 飞书群机器人 webhook 属敏感凭据：只落在服务器 env 配置，不写入仓库/
#   文档（置空/未配置 = 关闭推送）。默认每日 09:00（服务器本地时间）推送，
#   FRP_MONITOR_REPORT_TIMES 支持多时刻（逗号分隔 HH:MM，与采集时长调度同模式）。
# - 日报正文含主要服务商使用率与版本占比两段（KVWO-107 第六波追加，
#   2026-10-21 提出人确认），与 /api/assets/units、/api/assets/versions 同口径；
#   面板展示保留不变（见 feishu_report_text）。
# - 面板标题：env 覆盖 > lark-cli 解析 Base 全称（启动后台线程 best-effort）
#   > 兜底值（父需求 KVWO-78 记录的 Base 名称）。
def _parse_unit_list(spec):
    """解析逗号分隔单位清单（FRP_MONITOR_INVENTORY_UNITS），去重保序。"""
    seen, out = set(), []
    for part in (spec or "").split(","):
        name = part.strip()
        if name and name not in seen:
            seen.add(name)
            out.append(name)
    return out or ["上海办公室", "潍坊办公室"]


INVENTORY_UNITS = _parse_unit_list(os.getenv("FRP_MONITOR_INVENTORY_UNITS",
                                             "上海办公室,潍坊办公室"))
# 闲置口径（需求 4）：库存外在册设备中，最后在线距今 > IDLE_DAYS（含从未上线）；
# 阈值仅参数化不改口径（提出人 2026-10-20 确认：唯一判据 = 超阈值天数未上线）。
# 第六波（KVWO-107，提出人 2026-10-21）：默认 30 → 15 天，env 覆盖机制保留。
IDLE_DAYS = max(1, int(os.getenv("FRP_MONITOR_IDLE_DAYS", "15")))
FEISHU_WEBHOOK = os.getenv("FRP_MONITOR_FEISHU_WEBHOOK", "").strip()
# 群机器人若开启「签名校验」（实测推送返回 code 19021 sign match fail），
# 需配签名密钥（机器人设置页展示的 secret，同为敏感凭据仅落服务器 env）。
FEISHU_WEBHOOK_SECRET = os.getenv("FRP_MONITOR_FEISHU_WEBHOOK_SECRET", "").strip()
REPORT_TIMES = os.getenv("FRP_MONITOR_REPORT_TIMES", "09:00")
PANEL_TITLE_ENV = os.getenv("FRP_MONITOR_PANEL_TITLE", "").strip()
PANEL_TITLE_FALLBACK = "设备库存与流转管理"  # Base 全称兑底（lark-cli 解析失败时）

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

# 面板前端已拆分至 web/ 独立文件（2026-10-26）：index.html / app.css / app.js，
# 由 Handler 静态路由下发；server.py 仅保留后端逻辑（API / 认证 / 编排）。


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
# 维修面板（KVWO-78/79）：飞书维修表 lark-cli 同步 + 只读 API
# ----------------------------------------------------------------------------
# 飞书字段清单（field_id -> snake_case 键与解析方式）。lark-cli record-list 返回
# 行矩阵 + field_id_list，列序按 field_id 排序、并非固定顺序，因此按 field_id
# 动态对齐；飞书表加列/调序不影响解析，未登记字段自动忽略。
REPAIR_FIELD_MAP = {
    "fldx4c6Flq": ("device_no", "text"),             # 设备编号
    "fldekV0gKB": ("device_type", "select"),         # 设备所属类型（单目/双目/其他）
    "fldXSSDwxu": ("parts", "select_multi"),         # 所含配件（多选）
    "flddlnusAD": ("unit", "text"),                  # 所属单位
    "fld8pX79fy": ("stage", "select"),               # 当前阶段
    "fldQUKUSwQ": ("problem", "text"),               # 问题描述
    "fldPl56KmS": ("contact_name", "text"),          # 姓名
    "fldPyqR6Kz": ("contact_phone", "text"),         # 联系方式
    "fldaYxqLZP": ("recv_address", "text"),          # 收件地址
    "fldK0Omrot": ("return_address", "text"),        # 期望寄回地址和联系方式
    "fldNSnDuEW": ("tracking_no_in", "text"),        # 快递单号（寄入）
    "fld5JmUvOY": ("tracking_no_out", "text"),       # 发出快递单号（寄出）
    "fldb9pcfQp": ("owner_name", "user"),            # 维修负责人（user，取 name）
    "fldVVgRTyK": ("last_followup_at", "datetime"),  # 最后跟进时间
    "fldHpO5cwl": ("last_followup_note", "text"),    # 最后跟进说明
    "fld9KwsKJB": ("attachments", "attachment"),     # 附件（file_token/name/size）
    "fldiwa2Opq": ("created_at", "datetime"),        # 创建时间（飞书自动字段）
}
# 流转阶段固定顺序（排序用）；未知阶段排在已知之后（按名称）
REPAIR_STAGE_ORDER = ["待接收", "损坏原因确认中", "维修中", "待发出确认", "待发出", "已发出", "已完结"]
REPAIR_DONE_STAGE = "已完结"
REPAIR_ITEM_FIELDS = ["record_id", "device_no", "device_type", "parts", "unit", "stage",
                      "problem", "contact_name", "contact_phone", "recv_address",
                      "return_address", "tracking_no_in", "tracking_no_out", "owner_name",
                      "last_followup_at", "last_followup_note", "created_at"]
REPAIR_LARK_TIMEOUT = 120     # 单次 lark-cli record-list 子进程超时（秒）
REPAIR_ATTACH_TIMEOUT = 180   # 单个附件下载超时（秒）


def repair_feishu_url(record_id):
    """飞书原记录链接（面板详情页跳回飞书用）。"""
    return (f"https://archebase.feishu.cn/base/{REPAIR_BASE_TOKEN}"
            f"?table={REPAIR_TABLE_ID}&view={REPAIR_VIEW_ID}&record={record_id}")


def _repair_json_lenient(text):
    """从 lark-cli stdout 解析 JSON（容忍进度前缀，取首个 '{' 起）。"""
    text = text or ""
    pos = text.find("{")
    if pos < 0:
        raise ValueError("输出中不含 JSON")
    return json.loads(text[pos:])


def _repair_cell(cell, kind):
    """按字段类型解析飞书行矩阵单元格 → snake_case 值；空值一律 None。"""
    if cell is None or cell == "":
        return None
    if kind == "text":
        return str(cell).strip() or None
    if kind == "select":
        if isinstance(cell, list):
            items = [str(v).strip() for v in cell if str(v).strip()]
            return items[0] if items else None
        return str(cell).strip() or None
    if kind == "select_multi":
        if isinstance(cell, list):
            items = [str(v).strip() for v in cell if str(v).strip()]
            return items or None
        value = str(cell).strip()
        return [value] if value else None
    if kind == "user":
        if isinstance(cell, list):
            names = [str((u or {}).get("name") or "").strip()
                     for u in cell if isinstance(u, dict)]
            return "、".join(n for n in names if n) or None
        return str(cell).strip() or None
    if kind == "datetime":
        return str(cell).strip() or None
    if kind == "attachment":
        items = []
        for att in (cell if isinstance(cell, list) else [cell]):
            if not isinstance(att, dict):
                continue
            token = str(att.get("file_token") or "").strip()
            if token:
                items.append({"file_token": token,
                              "name": str(att.get("name") or token),
                              "size": int(att.get("size") or 0)})
        return items or None
    return None


def repair_parse_row(row, field_ids):
    """把一行行矩阵（与 field_id_list 对齐）解析为 17 字段 snake_case dict。"""
    rec = {spec[0]: None for spec in REPAIR_FIELD_MAP.values()}
    for fid, cell in zip(field_ids or [], row or []):
        spec = REPAIR_FIELD_MAP.get(fid)
        if spec is not None:
            rec[spec[0]] = _repair_cell(cell, spec[1])
    return rec


class RepairStore:
    """维修记录本地 SQLite 存储（飞书表全量镜像，record_id 为主键）。

    独立于可选的 MySQL 历史库：本地文件即数据，同步失败只记错误状态，
    已入库数据继续服务只读 API；单 RLock 串行化读写，与 Store 同样的
    “任何库问题都不拖垮面板”哲学。
    """

    _COLS = ["record_id", "device_no", "device_type", "parts", "unit", "stage",
             "problem", "contact_name", "contact_phone", "recv_address",
             "return_address", "tracking_no_in", "tracking_no_out", "owner_name",
             "last_followup_at", "last_followup_note", "attachments",
             "created_at", "synced_at"]

    def __init__(self, path):
        self.path = path
        self.lock = threading.RLock()
        self.conn = None
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            self.conn = sqlite3.connect(path, check_same_thread=False)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA busy_timeout=5000")
            self._migrate()
        except (OSError, sqlite3.Error) as exc:
            print(f"frp-monitor: 维修表本地库不可用（{path}: {exc}），维修面板无数据",
                  flush=True)
            self.conn = None

    def _migrate(self):
        with self.lock:
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS repairs ("
                "record_id TEXT PRIMARY KEY, "  # 飞书 record_id（幂等主键）
                + ", ".join(f"{c} TEXT" for c in self._COLS[1:-1])
                + ", synced_at REAL)")
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS repairs_sync ("
                "id INTEGER PRIMARY KEY CHECK (id = 1), "
                "last_sync_at REAL, "   # 最近一次同步尝试（含失败）
                "last_ok_at REAL, "     # 最近一次成功同步（数据新鲜度）
                "last_error TEXT, "     # 最近一次失败原因（成功则清空）
                "record_count INTEGER)")
            self.conn.execute("INSERT OR IGNORE INTO repairs_sync (id) VALUES (1)")
            self.conn.commit()

    @staticmethod
    def _decode(item):
        for col in ("parts", "attachments"):
            item[col] = json.loads(item[col]) if item[col] else None
        return item

    def replace_all(self, records, synced_at=None):
        """全量镜像式幂等写入：record_id upsert，飞书已删记录本地同步删除。"""
        if self.conn is None:
            return 0
        synced_at = time.time() if synced_at is None else synced_at
        json_cols = {"parts", "attachments"}
        cols = self._COLS
        with self.lock:
            cur = self.conn.cursor()
            try:
                cur.execute("BEGIN")
                for rid, rec in records.items():
                    row = {"record_id": rid, "synced_at": synced_at, **rec}
                    values = [json.dumps(row.get(c), ensure_ascii=False) if c in json_cols
                              else row.get(c) for c in cols]
                    cur.execute(f"INSERT OR REPLACE INTO repairs ({','.join(cols)}) "
                                f"VALUES ({','.join('?' * len(cols))})", values)
                if records:
                    cur.execute(f"DELETE FROM repairs WHERE record_id NOT IN "
                                f"({','.join('?' * len(records))})", list(records))
                else:
                    cur.execute("DELETE FROM repairs")
                cur.execute("UPDATE repairs_sync SET last_sync_at = ?, last_ok_at = ?, "
                            "last_error = NULL, record_count = ? WHERE id = 1",
                            (synced_at, synced_at, len(records)))
                self.conn.commit()
            except sqlite3.Error:
                self.conn.rollback()
                raise
        return len(records)

    def set_error(self, error):
        """同步失败：记错误状态，保留上次成功数据与计数不动。"""
        if self.conn is None:
            return
        with self.lock:
            self.conn.execute("UPDATE repairs_sync SET last_sync_at = ?, last_error = ? "
                              "WHERE id = 1", (time.time(), str(error)[:500]))
            self.conn.commit()

    def all_records(self):
        """全部记录 dict 列表，按阶段固定顺序 + 创建时间倒序排列。"""
        if self.conn is None:
            return []
        cols = self._COLS
        with self.lock:
            rows = self.conn.execute(f"SELECT {','.join(cols)} FROM repairs").fetchall()
        items = [self._decode(dict(zip(cols, row))) for row in rows]
        stage_rank = {s: i for i, s in enumerate(REPAIR_STAGE_ORDER)}

        def created_key(it):
            # ISO 8601（同一 +08:00 时区）字典序即时间序；空值排最后
            return (0, it.get("created_at") or "") if it.get("created_at") else (1, "")

        items.sort(key=created_key, reverse=True)
        items.sort(key=lambda it: stage_rank.get(it.get("stage") or "", len(stage_rank) + 1))
        return items

    def get(self, record_id):
        if self.conn is None:
            return None
        cols = self._COLS
        with self.lock:
            row = self.conn.execute(f"SELECT {','.join(cols)} FROM repairs "
                                    "WHERE record_id = ?", (record_id,)).fetchone()
        return self._decode(dict(zip(cols, row))) if row else None

    def sync_state(self):
        empty = {"last_sync_at": None, "last_ok_at": None, "last_sync_error": None,
                 "record_count": 0}
        if self.conn is None:
            return empty
        with self.lock:
            row = self.conn.execute("SELECT last_sync_at, last_ok_at, last_error, "
                                    "record_count FROM repairs_sync WHERE id = 1").fetchone()
        if row is None:
            return empty
        return {"last_sync_at": row[0], "last_ok_at": row[1],
                "last_sync_error": row[2], "record_count": row[3] or 0}


repair_store = RepairStore(REPAIR_DB_PATH)
REPAIR_SYNC_LOCK = threading.Lock()


def _repair_lark_run(args, timeout=REPAIR_LARK_TIMEOUT, cwd=None):
    """跑一次 lark-cli 子进程，stdout 解析 JSON；非零退出 / ok=false 抛 RuntimeError。"""
    argv = shlex.split(REPAIR_LARK_CMD) + list(args)
    try:
        proc = subprocess.run(argv, capture_output=True, text=True, timeout=timeout, cwd=cwd)
    except FileNotFoundError:
        raise RuntimeError(f"lark-cli 不存在（{REPAIR_LARK_CMD}），"
                           f"请安装或配置 FRP_MONITOR_REPAIR_LARK_CMD")
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"lark-cli 超时（>{timeout}s）")
    if proc.returncode != 0:
        raise RuntimeError(f"lark-cli 退出码 {proc.returncode}: "
                           f"{(proc.stderr or proc.stdout).strip()[:300]}")
    try:
        payload = _repair_json_lenient(proc.stdout)
    except (ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"lark-cli 输出非 JSON（{exc}）: {proc.stdout[:200]}")
    if not payload.get("ok"):
        raise RuntimeError("lark-cli 返回失败: "
                           + json.dumps(payload.get("error") or {}, ensure_ascii=False)[:300])
    return payload


def repair_fetch_records():
    """分页全量拉取飞书维修表 → {record_id: {17 字段 snake_case}}。"""
    records, offset = {}, 0
    field_ids = []
    while True:
        payload = _repair_lark_run(["base", "+record-list",
                                    "--base-token", REPAIR_BASE_TOKEN,
                                    "--table-id", REPAIR_TABLE_ID,
                                    "--format", "json",
                                    "--limit", "200", "--offset", str(offset)])
        data = payload.get("data") or {}
        field_ids = data.get("field_id_list") or field_ids
        rows = data.get("data") or []
        rids = data.get("record_id_list") or []
        for rid, row in zip(rids, rows):
            records[rid] = repair_parse_row(row, field_ids)
        if not data.get("has_more") or not rows:
            break
        offset += len(rows)
    return records


def _repair_attach_index():
    """扫描附件目录 → {去扩展名 token: 实际文件名}。

    lark-cli 下载会按内容类型自动给输出文件补扩展名（token → token.jpg），
    本地按裸 token 入索引，URL/服务侧统一用它解析真实文件名。
    """
    index = {}
    try:
        with os.scandir(REPAIR_ATTACH_DIR) as entries:
            for entry in entries:
                if entry.is_file():
                    index[os.path.splitext(entry.name)[0]] = entry.name
    except OSError:
        pass
    return index


def _repair_attach_file(token, index=None):
    """token → 附件目录内实际文件名（无则 None）。"""
    if not token:
        return None
    index = _repair_attach_index() if index is None else index
    return index.get(token)


def repair_sync_attachments(records):
    """把新出现的附件下载到本地静态目录（按 file_token 命名，存在即跳过）。

    多维表附件经 Drive 直下接口 403，走 lark-cli drive +preview
    --type source_file 通道可行（实测）；lark-cli 的 --output 只接受当前
    目录内相对路径，因此以附件目录为 cwd、裸 token 为文件名。单个附件
    失败只记日志，不影响整次同步（该附件 URL 置 null，下轮同步自动补下）。
    """
    try:
        os.makedirs(REPAIR_ATTACH_DIR, exist_ok=True)
    except OSError as exc:
        print(f"frp-monitor: 维修附件目录不可用（{REPAIR_ATTACH_DIR}: {exc}）", flush=True)
        return
    index = _repair_attach_index()
    for rec in records.values():
        for att in rec.get("attachments") or []:
            token = att.get("file_token")
            if not token or _repair_attach_file(token, index) is not None:
                continue
            try:
                _repair_lark_run(["drive", "+preview", "--file-token", token,
                                  "--type", "source_file", "--output", token],
                                 timeout=REPAIR_ATTACH_TIMEOUT, cwd=REPAIR_ATTACH_DIR)
                # lark-cli 会自动补扩展名，按 token 前缀找回实际落盘文件名
                for name in os.listdir(REPAIR_ATTACH_DIR):
                    if name == token or name.startswith(token + "."):
                        index[token] = name
                        break
            except RuntimeError as exc:
                print(f"frp-monitor: 维修附件下载失败 {token}"
                      f"（{att.get('name')}）: {exc}", flush=True)


def run_repair_sync(reason="scheduled"):
    """同步一次飞书维修表 → SQLite 全量镜像；错误也返回摘要、绝不抛出。"""
    with REPAIR_SYNC_LOCK:
        started = time.time()
        try:
            records = repair_fetch_records()
            repair_sync_attachments(records)
            repair_store.replace_all(records)
            print(f"frp-monitor: 维修表同步成功（{reason}）：{len(records)} 条，"
                  f"耗时 {time.time() - started:.1f}s", flush=True)
            return {"ok": True, "count": len(records), "reason": reason,
                    "elapsed": round(time.time() - started, 1)}
        except Exception as exc:  # noqa: BLE001 —— 同步失败要有明确错误状态，不影响其他页签
            try:
                repair_store.set_error(f"{reason}: {exc}")
            except Exception:
                pass
            print(f"frp-monitor: 维修表同步失败（{reason}）: {exc}", flush=True)
            return {"ok": False, "count": repair_store.sync_state()["record_count"],
                    "reason": reason, "error": str(exc),
                    "elapsed": round(time.time() - started, 1)}


def _repair_sync_loop():
    if not REPAIR_SYNC_ENABLED:
        return
    while True:
        run_repair_sync("scheduled")
        time.sleep(REPAIR_SYNC_INTERVAL)


def _repair_online_lookup():
    """返回 device_no → 在线状态 函数：True=当前在线（frps 快照）、
    False=台账已登记但未上线、None=台账与快照均未匹配（数据不合成，显示 —）。"""
    try:
        hosts, _ = get_snapshot()
        online = {h["hostname"] for h in hosts}
    except Exception:
        online = set()  # 快照暂不可得（仅首次拉取失败窗口）：登记设备按离线计
    registered = set(store.roster) if store.enabled else set()

    def lookup(device_no):
        if not device_no:
            return None
        key = str(device_no).strip().casefold()
        if key in online:
            return True
        if key in registered:
            return False
        return None

    return lookup


def _repair_public(rec, lookup=None, attach_index=None):
    """库内记录 → API 视图（17 字段 + feishu_url + 附件本地 URL + device_online）。"""
    item = {k: rec.get(k) for k in REPAIR_ITEM_FIELDS}
    item["feishu_url"] = repair_feishu_url(rec["record_id"])
    attachments = []
    for att in rec.get("attachments") or []:
        entry = dict(att)
        token = entry.get("file_token") or ""
        if _repair_attach_file(token, attach_index) is not None:
            entry["url"] = f"/repair-attachments/{token}/{quote(str(entry.get('name') or token))}"
        else:
            entry["url"] = None
        attachments.append(entry)
    item["attachments"] = attachments or None
    if lookup is not None:
        item["device_online"] = lookup(item.get("device_no"))
    item["synced_at"] = rec.get("synced_at")
    return item


def repairs_list_view(filters=None):
    """维修列表（stage/type 精确、unit 模糊、owner 精确、q 设备编号/问题描述模糊）。"""
    filters = filters or {}
    lookup = _repair_online_lookup()
    attach_index = _repair_attach_index()
    items = [_repair_public(rec, lookup, attach_index) for rec in repair_store.all_records()]
    stage = (filters.get("stage") or "").strip()
    if stage:
        items = [i for i in items if i.get("stage") == stage]
    dtype = (filters.get("type") or "").strip()
    if dtype:
        items = [i for i in items if i.get("device_type") == dtype]
    unit = (filters.get("unit") or "").strip()
    if unit:
        items = [i for i in items if unit in (i.get("unit") or "")]
    owner = (filters.get("owner") or "").strip()
    if owner:
        items = [i for i in items if i.get("owner_name") == owner]
    q = (filters.get("q") or "").strip().casefold()
    if q:
        items = [i for i in items
                 if q in (i.get("device_no") or "").casefold()
                 or q in (i.get("problem") or "").casefold()]
    return items


def repairs_stats_view():
    """顶部统计：总数 / 各阶段计数（7 阶段固定顺序）/ 未完结数 / 同步状态。"""
    items = repair_store.all_records()
    counts = {}
    for it in items:
        stage = it.get("stage") or ""
        counts[stage] = counts.get(stage, 0) + 1
    by_stage = {s: counts.get(s, 0) for s in REPAIR_STAGE_ORDER}
    for stage, n in counts.items():  # 未知阶段原样追加（数据不合成也不丢弃）
        if stage and stage not in by_stage:
            by_stage[stage] = n
    state = repair_store.sync_state()
    return {"total": len(items), "by_stage": by_stage,
            "active_count": sum(n for s, n in counts.items() if s != REPAIR_DONE_STAGE),
            "last_sync_at": state["last_ok_at"],
            "last_sync_attempt_at": state["last_sync_at"],
            "last_sync_error": state["last_sync_error"],
            "sync_enabled": REPAIR_SYNC_ENABLED,
            "sync_interval": int(REPAIR_SYNC_INTERVAL),
            "stage_order": REPAIR_STAGE_ORDER}


# ----------------------------------------------------------------------------
# 资产多表同步与聚合 API（KVWO-83，基于 KVWO-79 维修面板同步机制扩展）
#
# 同一飞书 Base 增量接入 5 张资产表，全部「全量拉取 + record_id 幂等镜像」：
#   设备明细（在册台账）/ 设备归档数据表（仅存量展示，不计入在册指标）/
#   异常处理流转 / 配件核对明细 / 流转记录。
# 各表列序以 lark-cli 返回的 fields 表头动态对齐（中文字段名键控），
# 不硬编码列序——归档表与设备明细列序不同（实测），重命名/调整列序不影响解析，
# 仅字段名变更或删除时该字段退化为 null。
# 同步失败按表隔离：一张表失败只记该表的错误状态，不影响其他表与维修页签。
# ----------------------------------------------------------------------------
ASSET_TABLES = {
    "devices": {
        "table_id": "tblg1YoctJNcZskM",
        "label": "设备明细",           # 在册台账（参与在册指标）
        "sql_table": "asset_devices",
        "fields": {
            "设备编号": ("device_no", "text"),
            "设备类型": ("device_type", "select"),
            "状态": ("status", "select"),
            "所属单位": ("unit", "select"),
            "配件情况": ("accessories", "text"),
            "去向/备注": ("note", "text"),
            "软件版本": ("software_version", "text"),
            "修改人": ("modified_by", "user"),       # 飞书自动字段 updated_by
            "最后更新时间": ("updated_at", "datetime"),  # 飞书自动字段 updated_at
        },
    },
    "archived": {
        "table_id": "tblAeG2cGJ8SjQzR",
        "label": "设备归档数据表",      # 归档/已拆解，仅存量展示，不计入在册指标
        "sql_table": "asset_archived",
        "fields": {  # 与设备明细同构但列序不同（动态对齐不依赖列序）
            "设备编号": ("device_no", "text"),
            "设备类型": ("device_type", "select"),
            "状态": ("status", "select"),
            "所属单位": ("unit", "select"),
            "配件情况": ("accessories", "text"),
            "去向/备注": ("note", "text"),
            "软件版本": ("software_version", "text"),
            "修改人": ("modified_by", "user"),
            "最后更新时间": ("updated_at", "datetime"),
        },
    },
    "actions": {
        "table_id": "tblri9gaBX4zVXzX",
        "label": "异常处理流转",
        "sql_table": "asset_actions",
        "fields": {
            "摘要": ("summary", "text"),
            "说明": ("note", "text"),
            "动作": ("action", "select"),
            "提交人": ("created_by", "user"),        # 飞书自动字段 created_by
            "操作人": ("operator", "user"),
            "转交给": ("transferred_to", "user"),
            "设备编号": ("device_no", "select"),     # 本表内是单选而非文本
            "操作时间": ("operated_at", "datetime"),  # 飞书自动字段 created_at
            "附件": ("attachments", "attachment"),
            "关联异常设备": ("linked_records", "link"),  # link → 异常设备表 record_id 列表
        },
    },
    "parts": {
        "table_id": "tblOLbWKpDlcJkDi",
        "label": "配件核对明细",
        "sql_table": "asset_parts",
        "fields": {
            "核对项": ("item", "text"),
            "配件": ("part", "select"),
            "设备编号": ("device_no", "text"),
            "到货状态": ("arrival_status", "select"),
            "状态说明": ("status_note", "text"),
            "核对人": ("checker", "user"),
            "核对时间": ("checked_at", "datetime"),   # 飞书自动字段 created_at
            "照片": ("photos", "attachment"),
            "关联异常设备": ("linked_records", "link"),
        },
    },
    "transfers": {
        "table_id": "tblxmVpjAHeATuMU",
        "label": "流转记录",
        "sql_table": "asset_transfers",
        "fields": {
            "时间": ("occurred_at", "datetime"),
            "物品": ("item", "text"),
            "数量": ("quantity", "number"),
            "说明": ("note", "text"),
            "所属单位": ("unit", "text"),            # 自由文本（非单选）
            "签收证明": ("proof", "attachment"),
        },
    },
}


def _asset_cell(cell, kind):
    """资产表单元格解析：text/select/多选/user/datetime/attachment 复用维修表
    解析器（KVWO-79），在此补充 link（关联记录 id 列表）与 number 两类。"""
    if kind in ("link", "number"):
        if cell is None or cell == "":
            return None
        if kind == "number":
            try:
                value = float(cell)
                return int(value) if value.is_integer() else value
            except (TypeError, ValueError):
                return None
        ids = []
        for item in (cell if isinstance(cell, list) else [cell]):
            if isinstance(item, dict) and str(item.get("id") or "").strip():
                ids.append(str(item["id"]).strip())
            elif isinstance(item, str) and item.strip():
                ids.append(item.strip())
        return ids or None
    return _repair_cell(cell, kind)


def asset_parse_row(row, fields, field_map):
    """按 fields 表头（中文字段名）动态对齐解析一行行矩阵 → snake_case dict。

    不依赖列序：zip(fields, row) 后按字段名查映射；未映射列忽略，
    映射内缺失列保持 None（数据不合成）。
    """
    rec = {spec[0]: None for spec in field_map.values()}
    for name, cell in zip(fields or [], row or []):
        spec = field_map.get(name)
        if spec is not None:
            rec[spec[0]] = _asset_cell(cell, spec[1])
    return rec


def asset_feishu_url(table_id, record_id):
    """飞书原记录链接（table + record 即可直达，无需视图）。"""
    return (f"https://archebase.feishu.cn/base/{ASSET_BASE_TOKEN}"
            f"?table={table_id}&record={record_id}")


class AssetStore:
    """资产 5 表本地 SQLite 镜像（每表 record_id 主键全量幂等 upsert）。

    与 RepairStore 同一套哲学：独立于可选 MySQL 历史库，本地文件即数据；
    同步失败按表记错误状态、保留上次成功数据；单 RLock 串行化读写。
    """

    def __init__(self, path):
        self.path = path
        self.lock = threading.RLock()
        self.conn = None
        try:
            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            self.conn = sqlite3.connect(path, check_same_thread=False)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA busy_timeout=5000")
            self._migrate()
        except (OSError, sqlite3.Error) as exc:
            print(f"frp-monitor: 资产表本地库不可用（{path}: {exc}），资产面板无数据",
                  flush=True)
            self.conn = None

    @staticmethod
    def _spec_cols(spec):
        """(全部列名, JSON 列集合)：列名为 snake_case JSON 键，含 number 列标记。"""
        cols = [v[0] for v in spec["fields"].values()]
        json_cols = {v[0] for v in spec["fields"].values()
                     if v[1] in ("attachment", "link", "select_multi")}
        return cols, json_cols

    def _migrate(self):
        with self.lock:
            for spec in ASSET_TABLES.values():
                cols, _ = self._spec_cols(spec)
                decls = ["record_id TEXT PRIMARY KEY"]
                for col, (name, (_, kind)) in zip(cols, spec["fields"].items()):
                    decls.append(f"{col} {'NUM' if kind == 'number' else 'TEXT'}")
                decls.append("synced_at REAL")
                self.conn.execute(
                    f"CREATE TABLE IF NOT EXISTS {spec['sql_table']} ({', '.join(decls)})")
            self.conn.execute(
                "CREATE TABLE IF NOT EXISTS asset_sync ("
                "tbl TEXT PRIMARY KEY, "      # ASSET_TABLES 键（按表隔离错误状态）
                "last_sync_at REAL, "         # 最近一次同步尝试（含失败）
                "last_ok_at REAL, "           # 最近一次成功同步（数据新鲜度）
                "last_error TEXT, "           # 最近一次失败原因（成功则清空）
                "record_count INTEGER)")
            self.conn.executemany(
                "INSERT OR IGNORE INTO asset_sync (tbl) VALUES (?)",
                [(k,) for k in ASSET_TABLES])
            self.conn.commit()

    def replace_all(self, key, records, synced_at=None):
        """单表全量镜像式幂等写入：record_id upsert + 删除飞书已删记录。"""
        if self.conn is None:
            return 0
        spec = ASSET_TABLES[key]
        cols, json_cols = self._spec_cols(spec)
        all_cols = ["record_id"] + cols + ["synced_at"]
        synced_at = time.time() if synced_at is None else synced_at
        with self.lock:
            cur = self.conn.cursor()
            try:
                cur.execute("BEGIN")
                for rid, rec in records.items():
                    row = {"record_id": rid, "synced_at": synced_at, **rec}
                    values = [json.dumps(row.get(c), ensure_ascii=False) if c in json_cols
                              else row.get(c) for c in all_cols]
                    cur.execute(
                        f"INSERT OR REPLACE INTO {spec['sql_table']} ({','.join(all_cols)}) "
                        f"VALUES ({','.join('?' * len(all_cols))})", values)
                if records:
                    cur.execute(
                        f"DELETE FROM {spec['sql_table']} WHERE record_id NOT IN "
                        f"({','.join('?' * len(records))})", list(records))
                else:
                    cur.execute(f"DELETE FROM {spec['sql_table']}")
                cur.execute(
                    "UPDATE asset_sync SET last_sync_at = ?, last_ok_at = ?, "
                    "last_error = NULL, record_count = ? WHERE tbl = ?",
                    (synced_at, synced_at, len(records), key))
                self.conn.commit()
            except sqlite3.Error:
                self.conn.rollback()
                raise
        return len(records)

    def set_error(self, key, error):
        """单表同步失败：只记该表错误状态，其余表与已有数据不受影响。"""
        if self.conn is None:
            return
        with self.lock:
            self.conn.execute(
                "UPDATE asset_sync SET last_sync_at = ?, last_error = ? WHERE tbl = ?",
                (time.time(), str(error)[:500], key))
            self.conn.commit()

    def all_records(self, key):
        """单表全部记录（dict 列表，原始库内顺序）。"""
        if self.conn is None:
            return []
        spec = ASSET_TABLES[key]
        cols, json_cols = self._spec_cols(spec)
        all_cols = ["record_id"] + cols + ["synced_at"]
        with self.lock:
            rows = self.conn.execute(
                f"SELECT {','.join(all_cols)} FROM {spec['sql_table']}").fetchall()
        items = []
        for row in rows:
            item = dict(zip(all_cols, row))
            for col in json_cols:
                item[col] = json.loads(item[col]) if item[col] else None
            items.append(item)
        return items

    def sync_state(self, key):
        empty = {"last_sync_at": None, "last_ok_at": None, "last_sync_error": None,
                 "record_count": 0}
        if self.conn is None:
            return empty
        with self.lock:
            row = self.conn.execute(
                "SELECT last_sync_at, last_ok_at, last_error, record_count "
                "FROM asset_sync WHERE tbl = ?", (key,)).fetchone()
        if row is None:
            return empty
        return {"last_sync_at": row[0], "last_ok_at": row[1],
                "last_sync_error": row[2], "record_count": row[3] or 0}

    def sync_states(self):
        """全部表同步状态（按表隔离展示：{key: {..., label, table_id}}）。"""
        out = {}
        for key, spec in ASSET_TABLES.items():
            state = self.sync_state(key)
            out[key] = {**state, "label": spec["label"], "table_id": spec["table_id"]}
        return out


asset_store = AssetStore(ASSET_DB_PATH)
ASSET_SYNC_LOCK = threading.Lock()


def _asset_lark_page(args, attempts=3):
    """拉一页（带退避重试）：lark-cli 瞬时失败（限流等）不该让整表本轮同步失败。"""
    last = None
    for attempt in range(attempts):
        if attempt:
            time.sleep(2 * attempt)
        try:
            return _repair_lark_run(args)
        except RuntimeError as exc:
            last = exc
    raise last


def asset_fetch_table(key):
    """分页全量拉取一张资产表 → {record_id: {snake_case 字段}}。

    列对齐键为 fields 表头（中文字段名），与列序无关；单页瞬时失败
    退避重试（限流），重试耗尽才让该表本轮同步失败（按表隔离）。
    """
    spec = ASSET_TABLES[key]
    records, offset = {}, 0
    fields = []
    while True:
        data = (_asset_lark_page(["base", "+record-list",
                                  "--base-token", ASSET_BASE_TOKEN,
                                  "--table-id", spec["table_id"],
                                  "--format", "json",
                                  "--limit", "200", "--offset", str(offset)])
                .get("data") or {})
        fields = data.get("fields") or fields
        rows = data.get("data") or []
        rids = data.get("record_id_list") or []
        for rid, row in zip(rids, rows):
            records[rid] = asset_parse_row(row, fields, spec["fields"])
        if not data.get("has_more") or not rows:
            break
        offset += len(rows)
    return records


def run_asset_sync(reason="scheduled"):
    """同步全部 5 张资产表 → SQLite 镜像；按表隔离失败，绝不抛出。

    单表失败只记该表错误状态，其余表照常完成（一张表失败不影响其他表
    与维修页签）。附件元数据入库但不自动下载（体积大且资产页签验收
    不要求附件可访问；如已存在本地文件则解析出 URL，见 _asset_public）。
    """
    with ASSET_SYNC_LOCK:
        started = time.time()
        results = {}
        for key, spec in ASSET_TABLES.items():
            t0 = time.time()
            try:
                records = asset_fetch_table(key)
                asset_store.replace_all(key, records)
                results[key] = {"ok": True, "count": len(records),
                                "elapsed": round(time.time() - t0, 1)}
                print(f"frp-monitor: 资产表同步成功（{spec['label']}，{reason}）："
                      f"{len(records)} 条，耗时 {time.time() - t0:.1f}s", flush=True)
            except Exception as exc:  # noqa: BLE001 —— 单表失败隔离，不拖垮其他表
                try:
                    asset_store.set_error(key, f"{reason}: {exc}")
                except Exception:
                    pass
                results[key] = {"ok": False, "count": asset_store.sync_state(key)["record_count"],
                                "error": str(exc), "elapsed": round(time.time() - t0, 1)}
                print(f"frp-monitor: 资产表同步失败（{spec['label']}，{reason}）: {exc}",
                      flush=True)
        return {"ok": all(r.get("ok") for r in results.values()),
                "reason": reason, "tables": results,
                "elapsed": round(time.time() - started, 1)}


def _asset_sync_loop():
    if not ASSET_SYNC_ENABLED:
        return
    while True:
        run_asset_sync("scheduled")
        time.sleep(ASSET_SYNC_INTERVAL)


# --- 资产日报定时飞书 webhook 推送（KVWO-97） -------------------------------
# 服务端自行定时推送，不依赖外部 agent 轮询；调度模式与采集时长
# （_collect_duration_loop，FRP_MONITOR_COLLECT_TIMES=19:00,21:00）一致：
# 每轮从墙上时钟重推下一次时刻，进程重启自然对齐，不依赖内存计数器。
def _parse_report_slots(spec):
    """解析 FRP_MONITOR_REPORT_TIMES（"09:00" 或多时刻）为排序去重时间点。"""
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
    return sorted(slots) or [(9, 0)]


REPORT_SLOTS = _parse_report_slots(REPORT_TIMES)
_FEISHU_REPORT_LOCK = threading.Lock()  # 定时调度与手动触发串行，避免同刻重复推送
_feishu_report_state = {"last": None, "next_run_at": None}  # 状态可查（GET /api/assets/report）


def _next_report_slot(now=None):
    """下一个日报推送时刻（服务器本地时间）。"""
    now = now or datetime.now()
    for hh, mm in REPORT_SLOTS:
        cand = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if cand > now:
            return cand
    hh, mm = REPORT_SLOTS[0]
    return (now + timedelta(days=1)).replace(hour=hh, minute=mm, second=0, microsecond=0)


def feishu_report_text(stats, now=None):
    """由 /api/assets/stats·units·versions 口径拼中文日报文本（同一数据源，不另算死数）。

    KVWO-107 追加（2026-10-21 提出人确认，日报必须体现）：正文含
    「主要服务商使用率（前 5 家，按在册数）」与「主要服务商版本占比
    （前 5 家）」两段，实时取 assets_units_view / assets_versions_view
    同口径同排序（在册数降序、并列按单位名稳定），定时推送与手动推送
    （POST /api/assets/report/run，含面板「立即推送日报」弹窗）共用本
    函数，文案天然一致；不足 5 家如实全列，无在册设备时整段「—」。
    """
    now = now or datetime.now()
    inv = stats.get("inventory") or {}
    by_unit = inv.get("by_unit") or {}
    units = inv.get("units") or list(INVENTORY_UNITS)
    inv_parts = " + ".join(f"{u} {by_unit.get(u, 0)} 台" for u in units)
    n = stats.get("registered_total") or 0
    by_status = stats.get("by_status") or {}
    repairing = by_status.get("维修中", 0) + by_status.get("待维修", 0)
    pending = by_status.get("待维修", 0)
    abnormal = by_status.get("异常", 0)

    def pct(v):
        return "—" if v is None else f"{v * 100:.1f}%"

    lines = [
        f"【{panel_title()} · 资产日报】{now.strftime('%Y-%m-%d %H:%M')}",
        f"库存：{inv.get('total', 0)} 台（{inv_parts}）",
        "口径：公司库存（" + "、".join(units) + "），服务商等其他单位持有不计库存",
        f"在册硬件：{n} 台（一线部署 {stats.get('deployed', 0)} 台；"
        f"另有归档 {stats.get('archived_total', 0)} 台不计在册）",
        f"维修率：{pct(stats.get('repair_rate'))}（维修中 {repairing - pending} 台 "
        f"+ 待维修 {pending} 台，分母在册 {n} 台）",
        f"损坏率：{pct(stats.get('damage_rate'))}（异常 {abnormal} 台 + 维修中 "
        f"{repairing - pending} 台 + 待维修 {pending} 台，分母在册 {n} 台）",
    ]
    idle = stats.get("idle") or {}
    if idle.get("total") is None:
        lines.append(f"闲置：—（fleet 库未启用，暂不可计算；口径：库存外超 "
                     f"{idle.get('threshold_days', IDLE_DAYS)} 天未上线，含从未上线）")
    else:
        lines.append(f"闲置：{idle['total']} 台（库存外超 "
                     f"{idle.get('threshold_days', IDLE_DAYS)} 天未上线，含从未上线）")
    # 主要服务商使用率 / 版本占比两段（KVWO-107 追加，置于闲置与数据时间之间）：
    # 使用率行 = 在线快照数/在册（百分比与既有 pct 同保留 1 位）；版本行紧凑分布，
    # share 取 API 同值（两位小数）展示为整数百分比；未探测/空版本归「未获取」桶。
    top_units = assets_units_view()[:5]
    lines.append("主要服务商使用率（前 5 家，按在册数）：")
    if top_units:
        lines.extend(f"{u['unit']}：在线 {u['online']}/在册 {u['total']}（{pct(u['usage_rate'])}）"
                     for u in top_units)
    else:
        lines.append("—（无在册设备）")
    top5 = assets_versions_view()["by_provider_top5"]
    lines.append("主要服务商版本占比（前 5 家）：")
    if top5:
        lines.extend(f"{t['unit']}（{t['total']} 台）："
                     + " / ".join(f"{v['version']} {v['count']} 台 {v['share'] * 100:.0f}%"
                                   for v in t["versions"])
                     for t in top5)
    else:
        lines.append("—（无在册设备）")
    ts = stats.get("last_sync_at")
    if ts:
        lines.append("数据时间：" + datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
                     + "（飞书台账最近一次成功同步）")
    else:
        lines.append("数据时间：—（尚未成功同步，以下指标可能为空）")
    if stats.get("last_sync_error"):
        lines.append("⚠ 飞书同步异常（数据可能滞后，以台账为准）："
                     + str(stats["last_sync_error"])[:120])
    return "\n".join(lines)


def _feishu_webhook_sign(secret, timestamp):
    """飞书自定义机器人签名（官方算法）：key = "timestamp\nsecret"，消息体为空。"""
    string_to_sign = f"{timestamp}\n{secret}"
    return base64.b64encode(
        hmac.new(string_to_sign.encode("utf-8"), digestmod=hashlib.sha256).digest()
    ).decode("utf-8")


def _feishu_webhook_post(text, attempts=3):
    """POST 飞书群机器人 text 消息；带退避重试，返回 (ok, http_status, detail)。

    配置了签名密钥时按官方算法附带 timestamp+sign（机器人开启签名校验
    必需，否则飞书拒绝 code=19021 sign match fail）。"""
    payload = {"msg_type": "text", "content": {"text": text}}
    if FEISHU_WEBHOOK_SECRET:
        ts = str(int(time.time()))
        payload.update(timestamp=ts, sign=_feishu_webhook_sign(FEISHU_WEBHOOK_SECRET, ts))
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    last_code, last_detail = None, ""
    for attempt in range(attempts):
        if attempt:
            time.sleep(2 * attempt)
        try:
            req = Request(FEISHU_WEBHOOK, data=body, method="POST",
                          headers={"Content-Type": "application/json"})
            with urlopen(req, timeout=10) as resp:
                status = resp.status
                raw = resp.read(4096).decode("utf-8", "replace")
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            last_code, last_detail = None, f"{type(exc).__name__}: {exc}"
            continue
        try:
            resp_json = json.loads(raw or "{}")
        except ValueError:
            resp_json = {}
        # 新版 {code:0} / 旧版 {StatusCode:0}；缺省视为成功（HTTP 200 即送达）
        biz = resp_json.get("code", resp_json.get("StatusCode", 0))
        if status == 200 and biz == 0:
            return True, status, raw[:200]
        if biz == 19021:
            last_detail = (f"飞书拒绝 code=19021（签名校验失败）：请在群机器人设置"
                           f"核对签名密钥（FRP_MONITOR_FEISHU_WEBHOOK_SECRET）或关闭"
                           f"签名校验；resp={raw[:120]}")
            last_code = status
            break  # 签名失败是确定性错误：重试无意义
        else:
            last_detail = raw[:200]
        last_code = status
    return False, last_code, last_detail


def run_feishu_report(reason="scheduled"):
    """推送一次资产日报到飞书群；失败只记日志与状态，绝不抛出、
    不影响面板服务与飞书同步（验收 4）。"""
    with _FEISHU_REPORT_LOCK:
        stats = assets_stats_view()
        text = feishu_report_text(stats)
        try:
            ok, http_status, detail = _feishu_webhook_post(text)
        except Exception as exc:  # noqa: BLE001 —— 推送链路异常不拖垮调度线程
            ok, http_status, detail = False, None, f"{type(exc).__name__}: {exc}"
        record = {"ok": ok, "reason": reason, "at": time.time(),
                  "http_status": http_status, "detail": (detail or "")[:200],
                  "text_preview": text[:80]}
        _feishu_report_state["last"] = record
        print(f"frp-monitor: 资产日报推送{'成功' if ok else '失败'}"
              f"（{reason}，HTTP {http_status}）"
              + ("" if ok else f": {detail}"), flush=True)
        return {**record, "text": text}  # 手动触发回显全文，便于验收核对文案


def _feishu_report_loop():
    if not FEISHU_WEBHOOK:
        return  # 未配置 webhook：不启动推送（FRP_MONITOR_FEISHU_WEBHOOK 置空=关闭）
    while True:
        deadline = _next_report_slot()
        with _FEISHU_REPORT_LOCK:
            _feishu_report_state["next_run_at"] = deadline.timestamp()
        while True:  # 分段睡眠到点，兼顾长时间休眠与时钟漂移
            remaining = (deadline - datetime.now()).total_seconds()
            if remaining <= 0:
                break
            time.sleep(min(30.0, remaining))
        try:
            run_feishu_report("scheduled")
        except Exception as exc:  # noqa: BLE001 —— 调度线程异常不退出
            print(f"frp-monitor: 资产日报调度异常: {exc}", flush=True)


# --- 面板标题（KVWO-97）：env 覆盖 > lark-cli 解析 Base 全称 > 兑底值 ----------
# 浏览器标签（<title>）与页头（<h1>）一致；Base 全称启动时后台 best-effort
# 解析（命令形态未文档化，多形态尝试，失败不阻塞），成功前先用兑底值服务。
_panel_title_lock = threading.Lock()
_panel_title = {"value": PANEL_TITLE_ENV or PANEL_TITLE_FALLBACK,
                "source": "env" if PANEL_TITLE_ENV else "fallback"}


def panel_title():
    with _panel_title_lock:
        return _panel_title["value"]


def _find_base_name(node, depth=0):
    """在 lark-cli 返回 JSON 里浅层找 Base 名称（name/base_name/app_name/title 键）。"""
    if depth > 3 or not isinstance(node, dict):
        return None
    for key in ("name", "base_name", "app_name", "title"):
        v = node.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    for v in node.values():
        if isinstance(v, dict):
            found = _find_base_name(v, depth + 1)
            if found:
                return found
    return None


def _lark_base_name():
    """best-effort 用 lark-cli 查 Base 全称；多命令形态尝试，全部失败返回 None。"""
    for args in (("base", "+base-info", "--base-token", ASSET_BASE_TOKEN),
                 ("base", "info", "--base-token", ASSET_BASE_TOKEN)):
        try:
            payload = _repair_lark_run(list(args), timeout=20)
        except Exception:  # noqa: BLE001 —— 命令不存在/失败：换下一个形态
            continue
        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        name = _find_base_name(data)
        if name:
            return name
    return None


def _resolve_panel_title():
    if PANEL_TITLE_ENV:
        return  # env 显式覆盖：不查 lark
    name = _lark_base_name()
    if name:
        with _panel_title_lock:
            _panel_title.update(value=name, source="lark")
        print(f"frp-monitor: 面板标题已取 Base 全称「{name}」", flush=True)


def _asset_fleet_lookup():
    """返回资产 API 的探测查询组（device_online / detected_version / 全量快照）。

    设备编号匹配规则（全库统一，大小写不敏感）：台账存在 `ep-000298` 小写、
    异常表 `EP-000289` 等混杂情况，一律 str.strip().casefold() 后比对——
    frps 主机名本身就是小写 `ep-000xxx`，casefold 即天然对齐。
    device_online：frps 快照在线→true；MySQL 台账已登记但未上线→false；
    均未匹配→null（数据不合成，前端显示「—」）。
    detected_version：在线取当前探测版本；离线但 MySQL 历史有最后已知版本
    则返回之；否则 null（探测不到 ≠ 离线，与 device_online 语义分开）。
    """
    try:
        hosts, _ = get_snapshot()
        snap = {}
        for h in hosts:
            snap[str(h.get("hostname") or "").strip().casefold()] = {
                "online": True, "version": (str(h.get("version") or "").strip() or None)}
    except Exception:
        snap = {}  # 快照暂不可得（仅首次拉取失败窗口）：登记设备按离线计
    registered = set(store.roster) if store.enabled else set()
    last_known = {}
    if store.enabled:
        with store.lock:
            last_known = {str(h).strip().casefold(): (v or {}).get("version") or None
                          for h, v in (store.state or {}).items()}

    def online(device_no):
        if not device_no:
            return None
        key = str(device_no).strip().casefold()
        if key in snap:
            return True
        if key in registered:
            return False
        return None

    def version(device_no):
        if not device_no:
            return None
        key = str(device_no).strip().casefold()
        info = snap.get(key)
        if info is not None:
            return info["version"]
        return last_known.get(key) or None

    return online, version, snap


def _dev_key(device_no):
    """设备编号归一（跨表关联统一入口，见 _asset_fleet_lookup 注释）。"""
    return str(device_no or "").strip().casefold()


ASSET_DEVICE_ITEM_FIELDS = ["record_id", "device_no", "device_type", "status", "unit",
                            "accessories", "note", "software_version", "modified_by",
                            "updated_at"]


def _asset_attach_urls(items):
    """附件元数据列表 → 带 url 的视图（仅当本地已存在同名 token 文件时给出
    /repair-attachments/ 静态路径；资产表附件默认不主动下载，url 多为 null）。"""
    out = []
    for att in items or []:
        if not isinstance(att, dict):
            continue
        entry = dict(att)
        token = str(entry.get("file_token") or "")
        entry["url"] = (f"/repair-attachments/{token}/"
                        + quote(str(entry.get("name") or token))
                        if token and _repair_attach_file(token) is not None else None)
        out.append(entry)
    return out or None


def _asset_device_public(rec, online=None, version=None, repairs_by_dev=None,
                          table_key="devices", last_seen=None):
    """设备明细/归档库内记录 → API 视图（全字段 + 探测联动 + 未完结维修数）。

    last_seen（fleet 库映射）非 None 时附 `last_seen`（最后在线，null=从未
    上线/无记录）与 `idle`（闲置判定；归档设备不在册，置 null）。"""
    item = {k: rec.get(k) for k in ASSET_DEVICE_ITEM_FIELDS}
    item["archived"] = table_key == "archived"
    item["feishu_url"] = asset_feishu_url(ASSET_TABLES[table_key]["table_id"],
                                          rec["record_id"])
    item["device_online"] = online(item.get("device_no")) if online else None
    item["detected_version"] = version(item.get("device_no")) if version else None
    item["active_repairs"] = ((repairs_by_dev or {}).get(_dev_key(item.get("device_no"))) or 0) \
        if repairs_by_dev is not None else None
    item["synced_at"] = rec.get("synced_at")
    item["last_seen"] = None
    item["idle"] = None
    if last_seen is not None:
        item["last_seen"] = last_seen.get(_dev_key(item.get("device_no"))) or None
        if not item["archived"]:
            item["idle"] = _device_idle(item.get("device_no"), item.get("unit"), last_seen)
    return item


def _asset_active_repairs_index():
    """{设备编号 casefold: 未完结维修记录数}（来源：KVWO-79 异常设备表镜像）。"""
    index = {}
    for rec in repair_store.all_records():
        stage = rec.get("stage")
        if not stage or stage == REPAIR_DONE_STAGE:
            continue
        key = _dev_key(rec.get("device_no"))
        if key:
            index[key] = index.get(key, 0) + 1
    return index


def _fleet_last_seen():
    """frps fleet 库每台设备最后在线时间（MySQL devices.last_seen_at，KVWO-97 需求 4）。

    返回 None = fleet 库未启用/不可查（闲置不可计算，如实置空不合成）；
    否则 {设备编号 casefold: 'YYYY-MM-DD HH:MM'}，无记录/空串 = 从未上线。
    """
    if not store.enabled:
        return None
    try:
        return {_dev_key(d["hostname"]): (d.get("last_seen") or "")
                for d in store.devices_view()}
    except Exception:  # noqa: BLE001 —— 查询失败同未启用：不合成在线历史
        return None


def _device_idle(device_no, unit, last_seen, now=None):
    """单台闲置判定：库存外 + fleet 最后在线距今 > IDLE_DAYS（含从未上线）。

    - 库存单位设备不算闲置（本就离线存放，即使从未上线）；
    - last_seen 为 None（fleet 库不可用）→ 返回 None（不可判定，不合成）；
    - 从未上线（无记录/空串/时间不可解析）→ 闲置；
    - 归档设备不在此判定（在册口径外，调用方置 idle=null）。
    """
    if last_seen is None:
        return None
    if (unit or "") in INVENTORY_UNITS:
        return False
    seen = last_seen.get(_dev_key(device_no))
    if not seen:
        return True
    try:
        return ((now or datetime.now()) - datetime.strptime(seen, "%Y-%m-%d %H:%M")
                > timedelta(days=IDLE_DAYS))
    except ValueError:
        return True


def _idle_stats(devices, last_seen, now=None):
    """闲置指标：{total, threshold_days, by_unit}（fleet 不可用时 total=None）。"""
    if last_seen is None:
        return {"total": None, "threshold_days": IDLE_DAYS, "by_unit": {}}
    now = now or datetime.now()
    by_unit, total = {}, 0
    for d in devices:
        if _device_idle(d.get("device_no"), d.get("unit"), last_seen, now):
            total += 1
            u = d.get("unit") or "(未填写)"
            by_unit[u] = by_unit.get(u, 0) + 1
    return {"total": total, "threshold_days": IDLE_DAYS, "by_unit": by_unit}


def assets_list_view(filters=None):
    """台账列表：status/type/unit 精确 + q（设备编号/备注模糊，大小写不敏感）；
    include_archived=1 时追加归档设备（archived=true 标记）。"""
    filters = filters or {}
    online, version, _ = _asset_fleet_lookup()
    last_seen = _fleet_last_seen()
    repairs_by_dev = _asset_active_repairs_index()
    items = [_asset_device_public(rec, online, version, repairs_by_dev,
                                  last_seen=last_seen)
             for rec in asset_store.all_records("devices")]
    if str(filters.get("include_archived") or "").strip().lower() in ("1", "true", "yes"):
        for rec in asset_store.all_records("archived"):
            items.append(_asset_device_public(rec, online, version, repairs_by_dev,
                                              table_key="archived", last_seen=last_seen))
    status = (filters.get("status") or "").strip()
    if status:
        items = [i for i in items if i.get("status") == status]
    dtype = (filters.get("type") or "").strip()
    if dtype:
        items = [i for i in items if i.get("device_type") == dtype]
    unit = (filters.get("unit") or "").strip()
    if unit:
        items = [i for i in items if i.get("unit") == unit]
    q = (filters.get("q") or "").strip().casefold()
    if q:
        items = [i for i in items
                 if q in (i.get("device_no") or "").casefold()
                 or q in (i.get("note") or "").casefold()]
    return items


def assets_stats_view():
    """资产总览指标（全部由同步数据实时计算，不落死数）。

    口径（提出人 2026-10-20 确认，归档不计入在册）：
    - 在册 registered_total = 设备明细全部记录
    - 库存 = 所属单位 ∈ INVENTORY_UNITS（默认上海办公室+潍坊办公室，均为
      公司库存未使用；服务商等其他单位持有不计库存，清单走
      FRP_MONITOR_INVENTORY_UNITS 可扩展，分单位明细见 inventory.by_unit）
    - 口径 B（备注含「未使用」）仅作参考保留：unused_note
    - 一线部署 deployed = 在册 − 库存
    - 维修率 = (维修中 + 待维修) / 在册；损坏率 = (异常 + 维修中 + 待维修) / 在册
    """
    devices = asset_store.all_records("devices")
    archived = asset_store.all_records("archived")
    n = len(devices)

    def counter(field):
        counts = {}
        for d in devices:
            key = d.get(field) or ""
            counts[key] = counts.get(key, 0) + 1
        # 空值单独成桶（数据不合成也不静默丢弃）
        return counts

    by_status = counter("status")
    by_type = counter("device_type")
    by_unit = counter("unit")
    # 库存分单位明细：配置单位全部列出（无设备时计 0，便于对账）
    inv_by_unit = {u: by_unit.get(u, 0) for u in INVENTORY_UNITS}
    inv_total = sum(inv_by_unit.values())
    inv_b = sum(1 for d in devices if "未使用" in str(d.get("note") or ""))
    repairing = by_status.get("维修中", 0) + by_status.get("待维修", 0)
    damaged = by_status.get("异常", 0) + repairing
    idle = _idle_stats(devices, _fleet_last_seen())
    state = asset_store.sync_state("devices")
    return {
        "registered_total": n,
        "archived_total": len(archived),
        "inventory": {"total": inv_total, "by_unit": inv_by_unit,
                      "units": list(INVENTORY_UNITS), "unused_note": inv_b},
        "deployed": n - inv_total,
        "idle": idle,
        "by_status": by_status,
        "by_type": by_type,
        "by_unit": by_unit,
        "repair_rate": round(repairing / n, 4) if n else None,
        "damage_rate": round(damaged / n, 4) if n else None,
        "last_sync_at": state["last_ok_at"],
        "last_sync_attempt_at": state["last_sync_at"],
        "last_sync_error": state["last_sync_error"],
        "tables": asset_store.sync_states(),
    }


def assets_units_view():
    """项目（所属单位）维度聚合：设备数 / 正常率 / 维修中 / 异常 / 使用率 / 最近更新时间。

    维修中数 = 状态为「维修中」+「待维修」；正常率 = 正常 / 该单位设备数；
    使用率（KVWO-107 第六波，口径：frps 在线快照数 / 该单位在册设备数，
    在册为 0 记 null；设备编号匹配与 /api/assets 同规则，大小写不敏感）；
    最近更新时间 = 该单位设备「最后更新时间」的最大值（ISO 字典序即时间序）。
    单位详情由前端引用 /api/assets?unit=… 过滤，不另设接口。
    """
    devices = asset_store.all_records("devices")
    _, _, snap = _asset_fleet_lookup()
    units = {}
    for d in devices:
        name = d.get("unit") or "(未填写)"
        u = units.setdefault(name, {"unit": name, "total": 0, "normal": 0,
                                    "repairing": 0, "abnormal": 0, "online": 0,
                                    "last_updated": None})
        u["total"] += 1
        if _dev_key(d.get("device_no")) in snap:
            u["online"] += 1
        status = d.get("status") or ""
        if status == "正常":
            u["normal"] += 1
        elif status in ("维修中", "待维修"):
            u["repairing"] += 1
        elif status == "异常":
            u["abnormal"] += 1
        ts = d.get("updated_at") or ""
        if ts and (u["last_updated"] or "") < ts:
            u["last_updated"] = ts
    items = []
    for u in units.values():
        items.append({**u,
                      "normal_rate": round(u["normal"] / u["total"], 4) if u["total"] else None,
                      "usage_rate": round(u["online"] / u["total"], 4) if u["total"] else None})
    items.sort(key=lambda x: (-x["total"], x["unit"]))
    return items


def _asset_last_versions(snap):
    """全量设备最后记录版本（版本视图统计基准，2026-10-26 需求变更）：
    universe = 当前在线快照 ∪ MySQL 设备台账（含离线设备）；
    每台取最后一次记录的版本（在线取当前探测，离线取历史最后已知），
    从未上报过版本的记录为 None（前端归「未获取」桶）。"""
    last_known = {}
    if store.enabled:
        with store.lock:
            last_known = {str(h).strip().casefold(): (v or {}).get("version") or None
                          for h, v in (store.state or {}).items()}
    merged = {}
    for key, info in (snap or {}).items():
        merged[str(key)] = (info or {}).get("version") or None
    for key, ver in last_known.items():
        merged.setdefault(str(key), ver or None)
    return merged


def assets_versions_view():
    """版本分布（统计基准 2026-10-26 变更：设备最后一次记录的版本，含离线）。

    版本归属：当前在线取探测版本，离线取 MySQL 历史最后已知版本，
    从未上报记「未获取」桶；不再只统计当前在线快照。
    匹配规则（大小写不敏感，见 _asset_fleet_lookup）：
    - matched：在册（设备明细）且 fleet 有过任何记录（在线或历史）
    - ledger_missing：在册但从未被 fleet 记录过（真未接入；离线但有历史
      版本的设备不再误归此类）
    - detected_only：fleet 有记录但在册与归档均未登记
    归档设备不计入 ledger_missing；by_provider_top5 同基准（每台按最后
    记录版本分桶，无记录归「未获取」，share = count / 在册数）。"""
    online, version, snap = _asset_fleet_lookup()
    devices = asset_store.all_records("devices")
    archived = asset_store.all_records("archived")
    ledger = {_dev_key(d.get("device_no")): d for d in devices if d.get("device_no")}
    archived_keys = {_dev_key(d.get("device_no")) for d in archived if d.get("device_no")}

    # 每台设备最后一次记录的版本（含离线；universe = 在线快照 ∪ 历史台账）
    last_versions = _asset_last_versions(snap)
    detected_by_version = {}
    for ver in last_versions.values():
        bucket = (ver or "").strip() or "未获取"
        detected_by_version[bucket] = detected_by_version.get(bucket, 0) + 1
    ledger_by_version = {}
    ledger_no_version = 0
    for d in devices:
        ver = (d.get("software_version") or "").strip()
        if ver:
            ledger_by_version[ver] = ledger_by_version.get(ver, 0) + 1
        else:
            ledger_no_version += 1

    detected_keys = set(last_versions)
    matched = sorted(k for k in ledger if k in detected_keys)
    ledger_missing = sorted(k for k in ledger if k not in detected_keys)
    detected_only = sorted(k for k in detected_keys
                           if k not in ledger and k not in archived_keys)
    # 主要服务商版本占比（在册数前 5）：每台按最后记录版本分桶（含离线
    # 历史版本；无任何记录归「未获取」桶）；share = count / 该单位在册数
    # （保留两位小数）；排序 = 在册数降序、并列按单位名稳定排序（与 units
    # 同规则），恰取前 5（不足 5 家如实返回全部）。
    units_ver = {}
    for d in devices:
        name = d.get("unit") or "(未填写)"
        u = units_ver.setdefault(name, {"total": 0, "by_version": {}})
        u["total"] += 1
        ver = ((last_versions.get(_dev_key(d.get("device_no"))) or "").strip()
               or "未获取")
        u["by_version"][ver] = u["by_version"].get(ver, 0) + 1
    by_provider_top5 = []
    for name, u in sorted(units_ver.items(),
                           key=lambda kv: (-kv[1]["total"], kv[0]))[:5]:
        versions = [{"version": ver, "count": cnt,
                     "share": round(cnt / u["total"], 2)}
                    for ver, cnt in sorted(u["by_version"].items(),
                                           key=lambda kv: (-kv[1], kv[0]))]
        by_provider_top5.append({"unit": name, "total": u["total"],
                                 "versions": versions})
    return {
        "detected_by_version": detected_by_version,
        "detected_total": len(last_versions),
        "version_basis": "last_recorded",
        "ledger_by_version": ledger_by_version,
        "ledger_registered": ledger_no_version + sum(ledger_by_version.values()),
        "ledger_no_version": ledger_no_version,
        "by_provider_top5": by_provider_top5,
        "match": {"matched": matched, "ledger_missing": ledger_missing,
                  "detected_only": detected_only},
        "match_counts": {"matched": len(matched), "ledger_missing": len(ledger_missing),
                         "detected_only": len(detected_only)},
        "last_sync_at": asset_store.sync_state("devices")["last_ok_at"],
    }


def _asset_action_public(rec):
    return {"record_id": rec["record_id"], "action": rec.get("action"),
            "summary": rec.get("summary"), "note": rec.get("note"),
            "operator": rec.get("operator"), "transferred_to": rec.get("transferred_to"),
            "created_by": rec.get("created_by"), "operated_at": rec.get("operated_at"),
            "attachments": _asset_attach_urls(rec.get("attachments")),
            "linked_records": rec.get("linked_records"),
            "feishu_url": asset_feishu_url(ASSET_TABLES["actions"]["table_id"],
                                           rec["record_id"])}


def _asset_part_public(rec):
    return {"record_id": rec["record_id"], "item": rec.get("item"),
            "part": rec.get("part"), "device_no": rec.get("device_no"),
            "arrival_status": rec.get("arrival_status"),
            "status_note": rec.get("status_note"), "checker": rec.get("checker"),
            "checked_at": rec.get("checked_at"), "photos": _asset_attach_urls(rec.get("photos")),
            "linked_records": rec.get("linked_records"),
            "feishu_url": asset_feishu_url(ASSET_TABLES["parts"]["table_id"],
                                           rec["record_id"])}


def _asset_transfer_public(rec):
    return {"record_id": rec["record_id"], "occurred_at": rec.get("occurred_at"),
            "item": rec.get("item"), "quantity": rec.get("quantity"),
            "note": rec.get("note"), "unit": rec.get("unit"),
            "proof": _asset_attach_urls(rec.get("proof")),
            "feishu_url": asset_feishu_url(ASSET_TABLES["transfers"]["table_id"],
                                           rec["record_id"])}


def asset_detail_view(device_no):
    """单设备 360°：台账 + 维修记录（异常设备表）+ 处理流水 + 配件核对 + 流转记录。

    关联规则（大小写不敏感，_dev_key 统一）：
    - 维修记录：异常设备表 device_no 与设备编号一致（全部阶段，倒序）；
    - 处理流水：异常处理流转.device_no 一致，或 linked_records 命中该设备
      任一维修记录的 record_id（双路关联）；
    - 配件核对：配件核对明细.device_no 一致；
    - 流转记录：无设备编号列，按单位关联——设备所属单位与记录单位去空格后
      相等或互为包含（该列为自由文本，存在「构元服务商」vs「潍坊构元服务商」）。
    未匹配到的源返回空数组；台账缺失字段一律 null，不合成。
    """
    key = _dev_key(device_no)
    if not key:
        return None
    rec = next((r for r in asset_store.all_records("devices")
                if _dev_key(r.get("device_no")) == key), None)
    archived = False
    if rec is None:
        rec = next((r for r in asset_store.all_records("archived")
                    if _dev_key(r.get("device_no")) == key), None)
        archived = rec is not None
    if rec is None:
        return None
    online, version, _ = _asset_fleet_lookup()
    item = _asset_device_public(rec, online, version, _asset_active_repairs_index(),
                                table_key="archived" if archived else "devices",
                                last_seen=_fleet_last_seen())

    repairs = [r for r in repair_store.all_records() if _dev_key(r.get("device_no")) == key]
    repairs.sort(key=lambda r: r.get("created_at") or "", reverse=True)
    repair_ids = {r["record_id"] for r in repairs}
    actions = [r for r in asset_store.all_records("actions")
               if _dev_key(r.get("device_no")) == key
               or any(rid in repair_ids for rid in (r.get("linked_records") or []))]
    actions.sort(key=lambda r: r.get("operated_at") or "", reverse=True)
    parts = [r for r in asset_store.all_records("parts")
             if _dev_key(r.get("device_no")) == key]
    parts.sort(key=lambda r: r.get("checked_at") or "", reverse=True)
    unit = (rec.get("unit") or "").strip()
    transfers = []
    if unit:
        for r in asset_store.all_records("transfers"):
            tu = (r.get("unit") or "").strip()
            if tu and (tu == unit or tu in unit or unit in tu):
                transfers.append(r)
        transfers.sort(key=lambda r: r.get("occurred_at") or "", reverse=True)

    item["repairs"] = [{"record_id": r["record_id"], "stage": r.get("stage"),
                        "device_type": r.get("device_type"),
                        "problem": r.get("problem"), "owner_name": r.get("owner_name"),
                        "last_followed_at": r.get("last_followed_at"),
                        "last_followup_note": r.get("last_followup_note"),
                        "created_at": r.get("created_at"),
                        "feishu_url": repair_feishu_url(r["record_id"])} for r in repairs]
    item["actions"] = [_asset_action_public(r) for r in actions]
    item["parts_checks"] = [_asset_part_public(r) for r in parts]
    item["transfers"] = [_asset_transfer_public(r) for r in transfers]
    return item


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



def _read_web(name):
    """读取 web/ 下前端文件（bytes）；仅允许扁平文件名（防目录穿越），缺失/非法返回 None。"""
    if name != os.path.basename(name) or "/" in name or "\\" in name:
        return None
    try:
        with open(os.path.join(WEB_DIR, name), "rb") as f:
            return f.read()
    except OSError:
        return None


def _session_sign(exp):
    """会话签名 = HMAC(secret, user|exp)；与用户名绑定，改用户名即失效。"""
    return hmac.new(SESSION_SECRET.encode("utf-8"),
                    f"{PANEL_USER}|{exp}".encode("utf-8"), hashlib.sha256).hexdigest()


def _session_token():
    exp = int(time.time() + SESSION_TTL)
    return f"{exp}.{_session_sign(exp)}"


def _session_valid(cookie_header):
    """从 Cookie 头解析会话票据并校验签名与有效期（常量时间比较）。"""
    for part in (cookie_header or "").split(";"):
        name, _, value = part.strip().partition("=")
        if name != SESSION_COOKIE or "." not in value:
            continue
        exp_s, _, sig = value.partition(".")
        if not exp_s.isdigit() or not sig:
            continue
        exp = int(exp_s)
        if exp >= time.time() and hmac.compare_digest(sig, _session_sign(exp)):
            return True
    return False


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
        # 1) 登录页颁发的会话 Cookie（HttpOnly；exp 与签名一体，防篡改/防伪造）
        if _session_valid(self.headers.get("Cookie") or ""):
            return True
        # 2) HTTP Basic 保留（curl -u / 既有运维脚本零改动）
        header = self.headers.get("Authorization") or ""
        if not header.startswith("Basic "):
            return False
        try:
            supplied = base64.b64decode(header[6:].strip()).decode("utf-8")
        except Exception:
            return False
        expected = f"{PANEL_USER}:{PANEL_PASSWORD}"
        return hmac.compare_digest(supplied.encode("utf-8"), expected.encode("utf-8"))

    def _auth_failed(self, path):
        """未认证：页面 302 跳登录页；接口 401 JSON（不带 WWW-Authenticate，
        防浏览器对 SPA 的 fetch 弹原生 Basic 框；curl -u 不受影响）。"""
        if path.startswith("/api/") or path.startswith("/attach/"):
            return self.send_json(401, {"error": "unauthorized"})
        self._redirect("/login")

    def _redirect(self, loc):
        self.send_response(302)
        self.send_header("Location", loc)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _serve_login(self):
        if not (PANEL_USER and PANEL_PASSWORD):
            return self._redirect("/")  # 未启用认证：无登录页概念
        page = _read_web("login.html")
        if page is None:
            return self.send_error(500, "web/login.html missing")
        err_hidden = "" if "err" in parse_qs(urlsplit(self.path).query) else " hidden"
        body = (page.decode("utf-8")
                .replace("{{TITLE}}", html.escape(panel_title(), quote=False))
                .replace("{{ERR_HIDDEN}}", err_hidden)
                .encode("utf-8"))
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def _serve_web_file(self, name, immutable=False):
        # web/ 内静态文件（支持 assets/ 一级子目录，Vite 构建产物）；realpath 防目录穿越。
        # immutable=True 用于带哈希文件名的构建产物，长缓存。
        base = os.path.realpath(WEB_DIR)
        fp = os.path.realpath(os.path.join(WEB_DIR, name))
        if fp != base and not fp.startswith(base + os.sep):
            return self.send_error(404)
        try:
            with open(fp, "rb") as f:
                data = f.read()
        except OSError:
            return self.send_error(404)
        self.send_response(200)
        self.send_header("Content-Type",
                         mimetypes.guess_type(name)[0] or "application/octet-stream")
        self.send_header("Cache-Control",
                         "public, max-age=31536000, immutable" if immutable else "no-cache")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _serve_page(self, name):
        """下发 web/ 下页面文件并注入运行时标题（Vue 入口 / classic 兜底 / 登录页）。"""
        page = _read_web(name)
        if page is None:
            return self.send_error(500, f"web/{name} missing")
        body = (page.decode("utf-8")
                .replace("{{TITLE}}", html.escape(panel_title(), quote=False))
                .encode("utf-8"))
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _login_submit(self):
        if not (PANEL_USER and PANEL_PASSWORD):
            return self._redirect("/")
        n = int(self.headers.get("Content-Length") or 0)
        form = parse_qs(self.rfile.read(n).decode("utf-8", "replace") if n else "")
        user = (form.get("username") or [""])[0].strip()
        password = (form.get("password") or [""])[0]
        ok = (hmac.compare_digest(user.encode("utf-8"), PANEL_USER.encode("utf-8"))
              and hmac.compare_digest(password.encode("utf-8"), PANEL_PASSWORD.encode("utf-8")))
        if not ok:
            time.sleep(0.4)  # 轻微限速，挫败在线爆破
            return self._redirect("/login?err=1")
        self.send_response(303)
        self.send_header("Set-Cookie",
                         f"{SESSION_COOKIE}={_session_token()}; Path=/; HttpOnly; "
                         f"SameSite=Lax; Max-Age={int(SESSION_TTL)}")
        self.send_header("Location", "/")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _logout(self):
        self.send_response(303)
        self.send_header("Set-Cookie",
                         f"{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0")
        self.send_header("Location", "/login")
        self.send_header("Content-Length", "0")
        self.end_headers()

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

    def _repair_attachment(self, token, name=None):
        """提供本地已下载的维修附件（token 定位文件，URL 已限制字符集，防目录穿越）。"""
        if not token or "/" in token or "\\" in token or token != os.path.basename(token):
            return self.send_error(404)
        filename = _repair_attach_file(token)
        if filename is None or os.path.splitext(filename)[0] != token:
            return self.send_error(404)
        fp = os.path.join(REPAIR_ATTACH_DIR, filename)
        # Content-Type 从 URL 里的原始文件名推断（本地文件名是裸 token[.ext]）
        ctype = mimetypes.guess_type(name or "")[0] if name else None
        ctype = ctype or mimetypes.guess_type(filename)[0] or "application/octet-stream"
        try:
            with open(fp, "rb") as f:
                body = f.read()
        except OSError:
            return self.send_error(404)
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "public, max-age=86400")  # token 内容寻址，可缓存
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlsplit(self.path).path
        # 公共路由（无需认证）：登录页与其样式、登出
        if path == "/login":
            return self._serve_login()
        if path == "/login.css":
            return self._serve_web_file("login.css")
        if path == "/app.css":
            return self._serve_web_file("app.css")
        if path == "/app.js":
            return self._serve_web_file("app.js")
        if path.startswith("/assets/"):
            # Vite 构建产物（web/assets/ 下带内容哈希，长缓存）
            return self._serve_web_file(path.lstrip("/"), immutable=True)
        if path == "/logout":
            return self._logout()
        if not self._authorized():
            return self._auth_failed(path)
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
        # --- 维修面板只读 API（KVWO-78/79，数据源飞书维修表 lark-cli 同步） ---
        if path == "/api/repairs":
            qs = parse_qs(urlsplit(self.path).query)
            filters = {k: (qs.get(k, [""])[0] or "") for k in ("stage", "type", "unit", "owner", "q")}
            items = repairs_list_view(filters)
            state = repair_store.sync_state()
            return self.send_json(200, {
                "enabled": REPAIR_SYNC_ENABLED, "total": len(items), "items": items,
                "last_sync_at": state["last_ok_at"],
                "last_sync_attempt_at": state["last_sync_at"],
                "last_sync_error": state["last_sync_error"],
                "sync_interval": int(REPAIR_SYNC_INTERVAL),
                "stage_order": REPAIR_STAGE_ORDER})
        if path == "/api/repairs/stats":
            return self.send_json(200, repairs_stats_view())
        m = re.match(r"^/api/repairs/([A-Za-z0-9]+)$", path)
        if m:
            rec = repair_store.get(m.group(1))
            if rec is None:
                return self.send_json(404, {"error": "unknown record", "record_id": m.group(1)})
            return self.send_json(200, _repair_public(rec, _repair_online_lookup(),
                                                      _repair_attach_index()))
        m = re.match(r"^/repair-attachments/([A-Za-z0-9_-]+)(?:/(.*))?$", path)
        if m:
            return self._repair_attachment(m.group(1), m.group(2))
        # --- 资产多表与聚合 API（KVWO-83，只读，数据源飞书资产表 lark-cli 同步） ---
        if path == "/api/assets":
            qs = parse_qs(urlsplit(self.path).query)
            filters = {k: (qs.get(k, [""])[0] or "")
                       for k in ("status", "type", "unit", "q", "include_archived")}
            items = assets_list_view(filters)
            state = asset_store.sync_state("devices")
            return self.send_json(200, {
                "enabled": ASSET_SYNC_ENABLED, "total": len(items), "items": items,
                "last_sync_at": state["last_ok_at"],
                "last_sync_attempt_at": state["last_sync_at"],
                "last_sync_error": state["last_sync_error"],
                "sync_interval": int(ASSET_SYNC_INTERVAL),
                "tables": asset_store.sync_states()})
        if path == "/api/assets/stats":
            return self.send_json(200, assets_stats_view())
        if path == "/api/assets/units":
            return self.send_json(200, {"units": assets_units_view(),
                                       "last_sync_at": asset_store.sync_state("devices")["last_ok_at"]})
        if path == "/api/assets/versions":
            return self.send_json(200, assets_versions_view())
        if path == "/api/assets/report":
            # 资产日报推送状态（KVWO-97）；webhook 地址属敏感凭据，不回显
            with _FEISHU_REPORT_LOCK:
                out = {"enabled": bool(FEISHU_WEBHOOK),
                       "times": [f"{hh:02d}:{mm:02d}" for hh, mm in REPORT_SLOTS],
                       "next_run_at": _feishu_report_state["next_run_at"],
                       "last": _feishu_report_state["last"],
                       "inventory_units": list(INVENTORY_UNITS),
                       "panel_title": panel_title()}
            return self.send_json(200, out)
        m = re.match(r"^/api/assets/([A-Za-z0-9_-]+)$", path)
        if m:
            detail = asset_detail_view(m.group(1))
            if detail is None:
                return self.send_json(404, {"error": "unknown device", "device_id": m.group(1)})
            return self.send_json(200, detail)
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
        if path == "/classic":
            # 原 vanilla 版面板（迁移兑底，稳定后可下线）
            return self._serve_page("classic.html")
        if path == "/api/panel-title":
            return self.send_json(200, {"title": panel_title(),
                                        "source": _panel_title.get("source", "")})
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
            # Vue 3 构建入口（frontend/ 工程产物，部署于 web/index.html + web/assets/）
            return self._serve_page("index.html")
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
        if path == "/login":
            return self._login_submit()
        if path == "/logout":
            return self._logout()
        if not self._authorized():
            return self._auth_failed(path)
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
        if path == "/api/repairs/sync":
            # 手动触发一次维修表同步（自测/验证用；与定时同步互斥串行）
            result = run_repair_sync("manual")
            return self.send_json(200, result)
        if path == "/api/assets/sync":
            # 手动触发一次资产 5 表同步（自测/验证用；与定时同步互斥串行，
            # 按表隔离失败）
            result = run_asset_sync("manual")
            return self.send_json(200, result)
        if path == "/api/assets/report/run":
            # 手动触发一次资产日报推送（测试验收用；与定时调度互斥串行）。
            # 未配置 webhook 时如实拒绝（置空=关闭推送）
            if not FEISHU_WEBHOOK:
                return self.send_json(400, {"ok": False,
                                            "error": "FRP_MONITOR_FEISHU_WEBHOOK 未配置（置空=关闭推送）"})
            result = run_feishu_report("manual")
            return self.send_json(200, result)
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
    threading.Thread(target=_repair_sync_loop, name="repair-syncer", daemon=True).start()
    threading.Thread(target=_asset_sync_loop, name="asset-syncer", daemon=True).start()
    threading.Thread(target=_feishu_report_loop, name="feishu-report", daemon=True).start()
    threading.Thread(target=_resolve_panel_title, name="panel-title-resolver", daemon=True).start()
    print(f"frp-monitor: serving on :{HTTP_PORT} (oss-signed-url distribution, "
          f"upgrade_parallel={UPGRADE_MAX_PARALLEL}, retention={RETENTION_DAYS}d, "
          f"backup={BACKUP_DIR} keep {BACKUP_KEEP}, "
          f"webfix={'auto' if (WEBFIX_AUTO and store.enabled and DEPLOY_PW) else 'off'} "
          f"ports {WEBFIX_PORT_BASE}-{WEBFIX_PORT_MAX}, stable-window {WEBFIX_DELAY:g}s, "
          f"repairs={'on' if REPAIR_SYNC_ENABLED else 'off'}"
          f"{'/' + str(int(REPAIR_SYNC_INTERVAL)) + 's' if REPAIR_SYNC_ENABLED else ''}, "
          f"report={'on@' + '/'.join(f'{hh:02d}:{mm:02d}' for hh, mm in REPORT_SLOTS) if FEISHU_WEBHOOK else 'off'}, "
          f"inventory_units={'+'.join(INVENTORY_UNITS)}, "
          f"title={panel_title()!r})",
          flush=True)
    httpd = ThreadingHTTPServer(("0.0.0.0", HTTP_PORT), Handler)
    httpd.daemon_threads = True  # SSE 长连接随主进程退出，不阻塞重启
    httpd.serve_forever()
