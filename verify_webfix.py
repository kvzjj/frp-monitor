# KVWO-19/44 自测：mock frps 快照 + 本地 MySQL（db/docker-compose.yml）起服务，验证
#  1) 检测判据：在线 + 有 ssh 代理 + 缺 web 代理 → 候选；web 代理已存在但
#     version_error（探测失败）不触发
#  2) 自动触发严格一次（auto 台账行唯一；失败后刷新循环不再自动补配）
#  3) 假设备失败路径：SSH 不可达 → 任务 failed 且 error 可读（不回滚、留现场）
#  4) API 骨架：GET /api/webfix/tasks（状态/candidates/counts）、
#     POST /api/webfix/retry（面板认证；attempts 累加、trigger_type=manual、
#     已有 web 代理/未知设备给出跳过原因）、GET /api/webfix/tasks/<id>/log
#  5) 闭环校验：/api/setup/status 比对 device_id（不一致立即失败且阻断类，一致成功）
#  6) KVWO-44 上线事件 + 稳定窗口：首观测起算、满窗才触发、窗口内掉线重置重计、
#     FRP_MONITOR_WEBFIX_DELAY 生效；version_error 仍不触发、一次为限不变
#  7) KVWO-44 阻断三态：机型未知 → auto 落 blocked 台账（占一次性名额、细分码
#     error_code 可区分）；修复登记表后手动重试走普通 failed 路径
# 用法：先 `docker compose up -d`（db/），再 `python3 verify_webfix.py`
import base64
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

os.environ.setdefault("FRPS_API", "http://127.0.0.1:19500/api/proxy/tcp")
os.environ.setdefault("FRP_MONITOR_DB_PORT", "3307")
os.environ.setdefault("FRP_MONITOR_DB_PASSWORD", "1f251745d9222190b9164b31d85295da")
os.environ.setdefault("DEPLOY_PW", "dummy-pw-for-test")
os.environ.setdefault("HTTP_PORT", "19102")
os.environ.setdefault("FRP_MONITOR_REFRESH_INTERVAL", "2")
os.environ.setdefault("FRP_MONITOR_WEBFIX_VERIFY_TIMEOUT", "6")
os.environ.setdefault("FRP_MONITOR_WEBFIX_DELAY", "4")   # KVWO-44：满窗 4s，缩短自测时长
os.environ.setdefault("FRP_MONITOR_PANEL_USER", "admin")
os.environ.setdefault("FRP_MONITOR_PANEL_PASSWORD", "secret")
os.environ["FRP_MONITOR_SSH_KNOWN_HOSTS"] = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                        ".verify-known-hosts")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import server  # noqa: E402  (env 必须在 import 前就位)

# ---- mock frps：ep-000777 只有 ssh 代理（候选）；ep-000888 两条齐全；
#      ep-000999 有 web 代理但本机探测必失败（version_error ≠ 缺配置，不触发）----
FRPS_PROXIES = {
    "proxies": [
        {"name": "ep-000777", "status": "online", "conf": {"remotePort": 20777},
         "curConns": 1, "lastStartTime": "10-16 10:00:00"},
        {"name": "ep-000888", "status": "online", "conf": {"remotePort": 20888},
         "curConns": 1, "lastStartTime": "10-16 10:00:00"},
        {"name": "ep-000888-web", "status": "online", "conf": {"remotePort": 30888},
         "curConns": 0, "lastStartTime": "10-16 10:00:00"},
        {"name": "ep-000999", "status": "online", "conf": {"remotePort": 20999},
         "curConns": 1, "lastStartTime": "10-16 10:00:00"},
        {"name": "ep-000999-web", "status": "online", "conf": {"remotePort": 30999},
         "curConns": 0, "lastStartTime": "10-16 10:00:00"},
    ]
}

# ---- mock 设备门户（闭环校验用）：device_id 可切换 ----
MOCK_DEVICE = {"device_id": "999", "release_version": "v0.4.6"}


class FrpsHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        body = json.dumps(FRPS_PROXIES).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)


class DeviceHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        if self.path == "/api/setup/status":
            body = json.dumps(MOCK_DEVICE).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_error(404)


frpsd = ThreadingHTTPServer(("127.0.0.1", 19500), FrpsHandler)
frpsd.daemon_threads = True
threading.Thread(target=frpsd.serve_forever, daemon=True).start()
devd = ThreadingHTTPServer(("127.0.0.1", 30777), DeviceHandler)  # ep-000777 的目标 web 端口
devd.daemon_threads = True
threading.Thread(target=devd.serve_forever, daemon=True).start()

fails = []


def check(name, got, want):
    ok = got == want
    print(f"{'PASS' if ok else 'FAIL'}  {name}: got={got!r} want={want!r}")
    if not ok:
        fails.append(name)


def api(path, method="GET", body=None, auth=True):
    req = urllib.request.Request(f"http://127.0.0.1:19102{path}", method=method)
    if auth:
        token = base64.b64encode(b"admin:secret").decode()
        req.add_header("Authorization", f"Basic {token}")
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, data=data, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode() or "{}")


# ---- 环境就绪：清台账 + 登记表预置机型（roster 在 import 时已加载，需手动刷新）----
assert server.store.enabled, "需要本地 MySQL（db/docker-compose.yml up -d）"
server.store._execute("DELETE FROM webfix_tasks")
server.store._execute(
    "DELETE FROM devices WHERE hostname IN "
    "('ep-000777','ep-000888','ep-000999','ep-000555')")
server.store._execute(
    "INSERT INTO devices (hostname, hardware, site) VALUES "
    "('ep-000777','V1','测试站'),('ep-000888','V0','测试站'),('ep-000999','V0','测试站')")
server.store._load_roster()

httpd = ThreadingHTTPServer(("127.0.0.1", 19102), server.Handler)
httpd.daemon_threads = True
threading.Thread(target=httpd.serve_forever, daemon=True).start()

# ---- 0b) 稳定窗口状态机（KVWO-44 纯函数；须在刷新线程启动前驱动，避免串扰）----
check("FRP_MONITOR_WEBFIX_DELAY 生效（env=4 → 常量）", server.WEBFIX_DELAY, 4.0)
with server._webfix_windows_lock:
    server._webfix_windows.clear()
check("窗口：首观测启动计时，未满不触发",
      server.webfix_advance_windows(["ep-000100"], now=1000.0), [])
check("窗口：存量设备首观测起算（<4s 不触发）",
      server.webfix_advance_windows(["ep-000100"], now=1003.9), [])
check("窗口：连续在线满 4s → 触发",
      server.webfix_advance_windows(["ep-000100"], now=1004.0), ["ep-000100"])
server.webfix_advance_windows(["ep-000100"], now=1004.5)   # 仍在线，跟踪条目保留
server.webfix_advance_windows([], now=1005.0)              # 窗口内掉线 → 计时作废
check("窗口：掉线后重计（重新上线 2s 不足 4s 不触发）",
      server.webfix_advance_windows(["ep-000100"], now=1007.0), [])
check("窗口：重计后再满窗 → 触发",
      server.webfix_advance_windows(["ep-000100"], now=1011.0), ["ep-000100"])
with server._webfix_windows_lock:
    server._webfix_windows.clear()

# ---- 0c) 阻断/失败细分码分类（KVWO-44 纯函数）----
check("错误码提取：同名代理端口冲突",
      server._webfix_error_code("x\nERROR_PROXY_CONFLICT: 已存在同名代理"), "proxy_conflict")
check("错误码提取：配置缺失",
      server._webfix_error_code("ERROR_NO_CONF: /etc/frp/frpc.toml 不存在"), "no_conf")
check("错误码提取：无 ERROR 标记为空", server._webfix_error_code("rc=1\nNo route"), "")
check("阻断分类：端口冲突/配置缺失/机型未知/错配属阻断",
      {"proxy_conflict", "no_conf", "arch_unknown", "device_id_mismatch"}
      <= server.WEBFIX_BLOCKED_CODES, True)
check("阻断分类：busy/verify_timeout 不属阻断",
      {"busy", "verify_timeout"} & server.WEBFIX_BLOCKED_CODES, set())

threading.Thread(target=server._refresh_loop, name="snapshot-refresher", daemon=True).start()
time.sleep(0.3)

# ---- 0) 纯函数：检测判据 / 端口推导 / device_id 比对 ----
check("候选判据：无 ssh 代理不算",
      server.webfix_candidates([{"hostname": "a", "port": None, "web_port": None}]), [])
check("候选判据：有 web 代理不算（即使 version_error）",
      server.webfix_candidates([{"hostname": "a", "port": 1, "web_port": 2,
                                 "version_error": "boom"}]), [])
check("候选判据：在线 + ssh + 缺 web → 候选",
      server.webfix_candidates([{"hostname": "a", "port": 1, "web_port": None}]), ["a"])
check("端口推导 ep-000160 → 30160", server.webfix_port_for("ep-000160"), 30160)
check("端口推导 非约定设备名 → None", server.webfix_port_for("random-host"), None)
check("device_id 数字按值比", server._webfix_id_matches("0160", "160"), True)
check("device_id 字面比", server._webfix_id_matches("EP-160", "ep-160"), True)
check("device_id 不匹配", server._webfix_id_matches("999", "160"), False)

# ---- 1) 刷新循环自动检测（上线事件 + 满窗后触发；假设备 SSH 不可达 → 失败路径）----
# 满窗前不触发：delay=4s、刷新 2s —— 首轮快照锚定窗口后，2.2s 时必未满窗
# （面板启动时已在线的存量设备 ep-000777 从首次观测起算同样窗口）
time.sleep(2.2)
check("稳定窗口：满窗前不触发（首观测 2.2s < 4s）",
      api("/api/webfix/tasks")[1]["tasks"], [])
deadline = time.time() + 90
tasks = []
while time.time() < deadline:
    status, data = api("/api/webfix/tasks")
    tasks = data.get("tasks") or []
    if tasks and tasks[0]["status"] in ("success", "failed"):
        break
    time.sleep(2)
check("自动触发：生成台账任务", len(tasks) >= 1, True)
t = tasks[0]
check("自动触发：目标设备", t["hostname"], "ep-000777")
check("自动触发：trigger_type=auto", t["trigger_type"], "auto")
check("自动触发：失败路径收敛为 failed", t["status"], "failed")
check("自动触发：error 可读（SSH 不可达）", "20777" in t["error"] or "ssh" in t["error"].lower(), True)
check("自动触发：环境性失败不落阻断（error_code 空）", t.get("error_code"), "")
check("自动触发：attempts=1", t["attempts"], 1)
check("candidates：仅缺 web 代理的在线设备",
      [c["hostname"] for c in api("/api/webfix/tasks")[1]["candidates"]], ["ep-000777"])
check("candidates：ep-000999 有 web 代理但 version_error，不触发",
      any(c["hostname"] == "ep-000999" for c in api("/api/webfix/tasks")[1]["candidates"]), False)
check("API 回显稳定窗口时长（KVWO-44 增补字段 delay）",
      api("/api/webfix/tasks")[1].get("delay"), 4.0)

# ---- 2) 自动触发严格一次：再跑几轮刷新，不新增 auto 台账行 ----
time.sleep(7)
rows = server.store._execute(
    "SELECT trigger_type, COUNT(*) FROM webfix_tasks WHERE hostname='ep-000777' "
    "GROUP BY trigger_type", fetch="all")
check("一次性语义：auto 记录唯一", dict((r[0], r[1]) for r in rows).get("auto"), 1)

# ---- 3) 手动重试 API（面板认证）----
status, _ = api("/api/webfix/retry", method="POST", body={"devices": ["ep-000777"]}, auth=False)
check("重试需面板认证", status, 401)
status, data = api("/api/webfix/retry", method="POST",
                   body={"devices": ["ep-000777", "ep-000888", "ep-000077", "ep-099999"]})
check("重试：受理", (status, data.get("ok")), (200, True))
check("重试：ep-000777 启动", [s["hostname"] for s in data["started"]], ["ep-000777"])
check("重试：ep-000777 attempts=2", data["started"][0]["attempts"], 2)
skipped = {s["hostname"]: s["reason"] for s in data["skipped"]}
check("重试：ep-000888 已有 web 代理被跳过", "web 代理已存在" in skipped.get("ep-000888", ""), True)
check("重试：未知设备被跳过", "未知设备" in skipped.get("ep-000077", ""), True)
check("重试：编号超出端口段被跳过", "超出网关预放宽段" in skipped.get("ep-099999", ""), True)
rows = server.store._execute(
    "SELECT trigger_type, attempts, status FROM webfix_tasks WHERE hostname='ep-000777' "
    "ORDER BY id DESC LIMIT 1", fetch="one")
check("重试台账：trigger_type=manual 且 attempts=2", (rows[0], rows[1]), ("manual", 2))
check("重试台账：auto 行仍在（一次性语义不受影响）",
      server.store.webfix_has_auto("ep-000777"), True)

# ---- 4) 日志端点（内存态 + DB 回看）----
task_id = tasks[0]["task_id"]
status, data = api(f"/api/webfix/tasks/{task_id}/log")
check("日志端点：可读", (status, data.get("ok")), (200, True))
check("日志端点：内容含面板侧记录", "[panel]" in data.get("log", ""), True)
status, _ = api("/api/webfix/tasks/999999/log")
check("日志端点：未知任务 404", status, 404)

# ---- 5) 闭环校验：device_id 不一致立即失败且阻断类；一致成功 ----
probe_task = server.WebfixTask(990001, "ep-000777", "manual", 3, 20777, "orangepi",
                               30777, "777")
MOCK_DEVICE["device_id"] = "999"
ok, err, code = server._webfix_verify(probe_task)
check("闭环校验：device_id 不一致 → 失败", ok, False)
check("闭环校验：错误提示指向端口错配", "不一致" in err and "人工" in err, True)
check("闭环校验：错配细分码 device_id_mismatch（阻断类）",
      (code, code in server.WEBFIX_BLOCKED_CODES), ("device_id_mismatch", True))
MOCK_DEVICE["device_id"] = "777"
ok, err, code = server._webfix_verify(probe_task)
check("闭环校验：device_id 一致 → 成功", (ok, err, code), (True, "", ""))
MOCK_DEVICE["release_version"] = ""
ok, err, code = server._webfix_verify(probe_task)
MOCK_DEVICE["release_version"] = "v0.4.6"
check("闭环校验：release_version 为空 → 超时失败", ok, False)
check("闭环校验：超时属普通失败（verify_timeout 非阻断）",
      (code, code in server.WEBFIX_BLOCKED_CODES), ("verify_timeout", False))

# ---- 6) 升级互斥：webfix 进行中的设备 device_busy（先等手动重试任务收口）----
deadline = time.time() + 90
while time.time() < deadline and server.webfix_active("ep-000777") is not None:
    time.sleep(2)
check("手动重试任务已收口（假设备失败路径）",
      server.webfix_active("ep-000777") is None, True)
probe_task = server.WebfixTask(990001, "ep-000777", "manual", 3, 20777, "orangepi",
                               30777, "777")
probe_task.status = "running"
with server.WEBFIX_LOCK:
    server.WEBFIX[990001] = probe_task
check("并发护栏：webfix 进行中 → device_busy", server.device_busy("ep-000777"), "webfix#990001")
check("并发护栏：重试对进行中设备拒单",
      "进行中" in api("/api/webfix/retry", method="POST",
                     body={"devices": ["ep-000777"]})[1]["skipped"][0]["reason"], True)
with server.WEBFIX_LOCK:
    server.WEBFIX.pop(990001, None)

# ---- 7) 设备端脚本（stub sudo/systemctl/frpc）：幂等 / 冲突 / 追加正确性 ----
import shutil  # noqa: E402
import subprocess  # noqa: E402
import tempfile  # noqa: E402


BASE_FRPC = ('serverAddr = "1.2.3.4"\nserverPort = 7000\n\n'
             '[[proxies]]\nname = "ep-000777"\ntype = "tcp"\n'
             'localPort = 22\nremotePort = 20777\n')


def run_device_script(conf_text, hostname="ep-000777", port="30777"):
    """在 stub 环境（假 sudo/systemctl/frpc）里跑 SCRIPT_WEBFIX，返回 (rc, 输出, 配置)。"""
    box = tempfile.mkdtemp(prefix="webfix-script-")
    conf = os.path.join(box, "frpc.toml")
    with open(conf, "w", encoding="utf-8") as fh:
        fh.write(conf_text)
    binp = os.path.join(box, "bin")
    os.makedirs(binp)
    restart_marker = os.path.join(box, "restarted")
    with open(os.path.join(binp, "sudo"), "w", encoding="utf-8") as fh:  # 剥掉 -S/-p 选项、吃掉 stdin 密码行后直接执行
        fh.write("#!/bin/sh\nwhile [ \"$1\" = -S ] || [ \"$1\" = -p ]; do\n"
                 "  [ \"$1\" = -p ] && shift\n  shift\ndone\nread _pw\nexec \"$@\"\n")
    with open(os.path.join(binp, "systemctl"), "w", encoding="utf-8") as fh:
        fh.write(f"#!/bin/sh\n[ \"$1\" = restart ] && touch {restart_marker}\n"
                 "echo active\n")
    with open(os.path.join(binp, "frpc"), "w", encoding="utf-8") as fh:
        fh.write("#!/bin/sh\n[ \"$1\" = verify ] && [ -f \"$3\" ] && { echo 'syntax check pass'; exit 0; }\n"
                 "exit 1\n")
    for f in ("sudo", "systemctl", "frpc"):
        os.chmod(os.path.join(binp, f), 0o755)
    script = server.SCRIPT_WEBFIX.replace("/etc/frp/frpc.toml", conf)
    env = dict(os.environ, PATH=f"{binp}:{os.environ['PATH']}",
               DEPLOY_PW="pw", WEBFIX_HOSTNAME=hostname, WEBFIX_PORT=port)
    proc = subprocess.run(["bash", "-c", script], env=env, capture_output=True,
                          text=True, timeout=60)
    with open(conf, encoding="utf-8") as fh:
        after = fh.read()
    backups = sorted(f for f in os.listdir(box) if f.startswith("frpc.toml.bak-webfix-"))
    shutil.rmtree(box, ignore_errors=True)
    return proc.returncode, proc.stdout + proc.stderr, after, backups, restart_marker


rc, out, after, backups, _ = run_device_script(BASE_FRPC)
check("脚本：首次补配 WEBFIX_APPLIED", "WEBFIX_APPLIED" in out, True)
check("脚本：原内容保留", after.startswith(BASE_FRPC.rstrip("\n")), True)
check("脚本：追加块符合 fleet 约定",
      after.endswith('\n[[proxies]]\nname = "ep-000777-web"\ntype = "tcp"\n'
                     'localIP = "127.0.0.1"\nlocalPort = 80\nremotePort = 30777\n'), True)
check("脚本：补配前有备份", len(backups), 1)

rc, out, after, backups, _ = run_device_script(after)
check("脚本：重复执行幂等（WEBFIX_ALREADY）", "WEBFIX_ALREADY" in out, True)
check("脚本：幂等执行不新增备份", len(backups), 0)
check("脚本：幂等执行不重复追加", after.count('name = "ep-000777-web"'), 1)

conflict = BASE_FRPC + ('\n[[proxies]]\nname = "ep-000777-web"\ntype = "tcp"\n'
                        'localPort = 80\nremotePort = 39999\n')
rc, out, after, backups, _ = run_device_script(conflict)
check("脚本：同名代理端口不符 → 冲突报错", "ERROR_PROXY_CONFLICT" in out, True)
check("脚本：冲突时不覆盖原配置", after, conflict)

no_newline = BASE_FRPC.rstrip("\n")  # 无结尾换行，追加块不得粘连上一行
rc, out, after, backups, _ = run_device_script(no_newline)
check("脚本：无结尾换行时补行后追加",
      "\n[[proxies]]\nname = \"ep-000777-web\"" in after, True)

rc, out, after, backups, _ = run_device_script(BASE_FRPC, hostname="bad host!")
check("脚本：非法设备名拒绝", "ERROR_BAD_HOSTNAME" in out, True)

# ---- 8) 阻断三态（KVWO-44）：机型未知 → auto 落 blocked 台账（占一次性名额）；
#      修复登记表后手动重试走普通 failed 路径，与 blocked 行可区分 ----
FRPS_PROXIES["proxies"].append(
    {"name": "ep-000555", "status": "online", "conf": {"remotePort": 20555},
     "curConns": 1, "lastStartTime": "10-16 10:00:00"})
server.store._execute(
    "INSERT INTO devices (hostname, hardware, site) VALUES ('ep-000555','','阻断测试站')")
server.store._load_roster()
deadline = time.time() + 30          # 稳定窗口 4s + 刷新 2s → ~8s 内 auto 触发
row = None
while time.time() < deadline:
    row = server.store._execute(
        "SELECT status, error_code, trigger_type, error FROM webfix_tasks "
        "WHERE hostname='ep-000555'", fetch="one")
    if row:
        break
    time.sleep(1)
check("阻断：机型未知 auto 落 blocked 台账", row[:3], ("blocked", "arch_unknown", "auto"))
check("阻断：原因指向设备登记表", "机型" in (row[3] or ""), True)
view = {t["hostname"]: t for t in api("/api/webfix/tasks")[1]["tasks"]}
check("阻断：API 与普通失败可区分（status + error_code）",
      (view.get("ep-000555", {}).get("status"), view.get("ep-000555", {}).get("error_code")),
      ("blocked", "arch_unknown"))
check("阻断：counts 含 blocked 计数",
      api("/api/webfix/tasks")[1]["counts"].get("blocked", 0) >= 1, True)
time.sleep(6)                        # 再跑几轮刷新：占一次性名额，不再重复 auto 触发
rows_n = server.store._execute(
    "SELECT COUNT(*) FROM webfix_tasks WHERE hostname='ep-000555'", fetch="one")
check("阻断：占一次性名额（不重复自动触发）", rows_n[0], 1)
# 人工补齐登记表 hardware 后：手动重试走普通 failed 路径（SSH 不可达），
# 与首行 blocked 同台账共存——「成功/失败/阻断」三态落库口径可区分
server.store._execute("UPDATE devices SET hardware='V1' WHERE hostname='ep-000555'")
server.store._load_roster()
status, data = api("/api/webfix/retry", method="POST", body={"devices": ["ep-000555"]})
check("阻断重试：修复登记表后手动重试受理",
      (status, [s["hostname"] for s in data.get("started", [])]), (200, ["ep-000555"]))
check("阻断重试：attempts 累加", data["started"][0]["attempts"], 2)
deadline = time.time() + 60
while time.time() < deadline and server.webfix_active("ep-000555") is not None:
    time.sleep(2)
rows_n = server.store._execute(
    "SELECT trigger_type, status, error_code FROM webfix_tasks "
    "WHERE hostname='ep-000555' ORDER BY id", fetch="all")
check("阻断重试：最新行 manual/failed（环境性失败，error_code 空）",
      tuple(rows_n[-1]), ("manual", "failed", ""))
check("阻断重试：blocked 行保留（三态同台账可回溯）", len(rows_n), 2)

frpsd.shutdown()
devd.shutdown()
httpd.shutdown()
os.remove(os.environ["FRP_MONITOR_SSH_KNOWN_HOSTS"]) if os.path.exists(
    os.environ["FRP_MONITOR_SSH_KNOWN_HOSTS"]) else None
print()
print("FAILED:", fails if fails else "none — all passed")
sys.exit(1 if fails else 0)
