# KVWO-21 自测：webfix 补配全链路（DB 关闭，内存态）。
# 1) SCRIPT_WEBFIX 设备端脚本：沙箱（stub sudo/frpc/systemctl）验证幂等追加 /
#    verify 失败还原 / 同名端口冲突 / 已存在跳过追加；
# 2) 引擎：检测防抖 → 一次性自动触发 → device_id 比对成功/失败 → 手动重试；
# 3) API：GET /api/webfix/status、POST /api/webfix/retry 契约。
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import server

SNAP = [
    # ep-000160：在线、有 SSH 隧道、缺 web 代理（V1 机型走登记表 hardware）
    {"hostname": "ep-000160", "version": "", "port": 6100, "web_port": None,
     "connections": 1, "started": "", "version_error": "", "device": None},
    # ep-000170：同样缺 web 代理（用于失败→手动重试链路）
    {"hostname": "ep-000170", "version": "", "port": 6101, "web_port": None,
     "connections": 1, "started": "", "version_error": "", "device": None},
    # ep-000214：已有 web 代理，绝不能被触发
    {"hostname": "ep-000214", "version": "v0.4.6", "port": 6200, "web_port": 8080,
     "connections": 1, "started": "", "version_error": "boom-transient", "device": None},
    # ep-000180：缺 web 代理但机型未知（不在 V0/V1 名单、登记表无 hardware）→ 阻断
    {"hostname": "ep-000180", "version": "", "port": 6102, "web_port": None,
     "connections": 1, "started": "", "version_error": "", "device": None},
]
server.get_snapshot = lambda: (SNAP, time.time())
server.store.roster = {
    "ep-000160": {"site": "", "owner": "", "hardware": "V1", "retired": False,
                  "hw_model": "", "ip": "", "ssh_port": 6100},
    "ep-000170": {"site": "", "owner": "", "hardware": "V0", "retired": False,
                  "hw_model": "", "ip": "", "ssh_port": 6101},
    "ep-000180": {"site": "", "owner": "", "hardware": "", "retired": False,
                  "hw_model": "", "ip": "", "ssh_port": 6102},
}
server.DEPLOY_PW = "test-pw"

fails = []


def check(name, got, want):
    ok = got == want
    print(f"{'PASS' if ok else 'FAIL'}  {name}: got={got!r} want={want!r}")
    if not ok:
        fails.append(name)


def wait_status(hostname, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        with server._WEBFIX_LOCK:
            row = dict(server._WEBFIX_STATE.get(hostname) or {})
        if row and row.get("status") not in ("running", None) and row.get("finished_at"):
            return row
        time.sleep(0.2)
    with server._WEBFIX_LOCK:
        return dict(server._WEBFIX_STATE.get(hostname) or {})


# ---------------------------------------------------------------------------
# Part 1: SCRIPT_WEBFIX 设备端脚本（沙箱：stub sudo / frpc / systemctl）
# ---------------------------------------------------------------------------
STUB_SUDO = """#!/usr/bin/env bash
args=(); haspw=0; onlyv=0
while [ $# -gt 0 ]; do
  case "$1" in
    -S) haspw=1 ;;
    -p) shift ;;
    -v) onlyv=1 ;;
    -n) ;;
    *) args+=("$1") ;;
  esac
  shift
done
[ $haspw = 1 ] && read -r _ || true
[ $onlyv = 1 ] && exit 0
exec "${args[@]}"
"""
STUB_FRPC = """#!/usr/bin/env bash
[ -n "${STUB_FRPC_FAIL:-}" ] && exit 1
echo "frpc: the configuration file syntax check pass"
exit 0
"""
STUB_SYSTEMCTL = """#!/usr/bin/env bash
if [ "$1" = restart ] && [ -n "${STUB_SYSTEMCTL_FAIL:-}" ]; then exit 1; fi
[ "$1" = is-active ] && echo active
exit 0
"""
BASE_CONF = """serverAddr = "1.2.3.4"
serverPort = 7000

[[proxies]]
name = "ep-000160"
type = "tcp"
localPort = 22
remotePort = 6100
"""


def run_webfix_script(conf_path, name="ep-000160-web", port=30160, env_extra=None):
    with tempfile.TemporaryDirectory() as stubdir:
        for fname, content in (("sudo", STUB_SUDO), ("frpc", STUB_FRPC),
                               ("systemctl", STUB_SYSTEMCTL)):
            path = os.path.join(stubdir, fname)
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(content)
            os.chmod(path, 0o755)
        env = dict(os.environ, PATH=stubdir + os.pathsep + os.environ.get("PATH", ""),
                   WF_NAME=name, WF_REMOTE_PORT=str(port), WF_CONF=conf_path,
                   DEPLOY_PW="test-pw")
        env.update(env_extra or {})
        proc = subprocess.run(["bash", "-s"], input=server.SCRIPT_WEBFIX,
                              env=env, capture_output=True, text=True, timeout=60)
        return proc.returncode, proc.stdout + proc.stderr


print("== Part 1: SCRIPT_WEBFIX 沙箱 ==")
with tempfile.TemporaryDirectory() as td:
    conf = os.path.join(td, "frpc.toml")
    with open(conf, "w", encoding="utf-8") as fh:
        fh.write(BASE_CONF)
    original = open(conf, encoding="utf-8").read()

    rc, out = run_webfix_script(conf)
    now = open(conf, encoding="utf-8").read()
    check("1a 追加成功 rc", rc, 0)
    check("1a WEBFIX_DONE", "WEBFIX_DONE" in out, True)
    check("1a 追加代理块", 'name = "ep-000160-web"\ntype = "tcp"\nlocalPort = 80\nremotePort = 30160' in now, True)
    check("1a 原有配置保留", BASE_CONF in now, True)
    check("1a 只追加一次", now.count('name = "ep-000160-web"'), 1)
    check("1a 备份生成", len([f for f in os.listdir(td) if ".webfix.bak-" in f]), 1)

    rc2, out2 = run_webfix_script(conf)
    now2 = open(conf, encoding="utf-8").read()
    check("1b 幂等重跑 rc", rc2, 0)
    check("1b 已存在跳过追加", "already present" in out2, True)
    check("1b 配置未再变", now2, now)

    rc3, out3 = run_webfix_script(conf, env_extra={"STUB_FRPC_FAIL": "1"})
    check("1c 已存在+verify 失败 rc", rc3, 24)
    check("1c 配置未被破坏", open(conf, encoding="utf-8").read(), now)

    rc4, out4 = run_webfix_script(conf, name="ep-000160-web", port=39999)
    check("1d 同名端口冲突 rc", rc4, 21)
    check("1d name_conflict", "WEBFIX_ERROR=name_conflict" in out4, True)
    check("1d 配置未动", open(conf, encoding="utf-8").read(), now)

    conf2 = os.path.join(td, "frpc2.toml")
    with open(conf2, "w", encoding="utf-8") as fh:
        fh.write(BASE_CONF)
    rc5, out5 = run_webfix_script(conf2, env_extra={"STUB_FRPC_FAIL": "1"})
    now5 = open(conf2, encoding="utf-8").read()
    check("1e 追加+verify 失败 rc", rc5, 24)
    check("1e verify_failed", "WEBFIX_ERROR=verify_failed" in out5, True)
    check("1e 已自动还原备份", now5, BASE_CONF)

    rc6, out6 = run_webfix_script(os.path.join(td, "nope.toml"))
    check("1f 配置不存在 rc", rc6, 20)
    check("1f conf_not_found", "WEBFIX_ERROR=conf_not_found" in out6, True)

    conf3 = os.path.join(td, "frpc3.toml")
    with open(conf3, "w", encoding="utf-8") as fh:
        fh.write(BASE_CONF)
    rc7, out7 = run_webfix_script(conf3, env_extra={"STUB_SYSTEMCTL_FAIL": "1"})
    now7 = open(conf3, encoding="utf-8").read()
    check("1g 重启失败 rc", rc7, 25)
    check("1g restart_failed", "WEBFIX_ERROR=restart_failed" in out7, True)
    check("1g 配置保留追加结果", 'remotePort = 30160' in now7, True)

# ---------------------------------------------------------------------------
# Part 2: 引擎（mock push_script / frps 注册 / 设备版本探测）
# ---------------------------------------------------------------------------
print("== Part 2: 检测→一次性自动触发→验证闭环→手动重试 ==")
script_calls = []


def fake_push_script(task, script, envs, timeout):
    script_calls.append((task.hostname, dict(envs)))
    return 0, ("conf=/etc/frp/frpc.toml\nproxy appended\n"
               "frpc verify: syntax check pass\nfrpc_active=active\nWEBFIX_DONE\n")


server.push_script = fake_push_script
server._frps_raw_proxies = lambda: [("ep-000160-web", 30160), ("ep-000170-web", 30170)]

PROBE = {"ep-000160": {"device_id": "ep-000160", "release_version": "v0.4.6"},
         "ep-000170": {"device_id": "ep-000099", "release_version": "v0.4.6"}}
probe_calls = []


def fake_get_device_version(port, want_hw=False):
    probe_calls.append(port)
    name = next((k for k, v in PROBE.items() if 30000 + int(k.split("-")[1]) == port), "")
    info = PROBE.get(name)
    if not info:
        raise ValueError("cannot read device information")
    return dict(info)


server.get_device_version = fake_get_device_version
server.WEBFIX_MISS_STREAK = 2

# 防抖：第 1 轮不触发
first = server.webfix_scan()
check("2a 首轮防抖不触发", first, [])
check("2a 已有 web 代理的设备不触发", "ep-000214" in server._WEBFIX_STATE, False)

# 第 2 轮触发 ep-000160/170；ep-000180 机型未知 → 阻断
second = server.webfix_scan()
triggered = {it["hostname"] for it in second if it.get("status") == "running"}
check("2b 第二轮触发补配", triggered, {"ep-000160", "ep-000170"})
check("2b 机型未知判阻断", next((it for it in second if it["hostname"] == "ep-000180"), {})
      .get("status"), "blocked")

row160 = wait_status("ep-000160")
check("2c ep-000160 成功", row160.get("status"), "success")
check("2c remote_port", row160.get("remote_port"), 30160)
check("2c 来源 auto", row160.get("trigger_source"), "auto")
check("2c attempts=1", row160.get("attempts"), 1)
check("2c 无错误码", row160.get("error_code"), "")
check("2c job 关联", isinstance(row160.get("job_id"), int), True)

row170 = wait_status("ep-000170")
check("2d ep-000170 device_id 不符判失败", row170.get("status"), "failed")
check("2d 错误码", row170.get("error_code"), "webfix_device_id_mismatch")
check("2d 失败原因落库(内存)", "端口归属校验失败" in (row170.get("error") or ""), True)

row180 = wait_status("ep-000180")
check("2e ep-000180 阻断落库", row180.get("status"), "blocked")
check("2e 阻断原因", "无法确定机型" in (row180.get("error") or ""), True)
check("2e 阻断无 job", row180.get("job_id"), None)

# 一次性：再扫两轮，ep-000160/170/180 都不再自动触发
attempts_before = {h: r["attempts"] for h, r in server._WEBFIX_STATE.items()}
server.webfix_scan()
server.webfix_scan()
check("2f 自动触发仅一次", {h: r["attempts"] for h, r in server._WEBFIX_STATE.items()},
      attempts_before)

# 手动重试：ep-000170 修正 device_id 后成功，attempts=2、来源 manual
PROBE["ep-000170"] = {"device_id": "ep-000170", "release_version": "v0.4.7"}
retry = server.trigger_webfix("ep-000170", "manual")
check("2g 手动重试受理", retry.get("ok"), True)
row170b = wait_status("ep-000170", timeout=10)
check("2g 重试成功", row170b.get("status"), "success")
check("2g attempts=2", row170b.get("attempts"), 2)
check("2g 来源 manual", row170b.get("trigger_source"), "manual")

# 进行中拒绝重复触发 / 未知设备拒绝
with server._WEBFIX_LOCK:
    server._WEBFIX_STATE["ep-000170"] = dict(server._WEBFIX_STATE["ep-000170"], status="running")
check("2h 进行中拒绝", server.trigger_webfix("ep-000170", "manual").get("error"),
      "该设备补配进行中，请等待完成")
with server._WEBFIX_LOCK:
    server._WEBFIX_STATE["ep-000170"]["status"] = "success"
check("2h 未知设备拒绝", server.trigger_webfix("ep-000999", "manual").get("error"),
      "未知设备（不在面板记录中）")

# 重启收口：running → failed（可手动重试）
with server._WEBFIX_LOCK:
    server._WEBFIX_STATE["ep-000160"] = dict(server._WEBFIX_STATE["ep-000160"], status="running")
server._webfix_finish("ep-000160", "failed", "面板进程重启，补配中断（可用手动重试）")
check("2i 状态行可回写", server._WEBFIX_STATE["ep-000160"]["status"], "failed")

# 脚本环境变量契约
check("2j WF_NAME 传参", {h: e.get("WF_NAME") for h, e in script_calls}.get("ep-000160"),
      "ep-000160-web")
check("2j WF_REMOTE_PORT 传参", str(next(e.get("WF_REMOTE_PORT") for h, e in script_calls
                                     if h == "ep-000160")), "30160")

# ---------------------------------------------------------------------------
# Part 3: API 契约
# ---------------------------------------------------------------------------
print("== Part 3: /api/webfix/* ==")
httpd = ThreadingHTTPServer(("127.0.0.1", 19102), server.Handler)
httpd.daemon_threads = True
threading.Thread(target=httpd.serve_forever, daemon=True).start()
time.sleep(0.3)
BASE = "http://127.0.0.1:19102"


def get(path):
    with urllib.request.urlopen(BASE + path, timeout=5) as resp:
        return resp.status, json.load(resp)


def post(path, body):
    req = urllib.request.Request(BASE + path, method="POST",
                                 data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.load(resp)
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc)


code, body = get("/api/webfix/status")
row160 = next(i for i in body["items"] if i["hostname"] == "ep-000160")
check("3a status 200", code, 200)
check("3a items 含成功行", row160["status"], "failed")  # 2i 中改为 failed
check("3a missing 实时缺配", set(body["missing"]) >= {"ep-000160", "ep-000170", "ep-000180"}, True)
check("3a enabled", body["enabled"], True)

code, body = get("/api/webfix/status?hostname=ep-000180")
check("3b hostname 过滤", [i["hostname"] for i in body["items"]], ["ep-000180"])
check("3b 单台阻断态", body["items"][0]["status"], "blocked")

code, body = post("/api/webfix/retry", {"hostname": "no-such-host"})
check("3c 未知设备 400", code, 400)
check("3c 错误信息", body.get("ok"), False)

code, body = post("/api/webfix/retry", {})
check("3d 缺 hostname 400", code, 400)

code, body = post("/api/webfix/retry", {"hostname": "ep-000180"})
check("3e 阻断设备可手动重试（受理）", code, 400)  # 机型仍未登记 → 拒绝并给原因
check("3e 给出阻断原因", "无法确定机型" in body.get("error", ""), True)

# webfix 任务纳入既有任务/日志体系（kind=webfix 的 UpgradeJob）
code, body = get("/api/upgrade/jobs")
wf_jobs = [j for j in body["jobs"] if j["kind"] == "webfix"]
check("3f webfix 任务出现在任务列表", len(wf_jobs) >= 3, True)
# 取 ep-000170 那单成功任务的日志
code, body = get("/api/webfix/status?hostname=ep-000170")
jid = body["items"][0]["job_id"]
code, body = get(f"/api/upgrade/jobs/{jid}/log?hostname=ep-000170")
check("3f 日志可回看", code, 200)
check("3f 日志含 device_id 比对", "device_id 比对通过" in body.get("log", ""), True)
code, body = get(f"/api/upgrade/jobs/{jid}")
check("3f 任务详情 kind=webfix", body.get("kind"), "webfix")
check("3f 任务终态 completed", body.get("status"), "completed")

httpd.shutdown()
print()
print("FAILED:", fails if fails else "none — all passed")
sys.exit(1 if fails else 0)
