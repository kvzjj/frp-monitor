# KVWO-14 自测：验证阶段容器判据按机型校准（V0=2 核心 / V1=4 容器）+ /api/refresh 兼容端点
#
# 1) job 8 日志回放：4 台 V0（ep-000120/121/128/160）升级 v0.4.6 后 docker ps 实况
#    为 2 个运行容器（backend + decxin_camera_imu）且全为新版镜像 —— 应判 success
#    （旧判据 new>=4 对 V0 是假阴性，4 台全部误标 failed）。
# 2) V0 真失败场景：仍有旧版 portal 容器在跑 → failed，文案含「新版容器 1/2（V0 核心=2）」。
# 3) V1（rk3588）判据维持不变：4 容器全为新版 → success；仅 2 个新版 → failed。
# 4) 其余维度（版本/service/HTTP/frpc）仍生效。
# 5) GET/POST /api/refresh 不再 404（SOP/旧 fleet-panel 强刷入口，等价 /api/hosts?refresh=1）。
import json
import os
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
FAIL = []


def check(name, got, want):
    ok = got == want
    print(f"{'PASS' if ok else 'FAIL'}  {name}: got={got!r} want={want!r}")
    if not ok:
        FAIL.append(name)


# job 8 升级后 verify 阶段设备端回放输出（依 issue 现场证据重建：
# version=v0.4.6 / service、frpc active / http 200 / api release_version=v0.4.6 /
# docker ps 实况 = 2 个运行容器全为新版镜像）
V0_OUT = """host=@HOST@
version=v0.4.6
device=EGO-Portal-@HOST@
service=active
http=200
api={"setup_done":true,"release_version":"v0.4.6"}
frpc=active
containers:
ego_portal-backend-1 | registry.cn-hangzhou.aliyuncs.com/ego/ego-portal-backend:v0.4.6 | Up 2 minutes
ego_portal-decxin_camera_imu-1 | registry.cn-hangzhou.aliyuncs.com/ego/ros-modules:v0.4.6 | Up 2 minutes
current=/opt/ego-portal/v0.4.6
disk: 38G free
"""

V1_4NEW = """host=ep-000214
version=v0.4.6
device=EGO-Portal-ep-000214
service=active
http=200
api={"setup_done":true,"release_version":"v0.4.6"}
frpc=active
containers:
ego_portal-backend-1 | registry.cn-hangzhou.aliyuncs.com/ego/ego-portal-backend:v0.4.6 | Up 3 minutes
ego_portal-frontend-1 | registry.cn-hangzhou.aliyuncs.com/ego/ego-portal-frontend:v0.4.6 | Up 3 minutes
ego_portal-ros-bridge-1 | registry.cn-hangzhou.aliyuncs.com/ego/ros-bridge:v0.4.6 | Up 3 minutes
ego_portal-decxin_camera_imu-1 | registry.cn-hangzhou.aliyuncs.com/ego/ros-modules:v0.4.6 | Up 3 minutes
current=/opt/ego-portal/v0.4.6
disk: 21G free
"""

env = dict(os.environ, FRP_MONITOR_DB_PORT="1")  # DB 不可达 → 内存模式，跳过持久化
code = """
import sys, json, threading, urllib.request, urllib.error
sys.path.insert(0, {here!r})
import server

V0_OUT = {v0!r}
V1_4NEW = {v1!r}

def run_verify(hostname, out, ctx):
    job = server.UpgradeJob(1, "verify", ctx.get("version", "v0.4.6"), {{}})
    task = server.UpgradeTask(2, job, hostname, 6100, ctx)
    server.push_script = lambda t, s, e, timeout: (0, out)
    ok = server.stage_verify(task, ctx)
    return {{"ok": ok, "status": task.status, "error": task.error, "log": task.text}}

def v0_out(hostname):
    return V0_OUT.replace("@HOST@", hostname)

results = {{}}

# 1) job 8 回放：4 台 V0 全为新版 2 容器 → success
for h in ("ep-000120", "ep-000121", "ep-000128", "ep-000160"):
    results["job8_" + h] = run_verify(h, v0_out(h), {{"version": "v0.4.6", "arch": "amd64"}})

# 2) V0 真失败：1 新 + 1 旧 portal 容器仍在跑
old_mixed = v0_out("ep-000120").replace(
    "ego_portal-decxin_camera_imu-1 | registry.cn-hangzhou.aliyuncs.com/ego/ros-modules:v0.4.6",
    "ego_portal-decxin_camera_imu-1 | registry.cn-hangzhou.aliyuncs.com/ego/ros-modules:v0.4.5")
results["v0_old_running"] = run_verify("ep-000120", old_mixed, {{"version": "v0.4.6", "arch": "amd64"}})

# 3) V1：4 容器全新 → success；仅 2 新（把 V0 实况套 V1 判据）→ failed
results["v1_4new"] = run_verify("ep-000214", V1_4NEW, {{"version": "v0.4.6", "arch": "rk3588-arm64"}})
results["v1_2new"] = run_verify("ep-000214", v0_out("ep-000214"),
                                {{"version": "v0.4.6", "arch": "rk3588-arm64"}})

# 4) 其余维度：frpc 非 active → failed（容器维度满足也不放行）
results["v0_frpc_down"] = run_verify("ep-000120", v0_out("ep-000120").replace(
    "frpc=active", "frpc=failed"), {{"version": "v0.4.6", "arch": "amd64"}})

# 5) arch 缺省（历史任务无 arch 字段）→ 按 V1 判据兜底，行为同旧版
results["no_arch_2new"] = run_verify("ep-000120", v0_out("ep-000120"), {{"version": "v0.4.6"}})

# 6) /api/refresh 端点：GET 与 POST 均不再 404（stub 快照，不依赖 frps）
server._snapshot = [{{"hostname": "ep-000120"}}]
server._snapshot_ts = 123.0
server.refresh_snapshot = lambda: None
httpd = server.ThreadingHTTPServer(("127.0.0.1", 0), server.Handler)
threading.Thread(target=httpd.serve_forever, daemon=True).start()
def http(method, path):
    req = urllib.request.Request("http://127.0.0.1:%d%s" % (httpd.server_port, path), method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())
results["refresh_get"] = http("GET", "/api/refresh")
results["refresh_post"] = http("POST", "/api/refresh")
results["refresh_get_body"] = {{k: results["refresh_get"][1][k] for k in ("ok", "total")}}
httpd.shutdown()
print("@@RESULTS@@")
print(json.dumps(results, ensure_ascii=False))
""".format(here=HERE, v0=V0_OUT, v1=V1_4NEW)

out = subprocess.run([sys.executable, "-c", code], env=env,
                     capture_output=True, text=True, timeout=120)
if out.returncode != 0:
    print(out.stderr)
    raise SystemExit("stage_verify replay test failed")
lines = out.stdout.strip().splitlines()
idx = lines.index("@@RESULTS@@")
res = json.loads(lines[idx + 1])

# ---- 1) job 8 回放：4 台 V0 应判 success -------------------------------------
for h in ("ep-000120", "ep-000121", "ep-000128", "ep-000160"):
    r = res[f"job8_{h}"]
    check(f"job8 {h} 判定通过", (r["ok"], r["status"], r["error"]),
          (True, "queued", ""))
    check(f"job8 {h} 日志含 新版容器=2/2（V0 核心=2）",
          "新版容器=2/2（V0 核心=2）" in r["log"], True)

# ---- 2) V0 真失败：旧容器还在跑 ----------------------------------------------
r = res["v0_old_running"]
check("V0 旧容器仍在跑 → failed", (r["ok"], r["status"]), (False, "failed"))
check("V0 失败文案含 新版容器 1/2（V0 核心=2）",
      "新版容器 1/2（V0 核心=2）" in r["error"], True)

# ---- 3) V1 判据维持 ----------------------------------------------------------
r = res["v1_4new"]
check("V1 4 容器全新 → 通过", (r["ok"], r["status"], r["error"]), (True, "queued", ""))
check("V1 日志含 新版容器=4/4（V1 核心=4）", "新版容器=4/4（V1 核心=4）" in r["log"], True)
r = res["v1_2new"]
check("V1 仅 2 新容器 → failed", (r["ok"], r["status"]), (False, "failed"))
check("V1 失败文案含 新版容器 2/2（V1 核心=4）",
      "新版容器 2/2（V1 核心=4）" in r["error"], True)

# ---- 4) 其余维度仍生效 --------------------------------------------------------
r = res["v0_frpc_down"]
check("frpc 非 active → failed", (r["ok"], r["status"]), (False, "failed"))

# ---- 5) arch 缺省兜底（无 arch 的历史任务按 V1 判据，行为同旧版） -------------
r = res["no_arch_2new"]
check("无 arch 时按 V1 判据兜底 → 2 容器 failed", (r["ok"], r["status"]), (False, "failed"))
check("无 arch 失败文案含 V1 核心=4", "V1 核心=4" in r["error"], True)

# ---- 6) /api/refresh：GET/POST 均 200，不再 404 ------------------------------
check("GET /api/refresh → 200", res["refresh_get"][0], 200)
check("POST /api/refresh → 200", res["refresh_post"][0], 200)
check("GET /api/refresh 响应体", res["refresh_get_body"], {"ok": True, "total": 1})

print()
print("FAILED:", FAIL if FAIL else "none — all passed")
sys.exit(1 if FAIL else 0)
