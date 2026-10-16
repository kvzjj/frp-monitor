# KVWO-13 自测：SSH 用户名按设备机型解析（amd64=archebase / arm=orangepi）
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


def run_case(extra_env, cases):
    """以指定环境变量导入 server 模块并断言映射结果。cases: [(label, fn, want)]"""
    env = dict(os.environ, **extra_env,
               FRP_MONITOR_DB_PORT="1")  # DB 不可达 → 内存模式，跳过持久化
    code = (
        "import sys, json; sys.path.insert(0, %r); import server\n" % HERE
    )
    payload = [(label, expr) for label, expr, _ in cases]
    code += "print(json.dumps({" + ",".join(
        f"{json.dumps(l)}: ({e})" for l, e in payload) + "}))\n"
    out = subprocess.run([sys.executable, "-c", code], env=env,
                         capture_output=True, text=True, timeout=60)
    if out.returncode != 0:
        print(out.stderr)
        raise SystemExit(f"import failed under env {extra_env}")
    res = json.loads(out.stdout.strip().splitlines()[-1])
    for label, _, want in cases:
        check(f"[{extra_env or 'default'}] {label}", res[label], want)


# ---- 1) 默认环境：按 hardware 字段（V0/V1）与 hw_model 型号识别 ----------------
run_case({}, [
    ("V0 硬件字段→archebase", "server.ssh_user_for_device('V0', '')", "archebase"),
    ("V1 硬件字段→orangepi", "server.ssh_user_for_device('V1', '')", "orangepi"),
    ("V0 大小写", "server.ssh_user_for_device('v0', '')", "archebase"),
    ("Intel N150 型号→archebase", "server.ssh_user_for_device('', 'Intel N150')", "archebase"),
    ("Celeron 型号→archebase", "server.ssh_user_for_device('', 'Intel(R) Celeron(R) N100')", "archebase"),
    ("amd64 型号→archebase", "server.ssh_user_for_device('', 'amd64')", "archebase"),
    ("rk3588 型号→orangepi", "server.ssh_user_for_device('', 'Rockchip RK3588S')", "orangepi"),
    ("Orange Pi 型号→orangepi", "server.ssh_user_for_device('', 'Orange Pi 5 Plus')", "orangepi"),
    ("rk3568 型号→orangepi", "server.ssh_user_for_device('', 'rk3568')", "orangepi"),
    ("hardware 优先于 hw_model", "server.ssh_user_for_device('V0', 'rk3588')", "archebase"),
    ("未知机型→archebase（其他）", "server.ssh_user_for_device('', '')", "archebase"),
    ("arch 直接解析 amd64", "server._ssh_user_for_arch('amd64')", "archebase"),
    ("arch 直接解析 rk3588-arm64", "server._ssh_user_for_arch('rk3588-arm64')", "orangepi"),
])

# ---- 2) 环境变量覆盖：全局 > 按机型 ------------------------------------------
run_case({"FRP_MONITOR_SSH_DEVICE_USER": "root"}, [
    ("全局覆盖对 V0", "server.ssh_user_for_device('V0', '')", "root"),
    ("全局覆盖对 V1", "server.ssh_user_for_device('V1', '')", "root"),
])
run_case({"FRP_MONITOR_SSH_USER_ARM": "opi", "FRP_MONITOR_SSH_USER_AMD": "ab"}, [
    ("按机型覆盖 arm", "server.ssh_user_for_device('V1', '')", "opi"),
    ("按机型覆盖 amd64", "server.ssh_user_for_device('V0', '')", "ab"),
])
run_case({"FRP_MONITOR_SSH_USER_AMD": "ab", "FRP_MONITOR_SSH_DEVICE_USER": "root"}, [
    ("优先级：全局覆盖>按机型", "server.ssh_user_for_device('V1', '')", "root"),
])

# ---- 3) 升级编排：UpgradeTask.ssh_user 与 _ssh_argv 接线 ----------------------
env = dict(os.environ, FRP_MONITOR_DB_PORT="1")
code = r"""
import sys; sys.path.insert(0, %r)
import server
print("@@RESULTS@@")
job = server.UpgradeJob(1, "precheck", "v0.4.6", {})
t_v0 = server.UpgradeTask(2, job, "ep-000145", 6100, {"arch": "amd64"})
t_v1 = server.UpgradeTask(3, job, "ep-000214", 6200, {"arch": "rk3588-arm64"})
print(t_v0.ssh_user, t_v1.ssh_user)
argv_v0 = server._ssh_argv(t_v0.ssh_port, t_v0.ssh_user, "echo hi")
argv_v1 = server._ssh_argv(t_v1.ssh_port, t_v1.ssh_user, "echo hi")
print(argv_v0[-2], argv_v1[-2])
print(argv_v0[-1], argv_v1[-1])
# 预检脚本 env 接线：PKG_HOME / PKG_ARCH（不真正 SSH，只验证 env 组装）
import server as s
orig = s.push_script
captured = {}
def fake_push(task, script, envs, timeout):
    captured[task.hostname] = dict(envs)
    return 0, "### done"
s.push_script = fake_push
s.stage_precheck(t_v0, {"version": "v0.4.6", "arch": "amd64"})
s.stage_precheck(t_v1, {"version": "v0.4.6", "arch": "rk3588-arm64", "oss_url": "https://x"})
s.push_script = orig
print(captured["ep-000145"]["PKG_HOME"], captured["ep-000145"]["PKG_ARCH"])
print(captured["ep-000214"]["PKG_HOME"], captured["ep-000214"]["PKG_ARCH"])
print(captured["ep-000214"].get("OSS_URL", ""))
print("orangepi-in-precheck:", "/home/orangepi/ego-portal" in s.SCRIPT_PRECHECK)
print("pkg-home-param:", '"${PKG_HOME:-/home/orangepi}"/ego-portal-*-"${PKG_ARCH:-rk3588-arm64}".tar.gz' in s.SCRIPT_PRECHECK)
""" % HERE
out = subprocess.run([sys.executable, "-c", code], env=env,
                     capture_output=True, text=True, timeout=60)
if out.returncode != 0:
    print(out.stderr)
    raise SystemExit("orchestration wiring test failed")
lines = out.stdout.strip().splitlines()
idx = lines.index("@@RESULTS@@")
lines = lines[idx + 1:]
check("UpgradeTask ssh_user (V0/V1)", tuple(lines[0].split()), ("archebase", "orangepi"))
check("_ssh_argv user@host (V0)", lines[1].split()[0], "archebase@127.0.0.1")
check("_ssh_argv user@host (V1)", lines[1].split()[1], "orangepi@127.0.0.1")
check("remote cmd 保留", lines[2], "echo hi echo hi")
check("precheck env V0 (home/arch)", (lines[3].split()[0], lines[3].split()[1]),
      ("/home/archebase", "amd64"))
check("precheck env V1 (home/arch)", (lines[4].split()[0], lines[4].split()[1]),
      ("/home/orangepi", "rk3588-arm64"))
check("precheck env OSS_URL 透传", lines[5], "https://x")
check("脚本不再硬编码 /home/orangepi 路径", lines[6].split(": ")[1], "False")
check("脚本改用 PKG_HOME/PKG_ARCH 参数", lines[7].split(": ")[1], "True")

print()
print("FAILED:", FAIL if FAIL else "none — all passed")
sys.exit(1 if FAIL else 0)
