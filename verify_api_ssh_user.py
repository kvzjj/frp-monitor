# KVWO-13 自测：起服务（mock frps 快照，DB 关闭）验证 /api/hosts 与 /api/device
# 的按设备 ssh_user / ssh_hint 输出（V0=ep-000145 Intel N150，V1=ep-000214 rk3588）。
import json
import os
import sys
import threading
import time
import urllib.request
from http.server import ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import server

SNAP = [
    {"hostname": "ep-000145", "version": "v0.4.6", "port": 6100, "web_port": 8080,
     "connections": 1, "started": "", "version_error": "",
     "device": {"device_id": "D145", "device_name": "ep-000145", "hw_model": "Intel N150",
                "ip": "10.0.0.145"}},
    {"hostname": "ep-000149", "version": "v0.4.6", "port": 6101, "web_port": 8080,
     "connections": 1, "started": "", "version_error": "",
     "device": {"device_id": "D149", "device_name": "ep-000149", "hw_model": "amd64",
                "ip": "10.0.0.149"}},
    {"hostname": "ep-000214", "version": "v0.4.6", "port": 6200, "web_port": 8080,
     "connections": 1, "started": "", "version_error": "",
     "device": {"device_id": "D214", "device_name": "ep-000214",
                "hw_model": "Rockchip RK3588S", "ip": "10.0.0.214"}},
]
server.get_snapshot = lambda: (SNAP, time.time())

httpd = ThreadingHTTPServer(("127.0.0.1", 19101), server.Handler)
httpd.daemon_threads = True
threading.Thread(target=httpd.serve_forever, daemon=True).start()
time.sleep(0.3)

fails = []


def check(name, got, want):
    ok = got == want
    print(f"{'PASS' if ok else 'FAIL'}  {name}: got={got!r} want={want!r}")
    if not ok:
        fails.append(name)


d = json.load(urllib.request.urlopen("http://127.0.0.1:19101/api/hosts", timeout=5))
by = {f["hostname"]: f for f in d["fleet"]}
check("fleet V0(Intel N150).ssh_user", by["ep-000145"]["ssh_user"], "archebase")
check("fleet V0(amd64).ssh_user", by["ep-000149"]["ssh_user"], "archebase")
check("fleet V1(rk3588).ssh_user", by["ep-000214"]["ssh_user"], "orangepi")
check("全局 ssh_hint 兼容默认", d["ssh_hint"], {"jump": "ubuntu@124.222.147.63",
                                                "user": "orangepi"})

dev = json.load(urllib.request.urlopen(
    "http://127.0.0.1:19101/api/device/ep-000145", timeout=5))
check("device V0 ssh_hint", dev["ssh_hint"], {"jump": "ubuntu@124.222.147.63",
                                              "user": "archebase"})
dev = json.load(urllib.request.urlopen(
    "http://127.0.0.1:19101/api/device/ep-000214", timeout=5))
check("device V1 ssh_hint", dev["ssh_hint"], {"jump": "ubuntu@124.222.147.63",
                                              "user": "orangepi"})

html = urllib.request.urlopen("http://127.0.0.1:19101/", timeout=5).read().decode()
check("前端 copyCmds 用 f.ssh_user", "${f.ssh_user||user}@127.0.0.1" in html, True)
check("前端不再用全局 user 拼命令", "${user}@127.0.0.1" in html, False)

httpd.shutdown()
print()
print("FAILED:", fails if fails else "none — all passed")
sys.exit(1 if fails else 0)
