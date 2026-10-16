#!/usr/bin/env python3
"""KVWO-18 前端自测桩：托管真实页面（server.INDEX/CSS/JS），伪造 /api/hosts、
/api/webfix/tasks、/api/webfix/retry、/api/device/* 等接口，供 headless Chrome 验证
卡片标记 / 详情行 / 重试按钮链路。用法：
  python3 verify_webfix_ui.py <port>   # 再用 headless 浏览器访问 /?drive=cards|modal|retry"""
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import server  # noqa: E402

retry_calls = []

DEVICES = {
    # 缺配 + 任务 failed → 期望卡片「补配失败」红标 + 详情失败原因 + 重试按钮
    "ep-000160": dict(online=True, ssh_port=41001, web_port=None, version="",
                      device_id="160", site="站点A"),
    # 缺配 + 任务 running → 期望「补配中…」转圈
    "ep-000118": dict(online=True, ssh_port=41002, web_port=None, version="",
                      device_id="118", site="站点A"),
    # 正常（有 web_port）→ 无标记
    "ep-000101": dict(online=True, ssh_port=41003, web_port=30101, version="v0.4.6",
                      device_id="101", site="站点B"),
    # 缺配 + 无任务 → 灰色「web 缺配」（接口启用时等待自动补配）
    "ep-000102": dict(online=True, ssh_port=41004, web_port=None, version="",
                      device_id="102", site="站点B"),
    # 离线 → 不判定、无标记
    "ep-000103": dict(online=False, ssh_port=None, web_port=None, version="",
                      device_id="103", site="站点B"),
}

TASKS = {
    "ep-000160": dict(hostname="EP-000160", status="failed",
                      error="frpc verify 失败: parse error", attempts=1,
                      trigger_type="auto", created_at="2026-10-16 10:00:00"),
    "ep-000118": dict(hostname="EP-000118", status="running", error="",
                      attempts=1, trigger_type="auto", created_at="2026-10-16 11:00:00"),
    "ep-000101": dict(hostname="EP-000101", status="success", error="",
                      attempts=1, trigger_type="auto", created_at="2026-10-16 09:00:00"),
}

DRIVER = """
<script>
(function(){
  const mode=new URLSearchParams(location.search).get('drive')||'cards';
  window.__driveDone=false;
  function settle(fn,ms){setTimeout(()=>{try{fn()}catch(e){window.__driveErr=String(e)}window.__driveDone=true},ms)}
  if(mode==='cards')return; // 仅渲染卡片，靠 dump-dom 断言
  if(mode==='modal'){settle(()=>openDevice('ep-000160'),900);return}
  if(mode==='retry'){
    settle(()=>{
      openDevice('ep-000160');
      const waitModal=setInterval(()=>{
        const rb=document.getElementById('webfixRetry');
        if(!rb)return;
        clearInterval(waitModal);
        rb.click();                    // 第一次：进入「确认重试？」
        window.__armed=rb.textContent;
        setTimeout(()=>{rb.click()},300);   // 第二次：真正发起 POST
      },100);
    },900);
    return;
  }
})();
</script>"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _json(self, obj, code=200):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/":
            html = server.INDEX.format(css=server.CSS, js=server.JS) + DRIVER
            body = html.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif path == "/api/hosts":
            fleet = [dict(hostname=h, online=d["online"], ssh_port=d["ssh_port"],
                          web_port=d["web_port"], version=d["version"],
                          device_id=d["device_id"], site=d["site"], hardware="V0",
                          ip="10.0.0.1", hw_model="amd64", connections=1,
                          started="2026-10-16 08:00:00")
                     for h, d in DEVICES.items()]
            self._json(dict(timestamp=time.time(), total=len(fleet),
                            online=sum(1 for f in fleet if f["online"]),
                            registered=len(fleet), fleet=fleet, sites=[], versions=[]))
        elif path == "/api/webfix/tasks":
            self._json(dict(enabled=True, tasks=list(TASKS.values())))
        elif path == "/api/events":
            self._json(dict(enabled=False, events=[]))
        elif path == "/api/collect-duration":
            self._json(dict(enabled=False, items=[]))
        elif path.startswith("/api/device/"):
            h = path[len("/api/device/"):]
            d = DEVICES.get(h)
            if not d:
                self._json(dict(error="not found"), 404)
            else:
                self._json(dict(hostname=h, site=d["site"], device_name="ego",
                                device_id=d["device_id"], hardware="V0",
                                hw_model="amd64", ip="10.0.0.1", online=d["online"],
                                release_version=d["version"] or "未获取",
                                ssh_port=d["ssh_port"], web_port=d["web_port"],
                                first_seen="2026-10-01 00:00:00",
                                last_seen="2026-10-16 12:00:00",
                                uptime_24h=1.0, uptime_7d=0.9, events=[], status=[]))
        else:
            self._json(dict(error="not found"), 404)

    def do_POST(self):
        global retry_calls
        length = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(length).decode() if length else ""
        if self.path.split("?")[0] == "/api/webfix/retry":
            retry_calls.append(raw)
            print("RETRY_POST:", raw, flush=True)
            self._json(dict(ok=True, accepted=["ep-000160"]))
        elif self.path.split("?")[0] == "/__calls":
            self._json(dict(calls=retry_calls))
        else:
            self._json(dict(error="not found"), 404)


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 19200
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print("mock ready on", port, flush=True)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
