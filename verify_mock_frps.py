#!/usr/bin/env python3
"""本地验证用 mock frps：固定在线名单，GET /api/proxy/tcp 返回 frps 兼容结构。

用法：python3 mock_frps.py [端口] [在线设备名,逗号分隔]
（KVWO-79 维修面板后端验证环境；不入生产。）
"""
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 7400
ONLINE = [h for h in (sys.argv[2] if len(sys.argv) > 2 else "ep-000019,ep-000146,ep-000110").split(",") if h]


class H(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def do_GET(self):
        proxies = []
        for i, host in enumerate(ONLINE):
            proxies.append({"name": host, "status": "online",
                            "conf": {"name": host, "remotePort": 20000 + i},
                            "curConns": 1, "lastStartTime": "10-20 10:00:00"})
            proxies.append({"name": f"{host}-web", "status": "online",
                            "conf": {"name": f"{host}-web", "remotePort": 30000 + i},
                            "curConns": 0, "lastStartTime": "10-20 10:00:00"})
        body = json.dumps({"proxies": proxies}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    print(f"mock-frps: serving on :{PORT}, online={ONLINE}", flush=True)
    ThreadingHTTPServer(("127.0.0.1", PORT), H).serve_forever()
