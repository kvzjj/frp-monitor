#!/usr/bin/env python3
"""维修面板后端验证脚本（KVWO-79）。

两部分：
1. 解析单测（不依赖服务）：飞书行矩阵 → snake_case 字段解析，覆盖
   多选 / user / datetime / attachment / select / 空值 / 空白文本。
2. 在线校验（--base http://127.0.0.1:9100）：与 lark-cli 直查结果比对
   /api/repairs 数据一致性、幂等、筛选、统计、详情、附件 URL 可达、
   device_online 台账联动、同步错误状态暴露。

用法：
  python3 verify_repairs.py                     # 仅解析单测
  python3 verify_repairs.py --base URL          # 全量（需 server.py 已在跑）
"""
import json
import subprocess
import sys
import urllib.parse
import urllib.request

sys.path.insert(0, ".")
PASS, FAIL = [], []


def check(name, ok, detail=""):
    (PASS if ok else FAIL).append(name)
    print(f"  {'✓' if ok else '✗'} {name}" + (f" — {detail}" if detail and not ok else ""))


def get(url, timeout=30):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.status, dict(resp.headers), resp.read()


def get_json(url):
    _, _, body = get(url)
    return json.loads(body)


# ---------------------------------------------------------------- 解析单测 --
def unit_tests():
    print("[1] 字段解析单测（不依赖服务）")
    import server as srv
    fids = sorted(srv.REPAIR_FIELD_MAP)  # lark-cli 实测按 field_id 排序返回列
    fx = {
        "fldx4c6Flq": None,                       # 设备编号 空
        "fld5JmUvOY": "",                         # 空串 → None
        "fld8pX79fy": ["维修中"],                  # select（列表形态）
        "fld9KwsKJB": [{"file_token": "t1", "name": "a.jpg", "size": 1},
                       {"file_token": "t2", "name": "b.png", "size": 2}],   # 附件
        "fldaYxqLZP": None,
        "fldb9pcfQp": [{"id": "o1", "name": "张三"}, {"id": "o2", "name": "李四"}],  # 多 user
        "flddlnusAD": "   ",                      # 空白文本 → None
        "fldekV0gKB": "双目",                      # select（裸字符串形态）
        "fldHpO5cwl": None,
        "fldiwa2Opq": "2026-10-01T10:00:00.000+08:00",   # created_at datetime
        "fldK0Omrot": None, "fldNSnDuEW": "SF123", "fldPl56KmS": None, "fldPyqR6Kz": None,
        "fldQUKUSwQ": "镜头损坏",
        "fldVVgRTyK": "2026-10-09T17:09:55.741+08:00",   # 跟进 datetime
        "fldXSSDwxu": ["头盔", "腰带"],             # 多选
    }
    row = [fx[f] for f in fids]
    rec = srv.repair_parse_row(row, fids)
    check("17 字段齐全", set(rec) == {s[0] for s in srv.REPAIR_FIELD_MAP.values()})
    check("多选 parts", rec["parts"] == ["头盔", "腰带"])
    check("user 取 name（多人拼接）", rec["owner_name"] == "张三、李四")
    check("datetime 原样保留", rec["created_at"] == "2026-10-01T10:00:00.000+08:00"
          and rec["last_followup_at"] == "2026-10-09T17:09:55.741+08:00")
    check("attachment 三元组", [a["file_token"] for a in rec["attachments"]] == ["t1", "t2"]
          and rec["attachments"][0]["size"] == 1)
    check("select 两种形态", rec["stage"] == "维修中" and rec["device_type"] == "双目")
    check("空值/空白/空串 → None", all(rec[k] is None for k in
          ("device_no", "unit", "contact_name", "tracking_no_out", "recv_address")))
    # RepairStore 幂等
    import os
    import shutil
    import tempfile
    tmp = tempfile.mkdtemp()
    try:
        st = srv.RepairStore(os.path.join(tmp, "r.sqlite"))
        st.replace_all({"rec1": rec}, synced_at=100.0)
        st.replace_all({"rec1": rec}, synced_at=200.0)
        check("SQLite 二次同步幂等（无重复）", len(st.all_records()) == 1
              and st.get("rec1")["synced_at"] == 200.0)
        st.replace_all({}, synced_at=300.0)
        check("全量镜像删除飞书已删记录", st.all_records() == [])
        st.set_error("boom")
        s = st.sync_state()
        check("同步失败状态记录（保留上次成功计数）",
              s["last_sync_error"] == "boom" and s["last_ok_at"] == 300.0 and s["record_count"] == 0)
    finally:
        shutil.rmtree(tmp)


# ---------------------------------------------------------------- 在线校验 --
def lark_direct():
    """lark-cli 直查（与 API 比对的基准）。"""
    out = subprocess.run(["lark-cli", "base", "+record-list",
                          "--base-token", "V0YwbaOzya0ADBsO2wSc8M9Cntf",
                          "--table-id", "tblf1nlwkHhMX1V9",
                          "--format", "json", "--limit", "200", "--offset", "0"],
                         capture_output=True, text=True, timeout=120)
    payload = json.loads(out.stdout[out.stdout.find("{"):])
    if not payload.get("ok"):
        raise RuntimeError(f"lark-cli 直查失败: {out.stdout[:300]}")
    data = payload["data"]
    return data


def online_tests(base):
    print(f"[2] 在线校验（{base}）")
    data = lark_direct()
    fids, rows, rids = data["field_id_list"], data["data"], data["record_id_list"]
    import server as srv
    direct = {rid: srv.repair_parse_row(row, fids) for rid, row in zip(rids, rows)}
    direct_by_stage = {}
    for rec in direct.values():
        key = rec["stage"] or ""
        direct_by_stage[key] = direct_by_stage.get(key, 0) + 1

    listing = get_json(f"{base}/api/repairs")
    items = {i["record_id"]: i for i in listing["items"]}
    check("记录数与 lark-cli 直查一致", listing["total"] == len(direct),
          f"api={listing['total']} direct={len(direct)}")
    check("record_id 集合一致", set(items) == set(direct))
    # 逐字段比对（17 字段，snake_case；attachments 的 url 为本地扩充字段，比对时剔除）
    mismatch = []
    for rid, rec in direct.items():
        for k, v in rec.items():
            got = items.get(rid, {}).get(k)
            if k == "attachments" and got:
                got = [{kk: vv for kk, vv in a.items() if kk != "url"} for a in got]
            if got != v:
                mismatch.append((rid, k, v, got))
    check("17 字段值与直查一致（含多选/user/datetime/附件元数据）", not mismatch, str(mismatch[:3]))
    check("阶段中文枚举原样返回", all((i["stage"] in srv.REPAIR_STAGE_ORDER) or i["stage"] is None
                                     for i in items.values()))
    check("feishu_url 指向原记录", all(i["feishu_url"].startswith(
        "https://archebase.feishu.cn/base/") and i["record_id"] in i["feishu_url"]
        for i in items.values()))

    # 幂等：再同步一次，无重复、值不变
    res = get_json(f"{base}/api/repairs", timeout=0) if False else None
    req = urllib.request.Request(f"{base}/api/repairs/sync", method="POST", data=b"")
    with urllib.request.urlopen(req, timeout=180) as resp:
        sync = json.loads(resp.read())
    check("手动二次同步成功", sync.get("ok") is True, str(sync))
    listing2 = get_json(f"{base}/api/repairs")
    ids2 = [i["record_id"] for i in listing2["items"]]
    check("二次同步幂等（总数不变、无重复）",
          listing2["total"] == listing["total"] and len(set(ids2)) == len(ids2))
    check("同步状态字段（last_sync_at / error=null）",
          listing2["last_sync_at"] is not None and listing2["last_sync_error"] is None)

    # 筛选
    stage0 = next(s for s in srv.REPAIR_STAGE_ORDER if direct_by_stage.get(s))
    f = get_json(f"{base}/api/repairs?" + urllib.parse.urlencode({"stage": stage0}))
    check(f"筛选 stage={stage0}", f["total"] == direct_by_stage[stage0]
          and all(i["stage"] == stage0 for i in f["items"]))
    q = get_json(f"{base}/api/repairs?q=nonexistent-xyz")
    check("搜索 q 无命中返回空", q["total"] == 0)

    # 统计
    stats = get_json(f"{base}/api/repairs/stats")
    check("stats.total 与列表一致", stats["total"] == listing2["total"] == len(direct))
    check("stats.by_stage 计数与直查一致",
          all(stats["by_stage"].get(s, 0) == direct_by_stage.get(s, 0) for s in srv.REPAIR_STAGE_ORDER))
    check("stats.active_count = 总数 − 已完结",
          stats["active_count"] == stats["total"] - direct_by_stage.get("已完结", 0))

    # 详情（synced_at 随每次同步刷新，比对时剔除；其余字段须与列表项一致）
    rid0 = next(iter(direct))
    detail = get_json(f"{base}/api/repairs/{rid0}")
    strip = lambda d: {k: v for k, v in d.items() if k != "synced_at"}
    check("单条详情与列表项一致（除 synced_at）", strip(detail) == strip(items[rid0]))
    try:
        get_json(f"{base}/api/repairs/recNOTEXIST")
        check("未知 record_id 返回 404", False)
    except urllib.error.HTTPError as exc:
        check("未知 record_id 返回 404", exc.code == 404)

    # 附件 URL 浏览器可直接打开
    with_att = [i for i in listing2["items"] if i["attachments"]]
    if with_att:
        att = next(a for i in with_att for a in i["attachments"] if a["url"])
        status, headers, body = get(base + att["url"])
        check(f"附件 URL 可直接打开（{headers.get('Content-Type')}，{len(body)}B）",
              status == 200 and len(body) == att["size"])
    else:
        check("附件 URL 可直接打开", False, "当前数据无附件")

    # device_online 台账联动：值域合法 + 与 /api/hosts 台账一致
    hosts = get_json(f"{base}/api/hosts")
    online_hosts = {h["hostname"] for h in hosts["hosts"]}
    registered = {d["hostname"] for d in hosts["fleet"]}
    bad = []
    for i in listing2["items"]:
        got, dn = i["device_online"], (i["device_no"] or "").strip().casefold()
        want = True if dn in online_hosts else (False if dn in registered else None)
        if got != want:
            bad.append((i["device_no"], got, want))
    check("device_online 与台账/快照一致（true/false/null）", not bad, str(bad[:3]))
    nulls = [i["device_no"] for i in listing2["items"] if i["device_online"] is None]
    print(f"    · device_online=null（未匹配，显示 —）: {nulls}")
    check("存在未匹配设备（null）", any(nulls))
    sampled = [i for i in listing2["items"] if i["device_no"] and "\n" not in i["device_no"]][:5]
    check("抽样核对 ≥5 台", len(sampled) == 5)


def main():
    base = None
    args = sys.argv[1:]
    if "--base" in args:
        base = args[args.index("--base") + 1].rstrip("/")
    unit_tests()
    if base:
        online_tests(base)
    print(f"\n结果：{len(PASS)} 通过，{len(FAIL)} 失败")
    if FAIL:
        print("失败项：", FAIL)
        sys.exit(1)


if __name__ == "__main__":
    main()
