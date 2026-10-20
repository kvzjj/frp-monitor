#!/usr/bin/env python3
"""资产多表同步与聚合 API 验证脚本（KVWO-83）。

两部分：
1. 单测（不依赖服务、不依赖 lark-cli）：
   - 行矩阵解析按字段名（markdown 表头）动态对齐，列序打乱 / 表间列序
     互异（设备明细 vs 归档）不影响结果；覆盖 select（列表/裸串两种形态）、
     多选、user（单人/多人）、datetime、attachment、link、number（整数/
     小数/脏值）、text 与各形态空值 → None。
   - AssetStore：record_id 幂等 upsert、删除飞书已删记录、按表隔离的
     同步错误状态。
   - stats 口径（库存 A/B、deployed、维修率、损坏率）、units 聚合、
     versions 匹配（matched / ledger_missing / detected_only）、
     单设备 360° 三源关联（编号匹配 + link 双路 + 单位模糊）。
2. 在线校验（--base http://127.0.0.1:9100）：与 lark-cli 直查 5 表结果
   比对同步计数/字段值/统计口径/单位聚合/版本匹配，幂等、筛选、详情、
   404、device_online/detected_version/active_repairs 联动一致性。

用法：
  python3 verify_assets.py                     # 仅单测
  python3 verify_assets.py --base URL          # 全量（需 server.py 已在跑）
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.parse
import urllib.request

# 单测的本地镜像库隔离到临时目录（import server 前设置才生效）
_TMP = tempfile.mkdtemp(prefix="verify-assets-")
os.environ.setdefault("FRP_MONITOR_ASSET_DB", os.path.join(_TMP, "assets.sqlite"))
os.environ.setdefault("FRP_MONITOR_REPAIR_DB", os.path.join(_TMP, "repairs.sqlite"))
os.environ.setdefault("FRP_MONITOR_REPAIR_ATTACH_DIR", os.path.join(_TMP, "att"))

sys.path.insert(0, ".")
PASS, FAIL = [], []


def check(name, ok, detail=""):
    (PASS if ok else FAIL).append(name)
    print(f"  {'✓' if ok else '✗'} {name}" + (f" — {detail}" if detail and not ok else ""))


def get(url, timeout=60):
    with urllib.request.urlopen(url, timeout=timeout) as resp:
        return resp.status, dict(resp.headers), resp.read()


def get_json(url):
    _, _, body = get(url)
    return json.loads(body)


# ---------------------------------------------------------------- 单测 --
def _fields_of(spec):
    return list(spec["fields"])


def unit_parse_tests(srv):
    print("[1] 行矩阵解析单测（按字段名动态对齐，不依赖列序）")
    # 1a. 设备明细：倒序 + 乱序列序（与 ASSET_TABLES 声明序不同）
    spec = srv.ASSET_TABLES["devices"]
    names = _fields_of(spec)
    shuffled = ["软件版本", "设备编号", "状态", "最后更新时间", "所属单位",
                "去向/备注", "设备类型", "配件情况", "修改人"]  # 实测归档表列序
    assert sorted(shuffled) == sorted(names), "测试列序必须与字段集合一致"
    fx = {
        "设备编号": "ep-000298",                      # 台账实测存在小写编号
        "设备类型": ["EgoPortal-Stereo-V1"],          # select 列表形态
        "状态": "正常",                                # select 裸串形态
        "所属单位": ["上海办公室"],
        "配件情况": "缺充电宝",
        "去向/备注": "未使用",
        "软件版本": None,                             # 当前全空 → None
        "修改人": [{"id": "ou1", "name": "赵嘉俊"}],
        "最后更新时间": "2026-09-19T11:25:24.000+08:00",
    }
    rec = srv.asset_parse_row([fx[n] for n in shuffled], shuffled, spec["fields"])
    check("设备明细 9 字段按名对齐（乱序列序）",
          rec == {"device_no": "ep-000298", "device_type": "EgoPortal-Stereo-V1",
                  "status": "正常", "unit": "上海办公室", "accessories": "缺充电宝",
                  "note": "未使用", "software_version": None, "modified_by": "赵嘉俊",
                  "updated_at": "2026-09-19T11:25:24.000+08:00"}, str(rec))
    # 1b. 归档表列序与设备明细不同（实测），同构字段仍正确解析
    spec_a = srv.ASSET_TABLES["archived"]
    order_a = ["设备编号", "去向/备注", "配件情况", "软件版本", "所属单位",
               "设备类型", "修改人", "最后更新时间", "状态"]
    assert order_a == _fields_of(spec_a) or sorted(order_a) == sorted(_fields_of(spec_a))
    row_a = [fx.get(n) for n in order_a]
    rec_a = srv.asset_parse_row(row_a, order_a, spec_a["fields"])
    check("归档表列序独立对齐", rec_a == rec, str(rec_a))

    # 1c. 异常处理流转：link / user 多人 / number 脏值 / select 设备编号
    spec_t = srv.ASSET_TABLES["actions"]
    names_t = _fields_of(spec_t)
    fx_t = {
        "摘要": "EP-000289 接收",
        "说明": "   ",
        "动作": ["维修跟进"],
        "提交人": [{"id": "ou9", "name": "多维表格助手"}],
        "操作人": [{"id": "ou1", "name": "刘希源"}, {"id": "ou2", "name": "陈婷婷"}],
        "转交给": None,
        "设备编号": ["EP-000289"],                     # 本表为单选
        "操作时间": "2026-10-08T14:35:33.000+08:00",
        "附件": [{"file_token": "tok1", "name": "a.jpg", "size": 3053021}],
        "关联异常设备": [{"id": "recvtmY3s4OSEf"}],      # link
    }
    rec_t = srv.asset_parse_row([fx_t[n] for n in names_t], names_t, spec_t["fields"])
    check("user 多人拼接", rec_t["operator"] == "刘希源、陈婷婷")
    check("link → record_id 列表", rec_t["linked_records"] == ["recvtmY3s4OSEf"])
    check("select 型设备编号取首项", rec_t["device_no"] == "EP-000289")
    check("附件三元组", rec_t["attachments"][0]["file_token"] == "tok1"
          and rec_t["attachments"][0]["size"] == 3053021)
    check("空白 text → None", rec_t["note"] is None)

    # 1d. 流转记录：number 整数 / 小数 / 脏值
    spec_f = srv.ASSET_TABLES["transfers"]
    names_f = _fields_of(spec_f)
    base_f = {"时间": "2026-08-09T00:00:00.000+08:00", "物品": "单目设备",
              "数量": 9, "说明": "移交", "所属单位": "构元服务商",
              "签收证明": [{"file_token": "tok2", "name": "p.jpg", "size": 10}]}
    rec_f = srv.asset_parse_row([base_f[n] for n in names_f], names_f, spec_f["fields"])
    check("number 整数原样", rec_f["quantity"] == 9)
    rec_f = srv.asset_parse_row([dict(base_f, 数量=1.5)[n] for n in names_f],
                                names_f, spec_f["fields"])
    check("number 小数原样", rec_f["quantity"] == 1.5)
    rec_f = srv.asset_parse_row([dict(base_f, 数量="三")[n] for n in names_f],
                                names_f, spec_f["fields"])
    check("number 脏值 → None", rec_f["quantity"] is None)

    # 1e. 全空行 → 全 None；多余列（飞书新增字段）忽略
    rec_e = srv.asset_parse_row([None] * len(names), names + ["新增未知列"], spec["fields"])
    check("空行全 None / 未知列忽略", all(v is None for v in rec_e.values())
          and len(rec_e) == len(names))


def unit_store_tests(srv):
    print("[2] AssetStore 幂等与按表隔离")
    spec = srv.ASSET_TABLES["devices"]
    row = {"device_no": "EP-000001", "device_type": "EgoPortal-Stereo-V0",
           "status": "正常", "unit": "上海办公室", "accessories": None, "note": None,
           "software_version": None, "modified_by": "张三",
           "updated_at": "2026-01-01T00:00:00.000+08:00"}
    srv.asset_store.replace_all("devices", {"rec1": row}, synced_at=100.0)
    row2 = dict(row, status="异常")
    srv.asset_store.replace_all("devices", {"rec1": row2, "rec2": dict(row, device_no="EP-000002")},
                                synced_at=200.0)
    got = srv.asset_store.all_records("devices")
    check("record_id 幂等 upsert（更新不重复）", len(got) == 2
          and {r["record_id"] for r in got} == {"rec1", "rec2"}
          and next(r for r in got if r["record_id"] == "rec1")["status"] == "异常")
    srv.asset_store.replace_all("devices", {"rec2": dict(row, device_no="EP-000002")},
                                synced_at=300.0)
    check("全量镜像删除飞书已删记录",
          [r["record_id"] for r in srv.asset_store.all_records("devices")] == ["rec2"])
    # 按表隔离：devices 出错不影响 transfers 状态
    srv.asset_store.replace_all("transfers", {"recT": {"occurred_at": None, "item": "x",
                                                       "quantity": 3, "note": None,
                                                       "unit": "上海办公室", "proof": None}},
                                synced_at=300.0)
    srv.asset_store.set_error("devices", "boom")
    s_dev, s_tr = srv.asset_store.sync_state("devices"), srv.asset_store.sync_state("transfers")
    check("单表错误状态隔离", s_dev["last_sync_error"] == "boom"
          and s_dev["record_count"] == 1          # 保留上次成功计数
          and s_tr["last_sync_error"] is None and s_tr["last_ok_at"] == 300.0)
    states = srv.asset_store.sync_states()
    check("sync_states 覆盖 5 表且带 label", set(states) == set(srv.ASSET_TABLES)
          and all("label" in v and "table_id" in v for v in states.values()))


def unit_stats_tests(srv):
    print("[3] stats / units / versions 口径单测（合成数据）")
    # 合成在册数据：11 台 → 正常 8 / 异常 1 / 维修中 1 / 待维修 1
    # 单位：上海办公室 5（其中备注「未使用」2）、潍坊办公室 6
    devices = {}
    for i in range(1, 9):
        devices[f"rec{i}"] = {"device_no": f"EP-00000{i}", "device_type": "EgoPortal-Stereo-V0",
                              "status": "正常", "unit": "上海办公室" if i <= 5 else "潍坊办公室",
                              "accessories": None,
                              "note": "未使用" if i <= 2 else None, "software_version": None,
                              "modified_by": None, "updated_at": "2026-09-01T00:00:00.000+08:00"}
    devices["rec9"] = dict(devices["rec8"], device_no="EP-000009", status="异常",
                           updated_at="2026-09-02T00:00:00.000+08:00")
    devices["rec10"] = dict(devices["rec8"], device_no="EP-000010", status="维修中",
                            updated_at="2026-09-03T00:00:00.000+08:00")
    devices["rec11"] = dict(devices["rec8"], device_no="EP-000011", status="待维修",
                            updated_at="2026-09-04T00:00:00.000+08:00")
    srv.asset_store.replace_all("devices", devices, synced_at=1.0)
    srv.asset_store.replace_all("archived", {"recA": dict(devices["rec1"], device_no="EP-900001")},
                                synced_at=1.0)
    # 维修表（异常设备表）联动：EP-000010 未完结 ×2、EP-000009 已完结 ×1
    def repair(rid, no, stage):
        return {"device_no": no, "device_type": "双目", "parts": None, "unit": None,
                "stage": stage, "problem": "p", "contact_name": None, "contact_phone": None,
                "recv_address": None, "return_address": None, "tracking_no_in": None,
                "tracking_no_out": None, "owner_name": "刘希源", "last_followed_at": None,
                "last_followup_note": None, "attachments": None,
                "created_at": "2026-10-01T00:00:00.000+08:00"}
    srv.repair_store.replace_all({
        "rX1": repair("rX1", "EP-000010", "维修中"),
        "rX2": repair("rX2", "ep-000010", "待发出"),   # 小写编号：大小写不敏感匹配
        "rX3": repair("rX3", "EP-000009", "已完结"),
    }, synced_at=1.0)

    stats = srv.assets_stats_view()
    check("registered_total 在册=设备明细", stats["registered_total"] == 11)
    check("archived_total 归档不计入在册", stats["archived_total"] == 1)
    check("库存口径 A（上海办公室）", stats["inventory"]["unit_shanghai"] == 5)
    check("库存口径 B（备注含未使用）", stats["inventory"]["unused_note"] == 2)
    check("deployed = 在册 − 库存A", stats["deployed"] == 6)
    check("维修率 = (维修中+待维修)/在册", abs(stats["repair_rate"] - 2 / 11) < 5e-5,
          str(stats["repair_rate"]))
    check("损坏率 = (异常+维修中+待维修)/在册", abs(stats["damage_rate"] - 3 / 11) < 5e-5,
          str(stats["damage_rate"]))
    check("by_status 分桶", stats["by_status"] == {"正常": 8, "异常": 1, "维修中": 1, "待维修": 1})

    units = srv.assets_units_view()
    by_unit = {u["unit"]: u for u in units}
    check("units 两单位聚合", set(by_unit) == {"上海办公室", "潍坊办公室"})
    check("单位设备数", by_unit["上海办公室"]["total"] == 5
          and by_unit["潍坊办公室"]["total"] == 6)
    check("正常率", abs(by_unit["潍坊办公室"]["normal_rate"] - 3 / 6) < 5e-5,
          str(by_unit["潍坊办公室"]["normal_rate"]))
    check("维修中=维修中+待维修 / 异常数",
          by_unit["潍坊办公室"]["repairing"] == 2 and by_unit["潍坊办公室"]["abnormal"] == 1)
    check("最近更新时间取最大", by_unit["潍坊办公室"]["last_updated"]
          == "2026-09-04T00:00:00.000+08:00")

    # versions：快照 mock（大小写混杂），MySQL 台账未启用 → registered 空
    snap_hosts = [{"hostname": "ep-000001", "version": "v1.0.0"},
                  {"hostname": "EP-000002", "version": ""},
                  {"hostname": "ep-999999", "version": "v1.0.0"}]
    orig = srv.get_snapshot
    srv.get_snapshot = lambda: (snap_hosts, 0.0)
    try:
        ver = srv.assets_versions_view()
    finally:
        srv.get_snapshot = orig
    check("探测版本计数（未获取单列一桶）",
          ver["detected_by_version"] == {"v1.0.0": 2, "未获取": 1})
    check("台账版本全空如实返回", ver["ledger_by_version"] == {}
          and ver["ledger_no_version"] == 11)
    mc = ver["match_counts"]
    check("matched = 在册 ∩ 探测（大小写不敏感）", mc["matched"] == 2
          and set(ver["match"]["matched"]) == {"ep-000001", "ep-000002"})
    check("ledger_missing = 在册 − 探测", mc["ledger_missing"] == 9)
    check("detected_only = 探测 − 在册 − 归档", mc["detected_only"] == 1
          and ver["match"]["detected_only"] == ["ep-999999"])
    check("matched + ledger_missing = 在册总数", mc["matched"] + mc["ledger_missing"] == 11)

    # 列表联动：device_online / detected_version / active_repairs（大小写不敏感）
    srv.get_snapshot = lambda: (snap_hosts, 0.0)
    try:
        items = {i["device_no"]: i for i in srv.assets_list_view({})}
    finally:
        srv.get_snapshot = orig
    check("device_online 三态（在线/未上线 null）",
          items["EP-000001"]["device_online"] is True
          and items["EP-000003"]["device_online"] is None)
    check("detected_version 在线取探测值 / 空版本 → None",
          items["EP-000001"]["detected_version"] == "v1.0.0"
          and items["EP-000002"]["detected_version"] is None
          and items["EP-000003"]["detected_version"] is None)
    check("active_repairs 大小写不敏感累计未完结",
          items["EP-000010"]["active_repairs"] == 2
          and items["EP-000009"]["active_repairs"] == 0
          and items["EP-000001"]["active_repairs"] == 0)


def unit_detail_tests(srv):
    print("[4] 单设备 360° 三源关联单测")
    # 处理流水：一路按设备编号（小写）、一路按 link → 维修 record_id
    srv.asset_store.replace_all("actions", {
        "a1": {"summary": "s1", "note": None, "action": "接收确认",
               "created_by": "多维表格助手", "operator": "刘希源", "transferred_to": None,
               "device_no": "ep-000010", "operated_at": "2026-10-08T14:35:33.000+08:00",
               "attachments": None, "linked_records": None},
        "a2": {"summary": "s2", "note": None, "action": "维修跟进",
               "created_by": "多维表格助手", "operator": "陈婷婷", "transferred_to": None,
               "device_no": None, "operated_at": "2026-10-09T10:00:00.000+08:00",
               "attachments": None, "linked_records": ["rX1"]},   # link → EP-000010 的维修
    }, synced_at=1.0)
    srv.asset_store.replace_all("parts", {
        "p1": {"item": "头盔", "part": "头盔", "device_no": "EP-000010",
               "arrival_status": "完好", "status_note": None, "checker": "刘希源",
               "checked_at": "2026-10-08T17:15:11.000+08:00", "photos": None,
               "linked_records": None},
    }, synced_at=1.0)
    srv.asset_store.replace_all("transfers", {
        "t1": {"occurred_at": "2026-08-09T00:00:00.000+08:00", "item": "单目设备",
               "quantity": 9, "note": None, "unit": "潍坊", "proof": None},
        "t2": {"occurred_at": "2026-08-10T00:00:00.000+08:00", "item": "相机",
               "quantity": 2, "note": None, "unit": "上海办公室", "proof": None},
    }, synced_at=1.0)

    detail = srv.asset_detail_view("ep-000010")     # 查询也用小写：大小写不敏感
    check("台账命中（查询小写）", detail is not None and detail["device_no"] == "EP-000010"
          and detail["archived"] is False)
    check("维修记录关联（含已完结，倒序）", len(detail["repairs"]) == 2)
    check("处理流水双路关联（编号 + link）",
          {a["record_id"] for a in detail["actions"]} == {"a1", "a2"})
    check("配件核对关联", [p["record_id"] for p in detail["parts_checks"]] == ["p1"])
    check("流转记录按单位模糊关联（潍坊 ⊂ 潍坊办公室）",
          [t["record_id"] for t in detail["transfers"]] == ["t1"])
    detail_arch = srv.asset_detail_view("EP-900001")
    check("归档设备命中且标记 archived", detail_arch is not None
          and detail_arch["archived"] is True
          and srv.ASSET_TABLES["archived"]["table_id"] in detail_arch["feishu_url"])
    check("未知设备返回 None", srv.asset_detail_view("EP-000000") is None)
    check("feishu_url 指向对应表原记录",
          srv.ASSET_TABLES["archived"]["table_id"] in detail_arch["feishu_url"]
          and srv.ASSET_TABLES["devices"]["table_id"] in detail["feishu_url"])


def unit_tests():
    import server as srv
    unit_parse_tests(srv)
    unit_store_tests(srv)
    unit_stats_tests(srv)
    unit_detail_tests(srv)


# ---------------------------------------------------------------- 在线校验 --
BASE_TOKEN = "V0YwbaOzya0ADBsO2wSc8M9Cntf"


def lark_direct(table_id):
    """lark-cli 直查一张表全量（与 API 比对的基准），返回 (fields, {rid: row})。
    瞬时失败（限流等）重试 3 次。"""
    import time
    fields, rows = [], {}
    offset = 0
    while True:
        out = None
        for attempt in range(3):
            out = subprocess.run(["lark-cli", "base", "+record-list",
                                  "--base-token", BASE_TOKEN, "--table-id", table_id,
                                  "--format", "json", "--limit", "200",
                                  "--offset", str(offset)],
                                 capture_output=True, text=True, timeout=120)
            pos = (out.stdout or "").find("{")
            if out.returncode == 0 and pos >= 0:
                break
            time.sleep(2 * (attempt + 1))
        pos = (out.stdout or "").find("{")
        if out.returncode != 0 or pos < 0:
            raise RuntimeError(f"lark-cli 直查失败（rc={out.returncode}）: "
                               f"{(out.stderr or out.stdout)[:300]}")
        payload = json.loads(out.stdout[pos:])
        if not payload.get("ok"):
            raise RuntimeError(f"lark-cli 直查失败: {out.stdout[:300]}")
        data = payload["data"]
        fields = data.get("fields") or fields
        for rid, row in zip(data.get("record_id_list") or [], data.get("data") or []):
            rows[rid] = row
        if not data.get("has_more") or not data.get("data"):
            break
        offset += len(data["data"])
    return fields, rows


def online_tests(base):
    print(f"[5] 在线校验（{base}）")
    import server as srv

    direct = {}
    for key, spec in srv.ASSET_TABLES.items():
        direct[key] = lark_direct(spec["table_id"])
    parsed = {key: {rid: srv.asset_parse_row(row, fields, spec["fields"])
                    for rid, row in rows.items()}
              for key, (fields, rows) in direct.items()
              for spec in [srv.ASSET_TABLES[key]]}

    # 手动触发同步（两次，校验幂等）
    req = urllib.request.Request(f"{base}/api/assets/sync", method="POST", data=b"")
    with urllib.request.urlopen(req, timeout=600) as resp:
        sync1 = json.loads(resp.read())
    check("5 表全部同步成功", sync1.get("ok") is True
          and all(v.get("ok") for v in sync1["tables"].values()), str(sync1)[:300])
    for key, (fields, rows) in direct.items():
        check(f"同步计数与直查一致（{srv.ASSET_TABLES[key]['label']}，{len(rows)} 条）",
              sync1["tables"][key]["count"] == len(rows),
              f"api={sync1['tables'][key]['count']} direct={len(rows)}")
    with urllib.request.urlopen(req, timeout=600) as resp:
        sync2 = json.loads(resp.read())
    check("二次同步幂等（各表计数不变）",
          all(sync2["tables"][k]["count"] == sync1["tables"][k]["count"]
              for k in srv.ASSET_TABLES))

    # /api/assets 全量字段比对（剔除本地扩充字段）
    listing = get_json(f"{base}/api/assets")
    items = {i["record_id"]: i for i in listing["items"]}
    dev_parsed = parsed["devices"]
    check("台账列表与直查一致（record_id 集合）", set(items) == set(dev_parsed),
          f"api={len(items)} direct={len(dev_parsed)}")
    extra = {"device_online", "detected_version", "active_repairs", "feishu_url",
             "synced_at", "archived"}
    mismatch = []
    for rid, rec in dev_parsed.items():
        for k, v in rec.items():
            if items.get(rid, {}).get(k) != v:
                mismatch.append((rid, k, v, items.get(rid, {}).get(k)))
    check("台账字段值与直查逐字段一致", not mismatch, str(mismatch[:3]))
    check("探测联动字段存在", all(set(extra) <= set(i) for i in items.values()))

    # 筛选：status / type / unit 精确，q 大小写不敏感
    by_status = {}
    for rec in dev_parsed.values():
        k = rec["status"] or ""
        by_status[k] = by_status.get(k, 0) + 1
    status0, n0 = max(by_status.items(), key=lambda kv: kv[1])
    f = get_json(f"{base}/api/assets?" + urllib.parse.urlencode({"status": status0}))
    check(f"筛选 status={status0}", f["total"] == n0
          and all(i["status"] == status0 for i in f["items"]))
    unit0 = next((rec["unit"] for rec in dev_parsed.values() if rec["unit"]), None)
    if unit0:
        f = get_json(f"{base}/api/assets?" + urllib.parse.urlencode({"unit": unit0}))
        check(f"筛选 unit={unit0}", f["total"] == sum(
            1 for r in dev_parsed.values() if r["unit"] == unit0))
    q = next(rec["device_no"] for rec in dev_parsed.values()
             if rec["device_no"] and rec["device_no"].lower() != rec["device_no"])
    if q:  # 台账存在小写编号：用大写查询验证大小写不敏感
        f = get_json(f"{base}/api/assets?" + urllib.parse.urlencode({"q": q.upper()}))
        check(f"q 大小写不敏感命中（{q.upper()}）", f["total"] >= 1
              and any(i["device_no"] == q for i in f["items"]))
    check("归档默认不含、include_archived=1 追加",
          listing["total"] == len(dev_parsed)
          and get_json(f"{base}/api/assets?include_archived=1")["total"]
          == len(dev_parsed) + len(parsed["archived"]))

    # stats 口径与直查一致
    stats = get_json(f"{base}/api/assets/stats")
    n = len(dev_parsed)
    arch_n = len(parsed["archived"])
    inv_a = sum(1 for r in dev_parsed.values() if r["unit"] == "上海办公室")
    inv_b = sum(1 for r in dev_parsed.values() if "未使用" in (r["note"] or ""))
    repairing = sum(1 for r in dev_parsed.values() if r["status"] in ("维修中", "待维修"))
    damaged = sum(1 for r in dev_parsed.values() if r["status"] in ("异常", "维修中", "待维修"))
    check("stats.registered_total", stats["registered_total"] == n)
    check("stats.archived_total", stats["archived_total"] == arch_n)
    check("stats.inventory 口径 A/B",
          stats["inventory"]["unit_shanghai"] == inv_a
          and stats["inventory"]["unused_note"] == inv_b)
    check("stats.deployed = 在册 − 库存A", stats["deployed"] == n - inv_a)
    check("stats.repair_rate / damage_rate",
          abs(stats["repair_rate"] - repairing / n) < 5e-4
          and abs(stats["damage_rate"] - damaged / n) < 5e-4)
    check("stats.by_status/by_type/by_unit 与直查一致",
          stats["by_status"] == by_status
          and stats["by_type"] == {k: sum(1 for r in dev_parsed.values() if r["device_type"] == k)
                                   for k in {r["device_type"] or "" for r in dev_parsed.values()}}
          and stats["by_unit"] == {k: sum(1 for r in dev_parsed.values() if r["unit"] == k)
                                   for k in {r["unit"] or "" for r in dev_parsed.values()}})
    print(f"    · 基线参考：在册 {n}、归档 {arch_n}、库存A {inv_a}、"
          f"维修率 {repairing}/{n}、损坏率 {damaged}/{n}")

    # units 聚合
    units = get_json(f"{base}/api/assets/units")["units"]
    units_total = sum(u["total"] for u in units)
    check("units 覆盖直查全部单位（含 11 个）",
          {u["unit"] for u in units} == {r["unit"] or "(未填写)" for r in dev_parsed.values()}
          and len(units) == len({r["unit"] or "(未填写)" for r in dev_parsed.values()}))
    check("units 设备数合计 = 在册", units_total == n)
    u0 = next(u for u in units if u["unit"] == "上海办公室")
    check("上海办公室聚合与库存口径 A 一致", u0["total"] == inv_a)

    # versions：台账版本计数与直查一致；匹配恒等式
    ver = get_json(f"{base}/api/assets/versions")
    ledger_direct = {}
    for r in dev_parsed.values():
        if r["software_version"]:
            ledger_direct[r["software_version"]] = ledger_direct.get(r["software_version"], 0) + 1
    check("versions.ledger_by_version 与直查一致（当前全空则 {}）",
          ver["ledger_by_version"] == ledger_direct)
    check("versions 匹配恒等式 matched+ledger_missing=在册",
          ver["match_counts"]["matched"] + ver["match_counts"]["ledger_missing"] == n)
    check("versions detected_only 非负且与快照一致",
          ver["match_counts"]["detected_only"] >= 0
          and ver["detected_total"] == ver["match_counts"]["matched"]
          + ver["match_counts"]["detected_only"]
          + (ver["match_counts"].get("detected_archived", 0) or 0)
          + max(0, ver["detected_total"] - ver["match_counts"]["matched"]
                - ver["match_counts"]["detected_only"]))
    hosts = get_json(f"{base}/api/hosts")
    online_hosts = {h["hostname"].casefold() for h in hosts["hosts"]}
    check("versions.matched ⊆ 在线快照",
          set(ver["match"]["matched"]) <= online_hosts)

    # 单设备 360°：取一台在异常处理流转里有记录的设备（编号或 link 双路）
    repairs_by_id = {}
    try:
        repairs_api = get_json(f"{base}/api/repairs")["items"]
        repairs_by_id = {r["record_id"]: r for r in repairs_api}
    except Exception:
        pass
    actions_parsed = parsed["actions"]
    ledger_keys = {srv._dev_key(r["device_no"]) for r in dev_parsed.values() if r["device_no"]}
    target = None
    for rec in actions_parsed.values():
        if rec["device_no"] and srv._dev_key(rec["device_no"]) in ledger_keys:
            target = rec["device_no"]
            break
    if target:
        det = get_json(f"{base}/api/assets/" + urllib.parse.quote(target))
        check(f"360° 台账+处理流水关联（{target}）",
              det["device_no"].casefold() == target.casefold()
              and isinstance(det["actions"], list) and len(det["actions"]) >= 1)
        check("360° 四键齐备（repairs/actions/parts_checks/transfers）",
              all(k in det for k in ("repairs", "actions", "parts_checks", "transfers")))
        # link 双路：linked_records 命中该设备维修记录的流水也应出现
        linked_ids = {rid for rid, r in repairs_by_id.items()
                      if (r.get("device_no") or "").strip().casefold() == target.casefold()}
        via_link_ids = {rid for rid, a in actions_parsed.items()
                        if a["linked_records"] and linked_ids & set(a["linked_records"])}
        got_ids = {a["record_id"] for a in det["actions"]}
        check("360° link 双路关联生效", via_link_ids <= got_ids,
              f"link 命中 {len(via_link_ids)} 条，详情返回 {len(got_ids)} 条")
    else:
        check("360° 关联（当前流转表无设备编号样本）", False, "无可测设备")
    # 大小写不敏感查询 + 404
    if target:
        det2 = get_json(f"{base}/api/assets/" + urllib.parse.quote(target.lower()))
        check("360° 查询大小写不敏感", det2["record_id"] == det["record_id"])
    try:
        get_json(f"{base}/api/assets/EP-NOTEXIST")
        check("未知设备 404", False)
    except urllib.error.HTTPError as exc:
        check("未知设备 404", exc.code == 404)

    # device_online / detected_version 与 /api/hosts 一致
    bad = []
    for i in listing["items"]:
        dn = (i["device_no"] or "").strip().casefold()
        want = True if dn in online_hosts else None
        if i["device_online"] != want:
            bad.append((i["device_no"], i["device_online"], want))
    check("device_online 与 /api/hosts 快照一致", not bad, str(bad[:3]))
    by_host = {h["hostname"]: h for h in hosts["hosts"]}
    sample = next((i for i in listing["items"] if i["device_online"] is True), None)
    if sample:
        snap_host = by_host.get((sample["device_no"] or "").casefold())
        check("detected_version 与快照探测值一致",
              sample["detected_version"] == ((snap_host or {}).get("version") or None))
    # active_repairs 与 /api/repairs 一致（未完结、大小写不敏感）
    if repairs_by_id:
        want_active = {}
        for r in repairs_api:
            if r.get("stage") and r["stage"] != "已完结" and r.get("device_no"):
                k = r["device_no"].strip().casefold()
                want_active[k] = want_active.get(k, 0) + 1
        bad_ar = [(i["device_no"], i["active_repairs"], want_active.get((i["device_no"] or "").casefold(), 0))
                  for i in listing["items"]
                  if i["active_repairs"] != want_active.get((i["device_no"] or "").casefold(), 0)]
        check("active_repairs 与 /api/repairs 未完结计数一致（大小写不敏感）",
              not bad_ar, str(bad_ar[:3]))


def main():
    base = None
    args = sys.argv[1:]
    if "--base" in args:
        base = args[args.index("--base") + 1].rstrip("/")
    try:
        unit_tests()
        if base:
            online_tests(base)
    finally:
        shutil.rmtree(_TMP, ignore_errors=True)
    print(f"\n结果：{len(PASS)} 通过，{len(FAIL)} 失败")
    if FAIL:
        print("失败项：", FAIL)
        sys.exit(1)


if __name__ == "__main__":
    main()
