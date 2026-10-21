#!/usr/bin/env python3
"""资产多表同步与聚合 API 验证脚本（KVWO-83；KVWO-97 增补日报/口径/标题；
KVWO-107 第六波：闲置默认 15 天 + units 使用率 + versions 前 5 服务商版本占比）。

两部分：
1. 单测（不依赖服务、不依赖 lark-cli）：
   - 行矩阵解析按字段名（markdown 表头）动态对齐，列序打乱 / 表间列序
     互异（设备明细 vs 归档）不影响结果；覆盖 select（列表/裸串两种形态）、
     多选、user（单人/多人）、datetime、attachment、link、number（整数/
     小数/脏值）、text 与各形态空值 → None。
   - AssetStore：record_id 幂等 upsert、删除飞书已删记录、按表隔离的
     同步错误状态。
   - stats 口径（KVWO-97：库存 = 上海办公室+潍坊办公室分单位明细，服务商
     不计；deployed、维修率、损坏率）、units 聚合、versions 匹配
     （matched / ledger_missing / detected_only）、单设备 360° 三源关联
     （编号匹配 + link 双路 + 单位模糊）。
   - 资产日报（KVWO-97）：库存单位清单/推送时刻解析、下一调度时刻、日报
     文案（四项指标+数据时间+同步异常提示）、webhook 成功/失败判定
     （monkeypatch，不发真实请求）、面板标题兑底与 <title>/<h1> 同源。
2. 在线校验（--base http://127.0.0.1:9100）：与 lark-cli 直查 5 表结果
   比对同步计数/字段值/统计口径/单位聚合/版本匹配，幂等、筛选、详情、
   404、device_online/detected_version/active_repairs 联动一致性；
   日报状态端点/手动推送文案与首页标题一致性。

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
    # 单位：上海办公室 5（其中备注「未使用」2）、潍坊办公室 3、潍坊构元服务商 3
    # （库存口径：上海+潍坊均为公司库存；服务商持有不计库存，计入一线部署）
    devices = {}
    for i in range(1, 9):
        devices[f"rec{i}"] = {"device_no": f"EP-00000{i}", "device_type": "EgoPortal-Stereo-V0",
                              "status": "正常", "unit": "上海办公室" if i <= 5 else "潍坊办公室",
                              "accessories": None,
                              "note": "未使用" if i <= 2 else None, "software_version": None,
                              "modified_by": None, "updated_at": "2026-09-01T00:00:00.000+08:00"}
    devices["rec9"] = dict(devices["rec8"], device_no="EP-000009", status="异常",
                           unit="潍坊构元服务商",
                           updated_at="2026-09-02T00:00:00.000+08:00")
    devices["rec10"] = dict(devices["rec8"], device_no="EP-000010", status="维修中",
                            unit="潍坊构元服务商",
                            updated_at="2026-09-03T00:00:00.000+08:00")
    devices["rec11"] = dict(devices["rec8"], device_no="EP-000011", status="待维修",
                            unit="潍坊构元服务商",
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
    check("库存口径（上海+潍坊分单位明细，服务商不计）",
          stats["inventory"]["total"] == 8
          and stats["inventory"]["by_unit"] == {"上海办公室": 5, "潍坊办公室": 3}
          and stats["inventory"]["units"] == ["上海办公室", "潍坊办公室"])
    check("库存口径 B（备注含未使用，仅参考）", stats["inventory"]["unused_note"] == 2)
    check("deployed = 在册 − 库存（服务商持有计部署）", stats["deployed"] == 3)
    check("维修率 = (维修中+待维修)/在册", abs(stats["repair_rate"] - 2 / 11) < 5e-5,
          str(stats["repair_rate"]))
    check("损坏率 = (异常+维修中+待维修)/在册", abs(stats["damage_rate"] - 3 / 11) < 5e-5,
          str(stats["damage_rate"]))
    check("by_status 分桶", stats["by_status"] == {"正常": 8, "异常": 1, "维修中": 1, "待维修": 1})

    units = srv.assets_units_view()
    by_unit = {u["unit"]: u for u in units}
    check("units 三单位聚合", set(by_unit) ==
          {"上海办公室", "潍坊办公室", "潍坊构元服务商"})
    check("单位设备数", by_unit["上海办公室"]["total"] == 5
          and by_unit["潍坊办公室"]["total"] == 3
          and by_unit["潍坊构元服务商"]["total"] == 3)
    check("正常率", abs(by_unit["潍坊办公室"]["normal_rate"] - 1.0) < 5e-5,
          str(by_unit["潍坊办公室"]["normal_rate"]))
    check("维修中=维修中+待维修 / 异常数",
          by_unit["潍坊构元服务商"]["repairing"] == 2
          and by_unit["潍坊构元服务商"]["abnormal"] == 1)
    check("最近更新时间取最大", by_unit["潍坊构元服务商"]["last_updated"]
          == "2026-09-04T00:00:00.000+08:00")

    # versions：快照 mock（大小写混杂），MySQL 台账未启用 → registered 空
    snap_hosts = [{"hostname": "ep-000001", "version": "v1.0.0"},
                  {"hostname": "EP-000002", "version": ""},
                  {"hostname": "ep-999999", "version": "v1.0.0"}]
    orig = srv.get_snapshot
    srv.get_snapshot = lambda: (snap_hosts, 0.0)
    try:
        ver = srv.assets_versions_view()
        units_snap = srv.assets_units_view()
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

    # units 使用率（KVWO-107 第六波）：frps 在线快照数 / 在册，大小写不敏感
    by_unit_snap = {u["unit"]: u for u in units_snap}
    check("units.online/usage_rate = 在线快照数/在册（快照大写也命中）",
          by_unit_snap["上海办公室"]["online"] == 2      # ep-000001 + EP-000002
          and abs(by_unit_snap["上海办公室"]["usage_rate"] - 0.4) < 5e-5
          and by_unit_snap["潍坊办公室"]["online"] == 0
          and by_unit_snap["潍坊办公室"]["usage_rate"] == 0.0
          and by_unit_snap["潍坊构元服务商"]["usage_rate"] == 0.0)
    # by_provider_top5：在册数降序、并列按单位名稳定；未探测/空版本归「未获取」
    top5 = ver["by_provider_top5"]
    check("by_provider_top5 排序（在册降序、并列按单位名，不足 5 家如实全列）",
          [t["unit"] for t in top5] == ["上海办公室", "潍坊办公室", "潍坊构元服务商"]
          and [t["total"] for t in top5] == [5, 3, 3])
    check("by_provider_top5 版本分桶（计数降序）+ share=数量/在册（两位小数）",
          top5[0]["versions"] == [{"version": "未获取", "count": 4, "share": 0.8},
                                 {"version": "v1.0.0", "count": 1, "share": 0.2}]
          and top5[1]["versions"] == [{"version": "未获取", "count": 3, "share": 1.0}]
          and all(sum(v["count"] for v in t["versions"]) == t["total"]
                  for t in top5))

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

    # 闲置口径（KVWO-97 需求 4）：库存外·超阈值天数未上线（含从未上线）；
    # 第六波（KVWO-107）：默认 30 → 15 天，env 覆盖机制保留
    from datetime import datetime as _dt, timedelta as _td
    check("fleet 库未启用 → idle.total 如实为 None（不合成）；默认阈值 15",
          stats["idle"]["total"] is None
          and stats["idle"]["threshold_days"] == 15
          and srv.IDLE_DAYS == 15
          and srv._device_idle("EP-000001", "上海办公室", None) is None)
    now = _dt.now()
    fmt = lambda d: d.strftime("%Y-%m-%d %H:%M")
    seen_40d, seen_now, seen_3d = fmt(now - _td(days=40)), fmt(now), fmt(now - _td(days=3))
    fake_seen = {"ep-000001": seen_40d,   # 库存（上海）超阈值未上线 → 不算闲置
                 "ep-000002": seen_now,    # 库存（上海）刚在线 → 不算
                 "ep-000009": seen_3d}    # 服务商 3 天前在线 → 不算；其余无记录=从未上线
    orig_ls = srv._fleet_last_seen
    srv._fleet_last_seen = lambda: fake_seen
    try:
        st2 = srv.assets_stats_view()
        items2 = {i["device_no"]: i for i in srv.assets_list_view({})}
        arch2 = {i["device_no"]: i for i in srv.assets_list_view({"include_archived": "1"})
                 if i["archived"]}
    finally:
        srv._fleet_last_seen = orig_ls
    check("闲置=库存外从未上线/超阈值；库存设备即使从未上线也不算",
          st2["idle"]["total"] == 2                      # rec10/rec11（服务商，从未上线）
          and st2["idle"]["by_unit"] == {"潍坊构元服务商": 2}
          and items2["EP-000010"]["idle"] is True       # 从未上线 → 闲置
          and items2["EP-000001"]["idle"] is False      # 库存超阈值未上线 → 不算
          and items2["EP-000006"]["idle"] is False)     # 库存从未上线 → 不算（边界）
    check("最近在线不闲置（3 天前）+ last_seen 原样返回",
          items2["EP-000009"]["idle"] is False
          and items2["EP-000009"]["last_seen"] == seen_3d
          and items2["EP-000010"]["last_seen"] is None)
    check("归档设备 idle=null（在册口径外）", arch2["EP-900001"]["idle"] is None)
    text3 = srv.feishu_report_text(st2, now=_dt(2026, 10, 21, 9, 0))
    check("日报文案：闲置行（数量+口径，阈值随默认值 15）",
          "闲置：2 台（库存外超 15 天未上线，含从未上线）" in text3)
    # 15 天边界（KVWO-107 默认）：「超过 15 天」为严格大于——恰好 15 天整
    # 不算，超出即算
    bnow = _dt(2026, 10, 21, 9, 0)
    seen_15d = fmt(bnow - _td(days=15))                     # 恰好 15 天整 → 不算
    seen_15d1m = fmt(bnow - _td(days=15) - _td(minutes=1))  # 15 天 + 1 分钟 → 算
    check("15 天边界：恰好 15 天整不算闲置，超过即算（严格 >）",
          srv._device_idle("EP-000010", "潍坊构元服务商",
                           {"ep-000010": seen_15d}, now=bnow) is False
          and srv._device_idle("EP-000010", "潍坊构元服务商",
                               {"ep-000010": seen_15d1m}, now=bnow) is True)
    # 阈值 env 可配生效（KVWO-98 F 组）：FRP_MONITOR_IDLE_DAYS=7 下，10 天未上线即闲置
    proc = subprocess.run(
        [sys.executable, "-c",
         "import server; "
         "assert server.IDLE_DAYS == 7, server.IDLE_DAYS; "
         "from datetime import datetime, timedelta; "
         "now = datetime(2026, 10, 21, 9, 0); "
         "seen10 = (now - timedelta(days=10)).strftime('%Y-%m-%d %H:%M'); "
         "ls = {'ep-000010': seen10}; "
         "assert server._device_idle('EP-000010', '潍坊构元服务商', ls, now=now) is True; "
         "assert server._idle_stats([{'device_no': 'EP-000010', 'unit': '潍坊构元服务商'}], ls, now=now)['threshold_days'] == 7; "
         "print('OK')"],
        capture_output=True, text=True, timeout=120,
        cwd=os.path.dirname(os.path.abspath(__file__)),
        env={**os.environ, "FRP_MONITOR_IDLE_DAYS": "7"})
    check("阈值 env FRP_MONITOR_IDLE_DAYS 可配生效（7 天阈值下 10 天未上线即闲置）",
          proc.returncode == 0 and "OK" in (proc.stdout or ""),
          detail=(proc.stderr or "")[-200:])


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


def unit_report_tests(srv):
    print("[4] 资产日报推送与面板标题单测（文案/调度/标题，不发真实请求）")
    from datetime import datetime as _dt

    # 配置解析：库存单位清单 / 推送时刻（与采集时长调度同模式）
    check("库存单位清单：空/默认 → 上海+潍坊",
          srv._parse_unit_list("") == ["上海办公室", "潍坊办公室"]
          and srv._parse_unit_list("上海办公室,潍坊办公室") == ["上海办公室", "潍坊办公室"])
    check("库存单位清单：env 扩展 + 去重保序",
          srv._parse_unit_list(" 上海办公室 , 苏州项目组 ,潍坊办公室,苏州项目组, ")
          == ["上海办公室", "苏州项目组", "潍坊办公室"])
    check("推送时刻解析：默认 09:00 / 多时刻 / 非法项忽略",
          srv._parse_report_slots("") == [(9, 0)]
          and srv._parse_report_slots("09:00,21:30") == [(9, 0), (21, 30)]
          and srv._parse_report_slots("9:00,25:00,xx,,09:00") == [(9, 0)])
    check("下一推送时刻：当日未到取当日 / 已过滚存次日",
          srv._next_report_slot(_dt(2026, 10, 21, 8, 59, 30)) == _dt(2026, 10, 21, 9, 0)
          and srv._next_report_slot(_dt(2026, 10, 21, 9, 0, 0)) == _dt(2026, 10, 22, 9, 0))
    check("webhook 未配置时调度线程直接退出（置空=关闭）",
          srv.FEISHU_WEBHOOK == "" and srv._feishu_report_loop() is None)

    # 日报文案：沿用 unit_stats_tests 落库的合成数据（在册 11/库存 8/部署 3）
    stats = srv.assets_stats_view()
    text = srv.feishu_report_text(stats, now=_dt(2026, 10, 21, 9, 0))
    check("日报文案：库存合计 + 分单位 + 口径说明",
          "库存：8 台（上海办公室 5 台 + 潍坊办公室 3 台）" in text
          and "服务商等其他单位持有不计库存" in text)
    check("日报文案：在册 / 部署 / 归档不计在册",
          "在册硬件：11 台（一线部署 3 台；另有归档 1 台不计在册）" in text)
    check("日报文案：维修率 / 损坏率（分母在册）",
          "维修率：18.2%（维修中 1 台 + 待维修 1 台，分母在册 11 台）" in text
          and "损坏率：27.3%（异常 1 台 + 维修中 1 台 + 待维修 1 台，分母在册 11 台）" in text)
    check("日报文案：数据时间取最近一次成功同步",
          "数据时间：" in text and "（飞书台账最近一次成功同步）" in text
          and "同步异常" not in text)
    srv.asset_store.set_error("devices", "boom")   # 同步异常须如实提示
    text2 = srv.feishu_report_text(srv.assets_stats_view(), now=_dt(2026, 10, 21, 9, 0))
    check("同步异常时日报如实提示（不吞错）",
          "⚠ 飞书同步异常" in text2 and "boom" in text2)

    # webhook 成功/失败判定：monkeypatch urlopen，不发真实请求
    calls = []

    class _Resp:
        status = 200

        def read(self, n=-1):
            return b'{"code":0,"msg":"success"}'

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def _ok_open(req, timeout=None):
        calls.append(req)
        return _Resp()

    def _fail_open(req, timeout=None):
        raise srv.URLError("no route to host")

    orig_open, orig_hook = srv.urlopen, srv.FEISHU_WEBHOOK
    srv.FEISHU_WEBHOOK = "https://example.invalid/hook"
    try:
        srv.urlopen = _ok_open
        ok1, code1, _ = srv._feishu_webhook_post("hello", attempts=2)
        calls_after_post = len(calls)
        out = srv.run_feishu_report("manual")
        srv.urlopen = _fail_open
        ok2, code2, _ = srv._feishu_webhook_post("hello", attempts=2)
    finally:
        srv.urlopen, srv.FEISHU_WEBHOOK = orig_open, orig_hook
    check("webhook 成功判定（HTTP 200 + 业务码 0，不重试）",
          ok1 is True and code1 == 200 and calls_after_post == 1
          and len(calls) == 2)   # run_feishu_report 复用同一判定链路再推一条
    check("webhook 失败退避重试后如实返回失败（绝不抛出）",
          ok2 is False and code2 is None)
    check("run_feishu_report：记录状态并回显全文",
          out["ok"] is True and out["reason"] == "manual"
          and "在册硬件：11 台" in out["text"]
          and srv._feishu_report_state["last"]["ok"] is True)

    # 签名校验（群机器人开启签名时必需，实测未签名会被拒 code=19021）
    import base64 as _b64, hashlib as _hl, hmac as _hmac, time as _time
    ts = "1761021000"
    expect_sign = _b64.b64encode(_hmac.new(f"{ts}\n/test-secret".encode(),
                                         digestmod=_hl.sha256).digest()).decode()
    check("签名算法与飞书官方一致（key=timestamp\nsecret，空消息体）",
          srv._feishu_webhook_sign("/test-secret", ts) == expect_sign)

    bodies = []

    def _sign_open(req, timeout=None):
        bodies.append(json.loads(req.data.decode("utf-8")))
        return _Resp()

    class _Resp19021(_Resp):
        def read(self, n=-1):
            return b'{"code":19021,"msg":"sign match fail or timestamp is not within one hour"}'

    def _open_19021(req, timeout=None):
        bodies.append(json.loads(req.data.decode("utf-8")))
        return _Resp19021()

    orig_secret = srv.FEISHU_WEBHOOK_SECRET
    srv.FEISHU_WEBHOOK = "https://example.invalid/hook"
    try:
        srv.FEISHU_WEBHOOK_SECRET = ""     # 未配密钥：不带签名字段
        srv.urlopen = _sign_open
        ok_ns, _, _ = srv._feishu_webhook_post("hi", attempts=1)
        unsigned = dict(bodies[-1])
        srv.FEISHU_WEBHOOK_SECRET = "/test-secret"
        ok3, _, _ = srv._feishu_webhook_post("hi", attempts=2)
        n_ok = len(bodies)
        srv.urlopen = _open_19021
        ok4, code4, detail4 = srv._feishu_webhook_post("hi", attempts=3)
        n_19021 = len(bodies) - n_ok
    finally:
        srv.urlopen, srv.FEISHU_WEBHOOK_SECRET = orig_open, orig_secret
        srv.FEISHU_WEBHOOK = orig_hook
    sent = bodies[n_ok - 1]   # 最后一次成功签名推送的载荷
    check("未配密钥时不带 timestamp/sign 字段（关闭签名校验的机器人可用）",
          ok_ns is True and unsigned.get("msg_type") == "text"
          and "sign" not in unsigned and "timestamp" not in unsigned)
    check("配密钥后载荷带 timestamp+sign（官方算法）",
          ok3 is True and sent.get("timestamp") and sent.get("sign")
          and sent["msg_type"] == "text"
          and abs(int(sent["timestamp"]) - int(_time.time())) < 60)
    check("code=19021 → 明示签名问题且不空转重试（1 次即停）",
          ok4 is False and code4 == 200 and "19021" in detail4
          and "签名" in detail4 and n_19021 == 1)

    # 面板标题：env 未设 → 兑底值；模板标签与页头一致
    title = srv.panel_title()
    check("面板标题：env 未设时用兑底值（台账全称）",
          title == (srv.PANEL_TITLE_ENV or srv.PANEL_TITLE_FALLBACK)
          and title == "设备库存与流转管理")
    doc = srv.INDEX.format(css="", js="", title=title)
    check("首页模板：<title> 与 <h1> 同源一致",
          f"<title>{title}</title>" in doc and f"<h1>{title}</h1>" in doc)
    check("Base 名称浅层解析（lark 返回 JSON）",
          srv._find_base_name({"data": {"app": {"name": "设备库存与流转管理"}}})
          == "设备库存与流转管理"
          and srv._find_base_name({"data": {"tables": [{"name": "设备明细"}]}}) is None)


def unit_top5_tests(srv):
    print("[5] 前 5 服务商版本占比单测（KVWO-107：恰 5 家/并列/share 舍入）")
    # 6 单位在册数：A 6 / B 5 / C 5 / D 4 / E 2 / F 2（B/C、E/F 并列，F 应被截断）
    devices = {}
    plan = [("服务商A", 6), ("服务商B", 5), ("服务商C", 5),
            ("服务商D", 4), ("服务商E", 2), ("服务商F", 2)]
    seq = 0
    for unit, count in plan:
        for _ in range(count):
            seq += 1
            devices[f"recT{seq}"] = {
                "device_no": f"EP-{seq:06d}", "device_type": "EgoPortal-Stereo-V0",
                "status": "正常", "unit": unit, "accessories": None, "note": None,
                "software_version": None, "modified_by": None,
                "updated_at": "2026-10-01T00:00:00.000+08:00"}
    srv.asset_store.replace_all("devices", devices, synced_at=1.0)
    # 快照：服务商A 前 3 台 v2.0.0 + 第 4 台 v2.1.0 在线（大写混杂），其余全离线
    snap_hosts = [{"hostname": f"ep-{i:06d}", "version": "v2.0.0"} for i in (1, 2, 3)]
    snap_hosts.append({"hostname": "EP-000004", "version": "v2.1.0"})
    orig = srv.get_snapshot
    srv.get_snapshot = lambda: (snap_hosts, 0.0)
    try:
        top5 = srv.assets_versions_view()["by_provider_top5"]
        units_top = {u["unit"]: u for u in srv.assets_units_view()}
    finally:
        srv.get_snapshot = orig
    check("恰 5 家：在册数降序、并列按单位名稳定（服务商F 2 台被截断）",
          [t["unit"] for t in top5] == ["服务商A", "服务商B", "服务商C",
                                       "服务商D", "服务商E"]
          and [t["total"] for t in top5] == [6, 5, 5, 4, 2])
    check("服务商A 分桶：探测版本计数降序，未探测归「未获取」，share 两位小数",
          top5[0]["versions"] == [{"version": "v2.0.0", "count": 3, "share": 0.5},
                                 {"version": "未获取", "count": 2, "share": 0.33},
                                 {"version": "v2.1.0", "count": 1, "share": 0.17}])
    check("未接入单位整桶「未获取」share=1.0；计数合计恒等于在册数",
          top5[1]["versions"] == [{"version": "未获取", "count": 5, "share": 1.0}]
          and all(sum(v["count"] for v in t["versions"]) == t["total"]
                  for t in top5))
    check("使用率与 top5 口径同源（服务商A 4/6）",
          units_top["服务商A"]["online"] == 4
          and abs(units_top["服务商A"]["usage_rate"] - 4 / 6) < 5e-5
          and units_top["服务商F"]["usage_rate"] == 0.0)


def unit_tests():
    import server as srv
    unit_parse_tests(srv)
    unit_store_tests(srv)
    unit_stats_tests(srv)
    unit_detail_tests(srv)
    unit_report_tests(srv)
    unit_top5_tests(srv)


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
    print(f"[6] 在线校验（{base}）")
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

    # stats 口径与直查一致（KVWO-97：库存 = 上海+潍坊，服务商不计）
    stats = get_json(f"{base}/api/assets/stats")
    n = len(dev_parsed)
    arch_n = len(parsed["archived"])
    inv_units = ["上海办公室", "潍坊办公室"]   # 提出人确认口径（env 可扩展）
    inv_by_unit = {u: sum(1 for r in dev_parsed.values() if r["unit"] == u)
                   for u in inv_units}
    inv_total = sum(inv_by_unit.values())
    inv_b = sum(1 for r in dev_parsed.values() if "未使用" in (r["note"] or ""))
    repairing = sum(1 for r in dev_parsed.values() if r["status"] in ("维修中", "待维修"))
    damaged = sum(1 for r in dev_parsed.values() if r["status"] in ("异常", "维修中", "待维修"))
    check("stats.registered_total", stats["registered_total"] == n)
    check("stats.archived_total", stats["archived_total"] == arch_n)
    check("stats.inventory 口径（上海+潍坊分单位，服务商不计）",
          stats["inventory"]["total"] == inv_total
          and stats["inventory"]["by_unit"] == inv_by_unit
          and stats["inventory"]["units"] == inv_units
          and stats["inventory"]["unused_note"] == inv_b)
    check("stats.deployed = 在册 − 库存", stats["deployed"] == n - inv_total)
    check("stats.idle 形状与台账列表联动（KVWO-97 需求 4）",
          stats["idle"]["threshold_days"] >= 1
          and (stats["idle"]["total"] is None
               or stats["idle"]["total"] == sum(1 for i in listing["items"]
                                               if i.get("idle") is True)))
    check("stats.repair_rate / damage_rate",
          abs(stats["repair_rate"] - repairing / n) < 5e-4
          and abs(stats["damage_rate"] - damaged / n) < 5e-4)
    check("stats.by_status/by_type/by_unit 与直查一致",
          stats["by_status"] == by_status
          and stats["by_type"] == {k: sum(1 for r in dev_parsed.values() if r["device_type"] == k)
                                   for k in {r["device_type"] or "" for r in dev_parsed.values()}}
          and stats["by_unit"] == {k: sum(1 for r in dev_parsed.values() if r["unit"] == k)
                                   for k in {r["unit"] or "" for r in dev_parsed.values()}})
    print(f"    · 基线参考：在册 {n}、归档 {arch_n}、库存 {inv_total}"
          f"（{' + '.join(f'{u} {c}' for u, c in inv_by_unit.items())}）、"
          f"部署 {n - inv_total}、维修率 {repairing}/{n}、损坏率 {damaged}/{n}")

    # 日报推送（KVWO-97）：状态端点 + 手动触发（enabled 时真推 webhook）
    rep = get_json(f"{base}/api/assets/report")
    check("report 状态端点：时刻/库存口径/标题齐备，不回显 webhook",
          set(rep) >= {"enabled", "times", "next_run_at", "last",
                       "inventory_units", "panel_title"}
          and rep["inventory_units"] == inv_units and rep["times"]
          and rep["panel_title"] and "webhook" not in rep)
    if rep["enabled"]:
        push_req = urllib.request.Request(f"{base}/api/assets/report/run",
                                          method="POST", data=b"")
        with urllib.request.urlopen(push_req, timeout=120) as resp:
            push = json.loads(resp.read())
        txt = push.get("text") or ""
        check("手动推送成功且文案齐全（四项指标+数据时间+分单位）",
              push.get("ok") is True
              and all(k in txt for k in ("库存：", "在册硬件：", "维修率：",
                                        "损坏率：", "数据时间："))
              and f"上海办公室 {inv_by_unit['上海办公室']} 台" in txt
              and f"潍坊办公室 {inv_by_unit['潍坊办公室']} 台" in txt
              and f"一线部署 {n - inv_total} 台" in txt)
        rep2 = get_json(f"{base}/api/assets/report")
        check("推送结果状态可查（last.ok）",
              (rep2.get("last") or {}).get("ok") is True
              and rep2["last"]["reason"] == "manual")
    else:
        push_req = urllib.request.Request(f"{base}/api/assets/report/run",
                                          method="POST", data=b"")
        try:
            with urllib.request.urlopen(push_req, timeout=60) as resp:
                push_code = resp.status
        except urllib.error.HTTPError as exc:
            push_code = exc.code
        check("未配置 webhook 时手动推送如实拒绝（400，不盲发）", push_code == 400)

    # 面板更名（KVWO-97）：浏览器标签与页头一致，均为台账全称
    title = rep["panel_title"]
    _, _, body = get(base + "/")
    doc = body.decode("utf-8", "replace")
    check("面板标题：浏览器标签与页头一致（台账全称）",
          f"<title>{title}</title>" in doc and f"<h1>{title}</h1>" in doc)

    # units 聚合
    units = get_json(f"{base}/api/assets/units")["units"]
    units_total = sum(u["total"] for u in units)
    check("units 覆盖直查全部单位（含 11 个）",
          {u["unit"] for u in units} == {r["unit"] or "(未填写)" for r in dev_parsed.values()}
          and len(units) == len({r["unit"] or "(未填写)" for r in dev_parsed.values()}))
    check("units 设备数合计 = 在册", units_total == n)
    u0 = next(u for u in units if u["unit"] == "上海办公室")
    check("上海办公室聚合与库存分单位一致", u0["total"] == inv_by_unit["上海办公室"])

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
    # by_provider_top5（KVWO-107 第六波）：与直查在册分单位排序一致、share 两位小数
    top5 = ver.get("by_provider_top5") or []
    n_by_unit = {}
    for r in dev_parsed.values():
        k = r["unit"] or "(未填写)"
        n_by_unit[k] = n_by_unit.get(k, 0) + 1
    want_top5 = sorted(n_by_unit.items(), key=lambda kv: (-kv[1], kv[0]))[:5]
    check("by_provider_top5 与直查一致（恰 5 家/在册降序/并列按名）",
          [(t["unit"], t["total"]) for t in top5] == want_top5
          and all(sum(v["count"] for v in t["versions"]) == t["total"]
                  for t in top5)
          and all(v["share"] == round(v["count"] / t["total"], 2)
                  for t in top5 for v in t["versions"]))
    hosts = get_json(f"{base}/api/hosts")
    online_hosts = {h["hostname"].casefold() for h in hosts["hosts"]}
    check("versions.matched ⊆ 在线快照",
          set(ver["match"]["matched"]) <= online_hosts)
    # units.online / usage_rate 与快照在线数一致（KVWO-107 第六波）
    bad_ur = []
    for u in units:
        want_online = sum(1 for i in listing["items"]
                          if (i["unit"] or "(未填写)") == u["unit"]
                          and i["device_online"] is True)
        want_rate = round(want_online / u["total"], 4) if u["total"] else None
        if u.get("online") != want_online or u.get("usage_rate") != want_rate:
            bad_ur.append((u["unit"], u.get("online"), want_online,
                           u.get("usage_rate"), want_rate))
    check("units.online/usage_rate 与快照在线数一致", not bad_ur, str(bad_ur[:3]))

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
