# 资产多表同步与聚合 API（KVWO-78 / KVWO-83；KVWO-97 库存口径修正+日报推送+面板更名）

基于维修面板（KVWO-79，见 [REPAIRS.md](REPAIRS.md)）的 lark-cli 同步机制扩展：
同一飞书 Base 增量接入 5 张资产表，全量拉取 + `record_id` 幂等镜像到本地
SQLite，指标全部由同步数据实时计算（不落死数）。面板只读，不写回飞书。

## 同步的表

| 键 | 表 | table_id | 记录数（2026-10-20） | 用途 |
|---|---|---|---|---|
| `devices` | 设备明细 | `tblg1YoctJNcZskM` | 248 | 在册台账（在册指标唯一来源） |
| `archived` | 设备归档数据表 | `tblAeG2cGJ8SjQzR` | 69 | 归档/已拆解，仅存量展示，不计入在册 |
| `actions` | 异常处理流转 | `tblri9gaBX4zVXzX` | 48 | 单设备 360° 处理流水 |
| `parts` | 配件核对明细 | `tblOLbWKpDlcJkDi` | 62 | 单设备 360° 配件核对 |
| `transfers` | 流转记录 | `tblxmVpjAHeATuMU` | 25 | 按单位关联的流转/签收 |

「损坏原因确认」表（`tbldvaccjy2dVnOX`）当前 0 条，未接入。

**列序动态对齐**：解析按 lark-cli 返回的 `fields` 表头（中文字段名）对齐，
不硬编码列序——归档表与设备明细列序不同（实测），字段调整列序不影响解析；
字段重命名/删除时该字段退化为 `null`。单页拉取带退避重试（lark-cli 瞬时
限流不让整表本轮失败）；重试耗尽该表记错误状态，**其余表与维修页签不受
影响**（按表隔离），下轮自动追平。

## 配置（环境变量）

复用维修面板的 `FRP_MONITOR_REPAIR_LARK_CMD` / `FRP_MONITOR_REPAIR_BASE_TOKEN`
（同一 Base、同一 lark-cli）；资产侧独立开关/间隔/库路径：

| 变量 | 默认 | 说明 |
|---|---|---|
| `FRP_MONITOR_ASSET_SYNC` | `1` | 同步开关；`0` 关闭（API 仍服务已入库数据） |
| `FRP_MONITOR_ASSET_INTERVAL` | `300` | 同步间隔（秒），下限 60 |
| `FRP_MONITOR_ASSET_BASE_TOKEN` | 同维修面板 | 飞书 Base token（默认同 Base） |
| `FRP_MONITOR_ASSET_DB` | `<server.py 所在目录>/assets.sqlite` | 本地 SQLite 路径 |
| `FRP_MONITOR_INVENTORY_UNITS` | `上海办公室,潍坊办公室` | 库存口径单位清单（逗号分隔，可扩展；提出人 2026-10-20 确认上海+潍坊均为公司库存未使用，服务商等其他单位持有不计库存） |
| `FRP_MONITOR_FEISHU_WEBHOOK` | 空（关闭） | 飞书群机器人 webhook（KVWO-97 资产日报推送）；**敏感凭据，只落在服务器 env 配置，勿写入仓库/文档；置空/未配置 = 关闭推送** |
| `FRP_MONITOR_FEISHU_WEBHOOK_SECRET` | 空（不签名） | 机器人签名密钥：群机器人开启「签名校验」时必配（未签名推送被拒 code=19021，2026-10-20 实测已遇到）；同敏感凭据仅落服务器 env；未开启签名校验不用配 |
| `FRP_MONITOR_REPORT_TIMES` | `09:00` | 日报推送时刻（服务器本地时间，逗号分隔 HH:MM 支持多时刻；调度模式同采集时长 `FRP_MONITOR_COLLECT_TIMES`） |
| `FRP_MONITOR_PANEL_TITLE` | 空 | 面板标题覆盖（浏览器标签与页头）；未设时启动后台线程用 lark-cli 查 Base 全称 best-effort，失败用兑底值「设备库存与流转管理」 |

附件（处理流水附件/配件照片/签收证明）只入库元数据（file_token/name/size），
**默认不主动下载**（体积大、资产页签验收不要求附件可访问）；本地若已存在
同名 token 文件（如经维修表通道下载过）则解析出 `/repair-attachments/` URL，
否则 `url=null`。

## API（与前端契约，变更需同步父 issue）

所有接口只读；筛选与统计由同步数据实时计算；缺失字段 `null`（前端「—」，
数据不合成）。**设备编号匹配规则全库统一：`strip().casefold()` 大小写
不敏感**（台账存在小写 `ep-000298`、异常表 `EP-000289` 等混杂情况；
frps 主机名本身就是小写 `ep-000xxx`）。

- `GET /api/assets` — 台账列表。筛选：`status` / `type`（精确）、`unit`（精确）、
  `q`（设备编号/去向备注模糊，大小写不敏感）、`include_archived=1`（追加归档，
  带 `archived:true`）。每条含台账全字段 + `feishu_url` + `device_online`
  （true 在线 / false 台账登记未上线 / null 未匹配）+ `detected_version`
  （探测版本：在线取当前探测值，离线取最后已知，否则 null）+
  `active_repairs`（关联未完结维修记录数，大小写不敏感）。
- `GET /api/assets/stats` — `{registered_total, archived_total,
  inventory:{total, by_unit, units, unused_note}, deployed, by_status, by_type,
  by_unit, repair_rate, damage_rate, last_sync_at, …}`。
  口径（KVWO-97 修正）：在册=设备明细；**库存=所属单位
  ∈ `units`（默认上海办公室+潍坊办公室，分单位明细 `by_unit`，配置单位
  无设备时计 0，服务商等其他单位持有不计库存）**；`unused_note`
  （备注含「未使用」）仅作参考；deployed=在册−库存；维修率=
  (维修中+待维修)/在册；损坏率=(异常+维修中+待维修)/在册；归档不计入
  在册。对账基线（2026-10-20）：在册 248、库存 149（上海 94+潍坊 55）、
  部署 99（随台账实时变化）。`tables` 带按表同步状态。
- `GET /api/assets/units` — 单位聚合 `{unit, total, normal_rate, repairing
  (=维修中+待维修), abnormal(=异常), last_updated}[]`，按设备数倒序；
  单位详情用 `/api/assets?unit=…` 过滤。
- `GET /api/assets/versions` — `detected_by_version`（探测版本计数，空版本
  归「未获取」桶）、`ledger_by_version`（台账软件版本计数，当前全空如实返回
  `{}`）+ `ledger_no_version`、`match:{matched, ledger_missing, detected_only}`
  与 `match_counts`。匹配按在册台账（归档不计入 ledger_missing；归档设备
  在线也不算 detected_only）。
- `GET /api/assets/{device_id}` — 单设备 360°：台账全字段（在册查不到回落
  归档，带 `archived` 标记；均无 404）+ `repairs[]`（该设备全部维修记录，
  倒序）+ `actions[]`（处理流水：设备编号一致 **或** `linked_records` 命中该
  设备任一维修记录，双路）+ `parts_checks[]`（配件核对，编号一致）+
  `transfers[]`（流转记录按单位关联：相等或互为包含——该列为自由文本，
  存在「构元服务商」vs「潍坊构元服务商」）。
- `POST /api/assets/sync` — 手动触发一次 5 表同步（验证/运维用）。
- `GET /api/assets/report` — 资产日报推送状态（KVWO-97）：
  `{enabled, times, next_run_at, last, inventory_units, panel_title}`；
  webhook 地址属敏感凭据，不回显。
- `POST /api/assets/report/run` — 手动触发一次日报推送（测试验收用，
  与定时调度串行互斥）；返回推送结果与消息全文便于核对文案；
  未配置 webhook 时 400 拒绝（不盲发）。

## 资产日报定时飞书推送（KVWO-97）

服务端自行定时推送资产核心指标到飞书群机器人，不依赖外部 agent 轮询：

- 调度模式与采集时长（19:00/21:00）一致：每轮从墙上时钟重推下一次时刻，
  进程重启自然对齐；默认每日 **09:00（服务器本地时间）**，
  `FRP_MONITOR_REPORT_TIMES` 支持多时刻。
- 推送内容（中文 text）：库存（合计=分单位明细，注明口径：公司库存，
  服务商持有不计）、在册硬件（附归档不计在册、一线部署）、维修率/
  损坏率（含分子分母）、数据时间（最近一次飞书成功同步；同步异常时
  如实提示 ⚠）——指标全部由 `/api/assets/stats` 同一数据源实时计算。
- 推送在独立线程（`feishu-report`）、带退避重试（3 次，签名失败 19021
  等确定性错误不空转重试）；失败只记日志与
  状态（`GET /api/assets/report` / stdout），**不影响面板服务与飞书同步**；
  webhook 置空即整体关闭（调度线程不启动）。机器人开启「签名校验」时配
  `FRP_MONITOR_FEISHU_WEBHOOK_SECRET`（飞书官方算法：
  key=`timestamp\nsecret`、空消息体，载荷附带 timestamp+sign）。
- 面板更名（同需求）：浏览器标签与页头一致，默认取飞书台账
  Base（`V0YwbaOzya0ADBsO2wSc8M9Cntf`）全称，`FRP_MONITOR_PANEL_TITLE`
  可覆盖。

## 验证

```
python3 verify_assets.py                          # 单测（无需服务，无需 lark-cli）
python3 verify_assets.py --base http://127.0.0.1:9100   # 全量（需 server.py 运行中）
```

单测覆盖：列序打乱/表间列序互异下的按名对齐、select（两种形态）/多选/user/
datetime/attachment/link/number/空值解析、SQLite 幂等与按表错误隔离、
stats/units/versions 口径（含 KVWO-97 库存=上海+潍坊分单位）、360° 双路
关联；资产日报（单位清单/推送时刻解析、下一调度时刻、文案四项指标+
数据时间+同步异常提示、webhook 成功/失败判定 monkeypatch、面板标题
<title>/<h1> 同源）。在线校验与 lark-cli 直查逐字段比对、二次同步幂等、
筛选/统计/单位/版本/详情/404、device_online/detected_version/
active_repairs 与既有 API 交叉一致；日报状态端点/手动推送文案与首页
标题一致性（webhook 已配置时手动推送会真发飞书群，验收时注意）。
本地可用 `verify_mock_frps.py` 模拟 frps（版本探测依赖设备可达，mock 下
版本为「未获取」桶，属如实返回）。前端另有 `verify_assets_ui.js` /
`verify_assets_real_api.js`（node，先 `python3 -c "import server"` 导出
`/tmp/panel_test.js`）：mock 基线（在册 248/库存 149=上海 94+潍坊 55/
部署 99）与真实 API 路径、断供降级。

## 部署注意（124.222.147.63）

- 与维修面板共用 lark-cli 凭证（见 REPAIRS.md）；同步失败按表记错误状态，
  面板其余页签不受影响，恢复后自动追平。
- `assets.sqlite` 落在 `/opt/frp-monitor/`，已加 .gitignore。
- 日报推送（KVWO-97）：在 `/etc/frp-monitor.env` 配置
  `FRP_MONITOR_FEISHU_WEBHOOK`（群机器人地址，敏感凭据勿入仓库）后重启；
  **机器人若开启「签名校验」需同配 `FRP_MONITOR_FEISHU_WEBHOOK_SECRET`
  （签名密钥）**；`POST /api/assets/report/run` 可立即验推一次；时刻用
  `FRP_MONITOR_REPORT_TIMES` 调整（默认 09:00）。
