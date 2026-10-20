# 维修面板后端（KVWO-78 / KVWO-79）

数据源为飞书多维表（唯一维护入口，面板**只读**，不写回）。后端用 `lark-cli`
定时全量拉取同步到本地 SQLite（`record_id` 主键，幂等镜像），前端读本地 REST API。

## 配置（环境变量）

全部有默认值，零配置即可跑；生产放 `/etc/frp-monitor.env`（systemd EnvironmentFile）。

| 变量 | 默认 | 说明 |
|---|---|---|
| `FRP_MONITOR_REPAIR_SYNC` | `1` | 同步开关；`0` 关闭（API 仍在，只服务已入库数据） |
| `FRP_MONITOR_REPAIR_INTERVAL` | `300` | 同步间隔（秒），下限 60 |
| `FRP_MONITOR_REPAIR_LARK_CMD` | `lark-cli` | lark-cli 命令（可写绝对路径）；lark-cli 凭证由其自身配置目录管理，与服务解耦 |
| `FRP_MONITOR_REPAIR_BASE_TOKEN` | `V0YwbaOzya0ADBsO2wSc8M9Cntf` | 飞书 Base token |
| `FRP_MONITOR_REPAIR_TABLE_ID` | `tblf1nlwkHhMX1V9` | 飞书表 ID |
| `FRP_MONITOR_REPAIR_VIEW` | `vewVBaWePF` | 视图 ID（仅用于拼飞书原记录链接） |
| `FRP_MONITOR_REPAIR_DB` | `<server.py 所在目录>/repairs.sqlite` | 本地 SQLite 路径 |
| `FRP_MONITOR_REPAIR_ATTACH_DIR` | `<server.py 所在目录>/repair-attachments` | 附件下载目录 |

lark-cli 不可用 / 认证失败 / 网络失败：同步只记录错误状态（`last_sync_error`），
**已入库数据继续服务，不影响面板其他页签**；恢复后下轮同步自动追平。

## API（与前端契约，变更需同步父 issue）

- `GET /api/repairs` — 全量列表，按阶段固定顺序（待接收 → 损坏原因确认中 → 维修中 →
  待发出确认 → 待发出 → 已发出 → 已完结）+ 创建时间倒序。筛选参数：`stage`、`type`
  （单目/双目/其他）、`unit`（模糊）、`owner`（精确）、`q`（设备编号/问题描述模糊）。
- `GET /api/repairs/stats` — `{total, by_stage, active_count, last_sync_at, ...}`。
- `GET /api/repairs/{record_id}` — 单条详情，404 若未知。
- `POST /api/repairs/sync` — 手动触发一次同步（验证/运维用）。

每条记录字段（snake_case，空值一律 `null`，前端显示「—」，数据不合成）：

```
record_id, device_no, device_type, parts[], unit, stage, problem,
contact_name, contact_phone, recv_address, return_address,
tracking_no_in, tracking_no_out, owner_name,
last_followup_at, last_followup_note, created_at,
attachments[{file_token, name, size, url}],   # url 为本地静态路径，已下载才有
feishu_url,                                   # 飞书原记录链接
device_online,                                # true 在线 / false 登记未上线 / null 未匹配
synced_at
```

列表响应额外带 `last_sync_at`（最近一次成功，数据新鲜度）、`last_sync_attempt_at`、
`last_sync_error`、`sync_interval`、`stage_order`。

`device_online` 判定：设备编号 casefold 后在 frps 当前快照 → `true`；在设备台账
（MySQL devices 表）但未上线 → `false`；两者都无 → `null`（显示「—」）。

## 附件

同步时经 `lark-cli drive +preview --type source_file` 下载到本地目录（多维表附件
Drive 直下接口 403，预览通道可行），按 `file_token` 命名，存在即跳过；单个失败只记
日志，该附件 `url=null`，下轮自动补下。访问路径 `/repair-attachments/<token>/<文件名>`
（Content-Type 按原始文件名推断；token 限定 `[A-Za-z0-9_-]`，无目录穿越）。

## 验证

```
python3 verify_repairs.py                          # 解析单测（无需服务）
python3 verify_repairs.py --base http://127.0.0.1:9100   # 全量（需 server.py 运行中）
```

在线校验覆盖：与 lark-cli 直查逐字段比对、二次同步幂等、筛选、统计、详情、
附件可达、device_online 三态与台账一致。本地起环境可用 `verify_mock_frps.py`
模拟 frps、`db/docker-compose.yml` 起 MySQL。

## 部署注意（124.222.147.63）

- 生产需安装 lark-cli 并完成一次用户级认证（凭证过期 → 同步错误状态，面板其余
  页签不受影响，恢复认证后自动追平）。
- `repairs.sqlite` / `repair-attachments/` 落在 `/opt/frp-monitor/`，已加 .gitignore。
