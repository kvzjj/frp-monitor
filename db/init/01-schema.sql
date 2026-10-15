-- frp-monitor 历史持久化表结构（首次启动自动执行）
USE frp_monitor;

-- 1) 设备登记表：面板自动登记观察到的设备；expected_* / site / owner 可人工补充
CREATE TABLE IF NOT EXISTS devices (
  hostname        VARCHAR(64)  NOT NULL PRIMARY KEY,   -- ep-000162（frps 代理名，小写）
  device_id       VARCHAR(32)  NOT NULL DEFAULT '',    -- 门户 device_id（如 200）
  device_name     VARCHAR(64)  NOT NULL DEFAULT '',    -- EP-000162
  release_version VARCHAR(32)  NOT NULL DEFAULT '',    -- 最近观测到的固件版本
  ssh_port        INT          NULL,                   -- 实际观测 FRP SSH 端口（20xxx）
  web_port        INT          NULL,                   -- 实际观测 FRP Web 端口（30xxx）
  hardware        VARCHAR(32)  NOT NULL DEFAULT '',    -- V0 / V1 / rk3588 / amd64（人工维护；空时由 hw_model 自动推断）
  hw_model        VARCHAR(64)  NOT NULL DEFAULT '',    -- 面板自动探测的 CPU/型号（如 RK3588S OPi 5 Pro）
  ip              VARCHAR(64)  NOT NULL DEFAULT '',    -- 面板自动探测的有线 IPv4（随探测刷新）
  site            VARCHAR(64)  NOT NULL DEFAULT '',    -- 站点（可人工维护）
  owner           VARCHAR(64)  NOT NULL DEFAULT '',    -- 负责人（可人工维护）
  first_seen_at   DATETIME     NULL,                   -- 首次观测（NULL = 已预登记未上线）
  last_seen_at    DATETIME     NULL,                   -- 最近一次在线
  retired         TINYINT(1)   NOT NULL DEFAULT 0,     -- 退役标记（人工）
  notes           VARCHAR(255) NOT NULL DEFAULT '',
  updated_at      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 2) 在线状态时序快照：默认每 5 分钟一行（含已知但离线设备的 online=0 行），用于在线率
CREATE TABLE IF NOT EXISTS host_status (
  id              BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  hostname        VARCHAR(64) NOT NULL,
  ts              DATETIME    NOT NULL,
  online          TINYINT(1)  NOT NULL DEFAULT 1,
  ssh_port        INT         NULL,
  web_port        INT         NULL,
  connections     INT         NOT NULL DEFAULT 0,
  frpc_started_at DATETIME    NULL,                    -- frpc 本次上线时刻（frps lastStartTime）
  release_version VARCHAR(32) NOT NULL DEFAULT '',
  version_error   VARCHAR(255) NOT NULL DEFAULT '',    -- 版本探测失败原因
  device_id       VARCHAR(32) NOT NULL DEFAULT '',
  device_name     VARCHAR(64) NOT NULL DEFAULT '',
  KEY idx_host_ts (hostname, ts),
  KEY idx_ts (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 3) 事件流：上下线 / 版本变更 / 新设备登记，长期保留（量小）
CREATE TABLE IF NOT EXISTS host_events (
  id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  hostname   VARCHAR(64) NOT NULL,
  ts         DATETIME    NOT NULL,
  event_type VARCHAR(32) NOT NULL,      -- online | offline | version_change | first_seen
  old_value  VARCHAR(255) NOT NULL DEFAULT '',
  new_value  VARCHAR(255) NOT NULL DEFAULT '',
  detail     VARCHAR(512) NOT NULL DEFAULT '',
  KEY idx_host_ts (hostname, ts),
  KEY idx_ts_type (ts, event_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 4) 刷新心跳：每次拉取 frps 一行，用于监控 monitor 自身健康 / 拉取失败时段
CREATE TABLE IF NOT EXISTS refresh_batches (
  id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  ts          DATETIME NOT NULL,
  total       INT NOT NULL DEFAULT 0,
  online      INT NOT NULL DEFAULT 0,
  duration_ms INT NOT NULL DEFAULT 0,
  error       VARCHAR(255) NOT NULL DEFAULT '',
  KEY idx_ts (ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 5) 设备采集时长：每天 19:00/21:00（服务器本地时间）各拉一次当时在线设备的当日
--    累计采集时长；同日以 (hostname, stat_date) 唯一，后一次成功覆盖前一次，
--    失败只记 error 不动已有成功值（启动时也会幂等建表，见 Store._migrate）
CREATE TABLE IF NOT EXISTS collect_duration (
  hostname     VARCHAR(64)  NOT NULL,               -- ep-000162（frps 代理名，小写）
  stat_date    DATE         NOT NULL,               -- 统计日（服务器本地时区）
  duration_sec INT          NULL,                   -- 当日累计采集时长（秒；NULL = 尚无成功值）
  sessions     INT          NOT NULL DEFAULT 0,     -- 当日采集段数（设备 total_session_count）
  run_tag      VARCHAR(16)  NOT NULL DEFAULT '',    -- 最近一次成功写入的批次：19:00 / 21:00 / manual
  error        VARCHAR(255) NOT NULL DEFAULT '',    -- 最近一次拉取失败原因（成功时清空）
  pulled_at    DATETIME     NULL,                   -- 最近一次成功写入时间
  UNIQUE KEY uq_host_date (hostname, stat_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 6) 批量升级：一次批量操作一行；kind ∈ precheck/download/prepare/install/
--    verify/cleanup/upgrade（全流程）；每日维护线程按 30 天保留窗口清理
CREATE TABLE IF NOT EXISTS upgrade_jobs (
  id          INT          NOT NULL PRIMARY KEY,    -- 面板分配（内存计数器）
  kind        VARCHAR(16)  NOT NULL,
  version     VARCHAR(32)  NOT NULL DEFAULT '',     -- 目标版本（precheck/verify 可为空）
  params      TEXT         NULL,                    -- 创建参数快照（不含凭据）
  status      VARCHAR(16)  NOT NULL DEFAULT 'running',  -- running / completed
  total       INT          NOT NULL DEFAULT 0,      -- 子任务（设备）数
  created_at  DATETIME     NOT NULL,
  finished_at DATETIME     NULL,
  KEY idx_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 7) 批量升级逐台子任务：阶段机 precheck→download→prepare→install→verify→
--    cleanup；任一阶段失败即停该设备，不影响其他设备；blocked = 护栏拦截
CREATE TABLE IF NOT EXISTS upgrade_tasks (
  id          INT          NOT NULL PRIMARY KEY,
  job_id      INT          NOT NULL,
  hostname    VARCHAR(64)  NOT NULL,
  ssh_port    INT          NULL,                    -- 以面板设备记录为准（SOP 坑 #8）
  stage       VARCHAR(16)  NOT NULL DEFAULT '',     -- 当前/最后阶段
  status      VARCHAR(16)  NOT NULL DEFAULT 'queued', -- queued/running/success/failed/blocked
  error       VARCHAR(500) NOT NULL DEFAULT '',
  log         LONGTEXT     NULL,                    -- 逐台完整日志（含 install.log 增量回传）
  started_at  DATETIME     NULL,
  finished_at DATETIME     NULL,
  KEY idx_job (job_id),
  KEY idx_host (hostname)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
