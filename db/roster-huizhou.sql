-- 惠州服务商设备归属清单（2026-09-12）
-- 在线设备仅更新 site/owner；未上线设备预登记（first_seen_at/last_seen_at 为 NULL）。
-- 导入命令（必须带 --default-character-set=utf8mb4，否则客户端默认 latin1 会双重编码）：
--   docker exec -i frp-monitor-mysql mysql --default-character-set=utf8mb4 -ufrp_monitor -p<密码> < roster-huizhou.sql
USE frp_monitor;

INSERT INTO devices (hostname, site, owner) VALUES
  ('ep-000168', '惠州', '惠州服务商'),
  ('ep-000195', '惠州', '惠州服务商'),
  ('ep-000210', '惠州', '惠州服务商'),
  ('ep-000212', '惠州', '惠州服务商'),
  ('ep-000217', '惠州', '惠州服务商'),
  ('ep-000219', '惠州', '惠州服务商'),
  ('ep-000250', '惠州', '惠州服务商'),
  ('ep-000253', '惠州', '惠州服务商'),
  ('ep-000263', '惠州', '惠州服务商'),
  ('ep-000264', '惠州', '惠州服务商'),
  ('ep-000265', '惠州', '惠州服务商'),
  ('ep-000269', '惠州', '惠州服务商'),
  ('ep-000270', '惠州', '惠州服务商'),
  ('ep-000271', '惠州', '惠州服务商'),
  ('ep-000272', '惠州', '惠州服务商'),
  ('ep-000273', '惠州', '惠州服务商'),
  ('ep-000274', '惠州', '惠州服务商'),
  ('ep-000278', '惠州', '惠州服务商'),
  ('ep-000280', '惠州', '惠州服务商'),
  ('ep-000305', '惠州', '惠州服务商')
ON DUPLICATE KEY UPDATE site = VALUES(site), owner = VALUES(owner);
