-- 河南分组设备归属清单（2026-09-22）
-- 范围：2026-09-18 装机的 20 台新设备（site18b/18c 批次，EP-000174…310）。
-- 在线设备仅更新 site/owner；未上线设备预登记（first_seen_at/last_seen_at 为 NULL）。
-- 设备真实端口以 frps 观测为准，预登记不填端口。
-- 导入命令（必须带 --default-character-set=utf8mb4，否则客户端默认 latin1 会双重编码）：
--   docker exec -i frp-monitor-mysql mysql --default-character-set=utf8mb4 -ufrp_monitor -p<密码> < roster-henan.sql
USE frp_monitor;

INSERT INTO devices (hostname, site, owner) VALUES
  ('ep-000174', '河南', '河南服务商'),
  ('ep-000176', '河南', '河南服务商'),
  ('ep-000245', '河南', '河南服务商'),
  ('ep-000246', '河南', '河南服务商'),
  ('ep-000254', '河南', '河南服务商'),
  ('ep-000275', '河南', '河南服务商'),
  ('ep-000283', '河南', '河南服务商'),
  ('ep-000284', '河南', '河南服务商'),
  ('ep-000285', '河南', '河南服务商'),
  ('ep-000287', '河南', '河南服务商'),
  ('ep-000288', '河南', '河南服务商'),
  ('ep-000289', '河南', '河南服务商'),
  ('ep-000291', '河南', '河南服务商'),
  ('ep-000292', '河南', '河南服务商'),
  ('ep-000298', '河南', '河南服务商'),
  ('ep-000300', '河南', '河南服务商'),
  ('ep-000302', '河南', '河南服务商'),
  ('ep-000307', '河南', '河南服务商'),
  ('ep-000309', '河南', '河南服务商'),
  ('ep-000310', '河南', '河南服务商')
ON DUPLICATE KEY UPDATE site = VALUES(site), owner = VALUES(owner);
