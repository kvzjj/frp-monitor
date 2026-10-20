-- KVWO-103 增量对账：提出人 2026-10-20 19:02 更新飞书台账——惠州设备全部划转宿迁服务商（19 台，site+owner）
-- 数据源: 生产 assets.sqlite（飞书同步 19:00:32 快照）vs MySQL devices diff，仅此 19 台差异。
-- 幂等。前置全量备份仍为 devices_backup_20261020_kvwo103。
USE frp_monitor;

UPDATE devices SET site='宿迁服务商', owner='宿迁服务商' WHERE hostname IN ('ep-000168','ep-000195','ep-000210','ep-000212','ep-000217','ep-000219','ep-000250','ep-000253','ep-000263','ep-000264','ep-000265','ep-000269','ep-000270','ep-000271','ep-000273','ep-000274','ep-000278','ep-000280','ep-000305');  -- 19 台
