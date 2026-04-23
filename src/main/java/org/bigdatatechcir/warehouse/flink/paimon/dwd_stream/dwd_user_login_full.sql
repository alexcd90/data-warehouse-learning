SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'pipeline.name' = 'paimon_dwd_stream_dwd_user_login_full';

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG paimon_hive;
create  DATABASE IF NOT EXISTS dwd_stream;

CREATE TABLE IF NOT EXISTS dwd_stream.dwd_user_login_full(
    `k1`             STRING COMMENT '分区字段',
    `user_id`        STRING COMMENT '用户ID',
    `date_id`        STRING COMMENT '日期ID',
    `login_time`     STRING COMMENT '登录时间',
    `channel`        STRING COMMENT '应用下载渠道',
    `province_id`    BIGINT COMMENT '省份id',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`         STRING COMMENT '设备id',
    `brand`          STRING COMMENT '设备品牌',
    `model`          STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统',
    PRIMARY KEY (`k1`,`user_id`,`date_id` ) NOT ENFORCED
    ) PARTITIONED BY (`k1`) WITH (
    'connector' = 'paimon',
    'metastore.partitioned-table' = 'true',
    'file.format' = 'parquet',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true',
    'partition.expiration-time' = '1 d',
    'partition.expiration-check-interval' = '1 h',
    'partition.timestamp-formatter' = 'yyyy-MM-dd',
    'partition.timestamp-pattern' = '$k1'
);

CREATE TEMPORARY VIEW dwd_user_login_login_events AS
SELECT
    common_uid user_id,
    common_ch channel,
    common_ar area_code,
    common_vc version_code,
    common_mid mid_id,
    common_ba brand,
    common_md model,
    common_os operate_system,
    DATE_FORMAT(FROM_UNIXTIME(CAST(ts / 1000 AS BIGINT)), 'yyyy-MM-dd') AS date_id,
    ts
from ods.`ods_log_inc$audit_log`
WHERE rowkind = '+I'
  AND common_uid IS NOT NULL
  AND page_last_page_id IS NULL
  AND page_page_id IS NOT NULL
;

CREATE TEMPORARY VIEW dwd_user_login_first_daily_login AS
SELECT
    user_id,
    date_id,
    date_id AS k1,
    MAX(channel) AS channel,
    MAX(area_code) AS area_code,
    MAX(version_code) AS version_code,
    MAX(mid_id) AS mid_id,
    MAX(brand) AS brand,
    MAX(model) AS model,
    MAX(operate_system) AS operate_system,
    MIN(ts) AS first_login_ts
FROM dwd_user_login_login_events
WHERE ts IS NOT NULL
GROUP BY user_id, date_id
;

INSERT INTO dwd_stream.dwd_user_login_full(
    k1,
    user_id,
    date_id,
    login_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
    )
SELECT
    login.k1,
    login.user_id,
    login.date_id,
    DATE_FORMAT(FROM_UNIXTIME(CAST(login.first_login_ts / 1000 AS BIGINT)), 'yyyy-MM-dd HH:mm:ss') AS login_time,
    login.channel,
    bp.province_id,
    login.version_code,
    login.mid_id,
    login.brand,
    login.model,
    login.operate_system
FROM dwd_user_login_first_daily_login login
LEFT JOIN
(
    SELECT
        id AS province_id,
        area_code
    FROM ods.ods_base_province_full
) bp
    ON login.area_code = bp.area_code
;


