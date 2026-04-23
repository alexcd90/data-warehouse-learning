SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'paimon_ads_stream_ads_user_retention_full';

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG paimon_hive;
CREATE DATABASE IF NOT EXISTS ads_stream;

CREATE TABLE IF NOT EXISTS ads_stream.ads_user_retention_full(
    `dt` STRING COMMENT 'stat date',
    `create_date` STRING COMMENT 'register date',
    `retention_day` INT COMMENT 'retention days',
    `retention_count` BIGINT COMMENT 'retained user count',
    `new_user_count` BIGINT COMMENT 'new user count',
    `retention_rate` DECIMAL(16, 2) COMMENT 'retention rate',
    PRIMARY KEY (`dt`, `create_date`, `retention_day`) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'file.format' = 'parquet',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true'
);

CREATE TEMPORARY VIEW tmp_ads_user_retention_register_snapshot AS
SELECT *
FROM dwd.dwd_user_register_full
;

CREATE TEMPORARY VIEW tmp_ads_user_retention_login_snapshot AS
SELECT *
FROM dws_stream.dws_user_user_login_td_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
;

INSERT INTO ads_stream.ads_user_retention_full(
    dt,
    create_date,
    retention_day,
    retention_count,
    new_user_count,
    retention_rate
)
SELECT
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    CAST(t1.login_date_first AS STRING) AS create_date,
    TIMESTAMPDIFF(DAY, t1.login_date_first, CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE)) AS retention_day,
    SUM(CASE WHEN t2.login_date_last = CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE) THEN 1 ELSE 0 END) AS retention_count,
    COUNT(*) AS new_user_count,
    CAST(
        SUM(CASE WHEN t2.login_date_last = CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE) THEN 1 ELSE 0 END) * 100.0
        / COUNT(*) AS DECIMAL(16, 2)
    ) AS retention_rate
FROM (
    SELECT
        CAST(user_id AS STRING) AS user_id,
        CAST(date_id AS DATE) AS login_date_first
    FROM tmp_ads_user_retention_register_snapshot
    WHERE CAST(k1 AS DATE) >= TIMESTAMPADD(DAY, -7, CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE))
      AND CAST(k1 AS DATE) < CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE)
) t1
JOIN (
    SELECT
        CAST(user_id AS STRING) AS user_id,
        CAST(login_date_last AS DATE) AS login_date_last
    FROM tmp_ads_user_retention_login_snapshot
    WHERE k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
) t2
    ON t1.user_id = t2.user_id
GROUP BY t1.login_date_first;


