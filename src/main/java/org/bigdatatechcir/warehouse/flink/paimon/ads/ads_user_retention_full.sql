SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG paimon_hive;
CREATE DATABASE IF NOT EXISTS ads;

CREATE TABLE IF NOT EXISTS ads.ads_user_retention_full(
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

INSERT INTO ads.ads_user_retention_full(
    dt,
    create_date,
    retention_day,
    retention_count,
    new_user_count,
    retention_rate
)
SELECT
    '${pdate}' AS dt,
    CAST(t1.login_date_first AS STRING) AS create_date,
    TIMESTAMPDIFF(DAY, t1.login_date_first, CAST('${pdate}' AS DATE)) AS retention_day,
    SUM(CASE WHEN t2.login_date_last = CAST('${pdate}' AS DATE) THEN 1 ELSE 0 END) AS retention_count,
    COUNT(*) AS new_user_count,
    CAST(
        SUM(CASE WHEN t2.login_date_last = CAST('${pdate}' AS DATE) THEN 1 ELSE 0 END) * 100.0
        / COUNT(*) AS DECIMAL(16, 2)
    ) AS retention_rate
FROM (
    SELECT
        CAST(user_id AS STRING) AS user_id,
        CAST(date_id AS DATE) AS login_date_first
    FROM dwd.dwd_user_register_full
    WHERE CAST(k1 AS DATE) >= TIMESTAMPADD(DAY, -7, CAST('${pdate}' AS DATE))
      AND CAST(k1 AS DATE) < CAST('${pdate}' AS DATE)
) t1
JOIN (
    SELECT
        CAST(user_id AS STRING) AS user_id,
        CAST(login_date_last AS DATE) AS login_date_last
    FROM dws.dws_user_user_login_td_full
    WHERE k1 = '${pdate}'
) t2
    ON t1.user_id = t2.user_id
GROUP BY t1.login_date_first;
