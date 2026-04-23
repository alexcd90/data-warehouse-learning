SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'paimon_ads_stream_ads_order_by_province_full';

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

CREATE TABLE IF NOT EXISTS ads_stream.ads_order_by_province_full(
    `dt` STRING COMMENT 'stat date',
    `recent_days` BIGINT COMMENT 'recent day window',
    `province_id` BIGINT COMMENT 'province id',
    `province_name` STRING COMMENT 'province name',
    `area_code` STRING COMMENT 'area code',
    `iso_code` STRING COMMENT 'iso code',
    `iso_code_3166_2` STRING COMMENT 'iso 3166-2 code',
    `order_count` BIGINT COMMENT 'order count',
    `order_total_amount` DECIMAL(16, 2) COMMENT 'order total amount',
    PRIMARY KEY (`dt`, `recent_days`, `province_id`) NOT ENFORCED
) PARTITIONED BY (`dt`) WITH (
    'connector' = 'paimon',
    'metastore.partitioned-table' = 'true',
    'file.format' = 'parquet',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true',
    'partition.expiration-time' = '1 d',
    'partition.expiration-check-interval' = '1 h',
    'partition.timestamp-formatter' = 'yyyy-MM-dd',
    'partition.timestamp-pattern' = '$dt'
);

CREATE TEMPORARY VIEW tmp_ads_order_by_province_recent_days AS
    SELECT CAST(7 AS BIGINT) AS recent_days
    UNION ALL
    SELECT CAST(30 AS BIGINT) AS recent_days
;

INSERT INTO ads_stream.ads_order_by_province_full(
    dt,
    recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_code_3166_2,
    order_count,
    order_total_amount
)
SELECT
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    CAST(1 AS BIGINT) AS recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2 AS iso_code_3166_2,
    order_count_1d AS order_count,
    order_total_amount_1d AS order_total_amount
FROM dws_stream.dws_trade_province_order_1d_full
WHERE k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
UNION ALL
SELECT
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    r.recent_days,
    n.province_id,
    n.province_name,
    n.area_code,
    n.iso_code,
    n.iso_3166_2 AS iso_code_3166_2,
    CASE WHEN r.recent_days = 7 THEN n.order_count_7d ELSE n.order_count_30d END AS order_count,
    CASE WHEN r.recent_days = 7 THEN n.order_total_amount_7d ELSE n.order_total_amount_30d END AS order_total_amount
FROM dws_stream.dws_trade_province_order_nd_full n
CROSS JOIN tmp_ads_order_by_province_recent_days r
WHERE n.k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd');


