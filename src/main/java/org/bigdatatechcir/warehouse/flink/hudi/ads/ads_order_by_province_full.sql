SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_ads;

CREATE TABLE IF NOT EXISTS hudi_ads.ads_order_by_province_full(
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
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

CREATE TEMPORARY VIEW tmp_ads_order_by_province_recent_days AS
    SELECT CAST(7 AS BIGINT) AS recent_days
    UNION ALL
    SELECT CAST(30 AS BIGINT) AS recent_days
;

INSERT INTO hudi_ads.ads_order_by_province_full(
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
    '${pdate}' AS dt,
    CAST(1 AS BIGINT) AS recent_days,
    province_id,
    province_name,
    area_code,
    iso_code,
    iso_3166_2 AS iso_code_3166_2,
    order_count_1d AS order_count,
    order_total_amount_1d AS order_total_amount
FROM hudi_dws.dws_trade_province_order_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
WHERE k1 = '${pdate}'
UNION ALL
SELECT
    '${pdate}' AS dt,
    r.recent_days,
    n.province_id,
    n.province_name,
    n.area_code,
    n.iso_code,
    n.iso_3166_2 AS iso_code_3166_2,
    CASE WHEN r.recent_days = 7 THEN n.order_count_7d ELSE n.order_count_30d END AS order_count,
    CASE WHEN r.recent_days = 7 THEN n.order_total_amount_7d ELSE n.order_total_amount_30d END AS order_total_amount
FROM hudi_dws.dws_trade_province_order_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */ n
CROSS JOIN tmp_ads_order_by_province_recent_days r
WHERE n.k1 = '${pdate}';
