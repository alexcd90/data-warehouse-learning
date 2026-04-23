SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'hudi_ads_stream_ads_coupon_stats_full';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_ads_stream;

CREATE TABLE IF NOT EXISTS hudi_ads_stream.ads_coupon_stats_full(
    `dt` STRING COMMENT 'stat date',
    `coupon_id` BIGINT COMMENT 'coupon id',
    `coupon_name` STRING COMMENT 'coupon name',
    `start_date` STRING COMMENT 'coupon start date',
    `rule_name` STRING COMMENT 'coupon rule',
    `reduce_rate` DECIMAL(16, 2) COMMENT 'coupon reduce rate',
    PRIMARY KEY (`dt`, `coupon_id`) NOT ENFORCED
) PARTITIONED BY (`dt`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_ads_stream.ads_coupon_stats_full(
    dt,
    coupon_id,
    coupon_name,
    start_date,
    rule_name,
    reduce_rate
)
SELECT
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    coupon_id,
    coupon_name,
    start_date,
    coupon_rule AS rule_name,
    CASE
        WHEN original_amount_30d = CAST(0 AS DECIMAL(16, 2)) THEN CAST(0 AS DECIMAL(16, 2))
        ELSE CAST(coupon_reduce_amount_30d / original_amount_30d AS DECIMAL(16, 2))
    END AS reduce_rate
FROM hudi_dws_stream.dws_trade_coupon_order_nd_full /*+ OPTIONS('read.streaming.enabled' = 'true') */
WHERE k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd');

