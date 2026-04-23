SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'paimon_ads_stream_ads_coupon_stats_full';

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

CREATE TABLE IF NOT EXISTS ads_stream.ads_coupon_stats_full(
    `dt` STRING COMMENT 'stat date',
    `coupon_id` BIGINT COMMENT 'coupon id',
    `coupon_name` STRING COMMENT 'coupon name',
    `start_date` STRING COMMENT 'coupon start date',
    `rule_name` STRING COMMENT 'coupon rule',
    `reduce_rate` DECIMAL(16, 2) COMMENT 'coupon reduce rate',
    PRIMARY KEY (`dt`, `coupon_id`) NOT ENFORCED
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

INSERT INTO ads_stream.ads_coupon_stats_full(
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
FROM dws_stream.dws_trade_coupon_order_nd_full
WHERE k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd');


