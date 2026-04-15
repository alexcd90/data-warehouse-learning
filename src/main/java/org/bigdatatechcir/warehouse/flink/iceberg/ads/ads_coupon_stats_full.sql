SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG iceberg_catalog;
CREATE DATABASE IF NOT EXISTS iceberg_ads;

CREATE TABLE IF NOT EXISTS iceberg_ads.ads_coupon_stats_full(
    `dt` STRING COMMENT 'stat date',
    `coupon_id` BIGINT COMMENT 'coupon id',
    `coupon_name` STRING COMMENT 'coupon name',
    `start_date` STRING COMMENT 'coupon start date',
    `rule_name` STRING COMMENT 'coupon rule',
    `reduce_rate` DECIMAL(16, 2) COMMENT 'coupon reduce rate',
    PRIMARY KEY (`dt`, `coupon_id`) NOT ENFORCED
) PARTITIONED BY (`dt`) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

INSERT INTO iceberg_ads.ads_coupon_stats_full /*+ OPTIONS('upsert-enabled' = 'true') */(
    dt,
    coupon_id,
    coupon_name,
    start_date,
    rule_name,
    reduce_rate
)
SELECT
    '${pdate}' AS dt,
    coupon_id,
    coupon_name,
    start_date,
    coupon_rule AS rule_name,
    CASE
        WHEN original_amount_30d = CAST(0 AS DECIMAL(16, 2)) THEN CAST(0 AS DECIMAL(16, 2))
        ELSE CAST(coupon_reduce_amount_30d / original_amount_30d AS DECIMAL(16, 2))
    END AS reduce_rate
FROM iceberg_dws.dws_trade_coupon_order_nd_full
WHERE k1 = '${pdate}';
