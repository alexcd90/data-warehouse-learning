SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'hudi_dws_stream_dws_trade_province_order_1d_full';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_dws_stream;

CREATE TABLE IF NOT EXISTS hudi_dws_stream.dws_trade_province_order_1d_full(
    `province_id` BIGINT COMMENT 'province id',
    `k1` STRING COMMENT 'partition field',
    `province_name` STRING COMMENT 'province name',
    `area_code` STRING COMMENT 'area code',
    `iso_code` STRING COMMENT 'iso code',
    `iso_3166_2` STRING COMMENT 'iso 3166-2 code',
    `order_count_1d` BIGINT COMMENT 'daily order count',
    `order_original_amount_1d` DECIMAL(16, 2) COMMENT 'daily original amount',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT 'daily activity reduce amount',
    `coupon_reduce_amount_1d` DECIMAL(16, 2) COMMENT 'daily coupon reduce amount',
    `order_total_amount_1d` DECIMAL(16, 2) COMMENT 'daily order total amount',
    PRIMARY KEY (`province_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dws_stream.dws_trade_province_order_1d_full(
    province_id,
    k1,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_1d,
    order_original_amount_1d,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d
)
SELECT
    o.province_id,
    o.k1,
    p.province_name,
    p.area_code,
    p.iso_code,
    p.iso_3166_2,
    o.order_count_1d,
    o.order_original_amount_1d,
    o.activity_reduce_amount_1d,
    o.coupon_reduce_amount_1d,
    o.order_total_amount_1d
FROM (
    SELECT
        province_id,
        k1,
        COUNT(DISTINCT order_id) AS order_count_1d,
        SUM(split_original_amount) AS order_original_amount_1d,
        COALESCE(SUM(split_activity_amount), CAST(0 AS DECIMAL(16, 2))) AS activity_reduce_amount_1d,
        COALESCE(SUM(split_coupon_amount), CAST(0 AS DECIMAL(16, 2))) AS coupon_reduce_amount_1d,
        COALESCE(SUM(split_total_amount), CAST(0 AS DECIMAL(16, 2))) AS order_total_amount_1d
    FROM hudi_dwd_stream.dwd_trade_order_detail_full /*+ OPTIONS('read.streaming.enabled' = 'true') */
    GROUP BY province_id, k1
) o
LEFT JOIN hudi_dim.dim_province_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ p
    ON o.province_id = p.id;

