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
SET 'pipeline.name' = 'hudi_dws_stream_dws_trade_user_order_1d_full';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_dws_stream;

CREATE TABLE IF NOT EXISTS hudi_dws_stream.dws_trade_user_order_1d_full(
    `user_id` BIGINT COMMENT 'user id',
    `k1` STRING COMMENT 'partition field',
    `order_count_1d` BIGINT COMMENT 'daily order count',
    `order_num_1d` BIGINT COMMENT 'daily sku num',
    `order_original_amount_1d` DECIMAL(16, 2) COMMENT 'daily original amount',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT 'daily activity reduce amount',
    `coupon_reduce_amount_1d` DECIMAL(16, 2) COMMENT 'daily coupon reduce amount',
    `order_total_amount_1d` DECIMAL(16, 2) COMMENT 'daily total amount',
    PRIMARY KEY (`user_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dws_stream.dws_trade_user_order_1d_full(
    user_id,
    k1,
    order_count_1d,
    order_num_1d,
    order_original_amount_1d,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d
)
SELECT
    user_id,
    k1,
    COUNT(DISTINCT order_id) AS order_count_1d,
    SUM(sku_num) AS order_num_1d,
    SUM(split_original_amount) AS order_original_amount_1d,
    COALESCE(SUM(split_activity_amount), CAST(0 AS DECIMAL(16, 2))) AS activity_reduce_amount_1d,
    COALESCE(SUM(split_coupon_amount), CAST(0 AS DECIMAL(16, 2))) AS coupon_reduce_amount_1d,
    SUM(split_total_amount) AS order_total_amount_1d
FROM hudi_dwd_stream.dwd_trade_order_detail_full /*+ OPTIONS('read.streaming.enabled' = 'true') */
GROUP BY user_id, k1;

