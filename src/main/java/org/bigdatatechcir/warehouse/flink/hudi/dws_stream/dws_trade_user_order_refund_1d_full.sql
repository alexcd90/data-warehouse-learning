SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'hudi_dws_stream_dws_trade_user_order_refund_1d_full';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_dws_stream;

CREATE TABLE IF NOT EXISTS hudi_dws_stream.dws_trade_user_order_refund_1d_full(
    `user_id` BIGINT COMMENT 'user id',
    `k1` STRING COMMENT 'partition field',
    `order_refund_count_1d` BIGINT COMMENT 'daily refund count',
    `order_refund_num_1d` BIGINT COMMENT 'daily refund sku num',
    `order_refund_amount_1d` DECIMAL(16, 2) COMMENT 'daily refund amount',
    PRIMARY KEY (`user_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dws_stream.dws_trade_user_order_refund_1d_full(
    user_id,
    k1,
    order_refund_count_1d,
    order_refund_num_1d,
    order_refund_amount_1d
)
SELECT
    user_id,
    k1,
    COUNT(*) AS order_refund_count_1d,
    SUM(refund_num) AS order_refund_num_1d,
    SUM(refund_amount) AS order_refund_amount_1d
FROM hudi_dwd_stream.dwd_trade_order_refund_full /*+ OPTIONS('read.streaming.enabled' = 'true') */
GROUP BY user_id, k1;

