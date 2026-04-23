SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'iceberg_dws_stream_dws_trade_user_payment_1d_full';

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG iceberg_catalog;
CREATE DATABASE IF NOT EXISTS iceberg_dws_stream;

CREATE TABLE IF NOT EXISTS iceberg_dws_stream.dws_trade_user_payment_1d_full(
    `user_id` BIGINT COMMENT 'user id',
    `k1` STRING COMMENT 'partition field',
    `payment_count_1d` BIGINT COMMENT 'daily payment count',
    `payment_num_1d` BIGINT COMMENT 'daily payment sku num',
    `payment_amount_1d` DECIMAL(16, 2) COMMENT 'daily payment amount',
    PRIMARY KEY (`user_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

INSERT INTO iceberg_dws_stream.dws_trade_user_payment_1d_full(
    user_id,
    k1,
    payment_count_1d,
    payment_num_1d,
    payment_amount_1d
)
SELECT
    user_id,
    k1,
    COUNT(DISTINCT order_id) AS payment_count_1d,
    SUM(sku_num) AS payment_num_1d,
    SUM(split_payment_amount) AS payment_amount_1d
FROM iceberg_dwd_stream.dwd_trade_pay_detail_suc_full
GROUP BY user_id, k1;


