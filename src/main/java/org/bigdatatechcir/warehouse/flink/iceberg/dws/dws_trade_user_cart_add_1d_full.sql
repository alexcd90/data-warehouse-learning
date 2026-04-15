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
CREATE DATABASE IF NOT EXISTS iceberg_dws;

CREATE TABLE IF NOT EXISTS iceberg_dws.dws_trade_user_cart_add_1d_full(
    `user_id` STRING COMMENT 'user id',
    `k1` STRING COMMENT 'partition field',
    `cart_add_count_1d` BIGINT COMMENT 'daily cart add count',
    `cart_add_num_1d` BIGINT COMMENT 'daily cart add sku num',
    PRIMARY KEY (`user_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

INSERT INTO iceberg_dws.dws_trade_user_cart_add_1d_full /*+ OPTIONS('upsert-enabled' = 'true') */(
    user_id,
    k1,
    cart_add_count_1d,
    cart_add_num_1d
)
SELECT
    user_id,
    k1,
    COUNT(*) AS cart_add_count_1d,
    SUM(sku_num) AS cart_add_num_1d
FROM iceberg_dwd.dwd_trade_cart_add_full
GROUP BY user_id, k1;
