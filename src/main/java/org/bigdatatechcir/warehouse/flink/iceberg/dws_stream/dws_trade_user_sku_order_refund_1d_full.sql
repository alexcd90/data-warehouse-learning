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
SET 'pipeline.name' = 'iceberg_dws_stream_dws_trade_user_sku_order_refund_1d_full';

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

CREATE TABLE IF NOT EXISTS iceberg_dws_stream.dws_trade_user_sku_order_refund_1d_full(
    `user_id` BIGINT COMMENT 'user id',
    `sku_id` BIGINT COMMENT 'sku id',
    `k1` STRING COMMENT 'partition field',
    `sku_name` STRING COMMENT 'sku name',
    `category1_id` BIGINT COMMENT 'category1 id',
    `category1_name` STRING COMMENT 'category1 name',
    `category2_id` BIGINT COMMENT 'category2 id',
    `category2_name` STRING COMMENT 'category2 name',
    `category3_id` BIGINT COMMENT 'category3 id',
    `category3_name` STRING COMMENT 'category3 name',
    `tm_id` BIGINT COMMENT 'tm id',
    `tm_name` STRING COMMENT 'tm name',
    `order_refund_count_1d` BIGINT COMMENT 'daily refund count',
    `order_refund_num_1d` BIGINT COMMENT 'daily refund sku num',
    `order_refund_amount_1d` DECIMAL(16, 2) COMMENT 'daily refund amount',
    PRIMARY KEY (`user_id`, `sku_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

INSERT INTO iceberg_dws_stream.dws_trade_user_sku_order_refund_1d_full(
    user_id,
    sku_id,
    k1,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    order_refund_count_1d,
    order_refund_num_1d,
    order_refund_amount_1d
)
SELECT
    rf.user_id,
    rf.sku_id,
    rf.k1,
    sku.sku_name,
    sku.category1_id,
    sku.category1_name,
    sku.category2_id,
    sku.category2_name,
    sku.category3_id,
    sku.category3_name,
    sku.tm_id,
    sku.tm_name,
    rf.order_refund_count_1d,
    rf.order_refund_num_1d,
    rf.order_refund_amount_1d
FROM (
    SELECT
        user_id,
        sku_id,
        k1,
        COUNT(*) AS order_refund_count_1d,
        SUM(refund_num) AS order_refund_num_1d,
        SUM(refund_amount) AS order_refund_amount_1d
    FROM iceberg_dwd_stream.dwd_trade_order_refund_full
    GROUP BY user_id, sku_id, k1
) rf
LEFT JOIN iceberg_dim_stream.dim_sku_full sku
    ON rf.sku_id = sku.id
   AND rf.k1 = sku.k1;


