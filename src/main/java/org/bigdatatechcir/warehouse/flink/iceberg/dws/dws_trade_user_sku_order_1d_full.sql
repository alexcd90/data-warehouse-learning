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

CREATE TABLE IF NOT EXISTS iceberg_dws.dws_trade_user_sku_order_1d_full(
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
    `order_count_1d` BIGINT COMMENT 'daily order count',
    `order_num_1d` BIGINT COMMENT 'daily sku num',
    `order_original_amount_1d` DECIMAL(16, 2) COMMENT 'daily original amount',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT 'daily activity reduce amount',
    `coupon_reduce_amount_1d` DECIMAL(16, 2) COMMENT 'daily coupon reduce amount',
    `order_total_amount_1d` DECIMAL(16, 2) COMMENT 'daily total amount',
    PRIMARY KEY (`user_id`, `sku_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

INSERT INTO iceberg_dws.dws_trade_user_sku_order_1d_full /*+ OPTIONS('upsert-enabled' = 'true') */(
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
    order_count_1d,
    order_num_1d,
    order_original_amount_1d,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d
)
SELECT
    od.user_id,
    od.sku_id,
    od.k1,
    sku.sku_name,
    sku.category1_id,
    sku.category1_name,
    sku.category2_id,
    sku.category2_name,
    sku.category3_id,
    sku.category3_name,
    sku.tm_id,
    sku.tm_name,
    od.order_count_1d,
    od.order_num_1d,
    od.order_original_amount_1d,
    od.activity_reduce_amount_1d,
    od.coupon_reduce_amount_1d,
    od.order_total_amount_1d
FROM (
    SELECT
        user_id,
        sku_id,
        k1,
        COUNT(*) AS order_count_1d,
        SUM(sku_num) AS order_num_1d,
        SUM(split_original_amount) AS order_original_amount_1d,
        COALESCE(SUM(split_activity_amount), CAST(0 AS DECIMAL(16, 2))) AS activity_reduce_amount_1d,
        COALESCE(SUM(split_coupon_amount), CAST(0 AS DECIMAL(16, 2))) AS coupon_reduce_amount_1d,
        SUM(split_total_amount) AS order_total_amount_1d
    FROM iceberg_dwd.dwd_trade_order_detail_full
    GROUP BY user_id, sku_id, k1
) od
LEFT JOIN iceberg_dim.dim_sku_full sku
    ON od.sku_id = sku.id
   AND od.k1 = sku.k1;
