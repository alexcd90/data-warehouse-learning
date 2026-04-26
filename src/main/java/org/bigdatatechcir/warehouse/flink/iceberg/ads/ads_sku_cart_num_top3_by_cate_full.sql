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

CREATE TABLE IF NOT EXISTS iceberg_ads.ads_sku_cart_num_top3_by_cate_full(
    `dt` STRING COMMENT 'stat date',
    `category1_id` STRING COMMENT 'category1 id',
    `category1_name` STRING COMMENT 'category1 name',
    `category2_id` STRING COMMENT 'category2 id',
    `category2_name` STRING COMMENT 'category2 name',
    `category3_id` STRING COMMENT 'category3 id',
    `category3_name` STRING COMMENT 'category3 name',
    `sku_id` STRING COMMENT 'sku id',
    `sku_name` STRING COMMENT 'sku name',
    `cart_num` BIGINT COMMENT 'cart num',
    `rk` BIGINT COMMENT 'rank',
    PRIMARY KEY (`dt`, `category1_id`, `category2_id`, `category3_id`, `sku_id`, `rk`) NOT ENFORCED
) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

INSERT INTO iceberg_ads.ads_sku_cart_num_top3_by_cate_full /*+ OPTIONS('upsert-enabled' = 'true') */(
    dt,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    sku_id,
    sku_name,
    cart_num,
    rk
)
SELECT
    '${pdate}' AS dt,
    CAST(category1_id AS STRING) AS category1_id,
    category1_name,
    CAST(category2_id AS STRING) AS category2_id,
    category2_name,
    CAST(category3_id AS STRING) AS category3_id,
    category3_name,
    CAST(sku_id AS STRING) AS sku_id,
    sku_name,
    cart_num,
    CAST(rk AS BIGINT) AS rk
FROM (
    SELECT
        sku_dim.category1_id,
        sku_dim.category1_name,
        sku_dim.category2_id,
        sku_dim.category2_name,
        sku_dim.category3_id,
        sku_dim.category3_name,
        cart.sku_id,
        sku_dim.sku_name,
        cart.cart_num,
        RANK() OVER (
            PARTITION BY sku_dim.category1_id, sku_dim.category2_id, sku_dim.category3_id
            ORDER BY cart.cart_num DESC, cart.sku_id
        ) AS rk
    FROM (
        SELECT
            sku_id,
            SUM(sku_num) AS cart_num
        FROM iceberg_dwd.dwd_trade_cart_full
        WHERE k1 = '${pdate}'
        GROUP BY sku_id
    ) cart
    LEFT JOIN (
        SELECT
            id,
            sku_name,
            category1_id,
            category1_name,
            category2_id,
            category2_name,
            category3_id,
            category3_name
        FROM iceberg_dim.dim_sku_full
    ) sku_dim
        ON cart.sku_id = sku_dim.id
) ranked
WHERE rk <= 3;
