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
SET 'pipeline.name' = 'iceberg_ads_stream_ads_sku_cart_num_top3_by_cate_full';

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG iceberg_catalog;
CREATE DATABASE IF NOT EXISTS iceberg_ads_stream;

CREATE TABLE IF NOT EXISTS iceberg_ads_stream.ads_sku_cart_num_top3_by_cate_full(
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

CREATE TEMPORARY VIEW tmp_ads_sku_cart_num_top3_cart_snapshot AS
SELECT *
FROM iceberg_dwd.dwd_trade_cart_full
;

CREATE TEMPORARY VIEW tmp_ads_sku_cart_num_top3_dim_sku_snapshot AS
SELECT *
FROM iceberg_dim_stream.dim_sku_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
;

CREATE TEMPORARY VIEW tmp_ads_sku_cart_num_top3_cart_agg AS
SELECT
    sku_id,
    SUM(sku_num) AS cart_num
FROM tmp_ads_sku_cart_num_top3_cart_snapshot
WHERE k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
GROUP BY sku_id
;

CREATE TEMPORARY VIEW tmp_ads_sku_cart_num_top3_joined AS
SELECT
    sku_dim.category1_id,
    sku_dim.category1_name,
    sku_dim.category2_id,
    sku_dim.category2_name,
    sku_dim.category3_id,
    sku_dim.category3_name,
    cart.sku_id,
    sku_dim.sku_name,
    cart.cart_num
FROM tmp_ads_sku_cart_num_top3_cart_agg cart
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
    FROM tmp_ads_sku_cart_num_top3_dim_sku_snapshot
) sku_dim
    ON cart.sku_id = sku_dim.id
;

CREATE TEMPORARY VIEW tmp_ads_sku_cart_num_top3_ranked AS
SELECT
    cur.category1_id,
    cur.category1_name,
    cur.category2_id,
    cur.category2_name,
    cur.category3_id,
    cur.category3_name,
    cur.sku_id,
    cur.sku_name,
    cur.cart_num,
    CAST(COUNT(higher.sku_id) + 1 AS BIGINT) AS rk
FROM tmp_ads_sku_cart_num_top3_joined cur
LEFT JOIN tmp_ads_sku_cart_num_top3_joined higher
    ON cur.category1_id = higher.category1_id
    AND cur.category2_id = higher.category2_id
    AND cur.category3_id = higher.category3_id
    AND (
        higher.cart_num > cur.cart_num
        OR (higher.cart_num = cur.cart_num AND higher.sku_id < cur.sku_id)
    )
GROUP BY
    cur.category1_id,
    cur.category1_name,
    cur.category2_id,
    cur.category2_name,
    cur.category3_id,
    cur.category3_name,
    cur.sku_id,
    cur.sku_name,
    cur.cart_num
;

INSERT INTO iceberg_ads_stream.ads_sku_cart_num_top3_by_cate_full(
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
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    CAST(category1_id AS STRING) AS category1_id,
    category1_name,
    CAST(category2_id AS STRING) AS category2_id,
    category2_name,
    CAST(category3_id AS STRING) AS category3_id,
    category3_name,
    CAST(sku_id AS STRING) AS sku_id,
    sku_name,
    cart_num,
    rk
FROM tmp_ads_sku_cart_num_top3_ranked
WHERE rk <= 3
;


