SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'iceberg_ads_stream_ads_sku_association_analysis_full';

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

CREATE TABLE IF NOT EXISTS iceberg_ads_stream.ads_sku_association_analysis_full(
    `dt` STRING COMMENT 'stat date',
    `recent_days` BIGINT COMMENT 'recent days',
    `source_sku_id` STRING COMMENT 'source sku id',
    `source_sku_name` STRING COMMENT 'source sku name',
    `source_category1_id` STRING COMMENT 'source category1 id',
    `source_category1_name` STRING COMMENT 'source category1 name',
    `target_sku_id` STRING COMMENT 'target sku id',
    `target_sku_name` STRING COMMENT 'target sku name',
    `target_category1_id` STRING COMMENT 'target category1 id',
    `target_category1_name` STRING COMMENT 'target category1 name',
    `co_purchase_count` BIGINT COMMENT 'co purchase count',
    `co_purchase_user_count` BIGINT COMMENT 'co purchase user count',
    `support` DECIMAL(10, 4) COMMENT 'support',
    `confidence` DECIMAL(10, 4) COMMENT 'confidence',
    `lift` DECIMAL(10, 4) COMMENT 'lift',
    `sequence_pattern` STRING COMMENT 'sequence pattern',
    `time_interval_avg` DECIMAL(16, 2) COMMENT 'avg time interval',
    `association_strength` STRING COMMENT 'association strength',
    `recommendation_score` DECIMAL(10, 2) COMMENT 'recommendation score',
    PRIMARY KEY (`dt`, `recent_days`, `source_sku_id`, `target_sku_id`) NOT ENFORCED
) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

CREATE TEMPORARY VIEW tmp_ads_sku_association_order_detail_snapshot AS
SELECT *
FROM iceberg_dwd.dwd_trade_order_detail_full
;

CREATE TEMPORARY VIEW tmp_ads_sku_association_dim_sku_snapshot AS
SELECT *
FROM iceberg_dim_stream.dim_sku_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
;

CREATE TEMPORARY VIEW tmp_ads_sku_association_recent_days AS
SELECT CAST(7 AS BIGINT) AS recent_days
UNION ALL
SELECT CAST(30 AS BIGINT)
UNION ALL
SELECT CAST(90 AS BIGINT);

CREATE TEMPORARY VIEW tmp_ads_sku_association_base_orders AS
SELECT
    k1,
    d.recent_days,
    user_id,
    order_id,
    sku_id
FROM tmp_ads_sku_association_order_detail_snapshot
CROSS JOIN tmp_ads_sku_association_recent_days d
WHERE CAST(k1 AS DATE) >= TIMESTAMPADD(DAY, -90, CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE))
  AND CAST(create_time AS DATE) >= TIMESTAMPADD(DAY, -CAST(d.recent_days - 1 AS INT), CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE))
  AND CAST(create_time AS DATE) <= CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE);

CREATE TEMPORARY VIEW tmp_ads_sku_association_sku_pairs AS
SELECT
    a.k1,
    a.recent_days,
    a.user_id,
    a.order_id,
    a.sku_id AS source_sku_id,
    b.sku_id AS target_sku_id
FROM tmp_ads_sku_association_base_orders a
JOIN tmp_ads_sku_association_base_orders b
    ON a.order_id = b.order_id
   AND a.recent_days = b.recent_days
   AND a.k1 = b.k1
   AND a.sku_id < b.sku_id;

CREATE TEMPORARY VIEW tmp_ads_sku_association_total_orders AS
SELECT
    k1,
    recent_days,
    COUNT(DISTINCT order_id) AS order_count
FROM tmp_ads_sku_association_base_orders
GROUP BY k1, recent_days;

CREATE TEMPORARY VIEW tmp_ads_sku_association_sku_purchases AS
SELECT
    k1,
    recent_days,
    sku_id,
    COUNT(DISTINCT order_id) AS purchase_count
FROM tmp_ads_sku_association_base_orders
GROUP BY k1, recent_days, sku_id;

INSERT INTO iceberg_ads_stream.ads_sku_association_analysis_full(
    dt,
    recent_days,
    source_sku_id,
    source_sku_name,
    source_category1_id,
    source_category1_name,
    target_sku_id,
    target_sku_name,
    target_category1_id,
    target_category1_name,
    co_purchase_count,
    co_purchase_user_count,
    support,
    confidence,
    lift,
    recommendation_score
)
SELECT
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    pair.recent_days,
    CAST(source.id AS STRING) AS source_sku_id,
    source.sku_name AS source_sku_name,
    CAST(source.category1_id AS STRING) AS source_category1_id,
    source.category1_name AS source_category1_name,
    CAST(target.id AS STRING) AS target_sku_id,
    target.sku_name AS target_sku_name,
    CAST(target.category1_id AS STRING) AS target_category1_id,
    target.category1_name AS target_category1_name,
    COUNT(*) AS co_purchase_count,
    COUNT(DISTINCT pair.user_id) AS co_purchase_user_count,
    CAST(COUNT(*) / total_orders.order_count AS DECIMAL(10, 4)) AS support,
    CAST(COUNT(*) / source_purchases.purchase_count AS DECIMAL(10, 4)) AS confidence,
    CAST(
        (COUNT(*) / source_purchases.purchase_count)
        / (target_purchases.purchase_count / total_orders.order_count)
        AS DECIMAL(10, 4)
    ) AS lift,
    CAST(
        (
            (COUNT(*) / total_orders.order_count) * 0.2
            + (COUNT(*) / source_purchases.purchase_count) * 0.5
            + (
                (COUNT(*) / source_purchases.purchase_count)
                / (target_purchases.purchase_count / total_orders.order_count)
            ) * 0.3
        ) AS DECIMAL(10, 2)
    ) AS recommendation_score
FROM tmp_ads_sku_association_sku_pairs pair
JOIN tmp_ads_sku_association_dim_sku_snapshot source
    ON pair.source_sku_id = source.id
JOIN tmp_ads_sku_association_dim_sku_snapshot target
    ON pair.target_sku_id = target.id
JOIN tmp_ads_sku_association_total_orders total_orders
    ON pair.k1 = total_orders.k1
   AND pair.recent_days = total_orders.recent_days
JOIN tmp_ads_sku_association_sku_purchases source_purchases
    ON pair.k1 = source_purchases.k1
   AND pair.recent_days = source_purchases.recent_days
   AND pair.source_sku_id = source_purchases.sku_id
JOIN tmp_ads_sku_association_sku_purchases target_purchases
    ON pair.k1 = target_purchases.k1
   AND pair.recent_days = target_purchases.recent_days
   AND pair.target_sku_id = target_purchases.sku_id
GROUP BY
    pair.recent_days,
    source.id,
    source.sku_name,
    source.category1_id,
    source.category1_name,
    target.id,
    target.sku_name,
    target.category1_id,
    target.category1_name,
    total_orders.order_count,
    source_purchases.purchase_count,
    target_purchases.purchase_count
HAVING CAST(COUNT(*) / total_orders.order_count AS DECIMAL(10, 4)) >= CAST(0.001 AS DECIMAL(10, 4));


