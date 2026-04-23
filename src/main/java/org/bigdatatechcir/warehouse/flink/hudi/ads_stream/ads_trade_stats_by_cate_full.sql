SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'hudi_ads_stream_ads_trade_stats_by_cate_full';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_ads_stream;

CREATE TABLE IF NOT EXISTS hudi_ads_stream.ads_trade_stats_by_cate_full(
    `dt` STRING COMMENT 'stat date',
    `recent_days` BIGINT COMMENT 'recent day window',
    `category1_id` STRING COMMENT 'category1 id',
    `category1_name` STRING COMMENT 'category1 name',
    `category2_id` STRING COMMENT 'category2 id',
    `category2_name` STRING COMMENT 'category2 name',
    `category3_id` STRING COMMENT 'category3 id',
    `category3_name` STRING COMMENT 'category3 name',
    `order_count` BIGINT COMMENT 'order count',
    `order_user_count` BIGINT COMMENT 'order user count',
    `order_refund_count` BIGINT COMMENT 'refund count',
    `order_refund_user_count` BIGINT COMMENT 'refund user count',
    PRIMARY KEY (`dt`, `recent_days`, `category1_id`, `category2_id`, `category3_id`) NOT ENFORCED
) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

CREATE TEMPORARY VIEW tmp_ads_trade_stats_by_cate_recent_days AS
SELECT CAST(7 AS BIGINT) AS recent_days
UNION ALL
SELECT CAST(30 AS BIGINT) AS recent_days
;

CREATE TEMPORARY VIEW tmp_ads_trade_stats_by_cate_base AS
SELECT
    CAST(1 AS BIGINT) AS recent_days,
    CAST(o.category1_id AS STRING) AS category1_id,
    COALESCE(o.category1_name, '') AS category1_name,
    CAST(o.category2_id AS STRING) AS category2_id,
    COALESCE(o.category2_name, '') AS category2_name,
    CAST(o.category3_id AS STRING) AS category3_id,
    COALESCE(o.category3_name, '') AS category3_name,
    SUM(o.order_count_1d) AS order_count,
    COUNT(DISTINCT CAST(o.user_id AS STRING)) AS order_user_count,
    CAST(0 AS BIGINT) AS order_refund_count,
    CAST(0 AS BIGINT) AS order_refund_user_count
FROM hudi_dws_stream.dws_trade_user_sku_order_1d_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ o
WHERE o.k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
  AND o.category1_id IS NOT NULL
  AND o.category2_id IS NOT NULL
  AND o.category3_id IS NOT NULL
GROUP BY
    CAST(o.category1_id AS STRING),
    COALESCE(o.category1_name, ''),
    CAST(o.category2_id AS STRING),
    COALESCE(o.category2_name, ''),
    CAST(o.category3_id AS STRING),
    COALESCE(o.category3_name, '')
UNION ALL
SELECT
    p.recent_days,
    CAST(o.category1_id AS STRING) AS category1_id,
    COALESCE(o.category1_name, '') AS category1_name,
    CAST(o.category2_id AS STRING) AS category2_id,
    COALESCE(o.category2_name, '') AS category2_name,
    CAST(o.category3_id AS STRING) AS category3_id,
    COALESCE(o.category3_name, '') AS category3_name,
    SUM(CASE WHEN p.recent_days = 7 THEN o.order_count_7d ELSE o.order_count_30d END) AS order_count,
    COUNT(
        DISTINCT CASE
            WHEN (CASE WHEN p.recent_days = 7 THEN o.order_count_7d ELSE o.order_count_30d END) > 0
            THEN CAST(o.user_id AS STRING)
            ELSE NULL
        END
    ) AS order_user_count,
    CAST(0 AS BIGINT) AS order_refund_count,
    CAST(0 AS BIGINT) AS order_refund_user_count
FROM tmp_ads_trade_stats_by_cate_recent_days p
JOIN hudi_dws_stream.dws_trade_user_sku_order_nd_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ o
    ON o.k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
WHERE o.category1_id IS NOT NULL
  AND o.category2_id IS NOT NULL
  AND o.category3_id IS NOT NULL
GROUP BY
    p.recent_days,
    CAST(o.category1_id AS STRING),
    COALESCE(o.category1_name, ''),
    CAST(o.category2_id AS STRING),
    COALESCE(o.category2_name, ''),
    CAST(o.category3_id AS STRING),
    COALESCE(o.category3_name, '')
UNION ALL
SELECT
    CAST(1 AS BIGINT) AS recent_days,
    CAST(r.category1_id AS STRING) AS category1_id,
    COALESCE(r.category1_name, '') AS category1_name,
    CAST(r.category2_id AS STRING) AS category2_id,
    COALESCE(r.category2_name, '') AS category2_name,
    CAST(r.category3_id AS STRING) AS category3_id,
    COALESCE(r.category3_name, '') AS category3_name,
    CAST(0 AS BIGINT) AS order_count,
    CAST(0 AS BIGINT) AS order_user_count,
    SUM(r.order_refund_count_1d) AS order_refund_count,
    COUNT(DISTINCT CAST(r.user_id AS STRING)) AS order_refund_user_count
FROM hudi_dws_stream.dws_trade_user_sku_order_refund_1d_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ r
WHERE r.k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
  AND r.category1_id IS NOT NULL
  AND r.category2_id IS NOT NULL
  AND r.category3_id IS NOT NULL
GROUP BY
    CAST(r.category1_id AS STRING),
    COALESCE(r.category1_name, ''),
    CAST(r.category2_id AS STRING),
    COALESCE(r.category2_name, ''),
    CAST(r.category3_id AS STRING),
    COALESCE(r.category3_name, '')
UNION ALL
SELECT
    p.recent_days,
    CAST(r.category1_id AS STRING) AS category1_id,
    COALESCE(r.category1_name, '') AS category1_name,
    CAST(r.category2_id AS STRING) AS category2_id,
    COALESCE(r.category2_name, '') AS category2_name,
    CAST(r.category3_id AS STRING) AS category3_id,
    COALESCE(r.category3_name, '') AS category3_name,
    CAST(0 AS BIGINT) AS order_count,
    CAST(0 AS BIGINT) AS order_user_count,
    SUM(CASE WHEN p.recent_days = 7 THEN r.order_refund_count_7d ELSE r.order_refund_count_30d END) AS order_refund_count,
    COUNT(
        DISTINCT CASE
            WHEN (CASE WHEN p.recent_days = 7 THEN r.order_refund_count_7d ELSE r.order_refund_count_30d END) > 0
            THEN CAST(r.user_id AS STRING)
            ELSE NULL
        END
    ) AS order_refund_user_count
FROM tmp_ads_trade_stats_by_cate_recent_days p
JOIN hudi_dws_stream.dws_trade_user_sku_order_refund_nd_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ r
    ON r.k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
WHERE r.category1_id IS NOT NULL
  AND r.category2_id IS NOT NULL
  AND r.category3_id IS NOT NULL
GROUP BY
    p.recent_days,
    CAST(r.category1_id AS STRING),
    COALESCE(r.category1_name, ''),
    CAST(r.category2_id AS STRING),
    COALESCE(r.category2_name, ''),
    CAST(r.category3_id AS STRING),
    COALESCE(r.category3_name, '')
;

INSERT INTO hudi_ads_stream.ads_trade_stats_by_cate_full(
    dt,
    recent_days,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    order_count,
    order_user_count,
    order_refund_count,
    order_refund_user_count
)
SELECT
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    recent_days,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    SUM(order_count) AS order_count,
    SUM(order_user_count) AS order_user_count,
    SUM(order_refund_count) AS order_refund_count,
    SUM(order_refund_user_count) AS order_refund_user_count
FROM tmp_ads_trade_stats_by_cate_base
GROUP BY
    recent_days,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name
;

