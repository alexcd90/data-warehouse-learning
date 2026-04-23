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
SET 'pipeline.name' = 'hudi_ads_stream_ads_trade_stats_by_tm_category_full';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_ads_stream;

CREATE TABLE IF NOT EXISTS hudi_ads_stream.ads_trade_stats_by_tm_category_full(
    `dt` STRING COMMENT 'stat date',
    `recent_days` BIGINT COMMENT 'recent days',
    `tm_id` STRING COMMENT 'trademark id',
    `tm_name` STRING COMMENT 'trademark name',
    `category1_id` STRING COMMENT 'category1 id',
    `category1_name` STRING COMMENT 'category1 name',
    `order_count` BIGINT COMMENT 'order count',
    `order_user_count` BIGINT COMMENT 'order user count',
    `order_sku_count` BIGINT COMMENT 'order sku count',
    `order_amount` DECIMAL(16, 2) COMMENT 'order amount',
    `category_amount_ratio` DECIMAL(10, 2) COMMENT 'category amount ratio',
    `tm_amount_ratio` DECIMAL(10, 2) COMMENT 'tm amount ratio',
    `sku_coverage_rate` DECIMAL(10, 2) COMMENT 'sku coverage rate',
    `wow_rate` DECIMAL(10, 2) COMMENT 'week over week rate',
    `yoy_rate` DECIMAL(10, 2) COMMENT 'year over year rate',
    `growth_type` STRING COMMENT 'growth type',
    PRIMARY KEY (`dt`, `recent_days`, `tm_id`, `category1_id`) NOT ENFORCED
) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

CREATE TEMPORARY TABLE tmp_ads_trade_stats_by_tm_category_order_detail_snapshot
WITH (
    'read.streaming.enabled' = 'false'
)
LIKE hudi_dwd_stream.dwd_trade_order_detail_full;

CREATE TEMPORARY TABLE tmp_ads_trade_stats_by_tm_category_dim_sku_snapshot
WITH (
    'read.streaming.enabled' = 'false'
)
LIKE hudi_dim.dim_sku_full;

CREATE TEMPORARY VIEW tmp_ads_trade_stats_by_tm_category_recent_days AS
SELECT CAST(7 AS BIGINT) AS recent_days
UNION ALL
SELECT CAST(30 AS BIGINT);

CREATE TEMPORARY VIEW tmp_ads_trade_stats_by_tm_category_dim_sku_valid AS
SELECT
    id,
    tm_id,
    tm_name,
    category1_id,
    category1_name
FROM tmp_ads_trade_stats_by_tm_category_dim_sku_snapshot
WHERE tm_id IS NOT NULL
  AND category1_id IS NOT NULL;

CREATE TEMPORARY VIEW tmp_ads_trade_stats_by_tm_category_base_detail AS
SELECT
    d.recent_days,
    od.user_id,
    od.order_id,
    od.sku_id,
    sku.tm_id,
    sku.tm_name,
    sku.category1_id,
    sku.category1_name,
    od.split_total_amount
FROM tmp_ads_trade_stats_by_tm_category_order_detail_snapshot od
CROSS JOIN tmp_ads_trade_stats_by_tm_category_recent_days d
JOIN tmp_ads_trade_stats_by_tm_category_dim_sku_valid sku
    ON od.sku_id = sku.id
WHERE CAST(od.k1 AS DATE) >= TIMESTAMPADD(DAY, -90, CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE))
  AND CAST(od.create_time AS DATE) >= TIMESTAMPADD(DAY, -CAST(d.recent_days - 1 AS INT), CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE))
  AND CAST(od.create_time AS DATE) <= CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE);

CREATE TEMPORARY VIEW tmp_ads_trade_stats_by_tm_category_final_stats AS
SELECT
    stats.recent_days,
    stats.tm_id,
    stats.tm_name,
    stats.category1_id,
    stats.category1_name,
    COUNT(DISTINCT stats.order_id) AS order_count,
    COUNT(DISTINCT stats.user_id) AS order_user_count,
    COUNT(DISTINCT stats.sku_id) AS order_sku_count,
    CAST(SUM(stats.split_total_amount) AS DECIMAL(16, 2)) AS order_amount
FROM tmp_ads_trade_stats_by_tm_category_base_detail stats
GROUP BY
    stats.recent_days,
    stats.tm_id,
    stats.tm_name,
    stats.category1_id,
    stats.category1_name;

CREATE TEMPORARY VIEW tmp_ads_trade_stats_by_tm_category_category_totals AS
SELECT
    recent_days,
    category1_id,
    CAST(SUM(split_total_amount) AS DECIMAL(16, 2)) AS category_total_amount,
    COUNT(DISTINCT sku_id) AS category_sku_count
FROM tmp_ads_trade_stats_by_tm_category_base_detail
GROUP BY recent_days, category1_id;

CREATE TEMPORARY VIEW tmp_ads_trade_stats_by_tm_category_tm_totals AS
SELECT
    recent_days,
    tm_id,
    CAST(SUM(split_total_amount) AS DECIMAL(16, 2)) AS tm_total_amount
FROM tmp_ads_trade_stats_by_tm_category_base_detail
GROUP BY recent_days, tm_id;

INSERT INTO hudi_ads_stream.ads_trade_stats_by_tm_category_full(
    dt,
    recent_days,
    tm_id,
    tm_name,
    category1_id,
    category1_name,
    order_count,
    order_user_count,
    order_sku_count,
    order_amount,
    category_amount_ratio,
    tm_amount_ratio,
    sku_coverage_rate,
    wow_rate,
    yoy_rate,
    growth_type
)
SELECT
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    final_stats.recent_days,
    CAST(final_stats.tm_id AS STRING) AS tm_id,
    final_stats.tm_name,
    CAST(final_stats.category1_id AS STRING) AS category1_id,
    final_stats.category1_name,
    final_stats.order_count,
    final_stats.order_user_count,
    final_stats.order_sku_count,
    final_stats.order_amount,
    CAST(final_stats.order_amount * 100 / category_totals.category_total_amount AS DECIMAL(10, 2)) AS category_amount_ratio,
    CAST(final_stats.order_amount * 100 / tm_totals.tm_total_amount AS DECIMAL(10, 2)) AS tm_amount_ratio,
    CAST(final_stats.order_sku_count * 100.0 / category_totals.category_sku_count AS DECIMAL(10, 2)) AS sku_coverage_rate,
    CAST(NULL AS DECIMAL(10, 2)) AS wow_rate,
    CAST(NULL AS DECIMAL(10, 2)) AS yoy_rate,
    CAST(NULL AS STRING) AS growth_type
FROM tmp_ads_trade_stats_by_tm_category_final_stats final_stats
JOIN tmp_ads_trade_stats_by_tm_category_category_totals category_totals
    ON final_stats.recent_days = category_totals.recent_days
   AND final_stats.category1_id = category_totals.category1_id
JOIN tmp_ads_trade_stats_by_tm_category_tm_totals tm_totals
    ON final_stats.recent_days = tm_totals.recent_days
   AND final_stats.tm_id = tm_totals.tm_id;

