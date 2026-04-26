SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG paimon_hive;
CREATE DATABASE IF NOT EXISTS ads;

CREATE TABLE IF NOT EXISTS ads.ads_trade_stats_by_tm_category_full(
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
    'connector' = 'paimon',
    'file.format' = 'parquet',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true'
);

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
FROM dim.dim_sku_full
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
FROM dwd.dwd_trade_order_detail_full od
CROSS JOIN tmp_ads_trade_stats_by_tm_category_recent_days d
JOIN tmp_ads_trade_stats_by_tm_category_dim_sku_valid sku
    ON od.sku_id = sku.id
WHERE CAST(od.k1 AS DATE) >= TIMESTAMPADD(DAY, -90, CAST('${pdate}' AS DATE))
  AND CAST(od.create_time AS DATE) >= TIMESTAMPADD(DAY, -CAST(d.recent_days - 1 AS INT), CAST('${pdate}' AS DATE))
  AND CAST(od.create_time AS DATE) <= CAST('${pdate}' AS DATE);

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
    CAST(SUM(stats.split_total_amount) AS DECIMAL(16, 2)) AS order_amount,
    LAG(CAST(SUM(stats.split_total_amount) AS DECIMAL(16, 2)), 1, CAST(0 AS DECIMAL(16, 2))) OVER (
        PARTITION BY stats.tm_id, stats.category1_id
        ORDER BY stats.recent_days
    ) AS prev_7d_amount,
    LAG(CAST(SUM(stats.split_total_amount) AS DECIMAL(16, 2)), 52, CAST(0 AS DECIMAL(16, 2))) OVER (
        PARTITION BY stats.tm_id, stats.category1_id
        ORDER BY stats.recent_days
    ) AS prev_year_amount
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

INSERT INTO ads.ads_trade_stats_by_tm_category_full(
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
    '${pdate}' AS dt,
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
    CASE
        WHEN final_stats.recent_days = 7 AND final_stats.prev_7d_amount > 0
            THEN CAST((final_stats.order_amount - final_stats.prev_7d_amount) * 100 / final_stats.prev_7d_amount AS DECIMAL(10, 2))
        ELSE NULL
    END AS wow_rate,
    CASE
        WHEN final_stats.recent_days = 30 AND final_stats.prev_year_amount > 0
            THEN CAST((final_stats.order_amount - final_stats.prev_year_amount) * 100 / final_stats.prev_year_amount AS DECIMAL(10, 2))
        ELSE NULL
    END AS yoy_rate,
    CASE
        WHEN final_stats.recent_days = 7 AND final_stats.prev_7d_amount > 0 THEN
            CASE
                WHEN (final_stats.order_amount - final_stats.prev_7d_amount) * 100 / final_stats.prev_7d_amount >= 20 THEN 'HIGH_GROWTH'
                WHEN (final_stats.order_amount - final_stats.prev_7d_amount) * 100 / final_stats.prev_7d_amount >= 0 THEN 'STEADY_GROWTH'
                WHEN (final_stats.order_amount - final_stats.prev_7d_amount) * 100 / final_stats.prev_7d_amount >= -20 THEN 'DECLINE'
                ELSE 'SHARP_DECLINE'
            END
        ELSE NULL
    END AS growth_type
FROM tmp_ads_trade_stats_by_tm_category_final_stats final_stats
JOIN tmp_ads_trade_stats_by_tm_category_category_totals category_totals
    ON final_stats.recent_days = category_totals.recent_days
   AND final_stats.category1_id = category_totals.category1_id
JOIN tmp_ads_trade_stats_by_tm_category_tm_totals tm_totals
    ON final_stats.recent_days = tm_totals.recent_days
   AND final_stats.tm_id = tm_totals.tm_id;
