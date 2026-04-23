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
SET 'pipeline.name' = 'paimon_ads_stream_ads_sales_trend_forecast_full';

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG paimon_hive;
CREATE DATABASE IF NOT EXISTS ads_stream;

CREATE TABLE IF NOT EXISTS ads_stream.ads_sales_trend_forecast_full(
    `dt` STRING COMMENT 'stat date',
    `forecast_date` STRING COMMENT 'forecast date',
    `forecast_type` STRING COMMENT 'forecast type',
    `dimension_id` STRING COMMENT 'dimension id',
    `dimension_name` STRING COMMENT 'dimension name',
    `forecast_order_count` BIGINT COMMENT 'forecast order count',
    `forecast_order_amount` DECIMAL(16, 2) COMMENT 'forecast order amount',
    `forecast_user_count` BIGINT COMMENT 'forecast user count',
    `forecast_interval_lower` DECIMAL(16, 2) COMMENT 'forecast interval lower',
    `forecast_interval_upper` DECIMAL(16, 2) COMMENT 'forecast interval upper',
    `confidence_level` DECIMAL(10, 2) COMMENT 'confidence level',
    `historical_avg_amount` DECIMAL(16, 2) COMMENT 'historical avg amount',
    `seasonal_index` DECIMAL(10, 2) COMMENT 'seasonal index',
    `trend_coefficient` DECIMAL(10, 2) COMMENT 'trend coefficient',
    `prediction_model` STRING COMMENT 'prediction model',
    `anomaly_flag` BOOLEAN COMMENT 'anomaly flag',
    `expected_growth_rate` DECIMAL(10, 2) COMMENT 'expected growth rate',
    PRIMARY KEY (`dt`, `forecast_date`, `forecast_type`, `dimension_id`) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'file.format' = 'parquet',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true'
);

CREATE TEMPORARY VIEW tmp_ads_sales_trend_forecast_order_detail_snapshot AS
SELECT *
FROM dwd.dwd_trade_order_detail_full
;

CREATE TEMPORARY VIEW tmp_ads_sales_trend_forecast_dim_sku_snapshot AS
SELECT *
FROM dim_stream.dim_sku_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
;

CREATE TEMPORARY VIEW tmp_ads_sales_trend_forecast_current_date_param AS
SELECT CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE) AS cur_date
;

CREATE TEMPORARY VIEW tmp_ads_sales_trend_forecast_future_days AS
SELECT CAST(1 AS BIGINT) AS day_offset
UNION ALL
SELECT CAST(2 AS BIGINT) AS day_offset
UNION ALL
SELECT CAST(3 AS BIGINT) AS day_offset
UNION ALL
SELECT CAST(4 AS BIGINT) AS day_offset
UNION ALL
SELECT CAST(5 AS BIGINT) AS day_offset
UNION ALL
SELECT CAST(6 AS BIGINT) AS day_offset
UNION ALL
SELECT CAST(7 AS BIGINT) AS day_offset
;

CREATE TEMPORARY VIEW tmp_ads_sales_trend_forecast_future_dates AS
SELECT
    d.day_offset,
    cp.cur_date + CAST(d.day_offset AS INTERVAL DAY) AS forecast_date
FROM tmp_ads_sales_trend_forecast_future_days d
CROSS JOIN tmp_ads_sales_trend_forecast_current_date_param cp
;

CREATE TEMPORARY VIEW tmp_ads_sales_trend_forecast_daily_overall AS
SELECT
    CAST(od.k1 AS DATE) AS dt,
    COUNT(DISTINCT od.order_id) AS order_count,
    AVG(CAST(0 AS DOUBLE)) + SUM(CAST(od.split_total_amount AS DOUBLE)) AS order_amount,
    COUNT(DISTINCT od.user_id) AS user_count
FROM tmp_ads_sales_trend_forecast_order_detail_snapshot od
CROSS JOIN tmp_ads_sales_trend_forecast_current_date_param cp
WHERE CAST(od.k1 AS DATE) BETWEEN cp.cur_date - INTERVAL '89' DAY AND cp.cur_date
GROUP BY CAST(od.k1 AS DATE)
;

CREATE TEMPORARY VIEW tmp_ads_sales_trend_forecast_overall_avg AS
SELECT
    COALESCE(AVG(order_amount), CAST(0 AS DOUBLE)) AS historical_avg_amount
FROM tmp_ads_sales_trend_forecast_daily_overall
;

CREATE TEMPORARY VIEW tmp_ads_sales_trend_forecast_overall_trend AS
SELECT
    CASE
        WHEN prev_avg_amount = 0 THEN CAST(0 AS DOUBLE)
        ELSE (recent_avg_amount - prev_avg_amount) / prev_avg_amount
    END AS trend_factor
FROM (
    SELECT
        COALESCE(AVG(CASE WHEN dt BETWEEN cp.cur_date - INTERVAL '29' DAY AND cp.cur_date THEN order_amount END), CAST(0 AS DOUBLE)) AS recent_avg_amount,
        COALESCE(AVG(CASE WHEN dt BETWEEN cp.cur_date - INTERVAL '59' DAY AND cp.cur_date - INTERVAL '30' DAY THEN order_amount END), CAST(0 AS DOUBLE)) AS prev_avg_amount
    FROM tmp_ads_sales_trend_forecast_daily_overall
    CROSS JOIN tmp_ads_sales_trend_forecast_current_date_param cp
) t
;

CREATE TEMPORARY VIEW tmp_ads_sales_trend_forecast_overall_baseline AS
SELECT
    f.day_offset,
    f.forecast_date,
    COALESCE(AVG(CAST(d.order_count AS DOUBLE)), CAST(0 AS DOUBLE)) AS base_order_count,
    COALESCE(AVG(d.order_amount), CAST(0 AS DOUBLE)) AS base_order_amount,
    COALESCE(AVG(CAST(d.user_count AS DOUBLE)), CAST(0 AS DOUBLE)) AS base_user_count
FROM tmp_ads_sales_trend_forecast_future_dates f
CROSS JOIN tmp_ads_sales_trend_forecast_current_date_param cp
LEFT JOIN tmp_ads_sales_trend_forecast_daily_overall d
    ON d.dt BETWEEN cp.cur_date - INTERVAL '55' DAY AND cp.cur_date
   AND DAYOFWEEK(d.dt) = DAYOFWEEK(f.forecast_date)
GROUP BY f.day_offset, f.forecast_date
;

CREATE TEMPORARY VIEW tmp_ads_sales_trend_forecast_sku_dim AS
SELECT
    s.id,
    CAST(s.category1_id AS STRING) AS category1_id,
    COALESCE(s.category1_name, '') AS category1_name
FROM tmp_ads_sales_trend_forecast_dim_sku_snapshot s
JOIN (
    SELECT MAX(k1) AS max_k1
    FROM tmp_ads_sales_trend_forecast_dim_sku_snapshot
) m
    ON s.k1 = m.max_k1
WHERE s.category1_id IS NOT NULL
;

CREATE TEMPORARY VIEW tmp_ads_sales_trend_forecast_daily_category AS
SELECT
    CAST(od.k1 AS DATE) AS dt,
    sd.category1_id,
    sd.category1_name,
    COUNT(DISTINCT od.order_id) AS order_count,
    AVG(CAST(0 AS DOUBLE)) + SUM(CAST(od.split_total_amount AS DOUBLE)) AS order_amount,
    COUNT(DISTINCT od.user_id) AS user_count
FROM tmp_ads_sales_trend_forecast_order_detail_snapshot od
JOIN tmp_ads_sales_trend_forecast_sku_dim sd
    ON od.sku_id = sd.id
CROSS JOIN tmp_ads_sales_trend_forecast_current_date_param cp
WHERE CAST(od.k1 AS DATE) BETWEEN cp.cur_date - INTERVAL '89' DAY AND cp.cur_date
GROUP BY CAST(od.k1 AS DATE), sd.category1_id, sd.category1_name
;

CREATE TEMPORARY VIEW tmp_ads_sales_trend_forecast_category_avg AS
SELECT
    category1_id,
    category1_name,
    COALESCE(AVG(order_amount), CAST(0 AS DOUBLE)) AS historical_avg_amount
FROM tmp_ads_sales_trend_forecast_daily_category
GROUP BY category1_id, category1_name
;

CREATE TEMPORARY VIEW tmp_ads_sales_trend_forecast_category_trend AS
SELECT
    category1_id,
    category1_name,
    CASE
        WHEN prev_avg_amount = 0 THEN CAST(0 AS DOUBLE)
        ELSE (recent_avg_amount - prev_avg_amount) / prev_avg_amount
    END AS trend_factor
FROM (
    SELECT
        dc.category1_id,
        dc.category1_name,
        COALESCE(AVG(CASE WHEN dc.dt BETWEEN cp.cur_date - INTERVAL '29' DAY AND cp.cur_date THEN dc.order_amount END), CAST(0 AS DOUBLE)) AS recent_avg_amount,
        COALESCE(AVG(CASE WHEN dc.dt BETWEEN cp.cur_date - INTERVAL '59' DAY AND cp.cur_date - INTERVAL '30' DAY THEN dc.order_amount END), CAST(0 AS DOUBLE)) AS prev_avg_amount
    FROM tmp_ads_sales_trend_forecast_daily_category dc
    CROSS JOIN tmp_ads_sales_trend_forecast_current_date_param cp
    GROUP BY dc.category1_id, dc.category1_name
) t
;

CREATE TEMPORARY VIEW tmp_ads_sales_trend_forecast_category_baseline AS
SELECT
    dc.category1_id,
    dc.category1_name,
    f.day_offset,
    f.forecast_date,
    COALESCE(AVG(CAST(dc.order_count AS DOUBLE)), CAST(0 AS DOUBLE)) AS base_order_count,
    COALESCE(AVG(dc.order_amount), CAST(0 AS DOUBLE)) AS base_order_amount,
    COALESCE(AVG(CAST(dc.user_count AS DOUBLE)), CAST(0 AS DOUBLE)) AS base_user_count
FROM tmp_ads_sales_trend_forecast_daily_category dc
JOIN tmp_ads_sales_trend_forecast_future_dates f
    ON DAYOFWEEK(dc.dt) = DAYOFWEEK(f.forecast_date)
CROSS JOIN tmp_ads_sales_trend_forecast_current_date_param cp
WHERE dc.dt BETWEEN cp.cur_date - INTERVAL '55' DAY AND cp.cur_date
GROUP BY dc.category1_id, dc.category1_name, f.day_offset, f.forecast_date
;

INSERT INTO ads_stream.ads_sales_trend_forecast_full(
    dt,
    forecast_date,
    forecast_type,
    dimension_id,
    dimension_name,
    forecast_order_count,
    forecast_order_amount,
    forecast_user_count,
    forecast_interval_lower,
    forecast_interval_upper,
    confidence_level,
    historical_avg_amount,
    seasonal_index,
    trend_coefficient,
    prediction_model,
    anomaly_flag,
    expected_growth_rate
)
SELECT
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    CAST(b.forecast_date AS STRING) AS forecast_date,
    '整体' AS forecast_type,
    'all' AS dimension_id,
    '全部' AS dimension_name,
    CAST(ROUND(b.base_order_count * (1 + t.trend_factor), 0) AS BIGINT) AS forecast_order_count,
    CAST(ROUND(b.base_order_amount * (1 + t.trend_factor), 2) AS DECIMAL(16, 2)) AS forecast_order_amount,
    CAST(ROUND(b.base_user_count * (1 + t.trend_factor), 0) AS BIGINT) AS forecast_user_count,
    CAST(ROUND(b.base_order_amount * (1 + t.trend_factor) * (1 - (0.10 + CAST(b.day_offset AS DOUBLE) * 0.01)), 2) AS DECIMAL(16, 2)) AS forecast_interval_lower,
    CAST(ROUND(b.base_order_amount * (1 + t.trend_factor) * (1 + (0.10 + CAST(b.day_offset AS DOUBLE) * 0.01)), 2) AS DECIMAL(16, 2)) AS forecast_interval_upper,
    CAST(ROUND(0.95 - CAST(b.day_offset AS DOUBLE) * 0.01, 2) AS DECIMAL(10, 2)) AS confidence_level,
    CAST(ROUND(a.historical_avg_amount, 2) AS DECIMAL(16, 2)) AS historical_avg_amount,
    CAST(
        ROUND(
            CASE
                WHEN a.historical_avg_amount = 0 THEN 1
                ELSE b.base_order_amount / a.historical_avg_amount
            END,
            2
        ) AS DECIMAL(10, 2)
    ) AS seasonal_index,
    CAST(ROUND(t.trend_factor, 2) AS DECIMAL(10, 2)) AS trend_coefficient,
    'TREND+WEEKDAY' AS prediction_model,
    CASE
        WHEN ABS(t.trend_factor) > 0.30 THEN TRUE
        ELSE FALSE
    END AS anomaly_flag,
    CAST(
        ROUND(
            (
                t.trend_factor
                + CASE
                    WHEN a.historical_avg_amount = 0 THEN 0
                    ELSE (b.base_order_amount / a.historical_avg_amount) - 1
                  END
            ) * 100,
            2
        ) AS DECIMAL(10, 2)
    ) AS expected_growth_rate
FROM tmp_ads_sales_trend_forecast_overall_baseline b
CROSS JOIN tmp_ads_sales_trend_forecast_overall_trend t
CROSS JOIN tmp_ads_sales_trend_forecast_overall_avg a
UNION ALL
SELECT
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    CAST(b.forecast_date AS STRING) AS forecast_date,
    '品类' AS forecast_type,
    b.category1_id AS dimension_id,
    b.category1_name AS dimension_name,
    CAST(ROUND(b.base_order_count * (1 + COALESCE(t.trend_factor, CAST(0 AS DOUBLE))), 0) AS BIGINT) AS forecast_order_count,
    CAST(ROUND(b.base_order_amount * (1 + COALESCE(t.trend_factor, CAST(0 AS DOUBLE))), 2) AS DECIMAL(16, 2)) AS forecast_order_amount,
    CAST(ROUND(b.base_user_count * (1 + COALESCE(t.trend_factor, CAST(0 AS DOUBLE))), 0) AS BIGINT) AS forecast_user_count,
    CAST(ROUND(b.base_order_amount * (1 + COALESCE(t.trend_factor, CAST(0 AS DOUBLE))) * (1 - (0.12 + CAST(b.day_offset AS DOUBLE) * 0.01)), 2) AS DECIMAL(16, 2)) AS forecast_interval_lower,
    CAST(ROUND(b.base_order_amount * (1 + COALESCE(t.trend_factor, CAST(0 AS DOUBLE))) * (1 + (0.12 + CAST(b.day_offset AS DOUBLE) * 0.01)), 2) AS DECIMAL(16, 2)) AS forecast_interval_upper,
    CAST(ROUND(0.93 - CAST(b.day_offset AS DOUBLE) * 0.01, 2) AS DECIMAL(10, 2)) AS confidence_level,
    CAST(ROUND(COALESCE(a.historical_avg_amount, CAST(0 AS DOUBLE)), 2) AS DECIMAL(16, 2)) AS historical_avg_amount,
    CAST(
        ROUND(
            CASE
                WHEN COALESCE(a.historical_avg_amount, CAST(0 AS DOUBLE)) = 0 THEN 1
                ELSE b.base_order_amount / a.historical_avg_amount
            END,
            2
        ) AS DECIMAL(10, 2)
    ) AS seasonal_index,
    CAST(ROUND(COALESCE(t.trend_factor, CAST(0 AS DOUBLE)), 2) AS DECIMAL(10, 2)) AS trend_coefficient,
    'TREND+WEEKDAY' AS prediction_model,
    CASE
        WHEN ABS(COALESCE(t.trend_factor, CAST(0 AS DOUBLE))) > 0.30 THEN TRUE
        ELSE FALSE
    END AS anomaly_flag,
    CAST(
        ROUND(
            (
                COALESCE(t.trend_factor, CAST(0 AS DOUBLE))
                + CASE
                    WHEN COALESCE(a.historical_avg_amount, CAST(0 AS DOUBLE)) = 0 THEN CAST(0 AS DOUBLE)
                    ELSE (b.base_order_amount / a.historical_avg_amount) - 1
                  END
            ) * 100,
            2
        ) AS DECIMAL(10, 2)
    ) AS expected_growth_rate
FROM tmp_ads_sales_trend_forecast_category_baseline b
LEFT JOIN tmp_ads_sales_trend_forecast_category_trend t
    ON b.category1_id = t.category1_id
LEFT JOIN tmp_ads_sales_trend_forecast_category_avg a
    ON b.category1_id = a.category1_id
;


