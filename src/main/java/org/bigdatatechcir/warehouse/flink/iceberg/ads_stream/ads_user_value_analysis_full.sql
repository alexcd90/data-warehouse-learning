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
SET 'pipeline.name' = 'iceberg_ads_stream_ads_user_value_analysis_full';

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

CREATE TABLE IF NOT EXISTS iceberg_ads_stream.ads_user_value_analysis_full(
    `dt` STRING COMMENT 'stat date',
    `user_id` STRING COMMENT 'user id',
    `order_count_td` BIGINT COMMENT 'to date order count',
    `order_amount_td` DECIMAL(16, 2) COMMENT 'to date order amount',
    `order_last_date` STRING COMMENT 'last order date',
    `order_first_date` STRING COMMENT 'first order date',
    `login_count_td` BIGINT COMMENT 'to date login count',
    `login_last_date` STRING COMMENT 'last login date',
    `average_order_amount` DECIMAL(16, 2) COMMENT 'avg order amount',
    `purchase_cycle_days` BIGINT COMMENT 'avg purchase cycle days',
    `account_days` BIGINT COMMENT 'account days',
    `life_time_value` DECIMAL(16, 2) COMMENT 'life time value',
    `recency_score` BIGINT COMMENT 'recency score',
    `frequency_score` BIGINT COMMENT 'frequency score',
    `monetary_score` BIGINT COMMENT 'monetary score',
    `rfm_score` BIGINT COMMENT 'rfm score',
    `user_value_level` STRING COMMENT 'user value level',
    `active_status` STRING COMMENT 'active status',
    `life_cycle_status` STRING COMMENT 'life cycle status',
    `shopping_preference` STRING COMMENT 'shopping preference',
    `growth_trend` STRING COMMENT 'growth trend',
    PRIMARY KEY (`dt`, `user_id`) NOT ENFORCED
) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

CREATE TEMPORARY VIEW tmp_ads_user_value_analysis_dim_user_zip_snapshot AS
SELECT *
FROM iceberg_dim_stream.dim_user_zip_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
;

CREATE TEMPORARY VIEW tmp_ads_user_value_analysis_order_td_snapshot AS
SELECT *
FROM iceberg_dws_stream.dws_trade_user_order_td_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
;

CREATE TEMPORARY VIEW tmp_ads_user_value_analysis_login_td_snapshot AS
SELECT *
FROM iceberg_dws_stream.dws_user_user_login_td_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
;

CREATE TEMPORARY VIEW tmp_ads_user_value_analysis_previous_score_snapshot AS
SELECT *
FROM iceberg_ads_stream.ads_user_value_analysis_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
;

CREATE TEMPORARY VIEW tmp_ads_user_value_analysis_current_date_param AS
SELECT CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE) AS cur_date
;

CREATE TEMPORARY VIEW tmp_ads_user_value_analysis_user_dim AS
SELECT
    CAST(d.id AS STRING) AS user_id,
    CAST(d.create_time AS DATE) AS register_date
FROM tmp_ads_user_value_analysis_dim_user_zip_snapshot d
JOIN (
    SELECT MAX(k1) AS max_k1
    FROM tmp_ads_user_value_analysis_dim_user_zip_snapshot
) m
    ON d.k1 = m.max_k1
;

CREATE TEMPORARY VIEW tmp_ads_user_value_analysis_previous_score AS
SELECT
    p.user_id,
    p.rfm_score AS previous_rfm_score
FROM tmp_ads_user_value_analysis_previous_score_snapshot p
CROSS JOIN tmp_ads_user_value_analysis_current_date_param cp
WHERE p.dt = CAST(cp.cur_date - INTERVAL '30' DAY AS STRING)
;

CREATE TEMPORARY VIEW tmp_ads_user_value_analysis_base AS
SELECT
    CAST(o.user_id AS STRING) AS user_id,
    o.order_count_td,
    o.total_amount_td AS order_amount_td,
    CAST(o.order_date_last AS DATE) AS order_last_date,
    CAST(o.order_date_first AS DATE) AS order_first_date,
    COALESCE(l.login_count_td, 0) AS login_count_td,
    COALESCE(CAST(l.login_date_last AS DATE), CAST(o.order_date_last AS DATE)) AS login_last_date,
    COALESCE(d.register_date, CAST(o.order_date_first AS DATE)) AS register_date,
    COALESCE(ps.previous_rfm_score, 0) AS previous_rfm_score
FROM tmp_ads_user_value_analysis_order_td_snapshot o
LEFT JOIN tmp_ads_user_value_analysis_login_td_snapshot l
    ON o.user_id = l.user_id
   AND l.k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
LEFT JOIN tmp_ads_user_value_analysis_user_dim d
    ON CAST(o.user_id AS STRING) = d.user_id
LEFT JOIN tmp_ads_user_value_analysis_previous_score ps
    ON CAST(o.user_id AS STRING) = ps.user_id
WHERE o.k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
;

CREATE TEMPORARY VIEW tmp_ads_user_value_analysis_scored AS
SELECT
    b.user_id,
    b.order_count_td,
    b.order_amount_td,
    b.order_last_date,
    b.order_first_date,
    b.login_count_td,
    b.login_last_date,
    CASE
        WHEN b.order_count_td > 0 THEN b.order_amount_td / CAST(b.order_count_td AS DECIMAL(16, 2))
        ELSE CAST(0 AS DECIMAL(16, 2))
    END AS average_order_amount,
    CASE
        WHEN b.order_count_td > 1 THEN CAST(TIMESTAMPDIFF(DAY, b.order_first_date, b.order_last_date) / (b.order_count_td - 1) AS BIGINT)
        ELSE NULL
    END AS purchase_cycle_days,
    CAST(
        CASE
            WHEN TIMESTAMPDIFF(DAY, b.register_date, cp.cur_date) < 0 THEN 0
            ELSE TIMESTAMPDIFF(DAY, b.register_date, cp.cur_date)
        END
        AS BIGINT
    ) AS account_days,
    CAST(
        CASE
            WHEN b.order_count_td > 0 AND GREATEST(TIMESTAMPDIFF(DAY, b.register_date, cp.cur_date), 1) > 0
            THEN
                (
                    (CAST(b.order_amount_td AS DOUBLE) / CAST(b.order_count_td AS DOUBLE))
                    * (CAST(b.order_count_td AS DOUBLE) * CAST(365 AS DOUBLE) / CAST(GREATEST(TIMESTAMPDIFF(DAY, b.register_date, cp.cur_date), 1) AS DOUBLE))
                    * CAST(3 AS DOUBLE)
                )
            ELSE CAST(0 AS DOUBLE)
        END
        AS DECIMAL(16, 2)
    ) AS life_time_value,
    CASE
        WHEN TIMESTAMPDIFF(DAY, b.order_last_date, cp.cur_date) <= 30 THEN 5
        WHEN TIMESTAMPDIFF(DAY, b.order_last_date, cp.cur_date) <= 60 THEN 4
        WHEN TIMESTAMPDIFF(DAY, b.order_last_date, cp.cur_date) <= 90 THEN 3
        WHEN TIMESTAMPDIFF(DAY, b.order_last_date, cp.cur_date) <= 180 THEN 2
        ELSE 1
    END AS recency_score,
    CASE
        WHEN b.order_count_td >= 20 THEN 5
        WHEN b.order_count_td >= 10 THEN 4
        WHEN b.order_count_td >= 5 THEN 3
        WHEN b.order_count_td >= 2 THEN 2
        ELSE 1
    END AS frequency_score,
    CASE
        WHEN b.order_amount_td >= 10000 THEN 5
        WHEN b.order_amount_td >= 5000 THEN 4
        WHEN b.order_amount_td >= 2000 THEN 3
        WHEN b.order_amount_td >= 500 THEN 2
        ELSE 1
    END AS monetary_score,
    b.previous_rfm_score
FROM tmp_ads_user_value_analysis_base b
CROSS JOIN tmp_ads_user_value_analysis_current_date_param cp
;

INSERT INTO iceberg_ads_stream.ads_user_value_analysis_full(
    dt,
    user_id,
    order_count_td,
    order_amount_td,
    order_last_date,
    order_first_date,
    login_count_td,
    login_last_date,
    average_order_amount,
    purchase_cycle_days,
    account_days,
    life_time_value,
    recency_score,
    frequency_score,
    monetary_score,
    rfm_score,
    user_value_level,
    active_status,
    life_cycle_status,
    shopping_preference,
    growth_trend
)
SELECT
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    s.user_id,
    s.order_count_td,
    s.order_amount_td,
    CAST(s.order_last_date AS STRING) AS order_last_date,
    CAST(s.order_first_date AS STRING) AS order_first_date,
    s.login_count_td,
    CAST(s.login_last_date AS STRING) AS login_last_date,
    s.average_order_amount,
    s.purchase_cycle_days,
    s.account_days,
    s.life_time_value,
    s.recency_score,
    s.frequency_score,
    s.monetary_score,
    s.recency_score + s.frequency_score + s.monetary_score AS rfm_score,
    CASE
        WHEN s.recency_score + s.frequency_score + s.monetary_score >= 13 THEN '高价值'
        WHEN s.recency_score + s.frequency_score + s.monetary_score >= 10 THEN '中高价值'
        WHEN s.recency_score + s.frequency_score + s.monetary_score >= 7 THEN '中价值'
        WHEN s.recency_score + s.frequency_score + s.monetary_score >= 4 THEN '低价值'
        ELSE '流失风险'
    END AS user_value_level,
    CASE
        WHEN TIMESTAMPDIFF(DAY, s.order_last_date, cp.cur_date) <= 30 OR TIMESTAMPDIFF(DAY, s.login_last_date, cp.cur_date) <= 7 THEN '活跃'
        WHEN TIMESTAMPDIFF(DAY, s.order_last_date, cp.cur_date) <= 90 OR TIMESTAMPDIFF(DAY, s.login_last_date, cp.cur_date) <= 30 THEN '沉默'
        ELSE '流失'
    END AS active_status,
    CASE
        WHEN TIMESTAMPDIFF(DAY, s.order_first_date, cp.cur_date) <= 30 AND s.order_count_td <= 2 THEN '新用户'
        WHEN s.previous_rfm_score <= 6 AND s.recency_score >= 4 THEN '回流'
        WHEN s.recency_score >= 4 AND s.frequency_score >= 4 THEN '成熟期'
        WHEN s.recency_score >= 4 AND s.frequency_score >= 2 THEN '成长期'
        WHEN s.recency_score <= 2 AND s.frequency_score >= 3 THEN '衰退期'
        ELSE '新用户'
    END AS life_cycle_status,
    CASE
        WHEN s.frequency_score >= 4 AND s.monetary_score <= 3 THEN '高频低额'
        WHEN s.frequency_score <= 3 AND s.monetary_score >= 4 THEN '低频高额'
        WHEN s.frequency_score >= 4 AND s.monetary_score >= 4 THEN '高频高额'
        ELSE '低频低额'
    END AS shopping_preference,
    CASE
        WHEN s.recency_score + s.frequency_score + s.monetary_score > s.previous_rfm_score THEN '上升'
        WHEN s.recency_score + s.frequency_score + s.monetary_score < s.previous_rfm_score THEN '下降'
        ELSE '稳定'
    END AS growth_trend
FROM tmp_ads_user_value_analysis_scored s
CROSS JOIN tmp_ads_user_value_analysis_current_date_param cp
;


