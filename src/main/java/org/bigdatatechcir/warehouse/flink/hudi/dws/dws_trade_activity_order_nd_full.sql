SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_dws;

CREATE TABLE IF NOT EXISTS hudi_dws.dws_trade_activity_order_nd_full(
    `activity_id` BIGINT COMMENT 'activity id',
    `k1` STRING COMMENT 'partition field',
    `activity_name` STRING COMMENT 'activity name',
    `activity_type_code` STRING COMMENT 'activity type code',
    `activity_type_name` STRING COMMENT 'activity type name',
    `start_date` STRING COMMENT 'activity start date',
    `original_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day original amount',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day activity reduce amount',
    PRIMARY KEY (`activity_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

CREATE TEMPORARY VIEW tmp_dws_trade_activity_order_nd_current_date_param AS
    SELECT CAST('${pdate}' AS DATE) AS cur_date
;

CREATE TEMPORARY VIEW tmp_dws_trade_activity_order_nd_activity_union AS
    SELECT
        activity_id,
        MAX(activity_name) AS activity_name,
        MAX(activity_type_code) AS activity_type_code,
        MAX(activity_type_name) AS activity_type_name,
        MAX(SUBSTRING(start_time, 1, 10)) AS start_date,
        CAST(0 AS DECIMAL(16, 2)) AS original_amount_30d,
        CAST(0 AS DECIMAL(16, 2)) AS activity_reduce_amount_30d
    FROM hudi_dim.dim_activity_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
    CROSS JOIN tmp_dws_trade_activity_order_nd_current_date_param
    WHERE CAST(k1 AS DATE) <= cur_date
    GROUP BY activity_id
    UNION ALL
    SELECT
        activity_id,
        CAST('' AS STRING) AS activity_name,
        CAST('' AS STRING) AS activity_type_code,
        CAST('' AS STRING) AS activity_type_name,
        CAST(NULL AS STRING) AS start_date,
        SUM(split_original_amount) AS original_amount_30d,
        SUM(COALESCE(split_activity_amount, CAST(0 AS DECIMAL(16, 2)))) AS activity_reduce_amount_30d
    FROM hudi_dwd.dwd_trade_order_detail_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
    CROSS JOIN tmp_dws_trade_activity_order_nd_current_date_param
    WHERE activity_id IS NOT NULL
        AND CAST(k1 AS DATE) BETWEEN cur_date - INTERVAL '29' DAY AND cur_date
    GROUP BY activity_id
;

INSERT INTO hudi_dws.dws_trade_activity_order_nd_full(
    activity_id,
    k1,
    activity_name,
    activity_type_code,
    activity_type_name,
    start_date,
    original_amount_30d,
    activity_reduce_amount_30d
)
SELECT
    u.activity_id,
    CAST(cp.cur_date AS STRING) AS k1,
    MAX(u.activity_name) AS activity_name,
    MAX(u.activity_type_code) AS activity_type_code,
    MAX(u.activity_type_name) AS activity_type_name,
    COALESCE(MAX(u.start_date), CAST(cp.cur_date AS STRING)) AS start_date,
    SUM(u.original_amount_30d) AS original_amount_30d,
    SUM(u.activity_reduce_amount_30d) AS activity_reduce_amount_30d
FROM tmp_dws_trade_activity_order_nd_activity_union u
CROSS JOIN tmp_dws_trade_activity_order_nd_current_date_param cp
GROUP BY u.activity_id, cp.cur_date;
