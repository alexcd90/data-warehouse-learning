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
SET 'pipeline.name' = 'paimon_dws_stream_dws_trade_activity_order_nd_full';

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG paimon_hive;
CREATE DATABASE IF NOT EXISTS dws_stream;

CREATE TABLE IF NOT EXISTS dws_stream.dws_trade_activity_order_nd_full(
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
    'connector' = 'paimon',
    'metastore.partitioned-table' = 'true',
    'file.format' = 'parquet',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true',
    'partition.expiration-time' = '1 d',
    'partition.expiration-check-interval' = '1 h',
    'partition.timestamp-formatter' = 'yyyy-MM-dd',
    'partition.timestamp-pattern' = '$k1'
);

CREATE TEMPORARY VIEW tmp_dws_trade_activity_order_nd_current_date_param AS
    SELECT CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE) AS cur_date
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
    FROM dim_stream.dim_activity_full
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
    FROM dwd_stream.dwd_trade_order_detail_full
    CROSS JOIN tmp_dws_trade_activity_order_nd_current_date_param
    WHERE activity_id IS NOT NULL
        AND CAST(k1 AS DATE) BETWEEN cur_date - INTERVAL '29' DAY AND cur_date
    GROUP BY activity_id
;

INSERT INTO dws_stream.dws_trade_activity_order_nd_full(
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


