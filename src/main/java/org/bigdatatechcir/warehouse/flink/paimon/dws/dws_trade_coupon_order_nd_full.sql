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

CREATE DATABASE IF NOT EXISTS dws;

CREATE TABLE IF NOT EXISTS dws.dws_trade_coupon_order_nd_full(
    `coupon_id` BIGINT COMMENT 'coupon id',
    `k1` STRING COMMENT 'partition field',
    `coupon_name` STRING COMMENT 'coupon name',
    `coupon_type_code` STRING COMMENT 'coupon type code',
    `coupon_type_name` STRING COMMENT 'coupon type name',
    `coupon_rule` STRING COMMENT 'coupon rule',
    `start_date` STRING COMMENT 'coupon start date',
    `original_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day original amount',
    `coupon_reduce_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day coupon reduce amount',
    PRIMARY KEY (`coupon_id`, `k1`) NOT ENFORCED
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

CREATE TEMPORARY VIEW tmp_dws_trade_coupon_order_nd_current_date_param AS
    SELECT CAST('${pdate}' AS DATE) AS cur_date
;

CREATE TEMPORARY VIEW tmp_dws_trade_coupon_order_nd_coupon_union AS
    SELECT
        id AS coupon_id,
        MAX(coupon_name) AS coupon_name,
        MAX(coupon_type_code) AS coupon_type_code,
        MAX(coupon_type_name) AS coupon_type_name,
        MAX(benefit_rule) AS coupon_rule,
        MAX(DATE_FORMAT(start_time, 'yyyy-MM-dd')) AS start_date,
        CAST(0 AS DECIMAL(16, 2)) AS original_amount_30d,
        CAST(0 AS DECIMAL(16, 2)) AS coupon_reduce_amount_30d
    FROM dim.dim_coupon_full
    CROSS JOIN tmp_dws_trade_coupon_order_nd_current_date_param
    WHERE CAST(k1 AS DATE) <= cur_date
    GROUP BY id
    UNION ALL
    SELECT
        c.coupon_id,
        CAST('' AS STRING) AS coupon_name,
        CAST('' AS STRING) AS coupon_type_code,
        CAST('' AS STRING) AS coupon_type_name,
        CAST('' AS STRING) AS coupon_rule,
        CAST(NULL AS STRING) AS start_date,
        SUM(od.split_original_amount) AS original_amount_30d,
        SUM(COALESCE(od.split_coupon_amount, CAST(0 AS DECIMAL(16, 2)))) AS coupon_reduce_amount_30d
    FROM dwd.dwd_tool_coupon_order_full c
    JOIN dwd.dwd_trade_order_detail_full od
        ON c.order_id = od.order_id
       AND c.coupon_id = od.coupon_id
    CROSS JOIN tmp_dws_trade_coupon_order_nd_current_date_param
    WHERE CAST(od.k1 AS DATE) BETWEEN cur_date - INTERVAL '29' DAY AND cur_date
    GROUP BY c.coupon_id
;

INSERT INTO dws.dws_trade_coupon_order_nd_full(
    coupon_id,
    k1,
    coupon_name,
    coupon_type_code,
    coupon_type_name,
    coupon_rule,
    start_date,
    original_amount_30d,
    coupon_reduce_amount_30d
)
SELECT
    u.coupon_id,
    CAST(cp.cur_date AS STRING) AS k1,
    MAX(u.coupon_name) AS coupon_name,
    MAX(u.coupon_type_code) AS coupon_type_code,
    MAX(u.coupon_type_name) AS coupon_type_name,
    MAX(u.coupon_rule) AS coupon_rule,
    COALESCE(MAX(u.start_date), CAST(cp.cur_date AS STRING)) AS start_date,
    SUM(u.original_amount_30d) AS original_amount_30d,
    SUM(u.coupon_reduce_amount_30d) AS coupon_reduce_amount_30d
FROM tmp_dws_trade_coupon_order_nd_coupon_union u
CROSS JOIN tmp_dws_trade_coupon_order_nd_current_date_param cp
GROUP BY u.coupon_id, cp.cur_date;

