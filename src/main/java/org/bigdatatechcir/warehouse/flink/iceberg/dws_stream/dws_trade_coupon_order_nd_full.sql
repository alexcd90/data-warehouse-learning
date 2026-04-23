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
SET 'pipeline.name' = 'iceberg_dws_stream_dws_trade_coupon_order_nd_full';

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG iceberg_catalog;
CREATE DATABASE IF NOT EXISTS iceberg_dws_stream;

CREATE TABLE IF NOT EXISTS iceberg_dws_stream.dws_trade_coupon_order_nd_full(
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
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

CREATE TEMPORARY VIEW tmp_dws_trade_coupon_order_nd_current_date_param AS
    SELECT CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE) AS cur_date
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
    FROM iceberg_dim_stream.dim_coupon_full
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
    FROM iceberg_dwd_stream.dwd_tool_coupon_order_full c
    JOIN iceberg_dwd_stream.dwd_trade_order_detail_full od
        ON c.order_id = od.order_id
       AND c.coupon_id = od.coupon_id
    CROSS JOIN tmp_dws_trade_coupon_order_nd_current_date_param
    WHERE CAST(od.k1 AS DATE) BETWEEN cur_date - INTERVAL '29' DAY AND cur_date
    GROUP BY c.coupon_id
;

INSERT INTO iceberg_dws_stream.dws_trade_coupon_order_nd_full(
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


