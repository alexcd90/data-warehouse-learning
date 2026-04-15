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

CREATE TABLE IF NOT EXISTS dws.dws_trade_user_order_nd_full(
    `user_id` BIGINT COMMENT 'user id',
    `k1` STRING COMMENT 'partition field',
    `order_count_7d` BIGINT COMMENT 'recent 7 day order count',
    `order_num_7d` BIGINT COMMENT 'recent 7 day sku count',
    `order_original_amount_7d` DECIMAL(16, 2) COMMENT 'recent 7 day original amount',
    `activity_reduce_amount_7d` DECIMAL(16, 2) COMMENT 'recent 7 day activity reduce amount',
    `coupon_reduce_amount_7d` DECIMAL(16, 2) COMMENT 'recent 7 day coupon reduce amount',
    `order_total_amount_7d` DECIMAL(16, 2) COMMENT 'recent 7 day order total amount',
    `order_count_30d` BIGINT COMMENT 'recent 30 day order count',
    `order_num_30d` BIGINT COMMENT 'recent 30 day sku count',
    `order_original_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day original amount',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day activity reduce amount',
    `coupon_reduce_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day coupon reduce amount',
    `order_total_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day order total amount',
    PRIMARY KEY (`user_id`, `k1`) NOT ENFORCED
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

CREATE TEMPORARY VIEW tmp_dws_trade_user_order_nd_current_date_param AS
    SELECT CAST('${pdate}' AS DATE) AS cur_date
;

CREATE TEMPORARY VIEW tmp_dws_trade_user_order_nd_order_1d AS
    SELECT
        user_id,
        CAST(k1 AS DATE) AS dt,
        order_count_1d,
        order_num_1d,
        order_original_amount_1d,
        activity_reduce_amount_1d,
        coupon_reduce_amount_1d,
        order_total_amount_1d
    FROM dws.dws_trade_user_order_1d_full
;

CREATE TEMPORARY VIEW tmp_dws_trade_user_order_nd_order_agg AS
    SELECT
        o.user_id,
        SUM(CASE WHEN o.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN o.order_count_1d ELSE 0 END) AS order_count_7d,
        SUM(CASE WHEN o.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN o.order_num_1d ELSE 0 END) AS order_num_7d,
        SUM(CASE WHEN o.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN o.order_original_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS order_original_amount_7d,
        SUM(CASE WHEN o.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN o.activity_reduce_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS activity_reduce_amount_7d,
        SUM(CASE WHEN o.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN o.coupon_reduce_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS coupon_reduce_amount_7d,
        SUM(CASE WHEN o.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN o.order_total_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS order_total_amount_7d,
        SUM(o.order_count_1d) AS order_count_30d,
        SUM(o.order_num_1d) AS order_num_30d,
        SUM(o.order_original_amount_1d) AS order_original_amount_30d,
        SUM(o.activity_reduce_amount_1d) AS activity_reduce_amount_30d,
        SUM(o.coupon_reduce_amount_1d) AS coupon_reduce_amount_30d,
        SUM(o.order_total_amount_1d) AS order_total_amount_30d
    FROM tmp_dws_trade_user_order_nd_order_1d o
    CROSS JOIN tmp_dws_trade_user_order_nd_current_date_param cp
    WHERE o.dt BETWEEN cp.cur_date - INTERVAL '29' DAY AND cp.cur_date
    GROUP BY o.user_id
;

INSERT INTO dws.dws_trade_user_order_nd_full(
    user_id,
    k1,
    order_count_7d,
    order_num_7d,
    order_original_amount_7d,
    activity_reduce_amount_7d,
    coupon_reduce_amount_7d,
    order_total_amount_7d,
    order_count_30d,
    order_num_30d,
    order_original_amount_30d,
    activity_reduce_amount_30d,
    coupon_reduce_amount_30d,
    order_total_amount_30d
)
SELECT
    oa.user_id,
    CAST(cp.cur_date AS STRING) AS k1,
    oa.order_count_7d,
    oa.order_num_7d,
    oa.order_original_amount_7d,
    oa.activity_reduce_amount_7d,
    oa.coupon_reduce_amount_7d,
    oa.order_total_amount_7d,
    oa.order_count_30d,
    oa.order_num_30d,
    oa.order_original_amount_30d,
    oa.activity_reduce_amount_30d,
    oa.coupon_reduce_amount_30d,
    oa.order_total_amount_30d
FROM tmp_dws_trade_user_order_nd_order_agg oa
CROSS JOIN tmp_dws_trade_user_order_nd_current_date_param cp;
