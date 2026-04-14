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

CREATE TABLE IF NOT EXISTS dws.dws_trade_user_order_td_full(
    `user_id` BIGINT COMMENT 'user id',
    `k1` STRING COMMENT 'partition field',
    `order_date_first` STRING COMMENT 'first order date',
    `order_date_last` STRING COMMENT 'last order date',
    `order_count_td` BIGINT COMMENT 'to date order count',
    `order_num_td` BIGINT COMMENT 'to date sku num',
    `original_amount_td` DECIMAL(16, 2) COMMENT 'to date original amount',
    `activity_reduce_amount_td` DECIMAL(16, 2) COMMENT 'to date activity reduce amount',
    `coupon_reduce_amount_td` DECIMAL(16, 2) COMMENT 'to date coupon reduce amount',
    `total_amount_td` DECIMAL(16, 2) COMMENT 'to date total amount',
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

CREATE TEMPORARY VIEW tmp_dws_trade_user_order_td_current_date_param AS
    SELECT CAST('${pdate}' AS DATE) AS cur_date
;

CREATE TEMPORARY VIEW tmp_dws_trade_user_order_td_order_1d AS
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

CREATE TEMPORARY VIEW tmp_dws_trade_user_order_td_order_agg AS
    SELECT
        o.user_id,
        MIN(o.dt) AS order_date_first_dt,
        MAX(o.dt) AS order_date_last_dt,
        SUM(o.order_count_1d) AS order_count_td,
        SUM(o.order_num_1d) AS order_num_td,
        SUM(o.order_original_amount_1d) AS original_amount_td,
        SUM(o.activity_reduce_amount_1d) AS activity_reduce_amount_td,
        SUM(o.coupon_reduce_amount_1d) AS coupon_reduce_amount_td,
        SUM(o.order_total_amount_1d) AS total_amount_td
    FROM tmp_dws_trade_user_order_td_order_1d o
    CROSS JOIN tmp_dws_trade_user_order_td_current_date_param cp
    WHERE o.dt <= cp.cur_date
    GROUP BY o.user_id
;

INSERT INTO dws.dws_trade_user_order_td_full(
    user_id,
    k1,
    order_date_first,
    order_date_last,
    order_count_td,
    order_num_td,
    original_amount_td,
    activity_reduce_amount_td,
    coupon_reduce_amount_td,
    total_amount_td
)
SELECT
    oa.user_id,
    CAST(cp.cur_date AS STRING) AS k1,
    CAST(oa.order_date_first_dt AS STRING) AS order_date_first,
    CAST(oa.order_date_last_dt AS STRING) AS order_date_last,
    oa.order_count_td,
    oa.order_num_td,
    oa.original_amount_td,
    oa.activity_reduce_amount_td,
    oa.coupon_reduce_amount_td,
    oa.total_amount_td
FROM tmp_dws_trade_user_order_td_order_agg oa
CROSS JOIN tmp_dws_trade_user_order_td_current_date_param cp;

