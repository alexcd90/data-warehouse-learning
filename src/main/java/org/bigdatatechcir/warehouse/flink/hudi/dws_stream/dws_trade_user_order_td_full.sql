SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'hudi_dws_stream_dws_trade_user_order_td_full';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_dws_stream;

CREATE TABLE IF NOT EXISTS hudi_dws_stream.dws_trade_user_order_td_full(
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
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

CREATE TEMPORARY VIEW tmp_dws_trade_user_order_td_current_date_param AS
    SELECT CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE) AS cur_date
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
    FROM hudi_dws_stream.dws_trade_user_order_1d_full /*+ OPTIONS('read.streaming.enabled' = 'true') */
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

INSERT INTO hudi_dws_stream.dws_trade_user_order_td_full(
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

