SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'hudi_dws_stream_dws_trade_user_sku_order_nd_full';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_dws_stream;

CREATE TABLE IF NOT EXISTS hudi_dws_stream.dws_trade_user_sku_order_nd_full(
    `user_id` BIGINT COMMENT 'user id',
    `sku_id` BIGINT COMMENT 'sku id',
    `k1` STRING COMMENT 'partition field',
    `sku_name` STRING COMMENT 'sku name',
    `category1_id` BIGINT COMMENT 'category1 id',
    `category1_name` STRING COMMENT 'category1 name',
    `category2_id` BIGINT COMMENT 'category2 id',
    `category2_name` STRING COMMENT 'category2 name',
    `category3_id` BIGINT COMMENT 'category3 id',
    `category3_name` STRING COMMENT 'category3 name',
    `tm_id` BIGINT COMMENT 'tm id',
    `tm_name` STRING COMMENT 'tm name',
    `order_count_7d` BIGINT COMMENT 'recent 7 day order count',
    `order_num_7d` BIGINT COMMENT 'recent 7 day sku num',
    `order_original_amount_7d` DECIMAL(16, 2) COMMENT 'recent 7 day original amount',
    `activity_reduce_amount_7d` DECIMAL(16, 2) COMMENT 'recent 7 day activity reduce amount',
    `coupon_reduce_amount_7d` DECIMAL(16, 2) COMMENT 'recent 7 day coupon reduce amount',
    `order_total_amount_7d` DECIMAL(16, 2) COMMENT 'recent 7 day order total amount',
    `order_count_30d` BIGINT COMMENT 'recent 30 day order count',
    `order_num_30d` BIGINT COMMENT 'recent 30 day sku num',
    `order_original_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day original amount',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day activity reduce amount',
    `coupon_reduce_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day coupon reduce amount',
    `order_total_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day order total amount',
    PRIMARY KEY (`user_id`, `sku_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

CREATE TEMPORARY VIEW tmp_dws_trade_user_sku_order_nd_current_date_param AS
    SELECT CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE) AS cur_date
;

CREATE TEMPORARY VIEW tmp_dws_trade_user_sku_order_nd_order_1d AS
    SELECT
        user_id,
        sku_id,
        CAST(k1 AS DATE) AS dt,
        order_count_1d,
        order_num_1d,
        order_original_amount_1d,
        activity_reduce_amount_1d,
        coupon_reduce_amount_1d,
        order_total_amount_1d
    FROM hudi_dws_stream.dws_trade_user_sku_order_1d_full /*+ OPTIONS('read.streaming.enabled' = 'true') */
;

CREATE TEMPORARY VIEW tmp_dws_trade_user_sku_order_nd_order_agg AS
    SELECT
        o.user_id,
        o.sku_id,
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
    FROM tmp_dws_trade_user_sku_order_nd_order_1d o
    CROSS JOIN tmp_dws_trade_user_sku_order_nd_current_date_param cp
    WHERE o.dt BETWEEN cp.cur_date - INTERVAL '29' DAY AND cp.cur_date
    GROUP BY o.user_id, o.sku_id
;

CREATE TEMPORARY VIEW tmp_dws_trade_user_sku_order_nd_sku_dim AS
    SELECT
        id AS sku_id,
        sku_name,
        category1_id,
        category1_name,
        category2_id,
        category2_name,
        category3_id,
        category3_name,
        tm_id,
        tm_name
    FROM hudi_dim.dim_sku_full /*+ OPTIONS('read.streaming.enabled' = 'true') */
    CROSS JOIN tmp_dws_trade_user_sku_order_nd_current_date_param cp
    WHERE CAST(k1 AS DATE) = cp.cur_date
;

INSERT INTO hudi_dws_stream.dws_trade_user_sku_order_nd_full(
    user_id,
    sku_id,
    k1,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
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
    oa.sku_id,
    CAST(cp.cur_date AS STRING) AS k1,
    sd.sku_name,
    sd.category1_id,
    sd.category1_name,
    sd.category2_id,
    sd.category2_name,
    sd.category3_id,
    sd.category3_name,
    sd.tm_id,
    sd.tm_name,
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
FROM tmp_dws_trade_user_sku_order_nd_order_agg oa
CROSS JOIN tmp_dws_trade_user_sku_order_nd_current_date_param cp
LEFT JOIN tmp_dws_trade_user_sku_order_nd_sku_dim sd
    ON oa.sku_id = sd.sku_id;

