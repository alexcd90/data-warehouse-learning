SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG iceberg_catalog;

CREATE DATABASE IF NOT EXISTS iceberg_dws;

CREATE TABLE IF NOT EXISTS iceberg_dws.dws_trade_province_order_nd_full(
    `province_id` BIGINT COMMENT 'province id',
    `k1` STRING COMMENT 'partition field',
    `province_name` STRING COMMENT 'province name',
    `area_code` STRING COMMENT 'area code',
    `iso_code` STRING COMMENT 'iso code',
    `iso_3166_2` STRING COMMENT 'iso 3166-2 code',
    `order_count_7d` BIGINT COMMENT 'recent 7 day order count',
    `order_original_amount_7d` DECIMAL(16, 2) COMMENT 'recent 7 day original amount',
    `activity_reduce_amount_7d` DECIMAL(16, 2) COMMENT 'recent 7 day activity reduce amount',
    `coupon_reduce_amount_7d` DECIMAL(16, 2) COMMENT 'recent 7 day coupon reduce amount',
    `order_total_amount_7d` DECIMAL(16, 2) COMMENT 'recent 7 day order total amount',
    `order_count_30d` BIGINT COMMENT 'recent 30 day order count',
    `order_original_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day original amount',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day activity reduce amount',
    `coupon_reduce_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day coupon reduce amount',
    `order_total_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day order total amount',
    PRIMARY KEY (`province_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);


CREATE TEMPORARY VIEW tmp_dws_trade_province_order_nd_current_date_param AS
    SELECT CAST('${pdate}' AS DATE) AS cur_date
;

CREATE TEMPORARY VIEW tmp_dws_trade_province_order_nd_province_1d AS
    SELECT
        province_id,
        province_name,
        area_code,
        iso_code,
        iso_3166_2,
        CAST(k1 AS DATE) AS dt,
        order_count_1d,
        order_original_amount_1d,
        activity_reduce_amount_1d,
        coupon_reduce_amount_1d,
        order_total_amount_1d
    FROM iceberg_dws.dws_trade_province_order_1d_full
;

CREATE TEMPORARY VIEW tmp_dws_trade_province_order_nd_province_agg AS
    SELECT
        p.province_id,
        MAX(p.province_name) AS province_name,
        MAX(p.area_code) AS area_code,
        MAX(p.iso_code) AS iso_code,
        MAX(p.iso_3166_2) AS iso_3166_2,
        SUM(CASE WHEN p.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN p.order_count_1d ELSE 0 END) AS order_count_7d,
        SUM(CASE WHEN p.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN p.order_original_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS order_original_amount_7d,
        SUM(CASE WHEN p.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN p.activity_reduce_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS activity_reduce_amount_7d,
        SUM(CASE WHEN p.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN p.coupon_reduce_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS coupon_reduce_amount_7d,
        SUM(CASE WHEN p.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN p.order_total_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS order_total_amount_7d,
        SUM(p.order_count_1d) AS order_count_30d,
        SUM(p.order_original_amount_1d) AS order_original_amount_30d,
        SUM(p.activity_reduce_amount_1d) AS activity_reduce_amount_30d,
        SUM(p.coupon_reduce_amount_1d) AS coupon_reduce_amount_30d,
        SUM(p.order_total_amount_1d) AS order_total_amount_30d
    FROM tmp_dws_trade_province_order_nd_province_1d p
    CROSS JOIN tmp_dws_trade_province_order_nd_current_date_param cp
    WHERE p.dt BETWEEN cp.cur_date - INTERVAL '29' DAY AND cp.cur_date
    GROUP BY p.province_id
;

INSERT INTO iceberg_dws.dws_trade_province_order_nd_full /*+ OPTIONS('upsert-enabled' = 'true') */(
    province_id,
    k1,
    province_name,
    area_code,
    iso_code,
    iso_3166_2,
    order_count_7d,
    order_original_amount_7d,
    activity_reduce_amount_7d,
    coupon_reduce_amount_7d,
    order_total_amount_7d,
    order_count_30d,
    order_original_amount_30d,
    activity_reduce_amount_30d,
    coupon_reduce_amount_30d,
    order_total_amount_30d
)
SELECT
    pa.province_id,
    CAST(cp.cur_date AS STRING) AS k1,
    pa.province_name,
    pa.area_code,
    pa.iso_code,
    pa.iso_3166_2,
    pa.order_count_7d,
    pa.order_original_amount_7d,
    pa.activity_reduce_amount_7d,
    pa.coupon_reduce_amount_7d,
    pa.order_total_amount_7d,
    pa.order_count_30d,
    pa.order_original_amount_30d,
    pa.activity_reduce_amount_30d,
    pa.coupon_reduce_amount_30d,
    pa.order_total_amount_30d
FROM tmp_dws_trade_province_order_nd_province_agg pa
CROSS JOIN tmp_dws_trade_province_order_nd_current_date_param cp;

