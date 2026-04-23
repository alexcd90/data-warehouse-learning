SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'iceberg_dws_stream_dws_trade_user_payment_nd_full';

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

CREATE TABLE IF NOT EXISTS iceberg_dws_stream.dws_trade_user_payment_nd_full(
    `user_id` BIGINT COMMENT 'user id',
    `k1` STRING COMMENT 'partition field',
    `payment_count_7d` BIGINT COMMENT 'recent 7 day payment count',
    `payment_num_7d` BIGINT COMMENT 'recent 7 day payment sku num',
    `payment_amount_7d` DECIMAL(16, 2) COMMENT 'recent 7 day payment amount',
    `payment_count_30d` BIGINT COMMENT 'recent 30 day payment count',
    `payment_num_30d` BIGINT COMMENT 'recent 30 day payment sku num',
    `payment_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day payment amount',
    PRIMARY KEY (`user_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

CREATE TEMPORARY VIEW tmp_dws_trade_user_payment_nd_current_date_param AS
    SELECT CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE) AS cur_date
;

CREATE TEMPORARY VIEW tmp_dws_trade_user_payment_nd_payment_1d AS
    SELECT
        user_id,
        CAST(k1 AS DATE) AS dt,
        payment_count_1d,
        payment_num_1d,
        payment_amount_1d
    FROM iceberg_dws_stream.dws_trade_user_payment_1d_full
;

CREATE TEMPORARY VIEW tmp_dws_trade_user_payment_nd_payment_agg AS
    SELECT
        p.user_id,
        SUM(CASE WHEN p.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN p.payment_count_1d ELSE 0 END) AS payment_count_7d,
        SUM(CASE WHEN p.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN p.payment_num_1d ELSE 0 END) AS payment_num_7d,
        SUM(CASE WHEN p.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN p.payment_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS payment_amount_7d,
        SUM(p.payment_count_1d) AS payment_count_30d,
        SUM(p.payment_num_1d) AS payment_num_30d,
        SUM(p.payment_amount_1d) AS payment_amount_30d
    FROM tmp_dws_trade_user_payment_nd_payment_1d p
    CROSS JOIN tmp_dws_trade_user_payment_nd_current_date_param cp
    WHERE p.dt BETWEEN cp.cur_date - INTERVAL '29' DAY AND cp.cur_date
    GROUP BY p.user_id
;

INSERT INTO iceberg_dws_stream.dws_trade_user_payment_nd_full(
    user_id,
    k1,
    payment_count_7d,
    payment_num_7d,
    payment_amount_7d,
    payment_count_30d,
    payment_num_30d,
    payment_amount_30d
)
SELECT
    pa.user_id,
    CAST(cp.cur_date AS STRING) AS k1,
    pa.payment_count_7d,
    pa.payment_num_7d,
    pa.payment_amount_7d,
    pa.payment_count_30d,
    pa.payment_num_30d,
    pa.payment_amount_30d
FROM tmp_dws_trade_user_payment_nd_payment_agg pa
CROSS JOIN tmp_dws_trade_user_payment_nd_current_date_param cp;


