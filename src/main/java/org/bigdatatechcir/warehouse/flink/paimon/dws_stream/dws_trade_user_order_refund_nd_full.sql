SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'paimon_dws_stream_dws_trade_user_order_refund_nd_full';

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

CREATE TABLE IF NOT EXISTS dws_stream.dws_trade_user_order_refund_nd_full(
    `user_id` BIGINT COMMENT 'user id',
    `k1` STRING COMMENT 'partition field',
    `order_refund_count_7d` BIGINT COMMENT 'recent 7 day refund count',
    `order_refund_num_7d` BIGINT COMMENT 'recent 7 day refund sku count',
    `order_refund_amount_7d` DECIMAL(16, 2) COMMENT 'recent 7 day refund amount',
    `order_refund_count_30d` BIGINT COMMENT 'recent 30 day refund count',
    `order_refund_num_30d` BIGINT COMMENT 'recent 30 day refund sku count',
    `order_refund_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day refund amount',
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

CREATE TEMPORARY VIEW tmp_dws_trade_user_order_refund_nd_current_date_param AS
    SELECT CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE) AS cur_date
;

CREATE TEMPORARY VIEW tmp_dws_trade_user_order_refund_nd_refund_1d AS
    SELECT
        user_id,
        CAST(k1 AS DATE) AS dt,
        order_refund_count_1d,
        order_refund_num_1d,
        order_refund_amount_1d
    FROM dws_stream.dws_trade_user_order_refund_1d_full
;

CREATE TEMPORARY VIEW tmp_dws_trade_user_order_refund_nd_refund_agg AS
    SELECT
        r.user_id,
        SUM(CASE WHEN r.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN r.order_refund_count_1d ELSE 0 END) AS order_refund_count_7d,
        SUM(CASE WHEN r.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN r.order_refund_num_1d ELSE 0 END) AS order_refund_num_7d,
        SUM(CASE WHEN r.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN r.order_refund_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS order_refund_amount_7d,
        SUM(r.order_refund_count_1d) AS order_refund_count_30d,
        SUM(r.order_refund_num_1d) AS order_refund_num_30d,
        SUM(r.order_refund_amount_1d) AS order_refund_amount_30d
    FROM tmp_dws_trade_user_order_refund_nd_refund_1d r
    CROSS JOIN tmp_dws_trade_user_order_refund_nd_current_date_param cp
    WHERE r.dt BETWEEN cp.cur_date - INTERVAL '29' DAY AND cp.cur_date
    GROUP BY r.user_id
;

INSERT INTO dws_stream.dws_trade_user_order_refund_nd_full(
    user_id,
    k1,
    order_refund_count_7d,
    order_refund_num_7d,
    order_refund_amount_7d,
    order_refund_count_30d,
    order_refund_num_30d,
    order_refund_amount_30d
)
SELECT
    ra.user_id,
    CAST(cp.cur_date AS STRING) AS k1,
    ra.order_refund_count_7d,
    ra.order_refund_num_7d,
    ra.order_refund_amount_7d,
    ra.order_refund_count_30d,
    ra.order_refund_num_30d,
    ra.order_refund_amount_30d
FROM tmp_dws_trade_user_order_refund_nd_refund_agg ra
CROSS JOIN tmp_dws_trade_user_order_refund_nd_current_date_param cp;


