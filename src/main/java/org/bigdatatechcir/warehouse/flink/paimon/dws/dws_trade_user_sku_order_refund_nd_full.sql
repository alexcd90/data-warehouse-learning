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

CREATE TABLE IF NOT EXISTS dws.dws_trade_user_sku_order_refund_nd_full(
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
    `order_refund_count_7d` BIGINT COMMENT 'recent 7 day refund count',
    `order_refund_num_7d` BIGINT COMMENT 'recent 7 day refund sku num',
    `order_refund_amount_7d` DECIMAL(16, 2) COMMENT 'recent 7 day refund amount',
    `order_refund_count_30d` BIGINT COMMENT 'recent 30 day refund count',
    `order_refund_num_30d` BIGINT COMMENT 'recent 30 day refund sku num',
    `order_refund_amount_30d` DECIMAL(16, 2) COMMENT 'recent 30 day refund amount',
    PRIMARY KEY (`user_id`, `sku_id`, `k1`) NOT ENFORCED
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

CREATE TEMPORARY VIEW tmp_dws_trade_user_sku_order_refund_nd_current_date_param AS
    SELECT CAST('${pdate}' AS DATE) AS cur_date
;

CREATE TEMPORARY VIEW tmp_dws_trade_user_sku_order_refund_nd_refund_1d AS
    SELECT
        user_id,
        sku_id,
        CAST(k1 AS DATE) AS dt,
        order_refund_count_1d,
        order_refund_num_1d,
        order_refund_amount_1d
    FROM dws.dws_trade_user_sku_order_refund_1d_full
;

CREATE TEMPORARY VIEW tmp_dws_trade_user_sku_order_refund_nd_refund_agg AS
    SELECT
        r.user_id,
        r.sku_id,
        SUM(CASE WHEN r.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN r.order_refund_count_1d ELSE 0 END) AS order_refund_count_7d,
        SUM(CASE WHEN r.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN r.order_refund_num_1d ELSE 0 END) AS order_refund_num_7d,
        SUM(CASE WHEN r.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN r.order_refund_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS order_refund_amount_7d,
        SUM(r.order_refund_count_1d) AS order_refund_count_30d,
        SUM(r.order_refund_num_1d) AS order_refund_num_30d,
        SUM(r.order_refund_amount_1d) AS order_refund_amount_30d
    FROM tmp_dws_trade_user_sku_order_refund_nd_refund_1d r
    CROSS JOIN tmp_dws_trade_user_sku_order_refund_nd_current_date_param cp
    WHERE r.dt BETWEEN cp.cur_date - INTERVAL '29' DAY AND cp.cur_date
    GROUP BY r.user_id, r.sku_id
;

CREATE TEMPORARY VIEW tmp_dws_trade_user_sku_order_refund_nd_sku_dim AS
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
    FROM dim.dim_sku_full
    CROSS JOIN tmp_dws_trade_user_sku_order_refund_nd_current_date_param cp
    WHERE CAST(k1 AS DATE) = cp.cur_date
;

INSERT INTO dws.dws_trade_user_sku_order_refund_nd_full(
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
    order_refund_count_7d,
    order_refund_num_7d,
    order_refund_amount_7d,
    order_refund_count_30d,
    order_refund_num_30d,
    order_refund_amount_30d
)
SELECT
    ra.user_id,
    ra.sku_id,
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
    ra.order_refund_count_7d,
    ra.order_refund_num_7d,
    ra.order_refund_amount_7d,
    ra.order_refund_count_30d,
    ra.order_refund_num_30d,
    ra.order_refund_amount_30d
FROM tmp_dws_trade_user_sku_order_refund_nd_refund_agg ra
CROSS JOIN tmp_dws_trade_user_sku_order_refund_nd_current_date_param cp
LEFT JOIN tmp_dws_trade_user_sku_order_refund_nd_sku_dim sd
    ON ra.sku_id = sd.sku_id;
