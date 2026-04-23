SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'iceberg_ads_stream_ads_trade_stats_full';

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG iceberg_catalog;
CREATE DATABASE IF NOT EXISTS iceberg_ads_stream;

CREATE TABLE IF NOT EXISTS iceberg_ads_stream.ads_trade_stats_full(
    `dt` STRING COMMENT 'stat date',
    `recent_days` BIGINT COMMENT 'recent day window',
    `order_total_amount` DECIMAL(16, 2) COMMENT 'order total amount',
    `order_count` BIGINT COMMENT 'order count',
    `order_user_count` BIGINT COMMENT 'order user count',
    `order_refund_count` BIGINT COMMENT 'refund count',
    `order_refund_user_count` BIGINT COMMENT 'refund user count',
    PRIMARY KEY (`dt`, `recent_days`) NOT ENFORCED
) PARTITIONED BY (`dt`) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

CREATE TEMPORARY VIEW tmp_ads_trade_stats_recent_days AS
    SELECT CAST(7 AS BIGINT) AS recent_days
    UNION ALL
    SELECT CAST(30 AS BIGINT) AS recent_days
;

CREATE TEMPORARY VIEW tmp_ads_trade_stats_order_1d_stats AS
    SELECT
        COALESCE(SUM(order_total_amount_1d), CAST(0 AS DECIMAL(16, 2))) AS order_total_amount,
        COALESCE(SUM(order_count_1d), 0) AS order_count,
        COUNT(*) AS order_user_count
    FROM iceberg_dws_stream.dws_trade_user_order_1d_full
    WHERE k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
;

CREATE TEMPORARY VIEW tmp_ads_trade_stats_order_nd_stats AS
    SELECT
        r.recent_days,
        COALESCE(SUM(CASE WHEN r.recent_days = 7 THEN o.order_total_amount_7d ELSE o.order_total_amount_30d END), CAST(0 AS DECIMAL(16, 2))) AS order_total_amount,
        COALESCE(SUM(CASE WHEN r.recent_days = 7 THEN o.order_count_7d ELSE o.order_count_30d END), 0) AS order_count,
        COALESCE(
            SUM(
                CASE
                    WHEN (CASE WHEN r.recent_days = 7 THEN o.order_count_7d ELSE o.order_count_30d END) > 0 THEN 1
                    ELSE 0
                END
            ),
            0
        ) AS order_user_count
    FROM tmp_ads_trade_stats_recent_days r
    LEFT JOIN iceberg_dws_stream.dws_trade_user_order_nd_full o
        ON o.k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
    GROUP BY r.recent_days
;

CREATE TEMPORARY VIEW tmp_ads_trade_stats_refund_1d_stats AS
    SELECT
        COALESCE(SUM(order_refund_count_1d), 0) AS order_refund_count,
        COUNT(*) AS order_refund_user_count
    FROM iceberg_dws_stream.dws_trade_user_order_refund_1d_full
    WHERE k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
;

CREATE TEMPORARY VIEW tmp_ads_trade_stats_refund_nd_stats AS
    SELECT
        r.recent_days,
        COALESCE(SUM(CASE WHEN r.recent_days = 7 THEN f.order_refund_count_7d ELSE f.order_refund_count_30d END), 0) AS order_refund_count,
        COALESCE(
            SUM(
                CASE
                    WHEN (CASE WHEN r.recent_days = 7 THEN f.order_refund_count_7d ELSE f.order_refund_count_30d END) > 0 THEN 1
                    ELSE 0
                END
            ),
            0
        ) AS order_refund_user_count
    FROM tmp_ads_trade_stats_recent_days r
    LEFT JOIN iceberg_dws_stream.dws_trade_user_order_refund_nd_full f
        ON f.k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
    GROUP BY r.recent_days
;

INSERT INTO iceberg_ads_stream.ads_trade_stats_full(
    dt,
    recent_days,
    order_total_amount,
    order_count,
    order_user_count,
    order_refund_count,
    order_refund_user_count
)
SELECT
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    CAST(1 AS BIGINT) AS recent_days,
    o.order_total_amount,
    o.order_count,
    o.order_user_count,
    r.order_refund_count,
    r.order_refund_user_count
FROM tmp_ads_trade_stats_order_1d_stats o
CROSS JOIN tmp_ads_trade_stats_refund_1d_stats r
UNION ALL
SELECT
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    o.recent_days,
    o.order_total_amount,
    o.order_count,
    o.order_user_count,
    COALESCE(r.order_refund_count, 0) AS order_refund_count,
    COALESCE(r.order_refund_user_count, 0) AS order_refund_user_count
FROM tmp_ads_trade_stats_order_nd_stats o
LEFT JOIN tmp_ads_trade_stats_refund_nd_stats r
    ON o.recent_days = r.recent_days;


