SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'iceberg_ads_stream_ads_repeat_purchase_by_tm_full';

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

CREATE TABLE IF NOT EXISTS iceberg_ads_stream.ads_repeat_purchase_by_tm_full(
    `dt` STRING COMMENT 'stat date',
    `recent_days` BIGINT COMMENT 'recent day window',
    `tm_id` STRING COMMENT 'trademark id',
    `tm_name` STRING COMMENT 'trademark name',
    `order_repeat_rate` DECIMAL(16, 2) COMMENT 'repeat purchase rate',
    PRIMARY KEY (`dt`, `recent_days`, `tm_id`) NOT ENFORCED
) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

CREATE TEMPORARY VIEW tmp_ads_repeat_purchase_by_tm_recent_days AS
SELECT CAST(7 AS BIGINT) AS recent_days
UNION ALL
SELECT CAST(30 AS BIGINT) AS recent_days
;

CREATE TEMPORARY VIEW tmp_ads_repeat_purchase_by_tm_user_tm_order AS
SELECT
    p.recent_days,
    CAST(o.tm_id AS STRING) AS tm_id,
    COALESCE(o.tm_name, '') AS tm_name,
    CAST(o.user_id AS STRING) AS user_id,
    SUM(
        CASE
            WHEN p.recent_days = 7 THEN o.order_count_7d
            ELSE o.order_count_30d
        END
    ) AS order_count
FROM tmp_ads_repeat_purchase_by_tm_recent_days p
JOIN iceberg_dws_stream.dws_trade_user_sku_order_nd_full o
    ON o.k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
WHERE o.tm_id IS NOT NULL
GROUP BY
    p.recent_days,
    CAST(o.tm_id AS STRING),
    COALESCE(o.tm_name, ''),
    CAST(o.user_id AS STRING)
;

INSERT INTO iceberg_ads_stream.ads_repeat_purchase_by_tm_full(
    dt,
    recent_days,
    tm_id,
    tm_name,
    order_repeat_rate
)
SELECT
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    recent_days,
    tm_id,
    tm_name,
    CASE
        WHEN SUM(CASE WHEN order_count >= 1 THEN 1 ELSE 0 END) = 0 THEN CAST(0 AS DECIMAL(16, 2))
        ELSE CAST(
            CAST(SUM(CASE WHEN order_count >= 2 THEN 1 ELSE 0 END) AS DECIMAL(16, 2))
            / CAST(SUM(CASE WHEN order_count >= 1 THEN 1 ELSE 0 END) AS DECIMAL(16, 2))
            AS DECIMAL(16, 2)
        )
    END AS order_repeat_rate
FROM tmp_ads_repeat_purchase_by_tm_user_tm_order
GROUP BY recent_days, tm_id, tm_name
;


