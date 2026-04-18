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
CREATE DATABASE IF NOT EXISTS iceberg_ads;

CREATE TABLE IF NOT EXISTS iceberg_ads.ads_user_action_full(
    `dt` STRING COMMENT 'stat date',
    `recent_days` BIGINT COMMENT 'recent day window',
    `home_count` BIGINT COMMENT 'home page user count',
    `good_detail_count` BIGINT COMMENT 'good detail page user count',
    `cart_count` BIGINT COMMENT 'cart add user count',
    `order_count` BIGINT COMMENT 'order user count',
    `payment_count` BIGINT COMMENT 'payment user count',
    PRIMARY KEY (`dt`, `recent_days`) NOT ENFORCED
) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

CREATE TEMPORARY VIEW tmp_ads_user_action_recent_days AS
SELECT CAST(1 AS BIGINT) AS recent_days
UNION ALL
SELECT CAST(7 AS BIGINT) AS recent_days
UNION ALL
SELECT CAST(30 AS BIGINT) AS recent_days
;

CREATE TEMPORARY VIEW tmp_ads_user_action_page AS
SELECT
    CAST(1 AS BIGINT) AS recent_days,
    SUM(CASE WHEN page_id = 'home' THEN 1 ELSE 0 END) AS home_count,
    SUM(CASE WHEN page_id = 'good_detail' THEN 1 ELSE 0 END) AS good_detail_count
FROM iceberg_dws.dws_traffic_page_visitor_page_view_1d_full
WHERE k1 = '${pdate}'
  AND page_id IN ('home', 'good_detail')
UNION ALL
SELECT
    p.recent_days,
    SUM(
        CASE
            WHEN pv.page_id = 'home'
             AND (CASE WHEN p.recent_days = 7 THEN pv.view_count_7d ELSE pv.view_count_30d END) > 0
            THEN 1
            ELSE 0
        END
    ) AS home_count,
    SUM(
        CASE
            WHEN pv.page_id = 'good_detail'
             AND (CASE WHEN p.recent_days = 7 THEN pv.view_count_7d ELSE pv.view_count_30d END) > 0
            THEN 1
            ELSE 0
        END
    ) AS good_detail_count
FROM tmp_ads_user_action_recent_days p
JOIN iceberg_dws.dws_traffic_page_visitor_page_view_nd_full pv
    ON p.recent_days IN (7, 30)
   AND pv.k1 = '${pdate}'
WHERE pv.page_id IN ('home', 'good_detail')
GROUP BY p.recent_days
;

CREATE TEMPORARY VIEW tmp_ads_user_action_cart AS
SELECT
    CAST(1 AS BIGINT) AS recent_days,
    COUNT(*) AS cart_count
FROM iceberg_dws.dws_trade_user_cart_add_1d_full
WHERE k1 = '${pdate}'
UNION ALL
SELECT
    p.recent_days,
    SUM(
        CASE
            WHEN (CASE WHEN p.recent_days = 7 THEN c.cart_add_count_7d ELSE c.cart_add_count_30d END) > 0
            THEN 1
            ELSE 0
        END
    ) AS cart_count
FROM tmp_ads_user_action_recent_days p
JOIN iceberg_dws.dws_trade_user_cart_add_nd_full c
    ON p.recent_days IN (7, 30)
   AND c.k1 = '${pdate}'
GROUP BY p.recent_days
;

CREATE TEMPORARY VIEW tmp_ads_user_action_order AS
SELECT
    CAST(1 AS BIGINT) AS recent_days,
    COUNT(*) AS order_count
FROM iceberg_dws.dws_trade_user_order_1d_full
WHERE k1 = '${pdate}'
UNION ALL
SELECT
    p.recent_days,
    SUM(
        CASE
            WHEN (CASE WHEN p.recent_days = 7 THEN o.order_count_7d ELSE o.order_count_30d END) > 0
            THEN 1
            ELSE 0
        END
    ) AS order_count
FROM tmp_ads_user_action_recent_days p
JOIN iceberg_dws.dws_trade_user_order_nd_full o
    ON p.recent_days IN (7, 30)
   AND o.k1 = '${pdate}'
GROUP BY p.recent_days
;

CREATE TEMPORARY VIEW tmp_ads_user_action_payment AS
SELECT
    CAST(1 AS BIGINT) AS recent_days,
    COUNT(*) AS payment_count
FROM iceberg_dws.dws_trade_user_payment_1d_full
WHERE k1 = '${pdate}'
UNION ALL
SELECT
    p.recent_days,
    SUM(
        CASE
            WHEN (CASE WHEN p.recent_days = 7 THEN pay.payment_count_7d ELSE pay.payment_count_30d END) > 0
            THEN 1
            ELSE 0
        END
    ) AS payment_count
FROM tmp_ads_user_action_recent_days p
JOIN iceberg_dws.dws_trade_user_payment_nd_full pay
    ON p.recent_days IN (7, 30)
   AND pay.k1 = '${pdate}'
GROUP BY p.recent_days
;

INSERT INTO iceberg_ads.ads_user_action_full /*+ OPTIONS('upsert-enabled' = 'true') */(
    dt,
    recent_days,
    home_count,
    good_detail_count,
    cart_count,
    order_count,
    payment_count
)
SELECT
    '${pdate}' AS dt,
    d.recent_days,
    COALESCE(p.home_count, 0) AS home_count,
    COALESCE(p.good_detail_count, 0) AS good_detail_count,
    COALESCE(c.cart_count, 0) AS cart_count,
    COALESCE(o.order_count, 0) AS order_count,
    COALESCE(pay.payment_count, 0) AS payment_count
FROM tmp_ads_user_action_recent_days d
LEFT JOIN tmp_ads_user_action_page p
    ON d.recent_days = p.recent_days
LEFT JOIN tmp_ads_user_action_cart c
    ON d.recent_days = c.recent_days
LEFT JOIN tmp_ads_user_action_order o
    ON d.recent_days = o.recent_days
LEFT JOIN tmp_ads_user_action_payment pay
    ON d.recent_days = pay.recent_days
;
