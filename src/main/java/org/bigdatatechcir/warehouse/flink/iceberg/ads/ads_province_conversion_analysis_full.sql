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

CREATE TABLE IF NOT EXISTS iceberg_ads.ads_province_conversion_analysis_full(
    `dt` STRING COMMENT 'stat date',
    `recent_days` BIGINT COMMENT 'recent days',
    `province_id` STRING COMMENT 'province id',
    `province_name` STRING COMMENT 'province name',
    `region_id` STRING COMMENT 'region id',
    `region_name` STRING COMMENT 'region name',
    `visitor_count` BIGINT COMMENT 'visitor count',
    `product_view_count` BIGINT COMMENT 'product view count',
    `cart_count` BIGINT COMMENT 'cart count',
    `order_count` BIGINT COMMENT 'order count',
    `payment_count` BIGINT COMMENT 'payment count',
    `view_to_cart_rate` DECIMAL(10, 2) COMMENT 'view to cart rate',
    `cart_to_order_rate` DECIMAL(10, 2) COMMENT 'cart to order rate',
    `order_to_payment_rate` DECIMAL(10, 2) COMMENT 'order to payment rate',
    `overall_conversion_rate` DECIMAL(10, 2) COMMENT 'overall conversion rate',
    `average_order_amount` DECIMAL(16, 2) COMMENT 'average order amount',
    `user_penetration_rate` DECIMAL(10, 2) COMMENT 'user penetration rate',
    `gmv_contribution_rate` DECIMAL(10, 2) COMMENT 'gmv contribution rate',
    `wow_change_rate` DECIMAL(10, 2) COMMENT 'week over week rate',
    `regional_rank` BIGINT COMMENT 'regional rank',
    PRIMARY KEY (`dt`, `recent_days`, `province_id`) NOT ENFORCED
) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

INSERT INTO iceberg_ads.ads_province_conversion_analysis_full /*+ OPTIONS('upsert-enabled' = 'true') */(
    dt,
    recent_days,
    province_id,
    province_name,
    region_id,
    region_name,
    visitor_count,
    product_view_count,
    cart_count,
    view_to_cart_rate,
    order_count,
    cart_to_order_rate,
    payment_count,
    order_to_payment_rate,
    overall_conversion_rate,
    average_order_amount,
    user_penetration_rate,
    gmv_contribution_rate,
    wow_change_rate,
    regional_rank
)
SELECT
    '${pdate}' AS dt,
    rd.recent_days,
    CAST(p.id AS STRING) AS province_id,
    p.province_name,
    CAST(p.region_id AS STRING) AS region_id,
    p.region_name,
    CAST(0 AS BIGINT) AS visitor_count,
    CAST(0 AS BIGINT) AS product_view_count,
    CAST(0 AS BIGINT) AS cart_count,
    CAST(0 AS DECIMAL(10, 2)) AS view_to_cart_rate,
    COALESCE(ord.user_count, 0) AS order_count,
    CAST(0 AS DECIMAL(10, 2)) AS cart_to_order_rate,
    COALESCE(pay.user_count, 0) AS payment_count,
    CAST(
        CASE
            WHEN COALESCE(ord.user_count, 0) = 0 THEN 0
            ELSE COALESCE(pay.user_count, 0) * 1.0 / COALESCE(ord.user_count, 0)
        END AS DECIMAL(10, 2)
    ) AS order_to_payment_rate,
    CAST(0 AS DECIMAL(10, 2)) AS overall_conversion_rate,
    CAST(
        CASE
            WHEN COALESCE(ord.user_count, 0) = 0 THEN 0
            ELSE COALESCE(ord.order_amount, 0) / COALESCE(ord.user_count, 0)
        END AS DECIMAL(16, 2)
    ) AS average_order_amount,
    CAST(
        CASE
            WHEN COALESCE(all_pay.user_count, 0) = 0 THEN 0
            ELSE COALESCE(pay.user_count, 0) * 1.0 / COALESCE(all_pay.user_count, 0)
        END AS DECIMAL(10, 2)
    ) AS user_penetration_rate,
    CAST(
        CASE
            WHEN COALESCE(all_ord.order_amount, 0) = 0 THEN 0
            ELSE COALESCE(ord.order_amount, 0) / COALESCE(all_ord.order_amount, 0)
        END AS DECIMAL(10, 2)
    ) AS gmv_contribution_rate,
    CAST(
        CASE
            WHEN COALESCE(last_week.order_amount, 0) = 0 THEN NULL
            ELSE (COALESCE(ord.order_amount, 0) - COALESCE(last_week.order_amount, 0)) / COALESCE(last_week.order_amount, 0)
        END AS DECIMAL(10, 2)
    ) AS wow_change_rate,
    ROW_NUMBER() OVER (PARTITION BY rd.recent_days ORDER BY COALESCE(ord.order_amount, 0) DESC) AS regional_rank
FROM iceberg_dim.dim_province_full p
CROSS JOIN (
    SELECT CAST(1 AS BIGINT) AS recent_days
    UNION ALL
    SELECT CAST(7 AS BIGINT)
    UNION ALL
    SELECT CAST(30 AS BIGINT)
) rd
LEFT JOIN (
    SELECT
        d.recent_days,
        od.province_id,
        COUNT(DISTINCT od.user_id) AS user_count,
        SUM(od.split_total_amount) AS order_amount
    FROM iceberg_dwd.dwd_trade_order_detail_full od
    CROSS JOIN (
        SELECT CAST(1 AS BIGINT) AS recent_days
        UNION ALL
        SELECT CAST(7 AS BIGINT)
        UNION ALL
        SELECT CAST(30 AS BIGINT)
    ) d
    WHERE CAST(od.k1 AS DATE) >= TIMESTAMPADD(DAY, -30, CAST('${pdate}' AS DATE))
      AND CAST(od.create_time AS DATE) >= TIMESTAMPADD(DAY, -CAST(d.recent_days - 1 AS INT), CAST('${pdate}' AS DATE))
      AND CAST(od.create_time AS DATE) <= CAST('${pdate}' AS DATE)
    GROUP BY d.recent_days, od.province_id
) ord
    ON p.id = ord.province_id
   AND rd.recent_days = ord.recent_days
LEFT JOIN (
    SELECT
        d.recent_days,
        pd.province_id,
        COUNT(DISTINCT pd.user_id) AS user_count
    FROM iceberg_dwd.dwd_trade_pay_detail_suc_full pd
    CROSS JOIN (
        SELECT CAST(1 AS BIGINT) AS recent_days
        UNION ALL
        SELECT CAST(7 AS BIGINT)
        UNION ALL
        SELECT CAST(30 AS BIGINT)
    ) d
    WHERE CAST(pd.k1 AS DATE) >= TIMESTAMPADD(DAY, -30, CAST('${pdate}' AS DATE))
      AND CAST(pd.callback_time AS DATE) >= TIMESTAMPADD(DAY, -CAST(d.recent_days - 1 AS INT), CAST('${pdate}' AS DATE))
      AND CAST(pd.callback_time AS DATE) <= CAST('${pdate}' AS DATE)
    GROUP BY d.recent_days, pd.province_id
) pay
    ON p.id = pay.province_id
   AND rd.recent_days = pay.recent_days
LEFT JOIN (
    SELECT
        d.recent_days,
        SUM(ao.split_total_amount) AS order_amount
    FROM iceberg_dwd.dwd_trade_order_detail_full ao
    CROSS JOIN (
        SELECT CAST(1 AS BIGINT) AS recent_days
        UNION ALL
        SELECT CAST(7 AS BIGINT)
        UNION ALL
        SELECT CAST(30 AS BIGINT)
    ) d
    WHERE CAST(ao.k1 AS DATE) >= TIMESTAMPADD(DAY, -30, CAST('${pdate}' AS DATE))
      AND CAST(ao.create_time AS DATE) >= TIMESTAMPADD(DAY, -CAST(d.recent_days - 1 AS INT), CAST('${pdate}' AS DATE))
      AND CAST(ao.create_time AS DATE) <= CAST('${pdate}' AS DATE)
    GROUP BY d.recent_days
) all_ord
    ON rd.recent_days = all_ord.recent_days
LEFT JOIN (
    SELECT
        d.recent_days,
        COUNT(DISTINCT ap.user_id) AS user_count
    FROM iceberg_dwd.dwd_trade_pay_detail_suc_full ap
    CROSS JOIN (
        SELECT CAST(1 AS BIGINT) AS recent_days
        UNION ALL
        SELECT CAST(7 AS BIGINT)
        UNION ALL
        SELECT CAST(30 AS BIGINT)
    ) d
    WHERE CAST(ap.k1 AS DATE) >= TIMESTAMPADD(DAY, -30, CAST('${pdate}' AS DATE))
      AND CAST(ap.callback_time AS DATE) >= TIMESTAMPADD(DAY, -CAST(d.recent_days - 1 AS INT), CAST('${pdate}' AS DATE))
      AND CAST(ap.callback_time AS DATE) <= CAST('${pdate}' AS DATE)
    GROUP BY d.recent_days
) all_pay
    ON rd.recent_days = all_pay.recent_days
LEFT JOIN (
    SELECT
        d.recent_days,
        lw.province_id,
        SUM(lw.split_total_amount) AS order_amount
    FROM iceberg_dwd.dwd_trade_order_detail_full lw
    CROSS JOIN (
        SELECT CAST(1 AS BIGINT) AS recent_days
        UNION ALL
        SELECT CAST(7 AS BIGINT)
        UNION ALL
        SELECT CAST(30 AS BIGINT)
    ) d
    WHERE CAST(lw.k1 AS DATE) >= TIMESTAMPADD(DAY, -30, CAST('${pdate}' AS DATE))
      AND CAST(lw.create_time AS DATE) >= TIMESTAMPADD(
            DAY,
            -CAST(d.recent_days - 1 AS INT),
            TIMESTAMPADD(DAY, -7, CAST('${pdate}' AS DATE))
          )
      AND CAST(lw.create_time AS DATE) <= TIMESTAMPADD(DAY, -7, CAST('${pdate}' AS DATE))
    GROUP BY d.recent_days, lw.province_id
) last_week
    ON p.id = last_week.province_id
   AND rd.recent_days = last_week.recent_days
WHERE COALESCE(ord.user_count, 0) > 0
   OR COALESCE(pay.user_count, 0) > 0;
