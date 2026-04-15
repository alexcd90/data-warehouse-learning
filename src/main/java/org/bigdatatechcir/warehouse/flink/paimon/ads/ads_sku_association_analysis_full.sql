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
CREATE DATABASE IF NOT EXISTS ads;

CREATE TABLE IF NOT EXISTS ads.ads_sku_association_analysis_full(
    `dt` STRING COMMENT '统计日期',
    `recent_days` BIGINT COMMENT '最近天数,7:最近7天,30:最近30天,90:最近90天',
    `source_sku_id` STRING COMMENT '来源商品ID',
    `source_sku_name` STRING COMMENT '来源商品名称',
    `source_category1_id` STRING COMMENT '来源商品一级类目ID',
    `source_category1_name` STRING COMMENT '来源商品一级类目名称',
    `target_sku_id` STRING COMMENT '目标商品ID',
    `target_sku_name` STRING COMMENT '目标商品名称',
    `target_category1_id` STRING COMMENT '目标商品一级类目ID',
    `target_category1_name` STRING COMMENT '目标商品一级类目名称',
    `co_purchase_count` BIGINT COMMENT '共同购买次数',
    `co_purchase_user_count` BIGINT COMMENT '共同购买用户数',
    `support` DECIMAL(10, 4) COMMENT '支持度 - 共同购买次数/总订单数',
    `confidence` DECIMAL(10, 4) COMMENT '置信度 - 购买来源商品后购买目标商品的概率',
    `lift` DECIMAL(10, 4) COMMENT '提升度 - 规则的有效性度量',
    `sequence_pattern` STRING COMMENT '购买序列模式(同时/先后)',
    `time_interval_avg` DECIMAL(16, 2) COMMENT '平均购买时间间隔(小时)',
    `association_strength` STRING COMMENT '关联强度(强/中/弱)',
    `recommendation_score` DECIMAL(10, 2) COMMENT '推荐分数',
    PRIMARY KEY (`dt`, `recent_days`, `source_sku_id`, `target_sku_id`) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'file.format' = 'parquet',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true'
);

INSERT INTO ads.ads_sku_association_analysis_full (dt, recent_days, source_sku_id, source_sku_name, source_category1_id, source_category1_name,
 target_sku_id, target_sku_name, target_category1_id, target_category1_name,
 co_purchase_count, co_purchase_user_count, support, confidence, lift, recommendation_score)
-- 商品关联分析ETL脚本
WITH base_orders AS (
    SELECT
        k1,
        recent_days,
        user_id,
        order_id,
        sku_id
    FROM dwd.dwd_trade_order_detail_full
             CROSS JOIN (SELECT 7 as recent_days UNION ALL SELECT 30 UNION ALL SELECT 90) d
    WHERE k1 >= date_sub(CAST('${pdate}' AS DATE), 90)
      AND date_format(create_time, 'yyyy-MM-dd') >= date_sub(CAST('${pdate}' AS DATE), d.recent_days - 1)
      AND date_format(create_time, 'yyyy-MM-dd') <= CAST('${pdate}' AS DATE)
),
     sku_pairs AS (
         SELECT
             a.k1,
             a.recent_days,
             a.user_id,
             a.order_id,
             a.sku_id AS source_sku_id,
             b.sku_id AS target_sku_id
         FROM base_orders a
                  JOIN base_orders b
                       ON a.order_id = b.order_id
                           AND a.recent_days = b.recent_days
                           AND a.k1 = b.k1
                           AND a.sku_id < b.sku_id
     ),
     total_orders AS (
         SELECT
             k1,
             recent_days,
             COUNT(DISTINCT order_id) AS order_count
         FROM base_orders
         GROUP BY k1, recent_days
     ),
     sku_purchases AS (
         SELECT
             k1,
             recent_days,
             sku_id,
             COUNT(DISTINCT order_id) AS purchase_count
         FROM base_orders
         GROUP BY k1, recent_days, sku_id
     )


SELECT
    CAST('${pdate}' AS DATE) AS dt,
    pair.recent_days,
    source.id AS source_sku_id,
    source.sku_name AS source_sku_name,
    source.category1_id AS source_category1_id,
    source.category1_name AS source_category1_name,
    target.id AS target_sku_id,
    target.sku_name AS target_sku_name,
    target.category1_id AS target_category1_id,
    target.category1_name AS target_category1_name,
    COUNT(*) AS co_purchase_count,
    COUNT(DISTINCT pair.user_id) AS co_purchase_user_count,
    CAST(COUNT(*) / total_orders.order_count AS DECIMAL(10, 4)) AS support,
    CAST(COUNT(*) / source_purchases.purchase_count AS DECIMAL(10, 4)) AS confidence,
    CAST((COUNT(*) / source_purchases.purchase_count) /
         (target_purchases.purchase_count / total_orders.order_count) AS DECIMAL(10, 4)) AS lift,
    CAST(
                    (COUNT(*) / total_orders.order_count) * 0.2 +
                    (COUNT(*) / source_purchases.purchase_count) * 0.5 +
                    ((COUNT(*) / source_purchases.purchase_count) /
                     (target_purchases.purchase_count / total_orders.order_count)) * 0.3
        AS DECIMAL(10, 4)) AS recommendation_score
FROM sku_pairs pair
         JOIN dim.dim_sku_full source ON pair.source_sku_id = source.id
         JOIN dim.dim_sku_full target ON pair.target_sku_id = target.id
         JOIN total_orders ON pair.k1 = total_orders.k1 AND pair.recent_days = total_orders.recent_days
         JOIN sku_purchases source_purchases ON pair.k1 = source_purchases.k1
    AND pair.recent_days = source_purchases.recent_days
    AND pair.source_sku_id = source_purchases.sku_id
         JOIN sku_purchases target_purchases ON pair.k1 = target_purchases.k1
    AND pair.recent_days = target_purchases.recent_days
    AND pair.target_sku_id = target_purchases.sku_id
GROUP BY
    pair.recent_days,
    source.id, source.sku_name, source.category1_id, source.category1_name,
    target.id, target.sku_name, target.category1_id, target.category1_name,
    total_orders.order_count, source_purchases.purchase_count, target_purchases.purchase_count
HAVING support >= 0.001
ORDER BY pair.recent_days, recommendation_score DESC;

