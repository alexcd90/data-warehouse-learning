SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'iceberg_dws_stream_dws_trade_tm_category1_cat_nd_full';

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

CREATE TABLE IF NOT EXISTS iceberg_dws_stream.dws_trade_tm_category1_cat_nd_full(
    `tm_id` STRING COMMENT '品牌ID - 标识品牌',
    `category1_id` STRING COMMENT '一级品类ID - 标识商品大类',
    `k1` STRING COMMENT '数据日期 - 分区字段，记录数据所属日期',
    `tm_name` STRING COMMENT '品牌名称 - 展示品牌信息',
    `category1_name` STRING COMMENT '一级品类名称 - 展示品类信息',
    `order_count_1d` BIGINT COMMENT '最近1日下单次数 - 订单总数',
    `order_num_1d` BIGINT COMMENT '最近1日下单件数 - 商品总数量',
    `order_user_count_1d` BIGINT COMMENT '最近1日下单用户数 - 下单用户去重数',
    `order_sku_count_1d` BIGINT COMMENT '最近1日下单商品种类数 - 购买的SKU种类数',
    `order_original_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单原始金额 - 未优惠的原始总金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日活动优惠金额 - 活动带来的优惠总金额',
    `coupon_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日优惠券优惠金额 - 优惠券带来的优惠总金额',
    `order_total_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单最终金额 - 优惠后的实际支付总金额',
    `order_count_7d` BIGINT COMMENT '最近7日下单次数 - 7天内订单总数',
    `order_num_7d` BIGINT COMMENT '最近7日下单件数 - 7天内商品总数量',
    `order_user_count_7d` BIGINT COMMENT '最近7日下单用户数 - 7天内下单用户去重数',
    `order_sku_count_7d` BIGINT COMMENT '最近7日下单商品种类数 - 7天内购买的SKU种类数',
    `order_original_amount_7d` DECIMAL(16, 2) COMMENT '最近7日下单原始金额 - 7天内未优惠的原始总金额',
    `activity_reduce_amount_7d` DECIMAL(16, 2) COMMENT '最近7日活动优惠金额 - 7天内活动带来的优惠总金额',
    `coupon_reduce_amount_7d` DECIMAL(16, 2) COMMENT '最近7日优惠券优惠金额 - 7天内优惠券带来的优惠总金额',
    `order_total_amount_7d` DECIMAL(16, 2) COMMENT '最近7日下单最终金额 - 7天内优惠后的实际支付总金额',
    `order_count_30d` BIGINT COMMENT '最近30日下单次数 - 30天内订单总数',
    `order_num_30d` BIGINT COMMENT '最近30日下单件数 - 30天内商品总数量',
    `order_user_count_30d` BIGINT COMMENT '最近30日下单用户数 - 30天内下单用户去重数',
    `order_sku_count_30d` BIGINT COMMENT '最近30日下单商品种类数 - 30天内购买的SKU种类数',
    `order_original_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单原始金额 - 30天内未优惠的原始总金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日活动优惠金额 - 30天内活动带来的优惠总金额',
    `coupon_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额 - 30天内优惠券带来的优惠总金额',
    `order_total_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单最终金额 - 30天内优惠后的实际支付总金额',
    `order_count_1d_wow_rate` DECIMAL(10, 2) COMMENT '最近1日下单次数周环比 - 与上周同期相比的增长率(%)',
    `order_total_amount_1d_wow_rate` DECIMAL(10, 2) COMMENT '最近1日下单金额周环比 - 与上周同期相比的增长率(%)',
    `order_count_7d_wow_rate` DECIMAL(10, 2) COMMENT '最近7日下单次数周环比 - 与前7天相比的增长率(%)',
    `order_total_amount_7d_wow_rate` DECIMAL(10, 2) COMMENT '最近7日下单金额周环比 - 与前7天相比的增长率(%)',
    `order_count_1d_yoy_rate` DECIMAL(10, 2) COMMENT '最近1日下单次数同比 - 与去年同期相比的增长率(%)',
    `order_total_amount_1d_yoy_rate` DECIMAL(10, 2) COMMENT '最近1日下单金额同比 - 与去年同期相比的增长率(%)',
    `category_amount_ratio_1d` DECIMAL(10, 2) COMMENT '品类金额占比 - 该品牌在此品类的销售额占品类总销售额的百分比(%)',
    `tm_amount_ratio_1d` DECIMAL(10, 2) COMMENT '品牌金额占比 - 此品类的销售额占该品牌总销售额的百分比(%)',
    `sku_coverage_rate_1d` DECIMAL(10, 2) COMMENT '品牌SKU覆盖率 - 该品牌在此品类销售的SKU数占品类SKU总数的百分比(%)',
    `cat_1d_type` STRING COMMENT '最近1日增长类型 - 高速增长/平稳增长/下降/急剧下降',
    `cat_7d_type` STRING COMMENT '最近7日增长类型 - 高速增长/平稳增长/下降/急剧下降',
    PRIMARY KEY (`tm_id`, `category1_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

CREATE TEMPORARY VIEW tmp_dws_trade_tm_category1_cat_nd_full_src AS
WITH date_param AS (
    SELECT CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE) AS cur_date
),
max_sku_date AS (
    SELECT MAX(k1) AS max_k1
    FROM iceberg_dim_stream.dim_sku_full
),
latest_sku AS (
    SELECT s.*
    FROM iceberg_dim_stream.dim_sku_full s
    CROSS JOIN max_sku_date m
    WHERE s.k1 = m.max_k1
),
detail_by_day_sku AS (
    SELECT
        COALESCE(CAST(s.tm_id AS STRING), '') AS tm_id,
        COALESCE(CAST(s.category1_id AS STRING), '') AS category1_id,
        CAST(od.k1 AS DATE) AS dt,
        CAST(od.sku_id AS STRING) AS sku_id,
        COALESCE(s.tm_name, '') AS tm_name,
        COALESCE(s.category1_name, '') AS category1_name,
        COUNT(DISTINCT od.order_id) AS order_count,
        SUM(od.sku_num) AS order_num,
        COUNT(DISTINCT od.user_id) AS order_user_count,
        SUM(od.split_original_amount) AS order_original_amount,
        SUM(COALESCE(od.split_activity_amount, CAST(0 AS DECIMAL(16, 2)))) AS activity_reduce_amount,
        SUM(COALESCE(od.split_coupon_amount, CAST(0 AS DECIMAL(16, 2)))) AS coupon_reduce_amount,
        SUM(od.split_total_amount) AS order_total_amount
    FROM iceberg_dwd_stream.dwd_trade_order_detail_full od
    LEFT JOIN latest_sku s
        ON od.sku_id = s.id
    GROUP BY
        COALESCE(CAST(s.tm_id AS STRING), ''),
        COALESCE(CAST(s.category1_id AS STRING), ''),
        CAST(od.k1 AS DATE),
        CAST(od.sku_id AS STRING),
        COALESCE(s.tm_name, ''),
        COALESCE(s.category1_name, '')
),
current_data AS (
    SELECT
        d.tm_id,
        d.category1_id,
        p.cur_date AS cur_date,
        MAX(d.tm_name) AS tm_name,
        MAX(d.category1_name) AS category1_name,
        SUM(CASE WHEN d.dt = p.cur_date THEN d.order_count ELSE 0 END) AS order_count_1d,
        SUM(CASE WHEN d.dt = p.cur_date THEN d.order_num ELSE 0 END) AS order_num_1d,
        SUM(CASE WHEN d.dt = p.cur_date THEN d.order_user_count ELSE 0 END) AS order_user_count_1d,
        COUNT(DISTINCT CASE WHEN d.dt = p.cur_date THEN d.sku_id ELSE NULL END) AS order_sku_count_1d,
        SUM(CASE WHEN d.dt = p.cur_date THEN d.order_original_amount ELSE CAST(0 AS DECIMAL(16, 2)) END) AS order_original_amount_1d,
        SUM(CASE WHEN d.dt = p.cur_date THEN d.activity_reduce_amount ELSE CAST(0 AS DECIMAL(16, 2)) END) AS activity_reduce_amount_1d,
        SUM(CASE WHEN d.dt = p.cur_date THEN d.coupon_reduce_amount ELSE CAST(0 AS DECIMAL(16, 2)) END) AS coupon_reduce_amount_1d,
        SUM(CASE WHEN d.dt = p.cur_date THEN d.order_total_amount ELSE CAST(0 AS DECIMAL(16, 2)) END) AS order_total_amount_1d,
        SUM(CASE WHEN d.dt BETWEEN p.cur_date - INTERVAL '6' DAY AND p.cur_date THEN d.order_count ELSE 0 END) AS order_count_7d,
        SUM(CASE WHEN d.dt BETWEEN p.cur_date - INTERVAL '6' DAY AND p.cur_date THEN d.order_num ELSE 0 END) AS order_num_7d,
        SUM(CASE WHEN d.dt BETWEEN p.cur_date - INTERVAL '6' DAY AND p.cur_date THEN d.order_user_count ELSE 0 END) AS order_user_count_7d,
        COUNT(DISTINCT CASE WHEN d.dt BETWEEN p.cur_date - INTERVAL '6' DAY AND p.cur_date THEN d.sku_id ELSE NULL END) AS order_sku_count_7d,
        SUM(CASE WHEN d.dt BETWEEN p.cur_date - INTERVAL '6' DAY AND p.cur_date THEN d.order_original_amount ELSE CAST(0 AS DECIMAL(16, 2)) END) AS order_original_amount_7d,
        SUM(CASE WHEN d.dt BETWEEN p.cur_date - INTERVAL '6' DAY AND p.cur_date THEN d.activity_reduce_amount ELSE CAST(0 AS DECIMAL(16, 2)) END) AS activity_reduce_amount_7d,
        SUM(CASE WHEN d.dt BETWEEN p.cur_date - INTERVAL '6' DAY AND p.cur_date THEN d.coupon_reduce_amount ELSE CAST(0 AS DECIMAL(16, 2)) END) AS coupon_reduce_amount_7d,
        SUM(CASE WHEN d.dt BETWEEN p.cur_date - INTERVAL '6' DAY AND p.cur_date THEN d.order_total_amount ELSE CAST(0 AS DECIMAL(16, 2)) END) AS order_total_amount_7d,
        SUM(CASE WHEN d.dt BETWEEN p.cur_date - INTERVAL '29' DAY AND p.cur_date THEN d.order_count ELSE 0 END) AS order_count_30d,
        SUM(CASE WHEN d.dt BETWEEN p.cur_date - INTERVAL '29' DAY AND p.cur_date THEN d.order_num ELSE 0 END) AS order_num_30d,
        SUM(CASE WHEN d.dt BETWEEN p.cur_date - INTERVAL '29' DAY AND p.cur_date THEN d.order_user_count ELSE 0 END) AS order_user_count_30d,
        COUNT(DISTINCT CASE WHEN d.dt BETWEEN p.cur_date - INTERVAL '29' DAY AND p.cur_date THEN d.sku_id ELSE NULL END) AS order_sku_count_30d,
        SUM(CASE WHEN d.dt BETWEEN p.cur_date - INTERVAL '29' DAY AND p.cur_date THEN d.order_original_amount ELSE CAST(0 AS DECIMAL(16, 2)) END) AS order_original_amount_30d,
        SUM(CASE WHEN d.dt BETWEEN p.cur_date - INTERVAL '29' DAY AND p.cur_date THEN d.activity_reduce_amount ELSE CAST(0 AS DECIMAL(16, 2)) END) AS activity_reduce_amount_30d,
        SUM(CASE WHEN d.dt BETWEEN p.cur_date - INTERVAL '29' DAY AND p.cur_date THEN d.coupon_reduce_amount ELSE CAST(0 AS DECIMAL(16, 2)) END) AS coupon_reduce_amount_30d,
        SUM(CASE WHEN d.dt BETWEEN p.cur_date - INTERVAL '29' DAY AND p.cur_date THEN d.order_total_amount ELSE CAST(0 AS DECIMAL(16, 2)) END) AS order_total_amount_30d
    FROM detail_by_day_sku d
    CROSS JOIN date_param p
    WHERE d.dt BETWEEN p.cur_date - INTERVAL '29' DAY AND p.cur_date
    GROUP BY d.tm_id, d.category1_id, p.cur_date
),
last_week_data AS (
    SELECT
        d.tm_id,
        d.category1_id,
        SUM(CASE WHEN d.dt = p.cur_date - INTERVAL '7' DAY THEN d.order_count ELSE 0 END) AS last_week_order_count_1d,
        SUM(CASE WHEN d.dt = p.cur_date - INTERVAL '7' DAY THEN d.order_total_amount ELSE CAST(0 AS DECIMAL(16, 2)) END) AS last_week_order_total_amount_1d,
        SUM(CASE WHEN d.dt BETWEEN p.cur_date - INTERVAL '13' DAY AND p.cur_date - INTERVAL '7' DAY THEN d.order_count ELSE 0 END) AS last_week_order_count_7d,
        SUM(CASE WHEN d.dt BETWEEN p.cur_date - INTERVAL '13' DAY AND p.cur_date - INTERVAL '7' DAY THEN d.order_total_amount ELSE CAST(0 AS DECIMAL(16, 2)) END) AS last_week_order_total_amount_7d
    FROM detail_by_day_sku d
    CROSS JOIN date_param p
    WHERE d.dt BETWEEN p.cur_date - INTERVAL '13' DAY AND p.cur_date - INTERVAL '7' DAY
    GROUP BY d.tm_id, d.category1_id
),
last_year_data AS (
    SELECT
        d.tm_id,
        d.category1_id,
        SUM(d.order_count) AS last_year_order_count_1d,
        SUM(d.order_total_amount) AS last_year_order_total_amount_1d
    FROM detail_by_day_sku d
    CROSS JOIN date_param p
    WHERE d.dt = p.cur_date - INTERVAL '1' YEAR
    GROUP BY d.tm_id, d.category1_id
),
tm_total AS (
    SELECT
        d.tm_id,
        SUM(d.order_total_amount) AS tm_total_amount_1d
    FROM detail_by_day_sku d
    CROSS JOIN date_param p
    WHERE d.dt = p.cur_date
    GROUP BY d.tm_id
),
category_total AS (
    SELECT
        d.category1_id,
        SUM(d.order_total_amount) AS category_total_amount_1d,
        COUNT(DISTINCT d.sku_id) AS category_sku_count_1d
    FROM detail_by_day_sku d
    CROSS JOIN date_param p
    WHERE d.dt = p.cur_date
    GROUP BY d.category1_id
)
SELECT
    cd.tm_id,
    cd.category1_id,
    CAST(cd.cur_date AS STRING) AS k1,
    cd.tm_name,
    cd.category1_name,
    cd.order_count_1d,
    cd.order_num_1d,
    cd.order_user_count_1d,
    cd.order_sku_count_1d,
    cd.order_original_amount_1d,
    cd.activity_reduce_amount_1d,
    cd.coupon_reduce_amount_1d,
    cd.order_total_amount_1d,
    cd.order_count_7d,
    cd.order_num_7d,
    cd.order_user_count_7d,
    cd.order_sku_count_7d,
    cd.order_original_amount_7d,
    cd.activity_reduce_amount_7d,
    cd.coupon_reduce_amount_7d,
    cd.order_total_amount_7d,
    cd.order_count_30d,
    cd.order_num_30d,
    cd.order_user_count_30d,
    cd.order_sku_count_30d,
    cd.order_original_amount_30d,
    cd.activity_reduce_amount_30d,
    cd.coupon_reduce_amount_30d,
    cd.order_total_amount_30d,
    CASE
        WHEN COALESCE(lw.last_week_order_count_1d, 0) = 0 THEN NULL
        ELSE CAST(
            ROUND(
                (CAST(cd.order_count_1d AS DECIMAL(16, 2)) - CAST(COALESCE(lw.last_week_order_count_1d, 0) AS DECIMAL(16, 2)))
                / CAST(COALESCE(lw.last_week_order_count_1d, 1) AS DECIMAL(16, 2))
                * 100,
                2
            ) AS DECIMAL(10, 2)
        )
    END AS order_count_1d_wow_rate,
    CASE
        WHEN COALESCE(lw.last_week_order_total_amount_1d, 0) = 0 THEN NULL
        ELSE CAST(
            ROUND(
                (cd.order_total_amount_1d - COALESCE(lw.last_week_order_total_amount_1d, CAST(0 AS DECIMAL(16, 2))))
                / CAST(COALESCE(lw.last_week_order_total_amount_1d, CAST(1 AS DECIMAL(16, 2))) AS DECIMAL(16, 2))
                * 100,
                2
            ) AS DECIMAL(10, 2)
        )
    END AS order_total_amount_1d_wow_rate,
    CASE
        WHEN COALESCE(lw.last_week_order_count_7d, 0) = 0 THEN NULL
        ELSE CAST(
            ROUND(
                (CAST(cd.order_count_7d AS DECIMAL(16, 2)) - CAST(COALESCE(lw.last_week_order_count_7d, 0) AS DECIMAL(16, 2)))
                / CAST(COALESCE(lw.last_week_order_count_7d, 1) AS DECIMAL(16, 2))
                * 100,
                2
            ) AS DECIMAL(10, 2)
        )
    END AS order_count_7d_wow_rate,
    CASE
        WHEN COALESCE(lw.last_week_order_total_amount_7d, 0) = 0 THEN NULL
        ELSE CAST(
            ROUND(
                (cd.order_total_amount_7d - COALESCE(lw.last_week_order_total_amount_7d, CAST(0 AS DECIMAL(16, 2))))
                / CAST(COALESCE(lw.last_week_order_total_amount_7d, CAST(1 AS DECIMAL(16, 2))) AS DECIMAL(16, 2))
                * 100,
                2
            ) AS DECIMAL(10, 2)
        )
    END AS order_total_amount_7d_wow_rate,
    CASE
        WHEN COALESCE(ly.last_year_order_count_1d, 0) = 0 THEN NULL
        ELSE CAST(
            ROUND(
                (CAST(cd.order_count_1d AS DECIMAL(16, 2)) - CAST(COALESCE(ly.last_year_order_count_1d, 0) AS DECIMAL(16, 2)))
                / CAST(COALESCE(ly.last_year_order_count_1d, 1) AS DECIMAL(16, 2))
                * 100,
                2
            ) AS DECIMAL(10, 2)
        )
    END AS order_count_1d_yoy_rate,
    CASE
        WHEN COALESCE(ly.last_year_order_total_amount_1d, 0) = 0 THEN NULL
        ELSE CAST(
            ROUND(
                (cd.order_total_amount_1d - COALESCE(ly.last_year_order_total_amount_1d, CAST(0 AS DECIMAL(16, 2))))
                / CAST(COALESCE(ly.last_year_order_total_amount_1d, CAST(1 AS DECIMAL(16, 2))) AS DECIMAL(16, 2))
                * 100,
                2
            ) AS DECIMAL(10, 2)
        )
    END AS order_total_amount_1d_yoy_rate,
    CASE
        WHEN COALESCE(ct.category_total_amount_1d, 0) = 0 THEN NULL
        ELSE CAST(
            ROUND(
                cd.order_total_amount_1d
                / CAST(COALESCE(ct.category_total_amount_1d, CAST(1 AS DECIMAL(16, 2))) AS DECIMAL(16, 2))
                * 100,
                2
            ) AS DECIMAL(10, 2)
        )
    END AS category_amount_ratio_1d,
    CASE
        WHEN COALESCE(tt.tm_total_amount_1d, 0) = 0 THEN NULL
        ELSE CAST(
            ROUND(
                cd.order_total_amount_1d
                / CAST(COALESCE(tt.tm_total_amount_1d, CAST(1 AS DECIMAL(16, 2))) AS DECIMAL(16, 2))
                * 100,
                2
            ) AS DECIMAL(10, 2)
        )
    END AS tm_amount_ratio_1d,
    CASE
        WHEN COALESCE(ct.category_sku_count_1d, 0) = 0 THEN NULL
        ELSE CAST(
            ROUND(
                CAST(cd.order_sku_count_1d AS DECIMAL(16, 2))
                / CAST(COALESCE(ct.category_sku_count_1d, 1) AS DECIMAL(16, 2))
                * 100,
                2
            ) AS DECIMAL(10, 2)
        )
    END AS sku_coverage_rate_1d,
    CASE
        WHEN COALESCE(lw.last_week_order_total_amount_1d, 0) = 0 THEN '数据不足'
        WHEN (
            (cd.order_total_amount_1d - COALESCE(lw.last_week_order_total_amount_1d, CAST(0 AS DECIMAL(16, 2))))
            / CAST(COALESCE(lw.last_week_order_total_amount_1d, CAST(1 AS DECIMAL(16, 2))) AS DECIMAL(16, 2))
            * 100
        ) > 30 THEN '高速增长'
        WHEN (
            (cd.order_total_amount_1d - COALESCE(lw.last_week_order_total_amount_1d, CAST(0 AS DECIMAL(16, 2))))
            / CAST(COALESCE(lw.last_week_order_total_amount_1d, CAST(1 AS DECIMAL(16, 2))) AS DECIMAL(16, 2))
            * 100
        ) >= 0 THEN '平稳增长'
        WHEN (
            (cd.order_total_amount_1d - COALESCE(lw.last_week_order_total_amount_1d, CAST(0 AS DECIMAL(16, 2))))
            / CAST(COALESCE(lw.last_week_order_total_amount_1d, CAST(1 AS DECIMAL(16, 2))) AS DECIMAL(16, 2))
            * 100
        ) >= -30 THEN '下降'
        ELSE '急剧下降'
    END AS cat_1d_type,
    CASE
        WHEN COALESCE(lw.last_week_order_total_amount_7d, 0) = 0 THEN '数据不足'
        WHEN (
            (cd.order_total_amount_7d - COALESCE(lw.last_week_order_total_amount_7d, CAST(0 AS DECIMAL(16, 2))))
            / CAST(COALESCE(lw.last_week_order_total_amount_7d, CAST(1 AS DECIMAL(16, 2))) AS DECIMAL(16, 2))
            * 100
        ) > 30 THEN '高速增长'
        WHEN (
            (cd.order_total_amount_7d - COALESCE(lw.last_week_order_total_amount_7d, CAST(0 AS DECIMAL(16, 2))))
            / CAST(COALESCE(lw.last_week_order_total_amount_7d, CAST(1 AS DECIMAL(16, 2))) AS DECIMAL(16, 2))
            * 100
        ) >= 0 THEN '平稳增长'
        WHEN (
            (cd.order_total_amount_7d - COALESCE(lw.last_week_order_total_amount_7d, CAST(0 AS DECIMAL(16, 2))))
            / CAST(COALESCE(lw.last_week_order_total_amount_7d, CAST(1 AS DECIMAL(16, 2))) AS DECIMAL(16, 2))
            * 100
        ) >= -30 THEN '下降'
        ELSE '急剧下降'
    END AS cat_7d_type
FROM current_data cd
LEFT JOIN last_week_data lw
    ON cd.tm_id = lw.tm_id
   AND cd.category1_id = lw.category1_id
LEFT JOIN last_year_data ly
    ON cd.tm_id = ly.tm_id
   AND cd.category1_id = ly.category1_id
LEFT JOIN tm_total tt
    ON cd.tm_id = tt.tm_id
LEFT JOIN category_total ct
    ON cd.category1_id = ct.category1_id;

INSERT INTO iceberg_dws_stream.dws_trade_tm_category1_cat_nd_full(
    tm_id,
    category1_id,
    k1,
    tm_name,
    category1_name,
    order_count_1d,
    order_num_1d,
    order_user_count_1d,
    order_sku_count_1d,
    order_original_amount_1d,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d,
    order_count_7d,
    order_num_7d,
    order_user_count_7d,
    order_sku_count_7d,
    order_original_amount_7d,
    activity_reduce_amount_7d,
    coupon_reduce_amount_7d,
    order_total_amount_7d,
    order_count_30d,
    order_num_30d,
    order_user_count_30d,
    order_sku_count_30d,
    order_original_amount_30d,
    activity_reduce_amount_30d,
    coupon_reduce_amount_30d,
    order_total_amount_30d,
    order_count_1d_wow_rate,
    order_total_amount_1d_wow_rate,
    order_count_7d_wow_rate,
    order_total_amount_7d_wow_rate,
    order_count_1d_yoy_rate,
    order_total_amount_1d_yoy_rate,
    category_amount_ratio_1d,
    tm_amount_ratio_1d,
    sku_coverage_rate_1d,
    cat_1d_type,
    cat_7d_type
)
SELECT * FROM tmp_dws_trade_tm_category1_cat_nd_full_src;


