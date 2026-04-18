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

CREATE TABLE IF NOT EXISTS iceberg_dws.dws_trade_province_sku_order_nd_full(
    `province_id` STRING COMMENT '省份ID - 标识销售区域',
    `sku_id` STRING COMMENT '商品SKU_ID - 标识具体售卖的商品',
    `k1` STRING COMMENT '数据日期 - 分区字段，记录数据所属日期',
    `province_name` STRING COMMENT '省份名称 - 用于展示销售区域',
    `province_area_code` STRING COMMENT '省份区号 - 电话区号，可用于地区分组',
    `province_iso_code` STRING COMMENT '省份ISO编码 - 国际标准编码',
    `sku_name` STRING COMMENT '商品名称 - 展示购买的商品名称',
    `category1_id` STRING COMMENT '一级品类ID - 商品所属一级品类',
    `category1_name` STRING COMMENT '一级品类名称 - 商品所属一级品类名称',
    `category2_id` STRING COMMENT '二级品类ID - 商品所属二级品类',
    `category2_name` STRING COMMENT '二级品类名称 - 商品所属二级品类名称',
    `category3_id` STRING COMMENT '三级品类ID - 商品所属三级品类',
    `category3_name` STRING COMMENT '三级品类名称 - 商品所属三级品类名称',
    `tm_id` STRING COMMENT '品牌ID - 商品所属品牌',
    `tm_name` STRING COMMENT '品牌名称 - 商品所属品牌名称',
    `order_count_1d` BIGINT COMMENT '最近1日下单次数 - 订单总数',
    `order_num_1d` BIGINT COMMENT '最近1日下单件数 - 商品总数量',
    `order_user_count_1d` BIGINT COMMENT '最近1日下单用户数 - 下单用户去重数',
    `order_original_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单原始金额 - 未优惠的原始总金额',
    `activity_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日活动优惠金额 - 活动带来的优惠总金额',
    `coupon_reduce_amount_1d` DECIMAL(16, 2) COMMENT '最近1日优惠券优惠金额 - 优惠券带来的优惠总金额',
    `order_total_amount_1d` DECIMAL(16, 2) COMMENT '最近1日下单最终金额 - 优惠后的实际支付总金额',
    `order_count_7d` BIGINT COMMENT '最近7日下单次数 - 7天内订单总数',
    `order_num_7d` BIGINT COMMENT '最近7日下单件数 - 7天内商品总数量',
    `order_user_count_7d` BIGINT COMMENT '最近7日下单用户数 - 7天内下单用户去重数',
    `order_original_amount_7d` DECIMAL(16, 2) COMMENT '最近7日下单原始金额 - 7天内未优惠的原始总金额',
    `activity_reduce_amount_7d` DECIMAL(16, 2) COMMENT '最近7日活动优惠金额 - 7天内活动带来的优惠总金额',
    `coupon_reduce_amount_7d` DECIMAL(16, 2) COMMENT '最近7日优惠券优惠金额 - 7天内优惠券带来的优惠总金额',
    `order_total_amount_7d` DECIMAL(16, 2) COMMENT '最近7日下单最终金额 - 7天内优惠后的实际支付总金额',
    `order_count_30d` BIGINT COMMENT '最近30日下单次数 - 30天内订单总数',
    `order_num_30d` BIGINT COMMENT '最近30日下单件数 - 商品总数量',
    `order_user_count_30d` BIGINT COMMENT '最近30日下单用户数 - 30天内下单用户去重数',
    `order_original_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单原始金额 - 30天内未优惠的原始总金额',
    `activity_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日活动优惠金额 - 30天内活动带来的优惠总金额',
    `coupon_reduce_amount_30d` DECIMAL(16, 2) COMMENT '最近30日优惠券优惠金额 - 30天内优惠券带来的优惠总金额',
    `order_total_amount_30d` DECIMAL(16, 2) COMMENT '最近30日下单最终金额 - 30天内优惠后的实际支付总金额',
    `order_count_1d_wow_rate` DECIMAL(10, 2) COMMENT '订单数量周环比变化率 - 与上周同期相比的变化百分比',
    `order_count_1d_yoy_rate` DECIMAL(10, 2) COMMENT '订单数量同比变化率 - 与去年同期相比的变化百分比',
    `order_total_amount_1d_wow_rate` DECIMAL(10, 2) COMMENT '订单金额周环比变化率 - 与上周同期相比的变化百分比',
    `order_total_amount_1d_yoy_rate` DECIMAL(10, 2) COMMENT '订单金额同比变化率 - 与去年同期相比的变化百分比',
    PRIMARY KEY (`province_id`, `sku_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

CREATE TEMPORARY VIEW tmp_dws_trade_province_sku_order_nd_full_src AS
WITH date_param AS (
    SELECT CAST('${pdate}' AS DATE) AS cur_date
),
source_1d AS (
    SELECT
        CAST(province_id AS STRING) AS province_id,
        CAST(sku_id AS STRING) AS sku_id,
        CAST(k1 AS DATE) AS dt,
        province_name,
        area_code AS province_area_code,
        iso_code AS province_iso_code,
        sku_name,
        CAST(category1_id AS STRING) AS category1_id,
        category1_name,
        CAST(category2_id AS STRING) AS category2_id,
        category2_name,
        CAST(category3_id AS STRING) AS category3_id,
        category3_name,
        CAST(tm_id AS STRING) AS tm_id,
        tm_name,
        order_count_1d,
        order_num_1d,
        order_user_count_1d,
        order_original_amount_1d,
        activity_reduce_amount_1d,
        coupon_reduce_amount_1d,
        order_total_amount_1d
    FROM iceberg_dws.dws_trade_province_sku_order_1d_full
),
current_data AS (
    SELECT
        s.province_id,
        s.sku_id,
        p.cur_date AS cur_date,
        MAX(s.province_name) AS province_name,
        MAX(s.province_area_code) AS province_area_code,
        MAX(s.province_iso_code) AS province_iso_code,
        MAX(s.sku_name) AS sku_name,
        MAX(s.category1_id) AS category1_id,
        MAX(s.category1_name) AS category1_name,
        MAX(s.category2_id) AS category2_id,
        MAX(s.category2_name) AS category2_name,
        MAX(s.category3_id) AS category3_id,
        MAX(s.category3_name) AS category3_name,
        MAX(s.tm_id) AS tm_id,
        MAX(s.tm_name) AS tm_name,
        SUM(CASE WHEN s.dt = p.cur_date THEN s.order_count_1d ELSE 0 END) AS order_count_1d,
        SUM(CASE WHEN s.dt = p.cur_date THEN s.order_num_1d ELSE 0 END) AS order_num_1d,
        SUM(CASE WHEN s.dt = p.cur_date THEN s.order_user_count_1d ELSE 0 END) AS order_user_count_1d,
        SUM(CASE WHEN s.dt = p.cur_date THEN s.order_original_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS order_original_amount_1d,
        SUM(CASE WHEN s.dt = p.cur_date THEN s.activity_reduce_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS activity_reduce_amount_1d,
        SUM(CASE WHEN s.dt = p.cur_date THEN s.coupon_reduce_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS coupon_reduce_amount_1d,
        SUM(CASE WHEN s.dt = p.cur_date THEN s.order_total_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS order_total_amount_1d,
        SUM(CASE WHEN s.dt BETWEEN p.cur_date - INTERVAL '6' DAY AND p.cur_date THEN s.order_count_1d ELSE 0 END) AS order_count_7d,
        SUM(CASE WHEN s.dt BETWEEN p.cur_date - INTERVAL '6' DAY AND p.cur_date THEN s.order_num_1d ELSE 0 END) AS order_num_7d,
        SUM(CASE WHEN s.dt BETWEEN p.cur_date - INTERVAL '6' DAY AND p.cur_date THEN s.order_user_count_1d ELSE 0 END) AS order_user_count_7d,
        SUM(CASE WHEN s.dt BETWEEN p.cur_date - INTERVAL '6' DAY AND p.cur_date THEN s.order_original_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS order_original_amount_7d,
        SUM(CASE WHEN s.dt BETWEEN p.cur_date - INTERVAL '6' DAY AND p.cur_date THEN s.activity_reduce_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS activity_reduce_amount_7d,
        SUM(CASE WHEN s.dt BETWEEN p.cur_date - INTERVAL '6' DAY AND p.cur_date THEN s.coupon_reduce_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS coupon_reduce_amount_7d,
        SUM(CASE WHEN s.dt BETWEEN p.cur_date - INTERVAL '6' DAY AND p.cur_date THEN s.order_total_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS order_total_amount_7d,
        SUM(CASE WHEN s.dt BETWEEN p.cur_date - INTERVAL '29' DAY AND p.cur_date THEN s.order_count_1d ELSE 0 END) AS order_count_30d,
        SUM(CASE WHEN s.dt BETWEEN p.cur_date - INTERVAL '29' DAY AND p.cur_date THEN s.order_num_1d ELSE 0 END) AS order_num_30d,
        SUM(CASE WHEN s.dt BETWEEN p.cur_date - INTERVAL '29' DAY AND p.cur_date THEN s.order_user_count_1d ELSE 0 END) AS order_user_count_30d,
        SUM(CASE WHEN s.dt BETWEEN p.cur_date - INTERVAL '29' DAY AND p.cur_date THEN s.order_original_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS order_original_amount_30d,
        SUM(CASE WHEN s.dt BETWEEN p.cur_date - INTERVAL '29' DAY AND p.cur_date THEN s.activity_reduce_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS activity_reduce_amount_30d,
        SUM(CASE WHEN s.dt BETWEEN p.cur_date - INTERVAL '29' DAY AND p.cur_date THEN s.coupon_reduce_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS coupon_reduce_amount_30d,
        SUM(CASE WHEN s.dt BETWEEN p.cur_date - INTERVAL '29' DAY AND p.cur_date THEN s.order_total_amount_1d ELSE CAST(0 AS DECIMAL(16, 2)) END) AS order_total_amount_30d
    FROM source_1d s
    CROSS JOIN date_param p
    WHERE s.dt BETWEEN p.cur_date - INTERVAL '29' DAY AND p.cur_date
    GROUP BY s.province_id, s.sku_id, p.cur_date
),
last_week_data AS (
    SELECT
        s.province_id,
        s.sku_id,
        SUM(s.order_count_1d) AS last_week_order_count_1d,
        SUM(s.order_total_amount_1d) AS last_week_order_total_amount_1d
    FROM source_1d s
    CROSS JOIN date_param p
    WHERE s.dt = p.cur_date - INTERVAL '7' DAY
    GROUP BY s.province_id, s.sku_id
),
last_year_data AS (
    SELECT
        s.province_id,
        s.sku_id,
        SUM(s.order_count_1d) AS last_year_order_count_1d,
        SUM(s.order_total_amount_1d) AS last_year_order_total_amount_1d
    FROM source_1d s
    CROSS JOIN date_param p
    WHERE s.dt = p.cur_date - INTERVAL '1' YEAR
    GROUP BY s.province_id, s.sku_id
)
SELECT
    cd.province_id,
    cd.sku_id,
    CAST(cd.cur_date AS STRING) AS k1,
    cd.province_name,
    cd.province_area_code,
    cd.province_iso_code,
    cd.sku_name,
    cd.category1_id,
    cd.category1_name,
    cd.category2_id,
    cd.category2_name,
    cd.category3_id,
    cd.category3_name,
    cd.tm_id,
    cd.tm_name,
    cd.order_count_1d,
    cd.order_num_1d,
    cd.order_user_count_1d,
    cd.order_original_amount_1d,
    cd.activity_reduce_amount_1d,
    cd.coupon_reduce_amount_1d,
    cd.order_total_amount_1d,
    cd.order_count_7d,
    cd.order_num_7d,
    cd.order_user_count_7d,
    cd.order_original_amount_7d,
    cd.activity_reduce_amount_7d,
    cd.coupon_reduce_amount_7d,
    cd.order_total_amount_7d,
    cd.order_count_30d,
    cd.order_num_30d,
    cd.order_user_count_30d,
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
        WHEN COALESCE(ly.last_year_order_total_amount_1d, 0) = 0 THEN NULL
        ELSE CAST(
            ROUND(
                (cd.order_total_amount_1d - COALESCE(ly.last_year_order_total_amount_1d, CAST(0 AS DECIMAL(16, 2))))
                / CAST(COALESCE(ly.last_year_order_total_amount_1d, CAST(1 AS DECIMAL(16, 2))) AS DECIMAL(16, 2))
                * 100,
                2
            ) AS DECIMAL(10, 2)
        )
    END AS order_total_amount_1d_yoy_rate
FROM current_data cd
LEFT JOIN last_week_data lw
    ON cd.province_id = lw.province_id
   AND cd.sku_id = lw.sku_id
LEFT JOIN last_year_data ly
    ON cd.province_id = ly.province_id
   AND cd.sku_id = ly.sku_id;

INSERT INTO iceberg_dws.dws_trade_province_sku_order_nd_full /*+ OPTIONS('upsert-enabled' = 'true') */(
    province_id,
    sku_id,
    k1,
    province_name,
    province_area_code,
    province_iso_code,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    tm_id,
    tm_name,
    order_count_1d,
    order_num_1d,
    order_user_count_1d,
    order_original_amount_1d,
    activity_reduce_amount_1d,
    coupon_reduce_amount_1d,
    order_total_amount_1d,
    order_count_7d,
    order_num_7d,
    order_user_count_7d,
    order_original_amount_7d,
    activity_reduce_amount_7d,
    coupon_reduce_amount_7d,
    order_total_amount_7d,
    order_count_30d,
    order_num_30d,
    order_user_count_30d,
    order_original_amount_30d,
    activity_reduce_amount_30d,
    coupon_reduce_amount_30d,
    order_total_amount_30d,
    order_count_1d_wow_rate,
    order_count_1d_yoy_rate,
    order_total_amount_1d_wow_rate,
    order_total_amount_1d_yoy_rate
)
SELECT * FROM tmp_dws_trade_province_sku_order_nd_full_src;
