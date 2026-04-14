SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'sql-client.display.max-column-width' = '4096';
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

SELECT CONCAT_WS(
    '^',
    'row',
    'dws_trade_activity_order_nd_full',
    CAST(activity_id AS STRING),
    k1,
    COALESCE(activity_name, ''),
    COALESCE(activity_type_code, ''),
    COALESCE(activity_type_name, ''),
    COALESCE(start_date, ''),
    CAST(original_amount_30d AS STRING),
    CAST(activity_reduce_amount_30d AS STRING)
) AS actual_line
FROM hudi_dws.dws_trade_activity_order_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_trade_coupon_order_nd_full',
    CAST(coupon_id AS STRING),
    k1,
    COALESCE(coupon_name, ''),
    COALESCE(coupon_type_code, ''),
    COALESCE(coupon_type_name, ''),
    COALESCE(coupon_rule, ''),
    COALESCE(start_date, ''),
    CAST(original_amount_30d AS STRING),
    CAST(coupon_reduce_amount_30d AS STRING)
)
FROM hudi_dws.dws_trade_coupon_order_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_trade_province_order_1d_full',
    CAST(province_id AS STRING),
    k1,
    COALESCE(province_name, ''),
    COALESCE(area_code, ''),
    COALESCE(iso_code, ''),
    COALESCE(iso_3166_2, ''),
    CAST(order_count_1d AS STRING),
    CAST(order_original_amount_1d AS STRING),
    CAST(activity_reduce_amount_1d AS STRING),
    CAST(coupon_reduce_amount_1d AS STRING),
    CAST(order_total_amount_1d AS STRING)
)
FROM hudi_dws.dws_trade_province_order_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_trade_province_order_nd_full',
    CAST(province_id AS STRING),
    k1,
    COALESCE(province_name, ''),
    COALESCE(area_code, ''),
    COALESCE(iso_code, ''),
    COALESCE(iso_3166_2, ''),
    CAST(order_count_7d AS STRING),
    CAST(order_original_amount_7d AS STRING),
    CAST(activity_reduce_amount_7d AS STRING),
    CAST(coupon_reduce_amount_7d AS STRING),
    CAST(order_total_amount_7d AS STRING),
    CAST(order_count_30d AS STRING),
    CAST(order_original_amount_30d AS STRING),
    CAST(activity_reduce_amount_30d AS STRING),
    CAST(coupon_reduce_amount_30d AS STRING),
    CAST(order_total_amount_30d AS STRING)
)
FROM hudi_dws.dws_trade_province_order_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_trade_user_cart_add_1d_full',
    user_id,
    k1,
    CAST(cart_add_count_1d AS STRING),
    CAST(cart_add_num_1d AS STRING)
)
FROM hudi_dws.dws_trade_user_cart_add_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_trade_user_cart_add_nd_full',
    user_id,
    k1,
    CAST(cart_add_count_7d AS STRING),
    CAST(cart_add_num_7d AS STRING),
    CAST(cart_add_count_30d AS STRING),
    CAST(cart_add_num_30d AS STRING)
)
FROM hudi_dws.dws_trade_user_cart_add_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_trade_user_order_1d_full',
    CAST(user_id AS STRING),
    k1,
    CAST(order_count_1d AS STRING),
    CAST(order_num_1d AS STRING),
    CAST(order_original_amount_1d AS STRING),
    CAST(activity_reduce_amount_1d AS STRING),
    CAST(coupon_reduce_amount_1d AS STRING),
    CAST(order_total_amount_1d AS STRING)
)
FROM hudi_dws.dws_trade_user_order_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_trade_user_order_nd_full',
    CAST(user_id AS STRING),
    k1,
    CAST(order_count_7d AS STRING),
    CAST(order_num_7d AS STRING),
    CAST(order_original_amount_7d AS STRING),
    CAST(activity_reduce_amount_7d AS STRING),
    CAST(coupon_reduce_amount_7d AS STRING),
    CAST(order_total_amount_7d AS STRING),
    CAST(order_count_30d AS STRING),
    CAST(order_num_30d AS STRING),
    CAST(order_original_amount_30d AS STRING),
    CAST(activity_reduce_amount_30d AS STRING),
    CAST(coupon_reduce_amount_30d AS STRING),
    CAST(order_total_amount_30d AS STRING)
)
FROM hudi_dws.dws_trade_user_order_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_trade_user_order_refund_1d_full',
    CAST(user_id AS STRING),
    k1,
    CAST(order_refund_count_1d AS STRING),
    CAST(order_refund_num_1d AS STRING),
    CAST(order_refund_amount_1d AS STRING)
)
FROM hudi_dws.dws_trade_user_order_refund_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_trade_user_order_refund_nd_full',
    CAST(user_id AS STRING),
    k1,
    CAST(order_refund_count_7d AS STRING),
    CAST(order_refund_num_7d AS STRING),
    CAST(order_refund_amount_7d AS STRING),
    CAST(order_refund_count_30d AS STRING),
    CAST(order_refund_num_30d AS STRING),
    CAST(order_refund_amount_30d AS STRING)
)
FROM hudi_dws.dws_trade_user_order_refund_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_trade_user_order_td_full',
    CAST(user_id AS STRING),
    k1,
    COALESCE(order_date_first, ''),
    COALESCE(order_date_last, ''),
    CAST(order_count_td AS STRING),
    CAST(order_num_td AS STRING),
    CAST(original_amount_td AS STRING),
    CAST(activity_reduce_amount_td AS STRING),
    CAST(coupon_reduce_amount_td AS STRING),
    CAST(total_amount_td AS STRING)
)
FROM hudi_dws.dws_trade_user_order_td_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_trade_user_payment_1d_full',
    CAST(user_id AS STRING),
    k1,
    CAST(payment_count_1d AS STRING),
    CAST(payment_num_1d AS STRING),
    CAST(payment_amount_1d AS STRING)
)
FROM hudi_dws.dws_trade_user_payment_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_trade_user_payment_nd_full',
    CAST(user_id AS STRING),
    k1,
    CAST(payment_count_7d AS STRING),
    CAST(payment_num_7d AS STRING),
    CAST(payment_amount_7d AS STRING),
    CAST(payment_count_30d AS STRING),
    CAST(payment_num_30d AS STRING),
    CAST(payment_amount_30d AS STRING)
)
FROM hudi_dws.dws_trade_user_payment_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_trade_user_sku_order_1d_full',
    CAST(user_id AS STRING),
    CAST(sku_id AS STRING),
    k1,
    COALESCE(sku_name, ''),
    CAST(category1_id AS STRING),
    COALESCE(category1_name, ''),
    CAST(category2_id AS STRING),
    COALESCE(category2_name, ''),
    CAST(category3_id AS STRING),
    COALESCE(category3_name, ''),
    CAST(tm_id AS STRING),
    COALESCE(tm_name, ''),
    CAST(order_count_1d AS STRING),
    CAST(order_num_1d AS STRING),
    CAST(order_original_amount_1d AS STRING),
    CAST(activity_reduce_amount_1d AS STRING),
    CAST(coupon_reduce_amount_1d AS STRING),
    CAST(order_total_amount_1d AS STRING)
)
FROM hudi_dws.dws_trade_user_sku_order_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_trade_user_sku_order_nd_full',
    CAST(user_id AS STRING),
    CAST(sku_id AS STRING),
    k1,
    COALESCE(sku_name, ''),
    CAST(category1_id AS STRING),
    COALESCE(category1_name, ''),
    CAST(category2_id AS STRING),
    COALESCE(category2_name, ''),
    CAST(category3_id AS STRING),
    COALESCE(category3_name, ''),
    CAST(tm_id AS STRING),
    COALESCE(tm_name, ''),
    CAST(order_count_7d AS STRING),
    CAST(order_num_7d AS STRING),
    CAST(order_original_amount_7d AS STRING),
    CAST(activity_reduce_amount_7d AS STRING),
    CAST(coupon_reduce_amount_7d AS STRING),
    CAST(order_total_amount_7d AS STRING),
    CAST(order_count_30d AS STRING),
    CAST(order_num_30d AS STRING),
    CAST(order_original_amount_30d AS STRING),
    CAST(activity_reduce_amount_30d AS STRING),
    CAST(coupon_reduce_amount_30d AS STRING),
    CAST(order_total_amount_30d AS STRING)
)
FROM hudi_dws.dws_trade_user_sku_order_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_trade_user_sku_order_refund_1d_full',
    CAST(user_id AS STRING),
    CAST(sku_id AS STRING),
    k1,
    COALESCE(sku_name, ''),
    CAST(category1_id AS STRING),
    COALESCE(category1_name, ''),
    CAST(category2_id AS STRING),
    COALESCE(category2_name, ''),
    CAST(category3_id AS STRING),
    COALESCE(category3_name, ''),
    CAST(tm_id AS STRING),
    COALESCE(tm_name, ''),
    CAST(order_refund_count_1d AS STRING),
    CAST(order_refund_num_1d AS STRING),
    CAST(order_refund_amount_1d AS STRING)
)
FROM hudi_dws.dws_trade_user_sku_order_refund_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_trade_user_sku_order_refund_nd_full',
    CAST(user_id AS STRING),
    CAST(sku_id AS STRING),
    k1,
    COALESCE(sku_name, ''),
    CAST(category1_id AS STRING),
    COALESCE(category1_name, ''),
    CAST(category2_id AS STRING),
    COALESCE(category2_name, ''),
    CAST(category3_id AS STRING),
    COALESCE(category3_name, ''),
    CAST(tm_id AS STRING),
    COALESCE(tm_name, ''),
    CAST(order_refund_count_7d AS STRING),
    CAST(order_refund_num_7d AS STRING),
    CAST(order_refund_amount_7d AS STRING),
    CAST(order_refund_count_30d AS STRING),
    CAST(order_refund_num_30d AS STRING),
    CAST(order_refund_amount_30d AS STRING)
)
FROM hudi_dws.dws_trade_user_sku_order_refund_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_traffic_page_visitor_page_view_1d_full',
    COALESCE(mid_id, ''),
    COALESCE(page_id, ''),
    k1,
    COALESCE(brand, ''),
    COALESCE(model, ''),
    COALESCE(operate_system, ''),
    CAST(during_time_1d AS STRING),
    CAST(view_count_1d AS STRING)
)
FROM hudi_dws.dws_traffic_page_visitor_page_view_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_traffic_page_visitor_page_view_nd_full',
    COALESCE(mid_id, ''),
    COALESCE(page_id, ''),
    k1,
    COALESCE(brand, ''),
    COALESCE(model, ''),
    COALESCE(operate_system, ''),
    CAST(during_time_7d AS STRING),
    CAST(view_count_7d AS STRING),
    CAST(during_time_30d AS STRING),
    CAST(view_count_30d AS STRING)
)
FROM hudi_dws.dws_traffic_page_visitor_page_view_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_traffic_session_page_view_1d_full',
    COALESCE(session_id, ''),
    COALESCE(mid_id, ''),
    k1,
    COALESCE(brand, ''),
    COALESCE(model, ''),
    COALESCE(operate_system, ''),
    COALESCE(version_code, ''),
    COALESCE(channel, ''),
    CAST(during_time_1d AS STRING),
    CAST(page_count_1d AS STRING)
)
FROM hudi_dws.dws_traffic_session_page_view_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS(
    '^',
    'row',
    'dws_user_user_login_td_full',
    CAST(user_id AS STRING),
    k1,
    COALESCE(login_date_last, ''),
    CAST(login_count_td AS STRING)
)
FROM hudi_dws.dws_user_user_login_td_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_trade_activity_order_nd_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_trade_activity_order_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_trade_coupon_order_nd_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_trade_coupon_order_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_trade_province_order_1d_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_trade_province_order_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_trade_province_order_nd_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_trade_province_order_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_trade_user_cart_add_1d_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_trade_user_cart_add_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_trade_user_cart_add_nd_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_trade_user_cart_add_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_trade_user_order_1d_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_trade_user_order_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_trade_user_order_nd_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_trade_user_order_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_trade_user_order_refund_1d_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_trade_user_order_refund_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_trade_user_order_refund_nd_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_trade_user_order_refund_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_trade_user_order_td_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_trade_user_order_td_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_trade_user_payment_1d_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_trade_user_payment_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_trade_user_payment_nd_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_trade_user_payment_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_trade_user_sku_order_1d_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_trade_user_sku_order_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_trade_user_sku_order_nd_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_trade_user_sku_order_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_trade_user_sku_order_refund_1d_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_trade_user_sku_order_refund_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_trade_user_sku_order_refund_nd_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_trade_user_sku_order_refund_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_traffic_page_visitor_page_view_1d_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_traffic_page_visitor_page_view_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_traffic_page_visitor_page_view_nd_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_traffic_page_visitor_page_view_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_traffic_session_page_view_1d_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_traffic_session_page_view_1d_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
UNION ALL
SELECT CONCAT_WS('^', 'count', 'dws_user_user_login_td_full', CAST(COUNT(*) AS STRING))
FROM hudi_dws.dws_user_user_login_td_full /*+ OPTIONS('read.streaming.enabled' = 'false') */;
