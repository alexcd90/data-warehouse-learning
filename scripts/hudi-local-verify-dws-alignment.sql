SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'sql-client.display.max-column-width' = '512';
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

SELECT CONCAT_WS(
    '|',
    'cart_add_nd',
    user_id,
    k1,
    CAST(cart_add_count_7d AS STRING),
    CAST(cart_add_num_7d AS STRING),
    CAST(cart_add_count_30d AS STRING),
    CAST(cart_add_num_30d AS STRING)
) AS actual_line
FROM hudi_dws.dws_trade_user_cart_add_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
WHERE k1 = '2024-06-14'
UNION ALL
SELECT CONCAT_WS(
    '|',
    'payment_nd',
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
WHERE k1 = '2024-06-14'
UNION ALL
SELECT CONCAT_WS(
    '|',
    'sku_order_nd',
    CAST(user_id AS STRING),
    CAST(sku_id AS STRING),
    k1,
    sku_name,
    CAST(category1_id AS STRING),
    category1_name,
    CAST(category2_id AS STRING),
    category2_name,
    CAST(category3_id AS STRING),
    category3_name,
    CAST(tm_id AS STRING),
    tm_name,
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
WHERE k1 = '2024-06-14'
UNION ALL
SELECT CONCAT_WS(
    '|',
    'sku_refund_nd',
    CAST(user_id AS STRING),
    CAST(sku_id AS STRING),
    k1,
    sku_name,
    CAST(category1_id AS STRING),
    category1_name,
    CAST(category2_id AS STRING),
    category2_name,
    CAST(category3_id AS STRING),
    category3_name,
    CAST(tm_id AS STRING),
    tm_name,
    CAST(order_refund_count_7d AS STRING),
    CAST(order_refund_num_7d AS STRING),
    CAST(order_refund_amount_7d AS STRING),
    CAST(order_refund_count_30d AS STRING),
    CAST(order_refund_num_30d AS STRING),
    CAST(order_refund_amount_30d AS STRING)
)
FROM hudi_dws.dws_trade_user_sku_order_refund_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
WHERE k1 = '2024-06-14'
UNION ALL
SELECT CONCAT_WS(
    '|',
    'traffic_nd',
    mid_id,
    page_id,
    k1,
    brand,
    model,
    operate_system,
    CAST(during_time_7d AS STRING),
    CAST(view_count_7d AS STRING),
    CAST(during_time_30d AS STRING),
    CAST(view_count_30d AS STRING)
)
FROM hudi_dws.dws_traffic_page_visitor_page_view_nd_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
WHERE k1 = '2024-06-14'
UNION ALL
SELECT CONCAT_WS(
    '|',
    'order_nd',
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
WHERE k1 = '2024-06-14'
UNION ALL
SELECT CONCAT_WS(
    '|',
    'order_td',
    CAST(user_id AS STRING),
    k1,
    order_date_first,
    order_date_last,
    CAST(order_count_td AS STRING),
    CAST(order_num_td AS STRING),
    CAST(original_amount_td AS STRING),
    CAST(activity_reduce_amount_td AS STRING),
    CAST(coupon_reduce_amount_td AS STRING),
    CAST(total_amount_td AS STRING)
)
FROM hudi_dws.dws_trade_user_order_td_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
WHERE k1 = '2024-06-14'
UNION ALL
SELECT CONCAT_WS(
    '|',
    'login_td',
    CAST(user_id AS STRING),
    k1,
    login_date_last,
    CAST(login_count_td AS STRING)
)
FROM hudi_dws.dws_user_user_login_td_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
WHERE k1 = '2024-06-14';
