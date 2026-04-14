SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_dws;
CREATE DATABASE IF NOT EXISTS hudi_dim;
CREATE DATABASE IF NOT EXISTS hudi_dwd;

DROP TABLE IF EXISTS hudi_dws.dws_trade_activity_order_nd_full;
DROP TABLE IF EXISTS hudi_dws.dws_trade_coupon_order_nd_full;
DROP TABLE IF EXISTS hudi_dws.dws_trade_province_order_1d_full;
DROP TABLE IF EXISTS hudi_dws.dws_trade_province_order_nd_full;
DROP TABLE IF EXISTS hudi_dws.dws_trade_user_cart_add_1d_full;
DROP TABLE IF EXISTS hudi_dws.dws_trade_user_cart_add_nd_full;
DROP TABLE IF EXISTS hudi_dws.dws_trade_user_order_1d_full;
DROP TABLE IF EXISTS hudi_dws.dws_trade_user_order_nd_full;
DROP TABLE IF EXISTS hudi_dws.dws_trade_user_order_refund_1d_full;
DROP TABLE IF EXISTS hudi_dws.dws_trade_user_order_refund_nd_full;
DROP TABLE IF EXISTS hudi_dws.dws_trade_user_order_td_full;
DROP TABLE IF EXISTS hudi_dws.dws_trade_user_payment_1d_full;
DROP TABLE IF EXISTS hudi_dws.dws_trade_user_payment_nd_full;
DROP TABLE IF EXISTS hudi_dws.dws_trade_user_sku_order_1d_full;
DROP TABLE IF EXISTS hudi_dws.dws_trade_user_sku_order_nd_full;
DROP TABLE IF EXISTS hudi_dws.dws_trade_user_sku_order_refund_1d_full;
DROP TABLE IF EXISTS hudi_dws.dws_trade_user_sku_order_refund_nd_full;
DROP TABLE IF EXISTS hudi_dws.dws_traffic_page_visitor_page_view_1d_full;
DROP TABLE IF EXISTS hudi_dws.dws_traffic_page_visitor_page_view_nd_full;
DROP TABLE IF EXISTS hudi_dws.dws_traffic_session_page_view_1d_full;
DROP TABLE IF EXISTS hudi_dws.dws_user_user_login_td_full;

DROP TABLE IF EXISTS hudi_dim.dim_activity_full;
DROP TABLE IF EXISTS hudi_dim.dim_coupon_full;
DROP TABLE IF EXISTS hudi_dim.dim_province_full;
DROP TABLE IF EXISTS hudi_dim.dim_sku_full;
DROP TABLE IF EXISTS hudi_dim.dim_user_zip_full;

DROP TABLE IF EXISTS hudi_dwd.dwd_tool_coupon_order_full;
DROP TABLE IF EXISTS hudi_dwd.dwd_trade_cart_add_full;
DROP TABLE IF EXISTS hudi_dwd.dwd_trade_order_detail_full;
DROP TABLE IF EXISTS hudi_dwd.dwd_trade_order_refund_full;
DROP TABLE IF EXISTS hudi_dwd.dwd_trade_pay_detail_suc_full;
DROP TABLE IF EXISTS hudi_dwd.dwd_traffic_page_view_full;
DROP TABLE IF EXISTS hudi_dwd.dwd_user_login_full;
