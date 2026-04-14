SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
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

CREATE TABLE IF NOT EXISTS hudi_dws.dws_trade_user_cart_add_1d_full(
    `user_id` STRING,
    `k1` STRING,
    `cart_add_count_1d` BIGINT,
    `cart_add_num_1d` BIGINT,
    PRIMARY KEY (`user_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dws.dws_trade_user_cart_add_1d_full VALUES
    ('900001', '2024-06-14', 2, 5),
    ('900001', '2024-06-10', 1, 3),
    ('900001', '2024-05-20', 4, 10),
    ('900001', '2024-05-15', 7, 20),
    ('900002', '2024-06-08', 3, 4),
    ('900002', '2024-05-16', 5, 8),
    ('900002', '2024-05-15', 1, 1),
    ('900003', '2024-05-25', 2, 6);

CREATE TABLE IF NOT EXISTS hudi_dws.dws_trade_user_payment_1d_full(
    `user_id` BIGINT,
    `k1` STRING,
    `payment_count_1d` BIGINT,
    `payment_num_1d` BIGINT,
    `payment_amount_1d` DECIMAL(16, 2),
    PRIMARY KEY (`user_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dws.dws_trade_user_payment_1d_full VALUES
    (900001, '2024-06-14', 1, 2, CAST(50.00 AS DECIMAL(16, 2))),
    (900001, '2024-06-12', 2, 3, CAST(120.00 AS DECIMAL(16, 2))),
    (900001, '2024-05-18', 4, 5, CAST(200.00 AS DECIMAL(16, 2))),
    (900001, '2024-05-15', 9, 9, CAST(900.00 AS DECIMAL(16, 2))),
    (900002, '2024-06-08', 1, 1, CAST(20.00 AS DECIMAL(16, 2))),
    (900002, '2024-05-16', 3, 4, CAST(100.00 AS DECIMAL(16, 2))),
    (900003, '2024-05-20', 2, 2, CAST(60.00 AS DECIMAL(16, 2)));

CREATE TABLE IF NOT EXISTS hudi_dws.dws_trade_user_sku_order_1d_full(
    `user_id` BIGINT,
    `sku_id` BIGINT,
    `k1` STRING,
    `order_count_1d` BIGINT,
    `order_num_1d` BIGINT,
    `order_original_amount_1d` DECIMAL(16, 2),
    `activity_reduce_amount_1d` DECIMAL(16, 2),
    `coupon_reduce_amount_1d` DECIMAL(16, 2),
    `order_total_amount_1d` DECIMAL(16, 2),
    PRIMARY KEY (`user_id`, `sku_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dws.dws_trade_user_sku_order_1d_full VALUES
    (900001, 101, '2024-06-14', 2, 3, CAST(100.00 AS DECIMAL(16, 2)), CAST(10.00 AS DECIMAL(16, 2)), CAST(5.00 AS DECIMAL(16, 2)), CAST(85.00 AS DECIMAL(16, 2))),
    (900001, 102, '2024-06-14', 1, 1, CAST(40.00 AS DECIMAL(16, 2)), CAST(4.00 AS DECIMAL(16, 2)), CAST(1.00 AS DECIMAL(16, 2)), CAST(35.00 AS DECIMAL(16, 2))),
    (900001, 101, '2024-06-10', 1, 1, CAST(50.00 AS DECIMAL(16, 2)), CAST(0.00 AS DECIMAL(16, 2)), CAST(0.00 AS DECIMAL(16, 2)), CAST(50.00 AS DECIMAL(16, 2))),
    (900001, 101, '2024-05-20', 4, 5, CAST(200.00 AS DECIMAL(16, 2)), CAST(20.00 AS DECIMAL(16, 2)), CAST(10.00 AS DECIMAL(16, 2)), CAST(170.00 AS DECIMAL(16, 2))),
    (900001, 102, '2024-05-16', 1, 1, CAST(20.00 AS DECIMAL(16, 2)), CAST(0.00 AS DECIMAL(16, 2)), CAST(2.00 AS DECIMAL(16, 2)), CAST(18.00 AS DECIMAL(16, 2))),
    (900002, 101, '2024-06-08', 1, 1, CAST(30.00 AS DECIMAL(16, 2)), CAST(3.00 AS DECIMAL(16, 2)), CAST(1.00 AS DECIMAL(16, 2)), CAST(26.00 AS DECIMAL(16, 2)));

CREATE TABLE IF NOT EXISTS hudi_dws.dws_trade_user_sku_order_refund_1d_full(
    `user_id` BIGINT,
    `sku_id` BIGINT,
    `k1` STRING,
    `order_refund_count_1d` BIGINT,
    `order_refund_num_1d` BIGINT,
    `order_refund_amount_1d` DECIMAL(16, 2),
    PRIMARY KEY (`user_id`, `sku_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dws.dws_trade_user_sku_order_refund_1d_full VALUES
    (900001, 101, '2024-06-14', 1, 1, CAST(30.00 AS DECIMAL(16, 2))),
    (900001, 102, '2024-06-14', 2, 2, CAST(50.00 AS DECIMAL(16, 2))),
    (900001, 101, '2024-05-20', 1, 3, CAST(90.00 AS DECIMAL(16, 2))),
    (900002, 101, '2024-06-08', 1, 1, CAST(20.00 AS DECIMAL(16, 2)));

CREATE TABLE IF NOT EXISTS hudi_dws.dws_traffic_page_visitor_page_view_1d_full(
    `mid_id` STRING,
    `page_id` STRING,
    `k1` STRING,
    `brand` STRING,
    `model` STRING,
    `operate_system` STRING,
    `during_time_1d` BIGINT,
    `view_count_1d` BIGINT,
    PRIMARY KEY (`mid_id`, `page_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dws.dws_traffic_page_visitor_page_view_1d_full VALUES
    ('mid-1', 'home', '2024-06-14', 'brand-a', 'model-x', 'android', 100, 2),
    ('mid-1', 'detail', '2024-06-14', 'brand-a', 'model-x', 'android', 30, 1),
    ('mid-1', 'home', '2024-06-10', 'brand-a', 'model-x', 'android', 50, 1),
    ('mid-1', 'home', '2024-05-20', 'brand-a', 'model-x', 'android', 200, 4),
    ('mid-2', 'home', '2024-05-18', 'brand-c', 'model-z', 'android', 60, 1);

CREATE TABLE IF NOT EXISTS hudi_dws.dws_trade_user_order_1d_full(
    `user_id` BIGINT,
    `k1` STRING,
    `order_count_1d` BIGINT,
    `order_num_1d` BIGINT,
    `order_original_amount_1d` DECIMAL(16, 2),
    `activity_reduce_amount_1d` DECIMAL(16, 2),
    `coupon_reduce_amount_1d` DECIMAL(16, 2),
    `order_total_amount_1d` DECIMAL(16, 2),
    PRIMARY KEY (`user_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dws.dws_trade_user_order_1d_full VALUES
    (900001, '2024-06-14', 2, 3, CAST(100.00 AS DECIMAL(16, 2)), CAST(10.00 AS DECIMAL(16, 2)), CAST(5.00 AS DECIMAL(16, 2)), CAST(85.00 AS DECIMAL(16, 2))),
    (900001, '2024-06-10', 1, 1, CAST(50.00 AS DECIMAL(16, 2)), CAST(0.00 AS DECIMAL(16, 2)), CAST(0.00 AS DECIMAL(16, 2)), CAST(50.00 AS DECIMAL(16, 2))),
    (900001, '2024-05-20', 4, 5, CAST(200.00 AS DECIMAL(16, 2)), CAST(20.00 AS DECIMAL(16, 2)), CAST(10.00 AS DECIMAL(16, 2)), CAST(170.00 AS DECIMAL(16, 2))),
    (900001, '2024-05-15', 7, 7, CAST(700.00 AS DECIMAL(16, 2)), CAST(70.00 AS DECIMAL(16, 2)), CAST(35.00 AS DECIMAL(16, 2)), CAST(595.00 AS DECIMAL(16, 2))),
    (900002, '2024-06-08', 3, 3, CAST(90.00 AS DECIMAL(16, 2)), CAST(9.00 AS DECIMAL(16, 2)), CAST(0.00 AS DECIMAL(16, 2)), CAST(81.00 AS DECIMAL(16, 2))),
    (900002, '2024-06-07', 2, 2, CAST(40.00 AS DECIMAL(16, 2)), CAST(4.00 AS DECIMAL(16, 2)), CAST(1.00 AS DECIMAL(16, 2)), CAST(35.00 AS DECIMAL(16, 2))),
    (900002, '2024-05-16', 5, 6, CAST(120.00 AS DECIMAL(16, 2)), CAST(0.00 AS DECIMAL(16, 2)), CAST(20.00 AS DECIMAL(16, 2)), CAST(100.00 AS DECIMAL(16, 2))),
    (900002, '2024-05-15', 1, 1, CAST(11.00 AS DECIMAL(16, 2)), CAST(1.00 AS DECIMAL(16, 2)), CAST(1.00 AS DECIMAL(16, 2)), CAST(9.00 AS DECIMAL(16, 2))),
    (900003, '2024-05-25', 1, 2, CAST(30.00 AS DECIMAL(16, 2)), CAST(3.00 AS DECIMAL(16, 2)), CAST(1.00 AS DECIMAL(16, 2)), CAST(26.00 AS DECIMAL(16, 2)));

CREATE TABLE IF NOT EXISTS hudi_dim.dim_sku_full(
    `id` BIGINT,
    `k1` STRING,
    `sku_name` STRING,
    `category1_id` BIGINT,
    `category1_name` STRING,
    `category2_id` BIGINT,
    `category2_name` STRING,
    `category3_id` BIGINT,
    `category3_name` STRING,
    `tm_id` BIGINT,
    `tm_name` STRING,
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dim.dim_sku_full VALUES
    (101, '2024-06-10', 'sku-101-old', 10, 'c1-old', 20, 'c2-old', 30, 'c3-old', 1, 'tm-old'),
    (101, '2024-06-14', 'sku-101-new', 11, 'c1-new', 21, 'c2-new', 31, 'c3-new', 1, 'tm-1'),
    (102, '2024-06-14', 'sku-102', 12, 'c1-12', 22, 'c2-22', 32, 'c3-32', 2, 'tm-2');

CREATE TABLE IF NOT EXISTS hudi_dim.dim_user_zip_full(
    `id` BIGINT,
    `k1` STRING,
    `create_time` TIMESTAMP(3),
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dim.dim_user_zip_full VALUES
    (1001, '2024-06-10', TIMESTAMP '2024-05-01 08:00:00'),
    (1001, '2024-06-14', TIMESTAMP '2024-05-01 08:00:00'),
    (1002, '2024-06-14', TIMESTAMP '2024-05-20 09:00:00'),
    (1003, '2024-06-14', TIMESTAMP '2024-06-13 10:00:00'),
    (1004, '2024-06-10', TIMESTAMP '2024-06-01 11:00:00'),
    (1004, '2024-06-14', TIMESTAMP '2024-06-01 11:00:00');

CREATE TABLE IF NOT EXISTS hudi_dwd.dwd_user_login_full(
    `user_id` STRING,
    `k1` STRING,
    PRIMARY KEY (`user_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dwd.dwd_user_login_full VALUES
    ('1001', '2024-05-21'),
    ('1001', '2024-06-10'),
    ('1001', '2024-06-14'),
    ('1002', '2024-06-01'),
    ('1002', '2024-06-08'),
    ('1004', '2024-06-09');
