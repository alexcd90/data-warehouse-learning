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
    (101, '2024-06-14', 'sku-101', 11, 'cate1-11', 21, 'cate2-21', 31, 'cate3-31', 1001, 'tm-1001'),
    (102, '2024-06-14', 'sku-102', 12, 'cate1-12', 22, 'cate2-22', 32, 'cate3-32', 1002, 'tm-1002');

CREATE TABLE IF NOT EXISTS hudi_dim.dim_activity_full(
    `activity_rule_id` INT,
    `activity_id` BIGINT,
    `k1` STRING,
    `activity_name` STRING,
    `activity_type_code` STRING,
    `activity_type_name` STRING,
    `start_time` STRING,
    PRIMARY KEY (`activity_rule_id`, `activity_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dim.dim_activity_full VALUES
    (1, 201, '2024-06-14', 'activity-201', '3101', 'full-minus', '2024-06-01 00:00:00'),
    (2, 202, '2024-06-14', 'activity-202', '3102', 'discount', '2024-05-20 00:00:00');

CREATE TABLE IF NOT EXISTS hudi_dim.dim_coupon_full(
    `id` BIGINT,
    `k1` STRING,
    `coupon_name` STRING,
    `coupon_type_code` STRING,
    `coupon_type_name` STRING,
    `benefit_rule` STRING,
    `start_time` TIMESTAMP(3),
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dim.dim_coupon_full VALUES
    (301, '2024-06-14', 'coupon-301', '3201', 'minus', 'minus-10', TIMESTAMP '2024-06-01 00:00:00'),
    (302, '2024-06-14', 'coupon-302', '3202', 'discount', 'discount-20', TIMESTAMP '2024-05-18 00:00:00');

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
    (1001, '2024-06-14', TIMESTAMP '2024-05-01 08:00:00'),
    (1002, '2024-06-14', TIMESTAMP '2024-05-20 09:00:00'),
    (1003, '2024-06-14', TIMESTAMP '2024-06-13 10:00:00');

CREATE TABLE IF NOT EXISTS hudi_dim.dim_province_full(
    `id` BIGINT,
    `province_name` STRING,
    `area_code` STRING,
    `iso_code` STRING,
    `iso_3166_2` STRING,
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dim.dim_province_full VALUES
    (11, 'province-11', '110000', 'CN-11', 'CN-11'),
    (12, 'province-12', '120000', 'CN-12', 'CN-12');

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
    ('1001', '2024-06-01'),
    ('1001', '2024-06-10'),
    ('1001', '2024-06-14'),
    ('1002', '2024-06-08');

CREATE TABLE IF NOT EXISTS hudi_dwd.dwd_trade_cart_add_full(
    `id` BIGINT,
    `k1` STRING,
    `user_id` STRING,
    `sku_id` BIGINT,
    `sku_num` BIGINT,
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dwd.dwd_trade_cart_add_full VALUES
    (10001, '2024-06-14', '900001', 101, 2),
    (10002, '2024-06-14', '900001', 102, 1),
    (10003, '2024-06-10', '900001', 101, 4),
    (10004, '2024-05-20', '900001', 102, 5),
    (10005, '2024-06-08', '900002', 102, 2),
    (10006, '2024-06-05', '900002', 101, 1),
    (10007, '2024-05-18', '900002', 102, 3),
    (10008, '2024-05-25', '900003', 101, 2);

CREATE TABLE IF NOT EXISTS hudi_dwd.dwd_trade_order_detail_full(
    `id` BIGINT,
    `k1` STRING,
    `order_id` BIGINT,
    `user_id` BIGINT,
    `sku_id` BIGINT,
    `province_id` BIGINT,
    `activity_id` BIGINT,
    `coupon_id` BIGINT,
    `sku_num` BIGINT,
    `split_original_amount` DECIMAL(16, 2),
    `split_activity_amount` DECIMAL(16, 2),
    `split_coupon_amount` DECIMAL(16, 2),
    `split_total_amount` DECIMAL(16, 2),
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dwd.dwd_trade_order_detail_full VALUES
    (20001, '2024-06-14', 5001, 900001, 101, 11, 201, 301, 2, CAST(100.00 AS DECIMAL(16, 2)), CAST(10.00 AS DECIMAL(16, 2)), CAST(5.00 AS DECIMAL(16, 2)), CAST(85.00 AS DECIMAL(16, 2))),
    (20002, '2024-06-14', 5001, 900001, 102, 11, CAST(NULL AS BIGINT), 301, 1, CAST(60.00 AS DECIMAL(16, 2)), CAST(0.00 AS DECIMAL(16, 2)), CAST(6.00 AS DECIMAL(16, 2)), CAST(54.00 AS DECIMAL(16, 2))),
    (20003, '2024-06-14', 5002, 900001, 101, 11, 201, CAST(NULL AS BIGINT), 1, CAST(50.00 AS DECIMAL(16, 2)), CAST(5.00 AS DECIMAL(16, 2)), CAST(0.00 AS DECIMAL(16, 2)), CAST(45.00 AS DECIMAL(16, 2))),
    (20004, '2024-06-10', 5003, 900001, 101, 11, 202, 302, 1, CAST(70.00 AS DECIMAL(16, 2)), CAST(7.00 AS DECIMAL(16, 2)), CAST(3.00 AS DECIMAL(16, 2)), CAST(60.00 AS DECIMAL(16, 2))),
    (20005, '2024-05-20', 5004, 900002, 101, 12, CAST(NULL AS BIGINT), 302, 1, CAST(30.00 AS DECIMAL(16, 2)), CAST(0.00 AS DECIMAL(16, 2)), CAST(3.00 AS DECIMAL(16, 2)), CAST(27.00 AS DECIMAL(16, 2))),
    (20006, '2024-05-20', 5004, 900002, 102, 12, CAST(NULL AS BIGINT), 302, 2, CAST(80.00 AS DECIMAL(16, 2)), CAST(0.00 AS DECIMAL(16, 2)), CAST(8.00 AS DECIMAL(16, 2)), CAST(72.00 AS DECIMAL(16, 2))),
    (20007, '2024-06-08', 5005, 900002, 102, 12, 202, CAST(NULL AS BIGINT), 1, CAST(40.00 AS DECIMAL(16, 2)), CAST(4.00 AS DECIMAL(16, 2)), CAST(0.00 AS DECIMAL(16, 2)), CAST(36.00 AS DECIMAL(16, 2))),
    (20008, '2024-05-18', 5006, 900003, 101, 11, CAST(NULL AS BIGINT), 301, 1, CAST(20.00 AS DECIMAL(16, 2)), CAST(0.00 AS DECIMAL(16, 2)), CAST(2.00 AS DECIMAL(16, 2)), CAST(18.00 AS DECIMAL(16, 2)));

CREATE TABLE IF NOT EXISTS hudi_dwd.dwd_tool_coupon_order_full(
    `id` BIGINT,
    `k1` STRING,
    `coupon_id` BIGINT,
    `order_id` BIGINT,
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dwd.dwd_tool_coupon_order_full VALUES
    (30001, '2024-06-14', 301, 5001),
    (30002, '2024-06-10', 302, 5003),
    (30003, '2024-05-20', 302, 5004),
    (30004, '2024-05-18', 301, 5006);

CREATE TABLE IF NOT EXISTS hudi_dwd.dwd_trade_pay_detail_suc_full(
    `id` BIGINT,
    `k1` STRING,
    `order_id` BIGINT,
    `user_id` BIGINT,
    `sku_id` BIGINT,
    `sku_num` BIGINT,
    `split_payment_amount` DECIMAL(16, 2),
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dwd.dwd_trade_pay_detail_suc_full VALUES
    (40001, '2024-06-14', 5001, 900001, 101, 2, CAST(85.00 AS DECIMAL(16, 2))),
    (40002, '2024-06-14', 5001, 900001, 102, 1, CAST(54.00 AS DECIMAL(16, 2))),
    (40003, '2024-06-14', 5002, 900001, 101, 1, CAST(45.00 AS DECIMAL(16, 2))),
    (40004, '2024-06-10', 5003, 900001, 101, 1, CAST(60.00 AS DECIMAL(16, 2))),
    (40005, '2024-05-20', 5004, 900002, 101, 1, CAST(27.00 AS DECIMAL(16, 2))),
    (40006, '2024-05-20', 5004, 900002, 102, 2, CAST(72.00 AS DECIMAL(16, 2))),
    (40007, '2024-06-08', 5005, 900002, 102, 1, CAST(36.00 AS DECIMAL(16, 2))),
    (40008, '2024-05-18', 5006, 900003, 101, 1, CAST(18.00 AS DECIMAL(16, 2)));

CREATE TABLE IF NOT EXISTS hudi_dwd.dwd_trade_order_refund_full(
    `id` BIGINT,
    `k1` STRING,
    `user_id` BIGINT,
    `order_id` BIGINT,
    `sku_id` BIGINT,
    `refund_num` BIGINT,
    `refund_amount` DECIMAL(16, 2),
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dwd.dwd_trade_order_refund_full VALUES
    (50001, '2024-06-14', 900001, 5001, 101, 1, CAST(40.00 AS DECIMAL(16, 2))),
    (50002, '2024-06-10', 900001, 5003, 101, 1, CAST(60.00 AS DECIMAL(16, 2))),
    (50003, '2024-05-20', 900002, 5004, 101, 1, CAST(27.00 AS DECIMAL(16, 2))),
    (50004, '2024-06-08', 900002, 5005, 102, 1, CAST(36.00 AS DECIMAL(16, 2)));

CREATE TABLE IF NOT EXISTS hudi_dwd.dwd_traffic_page_view_full(
    `id` STRING,
    `k1` STRING,
    `brand` STRING,
    `channel` STRING,
    `model` STRING,
    `mid_id` STRING,
    `operate_system` STRING,
    `version_code` STRING,
    `page_id` STRING,
    `session_id` STRING,
    `during_time` BIGINT,
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dwd.dwd_traffic_page_view_full VALUES
    ('pv1', '2024-06-14', 'brand-a', 'app', 'model-x', 'mid-1', 'android', '1.0.0', 'home', 's1', 10),
    ('pv2', '2024-06-14', 'brand-a', 'app', 'model-x', 'mid-1', 'android', '1.0.0', 'detail', 's1', 20),
    ('pv3', '2024-06-14', 'brand-a', 'app', 'model-x', 'mid-1', 'android', '1.0.0', 'home', 's1', 30),
    ('pv4', '2024-06-14', 'brand-c', 'app', 'model-z', 'mid-2', 'android', '1.0.0', 'home', 's2', 15),
    ('pv5', '2024-06-10', 'brand-a', 'app', 'model-x', 'mid-1', 'android', '1.0.0', 'home', 's3', 40),
    ('pv6', '2024-05-20', 'brand-a', 'app', 'model-x', 'mid-1', 'android', '1.0.0', 'home', 's4', 50);
