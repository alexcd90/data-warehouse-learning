SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

CREATE TABLE ware_order_task_full_mq (
    `id` BIGINT NOT NULL COMMENT '编号',
    `order_id` BIGINT COMMENT '订单编号',
    `consignee` STRING COMMENT '收货人',
    `consignee_tel` STRING COMMENT '收货人电话',
    `delivery_address` STRING COMMENT '送货地址',
    `order_comment` STRING COMMENT '订单备注',
    `payment_way` STRING COMMENT '付款方式 1:在线付款 2:货到付款',
    `task_status` STRING COMMENT '工作单状态',
    `order_body` STRING COMMENT '订单描述',
    `tracking_no` STRING COMMENT '物流单号',
    `create_time` TIMESTAMP(3) NOT NULL COMMENT '创建时间',
    `ware_id` BIGINT COMMENT '仓库编号',
    `task_comment` STRING COMMENT '工作单备注',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'ware_order_task',
    'server-time-zone' = 'Asia/Shanghai'
);

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG paimon_hive;
CREATE DATABASE IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.ods_ware_order_task_full(
    `id` BIGINT NOT NULL COMMENT '编号',
    `k1` STRING COMMENT 'partition field',
    `order_id` BIGINT COMMENT '订单编号',
    `consignee` STRING COMMENT '收货人',
    `consignee_tel` STRING COMMENT '收货人电话',
    `delivery_address` STRING COMMENT '送货地址',
    `order_comment` STRING COMMENT '订单备注',
    `payment_way` STRING COMMENT '付款方式 1:在线付款 2:货到付款',
    `task_status` STRING COMMENT '工作单状态',
    `order_body` STRING COMMENT '订单描述',
    `tracking_no` STRING COMMENT '物流单号',
    `create_time` TIMESTAMP(3) NOT NULL COMMENT '创建时间',
    `ware_id` BIGINT COMMENT '仓库编号',
    `task_comment` STRING COMMENT '工作单备注',
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'paimon',
    'metastore.partitioned-table' = 'true',
    'file.format' = 'parquet',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true',
    'partition.expiration-time' = '1 d',
    'partition.expiration-check-interval' = '1 h',
    'partition.timestamp-formatter' = 'yyyy-MM-dd',
    'partition.timestamp-pattern' = '$k1'
);

INSERT INTO ods.ods_ware_order_task_full(
    `id`,
    `k1`,
    `order_id`,
    `consignee`,
    `consignee_tel`,
    `delivery_address`,
    `order_comment`,
    `payment_way`,
    `task_status`,
    `order_body`,
    `tracking_no`,
    `create_time`,
    `ware_id`,
    `task_comment`
)
SELECT
    id,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,
    `order_id`,
    `consignee`,
    `consignee_tel`,
    `delivery_address`,
    `order_comment`,
    `payment_way`,
    `task_status`,
    `order_body`,
    `tracking_no`,
    `create_time`,
    `ware_id`,
    `task_comment`
FROM default_catalog.default_database.ware_order_task_full_mq
WHERE create_time IS NOT NULL;
