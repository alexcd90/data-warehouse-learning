SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

CREATE TABLE ware_order_task_detail_full_mq (
    `id` BIGINT NOT NULL COMMENT '编号',
    `sku_id` BIGINT COMMENT 'sku_id',
    `sku_name` STRING COMMENT 'sku名称',
    `sku_num` INT COMMENT '购买个数',
    `task_id` BIGINT COMMENT '工作单编号',
    `refund_status` STRING COMMENT 'refund_status',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'ware_order_task_detail',
    'server-time-zone' = 'Asia/Shanghai'
);

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_ods;

CREATE TABLE IF NOT EXISTS hudi_ods.ods_ware_order_task_detail_full(
    `id` BIGINT NOT NULL COMMENT '编号',
    `sku_id` BIGINT COMMENT 'sku_id',
    `sku_name` STRING COMMENT 'sku名称',
    `sku_num` INT COMMENT '购买个数',
    `task_id` BIGINT COMMENT '工作单编号',
    `refund_status` STRING COMMENT 'refund_status',
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_ods.ods_ware_order_task_detail_full(
    `id`,
    `sku_id`,
    `sku_name`,
    `sku_num`,
    `task_id`,
    `refund_status`
)
SELECT
    `id`,
    `sku_id`,
    `sku_name`,
    `sku_num`,
    `task_id`,
    `refund_status`
FROM default_catalog.default_database.ware_order_task_detail_full_mq;
