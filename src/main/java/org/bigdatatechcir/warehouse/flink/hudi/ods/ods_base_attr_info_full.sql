SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

CREATE TABLE base_attr_info_full_mq (
    `id` BIGINT NOT NULL COMMENT '编号',
    `attr_name` STRING NOT NULL COMMENT '属性名称',
    `category_id` BIGINT COMMENT '分类id',
    `category_level` INT COMMENT '分类层级',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'base_attr_info',
    'server-time-zone' = 'Asia/Shanghai'
);

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_ods;

CREATE TABLE IF NOT EXISTS hudi_ods.ods_base_attr_info_full(
    `id` BIGINT NOT NULL COMMENT '编号',
    `attr_name` STRING NOT NULL COMMENT '属性名称',
    `category_id` BIGINT COMMENT '分类id',
    `category_level` INT COMMENT '分类层级',
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_ods.ods_base_attr_info_full(
    `id`,
    `attr_name`,
    `category_id`,
    `category_level`
)
SELECT
    `id`,
    `attr_name`,
    `category_id`,
    `category_level`
FROM default_catalog.default_database.base_attr_info_full_mq;
