SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

CREATE TABLE cms_banner_full_mq (
    `id` BIGINT NOT NULL COMMENT 'ID',
    `title` STRING COMMENT '标题',
    `image_url` STRING NOT NULL COMMENT '图片地址',
    `link_url` STRING COMMENT '链接地址',
    `sort` INT NOT NULL COMMENT '排序',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'cms_banner',
    'server-time-zone' = 'Asia/Shanghai'
);

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_ods;

CREATE TABLE IF NOT EXISTS hudi_ods.ods_cms_banner_full(
    `id` BIGINT NOT NULL COMMENT 'ID',
    `title` STRING COMMENT '标题',
    `image_url` STRING NOT NULL COMMENT '图片地址',
    `link_url` STRING COMMENT '链接地址',
    `sort` INT NOT NULL COMMENT '排序',
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_ods.ods_cms_banner_full(
    `id`,
    `title`,
    `image_url`,
    `link_url`,
    `sort`
)
SELECT
    `id`,
    `title`,
    `image_url`,
    `link_url`,
    `sort`
FROM default_catalog.default_database.cms_banner_full_mq;
