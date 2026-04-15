SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

CREATE TABLE coupon_range_full_mq (
    `id` BIGINT NOT NULL COMMENT 'id',
    `coupon_id` BIGINT NOT NULL COMMENT 'coupon id',
    `range_type` STRING NOT NULL COMMENT 'range type',
    `range_id` BIGINT NOT NULL COMMENT 'range id',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'coupon_range',
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

CREATE TABLE IF NOT EXISTS ods.ods_coupon_range_full(
    `id` BIGINT NOT NULL COMMENT 'id',
    `coupon_id` BIGINT NOT NULL COMMENT 'coupon id',
    `range_type` STRING NOT NULL COMMENT 'range type',
    `range_id` BIGINT NOT NULL COMMENT 'range id',
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'file.format' = 'parquet',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true'
);

INSERT INTO ods.ods_coupon_range_full(
    `id`,
    `coupon_id`,
    `range_type`,
    `range_id`
)
select
    id,
    coupon_id,
    range_type,
    range_id
from default_catalog.default_database.coupon_range_full_mq;
