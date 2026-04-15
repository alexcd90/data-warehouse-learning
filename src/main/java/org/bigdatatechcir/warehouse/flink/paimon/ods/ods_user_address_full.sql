SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

CREATE TABLE user_address_full_mq (
    `id` BIGINT NOT NULL COMMENT '编号',
    `user_id` BIGINT COMMENT '用户id',
    `province_id` BIGINT COMMENT '省份id',
    `user_address` STRING COMMENT '用户地址',
    `consignee` STRING COMMENT '收件人',
    `phone_num` STRING COMMENT '联系方式',
    `is_default` STRING COMMENT '是否是默认',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'user_address',
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

CREATE TABLE IF NOT EXISTS ods.ods_user_address_full(
    `id` BIGINT NOT NULL COMMENT '编号',
    `user_id` BIGINT COMMENT '用户id',
    `province_id` BIGINT COMMENT '省份id',
    `user_address` STRING COMMENT '用户地址',
    `consignee` STRING COMMENT '收件人',
    `phone_num` STRING COMMENT '联系方式',
    `is_default` STRING COMMENT '是否是默认',
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'file.format' = 'parquet',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true'
);

INSERT INTO ods.ods_user_address_full(
    `id`,
    `user_id`,
    `province_id`,
    `user_address`,
    `consignee`,
    `phone_num`,
    `is_default`
)
SELECT
    `id`,
    `user_id`,
    `province_id`,
    `user_address`,
    `consignee`,
    `phone_num`,
    `is_default`
FROM default_catalog.default_database.user_address_full_mq;
