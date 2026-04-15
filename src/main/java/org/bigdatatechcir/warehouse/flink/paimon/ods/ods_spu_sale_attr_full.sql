SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

CREATE TABLE spu_sale_attr_full_mq (
    `id` BIGINT NOT NULL COMMENT '编号(业务中无关联)',
    `spu_id` BIGINT COMMENT '商品id',
    `base_sale_attr_id` BIGINT COMMENT '销售属性id',
    `sale_attr_name` STRING COMMENT '销售属性名称(冗余)',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'spu_sale_attr',
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

CREATE TABLE IF NOT EXISTS ods.ods_spu_sale_attr_full(
    `id` BIGINT NOT NULL COMMENT '编号(业务中无关联)',
    `spu_id` BIGINT COMMENT '商品id',
    `base_sale_attr_id` BIGINT COMMENT '销售属性id',
    `sale_attr_name` STRING COMMENT '销售属性名称(冗余)',
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'file.format' = 'parquet',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true'
);

INSERT INTO ods.ods_spu_sale_attr_full(
    `id`,
    `spu_id`,
    `base_sale_attr_id`,
    `sale_attr_name`
)
SELECT
    `id`,
    `spu_id`,
    `base_sale_attr_id`,
    `sale_attr_name`
FROM default_catalog.default_database.spu_sale_attr_full_mq;
