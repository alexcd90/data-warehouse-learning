SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

CREATE TABLE spu_sale_attr_value_full_mq (
    `id` BIGINT NOT NULL COMMENT '销售属性值编号',
    `spu_id` BIGINT COMMENT '商品id',
    `base_sale_attr_id` BIGINT COMMENT '销售属性id',
    `sale_attr_value_name` STRING COMMENT '销售属性值名称',
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
    'table-name' = 'spu_sale_attr_value',
    'server-time-zone' = 'Asia/Shanghai'
);

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG iceberg_catalog;
CREATE DATABASE IF NOT EXISTS iceberg_ods;

CREATE TABLE IF NOT EXISTS iceberg_ods.ods_spu_sale_attr_value_full(
    `id` BIGINT NOT NULL COMMENT '销售属性值编号',
    `spu_id` BIGINT COMMENT '商品id',
    `base_sale_attr_id` BIGINT COMMENT '销售属性id',
    `sale_attr_value_name` STRING COMMENT '销售属性值名称',
    `sale_attr_name` STRING COMMENT '销售属性名称(冗余)',
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

INSERT INTO iceberg_ods.ods_spu_sale_attr_value_full /*+ OPTIONS('upsert-enabled' = 'true') */(
    `id`,
    `spu_id`,
    `base_sale_attr_id`,
    `sale_attr_value_name`,
    `sale_attr_name`
)
SELECT
    `id`,
    `spu_id`,
    `base_sale_attr_id`,
    `sale_attr_value_name`,
    `sale_attr_name`
FROM default_catalog.default_database.spu_sale_attr_value_full_mq;
