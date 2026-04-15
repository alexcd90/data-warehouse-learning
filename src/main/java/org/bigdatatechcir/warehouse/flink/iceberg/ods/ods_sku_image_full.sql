SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

CREATE TABLE sku_image_full_mq (
    `id` BIGINT NOT NULL COMMENT '编号',
    `sku_id` BIGINT COMMENT '商品id',
    `img_name` STRING COMMENT '图片名称（冗余）',
    `img_url` STRING COMMENT '图片路径(冗余)',
    `spu_img_id` BIGINT COMMENT '商品图片id',
    `is_default` STRING COMMENT '是否默认',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'sku_image',
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

CREATE TABLE IF NOT EXISTS iceberg_ods.ods_sku_image_full(
    `id` BIGINT NOT NULL COMMENT '编号',
    `sku_id` BIGINT COMMENT '商品id',
    `img_name` STRING COMMENT '图片名称（冗余）',
    `img_url` STRING COMMENT '图片路径(冗余)',
    `spu_img_id` BIGINT COMMENT '商品图片id',
    `is_default` STRING COMMENT '是否默认',
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

INSERT INTO iceberg_ods.ods_sku_image_full /*+ OPTIONS('upsert-enabled' = 'true') */(
    `id`,
    `sku_id`,
    `img_name`,
    `img_url`,
    `spu_img_id`,
    `is_default`
)
SELECT
    `id`,
    `sku_id`,
    `img_name`,
    `img_url`,
    `spu_img_id`,
    `is_default`
FROM default_catalog.default_database.sku_image_full_mq;
