SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

CREATE TABLE ware_sku_full_mq (
    `id` BIGINT NOT NULL COMMENT '编号',
    `sku_id` BIGINT COMMENT 'skuid',
    `warehouse_id` BIGINT COMMENT '仓库id',
    `stock` INT COMMENT '库存数',
    `stock_name` STRING COMMENT '存货名称',
    `stock_locked` INT COMMENT '锁定库存数',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'ware_sku',
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

CREATE TABLE IF NOT EXISTS iceberg_ods.ods_ware_sku_full(
    `id` BIGINT NOT NULL COMMENT '编号',
    `sku_id` BIGINT COMMENT 'skuid',
    `warehouse_id` BIGINT COMMENT '仓库id',
    `stock` INT COMMENT '库存数',
    `stock_name` STRING COMMENT '存货名称',
    `stock_locked` INT COMMENT '锁定库存数',
    PRIMARY KEY (`id`) NOT ENFORCED
) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

INSERT INTO iceberg_ods.ods_ware_sku_full /*+ OPTIONS('upsert-enabled' = 'true') */(
    `id`,
    `sku_id`,
    `warehouse_id`,
    `stock`,
    `stock_name`,
    `stock_locked`
)
SELECT
    `id`,
    `sku_id`,
    `warehouse_id`,
    `stock`,
    `stock_name`,
    `stock_locked`
FROM default_catalog.default_database.ware_sku_full_mq;
