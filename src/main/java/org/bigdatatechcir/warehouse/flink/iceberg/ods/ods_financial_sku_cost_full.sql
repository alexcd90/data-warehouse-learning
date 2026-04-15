SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

CREATE TABLE financial_sku_cost_full_mq (
    `id` STRING NOT NULL COMMENT 'id',
    `sku_id` BIGINT COMMENT 'sku id',
    `sku_name` STRING COMMENT 'sku name',
    `busi_date` STRING COMMENT 'business date',
    `is_lastest` STRING COMMENT 'is latest',
    `sku_cost` DECIMAL(16, 2) COMMENT 'sku cost',
    `create_time` TIMESTAMP(3) NOT NULL COMMENT 'create time',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'financial_sku_cost',
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

CREATE TABLE IF NOT EXISTS iceberg_ods.ods_financial_sku_cost_full(
    `id` STRING NOT NULL COMMENT 'id',
    `k1` STRING COMMENT 'partition field',
    `sku_id` BIGINT COMMENT 'sku id',
    `sku_name` STRING COMMENT 'sku name',
    `busi_date` STRING COMMENT 'business date',
    `is_lastest` STRING COMMENT 'is latest',
    `sku_cost` DECIMAL(16, 2) COMMENT 'sku cost',
    `create_time` TIMESTAMP(3) NOT NULL COMMENT 'create time',
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

INSERT INTO iceberg_ods.ods_financial_sku_cost_full /*+ OPTIONS('upsert-enabled' = 'true') */(
    `id`,
    `k1`,
    `sku_id`,
    `sku_name`,
    `busi_date`,
    `is_lastest`,
    `sku_cost`,
    `create_time`
)
select
    id,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,
    sku_id,
    sku_name,
    busi_date,
    is_lastest,
    sku_cost,
    create_time
from default_catalog.default_database.financial_sku_cost_full_mq
where create_time is not null;
