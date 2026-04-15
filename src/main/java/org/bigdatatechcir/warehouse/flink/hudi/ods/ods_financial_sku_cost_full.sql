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

create catalog hudi_catalog with(
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

use CATALOG hudi_catalog;

create DATABASE IF NOT EXISTS hudi_ods;

CREATE TABLE IF NOT EXISTS hudi_ods.ods_financial_sku_cost_full(
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
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_ods.ods_financial_sku_cost_full(
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
