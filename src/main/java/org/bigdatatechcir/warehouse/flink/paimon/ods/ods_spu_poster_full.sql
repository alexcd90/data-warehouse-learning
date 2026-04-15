SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';

CREATE TABLE spu_poster_full_mq (
    `id` BIGINT NOT NULL COMMENT '编号',
    `spu_id` BIGINT COMMENT '商品id',
    `img_name` STRING COMMENT '文件名称',
    `img_url` STRING COMMENT '文件路径',
    `create_time` TIMESTAMP(3) NOT NULL COMMENT '创建时间',
    `update_time` TIMESTAMP(3) NOT NULL COMMENT '更新时间',
    `is_deleted` INT NOT NULL COMMENT '逻辑删除 1（true）已删除， 0（false）未删除',
    PRIMARY KEY(`id`) NOT ENFORCED
) WITH (
    'connector' = 'mysql-cdc',
    'scan.startup.mode' = 'earliest-offset',
    'hostname' = '192.168.244.129',
    'port' = '3306',
    'username' = 'root',
    'password' = '',
    'database-name' = 'gmall',
    'table-name' = 'spu_poster',
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

CREATE TABLE IF NOT EXISTS ods.ods_spu_poster_full(
    `id` BIGINT NOT NULL COMMENT '编号',
    `k1` STRING COMMENT 'partition field',
    `spu_id` BIGINT COMMENT '商品id',
    `img_name` STRING COMMENT '文件名称',
    `img_url` STRING COMMENT '文件路径',
    `create_time` TIMESTAMP(3) NOT NULL COMMENT '创建时间',
    `update_time` TIMESTAMP(3) NOT NULL COMMENT '更新时间',
    `is_deleted` INT NOT NULL COMMENT '逻辑删除 1（true）已删除， 0（false）未删除',
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'paimon',
    'metastore.partitioned-table' = 'true',
    'file.format' = 'parquet',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true',
    'partition.expiration-time' = '1 d',
    'partition.expiration-check-interval' = '1 h',
    'partition.timestamp-formatter' = 'yyyy-MM-dd',
    'partition.timestamp-pattern' = '$k1'
);

INSERT INTO ods.ods_spu_poster_full(
    `id`,
    `k1`,
    `spu_id`,
    `img_name`,
    `img_url`,
    `create_time`,
    `update_time`,
    `is_deleted`
)
SELECT
    id,
    DATE_FORMAT(create_time, 'yyyy-MM-dd') AS k1,
    `spu_id`,
    `img_name`,
    `img_url`,
    `create_time`,
    `update_time`,
    `is_deleted`
FROM default_catalog.default_database.spu_poster_full_mq
WHERE create_time IS NOT NULL;
