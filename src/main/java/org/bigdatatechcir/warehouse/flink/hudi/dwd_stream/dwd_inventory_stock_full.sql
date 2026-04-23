SET 'execution.checkpointing.interval' = '30s';
SET 'execution.runtime-mode' = 'streaming';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5s';
SET 'table.exec.mini-batch.size' = '5000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'table.dynamic-table-options.enabled' = 'true';
SET 'pipeline.name' = 'hudi_dwd_stream_dwd_inventory_stock_full';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_dwd_stream;

CREATE TABLE IF NOT EXISTS hudi_dwd_stream.dwd_inventory_stock_full(
    `id` STRING COMMENT '编号',
    `k1` STRING COMMENT 'stream partition marker',
    `sku_id` STRING COMMENT '商品ID',
    `sku_name` STRING COMMENT '商品名称',
    `warehouse_id` STRING COMMENT '仓库ID',
    `warehouse_name` STRING COMMENT '仓库名称',
    `warehouse_address` STRING COMMENT '仓库地址',
    `warehouse_areacode` STRING COMMENT '仓库区域编码',
    `stock` BIGINT COMMENT '库存数量',
    `stock_name` STRING COMMENT '库存名称',
    `is_default` STRING COMMENT '是否默认仓库',
    `create_time` STRING COMMENT 'stream materialized time',
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dwd_stream.dwd_inventory_stock_full(
    id,
    k1,
    sku_id,
    sku_name,
    warehouse_id,
    warehouse_name,
    warehouse_address,
    warehouse_areacode,
    stock,
    stock_name,
    is_default,
    create_time
)
SELECT
    CAST(ws.id AS STRING) AS id,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS k1,
    COALESCE(CAST(ws.sku_id AS STRING), '') AS sku_id,
    COALESCE(si.sku_name, '') AS sku_name,
    COALESCE(CAST(ws.warehouse_id AS STRING), '') AS warehouse_id,
    COALESCE(wi.name, '') AS warehouse_name,
    COALESCE(wi.address, '') AS warehouse_address,
    COALESCE(wi.areacode, '') AS warehouse_areacode,
    COALESCE(CAST(ws.stock AS BIGINT), 0) AS stock,
    COALESCE(
        ws.stock_name,
        CASE
            WHEN ws.stock = 0 THEN '无货'
            WHEN ws.stock < 10 THEN '低库存'
            WHEN ws.stock < 50 THEN '库存正常'
            ELSE '库存充足'
        END
    ) AS stock_name,
    '0' AS is_default,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') AS create_time
FROM hudi_ods.ods_ware_sku_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ ws
LEFT JOIN hudi_ods.ods_ware_info_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ wi
    ON ws.warehouse_id = wi.id
LEFT JOIN hudi_ods.ods_sku_info_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ si
    ON ws.sku_id = si.id;
