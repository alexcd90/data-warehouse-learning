SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'hudi_dwd_stream_dwd_marketing_seckill_full';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_dwd_stream;

CREATE TABLE IF NOT EXISTS hudi_dwd_stream.dwd_marketing_seckill_full(
    `id` STRING COMMENT '编号',
    `k1` STRING COMMENT '分区字段',
    `seckill_id` STRING COMMENT '秒杀活动ID',
    `spu_id` STRING COMMENT 'SPU ID',
    `sku_id` STRING COMMENT 'SKU ID',
    `sku_name` STRING COMMENT '商品名称',
    `sku_default_img` STRING COMMENT '商品图片',
    `original_price` DECIMAL(16, 2) COMMENT '原价',
    `seckill_price` DECIMAL(16, 2) COMMENT '秒杀价',
    `create_time` STRING COMMENT '创建时间',
    `check_time` STRING COMMENT '审核时间',
    `status` STRING COMMENT '状态',
    `start_time` STRING COMMENT '开始时间',
    `end_time` STRING COMMENT '结束时间',
    `total_num` BIGINT COMMENT '总数量',
    `stock_count` BIGINT COMMENT '剩余库存',
    `sku_desc` STRING COMMENT '商品描述',
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dwd_stream.dwd_marketing_seckill_full(
    id,
    k1,
    seckill_id,
    spu_id,
    sku_id,
    sku_name,
    sku_default_img,
    original_price,
    seckill_price,
    create_time,
    check_time,
    status,
    start_time,
    end_time,
    total_num,
    stock_count,
    sku_desc
)
SELECT
    CAST(sg.id AS STRING) AS id,
    sg.k1,
    CAST(sg.id AS STRING) AS seckill_id,
    COALESCE(CAST(sg.spu_id AS STRING), '') AS spu_id,
    COALESCE(CAST(sg.sku_id AS STRING), '') AS sku_id,
    COALESCE(sg.sku_name, si.sku_name, '') AS sku_name,
    COALESCE(sg.sku_default_img, si.sku_default_img, '') AS sku_default_img,
    CAST(COALESCE(sg.price, 0) AS DECIMAL(16, 2)) AS original_price,
    CAST(COALESCE(sg.cost_price, 0) AS DECIMAL(16, 2)) AS seckill_price,
    DATE_FORMAT(sg.create_time, 'yyyy-MM-dd HH:mm:ss') AS create_time,
    DATE_FORMAT(sg.check_time, 'yyyy-MM-dd HH:mm:ss') AS check_time,
    COALESCE(sg.status, '') AS status,
    DATE_FORMAT(sg.start_time, 'yyyy-MM-dd HH:mm:ss') AS start_time,
    DATE_FORMAT(sg.end_time, 'yyyy-MM-dd HH:mm:ss') AS end_time,
    COALESCE(CAST(sg.num AS BIGINT), 0) AS total_num,
    COALESCE(CAST(sg.stock_count AS BIGINT), 0) AS stock_count,
    COALESCE(sg.sku_desc, '') AS sku_desc
FROM hudi_ods.ods_seckill_goods_full sg
LEFT JOIN
(
    SELECT
        id,
        sku_name,
        sku_default_img
    FROM hudi_ods.ods_sku_info_full
    WHERE k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
) si
    ON sg.sku_id = si.id
WHERE sg.k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd');

