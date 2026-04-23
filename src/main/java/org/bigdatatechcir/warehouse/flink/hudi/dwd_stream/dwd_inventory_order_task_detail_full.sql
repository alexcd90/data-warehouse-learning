SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'hudi_dwd_stream_dwd_inventory_order_task_detail_full';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_dwd_stream;

CREATE TABLE IF NOT EXISTS hudi_dwd_stream.dwd_inventory_order_task_detail_full(
    `id` STRING COMMENT '编号',
    `k1` STRING COMMENT '分区字段',
    `task_id` STRING COMMENT '任务ID',
    `order_id` STRING COMMENT '订单ID',
    `sku_id` STRING COMMENT '商品ID',
    `sku_name` STRING COMMENT '商品名称',
    `sku_num` BIGINT COMMENT '商品数量',
    `sku_price` DECIMAL(16,2) COMMENT '商品价格',
    `warehouse_id` STRING COMMENT '仓库ID',
    `warehouse_name` STRING COMMENT '仓库名称',
    `create_time` STRING COMMENT '创建时间',
    `source_id` STRING COMMENT '来源ID',
    `source_type` STRING COMMENT '来源类型',
    `split_total_amount` DECIMAL(16,2) COMMENT '分摊总金额',
    `split_activity_amount` DECIMAL(16,2) COMMENT '分摊活动金额',
    `split_coupon_amount` DECIMAL(16,2) COMMENT '分摊优惠券金额',
    `task_status` STRING COMMENT '任务状态',
    `tracking_no` STRING COMMENT '物流单号',
    `user_id` STRING COMMENT '用户ID',
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dwd_stream.dwd_inventory_order_task_detail_full(
    id,
    k1,
    task_id,
    order_id,
    sku_id,
    sku_name,
    sku_num,
    sku_price,
    warehouse_id,
    warehouse_name,
    create_time,
    source_id,
    source_type,
    split_total_amount,
    split_activity_amount,
    split_coupon_amount,
    task_status,
    tracking_no,
    user_id
)
SELECT
    CAST(wotd.id AS STRING) AS id,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS k1,
    CAST(wotd.task_id AS STRING) AS task_id,
    COALESCE(CAST(wot.order_id AS STRING), COALESCE(CAST(od.order_id AS STRING), '')) AS order_id,
    COALESCE(CAST(wotd.sku_id AS STRING), '') AS sku_id,
    COALESCE(wotd.sku_name, si.sku_name, '') AS sku_name,
    COALESCE(CAST(wotd.sku_num AS BIGINT), 0) AS sku_num,
    CAST(COALESCE(od.order_price, 0) AS DECIMAL(16, 2)) AS sku_price,
    COALESCE(CAST(wot.warehouse_id AS STRING), '') AS warehouse_id,
    COALESCE(wi.name, '') AS warehouse_name,
    COALESCE(od.create_time, DATE_FORMAT(wot.create_time, 'yyyy-MM-dd HH:mm:ss')) AS create_time,
    COALESCE(CAST(od.source_id AS STRING), '') AS source_id,
    COALESCE(od.source_type, '') AS source_type,
    CAST(COALESCE(od.split_total_amount, 0) AS DECIMAL(16, 2)) AS split_total_amount,
    CAST(COALESCE(od.split_activity_amount, 0) AS DECIMAL(16, 2)) AS split_activity_amount,
    CAST(COALESCE(od.split_coupon_amount, 0) AS DECIMAL(16, 2)) AS split_coupon_amount,
    COALESCE(wot.task_status, '') AS task_status,
    COALESCE(wot.tracking_no, '') AS tracking_no,
    COALESCE(CAST(od.user_id AS STRING), '') AS user_id
FROM hudi_ods.ods_ware_order_task_detail_full wotd
LEFT JOIN
(
    SELECT
        id,
        order_id,
        ware_id AS warehouse_id,
        task_status,
        tracking_no,
        create_time
    FROM hudi_ods.ods_ware_order_task_full
    WHERE k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
) wot
    ON wotd.task_id = wot.id
LEFT JOIN
(
    SELECT
        id,
        sku_name
    FROM hudi_ods.ods_sku_info_full
    WHERE k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
) si
    ON wotd.sku_id = si.id
LEFT JOIN hudi_ods.ods_ware_info_full wi
    ON wot.warehouse_id = wi.id
LEFT JOIN
(
    SELECT
        od.order_id,
        od.sku_id,
        DATE_FORMAT(od.create_time, 'yyyy-MM-dd HH:mm:ss') AS create_time,
        od.source_id,
        od.source_type,
        od.order_price,
        od.split_total_amount,
        od.split_activity_amount,
        od.split_coupon_amount,
        oi.user_id
    FROM hudi_ods.ods_order_detail_full od
    LEFT JOIN hudi_ods.ods_order_info_full oi
        ON od.order_id = oi.id
       AND oi.k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
    WHERE od.k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
) od
    ON wot.order_id = od.order_id
   AND wotd.sku_id = od.sku_id
WHERE wotd.id IS NOT NULL;

