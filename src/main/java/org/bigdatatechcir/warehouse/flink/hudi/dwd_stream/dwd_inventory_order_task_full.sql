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
SET 'pipeline.name' = 'hudi_dwd_stream_dwd_inventory_order_task_full';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_dwd_stream;

CREATE TABLE IF NOT EXISTS hudi_dwd_stream.dwd_inventory_order_task_full(
    `id` STRING COMMENT '编号',
    `k1` STRING COMMENT '分区字段',
    `task_id` STRING COMMENT '任务ID',
    `order_id` STRING COMMENT '订单ID',
    `consignee` STRING COMMENT '收货人',
    `consignee_tel` STRING COMMENT '收货人电话',
    `delivery_address` STRING COMMENT '配送地址',
    `order_comment` STRING COMMENT '订单备注',
    `payment_way` STRING COMMENT '付款方式',
    `task_status` STRING COMMENT '任务状态',
    `order_body` STRING COMMENT '订单描述',
    `tracking_no` STRING COMMENT '物流单号',
    `warehouse_id` STRING COMMENT '仓库ID',
    `warehouse_name` STRING COMMENT '仓库名称',
    `create_time` STRING COMMENT '创建时间',
    `consign_time` STRING COMMENT '发货时间',
    `finish_time` STRING COMMENT '完成时间',
    `cancel_time` STRING COMMENT '取消时间',
    `cancel_reason` STRING COMMENT '取消原因',
    `sku_num` BIGINT COMMENT '商品数量',
    `sku_ids` STRING COMMENT '商品ID列表',
    `province_id` STRING COMMENT '省份ID',
    `city_id` STRING COMMENT '城市ID',
    `region_id` STRING COMMENT '区域ID',
    `user_id` STRING COMMENT '用户ID',
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dwd_stream.dwd_inventory_order_task_full(
    id,
    k1,
    task_id,
    order_id,
    consignee,
    consignee_tel,
    delivery_address,
    order_comment,
    payment_way,
    task_status,
    order_body,
    tracking_no,
    warehouse_id,
    warehouse_name,
    create_time,
    consign_time,
    finish_time,
    cancel_time,
    cancel_reason,
    sku_num,
    sku_ids,
    province_id,
    city_id,
    region_id,
    user_id
)
SELECT
    CAST(wot.id AS STRING) AS id,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS k1,
    CAST(wot.id AS STRING) AS task_id,
    COALESCE(CAST(wot.order_id AS STRING), '') AS order_id,
    COALESCE(wot.consignee, '') AS consignee,
    COALESCE(wot.consignee_tel, '') AS consignee_tel,
    COALESCE(wot.delivery_address, '') AS delivery_address,
    COALESCE(wot.order_comment, '') AS order_comment,
    COALESCE(wot.payment_way, '') AS payment_way,
    COALESCE(wot.task_status, '') AS task_status,
    COALESCE(wot.order_body, '') AS order_body,
    COALESCE(wot.tracking_no, '') AS tracking_no,
    COALESCE(CAST(wot.ware_id AS STRING), '') AS warehouse_id,
    COALESCE(wi.name, '') AS warehouse_name,
    DATE_FORMAT(wot.create_time, 'yyyy-MM-dd HH:mm:ss') AS create_time,
    CAST(NULL AS STRING) AS consign_time,
    CAST(NULL AS STRING) AS finish_time,
    CAST(NULL AS STRING) AS cancel_time,
    CAST(NULL AS STRING) AS cancel_reason,
    COALESCE(wtd.sku_num, 0) AS sku_num,
    COALESCE(wtd.sku_ids, '') AS sku_ids,
    COALESCE(CAST(oi.province_id AS STRING), '') AS province_id,
    CAST(NULL AS STRING) AS city_id,
    CAST(NULL AS STRING) AS region_id,
    COALESCE(CAST(oi.user_id AS STRING), '') AS user_id
FROM
(
    SELECT
        id,
        order_id,
        consignee,
        consignee_tel,
        delivery_address,
        order_comment,
        payment_way,
        task_status,
        order_body,
        tracking_no,
        ware_id,
        create_time
    FROM hudi_ods.ods_ware_order_task_full
    WHERE k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
) wot
LEFT JOIN hudi_ods.ods_ware_info_full wi
    ON wot.ware_id = wi.id
LEFT JOIN
(
    SELECT
        task_id,
        SUM(CAST(sku_num AS BIGINT)) AS sku_num,
        LISTAGG(CAST(sku_id AS STRING), ',') AS sku_ids
    FROM hudi_ods.ods_ware_order_task_detail_full
    GROUP BY task_id
) wtd
    ON wot.id = wtd.task_id
LEFT JOIN
(
    SELECT
        id,
        user_id,
        province_id
    FROM hudi_ods.ods_order_info_full
    WHERE k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
) oi
    ON wot.order_id = oi.id
WHERE wot.id IS NOT NULL;

