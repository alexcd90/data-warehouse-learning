SET 'execution.checkpointing.interval' = '30s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5s';
SET 'table.exec.mini-batch.size' = '5000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'table.dynamic-table-options.enabled' = 'true';
SET 'pipeline.name' = 'hudi_dwd_stream_dwd_user_address_full';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_dwd_stream;

CREATE TABLE IF NOT EXISTS hudi_dwd_stream.dwd_user_address_full(
    `id` STRING COMMENT '编号',
    `k1` STRING COMMENT 'stream partition marker',
    `user_id` STRING COMMENT '用户ID',
    `province_id` STRING COMMENT '省份ID',
    `province_name` STRING COMMENT '省份名称',
    `city_id` STRING COMMENT '城市ID',
    `city_name` STRING COMMENT '城市名称',
    `district_id` STRING COMMENT '区县ID',
    `district_name` STRING COMMENT '区县名称',
    `detail_address` STRING COMMENT '详细地址',
    `consignee` STRING COMMENT '收货人',
    `phone_num` STRING COMMENT '电话号码',
    `is_default` STRING COMMENT '是否默认地址',
    `create_time` STRING COMMENT 'stream materialized time',
    `operate_time` STRING COMMENT 'stream materialized time',
    `postal_code` STRING COMMENT '邮政编码',
    `full_address` STRING COMMENT '完整地址',
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dwd_stream.dwd_user_address_full(
    id,
    k1,
    user_id,
    province_id,
    province_name,
    city_id,
    city_name,
    district_id,
    district_name,
    detail_address,
    consignee,
    phone_num,
    is_default,
    create_time,
    operate_time,
    postal_code,
    full_address
)
SELECT
    CAST(ua.id AS STRING) AS id,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS k1,
    COALESCE(CAST(ua.user_id AS STRING), '') AS user_id,
    COALESCE(CAST(ua.province_id AS STRING), '') AS province_id,
    COALESCE(bp.name, '') AS province_name,
    COALESCE(CAST(ua.province_id AS STRING), '') AS city_id,
    COALESCE(bp.area_code, '') AS city_name,
    COALESCE(CAST(ua.province_id AS STRING), '') AS district_id,
    COALESCE(bp.iso_code, '') AS district_name,
    COALESCE(ua.user_address, '') AS detail_address,
    COALESCE(ua.consignee, '') AS consignee,
    COALESCE(ua.phone_num, '') AS phone_num,
    COALESCE(ua.is_default, '') AS is_default,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') AS create_time,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') AS operate_time,
    CAST(NULL AS STRING) AS postal_code,
    CONCAT_WS(' ', bp.name, bp.area_code, bp.iso_code, ua.user_address) AS full_address
FROM hudi_ods.ods_user_address_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ ua
LEFT JOIN hudi_ods.ods_base_province_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ bp
    ON ua.province_id = bp.id;
