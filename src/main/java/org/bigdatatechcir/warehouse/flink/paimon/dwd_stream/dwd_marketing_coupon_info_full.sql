SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'paimon_dwd_stream_dwd_marketing_coupon_info_full';

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG paimon_hive;
CREATE DATABASE IF NOT EXISTS dwd_stream;

CREATE TABLE IF NOT EXISTS dwd_stream.dwd_marketing_coupon_info_full(
    `id` STRING COMMENT '编号',
    `k1` STRING COMMENT '分区字段',
    `coupon_id` STRING COMMENT '优惠券ID',
    `coupon_name` STRING COMMENT '优惠券名称',
    `coupon_type` STRING COMMENT '优惠券类型',
    `condition_amount` DECIMAL(16, 2) COMMENT '满减金额',
    `benefit_amount` DECIMAL(16, 2) COMMENT '优惠金额',
    `start_time` STRING COMMENT '可用开始时间',
    `end_time` STRING COMMENT '可用结束时间',
    `create_time` STRING COMMENT '创建时间',
    `range_type` STRING COMMENT '范围类型：1-商品，2-品类，3-品牌',
    `range_ids` STRING COMMENT '范围ID集合',
    `range_names` STRING COMMENT '范围名称集合',
    `limit_num` INT COMMENT '每人限领张数',
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

CREATE TEMPORARY VIEW tmp_dwd_marketing_coupon_info_full_src AS
WITH coupon_range AS (
    SELECT
        coupon_id,
        LISTAGG(CAST(range_id AS STRING), ';') AS range_ids,
        LISTAGG(CAST(range_id AS STRING), ';') AS range_names
    FROM (
        SELECT DISTINCT
            coupon_id,
            range_id
        FROM ods.ods_coupon_range_full
    ) t
    GROUP BY coupon_id
)
SELECT
    CAST(ci.id AS STRING) AS id,
    ci.k1,
    CAST(ci.id AS STRING) AS coupon_id,
    COALESCE(ci.coupon_name, '') AS coupon_name,
    COALESCE(ci.coupon_type, '') AS coupon_type,
    CAST(COALESCE(ci.condition_amount, 0) AS DECIMAL(16, 2)) AS condition_amount,
    CAST(COALESCE(ci.benefit_amount, 0) AS DECIMAL(16, 2)) AS benefit_amount,
    DATE_FORMAT(ci.start_time, 'yyyy-MM-dd HH:mm:ss') AS start_time,
    DATE_FORMAT(ci.end_time, 'yyyy-MM-dd HH:mm:ss') AS end_time,
    DATE_FORMAT(ci.create_time, 'yyyy-MM-dd HH:mm:ss') AS create_time,
    COALESCE(ci.range_type, '') AS range_type,
    COALESCE(cr.range_ids, '') AS range_ids,
    COALESCE(cr.range_names, '') AS range_names,
    ci.limit_num
FROM ods.ods_coupon_info_full ci
LEFT JOIN coupon_range cr
    ON ci.id = cr.coupon_id
WHERE ci.k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd');

INSERT INTO dwd_stream.dwd_marketing_coupon_info_full(
    id,
    k1,
    coupon_id,
    coupon_name,
    coupon_type,
    condition_amount,
    benefit_amount,
    start_time,
    end_time,
    create_time,
    range_type,
    range_ids,
    range_names,
    limit_num
)
SELECT * FROM tmp_dwd_marketing_coupon_info_full_src;


