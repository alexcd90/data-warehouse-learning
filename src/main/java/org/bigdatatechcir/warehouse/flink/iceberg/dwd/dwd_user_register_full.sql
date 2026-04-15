SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG iceberg_catalog;
CREATE DATABASE IF NOT EXISTS iceberg_dwd;

CREATE TABLE IF NOT EXISTS iceberg_dwd.dwd_user_register_full(
    `id` STRING COMMENT '编号',
    `k1` STRING COMMENT '分区字段',
    `user_id` STRING COMMENT '用户ID',
    `date_id` STRING COMMENT '日期ID',
    `create_time` STRING COMMENT '注册时间',
    `channel` STRING COMMENT '应用下载渠道',
    `province_id` STRING COMMENT '省份ID',
    `version_code` STRING COMMENT '应用版本',
    `mid_id` STRING COMMENT '设备ID',
    `brand` STRING COMMENT '设备品牌',
    `model` STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '操作系统',
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

INSERT INTO iceberg_dwd.dwd_user_register_full /*+ OPTIONS('upsert-enabled' = 'true') */(
    id,
    k1,
    user_id,
    date_id,
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
)
WITH register_log AS (
    SELECT
        user_id,
        area_code,
        brand,
        channel,
        model,
        mid_id,
        operate_system,
        version_code
    FROM (
        SELECT
            common_uid AS user_id,
            common_ar AS area_code,
            common_ba AS brand,
            common_ch AS channel,
            common_md AS model,
            common_mid AS mid_id,
            common_os AS operate_system,
            common_vc AS version_code,
            ROW_NUMBER() OVER (PARTITION BY common_uid ORDER BY ts DESC, id DESC) AS rn
        FROM iceberg_ods.ods_log_inc
        WHERE page_page_id = 'register'
          AND common_uid IS NOT NULL
          AND k1 = '${pdate}'
    ) t
    WHERE rn = 1
)
SELECT
    CAST(ui.user_id AS STRING) AS id,
    ui.k1,
    CAST(ui.user_id AS STRING) AS user_id,
    ui.k1 AS date_id,
    DATE_FORMAT(ui.create_time, 'yyyy-MM-dd HH:mm:ss') AS create_time,
    COALESCE(log.channel, '') AS channel,
    COALESCE(CAST(bp.id AS STRING), '') AS province_id,
    COALESCE(log.version_code, '') AS version_code,
    COALESCE(log.mid_id, '') AS mid_id,
    COALESCE(log.brand, '') AS brand,
    COALESCE(log.model, '') AS model,
    COALESCE(log.operate_system, '') AS operate_system
FROM
(
    SELECT
        id AS user_id,
        k1,
        create_time
    FROM iceberg_ods.ods_user_info_full
    WHERE k1 = '${pdate}'
) ui
LEFT JOIN register_log log
    ON CAST(ui.user_id AS STRING) = log.user_id
LEFT JOIN iceberg_ods.ods_base_province_full bp
    ON log.area_code = bp.area_code;
