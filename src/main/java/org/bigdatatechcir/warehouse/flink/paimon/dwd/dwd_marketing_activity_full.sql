SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG paimon_hive;
CREATE DATABASE IF NOT EXISTS dwd;

CREATE TABLE IF NOT EXISTS dwd.dwd_marketing_activity_full(
    `id` STRING COMMENT '编号',
    `k1` STRING COMMENT '分区字段',
    `activity_id` STRING COMMENT '活动ID',
    `activity_name` STRING COMMENT '活动名称',
    `activity_type` STRING COMMENT '活动类型',
    `activity_desc` STRING COMMENT '活动描述',
    `start_time` STRING COMMENT '开始时间',
    `end_time` STRING COMMENT '结束时间',
    `create_time` STRING COMMENT '创建时间',
    `rules` STRING COMMENT '活动规则集合，JSON格式',
    `sku_ids` STRING COMMENT '参与活动的商品ID集合',
    `status` STRING COMMENT '活动状态',
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

CREATE TEMPORARY VIEW tmp_dwd_marketing_activity_full_src AS
WITH act_rule AS (
    SELECT
        activity_id,
        LISTAGG(
            CONCAT(
                COALESCE(CAST(condition_amount AS STRING), ''),
                ':',
                COALESCE(CAST(benefit_amount AS STRING), ''),
                ':',
                COALESCE(CAST(benefit_discount AS STRING), '')
            ),
            ';'
        ) AS rules
    FROM ods.ods_activity_rule_full
    GROUP BY activity_id
),
act_sku AS (
    SELECT
        activity_id,
        LISTAGG(CAST(sku_id AS STRING), ',') AS sku_ids
    FROM (
        SELECT DISTINCT
            activity_id,
            sku_id
        FROM ods.ods_activity_sku_full
        WHERE k1 = '${pdate}'
    ) t
    GROUP BY activity_id
)
SELECT
    CAST(ai.id AS STRING) AS id,
    ai.k1,
    CAST(ai.id AS STRING) AS activity_id,
    COALESCE(ai.activity_name, '') AS activity_name,
    COALESCE(ai.activity_type, '') AS activity_type,
    COALESCE(ai.activity_desc, '') AS activity_desc,
    COALESCE(ai.start_time, '') AS start_time,
    COALESCE(ai.end_time, '') AS end_time,
    COALESCE(ai.create_time, '') AS create_time,
    COALESCE(ar.rules, '') AS rules,
    COALESCE(sku.sku_ids, '') AS sku_ids,
    CASE
        WHEN DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') < ai.start_time THEN '未开始'
        WHEN DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') > ai.end_time THEN '已结束'
        ELSE '进行中'
    END AS status
FROM ods.ods_activity_info_full ai
LEFT JOIN act_rule ar
    ON ai.id = ar.activity_id
LEFT JOIN act_sku sku
    ON ai.id = sku.activity_id
WHERE ai.k1 = '${pdate}';

INSERT INTO dwd.dwd_marketing_activity_full(
    id,
    k1,
    activity_id,
    activity_name,
    activity_type,
    activity_desc,
    start_time,
    end_time,
    create_time,
    rules,
    sku_ids,
    status
)
SELECT * FROM tmp_dwd_marketing_activity_full_src;
