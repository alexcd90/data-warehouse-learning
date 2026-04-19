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

CREATE TABLE IF NOT EXISTS iceberg_dwd.dwd_product_category_full(
    `id` STRING COMMENT '编号',
    `k1` STRING COMMENT '分区字段',
    `category_id` STRING COMMENT '分类ID',
    `category_name` STRING COMMENT '分类名称',
    `category_level` INT COMMENT '分类级别：1-一级分类，2-二级分类，3-三级分类',
    `parent_id` STRING COMMENT '父级分类ID',
    `parent_name` STRING COMMENT '父级分类名称',
    `category_path` STRING COMMENT '分类路径，如1-2-3',
    `category_path_name` STRING COMMENT '分类路径名称，如手机-华为手机-P40系列',
    `create_time` STRING COMMENT '创建时间',
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

INSERT INTO iceberg_dwd.dwd_product_category_full /*+ OPTIONS('upsert-enabled' = 'true') */(
    id,
    k1,
    category_id,
    category_name,
    category_level,
    parent_id,
    parent_name,
    category_path,
    category_path_name,
    create_time
)
SELECT
    CAST(id AS STRING) AS id,
    '${pdate}' AS k1,
    CAST(id AS STRING) AS category_id,
    name AS category_name,
    1 AS category_level,
    CAST(NULL AS STRING) AS parent_id,
    CAST(NULL AS STRING) AS parent_name,
    CAST(id AS STRING) AS category_path,
    name AS category_path_name,
    CONCAT('${pdate}', ' 00:00:00') AS create_time
FROM iceberg_ods.ods_base_category1_full
UNION ALL
SELECT
    CAST(c2.id AS STRING) AS id,
    '${pdate}' AS k1,
    CAST(c2.id AS STRING) AS category_id,
    c2.name AS category_name,
    2 AS category_level,
    CAST(c2.category1_id AS STRING) AS parent_id,
    c1.name AS parent_name,
    CONCAT(CAST(c2.category1_id AS STRING), '-', CAST(c2.id AS STRING)) AS category_path,
    CONCAT(c1.name, '-', c2.name) AS category_path_name,
    CONCAT('${pdate}', ' 00:00:00') AS create_time
FROM iceberg_ods.ods_base_category2_full c2
LEFT JOIN iceberg_ods.ods_base_category1_full c1
    ON c2.category1_id = c1.id
UNION ALL
SELECT
    CAST(c3.id AS STRING) AS id,
    '${pdate}' AS k1,
    CAST(c3.id AS STRING) AS category_id,
    c3.name AS category_name,
    3 AS category_level,
    CAST(c3.category2_id AS STRING) AS parent_id,
    c2.name AS parent_name,
    CONCAT(
        CAST(c2.category1_id AS STRING),
        '-',
        CAST(c3.category2_id AS STRING),
        '-',
        CAST(c3.id AS STRING)
    ) AS category_path,
    CONCAT(c1.name, '-', c2.name, '-', c3.name) AS category_path_name,
    CONCAT('${pdate}', ' 00:00:00') AS create_time
FROM iceberg_ods.ods_base_category3_full c3
LEFT JOIN iceberg_ods.ods_base_category2_full c2
    ON c3.category2_id = c2.id
LEFT JOIN iceberg_ods.ods_base_category1_full c1
    ON c2.category1_id = c1.id;
