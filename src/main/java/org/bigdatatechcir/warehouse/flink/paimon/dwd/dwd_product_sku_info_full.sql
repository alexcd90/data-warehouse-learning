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

CREATE TABLE IF NOT EXISTS dwd.dwd_product_sku_info_full(
    `id` STRING COMMENT '编号',
    `k1` STRING COMMENT '分区字段',
    `sku_id` STRING COMMENT '商品ID',
    `spu_id` STRING COMMENT 'SPU ID',
    `price` DECIMAL(16, 2) COMMENT '价格',
    `sku_name` STRING COMMENT '商品名称',
    `sku_desc` STRING COMMENT '商品描述',
    `weight` DECIMAL(16, 2) COMMENT '重量',
    `tm_id` STRING COMMENT '品牌ID',
    `tm_name` STRING COMMENT '品牌名称',
    `category1_id` STRING COMMENT '一级分类ID',
    `category1_name` STRING COMMENT '一级分类名称',
    `category2_id` STRING COMMENT '二级分类ID',
    `category2_name` STRING COMMENT '二级分类名称',
    `category3_id` STRING COMMENT '三级分类ID',
    `category3_name` STRING COMMENT '三级分类名称',
    `default_img` STRING COMMENT '默认图片地址',
    `create_time` STRING COMMENT '创建时间',
    `attr_values` STRING COMMENT '平台属性值集合',
    `sale_attr_values` STRING COMMENT '销售属性值集合',
    `sku_cost` DECIMAL(16, 2) COMMENT '商品成本',
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

INSERT INTO dwd.dwd_product_sku_info_full(
    id,
    k1,
    sku_id,
    spu_id,
    price,
    sku_name,
    sku_desc,
    weight,
    tm_id,
    tm_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    default_img,
    create_time,
    attr_values,
    sale_attr_values,
    sku_cost
)
WITH attr AS (
    SELECT
        sku_id,
        LISTAGG(CONCAT(attr_name, ':', value_name), ';') AS attr_values
    FROM ods.ods_sku_attr_value_full
    GROUP BY sku_id
),
sale_attr AS (
    SELECT
        sku_id,
        LISTAGG(CONCAT(sale_attr_name, ':', sale_attr_value_name), ';') AS sale_attr_values
    FROM ods.ods_sku_sale_attr_value_full
    GROUP BY sku_id
),
cost AS (
    SELECT
        sku_id,
        MAX(sku_cost) AS sku_cost
    FROM ods.ods_financial_sku_cost_full
    WHERE is_lastest = '1'
    GROUP BY sku_id
)
SELECT
    CAST(s.id AS STRING) AS id,
    s.k1,
    CAST(s.id AS STRING) AS sku_id,
    COALESCE(CAST(s.spu_id AS STRING), '') AS spu_id,
    CAST(COALESCE(s.price, 0) AS DECIMAL(16, 2)) AS price,
    COALESCE(s.sku_name, '') AS sku_name,
    COALESCE(s.sku_desc, '') AS sku_desc,
    CAST(COALESCE(s.weight, 0) AS DECIMAL(16, 2)) AS weight,
    COALESCE(CAST(s.tm_id AS STRING), '') AS tm_id,
    COALESCE(tm.tm_name, '') AS tm_name,
    COALESCE(CAST(c2.category1_id AS STRING), '') AS category1_id,
    COALESCE(c1.name, '') AS category1_name,
    COALESCE(CAST(c3.category2_id AS STRING), '') AS category2_id,
    COALESCE(c2.name, '') AS category2_name,
    COALESCE(CAST(s.category3_id AS STRING), '') AS category3_id,
    COALESCE(c3.name, '') AS category3_name,
    COALESCE(s.sku_default_img, '') AS default_img,
    DATE_FORMAT(s.create_time, 'yyyy-MM-dd HH:mm:ss') AS create_time,
    COALESCE(attr.attr_values, '') AS attr_values,
    COALESCE(sale_attr.sale_attr_values, '') AS sale_attr_values,
    CAST(COALESCE(cost.sku_cost, 0) AS DECIMAL(16, 2)) AS sku_cost
FROM ods.ods_sku_info_full s
LEFT JOIN ods.ods_base_trademark_full tm
    ON s.tm_id = tm.id
LEFT JOIN ods.ods_base_category3_full c3
    ON s.category3_id = c3.id
LEFT JOIN ods.ods_base_category2_full c2
    ON c3.category2_id = c2.id
LEFT JOIN ods.ods_base_category1_full c1
    ON c2.category1_id = c1.id
LEFT JOIN attr
    ON s.id = attr.sku_id
LEFT JOIN sale_attr
    ON s.id = sale_attr.sku_id
LEFT JOIN cost
    ON s.id = cost.sku_id
WHERE s.k1 = '${pdate}';
