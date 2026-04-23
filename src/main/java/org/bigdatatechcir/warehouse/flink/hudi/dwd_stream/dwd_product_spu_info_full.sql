SET 'execution.checkpointing.interval' = '30s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '5s';
SET 'table.exec.mini-batch.size' = '5000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'table.dynamic-table-options.enabled' = 'true';
SET 'pipeline.name' = 'hudi_dwd_stream_dwd_product_spu_info_full';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_dwd_stream;

CREATE TABLE IF NOT EXISTS hudi_dwd_stream.dwd_product_spu_info_full(
    `id` STRING COMMENT '编号',
    `k1` STRING COMMENT 'stream partition marker',
    `spu_id` STRING COMMENT 'SPU ID',
    `spu_name` STRING COMMENT 'SPU名称',
    `description` STRING COMMENT '商品描述',
    `tm_id` STRING COMMENT '品牌ID',
    `tm_name` STRING COMMENT '品牌名称',
    `category1_id` STRING COMMENT '一级分类ID',
    `category1_name` STRING COMMENT '一级分类名称',
    `category2_id` STRING COMMENT '二级分类ID',
    `category2_name` STRING COMMENT '二级分类名称',
    `category3_id` STRING COMMENT '三级分类ID',
    `category3_name` STRING COMMENT '三级分类名称',
    `default_img` STRING COMMENT '默认图片地址',
    `create_time` STRING COMMENT 'stream materialized time',
    `sale_attrs` STRING COMMENT '销售属性集合',
    `images` STRING COMMENT '图片集合',
    `posters` STRING COMMENT '海报集合',
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

CREATE TEMPORARY VIEW tmp_dwd_product_spu_info_full_img AS
SELECT
    spu_id,
    MAX(img_url) AS default_img,
    LISTAGG(img_url, ';') AS images
FROM hudi_ods.ods_spu_image_full /*+ OPTIONS('read.streaming.enabled' = 'true') */
GROUP BY spu_id;

CREATE TEMPORARY VIEW tmp_dwd_product_spu_info_full_sale_attr AS
SELECT
    sa.spu_id,
    LISTAGG(
        CONCAT(sa.sale_attr_name, ':', COALESCE(sav.sale_attr_value_name, '')),
        ';'
    ) AS sale_attrs
FROM hudi_ods.ods_spu_sale_attr_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ sa
LEFT JOIN hudi_ods.ods_spu_sale_attr_value_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ sav
    ON sa.spu_id = sav.spu_id
   AND sa.base_sale_attr_id = sav.base_sale_attr_id
   AND sa.sale_attr_name = sav.sale_attr_name
GROUP BY sa.spu_id;

CREATE TEMPORARY VIEW tmp_dwd_product_spu_info_full_poster AS
SELECT
    spu_id,
    LISTAGG(img_url, ';') AS posters
FROM hudi_ods.ods_spu_poster_full /*+ OPTIONS('read.streaming.enabled' = 'true') */
WHERE is_deleted = 0
GROUP BY spu_id;

INSERT INTO hudi_dwd_stream.dwd_product_spu_info_full(
    id,
    k1,
    spu_id,
    spu_name,
    description,
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
    sale_attrs,
    images,
    posters
)
SELECT
    CAST(sp.id AS STRING) AS id,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS k1,
    CAST(sp.id AS STRING) AS spu_id,
    COALESCE(sp.spu_name, '') AS spu_name,
    COALESCE(sp.description, '') AS description,
    COALESCE(CAST(sp.tm_id AS STRING), '') AS tm_id,
    COALESCE(tm.tm_name, '') AS tm_name,
    COALESCE(CAST(c2.category1_id AS STRING), '') AS category1_id,
    COALESCE(c1.name, '') AS category1_name,
    COALESCE(CAST(c3.category2_id AS STRING), '') AS category2_id,
    COALESCE(c2.name, '') AS category2_name,
    COALESCE(CAST(sp.category3_id AS STRING), '') AS category3_id,
    COALESCE(c3.name, '') AS category3_name,
    COALESCE(img.default_img, '') AS default_img,
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd HH:mm:ss') AS create_time,
    COALESCE(sa.sale_attrs, '') AS sale_attrs,
    COALESCE(img.images, '') AS images,
    COALESCE(po.posters, '') AS posters
FROM hudi_ods.ods_spu_info_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ sp
LEFT JOIN hudi_ods.ods_base_trademark_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ tm
    ON sp.tm_id = tm.id
LEFT JOIN hudi_ods.ods_base_category3_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ c3
    ON sp.category3_id = c3.id
LEFT JOIN hudi_ods.ods_base_category2_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ c2
    ON c3.category2_id = c2.id
LEFT JOIN hudi_ods.ods_base_category1_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ c1
    ON c2.category1_id = c1.id
LEFT JOIN tmp_dwd_product_spu_info_full_img img
    ON sp.id = img.spu_id
LEFT JOIN tmp_dwd_product_spu_info_full_sale_attr sa
    ON sp.id = sa.spu_id
LEFT JOIN tmp_dwd_product_spu_info_full_poster po
    ON sp.id = po.spu_id;
