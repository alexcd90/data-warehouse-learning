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
SET 'pipeline.name' = 'hudi_dim_stream_dim_sku_full';

create catalog hudi_catalog with(
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

use CATALOG hudi_catalog;

create DATABASE IF NOT EXISTS hudi_dim_stream;

CREATE TABLE IF NOT EXISTS hudi_dim_stream.dim_sku_full(
    `id` BIGINT COMMENT 'sku id',
    `k1` STRING COMMENT 'partition field',
    `price` DECIMAL(16, 2) COMMENT 'price',
    `sku_name` STRING COMMENT 'sku name',
    `sku_desc` STRING COMMENT 'sku desc',
    `weight` DECIMAL(16, 2) COMMENT 'weight',
    `is_sale` INT COMMENT 'is sale',
    `spu_id` BIGINT COMMENT 'spu id',
    `spu_name` STRING COMMENT 'spu name',
    `category3_id` BIGINT COMMENT 'category3 id',
    `category3_name` STRING COMMENT 'category3 name',
    `category2_id` BIGINT COMMENT 'category2 id',
    `category2_name` STRING COMMENT 'category2 name',
    `category1_id` BIGINT COMMENT 'category1 id',
    `category1_name` STRING COMMENT 'category1 name',
    `tm_id` BIGINT COMMENT 'tm id',
    `tm_name` STRING COMMENT 'tm name',
    `default_img` STRING COMMENT 'default image',
    `attr_ids` STRING COMMENT 'platform attr ids',
    `attr_values` STRING COMMENT 'platform attr values',
    `sale_attr_ids` STRING COMMENT 'sale attr ids',
    `sale_attr_values` STRING COMMENT 'sale attr values',
    `sku_cost` DECIMAL(16, 2) COMMENT 'sku cost',
    `create_time` TIMESTAMP(3) COMMENT 'create time',
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dim_stream.dim_sku_full(
    id,
    k1,
    price,
    sku_name,
    sku_desc,
    weight,
    is_sale,
    spu_id,
    spu_name,
    category3_id,
    category3_name,
    category2_id,
    category2_name,
    category1_id,
    category1_name,
    tm_id,
    tm_name,
    default_img,
    attr_ids,
    attr_values,
    sale_attr_ids,
    sale_attr_values,
    sku_cost,
    create_time
)
select
    s.id,
    s.k1,
    s.price,
    s.sku_name,
    s.sku_desc,
    s.weight,
    s.is_sale,
    s.spu_id,
    sp.spu_name,
    s.category3_id,
    c3.name as category3_name,
    c3.category2_id,
    c2.name as category2_name,
    c2.category1_id,
    c1.name as category1_name,
    s.tm_id,
    tm.tm_name,
    s.sku_default_img,
    a.attr_ids,
    a.attr_values,
    sa.sale_attr_ids,
    sa.sale_attr_values,
    cost.sku_cost,
    s.create_time
from
    (
        select
            id,
            k1,
            price,
            sku_name,
            sku_desc,
            weight,
            is_sale,
            spu_id,
            category3_id,
            tm_id,
            sku_default_img,
            create_time
        from hudi_ods.ods_sku_info_full
    ) s
    left join
    (
        select
            id,
            spu_name
        from hudi_ods.ods_spu_info_full
    ) sp
    on s.spu_id = sp.id
    left join
    (
        select
            id,
            name,
            category2_id
        from hudi_ods.ods_base_category3_full
    ) c3
    on s.category3_id = c3.id
    left join
    (
        select
            id,
            name,
            category1_id
        from hudi_ods.ods_base_category2_full
    ) c2
    on c3.category2_id = c2.id
    left join
    (
        select
            id,
            name
        from hudi_ods.ods_base_category1_full
    ) c1
    on c2.category1_id = c1.id
    left join
    (
        select
            id,
            tm_name
        from hudi_ods.ods_base_trademark_full
    ) tm
    on s.tm_id = tm.id
    left join
    (
        select
            sku_id,
            LISTAGG(CAST(id AS STRING), ';') AS attr_ids,
            LISTAGG(CONCAT(attr_name, ':', value_name), ';') AS attr_values
        from hudi_ods.ods_sku_attr_value_full
        group by sku_id
    ) a
    on s.id = a.sku_id
    left join
    (
        select
            sku_id,
            LISTAGG(CAST(id AS STRING), ';') AS sale_attr_ids,
            LISTAGG(CONCAT(sale_attr_name, ':', sale_attr_value_name), ';') AS sale_attr_values
        from hudi_ods.ods_sku_sale_attr_value_full
        group by sku_id
    ) sa
    on s.id = sa.sku_id
    left join
    (
        select
            sku_id,
            max(sku_cost) as sku_cost
        from hudi_ods.ods_financial_sku_cost_full
        where is_lastest = '1'
        group by sku_id
    ) cost
    on s.id = cost.sku_id;

