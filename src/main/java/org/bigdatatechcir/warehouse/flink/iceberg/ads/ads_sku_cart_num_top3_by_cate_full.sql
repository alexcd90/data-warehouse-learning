SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG iceberg_catalog;
CREATE DATABASE IF NOT EXISTS iceberg_ads;

CREATE TABLE IF NOT EXISTS iceberg_ads.ads_sku_cart_num_top3_by_cate_full(
    `dt` STRING COMMENT '统计日期',
    `category1_id` STRING COMMENT '一级分类ID',
    `category1_name` STRING COMMENT '一级分类名称',
    `category2_id` STRING COMMENT '二级分类ID',
    `category2_name` STRING COMMENT '二级分类名称',
    `category3_id` STRING COMMENT '三级分类ID',
    `category3_name` STRING COMMENT '三级分类名称',
    `sku_id` STRING COMMENT '商品id',
    `sku_name` STRING COMMENT '商品名称',
    `cart_num` BIGINT COMMENT '购物车中商品数量',
    `rk` BIGINT COMMENT '排名',
    PRIMARY KEY (`dt`, `category1_id`, `category2_id`, `category3_id`, `sku_id`, `rk`) NOT ENFORCED
) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

INSERT INTO iceberg_ads.ads_sku_cart_num_top3_by_cate_full /*+ OPTIONS('upsert-enabled' = 'true') */(dt, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name, sku_id, sku_name, cart_num, rk)
select * from iceberg_ads.ads_sku_cart_num_top10_by_cate
union
select
    CAST('${pdate}' AS DATE) dt,                -- 统计日期
    category1_id,                       -- 一级类目ID
    category1_name,                     -- 一级类目名称
    category2_id,                       -- 二级类目ID
    category2_name,                     -- 二级类目名称
    category3_id,                       -- 三级类目ID
    category3_name,                     -- 三级类目名称
    sku_id,                             -- 商品ID
    sku_name,                           -- 商品名称
    cart_num,                           -- 购物车中商品数量
    rk                                  -- 排名
from
    (
    -- 对各级类目内的商品按购物车数量进行排名
    select
    sku_id,
    sku_name,
    category1_id,
    category1_name,
    category2_id,
    category2_name,
    category3_id,
    category3_name,
    cart_num,
    -- 计算在三级类目内的购物车数量排名
    rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
    from
    (
    -- 计算购物车中各商品的数量
    select
    sku_id,
    sum(sku_num) cart_num               -- 购物车中商品总数量
    from iceberg_dwd.dwd_trade_cart_full        -- 使用全量购物车事实表
    where k1 = '${pdate}'         -- 取当天分区数据
    group by sku_id
    )cart
    left join
    (
    -- 获取商品维度信息
    select
    id,
    sku_name,                          -- 商品名称
    category1_id,                      -- 一级类目ID
    category1_name,                    -- 一级类目名称
    category2_id,                      -- 二级类目ID
    category2_name,                    -- 二级类目名称
    category3_id,                      -- 三级类目ID
    category3_name                     -- 三级类目名称
    from iceberg_dim.dim_sku_full              -- 使用商品维度全量表
    )sku
    on cart.sku_id=sku.id              -- 关联购物车数据和商品维度信息
    )t1
where rk<=3;

