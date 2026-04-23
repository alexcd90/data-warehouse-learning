SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'iceberg_dim_stream_dim_coupon_full';

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG iceberg_catalog;
create DATABASE IF NOT EXISTS iceberg_dim_stream;

CREATE TABLE IF NOT EXISTS iceberg_dim_stream.dim_coupon_full(
    `id` BIGINT COMMENT 'coupon id',
    `k1` STRING COMMENT 'partition field',
    `coupon_name` STRING COMMENT 'coupon name',
    `coupon_type_code` STRING COMMENT 'coupon type code',
    `coupon_type_name` STRING COMMENT 'coupon type name',
    `condition_amount` DECIMAL(16, 2) COMMENT 'condition amount',
    `condition_num` BIGINT COMMENT 'condition num',
    `activity_id` BIGINT COMMENT 'activity id',
    `benefit_amount` DECIMAL(16, 2) COMMENT 'benefit amount',
    `benefit_discount` DECIMAL(16, 2) COMMENT 'benefit discount',
    `benefit_rule` STRING COMMENT 'benefit rule',
    `create_time` TIMESTAMP(3) COMMENT 'create time',
    `range_type_code` STRING COMMENT 'range type code',
    `range_type_name` STRING COMMENT 'range type name',
    `limit_num` BIGINT COMMENT 'limit num',
    `taken_count` BIGINT COMMENT 'taken count',
    `start_time` TIMESTAMP(3) COMMENT 'start time',
    `end_time` TIMESTAMP(3) COMMENT 'end time',
    `operate_time` TIMESTAMP(3) COMMENT 'operate time',
    `expire_time` TIMESTAMP(3) COMMENT 'expire time',
    `range_ids` STRING COMMENT 'range ids',
    `range_names` STRING COMMENT 'range names',
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

insert into iceberg_dim_stream.dim_coupon_full(
    id,
    k1,
    coupon_name,
    coupon_type_code,
    coupon_type_name,
    condition_amount,
    condition_num,
    activity_id,
    benefit_amount,
    benefit_discount,
    benefit_rule,
    create_time,
    range_type_code,
    range_type_name,
    limit_num,
    taken_count,
    start_time,
    end_time,
    operate_time,
    expire_time,
    range_ids,
    range_names
)
select
    ci.id,
    ci.k1,
    ci.coupon_name,
    ci.coupon_type,
    coupon_dic.dic_name,
    ci.condition_amount,
    ci.condition_num,
    ci.activity_id,
    ci.benefit_amount,
    ci.benefit_discount,
    case ci.coupon_type
        when '3201' then concat('满', cast(ci.condition_amount as STRING), '元减', cast(ci.benefit_amount as STRING), '元')
        when '3202' then concat('满', cast(ci.condition_num as STRING), '件打', cast((ci.benefit_discount * 10) as STRING), '折')
        when '3203' then concat('减', cast(ci.benefit_amount as STRING), '元')
    end as benefit_rule,
    ci.create_time,
    ci.range_type,
    range_dic.dic_name,
    ci.limit_num,
    ci.taken_count,
    ci.start_time,
    ci.end_time,
    ci.operate_time,
    ci.expire_time,
    cr.range_ids,
    cr.range_names
from
    (
        select
            id,
            k1,
            coupon_name,
            coupon_type,
            condition_amount,
            condition_num,
            activity_id,
            benefit_amount,
            benefit_discount,
            create_time,
            range_type,
            limit_num,
            taken_count,
            start_time,
            end_time,
            operate_time,
            expire_time
        from iceberg_ods.ods_coupon_info_full
    ) ci
    left join
    (
        select
            dic_code,
            dic_name
        from iceberg_ods.ods_base_dic_full
        where parent_code = '32'
    ) coupon_dic
    on ci.coupon_type = coupon_dic.dic_code
    left join
    (
        select
            dic_code,
            dic_name
        from iceberg_ods.ods_base_dic_full
        where parent_code = '33'
    ) range_dic
    on ci.range_type = range_dic.dic_code
    left join
    (
        select
            coupon_id,
            LISTAGG(CAST(range_id AS STRING), ';') AS range_ids,
            LISTAGG(CAST(range_id AS STRING), ';') AS range_names
        from
        (
            select distinct
                coupon_id,
                range_id
            from iceberg_ods.ods_coupon_range_full
        ) t
        group by coupon_id
    ) cr
    on ci.id = cr.coupon_id;


