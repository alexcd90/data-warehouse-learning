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
CREATE DATABASE IF NOT EXISTS dim;

CREATE TABLE IF NOT EXISTS dim.dim_activity_full(
    `activity_rule_id` INT COMMENT 'activity rule id',
    `activity_id` BIGINT COMMENT 'activity id',
    `k1` STRING COMMENT 'partition field',
    `activity_name` STRING COMMENT 'activity name',
    `activity_type_code` STRING COMMENT 'activity type code',
    `activity_type_name` STRING COMMENT 'activity type name',
    `activity_desc` STRING COMMENT 'activity description',
    `start_time` STRING COMMENT 'start time',
    `end_time` STRING COMMENT 'end time',
    `create_time` STRING COMMENT 'create time',
    `condition_amount` DECIMAL(16, 2) COMMENT 'condition amount',
    `condition_num` BIGINT COMMENT 'condition num',
    `benefit_amount` DECIMAL(16, 2) COMMENT 'benefit amount',
    `benefit_discount` DECIMAL(16, 2) COMMENT 'benefit discount',
    `benefit_rule` STRING COMMENT 'benefit rule',
    `benefit_level` BIGINT COMMENT 'benefit level',
    `sku_ids` STRING COMMENT 'activity sku ids',
    PRIMARY KEY (`activity_rule_id`, `activity_id`, `k1`) NOT ENFORCED
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

insert into dim.dim_activity_full(
    activity_rule_id,
    activity_id,
    k1,
    activity_name,
    activity_type_code,
    activity_type_name,
    activity_desc,
    start_time,
    end_time,
    create_time,
    condition_amount,
    condition_num,
    benefit_amount,
    benefit_discount,
    benefit_rule,
    benefit_level,
    sku_ids
)
select
    rule.id,
    info.id,
    info.k1,
    info.activity_name,
    rule.activity_type,
    dic.dic_name,
    info.activity_desc,
    info.start_time,
    info.end_time,
    info.create_time,
    rule.condition_amount,
    rule.condition_num,
    rule.benefit_amount,
    rule.benefit_discount,
    case rule.activity_type
        when '3101' then concat('满', cast(rule.condition_amount as STRING), '元减', cast(rule.benefit_amount as STRING), '元')
        when '3102' then concat('满', cast(rule.condition_num as STRING), '件打', cast((rule.benefit_discount * 10) as STRING), '折')
        when '3103' then concat('打', cast((rule.benefit_discount * 10) as STRING), '折')
    end as benefit_rule,
    rule.benefit_level,
    sku.sku_ids
from
    (
        select
            id,
            activity_id,
            activity_type,
            condition_amount,
            condition_num,
            benefit_amount,
            benefit_discount,
            benefit_level
        from ods.ods_activity_rule_full
    ) rule
    left join
    (
        select
            id,
            k1,
            activity_name,
            activity_desc,
            start_time,
            end_time,
            create_time
        from ods.ods_activity_info_full
    ) info
    on rule.activity_id = info.id
    left join
    (
        select
            activity_id,
            LISTAGG(CAST(sku_id AS STRING), ',') AS sku_ids
        from
        (
            select distinct
                activity_id,
                sku_id
            from ods.ods_activity_sku_full
        ) t
        group by activity_id
    ) sku
    on rule.activity_id = sku.activity_id
    left join
    (
        select
            dic_code,
            dic_name
        from ods.ods_base_dic_full
        where parent_code = '31'
    ) dic
    on rule.activity_type = dic.dic_code;
