SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'hudi_dim_stream_dim_province_full';

create catalog hudi_catalog with(
	'type' = 'hudi',
	'mode' = 'hms',
	'hive.conf.dir'='/opt/software/apache-hive-3.1.3-bin/conf'
);

use CATALOG hudi_catalog;

create  DATABASE IF NOT EXISTS hudi_dim_stream;

CREATE TABLE IF NOT EXISTS hudi_dim_stream.dim_province_full(
    `id`            BIGINT COMMENT 'id',
    `province_name` STRING COMMENT '省市名称',
    `area_code`     STRING COMMENT '地区编码',
    `iso_code`      STRING COMMENT '旧版ISO-3166-2编码，供可视化使用',
    `iso_3166_2`    STRING COMMENT '新版IOS-3166-2编码，供可视化使用',
    `region_id`     STRING COMMENT '地区id',
    `region_name`   STRING COMMENT '地区名称',
    PRIMARY KEY (`id` ) NOT ENFORCED
    ) WITH (
      'connector' = 'hudi',
      'table.type' = 'MERGE_ON_READ',
      'read.streaming.enabled' = 'true',
      'read.streaming.check-interval' = '4',
      'hive_sync.conf.dir'='/opt/software/apache-hive-3.1.3-bin/conf'
    );

insert into hudi_dim_stream.dim_province_full(id, province_name, area_code, iso_code, iso_3166_2, region_id, region_name)
select
    province.id,
    province.name,
    province.area_code,
    province.iso_code,
    province.iso_3166_2,
    region_id,
    region_name
from
    (
        select
            id,
            name,
            region_id,
            area_code,
            iso_code,
            iso_3166_2
        from hudi_ods.ods_base_province_full
    )province
        left join
    (
        select
            id,
            region_name
        from hudi_ods.ods_base_region_full
    )region
    on province.region_id=region.id;

