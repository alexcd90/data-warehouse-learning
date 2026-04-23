SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'hudi_dim_stream_dim_user_zip_full';

create catalog hudi_catalog with(
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

use CATALOG hudi_catalog;

create DATABASE IF NOT EXISTS hudi_dim_stream;

CREATE TABLE IF NOT EXISTS hudi_dim_stream.dim_user_zip_full(
    `id` BIGINT COMMENT 'user id',
    `k1` STRING COMMENT 'partition field',
    `login_name` STRING COMMENT 'login name',
    `nick_name` STRING COMMENT 'nick name',
    `name` STRING COMMENT 'name',
    `phone_num` STRING COMMENT 'phone num',
    `email` STRING COMMENT 'email',
    `user_level` STRING COMMENT 'user level',
    `birthday` STRING COMMENT 'birthday',
    `gender` STRING COMMENT 'gender',
    `create_time` TIMESTAMP(3) COMMENT 'create time',
    `operate_time` TIMESTAMP(3) COMMENT 'operate time',
    `start_date` CHAR(10) COMMENT 'start date',
    `end_date` CHAR(10) COMMENT 'end date',
    PRIMARY KEY (`id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

insert into hudi_dim_stream.dim_user_zip_full(
    id,
    k1,
    login_name,
    nick_name,
    name,
    phone_num,
    email,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    start_date,
    end_date
)
select
    id,
    k1,
    login_name,
    nick_name,
    md5(name),
    md5(phone_num),
    md5(email),
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    '2020-06-14' as start_date,
    '9999-12-31' as end_date
from hudi_ods.ods_user_info_full;

