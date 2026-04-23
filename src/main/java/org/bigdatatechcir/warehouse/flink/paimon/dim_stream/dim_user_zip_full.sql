SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'paimon_dim_stream_dim_user_zip_full';

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG paimon_hive;
create DATABASE IF NOT EXISTS dim_stream;

CREATE TABLE IF NOT EXISTS dim_stream.dim_user_zip_full(
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

insert into dim_stream.dim_user_zip_full(
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
from ods.ods_user_info_full;


