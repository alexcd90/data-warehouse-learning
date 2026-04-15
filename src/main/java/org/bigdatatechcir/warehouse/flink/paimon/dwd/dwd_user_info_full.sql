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

CREATE TABLE IF NOT EXISTS dwd.dwd_user_info_full(
    `id` STRING COMMENT '编号',
    `k1` STRING COMMENT '分区字段',
    `user_id` STRING COMMENT '用户ID',
    `login_name` STRING COMMENT '登录名',
    `nick_name` STRING COMMENT '昵称',
    `name` STRING COMMENT '用户姓名',
    `phone_num` STRING COMMENT '手机号码',
    `email` STRING COMMENT '邮箱',
    `head_img` STRING COMMENT '头像',
    `user_level` STRING COMMENT '用户等级',
    `birthday` STRING COMMENT '生日',
    `gender` STRING COMMENT '性别',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `status` STRING COMMENT '状态',
    `user_type` STRING COMMENT '用户类型',
    `is_blacklist` STRING COMMENT '是否黑名单',
    `last_login_time` STRING COMMENT '最后登录时间',
    `first_order_time` STRING COMMENT '首次下单时间',
    `first_payment_time` STRING COMMENT '首次支付时间',
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

INSERT INTO dwd.dwd_user_info_full(
    id,
    k1,
    user_id,
    login_name,
    nick_name,
    name,
    phone_num,
    email,
    head_img,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    status,
    user_type,
    is_blacklist,
    last_login_time,
    first_order_time,
    first_payment_time
)
SELECT
    CAST(id AS STRING) AS id,
    k1,
    CAST(id AS STRING) AS user_id,
    COALESCE(login_name, '') AS login_name,
    COALESCE(nick_name, '') AS nick_name,
    COALESCE(name, '') AS name,
    COALESCE(phone_num, '') AS phone_num,
    COALESCE(email, '') AS email,
    COALESCE(head_img, '') AS head_img,
    COALESCE(user_level, '') AS user_level,
    COALESCE(birthday, '') AS birthday,
    COALESCE(gender, '') AS gender,
    DATE_FORMAT(create_time, 'yyyy-MM-dd HH:mm:ss') AS create_time,
    DATE_FORMAT(operate_time, 'yyyy-MM-dd HH:mm:ss') AS operate_time,
    COALESCE(status, '') AS status,
    CAST(NULL AS STRING) AS user_type,
    CAST(NULL AS STRING) AS is_blacklist,
    CAST(NULL AS STRING) AS last_login_time,
    CAST(NULL AS STRING) AS first_order_time,
    CAST(NULL AS STRING) AS first_payment_time
FROM ods.ods_user_info_full
WHERE k1 = '${pdate}';
