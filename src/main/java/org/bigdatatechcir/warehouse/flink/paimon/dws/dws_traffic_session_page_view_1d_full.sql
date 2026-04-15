SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG paimon_hive;
CREATE DATABASE IF NOT EXISTS dws;

CREATE TABLE IF NOT EXISTS dws.dws_traffic_session_page_view_1d_full(
    `session_id` STRING COMMENT 'session id',
    `mid_id` STRING COMMENT 'device id',
    `k1` STRING COMMENT 'partition field',
    `brand` STRING COMMENT 'device brand',
    `model` STRING COMMENT 'device model',
    `operate_system` STRING COMMENT 'device os',
    `version_code` STRING COMMENT 'app version',
    `channel` STRING COMMENT 'channel',
    `during_time_1d` BIGINT COMMENT 'daily during time',
    `page_count_1d` BIGINT COMMENT 'daily page count',
    PRIMARY KEY (`session_id`, `mid_id`, `k1`) NOT ENFORCED
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

INSERT INTO dws.dws_traffic_session_page_view_1d_full(
    session_id,
    mid_id,
    k1,
    brand,
    model,
    operate_system,
    version_code,
    channel,
    during_time_1d,
    page_count_1d
)
SELECT
    session_id,
    mid_id,
    k1,
    brand,
    model,
    operate_system,
    version_code,
    channel,
    SUM(during_time) AS during_time_1d,
    COUNT(*) AS page_count_1d
FROM dwd.dwd_traffic_page_view_full
GROUP BY session_id, mid_id, k1, brand, model, operate_system, version_code, channel;
