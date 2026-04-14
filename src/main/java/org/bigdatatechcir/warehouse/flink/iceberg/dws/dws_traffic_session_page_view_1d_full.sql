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

CREATE DATABASE IF NOT EXISTS iceberg_dws;

CREATE TABLE IF NOT EXISTS iceberg_dws.dws_traffic_session_page_view_1d_full(
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
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);


INSERT INTO iceberg_dws.dws_traffic_session_page_view_1d_full /*+ OPTIONS('upsert-enabled' = 'true') */(
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
FROM iceberg_dwd.dwd_traffic_page_view_full
GROUP BY session_id, mid_id, k1, brand, model, operate_system, version_code, channel;

