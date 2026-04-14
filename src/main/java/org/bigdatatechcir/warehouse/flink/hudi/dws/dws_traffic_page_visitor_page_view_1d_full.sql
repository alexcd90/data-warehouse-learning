SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_dws;

CREATE TABLE IF NOT EXISTS hudi_dws.dws_traffic_page_visitor_page_view_1d_full(
    `mid_id` STRING COMMENT 'visitor id',
    `k1` STRING COMMENT 'partition field',
    `brand` STRING COMMENT 'device brand',
    `model` STRING COMMENT 'device model',
    `operate_system` STRING COMMENT 'device os',
    `page_id` STRING COMMENT 'page id',
    `during_time_1d` BIGINT COMMENT 'daily during time',
    `view_count_1d` BIGINT COMMENT 'daily page view count',
    PRIMARY KEY (`mid_id`, `page_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

INSERT INTO hudi_dws.dws_traffic_page_visitor_page_view_1d_full(
    mid_id,
    k1,
    brand,
    model,
    operate_system,
    page_id,
    during_time_1d,
    view_count_1d
)
SELECT
    mid_id,
    k1,
    brand,
    model,
    operate_system,
    page_id,
    SUM(during_time) AS during_time_1d,
    COUNT(*) AS view_count_1d
FROM hudi_dwd.dwd_traffic_page_view_full /*+ OPTIONS('read.streaming.enabled' = 'false') */
GROUP BY mid_id, k1, brand, model, operate_system, page_id;
