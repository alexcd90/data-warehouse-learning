SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'hudi_dws_stream_dws_traffic_page_visitor_page_view_nd_full';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_dws_stream;

CREATE TABLE IF NOT EXISTS hudi_dws_stream.dws_traffic_page_visitor_page_view_nd_full(
    `mid_id` STRING COMMENT 'visitor id',
    `page_id` STRING COMMENT 'page id',
    `k1` STRING COMMENT 'partition field',
    `brand` STRING COMMENT 'device brand',
    `model` STRING COMMENT 'device model',
    `operate_system` STRING COMMENT 'device os',
    `during_time_7d` BIGINT COMMENT 'recent 7 day during time',
    `view_count_7d` BIGINT COMMENT 'recent 7 day view count',
    `during_time_30d` BIGINT COMMENT 'recent 30 day during time',
    `view_count_30d` BIGINT COMMENT 'recent 30 day view count',
    PRIMARY KEY (`mid_id`, `page_id`, `k1`) NOT ENFORCED
) PARTITIONED BY (`k1`) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

CREATE TEMPORARY VIEW tmp_dws_traffic_page_visitor_page_view_nd_current_date_param AS
    SELECT CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE) AS cur_date
;

CREATE TEMPORARY VIEW tmp_dws_traffic_page_visitor_page_view_nd_page_view_1d AS
    SELECT
        mid_id,
        page_id,
        brand,
        model,
        operate_system,
        CAST(k1 AS DATE) AS dt,
        during_time_1d,
        view_count_1d
    FROM hudi_dws_stream.dws_traffic_page_visitor_page_view_1d_full /*+ OPTIONS('read.streaming.enabled' = 'true') */
;

CREATE TEMPORARY VIEW tmp_dws_traffic_page_visitor_page_view_nd_page_view_agg AS
    SELECT
        pv.mid_id,
        pv.page_id,
        pv.brand,
        pv.model,
        pv.operate_system,
        SUM(CASE WHEN pv.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN pv.during_time_1d ELSE 0 END) AS during_time_7d,
        SUM(CASE WHEN pv.dt BETWEEN cp.cur_date - INTERVAL '6' DAY AND cp.cur_date THEN pv.view_count_1d ELSE 0 END) AS view_count_7d,
        SUM(pv.during_time_1d) AS during_time_30d,
        SUM(pv.view_count_1d) AS view_count_30d
    FROM tmp_dws_traffic_page_visitor_page_view_nd_page_view_1d pv
    CROSS JOIN tmp_dws_traffic_page_visitor_page_view_nd_current_date_param cp
    WHERE pv.dt BETWEEN cp.cur_date - INTERVAL '29' DAY AND cp.cur_date
    GROUP BY pv.mid_id, pv.page_id, pv.brand, pv.model, pv.operate_system
;

INSERT INTO hudi_dws_stream.dws_traffic_page_visitor_page_view_nd_full(
    mid_id,
    page_id,
    k1,
    brand,
    model,
    operate_system,
    during_time_7d,
    view_count_7d,
    during_time_30d,
    view_count_30d
)
SELECT
    pa.mid_id,
    pa.page_id,
    CAST(cp.cur_date AS STRING) AS k1,
    pa.brand,
    pa.model,
    pa.operate_system,
    pa.during_time_7d,
    pa.view_count_7d,
    pa.during_time_30d,
    pa.view_count_30d
FROM tmp_dws_traffic_page_visitor_page_view_nd_page_view_agg pa
CROSS JOIN tmp_dws_traffic_page_visitor_page_view_nd_current_date_param cp;

