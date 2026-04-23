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
SET 'pipeline.name' = 'hudi_ads_stream_ads_traffic_stats_by_channel_full';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_ads_stream;

CREATE TABLE IF NOT EXISTS hudi_ads_stream.ads_traffic_stats_by_channel_full(
    `dt` STRING COMMENT 'stat date',
    `recent_days` BIGINT COMMENT 'recent day window',
    `channel` STRING COMMENT 'channel',
    `uv_count` BIGINT COMMENT 'visitor count',
    `avg_duration_sec` BIGINT COMMENT 'avg session duration seconds',
    `avg_page_count` BIGINT COMMENT 'avg page count',
    `sv_count` BIGINT COMMENT 'session count',
    `bounce_rate` DECIMAL(16, 2) COMMENT 'bounce rate',
    PRIMARY KEY (`dt`, `recent_days`, `channel`) NOT ENFORCED
) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

CREATE TEMPORARY VIEW tmp_ads_traffic_stats_by_channel_current_date_param AS
SELECT CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE) AS cur_date
;

CREATE TEMPORARY VIEW tmp_ads_traffic_stats_by_channel_recent_days AS
SELECT CAST(1 AS BIGINT) AS recent_days
UNION ALL
SELECT CAST(7 AS BIGINT) AS recent_days
UNION ALL
SELECT CAST(30 AS BIGINT) AS recent_days
;

INSERT INTO hudi_ads_stream.ads_traffic_stats_by_channel_full(
    dt,
    recent_days,
    channel,
    uv_count,
    avg_duration_sec,
    avg_page_count,
    sv_count,
    bounce_rate
)
SELECT
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    p.recent_days,
    COALESCE(s.channel, '') AS channel,
    COUNT(DISTINCT s.mid_id) AS uv_count,
    CAST(AVG(CAST(s.during_time_1d AS DOUBLE)) / 1000 AS BIGINT) AS avg_duration_sec,
    CAST(AVG(CAST(s.page_count_1d AS DOUBLE)) AS BIGINT) AS avg_page_count,
    COUNT(*) AS sv_count,
    CASE
        WHEN COUNT(*) = 0 THEN CAST(0 AS DECIMAL(16, 2))
        ELSE CAST(
            CAST(SUM(CASE WHEN s.page_count_1d = 1 THEN 1 ELSE 0 END) AS DECIMAL(16, 2))
            / CAST(COUNT(*) AS DECIMAL(16, 2))
            AS DECIMAL(16, 2)
        )
    END AS bounce_rate
FROM tmp_ads_traffic_stats_by_channel_recent_days p
CROSS JOIN tmp_ads_traffic_stats_by_channel_current_date_param cp
JOIN hudi_dws_stream.dws_traffic_session_page_view_1d_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ s
    ON CAST(s.k1 AS DATE) BETWEEN
        CASE
            WHEN p.recent_days = 1 THEN cp.cur_date
            WHEN p.recent_days = 7 THEN cp.cur_date - INTERVAL '6' DAY
            ELSE cp.cur_date - INTERVAL '29' DAY
        END
        AND cp.cur_date
GROUP BY p.recent_days, COALESCE(s.channel, '')
;

