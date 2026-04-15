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

CREATE DATABASE IF NOT EXISTS hudi_ads;

CREATE TABLE IF NOT EXISTS hudi_ads.ads_page_path_full(
    `dt` STRING COMMENT 'stat date',
    `recent_days` BIGINT COMMENT 'recent day window',
    `source` STRING COMMENT 'source page path node',
    `target` STRING COMMENT 'target page path node',
    `path_count` BIGINT COMMENT 'path count',
    PRIMARY KEY (`dt`, `recent_days`, `source`, `target`) NOT ENFORCED
) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

CREATE TEMPORARY VIEW tmp_ads_page_path_current_date_param AS
SELECT CAST('${pdate}' AS DATE) AS cur_date
;

CREATE TEMPORARY VIEW tmp_ads_page_path_recent_days AS
SELECT CAST(1 AS BIGINT) AS recent_days
UNION ALL
SELECT CAST(7 AS BIGINT) AS recent_days
UNION ALL
SELECT CAST(30 AS BIGINT) AS recent_days
;

CREATE TEMPORARY VIEW tmp_ads_page_path_base AS
SELECT
    p.recent_days,
    pv.session_id,
    pv.id,
    pv.view_time,
    pv.page_id
FROM tmp_ads_page_path_recent_days p
CROSS JOIN tmp_ads_page_path_current_date_param cp
JOIN hudi_dwd.dwd_traffic_page_view_full /*+ OPTIONS('read.streaming.enabled' = 'false') */ pv
    ON CAST(pv.k1 AS DATE) BETWEEN
        CASE
            WHEN p.recent_days = 1 THEN cp.cur_date
            WHEN p.recent_days = 7 THEN cp.cur_date - INTERVAL '6' DAY
            ELSE cp.cur_date - INTERVAL '29' DAY
        END
        AND cp.cur_date
;

CREATE TEMPORARY VIEW tmp_ads_page_path_sequence AS
SELECT
    recent_days,
    session_id,
    page_id,
    ROW_NUMBER() OVER (PARTITION BY recent_days, session_id ORDER BY view_time, id) AS rn,
    LEAD(page_id, 1) OVER (PARTITION BY recent_days, session_id ORDER BY view_time, id) AS next_page_id
FROM tmp_ads_page_path_base
;

INSERT INTO hudi_ads.ads_page_path_full(
    dt,
    recent_days,
    source,
    target,
    path_count
)
SELECT
    '${pdate}' AS dt,
    recent_days,
    CONCAT('step-', CAST(rn AS STRING), ':', COALESCE(page_id, 'null')) AS source,
    CASE
        WHEN next_page_id IS NULL THEN 'null'
        ELSE CONCAT('step-', CAST(rn + 1 AS STRING), ':', next_page_id)
    END AS target,
    COUNT(*) AS path_count
FROM tmp_ads_page_path_sequence
GROUP BY
    recent_days,
    CONCAT('step-', CAST(rn AS STRING), ':', COALESCE(page_id, 'null')),
    CASE
        WHEN next_page_id IS NULL THEN 'null'
        ELSE CONCAT('step-', CAST(rn + 1 AS STRING), ':', next_page_id)
    END
;
