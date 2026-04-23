SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'paimon_ads_stream_ads_page_path_full';

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG paimon_hive;
CREATE DATABASE IF NOT EXISTS ads_stream;

CREATE TABLE IF NOT EXISTS ads_stream.ads_page_path_full(
    `dt` STRING COMMENT 'stat date',
    `recent_days` BIGINT COMMENT 'recent day window',
    `source` STRING COMMENT 'source page path node',
    `target` STRING COMMENT 'target page path node',
    `path_count` BIGINT COMMENT 'path count',
    PRIMARY KEY (`dt`, `recent_days`, `source`, `target`) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'file.format' = 'parquet',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true'
);

CREATE TEMPORARY VIEW tmp_ads_page_path_base AS
SELECT
    CAST(1 AS BIGINT) AS recent_days,
    pv.session_id,
    pv.id,
    pv.view_time,
    pv.page_id
FROM dwd.dwd_traffic_page_view_full pv
WHERE CAST(pv.k1 AS DATE) =
    CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE)
UNION ALL
SELECT
    CAST(7 AS BIGINT) AS recent_days,
    pv.session_id,
    pv.id,
    pv.view_time,
    pv.page_id
FROM dwd.dwd_traffic_page_view_full pv
WHERE CAST(pv.k1 AS DATE) BETWEEN
    CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE) - INTERVAL '6' DAY
    AND CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE)
UNION ALL
SELECT
    CAST(30 AS BIGINT) AS recent_days,
    pv.session_id,
    pv.id,
    pv.view_time,
    pv.page_id
FROM dwd.dwd_traffic_page_view_full pv
WHERE CAST(pv.k1 AS DATE) BETWEEN
    CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE) - INTERVAL '29' DAY
    AND CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE)
;

CREATE TEMPORARY VIEW tmp_ads_page_path_ranked AS
SELECT
    curr.recent_days,
    curr.session_id,
    curr.id,
    curr.page_id,
    CAST(COUNT(pre_evt.id) + 1 AS BIGINT) AS rn
FROM tmp_ads_page_path_base curr
LEFT JOIN tmp_ads_page_path_base pre_evt
    ON curr.recent_days = pre_evt.recent_days
    AND curr.session_id = pre_evt.session_id
    AND (
        pre_evt.view_time < curr.view_time
        OR (pre_evt.view_time = curr.view_time AND pre_evt.id < curr.id)
    )
GROUP BY
    curr.recent_days,
    curr.session_id,
    curr.id,
    curr.page_id,
    curr.view_time
;

CREATE TEMPORARY VIEW tmp_ads_page_path_next AS
SELECT
    cur.recent_days,
    cur.session_id,
    cur.id,
    nxt.page_id AS next_page_id
FROM tmp_ads_page_path_base cur
LEFT JOIN tmp_ads_page_path_base nxt
    ON cur.recent_days = nxt.recent_days
    AND cur.session_id = nxt.session_id
    AND (
        nxt.view_time > cur.view_time
        OR (nxt.view_time = cur.view_time AND nxt.id > cur.id)
    )
LEFT JOIN tmp_ads_page_path_base mid
    ON cur.recent_days = mid.recent_days
    AND cur.session_id = mid.session_id
    AND nxt.id IS NOT NULL
    AND (
        mid.view_time > cur.view_time
        OR (mid.view_time = cur.view_time AND mid.id > cur.id)
    )
    AND (
        mid.view_time < nxt.view_time
        OR (mid.view_time = nxt.view_time AND mid.id < nxt.id)
    )
WHERE mid.id IS NULL
;

CREATE TEMPORARY VIEW tmp_ads_page_path_sequence AS
SELECT
    ranked.recent_days,
    ranked.session_id,
    ranked.page_id,
    ranked.rn,
    nxt.next_page_id
FROM tmp_ads_page_path_ranked ranked
LEFT JOIN tmp_ads_page_path_next nxt
    ON ranked.recent_days = nxt.recent_days
    AND ranked.session_id = nxt.session_id
    AND ranked.id = nxt.id
;

INSERT INTO ads_stream.ads_page_path_full(
    dt,
    recent_days,
    source,
    target,
    path_count
)
SELECT
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
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


