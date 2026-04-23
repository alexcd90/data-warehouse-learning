SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl' = '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer' = 'DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'hudi_ads_stream_ads_new_buyer_stats_full';

CREATE CATALOG hudi_catalog WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

USE CATALOG hudi_catalog;

CREATE DATABASE IF NOT EXISTS hudi_ads_stream;

CREATE TABLE IF NOT EXISTS hudi_ads_stream.ads_new_buyer_stats_full(
    `dt` STRING COMMENT 'stat date',
    `recent_days` BIGINT COMMENT 'recent day window',
    `new_order_user_count` BIGINT COMMENT 'new order user count',
    `new_payment_user_count` BIGINT COMMENT 'new payment user count',
    PRIMARY KEY (`dt`, `recent_days`) NOT ENFORCED
) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

CREATE TEMPORARY VIEW tmp_ads_new_buyer_stats_current_date_param AS
SELECT CAST(DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS DATE) AS cur_date
;

CREATE TEMPORARY VIEW tmp_ads_new_buyer_stats_recent_days AS
SELECT CAST(1 AS BIGINT) AS recent_days
UNION ALL
SELECT CAST(7 AS BIGINT) AS recent_days
UNION ALL
SELECT CAST(30 AS BIGINT) AS recent_days
;

CREATE TEMPORARY VIEW tmp_ads_new_buyer_stats_order AS
SELECT
    p.recent_days,
    COALESCE(
        SUM(
            CASE
                WHEN CAST(o.order_date_first AS DATE) >=
                    CASE
                        WHEN p.recent_days = 1 THEN cp.cur_date
                        WHEN p.recent_days = 7 THEN cp.cur_date - INTERVAL '6' DAY
                        ELSE cp.cur_date - INTERVAL '29' DAY
                    END
                THEN 1
                ELSE 0
            END
        ),
        0
    ) AS new_order_user_count
FROM tmp_ads_new_buyer_stats_recent_days p
CROSS JOIN tmp_ads_new_buyer_stats_current_date_param cp
LEFT JOIN hudi_dws_stream.dws_trade_user_order_td_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ o
    ON o.k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
GROUP BY p.recent_days
;

CREATE TEMPORARY VIEW tmp_ads_new_buyer_stats_payment AS
SELECT
    p.recent_days,
    COALESCE(
        SUM(
            CASE
                WHEN CAST(pay.payment_date_first AS DATE) >=
                    CASE
                        WHEN p.recent_days = 1 THEN cp.cur_date
                        WHEN p.recent_days = 7 THEN cp.cur_date - INTERVAL '6' DAY
                        ELSE cp.cur_date - INTERVAL '29' DAY
                    END
                THEN 1
                ELSE 0
            END
        ),
        0
    ) AS new_payment_user_count
FROM tmp_ads_new_buyer_stats_recent_days p
CROSS JOIN tmp_ads_new_buyer_stats_current_date_param cp
LEFT JOIN hudi_dws_stream.dws_trade_user_payment_td_full /*+ OPTIONS('read.streaming.enabled' = 'true') */ pay
    ON pay.k1 = DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd')
GROUP BY p.recent_days
;

INSERT INTO hudi_ads_stream.ads_new_buyer_stats_full(
    dt,
    recent_days,
    new_order_user_count,
    new_payment_user_count
)
SELECT
    DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS dt,
    p.recent_days,
    COALESCE(o.new_order_user_count, 0) AS new_order_user_count,
    COALESCE(pay.new_payment_user_count, 0) AS new_payment_user_count
FROM tmp_ads_new_buyer_stats_recent_days p
LEFT JOIN tmp_ads_new_buyer_stats_order o
    ON p.recent_days = o.recent_days
LEFT JOIN tmp_ads_new_buyer_stats_payment pay
    ON p.recent_days = pay.recent_days
;

