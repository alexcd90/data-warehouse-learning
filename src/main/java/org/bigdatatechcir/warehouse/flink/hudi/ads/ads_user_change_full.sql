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

CREATE TABLE IF NOT EXISTS hudi_ads.ads_user_change_full(
    `dt` STRING COMMENT 'stat date',
    `user_churn_count` BIGINT COMMENT 'churn user count',
    `user_back_count` BIGINT COMMENT 'backflow user count',
    PRIMARY KEY (`dt`) NOT ENFORCED
) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
);

CREATE TEMPORARY TABLE tmp_ads_user_change_login_snapshot
WITH (
    'read.streaming.enabled' = 'false'
)
LIKE hudi_dws.dws_user_user_login_td_full;

INSERT INTO hudi_ads.ads_user_change_full(
    dt,
    user_churn_count,
    user_back_count
)
SELECT
    churn.dt,
    churn.user_churn_count,
    back.user_back_count
FROM (
    SELECT
        '${pdate}' AS dt,
        COUNT(*) AS user_churn_count
    FROM tmp_ads_user_change_login_snapshot
    WHERE k1 = '${pdate}'
      AND CAST(login_date_last AS DATE) = TIMESTAMPADD(DAY, -7, CAST('${pdate}' AS DATE))
) churn
JOIN (
    SELECT
        '${pdate}' AS dt,
        COUNT(*) AS user_back_count
    FROM (
        SELECT
            t1.user_id
        FROM (
            SELECT
                user_id,
                CAST(login_date_last AS DATE) AS login_date_last
            FROM tmp_ads_user_change_login_snapshot
            WHERE k1 = '${pdate}'
        ) t1
        JOIN (
            SELECT
                user_id,
                CAST(login_date_last AS DATE) AS login_date_previous
            FROM tmp_ads_user_change_login_snapshot
            WHERE CAST(k1 AS DATE) = TIMESTAMPADD(DAY, -1, CAST('${pdate}' AS DATE))
        ) t2
            ON t1.user_id = t2.user_id
        WHERE TIMESTAMPDIFF(DAY, t2.login_date_previous, t1.login_date_last) >= 8
    ) s
) back
    ON churn.dt = back.dt;
