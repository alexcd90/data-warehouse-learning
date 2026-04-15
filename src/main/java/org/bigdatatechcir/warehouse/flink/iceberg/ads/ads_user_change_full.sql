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
CREATE DATABASE IF NOT EXISTS iceberg_ads;

CREATE TABLE IF NOT EXISTS iceberg_ads.ads_user_change_full(
    `dt` STRING COMMENT '统计日期',
    `user_churn_count` BIGINT COMMENT '流失用户数',
    `user_back_count` BIGINT COMMENT '回流用户数',
    PRIMARY KEY (`dt`) NOT ENFORCED
) WITH (
    'catalog-name' = 'hive_prod',
    'uri' = 'thrift://192.168.244.129:9083',
    'warehouse' = 'hdfs://192.168.244.129:9000/user/hive/warehouse/'
);

INSERT INTO iceberg_ads.ads_user_change_full /*+ OPTIONS('upsert-enabled' = 'true') */(dt, user_churn_count, user_back_count)
select * from iceberg_ads.ads_user_change
union
select
    churn.dt,                                -- 统计日期
    user_churn_count,                        -- 流失用户数
    user_back_count                          -- 回流用户数
from
    (
    -- 计算流失用户数: 最后登录日期正好是7天前的用户数
    select
        CAST('${pdate}' AS DATE) dt,                 -- 统计日期
    count(*) user_churn_count                -- 流失用户数
    from iceberg_dws.dws_user_user_login_td_full          -- 使用DWS层的用户登录历史表
where k1 = '${pdate}'                  -- 取当天分区数据
  and login_date_last=date_add(CAST('${pdate}' AS DATE),-7)  -- 最后登录日期正好是7天前
    )churn
    join
    (
    -- 计算回流用户数: 今日登录且登录间隔>=8天的用户数
select
    CAST('${pdate}' AS DATE) dt,                     -- 统计日期
    count(*) user_back_count                 -- 回流用户数
from
    (
    -- 获取今日的用户最后登录日期
    select
    user_id,
    login_date_last                          -- 最后登录日期
    from iceberg_dws.dws_user_user_login_td_full          -- 使用DWS层的用户登录历史表
    where k1 = '${pdate}'              -- 取当天分区数据
    )t1
    join
    (
    -- 获取昨日的用户最后登录日期
    select
    user_id,
    login_date_last login_date_previous      -- 前一次最后登录日期
    from iceberg_dws.dws_user_user_login_td_full          -- 使用DWS层的用户登录历史表
    where k1 = date_add(CAST('${pdate}' AS DATE),-1) -- 取前一天分区数据
    )t2
on t1.user_id=t2.user_id                     -- 关联用户ID
where datediff(login_date_last,login_date_previous)>=8  -- 今日登录且登录间隔>=8天
    )back
on churn.dt=back.dt;

