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
CREATE DATABASE IF NOT EXISTS ads;

CREATE TABLE IF NOT EXISTS ads.ads_user_retention_full(
    `dt` STRING COMMENT '统计日期',
    `create_date` STRING COMMENT '用户新增日期',
    `retention_day` INT COMMENT '截至当前日期留存天数',
    `retention_count` BIGINT COMMENT '留存用户数量',
    `new_user_count` BIGINT COMMENT '新增用户数量',
    `retention_rate` DECIMAL(16, 2) COMMENT '留存率',
    PRIMARY KEY (`dt`, `create_date`, `retention_day`) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'file.format' = 'parquet',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true'
);

INSERT INTO ads.ads_user_retention_full (dt, create_date, retention_day, retention_count, new_user_count, retention_rate)
select * from ads.ads_user_retention
union
select
    CAST('${pdate}' AS DATE) dt,                      -- 统计日期
    login_date_first create_date,             -- 用户注册日期
    datediff(CAST('${pdate}' AS DATE),login_date_first) retention_day,  -- 留存天数
    sum(if(login_date_last=CAST('${pdate}' AS DATE),1,0)) retention_count,  -- 留存用户数
    count(*) new_user_count,                  -- 新增用户数
    cast(sum(if(login_date_last=CAST('${pdate}' AS DATE),1,0))/count(*)*100 as decimal(16,2)) retention_rate  -- 留存率
from
    (
    -- 获取最近7天内注册的用户
    select
    user_id,
    date_id login_date_first                  -- 注册日期
    from dwd.dwd_user_register_full            -- 使用DWD层的用户注册事实表
    where k1>=date_add(CAST('${pdate}' AS DATE),-7)   -- 最近7天内注册的用户
    and k1 < CAST('${pdate}' AS DATE)                 -- 排除今日注册的用户

    )t1
    join
    (
    -- 获取用户的最后登录日期
    select
    user_id,
    login_date_last                           -- 最后登录日期
    from dws.dws_user_user_login_td_full           -- 使用DWS层的用户登录历史表
    where k1 = '${pdate}'               -- 取当天分区数据
    )t2
on t1.user_id=t2.user_id                      -- 关联用户ID
group by login_date_first;

