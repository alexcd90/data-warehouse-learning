SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'paimon_dwd_stream_dwd_traffic_error_full';

CREATE CATALOG paimon_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://192.168.244.129:9083',
    'hive-conf-dir' = '/opt/software/apache-hive-3.1.3-bin/conf',
    'hadoop-conf-dir' = '/opt/software/hadoop-3.1.3/etc/hadoop',
    'warehouse' = 'hdfs:////user/hive/warehouse'
);
USE CATALOG paimon_hive;
create  DATABASE IF NOT EXISTS dwd_stream;

CREATE TABLE IF NOT EXISTS dwd_stream.dwd_traffic_error_full(
    `id`              STRING,
    `k1`              STRING COMMENT '分区字段',
    `province_id`     BIGINT COMMENT '地区编码',
    `brand`           STRING COMMENT '手机品牌',
    `channel`         STRING COMMENT '渠道',
    `is_new`          STRING COMMENT '是否首次启动',
    `model`           STRING COMMENT '手机型号',
    `mid_id`          STRING COMMENT '设备id',
    `operate_system`  STRING COMMENT '操作系统',
    `user_id`         STRING COMMENT '会员id',
    `version_code`    STRING COMMENT 'app版本号',
    `page_item`       STRING COMMENT '目标id ',
    `page_item_type`  STRING COMMENT '目标类型',
    `last_page_id`    STRING COMMENT '上页类型',
    `page_id`         STRING COMMENT '页面ID ',
    `source_type`     STRING COMMENT '来源类型',
    `entry`           STRING COMMENT 'icon手机图标  notice 通知',
    `loading_time`    STRING COMMENT '启动加载时间',
    `open_ad_id`      STRING COMMENT '广告页ID ',
    `open_ad_ms`      STRING COMMENT '广告总共播放时间',
    `open_ad_skip_ms` STRING COMMENT '用户跳过广告时点',
    `action_id`        STRING COMMENT '动作id',
    `action_item`      STRING COMMENT '目标id ',
    `action_item_type` STRING COMMENT '目标类型',
    `action_time`      STRING COMMENT '动作发生时间',
    `display_type`      STRING COMMENT '曝光类型',
    `display_item`      STRING COMMENT '曝光对象id ',
    `display_item_type` STRING COMMENT 'app版本号',
    `display_order`     BIGINT COMMENT '曝光顺序',
    `display_pos_id`    BIGINT COMMENT '曝光位置',
    `date_id`         STRING COMMENT '日期id',
    `error_time`      STRING COMMENT '错误时间',
    `error_code`      BIGINT COMMENT '错误码',
    `error_msg`       STRING COMMENT '错误信息',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    ) PARTITIONED BY (`k1`) WITH (
    'connector' = 'paimon',
    'metastore.partitioned-table' = 'true',
    'file.format' = 'parquet',
    'write-buffer-size' = '512mb',
    'write-buffer-spillable' = 'true',
    'partition.expiration-time' = '1 d',
    'partition.expiration-check-interval' = '1 h',
    'partition.timestamp-formatter' = 'yyyy-MM-dd',
    'partition.timestamp-pattern' = '$k1'
);

CREATE TEMPORARY FUNCTION json_actions_array_parser AS 'org.bigdatatechcir.warehouse.flink.udf.JsonActionsArrayParser';

CREATE TEMPORARY FUNCTION json_displays_array_parser AS 'org.bigdatatechcir.warehouse.flink.udf.JsonDisplaysArrayParser';

INSERT INTO dwd_stream.dwd_traffic_error_full(
    id,
    k1,
    province_id,
    brand,
    channel,
    is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    page_item,
    page_item_type,
    last_page_id,
    page_id,
    source_type,
    entry,
    loading_time,
    open_ad_id,
    open_ad_ms,
    open_ad_skip_ms,
    action_id,
    action_item,
    action_item_type,
    action_time,
    display_type,
    display_item,
    display_item_type,
    display_order,
    display_pos_id,
    date_id,
    error_time,
    error_code,
    error_msg
    )
select
    id,
    k1,
    province_id,
    brand,
    channel,
    common_is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    page_item,
    page_item_type,
    page_last_page_id,
    page_page_id,
    page_source_type,
    start_entry,
    start_loading_time,
    start_open_ad_id,
    start_open_ad_ms,
    start_open_ad_skip_ms,
    action_id,
    action_item,
    action_item_type,
    DATE_FORMAT(FROM_UNIXTIME(cast(ts / 1000 as BIGINT)), 'yyyy-MM-dd') action_time,
    display_type,
    display_item,
    display_item_type,
    display_order,
    display_pos_id,
    DATE_FORMAT(FROM_UNIXTIME(cast(ts / 1000 as BIGINT)), 'yyyy-MM-dd') date_id,
    DATE_FORMAT(FROM_UNIXTIME(cast(ts / 1000 as BIGINT)), 'yyyy-MM-dd HH:mm:ss') error_time,
    err_error_code,
    error_msg
from
    (
        select
            id,
            k1,
            common_ar area_code,
            common_ba brand,
            common_ch channel,
            common_is_new,
            common_md model,
            common_mid mid_id,
            common_os operate_system,
            common_uid user_id,
            common_vc version_code,
            page_during_time,
            page_item page_item,
            page_item_type page_item_type,
            page_last_page_id,
            page_page_id,
            page_source_type,
            start_entry,
            start_loading_time,
            start_open_ad_id,
            start_open_ad_ms,
            start_open_ad_skip_ms,
            json_actions_array_parser(`actions`).`action_id` as action_id,
            json_actions_array_parser(`actions`).`item` as action_item,
            json_actions_array_parser(`actions`).`item_type` as action_item_type,
            json_actions_array_parser(`actions`).`ts` as ts,
            json_displays_array_parser(`displays`).`display_type` as display_type,
            json_displays_array_parser(`displays`).`item` as display_item,
            json_displays_array_parser(`displays`).`item_type` as display_item_type,
            json_displays_array_parser(`displays`).`order` as display_order,
            json_displays_array_parser(`displays`).`pos_id` as display_pos_id,
            err_error_code,
            err_msg error_msg
        from ods.ods_log_inc
        where  err_error_code is not null
    )log
        join
    (
        select
            id province_id,
            area_code
        from ods.ods_base_province_full
    )bp
    on log.area_code=bp.area_code;


