SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'pipeline.name' = 'hudi_dwd_stream_dwd_traffic_page_view_full';

create catalog hudi_catalog with(
	'type' = 'hudi',
	'mode' = 'hms',
	'hive.conf.dir'='/opt/software/apache-hive-3.1.3-bin/conf'
);

use CATALOG hudi_catalog;

create  DATABASE IF NOT EXISTS hudi_dwd_stream;

CREATE TABLE IF NOT EXISTS hudi_dwd_stream.dwd_traffic_page_view_full(
    `id`             STRING,
    `k1`             STRING COMMENT '分区字段',
    `province_id`    BIGINT COMMENT '省份id',
    `brand`          STRING COMMENT '手机品牌',
    `channel`        STRING COMMENT '渠道',
    `is_new`         STRING COMMENT '是否首次启动',
    `model`          STRING COMMENT '手机型号',
    `mid_id`         STRING COMMENT '设备id',
    `operate_system` STRING COMMENT '操作系统',
    `user_id`        STRING COMMENT '会员id',
    `version_code`   STRING COMMENT 'app版本号',
    `page_item`      STRING COMMENT '目标id ',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`   STRING COMMENT '上页类型',
    `page_id`        STRING COMMENT '页面ID ',
    `source_type`    STRING COMMENT '来源类型',
    `date_id`        STRING COMMENT '日期id',
    `view_time`      STRING COMMENT '跳入时间',
    `session_id`     STRING COMMENT '所属会话id',
    `during_time`    BIGINT COMMENT '持续时间毫秒',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );

CREATE TEMPORARY VIEW hudi_dwd_traffic_page_view_log AS
SELECT
    id,
    DATE_FORMAT(FROM_UNIXTIME(CAST(ts / 1000 AS BIGINT)), 'yyyy-MM-dd') AS event_date,
    common_ar area_code,
    common_ba brand,
    common_ch channel,
    common_is_new is_new,
    common_md model,
    common_mid mid_id,
    common_os operate_system,
    common_uid user_id,
    common_vc version_code,
    page_during_time,
    page_item,
    page_item_type,
    page_last_page_id,
    page_page_id,
    page_source_type,
    ts
from hudi_ods.ods_log_inc /*+ OPTIONS('read.streaming.enabled' = 'true') */
where page_during_time is not null
  and page_page_id is not null;

CREATE TEMPORARY VIEW hudi_dwd_traffic_page_view_session_start AS
SELECT
    common_mid mid_id,
    ts session_start_point
from hudi_ods.ods_log_inc /*+ OPTIONS('read.streaming.enabled' = 'true') */
where page_last_page_id is null
  and page_page_id is not null;

INSERT INTO hudi_dwd_stream.dwd_traffic_page_view_full(
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
    date_id,
    view_time,
    session_id,
    during_time
    )
SELECT
    log.id,
    log.event_date AS k1,
    bp.province_id,
    log.brand,
    log.channel,
    log.is_new,
    log.model,
    log.mid_id,
    log.operate_system,
    log.user_id,
    log.version_code,
    log.page_item,
    log.page_item_type,
    log.page_last_page_id,
    log.page_page_id,
    log.page_source_type,
    log.event_date AS date_id,
    DATE_FORMAT(FROM_UNIXTIME(CAST(log.ts / 1000 AS BIGINT)), 'yyyy-MM-dd HH:mm:ss') AS view_time,
    CONCAT(log.mid_id, '-', CAST(COALESCE(MAX(session_start.session_start_point), log.ts) AS STRING)) AS session_id,
    log.page_during_time
from hudi_dwd_traffic_page_view_log log
left join hudi_dwd_traffic_page_view_session_start session_start
    on log.mid_id = session_start.mid_id
   and session_start.session_start_point <= log.ts
   and session_start.session_start_point >= log.ts - 1800000
left join
(
    SELECT
        id AS province_id,
        area_code
    from hudi_ods.ods_base_province_full /*+ OPTIONS('read.streaming.enabled' = 'true') */
) bp
    on log.area_code = bp.area_code
GROUP BY
    log.id,
    log.event_date,
    bp.province_id,
    log.brand,
    log.channel,
    log.is_new,
    log.model,
    log.mid_id,
    log.operate_system,
    log.user_id,
    log.version_code,
    log.page_item,
    log.page_item_type,
    log.page_last_page_id,
    log.page_page_id,
    log.page_source_type,
    log.ts,
    log.page_during_time;

