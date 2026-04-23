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
SET 'pipeline.name' = 'hudi_dwd_stream_dwd_trade_refund_pay_suc_full';

create catalog hudi_catalog with(
	'type' = 'hudi',
	'mode' = 'hms',
	'hive.conf.dir'='/opt/software/apache-hive-3.1.3-bin/conf'
);

use CATALOG hudi_catalog;

create  DATABASE IF NOT EXISTS hudi_dwd_stream;

CREATE TABLE IF NOT EXISTS hudi_dwd_stream.dwd_trade_refund_pay_suc_full(
    `id`                BIGINT COMMENT '编号',
    `k1`                STRING COMMENT '分区字段',
    `user_id`           BIGINT COMMENT '用户ID',
    `order_id`          BIGINT COMMENT '订单编号',
    `sku_id`            BIGINT COMMENT 'SKU编号',
    `province_id`       BIGINT COMMENT '地区ID',
    `payment_type_code` STRING COMMENT '支付类型编码',
    `payment_type_name` STRING COMMENT '支付类型名称',
    `date_id`           STRING COMMENT '日期ID',
    `callback_time`     TIMESTAMP(3) COMMENT '支付成功时间',
    `refund_num`        DECIMAL(16, 2) COMMENT '退款件数',
    `refund_amount`     DECIMAL(16, 2) COMMENT '退款金额',
    PRIMARY KEY (`id`,`k1` ) NOT ENFORCED
    )   PARTITIONED BY (`k1` ) WITH (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'read.streaming.enabled' = 'true',
    'read.streaming.check-interval' = '4',
    'hive_sync.conf.dir' = '/opt/software/apache-hive-3.1.3-bin/conf'
    );

CREATE TEMPORARY VIEW tmp_hudi_dwd_trade_refund_pay_suc_refund_payment AS
SELECT
    id,
    order_id,
    sku_id,
    payment_type,
    callback_time,
    total_amount
FROM hudi_ods.ods_refund_payment_full
WHERE refund_status = '1602'
  AND callback_time IS NOT NULL
;

CREATE TEMPORARY VIEW tmp_hudi_dwd_trade_refund_pay_suc_order_info AS
SELECT
    id,
    user_id,
    province_id
FROM hudi_ods.ods_order_info_full
;

CREATE TEMPORARY VIEW tmp_hudi_dwd_trade_refund_pay_suc_refund_info AS
SELECT
    order_id,
    sku_id,
    refund_num,
    create_time
FROM hudi_ods.ods_order_refund_info_full
WHERE create_time IS NOT NULL
;

CREATE TEMPORARY VIEW tmp_hudi_dwd_trade_refund_pay_suc_payment_dic AS
SELECT
    dic_code,
    dic_name
FROM hudi_ods.ods_base_dic_full
WHERE parent_code = '11'
;

INSERT INTO hudi_dwd_stream.dwd_trade_refund_pay_suc_full(
    id,
    k1,
    user_id,
    order_id,
    sku_id,
    province_id,
    payment_type_code,
    payment_type_name,
    date_id,
    callback_time,
    refund_num,
    refund_amount
    )
SELECT
    rp.id,
    DATE_FORMAT(rp.callback_time, 'yyyy-MM-dd') AS k1,
    oi.user_id,
    rp.order_id,
    rp.sku_id,
    oi.province_id,
    rp.payment_type,
    dic.dic_name,
    DATE_FORMAT(rp.callback_time, 'yyyy-MM-dd') AS date_id,
    rp.callback_time,
    ri.refund_num,
    rp.total_amount
FROM tmp_hudi_dwd_trade_refund_pay_suc_refund_payment rp
LEFT JOIN tmp_hudi_dwd_trade_refund_pay_suc_order_info oi
    ON rp.order_id = oi.id
LEFT JOIN tmp_hudi_dwd_trade_refund_pay_suc_refund_info ri
    ON rp.order_id = ri.order_id
   AND rp.sku_id = ri.sku_id
   AND rp.callback_time >= ri.create_time
   AND rp.callback_time <= ri.create_time + INTERVAL '30' DAY
LEFT JOIN tmp_hudi_dwd_trade_refund_pay_suc_payment_dic dic
    ON rp.payment_type = dic.dic_code
;

