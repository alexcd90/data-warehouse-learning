SET 'table.dynamic-table-options.enabled' = 'true';
SET 'execution.checkpointing.interval' = '100s';
SET 'table.exec.state.ttl'= '8640000';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '60s';
SET 'table.exec.mini-batch.size' = '10000';
SET 'table.local-time-zone' = 'Asia/Shanghai';
SET 'table.exec.sink.not-null-enforcer'='DROP';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'pipeline.name' = 'paimon_dwd_stream_dwd_trade_pay_detail_suc_full';

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

CREATE TABLE IF NOT EXISTS dwd_stream.dwd_trade_pay_detail_suc_full(
    `id`                    BIGINT COMMENT '编号',
    `k1`                    STRING COMMENT '分区字段',
    `order_id`              BIGINT COMMENT '订单id',
    `user_id`               BIGINT COMMENT '用户id',
    `sku_id`                BIGINT COMMENT '商品id',
    `province_id`           BIGINT COMMENT '省份id',
    `activity_id`           BIGINT COMMENT '参与活动规则id',
    `activity_rule_id`      BIGINT COMMENT '参与活动规则id',
    `coupon_id`             BIGINT COMMENT '使用优惠券id',
    `payment_type_code`     STRING COMMENT '支付类型编码',
    `payment_type_name`     STRING COMMENT '支付类型名称',
    `date_id`               STRING COMMENT '支付日期id',
    `callback_time`         timestamp(3) COMMENT '支付成功时间',
    `source_id`             BIGINT COMMENT '来源编号',
    `source_type_code`      STRING COMMENT '来源类型编码',
    `source_type_name`      STRING COMMENT '来源类型名称',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '应支付原始金额',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '支付活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '支付优惠券优惠分摊',
    `split_payment_amount`  DECIMAL(16, 2) COMMENT '支付金额',
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

CREATE TEMPORARY VIEW tmp_dwd_trade_pay_detail_suc_order_detail AS
SELECT
    id,
    order_id,
    sku_id,
    create_time,
    source_id,
    source_type,
    sku_num,
    sku_num * order_price AS split_original_amount,
    split_total_amount,
    split_activity_amount,
    split_coupon_amount
FROM ods.ods_order_detail_full
WHERE create_time IS NOT NULL
;

CREATE TEMPORARY VIEW tmp_dwd_trade_pay_detail_suc_payment_success AS
SELECT
    user_id,
    order_id,
    payment_type,
    callback_time
FROM ods.ods_payment_info_full
WHERE payment_status = '1602'
  AND callback_time IS NOT NULL
;

CREATE TEMPORARY VIEW tmp_dwd_trade_pay_detail_suc_order_info AS
SELECT
    id,
    province_id
FROM ods.ods_order_info_full
;

CREATE TEMPORARY VIEW tmp_dwd_trade_pay_detail_suc_activity AS
SELECT
    order_detail_id,
    activity_id,
    activity_rule_id
FROM ods.ods_order_detail_activity_full
;

CREATE TEMPORARY VIEW tmp_dwd_trade_pay_detail_suc_coupon AS
SELECT
    order_detail_id,
    coupon_id
FROM ods.ods_order_detail_coupon_full
;

CREATE TEMPORARY VIEW tmp_dwd_trade_pay_detail_suc_payment_dic AS
SELECT
    dic_code,
    dic_name
FROM ods.ods_base_dic_full
WHERE parent_code = '11'
;

CREATE TEMPORARY VIEW tmp_dwd_trade_pay_detail_suc_source_dic AS
SELECT
    dic_code,
    dic_name
FROM ods.ods_base_dic_full
WHERE parent_code = '24'
;


INSERT INTO dwd_stream.dwd_trade_pay_detail_suc_full(
    id,
    k1,
    order_id,
    user_id,
    sku_id,
    province_id,
    activity_id,
    activity_rule_id,
    coupon_id,
    payment_type_code,
    payment_type_name,
    date_id,
    callback_time,
    source_id,
    source_type_code,
    source_type_name,
    sku_num,
    split_original_amount,
    split_activity_amount,
    split_coupon_amount,
    split_payment_amount
    )
SELECT
    od.id,
    DATE_FORMAT(pi.callback_time, 'yyyy-MM-dd') AS k1,
    od.order_id,
    pi.user_id,
    od.sku_id,
    oi.province_id,
    act.activity_id,
    act.activity_rule_id,
    cou.coupon_id,
    pi.payment_type,
    pay_dic.dic_name,
    DATE_FORMAT(pi.callback_time, 'yyyy-MM-dd') AS date_id,
    pi.callback_time,
    od.source_id,
    od.source_type,
    src_dic.dic_name,
    od.sku_num,
    od.split_original_amount,
    od.split_activity_amount,
    od.split_coupon_amount,
    od.split_total_amount
FROM tmp_dwd_trade_pay_detail_suc_order_detail od
JOIN tmp_dwd_trade_pay_detail_suc_payment_success pi
    ON od.order_id = pi.order_id
   AND pi.callback_time >= od.create_time
   AND pi.callback_time <= od.create_time + INTERVAL '30' DAY
LEFT JOIN tmp_dwd_trade_pay_detail_suc_order_info oi
    ON od.order_id = oi.id
LEFT JOIN tmp_dwd_trade_pay_detail_suc_activity act
    ON od.id = act.order_detail_id
LEFT JOIN tmp_dwd_trade_pay_detail_suc_coupon cou
    ON od.id = cou.order_detail_id
LEFT JOIN tmp_dwd_trade_pay_detail_suc_payment_dic pay_dic
    ON pi.payment_type = pay_dic.dic_code
LEFT JOIN tmp_dwd_trade_pay_detail_suc_source_dic src_dic
    ON od.source_type = src_dic.dic_code
;


