-- DROP TABLE IF EXISTS ods.ods_activity_rule_full;

CREATE TABLE ods.ods_activity_rule_full
(
    `id` BIGINT COMMENT '编号',
    `activity_id` BIGINT COMMENT '活动ID',
    `activity_type` STRING COMMENT '活动类型',
    `condition_amount` DECIMAL(16,2) COMMENT '满减金额',
    `condition_num` BIGINT COMMENT '满减件数',
    `benefit_amount` DECIMAL(16,2) COMMENT '优惠金额',
    `benefit_discount` DECIMAL(16,2) COMMENT '优惠折扣',
    `benefit_level` STRING COMMENT '优惠级别'
)
ENGINE=OLAP
UNIQUE KEY(`id`)
COMMENT '活动规则全量表'
DISTRIBUTED BY HASH(`id`) BUCKETS 8
PROPERTIES
(
    "replication_allocation" = "tag.location.default: 1",
    "is_being_synced" = "false",
    "storage_format" = "V2",
    "light_schema_change" = "true",
    "disable_auto_compaction" = "false",
    "enable_single_replica_compaction" = "false",
    "bloom_filter_columns" = "id,activity_id,activity_type",
    "compaction_policy" = "time_series",
    "enable_unique_key_merge_on_write" = "true",
    "in_memory" = "false",
    "max_filter_ratio" = "0.9"
);
