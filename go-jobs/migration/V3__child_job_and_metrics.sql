-- ============================================================
-- Migration: V3__child_job_and_es_config.sql
-- 描述: 新增子任务支持、ES配置、任务分组
-- 版本: v3.0.0
-- ============================================================

-- 任务分组表
CREATE TABLE IF NOT EXISTS `job_group` (
  `id`          bigint      NOT NULL AUTO_INCREMENT,
  `name`        varchar(64) NOT NULL  COMMENT '分组名称',
  `description` varchar(255) DEFAULT '',
  `create_time` datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='任务分组';

-- job_info 新增字段
ALTER TABLE `job_info`
  ADD COLUMN `group_id`      bigint  DEFAULT '0' COMMENT '分组ID' AFTER `executor_app`,
  ADD COLUMN `child_job_ids` varchar(255) DEFAULT '' COMMENT '子任务ID，逗号分隔' AFTER `sharding_num`,
  ADD COLUMN `trigger_count` bigint  DEFAULT '0' COMMENT '累计触发次数' AFTER `last_trigger_time`;

-- 执行器补充字段
ALTER TABLE `sys_executor`
  ADD COLUMN `cpu`    float DEFAULT '0' COMMENT 'CPU使用率' AFTER `weight`,
  ADD COLUMN `memory` float DEFAULT '0' COMMENT '内存使用率' AFTER `cpu`;
