-- ============================================================
-- Migration: V2__alarm_and_stats.sql
-- 描述: 新增告警配置表、统计表
-- 版本: v2.0.0
-- ============================================================

-- 告警配置表
CREATE TABLE IF NOT EXISTS `alarm_config` (
  `id`          bigint      NOT NULL AUTO_INCREMENT,
  `name`        varchar(64) NOT NULL  COMMENT '告警名称',
  `alarm_type`  varchar(16) NOT NULL  COMMENT 'EMAIL/DINGTALK/WECOM/WEBHOOK',
  `config`      text        NOT NULL  COMMENT '配置JSON',
  `status`      tinyint     NOT NULL DEFAULT '1',
  `create_time` datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='告警配置表';

-- 调度统计表（每小时聚合）
CREATE TABLE IF NOT EXISTS `schedule_stat` (
  `id`            bigint   NOT NULL AUTO_INCREMENT,
  `stat_hour`     datetime NOT NULL              COMMENT '统计小时',
  `total_count`   int      NOT NULL DEFAULT '0',
  `success_count` int      NOT NULL DEFAULT '0',
  `fail_count`    int      NOT NULL DEFAULT '0',
  `timeout_count` int      NOT NULL DEFAULT '0',
  `create_time`   datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_stat_hour` (`stat_hour`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='调度统计';
