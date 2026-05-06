-- ============================================================
-- Migration: V1__init_schema.sql
-- 描述: 初始化 go-jobs 数据库表结构
-- 版本: v1.0.0
-- 时间: 2024-01-01
-- ============================================================

-- 执行器信息表
CREATE TABLE IF NOT EXISTS `sys_executor` (
  `id`             bigint       NOT NULL AUTO_INCREMENT COMMENT '主键',
  `app_name`       varchar(64)  NOT NULL                COMMENT '应用名称',
  `title`          varchar(64)  DEFAULT NULL            COMMENT '执行器名称',
  `address`        varchar(128) NOT NULL                COMMENT '地址 ip:port',
  `register_type`  tinyint      NOT NULL DEFAULT '0'    COMMENT '注册类型 0=自动 1=手动',
  `status`         tinyint      NOT NULL DEFAULT '1'    COMMENT '状态 0=离线 1=在线',
  `weight`         int          NOT NULL DEFAULT '1'    COMMENT '权重',
  `heartbeat_time` datetime     DEFAULT NULL            COMMENT '最后心跳时间',
  `version`        varchar(32)  DEFAULT ''              COMMENT '版本号',
  `tags`           varchar(255) DEFAULT ''              COMMENT '标签',
  `create_time`    datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`    datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_app_address` (`app_name`, `address`),
  KEY `idx_app_name` (`app_name`),
  KEY `idx_status`   (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='执行器信息表';

-- 任务信息表
CREATE TABLE IF NOT EXISTS `job_info` (
  `id`               bigint       NOT NULL AUTO_INCREMENT COMMENT '任务ID',
  `executor_id`      bigint       NOT NULL,
  `executor_app`     varchar(64)  NOT NULL,
  `job_name`         varchar(128) NOT NULL,
  `job_desc`         varchar(255) DEFAULT '',
  `job_type`         tinyint      NOT NULL DEFAULT '1'    COMMENT '1=CRON 2=延时 3=一次性 4=分片广播',
  `cron_expression`  varchar(64)  DEFAULT '',
  `execute_type`     varchar(16)  NOT NULL DEFAULT 'BEAN',
  `execute_param`    varchar(512) DEFAULT '',
  `execute_handler`  varchar(255) NOT NULL,
  `route_strategy`   varchar(32)  NOT NULL DEFAULT 'ROUND_ROBIN',
  `block_strategy`   tinyint      NOT NULL DEFAULT '1'    COMMENT '1=串行 2=丢弃 3=覆盖',
  `misfire_strategy` tinyint      NOT NULL DEFAULT '1',
  `timeout`          int          NOT NULL DEFAULT '0',
  `retry_count`      int          NOT NULL DEFAULT '0',
  `retry_interval`   int          NOT NULL DEFAULT '0',
  `sharding_num`     int          NOT NULL DEFAULT '1',
  `alarm_email`      varchar(255) DEFAULT '',
  `alarm_webhook`    varchar(255) DEFAULT '',
  `status`           tinyint      NOT NULL DEFAULT '1'    COMMENT '0=停止 1=运行',
  `next_trigger_time`datetime     DEFAULT NULL,
  `last_trigger_time`datetime     DEFAULT NULL,
  `create_user`      varchar(32)  DEFAULT 'admin',
  `create_time`      datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`      datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_executor_id`       (`executor_id`),
  KEY `idx_status`            (`status`),
  KEY `idx_next_trigger_time` (`next_trigger_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='任务信息表';

-- 调度日志表
CREATE TABLE IF NOT EXISTS `job_log` (
  `id`               bigint       NOT NULL AUTO_INCREMENT,
  `job_id`           bigint       NOT NULL,
  `executor_id`      bigint       NOT NULL,
  `executor_address` varchar(128) DEFAULT '',
  `execute_param`    varchar(512) DEFAULT '',
  `status`           tinyint      NOT NULL DEFAULT '0'    COMMENT '0=初始 1=成功 2=失败 3=进行中 4=超时 5=终止',
  `error_msg`        varchar(512) DEFAULT '',
  `sharding_index`   int          DEFAULT '0',
  `sharding_total`   int          DEFAULT '0',
  `trigger_time`     datetime     NOT NULL,
  `start_time`       datetime     DEFAULT NULL,
  `end_time`         datetime     DEFAULT NULL,
  `duration_ms`      bigint       DEFAULT '0',
  `trigger_type`     tinyint      NOT NULL DEFAULT '1'    COMMENT '1=cron 2=手动 3=重试',
  `create_time`      datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_job_id`       (`job_id`),
  KEY `idx_status`       (`status`),
  KEY `idx_trigger_time` (`trigger_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='任务调度日志表';

-- 执行日志详情表
CREATE TABLE IF NOT EXISTS `job_log_detail` (
  `id`          bigint   NOT NULL AUTO_INCREMENT,
  `log_id`      bigint   NOT NULL,
  `job_id`      bigint   NOT NULL,
  `log_content` longtext,
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_log_id` (`log_id`),
  KEY `idx_job_id` (`job_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='执行日志详情';

-- 分布式锁表
CREATE TABLE IF NOT EXISTS `job_lock` (
  `id`          bigint       NOT NULL AUTO_INCREMENT,
  `lock_key`    varchar(128) NOT NULL,
  `lock_until`  datetime     NOT NULL,
  `lock_node`   varchar(128) NOT NULL,
  `create_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_lock_key` (`lock_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='分布式锁表';

-- 系统用户表
CREATE TABLE IF NOT EXISTS `sys_user` (
  `id`          bigint       NOT NULL AUTO_INCREMENT,
  `username`    varchar(32)  NOT NULL,
  `password`    varchar(128) NOT NULL,
  `nickname`    varchar(64)  DEFAULT '',
  `email`       varchar(128) DEFAULT '',
  `role`        tinyint      NOT NULL DEFAULT '2'  COMMENT '1=管理员 2=普通用户',
  `status`      tinyint      NOT NULL DEFAULT '1',
  `last_login`  datetime     DEFAULT NULL,
  `create_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='系统用户表';

-- 初始化管理员 admin/Admin@123
INSERT IGNORE INTO `sys_user` (`username`, `password`, `nickname`, `role`)
VALUES ('admin', '$2a$10$N.zmdr9k7uOCQb376NoUnuTJ8iAt6Z5EHsM8lE9lBpwTTyK.GHB8K', 'Administrator', 1);
