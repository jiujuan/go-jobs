-- ============================================================
-- go-jobs: 分布式任务调度平台 - 完整数据库 Schema
-- 版本: v1.0
-- 数据库: MySQL 8.0+
-- 字符集: utf8mb4_unicode_ci
-- ============================================================

CREATE DATABASE IF NOT EXISTS `go_jobs` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE `go_jobs`;

-- ============================================================
-- 1. 执行器信息表 sys_executor
--    执行器集群注册、心跳、节点管理
-- ============================================================
CREATE TABLE IF NOT EXISTS `sys_executor` (
  `id`             bigint       NOT NULL AUTO_INCREMENT COMMENT '主键',
  `app_name`       varchar(64)  NOT NULL                COMMENT '应用名称（执行器唯一标识）',
  `title`          varchar(64)  DEFAULT NULL            COMMENT '执行器别名（展示用）',
  `address`        varchar(128) NOT NULL                COMMENT '地址 ip:port',
  `register_type`  tinyint      NOT NULL DEFAULT '0'    COMMENT '注册类型 0=自动注册 1=手动录入',
  `status`         tinyint      NOT NULL DEFAULT '1'    COMMENT '状态 0=离线 1=在线',
  `weight`         int          NOT NULL DEFAULT '1'    COMMENT '权重（用于加权路由）',
  `heartbeat_time` datetime     DEFAULT NULL            COMMENT '最后心跳时间',
  `version`        varchar(32)  DEFAULT ''              COMMENT '执行器版本号',
  `tags`           varchar(255) DEFAULT ''              COMMENT '标签，逗号分隔',
  `create_time`    datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`    datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_app_address` (`app_name`, `address`),
  KEY `idx_app_name`  (`app_name`),
  KEY `idx_address`   (`address`),
  KEY `idx_status`    (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='执行器信息表';

-- ============================================================
-- 2. 任务信息表 job_info
--    核心：任务配置、cron、调度策略
-- ============================================================
CREATE TABLE IF NOT EXISTS `job_info` (
  `id`               bigint       NOT NULL AUTO_INCREMENT COMMENT '任务ID',
  `executor_id`      bigint       NOT NULL                COMMENT '执行器ID',
  `executor_app`     varchar(64)  NOT NULL                COMMENT '执行器appName',
  `job_name`         varchar(128) NOT NULL                COMMENT '任务名称',
  `job_desc`         varchar(255) DEFAULT ''              COMMENT '任务描述',
  `job_type`         tinyint      NOT NULL DEFAULT '1'    COMMENT '任务类型 1=CRON 2=延时 3=一次性 4=分片广播',
  `cron_expression`  varchar(64)  DEFAULT ''              COMMENT 'cron表达式',
  `execute_type`     varchar(16)  NOT NULL DEFAULT 'BEAN' COMMENT '执行模式 BEAN/SHELL/CMD/PYTHON',
  `execute_param`    varchar(512) DEFAULT ''              COMMENT '执行参数（JSON格式）',
  `execute_handler`  varchar(255) NOT NULL                COMMENT '执行器中注册的handler名称',
  `route_strategy`   varchar(32)  NOT NULL DEFAULT 'ROUND_ROBIN' COMMENT '路由策略 FIRST/LAST/ROUND_ROBIN/RANDOM/CONSISTENT_HASH/LFU/LRU/FAILOVER/BUSY_TRANSFER/SHARDING_BROADCAST',
  `block_strategy`   tinyint      NOT NULL DEFAULT '1'    COMMENT '阻塞处理策略 1=串行 2=丢弃后续 3=覆盖之前',
  `misfire_strategy` tinyint      NOT NULL DEFAULT '1'    COMMENT '调度过期策略 1=忽略 2=立即执行一次',
  `timeout`          int          NOT NULL DEFAULT '0'    COMMENT '超时时间(秒) 0=不限制',
  `retry_count`      int          NOT NULL DEFAULT '0'    COMMENT '失败重试次数',
  `retry_interval`   int          NOT NULL DEFAULT '0'    COMMENT '重试间隔(秒)',
  `sharding_num`     int          NOT NULL DEFAULT '1'    COMMENT '分片数量（分片广播任务用）',
  `alarm_email`      varchar(255) DEFAULT ''              COMMENT '告警邮箱，逗号分隔',
  `alarm_webhook`    varchar(255) DEFAULT ''              COMMENT '告警Webhook URL',
  `status`           tinyint      NOT NULL DEFAULT '1'    COMMENT '状态 0=停止 1=运行',
  `next_trigger_time`datetime     DEFAULT NULL            COMMENT '下次触发时间',
  `last_trigger_time`datetime     DEFAULT NULL            COMMENT '上次触发时间',
  `create_user`      varchar(32)  DEFAULT 'admin'         COMMENT '创建人',
  `create_time`      datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time`      datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_executor_id`       (`executor_id`),
  KEY `idx_status`            (`status`),
  KEY `idx_next_trigger_time` (`next_trigger_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='任务信息表';

-- ============================================================
-- 3. 任务调度日志表 job_log
--    记录每次调度是否下发成功，每次执行产生一条
-- ============================================================
CREATE TABLE IF NOT EXISTS `job_log` (
  `id`               bigint       NOT NULL AUTO_INCREMENT COMMENT '日志ID',
  `job_id`           bigint       NOT NULL                COMMENT '任务ID',
  `executor_id`      bigint       NOT NULL                COMMENT '执行器ID',
  `executor_address` varchar(128) DEFAULT ''              COMMENT '实际执行的执行器地址',
  `execute_param`    varchar(512) DEFAULT ''              COMMENT '本次执行参数',
  `status`           tinyint      NOT NULL DEFAULT '0'    COMMENT '状态 0=初始化 1=成功 2=失败 3=进行中 4=超时 5=手动终止',
  `error_msg`        varchar(512) DEFAULT ''              COMMENT '错误信息',
  `sharding_index`   int          DEFAULT '0'             COMMENT '分片序号',
  `sharding_total`   int          DEFAULT '0'             COMMENT '分片总数',
  `trigger_time`     datetime     NOT NULL                COMMENT '调度触发时间',
  `start_time`       datetime     DEFAULT NULL            COMMENT '执行器开始执行时间',
  `end_time`         datetime     DEFAULT NULL            COMMENT '执行器完成时间',
  `duration_ms`      bigint       DEFAULT '0'             COMMENT '执行耗时(毫秒)',
  `trigger_type`     tinyint      NOT NULL DEFAULT '1'    COMMENT '触发类型 1=cron 2=手动 3=重试 4=父任务触发',
  `create_time`      datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间',
  PRIMARY KEY (`id`),
  KEY `idx_job_id`       (`job_id`),
  KEY `idx_executor_id`  (`executor_id`),
  KEY `idx_status`       (`status`),
  KEY `idx_trigger_time` (`trigger_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='任务调度日志表';

-- ============================================================
-- 4. 执行日志详情表 job_log_detail
--    存放执行器上报的详细日志文本
-- ============================================================
CREATE TABLE IF NOT EXISTS `job_log_detail` (
  `id`           bigint    NOT NULL AUTO_INCREMENT,
  `log_id`       bigint    NOT NULL  COMMENT '对应 job_log.id',
  `job_id`       bigint    NOT NULL  COMMENT '任务ID',
  `log_content`  longtext            COMMENT '日志内容（执行器上报的完整日志）',
  `create_time`  datetime  NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_log_id` (`log_id`),
  KEY `idx_job_id` (`job_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='任务执行日志详情';

-- ============================================================
-- 5. 分布式锁表 job_lock
--    调度中心集群抢锁，保证同一任务同一时间只调度一次
-- ============================================================
CREATE TABLE IF NOT EXISTS `job_lock` (
  `id`          bigint      NOT NULL AUTO_INCREMENT,
  `lock_key`    varchar(128) NOT NULL COMMENT '锁名（通常为 job:{id}）',
  `lock_until`  datetime    NOT NULL COMMENT '锁到期时间',
  `lock_node`   varchar(128) NOT NULL COMMENT '持锁节点标识（ip:port）',
  `create_time` datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '加锁时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_lock_key` (`lock_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='分布式锁表';

-- ============================================================
-- 6. 系统用户表 sys_user
--    Web 后台登录与权限控制
-- ============================================================
CREATE TABLE IF NOT EXISTS `sys_user` (
  `id`          bigint      NOT NULL AUTO_INCREMENT,
  `username`    varchar(32) NOT NULL                COMMENT '用户名（唯一）',
  `password`    varchar(128) NOT NULL               COMMENT '密码（bcrypt hash）',
  `nickname`    varchar(64)  DEFAULT ''             COMMENT '昵称',
  `email`       varchar(128) DEFAULT ''             COMMENT '邮箱',
  `role`        tinyint      NOT NULL DEFAULT '2'   COMMENT '角色 1=管理员 2=普通用户',
  `status`      tinyint      NOT NULL DEFAULT '1'   COMMENT '状态 0=禁用 1=启用',
  `last_login`  datetime     DEFAULT NULL           COMMENT '最后登录时间',
  `create_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='系统用户表';

-- ============================================================
-- 7. 告警配置表 alarm_config
--    支持邮件/钉钉/企业微信/Webhook
-- ============================================================
CREATE TABLE IF NOT EXISTS `alarm_config` (
  `id`           bigint      NOT NULL AUTO_INCREMENT,
  `name`         varchar(64) NOT NULL  COMMENT '告警名称',
  `alarm_type`   varchar(16) NOT NULL  COMMENT '告警类型 EMAIL/DINGTALK/WECOM/WEBHOOK',
  `config`       text        NOT NULL  COMMENT '告警配置（JSON格式）',
  `status`       tinyint     NOT NULL DEFAULT '1' COMMENT '状态 0=禁用 1=启用',
  `create_time`  datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time`  datetime    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='告警配置表';

-- ============================================================
-- 8. 调度统计表 schedule_stat (每小时聚合)
-- ============================================================
CREATE TABLE IF NOT EXISTS `schedule_stat` (
  `id`             bigint   NOT NULL AUTO_INCREMENT,
  `stat_hour`      datetime NOT NULL              COMMENT '统计小时（精确到小时）',
  `total_count`    int      NOT NULL DEFAULT '0'  COMMENT '总调度次数',
  `success_count`  int      NOT NULL DEFAULT '0'  COMMENT '成功次数',
  `fail_count`     int      NOT NULL DEFAULT '0'  COMMENT '失败次数',
  `timeout_count`  int      NOT NULL DEFAULT '0'  COMMENT '超时次数',
  `create_time`    datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_stat_hour` (`stat_hour`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='调度统计表';

-- ============================================================
-- 初始化数据
-- ============================================================

-- 初始管理员账号 admin/Admin@123 (bcrypt hash)
INSERT INTO `sys_user` (`username`, `password`, `nickname`, `role`, `status`)
VALUES ('admin', '$2a$10$N.zmdr9k7uOCQb376NoUnuTJ8iAt6Z5EHsM8lE9lBpwTTyK.GHB8K', 'Administrator', 1, 1);

-- 示例执行器
INSERT INTO `sys_executor` (`app_name`, `title`, `address`, `register_type`, `status`)
VALUES ('go-jobs-example-executor', '示例执行器', '127.0.0.1:9901', 0, 0);
