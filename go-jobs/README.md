# go-jobs — 分布式任务调度平台

> 类 xxl-job 的 Go 实现，企业级分布式任务调度系统

[![Go Version](https://img.shields.io/badge/Go-1.21+-blue.svg)](https://go.dev/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## 架构概览

```
go-jobs-admin (Vue3 Web UI)
         │
         ▼
┌─────────────────────────────┐
│   调度中心 (Admin/Scheduler) │  ←→  MySQL + Redis + Etcd
│   cmd/admin/main.go         │
└────────────┬────────────────┘
             │ HTTP 触发
    ┌─────────┼─────────┐
    ▼         ▼         ▼
 Executor  Executor  Executor   (水平扩展)
  :9901     :9902     :9903
    │         │         │
    └────执行任务 & 上报日志────┘
```

---

## 技术栈

| 层次 | 技术 |
|------|------|
| 语言 | Go 1.21+ |
| Web 框架 | Gin |
| 配置 | Viper (functional options) |
| 日志 | Zap + lumberjack |
| ORM | GORM |
| 数据库 | MySQL 8.0 |
| 缓存/锁 | Redis (go-redis) |
| 注册发现 | Etcd |
| 日志检索 | ElasticSearch (v3.0可选) |
| 定时解析 | cron/v3 |
| 前端 | Vite + Vue3 + TypeScript + TailwindCSS |

---

## 快速启动

### 1. 依赖服务

```bash
# 启动 MySQL + Redis
docker compose -f deploy/docker/docker-compose.yml up mysql redis -d
```

### 2. 初始化数据库

```bash
mysql -u root -ppassword go_jobs < migration/V1__init_schema.sql
mysql -u root -ppassword go_jobs < migration/V2__alarm_and_stats.sql
```

### 3. 启动调度中心

```bash
go run ./cmd/admin -config config/config.yaml
# 访问: http://localhost:8080/health
```

### 4. 启动执行器

```bash
go run ./cmd/executor -config config/executor.dev.yaml
# 执行器会自动注册到调度中心
```

### 5. 启动前端

```bash
cd go-jobs-admin
npm install
npm run dev
# 访问: http://localhost:3000
# 账号: admin / Admin@123
```

---

## 版本规划

| 版本 | 功能 | 状态 |
|------|------|------|
| v1.0 | 核心调度引擎 + 基础 CRUD + BEAN/SHELL 执行 | ✅ 完成 |
| v2.0 | 分片广播 + 告警 + 失败重试 + 执行器健康检测 | 🔨 进行中 |
| v3.0 | Etcd 选主 + ES 日志 + 监控统计 + 任务依赖 | 📋 规划中 |

详见 [VERSIONS.md](VERSIONS.md)

---

## 目录结构

```
go-jobs/
├── cmd/
│   ├── admin/main.go          # 调度中心启动入口
│   └── executor/main.go       # 执行器启动入口
├── api/
│   ├── handler/
│   │   ├── admin/handlers.go  # 管理后台 HTTP handlers
│   │   └── executor/          # 执行器接收调度的 handlers
│   ├── middleware/middleware.go
│   ├── response/response.go
│   └── router.go
├── internal/
│   ├── model/model.go         # 全部 DB 模型
│   ├── dao/dao.go             # 数据访问层（接口+实现）
│   ├── service/               # 业务逻辑层
│   ├── scheduler/             # 调度核心引擎
│   └── executor/              # 执行器逻辑
├── pkg/
│   ├── conf/conf.go           # Viper 配置（functional options）
│   ├── logger/logger.go       # Zap 日志（functional options）
│   ├── mysql/mysql.go         # GORM 连接（functional options）
│   ├── redis/redis.go         # Redis 客户端 + 分布式锁
│   ├── utils/utils.go         # 通用工具
│   └── xerror/xerror.go       # 统一错误码
├── migration/
│   ├── schema.sql             # 完整建表 SQL
│   ├── V1__init_schema.sql
│   ├── V2__alarm_and_stats.sql
│   └── V3__child_job_and_metrics.sql
├── config/
│   ├── config.yaml            # 调度中心配置
│   └── executor.dev.yaml      # 执行器配置
├── deploy/docker/
│   ├── docker-compose.yml
│   ├── Dockerfile.admin
│   ├── Dockerfile.executor
│   └── nginx.conf
└── go-jobs-admin/             # Vue3 前端
    ├── src/
    │   ├── api/index.ts       # Axios API client
    │   ├── router/
    │   ├── views/
    │   └── components/
    └── package.json
```
