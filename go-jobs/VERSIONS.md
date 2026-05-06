# go-jobs v2.0 — 高可用增强版

## 累积功能（含 v1.0 全部功能）

### v1.0 核心功能
- ✅ 任务 CRUD、启动/停止/手动触发
- ✅ 执行器自动注册 + 心跳
- ✅ Redis 分布式锁
- ✅ Cron 调度（秒级）
- ✅ 7 种路由策略
- ✅ BEAN / SHELL / CMD / PYTHON 执行
- ✅ JWT 认证 + 执行日志

### v2.0 新增功能
- ✅ **失败重试**：job_info.retry_count + retry_interval，30s 扫一次失败日志
- ✅ **告警通知**：EmailAlarmer / DingtalkAlarmer / WeComAlarmer / WebhookAlarmer
- ✅ **阻塞处理策略**：BlockSerial（串行）/ BlockDiscard（丢弃）/ BlockOverride（覆盖）
- ✅ **Misfire 补偿**：MisfireIgnore（忽略）/ MisfireRunOnce（立即补跑一次）
- ✅ **调度统计**：每小时聚合到 schedule_stat 表，`GET /api/stats/dashboard` 查看

## 启动步骤

```bash
docker compose -f deploy/docker/docker-compose.yml up mysql redis -d
mysql -u root -ppassword go_jobs < migration/V1__init_schema.sql
mysql -u root -ppassword go_jobs < migration/V2__alarm_and_stats.sql
go run ./cmd/admin -config config/config.yaml
go run ./cmd/executor -config config/executor.dev.yaml
cd ../go-jobs-admin && npm install && npm run dev
```

默认账号：admin / Admin@123
