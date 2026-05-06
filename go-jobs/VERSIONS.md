# go-jobs v3.0 — 生产级完整版

## 累积功能（含 v1.0 + v2.0 全部功能）

### v1.0 核心功能
- ✅ 任务 CRUD、启动/停止/手动触发
- ✅ 执行器自动注册 + 心跳
- ✅ Redis 分布式锁
- ✅ Cron 调度（秒级）
- ✅ 7 种路由策略
- ✅ BEAN / SHELL / CMD / PYTHON 执行
- ✅ JWT 认证 + 执行日志

### v2.0 高可用增强
- ✅ 失败重试（retry_count + retry_interval）
- ✅ 告警通知（邮件 / 钉钉 / 企业微信 / Webhook）
- ✅ 阻塞处理策略完整实现（串行 / 丢弃 / 覆盖）
- ✅ Misfire 补偿执行
- ✅ 调度统计聚合（每小时）

### v3.0 生产级特性
- ✅ **Etcd 选主**：多 scheduler 节点竞争 Leader，只有 Leader 运行调度循环
- ✅ **ES 日志存储**：执行日志写入 ElasticSearch，支持全文检索
- ✅ **子任务依赖**：父任务成功后自动触发 child_job_ids 中的子任务
- ✅ **任务分组管理**：job_group 表，支持分组过滤
- ✅ **FAILOVER 路由**：探测所有执行器，返回第一个健康节点
- ✅ **执行器资源上报**：executor 表新增 CPU / Memory 字段
- ✅ **多 admin 节点**：docker-compose 启动 2 个调度中心 + etcd 选主演示

## 启动步骤

```bash
# 一键启动全栈（含 etcd + elasticsearch）
docker compose -f deploy/docker/docker-compose.yml up -d

# 或手动启动各依赖
docker run -d --name etcd -p 2379:2379 \
  quay.io/coreos/etcd:v3.5.13 \
  etcd --listen-client-urls http://0.0.0.0:2379 \
       --advertise-client-urls http://127.0.0.1:2379

docker run -d --name es -p 9200:9200 \
  -e discovery.type=single-node \
  -e xpack.security.enabled=false \
  elasticsearch:8.13.0

# 初始化数据库（含全部 migration）
mysql -u root -ppassword go_jobs < migration/V1__init_schema.sql
mysql -u root -ppassword go_jobs < migration/V2__alarm_and_stats.sql
mysql -u root -ppassword go_jobs < migration/V3__child_job_and_metrics.sql

# 启动两个调度中心（通过 Etcd 选主）
GOJOBS_SERVER_PORT=8080 go run ./cmd/admin -config config/config.yaml &
GOJOBS_SERVER_PORT=8081 go run ./cmd/admin -config config/config.yaml &

# 启动执行器集群
GOJOBS_EXECUTOR_PORT=9901 go run ./cmd/executor -config config/executor.dev.yaml &
GOJOBS_EXECUTOR_PORT=9902 go run ./cmd/executor -config config/executor.dev.yaml &

# 前端开发
cd ../go-jobs-admin && npm install && npm run dev
```

## v3.0 新增 API

```
GET  /api/logs/search?keyword=xxx&job_id=1    # ES 全文搜索日志
GET  /api/groups                               # 任务分组列表
POST /api/groups                               # 创建分组
DELETE /api/groups/:id                         # 删除分组
GET  /api/stats/dashboard                      # 调度统计（24h 汇总）
GET  /health                                   # 节点状态（含 is_leader）
```

## 选主测试

```bash
# kill admin-1，观察 admin-2 成为 Leader 并继续调度
docker stop go-jobs-admin-1
# 约 15 秒后 admin-2 成为 Leader
docker logs go-jobs-admin-2 | grep "elected as leader"
```

默认账号：admin / Admin@123
