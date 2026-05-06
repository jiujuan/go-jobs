# go-jobs v1.0 — 核心可用版本

## 已实现功能

- ✅ 任务 CRUD（创建/编辑/删除/查询）
- ✅ 任务启动/停止/手动触发
- ✅ 执行器自动注册 + 心跳 + 健康清扫
- ✅ Redis 分布式锁（防止跨节点重复执行）
- ✅ Cron 表达式解析与调度（秒级精度）
- ✅ 路由策略：FIRST / LAST / ROUND_ROBIN / RANDOM / CONSISTENT_HASH / LFU / LRU
- ✅ BEAN / SHELL / CMD / PYTHON 执行模式
- ✅ JWT 用户认证
- ✅ 执行日志记录（MySQL）
- ✅ 前端管理界面（Vue3 + Vite + TailwindCSS）
- ✅ Docker Compose 一键部署

## 启动步骤

```bash
docker compose -f deploy/docker/docker-compose.yml up mysql redis -d
mysql -u root -ppassword go_jobs < migration/V1__init_schema.sql
go run ./cmd/admin -config config/config.yaml
go run ./cmd/executor -config config/executor.dev.yaml
cd ../go-jobs-admin && npm install && npm run dev
```

默认账号：admin / Admin@123
