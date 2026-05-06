# go-jobs-admin

go-jobs 分布式任务调度平台 Web 管理后台

## 技术栈

- Vite 5 + Vue 3 + TypeScript
- TailwindCSS 3
- Vue Router 4
- Pinia
- Axios

## 开发启动

```bash
# 安装依赖
npm install

# 启动开发服务器（代理到 http://127.0.0.1:8080）
npm run dev

# 访问
open http://localhost:3000
# 默认账号: admin / Admin@123
```

## 构建生产包

```bash
npm run build
# 产物在 dist/ 目录，部署到 nginx 即可
```

## 目录结构

```
src/
├── api/          index.ts       Axios 封装 + 全部 API 接口定义
├── components/
│   └── layout/   DefaultLayout.vue   侧边栏 + 顶栏布局
├── composables/  usePagination.ts    分页 Hook
├── router/       index.ts            Vue Router 路由配置
├── store/        user.ts             Pinia 用户状态
├── types/        index.ts            全局 TS 类型 + 常量
├── utils/        index.ts            时间格式化等工具函数
└── views/
    ├── dashboard/ Dashboard.vue      控制台仪表板
    ├── executor/  ExecutorList.vue   执行器管理
    ├── job/       JobList.vue        任务管理（CRUD+启停+触发）
    ├── log/       JobLogs.vue        执行日志查看
    └── user/      Login.vue          登录页
```
