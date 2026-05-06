import { createRouter, createWebHistory, type RouteRecordRaw } from 'vue-router'

const routes: RouteRecordRaw[] = [
  {
    path: '/login',
    name: 'Login',
    component: () => import('@/views/user/Login.vue'),
    meta: { requiresAuth: false }
  },
  {
    path: '/',
    component: () => import('@/components/layout/DefaultLayout.vue'),
    meta: { requiresAuth: true },
    redirect: '/dashboard',
    children: [
      {
        path: 'dashboard',
        name: 'Dashboard',
        component: () => import('@/views/dashboard/Dashboard.vue'),
        meta: { title: '控制台' }
      },
      {
        path: 'jobs',
        name: 'Jobs',
        component: () => import('@/views/job/JobList.vue'),
        meta: { title: '任务管理' }
      },
      {
        path: 'jobs/:id/logs',
        name: 'JobLogs',
        component: () => import('@/views/log/JobLogs.vue'),
        meta: { title: '执行日志' }
      },
      {
        path: 'executors',
        name: 'Executors',
        component: () => import('@/views/executor/ExecutorList.vue'),
        meta: { title: '执行器管理' }
      }
    ]
  },
  { path: '/:pathMatch(.*)*', redirect: '/' }
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

router.beforeEach((to, _from, next) => {
  const token = localStorage.getItem('token')
  if (to.meta.requiresAuth !== false && !token) {
    next('/login')
  } else {
    next()
  }
})

export default router
