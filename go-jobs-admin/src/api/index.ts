// src/api/index.ts  – Axios-based API client for go-jobs-admin
import axios, { type AxiosInstance, type AxiosRequestConfig, type AxiosResponse } from 'axios'
import NProgress from 'nprogress'
import router from '@/router'

const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? '/api'

// ─── Response envelope matching Go server ─────────────────────────────────────
export interface ApiResponse<T = unknown> {
  code: number
  message: string
  data: T
}

export interface PageData<T> {
  list: T[]
  total: number
  page: number
  page_size: number
}

// ─── Create axios instance ─────────────────────────────────────────────────────
const http: AxiosInstance = axios.create({
  baseURL: BASE_URL,
  timeout: 30_000,
  headers: { 'Content-Type': 'application/json' }
})

// Request interceptor – attach token
http.interceptors.request.use(config => {
  NProgress.start()
  const token = localStorage.getItem('token')
  if (token) config.headers.Authorization = `Bearer ${token}`
  return config
})

// Response interceptor – unwrap envelope / handle errors
http.interceptors.response.use(
  (res: AxiosResponse<ApiResponse>) => {
    NProgress.done()
    const { code, message, data } = res.data
    if (code !== 0) {
      return Promise.reject(new Error(message ?? 'request failed'))
    }
    return data as any
  },
  err => {
    NProgress.done()
    if (err.response?.status === 401) {
      localStorage.removeItem('token')
      router.push('/login')
    }
    return Promise.reject(err)
  }
)

// ─── Type definitions ──────────────────────────────────────────────────────────
export interface LoginReq { username: string; password: string }
export interface LoginResp { token: string; expire_at: string; user: User }

export interface User {
  id: number
  username: string
  nickname: string
  email: string
  role: number
  status: number
}

export interface JobInfo {
  id: number
  executor_id: number
  executor_app: string
  job_name: string
  job_desc: string
  job_type: number
  cron_expression: string
  execute_type: string
  execute_param: string
  execute_handler: string
  route_strategy: string
  block_strategy: number
  timeout: number
  retry_count: number
  status: number
  next_trigger_time?: string
  last_trigger_time?: string
  create_user: string
  create_time: string
}

export interface JobLog {
  id: number
  job_id: number
  executor_address: string
  status: number
  error_msg: string
  trigger_time: string
  start_time?: string
  end_time?: string
  duration_ms: number
  trigger_type: number
}

export interface Executor {
  id: number
  app_name: string
  title: string
  address: string
  status: number
  heartbeat_time?: string
  version: string
}

// ─── Auth API ──────────────────────────────────────────────────────────────────
export const authApi = {
  login: (data: LoginReq) => http.post<any, LoginResp>('/login', data),
  me: () => http.get<any, User>('/user/me')
}

// ─── Job API ───────────────────────────────────────────────────────────────────
export interface JobListParams {
  page?: number
  page_size?: number
  job_name?: string
  executor_app?: string
  status?: number
}

export const jobApi = {
  list: (params: JobListParams) => http.get<any, PageData<JobInfo>>('/jobs', { params }),
  get: (id: number) => http.get<any, JobInfo>(`/jobs/${id}`),
  create: (data: Partial<JobInfo>) => http.post<any, JobInfo>('/jobs', data),
  update: (id: number, data: Partial<JobInfo>) => http.put<any, JobInfo>(`/jobs/${id}`, data),
  delete: (id: number) => http.delete(`/jobs/${id}`),
  start: (id: number) => http.post(`/jobs/${id}/start`),
  stop: (id: number) => http.post(`/jobs/${id}/stop`),
  trigger: (id: number, param?: string) => http.post(`/jobs/${id}/trigger`, { param }),
  logs: (id: number, params?: { page?: number; page_size?: number }) =>
    http.get<any, PageData<JobLog>>(`/jobs/${id}/logs`, { params })
}

// ─── Log API ───────────────────────────────────────────────────────────────────
export const logApi = {
  detail: (logID: number) => http.get(`/logs/${logID}/detail`),
  kill: (logID: number) => http.post(`/logs/${logID}/kill`)
}

// ─── Executor API ──────────────────────────────────────────────────────────────
export const executorApi = {
  list: (params?: { page?: number; page_size?: number }) =>
    http.get<any, PageData<Executor>>('/executors', { params })
}
