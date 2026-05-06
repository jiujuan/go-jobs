// Global shared TypeScript types for go-jobs-admin

export type UserRole = 1 | 2        // 1=admin 2=normal
export type UserStatus = 0 | 1      // 0=disabled 1=enabled
export type JobStatus = 0 | 1       // 0=stop 1=run
export type LogStatus = 0 | 1 | 2 | 3 | 4 | 5
export type ExecutorStatus = 0 | 1  // 0=offline 1=online

export const LOG_STATUS_MAP: Record<number, string> = {
  0: '初始化', 1: '成功', 2: '失败', 3: '进行中', 4: '超时', 5: '已终止'
}

export const ROUTE_STRATEGY_OPTIONS = [
  { value: 'ROUND_ROBIN',        label: '轮询' },
  { value: 'RANDOM',             label: '随机' },
  { value: 'FIRST',              label: '第一个' },
  { value: 'LAST',               label: '最后一个' },
  { value: 'CONSISTENT_HASH',    label: '一致性哈希' },
  { value: 'LFU',                label: '最不常用 (LFU)' },
  { value: 'LRU',                label: '最近最少使用 (LRU)' },
  { value: 'FAILOVER',           label: 'Failover 故障转移' },
  { value: 'SHARDING_BROADCAST', label: '分片广播' },
]

export const EXECUTE_TYPE_OPTIONS = [
  { value: 'BEAN',   label: 'BEAN (Go Handler)' },
  { value: 'SHELL',  label: 'SHELL 脚本' },
  { value: 'PYTHON', label: 'Python 脚本' },
  { value: 'CMD',    label: '命令行' },
]

export const BLOCK_STRATEGY_OPTIONS = [
  { value: 1, label: '串行（等待上次完成）' },
  { value: 2, label: '丢弃后续触发' },
  { value: 3, label: '覆盖之前（终止旧的）' },
]
