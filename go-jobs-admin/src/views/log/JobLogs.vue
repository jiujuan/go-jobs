<template>
  <div class="space-y-4">
    <div class="flex items-center gap-3">
      <button @click="$router.back()" class="text-gray-400 hover:text-gray-600 transition text-lg">←</button>
      <h1 class="text-xl font-bold text-gray-800">执行日志 · Job #{{ jobId }}</h1>
    </div>

    <div class="bg-white rounded-xl border border-gray-100 shadow-sm overflow-hidden">
      <table class="w-full text-sm">
        <thead class="bg-gray-50 text-gray-500 border-b border-gray-100">
          <tr>
            <th class="px-4 py-3 text-left font-medium">日志ID</th>
            <th class="px-4 py-3 text-left font-medium">执行器</th>
            <th class="px-4 py-3 text-left font-medium">状态</th>
            <th class="px-4 py-3 text-left font-medium">触发时间</th>
            <th class="px-4 py-3 text-left font-medium">耗时</th>
            <th class="px-4 py-3 text-left font-medium">错误信息</th>
            <th class="px-4 py-3 text-left font-medium">操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-if="loading">
            <td colspan="7" class="py-10 text-center text-gray-400">加载中...</td>
          </tr>
          <tr v-else-if="!logs.length">
            <td colspan="7" class="py-10 text-center text-gray-400">暂无日志</td>
          </tr>
          <tr v-for="log in logs" :key="log.id" class="border-b border-gray-50 hover:bg-gray-50">
            <td class="px-4 py-3 font-mono text-xs text-gray-400">{{ log.id }}</td>
            <td class="px-4 py-3 font-mono text-xs text-gray-500">{{ log.executor_address || '-' }}</td>
            <td class="px-4 py-3">
              <span :class="statusClass(log.status)" class="px-2 py-0.5 rounded-full text-xs font-medium">
                {{ statusLabel(log.status) }}
              </span>
            </td>
            <td class="px-4 py-3 text-xs text-gray-500">{{ fmtTime(log.trigger_time) }}</td>
            <td class="px-4 py-3 text-xs text-gray-500">
              {{ log.duration_ms ? log.duration_ms + 'ms' : '-' }}
            </td>
            <td class="px-4 py-3 text-xs text-red-500 max-w-xs truncate">{{ log.error_msg || '-' }}</td>
            <td class="px-4 py-3">
              <div class="flex gap-1.5">
                <button
                  @click="viewDetail(log.id)"
                  class="px-2 py-1 text-xs bg-blue-50 text-blue-600 rounded hover:bg-blue-100"
                >详情</button>
                <button
                  v-if="log.status === 3"
                  @click="killLog(log.id)"
                  class="px-2 py-1 text-xs bg-red-50 text-red-500 rounded hover:bg-red-100"
                >终止</button>
              </div>
            </td>
          </tr>
        </tbody>
      </table>
      <div class="flex items-center justify-between px-4 py-3 border-t border-gray-100 text-sm text-gray-500">
        <span>共 {{ total }} 条</span>
        <div class="flex gap-1">
          <button v-for="p in pages" :key="p" @click="page = p; fetchLogs()"
            :class="p === page ? 'bg-blue-600 text-white' : 'bg-white text-gray-600 hover:bg-gray-50'"
            class="w-8 h-8 rounded border border-gray-200 text-xs">{{ p }}</button>
        </div>
      </div>
    </div>

    <!-- Log detail modal -->
    <div v-if="detailVisible" class="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div class="bg-white rounded-2xl shadow-2xl w-full max-w-3xl max-h-[80vh] overflow-y-auto p-6">
        <div class="flex items-center justify-between mb-4">
          <h3 class="font-bold text-gray-800">日志详情</h3>
          <button @click="detailVisible = false" class="text-gray-400 hover:text-gray-600 text-xl">×</button>
        </div>
        <pre class="bg-gray-900 text-green-400 rounded-lg p-4 text-xs font-mono whitespace-pre-wrap overflow-auto max-h-96">{{ detailContent || '暂无日志内容' }}</pre>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { jobApi, logApi, type JobLog } from '@/api'
import dayjs from 'dayjs'

const route = useRoute()
const jobId = Number(route.params.id)

const logs = ref<JobLog[]>([])
const total = ref(0)
const page = ref(1)
const loading = ref(false)
const detailVisible = ref(false)
const detailContent = ref('')

const pages = computed(() => Array.from({ length: Math.min(Math.ceil(total.value / 20), 10) }, (_, i) => i + 1))

const fmtTime = (t: string) => dayjs(t).format('MM-DD HH:mm:ss')

const statusLabel = (s: number) => ({ 0: '初始化', 1: '成功', 2: '失败', 3: '进行中', 4: '超时', 5: '已终止' }[s] ?? '-')
const statusClass = (s: number) => ({
  0: 'bg-gray-100 text-gray-500',
  1: 'bg-green-100 text-green-700',
  2: 'bg-red-100 text-red-600',
  3: 'bg-blue-100 text-blue-600',
  4: 'bg-yellow-100 text-yellow-600',
  5: 'bg-gray-100 text-gray-500'
}[s] ?? 'bg-gray-100 text-gray-500')

async function fetchLogs() {
  loading.value = true
  try {
    const data = await jobApi.logs(jobId, { page: page.value, page_size: 20 })
    logs.value = data.list
    total.value = data.total
  } catch {}
  loading.value = false
}

async function viewDetail(logID: number) {
  try {
    const data: any = await logApi.detail(logID)
    detailContent.value = data?.log_content ?? '暂无内容'
  } catch {
    detailContent.value = '加载失败'
  }
  detailVisible.value = true
}

async function killLog(logID: number) {
  if (!confirm('确认终止该执行？')) return
  try { await logApi.kill(logID); await fetchLogs() } catch (e: any) { alert(e.message) }
}

onMounted(fetchLogs)
</script>
