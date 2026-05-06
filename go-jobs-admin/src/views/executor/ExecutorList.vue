<template>
  <div class="space-y-4">
    <h1 class="text-xl font-bold text-gray-800">执行器管理</h1>

    <div class="bg-white rounded-xl border border-gray-100 shadow-sm overflow-hidden">
      <table class="w-full text-sm">
        <thead class="bg-gray-50 text-gray-500 border-b border-gray-100">
          <tr>
            <th class="px-4 py-3 text-left font-medium">ID</th>
            <th class="px-4 py-3 text-left font-medium">应用名</th>
            <th class="px-4 py-3 text-left font-medium">别名</th>
            <th class="px-4 py-3 text-left font-medium">地址</th>
            <th class="px-4 py-3 text-left font-medium">版本</th>
            <th class="px-4 py-3 text-left font-medium">注册方式</th>
            <th class="px-4 py-3 text-left font-medium">状态</th>
            <th class="px-4 py-3 text-left font-medium">最后心跳</th>
          </tr>
        </thead>
        <tbody>
          <tr v-if="loading">
            <td colspan="8" class="py-10 text-center text-gray-400">加载中...</td>
          </tr>
          <tr v-else-if="!list.length">
            <td colspan="8" class="py-10 text-center text-gray-400">暂无执行器</td>
          </tr>
          <tr v-for="e in list" :key="e.id" class="border-b border-gray-50 hover:bg-gray-50 transition">
            <td class="px-4 py-3 text-gray-400 font-mono text-xs">{{ e.id }}</td>
            <td class="px-4 py-3 font-medium text-gray-800">{{ e.app_name }}</td>
            <td class="px-4 py-3 text-gray-500 text-xs">{{ e.title }}</td>
            <td class="px-4 py-3 font-mono text-xs text-blue-600">{{ e.address }}</td>
            <td class="px-4 py-3 text-xs text-gray-400">{{ e.version || '-' }}</td>
            <td class="px-4 py-3 text-xs text-gray-500">{{ e.register_type === 0 ? '自动注册' : '手动录入' }}</td>
            <td class="px-4 py-3">
              <div class="flex items-center gap-1.5">
                <span
                  :class="e.status === 1 ? 'bg-green-500' : 'bg-gray-300'"
                  class="w-2 h-2 rounded-full inline-block"
                ></span>
                <span
                  :class="e.status === 1 ? 'text-green-700' : 'text-gray-400'"
                  class="text-xs font-medium"
                >{{ e.status === 1 ? '在线' : '离线' }}</span>
              </div>
            </td>
            <td class="px-4 py-3 text-xs text-gray-400">
              {{ e.heartbeat_time ? fmtTime(e.heartbeat_time) : '-' }}
            </td>
          </tr>
        </tbody>
      </table>

      <div class="flex items-center justify-between px-4 py-3 border-t border-gray-100 text-sm text-gray-500">
        <span>共 {{ total }} 个执行器节点</span>
        <button @click="fetchList" class="text-blue-500 hover:text-blue-700 text-xs">刷新</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'
import { executorApi, type Executor } from '@/api'
import dayjs from 'dayjs'

const list = ref<Executor[]>([])
const total = ref(0)
const loading = ref(false)

const fmtTime = (t: string) => dayjs(t).format('MM-DD HH:mm:ss')

async function fetchList() {
  loading.value = true
  try {
    const data = await executorApi.list({ page: 1, page_size: 100 })
    list.value = data.list
    total.value = data.total
  } catch {}
  loading.value = false
}

// Auto-refresh every 15 seconds to show real-time heartbeat status
const timer = setInterval(fetchList, 15_000)
onMounted(fetchList)
onUnmounted(() => clearInterval(timer))
</script>
