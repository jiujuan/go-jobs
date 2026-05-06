<template>
  <div class="space-y-6">
    <h1 class="text-xl font-bold text-gray-800">控制台</h1>

    <!-- Stats cards -->
    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      <StatCard v-for="s in stats" :key="s.label" v-bind="s" />
    </div>

    <!-- Recent logs -->
    <div class="bg-white rounded-xl shadow-sm border border-gray-100 p-5">
      <h3 class="text-sm font-semibold text-gray-700 mb-4">执行器在线状态</h3>
      <table class="w-full text-sm">
        <thead>
          <tr class="text-left text-gray-400 border-b border-gray-100">
            <th class="pb-2 pr-4">应用名</th>
            <th class="pb-2 pr-4">地址</th>
            <th class="pb-2 pr-4">状态</th>
            <th class="pb-2">心跳时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="e in executors" :key="e.id" class="border-b border-gray-50 hover:bg-gray-50">
            <td class="py-2 pr-4 font-medium">{{ e.app_name }}</td>
            <td class="py-2 pr-4 text-gray-500 font-mono text-xs">{{ e.address }}</td>
            <td class="py-2 pr-4">
              <span :class="e.status === 1 ? 'text-green-600 bg-green-50' : 'text-gray-400 bg-gray-100'"
                class="px-2 py-0.5 rounded-full text-xs font-medium">
                {{ e.status === 1 ? '在线' : '离线' }}
              </span>
            </td>
            <td class="py-2 text-gray-400 text-xs">{{ e.heartbeat_time ? fmtTime(e.heartbeat_time) : '-' }}</td>
          </tr>
          <tr v-if="!executors.length">
            <td colspan="4" class="py-6 text-center text-gray-400">暂无在线执行器</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, defineComponent, h } from 'vue'
import { executorApi, jobApi } from '@/api'
import type { Executor } from '@/api'
import dayjs from 'dayjs'

// Inline StatCard component
const StatCard = defineComponent({
  props: { label: String, value: [String, Number], color: String, icon: String },
  setup(p) {
    return () => h('div', { class: `bg-white rounded-xl shadow-sm border border-gray-100 p-5 flex items-center gap-4` }, [
      h('div', { class: `w-12 h-12 rounded-xl ${p.color} flex items-center justify-center text-xl` }, p.icon),
      h('div', {}, [
        h('div', { class: 'text-2xl font-bold text-gray-800' }, String(p.value ?? 0)),
        h('div', { class: 'text-sm text-gray-400 mt-0.5' }, p.label)
      ])
    ])
  }
})

const executors = ref<Executor[]>([])
const stats = ref([
  { label: '任务总数', value: 0, color: 'bg-blue-50', icon: '⚙️' },
  { label: '运行中任务', value: 0, color: 'bg-green-50', icon: '▶️' },
  { label: '在线执行器', value: 0, color: 'bg-purple-50', icon: '🖥️' },
  { label: '今日调度次数', value: '-', color: 'bg-orange-50', icon: '📋' }
])

const fmtTime = (t: string) => dayjs(t).format('MM-DD HH:mm:ss')

onMounted(async () => {
  try {
    const [execData, allJobs, runningJobs] = await Promise.all([
      executorApi.list({ page: 1, page_size: 50 }),
      jobApi.list({ page: 1, page_size: 1 }),
      jobApi.list({ page: 1, page_size: 1, status: 1 })
    ])
    executors.value = execData.list
    const onlineCount = execData.list.filter(e => e.status === 1).length
    stats.value[0].value = allJobs.total
    stats.value[1].value = runningJobs.total
    stats.value[2].value = onlineCount
  } catch {}
})
</script>
