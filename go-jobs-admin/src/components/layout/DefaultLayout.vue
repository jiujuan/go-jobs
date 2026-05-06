<template>
  <div class="flex h-screen bg-gray-50 overflow-hidden">
    <!-- Sidebar -->
    <aside class="w-56 bg-gray-900 text-white flex flex-col flex-shrink-0">
      <div class="flex items-center gap-3 px-5 py-5 border-b border-gray-700">
        <div class="w-8 h-8 bg-blue-500 rounded-lg flex items-center justify-center text-sm font-bold">GJ</div>
        <span class="font-semibold text-base">go-jobs</span>
      </div>
      <nav class="flex-1 px-3 py-4 space-y-1">
        <router-link
          v-for="item in menuItems"
          :key="item.path"
          :to="item.path"
          class="flex items-center gap-3 px-3 py-2 rounded-lg text-sm font-medium transition"
          :class="isActive(item.path)
            ? 'bg-blue-600 text-white'
            : 'text-gray-400 hover:bg-gray-800 hover:text-white'"
        >
          <span class="text-base">{{ item.icon }}</span>
          {{ item.label }}
        </router-link>
      </nav>
      <div class="px-4 py-4 border-t border-gray-700 text-xs text-gray-500 text-center">
        go-jobs v1.0.0
      </div>
    </aside>

    <!-- Main content -->
    <div class="flex-1 flex flex-col overflow-hidden">
      <!-- Top bar -->
      <header class="bg-white border-b border-gray-200 px-6 py-3 flex items-center justify-between flex-shrink-0">
        <h2 class="text-base font-semibold text-gray-700">{{ currentTitle }}</h2>
        <div class="flex items-center gap-3">
          <span class="text-sm text-gray-500">{{ username }}</span>
          <button
            @click="handleLogout"
            class="text-sm text-gray-500 hover:text-red-500 transition"
          >退出</button>
        </div>
      </header>
      <!-- Page content -->
      <main class="flex-1 overflow-auto p-6">
        <router-view />
      </main>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import { useRoute, useRouter } from 'vue-router'

const route = useRoute()
const router = useRouter()

const menuItems = [
  { path: '/dashboard', label: '控制台', icon: '📊' },
  { path: '/jobs', label: '任务管理', icon: '⚙️' },
  { path: '/executors', label: '执行器', icon: '🖥️' }
]

const isActive = (path: string) => route.path === path || route.path.startsWith(path + '/')
const currentTitle = computed(() => String(route.meta.title ?? 'go-jobs'))
const username = computed(() => {
  try { return JSON.parse(atob(localStorage.getItem('token')!.split('.')[1])).username } catch { return 'admin' }
})

function handleLogout() {
  localStorage.removeItem('token')
  router.push('/login')
}
</script>
