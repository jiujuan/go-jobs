<template>
  <div class="space-y-4">
    <div class="flex items-center justify-between">
      <h1 class="text-xl font-bold text-gray-800">任务管理</h1>
      <button @click="openCreate" class="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition">
        + 新增任务
      </button>
    </div>

    <!-- Filters -->
    <div class="bg-white rounded-xl border border-gray-100 shadow-sm p-4 flex gap-3 flex-wrap">
      <input v-model="filters.job_name" @input="fetchList" placeholder="任务名称" class="border border-gray-200 rounded-lg px-3 py-1.5 text-sm w-44" />
      <input v-model="filters.executor_app" @input="fetchList" placeholder="执行器AppName" class="border border-gray-200 rounded-lg px-3 py-1.5 text-sm w-48" />
      <select v-model="filters.status" @change="fetchList" class="border border-gray-200 rounded-lg px-3 py-1.5 text-sm">
        <option value="">全部状态</option>
        <option value="1">运行中</option>
        <option value="0">已停止</option>
      </select>
    </div>

    <!-- Table -->
    <div class="bg-white rounded-xl border border-gray-100 shadow-sm overflow-hidden">
      <table class="w-full text-sm">
        <thead class="bg-gray-50 text-gray-500 border-b border-gray-100">
          <tr>
            <th class="px-4 py-3 text-left font-medium">ID</th>
            <th class="px-4 py-3 text-left font-medium">任务名称</th>
            <th class="px-4 py-3 text-left font-medium">执行器</th>
            <th class="px-4 py-3 text-left font-medium">Cron</th>
            <th class="px-4 py-3 text-left font-medium">路由策略</th>
            <th class="px-4 py-3 text-left font-medium">状态</th>
            <th class="px-4 py-3 text-left font-medium">下次触发</th>
            <th class="px-4 py-3 text-left font-medium w-48">操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-if="loading">
            <td colspan="8" class="py-10 text-center text-gray-400">加载中...</td>
          </tr>
          <tr v-else-if="!list.length">
            <td colspan="8" class="py-10 text-center text-gray-400">暂无数据</td>
          </tr>
          <tr v-for="job in list" :key="job.id" class="border-b border-gray-50 hover:bg-gray-50 transition">
            <td class="px-4 py-3 text-gray-400 font-mono text-xs">{{ job.id }}</td>
            <td class="px-4 py-3 font-medium text-gray-800">
              {{ job.job_name }}
              <p class="text-xs text-gray-400 font-normal mt-0.5">{{ job.job_desc }}</p>
            </td>
            <td class="px-4 py-3 text-gray-500 text-xs">{{ job.executor_app }}</td>
            <td class="px-4 py-3 font-mono text-xs text-blue-600">{{ job.cron_expression || '-' }}</td>
            <td class="px-4 py-3 text-xs text-gray-500">{{ job.route_strategy }}</td>
            <td class="px-4 py-3">
              <span
                :class="job.status === 1
                  ? 'bg-green-100 text-green-700'
                  : 'bg-gray-100 text-gray-500'"
                class="px-2 py-0.5 rounded-full text-xs font-medium"
              >
                {{ job.status === 1 ? '运行中' : '已停止' }}
              </span>
            </td>
            <td class="px-4 py-3 text-xs text-gray-400">
              {{ job.next_trigger_time ? fmtTime(job.next_trigger_time) : '-' }}
            </td>
            <td class="px-4 py-3">
              <div class="flex items-center gap-1.5">
                <button
                  v-if="job.status === 0"
                  @click="startJob(job.id)"
                  class="px-2 py-1 text-xs bg-green-50 text-green-600 rounded hover:bg-green-100 transition"
                >启动</button>
                <button
                  v-else
                  @click="stopJob(job.id)"
                  class="px-2 py-1 text-xs bg-yellow-50 text-yellow-600 rounded hover:bg-yellow-100 transition"
                >停止</button>
                <button
                  @click="triggerJob(job)"
                  class="px-2 py-1 text-xs bg-blue-50 text-blue-600 rounded hover:bg-blue-100 transition"
                >执行</button>
                <button
                  @click="viewLogs(job.id)"
                  class="px-2 py-1 text-xs bg-purple-50 text-purple-600 rounded hover:bg-purple-100 transition"
                >日志</button>
                <button
                  @click="openEdit(job)"
                  class="px-2 py-1 text-xs bg-gray-50 text-gray-600 rounded hover:bg-gray-100 transition"
                >编辑</button>
                <button
                  @click="deleteJob(job.id)"
                  class="px-2 py-1 text-xs bg-red-50 text-red-500 rounded hover:bg-red-100 transition"
                >删除</button>
              </div>
            </td>
          </tr>
        </tbody>
      </table>

      <!-- Pagination -->
      <div class="flex items-center justify-between px-4 py-3 border-t border-gray-100 text-sm text-gray-500">
        <span>共 {{ total }} 条记录</span>
        <div class="flex gap-1">
          <button
            v-for="p in totalPages"
            :key="p"
            @click="page = p; fetchList()"
            :class="p === page ? 'bg-blue-600 text-white' : 'bg-white text-gray-600 hover:bg-gray-50'"
            class="w-8 h-8 rounded border border-gray-200 text-xs transition"
          >{{ p }}</button>
        </div>
      </div>
    </div>

    <!-- Create/Edit Modal -->
    <div v-if="showModal" class="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div class="bg-white rounded-2xl shadow-2xl w-full max-w-2xl max-h-[90vh] overflow-y-auto p-8">
        <h2 class="text-lg font-bold text-gray-800 mb-6">{{ editMode ? '编辑任务' : '新增任务' }}</h2>
        <form @submit.prevent="submitJob" class="space-y-4">
          <div class="grid grid-cols-2 gap-4">
            <div>
              <label class="label">任务名称 *</label>
              <input v-model="form.job_name" required class="input" placeholder="唯一任务名称" />
            </div>
            <div>
              <label class="label">执行器 AppName *</label>
              <input v-model="form.executor_app" required class="input" placeholder="go-jobs-example-executor" />
            </div>
            <div>
              <label class="label">Handler 名称 *</label>
              <input v-model="form.execute_handler" required class="input" placeholder="demoJob" />
            </div>
            <div>
              <label class="label">Cron 表达式</label>
              <input v-model="form.cron_expression" class="input font-mono" placeholder="0/30 * * * * ?" />
            </div>
            <div>
              <label class="label">执行类型</label>
              <select v-model="form.execute_type" class="input">
                <option value="BEAN">BEAN (Go Handler)</option>
                <option value="SHELL">SHELL</option>
                <option value="PYTHON">PYTHON</option>
                <option value="CMD">CMD</option>
              </select>
            </div>
            <div>
              <label class="label">路由策略</label>
              <select v-model="form.route_strategy" class="input">
                <option value="ROUND_ROBIN">轮询</option>
                <option value="RANDOM">随机</option>
                <option value="FIRST">第一个</option>
                <option value="LAST">最后一个</option>
                <option value="CONSISTENT_HASH">一致性哈希</option>
                <option value="LFU">最不常用</option>
                <option value="LRU">最近最少使用</option>
                <option value="SHARDING_BROADCAST">分片广播</option>
              </select>
            </div>
            <div>
              <label class="label">阻塞策略</label>
              <select v-model="form.block_strategy" class="input">
                <option :value="1">串行</option>
                <option :value="2">丢弃后续</option>
                <option :value="3">覆盖之前</option>
              </select>
            </div>
            <div>
              <label class="label">超时(秒) <span class="text-gray-400 text-xs">0=不限</span></label>
              <input v-model.number="form.timeout" type="number" min="0" class="input" />
            </div>
            <div>
              <label class="label">失败重试次数</label>
              <input v-model.number="form.retry_count" type="number" min="0" class="input" />
            </div>
            <div>
              <label class="label">执行器 ID *</label>
              <input v-model.number="form.executor_id" type="number" required class="input" placeholder="1" />
            </div>
          </div>
          <div>
            <label class="label">执行参数 (JSON)</label>
            <textarea v-model="form.execute_param" class="input h-20 resize-none font-mono text-xs" placeholder='{"key":"value"}'></textarea>
          </div>
          <div>
            <label class="label">任务描述</label>
            <input v-model="form.job_desc" class="input" />
          </div>
          <div>
            <label class="label">告警邮箱</label>
            <input v-model="form.alarm_email" class="input" placeholder="a@b.com,c@d.com" />
          </div>

          <div v-if="submitError" class="text-red-500 text-sm">{{ submitError }}</div>

          <div class="flex gap-3 pt-2">
            <button type="submit" :disabled="submitting"
              class="flex-1 py-2.5 bg-blue-600 hover:bg-blue-700 text-white font-semibold rounded-lg transition disabled:opacity-50">
              {{ submitting ? '提交中...' : (editMode ? '保存修改' : '创建任务') }}
            </button>
            <button type="button" @click="closeModal"
              class="flex-1 py-2.5 bg-gray-100 hover:bg-gray-200 text-gray-700 font-semibold rounded-lg transition">
              取消
            </button>
          </div>
        </form>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { jobApi, type JobInfo } from '@/api'
import dayjs from 'dayjs'

const router = useRouter()

// ─── State ────────────────────────────────────────────────────────────────────
const list = ref<JobInfo[]>([])
const total = ref(0)
const page = ref(1)
const pageSize = 20
const loading = ref(false)
const filters = ref({ job_name: '', executor_app: '', status: '' })

const showModal = ref(false)
const editMode = ref(false)
const submitting = ref(false)
const submitError = ref('')

const defaultForm = (): Partial<JobInfo> => ({
  job_name: '',
  job_desc: '',
  executor_id: 0,
  executor_app: '',
  cron_expression: '',
  execute_type: 'BEAN',
  execute_handler: '',
  execute_param: '',
  route_strategy: 'ROUND_ROBIN',
  block_strategy: 1,
  timeout: 0,
  retry_count: 0,
  alarm_email: ''
})
const form = ref<Partial<JobInfo>>(defaultForm())

// ─── Computed ─────────────────────────────────────────────────────────────────
const totalPages = computed(() => {
  const n = Math.ceil(total.value / pageSize)
  return Array.from({ length: Math.min(n, 10) }, (_, i) => i + 1)
})

// ─── Methods ──────────────────────────────────────────────────────────────────
const fmtTime = (t: string) => dayjs(t).format('MM-DD HH:mm:ss')

async function fetchList() {
  loading.value = true
  try {
    const params: any = { page: page.value, page_size: pageSize, ...filters.value }
    if (params.status === '') delete params.status
    const data = await jobApi.list(params)
    list.value = data.list
    total.value = data.total
  } catch {}
  loading.value = false
}

function openCreate() {
  editMode.value = false
  form.value = defaultForm()
  submitError.value = ''
  showModal.value = true
}

function openEdit(job: JobInfo) {
  editMode.value = true
  form.value = { ...job }
  submitError.value = ''
  showModal.value = true
}

function closeModal() {
  showModal.value = false
}

async function submitJob() {
  submitError.value = ''
  submitting.value = true
  try {
    if (editMode.value && form.value.id) {
      await jobApi.update(form.value.id, form.value)
    } else {
      await jobApi.create(form.value)
    }
    closeModal()
    await fetchList()
  } catch (e: any) {
    submitError.value = e.message || '提交失败'
  }
  submitting.value = false
}

async function startJob(id: number) {
  try { await jobApi.start(id); await fetchList() } catch (e: any) { alert(e.message) }
}

async function stopJob(id: number) {
  try { await jobApi.stop(id); await fetchList() } catch (e: any) { alert(e.message) }
}

async function triggerJob(job: JobInfo) {
  const param = prompt(`手动执行任务 [${job.job_name}]\n执行参数 (可留空):`, job.execute_param)
  if (param === null) return
  try {
    await jobApi.trigger(job.id, param)
    alert('触发成功！')
  } catch (e: any) {
    alert('触发失败: ' + e.message)
  }
}

async function deleteJob(id: number) {
  if (!confirm('确认删除该任务？')) return
  try { await jobApi.delete(id); await fetchList() } catch (e: any) { alert(e.message) }
}

function viewLogs(id: number) {
  router.push(`/jobs/${id}/logs`)
}

onMounted(fetchList)
</script>

<style scoped>
.label { @apply block text-sm font-medium text-gray-700 mb-1; }
.input { @apply w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-400; }
</style>
